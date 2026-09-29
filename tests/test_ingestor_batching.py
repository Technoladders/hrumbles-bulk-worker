"""
tests/test_ingestor_batching.py
The ingestor must write s4 results in bulk (not one UPDATE per row) and
merge duplicate emails so the bulk talent-pool upsert doesn't fail.

Run:
    python -m unittest tests.test_ingestor_batching -v
"""
import os
import unittest
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault(
    "SUPABASE_SERVICE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoic2VydmljZV9yb2xlIn0.fake",
)
os.environ.setdefault("OPENAI_API_KEY", "sk-dummy")
os.environ.setdefault("YOHR_ALLOW_SHARED_KEY", "true")

from yohr import ingestor  # noqa: E402


class _Call:
    def __init__(self, log, table):
        self.log, self.table = log, table

    def upsert(self, payload, on_conflict=None, **k):
        self.log.append((self.table, "upsert", payload, on_conflict))
        self._payload = payload
        return self

    def update(self, payload):
        self.log.append((self.table, "update", payload, None))
        return self

    def eq(self, *a):
        return self

    def execute(self):
        class R:
            pass
        r = R()
        if self.table == "hr_talent_pool":
            recs = self._payload if isinstance(self._payload, list) else [self._payload]
            emails = [x["email"] for x in recs]
            if len(emails) != len({e.lower() for e in emails}):
                raise RuntimeError("ON CONFLICT DO UPDATE command cannot affect row a second time")
            r.data = [{"id": f"tp-{x['email'].lower()}", "email": x["email"]} for x in recs]
        else:
            r.data = []
        return r


class _Fake:
    def __init__(self):
        self.log = []

    def table(self, name):
        return _Call(self.log, name)


def _rows(n, dup_every=None):
    rows, recs = [], {}
    for i in range(n):
        email = f"c{i}@x.com"
        if dup_every and i % dup_every == 1:
            email = f"C{i-1}@X.com"          # same person, different case
        row = {"id": f"r{i}", "session_id": "s", "row_number": i + 1, "org_id": "o"}
        rows.append(row)
        recs[row["id"]] = {"email": email, "organization_id": "o", "candidate_name": f"N{i}",
                           "resume_path": None if i % 2 else f"p{i}"}
    return rows, recs


class TestIngestorBatching(unittest.TestCase):
    def test_500_rows_use_two_requests_not_500(self):
        fake = _Fake()
        rows, recs = _rows(500)
        with patch.object(ingestor, "supabase", fake), patch.object(ingestor, "INGEST_PARALLEL", 1):
            ingestor._upsert_rows(list(recs.values()), rows, recs)
        kinds = [(t, k) for t, k, _, _ in fake.log]
        self.assertEqual(kinds, [("hr_talent_pool", "upsert"), ("org_csv_import_rows", "upsert")])
        status = fake.log[1][2]
        self.assertEqual(len(status), 500)
        self.assertTrue(all(u["s4_status"] == "done" and u["talent_pool_id"] for u in status))
        self.assertTrue(all({"session_id", "row_number", "org_id"} <= set(u) for u in status))

    def test_duplicate_emails_are_merged_so_bulk_upsert_succeeds(self):
        fake = _Fake()
        rows, recs = _rows(10, dup_every=2)
        with patch.object(ingestor, "supabase", fake), patch.object(ingestor, "INGEST_PARALLEL", 1):
            ingestor._upsert_rows(list(recs.values()), rows, recs)
        self.assertEqual([k for _, k, _, _ in fake.log], ["upsert", "upsert"])  # no fallback
        sent = fake.log[0][2]
        self.assertEqual(len(sent), 5)
        # merged record keeps a non-empty resume_path from either copy
        self.assertTrue(all(r["resume_path"] for r in sent))
        status = fake.log[1][2]
        self.assertEqual(len(status), 10)
        self.assertTrue(all(u["talent_pool_id"] for u in status))

    def test_parallel_chunks_are_disjoint_and_mark_every_row(self):
        fake = _Fake()
        rows, recs = _rows(500, dup_every=2)          # 250 people, 2 rows each
        with patch.object(ingestor, "supabase", fake), patch.object(ingestor, "INGEST_PARALLEL", 4):
            ingestor._upsert_rows(list(recs.values()), rows, recs)
        talent = [p for t, k, p, _ in fake.log if t == "hr_talent_pool"]
        status = [u for t, k, p, _ in fake.log if t == "org_csv_import_rows" for u in p]
        self.assertEqual(len(talent), 4)                               # 4 parallel bulk upserts
        emails = [r["email"].lower() for chunk in talent for r in chunk]
        self.assertEqual(len(emails), 250)
        self.assertEqual(len(emails), len(set(emails)))                # no email in two chunks
        self.assertEqual(sorted(u["id"] for u in status), sorted(recs))  # every row marked once
        self.assertTrue(all(u["s4_status"] == "done" and u["talent_pool_id"] for u in status))


if __name__ == "__main__":
    unittest.main()
