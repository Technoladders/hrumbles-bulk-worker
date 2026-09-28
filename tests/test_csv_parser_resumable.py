"""
tests/test_csv_parser_resumable.py
Verifies the resumable/chunked rewrite of yohr/csv_parser.py's
_process_session: large-file chunking across ticks, resume-from-count on a
mid-batch failure, and that a transient error no longer marks the session
'failed' or silently truncates the rest of the file.

Uses a small stateful fake Supabase double (rows + sessions kept in-memory)
rather than a live DB, following this repo's existing fake-client pattern
for pipeline unit tests.

Run:
    python -m unittest tests.test_csv_parser_resumable -v
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

from yohr import csv_parser  # noqa: E402

SESSION_ID = "s-1"
ORG_ID = "org-1"


def _make_csv(n_rows: int) -> bytes:
    lines = ["name,email"]
    for i in range(1, n_rows + 1):
        lines.append(f"Candidate {i},candidate{i}@example.com")
    return ("\n".join(lines)).encode("utf-8")


class _FakeResult:
    def __init__(self, data=None, count=None):
        self.data = data
        self.count = count


class _FakeQuery:
    """Minimal chainable stand-in for postgrest's request builder."""

    def __init__(self, execute_fn):
        self._execute_fn = execute_fn

    def select(self, *a, **k):
        self._execute_fn.select_cols = a[0] if a else "*"
        self._execute_fn.head = k.get("head", False)
        return self

    def eq(self, *a, **k):
        return self

    def in_(self, *a, **k):
        return self

    def order(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def update(self, payload):
        self._execute_fn.update_payload = payload
        return self

    def insert(self, rows):
        self._execute_fn.insert_rows = rows
        return self

    def upsert(self, rows, on_conflict=None, ignore_duplicates=False):
        self._execute_fn.insert_rows = rows
        self._execute_fn.ignore_duplicates = ignore_duplicates
        return self

    def execute(self):
        return self._execute_fn()


class FakeSupabase:
    """Tracks org_csv_import_rows (a list) and one session dict, in memory.

    Mirrors two production behaviours the parser must cope with:
      * select(count="exact", head=True).count is 0 (our postgrest version
        doesn't populate it) -- the root cause of the duplicate-rows bug.
      * upsert(..., ignore_duplicates=True) against the unique
        (session_id, row_number) index skips rows that already exist.
    """

    def __init__(self, csv_bytes: bytes):
        self.rows = []
        self.session = {"id": SESSION_ID, "s1_complete": False, "status": "pending"}
        self._csv_bytes = csv_bytes
        self.insert_calls = 0
        self.fail_on_insert_call = None  # set to an int to simulate a mid-loop crash
        self.fail_download = False
        self.rpc_calls = []

        class _Storage:
            def from_(inner_self, bucket):
                return inner_self

            def download(inner_self, path):
                if self.fail_download:
                    raise RuntimeError("cannot access local variable 'response'")
                return self._csv_bytes

        self.storage = _Storage()

    def table(self, name):
        outer = self

        class _Ctx:
            insert_rows = None
            update_payload = None
            ignore_duplicates = False
            select_cols = "*"
            head = False

            def __call__(inner_self):
                if name == "org_csv_import_rows" and inner_self.insert_rows is not None:
                    outer.insert_calls += 1
                    if outer.fail_on_insert_call == outer.insert_calls:
                        raise RuntimeError("simulated ConnectionTerminated")
                    existing = {(r["session_id"], r["row_number"]) for r in outer.rows}
                    for r in inner_self.insert_rows:
                        key = (r["session_id"], r["row_number"])
                        if inner_self.ignore_duplicates and key in existing:
                            continue
                        outer.rows.append(r)
                        existing.add(key)
                    return _FakeResult(data=inner_self.insert_rows)
                if name == "org_csv_import_rows" and inner_self.update_payload is None:
                    if inner_self.head:
                        return _FakeResult(data=[], count=0)  # production behaviour
                    if inner_self.select_cols == "row_number":
                        top = max((r["row_number"] for r in outer.rows), default=None)
                        return _FakeResult(data=[] if top is None else [{"row_number": top}])
                    return _FakeResult(data=[], count=len(outer.rows))
                if name == "org_csv_import_sessions" and inner_self.update_payload is not None:
                    outer.session.update(inner_self.update_payload)
                    return _FakeResult(data=[outer.session])
                return _FakeResult(data=[])

        return _FakeQuery(_Ctx())

    def rpc(self, name, args):
        self.rpc_calls.append((name, args))
        outer = self

        class _R:
            def execute(inner_self):
                return _FakeResult(data=None)
        return _R()


SESSION_ROW = {
    "id": SESSION_ID, "org_id": ORG_ID, "filename": "test.csv",
    "file_storage_path": "x/y/source.csv", "column_mapping": None,
    "ai_processing_enabled": False, "resume_download_enabled": False,
}


class TestResumableCsvParsing(unittest.TestCase):
    def test_small_file_completes_in_one_tick(self):
        fake = FakeSupabase(_make_csv(50))
        with patch.object(csv_parser, "supabase", fake):
            csv_parser._process_session(dict(SESSION_ROW))
        self.assertEqual(len(fake.rows), 50)
        self.assertTrue(fake.session["s1_complete"])

    def test_large_file_chunks_across_ticks(self):
        total_rows = csv_parser.MAX_CSV_ROWS_PER_TICK + 500 if hasattr(csv_parser, "MAX_CSV_ROWS_PER_TICK") else 20_500
        from yohr.constants import MAX_CSV_ROWS_PER_TICK
        total_rows = MAX_CSV_ROWS_PER_TICK + 500
        fake = FakeSupabase(_make_csv(total_rows))

        with patch.object(csv_parser, "supabase", fake):
            csv_parser._process_session(dict(SESSION_ROW))
            self.assertEqual(len(fake.rows), MAX_CSV_ROWS_PER_TICK)
            self.assertFalse(fake.session["s1_complete"])
            self.assertNotEqual(fake.session.get("status"), "failed")

            # Second tick resumes from where the first left off.
            csv_parser._process_session(dict(SESSION_ROW))
            self.assertEqual(len(fake.rows), total_rows)
            self.assertTrue(fake.session["s1_complete"])

    def test_mid_batch_failure_preserves_prior_batches_and_does_not_fail_session(self):
        fake = FakeSupabase(_make_csv(1000))
        fake.fail_on_insert_call = 2  # 2nd batch of 200 raises

        with patch.object(csv_parser, "supabase", fake):
            csv_parser._process_session(dict(SESSION_ROW))

        # First batch (200 rows) committed; the crash aborts the rest of
        # THIS tick, but the session must not be marked failed/complete.
        self.assertEqual(len(fake.rows), 200)
        self.assertFalse(fake.session["s1_complete"])
        self.assertNotEqual(fake.session.get("status"), "failed")

        # Next tick resumes from row 201 and (with no more injected failures)
        # finishes the rest of the file.
        fake.fail_on_insert_call = None
        with patch.object(csv_parser, "supabase", fake):
            csv_parser._process_session(dict(SESSION_ROW))
        self.assertEqual(len(fake.rows), 1000)
        self.assertTrue(fake.session["s1_complete"])

    def test_empty_csv_marks_failed_and_s1_complete_only_on_first_attempt(self):
        fake = FakeSupabase(_make_csv(0))
        with patch.object(csv_parser, "supabase", fake):
            csv_parser._process_session(dict(SESSION_ROW))
        self.assertEqual(fake.session.get("status"), "failed")
        self.assertTrue(fake.session["s1_complete"])

    def test_rerunning_a_finished_session_adds_no_duplicates(self):
        fake = FakeSupabase(_make_csv(300))
        with patch.object(csv_parser, "supabase", fake):
            csv_parser._process_session(dict(SESSION_ROW))
            csv_parser._process_session(dict(SESSION_ROW))
            csv_parser._process_session(dict(SESSION_ROW))
        self.assertEqual(len(fake.rows), 300)
        self.assertEqual(len({r["row_number"] for r in fake.rows}), 300)

    def test_large_file_is_fully_read_across_ticks_without_duplicates(self):
        from yohr.constants import MAX_CSV_ROWS_PER_TICK
        total_rows = MAX_CSV_ROWS_PER_TICK * 3 + 7
        fake = FakeSupabase(_make_csv(total_rows))
        with patch.object(csv_parser, "supabase", fake):
            for _ in range(6):
                if fake.session["s1_complete"]:
                    break
                csv_parser._process_session(dict(SESSION_ROW))
        self.assertTrue(fake.session["s1_complete"])
        self.assertEqual(len(fake.rows), total_rows)
        self.assertEqual(max(r["row_number"] for r in fake.rows), total_rows)

    def test_download_failure_while_resuming_does_not_truncate_file(self):
        from yohr.constants import MAX_CSV_ROWS_PER_TICK
        total_rows = MAX_CSV_ROWS_PER_TICK + 50
        fake = FakeSupabase(_make_csv(total_rows))
        with patch.object(csv_parser, "supabase", fake):
            csv_parser._process_session(dict(SESSION_ROW))
            fake.fail_download = True
            csv_parser._process_session(dict(SESSION_ROW))
            self.assertFalse(fake.session["s1_complete"])
            self.assertNotEqual(fake.session.get("status"), "failed")
            fake.fail_download = False
            csv_parser._process_session(dict(SESSION_ROW))
        self.assertTrue(fake.session["s1_complete"])
        self.assertEqual(len(fake.rows), total_rows)


if __name__ == "__main__":
    unittest.main()
