"""
tests/test_ai_manual.py
On-demand AI runs (yohr/ai_manual.py): picks AI-skipped, already-ingested
rows (resume first), claims them, respects the budget gate, tracks progress
and finishes when N are done or nothing is left.

Run:
    python -m unittest tests.test_ai_manual -v
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

from yohr import ai_manual  # noqa: E402


class _Q:
    def __init__(self, db, table):
        self.db, self.table = db, table
        self.filters, self._update, self._limit, self._order, self._single = [], None, None, None, False

    def select(self, *a, **k): return self
    def eq(self, c, v): self.filters.append(lambda r: r.get(c) == v); return self
    def in_(self, c, vs): self.filters.append(lambda r: r.get(c) in vs); return self
    def is_(self, c, v): self.filters.append(lambda r: r.get(c) is None); return self
    @property
    def not_(self):
        outer = self
        class N:
            def is_(self, c, v): outer.filters.append(lambda r: r.get(c) is not None); return outer
        return N()
    def order(self, c, desc=False): self._order = c; return self
    def limit(self, n): self._limit = n; return self
    def single(self): self._single = True; return self
    def update(self, payload): self._update = payload; return self

    def execute(self):
        rows = [r for r in self.db[self.table] if all(f(r) for f in self.filters)]
        if self._order:
            rows.sort(key=lambda r: r.get(self._order) or 0)
        if self._update is not None:
            for r in rows:
                r.update(self._update)
        if self._limit is not None:
            rows = rows[: self._limit]
        class R: pass
        res = R()
        res.data = (dict(rows[0]) if rows else None) if self._single else [dict(r) for r in rows]
        return res


class _Fake:
    def __init__(self, rows, runs):
        self.db = {"org_csv_import_rows": rows, "yohr_ai_manual_runs": runs}
        self.rpcs = []
    def table(self, name): return _Q(self.db, name)
    def rpc(self, name, args):
        self.rpcs.append(name)
        class R:
            def execute(s): return None
        return R()


def _row(i, resume=True, session="s1", s3="skipped", s4="done"):
    return {"id": f"r{i}", "session_id": session, "org_id": "org", "row_number": i,
            "stored_resume_path": f"p/{i}.pdf" if resume else None,
            "s3_status": s3, "s4_status": s4, "raw_name": f"N{i}"}


def _run(n, session="s1", status="queued"):
    return {"id": "run1", "org_id": "org", "session_id": session, "requested_count": n,
            "processed_count": 0, "failed_count": 0, "status": status, "status_reason": None}


ALLOWED = {"allowed": True, "reason": "ok", "tokens_used": 0, "budget": 1}


class TestManualAiRuns(unittest.TestCase):
    def _tick(self, fake, budget=ALLOWED, ai=lambda text, client: ({"ok": True}, 100)):
        usage = []
        with patch.object(ai_manual, "supabase", fake), \
             patch.object(ai_manual, "ACTIVE_ORG_IDS", ["org"]), \
             patch.object(ai_manual, "get_budget_status", lambda *a, **k: budget), \
             patch.object(ai_manual, "record_usage", lambda t, o: usage.append(t)), \
             patch.object(ai_manual, "_call_ai_raw", ai), \
             patch.object(ai_manual, "_build_backfill_text", lambda row: "text"):
            ai_manual.run_ai_manual()
        return usage

    def test_runs_exactly_n_profiles_preferring_resumes(self):
        rows = [_row(1, resume=False), _row(2), _row(3), _row(4, resume=False), _row(5)]
        fake = _Fake(rows, [_run(3)])
        usage = self._tick(fake)
        done = sorted(r["id"] for r in rows if r["s3_status"] == "done")
        self.assertEqual(done, ["r2", "r3", "r5"])                       # resumes first
        self.assertTrue(all(r["s4_status"] == "pending" for r in rows if r["s3_status"] == "done"))
        self.assertEqual(usage, [100, 100, 100])
        run = fake.db["yohr_ai_manual_runs"][0]
        self.assertEqual((run["processed_count"], run["status"]), (3, "done"))
        self.assertIn("refresh_csv_session_counts", fake.rpcs)

    def test_only_its_own_import_and_only_ingested_skipped_rows(self):
        rows = [_row(1, session="other"), _row(2, s4="pending"), _row(3, s3="done"), _row(4)]
        fake = _Fake(rows, [_run(10)])
        self._tick(fake)
        self.assertEqual([r["id"] for r in rows if r["s3_status"] == "done" and r["s4_status"] == "pending"], ["r4"])
        run = fake.db["yohr_ai_manual_runs"][0]
        self.assertEqual(run["status"], "running")                        # 1 of 10 so far
        self._tick(fake)                                                  # nothing left
        self.assertEqual(run["status"], "done")
        self.assertIn("No more eligible profiles", run["status_reason"])

    def test_all_imports_when_no_session(self):
        rows = [_row(1, session="a"), _row(2, session="b")]
        fake = _Fake(rows, [_run(2, session=None)])
        self._tick(fake)
        self.assertEqual(sum(r["s3_status"] == "done" for r in rows), 2)

    def test_waits_outside_budget_or_hours(self):
        rows = [_row(1)]
        fake = _Fake(rows, [_run(1)])
        self._tick(fake, budget={"allowed": False, "reason": "outside_active_hours", "tokens_used": 0, "budget": 1})
        run = fake.db["yohr_ai_manual_runs"][0]
        self.assertEqual(run["status"], "waiting")
        self.assertIn("active hours", run["status_reason"])
        self.assertEqual(rows[0]["s3_status"], "skipped")                 # untouched
        self._tick(fake)                                                  # hours open again
        self.assertEqual(run["status"], "done")

    def test_ai_failure_marks_row_failed_and_keeps_talent_record(self):
        rows = [_row(1)]
        fake = _Fake(rows, [_run(1)])
        def boom(text, client): raise RuntimeError("model error")
        usage = self._tick(fake, ai=boom)
        self.assertEqual((rows[0]["s3_status"], rows[0]["s4_status"]), ("failed", "done"))
        self.assertEqual(usage, [])
        run = fake.db["yohr_ai_manual_runs"][0]
        self.assertEqual((run["failed_count"], run["status"]), (1, "done"))

    def test_rows_claimed_elsewhere_are_not_reprocessed(self):
        rows = [_row(1, s3="processing"), _row(2)]
        fake = _Fake(rows, [_run(2)])
        self._tick(fake)
        self.assertEqual(rows[0]["s3_status"], "processing")
        self.assertEqual(rows[1]["s3_status"], "done")


if __name__ == "__main__":
    unittest.main()
