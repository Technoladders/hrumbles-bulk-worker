"""
tests/test_fair_share.py
fetch_fair_share must not let one large session starve the others.

Run:
    python -m unittest tests.test_fair_share -v
"""
import os
import unittest
from collections import Counter
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault(
    "SUPABASE_SERVICE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoic2VydmljZV9yb2xlIn0.fake",
)
os.environ.setdefault("OPENAI_API_KEY", "sk-dummy")
os.environ.setdefault("YOHR_ALLOW_SHARED_KEY", "true")

from yohr import fair_share  # noqa: E402


class _Q:
    """Tiny in-memory query over a list of dicts (eq / gt / order / limit)."""

    def __init__(self, rows):
        self._rows = list(rows)
        self._limit = None

    def select(self, *a, **k):
        return self

    def in_(self, col, vals):
        self._rows = [r for r in self._rows if r.get(col) in vals]
        return self

    def eq(self, col, val):
        self._rows = [r for r in self._rows if r.get(col) == val]
        return self

    def gt(self, col, val):
        self._rows = [r for r in self._rows if r.get(col) > val]
        return self

    def order(self, col, desc=False):
        self._rows.sort(key=lambda r: r.get(col), reverse=desc)
        return self

    def limit(self, n):
        self._limit = n
        return self

    def execute(self):
        class R:
            pass
        r = R()
        r.data = self._rows[: self._limit] if self._limit is not None else self._rows
        return r


class _FakeSupabase:
    def __init__(self, sessions, rows):
        self.sessions, self.rows = sessions, rows

    def table(self, name):
        return _Q(self.sessions if name == "org_csv_import_sessions" else self.rows)


def _rows(session_id, n, start=1):
    return [{"id": f"{session_id}-{i}", "session_id": session_id, "row_number": i,
             "org_id": "org", "s2_status": "pending"} for i in range(start, start + n)]


class TestFairShare(unittest.TestCase):
    def _run(self, sessions, rows, limit):
        fake = _FakeSupabase(sessions, rows)
        with patch.object(fair_share, "supabase", fake), patch.object(fair_share, "ACTIVE_ORG_IDS", ["org"]):
            return fair_share.fetch_fair_share(
                lambda: fake.table("org_csv_import_rows").select("*").eq("s2_status", "pending"), limit
            )

    @staticmethod
    def _session(sid, created):
        return {"id": sid, "org_id": "org", "status": "processing", "created_at": created}

    def test_big_session_does_not_starve_a_newer_one(self):
        # Old session listed first with far more rows (the batch 4 / merged_part_03 case).
        rows = _rows("big", 77_000) + _rows("new", 57_000)
        picked = self._run([self._session("big", "1"), self._session("new", "2")], rows, 80)
        self.assertEqual(Counter(r["session_id"] for r in picked), {"big": 40, "new": 40})

    def test_idle_sessions_budget_goes_to_busy_ones(self):
        sessions = [self._session(f"idle{i}", str(i)) for i in range(20)]
        sessions += [self._session("a", "90"), self._session("b", "91")]
        rows = _rows("idle3", 2) + _rows("a", 1000) + _rows("b", 1000)
        picked = self._run(sessions, rows, 80)
        counts = Counter(r["session_id"] for r in picked)
        self.assertEqual(len(picked), 80)
        self.assertEqual(counts["idle3"], 2)
        self.assertGreaterEqual(counts["a"], 35)
        self.assertGreaterEqual(counts["b"], 35)

    def test_no_duplicates_and_row_order_within_session(self):
        rows = _rows("a", 50) + _rows("b", 5)
        picked = self._run([self._session("a", "1"), self._session("b", "2")], rows, 40)
        self.assertEqual(len(picked), len({r["id"] for r in picked}))
        a_rows = [r["row_number"] for r in picked if r["session_id"] == "a"]
        self.assertEqual(a_rows, sorted(a_rows))
        self.assertEqual(Counter(r["session_id"] for r in picked)["b"], 5)

    def test_falls_back_to_plain_fetch_without_open_sessions(self):
        picked = self._run([], _rows("orphan", 10), 80)
        self.assertEqual(len(picked), 10)


if __name__ == "__main__":
    unittest.main()
