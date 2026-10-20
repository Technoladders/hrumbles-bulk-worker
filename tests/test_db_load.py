"""
tests/test_db_load.py
Guards for the two production DB-load fixes:
  * the stuck-row resets run at most once per STUCK_RESET_INTERVAL_SECONDS,
    not on every scheduler tick (each run is a full scan of org_csv_import_rows);
  * fetch_fair_share stops re-querying sessions that had nothing pending for
    a stage, without losing rows that appear later.

Run:
    python -m unittest tests.test_db_load -v
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

from yohr import throttle, fair_share, resume_downloader as rd, ai_processor as ap  # noqa: E402
from tests.test_fair_share import _Q, _rows  # noqa: E402


class _Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


class TestThrottle(unittest.TestCase):
    def setUp(self):
        throttle.reset_all()

    def test_due_once_per_interval(self):
        clock = _Clock()
        with patch.object(throttle.time, "monotonic", clock):
            self.assertTrue(throttle.due("k", 300))
            clock.t += 299
            self.assertFalse(throttle.due("k", 300))
            clock.t += 2
            self.assertTrue(throttle.due("k", 300))

    def test_keys_are_independent(self):
        with patch.object(throttle.time, "monotonic", _Clock()):
            self.assertTrue(throttle.due("a", 300))
            self.assertTrue(throttle.due("b", 300))


class _ResetRecorder:
    """Fake supabase: records UPDATE calls on org_csv_import_rows."""

    def __init__(self):
        self.updates = 0

    def table(self, name):
        rec = self

        class _U:
            def update(self, payload):
                rec.updates += 1
                return self

            def in_(self, *a):
                return self

            def eq(self, *a):
                return self

            def lt(self, *a):
                return self

            def execute(self):
                class R:
                    data = []
                return R()

        return _U()


class TestStuckResetThrottled(unittest.TestCase):
    def setUp(self):
        throttle.reset_all()

    def _ticks(self, module, run, n_ticks, tick_seconds):
        fake, clock = _ResetRecorder(), _Clock()
        patches = [patch.object(module, "supabase", fake), patch.object(throttle.time, "monotonic", clock)]
        if module is rd:
            patches.append(patch.object(rd, "fetch_fair_share", lambda *a, **k: []))
        for p in patches:
            p.start()
        try:
            for _ in range(n_ticks):
                try:
                    run()
                except Exception:
                    pass  # downstream fetch hits the fake; only the reset count matters here
                clock.t += tick_seconds
        finally:
            for p in reversed(patches):
                p.stop()
        return fake.updates

    def test_downloader_reset_runs_every_5_min_not_every_tick(self):
        # 1 hour of 15 s ticks = 240 ticks; before the fix: 240 full-table scans.
        updates = self._ticks(rd, rd.run_downloader, 240, 15)
        self.assertEqual(updates, 12)

    def test_ai_processor_reset_runs_every_150_s_not_every_tick(self):
        # 1 hour of 20 s ticks = 180 ticks; before the fix: 180 full-table scans.
        # 150 s isn't a multiple of 20 s, so it fires every 160 s: t = 0, 160, ... 3520.
        updates = self._ticks(ap, ap.run_ai_processor, 180, 20)
        self.assertEqual(updates, 23)

    def test_first_tick_after_restart_resets_immediately(self):
        self.assertEqual(self._ticks(rd, rd.run_downloader, 1, 15), 1)


class _CountingSupabase:
    """Like test_fair_share._FakeSupabase, but counts per-session row queries."""

    def __init__(self, sessions, rows):
        self.sessions, self.rows = sessions, rows
        self.session_queries = Counter()

    def table(self, name):
        if name == "org_csv_import_sessions":
            return _Q(self.sessions)
        owner = self

        class _CQ(_Q):
            def eq(self, col, val):
                if col == "session_id":
                    owner.session_queries[val] += 1
                return super().eq(col, val)

        return _CQ(self.rows)


def _session(sid, created):
    return {"id": sid, "org_id": "org", "status": "partial", "created_at": created}


class TestIdleSessionCooldown(unittest.TestCase):
    def setUp(self):
        fair_share.reset_idle_cache()

    def _fetch(self, fake, clock, limit=80, stage="s2"):
        with patch.object(fair_share, "supabase", fake), \
             patch.object(fair_share, "ACTIVE_ORG_IDS", ["org"]), \
             patch.object(fair_share.time, "monotonic", clock):
            return fair_share.fetch_fair_share(
                lambda: fake.table("org_csv_import_rows").select("*").eq("s2_status", "pending"),
                limit, stage=stage,
            )

    def test_idle_sessions_are_not_requeried_every_tick(self):
        sessions = [_session(f"idle{i}", str(i)) for i in range(20)] + [_session("busy", "99")]
        fake, clock = _CountingSupabase(sessions, _rows("busy", 10_000)), _Clock()
        for _ in range(8):                      # 8 ticks of 15 s = 2 minutes
            self.assertEqual(len(self._fetch(fake, clock)), 80)
            clock.t += 15
        for i in range(20):
            self.assertEqual(fake.session_queries[f"idle{i}"], 1, "idle session re-queried inside cooldown")
        self.assertGreaterEqual(fake.session_queries["busy"], 8)

    def test_idle_session_is_rechecked_after_cooldown(self):
        fake, clock = _CountingSupabase([_session("a", "1")], []), _Clock()
        self._fetch(fake, clock)
        clock.t += fair_share.IDLE_SESSION_COOLDOWN_SECONDS + 1
        self._fetch(fake, clock)
        self.assertEqual(fake.session_queries["a"], 2)

    def test_rows_appearing_in_a_cooling_session_are_still_picked_up(self):
        fake, clock = _CountingSupabase([_session("a", "1")], []), _Clock()
        self.assertEqual(self._fetch(fake, clock), [])
        fake.rows = _rows("a", 5)                # retry put rows back to pending
        clock.t += 15
        picked = self._fetch(fake, clock)        # still cooling: found via top-up
        self.assertEqual(len(picked), 5)
        clock.t += 15
        self._fetch(fake, clock)                 # cooldown cleared by the top-up hit
        self.assertEqual(fake.session_queries["a"], 2)

    def test_without_stage_behaviour_is_unchanged(self):
        fake, clock = _CountingSupabase([_session("a", "1")], []), _Clock()
        for _ in range(3):
            self._fetch(fake, clock, stage=None)
            clock.t += 15
        self.assertEqual(fake.session_queries["a"], 3)


if __name__ == "__main__":
    unittest.main()
