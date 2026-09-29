"""
tests/test_downloader_nonblocking.py
A slow/hung resume download must not block the downloader tick (it used to
wait for every download, freezing the pipeline for minutes).

Run:
    python -m unittest tests.test_downloader_nonblocking -v
"""
import os
import threading
import time
import unittest
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault(
    "SUPABASE_SERVICE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoic2VydmljZV9yb2xlIn0.fake",
)
os.environ.setdefault("OPENAI_API_KEY", "sk-dummy")
os.environ.setdefault("YOHR_ALLOW_SHARED_KEY", "true")

from yohr import resume_downloader as rd  # noqa: E402


def _row(i, sid="s1"):
    return {"id": f"r{i}", "session_id": sid, "row_number": i, "org_id": "o",
            "raw_resume_url": f"https://x/{i}.pdf", "s2_attempts": 0}


class TestNonBlockingDownloader(unittest.TestCase):
    def setUp(self):
        rd._in_flight.clear()
        rd._touched_sessions.clear()

    def test_tick_returns_while_a_download_hangs(self):
        release = threading.Event()
        done = []

        def fake_download(row):
            if row["id"] == "r0":
                release.wait(5)          # simulate a hung URL
            done.append(row["id"])

        with patch.object(rd, "_download_row", fake_download), \
             patch.object(rd, "_refresh_touched_sessions", lambda: None):
            t0 = time.monotonic()
            rd._submit([_row(i) for i in range(10)])
            self.assertLess(time.monotonic() - t0, 1.0, "tick blocked on downloads")
            deadline = time.monotonic() + 3
            while len(done) < 9 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(len(done), 9, "other downloads were held up by the hung one")
            self.assertIn("r0", rd._in_flight)
            release.set()
            deadline = time.monotonic() + 3
            while rd._in_flight and time.monotonic() < deadline:
                time.sleep(0.05)
        self.assertEqual(rd._in_flight, set())
        self.assertIn("s1", rd._touched_sessions)

    def test_rows_already_in_flight_are_not_resubmitted(self):
        started = []
        gate = threading.Event()

        def fake_download(row):
            started.append(row["id"])
            gate.wait(3)

        with patch.object(rd, "_download_row", fake_download), \
             patch.object(rd, "_refresh_touched_sessions", lambda: None):
            rd._submit([_row(1)])
            rd._submit([_row(1), _row(2)])
            time.sleep(0.3)
            gate.set()
            time.sleep(0.3)
        self.assertEqual(sorted(started), ["r1", "r2"])

    def test_in_flight_is_capped(self):
        gate = threading.Event()
        with patch.object(rd, "_download_row", lambda row: gate.wait(3)), \
             patch.object(rd, "_refresh_touched_sessions", lambda: None), \
             patch.object(rd, "MAX_IN_FLIGHT", 5):
            rd._submit([_row(i) for i in range(20)])
            self.assertEqual(len(rd._in_flight), 5)
            gate.set()


class TestStatusUpdateSurvivesDroppedConnection(unittest.TestCase):
    """A dropped Supabase connection must not strand a row in "downloading"."""

    def _client(self, fail_times, calls):
        class Q:
            def update(s, payload):
                calls.append(payload)
                return s

            def eq(s, *a):
                return s

            def execute(s):
                if len(calls) <= fail_times:
                    raise RuntimeError("Server disconnected")
                return None

        class C:
            def table(s, name):
                return Q()
        return C()

    def test_retries_on_fresh_client_then_succeeds(self):
        calls, made = [], []

        def factory(*a):
            made.append(1)
            return self._client(2, calls)

        rd._thread_local.db = None
        with patch.object(rd, "create_client", factory), patch.object(rd.time, "sleep", lambda s: None):
            rd._set_row("r1", {"s2_status": "done"})
        self.assertEqual(len(calls), 3)
        self.assertEqual(len(made), 3)  # a new client after each failure
        rd._thread_local.db = None

    def test_gives_up_after_tries(self):
        calls = []
        rd._thread_local.db = None
        with patch.object(rd, "create_client", lambda *a: self._client(99, calls)), \
             patch.object(rd.time, "sleep", lambda s: None):
            with self.assertRaises(RuntimeError):
                rd._set_row("r1", {"s2_status": "done"})
        self.assertEqual(len(calls), 3)
        rd._thread_local.db = None

    def test_each_thread_gets_its_own_client(self):
        made = []
        rd._thread_local.db = None
        with patch.object(rd, "create_client", lambda *a: made.append(object()) or made[-1]):
            ids = []
            ts = [threading.Thread(target=lambda: ids.append(id(rd._db()))) for _ in range(4)]
            [t.start() for t in ts]
            [t.join() for t in ts]
        self.assertEqual(len(set(ids)), 4)

if __name__ == "__main__":
    unittest.main()
