"""
tests/test_supabase_disconnect_retry.py
Guards for the shared supabase client's "Server disconnected" fix (~50
ERROR lines/day in production, every YOHR stage failing in the same ms):
  * a GET/HEAD that hits httpx.RemoteProtocolError is retried exactly once;
  * writes (POST/PATCH/DELETE — insert, update, rpc) are never retried;
  * other errors pass through untouched;
  * config.supabase's PostgREST session actually goes through the wrapper,
    and postgrest surfaces the httpx error unwrapped.

Run:
    python -m unittest tests.test_supabase_disconnect_retry -v
"""
import os
import unittest
from unittest.mock import patch

import httpx

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault(
    "SUPABASE_SERVICE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoic2VydmljZV9yb2xlIn0.fake",
)
os.environ.setdefault("OPENAI_API_KEY", "sk-dummy")
os.environ.setdefault("YOHR_ALLOW_SHARED_KEY", "true")

import config  # noqa: E402


class _FlakyTransport(httpx.BaseTransport):
    """Raises `exc` for the first `failures` requests, then answers 200 []."""

    def __init__(self, failures=1, exc=None):
        self.failures = failures
        self.exc = exc or httpx.RemoteProtocolError("Server disconnected")
        self.calls = []
        self.closed = False

    def handle_request(self, request):
        self.calls.append(request.method)
        if len(self.calls) <= self.failures:
            raise self.exc
        return httpx.Response(200, json=[{"id": 1}], request=request)

    def close(self):
        self.closed = True


def _send(transport, method):
    return transport.handle_request(httpx.Request(method, "https://example.supabase.co/rest/v1/t"))


class RetryTransportTests(unittest.TestCase):
    def test_read_is_retried_once_after_disconnect(self):
        for method in ("GET", "HEAD"):
            inner = _FlakyTransport(failures=1)
            with self.assertLogs("config", level="WARNING"):
                resp = _send(config._RetryReadsOnDisconnect(inner), method)
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(inner.calls, [method, method])

    def test_read_gives_up_after_one_retry(self):
        inner = _FlakyTransport(failures=2)
        with self.assertLogs("config", level="WARNING"), \
             self.assertRaises(httpx.RemoteProtocolError):
            _send(config._RetryReadsOnDisconnect(inner), "GET")
        self.assertEqual(inner.calls, ["GET", "GET"])

    def test_writes_are_never_retried(self):
        for method in ("POST", "PATCH", "PUT", "DELETE"):
            inner = _FlakyTransport(failures=1)
            with self.assertRaises(httpx.RemoteProtocolError):
                _send(config._RetryReadsOnDisconnect(inner), method)
            self.assertEqual(inner.calls, [method])

    def test_other_errors_are_not_retried(self):
        inner = _FlakyTransport(failures=1, exc=httpx.ReadTimeout("timed out"))
        with self.assertRaises(httpx.ReadTimeout):
            _send(config._RetryReadsOnDisconnect(inner), "GET")
        self.assertEqual(inner.calls, ["GET"])

    def test_close_reaches_inner_transport(self):
        inner = _FlakyTransport()
        config._RetryReadsOnDisconnect(inner).close()
        self.assertTrue(inner.closed)


class SharedClientTests(unittest.TestCase):
    """End to end through config.supabase → postgrest → httpx (no network)."""

    def setUp(self):
        wrapper = config.supabase.postgrest.session._transport
        self.assertIsInstance(wrapper, config._RetryReadsOnDisconnect)
        self.inner = _FlakyTransport(failures=1)
        patcher = patch.object(wrapper, "_transport", self.inner)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_select_survives_a_dropped_connection(self):
        with self.assertLogs("config", level="WARNING"):
            data = config.supabase.table("org_csv_import_sessions").select("id").execute().data
        self.assertEqual(data, [{"id": 1}])
        self.assertEqual(self.inner.calls, ["GET", "GET"])

    def test_update_still_raises_without_retry(self):
        with self.assertRaises(httpx.RemoteProtocolError):
            config.supabase.table("org_csv_import_rows").update({"s4_status": "done"}).eq("id", 1).execute()
        self.assertEqual(self.inner.calls, ["PATCH"])

    def test_rpc_still_raises_without_retry(self):
        with self.assertRaises(httpx.RemoteProtocolError):
            config.supabase.rpc("claim_ingest_batch", {"p_limit": 1}).execute()
        self.assertEqual(self.inner.calls, ["POST"])


if __name__ == "__main__":
    unittest.main()
