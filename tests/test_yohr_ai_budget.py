"""
tests/test_yohr_ai_budget.py
Unit tests for the YOHR API-key isolation and AI-token-budget changes.
No live DB/Redis/OpenAI required -- Supabase/OpenAI clients are either
never constructed (subprocess import checks) or mocked (behavior checks).

Run:
    python -m unittest discover -s tests -v
"""
import base64
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]


def _fake_jwt() -> str:
    """supabase-py's create_client validates the key looks like a JWT
    (header.payload.signature) before ever making a network call -- a plain
    string is rejected at construction time. This is a well-formed but
    unsigned/fake token, good enough to satisfy that shape check offline."""
    def seg(obj):
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()
    return f"{seg({'alg': 'HS256', 'typ': 'JWT'})}.{seg({'role': 'service_role'})}.fake-signature"


# config.py requires these to even construct clients; dummy values are fine
# since supabase-py / openai's client constructors don't make network calls.
BASE_ENV = {
    "SUPABASE_URL": "https://example.supabase.co",
    "SUPABASE_SERVICE_KEY": _fake_jwt(),
    "OPENAI_API_KEY": "sk-dummy-general-key",
}


def _run_config_import(extra_env: dict) -> subprocess.CompletedProcess:
    """Import config.py in a clean subprocess with a controlled env, so each
    case gets a fresh module (config.py has import-time side effects/raises
    that unittest's shared interpreter can't cleanly re-trigger via reload)."""
    env = {**os.environ, **BASE_ENV, **extra_env}
    # Isolate from a real .env file / real key if one happens to be present.
    env.pop("YOHR_OPENAI_API_KEY", None)
    env.pop("YOHR_ALLOW_SHARED_KEY", None)
    env.update(extra_env)
    return subprocess.run(
        [sys.executable, "-c", "import config"],
        cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=30,
    )


class TestApiKeyIsolation(unittest.TestCase):
    """Section A / Test 1 & 2."""

    def test_missing_yohr_key_fails_clearly_in_production(self):
        result = _run_config_import({})
        self.assertNotEqual(result.returncode, 0, "config.py should refuse to import")
        self.assertIn("YOHR_OPENAI_API_KEY", result.stderr)
        self.assertIn("RuntimeError", result.stderr)

    def test_shared_key_opt_in_allows_missing_yohr_key(self):
        result = _run_config_import({"YOHR_ALLOW_SHARED_KEY": "true"})
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_yohr_key_set_uses_a_different_key_than_general_client(self):
        script = (
            "import config; "
            "assert config.openai_client.api_key != config.yohr_ai_client.api_key, "
            "'yohr_ai_client must not share the general OPENAI_API_KEY when "
            "YOHR_OPENAI_API_KEY is set'; "
            "print('OK')"
        )
        env = {**os.environ, **BASE_ENV, "YOHR_OPENAI_API_KEY": "sk-dummy-yohr-key"}
        env.pop("YOHR_ALLOW_SHARED_KEY", None)
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("OK", result.stdout)

    def test_no_secret_values_printed_on_failure(self):
        env = {**os.environ, **BASE_ENV}
        env["OPENAI_API_KEY"] = "sk-super-secret-general-key"
        env.pop("YOHR_OPENAI_API_KEY", None)
        env.pop("YOHR_ALLOW_SHARED_KEY", None)
        result = _run_config_import({})
        self.assertNotIn("sk-super-secret-general-key", result.stdout + result.stderr)


def _set_base_env():
    for k, v in BASE_ENV.items():
        os.environ.setdefault(k, v)
    os.environ["YOHR_ALLOW_SHARED_KEY"] = "true"  # so importing config in-process never raises
    os.environ.setdefault("YOHR_OPENAI_API_KEY", "sk-dummy-yohr-key")


_set_base_env()

# Imported once, after env is set, for the in-process mock-based tests below.
from yohr import ai_budget  # noqa: E402


class TestBudgetStatus(unittest.TestCase):
    """Section B / Test 5."""

    def _mock_supabase(self, config_row, usage_row):
        mock_sb = MagicMock()

        def table(name):
            m = MagicMock()
            if name == "yohr_ai_processing_config":
                m.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = (
                    [config_row] if config_row else []
                )
            elif name == "yohr_ai_daily_usage":
                m.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value.data = (
                    [usage_row] if usage_row else []
                )
            return m

        mock_sb.table.side_effect = table
        return mock_sb

    def test_disabled_config_blocks(self):
        mock_sb = self._mock_supabase({"enabled": False, "daily_token_budget": 1000}, None)
        with patch.object(ai_budget, "supabase", mock_sb):
            status = ai_budget.get_budget_status("org-1")
        self.assertFalse(status["allowed"])
        self.assertEqual(status["reason"], "disabled")

    def test_budget_exhausted_blocks(self):
        mock_sb = self._mock_supabase(
            {"enabled": True, "daily_token_budget": 1000},
            {"tokens_used": 1000},
        )
        with patch.object(ai_budget, "supabase", mock_sb):
            status = ai_budget.get_budget_status("org-1")
        self.assertFalse(status["allowed"])
        self.assertEqual(status["reason"], "budget_exhausted")

    def test_under_budget_allows(self):
        mock_sb = self._mock_supabase(
            {"enabled": True, "daily_token_budget": 1000},
            {"tokens_used": 999},
        )
        with patch.object(ai_budget, "supabase", mock_sb):
            status = ai_budget.get_budget_status("org-1")
        self.assertTrue(status["allowed"])


class TestRecordUsage(unittest.TestCase):
    """Section B & D / Test 3, 4, 6."""

    def test_record_usage_calls_atomic_rpc_not_read_then_write(self):
        mock_sb = MagicMock()
        with patch.object(ai_budget, "supabase", mock_sb):
            ai_budget.record_usage(1234, "org-1")

        mock_sb.rpc.assert_called_once()
        rpc_name, rpc_args = mock_sb.rpc.call_args[0]
        self.assertEqual(rpc_name, "yohr_ai_add_usage")
        self.assertEqual(rpc_args["p_tokens"], 1234)
        self.assertEqual(rpc_args["p_organization_id"], "org-1")
        # The whole point of the atomic RPC: no separate .table("yohr_ai_daily_usage")
        # select-then-update round trip from Python for the increment itself.
        for call in mock_sb.table.call_args_list:
            self.assertNotEqual(call.args[0], "yohr_ai_daily_usage")

    def test_zero_or_negative_tokens_never_calls_rpc(self):
        mock_sb = MagicMock()
        with patch.object(ai_budget, "supabase", mock_sb):
            ai_budget.record_usage(0, "org-1")
            ai_budget.record_usage(-5, "org-1")
        mock_sb.rpc.assert_not_called()


class TestWorkerConcurrencyBounded(unittest.TestCase):
    """Section C / Test 8."""

    def test_ai_workers_and_download_workers_are_reduced(self):
        from yohr import constants
        self.assertEqual(constants.MAX_AI_WORKERS, 2)
        # Downloads were raised (8, then 24) once each one's memory was capped:
        # keep the thread count bounded and the per-resume cap in place so
        # S2's peak memory (workers x cap) stays well under the 512 MiB limit.
        self.assertLessEqual(constants.MAX_DOWNLOAD_WORKERS, 32)
        self.assertLessEqual(constants.MAX_RESUME_BYTES, 10 * 1024 * 1024)
        self.assertLessEqual(constants.MAX_DOWNLOAD_WORKERS * constants.MAX_RESUME_BYTES, 128 * 1024 * 1024)


class TestBulkTasksUnaffected(unittest.TestCase):
    """Section F / Test 7 -- static check, since importing bulk_tasks.py
    pulls in rq/redis connections we don't want to require for this test."""

    def test_bulk_tasks_still_imports_general_openai_client_only(self):
        source = (REPO_ROOT / "bulk_tasks.py").read_text(encoding="utf-8")
        self.assertIn("from config import supabase, openai_client, STORAGE_BUCKET", source)
        self.assertNotIn("yohr_ai_client", source)
        self.assertNotIn("yohr_ai_daily_usage", source)


if __name__ == "__main__":
    unittest.main()
