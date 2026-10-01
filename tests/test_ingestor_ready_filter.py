"""
tests/test_ingestor_ready_filter.py
The ingestor must pick up rows whose AI step failed, and rows whose resume
download failed (they never enter AI), instead of leaving them in s4
"pending" forever. Checks the PostgREST filter the ingestor sends.

Run:
    python -m unittest tests.test_ingestor_ready_filter -v
"""
import os
import unittest
from urllib.parse import unquote
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault(
    "SUPABASE_SERVICE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoic2VydmljZV9yb2xlIn0.fake",
)
os.environ.setdefault("OPENAI_API_KEY", "sk-dummy")
os.environ.setdefault("YOHR_ALLOW_SHARED_KEY", "true")

from yohr import ingestor  # noqa: E402


def _ready(row: dict) -> bool:
    """Evaluate S3_READY_FILTER (plus the s2 gate) against one row."""
    s2, s3 = row["s2_status"], row["s3_status"]
    if s2 not in ("done", "skipped", "failed"):
        return False
    return s3 in ("done", "skipped", "failed") or (s2 == "failed" and s3 == "pending")


class TestIngestReadyFilter(unittest.TestCase):
    def test_filter_string_matches_intended_rules(self):
        self.assertEqual(
            ingestor.S3_READY_FILTER,
            "s3_status.in.(done,skipped,failed),and(s2_status.eq.failed,s3_status.eq.pending)",
        )

    def test_rules(self):
        cases = [
            ("done", "done", True), ("done", "skipped", True),
            ("done", "failed", True),            # AI gave up -> ingest without AI
            ("failed", "pending", True),         # download failed, never enters AI
            ("failed", "skipped", True),
            ("done", "pending", False),          # AI still to run
            ("done", "processing", False),
            ("pending", "skipped", False),       # resume still downloading
            ("downloading", "skipped", False),
        ]
        for s2, s3, want in cases:
            with self.subTest(s2=s2, s3=s3):
                self.assertEqual(_ready({"s2_status": s2, "s3_status": s3}), want)

    def test_query_sends_the_or_filter(self):
        captured = {}

        def fake_fetch(build_query, limit, exclude_ids=None):
            q = build_query()
            captured["params"] = unquote(str(q.params))
            return []

        with patch.object(ingestor, "fetch_fair_share", fake_fetch):
            ingestor.run_ingestor()
        self.assertIn(
            "or=(s3_status.in.(done,skipped,failed),and(s2_status.eq.failed,s3_status.eq.pending))",
            captured["params"],
        )


if __name__ == "__main__":
    unittest.main()
