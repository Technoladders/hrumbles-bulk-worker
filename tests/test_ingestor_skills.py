"""
tests/test_ingestor_skills.py
The ingestor must not write skills_master itself. The table is
(skill_name, organization_id NOT NULL, unique on both) and is filled by the
hr_talent_pool trigger trg_add_skills_from_talent_pool from the top_skills the
ingestor already upserts; the removed worker upsert used columns that don't
exist (name / name_lower / source) and failed with PGRST204 on every batch.

Run:
    python -m unittest tests.test_ingestor_skills -v
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


class _Recorder:
    """Records every table/rpc touched; answers the session lookup with no rows."""

    def __init__(self):
        self.tables, self.rpcs = [], []

    def table(self, name):
        self.tables.append(name)
        rec = self

        class _Q:
            def select(self, *a, **k):
                return self

            def in_(self, *a):
                return self

            def eq(self, *a):
                return self

            def upsert(self, *a, **k):
                return self

            def execute(self):
                class R:
                    data = []
                return R()

        return _Q()

    def rpc(self, name, params):
        self.rpcs.append(name)

        class _R:
            def execute(self):
                return None
        return _R()


def _row(i):
    return {
        "id": f"r{i}", "session_id": "s1", "row_number": i, "org_id": "org-1",
        "raw_name": f"Candidate {i}", "raw_email": f"c{i}@example.com",
        "raw_designation": "Engineer", "raw_company": "Acme", "raw_location": "Pune",
        "raw_notice": "", "raw_linkedin": None, "raw_extra_fields": {},
        "parsed_phone": None, "parsed_linkedin": None, "stored_resume_path": None,
        "resume_text_excerpt": "resume text",
        "ai_result": {"top_skills": ["Python", "SQL"]},
    }


class TestIngestorDoesNotWriteSkillsMaster(unittest.TestCase):
    def test_batch_upserts_talent_records_but_never_touches_skills_master(self):
        fake, upserted = _Recorder(), []
        with patch.object(ingestor, "supabase", fake), \
             patch.object(ingestor, "fetch_fair_share", lambda *a, **k: [_row(1), _row(2)]), \
             patch.object(ingestor, "_upsert_rows", lambda recs, rows, m: upserted.extend(recs)):
            ingestor.run_ingestor()

        self.assertNotIn("skills_master", fake.tables)
        self.assertEqual(len(upserted), 2)
        # The skills still reach hr_talent_pool, where the DB trigger syncs skills_master.
        for rec in upserted:
            self.assertTrue(rec.get("top_skills"), "top_skills must still be written to hr_talent_pool")
        self.assertIn("refresh_csv_session_counts", fake.rpcs)

    def test_removed_helper_is_gone(self):
        self.assertFalse(hasattr(ingestor, "_sync_skills_master"))


if __name__ == "__main__":
    unittest.main()
