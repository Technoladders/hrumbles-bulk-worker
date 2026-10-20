"""
tests/test_resume_churn.py
Guards for the hr_resume_files churn fix (~600k updates/day in production):
  * the 15-min job no longer zeroes attempts (force_reset_stuck_bulk_files);
    it only retires rows that exhausted their attempts — attempts untouched;
  * poll only marks ai_status='done' for files whose hr_resume_ai_results row
    was stored (claim_ingest_batch INNER JOINs it; 'done' without a result was
    claimed forever);
  * once an OpenAI batch exists, a bookkeeping failure never marks the job
    failed / files pending (that discarded paid results and resubmitted);
  * _update_file reports failed writes.

Run:
    python -m unittest tests.test_resume_churn -v
"""
import json
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault(
    "SUPABASE_SERVICE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoic2VydmljZV9yb2xlIn0.fake",
)
os.environ.setdefault("OPENAI_API_KEY", "sk-dummy")
os.environ.setdefault("YOHR_ALLOW_SHARED_KEY", "true")

import bulk_maintenance  # noqa: E402
import bulk_tasks  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


class _Op:
    """One recorded query: table, verb, payload and the filter chain."""

    def __init__(self, log, table, fail_on=None):
        self.log, self.table, self.fail_on = log, table, fail_on or set()
        self.entry = {"table": table, "verb": None, "payload": None, "filters": []}

    def _verb(self, verb, payload=None):
        self.entry.update(verb=verb, payload=payload)
        self.log.append(self.entry)
        return self

    def update(self, payload):
        return self._verb("update", payload)

    def insert(self, payload):
        return self._verb("insert", payload)

    def select(self, *a, **k):
        return self._verb("select")

    def __getattr__(self, name):          # eq / in_ / gte / lt / order / limit ...
        def f(*args, **kwargs):
            self.entry["filters"].append((name,) + args)
            return self
        return f

    def execute(self):
        key = (self.table, self.entry["verb"])
        if key in self.fail_on:
            raise RuntimeError(f"simulated failure on {key}")

        class R:
            data = []
        return R()


class _Fake:
    def __init__(self, fail_on=None):
        self.log, self.fail_on = [], set(fail_on or ())

    def table(self, name):
        return _Op(self.log, name, self.fail_on)

    def updates(self, table):
        return [e for e in self.log if e["table"] == table and e["verb"] == "update"]


class TestSweepReplacesAttemptReset(unittest.TestCase):
    def test_sweep_retires_only_exhausted_stale_rows_without_touching_attempts(self):
        fake = _Fake()
        now = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
        with patch.object(bulk_maintenance, "supabase", fake):
            bulk_maintenance.sweep_exhausted_files(now=now)

        ups = fake.updates("hr_resume_files")
        self.assertEqual(len(ups), 2)
        parse, ingest = ups
        self.assertEqual(parse["payload"]["parse_status"], "failed")
        self.assertEqual(ingest["payload"]["ingest_status"], "failed")
        for u in ups:
            self.assertFalse(any("attempts" in k for k in u["payload"]), "attempts must not be reset")
        self.assertIn(("eq", "parse_status", "parsing"), parse["filters"])
        self.assertIn(("gte", "parse_attempts", 3), parse["filters"])
        self.assertIn(("eq", "ingest_status", "ingesting"), ingest["filters"])
        self.assertIn(("gte", "ingest_attempts", 3), ingest["filters"])
        cutoff = "2026-10-20T11:45:00+00:00"       # 15 min > the claims' 10-min stale window
        self.assertIn(("lt", "updated_at", cutoff), parse["filters"])
        self.assertIn(("lt", "updated_at", cutoff), ingest["filters"])

    def test_one_failing_sweep_does_not_skip_the_other(self):
        fake = _Fake()
        calls = {"n": 0}
        orig = fake.table

        def flaky(name):
            calls["n"] += 1
            op = orig(name)
            if calls["n"] == 1:
                op.fail_on = {("hr_resume_files", "update")}
            return op
        fake.table = flaky
        with patch.object(bulk_maintenance, "supabase", fake):
            bulk_maintenance.sweep_exhausted_files()
        self.assertEqual(len(fake.updates("hr_resume_files")), 2)

    def test_scheduler_no_longer_zeroes_attempts(self):
        src = (ROOT / "app.py").read_text(encoding="utf-8")
        self.assertNotIn("rpc('force_reset_stuck_bulk_files'", src)
        self.assertIn("sweep_exhausted_files()", src)
        self.assertIn("id='sweep_exhausted'", src)


class _Batch:
    def __init__(self, status="completed", output_file_id="out-1", id="batch-1"):
        self.status, self.output_file_id, self.id = status, output_file_id, id


def _line(file_uuid, content=None, error=None):
    cid = "f" + file_uuid.replace("-", "")
    body = {"choices": [{"message": {"content": json.dumps(content or {"email": "a@b.com"})}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1}}
    return json.dumps({"custom_id": cid, "error": error, "response": {"body": body}})


class _OAI:
    def __init__(self, lines):
        text = "\n".join(lines)

        class Files:
            def content(self_inner, _id):
                class C:
                    pass
                c = C()
                c.text = text
                return c
        self.files = Files()

        class Batches:
            def retrieve(self_inner, _id):
                return _Batch()
        self.batches = Batches()


F1 = "11111111-1111-1111-1111-111111111111"
F2 = "22222222-2222-2222-2222-222222222222"


class TestPollMarksDoneOnlyWhenStored(unittest.TestCase):
    def _poll(self, fake):
        job = {"id": "job-1", "openai_batch_id": "batch-1", "organization_id": "org-1",
               "updated_at": datetime.now(timezone.utc).isoformat()}
        with patch.object(bulk_tasks, "supabase", fake), \
             patch.object(bulk_tasks, "openai_client", _OAI([_line(F1), _line(F2)])):
            bulk_tasks._check_one_batch(job)

    @staticmethod
    def _status_ids(fake, status):
        ids = []
        for u in fake.updates("hr_resume_files"):
            if u["payload"].get("ai_status") == status:
                for f in u["filters"]:
                    if f[0] == "in_":
                        ids += list(f[2])
        return ids

    def test_results_not_stored_become_failed_not_done(self):
        fake = _Fake(fail_on={("hr_resume_ai_results", "insert")})   # non-duplicate insert error
        self._poll(fake)
        self.assertEqual(self._status_ids(fake, "done"), [])
        self.assertCountEqual(self._status_ids(fake, "failed"), [F1, F2])

    def test_stored_results_become_done(self):
        fake = _Fake()
        self._poll(fake)
        self.assertCountEqual(self._status_ids(fake, "done"), [F1, F2])
        self.assertEqual(self._status_ids(fake, "failed"), [])


class TestSubmitAfterBatchCreated(unittest.TestCase):
    def _oai(self):
        class O:
            class files:
                @staticmethod
                def create(**k):
                    class F:
                        id = "file-1"
                    return F()

            class batches:
                @staticmethod
                def create(**k):
                    return _Batch(status="validating")
        return O()

    def test_bookkeeping_failure_after_create_never_reverts_files_to_pending(self):
        fake = _Fake(fail_on={("hr_resume_batch_jobs", "update")})
        orig = fake.table

        def table(name):
            op = orig(name)
            if name == "hr_resume_batch_jobs" and op.entry["verb"] is None:
                # the job row insert must succeed and return an id
                def insert(payload):
                    op._verb("insert", payload)

                    class R:
                        data = [{"id": "job-1"}]
                    op.execute = lambda: R()
                    return op
                op.insert = insert
            return op
        fake.table = table

        with patch.object(bulk_tasks, "supabase", fake), patch.object(bulk_tasks, "openai_client", self._oai()):
            with self.assertRaises(RuntimeError):
                bulk_tasks._submit_org_batch("org-1", [{"id": F1, "resume_text": "x"}])

        file_updates = fake.updates("hr_resume_files")
        self.assertFalse(any(u["payload"].get("ai_status") == "pending" for u in file_updates))
        job_updates = fake.updates("hr_resume_batch_jobs")
        self.assertFalse(any(u["payload"].get("status") == "failed" for u in job_updates))
        self.assertEqual(sum(1 for u in job_updates if u["payload"].get("status") == "processing"), 2)  # retried once


class TestUpdateFileReportsFailure(unittest.TestCase):
    def test_returns_false_on_failed_write(self):
        with patch.object(bulk_tasks, "supabase", _Fake(fail_on={("hr_resume_files", "update")})):
            self.assertFalse(bulk_tasks._update_file(F1, {"parse_status": "parsed"}))
        with patch.object(bulk_tasks, "supabase", _Fake()):
            self.assertTrue(bulk_tasks._update_file(F1, {"parse_status": "parsed"}))


if __name__ == "__main__":
    unittest.main()
