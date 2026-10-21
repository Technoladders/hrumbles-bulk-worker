"""
bulk_maintenance.py — periodic upkeep for hr_resume_files (bulk resume pipeline).

Replaces the 15-minute force_reset_stuck_bulk_files() call. That RPC set every
'parsing'/'ingesting' row back to 'pending' with attempts = 0, regardless of
age, so the 3-attempt cap in claim_parse_batch / claim_ingest_batch never held:
a row the claim could not finish (e.g. ai_status='done' with no
hr_resume_ai_results row, which claim_ingest_batch claims but its INNER JOIN
never returns) cycled claim → stale re-claim → reset forever. In production
that was ~600k updates/day on a ~230k-row table.

Recovery does not need the reset: both claim functions already re-claim a row
that has been 'parsing'/'ingesting' for more than 10 minutes, up to 3 attempts.
What was missing is a terminal state for rows that exhaust those attempts —
this sweep marks them 'failed' (attempts untouched), so they stop being claimed
and show up in /api/bulk/failed-files, where /api/bulk/retry can re-queue them.
"""
import logging
from datetime import datetime, timedelta, timezone

from config import supabase

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3          # must match the cap in claim_parse_batch / claim_ingest_batch
STALE_MINUTES = 15        # > the claims' 10-minute stale re-claim window


def sweep_exhausted_files(now: datetime | None = None) -> dict:
    """Mark rows stuck in parsing/ingesting after MAX_ATTEMPTS as failed."""
    now = now or datetime.now(timezone.utc)
    cutoff = (now - timedelta(minutes=STALE_MINUTES)).isoformat()
    result = {"parse": 0, "ingest": 0}

    try:
        r = (supabase.table("hr_resume_files")
             .update({"parse_status": "failed",
                      "parse_error": f"stuck in parsing after {MAX_ATTEMPTS} attempts"})
             .eq("parse_status", "parsing")
             .gte("parse_attempts", MAX_ATTEMPTS)
             .lt("updated_at", cutoff)
             .execute())
        result["parse"] = len(r.data or [])
    except Exception as exc:
        logger.error(f"[Sweep] parse sweep failed: {exc}")

    try:
        r = (supabase.table("hr_resume_files")
             .update({"ingest_status": "failed", "ingest_result": "FAILED",
                      "ingest_error": f"stuck in ingesting after {MAX_ATTEMPTS} attempts"})
             .eq("ingest_status", "ingesting")
             .gte("ingest_attempts", MAX_ATTEMPTS)
             .lt("updated_at", cutoff)
             .execute())
        result["ingest"] = len(r.data or [])
    except Exception as exc:
        logger.error(f"[Sweep] ingest sweep failed: {exc}")

    if result["parse"] or result["ingest"]:
        logger.info(f"[Sweep] marked failed after max attempts: {result}")
    return result
