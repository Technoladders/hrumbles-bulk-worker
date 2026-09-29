"""
yohr/ai_manual.py
On-demand "run AI on N profiles" requests from the CSV Import History page
(table yohr_ai_manual_runs, created via the request_yohr_ai_run RPC).

A request asks for AI on N profiles that were imported with AI disabled
(s3_status='skipped') and are already in the talent pool (s4_status='done'),
either within one import (session_id) or across all of the org's imports.
Profiles with a downloaded resume are picked first so the AI reads the real CV.

Same rules as the automatic ai_backfill: the same AI call and client, and
the same daily token budget + active hours (yohr_ai_processing_config via
ai_budget.get_budget_status). Outside active hours / budget spent / disabled,
the request is parked as 'waiting' with the reason and resumes automatically.

On success a row gets s3_status='done' + ai_result and s4_status='pending',
so the existing ingestor re-upserts it (on_conflict email+organization_id)
and the candidate's hr_talent_pool record is updated with the AI fields.

Rows are claimed (skipped -> processing, conditional) before the AI call so
two requests, or a request and the automatic backfill's next tick, don't
spend tokens on the same profile.
"""
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from .constants import supabase, yohr_ai_client, ACTIVE_ORG_IDS, MAX_AI_WORKERS
from .ai_processor import _call_ai_raw
from .ai_backfill import _build_backfill_text
from .ai_budget import get_budget_status, record_usage

logger = logging.getLogger(__name__)

# Profiles per tick per request. Each AI call takes a few seconds; keep ticks
# short so progress shows up steadily on the page.
BATCH_PER_TICK = 10

_ROW_COLS = (
    "id, session_id, org_id, stored_resume_path, raw_name, raw_designation, "
    "raw_company, raw_location, resume_text_excerpt"
)

_REASONS = {
    "disabled": "AI processing is turned off for this organisation (Single Organization Dashboard)",
    "outside_active_hours": "Waiting for AI active hours",
    "budget_exhausted": "Today's AI token budget is used up — resumes tomorrow",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _set_run(run_id: str, **fields) -> None:
    supabase.table("yohr_ai_manual_runs").update({**fields, "updated_at": _now()}).eq("id", run_id).execute()


def _eligible(run: dict, limit: int) -> list[dict]:
    """Up to `limit` AI-skipped, already-ingested rows for the request; rows
    with a downloaded resume first."""
    def base():
        q = (supabase.table("org_csv_import_rows").select(_ROW_COLS)
             .eq("org_id", run["org_id"])
             .eq("s3_status", "skipped")
             .eq("s4_status", "done"))
        if run.get("session_id"):
            q = q.eq("session_id", run["session_id"])
        return q

    rows = base().not_.is_("stored_resume_path", "null").order("row_number").limit(limit).execute().data or []
    if len(rows) < limit:
        seen = {r["id"] for r in rows}
        more = base().is_("stored_resume_path", "null").order("row_number").limit(limit - len(rows)).execute().data or []
        rows += [r for r in more if r["id"] not in seen]
    return rows


def _claim(rows: list[dict]) -> list[dict]:
    """skipped -> processing, only for rows still skipped. Returns claimed rows."""
    if not rows:
        return []
    ids = [r["id"] for r in rows]
    claimed = (supabase.table("org_csv_import_rows")
               # s4 -> pending now (not only on success): if the worker restarts
               # mid-call, ai_processor's stuck-row reset sends the row back to
               # s3 'pending', it finishes there, and the ingestor still
               # re-upserts it -- the talent-pool record isn't left stale.
               .update({"s3_status": "processing", "s3_error": None, "s4_status": "pending"})
               .in_("id", ids).eq("s3_status", "skipped")
               .execute().data) or []
    claimed_ids = {r["id"] for r in claimed}
    return [r for r in rows if r["id"] in claimed_ids]


def _process_row(row: dict) -> bool:
    """Run AI on one claimed row. Returns True on success."""
    try:
        full_text = _build_backfill_text(row)
        ai_result, tokens = _call_ai_raw(full_text, yohr_ai_client)
        supabase.table("org_csv_import_rows").update({
            "s3_status":           "done",
            "s3_error":            None,
            "resume_text_excerpt": full_text,
            "ai_result":           ai_result,
            "s4_status":           "pending",   # ingestor updates hr_talent_pool
        }).eq("id", row["id"]).execute()
        record_usage(tokens, row["org_id"])
        return True
    except Exception as exc:
        logger.warning("ai_manual: row %s failed: %s", row["id"], exc)
        # Failed explicitly (not back to 'skipped') so the same bad profile
        # isn't re-picked forever; it shows in the import's AI-failed count.
        supabase.table("org_csv_import_rows").update({
            "s3_status": "failed",
            "s3_error":  f"Manual AI run failed: {exc}"[:500],
            "s4_status": "done",   # unchanged talent-pool record is still valid
        }).eq("id", row["id"]).execute()
        return False


def _run_one_request(run: dict) -> None:
    run_id = run["id"]
    remaining = run["requested_count"] - run["processed_count"] - run["failed_count"]
    if remaining <= 0:
        _set_run(run_id, status="done", status_reason=None)
        return

    status = get_budget_status(run["org_id"], enforce_active_hours=True)
    if not status["allowed"]:
        reason = _REASONS.get(status["reason"], status["reason"])
        if run["status"] != "waiting" or run.get("status_reason") != reason:
            _set_run(run_id, status="waiting", status_reason=reason)
        return

    rows = _claim(_eligible(run, min(remaining, BATCH_PER_TICK)))
    if not rows:
        done = run["processed_count"] + run["failed_count"]
        _set_run(run_id, status="done",
                 status_reason=None if done >= run["requested_count"]
                 else f"No more eligible profiles (ran {done} of {run['requested_count']})")
        return

    _set_run(run_id, status="running", status_reason=None)
    with ThreadPoolExecutor(max_workers=max(1, MAX_AI_WORKERS)) as pool:
        results = list(pool.map(_process_row, rows))

    ok = sum(results)
    bad = len(results) - ok
    # Re-read counts in case the row was cancelled meanwhile.
    cur = supabase.table("yohr_ai_manual_runs").select("status, processed_count, failed_count").eq("id", run_id).single().execute().data
    processed = cur["processed_count"] + ok
    failed = cur["failed_count"] + bad
    fields = {"processed_count": processed, "failed_count": failed}
    if cur["status"] != "cancelled":
        fields["status"] = "done" if processed + failed >= run["requested_count"] else "running"
    _set_run(run_id, **fields)

    for sid in {r["session_id"] for r in rows}:
        try:
            supabase.rpc("refresh_csv_session_counts", {"p_session_id": sid}).execute()
        except Exception as exc:
            logger.warning("ai_manual: refresh counts failed for %s: %s", sid, exc)
    logger.info("ai_manual: run %s +%d done, +%d failed (%d/%d)",
                run_id, ok, bad, processed + failed, run["requested_count"])


def run_ai_manual() -> None:
    try:
        runs = (supabase.table("yohr_ai_manual_runs")
                .select("id, org_id, session_id, requested_count, processed_count, failed_count, status, status_reason")
                .in_("org_id", ACTIVE_ORG_IDS)
                .in_("status", ["queued", "running", "waiting"])
                .order("created_at")
                .limit(5)
                .execute().data) or []
    except Exception as exc:
        logger.error("ai_manual: failed to fetch requests: %s", exc)
        return

    for run in runs:
        try:
            _run_one_request(run)
        except Exception as exc:
            logger.error("ai_manual: request %s failed this tick: %s", run["id"], exc, exc_info=True)
