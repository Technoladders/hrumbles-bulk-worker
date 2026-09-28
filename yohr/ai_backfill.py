"""
yohr/ai_backfill.py
Deferred AI enrichment for rows that were ingested with ai_processing_enabled
disabled (raw CSV + resume data already in hr_talent_pool, no structured AI
extraction yet). Runs on its own schedule, gated by a per-org config the
superadmin edits from the "Single Organization Dashboard" (see
Hrumbles-Front-End_UI's yohr_ai_processing_config / yohr_ai_daily_usage
tables + yohr_ai_get_config / yohr_ai_save_config RPCs) — no env var, no
redeploy needed to change the daily token budget, active hours, or to pause
it entirely.

Deliberately a separate module from ai_processor.py (Stage 3's normal,
immediate path for ai_processing_enabled=true rows) so that stage's existing
per-row behavior is untouched by this one. The two share the request/parsing
logic via ai_processor._call_ai_raw, and now also share the budget
check/accounting logic via ai_budget.py (get_budget_status/record_usage),
so yohr_ai_daily_usage reflects total YOHR AI consumption from both paths,
not just this one. This module still owns its own row-selection query, its
own client (yohr_ai_client — see config.py's isolation note), and the one
behavior that makes this a "backfill" and not just a delayed first attempt:
on success, it resets s4_status back to 'pending' so the existing
ingestor.run_ingestor() naturally re-upserts (on_conflict=
"email,organization_id") and UPDATES the already-ingested hr_talent_pool row
with the newly-enriched fields, rather than ever touching hr_talent_pool
directly from here.

Timezone: "daily" and "active hours" are both interpreted in IST
(Asia/Kolkata, UTC+5:30, no DST) — confirmed with the org, since that's YO HR
Consultancy's own locale. All timestamptz columns are still stored/compared
in UTC as usual; only the *day boundary* and *hour-of-day* comparisons here
are done in IST (see ai_budget.py).
"""
import logging

from .constants import supabase, yohr_ai_client, YOHR_ORG_ID
from .ai_processor import _call_ai_raw, _extract_text, _sanitize
from .ai_budget import get_budget_status, record_usage

logger = logging.getLogger(__name__)

# Small batch per tick — this is a background enrichment pass, not the
# primary pipeline; no need to race through the whole backlog in one go.
BATCH_SIZE = 20


def _build_backfill_text(row: dict) -> str:
    full_text = row.get("resume_text_excerpt") or ""
    if not full_text and row.get("stored_resume_path"):
        full_text = _extract_text(row["stored_resume_path"])

    if not full_text:
        parts = []
        for key, label in [("raw_name", "Name"), ("raw_designation", "Title"),
                            ("raw_company", "Company"), ("raw_location", "Location")]:
            if row.get(key):
                parts.append(f"{label}: {row[key]}")
        full_text = "\n".join(parts) or f"Candidate: {row.get('raw_name', 'Unknown')}"

    return _sanitize(full_text)


def run_ai_backfill() -> None:
    status = get_budget_status(YOHR_ORG_ID, enforce_active_hours=True)
    if not status["allowed"]:
        return  # disabled, outside active hours, or today's budget already spent

    try:
        rows = (
            supabase.table("org_csv_import_rows")
            .select(
                "id, session_id, stored_resume_path, raw_name, raw_designation, "
                "raw_company, raw_location, resume_text_excerpt"
            )
            .eq("org_id", YOHR_ORG_ID)
            .eq("ai_processing_enabled", False)
            .eq("s3_status", "skipped")
            .eq("s4_status", "done")
            .limit(BATCH_SIZE)
            .execute()
            .data
        )
    except Exception as exc:
        logger.error("ai_backfill: fetch failed: %s", exc)
        return

    if not rows:
        return

    logger.info("ai_backfill: %d eligible rows, %d/%d tokens used today",
                len(rows), status["tokens_used"], status["budget"])

    for row in rows:
        # Re-check on every row (not just once per tick): this is what makes
        # the budget gate reflect the OTHER path's (normal ai_processor's)
        # concurrent consumption too, not just this loop's own running total.
        status = get_budget_status(YOHR_ORG_ID, enforce_active_hours=True)
        if not status["allowed"]:
            logger.info("ai_backfill: stopping for now (%s) — %d/%d tokens used today",
                        status["reason"], status["tokens_used"], status["budget"])
            break

        row_id = row["id"]
        try:
            full_text = _build_backfill_text(row)
            # tokens_this_call is only ever non-zero here because a real,
            # successful OpenAI response was returned — record_usage() below
            # is therefore never called for a row that didn't make a real
            # request, and never called at all from the except branch.
            ai_result, tokens_this_call = _call_ai_raw(full_text, yohr_ai_client)

            supabase.table("org_csv_import_rows").update({
                "s3_status":           "done",
                "s3_error":            None,
                "resume_text_excerpt": full_text,
                "ai_result":           ai_result,
                # The one new step: lets the existing ingestor pick this row
                # up again and UPDATE (not re-insert — on_conflict is
                # email+organization_id) the already-ingested hr_talent_pool
                # record with these enriched fields.
                "s4_status":           "pending",
            }).eq("id", row_id).execute()

            record_usage(tokens_this_call, YOHR_ORG_ID)

            logger.info("ai_backfill: row %s done (+%d tokens today)",
                        row_id, tokens_this_call)

        except Exception as exc:
            # Left as s3_status='skipped' deliberately — this was never a
            # "real" S3 attempt before (it was skipped by design), so there's
            # no s3_attempts/failed semantics to reuse here. Just retry next
            # tick. No usage recorded — no real OpenAI response was returned.
            logger.warning("ai_backfill: row %s failed, will retry next tick: %s",
                           row_id, exc)
