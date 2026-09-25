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
behavior is completely untouched by this one. The two share the request/
parsing logic via ai_processor._call_ai_raw, not duplicated here, but this
module owns its own row-selection query, its own client (yohr_ai_client —
see config.py's isolation note), and the one behavior that makes this a
"backfill" and not just a delayed first attempt: on success, it resets
s4_status back to 'pending' so the existing ingestor.run_ingestor() naturally
re-upserts (on_conflict="email,organization_id") and UPDATES the
already-ingested hr_talent_pool row with the newly-enriched fields, rather
than ever touching hr_talent_pool directly from here.

Timezone: "daily" and "active hours" are both interpreted in IST
(Asia/Kolkata, UTC+5:30, no DST) — confirmed with the org, since that's YO HR
Consultancy's own locale. All timestamptz columns are still stored/compared
in UTC as usual; only the *day boundary* and *hour-of-day* comparisons here
are done in IST.
"""
import logging
from datetime import datetime, timedelta, timezone

from .constants import supabase, yohr_ai_client, YOHR_ORG_ID
from .ai_processor import _call_ai_raw, _extract_text, _sanitize

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

# Small batch per tick — this is a background enrichment pass, not the
# primary pipeline; no need to race through the whole backlog in one go.
BATCH_SIZE = 20


def _current_ist() -> datetime:
    return datetime.now(timezone.utc).astimezone(IST)


def _load_config(org_id: str) -> dict | None:
    rows = (
        supabase.table("yohr_ai_processing_config")
        .select("*")
        .eq("organization_id", org_id)
        .limit(1)
        .execute()
        .data
    )
    return rows[0] if rows else None


def _in_active_window(cfg: dict, now_ist: datetime) -> bool:
    start_h = cfg.get("active_hours_start")
    end_h = cfg.get("active_hours_end")
    if start_h is None or end_h is None:
        return True  # no restriction configured
    hour = now_ist.hour
    if start_h <= end_h:
        return start_h <= hour < end_h
    return hour >= start_h or hour < end_h  # wraps past midnight, e.g. 22 -> 6


def _get_or_create_usage_row(org_id: str, usage_date: str) -> dict:
    existing = (
        supabase.table("yohr_ai_daily_usage")
        .select("*")
        .eq("organization_id", org_id)
        .eq("usage_date", usage_date)
        .limit(1)
        .execute()
        .data
    )
    if existing:
        return existing[0]
    inserted = (
        supabase.table("yohr_ai_daily_usage")
        .insert({
            "organization_id": org_id,
            "usage_date": usage_date,
            "tokens_used": 0,
            "rows_processed": 0,
        })
        .execute()
        .data
    )
    return inserted[0] if inserted else {"tokens_used": 0, "rows_processed": 0}


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
    cfg = _load_config(YOHR_ORG_ID)
    if not cfg or not cfg.get("enabled", False):
        return  # not configured yet, or paused from the dashboard

    now_ist = _current_ist()
    if not _in_active_window(cfg, now_ist):
        return

    usage_date = now_ist.date().isoformat()
    usage = _get_or_create_usage_row(YOHR_ORG_ID, usage_date)
    tokens_used = usage.get("tokens_used") or 0
    rows_processed = usage.get("rows_processed") or 0
    budget = cfg.get("daily_token_budget") or 0

    if tokens_used >= budget:
        return  # today's budget already spent

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
                len(rows), tokens_used, budget)

    for row in rows:
        if tokens_used >= budget:
            logger.info("ai_backfill: daily budget reached (%d/%d) — stopping for today",
                        tokens_used, budget)
            break

        row_id = row["id"]
        try:
            full_text = _build_backfill_text(row)
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

            tokens_used += tokens_this_call
            rows_processed += 1
            supabase.table("yohr_ai_daily_usage").update({
                "tokens_used":    tokens_used,
                "rows_processed": rows_processed,
                "updated_at":     datetime.now(timezone.utc).isoformat(),
            }).eq("organization_id", YOHR_ORG_ID).eq("usage_date", usage_date).execute()

            logger.info("ai_backfill: row %s done (+%d tokens, %d/%d today)",
                        row_id, tokens_this_call, tokens_used, budget)

        except Exception as exc:
            # Left as s3_status='skipped' deliberately — this was never a
            # "real" S3 attempt before (it was skipped by design), so there's
            # no s3_attempts/failed semantics to reuse here. Just retry next
            # tick.
            logger.warning("ai_backfill: row %s failed, will retry next tick: %s",
                           row_id, exc)
