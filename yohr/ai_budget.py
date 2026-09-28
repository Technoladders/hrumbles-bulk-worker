"""
yohr/ai_budget.py
Shared YOHR AI-token-budget helpers used by BOTH yohr/ai_backfill.py
(deferred path) and yohr/ai_processor.py's normal, immediate path — so
yohr_ai_daily_usage reflects TOTAL YOHR AI consumption, not just one
stage's share. bulk_tasks.py's unrelated Batch-API pipeline never calls
into this module; its own usage is intentionally out of scope here
(tracked separately, in hr_resume_ai_results).

The actual increment (record_usage) goes through a single atomic Postgres
upsert RPC (yohr_ai_add_usage), not a Python read-then-write, so concurrent
callers (the normal S3 thread pool + the backfill job, potentially
overlapping in wall-clock time) cannot lose each other's updates.

The pre-call check (get_budget_status) is a plain read, not itself
transactional — it cannot fully prevent every possible overshoot from
callers that pass the check within the same instant, but it can no longer
under-count what actually happened, which is the property that matters:
the recorded total will always match real consumption.
"""
import logging
from datetime import datetime, timedelta, timezone

from .constants import supabase, YOHR_ORG_ID

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))


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


def get_budget_status(org_id: str = YOHR_ORG_ID, enforce_active_hours: bool = False) -> dict:
    """
    Read-only snapshot of whether AI work is currently allowed for this org
    today, under the superadmin-configured budget:
      {"allowed": bool, "reason": str, "tokens_used": int, "budget": int}
    reason is one of: "disabled", "outside_active_hours", "budget_exhausted", "ok".
    """
    cfg = _load_config(org_id)
    if not cfg or not cfg.get("enabled", False):
        return {"allowed": False, "reason": "disabled", "tokens_used": 0, "budget": 0}

    now_ist = _current_ist()
    budget = cfg.get("daily_token_budget") or 0

    if enforce_active_hours and not _in_active_window(cfg, now_ist):
        return {"allowed": False, "reason": "outside_active_hours", "tokens_used": 0, "budget": budget}

    usage_date = now_ist.date().isoformat()
    usage_rows = (
        supabase.table("yohr_ai_daily_usage")
        .select("tokens_used")
        .eq("organization_id", org_id)
        .eq("usage_date", usage_date)
        .limit(1)
        .execute()
        .data
    )
    tokens_used = (usage_rows[0]["tokens_used"] if usage_rows else 0) or 0

    if tokens_used >= budget:
        return {"allowed": False, "reason": "budget_exhausted", "tokens_used": tokens_used, "budget": budget}

    return {"allowed": True, "reason": "ok", "tokens_used": tokens_used, "budget": budget}


def record_usage(tokens: int, org_id: str = YOHR_ORG_ID) -> None:
    """
    Atomically add `tokens` to today's (IST) yohr_ai_daily_usage row for
    org_id, via the yohr_ai_add_usage Postgres RPC (a single INSERT ...
    ON CONFLICT DO UPDATE) -- never a Python read-then-write. Call this
    ONLY after a real, successful OpenAI response; never speculatively,
    never from an exception/failure path.
    """
    if not tokens or tokens <= 0:
        return
    usage_date = _current_ist().date().isoformat()
    try:
        supabase.rpc("yohr_ai_add_usage", {
            "p_organization_id": org_id,
            "p_usage_date": usage_date,
            "p_tokens": tokens,
        }).execute()
    except Exception as exc:
        logger.error("ai_budget: failed to record %d tokens for %s/%s: %s",
                     tokens, org_id, usage_date, exc)
