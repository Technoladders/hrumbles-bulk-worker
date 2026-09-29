"""
yohr/fair_share.py
Pick a stage's next batch of rows fairly across import sessions.

A plain `.eq(status, "pending").limit(N)` lets Postgres return rows from
whichever session its index yields first, so one large import can occupy
every tick for hours while newer imports sit at 0% ("processing" with no
visible movement). This splits each tick's budget across every session that
still has work, oldest session first, then tops up any unused budget.
"""
import logging
from typing import Callable

from .constants import supabase, ACTIVE_ORG_IDS

logger = logging.getLogger(__name__)

# Sessions that can still have rows waiting in a later stage. 'partial' is
# included because retrying failed rows puts them back to 'pending' without
# necessarily changing the session status.
_OPEN_STATUSES = ["pending", "processing", "partial"]
MAX_SESSIONS_PER_TICK = 25


def _open_session_ids() -> list[str]:
    rows = (
        supabase.table("org_csv_import_sessions")
        .select("id, created_at")
        .in_("org_id", ACTIVE_ORG_IDS)
        .in_("status", _OPEN_STATUSES)
        .order("created_at")
        .limit(MAX_SESSIONS_PER_TICK)
        .execute()
        .data
    ) or []
    return [r["id"] for r in rows]


def fetch_fair_share(build_query: Callable[[], object], limit: int, exclude_ids=None) -> list[dict]:
    """
    build_query() must return a fresh postgrest query for the stage's pending
    rows (select + filters, no limit). Returns up to `limit` rows spread
    across open sessions.
    """
    try:
        session_ids = _open_session_ids()
    except Exception as exc:
        logger.warning("fair_share: session lookup failed, falling back to plain fetch: %s", exc)
        session_ids = []

    rows: list[dict] = []
    # Rows the caller is already working on count as seen, so they're skipped
    # instead of taking up this tick's budget.
    seen: set = set(exclude_ids or ())
    last_row: dict[str, int] = {}

    # Water-filling: give every open session an equal share; sessions that
    # used their whole share ("hungry") split whatever budget is left over,
    # so idle sessions don't waste it and busy ones progress side by side.
    hungry = list(session_ids)
    for _ in range(4):
        remaining = limit - len(rows)
        if remaining <= 0 or not hungry:
            break
        share = max(1, remaining // len(hungry))
        still_hungry = []
        for sid in hungry:
            if len(rows) >= limit:
                break
            q = build_query().eq("session_id", sid)
            if sid in last_row:
                q = q.gt("row_number", last_row[sid])
            take = min(share, limit - len(rows))
            batch = q.order("row_number").limit(take).execute().data or []
            for r in batch:
                if r["id"] not in seen:
                    seen.add(r["id"])
                    rows.append(r)
            if batch:
                last_row[sid] = batch[-1]["row_number"]
            if len(batch) == take:
                still_hungry.append(sid)
        hungry = still_hungry

    # Top up from anywhere (e.g. rows whose session isn't in the open list).
    if len(rows) < limit:
        extra = build_query().limit(limit).execute().data or []
        for r in extra:
            if len(rows) >= limit:
                break
            if r["id"] not in seen:
                seen.add(r["id"])
                rows.append(r)

    return rows
