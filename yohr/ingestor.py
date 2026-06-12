"""
yohr/ingestor.py
Stage 4 — upsert processed rows into hr_talent_pool.

FIXED vs previous version:
  - Correct hr_talent_pool column names (no first_name/last_name/full_name/
    resume_full_text/total_exp_years/qualification/institution — none exist)
  - candidate_name  ← raw_name  (full name in one field)
  - resume_text     ← ai profile_text  (column is NOT NULL — required)
  - highest_education ← ai qualification + institution
  - notice_period   ← text "X days"
  - Removed all invented columns that caused PGRST204 errors
"""
import logging
from typing import Optional

from .constants import supabase, YOHR_ORG_ID

logger = logging.getLogger(__name__)


def run_ingestor() -> None:
    try:
        rows = (
            supabase.table("org_csv_import_rows")
            .select(
                "id, session_id, "
                "raw_name, raw_designation, raw_company, raw_notice, raw_location, "
                "raw_email, raw_linkedin, raw_extra_fields, "
                "parsed_phone, parsed_linkedin, "
                "stored_resume_path, ai_result, resume_text_excerpt"
            )
            .eq("org_id", YOHR_ORG_ID)
            .in_("s3_status", ["done", "skipped"])
            .eq("s4_status", "pending")
            .limit(60)
            .execute()
            .data
        )
    except Exception as exc:
        logger.error("ingestor: fetch failed: %s", exc)
        return

    if not rows:
        return

    logger.info("ingestor: upserting %d rows", len(rows))

    talent_records: list[dict]   = []
    row_id_map:     dict[str, dict] = {}
    all_skills:     set[str]     = set()

    for row in rows:
        record, skills = _build_talent_record(row)
        if record:
            talent_records.append(record)
            row_id_map[row["id"]] = record
            all_skills.update(skills)

    if talent_records:
        _bulk_upsert(talent_records, rows, row_id_map)
        _sync_skills_master(all_skills)

    session_ids = {r["session_id"] for r in rows}
    for sid in session_ids:
        try:
            supabase.rpc("refresh_csv_session_counts", {"p_session_id": sid}).execute()
        except Exception as exc:
            logger.warning("ingestor: refresh counts failed for %s: %s", sid, exc)


def _build_talent_record(row: dict) -> tuple[Optional[dict], list[str]]:
    email = (row.get("raw_email") or "").strip().lower()
    if not email:
        return None, []

    ai: dict      = row.get("ai_result") or {}
    skills: list  = [s for s in (ai.get("skills") or []) if isinstance(s, str)]
    skills_lower  = [s.lower() for s in skills]

    # notice_period is TEXT in hr_talent_pool — store as "X days"
    notice_text: Optional[str] = None
    raw_notice = (row.get("raw_notice") or "").strip()
    if raw_notice:
        try:
            days = int(float(raw_notice))
            notice_text = f"{days} days" if days > 0 else "Immediate"
        except (ValueError, TypeError):
            notice_text = raw_notice  # keep raw if not numeric

    extra_data: dict = row.get("raw_extra_fields") or {}

    # resume_text is NOT NULL in hr_talent_pool — must always provide a value
    resume_text = _build_resume_text(row, ai)

    record = {
        # ── Core identity ────────────────────────────────────────────────────
        "email":           email,
        "organization_id": YOHR_ORG_ID,
        "candidate_name":  (row.get("raw_name") or "").strip() or None,  # full name

        # ── Contact ──────────────────────────────────────────────────────────
        "phone":           row.get("parsed_phone") or None,
        "linkedin_url":    row.get("parsed_linkedin") or None,

        # ── Professional ─────────────────────────────────────────────────────
        "current_designation": row.get("raw_designation") or None,
        "current_company":     row.get("raw_company") or None,
        "current_location":    row.get("raw_location") or None,
        "notice_period":       notice_text,

        # ── Resume / skills ──────────────────────────────────────────────────
        "resume_path":           row.get("stored_resume_path") or None,
        "resume_text":           resume_text,        # NOT NULL — always populated
        "professional_summary":  ai.get("profile_text") or None,
        "top_skills":            skills,
        "top_skills_lower":      skills_lower,

        # ── Parsed numerics ──────────────────────────────────────────────────
        "parsed_experience_years": _safe_int(ai.get("exp_years")),

        # ── Education — hr_talent_pool has highest_education (text) ──────────
        "highest_education": _build_education(ai),

        # ── Source ───────────────────────────────────────────────────────────
        "source_platform": "yohr_csv_migration",

        # ── Extra metadata in JSONB ──────────────────────────────────────────
        "other_details": {
            "source":      "yohr_csv",
            "session_id":  row.get("session_id"),
            "csv_row_id":  row.get("id"),
            "institution": ai.get("institution"),
            "exp_months":  ai.get("exp_months"),
            "extra_data":  extra_data,
        },
    }
    return record, skills


def _build_resume_text(row: dict, ai: dict) -> str:
    """
    Build resume_text for the NOT NULL constraint.
    Priority: AI profile_text → resume_text_excerpt → minimal CSV fields.
    """
    if ai.get("profile_text"):
        return ai["profile_text"]

    if row.get("resume_text_excerpt"):
        return row["resume_text_excerpt"][:2000]

    # Last resort: construct from raw CSV fields
    parts = []
    if row.get("raw_name"):        parts.append(f"Name: {row['raw_name']}")
    if row.get("raw_designation"): parts.append(f"Title: {row['raw_designation']}")
    if row.get("raw_company"):     parts.append(f"Company: {row['raw_company']}")
    if row.get("raw_location"):    parts.append(f"Location: {row['raw_location']}")
    skills = [s for s in (ai.get("skills") or []) if isinstance(s, str)]
    if skills:                     parts.append(f"Skills: {', '.join(skills[:10])}")
    return "\n".join(parts) or f"Candidate: {row.get('raw_name', 'Unknown')}"


def _build_education(ai: dict) -> Optional[str]:
    """Combine qualification + institution into the highest_education text field."""
    parts = [p for p in [ai.get("qualification"), ai.get("institution")] if p]
    return " — ".join(parts) if parts else None


def _bulk_upsert(talent_records: list[dict], rows: list[dict], row_id_map: dict) -> None:
    try:
        result = (
            supabase.table("hr_talent_pool")
            .upsert(talent_records, on_conflict="email,organization_id")
            .execute()
        )
        upserted_data  = result.data or []
        email_to_tp_id = {rec["email"]: rec["id"] for rec in upserted_data if "email" in rec and "id" in rec}

        for row_id, talent_record in row_id_map.items():
            tp_id = email_to_tp_id.get(talent_record["email"])
            supabase.table("org_csv_import_rows").update({
                "s4_status":      "done",
                "s4_error":       None,
                "talent_pool_id": tp_id,
            }).eq("id", row_id).execute()

        logger.info("ingestor: bulk upsert OK — %d records", len(upserted_data))

    except Exception as exc:
        logger.warning("ingestor: bulk upsert failed (%s) — falling back to individual", exc)
        for row_id, talent_record in row_id_map.items():
            try:
                res = (
                    supabase.table("hr_talent_pool")
                    .upsert(talent_record, on_conflict="email,organization_id")
                    .execute()
                )
                tp_id = res.data[0]["id"] if res.data else None
                supabase.table("org_csv_import_rows").update({
                    "s4_status":      "done",
                    "s4_error":       None,
                    "talent_pool_id": tp_id,
                }).eq("id", row_id).execute()
            except Exception as row_exc:
                supabase.table("org_csv_import_rows").update({
                    "s4_status": "failed",
                    "s4_error":  str(row_exc),
                }).eq("id", row_id).execute()
                logger.warning("ingestor: row %s upsert failed: %s", row_id, row_exc)


def _sync_skills_master(skills: set[str]) -> None:
    if not skills:
        return
    try:
        records = [{"name": s, "name_lower": s.lower(), "source": "yohr_csv"} for s in skills]
        supabase.table("skills_master").upsert(
            records, on_conflict="name_lower", ignore_duplicates=True
        ).execute()
    except Exception as exc:
        logger.warning("ingestor: skills_master sync failed: %s", exc)


def _safe_int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None