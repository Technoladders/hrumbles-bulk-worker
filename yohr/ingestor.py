"""
yohr/ingestor.py
Stage 4 — build hr_talent_pool records from processed rows, bulk upsert,
           sync new skills to skills_master (triggers enrichment pipeline).
"""
import logging
from typing import Optional

from .constants import supabase, YOHR_ORG_ID              # ← .constants not .config

logger = logging.getLogger(__name__)


def run_ingestor() -> None:
    try:
        rows = (
            supabase.table("org_csv_import_rows")
            .select(
                "id, session_id, "
                "raw_name, raw_designation, raw_company, raw_notice, raw_location, "
                "raw_email, raw_linkedin, raw_extra_fields, "
                "parsed_first_name, parsed_last_name, parsed_phone, parsed_linkedin, "
                "stored_resume_path, ai_result"
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

    talent_records = []
    row_id_map: dict[str, dict] = {}
    all_skills: set[str] = set()

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

    ai: dict    = row.get("ai_result") or {}
    skills: list[str] = [s for s in (ai.get("skills") or []) if isinstance(s, str)]
    skills_lower = [s.lower() for s in skills]

    notice: Optional[int] = None
    raw_notice = row.get("raw_notice", "")
    if raw_notice:
        try:
            notice = int(float(raw_notice))
        except (ValueError, TypeError):
            pass

    extra_data: dict = row.get("raw_extra_fields") or {}

    record = {
        "email":              email,
        "organization_id":    YOHR_ORG_ID,
        "full_name":          row.get("raw_name") or None,
        "first_name":         row.get("parsed_first_name") or None,
        "last_name":          row.get("parsed_last_name") or None,
        "phone":              row.get("parsed_phone") or None,
        "linkedin_url":       row.get("parsed_linkedin") or None,
        "current_designation": row.get("raw_designation") or None,
        "current_company":    row.get("raw_company") or None,
        "notice_period":      notice,
        "current_location":   row.get("raw_location") or None,
        "resume_path":        row.get("stored_resume_path") or None,
        "top_skills":         skills,
        "top_skills_lower":   skills_lower,
        "total_exp_years":    _safe_int(ai.get("exp_years")),
        "total_exp_months":   _safe_int(ai.get("exp_months")),
        "qualification":      ai.get("qualification") or None,
        "institution":        ai.get("institution") or None,
        "resume_full_text":   ai.get("profile_text") or None,
        "source":             "yohr_csv_migration",
        "other_details": {
            "source":     "yohr_csv",
            "session_id": row.get("session_id"),
            "csv_row_id": row.get("id"),
            "extra_data": extra_data,
        },
    }
    return record, skills


def _bulk_upsert(talent_records: list[dict], rows: list[dict], row_id_map: dict) -> None:
    row_lookup = {r["id"]: r for r in rows}
    try:
        result = (
            supabase.table("hr_talent_pool")
            .upsert(talent_records, on_conflict="email,organization_id")
            .execute()
        )
        upserted_data      = result.data or []
        email_to_tp_id     = {rec["email"]: rec["id"] for rec in upserted_data if "email" in rec and "id" in rec}

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