"""
yohr/ingestor.py
Stage 4 — upsert fully-structured AI result into hr_talent_pool.

Key changes vs previous version:
  - Maps ALL AI fields: work_experience, education, projects, certifications,
    suggested_title, total_experience, etc.
  - resume_path stored as FULL PUBLIC URL (not relative storage path)
  - resume_text stores FULL extracted text (from resume_text_excerpt)
  - CSV values (company, designation, notice, location) take priority over AI
    so structured CSV data is never overwritten by a weaker AI parse
"""
import json
import logging
import re
from typing import Any, Optional

from .constants import supabase, YOHR_ORG_ID, ACTIVE_ORG_IDS, STORAGE_PUBLIC_BASE

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_url(storage_path: Optional[str]) -> Optional[str]:
    """Convert relative storage path to full public URL."""
    if not storage_path:
        return None
    if storage_path.startswith("http"):
        return storage_path
    return f"{STORAGE_PUBLIC_BASE}/{storage_path}"


def _json_str(value: Any) -> Optional[str]:
    """Serialise a list/dict to JSON string for TEXT columns, return None if empty."""
    if not value:
        return None
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return None


def _parse_exp_years(total_exp: Optional[str]) -> Optional[int]:
    """Parse '5 years', '12+ years', '5.7 years' → integer."""
    if not total_exp:
        return None
    m = re.search(r'(\d+(?:\.\d+)?)', str(total_exp))
    return int(float(m.group(1))) if m else None


def _safe_str(value: Any) -> Optional[str]:
    s = str(value).strip() if value else None
    return s or None


def _parse_ctc_numeric(value: Any) -> Optional[float]:
    """'3,300,000' / '3300000.50' / '₹33L' (digits only) → 3300000.0. None if unparsable."""
    if value is None:
        return None
    digits = re.sub(r'[^\d.]', '', str(value))
    if not digits:
        return None
    try:
        return float(digits)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_talent_record(row: dict, created_by: Optional[str] = None) -> tuple[Optional[dict], list[str]]:
    """
    Build a single hr_talent_pool upsert record from an org_csv_import_row.
    Returns (record_dict, skill_list) or (None, []) if no email.

    created_by is the org_csv_import_sessions.created_by (the employee who
    uploaded the CSV) — passed in by the caller since it lives on the
    session, not the row.
    """
    email = (row.get("raw_email") or "").strip().lower()
    if not email:
        return None, []

    ai: dict = row.get("ai_result") or {}
    extra: dict = row.get("raw_extra_fields") or {}

    # ── Skills (JSONB arrays) ────────────────────────────────────────────
    skills: list = [s for s in (ai.get("top_skills") or []) if isinstance(s, str)]
    skills_lower = [s.lower() for s in skills]

    # ── Notice period (prefer CSV raw value, fall back to AI) ───────────
    notice_text: Optional[str] = None
    raw_notice = (row.get("raw_notice") or "").strip()
    if raw_notice:
        try:
            days = int(float(raw_notice))
            notice_text = "Immediate" if days == 0 else f"{days} days"
        except (ValueError, TypeError):
            notice_text = raw_notice
    if not notice_text:
        notice_text = _safe_str(ai.get("notice_period"))

    # ── Company / designation: CSV takes priority over AI ───────────────
    current_company     = _safe_str(row.get("raw_company")) or _safe_str(ai.get("current_company"))
    current_designation = _safe_str(row.get("raw_designation")) or _safe_str(ai.get("current_designation"))
    current_location    = _safe_str(row.get("raw_location")) or _safe_str(ai.get("current_location"))

    # ── Resume text: full extracted text ────────────────────────────────
    resume_text = (row.get("resume_text_excerpt") or "").strip()
    if not resume_text:
        # Last resort: build from CSV + AI profile summary
        parts = []
        if row.get("raw_name"):        parts.append(f"Name: {row['raw_name']}")
        if current_designation:        parts.append(f"Title: {current_designation}")
        if current_company:            parts.append(f"Company: {current_company}")
        if current_location:           parts.append(f"Location: {current_location}")
        resume_text = "\n".join(parts) or f"Candidate: {row.get('raw_name', 'Unknown')}"

    # ── Structured fields (TEXT columns, stored as JSON strings) ─────────
    work_experience = _json_str(ai.get("work_experience"))
    education       = _json_str(ai.get("education"))
    projects        = _json_str(ai.get("projects"))
    certifications  = _json_str(ai.get("certifications"))

    # ── Other details JSONB (source metadata + any AI extras) ───────────
    ai_other = ai.get("other_details") or {}
    other_details: dict = {
        "source":      "yohr_csv",
        "session_id":  row.get("session_id"),
        "csv_row_id":  row.get("id"),
    }
    # Preserve raw CSV phone — important when Excel sci notation truncated digits
    # e.g. actual 919766748078 was saved as "9.19767E+11" in CSV
    raw_phone_csv = extra.get("_raw_phone_csv") or row.get("raw_phone")
    if raw_phone_csv:
        other_details["raw_phone"] = raw_phone_csv
    if isinstance(ai_other, dict):
        other_details.update(ai_other)

    # ── Experience years ─────────────────────────────────────────────────
    parsed_exp_years = _parse_exp_years(ai.get("total_experience"))

    record = {
        # Identity
        "email":           email,
        "organization_id": row.get("org_id") or YOHR_ORG_ID,
        "candidate_name":  _safe_str(row.get("raw_name")) or _safe_str(ai.get("candidate_name")),
        "created_by":      created_by,

        # Contact
        # Phone: prefer AI-extracted (from actual PDF — correct even when CSV had sci notation)
        # Fall back to CSV-parsed phone if AI didn't extract one
        "phone":           _safe_str(ai.get("phone")) or row.get("parsed_phone"),
        "linkedin_url":    row.get("parsed_linkedin") or _safe_str(ai.get("linkedin_url")),
        "github_url":      _safe_str(ai.get("github_url")),

        # Professional
        "current_designation": current_designation,
        "current_company":     current_company,
        "current_location":    current_location,
        "notice_period":       notice_text,
        "suggested_title":     _safe_str(ai.get("suggested_title")),
        "total_experience":    _safe_str(ai.get("total_experience")),

        # CTC — CSV-only (AI doesn't extract these); current_ctc/expected_ctc
        # come through raw_extra_fields since they're not fixed raw_* columns
        "current_salary":      _safe_str(extra.get("current_ctc")),
        "expected_salary":     _safe_str(extra.get("expected_ctc")),
        "parsed_current_ctc":  _parse_ctc_numeric(extra.get("current_ctc")),
        "parsed_expected_ctc": _parse_ctc_numeric(extra.get("expected_ctc")),

        # Resume content
        "resume_path":           _to_url(row.get("stored_resume_path")),
        "resume_text":           resume_text,          # full extracted text (NOT NULL)
        "work_experience":       work_experience,
        "education":             education,
        "projects":              projects,
        "certifications":        certifications,

        # Skills (JSONB)
        "top_skills":            skills,
        "top_skills_lower":      skills_lower,

        # Education summary
        "highest_education":     _safe_str(ai.get("highest_education")),

        # Parsed numerics
        "parsed_experience_years": parsed_exp_years,

        # Source / metadata
        "source_platform": "yohr_csv_migration",
        "other_details":   other_details,
    }
    return record, skills


# ---------------------------------------------------------------------------
# Skills master sync
# ---------------------------------------------------------------------------

def _sync_skills_master(skills: set[str]) -> None:
    if not skills:
        return
    try:
        records = [
            {"name": s, "name_lower": s.lower(), "source": "yohr_csv"}
            for s in skills
        ]
        supabase.table("skills_master").upsert(
            records, on_conflict="name_lower", ignore_duplicates=True
        ).execute()
    except Exception as exc:
        logger.warning("ingestor: skills_master sync failed: %s", exc)


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

def _upsert_rows(talent_records: list[dict], rows: list[dict],
                 row_id_map: dict[str, dict]) -> None:
    """Bulk upsert with per-row fallback on failure."""
    try:
        result = (
            supabase.table("hr_talent_pool")
            .upsert(talent_records, on_conflict="email,organization_id")
            .execute()
        )
        upserted     = result.data or []
        email_to_id  = {r["email"]: r["id"] for r in upserted if "email" in r and "id" in r}

        for row_id, rec in row_id_map.items():
            tp_id = email_to_id.get(rec["email"])
            supabase.table("org_csv_import_rows").update({
                "s4_status":      "done",
                "s4_error":       None,
                "talent_pool_id": tp_id,
            }).eq("id", row_id).execute()

        logger.info("ingestor: bulk upsert OK — %d records", len(upserted))

    except Exception as exc:
        logger.warning("ingestor: bulk upsert failed (%s) — falling back to individual", exc)
        for row_id, rec in row_id_map.items():
            try:
                res = (
                    supabase.table("hr_talent_pool")
                    .upsert(rec, on_conflict="email,organization_id")
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
                logger.warning("ingestor: row %s failed: %s", row_id, row_exc)


# ---------------------------------------------------------------------------
# Scheduler entry point
# ---------------------------------------------------------------------------

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
            .in_("org_id", ACTIVE_ORG_IDS)
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

    # created_by lives on the session (the uploader), not the row — fetch it
    # once per distinct session in this batch rather than per row.
    session_ids_in_batch = {r["session_id"] for r in rows}
    session_created_by: dict = {}
    try:
        session_rows = (
            supabase.table("org_csv_import_sessions")
            .select("id, created_by")
            .in_("id", list(session_ids_in_batch))
            .execute()
            .data
        )
        session_created_by = {s["id"]: s.get("created_by") for s in (session_rows or [])}
    except Exception as exc:
        logger.warning("ingestor: failed to fetch session created_by (non-fatal): %s", exc)

    talent_records: list[dict] = []
    row_id_map:     dict       = {}
    all_skills:     set[str]   = set()

    for row in rows:
        created_by = session_created_by.get(row["session_id"])
        record, skills = _build_talent_record(row, created_by=created_by)
        if record:
            talent_records.append(record)
            row_id_map[row["id"]] = record
            all_skills.update(skills)

    if talent_records:
        _upsert_rows(talent_records, rows, row_id_map)
        _sync_skills_master(all_skills)

    session_ids = {r["session_id"] for r in rows}
    for sid in session_ids:
        try:
            supabase.rpc("refresh_csv_session_counts", {"p_session_id": sid}).execute()
        except Exception as exc:
            logger.warning("ingestor: refresh_counts failed for %s: %s", sid, exc)