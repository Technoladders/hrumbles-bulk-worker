"""
yohr/ai_processor.py
Stage 3 — extract resume text, call gpt-4.1-nano for structured fields + profile_text.
5 concurrent live API calls. Max 2 retries per row.
"""
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from .constants import (                                    # ← .constants not .config
    supabase, openai_client, YOHR_ORG_ID,
    STORAGE_BUCKET,
    OPENAI_MODEL, MAX_AI_TOKENS, RESUME_TEXT_LIMIT,
    MAX_AI_WORKERS, MAX_AI_RETRIES,
)

logger = logging.getLogger(__name__)

# ── Prompt ─────────────────────────────────────────────────────────────────────
AI_PROMPT = """Extract from the resume. Return ONLY valid JSON, no preamble, no markdown.

{{
  "skills": [],
  "exp_years": null,
  "exp_months": null,
  "qualification": null,
  "institution": null,
  "profile_text": null
}}

Rules:
- skills: array of all technical and professional skills, no duplicates
- exp_years: total experience as integer years (null if unclear)
- exp_months: remaining months 0-11 (null if unclear)
- qualification: highest degree name exactly as written (e.g. "B.Tech Computer Science")
- institution: university/college name for that degree
- profile_text: structured facts ONLY, NO adjectives. Use this exact format:
  {{designation}} | {{company}} | {{location}} | {{exp_years}}Y exp
  Skills: {{top 12 skills comma-separated}}
  Companies: {{company1}} (YYYY-YYYY), {{company2}} (YYYY-present)
  Education: {{qualification}}, {{institution}} (YYYY)

Resume:
{resume_text}"""

EMPTY_AI_RESULT = {
    "skills": [], "exp_years": None, "exp_months": None,
    "qualification": None, "institution": None, "profile_text": None,
}


def run_ai_processor() -> None:
    try:
        rows = (
            supabase.table("org_csv_import_rows")
            .select(
                "id, session_id, stored_resume_path, raw_name, raw_designation, "
                "raw_company, raw_location, s3_attempts, resume_text_excerpt"
            )
            .eq("org_id", YOHR_ORG_ID)
            .in_("s2_status", ["done", "skipped"])
            .eq("s3_status", "pending")
            .limit(50)
            .execute()
            .data
        )
    except Exception as exc:
        logger.error("ai_processor: fetch failed: %s", exc)
        return

    if not rows:
        return

    logger.info("ai_processor: processing %d rows", len(rows))

    with ThreadPoolExecutor(max_workers=MAX_AI_WORKERS) as pool:
        futures = {pool.submit(_process_row, row): row for row in rows}
        for future in as_completed(futures):
            row = futures[future]
            try:
                future.result()
            except Exception as exc:
                logger.error("ai_processor: unhandled error for row %s: %s", row["id"], exc)

    session_ids = {r["session_id"] for r in rows}
    for sid in session_ids:
        try:
            supabase.rpc("refresh_csv_session_counts", {"p_session_id": sid}).execute()
        except Exception as exc:
            logger.warning("ai_processor: refresh counts failed for %s: %s", sid, exc)


def _process_row(row: dict) -> None:
    row_id   = row["id"]
    attempts = row.get("s3_attempts", 0) + 1

    supabase.table("org_csv_import_rows").update(
        {"s3_status": "processing", "s3_attempts": attempts}
    ).eq("id", row_id).execute()

    try:
        resume_text = row.get("resume_text_excerpt")  # reuse on retry
        if not resume_text and row.get("stored_resume_path"):
            resume_text = _extract_text(row["stored_resume_path"])

        if not resume_text:
            resume_text = _minimal_context(row)

        truncated  = resume_text[:RESUME_TEXT_LIMIT]
        ai_result  = _call_ai(truncated)

        if not ai_result.get("profile_text"):
            ai_result["profile_text"] = _fallback_profile_text(row, ai_result)

        supabase.table("org_csv_import_rows").update({
            "s3_status":           "done",
            "s3_error":            None,
            "resume_text_excerpt": truncated,
            "ai_result":           ai_result,
        }).eq("id", row_id).execute()
        logger.debug("ai_processor: row %s done — %d skills", row_id, len(ai_result.get("skills", [])))

    except Exception as exc:
        error_msg  = str(exc)
        new_status = "failed" if attempts >= MAX_AI_RETRIES else "pending"
        logger.warning("ai_processor: row %s attempt %d failed: %s", row_id, attempts, error_msg)
        supabase.table("org_csv_import_rows").update({
            "s3_status":   new_status,
            "s3_attempts": attempts,
            "s3_error":    error_msg,
        }).eq("id", row_id).execute()


def _extract_text(storage_path: str) -> str:
    """Download PDF from storage, extract text locally using pypdf + pdfminer fallback.
    NO HTTP call — both libraries are already installed in the container."""
    import io

    pdf_bytes: bytes = supabase.storage.from_(STORAGE_BUCKET).download(storage_path)

    # Try pypdf first (faster)
    try:
        import pypdf
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for page in reader.pages:
            t = page.extract_text()
            if t:
                pages.append(t)
        text = "\n".join(pages).strip()
        if text:
            return text
    except Exception:
        pass

    # Fallback: pdfminer.six (better for complex PDFs)
    try:
        from pdfminer.high_level import extract_text as pm_extract
        return pm_extract(io.BytesIO(pdf_bytes)) or ""
    except Exception:
        return ""


def _call_ai(resume_text: str) -> dict:
    prompt = AI_PROMPT.format(resume_text=resume_text)
    response = openai_client.chat.completions.create(
        model=OPENAI_MODEL,
        max_tokens=MAX_AI_TOKENS,
        temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )
    raw_content = response.choices[0].message.content or ""
    clean = raw_content.strip()
    if clean.startswith("```"):
        clean = clean.split("```")[1]
        if clean.startswith("json"):
            clean = clean[4:]

    try:
        parsed = json.loads(clean)
    except json.JSONDecodeError as exc:
        raise ValueError(f"AI returned non-JSON: {raw_content[:200]}") from exc

    result          = {**EMPTY_AI_RESULT, **parsed}
    result["skills"] = [s for s in (result.get("skills") or []) if isinstance(s, str)]
    return result


def _minimal_context(row: dict) -> str:
    lines = []
    if row.get("raw_name"):        lines.append(f"Name: {row['raw_name']}")
    if row.get("raw_designation"): lines.append(f"Title: {row['raw_designation']}")
    if row.get("raw_company"):     lines.append(f"Company: {row['raw_company']}")
    if row.get("raw_location"):    lines.append(f"Location: {row['raw_location']}")
    return "\n".join(lines)


def _fallback_profile_text(row: dict, ai: dict) -> str:
    parts = []
    header_parts = [p for p in [
        row.get("raw_designation"), row.get("raw_company"), row.get("raw_location")
    ] if p]
    if ai.get("exp_years") is not None:
        header_parts.append(f"{ai['exp_years']}Y exp")
    if header_parts:
        parts.append(" | ".join(header_parts))
    skills = ai.get("skills") or []
    if skills:
        parts.append(f"Skills: {', '.join(skills[:12])}")
    edu = ", ".join(filter(None, [ai.get("qualification"), ai.get("institution")]))
    if edu:
        parts.append(f"Education: {edu}")
    return "\n".join(parts) or None