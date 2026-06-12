"""
yohr/ai_processor.py
Stage 3 — extract resume text, call gpt-4.1-nano for structured fields.
Fixes vs previous version:
  - MAX_AI_TOKENS raised to 700 (was 380; caused truncated JSON on skill-heavy resumes)
  - Prompt limits skills to top 20 to keep response smaller
  - Null-byte sanitization before writing to DB (prevents 22P05 errors on some PDFs)
"""
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from .constants import (
    supabase, openai_client, YOHR_ORG_ID,
    STORAGE_BUCKET, RESUME_PARSER_URL,
    OPENAI_MODEL, MAX_AI_TOKENS, RESUME_TEXT_LIMIT,
    MAX_AI_WORKERS, MAX_AI_RETRIES,
)

logger = logging.getLogger(__name__)

AI_PROMPT = """Extract from the resume below. Return ONLY valid JSON — no markdown, no preamble.

{{
  "skills": [],
  "exp_years": null,
  "exp_months": null,
  "qualification": null,
  "institution": null,
  "profile_text": null
}}

Rules:
- skills: TOP 20 technical/professional skills only, no duplicates, no soft skills
- exp_years: total experience as integer years (null if unclear)
- exp_months: remaining months 0-11 (null if unclear)
- qualification: highest degree exactly as written
- institution: university/college for that degree
- profile_text: one compact paragraph — designation, company, location, years exp, top 8 skills

Resume:
{resume_text}"""

EMPTY_AI_RESULT = {
    "skills": [], "exp_years": None, "exp_months": None,
    "qualification": None, "institution": None, "profile_text": None,
}


def _sanitize(text: str) -> str:
    """Strip null bytes that PostgreSQL rejects (error 22P05)."""
    return text.replace('\x00', '').replace('\u0000', '')


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
        resume_text = row.get("resume_text_excerpt")
        if not resume_text and row.get("stored_resume_path"):
            resume_text = _extract_text(row["stored_resume_path"])

        if not resume_text:
            resume_text = _minimal_context(row)

        # Sanitize null bytes before any DB write
        resume_text = _sanitize(resume_text)
        truncated   = resume_text[:RESUME_TEXT_LIMIT]

        ai_result = _call_ai(truncated)

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
    """Download PDF bytes from storage, extract text locally using pypdf + pdfminer fallback."""
    import io

    pdf_bytes: bytes = supabase.storage.from_(STORAGE_BUCKET).download(storage_path)

    # Try pypdf first (fast)
    try:
        import pypdf
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        pages  = [page.extract_text() for page in reader.pages if page.extract_text()]
        text   = "\n".join(pages).strip()
        if text:
            return text
    except Exception:
        pass

    # Fallback: pdfminer.six (better for complex layouts)
    try:
        from pdfminer.high_level import extract_text as pm_extract
        return pm_extract(io.BytesIO(pdf_bytes)) or ""
    except Exception:
        return ""


def _call_ai(resume_text: str) -> dict:
    prompt   = AI_PROMPT.format(resume_text=resume_text)
    response = openai_client.chat.completions.create(
        model=OPENAI_MODEL,
        max_tokens=MAX_AI_TOKENS,
        temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )
    raw_content = response.choices[0].message.content or ""
    clean = raw_content.strip()
    # Strip optional markdown code fences
    if clean.startswith("```"):
        clean = clean.split("```")[1]
        if clean.startswith("json"):
            clean = clean[4:]
    clean = clean.strip()

    try:
        parsed = json.loads(clean)
    except json.JSONDecodeError as exc:
        raise ValueError(f"AI returned non-JSON: {raw_content[:300]}") from exc

    result           = {**EMPTY_AI_RESULT, **parsed}
    result["skills"] = [s for s in (result.get("skills") or []) if isinstance(s, str)][:20]
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
    header = " | ".join(filter(None, [
        row.get("raw_designation"),
        row.get("raw_company"),
        row.get("raw_location"),
        f"{ai['exp_years']}Y exp" if ai.get("exp_years") is not None else None,
    ]))
    if header:
        parts.append(header)
    skills = ai.get("skills") or []
    if skills:
        parts.append(f"Skills: {', '.join(skills[:8])}")
    edu = " - ".join(filter(None, [ai.get("qualification"), ai.get("institution")]))
    if edu:
        parts.append(f"Education: {edu}")
    return "\n".join(parts) or None