"""
yohr/resume_downloader.py
Stage 2 — download PDFs from pyjamahr CDN, upload to talent-pool-resumes bucket.
8 concurrent downloads, max 3 retries per row.
"""
import logging
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests

from .constants import (                                    # ← .constants not .config
    supabase, YOHR_ORG_ID, STORAGE_BUCKET, RESUME_PATH_PREFIX,
    MAX_DOWNLOAD_WORKERS, MAX_DOWNLOAD_RETRIES, DOWNLOAD_TIMEOUT,
)

logger = logging.getLogger(__name__)


def safe_filename(raw_name: str) -> str:
    nfkd      = unicodedata.normalize("NFKD", raw_name or "resume")
    ascii_name = nfkd.encode("ASCII", "ignore").decode("ASCII")
    clean     = re.sub(r"[^\w\-.]", "_", ascii_name)
    return clean or "resume"


def _storage_path(session_id: str, original_url: str) -> str:
    parsed   = urlparse(original_url)
    filename = safe_filename(parsed.path.split("/")[-1])
    if not filename.lower().endswith(".pdf"):
        filename += ".pdf"
    return f"{YOHR_ORG_ID}/{RESUME_PATH_PREFIX}/{session_id}/{filename}"


def run_downloader() -> None:
    try:
        rows = (
            supabase.table("org_csv_import_rows")
            .select("id, session_id, raw_resume_url, s2_attempts")
            .eq("org_id", YOHR_ORG_ID)
            .eq("s1_status", "done")
            .eq("s2_status", "pending")
            .limit(80)
            .execute()
            .data
        )
    except Exception as exc:
        logger.error("downloader: failed to fetch rows: %s", exc)
        return

    if not rows:
        return

    logger.info("downloader: processing %d rows", len(rows))

    with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as pool:
        futures = {pool.submit(_download_row, row): row for row in rows}
        for future in as_completed(futures):
            row = futures[future]
            try:
                future.result()
            except Exception as exc:
                logger.error("downloader: unhandled error for row %s: %s", row["id"], exc)

    session_ids = {r["session_id"] for r in rows}
    for sid in session_ids:
        try:
            supabase.rpc("refresh_csv_session_counts", {"p_session_id": sid}).execute()
        except Exception as exc:
            logger.warning("downloader: refresh counts failed for %s: %s", sid, exc)


def _download_row(row: dict) -> None:
    row_id     = row["id"]
    session_id = row["session_id"]
    url        = row["raw_resume_url"]
    attempts   = row.get("s2_attempts", 0) + 1

    supabase.table("org_csv_import_rows").update(
        {"s2_status": "downloading", "s2_attempts": attempts}
    ).eq("id", row_id).execute()

    try:
        resp = requests.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
        resp.raise_for_status()
        pdf_bytes = resp.content

        if len(pdf_bytes) < 100:
            raise ValueError(f"Response too small ({len(pdf_bytes)} bytes)")

        storage_path = _storage_path(session_id, url)
        supabase.storage.from_(STORAGE_BUCKET).upload(
            path=storage_path,
            file=pdf_bytes,
            file_options={"content-type": "application/pdf", "upsert": "true"},
        )

        supabase.table("org_csv_import_rows").update({
            "s2_status":          "done",
            "stored_resume_path": storage_path,
            "s2_error":           None,
        }).eq("id", row_id).execute()
        logger.debug("downloader: row %s — OK (%d bytes)", row_id, len(pdf_bytes))

    except Exception as exc:
        error_msg  = str(exc)
        new_status = "failed" if attempts >= MAX_DOWNLOAD_RETRIES else "pending"
        logger.warning("downloader: row %s attempt %d failed: %s", row_id, attempts, error_msg)
        supabase.table("org_csv_import_rows").update({
            "s2_status":   new_status,
            "s2_attempts": attempts,
            "s2_error":    error_msg,
        }).eq("id", row_id).execute()