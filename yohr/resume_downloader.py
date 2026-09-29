"""
yohr/resume_downloader.py
Stage 2 — download PDFs from pyjamahr CDN, upload to talent-pool-resumes bucket.
8 concurrent downloads, max 3 retries per row.
"""
import logging
import threading
import time
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

import requests

from .fair_share import fetch_fair_share
from .constants import (
    supabase, ACTIVE_ORG_IDS, STORAGE_BUCKET, RESUME_PATH_PREFIX,
    MAX_DOWNLOAD_WORKERS, MAX_DOWNLOAD_RETRIES, DOWNLOAD_TIMEOUT,
    MAX_RESUME_BYTES, DOWNLOAD_ROWS_PER_TICK, DOWNLOAD_TOTAL_TIMEOUT,
)

logger = logging.getLogger(__name__)


def safe_filename(raw_name: str) -> str:
    nfkd      = unicodedata.normalize("NFKD", raw_name or "resume")
    ascii_name = nfkd.encode("ASCII", "ignore").decode("ASCII")
    clean     = re.sub(r"[^\w\-.]", "_", ascii_name)
    return clean or "resume"


def _storage_path(org_id: str, session_id: str, original_url: str) -> str:
    parsed   = urlparse(original_url)
    filename = safe_filename(parsed.path.split("/")[-1])
    if not filename.lower().endswith(".pdf"):
        filename += ".pdf"
    return f"{org_id}/{RESUME_PATH_PREFIX}/{session_id}/{filename}"


def run_downloader() -> None:
    # ── Reset rows stuck in "downloading" for >10 min (handles worker restarts) ──
    # NOTE: this previously checked s2_status == "processing", a value
    # _download_row never actually sets (it sets "downloading") and that
    # isn't even in org_csv_import_rows' s2_status CHECK constraint — so this
    # reset never matched anything. Fixed to check the real in-progress value.
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        reset = (
            supabase.table("org_csv_import_rows")
            .update({"s2_status": "pending", "s2_error": "auto-reset: stuck in downloading"})
            .in_("org_id", ACTIVE_ORG_IDS)
            .eq("s2_status", "downloading")
            .lt("updated_at", cutoff)
            .execute()
        )
        if reset.data:
            logger.info("downloader: reset %d stuck rows → pending", len(reset.data))
    except Exception as exc:
        logger.warning("downloader: stuck-row reset failed (non-fatal): %s", exc)

    try:
        rows = fetch_fair_share(
            lambda: (
                supabase.table("org_csv_import_rows")
                .select("id, session_id, row_number, org_id, raw_resume_url, s2_attempts")
                .in_("org_id", ACTIVE_ORG_IDS)
                .eq("s1_status", "done")
                .eq("s2_status", "pending")
            ),
            limit=DOWNLOAD_ROWS_PER_TICK,
            exclude_ids=_in_flight,
        )
    except Exception as exc:
        logger.error("downloader: failed to fetch rows: %s", exc)
        return

    if not rows:
        return

    _submit(rows)


# ── Background pool ───────────────────────────────────────────────────────────
# Downloads run on a long-lived pool and each tick returns immediately. Before,
# every tick waited for ALL of its downloads, so one slow/hung URL froze the
# whole pipeline for minutes (bursts of progress, then nothing). Now a hung
# download only occupies its own worker slot.
_thread_local = threading.local()


def _http() -> requests.Session:
    """Per-thread keep-alive session: resumes all come from the same few hosts,
    so reusing connections skips a TCP + TLS handshake on every download."""
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=4)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _thread_local.session = s
    return s


_pool = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS, thread_name_prefix="yohr-dl")
_in_flight: set[str] = set()           # row ids queued or running
_touched_sessions: set[str] = set()    # sessions with rows finished since last refresh
_lock = threading.Lock()
# Queue at most one tick's worth of rows ahead of the workers: enough to keep
# all workers busy between 15 s ticks, but bounded so a stall can't let the
# backlog grow forever. Queued rows are small dicts; only running downloads
# hold resume bytes (<= MAX_RESUME_BYTES each).
MAX_IN_FLIGHT = DOWNLOAD_ROWS_PER_TICK


def _submit(rows: list[dict]) -> None:
    _refresh_touched_sessions()
    with _lock:
        free = MAX_IN_FLIGHT - len(_in_flight)
        fresh = [r for r in rows if r["id"] not in _in_flight][:max(0, free)]
        for r in fresh:
            _in_flight.add(r["id"])
    if fresh:
        logger.info("downloader: queued %d rows (%d in flight)", len(fresh), len(_in_flight))
    for r in fresh:
        _pool.submit(_run_one, r)


def _run_one(row: dict) -> None:
    try:
        _download_row(row)
    except Exception as exc:
        logger.error("downloader: unhandled error for row %s: %s", row["id"], exc)
    finally:
        with _lock:
            _in_flight.discard(row["id"])
            _touched_sessions.add(row["session_id"])


def _refresh_touched_sessions() -> None:
    with _lock:
        sids = list(_touched_sessions)
        _touched_sessions.clear()
    for sid in sids:
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
        resp = _http().get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
        resp.raise_for_status()
        declared = int(resp.headers.get("Content-Length") or 0)
        if declared > MAX_RESUME_BYTES:
            raise ValueError(f"Resume too large ({declared} bytes > {MAX_RESUME_BYTES})")
        chunks, size = [], 0
        deadline = time.monotonic() + DOWNLOAD_TOTAL_TIMEOUT
        for chunk in resp.iter_content(chunk_size=64 * 1024):
            if time.monotonic() > deadline:
                raise TimeoutError(f"Download exceeded {DOWNLOAD_TOTAL_TIMEOUT}s")
            size += len(chunk)
            if size > MAX_RESUME_BYTES:
                raise ValueError(f"Resume too large (> {MAX_RESUME_BYTES} bytes)")
            chunks.append(chunk)
        pdf_bytes = b"".join(chunks)

        if len(pdf_bytes) < 100:
            raise ValueError(f"Response too small ({len(pdf_bytes)} bytes)")

        org_id = row.get("org_id", "")
        storage_path = _storage_path(org_id, session_id, url)
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