"""
yohr/constants.py — YOHR pipeline constants.
Imports shared clients from ROOT config.py.
"""
import os
from config import supabase, openai_client, yohr_ai_client, STORAGE_BUCKET  # noqa: F401

YOHR_ORG_ID    = "2e569073-86de-4199-9d36-99dfe4d2e8f6"
# Demo org used for testing — remove from this list when going live
DEMO_ORG_ID    = "53989f03-bdc9-439a-901c-45b274eff506"

# Orgs this YOHR pipeline instance should process, across all four stages
# (csv_parser, resume_downloader, ai_processor, ingestor all filter on this
# one list). Override with YOHR_ACTIVE_ORG_IDS (comma-separated org UUIDs) —
# e.g. to temporarily isolate one org's processing to a single instance
# during a migration. Unset/empty → today's default, unchanged (both orgs).
# This only affects the YOHR pipeline; it has no effect on any other
# pipeline/org handling elsewhere in this backend (verified: nothing outside
# the yohr/ package imports this constant).
_active_org_ids_env = os.getenv("YOHR_ACTIVE_ORG_IDS", "").strip()
if _active_org_ids_env:
    ACTIVE_ORG_IDS = [org_id.strip() for org_id in _active_org_ids_env.split(",") if org_id.strip()]
else:
    ACTIVE_ORG_IDS = [YOHR_ORG_ID, DEMO_ORG_ID]

RESUME_PATH_PREFIX = "yohr-csv"

# Upper bound on how many NEW CSV rows csv_parser.py will read+insert for a
# single session in one 30s tick. Keeps a single huge file (e.g. 500k rows)
# from building the whole thing into memory at once or blocking one tick for
# many minutes -- large files are instead processed across many ticks,
# resuming from however many rows already exist for that session. See
# yohr/csv_parser.py's _process_session and the s1_complete column.
MAX_CSV_ROWS_PER_TICK = 20_000

OPENAI_MODEL       = "gpt-4.1-nano"

# Enough for full structured extraction (work_exp + education + projects + certs)
MAX_AI_TOKENS      = 4096

# Characters sent to AI after compression — covers even 5-page resumes
MAX_AI_INPUT_CHARS = 20_000

# Lowered from 5/8: this container is capped at 512MiB total, shared with
# the separate RQ worker process (bulk_tasks.py) and this same process's
# other concurrent stages (S2 downloads, ai_backfill). Fewer concurrent
# threads here reduces this process's peak share of that shared ceiling.
MAX_AI_WORKERS       = 2
# Downloads are network-bound and hold at most one capped resume in memory
# each (MAX_RESUME_BYTES), so 8 threads peak at ~80 MB worst case, typically
# ~10 MB. The Sep-28 OOM kills were traced to the rq process, not S2.
MAX_DOWNLOAD_WORKERS = int(os.getenv("MAX_DOWNLOAD_WORKERS", "8"))
MAX_RESUME_BYTES     = 10 * 1024 * 1024
# Rows per scheduler tick (S2 every 15 s, S4 every 15 s). S4 was the hidden
# throughput ceiling at 60/tick (~240 rows/min) regardless of S2 speed.
DOWNLOAD_ROWS_PER_TICK = int(os.getenv("DOWNLOAD_ROWS_PER_TICK", "160"))
INGEST_ROWS_PER_TICK   = int(os.getenv("INGEST_ROWS_PER_TICK", "150"))
MAX_DOWNLOAD_RETRIES = 3
DOWNLOAD_TIMEOUT     = 30
MAX_AI_RETRIES       = 2

# Public URL base — used to build full downloadable resume links
_SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
STORAGE_PUBLIC_BASE = f"{_SUPABASE_URL}/storage/v1/object/public/{STORAGE_BUCKET}"