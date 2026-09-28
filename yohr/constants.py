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
MAX_DOWNLOAD_WORKERS = 4
MAX_DOWNLOAD_RETRIES = 3
DOWNLOAD_TIMEOUT     = 30
MAX_AI_RETRIES       = 2

# Public URL base — used to build full downloadable resume links
_SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
STORAGE_PUBLIC_BASE = f"{_SUPABASE_URL}/storage/v1/object/public/{STORAGE_BUCKET}"