"""
yohr/constants.py
YOHR-specific constants.
Imports shared clients (supabase, openai_client) from the ROOT config.py
so we reuse existing connections instead of creating duplicates.

RENAMED from yohr/config.py to avoid shadowing the root config.py module.
"""
# ── Import shared singletons from root config ─────────────────────────────────
# The root config.py already reads SUPABASE_SERVICE_KEY / OPENAI_API_KEY
# and builds the clients — reuse them.
from config import supabase, openai_client, RESUME_PARSER_URL, STORAGE_BUCKET  # noqa: F401

# ── YOHR-specific constants ───────────────────────────────────────────────────
YOHR_ORG_ID          = "2e569073-86de-4199-9d36-99dfe4d2e8f6"
RESUME_PATH_PREFIX   = "yohr-csv"       # stored as {org_id}/yohr-csv/{session}/{file}

OPENAI_MODEL         = "gpt-4.1-nano"
MAX_AI_TOKENS        = 380
RESUME_TEXT_LIMIT    = 3000             # chars sent to AI per resume
MAX_AI_WORKERS       = 5               # concurrent live API calls

MAX_DOWNLOAD_WORKERS = 8
MAX_DOWNLOAD_RETRIES = 3
DOWNLOAD_TIMEOUT     = 30              # seconds per download

MAX_AI_RETRIES       = 2