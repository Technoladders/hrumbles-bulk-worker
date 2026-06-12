"""
yohr/constants.py — YOHR pipeline constants.
Imports shared clients from ROOT config.py.
"""
import os
from config import supabase, openai_client, STORAGE_BUCKET  # noqa: F401

YOHR_ORG_ID        = "53989f03-bdc9-439a-901c-45b274eff506"
RESUME_PATH_PREFIX = "yohr-csv"

OPENAI_MODEL       = "gpt-4.1-nano"

# Enough for full structured extraction (work_exp + education + projects + certs)
MAX_AI_TOKENS      = 4096

# Characters sent to AI after compression — covers even 5-page resumes
MAX_AI_INPUT_CHARS = 20_000

MAX_AI_WORKERS       = 5
MAX_DOWNLOAD_WORKERS = 8
MAX_DOWNLOAD_RETRIES = 3
DOWNLOAD_TIMEOUT     = 30
MAX_AI_RETRIES       = 2

# Public URL base — used to build full downloadable resume links
_SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
STORAGE_PUBLIC_BASE = f"{_SUPABASE_URL}/storage/v1/object/public/{STORAGE_BUCKET}"