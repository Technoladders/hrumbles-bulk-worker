"""
yohr/constants.py — YOHR pipeline constants.
Imports shared clients from ROOT config.py.
"""
import os
from config import (  # noqa: F401
    supabase, openai_client, STORAGE_BUCKET, AI_PROVIDER, OLLAMA_BASE_URL,
)

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

# AI model name. Unset → today's production default, unchanged.
#   OpenAI (AI_PROVIDER=openai, default): e.g. "gpt-4.1-nano"
#   Ollama (AI_PROVIDER=ollama, local dev): e.g. "qwen3:1.7b"
AI_MODEL           = os.getenv("AI_MODEL", "gpt-4.1-nano")

# Enough for full structured extraction (work_exp + education + projects + certs)
MAX_AI_TOKENS      = 4096

# Characters sent to AI after compression — covers even 5-page resumes
MAX_AI_INPUT_CHARS = 20_000

# Ollama-only (ignored by OpenAI): context window passed as the request's
# options.num_ctx. MAX_AI_INPUT_CHARS (20,000 chars) is roughly 5,000-7,000
# tokens for English text; add the system prompt (~500 tokens) and the
# MAX_AI_TOKENS output reserve (4,096) and a full request needs on the order
# of 10,000-12,000 tokens of context. Ollama's own default context window is
# commonly 2048-4096, which would silently truncate long resumes. 16384 gives
# comfortable headroom over that budget without being wastefully large for a
# 1.7B model's KV-cache footprint. Override via OLLAMA_NUM_CTX if needed.
OLLAMA_NUM_CTX       = int(os.getenv("OLLAMA_NUM_CTX", "16384"))

# Concurrent AI requests. Production keeps today's default (5) unless
# overridden; for local Ollama, set MAX_AI_WORKERS=1 or 2 so a handful of
# simultaneous local model requests don't overwhelm the machine.
MAX_AI_WORKERS       = int(os.getenv("MAX_AI_WORKERS", "5"))
MAX_DOWNLOAD_WORKERS = 8
MAX_DOWNLOAD_RETRIES = 3
DOWNLOAD_TIMEOUT     = 30
MAX_AI_RETRIES       = 2

# Public URL base — used to build full downloadable resume links
_SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
STORAGE_PUBLIC_BASE = f"{_SUPABASE_URL}/storage/v1/object/public/{STORAGE_BUCKET}"