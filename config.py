import os
import logging
from supabase import create_client, Client
from openai import OpenAI
from redis import Redis
from rq import Queue
from dotenv import load_dotenv

load_dotenv()

# ── Required env vars ─────────────────────────────────────────────────────────
SUPABASE_URL        = os.environ['SUPABASE_URL']
SUPABASE_SERVICE_KEY = os.environ['SUPABASE_SERVICE_KEY']
OPENAI_API_KEY      = os.environ['OPENAI_API_KEY']

# Dedicated OpenAI key for the whole YOHR AI pipeline (yohr/ai_processor.py's
# normal path + yohr/ai_backfill.py) — deliberately a SECOND, independent
# client (yohr_ai_client below), never a reconfiguration of the shared
# openai_client: bulk_tasks.py imports that shared client for its own
# unrelated OpenAI Batch API calls (files.create/batches.create/etc.), which
# would silently break if openai_client were ever repointed at a different
# key/provider for YOHR's sake.
#
# Required in production: if unset, YOHR's cost/rate-limit isolation from the
# rest of this backend is not real (both clients would silently share the one
# OPENAI_API_KEY), so this fails loudly at startup instead of running that
# way unnoticed. For local development only, set YOHR_ALLOW_SHARED_KEY=true
# to explicitly opt into the fallback. Never log the key value itself.
YOHR_OPENAI_API_KEY   = os.getenv('YOHR_OPENAI_API_KEY', '').strip()
YOHR_ALLOW_SHARED_KEY = os.getenv('YOHR_ALLOW_SHARED_KEY', '').strip().lower() in ('1', 'true', 'yes')

if not YOHR_OPENAI_API_KEY and not YOHR_ALLOW_SHARED_KEY:
    raise RuntimeError(
        "YOHR_OPENAI_API_KEY is not set. The YOHR AI pipeline must use its own "
        "dedicated OpenAI key, independent of OPENAI_API_KEY, so its cost and "
        "rate limits are isolated from the rest of this backend. Set "
        "YOHR_OPENAI_API_KEY in the environment, or set "
        "YOHR_ALLOW_SHARED_KEY=true to explicitly opt into sharing "
        "OPENAI_API_KEY (local development only — do not set this in "
        "production)."
    )

# ── Optional env vars with defaults ──────────────────────────────────────────
REDIS_HOST          = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT          = int(os.getenv('REDIS_PORT', 6379))
STORAGE_BUCKET      = os.getenv('STORAGE_BUCKET', 'talent-pool-resumes')
STORAGE_BULK_PREFIX = os.getenv('STORAGE_BULK_PREFIX', 'bulk')
RESUME_PARSER_URL   = os.getenv('RESUME_PARSER_URL', 'http://resume-parser-container:5005')
PORT                = int(os.getenv('PORT', 5010))

BULK_QUEUE_NAME     = 'bulk-pipeline'

# ── Clients (module-level singletons) ────────────────────────────────────────
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
openai_client    = OpenAI(api_key=OPENAI_API_KEY)
yohr_ai_client   = OpenAI(api_key=YOHR_OPENAI_API_KEY or OPENAI_API_KEY)  # falls back only when YOHR_ALLOW_SHARED_KEY=true (checked above)
redis_conn       = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
bulk_queue       = Queue(BULK_QUEUE_NAME, connection=redis_conn)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
)