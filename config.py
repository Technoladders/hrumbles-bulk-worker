import os
import logging
from supabase import create_client, Client
from openai import OpenAI
from redis import Redis
from rq import Queue
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
)
_logger = logging.getLogger(__name__)

# ── Required env vars ─────────────────────────────────────────────────────────
SUPABASE_URL        = os.environ['SUPABASE_URL']
SUPABASE_SERVICE_KEY = os.environ['SUPABASE_SERVICE_KEY']

# ── AI provider ───────────────────────────────────────────────────────────────
# openai (default — production, unchanged if these vars are unset) or ollama
# (local development against a locally-running Ollama server).
AI_PROVIDER     = os.getenv('AI_PROVIDER', 'openai').strip().lower()
OLLAMA_BASE_URL = os.getenv('OLLAMA_BASE_URL', 'http://localhost:11434/v1')
OPENAI_API_KEY  = os.getenv('OPENAI_API_KEY', '')

# ── Optional env vars with defaults ──────────────────────────────────────────
REDIS_HOST          = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT          = int(os.getenv('REDIS_PORT', 6379))
STORAGE_BUCKET      = os.getenv('STORAGE_BUCKET', 'talent-pool-resumes')
STORAGE_BULK_PREFIX = os.getenv('STORAGE_BULK_PREFIX', 'bulk')
RESUME_PARSER_URL   = os.getenv('RESUME_PARSER_URL', 'http://resume-parser-container:5005')
PORT                = int(os.getenv('PORT', 5010))

BULK_QUEUE_NAME     = 'bulk-pipeline'


def _check_ollama_reachable(base_url: str) -> None:
    """Best-effort startup probe — logs clearly, never raises or blocks
    startup. Lets you tell at a glance whether the local backend can reach
    Ollama before any AI processing is attempted."""
    import requests as _requests
    try:
        resp = _requests.get(f"{base_url.rstrip('/')}/models", timeout=2)
        if resp.ok:
            _logger.info("[AI provider] Ollama reachable at %s", base_url)
        else:
            _logger.warning(
                "[AI provider] Ollama at %s responded with status %s",
                base_url, resp.status_code,
            )
    except Exception as exc:
        _logger.warning(
            "[AI provider] Ollama not reachable at %s (%s). Start it with "
            "`ollama serve` (or the Ollama app), and confirm your model is "
            "pulled with `ollama list`, before AI processing will succeed.",
            base_url, exc,
        )


# ── Clients (module-level singletons) ────────────────────────────────────────
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

if AI_PROVIDER == 'ollama':
    # Ollama's OpenAI-compatible endpoint doesn't validate the API key, but
    # the OpenAI SDK requires a non-empty string.
    openai_client = OpenAI(api_key='ollama', base_url=OLLAMA_BASE_URL)
    _logger.info("[AI provider] Using Ollama at %s", OLLAMA_BASE_URL)
    _check_ollama_reachable(OLLAMA_BASE_URL)
else:
    if not OPENAI_API_KEY:
        raise RuntimeError(
            "OPENAI_API_KEY is required when AI_PROVIDER=openai (the default). "
            "Set AI_PROVIDER=ollama and OLLAMA_BASE_URL instead for local "
            "Ollama/Qwen3 development."
        )
    openai_client = OpenAI(api_key=OPENAI_API_KEY)
    _logger.info("[AI provider] Using OpenAI")

redis_conn       = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
bulk_queue       = Queue(BULK_QUEUE_NAME, connection=redis_conn)