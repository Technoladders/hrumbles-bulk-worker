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
redis_conn       = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
bulk_queue       = Queue(BULK_QUEUE_NAME, connection=redis_conn)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
)