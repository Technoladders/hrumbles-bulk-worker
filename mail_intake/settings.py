"""
mail_intake/settings.py — Gmail Resume Intake configuration.

Everything heavy (Gmail reads, downloads, text extraction, AI parsing, the
queue and the review list) runs in this service and its own SQLite file.
Supabase is touched only to check the caller's hr_employees flag and, after a
reviewer approves a resume, to upload the file and call
upsert_candidate_with_timeframe.

Environment (/opt/hrumbles-bulk-worker/.env):

  GOOGLE_INTAKE_CLIENT_ID          Google OAuth web client (same GCP project as the Gmail app)
  GOOGLE_INTAKE_CLIENT_SECRET
  GOOGLE_INTAKE_REDIRECT_URI       https://bulk-api.xrilic.ai/api/mail-intake/oauth/callback
  INTAKE_ENCRYPTION_KEY            64 hex chars (32 bytes) — encrypts Gmail refresh tokens
  INTAKE_DATA_DIR                  optional; default /data (the mounted volume) or mail_intake/data locally
  INTAKE_ALLOWED_RETURN_ORIGINS    comma list of frontend origins the OAuth callback may return to
                                   (tenant subdomains of these hosts are accepted too)
  SUPABASE_JWT_SECRET              optional; verifies HS256 user tokens locally
  MAIL_INTAKE_OPENAI_API_KEY       optional; defaults to OPENAI_API_KEY
  MAIL_INTAKE_OPENAI_MODEL         optional; default gpt-6-luna
  MAIL_INTAKE_ENABLED              optional; "false" registers nothing (kill switch)

SUPABASE_URL / SUPABASE_SERVICE_KEY / STORAGE_BUCKET are the service's existing ones.
"""

import os


def _num(name: str, default: float) -> float:
    try:
        v = float(os.getenv(name, ''))
        return v if v > 0 else default
    except ValueError:
        return default


ENABLED = os.getenv('MAIL_INTAKE_ENABLED', 'true').strip().lower() not in ('0', 'false', 'no')

SUPABASE_URL = os.getenv('SUPABASE_URL', '').rstrip('/')
SUPABASE_JWT_SECRET = os.getenv('SUPABASE_JWT_SECRET', '')

GOOGLE_CLIENT_ID = os.getenv('GOOGLE_INTAKE_CLIENT_ID', '')
GOOGLE_CLIENT_SECRET = os.getenv('GOOGLE_INTAKE_CLIENT_SECRET', '')
GOOGLE_REDIRECT_URI = os.getenv('GOOGLE_INTAKE_REDIRECT_URI', '')

ENCRYPTION_KEY_HEX = os.getenv('INTAKE_ENCRYPTION_KEY', '')

# /data is the volume the deploy mounts ($APP_DIR/intake-data); locally it falls
# back to mail_intake/data.
DATA_DIR = os.path.abspath(
    os.getenv('INTAKE_DATA_DIR')
    or ('/data' if os.path.isdir('/data') else os.path.join(os.path.dirname(__file__), 'data'))
)
DB_PATH = os.path.join(DATA_DIR, 'mail-intake.db')
FILES_DIR = os.path.join(DATA_DIR, 'files')

ALLOWED_RETURN_ORIGINS = [
    o.strip()
    for o in os.getenv(
        'INTAKE_ALLOWED_RETURN_ORIGINS',
        'https://www.xrilic.ai,https://demo.dev.xrilic.ai,http://localhost:8081',
    ).split(',')
    if o.strip()
]

STORAGE_BUCKET = os.getenv('STORAGE_BUCKET', 'talent-pool-resumes')

OPENAI_API_KEY = os.getenv('MAIL_INTAKE_OPENAI_API_KEY') or os.getenv('OPENAI_API_KEY', '')
OPENAI_MODEL = os.getenv('MAIL_INTAKE_OPENAI_MODEL', 'gpt-6-luna')

# Pacing. Gmail allows 250 quota units/user/second; a message or attachment
# read costs 5, so ~6 reads/second stays far below it. The tick runs in the
# APScheduler thread pool next to the bulk/YOHR jobs, so it does a bounded
# amount of work and returns.
TICK_SECONDS = int(_num('INTAKE_TICK_SECONDS', 15))
LIVE_SYNC_EVERY_SECONDS = _num('INTAKE_LIVE_SYNC_SECONDS', 300)
MESSAGES_PER_TICK = int(_num('INTAKE_MESSAGES_PER_TICK', 25))
GMAIL_GAP_SECONDS = _num('INTAKE_GMAIL_GAP_SECONDS', 0.15)
LIST_PAGES_PER_TICK = int(_num('INTAKE_LIST_PAGES_PER_TICK', 2))
# Supabase writes after approval: few per tick, paced, because every
# hr_talent_pool row fires five triggers.
APPROVALS_PER_TICK = int(_num('INTAKE_APPROVALS_PER_TICK', 10))
APPROVAL_GAP_SECONDS = _num('INTAKE_APPROVAL_GAP_SECONDS', 0.5)
MAX_ATTEMPTS = 3

MAX_ATTACHMENT_BYTES = 10 * 1024 * 1024
MIN_ATTACHMENT_BYTES = 15 * 1024
