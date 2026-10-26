"""
mail_intake/store.py — local SQLite store.

Holds connected mailboxes, backfill runs, the per-message queue and the review
items. Nothing here is written to Supabase. Flask request threads and the
APScheduler tick share the file (WAL, busy timeout); each thread keeps its own
connection. INTAKE_DATA_DIR must be a mounted volume, otherwise every deploy
(docker rm + run) wipes it.
"""

import os
import sqlite3
import threading
import uuid
from datetime import datetime, timezone

from . import settings

SCHEMA = """
create table if not exists mailboxes (
  id                 text primary key,
  organization_id    text not null,
  connected_by       text not null,
  email              text not null,
  refresh_token_enc  text not null,
  status             text not null default 'active',   -- active | error
  history_id         text,
  last_sync_at       text,
  sync_error         text,
  created_at         text not null,
  updated_at         text not null,
  unique (organization_id, email)
);

create table if not exists oauth_states (
  state            text primary key,
  employee_id      text not null,
  organization_id  text not null,
  code_verifier    text not null,
  return_to        text not null,
  expires_at       real not null
);

create table if not exists backfill_runs (
  id           text primary key,
  mailbox_id   text not null references mailboxes(id) on delete cascade,
  after_date   text not null,
  before_date  text,
  state        text not null,          -- listing | processing | paused | completed | cancelled | failed
  page_token   text,
  listed       integer not null default 0,
  error        text,
  created_by   text not null,
  created_at   text not null,
  updated_at   text not null
);

create table if not exists messages (
  mailbox_id  text not null references mailboxes(id) on delete cascade,
  gmail_id    text not null,
  run_id      text,                    -- null = live (new mail)
  state       text not null default 'queued',   -- queued | done | skipped | failed
  attempts    integer not null default 0,
  reason      text,
  created_at  text not null,
  updated_at  text not null,
  primary key (mailbox_id, gmail_id)
);
create index if not exists idx_messages_queue on messages (mailbox_id, state, run_id);

create table if not exists items (
  id               text primary key,
  organization_id  text not null,
  mailbox_id       text not null references mailboxes(id) on delete cascade,
  gmail_id         text not null,
  part_id          text not null,
  attachment_id    text,
  filename         text not null,
  mime_type        text,
  size_bytes       integer,
  sha256           text,
  sender_email     text,
  sender_name      text,
  subject          text,
  received_at      text,
  meta_score       integer,
  content_score    integer,
  ai_is_resume     integer,
  ai_confidence    real,
  category         text not null,      -- likely | check | rejected
  reason           text,
  status           text not null default 'pending',
                   -- pending | reprocess | approving | added | updated | duplicate | rejected | failed
  parsed_json      text,
  resume_text      text,
  local_path       text,
  resume_url       text,
  input_tokens     integer,
  output_tokens    integer,
  result_message   text,
  decided_by       text,
  decided_at       text,
  created_at       text not null,
  updated_at       text not null,
  unique (mailbox_id, gmail_id, part_id)
);
create unique index if not exists idx_items_org_sha on items (organization_id, sha256) where sha256 is not null;
create index if not exists idx_items_review on items (organization_id, category, status, received_at);
create index if not exists idx_items_status on items (status);
"""

_local = threading.local()
_path = None
_init_lock = threading.Lock()


def _open(path: str) -> sqlite3.Connection:
    c = sqlite3.connect(path, timeout=5, isolation_level=None, check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute('pragma busy_timeout = 5000')
    c.execute('pragma foreign_keys = on')
    if path != ':memory:':
        c.execute('pragma journal_mode = wal')
    return c


def db() -> sqlite3.Connection:
    """This thread's connection (schema created on first use)."""
    global _path
    with _init_lock:
        if _path is None:
            os.makedirs(settings.DATA_DIR, exist_ok=True)
            os.makedirs(settings.FILES_DIR, exist_ok=True)
            _path = settings.DB_PATH
            c = _open(_path)
            c.executescript(SCHEMA)
            c.close()
    c = getattr(_local, 'conn', None)
    if c is None or getattr(_local, 'path', None) != _path:
        c = _open(_path)
        _local.conn, _local.path = c, _path
    return c


def use_memory_for_tests() -> sqlite3.Connection:
    """Tests only: a fresh in-memory database for the calling thread."""
    global _path
    _path = ':memory:'
    c = _open(':memory:')
    c.executescript(SCHEMA)
    _local.conn, _local.path = c, _path
    return c


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id() -> str:
    return str(uuid.uuid4())


def one(sql: str, params=()) -> dict | None:
    row = db().execute(sql, params).fetchone()
    return dict(row) if row else None


def all_rows(sql: str, params=()) -> list[dict]:
    return [dict(r) for r in db().execute(sql, params).fetchall()]


def run(sql: str, params=()) -> int:
    return db().execute(sql, params).rowcount
