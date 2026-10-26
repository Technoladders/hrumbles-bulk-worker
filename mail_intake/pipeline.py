"""
mail_intake/pipeline.py — per-message and per-item processing.

Dependencies (gmail client, extract_text, parse_resume, supabase, files) come
in a `deps` dict so the whole flow is tested with fakes
(tests/test_mail_intake_pipeline.py).
"""

import json
import os

from . import settings
from .crypto_utils import sha256_hex
from .detect import (
    CONTENT_REJECT, META_REJECT, attachment_gate, categorize, content_score, meta_score, skip_message, sniff_type,
)
from .gmail import summarize_message
from .parse import MIME_BY_KIND
from .store import db, new_id, now, one


class FileStore:
    """Resumes held for review, on the INTAKE_DATA_DIR volume."""

    def save(self, org_id: str, sha: str, kind: str, buf: bytes) -> str:
        d = os.path.join(settings.FILES_DIR, org_id)
        os.makedirs(d, exist_ok=True)
        p = os.path.join(d, f'{sha}.{kind}')
        with open(p, 'wb') as f:
            f.write(buf)
        return p

    def read(self, p: str) -> bytes:
        with open(p, 'rb') as f:
            return f.read()

    def exists(self, p: str | None) -> bool:
        return bool(p) and os.path.exists(p)

    def remove(self, p: str | None) -> None:
        try:
            if p:
                os.unlink(p)
        except OSError:
            pass


# ── analysis shared by new attachments and "process anyway" ────────────────

def analyse(deps: dict, gmail_id: str, att: dict, meta: int, org_id: str, force: bool = False,
            existing_buf: bytes | None = None) -> dict:
    """Download (if needed), extract and classify one attachment.
    force = a reviewer asked to process it anyway: skip the thresholds."""
    out = {'content_score': None, 'ai': None, 'sha': None, 'kind': None, 'buf': None, 'text': '', 'reason': None}

    buf = existing_buf if existing_buf is not None else deps['gmail'].get_attachment(gmail_id, att['attachment_id'])
    if len(buf) > settings.MAX_ATTACHMENT_BYTES:
        return {**out, 'category': 'rejected', 'reason': 'file larger than 10 MB'}
    kind = sniff_type(buf)
    if not kind:
        return {**out, 'category': 'rejected', 'reason': 'not a PDF or Word file'}
    out.update(sha=sha256_hex(buf), kind=kind, buf=buf)

    if not force and org_id:
        dup = one('select id from items where organization_id = ? and sha256 = ?', (org_id, out['sha']))
        if dup:
            return {**out, 'duplicate_of': dup['id']}

    try:
        out['text'] = deps['extract_text'](buf, kind) or ''
    except Exception:
        out['text'] = ''

    content = content_score(out['text'])
    out['content_score'] = content['score']
    if content['no_text']:
        reason = 'could not read this .doc file — open it to check' if kind == 'doc' else 'no readable text — may be a scanned resume'
        return {**out, 'category': 'check', 'reason': reason}
    if not force and content['score'] < CONTENT_REJECT:
        return {**out, 'category': 'rejected', 'reason': 'content does not look like a resume'}

    ai = deps['parse_resume'](out['text'])
    out['ai'] = ai
    category, reason = categorize(
        0 if force else meta,
        {'score': CONTENT_REJECT, 'no_text': False} if force else content,
        {'is_resume': ai['is_resume'], 'confidence': ai['confidence']},
        bool((ai.get('profile') or {}).get('email')),
    )
    return {**out, 'category': category, 'reason': reason}


def _fields(a: dict) -> dict:
    ai = a.get('ai')
    return {
        'sha256': a.get('sha'),
        'content_score': a.get('content_score'),
        'ai_is_resume': (1 if ai['is_resume'] else 0) if ai else None,
        'ai_confidence': ai['confidence'] if ai else None,
        'parsed_json': json.dumps(ai['profile']) if ai and ai['is_resume'] else None,
        'resume_text': None if a['category'] == 'rejected' else (a.get('text') or None),
        'input_tokens': ai['input_tokens'] if ai else None,
        'output_tokens': ai['output_tokens'] if ai else None,
        'category': a['category'],
        'reason': a.get('reason'),
    }


_INSERT = """
insert into items (id, organization_id, mailbox_id, gmail_id, part_id, attachment_id, filename, mime_type, size_bytes,
                   sha256, sender_email, sender_name, subject, received_at, meta_score, content_score,
                   ai_is_resume, ai_confidence, category, reason, status, parsed_json, resume_text, local_path,
                   input_tokens, output_tokens, created_at, updated_at)
values (:id, :organization_id, :mailbox_id, :gmail_id, :part_id, :attachment_id, :filename, :mime_type, :size_bytes,
        :sha256, :sender_email, :sender_name, :subject, :received_at, :meta_score, :content_score,
        :ai_is_resume, :ai_confidence, :category, :reason, 'pending', :parsed_json, :resume_text, :local_path,
        :input_tokens, :output_tokens, :created_at, :updated_at)
on conflict do nothing
"""


# ── one Gmail message ──────────────────────────────────────────────────────

def process_message(deps: dict, mailbox: dict, gmail_id: str) -> dict:
    """{'state': 'done'|'skipped', 'reason', 'items', 'duplicates'}; raises on transient errors."""
    raw = deps['gmail'].get_structure(gmail_id)
    if not raw:
        return {'state': 'skipped', 'reason': 'message deleted', 'items': 0, 'duplicates': 0}
    msg = summarize_message(raw)
    skip = skip_message(msg, mailbox['email'])
    if skip:
        return {'state': 'skipped', 'reason': skip, 'items': 0, 'duplicates': 0}

    docs = [a for a in msg['attachments']
            if not attachment_gate(a, settings.MIN_ATTACHMENT_BYTES, settings.MAX_ATTACHMENT_BYTES)]
    if not docs:
        return {'state': 'skipped', 'reason': 'no document attachment', 'items': 0, 'duplicates': 0}

    files = deps['files']
    items = duplicates = 0
    for att in docs:
        if one('select 1 as x from items where mailbox_id = ? and gmail_id = ? and part_id = ?',
               (mailbox['id'], gmail_id, att['part_id'])):
            continue
        meta, _reasons = meta_score(msg, att, len(docs))
        base = {
            'id': new_id(),
            'organization_id': mailbox['organization_id'],
            'mailbox_id': mailbox['id'],
            'gmail_id': gmail_id,
            'part_id': att['part_id'],
            'attachment_id': att['attachment_id'],
            'filename': att['filename'][:255],
            'mime_type': att['mime_type'],
            'size_bytes': att['size'],
            'sender_email': msg['from_email'],
            'sender_name': msg['from_name'],
            'subject': (msg['subject'] or '')[:500],
            'received_at': msg['received_at'],
            'meta_score': meta,
            'created_at': now(),
            'updated_at': now(),
        }

        if meta < META_REJECT:
            # Decided from headers alone: nothing is downloaded.
            db().execute(_INSERT, {**base, 'sha256': None, 'content_score': None, 'ai_is_resume': None,
                                   'ai_confidence': None, 'category': 'rejected',
                                   'reason': 'file name/sender do not look like a resume', 'parsed_json': None,
                                   'resume_text': None, 'local_path': None, 'input_tokens': None, 'output_tokens': None})
            items += 1
            continue

        a = analyse(deps, gmail_id, att, meta, mailbox['organization_id'])
        if a.get('duplicate_of'):
            duplicates += 1
            continue
        keep = a.get('buf') is not None and a['category'] != 'rejected'
        local_path = files.save(mailbox['organization_id'], a['sha'], a['kind'], a['buf']) if keep else None
        cur = db().execute(_INSERT, {**base, **_fields(a), 'local_path': local_path})
        if cur.rowcount == 0:
            # Lost a race on (org, sha256): the same file arrived in another message.
            files.remove(local_path)
            duplicates += 1
            continue
        items += 1
    return {'state': 'done', 'reason': None, 'items': items, 'duplicates': duplicates}


# ── "Process anyway" ──────────────────────────────────────────────────────

def reprocess_item(deps: dict, item: dict) -> None:
    files = deps['files']
    buf = files.read(item['local_path']) if files.exists(item['local_path']) else None
    a = analyse(deps, item['gmail_id'], {'attachment_id': item['attachment_id']}, 0, item['organization_id'],
                force=True, existing_buf=buf)
    # A reviewer explicitly asked for this one: never auto-reject it again.
    if a['category'] == 'rejected' and a.get('sha'):
        a['category'] = 'check'
        if a.get('ai') and not a['ai']['is_resume']:
            a['reason'] = 'AI thinks this is not a resume — please check'
    local_path = item['local_path']
    if a.get('buf') is not None and not files.exists(local_path):
        local_path = files.save(item['organization_id'], a['sha'], a['kind'], a['buf'])
    f = _fields(a)
    try:
        db().execute("""
            update items set sha256 = coalesce(:sha256, sha256), content_score = :content_score,
                   ai_is_resume = :ai_is_resume, ai_confidence = :ai_confidence, parsed_json = :parsed_json,
                   resume_text = :resume_text, input_tokens = :input_tokens, output_tokens = :output_tokens,
                   category = :category, reason = :reason, local_path = :local_path, status = 'pending',
                   updated_at = :updated_at
             where id = :id""", {**f, 'local_path': local_path, 'updated_at': now(), 'id': item['id']})
    except Exception as e:
        if 'UNIQUE' not in str(e).upper():
            raise
        # Same file already exists as another item: keep that one.
        db().execute("update items set status = 'rejected', reason = 'same file as another resume', updated_at = ? where id = ?",
                     (now(), item['id']))


# ── approval → Supabase ─────────────────────────────────────────────────

RPC_STATUS = {
    'INSERTED': ('added', 'Added to talent pool'),
    'UPDATED': ('updated', 'Existing candidate updated'),
    'SKIPPED_RECENT': ('duplicate', 'Already in talent pool (updated in the last 30 days) — resume attached'),
    'SKIPPED_NO_EMAIL': ('failed', 'No email address — cannot add'),
}


def approve_item(deps: dict, item: dict) -> str:
    """Uploads the file and calls upsert_candidate_with_timeframe — the same
    function and dedupe rules (lower(trim(email)) per organization) as the
    talent-pool bulk upload."""
    sb, files = deps['supabase'], deps['files']
    parsed = json.loads(item['parsed_json']) if item.get('parsed_json') else {}
    email = (parsed.get('email') or item.get('sender_email') or '').strip().lower() or None
    profile = {**parsed, 'candidate_name': parsed.get('candidate_name') or item.get('sender_name'), 'email': email}
    if not email:
        db().execute("update items set status = 'failed', result_message = ?, updated_at = ? where id = ?",
                     ('No email address — cannot add', now(), item['id']))
        return 'failed'

    buf = files.read(item['local_path']) if files.exists(item.get('local_path')) else None
    if buf is None and deps.get('gmail') is not None:
        buf = deps['gmail'].get_attachment(item['gmail_id'], item['attachment_id'])
    if buf is None:
        raise RuntimeError('Resume file is no longer available.')
    kind = sniff_type(buf) or 'pdf'
    sha = item.get('sha256') or sha256_hex(buf)

    key = f"{item['organization_id']}/gmail-intake/{sha}.{kind}"
    bucket = sb.storage.from_(settings.STORAGE_BUCKET)
    bucket.upload(key, buf, {'content-type': MIME_BY_KIND[kind], 'cache-control': '3600', 'upsert': 'true'})
    resume_url = bucket.get_public_url(key)

    res = sb.rpc('upsert_candidate_with_timeframe', {
        'profile_data': profile,
        'resume_text': item.get('resume_text') or '',
        'organization_id_input': item['organization_id'],
        'user_id_input': item['decided_by'],
        'resume_path_input': resume_url,
        'input_tokens_used': item.get('input_tokens') or 0,
        'output_tokens_used': item.get('output_tokens') or 0,
        'usage_type_input': 'gmail_resume_intake',
    }).execute()
    status = res.data if isinstance(res.data, str) else (res.data[0] if isinstance(res.data, list) and res.data else res.data)

    nxt, message = RPC_STATUS.get(status, ('failed', f'Unexpected result: {status}'))
    db().execute('update items set status = ?, result_message = ?, resume_url = ?, local_path = ?, updated_at = ? where id = ?',
                 (nxt, message, resume_url, item.get('local_path') if nxt == 'failed' else None, now(), item['id']))
    if nxt != 'failed':
        files.remove(item.get('local_path'))
    return nxt
