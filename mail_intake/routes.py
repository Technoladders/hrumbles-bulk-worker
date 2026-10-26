"""
mail_intake/routes.py — /api/mail-intake blueprint.

Every route except the Google OAuth callback needs a Supabase user with
hr_employees.gmail_resume_intake_access = true and only sees its own
organization's mailboxes and items. The heavy work happens in the tick
(worker.py); these routes only read/write the local SQLite file.

CORS: the app's existing CORS(app, resources={'/api/*': ...}) already covers
these paths.
"""

import logging
import re
import time
from datetime import date
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

from flask import Blueprint, Response, g, jsonify, redirect, request

from . import gmail as gmail_api
from . import settings
from .auth import require_intake_user
from .crypto_utils import decrypt, encrypt, pkce_pair, random_token
from .detect import sniff_type
from .parse import MIME_BY_KIND
from .pipeline import FileStore
from .store import all_rows, db, new_id, now, one

logger = logging.getLogger(__name__)
bp = Blueprint('mail_intake', __name__, url_prefix='/api/mail-intake')
files = FileStore()

OAUTH_STATE_TTL = 600
PAGE_SIZE_MAX = 100

TABS = {
    'likely': "category = 'likely' and status = 'pending'",
    'check': "category = 'check' and status = 'pending'",
    'rejected': "((category = 'rejected' and status = 'pending') or status = 'rejected')",
    'processing': "status in ('approving', 'reprocess')",
    'added': "status in ('added', 'updated', 'duplicate')",
    'failed': "status = 'failed'",
}


def _bad(status: int, error: str):
    return jsonify({'success': False, 'error': error}), status


def is_allowed_return(url: str) -> bool:
    try:
        u = urlparse(url)
    except ValueError:
        return False
    if u.scheme not in ('http', 'https') or not u.netloc:
        return False
    for origin in settings.ALLOWED_RETURN_ORIGINS:
        a = urlparse(origin)
        base = re.sub(r'^www\.', '', a.netloc)
        if u.scheme == a.scheme and (u.netloc == a.netloc or u.netloc.endswith('.' + base)):
            return True
    return False


def _with_param(url: str, key: str, value: str) -> str:
    u = urlparse(url)
    q = [(k, v) for k, v in parse_qsl(u.query) if k != key] + [(key, value)]
    return urlunparse(u._replace(query=urlencode(q)))


def _ids() -> list[str]:
    body = request.get_json(silent=True) or {}
    ids = body.get('ids')
    return [i for i in ids if isinstance(i, str)][:500] if isinstance(ids, list) else []


def _update_items(org: str, ids: list[str], set_sql: str, where_sql: str, params: dict | None = None) -> int:
    if not ids:
        return 0
    named = {f'id{i}': v for i, v in enumerate(ids)}
    placeholders = ','.join(f':{k}' for k in named)
    sql = f'update items set {set_sql}, updated_at = :now where organization_id = :org and id in ({placeholders}) and {where_sql}'
    return db().execute(sql, {**named, **(params or {}), 'org': org, 'now': now()}).rowcount


def _is_date(s: str | None) -> bool:
    if not s or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', s):
        return False
    try:
        date.fromisoformat(s)
        return True
    except ValueError:
        return False


# ── OAuth callback (no bearer: Google redirects the browser here) ─────────

@bp.get('/oauth/callback')
def oauth_callback():
    state = request.args.get('state', '')
    row = one('select * from oauth_states where state = ?', (state,)) if state else None
    if row:
        db().execute('delete from oauth_states where state = ?', (state,))
    if not row or row['expires_at'] < time.time():
        return Response('This sign-in link has expired. Please start again from Xrilic.', status=400, mimetype='text/plain')

    def back(key, value):
        return redirect(_with_param(row['return_to'], key, value), code=302)

    code, error = request.args.get('code'), request.args.get('error')
    if error or not code:
        return back('gmail_intake_error', error or 'cancelled')
    try:
        refresh_token, email = gmail_api.exchange_code(code, row['code_verifier'])
        enc = encrypt(refresh_token, settings.ENCRYPTION_KEY_HEX)
        existing = one('select id from mailboxes where organization_id = ? and email = ?', (row['organization_id'], email))
        if existing:
            db().execute("update mailboxes set refresh_token_enc = ?, connected_by = ?, status = 'active', sync_error = null, updated_at = ? where id = ?",
                         (enc, row['employee_id'], now(), existing['id']))
        else:
            db().execute("""insert into mailboxes (id, organization_id, connected_by, email, refresh_token_enc, status, created_at, updated_at)
                            values (?, ?, ?, ?, ?, 'active', ?, ?)""",
                         (new_id(), row['organization_id'], row['employee_id'], email, enc, now(), now()))
        return back('gmail_intake', 'connected')
    except Exception as e:
        logger.error(f'[mail_intake] oauth callback: {e}')
        return back('gmail_intake_error', str(e)[:200])


# ── everything below needs an intake user ──────────────────────────────────

@bp.get('/status')
@require_intake_user
def status():
    org = g.intake_user['organization_id']
    mailboxes = all_rows("""select id, email, status, sync_error, last_sync_at, connected_by, created_at
                              from mailboxes where organization_id = ? order by created_at""", (org,))
    runs = all_rows("""
        select r.id, r.mailbox_id, r.after_date, r.before_date, r.state, r.listed, r.error, r.created_at, r.updated_at,
               (select count(*) from messages m where m.run_id = r.id and m.state != 'queued') as processed,
               (select count(*) from messages m where m.run_id = r.id and m.state = 'queued') as queued
          from backfill_runs r join mailboxes b on b.id = r.mailbox_id
         where b.organization_id = ?
         order by r.created_at desc limit 10""", (org,))
    counts = {tab: one(f'select count(*) as n from items where organization_id = ? and {where}', (org,))['n']
              for tab, where in TABS.items()}
    live_queued = one("""select count(*) as n from messages m join mailboxes b on b.id = m.mailbox_id
                          where b.organization_id = ? and m.run_id is null and m.state = 'queued'""", (org,))['n']
    return jsonify({'success': True, 'mailboxes': mailboxes, 'runs': runs, 'counts': counts, 'liveQueued': live_queued})


@bp.post('/connect')
@require_intake_user
def connect():
    if not settings.GOOGLE_CLIENT_ID or not settings.GOOGLE_REDIRECT_URI:
        return _bad(503, 'Gmail connection is not configured on the server.')
    body = request.get_json(silent=True) or {}
    return_to = str(body.get('returnTo') or '')
    if not is_allowed_return(return_to):
        return _bad(400, 'Return address not allowed.')
    verifier, challenge = pkce_pair()
    state = random_token()
    db().execute('delete from oauth_states where expires_at < ?', (time.time(),))
    db().execute("""insert into oauth_states (state, employee_id, organization_id, code_verifier, return_to, expires_at)
                    values (?, ?, ?, ?, ?, ?)""",
                 (state, g.intake_user['id'], g.intake_user['organization_id'], verifier, return_to, time.time() + OAUTH_STATE_TTL))
    return jsonify({'success': True, 'url': gmail_api.build_auth_url(state, challenge, body.get('loginHint'))})


@bp.delete('/mailboxes/<mailbox_id>')
@require_intake_user
def disconnect(mailbox_id):
    mbx = one('select * from mailboxes where id = ? and organization_id = ?', (mailbox_id, g.intake_user['organization_id']))
    if not mbx:
        return _bad(404, 'Mailbox not found.')
    try:
        gmail_api.revoke(decrypt(mbx['refresh_token_enc'], settings.ENCRYPTION_KEY_HEX))
    except Exception:
        pass  # best effort
    for it in all_rows('select local_path from items where mailbox_id = ? and local_path is not null', (mbx['id'],)):
        files.remove(it['local_path'])
    db().execute('delete from mailboxes where id = ?', (mbx['id'],))  # cascades runs, messages, items
    return jsonify({'success': True})


@bp.post('/mailboxes/<mailbox_id>/backfill')
@require_intake_user
def start_backfill(mailbox_id):
    mbx = one('select id from mailboxes where id = ? and organization_id = ?', (mailbox_id, g.intake_user['organization_id']))
    if not mbx:
        return _bad(404, 'Mailbox not found.')
    body = request.get_json(silent=True) or {}
    after, before = str(body.get('afterDate') or ''), body.get('beforeDate') or None
    if not _is_date(after) or (before and not _is_date(before)):
        return _bad(400, 'Dates must be YYYY-MM-DD.')
    if before and before <= after:
        return _bad(400, "'To' date must be after 'From' date.")
    if one("select id from backfill_runs where mailbox_id = ? and state in ('listing', 'processing', 'paused')", (mbx['id'],)):
        return _bad(409, 'A backfill is already running for this mailbox.')
    run_id = new_id()
    db().execute("""insert into backfill_runs (id, mailbox_id, after_date, before_date, state, created_by, created_at, updated_at)
                    values (?, ?, ?, ?, 'listing', ?, ?, ?)""",
                 (run_id, mbx['id'], after, before, g.intake_user['id'], now(), now()))
    return jsonify({'success': True, 'id': run_id})


@bp.post('/backfill/<run_id>/<action>')
@require_intake_user
def backfill_action(run_id, action):
    run = one("""select r.* from backfill_runs r join mailboxes b on b.id = r.mailbox_id
                  where r.id = ? and b.organization_id = ?""", (run_id, g.intake_user['organization_id']))
    if not run:
        return _bad(404, 'Backfill not found.')
    transitions = {
        'pause': (('listing', 'processing'), 'paused'),
        'resume': (('paused',), 'listing' if run['page_token'] or run['listed'] == 0 else 'processing'),
        'cancel': (('listing', 'processing', 'paused'), 'cancelled'),
    }
    if action not in transitions:
        return _bad(400, 'Unknown action.')
    allowed, to = transitions[action]
    if run['state'] not in allowed:
        return _bad(409, f"Cannot {action} a {run['state']} backfill.")
    db().execute('update backfill_runs set state = ?, updated_at = ? where id = ?', (to, now(), run['id']))
    if to == 'cancelled':
        db().execute("update messages set state = 'skipped', reason = 'backfill cancelled', updated_at = ? where run_id = ? and state = 'queued'",
                     (now(), run['id']))
    return jsonify({'success': True})


@bp.get('/items')
@require_intake_user
def list_items():
    org = g.intake_user['organization_id']
    where = TABS.get(request.args.get('tab', ''), TABS['likely'])
    try:
        page_size = min(PAGE_SIZE_MAX, max(1, int(request.args.get('pageSize', 25))))
        page = max(1, int(request.args.get('page', 1)))
    except ValueError:
        page_size, page = 25, 1
    search = (request.args.get('search') or '').strip().lower()

    params = {'org': org, 'limit': page_size, 'offset': (page - 1) * page_size}
    flt = ''
    if search:
        flt = """ and (lower(filename) like :q or lower(coalesce(sender_email,'')) like :q or lower(coalesce(sender_name,'')) like :q
                      or lower(coalesce(subject,'')) like :q or lower(coalesce(parsed_json,'')) like :q)"""
        params['q'] = '%' + re.sub(r'[%_]', '', search) + '%'

    total = one(f'select count(*) as n from items where organization_id = :org and {where}{flt}', params)['n']
    rows = all_rows(f"""
        select id, mailbox_id, (select b.email from mailboxes b where b.id = items.mailbox_id) as mailbox_email,
               filename, mime_type, size_bytes, sender_email, sender_name, subject, received_at, category, reason, status,
               parsed_json, ai_confidence, result_message, resume_url, decided_at, (local_path is not null) as has_file
          from items
         where organization_id = :org and {where}{flt}
         order by received_at desc
         limit :limit offset :offset""", params)

    import json
    items = []
    for r in rows:
        p = json.loads(r.pop('parsed_json')) if r.get('parsed_json') else None
        r['has_file'] = bool(r['has_file'])
        r['candidate'] = {
            'name': p.get('candidate_name'),
            'email': p.get('email'),
            'phone': p.get('phone'),
            'title': p.get('suggested_title'),
            'location': p.get('current_location'),
            'skills': (p.get('top_skills') or [])[:8] if isinstance(p.get('top_skills'), list) else [],
        } if p else None
        items.append(r)
    return jsonify({'success': True, 'items': items, 'total': total, 'page': page, 'pageSize': page_size})


@bp.get('/items/<item_id>/file')
@require_intake_user
def item_file(item_id):
    item = one('select * from items where id = ? and organization_id = ?', (item_id, g.intake_user['organization_id']))
    if not item:
        return _bad(404, 'Not found.')
    buf = files.read(item['local_path']) if files.exists(item['local_path']) else None
    if buf is None:
        # Not held locally (rejected or already added): fetch live from Gmail, do not store.
        mbx = one('select * from mailboxes where id = ?', (item['mailbox_id'],))
        if not mbx or not item['attachment_id']:
            return _bad(404, 'File is no longer available.')
        try:
            client = gmail_api.GmailClient(decrypt(mbx['refresh_token_enc'], settings.ENCRYPTION_KEY_HEX))
            buf = client.get_attachment(item['gmail_id'], item['attachment_id'])
        except Exception as e:
            return _bad(502, f'Could not fetch the file from Gmail: {e}')
    kind = sniff_type(buf)
    safe_name = re.sub(r'["\r\n\\]', '_', item['filename'])
    return Response(buf, mimetype=MIME_BY_KIND.get(kind, 'application/octet-stream'), headers={
        'Content-Disposition': f'inline; filename="{safe_name}"',
        'Cache-Control': 'private, no-store',
    })


@bp.post('/items/approve')
@require_intake_user
def approve():
    n = _update_items(g.intake_user['organization_id'], _ids(),
                      "status = 'approving', decided_by = :by, decided_at = :now, result_message = null",
                      "status in ('pending', 'failed') and category != 'rejected'", {'by': g.intake_user['id']})
    return jsonify({'success': True, 'updated': n})


@bp.post('/items/reject')
@require_intake_user
def reject():
    org, ids = g.intake_user['organization_id'], _ids()
    for i in ids:
        it = one("select local_path from items where id = ? and organization_id = ? and status in ('pending', 'failed')", (i, org))
        if it and it['local_path']:
            files.remove(it['local_path'])
    n = _update_items(org, ids, "status = 'rejected', local_path = null, decided_by = :by, decided_at = :now",
                      "status in ('pending', 'failed')", {'by': g.intake_user['id']})
    return jsonify({'success': True, 'updated': n})


@bp.post('/items/reprocess')
@require_intake_user
def reprocess():
    n = _update_items(g.intake_user['organization_id'], _ids(), "status = 'reprocess'",
                      "status in ('pending', 'rejected', 'failed') and category in ('rejected', 'check')")
    return jsonify({'success': True, 'updated': n})


@bp.post('/items/restore')
@require_intake_user
def restore():
    n = _update_items(g.intake_user['organization_id'], _ids(), "status = 'pending', decided_by = null, decided_at = null",
                      "status = 'rejected'")
    return jsonify({'success': True, 'updated': n})
