"""
mail_intake/gmail.py — Google OAuth (offline, PKCE) and the few Gmail REST
calls the worker needs. Plain `requests`; no Google client library.

Scope: gmail.readonly (+ openid email to learn the address). This is its own
grant, separate from the SourceXR sequences mailbox; the refresh token is
stored encrypted in the local SQLite file.
"""

import base64
import random
import re
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests

from . import settings

AUTH_URL = 'https://accounts.google.com/o/oauth2/v2/auth'
TOKEN_URL = 'https://oauth2.googleapis.com/token'
REVOKE_URL = 'https://oauth2.googleapis.com/revoke'
USERINFO_URL = 'https://openidconnect.googleapis.com/v1/userinfo'
API = 'https://gmail.googleapis.com/gmail/v1/users/me'

GMAIL_READONLY = 'https://www.googleapis.com/auth/gmail.readonly'
SCOPES = ['openid', 'email', GMAIL_READONLY]
TIMEOUT = 30


class GmailAuthError(Exception):
    """Refresh token revoked/expired: the mailbox must be reconnected."""


class HistoryGoneError(Exception):
    """history.list 404: the stored cursor is too old."""


class GmailHttpError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(f'Gmail {status}: {message}')
        self.status = status


# ── OAuth ───────────────────────────────────────────────────────────────────

def build_auth_url(state: str, code_challenge: str, login_hint: str | None = None) -> str:
    params = {
        'client_id': settings.GOOGLE_CLIENT_ID,
        'redirect_uri': settings.GOOGLE_REDIRECT_URI,
        'response_type': 'code',
        'scope': ' '.join(SCOPES),
        'access_type': 'offline',
        'prompt': 'consent',              # always return a refresh token
        'include_granted_scopes': 'false',
        'state': state,
        'code_challenge': code_challenge,
        'code_challenge_method': 'S256',
    }
    if login_hint:
        params['login_hint'] = login_hint
    return f'{AUTH_URL}?{urlencode(params)}'


def _token_request(body: dict) -> dict:
    res = requests.post(TOKEN_URL, data={
        'client_id': settings.GOOGLE_CLIENT_ID,
        'client_secret': settings.GOOGLE_CLIENT_SECRET,
        **body,
    }, timeout=TIMEOUT)
    try:
        data = res.json()
    except ValueError:
        data = {}
    if not res.ok:
        msg = data.get('error_description') or data.get('error') or f'HTTP {res.status_code}'
        if data.get('error') == 'invalid_grant':
            raise GmailAuthError(msg)
        raise RuntimeError(f'Google token request failed: {msg}')
    return data


def exchange_code(code: str, code_verifier: str) -> tuple[str, str]:
    """Returns (refresh_token, email)."""
    data = _token_request({
        'grant_type': 'authorization_code',
        'code': code,
        'code_verifier': code_verifier,
        'redirect_uri': settings.GOOGLE_REDIRECT_URI,
    })
    if GMAIL_READONLY not in str(data.get('scope', '')).split():
        raise RuntimeError('Gmail read access was not granted. Please tick the Gmail permission on the Google screen.')
    if not data.get('refresh_token'):
        raise RuntimeError('Google did not return a refresh token.')
    info = requests.get(USERINFO_URL, headers={'Authorization': f"Bearer {data['access_token']}"}, timeout=TIMEOUT)
    email = (info.json() if info.ok else {}).get('email')
    if not email:
        raise RuntimeError('Could not read the Google account email.')
    return data['refresh_token'], email.lower()


def revoke(refresh_token: str) -> None:
    try:
        requests.post(REVOKE_URL, params={'token': refresh_token}, timeout=TIMEOUT)
    except Exception:
        pass  # best effort


# ── API client ──────────────────────────────────────────────────────────────

_PART = 'partId,mimeType,filename,body(attachmentId,size)'
# Attachment structure only — no body data — to keep each read small.
STRUCTURE_FIELDS = (
    'id,threadId,labelIds,internalDate,sizeEstimate,'
    f'payload({_PART},headers,parts({_PART},parts({_PART},parts({_PART},parts({_PART})))))'
)


class GmailClient:
    """One per mailbox per tick. Access tokens live in memory only."""

    def __init__(self, refresh_token: str, session: requests.Session | None = None):
        self._refresh_token = refresh_token
        self._http = session or requests.Session()
        self._access = None
        self._expires = 0.0

    def _token(self) -> str:
        if self._access and time.time() < self._expires - 60:
            return self._access
        data = _token_request({'grant_type': 'refresh_token', 'refresh_token': self._refresh_token})
        self._access = data['access_token']
        self._expires = time.time() + float(data.get('expires_in') or 3600)
        return self._access

    def _get(self, path: str, params: dict | None = None, attempt: int = 0) -> dict:
        res = self._http.get(f'{API}{path}', params=params,
                             headers={'Authorization': f'Bearer {self._token()}'}, timeout=TIMEOUT)
        if res.ok:
            return res.json()
        try:
            err = res.json().get('error', {})
        except ValueError:
            err = {}
        reason = ((err.get('errors') or [{}])[0].get('reason') or err.get('status') or '')
        if res.status_code == 401 and attempt == 0:
            self._access = None
            return self._get(path, params, attempt + 1)
        retryable = res.status_code in (429, 500, 502, 503, 504) or (res.status_code == 403 and 'ratelimit' in reason.lower())
        if retryable and attempt < 4:
            time.sleep(2 ** attempt + random.random() / 2)
            return self._get(path, params, attempt + 1)
        raise GmailHttpError(res.status_code, err.get('message') or reason or 'request failed')

    def get_profile(self) -> dict:
        return self._get('/profile')

    def list_messages(self, q: str, page_token: str | None = None) -> tuple[list[str], str | None]:
        params = {'q': q, 'maxResults': 500}
        if page_token:
            params['pageToken'] = page_token
        d = self._get('/messages', params)
        return [m['id'] for m in d.get('messages', [])], d.get('nextPageToken')

    def list_history(self, start_history_id: str, page_token: str | None = None) -> tuple[list[str], str | None, str | None]:
        params = {'startHistoryId': str(start_history_id), 'historyTypes': 'messageAdded', 'maxResults': 500}
        if page_token:
            params['pageToken'] = page_token
        try:
            d = self._get('/history', params)
        except GmailHttpError as e:
            if e.status == 404:
                raise HistoryGoneError() from e
            raise
        ids = [m['message']['id'] for h in d.get('history', []) for m in h.get('messagesAdded', [])]
        return ids, d.get('historyId'), d.get('nextPageToken')

    def get_structure(self, msg_id: str) -> dict | None:
        try:
            return self._get(f'/messages/{msg_id}', {'format': 'full', 'fields': STRUCTURE_FIELDS})
        except GmailHttpError as e:
            if e.status == 404:
                return None  # deleted since it was listed
            raise

    def get_attachment(self, msg_id: str, attachment_id: str) -> bytes:
        d = self._get(f'/messages/{msg_id}/attachments/{attachment_id}')
        data = d.get('data', '')
        return base64.urlsafe_b64decode(data + '=' * (-len(data) % 4))


# ── pure helpers ────────────────────────────────────────────────────────────

def build_backfill_query(after_date: str | None, before_date: str | None = None, own_email: str | None = None) -> str:
    """Gmail search for backfills: Gmail itself drops mail without a document."""
    parts = [
        'has:attachment', 'filename:(pdf OR doc OR docx)',
        '-in:sent', '-in:chats', '-in:spam', '-in:trash', '-in:drafts',
        '-category:promotions', '-category:social',
        'larger:10K', 'smaller:12M',
    ]
    if after_date:
        parts.append(f"after:{after_date[:10].replace('-', '/')}")
    if before_date:
        parts.append(f"before:{before_date[:10].replace('-', '/')}")
    if own_email and re.fullmatch(r'[^\s()"]+@[^\s()"]+', own_email):
        parts.append(f'-from:{own_email}')
    return ' '.join(parts)


def parse_address(value: str | None) -> tuple[str | None, str | None]:
    """'"Name" <A@B.com>' -> ('Name', 'a@b.com')."""
    v = (value or '').strip()
    m = re.match(r'^(.*)<([^>]+)>\s*$', v)
    if m:
        name = m.group(1).strip().strip('"').strip() or None
        return name, m.group(2).strip().lower()
    return None, (v.lower() or None)


def summarize_message(msg: dict) -> dict:
    """Flattens a get_structure() response into what the scorer needs."""
    payload = msg.get('payload') or {}
    headers = {str(h.get('name', '')).lower(): h.get('value', '') for h in payload.get('headers', [])}

    attachments = []

    def walk(part):
        if not part:
            return
        body = part.get('body') or {}
        if part.get('filename') and body.get('attachmentId'):
            attachments.append({
                'part_id': part.get('partId') or '0',
                'filename': part['filename'],
                'mime_type': part.get('mimeType'),
                'size': int(body.get('size') or 0),
                'attachment_id': body['attachmentId'],
            })
        for p in part.get('parts') or []:
            walk(p)

    walk(payload)
    name, email = parse_address(headers.get('from'))
    subject = headers.get('subject', '') or ''
    internal = msg.get('internalDate')
    return {
        'id': msg.get('id'),
        'label_ids': msg.get('labelIds') or [],
        'received_at': datetime.fromtimestamp(int(internal) / 1000, tz=timezone.utc).isoformat() if internal else None,
        'subject': subject,
        'from_email': email,
        'from_name': name,
        'is_reply': bool(headers.get('in-reply-to')) or bool(re.match(r'^\s*re\s*:', subject, re.I)),
        'auto_submitted': bool(re.search(r'auto-(generated|replied)', headers.get('auto-submitted', ''), re.I))
                          or bool(headers.get('list-unsubscribe')),
        'attachments': attachments,
    }
