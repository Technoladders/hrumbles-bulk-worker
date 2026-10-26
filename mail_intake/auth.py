"""
mail_intake/auth.py — @require_intake_user for every /api/mail-intake route
except the Google OAuth callback.

  1. Bearer token = the user's Supabase access token. Verified locally when
     possible (HS256 with SUPABASE_JWT_SECRET, or ES256/RS256 against the
     project's JWKS); otherwise Supabase Auth is asked once and the answer is
     cached until the token expires.
  2. hr_employees row must be active with gmail_resume_intake_access = true.
     Cached 60 s, so normal page use costs about one tiny query a minute.

Sets flask.g.intake_user = {'id', 'organization_id'}.
"""

import functools
import hashlib
import threading
import time

import jwt
from flask import g, jsonify, request

from . import settings

ACCESS_TTL = 60
_token_cache: dict[str, tuple[str, float]] = {}
_access_cache: dict[str, tuple[dict | None, float]] = {}
_lock = threading.Lock()
_jwks_client = None


def _supabase():
    from config import supabase  # the service's existing client
    return supabase


def _jwks():
    global _jwks_client
    if _jwks_client is None and settings.SUPABASE_URL:
        _jwks_client = jwt.PyJWKClient(f'{settings.SUPABASE_URL}/auth/v1/.well-known/jwks.json', cache_keys=True, lifespan=3600)
    return _jwks_client


def verify_token_locally(token: str, hs_secret: str = '', jwks_client=None) -> dict | None:
    """Payload when valid, None when invalid. Raises LookupError when this
    token cannot be checked locally."""
    try:
        alg = jwt.get_unverified_header(token).get('alg')
    except jwt.PyJWTError:
        return None
    opts = {'verify_aud': False, 'require': ['exp', 'sub']}
    try:
        if alg == 'HS256':
            if not hs_secret:
                raise LookupError('no secret')
            return jwt.decode(token, hs_secret, algorithms=['HS256'], options=opts)
        if alg in ('ES256', 'RS256'):
            if jwks_client is None:
                raise LookupError('no jwks')
            key = jwks_client.get_signing_key_from_jwt(token).key
            return jwt.decode(token, key, algorithms=[alg], options=opts)
    except jwt.PyJWKClientError as e:
        raise LookupError(str(e))
    except jwt.PyJWTError:
        return None
    raise LookupError(f'unsupported alg {alg}')


def _user_id(token: str) -> str | None:
    key = hashlib.sha256(token.encode()).hexdigest()
    now = time.time()
    with _lock:
        hit = _token_cache.get(key)
    if hit and hit[1] > now:
        return hit[0]

    try:
        payload = verify_token_locally(token, settings.SUPABASE_JWT_SECRET, _jwks())
    except LookupError:
        try:
            res = _supabase().auth.get_user(token)
            user = getattr(res, 'user', None)
        except Exception:
            user = None
        if not user:
            return None
        try:
            exp = float(jwt.decode(token, options={'verify_signature': False}).get('exp', now + 60))
        except jwt.PyJWTError:
            exp = now + 60
        payload = {'sub': user.id, 'exp': exp}
    if not payload:
        return None

    with _lock:
        if len(_token_cache) > 5000:
            for k in [k for k, v in _token_cache.items() if v[1] <= now]:
                _token_cache.pop(k, None)
        _token_cache[key] = (payload['sub'], float(payload['exp']))
    return payload['sub']


def _access(employee_id: str) -> dict | None:
    now = time.time()
    with _lock:
        hit = _access_cache.get(employee_id)
    if hit and now - hit[1] < ACCESS_TTL:
        return hit[0]

    res = _supabase().table('hr_employees') \
        .select('organization_id, status, gmail_resume_intake_access') \
        .eq('id', employee_id).limit(1).execute()
    row = (res.data or [None])[0]
    value = (
        {'id': employee_id, 'organization_id': row['organization_id']}
        if row and row.get('status') == 'active' and row.get('gmail_resume_intake_access') is True and row.get('organization_id')
        else None
    )
    with _lock:
        _access_cache[employee_id] = (value, now)
    return value


def require_intake_user(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        header = request.headers.get('Authorization', '')
        if not header.lower().startswith('bearer '):
            return jsonify({'success': False, 'error': 'unauthenticated'}), 401
        uid = _user_id(header[7:].strip())
        if not uid:
            return jsonify({'success': False, 'error': 'unauthenticated'}), 401
        access = _access(uid)
        if not access:
            return jsonify({'success': False, 'error': 'no_access'}), 403
        g.intake_user = access
        return fn(*args, **kwargs)
    return wrapper
