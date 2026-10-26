"""
mail_intake/crypto_utils.py — refresh-token encryption (AES-256-GCM), PKCE,
hashing. `cryptography` already ships with pdfminer.six.
"""

import base64
import hashlib
import os
import re
import secrets

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b'=').decode()


def _unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + '=' * (-len(s) % 4))


def _key(hex_key: str) -> bytes:
    if not re.fullmatch(r'[0-9a-fA-F]{64}', hex_key or ''):
        raise ValueError('INTAKE_ENCRYPTION_KEY must be 64 hex characters (32 bytes).')
    return bytes.fromhex(hex_key)


def encrypt(plain: str, hex_key: str) -> str:
    """Returns 'v1.<nonce>.<ciphertext+tag>' (base64url parts)."""
    nonce = os.urandom(12)
    ct = AESGCM(_key(hex_key)).encrypt(nonce, plain.encode(), None)
    return f'v1.{_b64(nonce)}.{_b64(ct)}'


def decrypt(token: str, hex_key: str) -> str:
    parts = str(token).split('.')
    if len(parts) != 3 or parts[0] != 'v1':
        raise ValueError('Unrecognised encrypted value.')
    return AESGCM(_key(hex_key)).decrypt(_unb64(parts[1]), _unb64(parts[2]), None).decode()


def pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(48)
    challenge = _b64(hashlib.sha256(verifier.encode()).digest())
    return verifier, challenge


def random_token(n: int = 24) -> str:
    return secrets.token_urlsafe(n)


def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()
