"""
tests/test_mail_intake_security.py
Refresh-token encryption, local JWT verification, OAuth return-address check.

Run:
    python -m unittest tests.test_mail_intake_security -v
"""
import time
import unittest

import jwt
from cryptography.hazmat.primitives.asymmetric import ec

from mail_intake.auth import verify_token_locally
from mail_intake.crypto_utils import decrypt, encrypt
from mail_intake.routes import is_allowed_return

KEY = '11' * 32


class CryptoTests(unittest.TestCase):
    def test_round_trip_and_tamper(self):
        enc = encrypt('1//refresh-token', KEY)
        self.assertNotIn('refresh-token', enc)
        self.assertEqual(decrypt(enc, KEY), '1//refresh-token')
        v, nonce, ct = enc.split('.')
        with self.assertRaises(Exception):
            decrypt(f'{v}.{nonce}.{ct[:-4]}AAAA', KEY)
        with self.assertRaises(Exception):
            decrypt(enc, '22' * 32)
        with self.assertRaises(ValueError):
            encrypt('x', 'short')


class _FakeJwks:
    def __init__(self, public_key):
        self.key = public_key

    def get_signing_key_from_jwt(self, _token):
        return self


class JwtTests(unittest.TestCase):
    def test_hs256(self):
        good = jwt.encode({'sub': 'u1', 'exp': int(time.time()) + 60}, 's3cret-for-tests-only-0123456789abcdef', algorithm='HS256')
        self.assertEqual(verify_token_locally(good, 's3cret-for-tests-only-0123456789abcdef')['sub'], 'u1')
        self.assertIsNone(verify_token_locally(good, 'other'))
        expired = jwt.encode({'sub': 'u1', 'exp': int(time.time()) - 5}, 's3cret-for-tests-only-0123456789abcdef', algorithm='HS256')
        self.assertIsNone(verify_token_locally(expired, 's3cret-for-tests-only-0123456789abcdef'))
        with self.assertRaises(LookupError):
            verify_token_locally(good, '')
        self.assertIsNone(verify_token_locally('not-a-jwt', 's3cret-for-tests-only-0123456789abcdef'))

    def test_es256(self):
        priv = ec.generate_private_key(ec.SECP256R1())
        token = jwt.encode({'sub': 'u2', 'exp': int(time.time()) + 60}, priv, algorithm='ES256', headers={'kid': 'k1'})
        self.assertEqual(verify_token_locally(token, '', _FakeJwks(priv.public_key()))['sub'], 'u2')
        other = ec.generate_private_key(ec.SECP256R1())
        self.assertIsNone(verify_token_locally(token, '', _FakeJwks(other.public_key())))


class ReturnAddressTests(unittest.TestCase):
    def test_allowed(self):
        self.assertTrue(is_allowed_return('https://www.xrilic.ai/talent-pool/gmail-intake'))
        self.assertTrue(is_allowed_return('https://clientco.xrilic.ai/talent-pool/gmail-intake'))
        self.assertTrue(is_allowed_return('http://demo.localhost:8081/talent-pool/gmail-intake'))

    def test_rejected(self):
        self.assertFalse(is_allowed_return('https://evil.com/?x=xrilic.ai'))
        self.assertFalse(is_allowed_return('https://xrilic.ai.evil.com/'))
        self.assertFalse(is_allowed_return('http://www.xrilic.ai/'))
        self.assertFalse(is_allowed_return('javascript:alert(1)'))


if __name__ == '__main__':
    unittest.main()
