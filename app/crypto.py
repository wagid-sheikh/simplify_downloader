"""Utility helpers for encrypting/decrypting secrets in system_config."""

from __future__ import annotations

import base64
import hashlib
from itertools import cycle


def _derived_key(secret_key: str) -> bytes:
    return hashlib.sha256(secret_key.encode("utf-8")).digest()


def encrypt_secret(secret_key: str, plaintext: str) -> str:
    key_stream = cycle(_derived_key(secret_key))
    data = plaintext.encode("utf-8")
    cipher_bytes = bytes(b ^ next(key_stream) for b in data)
    return base64.urlsafe_b64encode(cipher_bytes).decode("utf-8")


def decrypt_secret(secret_key: str, ciphertext: str) -> str:
    key_stream = cycle(_derived_key(secret_key))
    data = base64.urlsafe_b64decode(ciphertext.encode("utf-8"))
    plain_bytes = bytes(b ^ next(key_stream) for b in data)
    return plain_bytes.decode("utf-8")
