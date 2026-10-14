"""AES-GCM encryption at rest for stored credentials.

Used for:
- user_api_keys.api_key
- project_api_keys.api_key
- system_api_keys.api_key
- llm_endpoints.api_key

Ciphertext format: ``v1:<nonce-b64>:<ct-b64>`` — the prefix lets a future
version coexist with v1 ciphertexts during key rotation.

Key source: 32 raw bytes loaded from the ``APP_ENCRYPTION_KEY`` env var.
The value may be:
- 32 raw bytes encoded as standard base64 (44 chars including padding)
- 32 raw bytes encoded as URL-safe base64
- a 32-byte ASCII/alphanumeric string (randAlphaNum 32 output), used directly

Loading is lazy + cached. ``get_cipher()`` raises if the key is unset or the
wrong length, so credential-touching code paths fail loudly at boot rather
than silently storing plaintext.
"""

from __future__ import annotations

import base64
import hmac
import os
import secrets
from functools import lru_cache

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

ENV_VAR = "APP_ENCRYPTION_KEY"
CIPHERTEXT_PREFIX = "v1:"
NONCE_BYTES = 12  # AES-GCM standard nonce size
KEY_BYTES = 32  # AES-256


class EncryptionKeyError(RuntimeError):
    """Raised when APP_ENCRYPTION_KEY is missing or malformed."""


class DecryptionError(RuntimeError):
    """Raised when ciphertext is malformed or authentication fails."""


def _decode_key(raw: str) -> bytes:
    """Accept base64, url-safe base64, or a raw 32-char string."""
    raw = raw.strip()
    if not raw:
        raise EncryptionKeyError(f"{ENV_VAR} is empty")

    # Raw 32-char string (e.g., output of Helm's randAlphaNum 32).
    if len(raw) == KEY_BYTES:
        return raw.encode("utf-8")

    # Try standard base64, then url-safe base64. Both variants are tried
    # with and without added padding: Vault/ESO/YAML pipelines sometimes
    # strip trailing `=`, and Python's decoders reject unpadded input
    # rather than inferring the length. Only standard b64decode supports
    # validate=True; for the url-safe variant we rely on length check
    # after decode to detect garbage.
    padding = "=" * (-len(raw) % 4)
    for candidate in (raw, raw + padding):
        for decoder in (
            lambda s: base64.b64decode(s, validate=True),
            base64.urlsafe_b64decode,
        ):
            try:
                decoded = decoder(candidate)
            except (ValueError, base64.binascii.Error):
                continue
            if len(decoded) == KEY_BYTES:
                return decoded

    raise EncryptionKeyError(
        f"{ENV_VAR} must decode to {KEY_BYTES} bytes "
        f"(got {len(raw)}-char input that isn't valid base64 of 32 bytes "
        f"or a 32-char raw string)"
    )


def _load_key() -> bytes:
    """Load the deployment key without exposing it in errors or logs."""
    raw = os.getenv(ENV_VAR)
    if raw is None:
        raise EncryptionKeyError(
            f"{ENV_VAR} is not set; credential storage requires an encryption "
            f"key. In dev, generate one with "
            f"`python -c 'import secrets,base64; "
            f"print(base64.b64encode(secrets.token_bytes(32)).decode())'`"
        )
    return _decode_key(raw)


@lru_cache(maxsize=1)
def get_cipher() -> AESGCM:
    """Return the singleton AES-GCM cipher, loading the key on first call."""
    return AESGCM(_load_key())


def credential_fingerprint(plaintext: str) -> str:
    """Stable, keyed change detector for credentials, including weak secrets.

    A database-only leak must not provide an offline guessing oracle alongside
    encrypted credentials. Derive a separate HMAC key from APP_ENCRYPTION_KEY;
    the domain separates fingerprinting from encryption and other key uses.
    Rotation changes the fingerprint, causing managed Helm rows to reconcile.
    This is not a password verifier.
    """
    key = hmac.digest(_load_key(), b"srw:credential-fingerprint:v1", "sha256")
    digest = hmac.digest(key, plaintext.encode("utf-8"), "sha256").hex()
    return f"hmac-sha256:{digest}"


def live_control_token(connector_id: str) -> str:
    """The token SRW calls a live connector's pod with (orchestrator → pod).

    Derived, never stored: a separate HMAC key from APP_ENCRYPTION_KEY, over
    the connector id, so every pod of the connector (a restart, a superseding
    generation) accepts it and the orchestrator recomputes it per call. The
    pod reads it from its request file (live_session_jobs.md §5.3).
    """
    key = hmac.digest(_load_key(), b"srw:live-control:v1", "sha256")
    mac = hmac.digest(key, f"srw-live-control:{connector_id}".encode(), "sha256")
    return base64.urlsafe_b64encode(mac).rstrip(b"=").decode("ascii")


def reset_cipher_cache() -> None:
    """Invalidate the cached cipher. Test-only hook for key-rotation scenarios."""
    get_cipher.cache_clear()


def encrypt(plaintext: str) -> str:
    """Encrypt a UTF-8 string. Returns ``v1:<nonce-b64>:<ct-b64>``.

    Empty strings are rejected — callers should store NULL instead of an
    encrypted empty value so the resolver's ``key IS NOT NULL`` checks work.
    """
    if not isinstance(plaintext, str):
        raise TypeError(f"encrypt() expects str, got {type(plaintext).__name__}")
    if plaintext == "":
        raise ValueError("encrypt() refuses empty plaintext — store NULL instead")

    cipher = get_cipher()
    nonce = secrets.token_bytes(NONCE_BYTES)
    ct = cipher.encrypt(nonce, plaintext.encode("utf-8"), associated_data=None)
    return (
        CIPHERTEXT_PREFIX
        + base64.b64encode(nonce).decode("ascii")
        + ":"
        + base64.b64encode(ct).decode("ascii")
    )


def decrypt(ciphertext: str) -> str:
    """Decrypt a ``v1:``-prefixed ciphertext. Raises DecryptionError on any failure."""
    if not isinstance(ciphertext, str):
        raise DecryptionError(f"decrypt() expects str, got {type(ciphertext).__name__}")
    if not ciphertext.startswith(CIPHERTEXT_PREFIX):
        raise DecryptionError(
            f"ciphertext missing {CIPHERTEXT_PREFIX!r} prefix "
            f"(legacy plaintext values are not supported)"
        )

    body = ciphertext[len(CIPHERTEXT_PREFIX) :]
    parts = body.split(":")
    if len(parts) != 2:
        raise DecryptionError("malformed v1 ciphertext: expected 'v1:<nonce>:<ct>'")

    try:
        nonce = base64.b64decode(parts[0], validate=True)
        ct = base64.b64decode(parts[1], validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise DecryptionError(f"base64 decode failed: {exc}") from exc

    if len(nonce) != NONCE_BYTES:
        raise DecryptionError(
            f"invalid nonce length: {len(nonce)} (expected {NONCE_BYTES})"
        )

    try:
        plaintext = get_cipher().decrypt(nonce, ct, associated_data=None)
    except Exception as exc:
        raise DecryptionError(f"AES-GCM authentication failed: {exc}") from exc
    return plaintext.decode("utf-8")


def is_encrypted(value: str | None) -> bool:
    """True if ``value`` looks like a v1 ciphertext. Cheap structural check only."""
    return isinstance(value, str) and value.startswith(CIPHERTEXT_PREFIX)
