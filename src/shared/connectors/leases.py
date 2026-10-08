"""Credential lease tokens and the exchange vocabulary (slice C2).

A lease token stands in for a connector's upstream credential inside a
workspace; a driver exchanges it with SRW for the real credential. A driver
pod authenticates the exchange with its own identity token. Both are opaque:

* ``scl_`` + 32 random bytes in base62 (43 characters) + a CRC32 checksum in
  base62 (6 characters), for a lease;
* ``sdi_`` in the same shape, for a driver identity.

The prefix and the checksum let SRW's redaction and outside secret scanners
recognise a token and reject a mistyped one offline, as GitHub's tokens do.
SRW stores only :func:`token_digest` (SHA-256). The prefixes do not collide
with ``srw_`` (personal tokens) or ``sra_``/``srr_``/``srb_`` (runtime actor).

Design: knowledge-base/knowledge/features/connector_drivers.md, "The lease
service".
"""

from __future__ import annotations

import hashlib
import re
import secrets
import zlib
from collections.abc import Mapping
from typing import Any

from .contract import DriverSpec

LEASE_TOKEN_PREFIX = "scl"
DRIVER_IDENTITY_PREFIX = "sdi"
TOKEN_PREFIXES: tuple[str, ...] = (LEASE_TOKEN_PREFIX, DRIVER_IDENTITY_PREFIX)

_BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_RANDOM_BYTES = 32
#: 62**43 > 2**256, so 32 random bytes always fit in 43 digits.
_BODY_LENGTH = 43
_CHECKSUM_LENGTH = 6
TOKEN_LENGTH = 4 + _BODY_LENGTH + _CHECKSUM_LENGTH
#: Any SRW lease or driver identity token, for redaction and scanners.
TOKEN_PATTERN = (
    r"\bs(?:cl|di)_[0-9A-Za-z]{" + str(_BODY_LENGTH + _CHECKSUM_LENGTH) + r"}\b"
)
_TOKEN = re.compile(
    r"(s(?:cl|di))_([0-9A-Za-z]{"
    + str(_BODY_LENGTH)
    + r"})([0-9A-Za-z]{"
    + str(_CHECKSUM_LENGTH)
    + r"})"
)

#: Where a lease token file lands, relative to the workspace home. One file
#: per connector, named by its id, mode 0600; C0 keeps the directory out of
#: workspace snapshots.
LEASE_FILE_DIR = ".srw-credentials/leases"

#: The longest a driver may cache an exchange result, in seconds. This is
#: the revocation lag.
MAX_CACHE_SECONDS = 30
#: What a driver asks to do with the upstream credential, and the access
#: levels that allow it.
OPERATION_ACCESS: Mapping[str, frozenset[str]] = {
    "read": frozenset({"ReadOnly", "ReadWrite"}),
    "write": frozenset({"ReadWrite"}),
}


def _base62(value: int, width: int) -> str:
    digits: list[str] = []
    while value:
        value, digit = divmod(value, 62)
        digits.append(_BASE62[digit])
    return "".join(reversed(digits)).rjust(width, "0")


def _checksum(prefix: str, body: str) -> str:
    return _base62(zlib.crc32(f"{prefix}_{body}".encode("ascii")), _CHECKSUM_LENGTH)


def mint_token(prefix: str) -> str:
    """A new random token with ``prefix`` (``scl`` or ``sdi``)."""
    if prefix not in TOKEN_PREFIXES:
        raise ValueError(f"unknown token prefix {prefix!r}")
    body = _base62(
        int.from_bytes(secrets.token_bytes(_RANDOM_BYTES), "big"), _BODY_LENGTH
    )
    return f"{prefix}_{body}{_checksum(prefix, body)}"


def token_shape_valid(token: Any, prefix: str) -> bool:
    """Whether ``token`` is a well-formed ``prefix`` token with a good checksum."""
    if not isinstance(token, str) or len(token) != TOKEN_LENGTH:
        return False
    match = _TOKEN.fullmatch(token)
    if match is None or match.group(1) != prefix:
        return False
    return secrets.compare_digest(
        match.group(3), _checksum(match.group(1), match.group(2))
    )


def token_digest(token: str) -> bytes:
    """The SHA-256 digest SRW stores and looks a token up by."""
    return hashlib.sha256(token.encode("utf-8")).digest()


def last_four(token: str) -> str:
    """The token's last four characters, for display and audit."""
    return token[-4:]


def lease_file_name(connector_id: str) -> str:
    """The lease file of one connector, relative to the workspace home."""
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", connector_id or ""):
        raise ValueError("a lease file is named by a connector UUID")
    return f"{LEASE_FILE_DIR}/{connector_id.lower()}"


def lease_access(entry: Mapping[str, Any], spec: DriverSpec) -> str | None:
    """The access level a delivered connector's lease is issued at.

    A read-only project link clamps to the driver's lowest level; otherwise
    the driver's default, else its highest (as the agent reads a binding).
    """
    levels = spec.ranked_access_ids()
    if not levels:
        return None
    if entry.get("project_read_only"):
        return levels[0]
    return spec.default_access or levels[-1]


def operation_allowed(operation: str, access: str | None) -> bool:
    """Whether a lease at ``access`` may be exchanged for ``operation``."""
    return access in OPERATION_ACCESS.get(operation, frozenset())


__all__ = [
    "DRIVER_IDENTITY_PREFIX",
    "LEASE_FILE_DIR",
    "LEASE_TOKEN_PREFIX",
    "MAX_CACHE_SECONDS",
    "OPERATION_ACCESS",
    "TOKEN_LENGTH",
    "TOKEN_PATTERN",
    "TOKEN_PREFIXES",
    "last_four",
    "lease_access",
    "lease_file_name",
    "mint_token",
    "operation_allowed",
    "token_digest",
    "token_shape_valid",
]
