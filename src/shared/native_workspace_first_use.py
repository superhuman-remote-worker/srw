"""Strict, role-neutral signed notice for native pinned-session first use.

The gateway signs; orchestrator and agent verify with configured public keys.
This domain cannot validate an existing VM SSH access proof.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Sequence
from typing import Any
from uuid import UUID

import asyncssh

DOMAIN = "srw-native-workspace-first-use1"
TTL_SECONDS = 30
FORWARD_SKEW_SECONDS = 5
_HEX_ID = re.compile(r"[0-9a-f]{32}\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_BINDING = re.compile(r"[0-9a-f]{64}\Z")
_HANDLE = re.compile(r"s-[a-z0-9]{8,32}\Z")
_SIGNED_FIELDS = frozenset(
    {
        "domain",
        "action",
        "event_id",
        "connection_id",
        "channel_kind",
        "handle",
        "fingerprint",
        "thread_id",
        "runtime_generation",
        "agent_id",
        "pod_uid",
        "process_generation",
        "session_identity_fingerprint",
        "backend",
        "workspace_digest",
        "lease_id",
        "binding",
        "expires_at",
        "key_fingerprint",
    }
)


def _canonical_uuid(value: Any) -> bool:
    if not isinstance(value, str) or len(value) != 36:
        return False
    try:
        return str(UUID(value)) == value
    except ValueError:
        return False


def _valid(payload: Any) -> bool:
    if not isinstance(payload, dict) or set(payload) != _SIGNED_FIELDS | {"signature"}:
        return False
    if payload["domain"] != DOMAIN or payload["action"] != "first_use":
        return False
    if any(
        not isinstance(payload[name], str) or _HEX_ID.fullmatch(payload[name]) is None
        for name in ("event_id", "connection_id")
    ):
        return False
    if not isinstance(payload["channel_kind"], str) or payload["channel_kind"] not in {
        "ssh_session",
        "sftp",
    }:
        return False
    if not isinstance(payload["handle"], str) or not _HANDLE.fullmatch(
        payload["handle"]
    ):
        return False
    fingerprint = payload["fingerprint"]
    if (
        not isinstance(fingerprint, str)
        or not 1 <= len(fingerprint) <= 128
        or not fingerprint.isascii()
        or not fingerprint.isprintable()
    ):
        return False
    if any(
        not _canonical_uuid(payload[name])
        for name in (
            "thread_id",
            "runtime_generation",
            "agent_id",
            "pod_uid",
            "process_generation",
        )
    ):
        return False
    if any(
        not isinstance(payload[name], str) or _DIGEST.fullmatch(payload[name]) is None
        for name in ("session_identity_fingerprint", "workspace_digest")
    ):
        return False
    backend = payload["backend"]
    if backend == "container":
        if payload["lease_id"] != "" or payload["binding"] != "":
            return False
    elif backend == "vm":
        if (
            not _canonical_uuid(payload["lease_id"])
            or not isinstance(payload["binding"], str)
            or not _BINDING.fullmatch(payload["binding"])
        ):
            return False
    else:
        return False
    if type(payload["expires_at"]) is not int or payload["expires_at"] <= 0:
        return False
    key_fingerprint = payload["key_fingerprint"]
    if (
        not isinstance(key_fingerprint, str)
        or not 1 <= len(key_fingerprint) <= 128
        or not key_fingerprint.isascii()
        or not key_fingerprint.isprintable()
    ):
        return False
    signature = payload["signature"]
    return bool(
        isinstance(signature, str) and re.fullmatch(r"[0-9a-f]{166}", signature)
    )


def _message(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        {name: payload[name] for name in _SIGNED_FIELDS},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")


def mint_native_first_use_proof(
    key: asyncssh.SSHKey,
    *,
    now: float | None = None,
    **fields: Any,
) -> dict[str, Any]:
    if key.get_algorithm() != "ssh-ed25519":
        raise ValueError("native first-use signer must be Ed25519")
    payload = {
        **fields,
        "domain": DOMAIN,
        "action": "first_use",
        "expires_at": int(time.time() if now is None else now) + TTL_SECONDS,
        "key_fingerprint": key.get_fingerprint("sha256"),
        "signature": "0" * 166,
    }
    if not _valid(payload):
        raise ValueError("invalid native first-use proof input")
    payload["signature"] = key.sign(_message(payload), b"ssh-ed25519").hex()
    return payload


def verify_native_first_use_proof(
    payload: object,
    public_keys: Sequence[str],
    *,
    now: float | None = None,
) -> bool:
    if not _valid(payload):
        return False
    assert isinstance(payload, dict)
    clock = time.time() if now is None else now
    if not clock < payload["expires_at"] <= clock + TTL_SECONDS + FORWARD_SKEW_SECONDS:
        return False
    try:
        message = _message(payload)
        signature = bytes.fromhex(payload["signature"])
        for exported in public_keys:
            key = asyncssh.import_public_key(exported)
            if (
                key.get_algorithm() == "ssh-ed25519"
                and key.get_fingerprint("sha256") == payload["key_fingerprint"]
                and key.verify(message, signature)
            ):
                return True
    except (TypeError, ValueError, UnicodeError, asyncssh.KeyImportError):
        return False
    return False
