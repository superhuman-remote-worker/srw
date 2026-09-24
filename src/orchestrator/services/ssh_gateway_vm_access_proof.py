"""Gateway-host-key signature proving an SSH connection passed key verification.

The gateway alone holds the ed25519 host private key.  The orchestrator has
only its configured public halves, so the ubiquitous internal API key cannot
mint a VM access lease.  A proof is one short-lived, action-specific request;
the database still decides owner authorization and per-connection replay.
"""

from __future__ import annotations

import re
import time
from collections.abc import Sequence
from typing import Any

import asyncssh

from orchestrator.services.ssh_handles import is_valid_handle

_VERSION = "srw-vm-ssh-access1"
_ACTIONS = frozenset({"admit", "renew", "close"})
_CONNECTION = re.compile(r"[0-9a-f]{32}\Z")
_TTL = 30


def _message(payload: dict[str, Any]) -> bytes:
    return "\n".join(
        (
            _VERSION,
            payload["action"],
            payload["connection_id"],
            payload["handle"],
            payload["fingerprint"],
            str(payload["expires_at"]),
            payload["key_fingerprint"],
            payload["lease_id"],
            payload["binding"],
        )
    ).encode("ascii")


def mint_vm_access_proof(
    key: asyncssh.SSHKey,
    *,
    connection_id: str,
    handle: str,
    fingerprint: str,
    action: str,
    lease_id: str = "",
    binding: str = "",
    now: float | None = None,
) -> dict[str, Any]:
    if (
        key.get_algorithm() != "ssh-ed25519"
        or action not in _ACTIONS
        or not _CONNECTION.fullmatch(connection_id)
        or not is_valid_handle(handle)
        or not isinstance(fingerprint, str)
        or not 1 <= len(fingerprint) <= 128
        or not fingerprint.isascii()
        or "\n" in fingerprint
        or "\r" in fingerprint
        or not isinstance(lease_id, str)
        or not isinstance(binding, str)
        or not re.fullmatch(r"[0-9a-f-]{0,36}", lease_id)
        or not re.fullmatch(r"[0-9a-f]{0,64}", binding)
        or (action == "admit" and (lease_id or binding))
        or (action != "admit" and (len(lease_id) != 36 or len(binding) != 64))
    ):
        raise ValueError("invalid VM SSH proof input")
    payload = {
        "action": action,
        "connection_id": connection_id,
        "handle": handle,
        "fingerprint": fingerprint,
        "expires_at": int(time.time() if now is None else now) + _TTL,
        "key_fingerprint": key.get_fingerprint("sha256"),
        "lease_id": lease_id,
        "binding": binding,
    }
    payload["signature"] = key.sign(_message(payload), b"ssh-ed25519").hex()
    return payload


def verify_vm_access_proof(
    payload: object,
    public_keys: Sequence[str],
    *,
    action: str,
    now: float | None = None,
) -> bool:
    if not isinstance(payload, dict) or action not in _ACTIONS:
        return False
    try:
        if (
            set(payload)
            != {
                "action",
                "connection_id",
                "handle",
                "fingerprint",
                "expires_at",
                "key_fingerprint",
                "signature",
                "lease_id",
                "binding",
            }
            or payload["action"] != action
        ):
            return False
        if (
            not isinstance(payload["connection_id"], str)
            or not _CONNECTION.fullmatch(payload["connection_id"])
            or not isinstance(payload["handle"], str)
            or not is_valid_handle(payload["handle"])
            or not isinstance(payload["fingerprint"], str)
            or not 1 <= len(payload["fingerprint"]) <= 128
            or not payload["fingerprint"].isascii()
            or "\n" in payload["fingerprint"]
            or "\r" in payload["fingerprint"]
            or type(payload["expires_at"]) is not int
            or not isinstance(payload["key_fingerprint"], str)
            or not isinstance(payload["signature"], str)
            or len(payload["signature"]) > 1024
            or not isinstance(payload["lease_id"], str)
            or not isinstance(payload["binding"], str)
            or not re.fullmatch(r"[0-9a-f-]{0,36}", payload["lease_id"])
            or not re.fullmatch(r"[0-9a-f]{0,64}", payload["binding"])
            or (action == "admit" and (payload["lease_id"] or payload["binding"]))
            or (
                action != "admit"
                and (len(payload["lease_id"]) != 36 or len(payload["binding"]) != 64)
            )
        ):
            return False
        clock = time.time() if now is None else now
        if not clock <= payload["expires_at"] <= clock + _TTL + 5:
            return False
        signature = bytes.fromhex(payload["signature"])
        message = _message(payload)
        for exported in public_keys:
            key = asyncssh.import_public_key(exported)
            if (
                key.get_algorithm() == "ssh-ed25519"
                and key.get_fingerprint("sha256") == payload["key_fingerprint"]
                and key.verify(message, signature)
            ):
                return True
    except (TypeError, ValueError, KeyError, UnicodeError, asyncssh.KeyImportError):
        return False
    return False
