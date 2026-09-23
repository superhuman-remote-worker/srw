"""Non-secret identity for one exact pinned Job delivery projection."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping
from typing import Any


_DELIVERY_FIELDS = frozenset({
    "pinned_delivery_id", "pinned_projection_digest", "pinned_delivery_proof",
})


def pinned_job_projection_digest(payload: Mapping[str, Any]) -> str:
    """Hash the actual wire projection, without storing or logging its payload."""

    material = {
        key: value for key, value in payload.items() if key not in _DELIVERY_FIELDS
    }
    encoded = json.dumps(
        material, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(
        b"srw:pinned-job-delivery:v1\0" + encoded
    ).hexdigest()


def pinned_job_delivery_proof(
    secret: bytes,
    *,
    delivery_id: str,
    agent_id: str,
    process_generation: str,
    pod_uid: str,
    projection_digest: str,
) -> str:
    """Mint a process-bound report acknowledgment from an orchestrator-only key."""

    material = "\0".join((
        delivery_id, agent_id, process_generation, pod_uid, projection_digest,
    )).encode("utf-8")
    return hmac.new(
        secret, b"srw:pinned-job-report:v1\0" + material, hashlib.sha256,
    ).hexdigest()
