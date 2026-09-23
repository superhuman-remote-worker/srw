"""Non-secret identity for one exact pinned Job delivery projection."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping
from typing import Any
from uuid import uuid4


_DELIVERY_FIELDS = frozenset({
    "pinned_delivery_id", "pinned_projection_digest", "pinned_delivery_proof",
})
_INPUT_GROUPS = {
    "feedback": (
        "queued_feedback", "queued_feedback_reason", "queued_feedback_delivery_id",
    ),
    "delegation": ("delegation_results", "delegation_results_delivery_id"),
}


def stamp_pinned_resume_input_ids(
    updates: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Give each explicit new queued input its own durable generation."""

    if updates is None:
        return None
    stamped = dict(updates)
    if (stamped.get("queued_feedback") is not None
            and not stamped.get("queued_feedback_delivery_id")):
        stamped["queued_feedback_delivery_id"] = str(uuid4())
    if (stamped.get("delegation_results") is not None
            and not stamped.get("delegation_results_delivery_id")):
        stamped["delegation_results_delivery_id"] = str(uuid4())
    return stamped


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


def pinned_resume_input_digests(
    current: Mapping[str, Any], delivered: Mapping[str, Any] | None,
) -> dict[str, str] | None:
    """Freeze hashes of the exact one-shot context groups sent on a resume."""

    if delivered is None:
        return {}
    if not isinstance(delivered, Mapping):
        return None
    result = {}
    for kind, group in _INPUT_GROUPS.items():
        if not delivered.get(group[0]):
            continue
        if any(
            (key in current) != (key in delivered)
            or current.get(key) != delivered.get(key)
            for key in group
        ):
            return None
        result[kind] = _input_digest(kind, {key: delivered[key] for key in group if key in delivered})
    return result


def pinned_resume_consumed_keys(
    current: Mapping[str, Any], digests: Mapping[str, str] | None,
) -> list[str]:
    """Match only frozen input generations; newer values remain queued."""

    if not isinstance(digests, Mapping):
        return []
    keys = []
    for kind, group in _INPUT_GROUPS.items():
        if kind not in digests or not current.get(group[0]):
            continue
        material = {key: current[key] for key in group if key in current}
        if hmac.compare_digest(_input_digest(kind, material), digests[kind]):
            keys.extend(material)
    return keys


def _input_digest(kind: str, material: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        material, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(
        b"srw:pinned-job-resume-input:v1\0" + kind.encode() + b"\0" + encoded
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
