"""Exact, signed storage witnesses for cancelled never-ready Job VM retention."""

from __future__ import annotations

from collections.abc import Mapping
import json
import re
from uuid import UUID

from shared.vm_pre_ssh_stop import valid_frozen_stop_candidate


def _uuid(value):
    try:
        return isinstance(value, str) and str(UUID(value)) == value
    except (TypeError, ValueError, AttributeError):
        return False


def _name(value, limit):
    return (
        isinstance(value, str)
        and 0 < len(value) <= limit
        and bool(re.fullmatch(r"[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?", value))
    )


def _same(value, expected):
    if not isinstance(value, Mapping):
        return False
    try:
        return json.dumps(dict(value), sort_keys=True) == json.dumps(
            expected, sort_keys=True
        )
    except (TypeError, ValueError):
        return False


def valid_retention_preflight(value, frozen):
    if not isinstance(value, Mapping) or not valid_frozen_stop_candidate(frozen):
        return False
    if frozen["kind"] == "vm_initial_ready_positive_stop_candidate_v1":
        authority = value.get("frozen")
        return (
            value.get("kind") == "vm_job_initial_ready_preflight_v1"
            and valid_ready_retention_preflight(value, authority)
            and all(
                frozen[key] == authority[key]
                for key in (
                    "job_id",
                    "provision_generation",
                    "namespace",
                    "vm_uid",
                    "vmi_uid",
                    "launcher_uid",
                    "pvc_uid",
                    "node_uid",
                    "cleanup_request_id",
                    "cleanup_intent_digest",
                )
            )
        )
    if not _uuid(value.get("dv_uid")) or not _name(value.get("pvc_name"), 253):
        return False
    if not _name(frozen["namespace"], 63) or "." in frozen["namespace"]:
        return False
    return _same(
        value,
        {
            "version": 1,
            "kind": "vm_cancel_retention_preflight_v1",
            "stop_policy": "cancel_retention_v1",
            "frozen": dict(frozen),
            "namespace": frozen["namespace"],
            "owner_id": frozen["job_id"],
            "pvc_name": value["pvc_name"],
            "pvc_uid": frozen["pvc_uid"],
            "dv_uid": value["dv_uid"],
            "ownership": "standalone_dv",
            "deleting": False,
            "consumer_scope": "exact_frozen_runtime_only",
        },
    )


_READY_UUID_FIELDS = frozenset(
    {
        "job_id",
        "continuation_id",
        "request_id",
        "provision_generation",
        "reservation_id",
        "vm_uid",
        "vmi_uid",
        "launcher_uid",
        "node_uid",
        "pvc_uid",
        "cleanup_request_id",
    }
)
_READY_CANDIDATE_FIELDS = _READY_UUID_FIELDS | {
    "version",
    "kind",
    "owner_kind",
    "namespace",
    "cluster_id",
    "reservation_revision",
    "cleanup_intent_digest",
}


def valid_ready_retention_candidate(value):
    """Ready roots and continuations have distinct exact authority envelopes."""
    initial = (
        isinstance(value, Mapping)
        and value.get("kind") == "vm_job_initial_ready_stop_candidate_v1"
    )
    uuid_fields = (
        _READY_UUID_FIELDS - {"continuation_id"} if initial else _READY_UUID_FIELDS
    )
    fields = (
        _READY_CANDIDATE_FIELDS - {"continuation_id"}
        if initial
        else _READY_CANDIDATE_FIELDS
    )
    return (
        isinstance(value, Mapping)
        and set(value) == fields
        and type(value["version"]) is int
        and value["version"] == 1
        and value["kind"]
        in {
            "vm_job_retained_ready_stop_candidate_v1",
            "vm_job_initial_ready_stop_candidate_v1",
        }
        and value["owner_kind"] == "job"
        and all(_uuid(value[field]) for field in uuid_fields)
        and _name(value["namespace"], 63)
        and "." not in value["namespace"]
        and isinstance(value["cluster_id"], str)
        and 0 < len(value["cluster_id"]) <= 253
        and value["cluster_id"] == value["cluster_id"].strip()
        and not any(character.isspace() for character in value["cluster_id"])
        and type(value["reservation_revision"]) is int
        and value["reservation_revision"] > 0
        and isinstance(value["cleanup_intent_digest"], str)
        and re.fullmatch(r"sha256:[0-9a-f]{64}", value["cleanup_intent_digest"])
        is not None
    )


def valid_ready_retention_preflight(value, frozen):
    if not isinstance(value, Mapping) or not valid_ready_retention_candidate(frozen):
        return False
    if not _uuid(value.get("dv_uid")) or not _name(value.get("pvc_name"), 253):
        return False
    if value["pvc_name"] != f"agent-vm-{frozen['job_id']}-rootdisk":
        return False
    initial = frozen["kind"] == "vm_job_initial_ready_stop_candidate_v1"
    return _same(
        value,
        {
            "version": 1,
            "kind": "vm_job_initial_ready_preflight_v1"
            if initial
            else "vm_job_retained_ready_preflight_v1",
            "stop_policy": "initial_ready_cancel_v1"
            if initial
            else "retained_ready_continuation_v1",
            "frozen": dict(frozen),
            "namespace": frozen["namespace"],
            "owner_id": frozen["job_id"],
            "pvc_name": value["pvc_name"],
            "pvc_uid": frozen["pvc_uid"],
            "dv_uid": value["dv_uid"],
            "ownership": "standalone_dv",
            "deleting": False,
            "consumer_scope": "exact_frozen_runtime_only",
        },
    )


def retained_rootdisk_from_preflight(preflight):
    """Expected final witness; callers must still authenticate fresh observation."""
    if not isinstance(preflight, Mapping) or not (
        (
            preflight.get("kind") == "vm_cancel_retention_preflight_v1"
            and valid_retention_preflight(preflight, preflight.get("frozen"))
        )
        or (
            preflight.get("kind")
            in {
                "vm_job_retained_ready_preflight_v1",
                "vm_job_initial_ready_preflight_v1",
            }
            and valid_ready_retention_preflight(preflight, preflight.get("frozen"))
        )
    ):
        raise ValueError("retention_preflight_unproven")
    return {
        "version": 1,
        "kind": "vm_retained_rootdisk_v1",
        "namespace": preflight["namespace"],
        "owner_kind": "job",
        "owner_id": preflight["owner_id"],
        "pvc_name": preflight["pvc_name"],
        "pvc_uid": preflight["pvc_uid"],
        "dv_uid": preflight["dv_uid"],
        "ownership": "standalone_dv",
        "deleting": False,
        "no_consumers": True,
    }


def valid_retained_rootdisk(value, preflight):
    try:
        return _same(value, retained_rootdisk_from_preflight(preflight))
    except ValueError:
        return False
