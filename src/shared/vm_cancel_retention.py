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


def retained_rootdisk_from_preflight(preflight):
    """Expected final witness; callers must still authenticate fresh observation."""
    if not isinstance(preflight, Mapping) or not valid_retention_preflight(
        preflight, preflight.get("frozen")
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
