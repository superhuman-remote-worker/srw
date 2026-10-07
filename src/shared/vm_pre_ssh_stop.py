"""Strict metadata envelopes for the bounded VM pre-SSH positive-stop path."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
import re
from uuid import UUID


PRE_SSH_STOP_FINALIZER = "srw.io/vm-pre-ssh-positive-stop"
PRE_SSH_STOP_ANNOTATION = "srw.io/vm-pre-ssh-stop-intent-digest"

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_CANDIDATE_KEYS = {
    "kind",
    "job_id",
    "provision_generation",
    "namespace",
    "vm_name",
    "vm_uid",
    "vmi_uid",
    "launcher_name",
    "launcher_uid",
    "pvc_uid",
    "node_name",
    "node_uid",
    "vm_resource_version",
    "vm_generation",
    "launcher_resource_version",
    "containers",
}
_PROOF_KEYS = {
    "kind",
    "frozen_digest",
    "vm_uid",
    "vmi_uid",
    "launcher_uid",
    "node_uid",
    "vm_run_strategy",
    "vm_generation",
    "node_ready",
    "vmi_disposition",
    "same_generation_replacement",
    "pod_finalizer",
    "pod_intent_digest",
    "pod_terminal",
    "containers",
    "controller_authenticated",
}
_CONTAINER_KEYS = {"kind", "name", "container_id"}
_TERMINAL_CONTAINER_KEYS = _CONTAINER_KEYS | {
    "terminated_container_id",
    "restart_count",
    "state",
    "last_state",
    "finished_at",
    "reason",
}


def _canonical_uuid(value: object) -> bool:
    try:
        return isinstance(value, str) and str(UUID(value)) == value
    except (ValueError, AttributeError, TypeError):
        return False


def _text(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and value == value.strip()
        and not any(ch.isspace() for ch in value)
    )


def _container_vector(value: object) -> dict[tuple[str, str], str] | None:
    if not isinstance(value, list) or not value:
        return None
    result: dict[tuple[str, str], str] = {}
    names: set[str] = set()
    for item in value:
        if (
            not isinstance(item, Mapping)
            or set(item) != _CONTAINER_KEYS
            or item.get("kind") not in {"regular", "init"}
            or not _text(item.get("name"))
            or not _text(item.get("container_id"))
        ):
            return None
        name = item["name"]
        if name in names:
            return None
        names.add(name)
        result[(item["kind"], name)] = item["container_id"]
    if ("regular", "compute") not in result:
        return None
    return result


def valid_frozen_stop_candidate(value: object) -> bool:
    """Accept one complete, current launcher metadata vector, never a hint."""

    if not isinstance(value, Mapping) or set(value) != _CANDIDATE_KEYS:
        return False
    if value.get("kind") != "vm_pre_ssh_stop_candidate_v1":
        return False
    if any(
        not _canonical_uuid(value.get(key))
        for key in (
            "job_id",
            "provision_generation",
            "vm_uid",
            "vmi_uid",
            "launcher_uid",
            "pvc_uid",
            "node_uid",
        )
    ):
        return False
    if any(
        not _text(value.get(key))
        for key in (
            "namespace",
            "vm_name",
            "launcher_name",
            "node_name",
            "vm_resource_version",
            "launcher_resource_version",
        )
    ):
        return False
    if type(value.get("vm_generation")) is not int or value["vm_generation"] < 1:
        return False
    if value["vm_name"] != f"agent-vm-{value['job_id']}":
        return False
    return _container_vector(value.get("containers")) is not None


def valid_positive_stop_proof(
    frozen: object, observed: object, *, frozen_digest: str
) -> bool:
    """Check a signed controller observation against every frozen stop identity."""

    if (
        not valid_frozen_stop_candidate(frozen)
        or not isinstance(observed, Mapping)
        or set(observed) != _PROOF_KEYS
        or not isinstance(frozen_digest, str)
        or not _DIGEST.fullmatch(frozen_digest)
        or observed.get("kind") != "vm_pre_ssh_positive_stop_v1"
        or observed.get("frozen_digest") != frozen_digest
        or observed.get("pod_intent_digest") != frozen_digest
        or observed.get("pod_finalizer") != PRE_SSH_STOP_FINALIZER
        or observed.get("vm_run_strategy") != "Halted"
        or type(observed.get("vm_generation")) is not int
        or observed["vm_generation"] != frozen["vm_generation"] + 1
        or observed.get("vmi_disposition") not in {"absent", "terminal"}
        or observed.get("node_ready") is not True
        or observed.get("same_generation_replacement") is not False
        or observed.get("controller_authenticated") is not True
    ):
        return False
    if any(
        observed.get(key) != frozen.get(key)
        for key in ("vm_uid", "vmi_uid", "launcher_uid", "node_uid")
    ):
        return False
    terminal = observed.get("pod_terminal")
    if (
        not isinstance(terminal, Mapping)
        or set(terminal) != {"phase", "restart_policy"}
        or terminal.get("phase") not in {"Succeeded", "Failed"}
        or terminal.get("restart_policy") != "Never"
    ):
        return False
    frozen_vector = _container_vector(frozen["containers"])
    statuses = observed.get("containers")
    if not isinstance(statuses, list) or len(statuses) != len(frozen_vector):
        return False
    seen: set[tuple[str, str]] = set()
    for status in statuses:
        if not isinstance(status, Mapping) or set(status) != _TERMINAL_CONTAINER_KEYS:
            return False
        key = (status.get("kind"), status.get("name"))
        container_id = frozen_vector.get(key)
        if key in seen or container_id is None:
            return False
        seen.add(key)
        if (
            status.get("container_id") != container_id
            or status.get("terminated_container_id") != container_id
            or type(status.get("restart_count")) is not int
            or status["restart_count"] != 0
            or status.get("state") != "terminated"
            or status.get("last_state") is not None
            or not _text(status.get("reason"))
            or status["reason"] == "ContainerStatusUnknown"
        ):
            return False
        finished = status.get("finished_at")
        if not isinstance(finished, str) or not finished:
            return False
        try:
            timestamp = datetime.fromisoformat(finished.replace("Z", "+00:00"))
        except ValueError:
            return False
        if timestamp.tzinfo is None:
            return False
    return seen == set(frozen_vector)
