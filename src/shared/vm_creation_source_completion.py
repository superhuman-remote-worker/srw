"""Typed actual source readbacks, separate from immutable disposition intent."""

from copy import deepcopy
import json
import re
from uuid import UUID

from shared.vm_creation_source_disposition import (
    validate_disposed_pin,
    validate_prepared_disposition,
)

SOURCE_PINS_ANNOTATION = "srw.io/vm-create-source-pins"
MAX_COMPLETION_BYTES = 1024 * 1024


def _same(left, right):
    # JSON booleans must never compare equal to integer versions or generations.
    return json.dumps(left, sort_keys=True, allow_nan=False) == json.dumps(
        right, sort_keys=True, allow_nan=False
    )


def _uuid(value):
    if not isinstance(value, str) or str(UUID(value)) != value:
        raise ValueError("Source completion UUID is unproven")


def _plan(plan):
    # The caller must also compare this entire plan with locked immutable SQL
    # intent. Shape validation alone is not source or target authority.
    if (
        not isinstance(plan, dict)
        or set(plan)
        != {
            "version",
            "kind",
            "disposition_id",
            "request_id",
            "job_id",
            "provision_generation",
            "request_digest",
            "controller_configuration_digest",
            "source",
            "target",
            "tombstone",
        }
        or type(plan["version"]) is not int
        or plan["version"] != 1
        or plan["kind"] != "source_disposition_planned"
    ):
        raise ValueError("Source completion intent is unproven")
    for key in ("disposition_id", "request_id", "job_id", "provision_generation"):
        _uuid(plan[key])
    for key in ("request_digest", "controller_configuration_digest"):
        if not isinstance(plan[key], str) or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", plan[key]
        ):
            raise ValueError("Source completion digest is unproven")
    source, tombstone = plan["source"], plan["tombstone"]
    if source is not None and not isinstance(source, dict):
        raise ValueError("Source completion source is unproven")
    if tombstone is not None:
        if (
            not source
            or source.get("kind") not in {"golden", "prepared"}
            or source.get("mode") == "retained"
        ):
            raise ValueError("Source completion source kind changed")
        pin = validate_disposed_pin(plan["request_id"], tombstone, source["dv_uid"])
        if (
            pin["pvc_uid"] != source["pvc_uid"]
            or not _same(pin["target"], plan["target"])
            or any(
                pin[key] != plan[key]
                for key in ("job_id", "provision_generation", "disposition_id")
            )
        ):
            raise ValueError("Source completion pin intent changed")
    return source


def validate_source_observation(plan, observation):
    """Validate already reduced facts; absence is valid only for a known old UID."""
    source = _plan(plan)
    if (
        plan["tombstone"] is None
        or not isinstance(observation, dict)
        or set(observation) != {"dv", "pvc", "pin"}
    ):
        raise ValueError("Source completion observation is unproven")
    for key in ("dv", "pvc"):
        fact = observation[key]
        if fact is not None:
            if not isinstance(fact, dict) or set(fact) != {"uid", "resource_version"}:
                raise ValueError("Source completion identity is unproven")
            _uuid(fact["uid"])
            if (
                not isinstance(fact["resource_version"], str)
                or not 0 < len(fact["resource_version"]) <= 256
            ):
                raise ValueError("Source completion resource version is unproven")
    dm = observation["dv"]
    gone = dm is None or dm["uid"] != source["dv_uid"]
    if not _same(observation["pin"], None if gone else plan["tombstone"]):
        raise ValueError("Source completion CAS is unproven")
    return "source_identity_gone" if gone else "pin_disposed"


def allocation_completion(plan, record):
    """Keep only allocation identity and its exact terminal receipt, no build data."""
    if record is None:
        raise ValueError("Source completion allocation is missing")
    if plan["source"]["kind"] == "preparation_never_delivered" and any(
        record.state.get(key) is not None
        for key in ("creation_source", "creation_root", "rootdisk", "rootdisk_uid")
    ):
        raise ValueError("Preparation non-delivery changed")
    return {
        "name": record.name,
        "uid": record.uid,
        "resource_version": record.version,
        "request": deepcopy(record.request),
        "phase": record.state.get("phase"),
        "creation_binding": deepcopy(record.state.get("creation_binding")),
        "workspace_source_issued": record.state.get("workspace_source_issued"),
        "receipt": deepcopy(record.state.get("creation_disposition")),
    }


def _allocation(plan, allocation):
    source = plan["source"]
    required = source and (
        source["kind"] == "preparation_never_delivered"
        or source["kind"] == "prepared"
        and source.get("mode") == "clone"
    )
    if not required:
        if allocation is not None:
            raise ValueError("Unexpected source completion allocation")
        return
    if not isinstance(allocation, dict) or set(allocation) != {
        "name",
        "uid",
        "resource_version",
        "request",
        "phase",
        "creation_binding",
        "workspace_source_issued",
        "receipt",
    }:
        raise ValueError("Source completion allocation is unproven")
    expected = source["allocation"]
    if (
        any(
            not _same(allocation[key], expected[key])
            for key in ("name", "uid", "request")
        )
        or allocation["phase"] != "Cancelled"
    ):
        raise ValueError("Source completion allocation changed")
    _uuid(allocation["uid"])
    if (
        not isinstance(allocation["resource_version"], str)
        or not 0 < len(allocation["resource_version"]) <= 256
    ):
        raise ValueError("Source completion allocation version is unproven")
    if source["kind"] == "preparation_never_delivered":
        if (
            allocation["workspace_source_issued"] is not False
            or not _same(
                allocation["creation_binding"],
                expected["state"].get("creation_binding"),
            )
            or not _same(
                allocation["receipt"],
                {"version": 1, "kind": "prepared_source_never_delivered", "plan": plan},
            )
            or type(allocation["receipt"]["version"]) is not int
        ):
            raise ValueError("Source non-delivery completion changed")
    else:
        receipt = allocation["receipt"]
        if (
            not isinstance(receipt, dict)
            or not _same(receipt.get("plan"), plan)
            or allocation["workspace_source_issued"] is not True
        ):
            raise ValueError("Prepared completion receipt changed")
        validate_prepared_disposition(
            receipt,
            request=allocation["request"],
            state={
                "creation_binding": allocation["creation_binding"],
                "creation_source": source,
            },
            allocation_uid=allocation["uid"],
        )


def validate_source_completion(plan, evidence):
    source = _plan(plan)
    if (
        not isinstance(evidence, dict)
        or set(evidence)
        != {"version", "kind", "outcome", "plan", "source_observation", "allocation"}
        or type(evidence["version"]) is not int
        or evidence["version"] != 1
        or evidence["kind"] != "source_disposition_completed"
        or not _same(evidence["plan"], plan)
    ):
        raise ValueError("Source completion receipt changed")
    if plan["tombstone"] is not None:
        outcome = validate_source_observation(plan, evidence["source_observation"])
    elif source and source.get("kind") == "preparation_never_delivered":
        outcome = "allocation_never_delivered"
    elif (
        source is None
        or source.get("kind") in {"registry", "retained"}
        or source.get("kind") == "prepared"
        and source.get("mode") == "retained"
    ) and plan["target"] is None:
        outcome = "not_required"
    else:
        raise ValueError("Source completion intent is incomplete")
    if (
        evidence["outcome"] != outcome
        or plan["tombstone"] is None
        and evidence["source_observation"] is not None
    ):
        raise ValueError("Source completion outcome changed")
    _allocation(plan, evidence["allocation"])
    if len(json.dumps(evidence, allow_nan=False).encode()) > MAX_COMPLETION_BYTES:
        raise ValueError("Source completion evidence exceeds its bound")
    return evidence


def object_identity(obj, source):
    if obj is None:
        return None
    metadata = obj.get("metadata") if isinstance(obj, dict) else None
    if (
        not isinstance(metadata, dict)
        or metadata.get("name") != source["name"]
        or metadata.get("namespace") != source["namespace"]
        or not isinstance(metadata.get("resourceVersion"), str)
        or not 0 < len(metadata["resourceVersion"]) <= 256
        or not isinstance(metadata.get("uid"), str)
        or str(UUID(metadata["uid"])) != metadata["uid"]
    ):
        raise ValueError("Source completion observation is unreadable")
    return {
        "uid": metadata["uid"],
        "resource_version": metadata["resourceVersion"],
    }


def source_completion(plan, *, dv=None, pvc=None, allocation=None):
    """Reduce authenticated exact readbacks to closed, credential-free evidence."""
    source = _plan(plan)
    if plan["tombstone"] is None:
        result = {
            "version": 1,
            "kind": "source_disposition_completed",
            "outcome": "allocation_never_delivered"
            if source and source.get("kind") == "preparation_never_delivered"
            else "not_required",
            "plan": deepcopy(plan),
            "source_observation": None,
            "allocation": allocation_completion(plan, allocation)
            if allocation is not None
            else None,
        }
        return validate_source_completion(plan, result)
    dm, pm = object_identity(dv, source), object_identity(pvc, source)
    gone = dm is None or dm["uid"] != source["dv_uid"]
    if not gone:
        observed_pins = json.loads(
            dv["metadata"].get("annotations", {}).get(SOURCE_PINS_ANNOTATION, "{}")
        )
        if not isinstance(observed_pins, dict) or not _same(
            observed_pins.get(plan["request_id"]), plan["tombstone"]
        ):
            raise ValueError("Source completion CAS is unproven")
    result = {
        "version": 1,
        "kind": "source_disposition_completed",
        "outcome": "source_identity_gone" if gone else "pin_disposed",
        "plan": deepcopy(plan),
        "source_observation": {
            "dv": dm,
            "pvc": pm,
            "pin": None if gone else deepcopy(plan["tombstone"]),
        },
        "allocation": allocation_completion(plan, allocation)
        if allocation is not None
        else None,
    }
    return validate_source_completion(plan, result)
