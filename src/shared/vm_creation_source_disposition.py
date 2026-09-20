"""Immutable source-disposition intentions, explicitly not completion receipts."""

from copy import deepcopy
import re
from uuid import UUID

from shared.vm_creation_issuance import validate_rootdisk_source
from shared.vm_workspace_storage import storage_name


def source_disposition_plan(row, disposition, source, target):
    request = row["canonical_request"]
    if source is not None and source.get("kind") == "preparation_never_delivered":
        validate_never_delivered_source(source, row)
    elif source is not None:
        validate_rootdisk_source(
            source,
            request=request,
            configuration=row["controller_configuration"],
            expected_pvc_uid=str(row["expected_pvc_uid"])
            if row["expected_pvc_uid"]
            else None,
        )
    plan = {
        "version": 1,
        "kind": "source_disposition_planned",
        "disposition_id": disposition["disposition_id"],
        "request_id": str(row["request_id"]),
        "job_id": str(row["job_id"]),
        "provision_generation": str(row["provision_generation"]),
        "request_digest": row["request_digest"],
        "controller_configuration_digest": row["controller_configuration_digest"],
        "source": deepcopy(source),
        "target": deepcopy(target),
        "tombstone": None,
    }
    if (
        source
        and source["kind"] in {"golden", "prepared"}
        and source.get("mode") != "retained"
    ):
        binding = request.get("workspace_storage")
        plan["tombstone"] = {
            "version": 1,
            "state": "disposed",
            "job_id": plan["job_id"],
            "provision_generation": plan["provision_generation"],
            "disposition_id": plan["disposition_id"],
            "dv_uid": source["dv_uid"],
            "pvc_uid": source["pvc_uid"],
            "rootdisk_name": storage_name(binding)
            if binding
            else "agent-vm-" + plan["job_id"] + "-rootdisk",
            "target": deepcopy(target),
        }
        validate_disposed_pin(plan["request_id"], plan["tombstone"], source["dv_uid"])
    return plan


def completed_source_target(disposition, observation):
    from shared.vm_creation_issuance import public_effect_observation

    if (
        not isinstance(observation, dict)
        or observation.get("outcome") != "observed"
        or any(not isinstance(observation.get(key), dict) for key in ("object", "pvc"))
    ):
        raise ValueError("Retained clone observation is unproven")
    effect = next(
        effect
        for effect in disposition["effects"]
        if effect["effect_kind"] == "rootdisk" and effect["state"] == "observed"
    )
    root = disposition["objects"]["rootdisk"]
    evidence = public_effect_observation(
        effect["carrier_intent"],
        {"metadata": {"namespace": disposition["namespace"]}},
        observation,
    )
    if (
        evidence != root
        or observation["object"].get("status", {}).get("phase") != "Succeeded"
        or observation["pvc"].get("status", {}).get("phase") != "Bound"
    ):
        raise ValueError("Retained clone completion is unproven")
    return {
        "kind": "rootdisk_completed",
        **{key: root[key] for key in ("name", "namespace", "uid", "pvc_uid")},
    }


def validate_never_delivered_source(source, row):
    from shared.workspace_preparation import revision
    from shared.vm_preparation_target import workspace_target

    request = row["canonical_request"]["preparation"]
    binding = {
        key: str(row[key])
        for key in ("request_id", "provision_generation", "request_digest")
    }
    workspace = row["canonical_request"].get("workspace_storage")
    if workspace is not None:
        binding["target"] = workspace_target(
            workspace, row["controller_configuration"]["namespace"]
        )
    if (
        not isinstance(source, dict)
        or set(source) != {"kind", "allocation"}
        or source["kind"] != "preparation_never_delivered"
    ):
        raise ValueError("Preparation non-delivery identity is unproven")
    allocation = source["allocation"]
    if (
        not isinstance(allocation, dict)
        or set(allocation) != {"name", "uid", "resource_version", "request", "state"}
        or allocation["request"] != request
        or allocation["name"]
        != "srw-prep-allocation-"
        + revision(
            [
                request["ownerKind"],
                request["allocationId"],
                request.get("runtimeGeneration"),
            ]
        )[:32]
        or str(UUID(allocation["uid"])) != allocation["uid"]
        or not isinstance(allocation["resource_version"], str)
        or not allocation["resource_version"]
    ):
        raise ValueError("Preparation non-delivery allocation changed")
    state = allocation["state"]
    if (
        not isinstance(state, dict)
        or state.get("workspace_source_issued") is not False
        or state.get("creation_binding") not in (None, binding)
        or any(
            state.get(key) is not None
            for key in (
                "creation_source",
                "creation_root",
                "rootdisk",
                "rootdisk_uid",
                "creation_disposition",
            )
        )
    ):
        raise ValueError("Preparation source non-delivery is unproven")
    return binding


def validate_prepared_disposition(receipt, *, request, state, allocation_uid):
    """Match the full engine allocation/source to an externally read-back pin."""
    from shared.vm_preparation_target import creation_root_name

    if (
        not isinstance(receipt, dict)
        or set(receipt) != {"version", "kind", "plan", "source_resource_version"}
        or type(receipt["version"]) is not int
        or receipt["version"] != 1
        or receipt["kind"] != "prepared_source_disposed"
        or not isinstance(receipt["source_resource_version"], str)
        or not receipt["source_resource_version"]
    ):
        raise ValueError("Prepared source disposition is unproven")
    plan = receipt["plan"]
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
        raise ValueError("Prepared source disposition intent is unproven")
    source, binding = plan["source"], state.get("creation_binding")
    if any(
        not isinstance(plan[key], str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", plan[key])
        for key in ("request_digest", "controller_configuration_digest")
    ):
        raise ValueError("Prepared source disposition digest is unproven")
    if (
        source != state.get("creation_source")
        or not isinstance(source, dict)
        or source.get("kind") != "prepared"
        or source.get("mode") != "clone"
        or source["allocation"]["uid"] != allocation_uid
        or source["allocation"]["request"] != request
        or request["allocationId"] != plan["job_id"]
        or not isinstance(binding, dict)
        or any(
            binding.get(key) != plan[key]
            for key in ("request_id", "provision_generation", "request_digest")
        )
    ):
        raise ValueError("Prepared source disposition allocation changed")
    root = creation_root_name(binding, request, namespace=source["namespace"])
    pin = validate_disposed_pin(plan["request_id"], plan["tombstone"], source["dv_uid"])
    if (
        pin["rootdisk_name"] != root
        or pin["pvc_uid"] != source["pvc_uid"]
        or pin["target"] != plan["target"]
        or any(
            pin[key] != plan[key]
            for key in ("job_id", "provision_generation", "disposition_id")
        )
    ):
        raise ValueError("Prepared source disposition proof changed")
    return receipt


def validate_disposed_pin(request_id, pin, dv_uid):
    fields = {
        "version",
        "state",
        "job_id",
        "provision_generation",
        "disposition_id",
        "dv_uid",
        "pvc_uid",
        "rootdisk_name",
        "target",
    }
    if (
        not isinstance(pin, dict)
        or set(pin) != fields
        or type(pin["version"]) is not int
        or pin["version"] != 1
        or pin["state"] != "disposed"
        or pin["dv_uid"] != dv_uid
    ):
        raise ValueError("Source disposition pin is unproven")
    for value in (
        request_id,
        *(
            pin[key]
            for key in (
                "job_id",
                "provision_generation",
                "disposition_id",
                "dv_uid",
                "pvc_uid",
            )
        ),
    ):
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError("Source disposition identity is unproven")
    target = pin["target"]
    if pin["rootdisk_name"] != "agent-vm-" + pin[
        "job_id"
    ] + "-rootdisk" and not re.fullmatch(r"srw-ws-[0-9a-f]{32}", pin["rootdisk_name"]):
        raise ValueError("Source disposition root name is unproven")
    if (
        not isinstance(target, dict)
        or target.get("name") != pin["rootdisk_name"]
        or not isinstance(target.get("namespace"), str)
        or not target["namespace"]
    ):
        raise ValueError("Source disposition target is unproven")
    if target.get("kind") == "rootdisk_never_issued":
        if set(target) != {"kind", "name", "namespace"}:
            raise ValueError("Source non-issuance target changed")
    elif target.get("kind") == "rootdisk_purged":
        if (
            set(target)
            != {
                "version",
                "disposition_id",
                "kind",
                "namespace",
                "name",
                "uid",
                "pvc_uid",
                "admission_id",
                "request_id",
                "intent_digest",
            }
            or target["disposition_id"] != pin["disposition_id"]
            or type(target["version"]) is not int
            or target["version"] != 1
        ):
            raise ValueError("Source purge target changed")
        for field in ("uid", "pvc_uid", "admission_id", "request_id"):
            if str(UUID(target[field])) != target[field]:
                raise ValueError("Source purge identity is unproven")
        if not isinstance(target["intent_digest"], str) or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", target["intent_digest"]
        ):
            raise ValueError("Source purge digest is unproven")
    elif target.get("kind") == "rootdisk_completed":
        if set(target) != {"kind", "name", "namespace", "uid", "pvc_uid"} or any(
            str(UUID(target[key])) != target[key] for key in ("uid", "pvc_uid")
        ):
            raise ValueError("Source completed clone identity is unproven")
    else:
        raise ValueError("Source target disposition is unsupported")
    return pin
