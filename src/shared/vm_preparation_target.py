"""Strict clone target facts; current create/instance authority remains separate."""

from collections.abc import Mapping
from copy import deepcopy
import re
from uuid import UUID

from shared.vm_workspace_storage import storage_binding, storage_name


def workspace_target(binding, namespace):
    binding = storage_binding(binding)
    return {
        "name": storage_name(binding),
        "namespace": namespace,
        "workspace_storage": binding,
    }


def validate_workspace_target(target, *, allocation_id, namespace=None, binding=None):
    if not isinstance(target, Mapping) or set(target) != {
        "name",
        "namespace",
        "workspace_storage",
    }:
        raise ValueError("Prepared target is incomplete")
    original = storage_binding(target["workspace_storage"])
    ns = target["namespace"]
    if (
        not isinstance(ns, str)
        or len(ns) > 63
        or not re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", ns)
        or namespace is not None
        and ns != namespace
        or target["name"] != storage_name(original)
        or original["owner_kind"] != "job"
        or original["owner_id"] != allocation_id
        or original["generation"] != 1
        or original["pvc_uid"] is not None
    ):
        raise ValueError("Prepared original clone target changed")
    if binding is not None:
        current = storage_binding(binding)
        if any(
            current[key] != original[key] for key in ("uid", "owner_kind", "owner_id")
        ):
            raise ValueError("Prepared current workspace target changed")
    return deepcopy(dict(target))


def creation_root_name(creation, request, *, namespace=None):
    if (
        not isinstance(creation, Mapping)
        or set(creation)
        not in (
            {"request_id", "provision_generation", "request_digest"},
            {"request_id", "provision_generation", "request_digest", "target"},
        )
        or request["ownerKind"] != "job"
    ):
        raise ValueError("Preparation creation binding is invalid")
    for key in ("request_id", "provision_generation"):
        if (
            not isinstance(creation[key], str)
            or str(UUID(creation[key])) != creation[key]
        ):
            raise ValueError("Preparation creation identity is invalid")
    if not isinstance(creation["request_digest"], str) or not re.fullmatch(
        r"sha256:[0-9a-f]{64}", creation["request_digest"]
    ):
        raise ValueError("Preparation request digest is invalid")
    if "target" not in creation:
        return "agent-vm-" + request["allocationId"] + "-rootdisk"
    target = validate_workspace_target(
        creation["target"], allocation_id=request["allocationId"], namespace=namespace
    )
    return target["name"]
