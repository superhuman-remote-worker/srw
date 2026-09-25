"""Immutable network lineage for retained pinned Session disks."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from uuid import UUID

from shared.vm_creation_issuance import canonical_configuration_digest
from shared.vm_creation_retry import canonical_request_digest
from shared.vm_network_profile import (
    compatible_image,
    reusable_profile_evidence,
    validate_network_profile,
)


def document(value: object) -> dict | None:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return None
    return dict(value) if isinstance(value, Mapping) else None


def profile_enabled() -> bool:
    return os.getenv("VM_NETWORK_PROFILE_ENABLED", "false").lower() == "true"


def verified_source(
    source: Mapping | None,
    *,
    thread_id: str,
    generation: str,
    request_id: str | None = None,
) -> dict | None:
    """Return a complete frozen thread request, never a mutable VM subdocument."""
    if source is None:
        return None
    request = document(source["canonical_request"])
    configuration = document(source["controller_configuration"])
    if request is None or configuration is None:
        return None
    try:
        profile = request.get("network_profile")
        if profile is not None:
            validate_network_profile(profile)
        valid = (
            source["owner_kind"] == "thread"
            and str(source["thread_id"]) == thread_id
            and str(source["provision_generation"]) == generation
            and (request_id is None or str(source["request_id"]) == request_id)
            and request.get("entity_type") == "thread"
            and request.get("job_id") == thread_id
            and request.get("provision_generation") == generation
            and canonical_request_digest(request) == source["request_digest"]
            and configuration.get("version") == 3
            and canonical_configuration_digest(configuration)
            == source["controller_configuration_digest"]
            and (profile is None or request.get("preparation") is None)
            and (
                configuration.get("network_profile_policy") is None
                if profile is None
                else configuration.get("network_profile_policy")
                == {"version": 1, "image": request.get("vm_image"), "profile": profile}
                # This checks immutable image shape, not fresh admission. The
                # owner transaction applies selected_profile under its lock.
                and compatible_image(
                    request.get("vm_image"), allowlist=request.get("vm_image")
                )
            )
        )
    except (KeyError, TypeError, ValueError):
        return None
    return request if valid else None


async def inherited_profile_on_conn(
    conn,
    *,
    thread_id: UUID,
    operation: Mapping,
    vm: Mapping,
) -> tuple[bool, dict | None, str | None]:
    """Check predecessor, original source, and exact previous guest receipt."""
    predecessor = await conn.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE owner_kind='thread' "
        "AND thread_id=$1 AND provision_generation=$2 FOR SHARE",
        thread_id,
        operation["provision_generation"],
    )
    if predecessor is None:
        return (
            (not profile_enabled() and vm.get("network_profile_evidence") is None),
            None,
            None,
        )
    request = verified_source(
        predecessor,
        thread_id=str(thread_id),
        generation=str(operation["provision_generation"]),
    )
    if (
        request is None
        or predecessor["state"] != "succeeded"
        or predecessor["thread_runtime_generation"]
        != operation["thread_runtime_generation"]
        or predecessor["observed_pvc_uid"] != operation["pvc_uid"]
        or predecessor["observed_vm_uid"] != operation["vm_uid"]
    ):
        return False, None, None
    profile = request.get("network_profile")
    if profile is None:
        return (
            (not profile_enabled() and vm.get("network_profile_evidence") is None),
            None,
            None,
        )
    originals = await conn.fetch(
        "SELECT * FROM vm_creation_retries WHERE owner_kind='thread' "
        "AND thread_id=$1 AND expected_pvc_uid IS NULL "
        "AND observed_pvc_uid=$2 FOR SHARE",
        thread_id,
        operation["pvc_uid"],
    )
    if len(originals) != 1:
        return False, None, None
    original = originals[0]
    first = verified_source(
        original,
        thread_id=str(thread_id),
        generation=str(original["provision_generation"]),
    )
    if (
        first is None
        or original["state"] != "succeeded"
        or first.get("network_profile") != profile
        or first.get("vm_image") != request.get("vm_image")
        or first.get("preparation") is not None
    ):
        return False, None, None
    chain = await conn.fetch(
        "SELECT * FROM vm_creation_retries WHERE owner_kind='thread' "
        "AND thread_id=$1 AND (expected_pvc_uid=$2 OR observed_pvc_uid=$2) "
        "FOR SHARE",
        thread_id,
        operation["pvc_uid"],
    )
    if any(
        (
            item_request := verified_source(
                item,
                thread_id=str(thread_id),
                generation=str(item["provision_generation"]),
            )
        )
        is None
        or item_request.get("network_profile") != profile
        or item_request.get("vm_image") != first.get("vm_image")
        for item in chain
    ):
        return False, None, None
    if (
        not reusable_profile_evidence(
            vm.get("network_profile_evidence"),
            profile,
            provision_generation=str(operation["provision_generation"]),
            vm_uid=str(operation["vm_uid"]),
            pvc_uid=str(operation["pvc_uid"]),
            vmi_uid=str(operation["vmi_uid"]),
            launcher_uid=str(operation["launcher_uid"]),
            interface_mac=vm.get("interface_mac"),
        )
        or not isinstance(vm.get("interface_mac"), str)
        or not vm["interface_mac"]
    ):
        return False, None, None
    return True, profile, first["vm_image"]


async def successor_profile_on_conn(
    conn,
    *,
    thread_id: UUID,
    operation: Mapping,
    vm: Mapping,
) -> bool:
    source = await conn.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1 "
        "AND owner_kind='thread' AND thread_id=$2 FOR SHARE",
        operation["wake_request_id"],
        thread_id,
    )
    if source is None:
        return not profile_enabled() and vm.get("network_profile_evidence") is None
    request = verified_source(
        source,
        thread_id=str(thread_id),
        generation=str(operation["wake_generation"]),
        request_id=str(operation["wake_request_id"]),
    )
    if (
        request is None
        or source["thread_wake_operation_id"] != operation["id"]
        or source["expected_pvc_uid"] != operation["pvc_uid"]
        or source["state"] != "succeeded"
        or str(source["observed_pvc_uid"]) != vm.get("rootdisk_pvc_uid")
        or str(source["observed_vm_uid"]) != vm.get("vm_uid")
    ):
        return False
    profile = request.get("network_profile")
    if profile is None:
        return not profile_enabled() and vm.get("network_profile_evidence") is None
    return (
        isinstance(vm.get("interface_mac"), str)
        and bool(vm["interface_mac"])
        and reusable_profile_evidence(
            vm.get("network_profile_evidence"),
            profile,
            provision_generation=str(operation["wake_generation"]),
            vm_uid=vm.get("vm_uid"),
            pvc_uid=str(operation["pvc_uid"]),
            vmi_uid=vm.get("vmi_uid"),
            launcher_uid=vm.get("active_pod_uid"),
            interface_mac=vm["interface_mac"],
        )
    )
