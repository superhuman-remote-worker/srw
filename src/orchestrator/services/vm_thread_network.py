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


async def confirmed_pre_setup_source(
    store,
    *,
    thread_id,
    request_id,
    generation,
    runtime_generation,
    agent_id,
    attach_token,
) -> bool:
    """Admit only the original source's exact confirmed pre-setup successor.

    This proves creation lineage, never guest quiescence. Current binding and
    physical source must still match before writes and final Ready publication.
    The existing abort proof retains its published Pod UID and adoption guards.
    """
    try:
        values = (
            UUID(str(thread_id)),
            UUID(str(request_id)),
            UUID(str(generation)),
            UUID(str(runtime_generation)),
            UUID(str(agent_id)) if agent_id is not None else None,
            UUID(str(attach_token)) if attach_token is not None else None,
        )
    except (ValueError, TypeError, AttributeError):
        return False
    return (
        await store.fetchval(
            "SELECT EXISTS(SELECT 1 FROM threads t JOIN vm_creation_retries c "
            "ON c.thread_id=t.id AND c.owner_kind='thread' "
            "WHERE t.id=$1 AND c.request_id=$2 AND c.provision_generation=$3 "
        "AND t.execution_lane='pinned' AND t.runtime_generation=$4 "
        "AND t.status<>'ended' AND t.ended_at IS NULL "
            "AND t.agent_id IS NOT DISTINCT FROM $5::uuid "
            "AND t.runtime_attach_token IS NOT DISTINCT FROM $6::uuid "
            "AND t.runtime_retirement_token IS NULL "
            "AND t.pinned_idle_terminal_intent_at IS NULL "
            "AND t.metadata->'vm'->>'creation_request_id'=c.request_id::text "
            "AND t.metadata->'vm'->>'provision_generation'=c.provision_generation::text "
            "AND t.metadata->'vm'->>'vm_uid'=c.observed_vm_uid::text "
            "AND t.metadata->'vm'->>'rootdisk_pvc_uid'=c.observed_pvc_uid::text "
            "AND public.vm_thread_creation_pre_setup_abort_evidence(t,c) IS NOT NULL)",
            *values,
        )
        is True
    )


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
