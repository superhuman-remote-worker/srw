"""Admit a fresh pinned VM only after its exact agent has bound."""

from uuid import UUID

from fastapi import HTTPException

from orchestrator.services.manifest_execution_snapshot import read_execution
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from orchestrator.services.vm_workspace_config import vm_provisioning_options
from orchestrator.services.vm_workspace_policy import (
    VmPermissionDependencies,
    check_vm_permission,
)


async def ensure_initial_thread_vm(thread, *, store, provisioner):
    """Called after the workspace poll has authorized its exact actor and grants.

    The database admission rechecks this snapshot and fresh-only lineage under
    the owner row lock. In particular, no lifecycle advisory lock is acquired
    under the caller's datasource lock.
    """
    metadata = thread_metadata_object(thread)
    if (
        thread.get("execution_lane") != "pinned"
        or thread.get("status") != "created"
        or thread.get("agent_id") is None
        or thread.get("runtime_attach_token") is None
        or thread.get("runtime_retirement_token") is not None
        or thread.get("execution_harness_adapter") != "srw/v1"
        or metadata.get("vm") is not None
    ):
        raise HTTPException(409, "Initial VM creation authority is unavailable")
    snapshot = await read_execution(store, "Session", str(thread["id"]))
    if snapshot is None:
        raise HTTPException(409, "Initial VM execution is unavailable")
    user = (
        await store.get_user(str(thread["user_id"])) if thread.get("user_id") else None
    )
    await check_vm_permission(
        user,
        job_needs_vm=True,
        dependencies=VmPermissionDependencies(store=store),
    )
    options = await vm_provisioning_options(store, "Session", thread)
    stage = metadata.get("workspace_preparation")
    if stage is not None and (
        not isinstance(stage, dict)
        or stage.get("preparation_request") != options.get("preparation")
        or not options.get("preparation")
    ):
        raise HTTPException(409, "Initial VM preparation authority changed")
    accepted = await provisioner.create_thread_vm(
        thread_id=str(thread["id"]),
        agent_config=thread["config_name"],
        **options,
        expected_runtime_generation=str(thread["runtime_generation"]),
        expected_agent_id=str(thread["agent_id"]),
        expected_attach_token=str(thread["runtime_attach_token"]),
        expected_vm_context=None,
        initial_creation={
            "execution_id": str(snapshot["id"]),
            "execution_revision": snapshot["revision"],
            "execution_generation": snapshot["generation"],
        },
    )
    # A lost admission reply or a simultaneous poll may have installed the
    # exact source. Re-read, but never recapture a different actor or G.
    current = await store.get_thread(str(thread["id"]))
    if (
        current is None
        or any(
            current.get(key) != thread.get(key)
            for key in ("runtime_generation", "agent_id", "runtime_attach_token")
        )
        or current.get("runtime_retirement_token") is not None
    ):
        raise HTTPException(409, "Initial VM runtime changed during admission")
    vm = thread_metadata_object(current).get("vm")
    if isinstance(vm, dict) and vm.get("creation_request_id"):
        await require_current_initial_vm_source(current, store=store)
        return current
    if accepted and (
        isinstance(vm, dict)
        or isinstance(
            thread_metadata_object(current).get("workspace_preparation"), dict
        )
    ):
        return current
    raise HTTPException(503, "Initial VM creation is pending")


async def require_current_initial_vm_source(thread, *, store):
    """Pending delivery cannot bless an immutable source for another actor."""
    metadata = thread_metadata_object(thread)
    vm = metadata.get("vm")
    if metadata.get("protected_cloud") not in (None, False) or not isinstance(vm, dict):
        raise HTTPException(409, "Initial VM authority is unavailable")
    if vm.get("creation_request_id") is not None:
        try:
            request_id = UUID(str(vm["creation_request_id"]))
            generation = UUID(str(vm["provision_generation"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(409, "Initial VM source is malformed") from exc
        source = await store.fetchrow(
            "SELECT thread_runtime_generation,thread_agent_id,thread_attach_token,provision_generation "
            "FROM vm_creation_retries WHERE request_id=$1 AND owner_kind='thread' "
            "AND thread_id=$2",
            request_id,
            UUID(str(thread["id"])),
        )
        if source is None or not all(
            (
                source["thread_runtime_generation"] == thread["runtime_generation"],
                source["thread_agent_id"] == thread["agent_id"],
                source["thread_attach_token"] == thread["runtime_attach_token"],
                source["provision_generation"] == generation,
            )
        ):
            raise HTTPException(409, "Initial VM source runtime changed")


def initial_vm_wait_payload(thread):
    """Only readiness and the already-bound runtime contract cross this poll."""
    metadata = thread_metadata_object(thread)
    context = metadata.get("workspace_preparation") or metadata.get("vm") or {}
    status = str(context.get("status") or "provisioning")
    return {
        "status": "failed" if status == "failed" else "creating",
        "vm_status": status,
        "pinned_status_identity_contract": 1,
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": str(thread["runtime_generation"]),
        "config_override": {"workspace": {"backend": "vm"}},
        "protected_cloud": False,
        "protected_cloud_state": None,
    }
