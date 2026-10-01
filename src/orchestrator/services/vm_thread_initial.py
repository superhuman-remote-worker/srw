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


def is_initial_thread_vm_poll(thread, vm):
    """Existing wake/restore generations retain their original delivery path."""
    if thread_metadata_object(thread).get("vm") is None:
        return True
    if vm.get("rootdisk") == "kept" or vm.get("idle_wake_operation_id") is not None:
        return False
    marker = vm.get("initial_runtime")
    if marker is None:
        return False
    try:
        generation = str(UUID(str(marker["runtime_generation"])))
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(409, "Initial VM runtime marker is malformed") from exc
    return generation == str(thread["runtime_generation"])


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
    marker = vm.get("initial_runtime")
    if not isinstance(marker, dict) or any(
        marker.get(key) != str(thread.get(key))
        for key in ("runtime_generation", "agent_id", "runtime_attach_token")
    ):
        raise HTTPException(409, "Initial VM runtime marker changed")
    if vm.get("creation_request_id") is None:
        from shared.vm_resource_policy import configured_enforcement_required

        # Preparation has no metadata.vm. Only the legacy one-shot hosting
        # mode may have a marked physical create without a retry source.
        if configured_enforcement_required():
            raise HTTPException(409, "Initial VM creation source is unavailable")
        return
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


async def initial_vm_startup_view(thread, *, store):
    """Read one current initial Session source and its durable admission clock.

    This hint changes only the agent's poll lifetime. The query neither claims
    the retry nor admits a reservation and cannot grant runtime authority.
    """
    metadata = thread_metadata_object(thread)
    vm = metadata.get("vm")
    if (
        thread.get("execution_lane") != "pinned"
        or thread.get("status") != "created"
        or thread.get("runtime_retirement_token") is not None
        or thread.get("pinned_idle_terminal_intent_at") is not None
        or not thread.get("agent_id")
        or not thread.get("runtime_attach_token")
        or not isinstance(vm, dict)
        or vm.get("rootdisk") == "kept"
        or vm.get("idle_wake_operation_id") is not None
        or not isinstance(vm.get("initial_runtime"), dict)
    ):
        return None
    try:
        runtime = UUID(str(thread["runtime_generation"]))
        agent = UUID(str(thread["agent_id"]))
        attach = UUID(str(thread["runtime_attach_token"]))
        request = UUID(str(vm["creation_request_id"]))
        provision = UUID(str(vm["provision_generation"]))
        thread_id = UUID(str(thread["id"]))
        owner = UUID(str(thread["user_id"])) if thread.get("user_id") else None
        project = UUID(str(thread["project_id"])) if thread.get("project_id") else None
    except (KeyError, TypeError, ValueError):
        return None
    marker = vm["initial_runtime"]
    if any(
        marker.get(key) != str(value)
        for key, value in (
            ("runtime_generation", runtime),
            ("agent_id", agent),
            ("runtime_attach_token", attach),
        )
    ):
        return None
    if any(
        str(vm[key]) != str(value)
        for key, value in (
            ("creation_request_id", request),
            ("provision_generation", provision),
        )
    ):
        return None
    row = await store.fetchrow(
        "SELECT r.request_id,r.provision_generation,r.state,r.reason,r.boot_counted,"
        "r.observed_vm_uid,r.observed_pvc_uid,r.creation_admission_id,"
        "w.state AS waiter_state,"
        "(SELECT min(v.created_at) FROM vm_resource_reservations v "
        "WHERE v.request_id=r.request_id) AS first_reservation_at,"
        "(SELECT count(*) FROM vm_creation_effects e WHERE e.request_id=r.request_id "
        "AND e.state<>'rejected') AS issued_effects,"
        "clock_timestamp() AS read_at "
        "FROM threads t JOIN agents a ON a.id=t.agent_id AND a.thread_id=t.id "
        "JOIN vm_creation_retries r ON r.request_id=$5 AND r.owner_kind='thread' "
        "AND r.thread_id=t.id AND r.origin='initial' "
        "AND r.thread_runtime_generation=t.runtime_generation "
        "AND r.thread_agent_id=t.agent_id AND r.thread_attach_token=t.runtime_attach_token "
        "AND r.provision_generation=$6 AND r.thread_wake_operation_id IS NULL "
        "AND r.thread_owner_user_id IS NOT DISTINCT FROM t.user_id "
        "AND r.thread_owner_project_id IS NOT DISTINCT FROM t.project_id "
        "JOIN vm_resource_waiters w ON w.request_id=r.request_id "
        "AND w.owner_kind='thread' AND w.thread_id=t.id AND w.job_id IS NULL "
        "AND w.provision_generation=r.provision_generation "
        "AND w.request_digest=r.request_digest "
        "AND w.owner_key=CASE WHEN t.user_id IS NULL THEN 'system' "
        "ELSE 'user:'||t.user_id::text END "
        "AND w.project_id IS NOT DISTINCT FROM t.project_id "
        "WHERE t.id=$1 AND t.runtime_generation=$2 AND t.agent_id=$3 "
        "AND t.runtime_attach_token=$4 AND t.user_id IS NOT DISTINCT FROM $7::uuid "
        "AND t.project_id IS NOT DISTINCT FROM $8::uuid "
        "AND t.execution_lane='pinned' AND t.status='created' "
        "AND t.runtime_retirement_token IS NULL "
        "AND t.pinned_idle_terminal_intent_at IS NULL "
        "AND t.metadata->'vm'->>'creation_request_id'=$5::text "
        "AND t.metadata->'vm'->>'provision_generation'=$6::text "
        "AND t.metadata->'vm'->'initial_runtime'->>'runtime_generation'=$2::text "
        "AND t.metadata->'vm'->'initial_runtime'->>'agent_id'=$3::text "
        "AND t.metadata->'vm'->'initial_runtime'->>'runtime_attach_token'=$4::text",
        thread_id,
        runtime,
        agent,
        attach,
        request,
        provision,
        owner,
        project,
    )
    if row is None or row["state"] in {"attention", "cancel_requested", "settled"}:
        return None
    identity = {
        "contract_version": 1,
        "request_id": str(request),
        "provision_generation": str(provision),
        "runtime_generation": str(runtime),
    }
    if row["first_reservation_at"] is not None:
        if row["waiter_state"] != "admitted":
            return None
        return {
            **identity,
            "phase": "admitted",
            "admission_elapsed_s": max(
                0.0, (row["read_at"] - row["first_reservation_at"]).total_seconds()
            ),
        }
    if (
        row["state"] in {"queued", "reconciling"}
        and row["reason"] == "resource_wait"
        and row["waiter_state"] == "waiting"
        and not row["boot_counted"]
        and row["observed_vm_uid"] is None
        and row["observed_pvc_uid"] is None
        and row["creation_admission_id"] is None
        and row["issued_effects"] == 0
        and not vm.get("vm_uid")
        and not vm.get("rootdisk_pvc_uid")
    ):
        return {**identity, "phase": "resource_wait"}
    return None


def initial_vm_wait_payload(thread, *, startup_view=None):
    """Only readiness and the already-bound runtime contract cross this poll."""
    metadata = thread_metadata_object(thread)
    context = metadata.get("workspace_preparation") or metadata.get("vm") or {}
    status = str(context.get("status") or "provisioning")
    payload = {
        "status": "failed" if status == "failed" else "creating",
        "vm_status": status,
        "pinned_status_identity_contract": 1,
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": str(thread["runtime_generation"]),
        "config_override": {"workspace": {"backend": "vm"}},
        "protected_cloud": False,
        "protected_cloud_state": None,
    }
    if startup_view is not None:
        payload["vm_startup"] = startup_view
    return payload
