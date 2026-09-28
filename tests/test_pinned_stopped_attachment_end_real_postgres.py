"""Normal End of an exact stopped pinned agent remains a guarded retirement."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from orchestrator import main
from orchestrator.application import controls
from orchestrator.services import agent_provisioner as agent_module
from orchestrator.services import container_provisioner as container_module
from orchestrator.services.container_provisioner import WorkspaceTeardownIdentity
from shared.runtime.core.backends.remote import RemoteBackend
from tests import test_persistent_recycler_real_postgres as fixtures


db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied


async def _stopped_owner(db, monkeypatch):
    ids = await fixtures._seed(db, protected_agent_pod=True, workspace_claim=False)
    thread = await db.get_thread(ids["thread"])
    metadata = fixtures._json(thread["metadata"])
    metadata["config_override"]["officer"] = {"enabled": False}
    metadata["config_override"]["workspace"] = {"backend": "sandbox"}
    workspace_generation = str(uuid4())
    metadata["workspace_container"] = {
        "status": "ready",
        "provisioner": "k8s",
        "pod_ip": "10.42.0.25",
        "host": "ws-thread.example.svc",
        "port": 30022,
        "_canvas_workspace_generation": workspace_generation,
        "_runtime_incarnation": str(uuid4()),
    }
    metadata["_workspace_binding"] = {
        "generation": workspace_generation,
        "kind": "remote",
        "backing_id": f"k8s-pvc:{uuid4()}",
        "ssh_host_key_fingerprint": "SHA256:" + "A" * 43,
    }
    await db.execute(
        "UPDATE threads SET status='created', config_name='assistant', "
        "metadata=$2::jsonb "
        "WHERE id=$1::uuid",
        ids["thread"],
        json.dumps(metadata),
    )
    await db.execute(
        "UPDATE agents SET status='offline' WHERE id=$1::uuid", ids["agent"]
    )
    api = fixtures.StatefulPinnedK8sApi()
    pod_name = f"persistent-{ids['thread'][:12]}"
    api.install_old_pod(
        namespace="agents-a",
        name=pod_name,
        uid="old-pod",
        labels={"srw/component": "persistent-agent", "srw/thread-id": ids["thread"]},
    )
    api.mark_terminal("agents-a", pod_name)
    provider = fixtures._production_warm_provisioner(db, api, namespace="agents-a")
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(agent_module, "agent_provisioner", provider)
    return ids, api, provider, pod_name


async def _end(db, ids):
    return await controls.thread_retirement_operations(
        main.app.state.resources
    ).end_thread_flow(
        ids["thread"],
        dict(await db.get_thread(ids["thread"])),
        permanent=False,
        force=False,
    )


@pytest.mark.asyncio
async def test_stopped_exact_agent_allows_normal_end_to_close_admission(
    db, monkeypatch
):
    ids, _api, _provider, _pod_name = await _stopped_owner(db, monkeypatch)
    result = await _end(db, ids)
    assert result["status"] == "ending"
    pending = await db.get_thread(ids["thread"])
    assert pending["runtime_retirement_token"] is not None
    assert pending["runtime_retirement_authorized_at"] is not None
    assert pending["runtime_retirement_local_quiescence"] is None


@pytest.mark.asyncio
async def test_stopped_agent_normal_end_can_recover_exact_workspace_zero(
    db, monkeypatch
):
    ids, api, _provider, pod_name = await _stopped_owner(db, monkeypatch)
    assert (await _end(db, ids))["status"] == "ending"
    pending = await db.get_thread(ids["thread"])
    retirement = {
        "generation": str(pending["runtime_generation"]),
        "token": str(pending["runtime_retirement_token"]),
        "permanent": False,
        "context": fixtures._json(pending["runtime_retirement_context"]),
    }
    monkeypatch.setattr(
        controls.services, "resolve_ssh_key_path", lambda: "/tmp/unused"
    )
    zero_calls = []

    def _strict_zero(remote):
        zero_calls.append((remote._workspace_generation, remote._runtime_incarnation))
        return "workspace_process_zero_v1"

    monkeypatch.setattr(
        RemoteBackend, "protected_workspace_zero_cleanup_strict", _strict_zero
    )
    monkeypatch.setattr(RemoteBackend, "disconnect", lambda _remote: None)
    assert await controls.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    current = await db.get_thread(ids["thread"])
    receipt = fixtures._json(current["runtime_retirement_local_quiescence"])
    assert receipt["quiescence_protocol"] == "workspace_process_zero_v1"
    assert receipt["runtime_generation"] == retirement["generation"]
    assert receipt["retirement_token"] == retirement["token"]
    assert zero_calls == [
        (
            retirement["context"]["workspace_binding"]["generation"],
            retirement["context"]["workspace_container"]["_runtime_incarnation"],
        )
    ]
    assert ("agents-a", pod_name) not in api.pods
    workspace = retirement["context"]["workspace_container"]
    backing_uid = retirement["context"]["workspace_binding"]["backing_id"].rsplit(
        ":", 1
    )[-1]
    container = MagicMock(is_available=True)
    container.capture_workspace_teardown_identity = AsyncMock(
        return_value=WorkspaceTeardownIdentity(
            pod_uid=workspace["_runtime_incarnation"],
            pvc_uid=backing_uid,
            service_uid=str(uuid4()),
        )
    )
    container.release_workspace = AsyncMock(return_value=True)
    monkeypatch.setattr(container_module, "container_provisioner", container)
    monkeypatch.setattr(
        main.app.state.resources.session_router,
        "teardown_route",
        AsyncMock(return_value=True),
    )
    result = await _end(db, ids)
    assert result["status"] == "ended"
    assert (await db.get_thread(ids["thread"]))["status"] == "ended"
    assert container.release_workspace.await_args.kwargs["reclaim_volume"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "observation",
    [
        "live",
        "restarting",
        "incomplete",
        "absent",
        "replacement",
        "missing_finalizer",
        "unknown",
    ],
)
async def test_normal_end_refuses_without_exact_protected_terminal_pod(
    db, monkeypatch, observation
):
    ids, api, provider, pod_name = await _stopped_owner(db, monkeypatch)
    pod = api.pods[("agents-a", pod_name)]
    if observation == "live":
        api.mark_ready("agents-a", pod_name)
    elif observation == "restarting":
        pod.status.phase = "Running"
    elif observation == "incomplete":
        pod.status.container_statuses = []
    elif observation == "absent":
        del api.pods[("agents-a", pod_name)]
    elif observation == "replacement":
        pod.metadata.uid = "successor-pod"
    elif observation == "missing_finalizer":
        pod.metadata.finalizers = []
    elif observation == "unknown":
        provider.observe_agent_pod_exact = AsyncMock(side_effect=TimeoutError())

    with pytest.raises(HTTPException) as refused:
        await _end(db, ids)
    assert refused.value.status_code == 409
    assert refused.value.detail["code"] == "turn_in_flight"
    current = await db.get_thread(ids["thread"])
    assert current["runtime_retirement_token"] is None
    assert current["runtime_retirement_authorized_at"] is None


@pytest.mark.asyncio
async def test_normal_end_refuses_nonreciprocal_bound_agent(db, monkeypatch):
    ids, _api, _provider, _pod_name = await _stopped_owner(db, monkeypatch)
    # Model a broken legacy reciprocal row that the normal writer triggers
    # forbid. The End fallback must still not infer authority from the Pod.
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(
                "UPDATE agents SET thread_id=NULL WHERE id=$1::uuid",
                UUID(ids["agent"]),
            )
    with pytest.raises(HTTPException) as refused:
        await _end(db, ids)
    assert refused.value.detail["code"] == "turn_in_flight"
    assert (await db.get_thread(ids["thread"]))["runtime_retirement_token"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["runtime_generation", "runtime_attach_token"])
async def test_normal_end_refuses_stale_caller_runtime_tuple(db, monkeypatch, changed):
    ids, _api, _provider, _pod_name = await _stopped_owner(db, monkeypatch)
    stale = dict(await db.get_thread(ids["thread"]))
    stale[changed] = uuid4()
    with pytest.raises(HTTPException) as refused:
        await controls.thread_retirement_operations(
            main.app.state.resources
        ).end_thread_flow(ids["thread"], stale, permanent=False, force=False)
    assert refused.value.status_code == 409
    assert (await db.get_thread(ids["thread"]))["runtime_retirement_token"] is None


@pytest.mark.asyncio
async def test_normal_end_rechecks_agent_tuple_after_terminal_observation(
    db, monkeypatch
):
    ids, _api, _provider, _pod_name = await _stopped_owner(db, monkeypatch)
    original_get_agent = db.get_agent
    reads = 0

    async def _changed_second_read(agent_id):
        nonlocal reads
        agent = await original_get_agent(agent_id)
        reads += 1
        if reads == 2:
            return {**agent, "pod_uid": "different-pod"}
        return agent

    monkeypatch.setattr(db, "get_agent", _changed_second_read)
    with pytest.raises(HTTPException) as refused:
        await _end(db, ids)
    assert reads >= 2
    assert refused.value.detail["code"] == "turn_in_flight"
    assert (await db.get_thread(ids["thread"]))["runtime_retirement_token"] is None
