"""Exact live-actor drain handoffs nominate only the captured VM retirement."""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import MagicMock
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
import httpx
from fastapi import FastAPI, HTTPException

from orchestrator import main
from orchestrator.application import controls as composition
from orchestrator.services import agent_provisioner as agent_provider_module
from orchestrator.services import stale_agent_detector as detector
from orchestrator.services import vm_provisioner as vm_provider_module
from orchestrator.services.agent_provisioner import AgentProvisioner
from orchestrator.services.session_router import SessionRouterService
from orchestrator.services.vm_provisioner import VMTeardownResult
from orchestrator.routers.agent_thread_status import router
from orchestrator.security import access
from agent.api.orchestrator_client import OrchestratorClient
from tests import test_persistent_recycler_real_postgres as fixtures


pg_dsn = fixtures.pg_dsn
_base_schema = fixtures._schema_applied
_base_db = fixtures.db


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    await fixtures._schema_applied.__wrapped__(pg_dsn)
    migration = Path(__file__).resolve().parents[1] / (
        "src/orchestrator/database/migrations/app/"
        "0298_pinned_vm_retirement_actuator_request.sql"
    )
    if migration.exists():
        conn = await asyncpg.connect(pg_dsn)
        try:
            if not await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='threads'::regclass AND attname='runtime_retirement_actuator_request')"
            ):
                await conn.execute(migration.read_text())
        finally:
            await conn.close()


@pytest_asyncio.fixture
async def db(_base_db):
    yield _base_db


async def scenario(
    db, monkeypatch, *, permanent=False, legacy_vm_incarnation=False, vm_updates=None
):
    pod_uid = str(uuid4())
    ids = await fixtures._seed(
        db, protected_agent_pod=True, workspace_claim=False, pod_uid=pod_uid
    )
    ids["process_generation"] = str(uuid4())
    ids["pod_uid"] = pod_uid
    vm_generation, vm_uid, disk_uid = (str(uuid4()) for _ in range(3))
    launcher_uid, vmi_uid = str(uuid4()), str(uuid4())
    thread = await db.get_thread(ids["thread"])
    ids["generation"] = str(thread["runtime_generation"])
    vm = {
        "status": "ready",
        "provision_generation": vm_generation,
        "identity_provision_generation": vm_generation,
        "identity_authenticated": True,
        "vm_uid": vm_uid,
        "_runtime_incarnation": vm_uid,
        "rootdisk_pvc_uid": disk_uid,
        "active_pod_uid": launcher_uid,
        "vmi_uid": vmi_uid,
        "ssh_host": "192.0.2.10",
        "ssh_port": 22,
    }
    vm.update(vm_updates or {})
    metadata = fixtures._json(thread["metadata"])
    metadata["config_override"]["workspace"]["backend"] = "vm"
    metadata["config_override"]["officer"]["enabled"] = False
    metadata["vm"] = vm
    await db.execute(
        "UPDATE threads SET metadata=$2::jsonb WHERE id=$1::uuid",
        ids["thread"],
        json.dumps(metadata),
    )
    await db.execute(
        "UPDATE agents SET metadata=jsonb_build_object('dispatch_process_generation',$2::text) "
        "WHERE id=$1::uuid",
        ids["agent"],
        ids["process_generation"],
    )
    retirement = await db.begin_pinned_thread_retirement(
        ids["thread"], permanent=permanent
    )
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    request = {
        "agent_id": ids["agent"],
        "pod_uid": ids["pod_uid"],
        "process_generation": ids["process_generation"],
        "runtime_generation": ids["generation"],
        "runtime_attach_token": ids["attach_token"],
        "retirement_token": retirement["token"],
        "disposition": "ended",
        "permanent": permanent,
        "workspace_generation": vm_generation,
        "workspace_runtime_incarnation": vm_uid
        if legacy_vm_incarnation
        else launcher_uid,
    }
    events = []
    k8s = fixtures.StatefulPinnedK8sApi()
    pod_name = f"persistent-{ids['thread'][:12]}"
    k8s.install_old_pod(
        namespace="agents-a",
        name=pod_name,
        uid=ids["pod_uid"],
        labels={
            "srw/component": "persistent-agent",
            "srw/thread-id": ids["thread"],
            "srw.io/runtime-generation": ids["generation"],
            "srw.io/provision-attempt": ids["provision_attempt"],
        },
    )
    pod = k8s.pods[("agents-a", pod_name)]
    pod.spec = NS(
        containers=[NS(name="agent")],
        init_containers=[],
        ephemeral_containers=[],
        volumes=[],
    )
    # The external Kubernetes boundary acknowledges terminal status only
    # after the native exact-UID delete. Finalizer removal then proves absence.
    native_delete = k8s.delete_namespaced_pod

    def delete_pod(*args, **kwargs):
        events.append("pod-stop")
        result = native_delete(*args, **kwargs)
        k8s.mark_terminal("agents-a", pod_name)
        pod.status.container_statuses[0].name = "agent"
        return result

    k8s.delete_namespaced_pod = delete_pod
    agent = AgentProvisioner()
    agent._k8s_available = True
    agent._core_api = k8s
    monkeypatch.setattr(agent_provider_module, "agent_provisioner", agent)
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    core, networking = MagicMock(), MagicMock()
    core.read_namespaced_service.side_effect = fixtures._K8sError(404)
    networking.read_namespaced_ingress.side_effect = fixtures._K8sError(404)
    monkeypatch.setattr(
        main.app.state.resources,
        "session_router",
        SessionRouterService(
            namespace="agents-a",
            ingress_host="unused.example",
            core_api=core,
            networking_api=networking,
        ),
    )

    async def release_vm(thread_id, identity, **kwargs):
        assert ("agents-a", pod_name) not in k8s.pods
        current = await db.get_thread(thread_id)
        assert current["runtime_retirement_actuator_request"] is not None
        assert current["runtime_retirement_local_quiescence"] is None
        assert current["runtime_retirement_stage_receipt"] is None
        assert current["status"] == "active"
        assert current["runtime_retirement_authorized_at"] is not None
        assert str(current["runtime_retirement_token"]) == retirement["token"]
        assert (
            identity.vm_uid == vm_uid and identity.provision_generation == vm_generation
        )
        assert identity.rootdisk_pvc_uid == disk_uid
        assert kwargs["purge_disk"] is permanent
        events.append("vm-stop")
        assert await db.record_managed_repository_workspace_process_zero(
            thread_id,
            owner_kind="thread",
            scope="vm",
            provisioner="vm",
            runtime_incarnation=vm_generation,
        )
        assert await db.merge_thread_vm_context_if_provision_generation(
            thread_id,
            vm_generation,
            {"status": "deleted"},
        )
        return VMTeardownResult("completed", True)

    vm_provider = NS(lifecycle_available=True, release_vm_captured=release_vm)
    monkeypatch.setattr(vm_provider_module, "vm_provisioner", vm_provider)
    return ids, retirement, request, events, k8s, vm_provider


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
async def test_live_actor_handoff_nominates_and_stops_exact_runtime(
    db, monkeypatch, permanent
):
    ids, retirement, request, events, _, _ = await scenario(
        db, monkeypatch, permanent=permanent
    )
    assert await db.list_retryable_pinned_retirements() == []
    accepted = await db.request_pinned_thread_retirement_actuator(
        ids["thread"], **request
    )
    assert accepted["status"] == "actuator_requested"
    assert (
        await db.request_pinned_thread_retirement_actuator(ids["thread"], **request)
        == accepted
    )
    candidates = await db.list_retryable_pinned_retirements()
    assert len(candidates) == 1 and candidates[0]["nominated_before_grace"]
    assert candidates[0]["agent_status"] == "session"
    stopped = asyncio.Event()
    gc = db.gc_offline_agents

    async def finish_pass(**kwargs):
        result = await gc(**kwargs)
        stopped.set()
        return result

    monkeypatch.setattr(db, "gc_offline_agents", finish_pass)
    await asyncio.wait_for(
        detector.stale_agent_detector(
            stopped,
            dependencies=composition.stale_agent_detector_dependencies(
                main.app.state.resources
            ),
        ),
        timeout=15,
    )
    assert events == ["pod-stop", "vm-stop"]

    current = await db.get_thread(ids["thread"])
    assert current is None if permanent else current["status"] == "ended"
    assert await db.list_retryable_pinned_retirements() == []
    outcome = await db.request_pinned_thread_retirement_actuator(
        ids["thread"], **request
    )
    assert outcome["status"] == "settled_or_superseded"
    assert events == ["pod-stop", "vm-stop"]


@pytest.mark.asyncio
async def test_old_request_and_candidate_cannot_nominate_a_resumed_successor(
    db, monkeypatch
):
    from tests.test_pinned_vm_initial_binding_real_postgres import _bind_protected_agent

    ids, _, request, events, k8s, _ = await scenario(db, monkeypatch)
    assert await db.request_pinned_thread_retirement_actuator(ids["thread"], **request)
    candidate = (await db.list_retryable_pinned_retirements())[0]
    dependencies = composition.stale_agent_detector_dependencies(
        main.app.state.resources
    )
    assert await detector.retry_pending_pinned_retirement(
        candidate, dependencies=dependencies
    )
    assert await db.resume_thread(ids["thread"])
    successor = await _bind_protected_agent(db, ids["thread"])
    assert str(successor["runtime_generation"]) != request["runtime_generation"]
    before = dict(successor)
    outcome = await db.request_pinned_thread_retirement_actuator(
        ids["thread"], **request
    )
    assert outcome["status"] == "settled_or_superseded"
    assert not await detector.retry_pending_pinned_retirement(
        candidate, dependencies=dependencies
    )
    assert dict(await db.get_thread(ids["thread"])) == before
    assert await db.list_retryable_pinned_retirements() == []
    assert events == ["pod-stop", "vm-stop"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [
        "agent_id",
        "pod_uid",
        "process_generation",
        "runtime_generation",
        "runtime_attach_token",
        "retirement_token",
        "disposition",
        "permanent",
        "workspace_generation",
        "workspace_runtime_incarnation",
    ],
)
async def test_mismatched_request_never_nominates_or_mutates(db, monkeypatch, field):
    ids, _, request, events, _, _ = await scenario(db, monkeypatch)
    changed = dict(request)
    changed[field] = (
        True
        if field == "permanent"
        else "suspended"
        if field == "disposition"
        else str(uuid4())
    )
    assert (
        await db.request_pinned_thread_retirement_actuator(ids["thread"], **changed)
        is None
    )
    assert await db.list_retryable_pinned_retirements() == []
    current = await db.get_thread(ids["thread"])
    assert current["runtime_retirement_actuator_request"] is None
    assert current["runtime_retirement_local_quiescence"] is None
    assert events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("incarnation", ["vm_uid", "vmi_uid"])
async def test_new_marker_requires_launcher_not_another_vm_identity(
    db, monkeypatch, incarnation
):
    ids, retirement, request, events, _, _ = await scenario(db, monkeypatch)
    request["workspace_runtime_incarnation"] = retirement["context"]["vm"][incarnation]
    assert (
        await db.request_pinned_thread_retirement_actuator(ids["thread"], **request)
        is None
    )
    assert await db.list_retryable_pinned_retirements() == []
    assert events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("launcher", [None, "", "not-a-uid", "A" * 36])
@pytest.mark.parametrize("legacy_vm_incarnation", [False, True])
async def test_missing_or_malformed_captured_launcher_cannot_admit_new_marker(
    db, monkeypatch, launcher, legacy_vm_incarnation
):
    ids, _, request, events, _, _ = await scenario(
        db,
        monkeypatch,
        legacy_vm_incarnation=legacy_vm_incarnation,
        vm_updates={"active_pod_uid": launcher},
    )
    assert (
        await db.request_pinned_thread_retirement_actuator(ids["thread"], **request)
        is None
    )
    assert await db.list_retryable_pinned_retirements() == []
    assert events == []


@pytest.mark.asyncio
async def test_marker_is_independently_guarded_and_not_a_zero_receipt(db, monkeypatch):
    ids, _, request, events, _, _ = await scenario(db, monkeypatch)
    marker = {
        "kind": "vm_local_drain_complete_v1",
        "thread_id": ids["thread"],
        **request,
    }
    malformed = {**marker, "process_generation": "another-process"}
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE threads SET runtime_retirement_actuator_request=$2::jsonb WHERE id=$1::uuid",
            ids["thread"],
            json.dumps(malformed),
        )
    assert await db.request_pinned_thread_retirement_actuator(ids["thread"], **request)
    for replacement in (None, malformed):
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(
                "UPDATE threads SET runtime_retirement_actuator_request=$2::jsonb WHERE id=$1::uuid",
                ids["thread"],
                json.dumps(replacement) if replacement else None,
            )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE threads SET runtime_retirement_permanent=true WHERE id=$1::uuid",
            ids["thread"],
        )
    current = await db.get_thread(ids["thread"])
    assert current["runtime_retirement_stage_receipt"] is None
    assert current["runtime_retirement_local_quiescence"] is None
    assert not await db.settle_pinned_thread_retirement(
        ids["thread"],
        token=request["retirement_token"],
        generation=request["runtime_generation"],
    )
    assert events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["pod", "vm"])
async def test_unknown_stop_preserves_pending_marker_and_retries_after_restart(
    db, monkeypatch, boundary
):
    ids, _, request, events, k8s, provider = await scenario(db, monkeypatch)
    assert await db.request_pinned_thread_retirement_actuator(ids["thread"], **request)
    if boundary == "pod":
        original = k8s.delete_namespaced_pod

        def unknown(**_kwargs):
            raise fixtures._K8sError(503)

        k8s.delete_namespaced_pod = unknown
    else:
        original = provider.release_vm_captured

        async def unknown(*_args, **_kwargs):
            return VMTeardownResult("unknown", False)

        provider.release_vm_captured = unknown
    candidate = (await db.list_retryable_pinned_retirements())[0]
    assert not await detector.retry_pending_pinned_retirement(
        candidate,
        dependencies=composition.stale_agent_detector_dependencies(
            main.app.state.resources
        ),
    )
    current = await db.get_thread(ids["thread"])
    assert current["runtime_retirement_actuator_request"] is not None
    assert current["runtime_retirement_local_quiescence"] is None
    assert current["runtime_retirement_stage_receipt"] is None
    assert current["runtime_retirement_external_cleanup"] is None
    assert not await db.fetchval(
        "SELECT 1 FROM thread_runtime_retirement_outcomes WHERE thread_id=$1::uuid",
        ids["thread"],
    )
    if boundary == "pod":
        k8s.delete_namespaced_pod = original
    else:
        provider.release_vm_captured = original
    # Recompose all retirement operations: only durable work survives.
    candidate = (await db.list_retryable_pinned_retirements())[0]
    assert await detector.retry_pending_pinned_retirement(
        candidate,
        dependencies=composition.stale_agent_detector_dependencies(
            main.app.state.resources
        ),
    )
    assert events.count("vm-stop") == 1


@pytest.mark.asyncio
async def test_stale_candidate_refuses_before_pod_stop_after_process_changes(
    db, monkeypatch
):
    ids, _, request, events, _, _ = await scenario(db, monkeypatch)
    assert await db.request_pinned_thread_retirement_actuator(ids["thread"], **request)
    candidate = (await db.list_retryable_pinned_retirements())[0]
    await db.execute(
        "UPDATE agents SET metadata=jsonb_set(metadata,'{dispatch_process_generation}',to_jsonb($2::text)) WHERE id=$1::uuid",
        ids["agent"],
        str(uuid4()),
    )
    assert await db.list_retryable_pinned_retirements() == []
    assert not await detector.retry_pending_pinned_retirement(
        candidate,
        dependencies=composition.stale_agent_detector_dependencies(
            main.app.state.resources
        ),
    )
    assert events == []


@pytest.mark.asyncio
async def test_internal_http_client_reconciles_lost_handoff_response(db, monkeypatch):
    ids, _, request, events, _, _ = await scenario(db, monkeypatch)
    api = FastAPI()
    api.include_router(router)
    api.state.agent_thread_status_dependencies_factory = lambda: NS(
        db=db, require_internal=access.require_internal
    )
    monkeypatch.setattr(access, "_INTERNAL_KEY", "handoff-test-key")
    transport = httpx.ASGITransport(app=api)
    client = OrchestratorClient(
        orchestrator_url="http://test",
        pod_ip="192.0.2.1",
        pod_port=8001,
        hostname="test",
        config_name="creator",
        pid=123,
    )
    kwargs = {
        "pinned_agent_id": request["agent_id"],
        "pod_uid": request["pod_uid"],
        "process_generation": request["process_generation"],
        "session_runtime_generation": request["runtime_generation"],
        "session_runtime_attach_token": request["runtime_attach_token"],
        "session_runtime_retirement_token": request["retirement_token"],
        "retirement_disposition": "ended",
        "retirement_permanent": False,
        "workspace_generation": request["workspace_generation"],
        "workspace_runtime_incarnation": request["workspace_runtime_incarnation"],
    }
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        client._client = http
        assert (
            await client.request_thread_retirement_actuator(ids["thread"], **kwargs)
            is None
        )
        assert await db.list_retryable_pinned_retirements() == []
        http.headers["X-Internal-Key"] = "handoff-test-key"
        post = http.post
        lost = False

        async def lose_once(*args, **kwargs):
            nonlocal lost
            result = await post(*args, **kwargs)
            if not lost:
                lost = True
                assert result.status_code == 200
                raise httpx.ReadError("durable response lost")
            return result

        monkeypatch.setattr(http, "post", lose_once)
        assert (
            await client.request_thread_retirement_actuator(ids["thread"], **kwargs)
            is None
        )
        assert len(await db.list_retryable_pinned_retirements()) == 1
        accepted = await client.request_thread_retirement_actuator(
            ids["thread"], **kwargs
        )
        assert accepted["status"] == "actuator_requested"
        assert events == []


@pytest.mark.asyncio
async def test_permanent_end_refuses_marker_upgrade_without_stranding_soft_end(
    db, monkeypatch
):
    ids, _, request, events, _, provider = await scenario(db, monkeypatch)
    accepted = await db.request_pinned_thread_retirement_actuator(
        ids["thread"], **request
    )
    operations = composition.thread_retirement_operations(main.app.state.resources)
    current = await db.get_thread(ids["thread"])
    with pytest.raises(HTTPException) as error:
        await operations.end_thread_flow(
            ids["thread"], current, permanent=True, force=True
        )
    assert error.value.status_code == 409
    assert error.value.detail["reason"] == "retirement_mode_changed"
    assert (
        await db.request_pinned_thread_retirement_actuator(ids["thread"], **request)
        == accepted
    )
    assert events == []
    candidate = (await db.list_retryable_pinned_retirements())[0]
    assert await detector.retry_pending_pinned_retirement(
        candidate,
        dependencies=composition.stale_agent_detector_dependencies(
            main.app.state.resources
        ),
    )
    ended = await db.get_thread(ids["thread"])
    assert ended["status"] == "ended"
    assert ended["runtime_retirement_actuator_request"] is None

    # A later permanent request gets fresh authority after the original soft
    # retirement settled. Its retained-disk actuator is qualified separately.
    permanent = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    assert permanent["state"] == "pending"
    assert permanent["token"] != request["retirement_token"]
    assert permanent["permanent"] is True
    assert (
        await db.request_pinned_thread_retirement_actuator(ids["thread"], **request)
    )["status"] == "settled_or_superseded"
    assert events == ["pod-stop", "vm-stop"]


@pytest.mark.asyncio
async def test_acceptance_and_unknown_pod_keep_real_resource_charge(db, monkeypatch):
    from unittest.mock import AsyncMock
    from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
    from tests.test_pinned_vm_initial_binding_real_postgres import _bind_protected_agent
    from tests.test_vm_resource_thread_source_real_postgres import _ready_charged_thread

    prepared = await _ready_charged_thread(db, monkeypatch)
    thread_id = str(prepared["thread_id"])
    assert await VMProvisioningPhaseStore(db).publish_thread_ready(
        thread_id,
        str(prepared["generation"]),
        prepared["registration"],
        prepared["vm_uid"],
        prepared["updates"],
    )
    bound = await _bind_protected_agent(db, thread_id)
    metadata = fixtures._json(bound["metadata"])
    metadata["config_override"]["workspace"]["backend"] = "vm"
    await db.execute(
        "UPDATE threads SET metadata=$2::jsonb WHERE id=$1::uuid",
        thread_id,
        json.dumps(metadata),
    )
    process = str(uuid4())
    await db.execute(
        "UPDATE agents SET metadata=jsonb_build_object('dispatch_process_generation',$2::text) WHERE id=$1::uuid",
        bound["agent_id"],
        process,
    )
    actor = await db.fetchrow(
        "SELECT * FROM agents WHERE id=$1::uuid", bound["agent_id"]
    )
    retirement = await db.begin_pinned_thread_retirement(thread_id, permanent=False)
    assert retirement["state"] == "pending", retirement
    assert await db.authorize_pinned_thread_retirement(
        thread_id,
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    reservation_id = prepared["admitted"]["reservation_id"]
    charge = dict(
        await db.fetchrow(
            "SELECT * FROM vm_resource_reservations WHERE id=$1::uuid", reservation_id
        )
    )
    assert charge["state"] != "released"
    request = {
        "agent_id": str(bound["agent_id"]),
        "pod_uid": actor["pod_uid"],
        "process_generation": process,
        "runtime_generation": str(bound["runtime_generation"]),
        "runtime_attach_token": str(bound["runtime_attach_token"]),
        "retirement_token": retirement["token"],
        "disposition": "ended",
        "permanent": False,
        "workspace_generation": str(prepared["generation"]),
        "workspace_runtime_incarnation": prepared["launcher_uid"],
    }
    assert await db.request_pinned_thread_retirement_actuator(thread_id, **request)
    agent = AgentProvisioner()
    agent._k8s_available = True
    agent._core_api = MagicMock()
    agent._core_api.read_namespaced_pod.side_effect = fixtures._K8sError(503)
    vm_stop = AsyncMock(side_effect=AssertionError("unknown Pod cannot admit VM stop"))
    monkeypatch.setattr(agent_provider_module, "agent_provisioner", agent)
    monkeypatch.setattr(
        vm_provider_module,
        "vm_provisioner",
        NS(lifecycle_available=True, release_vm_captured=vm_stop),
    )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    candidate = (await db.list_retryable_pinned_retirements())[0]
    assert not await detector.retry_pending_pinned_retirement(
        candidate,
        dependencies=composition.stale_agent_detector_dependencies(
            main.app.state.resources
        ),
    )
    vm_stop.assert_not_called()
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_resource_reservations WHERE id=$1::uuid",
                reservation_id,
            )
        )
        == charge
    )
    current = await db.get_thread(thread_id)
    assert current["runtime_retirement_actuator_request"] is not None
    assert current["runtime_retirement_local_quiescence"] is None
    assert current["runtime_retirement_stage_receipt"] is None
    assert current["runtime_retirement_external_cleanup"] is None
    assert current["runtime_retirement_token"] is not None
    assert current["status"] != "ended"
    assert not await db.resume_thread(thread_id)


# ---------------------------------------------------------------------------
# Soft End keeps the root disk; a later permanent Delete reclaims it
# ---------------------------------------------------------------------------


async def _soft_end_settled(db, monkeypatch):
    """A legacy (non-v3) VM thread whose soft End retained its root disk."""

    ids, retirement, request, events, k8s, provider = await scenario(db, monkeypatch)
    assert await db.request_pinned_thread_retirement_actuator(ids["thread"], **request)
    candidate = (await db.list_retryable_pinned_retirements())[0]
    assert await detector.retry_pending_pinned_retirement(
        candidate,
        dependencies=composition.stale_agent_detector_dependencies(
            main.app.state.resources
        ),
    )
    assert events == ["pod-stop", "vm-stop"]
    ended = await db.get_thread(ids["thread"])
    assert ended["status"] == "ended"
    vm = fixtures._json(ended["metadata"])["vm"]
    ids["vm"] = vm
    return ids, retirement, provider


async def _cleanup_admissions(db, thread_id):
    rows = await db.fetch(
        "SELECT id,request_id,source,intent_digest,completed_at,outcome,pvc_uid "
        "FROM vm_workspace_cleanup_admissions "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid ORDER BY admitted_at",
        thread_id,
    )
    return [dict(row) for row in rows]


async def _owner_delete_until_settled(db, thread_id, attempts=4):
    operations = composition.thread_retirement_operations(main.app.state.resources)
    results = []
    for _ in range(attempts):
        current = await db.get_thread(thread_id)
        if current is None:
            break
        try:
            results.append(
                await operations.end_thread_flow(
                    thread_id, current, permanent=True, force=True
                )
            )
        except HTTPException as exc:
            results.append({"http": exc.status_code, "detail": exc.detail})
    return results


def _purging_provider(provider, ids, purges, *, fail_first=False):
    async def purge(thread_id, identity, **kwargs):
        assert thread_id == ids["thread"]
        assert kwargs["purge_disk"] is True
        purges.append(
            (
                identity.provision_generation,
                identity.vm_uid,
                identity.rootdisk_pvc_uid,
            )
        )
        if fail_first and len(purges) == 1:
            return VMTeardownResult("unknown", False)
        return VMTeardownResult("completed", True)

    provider.release_vm_captured = purge


@pytest.mark.asyncio
async def test_soft_end_then_permanent_delete_reclaims_the_retained_disk(
    db, monkeypatch
):
    ids, _, provider = await _soft_end_settled(db, monkeypatch)
    keep = await _cleanup_admissions(db, ids["thread"])
    assert [(row["source"], row["outcome"]) for row in keep] == [
        ("pinned_thread_retirement", "completed")
    ]
    purges = []
    _purging_provider(provider, ids, purges)

    results = await _owner_delete_until_settled(db, ids["thread"])

    assert await db.get_thread(ids["thread"]) is None, results
    vm = ids["vm"]
    assert purges == [
        (vm["provision_generation"], vm["vm_uid"], vm["rootdisk_pvc_uid"])
    ]
    admissions = await _cleanup_admissions(db, ids["thread"])
    # The soft End's keep admission is untouched; the purge is its own exact
    # admission for the same VM and disk, under a different request ID.
    assert admissions[0] == keep[0]
    assert len(admissions) == 2
    purge = admissions[1]
    assert purge["source"] == "pinned_thread_retirement"
    assert purge["pvc_uid"] == keep[0]["pvc_uid"]
    assert purge["request_id"] != keep[0]["request_id"]
    assert purge["intent_digest"] != keep[0]["intent_digest"]
    assert (purge["outcome"], purge["completed_at"] is not None) == ("completed", True)


@pytest.mark.asyncio
async def test_interrupted_permanent_delete_retries_its_own_purge_admission(
    db, monkeypatch
):
    ids, _, provider = await _soft_end_settled(db, monkeypatch)
    purges = []
    _purging_provider(provider, ids, purges, fail_first=True)

    results = await _owner_delete_until_settled(db, ids["thread"])

    assert await db.get_thread(ids["thread"]) is None, results
    # The unknown first purge left its admission open; the retry reused it
    # (same request ID, one purge admission) instead of minting another.
    assert len(purges) == 2 and purges[0] == purges[1]
    admissions = await _cleanup_admissions(db, ids["thread"])
    assert len(admissions) == 2
    assert admissions[1]["outcome"] == "completed"
