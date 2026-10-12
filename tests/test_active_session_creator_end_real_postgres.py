"""Normal End joins a Session observer before exact retirement admission."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import json
import sys
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

from fastapi import HTTPException, Request
import pytest

from orchestrator import main
from orchestrator.application import controls
from orchestrator.routers import thread_lifecycle
from orchestrator.services import container_provisioner as provider_module
from orchestrator.services import ssh_helpers, thread_retirement
from orchestrator.services.container_provisioner import ContainerProvisioner
from orchestrator.services.manifest_workspace_selection import (
    select_execution_workspace,
)
from orchestrator.services.session_provisioner import ensure_session_workspace
from orchestrator.services.workspace_lifecycle import EnsureOutcome
from tests import test_workspace_pull_failure_real_postgres as pull
from tests.test_pinned_failed_start_end_real_postgres import PinnedPullCluster
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_protected_agent
from tests.test_sandbox_workspace_provisioner import stub_plan_inputs

pg_dsn = pull.pg_dsn
db = pull.db
_schema_applied = pull._schema_applied

IMAGE = "registry.example/active-session:waiting"


class ObservingCluster(PinnedPullCluster):
    def create_namespaced_pod(self, *, body, **kwargs):
        pod = super().create_namespaced_pod(body=body, **kwargs)
        if (
            body["metadata"]
            .get("labels", {})
            .get(provider_module.WORKSPACE_PROVISION_FENCE_LABEL)
            != "true"
        ):
            self.pod_create_calls = getattr(self, "pod_create_calls", 0) + 1
        pod.status.container_statuses[0].state.waiting.reason = "ImagePullBackOff"
        return pod


def metadata(row):
    value = row.get("metadata") or {}
    return json.loads(value) if isinstance(value, str) else value


async def setup_case(db, monkeypatch, *, lane, protected_agent):
    actor = dict(
        await db.fetchrow(
            "INSERT INTO users(display_name,is_approved,is_admin) "
            "VALUES('Active Session End owner',TRUE,TRUE) RETURNING *"
        )
    )
    db.manifest_runtime_image = "test.invalid/srw:installed"
    workspace, selection = await select_execution_workspace(
        db,
        actor,
        role="session",
        project_id=None,
        supplied=True,
        workspace={
            "template": {
                "inline": {"backend": "sandbox", "environment": {"image": IMAGE}}
            }
        },
    )
    initial = {"config_override": {"workspace": workspace}}
    if lane == "stateless":
        initial["workspace_container"] = {
            "status": "pending",
            "provisioner": "k8s",
            "_runtime_creation": {
                "generation": str(uuid4()),
                "mode": "create",
                "attempted": False,
                "replaces_uid": None,
            },
        }
    thread_id = await db.create_thread(
        user_id=str(actor["id"]),
        datasource_ids=[],
        execution_lane=lane,
        initial_metadata=initial,
        workspace_selection=selection,
    )
    if protected_agent:
        # The helper runs real plan/effect/protection/bind transitions; only
        # the exact external agent Pod observation is modeled.
        await _bind_protected_agent(db, UUID(thread_id))
    cluster = ObservingCluster()
    provider = ContainerProvisioner()
    provider._db = db
    provider._k8s_available = True
    provider._namespace = "agent-workspaces"
    provider._storage_class = "test-storage"
    provider._pvc_enabled = True
    provider._core_api = cluster
    stub_plan_inputs(monkeypatch, provider)
    for name in ("open_interval", "close_interval"):
        monkeypatch.setattr(
            provider_module.workspace_metering, name, AsyncMock(return_value=None)
        )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(provider_module, "container_provisioner", provider)
    deps = controls.thread_lifecycle_dependencies(main.app.state.resources)

    async def owned_actor(request, store, requested_id):
        assert requested_id == thread_id and store is db
        current = await db.get_thread(thread_id)
        assert str(current["user_id"]) == str(actor["id"])
        return actor, current

    deps = replace(deps, require_thread_owner=owned_actor)
    agent_observation = SimpleNamespace(turn_in_flight=False)
    if protected_agent:

        class IdleAgentTransport:
            def __init__(self, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            async def post(self, url, *, json):
                assert url.endswith("/session/status")
                return SimpleNamespace(
                    status_code=200,
                    json=lambda: {
                        "recipient_verified": True,
                        "session_identity_fingerprint": json[
                            "session_identity_fingerprint"
                        ],
                        "thread_id": thread_id,
                        "turn_in_flight": agent_observation.turn_in_flight,
                    },
                )

        # Model only the exact recipient's HTTP idle observation. End still
        # verifies its fingerprint and current immutable retirement tuple.
        monkeypatch.setattr(thread_retirement.httpx, "AsyncClient", IdleAgentTransport)
    return SimpleNamespace(
        thread_id=thread_id,
        cluster=cluster,
        provider=provider,
        dependencies=deps,
        agent_observation=agent_observation,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "creator_active", [False, True], ids=["creator-returned", "creator-active"]
)
@pytest.mark.parametrize(
    "lane,protected_agent,observation,permanent",
    [
        pytest.param("pinned", False, "image", False, id="pinned-soft-image"),
        pytest.param("stateless", False, "image", False, id="stateless-soft-image"),
        pytest.param("pinned", True, "image", False, id="pinned-protected-soft-image"),
        pytest.param("pinned", False, "ssh", False, id="pinned-soft-ssh"),
        pytest.param("stateless", False, "ssh", False, id="stateless-soft-ssh"),
        pytest.param("pinned", False, "image", True, id="pinned-permanent-image"),
        pytest.param("stateless", False, "image", True, id="stateless-permanent-image"),
        pytest.param("pinned", False, "ssh", True, id="pinned-permanent-ssh"),
        pytest.param("stateless", False, "ssh", True, id="stateless-permanent-ssh"),
    ],
)
async def test_normal_end_during_session_readiness(
    db,
    monkeypatch,
    tmp_path,
    lane,
    protected_agent,
    observation,
    permanent,
    creator_active,
):
    case = await setup_case(db, monkeypatch, lane=lane, protected_agent=protected_agent)
    entered, ssh_started = asyncio.Event(), asyncio.Event()
    wait = case.provider._wait_for_ready
    probe_release = tmp_path / "release-ssh"
    processes = []

    async def observed_wait(*args, **kwargs):
        entered.set()
        if observation == "ssh":
            pod = case.cluster.objects["pod"]
            pod.status.phase = "Running"
            for status in pod.status.container_statuses:
                status.ready = True
                status.state = SimpleNamespace(
                    waiting=None, running=SimpleNamespace(), terminated=None
                )
        return await wait(*args, **kwargs)

    monkeypatch.setattr(case.provider, "_wait_for_ready", observed_wait)
    if observation == "ssh":
        spawn = ssh_helpers.create_owned_subprocess_exec

        async def owned_probe(*args, **kwargs):
            proc = await spawn(*args, **kwargs)
            processes.append(proc)
            ssh_started.set()
            return proc

        monkeypatch.setattr(ssh_helpers, "create_owned_subprocess_exec", owned_probe)
        monkeypatch.setattr(
            provider_module, "workspace_private_key_fingerprint", lambda _: "test-key"
        )
        monkeypatch.setattr(
            ssh_helpers,
            "build_agent_ssh_cmd",
            lambda *args, **kwargs: [
                sys.executable,
                "-c",
                "import pathlib,sys,time\np=pathlib.Path(sys.argv[1])\nwhile not p.exists(): time.sleep(0.01)\nraise SystemExit(1)",
                str(probe_release),
            ],
        )
        case.provider._ssh_auth_connect_timeout = 60
        case.provider._ssh_auth_ready_timeout = 0.1
    creator = asyncio.create_task(
        ensure_session_workspace(
            case.thread_id,
            db=db,
            provisioner=case.provider,
            suspension=SimpleNamespace(),
        )
    )
    probe_progress = None
    try:
        await asyncio.wait_for(entered.wait(), 15)
        if observation == "ssh":
            await asyncio.wait_for(ssh_started.wait(), 5)
        assert not creator.done()
        original = await db.get_thread(case.thread_id)
        original_pod = case.cluster.objects["pod"].metadata.uid
        original_pvc = case.cluster.objects["pvc"].metadata.uid
        if lane == "pinned":
            intent = await db.fetchrow(
                "SELECT * FROM thread_workspace_provision_intents WHERE thread_id=$1",
                UUID(case.thread_id),
            )
            assert str(intent["pod_uid"]) == original_pod
            assert str(intent["runtime_generation"]) == str(
                original["runtime_generation"]
            )
            assert str(intent["created_agent_id"] or "") == str(
                original["agent_id"] or ""
            )
            async with db.try_thread_advisory_lock(case.thread_id) as owned:
                assert not owned
        else:
            async with db.stateless_session_workspace_ensure_lock(
                case.thread_id
            ) as owned:
                assert not owned
            async with db.workspace_runtime_mutation_lock(
                case.thread_id,
                owner_kind="thread",
                scope="workspace_container",
                wait=False,
            ) as owned:
                assert not owned
        if not creator_active:
            if observation == "ssh":
                probe_release.touch()
            else:
                case.cluster.objects["pod"].status.container_statuses[
                    0
                ].state.waiting.reason = "InvalidImageName"
            result = await asyncio.wait_for(creator, 5)
            assert result.outcome is EnsureOutcome.FAILED

        started = time.monotonic()
        if creator_active and observation == "ssh":

            async def complete_owned_probe():
                for _ in range(500):
                    if await db.session_workspace_observation_yield_requested(
                        case.thread_id,
                        runtime_generation=str(original["runtime_generation"]),
                    ):
                        assert not creator.done()
                        assert processes and processes[0].returncode is None
                        probe_release.touch()
                        return
                    await asyncio.sleep(0.01)
                pytest.fail("End did not publish a readiness scheduling signal")

            probe_progress = asyncio.create_task(complete_owned_probe())
        try:
            response = await asyncio.wait_for(
                thread_lifecycle.end_thread(
                    case.thread_id,
                    Request(
                        {
                            "type": "http",
                            "method": "DELETE",
                            "path": f"/api/persistent/threads/{case.thread_id}",
                        }
                    ),
                    permanent=permanent,
                    force=False,
                    dependencies=case.dependencies,
                ),
                40,
            )
        except HTTPException as exc:
            current = await db.get_thread(case.thread_id)
            md = metadata(current)
            evidence = {
                "lane": lane,
                "protected_agent": protected_agent,
                "observation": observation,
                "creator_active": creator_active,
                "permanent": permanent,
                "elapsed": round(time.monotonic() - started, 3),
                "http": exc.status_code,
                "detail": exc.detail,
                "creator_still_running": not creator.done(),
                "status": current["status"],
                "same_generation": original["runtime_generation"]
                == current["runtime_generation"],
                "same_actor": original["agent_id"] == current["agent_id"],
                "same_attach": original["runtime_attach_token"]
                == current["runtime_attach_token"],
                "retirement_token": current.get("runtime_retirement_token"),
                "retirement_authorized": current.get(
                    "runtime_retirement_authorized_at"
                ),
                "stateless_retirement": md.get("_stateless_claim_retirement"),
                "same_pod": case.cluster.objects.get("pod") is not None
                and case.cluster.objects["pod"].metadata.uid == original_pod,
                "same_pvc": case.cluster.objects.get("pvc") is not None
                and case.cluster.objects["pvc"].metadata.uid == original_pvc,
            }
            pytest.fail(
                "Normal End did not accept intent: " + json.dumps(evidence, default=str)
            )
        assert response["status"] in {"ending", "ended", "deleted"}
        if creator_active:
            result = await asyncio.wait_for(asyncio.shield(creator), 5)
            assert result.outcome is EnsureOutcome.PENDING
            if probe_progress is not None:
                await probe_progress
                assert all(proc.returncode is not None for proc in processes)
        current = await db.get_thread(case.thread_id)
        if current is not None and response["status"] == "ending":
            assert current["runtime_retirement_authorized_at"] is not None
        elif current is not None:
            assert current["status"] == "ended"
        if not permanent and not protected_agent:
            assert case.cluster.objects["pvc"].metadata.uid == original_pvc
        if not creator_active and observation == "ssh":
            assert processes and all(proc.returncode == 1 for proc in processes)
    finally:
        probe_release.touch()
        if probe_progress is not None:
            if not probe_progress.done():
                probe_progress.cancel()
            await asyncio.gather(probe_progress, return_exceptions=True)
        if observation == "image" and "pod" in case.cluster.objects:
            status = case.cluster.objects["pod"].status.container_statuses[0]
            if status.state.waiting is not None:
                status.state.waiting.reason = "InvalidImageName"
        if not creator.done():
            try:
                await asyncio.wait_for(asyncio.shield(creator), 5)
            except TimeoutError:
                creator.cancel()
        await asyncio.gather(creator, return_exceptions=True)


async def normal_end(case, *, permanent=False):
    return await thread_lifecycle.end_thread(
        case.thread_id,
        Request({"type": "http", "method": "DELETE", "path": "/test"}),
        permanent=permanent,
        force=False,
        dependencies=case.dependencies,
    )


def observe_readiness(case, monkeypatch):
    entered = asyncio.Event()
    actual = case.provider._wait_for_ready

    async def observe(*args, **kwargs):
        entered.set()
        return await actual(*args, **kwargs)

    monkeypatch.setattr(case.provider, "_wait_for_ready", observe)
    return entered


async def exact_source(db, case, lane):
    table = (
        "thread_workspace_provision_intents"
        if lane == "pinned"
        else "managed_repository_workspace_creation_reservations"
    )
    column = "thread_id" if lane == "pinned" else "owner_id"
    return dict(
        await db.fetchrow(
            f"SELECT * FROM {table} WHERE {column}=$1::uuid", case.thread_id
        )
    )


def ready_external_runtime(case, monkeypatch):
    pod = case.cluster.objects["pod"]
    pod.status.phase = "Running"
    for status in pod.status.container_statuses:
        status.ready = True
        status.state = SimpleNamespace(
            waiting=None, running=SimpleNamespace(), terminated=None
        )
    case.cluster.connect_get_namespaced_pod_exec = lambda **_: None
    fingerprint = "SHA256:" + "A" * 43
    monkeypatch.setattr(
        provider_module, "workspace_private_key_fingerprint", lambda _: fingerprint
    )
    monkeypatch.setattr(
        provider_module,
        "_isolated_pod_exec",
        lambda *args, **kwargs: f"256 {fingerprint} workspace (ED25519)",
    )
    monkeypatch.setattr(
        ssh_helpers,
        "build_agent_ssh_cmd",
        lambda *args, **kwargs: [sys.executable, "-c", "raise SystemExit(0)"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_ready_finalization_wins_before_end_admission_under_real_row_lock(
    db, monkeypatch, lane
):
    case = await setup_case(db, monkeypatch, lane=lane, protected_agent=False)
    observing = observe_readiness(case, monkeypatch)
    actual_end = thread_retirement._end_thread_flow_owned
    admitted_ready = asyncio.Event()

    async def inspect_ready_then_end(tid, fresh, **kwargs):
        workspace = metadata(fresh)["workspace_container"]
        assert workspace["status"] == "ready"
        assert (
            workspace["_runtime_incarnation"]
            == case.cluster.objects["pod"].metadata.uid
        )
        admitted_ready.set()
        # An external kubelet reports this exact finished runtime. The normal
        # retirement must still record its own positive stop receipt; no SQL
        # proof or binding is written by this transport fixture.
        pod = case.cluster.objects["pod"]
        pod.status.phase = "Succeeded"
        for status in pod.status.container_statuses:
            status.state = SimpleNamespace(
                waiting=None, running=None, terminated=SimpleNamespace(exit_code=0)
            )
        return await actual_end(tid, fresh, **kwargs)

    monkeypatch.setattr(
        thread_retirement, "_end_thread_flow_owned", inspect_ready_then_end
    )
    creator = asyncio.create_task(
        ensure_session_workspace(
            case.thread_id,
            db=db,
            provisioner=case.provider,
            suspension=SimpleNamespace(),
        )
    )
    ending = None
    try:
        await asyncio.wait_for(observing.wait(), 10)
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.fetchrow(
                    "SELECT id FROM threads WHERE id=$1::uuid FOR UPDATE",
                    case.thread_id,
                )
                ready_external_runtime(case, monkeypatch)
                async with asyncio.timeout(10):
                    while True:
                        if creator.done():
                            result = await creator
                            pytest.fail(
                                f"creator returned before finalization row lock: {result}"
                            )
                        await conn.execute("SELECT pg_stat_clear_snapshot()")
                        if await conn.fetchval(
                            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() "
                            "AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%threads%FOR UPDATE%')"
                        ):
                            break
                        await asyncio.sleep(0.01)
                ending = asyncio.create_task(normal_end(case))
                original = await conn.fetchrow(
                    "SELECT * FROM threads WHERE id=$1::uuid", case.thread_id
                )
                async with asyncio.timeout(5):
                    while not await db.session_workspace_observation_yield_requested(
                        case.thread_id,
                        runtime_generation=str(original["runtime_generation"]),
                    ):
                        await asyncio.sleep(0.01)
                assert not admitted_ready.is_set()
                assert original["runtime_retirement_token"] is None
                assert not ending.done()
        assert (await asyncio.wait_for(creator, 10)).outcome is EnsureOutcome.PENDING
        assert (await asyncio.wait_for(ending, 10))["status"] == "ended"
        assert admitted_ready.is_set()
        assert (
            metadata(await db.get_thread(case.thread_id))["workspace_container"][
                "status"
            ]
            == "deleted"
        )
    finally:
        for task in (creator, ending):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (creator, ending) if task is not None),
            return_exceptions=True,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_end_cannot_fence_an_owned_sdk_before_uid_publication(
    db, monkeypatch, lane
):
    import threading

    case = await setup_case(db, monkeypatch, lane=lane, protected_agent=False)
    started, release = threading.Event(), threading.Event()
    create = case.cluster.create_namespaced_pod
    observing = observe_readiness(case, monkeypatch)

    def blocked_create(**kwargs):
        pod = create(**kwargs)
        started.set()
        assert release.wait(15)
        return pod

    monkeypatch.setattr(case.cluster, "create_namespaced_pod", blocked_create)
    monkeypatch.setattr(
        thread_retirement, "SESSION_END_WORKSPACE_HANDOFF_TIMEOUT_S", 0.2
    )
    creator = asyncio.create_task(
        ensure_session_workspace(
            case.thread_id,
            db=db,
            provisioner=case.provider,
            suspension=SimpleNamespace(),
        )
    )
    try:
        async with asyncio.timeout(10):
            while not started.is_set():
                await asyncio.sleep(0.01)
        original = await db.get_thread(case.thread_id)
        before = await exact_source(db, case, lane)
        assert before["pod_uid"] is None
        with pytest.raises(HTTPException) as refused:
            await normal_end(case)
        assert refused.value.status_code == 503
        assert refused.value.detail["code"] == "session_workspace_lifecycle_busy"
        assert refused.value.headers == {"Retry-After": "1"}
        current = await db.get_thread(case.thread_id)
        assert current["status"] == "created"
        assert current["runtime_retirement_token"] is None
        assert "_stateless_claim_retirement" not in metadata(current)
        assert current["runtime_generation"] == original["runtime_generation"]
        assert not creator.done()
        assert (await exact_source(db, case, lane))["pod_uid"] is None
        release.set()
        await asyncio.wait_for(observing.wait(), 5)
        source = await exact_source(db, case, lane)
        assert str(source["pod_uid"]) == case.cluster.objects["pod"].metadata.uid
        assert source.get("id", source.get("attempt_id")) == before.get(
            "id", before.get("attempt_id")
        )
        monkeypatch.setattr(
            thread_retirement, "SESSION_END_WORKSPACE_HANDOFF_TIMEOUT_S", 5.0
        )
        assert (await normal_end(case))["status"] == "ended"
        assert (await asyncio.wait_for(creator, 5)).outcome is EnsureOutcome.PENDING
        assert case.cluster.pod_create_calls == 1
    finally:
        release.set()
        if not creator.done() and "pod" in case.cluster.objects:
            case.cluster.objects["pod"].status.container_statuses[
                0
            ].state.waiting.reason = "InvalidImageName"
        await asyncio.wait_for(asyncio.gather(creator, return_exceptions=True), 10)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lane,protected", [("pinned", False), ("pinned", True), ("stateless", False)]
)
async def test_duplicate_end_signals_preserve_existing_retirement(
    db, monkeypatch, lane, protected
):
    case = await setup_case(db, monkeypatch, lane=lane, protected_agent=protected)
    observing = observe_readiness(case, monkeypatch)
    creator = asyncio.create_task(
        ensure_session_workspace(
            case.thread_id,
            db=db,
            provisioner=case.provider,
            suspension=SimpleNamespace(),
        )
    )
    try:
        await asyncio.wait_for(observing.wait(), 10)
        responses = await asyncio.gather(normal_end(case), normal_end(case))
        expected = "ending" if protected else "ended"
        assert all(response["status"] == expected for response in responses)
        assert (await asyncio.wait_for(creator, 5)).outcome is EnsureOutcome.PENDING
        if protected:
            current = await db.get_thread(case.thread_id)
            token = current["runtime_retirement_token"]
            assert current["runtime_retirement_authorized_at"] is not None
            assert (await normal_end(case))["status"] == "ending"
            assert (await db.get_thread(case.thread_id))[
                "runtime_retirement_token"
            ] == token
            assert case.cluster.pod_deletes == 0
    finally:
        if not creator.done() and "pod" in case.cluster.objects:
            case.cluster.objects["pod"].status.container_statuses[
                0
            ].state.waiting.reason = "InvalidImageName"
        await asyncio.wait_for(asyncio.gather(creator, return_exceptions=True), 10)


@pytest.mark.asyncio
async def test_nonforce_end_after_yield_keeps_exact_busy_actor_and_source(
    db, monkeypatch
):
    case = await setup_case(db, monkeypatch, lane="pinned", protected_agent=True)
    case.agent_observation.turn_in_flight = True
    observing = observe_readiness(case, monkeypatch)
    creator = asyncio.create_task(
        ensure_session_workspace(
            case.thread_id,
            db=db,
            provisioner=case.provider,
            suspension=SimpleNamespace(),
        )
    )
    try:
        await asyncio.wait_for(observing.wait(), 10)
        original = await db.get_thread(case.thread_id)
        source = await exact_source(db, case, "pinned")
        with pytest.raises(HTTPException) as refused:
            await normal_end(case)
        assert refused.value.status_code == 409
        assert refused.value.detail["code"] == "turn_in_flight"
        current = await db.get_thread(case.thread_id)
        for field in ("runtime_generation", "agent_id", "runtime_attach_token"):
            assert current[field] == original[field]
        assert current["runtime_retirement_token"] is None
        assert current["runtime_retirement_authorized_at"] is None
        assert (await exact_source(db, case, "pinned"))["pod_uid"] == source["pod_uid"]
        assert case.cluster.pod_deletes == 0
        assert not await db.session_workspace_observation_yield_requested(
            case.thread_id, runtime_generation=str(current["runtime_generation"])
        )
        assert (await asyncio.wait_for(creator, 5)).outcome is EnsureOutcome.PENDING
    finally:
        if not creator.done():
            case.cluster.objects["pod"].status.container_statuses[
                0
            ].state.waiting.reason = "InvalidImageName"
        await asyncio.wait_for(asyncio.gather(creator, return_exceptions=True), 10)


@pytest.mark.asyncio
async def test_end_waiter_cannot_follow_a_protected_binding_successor(db, monkeypatch):
    case = await setup_case(db, monkeypatch, lane="pinned", protected_agent=False)
    original = await db.get_thread(case.thread_id)
    end_request = None
    try:
        async with db.thread_advisory_lock(case.thread_id):
            end_request = asyncio.create_task(normal_end(case))
            async with asyncio.timeout(5):
                while not await db.session_workspace_observation_yield_requested(
                    case.thread_id,
                    runtime_generation=str(original["runtime_generation"]),
                ):
                    await asyncio.sleep(0.01)
            bound = await _bind_protected_agent(db, UUID(case.thread_id))
            assert (
                bound["runtime_generation"],
                bound["agent_id"],
                bound["runtime_attach_token"],
            ) != (
                original["runtime_generation"],
                original["agent_id"],
                original["runtime_attach_token"],
            )
        with pytest.raises(HTTPException) as refused:
            await end_request
        assert refused.value.status_code == 409
        current = await db.get_thread(case.thread_id)
        assert current["runtime_retirement_token"] is None
        assert current["agent_id"] == bound["agent_id"]
        assert current["runtime_generation"] == bound["runtime_generation"]
        assert case.cluster.objects == {}
    finally:
        if end_request is not None and not end_request.done():
            end_request.cancel()
            await asyncio.gather(end_request, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_borrowed_outer_lock_is_not_released_by_inner_creator_yield(
    db, monkeypatch, lane
):
    case = await setup_case(db, monkeypatch, lane=lane, protected_agent=False)
    observing = observe_readiness(case, monkeypatch)
    inner_returned, release_outer = asyncio.Event(), asyncio.Event()
    monkeypatch.setattr(
        thread_retirement, "SESSION_END_WORKSPACE_HANDOFF_TIMEOUT_S", 3.0
    )

    async def borrowed_creator():
        lock = (
            db.thread_advisory_lock(case.thread_id)
            if lane == "pinned"
            else db.stateless_session_workspace_ensure_lock(case.thread_id)
        )
        async with lock as owned:
            assert owned
            result = await ensure_session_workspace(
                case.thread_id,
                db=db,
                provisioner=case.provider,
                suspension=SimpleNamespace(),
                _pinned_runtime_lock_held=lane == "pinned",
                _workspace_lifecycle_lock_held=lane == "stateless",
            )
            assert result.outcome is EnsureOutcome.PENDING
            inner_returned.set()
            await release_outer.wait()
            return result

    creator = asyncio.create_task(borrowed_creator())
    try:
        await asyncio.wait_for(observing.wait(), 10)
        with pytest.raises(HTTPException) as refused:
            await normal_end(case)
        assert refused.value.status_code == 503
        assert inner_returned.is_set() and not creator.done()
        assert (await db.get_thread(case.thread_id))["runtime_retirement_token"] is None
        assert case.cluster.pod_deletes == 0
        release_outer.set()
        await asyncio.wait_for(creator, 5)
        assert (await normal_end(case))["status"] == "ended"
    finally:
        release_outer.set()
        if not creator.done() and "pod" in case.cluster.objects:
            case.cluster.objects["pod"].status.container_statuses[
                0
            ].state.waiting.reason = "InvalidImageName"
        await asyncio.wait_for(asyncio.gather(creator, return_exceptions=True), 10)


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_disconnected_end_after_yield_allows_exact_normal_prepare_retry(
    db, monkeypatch, lane
):
    case = await setup_case(db, monkeypatch, lane=lane, protected_agent=False)
    observing = observe_readiness(case, monkeypatch)
    before_begin = asyncio.Event()
    actual_end = thread_retirement._end_thread_flow_owned
    end_request = None

    async def pause_before_begin(*args, **kwargs):
        if asyncio.current_task() is end_request:
            before_begin.set()
            await asyncio.Event().wait()
        return await actual_end(*args, **kwargs)

    monkeypatch.setattr(thread_retirement, "_end_thread_flow_owned", pause_before_begin)
    creator = asyncio.create_task(
        ensure_session_workspace(
            case.thread_id,
            db=db,
            provisioner=case.provider,
            suspension=SimpleNamespace(),
        )
    )
    try:
        await asyncio.wait_for(observing.wait(), 10)
        before = await exact_source(db, case, lane)
        original = await db.get_thread(case.thread_id)
        pod_uid = case.cluster.objects["pod"].metadata.uid
        pvc_uid = case.cluster.objects["pvc"].metadata.uid
        end_request = asyncio.create_task(normal_end(case))
        await asyncio.wait_for(before_begin.wait(), 5)
        assert (await asyncio.wait_for(creator, 5)).outcome is EnsureOutcome.PENDING
        end_request.cancel()  # The HTTP caller disconnects; never cancel creator.
        with pytest.raises(asyncio.CancelledError):
            await end_request
        assert not await db.session_workspace_observation_yield_requested(
            case.thread_id, runtime_generation=str(original["runtime_generation"])
        )
        current = await db.get_thread(case.thread_id)
        assert current["runtime_retirement_token"] is None
        assert current["status"] == "created"
        assert "_stateless_claim_retirement" not in metadata(current)
        assert not metadata(current).get("_workspace_binding")
        assert case.thread_id not in {
            str(row["id"]) for row in await db.list_threads_needing_workspace()
        }  # Unattended initial discovery remains an explicit follow-up.

        # Same service operation scheduled by normal prepare/agent workspace
        # polling. Keep the image unready and prove it observes the same source.
        observing.clear()
        creator = asyncio.create_task(
            ensure_session_workspace(
                case.thread_id,
                db=db,
                provisioner=case.provider,
                suspension=SimpleNamespace(),
            )
        )
        await asyncio.wait_for(observing.wait(), 5)
        after = await exact_source(db, case, lane)
        assert after.get("id", after.get("attempt_id")) == before.get(
            "id", before.get("attempt_id")
        )
        assert str(
            after["runtime_generation"]
            if lane == "pinned"
            else after["thread_runtime_generation"]
        ) == str(original["runtime_generation"])
        assert str(after["pod_uid"]) == pod_uid
        assert case.cluster.objects["pvc"].metadata.uid == pvc_uid
        assert case.cluster.pod_create_calls == 1
        assert not metadata(await db.get_thread(case.thread_id)).get(
            "_workspace_binding"
        )
        case.cluster.objects["pod"].status.container_statuses[
            0
        ].state.waiting.reason = "InvalidImageName"
        await asyncio.wait_for(creator, 5)
        assert (await normal_end(case))["status"] == "ended"
    finally:
        if end_request is not None and not end_request.done():
            end_request.cancel()
            await asyncio.gather(end_request, return_exceptions=True)
        if not creator.done() and "pod" in case.cluster.objects:
            case.cluster.objects["pod"].status.container_statuses[
                0
            ].state.waiting.reason = "InvalidImageName"
        await asyncio.wait_for(asyncio.gather(creator, return_exceptions=True), 10)


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_end_handoff_deadline_includes_lifecycle_connection_acquisition(
    db, monkeypatch, lane
):
    from orchestrator.database import postgres as postgres_module

    case = await setup_case(db, monkeypatch, lane=lane, protected_agent=False)
    original = await db.get_thread(case.thread_id)
    actual_connect = postgres_module.asyncpg.connect
    calls = 0
    connection_cancelled = asyncio.Event()

    async def connect(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:  # Real PostgreSQL signal session.
            return await actual_connect(*args, **kwargs)
        try:
            await asyncio.Event().wait()
        finally:
            connection_cancelled.set()

    monkeypatch.setattr(postgres_module.asyncpg, "connect", connect)
    monkeypatch.setattr(
        thread_retirement, "SESSION_END_WORKSPACE_HANDOFF_TIMEOUT_S", 0.2
    )
    with pytest.raises(HTTPException) as refused:
        await asyncio.wait_for(normal_end(case), 2)
    assert refused.value.status_code == 503
    assert refused.value.detail["code"] == "session_workspace_lifecycle_busy"
    assert connection_cancelled.is_set()
    assert calls == 2
    current = await db.get_thread(case.thread_id)
    assert current["runtime_generation"] == original["runtime_generation"]
    assert current["runtime_retirement_token"] is None
    assert current["status"] == "created"
    assert "_stateless_claim_retirement" not in metadata(current)
    assert not await db.session_workspace_observation_yield_requested(
        case.thread_id, runtime_generation=str(original["runtime_generation"])
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_owned_ssh_probe_must_finish_before_end_can_be_accepted(
    db, monkeypatch, tmp_path, lane
):
    case = await setup_case(db, monkeypatch, lane=lane, protected_agent=False)
    probe_release = tmp_path / "release-probe"
    probe_started = asyncio.Event()
    processes = []
    actual_wait = case.provider._wait_for_ready
    actual_spawn = ssh_helpers.create_owned_subprocess_exec

    async def observe(*args, **kwargs):
        ready_external_runtime(case, monkeypatch)
        monkeypatch.setattr(
            ssh_helpers,
            "build_agent_ssh_cmd",
            lambda *args, **kwargs: [
                sys.executable,
                "-c",
                "import pathlib,sys,time\np=pathlib.Path(sys.argv[1])\nwhile not p.exists(): time.sleep(0.01)\nraise SystemExit(1)",
                str(probe_release),
            ],
        )
        return await actual_wait(*args, **kwargs)

    async def spawn(*args, **kwargs):
        proc = await actual_spawn(*args, **kwargs)
        processes.append(proc)
        probe_started.set()
        return proc

    monkeypatch.setattr(case.provider, "_wait_for_ready", observe)
    monkeypatch.setattr(ssh_helpers, "create_owned_subprocess_exec", spawn)
    monkeypatch.setattr(
        thread_retirement, "SESSION_END_WORKSPACE_HANDOFF_TIMEOUT_S", 0.2
    )
    case.provider._ssh_auth_connect_timeout = 60
    case.provider._ssh_auth_ready_timeout = 0.1
    creator = asyncio.create_task(
        ensure_session_workspace(
            case.thread_id,
            db=db,
            provisioner=case.provider,
            suspension=SimpleNamespace(),
        )
    )
    try:
        await asyncio.wait_for(probe_started.wait(), 10)
        original = await db.get_thread(case.thread_id)
        source = await exact_source(db, case, lane)
        with pytest.raises(HTTPException) as refused:
            await normal_end(case)
        assert refused.value.status_code == 503
        assert refused.value.detail["code"] == "session_workspace_lifecycle_busy"
        assert not creator.done()
        assert processes and all(proc.returncode is None for proc in processes)
        current = await db.get_thread(case.thread_id)
        assert current["status"] == "created"
        assert current["runtime_generation"] == original["runtime_generation"]
        assert current["runtime_retirement_token"] is None
        assert "_stateless_claim_retirement" not in metadata(current)
        assert (await exact_source(db, case, lane))["pod_uid"] == source["pod_uid"]
        assert not await db.session_workspace_observation_yield_requested(
            case.thread_id, runtime_generation=str(original["runtime_generation"])
        )
        probe_release.touch()
        assert (await asyncio.wait_for(creator, 5)).outcome is EnsureOutcome.FAILED
        assert all(proc.returncode == 1 for proc in processes)
        monkeypatch.setattr(
            thread_retirement, "SESSION_END_WORKSPACE_HANDOFF_TIMEOUT_S", 5.0
        )
        assert (await normal_end(case))["status"] == "ended"
    finally:
        probe_release.touch()
        await asyncio.wait_for(asyncio.gather(creator, return_exceptions=True), 10)
