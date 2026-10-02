"""Exercise the owned background runner with real sources and lifecycle locks."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
import asyncpg
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

from orchestrator.database.postgres import PostgresDB
from orchestrator.services import container_provisioner as provider_module
from orchestrator.services import session_provisioner, ssh_helpers
from tests import test_workspace_pull_failure_real_postgres as pull
from tests.test_active_session_creator_end_real_postgres import (
    metadata,
    ready_external_runtime,
    exact_source,
)
from tests.test_session_created_source_rediscovery_real_postgres import (
    leave_exact_source_after_end_disconnect,
    reconstructed_provider,
)

pg_dsn = pull.pg_dsn
_schema_applied = pull._schema_applied


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied):
    # The runner intentionally scans globally. Clone the fully migrated empty
    # template per case so each test owns every candidate it can discover.
    parts = urlsplit(pg_dsn)
    template = parts.path.lstrip("/")
    name = "session_runner_" + uuid4().hex
    control = await asyncpg.connect(urlunsplit(parts._replace(path="/postgres")))
    store = None
    try:
        await control.execute(f'CREATE DATABASE "{name}" TEMPLATE "{template}"')
        store = PostgresDB(
            urlunsplit(parts._replace(path="/" + name)),
            min_connections=1,
            max_connections=4,
        )
        await store.connect()
        yield store
    finally:
        if store is not None:
            await store.close()
        await control.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await control.close()


async def wait_ready(db, thread_id, *, timeout=10):
    async with asyncio.timeout(timeout):
        while (
            metadata(await db.get_thread(thread_id))["workspace_container"]["status"]
            != "ready"
        ):
            await asyncio.sleep(0.02)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lane,protected_agent", [("pinned", False), ("pinned", True), ("stateless", False)]
)
async def test_owned_sweeper_runner_recovers_exact_source_while_legacy_sweep_is_blocked(
    db, pg_dsn, monkeypatch, lane, protected_agent
):
    case, source, original = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, lane, protected_agent=protected_agent
    )
    restarted = PostgresDB(db._connection_string, min_connections=1, max_connections=4)
    await restarted.connect()
    restarted.manifest_runtime_image = "test.invalid/srw:installed"
    shutdown = asyncio.Event()
    legacy_entered = asyncio.Event()
    legacy_release = asyncio.Event()

    async def legacy(**kwargs):
        legacy_entered.set()
        await legacy_release.wait()
        return 0

    monkeypatch.setattr(session_provisioner, "reconcile_session_workspaces", legacy)
    provider = reconstructed_provider(restarted, case, monkeypatch)
    ready_external_runtime(case, monkeypatch)
    case.cluster.objects["pod"].status.conditions = [
        SimpleNamespace(
            type="Ready", status="True", last_transition_time=datetime.now(timezone.utc)
        )
    ]
    task = asyncio.create_task(
        session_provisioner.workspace_idle_sweeper(
            shutdown,
            store=restarted,
            provisioner=provider,
            suspension=SimpleNamespace(),
        )
    )
    try:
        await asyncio.wait_for(legacy_entered.wait(), 3)
        await wait_ready(restarted, case.thread_id)
        current = await restarted.get_thread(case.thread_id)
        result = await exact_source(restarted, case, lane)
        assert current["runtime_generation"] == original["runtime_generation"]
        assert result.get("id", result.get("attempt_id")) == source.get(
            "id", source.get("attempt_id")
        )
        for key in ("pod_uid", "pvc_uid", "seed_configmap_uid", "service_uid"):
            assert result[key] == source[key]
        assert case.cluster.pod_create_calls == 1
        assert (
            result.get("settled_at") is not None
            if lane == "stateless"
            else result["status"] == "published"
        )
    finally:
        shutdown.set()
        legacy_release.set()
        await asyncio.wait_for(asyncio.shield(task), 15)
        await restarted.close()


def make_ready(case, monkeypatch):
    ready_external_runtime(case, monkeypatch)
    case.cluster.objects["pod"].status.conditions = [
        SimpleNamespace(
            type="Ready", status="True", last_transition_time=datetime.now(timezone.utc)
        )
    ]


def shared_provider(db, cases, monkeypatch):
    provider = reconstructed_provider(db, cases[0], monkeypatch)

    class Clusters:
        def __getattr__(self, method):
            def call(**kwargs):
                name = kwargs.get("name") or (kwargs.get("body") or {}).get(
                    "metadata", {}
                ).get("name", "")
                matches = [c for c in cases if c.thread_id[:12] in name]
                assert len(matches) == 1, (method, name)
                return getattr(matches[0].cluster, method)(**kwargs)

            return call

    provider._core_api = Clusters()
    return provider


@pytest.mark.asyncio
async def test_v1_unscheduled_session_keeps_same_uid_until_later_ready(db, monkeypatch):
    """An open exact source can acquire a schedule after an earlier wait."""

    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )

    case, source, original = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, "stateless"
    )
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    provider = reconstructed_provider(db, case, monkeypatch)
    pod = case.cluster.objects["pod"]
    pod.spec.node_name = None
    pod.status.phase = "Pending"
    pod.status.conditions = [
        SimpleNamespace(type="PodScheduled", status="False", reason="Unschedulable")
    ]
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=provider, shutdown_event=asyncio.Event()
    )
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    assert not await runner._continue(candidate)
    waiting = await exact_source(db, case, "stateless")
    assert waiting["id"] == source["id"]
    assert waiting["pod_uid"] == source["pod_uid"]
    assert waiting["startup_protocol_version"] == 1
    assert waiting["startup_state"] == "waiting_capacity"
    assert waiting["scheduled_at"] is None

    scheduled = datetime.now(timezone.utc)
    pod.spec.node_name = "node8"
    ready_external_runtime(case, monkeypatch)
    # The staged waiter requires a verified pinned host key before SSH. Model
    # the key scan as accepted by the external verifier for this exact Pod.
    public_key = (
        Ed25519PrivateKey.from_private_bytes(bytes(range(32)))
        .public_key()
        .public_bytes(Encoding.OpenSSH, PublicFormat.OpenSSH)
        .decode("ascii")
    )
    fingerprint = ssh_helpers._fingerprint_host_key(public_key.split()[1])
    monkeypatch.setattr(
        provider_module, "workspace_private_key_fingerprint", lambda _: fingerprint
    )
    monkeypatch.setattr(
        provider_module,
        "_isolated_pod_exec",
        lambda *args, **kwargs: f"256 {fingerprint} workspace (ED25519)",
    )
    scan = AsyncMock(return_value=(f"{pod.status.pod_ip} {public_key}", b""))
    monkeypatch.setattr(ssh_helpers, "_scan_pinned_host_key", scan)
    pod.status.conditions = [
        SimpleNamespace(
            type="PodScheduled", status="True", last_transition_time=scheduled
        ),
        SimpleNamespace(type="Ready", status="True", last_transition_time=scheduled),
    ]
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    assert await runner._continue(candidate)
    assert scan.await_args.args == (pod.status.pod_ip, 30022, fingerprint)
    settled = await exact_source(db, case, "stateless")
    assert settled["id"] == source["id"]
    assert settled["pod_uid"] == source["pod_uid"]
    assert settled["scheduled_at"] == scheduled
    assert settled["startup_first_ready_at"] == scheduled
    assert settled["settled_at"] is not None
    assert (await db.get_thread(case.thread_id))["runtime_generation"] == original[
        "runtime_generation"
    ]
    assert case.cluster.pod_create_calls == 1


@pytest.mark.asyncio
async def test_v1_scheduled_pull_deadline_is_sticky_across_continuation(
    db, monkeypatch
):
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )

    case, source, _ = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, "stateless"
    )
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    provider = reconstructed_provider(db, case, monkeypatch)
    provider._reattach_ready_timeout = 2
    provider._image_pull_timeout = 2
    pod = case.cluster.objects["pod"]
    scheduled = datetime.now(timezone.utc)
    pod.spec.node_name = "node8"
    pod.status.conditions = [
        SimpleNamespace(
            type="PodScheduled", status="True", last_transition_time=scheduled
        )
    ]
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=provider, shutdown_event=asyncio.Event()
    )
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    assert not await runner._continue(candidate)
    starting = await exact_source(db, case, "stateless")
    assert starting["scheduled_at"] == scheduled
    assert starting["startup_state"] == "starting"
    assert starting["ready_budget_seconds"] == 2
    assert starting["pull_budget_seconds"] == 2

    await asyncio.sleep(2.2)
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    assert not await runner._continue(candidate)
    attention = await exact_source(db, case, "stateless")
    assert attention["startup_state"] == "attention"
    assert attention["startup_reason_code"] == "pull_deadline"
    assert attention["settled_at"] is None
    assert attention["scheduled_at"] == scheduled

    ready_external_runtime(case, monkeypatch)
    pod.status.conditions.append(
        SimpleNamespace(
            type="Ready", status="True", last_transition_time=datetime.now(timezone.utc)
        )
    )
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    assert not await runner._continue(candidate)
    held = await exact_source(db, case, "stateless")
    assert held["startup_state"] == "attention"
    assert held["settled_at"] is None
    assert held["scheduled_at"] == scheduled
    assert held["id"] == source["id"]
    assert case.cluster.pod_create_calls == 1


@pytest.mark.asyncio
async def test_failed_exit17_initial_session_records_frozen_deadline_without_client(
    db, monkeypatch
):
    """A terminated original Pod still needs an unattended startup observation."""

    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )

    case, original, thread = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, "stateless"
    )
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    provider = reconstructed_provider(db, case, monkeypatch)
    ssh = AsyncMock(side_effect=AssertionError("terminal Pod reached SSH"))
    monkeypatch.setattr(provider_module, "wait_for_agent_ssh", ssh)
    provider._reattach_ready_timeout = 10
    provider._image_pull_timeout = 10
    pod = case.cluster.objects["pod"]
    scheduled = datetime.now(timezone.utc)
    pod.spec.node_name = "node8"
    pod.spec.restart_policy = "Never"
    pod.status.conditions = [
        SimpleNamespace(
            type="PodScheduled", status="True", last_transition_time=scheduled
        )
    ]
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=provider, shutdown_event=asyncio.Event()
    )
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    assert not await runner._continue(candidate)
    frozen = await exact_source(db, case, "stateless")
    assert frozen["startup_state"] == "starting"
    assert frozen["scheduled_at"] == scheduled
    assert frozen["ready_budget_seconds"] == frozen["pull_budget_seconds"] == 10

    pod.status.phase = "Failed"
    (workspace_status,) = pod.status.container_statuses
    workspace_status.ready = False
    workspace_status.started = False
    workspace_status.restart_count = 0
    workspace_status.container_id = "containerd://original-exit17"
    workspace_status.state = SimpleNamespace(
        waiting=None,
        running=None,
        terminated=SimpleNamespace(
            exit_code=17,
            reason="Error",
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
        ),
    )
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    assert not await runner._continue(candidate)
    before_deadline = await exact_source(db, case, "stateless")
    assert before_deadline["startup_state"] == "starting"
    assert before_deadline["startup_attention_at"] is None
    await asyncio.sleep(10.2)
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    assert not await runner._continue(candidate)
    attention = await exact_source(db, case, "stateless")
    current = await db.get_thread(case.thread_id)
    assert (attention["startup_state"], attention["startup_reason_code"]) == (
        "attention",
        "readiness_deadline",
    )
    assert attention["startup_attention_at"] is not None
    assert attention["startup_first_ready_at"] is None
    assert attention["scheduled_at"] == scheduled
    assert attention["ready_budget_seconds"] == frozen["ready_budget_seconds"]
    assert attention["pull_budget_seconds"] == frozen["pull_budget_seconds"]
    for key in (
        "id",
        "thread_runtime_generation",
        "claim_token",
        "pod_uid",
        "pvc_uid",
        "service_uid",
        "seed_configmap_uid",
        "runtime_incarnation",
    ):
        assert attention[key] == frozen[key] == original[key]
    assert current["runtime_generation"] == thread["runtime_generation"]
    assert metadata(current)["workspace_container"]["status"] == "created"
    assert not metadata(current).get("_workspace_binding")
    ssh.assert_not_called()
    assert (
        await db.fetchval(
            "SELECT count(*) FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM thread_input_deliveries WHERE thread_id=$1::uuid",
            case.thread_id,
        )
        == 0
    )
    assert case.cluster.pod_create_calls == 1
    assert case.cluster.objects["pod"].metadata.uid == str(original["pod_uid"])
    assert all(kind in case.cluster.objects for kind in ("pod", "pvc", "service"))
    with pytest.raises(
        provider_module.WorkspaceRuntimeAuthorityError,
        match="authorized workspace Pod is terminal",
    ):
        await provider._read_stateless_creation_pod(
            provider_module.WorkspaceOwner.session(case.thread_id),
            generation=str(thread["runtime_generation"]),
            expected_runtime_incarnation=str(original["pod_uid"]),
            expected_network_tier=await provider._resolve_network_tier(
                case.thread_id, kind="thread"
            ),
            expected_pvc_name=provider_module._UNSPECIFIED_RESOURCE_BINDING,
            expected_seed_configmap=provider_module._UNSPECIFIED_RESOURCE_BINDING,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("second_snapshot", ["ready", "unknown"])
async def test_terminal_observation_refuses_changed_second_pod_read(
    db, monkeypatch, second_snapshot
):
    """A once-terminal Pod cannot enter Ready/SSH from a different reread."""

    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )

    case, _, _ = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, "stateless"
    )
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    provider = reconstructed_provider(db, case, monkeypatch)
    ssh = AsyncMock(side_effect=AssertionError("changed terminal Pod reached SSH"))
    monkeypatch.setattr(provider_module, "wait_for_agent_ssh", ssh)
    provider._reattach_ready_timeout = provider._image_pull_timeout = 2
    pod = case.cluster.objects["pod"]
    scheduled = datetime.now(timezone.utc)
    pod.spec.node_name = "node8"
    pod.spec.restart_policy = "Never"
    pod.status.conditions = [
        SimpleNamespace(
            type="PodScheduled", status="True", last_transition_time=scheduled
        )
    ]
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=provider, shutdown_event=asyncio.Event()
    )
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    assert not await runner._continue(candidate)
    pod.status.phase = "Failed"
    (status,) = pod.status.container_statuses
    status.ready = False
    status.started = False
    status.restart_count = 0
    status.container_id = "containerd://original-exit17"
    status.state = SimpleNamespace(
        waiting=None,
        running=None,
        terminated=SimpleNamespace(exit_code=17, reason="Error"),
    )
    await asyncio.sleep(2.2)

    original_read = case.cluster.read_namespaced_pod
    reads = 0

    def changed_second_read(*, name, **kwargs):
        nonlocal reads
        result = original_read(name=name, **kwargs)
        reads += 1
        if reads == 2:
            result.status.phase = "Running"
            status.state = SimpleNamespace(
                waiting=None,
                running=SimpleNamespace() if second_snapshot == "ready" else None,
                terminated=None,
            )
            status.ready = second_snapshot == "ready"
            if second_snapshot == "ready":
                result.status.conditions.append(
                    SimpleNamespace(
                        type="Ready", status="True", last_transition_time=scheduled
                    )
                )
        return result

    monkeypatch.setattr(case.cluster, "read_namespaced_pod", changed_second_read)
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    assert not await runner._continue(candidate)
    source = await exact_source(db, case, "stateless")
    assert reads >= 2
    assert source["startup_state"] == "starting"
    assert source["startup_first_ready_at"] is None
    assert source["startup_attention_at"] is None
    assert source["settled_at"] is None
    assert not metadata(await db.get_thread(case.thread_id)).get("_workspace_binding")
    ssh.assert_not_called()
    assert case.cluster.pod_create_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "pod_absent",
        "pod_uid",
        "reservation_annotation",
        "pvc_absent",
        "pvc_uid",
        "service_uid",
        "seed_uid",
        "stale_candidate_claim",
        "stale_candidate_generation",
    ],
)
async def test_terminal_observation_requires_exact_original_resources(
    db, monkeypatch, fault
):
    """A terminal shortcut never accepts a replacement or stale authority."""

    from dataclasses import replace
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )

    case, original, _ = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, "stateless", seeded=fault == "seed_uid"
    )
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    provider = reconstructed_provider(db, case, monkeypatch)
    provider._reattach_ready_timeout = provider._image_pull_timeout = 2
    pod = case.cluster.objects["pod"]
    scheduled = datetime.now(timezone.utc)
    pod.spec.node_name = "node8"
    pod.spec.restart_policy = "Never"
    pod.status.conditions = [
        SimpleNamespace(
            type="PodScheduled", status="True", last_transition_time=scheduled
        )
    ]
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=provider, shutdown_event=asyncio.Event()
    )
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    assert not await runner._continue(candidate)
    pod.status.phase = "Failed"
    (status,) = pod.status.container_statuses
    status.ready = False
    status.started = False
    status.restart_count = 0
    status.container_id = "containerd://original-exit17"
    status.state = SimpleNamespace(
        waiting=None,
        running=None,
        terminated=SimpleNamespace(exit_code=17, reason="Error"),
    )
    if fault == "pod_absent":
        case.cluster.objects.pop("pod")
    elif fault == "pod_uid":
        pod.metadata.uid = str(uuid4())
    elif fault == "reservation_annotation":
        pod.metadata.annotations[
            provider_module.WORKSPACE_CREATION_RESERVATION_ANNOTATION
        ] = str(uuid4())
    elif fault == "pvc_absent":
        case.cluster.objects.pop("pvc")
    elif fault in {"pvc_uid", "service_uid", "seed_uid"}:
        case.cluster.objects[fault.split("_")[0]].metadata.uid = str(uuid4())
    await asyncio.sleep(2.2)
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    if fault == "stale_candidate_claim":
        candidate = replace(candidate, claim_token=candidate.claim_token + 1)
    elif fault == "stale_candidate_generation":
        candidate = replace(candidate, runtime_generation=str(uuid4()))
    assert not await runner._continue(candidate)
    source = await exact_source(db, case, "stateless")
    assert source["id"] == original["id"]
    assert source["pod_uid"] == original["pod_uid"]
    assert source["startup_state"] == "starting"
    assert source["startup_first_ready_at"] is None
    assert source["startup_attention_at"] is None
    assert source["settled_at"] is None
    assert case.cluster.pod_create_calls == 1


@pytest.mark.asyncio
async def test_two_slow_sources_yield_slots_to_later_ready_source(db, monkeypatch):
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )

    cases = [
        (await leave_exact_source_after_end_disconnect(db, monkeypatch, lane))[0]
        for lane in ("pinned", "stateless", "pinned")
    ]
    provider = shared_provider(db, cases, monkeypatch)
    make_ready(cases[-1], monkeypatch)
    active = peak = 0
    seen = set()
    actual_wait = provider._wait_for_ready

    async def observed(*args, **kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        seen.add(kwargs["expected_owner"].id)
        try:
            return await actual_wait(*args, **kwargs)
        finally:
            active -= 1

    monkeypatch.setattr(provider, "_wait_for_ready", observed)
    shutdown = asyncio.Event()
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=provider, shutdown_event=shutdown
    )
    task = asyncio.create_task(runner.run())
    try:
        await wait_ready(db, cases[-1].thread_id, timeout=10)
        assert seen == {case.thread_id for case in cases}
        assert peak == 2
        assert all(case.cluster.pod_create_calls == 1 for case in cases)
        for case in cases[:2]:
            assert (
                metadata(await db.get_thread(case.thread_id))["workspace_container"][
                    "status"
                ]
                != "ready"
            )
    finally:
        shutdown.set()
        await asyncio.wait_for(asyncio.shield(task), 10)
    assert not runner._workers


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
@pytest.mark.parametrize("resource", ["pod", "pvc", "service"])
@pytest.mark.parametrize("drift", ["absent", "replacement"])
async def test_background_never_reissues_recorded_resources(
    db, monkeypatch, lane, resource, drift
):
    from uuid import uuid4
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )

    case, source, _ = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, lane
    )
    original = case.cluster.objects[resource]
    if drift == "absent":
        del case.cluster.objects[resource]
    else:
        original.metadata.uid = str(uuid4())
    provider = reconstructed_provider(db, case, monkeypatch)
    if "pod" in case.cluster.objects:
        make_ready(case, monkeypatch)
    creations = []
    for suffix in ("pod", "persistent_volume_claim", "service", "config_map"):
        method = "create_namespaced_" + suffix
        actual = getattr(case.cluster, method)

        def record(*args, _actual=actual, _method=method, **kwargs):
            creations.append(_method)
            return _actual(*args, **kwargs)

        monkeypatch.setattr(case.cluster, method, record)
    page = await db.list_current_session_creation_candidates()
    (candidate,) = page.candidates
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=provider, shutdown_event=asyncio.Event()
    )
    assert not await runner._continue(candidate)
    assert creations == []
    current_source = await exact_source(db, case, lane)
    for key in ("pod_uid", "pvc_uid", "service_uid"):
        assert current_source[key] == source[key]
    assert not metadata(await db.get_thread(case.thread_id)).get("_workspace_binding")


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_exact_candidate_rejects_changed_owner_source_and_resource_hints(
    db, monkeypatch, lane
):
    from dataclasses import replace
    from uuid import uuid4
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )

    case, _, _ = await leave_exact_source_after_end_disconnect(db, monkeypatch, lane)
    provider = reconstructed_provider(db, case, monkeypatch)
    page = await db.list_current_session_creation_candidates()
    (candidate,) = page.candidates
    assert await db.current_session_creation_candidate_is_exact(candidate)
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=provider, shutdown_event=asyncio.Event()
    )
    changes = [
        {"runtime_generation": str(uuid4())},
        {"agent_id": str(uuid4())},
        {"attach_token": str(uuid4())},
        {"pod_uid": str(uuid4())},
        {"pvc_uid": str(uuid4())},
        {"service_uid": str(uuid4())},
        {"seed_configmap_uid": str(uuid4())},
        {"namespace": "foreign"},
        {"lane": "stateless" if lane == "pinned" else "pinned"},
        {"cursor": replace(candidate.cursor, source_id=str(uuid4()))},
        {"claim_token": (candidate.claim_token or 0) + 1},
    ]
    for change in changes:
        stale = replace(candidate, **change)
        assert not await db.current_session_creation_candidate_is_exact(stale), change
        assert not await runner._continue(stale), change
    make_ready(case, monkeypatch)
    assert await runner._continue(candidate)
    assert not await db.current_session_creation_candidate_is_exact(candidate)
    assert not await runner._continue(candidate)
    assert case.cluster.pod_create_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
@pytest.mark.parametrize(
    "cancel_owner", [False, True], ids=["normal-shutdown", "repeated-owner-cancel"]
)
async def test_owned_sweeper_shutdown_joins_started_seed_patch(
    db, monkeypatch, lane, cancel_owner
):
    import threading
    from unittest.mock import AsyncMock

    case, source, _ = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, lane, seeded=True
    )
    provider = reconstructed_provider(db, case, monkeypatch)
    monkeypatch.setattr(
        provider,
        "_resolve_ide_seed_files",
        AsyncMock(return_value={"settings.json": {"content": "{}"}}),
    )
    entered, release = threading.Event(), threading.Event()
    actual = case.cluster.patch_namespaced_config_map
    finished = []

    def blocked_patch(**kwargs):
        entered.set()
        assert release.wait(15)
        result = actual(**kwargs)
        finished.append(result.metadata.uid)
        return result

    monkeypatch.setattr(case.cluster, "patch_namespaced_config_map", blocked_patch)
    monkeypatch.setattr(
        session_provisioner, "reconcile_session_workspaces", AsyncMock(return_value=0)
    )
    terminal_entered, terminal_stopped = asyncio.Event(), asyncio.Event()

    async def terminal_cleanup(**kwargs):
        terminal_entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            terminal_stopped.set()

    shutdown = asyncio.Event()
    task = asyncio.create_task(
        session_provisioner.workspace_idle_sweeper(
            shutdown,
            store=db,
            provisioner=provider,
            suspension=SimpleNamespace(),
            terminal_vm_controls_factory=lambda: SimpleNamespace(
                reconcile_terminal_vm_cleanups=terminal_cleanup
            ),
        )
    )
    try:
        async with asyncio.timeout(7):
            while not entered.is_set():
                await asyncio.sleep(0.01)
        await asyncio.wait_for(terminal_entered.wait(), 3)
        shutdown.set()
        if cancel_owner:
            task.cancel()
        for _ in range(3):
            await asyncio.sleep(0.03)
            assert not task.done()
            if cancel_owner:
                task.cancel()
        lock = (
            db.try_thread_advisory_lock(case.thread_id)
            if lane == "pinned"
            else db.stateless_session_workspace_ensure_lock(case.thread_id)
        )
        async with lock as acquired:
            assert not acquired
    finally:
        shutdown.set()
        release.set()
        if cancel_owner:
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(asyncio.shield(task), 10)
        else:
            await asyncio.wait_for(asyncio.shield(task), 10)
    assert terminal_stopped.is_set(), (
        "creation-runner cancellation bypassed terminal task drain"
    )
    assert finished == [str(source["seed_configmap_uid"])]
    current = await exact_source(db, case, lane)
    assert current["pod_uid"] == source["pod_uid"]
    assert not metadata(await db.get_thread(case.thread_id)).get("_workspace_binding")
    lock = (
        db.try_thread_advisory_lock(case.thread_id)
        if lane == "pinned"
        else db.stateless_session_workspace_ensure_lock(case.thread_id)
    )
    async with lock as acquired:
        assert acquired


@pytest.mark.asyncio
async def test_runner_pages_past_held_sources_skips_busy_owner_and_wraps(
    db, monkeypatch, tmp_path
):
    from orchestrator.database.session_creation_candidates import (
        SESSION_CREATION_SCAN_SQL,
    )
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )
    import json

    # More than the production page size, all admitted through production
    # creation and cooperative End handoff; no inserted/fabricated source rows.
    cases = []
    for index in range(35):
        case, _, _ = await leave_exact_source_after_end_disconnect(
            db, monkeypatch, "pinned" if index % 2 == 0 else "stateless"
        )
        cases.append(case)
        if 0 < index < 34:
            case.cluster.objects.pop("pod")  # API absence, never stop proof.
    provider = shared_provider(db, cases, monkeypatch)
    make_ready(cases[-1], monkeypatch)
    observed_cursors = []
    list_candidates = db.list_current_session_creation_candidates

    async def record_page(**kwargs):
        observed_cursors.append(kwargs.get("after"))
        return await list_candidates(**kwargs)

    monkeypatch.setattr(db, "list_current_session_creation_candidates", record_page)
    plan = await db.fetchval(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + SESSION_CREATION_SCAN_SQL,
        None,
        None,
        None,
        None,
        32,
    )
    plan = json.loads(plan) if isinstance(plan, str) else plan
    assert plan[0]["Plan"]["Actual Rows"] == 32
    (tmp_path / "session-creation-scan-plan.json").write_text(
        json.dumps(plan, indent=2)
    )
    # Only plan mechanics/costs are printed; no owner metadata or credentials.
    print("SESSION_CREATION_QUERY_PLAN", json.dumps(plan))
    shutdown = asyncio.Event()
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=provider, shutdown_event=shutdown, round_delay_s=0.1
    )
    task = None
    try:
        async with db.thread_advisory_lock(cases[0].thread_id):
            task = asyncio.create_task(runner.run())
            await wait_ready(db, cases[-1].thread_id, timeout=15)
            assert (
                metadata(await db.get_thread(cases[0].thread_id))[
                    "workspace_container"
                ]["status"]
                != "ready"
            )
            assert any(cursor is not None for cursor in observed_cursors)
        make_ready(cases[0], monkeypatch)
        await wait_ready(db, cases[0].thread_id, timeout=15)
        assert sum(cursor is None for cursor in observed_cursors) >= 2
        assert all(case.cluster.pod_create_calls == 1 for case in cases)
        for case in cases[1:-1]:
            assert not metadata(await db.get_thread(case.thread_id)).get(
                "_workspace_binding"
            )
    finally:
        shutdown.set()
        if task is not None:
            await asyncio.wait_for(asyncio.shield(task), 10)


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_multiple_observation_quanta_do_not_restart_image_pull_budget(
    db, monkeypatch, lane
):
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )
    from orchestrator.services.container_provisioner import WorkspaceImagePullError
    from orchestrator.services.workspace_lifecycle import (
        SessionWorkspaceObservationYielded,
    )

    case, source, _ = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, lane
    )
    provider = reconstructed_provider(db, case, monkeypatch)
    provider._image_pull_timeout = 8
    provider._reattach_ready_timeout = 1
    expired = asyncio.Event()
    yielded = []
    clocks = []
    actual = provider._wait_for_ready

    async def observed(*args, **kwargs):
        clocks.append(
            (kwargs.get("pull_started_at"), kwargs.get("readiness_started_at"))
        )
        try:
            return await actual(*args, **kwargs)
        except WorkspaceImagePullError:
            expired.set()
            raise
        except SessionWorkspaceObservationYielded:
            yielded.append(True)
            raise

    monkeypatch.setattr(provider, "_wait_for_ready", observed)
    shutdown = asyncio.Event()
    runner = SessionCreationContinuationRunner(
        db=db,
        provisioner=provider,
        shutdown_event=shutdown,
        quantum_s=0.4,
        round_delay_s=0.05,
    )
    task = asyncio.create_task(runner.run())
    try:
        await asyncio.wait_for(expired.wait(), 11)
        assert len(yielded) >= 2
        physical_start = case.cluster.objects["pod"].metadata.creation_timestamp
        assert clocks and all(
            clock == (physical_start, physical_start) for clock in clocks
        )
        assert (datetime.now(timezone.utc) - physical_start).total_seconds() >= 8
        current = await exact_source(db, case, lane)
        assert current["pod_uid"] == source["pod_uid"]
        assert current.get("settled_at") is None
        assert not metadata(await db.get_thread(case.thread_id)).get(
            "_workspace_binding"
        )
    finally:
        shutdown.set()
        await asyncio.wait_for(asyncio.shield(task), 10)


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
@pytest.mark.parametrize(
    "pre_pod_delay", [False, True], ids=["late-valid-pull", "pre-pod-sdk-delay"]
)
async def test_background_clock_allows_valid_late_pull_without_charging_pre_pod_delay(
    db, monkeypatch, lane, pre_pod_delay
):
    import time
    from tests.test_active_session_creator_end_real_postgres import ObservingCluster
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )

    if pre_pod_delay:
        original_create = ObservingCluster.create_namespaced_pod

        def delayed(self, **kwargs):
            time.sleep(2.5)  # Real elapsed time before the apiserver Pod birth.
            return original_create(self, **kwargs)

        monkeypatch.setattr(ObservingCluster, "create_namespaced_pod", delayed)
    # This case proves real elapsed time after Pod birth, so retain real polling.
    case, source, _ = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, lane, fast_polling=False
    )
    provider = reconstructed_provider(db, case, monkeypatch)
    provider._reattach_ready_timeout = 1
    provider._image_pull_timeout = 3 if pre_pod_delay else 8
    # This positive case covers physical-clock origin, not SSH expiry. Keep
    # room for real PostgreSQL admission under load; expiry has separate tests.
    provider._ssh_auth_ready_timeout = 30
    pod_start = case.cluster.objects["pod"].metadata.creation_timestamp
    assert (datetime.now(timezone.utc) - pod_start).total_seconds() > 1
    if pre_pod_delay:
        assert (pod_start - source["created_at"]).total_seconds() >= 2.5
    make_ready(case, monkeypatch)
    ready_start = case.cluster.objects["pod"].status.conditions[0].last_transition_time
    physical_clocks = []
    ready_clocks = []
    actual_wait = provider._wait_for_ready
    actual_ready_clock = provider._background_ready_clock

    async def observed_wait(*args, **kwargs):
        physical_clocks.append(
            (kwargs.get("pull_started_at"), kwargs.get("readiness_started_at"))
        )
        return await actual_wait(*args, **kwargs)

    def observed_ready_clock(pod, created_at):
        result = actual_ready_clock(pod, created_at)
        ready_clocks.append((created_at, result))
        return result

    monkeypatch.setattr(provider, "_wait_for_ready", observed_wait)
    monkeypatch.setattr(provider, "_background_ready_clock", observed_ready_clock)
    # Candidate discovery and guarded continuation can exceed the old 0.5s
    # SSH fixture budget under load; exercise that delay deliberately.
    await asyncio.sleep(0.75)
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=provider, shutdown_event=asyncio.Event()
    )
    assert await runner._continue(candidate)
    assert physical_clocks and all(
        clock == (pod_start, pod_start) for clock in physical_clocks
    )
    assert ready_clocks and all(
        clock == (pod_start, ready_start) for clock in ready_clocks
    )
    assert (
        metadata(await db.get_thread(case.thread_id))["workspace_container"]["status"]
        == "ready"
    )
    assert case.cluster.pod_create_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
@pytest.mark.parametrize("missing", ["pod-clock", "ready-clock"])
async def test_background_missing_physical_clock_holds_without_ssh(
    db, monkeypatch, lane, missing
):
    from unittest.mock import AsyncMock
    from orchestrator.services import container_provisioner as module
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )

    case, _, _ = await leave_exact_source_after_end_disconnect(db, monkeypatch, lane)
    provider = reconstructed_provider(db, case, monkeypatch)
    make_ready(case, monkeypatch)
    pod = case.cluster.objects["pod"]
    if missing == "pod-clock":
        pod.metadata.creation_timestamp = None
    else:
        pod.status.conditions[0].last_transition_time = None
    ssh = AsyncMock(side_effect=AssertionError("missing physical clock reached SSH"))
    monkeypatch.setattr(module, "wait_for_agent_ssh", ssh)
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=provider, shutdown_event=asyncio.Event()
    )
    assert not await runner._continue(candidate)
    ssh.assert_not_called()
    assert not metadata(await db.get_thread(case.thread_id)).get("_workspace_binding")


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
@pytest.mark.parametrize(
    "flapping",
    [False, True],
    ids=["early-ready-auth-failure", "ready-flaps-global-cap"],
)
async def test_repeated_ssh_observation_keeps_original_stage_and_global_budget(
    db, monkeypatch, lane, flapping
):
    import sys
    from orchestrator.services.container_provisioner import (
        WorkspaceSSHAuthenticationError,
    )
    from orchestrator.services import ssh_helpers
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )

    case, _, _ = await leave_exact_source_after_end_disconnect(db, monkeypatch, lane)
    provider = reconstructed_provider(db, case, monkeypatch)
    make_ready(case, monkeypatch)
    provider._reattach_ready_timeout = 1
    provider._image_pull_timeout = 3 if flapping else 8
    provider._ssh_auth_ready_timeout = 1
    provider._ssh_auth_poll_interval = 0.05
    monkeypatch.setattr(
        ssh_helpers,
        "build_agent_ssh_cmd",
        lambda *a, **k: [sys.executable, "-c", "raise SystemExit(1)"],
    )
    pod = case.cluster.objects["pod"]
    ready_at = pod.status.conditions[0].last_transition_time
    if flapping:
        original_read = case.cluster.read_namespaced_pod

        def read(**kwargs):
            current = original_read(**kwargs)
            current.status.conditions[0].last_transition_time = datetime.now(
                timezone.utc
            )
            return current

        monkeypatch.setattr(case.cluster, "read_namespaced_pod", read)
    expired = asyncio.Event()
    actual = provider._wait_for_ready
    quanta = []

    async def observed(*args, **kwargs):
        quanta.append(datetime.now(timezone.utc))
        try:
            return await actual(*args, **kwargs)
        except WorkspaceSSHAuthenticationError as exc:
            if "budget expired" in str(exc):
                expired.set()
            raise

    monkeypatch.setattr(provider, "_wait_for_ready", observed)
    shutdown = asyncio.Event()
    runner = SessionCreationContinuationRunner(
        db=db,
        provisioner=provider,
        shutdown_event=shutdown,
        quantum_s=0.2,
        round_delay_s=0.05,
    )
    task = asyncio.create_task(runner.run())
    try:
        await asyncio.wait_for(expired.wait(), 6)
        assert len(quanta) >= 2
        now = datetime.now(timezone.utc)
        if flapping:
            assert (now - pod.metadata.creation_timestamp).total_seconds() >= 4
        else:
            assert (now - ready_at).total_seconds() >= 1
        assert not metadata(await db.get_thread(case.thread_id)).get(
            "_workspace_binding"
        )
    finally:
        shutdown.set()
        await asyncio.wait_for(asyncio.shield(task), 10)


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_store_expected_existing_source_fence_cannot_reserve_a_successor(
    db, monkeypatch, lane
):
    from orchestrator.services.workspace_lifecycle import WorkspaceOwner
    from tests.test_active_session_creator_end_real_postgres import setup_case

    case, source, _ = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, lane
    )
    fresh = await setup_case(db, monkeypatch, lane=lane, protected_agent=False)

    async def reserve(target, expected_id, expected_token=None):
        row = await db.get_thread(target.thread_id)
        owner = WorkspaceOwner.session(target.thread_id)
        if lane == "pinned":
            return await db.reserve_pinned_thread_workspace_provision_intent(
                target.thread_id,
                expected_runtime_generation=str(row["runtime_generation"]),
                expected_agent_id=None,
                expected_attach_token=None,
                expected_workspace_context=metadata(row).get("workspace_container"),
                expected_binding_context=None,
                attempt_id=str(uuid4()),
                namespace="agent-workspaces",
                pod_name=owner.pod_name,
                pvc_name="pvc-" + owner.pod_name,
                seed_configmap_name=None,
                service_name=owner.pod_name,
                retained_service_uid=None,
                network_tier=source["network_tier"],
                manifest_fingerprint=source["manifest_fingerprint"],
                expected_existing_attempt_id=expected_id,
            )
        return await db.reserve_managed_repository_workspace_creation(
            target.thread_id,
            owner_kind="thread",
            scope="workspace_container",
            claimant=source["claimed_by"],
            operation_kind="create",
            desired_manifest_digest=source["desired_manifest_digest"],
            expected_existing_reservation_id=expected_id,
            expected_existing_claim_token=expected_token,
        )

    source_id = str(source.get("id", source.get("attempt_id")))
    token = source.get("claim_token")
    # Same owner/G but a different expected source cannot adopt the current one.
    assert await reserve(case, str(uuid4()), token) is None
    if token is not None:
        assert await reserve(case, source_id, token + 1) is None
    # A fresh owner would admit an initial reservation without the optional
    # fence. With it, source absence must not INSERT or prepare any new source.
    assert await reserve(fresh, source_id, token) is None
    table, owner_column = (
        ("thread_workspace_provision_intents", "thread_id")
        if lane == "pinned"
        else ("managed_repository_workspace_creation_reservations", "owner_id")
    )
    assert (
        await db.fetchval(
            f"SELECT count(*) FROM {table} WHERE {owner_column}=$1::uuid",
            fresh.thread_id,
        )
        == 0
    )
    assert case.cluster.pod_create_calls == 1
    assert fresh.cluster.objects == {}
    assert (
        str((await exact_source(db, case, lane)).get("id", source.get("attempt_id")))
        == source_id
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
@pytest.mark.parametrize("permanent", [False, True])
async def test_end_winning_after_discovery_invalidates_background_hint(
    db, monkeypatch, lane, permanent
):
    from fastapi import HTTPException
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )
    from tests.test_active_session_creator_end_real_postgres import normal_end

    case, _, _ = await leave_exact_source_after_end_disconnect(db, monkeypatch, lane)
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    # The real route and cleanup retain their existing pending-503 continuation.
    for _ in range(3):
        try:
            response = await normal_end(case, permanent=permanent)
        except HTTPException as exc:
            assert exc.status_code == 503
        else:
            if response["status"] in {"ended", "deleted"}:
                break
    else:
        pytest.fail("normal End did not settle")
    provider = reconstructed_provider(db, case, monkeypatch)
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=provider, shutdown_event=asyncio.Event()
    )
    assert not await db.current_session_creation_candidate_is_exact(candidate)
    assert not await runner._continue(candidate)
    assert case.cluster.pod_create_calls == 1
    assert not (await db.list_current_session_creation_candidates()).candidates


@pytest.mark.asyncio
async def test_later_actor_does_not_inherit_initial_source_rediscovery(db, monkeypatch):
    from uuid import UUID
    from tests.test_active_session_creator_end_real_postgres import (
        _bind_protected_agent,
    )
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )

    case, source, _ = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, "pinned"
    )
    (candidate,) = (await db.list_current_session_creation_candidates()).candidates
    await _bind_protected_agent(db, UUID(case.thread_id))
    current = await db.get_thread(case.thread_id)
    assert current["agent_id"] is not None
    assert source["created_agent_id"] is None
    assert not (await db.list_current_session_creation_candidates()).candidates
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=case.provider, shutdown_event=asyncio.Event()
    )
    assert not await runner._continue(candidate)
    assert (await exact_source(db, case, "pinned"))["created_agent_id"] is None
    assert case.cluster.pod_create_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_initial_source_classifier_excludes_unknown_and_historical_authority(
    db, monkeypatch, lane
):
    import copy
    import json
    from orchestrator.database.session_creation_candidates import (
        SESSION_CREATION_SCAN_SQL,
        candidate_from_record,
    )
    from tests.test_active_session_creator_end_real_postgres import setup_case

    never_used = await setup_case(db, monkeypatch, lane=lane, protected_agent=False)
    assert not (await db.list_current_session_creation_candidates()).candidates
    assert never_used.cluster.objects == {}
    case, _, _ = await leave_exact_source_after_end_disconnect(db, monkeypatch, lane)
    raw = dict(
        await db.fetchrow(
            SESSION_CREATION_SCAN_SQL, UUID(case.thread_id), None, None, None, 2
        )
    )
    for field in ("owner", "source"):
        raw[field] = (
            json.loads(raw[field]) if isinstance(raw[field], str) else raw[field]
        )
    assert candidate_from_record(raw) is not None
    # These are discovery observations only. The real owner/source is untouched,
    # and every effect still requires the independent exact reread afterward.
    changes = [
        (("initial_source",), False),
        (("owner", "runtime_retirement_token"), str(uuid4())),
        (("owner", "status"), "ended"),
        (("owner", "metadata", "vm"), {"status": "deleted", "rootdisk": "kept"}),
        (("owner", "metadata", "_workspace_binding"), {"kind": "remote"}),
        (("owner", "metadata", "_pinned_retained_creation_attempt"), str(uuid4())),
        (("owner", "metadata", "_stateless_claim_retirement"), {}),
        (("owner", "metadata", "_stateless_claim_loss_hold"), {}),
        (
            ("owner", "metadata", "workspace_container", "_snapshot_restore_required"),
            True,
        ),
        (
            ("owner", "metadata", "workspace_container", "_snapshot_restore_required"),
            "false",
        ),
        (
            (
                "owner",
                "metadata",
                "workspace_container",
                "_canvas_workspace_generation",
            ),
            str(uuid4()),
        ),
        (("owner", "metadata", "workspace_container", "provisioner"), "docker"),
        (("owner", "metadata", "config_override", "workspace", "backend"), "vm"),
        (("owner", "metadata", "config_override", "workspace", "backend"), "virtual"),
        (("source", "pod_uid"), None),
        (("source", "pvc_uid"), None),
    ]
    if lane == "pinned":
        changes += [
            (("source", "previous_binding"), {"kind": "virtual"}),
            (("source", "retained_source_attempt_id"), str(uuid4())),
            (("source", "status"), "published"),
        ]
    else:
        changes += [
            (("source", "operation_kind"), "restore"),
            (("source", "settled_at"), datetime.now(timezone.utc).isoformat()),
            (("source", "cancel_requested_at"), datetime.now(timezone.utc).isoformat()),
            (
                (
                    "owner",
                    "metadata",
                    "workspace_container",
                    "_runtime_creation",
                    "replaces_uid",
                ),
                str(uuid4()),
            ),
        ]
    for path, value in changes:
        changed = copy.deepcopy(raw)
        target = changed
        for field in path[:-1]:
            target = target[field]
        target[path[-1]] = value
        assert candidate_from_record(changed) is None, path
    assert await db.current_session_creation_candidate_is_exact(
        candidate_from_record(raw)
    )


@pytest.mark.asyncio
async def test_0294_history_index_is_valid_after_full_chain(db):
    index = await db.fetchrow("""
        SELECT indisvalid, indisready, pg_get_indexdef(indexrelid) AS definition
        FROM pg_index WHERE indexrelid='managed_repository_workspace_creation_owner_history'::regclass
    """)
    assert index["indisvalid"] is True and index["indisready"] is True
    assert "(owner_kind, owner_id, scope, id)" in index["definition"]


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_successful_joined_ssh_probe_can_complete_after_observation_quantum(
    db, monkeypatch, lane
):
    import sys
    from orchestrator.services import ssh_helpers
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )

    case, source, original = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, lane
    )
    provider = reconstructed_provider(db, case, monkeypatch)
    make_ready(case, monkeypatch)
    provider._ssh_auth_ready_timeout = 20
    monkeypatch.setattr(
        ssh_helpers,
        "build_agent_ssh_cmd",
        lambda *a, **k: [sys.executable, "-c", "import time; time.sleep(2.2)"],
    )
    shutdown = asyncio.Event()
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=provider, shutdown_event=shutdown, round_delay_s=0.05
    )
    task = asyncio.create_task(runner.run())
    try:
        await wait_ready(db, case.thread_id, timeout=7)
        current = await db.get_thread(case.thread_id)
        result = await exact_source(db, case, lane)
        assert current["runtime_generation"] == original["runtime_generation"]
        assert result.get("id", result.get("attempt_id")) == source.get(
            "id", source.get("attempt_id")
        )
        assert result["pod_uid"] == source["pod_uid"]
        assert case.cluster.pod_create_calls == 1
    finally:
        shutdown.set()
        await asyncio.wait_for(asyncio.shield(task), 10)


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_slow_recorded_bundle_read_does_not_consume_observation_quantum(
    db, monkeypatch, lane
):
    import time
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )

    case, source, original = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, lane
    )
    provider = reconstructed_provider(db, case, monkeypatch)
    make_ready(case, monkeypatch)
    shutdown = asyncio.Event()
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=provider, shutdown_event=shutdown, round_delay_s=0.05
    )
    pending_delay = False
    delayed_reads = 0
    continue_actual = runner._continue
    read_actual = case.cluster.read_namespaced_pod

    async def continue_with_slow_preparation(candidate):
        nonlocal pending_delay
        pending_delay = True
        return await continue_actual(candidate)

    def slow_recorded_read(**kwargs):
        nonlocal pending_delay, delayed_reads
        if pending_delay:
            pending_delay = False
            delayed_reads += 1
            time.sleep(2.2)  # A real SDK thread remains joined under its guard.
        return read_actual(**kwargs)

    monkeypatch.setattr(runner, "_continue", continue_with_slow_preparation)
    monkeypatch.setattr(case.cluster, "read_namespaced_pod", slow_recorded_read)
    task = asyncio.create_task(runner.run())
    try:
        await wait_ready(db, case.thread_id, timeout=7)
        assert delayed_reads == 1
        current = await db.get_thread(case.thread_id)
        result = await exact_source(db, case, lane)
        assert current["runtime_generation"] == original["runtime_generation"]
        assert result.get("id", result.get("attempt_id")) == source.get(
            "id", source.get("attempt_id")
        )
        assert result["pod_uid"] == source["pod_uid"]
        assert case.cluster.pod_create_calls == 1
    finally:
        shutdown.set()
        await asyncio.wait_for(asyncio.shield(task), 10)


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_slow_first_exact_pod_observation_can_reach_successful_ssh(
    db, monkeypatch, lane
):
    import time
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )

    case, source, _ = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, lane
    )
    provider = reconstructed_provider(db, case, monkeypatch)
    make_ready(case, monkeypatch)
    pending_delay = False
    delayed_reads = 0
    wait_actual = provider._wait_for_ready
    read_actual = case.cluster.read_namespaced_pod

    async def slow_first_observation(*args, **kwargs):
        nonlocal pending_delay
        pending_delay = True
        return await wait_actual(*args, **kwargs)

    def read(**kwargs):
        nonlocal pending_delay, delayed_reads
        if pending_delay:
            pending_delay = False
            delayed_reads += 1
            time.sleep(2.2)
        return read_actual(**kwargs)

    monkeypatch.setattr(provider, "_wait_for_ready", slow_first_observation)
    monkeypatch.setattr(case.cluster, "read_namespaced_pod", read)
    shutdown = asyncio.Event()
    runner = SessionCreationContinuationRunner(
        db=db, provisioner=provider, shutdown_event=shutdown, round_delay_s=0.05
    )
    task = asyncio.create_task(runner.run())
    try:
        await wait_ready(db, case.thread_id, timeout=7)
        assert delayed_reads == 1
        assert (await exact_source(db, case, lane))["pod_uid"] == source["pod_uid"]
        assert case.cluster.pod_create_calls == 1
    finally:
        shutdown.set()
        await asyncio.wait_for(asyncio.shield(task), 10)
