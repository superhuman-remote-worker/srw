"""Killed pinned life: truthful Pod zero, durable inbox and exact successor."""

import json
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from orchestrator import main
from orchestrator.application import controls
from orchestrator.services import agent_provisioner as agent_provider
from orchestrator.services import stale_agent_detector as detector
from orchestrator.services.agent_provisioner import AgentProvisioner
from shared.persistent_input_delivery import (
    persist_input_delivery,
    mark_input_delivery_queued,
    claim_pending_input_deliveries,
    transition_input_delivery,
)
from tests import test_persistent_recycler_real_postgres as fixtures
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent
from tests.test_pinned_retirement_retry_parity_real_postgres import (
    _run_one_detector_pass,
)

pg_dsn = fixtures.pg_dsn
_base_schema = fixtures._schema_applied
_base_db = fixtures.db


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    await fixtures._schema_applied.__wrapped__(pg_dsn)
    migration_root = (
        Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
    )
    conn = await asyncpg.connect(pg_dsn)
    try:
        for name in (
            "0313_pinned_abrupt_actor_exit.sql",
            "0317_pinned_virtual_without_backing_abrupt_exit.sql",
            "0335_pinned_abrupt_exit_subagents_and_earlier_lives.sql",
        ):
            migration = migration_root / name
            if migration.exists():
                await conn.execute(migration.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(_base_db):
    yield _base_db


async def killed_life(
    db,
    monkeypatch,
    *,
    backend="none",
    claim=False,
    partial=False,
    with_virtual_binding=True,
    authorized=True,
    before_retirement=None,
    settle_status="ended",
):
    pod_uid = str(uuid4())
    ids = await fixtures._seed(
        db, protected_agent_pod=True, workspace_claim=claim, pod_uid=pod_uid
    )
    ids["pod_uid"] = pod_uid
    if backend == "virtual" and with_virtual_binding:
        assert await db.bind_thread_workspace_backing(
            ids["thread"], backing_kind="virtual", backing_id=f"rclone:{'a' * 64}"
        )
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(jsonb_set(metadata,'{config_override,workspace,backend}',to_jsonb($2::text)),"
        "'{config_override,officer,enabled}','false'::jsonb) WHERE id=$1::uuid",
        UUID(ids["thread"]),
        backend,
    )
    generation = str((await db.get_thread(ids["thread"]))["runtime_generation"])
    process_generation = str(uuid4())
    deliveries = []
    async with db.acquire() as conn:
        async with conn.transaction():
            for index in range(3 if partial else 2):
                row = await persist_input_delivery(
                    conn,
                    thread_id=ids["thread"],
                    delivery_id=uuid4(),
                    role="human",
                    content=f"stranded input {index}",
                    source="direct_human",
                    turn_number=1,
                    agent_id=ids["agent"],
                    pod_uid=pod_uid,
                    runtime_generation=process_generation,
                    session_runtime_generation=generation,
                    runtime_attach_token=ids["attach_token"],
                )
                if index == 0 or (partial and index == 1):
                    assert await mark_input_delivery_queued(
                        conn,
                        delivery_id=row["delivery_id"],
                        agent_id=ids["agent"],
                        pod_uid=pod_uid,
                        runtime_generation=process_generation,
                        session_runtime_generation=generation,
                        runtime_attach_token=ids["attach_token"],
                        claim_generation=int(row["claim_generation"]),
                    )
                    if partial and index == 0:
                        assert await transition_input_delivery(
                            conn,
                            delivery_id=row["delivery_id"],
                            agent_id=ids["agent"],
                            pod_uid=pod_uid,
                            runtime_generation=process_generation,
                            session_runtime_generation=generation,
                            runtime_attach_token=ids["attach_token"],
                            claim_generation=int(row["claim_generation"]),
                            transition="admitted",
                            turn_number=1,
                        )
                deliveries.append(row["delivery_id"])
    if before_retirement is not None:
        ids["generation"] = generation
        ids["process_generation"] = process_generation
        await before_retirement(ids, deliveries)
    retirement = await db.begin_pinned_thread_retirement(
        ids["thread"], permanent=False, settle_status=settle_status
    )
    if authorized:
        assert await db.authorize_pinned_thread_retirement(
            ids["thread"],
            token=retirement["token"],
            generation=retirement["generation"],
            settle_status=settle_status,
        )
    api = fixtures.StatefulPinnedK8sApi()
    pod_name = f"persistent-{ids['thread'][:12]}"
    api.install_old_pod(
        namespace="agents-a",
        name=pod_name,
        uid=pod_uid,
        labels={
            "srw/thread-id": ids["thread"],
            "srw.io/runtime-generation": generation,
            "srw.io/provision-attempt": ids["provision_attempt"],
            "srw/component": "persistent-agent",
        },
    )
    api.mark_terminal("agents-a", pod_name)
    pod = api.pods[("agents-a", pod_name)]
    pod.spec = NS(
        containers=[NS(name="agent")],
        init_containers=[],
        ephemeral_containers=[],
        volumes=[],
    )
    pod.status.phase = "Failed"
    pod.status.container_statuses[0].name = "agent"
    pod.status.container_statuses[0].state.terminated.exit_code = 137
    pod.status.container_statuses[0].state.terminated.signal = 9
    provider = AgentProvisioner()
    provider._k8s_available = True
    provider._core_api = api
    monkeypatch.setattr(agent_provider, "agent_provisioner", provider)
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(
        main.app.state.resources.session_router,
        "teardown_route",
        AsyncMock(return_value=True),
    )
    return ids, retirement, deliveries, api, provider


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["none", "virtual"])
@pytest.mark.parametrize("partial", [False, True])
async def test_detector_settles_captured_sigkill_before_full_grace(
    db, monkeypatch, backend, partial
):
    """Exercise nomination and retry, rather than calling recovery directly."""
    ids, retirement, deliveries, api, _ = await killed_life(
        db,
        monkeypatch,
        backend=backend,
        partial=partial,
        with_virtual_binding=False,
    )
    for pod in api.pods.values():
        pod.spec.restart_policy = "Never"
    # Begin's timestamp is immutable. Exercise the shorter nomination clock
    # through the detector's explicit policy, without rewriting that ledger.
    monkeypatch.setattr(
        detector, "PINNED_RETIREMENT_TERMINAL_PROBE_GRACE_SECONDS", 0, raising=False
    )
    await db.execute(
        "UPDATE agents SET last_heartbeat=now()-interval '5 minutes' WHERE id=$1::uuid",
        ids["agent"],
    )

    await _run_one_detector_pass()

    current = await db.get_thread(ids["thread"])
    assert current["status"] == "ended"
    assert current["runtime_retirement_token"] is None
    assert current["agent_id"] is None
    assert not api.pods
    outcomes = await db.fetch(
        "SELECT runtime_generation,retirement_token,disposition,outcome "
        "FROM thread_runtime_retirement_outcomes WHERE thread_id=$1::uuid",
        ids["thread"],
    )
    assert len(outcomes) == 1
    assert str(outcomes[0]["runtime_generation"]) == retirement["generation"]
    assert str(outcomes[0]["retirement_token"]) == retirement["token"]
    assert outcomes[0]["disposition"] == "ended"
    assert outcomes[0]["outcome"] == "settled"
    rows = await db.fetch(
        "SELECT delivery_id,state FROM thread_input_deliveries "
        "WHERE thread_id=$1::uuid ORDER BY persisted_at",
        ids["thread"],
    )
    assert [row["delivery_id"] for row in rows] == deliveries
    assert [row["state"] for row in rows] == (
        ["admitted", "queued", "owned"] if partial else ["queued", "owned"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "live",
        "unknown",
        "replacement",
        "missing-uid",
        "absent",
        "api-error",
        "unobserved-container",
        "unobserved-init",
        "unobserved-ephemeral",
        "running-ephemeral",
        "restarting",
        "pod-gc",
    ],
)
async def test_early_detector_refuses_ambiguous_actor_without_effects(
    db, monkeypatch, fault
):
    ids, retirement, deliveries, api, _ = await killed_life(
        db, monkeypatch, backend="virtual", with_virtual_binding=False
    )
    pod = next(iter(api.pods.values()))
    pod.spec.restart_policy = "Never"
    if fault == "live":
        api.mark_ready(*next(iter(api.pods)))
    elif fault == "unknown":
        pod.status.container_statuses = []
        pod.status.phase = "Unknown"
    elif fault == "replacement":
        pod.metadata.uid = str(uuid4())
    elif fault == "missing-uid":
        pod.metadata.uid = None
    elif fault == "absent":
        api.pods.clear()
    elif fault == "api-error":

        def unavailable(**kwargs):
            raise fixtures._K8sError(503)

        monkeypatch.setattr(api, "read_namespaced_pod", unavailable)
    elif fault.startswith("unobserved-"):
        field = {
            "unobserved-container": "containers",
            "unobserved-init": "init_containers",
            "unobserved-ephemeral": "ephemeral_containers",
        }[fault]
        getattr(pod.spec, field).append(NS(name="unobserved-writer"))
    elif fault == "running-ephemeral":
        pod.spec.ephemeral_containers = [NS(name="writer")]
        pod.status.ephemeral_container_statuses = [
            NS(name="writer", state=NS(running=NS(), terminated=None))
        ]
    elif fault == "restarting":
        pod.spec.restart_policy = "Always"
    elif fault == "pod-gc":
        pod.status.conditions = [NS(type="DisruptionTarget", reason="DeletionByPodGC")]
    initial_pods = dict(api.pods)
    mutations = list(api.mutation_timeouts)
    assert not await controls.pinned_retirement_operations(
        main.app.state.resources
    ).captured_agent_is_terminal(retirement)
    monkeypatch.setattr(detector, "PINNED_RETIREMENT_TERMINAL_PROBE_GRACE_SECONDS", 0)
    await db.execute(
        "UPDATE agents SET last_heartbeat=now()-interval '5 minutes' WHERE id=$1::uuid",
        ids["agent"],
    )
    await _run_one_detector_pass()

    candidates = await db.list_retryable_pinned_retirements(
        terminal_probe_grace_seconds=0
    )
    assert len(candidates) == 1 and candidates[0]["nominated_before_grace"]

    current = await db.get_thread(ids["thread"])
    assert current["status"] == "active"
    assert str(current["runtime_retirement_token"]) == retirement["token"]
    assert current["runtime_retirement_local_quiescence"] is None
    assert api.pods == initial_pods
    assert api.mutation_timeouts == mutations
    rows = await db.fetch(
        "SELECT delivery_id,state FROM thread_input_deliveries "
        "WHERE thread_id=$1::uuid ORDER BY persisted_at",
        ids["thread"],
    )
    assert [row["delivery_id"] for row in rows] == deliveries
    assert [row["state"] for row in rows] == ["queued", "owned"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["generation", "token", "context", "permanent", "unauthorized"]
)
async def test_early_terminal_probe_refuses_stale_authority_before_kubernetes(
    db, monkeypatch, fault
):
    ids, retirement, _, _, provider = await killed_life(
        db, monkeypatch, authorized=fault != "unauthorized"
    )
    retirement = dict(retirement)
    if fault in {"generation", "token"}:
        retirement[fault] = str(uuid4())
    elif fault == "context":
        retirement["context"] = {**retirement["context"], "agent_id": str(uuid4())}
    elif fault == "permanent":
        retirement["permanent"] = True
    probe = AsyncMock(return_value=True)
    monkeypatch.setattr(provider, "agent_pod_terminal_process_zero", probe)
    assert not await controls.pinned_retirement_operations(
        main.app.state.resources
    ).captured_agent_is_terminal(retirement)
    probe.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend,claim,with_virtual_binding",
    [
        ("none", False, True),
        ("virtual", False, True),
        ("virtual", True, True),
        ("virtual", False, False),
    ],
)
async def test_killed_life_zero_preserves_stranded_inputs(
    db, monkeypatch, backend, claim, with_virtual_binding
):
    ids, retirement, deliveries, api, _ = await killed_life(
        db,
        monkeypatch,
        backend=backend,
        claim=claim,
        with_virtual_binding=with_virtual_binding,
    )
    operations = controls.pinned_retirement_operations(main.app.state.resources)
    assert await operations.recover_captured_process_zero(retirement)
    assert not api.pods
    current = await db.get_thread(ids["thread"])
    receipt = fixtures._json(current["runtime_retirement_local_quiescence"])
    assert receipt["quiescence_protocol"] == "agent_runtime_zero_v1"
    assert receipt["recovery_protocol"].startswith("abrupt_")
    rows = await db.fetch(
        "SELECT delivery_id,state FROM thread_input_deliveries WHERE thread_id=$1::uuid ORDER BY persisted_at",
        ids["thread"],
    )
    assert [r["state"] for r in rows] == ["queued", "owned"]
    assert [r["delivery_id"] for r in rows] == deliveries


@pytest.mark.asyncio
@pytest.mark.parametrize("with_virtual_binding", [True, False])
@pytest.mark.parametrize("partial", [False, True])
async def test_killed_life_end_resume_and_successor_claim_each_stranded_input_once(
    db, monkeypatch, with_virtual_binding, partial
):
    ids, retirement, deliveries, _, _ = await killed_life(
        db,
        monkeypatch,
        backend="virtual",
        with_virtual_binding=with_virtual_binding,
        partial=partial,
    )
    assert await controls.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    current = await db.get_thread(ids["thread"])
    result = await controls.thread_retirement_operations(
        main.app.state.resources
    ).end_thread_flow(
        ids["thread"],
        current,
        permanent=False,
        force=True,
        expected_runtime_generation=retirement["generation"],
        expected_agent_id=ids["agent"],
        expected_attach_token=ids["attach_token"],
        local_runtime_quiesced=True,
    )
    assert result["status"] == "ended"
    assert await db.resume_thread(ids["thread"])
    successor = await _bind_cold_agent(db, ids["thread"])
    assert str(successor["runtime_generation"]) != retirement["generation"]
    actor = await db.get_agent(str(successor["agent_id"]))
    process = str(uuid4())
    async with db.acquire() as conn:
        async with conn.transaction():
            recovered = await claim_pending_input_deliveries(
                conn,
                thread_id=ids["thread"],
                agent_id=str(successor["agent_id"]),
                pod_uid=actor["pod_uid"],
                runtime_generation=process,
                session_runtime_generation=str(successor["runtime_generation"]),
                runtime_attach_token=str(successor["runtime_attach_token"]),
            )
            assert [r["delivery_id"] for r in recovered] == (
                deliveries[1:] if partial else deliveries
            )
            for row in recovered:
                authority = dict(
                    delivery_id=row["delivery_id"],
                    agent_id=str(successor["agent_id"]),
                    pod_uid=actor["pod_uid"],
                    runtime_generation=process,
                    session_runtime_generation=str(successor["runtime_generation"]),
                    runtime_attach_token=str(successor["runtime_attach_token"]),
                    claim_generation=int(row["claim_generation"]),
                )
                assert await mark_input_delivery_queued(conn, **authority)
                assert await transition_input_delivery(
                    conn, **authority, transition="admitted", turn_number=1
                )
                assert await transition_input_delivery(
                    conn, **authority, transition="settled", turn_number=1
                )
            assert (
                await claim_pending_input_deliveries(
                    conn,
                    thread_id=ids["thread"],
                    agent_id=str(successor["agent_id"]),
                    pod_uid=actor["pod_uid"],
                    runtime_generation=process,
                    session_runtime_generation=str(successor["runtime_generation"]),
                    runtime_attach_token=str(successor["runtime_attach_token"]),
                )
                == []
            )
    if partial:
        original = await db.fetchrow(
            "SELECT state,admitted_turn_number,settled_at FROM thread_input_deliveries WHERE delivery_id=$1::uuid",
            deliveries[0],
        )
        assert original["state"] == "admitted"
        assert original["admitted_turn_number"] == 1
        assert original["settled_at"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend,with_virtual_binding", [("none", True), ("virtual", False)]
)
async def test_killed_partial_turn_is_retired_without_fabricating_completion_or_replaying_effects(
    db, monkeypatch, backend, with_virtual_binding
):
    ids, retirement, deliveries, _, _ = await killed_life(
        db,
        monkeypatch,
        partial=True,
        backend=backend,
        with_virtual_binding=with_virtual_binding,
    )
    assert await controls.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    rows = await db.fetch(
        "SELECT delivery_id,state,settled_at FROM thread_input_deliveries WHERE thread_id=$1::uuid ORDER BY persisted_at",
        ids["thread"],
    )
    assert [r["state"] for r in rows] == ["admitted", "queued", "owned"]
    assert rows[0]["settled_at"] is None
    assert rows[0]["delivery_id"] == deliveries[0]


@pytest.mark.asyncio
async def test_killed_virtual_actor_without_backing_has_exact_abrupt_sql_receipt(
    db, monkeypatch
):
    ids, retirement, _, _, _ = await killed_life(
        db, monkeypatch, backend="virtual", with_virtual_binding=False
    )
    receipt = await db.acknowledge_abrupt_pinned_actor_exit(
        ids["thread"],
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
        agent_id=ids["agent"],
        attach_token=ids["attach_token"],
        stopped_pod_uid=ids["pod_uid"],
    )
    assert receipt is not None
    assert receipt["recovery_protocol"] == "abrupt_virtual_actor_exit_v1"
    assert receipt["stranded_input_count"] == 2
    assert receipt["partial_admission_count"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "binding", [{"kind": "virtual"}, {"kind": "sandbox", "host": "separate-writer"}]
)
async def test_virtual_absence_receipt_refuses_new_current_binding(
    db, monkeypatch, binding
):
    ids, retirement, _, _, _ = await killed_life(
        db, monkeypatch, backend="virtual", with_virtual_binding=False
    )
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{_workspace_binding}',$2::jsonb) WHERE id=$1::uuid",
        ids["thread"],
        json.dumps(binding),
    )
    assert (
        await db.acknowledge_abrupt_pinned_actor_exit(
            ids["thread"],
            runtime_generation=retirement["generation"],
            retirement_token=retirement["token"],
            agent_id=ids["agent"],
            attach_token=ids["attach_token"],
            stopped_pod_uid=ids["pod_uid"],
        )
        is None
    )
    assert not await controls.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    assert (await db.get_thread(ids["thread"]))[
        "runtime_retirement_local_quiescence"
    ] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend,with_virtual_binding", [("none", True), ("virtual", False)]
)
@pytest.mark.parametrize(
    "defect",
    ["replacement_uid", "unavailable_proof", "stale_generation", "stale_token"],
)
async def test_killed_life_recovery_refuses_unproven_or_superseded_runtime(
    db, monkeypatch, defect, backend, with_virtual_binding
):
    ids, retirement, deliveries, api, provider = await killed_life(
        db, monkeypatch, backend=backend, with_virtual_binding=with_virtual_binding
    )
    if defect == "replacement_uid":
        pod = next(iter(api.pods.values()))
        pod.metadata.uid = str(uuid4())
    elif defect == "unavailable_proof":

        def unavailable(*args, **kwargs):
            raise fixtures._K8sError(503)

        api.read_namespaced_pod = unavailable
    elif defect == "stale_generation":
        retirement = {**retirement, "generation": str(uuid4())}
    else:
        retirement = {**retirement, "token": str(uuid4())}
    if defect in {"replacement_uid", "unavailable_proof"}:
        pod_name = f"persistent-{ids['thread'][:12]}"
        assert await provider.agent_pod_authority(
            pod_name, expected_pod_uid=ids["pod_uid"], namespace="agents-a"
        ) == ("replacement" if defect == "replacement_uid" else "unknown")
    assert not await controls.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    current = await db.get_thread(ids["thread"])
    assert current["runtime_retirement_local_quiescence"] is None
    if defect in {"stale_generation", "stale_token"}:
        assert api.pods, "a stale request must refuse before its first Pod effect"
    rows = await db.fetch(
        "SELECT delivery_id,state FROM thread_input_deliveries WHERE thread_id=$1::uuid ORDER BY persisted_at",
        ids["thread"],
    )
    assert [r["delivery_id"] for r in rows] == deliveries
    assert [r["state"] for r in rows] == ["queued", "owned"]


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["sandbox", "vm"])
async def test_killed_agent_pod_cannot_certify_ambiguous_remote_writers(
    db, monkeypatch, backend
):
    ids, retirement, deliveries, api, _ = await killed_life(
        db, monkeypatch, backend=backend
    )
    # Even truthful terminal agent-Pod evidence says nothing about a separate
    # workspace process namespace. Neither the API owner nor SQL may borrow
    # the lite/virtual shortcut without that namespace's exact proof.
    assert (
        await db.acknowledge_abrupt_pinned_actor_exit(
            ids["thread"],
            runtime_generation=retirement["generation"],
            retirement_token=retirement["token"],
            agent_id=ids["agent"],
            attach_token=ids["attach_token"],
            stopped_pod_uid=ids["pod_uid"],
        )
        is None
    )
    assert not await controls.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    current = await db.get_thread(ids["thread"])
    assert current["runtime_retirement_local_quiescence"] is None
    rows = await db.fetch(
        "SELECT delivery_id,state FROM thread_input_deliveries WHERE thread_id=$1::uuid ORDER BY persisted_at",
        ids["thread"],
    )
    assert [r["delivery_id"] for r in rows] == deliveries
    assert [r["state"] for r in rows] == ["queued", "owned"]
