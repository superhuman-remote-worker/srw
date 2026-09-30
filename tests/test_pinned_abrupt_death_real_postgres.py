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
from orchestrator.services.agent_provisioner import AgentProvisioner
from shared.persistent_input_delivery import (
    persist_input_delivery,
    mark_input_delivery_queued,
    claim_pending_input_deliveries,
    transition_input_delivery,
)
from tests import test_persistent_recycler_real_postgres as fixtures
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent

pg_dsn = fixtures.pg_dsn
_base_schema = fixtures._schema_applied
_base_db = fixtures.db


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    await fixtures._schema_applied.__wrapped__(pg_dsn)
    migration = (
        Path(__file__).resolve().parents[1]
        / "src/orchestrator/database/migrations/app/0309_pinned_abrupt_actor_exit.sql"
    )
    if migration.exists():
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(migration.read_text())
        finally:
            await conn.close()


@pytest_asyncio.fixture
async def db(_base_db):
    yield _base_db


async def killed_life(db, monkeypatch, *, backend="none", claim=False, partial=False):
    pod_uid = str(uuid4())
    ids = await fixtures._seed(
        db, protected_agent_pod=True, workspace_claim=claim, pod_uid=pod_uid
    )
    ids["pod_uid"] = pod_uid
    if backend == "virtual":
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
            for index in range(2):
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
                if index == 0:
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
                    if partial:
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
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=False)
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
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
@pytest.mark.parametrize(
    "backend,claim", [("none", False), ("virtual", False), ("virtual", True)]
)
async def test_killed_life_zero_preserves_stranded_inputs(
    db, monkeypatch, backend, claim
):
    ids, retirement, deliveries, api, _ = await killed_life(
        db, monkeypatch, backend=backend, claim=claim
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
async def test_killed_life_end_resume_and_successor_claim_each_stranded_input_once(
    db, monkeypatch
):
    ids, retirement, deliveries, _, _ = await killed_life(
        db, monkeypatch, backend="virtual"
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
            assert [r["delivery_id"] for r in recovered] == deliveries
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


@pytest.mark.asyncio
async def test_killed_partial_turn_is_retired_without_fabricating_completion_or_replaying_effects(
    db, monkeypatch
):
    ids, retirement, deliveries, _, _ = await killed_life(db, monkeypatch, partial=True)
    assert await controls.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    rows = await db.fetch(
        "SELECT delivery_id,state,settled_at FROM thread_input_deliveries WHERE thread_id=$1::uuid ORDER BY persisted_at",
        ids["thread"],
    )
    assert [r["state"] for r in rows] == ["admitted", "owned"]
    assert rows[0]["settled_at"] is None
    assert rows[0]["delivery_id"] == deliveries[0]
