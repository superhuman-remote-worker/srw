"""Attach must give a reclaimed durable inbox a consumer without new input."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from agent.api import persistent_app as pa
from agent.api.session_input import SessionInputRuntime
from agent.database.postgres_db import PostgresDB as AgentPostgresDB
from orchestrator import main
from orchestrator.application import controls
from tests import test_pinned_abrupt_death_real_postgres as abrupt
from tests import test_session_attach_characterization as attach
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent

pg_dsn = abrupt.pg_dsn
_base_schema = abrupt._base_schema
_base_db = abrupt._base_db
_schema_applied = abrupt._schema_applied
db = abrupt.db
_world = attach._world


@pytest.mark.asyncio
@pytest.mark.parametrize("partial", [False, True])
async def test_successor_attach_consumes_stranded_inbox_without_transport_or_input(
    db, pg_dsn, monkeypatch, partial
):
    ids, retirement, deliveries, _, _ = await abrupt.killed_life(
        db, monkeypatch, backend="virtual", with_virtual_binding=False, partial=partial
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
    actor = await db.get_agent(str(successor["agent_id"]))
    # The fixture's lifecycle port normally stands in for the status endpoint.
    # Set its known successor active; input claims still enforce the full real
    # reciprocal thread/agent/Pod/generation/attach-token authority in SQL.
    await db.update_thread_status(ids["thread"], "active")

    agent_db = AgentPostgresDB(pg_dsn, min_connections=1, max_connections=2)
    await agent_db.connect()
    tasks = []
    starts = []
    consumed = []
    expected = deliveries[1:] if partial else deliveries

    class Session(attach.FakeSession):
        async def setup(self, **kwargs):
            await super().setup(**kwargs)
            self.postgres_conn = agent_db

    async def consume():
        for turn in range(1, len(expected) + 1):
            item = pa._session_input.queue.get_nowait()
            consumed.append(item["delivery_id"])
            assert await pa._session_input.admit_delivery(
                item["delivery_id"], item["claim_generation"], turn
            )
            assert await pa._session_input.settle_delivery(
                item["delivery_id"], item["claim_generation"]
            )
        assert pa._session_input.queue.empty()

    def start(source):
        assert pa._session_ready()
        assert attach.EVENTS[-3:] == [
            ("recover", True),
            ("restore", True),
            ("watchdogs", True),
        ]
        starts.append(source)
        task = asyncio.create_task(consume(), name="test-successor-inbox-consumer")
        tasks.append(task)
        monkeypatch.setattr(pa, "_loop_task", task)
        return True

    try:
        monkeypatch.setenv("POD_UID", actor["pod_uid"])
        client = attach._client(
            generation=str(successor["runtime_generation"]),
            attach_token=str(successor["runtime_attach_token"]),
            contract=True,
        )
        client.agent_id = str(successor["agent_id"])
        workspace = attach._workspace(str(successor["runtime_generation"]))
        client.get_thread_workspace = AsyncMock(return_value=workspace)
        monkeypatch.setattr(pa, "_orchestrator_client", client)
        attach._poll(monkeypatch, workspace)
        attach.patch_collaborator(monkeypatch, "PersistentSession", Session)
        attach.patch_collaborator(monkeypatch, "_open_event_journal", AsyncMock())
        attach.patch_collaborator(monkeypatch, "_ensure_persistent_loop_started", start)
        monkeypatch.setattr(
            pa._session_input,
            "reclaim_pending",
            SessionInputRuntime.reclaim_pending.__get__(pa._session_input),
        )
        await asyncio.wait_for(attach.attach(thread_id=ids["thread"]), timeout=15)

        assert starts == ["attach_recovered_input"], (
            "successor reclaimed the inbox but attach gave it no consumer"
        )
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
        assert consumed == [str(item) for item in expected]
        assert await pa._session_input.reclaim_pending() == set()
        rows = await db.fetch(
            "SELECT d.delivery_id,d.state,d.admitted_turn_number,d.settled_at,d.owner_pod_uid "
            "FROM thread_input_deliveries d JOIN thread_messages m ON m.id=d.message_id "
            "WHERE d.thread_id=$1::uuid ORDER BY m.seq",
            ids["thread"],
        )
        recovered = rows[1:] if partial else rows
        assert [row["state"] for row in recovered] == ["settled", "settled"]
        assert [row["admitted_turn_number"] for row in recovered] == [1, 2]
        assert all(row["owner_pod_uid"] == actor["pod_uid"] for row in recovered)
        if partial:
            assert rows[0]["state"] == "admitted"
            assert rows[0]["settled_at"] is None
            assert rows[0]["owner_pod_uid"] == ids["pod_uid"]
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await agent_db.close()
