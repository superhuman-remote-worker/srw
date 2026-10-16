"""Stateless → pinned VM upgrade (C2) against the production schema snapshot.

All C2 real-PG cases live here (C1's rule): the reliability stream's suites
stay unedited and run as regression neighbours. Fixtures are C1's.
"""

from __future__ import annotations

import json
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from orchestrator.services.manifest_workspace_selection import (
    select_execution_workspace,
)
from orchestrator.services.vm_upgrade_seed import (
    PINNED_IDENTITY_MISMATCH,
    agent_vm_upgrade_seed_source,
    mark_vm_upgrade_seeded,
)
from tests import test_stateless_workspace_upgrade_real_postgres as c1
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent

actor = c1.actor
database = c1.database
postgres_url = c1.postgres_url
metadata = c1.metadata
lite_session = c1.lite_session


async def pinned_virtual_session(database, actor):
    workspace, selection = await select_execution_workspace(
        database,
        actor,
        role="session",
        project_id=None,
        supplied=True,
        workspace={"template": {"inline": {"backend": "virtual"}}},
    )
    return await database.create_thread(
        user_id=str(actor["id"]),
        datasource_ids=[],
        execution_lane="pinned",
        initial_metadata={"config_override": {"workspace": workspace}},
        workspace_selection=selection,
    )


async def bound_with_pending_marker(database, actor):
    thread_id = await pinned_virtual_session(database, actor)
    bound = await _bind_cold_agent(database, thread_id)
    generation, request_id = str(uuid4()), str(uuid4())
    assert await database.merge_thread_vm_context(
        str(thread_id),
        {
            "status": "ready",
            "provision_generation": generation,
            "upgrade_seed": {
                "control_request_id": request_id,
                "state": "pending",
                "source": None,
            },
        },
    )
    return thread_id, str(bound["agent_id"]), generation, request_id


@pytest.mark.asyncio
async def test_seeded_marks_the_exact_provision_once_and_idempotently(database, actor):
    thread_id, agent_id, generation, request_id = await bound_with_pending_marker(
        database, actor
    )
    for _ in range(2):
        assert await mark_vm_upgrade_seeded(
            database,
            str(thread_id),
            agent_id=agent_id,
            provision_generation=generation,
            control_request_id=request_id,
        )
    vm = metadata(await database.get_thread(thread_id))["vm"]
    assert vm["upgrade_seed"]["state"] == "seeded"
    assert vm["provision_generation"] == generation


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong", ["generation", "request", "agent"])
async def test_seeded_refuses_a_stale_provision_generation(database, actor, wrong):
    """Review Focus 3: a stale agent can't mark a newer VM as seeded."""
    thread_id, agent_id, generation, request_id = await bound_with_pending_marker(
        database, actor
    )
    kwargs = {
        "agent_id": agent_id,
        "provision_generation": generation,
        "control_request_id": request_id,
    }
    kwargs[
        {
            "generation": "provision_generation",
            "request": "control_request_id",
            "agent": "agent_id",
        }[wrong]
    ] = str(uuid4())
    assert not await mark_vm_upgrade_seeded(database, str(thread_id), **kwargs)
    vm = metadata(await database.get_thread(thread_id))["vm"]
    assert vm["upgrade_seed"]["state"] == "pending"


async def seed_source(database, thread):
    """``seed-source`` as the bound runtime presents itself."""
    request = SimpleNamespace(
        headers={
            "X-Agent-ID": str(thread["agent_id"]),
            "X-Session-Runtime-Generation": str(thread["runtime_generation"]),
            "X-Session-Runtime-Attach-Token": str(thread["runtime_attach_token"]),
        }
    )
    return await agent_vm_upgrade_seed_source(
        request,
        str(thread["id"]),
        dependencies=SimpleNamespace(require_internal=AsyncMock(), store=database),
    )


class _RollBack(Exception):
    """Leaves the half-unbound agent uncommitted: its deferred fence refuses it."""


@pytest.mark.asyncio
async def test_seed_source_refuses_an_agent_that_no_longer_points_at_the_session(
    database, actor
):
    thread_id, agent_id, generation, request_id = await bound_with_pending_marker(
        database, actor
    )
    await database.execute(
        "INSERT INTO thread_control_requests "
        "(id,thread_id,request_seq,client_request_id,verb,requested_by) "
        "VALUES ($1::uuid,$2::uuid,1,$3::uuid,'upgrade-to-workspace','owner')",
        request_id,
        str(thread_id),
        str(uuid4()),
    )
    thread = await database.get_thread(thread_id)
    served = await seed_source(database, thread)
    assert served["seed_from"] is None
    assert served["control_request_id"] == request_id
    assert served["provision_generation"] == generation
    with pytest.raises(_RollBack):
        async with database.transaction_scope():
            await database.execute(
                "UPDATE agents SET thread_id = NULL WHERE id = $1::uuid", agent_id
            )
            with pytest.raises(HTTPException) as refused:
                await seed_source(database, thread)
            assert refused.value.status_code == 409
            assert refused.value.detail == PINNED_IDENTITY_MISMATCH
            raise _RollBack


from shared.event_journal import append_system_frame, bump_epoch  # noqa: E402
from shared.run_queue import (  # noqa: E402
    UNIT_KIND_SESSION_TURN,
    claim_unit,
    fence_lease,
    record_input_seq,
)


async def quiet_queue(database, thread_id):
    """A session queue row with nothing pending: a turn ran and finished."""
    async with database.acquire() as conn:
        await record_input_seq(
            conn, unit_id=thread_id, unit_kind=UNIT_KIND_SESSION_TURN, input_seq=1
        )
        await conn.execute(
            "UPDATE run_queue SET state = 'done', consumed_seq = input_seq "
            "WHERE unit_id = $1::uuid",
            thread_id,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["virtual", "none"])
async def test_probe_the_switch_writes_pass_every_current_trigger(
    database, actor, backend
):
    """Probe (spec §2 "What the triggers need", re-run on develop's threads
    triggers): the switch's writes, in its order, on a quiet lite row."""
    thread_id = await lite_session(database, actor, backend=backend)
    await quiet_queue(database, thread_id)
    generation = await database.fetchval(
        "SELECT runtime_generation FROM threads WHERE id = $1::uuid", thread_id
    )
    async with database.acquire() as conn:
        async with conn.transaction():
            hwm = await conn.fetchval(
                "SELECT control_seq_hwm FROM threads WHERE id = $1::uuid FOR UPDATE",
                thread_id,
            )
            await conn.fetchval(
                "SELECT lease_token FROM run_queue WHERE unit_id = $1::uuid FOR UPDATE",
                thread_id,
            )
            seq = int(hwm or 0) + 1
            await conn.execute(
                "UPDATE threads SET control_seq_hwm = $2 WHERE id = $1::uuid",
                thread_id,
                seq,
            )
            await conn.execute(
                "INSERT INTO thread_control_requests (thread_id, request_seq, "
                "client_request_id, verb, payload, requested_by, accepted_agent_id, "
                "runtime_generation) VALUES ($1::uuid, $2, $3, "
                "'upgrade-to-workspace', $4::jsonb, 'probe', NULL, $5::uuid)",
                thread_id,
                seq,
                uuid4(),
                json.dumps(
                    {"runtime_generation": str(generation), "target_tier": "vm"}
                ),
                generation,
            )
            await conn.execute(
                "UPDATE run_queue SET lease_token = lease_token + 1, state = 'done', "
                "leased_by = NULL, last_leased_by = NULL, leased_until = NULL "
                "WHERE unit_id = $1::uuid",
                thread_id,
            )
            await bump_epoch(conn, thread_id=str(thread_id))
            await append_system_frame(
                conn,
                thread_id=str(thread_id),
                kind="session.lane_changed",
                payload={"probe": True},
            )
            await conn.execute(
                "UPDATE threads SET execution_lane = 'pinned', "
                "metadata = metadata - '_stateless_active_claim' WHERE id = $1::uuid",
                thread_id,
            )
    after = await database.get_thread(thread_id)
    assert after["execution_lane"] == "pinned"
    assert after["runtime_generation"] == generation


from orchestrator.services import stateless_input_admission  # noqa: E402
from orchestrator.services.stateless_vm_upgrade_handoff import (  # noqa: E402
    VM_UPGRADE_BACKGROUND_WORK,
    VM_UPGRADE_INPUT_PENDING,
    VM_UPGRADE_SESSION_ENDING,
    begin_stateless_vm_upgrade_handoff,
)
from orchestrator.services.stateless_workspace_upgrade import (  # noqa: E402
    VM_UPGRADE_NOT_POSSIBLE,
)
from orchestrator.services.thread_control_inbox import (  # noqa: E402
    ControlAdmissionError,
    ControlAdmissionNotReady,
    admit_thread_control,
)
from shared.cloud_push_tasks import bg_push_lease_is_current  # noqa: E402
from shared.thread_presence import refresh_thread_presence  # noqa: E402


async def switch(database, actor, thread_id, *, client_request_id=None):
    thread = await database.get_thread(thread_id)
    payload = {
        "runtime_generation": str(thread["runtime_generation"]),
        "target_tier": "vm",
    }
    async with database.stateless_session_workspace_ensure_lock(
        str(thread_id), wait=True
    ) as owned:
        assert owned
        return await begin_stateless_vm_upgrade_handoff(
            database,
            thread_id=thread_id,
            owner_user_id=actor["id"],
            client_request_id=client_request_id or uuid4(),
            payload=payload,
            requested_by="real-pg",
        )


async def upgrade_requests(database, thread_id):
    return await database.fetch(
        "SELECT id, payload, outcome, accepted_agent_id, runtime_generation "
        "FROM thread_control_requests "
        "WHERE thread_id = $1::uuid AND verb = 'upgrade-to-workspace'",
        thread_id,
    )


async def assert_not_switched(database, thread_id, before):
    after = await database.get_thread(thread_id)
    assert after["execution_lane"] == "stateless"
    assert metadata(after) == metadata(before)
    assert not await upgrade_requests(database, thread_id)


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["virtual", "none"])
async def test_the_switch_leaves_the_spec_success_state(database, actor, backend):
    thread_id = await lite_session(database, actor, backend=backend)
    await quiet_queue(database, thread_id)
    await database.execute(
        "UPDATE threads SET metadata = metadata || $2::jsonb WHERE id = $1::uuid",
        thread_id,
        json.dumps({"_stateless_active_claim": {"lease_token": 1, "pod": "p-1"}}),
    )
    before = await database.get_thread(thread_id)
    epoch_before = await database.fetchval(
        "SELECT events_epoch FROM threads WHERE id = $1::uuid", thread_id
    )
    client_request_id = uuid4()

    admitted = await switch(
        database, actor, thread_id, client_request_id=client_request_id
    )

    after = await database.get_thread(thread_id)
    meta = metadata(after)
    assert after["execution_lane"] == "pinned"
    assert (after["status"], after["runtime_generation"]) == (
        before["status"],
        before["runtime_generation"],
    )
    assert after["agent_id"] is None
    assert "_stateless_active_claim" not in meta
    assert meta.get("_workspace_binding") == metadata(before).get("_workspace_binding")
    queue = await database.fetchrow(
        "SELECT state, lease_token, leased_by, last_leased_by, leased_until "
        "FROM run_queue WHERE unit_id = $1::uuid",
        thread_id,
    )
    assert (queue["state"], int(queue["lease_token"])) == ("done", 1)
    assert queue["leased_by"] is None and queue["last_leased_by"] is None
    assert queue["leased_until"] is None
    epoch = await database.fetchval(
        "SELECT events_epoch FROM threads WHERE id = $1::uuid", thread_id
    )
    assert epoch == epoch_before + 1
    frame = await database.fetchrow(
        "SELECT kind, payload, control_request_id FROM thread_events "
        "WHERE thread_id = $1::uuid AND epoch = $2 ORDER BY seq DESC LIMIT 1",
        thread_id,
        epoch,
    )
    assert frame["kind"] == "session.lane_changed"
    assert frame["control_request_id"] is None
    assert _json(frame["payload"]) == {
        "from": "stateless",
        "to": "pinned",
        "reason": "vm_upgrade",
        "control_request_id": str(admitted.id),
        "client_request_id": str(client_request_id),
        "session_runtime_generation": str(before["runtime_generation"]),
    }
    [request] = await upgrade_requests(database, thread_id)
    assert request["id"] == admitted.id
    assert request["outcome"] is None and request["accepted_agent_id"] is None
    assert request["runtime_generation"] == before["runtime_generation"]


@pytest.mark.asyncio
async def test_a_same_uuid_retry_after_the_switch_returns_the_original_admission(
    database, actor
):
    thread_id = await lite_session(database, actor)
    client_request_id = uuid4()
    first = await switch(
        database, actor, thread_id, client_request_id=client_request_id
    )
    replay = await switch(
        database, actor, thread_id, client_request_id=client_request_id
    )
    assert (replay.id, replay.duplicate) == (first.id, True)
    assert len(await upgrade_requests(database, thread_id)) == 1


@pytest.mark.asyncio
async def test_a_leased_turn_defers_the_switch(database, actor):
    thread_id = await lite_session(database, actor)
    async with database.acquire() as conn:
        await record_input_seq(
            conn, unit_id=thread_id, unit_kind=UNIT_KIND_SESSION_TURN, input_seq=5
        )
        assert await claim_unit(conn, unit_kind=UNIT_KIND_SESSION_TURN, pod_name="p-1")
    before = await database.get_thread(thread_id)
    with pytest.raises(ControlAdmissionNotReady, match="completing a turn"):
        await switch(database, actor, thread_id)
    await assert_not_switched(database, thread_id, before)


@pytest.mark.asyncio
async def test_input_committed_first_defers_the_switch(database, actor):
    """Review Focus 1."""
    thread_id = await lite_session(database, actor)
    async with database.acquire() as conn:
        await record_input_seq(
            conn, unit_id=thread_id, unit_kind=UNIT_KIND_SESSION_TURN, input_seq=5
        )
    before = await database.get_thread(thread_id)
    with pytest.raises(ControlAdmissionNotReady) as refused:
        await switch(database, actor, thread_id)
    assert str(refused.value) == VM_UPGRADE_INPUT_PENDING
    await assert_not_switched(database, thread_id, before)


@pytest.mark.asyncio
async def test_a_parked_queue_refuses_the_switch(database, actor):
    thread_id = await lite_session(database, actor)
    await quiet_queue(database, thread_id)
    await database.execute(
        "UPDATE run_queue SET state = 'parked', park_reason = 'real-pg' "
        "WHERE unit_id = $1::uuid",
        thread_id,
    )
    before = await database.get_thread(thread_id)
    with pytest.raises(ControlAdmissionError, match="parked") as refused:
        await switch(database, actor, thread_id)
    assert type(refused.value) is ControlAdmissionError
    await assert_not_switched(database, thread_id, before)


@pytest.mark.asyncio
async def test_a_pending_stateless_control_defers_the_switch(database, actor):
    """Settled difference 4: one upgrade (or control) at a time."""
    thread_id = await lite_session(database, actor)
    generation = (await database.get_thread(thread_id))["runtime_generation"]
    await admit_thread_control(
        database,
        thread_id=thread_id,
        owner_user_id=actor["id"],
        client_request_id=uuid4(),
        verb="mode.set",
        payload={"mode": "supervised"},
        requested_by="real-pg",
        expected_runtime_generation=generation,
    )
    with pytest.raises(ControlAdmissionNotReady) as refused:
        await switch(database, actor, thread_id)
    assert str(refused.value) == VM_UPGRADE_BACKGROUND_WORK
    assert (await database.get_thread(thread_id))["execution_lane"] == "stateless"


@pytest.mark.asyncio
async def test_a_pending_container_upgrade_makes_the_switch_impossible(database, actor):
    """Settled difference 4, as built (S6): C1's admission already set the
    backend to ``sandbox``, so the row check answers 409, not 425."""
    thread_id = await lite_session(database, actor)
    await c1.admit_upgrade(database, actor, thread_id)
    with pytest.raises(ControlAdmissionError) as refused:
        await switch(database, actor, thread_id)
    assert type(refused.value) is ControlAdmissionError
    assert str(refused.value) == VM_UPGRADE_NOT_POSSIBLE
    after = await database.get_thread(thread_id)
    assert after["execution_lane"] == "stateless"
    [request] = await upgrade_requests(database, thread_id)
    assert _json(request["payload"])["target_tier"] == "sandbox"


@pytest.mark.asyncio
async def test_a_stranded_pinned_control_refuses_the_switch(database, actor):
    thread_id = await lite_session(database, actor)
    await database.execute(
        "INSERT INTO thread_control_requests (thread_id, request_seq, "
        "client_request_id, verb, payload, requested_by, accepted_agent_id) "
        "VALUES ($1::uuid, 1000, $2, 'mode.set', '{\"mode\": \"supervised\"}'::jsonb, "
        "'real-pg', $3)",
        thread_id,
        uuid4(),
        uuid4(),
    )
    with pytest.raises(
        ControlAdmissionError, match="awaiting its pinned owner"
    ) as refused:
        await switch(database, actor, thread_id)
    assert type(refused.value) is ControlAdmissionError


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["interrupt", "permission", "delivery"])
async def test_pending_interrupts_permissions_and_deliveries_defer_the_switch(
    database, actor, kind
):
    """A delivery is refused by the switch, typed, before the 0191 trigger fires."""
    thread_id = await lite_session(database, actor)
    async with database.acquire() as conn:
        if kind == "interrupt":
            await conn.execute(
                "INSERT INTO thread_interrupt_requests (thread_id, client_request_id, "
                "target_turn_id, accepted_lease_token, accepted_leased_by, requested_by) "
                "VALUES ($1::uuid, $2, 1, 1, 'p-1', 'real-pg')",
                thread_id,
                uuid4(),
            )
        elif kind == "permission":
            await conn.execute(
                "INSERT INTO thread_permission_requests (thread_id, tool_call_id, "
                "tool_name) VALUES ($1::uuid, 'call-1', 'shell')",
                thread_id,
            )
        else:
            message_id = uuid4()
            await conn.execute(
                "INSERT INTO thread_messages (id, thread_id, role, content, turn_number) "
                "VALUES ($1, $2::uuid, 'event', 'pending wake', 1)",
                message_id,
                thread_id,
            )
            await conn.execute(
                "INSERT INTO thread_input_deliveries (delivery_id, thread_id, message_id, "
                "source, execution_lane, conversation_revision) "
                "VALUES ($1, $2::uuid, $3, 'officer_wake', 'stateless', 0)",
                uuid4(),
                thread_id,
                message_id,
            )
    before = await database.get_thread(thread_id)
    with pytest.raises(ControlAdmissionNotReady) as refused:
        await switch(database, actor, thread_id)
    assert str(refused.value) == VM_UPGRADE_BACKGROUND_WORK
    await assert_not_switched(database, thread_id, before)


@pytest.mark.asyncio
async def test_pending_cloud_push_defers_the_switch(database, actor):
    """Review Focus 4: a handed-off push keeps writing off-claim; wait for it."""
    thread_id = await lite_session(database, actor)
    await database.execute(
        "INSERT INTO thread_cloud_sync_generations (thread_id, mount_id, "
        "required_generation, acknowledged_generation, required_lease_token, "
        "workspace_generation, sync_scope_sha256) "
        "VALUES ($1::uuid, 'cloud', 1, 0, 1, 'wg-1', $2)",
        thread_id,
        "0" * 64,
    )
    before = await database.get_thread(thread_id)
    with pytest.raises(ControlAdmissionNotReady) as refused:
        await switch(database, actor, thread_id)
    assert str(refused.value) == VM_UPGRADE_BACKGROUND_WORK
    await assert_not_switched(database, thread_id, before)


@pytest.mark.asyncio
async def test_end_first_refuses_the_switch(database, actor):
    thread_id = await lite_session(database, actor)
    begun = await database.begin_stateless_thread_workspace_retirement(
        thread_id, force=False, permanent=False
    )
    assert begun["state"] != "busy"
    with pytest.raises(ControlAdmissionError) as refused:
        await switch(database, actor, thread_id)
    assert str(refused.value) == VM_UPGRADE_SESSION_ENDING
    assert not await upgrade_requests(database, thread_id)


@pytest.mark.asyncio
async def test_a_claim_after_the_switch_finds_nothing_and_the_old_lease_is_fenced(
    database, actor
):
    thread_id = await lite_session(database, actor)
    async with database.acquire() as conn:
        await record_input_seq(
            conn, unit_id=thread_id, unit_kind=UNIT_KIND_SESSION_TURN, input_seq=1
        )
        claim = await claim_unit(conn, unit_kind=UNIT_KIND_SESSION_TURN, pod_name="p-1")
        assert claim is not None
        # The turn completes: the row returns to done with nothing pending.
        await conn.execute(
            "UPDATE run_queue SET state = 'done', consumed_seq = input_seq, "
            "leased_by = NULL, leased_until = NULL WHERE unit_id = $1::uuid",
            thread_id,
        )
    await switch(database, actor, thread_id)
    async with database.acquire() as conn:
        async with conn.transaction():
            assert not await fence_lease(
                conn, unit_id=thread_id, lease_token=claim.lease_token
            )
        again = await claim_unit(conn, unit_kind=UNIT_KIND_SESSION_TURN, pod_name="p-2")
    assert again is None or str(again.unit_id) != str(thread_id)


@pytest.mark.asyncio
async def test_input_after_the_switch_is_refused_on_the_stateless_lane(
    database, actor, monkeypatch
):
    """Review Focus 1: a stale stateless input POST can't revive the queue."""
    thread_id = await lite_session(database, actor)
    await quiet_queue(database, thread_id)
    stale = dict(await database.get_thread(thread_id))
    await switch(database, actor, thread_id)
    monkeypatch.setattr(
        stateless_input_admission, "refuse_unavailable_session_models", AsyncMock()
    )
    with pytest.raises(HTTPException) as refused:
        await stateless_input_admission.admit_stateless_input(
            stale,
            "hello",
            dependencies=stateless_input_admission.StatelessInputDependencies(
                store=database, schedule_stateless_workspace_ensure=lambda _tid: None
            ),
        )
    assert refused.value.status_code == 409
    queue = await database.fetchrow(
        "SELECT state, lease_token FROM run_queue WHERE unit_id = $1::uuid", thread_id
    )
    assert (queue["state"], int(queue["lease_token"])) == ("done", 1)


@pytest.mark.asyncio
async def test_control_admission_after_the_switch_waits_for_the_pinned_owner(
    database, actor
):
    thread_id = await lite_session(database, actor)
    await switch(database, actor, thread_id)
    generation = (await database.get_thread(thread_id))["runtime_generation"]
    with pytest.raises(ControlAdmissionNotReady):
        await admit_thread_control(
            database,
            thread_id=thread_id,
            owner_user_id=actor["id"],
            client_request_id=uuid4(),
            verb="mode.set",
            payload={"mode": "supervised"},
            requested_by="real-pg",
            expected_runtime_generation=generation,
        )


@pytest.mark.asyncio
async def test_stateless_end_after_the_switch_is_incompatible(database, actor):
    """End racing the switch, switch first: stateless End re-checks the lane."""
    thread_id = await lite_session(database, actor)
    await switch(database, actor, thread_id)
    begun = await database.begin_stateless_thread_workspace_retirement(
        thread_id, force=False, permanent=False
    )
    assert begun == {"state": "incompatible"}
    assert (await database.get_thread(thread_id))["execution_lane"] == "pinned"


@pytest.mark.asyncio
async def test_a_bg_push_lease_is_no_longer_current_after_the_switch(database, actor):
    thread_id = await lite_session(database, actor)
    task_id = uuid4()
    async with database.acquire() as conn:
        await conn.execute(
            "INSERT INTO run_queue (unit_id, unit_kind, state, lease_token, leased_by, "
            "leased_until) VALUES ($1, 'bg_task', 'leased', 1, 'p-1', "
            "now() + interval '1 minute')",
            task_id,
        )
        await conn.execute(
            "INSERT INTO run_queue_bg_tasks (unit_id, thread_id, task_kind, detail) "
            "VALUES ($1, $2::uuid, 'cloud_push', '{}'::jsonb)",
            task_id,
            thread_id,
        )
        assert await bg_push_lease_is_current(
            conn, unit_id=task_id, lease_token=1, pod_name="p-1"
        )
    await switch(database, actor, thread_id)
    async with database.acquire() as conn:
        assert not await bg_push_lease_is_current(
            conn, unit_id=task_id, lease_token=1, pod_name="p-1"
        )


@pytest.mark.asyncio
async def test_presence_renewal_is_a_no_op_after_the_switch(database, actor):
    thread_id = await lite_session(database, actor)
    await switch(database, actor, thread_id)
    refreshed = await refresh_thread_presence(
        database, thread_id=thread_id, establish=True
    )
    assert refreshed.served is False


@pytest.mark.asyncio
async def test_the_switch_restarts_a_paused_sessions_attention_timer(database, actor):
    """A natural pause's old timer must not reach the pinned attention-sleep
    sweep (it skips only stateless rows): the switch restarts it."""
    thread_id = await lite_session(database, actor)
    await database.execute(
        "UPDATE threads SET status = 'awaiting_user', "
        "awaiting_user_since = now() - interval '2 hours' WHERE id = $1::uuid",
        thread_id,
    )

    await switch(database, actor, thread_id)

    after = await database.fetchrow(
        "SELECT status::text AS status, execution_lane, "
        "now() - awaiting_user_since AS paused_for "
        "FROM threads WHERE id = $1::uuid",
        thread_id,
    )
    assert (after["status"], after["execution_lane"]) == ("awaiting_user", "pinned")
    assert timedelta(0) <= after["paused_for"] < timedelta(seconds=10)
