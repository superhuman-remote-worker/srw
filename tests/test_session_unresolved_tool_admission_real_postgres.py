"""A successor cannot repair an unknown tool result into a fresh model turn.

The schema, transcript, queue and park journal use the full migration chain.
Only the external claim bundle/physical attach boundary is replaced: a new
executor must refuse that boundary when a predecessor's tool is unresolved.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from agent.api import turn_executor as te
from orchestrator.services.stateless_queue_state import queue_block
from shared.run_queue import (
    UNIT_KIND_SESSION_TURN,
    claim_unit,
    complete_unit,
    queue_state_for,
    record_input_seq,
)
from tests import test_workspace_pull_failure_real_postgres as full

pg_dsn = full.pg_dsn
db = full.db
_schema_applied = full._schema_applied

POD = "successor-stateless"
POD_UID = "11111111-2222-3333-4444-555555555555"


async def seed(db, *, tool_name="run_command", resolved=False, thread=None):
    existing = thread is not None
    thread = uuid4() if thread is None else thread
    async with db.acquire() as conn:
        if not existing:
            await conn.execute(
                "INSERT INTO threads(id,status,execution_lane,metadata) "
                "VALUES($1,'active','stateless','{}'::jsonb)",
                thread,
            )
        human_seq = await conn.fetchval(
            "INSERT INTO thread_messages(id,thread_id,role,content,turn_number) "
            "VALUES($1,$2,'human','One original command',1) RETURNING seq",
            uuid4(),
            thread,
        )
        await record_input_seq(
            conn,
            unit_id=thread,
            unit_kind=UNIT_KIND_SESSION_TURN,
            input_seq=human_seq,
        )
        await conn.execute(
            "INSERT INTO thread_messages(id,thread_id,role,content,tool_calls,turn_number) "
            "VALUES($1,$2,'ai','',$3::jsonb,1)",
            uuid4(),
            thread,
            json.dumps([{"id": "original_call", "name": tool_name, "args": {}}]),
        )
        if resolved:
            await conn.execute(
                "INSERT INTO thread_messages(id,thread_id,role,content,tool_call_id,turn_number) "
                "VALUES($1,$2,'tool','Original result','original_call',1)",
                uuid4(),
                thread,
            )
        claim = await claim_unit(
            conn,
            unit_kind=UNIT_KIND_SESSION_TURN,
            pod_name=POD,
            prefer_unit_id=thread,
            affinity_grace_seconds=0,
        )
        assert claim is not None and claim.unit_id == thread
    return thread, claim


def executor(db, monkeypatch):
    pa = SimpleNamespace(_session=None)
    ex = te.StatelessTurnExecutor(pod_name=POD, pod_uid=POD_UID, audit_writer=None)
    monkeypatch.setattr(te.StatelessTurnExecutor, "_db", property(lambda self: db))
    monkeypatch.setattr(te, "_pa", lambda: pa)
    monkeypatch.setattr(ex, "_quiesce_claim_before_transition", AsyncMock())
    monkeypatch.setattr(ex, "_detach_cached_session", AsyncMock())
    monkeypatch.setattr(ex, "_ack_terminal_claim_loss", AsyncMock(return_value=True))
    monkeypatch.setattr(ex, "_clear_claim_tool_effect", Mock())
    # A new process has no physical writer. If admission incorrectly reaches
    # the real bundle boundary, emulate a refused bundle, not a model/tool.
    ex._fetch_bundle = AsyncMock(side_effect=AssertionError("Reached bundle boundary"))
    return ex, pa


async def rows(db, thread):
    async with db.acquire() as conn:
        return {
            "queue": dict(
                await conn.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", thread)
            ),
            "thread": dict(
                await conn.fetchrow("SELECT * FROM threads WHERE id=$1", thread)
            ),
            "messages": [
                dict(r)
                for r in await conn.fetch(
                    "SELECT * FROM thread_messages WHERE thread_id=$1 ORDER BY seq",
                    thread,
                )
            ],
            "events": [
                dict(r)
                for r in await conn.fetch(
                    "SELECT epoch,seq,kind,payload FROM thread_events WHERE thread_id=$1 ORDER BY epoch,seq",
                    thread,
                )
            ],
        }


@pytest.mark.asyncio
async def test_successor_parks_unknown_original_command_before_bundle(db, monkeypatch):
    thread, claim = await seed(db)
    ex, pa = executor(db, monkeypatch)
    before = await rows(db, thread)

    await ex._serve_claim_inner(
        pa, claim, str(thread), claim.lease_token, asyncio.Event()
    )

    after = await rows(db, thread)
    assert after["queue"]["state"] == "parked"
    ex._fetch_bundle.assert_not_awaited()
    assert after["messages"] == before["messages"]
    for key in (
        "input_seq",
        "consumed_seq",
        "lease_token",
        "attempts_since_completion",
    ):
        assert after["queue"][key] == before["queue"][key]
    assert after["queue"]["leased_by"] is None
    assert after["queue"]["leased_until"] is None
    assert after["thread"]["events_epoch"] == before["thread"]["events_epoch"] + 1
    assert [r["kind"] for r in after["events"]] == ["turn.parked"]
    payload = json.loads(after["events"][0]["payload"])
    assert payload["retryable"] is False
    state = await queue_state_for(db, unit_id=thread)
    projection = queue_block(state, after["thread"]["metadata"])
    assert projection["state"] == "parked"
    assert projection["pending_input"] is True
    assert projection["retryable"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tool_name,resolved", [("run_command", True), ("delegate_agent", False)]
)
async def test_resolved_or_delegation_only_history_can_reach_bundle(
    db, monkeypatch, tool_name, resolved
):
    thread, claim = await seed(db, tool_name=tool_name, resolved=resolved)
    ex, pa = executor(db, monkeypatch)
    assert await ex._admit_session_tool_history(pa, claim) is True
    assert (await rows(db, thread))["queue"]["state"] == "leased"
    ex._quiesce_claim_before_transition.assert_not_awaited()


@pytest.mark.asyncio
async def test_normal_completed_claim_leaves_old_active_record_for_next_bundle(
    db, monkeypatch
):
    thread, claim = await seed(db, resolved=True)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET metadata=$2::jsonb WHERE id=$1",
            thread,
            json.dumps(
                {
                    "_stateless_active_claim": {
                        "lease_token": claim.lease_token,
                        "pod": POD,
                        "pod_uid": POD_UID,
                    }
                }
            ),
        )
        assert (
            await complete_unit(
                conn,
                unit_id=thread,
                lease_token=claim.lease_token,
                consumed_seq=claim.input_seq,
            )
            == "done"
        )
        fresh = await conn.fetchval(
            "INSERT INTO thread_messages(id,thread_id,role,content,turn_number) "
            "VALUES($1,$2,'human','Next normal turn',2) RETURNING seq",
            uuid4(),
            thread,
        )
        await record_input_seq(
            conn, unit_id=thread, unit_kind=UNIT_KIND_SESSION_TURN, input_seq=fresh
        )
        successor = await claim_unit(
            conn,
            unit_kind=UNIT_KIND_SESSION_TURN,
            pod_name=POD,
            prefer_unit_id=thread,
            affinity_grace_seconds=0,
        )
    assert successor.lease_token == claim.lease_token + 1
    ex, pa = executor(db, monkeypatch)
    assert await ex._admit_session_tool_history(pa, successor) is True
    # No park needs old credential retirement. The ordinary bundle remains
    # responsible for publishing the successor's active record.
    assert (await rows(db, thread))["queue"]["state"] == "leased"


@pytest.mark.asyncio
async def test_unresolved_debt_cannot_retire_an_older_active_record(db, monkeypatch):
    thread, claim = await seed(db)
    await db.execute(
        "UPDATE threads SET metadata=$2::jsonb WHERE id=$1",
        thread,
        json.dumps(
            {
                "_stateless_active_claim": {
                    "lease_token": claim.lease_token,
                    "pod": POD,
                    "pod_uid": POD_UID,
                }
            }
        ),
    )
    await db.execute(
        "UPDATE run_queue SET lease_token=lease_token+1 WHERE unit_id=$1", thread
    )
    before = await rows(db, thread)
    ex, pa = executor(db, monkeypatch)
    assert (
        await ex._admit_session_tool_history(
            pa, replace(claim, lease_token=claim.lease_token + 1)
        )
        is False
    )
    assert ex._stop.is_set()
    assert await rows(db, thread) == before
    ex._quiesce_claim_before_transition.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind", ["wrong_turn", "rewound_result", "result_before_call", "foreign_thread"]
)
async def test_unrelated_result_does_not_resolve_original_call(db, monkeypatch, kind):
    thread, claim = await seed(db)
    async with db.acquire() as conn:
        result_thread = thread
        if kind == "foreign_thread":
            result_thread = uuid4()
            await conn.execute(
                "INSERT INTO threads(id,status) VALUES($1,'active')", result_thread
            )
        await conn.execute(
            "INSERT INTO thread_messages(id,thread_id,role,content,tool_call_id,turn_number,rewound_at) "
            "VALUES($1,$2,'tool','A different result','original_call',$3,$4)",
            uuid4(),
            result_thread,
            2 if kind == "wrong_turn" else 1,
            await conn.fetchval("SELECT now()") if kind == "rewound_result" else None,
        )
        if kind == "result_before_call":
            await conn.execute(
                "UPDATE thread_messages SET seq=0 WHERE thread_id=$1 AND role='tool'",
                thread,
            )
    ex, pa = executor(db, monkeypatch)
    assert await ex._admit_session_tool_history(pa, claim) is False
    assert (await rows(db, thread))["queue"]["state"] == "parked"


@pytest.mark.asyncio
async def test_consumed_input_and_fresh_human_cannot_erase_old_debt(db, monkeypatch):
    thread, claim = await seed(db)
    async with db.acquire() as conn:
        old_tail = await conn.fetchval(
            "SELECT max(seq) FROM thread_messages WHERE thread_id=$1", thread
        )
        await conn.execute(
            "UPDATE run_queue SET consumed_seq=$2 WHERE unit_id=$1", thread, old_tail
        )
        fresh = await conn.fetchval(
            "INSERT INTO thread_messages(id,thread_id,role,content,turn_number) "
            "VALUES($1,$2,'human','A fresh input',2) RETURNING seq",
            uuid4(),
            thread,
        )
        await record_input_seq(
            conn, unit_id=thread, unit_kind=UNIT_KIND_SESSION_TURN, input_seq=fresh
        )
    ex, pa = executor(db, monkeypatch)
    assert (
        await ex._admit_session_tool_history(
            pa, replace(claim, consumed_seq=old_tail, input_seq=fresh)
        )
        is False
    )
    assert (await rows(db, thread))["queue"]["consumed_seq"] == old_tail


@pytest.mark.asyncio
async def test_explicit_rewind_removes_history_debt_without_claiming_effect_undo(
    db, monkeypatch
):
    thread, claim = await seed(db)
    await db.execute(
        "UPDATE thread_messages SET rewound_at=now() WHERE thread_id=$1 AND role='ai'",
        thread,
    )
    ex, pa = executor(db, monkeypatch)
    assert await ex._admit_session_tool_history(pa, claim) is True


@pytest.mark.asyncio
async def test_mixed_delegation_row_remains_unknown_after_shell_result(db, monkeypatch):
    thread, claim = await seed(db, resolved=True)
    await db.execute(
        "UPDATE thread_messages SET tool_calls=tool_calls || $2::jsonb WHERE thread_id=$1 AND role='ai'",
        thread,
        json.dumps([{"id": "unresolved_child", "name": "delegate_agent", "args": {}}]),
    )
    ex, pa = executor(db, monkeypatch)
    assert await ex._admit_session_tool_history(pa, claim) is False
    assert (await rows(db, thread))["queue"]["state"] == "parked"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "token",
        "pod",
        "expired",
        "wrong_kind",
        "ended",
        "pinned",
        "assigned",
        "stop_marker",
        "foreign_active",
        "foreign_active_uid",
        "future_active_token",
        "malformed_metadata",
    ],
)
async def test_unproven_current_authority_never_admits_or_parks(
    db, monkeypatch, change
):
    thread, claim = await seed(db)
    async with db.acquire() as conn:
        if change == "token":
            await conn.execute(
                "UPDATE run_queue SET lease_token=lease_token+1 WHERE unit_id=$1",
                thread,
            )
        elif change == "pod":
            await conn.execute(
                "UPDATE run_queue SET leased_by='foreign-executor' WHERE unit_id=$1",
                thread,
            )
        elif change == "expired":
            await conn.execute(
                "UPDATE run_queue SET leased_until=now()-interval '1 second' WHERE unit_id=$1",
                thread,
            )
        elif change == "wrong_kind":
            await conn.execute(
                "UPDATE run_queue SET unit_kind='worker_batch' WHERE unit_id=$1", thread
            )
        elif change in {"ended", "pinned"}:
            column, value = (
                ("status", "ended")
                if change == "ended"
                else ("execution_lane", "pinned")
            )
            await conn.execute(
                f"UPDATE threads SET {column}=$2 WHERE id=$1", thread, value
            )
        elif change == "assigned":
            agent = await conn.fetchval(
                "INSERT INTO agents(hostname,config_name) VALUES('foreign','worker_base') RETURNING id"
            )
            await conn.execute(
                "UPDATE threads SET agent_id=$2 WHERE id=$1", thread, agent
            )
        else:
            value = {
                "stop_marker": {"_stateless_claim_loss_hold": None},
                "foreign_active": {
                    "_stateless_active_claim": {
                        "lease_token": claim.lease_token,
                        "pod": "foreign",
                        "pod_uid": str(uuid4()),
                    }
                },
                "foreign_active_uid": {
                    "_stateless_active_claim": {
                        "lease_token": claim.lease_token,
                        "pod": POD,
                        "pod_uid": str(uuid4()),
                    }
                },
                "future_active_token": {
                    "_stateless_active_claim": {
                        "lease_token": claim.lease_token + 1,
                        "pod": POD,
                        "pod_uid": POD_UID,
                    }
                },
                "malformed_metadata": [],
            }[change]
            await conn.execute(
                "UPDATE threads SET metadata=$2::jsonb WHERE id=$1",
                thread,
                json.dumps(value),
            )
    before = await rows(db, thread)
    ex, pa = executor(db, monkeypatch)
    if change == "malformed_metadata":
        with pytest.raises(RuntimeError, match="metadata root is malformed"):
            await ex._admit_session_tool_history(pa, claim)
    else:
        assert await ex._admit_session_tool_history(pa, claim) is False
        assert ex._stop.is_set()
    assert await rows(db, thread) == before
    ex._fetch_bundle.assert_not_awaited()
    ex._quiesce_claim_before_transition.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["token", "expired", "ended", "result"])
async def test_locked_recheck_after_local_drain_cannot_use_initial_decision(
    db, monkeypatch, change
):
    thread, claim = await seed(db)
    ex, pa = executor(db, monkeypatch)
    changed = {}

    async def drain(*args, **kwargs):
        assert "claim" not in kwargs  # Prior cached owner is never rebound.
        if change == "token":
            await db.execute(
                "UPDATE run_queue SET lease_token=lease_token+1 WHERE unit_id=$1",
                thread,
            )
        elif change == "expired":
            await db.execute(
                "UPDATE run_queue SET leased_until=now()-interval '1 second' WHERE unit_id=$1",
                thread,
            )
        elif change == "ended":
            await db.end_thread(str(thread))
        else:
            await db.execute(
                "INSERT INTO thread_messages(id,thread_id,role,content,tool_call_id,turn_number) "
                "VALUES($1,$2,'tool','Actual late result','original_call',1)",
                uuid4(),
                thread,
            )
        changed.update(await rows(db, thread))

    ex._quiesce_claim_before_transition.side_effect = drain
    assert await ex._admit_session_tool_history(pa, claim) is (change == "result")
    assert await rows(db, thread) == changed
    assert ex._stop.is_set() is (change != "result")


@pytest.mark.asyncio
async def test_answered_watermark_skip_does_no_model_or_tool_admission(db, monkeypatch):
    thread, claim = await seed(db)
    await db.execute(
        "UPDATE run_queue SET consumed_seq=input_seq WHERE unit_id=$1", thread
    )
    ex, pa = executor(db, monkeypatch)
    await ex._serve_claim(replace(claim, consumed_seq=claim.input_seq))
    ex._fetch_bundle.assert_not_awaited()
    assert (await rows(db, thread))["queue"]["state"] == "done"
    assert (await rows(db, thread))["events"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "calls",
    [
        None,
        False,
        {},
        "bad",
        [{"name": "run_command"}],
        [{"id": True, "name": "delegate_agent"}],
        [{"id": " ", "name": "run_command"}],
    ],
)
async def test_malformed_live_tool_history_cannot_reach_bundle(db, monkeypatch, calls):
    thread, claim = await seed(db)
    await db.execute(
        "UPDATE thread_messages SET tool_calls=$2::jsonb WHERE thread_id=$1 AND role='ai'",
        thread,
        json.dumps(calls),
    )
    ex, pa = executor(db, monkeypatch)
    assert await ex._admit_session_tool_history(pa, claim) is False
    assert (await rows(db, thread))["queue"]["state"] == "parked"
    ex._fetch_bundle.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("calls", [None, []])
async def test_normal_ai_without_calls_is_not_tool_debt(db, monkeypatch, calls):
    thread, claim = await seed(db)
    await db.execute(
        "UPDATE thread_messages SET tool_calls=$2::jsonb WHERE thread_id=$1 AND role='ai'",
        thread,
        None if calls is None else json.dumps(calls),
    )
    ex, pa = executor(db, monkeypatch)
    assert await ex._admit_session_tool_history(pa, claim) is True


@pytest.mark.asyncio
async def test_delegation_only_duplicate_ids_keep_existing_child_recovery(
    db, monkeypatch
):
    thread, claim = await seed(db, tool_name="delegate_agent")
    await db.execute(
        "UPDATE thread_messages SET tool_calls=tool_calls || tool_calls WHERE thread_id=$1 AND role='ai'",
        thread,
    )
    ex, pa = executor(db, monkeypatch)
    assert await ex._admit_session_tool_history(pa, claim) is True


@pytest.mark.asyncio
@pytest.mark.parametrize("separate_row", [False, True])
async def test_same_turn_duplicate_call_ids_cannot_share_one_result(
    db, monkeypatch, separate_row
):
    thread, claim = await seed(db)
    duplicate = json.dumps([{"id": "original_call", "name": "run_command", "args": {}}])
    async with db.acquire() as conn:
        if separate_row:
            await conn.execute(
                "INSERT INTO thread_messages(id,thread_id,role,content,tool_calls,turn_number) VALUES($1,$2,'ai','',$3::jsonb,1)",
                uuid4(),
                thread,
                duplicate,
            )
        else:
            await conn.execute(
                "UPDATE thread_messages SET tool_calls=tool_calls || $2::jsonb WHERE thread_id=$1 AND role='ai'",
                thread,
                duplicate,
            )
        await conn.execute(
            "INSERT INTO thread_messages(id,thread_id,role,content,tool_call_id,turn_number) VALUES($1,$2,'tool','One result','original_call',1)",
            uuid4(),
            thread,
        )
    ex, pa = executor(db, monkeypatch)
    assert await ex._admit_session_tool_history(pa, claim) is False
    assert (await rows(db, thread))["queue"]["state"] == "parked"


@pytest.mark.asyncio
async def test_reused_id_in_distinct_resolved_turns_is_not_ambiguous(db, monkeypatch):
    thread, claim = await seed(db, resolved=True)
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO thread_messages(id,thread_id,role,content,tool_calls,turn_number) VALUES($1,$2,'ai','',$3::jsonb,2)",
            uuid4(),
            thread,
            json.dumps([{"id": "original_call", "name": "run_command", "args": {}}]),
        )
        await conn.execute(
            "INSERT INTO thread_messages(id,thread_id,role,content,tool_call_id,turn_number) VALUES($1,$2,'tool','Second turn result','original_call',2)",
            uuid4(),
            thread,
        )
    ex, pa = executor(db, monkeypatch)
    assert await ex._admit_session_tool_history(pa, claim) is True


@pytest.mark.asyncio
async def test_blank_call_id_cannot_be_resolved_by_matching_malformed_result(
    db, monkeypatch
):
    thread, claim = await seed(db)
    await db.execute(
        "UPDATE thread_messages SET tool_calls=$2::jsonb WHERE thread_id=$1 AND role='ai'",
        thread,
        json.dumps([{"id": " ", "name": "run_command", "args": {}}]),
    )
    await db.execute(
        "INSERT INTO thread_messages(id,thread_id,role,content,tool_call_id,turn_number) VALUES($1,$2,'tool','Invalid pairing',' ',1)",
        uuid4(),
        thread,
    )
    ex, pa = executor(db, monkeypatch)
    assert await ex._admit_session_tool_history(pa, claim) is False


@pytest.mark.asyncio
async def test_exact_current_active_authority_is_retired_by_atomic_park(
    db, monkeypatch
):
    thread, claim = await seed(db)
    await db.execute(
        "UPDATE threads SET metadata=$2::jsonb WHERE id=$1",
        thread,
        json.dumps(
            {
                "_stateless_active_claim": {
                    "lease_token": claim.lease_token,
                    "pod": POD,
                    "pod_uid": POD_UID,
                }
            }
        ),
    )
    ex, pa = executor(db, monkeypatch)
    assert await ex._admit_session_tool_history(pa, claim) is False
    after = await rows(db, thread)
    assert "_stateless_active_claim" not in json.loads(after["thread"]["metadata"])
    assert len(after["events"]) == 1


@pytest.mark.asyncio
async def test_lease_expiry_during_park_cas_rolls_back_journal_and_queue(
    db, monkeypatch
):
    thread, claim = await seed(db)
    before = await rows(db, thread)
    ex, pa = executor(db, monkeypatch)
    original = te.park_unit

    async def prepare_expiry(*args, **kwargs):
        await db.execute(
            "UPDATE run_queue SET leased_until=clock_timestamp()+interval '1 second' WHERE unit_id=$1",
            thread,
        )

    async def expiry(conn, **kwargs):
        # Let the exact captured lease expire naturally after the locked
        # decision. Row locks prevent rotation, not wall-clock expiry.
        await conn.execute(
            "SELECT pg_sleep(GREATEST(EXTRACT(EPOCH FROM (leased_until-clock_timestamp())),0)+0.01) FROM run_queue WHERE unit_id=$1",
            thread,
        )
        return await original(conn, **kwargs)

    ex._quiesce_claim_before_transition.side_effect = prepare_expiry
    monkeypatch.setattr(te, "park_unit", expiry)
    monkeypatch.setattr(te, "COMPLETE_RETRY_ATTEMPTS", 1)
    with pytest.raises(te._PostEffectParkError, match="lease expired"):
        await ex._admit_session_tool_history(pa, claim)
    after = await rows(db, thread)
    # The externally arranged lease deadline survives; the failed handoff
    # cannot change queue state, owner, watermark, transcript or journal.
    before["queue"]["leased_until"] = after["queue"]["leased_until"]
    assert after == before


@pytest.mark.asyncio
async def test_park_journal_failure_rolls_back_the_whole_disposition(db, monkeypatch):
    thread, claim = await seed(db)
    before = await rows(db, thread)
    ex, pa = executor(db, monkeypatch)
    original = te.append_system_frame

    async def fail(conn, **kwargs):
        await original(conn, **kwargs)
        raise RuntimeError("journal response failed before commit")

    monkeypatch.setattr(te, "append_system_frame", fail)
    monkeypatch.setattr(te, "COMPLETE_RETRY_ATTEMPTS", 1)
    with pytest.raises(RuntimeError, match="journal response"):
        await ex._admit_session_tool_history(pa, claim)
    assert await rows(db, thread) == before


@pytest.mark.asyncio
async def test_commit_then_response_loss_replays_one_durable_park(db, monkeypatch):
    thread, claim = await seed(db)
    ex, pa = executor(db, monkeypatch)
    original = te._settle_session_release
    calls = 0

    async def ambiguous(*args, **kwargs):
        nonlocal calls
        result = await original(*args, **kwargs)
        calls += 1
        if calls == 1:
            raise RuntimeError("lost commit response")
        return result

    monkeypatch.setattr(te, "_settle_session_release", ambiguous)
    assert await ex._admit_session_tool_history(pa, claim) is False
    assert calls == 2
    after = await rows(db, thread)
    assert after["queue"]["state"] == "parked"
    assert len(after["events"]) == 1
    assert await ex._admit_session_tool_history(pa, claim) is False
    assert await rows(db, thread) == after


@pytest.mark.asyncio
async def test_previous_cached_push_is_drained_without_new_claim_handoff(
    db, monkeypatch
):
    thread, claim = await seed(db)
    ex, pa = executor(db, monkeypatch)
    # Exercise the real drain: its cloud writer still belongs to another unit.
    monkeypatch.setattr(
        ex,
        "_quiesce_claim_before_transition",
        te.StatelessTurnExecutor._quiesce_claim_before_transition.__get__(ex),
    )
    previous = str(uuid4())
    ex._activate_lease(previous, 7)
    drained = asyncio.Event()

    async def previous_push():
        assert ex._lease.unit_id == previous and ex._lease.lease_token == 7
        drained.set()

    pa._pending_cloud_push_task = asyncio.create_task(previous_push())
    pa._hand_off_cloud_push = AsyncMock(
        side_effect=AssertionError("Wrong successor handoff")
    )
    assert await ex._admit_session_tool_history(pa, claim) is False
    assert drained.is_set() and pa._pending_cloud_push_task is None
    assert ex._lease.unit_id == previous and ex._lease.lease_token == 7
    pa._hand_off_cloud_push.assert_not_awaited()


def driver_pa(ex, pa, monkeypatch):
    # PersistentApp has an exact-claim reader even before attach. This new
    # claim has no process-local effect marker; durable predecessor debt is
    # the guard's responsibility, not a missing reader's fail-closed fallback.
    pa._turn_tool_execution_identity_for_claim = lambda **kwargs: None
    pa._clear_turn_tool_execution_identity = Mock()
    pa._interrupt_owner_lease_token = None
    pa._interrupt_owner_turn_id = None
    pa._stop_thread_interrupt_watcher = AsyncMock()
    pa._stop_thread_control_watcher = AsyncMock()
    monkeypatch.setattr(ex, "_transcript_answered_seq", AsyncMock(return_value=None))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["classification", "journal"])
async def test_real_claim_loop_stops_without_generic_release_on_admission_ambiguity(
    db, monkeypatch, failure
):
    thread, claim = await seed(db)
    ex, pa = executor(db, monkeypatch)
    driver_pa(ex, pa, monkeypatch)
    ex._worker_enabled = False
    ex._bg_task_enabled = False
    before = await rows(db, thread)
    if failure == "classification":
        original = db.fetchrow

        async def unavailable(sql, *args):
            if sql == te._SESSION_TOOL_DEBT_AUTHORITY_SQL:
                raise RuntimeError("Debt classification unavailable")
            return await original(sql, *args)

        monkeypatch.setattr(db, "fetchrow", unavailable)
    else:
        monkeypatch.setattr(
            te,
            "append_system_frame",
            AsyncMock(side_effect=RuntimeError("Journal unavailable")),
        )
        monkeypatch.setattr(te, "COMPLETE_RETRY_ATTEMPTS", 1)
    claims = 0

    async def once(*args, **kwargs):
        nonlocal claims
        claims += 1
        if claims == 1:
            return claim
        ex.request_stop()
        return None

    monkeypatch.setattr(te, "claim_unit", once)
    release = AsyncMock(wraps=ex._release)
    monkeypatch.setattr(ex, "_release", release)
    await asyncio.wait_for(ex.run(), timeout=2)
    release.assert_not_awaited()
    assert claims == 1 and ex._stop.is_set()
    assert await rows(db, thread) == before
    ex._fetch_bundle.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["debt_read", "prior_push", "park"])
async def test_pre_activation_cancellation_never_uses_new_claim_shutdown_release(
    db, monkeypatch, stage
):
    thread, claim = await seed(db)
    ex, pa = executor(db, monkeypatch)
    driver_pa(ex, pa, monkeypatch)
    ex._activate_lease(str(uuid4()), 7)
    before = await rows(db, thread)
    entered = asyncio.Event()

    async def blocked():
        entered.set()
        await asyncio.Event().wait()

    if stage == "debt_read":
        original = ex._session_tool_debt

        async def read(*args):
            row = await original(*args)
            await blocked()
            return row

        monkeypatch.setattr(ex, "_session_tool_debt", read)
    elif stage == "prior_push":
        monkeypatch.setattr(
            ex,
            "_quiesce_claim_before_transition",
            te.StatelessTurnExecutor._quiesce_claim_before_transition.__get__(ex),
        )
        original = ex._await_cloud_push

        async def push(current_pa):
            entered.set()
            await original(current_pa)

        monkeypatch.setattr(ex, "_await_cloud_push", push)
        pa._pending_cloud_push_task = asyncio.create_task(asyncio.Event().wait())
        pa._hand_off_cloud_push = AsyncMock(
            side_effect=AssertionError("Wrong new-claim handoff")
        )
    else:

        async def park(*args, **kwargs):
            await blocked()

        monkeypatch.setattr(te, "park_unit", park)
    shutdown = AsyncMock(wraps=ex._shutdown_dispose_cancelled_claim)
    monkeypatch.setattr(ex, "_shutdown_dispose_cancelled_claim", shutdown)
    task = asyncio.create_task(ex._serve_claim(claim))
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        task.cancel()
        with pytest.raises(
            (asyncio.CancelledError, te._PostEffectParkError, te._ClaimQuiescenceError)
        ):
            await task
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        pending_push = getattr(pa, "_pending_cloud_push_task", None)
        if pending_push is not None and not pending_push.done():
            pending_push.cancel()
            await asyncio.gather(pending_push, return_exceptions=True)
    shutdown.assert_not_awaited()
    assert await rows(db, thread) == before
    ex._fetch_bundle.assert_not_awaited()
    if stage == "prior_push":
        pa._hand_off_cloud_push.assert_not_awaited()
