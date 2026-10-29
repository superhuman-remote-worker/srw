"""The owner's Continue / Stop decision on a parked Session's unknown tool results.

Full migration chain; the transcript, queue and debt query are real. Design:
knowledge-base/knowledge/issues/session_unknown_tool_result_needs_admin.md (F4).
"""

from __future__ import annotations

import json
from uuid import uuid4

import pytest

from orchestrator.services import session_unknown_tool_results as utr
from orchestrator.services.stateless_queue_state import queue_block_for_thread
from shared.run_queue import (
    UNIT_KIND_SESSION_TURN,
    claim_unit,
    complete_unit,
    park_unit,
    record_input_seq,
)
from shared.session_tool_debt import has_tool_debt
from tests import test_workspace_pull_failure_real_postgres as full

pg_dsn = full.pg_dsn
db = full.db
_schema_applied = full._schema_applied

POD = "decision-stateless"


async def _human(conn, thread, text, turn):
    seq = await conn.fetchval(
        "INSERT INTO thread_messages(id,thread_id,role,content,turn_number) "
        "VALUES($1,$2,'human',$3,$4) RETURNING seq",
        uuid4(),
        thread,
        text,
        turn,
    )
    await record_input_seq(
        conn, unit_id=thread, unit_kind=UNIT_KIND_SESSION_TURN, input_seq=seq
    )
    return seq


async def _call(conn, thread, turn, calls):
    await conn.execute(
        "INSERT INTO thread_messages(id,thread_id,role,content,tool_calls,turn_number) "
        "VALUES($1,$2,'ai','',$3::jsonb,$4)",
        uuid4(),
        thread,
        json.dumps(calls),
        turn,
    )


async def _claim(conn, thread):
    claim = await claim_unit(
        conn,
        unit_kind=UNIT_KIND_SESSION_TURN,
        pod_name=POD,
        prefer_unit_id=thread,
        affinity_grace_seconds=0,
    )
    assert claim is not None and claim.unit_id == thread
    return claim


async def seed_interrupted(db, *, metadata=None, calls=None):
    """A turn whose process died mid-call: its input is still pending."""
    thread = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads(id,status,execution_lane,metadata) "
            "VALUES($1,'active','stateless',$2::jsonb)",
            thread,
            json.dumps(metadata or {}),
        )
        human = await _human(conn, thread, "Transcribe the whole file", 1)
        await _call(
            conn,
            thread,
            1,
            calls
            or [
                {
                    "id": "call_sleep",
                    "name": "shell_execute",
                    "args": {"command": "sleep 570"},
                }
            ],
        )
        claim = await _claim(conn, thread)
        assert (
            await park_unit(
                conn,
                unit_id=thread,
                lease_token=claim.lease_token,
                reason="loop_died_after_tool_effect",
            )
            == "parked"
        )
    return thread, human


async def queue(db, thread):
    return dict(await db.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", thread))


async def tool_rows(db, thread):
    return [
        dict(r)
        for r in await db.fetch(
            "SELECT tool_call_id, turn_number, content FROM thread_messages "
            "WHERE thread_id=$1 AND role='tool' ORDER BY seq",
            thread,
        )
    ]


async def decide(db, thread, decision):
    async with db.acquire() as conn:
        return await utr.decide_unknown_tool_results(
            conn, thread_id=str(thread), decision=decision
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("decision", ["continue", "stop"])
async def test_decision_answers_the_call_and_settles_the_interrupted_turn(db, decision):
    thread, human = await seed_interrupted(db)

    outcome = await decide(db, thread, decision)

    rows = await tool_rows(db, thread)
    assert rows == [
        {
            "tool_call_id": "call_sleep",
            "turn_number": 1,
            "content": utr.result_text(decision),
        }
    ]
    async with db.acquire() as conn:
        assert await has_tool_debt(conn, str(thread)) is False
    after = await queue(db, thread)
    # The interrupted request is not replayed after its own partial work.
    assert after["state"] == "done"
    assert after["consumed_seq"] == human
    assert after["park_reason"] is None and after["parked_at"] is None
    assert outcome.call_ids == ["call_sleep"]
    assert outcome.needs_continue_input is (decision == "continue")


@pytest.mark.asyncio
async def test_input_sent_after_the_interruption_still_runs(db):
    """The incident shape: turn 13 completed with its call unanswered, the
    next message parked on it. Either decision lets that message run."""
    thread = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads(id,status,execution_lane,metadata) "
            "VALUES($1,'active','stateless','{}'::jsonb)",
            thread,
        )
        first = await _human(conn, thread, "Transcribe", 13)
        await _call(conn, thread, 13, [{"id": "call_sleep", "name": "shell_execute"}])
        claim = await _claim(conn, thread)
        assert (
            await complete_unit(
                conn,
                unit_id=thread,
                lease_token=claim.lease_token,
                consumed_seq=first,
            )
            == "done"
        )
        later = await _human(conn, thread, "Read the clip with read_file", 14)
        claim = await _claim(conn, thread)
        await park_unit(
            conn,
            unit_id=thread,
            lease_token=claim.lease_token,
            reason="loop_died_after_tool_effect",
        )

    outcome = await decide(db, thread, "stop")

    after = await queue(db, thread)
    assert after["state"] == "queued"
    assert after["consumed_seq"] == first
    assert after["input_seq"] == later
    assert outcome.needs_continue_input is False


@pytest.mark.asyncio
async def test_queue_block_lists_the_calls_the_card_decides(db):
    thread, _ = await seed_interrupted(
        db,
        calls=[
            {"id": "call_a", "name": "shell_execute", "args": {"command": "sleep 570"}},
            {"id": "call_b", "name": "write_file", "args": {"content": "x" * 5000}},
        ],
    )
    async with db.acquire() as conn:
        block = await queue_block_for_thread(conn, {"id": thread, "metadata": {}})
    assert block["state"] == "parked"
    assert block["unknown_tool_calls"] == [
        {"id": "call_a", "name": "shell_execute", "args": {"command": "sleep 570"}},
        # A huge payload stays out of the browser.
        {"id": "call_b", "name": "write_file", "args": None},
    ]


@pytest.mark.asyncio
async def test_correctness_hold_refuses_and_changes_nothing(db):
    thread, _ = await seed_interrupted(
        db, metadata={"_stateless_claim_loss_hold": {"token": 1}}
    )
    before = await queue(db, thread)

    with pytest.raises(utr.UnknownToolResultRefused) as err:
        await decide(db, thread, "continue")

    assert (err.value.status, err.value.code) == (409, "claim_loss_hold")
    assert await tool_rows(db, thread) == []
    assert await queue(db, thread) == before
    async with db.acquire() as conn:
        block = await queue_block_for_thread(
            conn,
            {"id": thread, "metadata": {"_stateless_claim_loss_hold": {"token": 1}}},
        )
    assert block["unknown_tool_calls"] == []


@pytest.mark.asyncio
async def test_ambiguous_history_rolls_back_every_row(db):
    """Duplicate same-turn ids stay debt after answering: the operator's case."""
    thread, _ = await seed_interrupted(
        db,
        calls=[
            {"id": "dup", "name": "shell_execute"},
            {"id": "dup", "name": "shell_execute"},
        ],
    )
    before = await queue(db, thread)

    with pytest.raises(utr.UnknownToolResultRefused) as err:
        await decide(db, thread, "stop")

    assert (err.value.status, err.value.code) == (409, "tool_history_ambiguous")
    assert await tool_rows(db, thread) == []
    assert await queue(db, thread) == before


@pytest.mark.asyncio
async def test_not_parked_or_nothing_to_answer_is_refused(db):
    thread, _ = await seed_interrupted(db)
    await decide(db, thread, "stop")

    with pytest.raises(utr.UnknownToolResultRefused) as err:
        await decide(db, thread, "stop")
    assert (err.value.status, err.value.code) == (404, "not_parked")
