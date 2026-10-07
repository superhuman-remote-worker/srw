"""Recovery from facts: a foreground child's delivery IS its ToolMessage.

Real-Postgres reproduction of dev thread ``ad7eb761`` (2026-09-08,
knowledge-base/knowledge/issues/stateless_parked_unit_swallows_input_and_subagent_recovery_400.md):
a *completed* foreground child whose ToolMessage is durable in the parent
transcript, a final AI message that carries tool calls (so no zero-tool-call
"final response" row exists), and a run_queue watermark still below the parent
input. Before this slice ``list_live_session_subagent_threads`` offered that
child on every re-attach and ``terminalize_session_subagent_thread`` raised —
HTTP 400 — every time, so the successor released the unit and the next pod
tried again, forever.

The contract now: the durable ToolMessage is the delivery fact. An ended child
with one is never listed, and recovering it answers ``already_delivered`` and
stamps ``subagent_foreground_recovery_generation`` without touching the
watermark. A child WITHOUT a ToolMessage is a real orphan and keeps the
existing recovery path (continuation event, watermark advance).
"""

from __future__ import annotations

import json
from uuid import UUID, uuid4

import asyncpg
import pytest

from orchestrator.database.migrate import run_migrations
from shared.run_queue import UNIT_KIND_SESSION_TURN, claim_unit, record_input_seq
from shared.session_subagent_authority import session_subagent_delivery_id
from tests.test_session_subagent_batch_settle_pg import _seed as _seed_delegation_turn
from tests.test_subagent_thread_migration import (  # noqa: F401  (scratch_pg_dsn fixture)
    MIGRATIONS,
    _orchestrator_db,
    _stamp_stateless_claim,
    _swap_db,
    scratch_pg_dsn,
)


@pytest.fixture(scope="module")
def pg_dsn(scratch_pg_dsn: str) -> str:  # noqa: F811 (pytest fixture param)
    """The migration test's scratch Postgres (testcontainers), reused as-is."""
    return scratch_pg_dsn


POD = "stateless-executor-1"
POD_UID = "stateless-executor-pod-1"
REPORT_PATH = ".subagents/reader-307f/report.md"


async def _fresh_pool(dsn: str) -> asyncpg.Pool:
    dbname = f"recovery_facts_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()
    pool = await asyncpg.create_pool(_swap_db(dsn, dbname), min_size=1, max_size=4)
    await run_migrations(pool, MIGRATIONS)
    return pool


async def _seed_stateless_session_with_completed_delegation(
    pool: asyncpg.Pool,
    orchestrator,
    *,
    with_tool_message: bool,
    with_turn_completed_frame: bool = False,
) -> dict:
    """One stateless session: turn-1 input, a live claim, a delegation AI row,
    a child completed on the normal path, and — optionally — the parent's
    ToolMessage plus a final AI row that itself carries a tool call."""

    owner = uuid4()
    session = uuid4()
    input_id = uuid4()
    ai_id = uuid4()
    call_id = f"call_delegate_{uuid4().hex[:10]}"
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, display_name) VALUES ($1, 'owner')", owner
        )
        await conn.execute(
            "INSERT INTO threads (id, user_id, title, status, execution_lane) "
            "VALUES ($1, $2, 'US vs Germany take-home pay', 'active', 'stateless')",
            session,
            owner,
        )
        input_seq = await conn.fetchval(
            "INSERT INTO thread_messages (id, thread_id, role, content, turn_number) "
            "VALUES ($1, $2, 'human', 'Compare take-home pay', 1) RETURNING seq",
            input_id,
            session,
        )
        await conn.execute("UPDATE threads SET total_turns=1 WHERE id=$1", session)
        await record_input_seq(
            conn,
            unit_id=session,
            unit_kind=UNIT_KIND_SESSION_TURN,
            input_seq=int(input_seq),
            fair_key=str(owner),
        )
        claim = await claim_unit(
            conn,
            unit_kind=UNIT_KIND_SESSION_TURN,
            pod_name=POD,
            prefer_unit_id=session,
        )
        assert claim is not None
        await _stamp_stateless_claim(
            conn, session, token=claim.lease_token, pod=POD, pod_uid=POD_UID
        )
        await conn.execute(
            "INSERT INTO thread_messages "
            "(id, thread_id, role, content, tool_calls, turn_number) "
            "VALUES ($1, $2, 'ai', '', $3::jsonb, 1)",
            ai_id,
            session,
            json.dumps(
                [
                    {
                        "id": call_id,
                        "name": "delegate_agent",
                        "args": {"subagent": "reader", "prompt": "deductions"},
                    }
                ]
            ),
        )
    authority = {
        "version": 1,
        "execution_lane": "stateless",
        "parent_thread_id": str(session),
        "lease_token": claim.lease_token,
        "executor_id": POD,
        "executor_pod_uid": POD_UID,
    }
    child = await orchestrator.create_session_subagent_thread(
        parent_thread_id=str(session),
        parent_authority=authority,
        handle="reader-307f",
        subagent_type="reader",
        parent_tool_call_id=call_id,
        parent_input_message_id=str(input_id),
        parent_ai_message_id=str(ai_id),
        parent_iteration=1,
    )
    assert child is not None
    # The normal, synchronous terminal — exactly what the child did at 13:14.
    normal = await orchestrator.terminalize_session_subagent_thread(
        parent_thread_id=str(session),
        parent_authority=authority,
        thread_id=child["thread_id"],
        runtime_generation=child["runtime_generation"],
        subagent_status="completed",
        outcome="completed",
        turns=8,
        tokens=61474,
        report_path=REPORT_PATH,
    )
    assert normal is not None and normal["result"] == "applied"
    if with_tool_message:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO thread_messages "
                "(id, thread_id, role, content, tool_call_id, turn_number) "
                "VALUES ($1, $2, 'tool', $3, $4, 1)",
                uuid4(),
                session,
                "[subagent reader-307f · reader · completed · 8 turns] <report>",
                call_id,
            )
            # The parent's final message: the answer PLUS a write_file call in
            # one AI message, so no zero-tool-call "final response" row exists.
            write_call = f"call_write_{uuid4().hex[:10]}"
            await conn.execute(
                "INSERT INTO thread_messages "
                "(id, thread_id, role, content, tool_calls, turn_number) "
                "VALUES ($1, $2, 'ai', $3, $4::jsonb, 1)",
                uuid4(),
                session,
                "The calculation is now done. With these assumptions ...",
                json.dumps(
                    [
                        {
                            "id": write_call,
                            "name": "write_file",
                            "args": {"path": "output/methodology.md"},
                        }
                    ]
                ),
            )
            await conn.execute(
                "INSERT INTO thread_messages "
                "(id, thread_id, role, content, tool_call_id, turn_number) "
                "VALUES ($1, $2, 'tool', 'Written: output/methodology.md', $3, 1)",
                uuid4(),
                session,
                write_call,
            )
            if with_turn_completed_frame:
                # The loop's turn-complete hook ran (ad7eb761: 13:38:27) even
                # though the completion CAS never did — the durable fact that
                # the turn answered its input.
                await conn.execute(
                    "INSERT INTO thread_events (thread_id, epoch, seq, kind, payload) "
                    "VALUES ($1, 0, 497, 'turn.completed', "
                    '\'{"turn_id": 1, "metrics": {}}\'::jsonb)',
                    session,
                )
    return {
        "session": session,
        "input_seq": int(input_seq),
        "authority": authority,
        "child": child,
        "call_id": call_id,
        "delivery_id": str(
            session_subagent_delivery_id(
                UUID(child["thread_id"]), UUID(child["runtime_generation"])
            )
        ),
    }


def _recovery_kwargs(seed: dict, *, status: str = "completed") -> dict:
    """What ``runtime.recover_orphans`` sends for a ``terminal_foreground``
    row: the child's terminal status and outcome, the row's totals and report
    path, the generation-stable delivery id, and the transcript envelope.

    ``status="ended"`` reproduces the request every agent image before this
    slice sent: the runtime read the listing row's ``status`` — the THREAD
    status — instead of ``subagent_status``, and the server refused the
    mismatch against the stored ``completed`` with HTTP 400 on every attach
    (dev ``ad7eb761``). The server-side verdict must hold for that request
    too, because orchestrator and agent images roll separately."""

    return dict(
        parent_thread_id=str(seed["session"]),
        parent_authority=seed["authority"],
        thread_id=seed["child"]["thread_id"],
        runtime_generation=seed["child"]["runtime_generation"],
        subagent_status=status,
        outcome=status,
        turns=8,
        tokens=61474,
        report_path=REPORT_PATH,
        delivery_id=seed["delivery_id"],
        message="[subagent reader-307f · reader · completed] transcript envelope",
        foreground_orphan_recovery=True,
    )


async def _queue_row(pool: asyncpg.Pool, session: UUID) -> asyncpg.Record:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT state, input_seq, consumed_seq FROM run_queue WHERE unit_id=$1",
            session,
        )


async def _child_stamp(pool: asyncpg.Pool, child_id: str) -> str | None:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT metadata->>'subagent_foreground_recovery_generation' "
            "FROM threads WHERE id=$1",
            UUID(child_id),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("turn_completed", [True, False])
async def test_durable_tool_message_is_the_delivery_fact(
    pg_dsn: str, turn_completed: bool
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed_stateless_session_with_completed_delegation(
            pool,
            orchestrator,
            with_tool_message=True,
            with_turn_completed_frame=turn_completed,
        )
        session, child = seed["session"], seed["child"]

        # The park left the watermark below the answered input (ad7eb761:
        # consumed_seq 74497 < input 74498). Nothing here may repair that —
        # the child's delivery verdict must not depend on it.
        before = await _queue_row(pool, session)
        assert before["state"] == "leased"
        assert (
            before["consumed_seq"] is None or before["consumed_seq"] < seed["input_seq"]
        )
        assert await _child_stamp(pool, child["thread_id"]) is None

        # 1. The listing does not offer a child whose ToolMessage is durable.
        listed = await orchestrator.list_live_session_subagent_threads(
            str(session), parent_authority=seed["authority"]
        )
        assert listed == []

        # 2. Recovering it anyway (a racing caller, a stale list, an agent
        #    image that still sends the thread status) is a no-op verdict:
        #    already delivered, stamped, nothing else touched.
        recovered = await orchestrator.terminalize_session_subagent_thread(
            **_recovery_kwargs(seed, status="ended")
        )
        assert recovered is not None
        assert recovered["result"] == "already_delivered"
        assert recovered["delivery_id"] is None
        assert (
            await _child_stamp(pool, child["thread_id"]) == child["runtime_generation"]
        )
        after = await _queue_row(pool, session)
        if turn_completed:
            # The turn provably answered its input: consume it, so the next
            # claim cannot answer it a second time (a final answer that
            # carries a tool call is invisible to the executor's own
            # zero-tool-call answered-check).
            assert after["consumed_seq"] == seed["input_seq"]
        else:
            # No proof the turn answered: leave the input for the next claim.
            assert after["consumed_seq"] == before["consumed_seq"]
        async with pool.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM thread_input_deliveries WHERE thread_id=$1",
                    session,
                )
                == 0
            )
            assert (
                await conn.fetchval(
                    "SELECT status || '/' || subagent_status FROM threads WHERE id=$1",
                    UUID(child["thread_id"]),
                )
                == "ended/completed"
            )

        # 3. Idempotent on repeat.
        again = await orchestrator.terminalize_session_subagent_thread(
            **_recovery_kwargs(seed)
        )
        assert again is not None and again["result"] == "already_delivered"
        assert (
            await orchestrator.list_live_session_subagent_threads(
                str(session), parent_authority=seed["authority"]
            )
            == []
        )
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_child_without_tool_message_is_still_a_real_orphan(
    pg_dsn: str,
) -> None:
    """The narrow crash seam the listing was built for stays covered: a child
    that ended but whose ToolMessage never landed is offered as
    ``terminal_foreground`` and recovered through the existing event path."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed_stateless_session_with_completed_delegation(
            pool, orchestrator, with_tool_message=False
        )
        session, child = seed["session"], seed["child"]

        listed = await orchestrator.list_live_session_subagent_threads(
            str(session), parent_authority=seed["authority"]
        )
        assert [str(row["id"]) for row in listed] == [child["thread_id"]]
        assert listed[0]["recovery_kind"] == "terminal_foreground"
        # The listing row carries the thread status AND the child's own.
        assert listed[0]["status"] == "ended"
        assert listed[0]["subagent_status"] == "completed"

        # The server still refuses a retry that changes the terminal status —
        # the request every pre-fix agent image sent for this row.
        with pytest.raises(ValueError, match="changed its terminal status"):
            await orchestrator.terminalize_session_subagent_thread(
                **_recovery_kwargs(seed, status="ended")
            )

        recovered = await orchestrator.terminalize_session_subagent_thread(
            **_recovery_kwargs(seed)
        )
        assert recovered is not None
        assert recovered["result"] in {"applied", "idempotent"}
        assert recovered["delivery_id"] == seed["delivery_id"]
        assert recovered["supersedes_input_seq"] == seed["input_seq"]
        # The continuation event is the delivery now, and the recovered turn's
        # input is consumed by the same transaction.
        after = await _queue_row(pool, session)
        assert after["consumed_seq"] == seed["input_seq"]
        async with pool.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM thread_input_deliveries "
                    "WHERE thread_id=$1 AND delivery_id=$2",
                    session,
                    UUID(seed["delivery_id"]),
                )
                == 1
            )
        assert (
            await _child_stamp(pool, child["thread_id"]) == child["runtime_generation"]
        )
        assert (
            await orchestrator.list_live_session_subagent_threads(
                str(session), parent_authority=seed["authority"]
            )
            == []
        )
    finally:
        await pool.close()


_DURABLE_RESULT_CONTINUATION = (
    "[subagent recovery] The original delegate_agent ToolMessage is already "
    "durable in this conversation, but the parent turn ended before its final "
    "response was recorded. Continue from that tool result and answer the "
    "original request directly. Do not delegate replacement work for this "
    "recovery turn."
)


@pytest.mark.asyncio
@pytest.mark.parametrize("turn_completed", [False, True])
async def test_pinned_ended_child_with_a_durable_result_owes_the_turn_its_answer(
    pg_dsn: str, turn_completed: bool
) -> None:
    """The pinned variant (parallel_subagents.md §14.1). The ToolMessage is
    still the child's delivery, but without proof that the turn answered its
    input the turn is owed: the pinned lane never serves an admitted input
    again, so settling the source alone would mark it answered. Like a live
    child with a durable result, the ended one queues the saved-result
    continuation and settles the source. With the turn's ``turn.completed``
    frame it stays ``already_delivered`` (and the source is settled), as the
    batch settle does. Only a racing caller reaches this: the listing never
    offers an ended child whose result is durable."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed_delegation_turn(
            pool, orchestrator, ["delivered"], lane="pinned"
        )
        (call_id,) = seed.call_ids
        child = seed.children[call_id]
        if turn_completed:
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO thread_events (thread_id, epoch, seq, kind, "
                    "payload) VALUES ($1, 0, 900, 'turn.completed', "
                    '\'{"turn_id": 1, "metrics": {}}\'::jsonb)',
                    seed.session,
                )
        assert (
            await orchestrator.list_live_session_subagent_threads(
                str(seed.session), parent_authority=seed.authority
            )
            == []
        )
        expected_delivery = str(
            session_subagent_delivery_id(
                UUID(child["thread_id"]), UUID(child["runtime_generation"])
            )
        )
        request = dict(
            parent_thread_id=str(seed.session),
            parent_authority=seed.authority,
            thread_id=child["thread_id"],
            runtime_generation=child["runtime_generation"],
            subagent_status="completed",
            outcome="completed",
            delivery_id=expected_delivery,
            message="[subagent reader-0000 · reader · completed] transcript envelope",
            foreground_orphan_recovery=True,
        )

        recovered = await orchestrator.terminalize_session_subagent_thread(**request)

        async def subagent_events() -> list[asyncpg.Record]:
            async with pool.acquire() as conn:
                return await conn.fetch(
                    "SELECT delivery.delivery_id, delivery.state, "
                    "       delivery.supersedes_input_seq, message.content "
                    "  FROM thread_input_deliveries AS delivery "
                    "  JOIN thread_messages AS message "
                    "    ON message.id = delivery.message_id "
                    " WHERE delivery.thread_id = $1 "
                    "   AND delivery.source = 'subagent'",
                    seed.session,
                )

        async def source_state() -> str:
            async with pool.acquire() as conn:
                return await conn.fetchval(
                    "SELECT state FROM thread_input_deliveries WHERE delivery_id=$1",
                    seed.source_delivery_id,
                )

        assert recovered is not None
        assert await source_state() == "settled"
        assert (
            await _child_stamp(pool, child["thread_id"]) == child["runtime_generation"]
        )
        if turn_completed:
            assert recovered["result"] == "already_delivered"
            assert recovered["delivery_id"] is None
            assert await subagent_events() == []
        else:
            # An ended child's first recovery reports ``idempotent`` (its
            # terminal row predates the call), as on the stateless lane.
            assert recovered["result"] in {"applied", "idempotent"}
            assert recovered["delivery_id"] == expected_delivery
            assert recovered["supersedes_input_seq"] == seed.input_seq
            (event,) = await subagent_events()
            assert str(event["delivery_id"]) == expected_delivery
            assert event["state"] == "owned"
            assert event["supersedes_input_seq"] == seed.input_seq
            assert event["content"] == _DURABLE_RESULT_CONTINUATION
            # The child's terminal facts are untouched.
            async with pool.acquire() as conn:
                assert (
                    await conn.fetchval(
                        "SELECT status || '/' || subagent_status FROM threads "
                        "WHERE id=$1",
                        UUID(child["thread_id"]),
                    )
                    == "ended/completed"
                )

        # A retry converges on the same verdict and writes nothing new.
        events = await subagent_events()
        again = await orchestrator.terminalize_session_subagent_thread(**request)
        assert again is not None
        if turn_completed:
            assert again["result"] == "already_delivered"
        else:
            assert again["result"] == "idempotent"
            assert again["delivery_id"] == expected_delivery
        assert await subagent_events() == events
    finally:
        await pool.close()
