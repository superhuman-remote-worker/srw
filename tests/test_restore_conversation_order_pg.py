"""Restore order on a real Postgres: what the history query hands the converter.

Design: knowledge-base/knowledge/features/parallel_subagents.md §10.2 (WP0b).

F19: the pinned lane numbers every input typed during a turn as the next
turn; only the delivery row knows which turn ran it. F20: the checkpoint
cursor loads the rows after the summary's last message. A bare ``seq``
cursor dropped input typed during a tool call, because that input has a
lower ``seq`` than the rest of its turn and the summary never saw it.
"""

from __future__ import annotations

import json
from uuid import uuid4

import asyncpg
import pytest

from agent.api.persistent_app import _db_rows_to_lc_messages
from agent.core.context import repair_tool_pairing
from tests.test_session_delegation_batch_characterization import (
    TYPED_DURING_THE_BATCH,
    _fresh_pool,
    _seed_stateless_batch,
)
from tests.test_subagent_thread_migration import (  # noqa: F401  (scratch_pg_dsn fixture)
    _agent_db,
    _orchestrator_db,
    scratch_pg_dsn,
)


@pytest.fixture(scope="module")
def pg_dsn(scratch_pg_dsn: str) -> str:  # noqa: F811 (pytest fixture param)
    """The migration test's scratch Postgres (testcontainers), reused as-is."""
    return scratch_pg_dsn


async def _message(
    conn: asyncpg.Connection,
    thread_id,
    role: str,
    content: str,
    turn: int,
    *,
    tool_calls: list | None = None,
    tool_call_id: str | None = None,
) -> tuple:
    message_id = uuid4()
    seq = await conn.fetchval(
        "INSERT INTO thread_messages "
        "(id, thread_id, role, content, tool_calls, tool_call_id, turn_number) "
        "VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7) RETURNING seq",
        message_id,
        thread_id,
        role,
        content,
        json.dumps(tool_calls) if tool_calls is not None else None,
        tool_call_id,
        turn,
    )
    return message_id, int(seq)


async def _settled_delivery(
    conn: asyncpg.Connection, thread_id, message_id, admitted_turn: int
) -> None:
    await conn.execute(
        "INSERT INTO thread_input_deliveries "
        "(delivery_id, thread_id, message_id, source, state, admitted_at, "
        " admitted_turn_number, settled_at, execution_lane) "
        "VALUES ($1, $2, $3, 'direct_human', 'settled', now(), $4, now(), "
        "        'pinned')",
        uuid4(),
        thread_id,
        message_id,
        admitted_turn,
    )


@pytest.mark.asyncio
async def test_pinned_input_restores_in_the_turn_that_admitted_it(
    pg_dsn: str,
) -> None:
    """Two messages typed during turn 1 of a pinned session both carry turn 2.
    The second ran as turn 3; restore reads that from its delivery."""

    pool = await _fresh_pool(pg_dsn)
    try:
        owner = uuid4()
        session = uuid4()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (id, display_name) VALUES ($1, 'owner')", owner
            )
            await conn.execute(
                "INSERT INTO threads (id, user_id, title, status, execution_lane) "
                "VALUES ($1, $2, 'Pinned', 'active', 'pinned')",
                session,
                owner,
            )
            await _message(conn, session, "human", "q1", 1)
            await _message(
                conn,
                session,
                "ai",
                "",
                1,
                tool_calls=[{"id": "c1", "name": "run_command", "args": {}}],
            )
            first, _ = await _message(conn, session, "human", "typed-1", 2)
            second, _ = await _message(conn, session, "human", "typed-2", 2)
            await _settled_delivery(conn, session, first, 2)
            await _settled_delivery(conn, session, second, 3)
            await _message(conn, session, "tool", "r1", 1, tool_call_id="c1")
            await _message(conn, session, "ai", "a1", 1)
            await _message(conn, session, "ai", "a2", 2)
            await _message(conn, session, "ai", "a3", 3)

        rows = await _agent_db(pool).get_thread_messages_history(
            thread_id=str(session), limit=1000, newest_first=True
        )
        restored = repair_tool_pairing(_db_rows_to_lc_messages(rows))

        assert [message.content for message in restored] == [
            "q1",
            "",
            "r1",
            "a1",
            "typed-1",
            "a2",
            "typed-2",
            "a3",
        ]
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_input_typed_during_a_tool_call_survives_a_checkpoint(
    pg_dsn: str,
) -> None:
    """A resume compaction at the claim that answers the typed input covers
    turn 1 up to its final answer. The typed input was pending then, so the
    summary never saw it; the next restore must load it from the tail."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed_stateless_batch(
            pool, orchestrator, children=1, typed_after_the_calls=True
        )
        (child,) = seed["children"]
        session = seed["session"]
        async with pool.acquire() as conn:
            await _message(
                conn,
                session,
                "tool",
                "the report",
                1,
                tool_call_id=child["call_id"],
            )
            _, turn_one_end = await _message(conn, session, "ai", "Comparison", 1)
            _, answer = await _message(conn, session, "ai", "Austria too", 2)
        assert seed["typed_seq"] < turn_one_end

        async def tail(boundary_seq: int) -> list[str]:
            rows = await _agent_db(pool).get_thread_messages_history(
                thread_id=str(session),
                limit=1000,
                seq_gt=boundary_seq,
                newest_first=True,
            )
            return [message.content for message in _db_rows_to_lc_messages(rows)]

        # The summary covered turn 1: the typed input and its answer follow.
        assert await tail(turn_one_end) == [TYPED_DURING_THE_BATCH, "Austria too"]
        # The summary covered turn 2 as well: nothing follows.
        assert await tail(answer) == []
        # The summary covered only the first input: all of turn 1 follows, then
        # the typed input, in conversation order.
        assert await tail(seed["input_seq"]) == [
            "",
            "the report",
            "Comparison",
            TYPED_DURING_THE_BATCH,
            "Austria too",
        ]
    finally:
        await pool.close()
