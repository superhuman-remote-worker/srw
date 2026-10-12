"""Session delegation batches BEFORE atomic batch recovery: what the code does.

Design: knowledge-base/knowledge/features/parallel_subagents.md §4 (F2, F3),
§5.4 and §10 WP0.

A session refuses a delegation batch wider than one child, so none of these
shapes reaches production today. The refusal exists because recovery is not
atomic for a batch. These tests pin the facts that refusal protects against,
on a real Postgres, so the batch settle (WP1) is built on measured behaviour
and not on a reading of the code.

Every assertion marked ``HAZARD`` records unwanted behaviour. WP1 replaces it
with the invariants of §9: exactly one recovery continuation per superseded
input, every report delivered to the parent exactly once. Tool results
adjacent to their calls hold since WP0b (§10.2).
"""

from __future__ import annotations

import json
from uuid import UUID, uuid4

import asyncpg
import pytest

from agent.api.persistent_app import _db_rows_to_lc_messages
from agent.api.turn_executor import strip_restored_pending_humans
from agent.core.context import (
    repair_tool_pairing,
    sanitize_history_for_provider_boundary,
)
from orchestrator.database.migrate import run_migrations
from shared.run_queue import UNIT_KIND_SESSION_TURN, claim_unit, record_input_seq
from shared.session_subagent_authority import session_subagent_delivery_id
from tests.test_subagent_thread_migration import (  # noqa: F401  (scratch_pg_dsn fixture)
    MIGRATIONS,
    _agent_db,
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
TYPED_DURING_THE_BATCH = "and include Austria as well"


async def _fresh_pool(dsn: str) -> asyncpg.Pool:
    dbname = f"batch_characterization_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()
    pool = await asyncpg.create_pool(_swap_db(dsn, dbname), min_size=1, max_size=4)
    await run_migrations(pool, MIGRATIONS)
    return pool


async def _seed_stateless_batch(
    pool: asyncpg.Pool,
    orchestrator,
    *,
    children: int,
    typed_after_the_calls: bool = False,
) -> dict:
    """One stateless session whose turn-1 AI message carries ``children``
    ``delegate_agent`` calls. Every child ran and completed on the normal
    path; NO tool result reached the parent transcript — the state a batch is
    in from the moment its first child ends until its slowest one does.

    ``typed_after_the_calls`` adds a human row the user sent while the batch
    ran: the orchestrator accepts input at any time on the stateless lane, so
    its ``seq`` falls between the calls and their results."""

    owner = uuid4()
    session = uuid4()
    input_id = uuid4()
    ai_id = uuid4()
    call_ids = [f"call_delegate_{index}_{uuid4().hex[:8]}" for index in range(children)]
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, display_name) VALUES ($1, 'owner')", owner
        )
        await conn.execute(
            "INSERT INTO threads (id, user_id, title, status, execution_lane) "
            "VALUES ($1, $2, 'Take-home pay by country', 'active', 'stateless')",
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
        ai_seq = await conn.fetchval(
            "INSERT INTO thread_messages "
            "(id, thread_id, role, content, tool_calls, turn_number) "
            "VALUES ($1, $2, 'ai', '', $3::jsonb, 1) RETURNING seq",
            ai_id,
            session,
            json.dumps(
                [
                    {
                        "id": call_id,
                        "name": "delegate_agent",
                        "args": {"subagent_type": "reader", "prompt": f"q{index}"},
                    }
                    for index, call_id in enumerate(call_ids)
                ]
            ),
        )
        typed_id = None
        typed_seq = None
        if typed_after_the_calls:
            typed_id = uuid4()
            typed_seq = await conn.fetchval(
                "INSERT INTO thread_messages "
                "(id, thread_id, role, content, turn_number) "
                "VALUES ($1, $2, 'human', $3, 2) RETURNING seq",
                typed_id,
                session,
                TYPED_DURING_THE_BATCH,
            )
    authority = {
        "version": 1,
        "execution_lane": "stateless",
        "parent_thread_id": str(session),
        "lease_token": claim.lease_token,
        "executor_id": POD,
        "executor_pod_uid": POD_UID,
    }
    rows = []
    for index, call_id in enumerate(call_ids):
        child = await orchestrator.create_session_subagent_thread(
            parent_thread_id=str(session),
            parent_authority=authority,
            handle=f"reader-{index:04x}",
            subagent_type="reader",
            parent_tool_call_id=call_id,
            parent_input_message_id=str(input_id),
            parent_ai_message_id=str(ai_id),
            parent_iteration=1,
        )
        assert child is not None
        ended = await orchestrator.terminalize_session_subagent_thread(
            parent_thread_id=str(session),
            parent_authority=authority,
            thread_id=child["thread_id"],
            runtime_generation=child["runtime_generation"],
            subagent_status="completed",
            outcome="completed",
            turns=4 + index,
            tokens=1000 + index,
            report_path=f".subagents/reader-{index:04x}/report.md",
        )
        assert ended is not None and ended["result"] == "applied"
        rows.append(
            {
                "call_id": call_id,
                "thread_id": child["thread_id"],
                "runtime_generation": child["runtime_generation"],
                "handle": f"reader-{index:04x}",
                "turns": 4 + index,
                "tokens": 1000 + index,
                "report_path": f".subagents/reader-{index:04x}/report.md",
                "delivery_id": str(
                    session_subagent_delivery_id(
                        UUID(child["thread_id"]), UUID(child["runtime_generation"])
                    )
                ),
            }
        )
    return {
        "session": session,
        "input_seq": int(input_seq),
        "ai_id": ai_id,
        "ai_seq": int(ai_seq),
        "typed_id": typed_id,
        "typed_seq": None if typed_seq is None else int(typed_seq),
        "authority": authority,
        "children": rows,
    }


def _recover(seed: dict, child: dict) -> dict:
    """What ``runtime.recover_orphans`` sends for one ended orphan."""

    return dict(
        parent_thread_id=str(seed["session"]),
        parent_authority=seed["authority"],
        thread_id=child["thread_id"],
        runtime_generation=child["runtime_generation"],
        subagent_status="completed",
        outcome="completed",
        turns=child["turns"],
        tokens=child["tokens"],
        report_path=child["report_path"],
        delivery_id=child["delivery_id"],
        message=f"[subagent {child['handle']} · reader · completed] the report",
        foreground_orphan_recovery=True,
    )


async def _recovery_deliveries(pool: asyncpg.Pool, seed: dict) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT delivery.delivery_id, delivery.supersedes_input_seq, "
            "       message.turn_number, message.role, message.content "
            "  FROM thread_input_deliveries AS delivery "
            "  JOIN thread_messages AS message ON message.id = delivery.message_id "
            " WHERE delivery.thread_id = $1 AND delivery.source = 'subagent' "
            " ORDER BY message.seq",
            seed["session"],
        )


async def _stamp(pool: asyncpg.Pool, child: dict) -> str | None:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT metadata->>'subagent_foreground_recovery_generation' "
            "FROM threads WHERE id=$1",
            UUID(child["thread_id"]),
        )


@pytest.mark.asyncio
async def test_two_orphans_under_one_input_write_two_recovery_events(
    pg_dsn: str,
) -> None:
    """Per-child recovery of two siblings is accepted twice. The database does
    not make a superseded input unique, and the recovery stamp is on the child
    row, so nothing collides — and nothing joins the two reports either."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed_stateless_batch(pool, orchestrator, children=2)
        first, second = seed["children"]

        listed = await orchestrator.list_live_session_subagent_threads(
            str(seed["session"]), parent_authority=seed["authority"]
        )
        assert {str(row["id"]) for row in listed} == {
            first["thread_id"],
            second["thread_id"],
        }
        assert {row["recovery_kind"] for row in listed} == {"terminal_foreground"}

        recovered = [
            await orchestrator.terminalize_session_subagent_thread(
                **_recover(seed, child)
            )
            for child in (first, second)
        ]

        for result, child in zip(recovered, (first, second)):
            assert result is not None
            assert result["result"] in {"applied", "idempotent"}
            assert result["delivery_id"] == child["delivery_id"]
            assert result["supersedes_input_seq"] == seed["input_seq"]
            # The stamp is per child: each carries its OWN generation.
            assert await _stamp(pool, child) == child["runtime_generation"]
        assert first["delivery_id"] != second["delivery_id"]

        deliveries = await _recovery_deliveries(pool, seed)
        # HAZARD: two continuations for one abandoned input. The executor runs
        # one pending input per claim, so the parent would be resumed twice,
        # each turn holding one report.
        assert len(deliveries) == 2
        assert [row["supersedes_input_seq"] for row in deliveries] == [
            seed["input_seq"],
            seed["input_seq"],
        ]
        # HAZARD: both events claim the abandoned turn's number.
        assert [row["turn_number"] for row in deliveries] == [1, 1]
        assert [row["role"] for row in deliveries] == ["event", "event"]

        async with pool.acquire() as conn:
            queue = await conn.fetchrow(
                "SELECT consumed_seq FROM run_queue WHERE unit_id=$1",
                seed["session"],
            )
        assert queue["consumed_seq"] == seed["input_seq"]
        assert (
            await orchestrator.list_live_session_subagent_threads(
                str(seed["session"]), parent_authority=seed["authority"]
            )
            == []
        )
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_a_sibling_recovered_after_the_first_answer_loses_its_report(
    pg_dsn: str,
) -> None:
    """Once the first recovery turn has answered, the turn counts as finished.
    A sibling recovered afterwards is closed as delivered without an event, is
    stamped, and is never offered again: its report reaches nobody."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed_stateless_batch(pool, orchestrator, children=2)
        first, second = seed["children"]

        recovered = await orchestrator.terminalize_session_subagent_thread(
            **_recover(seed, first)
        )
        assert recovered is not None
        assert recovered["delivery_id"] == first["delivery_id"]

        # The recovery turn for the first event ran as turn 1 and answered.
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO thread_messages "
                "(id, thread_id, role, content, turn_number) "
                "VALUES ($1, $2, 'ai', 'Here is the comparison ...', 1)",
                uuid4(),
                seed["session"],
            )

        late = await orchestrator.terminalize_session_subagent_thread(
            **_recover(seed, second)
        )

        assert late is not None
        # HAZARD: no delivery for a child that finished and was never read.
        assert late["result"] == "already_delivered"
        assert late["delivery_id"] is None
        deliveries = await _recovery_deliveries(pool, seed)
        assert [str(row["delivery_id"]) for row in deliveries] == [first["delivery_id"]]
        async with pool.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM thread_messages "
                    " WHERE thread_id=$1 AND role='tool' AND tool_call_id=$2",
                    seed["session"],
                    second["call_id"],
                )
                == 0
            )
        assert await _stamp(pool, second) == second["runtime_generation"]
        assert (
            await orchestrator.list_live_session_subagent_threads(
                str(seed["session"]), parent_authority=seed["authority"]
            )
            == []
        )
    finally:
        await pool.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("model", ["claude-opus-5-5", "gpt-5.5"])
async def test_input_typed_during_a_batch_restores_behind_the_results(
    pg_dsn: str, model: str
) -> None:
    """Input the user sent while the batch ran has a lower ``seq`` than the
    tool results, whether the live loop wrote them or a recovery does (§5.4).
    Restore orders the transcript by turn, so the results follow their calls
    and the input, which belongs to the next turn, is last. The executor then
    strips it before injecting it as that turn's input (F19, WP0b)."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed_stateless_batch(
            pool, orchestrator, children=2, typed_after_the_calls=True
        )
        async with pool.acquire() as conn:
            for child in seed["children"]:
                await conn.execute(
                    "INSERT INTO thread_messages "
                    "(id, thread_id, role, content, tool_call_id, turn_number) "
                    "VALUES ($1, $2, 'tool', $3, $4, 1)",
                    uuid4(),
                    seed["session"],
                    f"[subagent {child['handle']} · reader · completed] the report",
                    child["call_id"],
                )
        assert seed["ai_seq"] < seed["typed_seq"]

        rows = await _agent_db(pool).get_thread_messages_history(
            thread_id=str(seed["session"]), limit=1000, newest_first=True
        )
        restored = _db_rows_to_lc_messages(rows)
        restored = repair_tool_pairing(restored)
        restored = sanitize_history_for_provider_boundary(restored, model)

        assert [message.type for message in restored] == [
            "human",
            "ai",
            "tool",
            "tool",
            "human",
        ]
        assert {call["id"] for call in restored[1].tool_calls} == {
            message.tool_call_id for message in restored[2:4]
        }
        assert restored[4].content == TYPED_DURING_THE_BATCH

        pending = [
            {
                "id": str(seed["typed_id"]),
                "role": "human",
                "content": TYPED_DURING_THE_BATCH,
            }
        ]
        assert strip_restored_pending_humans(restored, pending) == 1
        assert [message.type for message in restored] == ["human", "ai", "tool", "tool"]
    finally:
        await pool.close()
