"""The batch settle of a session delegation turn, on a real Postgres (WP1).

Design: knowledge-base/knowledge/features/parallel_subagents.md §5.3 (the one
transaction), §9 (the invariants every test here asserts) and §11 (the matrix:
S0–S9 and S11; S10 is the unchanged single-child suite).

Setup throughout: one parent AI message with N = 4 ``delegate_agent`` calls
under a cap of 2, so a crash finds some children finished, some running and
some never started. The executor that ran the batch holds lease 1; its
successor claims the unit again and settles the turn. Every test gets a fresh,
fully migrated database.
"""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

import shared.persistent_input_delivery as input_delivery_module
from agent.api.persistent_app import _db_rows_to_lc_messages
from agent.api.turn_executor import _PENDING_INPUT_SQL, strip_restored_pending_humans
from orchestrator.database.migrate import run_migrations
from orchestrator.database.session_subagent_recovery import (
    load_delegation_manifest,
    load_recovery_parent_input,
)
from orchestrator.schemas.thread_rewind import StatelessRewindRequest
from orchestrator.services.thread_rewind import ThreadRewindService
from shared.persistent_input_delivery import (
    claim_stateless_input_delivery,
    message_row_id,
    persist_input_delivery,
    transition_input_delivery,
    transition_stateless_input_delivery,
)
from shared.run_queue import (
    UNIT_KIND_SESSION_TURN,
    claim_unit,
    complete_unit,
    record_input_seq,
    release_unit,
)
from shared.session_subagent_authority import SessionParentAuthorityRefused
from shared.session_subagent_batch import (
    DECLINED_RESULT_TEXT,
    RECOVERY_METRICS_KEY,
    batch_continuation_text,
    not_started_result_text,
    session_subagent_batch_delivery_id,
    session_subagent_batch_result_id,
)
from shared.thread_rewind import LIVE_SESSION_CHILD_EXISTS_SQL
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


POD, POD_UID = "stateless-executor-1", "stateless-executor-pod-1"
SUCCESSOR, SUCCESSOR_UID = "stateless-executor-2", "stateless-executor-pod-2"
TYPED_DURING_THE_BATCH = "and include Austria as well"
QUEUED_BEHIND_THE_INPUT = "and Switzerland, please"
INPUT = "Compare take-home pay in four countries"
S1 = ["completed", "running", "none", "none"]


async def _fresh_pool(dsn: str) -> asyncpg.Pool:
    dbname = f"batch_settle_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()
    pool = await asyncpg.create_pool(_swap_db(dsn, dbname), min_size=1, max_size=6)
    await run_migrations(pool, MIGRATIONS)
    return pool


@dataclass
class Seed:
    lane: str
    session: UUID
    owner: UUID
    input_id: UUID
    input_seq: int
    ai_id: UUID
    ai_seq: int
    call_ids: list[str]
    # The executor that ran the batch.
    authority: dict[str, Any]
    children: dict[str, dict[str, Any]] = field(default_factory=dict)
    source_delivery_id: UUID | None = None
    typed_id: UUID | None = None
    typed_seq: int | None = None
    queued_delivery_id: UUID | None = None
    queued_claim_generation: int | None = None

    @property
    def delivery_id(self) -> UUID:
        return session_subagent_batch_delivery_id(self.session, self.input_id)


async def _stateless_parent(pool: asyncpg.Pool) -> tuple[UUID, UUID, dict]:
    owner, session = uuid4(), uuid4()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, display_name) VALUES ($1, 'owner')", owner
        )
        await conn.execute(
            "INSERT INTO threads (id, user_id, title, status, execution_lane) "
            "VALUES ($1, $2, 'Take-home pay', 'active', 'stateless')",
            session,
            owner,
        )
    return owner, session, {}


async def _pinned_parent(pool: asyncpg.Pool, orchestrator) -> tuple[UUID, UUID, dict]:
    owner, session, agent_id = uuid4(), uuid4(), uuid4()
    pod_uid, pod_name, attempt = f"pod-{uuid4()}", f"parent-{uuid4()}", str(uuid4())
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, display_name) VALUES ($1, 'owner')", owner
        )
        generation = await conn.fetchval(
            "INSERT INTO threads (id, user_id, title, status) "
            "VALUES ($1, $2, 'Take-home pay', 'active') RETURNING runtime_generation",
            session,
            owner,
        )
    assert await orchestrator.reserve_pinned_agent_pod_provision_intent(
        str(session),
        expected_runtime_generation=str(generation),
        attempt_id=attempt,
        pod_name=pod_name,
        provisioner="agent",
        namespace="test",
    )
    assert await orchestrator.publish_pinned_agent_pod_provision_intent(
        str(session),
        expected_runtime_generation=str(generation),
        attempt_id=attempt,
        pod_name=pod_name,
        pod_uid=pod_uid,
        namespace="test",
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO agents (id, config_name, status, metadata, hostname, "
                "pod_uid) VALUES ($1, 'session_base', 'session', '{}'::jsonb, $2, $3)",
                agent_id,
                pod_name,
                pod_uid,
            )
            binding = await conn.fetchrow(
                "UPDATE threads SET agent_id=$2 WHERE id=$1 "
                "RETURNING runtime_generation, runtime_attach_token",
                session,
                agent_id,
            )
            await conn.execute(
                "UPDATE agents SET thread_id=$2 WHERE id=$1", agent_id, session
            )
    return (
        owner,
        session,
        {
            "version": 1,
            "execution_lane": "pinned",
            "parent_thread_id": str(session),
            "agent_id": str(agent_id),
            "pod_uid": pod_uid,
            "session_runtime_generation": str(binding["runtime_generation"]),
            "runtime_attach_token": str(binding["runtime_attach_token"]),
        },
    )


async def _seed(
    pool: asyncpg.Pool,
    orchestrator,
    states: list[str],
    *,
    lane: str = "stateless",
    typed_during_the_batch: bool = False,
    queued_behind_the_input: bool = False,
) -> Seed:
    """One session whose turn-1 AI message carries one ``delegate_agent`` call
    per state, then the durable facts each state leaves:

    ``completed`` a child ended on the normal path, its result never reached
    the parent; ``running`` a child still running; ``none`` no child (queued
    behind the cap); ``declined`` / ``expired`` no child and a permission
    request the user declined / nobody answered; ``delivered`` a completed
    child whose result is durable; ``delivered_running`` a running child whose
    result is durable anyway.

    ``queued_behind_the_input`` (pinned) adds a second message the user sent
    before turn 1 began: the pinned runtime numbers queued input
    ``turn_count + 1``, so it carries turn 1 as well until it runs.
    """

    if lane == "pinned":
        owner, session, authority = await _pinned_parent(pool, orchestrator)
    else:
        owner, session, authority = await _stateless_parent(pool)
    ai_id = uuid4()
    call_ids = [
        f"call_delegate_{index}_{uuid4().hex[:8]}" for index in range(len(states))
    ]
    source_delivery_id = None
    async with pool.acquire() as conn:
        if lane == "pinned":
            source_delivery_id = uuid4()
            source = await persist_input_delivery(
                conn,
                thread_id=session,
                delivery_id=source_delivery_id,
                role="human",
                content=INPUT,
                source="direct_human",
                turn_number=1,
                agent_id=authority["agent_id"],
                pod_uid=authority["pod_uid"],
                runtime_generation=authority["session_runtime_generation"],
                session_runtime_generation=authority["session_runtime_generation"],
                runtime_attach_token=authority["runtime_attach_token"],
            )
            assert await transition_input_delivery(
                conn,
                delivery_id=source_delivery_id,
                agent_id=authority["agent_id"],
                pod_uid=authority["pod_uid"],
                runtime_generation=authority["session_runtime_generation"],
                session_runtime_generation=authority["session_runtime_generation"],
                runtime_attach_token=authority["runtime_attach_token"],
                claim_generation=source["claim_generation"],
                transition="admitted",
                turn_number=1,
            )
            input_id = message_row_id(source_delivery_id)
            input_seq = int(source["seq"])
            if queued_behind_the_input:
                queued_delivery_id = uuid4()
                queued = await persist_input_delivery(
                    conn,
                    thread_id=session,
                    delivery_id=queued_delivery_id,
                    role="human",
                    content=QUEUED_BEHIND_THE_INPUT,
                    source="direct_human",
                    turn_number=None,
                    turn_number_hint=1,
                    agent_id=authority["agent_id"],
                    pod_uid=authority["pod_uid"],
                    runtime_generation=authority["session_runtime_generation"],
                    session_runtime_generation=authority["session_runtime_generation"],
                    runtime_attach_token=authority["runtime_attach_token"],
                )
                assert queued["turn_number"] == 1
        else:
            input_id = uuid4()
            input_seq = await conn.fetchval(
                "INSERT INTO thread_messages (id, thread_id, role, content, "
                "turn_number) VALUES ($1, $2, 'human', $3, 1) RETURNING seq",
                input_id,
                session,
                INPUT,
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
            authority = {
                "version": 1,
                "execution_lane": "stateless",
                "parent_thread_id": str(session),
                "lease_token": claim.lease_token,
                "executor_id": POD,
                "executor_pod_uid": POD_UID,
            }
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
                        "args": {
                            "subagent_type": "reader",
                            "description": f"country {index}",
                            "prompt": f"take-home pay in country {index}",
                        },
                    }
                    for index, call_id in enumerate(call_ids)
                ]
            ),
        )
    seed = Seed(
        lane=lane,
        session=session,
        owner=owner,
        input_id=input_id,
        input_seq=int(input_seq),
        ai_id=ai_id,
        ai_seq=int(ai_seq),
        call_ids=call_ids,
        authority=authority,
        source_delivery_id=source_delivery_id,
    )
    if queued_behind_the_input:
        seed.queued_delivery_id = queued_delivery_id
        seed.queued_claim_generation = int(queued["claim_generation"])
    if typed_during_the_batch:
        async with pool.acquire() as conn:
            seed.typed_id = uuid4()
            seed.typed_seq = int(
                await conn.fetchval(
                    "INSERT INTO thread_messages (id, thread_id, role, content, "
                    "turn_number) VALUES ($1, $2, 'human', $3, 2) RETURNING seq",
                    seed.typed_id,
                    session,
                    TYPED_DURING_THE_BATCH,
                )
            )
            await record_input_seq(
                conn,
                unit_id=session,
                unit_kind=UNIT_KIND_SESSION_TURN,
                input_seq=seed.typed_seq,
                fair_key=str(owner),
            )
    for index, (call_id, state) in enumerate(zip(call_ids, states)):
        if state in {"declined", "expired"}:
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO thread_permission_requests "
                    "(thread_id, tool_call_id, tool_name, status, decided_at, "
                    " decided_by) VALUES ($1, $2, 'delegate_agent', $3, now(), $4)",
                    session,
                    call_id,
                    "denied" if state == "declined" else "expired",
                    "owner" if state == "declined" else "system",
                )
            continue
        if state == "none":
            continue
        handle = f"reader-{index:04x}"
        child = await orchestrator.create_session_subagent_thread(
            parent_thread_id=str(session),
            parent_authority=authority,
            handle=handle,
            subagent_type="reader",
            parent_tool_call_id=call_id,
            parent_input_message_id=str(input_id),
            parent_ai_message_id=str(ai_id),
            parent_iteration=1,
            brief_description=f"country {index}",
        )
        assert child is not None
        seed.children[call_id] = {**child, "handle": handle}
        if state in {"completed", "delivered"}:
            ended = await orchestrator.terminalize_session_subagent_thread(
                parent_thread_id=str(session),
                parent_authority=authority,
                thread_id=child["thread_id"],
                runtime_generation=child["runtime_generation"],
                subagent_status="completed",
                outcome="completed",
                turns=4 + index,
                tokens=1000 + index,
                report_path=f".subagents/{handle}/report.md",
            )
            assert ended is not None and ended["result"] == "applied"
        if state in {"delivered", "delivered_running"}:
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO thread_messages "
                    "(id, thread_id, role, content, tool_call_id, turn_number) "
                    "VALUES ($1, $2, 'tool', $3, $4, 1)",
                    uuid4(),
                    session,
                    f"[subagent {handle} · reader · completed] live result",
                    call_id,
                )
    return seed


async def _successor(pool: asyncpg.Pool, seed: Seed) -> dict[str, Any]:
    """The executor died; another one claims the unit and holds lease 2."""

    async with pool.acquire() as conn:
        released = await release_unit(
            conn, unit_id=seed.session, lease_token=seed.authority["lease_token"]
        )
        assert released == "queued"
        claim = await claim_unit(
            conn,
            unit_kind=UNIT_KIND_SESSION_TURN,
            pod_name=SUCCESSOR,
            prefer_unit_id=seed.session,
        )
        assert claim is not None
        await _stamp_stateless_claim(
            conn,
            seed.session,
            token=claim.lease_token,
            pod=SUCCESSOR,
            pod_uid=SUCCESSOR_UID,
        )
    return {
        **seed.authority,
        "lease_token": claim.lease_token,
        "executor_id": SUCCESSOR,
        "executor_pod_uid": SUCCESSOR_UID,
    }


async def _plan(orchestrator, seed: Seed, authority: dict) -> dict[str, Any]:
    listed = await orchestrator.list_live_session_subagent_recovery(
        str(seed.session), parent_authority=authority
    )
    (plan,) = listed["recovery_turns"]
    assert plan["parent_input_message_id"] == str(seed.input_id)
    assert plan["parent_iteration"] == 1
    assert plan["supersedes_input_seq"] == seed.input_seq
    assert plan["delivery_id"] == str(seed.delivery_id)
    assert [call["tool_call_id"] for call in plan["calls"]] == seed.call_ids
    # The members a plan asks for are exactly the live-list candidates.
    assert {call["thread_id"] for call in plan["calls"] if call["needs_entry"]} == {
        str(row["id"]) for row in listed["subagents"]
    }
    return plan


def _report(call: dict) -> str:
    return f"[subagent {call['handle']} · reader · completed] the report"


def _interrupted(call: dict) -> str:
    return f"[delegate_agent: INTERRUPTED - no final report] handle: {call['handle']}"


def _members(plan: dict) -> list[dict[str, Any]]:
    """What a successor sends for a plan: one entry per member it must name."""

    members = []
    for call in plan["calls"]:
        if not call["needs_entry"]:
            continue
        member = {
            "thread_id": call["thread_id"],
            "runtime_generation": call["runtime_generation"],
        }
        if call["class"] == "ended":
            member.update(
                subagent_status=call["subagent_status"],
                outcome=call["outcome"],
                message=_report(call),
            )
        else:
            member.update(
                subagent_status="interrupted",
                outcome="interrupted:parent_restart",
                turns=2,
                tokens=300,
                error="the parent runtime restarted",
                message=_interrupted(call) if call["needs_message"] else None,
            )
        members.append(member)
    return members


async def _settle(orchestrator, seed: Seed, authority: dict, members: list) -> dict:
    return await orchestrator.settle_session_subagent_batch(
        parent_thread_id=str(seed.session),
        parent_authority=authority,
        parent_input_message_id=str(seed.input_id),
        parent_iteration=1,
        members=members,
    )


async def _tool_rows(pool: asyncpg.Pool, seed: Seed) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT id, seq, tool_call_id, content, turn_number, metrics "
            "FROM thread_messages WHERE thread_id=$1 AND role='tool' "
            "AND rewound_at IS NULL ORDER BY seq",
            seed.session,
        )


async def _continuations(pool: asyncpg.Pool, seed: Seed) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT delivery.delivery_id, delivery.state, delivery.source, "
            "       delivery.supersedes_input_seq, message.id AS message_id, "
            "       message.seq, message.role, message.content, "
            "       message.turn_number, message.metrics "
            "  FROM thread_input_deliveries AS delivery "
            "  JOIN thread_messages AS message ON message.id = delivery.message_id "
            " WHERE delivery.thread_id = $1 AND delivery.source = 'subagent' "
            " ORDER BY message.seq",
            seed.session,
        )


async def _child(pool: asyncpg.Pool, seed: Seed, call_id: str) -> asyncpg.Record:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT status, subagent_status, subagent_outcome, total_turns, "
            "       total_tokens, report_path, runtime_generation, "
            "       metadata->>'subagent_foreground_recovery_generation' AS stamp "
            "FROM threads WHERE id=$1",
            UUID(seed.children[call_id]["thread_id"]),
        )


async def _consumed(pool: asyncpg.Pool, seed: Seed) -> int | None:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT consumed_seq FROM run_queue WHERE unit_id=$1", seed.session
        )


async def _snapshot(pool: asyncpg.Pool, seed: Seed) -> tuple:
    """Everything a settle may write, for "nothing changed" assertions."""

    async with pool.acquire() as conn:
        return (
            [tuple(row) for row in await _tool_rows(pool, seed)],
            await conn.fetchval(
                "SELECT count(*) FROM thread_input_deliveries WHERE thread_id=$1",
                seed.session,
            ),
            await conn.fetchval(
                "SELECT count(*) FROM thread_messages WHERE thread_id=$1",
                seed.session,
            ),
            [
                tuple(row)
                for row in await conn.fetch(
                    "SELECT id, status, subagent_status, subagent_outcome, "
                    "total_turns, total_tokens, metadata "
                    "FROM threads WHERE parent_thread_id=$1 ORDER BY id",
                    seed.session,
                )
            ],
            await conn.fetchrow(
                "SELECT state, input_seq, consumed_seq, lease_token "
                "FROM run_queue WHERE unit_id=$1",
                seed.session,
            ),
        )


def _metrics(row: asyncpg.Record) -> dict[str, Any]:
    value = row["metrics"]
    value = json.loads(value) if isinstance(value, str) else value
    return value[RECOVERY_METRICS_KEY]


async def _assert_predicates_agree(pool, orchestrator, seed, authority) -> bool:
    """The live list and rewind's pending-child check read one predicate."""

    listed = await orchestrator.list_live_session_subagent_threads(
        str(seed.session), parent_authority=authority
    )
    async with pool.acquire() as conn:
        exists = await conn.fetchval(LIVE_SESSION_CHILD_EXISTS_SQL, seed.session)
    assert bool(listed) is bool(exists)
    return bool(exists)


async def _assert_invariants(
    pool, orchestrator, seed: Seed, authority: dict, *, settled: bool
) -> None:
    """§9, as durable facts, after a settle that committed."""

    continuations = await _continuations(pool, seed)
    superseding = [
        row for row in continuations if row["supersedes_input_seq"] == seed.input_seq
    ]
    # 1. At most one continuation per superseded input.
    assert len(superseding) == (1 if settled else 0)
    async with pool.acquire() as conn:
        # 2. At most one child per (parent thread, tool call).
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM (SELECT parent_tool_call_id FROM threads "
                "WHERE parent_thread_id=$1 GROUP BY parent_tool_call_id "
                "HAVING count(*) > 1) AS duplicate",
                seed.session,
            )
            == 0
        )
    rows = await _tool_rows(pool, seed)
    per_call = Counter(row["tool_call_id"] for row in rows)
    if settled:
        # 5 and 10. Every call of the turn has exactly one result, and the
        # results the settle wrote follow provider order.
        assert all(per_call[call_id] == 1 for call_id in seed.call_ids)
        written = [row["tool_call_id"] for row in rows if row["metrics"] is not None]
        assert written == [call_id for call_id in seed.call_ids if call_id in written]
        # 12. Every row the settle wrote carries the abandoned turn's number;
        # the continuation keeps source='subagent' and supersedes the input.
        assert {row["turn_number"] for row in rows if row["metrics"]} <= {1}
        (continuation,) = superseding
        assert continuation["turn_number"] == 1
        assert continuation["source"] == "subagent"
        assert continuation["role"] == "event"
        assert continuation["delivery_id"] == seed.delivery_id
        assert continuation["message_id"] == message_row_id(seed.delivery_id)
        for row in rows:
            if row["metrics"] is not None:
                assert row["id"] == session_subagent_batch_result_id(
                    seed.session, seed.input_id, row["tool_call_id"]
                )
    # 11. Recovery converged: the live list is empty, and neither predicate
    # reports an owed child.
    assert await _assert_predicates_agree(pool, orchestrator, seed, authority) is False
    if seed.lane == "stateless" and settled:
        # 11. The watermark consumed the abandoned input and nothing later.
        assert await _consumed(pool, seed) == seed.input_seq
    for call_id in seed.children:
        child = await _child(pool, seed, call_id)
        # Every child the settle closed is ended; no child is left live.
        assert child["status"] == "ended"


# ---------------------------------------------------------------------------
# The matrix
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_s0_no_child_rows_nothing_listed_nothing_written(pg_dsn: str) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, ["none"] * 4)
        authority = await _successor(pool, seed)
        listed = await orchestrator.list_live_session_subagent_recovery(
            str(seed.session), parent_authority=authority
        )
        assert listed == {"subagents": [], "recovery_turns": []}
        before = await _snapshot(pool, seed)

        # A settle nobody should send (the list is empty) is a no-op verdict:
        # nothing was spent on children, the input replays as a normal turn.
        result = await _settle(orchestrator, seed, authority, [])
        assert result["result"] == "nothing_to_recover"
        assert [call["class"] for call in result["calls"]] == ["not_started"] * 4
        assert await _snapshot(pool, seed) == before
        await _assert_invariants(pool, orchestrator, seed, authority, settled=False)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_s1_one_settle_reports_interrupts_and_marks_never_started(
    pg_dsn: str,
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, S1)
        completed, running, queued_a, queued_b = seed.call_ids
        authority = await _successor(pool, seed)

        plan = await _plan(orchestrator, seed, authority)
        assert [call["class"] for call in plan["calls"]] == [
            "ended",
            "live",
            "not_started",
            "not_started",
        ]
        assert [call["needs_message"] for call in plan["calls"]] == [
            True,
            True,
            False,
            False,
        ]
        assert plan["calls"][2]["subagent_type"] == "reader"
        assert plan["calls"][2]["description"] == "country 2"
        assert await _assert_predicates_agree(pool, orchestrator, seed, authority)

        result = await _settle(orchestrator, seed, authority, _members(plan))

        assert result["result"] == "applied"
        assert result["delivery_id"] == str(seed.delivery_id)
        assert result["supersedes_input_seq"] == seed.input_seq
        assert result["delivery"]["message_id"] == str(message_row_id(seed.delivery_id))
        assert result["delivery_state"] == "queued"
        assert result["execution_disposition"] == "current"
        assert [call["result_message_id"] for call in result["calls"]] == [
            str(session_subagent_batch_result_id(seed.session, seed.input_id, call))
            for call in seed.call_ids
        ]

        rows = {row["tool_call_id"]: row for row in await _tool_rows(pool, seed)}
        assert rows[completed]["content"] == _report(plan["calls"][0])
        assert rows[running]["content"] == _interrupted(plan["calls"][1])
        assert rows[queued_a]["content"] == not_started_result_text()
        assert rows[queued_b]["content"] == not_started_result_text()
        # The structured outcome the cockpit reads (metrics, never the text).
        assert _metrics(rows[completed]) == {
            "version": 1,
            "kind": "result",
            "class": "completed",
            "tool_call_id": completed,
            "thread_id": seed.children[completed]["thread_id"],
            "handle": "reader-0000",
            "subagent_type": "reader",
            "subagent_status": "completed",
            "report_path": ".subagents/reader-0000/report.md",
            "delivery_id": str(seed.delivery_id),
        }
        assert _metrics(rows[running])["class"] == "interrupted"
        assert _metrics(rows[running])["subagent_status"] == "interrupted"
        assert _metrics(rows[running])["handle"] == "reader-0001"
        assert _metrics(rows[queued_a]) == {
            "version": 1,
            "kind": "result",
            "class": "not_started",
            "tool_call_id": queued_a,
            "thread_id": None,
            "handle": None,
            "subagent_type": "reader",
            "subagent_status": None,
            "report_path": None,
            "delivery_id": str(seed.delivery_id),
        }

        (continuation,) = await _continuations(pool, seed)
        assert continuation["state"] == "queued"
        assert continuation["content"] == batch_continuation_text(
            calls=4, interrupted=1, not_started=2, declined=0, retired=0
        )
        assert "1 of 4 delegated tasks finished" in continuation["content"]
        assert _metrics(continuation) == {
            "version": 1,
            "kind": "continuation",
            "supersedes_input_seq": seed.input_seq,
            "calls": 4,
            "finished": 1,
            "interrupted": 1,
            "not_started": 2,
            "declined": 0,
            "retired": 0,
        }
        # The results precede the continuation that tells the model about them.
        assert max(row["seq"] for row in rows.values()) < continuation["seq"]

        # 3. A finished child keeps its terminal facts; the running one is
        # interrupted by the restart with the successor's counters.
        finished = await _child(pool, seed, completed)
        assert (finished["subagent_status"], finished["subagent_outcome"]) == (
            "completed",
            "completed",
        )
        assert (finished["total_turns"], finished["total_tokens"]) == (4, 1000)
        assert finished["report_path"] == ".subagents/reader-0000/report.md"
        interrupted = await _child(pool, seed, running)
        assert (interrupted["subagent_status"], interrupted["subagent_outcome"]) == (
            "interrupted",
            "interrupted:parent_restart",
        )
        assert (interrupted["total_turns"], interrupted["total_tokens"]) == (2, 300)
        for call_id in (completed, running):
            child = await _child(pool, seed, call_id)
            assert child["stamp"] == str(child["runtime_generation"])
        await _assert_invariants(pool, orchestrator, seed, authority, settled=True)

        # 9. Running recovery again is a no-op: same id, same counts.
        before = await _snapshot(pool, seed)
        again = await _settle(orchestrator, seed, authority, _members(plan))
        assert again["result"] == "idempotent"
        assert again["delivery_id"] == str(seed.delivery_id)
        assert await _snapshot(pool, seed) == before
        assert (
            await orchestrator.list_live_session_subagent_recovery(
                str(seed.session), parent_authority=authority
            )
        ) == {"subagents": [], "recovery_turns": []}
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_s2_all_finished_four_reports_one_continuation(pg_dsn: str) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, ["completed"] * 4)
        authority = await _successor(pool, seed)
        plan = await _plan(orchestrator, seed, authority)
        assert [call["class"] for call in plan["calls"]] == ["ended"] * 4

        result = await _settle(orchestrator, seed, authority, _members(plan))

        assert result["result"] == "applied"
        rows = await _tool_rows(pool, seed)
        assert [row["tool_call_id"] for row in rows] == seed.call_ids
        assert [row["content"] for row in rows] == [
            _report(call) for call in plan["calls"]
        ]
        assert {_metrics(row)["class"] for row in rows} == {"completed"}
        (continuation,) = await _continuations(pool, seed)
        assert continuation["content"].startswith(
            "[subagent recovery] This turn was resumed after the process running "
            "it was replaced. All 4 delegated tasks finished and their results "
            "are above."
        )
        await _assert_invariants(pool, orchestrator, seed, authority, settled=True)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_s3_two_of_four_durable_writes_only_the_missing_two(
    pg_dsn: str,
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(
            pool, orchestrator, ["delivered", "delivered", "completed", "completed"]
        )
        authority = await _successor(pool, seed)
        before = {
            row["tool_call_id"]: tuple(row) for row in await _tool_rows(pool, seed)
        }
        plan = await _plan(orchestrator, seed, authority)
        assert [call["class"] for call in plan["calls"]] == [
            "delivered",
            "delivered",
            "ended",
            "ended",
        ]
        assert [call["needs_entry"] for call in plan["calls"]] == [
            False,
            False,
            True,
            True,
        ]

        result = await _settle(orchestrator, seed, authority, _members(plan))

        assert result["result"] == "applied"
        rows = await _tool_rows(pool, seed)
        after = {row["tool_call_id"]: tuple(row) for row in rows}
        # The two durable results are untouched and not repeated.
        for call_id in seed.call_ids[:2]:
            assert after[call_id] == before[call_id]
        assert [row["tool_call_id"] for row in rows if row["metrics"]] == (
            seed.call_ids[2:]
        )
        (continuation,) = await _continuations(pool, seed)
        assert "All 4 delegated tasks finished" in continuation["content"]
        await _assert_invariants(pool, orchestrator, seed, authority, settled=True)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_s4_all_results_no_final_answer_no_event_watermark_untouched(
    pg_dsn: str,
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, ["delivered"] * 4)
        authority = await _successor(pool, seed)
        listed = await orchestrator.list_live_session_subagent_recovery(
            str(seed.session), parent_authority=authority
        )
        assert listed == {"subagents": [], "recovery_turns": []}
        consumed = await _consumed(pool, seed)
        before = await _snapshot(pool, seed)

        result = await _settle(orchestrator, seed, authority, [])

        # The stateless lane replays the input with the results in the
        # transcript: no event, the watermark stays below the input.
        assert result["result"] == "already_delivered"
        assert result["delivery_id"] is None
        assert await _snapshot(pool, seed) == before
        assert await _consumed(pool, seed) == consumed
        assert consumed is None or consumed < seed.input_seq
        await _assert_invariants(pool, orchestrator, seed, authority, settled=False)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_live_child_with_a_durable_result_is_closed_without_an_event(
    pg_dsn: str,
) -> None:
    """Every call has its result, but one child row is still live: the live
    list keeps it, the settle ends it and writes nothing else."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, ["delivered", "delivered_running"])
        authority = await _successor(pool, seed)
        plan = await _plan(orchestrator, seed, authority)
        assert [call["class"] for call in plan["calls"]] == ["delivered", "delivered"]
        assert [call["needs_entry"] for call in plan["calls"]] == [False, True]
        assert [call["needs_message"] for call in plan["calls"]] == [False, False]
        tool_rows = await _tool_rows(pool, seed)

        result = await _settle(orchestrator, seed, authority, _members(plan))

        assert result["result"] == "already_delivered"
        assert await _continuations(pool, seed) == []
        assert await _tool_rows(pool, seed) == tool_rows
        live = await _child(pool, seed, seed.call_ids[1])
        assert live["subagent_outcome"] == "interrupted:parent_restart"
        assert live["stamp"] == str(live["runtime_generation"])
        await _assert_invariants(pool, orchestrator, seed, authority, settled=False)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_a_turn_with_its_final_answer_closes_members_without_results(
    pg_dsn: str,
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, ["completed", "running"])
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO thread_messages (id, thread_id, role, content, "
                "turn_number) VALUES ($1, $2, 'ai', 'Here is the comparison', 1)",
                uuid4(),
                seed.session,
            )
        authority = await _successor(pool, seed)
        plan = await _plan(orchestrator, seed, authority)

        result = await _settle(orchestrator, seed, authority, _members(plan))

        assert result["result"] == "already_delivered"
        assert await _continuations(pool, seed) == []
        assert await _tool_rows(pool, seed) == []
        # The turn answered its input: it is consumed once, never replayed.
        assert await _consumed(pool, seed) == seed.input_seq
        for call_id in seed.call_ids:
            child = await _child(pool, seed, call_id)
            assert child["stamp"] == str(child["runtime_generation"])
        assert (await _child(pool, seed, seed.call_ids[1]))["subagent_outcome"] == (
            "interrupted:parent_restart"
        )
        await _assert_invariants(pool, orchestrator, seed, authority, settled=False)
    finally:
        await pool.close()


async def _journal_turn_completed(pool: asyncpg.Pool, seed: Seed) -> None:
    """The loop's ``turn.completed`` frame for turn 1, after the delegating
    row. The pinned loop broadcasts it before its best-effort reconcile."""

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO thread_events (thread_id, epoch, seq, kind, payload) "
            "VALUES ($1, 0, 900, 'turn.completed', "
            '\'{"turn_id": 1, "metrics": {}}\'::jsonb)',
            seed.session,
        )


async def _source_state(pool: asyncpg.Pool, seed: Seed) -> str:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT state FROM thread_input_deliveries WHERE delivery_id=$1",
            seed.source_delivery_id,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["stateless", "pinned"])
async def test_a_turn_completed_frame_alone_writes_the_missing_results_no_event(
    pg_dsn: str, lane: str
) -> None:
    """§14.1: the pinned loop broadcasts ``turn.completed`` before its
    best-effort reconcile. With the strict result saves and the reconcile both
    failed, the frame is journaled and no result reached the transcript. The
    frame proves the turn ended, not that the parent saw the reports: the
    settle writes them, queues no continuation (the turn is over) and consumes
    the input as before. Lane-agnostic; the stateless loop broadcasts only
    after its authoritative reconcile, so there it cannot leave a result out."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, ["completed", "running"], lane=lane)
        await _journal_turn_completed(pool, seed)
        authority = (
            await _successor(pool, seed) if lane == "stateless" else seed.authority
        )
        plan = await _plan(orchestrator, seed, authority)
        assert [call["class"] for call in plan["calls"]] == ["ended", "live"]
        assert [call["needs_message"] for call in plan["calls"]] == [True, True]

        result = await _settle(orchestrator, seed, authority, _members(plan))

        assert result["result"] == "already_delivered"
        assert result["delivery_id"] is None
        rows = await _tool_rows(pool, seed)
        assert [row["tool_call_id"] for row in rows] == seed.call_ids
        assert [row["content"] for row in rows] == [
            _report(plan["calls"][0]),
            _interrupted(plan["calls"][1]),
        ]
        assert [_metrics(row)["class"] for row in rows] == [
            "completed",
            "interrupted",
        ]
        assert {row["turn_number"] for row in rows} == {1}
        assert [row["id"] for row in rows] == [
            session_subagent_batch_result_id(seed.session, seed.input_id, call)
            for call in seed.call_ids
        ]
        assert [call["result_message_id"] for call in result["calls"]] == [
            str(row["id"]) for row in rows
        ]
        assert await _continuations(pool, seed) == []
        if lane == "stateless":
            assert await _consumed(pool, seed) == seed.input_seq
        else:
            assert await _source_state(pool, seed) == "settled"
        for call_id in seed.call_ids:
            child = await _child(pool, seed, call_id)
            assert child["stamp"] == str(child["runtime_generation"])
        assert (await _child(pool, seed, seed.call_ids[1]))["subagent_outcome"] == (
            "interrupted:parent_restart"
        )
        await _assert_invariants(pool, orchestrator, seed, authority, settled=False)

        # Converged: a stale retry with the same members writes nothing, and
        # an empty one (nothing is listed any more) changes nothing either.
        before = await _snapshot(pool, seed)
        stale = await _settle(orchestrator, seed, authority, _members(plan))
        assert stale["result"] == "stale"
        empty = await _settle(orchestrator, seed, authority, [])
        assert empty["result"] == "already_delivered"
        assert await _snapshot(pool, seed) == before
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_pinned_all_results_with_a_live_member_queue_one_continuation(
    pg_dsn: str,
) -> None:
    """§14.1: every call has its result, a member is still live, and the
    turn has no final answer. The pinned lane never serves an admitted input
    again, so leaving the source admitted strands the request. As the
    single-child path does for a live child with a durable result, the settle
    ends the member, writes the continuation alone and settles the source."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(
            pool, orchestrator, ["delivered", "delivered_running"], lane="pinned"
        )
        authority = seed.authority
        plan = await _plan(orchestrator, seed, authority)
        assert [call["class"] for call in plan["calls"]] == ["delivered", "delivered"]
        assert [call["needs_entry"] for call in plan["calls"]] == [False, True]
        tool_rows = await _tool_rows(pool, seed)

        result = await _settle(orchestrator, seed, authority, _members(plan))

        assert result["result"] == "applied"
        assert result["delivery_id"] == str(seed.delivery_id)
        assert [call["result_message_id"] for call in result["calls"]] == [None, None]
        assert await _tool_rows(pool, seed) == tool_rows
        (continuation,) = await _continuations(pool, seed)
        assert continuation["state"] == "owned"
        assert continuation["turn_number"] == 1
        assert continuation["supersedes_input_seq"] == seed.input_seq
        assert continuation["content"] == batch_continuation_text(
            calls=2, interrupted=0, not_started=0, declined=0, retired=0
        )
        assert "All 2 delegated tasks finished" in continuation["content"]
        assert _metrics(continuation)["kind"] == "continuation"
        assert await _source_state(pool, seed) == "settled"
        live = await _child(pool, seed, seed.call_ids[1])
        assert (live["status"], live["subagent_outcome"]) == (
            "ended",
            "interrupted:parent_restart",
        )
        assert live["stamp"] == str(live["runtime_generation"])
        await _assert_invariants(pool, orchestrator, seed, authority, settled=True)

        before = await _snapshot(pool, seed)
        again = await _settle(orchestrator, seed, authority, _members(plan))
        assert again["result"] == "idempotent"
        assert again["delivery_id"] == str(seed.delivery_id)
        assert await _snapshot(pool, seed) == before
        assert len(await _continuations(pool, seed)) == 1
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_s5_crash_before_commit_rolls_back_and_a_lost_response_is_idempotent(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, S1)
        authority = await _successor(pool, seed)
        plan = await _plan(orchestrator, seed, authority)
        members = _members(plan)
        before = await _snapshot(pool, seed)

        # The process dies after the results were written and before the
        # continuation: the transaction rolls back as a whole.
        real = input_delivery_module.persist_input_delivery

        async def dies(*args, **kwargs):
            raise RuntimeError("executor killed inside the settle")

        monkeypatch.setattr(input_delivery_module, "persist_input_delivery", dies)
        with pytest.raises(RuntimeError, match="executor killed"):
            await _settle(orchestrator, seed, authority, members)
        assert await _snapshot(pool, seed) == before
        assert await _assert_predicates_agree(pool, orchestrator, seed, authority)

        # The successor lists the same candidates and converges.
        monkeypatch.setattr(input_delivery_module, "persist_input_delivery", real)
        assert await _plan(orchestrator, seed, authority) == plan
        first = await _settle(orchestrator, seed, authority, members)
        assert first["result"] == "applied"
        settled = await _snapshot(pool, seed)

        # The response was lost after the commit: the retry is idempotent.
        retry = await _settle(orchestrator, seed, authority, members)
        assert retry["result"] == "idempotent"
        assert retry["delivery_id"] == first["delivery_id"]
        assert retry["delivery"] == first["delivery"]
        assert await _snapshot(pool, seed) == settled
        await _assert_invariants(pool, orchestrator, seed, authority, settled=True)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_s6_stale_authority_and_stale_members_write_nothing(pg_dsn: str) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, S1)
        authority = await _successor(pool, seed)
        plan = await _plan(orchestrator, seed, authority)
        members = _members(plan)
        completed, running = seed.call_ids[:2]
        before = await _snapshot(pool, seed)

        # 4. The executor that ran the batch lost its lease.
        with pytest.raises(SessionParentAuthorityRefused) as refused:
            await _settle(orchestrator, seed, seed.authority, members)
        assert refused.value.reason == "stateless_parent_not_current"
        assert await _snapshot(pool, seed) == before

        # A member at another generation, a missing member, an extra member:
        # the whole settle is refused with the server's view.
        wrong_generation = [dict(member) for member in members]
        wrong_generation[1]["runtime_generation"] = str(uuid4())
        missing = members[:1]
        extra = members + [
            {
                **members[0],
                "thread_id": str(uuid4()),
                "runtime_generation": str(uuid4()),
            }
        ]
        for request, reason in (
            (wrong_generation, "generation_differs"),
            (missing, "members_differ"),
            (extra, "members_differ"),
        ):
            stale = await _settle(orchestrator, seed, authority, request)
            assert stale["result"] == "stale"
            assert stale["reason"] == reason
            assert [call["class"] for call in stale["calls"]] == [
                call["class"] for call in plan["calls"]
            ]
            assert await _snapshot(pool, seed) == before

        # 3. Terminal facts are immutable, and a live child can only be
        # reported as interrupted by the restart.
        def replaced(index: int, **changes) -> list[dict]:
            request = [dict(member) for member in members]
            request[index].update(changes)
            return request

        for request, message in (
            (replaced(0, subagent_status="error"), "changed its terminal status"),
            (replaced(0, turns=99), "changed its turns"),
            (replaced(0, report_path="elsewhere.md"), "changed its report_path"),
            (replaced(1, subagent_status="completed"), "interrupted parent-restart"),
            (replaced(1, outcome="interrupted"), "interrupted parent-restart"),
            (replaced(0, message="  "), "needs a message"),
            (replaced(1, message=None), "needs a message"),
        ):
            with pytest.raises(ValueError, match=message):
                await _settle(orchestrator, seed, authority, request)
            assert await _snapshot(pool, seed) == before
        assert (await _child(pool, seed, running))["status"] == "active"
        assert (await _child(pool, seed, completed))["stamp"] is None
        assert await _continuations(pool, seed) == []
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_s7_rewind_refused_while_a_member_is_owed_and_applies_after(
    pg_dsn: str,
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, S1)
        user = {"id": str(seed.owner), "is_admin": False}
        rewind = ThreadRewindService(orchestrator, lambda: True)

        # The unit went idle with the input consumed and two members owed
        # (the executor advanced the watermark before it died).
        async with pool.acquire() as conn:
            assert (
                await complete_unit(
                    conn,
                    unit_id=seed.session,
                    lease_token=seed.authority["lease_token"],
                    consumed_seq=seed.input_seq,
                )
                == "done"
            )
            assert await conn.fetchval(LIVE_SESSION_CHILD_EXISTS_SQL, seed.session)
        preview = await rewind.preview(str(seed.session), seed.input_id, user)
        assert preview.eligible is False
        assert preview.refusal_code == "pending_child"

        # The unit is woken again; its successor settles the turn.
        async with pool.acquire() as conn:
            await record_input_seq(
                conn,
                unit_id=seed.session,
                unit_kind=UNIT_KIND_SESSION_TURN,
                input_seq=seed.input_seq,
            )
            claim = await claim_unit(
                conn,
                unit_kind=UNIT_KIND_SESSION_TURN,
                pod_name=SUCCESSOR,
                prefer_unit_id=seed.session,
            )
            await _stamp_stateless_claim(
                conn,
                seed.session,
                token=claim.lease_token,
                pod=SUCCESSOR,
                pod_uid=SUCCESSOR_UID,
            )
        authority = {
            **seed.authority,
            "lease_token": claim.lease_token,
            "executor_id": SUCCESSOR,
            "executor_pod_uid": SUCCESSOR_UID,
        }
        plan = await _plan(orchestrator, seed, authority)
        settled = await _settle(orchestrator, seed, authority, _members(plan))
        assert settled["result"] == "applied"
        await _assert_invariants(pool, orchestrator, seed, authority, settled=True)

        # The recovery turn runs the continuation to its answer, and the unit
        # goes idle again.
        async with pool.acquire() as conn:
            async with conn.transaction():
                claimed = await claim_stateless_input_delivery(
                    conn,
                    thread_id=seed.session,
                    delivery_id=seed.delivery_id,
                    lease_token=claim.lease_token,
                    executor_id=SUCCESSOR,
                    pod_uid=SUCCESSOR_UID,
                )
            for transition in ("admitted", "settled"):
                async with conn.transaction():
                    assert await transition_stateless_input_delivery(
                        conn,
                        thread_id=seed.session,
                        delivery_id=seed.delivery_id,
                        lease_token=claim.lease_token,
                        executor_id=SUCCESSOR,
                        pod_uid=SUCCESSOR_UID,
                        claim_generation=claimed["claim_generation"],
                        transition=transition,
                        turn_number=1 if transition == "admitted" else None,
                    )
                if transition == "admitted":
                    await conn.execute(
                        "INSERT INTO thread_messages (id, thread_id, role, content, "
                        "turn_number) VALUES ($1, $2, 'ai', 'Comparison', 1)",
                        uuid4(),
                        seed.session,
                    )
            input_seq = await conn.fetchval(
                "SELECT input_seq FROM run_queue WHERE unit_id=$1", seed.session
            )
            assert (
                await complete_unit(
                    conn,
                    unit_id=seed.session,
                    lease_token=claim.lease_token,
                    consumed_seq=int(input_seq),
                )
                == "done"
            )

        # Rewind to the abandoned prompt now applies and sweeps the batch,
        # its results and the continuation.
        preview = await rewind.preview(str(seed.session), seed.input_id, user)
        assert preview.eligible is True, preview.refusal_code
        result, duplicate = await rewind.apply(
            str(seed.session),
            StatelessRewindRequest(
                client_request_id=uuid4(),
                message_id=seed.input_id,
                mode="conversation",
                expected=preview.expected,
            ),
            user,
        )
        assert duplicate is False
        async with pool.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM thread_messages WHERE thread_id=$1 "
                    "AND rewound_at IS NULL",
                    seed.session,
                )
                == 0
            )
            assert not await conn.fetchval(LIVE_SESSION_CHILD_EXISTS_SQL, seed.session)

        # The user sends the prompt again; a stale successor retries the old
        # settle under the new lease. It gets the historical continuation back
        # and queues nothing.
        async with pool.acquire() as conn:
            resent = await conn.fetchval(
                "INSERT INTO thread_messages (id, thread_id, role, content, "
                "turn_number) VALUES ($1, $2, 'human', $3, 1) RETURNING seq",
                uuid4(),
                seed.session,
                INPUT,
            )
            await record_input_seq(
                conn,
                unit_id=seed.session,
                unit_kind=UNIT_KIND_SESSION_TURN,
                input_seq=int(resent),
            )
            claim = await claim_unit(
                conn,
                unit_kind=UNIT_KIND_SESSION_TURN,
                pod_name=SUCCESSOR,
                prefer_unit_id=seed.session,
            )
            await _stamp_stateless_claim(
                conn,
                seed.session,
                token=claim.lease_token,
                pod=SUCCESSOR,
                pod_uid=SUCCESSOR_UID,
            )
        authority = {**authority, "lease_token": claim.lease_token}
        assert (
            await _assert_predicates_agree(pool, orchestrator, seed, authority) is False
        )
        before = await _snapshot(pool, seed)
        retried = await _settle(orchestrator, seed, authority, _members(plan))
        assert retried["result"] == "idempotent"
        assert retried["delivery_id"] == str(seed.delivery_id)
        assert retried["execution_disposition"] == "historical"
        assert await _snapshot(pool, seed) == before
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_s8_a_declined_call_is_reported_as_declined(pg_dsn: str) -> None:
    """The user's decision is durable when it is made
    (``thread_permission_requests.status='denied'``); its result is written
    only after the whole batch. A timed-out request is not a decline."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(
            pool, orchestrator, ["completed", "declined", "expired", "running"]
        )
        completed, declined, expired, running = seed.call_ids
        authority = await _successor(pool, seed)
        plan = await _plan(orchestrator, seed, authority)
        assert [call["class"] for call in plan["calls"]] == [
            "ended",
            "declined",
            "not_started",
            "live",
        ]

        result = await _settle(orchestrator, seed, authority, _members(plan))

        assert result["result"] == "applied"
        rows = {row["tool_call_id"]: row for row in await _tool_rows(pool, seed)}
        assert rows[declined]["content"] == DECLINED_RESULT_TEXT
        assert _metrics(rows[declined])["class"] == "declined"
        assert _metrics(rows[declined])["thread_id"] is None
        assert rows[expired]["content"] == not_started_result_text()
        assert _metrics(rows[expired])["class"] == "not_started"
        (continuation,) = await _continuations(pool, seed)
        assert continuation["content"] == batch_continuation_text(
            calls=4, interrupted=1, not_started=1, declined=1, retired=0
        )
        assert "1 was declined by the user" in continuation["content"]
        await _assert_invariants(pool, orchestrator, seed, authority, settled=True)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_s9_pinned_batch_settles_the_source_delivery(pg_dsn: str) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, S1, lane="pinned")
        authority = seed.authority
        plan = await _plan(orchestrator, seed, authority)
        assert [call["class"] for call in plan["calls"]] == [
            "ended",
            "live",
            "not_started",
            "not_started",
        ]

        result = await _settle(orchestrator, seed, authority, _members(plan))

        assert result["result"] == "applied"
        async with pool.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT state FROM thread_input_deliveries WHERE delivery_id=$1",
                    seed.source_delivery_id,
                )
                == "settled"
            )
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM run_queue WHERE unit_id=$1", seed.session
                )
                == 0
            )
        (continuation,) = await _continuations(pool, seed)
        # The pinned runtime owns the continuation it will deliver.
        assert continuation["state"] == "owned"
        assert continuation["turn_number"] == 1
        await _assert_invariants(pool, orchestrator, seed, authority, settled=True)
        again = await _settle(orchestrator, seed, authority, _members(plan))
        assert again["result"] == "idempotent"
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_s9_pinned_all_results_no_event_source_untouched(pg_dsn: str) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, ["delivered"] * 4, lane="pinned")
        listed = await orchestrator.list_live_session_subagent_recovery(
            str(seed.session), parent_authority=seed.authority
        )
        assert listed == {"subagents": [], "recovery_turns": []}
        before = await _snapshot(pool, seed)

        result = await _settle(orchestrator, seed, seed.authority, [])

        assert result["result"] == "already_delivered"
        assert await _snapshot(pool, seed) == before
        async with pool.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT state FROM thread_input_deliveries WHERE delivery_id=$1",
                    seed.source_delivery_id,
                )
                == "admitted"
            )
        await _assert_invariants(
            pool, orchestrator, seed, seed.authority, settled=False
        )
    finally:
        await pool.close()


async def _queued_delivery_state(pool: asyncpg.Pool, seed: Seed) -> str:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT state FROM thread_input_deliveries WHERE delivery_id=$1",
            seed.queued_delivery_id,
        )


@pytest.mark.asyncio
async def test_s9_pinned_input_queued_behind_the_batch_keeps_the_manifest(
    pg_dsn: str,
) -> None:
    """A message sent before turn 1 began carries turn 1 too while it waits.
    It is not a boundary of turn 1: only the continuation that supersedes an
    input ends that input's rows. (Review of WP1: the manifest came back
    empty, so the settle was refused forever and rewind stayed blocked.)"""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(
            pool, orchestrator, S1, lane="pinned", queued_behind_the_input=True
        )
        plan = await _plan(orchestrator, seed, seed.authority)
        assert [call["class"] for call in plan["calls"]] == [
            "ended",
            "live",
            "not_started",
            "not_started",
        ]

        result = await _settle(orchestrator, seed, seed.authority, _members(plan))

        assert result["result"] == "applied"
        assert [row["tool_call_id"] for row in await _tool_rows(pool, seed)] == (
            seed.call_ids
        )
        await _assert_invariants(pool, orchestrator, seed, seed.authority, settled=True)
        # The queued message is left for its own turn.
        assert await _queued_delivery_state(pool, seed) == "owned"
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_s9_pinned_input_that_ran_as_the_next_turn_stays_out_of_the_batch(
    pg_dsn: str,
) -> None:
    """The queued message was admitted as turn 2 and its turn ran to an
    answer, delegating once itself. None of that joins turn 1: not its call,
    and not its final answer, which would otherwise close turn 1's members
    without their results."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(
            pool, orchestrator, S1, lane="pinned", queued_behind_the_input=True
        )
        authority = seed.authority
        next_turn_call = f"call_next_turn_{uuid4().hex[:8]}"
        async with pool.acquire() as conn:
            identity = dict(
                agent_id=authority["agent_id"],
                pod_uid=authority["pod_uid"],
                runtime_generation=authority["session_runtime_generation"],
                session_runtime_generation=authority["session_runtime_generation"],
                runtime_attach_token=authority["runtime_attach_token"],
                claim_generation=seed.queued_claim_generation,
            )
            assert await transition_input_delivery(
                conn,
                delivery_id=seed.queued_delivery_id,
                transition="admitted",
                turn_number=2,
                **identity,
            )
            for role, content, tool_calls, tool_call_id in (
                (
                    "ai",
                    "",
                    [{"id": next_turn_call, "name": "delegate_agent", "args": {}}],
                    None,
                ),
                ("tool", "[subagent explorer-0009] done", None, next_turn_call),
                ("ai", "Switzerland is included now.", None, None),
            ):
                await conn.execute(
                    "INSERT INTO thread_messages (id, thread_id, role, content, "
                    "tool_calls, tool_call_id, turn_number) "
                    "VALUES ($1, $2, $3, $4, $5::jsonb, $6, 2)",
                    uuid4(),
                    seed.session,
                    role,
                    content,
                    json.dumps(tool_calls) if tool_calls is not None else None,
                    tool_call_id,
                )
            assert await transition_input_delivery(
                conn,
                delivery_id=seed.queued_delivery_id,
                transition="settled",
                **identity,
            )
        plan = await _plan(orchestrator, seed, authority)
        assert next_turn_call not in {call["tool_call_id"] for call in plan["calls"]}

        result = await _settle(orchestrator, seed, authority, _members(plan))

        assert result["result"] == "applied"
        written = [row for row in await _tool_rows(pool, seed) if row["metrics"]]
        assert [row["tool_call_id"] for row in written] == seed.call_ids
        await _assert_invariants(pool, orchestrator, seed, authority, settled=True)
        assert await _queued_delivery_state(pool, seed) == "settled"
    finally:
        await pool.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("model", ["claude-opus-5-5", "gpt-5.5"])
async def test_s11_input_typed_during_the_batch_follows_the_continuation(
    pg_dsn: str, model: str
) -> None:
    """Invariant 12. The results a recovery writes land at the tail of ``seq``,
    behind input typed during the batch. They carry the abandoned turn's
    number, so restore places them behind their call; the continuation keeps
    ``source='subagent'`` and ``supersedes_input_seq``, so the executor serves
    it before the typed input; and after the recovery turn the restored order
    is call, results, continuation, answer, typed input."""

    from agent.core.context import (
        repair_tool_pairing,
        sanitize_history_for_provider_boundary,
    )

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        agent_db = _agent_db(pool)
        seed = await _seed(pool, orchestrator, S1, typed_during_the_batch=True)
        assert seed.ai_seq < seed.typed_seq
        authority = await _successor(pool, seed)
        plan = await _plan(orchestrator, seed, authority)
        result = await _settle(orchestrator, seed, authority, _members(plan))
        assert result["result"] == "applied"
        await _assert_invariants(pool, orchestrator, seed, authority, settled=True)
        rows = await _tool_rows(pool, seed)
        assert min(row["seq"] for row in rows) > seed.typed_seq
        (continuation,) = await _continuations(pool, seed)

        # The executor serves the continuation first, then the typed input.
        async with pool.acquire() as conn:
            pending = await conn.fetch(
                _PENDING_INPUT_SQL, seed.session, await _consumed(pool, seed), 10
            )
        assert [row["id"] for row in pending] == [
            continuation["message_id"],
            seed.typed_id,
        ]
        assert pending[0]["supersedes_input_seq"] == seed.input_seq

        async def restore() -> list:
            history = await agent_db.get_thread_messages_history(
                thread_id=str(seed.session), limit=1000, newest_first=True
            )
            restored = repair_tool_pairing(_db_rows_to_lc_messages(history))
            return sanitize_history_for_provider_boundary(restored, model)

        # While the continuation is pending it is not history yet.
        restored = await restore()
        assert [message.type for message in restored] == (
            ["human", "ai"] + ["tool"] * 4 + ["human"]
        )
        assert [message.tool_call_id for message in restored[2:6]] == seed.call_ids
        assert restored[6].content == TYPED_DURING_THE_BATCH

        # The recovery turn runs the continuation as the abandoned turn.
        async with pool.acquire() as conn:
            async with conn.transaction():
                claimed = await claim_stateless_input_delivery(
                    conn,
                    thread_id=seed.session,
                    delivery_id=seed.delivery_id,
                    lease_token=authority["lease_token"],
                    executor_id=SUCCESSOR,
                    pod_uid=SUCCESSOR_UID,
                )
            assert claimed is not None and claimed["turn_number"] == 1
            for transition in ("admitted", "settled"):
                async with conn.transaction():
                    assert await transition_stateless_input_delivery(
                        conn,
                        thread_id=seed.session,
                        delivery_id=seed.delivery_id,
                        lease_token=authority["lease_token"],
                        executor_id=SUCCESSOR,
                        pod_uid=SUCCESSOR_UID,
                        claim_generation=claimed["claim_generation"],
                        transition=transition,
                        turn_number=1 if transition == "admitted" else None,
                    )
                if transition == "admitted":
                    await conn.execute(
                        "INSERT INTO thread_messages (id, thread_id, role, content, "
                        "turn_number) VALUES ($1, $2, 'ai', 'The comparison', 1)",
                        uuid4(),
                        seed.session,
                    )

        restored = await restore()
        assert [message.type for message in restored] == (
            ["human", "ai"] + ["tool"] * 4 + ["human", "ai", "human"]
        )
        assert restored[1].tool_calls[0]["id"] == seed.call_ids[0]
        assert [message.tool_call_id for message in restored[2:6]] == seed.call_ids
        assert restored[6].content == continuation["content"]
        assert restored[7].content == "The comparison"
        assert restored[8].content == TYPED_DURING_THE_BATCH
        # 10. Each result reaches the parent exactly once (the two calls that
        # never started share one text).
        contents = Counter(str(message.content) for message in restored)
        written = Counter(row["content"] for row in rows)
        assert all(contents[text] == count for text, count in written.items())
        pending_typed = [
            {
                "id": str(seed.typed_id),
                "role": "human",
                "content": TYPED_DURING_THE_BATCH,
            }
        ]
        assert strip_restored_pending_humans(restored, pending_typed) == 1
        assert restored[-1].content == "The comparison"
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# Writers that must agree (§7)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retired_members_are_reported_as_cancelled(pg_dsn: str) -> None:
    """End cancelled the live child as ``cancelled:parent_retired``; a sibling
    that had finished stays owed. The settle after Resume reports both."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, ["completed", "running", "none"])
        completed, retired, queued = seed.call_ids
        async with pool.acquire() as conn:
            async with conn.transaction():
                assert (
                    await orchestrator._terminalize_live_session_subagents_for_retirement(
                        conn,
                        parent_thread_id=seed.session,
                        execution_lane="stateless",
                        disposition="ended",
                    )
                    == {"terminalized": 1, "deliveries": 0}
                )
        authority = await _successor(pool, seed)
        plan = await _plan(orchestrator, seed, authority)
        assert [call["class"] for call in plan["calls"]] == [
            "ended",
            "retired",
            "not_started",
        ]
        assert [call["needs_entry"] for call in plan["calls"]] == [True, False, False]

        result = await _settle(orchestrator, seed, authority, _members(plan))

        assert result["result"] == "applied"
        rows = {row["tool_call_id"]: row for row in await _tool_rows(pool, seed)}
        assert rows[retired]["content"].startswith(
            "[delegate_agent: CANCELLED - session stopped]\n"
            "handle: reader-0001   type: reader\n"
        )
        assert _metrics(rows[retired])["class"] == "retired"
        assert _metrics(rows[retired])["subagent_status"] == "cancelled"
        (continuation,) = await _continuations(pool, seed)
        assert (
            "1 never started and 1 was cancelled when the session was stopped"
            in continuation["content"]
        )
        assert (await _child(pool, seed, retired))["subagent_outcome"] == (
            "cancelled:parent_retired"
        )
        await _assert_invariants(pool, orchestrator, seed, authority, settled=True)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_live_list_and_rewind_predicate_agree_on_every_state(
    pg_dsn: str,
) -> None:
    """Rewind refuses exactly while recovery still owes a member."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        cases = {
            "no child": (["none"] * 4, False),
            "one owed": (S1, True),
            "all delivered": (["delivered"] * 4, False),
            "delivered but live": (["delivered", "delivered_running"], True),
        }
        for name, (states, owed) in cases.items():
            seed = await _seed(pool, orchestrator, states)
            owes = await _assert_predicates_agree(
                pool, orchestrator, seed, seed.authority
            )
            assert owes is owed, name
            if owed:
                authority = await _successor(pool, seed)
                plan = await _plan(orchestrator, seed, authority)
                await _settle(orchestrator, seed, authority, _members(plan))
                assert (
                    await _assert_predicates_agree(pool, orchestrator, seed, authority)
                    is False
                ), name
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_a_recovery_turn_that_delegates_is_settled_against_its_continuation(
    pg_dsn: str,
) -> None:
    """D3: the recovery turn may delegate again. Its children name the
    continuation as their parent input, and a crash in that turn is settled
    against the continuation. The original input is not touched again, and
    input the user typed during the first batch is still owed its turn."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(
            pool, orchestrator, ["completed", "none"], typed_during_the_batch=True
        )
        authority = await _successor(pool, seed)
        first = await _settle(
            orchestrator,
            seed,
            authority,
            _members(await _plan(orchestrator, seed, authority)),
        )
        assert first["result"] == "applied"
        (continuation,) = await _continuations(pool, seed)

        # The recovery turn is admitted and delegates two more children.
        async with pool.acquire() as conn:
            async with conn.transaction():
                claimed = await claim_stateless_input_delivery(
                    conn,
                    thread_id=seed.session,
                    delivery_id=seed.delivery_id,
                    lease_token=authority["lease_token"],
                    executor_id=SUCCESSOR,
                    pod_uid=SUCCESSOR_UID,
                )
            async with conn.transaction():
                assert await transition_stateless_input_delivery(
                    conn,
                    thread_id=seed.session,
                    delivery_id=seed.delivery_id,
                    lease_token=authority["lease_token"],
                    executor_id=SUCCESSOR,
                    pod_uid=SUCCESSOR_UID,
                    claim_generation=claimed["claim_generation"],
                    transition="admitted",
                    turn_number=1,
                )
            second_ai = uuid4()
            second_calls = [
                f"call_again_{index}_{uuid4().hex[:6]}" for index in range(2)
            ]
            await conn.execute(
                "INSERT INTO thread_messages "
                "(id, thread_id, role, content, tool_calls, turn_number) "
                "VALUES ($1, $2, 'ai', '', $3::jsonb, 1)",
                second_ai,
                seed.session,
                json.dumps(
                    [
                        {"id": call_id, "name": "delegate_agent", "args": {}}
                        for call_id in second_calls
                    ]
                ),
            )
        children = []
        for index, call_id in enumerate(second_calls):
            child = await orchestrator.create_session_subagent_thread(
                parent_thread_id=str(seed.session),
                parent_authority=authority,
                handle=f"explorer-{index:04x}",
                subagent_type="explorer",
                parent_tool_call_id=call_id,
                parent_input_message_id=str(continuation["message_id"]),
                parent_ai_message_id=str(second_ai),
                parent_iteration=1,
            )
            children.append(child)
        await orchestrator.terminalize_session_subagent_thread(
            parent_thread_id=str(seed.session),
            parent_authority=authority,
            thread_id=children[0]["thread_id"],
            runtime_generation=children[0]["runtime_generation"],
            subagent_status="completed",
            outcome="completed",
        )

        # That executor dies too; the next one settles the recovery turn.
        async with pool.acquire() as conn:
            await release_unit(
                conn, unit_id=seed.session, lease_token=authority["lease_token"]
            )
            claim = await claim_unit(
                conn,
                unit_kind=UNIT_KIND_SESSION_TURN,
                pod_name=POD,
                prefer_unit_id=seed.session,
            )
            await _stamp_stateless_claim(
                conn, seed.session, token=claim.lease_token, pod=POD, pod_uid=POD_UID
            )
        third = {**seed.authority, "lease_token": claim.lease_token}
        # The recovery turn reuses turn 1. The original input's rows end at
        # the continuation that supersedes it, so its manifest keeps its own
        # two calls and the recovery turn's calls belong to the continuation.
        async with pool.acquire() as conn:
            for source_id, calls in (
                (seed.input_id, seed.call_ids),
                (continuation["message_id"], second_calls),
            ):
                source = await load_recovery_parent_input(
                    conn,
                    execution_lane="stateless",
                    parent_thread_id=seed.session,
                    parent_input_message_id=source_id,
                    parent_iteration=1,
                )
                manifest = await load_delegation_manifest(
                    conn,
                    parent_thread_id=seed.session,
                    parent_input=source,
                    parent_iteration=1,
                    lock_children=False,
                )
                assert [call.tool_call_id for call in manifest] == calls
        listed = await orchestrator.list_live_session_subagent_recovery(
            str(seed.session), parent_authority=third
        )
        (plan,) = listed["recovery_turns"]
        assert plan["parent_input_message_id"] == str(continuation["message_id"])
        assert plan["supersedes_input_seq"] == continuation["seq"]
        assert [call["tool_call_id"] for call in plan["calls"]] == second_calls
        assert [call["class"] for call in plan["calls"]] == ["ended", "live"]

        second = await orchestrator.settle_session_subagent_batch(
            parent_thread_id=str(seed.session),
            parent_authority=third,
            parent_input_message_id=str(continuation["message_id"]),
            parent_iteration=1,
            members=_members(plan),
        )

        assert second["result"] == "applied"
        assert second["delivery_id"] == str(
            session_subagent_batch_delivery_id(seed.session, continuation["message_id"])
        )
        deliveries = await _continuations(pool, seed)
        assert [row["supersedes_input_seq"] for row in deliveries] == [
            seed.input_seq,
            continuation["seq"],
        ]
        # The first continuation's recovery turn is settled with it.
        assert [row["state"] for row in deliveries] == ["settled", "queued"]
        assert all(row["turn_number"] == 1 for row in deliveries)
        rows = await _tool_rows(pool, seed)
        assert [row["tool_call_id"] for row in rows] == seed.call_ids + second_calls
        # An event input is consumed by its own delivery (settled above), not
        # by the human watermark: that stays on the first input, and the typed
        # input is served right after the new continuation.
        assert await _consumed(pool, seed) == seed.input_seq
        async with pool.acquire() as conn:
            pending = await conn.fetch(
                _PENDING_INPUT_SQL, seed.session, seed.input_seq, 10
            )
        assert [row["id"] for row in pending] == [
            deliveries[1]["message_id"],
            seed.typed_id,
        ]
        assert (
            await orchestrator.list_live_session_subagent_threads(
                str(seed.session), parent_authority=third
            )
            == []
        )
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# Races, with real concurrent transactions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_recoverers_at_once_settle_the_turn_once(pg_dsn: str) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, S1)
        authority = await _successor(pool, seed)
        members = _members(await _plan(orchestrator, seed, authority))

        results = await asyncio.wait_for(
            asyncio.gather(
                _settle(orchestrator, seed, authority, members),
                _settle(orchestrator, seed, authority, members),
            ),
            timeout=10,
        )

        # 8. The parent lock serializes them: no deadlock, one write.
        assert sorted(result["result"] for result in results) == [
            "applied",
            "idempotent",
        ]
        assert {result["delivery_id"] for result in results} == {str(seed.delivery_id)}
        await _assert_invariants(pool, orchestrator, seed, authority, settled=True)
    finally:
        await pool.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("order", ["concurrent", "settle_first", "end_first"])
async def test_retirement_against_the_settle_never_mixes_outcomes(
    pg_dsn: str, order: str
) -> None:
    """Force-End and the settle both lock the parent first. Either the settle
    wins and End finds nothing live, or End cancels the live child and steals
    the lease, and the settle is refused and writes nothing. The concurrent
    run takes whichever order the locks give it; the other two force each."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, S1)
        running = seed.call_ids[1]
        authority = await _successor(pool, seed)
        members = _members(await _plan(orchestrator, seed, authority))

        async def settle_it():
            return await _settle(orchestrator, seed, authority, members)

        async def end_it():
            return await orchestrator.begin_stateless_thread_workspace_retirement(
                str(seed.session), force=True, permanent=False
            )

        async def captured(operation):
            try:
                return await operation()
            except Exception as exc:  # the loser's refusal is the outcome
                return exc

        if order == "concurrent":
            settle, retirement = await asyncio.wait_for(
                asyncio.gather(settle_it(), end_it(), return_exceptions=True),
                timeout=10,
            )
        elif order == "settle_first":
            settle = await captured(settle_it)
            retirement = await captured(end_it)
        else:
            retirement = await captured(end_it)
            settle = await captured(settle_it)

        assert not isinstance(retirement, BaseException), retirement
        assert retirement["state"] == "closed"
        child = await _child(pool, seed, running)
        rows = await _tool_rows(pool, seed)
        continuations = await _continuations(pool, seed)
        if order != "concurrent":
            assert isinstance(settle, BaseException) is (order == "end_first")
        if isinstance(settle, BaseException):
            assert isinstance(settle, SessionParentAuthorityRefused), settle
            assert child["subagent_outcome"] == "cancelled:parent_retired"
            assert rows == [] and continuations == []
        else:
            assert settle["result"] == "applied"
            assert child["subagent_outcome"] == "interrupted:parent_restart"
            assert [row["tool_call_id"] for row in rows] == seed.call_ids
            assert len(continuations) == 1
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_a_late_terminal_write_from_a_stale_generation_changes_nothing(
    pg_dsn: str,
) -> None:
    """The executor that ran the batch is not dead, only replaced: its running
    child finishes during and after the settle and tries to record it."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        agent_db = _agent_db(pool)
        seed = await _seed(pool, orchestrator, S1)
        running = seed.call_ids[1]
        child = seed.children[running]
        authority = await _successor(pool, seed)
        members = _members(await _plan(orchestrator, seed, authority))

        def late_write():
            return agent_db.update_session_subagent_thread(
                child["thread_id"],
                parent_thread_id=str(seed.session),
                parent_authority=seed.authority,
                runtime_generation=child["runtime_generation"],
                status="ended",
                subagent_status="completed",
                outcome="completed",
                turns=9,
                tokens=9000,
                ended=True,
            )

        settled, late = await asyncio.wait_for(
            asyncio.gather(
                _settle(orchestrator, seed, authority, members),
                late_write(),
                return_exceptions=True,
            ),
            timeout=10,
        )
        assert settled["result"] == "applied"
        assert isinstance(late, SessionParentAuthorityRefused)
        after = await _snapshot(pool, seed)

        # After the settle, through both writers and with either authority.
        with pytest.raises(SessionParentAuthorityRefused):
            await late_write()
        with pytest.raises(SessionParentAuthorityRefused):
            await orchestrator.terminalize_session_subagent_thread(
                parent_thread_id=str(seed.session),
                parent_authority=seed.authority,
                thread_id=child["thread_id"],
                runtime_generation=child["runtime_generation"],
                subagent_status="completed",
                outcome="completed",
            )
        assert (
            await agent_db.update_session_subagent_thread(
                child["thread_id"],
                parent_thread_id=str(seed.session),
                parent_authority=authority,
                runtime_generation=child["runtime_generation"],
                status="ended",
                subagent_status="completed",
                outcome="completed",
                ended=True,
            )
            is False
        )
        with pytest.raises(ValueError, match="changed its terminal status"):
            await orchestrator.terminalize_session_subagent_thread(
                parent_thread_id=str(seed.session),
                parent_authority=authority,
                thread_id=child["thread_id"],
                runtime_generation=child["runtime_generation"],
                subagent_status="completed",
                outcome="completed",
            )
        assert await _snapshot(pool, seed) == after
        interrupted = await _child(pool, seed, running)
        assert interrupted["subagent_outcome"] == "interrupted:parent_restart"
        await _assert_invariants(pool, orchestrator, seed, authority, settled=True)
    finally:
        await pool.close()
