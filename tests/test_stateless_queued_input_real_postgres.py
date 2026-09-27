"""Real-PostgreSQL proofs: a stateless input is answered only by its own turn.

knowledge-base/knowledge/issues/stateless_second_input_skipped_as_answered.md

The stateless lane admits input while a turn runs: admission commits the human
row (``turn_number = total_turns + 1``) and only advances the unit's input
watermark. The running turn's answer is persisted later, so its ``seq`` is
greater than the queued input's. The claim-time skip-if-answered check must not
read that later position as proof that the queued input was answered, and it
must still recognise a turn whose answer is durable but whose watermark
settlement never ran.

These tests drive the executor's real claim decision (``_serve_claim``: the
watermark and transcript legs of skip-if-answered) against the real admission,
queue, reaper and settlement SQL on a migrated database. Only the turn body is
simulated — by the exact durable writes the serving executor and the loop make
for one turn: open the input's interrupt window, persist the answer under the
claim's lease (the loop's authoritative final reconcile, or only its
incremental writer when settlement dies first), then the settled close
(watermark checkpoint) and completion.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from agent.api import turn_executor as te
from agent.api.lease_context import LeaseLostError, current_lease
from agent.database.postgres_db import PostgresDB as AgentPostgresDB
from orchestrator.database.postgres import PostgresDB
from orchestrator.services.stateless_input_admission import (
    StatelessInputDependencies,
    admit_stateless_input,
)
from shared.event_journal import append_system_frame
from shared.persistent_input_delivery import (
    claim_stateless_input_delivery,
    transition_stateless_input_delivery,
)
from shared.run_queue import (
    UNIT_KIND_SESSION_TURN,
    claim_unit,
    close_interrupt_admission,
    complete_unit,
    open_interrupt_admission,
    reap_expired,
)

POD = "stateless-executor-a"
POD_UID = "7d0c2c55-7a4e-4b4e-9f55-0a1f7c3e9a01"

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:
        pytest.skip(f"local PostgreSQL container unavailable: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA_FILE.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def store(pg_dsn, _schema_applied):
    db = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=8)
    await db.connect()
    async with db.acquire() as conn:
        await conn.execute(
            "TRUNCATE run_queue, completion_effects, thread_events, "
            "thread_input_deliveries, thread_messages, threads, users CASCADE"
        )
    try:
        yield db
    finally:
        await db.close()


@pytest_asyncio.fixture
async def agent_db(pg_dsn, _schema_applied):
    db = AgentPostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=8)
    await db.connect()
    try:
        yield db
    finally:
        await db.close()


@pytest.fixture
def executor(monkeypatch, agent_db):
    ex = te.StatelessTurnExecutor(pod_name=POD, pod_uid=POD_UID)
    monkeypatch.setattr(
        te.StatelessTurnExecutor, "_db", property(lambda self: agent_db)
    )
    runtime = MagicMock()
    runtime._session = MagicMock(stateless_warm_reuse_safe=True)
    monkeypatch.setattr(te, "_pa", lambda: runtime)
    monkeypatch.setattr(ex, "_ack_terminal_claim_loss", AsyncMock(return_value=True))
    token = current_lease.set(ex._lease)
    try:
        yield ex
    finally:
        current_lease.reset(token)


async def _seed_thread(store: PostgresDB) -> UUID:
    user_id, thread_id = uuid4(), uuid4()
    async with store.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, display_name, email) "
            "VALUES ($1, 'queued input owner', $2)",
            user_id,
            f"{user_id}@example.test",
        )
        await conn.execute(
            "INSERT INTO threads "
            "(id, user_id, status, execution_lane, config_name, metadata) "
            "VALUES ($1, $2, 'active', 'stateless', 'default', $3::jsonb)",
            thread_id,
            user_id,
            json.dumps({"config_override": {"workspace": {"backend": "virtual"}}}),
        )
    return thread_id


async def _admit(store: PostgresDB, thread_id: UUID, content: str) -> dict:
    """Owner input through the real stateless admission transaction."""

    async with store.acquire() as conn:
        thread = dict(
            await conn.fetchrow("SELECT * FROM threads WHERE id = $1", thread_id)
        )
    return await admit_stateless_input(
        thread,
        content,
        dependencies=StatelessInputDependencies(
            store=store, schedule_stateless_workspace_ensure=lambda _thread: None
        ),
    )


async def _queue(store: PostgresDB, thread_id: UUID) -> dict:
    async with store.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state, lease_token, input_seq, consumed_seq, "
            "interrupt_admission_lease_token FROM run_queue WHERE unit_id = $1",
            thread_id,
        )
    return dict(row)


async def _humans(store: PostgresDB, thread_id: UUID) -> list[dict]:
    async with store.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, seq, content, turn_number, turn_execution_id "
            "FROM thread_messages WHERE thread_id = $1 AND role = 'human' "
            "AND rewound_at IS NULL ORDER BY seq",
            thread_id,
        )
    return [dict(row) for row in rows]


async def _answers(store: PostgresDB, thread_id: UUID) -> list[tuple[int, str]]:
    async with store.acquire() as conn:
        rows = await conn.fetch(
            "SELECT turn_number, content FROM thread_messages "
            "WHERE thread_id = $1 AND role = 'ai' AND rewound_at IS NULL "
            "ORDER BY seq",
            thread_id,
        )
    return [(row["turn_number"], row["content"]) for row in rows]


async def _expire_and_reap(store: PostgresDB, thread_id: UUID) -> None:
    """The claimant died: its heartbeats stop, the reaper steals the lease."""

    async with store.acquire() as conn:
        await conn.execute(
            "UPDATE run_queue SET leased_until = now() - interval '1 hour' "
            "WHERE unit_id = $1 AND state = 'leased'",
            thread_id,
        )
        stolen = await reap_expired(
            conn,
            unit_kind=UNIT_KIND_SESSION_TURN,
            grace_seconds=0.0,
            backoff_base_seconds=0.0,
            jitter=0.0,
        )
    assert [unit.unit_id for unit in stolen] == [thread_id]


@dataclass
class _Turns:
    """The turn body the executor runs for every claim it does not skip.

    ``during`` runs while a turn is open (after its interrupt window opens,
    before its answer lands). ``dies`` names inputs whose claimant dies at a
    boundary: ``"after_answer"`` — the authoritative final reconcile committed,
    the watermark checkpoint never ran; ``"after_incremental_answer"`` — the
    loop's incremental writer persisted the final answer, then settlement died
    before the reconcile; ``"after_tool_call_answer"`` — the turn's last
    assistant message carried its answer text and a tool call, the reconcile
    committed and ``turn.completed`` was published, the checkpoint never ran;
    ``"mid_turn"`` — a tool call and its result landed, no answer.
    """

    store: PostgresDB
    agent_db: AgentPostgresDB
    thread_id: UUID
    during: dict[str, Callable[[], Awaitable[Any]]] = field(default_factory=dict)
    dies: dict[str, str] = field(default_factory=dict)
    executed: list[tuple[int, int, str]] = field(default_factory=list)

    async def serve(self, pa, claim, unit_id, token, claim_lost) -> None:
        del pa, claim_lost
        pending = await self._fetch_pending(claim)
        target = pending[0]
        content, turn = target["content"], int(target["turn_number"])
        self.executed.append((int(target["seq"]), turn, content))
        async with self.store.acquire() as conn:
            assert await open_interrupt_admission(
                conn, unit_id=unit_id, lease_token=token, turn_id=turn
            )
        delivery = None
        if target["delivery_id"] is not None:
            delivery = await self._deliver(unit_id, token, target, turn)
        if content in self.during:
            await self.during.pop(content)()
        death = self.dies.pop(content, None)
        if death == "mid_turn":
            call_id = f"call_{uuid4().hex[:12]}"
            await self.agent_db.save_thread_message(
                str(self.thread_id),
                "ai",
                content="Checking.",
                tool_calls=[{"id": call_id, "name": "read_file", "args": {}}],
                turn_number=turn,
                id=f"ai-{uuid4()}",
            )
            await self.agent_db.save_thread_message(
                str(self.thread_id),
                "tool",
                content="file body",
                tool_call_id=call_id,
                turn_number=turn,
                id=f"tool-{uuid4()}",
            )
            return
        if death == "after_incremental_answer":
            await self.agent_db.save_thread_message(
                str(self.thread_id),
                "ai",
                content=f"answer to {content}",
                turn_number=turn,
                id=f"ai-{uuid4()}",
            )
            return
        rows = [
            {
                "id": f"ai-{uuid4()}",
                "role": "ai",
                "content": f"answer to {content}",
                "turn_number": turn,
            }
        ]
        if death == "after_tool_call_answer":
            call_id = f"call_{uuid4().hex[:12]}"
            rows[0]["tool_calls"] = [
                {"id": call_id, "name": "write_file", "args": {"path": "a.md"}}
            ]
            rows.append(
                {
                    "id": f"tool-{uuid4()}",
                    "role": "tool",
                    "content": "written",
                    "tool_call_id": call_id,
                    "turn_number": turn,
                }
            )
        await self.agent_db.save_thread_messages(
            str(self.thread_id),
            rows,
            turn_input_message_id=target["id"],
            turn_number=turn,
            memory_scope_kind="thread",
            memory_scope_id=str(self.thread_id),
        )
        if death == "after_tool_call_answer":
            async with self.store.acquire() as conn:
                assert await append_system_frame(
                    conn,
                    thread_id=str(self.thread_id),
                    kind="turn.completed",
                    payload={"turn_id": turn, "metrics": {}},
                )
        if death in {"after_answer", "after_tool_call_answer"}:
            return
        if delivery is not None:
            await self._transition(unit_id, token, delivery, "settled")
        completed = max(
            int(target["seq"]),
            int(claim.consumed_seq) if claim.consumed_seq is not None else -1,
        )
        async with self.store.acquire() as conn:
            assert await close_interrupt_admission(
                conn,
                unit_id=unit_id,
                lease_token=token,
                turn_id=turn,
                completed_input_seq=completed,
            )
            assert await complete_unit(
                conn, unit_id=unit_id, lease_token=token, consumed_seq=completed
            ) in {"done", "queued"}

    async def _deliver(self, unit_id, token, target, turn) -> dict:
        """The executor claims a durable event delivery before injecting it;
        the loop's admit callback records the admitted turn."""

        async with self.store.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "UPDATE threads SET metadata = jsonb_set("
                    "COALESCE(metadata, '{}'::jsonb), '{_stateless_active_claim}', "
                    "jsonb_build_object('lease_token', $2::bigint, 'pod', $3::text, "
                    "'pod_uid', $4::text), true) WHERE id = $1",
                    UUID(str(unit_id)),
                    int(token),
                    POD,
                    POD_UID,
                )
                claimed = await claim_stateless_input_delivery(
                    conn,
                    thread_id=str(unit_id),
                    delivery_id=str(target["delivery_id"]),
                    lease_token=token,
                    executor_id=POD,
                    pod_uid=POD_UID,
                )
        assert claimed is not None and int(claimed["seq"]) == int(target["seq"])
        await self._transition(unit_id, token, claimed, "admitted", turn_number=turn)
        return claimed

    async def _transition(self, unit_id, token, delivery, transition, **extra):
        async with self.store.acquire() as conn:
            async with conn.transaction():
                assert await transition_stateless_input_delivery(
                    conn,
                    thread_id=str(unit_id),
                    delivery_id=str(delivery["delivery_id"]),
                    lease_token=token,
                    executor_id=POD,
                    pod_uid=POD_UID,
                    claim_generation=delivery["claim_generation"],
                    transition=transition,
                    **extra,
                )

    async def _fetch_pending(self, claim) -> list[dict]:
        rows = await self.agent_db.fetch(
            te._PENDING_INPUT_SQL,
            self.thread_id,
            claim.consumed_seq if claim.consumed_seq is not None else -1,
            te.PENDING_ROWS_LIMIT,
        )
        return [dict(row) for row in rows]


async def _drive(
    executor: te.StatelessTurnExecutor,
    turns: _Turns,
    *,
    max_claims: int = 12,
) -> list[dict]:
    """Claim and serve until the unit is not claimable; record each decision."""

    executor._serve_claim_inner = turns.serve
    decisions: list[dict] = []
    for _ in range(max_claims):
        async with turns.store.acquire() as conn:
            claim = await claim_unit(
                conn,
                unit_kind=UNIT_KIND_SESSION_TURN,
                pod_name=POD,
                affinity_grace_seconds=0.0,
            )
        if claim is None:
            return decisions
        executor._activate_lease(claim.unit_id, claim.lease_token)
        ran_before = len(turns.executed)
        await executor._serve_claim(claim)
        queue = await _queue(turns.store, turns.thread_id)
        decisions.append(
            {
                "token": claim.lease_token,
                "claimed_consumed_seq": claim.consumed_seq,
                "claimed_input_seq": claim.input_seq,
                "executed": turns.executed[ran_before:],
                "state": queue["state"],
                "consumed_seq": queue["consumed_seq"],
            }
        )
        if queue["state"] == "leased":
            return decisions
    raise AssertionError(f"unit did not settle within {max_claims} claims")


def _assert_watermarks_only_cover_consumed_work(
    decisions: list[dict], humans: list[dict], answered: set[int]
) -> None:
    """Every checkpoint lands on an input that ran or was already answered,
    and never passes over an input that is neither."""

    for decision in decisions:
        consumed = decision["consumed_seq"]
        if consumed is None:
            continue
        for human in humans:
            if human["seq"] <= consumed:
                assert human["seq"] in answered, (human, decision)


@pytest.mark.asyncio
async def test_input_admitted_while_a_turn_runs_is_executed_after_it(
    store, agent_db, executor
):
    thread_id = await _seed_thread(store)
    turns = _Turns(store, agent_db, thread_id)
    a = await _admit(store, thread_id, "input A")
    turns.during["input A"] = lambda: _admit(store, thread_id, "input B")

    decisions = await _drive(executor, turns)

    humans = await _humans(store, thread_id)
    assert [(h["content"], h["turn_number"]) for h in humans] == [
        ("input A", 1),
        ("input B", 2),
    ]
    # A's answer landed after B's row: the position the old check mistook.
    async with store.acquire() as conn:
        a_answer_seq = await conn.fetchval(
            "SELECT seq FROM thread_messages WHERE thread_id = $1 "
            "AND role = 'ai' AND turn_number = 1",
            thread_id,
        )
    assert a_answer_seq > humans[1]["seq"]
    assert a["turn_id"] == 1
    assert [row[2] for row in turns.executed] == ["input A", "input B"]
    assert await _answers(store, thread_id) == [
        (1, "answer to input A"),
        (2, "answer to input B"),
    ]
    assert all(h["turn_execution_id"] is not None for h in humans)
    queue = await _queue(store, thread_id)
    assert queue["state"] == "done"
    assert queue["consumed_seq"] == queue["input_seq"] == humans[1]["seq"]
    _assert_watermarks_only_cover_consumed_work(
        decisions, humans, {h["seq"] for h in humans}
    )


@pytest.mark.asyncio
async def test_several_queued_inputs_run_in_order_once_each(store, agent_db, executor):
    thread_id = await _seed_thread(store)
    turns = _Turns(store, agent_db, thread_id)
    await _admit(store, thread_id, "input A")

    async def queue_three() -> None:
        for name in ("input B", "input C", "input D"):
            await _admit(store, thread_id, name)

    turns.during["input A"] = queue_three
    # One more arrives while B runs.
    turns.during["input B"] = lambda: _admit(store, thread_id, "input E")

    decisions = await _drive(executor, turns)

    names = ["input A", "input B", "input C", "input D", "input E"]
    humans = await _humans(store, thread_id)
    assert [h["content"] for h in humans] == names
    assert [row[2] for row in turns.executed] == names
    assert [row[1] for row in turns.executed] == [1, 2, 3, 4, 5]
    assert await _answers(store, thread_id) == [
        (n, f"answer to {name}") for n, name in enumerate(names, start=1)
    ]
    queue = await _queue(store, thread_id)
    assert queue["state"] == "done"
    assert queue["consumed_seq"] == queue["input_seq"] == humans[-1]["seq"]
    # Each claim advanced the watermark by exactly the input it ran.
    assert [d["consumed_seq"] for d in decisions] == [h["seq"] for h in humans]
    _assert_watermarks_only_cover_consumed_work(
        decisions, humans, {h["seq"] for h in humans}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "death", ["after_answer", "after_incremental_answer", "after_tool_call_answer"]
)
@pytest.mark.parametrize("queued_behind", [False, True])
async def test_an_answered_input_is_not_run_again_after_its_claimant_dies(
    store, agent_db, executor, death, queued_behind
):
    """The answer is durable, the watermark checkpoint never ran, the claimant
    died. The successor must not answer A again, and must still run B."""

    thread_id = await _seed_thread(store)
    turns = _Turns(store, agent_db, thread_id, dies={"input A": death})
    await _admit(store, thread_id, "input A")
    if queued_behind:
        turns.during["input A"] = lambda: _admit(store, thread_id, "input B")

    first = await _drive(executor, turns)
    assert first[-1]["state"] == "leased"
    await _expire_and_reap(store, thread_id)
    decisions = first + await _drive(executor, turns)

    humans = await _humans(store, thread_id)
    expected = ["input A", "input B"] if queued_behind else ["input A"]
    assert [row[2] for row in turns.executed] == expected
    assert [turn for turn, _ in await _answers(store, thread_id)] == list(
        range(1, len(expected) + 1)
    )
    queue = await _queue(store, thread_id)
    assert queue["state"] == "done"
    assert queue["consumed_seq"] == queue["input_seq"] == humans[-1]["seq"]
    _assert_watermarks_only_cover_consumed_work(
        decisions, humans, {h["seq"] for h in humans}
    )


@pytest.mark.asyncio
async def test_a_turn_that_died_mid_turn_runs_again_and_nothing_is_skipped(
    store, agent_db, executor
):
    """A partial turn (a tool call and its result, no answer) is not an
    answer: the successor runs the input again, then the queued one."""

    thread_id = await _seed_thread(store)
    turns = _Turns(store, agent_db, thread_id, dies={"input A": "mid_turn"})
    await _admit(store, thread_id, "input A")
    turns.during["input A"] = lambda: _admit(store, thread_id, "input B")

    first = await _drive(executor, turns)
    assert first[-1]["state"] == "leased"
    await _expire_and_reap(store, thread_id)
    await _drive(executor, turns)

    assert [row[2] for row in turns.executed] == ["input A", "input A", "input B"]
    assert await _answers(store, thread_id) == [
        (1, "Checking."),
        (1, "answer to input A"),
        (2, "answer to input B"),
    ]
    queue = await _queue(store, thread_id)
    assert queue["state"] == "done"
    assert queue["consumed_seq"] == queue["input_seq"]


@pytest.mark.asyncio
async def test_unrelated_and_rewound_answers_never_consume_an_input(
    store, agent_db, executor
):
    """An answer of another turn and a rewound answer of the input's own turn
    number, both positioned after the pending input, prove nothing."""

    thread_id = await _seed_thread(store)
    turns = _Turns(store, agent_db, thread_id, dies={"input B": "mid_turn"})
    await _admit(store, thread_id, "input A")
    turns.during["input A"] = lambda: _admit(store, thread_id, "input B")
    await _drive(executor, turns)
    await _expire_and_reap(store, thread_id)
    humans = await _humans(store, thread_id)
    b = humans[1]
    async with store.acquire() as conn:
        # A tombstoned final answer carrying B's turn number (a rewound
        # timeline's row), positioned after B.
        await conn.execute(
            "INSERT INTO thread_messages "
            "(id, thread_id, role, content, turn_number, rewound_at) "
            "VALUES ($1, $2, 'ai', 'rewound answer', $3, now())",
            uuid4(),
            thread_id,
            b["turn_number"],
        )
    await _drive(executor, turns)

    assert [row[2] for row in turns.executed] == ["input A", "input B", "input B"]
    assert await _answers(store, thread_id) == [
        (1, "answer to input A"),
        (2, "Checking."),
        (2, "answer to input B"),
    ]
    queue = await _queue(store, thread_id)
    assert queue["state"] == "done"
    assert queue["consumed_seq"] == queue["input_seq"] == b["seq"]


@pytest.mark.asyncio
async def test_a_stale_claimant_cannot_answer_or_settle_after_the_steal(
    store, agent_db, executor
):
    thread_id = await _seed_thread(store)
    turns = _Turns(store, agent_db, thread_id, dies={"input A": "mid_turn"})
    await _admit(store, thread_id, "input A")
    first = await _drive(executor, turns)
    stale_token = first[-1]["token"]
    await _expire_and_reap(store, thread_id)
    target = (await _humans(store, thread_id))[0]

    # The old claimant wakes up after the steal: every fenced write is refused.
    executor._activate_lease(thread_id, stale_token)
    with pytest.raises(LeaseLostError):
        await agent_db.save_thread_messages(
            str(thread_id),
            [
                {
                    "id": f"ai-{uuid4()}",
                    "role": "ai",
                    "content": "late",
                    "turn_number": 1,
                }
            ],
            turn_input_message_id=str(target["id"]),
            turn_number=1,
            memory_scope_kind="thread",
            memory_scope_id=str(thread_id),
        )
    async with store.acquire() as conn:
        assert not await close_interrupt_admission(
            conn,
            unit_id=thread_id,
            lease_token=stale_token,
            turn_id=1,
            completed_input_seq=target["seq"],
        )
        assert (
            await complete_unit(
                conn,
                unit_id=thread_id,
                lease_token=stale_token,
                consumed_seq=target["seq"],
            )
            is None
        )
    assert (await _humans(store, thread_id))[0]["turn_execution_id"] is None

    await _drive(executor, turns)
    assert [row[2] for row in turns.executed] == ["input A", "input A"]
    assert await _answers(store, thread_id) == [
        (1, "Checking."),
        (1, "answer to input A"),
    ]
    queue = await _queue(store, thread_id)
    assert queue["state"] == "done"
    assert queue["consumed_seq"] == queue["input_seq"] == target["seq"]


@pytest.mark.asyncio
async def test_a_duplicate_event_delivery_runs_once_behind_queued_human_input(
    store, agent_db, executor
):
    """A stable event delivery retried while a human turn runs is admitted
    once and runs once, after the human input queued ahead of it."""

    thread_id = await _seed_thread(store)
    turns = _Turns(store, agent_db, thread_id)
    delivery_id = str(uuid4())
    receipts: list[dict] = []

    async def queue_input_and_event() -> None:
        await _admit(store, thread_id, "input B")
        for _ in range(2):
            receipts.append(
                await store.persist_thread_input_delivery(
                    thread_id=str(thread_id),
                    delivery_id=delivery_id,
                    role="event",
                    content="[wake] job finished",
                    source="officer_wake",
                )
            )

    await _admit(store, thread_id, "input A")
    turns.during["input A"] = queue_input_and_event

    await _drive(executor, turns)

    assert [r["transcript_inserted"] for r in receipts] == [True, False]
    assert receipts[0]["message_id"] == receipts[1]["message_id"]
    assert [row[2] for row in turns.executed] == [
        "input A",
        "input B",
        "[wake] job finished",
    ]
    async with store.acquire() as conn:
        delivery = await conn.fetchrow(
            "SELECT state, admitted_turn_number FROM thread_input_deliveries "
            "WHERE delivery_id = $1",
            UUID(delivery_id),
        )
    assert dict(delivery) == {"state": "settled", "admitted_turn_number": 3}
    queue = await _queue(store, thread_id)
    assert queue["state"] == "done"
    assert queue["consumed_seq"] == queue["input_seq"] == receipts[0]["seq"]
