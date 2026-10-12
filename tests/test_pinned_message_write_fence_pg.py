"""Pinned transcript writes are fenced on the runtime life (parallel_subagents P2).

``save_thread_message`` and ``save_thread_messages`` used to write unfenced on
the pinned lane, so a replaced pinned runtime could add rows after its
successor settled the turn (a late live tool result gets a random id, so it
duplicates a settled result). Both writers now lock the thread row on the armed
life (agent, runtime generation, attach token, live status), then the agent
row, then write, in one transaction, the pinned event journal's lock order.
An open retirement does not refuse: until the settle nulls the identity the
life still owns its turn (a forced End reaches it up to a heartbeat later).
The life comes from ``SessionIdentityRuntime`` through
``agent.api.pinned_write_fence``. Real Postgres with the full migration chain;
the last section drives the session loop's own writers.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import asyncpg
import orchestrator
import pytest
import pytest_asyncio
from langchain_core.messages import AIMessage
from testcontainers.postgres import PostgresContainer

from agent.api.lease_context import LeaseHandle, LeaseLostError, current_lease
from agent.api.pinned_write_fence import (
    PROCESS_PINNED_WRITE_FENCE,
    PinnedWriteFence,
    PinnedWriteIdentity,
    PinnedWriteRefused,
    current_pinned_write_identity,
)
from agent.api.session_identity import SessionIdentityPorts, SessionIdentityRuntime
from agent.database.postgres_db import PostgresDB as AgentDB
from agent.persistent_graph import DelegationResultNotDurable
from orchestrator.database.migrate import run_migrations
from orchestrator.database.postgres import PostgresDB
from orchestrator.services.session_attach_binding import (
    bind_registered_persistent_agent,
)
from shared.run_queue import UNIT_KIND_SESSION_TURN, claim_unit, enqueue_unit

ROOT = Path(orchestrator.__file__).resolve().parents[2]
MIGRATIONS = ROOT / "src/orchestrator/database/migrations/app"


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:16") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture(scope="module")
async def migrated_dsn(pg_dsn):
    async with asyncpg.create_pool(pg_dsn, min_size=1, max_size=2) as pool:
        await run_migrations(pool, MIGRATIONS)
    return pg_dsn


async def _new_agent(db: PostgresDB, thread_id: UUID, generation: str) -> str:
    """Register an agent in the thread's pod, publishing the pod if needed.

    The second call for one generation models a re-registration: the same
    published pod gets a new agent id (a heartbeat 404 re-registers).
    """

    agent_id = uuid4()
    pod_uid, pod_name, attempt = f"pod-{uuid4()}", f"pinned-{uuid4()}", str(uuid4())
    async with db.acquire() as conn:
        published = await conn.fetchval(
            "SELECT metadata->'agent_pod' FROM threads WHERE id=$1", thread_id
        )
    if published:
        published = json.loads(published) if isinstance(published, str) else published
        pod_uid, pod_name = published["pod_uid"], published["pod_name"]
    else:
        assert await db.reserve_pinned_agent_pod_provision_intent(
            str(thread_id),
            expected_runtime_generation=generation,
            attempt_id=attempt,
            pod_name=pod_name,
            provisioner="agent",
            namespace="test",
        )
        assert await db.publish_pinned_agent_pod_provision_intent(
            str(thread_id),
            expected_runtime_generation=generation,
            attempt_id=attempt,
            pod_name=pod_name,
            pod_uid=pod_uid,
            namespace="test",
        )
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO agents (id, config_name, status, metadata, hostname, pod_uid) "
            "VALUES ($1, 'session_base', 'session', '{}'::jsonb, $2, $3)",
            agent_id,
            pod_name,
            pod_uid,
        )
    return str(agent_id)


@pytest_asyncio.fixture
async def life(migrated_dsn):
    """One pinned session bound to life A (agent A, generation G, token T_A)."""

    db = PostgresDB(migrated_dsn, min_connections=1, max_connections=8)
    await db.connect()
    agent_db = AgentDB.__new__(AgentDB)
    agent_db._pool = db._pool
    agent_db._queries = {}
    owner, thread_id = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, display_name) VALUES ($1, 'owner')", owner
        )
        generation = str(
            await conn.fetchval(
                "INSERT INTO threads (id, user_id, title, status, metadata) "
                "VALUES ($1, $2, 'P2 fence', 'active', $3::jsonb) "
                "RETURNING runtime_generation",
                thread_id,
                owner,
                # The pinned lite tier: no workspace Pod, so the retiring
                # runtime's own quiescence receipt is all a settle needs.
                json.dumps({"config_override": {"workspace": {"backend": "none"}}}),
            )
        )
    agent_id = await _new_agent(db, thread_id, generation)
    attach_token = await bind_registered_persistent_agent(
        str(thread_id),
        agent_id,
        None,
        generation,
        dependencies=SimpleNamespace(store=db),
    )
    assert attach_token
    try:
        yield SimpleNamespace(
            db=db,
            agent_db=agent_db,
            owner=owner,
            thread_id=str(thread_id),
            agent_id=agent_id,
            generation=generation,
            attach_token=attach_token,
        )
    finally:
        PROCESS_PINNED_WRITE_FENCE.reset()
        await db.close()


def _identity_runtime(agent_id: str, *, stateless: bool = False):
    """A session identity owner wired to the process fence, as the app is."""

    return SessionIdentityRuntime(
        SessionIdentityPorts(
            agent_id=lambda: agent_id,
            pod_uid=lambda: "pod",
            lease=lambda: None,
            stateless_mode=lambda: stateless,
            orchestrator_client=lambda: None,
            identity_replaced=lambda: None,
            write_fence=PROCESS_PINNED_WRITE_FENCE,
        )
    )


def _adopt(agent_id: str, generation: str | None, token: str | None):
    runtime = _identity_runtime(agent_id)
    runtime.adopt(generation, token, contract_advertised=generation is not None)
    return runtime


async def _rows(life) -> list[str]:
    async with life.db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT content FROM thread_messages WHERE thread_id=$1::uuid ORDER BY seq",
            life.thread_id,
        )
    return [row["content"] for row in rows]


async def _total_turns(life) -> int:
    async with life.db.acquire() as conn:
        return await conn.fetchval(
            "SELECT total_turns FROM threads WHERE id=$1::uuid", life.thread_id
        )


async def _single(life, content: str, *, turn: int = 1):
    return await life.agent_db.save_thread_message(
        life.thread_id,
        role="tool",
        content=content,
        tool_call_id="call-1",
        turn_number=turn,
        # A late live result carries a random id: it would land as a new row.
        id=f"msg_{uuid4()}",
    )


async def _batch(life, content: str, *, turn: int = 1):
    return await life.agent_db.save_thread_messages(
        life.thread_id,
        [
            {
                "id": f"msg_{uuid4()}",
                "role": "ai",
                "content": content,
                "turn_number": turn,
            }
        ],
    )


async def _assert_refused(life, label: str) -> None:
    before_rows, before_turns = await _rows(life), await _total_turns(life)
    with pytest.raises(PinnedWriteRefused):
        await _single(life, f"{label} single", turn=99)
    with pytest.raises(PinnedWriteRefused):
        await _batch(life, f"{label} batch", turn=99)
    # Nothing landed: no row and no activity bump.
    assert await _rows(life) == before_rows
    assert await _total_turns(life) == before_turns


async def _rebind(life, agent_id: str) -> str:
    """Rebind the thread to a successor life in the same generation."""

    token = await bind_registered_persistent_agent(
        life.thread_id,
        agent_id,
        life.agent_id,
        life.generation,
        dependencies=SimpleNamespace(store=life.db),
    )
    assert token and token != life.attach_token
    return token


@pytest.mark.asyncio
async def test_bound_life_writes_single_and_batch(life):
    _adopt(life.agent_id, life.generation, life.attach_token)
    assert current_pinned_write_identity() == PinnedWriteIdentity(
        agent_id=life.agent_id,
        runtime_generation=life.generation,
        attach_token=life.attach_token,
    )

    saved = await _single(life, "A single", turn=3)
    await _batch(life, "A batch", turn=4)

    assert saved["seq"] is not None
    assert await _rows(life) == ["A single", "A batch"]
    assert await _total_turns(life) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("successor", ["same_agent_reattach", "new_agent"])
async def test_rebound_life_writes_nothing_and_successor_writes(life, successor):
    _adopt(life.agent_id, life.generation, life.attach_token)
    await _single(life, "A before rebind")

    agent_b = (
        life.agent_id
        if successor == "same_agent_reattach"
        else await _new_agent(life.db, UUID(life.thread_id), life.generation)
    )
    token_b = await _rebind(life, agent_b)

    # Life A's runtime still holds A: its late writes are refused.
    await _assert_refused(life, "A after rebind")

    # Life B adopts its own identity; its writes land.
    _adopt(agent_b, life.generation, token_b)
    await _single(life, "B single", turn=5)
    await _batch(life, "B batch", turn=5)
    assert await _rows(life) == ["A before rebind", "B single", "B batch"]


async def _begin_end(life) -> dict:
    """Begin a forced (owner) End: the retirement token closes admission."""

    authority = await life.db.begin_pinned_thread_retirement(
        life.thread_id, permanent=False
    )
    assert authority["state"] == "pending"
    return authority


async def _authorize_end(life, authority: dict) -> None:
    assert await life.db.authorize_pinned_thread_retirement(
        life.thread_id,
        token=authority["token"],
        generation=authority["generation"],
        settle_status="ended",
    )


async def _acknowledge_quiescence(life, authority: dict) -> None:
    """The retiring runtime's own local-quiescence receipt (agent-only tier)."""

    context = authority["context"]
    protocol = {
        "virtual": "agent_runtime_zero_v1",
        "none": "agent_runtime_zero_v1",
    }[str(context.get("workspace_backend") or "")]
    assert await life.db.acknowledge_pinned_thread_local_quiescence(
        life.thread_id,
        expected_runtime_generation=authority["generation"],
        expected_retirement_token=authority["token"],
        expected_agent_id=life.agent_id,
        expected_attach_token=life.attach_token,
        expected_settle_status="ended",
        expected_quiescence_protocol=protocol,
        expected_workspace_generation=None,
        expected_workspace_runtime_incarnation=None,
    )


async def _settle_end(life, authority: dict) -> bool:
    return await life.db.settle_pinned_thread_retirement(
        life.thread_id,
        token=authority["token"],
        generation=authority["generation"],
        final_status="ended",
    )


async def _identity_row(life):
    async with life.db.acquire() as conn:
        return await conn.fetchrow(
            "SELECT status::text AS status, agent_id, runtime_attach_token, "
            "runtime_retirement_token FROM threads WHERE id=$1::uuid",
            life.thread_id,
        )


@pytest.mark.asyncio
async def test_open_retirement_keeps_the_life_s_writes(life):
    """Begin and authorize leave the identity in place: the turn keeps its rows.

    A forced End reaches the agent up to a heartbeat later; the stream or tool
    that finishes meanwhile must not lose its rows (restore would strip a tool
    call whose side effect happened).
    """

    _adopt(life.agent_id, life.generation, life.attach_token)
    await _batch(life, "A before End")

    authority = await _begin_end(life)
    row = await _identity_row(life)
    assert str(row["agent_id"]) == life.agent_id
    assert str(row["runtime_attach_token"]) == life.attach_token
    assert row["runtime_retirement_token"] is not None
    await _single(life, "A after Begin single", turn=2)
    await _batch(life, "A after Begin batch", turn=2)

    await _authorize_end(life, authority)
    await _single(life, "A after authorize", turn=3)

    assert await _rows(life) == [
        "A before End",
        "A after Begin single",
        "A after Begin batch",
        "A after authorize",
    ]
    assert await _total_turns(life) == 3


@pytest.mark.asyncio
async def test_settled_retirement_refuses_every_write(life):
    """The settle nulls the identity; from then on the old life writes nothing."""

    runtime = _adopt(life.agent_id, life.generation, life.attach_token)
    authority = await _begin_end(life)
    await _authorize_end(life, authority)
    await _acknowledge_quiescence(life, authority)
    await _single(life, "A before the settle")
    assert await _settle_end(life, authority)

    row = await _identity_row(life)
    assert row["status"] == "ended"
    assert row["agent_id"] is None
    assert row["runtime_attach_token"] is None
    assert row["runtime_retirement_token"] is None
    await _assert_refused(life, "A after the settle")

    # End's last local step clears the identity; the dead life stays fenced.
    assert runtime.clear(
        expected_generation=life.generation,
        expected_attach_token=life.attach_token,
    )
    await _assert_refused(life, "A after clear")
    assert await _rows(life) == ["A before the settle"]


@pytest.mark.asyncio
async def test_cleared_life_stays_fenced(life):
    """Clearing the identity (End's last step) keeps the dead life's fence."""

    runtime = _adopt(life.agent_id, life.generation, life.attach_token)
    await _rebind(life, life.agent_id)
    assert runtime.clear(
        expected_generation=life.generation,
        expected_attach_token=life.attach_token,
    )

    assert current_pinned_write_identity() is not None
    await _assert_refused(life, "A after clear")


@pytest.mark.asyncio
async def test_stale_generation_is_refused(life):
    PROCESS_PINNED_WRITE_FENCE.arm(
        PinnedWriteIdentity(
            agent_id=life.agent_id,
            runtime_generation=str(uuid4()),
            attach_token=life.attach_token,
        )
    )
    await _assert_refused(life, "stale generation")


@pytest.mark.asyncio
async def test_stale_attach_token_is_refused(life):
    PROCESS_PINNED_WRITE_FENCE.arm(
        PinnedWriteIdentity(
            agent_id=life.agent_id,
            runtime_generation=life.generation,
            attach_token=str(uuid4()),
        )
    )
    await _assert_refused(life, "stale attach token")


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["agent_id", "attach_token"])
async def test_incomplete_identity_is_refused_like_the_event_journal(life, missing):
    values = {
        "agent_id": life.agent_id,
        "runtime_generation": life.generation,
        "attach_token": life.attach_token,
    }
    values[missing] = None
    PROCESS_PINNED_WRITE_FENCE.arm(PinnedWriteIdentity(**values))
    await _assert_refused(life, f"no {missing}")


@pytest.mark.asyncio
async def test_agent_row_unpaired_is_refused(life):
    _adopt(life.agent_id, life.generation, life.attach_token)
    async with life.db.acquire() as conn:
        async with conn.transaction():
            # Model a reciprocal agent row that no longer names the thread.
            await conn.execute("SET LOCAL session_replication_role = 'replica'")
            await conn.execute(
                "UPDATE agents SET thread_id=NULL WHERE id=$1::uuid", life.agent_id
            )
    await _assert_refused(life, "agent unpaired")


@pytest.mark.asyncio
async def test_no_generation_keeps_the_rolling_deploy_exception(life):
    """An orchestrator that advertises no generation leaves writes unfenced."""

    _adopt(life.agent_id, None, None)
    assert current_pinned_write_identity() is None
    await _rebind(life, life.agent_id)

    await _single(life, "legacy single")
    await _batch(life, "legacy batch")
    assert await _rows(life) == ["legacy single", "legacy batch"]


@pytest.mark.asyncio
async def test_stateless_writes_use_the_lease_not_the_pinned_fence(life):
    async with life.db.acquire() as conn:
        thread_id = await conn.fetchval(
            "INSERT INTO threads (id, user_id, title, status, execution_lane) "
            "VALUES ($1, $2, 'stateless', 'active', 'stateless') RETURNING id",
            uuid4(),
            life.owner,
        )
        await enqueue_unit(conn, unit_id=thread_id, unit_kind=UNIT_KIND_SESSION_TURN)
        claim = await claim_unit(
            conn,
            unit_kind=UNIT_KIND_SESSION_TURN,
            pod_name="executor",
            prefer_unit_id=thread_id,
        )
    assert claim is not None and str(claim.unit_id) == str(thread_id)
    # A pinned identity this thread does not have: a write that consulted it
    # would be refused.
    PROCESS_PINNED_WRITE_FENCE.arm(
        PinnedWriteIdentity(
            agent_id=life.agent_id,
            runtime_generation=life.generation,
            attach_token=life.attach_token,
        )
    )
    lease = LeaseHandle()
    lease.update(str(thread_id), claim.lease_token)
    context_token = current_lease.set(lease)
    try:
        await life.agent_db.save_thread_message(
            str(thread_id), role="ai", content="stateless single", turn_number=1
        )
        await life.agent_db.save_thread_messages(
            str(thread_id),
            [{"id": "msg_stateless", "role": "ai", "content": "stateless batch"}],
        )
        lease.update(str(thread_id), claim.lease_token + 1)
        with pytest.raises(LeaseLostError) as lost:
            await life.agent_db.save_thread_message(
                str(thread_id), role="ai", content="stale lease", turn_number=1
            )
        assert not isinstance(lost.value, PinnedWriteRefused)
    finally:
        current_lease.reset(context_token)
    async with life.db.acquire() as conn:
        contents = await conn.fetch(
            "SELECT content FROM thread_messages WHERE thread_id=$1 ORDER BY seq",
            thread_id,
        )
    assert [row["content"] for row in contents] == [
        "stateless single",
        "stateless batch",
    ]


# --- Concurrency with End --------------------------------------------------

_GATE_KEY = 742_014_202
_GATE_FUNCTION = "test_gate_pinned_message_fence"


@asynccontextmanager
async def _gate(db: PostgresDB, trigger: str):
    """Hold the first row write matching ``trigger`` inside its transaction.

    ``trigger`` is the ``BEFORE ... ON ... FOR EACH ROW [WHEN ...]`` clause;
    the held statement keeps every lock its transaction already took. Yields
    an idempotent ``release``.
    """

    async with db.acquire() as gate:
        await gate.execute(
            f"""
            CREATE OR REPLACE FUNCTION {_GATE_FUNCTION}()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                PERFORM pg_advisory_xact_lock_shared({_GATE_KEY});
                RETURN NEW;
            END
            $$
            """
        )
        table = trigger.split(" ON ", 1)[1].split()[0]
        await gate.execute(
            f"CREATE TRIGGER {_GATE_FUNCTION} {trigger} "
            f"EXECUTE FUNCTION {_GATE_FUNCTION}()"
        )
        await gate.fetchval("SELECT pg_advisory_lock($1)", _GATE_KEY)
        held = True

        async def release() -> None:
            nonlocal held
            if held:
                held = False
                await gate.fetchval("SELECT pg_advisory_unlock($1)", _GATE_KEY)

        try:
            yield release
        finally:
            await release()
            await gate.execute(f"DROP TRIGGER IF EXISTS {_GATE_FUNCTION} ON {table}")
            await gate.execute(f"DROP FUNCTION IF EXISTS {_GATE_FUNCTION}()")


_GATE_MESSAGE_INSERT = "BEFORE INSERT ON thread_messages FOR EACH ROW"
# The settle's own identity-nulling row update.
_GATE_SETTLE_UPDATE = (
    "BEFORE UPDATE ON threads FOR EACH ROW "
    "WHEN (OLD.agent_id IS NOT NULL AND NEW.agent_id IS NULL)"
)


async def _blocked(db: PostgresDB, *fragments: str, event: str = "") -> None:
    """Wait until a query matching ``fragments`` waits on a heavyweight lock."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + 10
    while loop.time() < deadline:
        async with db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT query, wait_event FROM pg_stat_activity "
                "WHERE datname = current_database() AND pid <> pg_backend_pid() "
                "AND state = 'active' AND wait_event_type = 'Lock'"
            )
        if any(
            all(f in str(row["query"]) for f in fragments)
            and (not event or row["wait_event"] == event)
            for row in rows
        ):
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"no query blocked on a lock matching {fragments!r}")


async def _settle_ready(life) -> dict:
    """Begin, authorize and acknowledge: only the settle is left."""

    authority = await _begin_end(life)
    await _authorize_end(life, authority)
    await _acknowledge_quiescence(life, authority)
    return authority


async def _gather(tasks: list) -> list:
    try:
        return await asyncio.wait_for(asyncio.gather(*tasks), 10)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("writer", ["single", "batch"])
@pytest.mark.parametrize("step", ["begin", "settle"])
async def test_end_waits_for_a_fenced_write_without_deadlock(life, writer, step):
    """The write holds thread then agent; End's step queues behind it."""

    _adopt(life.agent_id, life.generation, life.attach_token)
    write = _single if writer == "single" else _batch
    authority = await _settle_ready(life) if step == "settle" else None
    tasks: list[asyncio.Task] = []
    async with _gate(life.db, _GATE_MESSAGE_INSERT) as release:
        tasks.append(asyncio.create_task(write(life, "A racing End")))
        await _blocked(life.db, "INSERT INTO thread_messages")
        end_step = _begin_end(life) if step == "begin" else _settle_end(life, authority)
        tasks.append(asyncio.create_task(end_step))
        await _blocked(life.db, "FROM threads")
        await release()
        _saved, ended = await _gather(tasks)

    assert await _rows(life) == ["A racing End"]
    if step == "begin":
        assert ended["state"] == "pending"
        await _single(life, "A after Begin")
        assert await _rows(life) == ["A racing End", "A after Begin"]
    else:
        assert ended is True
        await _assert_refused(life, "A after the settle")


@pytest.mark.asyncio
@pytest.mark.parametrize("writer", ["single", "batch"])
async def test_a_write_queued_behind_begin_lands_without_deadlock(life, writer):
    """Begin holds the thread row first; the write waits, then lands."""

    _adopt(life.agent_id, life.generation, life.attach_token)
    write = _single if writer == "single" else _batch
    async with life.db.acquire() as conn:
        transaction = conn.transaction()
        await transaction.start()
        try:
            authority = await life.db.begin_pinned_thread_retirement(
                life.thread_id, permanent=False, _connection=conn
            )
            assert authority["state"] == "pending"
            racing = asyncio.create_task(write(life, "A behind Begin", turn=7))
            await _blocked(life.db, "FROM threads", "FOR NO KEY UPDATE")
        except BaseException:
            await transaction.rollback()
            raise
        await transaction.commit()

    await asyncio.wait_for(racing, 10)
    assert await _rows(life) == ["A behind Begin"]
    assert await _total_turns(life) == 7


@pytest.mark.asyncio
@pytest.mark.parametrize("writer", ["single", "batch"])
async def test_a_write_queued_behind_the_settle_is_refused_without_deadlock(
    life, writer
):
    """The settle holds the thread row first; the write waits, then refuses."""

    _adopt(life.agent_id, life.generation, life.attach_token)
    write = _single if writer == "single" else _batch
    authority = await _settle_ready(life)
    tasks: list[asyncio.Task] = []
    async with _gate(life.db, _GATE_SETTLE_UPDATE) as release:
        tasks.append(asyncio.create_task(_settle_end(life, authority)))
        await _blocked(life.db, event="advisory")
        tasks.append(asyncio.create_task(write(life, "A behind the settle", turn=9)))
        await _blocked(life.db, "FROM threads", "FOR NO KEY UPDATE")
        await release()
        settled, refused = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True), 10
        )

    assert settled is True
    assert isinstance(refused, PinnedWriteRefused)
    assert await _rows(life) == []
    assert await _total_turns(life) == 0


# --- Through the session loop's writers ------------------------------------


@pytest.fixture
def loop_session(life, monkeypatch):
    """``persistent_app``'s loop writers bound to life A on the real database."""

    import agent.api.persistent_app as persistent_app
    import agent.persistent_graph as persistent_graph

    runtime = _identity_runtime(life.agent_id)
    runtime.bind_thread(life.thread_id)
    runtime.adopt(life.generation, life.attach_token, contract_advertised=True)
    monkeypatch.setattr(persistent_app, "_session_identity", runtime)
    monkeypatch.setattr(
        persistent_app,
        "_session",
        SimpleNamespace(postgres_conn=life.agent_db, turn_count=1, tool_decisions={}),
    )
    monkeypatch.setattr(persistent_app, "_turn_event_open", False)
    monkeypatch.setattr(persistent_app, "_broadcast", MagicMock())
    monkeypatch.setattr(persistent_graph, "_DELEGATION_RESULT_PERSIST_RETRY_S", 0)
    return persistent_app


async def _loop_delegation_turn(persistent_app, final: str, while_child_runs=None):
    """One delegation call through ``_execute_turn``, persisted by the loop."""

    from tests.test_persistent_delegation_batch import _callbacks, _tool
    from tests.test_session_delegation_live_batch import (
        _Parent,
        _batch as _delegation_call,
        _session_context,
        _turn,
    )

    llm = _Parent([_delegation_call("c1"), AIMessage(content=final)])
    callbacks = _callbacks(
        persist_message=persistent_app._loop_persist_message,
        require_delegation_persistence=True,
    )

    async def _child(_call):
        if while_child_runs is not None:
            await while_child_runs()
        return "report"

    delegate = _tool("delegate_agent", _child)
    return llm, _turn(llm, {"delegate_agent": delegate}, callbacks, _session_context())


async def _roles(life) -> list[tuple[str, str | None]]:
    async with life.db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT role, tool_call_id FROM thread_messages "
            "WHERE thread_id=$1::uuid ORDER BY seq",
            life.thread_id,
        )
    return [(row["role"], row["tool_call_id"]) for row in rows]


@pytest.mark.asyncio
@pytest.mark.parametrize("rebound", ["before_the_turn", "while_the_child_runs"])
async def test_loop_writes_of_a_rebound_life_are_refused_and_swallowed(
    life, loop_session, caplog, rebound
):
    """Nothing lands after the rebind, no error row, and refusals stay bounded."""

    caplog.set_level(logging.WARNING, logger="agent.database.postgres_db")
    if rebound == "before_the_turn":
        await _rebind(life, life.agent_id)
        llm, turn = await _loop_delegation_turn(loop_session, "must not be asked")
        # The refused assistant row is swallowed; the loop then refuses to
        # start a child for a tool call that is not durable.
        with pytest.raises(RuntimeError, match="not durably persisted"):
            await turn
        expected_rows: list = []
        # The assistant row and the error row, once each.
        expected_refusals = 2
    else:
        llm, turn = await _loop_delegation_turn(
            loop_session,
            "must not be asked",
            while_child_runs=lambda: _rebind(life, life.agent_id),
        )
        with pytest.raises(DelegationResultNotDurable):
            await turn
        # The assistant row landed while A was bound.
        expected_rows = [("ai", None)]
        # The strict result save's three bounded attempts of the same row,
        # then the error row once.
        expected_refusals = 4
    # Either way the turn stops before another provider call.
    assert llm.calls == 1
    await loop_session._loop_on_error("The turn stopped.")

    refusals = [
        record
        for record in caplog.records
        if "pinned transcript fence refused" in record.getMessage()
    ]
    assert len(refusals) == expected_refusals
    assert await _roles(life) == expected_rows


@pytest.mark.asyncio
async def test_loop_writes_during_a_forced_end_land_until_the_settle(
    life, loop_session
):
    """The turn that finishes before End reaches the agent keeps its rows."""

    authority = await _begin_end(life)
    await _authorize_end(life, authority)

    _llm, turn = await _loop_delegation_turn(loop_session, "the answer streamed")
    await turn
    await loop_session._loop_on_error("End reached the turn.")
    assert await _roles(life) == [
        ("ai", None),
        ("tool", "c1"),
        ("ai", None),
        ("error", None),
    ]

    await _acknowledge_quiescence(life, authority)
    assert await _settle_end(life, authority)
    late = AIMessage(content="after the settle", id=f"msg_{uuid4()}")
    assert await loop_session._loop_persist_message(late) is False
    assert len(await _roles(life)) == 4


# --- Where the identity comes from -----------------------------------------


class TestSessionIdentityArmsTheFence:
    """``SessionIdentityRuntime`` is the one feeder of the process cell."""

    G1, T1 = str(uuid4()), str(uuid4())
    G2, T2 = str(uuid4()), str(uuid4())
    AGENT = str(uuid4())

    def _runtime(
        self, fence: PinnedWriteFence, *, stateless: bool = False, agent_id=None
    ):
        return SessionIdentityRuntime(
            SessionIdentityPorts(
                agent_id=agent_id or (lambda: self.AGENT),
                pod_uid=lambda: "pod",
                lease=lambda: None,
                stateless_mode=lambda: stateless,
                orchestrator_client=lambda: None,
                identity_replaced=lambda: None,
                write_fence=fence,
            )
        )

    def test_adopt_arms_clear_keeps_next_adopt_replaces(self):
        fence = PinnedWriteFence()
        runtime = self._runtime(fence)
        runtime.adopt(self.G1, self.T1, contract_advertised=True)
        armed = PinnedWriteIdentity(self.AGENT, self.G1, self.T1)
        assert fence.identity == armed

        assert runtime.clear(expected_generation=self.G1, expected_attach_token=self.T1)
        assert runtime.session_generation is None
        assert fence.identity == armed

        runtime.adopt(self.G2, self.T2, contract_advertised=True)
        assert fence.identity == PinnedWriteIdentity(self.AGENT, self.G2, self.T2)

    def test_no_generation_disarms(self):
        fence = PinnedWriteFence()
        runtime = self._runtime(fence)
        runtime.adopt(self.G1, self.T1, contract_advertised=True)
        runtime.adopt(None, None, contract_advertised=False)
        assert fence.identity is None

    def test_stateless_never_arms(self):
        fence = PinnedWriteFence()
        self._runtime(fence, stateless=True).adopt(
            self.G1, None, contract_advertised=True
        )
        assert fence.identity is None

    def test_unreadable_agent_id_never_fails_the_adoption(self):
        """A stub client without an agent id arms an identity the DB refuses."""

        def unreadable():
            raise AttributeError("client has no agent_id")

        fence = PinnedWriteFence()
        runtime = self._runtime(fence, agent_id=unreadable)
        runtime.adopt(self.G1, self.T1, contract_advertised=True)
        assert runtime.session_generation == self.G1
        assert fence.identity == PinnedWriteIdentity(None, self.G1, self.T1)

    def test_runtime_without_the_port_leaves_the_process_cell_alone(self):
        runtime = SessionIdentityRuntime(
            SessionIdentityPorts(
                agent_id=lambda: self.AGENT,
                pod_uid=lambda: "pod",
                lease=lambda: None,
                stateless_mode=lambda: False,
                orchestrator_client=lambda: None,
                identity_replaced=lambda: None,
            )
        )
        runtime.adopt(self.G1, self.T1, contract_advertised=True)
        assert current_pinned_write_identity() is None

    def test_the_app_runtime_feeds_the_process_cell(self):
        from agent.api import persistent_app

        assert (
            persistent_app._session_identity._ports.write_fence
            is PROCESS_PINNED_WRITE_FENCE
        )
