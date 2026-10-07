"""Pinned transcript writes are fenced on the runtime life (parallel_subagents P2).

``save_thread_message`` and ``save_thread_messages`` used to write unfenced on
the pinned lane, so a replaced or retiring pinned runtime could add rows after
its successor settled the turn (a late live tool result gets a random id, so it
duplicates a settled result). Both writers now lock the thread row on the armed
life (agent, runtime generation, attach token, no retirement token), then the
agent row, then write, in one transaction, the same fence and lock order as the
pinned event journal. The life comes from ``SessionIdentityRuntime`` through
``agent.api.pinned_write_fence``. Real Postgres with the full migration chain.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import orchestrator
import pytest
import pytest_asyncio
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
                "INSERT INTO threads (id, user_id, title, status) "
                "VALUES ($1, $2, 'P2 fence', 'active') RETURNING runtime_generation",
                thread_id,
                owner,
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


@pytest.mark.asyncio
async def test_retiring_life_writes_nothing(life):
    _adopt(life.agent_id, life.generation, life.attach_token)
    await _batch(life, "A before End")

    authority = await life.db.begin_pinned_thread_retirement(
        life.thread_id, permanent=False
    )
    assert authority["state"] == "pending"
    async with life.db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT agent_id, runtime_attach_token, runtime_retirement_token "
            "FROM threads WHERE id=$1::uuid",
            life.thread_id,
        )
    # Only the retirement token changed: agent and attach token still match A.
    assert str(row["agent_id"]) == life.agent_id
    assert str(row["runtime_attach_token"]) == life.attach_token
    assert row["runtime_retirement_token"] is not None

    await _assert_refused(life, "A during End")


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
_GATE_FUNCTION = "test_gate_pinned_thread_message_insert"


async def _blocked_on_lock(db: PostgresDB, *fragments: str) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 10
    while loop.time() < deadline:
        async with db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT query FROM pg_stat_activity "
                "WHERE datname = current_database() AND pid <> pg_backend_pid() "
                "AND state = 'active' AND wait_event_type = 'Lock'"
            )
        if any(all(f in str(row["query"]) for f in fragments) for row in rows):
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"no query blocked on a lock matching {fragments!r}")


@pytest.mark.asyncio
@pytest.mark.parametrize("writer", ["single", "batch"])
async def test_end_waits_for_a_fenced_write_without_deadlock(life, writer):
    """The write holds thread then agent; End queues behind it and succeeds."""

    _adopt(life.agent_id, life.generation, life.attach_token)
    tasks: list[asyncio.Task] = []
    async with life.db.acquire() as gate:
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
        await gate.execute(
            f"CREATE TRIGGER {_GATE_FUNCTION} BEFORE INSERT ON thread_messages "
            f"FOR EACH ROW EXECUTE FUNCTION {_GATE_FUNCTION}()"
        )
        await gate.fetchval("SELECT pg_advisory_lock($1)", _GATE_KEY)
        released = False
        try:
            write = _single if writer == "single" else _batch
            tasks.append(asyncio.create_task(write(life, "A racing End")))
            # The write holds its thread and agent locks, gated in its INSERT.
            await _blocked_on_lock(life.db, "INSERT INTO thread_messages")
            tasks.append(
                asyncio.create_task(
                    life.db.begin_pinned_thread_retirement(
                        life.thread_id, permanent=False
                    )
                )
            )
            await _blocked_on_lock(life.db, "FROM threads")
            await gate.fetchval("SELECT pg_advisory_unlock($1)", _GATE_KEY)
            released = True
            _saved, authority = await asyncio.wait_for(asyncio.gather(*tasks), 10)
        finally:
            if not released:
                await gate.fetchval("SELECT pg_advisory_unlock($1)", _GATE_KEY)
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await gate.execute(
                f"DROP TRIGGER IF EXISTS {_GATE_FUNCTION} ON thread_messages"
            )
            await gate.execute(f"DROP FUNCTION IF EXISTS {_GATE_FUNCTION}()")

    assert authority["state"] == "pending"
    # Committed before End took the row; nothing after it.
    assert await _rows(life) == ["A racing End"]
    await _assert_refused(life, "A after End")


@pytest.mark.asyncio
@pytest.mark.parametrize("writer", ["single", "batch"])
async def test_a_write_queued_behind_end_is_refused_without_deadlock(life, writer):
    """End holds the thread row first; the write waits, then refuses."""

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
            racing = asyncio.create_task(write(life, "A behind End", turn=99))
            await _blocked_on_lock(life.db, "FROM threads", "FOR NO KEY UPDATE")
        except BaseException:
            await transaction.rollback()
            raise
        await transaction.commit()

    with pytest.raises(PinnedWriteRefused):
        await asyncio.wait_for(racing, 10)
    assert await _rows(life) == []
    assert await _total_turns(life) == 0


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
