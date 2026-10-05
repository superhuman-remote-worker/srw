"""Real row-lock races for signed native first-use admission."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import asyncssh
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from agent.api.native_workspace_first_use import NativeFirstUseRefused, apply_native_first_use
from orchestrator.database.postgres import PostgresDB
from shared.native_workspace_first_use import mint_native_first_use_proof
from shared.pinned_session_identity import pinned_session_ready_identity_fingerprint
from tests.test_stateless_input_delivery_real_postgres import _seed_pinned_thread

SCHEMA = Path(__file__).resolve().parents[1] / "src/orchestrator/database/schema_current.sql"


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("pgvector/pgvector:pg15") as container:
        yield container.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied):
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=5)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute("TRUNCATE threads, agents, users CASCADE")
    try:
        yield store
    finally:
        await store.close()


async def native_life(db):
    pod, process = str(uuid4()), str(uuid4())
    _, thread, agent = await _seed_pinned_thread(db, pod_uid=pod)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE agents SET metadata=jsonb_build_object("
            "'dispatch_process_generation',$2::text) WHERE id=$1",
            agent, process,
        )
        await conn.execute(
            "UPDATE threads SET metadata=jsonb_set(metadata,"
            "'{config_override,officer,enabled}','false'::jsonb) WHERE id=$1",
            thread,
        )
    row = await db.get_thread(str(thread))
    generation = str(row["runtime_generation"])
    attach = str(row["runtime_attach_token"])
    fingerprint = pinned_session_ready_identity_fingerprint(
        thread_id=str(thread), runtime_generation=generation,
        agent_id=str(agent), runtime_attach_token=attach, pod_uid=pod,
    )
    signer = asyncssh.generate_private_key("ssh-ed25519")
    proof = mint_native_first_use_proof(
        signer, event_id=uuid4().hex, connection_id=uuid4().hex,
        channel_kind="ssh_session", handle="s-7f3a91c2", fingerprint="SHA256:user",
        thread_id=str(thread), runtime_generation=generation, agent_id=str(agent),
        pod_uid=pod, process_generation=process,
        session_identity_fingerprint=fingerprint,
        backend="container", workspace_digest="sha256:" + "b" * 64,
        lease_id="", binding="",
    )
    recipient = {
        "expected_thread_id": str(thread), "expected_agent_id": str(agent),
        "expected_pod_uid": pod, "expected_process_generation": process,
    }
    identity = SimpleNamespace(
        thread_id=str(thread), session_generation=generation,
        attach_token=attach, agent_id=str(agent), pod_uid=pod,
        runtime_contract=True, fingerprint=lambda: fingerprint,
    )
    identity.snapshot = lambda: identity
    observed = []
    current_conn = None

    @asynccontextmanager
    async def acquire():
        nonlocal current_conn
        async with db.acquire() as conn:
            current_conn = conn
            yield conn

    def note(_life):
        assert current_conn is not None and current_conn.is_in_transaction()
        observed.append(_life)
        return "accepted" if len(observed) == 1 else "already_observed"

    termination = SimpleNamespace(
        runtime_admission_closed=lambda: False,
        terminating=False,
        note_native_first_use=note,
    )
    kwargs = {
        "session": SimpleNamespace(postgres_conn=SimpleNamespace(acquire=acquire)),
        "identity": identity, "termination": termination,
        "agent_id": str(agent), "pod_uid": pod,
        "process_generation": process,
        "public_keys": [signer.export_public_key().decode()],
    }
    return proof, recipient, kwargs, observed


@pytest.mark.asyncio
async def test_real_lock_latches_once_and_rejects_registered_process_and_attach_rotation(db):
    proof, recipient, kwargs, observed = await native_life(db)
    assert (await apply_native_first_use(proof, recipient, **kwargs))["status"] == "accepted"
    assert (await apply_native_first_use(proof, recipient, **kwargs))["status"] == "already_observed"
    assert len(observed) == 2
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE agents SET metadata=jsonb_build_object("
            "'dispatch_process_generation',$2::text) WHERE id=$1",
            UUID(proof["agent_id"]), str(uuid4()),
        )
    with pytest.raises(NativeFirstUseRefused, match="stale_native_process"):
        await apply_native_first_use(proof, recipient, **kwargs)
    assert len(observed) == 2
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET runtime_attach_token=$2 WHERE id=$1",
            UUID(proof["thread_id"]), uuid4(),
        )
    with pytest.raises(NativeFirstUseRefused, match="runtime_authority_lost"):
        await apply_native_first_use(proof, recipient, **kwargs)
    assert len(observed) == 2


@pytest.mark.asyncio
async def test_blocked_guard_after_durable_retirement_refuses_without_latch(db):
    proof, recipient, kwargs, observed = await native_life(db)
    async with db.acquire() as blocker:
        async with blocker.transaction():
            await blocker.execute(
                "UPDATE threads SET status='ended' WHERE id=$1",
                UUID(proof["thread_id"]),
            )
            attempt = asyncio.create_task(apply_native_first_use(proof, recipient, **kwargs))
            await asyncio.sleep(0.1)
            assert not attempt.done()
            assert observed == []
    with pytest.raises(NativeFirstUseRefused, match="runtime_authority_lost"):
        await asyncio.wait_for(attempt, timeout=5)
    assert observed == []


@pytest.mark.asyncio
async def test_blocked_agent_lock_observes_committed_process_rotation(db):
    proof, recipient, kwargs, observed = await native_life(db)
    async with db.acquire() as blocker:
        async with blocker.transaction():
            await blocker.execute(
                "UPDATE agents SET metadata=jsonb_build_object("
                "'dispatch_process_generation',$2::text) WHERE id=$1",
                UUID(proof["agent_id"]), str(uuid4()),
            )
            attempt = asyncio.create_task(apply_native_first_use(proof, recipient, **kwargs))
            await asyncio.sleep(0.1)
            assert not attempt.done()
            assert observed == []
    with pytest.raises(NativeFirstUseRefused, match="stale_native_process"):
        await asyncio.wait_for(attempt, timeout=5)
    assert observed == []


@pytest.mark.asyncio
async def test_database_unavailable_refuses_notice_without_latch(db):
    proof, recipient, kwargs, observed = await native_life(db)
    await db.close()
    with pytest.raises(NativeFirstUseRefused, match="native_authority_unavailable"):
        await apply_native_first_use(proof, recipient, **kwargs)
    assert observed == []
