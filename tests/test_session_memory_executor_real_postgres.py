"""Real-vector-Postgres proofs for the session-memory destination ledger.

Mocks cannot prove that a parent memory, its retrieval triggers, and the
idempotency receipt share one server transaction. These tests replay the real
vector migration chain and exercise the production executor against asyncpg.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

pytest.importorskip("testcontainers.postgres")
pytest.importorskip("pgvector.asyncpg")

from orchestrator.database.migrate import run_migrations  # noqa: E402
from orchestrator.services.session_memory_effects import (  # noqa: E402
    SESSION_MEMORY_EFFECT_GROUP,
    SESSION_MEMORY_EFFECT_NAME,
    SessionMemoryEffect,
)
from orchestrator.services.session_memory_executor import (  # noqa: E402
    SessionMemoryEffectExecutor,
)
from agent.database.postgres_db import PostgresDB  # noqa: E402

PG_IMAGE = "pgvector/pgvector:pg15"
VECTOR_MIGRATIONS = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "migrations"
    / "vector"
)
DIMS = 4096

PRODUCER_ID = UUID("11111111-aaaa-4111-8111-111111111111")
THREAD_ID = UUID("22222222-bbbb-4222-8222-222222222222")
INPUT_ID = UUID("33333333-cccc-4333-8333-333333333333")
OUTPUT_ID = UUID("44444444-dddd-4444-8444-444444444444")
PROJECT_ID = UUID("55555555-eeee-4555-8555-555555555555")


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *_args):
        return None


class _AppConnection:
    async def fetchrow(self, sql, *_args):
        if "SELECT * FROM threads" in sql:
            return {"id": THREAD_ID, "project_id": PROJECT_ID, "metadata": {}}
        if "role, turn_number" in sql:
            return {
                "id": INPUT_ID,
                "seq": 10,
                "role": "event",
                "turn_number": 3,
                "turn_execution_id": PRODUCER_ID,
                "rewound_at": None,
            }
        if "SELECT turn_number, turn_execution_id" in sql:
            return {
                "turn_number": 3,
                "turn_execution_id": PRODUCER_ID,
                "rewound_at": None,
            }
        raise AssertionError(sql)

    async def fetch(self, _sql, *_args):
        return [
            {
                "id": INPUT_ID,
                "seq": 10,
                "role": "event",
                "content": "remember the transactional fact",
                "tool_calls": None,
                "tool_call_id": None,
                "turn_number": 3,
                "rewound_at": None,
            },
            {
                "id": OUTPUT_ID,
                "seq": 11,
                "role": "assistant",
                "content": "remembered",
                "tool_calls": None,
                "tool_call_id": None,
                "turn_number": 3,
                "rewound_at": None,
            },
        ]

    async def fetchval(self, _sql, *_args):
        return False


class _AppDB:
    def __init__(self):
        self.conn = _AppConnection()

    def acquire(self):
        return _Acquire(self.conn)


class _Embedding:
    def __init__(self, *, fail_retrieval: bool = False):
        self.fail_retrieval = fail_retrieval

    async def embed(self, _text):
        return [1.0] + [0.0] * (DIMS - 1)

    async def embed_batch(self, messages):
        if self.fail_retrieval:
            raise RuntimeError("retrieval embedding failed")
        return [[1.0] + [0.0] * (DIMS - 1) for _ in messages]


def _effect() -> SessionMemoryEffect:
    now = datetime.now(UTC)
    return SessionMemoryEffect(
        producer_id=str(PRODUCER_ID),
        scope_id=str(THREAD_ID),
        effect_name=SESSION_MEMORY_EFFECT_NAME,
        effect_group=SESSION_MEMORY_EFFECT_GROUP,
        attempts=1,
        max_attempts=5,
        created_at=now,
        complete_by=now + timedelta(minutes=2),
        detail={
            "input_message_id": str(INPUT_ID),
            "turn_number": 3,
            "boundary_seq": 10,
            "end_seq": 11,
            "memory_scope_kind": "project",
            "memory_scope_id": str(PROJECT_ID),
        },
        authority_permit=AsyncMock(),
    )


def _config():
    return SimpleNamespace(
        memory=SimpleNamespace(
            enabled=True,
            project_scoped=True,
            importance_threshold=0.3,
            dedup_threshold=0.92,
            ingestion=SimpleNamespace(enabled=False),
            extraction=SimpleNamespace(write_gate=True),
        ),
        auxiliary=SimpleNamespace(
            tasks={"extract_memories": SimpleNamespace(enabled=True)}
        ),
        agent_id="session",
    )


def _memory():
    return SimpleNamespace(
        content="The destination transaction is atomic.",
        summary="Atomic destination",
        keywords=["atomic"],
        importance=0.8,
        type="factual",
        retrieval_messages=["When must the destination be atomic?"],
    )


@pytest.fixture(scope="module")
def pg_dsn():
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer(PG_IMAGE)
    try:
        container.start()
    except Exception as exc:  # pragma: no cover - environment without runtime
        pytest.skip(f"no container runtime for testcontainers: {exc}")
    try:
        yield re.sub(
            r"^postgresql\+\w+://", "postgresql://", container.get_connection_url()
        )
    finally:
        container.stop()


@pytest_asyncio.fixture
async def vector_db(pg_dsn):
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    try:
        await run_migrations(pool, VECTOR_MIGRATIONS)
    finally:
        await pool.close()
    db = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=3)
    await db.connect()
    try:
        yield db
    finally:
        await db.close()


@pytest_asyncio.fixture(autouse=True)
async def clean_destination(vector_db):
    await vector_db.execute("DELETE FROM session_memory_effect_executions")
    await vector_db.execute("DELETE FROM memory_retrieval_messages")
    await vector_db.execute("DELETE FROM memories")


def _patch_executor(aux, embedding):
    return (
        patch(
            "orchestrator.services.session_memory_executor.load_config_from_resolved",
            return_value=_config(),
        ),
        patch(
            "orchestrator.services.session_memory_executor._build_auxiliary_llm",
            return_value=aux,
        ),
        patch(
            "orchestrator.services.session_memory_executor._build_embedding_service",
            return_value=embedding,
        ),
        patch(
            "orchestrator.services.session_memory_executor.resolve_memory_extraction_prompt",
            return_value="extract",
        ),
    )


@pytest.mark.asyncio
async def test_response_loss_replay_skips_all_vector_mutations(vector_db):
    aux = SimpleNamespace(
        chain=AsyncMock(return_value=SimpleNamespace(memories=[_memory()])),
        health=SimpleNamespace(record_success=MagicMock(), record_failure=MagicMock()),
    )
    resolver = AsyncMock(return_value={"agent": {}})
    patches = _patch_executor(aux, _Embedding())
    for item in patches:
        item.start()
    try:
        executor = SessionMemoryEffectExecutor(_AppDB(), vector_db, resolver)
        first = await executor(_effect())
        second = await executor(_effect())
    finally:
        for item in reversed(patches):
            item.stop()

    assert (
        first
        == second
        == {
            "disposition": "completed",
            "turn_number": 3,
            "extracted_count": 1,
            "stored_count": 1,
        }
    )
    assert aux.chain.await_count == 1
    assert await vector_db.fetchval("SELECT count(*) FROM memories") == 1
    assert (
        await vector_db.fetchval("SELECT count(*) FROM memory_retrieval_messages") == 1
    )
    ledger = await vector_db.fetchrow(
        "SELECT state, extracted_count, stored_count "
        "FROM session_memory_effect_executions"
    )
    assert dict(ledger) == {
        "state": "done",
        "extracted_count": 1,
        "stored_count": 1,
    }


@pytest.mark.asyncio
async def test_strict_retrieval_failure_rolls_back_parent_and_ledger(vector_db):
    aux = SimpleNamespace(
        chain=AsyncMock(return_value=SimpleNamespace(memories=[_memory()])),
        health=SimpleNamespace(record_success=MagicMock(), record_failure=MagicMock()),
    )
    resolver = AsyncMock(return_value={"agent": {}})
    patches = _patch_executor(aux, _Embedding(fail_retrieval=True))
    for item in patches:
        item.start()
    try:
        with pytest.raises(RuntimeError, match="retrieval embedding failed"):
            await SessionMemoryEffectExecutor(_AppDB(), vector_db, resolver)(_effect())
    finally:
        for item in reversed(patches):
            item.stop()

    assert await vector_db.fetchval("SELECT count(*) FROM memories") == 0
    assert (
        await vector_db.fetchval(
            "SELECT count(*) FROM session_memory_effect_executions"
        )
        == 0
    )
