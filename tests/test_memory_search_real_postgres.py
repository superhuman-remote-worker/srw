"""memory_search against real Postgres: handle lookup SQL and project scope.

``RecallStore.get_by_handle`` recomputes the display handle
(``m:`` + sha256(row id)[:6], ``context_entries.memory_handle``) in SQL, so
only a real server proves the two agree. The search half proves the tool
reads through the project-scoped hybrid search like the push path: another
project's memories never come back, and neither does a superseded row.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import asyncpg
import pytest
import pytest_asyncio

pytest.importorskip("testcontainers.postgres")
pytest.importorskip("pgvector.asyncpg")

from agent.database.postgres_db import PostgresDB  # noqa: E402
from agent.services.memory import MemoryRuntime  # noqa: E402
from agent.services.memory.plugins.memory_search import (  # noqa: E402
    MemorySearchExtension,
)
from orchestrator.database.migrate import run_migrations  # noqa: E402
from shared.runtime.core.context_entries import memory_handle  # noqa: E402
from shared.runtime.services.recall_store import RecallStore  # noqa: E402

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


@dataclass
class _ProjectMemoryConfig:
    enabled: bool = True
    project_scoped: bool = True
    budget_tokens: int = 100_000
    max_memories_per_injection: int = 50
    importance_threshold: float = 0.3
    dedup_threshold: float = 0.92
    default_ttl: int = 10


class _StubEmbedding:
    async def embed(self, text: str):
        return [1.0] + [0.0] * (DIMS - 1)


@pytest.fixture(scope="module")
def pg_dsn():
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer(PG_IMAGE)
    try:
        container.start()
    except Exception as exc:  # pragma: no cover - env without a runtime
        pytest.skip(f"no container runtime for testcontainers: {exc}")
    try:
        yield re.sub(
            r"^postgresql\+\w+://", "postgresql://", container.get_connection_url()
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def migrated_dsn(pg_dsn):
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    try:
        await run_migrations(pool, VECTOR_MIGRATIONS)
    finally:
        await pool.close()
    return pg_dsn


@pytest_asyncio.fixture
async def env(migrated_dsn):
    db = PostgresDB(connection_string=migrated_dsn)
    await db.connect()
    project, other = uuid.uuid4(), uuid.uuid4()
    store = RecallStore(
        db=db,
        embedding_service=_StubEmbedding(),
        job_id=uuid.uuid4(),
        config=_ProjectMemoryConfig(),
        project_id=project,
    )
    try:
        yield SimpleNamespace(db=db, store=store, project=project, other=other)
    finally:
        await db.close()


async def _seed(db, project_id, content, *, superseded=False):
    return await db.fetchval(
        """
        INSERT INTO memories (
            job_id, project_id, content, summary, keywords,
            embedding, sparse_keywords, importance, token_count, valid_to
        ) VALUES (
            $1, $2, $3, $4, $5, $6, to_tsvector('english', $3), $7, $8,
            CASE WHEN $9 THEN CURRENT_TIMESTAMP ELSE NULL END
        )
        RETURNING id
        """,
        uuid.uuid4(),
        project_id,
        content,
        content[:20],
        ["deploy", "region"],
        [1.0] + [0.0] * (DIMS - 1),
        0.8,
        10,
        superseded,
    )


def _extension(store) -> MemorySearchExtension:
    return MemorySearchExtension(
        MemoryRuntime(
            recall_store=store,
            memory_config=SimpleNamespace(
                pipeline=SimpleNamespace(scorers=[], policies=[])
            ),
        )
    )


@pytest.mark.asyncio
async def test_get_by_handle_matches_the_python_handle(env):
    mine = await _seed(env.db, env.project, "The prod deploy region is eu-central.")
    theirs = await _seed(env.db, env.other, "The other project deploys to us-east.")
    retired = await _seed(
        env.db, env.project, "The prod deploy region was us-west.", superseded=True
    )

    found = await env.store.get_by_handle(memory_handle(mine))
    assert found is not None and found.id == mine
    # Every accepted spelling resolves the same row.
    assert (await env.store.get_by_handle(f"[{memory_handle(mine)}]")).id == mine
    # Out of scope and retired rows never resolve; nor does a bad handle.
    assert await env.store.get_by_handle(memory_handle(theirs)) is None
    assert await env.store.get_by_handle(memory_handle(retired)) is None
    assert await env.store.get_by_handle("not a handle") is None
    # Each successful fetch counts as an access (two above).
    count = await env.db.fetchval(
        "SELECT access_count FROM memories WHERE id = $1", mine
    )
    assert count == 2


@pytest.mark.asyncio
async def test_search_and_fetch_stay_in_the_project(env):
    mine = await _seed(env.db, env.project, "The prod deploy region is eu-central.")
    theirs = await _seed(env.db, env.other, "The prod deploy region is us-east.")
    retired = await _seed(
        env.db, env.project, "The prod deploy region was us-west.", superseded=True
    )
    ext = _extension(env.store)

    out = await ext.run(query="deploy region")
    assert f"[{memory_handle(mine)}]" in out
    assert memory_handle(theirs) not in out
    assert memory_handle(retired) not in out

    fetched = await ext.run(handle=memory_handle(mine))
    assert fetched.startswith(f"Memory {memory_handle(mine)}:")
    assert "eu-central" in fetched
    missing = await ext.run(handle=memory_handle(theirs))
    assert missing.startswith("No current memory has the handle")
