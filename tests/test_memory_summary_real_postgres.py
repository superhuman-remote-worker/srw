"""The memory summary's read against real Postgres (append-only injection, D35).

``RecallStore.summary_stats`` counts the scope's memories by type and ranks
its keywords in SQL (``unnest``, normalisation, ``COUNT(DISTINCT id)``, C
collation for the tie-break), so only a real server proves the counts, the
project scoping, that superseded rows and rows below the retrieval floor
stay out, and that the read writes nothing.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import List

import asyncpg
import pytest
import pytest_asyncio

pytest.importorskip("testcontainers.postgres")

from agent.database.postgres_db import PostgresDB  # noqa: E402
from orchestrator.database.migrate import run_migrations  # noqa: E402
from shared.runtime.services.recall_store import (  # noqa: E402
    MemorySummaryStats,
    RecallStore,
)

PG_IMAGE = "pgvector/pgvector:pg15"
VECTOR_MIGRATIONS = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "migrations"
    / "vector"
)


@dataclass
class _ProjectMemoryConfig:
    project_scoped: bool = True
    retrieval_importance_floor: float = 0.4


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
    try:
        yield SimpleNamespace(db=db, project=uuid.uuid4(), other=uuid.uuid4())
    finally:
        await db.close()


def _store(db, *, project_ids: List[uuid.UUID], job_id=None) -> RecallStore:
    return RecallStore(
        db=db,
        embedding_service=None,
        job_id=job_id or uuid.uuid4(),
        config=_ProjectMemoryConfig(project_scoped=bool(project_ids)),
        project_id=project_ids[0] if project_ids else None,
        project_ids=project_ids or None,
    )


async def _seed(
    db,
    project_id,
    memory_type: str,
    keywords: List[str],
    *,
    importance: float = 0.8,
    superseded: bool = False,
    job_id=None,
):
    return await db.fetchval(
        """
        INSERT INTO memories (
            job_id, project_id, content, memory_type, keywords, importance,
            token_count, valid_to
        ) VALUES (
            $1, $2, $3, $4, $5, $6, 10,
            CASE WHEN $7 THEN CURRENT_TIMESTAMP ELSE NULL END
        )
        RETURNING id
        """,
        job_id or uuid.uuid4(),
        project_id,
        f"{memory_type} memory about {', '.join(keywords) or 'nothing'}",
        memory_type,
        keywords,
        importance,
        superseded,
    )


async def _seed_project(env) -> uuid.UUID:
    """Five servable memories plus three that must not count; returns a job."""
    job = uuid.uuid4()
    await _seed(env.db, env.project, "factual", ["Deploy", "helm"], job_id=job)
    await _seed(env.db, env.project, "procedural", ["deploy", "k3d"])
    await _seed(env.db, env.project, "error_solution", [" deploy ", "Helm", "alpha"])
    await _seed(env.db, env.project, "factual", ["k3d", "multi   word\ttopic"])
    await _seed(
        env.db,
        env.project,
        "factual",
        ["deploy", "DEPLOY", "", "   ", "x" * 41],  # one memory counts once
    )
    # Out: superseded, below the retrieval floor, another project.
    await _seed(env.db, env.project, "factual", ["retired"], superseded=True)
    await _seed(env.db, env.project, "relational", ["faint"], importance=0.2)
    await _seed(env.db, env.other, "vocabulary", ["elsewhere", "deploy"])
    return job


@pytest.mark.asyncio
async def test_counts_types_and_topics_of_the_project(env):
    await _seed_project(env)

    stats = await _store(env.db, project_ids=[env.project]).summary_stats()

    assert stats == MemorySummaryStats(
        total=5,
        by_type=(("factual", 3), ("error_solution", 1), ("procedural", 1)),
        # deploy 4; helm and k3d 2 each (alphabetical); then the singles.
        topics=("deploy", "helm", "k3d", "alpha", "multi word topic"),
    )


@pytest.mark.asyncio
async def test_the_topic_list_is_capped(env):
    await _seed_project(env)
    stats = await _store(env.db, project_ids=[env.project]).summary_stats(max_topics=2)
    assert stats.topics == ("deploy", "helm")


@pytest.mark.asyncio
async def test_a_multi_project_session_sees_every_project(env):
    await _seed_project(env)
    stats = await _store(env.db, project_ids=[env.project, env.other]).summary_stats()
    assert stats.total == 6
    assert ("vocabulary", 1) in stats.by_type
    assert stats.topics[0] == "deploy"
    assert "elsewhere" in stats.topics


@pytest.mark.asyncio
async def test_a_job_scoped_store_counts_its_job(env):
    job = await _seed_project(env)
    stats = await _store(env.db, project_ids=[], job_id=job).summary_stats()
    assert stats == MemorySummaryStats(
        total=1, by_type=(("factual", 1),), topics=("deploy", "helm")
    )


@pytest.mark.asyncio
async def test_an_empty_project_has_nothing(env):
    assert (
        await _store(env.db, project_ids=[uuid.uuid4()]).summary_stats()
        == MemorySummaryStats()
    )


@pytest.mark.asyncio
async def test_the_read_writes_nothing(env):
    await _seed_project(env)
    before = await env.db.fetch(
        "SELECT id, access_count, last_accessed FROM memories WHERE project_id = $1"
        " ORDER BY id",
        env.project,
    )

    await _store(env.db, project_ids=[env.project]).summary_stats()

    after = await env.db.fetch(
        "SELECT id, access_count, last_accessed FROM memories WHERE project_id = $1"
        " ORDER BY id",
        env.project,
    )
    assert [dict(r) for r in after] == [dict(r) for r in before]


@pytest.mark.asyncio
async def test_the_same_memories_render_the_same_summary(env):
    await _seed_project(env)
    store = _store(env.db, project_ids=[env.project])
    first = RecallStore.render_memory_summary(await store.summary_stats())
    second = RecallStore.render_memory_summary(await store.summary_stats())
    assert first == second
    assert first.startswith(
        "Project memory overview (harness context, not a user message): "
        "5 memories from earlier jobs and sessions.\n"
        "By type: 3 factual, 1 error_solution, 1 procedural.\n"
        "Frequent topics: deploy, helm, k3d, alpha, multi word topic.\n"
    )
