"""Real-Postgres proof that the orphan sweep archives orphans, not connectors.

``KnowledgeStore.reconcile_orphans`` runs after every reindex of a project
vault and soft-archives active pathless rows the git tree can never adopt.
The ``datasource`` note the knowledge projection writes for each connector a
project reaches (``ds-<id8>``) is pathless by design and never in any tree, so
it matched too: an hour after a project got its vault, the sweep archived
every connector note in it. The projection's upsert leaves ``status`` alone,
so the note stayed archived, and the row-level search
(``knowledge_hybrid_search``, ``status = 'active'``) that surfaces connectors
to the agent no longer found them.
"""

from __future__ import annotations

import re
import uuid
from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("testcontainers.postgres")

import asyncpg  # noqa: E402
import pytest_asyncio  # noqa: E402

from orchestrator.database.migrate import run_migrations  # noqa: E402
from shared.runtime.services.knowledge_store import KnowledgeStore  # noqa: E402

PG_IMAGE = "pgvector/pgvector:pg15"
VECTOR_MIGRATIONS = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "migrations"
    / "vector"
)


@pytest.fixture(scope="module")
def pg_dsn():
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer(PG_IMAGE)
    try:
        container.start()
    except Exception as exc:  # pragma: no cover - environment without a runtime
        pytest.skip(f"no container runtime for testcontainers: {exc}")
    try:
        yield re.sub(
            r"^postgresql\+\w+://", "postgresql://", container.get_connection_url()
        )
    finally:
        container.stop()


@pytest_asyncio.fixture
async def vector_pool(pg_dsn):
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    try:
        await run_migrations(pool, VECTOR_MIGRATIONS)
        yield pool
    finally:
        await pool.close()


async def _row(pool, project, note_id, note_type) -> None:
    """A pathless row written an hour and a half ago, past the sweep's grace."""
    await pool.execute(
        """
        INSERT INTO knowledge_index
            (note_id, project_id, title, note_type, status, content, indexed_at)
        VALUES ($1, $2, 'Title', $3, 'active', 'body',
                NOW() - INTERVAL '90 minutes')
        """,
        note_id,
        project,
        note_type,
    )


async def _status(pool, project, note_id) -> str:
    return await pool.fetchval(
        "SELECT status FROM knowledge_index WHERE project_id = $1 AND note_id = $2",
        project,
        note_id,
    )


@pytest.mark.asyncio
async def test_connector_notes_survive_the_orphan_sweep(vector_pool):
    store = KnowledgeStore(db=vector_pool, embedding_service=None)
    project = uuid.uuid4()
    await _row(vector_pool, project, "ds-1a2b3c4d", "datasource")
    await _row(vector_pool, project, "lost-provisional-note", "learning")
    await _row(vector_pool, project, "adoptable-note", "decision")

    archived = await store.reconcile_orphans(
        project_id=project, tree_slugs=["adoptable-note"], grace=timedelta(hours=1)
    )

    assert archived == 1
    assert await _status(vector_pool, project, "lost-provisional-note") == "archived"
    assert await _status(vector_pool, project, "ds-1a2b3c4d") == "active"
    assert await _status(vector_pool, project, "adoptable-note") == "active"
