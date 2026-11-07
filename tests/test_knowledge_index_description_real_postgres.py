"""knowledge_index.description against a real pgvector (vector/0030).

kb_update re-renders a note's whole file from its row, and the row had no
description, so an explicit description given to kb_write became the body's
first sentence on the next update. These tests apply the real vector
migration chain from zero, then drive the real store SQL: the reindexer's
upsert writes the file's description and the two agent-facing readers return
it. Skips when testcontainers or a container runtime is unavailable
(test_knowledge_note_revisions.py idiom).
"""

from __future__ import annotations

import asyncio
import re
import uuid
from pathlib import Path

import pytest
import pytest_asyncio

pytest.importorskip("testcontainers.postgres")

from orchestrator.database.migrate import run_migrations  # noqa: E402
from orchestrator.database.postgres import PostgresDB  # noqa: E402
from shared.runtime.services.knowledge_store import KnowledgeStore  # noqa: E402

# pgvector, not plain postgres: the vector migrations CREATE EXTENSION vector.
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
def migrated_dsn():
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer(PG_IMAGE)
    try:
        container.start()
    except Exception as exc:  # pragma: no cover - env without a runtime
        pytest.skip(f"no container runtime for testcontainers: {exc}")
    try:
        dsn = re.sub(
            r"^postgresql\+\w+://", "postgresql://", container.get_connection_url()
        )

        async def _migrate():
            import asyncpg

            pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
            try:
                await run_migrations(pool, VECTOR_MIGRATIONS)
            finally:
                await pool.close()

        asyncio.run(_migrate())
        yield dsn
    finally:
        container.stop()


@pytest_asyncio.fixture
async def store(migrated_dsn):
    db = PostgresDB(connection_string=migrated_dsn)
    await db.connect()
    try:
        yield KnowledgeStore(db=db, embedding_service=None)
    finally:
        await db.close()


async def _upsert(store, kb_id, description):
    await store.upsert_kb_note(
        kb_id=kb_id,
        note_id="chose-jwt",
        path="knowledge/chose-jwt.md",
        title="Chose JWT",
        note_type="decision",
        content="# Chose JWT\n\nWe use JWT.\n",
        blob_sha=None,
        embedding_version=None,
        description=description,
    )


@pytest.mark.asyncio
async def test_the_files_description_reaches_both_readers(store):
    kb_id = uuid.uuid4()
    description = r"Why JWT beat OAuth (see C:\docs\auth.md)."
    await _upsert(store, kb_id, description)

    note = await store.get_note_by_slug(kb_id, "chose-jwt")
    assert note["description"] == description
    rows = await store.list_notes_full(kb_id)
    assert [row["description"] for row in rows] == [description]


@pytest.mark.asyncio
async def test_a_reindex_replaces_it_and_a_file_without_one_clears_it(store):
    kb_id = uuid.uuid4()
    await _upsert(store, kb_id, "First summary.")
    await _upsert(store, kb_id, "Edited summary.")
    assert (await store.get_note_by_slug(kb_id, "chose-jwt"))[
        "description"
    ] == "Edited summary."

    # The file is the only source: no line means no description, not "keep".
    await _upsert(store, kb_id, None)
    assert (await store.get_note_by_slug(kb_id, "chose-jwt"))["description"] is None
