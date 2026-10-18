"""Real-Postgres concurrency tests for RecallStore's deadlock containment.

Guards knowledge-base/knowledge/issues/project_scoped_memory_deadlocks_under_parallel_jobs.md: five
same-project jobs racing the TTL decrement (a scope-wide UPDATE, retired since
with the TTL tier) against the access-stat write (``id = ANY(...)`` UPDATE in
RRF-score order) produced 138 contained retrieval deadlocks — PostgreSQL
acquired the overlapping tuple locks in divergent orders. The containment fix
locks in deterministic id order; the access-stat write still races itself.

**A mock cannot fail this way.** Lock-order cycles exist only inside a real
lock manager, so these run two RecallStores over separate connection pools
against a real pgvector container with the real vector migrations, and assert
via ``memory_health`` that nothing was contained.
"""

from __future__ import annotations

import asyncio
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

from orchestrator.database.migrate import run_migrations  # noqa: E402
from agent.database.postgres_db import PostgresDB  # noqa: E402
from shared.runtime.services.recall_store import RecallStore, memory_health  # noqa: E402

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

DIMS = 4096
TURNS = 15


@dataclass
class _ProjectMemoryConfig:
    """Minimal MemoryConfig stand-in with project scoping enabled."""

    enabled: bool = True
    project_scoped: bool = True
    budget_tokens: int = 100_000
    max_memories_per_injection: int = 50
    importance_threshold: float = 0.3
    dedup_threshold: float = 0.92


class _StubEmbedding:
    """Deterministic non-zero embedding — keeps cosine distance NaN-free."""

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
    """Apply the real vector migrations once for the module."""
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    try:
        await run_migrations(pool, VECTOR_MIGRATIONS)
    finally:
        await pool.close()
    return pg_dsn


@pytest_asyncio.fixture
async def env(migrated_dsn):
    """Two RecallStores on separate pools sharing one project scope.

    Separate pools mirror separate worker pods: each store's statements run in
    their own server-side transactions, so lock acquisition genuinely races.
    """
    project_id = uuid.uuid4()
    dbs = []
    stores = []
    for _ in range(2):
        db = PostgresDB(connection_string=migrated_dsn)
        await db.connect()
        dbs.append(db)
        stores.append(
            RecallStore(
                db=db,
                embedding_service=_StubEmbedding(),
                job_id=uuid.uuid4(),
                config=_ProjectMemoryConfig(),
                project_id=project_id,
            )
        )
    memory_health.reset()
    try:
        yield SimpleNamespace(stores=stores, project_id=project_id, db=dbs[0])
    finally:
        memory_health.reset()
        for db in dbs:
            await db.close()


async def _seed_project_memories(db, project_id, n=30):
    """Seed n project memories; every other row pinned by an older pod
    (remaining_turns=5)."""
    ids = []
    for i in range(n):
        mem_id = await db.fetchval(
            """
            INSERT INTO memories (
                job_id, project_id, content, summary, keywords,
                embedding, sparse_keywords, importance, token_count,
                remaining_turns
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, to_tsvector('english', $3), $7, $8, $9
            )
            RETURNING id
            """,
            uuid.uuid4(),
            project_id,
            f"shared project fact {i} about the parser pipeline",
            f"fact {i}",
            ["parser", "pipeline"],
            [0.0] * DIMS,
            0.8,
            10,
            5 if i % 2 == 0 else None,
        )
        ids.append(mem_id)
    return ids


@pytest.mark.asyncio
async def test_concurrent_same_project_turns_stay_clean(env):
    """Two consumers looping retrieve over one project corpus: no unhandled
    errors, every turn yields results, pinned rows are found like the rest
    (D36), and nothing was contained (no deadlocks on the access-stat write).
    """
    ids = await _seed_project_memories(env.db, env.project_id, n=30)
    pinned = set(ids[::2])

    async def consumer(store):
        per_turn = []
        seen = set()
        for _ in range(TURNS):
            memories = await store.retrieve("parser pipeline")
            assert isinstance(memories, list)
            per_turn.append(len(memories))
            seen.update(m.id for m in memories)
        return per_turn, seen

    results = await asyncio.gather(*(consumer(s) for s in env.stores))

    assert all(len(per_turn) == TURNS for per_turn, _ in results)
    assert all(count > 0 for per_turn, _ in results for count in per_turn)
    # Nothing ticks a pin any more, and search returns the pinned rows.
    assert all(pinned <= seen for _, seen in results)
    remaining = await env.db.fetch(
        "SELECT remaining_turns FROM memories WHERE project_id = $1",
        env.project_id,
    )
    assert (
        sorted(row["remaining_turns"] or 0 for row in remaining) == [0] * 15 + [5] * 15
    )
    # The crux: zero contained deadlocks/errors across both consumers.
    assert memory_health.snapshot() is None


@pytest.mark.asyncio
async def test_concurrent_access_stats_opposite_orders(env):
    """Hammer _record_access_stats with overlapping id sets passed in opposite
    orders: the internal sort aligns lock order, so every bump lands and
    nothing is contained.
    """
    ids = await _seed_project_memories(env.db, env.project_id, n=20)
    overlap = ids[5:15]
    set_a = ids[:15]
    set_b = list(reversed(ids[5:]))

    async def hammer(store, id_list):
        for _ in range(TURNS):
            await store._record_access_stats(list(id_list))

    await asyncio.gather(
        hammer(env.stores[0], set_a),
        hammer(env.stores[1], set_b),
    )

    rows = await env.db.fetch(
        "SELECT id, access_count FROM memories WHERE id = ANY($1)", overlap
    )
    assert len(rows) == len(overlap)
    # Exactly one bump per hammer iteration from each side — a contained
    # (swallowed) failure would leave a shortfall here and a nonzero counter.
    assert all(row["access_count"] == 2 * TURNS for row in rows)
    assert memory_health.snapshot() is None
