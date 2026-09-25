"""Normal job deletion cannot race a delayed vector writer across databases."""

from __future__ import annotations


import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI, HTTPException
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from testcontainers.postgres import PostgresContainer

from orchestrator.database.migrate import run_migrations
from orchestrator.database.postgres import PostgresDB
from orchestrator.routers import job_lifecycle as job_lifecycle_routes
from orchestrator.application import controls as controls_composition
from orchestrator.security import access as access_module
from orchestrator.services import (
    job_freeze_notifications as job_freeze_notifications_module,
)
from orchestrator.services import snapshot_service as snapshot_service_module
from orchestrator.services import thread_retirement as thread_retirement_module
import functools


MIGRATIONS = (
    Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations"
)


@pytest.fixture(scope="module")
def vector_dsn():
    with PostgresContainer("pgvector/pgvector:pg15") as container:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )


@pytest_asyncio.fixture(scope="module")
async def migrated(vector_dsn):
    parts = urlsplit(vector_dsn)
    app_dsn = urlunsplit(parts._replace(path="/job_retirement_test"))
    conn = await asyncpg.connect(vector_dsn)
    try:
        await conn.execute("CREATE DATABASE job_retirement_test")
    finally:
        await conn.close()
    for dsn, family in ((vector_dsn, "vector"), (app_dsn, "app")):
        async with asyncpg.create_pool(dsn, min_size=1, max_size=2) as pool:
            await run_migrations(pool, MIGRATIONS / family)
    async with AsyncPostgresSaver.from_conn_string(app_dsn) as saver:
        await saver.setup()
    return app_dsn, vector_dsn


@pytest_asyncio.fixture
async def stores(migrated):
    app, vector = (
        PostgresDB(connection_string=dsn, min_connections=1, max_connections=4)
        for dsn in migrated
    )
    await app.connect()
    await vector.connect()
    try:
        yield app, vector
    finally:
        await app.close()
        await vector.close()


@pytest_asyncio.fixture
async def client(stores, monkeypatch):
    import orchestrator.main as main

    app_db, vector_db = stores

    async def access(_request, _db, job_id):
        job = await app_db.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404)
        return {"id": str(uuid4()), "is_admin": True}, job

    monkeypatch.setattr(main.app.state.resources, "postgres_db", app_db)
    monkeypatch.setattr(main.app.state.resources, "vector_db", vector_db)
    monkeypatch.setattr(access_module, "require_job_access", access)
    monkeypatch.setattr(
        thread_retirement_module.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        AsyncMock(),
    )
    monkeypatch.setattr(
        job_freeze_notifications_module,
        "resolve_job_notifications",
        AsyncMock(),
    )
    monkeypatch.setattr(
        snapshot_service_module, "snapshot_service", SimpleNamespace(is_available=False)
    )
    app = FastAPI()
    app.state.job_control_route_dependencies_factory = functools.partial(
        controls_composition.job_mutation_route_dependencies, main.app.state.resources
    )
    app.add_api_route(
        "/api/jobs/{job_id}", job_lifecycle_routes.delete_job, methods=["DELETE"]
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        yield http


async def _job(app):
    async with app.acquire() as conn:
        job = await conn.fetchval(
            "INSERT INTO jobs (description,status,execution_lane) "
            "VALUES ('owned vector retirement fixture','completed','stateless') RETURNING id"
        )
        await conn.execute(
            "INSERT INTO run_queue (unit_id,unit_kind,state,lease_token) "
            "VALUES ($1,'worker_batch','done',1)",
            job,
        )
    return job


async def _memory(conn, owner):
    return await conn.fetchval(
        "INSERT INTO memories (job_id,content) VALUES ($1,'synthetic memory') RETURNING id",
        owner,
    )


@pytest.mark.asyncio
async def test_normal_delete_waits_for_inflight_memory_and_rejects_late_writer(
    stores, client
):
    app, vector = stores
    job = await _job(app)
    retained = uuid4()  # A session/project memory destination is independent.
    async with vector.acquire() as conn:
        retained_memory = await _memory(conn, retained)
    deletion = None
    try:
        async with vector.acquire() as writer:
            async with writer.transaction():
                memory = await _memory(writer, job)
                await writer.execute(
                    "INSERT INTO memory_retrieval_messages (memory_id,message) VALUES ($1,'synthetic retrieval')",
                    memory,
                )
                deletion = asyncio.create_task(client.delete(f"/api/jobs/{job}"))
                deadline = asyncio.get_running_loop().time() + 10
                waiting = False
                while (
                    not deletion.done() and asyncio.get_running_loop().time() < deadline
                ):
                    async with vector.acquire() as observer:
                        waiting = await observer.fetchval(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() "
                            "AND query LIKE '%retire_job_vector_scope%' AND cardinality(pg_blocking_pids(pid)) > 0)"
                        )
                    if waiting:
                        break
                    await asyncio.sleep(0.01)
                detail = "request still running"
                if deletion.done():
                    early = await deletion
                    detail = f"HTTP {early.status_code}: {early.text}"
                assert waiting, (
                    "DELETE returned before the in-flight memory transaction settled: "
                    + detail
                )
                assert await app.get_job(str(job)) is not None
        response = await asyncio.wait_for(deletion, timeout=10)
        assert response.status_code == 200, response.text
        assert await app.get_job(str(job)) is None
        async with vector.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM memories WHERE job_id=$1", job
                )
                == 0
            )
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM memory_retrieval_messages WHERE memory_id=$1",
                    memory,
                )
                == 0
            )
            assert (
                await conn.fetchval(
                    "SELECT id FROM memories WHERE id=$1", retained_memory
                )
                == retained_memory
            )
            with pytest.raises(asyncpg.CheckViolationError) as error:
                await _memory(conn, job)
            assert error.value.constraint_name == "job_vector_scope_retired"
    finally:
        if deletion is not None:
            await asyncio.gather(deletion, return_exceptions=True)


INSERTS = {
    "memories": "INSERT INTO memories (job_id,content) VALUES ($1,'synthetic')",
    "job_sources": "INSERT INTO job_sources (job_id,source_id) VALUES ($1,$2)",
    "citations": "INSERT INTO citations (job_id,claim,quote_context,source_id,locator) VALUES ($1,'synthetic','claim',$2,'{}')",
    "source_annotations": "INSERT INTO source_annotations (job_id,source_id,content) VALUES ($1,$2,'note')",
    "source_tags": "INSERT INTO source_tags (job_id,source_id,tag) VALUES ($1,$2,'tag')",
    "source_embeddings": "INSERT INTO source_embeddings (job_id,source_id,chunk_text) VALUES ($1,$2,'chunk')",
}


async def _source(conn):
    return await conn.fetchval(
        "INSERT INTO sources (type,identifier,name,content) "
        "VALUES ('custom','synthetic','retained source','synthetic content') RETURNING id"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("table", INSERTS)
async def test_all_owned_vector_tables_refuse_late_insert_or_scope_transfer(
    stores, client, table
):
    app, vector = stores
    job = await _job(app)
    retained = uuid4()
    async with vector.acquire() as conn:
        source = await _source(conn)
        for owner in (job, retained):
            args = (owner,) if table == "memories" else (owner, source)
            await conn.execute(INSERTS[table], *args)
    assert (await client.delete(f"/api/jobs/{job}")).status_code == 200
    async with vector.acquire() as conn:
        assert (
            await conn.fetchval(f"SELECT count(*) FROM {table} WHERE job_id=$1", job)
            == 0
        )
        assert (
            await conn.fetchval(
                f"SELECT count(*) FROM {table} WHERE job_id=$1", retained
            )
            == 1
        )
        assert (
            await conn.fetchval("SELECT id FROM sources WHERE id=$1", source) == source
        )
        args = (job,) if table == "memories" else (job, source)
        with pytest.raises(asyncpg.CheckViolationError) as error:
            await conn.execute(INSERTS[table], *args)
        assert error.value.constraint_name == "job_vector_scope_retired"
        with pytest.raises(asyncpg.CheckViolationError) as error:
            await conn.execute(
                f"UPDATE {table} SET job_id=$1 WHERE job_id=$2", job, retained
            )
        assert error.value.constraint_name == "job_vector_scope_retired"


@pytest.mark.asyncio
async def test_lost_vector_commit_response_retains_job_and_replays_exact_fence(
    stores, client, monkeypatch
):
    app, vector = stores
    job = await _job(app)
    original_acquire = vector.acquire

    @asynccontextmanager
    async def lost_response():
        async with original_acquire() as conn:

            async def execute(query, *args):
                await conn.execute(query, *args)
                raise ConnectionError("synthetic lost commit response")

            yield SimpleNamespace(execute=execute)

    monkeypatch.setattr(vector, "acquire", lost_response)
    assert (await client.delete(f"/api/jobs/{job}")).status_code == 503
    monkeypatch.setattr(vector, "acquire", original_acquire)
    assert await app.get_job(str(job)) is not None
    async with vector.acquire() as conn:
        retired_at = await conn.fetchval(
            "SELECT retired_at FROM job_vector_scopes WHERE job_id=$1", job
        )
        assert retired_at is not None
        with pytest.raises(asyncpg.CheckViolationError):
            await _memory(conn, job)
    assert (await client.delete(f"/api/jobs/{job}")).status_code == 200
    assert await app.get_job(str(job)) is None
    async with vector.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT retired_at FROM job_vector_scopes WHERE job_id=$1", job
            )
            == retired_at
        )
        for query in (
            "UPDATE job_vector_scopes SET retired_at=NULL WHERE job_id=$1",
            "DELETE FROM job_vector_scopes WHERE job_id=$1",
        ):
            with pytest.raises(asyncpg.CheckViolationError) as error:
                await conn.execute(query, job)
            assert (
                error.value.constraint_name == "job_vector_scope_retirement_immutable"
            )


@pytest.mark.asyncio
async def test_failure_during_vector_prune_rolls_back_fence_and_keeps_api_retry_handle(
    stores, client
):
    app, vector = stores
    job = await _job(app)
    async with vector.acquire() as conn:
        memory = await _memory(conn, job)
        source = await _source(conn)
        await conn.execute(INSERTS["job_sources"], job, source)
        await conn.execute("""
            CREATE FUNCTION public.test_refuse_vector_delete() RETURNS TRIGGER
            LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected vector failure'; END $$;
            CREATE TRIGGER test_refuse_vector_delete BEFORE DELETE ON job_sources
            FOR EACH ROW EXECUTE FUNCTION public.test_refuse_vector_delete();
        """)
    try:
        assert (await client.delete(f"/api/jobs/{job}")).status_code == 503
        assert await app.get_job(str(job)) is not None
        async with vector.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT retired_at FROM job_vector_scopes WHERE job_id=$1", job
                )
                is None
            )
            assert (
                await conn.fetchval("SELECT id FROM memories WHERE id=$1", memory)
                == memory
            )
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM job_sources WHERE job_id=$1", job
                )
                == 1
            )
    finally:
        async with vector.acquire() as conn:
            await conn.execute(
                "DROP TRIGGER test_refuse_vector_delete ON job_sources; DROP FUNCTION public.test_refuse_vector_delete()"
            )
    assert (await client.delete(f"/api/jobs/{job}")).status_code == 200
    assert await app.get_job(str(job)) is None


@pytest.mark.asyncio
async def test_retirement_wins_before_first_writer_without_blocking_other_scopes(
    stores,
):
    _app, vector = stores
    job, retained = uuid4(), uuid4()

    async def write():
        async with vector.acquire() as conn:
            return await _memory(conn, job)

    late = None
    try:
        async with vector.acquire() as retiring:
            async with retiring.transaction():
                await retiring.execute("SELECT retire_job_vector_scope($1::uuid)", job)
                late = asyncio.create_task(write())
                deadline = asyncio.get_running_loop().time() + 5
                waiting = False
                while asyncio.get_running_loop().time() < deadline:
                    async with vector.acquire() as observer:
                        waiting = await observer.fetchval(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() "
                            "AND query LIKE 'INSERT INTO memories%' AND cardinality(pg_blocking_pids(pid)) > 0)"
                        )
                    if waiting:
                        break
                    await asyncio.sleep(0.01)
                assert waiting and not late.done()
                async with vector.acquire() as unrelated:
                    await asyncio.wait_for(_memory(unrelated, retained), timeout=2)
        with pytest.raises(asyncpg.CheckViolationError) as error:
            await asyncio.wait_for(late, timeout=5)
        assert error.value.constraint_name == "job_vector_scope_retired"
    finally:
        if late is not None:
            await asyncio.gather(late, return_exceptions=True)
