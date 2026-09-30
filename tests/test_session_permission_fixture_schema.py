"""Keep the permission fixture's queue column aligned with the real schema."""

import ast
from pathlib import Path

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer


@pytest.mark.asyncio
async def test_permission_fixture_park_reason_matches_production_schema():
    root = Path(__file__).resolve().parents[1]
    tree = ast.parse(
        (root / "tests/test_session_permission_retirement_real_postgres.py").read_text()
    )
    fixture = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "pg_pool"
    )
    ddl = next(
        node.args[0].value
        for node in ast.walk(fixture)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "execute"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    )
    with PostgresContainer("pgvector/pgvector:pg15").with_kwargs(
        mem_limit="1g", nano_cpus=2_000_000_000
    ) as postgres:
        dsn = postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")
        conn = await asyncpg.connect(dsn)
        try:
            golden = conn.transaction()
            await golden.start()
            try:
                await conn.execute(
                    (root / "src/orchestrator/database/schema_current.sql").read_text()
                )
                expected = await conn.fetchrow(
                    "SELECT data_type,is_nullable FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name='run_queue' AND column_name='park_reason'"
                )
                assert expected is not None
            finally:
                await golden.rollback()
            try:
                await conn.execute(ddl)
            except asyncpg.PostgresError as exc:
                pytest.fail(
                    f"permission fixture cannot create its schema: {type(exc).__name__}",
                    pytrace=False,
                )
            actual = await conn.fetchrow(
                "SELECT data_type,is_nullable FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name='run_queue' AND column_name='park_reason'"
            )
            assert actual == expected
        finally:
            await conn.close()
