"""One catalog row per model id, against real PostgreSQL (the current schema).

A model id names one row across every provider (``uq_models_model_id``), a
row may send another name upstream (``served_model_id``), and migration 0552
renamed the duplicates an upgrade found without moving any traffic. See
knowledge-base/knowledge/issues/same_model_id_on_two_providers_routes_to_the_api_key_row.md.
"""

import re
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_0552 = (
    ROOT / "src/orchestrator/database/migrations/app/0552_models_served_model_id.sql"
)


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture(scope="module")
async def schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            (ROOT / "src/orchestrator/database/schema_current.sql").read_text()
        )
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, schema_applied):
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=4)
    await store.connect()
    try:
        await store.execute("TRUNCATE system_settings, models, llm_endpoints CASCADE")
        yield store
    finally:
        await store.close()


async def _endpoint(db, label):
    row = await db.create_system_llm_endpoint(
        label=label, base_url="https://proxy.invalid/v1", api_key=None, key_prefix=None
    )
    return str(row["id"])


def _row(provider_kind, provider_ref, model_id, **extra):
    return dict(
        provider_kind=provider_kind,
        provider_ref=provider_ref,
        model_id=model_id,
        display_label=model_id,
        capabilities=["chat", "auxiliary"],
        family="claude-opus-5-5",
        **extra,
    )


@pytest.mark.asyncio
async def test_a_second_provider_cannot_take_the_id(db):
    proxy = await _endpoint(db, "Claude subscription proxy")
    await db.create_model(**_row("endpoint", proxy, "claude-opus-5-5"))
    with pytest.raises(asyncpg.UniqueViolationError, match="uq_models_model_id"):
        await db.create_model(**_row("system", "anthropic", "claude-opus-5-5"))
    # The seeders' skip: any conflict is "already there", not a crash.
    assert (
        await db.create_model(
            **_row("system", "anthropic", "claude-opus-5-5"),
            on_conflict_do_nothing=True,
        )
        is None
    )
    holder = await db.find_model_by_model_id("claude-opus-5-5")
    assert (holder["provider_kind"], holder["provider_ref"]) == ("endpoint", proxy)


@pytest.mark.asyncio
async def test_each_row_resolves_to_its_own_transport_and_served_name(db):
    proxy = await _endpoint(db, "Claude subscription proxy")
    await db.create_model(**_row("endpoint", proxy, "claude-opus-5-5"))
    await db.create_model(
        **_row(
            "endpoint",
            await _endpoint(db, "Second proxy"),
            "claude-opus-5-5-second",
            served_model_id="claude-opus-5-5",
        )
    )
    first = await db.resolve_catalog_model("claude-opus-5-5")
    second = await db.resolve_catalog_model("claude-opus-5-5-second")
    assert first["endpoint_id"] is not None and second["endpoint_id"] is not None
    assert str(first["endpoint_id"]) == proxy
    assert str(second["endpoint_id"]) != proxy
    assert first["served_model_id"] is None
    assert second["served_model_id"] == "claude-opus-5-5"


def _rename_block() -> str:
    """0552's rename, the only part of it that touches rows."""
    text = MIGRATION_0552.read_text()
    match = re.search(r"DO \$\$.*?\$\$;", text, re.S)
    assert match, "0552 lost its DO block"
    return match.group(0)


@pytest.mark.asyncio
async def test_0552_keeps_the_routing_row_and_renames_the_rest(db):
    """The upgrade rename on duplicates written before the unique index."""
    proxy = await _endpoint(db, "Claude subscription proxy")
    local = await _endpoint(db, "  Local verified embedding!! ")
    await db.execute("DROP INDEX uq_models_model_id")
    try:
        await db.execute(
            """
            INSERT INTO models (provider_kind, provider_ref, model_id, display_label,
                                capabilities, family, enabled, created_at)
            VALUES
              -- dev's shape: the proxy routes, the API-key row is disabled
              ('endpoint', $1, 'claude-opus-5-5', 'p', '{chat}', 'f', TRUE,  '2026-09-28'),
              ('system', 'anthropic', 'claude-opus-5-5', 'a', '{chat}', 'f', FALSE, '2026-10-16'),
              -- both enabled: the system row is what resolved
              ('endpoint', $2, 'qwen3-reranker-8b', 'l', '{rerank}', 'q', TRUE, '2026-01-01'),
              ('system', 'openrouter', 'qwen3-reranker-8b', 'o', '{rerank}', 'q', TRUE, '2026-02-01'),
              -- the suffixed name is already taken
              ('system', 'openai', 'gpt-x', 'g', '{chat}', 'f', TRUE, '2026-01-01'),
              ('endpoint', $1, 'gpt-x', 'g', '{chat}', 'f', TRUE, '2026-01-02'),
              ('system', 'minimax', 'gpt-x-claude-subscription-proxy', 'm', '{chat}', 'f', TRUE, '2026-01-03')
            """,
            proxy,
            local,
        )
        await db.execute(_rename_block())
        rows = {
            (r["provider_kind"], r["provider_ref"], r["served_model_id"]): r["model_id"]
            for r in await db.list_models()
        }
    finally:
        await db.execute("TRUNCATE models CASCADE")
        await db.execute(
            "CREATE UNIQUE INDEX uq_models_model_id ON public.models(model_id)"
        )
    assert rows == {
        ("endpoint", proxy, None): "claude-opus-5-5",
        ("system", "anthropic", "claude-opus-5-5"): "claude-opus-5-5-anthropic",
        ("system", "openrouter", None): "qwen3-reranker-8b",
        ("endpoint", local, "qwen3-reranker-8b"): (
            "qwen3-reranker-8b-local-verified-embedding"
        ),
        ("system", "openai", None): "gpt-x",
        ("endpoint", proxy, "gpt-x"): "gpt-x-claude-subscription-proxy-2",
        ("system", "minimax", None): "gpt-x-claude-subscription-proxy",
    }
