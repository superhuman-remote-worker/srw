"""The readiness auto-pin against real PostgreSQL (the current schema).

Unit tests fake the three accessors; this pins their SQL together: a fresh
install that added one model per required capability goes from "pin a
default" to ready, the pins match what dispatch already resolves, and the
conditional write never replaces a stored pin in either value shape.
"""

import json
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services import readiness
from shared.helm_provenance import AUTO_PIN_BREADCRUMB

ROOT = Path(__file__).resolve().parents[1]


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


async def _add(db, endpoint_id, model_id, *capabilities):
    await db.create_model(
        provider_kind="endpoint",
        provider_ref=endpoint_id,
        model_id=model_id,
        display_label=model_id,
        capabilities=list(capabilities),
        family="default",
    )


@pytest.mark.asyncio
async def test_fresh_install_is_ready_after_the_auto_pin(db, monkeypatch):
    monkeypatch.setenv("EXPERTS_DB_ENABLED", "false")
    endpoint = await db.create_system_llm_endpoint(
        label="Gateway",
        base_url="https://gateway.invalid/v1",
        api_key=None,
        key_prefix=None,
    )
    ref = str(endpoint["id"])
    await _add(db, ref, "b-chat", "chat", "auxiliary")
    await _add(db, ref, "a-chat", "chat", "auxiliary")
    await _add(db, ref, "emb", "embedding")
    await _add(db, ref, "rr", "rerank")
    await _add(db, ref, "vis", "vision")

    before = await readiness.compute_readiness(db)
    pinned = await readiness.auto_pin_required_defaults(db)
    after = await readiness.compute_readiness(db)

    assert before["missing_defaults"] == ["chat", "embedding", "auxiliary", "rerank"]
    assert after["ready"] is True
    assert pinned == [
        ("chat", "a-chat"),
        ("embedding", "emb"),
        ("auxiliary", "a-chat"),
        ("rerank", "rr"),
    ]
    # No runtime change: each pin is what dispatch resolved without one.
    for capability, model_id in pinned:
        assert await db.resolve_default_for_capability(capability) == model_id
    assert await db.get_default_llm_model("vision") is None
    row = await db.get_system_setting("llm.default_rerank_model")
    assert (row["updated_by"], row["source"]) == (AUTO_PIN_BREADCRUMB, "default")
    # Idempotent: a second pass finds everything pinned.
    assert await readiness.auto_pin_required_defaults(db) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stored", "pins"),
    [
        (None, True),
        ({"model": "admin-pick"}, False),
        ("admin-pick", False),  # legacy bare-string value
        ({"model": ""}, True),
        ({}, True),
        ("", True),
    ],
)
async def test_conditional_pin_only_fills_an_empty_slot(db, stored, pins):
    key = "llm.default_chat_model"
    if stored is not None:
        await db.execute(
            "INSERT INTO system_settings (key, value, updated_by, source) "
            "VALUES ($1, $2::jsonb, 'admin-1', 'ui')",
            key,
            json.dumps(stored),
        )

    wrote = await db.pin_default_llm_model_if_unset(
        "chat", "a-chat", updated_by=AUTO_PIN_BREADCRUMB, source="default"
    )

    row = await db.get_system_setting(key)
    assert wrote is pins
    if pins:
        assert row["value"] == {"model": "a-chat"}
        assert (row["updated_by"], row["source"]) == (AUTO_PIN_BREADCRUMB, "default")
    else:
        assert row["value"] == stored
        assert (row["updated_by"], row["source"]) == ("admin-1", "ui")
