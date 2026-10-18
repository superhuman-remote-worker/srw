"""The shipped local overlay's one-key OpenRouter seed, end to end.

``deployment/values-local.yaml.example`` promises that pasting one OpenRouter
key is enough for jobs and sessions to start. This renders the seed payload
the chart builds from that file, runs the real seeder against PostgreSQL, and
asks the readiness gate. It also pins the two dev-loop behaviours the file's
comments describe: an empty key seeds nothing (and leaves nothing stuck), and
a corrected key replaces the stored one on the next run.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
import yaml
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.seed.llm_config import seed
from orchestrator.services import readiness

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "deployment/values-local.yaml.example"
CHAT = "openrouter/anthropic/claude-sonnet-5.5"
EMBEDDING = "openrouter/qwen/qwen3-embedding-8b"
RERANK = "openrouter/qwen/qwen3-reranker-8b"


def _rendered_payload() -> dict:
    result = subprocess.run(
        [
            "helm",
            "template",
            "srw",
            str(ROOT / "helm"),
            "-n",
            "srw",
            "-f",
            str(EXAMPLE),
            "--set",
            "global.domain=localhost",
            "--show-only",
            "templates/orchestrator/llm-seed-configmap.yaml",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    configmap = yaml.safe_load(result.stdout)
    return yaml.safe_load(configmap["data"]["llm.yaml"])


@pytest.fixture(scope="module")
def payload():
    if shutil.which("helm") is None:
        pytest.skip("Helm is not installed")
    return _rendered_payload()


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
async def db(pg_dsn, schema_applied, monkeypatch):
    # Startup seeds the worker/session expert defaults; this test is about the
    # model half of the gate only.
    monkeypatch.setenv("EXPERTS_DB_ENABLED", "false")
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=4)
    await store.connect()
    try:
        await store.execute(
            "TRUNCATE system_settings, system_api_keys, models, llm_endpoints CASCADE"
        )
        yield store
    finally:
        await store.close()


async def _run_job(db, payload):
    """What the Helm seed Job does: seed, auto-pin, record the manifest."""
    return await seed(db, payload, record_manifest=True, auto_pin=True)


def test_the_example_declares_one_openrouter_key_and_the_four_roles(payload):
    assert payload["systemApiKeys"] == [
        {
            "provider": "openrouter",
            "apiKeyEnv": "SEED_OPENROUTER_API_KEY",
            "reconcile": True,
        }
    ]
    assert not payload.get("systemEndpoints")
    rows = {m["id"]: m for m in payload["systemModels"]}
    assert {m["provider"] for m in rows.values()} == {"openrouter"}
    assert rows[CHAT]["capabilities"] == ["chat", "auxiliary"]
    assert rows[EMBEDDING]["capability"] == "embedding"
    assert rows[RERANK]["capability"] == "rerank"
    assert payload["defaults"] == {
        "chat": CHAT,
        "auxiliary": CHAT,
        "embedding": EMBEDDING,
        "rerank": RERANK,
    }


@pytest.mark.asyncio
async def test_an_empty_key_seeds_nothing_and_the_gate_says_why(
    db, payload, monkeypatch
):
    monkeypatch.setenv("SEED_OPENROUTER_API_KEY", "")

    report = await _run_job(db, payload)
    state = await readiness.compute_readiness(db)

    assert report.api_keys_seeded == []
    assert report.models_seeded == []
    assert await db.list_models() == []
    assert state["ready"] is False
    assert state["missing_providers"] == ["any"]
    assert state["missing_capabilities"] == ["chat", "embedding", "auxiliary", "rerank"]


@pytest.mark.asyncio
async def test_one_openrouter_key_makes_the_gate_ready(db, payload, monkeypatch):
    # First install with the key still empty, then the key pasted and
    # `helm upgrade`: the second run must seed everything the first skipped.
    monkeypatch.setenv("SEED_OPENROUTER_API_KEY", "")
    await _run_job(db, payload)
    monkeypatch.setenv("SEED_OPENROUTER_API_KEY", "sk-or-v1-first")

    report = await _run_job(db, payload)
    state = await readiness.compute_readiness(db)

    assert report.api_keys_seeded == ["openrouter"]
    assert sorted(model for _, model in report.models_seeded) == sorted(
        [CHAT, EMBEDDING, RERANK]
    )
    assert state == {
        "ready": True,
        "missing_providers": [],
        "missing_capabilities": [],
        "missing_defaults": [],
        "missing_expert_defaults": [],
        "optional_capability_fallbacks": {
            "vision": "use_chat",
            "whisper": None,
            "tts": None,
        },
    }
    for capability, model in (
        ("chat", CHAT),
        ("auxiliary", CHAT),
        ("embedding", EMBEDDING),
        ("rerank", RERANK),
    ):
        assert await db.get_default_llm_model(capability) == model
        assert await db.resolve_default_for_capability(capability) == model
    rows = await db.list_models()
    assert {(r["provider_kind"], r["provider_ref"]) for r in rows} == {
        ("system", "openrouter")
    }
    # Declared pins, not the automatic fallback.
    assert report.defaults_auto_pinned == []


@pytest.mark.asyncio
async def test_a_corrected_key_replaces_the_stored_one(db, payload, monkeypatch):
    monkeypatch.setenv("SEED_OPENROUTER_API_KEY", "sk-or-v1-typo")
    await _run_job(db, payload)
    monkeypatch.setenv("SEED_OPENROUTER_API_KEY", "sk-or-v1-fixed")

    report = await _run_job(db, payload)

    assert await db.get_system_api_key("openrouter") == "sk-or-v1-fixed"
    assert ("systemApiKeys", "openrouter") in report.reconciled
    # Models and pins stay insert-only: nothing else was rewritten.
    assert report.models_seeded == []
    assert report.defaults_seeded == []
