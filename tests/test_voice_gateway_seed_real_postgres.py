"""The voice gateway's catalog rows, from the chart's payload to a live call.

Renders the seed payload the chart builds with voiceGateway on, runs the
real seeder against PostgreSQL, and reads the rows back the way a phone line
does: its engine (the gateway's in-cluster Service and key), the voice an
expert that names none gets, and the price of its seconds. Insert-only, one
row per model id, like every other seeded row.
"""

from __future__ import annotations

import shutil
import subprocess
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest
import pytest_asyncio
import yaml
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.seed.llm_config import seed
from orchestrator.services.live.admission import line_default_voice
from orchestrator.services.live.exchange import live_engine
from orchestrator.services.live.ledger import record_session_usage

ROOT = Path(__file__).resolve().parents[1]
GATEWAY = "http://srw-voice-gateway.srw.svc:18440/v1"
VALUES = {
    "fullnameOverride": "srw",
    "voiceGateway": {
        "enabled": True,
        "lines": [
            {
                "name": "reception",
                "mode": "peer",
                "pbx": "192.0.2.10",
                "connectorId": "00000000-0000-4000-8000-000000000001",
                "webhookSecret": {"name": "lines", "key": "reception"},
            }
        ],
        "profiles": {
            "cascade-gpu": {"engine": "echo"},
            "echo": {"engine": "echo"},
        },
        "catalog": {
            "models": {"cascade-gpu": {"ratePerMinuteUsd": 0.03, "voice": "de-1"}}
        },
    },
    "asterisk": {"enabled": True, "nodeSelector": {"voice": "yes"}},
}


@pytest.fixture(scope="module")
def payload(tmp_path_factory):
    if shutil.which("helm") is None:
        pytest.skip("Helm is not installed")
    values = tmp_path_factory.mktemp("voice") / "values.yaml"
    values.write_text(yaml.safe_dump(VALUES))
    result = subprocess.run(
        [
            "helm",
            "template",
            "srw",
            str(ROOT / "helm"),
            "-n",
            "srw",
            "-f",
            str(ROOT / "helm/ci/test-values.yaml"),
            "-f",
            str(values),
            "--show-only",
            "templates/orchestrator/llm-seed-configmap.yaml",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    return yaml.safe_load(yaml.safe_load(result.stdout)["data"]["llm.yaml"])


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
    monkeypatch.setenv("EXPERTS_DB_ENABLED", "false")
    monkeypatch.setenv("SEED_VOICE_GATEWAY_API_KEY", "voice-gateway-test-key")
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
    """What the Helm seed Job does."""
    return await seed(db, payload, record_manifest=True, auto_pin=True)


@pytest.mark.asyncio
async def test_each_profile_is_a_live_voice_model_at_the_gateway(db, payload):
    report = await _run_job(db, payload)
    assert report.endpoints_seeded == ["SRW voice gateway"]
    for model in ("cascade-gpu", "echo"):
        engine = await live_engine(db, model)
        assert engine == {
            "api_key": "voice-gateway-test-key",
            "api_base": GATEWAY,
            "ws_base": "ws://srw-voice-gateway.srw.svc:18440/v1",
            "model": model,
        }
    rows = {row["model_id"]: row for row in await db.list_models()}
    assert {name: row["capabilities"] for name, row in rows.items()} == {
        "cascade-gpu": ["live_voice"],
        "echo": ["live_voice"],
    }


@pytest.mark.asyncio
async def test_the_rows_name_the_voice_and_price_of_their_profile(db, payload):
    await _run_job(db, payload)
    assert await line_default_voice(db, {"config": {"model": "cascade-gpu"}}) == "de-1"
    assert await line_default_voice(db, {"config": {"model": "echo"}}) is None

    ledger = Mock(is_available=True)
    ledger.resolve_rate = AsyncMock(return_value=None)
    ledger.record_events = AsyncMock(return_value=1)
    for model, cost in (("cascade-gpu", Decimal("0.06")), ("echo", Decimal(0))):
        session = {
            "id": "00000000-0000-4000-8000-0000000000aa",
            "model": model,
            "usage_seconds": 120,
            "ended_at": datetime(2026, 10, 21, tzinfo=timezone.utc),
            "job_id": None,
        }
        await record_session_usage(ledger, db, session)
        event = ledger.record_events.await_args.args[0][0]
        assert event.cost_usd == cost, model


@pytest.mark.asyncio
async def test_seeding_again_changes_nothing_an_admin_set(db, payload):
    await _run_job(db, payload)
    (endpoint,) = [
        row
        for row in await db.list_system_llm_endpoints()
        if row["label"] == "SRW voice gateway"
    ]
    await db.execute(
        "UPDATE models SET display_label = 'Renamed by an admin' WHERE model_id = 'echo'"
    )
    report = await _run_job(db, payload)
    assert report.endpoints_seeded == []
    rows = {row["model_id"]: row for row in await db.list_models()}
    assert rows["echo"]["display_label"] == "Renamed by an admin"
    assert [
        row["id"]
        for row in await db.list_system_llm_endpoints()
        if row["label"] == "SRW voice gateway"
    ] == [endpoint["id"]]


@pytest.mark.asyncio
async def test_a_profile_named_like_another_providers_model_is_skipped(db, payload):
    """One row per model id: a profile cannot take over a model id another
    endpoint already serves."""
    taken = await db.create_system_llm_endpoint(
        label="Hosted engine",
        base_url="https://engine.example/v1",
        api_key="hosted-key",
        key_prefix="hosted-k",
    )
    await db.create_model(
        provider_kind="endpoint",
        provider_ref=str(taken["id"]),
        model_id="echo",
        display_label="Hosted echo",
        capabilities=["live_voice"],
        family="default",
    )
    await _run_job(db, payload)
    engine = await live_engine(db, "echo")
    assert engine["api_base"] == "https://engine.example/v1"
    assert (await live_engine(db, "cascade-gpu"))["api_base"] == GATEWAY
