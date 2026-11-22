"""The llm_endpoints TLS pin columns against real PostgreSQL.

Pins: the format and pair CHECKs, and that the three endpoint reads plus
resolve_catalog_model return the pin (NULL for an unpinned row).
"""

from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB

ROOT = Path(__file__).resolve().parents[1]
FINGERPRINT = ":".join(["AB"] * 32)
PIN_KEYS = (
    "tls_pin_sha256",
    "tls_pin_cert",
    "tls_pin_subject",
    "tls_pin_issuer",
    "tls_pin_not_after",
    "tls_pinned_at",
    "tls_pinned_by",
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
        await store.execute("TRUNCATE models, llm_endpoints CASCADE")
        yield store
    finally:
        await store.close()


async def _endpoint(db, label="Pinned"):
    row = await db.create_system_llm_endpoint(
        label=label, base_url="https://10.0.0.1:8090/v1", api_key=None, key_prefix=None
    )
    return str(row["id"])


async def _pin(db, endpoint_id, sha=FINGERPRINT, cert="PEM"):
    await db.execute(
        "UPDATE llm_endpoints SET tls_pin_sha256 = $2, tls_pin_cert = $3,"
        " tls_pin_subject = 'CN=x', tls_pin_issuer = 'CN=x',"
        " tls_pin_not_after = now(), tls_pinned_at = now(),"
        " tls_pinned_by = $4::uuid WHERE id = $1::uuid",
        endpoint_id,
        sha,
        cert,
        str(uuid4()),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad", [":".join(["ab"] * 32), ":".join(["AB"] * 31), "AB" * 32]
)
async def test_format_check_rejects_malformed_fingerprint(db, bad):
    endpoint_id = await _endpoint(db)
    with pytest.raises(asyncpg.CheckViolationError):
        await _pin(db, endpoint_id, sha=bad)


@pytest.mark.asyncio
async def test_pair_check_rejects_fingerprint_without_certificate(db):
    endpoint_id = await _endpoint(db)
    with pytest.raises(asyncpg.CheckViolationError):
        await _pin(db, endpoint_id, cert=None)


@pytest.mark.asyncio
async def test_pair_check_rejects_certificate_without_fingerprint(db):
    endpoint_id = await _endpoint(db)
    with pytest.raises(asyncpg.CheckViolationError):
        await _pin(db, endpoint_id, sha=None)


@pytest.mark.asyncio
async def test_reads_return_null_pin_for_unpinned_row(db):
    endpoint_id = await _endpoint(db)
    for row in (
        await db.get_system_llm_endpoint(endpoint_id),
        (await db.list_system_llm_endpoints())[0],
    ):
        for key in PIN_KEYS:
            assert key in row and row[key] is None


@pytest.mark.asyncio
async def test_reads_return_the_pin(db):
    endpoint_id = await _endpoint(db)
    await _pin(db, endpoint_id)
    rows = [
        await db.get_system_llm_endpoint(endpoint_id),
        await db.get_user_llm_endpoint(endpoint_id),
        (await db.list_system_llm_endpoints())[0],
    ]
    for row in rows:
        assert row["tls_pin_sha256"] == FINGERPRINT
        assert row["tls_pin_cert"] == "PEM"
        assert row["tls_pin_subject"] == "CN=x"
        assert row["tls_pin_issuer"] == "CN=x"
        assert row["tls_pin_not_after"] is not None
        assert row["tls_pinned_at"] is not None
        assert row["tls_pinned_by"] is not None


@pytest.mark.asyncio
async def test_resolve_catalog_model_returns_the_pin(db):
    endpoint_id = await _endpoint(db)
    await db.create_model(
        provider_kind="endpoint",
        provider_ref=endpoint_id,
        model_id="pin-model",
        display_label="pin-model",
        capabilities=["chat"],
        family="default",
    )
    resolved = await db.resolve_catalog_model("pin-model")
    assert resolved["endpoint_tls_pin_sha256"] is None
    assert resolved["endpoint_tls_pin_cert"] is None
    await _pin(db, endpoint_id)
    resolved = await db.resolve_catalog_model("pin-model")
    assert resolved["endpoint_tls_pin_sha256"] == FINGERPRINT
    assert resolved["endpoint_tls_pin_cert"] == "PEM"
