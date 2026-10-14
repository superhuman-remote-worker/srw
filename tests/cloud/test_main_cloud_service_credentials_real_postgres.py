"""``main_cloud_service_credentials`` against a real PostgreSQL (migration
0549): the first-writer-wins insert, the compare-and-set replace, the
constraints, and two orchestrator replicas adopting agent-service's app
password at once. Guarded testcontainers fixture (skips without a container
runtime); applies ``schema_current.sql``, so a stale snapshot fails here.
"""

from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path

import asyncpg
import httpx
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.security import crypto
from orchestrator.services.cloud import service_credentials
from orchestrator.services.cloud.config import NextcloudSettings
from orchestrator.services.cloud.nextcloud import NextcloudBackend

_SCHEMA = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)
INSTANCE = "88888888-8888-4888-8888-888888888888"
UNKNOWN_INSTANCE = "99999999-9999-4999-8999-999999999999"
AGENT_USER = "agent-service"
LOGIN_PASSWORD = "agent-login-password"


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:
        pytest.skip(f"local PostgreSQL container unavailable: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(_SCHEMA.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied, monkeypatch):
    monkeypatch.setenv("APP_ENCRYPTION_KEY", "S" * 32)
    crypto.reset_cipher_cache()
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=6)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE main_cloud_service_credentials, main_cloud_active_backend, "
            "main_cloud_backend_instances CASCADE"
        )
        await conn.execute(
            """
            INSERT INTO main_cloud_backend_instances (
                id, backend_id, routing, routing_sha256,
                installation_proof_sha256, secret_refs
            ) VALUES ($1::uuid, 'nextcloud', $2::jsonb, $3, $4, $5::jsonb)
            """,
            INSTANCE,
            json.dumps({"version": 1, "backend_id": "nextcloud"}),
            "a" * 64,
            "b" * 64,
            json.dumps({"agent_password": "env:NEXTCLOUD_AGENT_PASSWORD"}),
        )
    try:
        yield store
    finally:
        await store.close()
        crypto.reset_cipher_cache()


async def _put(db: PostgresDB, secret: str, expected: int | None) -> bool:
    return await db.put_main_cloud_service_credential(
        INSTANCE,
        AGENT_USER,
        ciphertext=crypto.encrypt(secret),
        token_name=f"SuperhumanRemoteWorker agent-service {INSTANCE}",
        expected_revision=expected,
    )


@pytest.mark.asyncio
async def test_the_first_insert_wins_and_a_replace_is_compare_and_set(db):
    assert await db.get_main_cloud_service_credential(INSTANCE, AGENT_USER) is None
    assert await _put(db, "first", None) is True
    assert await _put(db, "second", None) is False

    row = await db.get_main_cloud_service_credential(INSTANCE, AGENT_USER)
    assert row["revision"] == 1
    assert row["backend_instance_id"] == INSTANCE
    assert row["token_name"] == f"SuperhumanRemoteWorker agent-service {INSTANCE}"
    assert crypto.decrypt(row["credential_ciphertext"]) == "first"

    assert await _put(db, "stale", 7) is False
    assert await _put(db, "third", 1) is True
    assert await _put(db, "lost", 1) is False
    row = await db.get_main_cloud_service_credential(INSTANCE, AGENT_USER)
    assert row["revision"] == 2
    assert crypto.decrypt(row["credential_ciphertext"]) == "third"
    assert row["updated_at"] >= row["created_at"]
    assert await db.get_main_cloud_service_credential(INSTANCE, "admin") is None
    assert await db.get_main_cloud_service_credential("not-a-uuid", AGENT_USER) is None


@pytest.mark.asyncio
async def test_constraints_refuse_plaintext_and_unknown_installations(db):
    with pytest.raises(asyncpg.CheckViolationError):
        await db.put_main_cloud_service_credential(
            INSTANCE,
            AGENT_USER,
            ciphertext="plaintext-app-password",
            token_name="t",
            expected_revision=None,
        )
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await db.put_main_cloud_service_credential(
            UNKNOWN_INSTANCE,
            AGENT_USER,
            ciphertext=crypto.encrypt("x"),
            token_name="t",
            expected_revision=None,
        )


class _FakeNextcloud:
    """Mints a token per call; both replicas mint before either stores."""

    def __init__(self) -> None:
        self.minted: list[str] = []
        self.revoked: list[str] = []
        self.both_minted = asyncio.Barrier(2)

    async def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/ocs/v2.php/core/getapppassword":
            token = f"app-password-{len(self.minted) + 1}"
            self.minted.append(token)
            try:
                await asyncio.wait_for(self.both_minted.wait(), 5)
            except (TimeoutError, asyncio.BrokenBarrierError):
                pass
            return httpx.Response(200, json={"ocs": {"data": {"apppassword": token}}})
        if request.url.path == "/ocs/v2.php/core/apppassword":
            header = request.headers["authorization"]
            self.revoked.append(base64.b64decode(header[6:]).decode().split(":", 1)[1])
            return httpx.Response(200, json={"ocs": {"data": []}})
        return httpx.Response(201)


def _replica(fake: _FakeNextcloud) -> NextcloudBackend:
    backend = NextcloudBackend(
        NextcloudSettings(
            base_url="http://nextcloud.internal",
            public_url="https://cloud.example",
            admin_user="admin",
            admin_password="admin-password",
            agent_user=AGENT_USER,
            agent_password=LOGIN_PASSWORD,
        )
    )
    backend._agent_secret_transport = httpx.MockTransport(fake.handler)
    backend.bind_backend_instance(INSTANCE)
    return backend


@pytest.mark.asyncio
async def test_two_replicas_adopt_one_app_password(db, monkeypatch):
    monkeypatch.setitem(service_credentials._state, "store", db)
    fake = _FakeNextcloud()
    first, second = _replica(fake), _replica(fake)

    await asyncio.gather(
        first.ensure_service_credentials(), second.ensure_service_credentials()
    )

    row = await db.get_main_cloud_service_credential(INSTANCE, AGENT_USER)
    stored = crypto.decrypt(row["credential_ciphertext"])
    assert row["revision"] == 1
    assert first.webdav_credentials["password"] == stored
    assert second.webdav_credentials["password"] == stored
    # Whichever replica lost the insert revoked the token it minted.
    assert set(fake.minted) - set(fake.revoked) == {stored}
    assert len(fake.minted) == 2
    async with db.acquire() as conn:
        assert (
            await conn.fetchval("SELECT count(*) FROM main_cloud_service_credentials")
            == 1
        )
