"""Real-PostgreSQL proofs for service-plane driver hosting (connector drivers D5).

Driver image resolutions and the moved-tag refusal at bind run against
``schema_current.sql``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.security.crypto import encrypt
from orchestrator.services import connector_credential_leases as leases
from orchestrator.services import connector_service_images as images
from orchestrator.services.connector_egress import private_addresses_allowed
from shared.connectors.contract import (
    AccessLevel,
    CredentialSlot,
    DriverSpec,
    ServiceSpec,
)
from shared.connectors.images import SPEC_LABEL
from shared.oci_registry import ResolvedImage

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)
DRIVER = "srw.test-service/v1"
REFERENCE = "ghcr.io/org/echo:latest"
D1 = "sha256:" + "1" * 64
D2 = "sha256:" + "2" * 64
D3 = "sha256:" + "3" * 64

SERVICE = DriverSpec(
    name=DRIVER,
    title="Test service",
    plane="service",
    delivery_forms=("lease_token",),
    config_schema={
        "type": "object",
        "additionalProperties": False,
        "properties": {"host": {"type": "string"}},
    },
    credential_slots=(
        CredentialSlot("secret", "secret_string", {"type": "object"}, required=True),
    ),
    access_levels=(AccessLevel("ReadWrite", 1, "the lease exchange"),),
    supported_backends=frozenset({"sandbox"}),
    workspace_requirements="none",
    credential_delivery="lease",
    service=ServiceSpec(),
)


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
        await conn.execute(SCHEMA_FILE.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied):
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=6)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE connector_credential_leases, connector_driver_identities, "
            "connector_driver_images, security_events, datasources, jobs, threads "
            "CASCADE"
        )
    try:
        yield store
    finally:
        await store.close()
        images.configure_service_images(images.ServiceImageSettings())


async def _connector(db, *, config: dict | None = None) -> str:
    connector_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO datasources (id, name, type, scope_mode, policy_revision, "
            "credentials, config) VALUES ($1, $2, 'echo_service', 'all', 1, "
            "$3::jsonb, $4::jsonb)",
            connector_id,
            f"echo-{str(connector_id)[:8]}",
            json.dumps(encrypt(json.dumps({"secret": "s"}))),
            json.dumps(config if config is not None else {"host": "example.com"}),
        )
    return str(connector_id)


async def _thread(db, status="active") -> str:
    thread_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id, status, metadata) VALUES ($1, $2, '{}'::jsonb)",
            thread_id,
            status,
        )
    return str(thread_id)


class _Resolver:
    """A registry whose tag the test moves."""

    def __init__(self) -> None:
        self.current: ResolvedImage | None = None
        self.calls = 0

    def push(self, digest: str, label: dict | None = None) -> None:
        self.current = ResolvedImage(
            reference=f"ghcr.io/org/echo@{digest}",
            digest=digest,
            entrypoint=("/echo",),
            cmd=(),
            labels={SPEC_LABEL: json.dumps(label)} if label else {},
        )

    async def resolve_image(self, image: str) -> ResolvedImage:
        self.calls += 1
        assert self.current is not None
        return self.current


def _label(**over) -> dict:
    label = {
        "name": DRIVER,
        "protocol_version": "1.0",
        "config_schema": SERVICE.config_schema,
        "credential_slots": [{"name": "secret"}],
    }
    label.update(over)
    return label


@pytest.fixture
def registry():
    resolver = _Resolver()
    images.configure_service_images(
        images.ServiceImageSettings(
            references={DRIVER: REFERENCE}, resolver=resolver, cache_seconds=0
        )
    )
    return resolver


async def _bind(db, connector: str, thread: str) -> leases.DeliveredLease:
    owner = leases.LeaseOwner.thread(thread)
    async with db.acquire() as conn:
        digest = await images.bind_service_image(
            conn, spec=SERVICE, connector_id=connector, owner=owner
        )
        return await leases.issue_or_redeliver(
            conn,
            owner=owner,
            connector_id=connector,
            driver=DRIVER,
            access="ReadWrite",
            image_digest=digest,
        )


async def _lease_digest(db, lease_id: str) -> str:
    async with db.acquire() as conn:
        return await conn.fetchval(
            "SELECT image_digest FROM connector_credential_leases WHERE id = $1",
            UUID(lease_id),
        )


# =============================================================================
# Image resolutions
# =============================================================================


@pytest.mark.asyncio
async def test_image_rows_reject_malformed_values(db):
    base = (
        "INSERT INTO connector_driver_images (driver, reference, digest, "
        "entrypoint, cmd, spec, protocol_version) VALUES ($1, $2, $3, $4::jsonb, "
        "$5::jsonb, $6::jsonb, $7)"
    )
    bad = [
        ("", REFERENCE, D1, "[]", "[]", None, "1.0"),
        (DRIVER, "", D1, "[]", "[]", None, "1.0"),
        (DRIVER, REFERENCE, "sha256:XYZ", "[]", "[]", None, "1.0"),
        (DRIVER, REFERENCE, D1, "{}", "[]", None, "1.0"),
        (DRIVER, REFERENCE, D1, "[]", '"x"', None, "1.0"),
        (DRIVER, REFERENCE, D1, "[]", "[]", "[1]", "1.0"),
        (DRIVER, REFERENCE, D1, "[]", "[]", None, "v1"),
    ]
    async with db.acquire() as conn:
        for row in bad:
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(base, *row)
        await conn.execute(base, DRIVER, REFERENCE, D1, "[]", "[]", None, "1.0")
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(base, DRIVER, REFERENCE, D1, "[]", "[]", None, "1.0")


@pytest.mark.asyncio
async def test_a_resolution_is_recorded_once_per_digest_and_refreshed(db, registry):
    registry.push(D1, _label())
    async with db.acquire() as conn:
        first = await images.resolve_driver_image(
            conn, driver=DRIVER, reference=REFERENCE
        )
        again = await images.resolve_driver_image(
            conn, driver=DRIVER, reference=REFERENCE
        )
        rows = await conn.fetch("SELECT * FROM connector_driver_images")
    assert first.digest == again.digest == D1
    assert len(rows) == 1
    assert rows[0]["first_resolved_at"] <= rows[0]["resolved_at"]
    assert json.loads(rows[0]["entrypoint"]) == ["/echo"]
    assert json.loads(rows[0]["spec"])["name"] == DRIVER
    async with db.acquire() as conn:
        found = await images.image_for_digest(conn, driver=DRIVER, digest=D1)
    assert found is not None and found.entrypoint == ("/echo",)


# =============================================================================
# The bind: one digest per binding, the moved-tag check
# =============================================================================


@pytest.mark.asyncio
async def test_each_binding_records_its_digest_and_redelivery_keeps_it(db, registry):
    connector = await _connector(db)
    thread_a, thread_b = await _thread(db), await _thread(db)
    registry.push(D1, _label())
    lease_a = await _bind(db, connector, thread_a)
    assert await _lease_digest(db, lease_a.id) == D1
    # The tag moves compatibly: a new binding follows it ...
    registry.push(D2, _label(protocol_version="1.1"))
    lease_b = await _bind(db, connector, thread_b)
    assert await _lease_digest(db, lease_b.id) == D2
    # ... and re-delivering the first binding is no new bind: same lease,
    # same digest, no registry call.
    calls = registry.calls
    again = await _bind(db, connector, thread_a)
    assert again.id == lease_a.id and not again.issued
    assert await _lease_digest(db, again.id) == D1
    assert registry.calls == calls


@pytest.mark.asyncio
async def test_a_moved_incompatible_tag_is_refused_at_bind(db, registry):
    connector = await _connector(db)
    registry.push(D1, _label())
    await _bind(db, connector, await _thread(db))
    # The author pushed the tag again with another protocol major and
    # without the secret slot.
    registry.push(D2, _label(protocol_version="2.0", credential_slots=[]))
    thread = await _thread(db)
    with pytest.raises(images.ServiceImageRefused) as refused:
        await _bind(db, connector, thread)
    message = str(refused.value)
    assert f"The image behind {REFERENCE} changed its contract" in message
    assert "protocol 2.0 is not supported" in message
    assert "credential slots disappeared: secret" in message
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM connector_credential_leases WHERE thread_id = $1",
                UUID(thread),
            )
            == 0
        )
        event = await conn.fetchrow(
            "SELECT * FROM security_events "
            "WHERE event_type = 'connector_driver_image_refused'"
        )
    assert event["resource_id"] == connector
    assert D2 in event["detail"]


@pytest.mark.asyncio
async def test_a_moved_tag_whose_schema_rejects_the_stored_config_is_refused(
    db, registry
):
    connector = await _connector(db, config={"host": "example.com"})
    registry.push(D1, _label())
    await _bind(db, connector, await _thread(db))
    schema = {
        "type": "object",
        "required": ["region"],
        "properties": {"region": {"type": "string"}},
    }
    registry.push(D3, _label(config_schema=schema))
    with pytest.raises(images.ServiceImageRefused, match="'region' is a required"):
        await _bind(db, connector, await _thread(db))


@pytest.mark.asyncio
async def test_the_first_bind_checks_a_label_against_the_installed_spec(db, registry):
    connector = await _connector(db)
    registry.push(D1, _label(name="srw.other/v1"))
    with pytest.raises(
        images.ServiceImageRefused, match="declares driver srw.other/v1"
    ):
        await _bind(db, connector, await _thread(db))
    # An image without a label keeps the installed spec.
    registry.push(D2)
    lease = await _bind(db, connector, await _thread(db))
    assert await _lease_digest(db, lease.id) == D2


@pytest.mark.asyncio
async def test_a_driver_without_an_image_fails_the_bind(db, registry):
    images.configure_service_images(images.ServiceImageSettings(resolver=registry))
    with pytest.raises(images.ServiceImageUnavailable, match="No image is configured"):
        await _bind(db, await _connector(db), await _thread(db))


@pytest.mark.asyncio
async def test_ensure_image_resolves_a_digest_no_row_holds(db, registry):
    registry.push(D1, _label())
    async with db.acquire() as conn:
        image = await images.ensure_image(
            conn, driver=DRIVER, reference=REFERENCE, digest=D1
        )
        assert image.entrypoint == ("/echo",)
        assert await conn.fetchval("SELECT count(*) FROM connector_driver_images") == 1
        with pytest.raises(images.ServiceImageUnavailable, match="another digest"):
            await images.ensure_image(
                conn, driver=DRIVER, reference=REFERENCE, digest=D2
            )


# =============================================================================
# The project tier decides private addresses
# =============================================================================


async def _project(db, tier: str) -> str:
    project_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name, network_tier) VALUES ($1, $2, $3)",
            project_id,
            f"p-{str(project_id)[:8]}",
            tier,
        )
    return str(project_id)


async def _link(db, project: str, connector: str) -> None:
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO project_datasources (project_id, datasource_id) "
            "VALUES ($1, $2)",
            UUID(project),
            UUID(connector),
        )


async def _private_allowed(db, connector: str) -> bool:
    async with db.acquire() as conn:
        return await private_addresses_allowed(
            conn, connector, private_tiers={"home-allowed"}
        )


@pytest.mark.asyncio
async def test_private_addresses_need_every_project_on_a_private_tier(db):
    connector = await _connector(db)
    # No project at all: the strictest tier.
    assert await _private_allowed(db, connector) is False
    home = await _project(db, "home-allowed")
    await _link(db, home, connector)
    assert await _private_allowed(db, connector) is True
    # One more project on the internet-only tier: the strictest decides.
    await _link(db, await _project(db, "internet-only"), connector)
    assert await _private_allowed(db, connector) is False


@pytest.mark.asyncio
async def test_the_owning_project_counts_and_a_public_connector_never_qualifies(db):
    connector = await _connector(db)
    home = await _project(db, "home-allowed")
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE datasources SET project_id = $2 WHERE id = $1",
            UUID(connector),
            UUID(home),
        )
    assert await _private_allowed(db, connector) is True
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE datasources SET is_global = true WHERE id = $1", UUID(connector)
        )
    assert await _private_allowed(db, connector) is False
    assert await _private_allowed(db, str(uuid4())) is False


def test_resolved_at_is_a_timestamp():
    bound = images.BoundImage(
        driver=DRIVER,
        reference=REFERENCE,
        digest=D1,
        resolved_at=datetime(2026, 10, 8, tzinfo=timezone.utc),
        spec=None,
        spec_hash=None,
        protocol_version="1.0",
    )
    assert bound.record()["resolved_at"] == "2026-10-08T00:00:00+00:00"
