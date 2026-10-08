"""Real-PostgreSQL proofs for service-plane driver hosting (connector drivers D5).

Driver image resolutions and the moved-tag refusal at bind run against
``schema_current.sql``.
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
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
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_lease_exchange import (
    ConnectorLeaseExchange,
    DenialLimiter,
)
from orchestrator.services.connector_service_hosting import (
    PodState,
    ServiceHostingReconciler,
    ServiceHostingSettings,
    connector_egress_view,
)
from shared.connectors.contract import (
    AccessLevel,
    CredentialSlot,
    DriverSpec,
    ServiceSpec,
)
from shared.connectors.builtin import ECHO_SERVICE_SPEC
from shared.connectors.images import SPEC_LABEL
from shared.connectors.leases import token_digest
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
def registry(db):
    resolver = _Resolver()
    images.configure_service_images(
        images.ServiceImageSettings(
            references={DRIVER: REFERENCE},
            resolver=resolver,
            cache_seconds=0,
            store=db,
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
    first = await images.resolve_driver_image(db, driver=DRIVER, reference=REFERENCE)
    again = await images.resolve_driver_image(db, driver=DRIVER, reference=REFERENCE)
    async with db.acquire() as conn:
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


# =============================================================================
# No registry lookup and no write inside the caller's transaction
# =============================================================================


async def _count(db, query: str, *args) -> int:
    async with db.acquire() as conn:
        return int(await conn.fetchval(query, *args))


@pytest.mark.asyncio
async def test_a_bind_inside_a_transaction_commits_its_image_row_apart(db, registry):
    connector, thread = await _connector(db), await _thread(db)
    registry.push(D1, _label())
    owner = leases.LeaseOwner.thread(thread)
    async with db.acquire() as conn:
        transaction = conn.transaction()
        await transaction.start()
        digest = await images.bind_service_image(
            conn, spec=SERVICE, connector_id=connector, owner=owner
        )
        # Committed on its own connection while the caller's is still open.
        assert await _count(db, "SELECT count(*) FROM connector_driver_images") == 1
        await transaction.rollback()
    assert digest == D1
    assert await _count(db, "SELECT count(*) FROM connector_driver_images") == 1


@pytest.mark.asyncio
async def test_a_bind_inside_a_transaction_scope_still_writes_apart(db, registry):
    """Inside ``transaction_scope`` the task's acquisitions share the scope's
    connection; the bind's writes run in a task of their own."""
    connector, thread = await _connector(db), await _thread(db)
    registry.push(D1, _label())
    owner = leases.LeaseOwner.thread(thread)
    with pytest.raises(RuntimeError, match="caller rolls back"):
        async with db.transaction_scope():
            async with db.acquire() as conn:
                await images.bind_service_image(
                    conn, spec=SERVICE, connector_id=connector, owner=owner
                )
            raise RuntimeError("caller rolls back")
    assert await _count(db, "SELECT count(*) FROM connector_driver_images") == 1


@pytest.mark.asyncio
async def test_a_refusal_audit_survives_the_callers_rollback(db, registry):
    connector = await _connector(db)
    registry.push(D1, _label())
    await _bind(db, connector, await _thread(db))
    registry.push(D2, _label(protocol_version="2.0"))
    owner = leases.LeaseOwner.thread(await _thread(db))
    async with db.acquire() as conn:
        transaction = conn.transaction()
        await transaction.start()
        with pytest.raises(images.ServiceImageRefused):
            await images.bind_service_image(
                conn, spec=SERVICE, connector_id=connector, owner=owner
            )
        await transaction.rollback()
    assert (
        await _count(
            db,
            "SELECT count(*) FROM security_events "
            "WHERE event_type = 'connector_driver_image_refused' AND resource_id = $1",
            connector,
        )
        == 1
    )


@pytest.mark.asyncio
async def test_a_slow_registry_costs_a_bind_its_cap_and_is_remembered_briefly(
    db, registry, monkeypatch
):
    calls: list[str] = []

    class Slow:
        async def resolve_image(self, image):
            calls.append(image)
            await asyncio.sleep(30)

    images.configure_service_images(
        images.ServiceImageSettings(
            references={DRIVER: REFERENCE},
            resolver=Slow(),
            cache_seconds=60,
            bind_timeout_seconds=0.5,
            store=db,
        )
    )
    connector = await _connector(db)
    for _ in range(2):
        started = time.monotonic()
        with pytest.raises(images.ServiceImageUnavailable):
            await _bind(db, connector, await _thread(db))
        assert time.monotonic() - started < 5
    # The capped failure spares the next bind the wait: asked once.
    assert len(calls) == 1
    # Only briefly, never for the whole window: a later bind asks again.
    monkeypatch.setattr(images, "BRIEF_FAILURE_SECONDS", 0.05)
    images.configure_service_images(images.service_image_settings())
    with pytest.raises(images.ServiceImageUnavailable):
        await _bind(db, connector, await _thread(db))
    await asyncio.sleep(0.1)
    with pytest.raises(images.ServiceImageUnavailable):
        await _bind(db, connector, await _thread(db))
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_rr_hung_registry_bind_reuses_the_last_digest(db, registry):
    """Registry unreachable (hangs): a bind with no prepared decision reuses
    the digest it resolved before (the module docstring's promise). The
    lookup gets less than the bind's cap, so the fallback read fits in it."""
    connector = await _connector(db)
    registry.push(D1, _label())
    await images.resolve_driver_image(db, driver=DRIVER, reference=REFERENCE)

    class Hung:
        async def resolve_image(self, image):
            await asyncio.sleep(30)

    images.configure_service_images(
        images.ServiceImageSettings(
            references={DRIVER: REFERENCE},
            resolver=Hung(),
            cache_seconds=60,
            bind_timeout_seconds=0.5,
            store=db,
        )
    )
    started = time.monotonic()
    lease = await _bind(db, connector, await _thread(db))
    assert time.monotonic() - started < 2
    assert await _lease_digest(db, lease.id) == D1


@pytest.mark.asyncio
async def test_a_prepared_decision_spares_the_bind_the_registry(db, registry):
    connector, thread = await _connector(db), await _thread(db)
    registry.push(D1, _label())
    images.configure_service_images(
        dataclasses.replace(images.service_image_settings(), cache_seconds=60)
    )
    entry = {"type": "echo_service", "name": "e", "datasource_id": connector}
    owner = leases.LeaseOwner.thread(thread)
    with mock.patch.object(leases, "lease_spec", lambda entry: SERVICE):
        await leases.prepare_lease_delivery(db, [entry], owner=owner)
    calls = registry.calls
    async with db.acquire() as conn:
        async with conn.transaction():
            digest = await images.bind_service_image(
                conn, spec=SERVICE, connector_id=connector, owner=owner
            )
    assert digest == D1 and registry.calls == calls


SERVICE_B = dataclasses.replace(SERVICE, name="srw.test-service-b/v1")
REFERENCE_B = "ghcr.io/org/other:latest"


class _TwoImages:
    """A registry with one image per repository."""

    def __init__(self) -> None:
        self.calls = 0

    async def resolve_image(self, image: str) -> ResolvedImage:
        self.calls += 1
        await asyncio.sleep(0.05)  # let the two claims interleave
        digest = D2 if "/other" in image else D1
        return ResolvedImage(
            reference=f"{image.rsplit(':', 1)[0]}@{digest}",
            digest=digest,
            entrypoint=("/echo",),
        )


@pytest.mark.asyncio
async def test_two_claims_binding_two_service_drivers_do_not_deadlock(db):
    """Two claim transactions, each binding a connector of each of two
    service drivers, at once: neither waits on the other's image rows."""
    images.configure_service_images(
        images.ServiceImageSettings(
            references={DRIVER: REFERENCE, SERVICE_B.name: REFERENCE_B},
            resolver=_TwoImages(),
            cache_seconds=0,
            store=db,
        )
    )
    a = await _connector(db)
    b = await _connector(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE datasources SET type = 'echo_service_b' WHERE id = $1", UUID(b)
        )
    specs = {"echo_service": SERVICE, "echo_service_b": SERVICE_B}

    async def claim(thread: str) -> int:
        entries = [
            {"type": "echo_service", "name": "a", "datasource_id": a},
            {"type": "echo_service_b", "name": "b", "datasource_id": b},
        ]
        async with db.acquire() as conn:
            async with conn.transaction():
                delivered = await leases.deliver_connector_leases(
                    conn, entries, owner=leases.LeaseOwner.thread(thread)
                )
                await asyncio.sleep(0.2)  # hold the claim open
                return delivered

    with mock.patch.object(
        leases, "lease_spec", lambda entry: specs.get(entry.get("type"))
    ):
        delivered = await asyncio.wait_for(
            asyncio.gather(claim(await _thread(db)), claim(await _thread(db))),
            timeout=30,
        )
    assert delivered == [2, 2]
    digests = await _count(
        db,
        "SELECT count(DISTINCT image_digest) FROM connector_credential_leases "
        "WHERE image_digest IS NOT NULL",
    )
    assert digests == 2
    assert await _count(db, "SELECT count(*) FROM connector_driver_images") == 2


@pytest.mark.asyncio
async def test_ensure_image_resolves_a_digest_no_row_holds(db, registry):
    registry.push(D1, _label())
    async with db.acquire() as conn:
        image = await images.ensure_image(
            conn, driver=DRIVER, reference=REFERENCE, digest=D1
        )
        assert image.entrypoint == ("/echo",)
        assert await conn.fetchval("SELECT count(*) FROM connector_driver_images") == 1
        with pytest.raises(images.ServiceImageUnavailable, match="cannot be resolved"):
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


# =============================================================================
# The reconciler: start, share, ready, idle stop, cap, identities
# =============================================================================

ECHO = "srw.echo-service/v1"
ECHO_REFERENCE = "srw-registry:5000/srw-driver-echo:tilt-1"
ADDRESSES = {
    "one.one.one.one": ["1.1.1.1"],
    "srw-orchestrator.srw.svc": ["10.43.0.20"],
    "nas.home": ["192.168.178.20"],
}


class FakeRuntime:
    """The Kubernetes side: launches recorded, pod states set by the test."""

    def __init__(self) -> None:
        self.plans: dict[str, object] = {}
        self.states: dict[str, object] = {}
        self.removed: list[str] = []
        self.binding_policies: dict[str, set[str]] = {}
        self.objects: list[tuple[object, str, str]] = []
        self.deleted: list[str] = []

    async def launch(self, plan):
        identity = plan.identity.identity_id
        self.plans[identity] = plan
        self.states[identity] = PodState("Pending", uid=f"uid-{identity[:8]}")
        return f"uid-{identity[:8]}"

    async def observe(self, identity):
        return self.states.get(identity.identity_id, PodState("Absent"))

    async def remove(self, identity):
        self.removed.append(identity.identity_id)
        self.states.pop(identity.identity_id, None)
        return True

    async def sync_binding_policies(self, identity, desired):
        self.binding_policies[identity.identity_id] = set(desired)

    async def managed_objects(self):
        return list(self.objects)

    async def delete_object(self, delete, name):
        self.deleted.append(name)

    def ready(self, identity_id: str) -> None:
        self.states[identity_id] = PodState("Running", uid="u", ready=True)


async def _echo_connector(db, *, host="one.one.one.one", secret="s") -> str:
    connector_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO datasources (id, name, type, scope_mode, policy_revision, "
            "credentials, config) VALUES ($1, $2, 'echo_service', 'all', 1, "
            "$3::jsonb, $4::jsonb)",
            connector_id,
            f"echo-{str(connector_id)[:8]}",
            json.dumps(encrypt(json.dumps({"secret": secret}))),
            json.dumps({"host": host, "port": 443}),
        )
    return str(connector_id)


async def _bind_echo(db, connector: str, thread: str, digest: str = D1):
    async with db.acquire() as conn:
        return await leases.issue_or_redeliver(
            conn,
            owner=leases.LeaseOwner.thread(thread),
            connector_id=connector,
            driver=ECHO,
            access="ReadWrite",
            image_digest=digest,
        )


async def _echo_image(db, digest: str = D1) -> None:
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO connector_driver_images (driver, reference, digest, "
            "entrypoint, cmd, protocol_version) VALUES ($1, $2, $3, "
            "'[\"/srw-driver-echo\"]'::jsonb, '[]'::jsonb, '1.0') "
            "ON CONFLICT DO NOTHING",
            ECHO,
            ECHO_REFERENCE,
            digest,
        )


async def _pods(db) -> list:
    async with db.acquire() as conn:
        return list(
            await conn.fetch(
                "SELECT * FROM connector_driver_identities "
                "WHERE credential_generation IS NOT NULL ORDER BY created_at"
            )
        )


async def _end(db, thread: str) -> None:
    async with db.acquire() as conn:
        await leases.revoke_execution_leases(
            conn, thread_id=thread, reason="session_end"
        )


@pytest.fixture
def reconciler(db):
    images.configure_service_images(
        images.ServiceImageSettings(references={ECHO: ECHO_REFERENCE})
    )
    offset = [timedelta()]

    async def resolver(host, ipv6):
        if host not in ADDRESSES:
            raise OSError("unknown host")
        return ADDRESSES[host]

    runtime = FakeRuntime()
    built = ServiceHostingReconciler(
        store=db,
        runtime=runtime,
        drivers=builtin_connector_drivers(echo_service_image=ECHO_REFERENCE),
        settings=ServiceHostingSettings(
            namespace="srw-connectors",
            release_namespace="srw",
            shim_image="srw-registry:5000/srw-driver-shim@sha256:" + "e" * 64,
            exchange_host="srw-orchestrator.srw.svc",
            exchange_port=8088,
            orchestrator_labels={"app.kubernetes.io/component": "orchestrator"},
            max_installation=2,
            idle_seconds=60,
            start_timeout_seconds=120,
            refused_cidrs=("10.0.50.0/24", "10.0.51.0/24"),
            pod_ip="10.42.0.9",
        ),
        resolver=resolver,
        clock=lambda: datetime.now(timezone.utc) + offset[0],
    )
    built.offset = offset
    built.fake = runtime
    return built


@pytest.mark.asyncio
async def test_a_binding_starts_one_shared_pod_with_its_own_identity(db, reconciler):
    connector = await _echo_connector(db)
    await _echo_image(db)
    thread_a, thread_b = await _thread(db), await _thread(db)
    await _bind_echo(db, connector, thread_a)
    report = await reconciler.reconcile_once()
    (pod,) = await _pods(db)
    assert report.started == [str(pod["id"])]
    assert pod["image_digest"] == D1
    assert pod["image_reference"] == ECHO_REFERENCE
    assert pod["pod_name"] == f"srw-drv-{UUID(str(pod['id'])).hex}"
    assert pod["pod_namespace"] == "srw-connectors"
    assert pod["pod_uid"] == f"uid-{str(pod['id'])[:8]}"
    assert pod["credential_generation"].startswith("hmac-sha256:")
    egress = json.loads(pod["egress"])
    assert egress["hosts"][0]["addresses"] == ["1.1.1.1"]
    assert egress["dns"] == "none"
    assert pod["egress_resolved_at"] is not None
    plan = reconciler.fake.plans[str(pod["id"])]
    # The pod runs the image at the binding's digest; its Secret holds the
    # identity the row stores the digest of, and no upstream secret.
    driver = plan.pod["spec"]["containers"][0]
    assert driver["image"] == f"srw-registry:5000/srw-driver-echo@{D1}"
    assert driver["args"] == ["/srw-driver-echo"]
    token = base64.b64decode(plan.secret["data"]["identity"]).decode()
    assert token_digest(token) == bytes(pod["token_hash"])
    request = json.loads(base64.b64decode(plan.secret["data"]["request.json"]))
    assert request["credentials"] == {}
    assert plan.pod["spec"]["hostAliases"][-1] == {
        "ip": "10.43.0.20",
        "hostnames": ["srw-orchestrator.srw.svc"],
    }
    # Each workspace binding gets its own ingress policy.
    assert len(reconciler.fake.binding_policies[str(pod["id"])]) == 1

    # A second binding of the same connector and digest shares the pod.
    await _bind_echo(db, connector, thread_b)
    report = await reconciler.reconcile_once()
    assert report.started == []
    assert len(await _pods(db)) == 1
    assert len(reconciler.fake.binding_policies[str(pod["id"])]) == 2

    reconciler.fake.ready(str(pod["id"]))
    report = await reconciler.reconcile_once()
    assert report.ready == [str(pod["id"])]
    (pod,) = await _pods(db)
    assert pod["ready_at"] is not None and pod["idle_since"] is None


@pytest.mark.asyncio
async def test_the_idle_stop_revokes_the_identity_then_removes_the_pod(db, reconciler):
    connector = await _echo_connector(db)
    await _echo_image(db)
    thread = await _thread(db)
    await _bind_echo(db, connector, thread)
    await reconciler.reconcile_once()
    (pod,) = await _pods(db)
    reconciler.fake.ready(str(pod["id"]))
    await _end(db, thread)
    await reconciler.reconcile_once()
    (pod,) = await _pods(db)
    assert pod["idle_since"] is not None and pod["revoked_at"] is None
    # Not idle long enough yet.
    assert (await reconciler.reconcile_once()).stopped == []
    reconciler.offset[0] = timedelta(seconds=61)
    report = await reconciler.reconcile_once()
    assert report.stopped == [(str(pod["id"]), "idle")]
    (pod,) = await _pods(db)
    assert pod["revoke_reason"] == "idle"
    assert pod["removed_at"] is not None
    assert reconciler.fake.removed == [str(pod["id"])]
    async with db.acquire() as conn:
        event = await conn.fetchrow(
            "SELECT * FROM security_events "
            "WHERE event_type = 'connector_driver_identity_revoked' "
            "AND resource_id = $1",
            str(pod["id"]),
        )
    assert event is not None and "reason=idle" in event["detail"]


@pytest.mark.asyncio
async def test_a_new_binding_after_the_idle_stop_starts_a_new_pod(db, reconciler):
    connector = await _echo_connector(db)
    await _echo_image(db)
    thread = await _thread(db)
    await _bind_echo(db, connector, thread)
    await reconciler.reconcile_once()
    await _end(db, thread)
    await reconciler.reconcile_once()
    reconciler.offset[0] = timedelta(seconds=61)
    await reconciler.reconcile_once()
    await _bind_echo(db, connector, await _thread(db))
    report = await reconciler.reconcile_once()
    assert len(report.started) == 1
    first, second = await _pods(db)
    assert first["revoked_at"] is not None and second["revoked_at"] is None
    assert first["pod_name"] != second["pod_name"]


@pytest.mark.asyncio
async def test_the_installation_cap_refuses_a_pod_past_it(db, reconciler):
    await _echo_image(db)
    for _ in range(3):
        await _bind_echo(db, await _echo_connector(db), await _thread(db))
    report = await reconciler.reconcile_once()
    assert len(report.started) == 2
    assert report.capacity == 1
    assert len(await _pods(db)) == 2


@pytest.mark.asyncio
async def test_a_credential_change_starts_a_new_pod_and_the_old_one_drains(
    db, reconciler
):
    connector = await _echo_connector(db)
    await _echo_image(db)
    await _bind_echo(db, connector, await _thread(db))
    await reconciler.reconcile_once()
    # A config change is a new generation: the request file is immutable.
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE datasources SET config = $2::jsonb WHERE id = $1",
            UUID(connector),
            json.dumps({"host": "one.one.one.one", "port": 443, "message": "v2"}),
        )
    report = await reconciler.reconcile_once()
    assert len(report.started) == 1
    old, new = await _pods(db)
    assert old["credential_generation"] != new["credential_generation"]
    assert old["idle_since"] is not None and old["revoked_at"] is None
    reconciler.offset[0] = timedelta(seconds=61)
    report = await reconciler.reconcile_once()
    assert (str(old["id"]), "idle") in report.stopped
    assert all(identity != str(new["id"]) for identity, _ in report.stopped)


async def _set_config(db, connector: str, config: dict) -> None:
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE datasources SET config = $2::jsonb WHERE id = $1",
            UUID(connector),
            json.dumps(config),
        )


@pytest.mark.asyncio
async def test_a_lost_private_tier_stops_the_old_pod_at_once(db, reconciler):
    """A tier downgrade: the pod pinned with private addresses allowed does
    not drain for the idle time with them; its replacement has none."""
    connector = await _echo_connector(db)
    await _link(db, await _project(db, "home-allowed"), connector)
    await _echo_image(db)
    await _bind_echo(db, connector, await _thread(db))
    await reconciler.reconcile_once()
    (old,) = await _pods(db)
    assert json.loads(old["egress"])["private_allowed"] is True
    reconciler.fake.ready(str(old["id"]))
    # One more project on the internet-only tier: the strictest decides.
    await _link(db, await _project(db, "internet-only"), connector)
    report = await reconciler.reconcile_once()
    assert report.stopped == [(str(old["id"]), "egress_withdrawn")]
    assert len(report.started) == 1
    old, new = await _pods(db)
    assert old["revoked_at"] is not None and old["removed_at"] is not None
    assert json.loads(new["egress"])["private_allowed"] is False
    assert new["revoked_at"] is None


@pytest.mark.asyncio
async def test_a_private_tier_grant_starts_a_new_pod_and_the_old_one_drains(
    db, reconciler
):
    """A tier upgrade is a new generation (the tier is in the fingerprint):
    a pod with the wider policy starts, the narrower one drains."""
    connector = await _echo_connector(db)
    internet = await _project(db, "internet-only")
    await _link(db, internet, connector)
    await _echo_image(db)
    await _bind_echo(db, connector, await _thread(db))
    await reconciler.reconcile_once()
    (old,) = await _pods(db)
    assert json.loads(old["egress"])["private_allowed"] is False
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE projects SET network_tier = 'home-allowed' WHERE id = $1",
            UUID(internet),
        )
    report = await reconciler.reconcile_once()
    assert len(report.started) == 1 and report.stopped == []
    old, new = await _pods(db)
    assert old["credential_generation"] != new["credential_generation"]
    assert json.loads(new["egress"])["private_allowed"] is True
    assert old["idle_since"] is not None and old["revoked_at"] is None


@pytest.mark.asyncio
async def test_an_egress_change_stops_the_old_pod_at_once(db, reconciler):
    """A connector pointed elsewhere: the old destination closes now, not
    after the idle time. A change that keeps the egress drains instead
    (test_a_credential_change_starts_a_new_pod_and_the_old_one_drains)."""
    ADDRESSES["dns.google"] = ["8.8.8.8"]
    try:
        connector = await _echo_connector(db)
        await _echo_image(db)
        await _bind_echo(db, connector, await _thread(db))
        await reconciler.reconcile_once()
        (old,) = await _pods(db)
        await _set_config(db, connector, {"host": "dns.google", "port": 443})
        report = await reconciler.reconcile_once()
        assert report.stopped == [(str(old["id"]), "egress_withdrawn")]
        assert len(report.started) == 1
        old, new = await _pods(db)
        assert json.loads(new["egress"])["hosts"][0]["addresses"] == ["8.8.8.8"]
        # A port change is an egress change too.
        await _set_config(db, connector, {"host": "dns.google", "port": 853})
        report = await reconciler.reconcile_once()
        assert report.stopped == [(str(new["id"]), "egress_withdrawn")]
    finally:
        ADDRESSES.pop("dns.google")


@pytest.mark.asyncio
async def test_an_unbound_pod_loses_its_private_egress_at_once(db, reconciler):
    """Idle pods too: the downgrade does not wait for the idle stop."""
    connector = await _echo_connector(db)
    await _link(db, await _project(db, "home-allowed"), connector)
    await _echo_image(db)
    thread = await _thread(db)
    await _bind_echo(db, connector, thread)
    await reconciler.reconcile_once()
    (pod,) = await _pods(db)
    await _end(db, thread)
    await reconciler.reconcile_once()
    await _link(db, await _project(db, "internet-only"), connector)
    report = await reconciler.reconcile_once()
    assert report.stopped == [(str(pod["id"]), "egress_withdrawn")]
    assert report.started == []


@pytest.mark.asyncio
async def test_an_egress_the_tier_forbids_refuses_the_launch_and_backs_off(
    db, reconciler
):
    connector = await _echo_connector(db, host="nas.home")
    await _echo_image(db)
    await _bind_echo(db, connector, await _thread(db))
    report = await reconciler.reconcile_once()
    assert report.started == []
    assert "network tier" in report.refused[0][1]
    (pod,) = await _pods(db)
    assert pod["revoke_reason"] == "launch_refused"
    assert "192.168.178.20" in pod["launch_error"]
    assert pod["removed_at"] is not None
    assert reconciler.fake.plans == {}
    # Backed off: the next pass mints nothing.
    await reconciler.reconcile_once()
    assert len(await _pods(db)) == 1


@pytest.mark.asyncio
async def test_a_home_tier_connector_never_reaches_the_nodes(db, reconciler):
    """Private addresses allowed, the refused ranges (k3s nodes, MetalLB)
    still are not: kube-apiserver, kubelet and etcd stay out of reach."""
    ADDRESSES["k3s-node.home"] = ["10.0.50.11"]
    try:
        connector = await _echo_connector(db, host="k3s-node.home")
        await _link(db, await _project(db, "home-allowed"), connector)
        await _echo_image(db)
        await _bind_echo(db, connector, await _thread(db))
        report = await reconciler.reconcile_once()
    finally:
        ADDRESSES.pop("k3s-node.home")
    assert report.started == []
    assert "nodes and load balancers" in report.refused[0][1]
    (pod,) = await _pods(db)
    assert pod["revoke_reason"] == "launch_refused"


@pytest.mark.asyncio
async def test_a_cluster_with_other_ranges_refuses_to_host(db, reconciler):
    """The orchestrator's pod and the exchange Service outside clusterCidrs:
    the ranges every pod policy refuses are not this cluster's. Every live
    pod stops and none starts."""
    connector = await _echo_connector(db)
    await _echo_image(db)
    await _bind_echo(db, connector, await _thread(db))
    await reconciler.reconcile_once()
    (pod,) = await _pods(db)
    reconciler.fake.ready(str(pod["id"]))
    reconciler.settings = dataclasses.replace(
        reconciler.settings, cluster_cidrs=("10.96.0.0/12", "10.244.0.0/16")
    )
    report = await reconciler.reconcile_once()
    assert report.stopped == [(str(pod["id"]), "hosting_refused")]
    assert report.refused[0][0] == "installation"
    assert "10.42.0.9" in report.refused[0][1]
    assert "clusterCidrs" in report.refused[0][1]
    (pod,) = await _pods(db)
    assert pod["revoked_at"] is not None and pod["removed_at"] is not None
    # Bindings remain; still nothing starts.
    report = await reconciler.reconcile_once()
    assert report.started == []
    assert len(await _pods(db)) == 1


@pytest.mark.parametrize(
    ("pod_ip", "addresses", "fragment"),
    [
        ("", None, "pod address is unknown"),
        ("10.42.0.9", ["192.0.2.10"], "Service address 192.0.2.10"),
    ],
)
@pytest.mark.asyncio
async def test_an_unknown_pod_or_an_exchange_outside_refuses_to_host(
    db, reconciler, pod_ip, addresses, fragment
):
    reconciler.settings = dataclasses.replace(reconciler.settings, pod_ip=pod_ip)
    saved = ADDRESSES["srw-orchestrator.srw.svc"]
    if addresses is not None:
        ADDRESSES["srw-orchestrator.srw.svc"] = addresses
    try:
        connector = await _echo_connector(db)
        await _echo_image(db)
        await _bind_echo(db, connector, await _thread(db))
        report = await reconciler.reconcile_once()
    finally:
        ADDRESSES["srw-orchestrator.srw.svc"] = saved
    assert report.started == []
    assert fragment in report.refused[0][1]
    assert await _pods(db) == []


@pytest.mark.asyncio
async def test_an_unresolvable_exchange_starts_nothing_and_stops_nothing(
    db, reconciler
):
    """A resolver blip is not a misconfiguration: live pods keep serving."""
    connector = await _echo_connector(db)
    await _echo_image(db)
    await _bind_echo(db, connector, await _thread(db))
    await reconciler.reconcile_once()
    (pod,) = await _pods(db)
    reconciler.fake.ready(str(pod["id"]))
    saved = ADDRESSES.pop("srw-orchestrator.srw.svc")
    try:
        await _bind_echo(db, await _echo_connector(db), await _thread(db))
        report = await reconciler.reconcile_once()
    finally:
        ADDRESSES["srw-orchestrator.srw.svc"] = saved
    assert report.started == [] and report.stopped == []
    live = [row for row in await _pods(db) if row["revoked_at"] is None]
    assert [str(row["id"]) for row in live] == [str(pod["id"])]
    # Resolvable again: the waiting binding gets its pod.
    assert len((await reconciler.reconcile_once()).started) == 1


@pytest.mark.asyncio
async def test_hosting_turned_off_revokes_every_live_identity_and_on_cleans_up(
    db, reconciler
):
    """Off: the exchange refuses the pods at once (database only). Back on:
    the first pass deletes the revoked pods' objects."""
    from orchestrator.services.connector_service_hosting import (
        revoke_unhosted_identities,
    )

    connector = await _echo_connector(db)
    await _echo_image(db)
    await _bind_echo(db, connector, await _thread(db))
    await reconciler.reconcile_once()
    (pod,) = await _pods(db)
    revoked = await revoke_unhosted_identities(db)
    assert revoked == [str(pod["id"])]
    (pod,) = await _pods(db)
    assert pod["revoke_reason"] == "hosting_disabled"
    assert pod["removed_at"] is None  # nothing deleted while off
    assert reconciler.fake.removed == []
    async with db.acquire() as conn:
        event = await conn.fetchrow(
            "SELECT * FROM security_events "
            "WHERE event_type = 'connector_driver_identity_revoked' "
            "AND resource_id = $1",
            str(pod["id"]),
        )
    assert event is not None and "reason=hosting_disabled" in event["detail"]
    assert await revoke_unhosted_identities(db) == []  # idempotent

    report = await reconciler.reconcile_once()
    assert str(pod["id"]) in report.removed
    assert reconciler.fake.removed[0] == str(pod["id"])
    # The binding is still live: a fresh pod with a fresh identity starts.
    assert len(report.started) == 1


@pytest.mark.asyncio
async def test_a_digest_launches_from_the_repository_it_was_resolved_from(
    db, reconciler
):
    """A binding made before the driver's image moved to another repository
    keeps running its digest from where that digest lives."""
    connector = await _echo_connector(db)
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO connector_driver_images (driver, reference, digest, "
            "entrypoint, cmd, protocol_version) VALUES ($1, $2, $3, "
            "'[\"/old-echo\"]'::jsonb, '[]'::jsonb, '1.0')",
            ECHO,
            "old-registry:5000/old-echo:1",
            D2,
        )
    await _bind_echo(db, connector, await _thread(db), digest=D2)
    await reconciler.reconcile_once()
    (plan,) = reconciler.fake.plans.values()
    driver = plan.pod["spec"]["containers"][0]
    assert driver["image"] == f"old-registry:5000/old-echo@{D2}"
    assert driver["args"] == ["/old-echo"]


@pytest.mark.asyncio
async def test_an_unexpected_build_error_leaves_no_live_identity_and_backs_off(
    db, reconciler
):
    resolve = reconciler.resolver

    async def broken(host, ipv6):
        if host == "one.one.one.one":
            raise RuntimeError("resolver crashed")
        return await resolve(host, ipv6)

    reconciler.resolver = broken
    connector = await _echo_connector(db)
    await _echo_image(db)
    await _bind_echo(db, connector, await _thread(db))
    report = await reconciler.reconcile_once()
    assert report.started == [] and report.refused == [(connector, "RuntimeError")]
    (pod,) = await _pods(db)
    assert pod["revoke_reason"] == "launch_failed"
    assert pod["removed_at"] is not None
    assert reconciler.fake.plans == {}
    await reconciler.reconcile_once()
    assert len(await _pods(db)) == 1


@pytest.mark.asyncio
async def test_a_lost_or_never_ready_pod_is_stopped_and_replaced(db, reconciler):
    connector = await _echo_connector(db)
    await _echo_image(db)
    await _bind_echo(db, connector, await _thread(db))
    await reconciler.reconcile_once()
    (pod,) = await _pods(db)
    reconciler.fake.states.pop(str(pod["id"]))  # deleted behind SRW's back
    report = await reconciler.reconcile_once()
    assert (str(pod["id"]), "pod_lost") in report.stopped
    assert len(report.started) == 1  # bindings remain: a new pod
    _lost, replacement = await _pods(db)
    reconciler.offset[0] = timedelta(seconds=121)
    report = await reconciler.reconcile_once()
    assert (str(replacement["id"]), "start_timeout") in report.stopped
    # A start timeout backs the key off.
    assert report.started == []


@pytest.mark.asyncio
async def test_a_pod_stopped_at_its_start_timeout_records_why(db, reconciler):
    """The canary wait refusing to start the driver (no enforced default
    deny) is what an operator reads in launch_error and the egress view."""
    connector = await _echo_connector(db)
    await _echo_image(db)
    await _bind_echo(db, connector, await _thread(db))
    await reconciler.reconcile_once()
    (pod,) = await _pods(db)
    verdict = (
        "canary-wait: canary-wait: no 3 rounds ...: the default deny is not enforced"
    )
    reconciler.fake.states[str(pod["id"])] = PodState(
        "Pending", uid="u", reason="CrashLoopBackOff", message=verdict
    )
    reconciler.offset[0] = timedelta(seconds=121)
    report = await reconciler.reconcile_once()
    assert (str(pod["id"]), "start_timeout") in report.stopped
    (pod,) = await _pods(db)
    assert pod["launch_error"] == verdict


@pytest.mark.asyncio
async def test_an_evicted_pod_with_bindings_is_replaced(db, reconciler):
    connector = await _echo_connector(db)
    await _echo_image(db)
    await _bind_echo(db, connector, await _thread(db))
    await reconciler.reconcile_once()
    (pod,) = await _pods(db)
    reconciler.fake.ready(str(pod["id"]))
    await reconciler.reconcile_once()
    reconciler.fake.states[str(pod["id"])] = PodState(
        "Failed", uid="u", reason="Evicted"
    )
    report = await reconciler.reconcile_once()
    assert (str(pod["id"]), "pod_lost") in report.stopped
    assert len(report.started) == 1


@pytest.mark.asyncio
async def test_a_pod_that_stays_unready_is_replaced_one_that_recovers_is_kept(
    db, reconciler
):
    connector = await _echo_connector(db)
    await _echo_image(db)
    await _bind_echo(db, connector, await _thread(db))
    await reconciler.reconcile_once()
    (pod,) = await _pods(db)
    identity = str(pod["id"])
    reconciler.fake.ready(identity)
    await reconciler.reconcile_once()
    # Long after it started, it turns unready (a node restart): within the
    # start timeout it is kept ...
    reconciler.offset[0] = timedelta(hours=1)
    turned = datetime.now(timezone.utc) + timedelta(hours=1)
    reconciler.fake.states[identity] = PodState(
        "Pending", uid="u", reason="CrashLoopBackOff", unready_since=turned
    )
    report = await reconciler.reconcile_once()
    assert report.stopped == [] and report.started == []
    # ... and recovering keeps it.
    reconciler.fake.ready(identity)
    assert (await reconciler.reconcile_once()).stopped == []
    # Unready past the start timeout, it is replaced.
    reconciler.fake.states[identity] = PodState(
        "Pending", uid="u", reason="CrashLoopBackOff", unready_since=turned
    )
    reconciler.offset[0] = timedelta(hours=1, seconds=121)
    report = await reconciler.reconcile_once()
    assert (identity, "not_ready") in report.stopped
    assert len(report.started) == 1


@pytest.mark.asyncio
async def test_objects_no_live_row_names_are_swept(db, reconciler):
    connector = await _echo_connector(db)
    await _echo_image(db)
    await _bind_echo(db, connector, await _thread(db))
    await reconciler.reconcile_once()
    (pod,) = await _pods(db)
    reconciler.fake.objects = [
        ("delete_pod", "srw-drv-orphan", str(uuid4())),
        ("delete_pod", pod["pod_name"], str(pod["id"])),
    ]
    report = await reconciler.reconcile_once()
    assert report.swept == 1
    assert reconciler.fake.deleted == ["srw-drv-orphan"]


@pytest.mark.asyncio
async def test_the_exchange_answers_the_pods_identity_until_it_is_revoked(
    db, reconciler
):
    """The pod's sdi_ identity exchanges its connector's lease, never another
    connector's, and stops working when the pod is stopped."""
    connector, other = await _echo_connector(db), await _echo_connector(db)
    await _echo_image(db)
    thread = await _thread(db)
    lease = await _bind_echo(db, connector, thread)
    other_lease = await _bind_echo(db, other, thread)
    await reconciler.reconcile_once()
    pods = {str(p["connector_id"]): p for p in await _pods(db)}
    plan = reconciler.fake.plans[str(pods[connector]["id"])]
    token = base64.b64decode(plan.secret["data"]["identity"]).decode()
    exchange = ConnectorLeaseExchange(
        store=db,
        drivers=builtin_connector_drivers(echo_service_image=ECHO_REFERENCE),
        limiter=DenialLimiter(),
    )
    ok = await exchange.exchange(
        identity_token=token, lease_token=lease.token, operation="read"
    )
    assert ok.status == 200 and ok.body["credential"] == "s"
    refused = await exchange.exchange(
        identity_token=token, lease_token=other_lease.token, operation="read"
    )
    assert refused.status == 403
    assert refused.body == {"error": "driver_identity_of_another_connector"}
    await _end(db, thread)
    await reconciler.reconcile_once()
    reconciler.offset[0] = timedelta(seconds=61)
    await reconciler.reconcile_once()
    revoked = await exchange.exchange(
        identity_token=token, lease_token=lease.token, operation="read"
    )
    assert revoked.status == 401
    assert revoked.body == {"error": "driver_identity_revoked"}


@pytest.mark.asyncio
async def test_the_connector_egress_view_shows_what_its_pods_enforce(db, reconciler):
    connector = await _echo_connector(db)
    await _echo_image(db)
    await _bind_echo(db, connector, await _thread(db))
    await reconciler.reconcile_once()
    view = await connector_egress_view(db, connector, spec=ECHO_SERVICE_SPEC)
    assert view["declared"]["rules"] == [
        {"host": "${config.host}", "ports": ["${config.port}"], "protocol": "tcp"}
    ]
    (pod,) = view["pods"]
    assert pod["live"] is True and pod["ready"] is False
    assert pod["enforced"]["hosts"][0]["addresses"] == ["1.1.1.1"]
    assert pod["resolved_at"] is not None
