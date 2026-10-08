"""Real-PostgreSQL proofs for registered image drivers (connector drivers D6).

Registration under the manifest scopes, names and shadowing, which
registration a connector runs, and the bind-time binding: one pod per
execution and connector whose result the delivery fills in, a moved tag
refused at bind with the reason on the connector, the result route, and the
reconciler's revoke. Everything runs against ``schema_current.sql``; the
registry and the driver pods are fakes at their boundary.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import json
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from fastapi import HTTPException
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services import connector_bind_time as bind_time
from orchestrator.services import connector_driver_registrations as registrations
from orchestrator.services import connector_service_images as images
from orchestrator.services import datasources as datasource_operations
from orchestrator.services.connector_credential_leases import (
    LeaseOwner,
    deliver_connector_leases,
    prepare_lease_delivery,
)
from orchestrator.services.connector_driver_imports import import_server_json
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_service_hosting import (
    PodState,
    ServiceHostingSettings,
)
from orchestrator.schemas.datasources import DatasourceCreate
from shared.connectors.envelope import DriverError, DriverOutcome
from shared.connectors.images import SPEC_LABEL
from shared.oci_registry import ResolvedImage

pytestmark = pytest.mark.asyncio

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)
REGISTRY = builtin_connector_drivers()
D1 = "sha256:" + "1" * 64
D2 = "sha256:" + "2" * 64
REFERENCE = "registry.example/acme/example-driver:1"
SPEC = {
    "name": "acme.env/v1",
    "title": "Acme environment",
    "protocol_version": "1.0",
    "plane": "bind_time",
    "delivery_forms": ["env_file", "credential_file"],
    "config_schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["variable"],
        "properties": {"variable": {"type": "string"}},
    },
    "credential_slots": [
        {
            "name": "token",
            "kind": "secret_string",
            "schema": {
                "type": "object",
                "properties": {"token": {"type": "string", "writeOnly": True}},
            },
            "required": True,
        }
    ],
    "access_levels": [
        {"id": "ReadOnly", "rank": 0, "enforced_by": "told", "advisory": True},
        {"id": "ReadWrite", "rank": 1, "enforced_by": "upstream"},
    ],
    "default_access": "ReadWrite",
    "supported_backends": ["sandbox", "vm"],
}
HOSTING = ServiceHostingSettings(
    namespace="srw-connectors",
    release_namespace="srw",
    shim_image="ghcr.io/x/shim@sha256:" + "e" * 64,
    exchange_host="srw-orchestrator.srw.svc",
    exchange_port=8088,
    orchestrator_labels={"app.kubernetes.io/component": "orchestrator"},
)


def _labelled(spec: dict | None = None) -> dict[str, str]:
    return {SPEC_LABEL: json.dumps(spec or SPEC)}


class FakeRegistry:
    """The registry: each reference answers its current digest and label."""

    def __init__(self) -> None:
        self.images: dict[str, ResolvedImage] = {}

    def push(self, reference: str, digest: str, labels: dict | None = None) -> None:
        self.images[reference] = ResolvedImage(
            reference=reference,
            digest=digest,
            entrypoint=("/driver",),
            cmd=(),
            labels=labels if labels is not None else _labelled(),
        )

    async def resolve_image(self, lookup: str) -> ResolvedImage:
        name, _, digest = lookup.partition("@")
        if digest:
            for image in self.images.values():
                if image.digest == digest:
                    return image
            raise LookupError(lookup)
        return self.images[lookup]


# =============================================================================
# Fixtures
# =============================================================================


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
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=8)
    await store.connect()
    store.connector_drivers = REGISTRY
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE connector_driver_operations, connector_bind_time_bindings, "
            "connector_driver_assignments, connector_driver_registrations, "
            "connector_driver_images, security_events, srw_resource_secrets, "
            "srw_resources, datasources, jobs, threads, project_members, projects, "
            "users CASCADE"
        )
    try:
        yield store
    finally:
        bind_time.configure_bind_time(None)
        images.configure_service_images(images.ServiceImageSettings())
        await store.close()


@pytest.fixture
def registry(db):
    fake = FakeRegistry()
    fake.push(REFERENCE, D1)
    images.configure_service_images(
        images.ServiceImageSettings(resolver=fake, store=db, cache_seconds=0)
    )
    return fake


async def _user(db, name: str, *, admin: bool = False) -> dict:
    return dict(
        await db.fetchrow(
            "INSERT INTO users(display_name,is_approved,is_admin) "
            "VALUES($1,TRUE,$2) RETURNING *",
            name,
            admin,
        )
    )


async def _project(db, members: dict[str, str]) -> str:
    project = await db.fetchval("INSERT INTO projects(name) VALUES('p') RETURNING id")
    for user_id, role in members.items():
        await db.execute(
            "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,$3)",
            project,
            UUID(user_id),
            role,
        )
    return str(project)


async def _register(db, user, registry, *, scope=None, spec=None, reference=REFERENCE):
    if spec is not None:
        registry.push(
            reference,
            registry.images.get(reference, SimpleNamespace(digest=D1)).digest,
            _labelled(spec),
        )
    return await registrations.register_driver(
        db,
        user,
        scope=scope,
        image_reference=reference,
        policy=registrations.DriverTrustPolicy(),
        resolve_image=registry.resolve_image,
    )


def _dependencies(db) -> datasource_operations.DatasourceDependencies:
    return datasource_operations.DatasourceDependencies(
        store=db,
        vector_db=None,
        knowledge_index=None,
        mcp_datasources_enabled=lambda: False,
        mcp_stdio_enabled=lambda: False,
        validate_mcp_datasource=lambda _url, _creds: None,
        connector_drivers=REGISTRY,
    )


async def _connector(
    db, user, *, name="acme", driver=None, registration_id=None, project_ids=None
):
    async def approve():
        return user

    async def owner(_project):
        return None

    body = DatasourceCreate(
        name=name,
        type="image_driver",
        credentials={"token": "upstream-secret"},
        config={"variable": "ACME_TOKEN"},
        driver=driver,
        driver_registration_id=registration_id,
        **(
            {"scope_mode": "projects", "project_ids": project_ids}
            if project_ids
            else {}
        ),
    )
    created = await datasource_operations.create_datasource(
        body=body,
        require_approved_user=approve,
        require_project_owner=owner,
        dependencies=_dependencies(db),
    )
    return str(created["id"])


async def _job(db, status: str = "processing") -> str:
    job_id = uuid4()
    await db.execute(
        "INSERT INTO jobs (id, description, status, context) "
        "VALUES ($1, 'd6', $2, '{}'::jsonb)",
        job_id,
        status,
    )
    return str(job_id)


# =============================================================================
# Registration
# =============================================================================


class TestRegistration:
    async def test_an_account_registration_is_its_users_alone(self, db, registry):
        alice = await _user(db, "alice")
        bob = await _user(db, "bob")
        admin = await _user(db, "admin", admin=True)
        registration = await _register(db, alice, registry)
        assert registration.scope == {"kind": "Account", "name": str(alice["id"])}
        assert registration.image_digest == D1
        assert registration.spec_source == "label"
        assert registration.spec.name == "acme.env/v1"
        assert [
            r.id for r in await registrations.list_visible_registrations(db, alice)
        ] == [registration.id]
        assert await registrations.list_visible_registrations(db, bob) == []
        with pytest.raises(HTTPException) as caught:
            await registrations.get_visible_registration(db, bob, registration.id)
        assert caught.value.status_code == 404
        assert (
            await registrations.get_visible_registration(db, admin, registration.id)
        ).id
        # Bob cannot use it for a connector, by id or by name.
        for kwargs in ({"registration_id": registration.id}, {"driver": "acme.env/v1"}):
            with pytest.raises(HTTPException) as caught:
                await _connector(db, bob, **kwargs)
            assert caught.value.status_code == 404
        event = await db.fetchrow(
            "SELECT * FROM security_events WHERE event_type='connector_driver_registered'"
        )
        assert "tier=custom" in event["detail"]

    async def test_project_editors_register_and_viewers_read(self, db, registry):
        editor = await _user(db, "editor")
        viewer = await _user(db, "viewer")
        project = await _project(
            db, {str(editor["id"]): "editor", str(viewer["id"]): "viewer"}
        )
        scope = {"kind": "Project", "name": project}
        with pytest.raises(HTTPException) as caught:
            await _register(db, viewer, registry, scope=scope)
        assert caught.value.status_code == 403
        registration = await _register(db, editor, registry, scope=scope)
        assert [
            r.id for r in await registrations.list_visible_registrations(db, viewer)
        ] == [registration.id]
        outsider = await _user(db, "outsider")
        assert await registrations.list_visible_registrations(db, outsider) == []

    async def test_only_administrators_publish_to_the_catalog(self, db, registry):
        user = await _user(db, "user")
        admin = await _user(db, "admin", admin=True)
        catalog = {"kind": "Catalog", "name": "shared"}
        with pytest.raises(HTTPException) as caught:
            await _register(db, user, registry, scope=catalog)
        assert caught.value.status_code == 403
        shared = await _register(db, admin, registry, scope=catalog)
        assert [
            r.id for r in await registrations.list_visible_registrations(db, user)
        ] == [shared.id]

    async def test_srw_names_are_refused(self, db, registry):
        user = await _user(db, "user")
        with pytest.raises(HTTPException) as caught:
            await _register(db, user, registry, spec={**SPEC, "name": "srw.git/v2"})
        assert caught.value.status_code == 422
        assert "SRW's own" in caught.value.detail

    async def test_no_shadowing_of_the_catalog_and_one_name_per_scope(
        self, db, registry
    ):
        user = await _user(db, "user")
        admin = await _user(db, "admin", admin=True)
        await _register(db, user, registry)
        with pytest.raises(HTTPException) as caught:
            await _register(db, user, registry)
        assert caught.value.status_code == 409
        await _register(
            db, admin, registry, scope={"kind": "Catalog", "name": "shared"}
        )
        other = await _user(db, "other")
        with pytest.raises(HTTPException) as caught:
            await _register(db, other, registry)
        assert caught.value.status_code == 409
        assert "cannot shadow" in caught.value.detail

    async def test_a_name_resolves_catalog_then_project_then_account(
        self, db, registry
    ):
        user = await _user(db, "user")
        admin = await _user(db, "admin", admin=True)
        project = await _project(db, {str(user["id"]): "editor"})
        account = await _register(db, user, registry)
        in_project = await _register(
            db, user, registry, scope={"kind": "Project", "name": project}
        )
        resolve = registrations.resolve_registration_for_use
        assert (await resolve(db, user, name="acme.env/v1")).id == account.id
        assert (
            await resolve(db, user, name="acme.env/v1", project_id=project)
        ).id == in_project.id
        # A connector pins what it resolved: a later Catalog registration of
        # the name never moves it.
        pinned = await _connector(db, user, driver="acme.env/v1")
        shared = await _register(
            db, admin, registry, scope={"kind": "Catalog", "name": "shared"}
        )
        assert (await resolve(db, user, name="acme.env/v1")).id == shared.id
        assert (
            await registrations.registration_for_connector(db, pinned)
        ).id == account.id

    async def test_an_unlabelled_image_needs_its_spec_operation(self, db, registry):
        user = await _user(db, "user")
        registry.push("registry.example/acme/bare:1", D2, labels={})
        with pytest.raises(HTTPException) as caught:
            await registrations.register_driver(
                db,
                user,
                scope=None,
                image_reference="registry.example/acme/bare:1",
                policy=registrations.DriverTrustPolicy(),
                resolve_image=registry.resolve_image,
            )
        assert "no io.srw.driver.spec label" in caught.value.detail

        async def run_spec(reference, resolved):
            assert resolved.digest == D2
            return SPEC

        registration = await registrations.register_driver(
            db,
            user,
            scope=None,
            image_reference="registry.example/acme/bare:1",
            policy=registrations.DriverTrustPolicy(),
            resolve_image=registry.resolve_image,
            run_spec=run_spec,
        )
        assert registration.spec_source == "spec_operation"

    async def test_the_in_pod_plane_needs_trust(self, db, registry):
        user = await _user(db, "user")
        spec = {**SPEC, "plane": "in_pod"}
        registry.push(REFERENCE, D1, _labelled(spec))
        for policy, expected in (
            (registrations.DriverTrustPolicy(), "trustedRepositories"),
            (
                registrations.DriverTrustPolicy(
                    trusted_repositories=("registry.example/acme",)
                ),
                "not available in this release",
            ),
            (
                registrations.DriverTrustPolicy(custom_drivers_privileged=True),
                "not available in this release",
            ),
        ):
            with pytest.raises(HTTPException) as caught:
                await registrations.register_driver(
                    db,
                    user,
                    scope=None,
                    image_reference=REFERENCE,
                    policy=policy,
                    resolve_image=registry.resolve_image,
                )
            assert expected in caught.value.detail

    async def test_a_registration_in_use_is_not_deleted(self, db, registry):
        user = await _user(db, "user")
        registration = await _register(db, user, registry)
        connector = await _connector(db, user, registration_id=registration.id)
        with pytest.raises(HTTPException) as caught:
            await registrations.delete_registration(db, user, registration.id)
        assert caught.value.status_code == 409
        await db.delete_datasource(connector)
        await registrations.delete_registration(db, user, registration.id)
        assert await registrations.list_visible_registrations(db, user) == []


class TestConnectors:
    async def test_config_and_credentials_follow_the_registered_spec(
        self, db, registry
    ):
        user = await _user(db, "user")
        registration = await _register(db, user, registry)

        async def approve():
            return user

        async def owner(_project):
            return None

        for body, message in (
            ({"config": {}, "credentials": {"token": "t"}}, "config"),
            ({"config": {"variable": "X"}, "credentials": {}}, "token credential"),
            (
                {"config": {"variable": "X"}, "credentials": {"other": "t"}},
                "no credential slot",
            ),
        ):
            with pytest.raises(HTTPException) as caught:
                await datasource_operations.create_datasource(
                    body=DatasourceCreate(
                        name="bad",
                        type="image_driver",
                        driver_registration_id=registration.id,
                        **body,
                    ),
                    require_approved_user=approve,
                    require_project_owner=owner,
                    dependencies=_dependencies(db),
                )
            assert caught.value.status_code == 400
            assert message in caught.value.detail

    async def test_the_connector_resource_names_the_registered_driver(
        self, db, registry
    ):
        user = await _user(db, "user")
        registration = await _register(db, user, registry)
        connector = await _connector(db, user, registration_id=registration.id)
        document = json.loads(
            await db.fetchval(
                "SELECT document FROM srw_resources WHERE id=$1", UUID(connector)
            )
        )
        assert document["spec"]["driver"] == "acme.env/v1"
        assert document["spec"]["config"] == {"variable": "ACME_TOKEN"}
        # The secret's keys follow the registered slot (and the shape SRW keeps).
        assert "token" in document["spec"]["credentials"]
        status = await registrations.connector_driver_status(db, connector)
        assert status["registration"]["id"] == registration.id
        assert status["last_bind"] is None

    async def test_only_a_registered_type_names_a_driver(self, db, registry):
        user = await _user(db, "user")

        async def approve():
            return user

        with pytest.raises(HTTPException) as caught:
            await datasource_operations.create_datasource(
                body=DatasourceCreate(
                    name="g", type="generic", credentials={}, driver="acme.env/v1"
                ),
                require_approved_user=approve,
                require_project_owner=approve,
                dependencies=_dependencies(db),
            )
        assert caught.value.status_code == 400


# =============================================================================
# Bind-time bindings
# =============================================================================


class FakeOperations:
    """The pod runner's boundary: the outcome of each operation it runs."""

    def __init__(self, outcomes: dict[str, DriverOutcome]):
        self.outcomes = outcomes
        self.calls: list[dict] = []
        self.settings = bind_time.BindTimeSettings(hosting=HOSTING, wait_seconds=5)

    async def run(self, **kwargs) -> DriverOutcome:
        self.calls.append(kwargs)
        return self.outcomes[kwargs["operation"]]


def _bound(*entries, driver="acme.env/v1") -> DriverOutcome:
    return DriverOutcome(
        result={
            "binding": {
                "driver": driver,
                "name": "acme",
                "access": "ReadWrite",
                "entries": list(entries),
            }
        },
        driver_state="state-1",
    )


ENV = {
    "recipient": "workspace",
    "form": "env_file",
    "value": {"name": "ACME_TOKEN", "value": "minted-for-this-binding"},
    "collision": "error",
}


def _runtime(db, operations) -> bind_time.BindTimeRuntime:
    runtime = bind_time.BindTimeRuntime(store=db, operations=operations)
    bind_time.configure_bind_time(runtime)
    return runtime


def _entry(connector: str) -> dict:
    return {
        "type": "image_driver",
        "name": "acme",
        "credentials": {},
        "project_read_only": False,
        "datasource_id": connector,
    }


class TestBinding:
    async def test_one_pod_binds_and_every_delivery_reuses_it(self, db, registry):
        user = await _user(db, "user")
        registration = await _register(db, user, registry)
        connector = await _connector(db, user, registration_id=registration.id)
        job = await _job(db)
        operations = FakeOperations({"bind": _bound(ENV)})
        _runtime(db, operations)
        owner = LeaseOwner.job(job)
        for _ in range(2):
            entries = [_entry(connector)]
            await prepare_lease_delivery(db, entries, owner=owner)
            async with db.acquire() as conn:
                async with conn.transaction():
                    assert (
                        await deliver_connector_leases(conn, entries, owner=owner) == 1
                    )
            assert entries[0]["credentials"] == {
                "env_vars": {"ACME_TOKEN": "minted-for-this-binding"}
            }
        (call,) = operations.calls
        request = call["request"]
        assert request.operation == "bind"
        assert request.config == {"variable": "ACME_TOKEN"}
        assert request.credentials == {"token": "upstream-secret"}
        assert request.access == "ReadWrite"
        assert request.execution.kind == "job" and request.execution.id == job
        row = await db.fetchrow("SELECT * FROM connector_bind_time_bindings")
        assert str(row["id"]) == request.binding_id
        # The record of the bind: reference, digest, time, spec hash, protocol.
        assert row["image_reference"] == REFERENCE
        assert row["image_digest"] == D1
        assert row["resolved_at"] is not None
        assert row["spec_hash"].startswith("sha256:")
        assert row["protocol_version"] == "1.0"
        assert "minted" not in (row["delivery_ciphertext"] or "")
        status = await registrations.connector_driver_status(
            db, connector, with_bindings=True
        )
        assert status["last_bind"]["status"] == "bound"
        assert status["bindings"][0]["digest"] == D1
        assert status["bindings"][0]["owner"] == {"kind": "job", "id": job}

    async def test_a_moved_tag_with_an_incompatible_spec_is_refused(self, db, registry):
        user = await _user(db, "user")
        registration = await _register(db, user, registry)
        connector = await _connector(db, user, registration_id=registration.id)
        operations = FakeOperations({"bind": _bound(ENV)})
        _runtime(db, operations)
        incompatible = copy.deepcopy(SPEC)
        incompatible["credential_slots"] = []
        incompatible["config_schema"]["required"] = ["variable", "region"]
        registry.push(REFERENCE, D2, _labelled(incompatible))
        owner = LeaseOwner.job(await _job(db))
        entries = [_entry(connector)]
        await prepare_lease_delivery(db, entries, owner=owner)
        assert operations.calls == []  # no pod runs
        async with db.acquire() as conn:
            with pytest.raises(bind_time.BindTimeRefused) as caught:
                await deliver_connector_leases(conn, entries, owner=owner)
        assert "changed its contract" in str(caught.value)
        assert "credential slots disappeared: token" in str(caught.value)
        assert entries[0]["credentials"] == {}
        status = await registrations.connector_driver_status(db, connector)
        assert status["last_bind"]["status"] == "failed"
        assert "changed its contract" in status["last_bind"]["message"]
        assert status["last_bind"]["digest"] == D2
        audit = await db.fetchval(
            "SELECT count(*) FROM security_events "
            "WHERE event_type='connector_driver_image_refused'"
        )
        assert audit == 1

    async def test_a_compatible_moved_tag_binds_on_the_new_digest(self, db, registry):
        user = await _user(db, "user")
        registration = await _register(db, user, registry)
        connector = await _connector(db, user, registration_id=registration.id)
        operations = FakeOperations({"bind": _bound(ENV)})
        _runtime(db, operations)
        registry.push(REFERENCE, D2, _labelled({**SPEC, "title": "Acme 1.1"}))
        owner = LeaseOwner.job(await _job(db))
        await prepare_lease_delivery(db, [_entry(connector)], owner=owner)
        assert operations.calls[0]["image"].digest == D2

    async def test_a_binding_srw_will_not_deliver_fails_with_why(self, db, registry):
        user = await _user(db, "user")
        registration = await _register(db, user, registry)
        connector = await _connector(db, user, registration_id=registration.id)
        harness = {**ENV, "recipient": "harness"}
        reserved = {**ENV, "value": {"name": "SRW_TOKEN", "value": "x"}}
        for entry, why in ((harness, "workspace"), (reserved, "reserved")):
            operations = FakeOperations({"bind": _bound(entry)})
            _runtime(db, operations)
            owner = LeaseOwner.job(await _job(db))
            await prepare_lease_delivery(db, [_entry(connector)], owner=owner)
            status = await registrations.connector_driver_status(db, connector)
            assert "will not deliver" in status["last_bind"]["message"]
            assert why in status["last_bind"]["message"]

    async def test_a_driver_error_is_shown_and_retried_after_a_pause(
        self, db, registry
    ):
        user = await _user(db, "user")
        registration = await _register(db, user, registry)
        connector = await _connector(db, user, registration_id=registration.id)
        failing = DriverOutcome(
            error=DriverError(
                "credentials", "The token was refused", "upstream said 401"
            )
        )
        operations = FakeOperations({"bind": failing})
        _runtime(db, operations)
        owner = LeaseOwner.job(await _job(db))
        entries = [_entry(connector)]
        await prepare_lease_delivery(db, entries, owner=owner)
        await prepare_lease_delivery(db, entries, owner=owner)
        assert len(operations.calls) == 1  # failed moments ago: no second pod
        async with db.acquire() as conn:
            with pytest.raises(bind_time.BindTimeRefused, match="token was refused"):
                await deliver_connector_leases(conn, entries, owner=owner)
        row = await db.fetchrow("SELECT * FROM connector_bind_time_bindings")
        assert row["error_class"] == "credentials"
        assert "401" not in row["error_message"]  # detail is operator-only
        with mock.patch.object(bind_time, "BIND_RETRY_SECONDS", 0):
            operations.outcomes["bind"] = _bound(ENV)
            await prepare_lease_delivery(db, entries, owner=owner)
        assert len(operations.calls) == 2

    async def test_a_delivery_that_prepared_nothing_starts_the_bind(self, db, registry):
        user = await _user(db, "user")
        registration = await _register(db, user, registry)
        connector = await _connector(db, user, registration_id=registration.id)
        operations = FakeOperations({"bind": _bound(ENV)})
        runtime = _runtime(db, operations)
        owner = LeaseOwner.job(await _job(db))
        entries = [_entry(connector)]
        async with db.acquire() as conn:
            with pytest.raises(bind_time.BindTimePending):
                await deliver_connector_leases(conn, entries, owner=owner)
        for _ in range(100):
            if (
                await db.fetchval("SELECT status FROM connector_bind_time_bindings")
                == "bound"
                and not runtime.inflight
            ):
                break
            await asyncio.sleep(0.05)
        async with db.acquire() as conn:
            assert await deliver_connector_leases(conn, entries, owner=owner) == 1

    async def test_without_driver_pods_a_registered_connector_is_refused(
        self, db, registry
    ):
        user = await _user(db, "user")
        registration = await _register(db, user, registry)
        connector = await _connector(db, user, registration_id=registration.id)
        bind_time.configure_bind_time(None)
        owner = LeaseOwner.job(await _job(db))
        async with db.acquire() as conn:
            with pytest.raises(bind_time.BindTimeRefused, match="no driver pods"):
                await deliver_connector_leases(conn, [_entry(connector)], owner=owner)


class TestRevocation:
    async def test_an_ended_job_s_binding_is_revoked_with_its_state(self, db, registry):
        user = await _user(db, "user")
        registration = await _register(db, user, registry)
        connector = await _connector(db, user, registration_id=registration.id)
        operations = FakeOperations(
            {"bind": _bound(ENV), "revoke": DriverOutcome(result={})}
        )
        runtime = _runtime(db, operations)
        job = await _job(db)
        await prepare_lease_delivery(db, [_entry(connector)], owner=LeaseOwner.job(job))
        report = await bind_time.reconcile_bind_time_once(runtime, object())
        assert report.marked == 0
        await db.execute("UPDATE jobs SET status='completed' WHERE id=$1", UUID(job))
        with mock.patch.object(bind_time, "_sweep", return_value=0):
            report = await bind_time.reconcile_bind_time_once(runtime, object())
        assert report.marked == 1 and len(report.revoked) == 1
        revoke = operations.calls[-1]
        assert revoke["operation"] == "revoke"
        assert revoke["request"].driver_state == "state-1"
        assert revoke["image"].digest == D1
        row = await db.fetchrow("SELECT * FROM connector_bind_time_bindings")
        assert row["status"] == "revoked"
        assert row["revoke_reason"] == "execution_ended"
        assert row["delivery_ciphertext"] is None

    async def test_a_transient_revoke_failure_waits_for_the_next_pass(
        self, db, registry
    ):
        user = await _user(db, "user")
        registration = await _register(db, user, registry)
        connector = await _connector(db, user, registration_id=registration.id)
        operations = FakeOperations(
            {
                "bind": _bound(ENV),
                "revoke": DriverOutcome(error=DriverError("transient", "try later")),
            }
        )
        runtime = _runtime(db, operations)
        job = await _job(db)
        await prepare_lease_delivery(db, [_entry(connector)], owner=LeaseOwner.job(job))
        await db.delete_datasource(connector)
        with mock.patch.object(bind_time, "_sweep", return_value=0):
            report = await bind_time.reconcile_bind_time_once(runtime, object())
        assert report.marked == 1 and report.revoked == []
        row = await db.fetchrow("SELECT * FROM connector_bind_time_bindings")
        assert row["status"] == "revoking"
        assert row["revoke_reason"] == "connector_deleted"
        assert row["revoke_error"] == "try later"


# =============================================================================
# The pod runner and the result route
# =============================================================================


class FakePods:
    """The driver namespace: a launched pod's shim posts at once."""

    def __init__(self, store, answer):
        self.store = store
        self.answer = answer
        self.plans = []
        self.removed = []

    async def service_cluster_ip(self, name, namespace):
        return "10.43.0.20"

    async def launch_operation(self, plan):
        self.plans.append(plan)
        token = base64.b64decode(plan.secret["data"]["identity"]).decode()
        request = json.loads(base64.b64decode(plan.secret["data"]["request.json"]))
        lines, code = self.answer(request)
        posted = {
            "operation": request["operation"],
            "exit_code": code,
            "lines": lines,
            "protocol_version": "1.0",
        }
        self.post = asyncio.get_running_loop().create_task(
            bind_time.record_operation_result(
                self.store, identity_token=token, posted=posted
            )
        )
        self.token = token
        return "pod-uid"

    async def observe_operation(self, name):
        return PodState("Running")

    async def remove_operation(self, name):
        self.removed.append(name)
        return True


class TestTheRunner:
    def _operations(self, db, pods, **settings):
        return bind_time.DriverOperations(
            store=db,
            settings=bind_time.BindTimeSettings(hosting=HOSTING, **settings),
            runtime=lambda: pods,
        )

    async def test_an_operation_runs_in_its_own_pod_and_reads_its_post(
        self, db, registry
    ):
        pods = FakePods(
            db,
            lambda request: (
                [{"type": "result", "result": {"status": "SUCCEEDED"}}],
                0,
            ),
        )
        operations = self._operations(db, pods)
        image = await images.resolve_driver_image(
            db, driver="acme.env/v1", reference=REFERENCE
        )
        outcome = await operations.run(
            operation="check",
            driver="acme.env/v1",
            image=image,
            request=bind_time.DriverRequest(
                operation="check", config={"variable": "X"}
            ),
            spec=None,
            config={},
        )
        assert outcome.result == {"status": "SUCCEEDED"}
        (plan,) = pods.plans
        assert pods.removed == [plan.pod_identity.pod_name]
        row = await db.fetchrow("SELECT * FROM connector_driver_operations")
        assert row["status"] == "finished"
        assert row["exit_code"] == 0
        assert row["removed_at"] is not None
        assert row["token_hash"] != pods.token.encode()
        # One post per identity: a second is refused.
        again = await bind_time.record_operation_result(
            db,
            identity_token=pods.token,
            posted={"operation": "check", "exit_code": 0, "lines": []},
        )
        assert again[0] == 409

    async def test_the_result_route_knows_only_its_own_identities(self, db, registry):
        status, _ = await bind_time.record_operation_result(
            db,
            identity_token="sdi_" + "A" * 49,
            posted={"operation": "bind", "exit_code": 0, "lines": []},
        )
        assert status == 401
        status, _ = await bind_time.record_operation_result(
            db, identity_token="not-a-token", posted={}
        )
        assert status == 401

    async def test_the_installation_cap_is_a_capacity_error(self, db, registry):
        pods = FakePods(db, lambda request: ([], 1))
        operations = self._operations(db, pods, max_pods=0)
        image = await images.resolve_driver_image(
            db, driver="acme.env/v1", reference=REFERENCE
        )
        with pytest.raises(bind_time.BindTimeCapacity, match="cap of 0"):
            await operations.run(
                operation="check",
                driver="acme.env/v1",
                image=image,
                request=bind_time.DriverRequest(operation="check"),
                spec=None,
                config={},
            )
        assert pods.plans == []

    async def test_a_protocol_breach_is_a_system_error(self, db, registry):
        pods = FakePods(db, lambda request: ([{"type": "result", "result": {}}], 0))
        operations = self._operations(db, pods)
        image = await images.resolve_driver_image(
            db, driver="acme.env/v1", reference=REFERENCE
        )
        outcome = await operations.run(
            operation="bind",
            driver="acme.env/v1",
            image=image,
            request=bind_time.DriverRequest(operation="bind", binding_id="b"),
            spec=None,
            config={},
        )
        assert outcome.error is not None
        assert outcome.error.error_class == "system"


class TestServerJsonImport:
    async def test_an_oci_package_registers_as_a_managed_mcp_driver(self, db, registry):
        user = await _user(db, "user")
        registry.push("ghcr.io/acme/weather:1.4.2", D2, labels={})
        server = {
            "name": "io.github.acme/weather",
            "version": "1.4.2",
            "description": "Weather",
            "packages": [
                {
                    "registryType": "oci",
                    "identifier": "ghcr.io/acme/weather:1.4.2",
                    "transport": {
                        "type": "streamable-http",
                        "url": "http://localhost:9000/mcp",
                    },
                }
            ],
        }
        registration = await import_server_json(
            db,
            user,
            server=server,
            scope=None,
            package=None,
            policy=registrations.DriverTrustPolicy(),
            resolve_image=registry.resolve_image,
        )
        assert registration.name == "io.github.acme.weather/v1"
        assert registration.plane == "service"
        assert registration.spec_source == "server_json"
        assert registration.source_document["name"] == "io.github.acme/weather"
        # Not bindable in this release: registered connectors are bind-time.
        with pytest.raises(HTTPException) as caught:
            await registrations.resolve_registration_for_use(
                db, user, registration_id=registration.id
            )
        assert caught.value.status_code == 422

    async def test_an_image_owned_by_another_server_is_refused(self, db, registry):
        user = await _user(db, "user")
        registry.push(
            "ghcr.io/acme/weather:1",
            D2,
            labels={"io.modelcontextprotocol.server.name": "io.github.evil/weather"},
        )
        server = {
            "name": "io.github.acme/weather",
            "version": "1.0.0",
            "packages": [
                {
                    "registryType": "oci",
                    "identifier": "ghcr.io/acme/weather:1",
                    "transport": {
                        "type": "streamable-http",
                        "url": "http://l:9000/mcp",
                    },
                }
            ],
        }
        with pytest.raises(HTTPException) as caught:
            await import_server_json(
                db,
                user,
                server=server,
                scope=None,
                package=None,
                policy=registrations.DriverTrustPolicy(),
                resolve_image=registry.resolve_image,
            )
        assert "io.github.evil/weather" in caught.value.detail
