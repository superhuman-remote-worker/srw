"""Control-plane connector drivers: the registry and each driver's contract.

The goldens (``tests/test_connector_goldens_*.py``) pin what the API and the
payload do end to end; these tests pin the driver seams themselves.
"""

from __future__ import annotations

import logging
import os
from dataclasses import replace

import pytest
from fastapi import HTTPException

from orchestrator.services.connector_drivers import (
    ConnectorDriverRegistry,
    builtin_connector_drivers,
)
from orchestrator.services.connector_drivers.base import BindContext, DeploymentGates
from orchestrator.services.connector_drivers.manifest import EnvDriver, FilesDriver
from shared.connectors import validate_binding


# =============================================================================
# Registry
# =============================================================================


class TestRegistry:
    def test_builtin_registry_serves_the_generic_hosting_drivers(self):
        registry = builtin_connector_drivers()
        assert isinstance(registry.manifest_driver("srw.env/v1"), EnvDriver)
        assert isinstance(registry.manifest_driver("srw.files/v1"), FilesDriver)
        assert registry.manifest_driver("srw.datasource/v1") is None
        assert registry.for_type("ftp") is None
        assert registry.for_type(None) is None

    def test_a_driver_name_registers_once(self):
        with pytest.raises(ValueError, match="registered twice"):
            ConnectorDriverRegistry([EnvDriver(), EnvDriver()])

    def test_an_invalid_spec_is_refused_at_registration(self):
        driver = EnvDriver()
        driver.spec = replace(driver.spec, name="env")
        with pytest.raises(ValueError, match="driver name 'env'"):
            ConnectorDriverRegistry([driver])

    def test_the_application_owns_one_registry(self):
        from orchestrator.application import build_application_resources

        resources = build_application_resources(None)
        assert isinstance(resources.connector_drivers, ConnectorDriverRegistry)


# =============================================================================
# srw.env/v1 and srw.files/v1
# =============================================================================


async def _resolve(ref):
    return f"secret-of-{ref['name']}"


class TestManifestDrivers:
    @pytest.mark.asyncio
    async def test_env_binds_values_and_credential_selections_in_order(self):
        connector = {
            "driver": "srw.env/v1",
            "config": {"env": {"REGION": "eu", "TOKEN": {"credential": "api"}}},
            "credentials": {"api": {"secretRef": {"name": "api-token"}}},
        }
        driver = EnvDriver()
        driver.validate(connector)
        binding = await driver.bind(
            connector, alias="billing", resolve_credential=_resolve
        )
        assert validate_binding(binding.to_json()) == []
        assert binding.name == "billing"
        assert binding.access is None
        assert [(e.recipient, e.form, dict(e.value)) for e in binding.entries] == [
            ("harness_pod", "pod_env", {"name": "REGION", "value": "eu"}),
            (
                "harness_pod",
                "pod_env",
                {"name": "TOKEN", "value": "secret-of-api-token"},
            ),
        ]

    @pytest.mark.asyncio
    async def test_files_bind_under_the_bindings_root(self):
        connector = {
            "driver": "srw.files/v1",
            "config": {"files": {"/run/srw/bindings/ca.pem": "PEM"}},
        }
        binding = await FilesDriver().bind(
            connector, alias="ca", resolve_credential=_resolve
        )
        assert [dict(e.value) for e in binding.entries] == [
            {"path": "/run/srw/bindings/ca.pem", "content": "PEM"}
        ]

    @pytest.mark.asyncio
    async def test_files_outside_the_bindings_root_are_refused(self):
        connector = {"driver": "srw.files/v1", "config": {"files": {"/etc/x": "y"}}}
        with pytest.raises(HTTPException) as caught:
            await FilesDriver().bind(connector, alias="x", resolve_credential=_resolve)
        assert caught.value.status_code == 422
        assert (
            caught.value.detail == "Connector files must be under /run/srw/bindings/."
        )

    @pytest.mark.asyncio
    async def test_an_undeclared_credential_selection_is_not_a_string(self):
        connector = {
            "driver": "srw.env/v1",
            "config": {"env": {"TOKEN": {"credential": "missing"}}},
        }
        with pytest.raises(HTTPException) as caught:
            await EnvDriver().bind(connector, alias="x", resolve_credential=_resolve)
        assert caught.value.detail == (
            "Connector values must be strings or declared credential selections."
        )

    @pytest.mark.parametrize(
        ("connector", "detail"),
        [
            (
                {"driver": "srw.env/v1", "config": {"env": {}}, "access": "ReadOnly"},
                "Env/file delivery cannot enforce ReadOnly/ReadWrite; use "
                "credentials scoped by the external resource.",
            ),
            (
                {"driver": "srw.env/v1", "config": {"env": [], "files": {}}},
                "Invalid env/file connector configuration.",
            ),
            (
                {"driver": "srw.env/v1", "config": {"env": {}, "extra": 1}},
                "Invalid env/file connector configuration.",
            ),
        ],
    )
    def test_validation_refuses_what_delivery_cannot_honour(self, connector, detail):
        with pytest.raises(HTTPException) as caught:
            EnvDriver().validate(connector)
        assert (caught.value.status_code, caught.value.detail) == (422, detail)

    @pytest.mark.asyncio
    async def test_no_access_level_and_no_connection_test(self):
        driver = EnvDriver()
        assert driver.effective_access({"driver": "srw.env/v1"}) is None
        assert (await driver.check({}))["status"] == "unsupported"


# =============================================================================
# Datasource drivers
# =============================================================================

_REGISTRY = builtin_connector_drivers()


def _bind_context() -> BindContext:
    gates = DeploymentGates(
        mcp_datasources_enabled=lambda: True, mcp_stdio_enabled=lambda: True
    )
    return BindContext(
        gates=gates, logger=logging.getLogger(__name__), default_known_hosts=""
    )


@pytest.mark.parametrize("type_id", _REGISTRY.type_ids())
class TestDatasourceDrivers:
    def test_the_driver_serves_its_builtin_spec(self, type_id):
        from shared.connectors import spec_for_type

        driver = _REGISTRY.for_type(type_id)
        assert driver.spec is spec_for_type(type_id)
        assert driver.type_id == type_id
        assert _REGISTRY.get(driver.spec.name) is driver

    @pytest.mark.parametrize("read_only", [False, True, None])
    def test_effective_access_is_one_of_the_drivers_levels(self, type_id, read_only):
        driver = _REGISTRY.for_type(type_id)
        row = {"type": type_id, "project_read_only": read_only, "config": {}}
        level = driver.effective_access(row)
        assert driver.spec.access_level(level) is not None

    @pytest.mark.asyncio
    async def test_revoke_is_a_no_op(self, type_id):
        driver = _REGISTRY.for_type(type_id)
        ctx = _bind_context()
        assert await driver.revoke({"type": type_id}, ctx=ctx) is None


def test_the_application_composes_its_registry_into_both_seams():
    from orchestrator.application import (
        build_application_resources,
        preparation,
        projects,
    )

    resources = build_application_resources(None)
    crud = projects.datasources_dependencies(resources).operations
    payload = preparation.datasource_payload_dependencies(resources)
    assert crud.connector_drivers is resources.connector_drivers
    assert payload.connector_drivers is resources.connector_drivers
    # The default SSH pins are the application's settings, read per use.
    resources.settings.workspace_ssh_known_hosts = "github.com ssh-ed25519 K"
    assert payload.workspace_ssh_known_hosts() == "github.com ssh-ed25519 K"


def test_every_type_has_a_driver_of_its_own():
    """The legacy adapter is gone: no type falls back to shared code."""
    from orchestrator.services.connector_drivers import builtin

    assert not hasattr(builtin, "LegacyDatasourceDriver")
    assert len({type(_REGISTRY.for_type(t)) for t in ("repository", "ssh_key")}) == 2


def test_only_ssh_key_connectors_deliver_workspace_ssh_identities():
    from orchestrator.services.connector_drivers.base import (
        SupportsTestOverrides,
        SupportsWorkspaceSshIdentity,
    )

    for capability in (SupportsTestOverrides, SupportsWorkspaceSshIdentity):
        assert {
            type_id
            for type_id in _REGISTRY.type_ids()
            if isinstance(_REGISTRY.for_type(type_id), capability)
        } == {"repository", "ssh_key"}


def test_only_the_knowledge_base_has_index_capabilities():
    from orchestrator.services.connector_drivers.base import (
        SupportsIndexOperations,
        SupportsWriteEffects,
    )

    for capability in (SupportsWriteEffects, SupportsIndexOperations):
        assert {
            type_id
            for type_id in _REGISTRY.type_ids()
            if isinstance(_REGISTRY.for_type(type_id), capability)
        } == {"kb"}


class TestKnowledgeBaseWriteEffects:
    @pytest.fixture
    def recorded(self, monkeypatch):
        from orchestrator.services import knowledge_index

        calls = []

        async def mark(datasource_id, *, dependencies):
            calls.append(("pending", datasource_id))

        def schedule(datasource_id, *, force_full, dependencies):
            calls.append(("rebuild", datasource_id, force_full))

        monkeypatch.setattr(knowledge_index, "mark_kb_datasource_pending", mark)
        monkeypatch.setattr(knowledge_index, "schedule_kb_datasource_reindex", schedule)
        return calls

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("created", "reindex_required", "rebuilds"),
        [(True, False, True), (False, True, True), (False, False, False)],
    )
    async def test_a_create_or_an_indexing_edit_rebuilds(
        self, recorded, created, reindex_required, rebuilds
    ):
        from orchestrator.services.connector_drivers.base import NormalizedConnector

        normalized = NormalizedConnector(
            None, None, None, reindex_required=reindex_required
        )
        await _REGISTRY.for_type("kb").after_write(
            "kb-1", normalized, created=created, knowledge_index=object()
        )
        expected = [("pending", "kb-1"), ("rebuild", "kb-1", True)]
        assert recorded == (expected if rebuilds else [])


def test_no_seam_falls_back_to_a_registry_of_its_own():
    """The registry is the application's, passed explicitly everywhere."""
    import dataclasses
    import inspect

    from orchestrator.services.agent_datasource_payload import (
        DatasourcePayloadDependencies,
    )
    from orchestrator.services.datasources import DatasourceDependencies
    from orchestrator.services.manifest_execution import ManifestExecutionService
    from orchestrator.services.user_administration import (
        UserAdministrationDependencies,
    )

    required = [
        (DatasourceDependencies, "connector_drivers"),
        (DatasourcePayloadDependencies, "connector_drivers"),
        (DatasourcePayloadDependencies, "workspace_ssh_known_hosts"),
        (UserAdministrationDependencies, "connector_drivers"),
    ]
    for dependencies, name in required:
        declared = {f.name: f for f in dataclasses.fields(dependencies)}[name]
        assert declared.default is dataclasses.MISSING
        assert declared.default_factory is dataclasses.MISSING
    parameter = inspect.signature(ManifestExecutionService).parameters[
        "connector_drivers"
    ]
    assert parameter.default is inspect.Parameter.empty


# =============================================================================
# Platform rows: what SRW creates itself goes through validate too
# =============================================================================


class TestPlatformConnectors:
    @pytest.mark.asyncio
    async def test_a_managed_row_is_normalized_as_a_create_would(self):
        from orchestrator.services.connector_drivers.platform import (
            validate_platform_connector,
        )

        normalized = await validate_platform_connector(
            builtin_connector_drivers(),
            "neo4j",
            name="Default Neo4j",
            connection_url="bolt://graph:7687",
            credentials={"username": "neo4j", "password": "pw"},
        )
        assert normalized.connection_url == "bolt://graph:7687"
        assert normalized.credentials == {"username": "neo4j", "password": "pw"}
        assert normalized.config == {}

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("ds_type", "kwargs", "message"),
        [
            ("ftp", {}, "No connector driver serves type 'ftp'"),
            ("postgresql", {"config": {"x": 1}}, "Connector config is only supported"),
            # No deployment gate is open for a platform row.
            ("mcp", {}, "disabled on this deployment"),
        ],
    )
    async def test_a_refusal_is_a_value_error_with_the_api_detail(
        self, ds_type, kwargs, message
    ):
        from orchestrator.services.connector_drivers.platform import (
            validate_platform_connector,
        )

        with pytest.raises(ValueError, match=message):
            await validate_platform_connector(
                builtin_connector_drivers(),
                ds_type,
                name="Seeded",
                connection_url="https://example.invalid/",
                **kwargs,
            )

    @pytest.mark.asyncio
    async def test_init_seeds_default_rows_through_their_drivers(self, monkeypatch):
        from unittest.mock import AsyncMock

        from orchestrator import init

        for name in list(os.environ):
            if name.startswith("DEFAULT_DS_"):
                monkeypatch.delenv(name)
        monkeypatch.setenv("DEFAULT_DS_POSTGRESQL_URL", "postgresql://db/app")
        monkeypatch.setenv("DEFAULT_DS_WEBDAV_URL", "https://cloud/dav")
        monkeypatch.setenv("DEFAULT_DS_WEBDAV_USERNAME", "srw")
        db = AsyncMock()

        await init._seed_default_datasources(db)

        calls = [call.kwargs for call in db.upsert_default_datasource.await_args_list]
        assert calls == [
            {
                "name": "Default PostgreSQL",
                "ds_type": "postgresql",
                "connection_url": "postgresql://db/app",
                "credentials": None,
            },
            {
                "name": "Default WebDAV",
                "ds_type": "webdav",
                "connection_url": "https://cloud/dav",
                "credentials": {"username": "srw"},
            },
        ]

    @pytest.mark.asyncio
    async def test_init_skips_a_row_its_driver_refuses(self):
        from unittest.mock import AsyncMock

        from orchestrator import init

        db = AsyncMock()
        assert not await init._upsert_seeded_datasource(
            db,
            builtin_connector_drivers(),
            name="Seeded MCP",
            ds_type="mcp",
            connection_url="https://mcp.invalid/",
        )
        db.upsert_default_datasource.assert_not_awaited()


def test_connector_writes_see_the_real_stdio_gate():
    from orchestrator.application import build_application_resources, projects
    from orchestrator.services import deployment_gates

    resources = build_application_resources(None)
    environment = projects.datasources_dependencies(
        resources
    ).operations.driver_environment()
    assert environment.gates.mcp_stdio_enabled is deployment_gates.mcp_stdio_enabled
    assert (
        environment.gates.mcp_datasources_enabled
        is deployment_gates.mcp_datasources_enabled
    )


_LOGIN = {"username": "u", "password": "p"}


@pytest.mark.parametrize(
    ("type_id", "keeps_login"),
    [("postgresql", False), ("mongodb", False), ("neo4j", True), ("webdav", True)],
)
def test_a_read_only_link_keeps_the_login_only_where_the_client_needs_it(
    type_id, keeps_login
):
    driver = _REGISTRY.for_type(type_id)
    ctx = _bind_context()
    row = {
        "type": type_id,
        "name": "x",
        "connection_url": "scheme://host",
        "project_read_only": True,
    }
    entry = driver.bind(row, dict(_LOGIN), ctx=ctx)
    assert entry["credentials"] == (_LOGIN if keeps_login else {})
    assert entry["project_read_only"] is True
    assert driver.effective_access(row) == "ReadOnly"


# =============================================================================
# Spec metadata against driver behaviour
# =============================================================================


def _environment(*, gates_on: bool, validate=None):
    from orchestrator.services.connector_drivers.base import DriverEnvironment
    from orchestrator.services.datasource_config import validate_mcp_datasource

    return DriverEnvironment(
        gates=DeploymentGates(
            mcp_datasources_enabled=lambda: gates_on,
            mcp_stdio_enabled=lambda: gates_on,
        ),
        validate_mcp_datasource=validate or validate_mcp_datasource,
    )


def _draft(body: dict):
    from orchestrator.schemas.datasources import DatasourceCreate
    from orchestrator.services.connector_drivers.base import ConnectorDraft

    return ConnectorDraft.from_body(DatasourceCreate(**body))


async def _accepts(type_id: str, body: dict) -> bool:
    """Whether the type's driver takes ``body`` as a create."""
    from unittest.mock import AsyncMock

    from fastapi import HTTPException

    from orchestrator.services.connector_drivers.base import ValidationContext

    driver = _REGISTRY.for_type(type_id)
    environment = _environment(gates_on=True)
    draft = _draft(body)
    try:
        await driver.prevalidate(draft, environment)
        await driver.validate(
            draft,
            existing=None,
            ctx=ValidationContext(
                environment=environment,
                can_autonomous_send=AsyncMock(return_value=True),
            ),
        )
    except HTTPException:
        return False
    return True


def _valid_create(type_id: str) -> dict:
    from tests.test_connector_goldens_api import (
        _VALID_CREATE,
        MCP_STDIO,
        REPOSITORY_TOKEN,
    )

    # The most permissive valid body: MCP over stdio needs no URL, and a
    # token repository that declares its forge needs none to infer the forge.
    return {
        "mcp": MCP_STDIO,
        "repository": {**REPOSITORY_TOKEN, "config": {"forge": "github"}},
    }.get(type_id, _VALID_CREATE[type_id])


@pytest.mark.asyncio
@pytest.mark.parametrize("auth", ["token", "ssh"])
async def test_a_repository_needs_its_url_whatever_its_auth(auth):
    from tests.test_connector_goldens_api import REPOSITORY_SSH, REPOSITORY_TOKEN

    body = {
        "token": {**REPOSITORY_TOKEN, "config": {"forge": "github"}},
        "ssh": REPOSITORY_SSH,
    }[auth]
    assert await _accepts("repository", body)
    without_url = {k: v for k, v in body.items() if k != "connection_url"}
    assert not await _accepts("repository", without_url)
    assert not await _accepts("repository", {**body, "connection_url": "  "})


@pytest.mark.asyncio
@pytest.mark.parametrize("type_id", _REGISTRY.type_ids())
async def test_spec_metadata_says_what_validation_does(type_id, monkeypatch):
    """legacy_connection_url, publishable and forced_read_only are the
    driver's real behaviour, so a form or the matrix can rely on them."""
    monkeypatch.setenv("KB_GIT_ALLOWED_HOSTS", "git.example.test")
    monkeypatch.setenv("MCP_STDIO_ENABLED", "true")
    spec = _REGISTRY.for_type(type_id).spec
    body = _valid_create(type_id)
    assert await _accepts(type_id, body)

    without_url = {k: v for k, v in body.items() if k != "connection_url"}
    # Its own URL where the body has one: an SSH endpoint is checked.
    with_url = {
        "connection_url": "https://git.example.test/acme/x.git",
        **body,
    }
    accepts = (
        await _accepts(type_id, without_url),
        await _accepts(type_id, with_url),
    )
    assert (
        accepts
        == {
            "required": (False, True),
            "optional": (True, True),
            "forbidden": (True, False),
        }[spec.legacy_connection_url]
    )

    assert await _accepts(type_id, {**body, "is_global": True}) is spec.publishable
    assert await _accepts(type_id, {**body, "read_only": False}) is (
        not spec.forced_read_only
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("type_id", _REGISTRY.type_ids())
async def test_the_deployment_gate_refuses_before_authentication(type_id):
    from fastapi import HTTPException

    from tests.test_connector_goldens_api import MCP_REMOTE

    driver = _REGISTRY.for_type(type_id)
    environment = _environment(gates_on=False, validate=lambda _url, _creds: None)
    body = MCP_REMOTE if type_id == "mcp" else _valid_create(type_id)
    if driver.spec.deployment_gate:
        with pytest.raises(HTTPException) as caught:
            await driver.prevalidate(_draft(body), environment)
        assert caught.value.status_code == 403
    else:
        await driver.prevalidate(_draft(body), environment)


def test_workspace_ssh_identities_are_the_drivers_answer():
    """The delivery field holds what the installed drivers deliver, no more."""
    from orchestrator.services.agent_datasource_payload import (
        DatasourcePayloadDependencies,
        build_workspace_ssh_identities,
    )
    from orchestrator.services.connector_drivers.ssh_key import SshKeyDriver
    from shared.runtime.utils.ssh_key import generate_ed25519_keypair

    row = {
        "id": "00000000-0000-4000-8000-0000000000a2",
        "type": "ssh_key",
        "name": "Bastion",
        "credentials": {
            "files": [{"contents": generate_ed25519_keypair().private_key}]
        },
        "config": {},
    }

    def deliver(*drivers):
        return build_workspace_ssh_identities(
            [row],
            dependencies=DatasourcePayloadDependencies(
                logger=logging.getLogger(__name__),
                mcp_datasources_enabled=lambda: True,
                mcp_stdio_enabled=lambda: True,
                connector_drivers=ConnectorDriverRegistry(drivers),
                workspace_ssh_known_hosts=lambda: "",
            ),
        )

    (identity,) = deliver(SshKeyDriver())
    assert identity["kind"] == "ssh_key"
    assert identity["private_key"] == row["credentials"]["files"][0]["contents"]
    assert deliver() is None


@pytest.mark.parametrize(
    ("stored", "expected"),
    [
        ({"host": "h"}, {"host": "h"}),
        ('{"host": "h"}', {"host": "h"}),
        ("not json", {}),
        ("[1, 2]", {}),
        (None, {}),
        (["host"], {}),
    ],
)
def test_one_stored_json_helper_reads_the_ssh_connector_columns(stored, expected):
    from orchestrator.services import workspace_ssh_connector
    from orchestrator.services.connector_drivers import repository, workspace_ssh
    from orchestrator.services.datasource_config import stored_json_object

    assert stored_json_object(stored) == expected
    # The SSH rules, the SSH driver base and the token probe share it.
    for module in (workspace_ssh_connector, workspace_ssh, repository):
        assert module.stored_json_object is stored_json_object
