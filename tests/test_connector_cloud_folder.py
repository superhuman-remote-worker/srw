"""``srw.cloud-folder/v1``: the main cloud's folder as a connector (slice 3.2).

Pinned here: the spec (strict levels, sessions only, created by SRW alone),
the levels a connector is offered (its provider's declaration for the
folder's kind and the workspace tier), what the mount plan gets for a row
and why it gets nothing, the config the driver stores, the API's refusal to
create one, and that the agent never receives one: no entry, no tools, no
credential.
"""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from orchestrator.services.cloud.capabilities import (
    ACCESS_RANKS,
    CloudCapability,
    ProviderCapabilities,
)
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_drivers.base import (
    AccessContext,
    ConnectorDraft,
    SupportsOfferedAccess,
    bound_access,
)
from orchestrator.services.connector_drivers.cloud_folder import (
    CloudFolderDriver,
    CloudFolderMount,
    CloudFolderUnavailable,
    normalize_cloud_folder_config,
)
from shared.connectors.builtin import (
    BUILTIN_SPECS,
    CLOUD_FOLDER_SPEC,
    DATASOURCE_SPECS,
    PLATFORM_SPECS,
    spec_for_type,
)
from shared.connectors.contract import strict_access, validate_spec

PROJECT_ID = "00000000-0000-0000-0000-0000000000b1"
INSTANCE_ID = "00000000-0000-0000-0000-00000000c10d"
CONNECTOR_ID = "00000000-0000-0000-0000-0000000000f1"
SECRET = "hunter2-main-cloud"

_REGISTRY = builtin_connector_drivers()


def _config(**changes):
    config = {
        "folder_kind": "project",
        "project_id": PROJECT_ID,
        "backend_id": "nextcloud",
        "backend_instance_id": INSTANCE_ID,
        "folder_handle": "42",
    }
    config.update(changes)
    return {key: value for key, value in config.items() if value is not None}


def _on(workspace_backend):
    return AccessContext(workspace_backend=workspace_backend)


def _row(**changes):
    row = {
        "id": CONNECTOR_ID,
        "type": "cloud_folder",
        "name": "Acme Launch",
        "description": None,
        "connection_url": None,
        "credentials": {},
        "config": _config(),
        "managed_key": f"project-cloud:{PROJECT_ID}",
        "read_only": None,
        "is_global": False,
        "project_read_only": None,
    }
    row.update(changes)
    return row


def _offering(*cells: tuple[str, str, frozenset[str]]) -> ProviderCapabilities:
    """A provider declaring ``(folder kind, level, tiers)`` as offered."""
    return ProviderCapabilities(
        backend_id="acme-cloud",
        title="Acme cloud",
        capabilities=tuple(
            CloudCapability("cloud_folder", kind, level, "offered", "a line", tiers)
            for kind, level, tiers in cells
        ),
    )


@pytest.fixture
def provider(monkeypatch):
    """Route ``provider_capabilities`` to one declaration for ``acme-cloud``."""
    from orchestrator.services import cloud

    declared: dict[str, ProviderCapabilities] = {}
    real = cloud.provider_capabilities

    def capabilities(backend_id):
        if backend_id == "acme-cloud":
            return declared.get("acme")
        return real(backend_id)

    monkeypatch.setattr(cloud, "provider_capabilities", capabilities)

    def declare(*cells):
        declared["acme"] = _offering(*cells)

    return declare


# =============================================================================
# The spec
# =============================================================================


class TestSpec:
    def test_it_is_valid_and_strict(self):
        assert validate_spec(CLOUD_FOLDER_SPEC) == []
        assert strict_access(CLOUD_FOLDER_SPEC)
        assert [
            (level.id, level.rank, level.writes)
            for level in CLOUD_FOLDER_SPEC.access_levels
        ] == [
            ("read_only", 10, "none"),
            ("protected", 20, "reviewed"),
            ("read_write", 30, "direct"),
        ]

    def test_its_levels_are_the_support_matrix_vocabulary(self):
        assert {
            level.id: level.rank for level in CLOUD_FOLDER_SPEC.access_levels
        } == ACCESS_RANKS["cloud_folder"]

    def test_it_mounts_into_sessions_and_only_srw_creates_one(self):
        spec = CLOUD_FOLDER_SPEC
        assert (spec.legacy_type, spec.plane, spec.delivery_forms) == (
            "cloud_folder",
            "in_pod",
            ("cloud_mount",),
        )
        assert spec.executions == ("session",)
        assert spec.user_creatable is False
        assert spec.publishable is False
        assert (spec.live_attach, spec.live_detach) == (False, "next_attach")
        assert spec.credential_slots == ()
        assert spec.legacy_connection_url == "forbidden"
        assert spec_for_type("cloud_folder") is spec

    def test_it_is_in_no_catalogue_users_create_from(self):
        from shared.runtime.core.datasource_catalog import (
            DATASOURCE_TYPES,
            PLATFORM_TYPE_CATALOG,
        )

        assert CLOUD_FOLDER_SPEC in PLATFORM_SPECS
        assert CLOUD_FOLDER_SPEC in BUILTIN_SPECS
        assert CLOUD_FOLDER_SPEC not in DATASOURCE_SPECS
        assert "cloud_folder" not in DATASOURCE_TYPES
        assert [(d.type_id, d.runtime_kind) for d in PLATFORM_TYPE_CATALOG] == [
            ("cloud_folder", "cloud_mount")
        ]
        assert all(spec.user_creatable for spec in DATASOURCE_SPECS)
        assert not any(spec.user_creatable for spec in PLATFORM_SPECS)

    def test_the_new_flags_are_validated(self):
        from dataclasses import replace

        assert "executions" in "\n".join(
            validate_spec(replace(CLOUD_FOLDER_SPEC, executions=()))
        )
        assert "executions" in "\n".join(
            validate_spec(replace(CLOUD_FOLDER_SPEC, executions=("session", "cron")))
        )
        assert "executions" in "\n".join(
            validate_spec(replace(CLOUD_FOLDER_SPEC, executions=("job", "job")))
        )
        assert (
            validate_spec(replace(CLOUD_FOLDER_SPEC, executions=("session", "job")))
            == []
        )
        assert validate_spec(
            replace(CLOUD_FOLDER_SPEC, legacy_type=None, user_creatable=False)
        ) == ["only a driver with a datasource row is created by SRW alone"]

    def test_the_matrix_shows_the_flags_only_where_they_differ(self):
        from orchestrator.services.connector_drivers.matrix import capability_matrix

        rows = {row["name"]: row for row in capability_matrix(_REGISTRY)["drivers"]}
        folder = rows["srw.cloud-folder/v1"]
        assert folder["executions"] == ["session"]
        assert folder["user_creatable"] is False
        assert [level["writes"] for level in folder["access_levels"]] == [
            "none",
            "reviewed",
            "direct",
        ]
        for name, row in rows.items():
            if name != "srw.cloud-folder/v1":
                assert "executions" not in row and "user_creatable" not in row


# =============================================================================
# The registry and the API
# =============================================================================


class TestRegistry:
    def test_its_type_has_its_driver_but_is_not_created_by_type(self):
        driver = _REGISTRY.for_type("cloud_folder")
        assert isinstance(driver, CloudFolderDriver)
        assert isinstance(driver, SupportsOfferedAccess)
        assert _REGISTRY.get("srw.cloud-folder/v1") is driver
        assert "cloud_folder" not in _REGISTRY.type_ids()

    @pytest.mark.asyncio
    async def test_the_api_refuses_to_create_one(self):
        from orchestrator.schemas.datasources import DatasourceCreate
        from orchestrator.services.datasources import (
            DatasourceDependencies,
            create_datasource,
        )

        async def unreachable(*_args, **_kwargs):
            raise AssertionError("refused before any caller is authenticated")

        store = MagicMock()
        with pytest.raises(HTTPException) as caught:
            await create_datasource(
                body=DatasourceCreate(
                    name="Someone else's folder",
                    type="cloud_folder",
                    config=_config(folder_handle="999"),
                ),
                require_approved_user=unreachable,
                require_project_owner=unreachable,
                dependencies=DatasourceDependencies(
                    store=store,
                    vector_db=MagicMock(),
                    knowledge_index=MagicMock(),
                    mcp_datasources_enabled=lambda: False,
                    validate_mcp_datasource=lambda _url, _creds: None,
                    connector_drivers=_REGISTRY,
                ),
            )
        assert (caught.value.status_code, caught.value.detail) == (
            400,
            "Cloud folder connectors cannot be created here: SRW provisions these",
        )
        assert store.method_calls == []


# =============================================================================
# Config
# =============================================================================


class TestConfig:
    def test_a_project_folder_is_kept_in_the_specs_order(self):
        raw = {
            "folder_handle": "42",
            "backend_instance_id": INSTANCE_ID.upper(),
            "backend_id": "nextcloud",
            "project_id": PROJECT_ID,
            "folder_kind": "project",
        }
        normalized = normalize_cloud_folder_config(raw)
        assert list(normalized) == [
            "folder_kind",
            "project_id",
            "backend_id",
            "backend_instance_id",
            "folder_handle",
        ]
        assert normalized["backend_instance_id"] == INSTANCE_ID

    def test_a_user_root_needs_no_project(self):
        assert "project_id" not in normalize_cloud_folder_config(
            _config(folder_kind="user_root", project_id=None)
        )

    @pytest.mark.parametrize(
        ("config", "message"),
        [
            (None, "config must be an object"),
            (
                {**_config(), "password": SECRET},
                "Unknown cloud folder config field(s): password",
            ),
            (_config(project_id=None), "needs project_id"),
            (_config(folder_handle=""), "needs folder_handle"),
            (_config(backend_id=None), "needs backend_id"),
            (_config(backend_instance_id=None), "needs backend_instance_id"),
            (
                _config(folder_kind="team"),
                "folder_kind must be one of project, user_root",
            ),
            (_config(project_id="p1"), "project_id must be a UUID"),
            (_config(backend_id=7), "backend_id must be a non-empty string"),
        ],
    )
    def test_anything_else_is_refused_with_its_reason(self, config, message):
        with pytest.raises(ValueError) as caught:
            normalize_cloud_folder_config(config)
        assert message in str(caught.value)

    @pytest.mark.asyncio
    async def test_srw_creates_one_through_its_driver(self):
        from orchestrator.services.connector_drivers.platform import (
            validate_platform_connector,
        )

        normalized = await validate_platform_connector(
            _REGISTRY,
            "cloud_folder",
            name="Acme Launch",
            connection_url=None,
            config=_config(),
        )
        assert normalized.config == _config()
        assert normalized.credentials == {}
        assert normalized.connection_url is None

    @staticmethod
    def _draft(**fields):
        values = dict.fromkeys(
            (
                "name",
                "connection_url",
                "credentials",
                "config",
                "read_only",
                "is_global",
                "default_branch",
            )
        )
        values.update(fields)
        return ConnectorDraft(**values, supplied=frozenset(fields))

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("fields", "detail"),
        [
            ({"credentials": {"password": SECRET}}, "holds no credentials"),
            ({"connection_url": "https://cloud.example/dav"}, "no connection URL"),
            ({"is_global": True}, "cannot be published"),
            ({"config": _config(folder_handle="999")}, "SRW sets which folder"),
        ],
    )
    async def test_an_edit_keeps_the_folder_srw_set(self, fields, detail):
        driver = CloudFolderDriver()
        with pytest.raises(HTTPException) as caught:
            await driver.validate(
                self._draft(**fields), existing=_row(), ctx=MagicMock()
            )
        assert caught.value.status_code == 400
        assert detail in caught.value.detail

    @pytest.mark.asyncio
    async def test_a_name_edit_or_the_same_config_changes_nothing_else(self):
        driver = CloudFolderDriver()
        for draft in (
            self._draft(name="Launch"),
            self._draft(config=_config(backend_instance_id=INSTANCE_ID.upper())),
        ):
            normalized = await driver.validate(draft, existing=_row(), ctx=MagicMock())
            assert (
                normalized.connection_url,
                normalized.config,
                normalized.credentials,
            ) == (None, None, None)


# =============================================================================
# The levels a connector is offered, and its mount
# =============================================================================


class TestOfferedAccess:
    def test_the_provider_declaration_decides_per_kind_and_tier(self, provider):
        provider(
            ("project", "protected", frozenset({"sandbox"})),
            ("project", "read_write", frozenset({"sandbox", "vm"})),
            ("user_root", "read_write", frozenset({"sandbox"})),
        )
        driver = CloudFolderDriver()
        row = _row(config=_config(backend_id="acme-cloud"))
        assert driver.offered_access(row, _on("sandbox")) == ("protected", "read_write")
        assert driver.offered_access(row, _on("vm")) == ("read_write",)
        assert driver.offered_access(row, _on("virtual")) == ()
        assert driver.offered_access(row, AccessContext()) == (
            "protected",
            "read_write",
        )
        root = _row(
            config=_config(
                backend_id="acme-cloud", folder_kind="user_root", project_id=None
            )
        )
        assert driver.offered_access(root, _on("sandbox")) == ("read_write",)

    def test_the_reviewed_level_needs_the_protected_lane(self, provider):
        provider(
            ("project", "protected", frozenset({"sandbox"})),
            ("project", "read_write", frozenset({"sandbox"})),
        )
        driver = CloudFolderDriver()
        row = _row(config=_config(backend_id="acme-cloud"))
        for ctx in (
            AccessContext("sandbox", officer=True),
            AccessContext("sandbox", protected_mode_enabled=False),
        ):
            assert driver.offered_access(row, ctx) == ("read_write",)
            refused = driver.mount_binding(row, ctx=ctx, requested="protected")
            assert isinstance(refused, CloudFolderUnavailable)
            assert refused.reason == "access_unavailable"

    def test_an_unknown_provider_or_a_broken_config_offers_nothing(self):
        driver = CloudFolderDriver()
        anywhere = AccessContext()
        assert (
            driver.offered_access(_row(config=_config(backend_id="ftp")), anywhere)
            == ()
        )
        assert (
            driver.offered_access(_row(config={"folder_handle": "42"}), anywhere) == ()
        )

    @pytest.mark.parametrize("backend_id", ["nextcloud", "opencloud"])
    @pytest.mark.parametrize("tier", ["sandbox", "vm", None])
    def test_the_shipped_providers_offer_what_they_declare(self, backend_id, tier):
        from orchestrator.services.cloud import provider_offers

        offered = CloudFolderDriver().offered_access(
            _row(config=_config(backend_id=backend_id)), AccessContext(tier)
        )
        assert offered == tuple(
            level
            for level in CLOUD_FOLDER_SPEC.ranked_access_ids()
            if provider_offers(
                backend_id, ("cloud_folder", "project", level), workspace_backend=tier
            )
        )

    def test_today_nextcloud_offers_no_read_only_so_a_read_only_link_is_refused(
        self,
    ):
        """Until the reader grant is offered as its own level, a read-only
        binding of a Nextcloud project folder binds nothing: never the
        reviewed or the writable level in its place."""
        from orchestrator.services.cloud import provider_offers

        if provider_offers("nextcloud", ("cloud_folder", "project", "read_only")):
            pytest.skip("Nextcloud offers the read-only level now")
        resolved = bound_access(
            CloudFolderDriver(), _row(project_read_only=True), ctx=_on("sandbox")
        )
        assert resolved.code == "read_only_unavailable"


class TestMountBinding:
    def test_a_row_srw_created_mounts_at_its_name(self, provider):
        provider(
            ("project", "protected", frozenset({"sandbox"})),
            ("project", "read_write", frozenset({"sandbox"})),
        )
        driver = CloudFolderDriver()
        row = _row(config=_config(backend_id="acme-cloud"))
        mount = driver.mount_binding(row, ctx=_on("sandbox"))
        assert mount == CloudFolderMount(
            connector_id=CONNECTOR_ID,
            name="acme_launch",
            access="read_write",
            folder_kind="project",
            project_id=PROJECT_ID,
            backend_id="acme-cloud",
            backend_instance_id=INSTANCE_ID,
            folder_handle="42",
        )
        assert mount.target_path == "cloud/acme_launch"
        assert mount.mount_row() == {
            "id": CONNECTOR_ID,
            "source_kind": "cloud_folder",
            "source_ref": CONNECTOR_ID,
            "mount_kind": "project",
            "target_path": "cloud/acme_launch",
            "backend_id": "acme-cloud",
            "backend_instance_id": INSTANCE_ID,
            "cloud_handle": "42",
            "access": "read_write",
        }
        assert driver.mount_binding(
            row, ctx=_on("sandbox"), requested="protected"
        ).access == ("protected")

    def test_a_read_only_binding_takes_the_reader_level_or_nothing(self, provider):
        provider(
            ("project", "read_only", frozenset({"sandbox"})),
            ("project", "protected", frozenset({"sandbox"})),
            ("project", "read_write", frozenset({"sandbox"})),
        )
        driver = CloudFolderDriver()
        config = _config(backend_id="acme-cloud")
        for row in (
            _row(config=config, project_read_only=True),
            _row(config=config, read_only=True),
        ):
            mount = driver.mount_binding(row, ctx=_on("sandbox"))
            assert isinstance(mount, CloudFolderMount) and mount.access == "read_only"
        provider(("project", "protected", frozenset({"sandbox"})))
        refused = driver.mount_binding(
            _row(config=config, project_read_only=True), ctx=_on("sandbox")
        )
        assert refused == CloudFolderUnavailable(
            CONNECTOR_ID,
            "access_unavailable",
            "Cloud folder cannot bind this connector read-only here: every level "
            "it offers (protected) lets writes through",
        )

    def test_a_tier_the_provider_does_not_serve_mounts_nothing(self, provider):
        provider(("project", "read_write", frozenset({"sandbox"})))
        refused = CloudFolderDriver().mount_binding(
            _row(config=_config(backend_id="acme-cloud")), ctx=_on("vm")
        )
        assert refused == CloudFolderUnavailable(
            CONNECTOR_ID,
            "access_unavailable",
            "Cloud folder offers this connector no access level here",
        )

    def test_a_level_asked_for_that_is_not_offered_is_refused(self, provider):
        provider(("project", "read_write", frozenset({"sandbox"})))
        refused = CloudFolderDriver().mount_binding(
            _row(config=_config(backend_id="acme-cloud")),
            ctx=_on("sandbox"),
            requested="protected",
        )
        assert isinstance(refused, CloudFolderUnavailable)
        assert refused.reason == "access_unavailable"
        assert "'protected'" in refused.detail

    @pytest.mark.parametrize(
        ("changes", "detail"),
        [
            ({"managed_key": None}, "not created by SRW"),
            ({"config": {"folder_kind": "project"}}, "needs"),
            # Only the key of the project its config names.
            (
                {"managed_key": "project-cloud:00000000-0000-0000-0000-0000000000b2"},
                "not its project's own folder",
            ),
            ({"managed_key": f"project-kb:{PROJECT_ID}"}, "not its project's own"),
        ],
    )
    def test_a_row_that_names_no_folder_srw_made_mounts_nothing(self, changes, detail):
        refused = CloudFolderDriver().mount_binding(_row(**changes), ctx=_on("sandbox"))
        assert isinstance(refused, CloudFolderUnavailable)
        assert refused.reason == "config_invalid"
        assert detail in refused.detail


# =============================================================================
# The agent never receives one
# =============================================================================


def _payload_dependencies():
    from orchestrator.services.agent_datasource_payload import (
        DatasourcePayloadDependencies,
    )

    return DatasourcePayloadDependencies(
        logger=logging.getLogger(__name__),
        mcp_datasources_enabled=lambda: True,
        connector_drivers=_REGISTRY,
        workspace_ssh_known_hosts=lambda: "",
    )


class TestNeverToTheAgent:
    def _rows(self):
        folder = _row(credentials={"password": SECRET})
        postgres = {
            "id": "00000000-0000-0000-0000-0000000000d2",
            "type": "postgresql",
            "name": "Warehouse",
            "description": None,
            "connection_url": "postgresql://reader@db/warehouse",
            "credentials": {},
            "project_read_only": True,
        }
        return [folder, postgres]

    def test_the_payload_has_no_entry_and_no_credential_for_it(self):
        from orchestrator.services.agent_datasource_payload import (
            build_datasources_payload,
        )

        payload = build_datasources_payload(
            self._rows(), dependencies=_payload_dependencies()
        )
        assert [entry["type"] for entry in payload] == ["postgresql"]
        wire = json.dumps(payload)
        assert SECRET not in wire
        assert "cloud_folder" not in wire and INSTANCE_ID not in wire

    def test_a_session_with_only_a_folder_gets_no_payload(self):
        from orchestrator.services.agent_datasource_payload import (
            build_datasources_payload,
        )

        assert (
            build_datasources_payload(
                [_row(credentials={"password": SECRET})],
                dependencies=_payload_dependencies(),
            )
            is None
        )

    def test_it_is_not_forwarded_so_it_grants_no_tools(self):
        from orchestrator.services.agent_datasource_payload import (
            build_datasource_tool_override,
            forwarded_datasources,
        )

        dependencies = _payload_dependencies()
        assert [
            row["type"]
            for row in forwarded_datasources(self._rows(), dependencies=dependencies)
        ] == ["postgresql"]
        with_folder = build_datasource_tool_override(
            self._rows(), None, dependencies=dependencies
        )
        without = build_datasource_tool_override(
            self._rows()[1:], None, dependencies=dependencies
        )
        assert with_folder == without

    def test_its_driver_binds_nothing_even_when_asked(self):
        ctx = SimpleNamespace(gates=None, logger=None, default_known_hosts="")
        driver = CloudFolderDriver()
        assert driver.bind(_row(), {"password": SECRET}, ctx=ctx) is None
        assert driver.runtime_allowed(_row(), None) is False
