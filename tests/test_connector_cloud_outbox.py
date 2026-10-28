"""``srw.cloud-outbox/v1``: a user's outbox in the main cloud (slice 6.1).

Pinned here: the spec (one strict level, jobs and sessions, created by SRW
alone), the tiers it is offered on per execution (a session's mount needs
the tier its provider declares, a job's completion copy needs none), what a
session's mount plan gets for the row and why it gets nothing, the API's and
the driver's refusal to create one outside SRW, and that the agent never
receives one: no entry, no tools, no credential.
"""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from orchestrator.services.cloud.capabilities import ACCESS_RANKS
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_drivers.base import (
    AccessContext,
    ConnectorDraft,
    SupportsOfferedAccess,
    ValidationContext,
)
from orchestrator.services.connector_drivers.cloud_outbox import (
    CloudOutboxDriver,
    CloudOutboxMount,
    CloudOutboxUnavailable,
    is_outbox_row,
)
from shared.connectors.builtin import (
    BUILTIN_SPECS,
    CLOUD_OUTBOX_SPEC,
    DATASOURCE_SPECS,
    PLATFORM_SPECS,
    SHELL_BACKENDS,
    spec_for_type,
)
from shared.connectors.contract import strict_access, validate_spec

USER_ID = "00000000-0000-0000-0000-0000000000a1"
OTHER_USER = "00000000-0000-0000-0000-0000000000a2"
INSTANCE_ID = "00000000-0000-0000-0000-00000000c10d"
CONNECTOR_ID = "00000000-0000-0000-0000-0000000000f2"
SESSION_FOLDER = "nextcloud:sessions/abcd1234"
SECRET = "hunter2-main-cloud"

_REGISTRY = builtin_connector_drivers()


def _row(**changes):
    row = {
        "id": CONNECTOR_ID,
        "type": "cloud_outbox",
        "name": "Session outbox",
        "description": None,
        "connection_url": None,
        "credentials": {},
        "config": {},
        "managed_key": f"cloud-outbox:{USER_ID}",
        "created_by": USER_ID,
        "read_only": None,
        "is_global": False,
        "project_read_only": None,
    }
    row.update(changes)
    return row


def _session(tier="sandbox", backend="nextcloud"):
    return AccessContext(tier, execution="session", main_cloud_backend=backend)


def _bind(row=None, *, ctx=None, owner_id=USER_ID, folder=SESSION_FOLDER, **kw):
    return CloudOutboxDriver().mount_binding(
        row if row is not None else _row(),
        ctx=ctx or _session(),
        owner_id=owner_id,
        session_folder=folder,
        backend_instance_id=INSTANCE_ID,
        **kw,
    )


# =============================================================================
# The spec
# =============================================================================


class TestSpec:
    def test_it_is_valid_strict_and_has_one_level(self):
        assert validate_spec(CLOUD_OUTBOX_SPEC) == []
        assert strict_access(CLOUD_OUTBOX_SPEC)
        assert [
            (level.id, level.rank, level.writes)
            for level in CLOUD_OUTBOX_SPEC.access_levels
        ] == [("read_write", 10, "direct")]
        assert {
            level.id: level.rank for level in CLOUD_OUTBOX_SPEC.access_levels
        } == ACCESS_RANKS["cloud_outbox"]
        # Honest about whose account holds the folder.
        assert "service account" in CLOUD_OUTBOX_SPEC.access_levels[0].enforced_by

    def test_it_attaches_to_jobs_and_sessions_and_only_srw_creates_one(self):
        spec = CLOUD_OUTBOX_SPEC
        assert (spec.name, spec.legacy_type, spec.plane, spec.delivery_forms) == (
            "srw.cloud-outbox/v1",
            "cloud_outbox",
            "in_pod",
            ("cloud_mount", "cloud_remote"),
        )
        assert spec.plane_for("cloud_remote") == "harness"
        assert set(spec.executions) == {"job", "session"}
        assert spec.user_creatable is False
        assert spec.publishable is False
        assert (spec.live_attach, spec.live_detach) == (False, "next_attach")
        assert spec.credential_slots == ()
        assert spec.legacy_connection_url == "forbidden"
        # Both shell tiers and Virtual: a job's copy works on either shell
        # tier; a session's binding is narrowed per tier by offered_access.
        assert spec.supported_backends == SHELL_BACKENDS | {"virtual"}
        assert spec_for_type("cloud_outbox") is spec

    def test_it_is_in_no_catalogue_users_create_from(self):
        from shared.runtime.core.datasource_catalog import (
            DATASOURCE_TYPES,
            PLATFORM_TYPE_CATALOG,
        )

        assert CLOUD_OUTBOX_SPEC in PLATFORM_SPECS
        assert CLOUD_OUTBOX_SPEC in BUILTIN_SPECS
        assert CLOUD_OUTBOX_SPEC not in DATASOURCE_SPECS
        assert "cloud_outbox" not in DATASOURCE_TYPES
        assert ("cloud_outbox", "cloud_mount") in [
            (d.type_id, d.runtime_kind) for d in PLATFORM_TYPE_CATALOG
        ]

    def test_only_the_none_tier_refuses_it(self):
        from orchestrator.services.datasource_policy import workspace_tier_refuses

        row = {"type": "cloud_outbox"}
        assert not workspace_tier_refuses(row, "virtual")  # the cloud_remote form
        assert workspace_tier_refuses(row, "none")
        assert not workspace_tier_refuses(row, "sandbox")
        assert not workspace_tier_refuses(row, "vm")


# =============================================================================
# The registry, the API and the driver's own create
# =============================================================================


class TestCreation:
    def test_its_type_has_its_driver_but_is_not_created_by_type(self):
        driver = _REGISTRY.for_type("cloud_outbox")
        assert isinstance(driver, CloudOutboxDriver)
        assert isinstance(driver, SupportsOfferedAccess)
        assert _REGISTRY.get("srw.cloud-outbox/v1") is driver
        assert "cloud_outbox" not in _REGISTRY.type_ids()

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
                body=DatasourceCreate(name="My outbox", type="cloud_outbox"),
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
            "Session outbox connectors cannot be created here: SRW provisions these",
        )
        assert store.method_calls == []

    @pytest.mark.asyncio
    async def test_srw_creates_one_through_its_driver(self):
        from orchestrator.services.connector_drivers.platform import (
            validate_platform_connector,
        )

        normalized = await validate_platform_connector(
            _REGISTRY, "cloud_outbox", name="Session outbox", connection_url=None
        )
        assert (normalized.connection_url, normalized.config) == (None, {})
        assert normalized.credentials == {}

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
    async def test_a_create_outside_srws_context_is_refused(self):
        from orchestrator.services.connector_drivers.base import DriverEnvironment

        async def never():
            return False

        ctx = ValidationContext(
            environment=DriverEnvironment(gates=None, validate_mcp_datasource=None),
            can_autonomous_send=never,
        )
        with pytest.raises(HTTPException) as caught:
            await CloudOutboxDriver().validate(
                self._draft(name="Mine"), existing=None, ctx=ctx
            )
        assert caught.value.detail == "SRW creates a user's session outbox itself"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("fields", "detail"),
        [
            ({"credentials": {"password": SECRET}}, "holds no credentials"),
            ({"connection_url": "https://cloud.example"}, "no connection URL"),
            ({"is_global": True}, "cannot be published"),
            ({"config": {"folder_handle": "elsewhere"}}, "has no config"),
        ],
    )
    async def test_an_edit_can_change_nothing_but_its_name(self, fields, detail):
        from orchestrator.services.connector_drivers.platform import (
            PLATFORM_ENVIRONMENT,
        )

        async def never():
            return False

        ctx = ValidationContext(
            environment=PLATFORM_ENVIRONMENT, can_autonomous_send=never
        )
        driver = CloudOutboxDriver()
        with pytest.raises(HTTPException) as caught:
            await driver.validate(self._draft(**fields), existing=_row(), ctx=ctx)
        assert detail in caught.value.detail
        renamed = await driver.validate(
            self._draft(name="Outbox"), existing=_row(), ctx=ctx
        )
        assert (renamed.connection_url, renamed.config, renamed.credentials) == (
            None,
            None,
            None,
        )


# =============================================================================
# The levels a binding is offered, per execution
# =============================================================================


class TestOfferedAccess:
    @pytest.mark.parametrize("backend", ["nextcloud", "opencloud"])
    def test_a_session_is_offered_it_on_the_tier_its_provider_declares(self, backend):
        driver = CloudOutboxDriver()
        assert driver.offered_access(_row(), _session("sandbox", backend)) == (
            "read_write",
        )
        # The service account's credential must not reach a root VM.
        assert driver.offered_access(_row(), _session("vm", backend)) == ()
        # A Virtual Session holds it in the executor process (ruling 2 of
        # features/cloud_folders_on_virtual_sessions.md); OpenCloud's bearer
        # token follows in the feature's second run.
        assert driver.offered_access(_row(), _session("virtual", backend)) == (
            ("read_write",) if backend == "nextcloud" else ()
        )

    def test_a_jobs_copy_is_offered_on_any_tier(self):
        driver = CloudOutboxDriver()
        for tier in ("sandbox", "vm"):
            ctx = AccessContext(tier, execution="job", main_cloud_backend="nextcloud")
            assert driver.offered_access(_row(), ctx) == ("read_write",)

    def test_an_unknown_provider_or_a_config_offers_nothing(self):
        driver = CloudOutboxDriver()
        assert driver.offered_access(_row(), _session(backend="acme-cloud")) == ()
        assert (
            driver.offered_access(_row(config={"folder_handle": "x"}), _session()) == ()
        )

    def test_without_a_provider_any_shipped_one_answers(self):
        driver = CloudOutboxDriver()
        assert driver.offered_access(_row(), AccessContext()) == ("read_write",)
        assert driver.effective_access(_row()) == "read_write"
        assert driver.offered_access(_row(), AccessContext("vm")) == ()


# =============================================================================
# What a session's mount plan gets
# =============================================================================


class TestMountBinding:
    def test_the_owners_outbox_mounts_at_cloud_outbox(self):
        mount = _bind(shared=True)
        assert mount == CloudOutboxMount(
            connector_id=CONNECTOR_ID,
            access="read_write",
            backend_id="nextcloud",
            backend_instance_id=INSTANCE_ID,
            folder_handle=SESSION_FOLDER,
            shared=True,
        )
        assert (mount.name, mount.target_path) == ("outbox", "cloud/outbox")
        assert mount.mount_row() == {
            "id": CONNECTOR_ID,
            "source_kind": "cloud_outbox",
            "source_ref": CONNECTOR_ID,
            "mount_kind": "cloud_outbox",
            "target_path": "cloud/outbox",
            "backend_id": "nextcloud",
            "backend_instance_id": INSTANCE_ID,
            "cloud_handle": SESSION_FOLDER,
            "access": "read_write",
        }

    @pytest.mark.parametrize(
        ("kwargs", "reason"),
        [
            ({"row": _row(managed_key=None)}, "config_invalid"),
            ({"row": _row(managed_key=f"user-cloud-root:{USER_ID}")}, "config_invalid"),
            ({"row": _row(config={"folder_handle": "x"})}, "config_invalid"),
            ({"owner_id": OTHER_USER}, "not_eligible"),
            ({"owner_id": None}, "not_eligible"),
            ({"ctx": _session(backend=None)}, "backend_unavailable"),
            ({"ctx": _session("vm")}, "access_unavailable"),
            ({"row": _row(read_only=True)}, "access_unavailable"),
            ({"folder": None}, "no_session_folder"),
            ({"folder": ""}, "no_session_folder"),
        ],
    )
    def test_why_it_mounts_nothing(self, kwargs, reason):
        unavailable = _bind(**kwargs)
        assert isinstance(unavailable, CloudOutboxUnavailable)
        assert unavailable.connector_id == CONNECTOR_ID
        assert unavailable.reason == reason
        assert unavailable.detail

    def test_a_tier_it_is_not_offered_on_says_so_before_the_folder(self):
        # SRW creates no folder where the outbox cannot be mounted, so the
        # reason a VM session sees is the tier, not the missing folder.
        assert _bind(ctx=_session("vm"), folder=None).reason == "access_unavailable"

    def test_only_an_outbox_key_is_an_outbox(self):
        assert is_outbox_row(_row())
        assert not is_outbox_row(_row(managed_key=f"project-cloud:{USER_ID}"))
        assert not is_outbox_row(_row(managed_key=None))
        assert not is_outbox_row(None)


# =============================================================================
# Never to the agent
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
        outbox = _row(credentials={"password": SECRET})
        postgres = {
            "id": "00000000-0000-0000-0000-0000000000d2",
            "type": "postgresql",
            "name": "Warehouse",
            "description": None,
            "connection_url": "postgresql://reader@db/warehouse",
            "credentials": {},
            "project_read_only": True,
        }
        return [outbox, postgres]

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
        assert "cloud_outbox" not in wire and CONNECTOR_ID not in wire

    def test_a_session_with_only_its_outbox_gets_no_payload(self):
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
        assert build_datasource_tool_override(
            self._rows(), None, dependencies=dependencies
        ) == build_datasource_tool_override(
            self._rows()[1:], None, dependencies=dependencies
        )

    def test_its_driver_binds_nothing_even_when_asked(self):
        ctx = SimpleNamespace(gates=None, logger=None, default_known_hosts="")
        driver = CloudOutboxDriver()
        assert driver.bind(_row(), {"password": SECRET}, ctx=ctx) is None
        assert driver.runtime_allowed(_row(), None) is False
