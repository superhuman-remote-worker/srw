"""``srw.cloud-folder-checkout/v1``: a project's folder copied into a job (6.3).

Pinned here: the spec (two strict levels, jobs only, created by SRW alone,
the new ``cloud_checkout`` form), the levels its provider's support matrix
offers (``reviewed_write_back`` waits for its gate), what a job's admission
gets for an attached row and why it gets nothing, the API's and the
driver's refusal to create one outside SRW, and that the agent never
receives one.
"""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from orchestrator.services.cloud import REGISTRY, provider_capabilities
from orchestrator.services.cloud.capabilities import ACCESS_RANKS, MATRIX_ROWS
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_drivers.base import (
    AccessContext,
    ConnectorDraft,
    SupportsOfferedAccess,
    ValidationContext,
)
from orchestrator.services.connector_drivers.cloud_folder_checkout import (
    CloudFolderCheckout,
    CloudFolderCheckoutDriver,
    CloudFolderCheckoutUnavailable,
    checkout_key,
)
from shared.connectors.builtin import (
    CLOUD_FOLDER_CHECKOUT_SPEC,
    DATASOURCE_SPECS,
    PLATFORM_SPECS,
    SHELL_BACKENDS,
    spec_for_type,
)
from shared.connectors.contract import strict_access, validate_spec
from shared.runtime.core.datasource_catalog import PLATFORM_TYPE_CATALOG

PROJECT_ID = "00000000-0000-0000-0000-0000000000b1"
OTHER_PROJECT = "00000000-0000-0000-0000-0000000000b2"
CONNECTOR_ID = "00000000-0000-0000-0000-0000000000f3"
SECRET = "hunter2-main-cloud"

_REGISTRY = builtin_connector_drivers()


def _row(**changes):
    row = {
        "id": CONNECTOR_ID,
        "type": "cloud_folder_checkout",
        "name": "Project folder copy",
        "description": None,
        "connection_url": None,
        "credentials": {},
        "config": {"project_id": PROJECT_ID},
        "managed_key": f"project-cloud-checkout:{PROJECT_ID}",
        "created_by": None,
        "read_only": None,
        "is_global": False,
        "project_read_only": None,
    }
    row.update(changes)
    return row


def _job(backend="nextcloud", tier=None):
    return AccessContext(tier, execution="job", main_cloud_backend=backend)


class TestSpec:
    def test_it_is_valid_strict_and_has_two_levels(self):
        assert validate_spec(CLOUD_FOLDER_CHECKOUT_SPEC) == []
        assert strict_access(CLOUD_FOLDER_CHECKOUT_SPEC)
        assert [
            (level.id, level.rank, level.writes)
            for level in CLOUD_FOLDER_CHECKOUT_SPEC.access_levels
        ] == [("read_only", 10, "none"), ("reviewed_write_back", 20, "reviewed")]
        # The matrix ranks the levels as the spec does.
        assert ACCESS_RANKS["cloud_folder_checkout"] == {
            level.id: level.rank for level in CLOUD_FOLDER_CHECKOUT_SPEC.access_levels
        }
        assert CLOUD_FOLDER_CHECKOUT_SPEC.default_access is None

    def test_it_is_a_jobs_and_only_srw_creates_one(self):
        spec = CLOUD_FOLDER_CHECKOUT_SPEC
        assert spec.executions == ("job",)
        assert spec.user_creatable is False
        assert spec.plane == "bind_time"
        assert spec.delivery_forms == ("cloud_checkout",)
        assert spec.live_attach is False
        assert spec.publishable is False
        assert spec.supported_backends == SHELL_BACKENDS
        assert spec_for_type("cloud_folder_checkout") is spec

    def test_it_is_in_no_catalogue_users_create_from(self):
        assert CLOUD_FOLDER_CHECKOUT_SPEC in PLATFORM_SPECS
        assert CLOUD_FOLDER_CHECKOUT_SPEC not in DATASOURCE_SPECS
        assert ("cloud_folder_checkout", "cloud_checkout") in [
            (d.type_id, d.runtime_kind) for d in PLATFORM_TYPE_CATALOG
        ]
        assert "cloud_folder_checkout" not in _REGISTRY.type_ids()
        driver = _REGISTRY.for_type("cloud_folder_checkout")
        assert isinstance(driver, CloudFolderCheckoutDriver)
        assert isinstance(driver, SupportsOfferedAccess)


class TestMatrix:
    def test_both_providers_offer_read_only_and_gate_the_write_back(self):
        assert ("cloud_folder_checkout", "project", "read_only") in MATRIX_ROWS
        for backend_id in REGISTRY:
            declared = provider_capabilities(backend_id)
            read_only = declared.capability(
                "cloud_folder_checkout", "project", "read_only"
            )
            assert read_only.status == "offered"
            assert read_only.supported_backends == frozenset({"sandbox", "vm"})
            write_back = declared.capability(
                "cloud_folder_checkout", "project", "reviewed_write_back"
            )
            assert write_back.status == "planned"
            assert write_back.slice == 6
            assert "0 of 432" in write_back.note

    @pytest.mark.parametrize("backend", ["nextcloud", "opencloud", None])
    @pytest.mark.parametrize("tier", ["sandbox", "vm", None])
    def test_a_job_is_offered_what_the_matrix_offers(self, backend, tier):
        driver = CloudFolderCheckoutDriver()
        assert driver.offered_access(_row(), _job(backend, tier)) == ("read_only",)

    def test_a_session_an_unknown_provider_or_a_bad_row_gets_nothing(self):
        driver = CloudFolderCheckoutDriver()
        session = AccessContext(execution="session", main_cloud_backend="nextcloud")
        assert driver.offered_access(_row(), session) == ()
        assert driver.offered_access(_row(), _job("dropbox")) == ()
        assert driver.offered_access(_row(config={}), _job()) == ()
        assert driver.offered_access(_row(config={"project_id": "x"}), _job()) == ()


class TestCheckoutBinding:
    def test_the_projects_own_row_binds_at_its_highest_offered_level(self):
        bound = CloudFolderCheckoutDriver().checkout_binding(
            _row(), project_id=PROJECT_ID, ctx=_job()
        )
        assert bound == CloudFolderCheckout(CONNECTOR_ID, PROJECT_ID, "read_only")
        assert bound.context() == {
            "datasource_id": CONNECTOR_ID,
            "access": "read_only",
        }

    def test_a_read_only_link_binds_read_only(self):
        bound = CloudFolderCheckoutDriver().checkout_binding(
            _row(project_read_only=True), project_id=PROJECT_ID, ctx=_job()
        )
        assert bound.access == "read_only"

    def test_a_level_not_offered_is_refused_not_floored(self):
        bound = CloudFolderCheckoutDriver().checkout_binding(
            _row(), project_id=PROJECT_ID, ctx=_job(), requested="reviewed_write_back"
        )
        assert isinstance(bound, CloudFolderCheckoutUnavailable)
        assert bound.reason == "access_unavailable"
        assert "reviewed_write_back" in bound.detail

    @pytest.mark.parametrize(
        "row",
        [
            _row(managed_key=None),
            _row(managed_key=f"project-cloud:{PROJECT_ID}"),
            _row(managed_key=f"project-cloud-checkout:{OTHER_PROJECT}"),
            _row(config={"project_id": OTHER_PROJECT}),
            _row(config={"project_id": PROJECT_ID, "folder_handle": "elsewhere"}),
        ],
    )
    def test_only_the_jobs_projects_own_row_copies_anything(self, row):
        bound = CloudFolderCheckoutDriver().checkout_binding(
            row, project_id=PROJECT_ID, ctx=_job()
        )
        assert isinstance(bound, CloudFolderCheckoutUnavailable)
        assert bound.reason == "config_invalid"

    def test_the_key_is_the_platform_kind(self):
        assert checkout_key(PROJECT_ID) == f"project-cloud-checkout:{PROJECT_ID}"


class TestCreation:
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
                body=DatasourceCreate(name="Copy", type="cloud_folder_checkout"),
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
        assert caught.value.status_code == 400
        assert "SRW provisions these" in caught.value.detail
        assert store.method_calls == []

    @pytest.mark.asyncio
    async def test_srw_creates_one_through_its_driver(self):
        from orchestrator.services.connector_drivers.platform import (
            validate_platform_connector,
        )

        normalized = await validate_platform_connector(
            _REGISTRY,
            "cloud_folder_checkout",
            name="Project folder copy",
            connection_url=None,
            config={"project_id": PROJECT_ID.upper()},
        )
        assert normalized.config == {"project_id": PROJECT_ID}
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
            await CloudFolderCheckoutDriver().validate(
                self._draft(name="Mine", config={"project_id": PROJECT_ID}),
                existing=None,
                ctx=ctx,
            )
        assert "SRW creates" in caught.value.detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("fields", "detail"),
        [
            ({"credentials": {"password": SECRET}}, "holds no credentials"),
            ({"connection_url": "https://cloud.example"}, "no connection URL"),
            ({"is_global": True}, "cannot be published"),
            ({"config": {"project_id": OTHER_PROJECT}}, "which project"),
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
        with pytest.raises(HTTPException) as caught:
            await CloudFolderCheckoutDriver().validate(
                self._draft(**fields), existing=_row(), ctx=ctx
            )
        assert detail in caught.value.detail


class TestNeverToTheAgent:
    def _dependencies(self):
        from orchestrator.services.agent_datasource_payload import (
            DatasourcePayloadDependencies,
        )

        return DatasourcePayloadDependencies(
            logger=logging.getLogger(__name__),
            mcp_datasources_enabled=lambda: True,
            connector_drivers=_REGISTRY,
            workspace_ssh_known_hosts=lambda: "",
        )

    def test_the_payload_has_no_entry_for_it(self):
        from orchestrator.services.agent_datasource_payload import (
            build_datasources_payload,
        )

        postgres = {
            "id": "00000000-0000-0000-0000-0000000000d2",
            "type": "postgresql",
            "name": "Warehouse",
            "description": None,
            "connection_url": "postgresql://reader@db/warehouse",
            "credentials": {},
            "project_read_only": True,
        }
        payload = build_datasources_payload(
            [_row(credentials={"password": SECRET}), postgres],
            dependencies=self._dependencies(),
        )
        assert [entry["type"] for entry in payload] == ["postgresql"]
        wire = json.dumps(payload)
        assert SECRET not in wire and CONNECTOR_ID not in wire

    def test_its_driver_binds_nothing(self):
        ctx = SimpleNamespace(gates=None, logger=None, default_known_hosts="")
        driver = CloudFolderCheckoutDriver()
        assert driver.bind(_row(), {}, ctx=ctx) is None
        assert driver.runtime_allowed(_row(), None) is False
