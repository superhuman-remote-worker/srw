"""The provider support matrix each main-cloud adapter declares.

main_cloud_as_connectors.md, slice 2: the matrix is declared per adapter as
data and replaces the ad hoc ``backend_id == "nextcloud"`` checks. Its
vocabulary is the driver contract's, so the ``cloud_folder`` driver (slice 3)
takes its access levels as they are.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from agent.services.cloud_sync.protected_lower import is_protected_reader_transport
from orchestrator.services.cloud import (
    PROTECTED_PROJECT_FOLDER,
    REGISTRY,
    FeatureNotAvailable,
    NextcloudBackend,
    NextcloudSettings,
    OpenCloudBackend,
    OpenCloudSettings,
    ProjectFolderHandle,
    protected_provider,
    provider_capabilities,
    provider_matrix,
    provider_offers,
)
from orchestrator.services.cloud.capabilities import (
    MATRIX_ROWS,
    CloudCapability,
    ProviderCapabilities,
    capability_matrix,
    validate_capabilities,
)
from shared.connectors.contract import DriverSpec, validate_spec

NEXTCLOUD = NextcloudBackend.capabilities
OPENCLOUD = OpenCloudBackend.capabilities


def _cell(declared: ProviderCapabilities, row: tuple) -> tuple:
    cap = declared.capability(*row)
    assert cap is not None, row
    return (cap.status, sorted(cap.supported_backends), cap.slice)


class TestTheDeclarations:
    @pytest.mark.parametrize("adapter", list(REGISTRY.values()))
    def test_every_adapter_declares_a_valid_complete_matrix(self, adapter):
        assert validate_capabilities(adapter.capabilities) == []
        assert adapter.capabilities.backend_id == adapter.backend_id

    def test_nextcloud_is_the_design_table(self):
        assert [_cell(NEXTCLOUD, row) for row in MATRIX_ROWS] == [
            ("planned", ["sandbox", "vm"], 5),  # project · read_only
            ("offered", ["sandbox"], None),  # project · read_write
            ("offered", ["sandbox"], None),  # project · protected (container only)
            ("planned", ["sandbox"], 4),  # user root · read_write (app token)
            ("unsupported", [], None),  # user root · read_only
            ("unsupported", [], None),  # user root · protected
            ("offered", ["sandbox", "vm"], None),  # checkout
            ("offered", ["sandbox"], None),  # outbox
        ]

    def test_opencloud_is_the_design_table(self):
        assert [_cell(OPENCLOUD, row) for row in MATRIX_ROWS] == [
            ("unsupported", [], None),  # RO reader code exists, uncalled
            ("offered", ["sandbox"], None),
            ("unsupported", [], None),
            ("unsupported", [], None),  # deprecated impersonation, not offered
            ("unsupported", [], None),
            ("unsupported", [], None),
            ("offered", ["sandbox", "vm"], None),
            ("offered", ["sandbox"], None),
        ]

    def test_every_cell_says_how_or_why(self):
        for declared in (NEXTCLOUD, OPENCLOUD):
            assert all(cap.note for cap in declared.capabilities)

    def test_an_app_token_is_why_the_user_root_has_no_read_only(self):
        cap = NEXTCLOUD.capability("cloud_folder", "user_root", "read_only")
        assert "app token" in cap.note


class TestTheDriverVocabulary:
    def test_offered_levels_are_a_driver_specs_access_levels(self):
        levels = NEXTCLOUD.access_levels("cloud_folder", "project")
        assert [level.id for level in levels] == ["protected", "read_write"]
        assert all(level.enforced_by for level in levels)
        spec = DriverSpec(
            name="srw.cloud-folder/v1",
            title="Cloud folder",
            plane="harness",
            delivery_forms=("managed_connection",),
            config_schema={"type": "object"},
            access_levels=levels,
            supported_backends=frozenset({"sandbox"}),
            workspace_requirements="rclone and FUSE",
        )
        assert validate_spec(spec) == []

    def test_a_provider_with_one_level_ranks_it_alone(self):
        levels = OPENCLOUD.access_levels("cloud_folder", "project")
        assert [(level.id, level.rank) for level in levels] == [("read_write", 30)]

    def test_planned_and_unsupported_levels_are_absent(self):
        assert NEXTCLOUD.access_levels("cloud_folder", "user_root") == ()


class TestQueries:
    def test_offers_is_the_offered_status_on_the_tier(self):
        assert NEXTCLOUD.offers(*PROTECTED_PROJECT_FOLDER)
        assert NEXTCLOUD.offers(*PROTECTED_PROJECT_FOLDER, workspace_backend="sandbox")
        assert not NEXTCLOUD.offers(*PROTECTED_PROJECT_FOLDER, workspace_backend="vm")
        assert not NEXTCLOUD.offers("cloud_folder", "project", "read_only")
        assert not OPENCLOUD.offers(*PROTECTED_PROJECT_FOLDER)

    @pytest.mark.parametrize("backend_id", [None, "", "ms365", "NEXTCLOUD", 7])
    def test_an_unknown_provider_offers_nothing(self, backend_id):
        assert provider_capabilities(backend_id) is None
        assert provider_offers(backend_id, PROTECTED_PROJECT_FOLDER) is False

    def test_protected_provider_fails_closed(self):
        assert protected_provider("nextcloud") == "nextcloud"
        for backend_id in ("opencloud", None, "fake"):
            with pytest.raises(FeatureNotAvailable):
                protected_provider(backend_id)


class TestValidation:
    def _declared(self, **change) -> ProviderCapabilities:
        caps = list(NEXTCLOUD.capabilities)
        caps[0] = replace(caps[0], **change)
        return replace(NEXTCLOUD, capabilities=tuple(caps))

    def test_a_missing_combination_is_reported(self):
        declared = replace(NEXTCLOUD, capabilities=NEXTCLOUD.capabilities[1:])
        assert any("not declared" in p for p in validate_capabilities(declared))

    @pytest.mark.parametrize(
        ("change", "problem"),
        [
            ({"slice": None}, "names a slice"),
            ({"note": ""}, "enforced_by line or a reason"),
            ({"supported_backends": frozenset({"cluster"})}, "unknown workspace"),
            ({"supported_backends": frozenset()}, "names its tiers"),
            ({"status": "maybe"}, "is not one of"),
            (
                {"status": "unsupported", "slice": None},
                "an unsupported level names no tiers",
            ),
        ],
    )
    def test_a_bad_cell_is_reported(self, change, problem):
        problems = validate_capabilities(self._declared(**change))
        assert any(problem in p for p in problems), problems

    def test_a_combination_outside_the_matrix_is_reported(self):
        extra = CloudCapability("cloud_folder", "team", "read_only", "unsupported", "x")
        declared = replace(NEXTCLOUD, capabilities=(*NEXTCLOUD.capabilities, extra))
        assert any(
            "is not a matrix combination" in p for p in validate_capabilities(declared)
        )


class TestTheMatrix:
    def test_one_row_per_combination_and_one_cell_per_provider(self):
        matrix = provider_matrix(active="nextcloud")
        assert [p["backend_id"] for p in matrix["providers"]] == list(REGISTRY)
        assert [p["active"] for p in matrix["providers"]] == [True, False]
        assert len(matrix["rows"]) == len(MATRIX_ROWS)
        protected = matrix["rows"][2]
        assert (protected["folder_kind"], protected["access"]) == (
            "project",
            "protected",
        )
        assert protected["cells"]["nextcloud"]["status"] == "offered"
        assert protected["cells"]["nextcloud"]["workspace_backends"] == ["sandbox"]
        assert protected["cells"]["opencloud"]["status"] == "unsupported"

    def test_an_undeclared_cell_is_unsupported(self):
        empty = ProviderCapabilities("dropbox", "Dropbox", ())
        matrix = capability_matrix([empty])
        assert {row["cells"]["dropbox"]["status"] for row in matrix["rows"]} == {
            "unsupported"
        }


def _nextcloud() -> NextcloudBackend:
    return NextcloudBackend(
        NextcloudSettings(
            base_url="http://nc.internal",
            public_url="https://cloud.example",
            admin_user="admin",
            admin_password="admin-secret",
            agent_user="agent-service",
            agent_password="agent-secret",
        )
    )


def _opencloud() -> OpenCloudBackend:
    return OpenCloudBackend(
        OpenCloudSettings(
            base_url="http://oc.internal",
            public_url="https://cloud.example",
            keycloak_issuer="https://auth.example/realms/srw/",
            keycloak_client_id="opencloud-orchestrator",
            keycloak_client_secret="client-secret",
        )
    )


class TestAdapterMethods:
    """What moved out of ``agent_cloud_mounts``, ``init`` and provisioning."""

    def test_nextcloud_syncs_as_the_agent_service_account(self):
        assert _nextcloud().cloud_sync_config("https://nc/dav/") == {
            "backend": "nextcloud",
            "webdav_url": "https://nc/dav/",
            "auth": {
                "type": "basic",
                "username": "agent-service",
                "password": "agent-secret",
            },
        }

    def test_opencloud_syncs_with_client_credentials_or_as_the_owner(self):
        backend = _opencloud()
        plain = backend.cloud_sync_config("https://oc/dav/")
        assert plain["auth"] == {
            "type": "keycloak_client_credentials",
            "issuer": "https://auth.example/realms/srw",
            "client_id": "opencloud-orchestrator",
            "client_secret": "client-secret",
        }
        owner = backend.cloud_sync_config("https://oc/dav/", target_user_sub="sub-1")
        assert owner["auth"]["type"] == "keycloak_user_impersonation"
        assert owner["auth"]["target_user_sub"] == "sub-1"

    def test_only_nextcloud_has_a_legacy_folder_id(self):
        handle = ProjectFolderHandle(backend="nextcloud", native_id="42")
        assert _nextcloud().legacy_folder_id(handle) == 42
        assert _nextcloud().legacy_folder_id(replace(handle, native_id="x")) is None
        assert _opencloud().legacy_folder_id(handle) is None

    def test_the_protected_lower_transport_is_the_readers(self):
        row = {"webdav_url": "https://nc/r/", "reader_id": "srw-r", "credentials": "p"}
        transport = NextcloudBackend.protected_lower_transport(row)
        assert transport == {
            "backend": "nextcloud",
            "source": {
                "type": "webdav",
                "config": {
                    "url": "https://nc/r/",
                    "vendor": "nextcloud",
                    "user": "srw-r",
                },
            },
            "auth": {"type": "basic", "password": "p"},
        }
        assert is_protected_reader_transport(transport)


class TestTheAgentAcceptsOnlyTheReaderTransport:
    def _transport(self) -> dict:
        return NextcloudBackend.protected_lower_transport(
            {"webdav_url": "https://nc/r/", "reader_id": "srw-r", "credentials": "p"}
        )

    @pytest.mark.parametrize(
        "mutate",
        [
            lambda t: t.update(backend="opencloud"),
            lambda t: t["source"].update(type="s3"),
            lambda t: t["source"]["config"].update(vendor="owncloud"),
            lambda t: t["source"]["config"].update(url=""),
            lambda t: t["source"]["config"].pop("user"),
            lambda t: t["auth"].update(type="keycloak_client_credentials"),
            lambda t: t["auth"].update(password=None),
            lambda t: t.pop("auth"),
        ],
    )
    def test_any_other_shape_is_refused(self, mutate):
        transport = self._transport()
        mutate(transport)
        assert not is_protected_reader_transport(transport)
