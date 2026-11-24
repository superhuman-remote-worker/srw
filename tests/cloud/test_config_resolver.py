"""Tests for the Phase 3 backend resolver.

``load_main_cloud_config`` picks a backend based on the environment. The
resolution order is:

  1. ``backend_override`` parameter (used by ``MainCloudRouter._legacy``).
  2. ``MAIN_CLOUD_BACKEND`` env var — explicit operator override.
  3. ``_detect_legacy_nextcloud_mode()`` heuristic — if any
     ``NEXTCLOUD_*`` env var (other than ``NEXTCLOUD_PORT``) is set,
     route to Nextcloud.
  4. Default → ``opencloud``.

These tests isolate the resolver with ``monkeypatch`` so the behaviour
is deterministic regardless of what's in the developer's shell.
"""

from __future__ import annotations

import pytest

from orchestrator.services.cloud.config import (
    NextcloudSettings,
    OpenCloudSettings,
    _detect_legacy_nextcloud_mode,
    load_main_cloud_config,
    main_cloud_selected,
)


def _clear_cloud_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wipe every MAIN_CLOUD_*, NEXTCLOUD_*, OPENCLOUD_*, MS365_* var."""
    import os

    for key in list(os.environ):
        if key.startswith(("MAIN_CLOUD_", "NEXTCLOUD_", "OPENCLOUD_", "MS365_")):
            monkeypatch.delenv(key, raising=False)


def _opencloud_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Seed the minimum OpenCloud env vars so validation passes."""
    monkeypatch.setenv("OPENCLOUD_URL", "http://opencloud:9200")
    monkeypatch.setenv("OPENCLOUD_PUBLIC_URL", "http://localhost:9200")
    monkeypatch.setenv("OPENCLOUD_KEYCLOAK_ISSUER", "http://keycloak:8080/realms/srw")
    monkeypatch.setenv("OPENCLOUD_KEYCLOAK_CLIENT_SECRET", "secret")


class TestDetectLegacyNextcloudMode:
    def test_no_env_vars_returns_false(self, monkeypatch: pytest.MonkeyPatch):
        _clear_cloud_env(monkeypatch)
        assert _detect_legacy_nextcloud_mode() is False

    def test_nextcloud_admin_password_triggers(self, monkeypatch: pytest.MonkeyPatch):
        _clear_cloud_env(monkeypatch)
        monkeypatch.setenv("NEXTCLOUD_ADMIN_PASSWORD", "admin")
        assert _detect_legacy_nextcloud_mode() is True

    def test_nextcloud_port_alone_does_not_trigger(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """NEXTCLOUD_PORT may be set even on an OpenCloud-primary deploy
        (e.g. when the Nextcloud profile is only available for migration).
        By itself it should not flip the default."""
        _clear_cloud_env(monkeypatch)
        monkeypatch.setenv("NEXTCLOUD_PORT", "8800")
        assert _detect_legacy_nextcloud_mode() is False

    def test_main_cloud_backend_short_circuits(self, monkeypatch: pytest.MonkeyPatch):
        _clear_cloud_env(monkeypatch)
        monkeypatch.setenv("MAIN_CLOUD_BACKEND", "opencloud")
        monkeypatch.setenv("NEXTCLOUD_ADMIN_PASSWORD", "admin")
        assert _detect_legacy_nextcloud_mode() is False


class TestLoadMainCloudConfigResolverOrder:
    def test_greenfield_defaults_to_opencloud(self, monkeypatch: pytest.MonkeyPatch):
        _clear_cloud_env(monkeypatch)
        _opencloud_env(monkeypatch)
        settings = load_main_cloud_config()
        assert isinstance(settings, OpenCloudSettings)
        assert settings.backend_id == "opencloud"
        # Safe-by-default: TLS verification on rclone mounts stays on.
        assert settings.mount_insecure_tls is False

    def test_opencloud_mount_insecure_tls_env(self, monkeypatch: pytest.MonkeyPatch):
        """OPENCLOUD_MOUNT_INSECURE_TLS=true (local k3d) flips the knob."""
        _clear_cloud_env(monkeypatch)
        _opencloud_env(monkeypatch)
        monkeypatch.setenv("OPENCLOUD_MOUNT_INSECURE_TLS", "true")
        settings = load_main_cloud_config()
        assert isinstance(settings, OpenCloudSettings)
        assert settings.mount_insecure_tls is True

    def test_legacy_nextcloud_env_routes_to_nextcloud(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        _clear_cloud_env(monkeypatch)
        monkeypatch.setenv("NEXTCLOUD_ADMIN_PASSWORD", "admin")
        settings = load_main_cloud_config()
        assert isinstance(settings, NextcloudSettings)
        assert settings.backend_id == "nextcloud"

    def test_explicit_env_override_wins(self, monkeypatch: pytest.MonkeyPatch):
        _clear_cloud_env(monkeypatch)
        # Even with NEXTCLOUD_* in the env, MAIN_CLOUD_BACKEND=opencloud wins.
        monkeypatch.setenv("NEXTCLOUD_ADMIN_PASSWORD", "admin")
        monkeypatch.setenv("MAIN_CLOUD_BACKEND", "opencloud")
        _opencloud_env(monkeypatch)
        settings = load_main_cloud_config()
        assert isinstance(settings, OpenCloudSettings)

    def test_explicit_nextcloud_override_on_greenfield_env(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """Operator can force nextcloud on a fresh .env without legacy vars."""
        _clear_cloud_env(monkeypatch)
        monkeypatch.setenv("MAIN_CLOUD_BACKEND", "nextcloud")
        settings = load_main_cloud_config()
        assert isinstance(settings, NextcloudSettings)

    def test_backend_override_parameter_wins_over_env(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        _clear_cloud_env(monkeypatch)
        monkeypatch.setenv("MAIN_CLOUD_BACKEND", "nextcloud")
        _opencloud_env(monkeypatch)
        settings = load_main_cloud_config(backend_override="opencloud")
        assert isinstance(settings, OpenCloudSettings)

    def test_port_only_nextcloud_env_still_greenfield(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        _clear_cloud_env(monkeypatch)
        monkeypatch.setenv("NEXTCLOUD_PORT", "8800")
        _opencloud_env(monkeypatch)
        settings = load_main_cloud_config()
        assert isinstance(settings, OpenCloudSettings)


class TestOpenCloudZeroConfigDefaults:
    """A fresh `.env` with no OPENCLOUD_* variables set MUST still
    produce a valid OpenCloudSettings — the defaults target a locally
    reachable OpenCloud. This is the contract the user relies on when
    copying ``.env.example`` and only filling in API keys.
    """

    def test_zero_config_loads_with_builtin_defaults(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        _clear_cloud_env(monkeypatch)
        settings = load_main_cloud_config()
        assert isinstance(settings, OpenCloudSettings)
        assert str(settings.base_url) == "http://localhost:9200/"
        assert str(settings.public_url) == "http://localhost:9200/"
        assert str(settings.keycloak_issuer) == "http://localhost:8180/realms/srw"
        assert settings.keycloak_client_id == "opencloud-orchestrator"
        assert (
            settings.keycloak_client_secret.get_secret_value()
            == "opencloud-orchestrator-local-secret"
        )
        assert settings.admin_role_claim_value == "opencloudAdmin"
        assert settings.default_quota_bytes is None

    def test_explicit_env_overrides_default(self, monkeypatch: pytest.MonkeyPatch):
        _clear_cloud_env(monkeypatch)
        monkeypatch.setenv("OPENCLOUD_URL", "http://prod-opencloud:9200")
        monkeypatch.setenv("OPENCLOUD_KEYCLOAK_CLIENT_SECRET", "rotated-secret")
        settings = load_main_cloud_config()
        assert isinstance(settings, OpenCloudSettings)
        assert str(settings.base_url) == "http://prod-opencloud:9200/"
        # public_url falls back to base_url when not explicitly set
        assert str(settings.public_url) == "http://prod-opencloud:9200/"
        assert settings.keycloak_client_secret.get_secret_value() == "rotated-secret"

    def test_public_url_falls_back_to_base_url(self, monkeypatch: pytest.MonkeyPatch):
        _clear_cloud_env(monkeypatch)
        monkeypatch.setenv("OPENCLOUD_URL", "http://internal-oc:9200")
        settings = load_main_cloud_config()
        assert isinstance(settings, OpenCloudSettings)
        assert str(settings.public_url) == "http://internal-oc:9200/"


class TestBuildBackendDefaultRouting:
    """``build_backend(None)`` → routed through ``load_main_cloud_config``."""

    def test_no_arg_returns_opencloud_on_greenfield(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        from orchestrator.services.cloud import OpenCloudBackend, build_backend

        _clear_cloud_env(monkeypatch)
        _opencloud_env(monkeypatch)
        backend = build_backend()
        assert isinstance(backend, OpenCloudBackend)
        assert backend.backend_id == "opencloud"

    def test_no_arg_returns_nextcloud_on_legacy(self, monkeypatch: pytest.MonkeyPatch):
        from orchestrator.services.cloud import NextcloudBackend, build_backend

        _clear_cloud_env(monkeypatch)
        monkeypatch.setenv("NEXTCLOUD_ADMIN_PASSWORD", "admin")
        backend = build_backend()
        assert isinstance(backend, NextcloudBackend)

    def test_explicit_nextcloud_still_works(self, monkeypatch: pytest.MonkeyPatch):
        from orchestrator.services.cloud import NextcloudBackend, build_backend

        _clear_cloud_env(monkeypatch)
        _opencloud_env(monkeypatch)
        backend = build_backend("nextcloud")
        assert isinstance(backend, NextcloudBackend)

    def test_explicit_opencloud_still_works(self, monkeypatch: pytest.MonkeyPatch):
        from orchestrator.services.cloud import OpenCloudBackend, build_backend

        _clear_cloud_env(monkeypatch)
        _opencloud_env(monkeypatch)
        backend = build_backend("opencloud")
        assert isinstance(backend, OpenCloudBackend)


class TestMainCloudSelected:
    """Whether the deployment names a main cloud at all.

    A Helm release without a cloud renders no cloud env, and the resolver
    then builds the development OpenCloud at localhost, which reports itself
    configured; ``main_cloud_selected`` tells that release apart from one
    whose cloud is down (external_nextcloud_gives_srw_no_installation_proof).
    """

    @pytest.mark.parametrize(
        ("env", "selected"),
        [
            pytest.param({}, False, id="no-cloud-env"),
            pytest.param({"NEXTCLOUD_PORT": "8800"}, False, id="port-only"),
            pytest.param({"MAIN_CLOUD_BACKEND": "nextcloud"}, True, id="nextcloud"),
            pytest.param({"MAIN_CLOUD_BACKEND": "opencloud"}, True, id="opencloud"),
            pytest.param({"MAIN_CLOUD_BACKEND": "webdav"}, False, id="webdav"),
            pytest.param(
                {"NEXTCLOUD_URL": "http://nc.example"}, True, id="legacy-nextcloud"
            ),
            pytest.param(
                {"OPENCLOUD_URL": "http://opencloud:9200"}, True, id="opencloud-url"
            ),
            pytest.param(
                {"MAIN_CLOUD_URL": "http://cloud.example"}, True, id="main-cloud-url"
            ),
        ],
    )
    def test_the_environment_decides(
        self, monkeypatch: pytest.MonkeyPatch, env: dict[str, str], selected: bool
    ):
        _clear_cloud_env(monkeypatch)
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        assert main_cloud_selected() is selected

    def test_a_cloudless_release_still_resolves_to_a_configured_opencloud(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """Why the predicate exists: the fallback looks configured."""
        from orchestrator.services.cloud import build_backend

        _clear_cloud_env(monkeypatch)
        assert build_backend().is_configured is True
        assert main_cloud_selected() is False
