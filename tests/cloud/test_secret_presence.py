"""Tests for ``missing_secret_envs`` — the Issue 5 fail-loud presence check.

The loader keeps built-in dev defaults so a bare ``.env`` / test run "just
works", but the admin PUT/test endpoints must refuse to *activate* (or warn
before probing) a backend whose real secrets are not wired. ``missing_secret_envs``
reports exactly which required secret env vars are unset, mirroring the loader's
resolution precedence (``credentials_ref`` > legacy env fallbacks) without ever
reading a secret value or falling back to a dev default.

See ``knowledge-base/knowledge/issues/main_cloud.md`` Issue 5.
"""

from __future__ import annotations

import os

import pytest

from orchestrator.services.cloud.config import missing_secret_envs


def _clear_cloud_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wipe every MAIN_CLOUD_*, NEXTCLOUD_*, OPENCLOUD_*, MS365_* var."""
    for key in list(os.environ):
        if key.startswith(("MAIN_CLOUD_", "NEXTCLOUD_", "OPENCLOUD_", "MS365_")):
            monkeypatch.delenv(key, raising=False)


class TestMissingSecretEnvs:
    def test_nextcloud_all_secrets_missing(self, monkeypatch: pytest.MonkeyPatch):
        _clear_cloud_env(monkeypatch)
        fields = {m["field"] for m in missing_secret_envs("nextcloud")}
        assert fields == {"admin_password", "agent_password"}
        # oidc_client_secret is Optional on NextcloudSettings → never required.
        assert "oidc_client_secret" not in fields

    def test_nextcloud_all_secrets_present(self, monkeypatch: pytest.MonkeyPatch):
        _clear_cloud_env(monkeypatch)
        monkeypatch.setenv("NEXTCLOUD_ADMIN_PASSWORD", "real-admin")
        monkeypatch.setenv("NEXTCLOUD_AGENT_PASSWORD", "real-agent")
        assert missing_secret_envs("nextcloud") == []

    def test_main_cloud_alias_counts_as_present(self, monkeypatch: pytest.MonkeyPatch):
        _clear_cloud_env(monkeypatch)
        # The loader also accepts the MAIN_CLOUD_* aliases; the helper must too.
        monkeypatch.setenv("MAIN_CLOUD_ADMIN_PASSWORD", "x")
        monkeypatch.setenv("MAIN_CLOUD_AGENT_PASSWORD", "y")
        assert missing_secret_envs("nextcloud") == []

    def test_nextcloud_partial_missing(self, monkeypatch: pytest.MonkeyPatch):
        _clear_cloud_env(monkeypatch)
        monkeypatch.setenv("NEXTCLOUD_ADMIN_PASSWORD", "real-admin")
        missing = missing_secret_envs("nextcloud")
        assert [m["field"] for m in missing] == ["agent_password"]
        assert missing[0]["env_var"] == "NEXTCLOUD_AGENT_PASSWORD"

    def test_empty_string_treated_as_missing(self, monkeypatch: pytest.MonkeyPatch):
        _clear_cloud_env(monkeypatch)
        monkeypatch.setenv("NEXTCLOUD_ADMIN_PASSWORD", "")
        monkeypatch.setenv("NEXTCLOUD_AGENT_PASSWORD", "")
        fields = {m["field"] for m in missing_secret_envs("nextcloud")}
        assert fields == {"admin_password", "agent_password"}

    def test_opencloud_secret_missing(self, monkeypatch: pytest.MonkeyPatch):
        _clear_cloud_env(monkeypatch)
        missing = missing_secret_envs("opencloud")
        assert [m["field"] for m in missing] == ["keycloak_client_secret"]
        assert missing[0]["env_var"] == "OPENCLOUD_KEYCLOAK_CLIENT_SECRET"

    def test_opencloud_secret_present(self, monkeypatch: pytest.MonkeyPatch):
        _clear_cloud_env(monkeypatch)
        monkeypatch.setenv("OPENCLOUD_KEYCLOAK_CLIENT_SECRET", "shh")
        assert missing_secret_envs("opencloud") == []

    def test_the_environment_is_the_only_source(self, monkeypatch: pytest.MonkeyPatch):
        """No overlay can name another env var: Helm configures the cloud."""
        _clear_cloud_env(monkeypatch)
        monkeypatch.setenv("VAULT_NC_PASS", "from-vault")
        missing = missing_secret_envs("nextcloud")
        assert {m["field"] for m in missing} == {"admin_password", "agent_password"}
        assert {m["env_var"] for m in missing} == {
            "NEXTCLOUD_ADMIN_PASSWORD",
            "NEXTCLOUD_AGENT_PASSWORD",
        }

    def test_unknown_backend_returns_empty(self, monkeypatch: pytest.MonkeyPatch):
        _clear_cloud_env(monkeypatch)
        assert missing_secret_envs("definitely-not-a-backend") == []
