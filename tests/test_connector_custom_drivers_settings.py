"""Registered driver images: their chart keys, settings and routes (D6).

* ``connectors.drivers.trustedRepositories`` and
  ``connectors.customDrivers.{privileged,bindDeadlineSeconds,bindWaitSeconds}``
  reach the orchestrator; privilege is off by default and opt-in only;
* the bind-time cap is the namespace's Terminating pod quota;
* ``/api/connector-drivers`` authenticates first and answers the service's
  registrations, never a stored secret.
"""

from __future__ import annotations

import json
import shutil
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.application.settings import (
    MAX_BIND_WAIT_SECONDS,
    DeploymentSettings,
    parse_repository_list,
)
from tests._mounted_router import mount_router
from tests.test_connector_service_hosting_helm import orchestrator_env, render

helm = pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")


class TestSettings:
    def test_defaults_trust_nothing_and_grant_no_privilege(self, monkeypatch):
        for name in (
            "CONNECTOR_DRIVER_TRUSTED_REPOSITORIES",
            "CONNECTOR_CUSTOM_DRIVERS_PRIVILEGED",
            "CONNECTOR_BIND_TIME_MAX_PODS",
            "CONNECTOR_BIND_TIME_DEADLINE_SECONDS",
            "CONNECTOR_BIND_TIME_WAIT_SECONDS",
            "CONNECTOR_BIND_TIME_SPEC_PODS_PER_USER",
        ):
            monkeypatch.delenv(name, raising=False)
        settings = DeploymentSettings.from_environment()
        assert settings.connector_driver_trusted_repositories == ()
        assert settings.connector_custom_drivers_privileged is False
        assert settings.connector_bind_time_max_pods == 10
        assert settings.connector_bind_time_deadline_seconds == 120.0
        # Under the agent's 30 s request to the orchestrator.
        assert settings.connector_bind_time_wait_seconds == 20.0
        assert settings.connector_bind_time_spec_pods_per_user == 2

    def test_the_bind_wait_never_outlasts_the_agent_s_request(self, monkeypatch):
        monkeypatch.setenv("CONNECTOR_BIND_TIME_WAIT_SECONDS", "60")
        settings = DeploymentSettings.from_environment()
        assert settings.connector_bind_time_wait_seconds == MAX_BIND_WAIT_SECONDS
        assert MAX_BIND_WAIT_SECONDS < 30
        monkeypatch.setenv("CONNECTOR_BIND_TIME_WAIT_SECONDS", "0")
        assert (
            DeploymentSettings.from_environment().connector_bind_time_wait_seconds == 0
        )

    def test_the_environment_sets_them(self, monkeypatch):
        monkeypatch.setenv(
            "CONNECTOR_DRIVER_TRUSTED_REPOSITORIES", '["ghcr.io/acme", "quay.io/x/y"]'
        )
        monkeypatch.setenv("CONNECTOR_CUSTOM_DRIVERS_PRIVILEGED", "true")
        monkeypatch.setenv("CONNECTOR_BIND_TIME_MAX_PODS", "3")
        monkeypatch.setenv("CONNECTOR_BIND_TIME_DEADLINE_SECONDS", "5")
        monkeypatch.setenv("CONNECTOR_BIND_TIME_WAIT_SECONDS", "15")
        monkeypatch.setenv("CONNECTOR_BIND_TIME_SPEC_PODS_PER_USER", "1")
        settings = DeploymentSettings.from_environment()
        assert settings.connector_driver_trusted_repositories == (
            "ghcr.io/acme",
            "quay.io/x/y",
        )
        assert settings.connector_custom_drivers_privileged is True
        assert settings.connector_bind_time_max_pods == 3
        # A pod needs time for its canary wait: never under 30 seconds.
        assert settings.connector_bind_time_deadline_seconds == 30.0
        assert settings.connector_bind_time_wait_seconds == 15.0
        assert settings.connector_bind_time_spec_pods_per_user == 1

    @pytest.mark.parametrize("raw", ["yes please", "1", "on", "TRUE"])
    def test_privilege_is_opt_in(self, monkeypatch, raw):
        monkeypatch.setenv("CONNECTOR_CUSTOM_DRIVERS_PRIVILEGED", raw)
        expected = raw.strip().lower() in ("1", "true", "yes", "on")
        assert (
            DeploymentSettings.from_environment().connector_custom_drivers_privileged
            is expected
        )

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            (None, ()),
            ("", ()),
            ('["ghcr.io/acme"]', ("ghcr.io/acme",)),
            ("ghcr.io/acme", ()),
            ('{"a": 1}', ()),
            ('["ok", ""]', ()),
        ],
    )
    def test_a_malformed_list_trusts_nothing(self, raw, expected):
        assert parse_repository_list(raw) == expected


@helm
class TestChart:
    def test_custom_drivers_are_unprivileged_by_default(self):
        env = orchestrator_env(render())
        assert json.loads(env["CONNECTOR_DRIVER_TRUSTED_REPOSITORIES"]) == []
        assert env["CONNECTOR_CUSTOM_DRIVERS_PRIVILEGED"] == "false"
        assert env["CONNECTOR_BIND_TIME_DEADLINE_SECONDS"] == "120"
        assert env["CONNECTOR_BIND_TIME_WAIT_SECONDS"] == "20"
        assert env["CONNECTOR_BIND_TIME_SPEC_PODS_PER_USER"] == "2"
        # The orchestrator's cap is the namespace's Terminating pod quota.
        assert env["CONNECTOR_BIND_TIME_MAX_PODS"] == "10"

    def test_the_operator_sets_trust_and_privilege(self):
        env = orchestrator_env(
            render(
                "connectors.drivers.trustedRepositories[0]=ghcr.io/acme",
                "connectors.drivers.trustedRepositories[1]=quay.io/team",
                "connectors.customDrivers.privileged=true",
                "connectors.customDrivers.bindDeadlineSeconds=300",
                "connectors.servicePods.quota.bindTimePods=4",
            )
        )
        assert json.loads(env["CONNECTOR_DRIVER_TRUSTED_REPOSITORIES"]) == [
            "ghcr.io/acme",
            "quay.io/team",
        ]
        assert env["CONNECTOR_CUSTOM_DRIVERS_PRIVILEGED"] == "true"
        assert env["CONNECTOR_BIND_TIME_DEADLINE_SECONDS"] == "300"
        assert env["CONNECTOR_BIND_TIME_MAX_PODS"] == "4"

    def test_a_deadline_under_thirty_seconds_fails_the_schema(self):
        import subprocess

        with pytest.raises(subprocess.CalledProcessError):
            render("connectors.customDrivers.bindDeadlineSeconds=5")

    def test_a_bind_wait_past_the_agent_s_request_fails_the_schema(self):
        import subprocess

        with pytest.raises(subprocess.CalledProcessError):
            render("connectors.customDrivers.bindWaitSeconds=30")
        env = orchestrator_env(render("connectors.customDrivers.bindWaitSeconds=0"))
        assert env["CONNECTOR_BIND_TIME_WAIT_SECONDS"] == "0"


USER = {"id": "00000000-0000-0000-0000-0000000000d6", "is_admin": False}


def _client(**over):
    from orchestrator.routers.connector_drivers import (
        ConnectorDriversDependencies,
        router,
    )
    from orchestrator.services.connector_driver_registrations import (
        DriverTrustPolicy,
    )

    async def approved(_request, _store):
        return USER

    values = {
        # The usage a manager's view shows (none here).
        "store": SimpleNamespace(fetch=AsyncMock(return_value=[])),
        "resolve_image": AsyncMock(),
        "run_spec": None,
        "trust": DriverTrustPolicy(trusted_repositories=("ghcr.io/acme",)),
        "require_approved_user": approved,
        **over,
    }
    deps = ConnectorDriversDependencies(**values)
    app = mount_router(
        router, factories={"connector_drivers_dependencies_factory": lambda: deps}
    )
    return TestClient(app), deps


def _registration():
    return SimpleNamespace(
        id="00000000-0000-0000-0000-0000000000e1",
        scope_kind="Account",
        owner_id=USER["id"],
        project_id=None,
        image_reference="ghcr.io/acme/env:1",
        view=lambda policy: {
            "id": "00000000-0000-0000-0000-0000000000e1",
            "trust": policy.trust("ghcr.io/acme/env:1"),
        },
    )


class TestRoutes:
    def test_list_answers_the_visible_registrations(self):
        client, deps = _client()
        with patch(
            "orchestrator.services.connector_driver_registrations."
            "list_visible_registrations",
            AsyncMock(return_value=[_registration()]),
        ) as listed:
            response = client.get("/api/connector-drivers")
        assert response.status_code == 200
        assert response.json() == {
            "registrations": [
                {
                    "id": "00000000-0000-0000-0000-0000000000e1",
                    "trust": {
                        "tier": "trusted",
                        "trusted": True,
                        "privileged": True,
                        "image": "ghcr.io/acme/env:1",
                        "claims_declared_by_author": False,
                    },
                    # The caller's own Account: they may disable it.
                    "can_manage": True,
                }
            ]
        }
        assert listed.await_args.args == (deps.store, USER)

    def test_register_passes_the_scope_and_answers_201(self):
        client, deps = _client()
        with patch(
            "orchestrator.services.connector_driver_registrations.register_driver",
            AsyncMock(return_value=_registration()),
        ) as registered:
            response = client.post(
                "/api/connector-drivers",
                json={
                    "image": "ghcr.io/acme/env:1",
                    "scope": {"kind": "Project", "name": "p1"},
                    "name": "acme.env/v1",
                },
            )
        assert response.status_code == 201
        kwargs = registered.await_args.kwargs
        assert kwargs["scope"] == {"kind": "Project", "name": "p1"}
        assert kwargs["image_reference"] == "ghcr.io/acme/env:1"
        assert kwargs["name"] == "acme.env/v1"
        assert kwargs["policy"] is deps.trust

    def test_unknown_fields_and_scopes_are_refused(self):
        client, _ = _client()
        assert (
            client.post(
                "/api/connector-drivers", json={"image": "x", "spec": {}}
            ).status_code
            == 422
        )
        assert (
            client.post(
                "/api/connector-drivers",
                json={"image": "x", "scope": {"kind": "Cluster", "name": "c"}},
            ).status_code
            == 422
        )

    def test_an_unapproved_caller_is_refused_first(self):
        from fastapi import HTTPException

        async def refuse(_request, _store):
            raise HTTPException(status_code=403, detail="not approved")

        client, _ = _client(require_approved_user=refuse)
        with patch(
            "orchestrator.services.connector_driver_registrations.register_driver",
            AsyncMock(),
        ) as registered:
            assert (
                client.post("/api/connector-drivers", json={"image": "x"}).status_code
                == 403
            )
        registered.assert_not_awaited()

    @pytest.mark.parametrize(
        ("action", "disabled"), [("disable", True), ("enable", False)]
    )
    def test_disable_and_enable_are_the_service_s(self, action, disabled):
        client, deps = _client()
        with patch(
            "orchestrator.services.connector_driver_registrations."
            "set_registration_disabled",
            AsyncMock(return_value=_registration()),
        ) as switched:
            response = client.post(f"/api/connector-drivers/r1/{action}")
        assert response.status_code == 200
        assert switched.await_args.args == (deps.store, USER, "r1")
        assert switched.await_args.kwargs["disabled"] is disabled
