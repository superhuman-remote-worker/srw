"""Provider-minted credentials' chart surface (connector drivers C5).

``connectors.providerMinting`` turns minting on or off and names the
provider hosts the operator trusts at a private or cluster address; the
orchestrator reads both from its environment.
"""

from __future__ import annotations

import shutil
import subprocess

import pytest

from orchestrator.application.settings import DeploymentSettings
from tests.test_connector_lease_helm import _env, _orchestrator, render

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


def test_minting_is_on_by_default_with_no_trusted_private_host():
    _, container, _ = _orchestrator(render())
    env = _env(container)
    assert env.get("CONNECTOR_PROVIDER_MINTING_ENABLED", "true") == "true"
    assert env.get("CONNECTOR_PROVIDER_MINTING_PRIVATE_HOSTS", "") == ""


def test_the_values_reach_the_orchestrator():
    _, container, _ = _orchestrator(
        render(
            "connectors.providerMinting.enabled=false",
            "connectors.providerMinting.privateHosts={kubernetes.default.svc,"
            "ghe.corp.example:8443}",
        )
    )
    env = _env(container)
    assert env["CONNECTOR_PROVIDER_MINTING_ENABLED"] == "false"
    assert env["CONNECTOR_PROVIDER_MINTING_PRIVATE_HOSTS"] == (
        "kubernetes.default.svc,ghe.corp.example:8443"
    )


@pytest.mark.parametrize(
    "host", ["https://kubernetes.default.svc", "Kube.Internal", "a/b", "h:p"]
)
def test_a_private_host_is_a_host_or_host_and_port(host):
    with pytest.raises(subprocess.CalledProcessError):
        render(f"connectors.providerMinting.privateHosts={{{host}}}")


def test_the_settings_read_the_environment(monkeypatch):
    monkeypatch.setenv("CONNECTOR_PROVIDER_MINTING_ENABLED", "false")
    monkeypatch.setenv(
        "CONNECTOR_PROVIDER_MINTING_PRIVATE_HOSTS",
        "Kubernetes.Default.Svc, ghe.corp.example:8443",
    )
    settings = DeploymentSettings.from_environment()
    assert settings.connector_provider_minting_enabled is False
    assert settings.connector_provider_minting_private_hosts == frozenset(
        {"kubernetes.default.svc", "ghe.corp.example:8443"}
    )
    monkeypatch.delenv("CONNECTOR_PROVIDER_MINTING_ENABLED")
    monkeypatch.delenv("CONNECTOR_PROVIDER_MINTING_PRIVATE_HOSTS")
    settings = DeploymentSettings.from_environment()
    assert settings.connector_provider_minting_enabled is True
    assert settings.connector_provider_minting_private_hosts == frozenset()
