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


def test_test_trusts_the_gitea_the_chart_configures_on_its_ports_only():
    """A connector's Test trusts SRW's own Gitea at the endpoints the chart
    gives the orchestrator (its ConfigMap's GITEA_* values), with no
    privateHosts entry; every endpoint names its port."""
    from orchestrator.application.settings import parse_gitea_endpoints

    docs = render()
    data: dict[str, str] = {}
    for doc in docs:
        if doc["kind"] == "ConfigMap" and "GITEA_INTERNAL_URL" in (
            doc.get("data") or {}
        ):
            data = doc["data"]
    assert data, "no ConfigMap carries GITEA_INTERNAL_URL"
    endpoints = parse_gitea_endpoints(data)
    assert endpoints
    assert all(entry.rsplit(":", 1)[1].isdigit() for entry in endpoints)
    internal = data["GITEA_INTERNAL_URL"]
    if internal:
        from urllib.parse import urlsplit

        parsed = urlsplit(internal)
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        assert f"{parsed.hostname}:{port}" in endpoints
    if data.get("GITEA_SSH_INTERNAL_PORT", "0") != "0":
        assert any(
            entry.endswith(f":{data['GITEA_SSH_INTERNAL_PORT']}") for entry in endpoints
        )


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
