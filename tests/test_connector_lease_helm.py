"""The credential lease exchange's chart surface (connector drivers C2).

The exchange gets its own container and Service port, no Ingress routes it,
and a NetworkPolicy admits only the driver namespace to it while the API
port stays open to every source.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


def render(*settings: str) -> list[dict]:
    command = [
        "helm",
        "template",
        "srw",
        str(ROOT / "helm"),
        "-n",
        "srw",
        "-f",
        str(ROOT / "helm/ci/test-values.yaml"),
    ]
    for setting in settings:
        command.extend(["--set", setting])
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def _orchestrator(docs: list[dict]) -> tuple[dict, dict, dict]:
    deployment = next(
        doc
        for doc in docs
        if doc["kind"] == "Deployment"
        and doc["metadata"]["name"].endswith("-orchestrator")
    )
    container = next(
        c
        for c in deployment["spec"]["template"]["spec"]["containers"]
        if c["name"] == "orchestrator"
    )
    service = next(
        doc
        for doc in docs
        if doc["kind"] == "Service"
        and doc["metadata"]["name"].endswith("-orchestrator")
    )
    return deployment, container, service


def _env(container: dict) -> dict[str, str]:
    return {item["name"]: item.get("value") for item in container.get("env", [])}


def _policy(docs: list[dict]) -> dict | None:
    return next(
        (
            doc
            for doc in docs
            if doc["kind"] == "NetworkPolicy"
            and doc["metadata"]["name"].endswith("-orchestrator-ingress")
        ),
        None,
    )


def test_the_exchange_has_its_own_port_and_the_probe_is_off():
    docs = render()
    _deployment, container, service = _orchestrator(docs)
    assert {"name": "lease-exchange", "containerPort": 8088} in container["ports"]
    ports = {port["name"]: port for port in service["spec"]["ports"]}
    assert ports["http"]["port"] == 8085
    assert ports["lease-exchange"] == {
        "name": "lease-exchange",
        "port": 8088,
        "targetPort": "lease-exchange",
    }
    env = _env(container)
    assert env["CONNECTOR_LEASE_EXCHANGE_PORT"] == "8088"
    assert env["CONNECTOR_LEASE_TTL_SECONDS"] == "900"
    assert env["CONNECTOR_LEASE_PROBE_ENABLED"] == "false"


def test_the_policy_keeps_the_api_open_and_admits_only_drivers_to_the_exchange():
    policy = _policy(render())
    assert policy is not None
    assert policy["spec"]["policyTypes"] == ["Ingress"]
    api, exchange = policy["spec"]["ingress"]
    assert api == {"ports": [{"protocol": "TCP", "port": 8085}]}
    assert exchange["ports"] == [{"protocol": "TCP", "port": 8088}]
    (peer,) = exchange["from"]
    assert peer == {
        "namespaceSelector": {
            "matchLabels": {
                "kubernetes.io/metadata.name": "srw-superhuman-remote-worker-connectors"
            }
        }
    }


def test_no_ingress_routes_the_exchange_port():
    for doc in render():
        if doc["kind"] != "Ingress":
            continue
        for rule in doc["spec"].get("rules", []):
            for path in rule.get("http", {}).get("paths", []):
                port = path["backend"]["service"].get("port", {})
                assert port.get("number") != 8088
                assert port.get("name") != "lease-exchange"


def test_port_zero_turns_the_exchange_and_its_policy_off():
    docs = render("orchestrator.connectorLeases.exchangePort=0")
    _deployment, container, service = _orchestrator(docs)
    assert all(port.get("name") != "lease-exchange" for port in container["ports"])
    assert [port["port"] for port in service["spec"]["ports"]] == [8085]
    assert _env(container)["CONNECTOR_LEASE_EXCHANGE_PORT"] == "0"
    assert _policy(docs) is None


def test_the_driver_namespace_and_the_policy_switch_are_settable():
    docs = render(
        "orchestrator.connectorLeases.networkPolicy.driverNamespace=drivers",
        "orchestrator.connectorLeases.probeDriver=true",
    )
    policy = _policy(docs)
    selector = policy["spec"]["ingress"][1]["from"][0]["namespaceSelector"]
    assert selector["matchLabels"]["kubernetes.io/metadata.name"] == "drivers"
    _deployment, container, _service = _orchestrator(docs)
    assert _env(container)["CONNECTOR_LEASE_PROBE_ENABLED"] == "true"
    assert (
        _policy(render("orchestrator.connectorLeases.networkPolicy.enabled=false"))
        is None
    )


def test_the_api_port_is_refused_as_the_exchange_port():
    with pytest.raises(subprocess.CalledProcessError):
        render("orchestrator.connectorLeases.exchangePort=8085")


def test_the_k3d_profile_turns_the_probe_on_with_a_short_window():
    example = yaml.safe_load(
        (ROOT / "deployment/values-local.yaml.example").read_text()
    )
    leases = example["orchestrator"]["connectorLeases"]
    assert leases["probeDriver"] is True
    assert leases["ttlSeconds"] <= 300
    assert leases["sweepIntervalSeconds"] <= leases["ttlSeconds"] / 4
