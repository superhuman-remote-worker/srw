"""Service-plane driver hosting's chart surface (connector drivers D5).

The connector driver namespace and its baseline (Pod Security baseline, a
static default deny, quotas, a LimitRange, a token-less ServiceAccount and the
orchestrator's Role) render only when ``connectors.servicePods.enabled``; the
chart renders cleanly either way, and the k3d profile turns it on.
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

NAMESPACE = "srw-superhuman-remote-worker-connectors"
EXCHANGE = "orchestrator.connectorLeases.exchangePort=8088"
ON = "connectors.servicePods.enabled=true"


def render(*settings: str, values: tuple[Path, ...] = ()) -> list[dict]:
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
    for path in values:
        command.extend(["-f", str(path)])
    for setting in settings:
        command.extend(["--set", setting])
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def in_namespace(docs: list[dict], namespace: str = NAMESPACE) -> list[dict]:
    return [
        doc
        for doc in docs
        if doc["metadata"].get("namespace") == namespace
        or (doc["kind"] == "Namespace" and doc["metadata"]["name"] == namespace)
    ]


def one(docs: list[dict], kind: str, name: str | None = None) -> dict:
    found = [
        doc
        for doc in docs
        if doc["kind"] == kind and (name is None or doc["metadata"]["name"] == name)
    ]
    assert len(found) == 1, (kind, name, [d["metadata"]["name"] for d in found])
    return found[0]


def test_off_by_default_renders_no_driver_namespace():
    docs = render()
    assert in_namespace(docs) == []
    assert not any(doc["kind"] == "LimitRange" for doc in docs)


def test_the_baseline_renders_when_enabled():
    docs = in_namespace(render(EXCHANGE, ON))
    namespace = one(docs, "Namespace")
    labels = namespace["metadata"]["labels"]
    assert labels["pod-security.kubernetes.io/enforce"] == "baseline"
    assert labels["pod-security.kubernetes.io/enforce-version"] == "v1.31"
    assert labels["pod-security.kubernetes.io/audit"] == "restricted"
    assert namespace["metadata"]["annotations"] == {"helm.sh/resource-policy": "keep"}

    deny = one(docs, "NetworkPolicy", "srw-connectors-default-deny")
    assert deny["spec"] == {"podSelector": {}, "policyTypes": ["Ingress", "Egress"]}

    account = one(docs, "ServiceAccount", "srw-connector-driver")
    assert account["automountServiceAccountToken"] is False
    # The driver account is bound to nothing.
    assert not any(
        doc["kind"] in {"RoleBinding", "ClusterRoleBinding"}
        and any(
            subject.get("name") == "srw-connector-driver"
            for subject in doc.get("subjects", [])
        )
        for doc in render(EXCHANGE, ON)
    )


def test_quotas_split_service_and_bind_time_pods():
    docs = in_namespace(
        render(EXCHANGE, ON, "connectors.servicePods.maxInstallation=7")
    )
    service = one(docs, "ResourceQuota", "srw-connector-service-pods")
    assert service["spec"] == {"scopes": ["NotTerminating"], "hard": {"pods": "7"}}
    bind_time = one(docs, "ResourceQuota", "srw-connector-bind-time-pods")
    assert bind_time["spec"] == {"scopes": ["Terminating"], "hard": {"pods": "10"}}
    compute = one(docs, "ResourceQuota", "srw-connector-compute")["spec"]["hard"]
    assert {
        "requests.cpu",
        "requests.memory",
        "limits.cpu",
        "limits.memory",
        "requests.ephemeral-storage",
        "count/secrets",
        "count/services",
    } <= set(compute)
    assert compute["services.loadbalancers"] == "0"
    assert compute["services.nodeports"] == "0"
    (limits,) = one(docs, "LimitRange")["spec"]["limits"]
    assert limits["type"] == "Container"
    assert set(limits) == {"type", "defaultRequest", "default", "max"}
    assert limits["max"]["memory"] == "2Gi"


def test_the_orchestrator_role_has_no_exec_attach_or_log():
    docs = in_namespace(render(EXCHANGE, ON))
    role = one(docs, "Role", "srw-connector-hosting")
    resources = {
        resource: set(rule["verbs"])
        for rule in role["rules"]
        for resource in rule["resources"]
    }
    assert set(resources) == {"pods", "secrets", "services", "networkpolicies"}
    assert "list" in resources["pods"] and "list" in resources["networkpolicies"]
    assert not {"pods/exec", "pods/attach", "pods/log"} & set(resources)
    binding = one(docs, "RoleBinding", "srw-connector-hosting")
    (subject,) = binding["subjects"]
    assert subject["kind"] == "ServiceAccount"
    assert subject["namespace"] == "srw"


def test_one_namespace_for_the_baseline_and_the_exchange_policy():
    docs = render(
        EXCHANGE,
        ON,
        "orchestrator.connectorLeases.networkPolicy.driverNamespace=drivers",
    )
    assert one(in_namespace(docs, "drivers"), "Namespace")
    policy = next(
        doc
        for doc in docs
        if doc["kind"] == "NetworkPolicy"
        and doc["metadata"]["name"].endswith("-orchestrator-ingress")
    )
    peer = policy["spec"]["ingress"][1]["from"][0]["namespaceSelector"]
    assert peer["matchLabels"]["kubernetes.io/metadata.name"] == "drivers"
    # The hosting key wins over the exchange's.
    docs = render(
        EXCHANGE,
        ON,
        "orchestrator.connectorLeases.networkPolicy.driverNamespace=drivers",
        "connectors.servicePods.namespace=hosted",
    )
    assert one(in_namespace(docs, "hosted"), "Namespace")
    assert in_namespace(docs, "drivers") == []


@pytest.mark.parametrize(
    "settings",
    [
        # Hosting needs the exchange: driver pods authenticate to it.
        (ON,),
        (EXCHANGE, ON, "connectors.servicePods.namespace=srw"),
        (
            EXCHANGE,
            ON,
            "connectors.servicePods.namespace=same",
            "manifestHosting.namespace=same",
        ),
    ],
)
def test_misconfigurations_fail_at_template_time(settings):
    with pytest.raises(subprocess.CalledProcessError):
        render(*settings)


def test_the_k3d_profile_turns_hosting_on_and_renders():
    example = ROOT / "deployment/values-local.yaml.example"
    values = yaml.safe_load(example.read_text())
    assert values["connectors"]["servicePods"]["enabled"] is True
    docs = in_namespace(render(values=(example,)), "srw-connectors")
    assert one(docs, "Namespace")
    assert one(docs, "NetworkPolicy", "srw-connectors-default-deny")
