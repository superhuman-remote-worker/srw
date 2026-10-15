"""Native hosting needs independent authority before an operator enables it."""

from pathlib import Path
import json
import shutil
import subprocess

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


def render(*settings, check=True):
    command = [
        "helm",
        "template",
        "native-test",
        str(ROOT / "helm"),
        "-n",
        "control-plane",
        "-f",
        str(ROOT / "helm/ci/test-values.yaml"),
    ]
    for setting in settings:
        command.extend(["--set", setting])
    result = subprocess.run(command, capture_output=True, text=True, check=check)
    if not check:
        return result
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


@pytest.mark.parametrize("enabled", [False, True])
def test_native_namespace_baseline_and_scoped_rbac_exist_before_enablement(enabled):
    docs = render(f"manifestHosting.networkIsolationVerified={str(enabled).lower()}")
    config = next(
        doc
        for doc in docs
        if doc["kind"] == "ConfigMap" and "MANIFEST_NAMESPACE" in doc.get("data", {})
    )
    namespace = config["data"]["MANIFEST_NAMESPACE"]
    assert namespace != "control-plane"
    assert config["data"]["MANIFEST_NETWORK_ISOLATION_VERIFIED"] == str(enabled).lower()
    assert any(
        doc["kind"] == "Namespace" and doc["metadata"]["name"] == namespace
        for doc in docs
    )
    policy = next(
        doc
        for doc in docs
        if doc["kind"] == "NetworkPolicy"
        and doc["metadata"].get("namespace") == namespace
    )
    assert policy["spec"] == {"podSelector": {}, "policyTypes": ["Ingress", "Egress"]}
    role = next(
        doc
        for doc in docs
        if doc["kind"] == "Role" and doc["metadata"].get("namespace") == namespace
    )
    assert {resource for rule in role["rules"] for resource in rule["resources"]} == {
        "pods",
        "pods/finalizers",
        "persistentvolumeclaims",
        "secrets",
        "networkpolicies",
    }
    binding = next(
        doc
        for doc in docs
        if doc["kind"] == "RoleBinding"
        and doc["metadata"].get("namespace") == namespace
    )
    assert binding["subjects"][0]["namespace"] == "control-plane"
    assert binding["subjects"][0]["kind"] == "ServiceAccount"


def test_native_hosting_defaults_disabled_and_contract_cutover_recreates():
    docs = render()
    config = next(
        doc
        for doc in docs
        if doc["kind"] == "ConfigMap" and "MANIFEST_NAMESPACE" in doc.get("data", {})
    )
    assert config["data"]["MANIFEST_NETWORK_ISOLATION_VERIFIED"] == "false"
    assert json.loads(config["data"]["MANIFEST_HARNESS_EGRESS"]) == []
    deployment = next(
        doc
        for doc in docs
        if doc["kind"] == "Deployment"
        and doc["metadata"]["name"].endswith("-orchestrator")
    )
    assert deployment["spec"]["strategy"]["type"] == "Recreate"


@pytest.mark.parametrize(
    ("settings", "expected_namespace"),
    [
        ((), "native-test-native"),
        (("manifestHosting.namespace=other-native",), "other-native"),
    ],
)
def test_core_receives_the_native_namespace_bound_by_the_chart(
    settings, expected_namespace
):
    docs = render("fullnameOverride=native-test", *settings)
    config = next(
        doc
        for doc in docs
        if doc["kind"] == "ConfigMap" and "MANIFEST_NAMESPACE" in doc.get("data", {})
    )
    deployment = next(
        doc
        for doc in docs
        if doc["kind"] == "Deployment"
        and doc["metadata"]["name"].endswith("-orchestrator")
    )
    namespace_env = [
        entry
        for entry in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
        if entry["name"] == "MANIFEST_NAMESPACE"
    ]
    assert namespace_env == [
        {
            "name": "MANIFEST_NAMESPACE",
            "valueFrom": {
                "configMapKeyRef": {
                    "name": config["metadata"]["name"],
                    "key": "MANIFEST_NAMESPACE",
                }
            },
        }
    ]
    assert config["data"]["MANIFEST_NAMESPACE"] == expected_namespace
    assert any(
        doc["kind"] == "Namespace" and doc["metadata"]["name"] == expected_namespace
        for doc in docs
    )


@pytest.mark.parametrize("configured", [False, True])
@pytest.mark.parametrize(
    ("key", "default", "override"),
    [
        ("MANIFEST_NETWORK_ISOLATION_VERIFIED", "false", "true"),
        ("MANIFEST_MAX_CONCURRENT_JOBS", "10", "2"),
        (
            "MANIFEST_HARNESS_EGRESS",
            [],
            [
                {
                    "to": [{"ipBlock": {"cidr": "192.0.2.5/32"}}],
                    "ports": [{"port": 443}],
                }
            ],
        ),
    ],
)
def test_core_receives_existing_native_hosting_controls(
    configured, key, default, override
):
    settings = (
        (
            "manifestHosting.networkIsolationVerified=true",
            "manifestHosting.maxConcurrentJobs=2",
            "manifestHosting.harnessEgress[0].to[0].ipBlock.cidr=192.0.2.5/32",
            "manifestHosting.harnessEgress[0].ports[0].port=443",
        )
        if configured
        else ()
    )
    docs = render(*settings)
    config = next(
        doc for doc in docs if doc["kind"] == "ConfigMap" and key in doc.get("data", {})
    )
    deployment = next(
        doc
        for doc in docs
        if doc["kind"] == "Deployment"
        and doc["metadata"]["name"].endswith("-orchestrator")
    )
    assert [
        entry
        for entry in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
        if entry["name"] == key
    ] == [
        {
            "name": key,
            "valueFrom": {
                "configMapKeyRef": {"name": config["metadata"]["name"], "key": key}
            },
        }
    ]
    expected = override if configured else default
    value = config["data"][key]
    assert (json.loads(value) if isinstance(expected, list) else value) == expected


@pytest.mark.parametrize(
    "setting",
    [
        "manifestHosting.networkIsolationVerified=true",
        "manifestHosting.namespace=other-native",
        "manifestHosting.maxConcurrentJobs=2",
        "manifestHosting.harnessEgress[0].to[0].ipBlock.cidr=192.0.2.5/32",
    ],
)
def test_native_hosting_changes_roll_process_without_reloader(setting):
    def template(settings):
        return next(
            doc["spec"]["template"]
            for doc in render("reloader.enabled=false", *settings)
            if doc["kind"] == "Deployment"
            and doc["metadata"]["name"].endswith("-orchestrator")
        )

    before = template(())
    after = template((setting,))
    assert before != after
    key = "checksum/manifest-hosting"
    assert (
        before["metadata"]["annotations"][key] != after["metadata"]["annotations"][key]
    )


@pytest.mark.parametrize(
    "setting",
    [
        "manifestHosting.namespace=control-plane",
        "manifestHosting.namespace=InvalidNamespace",
        "manifestHosting.maxConcurrentJobs=0",
        "manifestHosting.harnessEgress[0].unknown=true",
        "manifestHosting.harnessEgress[0].ports[0].port=70000",
    ],
)
def test_invalid_native_hosting_authority_fails_render(setting):
    assert render(setting, check=False).returncode != 0


def test_operator_provider_and_dns_rules_reach_the_installed_json_setting():
    docs = render(
        "manifestHosting.harnessEgress[0].to[0].ipBlock.cidr=192.0.2.5/32",
        "manifestHosting.harnessEgress[0].ports[0].port=443",
        "manifestHosting.harnessEgress[1].to[0].namespaceSelector.matchLabels.name=kube-system",
        "manifestHosting.harnessEgress[1].to[0].podSelector.matchLabels.app=dns",
        "manifestHosting.harnessEgress[1].ports[0].port=53",
        "manifestHosting.harnessEgress[1].ports[0].protocol=UDP",
    )
    config = next(
        doc
        for doc in docs
        if doc["kind"] == "ConfigMap"
        and "MANIFEST_HARNESS_EGRESS" in doc.get("data", {})
    )
    rules = json.loads(config["data"]["MANIFEST_HARNESS_EGRESS"])
    assert rules == [
        {"to": [{"ipBlock": {"cidr": "192.0.2.5/32"}}], "ports": [{"port": 443}]},
        {
            "to": [
                {
                    "namespaceSelector": {"matchLabels": {"name": "kube-system"}},
                    "podSelector": {"matchLabels": {"app": "dns"}},
                }
            ],
            "ports": [{"port": 53, "protocol": "UDP"}],
        },
    ]
