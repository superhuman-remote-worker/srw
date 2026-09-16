"""Preparation admission, controller settings, RBAC and network policy agree."""

import json
import pytest
from tests.test_manifest_hosting_helm import render

ENABLED = (
    "vm.mode=same-cluster",
    "agent.tailscale.enabled=false",
    "vm.lifecycleAuthSecretName=preparation-auth",
    "vmController.persistentRootdisk.enabled=true",
    "vmController.preparation.enabled=true",
)


@pytest.mark.parametrize("enabled", ["true", "false"])
def test_orchestrator_receives_every_preparation_admission_setting(enabled):
    docs = render(*ENABLED, "vmController.preparation.enabled=" + enabled)
    config = next(
        d
        for d in docs
        if d["kind"] == "ConfigMap" and "VM_PREPARATION_ENABLED" in d.get("data", {})
    )
    orchestrator = next(
        d
        for d in docs
        if d["kind"] == "Deployment" and d["metadata"]["name"].endswith("-orchestrator")
    )
    env = {
        item["name"]: item
        for item in orchestrator["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    for key in config["data"]:
        if key.startswith("VM_PREPARATION_"):
            assert env[key]["valueFrom"]["configMapKeyRef"] == {
                "name": config["metadata"]["name"],
                "key": key,
            }
    assert config["data"]["VM_PREPARATION_ENABLED"] == enabled


def test_preparation_capability_map_matches_controller_and_only_builder_gets_egress_policy():
    docs = render(*ENABLED)
    config = next(
        d["data"]
        for d in docs
        if d["kind"] == "ConfigMap" and "VM_PREPARATION_ENABLED" in d.get("data", {})
    )
    controller = next(
        d
        for d in docs
        if d["kind"] == "Deployment"
        and d["metadata"]["name"].endswith("-vm-controller")
    )
    env = {
        v["name"]: v.get("value")
        for v in controller["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert {k: v for k, v in config.items() if k.startswith("VM_PREPARATION_")} == {
        k: v for k, v in env.items() if k.startswith("VM_PREPARATION_")
    }
    assert json.loads(config["VM_PREPARATION_IMAGE_PULL_SECRETS"]) == []
    policy = next(
        d
        for d in docs
        if d["kind"] == "NetworkPolicy"
        and d["metadata"]["name"].endswith("-workspace-preparer")
    )
    assert policy["spec"]["podSelector"] == {
        "matchLabels": {"app.kubernetes.io/component": "workspace-preparer"}
    }
    assert policy["spec"]["ingress"] == policy["spec"]["egress"] == []
    role = next(
        d
        for d in docs
        if d["kind"] == "Role" and d["metadata"]["name"].endswith("-vm-controller")
    )
    assert any(
        r["resources"] == ["configmaps"] and "update" in r["verbs"]
        for r in role["rules"]
    )
    assert not any("pods/exec" in r["resources"] for r in role["rules"])


@pytest.mark.parametrize(
    "setting",
    [
        "vm.mode=off",
        "vmController.persistentRootdisk.enabled=false",
        "vm.lifecycleAuthSecretName=",
        "vmController.preparation.maxConcurrent=0",
        "vmController.preparation.timeoutSeconds=3601",
        "vmController.preparation.network.enabled=true",
    ],
)
def test_invalid_hosting_is_rejected_before_deployment(setting):
    assert render(*ENABLED, setting, check=False).returncode != 0


def test_online_preparation_only_allows_dns_public_http_and_explicit_operator_rules():
    docs = render(
        *ENABLED,
        "vmController.preparation.network.enabled=true",
        "vmController.preparation.network.enforcementVerified=true",
    )
    policy = next(
        d
        for d in docs
        if d["kind"] == "NetworkPolicy"
        and d["metadata"]["name"].endswith("-workspace-preparer")
    )
    rules = policy["spec"]["egress"]
    assert {p["port"] for p in rules[0]["ports"]} == {53}
    assert {p["port"] for p in rules[1]["ports"]} == {80, 443}
    assert {"10.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.168.0.0/16"} <= set(
        rules[1]["to"][0]["ipBlock"]["except"]
    )


def test_disabling_admission_keeps_existing_builder_policy_and_cleanup_authority():
    docs = render(*ENABLED, "vmController.preparation.enabled=false")
    policy = next(
        d
        for d in docs
        if d["kind"] == "NetworkPolicy"
        and d["metadata"]["name"].endswith("-workspace-preparer")
    )
    assert policy["spec"]["egress"] == []
    role = next(
        d
        for d in docs
        if d["kind"] == "Role" and d["metadata"]["name"].endswith("-vm-controller")
    )
    assert any(
        r["resources"] == ["pods"] and "delete" in r["verbs"] for r in role["rules"]
    )


def test_recovery_observation_has_node_uid_read_and_read_only_guest_diagnostic():
    docs = render(*ENABLED)
    cluster_role = next(
        d
        for d in docs
        if d["kind"] == "ClusterRole"
        and d["metadata"]["name"].endswith("-vm-controller-node-observer")
    )
    assert cluster_role["rules"] == [
        {"apiGroups": [""], "resources": ["nodes"], "verbs": ["get"]}
    ]
    template = next(
        d["data"]["cloud-init.yaml"]
        for d in docs
        if d["kind"] == "ConfigMap" and "cloud-init.yaml" in d.get("data", {})
    )
    assert "/usr/local/bin/srw-network-qualification" in template
    assert "cloud_init_cache_cleaned': False" in template
    assert "cloud-init clean" not in template


def test_pod_firewall_profile_is_shared_by_admission_and_controller():
    docs = render(*ENABLED, "vmController.preparation.network.podFirewall=true")
    config = next(
        d["data"]
        for d in docs
        if d["kind"] == "ConfigMap" and "VM_PREPARATION_ENABLED" in d.get("data", {})
    )
    controller = next(
        d
        for d in docs
        if d["kind"] == "Deployment"
        and d["metadata"]["name"].endswith("-vm-controller")
    )
    env = {
        v["name"]: v.get("value")
        for v in controller["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert (
        config["VM_PREPARATION_POD_FIREWALL"]
        == env["VM_PREPARATION_POD_FIREWALL"]
        == "true"
    )
    assert config["VM_PREPARATION_BLOCKED_CIDRS"] == env["VM_PREPARATION_BLOCKED_CIDRS"]
    assert (
        config["VM_PREPARATION_ADDITIONAL_EGRESS"]
        == env["VM_PREPARATION_ADDITIONAL_EGRESS"]
        == "[]"
    )
    invalid = render(
        *ENABLED,
        "vmController.preparation.network.podFirewall=true",
        "vmController.preparation.network.additionalEgress[0].ports[0].port=22",
        check=False,
    )
    assert invalid.returncode != 0
    assert "does not support network.additionalEgress" in invalid.stderr
