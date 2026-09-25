"""Rendered contracts for bundled off-pod web research services."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


def _render(*settings: str) -> list[dict]:
    command = [
        "helm",
        "template",
        "search-provider-test",
        str(CHART),
        "-f",
        str(CHART / "ci/test-values.yaml"),
    ]
    for setting in settings:
        command.extend(["--set", setting])
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"helm template failed:\n{result.stderr}"
    return [document for document in yaml.safe_load_all(result.stdout) if document]


def _components(objects: list[dict], component: str) -> list[dict]:
    return [
        item
        for item in objects
        if item.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component")
        == component
    ]


def _one(objects: list[dict], component: str, kind: str) -> dict:
    matches = [item for item in _components(objects, component) if item["kind"] == kind]
    assert len(matches) == 1
    return matches[0]


def _assert_confined_public_http_egress(policy: dict) -> None:
    assert policy["spec"]["policyTypes"] == ["Egress"]
    rules = policy["spec"]["egress"]
    dns_rules = [
        rule
        for rule in rules
        if {(port["protocol"], port["port"]) for port in rule.get("ports", [])}
        == {("UDP", 53), ("TCP", 53)}
    ]
    assert len(dns_rules) == 1
    dns_peer = dns_rules[0]["to"][0]
    assert dns_peer["namespaceSelector"]["matchLabels"] == {
        "kubernetes.io/metadata.name": "kube-system"
    }
    assert dns_peer["podSelector"]["matchLabels"] == {"k8s-app": "kube-dns"}

    internet_rules = [
        rule
        for rule in rules
        if rule.get("to", [{}])[0].get("ipBlock", {}).get("cidr") == "0.0.0.0/0"
    ]
    assert len(internet_rules) == 1
    assert {
        (port["protocol"], port["port"]) for port in internet_rules[0]["ports"]
    } == {
        ("TCP", 80),
        ("TCP", 443),
    }
    blocked = set(internet_rules[0]["to"][0]["ipBlock"]["except"])
    assert {
        "0.0.0.0/8",
        "10.0.0.0/8",
        "100.64.0.0/10",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.168.0.0/16",
    } <= blocked


def test_default_enables_searxng_and_disables_crawl4ai() -> None:
    objects = _render()

    assert {item["kind"] for item in _components(objects, "searxng")} == {
        "ConfigMap",
        "Secret",
        "Deployment",
        "Service",
        "NetworkPolicy",
    }
    assert _components(objects, "crawl4ai") == []
    _assert_confined_public_http_egress(_one(objects, "searxng", "NetworkPolicy"))

    seed = _one(objects, "research-provider-seed", "Job")
    container = seed["spec"]["template"]["spec"]["containers"][0]
    assert container["args"] == ["--research-providers-only"]
    env = container["env"]
    assert "TAVILY_API_KEY" in {item["name"] for item in env}
    searxng_url = next(item for item in env if item["name"] == "SEARXNG_BASE_URL")
    assert searxng_url["value"].endswith("-searxng:8080")
    # No component, no credential and no catalog row: the seeder must not be
    # told about a Crawl4AI this release never deployed.
    assert not {item["name"] for item in env} & {
        "CRAWL4AI_BASE_URL",
        "CRAWL4AI_API_TOKEN",
    }


def test_searxng_settings_keep_upstream_engines_by_default() -> None:
    settings = yaml.safe_load(
        _one(_render(), "searxng", "ConfigMap")["data"]["settings.yml"]
    )

    assert settings["use_default_settings"] is True
    assert "engines" not in settings


def test_searxng_engine_overrides_render_into_settings() -> None:
    objects = _render(
        "searxng.engines[0].name=bing",
        "searxng.engines[0].disabled=false",
        "searxng.engines[1].name=duckduckgo",
        "searxng.engines[1].disabled=true",
    )
    settings = yaml.safe_load(
        _one(objects, "searxng", "ConfigMap")["data"]["settings.yml"]
    )

    # SearXNG merges these by name into its upstream list, so the rendered
    # list carries only the overrides, never a copy of the defaults.
    assert settings["use_default_settings"] is True
    assert settings["engines"] == [
        {"name": "bing", "disabled": False},
        {"name": "duckduckgo", "disabled": True},
    ]
    assert settings["server"]["limiter"] is False


def test_flags_disable_searxng_and_enable_confined_crawl4ai() -> None:
    objects = _render("searxng.enabled=false", "crawl4ai.enabled=true")

    assert _components(objects, "searxng") == []
    assert {item["kind"] for item in _components(objects, "crawl4ai")} == {
        "ConfigMap",
        "Deployment",
        "Service",
        "NetworkPolicy",
    }
    _assert_confined_public_http_egress(_one(objects, "crawl4ai", "NetworkPolicy"))

    deployment = _one(objects, "crawl4ai", "Deployment")
    pod_spec = deployment["spec"]["template"]["spec"]
    assert pod_spec["automountServiceAccountToken"] is False
    container = pod_spec["containers"][0]
    assert container["resources"]["limits"]["memory"] == "4Gi"
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    token = next(
        item for item in container["env"] if item["name"] == "CRAWL4AI_API_TOKEN"
    )
    assert token["valueFrom"]["secretKeyRef"]["key"] == "CRAWL4AI_API_TOKEN"
    signing_key = next(
        item for item in container["env"] if item["name"] == "SECRET_KEY"
    )
    assert (
        signing_key["valueFrom"]["secretKeyRef"] == token["valueFrom"]["secretKeyRef"]
    )

    config = _one(objects, "crawl4ai", "ConfigMap")["data"]["config.yml"]
    assert "jwt_enabled: true" in config

    seed = _one(objects, "research-provider-seed", "Job")
    env = seed["spec"]["template"]["spec"]["containers"][0]["env"]
    assert "SEARXNG_BASE_URL" not in {item["name"] for item in env}
    # Deploying the component must also register it, or the fetch slot stays
    # empty and the pod is 4 GiB of unreachable service.
    crawl_url = next(item for item in env if item["name"] == "CRAWL4AI_BASE_URL")
    assert crawl_url["value"].endswith("-crawl4ai:11235")
    seed_token = next(item for item in env if item["name"] == "CRAWL4AI_API_TOKEN")
    assert seed_token["valueFrom"]["secretKeyRef"]["key"] == "CRAWL4AI_API_TOKEN"
    # Optional on purpose: a missing key degrades to an unregistered provider
    # instead of failing the hook and wedging the release.
    assert seed_token["valueFrom"]["secretKeyRef"]["optional"] is True


def test_research_seed_hook_does_not_render_while_orchestrator_is_quiesced() -> None:
    objects = _render("orchestrator.replicas=0")
    assert _components(objects, "research-provider-seed") == []


def test_agent_egress_policy_admits_only_fixed_provider_pods() -> None:
    objects = _render(
        "agent.networkPolicy.enabled=true",
        "searxng.enabled=true",
        "crawl4ai.enabled=true",
    )
    agent_policy = next(
        item
        for item in objects
        if item.get("kind") == "NetworkPolicy"
        and item.get("metadata", {}).get("name", "").endswith("-agent-egress")
    )

    fixed_destinations: dict[str, set[int]] = {}
    for rule in agent_policy["spec"]["egress"]:
        peers = rule.get("to", [])
        if len(peers) != 1:
            continue
        labels = peers[0].get("podSelector", {}).get("matchLabels", {})
        component = labels.get("app.kubernetes.io/component")
        if component in {"searxng", "crawl4ai"}:
            fixed_destinations[component] = {
                port["port"] for port in rule.get("ports", [])
            }

    assert fixed_destinations == {"searxng": {8080}, "crawl4ai": {11235}}


def _research_seed_wait(objects: list[dict]) -> tuple[int, int]:
    """(attempt cap in the init-container wait loop, Job activeDeadlineSeconds)."""
    seed = _one(objects, "research-provider-seed", "Job")
    init = seed["spec"]["template"]["spec"]["initContainers"][0]
    script = init["command"][-1]
    import re

    match = re.search(r'\[ "\$i" -ge (\d+) \]', script)
    assert match, script
    return int(match.group(1)), int(seed["spec"]["activeDeadlineSeconds"])


def test_research_seed_hook_wait_defaults_to_five_minutes() -> None:
    attempts, deadline = _research_seed_wait(_render())
    assert attempts == 100  # 300 s / 3 s poll
    assert deadline == 600  # wait + 300 s for the seed itself


def test_research_seed_hook_wait_is_configurable_for_cold_local_installs() -> None:
    """A fresh k3d install pulls every image and walks Keycloak → Gitea →
    orchestrator before the hook can succeed; 300 s is not enough (probe 1,
    2026-09-04). The wait and the Job deadline must scale together."""
    attempts, deadline = _research_seed_wait(
        _render("llm.seed.researchProviderWaitSeconds=900")
    )
    assert attempts == 300
    assert deadline == 1200
