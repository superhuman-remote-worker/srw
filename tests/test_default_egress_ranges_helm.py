"""The chart's default egress `except` ranges name no particular installation.

Workspace tier policies and the opt-in agent egress policy subtract ranges from
their 0.0.0.0/0 allow. The defaults must hold on any cluster: `internet-only`
and the agent policy deny every private, shared, loopback and link-local range;
`home-allowed` gives private addresses back but keeps the k3s pod and service
ranges and metadata denied.
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

NON_PUBLIC = {
    "0.0.0.0/8",
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "172.16.0.0/12",
    "192.168.0.0/16",
}


def _policies(*settings: str) -> dict[str, dict]:
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
    return {
        doc["metadata"]["name"]: doc
        for doc in yaml.safe_load_all(result.stdout)
        if doc and doc["kind"] == "NetworkPolicy"
    }


def _internet_except(policy: dict) -> set[str]:
    blocks = [
        to["ipBlock"]
        for rule in policy["spec"]["egress"]
        for to in rule.get("to", [])
        if to.get("ipBlock", {}).get("cidr") == "0.0.0.0/0"
    ]
    assert len(blocks) == 1
    return set(blocks[0]["except"])


def test_workspace_tiers_default_to_installation_neutral_ranges():
    policies = _policies()
    internet_only = _internet_except(
        policies["srw-superhuman-remote-worker-workspace-policy-internet-only"]
    )
    home_allowed = _internet_except(
        policies["srw-superhuman-remote-worker-workspace-policy-home-allowed"]
    )

    assert internet_only == NON_PUBLIC
    assert home_allowed == {
        "0.0.0.0/8",
        "10.42.0.0/16",
        "10.43.0.0/16",
        "127.0.0.0/8",
        "169.254.0.0/16",
    }


def test_the_agent_policy_denies_every_non_public_range():
    policies = _policies("agent.networkPolicy.enabled=true")
    agent = _internet_except(policies["srw-superhuman-remote-worker-agent-egress"])
    assert agent == NON_PUBLIC
