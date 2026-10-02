"""Pool startup settings must converge without an optional Reloader."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[1] / "helm"
POOL_FIELDS = {
    "minAgents": ("MIN_AGENTS", "1", "0"),
    "maxAgents": ("MAX_AGENTS", "4", "5"),
    "buffer": ("AGENT_BUFFER", "0", "1"),
    "reservedSessionSlots": ("RESERVED_SESSION_SLOTS", "0", "1"),
    "reservedJobSlots": ("RESERVED_JOB_SLOTS", "0", "1"),
}


def render_pool(*, changed=None, numeric_zero=False):
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")
    command = [
        "helm",
        "template",
        "pool-rollout-proof",
        str(CHART),
        "-f",
        str(CHART / "ci/test-values.yaml"),
        "--set",
        "reloader.enabled=false",
    ]
    for key, (_, original, replacement) in POOL_FIELDS.items():
        value = replacement if key == changed else original
        flag = "--set" if numeric_zero and key == "minAgents" else "--set-string"
        command.extend([flag, f"agent.pool.{key}={value}"])
    output = subprocess.run(
        command, check=True, capture_output=True, text=True, timeout=30
    ).stdout
    documents = [doc for doc in yaml.safe_load_all(output) if doc]
    config = next(
        doc
        for doc in documents
        if doc.get("kind") == "ConfigMap" and "MIN_AGENTS" in doc.get("data", {})
    )
    deployment = next(
        doc
        for doc in documents
        if doc.get("kind") == "Deployment"
        and any(
            c["name"] == "orchestrator"
            for c in doc["spec"]["template"]["spec"]["containers"]
        )
    )
    return config, deployment


@pytest.mark.parametrize("setting", POOL_FIELDS)
def test_changed_pool_setting_replaces_startup_snapshot_without_reloader(setting):
    before, old_deployment = render_pool()
    after, new_deployment = render_pool(changed=setting)
    environment_key, old_value, new_value = POOL_FIELDS[setting]
    assert before["data"][environment_key] == old_value
    assert after["data"][environment_key] == new_value
    container = next(
        c
        for c in new_deployment["spec"]["template"]["spec"]["containers"]
        if c["name"] == "orchestrator"
    )
    env = {entry["name"]: entry for entry in container["env"]}
    assert env[environment_key]["valueFrom"]["configMapKeyRef"] == {
        "name": after["metadata"]["name"],
        "key": environment_key,
    }
    assert "reloader.stakater.com/auto" not in new_deployment["metadata"].get(
        "annotations", {}
    )
    assert old_deployment["spec"]["template"] != new_deployment["spec"]["template"], (
        f"ConfigMap changed {environment_key}, but the Pod template left its "
        "startup settings running unchanged"
    )


def test_identical_pool_settings_have_a_stable_pod_template():
    _, first = render_pool()
    _, second = render_pool()
    assert first["spec"]["template"] == second["spec"]["template"]


def test_zero_string_and_number_have_the_same_config_and_rollout_identity():
    strings, string_deployment = render_pool(changed="minAgents")
    numbers, number_deployment = render_pool(changed="minAgents", numeric_zero=True)
    assert strings["data"]["MIN_AGENTS"] == numbers["data"]["MIN_AGENTS"] == "0"
    assert (
        string_deployment["spec"]["template"] == number_deployment["spec"]["template"]
    )
