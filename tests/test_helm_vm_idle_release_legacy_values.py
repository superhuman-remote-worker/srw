"""An old release without new idle settings must still render safely off."""

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from tests.test_helm_vm_workspace_recovery import _env, _orchestrator


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is absent")
def test_idle_release_missing_from_reused_release_values_defaults_off(tmp_path):
    root = Path(__file__).resolve().parents[1]
    chart = tmp_path / "helm"
    shutil.copytree(root / "helm", chart)
    # --reuse-values renders with the saved release values in place of the
    # new chart defaults. Remove the map from those defaults to exercise that
    # actual missing-parent condition without needing a running Helm release.
    values_path = chart / "values.yaml"
    # Preserve other YAML scalar spellings (notably quoted numeric strings).
    saved = values_path.read_text().replace(
        "  vmIdleRelease:\n    enabled: false\n", ""
    )
    assert "vmIdleRelease" not in yaml.safe_load(saved)["orchestrator"]
    values_path.write_text(saved)
    result = subprocess.run(
        [
            "helm",
            "template",
            "legacy-idle",
            str(chart),
            "-f",
            str(chart / "ci/test-values.yaml"),
            "--set",
            "agent.stateless.enabled=true",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    documents = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    assert (
        _env(documents, _orchestrator(documents))["WORKSPACE_IDLE_RELEASE_ENABLED"]
        == "false"
    )
    config = next(
        doc["data"]
        for doc in documents
        if doc["kind"] == "ConfigMap"
        and "WORKSPACE_IDLE_RELEASE_ENABLED" in doc.get("data", {})
    )
    assert config["WORKSPACE_IDLE_RELEASE_ENABLED"] == "false"
