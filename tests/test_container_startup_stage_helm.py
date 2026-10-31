"""The container startup protocol must ship dark and roll on activation."""

from pathlib import Path
import shutil
import subprocess

import pytest
import yaml

from tests.test_helm_vm_workspace_recovery import _env, _orchestrator
from tests.test_manifest_hosting_helm import render


pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is absent")
ROOT = Path(__file__).resolve().parents[1]
SETTING = "orchestrator.containerStartupStageAuthority.enabled"
ENV = "CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED"
CHECKSUM = "checksum/container-startup-stage-authority"


def test_startup_authority_defaults_on_in_runtime_and_example():
    # On by default since 2026-10-21, with stateless Sessions (their container
    # upgrade needs it); the bridge shipped in v0.0.29.
    for name in ("values.yaml", "values.example.yaml"):
        values = yaml.safe_load((ROOT / "helm" / name).read_text())
        assert values["orchestrator"]["containerStartupStageAuthority"] == {
            "enabled": True
        }
    documents = render()
    assert _env(documents, _orchestrator(documents))[ENV] == "true"


def test_startup_activation_rolls_orchestrator_without_reloader():
    common = ("reloader.enabled=false",)
    off = render(*common, f"{SETTING}=false")
    on = render(*common, f"{SETTING}=true")
    unrelated = render(*common, f"{SETTING}=false", "orchestrator.replicas=2")
    off_deployment, on_deployment = _orchestrator(off), _orchestrator(on)
    assert _env(off, off_deployment)[ENV] == "false"
    assert _env(on, on_deployment)[ENV] == "true"
    off_annotations = off_deployment["spec"]["template"]["metadata"]["annotations"]
    on_annotations = on_deployment["spec"]["template"]["metadata"]["annotations"]
    assert off_annotations[CHECKSUM] != on_annotations[CHECKSUM]
    assert (
        off_annotations[CHECKSUM]
        == _orchestrator(unrelated)["spec"]["template"]["metadata"]["annotations"][
            CHECKSUM
        ]
    )


def test_startup_authority_missing_legacy_parent_defaults_off(tmp_path):
    chart = tmp_path / "helm"
    shutil.copytree(ROOT / "helm", chart)
    values_path = chart / "values.yaml"
    saved = values_path.read_text().replace(
        "  containerStartupStageAuthority:\n    enabled: true\n", ""
    )
    assert "containerStartupStageAuthority" not in yaml.safe_load(saved)["orchestrator"]
    values_path.write_text(saved)
    result = subprocess.run(
        [
            "helm",
            "template",
            "legacy-startup",
            str(chart),
            "-f",
            str(chart / "ci/test-values.yaml"),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    documents = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    assert _env(documents, _orchestrator(documents))[ENV] == "false"


def test_startup_authority_rejects_non_boolean_activation():
    result = render(f"{SETTING}=unsafe", check=False)
    assert result.returncode != 0
    assert "containerStartupStageAuthority.enabled" in result.stderr.replace("/", ".")
    assert "boolean" in result.stderr
