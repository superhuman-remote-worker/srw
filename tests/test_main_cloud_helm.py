"""The main cloud is configured by Helm only (main_cloud_as_connectors.md,
slice 2): the chart renders the provider into the orchestrator's environment,
and the one operator decision with installation-authority consequences,
replacing the active installation with a different one, is a chart value too.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from orchestrator.application.settings import DeploymentSettings

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"
INSTANCE = "75e453dc-78a1-4f4a-8d7e-312b098ce4ad"


def _config(*extra: str) -> dict[str, str]:
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")
    rendered = subprocess.run(
        [
            "helm",
            "template",
            "main-cloud-proof",
            str(CHART),
            "-f",
            str(CHART / "ci" / "test-values.yaml"),
            *extra,
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    for document in yaml.safe_load_all(rendered):
        if (
            isinstance(document, dict)
            and document.get("kind") == "ConfigMap"
            and document["metadata"]["name"].endswith("-config")
            and "MAIN_CLOUD_BACKEND" in (document.get("data") or {})
        ):
            return document["data"]
    pytest.fail("no orchestrator ConfigMap carries MAIN_CLOUD_BACKEND")


@pytest.mark.parametrize(
    ("values", "backend"),
    [
        (
            ("--set", "opencloud.enabled=true", "--set", "nextcloud.enabled=false"),
            "opencloud",
        ),
        (
            ("--set", "opencloud.enabled=false", "--set", "nextcloud.enabled=true"),
            "nextcloud",
        ),
        (
            (
                "--set",
                "opencloud.enabled=false",
                "--set",
                "nextcloud.enabled=false",
                "--set",
                "cloud.externalBackend=nextcloud",
                "--set",
                "cloud.externalUrl=https://cloud.example",
            ),
            "nextcloud",
        ),
    ],
)
def test_the_chart_chooses_the_provider(values, backend):
    assert _config(*values)["MAIN_CLOUD_BACKEND"] == backend


def test_no_replacement_is_confirmed_by_default():
    config = _config(
        "--set", "opencloud.enabled=false", "--set", "nextcloud.enabled=true"
    )
    assert "MAIN_CLOUD_REPLACE_INSTALLATION" not in config


def test_the_operator_confirms_a_replacement_in_values():
    config = _config(
        "--set",
        "opencloud.enabled=false",
        "--set",
        "nextcloud.enabled=true",
        "--set",
        f"cloud.replaceInstallation={INSTANCE}",
    )
    assert config["MAIN_CLOUD_REPLACE_INSTALLATION"] == INSTANCE


def test_the_orchestrator_reads_the_confirmation(monkeypatch):
    monkeypatch.setenv("MAIN_CLOUD_REPLACE_INSTALLATION", f" {INSTANCE} ")
    assert DeploymentSettings.from_environment().main_cloud_replace_installation == (
        INSTANCE
    )
    monkeypatch.delenv("MAIN_CLOUD_REPLACE_INSTALLATION")
    assert DeploymentSettings.from_environment().main_cloud_replace_installation == ""
