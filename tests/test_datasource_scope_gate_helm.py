"""The connector scope/auto-attach capability ships on, with an opt-out.

Since 0.2.0 the chart turns ``DATASOURCE_SCOPE_AUTO_ATTACH_V1_ENABLED`` on: the
Cockpit then preselects a project's connector defaults in New Session and New
Job (project_connector_defaults_not_preselected_in_create_forms). An operator
keeps the old forms with ``agent.datasourceScopeAutoAttachV1Enabled: "false"``,
as a string or a boolean.
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
KEY = "DATASOURCE_SCOPE_AUTO_ATTACH_V1_ENABLED"
VALUE = "agent.datasourceScopeAutoAttachV1Enabled"


def rendered_gate(*arguments: str) -> str:
    command = [
        "helm",
        "template",
        "srw",
        str(ROOT / "helm"),
        "-n",
        "srw",
        "-f",
        str(ROOT / "helm/ci/test-values.yaml"),
        *arguments,
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    values = [
        doc["data"][KEY]
        for doc in yaml.safe_load_all(result.stdout)
        if doc and doc.get("kind") == "ConfigMap" and KEY in (doc.get("data") or {})
    ]
    assert len(values) == 1, values
    return values[0]


def test_the_chart_turns_the_capability_on_by_default():
    assert rendered_gate() == "true"


@pytest.mark.parametrize("flag", ["--set-string", "--set"])
def test_an_operator_keeps_the_old_forms(flag):
    # A string "false" and a boolean false must both turn it off: Helm's
    # `default` treats a boolean false as empty, so the template must not
    # default to on.
    assert rendered_gate(flag, f"{VALUE}=false") == "false"
