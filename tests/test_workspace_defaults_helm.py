"""workspace.defaults renders WORKSPACE_DEFAULTS (Slice A2b)."""

import json
import shutil

import pytest

from shared.workspace_defaults import InstallationDefaults, installation_defaults
from tests.test_manifest_hosting_helm import render
from tests.test_workspace_builtin_templates_helm import environment

pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is absent")


def defaults(*settings) -> dict:
    return json.loads(environment(*settings)["WORKSPACE_DEFAULTS"])


def test_the_default_render_keeps_todays_behaviour():
    assert defaults() == {
        "jobs": "container",
        "sessions": "virtual",
        "container": "",
        "vm": "",
    }
    parsed = installation_defaults({"WORKSPACE_DEFAULTS": json.dumps(defaults())})
    assert parsed == InstallationDefaults()


def test_operators_can_point_every_container_at_their_template():
    rendered = defaults(
        "workspace.defaults.container=company-image", "workspace.defaults.jobs=vm"
    )
    assert rendered["container"] == "company-image"
    assert rendered["jobs"] == "vm"


@pytest.mark.parametrize(
    "setting",
    [
        "workspace.defaults.jobs=sandbox",
        "workspace.defaults.sessions=big",
        "workspace.defaults.container=Not_A_Name",
    ],
)
def test_invalid_values_fail_the_render(setting):
    assert render(setting, check=False).returncode != 0
