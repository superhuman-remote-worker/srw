"""Helm gate for ``canvas.sharedBrowser``."""

import json
from pathlib import Path

import yaml


REPO = Path(__file__).resolve().parents[1]


def test_shared_browser_helm_gate_is_boolean_and_default_on():
    values = yaml.safe_load((REPO / "helm/values.yaml").read_text())
    assert values["canvas"]["sharedBrowser"]["enabled"] is True

    schema = json.loads((REPO / "helm/values.schema.json").read_text())
    shared = schema["properties"]["canvas"]["properties"]["sharedBrowser"]
    assert shared["properties"]["enabled"]["type"] == "boolean"


def test_shared_browser_env_reaches_orchestrator():
    configmap = (REPO / "helm/templates/configmap.yaml").read_text()
    assert "CANVAS_SHARED_BROWSER_ENABLED" in configmap
    assert ".Values.canvas.sharedBrowser.enabled" in configmap

    deployment = (REPO / "helm/templates/orchestrator/deployment.yaml").read_text()
    assert "CANVAS_SHARED_BROWSER_ENABLED" in deployment


def test_container_and_vm_install_the_same_stream_conformance_program():
    check = REPO / "docker/check-browser-stream.py"
    assert check.read_text().startswith("#!/usr/bin/env python3\n")
    assert not (REPO / "docker/agent-vm-base/files/check-browser-stream.py").exists()

    dockerfile = (REPO / "docker/Dockerfile.workspace").read_text()
    assert (
        "COPY --chmod=0755 docker/check-browser-stream.py /usr/local/bin/check-browser-stream"
        in dockerfile
    )

    packer = (REPO / "docker/agent-vm-base/stage2.pkr.hcl").read_text()
    provision = (REPO / "docker/agent-vm-base/scripts/provision-stage2.sh").read_text()
    assert '"../check-browser-stream.py"' in packer
    assert (
        "install -o root -g root -m 0755 /tmp/check-browser-stream.py "
        "/usr/local/bin/check-browser-stream"
    ) in provision


def test_shared_browser_stack_assertion_runs_live_stream_conformance():
    assertion = (REPO / "docker/assert-browser-stack.sh").read_text()
    expected = (
        '_check "shared-browser stream conformance" \\'
        "\n    /usr/local/bin/check-browser-stream"
    )
    assert expected in assertion
    assert '[ "$label" = "shared-browser stream conformance" ]' in assertion


def _detector_inputs(workflow: str, name: str) -> str:
    """The path list feeding develop.yml's ``<name>`` change detector.

    Two spellings are in play. 99223c6d hoisted some lists into a
    ``<NAME>_PATHS=(...)`` array so the same paths could also drive
    ``last_input_sha``; the rest are still inline in ``<NAME>=$(has_changes ...)``.
    Prefer the array — where one exists the inline form only references it, so
    reading the inline form would find no paths at all. Neither list nests
    parens, so the first ``)`` closes it.
    """
    for opener in (f"{name}_PATHS=(", f"{name}=$(has_changes"):
        _, sep, rest = workflow.partition(opener)
        if sep:
            return rest.split(")", 1)[0]
    raise AssertionError(f"no change detector found for {name} in develop.yml")


def test_stream_conformance_changes_rebuild_every_workspace_image():
    workflow = (REPO / ".github/workflows/develop.yml").read_text()
    assert "docker/check-browser-stream.py" in _detector_inputs(workflow, "WORKSPACE")
    assert "docker/check-browser-stream.py" in _detector_inputs(workflow, "VM_BASE")

    tilt = (REPO / "Tiltfile").read_text()
    workspace_build = tilt.split("docker_build(\n    'srw-workspace'", 1)[1].split(
        "# -----------------------------------------------------------------------------",
        1,
    )[0]
    assert "'docker/check-browser-stream.py'" in workspace_build
