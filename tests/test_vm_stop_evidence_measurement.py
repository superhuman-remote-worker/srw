"""Real retained-launcher observations before and after VM Halted in local k3d."""

import json
from pathlib import Path

from vm_controller.controller import _exact_terminal_container_evidence


def measurement():
    return json.loads(
        (
            Path(__file__).parent
            / "fixtures/vm_stop_evidence/kubevirt-1.6.6-native-sidecar.json"
        ).read_text()
    )["observations"]


def test_retained_manual_launcher_has_current_native_sidecar_stop_evidence():
    manual, _ = measurement()
    assert manual["strategy"] == "Manual"
    pod = manual["pod"]
    assert pod["spec"]["initContainers"] == [
        {"name": "guest-console-log", "restartPolicy": "Always"}
    ]
    assert "deletionGracePeriodSeconds" not in pod["metadata"]
    evidence = _exact_terminal_container_evidence(pod)
    assert evidence is not None
    assert evidence["declared_containers"] == {
        "init": ["guest-console-log"],
        "regular": ["compute"],
    }
    assert len(evidence["containers"]) == 2
    assert all(item["restart_count"] == 0 for item in evidence["containers"])


def test_halted_terminal_launcher_cannot_supply_fresh_stop_proof_after_grace_zero():
    _, halted = measurement()
    assert halted["strategy"] == "Halted"
    pod = halted["pod"]
    assert pod["status"]["phase"] == "Succeeded"
    assert pod["metadata"]["deletionGracePeriodSeconds"] == 0
    assert _exact_terminal_container_evidence(pod) is None
