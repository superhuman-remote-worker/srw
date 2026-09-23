"""The A1 adapter is default-off and gains read-only PVC/PV evidence only."""

from __future__ import annotations

from pathlib import Path
import shutil
import subprocess

import pytest
import yaml


pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is absent")
ROOT = Path(__file__).resolve().parents[1]


def render(*settings: str, check: bool = True):
    command = ["helm", "template", "srw", str(ROOT / "helm"), "-n", "srw",
               "-f", str(ROOT / "helm/ci/test-values.yaml")]
    for setting in settings:
        command.extend(["--set", setting])
    result = subprocess.run(command, capture_output=True, text=True, check=check)
    return result if not check else [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def enabled_settings() -> tuple[str, ...]:
    return (
        "vm.mode=same-cluster", "vm.lifecycleAuthSecretName=srw-lifecycle-auth",
        "agent.tailscale.enabled=false",
        "orchestrator.vmProvisioning.creationRetryEnabled=true",
        "orchestrator.vmRemoteOperationProtocol.enabled=true",
        "vmController.persistentRootdisk.enabled=true",
        "vmController.networkProfile.enabled=true",
        "vmController.networkProfile.imageAllowlist[0]=registry.example/srw@sha256:" + "a" * 64,
        "orchestrator.vmRetainedResumeAcceptanceGate.enabled=true",
    )


def test_a1_gate_default_off_and_explicit_read_only_adapter() -> None:
    defaults = render()
    assert not any(
        doc.get("kind") == "ConfigMap"
        and doc.get("metadata", {}).get("labels", {}).get("srw.io/vm-retained-resume-gate-adapter")
        for doc in defaults
    )
    orchestrator = next(doc for doc in defaults if doc.get("kind") == "Deployment"
                        and doc["metadata"]["name"].endswith("-orchestrator"))
    env = orchestrator["spec"]["template"]["spec"]["containers"][0]["env"]
    assert next(x for x in env if x["name"] == "VM_RETAINED_RESUME_ACCEPTANCE_GATE_ENABLED")["value"] == "false"

    documents = render(*enabled_settings())
    adapter = next(doc for doc in documents if doc.get("kind") == "ConfigMap"
                   and doc.get("metadata", {}).get("labels", {}).get("srw.io/vm-retained-resume-gate-adapter"))
    assert adapter["data"]["protocolVersion"] == "1"
    assert yaml.safe_load(adapter["data"]["commandJson"]) == [
        "python", "-m", "orchestrator.operator_cli.vm_retained_resume_acceptance",
    ]
    gate_roles = [doc for doc in documents if doc.get("kind") in {"Role", "ClusterRole"}
                  and "vm-retained-resume" in doc["metadata"]["name"]]
    assert gate_roles
    assert all(set(rule["verbs"]) == {"get"} for doc in gate_roles for rule in doc["rules"])
    assert not any("resourcequotas" in rule["resources"] for doc in gate_roles for rule in doc["rules"])


def test_a1_gate_rejects_missing_profile_or_durable_creation() -> None:
    for missing in ("vmController.networkProfile.enabled=false",
                    "orchestrator.vmProvisioning.creationRetryEnabled=false"):
        result = render(*enabled_settings(), missing, check=False)
        assert result.returncode != 0
        assert "vmRetainedResumeAcceptanceGate requires" in result.stderr


def test_a1_gate_absent_map_renders_disabled_for_reused_values() -> None:
    command = [
        "helm", "template", "srw", str(ROOT / "helm"), "-n", "srw",
        "-f", str(ROOT / "helm/ci/test-values.yaml"),
        "--set-json", "orchestrator.vmRetainedResumeAcceptanceGate=null",
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    documents = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    assert not any(doc.get("kind") == "ConfigMap" and
                   doc.get("metadata", {}).get("labels", {}).get(
                       "srw.io/vm-retained-resume-gate-adapter") for doc in documents)
    deployment = next(doc for doc in documents if doc.get("kind") == "Deployment"
                      and doc["metadata"]["name"].endswith("-orchestrator"))
    env = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    assert next(x for x in env if x["name"] ==
                "VM_RETAINED_RESUME_ACCEPTANCE_GATE_ENABLED")["value"] == "false"
