"""The chart declares the built-in workspace templates and trusts minimal."""

import json
import shutil
import subprocess

import pytest

from orchestrator.services.manifest_workspace_selection import srw_workspace_config
from shared.manifests import preview_documents
from tests.test_helm_vm_workspace_recovery import _env, _orchestrator
from tests.test_manifest_hosting_helm import render

pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is absent")

SCOPE = {"kind": "Catalog", "name": "shared"}
MINIMAL = "ghcr.io/superhuman-remote-worker/srw-workspace-minimal"
DIGEST = "sha256:" + "a" * 64
# helm/ci/test-values.yaml models an external VM cluster. The chart refuses the
# other two modes unless the settings that belong to that model are cleared.
VM_OFF = (
    "vm.mode=off",
    "orchestratorId=",
    "headscale.enabled=false",
    "headscale.url=",
    "agent.tailscale.enabled=false",
)
VM_SAME_CLUSTER = (
    "vm.mode=same-cluster",
    "vm.lifecycleAuthSecretName=srw-lifecycle-auth",
    "agent.tailscale.enabled=false",
)


def environment(*settings) -> dict[str, str]:
    documents = render(*settings)
    return _env(documents, _orchestrator(documents))


def builtins(*settings) -> dict[str, dict]:
    declared = json.loads(environment(*settings)["WORKSPACE_BUILTIN_TEMPLATES"])
    return {document["metadata"]["name"]: document for document in declared}


def test_the_default_render_declares_four_builtins():
    declared = builtins()
    assert list(declared) == [
        "virtual",
        "container-minimal",
        "container-full",
        "vm-full",
    ]
    for document in declared.values():
        assert document["apiVersion"] == "srw/v1alpha1"
        assert document["kind"] == "WorkspaceTemplate"
        assert document["metadata"]["scope"] == SCOPE
        annotations = document["metadata"]["annotations"]
        assert annotations["srw.io/display-name"]
        assert annotations["srw.io/description"]


def test_vm_full_needs_the_vm_tier():
    assert list(builtins(*VM_OFF)) == [
        "virtual",
        "container-minimal",
        "container-full",
    ]


def test_the_switch_turns_the_whole_set_off():
    env = environment("workspace.builtinTemplates.enabled=false")
    assert env["WORKSPACE_BUILTIN_TEMPLATES"] == "[]"


def test_specs_carry_a_backend_an_image_and_sizes():
    env = environment()
    declared = builtins()
    sizes = {
        "cpu": 2,
        "memory": "4Gi",
        "requests": {"cpu": 0.5, "memory": "1Gi"},
    }
    assert declared["virtual"]["spec"] == {"backend": "virtual"}
    assert declared["container-full"]["spec"] == {
        "backend": "sandbox",
        "environment": {"image": env["WORKSPACE_IMAGE"], "pullPolicy": "Always"},
        "resources": sizes,
    }
    assert declared["container-minimal"]["spec"] == {
        "backend": "sandbox",
        "environment": {"image": MINIMAL + ":latest", "pullPolicy": "Always"},
        "resources": sizes,
    }
    assert declared["vm-full"]["spec"]["backend"] == "vm"
    assert declared["vm-full"]["spec"]["resources"] == {
        "cpu": 8,
        "memory": "16Gi",
        "storage": "30Gi",
    }


def test_container_full_follows_the_installation_image_by_digest():
    settings = (f"image.workspace.digest={DIGEST}",)
    image = builtins(*settings)["container-full"]["spec"]["environment"]["image"]
    assert image == environment(*settings)["WORKSPACE_IMAGE"]
    assert image.endswith("@" + DIGEST)


def test_container_sizes_come_from_values():
    declared = builtins(
        "workspace.builtinTemplates.containerResources.cpu=4",
        "workspace.builtinTemplates.containerResources.memory=8Gi",
        "workspace.builtinTemplates.containerResources.requests.cpu=1",
        "workspace.builtinTemplates.containerResources.requests.memory=2Gi",
    )
    for name in ("container-minimal", "container-full"):
        assert declared[name]["spec"]["resources"] == {
            "cpu": 4,
            "memory": "8Gi",
            "requests": {"cpu": 1, "memory": "2Gi"},
        }


def test_vm_sizes_come_from_values_and_not_from_the_controller():
    declared = builtins(
        "workspace.builtinTemplates.vmResources.cpu=12",
        "workspace.builtinTemplates.vmResources.memory=24Gi",
        "workspace.builtinTemplates.vmResources.storage=120Gi",
        "vmController.defaultCpu=2",
        "vmController.defaultMemory=4Gi",
        "vmController.vmDiskSize=50Gi",
    )
    assert declared["vm-full"]["spec"]["resources"] == {
        "cpu": 12,
        "memory": "24Gi",
        "storage": "120Gi",
    }


def test_vm_full_names_the_vm_controllers_image():
    image = "registry.example/vm-base@" + DIGEST
    documents = render(*VM_SAME_CLUSTER, f"vmController.defaultVmImage={image}")
    env = _env(documents, _orchestrator(documents))
    declared = json.loads(env["WORKSPACE_BUILTIN_TEMPLATES"])
    vm_full = next(d for d in declared if d["metadata"]["name"] == "vm-full")
    assert vm_full["spec"]["environment"]["image"] == image
    controller = next(
        document
        for document in documents
        if document.get("kind") == "Deployment"
        and document["metadata"]["name"].endswith("-vm-controller")
    )
    controller_env = {
        entry["name"]: entry.get("value")
        for entry in controller["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert controller_env["DEFAULT_VM_IMAGE"] == image


def test_every_builtin_passes_the_applications_own_validation():
    for name, document in builtins().items():
        resolved = preview_documents([document])["resolved"][0]["spec"]
        rendered = srw_workspace_config({"template": {"inline": resolved}})
        assert rendered["backend"] == document["spec"]["backend"], name


def test_minimal_is_always_a_trusted_repository():
    for settings in ((), ("workspace.builtinTemplates.enabled=false",)):
        trusted = json.loads(
            environment(*settings)["WORKSPACE_TRUSTED_IMAGE_REPOSITORIES"]
        )
        assert trusted == [MINIMAL + ":latest"]


def test_operator_repositories_are_kept_in_front_of_minimal():
    trusted = json.loads(
        environment("workspace.images.trustedRepositories[0]=ghcr.io/org/custom")[
            "WORKSPACE_TRUSTED_IMAGE_REPOSITORIES"
        ]
    )
    assert trusted == ["ghcr.io/org/custom", MINIMAL + ":latest"]


def test_the_new_variable_is_the_last_orchestrator_variable():
    documents = render()
    entries = _orchestrator(documents)["spec"]["template"]["spec"]["containers"][0][
        "env"
    ]
    # WORKSPACE_DEFAULTS (Slice A2b, Task 8) is appended after
    # WORKSPACE_BUILTIN_TEMPLATES, which is itself appended-at-the-end (see
    # helm/templates/orchestrator/deployment.yaml); appending, never
    # inserting, avoids the Kubernetes strategic-merge `env[N].valueFrom`
    # patch bug.
    assert entries[-2]["name"] == "WORKSPACE_BUILTIN_TEMPLATES"
    assert entries[-1]["name"] == "WORKSPACE_DEFAULTS"


def test_container_builtins_carry_the_installation_pull_policy():
    declared = builtins()
    assert declared["container-full"]["spec"]["environment"]["pullPolicy"] == "Always"
    assert (
        declared["container-minimal"]["spec"]["environment"]["pullPolicy"] == "Always"
    )
    assert "pullPolicy" not in declared["vm-full"]["spec"].get("environment", {})


def test_container_builtins_follow_an_overridden_pull_policy():
    declared = builtins("image.workspace.pullPolicy=IfNotPresent")
    assert (
        declared["container-full"]["spec"]["environment"]["pullPolicy"]
        == "IfNotPresent"
    )
    # workspaceMinimal has no pullPolicy of its own; it falls back to
    # image.workspace.pullPolicy.
    assert (
        declared["container-minimal"]["spec"]["environment"]["pullPolicy"]
        == "IfNotPresent"
    )


@pytest.mark.parametrize(
    "setting",
    [
        "workspace.builtinTemplates.enabled=maybe",
        "workspace.builtinTemplates.containerResources.memory=lots",
        "workspace.builtinTemplates.containerResources.cpu=0",
        "workspace.builtinTemplates.containerResources.disk=10Gi",
        "workspace.builtinTemplates.vmResources.cpu=2.5",
        "workspace.builtinTemplates.vmResources.cpu=0",
        "workspace.builtinTemplates.vmResources.storage=big",
        "workspace.builtinTemplates.vmResources.requests.cpu=1",
        "image.workspaceMinimal.digest=latest",
    ],
)
def test_the_schema_refuses_malformed_values(setting):
    with pytest.raises(subprocess.CalledProcessError):
        render(setting)
