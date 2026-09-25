"""Container WorkspaceTemplates render image, pull policy and resources."""

from fastapi import HTTPException
import pytest

from orchestrator.services.config_resolver import resolve_config
from orchestrator.services.manifest_workspace_selection import srw_workspace_config
from shared.manifests import preview_documents

IMAGE = "registry.example/team/workspace@sha256:" + "b" * 64


def resolved(spec: dict) -> dict:
    document = {
        "apiVersion": "srw/v1alpha1",
        "kind": "WorkspaceTemplate",
        "metadata": {"name": "container"},
        "spec": spec,
    }
    return preview_documents(
        [document], default_scope={"kind": "Catalog", "name": "shared"}
    )["resolved"][0]["spec"]


def render(spec: dict) -> dict:
    return srw_workspace_config({"template": {"inline": resolved(spec)}})


def test_backend_only_sandbox_template_is_unchanged():
    assert render({"backend": "sandbox"}) == {"backend": "sandbox"}


def test_sandbox_template_renders_image_pull_policy_and_resources():
    assert render(
        {
            "backend": "sandbox",
            "resources": {"cpu": 0.5, "memory": "3Gi", "storage": "15Gi"},
            "environment": {"image": IMAGE, "pullPolicy": "Always"},
        }
    ) == {
        "backend": "sandbox",
        "sandbox": {
            "image": IMAGE,
            "pull_policy": "Always",
            "cpu": 0.5,
            "memory": "3Gi",
            "storage": "15Gi",
        },
    }


def test_resolution_default_pull_policy_travels_with_the_image():
    assert render({"backend": "sandbox", "environment": {"image": IMAGE}})[
        "sandbox"
    ] == {"image": IMAGE, "pull_policy": "IfNotPresent"}


def test_resources_without_an_image_keep_the_installation_image():
    assert render({"backend": "sandbox", "resources": {"storage": "40Gi"}}) == {
        "backend": "sandbox",
        "sandbox": {"storage": "40Gi"},
    }


def test_empty_prepare_is_a_no_op():
    rendered = render(
        {"backend": "sandbox", "environment": {"image": IMAGE, "prepare": []}}
    )
    assert rendered["sandbox"]["image"] == IMAGE


@pytest.mark.parametrize(
    "environment",
    [
        {"image": IMAGE, "prepare": [{"command": ["apt-get", "install", "-y", "g++"]}]},
        {"image": IMAGE, "cache": "Rebuild"},
    ],
)
def test_container_prepare_is_refused_with_the_alternative(environment):
    with pytest.raises(HTTPException) as denied:
        render({"backend": "sandbox", "environment": environment})
    assert denied.value.status_code == 422
    assert "Container templates can't run prepare steps." in denied.value.detail
    assert "Build your own image FROM an SRW base image" in denied.value.detail


def test_container_initialize_is_refused_with_the_alternative():
    with pytest.raises(HTTPException) as denied:
        render(
            {
                "backend": "sandbox",
                "initialize": [{"command": ["mkdir", "-p", "project"]}],
            }
        )
    assert denied.value.status_code == 422
    assert "bake setup into the image" in denied.value.detail


def test_malformed_container_image_is_refused():
    with pytest.raises(HTTPException) as denied:
        srw_workspace_config(
            {
                "template": {
                    "inline": {
                        "backend": "sandbox",
                        "environment": {"image": "bad\nmanifest: injected"},
                    }
                }
            }
        )
    assert denied.value.status_code == 422


@pytest.mark.parametrize("role", ["worker", "session"])
def test_expert_private_config_never_sizes_or_images_a_container(role):
    row = {
        "config": {
            "workspace": {
                "sandbox": {"image": "attacker.example/image:1", "cpu": 64},
                "container": {"image": "attacker.example/image:1"},
            }
        }
    }
    blob = resolve_config(
        base_config_name=f"{role}_base",
        expert_type=role,
        expert_row=row,
        request_override={"workspace": {"backend": "sandbox"}},
    )
    assert "sandbox" not in blob["agent"]["workspace"]


@pytest.mark.parametrize("role", ["worker", "session"])
def test_selected_sandbox_settings_survive_binding(role):
    # blob["agent"]["workspace"] only carries WorkspaceConfig's declared
    # fields (backend/remote/mounts/...); vm/sandbox never reach it, same as
    # today's vm behaviour. The captured policy path -- what admission freezes
    # and the provisioner reads -- is capture["merged_fragment"]["workspace"].
    sandbox = {"image": IMAGE, "cpu": 2}
    capture: dict = {}
    resolve_config(
        base_config_name=f"{role}_base",
        expert_type=role,
        expert_row={"config": {}},
        request_override={"workspace": {"backend": "sandbox", "sandbox": sandbox}},
        capture=capture,
    )
    assert capture["merged_fragment"]["workspace"]["sandbox"] == sandbox
