"""Project defaults.workspace: shorthand, null and the object (Slice A2b)."""

import pytest

from shared.manifests.errors import ManifestError
from shared.manifests.validation import validate_documents
from shared.manifests.resolution import preview_documents
from shared.manifests.workspace_defaults import project_workspace_defaults

SANDBOX = {
    "inline": {"backend": "sandbox", "environment": {"image": "r.example/site:1"}}
}
VM = {"inline": {"backend": "vm"}}
VIRTUAL = {"inline": {"backend": "virtual"}}


def spec(workspace):
    return {
        "resources": {"workspaces": {"site": SANDBOX, "box": VM, "lite": VIRTUAL}},
        "defaults": {} if workspace is ... else {"workspace": workspace},
    }


def project(workspace):
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "Project",
        "metadata": {
            "name": "website",
            "scope": {"kind": "Account", "name": "personal"},
        },
        "spec": {
            "resources": {"workspaces": {"site": SANDBOX, "box": VM}},
            "defaults": {"workspace": workspace},
        },
    }


def test_no_workspace_key_means_no_row():
    assert project_workspace_defaults(spec(...)) is None


def test_the_shorthand_sets_both_modes_and_the_tier_template():
    assert project_workspace_defaults(spec("site")) == {
        "jobs": "container",
        "sessions": "container",
        "container": SANDBOX,
        "vm": None,
    }
    assert project_workspace_defaults(spec("lite")) == {
        "jobs": "virtual",
        "sessions": "virtual",
        "container": None,
        "vm": None,
    }


def test_null_means_none_for_both_roles():
    assert project_workspace_defaults(spec(None)) == {
        "jobs": "none",
        "sessions": "none",
        "container": None,
        "vm": None,
    }


def test_the_object_keeps_missing_fields_empty():
    assert project_workspace_defaults(spec({"jobs": "vm", "container": "site"})) == {
        "jobs": "vm",
        "sessions": None,
        "container": SANDBOX,
        "vm": None,
    }


def test_a_tier_template_must_match_its_tier():
    with pytest.raises(ValueError, match="The vm template must be a vm workspace."):
        project_workspace_defaults(spec({"vm": "site"}))


def test_the_schema_accepts_the_object_and_rejects_unknown_fields():
    validate_documents(
        [
            project(
                {
                    "jobs": "container",
                    "sessions": "virtual",
                    "container": "site",
                    "vm": "box",
                }
            )
        ]
    )
    with pytest.raises(ManifestError):
        validate_documents([project({"jobs": "sandbox"})])
    with pytest.raises(ManifestError):
        validate_documents([project({"cpu": 2})])


def test_object_aliases_must_be_declared():
    with pytest.raises(ManifestError) as error:
        validate_documents([project({"container": "missing"})])
    assert error.value.issue.code == "UnknownAlias"


def test_resolution_rejects_a_backend_mismatch():
    with pytest.raises(ManifestError) as error:
        preview_documents([project({"vm": "site"})])
    assert error.value.issue.code == "WorkspaceDefaultBackendMismatch"
