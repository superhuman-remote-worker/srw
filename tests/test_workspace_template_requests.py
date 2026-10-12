"""A container template may reserve less than its maximum."""

import pytest

from shared.manifests import preview_documents
from shared.manifests.errors import ManifestError


def resolve(spec: dict) -> dict:
    document = {
        "apiVersion": "srw/v1alpha1",
        "kind": "WorkspaceTemplate",
        "metadata": {"name": "sized"},
        "spec": spec,
    }
    return preview_documents(
        [document], default_scope={"kind": "Catalog", "name": "shared"}
    )["resolved"][0]["spec"]


def refused(spec: dict) -> ManifestError:
    with pytest.raises(ManifestError) as error:
        resolve(spec)
    return error.value


def test_a_sandbox_template_keeps_its_requests():
    resources = {
        "cpu": 2,
        "memory": "4Gi",
        "storage": "10Gi",
        "requests": {"cpu": 0.5, "memory": "1Gi"},
    }
    spec = resolve({"backend": "sandbox", "resources": resources})
    assert spec["resources"] == resources


@pytest.mark.parametrize("requests", [{"cpu": 0.5}, {"memory": "1Gi"}])
def test_one_request_may_be_given_without_the_other(requests):
    spec = resolve(
        {
            "backend": "sandbox",
            "resources": {"cpu": 2, "memory": "4Gi", "requests": requests},
        }
    )
    assert spec["resources"]["requests"] == requests


@pytest.mark.parametrize(
    "spec",
    [
        {"backend": "vm", "resources": {"cpu": 4, "requests": {"cpu": 1}}},
        {"backend": "virtual", "resources": {"requests": {"cpu": 1}}},
    ],
)
def test_only_container_templates_accept_requests(spec):
    error = refused(spec)
    assert error.issue.code == "UnsupportedWorkspace"
    assert error.issue.message == (
        "Only container workspaces support resources.requests."
    )
    assert error.issue.path.endswith("/resources/requests")


@pytest.mark.parametrize(
    "resources,field",
    [
        ({"memory": "4Gi", "requests": {"cpu": 0.5}}, "cpu"),
        ({"cpu": 2, "requests": {"memory": "1Gi"}}, "memory"),
    ],
)
def test_a_request_needs_its_maximum(resources, field):
    error = refused({"backend": "sandbox", "resources": resources})
    assert error.issue.code == "InvalidResources"
    assert error.issue.message == (
        "A resource request needs its maximum in the same template."
    )
    assert error.issue.path.endswith(f"/resources/requests/{field}")


@pytest.mark.parametrize(
    "resources,field",
    [
        ({"cpu": 1, "requests": {"cpu": 1.5}}, "cpu"),
        ({"memory": "1Gi", "requests": {"memory": "2Gi"}}, "memory"),
    ],
)
def test_a_request_above_its_maximum_is_refused(resources, field):
    error = refused({"backend": "sandbox", "resources": resources})
    assert error.issue.code == "InvalidResources"
    assert error.issue.message == "Resource request exceeds its limit."
    assert error.issue.path.endswith(f"/resources/requests/{field}")


def test_a_request_equal_to_its_maximum_is_accepted():
    resources = {"cpu": 1, "memory": "2Gi", "requests": {"cpu": 1, "memory": "2Gi"}}
    assert (
        resolve({"backend": "sandbox", "resources": resources})["resources"]
        == resources
    )


def test_memory_requests_compare_by_size_not_by_text():
    # "1024Mi" sorts after "1Gi" as text but is the same size.
    equal = {"memory": "1Gi", "requests": {"memory": "1024Mi"}}
    assert resolve({"backend": "sandbox", "resources": equal})["resources"] == equal
    # "2048Mi" is twice "1Gi".
    error = refused(
        {
            "backend": "sandbox",
            "resources": {"memory": "1Gi", "requests": {"memory": "2048Mi"}},
        }
    )
    assert error.issue.message == "Resource request exceeds its limit."


def test_a_template_without_requests_is_unchanged():
    spec = resolve({"backend": "sandbox", "resources": {"cpu": 2, "memory": "4Gi"}})
    assert spec["resources"] == {"cpu": 2, "memory": "4Gi"}
