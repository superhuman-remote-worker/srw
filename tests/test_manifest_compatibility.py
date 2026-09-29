"""Published alpha fixtures and explicit manifest-version boundaries."""

from copy import deepcopy
import json
from pathlib import Path

import pytest

from shared.manifests import (
    API_VERSION,
    ManifestError,
    export_documents,
    load_schema,
    parse_documents,
    preview_documents,
)


FIXTURE = (
    Path(__file__).resolve().parents[1] / "examples/manifests/conformance/v1alpha1"
)


def workspace_defaults_chain(preview):
    """The frozen baseline with the workspace defaults chain applied.

    Recorded behaviour change within srw/v1alpha1 (Slice A2b, owner decision
    2026-09-29): a Job that omits ``execution.workspace`` keeps it omitted in
    the preview. For an SRW harness Job the Project's ``defaults.workspace`` (or
    the installation's defaults) decides it at admission; a generic-image Job
    runs without one. Either way the preview no longer copies the default.
    """
    expected = deepcopy(preview)
    omitted = {
        number
        for number, document in enumerate(expected["documents"], start=1)
        if document["kind"] == "Job"
        and "workspace" not in document["spec"]["execution"]
    }

    def copied(entry):
        return entry["document"] in omitted and entry["path"].startswith(
            "/spec/execution/workspace"
        )

    expected["defaults"] = [e for e in expected["defaults"] if not copied(e)]
    expected["dependencies"] = [e for e in expected["dependencies"] if not copied(e)]
    for number in omitted:
        expected["resolved"][number - 1]["spec"]["execution"].pop("workspace")
    return expected


def test_published_alpha_fixture_preserves_resolution_defaults_and_json_values():
    baseline = json.loads((FIXTURE / "baseline.json").read_text())
    documents = parse_documents((FIXTURE / "portable.yaml").read_text())
    before = deepcopy(documents)
    assert API_VERSION == baseline["apiVersion"]
    assert load_schema()["properties"]["apiVersion"]["const"] == API_VERSION
    assert preview_documents(documents) == workspace_defaults_chain(baseline["preview"])
    assert documents == before
    for format in ("json", "yaml"):
        exported = export_documents(documents, format=format)
        assert parse_documents(exported, format=format) == baseline["documents"]


@pytest.mark.parametrize(
    "version", ["srw/v1", "srw/v1beta1", "srw/v2", "unknown/PRIVATE-SENTINEL"]
)
def test_unsupported_manifest_version_has_a_safe_explicit_diagnostic(version):
    document = {
        "apiVersion": version,
        "kind": "Expert",
        "metadata": {"name": "worker"},
        "spec": {"runtime": {"image": "example.invalid/worker:1"}},
    }
    with pytest.raises(ManifestError) as error:
        parse_documents(json.dumps(document), format="json")
    issue = error.value.issue
    assert issue.code == "UnsupportedAPIVersion"
    assert issue.document == 1
    assert issue.path == "/apiVersion"
    assert API_VERSION in issue.message
    assert "PRIVATE-SENTINEL" not in str(error.value.as_dict())


def test_manifest_version_does_not_interpret_harness_private_version_strings():
    document = {
        "apiVersion": API_VERSION,
        "kind": "Expert",
        "metadata": {
            "name": "custom",
            "scope": {"kind": "Account", "name": "personal"},
        },
        "spec": {
            "runtime": {
                "image": "example.invalid/custom:1",
                "config": {
                    "apiVersion": "custom/v7",
                    "format": "srw/resolved-config-v999",
                },
            }
        },
    }
    result = preview_documents(parse_documents(json.dumps(document), format="json"))
    assert (
        result["resolved"][0]["spec"]["runtime"]["config"]
        == document["spec"]["runtime"]["config"]
    )


def test_unsupported_version_in_a_later_document_reports_its_own_position():
    documents = parse_documents((FIXTURE / "portable.yaml").read_text())
    documents[-1]["apiVersion"] = "srw/v2"
    with pytest.raises(ManifestError) as error:
        parse_documents(json.dumps(documents), format="json")
    assert error.value.issue.code == "UnsupportedAPIVersion"
    assert error.value.issue.document == len(documents)
    assert error.value.issue.path == "/apiVersion"
