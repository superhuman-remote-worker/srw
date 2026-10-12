"""Conformance and resolution behavior for the independent manifest contract."""

from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys

import pytest
import yaml

from shared.manifests import (
    API_VERSION,
    ManifestError,
    export_documents,
    load_schema,
    parse_documents,
    preview_documents,
    validate_documents,
)
from shared.manifests.resolution import content_revision
from shared.manifests.validation import MAX_DEPTH, MAX_DOCUMENTS, MAX_SOURCE_BYTES

EXAMPLES = Path(__file__).resolve().parents[1] / "examples" / "manifests"
ACCOUNT = {"kind": "Account", "name": "personal"}
PROJECT = {"kind": "Project", "name": "team"}
CATALOG = {"kind": "Catalog", "name": "shared"}


def resource(kind="Expert", name="worker", spec=None, scope=ACCOUNT):
    return {
        "apiVersion": API_VERSION,
        "kind": kind,
        "metadata": {"name": name, "scope": deepcopy(scope)},
        "spec": deepcopy(
            spec if spec is not None else {"runtime": {"image": "example/worker:1"}}
        ),
    }


def assignment(execution, **spec):
    return resource(
        "Job",
        "assignment",
        {"task": {"text": "Do the work"}, "execution": execution, **spec},
    )


def error_code(code, operation, *args, **kwargs):
    with pytest.raises(ManifestError) as error:
        operation(*args, **kwargs)
    assert error.value.issue.code == code
    return error.value.issue


def test_all_public_examples_validate_and_resolve_without_mutating_input():
    documents = [
        doc
        for path in sorted(EXAMPLES.glob("*.yaml"))
        for doc in parse_documents(path.read_text())
    ]
    original = deepcopy(documents)
    # srw-workspace-selection.yaml authors no scope on purpose: it documents the
    # scope the authenticated API or the preview CLI supplies. An explicit
    # metadata.scope in the other examples still wins over this default.
    scoped = [
        doc
        if "scope" in doc["metadata"]
        else {**doc, "metadata": {**doc["metadata"], "scope": ACCOUNT}}
        for doc in original
    ]
    result = preview_documents(documents, default_scope=ACCOUNT)
    assert len(result["documents"]) == len(documents) >= 9
    assert documents == original
    # Resolution fills the supplied default scope and changes nothing else.
    assert result["documents"] == scoped
    assert result["admissionReady"] is False
    assert result["effects"] == []
    assert "resourceAuthorization" in result["pendingChecks"]
    assert "workspaceInstances" in result["pendingChecks"]
    job = next(
        doc
        for doc in result["resolved"]
        if doc["metadata"]["name"] == "implement-cpp-feature"
    )
    execution = job["spec"]["execution"]
    assert execution["workspace"]["template"]["inline"]["retention"] == "Retain"
    assert (
        execution["connectors"]["source"]["inline"]["credentials"]["token"][
            "secretRef"
        ]["scope"]
        == ACCOUNT
    )
    assert parse_documents(export_documents(documents, default_scope=ACCOUNT)) == scoped
    assert documents == original


def test_private_configuration_and_image_defaults_survive_every_operation():
    config = {
        "unknownTool": {"optional": None},
        "tools": ["not-installed"],
        "password": "authored-placeholder",
        "text": ["1e3", "yes", "012", "2026-09-09", "null"],
        "values": [True, False, None, 1, 1.25],
    }
    expert = resource(spec={"runtime": {"image": "example/custom:1", "config": config}})
    connector = resource(
        "Connector", "credentials", {"driver": "custom", "config": config}
    )
    documents = [expert, connector]
    assert parse_documents(json.dumps(documents), format="json") == parse_documents(
        export_documents(documents)
    )
    result = preview_documents(documents)
    runtime = result["resolved"][0]["spec"]["runtime"]
    assert runtime["config"] == config
    assert "command" not in runtime and "args" not in runtime and "env" not in runtime
    assert result["resolved"][1]["spec"]["config"] == config
    for format in ("json", "yaml"):
        assert (
            parse_documents(export_documents(documents, format=format), format=format)
            == documents
        )


@pytest.mark.parametrize(
    "format,source,code",
    [
        ("json", '{"kind": "Expert", "kind": "Job"}', "DuplicateKey"),
        ("yaml", "kind: Expert\nkind: Job", "DuplicateKey"),
        ("yaml", "base: &base {a: 1}\nmerged: {<<: *base}", "YAMLMergeKey"),
        ("yaml", "number: yes", "AmbiguousScalar"),
        ("yaml", "number: 012", "AmbiguousScalar"),
        ("yaml", "number: 0x12", "AmbiguousScalar"),
        ("yaml", "number: .nan", "AmbiguousScalar"),
        ("yaml", "number: ", "AmbiguousScalar"),
        ("yaml", "1: value", "InvalidObjectKey"),
        ("yaml", "date: 2026-09-09", "InvalidJSONValue"),
        ("yaml", "value: !custom text", "InvalidSyntax"),
        ("yaml", "value: &cycle [*cycle]", "RecursiveAlias"),
        ("json", '{"value": NaN}', "InvalidJSONValue"),
        ("json", '{"value": 1e999}', "InvalidJSONValue"),
    ],
)
def test_non_json_or_ambiguous_inputs_fail(format, source, code):
    error_code(code, parse_documents, source, format=format)


@pytest.mark.parametrize("source", ['{"value": "\\ud800"}', '{"\\udfff": 1}'])
def test_unpaired_unicode_surrogates_cannot_reach_http_or_export(source):
    error_code("InvalidJSONValue", parse_documents, source, format="json")


def test_errors_identify_document_and_path_without_echoing_values():
    doc = resource(
        spec={"runtime": {"image": "example/worker:1", "typo": "PRIVATE-SENTINEL"}}
    )
    issue = error_code(
        "UnknownField", parse_documents, json.dumps([resource(), doc]), format="json"
    )
    assert issue.document == 2
    assert issue.path == "/spec/runtime"
    assert "PRIVATE-SENTINEL" not in str(issue)
    error = error_code(
        "DuplicateKey",
        parse_documents,
        yaml.safe_dump(resource()) + "---\nkey: one\nkey: PRIVATE-SENTINEL",
    )
    assert error.document == 2
    assert "PRIVATE-SENTINEL" not in str(error)


def test_input_limits_cover_text_depth_count_and_alias_expansion():
    error_code("InputLimitExceeded", parse_documents, " " * (MAX_SOURCE_BYTES + 1))
    error_code("DocumentLimit", validate_documents, [resource()] * (MAX_DOCUMENTS + 1))
    value = None
    for _ in range(MAX_DEPTH):
        value = [value]
    error_code(
        "InputLimitExceeded",
        validate_documents,
        [resource(spec={"runtime": {"image": "image", "config": {"deep": value}}})],
    )
    # Aliases occupy little source, but expand during JSON serialization.
    source = (
        yaml.safe_dump(resource())
        + "extra: &a [1, 2]\nb: &b [*a, *a, *a, *a, *a, *a, *a, *a, *a, *a]\nc: &c [*b, *b, *b, *b, *b, *b, *b, *b, *b, *b]\nd: &d [*c, *c, *c, *c, *c, *c, *c, *c, *c, *c]\ne: &e [*d, *d, *d, *d, *d, *d, *d, *d, *d, *d]\nf: [*e, *e, *e, *e, *e, *e, *e, *e, *e, *e]"
    )
    error_code("InputLimitExceeded", parse_documents, source)


def test_repeated_references_are_bounded_before_export_expansion():
    expert = resource(
        spec={
            "runtime": {"image": "example/worker:1", "config": {"large": "x" * 500_000}}
        }
    )
    jobs = [assignment({"expert": {"ref": {"name": "worker"}}}) for _ in range(20)]
    for number, job in enumerate(jobs):
        job["metadata"]["name"] = f"job-{number}"
    error_code("InputLimitExceeded", preview_documents, [expert, *jobs])


@pytest.mark.parametrize(
    "mutation",
    [
        lambda doc: doc.update(status={"phase": "Completed"}),
        lambda doc: doc["metadata"].update(uid="server-owned"),
        lambda doc: doc["spec"]["runtime"].update(command=[]),
        lambda doc: doc["spec"]["runtime"].update(env={"SRW_CONFIG_FILE": "override"}),
    ],
)
def test_platform_and_server_owned_fields_are_strict(mutation):
    doc = resource()
    mutation(doc)
    with pytest.raises(ManifestError):
        validate_documents([doc])


def test_selection_requires_exactly_one_of_ref_and_inline():
    for selection in ({}, {"ref": {"name": "worker"}, "inline": resource()["spec"]}):
        with pytest.raises(ManifestError):
            validate_documents([assignment({"expert": selection})])


def test_reported_completion_requires_a_deadline_but_plain_images_need_no_hooks():
    job = assignment({"expert": {"inline": resource()["spec"]}})
    result = preview_documents([job])["resolved"][0]["spec"]
    assert result["completion"] == {"mode": "ProcessExit"}
    assert result["retry"] == {"maxAttempts": 1}
    job["spec"]["completion"] = {"mode": "Reported"}
    with pytest.raises(ManifestError):
        validate_documents([job])
    job["spec"]["timeoutSeconds"] = 60
    assert validate_documents([job]) == [job]


def test_ref_inline_equivalence_and_definition_scope_for_credentials():
    expert = resource(
        scope=CATALOG,
        spec={
            "runtime": {
                "image": "image",
                "env": {"TOKEN": {"secretRef": {"name": "auth", "key": "token"}}},
            }
        },
    )
    job = assignment({"expert": {"ref": {"name": "worker", "scope": CATALOG}}})
    result = preview_documents([expert, job])
    runtime = result["resolved"][1]["spec"]["execution"]["expert"]["inline"]["runtime"]
    assert runtime["env"]["TOKEN"]["secretRef"]["scope"] == CATALOG
    inline = assignment({"expert": {"inline": {"runtime": runtime}}})
    assert (
        preview_documents([inline])["resolved"][0]["spec"]
        == result["resolved"][1]["spec"]
    )
    assert result["dependencies"][0]["revision"] == content_revision(
        result["resolved"][0]["spec"]
    )
    job["spec"]["execution"]["expert"]["ref"]["revision"] = result["dependencies"][0][
        "revision"
    ]
    preview_documents([expert, job])
    job["spec"]["execution"]["expert"]["ref"]["revision"] = "sha256:wrong"
    error_code("RevisionMismatch", preview_documents, [expert, job])
    del job["spec"]["execution"]["expert"]["ref"]["scope"]
    error_code("UnresolvedReference", preview_documents, [expert, job])


def project_bundle():
    project = resource(
        "Project",
        "team",
        {
            "resources": {
                "experts": {"worker": {"inline": resource()["spec"]}},
                "workspaces": {"code": {"inline": {"backend": "sandbox"}}},
                "connectors": {
                    "source": {
                        "inline": {
                            "driver": "repository",
                            "credentials": {
                                "key": {"secretRef": {"name": "repo", "key": "ssh"}}
                            },
                        }
                    }
                },
            },
            "defaults": {
                "expert": "worker",
                "workspace": "code",
                "connectors": ["source"],
            },
        },
    )
    job = assignment({})
    job["metadata"]["scope"] = deepcopy(PROJECT)
    return project, job


def test_project_defaults_apply_only_to_omitted_fields_and_owned_children_get_project_scope():
    project, job = project_bundle()
    result = preview_documents([project, job])
    execution = result["resolved"][1]["spec"]["execution"]
    # defaults.workspace is not copied: admission resolves the defaults chain.
    assert "workspace" not in execution
    assert (
        execution["connectors"]["source"]["inline"]["credentials"]["key"]["secretRef"][
            "scope"
        ]
        == PROJECT
    )
    assert len(result["defaults"]) == 2
    job["spec"]["execution"].update(workspace=None, connectors={})
    result = preview_documents([project, job])
    assert result["resolved"][1]["spec"]["execution"]["workspace"] is None
    assert result["resolved"][1]["spec"]["execution"]["connectors"] == {}
    assert len(result["defaults"]) == 1
    job["spec"]["execution"]["expert"] = {"ref": {"name": "worker"}}
    assert (
        preview_documents([project, job])["resolved"][1]["spec"]["execution"]["expert"]
        == execution["expert"]
    )


def test_project_default_aliases_scope_and_resource_collisions_fail_explicitly():
    project, job = project_bundle()
    error_code("UnresolvedProjectDefaults", preview_documents, [job])
    project["spec"]["defaults"]["expert"] = "missing"
    error_code("UnknownAlias", validate_documents, [project])
    project, job = project_bundle()
    error_code(
        "DuplicateResource", preview_documents, [project, resource(scope=PROJECT)]
    )
    error_code("DuplicateResource", preview_documents, [resource(), resource()])
    other = deepcopy(project)
    other["metadata"]["scope"]["name"] = "another-account"
    error_code("AmbiguousProject", preview_documents, [project, other])
    del project["metadata"]["scope"]
    error_code("InvalidScope", preview_documents, [project], default_scope=PROJECT)


def test_scope_context_is_required_for_export_and_authored_references_remain_portable():
    job = assignment({"expert": {"ref": {"name": "external"}}})
    del job["metadata"]["scope"]
    error_code("MissingScope", export_documents, [job])
    exported = parse_documents(export_documents([job], default_scope=ACCOUNT))
    assert exported[0]["metadata"]["scope"] == ACCOUNT
    assert exported[0]["spec"]["execution"]["expert"] == {"ref": {"name": "external"}}
    error_code("UnresolvedReference", preview_documents, exported)


def test_compute_comparisons_use_units_and_virtual_backend_rejects_os_features():
    expert = resource(
        spec={
            "runtime": {
                "image": "image",
                "resources": {
                    "requests": {"cpu": 0.5, "memory": "1024Mi"},
                    "limits": {"cpu": 1, "memory": "1Gi"},
                },
            }
        }
    )
    preview_documents([expert])
    expert["spec"]["runtime"]["resources"]["requests"]["memory"] = "2Gi"
    error_code("InvalidResources", preview_documents, [expert])
    for fields in (
        {"environment": {"image": "image"}},
        {"resources": {"cpu": 1}},
        {"initialize": [{"command": ["setup"]}]},
    ):
        error_code(
            "UnsupportedWorkspace",
            preview_documents,
            [
                resource(
                    "WorkspaceTemplate", "virtual", {"backend": "virtual", **fields}
                )
            ],
        )


def test_oversized_quantity_produces_a_diagnostic_instead_of_an_internal_error():
    quantity = "9" * (sys.int_info.default_max_str_digits + 1) + "Gi"
    expert = resource(
        spec={
            "runtime": {
                "image": "image",
                "resources": {
                    "requests": {"memory": quantity},
                    "limits": {"memory": "1Gi"},
                },
            }
        }
    )
    issue = error_code("InputLimitExceeded", preview_documents, [expert])
    assert issue.path == "/spec/runtime/resources/requests/memory"


def test_schema_and_cli_work_outside_checkout_without_importing_runtime(tmp_path):
    code = """
import importlib.abc
import sys
class Blocker(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in {"orchestrator", "agent", "fastapi", "pydantic"} or fullname.startswith("shared.runtime"):
            raise AssertionError("Generic manifest path loaded " + fullname)
sys.meta_path.insert(0, Blocker())
from shared.manifests import load_schema, parse_documents, preview_documents
import json
doc = {"apiVersion": "srw/v1alpha1", "kind": "Expert", "metadata": {"name": "worker", "scope": {"kind": "Account", "name": "personal"}}, "spec": {"runtime": {"image": "image", "config": {"literal": None}}}}
assert load_schema()["$defs"]["ExpertSpec"]
assert preview_documents(parse_documents(json.dumps(doc), format="json"))["resolved"][0]["spec"]["runtime"]["config"] == {"literal": None}
"""
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-c", code],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-m",
            "shared.manifests",
            "validate",
            str(EXAMPLES / "inline-job.yaml"),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"valid": True, "documents": 1}
    # Callers cannot mutate the cached schema for subsequent requests.
    schema = load_schema()
    schema.clear()
    assert load_schema()["properties"]["apiVersion"]["const"] == API_VERSION
