"""Keep the public list document, mounted JSON and Cockpit projection aligned."""

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from fastapi.openapi.utils import get_openapi
import pytest

from orchestrator.schemas.job_list import PublicJobListPage


ROOT = Path(__file__).resolve().parents[1]
MODELS = ROOT / "cockpit/src/app/core/models"


def test_list_schema_import_does_not_start_application_or_agent_runtime():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
from orchestrator.schemas.job_list import PublicJobListPage
assert 'orchestrator.main' not in sys.modules
assert 'agent' not in sys.modules
assert 'origin' in PublicJobListPage.model_json_schema()['$defs']['PublicJobListFilters']['properties']
""",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


@pytest.fixture(scope="module")
def document():
    from orchestrator.main import app

    return get_openapi(title=app.title, version=app.version, routes=app.routes)


def test_composed_list_operation_documents_models_without_filtering_runtime_response(
    document,
):
    from orchestrator.routers.job_reads import router

    route = next(
        r
        for r in router.routes
        if getattr(r, "path", None) == "/api/jobs"
        and "GET" in getattr(r, "methods", ())
    )
    assert route.response_model == dict[str, Any]
    operation = document["paths"]["/api/jobs"]["get"]
    assert operation["operationId"] == "list_jobs_api_jobs_get"
    assert set(operation["responses"]) == {"200", "400", "401", "403", "422", "500"}
    response_schema = operation["responses"]["200"]["content"]["application/json"][
        "schema"
    ]
    assert response_schema["$ref"] == "#/components/schemas/PublicJobListPage"
    schemas = document["components"]["schemas"]
    for name in ("PublicJobListPage", "PublicJobListFilters", "PublicJobListItem"):
        assert schemas[name]["additionalProperties"] is True
        assert set(schemas[name]["required"]) == set(schemas[name]["properties"])
    # Query arrays remain repeated parameters, not a newly introduced body.
    assert "requestBody" not in operation
    params = {p["name"]: p for p in operation["parameters"]}
    assert set(params) == {
        "status",
        "origin",
        "project_id",
        "has_project",
        "include_archived_projects",
        "search",
        "as_of",
        "user_id",
        "limit",
        "offset",
        "include_total",
    }
    for key in ("status", "origin", "project_id"):
        assert {branch["type"] for branch in params[key]["schema"]["anyOf"]} == {
            "array",
            "null",
        }
    assert params["include_total"]["schema"]["default"] is True


def test_shared_wire_fixture_validates_without_losing_nulls_or_rows():
    payload = json.loads((MODELS / "fixtures/job-list-page.json").read_text())
    parsed = PublicJobListPage.model_validate_json(json.dumps(payload), strict=True)
    assert parsed.model_dump(mode="json") == payload
    assert len(parsed.jobs) > parsed.limit
    assert parsed.jobs[0].config_name is None and parsed.jobs[0].audit_count is None
    assert parsed.filters.origin == ["user", "subjob"]
    payload["total"] = None
    payload["total_is_capped"] = False
    parsed = PublicJobListPage.model_validate_json(json.dumps(payload), strict=True)
    assert parsed.total is None


def interface_fields(filename, name):
    source = (MODELS / filename).read_text()
    body = re.search(rf"export interface {name} \{{(.*?)^\}}", source, re.S | re.M)
    assert body is not None
    return {
        field: value
        for field, value in re.findall(r"^  (\w+)\??: ([^;]+);", body.group(1), re.M)
    }


def test_cockpit_envelope_and_filter_fields_track_the_published_contract(document):
    schemas = document["components"]["schemas"]
    for ts, model in (
        ("JobListPage", "PublicJobListPage"),
        ("JobListFilters", "PublicJobListFilters"),
    ):
        fields = interface_fields("audit.model.ts", ts)
        properties = schemas[model]["properties"]
        assert set(fields) == set(properties)
        for name, declaration in fields.items():
            nullable = any(
                branch.get("type") == "null"
                for branch in properties[name].get("anyOf", [])
            )
            assert ("null" in declaration.split(" | ")) is nullable, name


def test_shared_job_fields_allow_the_wire_nulls_in_both_cockpit_projections(document):
    properties = document["components"]["schemas"]["PublicJobListItem"]["properties"]
    for filename, interface in (
        ("audit.model.ts", "JobSummary"),
        ("api.model.ts", "Job"),
    ):
        fields = interface_fields(filename, interface)
        for name in fields.keys() & properties.keys():
            if any(
                branch.get("type") == "null"
                for branch in properties[name].get("anyOf", [])
            ):
                assert "null" in fields[name].split(" | "), (
                    f"{interface}.{name} excludes wire null"
                )


def test_list_document_excludes_private_join_fields(document):
    fields = document["components"]["schemas"]["PublicJobListItem"]["properties"]
    assert {
        "_workspace_config_override",
        "_workspace_context",
        "project_has_cloud_folder",
    }.isdisjoint(fields)
    assert "workspace_recovery" in fields
    recovery = document["components"]["schemas"]["WorkspaceRecoveryView"]
    assert set(recovery["properties"]) == {
        "operation_id",
        "state",
        "reason_code",
        "message",
        "started_at",
        "deadline_at",
        "next_check_at",
        "retryable",
        "cleanup_pending",
    }
    assert {
        "private_endpoint",
        "latest_diagnostic",
        "captured_identity",
    }.isdisjoint(recovery["properties"])
