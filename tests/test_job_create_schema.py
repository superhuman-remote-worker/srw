"""The published create schema cannot grant internal command capabilities."""

import subprocess
import sys
import re
from pathlib import Path

from fastapi import FastAPI, Request
import httpx
import pytest

from orchestrator.schemas.job_create import (
    JobCreate,
    PublicJobCreateBody,
    public_job_create_schema,
)


INTERNAL = {
    "thread_id",
    "parent_job_id",
    "creation_order",
    "worktree_path",
    "delegation_context",
    "ticket",
    "work_category",
}


def test_request_schema_import_is_independent_of_application_startup():
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
from orchestrator.schemas.job_create import JobCreate, public_job_create_schema
assert 'orchestrator.main' not in sys.modules
assert 'agent' not in sys.modules
assert JobCreate(description='minimal').description == 'minimal'
assert public_job_create_schema()['properties']['datasource_ids']['type'] == 'array'
""",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert probe.returncode == 0, probe.stderr


def test_public_schema_excludes_internal_commands_and_documents_legacy_identity():
    schema = public_job_create_schema()
    assert INTERNAL.isdisjoint(schema["properties"])
    assert "builder_session_id" not in schema["properties"]
    assert schema["required"] == ["description"]
    assert {
        "expert",
        "required_deliverables",
        "use_datasource_defaults",
        "execution_lane",
    } <= set(schema["properties"])
    assert schema["properties"]["user_id"]["deprecated"] is True
    for alias in ("config_name", "expert_id"):
        assert schema["properties"][alias]["deprecated"] is True
    # This documentation projection does not add a new extra-field rejection.
    assert schema.get("additionalProperties") is not False


def test_cockpit_create_projection_tracks_public_field_names_and_required_inputs():
    # This is a deliberately flat consumer interface, not a generated API
    # client. Catch the missing modern fields/stale builder input that motivated
    # this slice, without requiring Node in the Python CI job.
    source = (
        Path(__file__).parents[1] / "cockpit/src/app/core/models/api.model.ts"
    ).read_text()
    body = re.search(
        r"export interface JobCreateRequest \{(.*?)^\}", source, re.S | re.M
    )
    assert body is not None
    fields = dict(re.findall(r"^  (\w+)(\??):", body.group(1), re.M))
    schema = public_job_create_schema()
    assert set(fields) == set(schema["properties"])
    assert {name for name, optional in fields.items() if not optional} == set(
        schema["required"]
    )


def test_public_schema_does_not_turn_absence_into_explicit_null():
    schema = public_job_create_schema()
    field = schema["properties"]["datasource_ids"]
    assert field["type"] == "array" and "anyOf" not in field and "default" not in field
    assert schema["not"] == {
        "required": ["datasource_ids", "use_datasource_defaults"],
        "properties": {"use_datasource_defaults": {"const": True}},
    }
    assert "datasource_ids" not in JobCreate(description="omitted").model_fields_set
    assert JobCreate(description="empty", datasource_ids=[]).datasource_ids == []
    with pytest.raises(ValueError, match="not null"):
        JobCreate(description="null", datasource_ids=None)


def test_public_schema_is_a_fresh_projection_without_mutating_command_fields():
    before = JobCreate.model_json_schema()
    schema = public_job_create_schema()
    schema["properties"]["description"]["type"] = "null"
    assert public_job_create_schema()["properties"]["description"]["type"] == "string"
    assert JobCreate.model_json_schema() == before
    assert INTERNAL <= set(before["properties"])


@pytest.mark.asyncio
async def test_public_documentation_annotation_keeps_internal_parsing_and_raw_ingress_fences():
    app = FastAPI()

    @app.post("/probe")
    def probe(request: Request, body: PublicJobCreateBody):
        assert isinstance(body, JobCreate)
        return {
            "command": body.model_dump(),
            "fields_set": sorted(body.model_fields_set),
        }

    schema = app.openapi()["paths"]["/probe"]["post"]["requestBody"]["content"][
        "application/json"
    ]["schema"]
    assert INTERNAL.isdisjoint(schema["properties"])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://schema.test"
    ) as client:
        response = await client.post(
            "/probe",
            json={
                "description": "internal parsing",
                "parent_job_id": "parent",
                "creation_order": 3,
                "ticket": "ticket",
                "context": {"evidence_manifest": {"forged": True}, "safe": 1},
                "config_override": {
                    "extra": [{"repository_auth": "synthetic", "safe": True}]
                },
            },
        )
    assert response.status_code == 200
    result = response.json()
    assert result["command"]["parent_job_id"] == "parent"
    assert result["command"]["creation_order"] == 3
    assert result["command"]["ticket"] == "ticket"
    assert result["command"]["context"] == {"safe": 1}
    assert result["command"]["config_override"] == {"extra": [{"safe": True}]}
    assert "datasource_ids" not in result["fields_set"]


def test_actual_composed_operations_keep_ids_and_publish_public_request_schema():
    from fastapi.openapi.utils import get_openapi
    from orchestrator.main import app
    from orchestrator.schemas.job_create import JobCreate as legacy

    assert legacy is JobCreate
    document = get_openapi(title=app.title, version=app.version, routes=app.routes)
    for path, operation_id in {
        "/api/jobs": "create_job_api_jobs_post",
        "/api/projects/{project_id}/jobs": "create_project_job_api_projects__project_id__jobs_post",
    }.items():
        operation = document["paths"][path]["post"]
        assert operation["operationId"] == operation_id
        schema = operation["requestBody"]["content"]["application/json"]["schema"]
        assert INTERNAL.isdisjoint(schema["properties"])
        assert schema["properties"]["datasource_ids"]["type"] == "array"
        assert set(operation["responses"]) == {"200", "422"}
        # Creation still returns its redacted row without newly filtering fields.
        response = operation["responses"]["200"]["content"]["application/json"][
            "schema"
        ]
        assert response["type"] == "object" and response["additionalProperties"] is True
