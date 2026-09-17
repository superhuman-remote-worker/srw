"""The mounted list route's query/JSON contract, shared with Cockpit.

Identity, storage and audit availability are controlled; the real query parsing,
visibility checks, response serializer and redactors run without app startup.
Real SQL pagination is exercised in test_job_subjob_roster_real_postgres.py.
"""

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from orchestrator import main
from orchestrator.routers.job_reads import router as job_reads_router


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "cockpit/src/app/core/models/fixtures/job-list-page.json"
USER = "11111111-1111-4111-8111-111111111111"
PROJECT = "22222222-2222-4222-8222-222222222222"
OTHER_PROJECT = "22222222-2222-4222-8222-222222222223"
JOB = "33333333-3333-4333-8333-333333333333"
CHILD = "33333333-3333-4333-8333-333333333334"
STAMP = datetime(2026, 9, 6, 8, 0, tzinfo=timezone.utc)
WATERMARK = "2026-09-06T09:00:00Z"


def row(**fields):
    """Representative query_jobs row, including nullable PostgreSQL columns."""
    return {
        "id": UUID(JOB),
        "description": "Job-list contract fixture",
        "status": "completed",
        "completion_outcome_kind": None,
        "origin": "user",
        "config_name": None,
        "assigned_agent_id": None,
        "user_id": UUID(USER),
        "project_id": UUID(PROJECT),
        "parent_job_id": None,
        "priority": 5,
        "repo_name": None,
        "branch_name": None,
        "merge_status": None,
        "diff_status": None,
        "exported_at": None,
        "exported_folder_handle": None,
        "error_message": None,
        "created_at": STAMP,
        "created_by_thread_id": None,
        "snapshot_status": None,
        "project_name": "Contract project",
        "project_has_cloud_folder": False,
        "pending_approval": False,
        "pending_approval_request_id": None,
        "display_root_id": UUID(JOB),
        "is_display_root": True,
        "subjob_count": 1,
        "workspace_contract": {"state": "waiting"},
        **fields,
    }


@pytest.fixture
def wire(monkeypatch):
    user = {"id": USER, "is_admin": False, "is_approved": True}

    async def approved(request, _db):
        if request.headers.get("x-test-user") != USER:
            raise HTTPException(401, "Authentication required")
        if not user["is_approved"]:
            raise HTTPException(403, "Account pending approval")
        return user

    result = SimpleNamespace(
        jobs=[row()], total=1, total_is_capped=False, has_more=False
    )
    db = SimpleNamespace(
        query_jobs=AsyncMock(side_effect=lambda **kw: copy.deepcopy(result))
    )
    visible = AsyncMock(return_value=[UUID(PROJECT), UUID(OTHER_PROJECT)])
    audit = SimpleNamespace(
        is_available=False, get_audit_counts=AsyncMock(return_value={})
    )
    monkeypatch.setattr(main, "postgres_db", db)
    monkeypatch.setattr(main, "require_approved_user", approved)
    monkeypatch.setattr(main, "user_visible_project_ids", visible)
    monkeypatch.setattr(main, "audit_reader", audit)
    # Mount the production read router, retaining decorator response metadata.
    app = FastAPI(default_response_class=main.CustomJSONResponse)
    app.state.job_reads_dependencies_factory = main._job_reads_dependencies
    app.include_router(job_reads_router)
    return SimpleNamespace(
        app=app, user=user, db=db, visible=visible, audit=audit, result=result
    )


async def get(wire, params=(), *, authenticated=True):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wire.app), base_url="http://list.test"
    ) as client:
        return await client.get(
            "/api/jobs",
            params=params,
            headers={"x-test-user": USER} if authenticated else {},
        )


@pytest.mark.asyncio
async def test_shared_cockpit_fixture_is_actual_serialized_list_json(wire):
    wire.result.jobs.append(
        row(
            id=UUID(CHILD),
            parent_job_id=UUID(JOB),
            origin="subjob",
            description="Matching child",
            config_name="scholar",
            is_display_root=False,
            subjob_count=0,
        )
    )
    wire.result.total = 10_000
    wire.result.total_is_capped = True
    wire.result.has_more = True
    response = await get(
        wire,
        [
            ("status", "completed"),
            ("status", "completed"),
            ("origin", "user"),
            ("origin", "subjob"),
            ("origin", "user"),
            ("project_id", PROJECT),
            ("project_id", OTHER_PROJECT),
            ("project_id", PROJECT),
            ("as_of", WATERMARK),
            ("limit", "1"),
            ("offset", "2"),
        ],
    )
    assert response.status_code == 200, response.text
    assert response.json() == json.loads(FIXTURE.read_text())
    kw = wire.db.query_jobs.call_args.kwargs
    assert kw == {
        "owner_user_id": USER,
        "visible_project_ids": [PROJECT, OTHER_PROJECT],
        "scope_project_id": None,
        "statuses": ["completed"],
        "origins": ["user", "subjob"],
        "project_ids": [PROJECT, OTHER_PROJECT],
        "has_project": None,
        "include_archived_projects": False,
        "search": None,
        "as_of": datetime(2026, 9, 6, 9, tzinfo=timezone.utc),
        "user_id": None,
        "limit": 1,
        "offset": 2,
        "include_total": True,
    }


@pytest.mark.asyncio
async def test_list_retry_action_is_only_projected_for_canonical_recovery_owner(wire):
    recovery = {
        "operation_id": "44444444-4444-4444-8444-444444444444",
        "state": "paused_attention",
        "reason_code": "prior_runtime_unfenced",
        "started_at": "2026-09-16T08:00:00Z",
        "deadline_at": "2026-09-16T08:15:00Z",
        "next_check_at": None,
    }
    wire.result.jobs = [
        row(_workspace_recovery={**recovery, "canonical_owner": True}),
        row(
            id=UUID(CHILD),
            parent_job_id=UUID(JOB),
            is_display_root=False,
            _workspace_recovery={**recovery, "canonical_owner": False},
        ),
    ]

    response = await get(wire)

    assert response.status_code == 200
    owner, child = response.json()["jobs"]
    assert owner["workspace_recovery"]["retryable"] is True
    assert child["workspace_recovery"]["retryable"] is False
    assert "canonical_owner" not in json.dumps(response.json())


@pytest.mark.asyncio
async def test_default_query_echo_and_watermark_round_trip_without_count(wire):
    before = datetime.now(timezone.utc)
    first = await get(wire)
    assert first.status_code == 200
    page = first.json()
    assert before <= datetime.fromisoformat(page["as_of"]) <= datetime.now(timezone.utc)
    assert page["as_of"].endswith("Z")
    assert page["limit"] == 100 and page["offset"] == 0
    assert page["filters"] == {
        "status": [],
        "origin": [],
        "project_id": [],
        "has_project": None,
        "include_archived_projects": False,
        "search": None,
        "user_id": None,
    }
    wire.result.total = None
    second = await get(
        wire, {"as_of": page["as_of"], "offset": 100, "include_total": "false"}
    )
    assert second.status_code == 200
    assert second.json()["as_of"] == page["as_of"]
    assert second.json()["total"] is None
    assert second.json()["total_is_capped"] is False
    assert wire.db.query_jobs.call_args.kwargs["include_total"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stamp", [WATERMARK, "2026-09-06T11:00:00+02:00", "2026-09-06T09:00:00"]
)
async def test_accepted_watermark_formats_are_not_newly_normalized(wire, stamp):
    response = await get(wire, {"as_of": stamp})
    assert response.status_code == 200
    assert response.json()["as_of"] == stamp
    assert wire.db.query_jobs.call_args.kwargs["as_of"] == datetime.fromisoformat(stamp)


@pytest.mark.asyncio
@pytest.mark.parametrize("search", [None, "", "literal + & # résumé"])
async def test_search_omission_and_empty_echo_stay_distinct(wire, search):
    params = {} if search is None else {"search": search}
    response = await get(wire, params)
    assert response.status_code == 200
    assert response.json()["filters"]["search"] == search
    assert wire.db.query_jobs.call_args.kwargs["search"] == (search or None)


@pytest.mark.asyncio
async def test_projectless_and_archived_flags_are_applied_and_echoed(wire):
    response = await get(
        wire, {"project_id": "none", "include_archived_projects": "true"}
    )
    assert response.status_code == 200
    assert response.json()["filters"]["project_id"] == []
    assert response.json()["filters"]["has_project"] is False
    assert response.json()["filters"]["include_archived_projects"] is True
    assert wire.db.query_jobs.call_args.kwargs["project_ids"] is None
    assert wire.db.query_jobs.call_args.kwargs["has_project"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("available,count", [(False, None), (True, 0), (True, 7)])
async def test_optional_audit_service_uses_null_for_unavailable_and_zero_for_missing(
    wire, available, count
):
    wire.audit.is_available = available
    wire.audit.get_audit_counts.return_value = {JOB: count} if count else {}
    response = await get(wire)
    assert response.status_code == 200
    assert response.json()["jobs"][0]["audit_count"] == count
    if available:
        wire.audit.get_audit_counts.assert_awaited_once_with([JOB])
    else:
        wire.audit.get_audit_counts.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("as_text", [False, True])
async def test_redaction_preserves_jsonb_shape_and_unmodeled_public_extensions(
    wire, as_text
):
    context = {
        "safe": "context",
        "vm": {"ssh_host": "synthetic-private"},
        "workspace_container": {"pod_name": "synthetic-private"},
    }
    config = {
        "llm": {"model": "synthetic-model", "api_key": "synthetic-private"},
        "workspace": {"remote": {"host": "synthetic-private"}, "backend": "remote"},
    }
    wire.result.jobs[0].update(
        context=json.dumps(context) if as_text else context,
        config_override=json.dumps(config) if as_text else config,
        existing_extension={"nullable": None, "id": UUID(CHILD), "at": STAMP},
    )
    response = await get(wire)
    assert response.status_code == 200
    item = response.json()["jobs"][0]
    assert "synthetic-private" not in response.text
    assert "project_has_cloud_folder" not in item
    assert isinstance(item["context"], str if as_text else dict)
    assert isinstance(item["config_override"], str if as_text else dict)
    public_context = json.loads(item["context"]) if as_text else item["context"]
    public_config = (
        json.loads(item["config_override"]) if as_text else item["config_override"]
    )
    assert public_context == {"safe": "context"}
    assert public_config["llm"]["model"] == "synthetic-model"
    assert (
        "api_key" not in public_config["llm"]
        and "remote" not in public_config["workspace"]
    )
    assert item["existing_extension"] == {
        "nullable": None,
        "id": CHILD,
        "at": "2026-09-06T08:00:00Z",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "authenticated,approved,code", [(False, True, 401), (True, False, 403)]
)
async def test_refused_identity_never_queries_storage(
    wire, authenticated, approved, code
):
    wire.user["is_approved"] = approved
    response = await get(wire, authenticated=authenticated)
    assert response.status_code == code and isinstance(response.json()["detail"], str)
    wire.db.query_jobs.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("admin", [False, True])
async def test_self_owner_filter_keeps_nonadmin_project_visibility(wire, admin):
    wire.user["is_admin"] = admin
    response = await get(wire, {"user_id": USER})
    assert response.status_code == 200
    kw = wire.db.query_jobs.call_args.kwargs
    assert kw["user_id"] == (USER if admin else None)
    assert kw["owner_user_id"] == (None if admin else USER)
    assert kw["visible_project_ids"] == (None if admin else [PROJECT, OTHER_PROJECT])
    assert response.json()["filters"]["user_id"] == (USER if admin else None)


@pytest.mark.asyncio
async def test_mcp_project_scope_remains_an_additional_narrowing_filter(wire):
    wire.user.update(auth_method="mcp", scopes=[f"project:{PROJECT}"])
    ok = await get(wire, {"project_id": PROJECT})
    assert ok.status_code == 200
    assert wire.db.query_jobs.call_args.kwargs["scope_project_id"] == PROJECT
    wire.db.query_jobs.reset_mock()
    refused = await get(wire, {"project_id": OTHER_PROJECT})
    assert refused.status_code == 403
    assert refused.json() == {"detail": "Access denied by MCP token scope"}
    wire.db.query_jobs.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params,code,fragment",
    [
        ({"user_id": JOB}, 403, "other users"),
        ({"project_id": JOB}, 403, "Not authorized"),
        ({"status": "typo"}, 422, "Unknown job status"),
        ({"origin": "all"}, 422, "Unknown job origin"),
        ({"project_id": "typo"}, 422, "not a uuid"),
        ([("project_id", "none"), ("project_id", PROJECT)], 422, "cannot be combined"),
        ({"offset": main.JOBS_MAX_OFFSET + 1}, 400, "exceeds the maximum"),
    ],
)
async def test_handler_error_details_survive_http_and_never_query_storage(
    wire, params, code, fragment
):
    response = await get(wire, params)
    assert response.status_code == code
    assert fragment in response.json()["detail"]
    wire.db.query_jobs.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params,field",
    [
        ({"limit": 0}, "limit"),
        ({"limit": 501}, "limit"),
        ({"offset": -1}, "offset"),
        ({"as_of": "not-a-time"}, "as_of"),
        ({"include_total": "maybe"}, "include_total"),
        ({"has_project": "maybe"}, "has_project"),
        ({"search": "x" * 201}, "search"),
    ],
)
async def test_query_validation_errors_keep_fastapi_detail_array(wire, params, field):
    response = await get(wire, params)
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["query", field]
    wire.db.query_jobs.assert_not_awaited()


@pytest.mark.asyncio
async def test_storage_failure_keeps_existing_string_detail(wire):
    wire.db.query_jobs.side_effect = RuntimeError("synthetic storage failure")
    response = await get(wire)
    assert response.status_code == 500
    assert response.json() == {"detail": "synthetic storage failure"}
