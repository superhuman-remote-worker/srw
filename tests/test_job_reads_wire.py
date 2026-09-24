"""Mounted job reads preserve their distinct wire and authorization contracts.

The paged list already has exhaustive wire coverage in test_job_list_wire.py.
These cases cover the detail, project array and status facets, plus competing
invalid inputs whose ordering must survive the shared read-router extraction.
"""

import asyncio
import copy
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from orchestrator import main
from orchestrator.routers.job_reads import JobReadsDependencies, router
from orchestrator.services.job_queries import JobQueryDependencies
from orchestrator.services.job_reads import JobReadDependencies
from orchestrator.application import http as http_composition
from orchestrator.application import jobs as jobs_composition
from orchestrator.security import access as access_module
from orchestrator.security import auth as auth_module
from orchestrator.services import job_queries as job_queries_module
import functools


USER = "11111111-1111-4111-8111-111111111111"
PROJECT = "22222222-2222-4222-8222-222222222222"
JOB = "33333333-3333-4333-8333-333333333333"
CHILD = "33333333-3333-4333-8333-333333333334"
STAMP = datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc)
DETAIL = f"/api/jobs/{JOB}"
PROJECT_JOBS = f"/api/projects/{PROJECT}/jobs"
STATS = "/api/stats/jobs"


def job_row(**fields):
    return {
        "id": UUID(JOB),
        "project_id": UUID(PROJECT),
        "user_id": UUID(USER),
        "status": "completed",
        "created_at": STAMP,
        "workspace_contract": {"state": "waiting"},
        "project_has_cloud_folder": False,
        **fields,
    }


@pytest.fixture
def reads_wire(monkeypatch):
    events = []
    user = {"id": USER, "is_admin": False, "is_approved": True}
    state = SimpleNamespace(
        job=job_row(),
        rows=[job_row(), job_row(id=UUID(CHILD), parent_job_id=UUID(JOB))],
        project={"id": PROJECT, "main_cloud_folder_handle": "synthetic-folder"},
        stats={"total_jobs": 3, "completed": 1, "by_status": {"future_status": 2}},
    )

    async def approved(request, _db):
        events.append("approved")
        if request.headers.get("x-test-user") != USER:
            raise HTTPException(401, "Authentication required")
        return user

    async def job_access(request, db, job_id):
        await approved(request, db)
        events.append("job_access")
        assert job_id == JOB
        return user, copy.deepcopy(state.job)

    async def project_member(request, db, project_id):
        await approved(request, db)
        events.append("project_member")
        assert project_id == PROJECT
        return user, copy.deepcopy(state.project)

    async def fetch(*_args):
        events.append("fetch")
        return copy.deepcopy(state.rows)

    async def release(*_args):
        events.append("release")

    async def project(*_args):
        events.append("project")
        return copy.deepcopy(state.project)

    async def counts(*_args):
        events.append("audit_batch")
        return {JOB: 7}

    conn = SimpleNamespace(fetch=AsyncMock(side_effect=fetch))
    acquired = MagicMock()
    acquired.__aenter__ = AsyncMock(return_value=conn)
    acquired.__aexit__ = AsyncMock(side_effect=release)
    db = SimpleNamespace(
        acquire=MagicMock(return_value=acquired),
        get_project=AsyncMock(side_effect=project),
        query_jobs=AsyncMock(
            return_value=SimpleNamespace(
                jobs=[], total=0, total_is_capped=False, has_more=False
            )
        ),
        get_job_statistics=AsyncMock(
            side_effect=lambda **_kw: copy.deepcopy(state.stats)
        ),
    )
    audit = SimpleNamespace(
        is_available=False,
        get_audit_count=AsyncMock(return_value=0),
        get_audit_counts=AsyncMock(side_effect=counts),
    )
    visible = AsyncMock(return_value=[UUID(PROJECT)])
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(main.app.state.resources, "audit_reader", audit)
    monkeypatch.setattr(auth_module, "require_approved_user", approved)
    monkeypatch.setattr(
        access_module, "require_job_access", AsyncMock(side_effect=job_access)
    )
    monkeypatch.setattr(
        access_module, "require_project_member", AsyncMock(side_effect=project_member)
    )
    monkeypatch.setattr(access_module, "user_visible_project_ids", visible)
    app = FastAPI(default_response_class=http_composition.CustomJSONResponse)
    app.state.job_reads_dependencies_factory = functools.partial(
        jobs_composition.job_reads_dependencies, main.app.state.resources
    )
    app.include_router(router)
    return SimpleNamespace(
        app=app,
        state=state,
        user=user,
        db=db,
        conn=conn,
        audit=audit,
        events=events,
        visible=visible,
    )


async def get(wire, path, params=(), *, authenticated=True):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wire.app), base_url="http://reads.test"
    ) as client:
        return await client.get(
            path,
            params=params,
            headers={"x-test-user": USER} if authenticated else {},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,predicate",
    [
        (None, ""),
        ("completed", " AND js.status = $2"),
        (
            "cancelled",
            " AND js.status = $2 AND js.completion_outcome_kind IS DISTINCT FROM 'blocked_undelivered'",
        ),
        ("blocked_undelivered", " AND js.completion_outcome_kind = $2"),
    ],
)
async def test_project_array_keeps_children_change_record_fields_and_distinct_sql(
    reads_wire, status, predicate
):
    reads_wire.state.rows[0].update(
        delivery_status="delivered",
        delivery_ref="refs/heads/output",
        delivery_sha="synthetic-sha",
        change_record_type="delivered",
        existing_extension={"nullable": None, "id": UUID(CHILD), "at": STAMP},
    )
    params = {"limit": "17"}
    if status:
        params["status"] = status
    response = await get(reads_wire, PROJECT_JOBS, params)
    assert response.status_code == 200, response.text
    jobs = response.json()
    assert isinstance(jobs, list)
    assert [job["id"] for job in jobs] == [JOB, CHILD]
    assert jobs[1]["parent_job_id"] == JOB
    assert jobs[0]["delivery_status"] == "delivered"
    assert jobs[0]["delivery_ref"] == "refs/heads/output"
    assert jobs[0]["delivery_sha"] == "synthetic-sha"
    assert jobs[0]["change_record_type"] == "delivered"
    assert jobs[0]["existing_extension"] == {
        "nullable": None,
        "id": CHILD,
        "at": "2026-09-07T08:00:00Z",
    }
    assert all(job["cloud_review_mode"] == "diff" for job in jobs)
    assert all("project_has_cloud_folder" not in job for job in jobs)
    query, *args = reads_wire.conn.fetch.call_args.args
    expected = (
        "SELECT js.*, jcr.delivery_status, jcr.delivery_ref, "
        "jcr.delivery_sha, jcr.record_type AS change_record_type "
        "FROM job_summary js LEFT JOIN job_change_records jcr ON jcr.job_id = js.id "
        "WHERE js.project_id = $1"
        + predicate
        + " ORDER BY js.created_at DESC LIMIT $"
        + ("3" if status else "2")
    )
    assert " ".join(query.split()) == expected
    assert args == ([PROJECT, status, 17] if status else [PROJECT, 17])
    reads_wire.db.query_jobs.assert_not_awaited()
    reads_wire.db.get_project.assert_awaited_once_with(PROJECT)
    assert reads_wire.events == [
        "approved",
        "project_member",
        "fetch",
        "release",
        "project",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("available", [False, True])
async def test_project_audit_is_one_batch_with_null_or_zero_for_missing_counts(
    reads_wire, available
):
    reads_wire.audit.is_available = available
    response = await get(reads_wire, PROJECT_JOBS)
    assert response.status_code == 200, response.text
    assert [job["audit_count"] for job in response.json()] == (
        [7, 0] if available else [None, None]
    )
    if available:
        reads_wire.audit.get_audit_counts.assert_awaited_once_with([JOB, CHILD])
        assert reads_wire.events[-3:] == ["release", "project", "audit_batch"]
    else:
        reads_wire.audit.get_audit_counts.assert_not_awaited()
    reads_wire.audit.get_audit_count.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("available,count", [(False, None), (True, 0), (True, 9)])
@pytest.mark.parametrize("as_text", [False, True])
async def test_detail_audit_and_private_projection_preserve_jsonb_and_extensions(
    reads_wire, available, count, as_text
):
    context = {"safe": "context", "vm": {"ssh_host": "synthetic-private"}}
    config = {
        "llm": {"model": "synthetic-model", "api_key": "synthetic-private"},
        "workspace": {"backend": "remote", "remote": {"host": "synthetic-private"}},
    }
    reads_wire.state.job.update(
        context=json.dumps(context) if as_text else context,
        config_override=json.dumps(config) if as_text else config,
        existing_extension={"id": UUID(CHILD), "nullable": None},
        project_has_cloud_folder=True,
    )
    reads_wire.audit.is_available = available
    reads_wire.audit.get_audit_count.return_value = count
    response = await get(reads_wire, DETAIL)
    assert response.status_code == 200, response.text
    item = response.json()
    assert item["audit_count"] == count
    assert "synthetic-private" not in response.text
    assert isinstance(item["context"], str if as_text else dict)
    assert isinstance(item["config_override"], str if as_text else dict)
    assert (json.loads(item["context"]) if as_text else item["context"]) == {
        "safe": "context"
    }
    assert item["existing_extension"] == {"id": CHILD, "nullable": None}
    assert item["cloud_review_mode"] == "diff"
    assert "project_has_cloud_folder" not in item
    if available:
        reads_wire.audit.get_audit_count.assert_awaited_once_with(JOB)
    else:
        reads_wire.audit.get_audit_count.assert_not_awaited()
    reads_wire.audit.get_audit_counts.assert_not_awaited()
    assert reads_wire.events == ["approved", "job_access"]


@pytest.mark.asyncio
async def test_stats_wire_ignores_status_and_preserves_future_status_counts(reads_wire):
    reads_wire.state.stats["existing_extension"] = {"id": UUID(CHILD), "at": STAMP}
    response = await get(
        reads_wire,
        STATS,
        [
            ("status", "not-a-status"),
            ("status", "completed"),
            ("origin", "user"),
            ("origin", "user"),
            ("project_id", PROJECT),
            ("search", ""),
            ("as_of", "2026-09-07T08:00:00Z"),
        ],
    )
    assert response.status_code == 200, response.text
    assert response.json() == {
        "total_jobs": 3,
        "completed": 1,
        "by_status": {"future_status": 2},
        "existing_extension": {"id": CHILD, "at": "2026-09-07T08:00:00Z"},
    }
    assert reads_wire.db.get_job_statistics.call_args.kwargs == {
        "owner_user_id": USER,
        "visible_project_ids": [PROJECT],
        "scope_project_id": None,
        "origins": ["user"],
        "project_ids": [PROJECT],
        "has_project": None,
        "include_archived_projects": False,
        "search": None,
        "as_of": STAMP,
    }
    reads_wire.audit.get_audit_count.assert_not_awaited()
    reads_wire.audit.get_audit_counts.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path,params,status,fragment",
    [
        (
            "/api/jobs",
            {"user_id": JOB, "status": "invalid", "origin": "invalid"},
            403,
            "other users",
        ),
        (
            "/api/jobs",
            {
                "status": "invalid",
                "origin": "invalid",
                "offset": job_queries_module.JOBS_MAX_OFFSET + 1,
            },
            422,
            "Unknown job status",
        ),
        (
            "/api/jobs",
            {
                "origin": "invalid",
                "offset": job_queries_module.JOBS_MAX_OFFSET + 1,
                "project_id": "invalid",
            },
            422,
            "Unknown job origin",
        ),
        (
            "/api/jobs",
            {"offset": job_queries_module.JOBS_MAX_OFFSET + 1, "project_id": "invalid"},
            400,
            "exceeds the maximum",
        ),
        (
            STATS,
            {"origin": "invalid", "project_id": "invalid"},
            422,
            "Unknown job origin",
        ),
    ],
)
async def test_handler_refusal_order_precedes_visibility_and_storage(
    reads_wire, path, params, status, fragment
):
    response = await get(reads_wire, path, params)
    assert response.status_code == status
    assert fragment in response.json()["detail"]
    reads_wire.visible.assert_not_awaited()
    reads_wire.db.query_jobs.assert_not_awaited()
    reads_wire.db.get_job_statistics.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", [DETAIL, PROJECT_JOBS])
async def test_access_refusal_precedes_audit_and_project_query(reads_wire, path):
    reads_wire.audit.is_available = True
    response = await get(reads_wire, path, {"status": "invalid"}, authenticated=False)
    assert response.status_code == 401
    assert response.json() == {"detail": "Authentication required"}
    reads_wire.audit.get_audit_count.assert_not_awaited()
    reads_wire.audit.get_audit_counts.assert_not_awaited()
    reads_wire.db.acquire.assert_not_called()
    reads_wire.db.get_project.assert_not_awaited()


@pytest.mark.asyncio
async def test_project_status_rejection_runs_after_membership_before_sql(reads_wire):
    response = await get(reads_wire, PROJECT_JOBS, {"status": "invalid"})
    assert response.status_code == 422
    assert "Unknown job status/outcome" in response.json()["detail"]
    assert reads_wire.events == ["approved", "project_member"]
    reads_wire.db.acquire.assert_not_called()
    reads_wire.db.get_project.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", [PROJECT_JOBS, STATS])
async def test_framework_query_validation_still_precedes_authentication(
    reads_wire, path
):
    params = {"limit": 0} if path == PROJECT_JOBS else {"has_project": "invalid"}
    response = await get(reads_wire, path, params, authenticated=False)
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"][0] == "query"
    assert reads_wire.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("path", [DETAIL, PROJECT_JOBS, STATS])
@pytest.mark.parametrize("http_error", [False, True])
async def test_read_failures_keep_http_exception_or_existing_string_detail(
    reads_wire, path, http_error
):
    reads_wire.audit.is_available = True
    collaborator = {
        DETAIL: reads_wire.audit.get_audit_count,
        PROJECT_JOBS: reads_wire.audit.get_audit_counts,
        STATS: reads_wire.db.get_job_statistics,
    }[path]
    collaborator.side_effect = (
        HTTPException(503, "synthetic read refusal")
        if http_error
        else RuntimeError("synthetic read failure")
    )
    response = await get(reads_wire, path)
    assert response.status_code == (503 if http_error else 500)
    assert response.json() == {
        "detail": "synthetic read refusal" if http_error else "synthetic read failure"
    }


def isolated_app(label, owner, count):
    """Compose a real router twice, with no collaborators taken from main."""
    app = FastAPI(default_response_class=http_composition.CustomJSONResponse)
    app.include_router(router)
    row = job_row(user_id=owner, existing_extension={"application": label})
    conn = SimpleNamespace(fetch=AsyncMock(return_value=[row]))
    acquired = MagicMock()
    acquired.__aenter__ = AsyncMock(return_value=conn)
    acquired.__aexit__ = AsyncMock(return_value=None)
    store = SimpleNamespace(
        acquire=MagicMock(return_value=acquired),
        get_project=AsyncMock(return_value={"main_cloud_folder_handle": None}),
        query_jobs=AsyncMock(
            return_value=SimpleNamespace(
                jobs=[row], total=count, total_is_capped=False, has_more=False
            )
        ),
        get_job_statistics=AsyncMock(return_value={"total_jobs": count}),
    )
    audit = SimpleNamespace(
        is_available=True,
        get_audit_count=AsyncMock(return_value=count),
        get_audit_counts=AsyncMock(return_value={JOB: count}),
    )

    async def authenticate(request, auth_store):
        assert request.app is app
        assert auth_store is store
        # Let the other app's request run after resolving this app's factory.
        await asyncio.sleep(0)
        return {"id": owner, "is_admin": False}

    async def job_access(request, auth_store, job_id):
        principal = await authenticate(request, auth_store)
        assert job_id == JOB
        return principal, copy.deepcopy(row)

    async def project_member(request, auth_store, project_id):
        principal = await authenticate(request, auth_store)
        assert project_id == PROJECT
        return principal, {"id": PROJECT}

    approved = AsyncMock(side_effect=authenticate)
    job_gate = AsyncMock(side_effect=job_access)
    project_gate = AsyncMock(side_effect=project_member)
    queries = JobQueryDependencies(
        query_jobs=store.query_jobs,
        get_job_statistics=store.get_job_statistics,
        visible_project_ids=AsyncMock(return_value=[PROJECT]),
        scope_project_id=lambda _user: None,
        audit_available=lambda: audit.is_available,
        audit_counts=audit.get_audit_counts,
        project_job=dict,
        now=lambda: STAMP,
        status_filter_values=("completed",),
        known_origins=frozenset({"user"}),
    )
    reads = JobReadDependencies(
        store=store,
        audit_reader=audit,
        redact_job=dict,
        with_cloud_review_mode=dict,
        status_filter_values=("completed",),
    )
    dependencies = JobReadsDependencies(
        store=store,
        queries=queries,
        reads=reads,
        require_approved_user=approved,
        require_job_access=job_gate,
        require_project_member=project_gate,
    )
    factory = MagicMock(return_value=dependencies)
    app.state.job_reads_dependencies_factory = factory
    return SimpleNamespace(
        app=app,
        store=store,
        factory=factory,
        approved=approved,
        job_gate=job_gate,
        project_gate=project_gate,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/api/jobs", DETAIL, PROJECT_JOBS, STATS])
async def test_job_read_router_isolates_two_apps_and_resolves_each_request(
    monkeypatch, path
):
    forbidden_factory = MagicMock(
        side_effect=AssertionError("global application accessed")
    )
    monkeypatch.setattr(jobs_composition, "job_reads_dependencies", forbidden_factory)
    first = isolated_app("first", USER, 3)
    second = isolated_app("second", CHILD, 8)
    responses = await asyncio.gather(get(first, path), get(second, path))
    for app, response, label, owner, count in zip(
        (first, second), responses, ("first", "second"), (USER, CHILD), (3, 8)
    ):
        assert response.status_code == 200, response.text
        body = response.json()
        if path == STATS:
            assert body == {"total_jobs": count}
            assert (
                app.store.get_job_statistics.call_args.kwargs["owner_user_id"] == owner
            )
        else:
            item = (
                body["jobs"][0]
                if path == "/api/jobs"
                else body[0]
                if path == PROJECT_JOBS
                else body
            )
            assert item["audit_count"] == count
            assert item["existing_extension"] == {"application": label}
            assert item["user_id"] == owner
        app.factory.assert_called_once_with()
        if path == DETAIL:
            app.job_gate.assert_awaited_once()
            app.approved.assert_not_awaited()
            app.project_gate.assert_not_awaited()
        elif path == PROJECT_JOBS:
            app.project_gate.assert_awaited_once()
            app.job_gate.assert_not_awaited()
            app.approved.assert_not_awaited()
        else:
            app.approved.assert_awaited_once()
            app.job_gate.assert_not_awaited()
            app.project_gate.assert_not_awaited()
    # Replacement is local to one app and visible on its next request, rather
    # than a router-import-time capture or a process-global dependency override.
    replacement = MagicMock(return_value=first.factory.return_value)
    first.app.state.job_reads_dependencies_factory = replacement
    response = await get(first, path)
    assert response.status_code == 200
    replacement.assert_called_once_with()
    second.factory.assert_called_once_with()
    forbidden_factory.assert_not_called()


@pytest.mark.asyncio
async def test_unconfigured_read_router_never_falls_back_to_main(monkeypatch):
    forbidden_factory = MagicMock(
        side_effect=AssertionError("global application accessed")
    )
    monkeypatch.setattr(jobs_composition, "job_reads_dependencies", forbidden_factory)
    app = FastAPI(default_response_class=http_composition.CustomJSONResponse)
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://unconfigured.test",
    ) as client:
        response = await client.get("/api/jobs")
    assert response.status_code == 500
    forbidden_factory.assert_not_called()
