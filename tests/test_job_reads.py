"""Authorized detail/project reads preserve enrichment, SQL, and error ordering."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

from fastapi import HTTPException, Query
import pytest

from orchestrator.services.job_reads import (
    JobReadDependencies,
    read_job,
    read_project_jobs,
)


STATUSES = ("created", "processing", "completed", "cancelled", "blocked_undelivered")
PROJECT = "10000000-0000-4000-8000-000000000001"
JOB = "20000000-0000-4000-8000-000000000002"
OTHER_JOB = "30000000-0000-4000-8000-000000000003"
PROJECT_SQL = (
    "SELECT js.*, jcr.delivery_status, jcr.delivery_ref, "
    "jcr.delivery_sha, jcr.record_type AS change_record_type, "
    "j.diff_status, j.cloud_diff_baseline_commit, "
    "j.context->'cloud_checkout' AS cloud_checkout "
    "FROM job_summary js "
    "JOIN jobs j ON j.id = js.id "
    "LEFT JOIN job_change_records jcr ON jcr.job_id = js.id "
    "WHERE js.project_id = $1"
)


@pytest.fixture
def reads():
    events = []
    rows = [
        {
            "id": UUID(JOB),
            "config_override": {"safe": 1},
            "extension": {"nullable": None},
        },
        {"id": UUID(OTHER_JOB), "config_override": None, "extension": 4},
    ]
    conn = SimpleNamespace(fetch=AsyncMock(return_value=rows))

    @asynccontextmanager
    async def acquire():
        events.append("acquire")
        try:
            yield conn
        finally:
            events.append("release")

    async def get_project(_project_id):
        events.append("project")
        return {"id": PROJECT, "main_cloud_folder_handle": "opaque"}

    async def get_audit_counts(job_ids):
        events.append("audit_batch")
        return {JOB: 3}

    def redact(job):
        events.append("redact")
        assert "audit_count" in job
        assert "project_has_cloud_folder" not in job
        return dict(job)

    def cloud(job):
        events.append("cloud")
        value = dict(job)
        value["cloud_review_mode"] = (
            "diff" if value.pop("project_has_cloud_folder", False) else "open_folder"
        )
        return value

    store = SimpleNamespace(
        acquire=Mock(side_effect=acquire),
        get_project=AsyncMock(side_effect=get_project),
    )
    audit = SimpleNamespace(
        is_available=True,
        get_audit_count=AsyncMock(return_value=7),
        get_audit_counts=AsyncMock(side_effect=get_audit_counts),
    )
    deps = JobReadDependencies(
        store=store,
        audit_reader=audit,
        redact_job=Mock(side_effect=redact),
        with_cloud_review_mode=Mock(side_effect=cloud),
        status_filter_values=STATUSES,
    )
    return SimpleNamespace(
        deps=deps, store=store, audit=audit, conn=conn, rows=rows, events=events
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("available,count", [(False, None), (True, 0), (True, 7)])
async def test_detail_reads_exact_authorized_row_and_preserves_optional_audit(
    reads, available, count
):
    reads.audit.is_available = available
    reads.audit.get_audit_count.return_value = count
    row = {"id": JOB, "extension": {"unmodeled": True}}

    result = await read_job(job_id=JOB, authorized_job=row, dependencies=reads.deps)

    assert result == {**row, "audit_count": count, "cloud_review_mode": "open_folder"}
    # Existing access-row mutation remains observable to in-process callers.
    assert row["audit_count"] == count
    assert reads.deps.redact_job.call_args.args[0] is row
    assert reads.events == ["redact", "cloud"]
    reads.store.acquire.assert_not_called()
    reads.store.get_project.assert_not_awaited()
    reads.audit.get_audit_counts.assert_not_awaited()
    if available:
        reads.audit.get_audit_count.assert_awaited_once_with(JOB)
    else:
        reads.audit.get_audit_count.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["audit", "redact", "cloud"])
@pytest.mark.parametrize("http_error", [False, True])
async def test_detail_error_mapping_keeps_http_errors_and_stops_later_work(
    reads, failure_stage, http_error
):
    error = (
        HTTPException(status_code=409, detail={"reason": "existing"})
        if http_error
        else ValueError("read unavailable")
    )
    target = {
        "audit": reads.audit.get_audit_count,
        "redact": reads.deps.redact_job,
        "cloud": reads.deps.with_cloud_review_mode,
    }[failure_stage]
    target.side_effect = error

    with pytest.raises(HTTPException) as caught:
        await read_job(job_id=JOB, authorized_job={"id": JOB}, dependencies=reads.deps)

    if http_error:
        assert caught.value is error
    else:
        assert caught.value.status_code == 500
        assert caught.value.detail == "read unavailable"
        assert caught.value.__cause__ is error
    if failure_stage == "audit":
        reads.deps.redact_job.assert_not_called()
    if failure_stage != "cloud":
        reads.deps.with_cloud_review_mode.assert_not_called()


@pytest.mark.asyncio
async def test_detail_task_cancellation_is_not_translated(reads):
    reads.audit.get_audit_count.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await read_job(job_id=JOB, authorized_job={}, dependencies=reads.deps)
    reads.deps.redact_job.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, suffix, params",
    [
        (None, " ORDER BY js.created_at DESC LIMIT $2", (PROJECT, 17)),
        (Query(default=None), " ORDER BY js.created_at DESC LIMIT $2", (PROJECT, 17)),
        (
            "processing",
            " AND js.status = $2 ORDER BY js.created_at DESC LIMIT $3",
            (PROJECT, "processing", 17),
        ),
        (
            "cancelled",
            " AND js.status = $2 AND js.completion_outcome_kind IS DISTINCT FROM 'blocked_undelivered' ORDER BY js.created_at DESC LIMIT $3",
            (PROJECT, "cancelled", 17),
        ),
        (
            "blocked_undelivered",
            " AND js.completion_outcome_kind = $2 ORDER BY js.created_at DESC LIMIT $3",
            (PROJECT, "blocked_undelivered", 17),
        ),
    ],
)
async def test_project_reads_keep_distinct_unpaged_summary_sql(
    reads, status, suffix, params
):
    before = deepcopy(reads.rows)

    result = await read_project_jobs(
        project_id=PROJECT, status=status, limit=17, dependencies=reads.deps
    )

    reads.conn.fetch.assert_awaited_once_with(PROJECT_SQL + suffix, *params)
    assert reads.rows == before
    assert isinstance(result, list)
    assert [job["id"] for job in result] == [UUID(JOB), UUID(OTHER_JOB)]
    assert [job["audit_count"] for job in result] == [3, 0]
    assert [job["cloud_review_mode"] for job in result] == ["diff", "diff"]
    assert result[0]["extension"] == {"nullable": None}
    assert result[1]["extension"] == 4
    assert reads.events == [
        "acquire",
        "release",
        "project",
        "audit_batch",
        "redact",
        "cloud",
        "redact",
        "cloud",
    ]
    reads.audit.get_audit_counts.assert_awaited_once_with([JOB, OTHER_JOB])
    reads.audit.get_audit_count.assert_not_awaited()
    reads.store.get_project.assert_awaited_once_with(PROJECT)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["", "unknown", "completed OR true"])
async def test_invalid_project_status_is_refused_before_storage(reads, status):
    with pytest.raises(HTTPException) as caught:
        await read_project_jobs(
            project_id=PROJECT, status=status, limit=17, dependencies=reads.deps
        )
    assert caught.value.status_code == 422
    assert (
        caught.value.detail
        == f"Unknown job status/outcome: {status}. Valid values: {', '.join(STATUSES)}"
    )
    reads.store.acquire.assert_not_called()
    reads.store.get_project.assert_not_awaited()
    reads.audit.get_audit_counts.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("available", [False, True])
async def test_empty_project_still_resolves_cloud_then_optional_batched_audit(
    reads, available
):
    reads.conn.fetch.return_value = []
    reads.audit.is_available = available
    assert (
        await read_project_jobs(
            project_id=PROJECT, status=None, limit=100, dependencies=reads.deps
        )
        == []
    )
    reads.store.get_project.assert_awaited_once_with(PROJECT)
    if available:
        reads.audit.get_audit_counts.assert_awaited_once_with([])
        assert reads.events == ["acquire", "release", "project", "audit_batch"]
    else:
        reads.audit.get_audit_counts.assert_not_awaited()
        assert reads.events == ["acquire", "release", "project"]
    reads.deps.redact_job.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "project",
    [None, {}, {"main_cloud_folder_handle": None}, {"main_cloud_folder_handle": ""}],
)
async def test_project_without_cloud_folder_uses_open_folder_and_unavailable_audit_is_null(
    reads, project
):
    reads.store.get_project.side_effect = None
    reads.store.get_project.return_value = project
    reads.audit.is_available = False
    result = await read_project_jobs(
        project_id=PROJECT, status=None, limit=100, dependencies=reads.deps
    )
    assert [job["cloud_review_mode"] for job in result] == [
        "open_folder",
        "open_folder",
    ]
    assert [job["audit_count"] for job in result] == [None, None]
    reads.audit.get_audit_counts.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_stage", ["fetch", "project", "audit", "redact", "cloud"]
)
@pytest.mark.parametrize("http_error", [False, True])
async def test_project_error_mapping_keeps_http_errors_and_releases_connection(
    reads, failure_stage, http_error
):
    error = (
        HTTPException(status_code=503, detail="existing unavailable")
        if http_error
        else ValueError("project read unavailable")
    )
    target = {
        "fetch": reads.conn.fetch,
        "project": reads.store.get_project,
        "audit": reads.audit.get_audit_counts,
        "redact": reads.deps.redact_job,
        "cloud": reads.deps.with_cloud_review_mode,
    }[failure_stage]
    target.side_effect = error

    with pytest.raises(HTTPException) as caught:
        await read_project_jobs(
            project_id=PROJECT, status=None, limit=100, dependencies=reads.deps
        )

    if http_error:
        assert caught.value is error
    else:
        assert caught.value.status_code == 500
        assert caught.value.detail == "project read unavailable"
        assert caught.value.__cause__ is error
    assert reads.events[:2] == ["acquire", "release"]
    if failure_stage == "fetch":
        reads.store.get_project.assert_not_awaited()
    if failure_stage in ("fetch", "project"):
        reads.audit.get_audit_counts.assert_not_awaited()
    if failure_stage in ("fetch", "project", "audit"):
        reads.deps.redact_job.assert_not_called()
    if failure_stage != "cloud":
        reads.deps.with_cloud_review_mode.assert_not_called()


@pytest.mark.asyncio
async def test_project_fetch_cancellation_releases_connection_without_error_translation(
    reads,
):
    reads.conn.fetch.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await read_project_jobs(
            project_id=PROJECT, status=None, limit=100, dependencies=reads.deps
        )
    assert reads.events == ["acquire", "release"]
    reads.store.get_project.assert_not_awaited()


@pytest.mark.asyncio
async def test_read_dependencies_do_not_share_audit_or_projection_state(reads):
    other_audit = SimpleNamespace(
        is_available=False, get_audit_count=AsyncMock(), get_audit_counts=AsyncMock()
    )
    other = replace(
        reads.deps,
        audit_reader=other_audit,
        with_cloud_review_mode=lambda job: {**job, "other": True},
    )
    first, second = await asyncio.gather(
        read_job(job_id=JOB, authorized_job={"id": JOB}, dependencies=reads.deps),
        read_job(
            job_id=OTHER_JOB, authorized_job={"id": OTHER_JOB}, dependencies=other
        ),
    )
    assert first["audit_count"] == 7 and "other" not in first
    assert second["audit_count"] is None and second["other"] is True
    other_audit.get_audit_count.assert_not_awaited()
