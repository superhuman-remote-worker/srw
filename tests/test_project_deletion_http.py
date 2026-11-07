"""The HTTP face of a decided project delete: 202 while it finishes, and a
409 for a write that lost the race with the decision."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import httpx
import pytest
from fastapi import FastAPI

from orchestrator.application import http as http_application
from orchestrator.routers import projects as projects_router
from orchestrator.services.project_status import PROJECT_DELETING_DETAIL

PROJECT_ID = "44444444-4444-4444-8444-444444444444"


def _violation(constraint):
    exc = asyncpg.CheckViolationError("violates")
    exc.constraint_name = constraint
    return exc


@pytest.mark.asyncio
async def test_the_fence_answers_409_with_the_gates_sentence():
    response = await http_application.project_deletion_fence_handler(
        SimpleNamespace(), _violation("project_deletion_in_progress")
    )
    assert response.status_code == 409
    assert response.body == b'{"detail":"' + PROJECT_DELETING_DETAIL.encode() + b'"}'


@pytest.mark.asyncio
async def test_any_other_check_violation_is_not_mapped():
    other = _violation("managed_repository_cleanup_required")
    with pytest.raises(asyncpg.CheckViolationError) as raised:
        await http_application.project_deletion_fence_handler(SimpleNamespace(), other)
    assert raised.value is other


async def _delete_status(monkeypatch, result):
    calls = {}

    async def delete_project(project_id, **kwargs):
        calls.update(kwargs, project_id=project_id)
        return result

    monkeypatch.setattr(projects_router.projects, "delete_project", delete_project)
    owner = AsyncMock(return_value=({"id": "user-1"}, {"id": PROJECT_ID}))
    app = FastAPI()
    app.state.projects_dependencies_factory = (
        lambda: projects_router.ProjectsDependencies(
            store=object(),
            operations=object(),
            require_admin=AsyncMock(),
            require_approved_user=AsyncMock(),
            require_project_owner=owner,
        )
    )
    app.include_router(projects_router.router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.delete(f"/api/projects/{PROJECT_ID}")
    assert owner.await_args.kwargs == {"allow_deleting": True}
    assert calls["requested_by"] == "user-1"
    return response


@pytest.mark.asyncio
async def test_a_finished_delete_is_200(monkeypatch):
    response = await _delete_status(monkeypatch, {"status": "deleted"})
    assert response.status_code == 200
    assert response.json() == {"status": "deleted"}


@pytest.mark.asyncio
async def test_a_delete_that_finishes_in_the_background_is_202(monkeypatch):
    response = await _delete_status(
        monkeypatch, {"status": "deleting", "detail": "retried in the background"}
    )
    assert response.status_code == 202
    assert response.json()["status"] == "deleting"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result,status",
    [
        ({"deleted": True, "uid": PROJECT_ID}, 200),
        ({"deleted": False, "deleting": True, "uid": PROJECT_ID, "detail": "x"}, 202),
    ],
)
async def test_a_manifest_project_delete_answers_like_the_project_delete(
    result, status
):
    from orchestrator.routers import manifests as manifests_router

    resources = SimpleNamespace(delete=AsyncMock(return_value=result))
    app = FastAPI()
    app.state.manifest_dependencies_factory = lambda: (
        manifests_router.ManifestDependencies(
            service=object(),
            require_approved_user=AsyncMock(return_value={"id": "user-1"}),
            resources=resources,
        )
    )
    app.include_router(manifests_router.router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.delete(
            f"/api/resources/{PROJECT_ID}", params={"expected_version": 3}
        )
    assert response.status_code == status
    assert response.json() == result
