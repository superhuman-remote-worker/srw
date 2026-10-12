"""``POST /api/projects/{project_id}/jobs`` — create a job inside a project.

Moved from ``orchestrator.main`` (R1.B12) with its explicit operation id. It
delegates to the shared REST admission adapter with the application's job
lifecycle dependencies (one build per request serves both the member gate and
the admission, as the former module read one store for both); the router is included last, where
the route used to be declared, so route order is unchanged.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from orchestrator.routers import job_lifecycle as job_lifecycle_routes
from orchestrator.schemas.job_create import PublicJobCreateBody

router = APIRouter()


@router.post(
    "/api/projects/{project_id}/jobs",
    operation_id="create_project_job_api_projects__project_id__jobs_post",
)
async def create_project_job(
    request: Request, project_id: str, job: PublicJobCreateBody
) -> dict[str, Any]:
    """Create a job within a project — delegates to create_job. Requires editor or higher."""
    dependencies = request.app.state.job_lifecycle_route_dependencies_factory()
    await dependencies.require_project_member(
        request,
        dependencies.store,
        project_id,
        min_role="editor",
        allow_archived=False,
    )
    job.project_id = project_id
    return await job_lifecycle_routes.admit_job_request(
        request, job, dependencies=dependencies
    )


__all__ = ["create_project_job", "router"]
