"""Inspect authorized jobs and session children without owning HTTP or lifecycle.

The store's roster queries and canonical liveness computation remain the
policy authorities. Active-job browsing deliberately retains its own admin
owner-only contract, distinct from the fleet list's admin visibility.
"""

from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass
import json
from typing import Any, Literal, Protocol
from uuid import UUID

from fastapi import HTTPException

from orchestrator.services.subagent_projection import subagent_thread_payload


ME_ACTIVE_JOB_STATUSES = {"created", "processing", "paused", "pending_review"}


class ActiveJobQueryResult(Protocol):
    jobs: list[dict[str, Any]]


class JobInspectionStore(Protocol):
    async def get_job(self, job_id: str) -> dict[str, Any] | None: ...
    async def get_agent(self, agent_id: str) -> dict[str, Any] | None: ...
    async def get_job_progress(self, job_id: str) -> dict[str, Any] | None: ...
    async def get_job_subjob_roster(self, job_id: str) -> list[dict[str, Any]]: ...
    async def list_subagent_threads(self, job_id: str) -> list[dict[str, Any]]: ...
    async def list_session_subagent_threads(
        self, thread_id: str
    ) -> list[dict[str, Any]]: ...
    async def get_projects_for_user(
        self,
        user_id: str,
        limit: int = 100,
        statuses: list[str] | None = None,
    ) -> list[dict[str, Any]]: ...
    async def query_jobs(
        self,
        *,
        owner_user_id: str | None,
        visible_project_ids: list[str] | None,
        scope_project_id: str | None,
        statuses: list[str],
        user_id: str | None,
        limit: int,
        include_total: bool,
    ) -> ActiveJobQueryResult: ...


class LivenessAuditReader(Protocol):
    @property
    def is_available(self) -> bool: ...
    async def get_audit_time_range(self, job_id: str) -> dict[str, Any] | None: ...


@dataclass(frozen=True)
class JobInspectionDependencies:
    store: JobInspectionStore
    audit_reader: LivenessAuditReader
    user_visible_project_ids: Callable[
        [dict[str, Any], JobInspectionStore], Awaitable[set[UUID] | Literal["all"]]
    ]
    mcp_scope_project_id: Callable[[dict[str, Any]], UUID | None]
    active_job_statuses: Collection[str]


async def get_job_progress(
    *,
    job_id: str,
    authorized_job: dict[str, Any],
    dependencies: JobInspectionDependencies,
) -> dict[str, Any]:
    """Run the existing read after its transport authorization has completed."""
    from orchestrator.services.job_liveness import compute_job_liveness
    from orchestrator.services.job_projection import workspace_recovery_projection

    try:
        progress = await dependencies.store.get_job_progress(job_id)
        if not progress:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        liveness = await compute_job_liveness(
            authorized_job,
            audit_reader=dependencies.audit_reader,
            db=dependencies.store,
        )
        return {
            **progress,
            **liveness,
            "workspace_recovery": workspace_recovery_projection(authorized_job),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def get_job_subjobs(
    *, job_id: str, dependencies: JobInspectionDependencies
) -> dict[str, Any]:
    """Run the existing read after its transport authorization has completed."""
    try:
        rows = await dependencies.store.get_job_subjob_roster(job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    return {
        "job_id": job_id,
        "count": len(rows),
        "subjobs": [
            {
                "id": str(row["id"]),
                "parent_job_id": str(row["parent_job_id"])
                if row.get("parent_job_id")
                else None,
                # 0 = a direct child. Present so a nested renderer does not have
                # to rebuild the tree from parent ids it may only partly hold.
                "depth": row["depth"],
                "description": row["description"],
                "status": row["status"],
                # The role label: 'scholar', 'critic', 'curator'. This is what
                # makes a roster row readable at a glance, and it is the same
                # value the list's own child rows badge themselves with.
                "config_name": row["config_name"],
                "origin": row["origin"],
                "error_message": row["error_message"],
                "created_at": row["created_at"],
                "completed_at": row["completed_at"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ],
    }


async def get_job_subagents(
    *, job_id: str, dependencies: JobInspectionDependencies
) -> dict[str, Any]:
    """Run the existing read after its transport authorization has completed."""
    try:
        rows = await dependencies.store.list_subagent_threads(job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    return {
        "job_id": job_id,
        "count": len(rows),
        "subagents": [subagent_thread_payload(row) for row in rows],
    }


async def get_session_subagents(
    *,
    thread_id: str,
    authorized_parent: dict[str, Any],
    dependencies: JobInspectionDependencies,
) -> dict[str, Any]:
    """Run the existing read after its transport authorization has completed."""
    if str(authorized_parent.get("kind") or "session") != "session":
        raise HTTPException(status_code=404, detail="Parent session not found")
    try:
        rows = await dependencies.store.list_session_subagent_threads(thread_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {
        "parent_thread_id": thread_id,
        "count": len(rows),
        "subagents": [subagent_thread_payload(row) for row in rows],
    }


async def get_job_brief(
    *, job_id: str, dependencies: JobInspectionDependencies
) -> dict[str, Any]:
    """Run the existing read after its transport authorization has completed."""
    job = await dependencies.store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    context = job.get("context") or {}
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (json.JSONDecodeError, TypeError):
            context = {}

    return {
        "description": job.get("description") or "",
        "required_deliverables": context.get("required_deliverables"),
        "kickoff_message": context.get("kickoff_message"),
    }


async def list_my_active_jobs(
    *, user: dict[str, Any], limit: int, dependencies: JobInspectionDependencies
) -> list[dict[str, Any]]:
    """Run the existing read after its transport authorization has completed."""
    is_admin = bool(user.get("is_admin"))
    scope_pid = dependencies.mcp_scope_project_id(user)
    try:
        if is_admin:
            # Admins get their own in-flight jobs here, not the fleet — the
            # fleet view is /api/agents. Expressed as an owner filter rather
            # than the visibility OR so it stays own-jobs-only.
            owner_user_id = None
            project_ids = None
            owner_filter = str(user["id"])
        else:
            visible = await dependencies.user_visible_project_ids(
                user, dependencies.store
            )
            owner_user_id = str(user["id"])
            project_ids = [str(p) for p in visible] if visible != "all" else []
            owner_filter = None

        result = await dependencies.store.query_jobs(
            owner_user_id=owner_user_id,
            visible_project_ids=project_ids,
            scope_project_id=str(scope_pid) if scope_pid else None,
            statuses=sorted(dependencies.active_job_statuses),
            user_id=owner_filter,
            limit=limit,
            include_total=False,
        )
        # Filtering in SQL is what makes ?limit= mean "up to N active jobs"
        # rather than "the active ones among the newest N of any status".
        # The post-filter stays as the gate: it is what a caller-side test
        # pins, and it costs nothing once the query already narrowed.
        return [
            j
            for j in result.jobs
            if j.get("status") in dependencies.active_job_statuses
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
