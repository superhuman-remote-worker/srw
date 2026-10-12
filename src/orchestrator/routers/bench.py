"""Authenticated HTTP surface and lifecycle wiring for Job Bench v1."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from typing import Any, Awaitable, Callable, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from orchestrator.schemas.job_create import JobCreate
from orchestrator.security.access import mcp_scope_project_id, require_project_member
from orchestrator.security.auth import require_approved_user
from orchestrator.services.agent_pod_entrypoint import validate_config_name
from orchestrator.services.automations import validate_automation_expert_selection
from orchestrator.services.bench import (
    BenchStateError,
    BenchStore,
    bench_sweeper_loop,
    build_bench_job_payload,
    cancel_bench_run,
    compute_bench_report,
    create_bench_run,
)
from orchestrator.services.default_experts import ExpertSelectionError


class BenchTaskSpec(BaseModel):
    """One fully inline benchmark task."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., min_length=1, max_length=200)
    family: str | None = Field(None, max_length=100)
    description: str = Field(..., min_length=1)
    required_deliverables: list[str] = Field(default_factory=list)
    config_name: str = Field("worker_base", min_length=1, max_length=300)
    config_override: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(5, ge=0, le=10)

    # A run's specs are persisted before any job is created, and every
    # replicate later hands this word to the agent pod's entrypoint. Same
    # allow-list, applied on write, so the 422 names the spec instead of a
    # sweeper failing task after task.
    @field_validator("config_name")
    @classmethod
    def _bundled_config_selector(cls, value: str) -> str:
        return validate_config_name(value)


class BenchArmSpec(BaseModel):
    """One config/model arm applied to every task."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=200)
    config_name: str | None = Field(None, min_length=1, max_length=300)
    expert_id: UUID | None = None
    project_id: UUID | None = Field(
        None,
        description=(
            "Project used by this arm. Omitted arms inherit the run project. "
            "Per-arm projects keep memory state from coupling A/B treatments."
        ),
    )
    config_override: dict[str, Any] = Field(default_factory=dict)
    model: str = Field(..., min_length=1, max_length=300)
    execution_lane: Literal["pinned", "stateless"] | None = Field(
        None,
        description=(
            "Execution plane for this arm. The lane is a top-level JobCreate "
            "field rather than config, so it cannot ride in config_override — "
            "a lane A/B has to name it here. Omitted keeps the JobCreate "
            "default (roots stay pinned). Requesting 'stateless' does not "
            "bypass admission: create_job runs the same gate for the bench's "
            "in-process call as for a user request, so a task whose workspace "
            "needs a VM is pinned by capability whatever the arm asks for."
        ),
    )

    @field_validator("config_name")
    @classmethod
    def _bundled_config_selector(cls, value: str | None) -> str | None:
        return validate_config_name(value)

    @model_validator(mode="after")
    def one_expert_source(self) -> "BenchArmSpec":
        if self.config_name and self.expert_id:
            raise ValueError("An arm may set config_name or expert_id, not both")
        return self


class BenchRunCreate(BaseModel):
    """Request body for ``POST /api/bench/runs``."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=300)
    tasks: list[BenchTaskSpec] = Field(..., min_length=1, max_length=500)
    replicates: int = Field(3, ge=1, le=100)
    max_in_flight: int = Field(2, ge=1, le=100)
    arms: list[BenchArmSpec] = Field(..., min_length=1, max_length=50)
    project_id: UUID | None = None

    @model_validator(mode="after")
    def unique_names(self) -> "BenchRunCreate":
        task_ids = [task.id for task in self.tasks]
        if len(task_ids) != len(set(task_ids)):
            raise ValueError("Task ids must be unique within a bench run")
        arm_names = [arm.name for arm in self.arms]
        if len(arm_names) != len(set(arm_names)):
            raise ValueError("Arm names must be unique within a bench run")
        return self


CreateBenchJob = Callable[[str, JobCreate], Awaitable[dict[str, Any]]]
ValidateToolOverrides = Callable[[dict[str, Any] | None], dict[str, Any] | None]
ResolveJobRepo = Callable[[str], Awaitable[tuple[str, str | None]]]
CancelBenchJob = Callable[[str, dict[str, Any]], Awaitable[Any]]


@dataclass(frozen=True)
class BenchDependencies:
    store: BenchStore
    create_job: CreateBenchJob
    validate_tool_overrides: ValidateToolOverrides | None = None
    audit_reader: Any | None = None
    forge: Any | None = None
    resolve_job_repo: ResolveJobRepo | None = None
    cancel_job: CancelBenchJob | None = None


def _request_dependencies(request: Request) -> BenchDependencies:
    """Resolve the collaborators owned by the mounted application."""

    factory = getattr(request.app.state, "bench_dependencies_factory", None)
    if not callable(factory):
        raise RuntimeError("Bench application dependencies are not configured")
    return factory()


async def _create_job_through_admission(
    run: dict[str, Any],
    task: dict[str, Any],
    arm: dict[str, Any],
    replicate: int,
    *,
    create_job: CreateBenchJob,
) -> dict[str, Any]:
    """Submit the frozen payload through application-owned job admission."""

    created_by = str(run["created_by"])
    payload = build_bench_job_payload(run, task, arm, replicate)
    return await create_job(
        created_by,
        JobCreate(**payload),
    )


@asynccontextmanager
async def _bench_lifespan(app: Any):
    """Start after the app DB lifespan and drain before DB shutdown."""

    dependencies = app.state.bench_dependencies_factory()

    shutdown_event = asyncio.Event()
    task = asyncio.create_task(
        bench_sweeper_loop(
            dependencies.store,
            shutdown_event,
            create_job_fn=partial(
                _create_job_through_admission, create_job=dependencies.create_job
            ),
        )
    )
    try:
        yield
    finally:
        shutdown_event.set()
        await task


router = APIRouter(
    prefix="/api/bench",
    tags=["Job Bench"],
    lifespan=_bench_lifespan,
)


async def _visible_run_or_404(
    store: BenchStore,
    run_id: str,
    caller: dict[str, Any],
) -> dict[str, Any]:
    run = await store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Bench run not found")
    if not caller.get("is_admin") and str(run["created_by"]) != str(caller["id"]):
        # Match neighboring owner-scoped resources: do not disclose existence.
        raise HTTPException(status_code=404, detail="Bench run not found")
    return run


@router.post("/runs", status_code=status.HTTP_201_CREATED)
async def create_run(request: Request, body: BenchRunCreate) -> dict[str, Any]:
    """Freeze and start a creator-owned benchmark run."""

    dependencies = _request_dependencies(request)
    database = dependencies.store.db
    validate_tool_overrides = dependencies.validate_tool_overrides
    if validate_tool_overrides is None:
        raise RuntimeError("Bench tool validation is not configured")
    caller = await require_approved_user(request, database)
    payload = body.model_dump(mode="python")

    # Match POST /api/jobs' scope rules, but resolve the project now so it is
    # part of the frozen row instead of following a changed user default later.
    requested_project = str(body.project_id) if body.project_id else None
    scoped_project = mcp_scope_project_id(caller)
    if scoped_project is not None:
        scoped_project_id = str(scoped_project)
        if requested_project and requested_project != scoped_project_id:
            raise HTTPException(
                status_code=403,
                detail="Bench run is outside the token's project scope",
            )
        requested_project = requested_project or scoped_project_id
    if not requested_project and caller.get("default_project_id"):
        requested_project = str(caller["default_project_id"])
    if requested_project:
        await require_project_member(
            request,
            database,
            requested_project,
            min_role="editor",
            allow_archived=False,
        )
    payload["project_id"] = requested_project

    # A paired run must be able to isolate each arm's project memory without
    # splitting the arms into separate (and therefore non-adjacent) runs. Each
    # explicit arm project receives the same scope/membership check as the run
    # default; an omitted value inherits the already-validated run project.
    checked_arm_projects: set[str] = set()
    for arm in payload["arms"]:
        arm_project = arm.get("project_id")
        if arm_project is None:
            continue
        arm_project_id = str(arm_project)
        if scoped_project is not None and arm_project_id != str(scoped_project):
            raise HTTPException(
                status_code=403,
                detail="Bench arm is outside the token's project scope",
            )
        if arm_project_id not in checked_arm_projects:
            await require_project_member(
                request,
                database,
                arm_project_id,
                min_role="editor",
                allow_archived=False,
            )
            checked_arm_projects.add(arm_project_id)
        arm["project_id"] = arm_project_id

    # Fail invalid tool vocabularies at run creation rather than persisting a
    # run whose sweeper can never create its first job. The regular handler
    # validates again at each actual submission.
    for task in payload["tasks"]:
        task["config_override"] = (
            validate_tool_overrides(task.get("config_override")) or {}
        )
    for arm in payload["arms"]:
        arm["config_override"] = (
            validate_tool_overrides(arm.get("config_override")) or {}
        )
        if arm.get("expert_id"):
            try:
                await validate_automation_expert_selection(
                    database,
                    owner_id=str(caller["id"]),
                    project_id=requested_project,
                    expert="worker_base",
                    expert_id=str(arm["expert_id"]),
                    caller_is_admin=bool(caller.get("is_admin")),
                )
            except ExpertSelectionError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc

    return await create_bench_run(
        dependencies.store,
        name=body.name,
        created_by=str(caller["id"]),
        spec=payload,
    )


@router.get("/runs")
async def list_runs(
    request: Request,
    limit: int = Query(100, ge=1, le=500),
) -> list[dict[str, Any]]:
    """List caller-owned runs; admins can inspect the full fleet."""

    dependencies = _request_dependencies(request)
    caller = await require_approved_user(request, dependencies.store.db)
    return await dependencies.store.list_runs(
        created_by=None if caller.get("is_admin") else str(caller["id"]),
        limit=limit,
    )


@router.get("/runs/{run_id}")
async def get_run(request: Request, run_id: str) -> dict[str, Any]:
    """Return one visible run, including its frozen spec and live ledger."""

    dependencies = _request_dependencies(request)
    caller = await require_approved_user(request, dependencies.store.db)
    return await _visible_run_or_404(dependencies.store, run_id, caller)


@router.get("/runs/{run_id}/report")
async def get_report(request: Request, run_id: str) -> dict[str, Any]:
    """Compute the v0 phase/cost report server-side from authoritative data."""

    dependencies = _request_dependencies(request)
    caller = await require_approved_user(request, dependencies.store.db)
    run = await _visible_run_or_404(dependencies.store, run_id, caller)
    if dependencies.audit_reader is None or not dependencies.audit_reader.is_available:
        raise HTTPException(status_code=503, detail="Audit store not available")
    if dependencies.forge is None or dependencies.resolve_job_repo is None:
        raise RuntimeError("Bench reporting dependencies are not configured")
    return await compute_bench_report(
        dependencies.store.db,
        run,
        audit_reader=dependencies.audit_reader,
        gitea_client=dependencies.forge,
        resolve_job_repo=dependencies.resolve_job_repo,
    )


@router.post("/runs/{run_id}/cancel")
async def cancel_run(request: Request, run_id: str) -> dict[str, Any]:
    """Stop future submissions and cancel all currently live member jobs."""

    dependencies = _request_dependencies(request)
    caller = await require_approved_user(request, dependencies.store.db)
    store = dependencies.store
    run = await _visible_run_or_404(store, run_id, caller)
    if dependencies.cancel_job is None:
        raise RuntimeError("Bench cancellation is not configured")

    async def cancel_member(job_id: str) -> Any:
        return await dependencies.cancel_job(job_id, caller)

    try:
        return await cancel_bench_run(
            store,
            run,
            cancel_job_fn=cancel_member,
        )
    except BenchStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


__all__ = [
    "BenchArmSpec",
    "BenchRunCreate",
    "BenchTaskSpec",
    "router",
]
