"""The v1 harness routes the orchestrator calls on port 8001."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from .orchestrator import PinnedEcho
from .job import JobRejected, JobSpec, parse_job_start, require_anthropic_route

CANCEL_WAIT_SECONDS = 120.0
PINNED_FIELDS = (
    "pinned_delivery_id",
    "pinned_projection_digest",
    "pinned_delivery_proof",
)


async def _no_runner(spec: JobSpec) -> None:  # replaced by the entrypoint
    raise RuntimeError("no job runner is wired")


async def _no_reporter(job_id: str, pinned: PinnedEcho | None, message: str) -> None:
    raise RuntimeError("no rejection reporter is wired")


@dataclass
class HarnessState:
    agent_id: str | None = None
    process_generation: str | None = None
    pod_uid: str = ""
    started: float = field(default_factory=time.monotonic)
    current: JobSpec | None = None
    rejected_job_id: str | None = None  # accepted, then reported as rejected
    acknowledgement: dict[str, Any] | None = None
    stop_requested: asyncio.Event = field(default_factory=asyncio.Event)
    job_finished: asyncio.Event = field(default_factory=asyncio.Event)
    run_job: Callable[[JobSpec], Awaitable[None]] = _no_runner
    report_rejection: Callable[[str, PinnedEcho | None, str], Awaitable[None]] = (
        _no_reporter
    )
    cancel_wait_seconds: float = CANCEL_WAIT_SECONDS
    tasks: set[asyncio.Task] = field(default_factory=set, repr=False)


def _recipient_matches(state: HarnessState, recipient: Any, job_id: str | None) -> bool:
    if not isinstance(recipient, dict):
        return False

    def text(value: Any) -> str | None:
        return str(value).strip() if value is not None and str(value).strip() else None

    return (
        recipient.get("expected_agent_id") == state.agent_id
        and text(recipient.get("expected_pod_uid")) == (text(state.pod_uid))
        and recipient.get("expected_process_generation") == state.process_generation
        and recipient.get("expected_job_id") == job_id
    )


def _current_id(state: HarnessState) -> str | None:
    if state.current is not None:
        return state.current.job_id
    return state.rejected_job_id


def _busy(state: HarnessState) -> bool:
    return _current_id(state) is not None and not state.job_finished.is_set()


def _uptime(state: HarnessState) -> float:
    return round(time.monotonic() - state.started, 3)


def _detail(status: int, detail: str) -> JSONResponse:
    return JSONResponse({"detail": detail}, status_code=status)


async def _json_body(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def create_app(state: HarnessState) -> Starlette:
    async def health(request: Request) -> JSONResponse:
        return JSONResponse(
            {
                "status": "healthy",
                "agent_id": state.agent_id or "",
                "uptime_seconds": _uptime(state),
            }
        )

    async def ready(request: Request) -> JSONResponse:
        registered = state.agent_id is not None
        is_ready = registered and not state.stop_requested.is_set()
        return JSONResponse(
            {
                "ready": is_ready,
                "message": "Ready" if is_ready else "Not ready",
                "connections": {"orchestrator": registered},
                "capabilities": {
                    "pinned_recipient_binding": True,
                    "jobs": True,
                    "model_protocols": ["anthropic-messages"],
                },
            }
        )

    async def status(request: Request) -> JSONResponse:
        return JSONResponse(
            {
                "mode": "claude-code",
                "current_job_id": _current_id(state),
                "uptime_seconds": _uptime(state),
            }
        )

    async def run(spec: JobSpec) -> None:
        try:
            await state.run_job(spec)
        finally:
            state.job_finished.set()

    async def reject(job_id: str, pinned: PinnedEcho | None, message: str) -> None:
        try:
            await state.report_rejection(job_id, pinned, message)
        finally:
            state.job_finished.set()

    async def start(request: Request) -> JSONResponse:
        body = await _json_body(request)
        if state.agent_id is None:
            return _detail(503, "Agent is not registered yet")
        job_id = str(body.get("job_id") or "")
        if not _recipient_matches(state, body.get("recipient"), job_id):
            return _detail(409, "pinned recipient mismatch")
        present = [body.get(name) for name in PINNED_FIELDS]
        if any(present) and not all(present):
            return _detail(409, "pinned delivery is incomplete")
        accepted: dict[str, Any] = {"job_id": job_id, "status": "accepted"}
        if all(present):
            accepted["pinned_delivery_id"] = body["pinned_delivery_id"]
            accepted["pinned_projection_digest"] = body["pinned_projection_digest"]
        if job_id and _current_id(state) == job_id:
            return JSONResponse(
                {**accepted, "message": "Job was already accepted by this runtime"},
                status_code=202,
            )
        if _busy(state):
            return _detail(409, "Pod is busy")
        try:
            spec = parse_job_start(body)
            require_anthropic_route(spec.model)
            state.current, state.rejected_job_id = spec, None
            coro = run(spec)
        except JobRejected as exc:
            # Answering non-2xx would leave the job claimed and requeued forever;
            # accept it and complete it with the plain error instead.
            if not job_id:
                return _detail(422, str(exc))
            pinned = PinnedEcho(*present) if any(present) else None
            state.rejected_job_id = job_id
            coro = reject(job_id, pinned, str(exc))
        state.job_finished.clear()
        task = asyncio.create_task(coro)
        state.tasks.add(task)
        task.add_done_callback(state.tasks.discard)
        return JSONResponse(
            {**accepted, "message": "Job processing started"}, status_code=202
        )

    async def cancel(request: Request) -> JSONResponse:
        body = await _json_body(request)
        if not _busy(state):
            return _detail(404, "No job currently running")
        job_id = _current_id(state)
        if not _recipient_matches(state, body.get("recipient"), job_id):
            return _detail(409, "pinned recipient mismatch")
        state.stop_requested.set()
        try:
            await asyncio.wait_for(
                state.job_finished.wait(), timeout=state.cancel_wait_seconds
            )
            graceful = True
        except asyncio.TimeoutError:
            graceful = False
        return JSONResponse(
            {
                "job_id": job_id,
                "status": "cancelled",
                "reason": body.get("reason") or "Cancelled by orchestrator",
                "graceful": graceful,
            }
        )

    return Starlette(
        routes=[
            Route("/health", health, methods=["GET"]),
            Route("/ready", ready, methods=["GET"]),
            Route("/status", status, methods=["GET"]),
            Route("/job/start", start, methods=["POST"]),
            Route("/job/cancel", cancel, methods=["POST"]),
        ]
    )
