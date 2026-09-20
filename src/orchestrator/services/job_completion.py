"""Complete-job admission, replay, and durable workflow composition."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
import logging
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from orchestrator.schemas.job_runtime import JobCompleteRequest


DURABLE_COMPLETION_HTTP_ERROR = "_completion_http_error"


@dataclass(frozen=True, slots=True)
class JobCompletionDependencies:
    """Per-application collaborators for completion report handling."""

    store: Any
    require_internal: Callable[[Request], Awaitable[Any]]
    commands_enabled: Callable[[], bool]
    status_reorder_enabled: Callable[[], bool]
    inline_delay_seconds: Callable[[], float]
    accept_command: Callable[..., Awaitable[Any]]
    finalizer: Callable[[], Any]
    legacy_complete: Callable[..., Awaitable[dict[str, Any]]]
    logger: logging.Logger
    sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep


@dataclass(frozen=True, slots=True)
class PersistedCompletionDependencies:
    """The sole operation needed by a background finalizer resume."""

    legacy_complete: Callable[..., Awaitable[dict[str, Any]]]


def durable_completion_http_outcome(exc: HTTPException) -> dict[str, Any]:
    """Encode a deterministic HTTP guard as an exact replayable outcome."""

    return {
        DURABLE_COMPLETION_HTTP_ERROR: {
            "status_code": int(exc.status_code),
            "detail": exc.detail,
            "headers": dict(exc.headers or {}),
        }
    }


def raise_durable_completion_http_outcome(outcome: Mapping[str, Any]) -> None:
    """Raise a stored deterministic HTTP result without changing its shape."""

    envelope = outcome.get(DURABLE_COMPLETION_HTTP_ERROR)
    if not isinstance(envelope, Mapping):
        return
    raise HTTPException(
        status_code=int(envelope.get("status_code", 500)),
        detail=envelope.get("detail"),
        headers=dict(envelope.get("headers") or {}) or None,
    )


async def run_persisted_completion_workflow(
    effect_runner: Any,
    *,
    dependencies: PersistedCompletionDependencies,
) -> dict[str, Any]:
    """Rebuild the authenticated request body for a background resume."""

    command = effect_runner.command
    payload = dict(command.get("payload") or {})
    from orchestrator.services.job_completion_commands import (
        ACCEPTED_COMPLETION_DECISION_KEY,
    )

    from orchestrator.services.workspace_idle_completion_events import ACCEPTED_IDLE_WAIT_SOURCE_KEY

    payload.pop(ACCEPTED_COMPLETION_DECISION_KEY, None)
    payload.pop(ACCEPTED_IDLE_WAIT_SOURCE_KEY, None)
    payload.update(
        {
            "lease_token": command.get("accepted_lease_token"),
            "agent_id": command.get("accepted_agent_id"),
            "client_report_id": command.get("client_report_id"),
        }
    )
    body = JobCompleteRequest(**payload)
    try:
        return await dependencies.legacy_complete(
            None,
            str(command["job_id"]),
            body,
            _authorized=True,
            _effect_runner=effect_runner,
        )
    except HTTPException as exc:
        if exc.status_code >= 500:
            raise
        return durable_completion_http_outcome(exc)


async def complete_job(
    request: Request,
    job_id: str,
    body: JobCompleteRequest,
    *,
    dependencies: JobCompletionDependencies,
    _authorized: bool = False,
) -> Any:
    """Authenticate, optionally admit a durable command, then run effects.

    With the default-off gate closed this calls the legacy implementation
    directly and never reads or writes any completion-command relation.
    """

    if not _authorized:
        await dependencies.require_internal(request)
    if not dependencies.commands_enabled():
        return await dependencies.legacy_complete(
            request,
            job_id,
            body,
            _authorized=True,
        )

    from orchestrator.services.job_completion_commands import (
        CompletionCommandNotFound,
        CompletionControlInProgress,
        CompletionFenceRejected,
        CompletionInProgress,
        CompletionNonTerminalReport,
        CompletionPayloadMismatch,
        CompletionTeardownInProgress,
    )

    payload = body.model_dump(
        mode="json",
        exclude={"lease_token", "agent_id", "client_report_id"},
    )
    try:
        accepted = await dependencies.accept_command(
            dependencies.store,
            job_id=job_id,
            payload=payload,
            status_reorder_enabled=dependencies.status_reorder_enabled(),
            lease_token=body.lease_token,
            agent_id=str(body.agent_id) if body.agent_id is not None else None,
            client_report_id=(
                str(body.client_report_id)
                if body.client_report_id is not None
                else None
            ),
            requested_by=(
                f"agent:{body.agent_id}"
                if body.agent_id is not None
                else f"worker-lease:{body.lease_token}"
            ),
        )
    except CompletionCommandNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except CompletionNonTerminalReport as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "completion_non_terminal_report",
                "message": str(exc),
            },
        ) from exc
    except CompletionPayloadMismatch as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except CompletionInProgress as exc:
        raise HTTPException(
            status_code=409,
            detail=str(exc),
            headers={"Retry-After": "1"},
        ) from exc
    except CompletionTeardownInProgress as exc:
        raise HTTPException(
            status_code=409,
            detail=str(exc),
            headers={"Retry-After": "1"},
        ) from exc
    except CompletionControlInProgress as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except CompletionFenceRejected as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    if accepted.disposition == "replay_done":
        raise_durable_completion_http_outcome(accepted.outcome or {})
        return JSONResponse(
            content=accepted.outcome or {},
            headers={"Idempotent-Replayed": "true"},
        )
    if accepted.disposition == "replay_parked":
        return JSONResponse(
            status_code=202,
            content={
                "status": "still_pending",
                "job_id": accepted.job_id,
                "command_id": accepted.command_id,
                "command_state": accepted.state,
            },
            headers={"Idempotent-Replayed": "true"},
        )
    if accepted.disposition == "replay_superseded":
        outcome = dict(accepted.outcome or {})
        outcome.setdefault("status", "superseded")
        outcome.setdefault("job_id", accepted.job_id)
        outcome.setdefault("winning_report_seq", accepted.winning_report_seq)
        return JSONResponse(
            content=outcome,
            headers={"Idempotent-Replayed": "true"},
        )
    if accepted.disposition == "replay_force_resolved":
        outcome = dict(accepted.outcome or {})
        outcome.setdefault("status", "force_resolved")
        outcome.setdefault("job_id", accepted.job_id)
        outcome.setdefault("abandoned_effects", list(accepted.abandoned_effects))
        return JSONResponse(
            content=outcome,
            headers={"Idempotent-Replayed": "true"},
        )

    dependencies.logger.info(
        "Completion command %s accepted for job %s",
        accepted.command_id,
        accepted.job_id,
    )

    if accepted.disposition == "fresh" and accepted.queue_terminalized:
        return JSONResponse(
            status_code=202,
            content={
                "status": "accepted_pending",
                "job_id": accepted.job_id,
                "command_id": accepted.command_id,
                "command_state": accepted.state,
            },
        )

    finalizer = dependencies.finalizer()
    inline_error: HTTPException | None = None

    async def _inline_workflow(effect_runner: Any) -> dict[str, Any]:
        nonlocal inline_error
        inline_delay = dependencies.inline_delay_seconds()
        if inline_delay > 0:
            dependencies.logger.info(
                "Completion command %s claimed for job %s; inline delay %.3fs",
                accepted.command_id,
                accepted.job_id,
                inline_delay,
            )
            await dependencies.sleep(inline_delay)
        try:
            return await dependencies.legacy_complete(
                request,
                job_id,
                body,
                _authorized=True,
                _effect_runner=effect_runner,
            )
        except HTTPException as exc:
            if exc.status_code >= 500:
                inline_error = exc
                raise
            return durable_completion_http_outcome(exc)

    finalized = await finalizer.finalize_command(
        accepted.command_id,
        callback=_inline_workflow,
        inline=True,
    )
    if (
        finalized.disposition in {"done", "terminal", "superseded", "force_resolved"}
        and finalized.outcome
    ):
        raise_durable_completion_http_outcome(finalized.outcome)
        return finalized.outcome
    if inline_error is not None:
        raise inline_error
    if finalized.state == "missing":
        raise HTTPException(
            status_code=404,
            detail="Accepted completion command no longer exists",
        )
    return JSONResponse(
        status_code=202,
        content={
            "status": "accepted_pending",
            "job_id": accepted.job_id,
            "command_id": accepted.command_id,
            "command_state": finalized.state,
        },
    )


__all__ = [
    "DURABLE_COMPLETION_HTTP_ERROR",
    "JobCompletionDependencies",
    "PersistedCompletionDependencies",
    "complete_job",
    "durable_completion_http_outcome",
    "raise_durable_completion_http_outcome",
    "run_persisted_completion_workflow",
]
