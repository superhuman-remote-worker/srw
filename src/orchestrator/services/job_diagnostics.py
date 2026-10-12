"""Per-job and per-thread operational diagnostics: logs, LLM requests, shell state.

Distinct from :mod:`orchestrator.services.diagnostics`, which answers questions
about the *deployment* (email-template previews, workspace configuration) under
an admin gate. Everything here is scoped to one job or one session and gated by
the caller's access to that record.

One log reader serves both the job and the thread routes. Job logs prefer the
live per-job file and fall back to the S3 archive written at agent-pod deletion;
session logs are archive-only, because a live session's log is on its pod. Both
paths run the same level/grep filter and the same id-scoping, so a shared pod
log is disaggregated identically no matter which route asked for it.

Design: knowledge-base/knowledge/features/job_log_archive.md.
"""

from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

import httpx
from fastapi import HTTPException
from fastapi.responses import PlainTextResponse

from shared.content_redaction import sanitize, sanitize_text

#: Levels the log filter understands.
LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")

#: Timeout for the agent shell-state round trip.
SHELL_STATE_TIMEOUT_SECONDS = 10.0


class DiagnosticsWorkspace(Protocol):
    @property
    def base_path(self) -> Path: ...


class ArchiveStore(Protocol):
    def get_blob(self, key: str) -> Awaitable[bytes | None]: ...


class LlmRequestReader(Protocol):
    @property
    def is_available(self) -> bool: ...

    def list_llm_requests(
        self,
        job_id: str,
        *,
        limit: int,
        offset: int,
        call_type: str | None,
        status: str | None,
    ) -> Awaitable[dict[str, Any]]: ...


@dataclass(frozen=True)
class JobDiagnosticsDependencies:
    """Application collaborators, resolved per invocation by the app factory."""

    workspace: DiagnosticsWorkspace
    snapshots: ArchiveStore
    audit_reader: LlmRequestReader
    #: Freshly attests the exact registered agent process for pinned control.
    prepare_pinned_job_mutation_target: Callable[..., Awaitable[Any]]


# =============================================================================
# Shared log reader
# =============================================================================


def filter_log_lines(
    all_lines: list[str], level: str | None, grep: str | None
) -> tuple[list[str], bool]:
    """Apply level/grep filters to log lines (text- and JSON-format aware)."""
    filtered = False
    if level:
        level_upper = level.upper()
        if level_upper not in LOG_LEVELS:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid level: {level}. Must be DEBUG, INFO, WARNING, or ERROR",
            )
        # Both formats occur: local dev files use the text formatter
        # ("… - name - LEVEL - …"); archived pod logs are JSON lines
        # ('"level": "LEVEL"'), possibly prefixed with kubelet timestamps.
        safe_level = re.escape(level_upper)
        pattern = re.compile(
            rf"^\d{{4}}-\d{{2}}-\d{{2}}\s+\d{{2}}:\d{{2}}:\d{{2}}\s+-\s+\S+\s+-\s+{safe_level}\s+-"
        )
        json_tokens = (f'"level": "{level_upper}"', f'"level":"{level_upper}"')
        all_lines = [
            line
            for line in all_lines
            if pattern.match(line) or any(t in line for t in json_tokens)
        ]
        filtered = True

    if grep:
        grep_lower = grep.lower()
        all_lines = [line for line in all_lines if grep_lower in line.lower()]
        filtered = True

    return all_lines, filtered


async def read_archived_agent_log(
    meta: dict | str | None, *, dependencies: JobDiagnosticsDependencies
) -> str | None:
    """Stitch the archived agent-pod log(s) referenced by a job/thread row.

    ``log_archive_keys`` is stamped at pod deletion by the log archive
    (knowledge-base/knowledge/features/job_log_archive.md). Returns the concatenated text, or
    ``None`` when the row has no archive or the store is unavailable.
    """
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except (json.JSONDecodeError, TypeError):
            return None
    keys = (meta or {}).get("log_archive_keys") or []
    if not isinstance(keys, list) or not keys:
        return None
    chunks: list[str] = []
    for key in keys:
        data = await dependencies.snapshots.get_blob(str(key))
        if data:
            chunks.append(data.decode("utf-8", "replace"))
    return "\n".join(chunks) if chunks else None


def scope_archived_lines(text: str, token: str) -> list[str]:
    """Scope a shared pod log to one job/thread by its id-tagged lines.

    In-cluster lines are correlation-tagged JSON (Slice 0 of
    centralized_logging), so substring-matching the uuid disaggregates a
    multi-job worker-pod log. If nothing matches, the log predates tagging
    (or ran LOG_FORMAT=text) — return it whole rather than hide it.
    """
    all_lines = text.splitlines()
    matched = [line for line in all_lines if token in line]
    return matched or all_lines


# =============================================================================
# Job diagnostics
# =============================================================================


async def get_job_logs(
    *,
    job_id: str,
    job: dict[str, Any],
    lines: int,
    grep: str | None,
    level: str | None,
    raw: bool,
    dependencies: JobDiagnosticsDependencies,
) -> Any:
    """Read the tail of a job's log file with optional filtering.

    Serves the live per-job file when it exists (compose/dev shared volume),
    else falls back to the S3 log archive written at agent-pod deletion
    (knowledge-base/knowledge/features/job_log_archive.md) — logs stay readable after the reap.
    """
    archived = False
    log_path = dependencies.workspace.base_path / "logs" / f"job_{job_id}.log"
    if log_path.exists():
        try:
            text = log_path.read_text()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to read log file: {e}")
        all_lines = text.splitlines()
    else:
        archived_text = await read_archived_agent_log(
            job.get("context"), dependencies=dependencies
        )
        if archived_text is None:
            raise HTTPException(
                status_code=404, detail=f"Log file not found for job {job_id}"
            )
        archived = True
        text = archived_text
        all_lines = scope_archived_lines(text, job_id)

    if raw:
        return PlainTextResponse(text)

    all_lines, filtered = filter_log_lines(all_lines, level, grep)
    total_lines = len(all_lines)

    # Tail N lines
    tail_lines = all_lines[-lines:]

    return {
        "job_id": job_id,
        "lines": tail_lines,
        "total_lines": total_lines,
        "filtered": filtered,
        "archived": archived,
        "log_path": None if archived else str(log_path),
    }


async def get_thread_logs(
    *,
    thread_id: str,
    thread: dict[str, Any],
    lines: int,
    grep: str | None,
    level: str | None,
    raw: bool,
    dependencies: JobDiagnosticsDependencies,
) -> Any:
    """Read the archived agent-pod log for a session.

    Post-mortem debugging for sessions whose agent pod is gone: serves the
    S3 archive written at pod deletion (knowledge-base/knowledge/features/job_log_archive.md).
    404s until the pod has been deleted at least once — while it is alive,
    the log lives on the pod (``kubectl logs``).
    """
    text = await read_archived_agent_log(
        thread.get("metadata"), dependencies=dependencies
    )
    if text is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "No archived log for this session — the agent pod may still "
                "be running, or it was deleted before the log archive existed"
            ),
        )
    if raw:
        return PlainTextResponse(text)

    all_lines = scope_archived_lines(text, thread_id)
    all_lines, filtered = filter_log_lines(all_lines, level, grep)
    total_lines = len(all_lines)

    return {
        "thread_id": thread_id,
        "lines": all_lines[-lines:],
        "total_lines": total_lines,
        "filtered": filtered,
        "archived": True,
    }


async def get_job_llm_requests(
    *,
    job_id: str,
    limit: int,
    offset: int,
    call_type: str | None,
    status: str | None,
    dependencies: JobDiagnosticsDependencies,
) -> dict[str, Any]:
    """List LLM requests for a job with summary fields.

    Returns model, timestamp, token usage, tool call names, call_type, and
    iteration for each request. Use the _id with GET /api/requests/{doc_id} to
    get the full request/response.
    """
    if not dependencies.audit_reader.is_available:
        raise HTTPException(status_code=503, detail="Audit store not available")

    try:
        UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid job_id format: {job_id}")

    try:
        data = await dependencies.audit_reader.list_llm_requests(
            job_id,
            limit=limit,
            offset=offset,
            call_type=call_type,
            status=status,
        )
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


def _sanitized_shell_state(data: Any) -> Any:
    """Redact credentials from the panes before anyone reads them.

    A raw pane is exactly where ``git remote -v`` against a token-authenticated
    clone printed both remote URLs to a coordinator. Redacted HERE rather than
    only on the agent, so an agent image from before the redaction cannot put
    one back on the wire. A tab that lost values says how many, so the reader
    knows the pane was edited rather than silent.
    """
    if not isinstance(data, dict) or not isinstance(data.get("tabs"), list):
        return data
    tabs: list[Any] = []
    for tab in data["tabs"]:
        if isinstance(tab, dict) and isinstance(tab.get("recent_output"), str):
            clean = sanitize(tab["recent_output"])
            tab = {**tab, "recent_output": clean.text}
            if clean.redacted:
                tab["redacted"] = True
                tab["redacted_count"] = clean.count
        tabs.append(tab)
    return {**data, "tabs": tabs}


async def get_job_shell_state(
    *,
    job_id: str,
    job: dict[str, Any],
    dependencies: JobDiagnosticsDependencies,
) -> dict[str, Any]:
    """Proxy shell state request to the agent processing a job.

    The stored Pod IP is only a coordinate. Freshly attest the exact registered
    process and require its recipient envelope before returning shell output.
    """
    try:
        if job.get("status") != "processing":
            raise HTTPException(
                status_code=400,
                detail=f"Job is not processing (status: {job.get('status')})",
            )

        assigned_agent_id = job.get("assigned_agent_id")
        if not assigned_agent_id:
            raise HTTPException(status_code=400, detail="Job has no assigned agent")

        target = await dependencies.prepare_pinned_job_mutation_target(
            agent_id=str(assigned_agent_id),
            job_id=job_id,
            require_idle=False,
        )
        if target is None:
            raise HTTPException(
                status_code=409,
                detail={"code": "pinned_recipient_unavailable"},
            )

        agent_url = (
            f"http://{target.agent['pod_ip']}:{target.agent['pod_port']}"
            "/system/shell-state"
        )

        async with httpx.AsyncClient(timeout=SHELL_STATE_TIMEOUT_SECONDS) as client:
            response = await client.post(
                agent_url,
                json=target.recipient.model_dump(mode="json"),
            )

        if response.status_code != 200:
            if response.status_code == 409:
                raise HTTPException(
                    status_code=409,
                    detail={"code": "pinned_recipient_mismatch"},
                )
            raise HTTPException(
                status_code=502,
                detail=(
                    f"Agent returned {response.status_code}: "
                    f"{sanitize_text(response.text)}"
                ),
            )

        return _sanitized_shell_state(response.json())

    except HTTPException:
        raise
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to connect to agent: {str(e)}",
        ) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
