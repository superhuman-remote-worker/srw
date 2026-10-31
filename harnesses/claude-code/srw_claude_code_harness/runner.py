"""One job: connect the workspace, run Claude Code against it, build the report.

The runner returns the :class:`CompletionReport` the entrypoint sends, or
``None`` after a stop (SRW's cancel or SIGTERM): the orchestrator has already
recorded a cancel, so no report follows. It never sends anything itself while
the job runs; :meth:`JobRunner.complete` is the one call that reports.

The transcript is written under the run directory and copied, best effort, to
``<workspace root>/.srw/claude-code/transcript.jsonl`` so the job's result can
be reviewed with the workspace.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import posixpath
import sys
from collections.abc import Awaitable, Iterable
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from .job import JobRejected, JobSpec, require_anthropic_route
from .orchestrator import CompletionReport, OrchestratorClient, PinnedEcho
from .session import SessionOutcome, job_options, route_env, run_session
from .tools import TOOL_NAMES, ToolHandlers, make_tools
from .transcript import Transcript
from .workspace import Workspace, WorkspaceError

TRANSCRIPT_PATH = ".srw/claude-code/transcript.jsonl"
# The copy into the workspace gets this long; once stop has fired, the shorter
# limit (the pod has ~30 s before SIGKILL).
TRANSCRIPT_COPY_SECONDS = 60.0
STOP_TRANSCRIPT_COPY_SECONDS = 5.0
CLOSE_WAIT_SECONDS = 5.0
# How long a connection attempt cancelled by the stop gets to unwind.
CANCEL_WAIT_SECONDS = 2.0
MESSAGE_LIMIT = 1000
_MIN_SECRET_LENGTH = 4


def progress(message: str) -> None:
    print(f"[srw-harness] {message}", flush=True)


def warn(message: str) -> None:
    print(f"[srw-harness] {message}", file=sys.stderr, flush=True)


def _relative_to(root: str, paths) -> list[str]:
    """Workspace-relative deliverables, as SRW's agent reports them."""
    base = PurePosixPath(root)
    relative = []
    for path in paths:
        try:
            relative.append(str(PurePosixPath(path).relative_to(base)))
        except ValueError:
            relative.append(str(path))
    return sorted(relative)


def error_report(message: str, *, kind: str = "job_error") -> CompletionReport:
    """A failed job: the plain message is what the user reads."""
    return CompletionReport(
        should_stop=True,
        goal_achieved=False,
        error={"message": message, "type": kind, "recoverable": False},
        freeze_data=None,
    )


def spec_secrets(spec: JobSpec) -> tuple[str | None, ...]:
    """The values of a start request that must never be printed or reported."""
    proof = spec.pinned.delivery_proof if spec.pinned is not None else None
    return (spec.model.api_key, proof)


def redact(text: str, secrets: Iterable[str | None]) -> str:
    """``text`` with every secret replaced by ``***``, cut to ``MESSAGE_LIMIT``."""
    for secret in sorted(
        (s for s in secrets if s and len(s) >= _MIN_SECRET_LENGTH),
        key=len,
        reverse=True,
    ):
        text = text.replace(secret, "***")
    return text[:MESSAGE_LIMIT]


class _Stopped(Exception):
    """The stop fired while the workspace was connecting."""


def _local_cwd(root: str, run_dir: Path) -> Path:
    """The CLI's local working directory.

    Claude Code tells the model its working directory and the tools resolve
    paths against the workspace root, so the CLI runs in a local directory with
    the root's own path when the harness can have one (the image provides it);
    otherwise in a per-run directory.
    """
    path = Path(root)
    with contextlib.suppress(OSError):
        path.mkdir(parents=True, exist_ok=True)
    if path.is_dir() and os.access(path, os.R_OK | os.X_OK):
        return path
    fallback = run_dir / "cwd"
    fallback.mkdir(mode=0o700, parents=True, exist_ok=True)
    return fallback


def _session_failure(outcome: SessionOutcome, timeout_seconds: float) -> str | None:
    """Why the session did not succeed, or ``None`` when it did."""
    if outcome.timed_out:
        return f"timed out: the {timeout_seconds:g} s wall-clock limit was reached"
    result = outcome.result
    if result is None:
        detail = outcome.error or "the session ended without a result"
        return f"run error: no result ({detail})"
    subtype = getattr(result, "subtype", None)
    if subtype != "success":
        return f"run error: {subtype}"
    if getattr(result, "is_error", False):
        return "run error: the result is marked as an error"
    return None


class JobRunner:
    def __init__(
        self,
        *,
        client: OrchestratorClient,
        pod_uid: str,
        run_dir: Path,
        stop: asyncio.Event,
        connect: Any = Workspace.connect,
        client_factory: Any = None,
        path: str | None = None,
    ) -> None:
        """``connect`` replaces :meth:`Workspace.connect` and ``client_factory``
        replaces ``ClaudeSDKClient`` (tests). ``path`` is the CLI's ``PATH``
        (default: this process's)."""
        self.client = client
        self.pod_uid = pod_uid
        self.run_dir = Path(run_dir)
        self.stop = stop
        self.connect = connect
        self.client_factory = client_factory
        self.path = path

    async def complete(
        self, job_id: str, report: CompletionReport, *, pinned: PinnedEcho | None
    ) -> bool:
        """Send ``report`` for the job; ``True`` when the orchestrator took it."""
        return await self.client.complete(
            job_id, report, pinned=pinned, pod_uid=self.pod_uid
        )

    async def run(self, spec: JobSpec) -> CompletionReport | None:
        """Run the job; the report to send, or ``None`` after a stop.

        Raises only for a harness fault (the entrypoint reports that as a
        ``job_error``); a refused route, an unreachable workspace and every
        session outcome become the report.
        """
        try:
            require_anthropic_route(spec.model)
        except JobRejected as exc:
            return error_report(str(exc))
        secrets = spec_secrets(spec)
        progress("connecting to the workspace")
        try:
            workspace = await self._unless_stopped(self.connect(spec.workspace))
        except _Stopped:
            progress("stopped while connecting to the workspace")
            return None
        except WorkspaceError as exc:
            return error_report(redact(str(exc), secrets), kind="workspace_unavailable")
        transcript: Transcript | None = None
        try:
            self.run_dir.mkdir(parents=True, exist_ok=True)
            transcript = Transcript(self.run_dir / "transcript.jsonl")
            handlers = ToolHandlers(workspace)
            tools = make_tools(workspace, TOOL_NAMES, handlers=handlers)
            env = route_env(
                spec.model,
                run_dir=self.run_dir,
                path=self.path or os.environ.get("PATH") or os.defpath,
            )
            cwd = _local_cwd(workspace.root, self.run_dir)
            options = job_options(spec, tools=tools, env=env, cwd=cwd)
            progress(f"starting Claude Code (model {spec.model.model})")
            outcome = await run_session(
                spec.prompt,
                options,
                transcript=transcript,
                deadline_seconds=spec.limits.timeout_seconds,
                stop=self.stop,
                client_factory=self.client_factory,
            )
            if outcome.interrupted or self.stop.is_set():
                progress("session stopped")
                return None
            failure = _session_failure(outcome, spec.limits.timeout_seconds)
            if failure is not None:
                failure = redact(failure, secrets)
                progress(f"session ended: {failure}")
                return error_report(failure)
            progress("session ended: success")
            # Like SRW's agent: the goal counts as achieved only under full
            # autonomy; otherwise the finished job waits in Review.
            return CompletionReport(
                True,
                spec.autonomy == "full",
                None,
                {
                    "status": "job_completed",
                    "freeze_type": "job_complete",
                    "summary": getattr(outcome.result, "result", None) or "",
                    "deliverables": _relative_to(
                        workspace.root, handlers.written_paths
                    ),
                    "confidence": 1.0,
                    "job_id": spec.job_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "model": spec.model.model,
                },
            )
        finally:
            if transcript is not None:
                with contextlib.suppress(Exception):
                    transcript.close()
                await self._copy_transcript(workspace, transcript.path)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(workspace.close(), CLOSE_WAIT_SECONDS)

    async def _unless_stopped(self, awaitable: Awaitable[Any]) -> Any:
        """Await ``awaitable`` unless the stop fires first; then cancel it and
        raise :class:`_Stopped`. A workspace that connected anyway is closed."""
        if self.stop.is_set():
            if asyncio.iscoroutine(awaitable):
                awaitable.close()
            raise _Stopped
        task = asyncio.ensure_future(awaitable)
        waiter = asyncio.ensure_future(self.stop.wait())
        try:
            await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            task.cancel()
            raise
        finally:
            waiter.cancel()
        if task.done():
            return task.result()
        task.cancel()
        await asyncio.wait({task}, timeout=CANCEL_WAIT_SECONDS)
        if task.done() and not task.cancelled() and task.exception() is None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(task.result().close(), CLOSE_WAIT_SECONDS)
        raise _Stopped

    async def _copy_transcript(self, workspace: Workspace, path: Path) -> None:
        target = posixpath.join(workspace.root, TRANSCRIPT_PATH)
        seconds = (
            STOP_TRANSCRIPT_COPY_SECONDS
            if self.stop.is_set()
            else TRANSCRIPT_COPY_SECONDS
        )
        try:
            data = path.read_bytes()
            await workspace.write_bytes(target, data, timeout=seconds)
        except Exception as exc:  # noqa: BLE001 - best effort, never the outcome
            warn(
                f"the transcript was not copied to the workspace ({type(exc).__name__})"
            )
        else:
            progress(f"transcript copied to {target}")
