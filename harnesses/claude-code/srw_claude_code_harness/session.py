"""Run one Claude Code session through the Agent SDK (spec B3 steps 2, 3, 5; B5).

- ``job_options`` turns every built-in tool off and exposes only the harness's
  own in-process MCP tools; nothing from the job reaches an option that starts a
  program or loads code (hooks, settings files, plugins, ``cli_path``,
  ``extra_args``, stdio MCP servers).
- ``route_env`` builds the CLI's whole environment from fixed values plus the
  job's model route. The SDK merges ``ClaudeAgentOptions.env`` over
  ``os.environ``, so the entrypoint calls ``scrub_process_environment`` first.
- ``run_session`` streams every message to the transcript and enforces the
  wall-clock deadline and the stop request by interrupting the CLI.

The SDK is imported inside functions so the rest of the package (and its tests)
does not need it.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .job import JobSpec, ModelRoute
from .transcript import Transcript

if TYPE_CHECKING:
    from claude_agent_sdk import ClaudeAgentOptions, ResultMessage

MCP_SERVER_NAME = "ws"

# Names verified against Claude Code 2.1.310.
CLI_FIXED_ENV = {
    "CLAUDE_CODE_DISABLE_AUTO_MEMORY": "1",
    "CLAUDE_CODE_DISABLE_CLAUDE_MDS": "1",
    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
    "ENABLE_TOOL_SEARCH": "false",
    "DISABLE_AUTOUPDATER": "1",
    "DISABLE_TELEMETRY": "1",
    "DISABLE_ERROR_REPORTING": "1",
}

# Deadline path (defaults of ``run_session``'s keyword arguments): after the
# interrupt, how long to wait for the CLI's final ResultMessage, then how long to
# wait for ``disconnect()``. The SDK bounds its own close (~20 s: stdin close,
# graceful exit, terminate, kill); it is never cancelled from here, because a
# cancelled close skips the kill escalation. Past the wait the close keeps
# running in the background and the session returns.
INTERRUPT_GRACE_SECONDS = 15.0
DISCONNECT_WAIT_SECONDS = 25.0
# Stop path (SIGTERM / SRW cancel; the pod has ~30 s before SIGKILL and still has
# to copy the transcript and deregister). Once ``stop`` fires — before or during
# the session, or during a deadline's grace — waiting for the ResultMessage and
# unwinding the conversation end at most this long after it fired ...
STOP_INTERRUPT_GRACE_SECONDS = 5.0
# ... and the disconnect is waited for at most this long, then abandoned.
STOP_DISCONNECT_WAIT_SECONDS = 2.0
# How long to wait for a cancelled task to unwind (never less than the minimum,
# so a cooperative task gets to finish even when the stop budget is spent).
SETTLE_WAIT_SECONDS = 5.0
SETTLE_MIN_SECONDS = 0.1


def _fixed_env(run_dir: Path, path: str) -> dict[str, str]:
    """Fixed CLI values plus the per-run ``home``, ``claude`` and ``tmp`` dirs."""
    dirs = {
        "HOME": run_dir / "home",
        "CLAUDE_CONFIG_DIR": run_dir / "claude",
        "TMPDIR": run_dir / "tmp",
    }
    for directory in dirs.values():
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    env = dict(CLI_FIXED_ENV)
    env["PATH"] = path
    env.update({key: str(directory) for key, directory in dirs.items()})
    return env


def route_env(route: ModelRoute, *, run_dir: Path, path: str) -> dict[str, str]:
    """The CLI's environment for a job: fixed values plus the job's model route.

    A route without a key (a keyless gateway) gets a placeholder, since the CLI
    refuses to start without one. Trailing slashes and then one ``/v1`` are
    stripped from the base URL because the CLI appends its own path.
    """
    env = _fixed_env(run_dir, path)
    env["ANTHROPIC_API_KEY"] = route.api_key or "not-needed"
    if route.base_url:
        env["ANTHROPIC_BASE_URL"] = route.base_url.rstrip("/").removesuffix("/v1")
    return env


def job_options(
    spec: JobSpec, *, tools: list, env: dict[str, str], cwd: Path
) -> ClaudeAgentOptions:
    """SDK options for a job, built-ins off and only ``mcp__ws__<tool>`` allowed.

    ``fallback_model``, ``hooks``, ``settings``, ``plugins``, ``extra_args``,
    ``cli_path``, ``add_dirs``, ``agents`` and ``skills`` keep the SDK defaults;
    no value from the job ever reaches them (spec B5).
    """
    from claude_agent_sdk import ClaudeAgentOptions, create_sdk_mcp_server

    prompt: dict[str, str] = {"type": "preset", "preset": "claude_code"}
    if spec.instructions:
        prompt["append"] = spec.instructions
    return ClaudeAgentOptions(
        tools=[],
        mcp_servers={
            MCP_SERVER_NAME: create_sdk_mcp_server(MCP_SERVER_NAME, "1.0.0", tools)
        },
        allowed_tools=[f"mcp__{MCP_SERVER_NAME}__{t.name}" for t in tools],
        strict_mcp_config=True,
        permission_mode="default",
        setting_sources=[],
        system_prompt=prompt,
        model=spec.model.model,
        max_turns=spec.limits.max_turns,
        max_budget_usd=spec.limits.max_budget_usd,
        effort=spec.effort,
        env=dict(env),
        cwd=cwd,
        include_partial_messages=False,
    )


@dataclass
class SessionOutcome:
    """How a session ended.

    ``timed_out``: the wall-clock deadline fired. ``interrupted``: the stop
    request (SIGTERM / SRW cancel) fired while the session was still running;
    both are set when stop fires during the deadline's grace. Either way
    ``result`` holds the CLI's ResultMessage if one arrived within the grace, and
    in a race (the CLI finished as the interrupt landed) that may be a
    ``success`` result: callers treat either flag as interrupted, whatever the
    result says. ``error``: an SDK/CLI failure, described as
    ``"<Type>: <message>"``.
    """

    result: ResultMessage | None
    timed_out: bool
    interrupted: bool
    error: str | None


def _describe(exc: BaseException) -> str:
    text = str(exc)
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def _note(transcript: Transcript, payload: dict[str, Any]) -> None:
    """Record a harness event; bookkeeping must never mask the real outcome."""
    with contextlib.suppress(Exception):
        transcript.record("harness", payload)


class _StopWatch:
    """Notices ``stop`` during the shutdown waits and shortens them once it fires."""

    def __init__(self, stop: asyncio.Event) -> None:
        self._loop = asyncio.get_running_loop()
        self._stop = stop
        self.waiter = asyncio.ensure_future(stop.wait())
        self.fired_at: float | None = None

    def now(self) -> float:
        return self._loop.time()

    def fired(self) -> bool:
        if self.fired_at is None and self._stop.is_set():
            self.fired_at = self._loop.time()
        return self.fired_at is not None

    async def wait(
        self, task: asyncio.Future, *, end: float, stop_end: Callable[[float], float]
    ) -> bool:
        """Wait for ``task`` until ``end``; once stop fires, until ``stop_end(fired_at)``
        if that is sooner. Returns whether ``task`` finished. Never cancels it."""
        while not task.done():
            fired = self.fired()
            limit = min(end, stop_end(self.fired_at)) if fired else end
            remaining = limit - self._loop.time()
            if remaining <= 0:
                break
            waiters = {task}
            if not fired and not self.waiter.done():
                waiters.add(self.waiter)
            await asyncio.wait(
                waiters, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
            )
        return task.done()

    async def close(self) -> None:
        if not self.waiter.done():
            self.waiter.cancel()
            await asyncio.wait({self.waiter})


def _stop_grace_end(fired_at: float) -> float:
    return fired_at + STOP_INTERRUPT_GRACE_SECONDS


async def _settle(task: asyncio.Future | None, watch: _StopWatch) -> None:
    """Cancel ``task`` if still running and wait for it to unwind, bounded.

    ``asyncio.wait`` never raises the task's own exception, but a cancellation of
    the caller still propagates. A task that ignores cancellation is abandoned
    after ``SETTLE_WAIT_SECONDS``, or once the stop budget is spent.
    """
    if task is None:
        return
    if not task.done():
        task.cancel()
        start = watch.now()
        await watch.wait(
            task,
            end=start + SETTLE_WAIT_SECONDS,
            stop_end=lambda fired: max(
                _stop_grace_end(fired), start + SETTLE_MIN_SECONDS
            ),
        )
    if task.done() and not task.cancelled():
        task.exception()  # mark retrieved; the outcome was already taken


async def run_session(
    prompt: str,
    options: Any,
    *,
    transcript: Transcript,
    deadline_seconds: float,
    stop: asyncio.Event,
    client_factory: Any = None,
    interrupt_grace_seconds: float = INTERRUPT_GRACE_SECONDS,
    disconnect_wait_seconds: float = DISCONNECT_WAIT_SECONDS,
) -> SessionOutcome:
    """Run ``prompt`` to the first ResultMessage, the deadline or ``stop``.

    Never raises for SDK/CLI failures (they become ``error``);
    ``asyncio.CancelledError`` propagates after the client is disconnected.

    Deadline: interrupt, wait up to ``interrupt_grace_seconds`` for the
    ResultMessage, then disconnect (waited for up to
    ``disconnect_wait_seconds``). Stop (before or during the session, or during
    a deadline's grace): everything up to the disconnect ends at most
    ``STOP_INTERRUPT_GRACE_SECONDS`` after stop fired, and the disconnect is
    waited for at most ``STOP_DISCONNECT_WAIT_SECONDS``, so the entrypoint keeps
    most of the pod's grace period for the transcript copy and the deregistration.
    """
    outcome = SessionOutcome(
        result=None, timed_out=False, interrupted=False, error=None
    )
    if stop.is_set():
        outcome.interrupted = True
        _note(transcript, {"event": "stop", "phase": "before_start"})
        return outcome

    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.0, float(deadline_seconds))
    watch = _StopWatch(stop)
    client: Any = None
    connected = False
    conversation: asyncio.Task | None = None
    try:
        from claude_agent_sdk import ResultMessage

        if client_factory is None:
            from claude_agent_sdk import ClaudeSDKClient

            client_factory = ClaudeSDKClient
        client = client_factory(options)

        async def converse() -> ResultMessage | None:
            nonlocal connected
            await client.connect()
            connected = True
            await client.query(prompt)
            messages = client.receive_messages()
            async with contextlib.aclosing(messages):
                async for message in messages:
                    transcript.record(type(message).__name__, message)
                    if isinstance(message, ResultMessage):
                        return message
            return None

        conversation = asyncio.create_task(converse())
        await asyncio.wait(
            {conversation, watch.waiter},
            timeout=max(0.0, deadline - loop.time()),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not conversation.done():
            if watch.fired():
                outcome.interrupted = True
                _note(transcript, {"event": "stop", "phase": "session"})
            else:
                outcome.timed_out = True
                _note(
                    transcript,
                    {"event": "deadline", "deadlineSeconds": deadline_seconds},
                )
            await _interrupt(
                client,
                conversation,
                connected=connected,
                transcript=transcript,
                watch=watch,
                grace=interrupt_grace_seconds,
            )
            if watch.fired() and not outcome.interrupted:
                outcome.interrupted = True
                _note(transcript, {"event": "stop", "phase": "deadline_grace"})
        if not conversation.done():
            _note(transcript, {"event": "no_result_after_interrupt"})
        elif conversation.cancelled():
            outcome.error = "CancelledError: the SDK cancelled the conversation"
            _note(transcript, {"event": "error", "error": outcome.error})
        else:
            # Re-raises an SDK/CLI failure from the conversation (handled below).
            outcome.result = conversation.result()
            if outcome.result is None and not (
                outcome.timed_out or outcome.interrupted
            ):
                outcome.error = "the message stream ended without a ResultMessage"
                _note(transcript, {"event": "error", "error": outcome.error})
    except Exception as exc:  # noqa: BLE001 - SDK/CLI failures become the outcome
        outcome.error = _describe(exc)
        _note(transcript, {"event": "error", "error": outcome.error})
    finally:
        await _settle(conversation, watch)
        if client is not None:
            await _disconnect(client, transcript, watch, disconnect_wait_seconds)
        await watch.close()
    _note(
        transcript,
        {
            "event": "session_end",
            "timedOut": outcome.timed_out,
            "interrupted": outcome.interrupted,
            "error": outcome.error,
            "resultSubtype": getattr(outcome.result, "subtype", None),
        },
    )
    return outcome


async def _interrupt(
    client: Any,
    conversation: asyncio.Task,
    *,
    connected: bool,
    transcript: Transcript,
    watch: _StopWatch,
    grace: float,
) -> None:
    """Interrupt the CLI and give the conversation the grace to deliver a result.

    The interrupt request and the wait for the result share one grace window,
    shortened to the stop budget once stop fires. Before the client is
    connected there is nothing to interrupt: the caller cancels the
    conversation. If the interrupt itself fails the CLI is not listening, so
    waiting the grace out would not help either.
    """
    if not connected:
        _note(transcript, {"event": "interrupt_skipped", "reason": "not_connected"})
        return
    grace_end = watch.now() + grace
    interrupting = asyncio.ensure_future(client.interrupt())
    if not await watch.wait(interrupting, end=grace_end, stop_end=_stop_grace_end):
        await _settle(interrupting, watch)
        _note(
            transcript,
            {"event": "interrupt_failed", "error": "no answer within the grace"},
        )
        return
    exc = None if interrupting.cancelled() else interrupting.exception()
    if interrupting.cancelled() or exc is not None:
        error = _describe(exc) if exc is not None else "cancelled"
        _note(transcript, {"event": "interrupt_failed", "error": error})
        return
    _note(transcript, {"event": "interrupt_sent"})
    await watch.wait(conversation, end=grace_end, stop_end=_stop_grace_end)


async def _disconnect(
    client: Any, transcript: Transcript, watch: _StopWatch, seconds: float
) -> None:
    """Disconnect, waiting at most ``seconds`` (``STOP_DISCONNECT_WAIT_SECONDS``
    once stop has fired); the close is never cancelled, only abandoned."""
    closing = asyncio.ensure_future(client.disconnect())
    start = watch.now()
    done = await watch.wait(
        closing,
        end=start + seconds,
        stop_end=lambda fired: max(fired, start) + STOP_DISCONNECT_WAIT_SECONDS,
    )
    if not done:
        _note(transcript, {"event": "disconnect_abandoned"})
        return
    exc = None if closing.cancelled() else closing.exception()
    if exc is not None:
        _note(transcript, {"event": "disconnect_failed", "error": _describe(exc)})


def scrub_process_environment(keep: Iterable[str]) -> dict[str, str]:
    """Remove every ``os.environ`` variable except ``keep``; return the removed ones.

    The SDK starts the CLI with ``os.environ`` overlaid by
    ``ClaudeAgentOptions.env``, so without this the pod's environment (database
    credentials, service tokens) would reach the CLI. Called by the entrypoint
    before the session, never from ``run_session``.
    """
    if isinstance(keep, str):
        raise TypeError("keep must be an iterable of names, not a string")
    kept = set(keep)
    removed: dict[str, str] = {}
    for name in list(os.environ):
        if name not in kept:
            removed[name] = os.environ.pop(name)
    return removed
