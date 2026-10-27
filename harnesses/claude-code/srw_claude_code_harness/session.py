"""Run one Claude Code session through the Agent SDK (spec B3 steps 2, 3, 5; B5).

- ``build_options`` turns every built-in tool off and exposes only the harness's
  own in-process MCP tools; nothing from the configuration reaches an option that
  starts a program or loads code (hooks, settings files, plugins, ``cli_path``,
  ``extra_args``, stdio MCP servers).
- ``cli_env`` builds the CLI's whole environment from fixed values plus the
  credential. The SDK merges ``ClaudeAgentOptions.env`` over ``os.environ``, so
  the entrypoint calls ``scrub_process_environment`` before the session.
- ``run_session`` streams every message to the transcript and enforces the
  wall-clock deadline and the stop request by interrupting the CLI.

The SDK is imported inside functions so the rest of the package (and its tests)
does not need it.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .config import _CREDENTIAL_ENV, HarnessConfig, SystemPrompt
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

# After an interrupt (deadline or stop), how long to wait for the CLI's final
# ResultMessage before disconnecting regardless.
INTERRUPT_GRACE_SECONDS = 15.0
# How long to wait for ``disconnect()``. The SDK bounds its own close (~20 s:
# stdin close, graceful exit, terminate, kill); it is never cancelled from here,
# because a cancelled close skips the kill escalation. Past this the close keeps
# running in the background and the session returns.
DISCONNECT_WAIT_SECONDS = 25.0
# How long to wait for a cancelled conversation task to unwind.
SETTLE_WAIT_SECONDS = 5.0


def cli_env(
    config: HarnessConfig,
    credential: tuple[str, str],
    *,
    run_dir: Path,
    path: str,
) -> dict[str, str]:
    """The CLI's complete environment: fixed values, per-run dirs, the credential.

    Creates the per-run ``home``, ``claude`` (config) and ``tmp`` directories
    under ``run_dir`` so the variables naming them point at real directories.
    """
    name, value = credential
    if name not in _CREDENTIAL_ENV:
        raise ValueError(f"credential must be one of {list(_CREDENTIAL_ENV)}")
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
    if not config.auto_compact:
        env["DISABLE_AUTO_COMPACT"] = "1"
    env[name] = value
    return env


def _system_prompt(prompt: SystemPrompt) -> dict[str, str] | str:
    if prompt.kind == "preset":
        return {"type": "preset", "preset": "claude_code"}
    if prompt.kind == "preset_append":
        return {"type": "preset", "preset": "claude_code", "append": prompt.text or ""}
    return prompt.text or ""


def build_options(
    config: HarnessConfig, *, tools: list, env: dict[str, str], cwd: Path
) -> ClaudeAgentOptions:
    """SDK options with built-ins off and only ``mcp__ws__<tool>`` allowed.

    ``fallback_model``, ``hooks``, ``settings``, ``plugins``, ``extra_args``,
    ``cli_path``, ``add_dirs``, ``agents`` and ``skills`` keep the SDK defaults;
    no configuration value ever reaches them (spec B5).
    """
    from claude_agent_sdk import ClaudeAgentOptions, create_sdk_mcp_server

    return ClaudeAgentOptions(
        tools=[],
        mcp_servers={
            MCP_SERVER_NAME: create_sdk_mcp_server(MCP_SERVER_NAME, "1.0.0", tools)
        },
        allowed_tools=[f"mcp__{MCP_SERVER_NAME}__{t.name}" for t in tools],
        strict_mcp_config=True,
        permission_mode="default",
        setting_sources=[],
        system_prompt=_system_prompt(config.system_prompt),
        model=config.model,
        max_turns=config.limits.max_turns,
        max_budget_usd=config.limits.max_budget_usd,
        effort=config.effort,
        thinking=dict(config.thinking) if config.thinking is not None else None,
        env=dict(env),
        cwd=cwd,
        include_partial_messages=False,
    )


@dataclass
class SessionOutcome:
    """How a session ended.

    ``timed_out``: the wall-clock deadline fired. ``interrupted``: the stop
    request (SIGTERM / SRW cancel) fired. At most one of them is set; either way
    ``result`` holds the CLI's ResultMessage if one arrived within the grace.
    ``error``: an SDK/CLI failure, described as ``"<Type>: <message>"``.
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


async def _settle(task: asyncio.Task | None) -> None:
    """Cancel ``task`` if still running and wait for it to finish.

    ``asyncio.wait`` never raises the task's own exception, but a cancellation of
    the caller still propagates. A task that ignores cancellation is abandoned
    after ``SETTLE_WAIT_SECONDS`` so the session stays bounded.
    """
    if task is None:
        return
    if not task.done():
        task.cancel()
        await asyncio.wait({task}, timeout=SETTLE_WAIT_SECONDS)
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
) -> SessionOutcome:
    """Run ``prompt`` to the first ResultMessage, the deadline or ``stop``.

    Never raises for SDK/CLI failures (they become ``error``);
    ``asyncio.CancelledError`` propagates after the client is disconnected.
    After an interrupt it waits up to ``INTERRUPT_GRACE_SECONDS`` for the
    ResultMessage, then disconnects regardless.
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
    client: Any = None
    connected = False
    conversation: asyncio.Task | None = None
    stop_waiter: asyncio.Task | None = None
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
        stop_waiter = asyncio.create_task(stop.wait())
        await asyncio.wait(
            {conversation, stop_waiter},
            timeout=max(0.0, deadline - loop.time()),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not conversation.done():
            if stop.is_set():
                outcome.interrupted = True
                _note(transcript, {"event": "stop"})
            else:
                outcome.timed_out = True
                _note(
                    transcript,
                    {"event": "deadline", "deadlineSeconds": deadline_seconds},
                )
            await _interrupt(client, conversation, connected, transcript)
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
        await _settle(stop_waiter)
        await _settle(conversation)
        if client is not None:
            await _disconnect(client, transcript)
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
    client: Any, conversation: asyncio.Task, connected: bool, transcript: Transcript
) -> None:
    """Interrupt the CLI and give the conversation the grace to deliver a result.

    Before the client is connected there is nothing to interrupt: the caller
    cancels the conversation. If the interrupt itself fails the CLI is not
    listening, so waiting the grace out would not help either.
    """
    if not connected:
        _note(transcript, {"event": "interrupt_skipped", "reason": "not_connected"})
        return
    loop = asyncio.get_running_loop()
    grace = INTERRUPT_GRACE_SECONDS
    grace_end = loop.time() + grace
    try:
        await asyncio.wait_for(client.interrupt(), timeout=grace)
    except Exception as exc:  # noqa: BLE001 - includes TimeoutError
        _note(transcript, {"event": "interrupt_failed", "error": _describe(exc)})
        return
    _note(transcript, {"event": "interrupt_sent"})
    await asyncio.wait({conversation}, timeout=max(0.0, grace_end - loop.time()))


async def _disconnect(client: Any, transcript: Transcript) -> None:
    closing = asyncio.ensure_future(client.disconnect())
    done, _ = await asyncio.wait({closing}, timeout=DISCONNECT_WAIT_SECONDS)
    if not done:
        _note(transcript, {"event": "disconnect_timeout"})
        return
    exc = closing.exception()
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
