"""Healthy VM End drains locally without claiming whole-guest process zero."""

from agent.api import session_termination
import re
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4
from unittest.mock import AsyncMock

import pytest

from agent.api import persistent_app as app
from agent.managers.git_manager import GitManager
from agent.tools.shell.shell_manager import ShellManager
from shared.runtime.core.backends.remote import RemoteBackend, _RemoteTab
from shared.runtime.core.workspace_backend import WorkspaceUnavailableError
from tests.test_persistent_session import _make_session


def native_vm_session(monkeypatch, *, tier="vm", shell_failure=False):
    """Only SSH/SFTP transports are replaced; shell and Git delegation are real."""
    events = []
    session = _make_session(
        pinned_runtime_identity_required=True,
        workspace_backend_tier=tier,
        workspace_generation=str(uuid4()),
        workspace_runtime_incarnation=str(uuid4()),
    )
    backend = RemoteBackend(
        host="192.0.2.10",
        job_id=session.thread_id,
        workspace_generation=session.workspace_generation,
        runtime_incarnation=session.workspace_runtime_incarnation,
        workspace_tier=tier,
        sudo_action="allow",
    )
    # Start from an attached, initialized shell. The fake SSH server returns
    # genuine protocol-shaped completion records to the native shell parser.
    backend._shell_initialized = True
    backend._tabs["git"] = _RemoteTab("git", pane_id="%1")
    scrollback = []

    def ssh(command, **_kwargs):
        if command.startswith("tmux capture-pane"):
            return "\n".join(scrollback), 0
        if "tmux kill-session" in command:
            events.append("shell-stop")
            if shell_failure:
                raise OSError("strict SSH acknowledgement lost")
            return "", 0
        if "tmux send-keys" in command:
            marker = re.search(r"__DONE_[0-9a-f]{12}__", command).group()
            if "git status" in command:
                events.append("git-status")
                output = ""
            elif "git remote" in command:
                output = "https://example.invalid/repo"
            elif "git branch" in command:
                output = "main"
            elif "git push" in command:
                events.append("git-push")
                output = ""
            else:
                raise AssertionError(command)
            scrollback.extend([output, f"{marker} 0 /home/agent-host/workspace"])
        return "", 0

    monkeypatch.setattr(backend, "_exec_with_status", ssh)
    monkeypatch.setattr(backend, "_remote_stat", lambda _path: object())
    session.workspace_manager = SimpleNamespace(
        backend=backend, git_manager=GitManager(Path("/workspace"), backend=backend)
    )
    session.shell_manager = ShellManager(session.thread_id, backend=backend)
    return session, backend, events


def attach_native_session(monkeypatch, session, events, *, lose_response=False):
    generation, attach, retirement = (str(uuid4()) for _ in range(3))
    owner = str(uuid4())
    accepted = []

    @asynccontextmanager
    async def transaction():
        yield

    @asynccontextmanager
    async def acquire():
        yield SimpleNamespace(
            transaction=transaction,
            fetchrow=AsyncMock(return_value={
                "agent_id": owner, "execution_lane": "pinned",
                "status": "ending", "runtime_generation": generation,
                "runtime_attach_token": attach, "runtime_retirement_token": retirement,
            }),
        )

    session.postgres_conn = SimpleNamespace(acquire=acquire)

    class Client:
        agent_id = owner
        dispatch_process_generation = str(uuid4())
        async def begin_thread_retirement(self, thread_id, **kwargs):
            events.append("begin")
            return {
                "status": "ending",
                "session_runtime_retirement_token": retirement,
                "retirement_disposition": "ended",
                "retirement_permanent": False,
            }

        async def request_thread_retirement_actuator(self, thread_id, **kwargs):
            assert session.local_quiescence_protocol == ""
            assert backend_retired()
            events.append("handoff")
            accepted.append((thread_id, kwargs))
            if lose_response and len(accepted) == 1:
                raise OSError("accepted handoff response lost")
            return {"status": "actuator_requested", **kwargs}

    def backend_retired():
        return session.workspace_manager.backend._retired

    values = {
        "_session": session,
        "_control_owner_agent_id": owner,
        "_orchestrator_client": Client(),
        # Begin has already returned this immutable authority. The terminal
        # path must reuse it, including on a response-loss retry.
        "_retirement_admission_identity": (session.thread_id, generation, attach),
        "_retirement_admission_token": retirement,
        "_retirement_admission_disposition": "ended",
        "_retirement_admission_permanent": False,
        "_loop_task": None,
        "_watchdog_tasks": [],
        "_event_writer": None,
        "_termination_task": None,
        "_terminating": False,
        "_max_sessions_per_process": 0,
        "_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS": (0,),
    }
    for name, value in values.items():
        fields = {
            "_retirement_admission_identity": "retirement_admission_identity",
            "_retirement_admission_token": "retirement_admission_token",
            "_retirement_admission_disposition": "retirement_admission_disposition",
            "_retirement_admission_permanent": "retirement_admission_permanent",
            "_watchdog_tasks": "watchdog_tasks",
            "_termination_task": "termination_task",
            "_terminating": "terminating",
            "_max_sessions_per_process": "max_sessions_per_process",
        }
        if name == "_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS":
            monkeypatch.setattr(session_termination, name, value)
        elif name in fields:
            monkeypatch.setattr(app._session_termination, fields[name], value)
        else:
            monkeypatch.setattr(app, name, value)
    for name, value in {
        "_thread_id": session.thread_id,
        "_session_generation": generation,
        "_attach_token": attach,
        "_runtime_contract": True,
        "_status_contract": True,
    }.items():
        monkeypatch.setattr(app._session_identity, name, value)
    monkeypatch.setattr(app, "_stateless_mode", lambda: False)
    monkeypatch.setenv("POD_UID", str(uuid4()))
    return accepted


@pytest.mark.asyncio
@pytest.mark.parametrize("tier", ["vm", "remote"])
@pytest.mark.parametrize("lose_response", [False, True])
async def test_native_terminal_vm_handoff_keeps_end_pending(
    monkeypatch, tier, lose_response
):
    session, backend, events = native_vm_session(monkeypatch, tier=tier)
    accepted = attach_native_session(
        monkeypatch, session, events, lose_response=lose_response
    )

    result = await app._session_termination._terminate_inner("shutdown")

    assert result == "actuator_requested"
    assert events.count("git-status") == 1
    assert events.count("git-push") == 1
    assert events.count("shell-stop") == 1
    assert events.index("git-push") < events.index("shell-stop")
    assert len(accepted) == (2 if lose_response else 1)
    assert accepted[-1] == accepted[0]
    assert backend._retired and backend._shell_retired
    assert session.local_quiescence_protocol == ""
    assert app._session is session
    assert app._session_termination.retirement_admission_closed()
    assert not app._session_ready()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["shell", "browser", "mount", "backend", "writer", "background"])
async def test_required_drain_failure_never_requests_vm_actuator(monkeypatch, failure):
    session, backend, events = native_vm_session(monkeypatch, shell_failure=failure == "shell")
    accepted = attach_native_session(monkeypatch, session, events)

    def fail(*_args, **_kwargs):
        raise WorkspaceUnavailableError("required drain acknowledgement unknown")

    if failure == "browser":
        session.tool_context = SimpleNamespace(
            close_browser=AsyncMock(side_effect=fail), citation_engine=None,
        )
    elif failure == "mount":
        session.cloud_mount_manager = SimpleNamespace(aclose=AsyncMock(side_effect=fail))
    elif failure == "backend":
        monkeypatch.setattr(backend, "retire", fail)
    elif failure == "writer":
        monkeypatch.setattr(app, "_event_writer", SimpleNamespace(close=AsyncMock(side_effect=fail)))
    elif failure == "background":
        session.memory_service = SimpleNamespace(close_background=AsyncMock(side_effect=fail))

    with pytest.raises((WorkspaceUnavailableError, app.EventJournalUnavailable)):
        await app._session_termination._terminate_inner("shutdown")
    assert accepted == []
    assert session.local_quiescence_protocol == ""
    assert app._session is session and app._session_termination.retirement_admission_closed()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["shell", "browser", "writer"])
async def test_required_drain_retry_does_not_replay_final_git(monkeypatch, failure):
    session, backend, events = native_vm_session(monkeypatch)
    accepted = attach_native_session(monkeypatch, session, events)
    if failure == "shell":
        ssh = backend._exec_with_status
        fail_once = True

        def transport(command, **kwargs):
            nonlocal fail_once
            if "tmux kill-session" in command and fail_once:
                fail_once = False
                raise OSError("remote stop unknown")
            return ssh(command, **kwargs)

        monkeypatch.setattr(backend, "_exec_with_status", transport)
    elif failure == "browser":
        session.tool_context = SimpleNamespace(
            close_browser=AsyncMock(side_effect=[OSError("browser stop unknown"), None]),
            citation_engine=None,
        )
    else:
        monkeypatch.setattr(app, "_event_writer", SimpleNamespace(
            close=AsyncMock(side_effect=[OSError("writer drain unknown"), None]),
        ))
    with pytest.raises((WorkspaceUnavailableError, app.EventJournalUnavailable)):
        await app._session_termination._terminate_inner("shutdown")
    assert not backend._retired and accepted == []
    assert await app._session_termination.terminate("shutdown") == "actuator_requested"
    assert events.count("git-status") == events.count("git-push") == 1
    assert backend._retired and len(accepted) == 1


@pytest.mark.asyncio
async def test_archive_handoff_sends_pending_without_false_end(monkeypatch):
    session, backend, events = native_vm_session(monkeypatch)
    attach_native_session(monkeypatch, session, events)
    frames = []

    async def send(_ws, method, params):
        frames.append((method, params))

    monkeypatch.setattr(app, "_ws_send", send)
    await app._handle_archive(object())
    assert [method for method, _ in frames] == ["session.ending"]
    assert app._session is session and backend._retired


@pytest.mark.asyncio
async def test_suspended_vm_retirement_keeps_existing_proof_refusal(monkeypatch):
    session, backend, events = native_vm_session(monkeypatch)
    accepted = attach_native_session(monkeypatch, session, events)
    monkeypatch.setattr(app._session_termination, "retirement_admission_disposition", "suspended")
    with pytest.raises(WorkspaceUnavailableError):
        await app._session_termination._terminate_inner("drain", mark_thread=False, preserve_shell=False)
    assert not backend._retired and accepted == []
    assert session.local_quiescence_protocol == ""
    assert app._session is session


def test_public_projection_excludes_vm_actuator_request():
    from orchestrator.services.thread_projection import redact_thread_metadata

    projected = redact_thread_metadata({
        "runtime_retirement_actuator_request": {"process_generation": "private"},
        "runtime_retirement_token": str(uuid4()),
        "runtime_retirement_authorized_at": "now",
        "runtime_retirement_context": {"settle_status": "ended"},
    })
    assert "runtime_retirement_actuator_request" not in projected
    assert projected["runtime_retirement_pending"] is True


@pytest.mark.asyncio
async def test_partially_retired_backend_retries_only_unfinished_local_close(monkeypatch):
    session, backend, events = native_vm_session(monkeypatch)
    accepted = attach_native_session(monkeypatch, session, events)
    retire = backend.retire
    first = True

    def retire_once_uncertain():
        nonlocal first
        retire()
        if first:
            first = False
            raise OSError("disconnect acknowledgement unavailable after admission retired")

    monkeypatch.setattr(backend, "retire", retire_once_uncertain)
    with pytest.raises(WorkspaceUnavailableError):
        await app._session_termination._terminate_inner("shutdown")
    assert backend._retired and not accepted
    assert await app._session_termination.terminate("shutdown") == "actuator_requested"
    assert events.count("git-push") == events.count("shell-stop") == 1


@pytest.mark.asyncio
async def test_optional_flush_failures_are_logged_once_and_do_not_block_drain(monkeypatch, caplog):
    session, backend, events = native_vm_session(monkeypatch)
    accepted = attach_native_session(monkeypatch, session, events, lose_response=True)
    ssh = backend._exec_with_status

    def unavailable_git(command, **kwargs):
        if "tmux send-keys" in command and "git push" in command:
            events.append("git-push-failed")
            raise OSError("optional remote unavailable")
        return ssh(command, **kwargs)

    monkeypatch.setattr(backend, "_exec_with_status", unavailable_git)
    session.workspace_sync = SimpleNamespace(
        push_all=AsyncMock(side_effect=OSError("optional cloud unavailable")),
        pull_all=AsyncMock(), aclose=AsyncMock(),
    )
    assert await app._session_termination.terminate("shutdown") == "actuator_requested"
    assert len(accepted) == 2 and backend._retired
    assert events.count("git-push-failed") == events.count("shell-stop") == 1
    assert "Final git push was unsuccessful" in caplog.text
    assert "Final cloud sync failed (non-fatal)" in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["shell", "backend"])
async def test_opt_in_requires_real_shell_and_backend_acknowledgements(monkeypatch, missing):
    session, _, _ = native_vm_session(monkeypatch)
    if missing == "shell":
        session.shell_manager = None
    else:
        session.workspace_manager = None
    with pytest.raises(WorkspaceUnavailableError, match="lacks a strict shell/backend"):
        await session.cleanup(allow_vm_actuator_handoff=True)
    assert not session.terminal_vm_drain_complete


@pytest.mark.asyncio
async def test_rest_detach_reports_pending_after_native_vm_drain(monkeypatch):
    import json

    session, backend, events = native_vm_session(monkeypatch)
    attach_native_session(monkeypatch, session, events)
    api = app.create_persistent_app("session_base", thread_id=session.thread_id)
    endpoint = next(route.endpoint for route in api.routes if route.path == "/session/detach")
    fingerprint = app._session_identity.fingerprint()
    assert fingerprint
    response = await endpoint({"session_identity_fingerprint": fingerprint})
    assert response.status_code == 202
    assert json.loads(response.body) == {"status": "ending", "thread_id": session.thread_id}
    assert app._session is session and backend._retired
    assert not app._session_ready()
