"""An End while a pinned attach waits for its workspace stops the attach.

Owner End on a pinned session whose runtime has registered but not attached
authorizes the life's retirement at once. From then on the orchestrator's
workspace endpoint refuses the runtime with 409 ``session_ending`` (the
exact generation bound), and only turns that into ``session_ended`` after the
retirement settles -- which for a VM needs this very process to be gone.
The attach must therefore treat ``session_ending`` for its own life as the
end of the attach: stop waiting, run the normal failed-attach cleanup (its
proof is kept), and leave through the ended-session exit. A refusal naming
another generation means this attach was superseded.

Live evidence (R3 execution note §13.13 ``vcancel7``): the poll mapped every
such 409 to "workspace status unavailable" and kept waiting within its 900 s
VM budget; the Pod was only stopped by the orchestrator's claimant deletion
and a SIGKILL, about 16 minutes after the End.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

import agent.api.persistent_app as pa
from agent.api import session_workspace
from agent.api.orchestrator_client import OrchestratorClient, SessionEnded
from agent.api.session_contract import WorkspaceNotReady

G1 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
T1 = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1"
G2 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2"
THREAD = "10000000-0000-4000-8000-00000000000a"
AGENT = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
POD_UID = "pod-uid-end-during-boot"


class _Orchestrator:
    """The workspace endpoint as the orchestrator answers a booting VM life."""

    def __init__(self, *, ending_generation: str | None) -> None:
        self.ending_generation = ending_generation
        self.ended = False
        self.reads_after_end = 0
        self.releases: list[dict[str, Any]] = []

    def end(self) -> None:
        self.ended = True

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/workspace"):
            if self.ended:
                self.reads_after_end += 1
                detail: dict[str, Any] = {
                    "code": "session_ending",
                    "message": "This session is finishing its exact runtime cleanup.",
                    "retirement_disposition": "ended",
                }
                if self.ending_generation is not None:
                    detail["pinned_runtime_generation_contract"] = 1
                    detail["session_runtime_generation"] = self.ending_generation
                return httpx.Response(409, json={"detail": detail})
            return httpx.Response(
                200,
                json={
                    "vm_status": "provisioning",
                    "config_override": {"workspace": {"backend": "vm"}},
                    "pinned_runtime_generation_contract": 1,
                    "session_runtime_generation": G1,
                },
            )
        if "release" in path:
            self.releases.append(json.loads(request.content or b"{}"))
            return httpx.Response(409, json={"detail": {"code": "session_ending"}})
        return httpx.Response(404)


def _client(orchestrator: _Orchestrator) -> OrchestratorClient:
    client = OrchestratorClient(
        orchestrator_url="http://orchestrator.test",
        pod_ip="10.0.0.1",
        pod_port=8001,
        hostname="agent-pod",
        config_name="session_base",
    )
    client.agent_id = AGENT
    client.dispatch_process_generation = "ffffffff-ffff-4fff-8fff-ffffffffffff"
    # A dedicated Pod receives its exact identity at registration.
    client.session_runtime_generation = G1
    client.session_runtime_attach_token = T1
    client.pinned_runtime_generation_contract = True
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(orchestrator.handler)
    )
    return client


@pytest.fixture
def boot(monkeypatch):
    """A dedicated pinned VM attach whose VM is still booting."""

    from shared.runtime.core.loader import AgentConfig

    monkeypatch.setenv("POD_UID", POD_UID)
    monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)
    agent = MagicMock()
    agent.config = AgentConfig(agent_id="session_base", display_name="Session")
    monkeypatch.setattr(pa, "_agent", agent)
    monkeypatch.setattr(pa, "_session", None)
    monkeypatch.setattr(pa, "_event_writer", None)
    for name in ("_thread_id", "_session_generation", "_attach_token"):
        monkeypatch.setattr(pa._session_identity, name, None)
    monkeypatch.setattr(pa._session_identity, "_runtime_contract", False)
    for name in ("_release_receipt", "_cleanup_context"):
        monkeypatch.setattr(pa._session_attach, name, None)
    monkeypatch.setattr(pa, "PersistentSession", MagicMock(side_effect=AssertionError))
    real_poll = session_workspace.poll_workspace_ready

    async def bounded_poll(client, thread_id, **kwargs):
        # Bounded so the pre-fix behaviour (wait out the budget) terminates.
        kwargs.update(timeout=1.0, vm_timeout=1.0, poll_interval=0.01)
        return await real_poll(client, thread_id, **kwargs)

    monkeypatch.setattr(session_workspace, "poll_workspace_ready", bounded_poll)

    def run(orchestrator: _Orchestrator):
        client = _client(orchestrator)
        monkeypatch.setattr(pa, "_orchestrator_client", client)
        polls = {"count": 0}
        get = client.get_thread_workspace

        async def get_thread_workspace(thread_id, **kwargs):
            polls["count"] += 1
            if polls["count"] == 2:
                # The owner's End lands while the poll waits for the VM.
                orchestrator.end()
            return await get(thread_id, **kwargs)

        client.get_thread_workspace = get_thread_workspace
        return pa._session_attach.attach(THREAD)

    yield run
    pa._session_input.teardown()


@pytest.mark.asyncio
async def test_end_during_vm_boot_ends_the_attach_at_the_first_refusal(boot):
    orchestrator = _Orchestrator(ending_generation=G1)

    with pytest.raises(SessionEnded):
        await boot(orchestrator)

    assert orchestrator.reads_after_end == 1
    # The ordinary failed-attach cleanup ran and kept its exact proof.
    assert pa._session is None
    assert pa._session_identity.session_generation is None
    receipt = pa._session_attach.release_receipt
    assert receipt["session_runtime_generation"] == G1
    assert receipt["session_runtime_attach_token"] == T1
    assert receipt["local_quiescence_protocol"] == "agent_attach_not_started_v1"
    assert pa._session_attach.cleanup_context is None


@pytest.mark.asyncio
async def test_a_refusal_for_another_generation_is_a_superseded_attach(boot):
    orchestrator = _Orchestrator(ending_generation=G2)

    with pytest.raises(WorkspaceNotReady, match="generation changed") as raised:
        await boot(orchestrator)

    assert not isinstance(raised.value, SessionEnded)
    assert orchestrator.reads_after_end == 1


@pytest.mark.asyncio
async def test_an_unbound_ending_refusal_still_ends_this_attach(boot):
    """An older orchestrator names no generation: the thread's life is ending."""

    orchestrator = _Orchestrator(ending_generation=None)

    with pytest.raises(SessionEnded):
        await boot(orchestrator)
    assert orchestrator.reads_after_end == 1


@pytest.mark.asyncio
async def test_the_dedicated_boot_leaves_through_the_ended_session_exit(
    boot, monkeypatch
):
    """The lifespan maps the ending fence to the ended-session exit: no
    second End, no release loop, no wait for the VM."""

    ended = AsyncMock()
    not_ready = AsyncMock()
    monkeypatch.setattr(pa, "_exit_session_ended", ended)
    monkeypatch.setattr(pa, "_exit_workspace_not_ready", not_ready)
    orchestrator = _Orchestrator(ending_generation=G1)
    client = _client(orchestrator)
    orchestrator.end()

    fake_agent = MagicMock()
    fake_agent.initialize = AsyncMock()
    fake_agent.shutdown = AsyncMock()
    fake_agent.config.agent_id = "session_base"
    client.connect = AsyncMock()
    client.register = AsyncMock(return_value=True)
    client.run_heartbeat_loop = AsyncMock()
    client.stop_heartbeat = MagicMock()
    client.deregister = AsyncMock()
    client.close = AsyncMock()
    monkeypatch.setattr(pa.UniversalAgent, "from_config", lambda _path: fake_agent)
    monkeypatch.setattr(pa, "create_orchestrator_client_from_env", lambda _id: client)
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://orchestrator.test")
    pa._session_identity.bind_thread(THREAD)
    monkeypatch.setattr(pa, "_config_path", "session_base")

    manager = pa.lifespan(MagicMock())
    await manager.__aenter__()
    try:
        ended.assert_awaited_once_with(THREAD)
        not_ready.assert_not_awaited()
        assert orchestrator.reads_after_end >= 1
    finally:
        pa._session = None
        await manager.__aexit__(None, None, None)


class _ConstructedSession:
    """Enough of a constructed session for the construction/cleanup path."""

    def __init__(self, **kwargs: Any) -> None:
        from pathlib import Path
        from types import SimpleNamespace

        self.kwargs = kwargs
        self.thread_id = kwargs.get("thread_id")
        self.protected_cloud_required = False
        self.cloud_mount_manager = None
        self.cloud_mount_error = None
        self.workspace_manager = SimpleNamespace(
            path=Path("/workspace"), backend=MagicMock(), git_manager=None
        )
        self.workspace_sync = None
        self.postgres_conn = None
        self.tool_context = None
        self.llm_with_tools = None
        self.local_quiescence_protocol = "workspace_process_zero_v1"
        self.workspace_generation = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        self.workspace_runtime_incarnation = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
        self.cleanups: list[dict[str, Any]] = []

    async def setup(self, **_kwargs: Any) -> None:
        self.llm_with_tools = object()

    async def cleanup(self, **kwargs: Any) -> None:
        self.cleanups.append(kwargs)

    async def recover_subagents(self) -> None:
        return None


@pytest.mark.asyncio
async def test_end_after_construction_retires_the_partial_runtime(monkeypatch):
    """An End first seen after the one-way boundary (the late workspace read)
    still ends the attach; the constructed runtime is retired destructively
    and its exact proof is retained -- nothing is published as ready."""

    from shared.runtime.core.loader import AgentConfig

    monkeypatch.setenv("POD_UID", POD_UID)
    monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)
    agent = MagicMock()
    agent.config = AgentConfig(agent_id="session_base", display_name="Session")
    agent.postgres_conn = None
    monkeypatch.setattr(pa, "_agent", agent)
    monkeypatch.setattr(pa, "_session", None)
    monkeypatch.setattr(pa, "_event_writer", None)
    for name in ("_thread_id", "_session_generation", "_attach_token"):
        monkeypatch.setattr(pa._session_identity, name, None)
    for name in ("_release_receipt", "_cleanup_context"):
        monkeypatch.setattr(pa._session_attach, name, None)
    built: list[_ConstructedSession] = []

    def factory(**kwargs):
        built.append(_ConstructedSession(**kwargs))
        return built[-1]

    monkeypatch.setattr(pa, "PersistentSession", factory)
    status = AsyncMock(return_value=True)
    monkeypatch.setattr(pa, "_update_thread_status", status)
    import agent.tools.registry as registry

    monkeypatch.setattr(registry, "register_mcp_tools", MagicMock())
    monkeypatch.setattr(
        "agent.api.session_attach.apply_session_embedding_env", MagicMock()
    )
    ready = {
        "status": "ready",
        "backend": "sandbox",
        "protected_cloud": False,
        "pod_ip": "10.0.0.9",
        "remote": {"host": "10.0.0.9", "port": 22},
        "workspace_generation": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        "workspace_runtime_incarnation": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        "workspace_ssh_host_key_fingerprint": "SHA256:x",
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": G1,
    }

    async def poll(client, thread_id, **_kwargs):
        return dict(ready)

    monkeypatch.setattr(session_workspace, "poll_workspace_ready", poll)
    orchestrator = _Orchestrator(ending_generation=G1)
    client = _client(orchestrator)
    reads = {"count": 0}

    async def get_thread_workspace(thread_id, **kwargs):
        reads["count"] += 1
        if reads["count"] == 1:
            return dict(ready)  # the revalidation read before construction
        orchestrator.end()  # the End lands before the late cloud read
        return await OrchestratorClient.get_thread_workspace(
            client, thread_id, **kwargs
        )

    client.get_thread_workspace = get_thread_workspace
    monkeypatch.setattr(pa, "_orchestrator_client", client)

    try:
        with pytest.raises(SessionEnded):
            await pa._session_attach.attach(
                THREAD,
                config_override={"workspace": {"backend": "sandbox"}},
            )
    finally:
        pa._session_input.teardown()

    (session,) = built
    assert session.cleanups == [
        {"preserve_shell": False, "preserve_workspace_daemons": False}
    ]
    status.assert_not_awaited()
    assert pa._session is None
    receipt = pa._session_attach.release_receipt
    assert receipt["local_quiescence_protocol"] == "workspace_process_zero_v1"
    assert receipt["session_runtime_generation"] == G1


@pytest.mark.asyncio
async def test_cancelling_the_attach_while_it_waits_keeps_the_cleanup_proof(boot):
    """Task cancellation is not a proof: the wrapper still runs the exact
    cleanup, which here can truthfully prove only 'setup never started'."""

    import asyncio

    orchestrator = _Orchestrator(ending_generation=G1)
    waiting = asyncio.Event()
    real = session_workspace.poll_workspace_ready

    async def parked_poll(client, thread_id, **kwargs):
        waiting.set()
        await asyncio.Event().wait()
        return await real(client, thread_id, **kwargs)

    session_workspace.poll_workspace_ready = parked_poll
    try:
        task = asyncio.ensure_future(boot(orchestrator))
        await waiting.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        session_workspace.poll_workspace_ready = real

    receipt = pa._session_attach.release_receipt
    assert receipt["local_quiescence_protocol"] == "agent_attach_not_started_v1"
    assert receipt["session_runtime_generation"] == G1
    assert pa._session_identity.session_generation is None
    assert pa._session is None
