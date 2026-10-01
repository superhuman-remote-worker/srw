"""R3.3b characterization: session attach and runtime identity at stable seams.

Written and run against ``10f03d02f`` before ownership moves. The tests drive
only entry points that survive the extraction -- the exception-safe attach
transaction, pool admission, the receipt/release and cleanup operations, the
runtime identity reader the input owner consumes, dual ``/session/attach``
and the teardown used by pool reuse -- and pin what they publish, refuse and
clear. The **arrangement adapter** below is the only block the extraction may
change: it says where identity, pool-claim and receipt state live and which
callable is the attach entry point.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests._session_termination_adapter import termination_target

import agent.api.persistent_app as pa
from agent.api.lease_context import LeaseHandle, current_lease
from agent.api.orchestrator_client import OrchestratorClient
from agent.api.session_contract import SessionIdentityMismatch, WorkspaceNotReady
from shared.pinned_session_identity import pinned_session_ready_identity_fingerprint

# ---------------------------------------------------------------------------
# Arrangement adapter -- the only part R3.3b may edit.
# ---------------------------------------------------------------------------

_IDENTITY_FIELDS = {
    "thread_id": "_thread_id",
    "generation": "_session_generation",
    "attach_token": "_attach_token",
    "runtime_contract": "_runtime_contract",
    "status_contract": "_status_contract",
    "process_generation": "_process_generation",
    "attach_generation": "_attach_generation",
}
_ATTACH_STATE_FIELDS = (
    "_pool_claim",
    "_pool_claim_generation",
    "_pool_claim_token",
    "_pool_task",
    "_release_receipt",
    "_release_restore_thread_id",
    "_cleanup_context",
)
# Collaborators that moved to a pure owner module with the coordinator.
_MOVED_COLLABORATORS = {
    "_poll_workspace_ready": ("agent.api.session_workspace", "poll_workspace_ready"),
    "_apply_session_embedding_env": (
        "agent.api.session_attach",
        "apply_session_embedding_env",
    ),
    "_strict_cleanup_partial_sandbox_workspace": (
        "agent.api.session_attach",
        "strict_cleanup_partial_sandbox_workspace",
    ),
}


def identity() -> dict[str, Any]:
    owner = pa._session_identity
    return {key: getattr(owner, name) for key, name in _IDENTITY_FIELDS.items()}


def seed_identity(monkeypatch, **values: Any) -> None:
    for key, value in values.items():
        monkeypatch.setattr(pa._session_identity, _IDENTITY_FIELDS[key], value)


def identity_snapshot():
    return pa._session_identity.snapshot()


def fingerprint() -> str | None:
    return pa._session_identity.fingerprint()


def attach(**kwargs: Any):
    return pa._session_attach.attach(**kwargs)


def pool_admit(request: dict[str, Any]):
    return pa._pool_session_attach_response(request)


def pool_claim() -> tuple[Any, Any, Any]:
    return pa._session_attach.pool_claim


def pool_task():
    return pa._session_attach.pool_task


def heartbeat_status() -> str:
    return pa._session_attach.pool_heartbeat_status()


def release_receipt():
    return pa._session_attach.release_receipt


def set_release_receipt(receipt) -> None:
    pa._session_attach._release_receipt = receipt


def retain_receipt(receipt) -> bool:
    return pa._session_attach.retain_release_receipt(receipt)


def release_until_confirmed(thread_id, generation, token):
    return pa._session_attach.release_receipt_until_confirmed(
        thread_id, runtime_generation=generation, runtime_attach_token=token
    )


def cleanup_until_proven(thread_id, restore_thread_id=None):
    return pa._session_attach.cleanup_failed_attach_until_proven(
        thread_id, restore_thread_id=restore_thread_id
    )


def cleanup_context():
    return pa._session_attach.cleanup_context


def patch_cleanup_step(monkeypatch, fake) -> None:
    """Replace the single failed-attach cleanup attempt."""

    monkeypatch.setattr(pa._session_attach, "cleanup_failed_attach", fake)


def patch_retry_delays(monkeypatch, delays) -> None:
    import agent.api.session_attach as session_attach

    monkeypatch.setattr(session_attach, "EXACT_SETTLEMENT_RETRY_DELAYS", delays)


def apply_advertisement(batch_settle_contract, fanout) -> bool:
    return pa._session_attach.apply_subagent_advertisement(
        batch_settle_contract, fanout
    )


def patch_collaborator(monkeypatch, name: str, value: Any) -> None:
    """Collaborators that stay with the runtime (session factory, workspace
    poll, lifecycle CAS, restore, watchdogs, ...)."""

    moved = _MOVED_COLLABORATORS.get(name)
    if moved is not None:
        import importlib

        monkeypatch.setattr(importlib.import_module(moved[0]), moved[1], value)
        return
    monkeypatch.setattr(*termination_target(pa, name), value)


def reset_attach_state() -> None:
    pa._session = None
    owner = pa._session_identity
    for name in _IDENTITY_FIELDS.values():
        if name != "_attach_generation":
            setattr(owner, name, False if name.endswith("_contract") else None)
    for name in _ATTACH_STATE_FIELDS:
        setattr(pa._session_attach, name, None)


def saved_attach_state() -> tuple[Any, ...]:
    owner = pa._session_identity
    return (
        tuple(getattr(owner, name) for name in _IDENTITY_FIELDS.values()),
        tuple(getattr(pa._session_attach, name) for name in _ATTACH_STATE_FIELDS),
    )


def restore_attach_state(saved) -> None:
    identity_values, attach_values = saved
    for name, value in zip(_IDENTITY_FIELDS.values(), identity_values):
        setattr(pa._session_identity, name, value)
    for name, value in zip(_ATTACH_STATE_FIELDS, attach_values):
        setattr(pa._session_attach, name, value)


# ---------------------------------------------------------------------------
# Fixtures and fakes (stable across the extraction).
# ---------------------------------------------------------------------------

G1 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
T1 = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1"
G2 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2"
T2 = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2"
WSG = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
WSI = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
AGENT_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
POD_UID = "pod-uid-r33b"
TA = "10000000-0000-4000-8000-00000000000a"
TB = "10000000-0000-4000-8000-00000000000b"
TD = "10000000-0000-4000-8000-00000000000d"
TS = "10000000-0000-4000-8000-00000000000e"
TO = "10000000-0000-4000-8000-00000000000f"
PROCESS_GENERATION = "ffffffff-ffff-4fff-8fff-ffffffffffff"


def _workspace(generation: str | None = G1, **extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": "ready",
        "backend": "sandbox",
        "protected_cloud": False,
        "remote": {
            "host": "workspace.internal",
            "port": 22,
            "username": "agent-host",
            "key_path": "/run/secrets/workspace-key",
        },
        "workspace_generation": WSG,
        "workspace_runtime_incarnation": WSI,
        "workspace_ssh_host_key_fingerprint": "SHA256:exact-host",
    }
    if generation is not None:
        payload["pinned_runtime_generation_contract"] = 1
        payload["session_runtime_generation"] = generation
    payload.update(extra)
    return payload


class FakeSession:
    """The attached resources, as far as attach and teardown touch them."""

    instances: list["FakeSession"] = []
    fail_setup: BaseException | None = None

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.thread_id = kwargs.get("thread_id")
        self.config = kwargs.get("config")
        self.shell_owner_token = kwargs.get("shell_owner_token")
        self.protected_cloud_required = kwargs.get("protected_cloud_required", False)
        self.cloud_mount_manager = None
        self.cloud_mount_error = None
        self.workspace_manager = SimpleNamespace(
            path=Path("/workspace"), backend=MagicMock(), git_manager=None
        )
        self.workspace_sync = None
        self.postgres_conn = None
        self.tool_context = None
        self.llm_with_tools = None
        self.tools: list[Any] = []
        self.messages: list[Any] = []
        self.turn_count = 0
        self.memory_service = None
        self.final_memory_extracted = False
        self.terminal_finalization_attempted = False
        self.terminal_vm_drain_complete = False
        self.workspace_backend_tier = "sandbox"
        self.local_quiescence_protocol = "workspace_process_zero_v1"
        self.workspace_generation = WSG
        self.workspace_runtime_incarnation = WSI
        self.stateless_warm_reuse_safe = True
        self.cleanups: list[dict[str, Any]] = []
        self.advertisements: list[tuple[bool, bool]] = []
        self.batch_settle_contract = kwargs.get("subagent_batch_settle_contract")
        self.fanout = kwargs.get("subagent_fanout")
        FakeSession.instances.append(self)

    async def setup(self, **kwargs: Any) -> None:
        if FakeSession.fail_setup is not None:
            raise FakeSession.fail_setup
        self.setup_kwargs = kwargs
        self.llm_with_tools = object()

    async def recover_subagents(self) -> None:
        EVENTS.append(("recover", pa._session_input.queue is None))

    async def cleanup(self, **kwargs: Any) -> None:
        self.cleanups.append(kwargs)

    def protected_cloud_ready(self) -> bool:
        return True

    def apply_subagent_fanout_advertisement(self, *, batch_settle_contract, fanout):
        changed = (batch_settle_contract, fanout) != (
            self.batch_settle_contract,
            self.fanout,
        )
        self.batch_settle_contract, self.fanout = batch_settle_contract, fanout
        self.advertisements.append((batch_settle_contract, fanout))
        return changed

    async def quiesce_background_tasks(self) -> None:
        return None

    def retire_shell_owner(self) -> None:
        return None

    def set_shell_owner_token(self, token) -> None:
        self.shell_owner_token = token


EVENTS: list[tuple[str, Any]] = []


def _client(
    *, generation: str | None = None, attach_token: str | None = None, contract=False
) -> OrchestratorClient:
    client = OrchestratorClient(
        orchestrator_url="http://orchestrator.test",
        pod_ip="10.0.0.1",
        pod_port=8001,
        hostname="agent-pod",
        config_name="session_base",
    )
    client.agent_id = AGENT_ID
    client.dispatch_process_generation = PROCESS_GENERATION
    client.session_runtime_generation = generation
    client.session_runtime_attach_token = attach_token
    client.pinned_runtime_generation_contract = contract
    client.get_thread_workspace = AsyncMock(return_value=None)
    client.release_thread_agent = AsyncMock(return_value=True)
    return client


@pytest.fixture(autouse=True)
def _world(monkeypatch):
    saved = saved_attach_state()
    reset_attach_state()
    EVENTS.clear()
    FakeSession.instances = []
    FakeSession.fail_setup = None
    monkeypatch.setenv("POD_UID", POD_UID)
    monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)
    from shared.runtime.core.loader import AgentConfig

    agent = SimpleNamespace(
        config=AgentConfig(agent_id="session_base", display_name="Session"),
        _tactical_llm=None,
        _llm=object(),
        _auxiliary_llm=object(),
        postgres_conn=None,
        vector_conn=None,
    )
    monkeypatch.setattr(pa, "_agent", agent)
    monkeypatch.setattr(pa, "_orchestrator_client", None)
    monkeypatch.setattr(pa, "_event_writer", None)
    monkeypatch.setattr(pa, "_loop_task", None)
    monkeypatch.setattr(pa._session_termination, "retirement_admission_identity", None)
    monkeypatch.setattr(
        pa._session_termination, "retirement_admission_disposition", None
    )
    monkeypatch.setattr(pa._session_termination, "retirement_admission_token", None)
    monkeypatch.setattr(pa._session_termination, "retirement_admission_permanent", None)
    monkeypatch.setattr(pa._session_termination, "termination_admission_fenced", False)
    patch_collaborator(monkeypatch, "PersistentSession", FakeSession)
    patch_collaborator(monkeypatch, "_build_sync_coordinator", MagicMock())
    patch_collaborator(monkeypatch, "_apply_session_embedding_env", MagicMock())

    async def restore():
        EVENTS.append(("restore", pa._session_input.queue is None))

    async def status(value, **_kwargs):
        EVENTS.append(("status", value))
        return True

    def watchdogs():
        EVENTS.append(("watchdogs", pa._session_input.queue is not None))

    async def reclaim():
        EVENTS.append(("reclaim", pa._session_input.queue is not None))
        return set()

    patch_collaborator(monkeypatch, "_restore_session_messages", restore)
    patch_collaborator(monkeypatch, "_update_thread_status", status)
    patch_collaborator(monkeypatch, "_start_watchdogs", watchdogs)
    monkeypatch.setattr(pa._session_input, "reclaim_pending", reclaim)
    import agent.tools.registry as registry

    monkeypatch.setattr(registry, "register_mcp_tools", MagicMock())
    yield
    task = pool_task()
    if task is not None and not task.done():
        task.cancel()
    pa._session_input.teardown()
    reset_attach_state()
    restore_attach_state(saved)


def _poll(monkeypatch, *responses, on_call=None):
    calls: list[dict[str, Any]] = []
    queue = list(responses)

    async def poll(client, thread_id, **kwargs):
        calls.append({"thread_id": thread_id, **kwargs})
        if on_call is not None:
            on_call()
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(item, BaseException):
            raise item
        return item

    patch_collaborator(monkeypatch, "_poll_workspace_ready", poll)
    return calls


def _pinned_request(thread_id: str, generation: str, token: str | None, **extra):
    request: dict[str, Any] = {
        "thread_id": thread_id,
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": generation,
        "session_runtime_attach_token": token,
        "_recipient": {
            "expected_thread_id": thread_id,
            "expected_agent_id": AGENT_ID,
            "expected_pod_uid": POD_UID,
            "expected_process_generation": PROCESS_GENERATION,
        },
    }
    request.update(extra)
    return request


def _expected_fingerprint(thread_id, generation, token):
    expected = pinned_session_ready_identity_fingerprint(
        thread_id=thread_id,
        runtime_generation=generation,
        agent_id=AGENT_ID,
        runtime_attach_token=token,
        pod_uid=POD_UID,
    )
    assert expected is not None
    return expected


# ---------------------------------------------------------------------------
# A. Adoption, ordering and publication
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dedicated_attach_adopts_registration_identity_before_any_await(
    monkeypatch,
):
    client = _client(generation=G1, attach_token=T1, contract=True)
    client.get_thread_workspace = AsyncMock(return_value=_workspace())
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    seen: list[dict[str, Any]] = []

    def during_poll():
        seen.append(
            {
                "identity": identity(),
                "fingerprint": fingerprint(),
                "ready": pa._session_ready(),
                "client": (
                    client.session_runtime_generation,
                    client.session_runtime_attach_token,
                ),
            }
        )

    _poll(monkeypatch, _workspace(), on_call=during_poll)
    before = identity()["attach_generation"]

    await attach(thread_id=TA)

    (during,) = seen
    assert during["identity"]["thread_id"] == TA
    assert during["identity"]["generation"] == G1
    assert during["identity"]["attach_token"] == T1
    assert during["identity"]["runtime_contract"] is True
    assert during["identity"]["process_generation"] is None
    assert during["ready"] is False
    assert during["fingerprint"] == _expected_fingerprint(TA, G1, T1)
    assert during["client"] == (G1, T1)

    after = identity()
    assert after["attach_generation"] == before + 1
    assert str(uuid.UUID(after["process_generation"])) == after["process_generation"]
    assert after["process_generation"] not in {G1, T1}
    snap = identity_snapshot()
    assert (snap.thread_id, snap.session_generation, snap.attach_token) == (
        TA,
        G1,
        T1,
    )
    assert (snap.agent_id, snap.pod_uid, snap.lease) == (AGENT_ID, POD_UID, None)
    assert snap.process_generation == after["process_generation"]
    assert snap.attach_generation == after["attach_generation"]
    assert pa._session_ready() is True
    assert fingerprint() == _expected_fingerprint(TA, G1, T1)
    # Publication order: lifecycle CAS, recovery and restore with the queue
    # still closed, then pinned reclaim and watchdogs after it opened.
    assert EVENTS == [
        ("status", "active"),
        ("recover", True),
        ("restore", True),
        ("reclaim", True),
        ("watchdogs", True),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("stateless", [False, True])
async def test_attach_without_reclaimed_pinned_input_keeps_loop_lazy(
    monkeypatch, stateless
):
    workspace = _workspace(None if stateless else G1)
    client = _client(
        generation=None if stateless else G1,
        attach_token=None if stateless else T1,
        contract=not stateless,
    )
    client.get_thread_workspace = AsyncMock(return_value=workspace)
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    _poll(monkeypatch, workspace)
    start = MagicMock()
    patch_collaborator(monkeypatch, "_ensure_persistent_loop_started", start)
    token = None
    if stateless:
        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        lease = LeaseHandle()
        lease.update(TA, 7, executor_id="executor-a", pod_uid=POD_UID)
        token = current_lease.set(lease)
    try:
        await attach(thread_id=TA)
        start.assert_not_called()
        assert [name for name, _ in EVENTS].count("reclaim") == (0 if stateless else 1)
    finally:
        if token is not None:
            current_lease.reset(token)


@pytest.mark.asyncio
async def test_reclaimed_input_loop_start_still_refuses_closed_admission(monkeypatch):
    client = _client(generation=G1, attach_token=T1, contract=True)
    client.get_thread_workspace = AsyncMock(return_value=_workspace())
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    _poll(monkeypatch, _workspace())

    async def reclaim_then_fence():
        monkeypatch.setattr(
            pa._session_termination, "termination_admission_fenced", True
        )
        return {("captured-delivery", 2)}

    monkeypatch.setattr(pa._session_input, "reclaim_pending", reclaim_then_fence)
    original_start = pa._ensure_persistent_loop_started

    def refuse_start(source):
        assert not pa._session_ready(), "closed admission became ready during recovery"
        return original_start(source)

    start = MagicMock(side_effect=refuse_start)
    patch_collaborator(monkeypatch, "_ensure_persistent_loop_started", start)
    await attach(thread_id=TA)
    start.assert_called_once_with("attach_recovered_input")
    assert pa._loop_task is None
    assert not pa._session_ready()


@pytest.mark.asyncio
async def test_payload_identity_beats_registration_and_is_mirrored(monkeypatch):
    client = _client(generation=G1, attach_token=T1, contract=True)
    client.get_thread_workspace = AsyncMock(return_value=_workspace(G2))
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    _poll(monkeypatch, _workspace(G2))

    await attach(
        thread_id=TA,
        pinned_runtime_generation_contract=1,
        session_runtime_generation=G2,
        session_runtime_attach_token=T2,
    )

    assert (identity()["generation"], identity()["attach_token"]) == (G2, T2)
    assert (client.session_runtime_generation, client.session_runtime_attach_token) == (
        G2,
        T2,
    )
    assert fingerprint() == _expected_fingerprint(TA, G2, T2)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    (
        {"session_runtime_generation": "not-a-uuid"},
        {"session_runtime_generation": G1, "session_runtime_attach_token": "x"},
        {"pinned_runtime_generation_contract": 1, "session_runtime_generation": G1},
    ),
)
async def test_malformed_identity_refused_before_any_await(monkeypatch, payload):
    calls = _poll(monkeypatch, _workspace())
    seed_identity(monkeypatch, thread_id=None)

    with pytest.raises(WorkspaceNotReady):
        await attach(thread_id=TA, **payload)

    assert calls == []
    assert pa._session is None
    assert identity()["thread_id"] is None
    assert identity()["generation"] is None
    assert release_receipt() is None
    assert FakeSession.instances == []


@pytest.mark.asyncio
async def test_workspace_generation_drift_rolls_back_with_not_started_proof(
    monkeypatch,
):
    client = _client()
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    _poll(monkeypatch, _workspace(G2))

    with pytest.raises(WorkspaceNotReady, match="generation changed"):
        await attach(
            thread_id=TA,
            pinned_runtime_generation_contract=1,
            session_runtime_generation=G1,
            session_runtime_attach_token=T1,
        )

    assert pa._session is None
    assert identity()["thread_id"] == TA
    assert (identity()["generation"], identity()["attach_token"]) == (G1, T1)
    assert identity()["runtime_contract"] is True
    assert (client.session_runtime_generation, client.session_runtime_attach_token) == (
        G1,
        T1,
    )
    assert release_receipt() == {
        "thread_id": TA,
        "session_runtime_generation": G1,
        "session_runtime_attach_token": T1,
        "agent_pod_uid": POD_UID,
        "local_runtime_quiesced": True,
        "local_quiescence_protocol": "agent_attach_not_started_v1",
        "workspace_generation": None,
        "workspace_runtime_incarnation": None,
    }
    assert cleanup_context() is None
    assert heartbeat_status() == "session"


@pytest.mark.asyncio
async def test_contract_that_disappears_from_workspace_is_refused(monkeypatch):
    monkeypatch.setattr(pa, "_orchestrator_client", _client())
    _poll(monkeypatch, _workspace(None))

    with pytest.raises(WorkspaceNotReady, match="contract disappeared"):
        await attach(
            thread_id=TA,
            pinned_runtime_generation_contract=1,
            session_runtime_generation=G1,
            session_runtime_attach_token=T1,
        )

    assert pa._session is None
    assert release_receipt()["local_quiescence_protocol"] == (
        "agent_attach_not_started_v1"
    )


@pytest.mark.asyncio
async def test_legacy_attach_adopts_an_unadvertised_workspace_generation(
    monkeypatch,
):
    legacy = _workspace(None, session_runtime_generation=G2)
    client = _client()
    client.get_thread_workspace = AsyncMock(return_value=legacy)
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    _poll(monkeypatch, legacy)

    await attach(thread_id=TA)

    assert identity()["generation"] == G2
    assert identity()["attach_token"] is None
    assert identity()["runtime_contract"] is False
    assert client.session_runtime_generation == G2


@pytest.mark.asyncio
async def test_advertised_workspace_generation_without_attach_token_is_refused(
    monkeypatch,
):
    client = _client()
    client.get_thread_workspace = AsyncMock(return_value=_workspace(G2))
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    calls = _poll(monkeypatch, _workspace(G2))

    with pytest.raises(WorkspaceNotReady, match="omitted its exact generation"):
        await attach(thread_id=TA)

    # Refused at the dedicated peek, before the readiness poll.
    assert calls == []
    assert pa._session is None
    assert identity()["thread_id"] is None


@pytest.mark.asyncio
async def test_status_contract_follows_the_newest_workspace_response(monkeypatch):
    client = _client(generation=G1, attach_token=T1, contract=True)
    # Dedicated peek advertises the status contract; the final revalidation
    # response no longer does.
    client.get_thread_workspace = AsyncMock(
        side_effect=[_workspace(pinned_status_identity_contract=1), _workspace()]
    )
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    status_during_poll: list[bool] = []
    _poll(
        monkeypatch,
        _workspace(pinned_status_identity_contract=1),
        on_call=lambda: status_during_poll.append(identity()["status_contract"]),
    )

    await attach(thread_id=TA)

    assert status_during_poll == [True]
    assert identity()["status_contract"] is False


@pytest.mark.asyncio
async def test_status_contract_starts_from_the_payload(monkeypatch):
    client = _client(generation=G1, attach_token=T1, contract=True)
    seen: list[bool] = []

    async def fetch(thread_id, **_kwargs):
        seen.append(identity()["status_contract"])
        return None

    client.get_thread_workspace = fetch
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    _poll(monkeypatch, _workspace())

    await attach(thread_id=TA, pinned_status_identity_contract=1)

    # The payload value holds until a workspace response replaces it.
    assert seen[0] is True
    assert identity()["status_contract"] is False


# ---------------------------------------------------------------------------
# B. Failed attach at meaningful boundaries
# ---------------------------------------------------------------------------


async def _exact_attach(monkeypatch, **kwargs):
    await attach(
        thread_id=TA,
        pinned_runtime_generation_contract=1,
        session_runtime_generation=G1,
        session_runtime_attach_token=T1,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_constructor_failure_after_setup_boundary_requires_process_zero(
    monkeypatch,
):
    monkeypatch.setattr(pa, "_orchestrator_client", _client())
    _poll(monkeypatch, _workspace())
    patch_collaborator(
        monkeypatch, "PersistentSession", MagicMock(side_effect=RuntimeError("ctor"))
    )
    zero = AsyncMock(return_value="workspace_process_zero_v1")
    patch_collaborator(monkeypatch, "_strict_cleanup_partial_sandbox_workspace", zero)

    with pytest.raises(RuntimeError, match="ctor"):
        await _exact_attach(monkeypatch)

    (context,) = zero.await_args.args
    assert context["setup_started"] is True
    assert context["workspace_generation"] == WSG
    receipt = release_receipt()
    assert receipt["local_quiescence_protocol"] == "workspace_process_zero_v1"
    assert (
        receipt["workspace_generation"],
        receipt["workspace_runtime_incarnation"],
    ) == (
        WSG,
        WSI,
    )
    assert cleanup_context() is None
    assert pa._session is None


@pytest.mark.asyncio
async def test_session_setup_failure_retires_constructed_session_destructively(
    monkeypatch,
):
    monkeypatch.setattr(pa, "_orchestrator_client", _client())
    _poll(monkeypatch, _workspace())
    FakeSession.fail_setup = RuntimeError("setup refused")

    with pytest.raises(RuntimeError, match="setup refused"):
        await _exact_attach(monkeypatch)

    (session,) = FakeSession.instances
    assert session.cleanups == [
        {"preserve_shell": False, "preserve_workspace_daemons": False}
    ]
    assert release_receipt()["local_quiescence_protocol"] == (
        "workspace_process_zero_v1"
    )
    assert pa._session is None
    assert identity()["process_generation"] is None


@pytest.mark.asyncio
async def test_lifecycle_cas_refusal_aborts_before_queue_publication(monkeypatch):
    monkeypatch.setattr(pa, "_orchestrator_client", _client())
    _poll(monkeypatch, _workspace())
    ready_seen: list[bool] = []

    async def refused(value, **_kwargs):
        ready_seen.append(pa._session_ready())
        return False

    patch_collaborator(monkeypatch, "_update_thread_status", refused)

    with pytest.raises(Exception, match="lifecycle authority"):
        await _exact_attach(monkeypatch)

    assert ready_seen == [False]
    assert pa._session_input.queue is None
    assert pa._session is None
    assert identity()["thread_id"] == TA
    assert release_receipt()["thread_id"] == TA
    assert [name for name, _ in EVENTS] == []


@pytest.mark.asyncio
async def test_restore_failure_after_recovery_tears_down_input_and_generation(
    monkeypatch,
):
    monkeypatch.setattr(pa, "_orchestrator_client", _client())
    _poll(monkeypatch, _workspace())

    async def restore():
        EVENTS.append(("restore", pa._session_input.queue is None))
        raise RuntimeError("restore failed")

    patch_collaborator(monkeypatch, "_restore_session_messages", restore)

    with pytest.raises(RuntimeError, match="restore failed"):
        await _exact_attach(monkeypatch)

    assert EVENTS == [("status", "active"), ("recover", True), ("restore", True)]
    assert pa._session_input.queue is None
    assert identity()["process_generation"] is None
    assert pa._session_ready() is False
    assert release_receipt()["session_runtime_generation"] == G1


@pytest.mark.asyncio
async def test_vm_partial_setup_stays_nonclaimable_until_cleanup_is_proven(
    monkeypatch,
):
    attempts: list[str] = []
    receipt = {"thread_id": TA, "session_runtime_generation": G1}

    async def cleanup(thread_id, *, restore_thread_id=None):
        attempts.append(thread_id)
        if len(attempts) < 3:
            raise pa.EventJournalUnavailable("partial physical attach")
        return receipt

    seed_identity(
        monkeypatch,
        thread_id=TA,
        generation=G1,
        attach_token=T1,
        runtime_contract=True,
    )
    patch_cleanup_step(monkeypatch, cleanup)
    patch_retry_delays(monkeypatch, (0.0,))

    assert await cleanup_until_proven(TA) == receipt
    assert attempts == [TA] * 3


@pytest.mark.asyncio
async def test_cleanup_retry_stops_once_a_successor_identity_is_adopted(monkeypatch):
    attempts: list[str] = []

    async def cleanup(thread_id, *, restore_thread_id=None):
        attempts.append(thread_id)
        # A successor adopts its own identity between attempts.
        seed_identity(monkeypatch, generation=G2, attach_token=T2)
        raise pa.EventJournalUnavailable("still unproven")

    seed_identity(
        monkeypatch,
        thread_id=TA,
        generation=G1,
        attach_token=T1,
        runtime_contract=True,
    )
    patch_cleanup_step(monkeypatch, cleanup)
    patch_retry_delays(monkeypatch, (0.0,))

    with pytest.raises(pa.EventJournalUnavailable):
        await cleanup_until_proven(TA)
    assert attempts == [TA]
    assert (identity()["generation"], identity()["attach_token"]) == (G2, T2)


# ---------------------------------------------------------------------------
# C. Receipts, release and the pool claim
# ---------------------------------------------------------------------------


def _receipt(thread_id=TA, generation=G1, token=T1):
    return {
        "thread_id": thread_id,
        "session_runtime_generation": generation,
        "session_runtime_attach_token": token,
        "agent_pod_uid": POD_UID,
        "local_runtime_quiesced": True,
        "local_quiescence_protocol": "agent_runtime_zero_v1",
        "workspace_generation": None,
        "workspace_runtime_incarnation": None,
    }


def test_retained_receipt_incumbent_wins_and_equal_is_idempotent():
    first = _receipt()
    assert retain_receipt(first) is True
    assert retain_receipt(dict(first)) is True
    assert retain_receipt(_receipt(generation=G2, token=T2)) is False
    assert release_receipt() == first
    assert retain_receipt("not-a-receipt") is False


@pytest.mark.asyncio
async def test_release_loop_stops_when_a_successor_replaces_the_receipt(monkeypatch):
    client = _client()
    successor = _receipt(generation=G2, token=T2)

    async def refuse(*_args, **_kwargs):
        set_release_receipt(successor)
        return False

    client.release_thread_agent = AsyncMock(side_effect=refuse)
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    set_release_receipt(_receipt())
    patch_retry_delays(monkeypatch, (0.0,))

    assert await release_until_confirmed(TA, G1, T1) is False
    assert client.release_thread_agent.await_count == 1
    assert release_receipt() == successor


@pytest.mark.asyncio
async def test_release_refuses_a_receipt_for_another_identity(monkeypatch):
    client = _client()
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    set_release_receipt(_receipt(generation=G2, token=T2))

    assert await release_until_confirmed(TA, G1, T1) is False
    client.release_thread_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_pool_admission_claims_synchronously_and_refuses_duplicates(
    monkeypatch,
):
    client = _client()
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    entered = asyncio.Event()
    hold = asyncio.Event()

    async def blocked(client_, thread_id, **_kwargs):
        entered.set()
        await hold.wait()
        return _workspace()

    patch_collaborator(monkeypatch, "_poll_workspace_ready", blocked)
    request = _pinned_request(TA, G1, T1)

    first = await pool_admit(dict(request))
    assert first.status_code == 200
    assert pool_claim() == (TA, G1, T1)
    assert heartbeat_status() == "session"
    assert (identity()["generation"], identity()["attach_token"]) == (G1, T1)
    await entered.wait()

    duplicate = await pool_admit(dict(request))
    other = await pool_admit(_pinned_request(TB, G2, T2))
    assert duplicate.status_code == other.status_code == 409
    assert pool_claim() == (TA, G1, T1)
    assert (identity()["generation"], identity()["attach_token"]) == (G1, T1)

    hold.set()
    await pool_task()
    assert pool_claim() == (None, None, None)
    assert pa._session is not None and pa._session_ready() is True
    assert heartbeat_status() == "session"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    (
        {"session_runtime_generation": "bad"},
        {"session_runtime_attach_token": None},
    ),
)
async def test_pool_admission_refuses_inexact_identity_without_a_claim(
    monkeypatch, overrides
):
    monkeypatch.setattr(pa, "_orchestrator_client", _client())
    request = _pinned_request(TA, G1, T1, **overrides)

    response = await pool_admit(request)

    assert response.status_code == 409
    assert pool_claim() == (None, None, None)
    assert pool_task() is None
    assert identity()["generation"] is None
    assert heartbeat_status() == "ready"


@pytest.mark.asyncio
async def test_pool_failure_confirmed_release_reopens_the_pool(monkeypatch):
    client = _client()
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    _poll(monkeypatch, _workspace(G2))

    response = await pool_admit(_pinned_request(TA, G1, T1))
    assert response.status_code == 200
    await pool_task()

    client.release_thread_agent.assert_awaited_once()
    assert (
        client.release_thread_agent.await_args.kwargs["session_runtime_generation"]
        == G1
    )
    assert pool_claim() == (None, None, None)
    assert pool_task() is None
    assert release_receipt() is None
    assert identity()["generation"] is None
    assert heartbeat_status() == "ready"


@pytest.mark.asyncio
async def test_pool_failure_unconfirmed_release_keeps_the_claim(monkeypatch):
    client = _client()
    released = asyncio.Event()

    async def unconfirmed(*_args, **_kwargs):
        released.set()
        return False

    client.release_thread_agent = AsyncMock(side_effect=unconfirmed)
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    _poll(monkeypatch, _workspace(G2))
    patch_retry_delays(monkeypatch, (0.0, 0.01))

    await pool_admit(_pinned_request(TA, G1, T1))
    await released.wait()
    assert pool_claim() == (TA, G1, T1)
    assert heartbeat_status() == "session"
    task = pool_task()
    task.cancel()
    # The release step contains cancellation: the task ends normally and the
    # unconfirmed claim and receipt stay retained (non-ready, nonclaimable).
    await task
    assert pool_claim() == (TA, G1, T1)
    assert heartbeat_status() == "session"
    assert release_receipt()["session_runtime_generation"] == G1


# ---------------------------------------------------------------------------
# D. Replacement, stale identities and side-task scoping
# ---------------------------------------------------------------------------


async def _detach_for_reuse():
    await pa._session_termination.terminate(
        "claim_switch", mark_thread=False, preserve_shell=True
    )


@pytest.mark.asyncio
async def test_replacement_attach_serves_only_the_successor_identity(monkeypatch):
    client = _client()
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    patch_collaborator(monkeypatch, "_close_pinned_control_inbox", AsyncMock(True))
    patch_collaborator(monkeypatch, "_retire_announced_permission_rows", AsyncMock())
    _poll(monkeypatch, _workspace(G1))
    await _exact_attach(monkeypatch)
    first = identity()
    old_fingerprint = fingerprint()

    await _detach_for_reuse()
    assert pa._session is None
    assert identity()["thread_id"] is None
    assert (identity()["generation"], identity()["attach_token"]) == (None, None)
    assert identity()["process_generation"] is None
    assert (client.session_runtime_generation, client.session_runtime_attach_token) == (
        None,
        None,
    )

    _poll(monkeypatch, _workspace(G2))
    await attach(
        thread_id=TB,
        pinned_runtime_generation_contract=1,
        session_runtime_generation=G2,
        session_runtime_attach_token=T2,
    )
    second = identity()
    assert second["attach_generation"] == first["attach_generation"] + 1
    assert second["process_generation"] != first["process_generation"]
    snap = identity_snapshot()
    assert (snap.thread_id, snap.session_generation, snap.attach_token) == (
        TB,
        G2,
        T2,
    )
    assert fingerprint() != old_fingerprint
    with pytest.raises(SessionIdentityMismatch):
        await pa._session_input.accept(
            "stale", expected_session_identity_fingerprint=old_fingerprint
        )


@pytest.mark.asyncio
async def test_side_task_scope_does_not_match_the_next_attach(monkeypatch):
    monkeypatch.setattr(pa, "_orchestrator_client", _client())
    patch_collaborator(monkeypatch, "_close_pinned_control_inbox", AsyncMock(True))
    patch_collaborator(monkeypatch, "_retire_announced_permission_rows", AsyncMock())
    _poll(monkeypatch, _workspace(G1))
    await _exact_attach(monkeypatch)
    session = pa._session
    scope = (session, TA, identity()["attach_generation"])
    assert pa._session_identity_matches(*scope) is True

    await _detach_for_reuse()
    _poll(monkeypatch, _workspace(G1))
    await _exact_attach(monkeypatch)

    assert pa._session_identity_matches(*scope) is False


@pytest.mark.asyncio
async def test_attach_refuses_while_a_session_or_cleanup_proof_is_pending(
    monkeypatch,
):
    monkeypatch.setattr(pa, "_orchestrator_client", _client())
    _poll(monkeypatch, _workspace(G1))
    await _exact_attach(monkeypatch)
    attached = identity()

    with pytest.raises(RuntimeError, match="already attached"):
        await attach(thread_id=TB)
    # The refused request cannot have replaced the live identity.
    assert identity()["thread_id"] == TA
    assert identity()["attach_generation"] == attached["attach_generation"]


# ---------------------------------------------------------------------------
# E. Stateless lease identity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stateless_attach_reads_the_current_lease_on_every_snapshot(
    monkeypatch,
):
    monkeypatch.setenv("STATELESS_EXECUTOR", "1")
    client = _client()
    client.get_thread_workspace = AsyncMock(
        return_value=_workspace(None, session_subagent_fanout=True)
    )
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    _poll(monkeypatch, _workspace(None, session_subagent_fanout=True))
    handle = LeaseHandle()
    handle.update(TS, 7, executor_id="executor-a", pod_uid=POD_UID)
    token = current_lease.set(handle)
    try:
        # The claim bundle carries the capability but not the switch; the
        # workspace responses advertise both.
        await attach(thread_id=TS, session_subagent_batch_settle_contract=1)
        (session,) = FakeSession.instances
        assert session.kwargs["shell_owner_token"] == 7
        # Stateless never falls back to the workspace advertisement.
        assert session.kwargs["subagent_batch_settle_contract"] is True
        assert session.kwargs["subagent_fanout"] is False
        assert identity_snapshot().lease is handle
        handle.lease_token = 9
        assert identity_snapshot().lease.lease_token == 9
        assert [name for name, _ in EVENTS] == [
            "status",
            "recover",
            "restore",
            "watchdogs",
        ]
        assert apply_advertisement(1, True) is True
        assert apply_advertisement(1, True) is False
        assert session.advertisements[-1] == (True, True)
    finally:
        current_lease.reset(token)


@pytest.mark.asyncio
async def test_stateless_attach_refuses_a_lease_for_another_thread(monkeypatch):
    monkeypatch.setenv("STATELESS_EXECUTOR", "1")
    monkeypatch.setattr(pa, "_orchestrator_client", _client())
    _poll(monkeypatch, _workspace(None))
    other = LeaseHandle()
    other.update(TO, 3, executor_id="executor-a", pod_uid=POD_UID)
    token = current_lease.set(other)
    try:
        with pytest.raises(RuntimeError, match="lease identity"):
            await attach(thread_id=TS)
    finally:
        current_lease.reset(token)
    assert pa._session is None
    assert identity()["thread_id"] is None


# ---------------------------------------------------------------------------
# F. Delegation capability through attach
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_self_attaching_pinned_pod_takes_the_newest_ready_advertisement(
    monkeypatch,
):
    client = _client(generation=G1, attach_token=T1, contract=True)
    # The final revalidation fetch is the newest ready response.
    client.get_thread_workspace = AsyncMock(
        return_value=_workspace(
            session_subagent_batch_settle_contract=1, session_subagent_fanout=True
        )
    )
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    _poll(monkeypatch, _workspace(session_subagent_fanout=False))

    await attach(thread_id=TA)

    (session,) = FakeSession.instances
    assert session.kwargs["subagent_batch_settle_contract"] is True
    assert session.kwargs["subagent_fanout"] is True


@pytest.mark.asyncio
async def test_pushed_pool_advertisement_beats_the_workspace(monkeypatch):
    client = _client()
    client.get_thread_workspace = AsyncMock(
        return_value=_workspace(G1, session_subagent_fanout=True)
    )
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    _poll(monkeypatch, _workspace(G1, session_subagent_fanout=True))

    await _exact_attach(
        monkeypatch,
        session_subagent_batch_settle_contract=1,
        session_subagent_fanout=False,
    )

    (session,) = FakeSession.instances
    assert session.kwargs["subagent_fanout"] is False
    assert session.kwargs["subagent_batch_settle_contract"] is True


# ---------------------------------------------------------------------------
# G. Dual /session/attach reaches the same identity
# ---------------------------------------------------------------------------


@pytest.fixture
def dual(monkeypatch):
    import agent.api.dual_app as dual_app

    saved = (
        dual_app._pod_state,
        dual_app._session_attach_claim,
        dual_app._session_attach_task,
    )
    monkeypatch.setattr(dual_app, "_pod_state", dual_app.PodState.IDLE)
    monkeypatch.setattr(dual_app, "_session_attach_claim", None)
    monkeypatch.setattr(dual_app, "_session_attach_task", None)
    yield dual_app
    task = dual_app._session_attach_task
    if task is not None and not task.done():
        task.cancel()
    (
        dual_app._pod_state,
        dual_app._session_attach_claim,
        dual_app._session_attach_task,
    ) = saved


async def _dual_attach(dual_app, client, request):
    import httpx

    dual_app._orchestrator_client = client
    dual_app._agent = pa._agent
    app = dual_app.create_dual_app("session_base")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://agent"
    ) as http:
        response = await http.post("/session/attach", json=request)
    return response


@pytest.mark.asyncio
async def test_dual_attach_adopts_the_runtime_identity(monkeypatch, dual):
    client = _client()
    client.bind_pod_runtime_actor = AsyncMock(return_value=True)
    _poll(monkeypatch, _workspace(G1))
    monkeypatch.setattr(dual, "_orchestrator_client", client)

    response = await _dual_attach(
        dual,
        client,
        _pinned_request(TD, G1, T1),
    )
    assert response.status_code == 200
    await dual._session_attach_task

    assert (identity()["thread_id"], identity()["generation"]) == (TD, G1)
    assert identity()["attach_token"] == T1
    assert fingerprint() == _expected_fingerprint(TD, G1, T1)
    assert pa._session_ready() is True


@pytest.mark.asyncio
async def test_dual_attach_failure_clears_exactly_and_returns_idle(monkeypatch, dual):
    client = _client()
    client.bind_pod_runtime_actor = AsyncMock(return_value=True)
    _poll(monkeypatch, _workspace(G2))
    monkeypatch.setattr(dual, "_orchestrator_client", client)

    response = await _dual_attach(
        dual,
        client,
        _pinned_request(TD, G1, T1),
    )
    assert response.status_code == 200
    await dual._session_attach_task

    assert pa._session is None
    assert identity()["thread_id"] is None
    assert identity()["generation"] is None
    client.release_thread_agent.assert_awaited_once()
    assert dual._pod_state is dual.PodState.IDLE
    assert (client.session_runtime_generation, client.session_runtime_attach_token) == (
        None,
        None,
    )


@pytest.mark.asyncio
async def test_failed_attach_retains_exact_heartbeat_identity_until_release(
    monkeypatch,
):
    client = _client()
    client.release_thread_agent = AsyncMock(return_value=True)
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    _poll(monkeypatch, _workspace(G2))
    with pytest.raises(WorkspaceNotReady, match="generation changed"):
        await attach(
            thread_id=TA,
            pinned_runtime_generation_contract=1,
            pinned_status_identity_contract=1,
            session_runtime_generation=G1,
            session_runtime_attach_token=T1,
        )
    assert heartbeat_status() == "session"
    assert (
        identity()["thread_id"],
        identity()["generation"],
        identity()["attach_token"],
    ) == (TA, G1, T1)
    assert (client.session_runtime_generation, client.session_runtime_attach_token) == (
        G1,
        T1,
    )
    assert await release_until_confirmed(TA, G1, T1) is True
    assert identity()["generation"] is None
    assert identity()["attach_token"] is None
    assert identity()["thread_id"] is None
    assert heartbeat_status() == "ready"


@pytest.mark.asyncio
async def test_failed_attach_release_cannot_clear_successor_identity(monkeypatch):
    client = _client()
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    _poll(monkeypatch, _workspace(G2))
    with pytest.raises(WorkspaceNotReady):
        await attach(
            thread_id=TA,
            pinned_runtime_generation_contract=1,
            session_runtime_generation=G1,
            session_runtime_attach_token=T1,
        )

    async def confirm(*args, **kwargs):
        seed_identity(
            monkeypatch,
            thread_id=TB,
            generation=G2,
            attach_token=T2,
            runtime_contract=True,
            status_contract=True,
        )
        return True

    client.release_thread_agent = AsyncMock(side_effect=confirm)
    assert await release_until_confirmed(TA, G1, T1) is True
    assert (
        identity()["thread_id"],
        identity()["generation"],
        identity()["attach_token"],
    ) == (TB, G2, T2)
    assert identity()["status_contract"] is True
