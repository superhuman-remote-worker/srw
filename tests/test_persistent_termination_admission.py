"""P0: a terminating persistent pod cannot admit another paid turn."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import WebSocketDisconnect
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from orchestrator.services import session_lifecycle, session_wake, sitrep
from agent.api import persistent_app, persistent_termination
from agent.api.persistent_session import PersistentSession
from agent.persistent_graph import (
    PersistentLoopCallbacks,
    _execute_turn,
    run_persistent_loop,
)
from shared.runtime.services.auxiliary import (
    AuxiliaryLLM,
    AuxiliaryProviderAdmissionClosed,
)
from agent.services.memory import CaptureEvent
from agent.services.memory.manager import MemoryManager
from shared.runtime.core.workspace_backend import WorkspaceUnavailableError
from shared.pinned_session_identity import PinnedSessionBinding


def _config() -> MagicMock:
    config = MagicMock()
    config.llm.timeout = 30
    config.memory.enabled = False
    config.memory.observer_interval = 5
    config.context_management.max_summary_length = 10_000
    config.officer.enabled = False
    return config


def _context_manager() -> MagicMock:
    manager = MagicMock()
    manager.ensure_within_limits = AsyncMock(
        side_effect=lambda messages, *_args, **_kwargs: messages
    )
    return manager


def _callbacks(inputs, **overrides) -> PersistentLoopCallbacks:
    source = iter(inputs)

    async def _input():
        try:
            return next(source)
        except StopIteration:
            raise asyncio.CancelledError from None

    values = {
        "get_user_input": _input,
        "on_token": AsyncMock(),
        "on_thinking": AsyncMock(),
        "on_tool_start": AsyncMock(),
        "on_tool_result": AsyncMock(),
        "permission_check": AsyncMock(return_value=True),
        "on_turn_start": AsyncMock(),
        "on_turn_complete": AsyncMock(),
        "on_error": AsyncMock(),
        "check_interrupt": MagicMock(return_value=False),
        "persist_message": AsyncMock(),
        "on_turn_settled": AsyncMock(),
    }
    values.update(overrides)
    return PersistentLoopCallbacks(**values)


class _InsertOnceDB:
    def __init__(self) -> None:
        self.ids: set[str] = set()
        self.rows: list[dict] = []
        self.deliveries: dict[str, dict] = {}
        self.after_persist = None
        self.runtime_retirement_token: str | None = None
        self.effect_authority_reads = 0

    async def verify_pinned_runtime_effect_authority(self, **_identity):
        self.effect_authority_reads += 1
        return self.runtime_retirement_token is None

    async def persist_pinned_input_delivery(self, **row):
        delivery_id = row["delivery_id"]
        inserted = delivery_id not in self.deliveries
        if inserted:
            self.rows.append(dict(row))
            self.deliveries[delivery_id] = {
                **row,
                "state": "owned",
                "claim_generation": 1,
                "message_id": str(uuid4()),
                "owner_runtime_generation": row["runtime_generation"],
            }
        result = self.deliveries[delivery_id]
        if not inserted and result["state"] == "deferred":
            result.update(
                state="owned",
                claim_generation=result["claim_generation"] + 1,
                owner_runtime_generation=row["runtime_generation"],
                runtime_generation=row["runtime_generation"],
            )
        if self.after_persist is not None:
            self.after_persist()
        return {**result, "transcript_inserted": inserted}

    async def mark_pinned_input_delivery_queued(self, **row):
        delivery = self.deliveries[row["delivery_id"]]
        if delivery["state"] not in {"owned", "queued"}:
            return False
        delivery["state"] = "queued"
        return True

    async def transition_pinned_input_delivery(self, **row):
        delivery = self.deliveries[row["delivery_id"]]
        transition = row["transition"]
        if transition == "deferred":
            delivery["state"] = "deferred"
        elif transition == "unadmit":
            delivery["state"] = "deferred"
        elif transition == "admitted":
            delivery["state"] = "admitted"
        elif transition == "settled":
            delivery["state"] = "settled"
        elif transition == "cancelled":
            if delivery["source"] != "direct_human":
                return False
            delivery["state"] = "cancelled"
            delivery["cancelled_turn_number"] = row["turn_number"]
            delivery["cancelled_reason"] = row["reason"]
        return True

    async def claim_pending_pinned_input_deliveries(self, **row):
        result = []
        for delivery in self.deliveries.values():
            if delivery["state"] in {"admitted", "settled", "cancelled"}:
                continue
            if (
                delivery["owner_runtime_generation"] != row["runtime_generation"]
                or delivery["state"] == "deferred"
            ):
                delivery["owner_runtime_generation"] = row["runtime_generation"]
                delivery["runtime_generation"] = row["runtime_generation"]
                delivery["claim_generation"] += 1
                delivery["state"] = "owned"
            result.append(delivery)
        return result


def _wire_input_runtime(monkeypatch, tmp_path, db, *, turn_count: int = 5):
    queue: asyncio.Queue = asyncio.Queue()
    monkeypatch.setattr(
        persistent_app,
        "_TERMINATION_SENTINEL_PATH",
        tmp_path / "terminating",
    )
    monkeypatch.setattr(persistent_app, "_termination_admission_fenced", False)
    monkeypatch.setattr(persistent_app, "_termination_fence_reason", None)
    monkeypatch.setattr(persistent_app, "_awaiting_input", False)
    monkeypatch.setattr(persistent_app, "_turn_event_open", False)
    monkeypatch.setattr(persistent_app, "_tool_inflight", False)
    monkeypatch.setattr(persistent_app, "_loop_user_queue", queue)
    monkeypatch.setattr(persistent_app, "_loop_last_user_content", [""])
    monkeypatch.setattr(persistent_app, "_input_delivery_reclaim_lock", asyncio.Lock())
    monkeypatch.setattr(persistent_app, "_thread_id", str(uuid4()))
    monkeypatch.setenv("POD_UID", "pod-uid-test")
    monkeypatch.setattr(
        persistent_app,
        "_orchestrator_client",
        SimpleNamespace(agent_id=str(uuid4())),
    )
    process_generation = str(uuid4())
    session_generation = str(uuid4())
    monkeypatch.setattr(persistent_app, "_input_runtime_generation", process_generation)
    monkeypatch.setattr(
        persistent_app, "_session_runtime_generation", session_generation
    )
    monkeypatch.setattr(persistent_app, "_session_runtime_attach_token", str(uuid4()))
    monkeypatch.setattr(persistent_app, "_pinned_runtime_generation_enabled", False)
    persistent_app._queued_input_claims.clear()
    monkeypatch.setattr(
        persistent_app,
        "_session",
        SimpleNamespace(
            postgres_conn=db,
            turn_count=turn_count,
            protected_cloud_required=False,
        ),
    )
    monkeypatch.setattr(persistent_app, "_broadcast", MagicMock())
    return queue


def _current_session_identity_fingerprint() -> str:
    value = persistent_app._current_pinned_session_identity_fingerprint()
    assert value is not None
    return value


async def _run_websocket_input(monkeypatch, content: str, *, before_receive=None):
    """Drive the production WS receive loop for exactly one human input."""

    session = persistent_app._session
    session.llm_with_tools = MagicMock()
    session.messages = []
    session.permission_mode = "supervised"
    session.narration_mode = "auto"
    session.config = SimpleNamespace(
        llm=SimpleNamespace(model="test-model", temperature=0.0)
    )
    session.session_task_manager = None

    receives = 0

    async def _receive_text():
        nonlocal receives
        receives += 1
        if receives == 1:
            if before_receive is not None:
                before_receive()
            return json.dumps({"method": "message", "content": content})
        raise WebSocketDisconnect()

    async def _park_pump(*_args, **_kwargs):
        await asyncio.Future()

    ws = AsyncMock()
    ws.state = SimpleNamespace(
        session_identity_fingerprint=_current_session_identity_fingerprint()
    )
    ws.receive_text.side_effect = _receive_text
    monkeypatch.setattr(persistent_app, "_signal_ws_connected", MagicMock())
    monkeypatch.setattr(
        persistent_app,
        "_durable_session_control_modes",
        AsyncMock(return_value=("supervised", "auto")),
    )
    monkeypatch.setattr(
        persistent_app,
        "_pending_permission_requests",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        persistent_app, "_ensure_persistent_loop_started", MagicMock(return_value=True)
    )
    subscriber_queue: asyncio.Queue = asyncio.Queue()
    monkeypatch.setattr(
        persistent_app, "_subscribe", lambda _client_id: subscriber_queue
    )
    monkeypatch.setattr(persistent_app, "_unsubscribe", MagicMock())
    monkeypatch.setattr(persistent_app, "_clear_canvas_awareness", MagicMock())
    monkeypatch.setattr(persistent_app, "_run_subscriber_pump", _park_pump)
    await persistent_app.handle_persistent_websocket(ws)
    return ws


def _ws_frames(ws, method: str) -> list[dict]:
    return [
        call.args[0]
        for call in ws.send_json.await_args_list
        if call.args[0].get("method") == method
    ]


def test_prestop_helper_publishes_sentinel_before_loopback_wait(monkeypatch, tmp_path):
    sentinel = tmp_path / "terminating"
    observed = []

    class _Response:
        status = 200

        def __enter__(self):
            observed.append(sentinel.exists())
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(
        persistent_termination, "TERMINATION_SENTINEL_PATH", str(sentinel)
    )
    monkeypatch.setattr(
        persistent_termination.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Response(),
    )

    assert persistent_termination.main() == 0
    assert observed == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("capabilities", "expected"),
    [({}, False), ({"durable_input_delivery": True}, True)],
)
async def test_wake_probe_requires_input_ledger_capability(
    monkeypatch, capabilities, expected
):
    class _Response:
        status_code = 200

        @staticmethod
        def json():
            return {"ready": True, "capabilities": capabilities}

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, _url):
            return _Response()

    monkeypatch.setattr(session_lifecycle.httpx, "AsyncClient", _Client)
    assert (
        await session_lifecycle.probe_ready(
            "127.0.0.1",
            8001,
            required_capability="durable_input_delivery",
        )
        is expected
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ready", "capabilities", "expected"),
    [
        (True, {"protected_cloud_contract": 1, "protected_cloud_ready": True}, True),
        (False, {"protected_cloud_contract": 1, "protected_cloud_ready": True}, False),
        (None, {"protected_cloud_contract": 1, "protected_cloud_ready": True}, False),
        ("true", {"protected_cloud_contract": 1, "protected_cloud_ready": True}, False),
        (1, {"protected_cloud_contract": 1, "protected_cloud_ready": True}, False),
        (True, {}, False),
        (
            True,
            {"protected_cloud_contract": True, "protected_cloud_ready": True},
            False,
        ),
        (True, {"protected_cloud_contract": 1.0, "protected_cloud_ready": True}, False),
        (True, {"protected_cloud_contract": "1", "protected_cloud_ready": True}, False),
        (True, {"protected_cloud_contract": 1, "protected_cloud_ready": False}, False),
        (True, {"protected_cloud_contract": 1, "protected_cloud_ready": "true"}, False),
    ],
)
async def test_protected_probe_requires_exact_joined_capability(
    monkeypatch, ready, capabilities, expected
):
    class _Response:
        status_code = 200

        @staticmethod
        def json():
            return {"ready": ready, "capabilities": capabilities}

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, _url):
            return _Response()

    monkeypatch.setattr(session_lifecycle.httpx, "AsyncClient", _Client)

    assert (
        await session_lifecycle.probe_ready(
            "127.0.0.1", 8001, require_protected_cloud=True
        )
        is expected
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("contract", "observed", "expected"),
    [
        (1, "sha256:" + "a" * 64, True),
        (True, "sha256:" + "a" * 64, False),
        (1.0, "sha256:" + "a" * 64, False),
        ("1", "sha256:" + "a" * 64, False),
        (None, "sha256:" + "a" * 64, False),
        (1, "sha256:" + "b" * 64, False),
        (1, None, False),
    ],
)
async def test_probe_requires_exact_pinned_session_identity_fingerprint(
    monkeypatch, contract, observed, expected
):
    class _Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "ready": True,
                "session_identity_fingerprint": observed,
                "capabilities": {"pinned_session_identity_contract": contract},
            }

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, _url):
            return _Response()

    monkeypatch.setattr(session_lifecycle.httpx, "AsyncClient", _Client)
    assert (
        await session_lifecycle.probe_ready(
            "127.0.0.1",
            8001,
            expected_session_identity_fingerprint="sha256:" + "a" * 64,
        )
        is expected
    )


@pytest.mark.asyncio
async def test_retry_stable_event_is_enqueued_once_and_fence_rejects_new_wake(
    monkeypatch, tmp_path
):
    """A lost response retry cannot create a second transcript row or turn."""

    db = _InsertOnceDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)
    delivery_id = str(uuid4())

    first = await persistent_app._accept_user_input(
        "timer wake", role="event", delivery_id=delivery_id
    )
    retry = await persistent_app._accept_user_input(
        "timer wake", role="event", delivery_id=delivery_id
    )

    assert first.enqueued is True
    assert retry.duplicate is True and retry.enqueued is False
    assert queue.qsize() == 1
    assert len(db.rows) == 1

    assert persistent_app.activate_termination_admission_fence("test") is True
    with pytest.raises(persistent_app.TerminationAdmissionClosed):
        await persistent_app._accept_user_input(
            "must wait", role="event", delivery_id=str(uuid4())
        )
    assert queue.qsize() == 1


@pytest.mark.asyncio
async def test_supplied_event_delivery_identity_requires_internal_authority(
    monkeypatch, tmp_path
):
    db = _InsertOnceDB()
    _wire_input_runtime(monkeypatch, tmp_path, db)
    monkeypatch.setenv("MCP_INTERNAL_KEY", "synthetic-internal-key")
    monkeypatch.setattr(
        persistent_app, "_ensure_persistent_loop_started", MagicMock(return_value=True)
    )
    delivery_id = str(uuid4())

    denied = await persistent_app.handle_api_input(
        SimpleNamespace(
            headers={},
            json=AsyncMock(
                return_value={
                    "content": "server wake",
                    "role": "event",
                    "delivery_id": delivery_id,
                    "session_identity_fingerprint": (
                        _current_session_identity_fingerprint()
                    ),
                }
            ),
        )
    )
    assert denied.status_code == 403
    assert db.rows == []

    accepted = await persistent_app.handle_api_input(
        SimpleNamespace(
            headers={"X-Internal-Key": "synthetic-internal-key"},
            json=AsyncMock(
                return_value={
                    "content": "server wake",
                    "role": "event",
                    "delivery_id": delivery_id,
                    "session_identity_fingerprint": (
                        _current_session_identity_fingerprint()
                    ),
                }
            ),
        )
    )
    assert accepted.status_code == 202
    assert len(db.rows) == 1


@pytest.mark.asyncio
async def test_wrong_runtime_event_is_refused_before_loop_or_persist(
    monkeypatch, tmp_path
):
    db = _InsertOnceDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)
    monkeypatch.setenv("MCP_INTERNAL_KEY", "synthetic-internal-key")
    start_loop = MagicMock(return_value=True)
    monkeypatch.setattr(persistent_app, "_ensure_persistent_loop_started", start_loop)

    response = await persistent_app.handle_api_input(
        SimpleNamespace(
            headers={"X-Internal-Key": "synthetic-internal-key"},
            json=AsyncMock(
                return_value={
                    "content": "wrong-runtime wake",
                    "role": "event",
                    "delivery_id": str(uuid4()),
                    "session_identity_fingerprint": "sha256:" + ("b" * 64),
                }
            ),
        )
    )

    assert response.status_code == 409
    assert json.loads(response.body)["error"] == "session_identity_mismatch"
    start_loop.assert_not_called()
    assert db.rows == []
    assert queue.empty()


@pytest.mark.asyncio
async def test_wrong_runtime_human_input_is_refused_before_loop_or_persist(
    monkeypatch, tmp_path
):
    db = _InsertOnceDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)
    start_loop = MagicMock(return_value=True)
    monkeypatch.setattr(persistent_app, "_ensure_persistent_loop_started", start_loop)

    wrong = await persistent_app.handle_api_input(
        SimpleNamespace(
            headers={},
            json=AsyncMock(
                return_value={
                    "content": "wrong runtime",
                    "session_identity_fingerprint": "sha256:" + ("b" * 64),
                }
            ),
        )
    )

    assert wrong.status_code == 409
    assert json.loads(wrong.body)["error"] == "session_identity_mismatch"
    start_loop.assert_not_called()
    assert db.rows == []
    assert queue.empty()


@pytest.mark.asyncio
async def test_human_input_without_runtime_identity_is_refused_before_effects(
    monkeypatch, tmp_path
):
    db = _InsertOnceDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)
    start_loop = MagicMock(return_value=True)
    monkeypatch.setattr(persistent_app, "_ensure_persistent_loop_started", start_loop)

    response = await persistent_app.handle_api_input(
        SimpleNamespace(
            headers={},
            json=AsyncMock(return_value={"content": "missing identity"}),
        )
    )

    assert response.status_code == 409
    assert json.loads(response.body) == {
        "error": "session_identity_mismatch",
        "retryable": True,
    }
    start_loop.assert_not_called()
    assert db.rows == []
    assert queue.empty()


@pytest.mark.asyncio
async def test_human_rest_input_carries_exact_runtime_through_durable_admission(
    monkeypatch, tmp_path
):
    db = _InsertOnceDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)
    monkeypatch.setattr(
        persistent_app, "_ensure_persistent_loop_started", MagicMock(return_value=True)
    )

    response = await persistent_app.handle_api_input(
        SimpleNamespace(
            headers={},
            json=AsyncMock(
                return_value={
                    "content": "exact runtime",
                    "session_identity_fingerprint": (
                        _current_session_identity_fingerprint()
                    ),
                }
            ),
        )
    )

    assert response.status_code == 202
    assert len(db.rows) == 1
    assert queue.qsize() == 1


@pytest.mark.asyncio
async def test_event_role_without_stable_internal_identity_is_rejected(
    monkeypatch, tmp_path
):
    db = _InsertOnceDB()
    _wire_input_runtime(monkeypatch, tmp_path, db)
    monkeypatch.setenv("MCP_INTERNAL_KEY", "synthetic-internal-key")
    monkeypatch.setattr(
        persistent_app, "_ensure_persistent_loop_started", MagicMock(return_value=True)
    )

    response = await persistent_app.handle_api_input(
        SimpleNamespace(
            headers={"X-Internal-Key": "synthetic-internal-key"},
            json=AsyncMock(return_value={"content": "forged wake", "role": "event"}),
        )
    )

    assert response.status_code == 400
    assert db.rows == []


@pytest.mark.asyncio
async def test_direct_input_fenced_after_persist_is_truthfully_deferred(
    monkeypatch, tmp_path
):
    """The accept/fence race retains human input instead of silently losing it."""

    db = _InsertOnceDB()
    db.after_persist = lambda: persistent_app.activate_termination_admission_fence(
        "persist_race"
    )
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)

    accepted = await persistent_app._accept_user_input("please retain this")

    assert accepted.deferred is True
    assert accepted.enqueued is False
    assert len(db.rows) == 1
    assert queue.empty()


@pytest.mark.asyncio
async def test_protected_mount_loss_rejects_before_persist_and_recovers(
    monkeypatch, tmp_path
):
    db = _InsertOnceDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)
    state = {"ready": False}
    persistent_app._session.protected_cloud_required = True
    persistent_app._session.protected_cloud_ready = lambda: state["ready"]

    with pytest.raises(persistent_app.ProtectedCloudUnavailable):
        await persistent_app._accept_user_input("do not persist yet")
    assert db.rows == []
    assert queue.empty()
    assert persistent_app._loop_provider_admission_open() is False

    state["ready"] = True
    accepted = await persistent_app._accept_user_input("now it is safe")
    assert accepted.enqueued is True
    assert len(db.rows) == 1
    assert queue.get_nowait()["content"] == "now it is safe"


@pytest.mark.asyncio
@pytest.mark.parametrize("available_half", ["lower", "overlay", "health-proof"])
async def test_partial_protected_mount_join_blocks_ready_input_provider_and_tool(
    monkeypatch, tmp_path, available_half
):
    """Mutation pin the real joined manager invariant at every effect gate."""

    db = _InsertOnceDB()
    _wire_input_runtime(monkeypatch, tmp_path, db)
    session = persistent_app._session
    session.protected_cloud_required = True
    session.llm_with_tools = object()
    session._protected_mount_id = "lower-1"
    lower_state = SimpleNamespace(
        mount_id="lower-1",
        mount_kind="protected_lower",
        target_path="/cloud/lower",
    )
    session.cloud_mount_manager = SimpleNamespace(
        active=available_half in {"lower", "health-proof"},
        mounts=([lower_state] if available_half in {"lower", "health-proof"} else []),
    )
    session.overlay_mount_manager = (
        SimpleNamespace(
            active=True,
            lower="/cloud/lower",
            merged="/cloud/merged",
        )
        if available_half in {"overlay", "health-proof"}
        else None
    )
    session._protected_cloud_health_ready = available_half != "health-proof"
    session.protected_cloud_ready = PersistentSession.protected_cloud_ready.__get__(
        session, type(session)
    )

    assert persistent_app._session_ready() is False
    assert persistent_app._loop_provider_admission_open() is False
    with pytest.raises(persistent_app.ProtectedCloudUnavailable):
        await persistent_app._accept_user_input("must not cross a partial mount")
    assert db.rows == []
    with pytest.raises(WorkspaceUnavailableError, match="protected cloud"):
        await persistent_app._loop_on_tool_execution_start("shell", "call-partial")

    reclaim = persistent_app._protected_input_reclaim_task
    if reclaim is not None:
        reclaim.cancel()
        await asyncio.gather(reclaim, return_exceptions=True)


@pytest.mark.asyncio
async def test_protected_loss_after_persist_defers_then_reclaims_exactly_once(
    monkeypatch, tmp_path
):
    db = _InsertOnceDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)
    state = {"ready": True}
    persistent_app._session.protected_cloud_required = True
    persistent_app._session.protected_cloud_ready = lambda: state["ready"]
    db.after_persist = lambda: state.update(ready=False)

    accepted = await persistent_app._accept_user_input("retain once")
    assert accepted.deferred is True
    assert accepted.enqueued is False
    assert len(db.rows) == 1
    assert queue.empty()

    db.after_persist = None
    state["ready"] = True
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    item = queue.get_nowait()
    assert item["delivery_id"] == accepted.delivery_id
    assert queue.empty()
    assert len(db.rows) == 1
    assert len(db.deliveries) == 1


@pytest.mark.asyncio
async def test_websocket_fence_before_persist_rejects_as_retryable(
    monkeypatch, tmp_path
):
    """Only a pre-transaction termination race tells the human to retry."""

    db = _InsertOnceDB()
    _wire_input_runtime(monkeypatch, tmp_path, db)
    ws = await _run_websocket_input(
        monkeypatch,
        "not persisted",
        before_receive=lambda: persistent_app.activate_termination_admission_fence(
            "before_persist"
        ),
    )

    rejected = _ws_frames(ws, "input.rejected")
    assert len(rejected) == 1
    assert rejected[0]["params"] == {
        "error": "runtime_terminating",
        "retryable": True,
        "message": "Retry input on the replacement runtime.",
    }
    assert _ws_frames(ws, "input.accepted") == []
    assert db.rows == []
    assert db.deliveries == {}


@pytest.mark.asyncio
async def test_existing_websocket_rejects_protected_loss_before_persist(
    monkeypatch, tmp_path
):
    db = _InsertOnceDB()
    _wire_input_runtime(monkeypatch, tmp_path, db)
    state = {"ready": True}
    persistent_app._session.protected_cloud_required = True
    persistent_app._session.protected_cloud_ready = lambda: state["ready"]

    ws = await _run_websocket_input(
        monkeypatch,
        "must wait for the mount",
        before_receive=lambda: state.update(ready=False),
    )

    rejected = _ws_frames(ws, "input.rejected")
    assert rejected == [
        {
            "method": "input.rejected",
            "params": {
                "error": "protected_cloud_unavailable",
                "retryable": True,
                "message": "Retry input when the protected cloud mount recovers.",
            },
        }
    ]
    assert db.rows == []


@pytest.mark.asyncio
async def test_existing_websocket_rejects_successor_generation_before_persist(
    monkeypatch, tmp_path
):
    db = _InsertOnceDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)

    def _rotate_generation():
        persistent_app._session_runtime_generation = str(uuid4())

    ws = await _run_websocket_input(
        monkeypatch,
        "must remain with predecessor",
        before_receive=_rotate_generation,
    )

    ws.close.assert_awaited_with(code=4403, reason="session identity changed")
    assert db.rows == []
    assert queue.empty()


@pytest.mark.asyncio
async def test_protected_loss_at_provider_admission_unadmits_and_defers(
    monkeypatch, tmp_path
):
    db = _InsertOnceDB()
    _wire_input_runtime(monkeypatch, tmp_path, db)
    state = {"ready": True}
    persistent_app._session.protected_cloud_required = True
    persistent_app._session.protected_cloud_ready = lambda: state["ready"]
    transitions: list[str] = []

    async def transition(_delivery_id, _claim_generation, action, **_kwargs):
        transitions.append(action)
        if action == "admitted":
            state["ready"] = False
        return True

    monkeypatch.setattr(persistent_app, "_transition_claimed_input", transition)

    result = await persistent_app._loop_admit_input_delivery("delivery", 3, 7)

    assert result is None
    assert transitions == ["admitted", "unadmit"]
    assert persistent_app._loop_provider_admission_open() is False
    state["ready"] = True
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_protected_loss_fences_tool_effect_boundary(monkeypatch, tmp_path):
    db = _InsertOnceDB()
    _wire_input_runtime(monkeypatch, tmp_path, db)
    persistent_app._session.protected_cloud_required = True
    persistent_app._session.protected_cloud_ready = lambda: False

    with pytest.raises(WorkspaceUnavailableError, match="protected cloud"):
        await persistent_app._loop_on_tool_execution_start("shell", "call-1")


@pytest.mark.asyncio
async def test_authorized_retirement_fences_provider_and_tool_before_effect(
    monkeypatch, tmp_path
):
    """A durable End token is observed without waiting for the 60s watchdog."""

    db = _InsertOnceDB()
    _wire_input_runtime(monkeypatch, tmp_path, db)
    monkeypatch.setattr(persistent_app, "_pinned_runtime_generation_enabled", True)
    db.verify_pinned_runtime_effect_authority = AsyncMock(return_value=False)

    assert await persistent_app._loop_runtime_effect_authority_current() is False
    db.verify_pinned_runtime_effect_authority.assert_awaited_once_with(
        thread_id=persistent_app._thread_id,
        agent_id=persistent_app._orchestrator_client.agent_id,
        pod_uid="pod-uid-test",
        session_runtime_generation=persistent_app._session_runtime_generation,
        runtime_attach_token=persistent_app._session_runtime_attach_token,
    )

    with pytest.raises(
        persistent_app.TerminationAdmissionClosed,
        match="runtime authority closed",
    ):
        await persistent_app._loop_on_tool_execution_start("shell", "call-after-end")
    assert persistent_app._tool_inflight is False


@pytest.mark.asyncio
async def test_stateless_effect_boundary_awaits_exact_queue_lease(monkeypatch):
    from agent.api.lease_context import LeaseHandle, current_lease

    thread_id = str(uuid4())
    lease = LeaseHandle()
    lease.update(
        thread_id,
        17,
        executor_id="executor-a",
        pod_uid="pod-a",
    )
    probe = AsyncMock(return_value=False)
    token = current_lease.set(lease)
    try:
        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        monkeypatch.setattr(persistent_app, "_thread_id", thread_id)
        monkeypatch.setattr(
            persistent_app,
            "_session",
            SimpleNamespace(
                postgres_conn=SimpleNamespace(
                    session_parent_authority_current=probe,
                ),
                protected_cloud_required=False,
            ),
        )
        monkeypatch.setattr(persistent_app, "_runtime_admission_closed", lambda: False)

        assert await persistent_app._loop_runtime_effect_authority_current() is False

        authority = probe.await_args.args[0]
        assert authority.model_dump(exclude_none=True, mode="json") == {
            "version": 1,
            "execution_lane": "stateless",
            "parent_thread_id": thread_id,
            "lease_token": 17,
            "executor_id": "executor-a",
            "executor_pod_uid": "pod-a",
        }
        assert lease.lost.is_set()
    finally:
        current_lease.reset(token)


@pytest.mark.asyncio
async def test_stateless_effect_boundary_rechecks_local_life_after_db_await(
    monkeypatch,
):
    from agent.api.lease_context import LeaseHandle, current_lease

    thread_id = str(uuid4())
    lease = LeaseHandle()
    lease.update(
        thread_id,
        17,
        executor_id="executor-a",
        pod_uid="pod-a",
    )

    async def rotate_while_proving(_authority):
        lease.update(
            thread_id,
            18,
            executor_id="executor-a",
            pod_uid="pod-a",
        )
        return True

    probe = AsyncMock(side_effect=rotate_while_proving)
    token = current_lease.set(lease)
    try:
        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        monkeypatch.setattr(persistent_app, "_thread_id", thread_id)
        monkeypatch.setattr(
            persistent_app,
            "_session",
            SimpleNamespace(
                postgres_conn=SimpleNamespace(
                    session_parent_authority_current=probe,
                ),
                protected_cloud_required=False,
            ),
        )
        monkeypatch.setattr(persistent_app, "_runtime_admission_closed", lambda: False)

        assert await persistent_app._loop_runtime_effect_authority_current() is False
        assert lease.lease_token == 18
        assert not lease.lost.is_set()
    finally:
        current_lease.reset(token)


@pytest.mark.asyncio
async def test_force_end_after_provider_response_blocks_real_tool_effect(
    monkeypatch, tmp_path
):
    """The exact DB token wins even while the lifecycle watchdog is delayed."""

    db = _InsertOnceDB()
    _wire_input_runtime(monkeypatch, tmp_path, db)
    monkeypatch.setattr(persistent_app, "_pinned_runtime_generation_enabled", True)
    monkeypatch.setattr(persistent_app, "_retirement_admission_identity", None)

    provider_calls = 0
    response_with_tool = AIMessage(
        content="",
        tool_calls=[{"name": "write", "args": {"value": "x"}, "id": "tc1"}],
    )

    async def _astream(_messages, **_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        yield response_with_tool

    llm = MagicMock(reasoning=None)
    llm.astream = _astream
    tool = MagicMock()
    tool.args_schema = None
    tool.ainvoke = AsyncMock(return_value="must not run")
    on_tool_result = AsyncMock()

    async def _authorize_after_visible_tool_start(*_args):
        # This is the durable row transition the production verifier reads.
        # The lifecycle watchdog is deliberately absent: only the immediate
        # pre-effect exact thread/agent/pod/G/attach/token-NULL read can see it.
        db.runtime_retirement_token = str(uuid4())

    callbacks = _callbacks(
        [],
        before_provider_execution=(
            persistent_app._loop_runtime_effect_authority_current
        ),
        on_tool_start=AsyncMock(side_effect=_authorize_after_visible_tool_start),
        on_tool_execution_start=persistent_app._loop_on_tool_execution_start,
        on_tool_result=on_tool_result,
    )
    tool_context = MagicMock()
    tool_context.consume_freeze_request.return_value = None

    with pytest.raises(
        persistent_app.TerminationAdmissionClosed,
        match="runtime authority closed",
    ):
        await _execute_turn(
            llm_with_tools=llm,
            tool_map={"write": tool},
            context_manager=_context_manager(),
            messages=[HumanMessage(content="write only if still authoritative")],
            callbacks=callbacks,
            llm_timeout=30,
            auxiliary_llm=None,
            config=_config(),
            tool_context=tool_context,
        )

    assert provider_calls == 1
    assert db.effect_authority_reads == 2
    tool.ainvoke.assert_not_awaited()
    on_tool_result.assert_not_awaited()
    assert persistent_app._tool_inflight is False


@pytest.mark.asyncio
async def test_websocket_post_persist_fence_acknowledges_and_successor_reclaims_once(
    monkeypatch, tmp_path
):
    """A committed human input is owned work, never a retry invitation."""

    db = _InsertOnceDB()
    db.after_persist = lambda: persistent_app.activate_termination_admission_fence(
        "after_persist"
    )
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)

    ws = await _run_websocket_input(monkeypatch, "persist exactly once")

    assert _ws_frames(ws, "input.rejected") == []
    acknowledged = _ws_frames(ws, "input.accepted")
    assert len(acknowledged) == 1
    params = acknowledged[0]["params"]
    assert params["accepted"] is True
    assert params["deferred"] is True
    assert params["retryable"] is False
    assert params["delivery_state"] == "deferred"
    assert params["message_id"]
    assert params["delivery_id"] in db.deliveries
    assert len(db.rows) == 1
    assert len(db.deliveries) == 1
    assert queue.empty()

    # The response contract does not ask the client to mint a second random
    # identity. A new process generation claims the existing delivery and
    # publishes exactly that one input to its queue.
    db.after_persist = None
    persistent_app._termination_admission_fenced = False
    persistent_app._input_runtime_generation = str(uuid4())
    persistent_app._queued_input_claims.clear()
    reclaimed = await persistent_app._reclaim_pending_pinned_inputs()
    assert reclaimed == {(params["delivery_id"], 2)}
    item = queue.get_nowait()
    assert item["delivery_id"] == params["delivery_id"]
    assert item["id"] == params["message_id"]
    assert queue.empty()
    assert len(db.rows) == 1
    assert len(db.deliveries) == 1


@pytest.mark.asyncio
async def test_rest_post_persist_event_defer_is_accepted_for_outbox_retry(
    monkeypatch, tmp_path
):
    """REST reports persistence while the wake outbox retains execution debt."""

    db = _InsertOnceDB()
    db.after_persist = lambda: persistent_app.activate_termination_admission_fence(
        "after_persist"
    )
    _wire_input_runtime(monkeypatch, tmp_path, db)
    monkeypatch.setenv("MCP_INTERNAL_KEY", "synthetic-internal-key")
    monkeypatch.setattr(
        persistent_app, "_ensure_persistent_loop_started", MagicMock(return_value=True)
    )
    delivery_id = str(uuid4())

    response = await persistent_app.handle_api_input(
        SimpleNamespace(
            headers={"X-Internal-Key": "synthetic-internal-key"},
            json=AsyncMock(
                return_value={
                    "content": "stable outbox wake",
                    "role": "event",
                    "delivery_id": delivery_id,
                    "session_identity_fingerprint": (
                        _current_session_identity_fingerprint()
                    ),
                }
            ),
        )
    )
    payload = json.loads(response.body)
    assert response.status_code == 202
    assert payload["accepted"] is True
    assert payload["deferred"] is True
    assert payload["retryable"] is False
    assert payload["delivery_id"] == delivery_id
    assert payload["delivery_state"] == "deferred"
    assert len(db.rows) == 1
    assert len(db.deliveries) == 1


@pytest.mark.asyncio
async def test_committed_insert_before_queue_is_reclaimed_by_new_process(
    monkeypatch, tmp_path
):
    db = _InsertOnceDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)
    delivery_id = str(uuid4())
    original_queue = persistent_app._queue_claimed_input

    async def _die_after_commit(_row):
        raise RuntimeError("process died before queue publication")

    monkeypatch.setattr(persistent_app, "_queue_claimed_input", _die_after_commit)
    with pytest.raises(RuntimeError, match="process died"):
        await persistent_app._accept_user_input(
            "persisted wake", role="event", delivery_id=delivery_id
        )
    assert len(db.rows) == 1
    assert queue.empty()

    monkeypatch.setattr(persistent_app, "_queue_claimed_input", original_queue)
    persistent_app._input_runtime_generation = str(uuid4())
    persistent_app._queued_input_claims.clear()
    assert await persistent_app._reclaim_pending_pinned_inputs() == {(delivery_id, 2)}
    item = queue.get_nowait()
    assert item["delivery_id"] == delivery_id
    assert item["claim_generation"] == 2
    assert len(db.rows) == 1


@pytest.mark.asyncio
async def test_cancelled_direct_human_delivery_is_terminal_and_not_reclaimed(
    monkeypatch, tmp_path
):
    db = _InsertOnceDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)
    monkeypatch.setenv("PERSISTENT_INPUT_CANCELLATION_ENABLED", "true")

    accepted = await persistent_app._accept_user_input("stop this before provider")
    item = queue.get_nowait()
    assert item["source"] == "direct_human"
    assert await persistent_app._loop_cancel_input_delivery(
        accepted.delivery_id,
        accepted.claim_generation,
        6,
        "human_stop_before_provider",
    )
    assert db.deliveries[accepted.delivery_id]["state"] == "cancelled"

    persistent_app._input_runtime_generation = str(uuid4())
    persistent_app._queued_input_claims.clear()
    assert await persistent_app._reclaim_pending_pinned_inputs() == set()
    assert queue.empty()
    assert len(db.rows) == 1


@pytest.mark.asyncio
async def test_cancellation_writer_gate_defaults_off(monkeypatch, tmp_path):
    db = _InsertOnceDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)
    monkeypatch.delenv("PERSISTENT_INPUT_CANCELLATION_ENABLED", raising=False)
    accepted = await persistent_app._accept_user_input("not written as cancelled yet")
    queue.get_nowait()

    assert not await persistent_app._loop_cancel_input_delivery(
        accepted.delivery_id,
        accepted.claim_generation,
        6,
        "human_stop_before_provider",
    )
    assert db.deliveries[accepted.delivery_id]["state"] == "queued"


@pytest.mark.asyncio
async def test_interrupted_event_defers_and_successor_reclaims_exactly_once(
    monkeypatch, tmp_path
):
    db = _InsertOnceDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)
    delivery_id = str(uuid4())
    accepted = await persistent_app._accept_user_input(
        "retry this wake",
        role="event",
        delivery_id=delivery_id,
    )
    first = queue.get_nowait()
    assert first["source"] == "officer_wake"
    assert not await persistent_app._loop_cancel_input_delivery(
        delivery_id,
        accepted.claim_generation,
        6,
        "human_stop_before_provider",
    )
    assert await persistent_app._loop_defer_input_delivery(
        delivery_id,
        accepted.claim_generation,
        "turn_interrupted_before_provider",
    )

    persistent_app._input_runtime_generation = str(uuid4())
    persistent_app._queued_input_claims.clear()
    assert await persistent_app._reclaim_pending_pinned_inputs() == {(delivery_id, 2)}
    assert await persistent_app._reclaim_pending_pinned_inputs() == set()
    replay = queue.get_nowait()
    assert replay["delivery_id"] == delivery_id
    assert replay["claim_generation"] == 2
    assert queue.empty()
    assert len(db.rows) == 1


@pytest.mark.asyncio
async def test_interrupted_event_priority_reclaim_uses_new_generation_not_fifo(
    monkeypatch, tmp_path
):
    db = _InsertOnceDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)
    delivery_id = str(uuid4())
    accepted = await persistent_app._accept_user_input(
        "priority wake",
        role="event",
        delivery_id=delivery_id,
    )
    queue.get_nowait()

    replay = await persistent_app._loop_defer_and_requeue_input_delivery(
        delivery_id,
        accepted.claim_generation,
        "priority wake",
        "event",
        "officer_wake",
        "turn_interrupted_before_provider",
    )

    assert replay is not None
    assert replay["delivery_id"] == delivery_id
    assert replay["claim_generation"] == accepted.claim_generation + 1
    assert (delivery_id, accepted.claim_generation + 1) in (
        persistent_app._queued_input_claims
    )
    assert queue.empty()
    assert len(db.rows) == 1


@pytest.mark.asyncio
async def test_event_priority_reclaim_serializes_concurrent_human_accept(
    monkeypatch, tmp_path
):
    priority_claim_entered = asyncio.Event()
    release_priority_claim = asyncio.Event()

    class _BarrierDB(_InsertOnceDB):
        async def persist_pinned_input_delivery(self, **row):
            existing = self.deliveries.get(row["delivery_id"])
            if existing is not None and existing["state"] == "deferred":
                priority_claim_entered.set()
                await release_priority_claim.wait()
            return await super().persist_pinned_input_delivery(**row)

    db = _BarrierDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)
    event_id = str(uuid4())
    event = await persistent_app._accept_user_input(
        "wake A",
        role="event",
        delivery_id=event_id,
    )
    queue.get_nowait()

    replay_task = asyncio.create_task(
        persistent_app._loop_defer_and_requeue_input_delivery(
            event_id,
            event.claim_generation,
            "wake A",
            "event",
            "officer_wake",
            "turn_interrupted_before_provider",
        )
    )
    await asyncio.wait_for(priority_claim_entered.wait(), timeout=1)
    human_task = asyncio.create_task(persistent_app._accept_user_input("human B"))
    await asyncio.sleep(0)
    release_priority_claim.set()

    replay, human = await asyncio.wait_for(
        asyncio.gather(replay_task, human_task), timeout=1
    )
    assert replay is not None
    assert replay["content"] == "wake A"
    assert replay["claim_generation"] == event.claim_generation + 1
    assert human.enqueued is True
    fifo_item = queue.get_nowait()
    assert fifo_item["content"] == "human B"
    assert queue.empty()
    assert sum(row["content"] == "wake A" for row in db.rows) == 1


@pytest.mark.asyncio
async def test_retry_before_and_after_admission_never_requeues(monkeypatch, tmp_path):
    db = _InsertOnceDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)
    delivery_id = str(uuid4())
    first = await persistent_app._accept_user_input(
        "wake", role="event", delivery_id=delivery_id
    )
    before_admission = await persistent_app._accept_user_input(
        "wake", role="event", delivery_id=delivery_id
    )
    assert first.delivery_state == "queued"
    assert before_admission.delivery_state == "queued"
    assert queue.qsize() == 1

    assert await db.transition_pinned_input_delivery(
        delivery_id=delivery_id, transition="admitted"
    )
    after_admission = await persistent_app._accept_user_input(
        "wake", role="event", delivery_id=delivery_id
    )
    assert after_admission.delivery_state == "admitted"
    assert after_admission.enqueued is False
    assert queue.qsize() == 1
    assert len(db.rows) == 1


@pytest.mark.asyncio
async def test_concurrent_duplicate_delivery_attempts_publish_one_queue_item(
    monkeypatch, tmp_path
):
    entered = 0
    first_entered = asyncio.Event()
    release = asyncio.Event()

    class _RacingDB(_InsertOnceDB):
        async def mark_pinned_input_delivery_queued(self, **row):
            nonlocal entered
            entered += 1
            first_entered.set()
            await release.wait()
            return await super().mark_pinned_input_delivery_queued(**row)

    db = _RacingDB()
    queue = _wire_input_runtime(monkeypatch, tmp_path, db)
    delivery_id = str(uuid4())
    attempts = [
        asyncio.create_task(
            persistent_app._accept_user_input(
                "same wake", role="event", delivery_id=delivery_id
            )
        )
        for _ in range(2)
    ]
    await asyncio.wait_for(first_entered.wait(), timeout=1)
    await asyncio.sleep(0)
    # The shared reclaim lock deliberately serializes durable CAS plus local
    # publication; a second marker call here would reopen the duplicate race.
    assert entered == 1
    release.set()
    results = await asyncio.wait_for(asyncio.gather(*attempts), timeout=1)
    assert sum(result.enqueued for result in results) == 1
    assert queue.qsize() == 1
    assert len(db.rows) == 1


@pytest.mark.asyncio
async def test_fence_after_loop_consumption_defers_before_first_provider():
    admission_open = True
    provider_calls = 0
    admit = AsyncMock(return_value=True)
    defer = AsyncMock(return_value=True)

    async def _authorize():
        nonlocal admission_open
        admission_open = False
        return True, "authorized"

    async def _stream(*_args, **_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        yield AIMessage(content="must not run")

    llm = MagicMock(reasoning=None)
    llm.astream = _stream
    callbacks = _callbacks(
        [
            {
                "id": str(uuid4()),
                "role": "event",
                "content": "queued wake",
                "delivery_id": str(uuid4()),
                "claim_generation": 7,
            }
        ],
        before_turn_authorization=_authorize,
        before_provider_admission=lambda: admission_open,
        admit_input_delivery=admit,
        defer_input_delivery=defer,
    )

    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=[],
    )
    assert provider_calls == 0
    admit.assert_not_awaited()
    defer.assert_awaited_once()


@pytest.mark.asyncio
async def test_direct_human_stop_before_provider_cancels_and_leaves_model_context():
    provider_calls = 0
    cancel = AsyncMock(return_value=True)
    defer = AsyncMock(return_value=True)
    messages = []

    async def _stream(*_args, **_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        yield AIMessage(content="must not run")

    llm = MagicMock(reasoning=None)
    llm.astream = _stream
    memory_service = MagicMock()
    memory_service.assemble = AsyncMock()
    memory_service.capture = AsyncMock()
    git_manager = MagicMock()
    git_manager.is_active = True
    git_manager.has_uncommitted_changes.return_value = False
    git_manager.has_unpushed_commits.return_value = True
    git_manager.push.return_value = True
    tool_context = SimpleNamespace(
        workspace_manager=SimpleNamespace(git_manager=git_manager)
    )
    workspace_commit = AsyncMock()
    callbacks = _callbacks(
        [
            {
                "id": str(uuid4()),
                "role": "human",
                "source": "direct_human",
                "content": "cancel this input",
                "delivery_id": "11111111-1111-4111-8111-111111111111",
                "claim_generation": 3,
            }
        ],
        check_interrupt=MagicMock(return_value="hard"),
        cancel_input_delivery=cancel,
        defer_input_delivery=defer,
        on_workspace_commit=workspace_commit,
    )

    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=messages,
        memory_service=memory_service,
        tool_context=tool_context,
    )

    assert provider_calls == 0
    memory_service.assemble.assert_not_awaited()
    memory_service.capture.assert_not_awaited()
    git_manager.has_uncommitted_changes.assert_not_called()
    git_manager.has_unpushed_commits.assert_not_called()
    git_manager.push.assert_not_called()
    workspace_commit.assert_not_awaited()
    cancel.assert_awaited_once_with(
        "11111111-1111-4111-8111-111111111111",
        3,
        1,
        "human_stop_before_provider",
    )
    defer.assert_not_awaited()
    assert callbacks.on_turn_complete.await_args.kwargs == {
        "skip_message_reconcile": True
    }
    assert not any(
        isinstance(message, HumanMessage) and message.content == "cancel this input"
        for message in messages
    )


@pytest.mark.asyncio
async def test_stateless_event_without_delivery_metadata_keeps_legacy_stop_boundary():
    provider_calls = 0

    async def _stream(*_args, **_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        yield AIMessage(content="must not run")

    llm = MagicMock(reasoning=None)
    llm.astream = _stream
    callbacks = _callbacks(
        [{"id": "session-turn-input", "role": "event", "content": "run claim"}],
        check_interrupt=MagicMock(side_effect=["hard", None]),
    )
    messages = []

    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=messages,
        defer_memory_extraction_to_outbox=True,
    )

    assert provider_calls == 0
    assert callbacks.on_turn_complete.await_count == 1
    assert callbacks.on_turn_complete.await_args.kwargs == {}
    assert any(
        isinstance(message, HumanMessage) and message.content == "run claim"
        for message in messages
    )


@pytest.mark.asyncio
async def test_late_turn_n_interrupt_clears_before_durable_a_and_b_execute(monkeypatch):
    """Terminal-edge Stop cannot jump from completed N onto queued A."""
    from agent.api import persistent_app as pa

    a_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    b_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    ledger = {a_id: "queued", b_id: "queued"}
    provider_order: list[str] = []

    async def _stream(provider_messages, **_kwargs):
        human = [
            str(message.content)
            for message in provider_messages
            if isinstance(message, HumanMessage)
        ]
        provider_order.append(human[-1])
        yield AIMessage(content=f"answer {human[-1]}")

    async def _admit(delivery_id, _generation, _turn_id):
        assert ledger[delivery_id] == "queued"
        ledger[delivery_id] = "admitted"
        return True

    async def _settle(delivery_id, _generation):
        assert ledger[delivery_id] == "admitted"
        ledger[delivery_id] = "settled"
        return True

    async def _turn_start(turn_id):
        pa._session.turn_count = turn_id
        pa._turn_event_open = True

    async def _turn_complete(turn_id, metrics, *args, **kwargs):
        if turn_id == 1:
            assert pa._signal_interrupt_for_turn(turn_id) == "hard"
            assert pa._loop_interrupt_target_turn_id == 1
        await pa._loop_on_turn_complete(
            turn_id,
            metrics,
            turn_input_message_id=args[0] if args else None,
            memory_scope_kind=args[1] if len(args) > 1 else None,
            memory_scope_id=args[2] if len(args) > 2 else None,
            **kwargs,
        )
        if turn_id == 1:
            assert pa._loop_interrupt_flag is None
            assert pa._loop_interrupt_target_turn_id is None
            assert not pa._hard_interrupt_event.is_set()

    llm = MagicMock(reasoning=None)
    llm.astream = _stream
    callbacks = _callbacks(
        [
            "turn N",
            {
                "id": "message-a",
                "role": "human",
                "source": "direct_human",
                "content": "durable A",
                "delivery_id": a_id,
                "claim_generation": 1,
            },
            {
                "id": "message-b",
                "role": "human",
                "source": "direct_human",
                "content": "durable B",
                "delivery_id": b_id,
                "claim_generation": 1,
            },
        ],
        on_turn_start=_turn_start,
        on_turn_complete=_turn_complete,
        check_interrupt=pa._loop_check_interrupt,
        admit_input_delivery=_admit,
        settle_input_delivery=_settle,
        cancel_input_delivery=AsyncMock(return_value=True),
    )
    session = SimpleNamespace(turn_count=0)

    monkeypatch.setattr(pa, "_session", session)
    monkeypatch.setattr(pa, "_turn_event_open", False)
    monkeypatch.setattr(pa, "_tool_inflight", False)
    monkeypatch.setattr(pa, "_loop_interrupt_flag", None)
    monkeypatch.setattr(pa, "_loop_interrupt_target_turn_id", None)
    monkeypatch.setattr(pa, "_hard_interrupt_event", asyncio.Event())
    monkeypatch.setattr(pa, "_loop_on_turn_complete_body", AsyncMock())
    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=[],
    )

    assert provider_order == ["turn N", "durable A", "durable B"]
    assert ledger == {a_id: "settled", b_id: "settled"}
    callbacks.cancel_input_delivery.assert_not_awaited()


@pytest.mark.asyncio
async def test_event_stop_before_provider_retries_ahead_of_b_without_ending_loop():
    cancel = AsyncMock(return_value=True)
    defer = AsyncMock(return_value=True)
    admit = AsyncMock(return_value=True)
    settle = AsyncMock(return_value=True)
    on_turn_start = AsyncMock()
    provider_order: list[str] = []
    messages = []

    interrupt_checks = 0

    def _check_interrupt():
        nonlocal interrupt_checks
        interrupt_checks += 1
        return "hard" if interrupt_checks == 1 else None

    async def _stream(provider_messages, **_kwargs):
        human = [
            str(message.content)
            for message in provider_messages
            if isinstance(message, HumanMessage)
        ]
        provider_order.append(human[-1])
        yield AIMessage(content=f"handled {human[-1]}")

    llm = MagicMock(reasoning=None)
    llm.astream = _stream
    replay = {
        "id": "event-message-id",
        "role": "event",
        "source": "officer_wake",
        "content": "retryable wake",
        "delivery_id": "22222222-2222-4222-8222-222222222222",
        "claim_generation": 6,
    }
    requeue = AsyncMock(return_value=replay)
    callbacks = _callbacks(
        [
            {
                "id": "event-message-id",
                "role": "event",
                "source": "officer_wake",
                "content": "retryable wake",
                "delivery_id": "22222222-2222-4222-8222-222222222222",
                "claim_generation": 5,
            },
            {
                "id": str(uuid4()),
                "role": "human",
                "source": "direct_human",
                "content": "must not overtake wake",
                "delivery_id": "55555555-5555-4555-8555-555555555555",
                "claim_generation": 1,
            },
        ],
        check_interrupt=MagicMock(side_effect=_check_interrupt),
        cancel_input_delivery=cancel,
        defer_input_delivery=defer,
        defer_and_requeue_input_delivery=requeue,
        admit_input_delivery=admit,
        settle_input_delivery=settle,
        on_turn_start=on_turn_start,
    )

    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=messages,
    )

    cancel.assert_not_awaited()
    defer.assert_not_awaited()
    requeue.assert_awaited_once_with(
        "22222222-2222-4222-8222-222222222222",
        5,
        "retryable wake",
        "event",
        "officer_wake",
        "turn_interrupted_before_provider",
    )
    assert [call.args[0] for call in on_turn_start.await_args_list] == [1, 2, 3]
    assert provider_order == ["retryable wake", "must not overtake wake"]
    assert callbacks.on_turn_complete.await_args_list[0].kwargs == {
        "skip_message_reconcile": True
    }
    assert admit.await_args_list[0].args == (
        "22222222-2222-4222-8222-222222222222",
        6,
        2,
    )
    assert settle.await_args_list[0].args == (
        "22222222-2222-4222-8222-222222222222",
        6,
    )
    assert callbacks.on_turn_settled.await_count == 3


@pytest.mark.asyncio
async def test_failed_pre_provider_cancellation_halts_before_next_delivery():
    first = "33333333-3333-4333-8333-333333333333"
    second = "44444444-4444-4444-8444-444444444444"
    cancel = AsyncMock(return_value=False)
    on_turn_start = AsyncMock()
    on_error = AsyncMock()
    callbacks = _callbacks(
        [
            {
                "id": str(uuid4()),
                "role": "human",
                "source": "direct_human",
                "content": "first",
                "delivery_id": first,
                "claim_generation": 1,
            },
            {
                "id": str(uuid4()),
                "role": "human",
                "source": "direct_human",
                "content": "must remain queued",
                "delivery_id": second,
                "claim_generation": 1,
            },
        ],
        check_interrupt=MagicMock(return_value="hard"),
        cancel_input_delivery=cancel,
        on_turn_start=on_turn_start,
        on_error=on_error,
    )
    await run_persistent_loop(
        llm_with_tools=MagicMock(reasoning=None),
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=[],
    )

    cancel.assert_awaited_once()
    on_turn_start.assert_awaited_once_with(1)
    on_error.assert_awaited_once()
    assert "halted" in on_error.await_args.args[0]


@pytest.mark.asyncio
async def test_raising_pre_provider_cancellation_halts_before_next_delivery():
    cancel = AsyncMock(side_effect=RuntimeError("ambiguous database response"))
    on_turn_start = AsyncMock()
    on_error = AsyncMock()
    messages = []
    callbacks = _callbacks(
        [
            {
                "id": "raising-cancel-a",
                "role": "human",
                "source": "direct_human",
                "content": "stopped A",
                "delivery_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
                "claim_generation": 1,
            },
            {
                "id": "raising-cancel-b",
                "role": "human",
                "source": "direct_human",
                "content": "queued B",
                "delivery_id": "ffffffff-ffff-4fff-8fff-ffffffffffff",
                "claim_generation": 1,
            },
        ],
        check_interrupt=MagicMock(return_value="hard"),
        cancel_input_delivery=cancel,
        on_turn_start=on_turn_start,
        on_error=on_error,
    )

    await run_persistent_loop(
        llm_with_tools=MagicMock(reasoning=None),
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=messages,
    )

    cancel.assert_awaited_once()
    on_turn_start.assert_awaited_once_with(1)
    on_error.assert_awaited_once()
    assert "halted" in on_error.await_args.args[0]
    assert not any(
        isinstance(message, HumanMessage) and message.content == "stopped A"
        for message in messages
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("raises", [False, True])
async def test_admission_authority_failure_halts_before_provider_and_b(raises):
    admit = (
        AsyncMock(side_effect=RuntimeError("ambiguous admission"))
        if raises
        else AsyncMock(return_value=False)
    )
    defer = AsyncMock(return_value=True)
    on_turn_start = AsyncMock()
    on_error = AsyncMock()
    provider_calls = 0

    async def _stream(*_args, **_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        yield AIMessage(content="must not run")

    llm = MagicMock(reasoning=None)
    llm.astream = _stream
    callbacks = _callbacks(
        [
            {
                "id": "admit-a",
                "role": "human",
                "source": "direct_human",
                "content": "A",
                "delivery_id": "12121212-1212-4212-8212-121212121212",
                "claim_generation": 2,
            },
            {
                "id": "admit-b",
                "role": "human",
                "source": "direct_human",
                "content": "B",
                "delivery_id": "34343434-3434-4434-8434-343434343434",
                "claim_generation": 1,
            },
        ],
        admit_input_delivery=admit,
        defer_input_delivery=defer,
        on_turn_start=on_turn_start,
        on_error=on_error,
    )

    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=[],
    )

    assert provider_calls == 0
    admit.assert_awaited_once()
    defer.assert_not_awaited()
    on_turn_start.assert_awaited_once_with(1)
    on_error.assert_awaited_once()
    assert "halted" in on_error.await_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("raises", [False, True])
async def test_settlement_authority_failure_halts_before_b(raises):
    settle = (
        AsyncMock(side_effect=RuntimeError("ambiguous settlement"))
        if raises
        else AsyncMock(return_value=False)
    )
    provider_order: list[str] = []
    on_turn_start = AsyncMock()
    on_error = AsyncMock()

    async def _stream(provider_messages, **_kwargs):
        human = [
            str(message.content)
            for message in provider_messages
            if isinstance(message, HumanMessage)
        ]
        provider_order.append(human[-1])
        yield AIMessage(content="done")

    llm = MagicMock(reasoning=None)
    llm.astream = _stream
    callbacks = _callbacks(
        [
            {
                "id": "settle-a",
                "role": "human",
                "source": "direct_human",
                "content": "A",
                "delivery_id": "56565656-5656-4656-8656-565656565656",
                "claim_generation": 1,
            },
            {
                "id": "settle-b",
                "role": "human",
                "source": "direct_human",
                "content": "B",
                "delivery_id": "78787878-7878-4878-8878-787878787878",
                "claim_generation": 1,
            },
        ],
        admit_input_delivery=AsyncMock(return_value=True),
        settle_input_delivery=settle,
        on_turn_start=on_turn_start,
        on_error=on_error,
    )

    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=[],
    )

    assert provider_order == ["A"]
    settle.assert_awaited_once()
    on_turn_start.assert_awaited_once_with(1)
    on_error.assert_awaited_once()
    assert "settled safely" in on_error.await_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("raises", [False, True])
async def test_failed_authorization_deferral_halts_before_next_delivery(raises):
    defer = (
        AsyncMock(side_effect=RuntimeError("ambiguous deferral"))
        if raises
        else AsyncMock(return_value=False)
    )
    on_turn_start = AsyncMock()
    on_error = AsyncMock()
    callbacks = _callbacks(
        [
            {
                "id": str(uuid4()),
                "role": "event",
                "source": "officer_wake",
                "content": "first wake",
                "delivery_id": "66666666-6666-4666-8666-666666666666",
                "claim_generation": 2,
            },
            {
                "id": str(uuid4()),
                "role": "human",
                "source": "direct_human",
                "content": "must not run",
                "delivery_id": "77777777-7777-4777-8777-777777777777",
                "claim_generation": 1,
            },
        ],
        before_turn_authorization=AsyncMock(return_value=(False, "grant lost")),
        defer_input_delivery=defer,
        on_turn_start=on_turn_start,
        on_error=on_error,
    )

    await run_persistent_loop(
        llm_with_tools=MagicMock(reasoning=None),
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=[],
    )

    defer.assert_awaited_once()
    on_turn_start.assert_awaited_once_with(1)
    assert on_error.await_count == 2
    assert "halted" in on_error.await_args_list[-1].args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("raises", [False, True])
async def test_failed_priority_requeue_halts_before_b(raises):
    requeue = (
        AsyncMock(side_effect=RuntimeError("ambiguous priority claim"))
        if raises
        else AsyncMock(return_value=None)
    )
    on_turn_start = AsyncMock()
    on_error = AsyncMock()
    messages = []
    callbacks = _callbacks(
        [
            {
                "id": "requeue-a",
                "role": "event",
                "source": "officer_wake",
                "content": "wake A",
                "delivery_id": "90909090-9090-4090-8090-909090909090",
                "claim_generation": 3,
            },
            {
                "id": "requeue-b",
                "role": "human",
                "source": "direct_human",
                "content": "human B",
                "delivery_id": "91919191-9191-4191-8191-919191919191",
                "claim_generation": 1,
            },
        ],
        check_interrupt=MagicMock(return_value="hard"),
        defer_and_requeue_input_delivery=requeue,
        on_turn_start=on_turn_start,
        on_error=on_error,
    )

    await run_persistent_loop(
        llm_with_tools=MagicMock(reasoning=None),
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=messages,
    )

    requeue.assert_awaited_once()
    on_turn_start.assert_awaited_once_with(1)
    on_error.assert_awaited_once()
    assert "halted" in on_error.await_args.args[0]
    assert not any(
        isinstance(message, HumanMessage) and message.content == "wake A"
        for message in messages
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("defer_succeeds", [True, False])
async def test_task_cancellation_before_admission_quarantines_delivery(
    defer_succeeds,
):
    defer = AsyncMock(return_value=defer_succeeds)
    on_error = AsyncMock()
    provider_calls = 0

    async def _stream(*_args, **_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        yield AIMessage(content="must not run")

    llm = MagicMock(reasoning=None)
    llm.astream = _stream
    memory_service = MagicMock()
    memory_service.assemble = AsyncMock(side_effect=asyncio.CancelledError)
    memory_service.capture = AsyncMock()
    messages = []
    callbacks = _callbacks(
        [
            {
                "id": "cancelled-task-message",
                "role": "human",
                "source": "direct_human",
                "content": "must leave live context",
                "delivery_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
                "claim_generation": 4,
            },
            {
                "id": "later-message",
                "role": "human",
                "source": "direct_human",
                "content": "must remain queued",
                "delivery_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                "claim_generation": 1,
            },
        ],
        defer_input_delivery=defer,
        on_error=on_error,
    )
    config = _config()
    config.memory.query = None

    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[],
        context_manager=_context_manager(),
        config=config,
        system_prompt="system",
        callbacks=callbacks,
        messages=messages,
        memory_service=memory_service,
    )

    assert provider_calls == 0
    defer.assert_awaited_once_with(
        "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        4,
        "turn_cancelled_before_provider",
    )
    assert not any(
        isinstance(message, HumanMessage)
        and message.content == "must leave live context"
        for message in messages
    )
    callbacks.on_turn_complete.assert_not_awaited()
    callbacks.on_turn_settled.assert_not_awaited()
    memory_service.capture.assert_not_awaited()
    if defer_succeeds:
        on_error.assert_not_awaited()
    else:
        on_error.assert_awaited_once()
        assert "halted" in on_error.await_args.args[0]


@pytest.mark.asyncio
async def test_mid_tool_termination_settles_pair_and_starts_no_second_provider():
    """An admitted tool batch may settle; its follow-up provider call may not."""

    admission_open = True
    provider_calls = 0
    persisted: list = []

    async def _stream(_messages, **_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        yield AIMessage(
            content="",
            tool_calls=[{"id": "call_term", "name": "inspect", "args": {}}],
        )

    async def _tool(_args):
        nonlocal admission_open
        admission_open = False
        return "settled result"

    llm = MagicMock(reasoning=None)
    llm.astream = _stream
    tool = MagicMock()
    tool.name = "inspect"
    tool.ainvoke = AsyncMock(side_effect=_tool)
    callbacks = _callbacks(
        ["inspect"],
        persist_message=AsyncMock(side_effect=persisted.append),
        before_provider_admission=lambda: admission_open,
    )
    messages: list = []

    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[tool],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=messages,
    )

    assert provider_calls == 1
    call_ids = {
        call["id"]
        for message in persisted
        if isinstance(message, AIMessage)
        for call in (message.tool_calls or [])
    }
    result_ids = {
        message.tool_call_id
        for message in persisted
        if isinstance(message, ToolMessage)
    }
    assert call_ids == result_ids == {"call_term"}
    assert callbacks.on_turn_complete.await_count == 1

    replacement_inputs: list[list] = []

    async def _replacement_stream(provider_input, **_kwargs):
        replacement_inputs.append(list(provider_input))
        yield AIMessage(content="replacement continued")

    replacement = MagicMock(reasoning=None)
    replacement.astream = _replacement_stream
    await run_persistent_loop(
        llm_with_tools=replacement,
        tools=[tool],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=_callbacks(["next wake"]),
        messages=list(messages),
    )
    restored_calls = {
        call["id"]
        for message in replacement_inputs[0]
        if isinstance(message, AIMessage)
        for call in (message.tool_calls or [])
    }
    restored_results = {
        message.tool_call_id
        for message in replacement_inputs[0]
        if isinstance(message, ToolMessage)
    }
    assert restored_calls == restored_results == {"call_term"}


@pytest.mark.asyncio
async def test_termination_fence_survives_model_and_tool_hot_swap():
    provider_calls = 0

    async def _stream(*_args, **_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        yield AIMessage(content="must not run")

    original = MagicMock(reasoning=None)
    original.astream = _stream
    rebuilt = MagicMock(reasoning=None)
    rebuilt.astream = _stream
    rebuilt_tool = MagicMock()
    rebuilt_tool.name = "list_jobs"
    authorization = AsyncMock(return_value=(True, "maintained"))
    callbacks = _callbacks(
        ["direct", {"id": "queued", "role": "event", "content": "wake"}],
        before_turn_authorization=authorization,
        before_provider_admission=lambda: False,
    )
    manager = _context_manager()
    current = MagicMock(return_value=(rebuilt, [rebuilt_tool]))

    await run_persistent_loop(
        llm_with_tools=original,
        tools=[],
        context_manager=manager,
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=[],
        get_current_tools=current,
    )

    assert current.call_count == 2
    assert callbacks.persist_message.await_count == 2
    assert authorization.await_count == 0
    assert manager.ensure_within_limits.await_count == 0
    assert provider_calls == 0


@pytest.mark.asyncio
async def test_precompaction_background_capture_cannot_start_provider_after_fence():
    """A task scheduled pre-fence re-checks at the actual aux invocation."""

    entered = asyncio.Event()
    release = asyncio.Event()
    gate_open = True
    provider = MagicMock()
    provider.ainvoke = AsyncMock(return_value=AIMessage(content="must not run"))
    aux = AuxiliaryLLM(provider)
    aux.set_provider_admission_gate(lambda: gate_open)

    class _Writer:
        event_kinds = frozenset({"pre_compaction"})

        async def on_event(self, _event):
            entered.set()
            await release.wait()
            await aux.ainvoke([], task_name="pre_compaction")

    manager = MemoryManager(
        SimpleNamespace(),
        writers=[("pre_compaction", _Writer())],
    )
    task = manager.capture_nowait(
        CaptureEvent(kind="pre_compaction", messages=[], phase=0)
    )
    await entered.wait()
    gate_open = False
    release.set()
    await task

    provider.ainvoke.assert_not_awaited()
    assert manager.background_tasks_inflight == 0
    assert aux.provider_calls_inflight == 0


@pytest.mark.asyncio
async def test_failed_officer_maintenance_fences_queued_auxiliary_and_rebuild(
    monkeypatch, tmp_path
):
    """A queued aux call re-checks authorization after the pre-turn failure."""

    entered = asyncio.Event()
    release = asyncio.Event()
    boot_model = MagicMock()
    boot_model.ainvoke = AsyncMock(return_value=AIMessage(content="must not run"))
    boot_aux = AuxiliaryLLM(boot_model)
    officer_config = SimpleNamespace(officer=SimpleNamespace(enabled=True))
    session = SimpleNamespace(
        config=officer_config,
        auxiliary_llm=boot_aux,
        memory_service=None,
    )
    client = SimpleNamespace(
        maintain_runtime_actor=AsyncMock(
            side_effect=[
                (False, "verification maintenance failed"),
                (True, "verification maintenance recovered"),
            ]
        )
    )
    monkeypatch.setattr(persistent_app, "_session", session)
    monkeypatch.setattr(persistent_app, "_orchestrator_client", client)
    monkeypatch.setattr(
        persistent_app, "_TERMINATION_SENTINEL_PATH", tmp_path / "terminating"
    )
    monkeypatch.setattr(persistent_app, "_termination_admission_fenced", False)
    monkeypatch.setattr(persistent_app, "_runtime_authorization_admission_open", True)
    persistent_app._wire_session_aux_archiver()

    class _Writer:
        event_kinds = frozenset({"pre_compaction"})

        async def on_event(self, _event):
            entered.set()
            await release.wait()
            await boot_aux.ainvoke([], task_name="pre_compaction")

    manager = MemoryManager(
        SimpleNamespace(),
        writers=[("pre_compaction", _Writer())],
    )
    queued = manager.capture_nowait(
        CaptureEvent(kind="pre_compaction", messages=[], phase=0)
    )
    await entered.wait()

    assert await persistent_app._loop_before_turn_authorization() == (
        False,
        "verification maintenance failed",
    )
    # The primary loop remains able to reach a later maintenance retry, while
    # background provider work is closed immediately.
    assert persistent_app._loop_provider_admission_open() is True
    assert persistent_app._loop_auxiliary_provider_admission_open() is False
    release.set()
    await queued
    boot_model.ainvoke.assert_not_awaited()

    # A live config rebuild gets the same process-owned callback rather than
    # inheriting an open provider gate from a new AuxiliaryLLM instance.
    rebuilt_model = MagicMock()
    rebuilt_model.ainvoke = AsyncMock(return_value=AIMessage(content="recovered"))
    rebuilt_aux = AuxiliaryLLM(rebuilt_model)
    session.auxiliary_llm = rebuilt_aux
    persistent_app._wire_session_aux_archiver()
    with pytest.raises(AuxiliaryProviderAdmissionClosed):
        await rebuilt_aux.ainvoke([], task_name="rebuilt_while_failed")
    rebuilt_model.ainvoke.assert_not_awaited()

    assert await persistent_app._loop_before_turn_authorization() == (
        True,
        "verification maintenance recovered",
    )
    assert persistent_app._loop_auxiliary_provider_admission_open() is True
    response = await rebuilt_aux.ainvoke([], task_name="rebuilt_after_recovery")
    assert response.content == "recovered"
    rebuilt_model.ainvoke.assert_awaited_once()


@pytest.mark.asyncio
async def test_auxiliary_gate_survives_rebuild_and_quiescence_tracks_inflight(
    monkeypatch, tmp_path
):
    started = asyncio.Event()
    release = asyncio.Event()

    async def _invoke(_messages):
        started.set()
        await release.wait()
        return AIMessage(content="done")

    model = MagicMock()
    model.ainvoke = AsyncMock(side_effect=_invoke)
    rebuilt = AuxiliaryLLM(model)
    session = SimpleNamespace(auxiliary_llm=rebuilt, memory_service=None)
    monkeypatch.setattr(persistent_app, "_session", session)
    monkeypatch.setattr(
        persistent_app, "_TERMINATION_SENTINEL_PATH", tmp_path / "terminating"
    )
    monkeypatch.setattr(persistent_app, "_termination_admission_fenced", False)
    monkeypatch.setattr(persistent_app, "_thread_id", str(uuid4()))
    monkeypatch.setattr(persistent_app, "_tool_inflight", False)
    monkeypatch.setattr(persistent_app, "_turn_event_open", False)
    monkeypatch.setattr(persistent_app, "_loop_task", None)
    persistent_app._wire_session_aux_archiver()

    call = asyncio.create_task(rebuilt.ainvoke([], task_name="rebuilt_aux"))
    await started.wait()
    assert rebuilt.provider_calls_inflight == 1
    assert persistent_app._termination_quiescent() is False
    release.set()
    await call
    assert persistent_app._termination_quiescent() is True

    persistent_app.activate_termination_admission_fence("test")
    with pytest.raises(AuxiliaryProviderAdmissionClosed):
        await rebuilt.ainvoke([], task_name="after_fence")
    assert model.ainvoke.await_count == 1


@pytest.mark.asyncio
async def test_callback_absence_preserves_ordinary_persistent_behavior():
    calls = 0

    async def _stream(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        yield AIMessage(content="ordinary reply")

    llm = MagicMock(reasoning=None)
    llm.astream = _stream
    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=_callbacks(["hello"]),
        messages=[],
    )
    assert calls == 1


@pytest.mark.asyncio
async def test_deferred_durable_wake_retry_settles_once(
    monkeypatch,
):
    """Persistence without admission defers one stable identity until settled."""

    thread_id = str(uuid4())
    event_id = 41
    delivery_id = str(uuid4())
    row = {
        "id": event_id,
        "thread_id": thread_id,
        "project_id": None,
        "source": "timer",
        "dedup_key": "timer",
        "payload": {
            "minutes": 30,
            "reason": "wake after replacement",
            "_delivery_id": delivery_id,
        },
        "delivery_id": delivery_id,
    }
    db = SimpleNamespace(
        claim_pending_session_wake_events=AsyncMock(return_value=[row]),
        assign_session_wake_delivery_groups=AsyncMock(return_value=[row]),
        get_session_wake_delivery_group=AsyncMock(return_value=[row]),
        get_thread=AsyncMock(
            return_value={
                "id": thread_id,
                "project_id": None,
                "metadata": {"config_override": {"officer": {"enabled": True}}},
            }
        ),
        finish_session_wake_events=AsyncMock(),
        release_session_wake_events=AsyncMock(),
        defer_session_wake_events=AsyncMock(),
        defer_session_wake_events_for_input=AsyncMock(),
        persist_thread_input_delivery=AsyncMock(
            side_effect=[
                {
                    "thread_id": thread_id,
                    "state": "persisted",
                    "transcript_inserted": True,
                },
                {
                    "thread_id": thread_id,
                    "state": "settled",
                    "transcript_inserted": False,
                },
            ]
        ),
        merge_thread_officer_state=AsyncMock(),
    )
    monkeypatch.setattr(
        session_wake,
        "_officer_ceiling_deferral",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        sitrep,
        "build_wake_message",
        AsyncMock(return_value=("bounded wake", {"fingerprint": "next"})),
    )

    assert await session_wake.drain_pending_event_wakes(db) == 0
    assert db.defer_session_wake_events_for_input.await_args.args[0] == [event_id]
    db.release_session_wake_events.assert_not_awaited()
    db.finish_session_wake_events.assert_not_awaited()

    assert await session_wake.drain_pending_event_wakes(db) == 1
    db.finish_session_wake_events.assert_awaited_once_with([event_id])
    assert [
        call.kwargs["delivery_id"]
        for call in db.persist_thread_input_delivery.await_args_list
    ] == [
        delivery_id,
        delivery_id,
    ]


@pytest.mark.asyncio
async def test_orchestrator_surfaces_terminating_direct_input_as_retryable(
    monkeypatch,
):
    from fastapi import HTTPException
    from orchestrator.services import pinned_forwarding

    class _Response:
        status_code = 503
        text = '{"error":"runtime_terminating"}'
        headers = {"Retry-After": "7"}

        @staticmethod
        def json():
            return {"error": "runtime_terminating", "retryable": True}

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, *args, **kwargs):
            return _Response()

    monkeypatch.setattr(pinned_forwarding.httpx, "AsyncClient", _Client)
    binding = PinnedSessionBinding(
        thread_id="11111111-1111-4111-8111-111111111111",
        runtime_generation="22222222-2222-4222-8222-222222222222",
        agent_id="33333333-3333-4333-8333-333333333333",
        runtime_attach_token="44444444-4444-4444-8444-444444444444",
        agent_hostname="persistent-thread-a",
        pod_namespace="srw",
        pod_uid="pod-uid-a",
        pod_ip="127.0.0.1",
        pod_port=8001,
        agent_status="session",
    )
    monkeypatch.setattr(
        pinned_forwarding,
        "revalidate_pinned_forwarding_binding",
        AsyncMock(return_value=binding),
    )

    with pytest.raises(HTTPException) as caught:
        await pinned_forwarding.forward_to_agent(
            binding,
            "/api/input",
            {"content": "retain me"},
            store=object(),
        )

    assert caught.value.status_code == 503
    assert caught.value.detail["error"] == "runtime_terminating"
    assert caught.value.detail["retryable"] is True
    assert caught.value.headers == {"Retry-After": "7"}
