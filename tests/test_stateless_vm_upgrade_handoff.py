"""The stateless → pinned lane switch for a VM upgrade (C2): one transaction."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from uuid import UUID

import pytest

from orchestrator.services import stateless_vm_upgrade_handoff as handoff
from orchestrator.services.stateless_workspace_upgrade import VM_UPGRADE_NOT_POSSIBLE
from orchestrator.services.thread_control_inbox import (
    ControlAdmissionError,
    ControlAdmissionNotReady,
)
from shared.session_retirement import STATELESS_STOP_KEYS

THREAD_ID = UUID("11111111-aaaa-4444-8888-111111111111")
OWNER_ID = UUID("22222222-bbbb-4444-8888-222222222222")
GENERATION = UUID("77777777-aaaa-4444-8888-777777777777")
CLIENT_REQUEST_ID = UUID("66666666-ffff-4444-8888-666666666666")
REQUEST_ID = UUID("55555555-eeee-4444-8888-555555555555")
PAYLOAD = {"runtime_generation": str(GENERATION), "target_tier": "vm"}


def _thread(**over):
    row = {
        "id": THREAD_ID,
        "user_id": OWNER_ID,
        "status": "active",
        "kind": "session",
        "execution_lane": "stateless",
        "agent_id": None,
        "runtime_generation": GENERATION,
        "runtime_attach_token": None,
        "runtime_retirement_token": None,
        "control_admission_agent_id": None,
        "control_seq_hwm": 7,
        "metadata": {"config_override": {"workspace": {"backend": "virtual"}}},
    }
    row.update(over)
    return row


def _queue(**over):
    row = {
        "unit_kind": "session_turn",
        "state": "done",
        "lease_token": 4,
        "input_seq": 3,
        "consumed_seq": 3,
        "control_input_seq": 7,
        "control_consumed_seq": 7,
    }
    row.update(over)
    return row


class _Conn:
    def __init__(
        self,
        *,
        thread=None,
        existing=None,
        queue=None,
        pending=None,
        background=False,
        agent_row=False,
        flipped=True,
    ):
        self.answers = {
            handoff._THREAD_SQL: thread if thread is not None else _thread(),
            handoff._EXISTING_SQL: existing,
            handoff._QUEUE_SQL: queue,
            handoff._PENDING_CONTROL_SQL: pending,
            handoff._BACKGROUND_WORK_SQL: background,
            handoff._AGENT_ROW_SQL: agent_row,
            handoff._BUMP_HWM_SQL: THREAD_ID,
            handoff._INSERT_REQUEST_SQL: REQUEST_ID,
            handoff._CLOSE_QUEUE_SQL: 5,
            handoff._FLIP_LANE_SQL: THREAD_ID if flipped else None,
        }
        self.calls: list = []

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args))
        return self.answers[sql]

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        return self.answers[sql]

    @asynccontextmanager
    async def transaction(self):
        yield


class _DB:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


@pytest.fixture(autouse=True)
def journal(monkeypatch):
    async def bump(conn, *, thread_id):
        conn.calls.append(("bump_epoch", (thread_id,)))
        return 3

    async def append(conn, *, thread_id, kind, payload):
        conn.calls.append(("append_system_frame", (kind, payload)))
        return (3, 11)

    monkeypatch.setattr(handoff, "bump_epoch", bump)
    monkeypatch.setattr(handoff, "append_system_frame", append)


async def _switch(conn, payload=PAYLOAD):
    return await handoff.begin_stateless_vm_upgrade_handoff(
        _DB(conn),
        thread_id=THREAD_ID,
        owner_user_id=OWNER_ID,
        client_request_id=CLIENT_REQUEST_ID,
        payload=payload,
        requested_by="owner",
    )


def _sqls(conn):
    return [sql for sql, _ in conn.calls]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "queue", [_queue(), _queue(state="queued")], ids=["done", "queued-idle"]
)
async def test_a_quiet_lite_row_switches_in_the_spec_order(queue):
    conn = _Conn(queue=queue)
    admitted = await _switch(conn)

    assert (admitted.id, admitted.request_seq, admitted.duplicate) == (
        REQUEST_ID,
        8,
        False,
    )
    assert (admitted.state, admitted.runtime_generation) == ("pending", GENERATION)
    assert _sqls(conn) == [
        handoff._THREAD_SQL,
        handoff._EXISTING_SQL,
        handoff._QUEUE_SQL,
        handoff._PENDING_CONTROL_SQL,
        handoff._BACKGROUND_WORK_SQL,
        handoff._AGENT_ROW_SQL,
        handoff._BUMP_HWM_SQL,
        handoff._INSERT_REQUEST_SQL,
        handoff._CLOSE_QUEUE_SQL,
        "bump_epoch",
        "append_system_frame",
        handoff._FLIP_LANE_SQL,
    ]
    args = dict(conn.calls)
    insert = args[handoff._INSERT_REQUEST_SQL]
    assert insert[1] == 8 and insert[3] == "upgrade-to-workspace"
    assert json.loads(insert[4]) == PAYLOAD and insert[6] == GENERATION
    assert args[handoff._CLOSE_QUEUE_SQL] == (THREAD_ID, 4)
    assert args[handoff._FLIP_LANE_SQL] == (
        THREAD_ID,
        GENERATION,
        "_stateless_active_claim",
    )
    kind, frame = args["append_system_frame"]
    assert kind == "session.lane_changed"
    assert frame == {
        "from": "stateless",
        "to": "pinned",
        "reason": "vm_upgrade",
        "control_request_id": str(REQUEST_ID),
        "client_request_id": str(CLIENT_REQUEST_ID),
        "session_runtime_generation": str(GENERATION),
    }


@pytest.mark.asyncio
async def test_a_template_payload_is_stored_as_the_client_sent_it():
    conn = _Conn(queue=None)
    payload = {"runtime_generation": str(GENERATION), "template": "vm-big"}
    await _switch(conn, payload)
    assert json.loads(dict(conn.calls)[handoff._INSERT_REQUEST_SQL][4]) == payload
    assert handoff._CLOSE_QUEUE_SQL not in _sqls(conn)


@pytest.mark.asyncio
async def test_a_same_uuid_retry_returns_the_original_admission_even_after_the_switch():
    existing = {
        "id": REQUEST_ID,
        "request_seq": 8,
        "client_request_id": CLIENT_REQUEST_ID,
        "verb": "upgrade-to-workspace",
        "payload": json.dumps(PAYLOAD),
        "outcome": None,
        "runtime_generation": GENERATION,
    }
    conn = _Conn(thread=_thread(execution_lane="pinned"), existing=existing)
    admitted = await _switch(conn)
    assert (admitted.id, admitted.duplicate) == (REQUEST_ID, True)
    assert _sqls(conn) == [handoff._THREAD_SQL, handoff._EXISTING_SQL]


@pytest.mark.asyncio
async def test_a_reused_uuid_for_another_control_is_refused():
    existing = {
        "id": REQUEST_ID,
        "request_seq": 8,
        "client_request_id": CLIENT_REQUEST_ID,
        "verb": "compact",
        "payload": "{}",
        "outcome": None,
        "runtime_generation": GENERATION,
    }
    with pytest.raises(ControlAdmissionError, match="already used"):
        await _switch(_Conn(existing=existing))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "over",
    [
        {"execution_lane": "pinned"},
        {"status": "ended"},
        {"runtime_generation": UUID(int=9)},
        {"agent_id": UUID(int=1)},
        {"runtime_attach_token": UUID(int=2)},
        {"control_admission_agent_id": UUID(int=3)},
        {"runtime_retirement_token": UUID(int=4)},
        {"kind": "subagent"},
        {
            "metadata": {
                "config_override": {"workspace": {"backend": "virtual"}},
                "vm": {"status": "failed"},
            }
        },
        {"metadata": {"config_override": {"workspace": {"backend": "sandbox"}}}},
        {
            "metadata": {
                "config_override": {"workspace": {"backend": "virtual"}},
                "protected_cloud": True,
            }
        },
    ],
)
async def test_a_row_that_cannot_switch_is_refused_before_any_write(over):
    conn = _Conn(thread=_thread(**over), queue=_queue())
    with pytest.raises(ControlAdmissionError) as refused:
        await _switch(conn)
    assert type(refused.value) is ControlAdmissionError
    assert str(refused.value) == VM_UPGRADE_NOT_POSSIBLE
    assert _sqls(conn) == [handoff._THREAD_SQL, handoff._EXISTING_SQL]


@pytest.mark.asyncio
@pytest.mark.parametrize("key", sorted(STATELESS_STOP_KEYS))
async def test_a_stop_marker_says_the_session_is_ending(key):
    thread = _thread()
    thread["metadata"] = {**thread["metadata"], key: {}}
    with pytest.raises(ControlAdmissionError) as refused:
        await _switch(_Conn(thread=thread, queue=_queue()))
    assert str(refused.value) == handoff.VM_UPGRADE_SESSION_ENDING


@pytest.mark.asyncio
async def test_another_owner_sees_an_unavailable_thread():
    with pytest.raises(ControlAdmissionError, match="Thread is unavailable"):
        await _switch(_Conn(thread=_thread(user_id=UUID(int=8))))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "queue,error,message",
    [
        (
            _queue(state="leased"),
            ControlAdmissionNotReady,
            handoff.VM_UPGRADE_TURN_RUNNING,
        ),
        (
            _queue(state="queued", input_seq=4),
            ControlAdmissionNotReady,
            handoff.VM_UPGRADE_INPUT_PENDING,
        ),
        (
            _queue(state="queued", consumed_seq=None),
            ControlAdmissionNotReady,
            handoff.VM_UPGRADE_INPUT_PENDING,
        ),
        (
            _queue(control_input_seq=8),
            ControlAdmissionNotReady,
            handoff.VM_UPGRADE_BACKGROUND_WORK,
        ),
        (
            _queue(state="parked"),
            ControlAdmissionError,
            "Session queue is parked; unpark it before sending controls",
        ),
        (
            _queue(unit_kind="bg_task"),
            ControlAdmissionError,
            "Session queue identity is incompatible",
        ),
    ],
)
async def test_queue_state_refusals(queue, error, message):
    conn = _Conn(queue=queue)
    with pytest.raises(ControlAdmissionError) as refused:
        await _switch(conn)
    assert type(refused.value) is error
    assert str(refused.value) == message
    assert handoff._INSERT_REQUEST_SQL not in _sqls(conn)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stranded,error,message",
    [
        (
            True,
            ControlAdmissionError,
            "Session has a control awaiting its pinned owner",
        ),
        (False, ControlAdmissionNotReady, handoff.VM_UPGRADE_BACKGROUND_WORK),
    ],
)
async def test_pending_controls_refuse(stranded, error, message):
    conn = _Conn(queue=_queue(), pending={"stranded": stranded})
    with pytest.raises(ControlAdmissionError) as refused:
        await _switch(conn)
    assert (type(refused.value), str(refused.value)) == (error, message)


@pytest.mark.asyncio
async def test_background_work_defers_the_switch():
    """Interrupts, permissions, input deliveries, live subagents, cloud pushes."""
    with pytest.raises(ControlAdmissionNotReady) as refused:
        await _switch(_Conn(queue=_queue(), background=True))
    assert str(refused.value) == handoff.VM_UPGRADE_BACKGROUND_WORK


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["agent_row", "lost_flip"])
async def test_a_reciprocal_agent_row_or_a_lost_flip_refuses(change):
    conn = _Conn(
        queue=_queue(), agent_row=change == "agent_row", flipped=change != "lost_flip"
    )
    with pytest.raises(ControlAdmissionError) as refused:
        await _switch(conn)
    assert str(refused.value) == VM_UPGRADE_NOT_POSSIBLE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"target_tier": "vm"},
        {"runtime_generation": "not-a-uuid", "target_tier": "vm"},
        {"runtime_generation": str(GENERATION), "target_tier": "sandbox"},
        {"runtime_generation": str(GENERATION), "focus": "x"},
    ],
)
async def test_the_payload_is_rechecked(payload):
    conn = _Conn(queue=_queue())
    with pytest.raises(ControlAdmissionError):
        await _switch(conn, payload)
    assert conn.calls == []
