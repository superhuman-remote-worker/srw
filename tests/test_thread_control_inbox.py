"""Focused contract tests for durable session-control admission.

These tests deliberately stop at the admission boundary. The orchestrator may
persist request/queue watermarks and wake the stateless queue, but it never
publishes the desired scalar or writes its journal acknowledgement; those are
lease-owner duties.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from tests import _b09_control_seams as control_seams

# R1.B06: this operation moved to services/agent_thread_status.
from orchestrator.services import agent_thread_status  # noqa: E402
from fastapi import HTTPException
from pydantic import ValidationError

from orchestrator.routers import thread_session
from orchestrator.schemas.thread_session import ThreadControlRequest
from orchestrator.services.thread_control_inbox import (
    AdmittedControl,
    ControlAdmissionError,
    ControlAdmissionNotReady,
    admit_thread_control,
)
from orchestrator.application import sessions as sessions_composition
from orchestrator.schemas import agent_thread_status as agent_thread_status_module
from orchestrator.security import access as access_module
from orchestrator.services import officer_conference as officer_conference_module
from orchestrator.services import thread_retirement as thread_retirement_module


THREAD_ID = UUID("11111111-aaaa-4444-8888-111111111111")
OWNER_ID = UUID("22222222-bbbb-4444-8888-222222222222")
PROJECT_ID = UUID("33333333-cccc-4444-8888-333333333333")
AGENT_ID = UUID("44444444-dddd-4444-8888-444444444444")
PROCESS_GENERATION = "99999999-9999-4999-8999-999999999999"
REQUEST_ID = UUID("55555555-eeee-4444-8888-555555555555")
CLIENT_REQUEST_ID = UUID("66666666-ffff-4444-8888-666666666666")
RUNTIME_GENERATION = UUID("77777777-aaaa-4444-8888-777777777777")
ATTACH_TOKEN = UUID("99999999-aaaa-4444-8888-999999999999")


class _AsyncContext:
    def __init__(self, value: Any = None) -> None:
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Transaction:
    def __init__(self, conn: "_ControlConn") -> None:
        self.conn = conn

    async def __aenter__(self):
        assert not self.conn.in_transaction
        self.conn.in_transaction = True
        self.conn.calls.append(("transaction_enter", "", ()))

    async def __aexit__(self, exc_type, exc, tb):
        self.conn.calls.append(("transaction_exit", "", (exc_type,)))
        self.conn.in_transaction = False
        return False


class _ControlConn:
    """Query-aware asyncpg stand-in that also proves one transaction owns all IO."""

    def __init__(
        self,
        *,
        thread: dict[str, Any] | None,
        existing: dict[str, Any] | None = None,
        reciprocal: int | None = None,
        queue_state: str | None = None,
        queue_kind: str = "session_turn",
        input_seq: int | None = None,
        consumed_seq: int | None = None,
        stranded_pinned: int | None = None,
        baseline_input_seq: int = 0,
        recorded_queue_state: str = "queued",
        request_id: UUID = REQUEST_ID,
    ) -> None:
        self.thread = thread
        self.existing = existing
        self.reciprocal = reciprocal
        self.queue_state = queue_state
        self.queue_kind = queue_kind
        self.input_seq = input_seq
        self.consumed_seq = consumed_seq
        self.stranded_pinned = stranded_pinned
        self.baseline_input_seq = baseline_input_seq
        self.recorded_queue_state = recorded_queue_state
        self.request_id = request_id
        self.in_transaction = False
        self.calls: list[tuple[str, str, tuple[Any, ...]]] = []

    def transaction(self):
        return _Transaction(self)

    def _record(self, operation: str, sql: str, args: tuple[Any, ...]) -> None:
        assert self.in_transaction, f"{operation} escaped admission transaction"
        self.calls.append((operation, sql, args))

    async def fetchrow(self, sql: str, *args):
        self._record("fetchrow", sql, args)
        if "FROM threads WHERE id" in sql:
            return self.thread
        if "FROM thread_control_requests" in sql:
            return self.existing
        if "FROM run_queue" in sql:
            if self.queue_state is None:
                return None
            return {
                "unit_kind": self.queue_kind,
                "state": self.queue_state,
                "input_seq": self.input_seq,
                "consumed_seq": self.consumed_seq,
            }
        raise AssertionError(f"unexpected fetchrow query: {sql}")

    async def fetchval(self, sql: str, *args):
        self._record("fetchval", sql, args)
        if "FROM agents" in sql:
            return self.reciprocal
        if "accepted_agent_id IS NOT NULL" in sql:
            return self.stranded_pinned
        if "INSERT INTO thread_control_requests" in sql:
            return self.request_id
        if "MAX(seq)" in sql and "thread_messages" in sql:
            return self.baseline_input_seq
        if "control_input_seq = GREATEST" in sql:
            return self.recorded_queue_state
        raise AssertionError(f"unexpected fetchval query: {sql}")

    async def execute(self, sql: str, *args):
        self._record("execute", sql, args)
        assert "UPDATE threads SET control_seq_hwm" in sql
        return "UPDATE 1"


class _ControlDB:
    def __init__(self, conn: _ControlConn) -> None:
        self.conn = conn

    def acquire(self):
        return _AsyncContext(self.conn)


def _thread(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": THREAD_ID,
        "user_id": OWNER_ID,
        "agent_id": None,
        "status": "active",
        "execution_lane": "stateless",
        "control_seq_hwm": 4,
        "control_admission_agent_id": None,
        "runtime_generation": RUNTIME_GENERATION,
        "runtime_retirement_token": None,
        "runtime_attach_token": None,
        "metadata": {"config_override": {"workspace": {"backend": "virtual"}}},
    }
    row.update(overrides)
    return row


def _calls(conn: _ControlConn, operation: str, contains: str):
    return [call for call in conn.calls if call[0] == operation and contains in call[1]]


def _unused(name: str):
    async def _fail(*_args: Any, **_kwargs: Any):
        raise AssertionError(f"the control route must not reach {name}")

    return _fail


def _control_dependencies(
    *, owner: Any, enforce: Any = None, store: Any = None
) -> thread_session.ThreadSessionDependencies:
    """The control route's application collaborators: the owner gate and PDP.

    Admission, idempotency lookup, the workspace gate and the audit log are
    module-level names of ``routers.thread_session`` and are patched there.
    """
    return thread_session.ThreadSessionDependencies(
        store=store if store is not None else MagicMock(name="store"),
        require_thread_owner=owner,
        require_approved_user=_unused("require_approved_user"),
        resolve_cloud_session_url=_unused("resolve_cloud_session_url"),
        resolve_session_config=_unused("resolve_session_config"),
        enforce_session_create_grants=(
            enforce if enforce is not None else _unused("enforce_session_create_grants")
        ),
        tool_view=SimpleNamespace(),
    )


@pytest.mark.parametrize(
    "payload",
    [
        {
            "client_request_id": str(CLIENT_REQUEST_ID),
            "method": "mode.set",
            "mode": "verbose",
        },
        {
            "client_request_id": str(CLIENT_REQUEST_ID),
            "method": "narration.set",
            "mode": "autonomous",
        },
        {
            "client_request_id": "not-a-uuid",
            "method": "mode.set",
            "mode": "supervised",
        },
    ],
)
def test_public_control_envelope_rejects_invalid_method_mode_or_request_id(payload):
    with pytest.raises(ValidationError):
        ThreadControlRequest.model_validate(payload)


def test_public_workspace_undo_envelope_has_empty_canonical_payload():
    body = ThreadControlRequest(
        client_request_id=CLIENT_REQUEST_ID,
        method="workspace.undo",
    )
    assert body.mode is None
    assert body.control_payload() == {}

    with pytest.raises(ValidationError, match="does not accept a mode"):
        ThreadControlRequest(
            client_request_id=CLIENT_REQUEST_ID,
            method="workspace.undo",
            mode="auto",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["permission_mode", "narration_mode"])
async def test_generic_orchestrator_config_update_cannot_bypass_inbox(key):
    with pytest.raises(HTTPException) as exc:
        await control_seams.apply_thread_config_update(
            str(THREAD_ID),
            {"id": THREAD_ID, "user_id": OWNER_ID},
            {"interactive": {key: "auto"}},
            None,
            request=MagicMock(),
            actor={"id": OWNER_ID},
        )

    assert exc.value.status_code == 409
    assert "session control endpoint" in exc.value.detail


@pytest.mark.asyncio
async def test_control_endpoint_stops_at_exact_owner_gate():
    denial = HTTPException(status_code=403, detail="Not thread owner")
    owner = AsyncMock(side_effect=denial)
    admit = AsyncMock()
    request = MagicMock()
    store = MagicMock(name="store")
    body = ThreadControlRequest(
        client_request_id=CLIENT_REQUEST_ID,
        method="narration.set",
        mode="verbose",
    )

    with patch.object(thread_session, "admit_thread_control", admit):
        with pytest.raises(HTTPException) as exc:
            await thread_session.submit_thread_control(
                str(THREAD_ID),
                body,
                request,
                dependencies=_control_dependencies(owner=owner, store=store),
            )

    assert exc.value is denial
    owner.assert_awaited_once_with(request, store, str(THREAD_ID))
    admit.assert_not_awaited()


@pytest.mark.asyncio
async def test_control_endpoint_admits_owner_request_without_exposing_lane():
    user = {"id": OWNER_ID}
    thread = {
        "id": THREAD_ID,
        "user_id": OWNER_ID,
        "project_id": PROJECT_ID,
        # Server-side routing fact must not reach the response.
        "execution_lane": "stateless",
        "metadata": {"config_override": {"workspace": {"backend": "virtual"}}},
    }
    owner = AsyncMock(return_value=(user, thread))
    enforce = AsyncMock()
    admitted = AdmittedControl(
        id=REQUEST_ID,
        request_seq=5,
        client_request_id=CLIENT_REQUEST_ID,
        verb="mode.set",
        state="pending",
        duplicate=False,
    )
    admit = AsyncMock(return_value=admitted)
    audit = AsyncMock()
    request = MagicMock()
    store = MagicMock(name="store")
    body = ThreadControlRequest(
        client_request_id=CLIENT_REQUEST_ID,
        method="mode.set",
        mode="supervised",
    )

    with (
        patch.object(
            thread_session,
            "find_existing_thread_control",
            new=AsyncMock(return_value=None),
        ),
        patch.object(thread_session, "admit_thread_control", admit),
        patch.object(thread_session, "log_security_event", audit),
    ):
        result = await thread_session.submit_thread_control(
            str(THREAD_ID),
            body,
            request,
            dependencies=_control_dependencies(
                owner=owner, enforce=enforce, store=store
            ),
        )

    assert result == {
        "accepted": True,
        "request_id": str(REQUEST_ID),
        "client_request_id": str(CLIENT_REQUEST_ID),
        "request_seq": 5,
        "method": "mode.set",
        "state": "pending",
        "duplicate": False,
        "session_runtime_generation": None,
    }
    assert "execution_lane" not in result
    owner.assert_awaited_once_with(request, store, str(THREAD_ID))
    enforce.assert_awaited_once_with(
        {"interactive": {"permission_mode": "supervised"}},
        user_id=str(OWNER_ID),
        project_ids=[str(PROJECT_ID)],
    )
    admit.assert_awaited_once_with(
        store,
        thread_id=str(THREAD_ID),
        owner_user_id=OWNER_ID,
        client_request_id=CLIENT_REQUEST_ID,
        verb="mode.set",
        payload={"mode": "supervised"},
        requested_by=str(OWNER_ID),
        expected_runtime_generation=None,
        require_pinned_runtime_generation=False,
    )
    audit.assert_awaited_once()


@pytest.mark.asyncio
async def test_control_endpoint_refuses_unattested_stateless_sandbox_request():
    thread = {
        "id": THREAD_ID,
        "user_id": OWNER_ID,
        "project_id": None,
        "execution_lane": "stateless",
        "metadata": {"config_override": {"workspace": {"backend": "sandbox"}}},
    }
    admit = AsyncMock()
    with (
        patch.object(
            thread_session,
            "find_existing_thread_control",
            AsyncMock(return_value=None),
        ),
        patch.object(thread_session, "admit_thread_control", admit),
    ):
        with pytest.raises(HTTPException) as exc:
            await thread_session.submit_thread_control(
                str(THREAD_ID),
                ThreadControlRequest(
                    client_request_id=CLIENT_REQUEST_ID,
                    method="narration.set",
                    mode="verbose",
                ),
                MagicMock(),
                dependencies=_control_dependencies(
                    owner=AsyncMock(return_value=({"id": OWNER_ID}, thread))
                ),
            )

    assert exc.value.status_code == 409
    assert "attested Kubernetes sandbox" in str(exc.value.detail)
    admit.assert_not_awaited()


@pytest.mark.asyncio
async def test_control_endpoint_admits_lane_free_workspace_undo_payload():
    thread = {
        "id": THREAD_ID,
        "user_id": OWNER_ID,
        "project_id": PROJECT_ID,
        "execution_lane": "stateless",
        "metadata": {"config_override": {"workspace": {"backend": "sandbox"}}},
    }
    admitted = AdmittedControl(
        id=REQUEST_ID,
        request_seq=8,
        client_request_id=CLIENT_REQUEST_ID,
        verb="workspace.undo",
        state="pending",
        duplicate=False,
    )
    admit = AsyncMock(return_value=admitted)
    gate = MagicMock(return_value="sandbox")
    grants = AsyncMock()
    with (
        patch.object(
            thread_session,
            "find_existing_thread_control",
            AsyncMock(return_value=None),
        ) as find_existing,
        patch.object(thread_session, "require_stateless_workspace", gate),
        patch.object(thread_session, "admit_thread_control", admit),
        patch.object(thread_session, "log_security_event", AsyncMock()),
    ):
        response = await thread_session.submit_thread_control(
            str(THREAD_ID),
            ThreadControlRequest(
                client_request_id=CLIENT_REQUEST_ID,
                method="workspace.undo",
            ),
            MagicMock(),
            dependencies=_control_dependencies(
                owner=AsyncMock(return_value=({"id": OWNER_ID}, thread)),
                enforce=grants,
            ),
        )

    assert response["method"] == "workspace.undo"
    assert response["state"] == "pending"
    gate.assert_called_once_with(thread)
    grants.assert_not_called()
    assert find_existing.await_args.kwargs["payload"] == {}
    assert admit.await_args.kwargs["payload"] == {}


@pytest.mark.asyncio
async def test_admin_can_control_ownerless_legacy_thread_without_fake_uuid():
    """Admin-only ownerless threads retain their pre-REST control surface."""

    admin_id = UUID("77777777-7777-4777-8777-777777777777")
    thread = {
        "id": THREAD_ID,
        "user_id": None,
        "project_id": None,
        "execution_lane": "pinned",
    }
    admit = AsyncMock(
        return_value=AdmittedControl(
            id=REQUEST_ID,
            request_seq=1,
            client_request_id=CLIENT_REQUEST_ID,
            verb="mode.set",
            state="pending",
            duplicate=False,
        )
    )
    enforce = AsyncMock()
    with (
        patch.object(
            thread_session,
            "find_existing_thread_control",
            AsyncMock(return_value=None),
        ),
        patch.object(thread_session, "admit_thread_control", admit),
        patch.object(thread_session, "log_security_event", AsyncMock()),
    ):
        response = await thread_session.submit_thread_control(
            str(THREAD_ID),
            ThreadControlRequest(
                client_request_id=CLIENT_REQUEST_ID,
                method="mode.set",
                mode="supervised",
            ),
            MagicMock(),
            dependencies=_control_dependencies(
                owner=AsyncMock(
                    return_value=({"id": admin_id, "is_admin": True}, thread)
                ),
                enforce=enforce,
            ),
        )

    assert response["accepted"] is True
    enforce.assert_awaited_once_with(
        {"interactive": {"permission_mode": "supervised"}},
        user_id=str(admin_id),
        project_ids=[],
    )
    assert admit.await_args.kwargs["owner_user_id"] is None
    assert admit.await_args.kwargs["requested_by"] == str(admin_id)


@pytest.mark.asyncio
async def test_control_endpoint_maps_admission_conflict_to_409():
    body = ThreadControlRequest(
        client_request_id=CLIENT_REQUEST_ID,
        method="narration.set",
        mode="silent",
    )
    owner = AsyncMock(
        return_value=(
            {"id": OWNER_ID},
            {"id": THREAD_ID, "user_id": OWNER_ID, "project_id": None},
        )
    )
    # Raise the exact class object the route module catches.
    conflict = AsyncMock(
        side_effect=thread_session.ControlAdmissionError(
            "client_request_id was already used for a different control"
        )
    )
    admit = AsyncMock()

    with (
        patch.object(thread_session, "find_existing_thread_control", conflict),
        patch.object(thread_session, "admit_thread_control", admit),
    ):
        with pytest.raises(HTTPException) as exc:
            await thread_session.submit_thread_control(
                str(THREAD_ID),
                body,
                MagicMock(),
                dependencies=_control_dependencies(owner=owner),
            )

    assert exc.value.status_code == 409
    assert "client_request_id" in exc.value.detail
    admit.assert_not_awaited()


@pytest.mark.asyncio
async def test_control_endpoint_maps_transient_owner_readiness_to_425():
    body = ThreadControlRequest(
        client_request_id=CLIENT_REQUEST_ID,
        method="narration.set",
        mode="silent",
    )
    owner = AsyncMock(
        return_value=(
            {"id": OWNER_ID},
            {"id": THREAD_ID, "user_id": OWNER_ID, "project_id": None},
        )
    )
    admit = AsyncMock(
        side_effect=thread_session.ControlAdmissionNotReady(
            "Session is not ready to accept controls"
        )
    )

    with (
        patch.object(
            thread_session,
            "find_existing_thread_control",
            AsyncMock(return_value=None),
        ),
        patch.object(thread_session, "admit_thread_control", admit),
        patch.object(thread_session, "log_security_event", AsyncMock()),
    ):
        with pytest.raises(HTTPException) as exc:
            await thread_session.submit_thread_control(
                str(THREAD_ID),
                body,
                MagicMock(),
                dependencies=_control_dependencies(owner=owner),
            )

    assert exc.value.status_code == 425
    assert exc.value.detail == "Session is not ready to accept controls"


@pytest.mark.asyncio
async def test_exact_pinned_ended_status_refuses_successor_binding():
    import orchestrator.main as orchestrator_main

    successor = UUID("88888888-8888-4888-8888-888888888888")
    conn = MagicMock()
    conn.transaction = lambda: _AsyncContext()
    conn.fetchrow = AsyncMock(
        return_value={
            "id": THREAD_ID,
            "agent_id": successor,
            "execution_lane": "pinned",
            "status": "active",
            "metadata": {},
        }
    )
    conn.fetchval = AsyncMock()
    db = MagicMock()
    db.acquire = lambda: _AsyncContext(conn)
    db.get_thread = AsyncMock(
        return_value={"execution_lane": "pinned", "status": "active"}
    )

    with (
        patch.object(access_module, "require_internal", AsyncMock()),
        patch.object(orchestrator_main.app.state.resources, "postgres_db", db),
    ):
        with pytest.raises(HTTPException) as exc:
            await agent_thread_status.update_thread_status(
                str(THREAD_ID),
                agent_thread_status_module.AgentThreadStatusRequest(
                    status="ended",
                    agent_id=AGENT_ID,
                    # A pinned status write names its exact registered process;
                    # the refusal under test is the endpoint's, not the model's.
                    process_generation=PROCESS_GENERATION,
                ),
                dependencies=sessions_composition.agent_thread_status_dependencies(
                    orchestrator_main.app.state.resources
                ),
            )

    assert exc.value.status_code == 409
    conn.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["active", "awaiting_user"])
async def test_exact_pinned_live_status_refuses_stale_pre_resume_agent(status):
    import orchestrator.main as orchestrator_main

    successor = UUID("88888888-8888-4888-8888-888888888888")
    conn = MagicMock()
    conn.transaction = lambda: _AsyncContext()
    conn.fetchrow = AsyncMock(
        return_value={
            "id": THREAD_ID,
            "agent_id": successor,
            "execution_lane": "pinned",
            "status": "created",
            "metadata": {},
        }
    )
    conn.fetchval = AsyncMock()
    db = MagicMock()
    db.acquire = lambda: _AsyncContext(conn)
    db.get_thread = AsyncMock(
        return_value={
            "execution_lane": "pinned",
            "status": "created",
            "agent_id": successor,
        }
    )

    with (
        patch.object(access_module, "require_internal", AsyncMock()),
        patch.object(orchestrator_main.app.state.resources, "postgres_db", db),
    ):
        with pytest.raises(HTTPException) as exc:
            await agent_thread_status.update_thread_status(
                str(THREAD_ID),
                agent_thread_status_module.AgentThreadStatusRequest(
                    status=status,
                    agent_id=AGENT_ID,
                    process_generation=PROCESS_GENERATION,
                ),
                dependencies=sessions_composition.agent_thread_status_dependencies(
                    orchestrator_main.app.state.resources
                ),
            )

    assert exc.value.status_code == 409
    conn.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["active", "awaiting_user"])
async def test_exact_pinned_live_status_preserves_runtime_resources(status):
    import orchestrator.main as orchestrator_main

    thread = {
        "id": THREAD_ID,
        "agent_id": AGENT_ID,
        "execution_lane": "pinned",
        "status": "created",
        "metadata": {},
        "project_id": PROJECT_ID,
        "title": "protected session",
        "runtime_generation": RUNTIME_GENERATION,
        "runtime_attach_token": ATTACH_TOKEN,
        "runtime_retirement_token": None,
    }
    conn = MagicMock()
    conn.transaction = lambda: _AsyncContext()
    conn.fetchrow = AsyncMock(
        side_effect=[
            thread,
            {
                "pod_uid": None,
                "metadata": {
                    "dispatch_process_generation": PROCESS_GENERATION,
                },
            },
        ]
    )
    conn.fetchval = AsyncMock(side_effect=[1, THREAD_ID])
    db = MagicMock()
    db.acquire = lambda: _AsyncContext(conn)
    db.get_thread = AsyncMock(return_value=thread)
    suspend_resources = AsyncMock()
    conclude_conference = AsyncMock()

    with (
        patch.object(access_module, "require_internal", AsyncMock()),
        patch.object(orchestrator_main.app.state.resources, "postgres_db", db),
        patch.object(
            thread_retirement_module.ThreadRetirementOperations,
            "suspend_thread_resources",
            suspend_resources,
        ),
        patch.object(
            officer_conference_module, "conclude_conference_if_any", conclude_conference
        ),
    ):
        result = await agent_thread_status.update_thread_status(
            str(THREAD_ID),
            agent_thread_status_module.AgentThreadStatusRequest(
                status=status,
                agent_id=AGENT_ID,
                process_generation=PROCESS_GENERATION,
                session_runtime_generation=RUNTIME_GENERATION,
                session_runtime_attach_token=ATTACH_TOKEN,
            ),
            dependencies=sessions_composition.agent_thread_status_dependencies(
                orchestrator_main.app.state.resources
            ),
        )

    assert result == {"status": status}
    assert status in conn.fetchval.await_args_list[-1].args[0]
    suspend_resources.assert_not_awaited()
    conclude_conference.assert_not_awaited()


@pytest.mark.asyncio
async def test_strict_pinned_status_phase_rejects_missing_identity(monkeypatch):
    import orchestrator.main as orchestrator_main

    db = MagicMock()
    db.get_thread = AsyncMock(
        return_value={"execution_lane": "pinned", "status": "active"}
    )
    monkeypatch.setenv("REQUIRE_PINNED_STATUS_IDENTITY", "true")
    with (
        patch.object(access_module, "require_internal", AsyncMock()),
        patch.object(orchestrator_main.app.state.resources, "postgres_db", db),
    ):
        with pytest.raises(HTTPException) as exc:
            await agent_thread_status.update_thread_status(
                str(THREAD_ID),
                agent_thread_status_module.AgentThreadStatusRequest(
                    status="awaiting_user"
                ),
                dependencies=sessions_composition.agent_thread_status_dependencies(
                    orchestrator_main.app.state.resources
                ),
            )

    assert exc.value.status_code == 409
    assert exc.value.detail["code"] == "pinned_status_identity_required"


@pytest.mark.asyncio
async def test_committed_retry_bypasses_mutable_grant_policy():
    thread = {"id": THREAD_ID, "user_id": OWNER_ID, "project_id": PROJECT_ID}
    duplicate = AdmittedControl(
        id=REQUEST_ID,
        request_seq=5,
        client_request_id=CLIENT_REQUEST_ID,
        verb="mode.set",
        state="applied",
        duplicate=True,
    )
    enforce = AsyncMock(side_effect=HTTPException(status_code=422, detail="revoked"))
    admit = AsyncMock(return_value=duplicate)
    with (
        patch.object(
            thread_session,
            "find_existing_thread_control",
            AsyncMock(return_value=duplicate),
        ),
        patch.object(thread_session, "admit_thread_control", admit),
        patch.object(thread_session, "log_security_event", AsyncMock()),
    ):
        response = await thread_session.submit_thread_control(
            str(THREAD_ID),
            ThreadControlRequest(
                client_request_id=CLIENT_REQUEST_ID,
                method="mode.set",
                mode="autonomous",
            ),
            MagicMock(),
            dependencies=_control_dependencies(
                owner=AsyncMock(return_value=({"id": OWNER_ID}, thread)),
                enforce=enforce,
            ),
        )

    assert response["state"] == "applied"
    assert response["duplicate"] is True
    enforce.assert_not_awaited()
    admit.assert_awaited_once()


@pytest.mark.asyncio
async def test_duplicate_client_request_replays_receipt_before_lifecycle_checks():
    existing = {
        "id": REQUEST_ID,
        "request_seq": 7,
        "client_request_id": CLIENT_REQUEST_ID,
        "verb": "narration.set",
        "payload": json.dumps({"mode": "verbose"}),
        "outcome": "applied",
    }
    # A committed request remains idempotently observable even if its session
    # ended after admission; do not re-run lane/fence/update/queue logic.
    conn = _ControlConn(
        thread=_thread(status="ended", execution_lane="future-lane"),
        existing=existing,
    )

    result = await admit_thread_control(
        _ControlDB(conn),
        thread_id=THREAD_ID,
        owner_user_id=OWNER_ID,
        client_request_id=CLIENT_REQUEST_ID,
        verb="narration.set",
        payload={"mode": "verbose"},
        requested_by="retrying-owner",
    )

    assert result == AdmittedControl(
        id=REQUEST_ID,
        request_seq=7,
        client_request_id=CLIENT_REQUEST_ID,
        verb="narration.set",
        state="applied",
        duplicate=True,
    )
    assert not [call for call in conn.calls if call[0] in {"execute", "fetchval"}]


@pytest.mark.asyncio
async def test_reused_client_request_id_with_different_payload_is_rejected():
    conn = _ControlConn(
        thread=_thread(),
        existing={
            "id": REQUEST_ID,
            "request_seq": 5,
            "client_request_id": CLIENT_REQUEST_ID,
            "verb": "mode.set",
            "payload": {"mode": "supervised"},
            "outcome": None,
        },
    )

    with pytest.raises(ControlAdmissionError, match="different control"):
        await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="mode.set",
            payload={"mode": "autonomous"},
            requested_by=str(OWNER_ID),
        )

    assert not [call for call in conn.calls if call[0] in {"execute", "fetchval"}]


@pytest.mark.asyncio
async def test_admission_rechecks_owner_under_thread_lock_before_idempotency_lookup():
    other_owner = UUID("77777777-aaaa-4444-8888-777777777777")
    conn = _ControlConn(
        thread=_thread(user_id=other_owner),
        # Even an otherwise matching request cannot be disclosed cross-owner.
        existing={
            "id": REQUEST_ID,
            "request_seq": 5,
            "client_request_id": CLIENT_REQUEST_ID,
            "verb": "mode.set",
            "payload": {"mode": "supervised"},
            "outcome": None,
        },
    )

    with pytest.raises(ControlAdmissionError, match="Thread is unavailable"):
        await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="mode.set",
            payload={"mode": "supervised"},
            requested_by=str(OWNER_ID),
        )

    assert not _calls(conn, "fetchrow", "FROM thread_control_requests")
    assert not [call for call in conn.calls if call[0] in {"execute", "fetchval"}]


@pytest.mark.asyncio
async def test_locked_stateless_workspace_refusal_precedes_every_admission_write():
    """The locked row, not the route's earlier snapshot, owns tier admission."""
    conn = _ControlConn(
        thread=_thread(
            metadata={
                "config_override": {"workspace": {"backend": "virtual"}},
                "workspace_container": {"status": "ready", "pod_ip": "10.0.0.9"},
            }
        ),
        queue_state="done",
    )

    with pytest.raises(ControlAdmissionError, match="workspace binding"):
        await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="narration.set",
            payload={"mode": "verbose"},
            requested_by=str(OWNER_ID),
        )

    locked_read = _calls(conn, "fetchrow", "FROM threads WHERE id")
    assert len(locked_read) == 1
    assert "metadata" in locked_read[0][1]
    assert "FOR UPDATE" in locked_read[0][1]
    assert not _calls(conn, "fetchrow", "FROM run_queue")
    assert not [call for call in conn.calls if call[0] in {"execute", "fetchval"}]


@pytest.mark.asyncio
async def test_locked_stateless_protected_cloud_refusal_precedes_every_write():
    """The protected marker is authoritative at the locked write boundary."""
    conn = _ControlConn(
        thread=_thread(
            metadata={
                "config_override": {"workspace": {"backend": "virtual"}},
                "protected_cloud": True,
            }
        ),
        queue_state="done",
    )

    with pytest.raises(ControlAdmissionError, match="workspace binding"):
        await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="narration.set",
            payload={"mode": "verbose"},
            requested_by=str(OWNER_ID),
        )

    locked_read = _calls(conn, "fetchrow", "FROM threads WHERE id")
    assert len(locked_read) == 1
    assert "metadata" in locked_read[0][1]
    assert "FOR UPDATE" in locked_read[0][1]
    assert not _calls(conn, "fetchrow", "FROM run_queue")
    assert not [call for call in conn.calls if call[0] in {"execute", "fetchval"}]


@pytest.mark.asyncio
@pytest.mark.parametrize("class_flag", ["enabled", "conference"])
async def test_locked_stateless_session_class_refusal_precedes_every_write(class_flag):
    """The locked materialized class bits, not route preflight, are authority."""

    conn = _ControlConn(
        thread=_thread(
            metadata={
                "config_override": {
                    "workspace": {"backend": "virtual"},
                    "officer": {class_flag: True},
                }
            }
        ),
        queue_state="done",
    )

    with pytest.raises(ControlAdmissionError, match="workspace binding"):
        await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="narration.set",
            payload={"mode": "verbose"},
            requested_by=str(OWNER_ID),
        )

    assert not _calls(conn, "fetchrow", "FROM run_queue")
    assert not [call for call in conn.calls if call[0] in {"execute", "fetchval"}]


@pytest.mark.asyncio
@pytest.mark.parametrize("class_flag", ["enabled", "conference"])
@pytest.mark.parametrize("value", [None, 0, "", [], {}, "false", 1])
async def test_locked_stateless_malformed_session_class_refuses_before_every_write(
    class_flag, value
):
    """The locked admission gate must match central malformed-class refusal."""

    conn = _ControlConn(
        thread=_thread(
            metadata={
                "config_override": {
                    "workspace": {"backend": "virtual"},
                    "officer": {class_flag: value},
                }
            }
        ),
        queue_state="done",
    )

    with pytest.raises(ControlAdmissionError, match="session_class_malformed"):
        await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="narration.set",
            payload={"mode": "verbose"},
            requested_by=str(OWNER_ID),
        )

    assert not _calls(conn, "fetchrow", "FROM run_queue")
    assert not [call for call in conn.calls if call[0] in {"execute", "fetchval"}]


@pytest.mark.asyncio
async def test_pinned_admission_captures_exact_reciprocal_agent_fence():
    conn = _ControlConn(
        thread=_thread(
            execution_lane="pinned",
            agent_id=AGENT_ID,
            control_admission_agent_id=AGENT_ID,
        ),
        reciprocal=1,
    )

    result = await admit_thread_control(
        _ControlDB(conn),
        thread_id=THREAD_ID,
        owner_user_id=OWNER_ID,
        client_request_id=CLIENT_REQUEST_ID,
        verb="mode.set",
        payload={"mode": "auto_accept"},
        requested_by=str(OWNER_ID),
    )

    assert result.request_seq == 5
    assert result.state == "pending"
    reciprocal = _calls(conn, "fetchval", "FROM agents")
    assert len(reciprocal) == 1
    assert "id = $1 AND thread_id = $2 FOR SHARE" in reciprocal[0][1]
    assert reciprocal[0][2] == (AGENT_ID, THREAD_ID)

    updates = _calls(conn, "execute", "control_seq_hwm")
    assert len(updates) == 1
    assert updates[0][2] == (THREAD_ID, 5, RUNTIME_GENERATION)
    assert "permission_mode" not in updates[0][1]

    inserts = _calls(conn, "fetchval", "INSERT INTO thread_control_requests")
    assert len(inserts) == 1
    assert inserts[0][2] == (
        THREAD_ID,
        5,
        CLIENT_REQUEST_ID,
        "mode.set",
        '{"mode":"auto_accept"}',
        str(OWNER_ID),
        AGENT_ID,
        RUNTIME_GENERATION,
    )
    assert not _calls(conn, "fetchval", "control_input_seq = GREATEST")


@pytest.mark.asyncio
async def test_ownerless_pinned_admission_rechecks_null_owner_under_lock():
    conn = _ControlConn(
        thread=_thread(
            user_id=None,
            execution_lane="pinned",
            agent_id=AGENT_ID,
            control_admission_agent_id=AGENT_ID,
        ),
        reciprocal=1,
    )

    result = await admit_thread_control(
        _ControlDB(conn),
        thread_id=THREAD_ID,
        owner_user_id=None,
        client_request_id=CLIENT_REQUEST_ID,
        verb="narration.set",
        payload={"mode": "auto"},
        requested_by="admin",
    )

    assert result.state == "pending"
    assert _calls(conn, "fetchval", "INSERT INTO thread_control_requests")


@pytest.mark.asyncio
async def test_pinned_admission_rejects_nonreciprocal_binding_before_any_write():
    conn = _ControlConn(
        thread=_thread(
            execution_lane="pinned",
            agent_id=AGENT_ID,
            control_admission_agent_id=AGENT_ID,
        ),
        reciprocal=None,
    )

    with pytest.raises(ControlAdmissionNotReady, match="not ready"):
        await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="narration.set",
            payload={"mode": "auto"},
            requested_by=str(OWNER_ID),
        )

    assert not [call for call in conn.calls if call[0] == "execute"]
    assert not _calls(conn, "fetchval", "INSERT INTO thread_control_requests")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "capability", [None, UUID("99999999-9999-4999-8999-999999999999")]
)
async def test_pinned_admission_requires_exact_capable_owner_generation(capability):
    """A closed gate or a credential left by a prior owner cannot transfer."""

    conn = _ControlConn(
        thread=_thread(
            execution_lane="pinned",
            agent_id=AGENT_ID,
            control_admission_agent_id=capability,
        ),
        reciprocal=1,
    )

    with pytest.raises(ControlAdmissionNotReady, match="not ready"):
        await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="mode.set",
            payload={"mode": "supervised"},
            requested_by=str(OWNER_ID),
        )

    assert not _calls(conn, "fetchval", "FROM agents")
    assert not [call for call in conn.calls if call[0] == "execute"]
    assert not _calls(conn, "fetchval", "INSERT INTO thread_control_requests")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("queue_state", "recorded_state"),
    [("done", "queued"), ("leased", "leased")],
    ids=["done-row-wakes", "leased-row-keeps-lease"],
)
async def test_stateless_admission_advances_control_watermark_without_human_input(
    queue_state, recorded_state
):
    conn = _ControlConn(
        thread=_thread(control_seq_hwm=10),
        queue_state=queue_state,
        baseline_input_seq=73,
        recorded_queue_state=recorded_state,
    )

    result = await admit_thread_control(
        _ControlDB(conn),
        thread_id=THREAD_ID,
        owner_user_id=OWNER_ID,
        client_request_id=CLIENT_REQUEST_ID,
        verb="narration.set",
        payload={"mode": "silent"},
        requested_by=str(OWNER_ID),
    )

    assert result == AdmittedControl(
        id=REQUEST_ID,
        request_seq=11,
        client_request_id=CLIENT_REQUEST_ID,
        verb="narration.set",
        state="pending",
        duplicate=False,
        runtime_generation=RUNTIME_GENERATION,
    )
    updates = _calls(conn, "execute", "control_seq_hwm")
    assert len(updates) == 1
    assert updates[0][2] == (THREAD_ID, 11, RUNTIME_GENERATION)
    assert "narration_mode" not in updates[0][1]

    inserts = _calls(conn, "fetchval", "INSERT INTO thread_control_requests")
    assert inserts[0][2][-2] is None  # Stateless admission has no agent credential.
    assert inserts[0][2][-1] == RUNTIME_GENERATION

    watermark = _calls(conn, "fetchval", "control_input_seq = GREATEST")
    assert len(watermark) == 1
    assert watermark[0][2] == (
        THREAD_ID,
        "session_turn",
        11,
        str(OWNER_ID),
        73,
    )
    insert_index = conn.calls.index(inserts[0])
    watermark_index = conn.calls.index(watermark[0])
    assert insert_index < watermark_index


@pytest.mark.asyncio
async def test_stateless_parked_queue_refuses_control_before_persisting_it():
    conn = _ControlConn(thread=_thread(), queue_state="parked")

    with pytest.raises(ControlAdmissionError, match="queue is parked"):
        await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="mode.set",
            payload={"mode": "supervised"},
            requested_by=str(OWNER_ID),
        )

    assert not [call for call in conn.calls if call[0] == "execute"]
    assert not _calls(conn, "fetchval", "INSERT INTO thread_control_requests")
    assert not _calls(conn, "fetchval", "control_input_seq = GREATEST")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("queue_kind", "queue_state", "message"),
    [
        ("worker_batch", "done", "identity is incompatible"),
        ("session_turn", "future-state", "state is unavailable"),
    ],
)
async def test_stateless_admission_rejects_incompatible_queue_rows(
    queue_kind, queue_state, message
):
    conn = _ControlConn(
        thread=_thread(), queue_kind=queue_kind, queue_state=queue_state
    )

    with pytest.raises(ControlAdmissionError, match=message):
        await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="mode.set",
            payload={"mode": "supervised"},
            requested_by=str(OWNER_ID),
        )

    assert not [call for call in conn.calls if call[0] == "execute"]
    assert not _calls(conn, "fetchval", "INSERT INTO thread_control_requests")


@pytest.mark.asyncio
async def test_stateless_admission_refuses_pending_pinned_generation():
    conn = _ControlConn(thread=_thread(), queue_state="done", stranded_pinned=1)

    with pytest.raises(ControlAdmissionError, match="awaiting its pinned owner"):
        await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="narration.set",
            payload={"mode": "silent"},
            requested_by=str(OWNER_ID),
        )

    assert not [call for call in conn.calls if call[0] == "execute"]


@pytest.mark.asyncio
async def test_workspace_undo_admits_only_idle_stateless_sandbox():
    conn = _ControlConn(
        thread=_thread(
            metadata={"config_override": {"workspace": {"backend": "sandbox"}}}
        ),
        queue_state="done",
        baseline_input_seq=21,
    )

    with patch(
        "orchestrator.services.thread_control_inbox.stateless_session_workspace_check",
        return_value=("sandbox", None),
    ):
        result = await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="workspace.undo",
            payload={},
            requested_by=str(OWNER_ID),
        )

    assert result.verb == "workspace.undo"
    inserts = _calls(conn, "fetchval", "INSERT INTO thread_control_requests")
    assert len(inserts) == 1
    assert inserts[0][2][3:5] == ("workspace.undo", "{}")
    assert _calls(conn, "fetchval", "control_input_seq = GREATEST")


@pytest.mark.asyncio
async def test_workspace_undo_refuses_live_turn_before_any_write():
    conn = _ControlConn(
        thread=_thread(
            metadata={"config_override": {"workspace": {"backend": "sandbox"}}}
        ),
        queue_state="leased",
    )

    with (
        patch(
            "orchestrator.services.thread_control_inbox.stateless_session_workspace_check",
            return_value=("sandbox", None),
        ),
        pytest.raises(ControlAdmissionNotReady, match="completing a turn"),
    ):
        await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="workspace.undo",
            payload={},
            requested_by=str(OWNER_ID),
        )

    assert not [call for call in conn.calls if call[0] == "execute"]
    assert not _calls(conn, "fetchval", "INSERT INTO thread_control_requests")


@pytest.mark.asyncio
async def test_workspace_undo_cannot_overtake_earlier_human_input():
    conn = _ControlConn(
        thread=_thread(
            metadata={"config_override": {"workspace": {"backend": "sandbox"}}}
        ),
        queue_state="queued",
        input_seq=22,
        consumed_seq=21,
    )

    with (
        patch(
            "orchestrator.services.thread_control_inbox.stateless_session_workspace_check",
            return_value=("sandbox", None),
        ),
        pytest.raises(ControlAdmissionNotReady, match="pending input"),
    ):
        await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="workspace.undo",
            payload={},
            requested_by=str(OWNER_ID),
        )

    assert not [call for call in conn.calls if call[0] == "execute"]
    assert not _calls(conn, "fetchval", "INSERT INTO thread_control_requests")


@pytest.mark.asyncio
async def test_workspace_undo_allows_queued_control_only_row():
    conn = _ControlConn(
        thread=_thread(
            metadata={"config_override": {"workspace": {"backend": "sandbox"}}}
        ),
        queue_state="queued",
        input_seq=21,
        consumed_seq=21,
        baseline_input_seq=21,
    )

    with patch(
        "orchestrator.services.thread_control_inbox.stateless_session_workspace_check",
        return_value=("sandbox", None),
    ):
        result = await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="workspace.undo",
            payload={},
            requested_by=str(OWNER_ID),
        )

    assert result.verb == "workspace.undo"
    assert _calls(conn, "fetchval", "INSERT INTO thread_control_requests")


@pytest.mark.asyncio
async def test_workspace_undo_fails_closed_for_virtual_and_pinned_lanes():
    virtual = _ControlConn(thread=_thread(), queue_state="done")
    with pytest.raises(ControlAdmissionError, match="only for sandbox"):
        await admit_thread_control(
            _ControlDB(virtual),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="workspace.undo",
            payload={},
            requested_by=str(OWNER_ID),
        )

    pinned = _ControlConn(
        thread=_thread(execution_lane="pinned", agent_id=AGENT_ID),
        reciprocal=1,
    )
    with pytest.raises(ControlAdmissionError, match="live session transport"):
        await admit_thread_control(
            _ControlDB(pinned),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="workspace.undo",
            payload={},
            requested_by=str(OWNER_ID),
        )

    assert not [call for call in virtual.calls if call[0] == "execute"]
    assert not [call for call in pinned.calls if call[0] == "execute"]


@pytest.mark.asyncio
async def test_workspace_undo_payload_is_rechecked_inside_locked_admission():
    conn = _ControlConn(thread=_thread(), queue_state="done")

    with pytest.raises(ControlAdmissionError, match="does not accept"):
        await admit_thread_control(
            _ControlDB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=CLIENT_REQUEST_ID,
            verb="workspace.undo",
            payload={"mode": "auto"},
            requested_by=str(OWNER_ID),
        )

    assert not _calls(conn, "fetchrow", "FROM run_queue")
    assert not [call for call in conn.calls if call[0] == "execute"]
