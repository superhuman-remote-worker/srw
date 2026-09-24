"""Linearization tests for lifecycle ownership versus completion admission."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import time
from unittest.mock import AsyncMock

import pytest

from orchestrator.services.completion_lifecycle import CompletionLifecycleOwnership
from orchestrator.services.job_completion_commands import (
    CompletionControlInProgress,
    accept_completion_command,
)


JOB_ID = "11111111-2222-3333-4444-555555555555"
AGENT_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
REPORT_ID = "99999999-8888-4777-8666-555555555555"
COMMAND_ID = "12345678-1234-4678-9abc-123456789abc"


class _State:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.job_exists = True
        self.status = "completed"
        self.lane = "pinned"
        self.context: dict = {}
        self.route: dict | None = None
        self.claim_active = False
        self.claim_id: str | None = None
        self.renew_results: list[bool] = []
        self.sql: list[str] = []


class _Transaction:
    def __init__(self, state: _State) -> None:
        self.state = state

    async def __aenter__(self):
        await self.state.lock.acquire()

    async def __aexit__(self, *_exc):
        self.state.lock.release()


class _Connection:
    def __init__(self, state: _State) -> None:
        self.state = state

    def transaction(self) -> _Transaction:
        return _Transaction(self.state)

    async def fetchrow(self, sql: str, *args):
        compact = " ".join(sql.split())
        self.state.sql.append(compact)
        if "FROM run_queue" in compact:
            return None
        if "completion_seq_hwm" in compact and "FROM jobs" in compact:
            if not self.state.job_exists:
                return None
            return {
                "id": JOB_ID,
                "status": self.state.status,
                "execution_lane": self.state.lane,
                "assigned_agent_id": AGENT_ID,
                "completion_seq_hwm": 0,
                "context": self.state.context,
                "db_now_epoch": time.time(),
            }
        if "AS control_active" in compact and "FROM jobs" in compact:
            if not self.state.job_exists:
                return None
            return {
                "status": self.state.status,
                "execution_lane": self.state.lane,
                "context": self.state.context,
                "control_active": self.state.claim_active,
            }
        if compact.startswith(
            "SELECT command_id, route FROM job_completion_sweep_exclusions"
        ):
            return dict(self.state.route) if self.state.route else None
        if "FROM job_completion_commands" in compact and "client_report_id" in compact:
            return None
        if "FROM completion_effects AS effect" in compact:
            return None
        if "INSERT INTO job_completion_commands" in compact:
            self.state.route = {
                "command_id": COMMAND_ID,
                "route": "stand_down",
            }
            return {
                "id": COMMAND_ID,
                "job_id": JOB_ID,
                "report_seq": 1,
                "client_report_id": REPORT_ID,
                "payload": {},
                "payload_digest": args[4],
                "accepted_lease_token": None,
                "accepted_agent_id": AGENT_ID,
                "accepted_job_status": self.state.status,
                "state": "pending",
                "outcome": None,
            }
        if "'fence_kind'" in compact and "UPDATE jobs" in compact:
            if self.state.claim_active or self.state.route is not None:
                return None
            self.state.claim_id = str(args[2])
            self.state.claim_active = True
            self.state.context = {
                "_completion_control_claim": {
                    "version": 1,
                    "claim_id": self.state.claim_id,
                    "expires_epoch": time.time() + 3600,
                }
            }
            return {"context": self.state.context}
        if "- '_completion_control_claim'" in compact:
            if not self.state.claim_active or str(args[1]) != self.state.claim_id:
                return None
            self.state.claim_active = False
            self.state.context = {}
            return {"id": JOB_ID}
        if "expires_epoch" in compact and "UPDATE jobs" in compact:
            allowed = (
                self.state.renew_results.pop(0)
                if self.state.renew_results
                else self.state.claim_active
            )
            return {"id": JOB_ID} if allowed else None
        raise AssertionError(f"unexpected SQL: {compact}")

    async def fetchval(self, sql: str, *_args):
        compact = " ".join(sql.split())
        self.state.sql.append(compact)
        if "FROM vm_idle_operations" in compact and "closed_at IS NULL" in compact:
            # No open pinned idle operation owns this Job's report authority.
            return False
        raise AssertionError(f"unexpected SQL: {compact}")

    async def execute(self, sql: str, *_args):
        self.state.sql.append(" ".join(sql.split()))
        return "UPDATE 1"


class _DB:
    def __init__(self, state: _State) -> None:
        self.state = state
        self.acquire_count = 0

    @asynccontextmanager
    async def acquire(self):
        self.acquire_count += 1
        yield _Connection(self.state)


def _ownership(
    state: _State,
    router: AsyncMock,
    *,
    heartbeat: float = 60,
) -> CompletionLifecycleOwnership:
    return CompletionLifecycleOwnership(
        _DB(state),
        router,
        lease_seconds=120,
        heartbeat_seconds=heartbeat,
    )


def _action(service: CompletionLifecycleOwnership):
    return service.action(
        JOB_ID,
        source="test_reap",
        resource_kind="workspace",
        resource_identity="pod-uid-1",
        expected_status="completed",
        expected_lane="pinned",
    )


@pytest.mark.asyncio
async def test_lifecycle_wins_jobs_lock_then_fresh_accept_refuses_marker() -> None:
    state = _State()
    router = AsyncMock()
    service = _ownership(state, router)

    async with _action(service) as permit:
        assert permit.local
        with pytest.raises(CompletionControlInProgress):
            await accept_completion_command(
                _DB(state),
                job_id=JOB_ID,
                payload={},
                lease_token=None,
                agent_id=AGENT_ID,
                client_report_id=REPORT_ID,
                requested_by="agent",
            )
        permit.complete()

    assert not state.claim_active
    assert "FROM run_queue" in state.sql[0]
    assert "FROM jobs" in state.sql[1]


@pytest.mark.asyncio
async def test_accept_wins_jobs_lock_then_lifecycle_routes_without_local_io() -> None:
    state = _State()
    router = AsyncMock()
    await accept_completion_command(
        _DB(state),
        job_id=JOB_ID,
        payload={},
        lease_token=None,
        agent_id=AGENT_ID,
        client_report_id=REPORT_ID,
        requested_by="agent",
    )
    service = _ownership(state, router)
    local_io = AsyncMock()

    async with _action(service) as permit:
        assert not permit.local
        if permit.local:
            await local_io()

    local_io.assert_not_awaited()
    router.enqueue_job.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("route", "enqueued"),
    [
        ("stand_down", False),
        ("resume_finalizer", True),
        ("park_alert", True),
        ("alert_only", True),
    ],
)
async def test_command_route_never_runs_parallel_lifecycle_io(
    route: str,
    enqueued: bool,
) -> None:
    state = _State()
    state.route = {"command_id": COMMAND_ID, "route": route}
    router = AsyncMock()
    service = _ownership(state, router)
    local_io = AsyncMock()

    async with _action(service) as permit:
        assert not permit.local
        if permit.local:
            await local_io()

    local_io.assert_not_awaited()
    if enqueued:
        router.enqueue_job.assert_awaited_once_with(JOB_ID, source="test_reap")
    else:
        router.enqueue_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_one_claim_spans_snapshot_then_delete_and_exact_clears() -> None:
    state = _State()
    service = _ownership(state, AsyncMock())
    observed: list[tuple[str, bool]] = []

    async with _action(service) as permit:
        observed.append(("snapshot", state.claim_active))
        assert await service.refresh(permit)
        observed.append(("delete", state.claim_active))
        permit.complete()

    assert observed == [("snapshot", True), ("delete", True)]
    assert not state.claim_active


@pytest.mark.asyncio
async def test_heartbeat_loss_fences_follow_on_and_retains_marker() -> None:
    state = _State()
    state.renew_results = [False]
    service = _ownership(state, AsyncMock(), heartbeat=0.01)

    async with _action(service) as permit:
        await asyncio.sleep(0.03)
        assert not permit.local
        assert not await service.refresh(permit)
        permit.complete()

    assert state.claim_active


@pytest.mark.asyncio
async def test_crash_or_ambiguous_result_retains_bounded_marker() -> None:
    state = _State()
    service = _ownership(state, AsyncMock())

    with pytest.raises(RuntimeError, match="response lost"):
        async with _action(service) as permit:
            assert permit.local
            raise RuntimeError("response lost")

    assert state.claim_active


@pytest.mark.asyncio
async def test_expired_old_owner_cannot_follow_external_io_after_accept_wins() -> None:
    """Model SIGSTOP across lease expiry and resumption after a new accept."""

    state = _State()
    service = _ownership(state, AsyncMock())
    follow_on = AsyncMock()

    async with _action(service) as permit:
        assert await service.refresh(permit)
        # The exact-identity external call was admitted while this term lived.
        # The process is then stopped until its DB-clock lease is expired.
        state.context["_completion_control_claim"]["expires_epoch"] = time.time() - 1
        state.claim_active = False
        await accept_completion_command(
            _DB(state),
            job_id=JOB_ID,
            payload={},
            lease_token=None,
            agent_id=AGENT_ID,
            client_report_id=REPORT_ID,
            requested_by="agent",
        )

        # The old await may return, but its mandatory post-I/O refresh loses.
        assert not await service.refresh(permit)
        if permit.local:
            await follow_on()
        permit.complete()

    follow_on.assert_not_awaited()
    assert state.route == {"command_id": COMMAND_ID, "route": "stand_down"}
    assert not any("- '_completion_control_claim'" in sql for sql in state.sql)


@pytest.mark.asyncio
async def test_missing_job_is_legacy_orphan_without_marker() -> None:
    state = _State()
    state.job_exists = False
    service = _ownership(state, AsyncMock())

    async with _action(service) as permit:
        assert permit.local
        assert permit.decision.disposition == "missing_job"
        permit.complete()

    assert not state.claim_active


@pytest.mark.asyncio
async def test_reconciler_off_capability_never_calls_new_action_or_mutates_metadata() -> (
    None
):
    from orchestrator.services.lifecycle.reconciler import InstanceLifecycleReconciler

    class _OffManager:
        completion_lifecycle_ownership_enabled = False
        lifecycle_action = AsyncMock(side_effect=AssertionError("must stay dark"))

    instance = type("I", (), {"metadata": {"legacy": True}})()
    before = dict(instance.metadata)
    async with InstanceLifecycleReconciler._lifecycle_action(
        _OffManager(), instance, source="reap"
    ) as permit:
        assert permit.local

    assert instance.metadata == before
    _OffManager.lifecycle_action.assert_not_called()
