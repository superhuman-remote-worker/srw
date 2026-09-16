"""Focused worker composition tests over the frozen run_queue contract."""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

import shared.worker_queue as worker_queue
from shared.run_queue import ClaimedUnit, EnqueueResult


class _Connection:
    def __init__(self) -> None:
        self.fetchrow = AsyncMock()
        self.fetchval = AsyncMock()
        self.execute = AsyncMock()

    @asynccontextmanager
    async def transaction(self):
        yield


def _unit(*, input_seq=None, token=4) -> ClaimedUnit:
    return ClaimedUnit(
        unit_id=uuid4(),
        unit_kind="worker_batch",
        fair_key="tenant-a",
        lease_token=token,
        input_seq=input_seq,
        consumed_seq=None,
        attempts_since_completion=1,
        leased_until=datetime.now(timezone.utc),
    )


def test_control_marker_nan_expiry_fails_closed():
    assert worker_queue._completion_control_claim_active(
        {
            "_completion_control_claim": {
                "version": 1,
                "expires_epoch": float("nan"),
            }
        }
    )
    assert worker_queue._completion_control_claim_active(
        {
            "_completion_control_claim": {
                "version": 1,
                "expires_epoch": 10**10000,
            }
        }
    )


@pytest.mark.asyncio
async def test_claim_cas_accepts_processing_reclaim(monkeypatch):
    conn = _Connection()
    unit = _unit()
    conn.fetchrow.side_effect = [
        {
            "status": "processing",
            "execution_lane": "stateless",
            "assigned_agent_id": None,
            "context": {"worker_resume_id": "resume-generation-7"},
        },
        {"id": unit.unit_id},
    ]
    monkeypatch.setattr(
        worker_queue,
        "claim_unit",
        AsyncMock(return_value=unit),
    )

    claim = await worker_queue.claim_worker_batch(conn, pod_name="pod-b")

    assert claim is not None
    assert claim.prior_job_status == "processing"
    assert claim.resume is True
    assert claim.resume_id == "resume-generation-7"
    cas_sql = conn.fetchrow.await_args_list[1].args[0]
    assert "status = 'processing'" in cas_sql
    assert "assigned_agent_id IS NULL" in cas_sql
    assert "_workspace_dispatch_authority" in cas_sql
    assert "'dispatch_kind', 'stateless'" in cas_sql
    assert conn.fetchrow.await_args_list[1].args[3:] == (
        "pod-b",
        unit.lease_token,
        unit.leased_until,
    )


@pytest.mark.asyncio
async def test_command_aware_claim_skips_ineligible_head_in_composed_query(monkeypatch):
    conn = _Connection()
    unit = _unit()
    conn.fetchrow.side_effect = [
        unit.__dict__
        if hasattr(unit, "__dict__")
        else {
            "unit_id": unit.unit_id,
            "unit_kind": unit.unit_kind,
            "fair_key": unit.fair_key,
            "lease_token": unit.lease_token,
            "input_seq": unit.input_seq,
            "consumed_seq": unit.consumed_seq,
            "control_input_seq": unit.control_input_seq,
            "control_consumed_seq": unit.control_consumed_seq,
            "attempts_since_completion": unit.attempts_since_completion,
            "leased_until": unit.leased_until,
        },
        {
            "status": "processing",
            "execution_lane": "stateless",
            "assigned_agent_id": None,
            "freeze_data": None,
            "context": {},
        },
        {"id": unit.unit_id},
    ]
    conn.fetchval.return_value = False
    legacy_claim = AsyncMock()
    monkeypatch.setattr(worker_queue, "claim_unit", legacy_claim)

    claim = await worker_queue.claim_worker_batch(
        conn,
        pod_name="pod-b",
        completion_commands_enabled=True,
    )

    assert claim is not None
    legacy_claim.assert_not_awaited()
    candidate_sql = conn.fetchrow.await_args_list[0].args[0]
    assert "job_completion_sweep_exclusions" in candidate_sql
    assert "_completion_control_claim" in candidate_sql
    assert "expires_epoch" in candidate_sql
    assert "ORDER BY queue.priority DESC, queue.queued_at" in candidate_sql
    assert "FOR UPDATE OF queue SKIP LOCKED" in candidate_sql
    recheck_sql = conn.fetchval.await_args.args[0]
    assert "job_completion_sweep_exclusions" in recheck_sql


@pytest.mark.asyncio
async def test_terminal_claim_reconciliation_consumes_current_watermark(monkeypatch):
    conn = _Connection()
    unit = _unit(input_seq=23)
    conn.fetchrow.return_value = {
        "status": "completed",
        "execution_lane": "stateless",
        "assigned_agent_id": None,
    }
    complete = AsyncMock(return_value="done")
    monkeypatch.setattr(worker_queue, "claim_unit", AsyncMock(return_value=unit))
    monkeypatch.setattr(worker_queue, "complete_unit", complete)

    assert await worker_queue.claim_worker_batch(conn, pod_name="pod-b") is None

    complete.assert_awaited_once_with(
        conn,
        unit_id=unit.unit_id,
        lease_token=unit.lease_token,
        consumed_seq=23,
    )
    # Terminal status is authoritative: no jobs-row processing CAS and no
    # graph/runtime invocation can occur after reconciliation returns None.
    assert conn.fetchrow.await_count == 1


@pytest.mark.asyncio
async def test_frozen_human_pause_is_consumed_not_changed_back_to_processing(
    monkeypatch,
):
    conn = _Connection()
    unit = _unit(input_seq=24)
    conn.fetchrow.return_value = {
        "status": "paused",
        "execution_lane": "stateless",
        "assigned_agent_id": None,
        "freeze_data": {"freeze_type": "blocking_message"},
        "context": {},
    }
    complete = AsyncMock(return_value="done")
    monkeypatch.setattr(worker_queue, "claim_unit", AsyncMock(return_value=unit))
    monkeypatch.setattr(worker_queue, "complete_unit", complete)

    assert await worker_queue.claim_worker_batch(conn, pod_name="pod-b") is None

    complete.assert_awaited_once_with(
        conn,
        unit_id=unit.unit_id,
        lease_token=unit.lease_token,
        consumed_seq=24,
    )
    assert conn.fetchrow.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "vm_marker",
    [
        {"context": {"vm": {"requested": True}}},
        {"config_override": {"workspace": {"backend": "vm"}}},
        {"config_override": {"workspace": {"backend": "remote"}}},
    ],
)
async def test_worker_claim_rejects_vm_marker_before_processing_cas(
    monkeypatch,
    vm_marker,
):
    conn = _Connection()
    unit = _unit(input_seq=25)
    conn.fetchrow.return_value = {
        "status": "paused",
        "execution_lane": "stateless",
        "assigned_agent_id": None,
        "freeze_data": None,
        "context": {},
        "config_override": {},
        **vm_marker,
    }
    complete = AsyncMock(return_value="done")
    monkeypatch.setattr(worker_queue, "claim_unit", AsyncMock(return_value=unit))
    monkeypatch.setattr(worker_queue, "complete_unit", complete)

    assert await worker_queue.claim_worker_batch(conn, pod_name="pod-b") is None

    complete.assert_awaited_once_with(
        conn,
        unit_id=unit.unit_id,
        lease_token=unit.lease_token,
        consumed_seq=25,
    )
    assert conn.fetchrow.await_count == 1


@pytest.mark.asyncio
async def test_worker_claim_heals_legacy_processing_vm_for_dispatcher_repair(
    monkeypatch,
):
    conn = _Connection()
    unit = _unit(input_seq=26)
    conn.fetchrow.side_effect = [
        {
            "status": "processing",
            "execution_lane": "stateless",
            "assigned_agent_id": None,
            "freeze_data": None,
            "context": {"vm": {"requested": True}},
            "config_override": {},
        },
        {"id": unit.unit_id},
    ]
    complete = AsyncMock(return_value="done")
    monkeypatch.setattr(worker_queue, "claim_unit", AsyncMock(return_value=unit))
    monkeypatch.setattr(worker_queue, "complete_unit", complete)

    assert await worker_queue.claim_worker_batch(conn, pod_name="pod-b") is None

    heal_sql = conn.fetchrow.await_args_list[1].args[0]
    assert "status = 'paused'" in heal_sql
    assert "status = 'processing'" in heal_sql
    complete.assert_awaited_once_with(
        conn,
        unit_id=unit.unit_id,
        lease_token=unit.lease_token,
        consumed_seq=26,
    )


@pytest.mark.asyncio
async def test_worker_complete_entry_fence_is_token_only():
    conn = _Connection()
    conn.fetchval.return_value = True
    job_id = uuid4()

    assert await worker_queue.worker_lease_is_current(
        conn,
        job_id=job_id,
        lease_token=19,
    )

    sql = conn.fetchval.await_args.args[0]
    assert "lease_token = $2::bigint" in sql
    assert "unit_kind = 'worker_batch'" in sql
    assert "state = 'leased'" not in sql


@pytest.mark.asyncio
async def test_worker_resume_wake_locks_and_advances_current_input(monkeypatch):
    conn = _Connection()
    conn.fetchrow.return_value = {
        "unit_kind": "worker_batch",
        "input_seq": 18,
    }
    enqueue = AsyncMock(return_value=EnqueueResult("input_recorded", "leased"))
    monkeypatch.setattr(worker_queue, "enqueue_worker_batch", enqueue)
    job_id = uuid4()

    result = await worker_queue.enqueue_worker_batch_wake(
        conn,
        job_id=job_id,
        fair_key="tenant-a",
    )

    assert result.state == "leased"
    lock_sql, locked_id = conn.fetchrow.await_args.args
    assert "FOR UPDATE" in lock_sql
    assert locked_id == job_id
    enqueue.assert_awaited_once_with(
        conn,
        job_id=job_id,
        fair_key="tenant-a",
        priority=0,
        run_after=None,
        input_seq=19,
    )


@pytest.mark.asyncio
async def test_workspace_preflight_creates_then_closes_absent_worker_row(monkeypatch):
    conn = _Connection()
    conn.fetchrow.side_effect = [None, {"state": "done", "lease_token": 0}]
    enqueue = AsyncMock(return_value=EnqueueResult("inserted", "queued"))
    monkeypatch.setattr(worker_queue, "enqueue_worker_batch", enqueue)
    job_id = uuid4()

    await worker_queue.hold_worker_batch_for_preflight(conn, job_id=job_id)

    enqueue.assert_awaited_once_with(conn, job_id=job_id)
    close_sql = conn.fetchrow.await_args_list[1].args[0]
    assert "state = 'done'" in close_sql
    assert "attempts_since_completion = 0" in close_sql


@pytest.mark.asyncio
async def test_workspace_preflight_fences_and_closes_exact_live_holder(
    monkeypatch,
):
    conn = _Connection()
    conn.fetchrow.side_effect = [
        {"unit_kind": "worker_batch", "state": "leased", "input_seq": 4},
        {"state": "done", "lease_token": 5},
    ]
    enqueue = AsyncMock()
    monkeypatch.setattr(worker_queue, "enqueue_worker_batch", enqueue)

    await worker_queue.hold_worker_batch_for_preflight(conn, job_id=uuid4())

    enqueue.assert_not_awaited()
    close_sql = conn.fetchrow.await_args_list[1].args[0]
    assert "lease_token + CASE WHEN state = 'leased' THEN 1" in close_sql
    assert "leased_by = NULL" in close_sql


@pytest.mark.asyncio
async def test_workspace_preflight_resets_parked_attempts_before_closing(monkeypatch):
    conn = _Connection()
    conn.fetchrow.side_effect = [
        {"unit_kind": "worker_batch", "state": "parked"},
        {"state": "done", "lease_token": 4},
    ]
    job_id = uuid4()

    await worker_queue.hold_worker_batch_for_preflight(conn, job_id=job_id)

    close_sql = conn.fetchrow.await_args_list[1].args[0]
    assert "attempts_since_completion = 0" in close_sql
    assert "state = 'done'" in close_sql


@pytest.mark.asyncio
async def test_explicit_resume_attempt_reset_preserves_queue_state_and_token():
    conn = _Connection()
    conn.fetchrow.return_value = {"state": "leased"}
    job_id = uuid4()

    state = await worker_queue.reset_worker_batch_attempts(conn, job_id=job_id)

    assert state == "leased"
    sql, bound_id = conn.fetchrow.await_args.args
    assert "attempts_since_completion = 0" in sql
    assert "lease_token" not in sql.split("RETURNING")[0]
    assert "state =" not in sql
    assert bound_id == job_id


@pytest.mark.parametrize(
    ("status", "preempted"),
    [
        ("processing", False),
        ("reviewing", False),
        ("pending_review", False),
        ("completed", False),
        ("waiting", False),
        ("failed", True),
        ("cancelled", True),
        ("paused", True),
    ],
)
def test_worker_renewal_uses_the_existing_control_status_deny_list(
    status,
    preempted,
):
    renewal = worker_queue.WorkerRenewal(
        leased_until=datetime.now(timezone.utc),
        job_status=status,
        job_context={},
        pending_guidance=(),
        queued_replies=(),
    )

    assert renewal.preempted is preempted


@pytest.mark.asyncio
async def test_twenty_rotations_advance_watermark_and_reset_attempts(monkeypatch):
    """The prescribed leased-enqueue → complete(old watermark) recipe."""

    conn = _Connection()
    row = {
        "state": "leased",
        "input_seq": None,
        "attempts": 1,
    }
    conn.fetchrow.side_effect = lambda *_args: {"input_seq": row["input_seq"]}
    calls = []

    async def enqueue_unit(
        _conn,
        *,
        unit_id,
        unit_kind,
        fair_key,
        priority,
        run_after,
        input_seq,
    ):
        assert row["state"] == "leased"
        row["input_seq"] = max(row["input_seq"] or input_seq, input_seq)
        calls.append(("enqueue", input_seq))
        return EnqueueResult(status="input_recorded", state="leased")

    async def complete_unit(
        _conn,
        *,
        unit_id,
        lease_token,
        consumed_seq,
    ):
        assert row["state"] == "leased"
        calls.append(("complete", consumed_seq))
        row["state"] = (
            "queued"
            if row["input_seq"] is not None
            and (consumed_seq is None or row["input_seq"] > consumed_seq)
            else "done"
        )
        row["attempts"] = 0
        return row["state"]

    monkeypatch.setattr(worker_queue, "enqueue_unit", enqueue_unit)
    monkeypatch.setattr(worker_queue, "complete_unit", complete_unit)
    job_id = uuid4()
    watermark = None

    for expected in range(1, 21):
        row["state"] = "leased"  # successor claim
        row["attempts"] += 1
        rotation = await worker_queue.rotate_worker_batch(
            conn,
            unit_id=job_id,
            lease_token=expected,
            input_seq=watermark,
            fair_key="tenant-a",
        )
        assert rotation is not None
        assert rotation.state == "queued"
        assert rotation.enqueue.state == "leased"
        assert rotation.next_input_seq == expected
        assert row["attempts"] == 0
        watermark = rotation.next_input_seq

    assert calls == [
        item
        for old in [None, *range(1, 20)]
        for item in (("enqueue", (old or 0) + 1), ("complete", old))
    ]
    assert watermark == 20


@pytest.mark.asyncio
async def test_stale_rotation_cannot_mutate_successor_watermark(monkeypatch):
    """Token N must be rejected before the tokenless enqueue primitive."""

    conn = _Connection()
    conn.fetchrow.return_value = None
    enqueue = AsyncMock()
    complete = AsyncMock()
    monkeypatch.setattr(worker_queue, "enqueue_unit", enqueue)
    monkeypatch.setattr(worker_queue, "complete_unit", complete)
    job_id = uuid4()

    result = await worker_queue.rotate_worker_batch(
        conn,
        unit_id=job_id,
        lease_token=41,
        input_seq=7,
        fair_key="tenant-a",
    )

    assert result is None
    enqueue.assert_not_awaited()
    complete.assert_not_awaited()
    lock_sql, locked_job_id, locked_token = conn.fetchrow.await_args.args
    assert "state = 'leased'" in lock_sql
    assert "lease_token = $2::bigint" in lock_sql
    assert locked_job_id == job_id
    assert locked_token == 41


@pytest.mark.asyncio
async def test_rotation_complete_miss_rolls_back_tokenless_enqueue(monkeypatch):
    """A post-enqueue fence miss must escape the transaction as an error."""

    conn = _Connection()
    conn.fetchrow.return_value = {"input_seq": 3}
    monkeypatch.setattr(
        worker_queue,
        "enqueue_unit",
        AsyncMock(return_value=EnqueueResult("input_recorded", "leased")),
    )
    monkeypatch.setattr(worker_queue, "complete_unit", AsyncMock(return_value=None))

    with pytest.raises(RuntimeError, match="lost its locked lease"):
        await worker_queue.rotate_worker_batch(
            conn,
            unit_id=uuid4(),
            lease_token=9,
            input_seq=3,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("park_on_exhaustion", "state"),
    [(True, "parked"), (False, "queued")],
)
async def test_worker_error_release_has_explicit_terminal_report_exception(
    park_on_exhaustion,
    state,
):
    conn = _Connection()
    conn.fetchval.return_value = state
    job_id = uuid4()

    result = await worker_queue.release_worker_batch(
        conn,
        unit_id=job_id,
        lease_token=12,
        park_on_exhaustion=park_on_exhaustion,
    )

    assert result == state
    sql, bound_job, token, park, backoff = conn.fetchval.await_args.args
    assert "attempts_since_completion >= max_attempts" in sql
    assert "THEN 'parked'" in sql
    assert "unit_kind = 'worker_batch'" in sql
    assert bound_job == job_id
    assert token == 12
    assert park is park_on_exhaustion
    assert backoff == 5.0


class TestStatelessWorkerBackendAdmissible:
    """One predicate for both admission twins (claim CAS + agent guard)."""

    def test_sandbox_always_admissible(self, monkeypatch):
        from shared.workspace_contract import stateless_worker_backend_admissible

        for mode in ("off", "same-cluster", "external"):
            assert stateless_worker_backend_admissible("sandbox", vm_mode=mode)
        assert stateless_worker_backend_admissible("container", vm_mode="off")

    def test_vm_admissible_only_on_pod_network(self):
        from shared.workspace_contract import stateless_worker_backend_admissible

        assert stateless_worker_backend_admissible("vm", vm_mode="same-cluster")
        assert stateless_worker_backend_admissible("remote", vm_mode="same-cluster")
        assert not stateless_worker_backend_admissible("vm", vm_mode="external")
        assert not stateless_worker_backend_admissible("vm", vm_mode="off")

    def test_lite_and_junk_never_admissible(self):
        from shared.workspace_contract import stateless_worker_backend_admissible

        for backend in ("virtual", "none", "", None, 7, "desktop"):
            assert not stateless_worker_backend_admissible(
                backend, vm_mode="same-cluster"
            )

    def test_job_requests_vm_follows_topology(self, monkeypatch):
        from shared.worker_queue import _job_requests_vm

        vm_job = {
            "config_override": {"workspace": {"backend": "vm"}},
            "context": {
                "_workspace_contract": {
                    "version": 1,
                    "assigned_backend": "vm",
                    "requested_backend": "vm",
                    "assignment_source": "test",
                }
            },
        }
        monkeypatch.setenv("VM_MODE", "same-cluster")
        assert _job_requests_vm(vm_job) is False
        monkeypatch.setenv("VM_MODE", "external")
        assert _job_requests_vm(vm_job) is True
        monkeypatch.delenv("VM_MODE")
        assert _job_requests_vm(vm_job) is True
