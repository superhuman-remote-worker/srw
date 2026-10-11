"""Rejected cancellation reports workspace conflicts without accepting cleanup."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi import HTTPException
import pytest

from orchestrator.services.job_mutation_controls import JobControlOperations
from tests.test_pinned_cancel_retirement_order import (
    JOB_ID,
    _bind_runtime,
    _cancel_client,
    _job,
    access_module,
    thread_retirement_module,
)


def rejection_store(lane, refreshed, *, active_access=False):
    initial = _job("paused", lane=lane)
    if lane == "stateless":
        initial["context"] = {
            "vm": {"status": "ready", "provision_generation": "original-generation"},
            "_worker_execution_hold": {"reason": "post_bundle_executor_loss"},
        }
    calls = 0

    def get_job(_job_id):
        nonlocal calls
        calls += 1
        return initial if calls == 1 else refreshed

    return SimpleNamespace(
        get_job=AsyncMock(side_effect=get_job),
        cancel_stateless_job=AsyncMock(return_value=(False, False)),
        linearize_pinned_cancel=AsyncMock(return_value=False),
        cancel_job=AsyncMock(return_value=False),
        has_active_job_workspace_access=AsyncMock(return_value=active_access),
        delete_checkpoint_thread=AsyncMock(),
    )


def retirement_spies(monkeypatch):
    cleanup, cascade, settle, finish = (
        AsyncMock(),
        AsyncMock(return_value=True),
        AsyncMock(return_value=True),
        AsyncMock(),
    )
    monkeypatch.setattr(
        thread_retirement_module.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        cleanup,
    )
    for name, spy in (
        ("cascade_cancel_to_children", cascade),
        ("wait_for_stateless_cancel_settle", settle),
        ("_finish_cancel", finish),
    ):
        monkeypatch.setattr(JobControlOperations, name, spy)
    return cleanup, cascade, settle, finish


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["stateless", "pinned", None])
@pytest.mark.parametrize(
    "active_access,detail",
    [
        (True, "Close active workspace access, then retry cancellation"),
        (False, "Job cancellation blocked because workspace state changed"),
    ],
)
async def test_rejected_nonterminal_cancel_reports_workspace_conflict(
    monkeypatch, lane, active_access, detail
):
    store = rejection_store(lane, _job("paused", lane=lane), active_access=active_access)
    main = _bind_runtime(monkeypatch, store, True)
    cleanup, cascade, settle, finish = retirement_spies(monkeypatch)
    async with _cancel_client(main, monkeypatch, store) as client:
        response = await client.put(f"/api/jobs/{JOB_ID}/cancel")
    assert response.status_code == 409
    assert response.json() == {"detail": detail}
    store.has_active_job_workspace_access.assert_awaited_once_with(JOB_ID)
    settle.assert_not_awaited()
    finish.assert_not_awaited()
    store.delete_checkpoint_thread.assert_not_awaited()
    if lane is None:
        # Preserve the legacy path's existing retirement-before-CAS order.
        cleanup.assert_awaited_once_with(JOB_ID)
        cascade.assert_awaited_once_with(JOB_ID)
    else:
        cleanup.assert_not_awaited()
        cascade.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["stateless", "pinned", None])
@pytest.mark.parametrize("status", [None, "completed"])
async def test_rejected_missing_or_completed_cancel_preserves_terminal_response(
    monkeypatch, lane, status
):
    refreshed = None if status is None else _job(status, lane=lane)
    store = rejection_store(lane, refreshed, active_access=True)
    main = _bind_runtime(monkeypatch, store, True)
    retirement_spies(monkeypatch)
    async with _cancel_client(main, monkeypatch, store) as client:
        response = await client.put(f"/api/jobs/{JOB_ID}/cancel")
    assert response.status_code == 400
    assert response.json() == {
        "detail": "Job cannot be cancelled (already completed or cancelled)"
    }
    store.has_active_job_workspace_access.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["stateless", "pinned", None])
async def test_rejected_cancel_of_already_cancelled_job_keeps_existing_cleanup(
    monkeypatch, lane
):
    store = rejection_store(lane, _job("cancelled", lane=lane), active_access=True)
    main = _bind_runtime(monkeypatch, store, True)
    _, _, _, finish = retirement_spies(monkeypatch)
    async with _cancel_client(main, monkeypatch, store) as client:
        response = await client.put(f"/api/jobs/{JOB_ID}/cancel")
    assert response.status_code == 200, response.json()
    assert response.json() == {"status": "cancelled"}
    store.has_active_job_workspace_access.assert_not_awaited()
    finish.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["stateless", "pinned", None])
async def test_accepted_cancel_does_not_classify_workspace_access(monkeypatch, lane):
    row = _job("paused", lane=lane)
    store = rejection_store(lane, row, active_access=True)
    store.get_job = AsyncMock(return_value=row)

    async def accept(*_args, **_kwargs):
        row["status"] = "cancelled"
        return (True, True) if lane == "stateless" else True

    if lane == "stateless":
        store.cancel_stateless_job.side_effect = accept
    elif lane == "pinned":
        store.linearize_pinned_cancel.side_effect = accept
    else:
        store.cancel_job.side_effect = accept
    main = _bind_runtime(monkeypatch, store, True)
    _, _, _, finish = retirement_spies(monkeypatch)
    async with _cancel_client(main, monkeypatch, store) as client:
        response = await client.put(f"/api/jobs/{JOB_ID}/cancel")
    assert response.status_code == 200, response.json()
    assert response.json() == {"status": "cancelled"}
    store.has_active_job_workspace_access.assert_not_awaited()
    finish.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancel_authorization_failure_does_not_query_workspace_access(monkeypatch):
    store = rejection_store("stateless", _job("paused", lane="stateless"), active_access=True)
    main = _bind_runtime(monkeypatch, store, True)

    async def denied(*_args):
        raise HTTPException(status_code=403, detail="Job access denied")

    async with _cancel_client(main, monkeypatch, store) as client:
        monkeypatch.setattr(access_module, "require_internal_or_job_access", denied)
        response = await client.put(f"/api/jobs/{JOB_ID}/cancel")
    assert response.status_code == 403
    store.get_job.assert_not_awaited()
    store.cancel_stateless_job.assert_not_awaited()
    store.has_active_job_workspace_access.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["stateless", "pinned", None])
async def test_deadline_cancel_cas_loss_remains_unchanged_without_access_query(lane):
    store = rejection_store(lane, _job("paused", lane=lane), active_access=True)
    operations = JobControlOperations(
        SimpleNamespace(
            store=store,
            completion_commands_enabled=lambda: False,
            completion_control=SimpleNamespace(dispatch_guard_kwargs=lambda: {}),
        )
    )
    result = await operations.cancel(
        JOB_ID, job=_job("paused", lane=lane), expected_execution_deadline="original"
    )
    assert result == {"status": "unchanged"}
    store.has_active_job_workspace_access.assert_not_awaited()
    store.get_job.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", ["completion", "deletion"])
async def test_existing_stateless_cancel_conflicts_precede_access_classification(
    monkeypatch, conflict
):
    refreshed = _job("paused", lane="stateless")
    if conflict == "deletion":
        # Match the JSONB text shape returned by asyncpg.
        refreshed["context"] = '{"_stateless_delete_pending": true}'
    store = rejection_store("stateless", refreshed, active_access=True)
    main = _bind_runtime(monkeypatch, store, True)
    retirement_spies(monkeypatch)
    if conflict == "completion":
        monkeypatch.setattr(
            main.app.state.resources,
            "completion_control_boundary",
            SimpleNamespace(
                dispatch_guard_kwargs=lambda: {},
                active_claim=lambda _row: True,
                claim_detail=lambda _row: "existing control conflict",
            ),
        )
    async with _cancel_client(main, monkeypatch, store) as client:
        response = await client.put(f"/api/jobs/{JOB_ID}/cancel")
    assert response.status_code == 409
    assert response.json() == {
        "detail": (
            "Job deletion is already in progress"
            if conflict == "deletion"
            else "existing control conflict"
        )
    }
    store.has_active_job_workspace_access.assert_not_awaited()
