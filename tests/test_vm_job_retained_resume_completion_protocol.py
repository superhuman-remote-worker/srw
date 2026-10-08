"""S36 reuses the retained C stop without promoting it to a purge."""

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator.services import completion_effects
from orchestrator.services.completion_effects import (
    CompletionEffectDependencies,
    run_completion_workspace_teardown,
)
from orchestrator.services.completion_finalizer import CompletionDispositionSuperseded
from orchestrator.services.vm_provisioner import VMTeardownIdentity, VMTeardownResult
from orchestrator.services.vm_workspace_recovery_store import CleanupPermit
from orchestrator.services import vm_job_retained_resume


class Runner:
    def __init__(self, *, authorized=True):
        self.command_id = str(uuid4())
        self.authorized = authorized
        self.intent = None
        self.actions = []

    async def authorize_workspace_teardown(self):
        self.actions.append("authorize")
        return SimpleNamespace(
            authorized=self.authorized,
            superseded=not self.authorized,
            operator_hold=False,
            observed_status="cancelled" if not self.authorized else "completed",
            expected_status="completed",
        )

    async def capture_intent(self, _name, detail=None):
        self.actions.append("capture")
        if detail is not None:
            self.intent = detail
        return self.intent

    async def run(self, *, callback, **_kwargs):
        self.actions.append("effect")
        return await callback()


def case(
    monkeypatch,
    *,
    authorized=True,
    outcome="completed",
    permit_allowed=True,
    replayed=None,
    charge_proven=True,
    terminal_result=True,
    purge_disk=False,
):
    job_id, generation, pvc_uid = (str(uuid4()) for _ in range(3))
    identity = VMTeardownIdentity(generation, None, pvc_uid)
    runner = Runner(authorized=authorized)
    calls = []

    async def release(*_args, **_kwargs):
        calls.append("stop")
        return VMTeardownResult(outcome, outcome == "completed")

    db = SimpleNamespace(
        get_job=AsyncMock(
            return_value={"context": {"vm": {"status": "creation_pending"}}}
        )
    )
    recovery = SimpleNamespace(db=db)
    provisioner = SimpleNamespace(
        capture_vm_teardown_identity=AsyncMock(return_value=identity),
        release_vm_captured=AsyncMock(side_effect=release),
    )
    parent = {
        "admission_id": str(uuid4()),
        "request_id": str(uuid4()),
        "intent_digest": "sha256:" + "a" * 64,
        "intent": {"source": "completion_workspace_teardown", "purge_disk": purge_disk},
    }
    permit = CleanupPermit(
        allowed=permit_allowed,
        admission_id=uuid4() if permit_allowed else None,
        reason=None if permit_allowed else "retained_terminal_held",
        completed_outcome=replayed,
        parent_cleanup=parent if permit_allowed else None,
    )
    expected_job_id = job_id
    expected_identity = identity
    expected_provisioner = provisioner

    async def acquire(
        _store, _provisioner, *, job_id: str, identity: VMTeardownIdentity
    ):
        assert job_id == expected_job_id and identity == expected_identity
        calls.append("acquire")
        return permit

    async def prepare(_store, supplied):
        assert supplied is permit
        calls.append("prepare")
        return {"candidate": "charged-C"} if charge_proven else None

    async def complete(_store, supplied, *, outcome: str, provisioner: object):
        assert supplied is permit and outcome == "completed"
        assert provisioner is expected_provisioner
        calls.append("complete")

    async def terminal(_db, asked, *, clear_pending):
        assert asked == job_id and clear_pending is False
        calls.append("terminal")
        return terminal_result

    monkeypatch.setattr(
        vm_job_retained_resume,
        "acquire_retained_terminal_cleanup",
        acquire,
        raising=False,
    )
    monkeypatch.setattr(vm_job_retained_resume, "complete_retained_cancel", terminal)
    monkeypatch.setattr(completion_effects, "prepare_vm_cleanup_resource", prepare)
    monkeypatch.setattr(completion_effects, "complete_vm_cleanup_permit", complete)
    archive = AsyncMock(side_effect=AssertionError("legacy cleanup must not run"))
    dependencies = CompletionEffectDependencies(
        store=db,
        container_provisioner=None,
        vm_provisioner=provisioner,
        get_container_context=lambda _row: {},
        get_vm_context=lambda row: row["context"]["vm"],
        archive_and_cleanup_workspace=archive,
        s36_exact_absence_timeout_seconds=lambda: 1.0,
        logger=logging.getLogger(__name__),
        recovery_store=recovery,
    )
    return job_id, runner, dependencies, identity, provisioner, calls, archive


@pytest.mark.asyncio
async def test_s36_retained_c_stops_with_shared_false_parent_before_terminal(
    monkeypatch,
):
    job_id, runner, dependencies, identity, provisioner, calls, archive = case(
        monkeypatch
    )

    result = await run_completion_workspace_teardown(
        job_id, runner, dependencies=dependencies
    )

    assert result == {"actions": ["vm released"], "teardown_disposition": "completed"}
    assert runner.actions[0] == "effect"
    assert runner.actions[1] == "authorize"
    assert calls == ["acquire", "prepare", "stop", "complete", "terminal"]
    provisioner.release_vm_captured.assert_awaited_once()
    args, kwargs = provisioner.release_vm_captured.await_args
    assert args == (job_id, identity)
    assert kwargs["purge_disk"] is False
    assert kwargs["capture_snapshot"] is False
    assert kwargs["parent_cleanup"]["intent"]["purge_disk"] is False
    archive.assert_not_awaited()


@pytest.mark.asyncio
async def test_s36_retained_c_replay_uses_completed_stop_without_second_release(
    monkeypatch,
):
    job_id, runner, dependencies, _identity, provisioner, calls, archive = case(
        monkeypatch, replayed="completed"
    )

    result = await run_completion_workspace_teardown(
        job_id, runner, dependencies=dependencies
    )

    assert result == {"actions": ["vm released"], "teardown_disposition": "completed"}
    assert calls == ["acquire", "complete", "terminal"]
    provisioner.release_vm_captured.assert_not_awaited()
    archive.assert_not_awaited()


@pytest.mark.asyncio
async def test_s36_retained_c_rejects_wrong_purge_parent_before_stop(monkeypatch):
    job_id, runner, dependencies, _identity, provisioner, calls, archive = case(
        monkeypatch, purge_disk=True
    )

    result = await run_completion_workspace_teardown(
        job_id, runner, dependencies=dependencies
    )

    assert result["teardown_disposition"] == "retry_pending"
    assert calls == ["acquire"]
    provisioner.release_vm_captured.assert_not_awaited()
    archive.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "charge_proven,terminal_result",
    [
        (False, True),
        (True, False),
    ],
)
async def test_s36_retained_c_never_finishes_before_charge_and_terminal_proof(
    monkeypatch,
    charge_proven,
    terminal_result,
):
    job_id, runner, dependencies, _identity, provisioner, calls, archive = case(
        monkeypatch,
        charge_proven=charge_proven,
        terminal_result=terminal_result,
    )

    result = await run_completion_workspace_teardown(
        job_id, runner, dependencies=dependencies
    )

    assert result["teardown_disposition"] == "retry_pending"
    if not charge_proven:
        assert calls == ["acquire", "prepare"]
        provisioner.release_vm_captured.assert_not_awaited()
    else:
        assert calls == ["acquire", "prepare", "stop", "complete", "terminal"]
        provisioner.release_vm_captured.assert_awaited_once()
    archive.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "authorized,outcome,permit_allowed",
    [
        (False, "completed", True),
        (True, "identity_unknown", True),
        (True, "completed", False),
    ],
)
async def test_s36_retained_c_holds_without_stop_proof_or_authority(
    monkeypatch, authorized, outcome, permit_allowed
):
    job_id, runner, dependencies, _identity, provisioner, calls, archive = case(
        monkeypatch,
        authorized=authorized,
        outcome=outcome,
        permit_allowed=permit_allowed,
    )

    if not authorized:
        with pytest.raises(CompletionDispositionSuperseded):
            await run_completion_workspace_teardown(
                job_id, runner, dependencies=dependencies
            )
        assert calls == []
        provisioner.release_vm_captured.assert_not_awaited()
    else:
        result = await run_completion_workspace_teardown(
            job_id, runner, dependencies=dependencies
        )
        assert result["teardown_disposition"] == "retry_pending"
    if authorized and not permit_allowed:
        assert result["teardown_disposition"] == "retry_pending"
        assert calls == ["acquire"]
        provisioner.release_vm_captured.assert_not_awaited()
    elif authorized:
        assert calls == ["acquire", "prepare", "stop"]
        provisioner.release_vm_captured.assert_awaited_once()
    archive.assert_not_awaited()
