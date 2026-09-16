"""Bounded orchestration for durable VM workspace recovery."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from orchestrator.services.vm_workspace_recovery import (
    VMWorkspaceRecoveryService,
    recovery_retry_delay,
)
from orchestrator.services.vm_workspace_recovery_store import RecoveryClaim
from shared.workspace_recovery import WorkspaceRecoveryCode


OPERATION_ID = UUID("00000000-0000-4000-8000-000000000601")
OWNER_ID = UUID("00000000-0000-4000-8000-000000000602")
GENERATION = UUID("00000000-0000-4000-8000-000000000603")
VM_UID = UUID("00000000-0000-4000-8000-000000000604")
OLD_VMI_UID = UUID("00000000-0000-4000-8000-000000000605")
OLD_LAUNCHER_UID = UUID("00000000-0000-4000-8000-000000000606")
PVC_UID = UUID("00000000-0000-4000-8000-000000000607")
NEW_VMI_UID = UUID("00000000-0000-4000-8000-000000000608")
NEW_LAUNCHER_UID = UUID("00000000-0000-4000-8000-000000000609")


def claim(*, version: int = 2, attempt: int = 1) -> RecoveryClaim:
    return RecoveryClaim(
        operation_id=OPERATION_ID,
        version=version,
        claim_token=7,
        deadline_at=datetime.now(timezone.utc) + timedelta(minutes=15),
        remaining_seconds=900.0,
        captured_identity={
            "owner_kind": "job",
            "owner_id": OWNER_ID,
            "provision_generation": GENERATION,
            "cluster_name": "test",
            "namespace": "agent-vms",
            "vm_uid": VM_UID,
            "prior_vmi_uid": OLD_VMI_UID,
            "prior_launcher_uid": OLD_LAUNCHER_UID,
            "root_pvc_uid": PVC_UID,
        },
        attempt=attempt,
        global_slot=0,
        node_key="node-old",
    )


def ready_observation() -> dict[str, object]:
    return {
        "ready": True,
        "authenticated": True,
        "owner_kind": "job",
        "owner_id": str(OWNER_ID),
        "provision_generation": str(GENERATION),
        "vm_uid": str(VM_UID),
        "root_pvc_uid": str(PVC_UID),
        "prior_runtime": "stopped",
        "stop_receipt_digest": "sha256:trusted-stop",
        "remote_operations": "settled",
        "continuation": "safe",
        "successor": {
            "vmi_uid": str(NEW_VMI_UID),
            "launcher_uid": str(NEW_LAUNCHER_UID),
            "node_uid": "node-new",
            "pod_ip": "10.42.0.90",
            "ssh_registration_id": "registration-1",
        },
    }


class FakeStore:
    def __init__(self, recovery_claim: RecoveryClaim | None = None) -> None:
        self.recovery_claim = recovery_claim or claim()
        self.current = True
        self.deferred: list[dict[str, object]] = []
        self.paused: list[dict[str, object]] = []
        self.staged: list[dict[str, object]] = []
        self.released: list[dict[str, object]] = []
        self.disabled_pauses = 0

    async def claim_due(self, operation_id, *, ttl_seconds=30):
        assert operation_id == OPERATION_ID
        assert ttl_seconds == 30
        return self.recovery_claim

    async def claim_is_current(self, recovery_claim):
        return self.current and recovery_claim.claim_token == 7

    async def defer_claim(self, **kwargs):
        self.deferred.append(kwargs)
        return True

    async def pause_for_attention(self, **kwargs):
        self.paused.append(kwargs)
        return True

    async def stage_observation(self, **kwargs):
        self.staged.append(kwargs)
        return replace(self.recovery_claim, version=self.recovery_claim.version + 1)

    async def release_recovered(self, **kwargs):
        self.released.append(kwargs)
        return True

    async def list_due_operation_ids(self, *, limit=32):
        return [OPERATION_ID]

    async def pause_automatic_disabled(self):
        self.disabled_pauses += 1
        return 1


class Observer:
    def __init__(self, observations: list[dict[str, object]]) -> None:
        self.observations = observations
        self.calls = 0

    async def observe_workspace_recovery(self, captured_identity):
        assert captured_identity["root_pvc_uid"] == PVC_UID
        value = self.observations[self.calls]
        self.calls += 1
        return value


def service(store: FakeStore, observer: object) -> VMWorkspaceRecoveryService:
    return VMWorkspaceRecoveryService(
        store,
        observer,
        jitter=lambda: 1.0,
        claim_poll_seconds=0.001,
    )


def test_recovery_retry_delay_is_jittered_and_capped() -> None:
    assert [recovery_retry_delay(n, jitter=lambda: 1.0) for n in range(1, 7)] == [
        10.0,
        20.0,
        40.0,
        60.0,
        60.0,
        60.0,
    ]
    assert recovery_retry_delay(4, remaining_seconds=12, jitter=lambda: 1.0) == 12
    assert recovery_retry_delay(4, jitter=lambda: 1.5) == 60


@pytest.mark.asyncio
async def test_ready_replacement_is_re_attested_before_final_release() -> None:
    recovery_store = FakeStore()
    observation = ready_observation()
    observer = Observer([observation, observation.copy()])

    await service(recovery_store, observer).reconcile_once(OPERATION_ID)

    assert observer.calls == 2
    assert recovery_store.staged[0]["phase"] == "attesting"
    assert recovery_store.released[0]["final_observation"] == observation


@pytest.mark.asyncio
async def test_changed_final_attestation_retains_hold_for_attention() -> None:
    recovery_store = FakeStore()
    initial = ready_observation()
    changed = ready_observation()
    changed["successor"] = {
        **changed["successor"],  # type: ignore[arg-type]
        "launcher_uid": str(UUID(int=99)),
    }

    await service(recovery_store, Observer([initial, changed])).reconcile_once(
        OPERATION_ID
    )

    assert not recovery_store.released
    assert recovery_store.paused[-1]["code"] is WorkspaceRecoveryCode.IDENTITY_CONFLICT


@pytest.mark.parametrize(
    ("key", "changed"),
    [
        ("owner_id", UUID(int=91)),
        ("provision_generation", UUID(int=92)),
        ("root_pvc_uid", UUID(int=93)),
    ],
)
@pytest.mark.asyncio
async def test_changed_captured_identity_pauses_before_attestation(
    key: str, changed: UUID
) -> None:
    recovery_store = FakeStore()
    observation = ready_observation()
    observation[key] = str(changed)

    await service(recovery_store, Observer([observation])).reconcile_once(OPERATION_ID)

    assert not recovery_store.staged
    assert not recovery_store.released
    assert recovery_store.paused[-1]["code"] is WorkspaceRecoveryCode.IDENTITY_CONFLICT


@pytest.mark.asyncio
async def test_unknown_tool_outcome_pauses_without_releasing() -> None:
    recovery_store = FakeStore()
    observation = ready_observation()
    observation["continuation"] = "unknown"

    await service(recovery_store, Observer([observation])).reconcile_once(OPERATION_ID)

    assert not recovery_store.released
    assert recovery_store.paused[-1]["code"] is WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN


@pytest.mark.asyncio
async def test_unresolved_remote_operation_defers_inside_immutable_budget() -> None:
    recovery_store = FakeStore(claim(attempt=3))
    observation = ready_observation()
    observation["remote_operations"] = "pending"

    await service(recovery_store, Observer([observation])).reconcile_once(OPERATION_ID)

    assert not recovery_store.released
    assert recovery_store.deferred[-1]["phase"] == "reconciling_outcome"
    assert recovery_store.deferred[-1]["next_check_seconds"] == 40


@pytest.mark.asyncio
async def test_lost_claim_cancels_local_probe_and_discards_result() -> None:
    recovery_store = FakeStore()
    started = asyncio.Event()
    cancelled = asyncio.Event()

    class DeferredObserver:
        async def observe_workspace_recovery(self, _identity):
            started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    task = asyncio.create_task(
        service(recovery_store, DeferredObserver()).reconcile_once(OPERATION_ID)
    )
    await started.wait()
    recovery_store.current = False
    await asyncio.wait_for(task, timeout=1)

    assert cancelled.is_set()
    assert not recovery_store.deferred
    assert not recovery_store.paused
    assert not recovery_store.released


@pytest.mark.asyncio
async def test_automation_disabled_visibly_pauses_due_work() -> None:
    recovery_store = FakeStore()
    shutdown = asyncio.Event()
    shutdown.set()
    recovery_service = VMWorkspaceRecoveryService(
        recovery_store,
        SimpleNamespace(),
        automatic_enabled=False,
    )

    await recovery_service.run(shutdown)

    assert recovery_store.disabled_pauses == 1


@pytest.mark.asyncio
async def test_observer_exception_is_deferred_without_losing_hold() -> None:
    recovery_store = FakeStore()
    observer = SimpleNamespace(
        observe_workspace_recovery=AsyncMock(side_effect=RuntimeError("controller down"))
    )

    await service(recovery_store, observer).reconcile_once(OPERATION_ID)

    assert recovery_store.deferred[-1]["phase"] == "waiting_runtime"
    assert "controller down" in recovery_store.deferred[-1]["diagnostic"]["detail"]


@pytest.mark.asyncio
async def test_final_attestation_failure_is_deferred_under_same_hold() -> None:
    recovery_store = FakeStore()

    class FailingFinalObserver(Observer):
        async def observe_workspace_recovery(self, captured_identity):
            if self.calls:
                raise RuntimeError("final controller read failed")
            return await super().observe_workspace_recovery(captured_identity)

    await service(
        recovery_store,
        FailingFinalObserver([ready_observation()]),
    ).reconcile_once(OPERATION_ID)

    assert not recovery_store.released
    assert recovery_store.deferred[-1]["phase"] == "attesting"
