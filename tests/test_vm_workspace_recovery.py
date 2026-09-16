"""Bounded orchestration for durable VM workspace recovery."""

from __future__ import annotations

import asyncio
import json
import logging
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
from orchestrator.logging_config import JsonLogFormatter
from orchestrator.services.vm_workspace_recovery_store import RecoveryClaim
from orchestrator.services.vm_workspace_recovery_store import RetentionPinCommand
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
    guest_network = {
        "challenge": "fresh-challenge-one",
        "boot_id": "00000000-0000-4000-8000-000000000041",
        "machine_id": "41" * 16,
        "interfaces": [
            {
                "ifname": "eth0",
                "address": "10.0.2.15",
                "mac": "02:00:00:00:00:41",
            }
        ],
        "address": "10.0.2.15",
        "routes": [{"dst": "default", "gateway": "10.0.2.2"}],
        "default_route": {"dst": "default", "gateway": "10.0.2.2"},
        "dns": "nameserver 10.0.2.3",
        "netplan_sha256": {"/etc/netplan/50-cloud-init.yaml": "a" * 64},
        "networkd_sha256": {},
        "cloud_init_instance_id": "iid-datasource-none",
        "cloud_init_cache_identity": "b" * 64,
        "cloud_init_cache_cleaned": False,
    }
    return {
        "ready": True,
        "authenticated": True,
        "ambiguous": False,
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
            "ssh_registration_id": "51" * 16,
            "guest_boot_id": "00000000-0000-4000-8000-000000000041",
            "guest_machine_id": "41" * 16,
            "interface_mac": "02:00:00:00:00:41",
            "guest_network": guest_network,
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
        self.renewed: list[RecoveryClaim] = []
        self.pin_deferred: list[str] = []
        self.pin_acknowledged = True
        self.pin_command = RetentionPinCommand(
            recovery_id=OPERATION_ID,
            pvc_uid=PVC_UID,
            provision_generation=GENERATION,
            desired_state="active",
            owner_kind="job",
            owner_id=OWNER_ID,
            namespace="agent-vms",
        )

    async def claim_due(
        self,
        operation_id,
        *,
        ttl_seconds=30,
        permit_ttl_seconds=30,
        max_global_probes=4,
        max_probes_per_node=1,
    ):
        assert operation_id == OPERATION_ID
        assert ttl_seconds == 30
        assert permit_ttl_seconds == 30
        assert max_global_probes == 4
        assert max_probes_per_node == 1
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

    async def renew_claim(self, recovery_claim, *, ttl_seconds, permit_ttl_seconds=30):
        assert ttl_seconds == 30
        assert permit_ttl_seconds == 30
        self.renewed.append(recovery_claim)
        return recovery_claim

    async def list_due_operation_ids(self, *, limit=32):
        return [OPERATION_ID]

    async def pause_automatic_disabled(self):
        self.disabled_pauses += 1
        return 1

    async def list_retention_pin_commands(self, *, limit=32):
        assert limit == 32
        return []

    async def retention_pin_command(self, recovery_claim):
        return self.pin_command

    async def retention_pin_is_acknowledged(self, recovery_claim):
        return self.pin_acknowledged

    async def acknowledge_retention_pin(self, command, result):
        self.pin_acknowledged = result.get("state") == "active"
        return self.pin_acknowledged

    async def defer_retention_pin_command(self, command, *, error):
        self.pin_deferred.append(error)

    async def accept_stop_evidence(self, recovery_claim, evidence):
        return evidence.get("evidence_digest")

    async def trusted_stop_receipt(self, recovery_claim):
        return "sha256:trusted-stop"


class PinStore(FakeStore):
    def __init__(self, *, acknowledged: bool = False) -> None:
        super().__init__()
        self.acknowledged = acknowledged
        self.pin_acknowledged = acknowledged
        self.command = RetentionPinCommand(
            recovery_id=OPERATION_ID,
            pvc_uid=PVC_UID,
            provision_generation=GENERATION,
            desired_state="active",
            owner_kind="job",
            owner_id=OWNER_ID,
            namespace="agent-vms",
        )

    async def retention_pin_command(self, recovery_claim):
        return self.command

    async def retention_pin_is_acknowledged(self, recovery_claim):
        return self.acknowledged

    async def acknowledge_retention_pin(self, command, result):
        self.acknowledged = result.get("state") == "active"
        return self.acknowledged

    async def defer_retention_pin_command(self, command, *, error):
        self.pin_deferred.append(error)


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


@pytest.mark.asyncio
async def test_observation_waits_for_exact_controller_pin_ack() -> None:
    recovery_store = PinStore()
    observer = Observer([ready_observation()])

    await service(recovery_store, observer).reconcile_once(OPERATION_ID)

    assert observer.calls == 0
    assert recovery_store.deferred[-1]["diagnostic"]["reason"] == (
        "controller_retention_pin_unacknowledged"
    )
    assert recovery_store.pin_deferred


@pytest.mark.asyncio
async def test_pin_response_loss_retries_before_observation() -> None:
    recovery_store = PinStore()
    observation = ready_observation()

    class PinObserver(Observer):
        def __init__(self):
            super().__init__([observation, observation.copy()])
            self.pin_attempts = 0

        async def reconcile_workspace_recovery_pin(self, command):
            self.pin_attempts += 1
            if self.pin_attempts == 1:
                raise TimeoutError("response lost")
            return {
                "state": "active",
                "recovery_id": str(command.recovery_id),
                "pvc_uid": str(command.pvc_uid),
                "provision_generation": str(command.provision_generation),
                "pin_uid": "pin-uid",
                "resource_version": "2",
            }

    observer = PinObserver()
    recovery = service(recovery_store, observer)
    await recovery.reconcile_once(OPERATION_ID)
    await recovery.reconcile_once(OPERATION_ID)

    assert observer.pin_attempts == 2
    assert observer.calls == 2
    assert recovery_store.released


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
    assert recovery_store.renewed
    assert recovery_store.released[0]["final_observation"] == observation


@pytest.mark.asyncio
async def test_fresh_re_attestation_allows_server_registration_rotation() -> None:
    recovery_store = FakeStore()
    initial = ready_observation()
    final = ready_observation()
    final["successor"] = {
        **final["successor"],  # type: ignore[arg-type]
        "ssh_registration_id": "52" * 16,
        "guest_network": {
            **final["successor"]["guest_network"],  # type: ignore[index]
            "challenge": "fresh-challenge-two",
        },
    }

    await service(recovery_store, Observer([initial, final])).reconcile_once(
        OPERATION_ID
    )

    assert not recovery_store.paused
    assert recovery_store.released[0]["final_observation"] == final


@pytest.mark.asyncio
async def test_fresh_re_attestation_rejects_changed_guest_identity() -> None:
    recovery_store = FakeStore()
    initial = ready_observation()
    final = ready_observation()
    final["successor"] = {
        **final["successor"],  # type: ignore[arg-type]
        "guest_machine_id": "42" * 16,
    }

    await service(recovery_store, Observer([initial, final])).reconcile_once(
        OPERATION_ID
    )

    assert not recovery_store.released
    assert recovery_store.paused[-1]["code"] is WorkspaceRecoveryCode.IDENTITY_CONFLICT


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
    "mutation",
    [
        lambda value: value.__setitem__("ambiguous", True),
        lambda value: value["successor"].pop("node_uid"),
        lambda value: value["successor"].pop("ssh_registration_id"),
        lambda value: value["successor"].pop("guest_boot_id"),
        lambda value: value["successor"].pop("guest_machine_id"),
        lambda value: value.__setitem__("continuation", "unknown"),
        lambda value: value.__setitem__("remote_operations", "pending"),
    ],
)
@pytest.mark.asyncio
async def test_final_attestation_repeats_every_safety_predicate(mutation) -> None:
    recovery_store = FakeStore()
    initial = ready_observation()
    final = ready_observation()
    mutation(final)

    await service(recovery_store, Observer([initial, final])).reconcile_once(
        OPERATION_ID
    )

    assert not recovery_store.released
    assert recovery_store.paused or recovery_store.deferred


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
    assert (
        recovery_store.paused[-1]["code"] is WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
    )


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
        observe_workspace_recovery=AsyncMock(
            side_effect=RuntimeError("controller down")
        )
    )

    await service(recovery_store, observer).reconcile_once(OPERATION_ID)

    assert recovery_store.deferred[-1]["phase"] == "waiting_runtime"
    assert "controller down" in recovery_store.deferred[-1]["diagnostic"]["detail"]


@pytest.mark.asyncio
async def test_reconciler_production_path_emits_redacted_probe_and_pause_audits(
    caplog,
) -> None:
    recovery_store = FakeStore()
    observation = ready_observation()
    observation["vm_uid"] = "00000000-0000-4000-8000-00000000feed"

    with caplog.at_level(
        logging.INFO,
        logger="orchestrator.services.vm_workspace_recovery_telemetry",
    ):
        await service(recovery_store, Observer([observation])).reconcile_once(
            OPERATION_ID
        )

    payloads = [
        json.loads(JsonLogFormatter().format(record))
        for record in caplog.records
        if getattr(record, "audit_event", None) == "vm_workspace_recovery"
    ]
    assert [payload["recovery_event"] for payload in payloads] == ["probe", "pause"]
    encoded = json.dumps(payloads)
    for raw in (str(OPERATION_ID), str(OWNER_ID), str(VM_UID), str(PVC_UID)):
        assert raw not in encoded
    assert payloads[-1]["recovery_reason"] == (
        "captured_workspace_identity_changed_or_ambiguous"
    )
    assert payloads[-1]["controller_observation_digest"].startswith("sha256:")


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
