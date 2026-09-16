import asyncio
from contextlib import asynccontextmanager
import json
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.container_provisioner import WorkspaceRuntimeAttestation
from orchestrator.services.vm_readiness import VMReadinessService
from orchestrator.services.vm_readiness import qualify_recovery_successor
from orchestrator.services.ssh_helpers import orchestrator_can_reach


GENERATION = "11111111-1111-4111-8111-111111111111"
HOST_KEY_FINGERPRINT = "SHA256:" + ("A" * 43)
GUEST_MACHINE_ID = "41" * 16
SERVER_REGISTRATION_ID = "51" * 16


def complete_recovery_network(challenge="fresh-challenge"):
    return {
        "challenge": challenge,
        "boot_id": "00000000-0000-4000-8000-000000000041",
        "machine_id": GUEST_MACHINE_ID,
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


def wire_recovery_qualification(monkeypatch, telemetry):
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.wait_for_agent_ssh",
        AsyncMock(return_value=(True, 1, None)),
    )
    monkeypatch.setattr(
        "secrets.token_urlsafe",
        lambda _size: "fresh-challenge",
    )
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.uuid4",
        lambda: UUID(hex=SERVER_REGISTRATION_ID),
    )

    @asynccontextmanager
    async def command(*_args, **_kwargs):
        yield ["ssh", "qualified-guest"]

    process = MagicMock(returncode=0)
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.pinned_agent_ssh_command", command
    )
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.create_owned_subprocess_exec",
        AsyncMock(return_value=process),
    )
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.communicate_bounded",
        AsyncMock(return_value=(json.dumps(telemetry).encode(), b"")),
    )


@pytest.mark.asyncio
async def test_recovery_successor_requires_complete_fresh_guest_telemetry(
    monkeypatch,
) -> None:
    telemetry = complete_recovery_network()
    wire_recovery_qualification(monkeypatch, telemetry)

    result = await qualify_recovery_successor(
        {
            "pod_ip": "10.42.0.90",
            "vmi_uid": "00000000-0000-4000-8000-000000000042",
            "launcher_uid": "00000000-0000-4000-8000-000000000043",
            "interface_mac": "02:00:00:00:00:41",
        },
        host_key_fingerprint=HOST_KEY_FINGERPRINT,
    )

    assert result == {
        "pod_ip": "10.42.0.90",
        "ssh_registration_id": SERVER_REGISTRATION_ID,
        "guest_boot_id": "00000000-0000-4000-8000-000000000041",
        "guest_machine_id": GUEST_MACHINE_ID,
        "guest_network": telemetry,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing",
    [
        "interfaces",
        "address",
        "default_route",
        "dns",
        "cloud_init_instance_id",
        "cloud_init_cache_identity",
        "machine_id",
    ],
)
async def test_recovery_successor_rejects_incomplete_guest_telemetry(
    monkeypatch, missing
) -> None:
    telemetry = complete_recovery_network()
    telemetry.pop(missing)
    wire_recovery_qualification(monkeypatch, telemetry)

    assert (
        await qualify_recovery_successor(
            {
                "pod_ip": "10.42.0.90",
                "vmi_uid": "00000000-0000-4000-8000-000000000042",
                "launcher_uid": "00000000-0000-4000-8000-000000000043",
                "interface_mac": "02:00:00:00:00:41",
            },
            host_key_fingerprint=HOST_KEY_FINGERPRINT,
        )
        is None
    )


@pytest.mark.asyncio
async def test_recovery_successor_rejects_stale_guest_challenge(monkeypatch) -> None:
    wire_recovery_qualification(
        monkeypatch, complete_recovery_network(challenge="replayed-challenge")
    )

    assert (
        await qualify_recovery_successor(
            {
                "pod_ip": "10.42.0.90",
                "vmi_uid": "00000000-0000-4000-8000-000000000042",
                "launcher_uid": "00000000-0000-4000-8000-000000000043",
                "interface_mac": "02:00:00:00:00:41",
            },
            host_key_fingerprint=HOST_KEY_FINGERPRINT,
        )
        is None
    )


@pytest.mark.asyncio
async def test_recovery_successor_rejects_guest_supplied_registration_id(
    monkeypatch,
) -> None:
    telemetry = complete_recovery_network()
    telemetry["registration_id"] = "constant-unbound-registration"
    wire_recovery_qualification(monkeypatch, telemetry)

    assert (
        await qualify_recovery_successor(
            {
                "pod_ip": "10.42.0.90",
                "vmi_uid": "00000000-0000-4000-8000-000000000042",
                "launcher_uid": "00000000-0000-4000-8000-000000000043",
                "interface_mac": "02:00:00:00:00:41",
            },
            host_key_fingerprint=HOST_KEY_FINGERPRINT,
        )
        is None
    )


@pytest.mark.asyncio
async def test_recovery_successor_re_attests_same_guest_with_fresh_nonces(
    monkeypatch,
) -> None:
    challenges = iter(("fresh-challenge-one", "fresh-challenge-two"))
    registrations = iter((UUID(int=81), UUID(int=82)))
    telemetry = iter(
        (
            complete_recovery_network("fresh-challenge-one"),
            complete_recovery_network("fresh-challenge-two"),
        )
    )
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.wait_for_agent_ssh",
        AsyncMock(return_value=(True, 1, None)),
    )
    monkeypatch.setattr("secrets.token_urlsafe", lambda _size: next(challenges))
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.uuid4", lambda: next(registrations)
    )

    @asynccontextmanager
    async def command(*_args, **_kwargs):
        yield ["ssh", "qualified-guest"]

    process = MagicMock(returncode=0)
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.pinned_agent_ssh_command", command
    )
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.create_owned_subprocess_exec",
        AsyncMock(return_value=process),
    )
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.communicate_bounded",
        AsyncMock(
            side_effect=lambda *_args, **_kwargs: (
                json.dumps(next(telemetry)).encode(),
                b"",
            )
        ),
    )
    successor = {
        "pod_ip": "10.42.0.90",
        "vmi_uid": "00000000-0000-4000-8000-000000000042",
        "launcher_uid": "00000000-0000-4000-8000-000000000043",
        "interface_mac": "02:00:00:00:00:41",
    }

    first = await qualify_recovery_successor(
        successor, host_key_fingerprint=HOST_KEY_FINGERPRINT
    )
    second = await qualify_recovery_successor(
        successor, host_key_fingerprint=HOST_KEY_FINGERPRINT
    )

    assert first is not None and second is not None
    assert first["ssh_registration_id"] != second["ssh_registration_id"]
    assert first["guest_boot_id"] == second["guest_boot_id"]
    assert first["guest_machine_id"] == second["guest_machine_id"]


def test_same_cluster_reachability_ignores_address_class(monkeypatch):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    assert orchestrator_can_reach("100.64.23.180") is True


def candidate(entity_id="11111111-1111-4111-8111-111111111112", **vm):
    return {
        "entity_id": entity_id,
        "user_id": "11111111-1111-4111-8111-111111111113",
        "vm": {
            "status": "created",
            "provision_generation": GENERATION,
            "ssh_host_key_fingerprint": HOST_KEY_FINGERPRINT,
            **vm,
        },
    }


class FakeDB:
    def __init__(
        self,
        jobs=(),
        threads=(),
        ready_jobs=(),
        ready_threads=(),
        *,
        promote_result=True,
        recovery_owned=False,
    ):
        self.jobs = list(jobs)
        self.threads = list(threads)
        self.ready_jobs = list(ready_jobs)
        self.ready_threads = list(ready_threads)
        self.calls = []
        self.promotions = []
        self.promote_result = promote_result
        self.recovery_owned = recovery_owned
        self.recovery_checks = 0

    async def vm_workspace_recovery_owns_authority(self, entity_type, entity_id):
        del entity_type, entity_id
        self.recovery_checks += 1
        if callable(self.recovery_owned):
            return bool(self.recovery_owned(self.recovery_checks))
        return bool(self.recovery_owned)

    async def list_job_vm_readiness_candidates(self, *, ready=False):
        self.calls.append(("job", ready))
        return list(self.ready_jobs if ready else self.jobs)

    async def list_thread_vm_readiness_candidates(self, *, ready=False):
        self.calls.append(("thread", ready))
        return list(self.ready_threads if ready else self.threads)

    async def merge_vm_context_if_current(self, entity_id, registration_id, updates):
        self.promotions.append(("job", entity_id, registration_id, updates))
        return self.promote_result

    async def merge_thread_vm_context_if_current(
        self, entity_id, registration_id, updates
    ):
        self.promotions.append(("thread", entity_id, registration_id, updates))
        return self.promote_result


class FakeProvisioner:
    def __init__(self, status, *, write_result=True):
        self.status = status
        self.write_result = write_result
        self.writes = []
        self.queries = []

    async def query_status(self, entity_id, *, entity_type="job"):
        self.queries.append((entity_type, entity_id))
        value = (
            self.status(entity_id, entity_type)
            if callable(self.status)
            else self.status
        )
        if asyncio.iscoroutine(value):
            value = await value
        return value

    async def _set_context_if_generation(
        self,
        entity_type,
        entity_id,
        generation,
        updates,
        *,
        require_status_not_ready=False,
    ):
        self.writes.append(
            (entity_type, entity_id, generation, updates, require_status_not_ready)
        )
        return self.write_result

    async def attest_workspace_runtime(self, entity_id, *, entity_type="job"):
        del entity_id, entity_type
        if not self.writes:
            raise RuntimeError("runtime identity is not prepared")
        updates = self.writes[-1][3]
        uid = updates.get("active_pod_uid")
        host = updates.get("ssh_host")
        if not uid or not host:
            # Final/transient writes may omit coordinates; use the most recent
            # exact prepared tuple, mirroring the durable JSONB merge.
            prepared = next(
                write[3]
                for write in reversed(self.writes)
                if write[3].get("active_pod_uid") and write[3].get("ssh_host")
            )
            uid = prepared["active_pod_uid"]
            host = prepared["ssh_host"]
        return WorkspaceRuntimeAttestation(
            backing_id=f"k8s-vmi:{uid}",
            workspace_generation=GENERATION,
            runtime_incarnation=uid,
            ssh_host_key_fingerprint=HOST_KEY_FINGERPRINT,
            host=host,
            pod_ip=host,
            port=22,
        )


@pytest.mark.asyncio
async def test_readiness_does_not_probe_while_recovery_owns_authority() -> None:
    db = FakeDB(jobs=[candidate()], recovery_owned=True)
    provisioner = FakeProvisioner({"ready": True})

    await VMReadinessService(db, provisioner, trigger_dispatch=lambda: None).run_cycle()

    assert provisioner.queries == []
    assert provisioner.writes == []
    assert db.promotions == []


@pytest.mark.asyncio
async def test_readiness_does_not_mutate_when_recovery_wins_during_probe() -> None:
    db = FakeDB(
        jobs=[candidate()],
        recovery_owned=lambda check: check >= 2,
    )
    provisioner = FakeProvisioner(
        {
            "ready": True,
            "pod_ip": "10.42.0.10",
            "phase": "Running",
            "active_pod_uid": "pod-1",
        }
    )

    await VMReadinessService(db, provisioner, trigger_dispatch=lambda: None).run_cycle()

    assert provisioner.queries == [("job", candidate()["entity_id"])]
    assert provisioner.writes == []
    assert db.promotions == []


@pytest.fixture
def successful_ssh(monkeypatch):
    auth = AsyncMock(return_value=(True, 1, ""))
    seed = AsyncMock()
    monkeypatch.setattr("orchestrator.services.vm_readiness.wait_for_agent_ssh", auth)
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.seed_ide_config_for_user", seed
    )
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.resolve_ssh_key_path", lambda: "/key"
    )
    return auth, seed


@pytest.mark.asyncio
async def test_first_probe_success_promotes_once_with_cas(successful_ssh):
    row = candidate()
    db = FakeDB(jobs=[row])
    provisioner = FakeProvisioner(
        {
            "ready": True,
            "pod_ip": "10.42.0.10",
            "phase": "Running",
            "active_pod_uid": "pod-1",
        }
    )
    trigger = MagicMock()
    service = VMReadinessService(db, provisioner, trigger_dispatch=trigger)

    await service.run_cycle()

    entity_type, _, generation, prepared, cas = provisioner.writes[-1]
    assert (entity_type, generation, cas) == ("job", GENERATION, True)
    assert prepared["status"] == "ssh_pending"
    promoted_type, _, registration_id, updates = db.promotions[-1]
    assert promoted_type == "job"
    assert updates["status"] == "ready"
    assert updates["ssh_host"] == updates["pod_ip"] == "10.42.0.10"
    assert updates["ssh_port"] == 22
    assert updates["active_pod_uid"] == "pod-1"
    assert updates["ssh_ready_source"] == "provisioner_probe"
    assert updates["ssh_registration_id"] == registration_id
    assert updates["ssh_probe_error"] is None
    assert updates["recovering"] is False
    successful_ssh[0].assert_awaited_once_with(
        "10.42.0.10",
        22,
        key_path="/key",
        deadline_s=10.0,
        connect_timeout_s=10,
        interval_s=0.5,
        expected_host_key_fingerprint=HOST_KEY_FINGERPRINT,
    )
    successful_ssh[1].assert_awaited_once()
    assert (
        successful_ssh[1].await_args.kwargs["expected_host_key_fingerprint"]
        == HOST_KEY_FINGERPRINT
    )
    assert callable(successful_ssh[1].await_args.kwargs["mutation_authority"])
    trigger.assert_called_once()
    assert len(provisioner.writes) == 1


@pytest.mark.asyncio
async def test_replaced_launcher_at_same_ip_receives_zero_seed_bytes(monkeypatch):
    ssh = AsyncMock(return_value=(True, 1, ""))
    effects = []

    async def seed(*_args, **kwargs):
        target = await kwargs["mutation_authority"]()
        if target is not None:
            effects.append(target)
        return target is not None

    monkeypatch.setattr("orchestrator.services.vm_readiness.wait_for_agent_ssh", ssh)
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.seed_ide_config_for_user", seed
    )
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.resolve_ssh_key_path", lambda: "/key"
    )

    class ReplacedProvisioner(FakeProvisioner):
        def __init__(self):
            super().__init__(
                {
                    "ready": True,
                    "pod_ip": "10.42.0.55",
                    "phase": "Running",
                    "active_pod_uid": "00000000-0000-4000-8000-000000000055",
                }
            )
            self.attestations = 0

        async def attest_workspace_runtime(self, entity_id, *, entity_type="job"):
            del entity_id, entity_type
            self.attestations += 1
            uid = (
                "00000000-0000-4000-8000-000000000055"
                if self.attestations == 1
                else "00000000-0000-4000-8000-000000000099"
            )
            fingerprint = (
                HOST_KEY_FINGERPRINT
                if self.attestations == 1
                else "SHA256:" + ("B" * 43)
            )
            return WorkspaceRuntimeAttestation(
                backing_id=f"k8s-vmi:{uid}",
                workspace_generation=GENERATION,
                runtime_incarnation=uid,
                ssh_host_key_fingerprint=fingerprint,
                host="10.42.0.55",
                pod_ip="10.42.0.55",
                port=22,
            )

    provisioner = ReplacedProvisioner()
    trigger = MagicMock()
    await VMReadinessService(
        FakeDB(jobs=[candidate()]),
        provisioner,
        trigger_dispatch=trigger,
    ).run_cycle()

    assert effects == []
    assert all(write[3].get("status") != "ready" for write in provisioner.writes)
    trigger.assert_not_called()


@pytest.mark.asyncio
async def test_transient_failure_records_pending_attempt(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.wait_for_agent_ssh",
        AsyncMock(return_value=(False, 1, "SSH authentication failed")),
    )
    row = candidate(ssh_probe_attempts=2)
    provisioner = FakeProvisioner(
        {
            "ready": True,
            "pod_ip": "10.42.0.11",
            "active_pod_uid": "pod-transient",
            "phase": "Running",
        }
    )
    await VMReadinessService(
        FakeDB(jobs=[row]), provisioner, trigger_dispatch=lambda: None
    ).run_cycle()
    updates = provisioner.writes[-1][3]
    assert updates["status"] == "ssh_pending"
    assert updates["ssh_probe_attempts"] == 3
    assert "authentication" in updates["ssh_probe_error"]
    assert provisioner.writes[-1][4] is True


@pytest.mark.asyncio
async def test_wrong_presented_host_key_refuses_promotion_and_records_mismatch(
    monkeypatch,
):
    ssh = AsyncMock(return_value=(False, 1, "SSH host-key fingerprint mismatch"))
    monkeypatch.setattr("orchestrator.services.vm_readiness.wait_for_agent_ssh", ssh)
    provisioner = FakeProvisioner(
        {
            "ready": True,
            "pod_ip": "10.42.0.14",
            "active_pod_uid": "pod-wrong-key",
            "phase": "Running",
        }
    )
    trigger = MagicMock()

    await VMReadinessService(
        FakeDB(jobs=[candidate()]), provisioner, trigger_dispatch=trigger
    ).run_cycle()

    updates = provisioner.writes[-1][3]
    assert updates["status"] == "ssh_pending"
    assert updates["ssh_probe_error"] == "SSH host-key fingerprint mismatch"
    assert all(write[3].get("status") != "ready" for write in provisioner.writes)
    trigger.assert_not_called()


@pytest.mark.asyncio
async def test_absent_host_key_pin_refuses_promotion_without_ssh(monkeypatch):
    ssh = AsyncMock()
    monkeypatch.setattr("orchestrator.services.vm_readiness.wait_for_agent_ssh", ssh)
    provisioner = FakeProvisioner(
        {
            "ready": True,
            "pod_ip": "10.42.0.15",
            "active_pod_uid": "pod-no-pin",
            "phase": "Running",
        }
    )

    await VMReadinessService(
        FakeDB(jobs=[candidate(ssh_host_key_fingerprint=None)]),
        provisioner,
        trigger_dispatch=lambda: None,
    ).run_cycle()

    updates = provisioner.writes[-1][3]
    assert updates["status"] == "ssh_pending"
    assert updates["ssh_probe_error"] == "SSH host-key fingerprint pin is absent"
    ssh.assert_not_awaited()


@pytest.mark.asyncio
async def test_reprobe_controller_blip_preserves_ready_row(successful_ssh):
    row = candidate(status="ready", pod_ip="10.42.0.20", active_pod_uid="pod-old")
    provisioner = FakeProvisioner(None)

    await VMReadinessService(
        FakeDB(ready_jobs=[row]), provisioner, trigger_dispatch=lambda: None
    ).run_cycle()

    assert provisioner.writes == []
    successful_ssh[0].assert_not_awaited()


@pytest.mark.asyncio
async def test_reprobe_probe_failure_preserves_ready_row(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.wait_for_agent_ssh",
        AsyncMock(return_value=(False, 1, "SSH authentication failed")),
    )
    row = candidate(status="ready", pod_ip="10.42.0.20", active_pod_uid="pod-old")
    provisioner = FakeProvisioner(
        {
            "ready": True,
            "pod_ip": "10.42.0.21",
            "active_pod_uid": "pod-new",
            "phase": "Running",
        }
    )

    await VMReadinessService(
        FakeDB(ready_jobs=[row]), provisioner, trigger_dispatch=lambda: None
    ).run_cycle()

    assert provisioner.writes == []


@pytest.mark.asyncio
async def test_reprobe_fingerprint_mismatch_demotes_ready_row(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.wait_for_agent_ssh",
        AsyncMock(return_value=(False, 1, "SSH host-key fingerprint mismatch")),
    )
    row = candidate(status="ready", pod_ip="10.42.0.20", active_pod_uid="pod-old")
    provisioner = FakeProvisioner(
        {
            "ready": True,
            "pod_ip": "10.42.0.20",
            "active_pod_uid": "pod-old",
            "phase": "Running",
        }
    )

    await VMReadinessService(
        FakeDB(ready_jobs=[row]), provisioner, trigger_dispatch=lambda: None
    ).run_cycle()

    assert provisioner.writes[-1][3]["status"] == "ssh_pending"
    assert "fingerprint mismatch" in provisioner.writes[-1][3]["ssh_probe_error"]
    assert provisioner.writes[-1][4] is False


@pytest.mark.asyncio
async def test_not_found_marks_ready_vm_unreachable(successful_ssh):
    row = candidate(status="ready", pod_ip="10.42.0.20", active_pod_uid="pod-old")
    provisioner = FakeProvisioner({"status": "not_found"})

    await VMReadinessService(
        FakeDB(ready_jobs=[row]), provisioner, trigger_dispatch=lambda: None
    ).run_cycle()

    assert provisioner.writes[-1][3] == {
        "status": "ssh_unreachable",
        "ssh_probe_error": "vm not found",
    }


@pytest.mark.asyncio
async def test_stopped_guest_becomes_unreachable(monkeypatch):
    ssh = AsyncMock()
    monkeypatch.setattr("orchestrator.services.vm_readiness.wait_for_agent_ssh", ssh)
    provisioner = FakeProvisioner(
        {"ready": False, "phase": "Succeeded", "pod_ip": "10.42.0.12"}
    )
    await VMReadinessService(
        FakeDB(jobs=[candidate()]), provisioner, trigger_dispatch=lambda: None
    ).run_cycle()
    assert provisioner.writes[-1][3] == {
        "status": "ssh_unreachable",
        "ssh_probe_error": "vm stopped",
    }
    ssh.assert_not_awaited()


@pytest.mark.asyncio
async def test_ip_change_reprobes_ready_vm(successful_ssh):
    row = candidate(status="ready", pod_ip="10.42.0.20", active_pod_uid="pod-old")
    db = FakeDB(ready_jobs=[row])
    provisioner = FakeProvisioner(
        {
            "ready": True,
            "pod_ip": "10.42.0.21",
            "phase": "Running",
            "active_pod_uid": "pod-new",
        }
    )
    service = VMReadinessService(db, provisioner, trigger_dispatch=lambda: None)
    await service.run_cycle()
    assert provisioner.writes[-1][3]["ssh_host"] == "10.42.0.21"
    assert provisioner.writes[0][4] is False
    assert db.promotions[-1][3]["active_pod_uid"] == "pod-new"


@pytest.mark.asyncio
async def test_reprobe_unchanged_identity_reverifies_pin_without_repromotion(
    successful_ssh,
):
    row = candidate(status="ready", pod_ip="10.42.0.20", active_pod_uid="pod-old")
    provisioner = FakeProvisioner(
        {
            "ready": True,
            "pod_ip": "10.42.0.20",
            "phase": "Running",
            "active_pod_uid": "pod-old",
        }
    )

    await VMReadinessService(
        FakeDB(ready_jobs=[row]), provisioner, trigger_dispatch=lambda: None
    ).run_cycle()

    assert provisioner.writes == []
    successful_ssh[0].assert_awaited_once()


@pytest.mark.asyncio
async def test_stale_controller_generation_is_ignored(successful_ssh):
    provisioner = FakeProvisioner(
        {
            "ready": True,
            "pod_ip": "10.42.0.21",
            "active_pod_uid": "pod-new",
            "provision_generation": "33333333-3333-4333-8333-333333333333",
        }
    )

    await VMReadinessService(
        FakeDB(jobs=[candidate()]), provisioner, trigger_dispatch=lambda: None
    ).run_cycle()

    assert provisioner.writes == []
    successful_ssh[0].assert_not_awaited()


@pytest.mark.asyncio
async def test_rejected_promotion_suppresses_seed_and_dispatch(successful_ssh):
    provisioner = FakeProvisioner(
        {
            "ready": True,
            "pod_ip": "10.42.0.22",
            "phase": "Running",
            "active_pod_uid": "pod-new",
        },
        write_result=False,
    )
    trigger = MagicMock()

    await VMReadinessService(
        FakeDB(jobs=[candidate()]), provisioner, trigger_dispatch=trigger
    ).run_cycle()

    assert len(provisioner.writes) == 1
    successful_ssh[1].assert_not_awaited()
    trigger.assert_not_called()


@pytest.mark.asyncio
async def test_registration_cas_loss_cannot_publish_stale_ready(successful_ssh):
    db = FakeDB(jobs=[candidate()], promote_result=False)
    provisioner = FakeProvisioner(
        {
            "ready": True,
            "pod_ip": "10.42.0.23",
            "phase": "Running",
            "active_pod_uid": "pod-b",
        }
    )
    trigger = MagicMock()

    await VMReadinessService(db, provisioner, trigger_dispatch=trigger).run_cycle()

    assert db.promotions[-1][3]["status"] == "ready"
    assert provisioner.writes[-1][3]["status"] == "ssh_pending"
    trigger.assert_not_called()


@pytest.mark.asyncio
async def test_backoff_skips_candidate(successful_ssh):
    row = candidate()
    provisioner = FakeProvisioner({"ready": False})
    service = VMReadinessService(
        FakeDB(jobs=[row]), provisioner, trigger_dispatch=lambda: None
    )
    key = ("job", row["entity_id"], GENERATION)
    service._retry_after[key] = asyncio.get_running_loop().time() + 60

    await service.run_cycle()

    assert provisioner.queries == []


@pytest.mark.asyncio
async def test_inflight_key_deduplicates_candidate(successful_ssh):
    row = candidate()
    provisioner = FakeProvisioner({"ready": False})
    service = VMReadinessService(
        FakeDB(jobs=[row]), provisioner, trigger_dispatch=lambda: None
    )
    service._inflight.add(("job", row["entity_id"], GENERATION))

    await service.run_cycle()

    assert provisioner.queries == []


@pytest.mark.asyncio
async def test_retry_state_pruned_for_non_candidates(successful_ssh):
    service = VMReadinessService(
        FakeDB(), FakeProvisioner({"ready": False}), trigger_dispatch=lambda: None
    )
    stale = ("job", "11111111-1111-4111-8111-111111111199", GENERATION)
    service._failures[stale] = 4
    service._retry_after[stale] = 999999999.0

    await service.run_cycle()

    assert service._failures == {}
    assert service._retry_after == {}


@pytest.mark.asyncio
async def test_new_leader_rearms_from_db_rows(successful_ssh):
    db = FakeDB(jobs=[candidate()])
    status = {
        "ready": True,
        "pod_ip": "10.42.0.30",
        "phase": "Running",
        "active_pod_uid": "pod",
    }
    first = FakeProvisioner(status)
    second = FakeProvisioner(status)
    await VMReadinessService(db, first, trigger_dispatch=lambda: None).run_cycle()
    await VMReadinessService(db, second, trigger_dispatch=lambda: None).run_cycle()
    assert first.writes and second.writes
    assert db.calls.count(("job", False)) == 2


@pytest.mark.asyncio
async def test_concurrency_cap(successful_ssh):
    active = 0
    peak = 0

    async def status(_entity_id, _entity_type):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return {"ready": False, "phase": "Starting"}

    rows = [candidate(f"11111111-1111-4111-8111-{index:012d}") for index in range(10)]
    service = VMReadinessService(
        FakeDB(jobs=rows),
        FakeProvisioner(status),
        trigger_dispatch=lambda: None,
        max_inflight=3,
    )
    await service.run_cycle()
    assert peak == 3


@pytest.mark.asyncio
async def test_thread_entity_promotes_without_dispatch(successful_ssh):
    provisioner = FakeProvisioner(
        {
            "ready": True,
            "pod_ip": "10.42.0.40",
            "phase": "Running",
            "active_pod_uid": "pod",
        }
    )
    trigger = MagicMock()
    await VMReadinessService(
        FakeDB(threads=[candidate()]), provisioner, trigger_dispatch=trigger
    ).run_cycle()
    assert provisioner.writes[-1][0] == "thread"
    trigger.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "status_expression"),
    [
        ("merge_vm_context_if_provision_generation", "context->'vm'->>'status'"),
        (
            "merge_thread_vm_context_if_provision_generation",
            "metadata->'vm'->>'status'",
        ),
    ],
)
async def test_database_generation_merge_includes_ready_cas(
    method_name, status_expression
):
    executed = []

    class Connection:
        async def execute(self, query, *args):
            executed.append((query, args))
            return "UPDATE 1"

    db = PostgresDB("postgresql://unused")

    @asynccontextmanager
    async def acquire():
        yield Connection()

    db.acquire = acquire
    method = getattr(db, method_name)
    assert await method(
        "11111111-1111-4111-8111-111111111112",
        GENERATION,
        {"status": "ready"},
        require_status_not_ready=True,
    )
    query, args = executed[0]
    assert status_expression in query
    assert "IS DISTINCT FROM 'ready'" in query
    assert args[-1] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "terminal_predicate"),
    [
        (
            "list_job_vm_readiness_candidates",
            "jobs.status NOT IN ('completed','failed','cancelled')",
        ),
        (
            "list_thread_vm_readiness_candidates",
            "threads.status <> 'ended' AND threads.ended_at IS NULL",
        ),
    ],
)
async def test_readiness_queries_filter_finished_fake_row(
    method_name, terminal_predicate
):
    stale_row = candidate(status="created")

    class Connection:
        async def fetch(self, query):
            return [] if terminal_predicate in query else [stale_row]

    db = PostgresDB("postgresql://unused")

    @asynccontextmanager
    async def acquire():
        yield Connection()

    db.acquire = acquire
    assert await getattr(db, method_name)() == []


@pytest.mark.asyncio
async def test_reprobe_scan_no_key_blip_preserves_ready_row(monkeypatch):
    """Unreachable/closed-port scans are availability, never identity (D2)."""
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.wait_for_agent_ssh",
        AsyncMock(
            return_value=(False, 1, "SSH host-key scan found no ed25519 host key")
        ),
    )
    row = candidate(status="ready", pod_ip="10.42.0.20", active_pod_uid="pod-old")
    provisioner = FakeProvisioner(
        {
            "ready": True,
            "pod_ip": "10.42.0.20",
            "active_pod_uid": "pod-old",
            "phase": "Running",
        }
    )

    await VMReadinessService(
        FakeDB(ready_jobs=[row]), provisioner, trigger_dispatch=lambda: None
    ).run_cycle()

    assert provisioner.writes == []


@pytest.mark.asyncio
async def test_reprobe_ssh_process_verification_failure_demotes_ready_row(monkeypatch):
    """The scan->connect race surfaces ssh's own wording; still identity."""
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.wait_for_agent_ssh",
        AsyncMock(return_value=(False, 1, "Host key verification failed.")),
    )
    row = candidate(status="ready", pod_ip="10.42.0.20", active_pod_uid="pod-old")
    provisioner = FakeProvisioner(
        {
            "ready": True,
            "pod_ip": "10.42.0.20",
            "active_pod_uid": "pod-old",
            "phase": "Running",
        }
    )

    await VMReadinessService(
        FakeDB(ready_jobs=[row]), provisioner, trigger_dispatch=lambda: None
    ).run_cycle()

    assert provisioner.writes[-1][3]["status"] == "ssh_pending"
    assert provisioner.writes[-1][4] is False


class _FakeKeyscanProc:
    def __init__(self, stdout: bytes):
        self.stdout = MagicMock()
        self.stdout.read = AsyncMock(side_effect=[stdout, b""])
        self.stderr = MagicMock()
        self.stderr.read = AsyncMock(side_effect=[b""])
        self.wait = AsyncMock(return_value=0)
        self.returncode = 0


@pytest.mark.asyncio
async def test_scan_empty_output_is_availability_not_identity(monkeypatch):
    from orchestrator.services import ssh_helpers

    async def fake_exec(*_args, **_kwargs):
        return _FakeKeyscanProc(b"")

    monkeypatch.setattr(ssh_helpers.asyncio, "create_subprocess_exec", fake_exec)
    line, error = await ssh_helpers._scan_pinned_host_key(
        "10.0.0.1", 22, "SHA256:" + "A" * 43
    )
    assert line is None
    assert error == b"SSH host-key scan found no ed25519 host key"
    assert b"fingerprint" not in error


@pytest.mark.asyncio
async def test_scan_presented_wrong_key_is_identity_mismatch(monkeypatch):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

    from orchestrator.services import ssh_helpers

    pub = (
        Ed25519PrivateKey.generate()
        .public_key()
        .public_bytes(Encoding.OpenSSH, PublicFormat.OpenSSH)
        .decode("ascii")
    )

    async def fake_exec(*_args, **_kwargs):
        return _FakeKeyscanProc(f"10.0.0.1 {pub}\n".encode("ascii"))

    monkeypatch.setattr(ssh_helpers.asyncio, "create_subprocess_exec", fake_exec)
    line, error = await ssh_helpers._scan_pinned_host_key(
        "10.0.0.1", 22, "SHA256:" + "A" * 43
    )
    assert line is None
    assert error == b"SSH server host key did not match the pinned fingerprint"


@pytest.mark.asyncio
@pytest.mark.parametrize("vmi", [None, "Pending", "Scheduling", "Scheduled"])
async def test_initial_stopped_vm_keeps_probing_while_its_disk_is_allocating(
    successful_ssh,
    vmi,
):
    provisioner = FakeProvisioner(
        {
            "ready": False,
            "phase": "Stopped",
            "credential_runtime_started": vmi is not None,
            "vmi_phase": vmi,
        }
    )
    await VMReadinessService(
        FakeDB(jobs=[candidate()]), provisioner, trigger_dispatch=lambda: None
    ).run_cycle()
    assert provisioner.writes[-1][3]["status"] == "ssh_pending"
    assert provisioner.writes[-1][3]["ssh_probe_attempts"] == 1
    successful_ssh[0].assert_not_awaited()
