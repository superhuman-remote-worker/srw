"""Contracts for the in-image real VM recovery fault scenario."""

from __future__ import annotations

from contextlib import asynccontextmanager
import asyncio
from datetime import datetime, timedelta, timezone
import inspect
import json
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock

import pytest

from orchestrator.services import vm_workspace_recovery_config as recovery_config
from orchestrator.services.ssh_helpers import SSHHostKeyVerificationError
from orchestrator.services.vm_workspace_recovery import VMWorkspaceRecoveryService
from orchestrator.services.vm_workspace_recovery_store import RecoveryClaim
from orchestrator.operator_cli.vm_workspace_recovery_acceptance import (
    CONFIRMATION,
    LiveScenario,
    PROTOCOL_VERSION,
    REQUIRED_SCENARIOS,
    require_execution_guard,
)
from orchestrator.operator_cli import vm_workspace_recovery_acceptance as acceptance


def test_acceptance_command_carries_the_complete_live_scenario_matrix() -> None:
    assert PROTOCOL_VERSION == 1
    assert REQUIRED_SCENARIOS == (
        "response_loss",
        "leader_overlap",
        "slow_boot",
        "deadline",
        "missing_stop_evidence",
        "forced_deletion",
        "replacement",
    )


def test_acceptance_command_requires_chart_gate_and_exact_confirmation() -> None:
    with pytest.raises(RuntimeError, match="chart gate"):
        require_execution_guard({}, confirmation=CONFIRMATION, protocol_version=1)
    with pytest.raises(RuntimeError, match="confirmation"):
        require_execution_guard(
            {"VM_WORKSPACE_RECOVERY_ACCEPTANCE_GATE_ENABLED": "true"},
            confirmation="wrong",
            protocol_version=1,
        )
    with pytest.raises(RuntimeError, match="protocol"):
        require_execution_guard(
            {"VM_WORKSPACE_RECOVERY_ACCEPTANCE_GATE_ENABLED": "true"},
            confirmation=CONFIRMATION,
            protocol_version=2,
        )

    require_execution_guard(
        {"VM_WORKSPACE_RECOVERY_ACCEPTANCE_GATE_ENABLED": "true"},
        confirmation=CONFIRMATION,
        protocol_version=1,
    )


@pytest.mark.parametrize(
    ("profiled", "retry", "mode", "image", "allowlisted"),
    [
        (True, False, "same-cluster", None, False),
        (True, True, "remote", "digest", True),
        (True, True, "same-cluster", None, False),
        (True, True, "same-cluster", "digest", False),
    ],
)
def test_profiled_gate_fails_fast_on_incompatible_creation_settings(
    monkeypatch,
    profiled,
    retry,
    mode,
    image,
    allowlisted,
) -> None:
    digest = "registry.example/guest@sha256:" + "a" * 64
    monkeypatch.setenv(
        "VM_NETWORK_PROFILE_IMAGE_ALLOWLIST", digest if allowlisted else ""
    )
    scenario = object.__new__(LiveScenario)
    scenario.profiled_fixture, scenario.protocol_fixture = profiled, retry
    scenario.fixture_image = digest if image else None
    scenario.provisioner = SimpleNamespace(mode=mode)
    with pytest.raises(acceptance.AcceptanceFailure):
        scenario._require_fixture_configuration()


def test_acceptance_gate_process_owns_the_reconciler() -> None:
    assert (
        recovery_config.automatic_reconciler_enabled(
            {
                "VM_WORKSPACE_RECOVERY_ENABLED": "true",
                "VM_WORKSPACE_RECOVERY_ACCEPTANCE_GATE_ENABLED": "true",
            }
        )
        is False
    )
    assert (
        recovery_config.automatic_reconciler_enabled(
            {
                "VM_WORKSPACE_RECOVERY_ENABLED": "true",
                "VM_WORKSPACE_RECOVERY_ACCEPTANCE_GATE_ENABLED": "false",
            }
        )
        is True
    )


@pytest.mark.asyncio
async def test_acceptance_marker_io_rejects_the_wrong_pinned_host_key(
    monkeypatch,
) -> None:
    calls: list[dict[str, object]] = []

    @asynccontextmanager
    async def reject(*_args, **kwargs):
        calls.append(kwargs)
        raise SSHHostKeyVerificationError("host key mismatch")
        yield []

    monkeypatch.setattr(
        "orchestrator.services.ssh_helpers.pinned_agent_ssh_command", reject
    )
    scenario = object.__new__(LiveScenario)

    with pytest.raises(SSHHostKeyVerificationError, match="host key mismatch"):
        await scenario._ssh_file(
            {
                "pod_ip": "10.42.0.91",
                "ssh_host_key_fingerprint": "SHA256:" + "A" * 43,
            },
            "/home/agent-host/.srw-recovery-gate/marker",
            None,
        )

    assert calls == [
        {
            "expected_host_key_fingerprint": "SHA256:" + "A" * 43,
            "key_path": ANY,
            "connect_timeout_s": 10,
            "batch_mode": True,
        }
    ]


@pytest.mark.asyncio
async def test_stale_observation_barrier_finishes_only_after_claim_handoff() -> None:
    class Observer:
        async def observe_workspace_recovery(self, identity):
            return {"owner_id": identity["owner_id"]}

    barrier = acceptance.RecoveryObservationBarrier(
        Observer(), finish_after_cancellation=True
    )
    task = asyncio.create_task(
        barrier.observe_workspace_recovery({"owner_id": "job-1"})
    )
    await barrier.started.wait()
    task.cancel()
    await barrier.cancelled.wait()

    assert task.done() is False
    assert barrier.finished.is_set() is False

    barrier.release.set()
    assert await task == {"owner_id": "job-1"}
    assert barrier.finished.is_set() is True


@pytest.mark.asyncio
async def test_stale_observe_records_claim_fence_before_discarding_late_result() -> (
    None
):
    class Store:
        worker_id = "gate-leader-a:test"

        async def claim_is_current(self, _claim):
            return False

    class Observer:
        async def observe_workspace_recovery(self, _identity):
            return {"state": "ready"}

    claim_check_gate = asyncio.Event()
    store = acceptance.StaleEvidenceStore(Store(), claim_check_gate=claim_check_gate)
    barrier = acceptance.RecoveryObservationBarrier(
        Observer(), finish_after_cancellation=True
    )
    service = VMWorkspaceRecoveryService(
        store,
        barrier,
        probe_timeout_seconds=1,
        claim_poll_seconds=0.001,
    )
    claim = RecoveryClaim(
        operation_id=acceptance.uuid4(),
        version=2,
        claim_token=7,
        deadline_at=datetime.now(timezone.utc) + timedelta(minutes=15),
        remaining_seconds=900,
        captured_identity={"owner_kind": "job", "owner_id": str(acceptance.uuid4())},
    )

    task = asyncio.create_task(service._observe(claim))
    await asyncio.wait_for(barrier.started.wait(), timeout=1)
    await asyncio.sleep(0.01)
    assert store.boundary_checked.is_set() is False

    claim_check_gate.set()
    await asyncio.wait_for(store.boundary_checked.wait(), timeout=1)
    await asyncio.wait_for(barrier.cancelled.wait(), timeout=1)

    assert store.rejected_boundary == "claim_is_current"
    assert store.stage_attempted is False
    assert task.done() is False

    barrier.release.set()
    assert await asyncio.wait_for(task, timeout=1) is None


def test_deadline_gate_does_not_rewrite_the_immutable_deadline() -> None:
    source = inspect.getsource(LiveScenario.execute)
    assert "first_observed_at=clock_timestamp()-interval" not in source
    assert "deadline_at=clock_timestamp()-interval" not in source

    deadline_source = inspect.getsource(LiveScenario._deadline_barrier_scenario)
    assert "finish_after_cancellation=True" in deadline_source
    assert "allow_observation_past_deadline_for_acceptance=True" in deadline_source
    assert "precondition_check_rejected" in deadline_source
    assert "stage_observation_attempted" in deadline_source


def test_execute_owns_reconciliation_for_every_uncontrolled_wait() -> None:
    source = inspect.getsource(LiveScenario.execute)

    assert source.count("async with self._gate_owned_reconciler(") == 3
    assert "replacement recovery" in source
    assert "missing-stop-evidence pause" in source
    assert "forced-deletion attention pause" in source
    replacement_start = source.index('self._gate_owned_reconciler("replacement")')
    crash = source.index("await self._crash_launcher(identity)")
    assert source.index("await self._sync_gate_retention_pins(") < crash
    assert crash < replacement_start
    forced_start = source.index('self._gate_owned_reconciler("forced-deletion")')
    assert source.index("await self._force_delete_vmi(job_id)") < forced_start


def test_leader_overlap_uses_real_leader_boundary_and_reconciler_loops() -> None:
    source = inspect.getsource(LiveScenario._leader_handoff_scenario)

    assert "GateLeaderLease" in source
    assert source.count(".run(") >= 2
    assert ".reconcile_once(" not in source
    assert "leadership_transfer_succeeded" in source
    assert "stale_task.cancel()" not in source
    assert "stale_store_boundary_rejected" in source
    assert "stale_store_boundary" in source
    assert "stale_stage_attempted" in source
    assert "leader_a_backend_pid" in source
    assert "leader_b_backend_pid" in source


@pytest.mark.asyncio
async def test_gate_owned_reconciler_runs_only_inside_its_scenario_scope(
    monkeypatch,
) -> None:
    started = asyncio.Event()
    stopped = asyncio.Event()

    class Service:
        async def run(self, shutdown):
            started.set()
            await shutdown.wait()
            stopped.set()

    monkeypatch.setattr(
        "orchestrator.services.vm_workspace_recovery.VMWorkspaceRecoveryService.from_settings",
        lambda *_args, **_kwargs: Service(),
    )
    scenario = object.__new__(LiveScenario)
    scenario.db = object()
    scenario.provisioner = object()
    scenario.run_id = "scope"
    scenario.settings = SimpleNamespace(external_call_timeout_seconds=1)

    async with scenario._gate_owned_reconciler("replacement"):
        await asyncio.wait_for(started.wait(), timeout=1)
        assert stopped.is_set() is False

    assert stopped.is_set() is True


@pytest.mark.asyncio
async def test_gate_leader_lease_requires_real_exclusive_transfer() -> None:
    state = {"held": False, "next_pid": 4100}

    class Connection:
        def __init__(self, backend_pid):
            self.backend_pid = backend_pid

        async def fetchval(self, query, *_args):
            if "pg_try_advisory_lock" in query:
                if state["held"]:
                    return False
                state["held"] = True
                return True
            if "pg_advisory_unlock" in query:
                was_held = state["held"]
                state["held"] = False
                return was_held
            if "pg_backend_pid" in query:
                return self.backend_pid
            raise AssertionError(query)

    class Pool:
        async def acquire(self):
            state["next_pid"] += 1
            return Connection(state["next_pid"])

        async def release(self, _connection):
            return None

    db = SimpleNamespace(_pool=Pool())
    first = acceptance.GateLeaderLease(db, lock_id=91, identity="leader-a")
    second = acceptance.GateLeaderLease(db, lock_id=91, identity="leader-b")

    assert await first.acquire() is True
    assert await second.acquire() is False
    assert await first.unlock() is True
    assert await second.acquire() is True
    assert first.backend_pid != second.backend_pid
    await first.close()
    assert await second.release() is True
    assert first.identity != second.identity


@pytest.mark.asyncio
async def test_acceptance_vm_patch_uses_supported_custom_objects_arguments() -> None:
    calls: list[dict[str, object]] = []

    class CustomObjects:
        def patch_namespaced_custom_object(
            self, *, group, version, namespace, plural, name, body
        ):
            calls.append(
                {
                    "group": group,
                    "version": version,
                    "namespace": namespace,
                    "plural": plural,
                    "name": name,
                    "body": body,
                }
            )

    scenario = object.__new__(LiveScenario)
    scenario.namespace = "agent-vms"
    scenario._custom = CustomObjects()

    await scenario._set_vm_run_strategy(
        "00000000-0000-4000-8000-000000000611", "Halted"
    )

    assert calls == [
        {
            "group": "kubevirt.io",
            "version": "v1",
            "namespace": "agent-vms",
            "plural": "virtualmachines",
            "name": "agent-vm-00000000-0000-4000-8000-000000000611",
            "body": {"spec": {"runStrategy": "Halted"}},
        }
    ]


@pytest.mark.asyncio
async def test_acceptance_fixture_uses_production_job_creation_boundary() -> None:
    executed: list[tuple[str, tuple[object, ...]]] = []
    create_kwargs: list[dict[str, object]] = []

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    class Connection:
        def transaction(self):
            return Transaction()

        async def execute(self, query, *values):
            executed.append((query, values))

    class Acquire:
        async def __aenter__(self):
            return Connection()

        async def __aexit__(self, *_args):
            return None

    class Database:
        async def create_job(self, **kwargs):
            create_kwargs.append(kwargs)
            return {"id": kwargs["job_id"]}

        def acquire(self):
            return Acquire()

    scenario = object.__new__(LiveScenario)
    scenario.db = Database()
    scenario.run_id = "fixture-boundary"
    scenario.job_id = None
    scenario.gate_user_id = None

    job_id = await scenario._create_job()

    assert create_kwargs == [
        {
            "description": "[vm-recovery-gate:fixture-boundary] retained disk fixture",
            "context": {"vm_workspace_recovery_acceptance_gate": "fixture-boundary"},
            "origin": "lifecycle",
            "status": "processing",
            "execution_lane": "stateless",
            "user_id": str(scenario.gate_user_id),
            "job_id": job_id,
        }
    ]
    assert scenario.job_id == job_id
    assert "INSERT INTO users" in executed[0][0]
    assert "true,false" in executed[0][0]
    assert len(executed) == 1  # Only the dedicated gate principal is inserted.
    assert all("INSERT INTO jobs" not in query for query, _values in executed)


@pytest.mark.asyncio
@pytest.mark.parametrize("stored_as_json", [False, True])
async def test_ready_identity_uses_matching_authenticated_nested_vmi(stored_as_json):
    import json
    from uuid import uuid4
    from unittest.mock import AsyncMock

    job, generation, vm, vmi, launcher, pvc = [str(uuid4()) for _ in range(6)]
    context = dict(
        provision_generation=generation,
        vm_uid=vm,
        rootdisk_pvc_uid=pvc,
        active_pod_uid=launcher,
        namespace="agent-vms",
        pod_ip="10.42.0.2",
        ssh_registration_id=str(uuid4()),
        ssh_host_key_fingerprint="SHA256:" + "A" * 43,
        provisioning={"identity": {"vmi_uid": vmi}},
    )
    reply = dict(
        ready=True,
        provision_generation=generation,
        vm_uid=vm,
        rootdisk_pvc_uid=pvc,
        active_pod_uid=launcher,
        vmi_uid=vmi,
        interface_mac="02:00:00:00:00:41",
        provisioning={"vmi_uid": vmi},
    )
    scenario = object.__new__(LiveScenario)
    scenario.namespace = "agent-vms"
    scenario.provisioner = SimpleNamespace(query_status=AsyncMock(return_value=reply))
    body = {"vm": context}
    scenario._row = AsyncMock(
        return_value={"context": json.dumps(body) if stored_as_json else body}
    )
    result = await scenario._ready_identity(acceptance.UUID(job))
    assert result["prior_vmi_uid"] == vmi
    assert result["interface_mac"] == "02:00:00:00:00:41"
    context["provisioning"]["identity"]["vmi_uid"] = str(uuid4())
    scenario._row.return_value = {"context": {"vm": context}}
    assert await scenario._ready_identity(acceptance.UUID(job)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "missing_mac", "malformed_mac", "stale_live_vmi",
        "stale_vmi_receipt", "stale_launcher_receipt",
        "stale_receipt_mac", "missing_receipt",
    ],
)
async def test_ready_identity_binds_controller_mac_and_stored_profile_receipt(change):
    from shared.vm_network_profile import NETWORK_PROFILE
    from uuid import uuid4
    from unittest.mock import AsyncMock

    job, generation, vm_uid, pvc_uid, vmi_uid, launcher_uid = (
        str(uuid4()) for _ in range(6)
    )
    mac = "02:00:00:00:00:41"
    receipt = {
        "profile": NETWORK_PROFILE,
        "provision_generation": generation,
        "vm_uid": vm_uid, "pvc_uid": pvc_uid,
        "vmi_uid": vmi_uid, "launcher_uid": launcher_uid,
        "guest_boot_id": str(uuid4()),
        "cloud_init_instance_id": "first-boot",
        "cloud_init_cached_instance_id": "first-boot",
        "network_file_sha256": "a" * 64,
        "name_only_dhcp": True,
    }
    vm = {
        "provision_generation": generation,
        "vm_uid": vm_uid, "rootdisk_pvc_uid": pvc_uid,
        "active_pod_uid": launcher_uid,
        "pod_ip": "10.42.0.2",
        "ssh_registration_id": str(uuid4()),
        "ssh_host_key_fingerprint": "SHA256:" + "A" * 43,
        "provisioning": {"identity": {"vmi_uid": vmi_uid}},
        "network_profile_evidence": receipt,
    }
    status = {
        "ready": True,
        "provision_generation": generation,
        "vm_uid": vm_uid, "rootdisk_pvc_uid": pvc_uid,
        "active_pod_uid": launcher_uid,
        "vmi_uid": vmi_uid,
        "interface_mac": mac,
        "provisioning": {"vmi_uid": vmi_uid},
    }
    scenario = object.__new__(LiveScenario)
    scenario.namespace = "agent-vms"
    scenario.profiled_fixture = True
    scenario.provisioner = SimpleNamespace(
        query_status=AsyncMock(return_value=status)
    )
    scenario._row = AsyncMock(return_value={"context": {"vm": vm}})
    accepted = await scenario._ready_identity(acceptance.UUID(job))
    assert accepted is not None
    assert accepted["interface_mac"] == mac
    assert accepted["network_profile_receipt"] == receipt

    if change == "missing_mac":
        status.pop("interface_mac")
    elif change == "malformed_mac":
        status["interface_mac"] = "not-a-mac"
    elif change == "stale_live_vmi":
        status["vmi_uid"] = str(uuid4())
    elif change == "stale_vmi_receipt":
        receipt["vmi_uid"] = str(uuid4())
    elif change == "stale_launcher_receipt":
        receipt["launcher_uid"] = str(uuid4())
    elif change == "stale_receipt_mac":
        receipt["interface_mac"] = "02:00:00:00:00:43"
    else:
        vm.pop("network_profile_evidence")
    assert await scenario._ready_identity(acceptance.UUID(job)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("owned", [True, False])
async def test_job_api_probe_requires_fixture_owner_headers(monkeypatch, owned):
    import httpx
    from uuid import uuid4
    from unittest.mock import AsyncMock

    user, job = uuid4(), uuid4()
    calls = []

    def handler(request):
        calls.append(request)
        assert request.headers["X-Internal-Key"] == "gate-internal"
        assert request.headers["X-MCP-User-Id"] == str(user)
        return httpx.Response(200, json={"id": str(job)})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: client)
    monkeypatch.setenv("MCP_INTERNAL_KEY", "gate-internal")
    scenario = object.__new__(LiveScenario)
    scenario.gate_user_id = user
    scenario._row = AsyncMock(return_value={"user_id": user if owned else uuid4()})
    if owned:
        result = await scenario._application_api_evidence(job)
        assert result == {"status_code": 200, "job_visible": True}
        assert len(calls) == 1
    else:
        with pytest.raises(acceptance.AcceptanceFailure, match="does not own"):
            await scenario._application_api_evidence(job)
        assert not calls
        await client.aclose()


# Fixture issuance is operator-only SQL; test its guards against real PostgreSQL.
from tests.test_non_pinned_workspace_lifecycle_real_postgres import (  # noqa: E402
    _schema_applied,  # noqa: F401
    db as _gate_lease_db,
    pg_dsn,  # noqa: F401
)


gate_lease_db = _gate_lease_db


async def _issuance_fixture(db):
    from uuid import uuid4

    scenario = object.__new__(LiveScenario)
    scenario.db, scenario.run_id = db, "lease-issuance-review"
    scenario.gate_user_id = None
    owner = await scenario._ensure_gate_user()
    job = uuid4()
    await db.create_job(
        job_id=job,
        description="gate lease issuance test",
        status="processing",
        origin="lifecycle",
        execution_lane="stateless",
        user_id=str(owner),
        context={"vm_workspace_recovery_acceptance_gate": scenario.run_id},
    )
    return scenario, job


@pytest.mark.asyncio
async def test_fixture_job_has_no_preboot_queue_or_attempt(gate_lease_db):
    from uuid import UUID

    scenario = object.__new__(LiveScenario)
    scenario.db, scenario.run_id = gate_lease_db, "no-preboot-queue"
    scenario.gate_user_id, scenario.job_id = None, None
    job = await scenario._create_job()
    assert isinstance(job, UUID)
    async with gate_lease_db.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM run_queue WHERE unit_id=$1)", job
        )
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM worker_batch_attempts WHERE job_id=$1)", job
        )
        assert (
            await conn.fetchval("SELECT status FROM jobs WHERE id=$1", job)
            == "processing"
        )


@pytest.mark.asyncio
async def test_profiled_fixture_freezes_real_paused_job_snapshot_and_preflight(
    gate_lease_db,
    monkeypatch,
):
    import json
    from orchestrator.services.manifest_execution_snapshot import (
        read_execution,
        srw_snapshot_config,
    )
    from orchestrator.services.vm_provisioner import VMProvisioner
    from shared.vm_network_profile import NETWORK_PROFILE

    image = "registry.example/srw-vm@sha256:" + "a" * 64
    for key, value in {
        "VM_MODE": "same-cluster",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_NETWORK_PROFILE_ENABLED": "true",
        "VM_NETWORK_PROFILE_IMAGE_ALLOWLIST": image,
    }.items():
        monkeypatch.setenv(key, value)
    scenario = object.__new__(LiveScenario)
    scenario.db, scenario.run_id = gate_lease_db, "profiled-fixture"
    scenario.gate_user_id, scenario.job_id = None, None
    scenario.protocol_fixture = True
    scenario.profiled_fixture = True
    scenario.fixture_image = image
    job = await scenario._create_job()
    user = await gate_lease_db.fetchrow(
        "SELECT is_approved,is_admin,can_use_vm FROM users WHERE id=$1",
        scenario.gate_user_id,
    )
    assert user["is_approved"] is True
    assert user["is_admin"] is False
    assert user["can_use_vm"] is True
    assert (
        await gate_lease_db.fetchval(
            "SELECT count(*) FROM capability_grants WHERE scope_kind='user' "
            "AND scope_id=$1 AND key='vm_workspace' AND value_json='true'::jsonb",
            scenario.gate_user_id,
        )
        == 1
    )
    assert (
        await gate_lease_db.fetchval(
            "SELECT count(*) FROM jobs WHERE user_id=$1", scenario.gate_user_id
        )
        == 1
    )
    assert (
        await gate_lease_db.fetchval("SELECT status FROM jobs WHERE id=$1", job)
        == "paused"
    )
    snapshot = await read_execution(gate_lease_db, "Job", str(job))
    _, policy = srw_snapshot_config(snapshot)
    assert policy["workspace"]["backend"] == "vm"
    assert policy["workspace"]["vm"]["image"] == image
    provisioner = VMProvisioner()
    provisioner._db = gate_lease_db
    response = await provisioner.create_vm(
        str(job),
        cpu_cores=2,
        memory="2Gi",
        disk_size="12Gi",
        vm_image=image,
    )
    assert response["status"] == "creation_pending"
    context = json.loads(
        await gate_lease_db.fetchval("SELECT context FROM jobs WHERE id=$1", job)
    )
    preflight = context["vm"]["creation_preflight"]
    assert preflight["request"]["vm_image"] == image
    assert preflight["request"]["network_profile"] == NETWORK_PROFILE
    assert context["_vm_creation_pending"] == response["request_id"]
    assert (
        await gate_lease_db.fetchval(
            "SELECT state FROM run_queue WHERE unit_id=$1",
            job,
        )
        == "done"
    )


async def _profiled_ready_lease_fixture(
    db,
    monkeypatch,
    *,
    observed_vm_uid=None,
    observed_pvc_uid=None,
    expect_qualified=True,
):
    """Seed a completed controller observation after the real preflight."""
    import json
    from uuid import uuid4

    from orchestrator.services.vm_provisioner import VMProvisioner
    from shared.vm_network_profile import NETWORK_PROFILE

    image = "registry.example/srw-vm@sha256:" + "a" * 64
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "true")
    monkeypatch.setenv("VM_NETWORK_PROFILE_IMAGE_ALLOWLIST", image)
    scenario = object.__new__(LiveScenario)
    scenario.db, scenario.run_id = db, "profiled-ready-lease"
    scenario.namespace = "agent-vms"
    scenario.gate_user_id, scenario.job_id = None, None
    scenario.protocol_fixture = scenario.profiled_fixture = True
    scenario.fixture_image = image
    job = await scenario._create_job()
    provisioner = VMProvisioner()
    provisioner._db = db
    ack = await provisioner.create_vm(
        str(job),
        cpu_cores=2,
        memory="2Gi",
        disk_size="12Gi",
        vm_image=image,
    )
    await scenario._require_creation_ack(job, ack)
    context = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", job))
    vm = context["vm"]
    preflight = vm["creation_preflight"]
    generation = scenario.fixture_generation
    vm_uid, pvc_uid, vmi_uid, launcher_uid = (str(uuid4()) for _ in range(4))
    registration = str(uuid4())
    vm.update(
        status="ready",
        vm_uid=vm_uid,
        rootdisk_pvc_uid=pvc_uid,
        active_pod_uid=launcher_uid,
        pod_ip="10.42.0.2",
        ssh_registration_id=registration,
        ssh_host_key_fingerprint="SHA256:" + "A" * 43,
        identity_authenticated=True,
        identity_provision_generation=generation,
        creation_request_id=scenario.fixture_request_id,
        provisioning={"identity": {"vmi_uid": vmi_uid}},
        network_profile_evidence={
            "profile": NETWORK_PROFILE,
            "provision_generation": generation,
            "vm_uid": vm_uid,
            "pvc_uid": pvc_uid,
            "vmi_uid": vmi_uid,
            "launcher_uid": launcher_uid,
            "guest_boot_id": str(uuid4()),
            "cloud_init_instance_id": "i-profiled-gate",
            "cloud_init_cached_instance_id": "i-profiled-gate",
            "network_file_sha256": "a" * 64,
            "name_only_dhcp": True,
        },
    )
    context.pop("_vm_creation_pending")
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1", job, json.dumps(context)
    )
    admission = uuid4()
    await db.execute(
        "INSERT INTO vm_workspace_cleanup_admissions "
        "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) "
        "VALUES($1,'job',$2,$3,'controller_vm_create',$4,'test',clock_timestamp(),'adopted')",
        admission,
        job,
        pvc_uid,
        uuid4(),
    )
    await db.execute(
        "INSERT INTO vm_creation_retries "
        "(request_id,job_id,provision_generation,origin,request_digest,canonical_request,"
        "controller_configuration_digest,execution_id,execution_revision,execution_generation,"
        "admission_deadline,creation_admission_id,state,reason,boot_counted,"
        "observed_vm_uid,observed_pvc_uid,ready_at,resolved_at) "
        "VALUES($1,$2,$3,'initial',$4,$5::jsonb,$6,$7,$8,$9,$10,$11,"
        "'succeeded','creation_adopted',true,$12,$13,clock_timestamp(),clock_timestamp())",
        scenario.fixture_request_id,
        job,
        generation,
        preflight["request_digest"],
        json.dumps(preflight["request"]),
        "sha256:" + "b" * 64,
        preflight["execution_id"],
        preflight["execution_revision"],
        preflight["execution_generation"],
        (
            datetime.fromisoformat(preflight["admission_deadline"])
            if preflight["admission_deadline"]
            else None
        ),
        admission,
        observed_vm_uid or vm_uid,
        observed_pvc_uid or pvc_uid,
    )
    scenario.provisioner = SimpleNamespace(
        query_status=AsyncMock(
            return_value={
                "ready": True,
                "vm_uid": vm_uid,
                "provision_generation": generation,
                "rootdisk_pvc_uid": pvc_uid,
                "active_pod_uid": launcher_uid,
                "vmi_uid": vmi_uid,
                "interface_mac": "02:00:00:00:00:41",
                "provisioning": {"vmi_uid": vmi_uid},
            }
        )
    )
    identity = await (
        scenario._fixture_ready_identity(job)
        if expect_qualified
        else scenario._ready_identity(job)
    )
    assert identity is not None
    return scenario, job, identity


@pytest.mark.asyncio
async def test_profiled_fixture_atomically_claims_preflight_hold_with_native_marker(
    gate_lease_db,
    monkeypatch,
):
    scenario, job, identity = await _profiled_ready_lease_fixture(
        gate_lease_db, monkeypatch
    )
    before = await gate_lease_db.fetchrow(
        "SELECT state,lease_token FROM run_queue WHERE unit_id=$1", job
    )
    assert before["state"] == "done"
    assert await scenario._issue_fixture_lease(job, identity) == 27
    row = await gate_lease_db.fetchrow(
        "SELECT job.status,job.context,queue.state,queue.lease_token,queue.leased_by,"
        "queue.leased_until FROM jobs job JOIN run_queue queue ON queue.unit_id=job.id "
        "WHERE job.id=$1",
        job,
    )
    assert row["status"] == "processing" and row["state"] == "leased"
    assert (
        row["lease_token"] == 27
        and row["leased_by"] == f"vm-recovery-gate:{scenario.run_id}"
    )
    marker = json.loads(row["context"])["_workspace_dispatch_authority"]
    assert marker["dispatch_kind"] == "stateless"
    assert marker["queue_lease_token"] == 27
    assert marker["worker_pod"] == row["leased_by"]
    assert (
        await gate_lease_db.fetchval(
            "SELECT count(*) FROM worker_batch_attempts WHERE job_id=$1 AND lease_token=27",
            job,
        )
        == 1
    )


@pytest.mark.asyncio
async def test_profiled_fixture_native_dispatch_rejection_rolls_back_queue(
    gate_lease_db,
    monkeypatch,
):
    import asyncpg
    from shared import worker_queue

    scenario, job, identity = await _profiled_ready_lease_fixture(
        gate_lease_db, monkeypatch
    )
    before = await gate_lease_db.fetchrow(
        "SELECT * FROM run_queue WHERE unit_id=$1",
        job,
    )
    # Deliberately corrupt only the in-test claimant's marker: the database
    # trigger must reject it after the queue update, rolling back both writes.
    monkeypatch.setattr(
        worker_queue,
        "_CAS_JOB_SQL",
        worker_queue._CAS_JOB_SQL.replace(
            "'worker_pod', $3::text", "'worker_pod', ($3::text || '-wrong')"
        ),
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await scenario._issue_fixture_lease(job, identity)
    assert (
        await gate_lease_db.fetchrow(
            "SELECT * FROM run_queue WHERE unit_id=$1",
            job,
        )
        == before
    )
    assert (
        await gate_lease_db.fetchval("SELECT status FROM jobs WHERE id=$1", job)
        == "paused"
    )
    assert (
        await gate_lease_db.fetchval(
            "SELECT count(*) FROM worker_batch_attempts WHERE job_id=$1", job
        )
        == 0
    )


@pytest.mark.asyncio
async def test_profiled_fixture_rechecks_live_runtime_before_using_ready_receipt(
    gate_lease_db,
    monkeypatch,
):
    from uuid import uuid4

    scenario, job, _identity = await _profiled_ready_lease_fixture(
        gate_lease_db, monkeypatch
    )
    original = scenario.provisioner.query_status.return_value
    scenario.provisioner.query_status.side_effect = [
        original,
        {**original, "vm_uid": str(uuid4())},
    ]
    assert await scenario._fixture_ready_identity(job) is None
    assert (
        await gate_lease_db.fetchval(
            "SELECT state FROM run_queue WHERE unit_id=$1", job
        )
        == "done"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["observed_vm_uid", "observed_pvc_uid"])
async def test_profiled_fixture_rejects_different_adopted_vm_or_pvc_before_marker_and_lease(
    gate_lease_db,
    monkeypatch,
    field,
):
    from uuid import uuid4

    scenario, job, identity = await _profiled_ready_lease_fixture(
        gate_lease_db,
        monkeypatch,
        **{field: uuid4(), "expect_qualified": False},
    )
    before = await gate_lease_db.fetchrow(
        "SELECT job.status,job.context,queue.* FROM jobs job "
        "JOIN run_queue queue ON queue.unit_id=job.id WHERE job.id=$1",
        job,
    )
    assert await scenario._fixture_ready_identity(job) is None
    with pytest.raises(acceptance.AcceptanceFailure, match="could not be issued"):
        await scenario._issue_fixture_lease(job, identity)
    assert (
        await gate_lease_db.fetchrow(
            "SELECT job.status,job.context,queue.* FROM jobs job "
            "JOIN run_queue queue ON queue.unit_id=job.id WHERE job.id=$1",
            job,
        )
        == before
    )
    assert (
        await gate_lease_db.fetchval(
            "SELECT count(*) FROM worker_batch_attempts WHERE job_id=$1",
            job,
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["observed_vm_uid", "observed_pvc_uid"])
async def test_profiled_fixture_missing_adoption_uid_cannot_authorize_ready(
    gate_lease_db,
    monkeypatch,
    field,
):
    scenario, job, identity = await _profiled_ready_lease_fixture(
        gate_lease_db, monkeypatch
    )
    current = dict(
        await gate_lease_db.fetchrow(
            "SELECT id,status,execution_lane,assigned_agent_id,user_id,context,"
            "config_override,freeze_data,lease_expires_at FROM jobs WHERE id=$1",
            job,
        )
    )
    retry = dict(
        await gate_lease_db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1",
            acceptance.UUID(scenario.fixture_request_id),
        )
    )
    retry.pop(field)
    assert not scenario._fixture_authority_matches(job, current, retry, identity)
    retry[field] = None
    assert not scenario._fixture_authority_matches(job, current, retry, identity)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "changed_identity",
        "prior_attempt",
        "parked",
        "leased",
        "prior_token",
        "control_input",
        "wrong_kind",
        "workspace_image",
        "missing_receipt",
    ],
)
async def test_profiled_fixture_rejects_changed_or_used_hold_without_mutation(
    gate_lease_db,
    monkeypatch,
    change,
):
    import json
    from uuid import uuid4

    scenario, job, identity = await _profiled_ready_lease_fixture(
        gate_lease_db, monkeypatch
    )
    if change == "changed_identity":
        identity = {**identity, "prior_vmi_uid": str(uuid4())}
    elif change == "prior_attempt":
        await gate_lease_db.execute(
            "INSERT INTO worker_batch_attempts(job_id,lease_token,claimed_attempt) VALUES($1,4,1)",
            job,
        )
    elif change == "parked":
        await gate_lease_db.execute(
            "UPDATE run_queue SET state='parked' WHERE unit_id=$1", job
        )
    elif change == "leased":
        await gate_lease_db.execute(
            "UPDATE run_queue SET state='leased',lease_token=1,leased_by='other',"
            "leased_until=clock_timestamp()+interval '5 minutes' WHERE unit_id=$1",
            job,
        )
    elif change == "prior_token":
        await gate_lease_db.execute(
            "UPDATE run_queue SET lease_token=1 WHERE unit_id=$1", job
        )
    elif change == "control_input":
        await gate_lease_db.execute(
            "UPDATE run_queue SET control_input_seq=1 WHERE unit_id=$1", job
        )
    elif change == "wrong_kind":
        await gate_lease_db.execute(
            "UPDATE run_queue SET unit_kind='session_turn' WHERE unit_id=$1", job
        )
    elif change == "workspace_image":
        await gate_lease_db.execute(
            "UPDATE jobs SET config_override=jsonb_set(config_override,"
            "'{workspace,vm,image}',to_jsonb($2::text)) WHERE id=$1",
            job,
            "registry.example/other@sha256:" + "b" * 64,
        )
    else:
        context = json.loads(
            await gate_lease_db.fetchval("SELECT context FROM jobs WHERE id=$1", job)
        )
        context["vm"].pop("network_profile_evidence")
        await gate_lease_db.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1", job, json.dumps(context)
        )
    before = await gate_lease_db.fetchrow(
        "SELECT job.status,job.context,queue.state,queue.lease_token,queue.leased_by "
        "FROM jobs job JOIN run_queue queue ON queue.unit_id=job.id WHERE job.id=$1",
        job,
    )
    with pytest.raises(acceptance.AcceptanceFailure, match="could not be issued"):
        await scenario._issue_fixture_lease(job, identity)
    assert (
        await gate_lease_db.fetchrow(
            "SELECT job.status,job.context,queue.state,queue.lease_token,queue.leased_by "
            "FROM jobs job JOIN run_queue queue ON queue.unit_id=job.id WHERE job.id=$1",
            job,
        )
        == before
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed",
    [
        None,
        "queue_leased",
        "queue_queued",
        "queue_parked",
        "queue_done",
        "queue_other_kind",
        "owner",
        "status",
        "marker",
        "missing_marker",
        "lane",
    ],
)
async def test_fixture_lease_issuance_requires_exact_unused_gate_job(
    gate_lease_db, changed
):
    import json
    from uuid import uuid4

    db = gate_lease_db
    scenario, job = await _issuance_fixture(db)
    async with db.acquire() as conn:
        if changed and changed.startswith("queue_"):
            state = changed.removeprefix("queue_")
            await conn.execute(
                "INSERT INTO run_queue(unit_id,unit_kind,state,lease_token,leased_by,"
                "leased_until,input_seq,consumed_seq,attempts_since_completion) "
                "VALUES($1,$2,$3,28,'another-worker',clock_timestamp()-interval '1 minute',4,2,3)",
                job,
                "session_turn" if state == "other_kind" else "worker_batch",
                "leased" if state == "other_kind" else state,
            )
        elif changed == "owner":
            other = uuid4()
            await conn.execute(
                "INSERT INTO users(id,display_name) VALUES($1,'another owner')", other
            )
            await conn.execute("UPDATE jobs SET user_id=$2 WHERE id=$1", job, other)
        elif changed == "lane":
            await conn.execute(
                "UPDATE jobs SET execution_lane='pinned' WHERE id=$1", job
            )
        elif changed == "status":
            await conn.execute("UPDATE jobs SET status='cancelled' WHERE id=$1", job)
        elif changed in {"marker", "missing_marker"}:
            context = (
                {}
                if changed == "missing_marker"
                else {"vm_workspace_recovery_acceptance_gate": "another-run"}
            )
            await conn.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                job,
                json.dumps(context),
            )
        before_job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", job)
        before_queue = await conn.fetch("SELECT * FROM run_queue WHERE unit_id=$1", job)
        before_attempts = await conn.fetch(
            "SELECT * FROM worker_batch_attempts WHERE job_id=$1", job
        )
    if changed is None:
        assert await scenario._issue_fixture_lease(job) == 27
    else:
        with pytest.raises(acceptance.AcceptanceFailure, match="could not be issued"):
            await scenario._issue_fixture_lease(job)
    async with db.acquire() as conn:
        assert await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", job) == before_job
        queue = await conn.fetch("SELECT * FROM run_queue WHERE unit_id=$1", job)
        attempts = await conn.fetch(
            "SELECT * FROM worker_batch_attempts WHERE job_id=$1", job
        )
        now = await conn.fetchval("SELECT clock_timestamp()")
    if changed is not None:
        assert queue == before_queue and attempts == before_attempts
        return
    assert len(queue) == len(attempts) == 1
    assert queue[0]["state"] == "leased"
    assert queue[0]["lease_token"] == attempts[0]["lease_token"] == 27
    assert queue[0]["leased_by"] == "vm-recovery-gate:lease-issuance-review"
    assert queue[0]["attempts_since_completion"] == attempts[0]["claimed_attempt"] == 1
    assert queue[0]["input_seq"] == 1 and queue[0]["consumed_seq"] == 0
    assert now < queue[0]["leased_until"] <= now + timedelta(minutes=5)
    with pytest.raises(acceptance.AcceptanceFailure, match="could not be issued"):
        await scenario._issue_fixture_lease(job)
    async with db.acquire() as conn:
        assert (
            await conn.fetch("SELECT * FROM run_queue WHERE unit_id=$1", job) == queue
        )
        assert (
            await conn.fetch("SELECT * FROM worker_batch_attempts WHERE job_id=$1", job)
            == attempts
        )


@pytest.mark.asyncio
async def test_fixture_issuance_occurs_once_after_ssh_setup_not_on_admission_replay(
    monkeypatch,
):
    from uuid import uuid4
    from unittest.mock import AsyncMock

    class ReplayReached(Exception):
        pass

    job, token, identity = uuid4(), 27, {"fixture": "ready"}
    events = []
    scenario = object.__new__(LiveScenario)
    scenario.run_id = "lease-issuance-placement"
    scenario._create_job = AsyncMock(return_value=job)
    scenario.provisioner = SimpleNamespace(create_vm=AsyncMock(return_value=True))
    scenario._wait = AsyncMock(return_value=identity)
    scenario._application_api_evidence = AsyncMock(return_value={"job_visible": True})
    scenario._row = AsyncMock(return_value={})

    async def ssh(_identity, path, value):
        assert _identity is identity and value
        events.append(path.rsplit("/", 1)[1])

    async def issue(actual_job):
        assert actual_job == job
        events.append("issue")
        return token

    admissions = []

    async def admit(**kwargs):
        admissions.append(kwargs)
        events.append("admit")
        if len(admissions) == 2:
            raise ReplayReached
        return SimpleNamespace(operation_id=uuid4())

    scenario._ssh_file = ssh
    monkeypatch.setattr(scenario, "_issue_fixture_lease", issue, raising=False)
    scenario._admit = admit
    with pytest.raises(ReplayReached):
        await scenario.execute()
    assert events == ["marker", "checkpoint", "issue", "admit", "admit"]
    assert admissions[0] == admissions[1]
    assert admissions[0]["lease_token"] == token


@pytest.mark.asyncio
async def test_fixture_issuance_rolls_back_queue_if_attempt_already_exists(
    gate_lease_db,
):
    import asyncpg

    db = gate_lease_db
    scenario, job = await _issuance_fixture(db)
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO worker_batch_attempts(job_id,lease_token,claimed_attempt) VALUES($1,27,1)",
            job,
        )
        before = await conn.fetch(
            "SELECT * FROM worker_batch_attempts WHERE job_id=$1", job
        )
    with pytest.raises((acceptance.AcceptanceFailure, asyncpg.UniqueViolationError)):
        await scenario._issue_fixture_lease(job)
    async with db.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM run_queue WHERE unit_id=$1)", job
        )
        assert (
            await conn.fetch("SELECT * FROM worker_batch_attempts WHERE job_id=$1", job)
            == before
        )


@pytest.mark.asyncio
async def test_fixture_issuance_rechecks_job_after_row_lock_wait(gate_lease_db):
    db = gate_lease_db
    scenario, job = await _issuance_fixture(db)
    async with db.acquire() as blocker:
        async with blocker.transaction():
            await blocker.execute("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job)
            task = asyncio.create_task(scenario._issue_fixture_lease(job))
            await asyncio.sleep(0.15)
            assert not task.done()
            await blocker.execute("UPDATE jobs SET status='cancelled' WHERE id=$1", job)
        with pytest.raises(acceptance.AcceptanceFailure, match="could not be issued"):
            await task
    async with db.acquire() as conn:
        assert (
            await conn.fetchval("SELECT status FROM jobs WHERE id=$1", job)
            == "cancelled"
        )
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM run_queue WHERE unit_id=$1)", job
        )
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM worker_batch_attempts WHERE job_id=$1)", job
        )


@pytest.mark.asyncio
async def test_fixture_concurrent_issuance_creates_one_lease_and_attempt(gate_lease_db):
    import asyncpg

    db = gate_lease_db
    scenario, job = await _issuance_fixture(db)
    results = await asyncio.gather(
        scenario._issue_fixture_lease(job),
        scenario._issue_fixture_lease(job),
        return_exceptions=True,
    )
    assert results.count(27) == 1
    assert (
        sum(
            isinstance(
                result, (acceptance.AcceptanceFailure, asyncpg.UniqueViolationError)
            )
            for result in results
        )
        == 1
    )
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM run_queue WHERE unit_id=$1 AND lease_token=27 AND state='leased'",
                job,
            )
            == 1
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM worker_batch_attempts WHERE job_id=$1 AND lease_token=27 AND claimed_attempt=1",
                job,
            )
            == 1
        )
