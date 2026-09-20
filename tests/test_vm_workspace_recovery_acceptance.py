"""Contracts for the in-image real VM recovery fault scenario."""

from __future__ import annotations

from contextlib import asynccontextmanager
import asyncio
from datetime import datetime, timedelta, timezone
import inspect
from types import SimpleNamespace
from unittest.mock import ANY

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

    job_id, lease_token = await scenario._create_job()

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
    assert lease_token == 27
    assert scenario.job_id == job_id
    assert "INSERT INTO users" in executed[0][0]
    assert "true,false" in executed[0][0]
    assert ["run_queue" in query for query, _values in executed] == [False, True, False]
    assert ["worker_batch_attempts" in query for query, _values in executed] == [
        False,
        False,
        True,
    ]
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
    context["provisioning"]["identity"]["vmi_uid"] = str(uuid4())
    scenario._row.return_value = {"context": {"vm": context}}
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


# The lease refresh is operator-fixture SQL: use real PostgreSQL to prove its
# predicate instead of teaching a fake database the same conditional update.
from tests.test_non_pinned_workspace_lifecycle_real_postgres import (  # noqa: E402
    _schema_applied,  # noqa: F401
    db as _gate_lease_db,
    pg_dsn,  # noqa: F401
)


gate_lease_db = _gate_lease_db


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", [None, "token", "owner", "parked", "queued", "done", "unit_kind"]
)
async def test_fixture_lease_refresh_requires_exact_current_synthetic_owner(
    gate_lease_db, changed
):
    from uuid import uuid4

    db = gate_lease_db
    job, run_id, token = uuid4(), "lease-refresh-review", 27
    owner = f"vm-recovery-gate:{run_id}"
    state = changed if changed in {"parked", "queued", "done"} else "leased"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs(id,description,status,execution_lane) "
            "VALUES($1,'gate lease test','created','stateless')",
            job,
        )
        await conn.execute(
            "INSERT INTO run_queue(unit_id,unit_kind,state,lease_token,leased_by,"
            "leased_until,input_seq,consumed_seq,attempts_since_completion) "
            "VALUES($1,$2,$3,$4,$5,clock_timestamp()-interval '1 minute',1,0,3)",
            job,
            "session_turn" if changed == "unit_kind" else "worker_batch",
            state,
            token + 1 if changed == "token" else token,
            "another-worker" if changed == "owner" else owner,
        )
        before = dict(
            await conn.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job)
        )
    scenario = object.__new__(LiveScenario)
    scenario.db, scenario.run_id = db, run_id
    if changed is None:
        await scenario._refresh_fixture_lease(job, token)
    else:
        with pytest.raises(
            acceptance.AcceptanceFailure, match="could not be refreshed"
        ):
            await scenario._refresh_fixture_lease(job, token)
    async with db.acquire() as conn:
        after = dict(
            await conn.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job)
        )
        now = await conn.fetchval("SELECT clock_timestamp()")
    if changed is None:
        assert before["leased_until"] < now < after["leased_until"]
        assert after["leased_until"] <= now + timedelta(minutes=5)
        after["leased_until"] = before["leased_until"]
    assert after == before  # Includes token, claimant, state and attempt count.


@pytest.mark.asyncio
async def test_fixture_refresh_occurs_once_after_ssh_setup_not_on_admission_replay(
    monkeypatch,
):
    from uuid import uuid4
    from unittest.mock import AsyncMock

    class ReplayReached(Exception):
        pass

    job, token, identity = uuid4(), 27, {"fixture": "ready"}
    events = []
    scenario = object.__new__(LiveScenario)
    scenario.run_id = "lease-refresh-placement"
    scenario._create_job = AsyncMock(return_value=(job, token))
    scenario.provisioner = SimpleNamespace(create_vm=AsyncMock(return_value=True))
    scenario._wait = AsyncMock(return_value=identity)
    scenario._application_api_evidence = AsyncMock(return_value={"job_visible": True})
    scenario._row = AsyncMock(return_value={})

    async def ssh(_identity, path, value):
        assert _identity is identity and value
        events.append(path.rsplit("/", 1)[1])

    async def refresh(actual_job, actual_token):
        assert (actual_job, actual_token) == (job, token)
        events.append("refresh")

    admissions = []

    async def admit(**kwargs):
        admissions.append(kwargs)
        events.append("admit")
        if len(admissions) == 2:
            raise ReplayReached
        return SimpleNamespace(operation_id=uuid4())

    scenario._ssh_file = ssh
    monkeypatch.setattr(scenario, "_refresh_fixture_lease", refresh, raising=False)
    scenario._admit = admit
    with pytest.raises(ReplayReached):
        await scenario.execute()
    assert events == ["marker", "checkpoint", "refresh", "admit", "admit"]
    assert admissions[0] == admissions[1]
