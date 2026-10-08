"""M1 regression: an issued VM Worker must not replay an uncertain tool.

Native source/admission/claim/reaper/bundle paths are real PostgreSQL. Runtime
coordinates are fixture evidence, not a live VM or physical-stop qualification.
"""

import asyncio
import json
from uuid import UUID

import pytest

from orchestrator.services import run_queue_reaper
from agent.api import turn_executor
from shared import worker_queue
from shared.worker_execution_hold import hold_container_worker_attempt
from shared.workspace_contract import workspace_runtime_authority_digest
from tests.test_vm_pre_ssh_stop_real_postgres import seeded_stop
from tests.test_vm_job_retained_resume_real_postgres import (  # noqa: F401
    _base_db,
    _db_fixture,
    _schema_applied,
    _pre_ssh_db,
    _retention_db,
    db as _resume_db,
    pg_dsn,
    postgres_db_fixture,
    pre_ssh_schema,
    retention_schema,
    resume_schema,
    whole_schema,
)

db = _resume_db


@pytest.mark.parametrize(
    ("backend", "provisioner", "recovery", "error", "expected"),
    [
        ("vm", None, "false", {"type": "workspace_unavailable"}, True),
        ("vm", None, "true", {"type": "workspace_unavailable"}, False),
        ("vm", None, "false", {"type": "provider_unavailable"}, False),
        ("sandbox", "k8s", "true", {"type": "workspace_unavailable"}, True),
        ("sandbox", "docker", "false", {"type": "workspace_unavailable"}, False),
    ],
)
def test_worker_typed_workspace_report_selects_vm_hold_only_when_recovery_off(
    monkeypatch, backend, provisioner, recovery, error, expected
):
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", recovery)
    assert (
        turn_executor._uncertain_worker_workspace_report(
            completion_commands_enabled=True,
            workspace_backend=backend,
            workspace_provisioner=provisioner,
            goal_achieved=False,
            error=error,
        )
        is expected
    )


@pytest.mark.asyncio
async def test_initial_ready_cancel_cannot_admit_purge_without_preservation(
    db, monkeypatch
):
    from orchestrator.services.vm_job_cancel_retention import acquire_cancel_retention
    from orchestrator.services.vm_provisioner import VMTeardownIdentity
    from orchestrator.services.vm_workspace_recovery_store import (
        VMWorkspaceRecoveryStore,
        acquire_vm_cleanup_permit,
    )

    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", "true")
    state = await seeded_stop(db, cleanup_source=None, retiring=False)
    owner = UUID(state["job_id"])
    await db.execute(
        "UPDATE vm_creation_retries SET ready_at=clock_timestamp() WHERE job_id=$1",
        owner,
    )
    await db.execute(
        "UPDATE jobs SET status='created',config_override=$2::jsonb,"
        "context=jsonb_set(context,'{vm,status}','\"ready\"') WHERE id=$1",
        owner,
        json.dumps({"workspace": {"backend": "vm"}}),
    )
    cancelled, _queue_changed = await db.cancel_stateless_job(str(owner))
    assert cancelled is True
    assert (
        await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner)
        == "done"
    )
    identity = VMTeardownIdentity(
        state["generation"], state["frozen"]["vm_uid"], state["frozen"]["pvc_uid"]
    )
    store = VMWorkspaceRecoveryStore(db)
    selected = await acquire_cancel_retention(
        store, job_id=str(owner), identity=identity
    )
    # This is the actual archive selector/fallback, with native admission. No
    # signed keep preflight was supplied, so absence of preservation must hold.
    permit = selected
    if selected is None:
        permit = await acquire_vm_cleanup_permit(
            store,
            owner_kind="job",
            owner_id=str(owner),
            identity=identity,
            source="job_terminal_vm_release",
            purge_disk=True,
        )
    assert permit is not None
    assert (
        not permit.allowed or permit.parent_cleanup["intent"]["purge_disk"] is False
    ), (
        "Initial durably Ready owner Cancel admitted exact True purge without "
        f"preservation: selector={selected}, allowed={permit.allowed}, "
        f"purge_disk={permit.parent_cleanup['intent']['purge_disk']}"
    )


async def issued_vm_claim(db, *, authorize=True):
    state = await seeded_stop(db, cleanup_source=None, retiring=False)
    owner = UUID(state["job_id"])
    await db.execute(
        "UPDATE vm_creation_retries SET ready_at=clock_timestamp() WHERE job_id=$1",
        owner,
    )
    await db.execute(
        "UPDATE jobs SET status='created',config_override=$2::jsonb,"
        "context=jsonb_set(context,'{vm}',context->'vm'||$3::jsonb) WHERE id=$1",
        owner,
        json.dumps({"workspace": {"backend": "vm"}}),
        json.dumps(
            {
                "status": "ready",
                "ssh_host": "10.42.0.91",
                "ssh_port": 22,
                "ssh_ready_source": "provisioner_probe",
            }
        ),
    )
    await worker_queue.enqueue_worker_batch(db, job_id=owner)
    claim = await worker_queue.claim_worker_batch(db, pod_name="executor-original")
    assert claim is not None and claim.unit_id == owner
    if authorize:
        job = await db.get_job(str(owner))
        digest = workspace_runtime_authority_digest(job, vm_mode="same-cluster")
        assert digest is not None
        async with db.acquire() as conn:
            assert await worker_queue.record_worker_bundle_authorized(
                conn,
                job_id=owner,
                lease_token=claim.lease_token,
                authority_digest=digest,
                vm_mode="same-cluster",
                vm_binding_required=True,
            )
        assert (
            await db.fetchval(
                "SELECT count(*) FROM vm_job_worker_delivery_bindings WHERE job_id=$1",
                owner,
            )
            == 1
        )
    return owner, claim


@pytest.mark.asyncio
async def test_issued_vm_loss_cannot_authorize_successor_without_tool_disposition(
    db, monkeypatch
):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")
    monkeypatch.setenv("VM_WORKSPACE_REPLACEMENT_RECOVERY_ENABLED", "false")
    owner, claim = await issued_vm_claim(db)
    # Expiry represents executor loss. Do not claim process zero or disposition.
    await db.execute(
        "UPDATE run_queue SET leased_until=clock_timestamp()-interval '1 second' "
        "WHERE unit_id=$1 AND lease_token=$2",
        owner,
        claim.lease_token,
    )
    async with db.acquire() as conn:
        await run_queue_reaper.reap_cycle(conn, grace_seconds=0)
    after = dict(await db.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", owner))
    held_job = await db.get_job(str(owner))
    binding = await db.fetchrow(
        "SELECT * FROM vm_job_worker_delivery_bindings WHERE job_id=$1 AND lease_token=$2",
        owner,
        claim.lease_token,
    )
    held_context = json.loads(held_job["context"])
    marker = held_context["_worker_execution_hold"]
    assert marker["phase"] == "pending"
    assert marker["lease_token"] == claim.lease_token
    assert marker["revoked_lease_token"] == claim.lease_token + 1
    assert marker["worker_pod"] == "executor-original"
    assert marker["executor_pod_uid"] is None
    assert marker["bundle_authority_digest"] == binding["authority_digest"]
    for key in ("request_id", "provision_generation", "vm_uid", "pvc_uid"):
        assert marker[key] == str(binding[key])
    assert held_job["status"] == "paused"
    assert held_context["_operator_pause_hold"]["hold_id"] == marker["hold_id"]
    delay = await db.fetchval(
        "SELECT greatest(0,extract(epoch FROM run_after-clock_timestamp()))::float8 "
        "FROM run_queue WHERE unit_id=$1",
        owner,
    )
    assert delay < 30
    await asyncio.sleep(delay + 0.02)
    successor = await worker_queue.claim_worker_batch(db, pod_name="executor-new")
    successor_authorized = False
    if successor is not None:
        latest = await db.get_job(str(owner))
        async with db.acquire() as conn:
            successor_authorized = await worker_queue.record_worker_bundle_authorized(
                conn,
                job_id=owner,
                lease_token=successor.lease_token,
                authority_digest=workspace_runtime_authority_digest(
                    latest, vm_mode="same-cluster"
                ),
                vm_mode="same-cluster",
                vm_binding_required=True,
            )
    assert (after["state"], successor, successor_authorized) == (
        "parked",
        None,
        False,
    ), (
        "Issued VM loss became runnable without a predecessor tool disposition: "
        f"state={after['state']}, successor_token="
        f"{successor.lease_token if successor else None}, "
        f"bundle_authorized={successor_authorized}"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ("never_authorized", "not_applicable"),
        ("recovery_enabled", "not_applicable"),
        ("malformed_recovery_flag", "blocked"),
        ("missing_vm_binding", "blocked"),
        ("stale_worker_pod", "blocked"),
        ("accepted_terminal_command", "blocked"),
    ],
)
async def test_vm_worker_hold_preserves_prebundle_native_and_stale_boundaries(
    db, monkeypatch, case, expected
):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv(
        "VM_WORKSPACE_RECOVERY_ENABLED",
        "true"
        if case == "recovery_enabled"
        else "invalid"
        if case == "malformed_recovery_flag"
        else "false",
    )
    owner, claim = await issued_vm_claim(
        db, authorize=case not in {"never_authorized", "missing_vm_binding"}
    )
    if case == "missing_vm_binding":
        job = await db.get_job(str(owner))
        digest = workspace_runtime_authority_digest(job, vm_mode="same-cluster")
        assert digest is not None
        async with db.acquire() as conn:
            assert await worker_queue.record_worker_bundle_authorized(
                conn,
                job_id=owner,
                lease_token=claim.lease_token,
                authority_digest=digest,
                vm_mode="same-cluster",
                vm_binding_required=False,
            )
    if case == "stale_worker_pod":
        await db.execute(
            "UPDATE run_queue SET leased_by='foreign-worker' WHERE unit_id=$1",
            owner,
        )
    if case == "accepted_terminal_command":
        await db.execute(
            "INSERT INTO job_completion_commands(job_id,report_seq,client_report_id,"
            "payload,payload_digest,accepted_lease_token,origin,requested_by,"
            "deadline_at,code_version) VALUES($1,1,$2,'{}'::jsonb,$3,$4,"
            "'agent','worker-hold-test',clock_timestamp()+interval '1 minute','test')",
            owner,
            UUID("88888888-8888-4888-8888-888888888888"),
            "sha256:" + "a" * 64,
            claim.lease_token,
        )
    async with db.acquire() as conn:
        decision = await hold_container_worker_attempt(
            conn,
            job_id=owner,
            lease_token=claim.lease_token,
            reason="post_bundle_executor_loss",
        )
    assert decision == expected
    assert (
        await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner)
        == "leased"
    )
    assert "_worker_execution_hold" not in json.loads(
        (await db.get_job(str(owner)))["context"]
    )
