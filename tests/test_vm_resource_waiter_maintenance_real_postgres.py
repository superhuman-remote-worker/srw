"""Bounded resource wait maintenance with actual owner/policy locking."""

import asyncio
from copy import deepcopy
import hashlib
import json
from uuid import uuid4

import pytest

from tests.test_vm_resource_reservation_store_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    environment,
    waiter,
    decide,
)

db = _db_fixture


def maintenance(store):
    from orchestrator.services.vm_resource_waiter_maintenance import (
        VMResourceWaiterMaintenance,
    )

    return VMResourceWaiterMaintenance(store)


async def row(db, request):
    return dict(
        await db.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
            request["request_id"],
        )
    )


async def attention(db, request):
    await db.execute(
        "UPDATE vm_creation_retries SET state='reconciling' WHERE request_id=$1",
        request["request_id"],
    )
    await db.execute(
        "UPDATE vm_creation_retries SET state='attention',reason='transport_attention' WHERE request_id=$1",
        request["request_id"],
    )


@pytest.mark.asyncio
async def test_cursor_advances_across_replicas_and_wraps_after_crash(db):
    store, inventory, _ = await environment(db)
    requests = [await waiter(db, inventory) for _ in range(5)]
    maint = maintenance(store)
    batches = await asyncio.gather(
        maint.candidates(limit=2), maintenance(store).candidates(limit=2)
    )
    assert {v for batch in batches for v in batch} == {
        str(r["request_id"]) for r in requests[:4]
    }
    assert await maintenance(store).candidates(limit=2) == [
        str(requests[4]["request_id"])
    ]
    assert await maint.candidates(limit=2) == []
    assert await maint.candidates(limit=2) == [
        str(r["request_id"]) for r in requests[:2]
    ]
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_waiters WHERE state<>'waiting' OR revision<>1"
        )
        == 0
    )
    for limit in (0, 101, True):
        with pytest.raises(ValueError, match="maintenance_limit"):
            await maint.candidates(limit=limit)


@pytest.mark.asyncio
async def test_park_and_resume_preserve_fairness_and_are_idempotent(db):
    store, inventory, _ = await environment(db)
    request = await waiter(db, inventory)
    await db.execute(
        "UPDATE vm_resource_waiters SET bypasses=3,protected_order=7 WHERE request_id=$1",
        request["request_id"],
    )
    prior = await row(db, request)
    await attention(db, request)
    maint = maintenance(store)
    assert await maint.maintain(request_id=str(request["request_id"])) == {
        "action": "parked",
        "reason": "retry_attention",
    }
    parked = await row(db, request)
    assert parked["revision"] == prior["revision"] + 1
    assert await maint.maintain(request_id=str(request["request_id"])) == {
        "action": "parked",
        "reason": "retry_attention",
    }
    assert await row(db, request) == parked
    await db.execute(
        "UPDATE vm_creation_retries SET state='queued',reason=NULL WHERE request_id=$1",
        request["request_id"],
    )
    assert (await maint.maintain(request_id=str(request["request_id"])))[
        "action"
    ] == "reactivated"
    current = await row(db, request)
    for key in ("enqueued_at", "bypasses", "protected_order", "priority", "request_id"):
        assert current[key] == prior[key]
    assert (
        current["state"] == "waiting" and current["revision"] == parked["revision"] + 1
    )
    assert (await maint.maintain(request_id=str(request["request_id"])))[
        "action"
    ] == "unchanged"
    assert await row(db, request) == current
    assert (
        await db.fetchval(
            "SELECT admission_sequence FROM vm_resource_admission_policy WHERE cluster_id=$1",
            inventory.cluster_id,
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cause", ["cancelled", "generation", "execution", "deadline", "policy"]
)
async def test_permanently_invalid_request_cancels_without_spending_attempts(db, cause):
    store, inventory, _ = await environment(db)
    request = await waiter(db, inventory, timeout=3600)
    expected = {
        "cancelled": "job_cancelled",
        "generation": "generation_changed",
        "execution": "execution_manifest_changed",
        "deadline": "admission_deadline_expired",
        "policy": "policy_superseded",
    }[cause]
    if cause == "cancelled":
        await db.execute(
            "UPDATE jobs SET status='cancelled' WHERE id=$1", request["job_id"]
        )
    elif cause == "generation":
        from tests._previous_release_seed import seed_previous_release_row

        async with db.acquire() as conn:
            await seed_previous_release_row(
                conn,
                "jobs",
                "UPDATE jobs SET context=jsonb_set(context,'{vm,provision_generation}',$2::jsonb) WHERE id=$1",
                request["job_id"],
                json.dumps(str(uuid4())),
            )
    elif cause == "execution":
        await db.execute(
            "UPDATE srw_execution_specs SET revision='other' WHERE work_id=$1",
            request["job_id"],
        )
    elif cause == "deadline":
        # A second immediate-deadline request is admitted while still live.
        request = await waiter(db, inventory, timeout=2)
        await db.execute(
            "SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM ($1::timestamptz-clock_timestamp())))+0.05)",
            request["admission_deadline"],
        )
    else:
        # Installation of a new exact policy is outside maintenance; model its
        # already committed result and construct the matching current reader.
        config = deepcopy(store.policy_document)
        config["policy"]["fairness"]["priorityAgingSeconds"] += 1
        document = json.dumps(config, sort_keys=True, separators=(",", ":"))
        inventory.policy_digest = (
            "sha256:" + hashlib.sha256(document.encode()).hexdigest()
        )
        await db.execute(
            "UPDATE vm_resource_admission_policy SET revision=revision+1,document=$1::jsonb,policy_digest=$2 WHERE cluster_id=$3",
            document,
            inventory.policy_digest,
            inventory.cluster_id,
        )
        store = type(store)(
            db, inventory=inventory, policy_document=config, policy_revision=2
        )
    before = await db.fetchrow(
        "SELECT backoff_attempt,boot_counted FROM vm_creation_retries WHERE request_id=$1",
        request["request_id"],
    )
    maint = maintenance(store)
    assert await maint.maintain(request_id=str(request["request_id"])) == {
        "action": "cancelled",
        "reason": expected,
    }
    current = await row(db, request)
    assert current["state"] == "cancelled"
    assert await maint.maintain(request_id=str(request["request_id"])) == {
        "action": "unchanged",
        "reason": "terminal_waiter",
    }
    assert await row(db, request) == current
    retry = await db.fetchrow(
        "SELECT state,backoff_attempt,boot_counted FROM vm_creation_retries WHERE request_id=$1",
        request["request_id"],
    )
    assert retry["state"] == "cancel_requested"
    assert (retry["backoff_attempt"], retry["boot_counted"]) == (
        before["backoff_attempt"],
        before["boot_counted"],
    )
    assert (
        await db.fetchval(
            "SELECT admission_sequence FROM vm_resource_admission_policy WHERE cluster_id=$1",
            inventory.cluster_id,
        )
        == 0
    )


@pytest.mark.asyncio
async def test_held_cancelled_job_never_releases_its_reservation(db):
    store, inventory, _ = await environment(db)
    request = await waiter(db, inventory)
    assert (await decide(store, request))["action"] == "admitted"
    reservation = dict(
        await db.fetchrow(
            "SELECT * FROM vm_resource_reservations WHERE request_id=$1",
            request["request_id"],
        )
    )
    await db.execute(
        "UPDATE jobs SET status='cancelled' WHERE id=$1", request["job_id"]
    )
    assert await maintenance(store).maintain(request_id=str(request["request_id"])) == {
        "action": "unchanged",
        "reason": "held_or_admitted",
    }
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_resource_reservations WHERE request_id=$1",
                request["request_id"],
            )
        )
        == reservation
    )
    assert (await row(db, request))["state"] == "admitted"


@pytest.mark.asyncio
async def test_invalid_protected_prefix_does_not_starve_valid_owner(db):
    store, inventory, _ = await environment(db)
    requests = [await waiter(db, inventory) for _ in range(4)]
    for order, request in enumerate(requests[:3]):
        await attention(db, request)
        await db.execute(
            "UPDATE vm_resource_waiters SET bypasses=1,protected_order=$2 WHERE request_id=$1",
            request["request_id"],
            order,
        )
    assert await decide(store, requests[3]) == {
        "action": "nominate",
        "request_id": str(requests[0]["request_id"]),
    }
    for _ in range(2):
        maint = maintenance(store)  # a new process continues the durable cursor
        for request_id in await maint.candidates(limit=2):
            await maint.maintain(request_id=request_id)
    assert (
        await db.fetchval(
            "SELECT admission_sequence FROM vm_resource_admission_policy WHERE cluster_id=$1",
            inventory.cluster_id,
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_waiters WHERE state='parked' AND bypasses=1 AND protected_order IS NOT NULL"
        )
        == 3
    )
    assert (await decide(store, requests[3]))["action"] == "admitted"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["cleanup", "recovery", "control", "queue"])
async def test_transient_authority_holds_park_then_restore_exact_waiter(db, kind):
    store, inventory, _ = await environment(db)
    request = await waiter(db, inventory)
    await db.execute(
        "UPDATE vm_resource_waiters SET bypasses=3,protected_order=2 WHERE request_id=$1",
        request["request_id"],
    )
    before = await row(db, request)
    if kind == "cleanup":
        from orchestrator.services.vm_workspace_recovery_store import (
            VMWorkspaceRecoveryStore,
        )

        cleanup = VMWorkspaceRecoveryStore(db)
        permit = await cleanup.acquire_cleanup_permit(
            owner_kind="job",
            owner_id=request["job_id"],
            pvc_uid=None,
            request_id=uuid4(),
            source="test_cleanup",
            intent_digest="fixture",
        )
        assert permit.allowed
    elif kind == "recovery":
        recovery_id = await db.fetchval(
            "INSERT INTO vm_workspace_recoveries(owner_kind,owner_id,workspace_contract_digest,cluster_name,phase,reason_code,"
            "provision_generation,namespace,vm_uid,prior_vmi_uid,prior_launcher_uid,root_pvc_uid) "
            "VALUES('job',$1,'fixture','cluster','paused_attention','workspace_identity_conflict',$2,'srw',$3,$4,$5,$6) RETURNING id",
            request["job_id"],
            request["provision_generation"],
            uuid4(),
            uuid4(),
            uuid4(),
            uuid4(),
        )
    elif kind == "control":
        await db.execute(
            "UPDATE jobs SET context=context||'{\"_completion_control_claim\":{}}'::jsonb WHERE id=$1",
            request["job_id"],
        )
    else:
        await db.execute(
            "INSERT INTO run_queue(unit_id,unit_kind,state,lease_token) VALUES($1,'worker_batch','leased',1)",
            request["job_id"],
        )
    reason = {
        "cleanup": "workspace_cleanup_held",
        "recovery": "workspace_recovery_held",
        "control": "job_control_busy",
        "queue": "worker_lease_active",
    }[kind]
    maint = maintenance(store)
    assert await maint.maintain(request_id=str(request["request_id"])) == {
        "action": "parked",
        "reason": reason,
    }
    if kind == "cleanup":
        assert await cleanup.complete_cleanup_permit(
            permit.admission_id, outcome="completed"
        )
    elif kind == "recovery":
        await db.execute(
            "UPDATE vm_workspace_recoveries SET phase='cancelled',resolved_at=clock_timestamp() WHERE id=$1",
            recovery_id,
        )
    elif kind == "control":
        await db.execute(
            "UPDATE jobs SET context=context-'_completion_control_claim' WHERE id=$1",
            request["job_id"],
        )
    else:
        await db.execute(
            "UPDATE run_queue SET state='done' WHERE unit_id=$1", request["job_id"]
        )
    assert (await maint.maintain(request_id=str(request["request_id"])))[
        "action"
    ] == "reactivated"
    after = await row(db, request)
    assert all(
        after[key] == before[key]
        for key in ("enqueued_at", "bypasses", "protected_order", "priority")
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["issued", "observed", "rejected"])
async def test_even_terminal_request_with_any_effect_stays_parked(db, state):
    store, inventory, _ = await environment(db)
    request = await waiter(db, inventory)
    await db.execute(
        "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,effect_kind,carrier_uid,carrier_namespace,carrier_intent,state,evidence,resolved_at) "
        "VALUES($1,$2,1,'cloud_init',$3,'srw','{}',$4,$5::jsonb,CASE WHEN $4='issued' THEN NULL ELSE clock_timestamp() END)",
        uuid4(),
        request["request_id"],
        uuid4(),
        state,
        json.dumps({} if state == "issued" else {"fixture": state}),
    )
    await db.execute(
        "UPDATE jobs SET status='cancelled' WHERE id=$1", request["job_id"]
    )
    before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_creation_effects WHERE request_id=$1",
            request["request_id"],
        )
    )
    assert await maintenance(store).maintain(request_id=str(request["request_id"])) == {
        "action": "parked",
        "reason": "creation_effect_present",
    }
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_effects WHERE request_id=$1",
                request["request_id"],
            )
        )
        == before
    )


async def waiting_on(db, pattern, task=None):
    deadline = asyncio.get_running_loop().time() + 5
    while asyncio.get_running_loop().time() < deadline:
        if task is not None and task.done():
            pytest.fail(f"maintenance finished before expected lock: {task.result()}")
        if await db.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE $1)",
            pattern,
        ):
            return
        await asyncio.sleep(0.02)
    active = await db.fetch(
        "SELECT query,wait_event_type,wait_event FROM pg_stat_activity WHERE state='active'"
    )
    pytest.fail(f"maintenance did not reach expected lock: {active}")


@pytest.mark.asyncio
async def test_job_lock_precedes_policy_and_hint_is_not_authority(db):
    store, inventory, _ = await environment(db)
    request = await waiter(db, inventory)
    maint = maintenance(store)
    assert await maint.candidates(limit=1) == [str(request["request_id"])]
    async with db.acquire() as blocker, blocker.transaction():
        await blocker.fetchrow(
            "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", request["job_id"]
        )
        task = asyncio.create_task(
            maint.maintain(request_id=str(request["request_id"]))
        )
        await waiting_on(db, "SELECT * FROM jobs WHERE id=ANY%FOR UPDATE")
        # The target waits without acquiring policy or blocking another scan.
        assert await asyncio.wait_for(maintenance(store).candidates(limit=1), 1) == []
        await blocker.execute(
            "UPDATE srw_execution_specs SET revision='changed-while-waiting' WHERE work_id=$1",
            request["job_id"],
        )
    assert await asyncio.wait_for(task, 5) == {
        "action": "cancelled",
        "reason": "execution_manifest_changed",
    }


@pytest.mark.asyncio
async def test_deadline_is_checked_after_policy_wait(db):
    store, inventory, _ = await environment(db)
    request = await waiter(db, inventory, timeout=2)
    async with db.acquire() as blocker, blocker.transaction():
        await blocker.fetchrow(
            "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1 FOR UPDATE",
            inventory.cluster_id,
        )
        task = asyncio.create_task(
            maintenance(store).maintain(request_id=str(request["request_id"]))
        )
        await waiting_on(db, "SELECT%vm_resource_admission_policy%FOR UPDATE%", task)
        await blocker.execute(
            "SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM ($1::timestamptz-clock_timestamp())))+0.05)",
            request["admission_deadline"],
        )
    assert await asyncio.wait_for(task, 5) == {
        "action": "cancelled",
        "reason": "admission_deadline_expired",
    }


@pytest.mark.asyncio
async def test_drain_maintenance_preserves_hold_and_refuses_stale_policy(db):
    store, inventory, _ = await environment(db)
    request = await waiter(db, inventory)
    await attention(db, request)
    await db.execute(
        "UPDATE vm_resource_admission_policy SET mode='drain' WHERE cluster_id=$1",
        inventory.cluster_id,
    )
    assert (await maintenance(store).maintain(request_id=str(request["request_id"])))[
        "action"
    ] == "parked"
    assert (await decide(store, request))["action"] == "unavailable"
    await db.execute(
        "UPDATE vm_resource_admission_policy SET revision=revision+1 WHERE cluster_id=$1",
        inventory.cluster_id,
    )
    before = await row(db, request)
    assert await maintenance(store).maintain(request_id=str(request["request_id"])) == {
        "action": "unavailable",
        "reason": "resource_policy_changed",
    }
    assert await row(db, request) == before


@pytest.mark.asyncio
async def test_malformed_runtime_metadata_is_parked_without_consuming_fairness(db):
    store, inventory, _ = await environment(db)
    request = await waiter(db, inventory)
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','[]'::jsonb) WHERE id=$1",
        request["job_id"],
    )
    assert await maintenance(store).maintain(request_id=str(request["request_id"])) == {
        "action": "parked",
        "reason": "runtime_identity_unproven",
    }
    assert (
        await db.fetchval(
            "SELECT admission_sequence FROM vm_resource_admission_policy WHERE cluster_id=$1",
            inventory.cluster_id,
        )
        == 0
    )


@pytest.mark.asyncio
async def test_waiting_row_with_existing_reservation_is_never_cancelled(db):
    store, inventory, snapshot = await environment(db)
    request = await waiter(db, inventory)
    node = snapshot["nodes"][0]
    from uuid import UUID

    await db.execute(
        "INSERT INTO vm_resource_nodes(cluster_id,node_uid,node_name) VALUES($1,$2,$3)",
        inventory.cluster_id,
        UUID(node["uid"]),
        node["name"],
    )
    await db.execute(
        "INSERT INTO vm_resource_reservations(id,request_id,revision,cluster_id,policy_digest,node_uid,node_name,cpu_millicores,memory_bytes,kvm_devices,snapshot_id,snapshot_digest) "
        "SELECT $1,w.request_id,1,w.cluster_id,w.policy_digest,$2,$3,w.cpu_millicores,w.memory_bytes,w.kvm_devices,s.snapshot_id,s.digest "
        "FROM vm_resource_waiters w JOIN vm_resource_inventory_snapshots s ON s.cluster_id=w.cluster_id AND s.policy_digest=w.policy_digest WHERE w.request_id=$4 AND s.snapshot_id=$5",
        uuid4(),
        UUID(node["uid"]),
        node["name"],
        request["request_id"],
        UUID(snapshot["snapshot_id"]),
    )
    await db.execute(
        "UPDATE jobs SET status='cancelled' WHERE id=$1", request["job_id"]
    )
    before = await row(db, request)
    assert before["state"] == "waiting"
    assert await maintenance(store).maintain(request_id=str(request["request_id"])) == {
        "action": "unchanged",
        "reason": "held_or_admitted",
    }
    assert await row(db, request) == before
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
            request["request_id"],
        )
        == "reserved"
    )


@pytest.mark.asyncio
async def test_cleanup_added_during_owner_wait_is_observed_before_policy(db):
    store, inventory, _ = await environment(db)
    request = await waiter(db, inventory)
    async with db.acquire() as blocker, blocker.transaction():
        await blocker.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery:job:{request['job_id']}",
        )
        task = asyncio.create_task(
            maintenance(store).maintain(request_id=str(request["request_id"]))
        )
        await waiting_on(db, "SELECT pg_advisory_xact_lock%", task)
        await blocker.execute(
            "INSERT INTO vm_workspace_cleanup_admissions(id,owner_kind,owner_id,source,request_id,intent_digest) VALUES($1,'job',$2,'test_cleanup',$3,'fixture')",
            uuid4(),
            request["job_id"],
            uuid4(),
        )
    assert await asyncio.wait_for(task, 5) == {
        "action": "parked",
        "reason": "workspace_cleanup_held",
    }


async def break_lineage(db, request):
    execution_id = request["execution_id"]
    owner = await db.fetchval(
        "INSERT INTO users(display_name,is_approved) VALUES('lineage proof',TRUE) RETURNING id"
    )
    instance_id = uuid4()
    await db.execute(
        "INSERT INTO srw_workspace_instances(id,owner_id,recipe,revision,pvc_name,pvc_uid,generation,execution_id,backend_state) "
        "VALUES($1,$2,$3::jsonb,'fixture','fixture-pvc',$4,2,$5,'{}')",
        instance_id,
        owner,
        json.dumps({"backend": "vm", "retention": "Retain"}),
        str(uuid4()),
        execution_id,
    )
    await db.execute(
        "INSERT INTO srw_execution_workspace_bindings(execution_id,instance_id) VALUES($1,$2)",
        execution_id,
        instance_id,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", [False, True])
async def test_broken_lineage_quarantine_cannot_cancel_or_block_other_owners(
    db, terminal
):
    store, inventory, _ = await environment(db)
    bad, good = await waiter(db, inventory), await waiter(db, inventory)
    await db.execute(
        "UPDATE vm_resource_waiters SET bypasses=1,protected_order=0 WHERE request_id=$1",
        bad["request_id"],
    )
    before = await row(db, bad)
    await break_lineage(db, bad)
    if terminal:
        await db.execute(
            "UPDATE jobs SET status='cancelled' WHERE id=$1", bad["job_id"]
        )
    retry_before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1", bad["request_id"]
        )
    )
    assert await maintenance(store).maintain(request_id=str(bad["request_id"])) == {
        "action": "parked",
        "reason": "creation_attachment_lineage_unproven",
    }
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                bad["request_id"],
            )
        )
        == retry_before
    )
    parked = await row(db, bad)
    assert all(
        parked[key] == before[key]
        for key in ("enqueued_at", "bypasses", "protected_order")
    )
    assert (await decide(store, good))["action"] == "admitted"
    assert (await decide(store, bad))["action"] == "unavailable"
    assert (await maintenance(store).maintain(request_id=str(bad["request_id"])))[
        "action"
    ] == "parked"
    assert await row(db, bad) == parked
    if not terminal:
        # Restore this fixture's original no-binding contract. Quarantine alone
        # cannot reactivate it; a new complete authority pass must succeed.
        await db.execute(
            "DELETE FROM srw_execution_workspace_bindings WHERE execution_id=$1",
            bad["execution_id"],
        )
        assert (await maintenance(store).maintain(request_id=str(bad["request_id"])))[
            "action"
        ] == "reactivated"
        restored = await row(db, bad)
        assert all(
            restored[key] == before[key]
            for key in ("enqueued_at", "bypasses", "protected_order")
        )


@pytest.mark.asyncio
async def test_broken_lineage_quarantine_never_changes_held_reservation(db):
    store, inventory, _ = await environment(db)
    request = await waiter(db, inventory)
    assert (await decide(store, request))["action"] == "admitted"
    before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_resource_reservations WHERE request_id=$1",
            request["request_id"],
        )
    )
    await break_lineage(db, request)
    assert await maintenance(store).maintain(request_id=str(request["request_id"])) == {
        "action": "unchanged",
        "reason": "held_or_admitted",
    }
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_resource_reservations WHERE request_id=$1",
                request["request_id"],
            )
        )
        == before
    )
