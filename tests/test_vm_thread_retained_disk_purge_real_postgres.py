"""Sequential native End preserves compute history and requires fresh disk proof."""

import json
from uuid import UUID

import pytest

from orchestrator.services.vm_provisioner import VMTeardownResult
from tests.test_vm_resource_thread_cleanup_real_postgres import (
    PhysicalStop,
    operations,
    retiring,
    db as _db,
    thread_schema,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    pg_dsn as _pg_dsn,
)


db = _db
pg_dsn = _pg_dsn


class RetainedDisk(PhysicalStop):
    """The second actuator sees released compute and a surviving exact disk."""

    def __init__(self, db, case):
        super().__init__(db, case)
        self.effects = []
        self.observations = []

    async def release_vm_captured(self, owner, identity, **kwargs):
        self.effects.append(kwargs["parent_cleanup"])
        if not kwargs["purge_disk"]:
            return await super().release_vm_captured(owner, identity, **kwargs)
        assert self.stopped
        assert owner == str(self.case["thread_id"])
        assert identity.vm_uid == self.case["vm_uid"]
        assert identity.rootdisk_pvc_uid == self.case["pvc_uid"]
        assert (
            await self.db.fetchval(
                "SELECT state FROM vm_resource_reservations WHERE id=$1",
                UUID(self.case["admitted"]["reservation_id"]),
            )
            == "released"
        )
        self.purged = True
        return VMTeardownResult("completed", True)

    async def attest_vm_cleanup_stop(self, candidate):
        self.observations.append(candidate)
        return await super().attest_vm_cleanup_stop(candidate)


async def predecessor_snapshot(db, case):
    # Forward Job-only extensions must remain NULL on historical thread rows.
    # Compare the original data separately from these newly added columns.
    assert not await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM vm_creation_retries r WHERE "
        "to_jsonb(r)->>'job_retained_resume_id' IS NOT NULL OR "
        "to_jsonb(r)->>'job_retained_resume_admitted_xact_id' IS NOT NULL)"
    )
    assert not await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM vm_workspace_cleanup_admissions r "
        "WHERE source='pinned_thread_retirement' AND "
        "to_jsonb(r)->>'retained_resume_admitted_xact_id' IS NOT NULL)"
    )
    result = {}
    for table in (
        "vm_creation_retries",
        "vm_resource_reservations",
        "vm_resource_waiters",
        "vm_resource_thread_cleanup_authorities",
        "vm_resource_thread_cleanup_stops",
    ):
        result[table] = await db.fetch(
            f"SELECT (to_jsonb(r)-'thread_retained_resume_id'"
            f"-'job_retained_resume_id'-'job_retained_resume_admitted_xact_id')::text AS row "
            f"FROM {table} r ORDER BY to_jsonb(r)::text"
        )
    result["cleanup"] = await db.fetch(
        "SELECT (to_jsonb(r)-'retained_resume_admitted_xact_id')::text AS row "
        "FROM vm_workspace_cleanup_admissions r "
        "WHERE source='pinned_thread_retirement' ORDER BY id"
    )
    return result


async def settled(db, monkeypatch, *, ready=True):
    case = await retiring(db, monkeypatch, ready=ready)
    await db.execute(
        "INSERT INTO thread_messages(thread_id,role,content) VALUES($1,'user','keep my work')",
        case["thread_id"],
    )
    physical = RetainedDisk(db, case)
    await operations(db, physical).cleanup_pinned_thread_retirement(
        case["retirement"], cleanup_agent_pod=False
    )
    assert physical.stopped and physical.purged is False
    assert await db.settle_pinned_thread_retirement(
        str(case["thread_id"]),
        token=case["retirement"]["token"],
        generation=case["retirement"]["generation"],
    )
    assert (
        await db.fetchval(
            "SELECT content FROM thread_messages WHERE thread_id=$1", case["thread_id"]
        )
        == "keep my work"
    )
    return case, physical


async def permanent_begin(db, case):
    retirement = await db.begin_pinned_thread_retirement(
        str(case["thread_id"]), permanent=True
    )
    assert retirement["state"] == "pending", retirement
    assert retirement["context"]["entry_status"] == "ended"
    assert retirement["context"]["agent_id"] is None
    assert await db.authorize_pinned_thread_retirement(
        str(case["thread_id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    return retirement


@pytest.mark.asyncio
@pytest.mark.parametrize("ready", [True, False])
async def test_native_soft_then_permanent_end_preserves_old_compute_history(
    db, monkeypatch, ready
):
    case, physical = await settled(db, monkeypatch, ready=ready)
    original = await predecessor_snapshot(db, case)
    permanent = await permanent_begin(db, case)
    await operations(db, physical).cleanup_pinned_thread_retirement(
        permanent, cleanup_agent_pod=False
    )
    assert physical.purged is True
    assert len(physical.observations) == 2
    assert physical.effects[0]["admission_id"] != physical.effects[1]["admission_id"]
    assert (
        physical.effects[1]["intent"]["source"] == "pinned_thread_retained_disk_purge"
    )
    assert await predecessor_snapshot(db, case) == original
    await db.delete_thread(
        str(case["thread_id"]),
        expected_runtime_retirement_token=permanent["token"],
        expected_runtime_generation=permanent["generation"],
    )
    assert await db.get_thread(str(case["thread_id"])) is None
    assert (
        await db.fetchval(
            "SELECT count(*) FROM thread_messages WHERE thread_id=$1", case["thread_id"]
        )
        == 0
    )
    assert await predecessor_snapshot(db, case) == original
    receipt = json.loads(
        await db.fetchval(
            "SELECT deletion_receipt FROM vm_thread_creation_owners WHERE thread_id=$1",
            case["thread_id"],
        )
    )
    assert receipt["kind"] == "retained_vm_disk_purge"
    assert (
        receipt["compute_cleanup_admission_id"] == physical.effects[0]["admission_id"]
    )
    assert receipt["disk_cleanup_admission_id"] == physical.effects[1]["admission_id"]
    assert receipt["compute_retirement_token"] == case["retirement"]["token"]
    assert receipt["retirement_token"] == permanent["token"]


async def admit(db, physical, permanent):
    ops = operations(db, physical)
    identity = ops._captured_vm_recovery_identity(permanent["context"], permanent=True)
    return await ops._admit_vm_cleanup(
        str(physical.case["thread_id"]), identity, purge_disk=True, retirement=permanent
    )


async def corrupt(db, case, fault):
    """Inject impossible durable drift as a privileged writer; production must refuse."""
    from uuid import uuid4

    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        statements = {
            "missing_stop": "DELETE FROM vm_resource_thread_cleanup_stops",
            "wrong_stop": 'UPDATE vm_resource_thread_cleanup_stops SET stop_evidence=stop_evidence||\'{"pvc_disposition":"purged"}\'',
            "missing_zero": "DELETE FROM managed_repository_process_zero_receipts",
            "wrong_zero": "UPDATE managed_repository_process_zero_receipts SET runtime_incarnation=gen_random_uuid()::text",
            "missing_outcome": "DELETE FROM thread_runtime_retirement_outcomes",
            "wrong_actor": "UPDATE thread_runtime_retirement_outcomes SET agent_id=gen_random_uuid()",
            "wrong_digest": "UPDATE vm_resource_thread_cleanup_authorities SET intent_digest='sha256:wrong'",
            "wrong_release": "UPDATE vm_resource_reservations SET release_evidence='{}'",
            "wrong_revision": "UPDATE vm_resource_thread_cleanup_authorities SET reservation_revision=reservation_revision+1",
            "wrong_waiter": "UPDATE vm_resource_waiters SET state='cancelled'",
            "inverse_actor": "INSERT INTO agents(config_name,thread_id) SELECT config_name,id FROM threads",
            "source_actor": "UPDATE vm_creation_retries SET thread_agent_id=gen_random_uuid(),thread_attach_token=gen_random_uuid()",
            "source_generation": "UPDATE vm_creation_retries SET thread_runtime_generation=gen_random_uuid()",
            "source_revision": "UPDATE vm_creation_retries SET revision=revision+1",
            "source_resumed": "UPDATE vm_creation_retries SET origin='resume'",
            "current_token": "UPDATE threads SET runtime_retirement_token=gen_random_uuid()",
            "current_generation": "UPDATE threads SET runtime_generation=gen_random_uuid()",
            "unauthorized": "UPDATE threads SET runtime_retirement_authorized_at=NULL",
            "current_actor": "UPDATE threads SET runtime_attach_token=gen_random_uuid()",
            "current_context": "UPDATE threads SET runtime_retirement_context=jsonb_set(runtime_retirement_context,'{entry_status}','\"active\"')",
            "current_source": "UPDATE threads SET metadata=jsonb_set(metadata,'{vm,creation_request_id}',to_jsonb(gen_random_uuid()::text))",
            "current_vm": "UPDATE threads SET metadata=jsonb_set(metadata,'{vm,vm_uid}',to_jsonb(gen_random_uuid()::text))",
            "current_pvc": "UPDATE threads SET metadata=jsonb_set(metadata,'{vm,rootdisk_pvc_uid}',to_jsonb(gen_random_uuid()::text))",
            "unauthenticated_backing": "UPDATE threads SET metadata=jsonb_set(metadata,'{vm,identity_authenticated}','false')",
            "captured_pvc": "UPDATE threads SET runtime_retirement_context=jsonb_set(runtime_retirement_context,'{vm,rootdisk_pvc_uid}',to_jsonb(gen_random_uuid()::text))",
        }
        if fault in statements:
            await conn.execute(statements[fault])
        elif fault == "newer_reservation":
            await conn.execute(
                "INSERT INTO vm_resource_reservations SELECT (jsonb_populate_record(NULL::vm_resource_reservations,"
                "to_jsonb(r)||jsonb_build_object('id',$2::uuid,'revision',r.revision+1))).* "
                "FROM vm_resource_reservations r WHERE request_id=$1",
                case["request_id"],
                uuid4(),
            )
        elif fault == "open_cleanup":
            await conn.execute(
                "INSERT INTO vm_workspace_cleanup_admissions(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,parent_admission_id) "
                "VALUES($1,'thread',$2,$3,'controller_rootdisk_delete',$4,'sha256:conflict',"
                "(SELECT cleanup_admission_id FROM vm_resource_thread_cleanup_authorities LIMIT 1))",
                uuid4(),
                case["thread_id"],
                UUID(case["pvc_uid"]),
                uuid4(),
            )
        elif fault == "recovery_pin":
            recovery = uuid4()
            await conn.execute(
                "INSERT INTO vm_workspace_recoveries(id,owner_kind,owner_id,workspace_contract_digest,cluster_name,phase,reason_code) "
                "VALUES($1,'thread',$2,'test','test','paused_attention','workspace_runtime_not_ready')",
                recovery,
                case["thread_id"],
            )
            await conn.execute(
                "INSERT INTO vm_workspace_recovery_retention_pins(recovery_id,pvc_uid,provision_generation) VALUES($1,$2,$3)",
                recovery,
                UUID(case["pvc_uid"]),
                case["generation"],
            )
        elif fault == "successor":
            await conn.execute(
                "INSERT INTO vm_resource_recovery_successors(recovery_id,reservation_id,ordinal,owner_id,provision_generation,vm_uid,root_pvc_uid,"
                "prior_vmi_uid,prior_launcher_uid,successor_vmi_uid,successor_launcher_uid,stop_receipt_digest,final_attestation_digest) "
                "VALUES($1,$2,1,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11)",
                uuid4(),
                UUID(case["admitted"]["reservation_id"]),
                case["thread_id"],
                case["generation"],
                UUID(case["vm_uid"]),
                UUID(case["pvc_uid"]),
                UUID(case["vmi_uid"]),
                UUID(case["launcher_uid"]),
                uuid4(),
                uuid4(),
                "sha256:" + "1" * 64,
            )
        elif fault == "access":
            await conn.execute(
                "INSERT INTO vm_idle_access_leases(id,owner_kind,owner_id,provision_generation,vm_uid,kind,claimed_by,expires_at,max_expires_at) "
                "VALUES($1,'thread',$2,$3,$4,'ssh','test',clock_timestamp()+interval '5 minutes',clock_timestamp()+interval '6 minutes')",
                uuid4(),
                case["thread_id"],
                case["generation"],
                UUID(case["vm_uid"]),
            )
        else:
            raise AssertionError(fault)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "missing_stop",
        "wrong_stop",
        "missing_zero",
        "wrong_zero",
        "missing_outcome",
        "wrong_actor",
        "wrong_digest",
        "wrong_release",
        "wrong_revision",
        "wrong_waiter",
        "source_actor",
        "source_generation",
        "source_resumed",
        "inverse_actor",
        "recovery_pin",
        "successor",
        "current_token",
        "current_generation",
        "unauthorized",
        "current_actor",
        "current_context",
        "current_source",
        "current_vm",
        "current_pvc",
        "unauthenticated_backing",
        "captured_pvc",
        "newer_reservation",
        "open_cleanup",
        "access",
    ],
)
async def test_incomplete_or_changed_lineage_refuses_before_disk_effect(
    db, monkeypatch, fault
):
    import asyncpg
    from shared.vm_resource_admission import ResourceAdmissionError

    case, physical = await settled(db, monkeypatch)
    permanent = await permanent_begin(db, case)
    await corrupt(db, case, fault)
    try:
        permit = await admit(db, physical, permanent)
    except (asyncpg.CheckViolationError, ResourceAdmissionError):
        permit = None
    assert permit is None or not permit.allowed
    assert physical.purged is False
    assert len(physical.effects) == 1
    assert (
        await db.fetchval("SELECT count(*) FROM vm_thread_retained_disk_purge_receipts")
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "source_revision",
        "recovery_pin",
        "source_actor",
        "current_token",
        "current_source",
        "current_pvc",
        "current_generation",
        "access",
        "open_cleanup",
    ],
)
async def test_drift_during_fresh_probe_cannot_accept_purge_or_debit(
    db, monkeypatch, fault
):
    import asyncio
    import asyncpg
    from shared.vm_resource_admission import ResourceAdmissionError
    from orchestrator.services.vm_workspace_recovery_store import (
        complete_vm_cleanup_permit,
    )

    case, physical = await settled(db, monkeypatch)
    permanent = await permanent_begin(db, case)
    permit = await admit(db, physical, permanent)
    original = await predecessor_snapshot(db, case)
    physical.purged = True
    original_probe = physical.attest_vm_cleanup_stop
    entered, finish = asyncio.Event(), asyncio.Event()

    async def blocked(candidate):
        entered.set()
        await finish.wait()
        return await original_probe(candidate)

    physical.attest_vm_cleanup_stop = blocked
    task = asyncio.create_task(
        complete_vm_cleanup_permit(
            operations(db, physical).dependencies.recovery_store,
            permit,
            outcome="completed",
            provisioner=physical,
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), 5)
        # Acquiring these rows NOWAIT proves controller I/O holds no owner/source transaction.
        async with db.acquire() as conn, conn.transaction():
            await conn.fetchrow(
                "SELECT id FROM threads WHERE id=$1 FOR UPDATE NOWAIT",
                case["thread_id"],
            )
            await conn.fetchrow(
                "SELECT request_id FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE NOWAIT",
                case["request_id"],
            )
        await corrupt(db, case, fault)
        finish.set()
        with pytest.raises((asyncpg.CheckViolationError, ResourceAdmissionError)):
            await task
    finally:
        finish.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert (
        await db.fetchval("SELECT count(*) FROM vm_thread_retained_disk_purge_receipts")
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT completed_at FROM vm_workspace_cleanup_admissions WHERE id=$1",
            permit.admission_id,
        )
        is None
    )
    current = await predecessor_snapshot(db, case)
    assert current["vm_resource_reservations"] == original["vm_resource_reservations"]
    assert current["vm_resource_waiters"] == original["vm_resource_waiters"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "pvc_disposition",
        "controller_authenticated",
        "owner_id",
        "pvc_uid",
        "vm_uid",
        "provision_generation",
        "vmi_uid",
        "launcher_uid",
        "vm_absent",
        "vmi_absent",
        "launcher_absent",
        "same_generation_replacement",
    ],
)
async def test_new_receipt_requires_exact_fresh_purge_observation(
    db, monkeypatch, fault
):
    import asyncpg
    from shared.vm_resource_admission import ResourceAdmissionError
    from orchestrator.services.vm_workspace_recovery_store import (
        complete_vm_cleanup_permit,
    )

    case, physical = await settled(db, monkeypatch)
    permanent = await permanent_begin(db, case)
    permit = await admit(db, physical, permanent)
    physical.purged = True
    original_probe = physical.attest_vm_cleanup_stop

    async def wrong(candidate):
        proof = await original_probe(candidate)
        if fault == "none":
            return None
        proof[fault] = {
            "pvc_disposition": "retained",
            "controller_authenticated": False,
            "vm_absent": False,
            "vmi_absent": False,
            "launcher_absent": False,
            "same_generation_replacement": True,
        }.get(fault, "different-identity")
        return proof

    physical.attest_vm_cleanup_stop = wrong
    with pytest.raises((asyncpg.CheckViolationError, ResourceAdmissionError)):
        await complete_vm_cleanup_permit(
            operations(db, physical).dependencies.recovery_store,
            permit,
            outcome="completed",
            provisioner=physical,
        )
    assert len(physical.observations) == 2
    assert (
        await db.fetchval("SELECT count(*) FROM vm_thread_retained_disk_purge_receipts")
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT completed_at FROM vm_workspace_cleanup_admissions WHERE id=$1",
            permit.admission_id,
        )
        is None
    )


@pytest.mark.asyncio
async def test_raw_completion_and_append_only_guards(db, monkeypatch):
    import asyncpg
    from orchestrator.services.vm_workspace_recovery_store import (
        complete_vm_cleanup_permit,
    )

    case, physical = await settled(db, monkeypatch)
    permanent = await permanent_begin(db, case)
    permit = await admit(db, physical, permanent)
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),outcome='completed' WHERE id=$1",
            permit.admission_id,
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute("DELETE FROM threads WHERE id=$1", case["thread_id"])
    physical.purged = True
    await complete_vm_cleanup_permit(
        operations(db, physical).dependencies.recovery_store,
        permit,
        outcome="completed",
        provisioner=physical,
    )
    for table, key in (
        ("vm_thread_retained_disk_purge_authorities", "intent_digest"),
        ("vm_thread_retained_disk_purge_receipts", "purge_evidence"),
        ("vm_resource_thread_cleanup_authorities", "intent_digest"),
        ("vm_resource_thread_cleanup_stops", "stop_evidence"),
    ):
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(f"UPDATE {table} SET {key}={key}")
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(f"DELETE FROM {table}")


@pytest.mark.asyncio
async def test_response_loss_restart_simultaneous_completion_and_endpoint_replay(
    db, monkeypatch
):
    import asyncio
    from orchestrator.services.vm_workspace_recovery_store import (
        complete_vm_cleanup_permit,
    )

    case, physical = await settled(db, monkeypatch)
    permanent = await permanent_begin(db, case)
    original = await predecessor_snapshot(db, case)
    original_release = physical.release_vm_captured

    async def lose_delete(*args, **kwargs):
        await original_release(*args, **kwargs)
        raise TimeoutError("delete response lost")

    physical.release_vm_captured = lose_delete
    with pytest.raises(TimeoutError):
        await operations(db, physical).cleanup_pinned_thread_retirement(
            permanent, cleanup_agent_pod=False
        )
    assert physical.purged
    permit = await admit(db, physical, permanent)
    original_probe = physical.attest_vm_cleanup_stop

    async def lose_probe(candidate):
        await original_probe(candidate)
        raise TimeoutError("probe response lost")

    physical.attest_vm_cleanup_stop = lose_probe
    with pytest.raises(TimeoutError):
        await complete_vm_cleanup_permit(
            operations(db, physical).dependencies.recovery_store,
            permit,
            outcome="completed",
            provisioner=physical,
        )
    assert (
        await db.fetchval("SELECT count(*) FROM vm_thread_retained_disk_purge_receipts")
        == 0
    )
    physical.attest_vm_cleanup_stop = original_probe
    # Fresh operation/store instances represent orchestrator restart. Both see
    # one immutable authority and converge without another compute transition.
    await asyncio.gather(
        *[
            complete_vm_cleanup_permit(
                operations(db, physical).dependencies.recovery_store,
                permit,
                outcome="completed",
                provisioner=physical,
            )
            for _ in range(2)
        ]
    )
    assert (
        await db.fetchval("SELECT count(*) FROM vm_thread_retained_disk_purge_receipts")
        == 1
    )
    observed = len(physical.observations)
    # Commit-response loss retries the persisted receipt, without re-observing.
    await complete_vm_cleanup_permit(
        operations(db, physical).dependencies.recovery_store,
        permit,
        outcome="completed",
        provisioner=physical,
    )
    assert len(physical.observations) == observed
    physical.release_vm_captured = original_release
    await operations(db, physical).cleanup_pinned_thread_retirement(
        permanent, cleanup_agent_pod=False
    )
    assert "vm" not in json.loads(
        await db.fetchval("SELECT metadata FROM threads WHERE id=$1", case["thread_id"])
    )
    await operations(db, physical).cleanup_pinned_thread_retirement(
        permanent, cleanup_agent_pod=False
    )
    assert len(physical.observations) == observed
    assert await predecessor_snapshot(db, case) == original


@pytest.mark.asyncio
@pytest.mark.parametrize("ready", [True, False])
async def test_pre0295_settled_then_reconciled_compute_qualifies(
    db, monkeypatch, ready
):
    from tests.test_vm_resource_thread_cleanup_real_postgres import old_settled_end

    case, old = await old_settled_end(db, monkeypatch, ready=ready)
    result = await operations(db, old).reconcile_settled_vm_resources()
    assert [entry["state"] for entry in result["results"]] == ["released"]
    assert (
        await db.fetchval(
            "SELECT retirement_context FROM vm_resource_thread_cleanup_authorities"
        )
        is None
    )
    original = await predecessor_snapshot(db, case)
    physical = RetainedDisk(db, case)
    physical.stopped, physical.purged = True, False
    permanent = await permanent_begin(db, case)
    await operations(db, physical).cleanup_pinned_thread_retirement(
        permanent, cleanup_agent_pod=False
    )
    await db.delete_thread(
        str(case["thread_id"]),
        expected_runtime_retirement_token=permanent["token"],
        expected_runtime_generation=permanent["generation"],
    )
    assert await db.get_thread(str(case["thread_id"])) is None
    assert await predecessor_snapshot(db, case) == original


@pytest.mark.asyncio
async def test_already_pending_permanent_with_held_old_compute_refuses(db, monkeypatch):
    from tests.test_vm_resource_thread_cleanup_real_postgres import old_settled_end

    case, physical = await old_settled_end(db, monkeypatch, ready=True)
    permanent = await permanent_begin(db, case)
    original = await predecessor_snapshot(db, case)
    with pytest.raises(RuntimeError, match="held"):
        await operations(db, physical).cleanup_pinned_thread_retirement(
            permanent, cleanup_agent_pod=False
        )
    assert await predecessor_snapshot(db, case) == original
    assert physical.purged is False


async def child_permit(db, physical, parent, *, request_id=None):
    from uuid import uuid4
    from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest

    case = physical.case
    request_id = request_id or uuid4()
    digest = cleanup_intent_digest({"resource": "disk", "pvc_uid": case["pvc_uid"]})
    store = operations(db, physical).dependencies.recovery_store
    child = await store.acquire_cleanup_permit(
        owner_kind="thread",
        owner_id=case["thread_id"],
        pvc_uid=UUID(case["pvc_uid"]),
        request_id=request_id,
        source="controller_rootdisk_delete",
        intent_digest=digest,
        parent_cleanup=parent.parent_cleanup,
        parent_provision_generation=str(case["generation"]),
        expected_vm_uid=case["vm_uid"],
        revalidate_completed=True,
    )
    return child, request_id, digest


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["current_token", "current_pvc", "source_revision", "unauthorized"]
)
async def test_direct_controller_child_and_open_carrier_replay_revalidate_current_parent(
    db, monkeypatch, fault
):
    import asyncpg
    from shared.vm_resource_admission import ResourceAdmissionError

    case, physical = await settled(db, monkeypatch)
    permanent = await permanent_begin(db, case)
    parent = await admit(db, physical, permanent)
    child, request, digest = await child_permit(db, physical, parent)
    assert child.allowed
    store = operations(db, physical).dependencies.recovery_store
    replay = dict(
        owner_kind="thread",
        owner_id=case["thread_id"],
        source="controller_rootdisk_delete",
        request_id=request,
        intent_digest=digest,
    )
    assert (await store.resume_cleanup_permit(child.admission_id, **replay)).allowed
    await corrupt(db, case, fault)
    # These callers bypass native binding and use the originally valid durable
    # parent. Neither acquisition nor controller restart may renew its effect.
    with pytest.raises((asyncpg.CheckViolationError, ResourceAdmissionError)):
        await child_permit(db, physical, parent, request_id=request)
    with pytest.raises((asyncpg.CheckViolationError, ResourceAdmissionError)):
        await store.resume_cleanup_permit(child.admission_id, **replay)
    assert await store.complete_cleanup_permit(
        child.admission_id, outcome="deleted", request_id=request, intent_digest=digest
    )
    completed = await store.resume_cleanup_permit(child.admission_id, **replay)
    assert not completed.allowed and completed.completed_outcome == "deleted"


@pytest.mark.asyncio
async def test_completed_parent_missing_receipt_never_replays_success(db, monkeypatch):
    import asyncpg
    from orchestrator.services.vm_workspace_recovery_store import (
        complete_vm_cleanup_permit,
    )

    case, physical = await settled(db, monkeypatch)
    permanent = await permanent_begin(db, case)
    parent = await admit(db, physical, permanent)
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        await conn.execute(
            "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),outcome='completed' WHERE id=$1",
            parent.admission_id,
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await complete_vm_cleanup_permit(
            operations(db, physical).dependencies.recovery_store,
            parent,
            outcome="completed",
            provisioner=physical,
        )
    assert physical.purged is False
    assert len(physical.observations) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "missing_purge",
        "retained_purge",
        "missing_external",
        "wrong_external",
        "wrong_outcome",
        "held_reservation",
        "unfinished_source",
    ],
)
async def test_raw_delete_requires_current_permanent_evidence_and_owner_wide_exclusions(
    db, monkeypatch, fault
):
    import asyncpg
    from uuid import uuid4

    case, physical = await settled(db, monkeypatch)
    permanent = await permanent_begin(db, case)
    await operations(db, physical).cleanup_pinned_thread_retirement(
        permanent, cleanup_agent_pod=False
    )
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        if fault == "missing_purge":
            await conn.execute("DELETE FROM vm_thread_retained_disk_purge_receipts")
        elif fault == "retained_purge":
            await conn.execute(
                "UPDATE vm_thread_retained_disk_purge_receipts SET purge_evidence=jsonb_set(purge_evidence,'{pvc_disposition}','\"retained\"')"
            )
        elif fault == "missing_external":
            await conn.execute(
                "UPDATE threads SET runtime_retirement_external_cleanup=NULL"
            )
        elif fault == "wrong_external":
            await conn.execute(
                "UPDATE threads SET runtime_retirement_external_cleanup='{}'"
            )
        elif fault in {"held_reservation", "unfinished_source"}:
            request, generation = uuid4(), uuid4()
            await conn.execute(
                "INSERT INTO vm_creation_retries SELECT (jsonb_populate_record(NULL::vm_creation_retries,"
                "to_jsonb(r)||jsonb_build_object('request_id',$2::uuid,'provision_generation',$3::uuid))).* "
                "FROM vm_creation_retries r WHERE request_id=$1",
                case["request_id"],
                request,
                generation,
            )
            if fault == "held_reservation":
                await conn.execute(
                    "INSERT INTO vm_resource_reservations SELECT (jsonb_populate_record(NULL::vm_resource_reservations,"
                    "to_jsonb(r)||jsonb_build_object('id',$2::uuid,'request_id',$3::uuid,'state','active','released_at',NULL,'release_evidence',NULL))).* "
                    "FROM vm_resource_reservations r WHERE request_id=$1",
                    case["request_id"],
                    uuid4(),
                    request,
                )
            else:
                await conn.execute(
                    "UPDATE vm_creation_retries SET state='attention',ready_at=NULL,resolved_at=NULL WHERE request_id=$1",
                    request,
                )
        # Install an outcome just as native delete does, then exercise the
        # database deletion gate directly, including wrong outcome rejection.
        await conn.execute(
            "INSERT INTO thread_runtime_retirement_outcomes(thread_id,runtime_generation,retirement_token,disposition,permanent,outcome) "
            "VALUES($1,$2,$3,'ended',true,$4)",
            case["thread_id"],
            UUID(permanent["generation"]),
            UUID(permanent["token"]),
            "settled" if fault == "wrong_outcome" else "deleted",
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute("DELETE FROM threads WHERE id=$1", case["thread_id"])
    assert await db.get_thread(str(case["thread_id"])) is not None
    assert (
        await db.fetchval(
            "SELECT count(*) FROM thread_messages WHERE thread_id=$1", case["thread_id"]
        )
        == 1
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("head", ["0294", "0295", "0296"])
@pytest.mark.parametrize("ready", [True, False])
async def test_forward_upgrade_preserves_populated_soft_end_tables(
    pg_dsn, monkeypatch, tmp_path, head, ready
):
    import asyncpg
    from pathlib import Path
    from urllib.parse import urlsplit, urlunsplit
    from uuid import uuid4
    from orchestrator.database.migrate import run_migrations
    from orchestrator.database.postgres import PostgresDB
    from tests.test_vm_resource_thread_cleanup_real_postgres import old_settled_end

    database = f"retained_upgrade_{uuid4().hex}"
    admin = await asyncpg.connect(pg_dsn)
    await admin.execute(f'CREATE DATABASE "{database}"')
    parts = urlsplit(pg_dsn)
    dsn = urlunsplit(parts._replace(path=f"/{database}"))
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    store = PostgresDB(connection_string=dsn, min_connections=1, max_connections=8)
    migrations = (
        Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
    )
    stage = tmp_path / "migrations"
    stage.mkdir()
    try:
        for path in migrations.glob("*.sql"):
            if path.name.split("_", 1)[0] <= head:
                (stage / path.name).write_text(path.read_text())
        await run_migrations(pool, stage)
        await store.connect()
        if head == "0294":
            case, old = await old_settled_end(store, monkeypatch, ready=ready)
            path = migrations / "0295_vm_thread_cleanup_resource_release.sql"
            (stage / path.name).write_text(path.read_text())
            await store.close()
            await run_migrations(pool, stage)
            await store.connect()
            assert [
                r["state"]
                for r in (
                    await operations(store, old).reconcile_settled_vm_resources()
                )["results"]
            ] == ["released"]
            physical = RetainedDisk(store, case)
            physical.stopped, physical.purged = True, False
        else:
            case, physical = await settled(store, monkeypatch, ready=ready)
        original = await predecessor_snapshot(store, case)
        historical_checksums = await pool.fetch(
            "SELECT filename,checksum FROM schema_migrations ORDER BY filename"
        )
        # Model the restarted application, whose pool has not prepared owner
        # queries before startup migrations. Keeping the old app pool across
        # external ALTER TABLE reuses a SELECT * plan with the old row shape;
        # asyncpg cannot re-prepare that statement inside Begin's transaction.
        await store.close()
        await run_migrations(pool, migrations)
        await run_migrations(pool, migrations)
        await store.connect()
        assert await predecessor_snapshot(store, case) == original
        assert (
            await pool.fetch(
                "SELECT filename,checksum FROM schema_migrations WHERE filename=ANY($1::text[]) ORDER BY filename",
                [row["filename"] for row in historical_checksums],
            )
            == historical_checksums
        )
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM vm_thread_retained_disk_purge_authorities"
            )
            == 0
        )
        permanent = await permanent_begin(store, case)
        await operations(store, physical).cleanup_pinned_thread_retirement(
            permanent, cleanup_agent_pod=False
        )
        await store.delete_thread(
            str(case["thread_id"]),
            expected_runtime_retirement_token=permanent["token"],
            expected_runtime_generation=permanent["generation"],
        )
        assert await store.get_thread(str(case["thread_id"])) is None
        assert await predecessor_snapshot(store, case) == original
    finally:
        await store.close()
        await pool.close()
        await admin.execute(f'DROP DATABASE "{database}"')
        await admin.close()


@pytest.mark.asyncio
async def test_lost_final_transaction_response_replays_the_committed_receipt(
    db, monkeypatch
):
    from contextlib import asynccontextmanager
    from orchestrator.services.vm_workspace_recovery_store import (
        complete_vm_cleanup_permit,
        VMWorkspaceRecoveryStore,
    )

    case, physical = await settled(db, monkeypatch)
    permanent = await permanent_begin(db, case)
    parent = await admit(db, physical, permanent)
    physical.purged = True
    original = await predecessor_snapshot(db, case)

    class LostCommitDB:
        transactions = 0

        @asynccontextmanager
        async def acquire(self):
            async with db.acquire() as underlying:
                lost_db = self

                class Connection:
                    def __getattr__(self, key):
                        return getattr(underlying, key)

                    @asynccontextmanager
                    async def transaction(self):
                        lost_db.transactions += 1
                        async with underlying.transaction():
                            yield
                        if lost_db.transactions == 2:
                            raise TimeoutError(
                                "response lost after durable final commit"
                            )

                yield Connection()

    with pytest.raises(TimeoutError, match="durable final commit"):
        await complete_vm_cleanup_permit(
            VMWorkspaceRecoveryStore(LostCommitDB()),
            parent,
            outcome="completed",
            provisioner=physical,
        )
    assert (
        await db.fetchval("SELECT count(*) FROM vm_thread_retained_disk_purge_receipts")
        == 1
    )
    observed = len(physical.observations)
    await complete_vm_cleanup_permit(
        VMWorkspaceRecoveryStore(db), parent, outcome="completed", provisioner=physical
    )
    assert len(physical.observations) == observed
    assert await predecessor_snapshot(db, case) == original


@pytest.mark.asyncio
async def test_carrier_completion_racing_parent_validation_does_not_reauthorize_effect(
    db, monkeypatch
):
    import asyncio
    from orchestrator.services import vm_thread_retained_disk_purge as retained

    case, physical = await settled(db, monkeypatch)
    permanent = await permanent_begin(db, case)
    parent = await admit(db, physical, permanent)
    child, request, digest = await child_permit(db, physical, parent)
    store = operations(db, physical).dependencies.recovery_store
    entered = asyncio.Event()
    proceed = asyncio.Event()
    original = retained.validate_retained_disk_parent

    async def observe_validation(conn, admission_id):
        entered.set()
        # Completion also takes the owner/PVC advisory locks. Suspend before
        # validation acquires them so the test exercises a real possible race,
        # rather than holding an owner row while waiting for its own successor.
        await proceed.wait()
        await original(conn, admission_id)

    monkeypatch.setattr(retained, "validate_retained_disk_parent", observe_validation)
    task = None
    try:
        task = asyncio.create_task(
            store.resume_cleanup_permit(
                child.admission_id,
                owner_kind="thread",
                owner_id=case["thread_id"],
                source="controller_rootdisk_delete",
                request_id=request,
                intent_digest=digest,
            )
        )
        await asyncio.wait_for(entered.wait(), 5)
        assert await store.complete_cleanup_permit(
            child.admission_id,
            outcome="deleted",
            request_id=request,
            intent_digest=digest,
        )
        proceed.set()
        resumed = await asyncio.wait_for(task, 5)
        assert resumed.allowed is False
        assert resumed.completed_outcome == "deleted"
    finally:
        if task is not None:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_carrier_completion_waits_for_owner_without_locking_child(db, monkeypatch):
    import asyncio

    case, physical = await settled(db, monkeypatch)
    permanent = await permanent_begin(db, case)
    parent = await admit(db, physical, permanent)
    child, request, digest = await child_permit(db, physical, parent)
    store = operations(db, physical).dependencies.recovery_store
    task = None
    try:
        async with db.acquire() as blocker, blocker.transaction():
            pid = await blocker.fetchval("SELECT pg_backend_pid()")
            await blocker.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"workspace-recovery:thread:{case['thread_id']}",
            )
            task = asyncio.create_task(
                store.complete_cleanup_permit(
                    child.admission_id,
                    outcome="deleted",
                    request_id=request,
                    intent_digest=digest,
                )
            )
            async with db.acquire() as observer:
                async def wait_for_blocked_completion():
                    while not await observer.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                        "WHERE $1=ANY(pg_blocking_pids(pid)))",
                        pid,
                    ):
                        if task.done():
                            await task
                            pytest.fail("completion did not wait for the owner lock")
                        await asyncio.sleep(0.01)

                await asyncio.wait_for(wait_for_blocked_completion(), 5)
                # A concurrent parent validator holding this same owner lock
                # must be able to read/lock the child. Row-first completion
                # deadlocks with the established owner-first validation path.
                async with observer.transaction():
                    assert await observer.fetchval(
                        "SELECT id FROM vm_workspace_cleanup_admissions "
                        "WHERE id=$1 FOR UPDATE NOWAIT",
                        child.admission_id,
                    ) == child.admission_id
        assert await asyncio.wait_for(task, 5)
    finally:
        if task is not None:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_adopted_never_ready_with_observed_incarnation_keeps_reservation_unbound(
    db, monkeypatch
):
    from uuid import uuid4
    from tests import test_vm_resource_thread_cleanup_real_postgres as first_end

    original_adopt = first_end._adopted_charged_thread
    observed_vmi, observed_launcher = str(uuid4()), str(uuid4())

    async def adopt_with_pre_ready_observation(store, patch):
        adopted = await original_adopt(store, patch)
        await store.execute(
            "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',metadata->'vm'||$2::jsonb) WHERE id=$1",
            adopted[4],
            json.dumps({"vmi_uid": observed_vmi, "active_pod_uid": observed_launcher}),
        )
        return adopted

    monkeypatch.setattr(
        first_end, "_adopted_charged_thread", adopt_with_pre_ready_observation
    )
    case, physical = await settled(db, monkeypatch, ready=False)
    original = await predecessor_snapshot(db, case)
    permanent = await permanent_begin(db, case)
    assert permanent["context"]["vm"]["vmi_uid"] == observed_vmi
    assert (
        await db.fetchval(
            "SELECT vm_uid FROM vm_resource_reservations WHERE request_id=$1",
            case["request_id"],
        )
        is None
    )
    await operations(db, physical).cleanup_pinned_thread_retirement(
        permanent, cleanup_agent_pod=False
    )
    await db.delete_thread(
        str(case["thread_id"]),
        expected_runtime_retirement_token=permanent["token"],
        expected_runtime_generation=permanent["generation"],
    )
    assert physical.purged
    assert await db.get_thread(str(case["thread_id"])) is None
    assert await predecessor_snapshot(db, case) == original
    proof = json.loads(
        await db.fetchval(
            "SELECT purge_evidence FROM vm_thread_retained_disk_purge_receipts"
        )
    )
    assert proof["vmi_uid"] is None and proof["launcher_uid"] is None
    assert proof["vm_absent"] and proof["vmi_absent"] and proof["launcher_absent"]


@pytest.mark.asyncio
@pytest.mark.parametrize("binding", ["missing", "mislabelled"])
async def test_reconstructed_completed_parent_cannot_bypass_durable_source(
    db, monkeypatch, binding
):
    from dataclasses import replace
    from shared.vm_resource_admission import ResourceAdmissionError
    from orchestrator.services.vm_workspace_recovery_store import (
        complete_vm_cleanup_permit,
        cleanup_intent_digest,
    )

    case, physical = await settled(db, monkeypatch)
    permanent = await permanent_begin(db, case)
    parent = await admit(db, physical, permanent)
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        await conn.execute(
            "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),outcome='completed' WHERE id=$1",
            parent.admission_id,
        )
    proof = None
    if binding == "mislabelled":
        intent = {
            **parent.parent_cleanup["intent"],
            "source": "pinned_thread_retirement",
        }
        proof = {
            **parent.parent_cleanup,
            "intent": intent,
            "intent_digest": cleanup_intent_digest(intent),
        }
    reconstructed = replace(parent, completed_outcome="completed", parent_cleanup=proof)
    with pytest.raises(ResourceAdmissionError):
        await complete_vm_cleanup_permit(
            operations(db, physical).dependencies.recovery_store,
            reconstructed,
            outcome="completed",
            provisioner=physical,
        )
    assert len(physical.observations) == 1
    assert (
        await db.fetchval("SELECT count(*) FROM vm_thread_retained_disk_purge_receipts")
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "source='legacy'",
        "source='legacy',completed_at=clock_timestamp(),outcome='completed'",
        "request_id=gen_random_uuid()",
        "intent_digest='sha256:changed'",
        "owner_id=gen_random_uuid()",
        "pvc_uid=gen_random_uuid()",
    ],
)
async def test_new_parent_identity_cannot_be_rewritten_to_bypass_completion_guard(
    db, monkeypatch, change
):
    import asyncpg

    case, physical = await settled(db, monkeypatch)
    permanent = await permanent_begin(db, case)
    parent = await admit(db, physical, permanent)
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            f"UPDATE vm_workspace_cleanup_admissions SET {change} WHERE id=$1",
            parent.admission_id,
        )
    assert (
        await db.fetchval(
            "SELECT completed_at FROM vm_workspace_cleanup_admissions WHERE id=$1",
            parent.admission_id,
        )
        is None
    )
