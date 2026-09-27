"""Native pinned VM End debits only exact, authenticated physical stop."""

import json
import logging
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncio
import asyncpg

import pytest

from orchestrator.services.pinned_retirement import PinnedRetirementOperations
from orchestrator.services.vm_provisioner import VMTeardownResult
from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
from tests.test_vm_resource_thread_source_real_postgres import (
    db as _db,
    thread_schema,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    pg_dsn,  # noqa: F401
    _ready_charged_thread,
    _adopted_charged_thread,
)

db = _db


async def retiring(db, monkeypatch, *, ready=True, permanent=False):
    if ready:
        case = await _ready_charged_thread(db, monkeypatch)
        assert await VMProvisioningPhaseStore(db).publish_thread_ready(
            str(case["thread_id"]),
            str(case["generation"]),
            case["registration"],
            case["vm_uid"],
            case["updates"],
        )
    else:
        (
            _,
            _,
            _,
            _,
            thread_id,
            runtime,
            generation,
            request_id,
            admitted,
            observations,
            _,
        ) = await _adopted_charged_thread(db, monkeypatch)
        case = dict(
            thread_id=thread_id,
            runtime=runtime,
            generation=generation,
            request_id=request_id,
            admitted=admitted,
            vm_uid=observations["vm"]["object"]["metadata"]["uid"],
            vmi_uid=None,
            launcher_uid=None,
        )
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{config_override}',"
        '\'{"workspace":{"backend":"vm"}}\') WHERE id=$1',
        case["thread_id"],
    )
    retirement = await db.begin_pinned_thread_retirement(
        str(case["thread_id"]),
        permanent=permanent,
    )
    assert retirement["state"] == "pending", retirement
    assert await db.authorize_pinned_thread_retirement(
        str(case["thread_id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    case["retirement"] = retirement
    case["pvc_uid"] = retirement["context"]["vm"]["rootdisk_pvc_uid"]
    return case


class PhysicalStop:
    lifecycle_available = True

    def __init__(self, db, case):
        self.db, self.case = db, case
        self.stopped = False
        self.purged = None

    async def release_vm_captured(self, owner, identity, **kwargs):
        assert owner == str(self.case["thread_id"])
        assert identity.vm_uid == self.case["vm_uid"]
        assert identity.rootdisk_pvc_uid == self.case["pvc_uid"]
        assert kwargs["entity_type"] == "thread"
        assert await self.db.fetchval(
            "SELECT state<>'released' FROM vm_resource_reservations WHERE id=$1",
            UUID(self.case["admitted"]["reservation_id"]),
        )
        self.stopped = True
        self.purged = kwargs["purge_disk"]
        await self.db.execute(
            "INSERT INTO managed_repository_process_zero_receipts "
            "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
            "VALUES('thread',$1,'vm','vm',$2) ON CONFLICT DO NOTHING",
            self.case["thread_id"],
            str(self.case["generation"]),
        )
        await self.db.execute(
            "UPDATE threads SET metadata=jsonb_set(metadata,'{vm,status}','\"deleted\"') WHERE id=$1",
            self.case["thread_id"],
        )
        return VMTeardownResult("completed", True)

    async def attest_vm_cleanup_stop(self, candidate):
        assert self.stopped
        return {
            "version": 1,
            "kind": "vm_cleanup_physical_stop",
            "owner_kind": "thread",
            "owner_id": str(self.case["thread_id"]),
            "provision_generation": str(self.case["generation"]),
            "vm_uid": self.case["vm_uid"],
            "vmi_uid": self.case["vmi_uid"],
            "launcher_uid": self.case["launcher_uid"],
            "pvc_uid": self.case["pvc_uid"],
            "vm_absent": True,
            "vmi_absent": True,
            "launcher_absent": True,
            "same_generation_replacement": False,
            "pvc_disposition": "purged" if self.purged else "retained",
            "controller_authenticated": True,
        }


def operations(db, provisioner):
    return PinnedRetirementOperations(
        SimpleNamespace(
            store=db,
            vm_provisioner=provisioner,
            recovery_store=VMWorkspaceRecoveryStore(db),
            logger=logging.getLogger(__name__),
        )
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("ready", [True, False])
@pytest.mark.parametrize("permanent", [False, True])
async def test_native_end_releases_exact_ready_or_adopted_charge(
    db,
    monkeypatch,
    ready,
    permanent,
):
    case = await retiring(db, monkeypatch, ready=ready, permanent=permanent)
    physical = PhysicalStop(db, case)
    await operations(db, physical).cleanup_pinned_thread_retirement(
        case["retirement"],
        cleanup_agent_pod=False,
    )
    assert physical.stopped
    assert physical.purged is permanent
    charge = await db.fetchrow(
        "SELECT * FROM vm_resource_reservations WHERE id=$1",
        UUID(case["admitted"]["reservation_id"]),
    )
    assert charge["state"] == "released"
    assert (
        json.loads(charge["release_evidence"])["kind"] == "exact_cleanup_compute_absent"
    )
    if permanent:
        await db.delete_thread(
            str(case["thread_id"]),
            expected_runtime_retirement_token=case["retirement"]["token"],
            expected_runtime_generation=case["retirement"]["generation"],
        )
        assert await db.get_thread(str(case["thread_id"])) is None
        audit = await db.fetchrow(
            "SELECT * FROM vm_thread_creation_owners WHERE thread_id=$1",
            case["thread_id"],
        )
        assert audit["live_thread_id"] is None
        assert (
            str(audit["deleted_runtime_generation"]) == case["retirement"]["generation"]
        )
        assert str(audit["deleted_retirement_token"]) == case["retirement"]["token"]
        assert json.loads(audit["deletion_receipt"])["kind"] == "adopted_vm_cleanup"
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(
                "UPDATE vm_creation_retries SET reason='erased' WHERE request_id=$1",
                case["request_id"],
            )
        assert (
            await db.fetchval(
                "SELECT state FROM vm_creation_retries WHERE request_id=$1",
                case["request_id"],
            )
            == "succeeded"
        )
    else:
        assert await db.settle_pinned_thread_retirement(
            str(case["thread_id"]),
            token=case["retirement"]["token"],
            generation=case["retirement"]["generation"],
        )


async def old_settled_end(db, monkeypatch, *, ready):
    """Execute the pre-upgrade native path, which completed physical permits only."""
    from orchestrator.services import vm_workspace_recovery_store as recovery

    case = await retiring(db, monkeypatch, ready=ready)
    physical = PhysicalStop(db, case)

    async def no_accounting(*args, **kwargs):
        return None

    with monkeypatch.context() as legacy:
        legacy.setattr(recovery, "prepare_vm_cleanup_resource", no_accounting)
        await operations(db, physical).cleanup_pinned_thread_retirement(
            case["retirement"],
            cleanup_agent_pod=False,
        )
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm,status}','\"deleted\"') WHERE id=$1",
        case["thread_id"],
    )
    assert await db.settle_pinned_thread_retirement(
        str(case["thread_id"]),
        token=case["retirement"]["token"],
        generation=case["retirement"]["generation"],
    )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(case["admitted"]["reservation_id"]),
    ) == ("active" if ready else "reserved")
    return case, physical


@pytest.mark.asyncio
@pytest.mark.parametrize("ready", [True, False])
async def test_bounded_reconciliation_releases_only_surviving_exact_old_end(
    db,
    monkeypatch,
    ready,
):
    case, physical = await old_settled_end(db, monkeypatch, ready=ready)
    result = await operations(db, physical).reconcile_settled_vm_resources(limit=1)
    assert [row["state"] for row in result["results"]] == ["released"]
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(case["admitted"]["reservation_id"]),
        )
        == "released"
    )
    assert (await operations(db, physical).reconcile_settled_vm_resources(limit=1))[
        "results"
    ] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "unknown",
        "vm_uid",
        "vmi_uid",
        "launcher_uid",
        "pvc_uid",
        "provision_generation",
        "owner_id",
        "owner_kind",
        "controller_authenticated",
        "same_generation_replacement",
        "pvc_disposition",
        "missing_zero",
        "wrong_zero",
    ],
)
async def test_cleanup_refuses_inexact_stop_and_process_zero(db, monkeypatch, fault):
    from shared.vm_resource_admission import ResourceAdmissionError

    case = await retiring(db, monkeypatch)
    physical = PhysicalStop(db, case)
    original = physical.attest_vm_cleanup_stop

    async def observe(candidate):
        proof = await original(candidate)
        if fault in {"missing_zero", "wrong_zero"}:
            await db.execute(
                "DELETE FROM managed_repository_process_zero_receipts WHERE owner_id=$1",
                case["thread_id"],
            )
            if fault == "wrong_zero":
                await db.execute(
                    "INSERT INTO managed_repository_process_zero_receipts "
                    "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
                    "VALUES('thread',$1,'vm','vm',$2)",
                    case["thread_id"],
                    str(uuid4()),
                )
        elif fault == "unknown":
            return None
        else:
            proof[fault] = {
                "controller_authenticated": False,
                "same_generation_replacement": True,
                "pvc_disposition": "purged",
                "owner_kind": "job",
            }.get(fault, str(uuid4()))
        return proof

    physical.attest_vm_cleanup_stop = observe
    with pytest.raises(ResourceAdmissionError):
        await operations(db, physical).cleanup_pinned_thread_retirement(
            case["retirement"],
            cleanup_agent_pod=False,
        )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(case["admitted"]["reservation_id"]),
        )
        == "teardown"
    )
    assert (
        await db.fetchval("SELECT count(*) FROM vm_resource_thread_cleanup_stops") == 0
    )
    assert await db.fetchval(
        "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions "
        "WHERE owner_id=$1 AND source='pinned_thread_retirement'",
        case["thread_id"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "runtime_generation",
        "runtime_retirement_token",
        "agent_id",
        "runtime_attach_token",
        "source_runtime",
        "source_vm",
        "source_pvc",
        "reservation_revision",
        "vm_uid",
        "rootdisk_pvc_uid",
        "vmi_uid",
        "active_pod_uid",
        "_runtime_incarnation",
        "creation_request_id",
    ],
)
async def test_owner_or_resource_change_during_probe_cannot_debit(
    db, monkeypatch, fault
):
    case = await retiring(db, monkeypatch)
    physical = PhysicalStop(db, case)
    original = physical.attest_vm_cleanup_stop

    async def mutate():
        # Model an independent stale/corrupt writer. A separate connection also
        # proves controller waits hold no owner-row locks.
        async with db.acquire() as conn, conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            if fault in {
                "runtime_generation",
                "runtime_retirement_token",
                "agent_id",
                "runtime_attach_token",
            }:
                await conn.execute(
                    f"UPDATE threads SET {fault}=$2 WHERE id=$1",
                    case["thread_id"],
                    uuid4(),
                )
            elif fault.startswith("source_"):
                field = {
                    "source_runtime": "thread_runtime_generation",
                    "source_vm": "observed_vm_uid",
                    "source_pvc": "observed_pvc_uid",
                }[fault]
                await conn.execute(
                    f"UPDATE vm_creation_retries SET {field}=$2 WHERE request_id=$1",
                    case["request_id"],
                    uuid4(),
                )
            elif fault == "reservation_revision":
                await conn.execute(
                    "UPDATE vm_resource_reservations SET revision=revision+1 WHERE id=$1",
                    UUID(case["admitted"]["reservation_id"]),
                )
            else:
                await conn.execute(
                    "UPDATE threads SET metadata=jsonb_set(metadata,$2::text[],$3::jsonb) WHERE id=$1",
                    case["thread_id"],
                    ["vm", fault],
                    json.dumps(str(uuid4())),
                )

    async def observe(candidate):
        proof = await original(candidate)
        await asyncio.wait_for(mutate(), timeout=3)
        return proof

    physical.attest_vm_cleanup_stop = observe
    with pytest.raises(asyncpg.CheckViolationError):
        await operations(db, physical).cleanup_pinned_thread_retirement(
            case["retirement"],
            cleanup_agent_pod=False,
        )
    assert (
        await db.fetchval("SELECT count(*) FROM vm_resource_thread_cleanup_stops") == 0
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(case["admitted"]["reservation_id"]),
        )
        == "teardown"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["source_runtime", "zero", "intent", "metadata", "outcome", "resumed"]
)
async def test_old_end_with_incomplete_or_replaced_lineage_stays_held(
    db, monkeypatch, fault
):
    case, physical = await old_settled_end(db, monkeypatch, ready=False)
    if fault == "resumed":
        assert await db.resume_thread(str(case["thread_id"]))
    else:
        async with db.acquire() as conn, conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            if fault == "source_runtime":
                await conn.execute(
                    "UPDATE vm_creation_retries SET thread_runtime_generation=$2 WHERE request_id=$1",
                    case["request_id"],
                    uuid4(),
                )
            elif fault == "zero":
                await conn.execute(
                    "DELETE FROM managed_repository_process_zero_receipts WHERE owner_id=$1",
                    case["thread_id"],
                )
            elif fault == "intent":
                await conn.execute(
                    "UPDATE vm_workspace_cleanup_admissions SET intent_digest=$2 WHERE owner_id=$1 AND source='pinned_thread_retirement'",
                    case["thread_id"],
                    "sha256:" + "f" * 64,
                )
            elif fault == "metadata":
                await conn.execute(
                    "UPDATE threads SET metadata=metadata-'vm' WHERE id=$1",
                    case["thread_id"],
                )
            else:
                await conn.execute(
                    "DELETE FROM thread_runtime_retirement_outcomes WHERE thread_id=$1",
                    case["thread_id"],
                )
    result = await operations(db, physical).reconcile_settled_vm_resources(limit=1)
    assert (
        result["results"] == []
        if fault == "resumed"
        else result["results"][0]["state"] == "held"
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(case["admitted"]["reservation_id"]),
        )
        == "reserved"
    )


@pytest.mark.asyncio
async def test_lost_attestation_completed_permit_and_concurrent_replay_release_once(
    db, monkeypatch
):
    from shared.vm_resource_admission import ResourceAdmissionError

    case = await retiring(db, monkeypatch, ready=False)
    physical = PhysicalStop(db, case)
    original = physical.attest_vm_cleanup_stop

    async def lost(candidate):
        await original(candidate)
        return None

    physical.attest_vm_cleanup_stop = lost
    with pytest.raises(ResourceAdmissionError):
        await operations(db, physical).cleanup_pinned_thread_retirement(
            case["retirement"],
            cleanup_agent_pod=False,
        )
    # Model the completed physical permit left by an older process. Native
    # retry must still attest/debit before treating the permit as fully done.
    await db.execute(
        "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),outcome='completed' "
        "WHERE owner_id=$1 AND source='pinned_thread_retirement'",
        case["thread_id"],
    )
    physical.attest_vm_cleanup_stop = original
    await asyncio.gather(
        *(
            operations(db, physical).cleanup_pinned_thread_retirement(
                case["retirement"], cleanup_agent_pod=False
            )
            for _ in range(2)
        )
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(case["admitted"]["reservation_id"]),
        )
        == "released"
    )
    assert (
        await db.fetchval("SELECT count(*) FROM vm_resource_thread_cleanup_stops") == 1
    )
    await operations(db, physical).cleanup_pinned_thread_retirement(
        case["retirement"], cleanup_agent_pod=False
    )
    assert (
        await db.fetchval("SELECT count(*) FROM vm_resource_thread_cleanup_stops") == 1
    )


@pytest.mark.asyncio
async def test_direct_sql_cannot_release_without_stop_or_mutate_authority(
    db, monkeypatch
):
    case = await retiring(db, monkeypatch)
    physical = PhysicalStop(db, case)
    ops = operations(db, physical)
    identity = ops._captured_vm_recovery_identity(
        case["retirement"]["context"], permanent=False
    )
    permit = await ops._admit_vm_cleanup(
        str(case["thread_id"]), identity, purge_disk=False
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_resource_reservations SET state='released',release_evidence=$2::jsonb WHERE id=$1",
            UUID(case["admitted"]["reservation_id"]),
            json.dumps(
                {
                    "kind": "exact_cleanup_compute_absent",
                    "owner_kind": "thread",
                    "thread_id": str(case["thread_id"]),
                    "cleanup_admission_id": str(permit.admission_id),
                }
            ),
        )
    for command in (
        "UPDATE vm_resource_thread_cleanup_authorities SET retirement_token=gen_random_uuid()",
        "DELETE FROM vm_resource_thread_cleanup_authorities",
    ):
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(command)
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(case["admitted"]["reservation_id"]),
        )
        == "teardown"
    )


@pytest.mark.asyncio
async def test_reconciliation_yields_and_advances_past_blocked_controller(
    db, monkeypatch
):
    case, physical = await old_settled_end(db, monkeypatch, ready=False)
    entered = asyncio.Event()

    async def blocked(candidate):
        entered.set()
        await asyncio.Event().wait()

    physical.attest_vm_cleanup_stop = blocked
    result = await asyncio.wait_for(
        operations(db, physical).reconcile_settled_vm_resources(
            limit=1, timeout_seconds=0.1
        ),
        timeout=1,
    )
    assert entered.is_set()
    assert result["results"][0]["state"] == "held"
    assert result["results"][0]["reason"] == "resource_cleanup_probe_timeout"
    assert result["after"] is not None
    assert (
        await operations(db, physical).reconcile_settled_vm_resources(
            limit=1,
            after=result["after"],
        )
    )["results"] == []
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(case["admitted"]["reservation_id"]),
        )
        != "released"
    )


@pytest.mark.asyncio
async def test_reconciliation_propagates_shutdown_cancellation(db, monkeypatch):
    case, physical = await old_settled_end(db, monkeypatch, ready=False)
    entered = asyncio.Event()

    async def blocked(candidate):
        entered.set()
        await asyncio.Event().wait()

    physical.attest_vm_cleanup_stop = blocked
    task = asyncio.create_task(
        operations(db, physical).reconcile_settled_vm_resources(limit=1)
    )
    await asyncio.wait_for(entered.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(case["admitted"]["reservation_id"]),
        )
        == "teardown"
    )
    assert (
        await db.fetchval("SELECT count(*) FROM vm_resource_thread_cleanup_stops") == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "missing_stop",
        "retained_disk",
        "wrong_revision",
        "unfinished_source",
        "held_reservation",
    ],
)
async def test_permanent_delete_requires_exact_purge_and_preserves_audit_owner(
    db, monkeypatch, fault
):
    case = await retiring(db, monkeypatch, permanent=True)
    physical = PhysicalStop(db, case)
    await operations(db, physical).cleanup_pinned_thread_retirement(
        case["retirement"], cleanup_agent_pod=False
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", case["request_id"]
    )
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        if fault == "missing_stop":
            await conn.execute("DELETE FROM vm_resource_thread_cleanup_stops")
        elif fault == "retained_disk":
            await conn.execute(
                "UPDATE vm_resource_thread_cleanup_stops SET stop_evidence=jsonb_set(stop_evidence,'{pvc_disposition}','\"retained\"')"
            )
            # Even a matching compute debit digest cannot turn retained disk
            # evidence into purge authority for deleting its owner.
            await conn.execute(
                "UPDATE vm_resource_reservations r SET release_evidence=jsonb_set(r.release_evidence,"
                "'{stop_evidence_digest}',to_jsonb('sha256:'||encode(sha256(convert_to(s.stop_evidence::text,'UTF8')),'hex'))) "
                "FROM vm_resource_thread_cleanup_authorities a JOIN vm_resource_thread_cleanup_stops s USING(cleanup_admission_id) "
                "WHERE r.id=a.reservation_id",
            )
        elif fault == "wrong_revision":
            await conn.execute(
                "UPDATE vm_resource_thread_cleanup_authorities SET reservation_revision=reservation_revision+1"
            )
        elif fault == "held_reservation":
            previous_request, previous_generation = uuid4(), uuid4()
            await conn.execute(
                "INSERT INTO vm_creation_retries SELECT (jsonb_populate_record(NULL::vm_creation_retries,"
                "to_jsonb(r)||jsonb_build_object('request_id',$2::uuid,'provision_generation',$3::uuid))).* "
                "FROM vm_creation_retries r WHERE request_id=$1",
                case["request_id"],
                previous_request,
                previous_generation,
            )
            await conn.execute(
                "INSERT INTO vm_resource_waiters SELECT (jsonb_populate_record(NULL::vm_resource_waiters,"
                "to_jsonb(w)||jsonb_build_object('request_id',$2::uuid,'provision_generation',$3::uuid,"
                "'state','admitted'))).* FROM vm_resource_waiters w WHERE request_id=$1",
                case["request_id"],
                previous_request,
                previous_generation,
            )
            await conn.execute(
                "INSERT INTO vm_resource_reservations SELECT (jsonb_populate_record(NULL::vm_resource_reservations,"
                "to_jsonb(r)||jsonb_build_object('id',$2::uuid,'request_id',$3::uuid,'state','active',"
                "'released_at',NULL,'release_evidence',NULL))).* FROM vm_resource_reservations r WHERE request_id=$1",
                case["request_id"],
                uuid4(),
                previous_request,
            )
        else:
            # Another unfinished historical source for this audit owner must
            # survive; a successful latest purge cannot erase its obligations.
            await conn.execute(
                "INSERT INTO vm_creation_retries (request_id,owner_kind,thread_id,thread_runtime_generation,"
                "provision_generation,origin,request_digest,canonical_request,controller_configuration_digest,controller_configuration) "
                "VALUES($1,'thread',$2,$3,$4,'initial',$5,$6::jsonb,$7,$8::jsonb)",
                uuid4(),
                case["thread_id"],
                uuid4(),
                uuid4(),
                source["request_digest"],
                source["canonical_request"],
                source["controller_configuration_digest"],
                source["controller_configuration"],
            )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.delete_thread(
            str(case["thread_id"]),
            expected_runtime_retirement_token=case["retirement"]["token"],
            expected_runtime_generation=case["retirement"]["generation"],
        )
    assert await db.get_thread(str(case["thread_id"])) is not None
    assert (
        await db.fetchval(
            "SELECT live_thread_id FROM vm_thread_creation_owners WHERE thread_id=$1",
            case["thread_id"],
        )
        == case["thread_id"]
    )
    assert (
        await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1", case["request_id"]
        )
        == source
    )


@pytest.mark.asyncio
async def test_reconciliation_deadline_also_bounds_blocked_nomination(db, monkeypatch):
    case, physical = await old_settled_end(db, monkeypatch, ready=False)
    async with db.acquire() as conn, conn.transaction():
        await conn.execute(
            "LOCK TABLE vm_workspace_cleanup_admissions IN ACCESS EXCLUSIVE MODE"
        )
        result = await asyncio.wait_for(
            operations(db, physical).reconcile_settled_vm_resources(
                timeout_seconds=0.1
            ),
            timeout=1,
        )
    assert result == {
        "after": None,
        "results": [],
        "reason": "resource_cleanup_nomination_timeout",
    }
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(case["admitted"]["reservation_id"]),
        )
        == "reserved"
    )
