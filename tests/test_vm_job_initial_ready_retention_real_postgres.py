"""Initial Ready owner Cancel uses exact retained custody, never a purge hint."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from orchestrator.services.vm_job_cancel_retention import acquire_cancel_retention
from orchestrator.services.vm_job_retained_resume import (
    acquire_retained_terminal_cleanup,
)
from tests.test_vm_job_cancel_retention_real_postgres import cancelled
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


@pytest_asyncio.fixture(scope="module")
async def initial_ready_schema(pg_dsn, resume_schema):  # noqa: F811
    path = Path(__file__).resolve().parents[1] / (
        "src/orchestrator/database/migrations/app/0340_vm_job_ready_cancel_retention.sql"
    )
    conn = await asyncpg.connect(pg_dsn)
    try:
        if path.exists() and not await conn.fetchval(
            "SELECT to_regprocedure('public.vm_job_initial_ready_retention_candidate(uuid,uuid,uuid,uuid,uuid,uuid,boolean)') IS NOT NULL"
        ):
            await conn.execute(path.read_text())
        forward = path.with_name("0341_vm_job_deleted_retention_replay.sql")
        if forward.exists() and not await conn.fetchval(
            "SELECT to_regprocedure('public.vm_job_deleted_retention_replay_allowed(integer,uuid,uuid,uuid,uuid,uuid,boolean,uuid,uuid,bigint,uuid,uuid,uuid)') IS NOT NULL"
        ):
            await conn.execute(forward.read_text())
        yield
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(initial_ready_schema, _resume_db):  # noqa: F811
    # The shared source fixture removes Job rows but keeps queue history. A
    # previous explicit Resume must not nominate an orphan in the next test.
    await _resume_db.execute(
        "DELETE FROM run_queue q WHERE q.unit_kind='worker_batch' "
        "AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.id=q.unit_id)"
    )
    yield _resume_db


@pytest.fixture(autouse=True)
def enabled(monkeypatch):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", "true")


async def ready_root(db, *, status="ready", old=False, ready_history=True):
    state = await cancelled(db, old=old, retiring=False)
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm}',context->'vm'||$2::jsonb) WHERE id=$1",
        UUID(state["job_id"]),
        json.dumps(
            {"status": status, "active_pod_uid": state["frozen"]["launcher_uid"]}
        ),
    )
    if ready_history:
        await db.execute(
            "UPDATE vm_creation_retries SET ready_at=clock_timestamp() WHERE job_id=$1",
            UUID(state["job_id"]),
        )
    return state


def ready_root_witness(candidate):
    return {
        "version": 1,
        "kind": "vm_job_initial_ready_preflight_v1",
        "stop_policy": "initial_ready_cancel_v1",
        "frozen": candidate,
        "namespace": candidate["namespace"],
        "owner_id": candidate["job_id"],
        "pvc_name": f"agent-vm-{candidate['job_id']}-rootdisk",
        "pvc_uid": candidate["pvc_uid"],
        "dv_uid": "aaaaaaaa-1111-4222-8333-bbbbbbbbbbbb",
        "ownership": "standalone_dv",
        "deleting": False,
        "consumer_scope": "exact_frozen_runtime_only",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    ["created", "ssh_pending", "ssh_unreachable", "ready", "retiring_process_zero"],
)
async def test_initial_ready_cancel_admits_only_signed_false_keep(db, status):
    state = await ready_root(db, status=status)
    provisioner = SimpleNamespace(
        qualify_retained_ready_stop=AsyncMock(side_effect=ready_root_witness)
    )
    permit = await acquire_retained_terminal_cleanup(
        state["recovery"],
        provisioner,
        job_id=state["job_id"],
        identity=state["identity"],
    )
    assert permit is not None and permit.allowed
    assert permit.parent_cleanup["intent"]["purge_disk"] is False
    candidate = permit.parent_cleanup["retention_preflight"]["frozen"]
    assert candidate["kind"] == "vm_job_initial_ready_stop_candidate_v1"
    assert "continuation_id" not in candidate
    row = await db.fetchrow(
        "SELECT * FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1",
        permit.admission_id,
    )
    assert row["policy_version"] == 3 and row["job_retained_resume_id"] is None
    assert json.loads(row["ready_retention_preflight"]) == ready_root_witness(candidate)
    replay = await acquire_cancel_retention(
        state["recovery"], job_id=state["job_id"], identity=state["identity"]
    )
    assert replay == permit
    provisioner.qualify_retained_ready_stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_initial_ready_retention_does_not_select_policy1_stop(db):
    from orchestrator.services.vm_job_cancel_retention import (
        current_policy1_retention_parent,
    )

    state = await ready_root(db)
    provisioner = SimpleNamespace(
        qualify_retained_ready_stop=AsyncMock(side_effect=ready_root_witness)
    )
    permit = await acquire_retained_terminal_cleanup(
        state["recovery"],
        provisioner,
        job_id=state["job_id"],
        identity=state["identity"],
    )
    assert permit is not None and permit.allowed
    assert (
        await current_policy1_retention_parent(
            db,
            permit.parent_cleanup,
            job_id=state["job_id"],
            generation=state["generation"],
            vm_uid=state["frozen"]["vm_uid"],
            pvc_uid=state["frozen"]["pvc_uid"],
        )
        == "other"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("gate", ["false", "true"])
async def test_initial_ready_without_proof_never_falls_through_to_purge(
    db, monkeypatch, gate
):
    state = await ready_root(db)
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", gate)
    permit = await acquire_cancel_retention(
        state["recovery"], job_id=state["job_id"], identity=state["identity"]
    )
    assert permit is not None and not permit.allowed
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_cancel_retention_authorities")
        == 0
    )


@pytest.mark.asyncio
async def test_initial_ready_reader_requires_exact_committed_parent(db):
    from orchestrator.services.vm_job_retained_resume import (
        read_current_ready_preflight,
    )
    from shared.vm_resource_admission import ResourceAdmissionError

    state = await ready_root(db)
    provisioner = SimpleNamespace(
        qualify_retained_ready_stop=AsyncMock(side_effect=ready_root_witness)
    )
    permit = await acquire_retained_terminal_cleanup(
        state["recovery"],
        provisioner,
        job_id=state["job_id"],
        identity=state["identity"],
    )
    proof = await read_current_ready_preflight(
        db,
        permit.parent_cleanup,
        job_id=state["job_id"],
        generation=state["generation"],
    )
    assert proof == permit.parent_cleanup["retention_preflight"]
    for bad in (
        None,
        {k: v for k, v in permit.parent_cleanup.items() if k != "retention_preflight"},
    ):
        with pytest.raises(ResourceAdmissionError):
            await read_current_ready_preflight(
                db, bad, job_id=state["job_id"], generation=state["generation"]
            )


@pytest.mark.asyncio
async def test_ready_history_after_old_parent_cannot_convert_its_policy(db):
    state = await ready_root(db, old=True)
    provisioner = SimpleNamespace(
        qualify_retained_ready_stop=AsyncMock(side_effect=ready_root_witness)
    )
    permit = await acquire_retained_terminal_cleanup(
        state["recovery"],
        provisioner,
        job_id=state["job_id"],
        identity=state["identity"],
    )
    assert permit is not None and not permit.allowed
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_cancel_retention_authorities")
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT completed_at FROM vm_workspace_cleanup_admissions WHERE id=$1",
            state["cleanup_permit"].admission_id,
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "source_uid",
        "source_request",
        "ready_missing",
        "launcher_missing",
        "queue_active",
        "new_kind_as_old",
    ],
)
async def test_initial_ready_refuses_drift_without_authority_or_charge_change(
    db, fault
):
    from copy import deepcopy
    from shared.vm_resource_admission import ResourceAdmissionError

    state = await ready_root(db, ready_history=fault != "ready_missing")
    owner = UUID(state["job_id"])
    if fault == "source_uid":
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,vm_uid}','\"ffffffff-1111-4222-8333-ffffffffffff\"') WHERE id=$1",
            owner,
        )
    elif fault == "source_request":
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,creation_request_id}','\"ffffffff-1111-4222-8333-ffffffffffff\"') WHERE id=$1",
            owner,
        )
    elif fault == "launcher_missing":
        await db.execute(
            "UPDATE jobs SET context=context #- '{vm,active_pod_uid}' WHERE id=$1",
            owner,
        )
    elif fault == "queue_active":
        await db.execute("UPDATE run_queue SET state='queued' WHERE unit_id=$1", owner)

    def witness(candidate):
        result = deepcopy(ready_root_witness(candidate))
        if fault == "new_kind_as_old":
            result["kind"] = "vm_job_retained_ready_preflight_v1"
        return result

    before = await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE request_id=(SELECT request_id FROM vm_creation_retries WHERE job_id=$1)",
        owner,
    )
    provisioner = SimpleNamespace(
        qualify_retained_ready_stop=AsyncMock(side_effect=witness)
    )
    try:
        permit = await acquire_retained_terminal_cleanup(
            state["recovery"],
            provisioner,
            job_id=state["job_id"],
            identity=state["identity"],
        )
    except ResourceAdmissionError:
        pass
    else:
        assert permit is not None and not permit.allowed
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_cancel_retention_authorities")
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE request_id=(SELECT request_id FROM vm_creation_retries WHERE job_id=$1)",
            owner,
        )
        == before
    )


async def ready_stop_intent(db):
    from orchestrator.services.vm_pre_ssh_stop_store import VMPreSSHStopStore

    state = await ready_root(db)
    provisioner = SimpleNamespace(
        qualify_retained_ready_stop=AsyncMock(side_effect=ready_root_witness)
    )
    permit = await acquire_retained_terminal_cleanup(
        state["recovery"],
        provisioner,
        job_id=state["job_id"],
        identity=state["identity"],
    )
    state["retention"] = permit
    state["preflight"] = permit.parent_cleanup["retention_preflight"]
    frozen = {
        **state["frozen"],
        "kind": "vm_initial_ready_positive_stop_candidate_v1",
        "cleanup_admission_id": str(permit.admission_id),
        "cleanup_request_id": permit.parent_cleanup["request_id"],
        "cleanup_intent_digest": permit.parent_cleanup["intent_digest"],
    }
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"retiring_process_zero\"') WHERE id=$1",
        UUID(state["job_id"]),
    )
    store = VMPreSSHStopStore(db)
    wire = await store.admit_intent(
        state["job_id"],
        state["generation"],
        permit.parent_cleanup,
        frozen,
        retention_preflight=state["preflight"],
    )
    return state, store, wire


@pytest.mark.asyncio
async def test_ready_positive_stop_commits_exact_proof_before_zero(db):
    from tests.test_vm_pre_ssh_stop_protocol import proof

    state, store, wire = await ready_stop_intent(db)
    assert (
        await store.committed_proof(
            state["job_id"], state["generation"], state["retention"].parent_cleanup
        )
        is None
    )
    observed = proof(wire["frozen"])
    observed.update(
        kind="vm_initial_ready_positive_stop_v1",
        frozen_digest=wire["frozen_digest"],
        pod_intent_digest=wire["frozen_digest"],
    )
    result = await store.commit_positive_proof(
        state["job_id"],
        state["generation"],
        state["retention"].parent_cleanup,
        observed,
    )
    committed = await store.committed_proof(
        state["job_id"], state["generation"], state["retention"].parent_cleanup
    )
    assert committed["process_zero_receipt_id"] == result["process_zero_receipt_id"]
    assert committed["terminal_evidence"] == observed
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["preflight"]["frozen"]["reservation_id"]),
        )
        == "teardown"
    )
    assert (
        await db.fetchval(
            "SELECT completed_at FROM vm_workspace_cleanup_admissions WHERE id=$1",
            state["retention"].admission_id,
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ssh", ["missing", "failed", "lost_release", "stale_generation"]
)
async def test_actual_ready_release_uses_positive_stop_then_signed_retained_settlement(
    db, monkeypatch, ssh
):
    from orchestrator.services.vm_provisioner import (
        VMProvisioner,
        VMTeardownIdentity,
        _VMTeardownProbe,
    )
    from orchestrator.services.vm_workspace_recovery_store import (
        complete_vm_cleanup_permit,
    )
    from shared.vm_cancel_retention import retained_rootdisk_from_preflight
    from tests.test_vm_pre_ssh_stop_protocol import proof

    state = await ready_root(db)
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._controller_url = "http://controller.invalid"
    provisioner.qualify_retained_ready_stop = AsyncMock(side_effect=ready_root_witness)
    permit = await acquire_retained_terminal_cleanup(
        state["recovery"],
        provisioner,
        job_id=state["job_id"],
        identity=state["identity"],
    )
    retained = retained_rootdisk_from_preflight(
        permit.parent_cleanup["retention_preflight"]
    )
    identity = VMTeardownIdentity(
        state["generation"],
        state["frozen"]["vm_uid"],
        state["frozen"]["pvc_uid"],
        ssh_host="192.0.2.1" if ssh == "failed" else None,
        ssh_port=22 if ssh == "failed" else None,
        ssh_host_key_fingerprint="SHA256:known" if ssh == "failed" else None,
        credential_runtime_started=True,
    )
    state["identity"] = identity
    deleted = False
    events = []

    async def probe(job_id, generation, **kwargs):
        assert job_id == state["job_id"] and generation == state["generation"]
        if not deleted:
            return _VMTeardownProbe("present", identity, rootdisk_identity_known=True)
        return _VMTeardownProbe(
            "absent",
            VMTeardownIdentity(generation, None, identity.rootdisk_pvc_uid),
            rootdisk_identity_known=True,
            runtime_absence_known=True,
            vmi_absent=True,
            launcher_absent=True,
            retained_rootdisk=retained,
        )

    async def transport(payload):
        events.append(payload["action"])
        if payload["action"] == "inspect_initial_ready":
            assert payload["parent_cleanup"] == permit.parent_cleanup
            frozen = {
                **state["frozen"],
                "kind": "vm_initial_ready_positive_stop_candidate_v1",
                "cleanup_admission_id": str(permit.admission_id),
                "cleanup_request_id": permit.parent_cleanup["request_id"],
                "cleanup_intent_digest": permit.parent_cleanup["intent_digest"],
            }
            return {
                "status": "candidate",
                "frozen": frozen,
                "retention_preflight": permit.parent_cleanup["retention_preflight"],
                "_identity_authenticated": True,
            }
        if payload["action"] == "stop":
            if ssh == "stale_generation":
                await db.execute(
                    "UPDATE jobs SET context=jsonb_set(context,'{vm,provision_generation}','\"ffffffff-1111-4222-8333-ffffffffffff\"') WHERE id=$1",
                    UUID(state["job_id"]),
                )
            assert (
                await db.fetchval("SELECT count(*) FROM vm_pre_ssh_stop_intents") == 1
            )
            assert (
                await db.fetchval(
                    "SELECT count(*) FROM managed_repository_process_zero_receipts WHERE owner_id=$1",
                    UUID(state["job_id"]),
                )
                == 0
            )
            terminal = proof(payload["frozen"])
            terminal.update(
                kind="vm_initial_ready_positive_stop_v1",
                frozen_digest=payload["frozen_digest"],
                pod_intent_digest=payload["frozen_digest"],
            )
            return {
                "status": "positive_terminal_proof",
                "terminal_evidence": terminal,
                "_identity_authenticated": True,
            }
        assert payload["action"] == "release"
        assert (
            payload["retention_preflight"]
            == permit.parent_cleanup["retention_preflight"]
        )
        assert (
            await db.fetchval(
                "SELECT count(*) FROM managed_repository_process_zero_receipts WHERE owner_id=$1",
                UUID(state["job_id"]),
            )
            == 1
        )
        if ssh == "lost_release" and events.count("release") == 1:
            return None
        return {"status": "finalizer_released", "_identity_authenticated": True}

    async def delete(job_id, **kwargs):
        nonlocal deleted
        assert job_id == state["job_id"] and kwargs["purge_disk"] is False
        assert kwargs["parent_cleanup"] == permit.parent_cleanup
        assert events[-1] == "release"
        deleted = True
        return True

    provisioner._probe_vm_teardown_identity = AsyncMock(side_effect=probe)
    provisioner._request_pre_ssh_stop = AsyncMock(side_effect=transport)
    provisioner._delete_http = AsyncMock(side_effect=delete)
    monkeypatch.setattr(
        "orchestrator.services.vm_provisioner.retire_managed_repository_processes",
        AsyncMock(return_value=False),
    )
    result = await provisioner.release_vm_captured(
        state["job_id"],
        identity,
        purge_disk=False,
        capture_snapshot=False,
        parent_cleanup=permit.parent_cleanup,
    )
    if ssh == "stale_generation":
        assert result.disposition == "process_zero_unproven"
        assert (
            await db.fetchval(
                "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1",
                UUID(state["job_id"]),
            )
            == 0
        )
        assert (
            await db.fetchval(
                "SELECT count(*) FROM managed_repository_process_zero_receipts WHERE owner_id=$1",
                UUID(state["job_id"]),
            )
            == 0
        )
        assert (
            await db.fetchval(
                "SELECT state FROM vm_resource_reservations WHERE id=$1",
                UUID(
                    permit.parent_cleanup["retention_preflight"]["frozen"][
                        "reservation_id"
                    ]
                ),
            )
            == "teardown"
        )
        return
    if ssh == "lost_release":
        assert result.disposition == "process_zero_unproven"
        result = await provisioner.release_vm_captured(
            state["job_id"],
            identity,
            purge_disk=False,
            capture_snapshot=False,
            parent_cleanup=permit.parent_cleanup,
        )
    assert result.disposition == "completed"
    assert events == ["inspect_initial_ready", "stop", "release"] + (
        ["release"] if ssh == "lost_release" else []
    )
    await complete_vm_cleanup_permit(
        state["recovery"], permit, outcome="completed", provisioner=provisioner
    )
    assert await db.fetchval(
        "SELECT public.vm_job_cancel_retention_settled($1)", permit.admission_id
    )
    assert await db.complete_stateless_cancel_cleanup(state["job_id"])
    vm = json.loads(
        await db.fetchval(
            "SELECT context->'vm' FROM jobs WHERE id=$1", UUID(state["job_id"])
        )
    )
    assert vm["compute_released"] is True and vm["disk_kept"] is True
    assert vm["rootdisk_pvc_uid"] == identity.rootdisk_pvc_uid


@pytest.mark.asyncio
@pytest.mark.parametrize("purge", [False, True])
async def test_native_ready_parent_without_policy3_cannot_commit(db, purge):
    from orchestrator.services.vm_workspace_recovery_store import (
        acquire_vm_cleanup_permit,
    )

    state = await ready_root(db)
    with pytest.raises(
        asyncpg.CheckViolationError, match="Initial Ready stop bootstrap"
    ):
        await acquire_vm_cleanup_permit(
            state["recovery"],
            owner_kind="job",
            owner_id=state["job_id"],
            identity=state["identity"],
            source="job_terminal_vm_release",
            purge_disk=purge,
        )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE source='job_terminal_vm_release' AND owner_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )


async def ssh_settled_ready(db):
    from orchestrator.services.vm_workspace_recovery_store import (
        complete_vm_cleanup_permit,
    )
    from shared.vm_cancel_retention import retained_rootdisk_from_preflight

    state = await ready_root(db)
    provisioner = SimpleNamespace(
        qualify_retained_ready_stop=AsyncMock(side_effect=ready_root_witness)
    )
    permit = await acquire_retained_terminal_cleanup(
        state["recovery"],
        provisioner,
        job_id=state["job_id"],
        identity=state["identity"],
    )
    assert await db.claim_managed_repository_workspace_retirement(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["generation"],
    )
    assert await db.record_managed_repository_workspace_process_zero(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["generation"],
    )
    evidence = dict(
        version=1,
        kind="vm_cleanup_physical_stop",
        job_id=state["job_id"],
        provision_generation=state["generation"],
        **{
            key: state["frozen"][key]
            for key in ("vm_uid", "vmi_uid", "launcher_uid", "pvc_uid")
        },
        vm_absent=True,
        vmi_absent=True,
        launcher_absent=True,
        same_generation_replacement=False,
        pvc_disposition="retained",
        controller_authenticated=True,
        retained_rootdisk=retained_rootdisk_from_preflight(
            permit.parent_cleanup["retention_preflight"]
        ),
    )
    provisioner.attest_vm_cleanup_stop = AsyncMock(return_value=evidence)
    await complete_vm_cleanup_permit(
        state["recovery"], permit, outcome="completed", provisioner=provisioner
    )
    assert await db.complete_stateless_cancel_cleanup(state["job_id"])
    state["retention"] = permit
    return state


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", [2, 3])
async def test_deleted_controller_status_replays_exact_open_ready_cleanup(db, policy):
    """Controller DELETE may precede a conclusive whole-runtime stop probe."""
    from orchestrator.services.vm_job_retained_resume import (
        read_current_ready_preflight,
    )
    from orchestrator.services.vm_workspace_recovery_store import (
        complete_vm_cleanup_permit,
        prepare_vm_cleanup_resource,
    )
    from shared.vm_cancel_retention import retained_rootdisk_from_preflight
    from tests.test_vm_job_retained_resume_real_postgres import ready_keep

    if policy == 2:
        state = await ready_keep(db)
        permit = state["keep"]
    else:
        state = await ready_root(db)
        provisioner = SimpleNamespace(
            qualify_retained_ready_stop=AsyncMock(side_effect=ready_root_witness)
        )
        permit = await acquire_retained_terminal_cleanup(
            state["recovery"],
            provisioner,
            job_id=state["job_id"],
            identity=state["identity"],
        )
    assert permit.allowed
    candidate = await prepare_vm_cleanup_resource(state["recovery"], permit)
    assert (
        candidate["retention_preflight"] == permit.parent_cleanup["retention_preflight"]
    )
    assert await db.claim_managed_repository_workspace_retirement(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["identity"].provision_generation,
    )
    assert await db.record_managed_repository_workspace_process_zero(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["identity"].provision_generation,
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"deleted\"') WHERE id=$1",
        UUID(state["job_id"]),
    )
    # Ordinary retry must recover its same committed parent and frozen proof.
    assert (
        await read_current_ready_preflight(
            db,
            permit.parent_cleanup,
            job_id=state["job_id"],
            generation=state["identity"].provision_generation,
        )
        == permit.parent_cleanup["retention_preflight"]
    )
    replay = await acquire_cancel_retention(
        state["recovery"],
        job_id=state["job_id"],
        identity=state["identity"],
    )
    assert replay == permit
    assert await prepare_vm_cleanup_resource(state["recovery"], replay) == candidate
    evidence = dict(
        version=1,
        kind="vm_cleanup_physical_stop",
        **{
            key: candidate[key]
            for key in (
                "job_id",
                "provision_generation",
                "vm_uid",
                "vmi_uid",
                "launcher_uid",
                "pvc_uid",
            )
        },
        vm_absent=True,
        vmi_absent=True,
        launcher_absent=True,
        same_generation_replacement=False,
        pvc_disposition="retained",
        controller_authenticated=True,
        retained_rootdisk=retained_rootdisk_from_preflight(
            candidate["retention_preflight"]
        ),
    )
    witness = SimpleNamespace(attest_vm_cleanup_stop=AsyncMock(return_value=evidence))
    await complete_vm_cleanup_permit(
        state["recovery"],
        replay,
        outcome="completed",
        provisioner=witness,
    )
    assert await db.fetchval(
        "SELECT public.vm_job_cancel_retention_settled($1)", permit.admission_id
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(
                permit.parent_cleanup["retention_preflight"]["frozen"]["reservation_id"]
            ),
        )
        == "released"
    )
    assert await db.complete_stateless_cancel_cleanup(state["job_id"])


@pytest.mark.asyncio
async def test_retained_ready_acquisition_fences_charge_before_controller_delete(db):
    """The admitted policy-2 parent must own teardown before external DELETE."""
    from tests.test_vm_job_retained_resume_real_postgres import ready_keep

    state = await ready_keep(db)
    charge_id = UUID(state["ready_proof"]["frozen"]["reservation_id"])
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1", charge_id
        )
        == "teardown"
    )
    assert await db.claim_managed_repository_workspace_retirement(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["identity"].provision_generation,
    )
    assert await db.record_managed_repository_workspace_process_zero(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["identity"].provision_generation,
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"deleted\"') WHERE id=$1",
        UUID(state["job_id"]),
    )
    replay = await acquire_cancel_retention(
        state["recovery"], job_id=state["job_id"], identity=state["identity"]
    )
    assert replay == state["keep"]


async def historical_unprepared_ready_keep(db):
    """Admit the old policy-2 parent without bypassing any SQL transition guard."""
    from orchestrator.services import vm_workspace_recovery_store as cleanup_module
    from orchestrator.services.vm_job_retained_resume import retained_ready_candidate
    from tests.test_vm_job_retained_resume_real_postgres import (
        adopted_resume,
        ready_witness,
    )

    state = await adopted_resume(db)
    state["ready_proof"] = ready_witness(
        await retained_ready_candidate(
            db, job_id=state["job_id"], identity=state["identity"]
        )
    )
    state["recovery"] = cleanup_module.VMWorkspaceRecoveryStore(db)
    # Only the final policy-2 acquisition emulates the deployed pre-fix code.
    # Ancestor Resume setup and every database constraint remain real.
    with patch.object(
        cleanup_module, "prepare_vm_cleanup_resource", AsyncMock(return_value={})
    ) as skipped:
        state["keep"] = await acquire_cancel_retention(
            state["recovery"],
            job_id=state["job_id"],
            identity=state["identity"],
            retention_preflight=state["ready_proof"],
        )
        skipped.assert_awaited_once()
    assert state["keep"].allowed
    return state


@pytest.mark.asyncio
async def test_retained_ready_replay_recovers_historical_active_deleted_charge(db):
    """A committed exact parent can fence the charge missed by old DELETE order."""

    state = await historical_unprepared_ready_keep(db)
    charge_id = UUID(state["ready_proof"]["frozen"]["reservation_id"])
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1", charge_id
        )
        == "active"
    )
    assert await db.record_managed_repository_workspace_process_zero(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["identity"].provision_generation,
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"deleted\"') WHERE id=$1",
        UUID(state["job_id"]),
    )
    replay = await acquire_cancel_retention(
        state["recovery"], job_id=state["job_id"], identity=state["identity"]
    )
    assert replay == state["keep"]
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1", charge_id
        )
        == "teardown"
    )


@pytest.mark.asyncio
async def test_retained_ready_replay_rolls_back_charge_on_queue_drift(db):
    """A no-longer-done worker queue cannot launder replay preparation."""

    state = await historical_unprepared_ready_keep(db)
    charge_id = UUID(state["ready_proof"]["frozen"]["reservation_id"])
    assert await db.record_managed_repository_workspace_process_zero(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["identity"].provision_generation,
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"deleted\"') WHERE id=$1",
        UUID(state["job_id"]),
    )
    await db.execute(
        "UPDATE run_queue SET state='queued' WHERE unit_id=$1", UUID(state["job_id"])
    )
    with pytest.raises(asyncpg.CheckViolationError, match="current authority changed"):
        await acquire_cancel_retention(
            state["recovery"], job_id=state["job_id"], identity=state["identity"]
        )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1", charge_id
        )
        == "active"
    )


@pytest.mark.asyncio
async def test_retained_ready_replay_rolls_back_charge_on_storage_drift(db):
    """An unrelated workspace binding cannot be laundered by replay prepare."""

    state = await historical_unprepared_ready_keep(db)
    charge_id = UUID(state["ready_proof"]["frozen"]["reservation_id"])
    assert await db.record_managed_repository_workspace_process_zero(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["identity"].provision_generation,
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(jsonb_set(context,'{vm,status}',"
        "'\"deleted\"'),'{vm,workspace_storage}',"
        '\'{"id":"changed"}\'::jsonb) WHERE id=$1',
        UUID(state["job_id"]),
    )
    with pytest.raises(asyncpg.CheckViolationError, match="current authority changed"):
        await acquire_cancel_retention(
            state["recovery"], job_id=state["job_id"], identity=state["identity"]
        )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1", charge_id
        )
        == "active"
    )


@pytest.mark.asyncio
async def test_deleted_controller_status_replays_exact_open_never_ready_cleanup(db):
    from orchestrator.services.vm_workspace_recovery_store import (
        complete_vm_cleanup_permit,
        prepare_vm_cleanup_resource,
    )
    from tests.test_vm_job_cancel_retention_real_postgres import positive_retention

    state = await positive_retention(db)
    permit = state["retention"]
    assert await prepare_vm_cleanup_resource(state["recovery"], permit)
    assert await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts "
        "WHERE owner_kind='job' AND owner_id=$1 AND scope='vm' AND provisioner='vm' "
        "AND runtime_incarnation=$2)",
        UUID(state["job_id"]),
        state["generation"],
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"deleted\"') WHERE id=$1",
        UUID(state["job_id"]),
    )
    replay = await acquire_cancel_retention(
        state["recovery"],
        job_id=state["job_id"],
        identity=state["identity"],
    )
    assert replay == permit
    witness = SimpleNamespace(
        attest_vm_cleanup_stop=AsyncMock(return_value=state["evidence"])
    )
    await complete_vm_cleanup_permit(
        state["recovery"],
        replay,
        outcome="completed",
        provisioner=witness,
    )
    assert await db.fetchval(
        "SELECT public.vm_job_cancel_retention_settled($1)", permit.admission_id
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", [1, 2, 3])
async def test_deleted_status_never_admits_fresh_retention(db, policy):
    from tests.test_vm_job_retained_resume_real_postgres import (
        adopted_resume,
        ready_witness,
    )
    from orchestrator.services.vm_job_retained_resume import retained_ready_candidate

    if policy == 1:
        state = await cancelled(db, old=False, retiring=False)
    elif policy == 2:
        state = await adopted_resume(db)
        proof = ready_witness(
            await retained_ready_candidate(
                db,
                job_id=state["job_id"],
                identity=state["identity"],
            )
        )
    else:
        state = await ready_root(db)
    assert await db.claim_managed_repository_workspace_retirement(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["identity"].provision_generation,
    )
    assert await db.record_managed_repository_workspace_process_zero(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["identity"].provision_generation,
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"deleted\"') WHERE id=$1",
        UUID(state["job_id"]),
    )
    prior_count = await db.fetchval(
        "SELECT count(*) FROM vm_job_cancel_retention_authorities WHERE job_id=$1",
        UUID(state["job_id"]),
    )
    if policy == 3:
        permit = await acquire_retained_terminal_cleanup(
            state["recovery"],
            SimpleNamespace(
                qualify_retained_ready_stop=AsyncMock(side_effect=ready_root_witness)
            ),
            job_id=state["job_id"],
            identity=state["identity"],
        )
    else:
        permit = await acquire_cancel_retention(
            state["recovery"],
            job_id=state["job_id"],
            identity=state["identity"],
            retention_preflight=proof if policy == 2 else None,
        )
    assert permit is not None and not permit.allowed
    assert prior_count == await db.fetchval(
        "SELECT count(*) FROM vm_job_cancel_retention_authorities WHERE job_id=$1",
        UUID(state["job_id"]),
    )


@pytest.mark.asyncio
async def test_deleted_replay_stays_held_without_process_zero_or_physical_absence(db):
    from orchestrator.services.vm_job_retained_resume import (
        read_current_ready_preflight,
    )
    from orchestrator.services.vm_workspace_recovery_store import (
        complete_vm_cleanup_permit,
        prepare_vm_cleanup_resource,
    )
    from shared.vm_resource_admission import ResourceAdmissionError

    state = await ready_root(db)
    permit = await acquire_retained_terminal_cleanup(
        state["recovery"],
        SimpleNamespace(
            qualify_retained_ready_stop=AsyncMock(side_effect=ready_root_witness)
        ),
        job_id=state["job_id"],
        identity=state["identity"],
    )
    candidate = await prepare_vm_cleanup_resource(state["recovery"], permit)
    with pytest.raises(asyncpg.CheckViolationError, match="process-zero authority"):
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"deleted\"') WHERE id=$1",
            UUID(state["job_id"]),
        )
    assert await db.claim_managed_repository_workspace_retirement(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["identity"].provision_generation,
    )
    assert await db.record_managed_repository_workspace_process_zero(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["identity"].provision_generation,
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"deleted\"') WHERE id=$1",
        UUID(state["job_id"]),
    )
    assert (
        await read_current_ready_preflight(
            db,
            permit.parent_cleanup,
            job_id=state["job_id"],
            generation=state["identity"].provision_generation,
        )
        == permit.parent_cleanup["retention_preflight"]
    )
    with pytest.raises(ResourceAdmissionError, match="stop_unproven"):
        await complete_vm_cleanup_permit(
            state["recovery"],
            permit,
            outcome="completed",
            provisioner=SimpleNamespace(
                attest_vm_cleanup_stop=AsyncMock(return_value=None)
            ),
        )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(candidate["retention_preflight"]["frozen"]["reservation_id"]),
        )
        == "teardown"
    )
    assert await db.fetchval(
        "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
        permit.admission_id,
    )


@pytest.mark.asyncio
async def test_deleted_replay_refuses_changed_parent_revision_and_storage(db):
    from orchestrator.services.vm_workspace_recovery_store import (
        prepare_vm_cleanup_resource,
    )

    state = await ready_root(db)
    permit = await acquire_retained_terminal_cleanup(
        state["recovery"],
        SimpleNamespace(
            qualify_retained_ready_stop=AsyncMock(side_effect=ready_root_witness)
        ),
        job_id=state["job_id"],
        identity=state["identity"],
    )
    await prepare_vm_cleanup_resource(state["recovery"], permit)
    assert await db.claim_managed_repository_workspace_retirement(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["identity"].provision_generation,
    )
    assert await db.record_managed_repository_workspace_process_zero(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["identity"].provision_generation,
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"deleted\"') WHERE id=$1",
        UUID(state["job_id"]),
    )
    authority = await db.fetchrow(
        "SELECT * FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1",
        permit.admission_id,
    )
    query = (
        "SELECT public.vm_job_deleted_retention_replay_allowed("
        "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)"
    )
    args = [
        3,
        authority["job_id"],
        authority["provision_generation"],
        authority["vm_uid"],
        authority["pvc_uid"],
        permit.admission_id,
        True,
        authority["creation_request_id"],
        authority["reservation_id"],
        authority["reservation_revision"],
        authority["vmi_uid"],
        authority["launcher_uid"],
        authority["node_uid"],
    ]
    assert await db.fetchval(query, *args)
    for index, wrong in ((5, uuid4()), (9, args[9] + 1), (2, uuid4())):
        changed = args.copy()
        changed[index] = wrong
        assert not await db.fetchval(query, *changed)
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,workspace_storage}',"
        '\'{"id":"changed"}\'::jsonb) WHERE id=$1',
        UUID(state["job_id"]),
    )
    with pytest.raises(asyncpg.CheckViolationError, match="current authority changed"):
        await db.fetchval(
            "SELECT public.validate_vm_job_cancel_retention($1,false)",
            permit.admission_id,
        )
    assert await db.fetchval(
        "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
        permit.admission_id,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("held", [False, True])
async def test_settled_initial_ready_keeps_bytes_but_unknown_execution_remains_held(
    db, held
):
    state = await ssh_settled_ready(db)
    owner = UUID(state["job_id"])
    if held:
        await db.execute(
            'UPDATE jobs SET context=context||\'{"_worker_execution_hold":{"version":1,"reason":"executor_loss_unknown_tool_effect"}}\'::jsonb WHERE id=$1',
            owner,
        )
    actor = await db.fetchval("SELECT user_id FROM jobs WHERE id=$1", owner)
    resumed = await db.prepare_stateless_job_for_workspace_resume(
        state["job_id"], "vm", expected_status="cancelled", owner_resume_user_id=actor
    )
    assert resumed is not held
    assert await db.fetchval(
        "SELECT count(*) FROM vm_job_retained_resumes WHERE job_id=$1", owner
    ) == (0 if held else 1)
    if held:
        assert await db.fetchval(
            "SELECT context ? '_worker_execution_hold' FROM jobs WHERE id=$1", owner
        )
    else:
        row = await db.fetchrow(
            "SELECT * FROM vm_job_retained_resumes WHERE job_id=$1", owner
        )
        assert row["physical_cleanup_admission_id"] == state["retention"].admission_id
        assert str(row["pvc_uid"]) == state["frozen"]["pvc_uid"]


async def native_ready_intent(conn, state, permit):
    frozen = {
        **state["frozen"],
        "kind": "vm_initial_ready_positive_stop_candidate_v1",
        "cleanup_admission_id": str(permit.admission_id),
        "cleanup_request_id": permit.parent_cleanup["request_id"],
        "cleanup_intent_digest": permit.parent_cleanup["intent_digest"],
    }
    await conn.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"retiring_process_zero\"') WHERE id=$1",
        UUID(state["job_id"]),
    )
    return await conn.fetchrow(
        "INSERT INTO vm_pre_ssh_stop_intents (cleanup_admission_id,job_id,provision_generation,creation_request_id,reservation_id,reservation_revision,vm_uid,vmi_uid,launcher_uid,pvc_uid,node_uid,cleanup_intent_digest,frozen,frozen_digest,retention_preflight) "
        "SELECT cleanup_admission_id,job_id,provision_generation,creation_request_id,reservation_id,reservation_revision,vm_uid,vmi_uid,launcher_uid,pvc_uid,node_uid,intent_digest,$2::jsonb,'sha256:'||encode(sha256(convert_to($2::jsonb::text,'UTF8')),'hex'),ready_retention_preflight FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1 RETURNING *",
        permit.admission_id,
        json.dumps(frozen),
    )


async def native_ready_proof(conn, row, observed):
    return await conn.execute(
        "INSERT INTO vm_pre_ssh_stop_proofs (cleanup_admission_id,job_id,provision_generation,frozen_digest,terminal_evidence,evidence_digest) VALUES($1,$2,$3,$4,$5::jsonb,'sha256:'||encode(sha256(convert_to($5::jsonb::text,'UTF8')),'hex'))",
        row["cleanup_admission_id"],
        row["job_id"],
        row["provision_generation"],
        row["frozen_digest"],
        json.dumps(observed),
    )


@pytest.mark.asyncio
async def test_actual_ready_stop_cannot_send_uncommitted_new_intent(db):
    from contextlib import asynccontextmanager

    from orchestrator.services.vm_provisioner import VMProvisioner

    state = await ready_root(db)
    provisioner = VMProvisioner()
    provisioner.qualify_retained_ready_stop = AsyncMock(side_effect=ready_root_witness)
    permit = await acquire_retained_terminal_cleanup(
        state["recovery"],
        provisioner,
        job_id=state["job_id"],
        identity=state["identity"],
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"retiring_process_zero\"') WHERE id=$1",
        UUID(state["job_id"]),
    )
    frozen = {
        **state["frozen"],
        "kind": "vm_initial_ready_positive_stop_candidate_v1",
        "cleanup_admission_id": str(permit.admission_id),
        "cleanup_request_id": permit.parent_cleanup["request_id"],
        "cleanup_intent_digest": permit.parent_cleanup["intent_digest"],
    }
    events = []

    async def transport(payload):
        events.append(payload["action"])
        if payload["action"] == "inspect_initial_ready":
            return {
                "status": "candidate",
                "frozen": frozen,
                "retention_preflight": permit.parent_cleanup["retention_preflight"],
                "_identity_authenticated": True,
            }
        return None

    provisioner._request_pre_ssh_stop = AsyncMock(side_effect=transport)
    async with db.acquire() as conn, conn.transaction():

        @asynccontextmanager
        async def acquire():
            yield conn

        # Admission's nested transaction is only a savepoint here. The actual
        # provisioner must refuse a stop until the enclosing transaction commits.
        provisioner._db = SimpleNamespace(acquire=acquire)
        assert not await provisioner._attempt_pre_ssh_positive_stop(
            state["job_id"], state["identity"], permit.parent_cleanup
        )
        assert await conn.fetchval(
            "SELECT initial_ready_xact_id=pg_current_xact_id() "
            "FROM vm_pre_ssh_stop_intents WHERE cleanup_admission_id=$1",
            permit.admission_id,
        )
        assert events == ["inspect_initial_ready"]


@pytest.mark.asyncio
async def test_ready_positive_proof_cannot_use_uncommitted_intent(db):
    from tests.test_vm_pre_ssh_stop_protocol import proof

    state = await ready_root(db)
    provisioner = SimpleNamespace(
        qualify_retained_ready_stop=AsyncMock(side_effect=ready_root_witness)
    )
    permit = await acquire_retained_terminal_cleanup(
        state["recovery"],
        provisioner,
        job_id=state["job_id"],
        identity=state["identity"],
    )
    async with db.acquire() as conn, conn.transaction():
        row = await native_ready_intent(conn, state, permit)
        frozen = json.loads(row["frozen"])
        observed = proof(frozen)
        observed.update(
            kind="vm_initial_ready_positive_stop_v1",
            frozen_digest=row["frozen_digest"],
            pod_intent_digest=row["frozen_digest"],
        )
        with pytest.raises(asyncpg.CheckViolationError, match="committed"):
            async with conn.transaction():
                await native_ready_proof(conn, row, observed)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "old_kind",
        "missing_container",
        "running",
        "restart",
        "replacement",
        "node",
        "vmi",
        "unknown_termination",
        "foreign_vmi",
    ],
)
async def test_native_ready_positive_proof_refuses_partial_or_confused_evidence(
    db, fault
):
    from tests.test_vm_pre_ssh_stop_protocol import proof

    state = await ready_root(db)
    provisioner = SimpleNamespace(
        qualify_retained_ready_stop=AsyncMock(side_effect=ready_root_witness)
    )
    permit = await acquire_retained_terminal_cleanup(
        state["recovery"],
        provisioner,
        job_id=state["job_id"],
        identity=state["identity"],
    )
    async with db.acquire() as conn, conn.transaction():
        row = await native_ready_intent(conn, state, permit)
    observed = proof(json.loads(row["frozen"]))
    observed.update(
        kind="vm_initial_ready_positive_stop_v1",
        frozen_digest=row["frozen_digest"],
        pod_intent_digest=row["frozen_digest"],
    )
    if fault == "old_kind":
        observed["kind"] = "vm_pre_ssh_positive_stop_v1"
    elif fault == "missing_container":
        observed["containers"].pop()
    elif fault == "running":
        observed["containers"][0]["state"] = "running"
    elif fault == "restart":
        observed["containers"][0]["restart_count"] = 1
    elif fault == "replacement":
        observed["same_generation_replacement"] = True
    elif fault == "node":
        observed["node_ready"] = False
    elif fault == "vmi":
        observed["vmi_disposition"] = "running"
    elif fault == "unknown_termination":
        observed["containers"][0]["reason"] = "ContainerStatusUnknown"
    elif fault == "foreign_vmi":
        observed["vmi_uid"] = "ffffffff-1111-4222-8333-ffffffffffff"
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            async with conn.transaction():
                await native_ready_proof(conn, row, observed)
    assert await db.fetchval("SELECT count(*) FROM vm_pre_ssh_stop_proofs") == 0
    assert (
        await db.fetchval(
            "SELECT count(*) FROM managed_repository_process_zero_receipts WHERE owner_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            row["reservation_id"],
        )
        == "teardown"
    )


@pytest.mark.asyncio
async def test_initial_ready_retained_public_delete_uses_exact_typed_purge_and_keeps_audit(
    db,
):
    from contextlib import asynccontextmanager
    from functools import partial
    import logging
    from orchestrator.services import thread_retirement
    from orchestrator.services.job_mutation_controls import JobControlOperations
    from orchestrator.services.vm_workspace_policy import vm_needs_release
    from orchestrator.services.vm_provisioner import (
        VMProvisioner,
        VMTeardownIdentity,
        _VMTeardownProbe,
    )

    state = await ssh_settled_ready(db)
    owner = UUID(state["job_id"])
    # Unknown execution cannot Resume, but explicit permanent Delete remains a
    # separate destructive owner operation with its own native authority.
    await db.execute(
        'UPDATE jobs SET context=context||\'{"_worker_execution_hold":{"version":1}}\'::jsonb WHERE id=$1',
        owner,
    )
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._controller_url = "http://controller.invalid"
    purged = False
    parents = []

    async def probe(job_id, generation, **kwargs):
        assert job_id == state["job_id"] and generation == state["generation"]
        return _VMTeardownProbe(
            "absent",
            VMTeardownIdentity(
                generation, None, None if purged else state["frozen"]["pvc_uid"]
            ),
            rootdisk_identity_known=True,
            runtime_absence_known=True,
            vmi_absent=True,
            launcher_absent=True,
        )

    async def delete(job_id, **kwargs):
        nonlocal purged
        assert job_id == state["job_id"] and kwargs["purge_disk"] is True
        parent = kwargs["parent_cleanup"]
        assert parent["intent"]["source"] == "public_vm_delete"
        assert parent["intent"]["pvc_uid"] == state["frozen"]["pvc_uid"]
        assert await db.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_job_retained_disk_purge_authorities WHERE cleanup_admission_id=$1)",
            UUID(parent["admission_id"]),
        )
        assert not await db.fetchval(
            "SELECT public.vm_job_cancel_retention_discharged($1)",
            state["retention"].admission_id,
        )
        parents.append(parent["admission_id"])
        purged = True
        return True

    provisioner._probe_vm_teardown_identity = AsyncMock(side_effect=probe)
    provisioner._delete_http = AsyncMock(side_effect=delete)
    dependencies = SimpleNamespace(
        store=db,
        recovery_store=state["recovery"],
        vm_provisioner=provisioner,
        container_provisioner=None,
        docker_provisioner=None,
        get_container_context=lambda _: {},
        get_vm_context=lambda job: json.loads(job["context"])["vm"],
        vm_needs_release=vm_needs_release,
    )

    @asynccontextmanager
    async def vectors():
        yield SimpleNamespace(execute=AsyncMock())

    controls = JobControlOperations(
        SimpleNamespace(
            store=db,
            logger=logging.getLogger(__name__),
            archive_and_cleanup_workspace=partial(
                thread_retirement.archive_and_cleanup_workspace,
                dependencies=dependencies,
            ),
            snapshot_service=SimpleNamespace(is_available=False),
            vector_db=SimpleNamespace(acquire=vectors),
            resolve_job_notifications=AsyncMock(),
        )
    )
    job = await db.get_job(state["job_id"])
    result = await controls.delete(
        state["job_id"], caller={"id": str(job["user_id"])}, job=job
    )
    assert result["status"] == "deleted" and len(parents) == 1
    assert await db.get_job(state["job_id"]) is None
    assert await db.fetchval(
        "SELECT public.vm_job_cancel_retention_discharged($1)",
        state["retention"].admission_id,
    )
    assert (
        await db.fetchval(
            "SELECT policy_version FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1",
            state["retention"].admission_id,
        )
        == 3
    )


@pytest.mark.asyncio
async def test_actual_vm_loss_hold_survives_cancel_retention_and_forbids_resume(
    db, monkeypatch
):
    from orchestrator.services import run_queue_reaper
    from orchestrator.services.vm_provisioner import VMTeardownIdentity
    from orchestrator.services.vm_workspace_recovery_store import (
        VMWorkspaceRecoveryStore,
        complete_vm_cleanup_permit,
    )
    from shared.vm_cancel_retention import retained_rootdisk_from_preflight
    from shared import worker_queue
    from tests.test_vm_worker_executor_loss_replay_real_postgres import issued_vm_claim

    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")
    owner, claim = await issued_vm_claim(db)
    await db.execute(
        "UPDATE run_queue SET leased_until=clock_timestamp()-interval '1 second' WHERE unit_id=$1",
        owner,
    )
    async with db.acquire() as conn:
        await run_queue_reaper.reap_cycle(conn, grace_seconds=0)
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
    )
    hold = context["_worker_execution_hold"]
    binding = dict(
        await db.fetchrow(
            "SELECT * FROM vm_job_worker_delivery_bindings WHERE job_id=$1 AND lease_token=$2",
            owner,
            claim.lease_token,
        )
    )
    assert await worker_queue.claim_worker_batch(db, pod_name="must-not-replay") is None
    cancelled, _ = await db.cancel_stateless_job(str(owner))
    assert cancelled
    vm = context["vm"]
    identity = VMTeardownIdentity(
        vm["provision_generation"], vm["vm_uid"], vm["rootdisk_pvc_uid"]
    )
    recovery = VMWorkspaceRecoveryStore(db)
    provisioner = SimpleNamespace(
        qualify_retained_ready_stop=AsyncMock(side_effect=ready_root_witness)
    )
    permit = await acquire_retained_terminal_cleanup(
        recovery, provisioner, job_id=str(owner), identity=identity
    )
    assert permit.allowed and permit.parent_cleanup["intent"]["purge_disk"] is False
    assert await db.claim_managed_repository_workspace_retirement(
        str(owner),
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=identity.provision_generation,
    )
    assert await db.record_managed_repository_workspace_process_zero(
        str(owner),
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=identity.provision_generation,
    )
    frozen = permit.parent_cleanup["retention_preflight"]["frozen"]
    evidence = dict(
        version=1,
        kind="vm_cleanup_physical_stop",
        job_id=str(owner),
        provision_generation=identity.provision_generation,
        **{
            key: frozen[key] for key in ("vm_uid", "vmi_uid", "launcher_uid", "pvc_uid")
        },
        vm_absent=True,
        vmi_absent=True,
        launcher_absent=True,
        same_generation_replacement=False,
        pvc_disposition="retained",
        controller_authenticated=True,
        retained_rootdisk=retained_rootdisk_from_preflight(
            permit.parent_cleanup["retention_preflight"]
        ),
    )
    provisioner.attest_vm_cleanup_stop = AsyncMock(return_value=evidence)
    await complete_vm_cleanup_permit(
        recovery, permit, outcome="completed", provisioner=provisioner
    )
    assert await db.complete_stateless_cancel_cleanup(str(owner))
    after = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))
    assert after["_worker_execution_hold"] == hold
    assert after["vm"]["disk_kept"] is True and after["vm"]["compute_released"] is True
    actor = await db.fetchval("SELECT user_id FROM jobs WHERE id=$1", owner)
    assert not await db.prepare_stateless_job_for_workspace_resume(
        str(owner), "vm", expected_status="cancelled", owner_resume_user_id=actor
    )
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_job_worker_delivery_bindings WHERE job_id=$1 AND lease_token=$2",
                owner,
                claim.lease_token,
            )
        )
        == binding
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_job_retained_resumes WHERE job_id=$1", owner
        )
        == 0
    )


@pytest.mark.asyncio
async def test_native_resume_insert_cannot_clear_unknown_execution_hold(db):
    from uuid import uuid4, uuid5

    state = await ssh_settled_ready(db)
    owner = UUID(state["job_id"])
    await db.execute(
        'UPDATE jobs SET context=context||\'{"_worker_execution_hold":{"version":1}}\'::jsonb WHERE id=$1',
        owner,
    )
    op = uuid4()
    with pytest.raises(
        asyncpg.CheckViolationError, match="Retained Job Resume authority"
    ):
        await db.execute(
            "INSERT INTO vm_job_retained_resumes (id,job_id,root_retention_admission_id,physical_cleanup_admission_id,predecessor_request_id,predecessor_generation,pvc_uid,request_id,provision_generation,explicit_resume_id,requested_by,source_revision,retained_vm) "
            "SELECT $1,a.job_id,a.cleanup_admission_id,a.cleanup_admission_id,a.creation_request_id,a.provision_generation,a.pvc_uid,$2,$3,$4,j.user_id,r.revision,j.context->'vm' FROM vm_job_cancel_retention_authorities a JOIN jobs j ON j.id=a.job_id JOIN vm_creation_retries r ON r.request_id=a.creation_request_id WHERE a.cleanup_admission_id=$5",
            op,
            uuid5(op, "source"),
            uuid5(op, "provision"),
            uuid4(),
            state["retention"].admission_id,
        )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_job_retained_resumes WHERE job_id=$1", owner
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("tail", ["source_absent", "never_issued"])
async def test_initial_ready_root_reuses_full_same_pvc_continuation_and_public_delete(
    db, monkeypatch, tail
):
    from tests import test_vm_job_retained_resume_real_postgres as fixtures
    from tests.test_vm_job_retained_resume_multikeep_public_delete_real_postgres import (
        test_public_delete_purges_two_keeps_with_immutable_logical_tail,
    )

    roots = []

    async def ready_kept(source_db):
        state = await ssh_settled_ready(source_db)
        roots.append(state["retention"].admission_id)
        return state

    # Substitute only the test's initial physical keep fixture. The existing
    # native issuance, descendant keep/no-effect tail and actual public Delete
    # path run unchanged over the new policy3 root.
    monkeypatch.setattr(fixtures, "kept", ready_kept)
    await test_public_delete_purges_two_keeps_with_immutable_logical_tail(
        db, tail, monkeypatch
    )
    assert len(roots) == 1
    assert (
        await db.fetchval(
            "SELECT policy_version FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1",
            roots[0],
        )
        == 3
    )
    assert await db.fetchval(
        "SELECT public.vm_job_cancel_retention_discharged($1)", roots[0]
    )
