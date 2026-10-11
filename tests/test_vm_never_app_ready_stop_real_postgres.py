"""Committed policy-1 retained stop with an older process-zero receipt."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4, uuid5

import asyncpg
import pytest
import pytest_asyncio

from orchestrator.services.vm_pre_ssh_stop_store import (
    VMPreSSHStopConflict,
    VMPreSSHStopStore,
)
from orchestrator.services.vm_job_cancel_retention import current_policy1_stop_authority
from tests.test_vm_job_cancel_retention_real_postgres import (
    acquire,
    cancelled,
    preflight,
    terminal_proof,
)
from tests.test_vm_job_initial_ready_retention_real_postgres import (  # noqa: F401
    _base_db,
    _db_fixture,
    _pre_ssh_db,
    _retention_db,
    _resume_db,
    _schema_applied,
    db as _initial_ready_db,
    initial_ready_schema,
    pg_dsn,
    postgres_db_fixture,
    pre_ssh_schema,
    retention_schema,
    resume_schema,
    whole_schema,
)


@pytest_asyncio.fixture(scope="module")
async def held_stop_schema(pg_dsn, initial_ready_schema):  # noqa: F811
    root = (
        Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
    )
    conn = await asyncpg.connect(pg_dsn)
    try:
        installed = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='vm_pre_ssh_stop_intents' "
            "AND column_name='held_stop_xact_id')"
        )
        if not installed:
            for name in (
                "0431_vm_never_app_ready_retained_stop.sql",
                "0432_validate_vm_never_app_ready_retained_stop.sql",
            ):
                await conn.execute((root / name).read_text())
        late = root / "0433_vm_cancel_late_adopted_retention.sql"
        if late.exists() and not await conn.fetchval(
            "SELECT to_regprocedure("
            "'public.vm_cancel_late_adopted_retained_terminal(uuid)') IS NOT NULL"
        ):
            await conn.execute(late.read_text())
        assert (
            await conn.fetchval(
                "SELECT convalidated FROM pg_constraint "
                "WHERE conname='vm_pre_ssh_stop_prior_zero_fk'"
            )
            is True
        )
        yield
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(held_stop_schema, _initial_ready_db):  # noqa: F811
    yield _initial_ready_db


@pytest.fixture(autouse=True)
def enabled(monkeypatch):
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", "true")


def frozen_candidate(state, parent):
    return {
        **state["frozen"],
        "kind": "vm_job_never_app_ready_retained_stop_candidate_v1",
        "cleanup_admission_id": parent["admission_id"],
        "cleanup_request_id": parent["request_id"],
        "cleanup_intent_digest": parent["intent_digest"],
        "kube_vm_ready_at_inspection": True,
    }


async def held_state(db, *, zero=True):
    state = await cancelled(db, old=False)
    permit = await acquire(state)
    assert permit.allowed, permit.reason
    parent = permit.parent_cleanup
    state["frozen"] = frozen_candidate(state, parent)
    state["parent"] = parent
    state["preflight"] = preflight(state)
    state["preflight"]["pvc_name"] = f"agent-vm-{state['job_id']}-rootdisk"
    state["zero"] = None
    if zero:
        assert await db.record_managed_repository_workspace_process_zero(
            state["job_id"],
            owner_kind="job",
            scope="vm",
            provisioner="vm",
            runtime_incarnation=state["generation"],
        )
        state["zero"] = await db.fetchrow(
            "SELECT * FROM managed_repository_process_zero_receipts WHERE "
            "owner_kind='job' AND owner_id=$1 AND scope='vm' AND "
            "runtime_incarnation=$2",
            UUID(state["job_id"]),
            state["generation"],
        )
    return state


async def late_adopted_state(db, *, issued=False, wrong_effect=False):
    """The persisted Cancel-before-adoption shape, including an unbound charge."""
    state = await cancelled(
        db, old=False, retiring=False, bound=False, cancel_before_adoption=True
    )
    owner = UUID(state["job_id"])
    request = await db.fetchval(
        "SELECT request_id FROM vm_creation_retries WHERE job_id=$1", owner
    )
    for number, kind in enumerate(("rootdisk", "cloud_init", "vm"), 1):
        is_issued = issued and number == 3
        await db.execute(
            "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,"
            "effect_kind,carrier_uid,carrier_namespace,carrier_intent,state,evidence,"
            "resolved_at) VALUES($1,$2,$3,$4,$5,$6,'{}'::jsonb,$7,$8::jsonb,"
            "CASE WHEN $7='issued' THEN NULL ELSE clock_timestamp() END)",
            uuid4(),
            request,
            number,
            kind,
            uuid4(),
            state["frozen"]["namespace"],
            "issued" if is_issued else "observed",
            "{}"
            if is_issued
            else json.dumps(
                {
                    "outcome": "observed",
                    "kind": kind,
                    **(
                        {"pvc_uid": state["frozen"]["pvc_uid"]}
                        if kind in {"rootdisk", "vm"}
                        else {}
                    ),
                    **(
                        {
                            "uid": str(uuid4())
                            if wrong_effect
                            else state["frozen"]["vm_uid"]
                        }
                        if kind == "vm"
                        else {}
                    ),
                }
            ),
        )
    return state


async def complete_synthetic_retained_delete(db, recovery, purge):
    from orchestrator.services.vm_job_retained_disk_purge import (
        complete_job_retained_disk_purge,
        read_job_retained_disk_purge_candidate,
    )

    candidate = await read_job_retained_disk_purge_candidate(recovery, purge)
    evidence = {
        "version": 1,
        "kind": "vm_cleanup_physical_stop",
        "job_id": candidate["job_id"],
        "provision_generation": candidate["provision_generation"],
        "vm_uid": candidate["vm_uid"],
        "vmi_uid": candidate["vmi_uid"],
        "launcher_uid": candidate["launcher_uid"],
        "pvc_uid": candidate["pvc_uid"],
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "controller_authenticated": True,
        "pvc_disposition": "purged",
        "controller_scope": candidate["controller_scope"],
    }
    await complete_job_retained_disk_purge(
        recovery,
        purge,
        outcome="completed",
        provisioner=SimpleNamespace(
            attest_vm_cleanup_stop=AsyncMock(return_value=evidence)
        ),
    )
    assert await db.fetchval(
        "SELECT public.validate_vm_job_retained_disk_purge($1,true)",
        purge.admission_id,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("deleted_before_completion", [False, True])
async def test_cancel_before_late_adoption_binds_then_commits_held_stop(
    db, deleted_before_completion
):
    state = await late_adopted_state(db)
    assert not (await acquire(state)).allowed  # No signed runtime inventory yet.
    from tests.test_vm_job_cancel_retention_real_postgres import (
        publish_cancelled_launcher,
    )

    await publish_cancelled_launcher(state)
    permit = await acquire(state)
    assert permit.allowed, permit.reason
    assert permit.parent_cleanup["intent"]["purge_disk"] is False
    charge = await db.fetchrow(
        "SELECT state,vm_uid,vmi_uid,launcher_uid FROM vm_resource_reservations WHERE id=$1",
        UUID(state["reservation_id"]),
    )
    assert charge["state"] == "teardown"
    assert str(charge["vm_uid"]) == state["frozen"]["vm_uid"]
    assert str(charge["vmi_uid"]) == state["frozen"]["vmi_uid"]
    assert str(charge["launcher_uid"]) == state["frozen"]["launcher_uid"]
    assert (
        await db.fetchval(
            "SELECT ready_at FROM vm_creation_retries WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        is None
    )
    assert (
        await db.fetchval(
            "SELECT context->'vm'->>'status' FROM jobs WHERE id=$1",
            UUID(state["job_id"]),
        )
        == "waiting_creation_configuration"
    )
    # The ordinary release path moves only this cancelled owner into retirement.
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',"
        "'\"retiring_process_zero\"') WHERE id=$1",
        UUID(state["job_id"]),
    )
    frozen = frozen_candidate(state, permit.parent_cleanup)
    state["frozen"] = frozen
    preflight_value = preflight(state)
    preflight_value["pvc_name"] = f"agent-vm-{state['job_id']}-rootdisk"
    intent = await state["store"].admit_intent(
        state["job_id"],
        state["generation"],
        permit.parent_cleanup,
        frozen,
        retention_preflight=preflight_value,
    )
    assert intent["frozen"] == frozen
    proof = terminal_proof(frozen, intent["frozen_digest"])
    proof["kind"] = "vm_job_never_app_ready_retained_positive_stop_v1"
    committed = await state["store"].commit_positive_proof(
        state["job_id"], state["generation"], permit.parent_cleanup, proof
    )
    assert committed["process_zero_receipt_id"]
    assert not await db.fetchval(
        "SELECT public.vm_cancel_late_adopted_retained_terminal($1)",
        permit.admission_id,
    )  # Stop proof alone is not a completed retained cleanup.
    if deleted_before_completion:
        # _delete_http projects deleted after genuine Controller DELETE and
        # before the retention cleanup receipt/charge release transaction.
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',"
            "'\"deleted\"') WHERE id=$1",
            UUID(state["job_id"]),
        )
        open_replay = await acquire(state)
        assert open_replay.allowed and open_replay.admission_id == permit.admission_id
    from orchestrator.services.vm_job_cancel_retention import (
        complete_cancel_retention_marker,
    )
    from orchestrator.services.vm_workspace_recovery_store import (
        complete_vm_cleanup_permit,
    )

    evidence = {
        "version": 1,
        "kind": "vm_cleanup_physical_stop",
        "job_id": state["job_id"],
        "provision_generation": state["generation"],
        "vm_uid": frozen["vm_uid"],
        "vmi_uid": frozen["vmi_uid"],
        "launcher_uid": frozen["launcher_uid"],
        "pvc_uid": frozen["pvc_uid"],
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "pvc_disposition": "retained",
        "controller_authenticated": True,
        "retained_rootdisk": {
            "version": 1,
            "kind": "vm_retained_rootdisk_v1",
            "namespace": frozen["namespace"],
            "owner_kind": "job",
            "owner_id": state["job_id"],
            "pvc_name": preflight_value["pvc_name"],
            "pvc_uid": frozen["pvc_uid"],
            "dv_uid": preflight_value["dv_uid"],
            "ownership": "standalone_dv",
            "deleting": False,
            "no_consumers": True,
        },
    }
    provisioner = SimpleNamespace(
        attest_vm_cleanup_stop=AsyncMock(return_value=evidence)
    )
    await complete_vm_cleanup_permit(
        state["recovery"], permit, outcome="completed", provisioner=provisioner
    )
    assert await db.fetchval(
        "SELECT public.vm_job_cancel_retention_settled($1)", permit.admission_id
    )
    assert await complete_cancel_retention_marker(db, state["job_id"]) is True
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", UUID(state["job_id"]))
    )
    assert "_stateless_cancel_cleanup_pending" not in context
    assert "_vm_creation_pending" not in context
    assert context["vm"]["status"] == "deleted"
    replay = await acquire(state)
    assert replay.allowed and replay.completed_outcome == "completed"
    assert replay.admission_id == permit.admission_id
    assert await complete_cancel_retention_marker(db, state["job_id"]) is True
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_job_cancel_retention_authorities WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 1
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{_vm_creation_pending}',"
        "to_jsonb($2::text)) WHERE id=$1",
        UUID(state["job_id"]),
        str(uuid4()),
    )
    changed_replay = await acquire(state)
    assert not changed_replay.allowed
    await db.execute(
        "UPDATE jobs SET context=context-'_vm_creation_pending' WHERE id=$1",
        UUID(state["job_id"]),
    )

    class RollBackHold(Exception):
        pass

    try:
        async with db.acquire() as conn, conn.transaction():
            await conn.execute(
                "UPDATE jobs SET context=context||"
                '\'{"_worker_execution_hold":{"version":1}}\'::jsonb WHERE id=$1',
                UUID(state["job_id"]),
            )
            operation_id = uuid4()
            with pytest.raises(
                asyncpg.CheckViolationError, match="Retained Job Resume authority"
            ):
                await conn.execute(
                    "INSERT INTO vm_job_retained_resumes "
                    "(id,job_id,root_retention_admission_id,physical_cleanup_admission_id,"
                    "predecessor_request_id,predecessor_generation,pvc_uid,request_id,"
                    "provision_generation,explicit_resume_id,requested_by,source_revision,retained_vm) "
                    "SELECT $1,a.job_id,a.cleanup_admission_id,a.cleanup_admission_id,"
                    "a.creation_request_id,a.provision_generation,a.pvc_uid,$2,$3,$4,"
                    "j.user_id,r.revision,j.context->'vm' FROM vm_job_cancel_retention_authorities a "
                    "JOIN jobs j ON j.id=a.job_id JOIN vm_creation_retries r "
                    "ON r.request_id=a.creation_request_id WHERE a.cleanup_admission_id=$5",
                    operation_id,
                    uuid5(operation_id, "source"),
                    uuid5(operation_id, "provision"),
                    uuid4(),
                    permit.admission_id,
                )
            raise RollBackHold
    except RollBackHold:
        pass
    if deleted_before_completion:
        from orchestrator.services.vm_job_retained_disk_purge import (
            acquire_job_retained_disk_purge,
        )
        from orchestrator.services.vm_provisioner import VMTeardownIdentity

        wrong_identity = await acquire_job_retained_disk_purge(
            state["recovery"],
            job_id=state["job_id"],
            identity=VMTeardownIdentity(
                state["generation"], str(uuid4()), frozen["pvc_uid"]
            ),
        )
        assert not wrong_identity.allowed
        purge = await acquire_job_retained_disk_purge(
            state["recovery"],
            job_id=state["job_id"],
            identity=VMTeardownIdentity(
                state["generation"], frozen["vm_uid"], frozen["pvc_uid"]
            ),
        )
        assert purge.allowed, purge.reason
        assert purge.parent_cleanup["intent"]["purge_disk"] is True
        await complete_synthetic_retained_delete(db, state["recovery"], purge)
        return
    actor = await db.fetchval(
        "SELECT user_id FROM jobs WHERE id=$1", UUID(state["job_id"])
    )
    assert await db.prepare_stateless_job_for_workspace_resume(
        state["job_id"],
        "vm",
        expected_status="cancelled",
        owner_resume_user_id=actor,
    )
    operation = await db.fetchrow(
        "SELECT * FROM vm_job_retained_resumes WHERE job_id=$1",
        UUID(state["job_id"]),
    )
    assert operation["predecessor_request_id"] == await db.fetchval(
        "SELECT request_id FROM vm_creation_retries WHERE job_id=$1",
        UUID(state["job_id"]),
    )
    assert operation["physical_cleanup_admission_id"] == permit.admission_id
    assert (await db.cancel_stateless_job(state["job_id"]))[0]
    assert await db.complete_stateless_cancel_cleanup(state["job_id"])
    from orchestrator.services.vm_job_retained_disk_purge import (
        acquire_job_retained_disk_purge,
    )
    from orchestrator.services.vm_provisioner import VMTeardownIdentity

    tail = await db.fetchrow(
        "SELECT * FROM vm_job_retained_resume_terminals WHERE resume_id=$1",
        operation["id"],
    )
    assert tail["terminal_kind"] == "source_absent"
    assert await db.fetchval(
        "SELECT public.vm_cancel_late_adopted_retained_terminal($1)",
        permit.admission_id,
    )
    # Public Delete records this intent before acquiring the retained purge.
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{_stateless_delete_pending}',"
        "'true'::jsonb) WHERE id=$1",
        UUID(state["job_id"]),
    )
    purge = await acquire_job_retained_disk_purge(
        state["recovery"],
        job_id=state["job_id"],
        identity=VMTeardownIdentity(
            state["generation"], frozen["vm_uid"], frozen["pvc_uid"]
        ),
    )
    assert purge.allowed, purge.reason
    await complete_synthetic_retained_delete(db, state["recovery"], purge)


@pytest.mark.asyncio
async def test_fresh_deleted_late_adoption_requires_process_zero(db):
    state = await late_adopted_state(db)
    from tests.test_vm_job_cancel_retention_real_postgres import (
        publish_cancelled_launcher,
    )

    await publish_cancelled_launcher(state)
    with pytest.raises(asyncpg.CheckViolationError, match="process-zero authority"):
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',"
            "'\"deleted\"') WHERE id=$1",
            UUID(state["job_id"]),
        )
    charge = await db.fetchrow(
        "SELECT state,vm_uid,vmi_uid,launcher_uid FROM vm_resource_reservations WHERE id=$1",
        UUID(state["reservation_id"]),
    )
    assert dict(charge) == {
        "state": "reserved",
        "vm_uid": None,
        "vmi_uid": None,
        "launcher_uid": None,
    }
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_cancel_retention_authorities")
        == 0
    )


@pytest.mark.asyncio
async def test_open_late_adoption_missing_marker_cannot_continue_before_proof(db):
    state = await late_adopted_state(db)
    from tests.test_vm_job_cancel_retention_real_postgres import (
        publish_cancelled_launcher,
    )

    await publish_cancelled_launcher(state)
    permit = await acquire(state)
    assert permit.allowed
    await db.execute(
        "UPDATE jobs SET context=context-'_vm_creation_pending' WHERE id=$1",
        UUID(state["job_id"]),
    )
    with pytest.raises(asyncpg.CheckViolationError, match="current authority changed"):
        await acquire(state)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "issued",
        "request",
        "queue",
        "owner",
        "generation",
        "inventory",
        "inherited",
        "effect_vm",
        "reason",
    ],
)
async def test_cancel_before_late_adoption_refuses_inexact_scope(db, fault):
    state = await late_adopted_state(
        db, issued=fault == "issued", wrong_effect=fault == "effect_vm"
    )
    from tests.test_vm_job_cancel_retention_real_postgres import (
        publish_cancelled_launcher,
    )

    await publish_cancelled_launcher(
        state, invalid="owner" if fault == "inventory" else None
    )
    owner = UUID(state["job_id"])
    if fault == "request":
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{_vm_creation_pending}',to_jsonb($2::text)) WHERE id=$1",
            owner,
            str(uuid4()),
        )
    elif fault == "queue":
        await db.execute("UPDATE run_queue SET state='queued' WHERE unit_id=$1", owner)
    elif fault == "owner":
        await db.execute("UPDATE jobs SET execution_lane='pinned' WHERE id=$1", owner)
    elif fault == "generation":
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,identity_provision_generation}',"
            "to_jsonb($2::text)) WHERE id=$1",
            owner,
            str(uuid4()),
        )
    elif fault == "inherited":
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{inherits_parent_workspace}',"
            "'true'::jsonb) WHERE id=$1",
            owner,
        )
    elif fault == "reason":
        await db.execute(
            "UPDATE vm_creation_retries SET reason='creation_disposed' WHERE job_id=$1",
            owner,
        )
    before = await db.fetchrow(
        "SELECT state,vm_uid,vmi_uid,launcher_uid FROM vm_resource_reservations WHERE id=$1",
        UUID(state["reservation_id"]),
    )
    permit = await acquire(state)
    assert permit is None or not permit.allowed
    assert (
        await db.fetchrow(
            "SELECT state,vm_uid,vmi_uid,launcher_uid FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == before
    )
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_cancel_retention_authorities")
        == 0
    )


@pytest.mark.asyncio
async def test_exact_prior_zero_admits_immutable_intent_then_committed_proof(db):
    state = await held_state(db)
    store, parent, frozen, old_zero = (
        state["store"],
        state["parent"],
        state["frozen"],
        state["zero"],
    )
    intent = await store.admit_intent(
        state["job_id"],
        state["generation"],
        parent,
        frozen,
        retention_preflight=state["preflight"],
        prior_zero_receipt_id=str(old_zero["id"]),
    )
    assert intent["frozen"] == frozen
    assert (
        await db.fetchval(
            "SELECT prior_zero_receipt_id FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == old_zero["id"]
    )
    assert (
        await store.committed_proof(state["job_id"], state["generation"], parent)
        is None
    )
    observed = terminal_proof(frozen, intent["frozen_digest"])
    observed["kind"] = "vm_job_never_app_ready_retained_positive_stop_v1"
    proof = await store.commit_positive_proof(
        state["job_id"], state["generation"], parent, observed
    )
    assert proof["process_zero_receipt_id"] == str(old_zero["id"])
    assert await store.committed_proof(state["job_id"], state["generation"], parent)
    assert dict(
        await db.fetchrow(
            "SELECT * FROM managed_repository_process_zero_receipts WHERE id=$1",
            old_zero["id"],
        )
    ) == dict(old_zero)
    assert (
        await store.admit_intent(
            state["job_id"],
            state["generation"],
            parent,
            frozen,
            retention_preflight=state["preflight"],
            prior_zero_receipt_id=str(old_zero["id"]),
        )
        == intent
    )
    with pytest.raises((asyncpg.CheckViolationError, VMPreSSHStopConflict)):
        await store.admit_intent(
            state["job_id"],
            state["generation"],
            parent,
            frozen,
            retention_preflight=state["preflight"],
            prior_zero_receipt_id=str(uuid4()),
        )


@pytest.mark.asyncio
async def test_authority_is_native_committed_and_exact(db):
    state = await held_state(db)
    args = dict(
        job_id=state["job_id"],
        generation=state["generation"],
        vm_uid=state["frozen"]["vm_uid"],
        pvc_uid=state["frozen"]["pvc_uid"],
    )
    authority = await current_policy1_stop_authority(db, state["parent"], **args)
    assert authority["kind"] == "vm_job_cancel_retention_held_stop_authority_v1"
    assert authority["cleanup_admission_id"] == state["parent"]["admission_id"]
    assert (
        await current_policy1_stop_authority(db, state["parent"], **args) == authority
    )
    assert (
        await current_policy1_stop_authority(
            db, state["parent"], **{**args, "vm_uid": str(uuid4())}
        )
        is None
    )
    changed = {**state["parent"], "request_id": str(uuid4())}
    assert await current_policy1_stop_authority(db, changed, **args) is None
    await db.execute(
        "UPDATE run_queue SET state='queued' WHERE unit_id=$1", UUID(state["job_id"])
    )
    assert await current_policy1_stop_authority(db, state["parent"], **args) is None
    await db.execute(
        "UPDATE run_queue SET state='done' WHERE unit_id=$1", UUID(state["job_id"])
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("receipt", ["missing", "wrong"])
async def test_existing_zero_rejects_missing_or_wrong_link(db, receipt):
    state = await held_state(db)
    with pytest.raises(VMPreSSHStopConflict):
        await state["store"].admit_intent(
            state["job_id"],
            state["generation"],
            state["parent"],
            state["frozen"],
            retention_preflight=state["preflight"],
            prior_zero_receipt_id=None if receipt == "missing" else str(uuid4()),
        )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )


@pytest.mark.asyncio
async def test_old_kind_cannot_borrow_existing_zero(db):
    state = await held_state(db)
    old_frozen = {
        key: value
        for key, value in state["frozen"].items()
        if key
        not in {
            "cleanup_admission_id",
            "cleanup_request_id",
            "cleanup_intent_digest",
            "kube_vm_ready_at_inspection",
        }
    }
    old_frozen["kind"] = "vm_pre_ssh_stop_candidate_v1"
    with pytest.raises(VMPreSSHStopConflict):
        await state["store"].admit_intent(
            state["job_id"],
            state["generation"],
            state["parent"],
            old_frozen,
            prior_zero_receipt_id=str(state["zero"]["id"]),
        )


@pytest.mark.asyncio
async def test_no_proof_cannot_complete_retaining_parent(db):
    state = await held_state(db)
    await state["store"].admit_intent(
        state["job_id"],
        state["generation"],
        state["parent"],
        state["frozen"],
        retention_preflight=state["preflight"],
        prior_zero_receipt_id=str(state["zero"]["id"]),
    )
    assert (
        await state["store"].committed_proof(
            state["job_id"], state["generation"], state["parent"]
        )
        is None
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),"
            "outcome='completed' WHERE id=$1",
            UUID(state["parent"]["admission_id"]),
        )
    assert (
        await db.fetchval(
            "SELECT completed_at FROM vm_workspace_cleanup_admissions WHERE id=$1",
            UUID(state["parent"]["admission_id"]),
        )
        is None
    )


@pytest.mark.asyncio
async def test_positive_proof_requires_prior_intent(db):
    state = await held_state(db)
    proof = terminal_proof(state["frozen"], "sha256:" + "a" * 64)
    proof["kind"] = "vm_job_never_app_ready_retained_positive_stop_v1"
    with pytest.raises(VMPreSSHStopConflict, match="stop_intent_absent"):
        await state["store"].commit_positive_proof(
            state["job_id"], state["generation"], state["parent"], proof
        )


@pytest.mark.asyncio
async def test_fresh_held_stop_intent_does_not_require_prior_zero(db):
    state = await held_state(db, zero=False)
    intent = await state["store"].admit_intent(
        state["job_id"],
        state["generation"],
        state["parent"],
        state["frozen"],
        retention_preflight=state["preflight"],
    )
    assert intent["frozen"] == state["frozen"]
    assert (
        await db.fetchval(
            "SELECT prior_zero_receipt_id FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        is None
    )
    observed = terminal_proof(state["frozen"], intent["frozen_digest"])
    observed["kind"] = "vm_job_never_app_ready_retained_positive_stop_v1"
    proof = await state["store"].commit_positive_proof(
        state["job_id"], state["generation"], state["parent"], observed
    )
    assert proof["process_zero_receipt_id"]
    assert await state["store"].committed_proof(
        state["job_id"], state["generation"], state["parent"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["owner", "queue", "charge"])
async def test_concurrent_writer_invalidates_authority_after_lock(db, mutation):
    state = await held_state(db)
    args = dict(
        job_id=state["job_id"],
        generation=state["generation"],
        vm_uid=state["frozen"]["vm_uid"],
        pvc_uid=state["frozen"]["pvc_uid"],
    )
    reader = None
    try:
        async with db.acquire() as writer:
            async with writer.transaction():
                if mutation == "owner":
                    await writer.execute(
                        "UPDATE jobs SET execution_lane='pinned' WHERE id=$1",
                        UUID(state["job_id"]),
                    )
                elif mutation == "queue":
                    await writer.execute(
                        "UPDATE run_queue SET state='queued' WHERE unit_id=$1",
                        UUID(state["job_id"]),
                    )
                else:
                    await writer.execute(
                        "UPDATE vm_resource_reservations SET "
                        "observed_cpu_millicores=observed_cpu_millicores+1 WHERE id=$1",
                        UUID(state["reservation_id"]),
                    )
                reader = asyncio.create_task(
                    current_policy1_stop_authority(db, state["parent"], **args)
                )
                await asyncio.sleep(0.05)
                assert not reader.done()
        result = await asyncio.wait_for(reader, 5)
        assert (result is not None) if mutation == "charge" else (result is None)
    finally:
        if reader is not None and not reader.done():
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)


class SameConnectionDB:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


@pytest.mark.asyncio
async def test_no_intent_to_committed_intent_keeps_owner_before_job_lock(db, pg_dsn):  # noqa: F811
    state = await held_state(db)
    reader_conn = await asyncpg.connect(pg_dsn)
    reader = None
    try:
        async with db.acquire() as writer:
            async with writer.transaction():
                await writer.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                    f"workspace-recovery:job:{state['job_id']}",
                )
                reader = asyncio.create_task(
                    VMPreSSHStopStore(SameConnectionDB(reader_conn)).current_intent(
                        state["job_id"], state["generation"], state["parent"]
                    )
                )
                # Confirm that the independent reader is waiting on the owner
                # advisory lock before attempting the Job row lock.
                for _ in range(40):
                    waiting = await writer.fetchval(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE wait_event='advisory' AND datname=current_database()"
                    )
                    if waiting:
                        break
                    await asyncio.sleep(0.025)
                assert waiting, "reader did not wait on the owner advisory lock"
                await writer.fetchrow(
                    "SELECT id FROM jobs WHERE id=$1 FOR UPDATE NOWAIT",
                    UUID(state["job_id"]),
                )
                await VMPreSSHStopStore(SameConnectionDB(writer)).admit_intent(
                    state["job_id"],
                    state["generation"],
                    state["parent"],
                    state["frozen"],
                    retention_preflight=state["preflight"],
                    prior_zero_receipt_id=str(state["zero"]["id"]),
                )
        intent = await asyncio.wait_for(reader, 5)
        assert intent["frozen"] == state["frozen"]
    finally:
        if reader is not None and not reader.done():
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
        await reader_conn.close()


@pytest.mark.asyncio
async def test_savepoint_release_cannot_publish_uncommitted_held_intent(db):
    state = await held_state(db)
    with pytest.raises(RuntimeError, match="roll back outer"):
        async with db.acquire() as conn:
            same = SameConnectionDB(conn)
            store = VMPreSSHStopStore(same)
            async with conn.transaction():
                async with conn.transaction():
                    await store.admit_intent(
                        state["job_id"],
                        state["generation"],
                        state["parent"],
                        state["frozen"],
                        retention_preflight=state["preflight"],
                        prior_zero_receipt_id=str(state["zero"]["id"]),
                    )
                with pytest.raises(VMPreSSHStopConflict, match="uncommitted"):
                    await store.current_intent(
                        state["job_id"], state["generation"], state["parent"]
                    )
                observed = terminal_proof(
                    state["frozen"],
                    await conn.fetchval(
                        "SELECT frozen_digest FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
                        UUID(state["job_id"]),
                    ),
                )
                observed["kind"] = "vm_job_never_app_ready_retained_positive_stop_v1"
                with pytest.raises(VMPreSSHStopConflict, match="uncommitted"):
                    await store.commit_positive_proof(
                        state["job_id"], state["generation"], state["parent"], observed
                    )
                assert (
                    await current_policy1_stop_authority(
                        same,
                        state["parent"],
                        job_id=state["job_id"],
                        generation=state["generation"],
                        vm_uid=state["frozen"]["vm_uid"],
                        pvc_uid=state["frozen"]["pvc_uid"],
                    )
                    is None
                )
                raise RuntimeError("roll back outer")
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )


@pytest.mark.asyncio
async def test_native_intent_stamp_overwrites_forged_input(db):
    state = await held_state(db, zero=False)
    row = await db.fetchrow(
        "INSERT INTO vm_pre_ssh_stop_intents "
        "(cleanup_admission_id,job_id,provision_generation,creation_request_id,"
        "reservation_id,reservation_revision,vm_uid,vmi_uid,launcher_uid,pvc_uid,node_uid,"
        "cleanup_intent_digest,frozen,frozen_digest,retention_preflight,held_stop_xact_id) "
        "SELECT cleanup_admission_id,job_id,provision_generation,creation_request_id,"
        "reservation_id,reservation_revision,vm_uid,vmi_uid,launcher_uid,pvc_uid,node_uid,"
        "intent_digest,$2::jsonb,'sha256:'||encode(sha256(convert_to($2::jsonb::text,'UTF8')),'hex'),"
        "$3::jsonb,'0'::xid8 FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1 "
        "RETURNING held_stop_xact_id,frozen_digest",
        UUID(state["parent"]["admission_id"]),
        json.dumps(state["frozen"]),
        json.dumps(state["preflight"]),
    )
    assert row["held_stop_xact_id"] is not None
    assert str(row["held_stop_xact_id"]) != "0"
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_pre_ssh_stop_intents SET held_stop_xact_id='0'::xid8 "
            "WHERE cleanup_admission_id=$1",
            UUID(state["parent"]["admission_id"]),
        )
    observed = terminal_proof(state["frozen"], row["frozen_digest"])
    observed["kind"] = "vm_job_never_app_ready_retained_positive_stop_v1"
    proof_stamp = await db.fetchval(
        "INSERT INTO vm_pre_ssh_stop_proofs "
        "(cleanup_admission_id,job_id,provision_generation,frozen_digest,"
        "terminal_evidence,evidence_digest,held_stop_xact_id) "
        "VALUES($1,$2,$3,$4,$5::jsonb,"
        "'sha256:'||encode(sha256(convert_to($5::jsonb::text,'UTF8')),'hex'),'0'::xid8) "
        "RETURNING held_stop_xact_id",
        UUID(state["parent"]["admission_id"]),
        UUID(state["job_id"]),
        UUID(state["generation"]),
        row["frozen_digest"],
        json.dumps(observed),
    )
    assert proof_stamp is not None
    assert str(proof_stamp) != "0"
    assert proof_stamp != row["held_stop_xact_id"]
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_pre_ssh_stop_proofs SET held_stop_xact_id='0'::xid8 "
            "WHERE cleanup_admission_id=$1",
            UUID(state["parent"]["admission_id"]),
        )


@pytest.mark.asyncio
async def test_ready_application_retry_revokes_held_authority(db):
    state = await held_state(db)
    await db.execute(
        "UPDATE vm_creation_retries SET ready_at=clock_timestamp() WHERE job_id=$1",
        UUID(state["job_id"]),
    )
    assert (
        await current_policy1_stop_authority(
            db,
            state["parent"],
            job_id=state["job_id"],
            generation=state["generation"],
            vm_uid=state["frozen"]["vm_uid"],
            pvc_uid=state["frozen"]["pvc_uid"],
        )
        is None
    )
    with pytest.raises(VMPreSSHStopConflict):
        await state["store"].admit_intent(
            state["job_id"],
            state["generation"],
            state["parent"],
            state["frozen"],
            retention_preflight=state["preflight"],
            prior_zero_receipt_id=str(state["zero"]["id"]),
        )


@pytest.mark.asyncio
async def test_stale_generation_revision_and_successor_refuse_authority(db):
    state = await held_state(db)
    args = dict(
        job_id=state["job_id"],
        generation=state["generation"],
        vm_uid=state["frozen"]["vm_uid"],
        pvc_uid=state["frozen"]["pvc_uid"],
    )
    assert (
        await current_policy1_stop_authority(
            db, state["parent"], **{**args, "generation": str(uuid4())}
        )
        is None
    )

    async with db.acquire() as conn:
        old_revision = await conn.fetchval(
            "SELECT revision FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        # Model stale persisted data without making production mutators accept
        # an impossible revision or a successor with no recovery fixture.
        await conn.execute("SET session_replication_role=replica")
        try:
            await conn.execute(
                "UPDATE vm_resource_reservations SET revision=revision+1 WHERE id=$1",
                UUID(state["reservation_id"]),
            )
        finally:
            await conn.execute("SET session_replication_role=origin")
    assert await current_policy1_stop_authority(db, state["parent"], **args) is None
    async with db.acquire() as conn:
        await conn.execute("SET session_replication_role=replica")
        try:
            await conn.execute(
                "UPDATE vm_resource_reservations SET revision=$2 WHERE id=$1",
                UUID(state["reservation_id"]),
                old_revision,
            )
        finally:
            await conn.execute("SET session_replication_role=origin")

    successor_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute("SET session_replication_role=replica")
        try:
            await conn.execute(
                "INSERT INTO vm_resource_recovery_successors "
                "(recovery_id,reservation_id,ordinal,owner_id,provision_generation,"
                "vm_uid,root_pvc_uid,prior_vmi_uid,prior_launcher_uid,"
                "successor_vmi_uid,successor_launcher_uid,stop_receipt_digest,"
                "final_attestation_digest) "
                "VALUES($1,$2,1,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11)",
                successor_id,
                UUID(state["reservation_id"]),
                UUID(state["job_id"]),
                UUID(state["generation"]),
                UUID(state["frozen"]["vm_uid"]),
                UUID(state["frozen"]["pvc_uid"]),
                UUID(state["frozen"]["vmi_uid"]),
                UUID(state["frozen"]["launcher_uid"]),
                uuid4(),
                uuid4(),
                "sha256:" + "f" * 64,
            )
        finally:
            await conn.execute("SET session_replication_role=origin")
    try:
        assert await current_policy1_stop_authority(db, state["parent"], **args) is None
    finally:
        async with db.acquire() as conn:
            await conn.execute("SET session_replication_role=replica")
            try:
                await conn.execute(
                    "DELETE FROM vm_resource_recovery_successors WHERE recovery_id=$1",
                    successor_id,
                )
            finally:
                await conn.execute("SET session_replication_role=origin")
