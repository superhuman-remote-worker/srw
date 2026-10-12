"""Explicit same-PVC Job Resume keeps one immutable disk-custody chain."""

from pathlib import Path
import json
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from orchestrator.database.postgres import JobVMAuditNotReady
from orchestrator.services.vm_idle_access import VMIdleAccessStore
from tests.test_vm_job_cancel_retention_real_postgres import (
    _base_db,  # noqa: F401
    _db_fixture,  # noqa: F401
    _schema_applied,  # noqa: F401
    _pre_ssh_db,  # noqa: F401
    db as _retention_db,  # noqa: F401
    enabled,  # noqa: F401
    acquire,
    cancelled as cancelled_retention,
    pg_dsn,  # noqa: F401
    positive_retention,
    postgres_db_fixture,  # noqa: F401
    pre_ssh_schema,  # noqa: F401
    retention_schema,  # noqa: F401
    settle_sql,
    whole_schema,  # noqa: F401
)


@pytest_asyncio.fixture(scope="module")
async def resume_schema(pg_dsn, retention_schema):  # noqa: F811
    migration = Path(__file__).resolve().parents[1] / (
        "src/orchestrator/database/migrations/app/0339_vm_job_retained_resume.sql"
    )
    conn = await asyncpg.connect(pg_dsn)
    try:
        if migration.exists() and not await conn.fetchval(
            "SELECT to_regclass('public.vm_job_retained_resumes') IS NOT NULL"
        ):
            await conn.execute(migration.read_text())
        yield
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(resume_schema, _retention_db):  # noqa: F811
    yield _retention_db


async def kept(db):
    state = await positive_retention(db)
    await settle_sql(db, state)
    assert await db.complete_stateless_cancel_cleanup(state["job_id"])
    return state


async def accepted_resume(db):
    state = await kept(db)
    owner = UUID(state["job_id"])
    actor = await db.fetchval("SELECT user_id FROM jobs WHERE id=$1", owner)
    assert await db.prepare_stateless_job_for_workspace_resume(
        str(owner), "vm", expected_status="cancelled", owner_resume_user_id=actor
    )
    state["resume"] = await db.fetchrow(
        "SELECT * FROM vm_job_retained_resumes WHERE job_id=$1", owner
    )
    return state


async def begin_resume(db, state):
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
    from orchestrator.services.vm_provisioner import VMProvisioner

    request = json.loads(
        await db.fetchval(
            "SELECT canonical_request FROM vm_creation_retries WHERE request_id=$1",
            state["resume"]["predecessor_request_id"],
        )
    )
    fresh = VMProvisioner._fresh_provision_ctx()
    request["provision_generation"] = fresh["provision_generation"]
    return await VMCreationPreflightStore(db).begin(
        job_id=state["job_id"], request=request, fresh_context=fresh
    )


async def admit_resume(db, state):
    from orchestrator.services.vm_creation_request import capture_vm_creation_request
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore

    preflight = await begin_resume(db, state)
    prior = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1",
        state["resume"]["predecessor_request_id"],
    )
    async with db.acquire() as conn, conn.transaction():
        snapshot = await capture_vm_creation_request(
            db,
            job_id=state["job_id"],
            generation=preflight["request"]["provision_generation"],
            request=preflight["request"],
            initial_request=True,
            controller_configuration=json.loads(prior["controller_configuration"]),
            controller_configuration_digest=prior["controller_configuration_digest"],
            _conn=conn,
        )
        return await VMCreationRetryStore(db).admit_on_conn(
            conn,
            job_id=state["job_id"],
            expected_generation=preflight["request"]["provision_generation"],
            request_id=preflight["request_id"],
            proposal={
                "origin": "resume",
                "expected_status": "paused",
                "request_digest": snapshot["request_digest"],
                "controller_configuration_digest": snapshot[
                    "controller_configuration_digest"
                ],
                **{
                    key: preflight[key]
                    for key in (
                        "expected_pvc_uid",
                        "predecessor_evidence",
                        "predecessor_cleanup_admission_id",
                    )
                },
            },
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("gate_enabled", [False, True])
async def test_generic_resume_cannot_shed_a_protected_job(
    db, monkeypatch, gate_enabled
):
    state = await kept(db)
    owner = UUID(state["job_id"])
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", str(gate_enabled).lower())
    before = await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
    assert not await db.prepare_stateless_job_for_workspace_resume(
        str(owner), "vm", expected_status="cancelled"
    )
    assert await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner) == before


@pytest.mark.asyncio
async def test_owner_resume_records_one_same_disk_successor_before_preflight(db):
    state = await kept(db)
    owner = UUID(state["job_id"])
    actor = await db.fetchval("SELECT user_id FROM jobs WHERE id=$1", owner)
    assert await db.prepare_stateless_job_for_workspace_resume(
        str(owner), "vm", expected_status="cancelled", owner_resume_user_id=actor
    )
    operation = await db.fetchrow(
        "SELECT * FROM vm_job_retained_resumes WHERE job_id=$1", owner
    )
    assert operation is not None
    assert operation["root_retention_admission_id"] == state["retention"].admission_id
    assert operation["physical_cleanup_admission_id"] == state["retention"].admission_id
    assert operation["pvc_uid"] == UUID(state["frozen"]["pvc_uid"])
    assert operation["provision_generation"] != UUID(state["generation"])
    assert operation["request_id"] != operation["predecessor_request_id"]
    assert operation["requested_by"] == actor
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
    )
    assert context["_vm_job_retained_resume"] == str(operation["id"])
    assert "vm" not in context
    assert (
        await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner)
        != "runnable"
    )
    assert not await db.prepare_stateless_job_for_workspace_resume(
        str(owner), "vm", expected_status="cancelled", owner_resume_user_id=actor
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_job_retained_resumes WHERE job_id=$1", owner
        )
        == 1
    )


@pytest.mark.asyncio
async def test_disabled_owner_resume_keeps_protected_job_unchanged(db, monkeypatch):
    state = await kept(db)
    owner = UUID(state["job_id"])
    actor = await db.fetchval("SELECT user_id FROM jobs WHERE id=$1", owner)
    before = await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", "false")
    assert not await db.prepare_stateless_job_for_workspace_resume(
        str(owner), "vm", expected_status="cancelled", owner_resume_user_id=actor
    )
    assert await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner) == before


@pytest.mark.asyncio
async def test_preflight_uses_explicit_resume_ids_and_exact_physical_predecessor(db):
    state = await accepted_resume(db)
    result = await begin_resume(db, state)
    operation = state["resume"]
    assert result["request_id"] == str(operation["request_id"])
    assert result["request"]["provision_generation"] == str(
        operation["provision_generation"]
    )
    assert result["expected_pvc_uid"] == str(operation["pvc_uid"])
    assert result["predecessor_cleanup_admission_id"] == str(
        state["retention"].admission_id
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("with_preflight", [False, True])
async def test_cancel_before_source_closes_resume_without_fabricating_a_retry(
    db, with_preflight
):
    state = await accepted_resume(db)
    if with_preflight:
        await begin_resume(db, state)
    cancelled, _ = await db.cancel_stateless_job(state["job_id"])
    assert cancelled
    assert await db.complete_stateless_cancel_cleanup(state["job_id"])
    terminal = await db.fetchrow(
        "SELECT * FROM vm_job_retained_resume_terminals WHERE resume_id=$1",
        state["resume"]["id"],
    )
    assert terminal["terminal_kind"] == "source_absent"
    assert terminal["source_request_id"] is None
    assert terminal["physical_cleanup_admission_id"] == state["retention"].admission_id
    assert not await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE request_id=$1)",
        state["resume"]["request_id"],
    )
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict

    with pytest.raises(VMCreationRetryConflict):
        await begin_resume(db, state)


@pytest.mark.asyncio
async def test_next_resume_after_source_absent_uses_logical_tail_and_original_disk(db):
    state = await accepted_resume(db)
    cancelled, _ = await db.cancel_stateless_job(state["job_id"])
    assert cancelled
    assert await db.complete_stateless_cancel_cleanup(state["job_id"])
    assert await db.prepare_stateless_job_for_workspace_resume(
        state["job_id"],
        "vm",
        expected_status="cancelled",
        owner_resume_user_id=state["resume"]["requested_by"],
    )
    successor = await db.fetchrow(
        "SELECT * FROM vm_job_retained_resumes WHERE job_id=$1 AND id<>$2",
        UUID(state["job_id"]),
        state["resume"]["id"],
    )
    assert successor["predecessor_terminal_id"] is not None
    assert successor["physical_cleanup_admission_id"] == state["retention"].admission_id
    state["resume"] = successor
    preflight = await begin_resume(db, state)
    assert preflight["request_id"] == str(successor["request_id"])
    assert preflight["expected_pvc_uid"] == state["frozen"]["pvc_uid"]


@pytest.mark.asyncio
async def test_wrong_owner_cannot_admit_retained_resume(db):
    state = await kept(db)
    assert not await db.prepare_stateless_job_for_workspace_resume(
        state["job_id"], "vm", expected_status="cancelled", owner_resume_user_id=uuid4()
    )


@pytest.mark.asyncio
async def test_admitted_resume_binds_native_source_and_allows_exact_creation_only(db):
    from uuid import NAMESPACE_URL, uuid5
    from orchestrator.services.vm_creation_retry_store import (
        VMCreationRetryStore,
        _creation_intent,
    )
    from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest

    state = await accepted_resume(db)
    retry = await admit_resume(db, state)
    assert retry["origin"] == "resume"
    assert retry["job_retained_resume_id"] == state["resume"]["id"]
    retry = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    request = uuid5(NAMESPACE_URL, "vm-create:" + str(retry["request_id"]))
    digest = cleanup_intent_digest(_creation_intent(retry))
    allowed = await db.fetchval(
        "SELECT public.vm_job_cancel_retention_cleanup_allowed('job',$1,$2,'controller_vm_create',$3,$4,NULL,false)",
        UUID(state["job_id"]),
        retry["expected_pvc_uid"],
        request,
        digest,
    )
    assert allowed
    for source, candidate_request, candidate_digest in (
        ("controller_vm_create", uuid4(), digest),
        ("controller_vm_create", request, "sha256:" + "f" * 64),
        ("controller_rootdisk_delete", request, digest),
        ("controller_rootdisk_gc", request, digest),
    ):
        assert not await db.fetchval(
            "SELECT public.vm_job_cancel_retention_cleanup_allowed('job',$1,$2,$3,$4,$5,NULL,false)",
            UUID(state["job_id"]),
            retry["expected_pvc_uid"],
            source,
            candidate_request,
            candidate_digest,
        )


@pytest.mark.asyncio
async def test_direct_source_insert_after_absent_terminal_is_refused(db):
    state = await accepted_resume(db)
    cancelled, _ = await db.cancel_stateless_job(state["job_id"])
    assert cancelled and await db.complete_stateless_cancel_cleanup(state["job_id"])
    with pytest.raises(asyncpg.CheckViolationError, match="live explicit Resume"):
        await db.execute(
            "INSERT INTO vm_creation_retries(request_id,job_id,provision_generation,origin,request_digest,"
            "canonical_request,controller_configuration_digest,controller_configuration,execution_id,"
            "execution_revision,execution_generation,expected_pvc_uid,predecessor_cleanup_admission_id,job_retained_resume_id) "
            "SELECT $1,job_id,$2,'resume',request_digest,canonical_request,controller_configuration_digest,"
            "controller_configuration,execution_id,execution_revision,execution_generation,$3,$4,$5 "
            "FROM vm_creation_retries WHERE request_id=$6",
            state["resume"]["request_id"],
            state["resume"]["provision_generation"],
            state["resume"]["pvc_uid"],
            state["retention"].admission_id,
            state["resume"]["id"],
            state["resume"]["predecessor_request_id"],
        )


@pytest.mark.asyncio
async def test_repeated_cancel_replays_absent_terminal_and_clears_its_marker(db):
    state = await accepted_resume(db)
    for _ in range(2):
        cancelled, _ = await db.cancel_stateless_job(state["job_id"])
        assert cancelled
        assert await db.complete_stateless_cancel_cleanup(state["job_id"])
        assert not await db.stateless_cancel_cleanup_pending(state["job_id"])
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_job_retained_resume_terminals WHERE resume_id=$1",
            state["resume"]["id"],
        )
        == 1
    )


@pytest.mark.asyncio
async def test_absent_terminal_replay_cannot_clear_another_generation(db):
    state = await accepted_resume(db)
    cancelled, _ = await db.cancel_stateless_job(state["job_id"])
    assert cancelled and await db.complete_stateless_cancel_cleanup(state["job_id"])
    await db.execute(
        "UPDATE jobs SET context=context||$2::jsonb WHERE id=$1",
        UUID(state["job_id"]),
        json.dumps(
            {
                "_stateless_cancel_cleanup_pending": True,
                "vm": {
                    "provision_generation": str(uuid4()),
                    "identity_authenticated": False,
                },
            }
        ),
    )
    assert not await db.complete_stateless_cancel_cleanup(state["job_id"])
    assert await db.stateless_cancel_cleanup_pending(state["job_id"])


@pytest.mark.asyncio
async def test_public_cancel_archive_handles_typed_source_absent_before_q1(db):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from orchestrator.services.thread_retirement import archive_and_cleanup_workspace
    from orchestrator.services.vm_workspace_policy import vm_needs_release

    state = await accepted_resume(db)
    await begin_resume(db, state)
    cancelled, _ = await db.cancel_stateless_job(state["job_id"])
    assert cancelled
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", UUID(state["job_id"]))
    )
    provisioner = SimpleNamespace(
        lifecycle_available=True, capture_vm_teardown_identity=AsyncMock()
    )
    actions = await archive_and_cleanup_workspace(
        state["job_id"],
        dependencies=SimpleNamespace(
            store=db,
            recovery_store=state["recovery"],
            vm_provisioner=provisioner,
            container_provisioner=None,
            docker_provisioner=None,
            get_container_context=lambda _: {},
            get_vm_context=lambda _: context.get("vm", {}),
            vm_needs_release=vm_needs_release,
        ),
    )
    assert "retained vm Resume source absent" in actions
    provisioner.capture_vm_teardown_identity.assert_not_awaited()
    assert await db.stateless_cancel_cleanup_pending(state["job_id"])
    assert await db.complete_stateless_cancel_cleanup(state["job_id"])
    assert not await db.stateless_cancel_cleanup_pending(state["job_id"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status", ["created", "ssh_pending", "ssh_unreachable", "retiring_process_zero"]
)
@pytest.mark.parametrize("old_parent", [False, True])
async def test_never_ready_ssh_projection_admits_only_the_same_retention_scope(
    db, status, old_parent
):
    state = await cancelled_retention(
        db, old=old_parent, retiring=status == "retiring_process_zero"
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',to_jsonb($2::text)) WHERE id=$1",
        UUID(state["job_id"]),
        status,
    )
    permit = await acquire(state)
    assert permit is not None and permit.allowed
    assert permit.parent_cleanup["intent"]["purge_disk"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["ssh_pending", "ssh_unreachable"])
@pytest.mark.parametrize("fault", ["ready_at", "identity_missing", "identity_mismatch"])
async def test_ssh_projection_does_not_relax_never_ready_identity(db, status, fault):
    state = await cancelled_retention(db, retiring=False)
    owner = UUID(state["job_id"])
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
    )
    context["vm"]["status"] = status
    if fault == "ready_at":
        await db.execute(
            "UPDATE vm_creation_retries SET ready_at=clock_timestamp() WHERE job_id=$1",
            owner,
        )
    elif fault == "identity_missing":
        context["vm"].pop("identity_provision_generation")
    else:
        context["vm"]["vm_uid"] = str(uuid4())
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1", owner, json.dumps(context)
    )
    permit = await acquire(state)
    assert permit is not None and not permit.allowed


@pytest.mark.asyncio
async def test_historical_keep_does_not_authorize_prune_during_resume(db):
    from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest

    state = await accepted_resume(db)
    owner = UUID(state["job_id"])
    digest = cleanup_intent_digest(
        {
            "mode": "delete_thread",
            "resource": "checkpoint_thread",
            "thread_id": str(owner),
        }
    )
    assert not await db.fetchval(
        "SELECT public.vm_job_cancel_retention_cleanup_allowed('job',$1,NULL,'terminal_checkpoint_prune',$2,$3,NULL,false)",
        owner,
        uuid4(),
        digest,
    )


@pytest.mark.asyncio
async def test_typed_cancel_defers_strict_prune_until_its_terminal_proof(
    db, monkeypatch
):
    from unittest.mock import AsyncMock
    from orchestrator.services.vm_job_retained_resume import (
        complete_source_absent_cancel,
    )

    state = await accepted_resume(db)
    cancelled, _ = await db.cancel_stateless_job(state["job_id"])
    assert cancelled
    prune = AsyncMock()
    monkeypatch.setattr(db, "delete_checkpoint_thread", prune)
    assert await db.finalize_cancelled_stateless_job(state["job_id"])
    prune.assert_not_awaited()
    assert await complete_source_absent_cancel(db, state["job_id"], clear_pending=False)
    assert await db.stateless_cancel_cleanup_pending(state["job_id"])
    assert await db.complete_stateless_cancel_cleanup(state["job_id"])
    prune.assert_awaited_once_with(state["job_id"], strict=True)


@pytest.mark.asyncio
async def test_failed_deferred_prune_does_not_clear_cancel_hold(db, monkeypatch):
    from unittest.mock import AsyncMock

    state = await accepted_resume(db)
    cancelled, _ = await db.cancel_stateless_job(state["job_id"])
    assert cancelled
    monkeypatch.setattr(
        db,
        "delete_checkpoint_thread",
        AsyncMock(side_effect=RuntimeError("prune held")),
    )
    with pytest.raises(RuntimeError, match="prune held"):
        await db.complete_stateless_cancel_cleanup(state["job_id"])
    assert await db.stateless_cancel_cleanup_pending(state["job_id"])


async def claimed_resume(db):
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
    from orchestrator.services.vm_resource_job_runtime import (
        installed_job_resource_store,
    )

    state = await accepted_resume(db)
    retry = await admit_resume(db, state)
    async with db.acquire() as conn:
        resource = await installed_job_resource_store(
            conn, db, retry["controller_configuration"]
        )
    from tests.test_vm_resource_inventory_real_postgres import publish, successor

    sample = successor(
        json.loads(
            await db.fetchval(
                "SELECT s.document FROM vm_resource_inventory_snapshots s JOIN vm_resource_inventory_heads h "
                "ON h.current_snapshot_id=s.snapshot_id WHERE h.cluster_id=$1",
                resource.inventory.cluster_id,
            )
        )
    )
    pvc_uid, pv_uid = str(retry["expected_pvc_uid"]), str(uuid4())
    sample["pvcs"] = [
        {
            "uid": pvc_uid,
            "name": f"agent-vm-{state['job_id']}-rootdisk",
            "pv_uid": pv_uid,
            "pv_name": "retained-job",
            "phase": "Bound",
            "storage_class_uid": sample["storage_classes"][0]["uid"],
        }
    ]
    sample["pvs"] = [
        {
            "uid": pv_uid,
            "name": "retained-job",
            "claim_uid": pvc_uid,
            "required_affinity": None,
        }
    ]
    await publish(resource.inventory, sample)
    admitted = await resource.admit(request_id=str(retry["request_id"]))
    assert admitted["action"] == "admitted", admitted
    state["resource"] = resource
    state["charge"] = admitted
    state["retry"] = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    return state


async def authorize_resume(db, state):
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore

    retry = state["retry"]
    permit = await VMCreationRetryStore(db).authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(retry["claim_token"]),
        observed={
            key: str(retry[key]) if retry[key] is not None else None
            for key in (
                "job_id",
                "provision_generation",
                "request_digest",
                "controller_configuration_digest",
                "expected_pvc_uid",
            )
        },
    )
    assert permit["allowed"], permit
    return permit


@pytest.mark.asyncio
async def test_creation_parent_cannot_commit_without_atomic_source_link(db):
    from uuid import NAMESPACE_URL, uuid5
    from orchestrator.services.vm_creation_retry_store import _creation_intent
    from orchestrator.services.vm_workspace_recovery_store import (
        VMWorkspaceRecoveryStore,
        cleanup_intent_digest,
    )

    state = await claimed_resume(db)
    retry = state["retry"]
    with pytest.raises(asyncpg.CheckViolationError, match="source link"):
        async with db.acquire() as conn, conn.transaction():
            permit = await VMWorkspaceRecoveryStore(db).acquire_cleanup_permit_on_conn(
                conn,
                owner_kind="job",
                owner_id=retry["job_id"],
                pvc_uid=retry["expected_pvc_uid"],
                request_id=uuid5(
                    NAMESPACE_URL, "vm-create:" + str(retry["request_id"])
                ),
                source="controller_vm_create",
                intent_digest=cleanup_intent_digest(_creation_intent(retry)),
            )
            assert permit.allowed


@pytest.mark.asyncio
@pytest.mark.parametrize("release_savepoint", [False, True])
async def test_creation_permit_reader_refuses_uncommitted_parent_even_after_savepoint(
    db, release_savepoint
):
    from contextlib import asynccontextmanager
    from orchestrator.services.vm_creation_retry_store import (
        VMCreationRetryStore,
        VMCreationRetryConflict,
    )

    state = await claimed_resume(db)
    async with db.acquire() as conn:

        class BorrowedConnection:
            def __getattr__(self, name):
                return getattr(db, name)

            @asynccontextmanager
            async def acquire(self):
                yield conn

        outer = conn.transaction()
        await outer.start()
        savepoint = conn.transaction()
        await savepoint.start()
        try:
            await authorize_resume(BorrowedConnection(), state)
            if release_savepoint:
                await savepoint.commit()
            retry = await conn.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                state["retry"]["request_id"],
            )
            with pytest.raises(VMCreationRetryConflict, match="committed"):
                await VMCreationRetryStore(db)._creation_permit_on_conn(conn, retry)
        finally:
            await outer.rollback()


@pytest.mark.asyncio
async def test_committed_creation_parent_reader_accepts_and_cancel_fences_native_effect(
    db,
):
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore

    state = await claimed_resume(db)
    permit = await authorize_resume(db, state)
    retry_id = state["retry"]["request_id"]
    async with db.acquire() as conn:
        retry = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1", retry_id
        )
        assert (await VMCreationRetryStore(db)._creation_permit_on_conn(conn, retry))[
            "id"
        ] == permit["admission_id"]
    # Model the owner CAS winning immediately before native effect issuance,
    # independently of a later retry reconciler observing cancellation.
    await db.execute(
        "UPDATE jobs SET status='cancelled',context=context||'{\"_stateless_cancel_cleanup_pending\":true}'::jsonb WHERE id=$1",
        retry["job_id"],
    )
    with pytest.raises(asyncpg.CheckViolationError, match="committed live authority"):
        await db.execute(
            "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,effect_kind,carrier_uid,carrier_namespace,carrier_intent) "
            "VALUES($1,$2,1,'rootdisk',$3,'srw','{}'::jsonb)",
            uuid4(),
            retry_id,
            uuid4(),
        )


async def adopted_resume(db, *, ready=True):
    state = await claimed_resume(db)
    permit = await authorize_resume(db, state)
    retry = state["retry"]
    vm_uid, vmi_uid, launcher_uid = (uuid4() for _ in range(3))
    await db.execute(
        "UPDATE vm_creation_retries SET state='succeeded',reason='creation_adopted',boot_counted=true,"
        "revision=revision+1,observed_vm_uid=$2,observed_pvc_uid=expected_pvc_uid,"
        "ready_at=CASE WHEN $3 THEN clock_timestamp() END,resolved_at=clock_timestamp(),"
        "claim_token=NULL,claim_expires_at=NULL WHERE request_id=$1",
        retry["request_id"],
        vm_uid,
        ready,
    )
    await db.execute(
        "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),outcome='adopted' WHERE id=$1",
        permit["admission_id"],
    )
    await db.execute(
        "UPDATE vm_resource_reservations SET state='active',vm_uid=$2,vmi_uid=$3,launcher_uid=$4 WHERE request_id=$1",
        retry["request_id"],
        vm_uid,
        vmi_uid,
        launcher_uid,
    )
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", retry["job_id"])
    )
    context["vm"].update(
        status="ready" if ready else "ssh_pending",
        vm_uid=str(vm_uid),
        vmi_uid=str(vmi_uid),
        active_pod_uid=str(launcher_uid),
        rootdisk_pvc_uid=str(retry["expected_pvc_uid"]),
        identity_authenticated=True,
        identity_provision_generation=str(retry["provision_generation"]),
        creation_request_id=str(retry["request_id"]),
        workspace_storage=None,
    )
    context.pop("_vm_creation_pending", None)
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
        retry["job_id"],
        json.dumps(context),
    )
    assert (await db.cancel_stateless_job(state["job_id"]))[0]
    from orchestrator.services.vm_provisioner import VMTeardownIdentity

    state["identity"] = VMTeardownIdentity(
        str(retry["provision_generation"]), str(vm_uid), str(retry["expected_pvc_uid"])
    )
    return state


@pytest.mark.asyncio
async def test_ready_continuation_without_proof_refuses_before_release(db):
    from orchestrator.services import vm_job_retained_resume
    from shared.vm_resource_admission import ResourceAdmissionError

    state = await adopted_resume(db)
    with pytest.raises(ResourceAdmissionError, match="retention"):
        await vm_job_retained_resume.read_current_ready_preflight(
            db,
            None,
            job_id=state["job_id"],
            generation=str(state["resume"]["provision_generation"]),
        )


@pytest.mark.asyncio
async def test_legacy_generation_has_no_ready_continuation_proof_requirement(db):
    from orchestrator.services import vm_job_retained_resume

    state = await kept(db)
    assert (
        await vm_job_retained_resume.read_current_ready_preflight(
            db,
            None,
            job_id=state["job_id"],
            generation=state["generation"],
        )
        is None
    )


def ready_witness(candidate):
    return {
        "version": 1,
        "kind": "vm_job_retained_ready_preflight_v1",
        "stop_policy": "retained_ready_continuation_v1",
        "frozen": candidate,
        "namespace": candidate["namespace"],
        "owner_id": candidate["job_id"],
        "pvc_name": f"agent-vm-{candidate['job_id']}-rootdisk",
        "pvc_uid": candidate["pvc_uid"],
        "dv_uid": str(uuid4()),
        "ownership": "standalone_dv",
        "deleting": False,
        "consumer_scope": "exact_frozen_runtime_only",
    }


@pytest.mark.asyncio
async def test_ready_continuation_stores_exact_proof_and_rechecks_before_effect(db):
    from orchestrator.services import vm_job_retained_resume
    from orchestrator.services.vm_job_cancel_retention import acquire_cancel_retention
    from orchestrator.services.vm_workspace_recovery_store import (
        VMWorkspaceRecoveryStore,
    )
    from shared.vm_resource_admission import ResourceAdmissionError

    state = await adopted_resume(db)
    candidate = await vm_job_retained_resume.retained_ready_candidate(
        db,
        job_id=state["job_id"],
        identity=state["identity"],
    )
    proof = ready_witness(candidate)
    permit = await acquire_cancel_retention(
        VMWorkspaceRecoveryStore(db),
        job_id=state["job_id"],
        identity=state["identity"],
        retention_preflight=proof,
    )
    assert permit.allowed, permit.reason
    assert permit.parent_cleanup["intent"]["purge_disk"] is False
    assert permit.parent_cleanup["retention_preflight"] == proof
    assert (
        await vm_job_retained_resume.read_current_ready_preflight(
            db,
            permit.parent_cleanup,
            job_id=state["job_id"],
            generation=state["identity"].provision_generation,
        )
        == proof
    )
    for altered in (
        None,
        {**permit.parent_cleanup, "retention_preflight": None},
        {
            **permit.parent_cleanup,
            "intent": {**permit.parent_cleanup["intent"], "purge_disk": True},
        },
    ):
        with pytest.raises(ResourceAdmissionError):
            await vm_job_retained_resume.read_current_ready_preflight(
                db,
                altered,
                job_id=state["job_id"],
                generation=state["identity"].provision_generation,
            )
    with pytest.raises(ResourceAdmissionError):
        await vm_job_retained_resume.read_current_ready_preflight(
            db,
            permit.parent_cleanup,
            job_id=state["job_id"],
            generation=state["generation"],
        )
    authority = await db.fetchrow(
        "SELECT * FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1",
        permit.admission_id,
    )
    assert (
        authority["policy_version"] == 2
        and authority["job_retained_resume_id"] == state["resume"]["id"]
    )
    from orchestrator.services.vm_job_cancel_retention import (
        current_policy1_retention_parent,
    )

    assert (
        await current_policy1_retention_parent(
            db,
            permit.parent_cleanup,
            job_id=state["job_id"],
            generation=state["identity"].provision_generation,
            vm_uid=state["identity"].vm_uid,
            pvc_uid=state["identity"].rootdisk_pvc_uid,
        )
        == "other"
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_job_cancel_retention_authorities SET ready_retention_preflight=NULL WHERE cleanup_admission_id=$1",
            permit.admission_id,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("ready", [False, True])
async def test_continuation_compute_keep_never_reactivates_ancestor_purge(
    db, ready, monkeypatch
):
    from orchestrator.services.vm_job_cancel_retention import acquire_cancel_retention
    from orchestrator.services.vm_workspace_recovery_store import (
        VMWorkspaceRecoveryStore,
    )
    from orchestrator.services import vm_job_retained_resume

    state = await adopted_resume(db, ready=ready)
    proof = None
    if ready:
        proof = ready_witness(
            await vm_job_retained_resume.retained_ready_candidate(
                db, job_id=state["job_id"], identity=state["identity"]
            )
        )
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", "false")
    # Existing typed custody must still settle even when admission is disabled.
    permit = await acquire_cancel_retention(
        VMWorkspaceRecoveryStore(db),
        job_id=state["job_id"],
        identity=state["identity"],
        retention_preflight=proof,
    )
    assert permit.allowed, permit.reason
    assert permit.parent_cleanup["intent"]["purge_disk"] is False
    assert not await db.fetchval(
        "SELECT public.vm_job_cancel_retention_discharged($1)",
        state["retention"].admission_id,
    )
    assert not await db.fetchval(
        "SELECT public.vm_job_cancel_retention_cleanup_allowed('job',$1,$2,'controller_rootdisk_delete',$3,$4,NULL,false)",
        UUID(state["job_id"]),
        state["resume"]["pvc_uid"],
        uuid4(),
        "sha256:" + "a" * 64,
    )


async def ready_keep(db):
    from orchestrator.services.vm_job_retained_resume import retained_ready_candidate
    from orchestrator.services.vm_job_cancel_retention import acquire_cancel_retention
    from orchestrator.services.vm_workspace_recovery_store import (
        VMWorkspaceRecoveryStore,
    )

    state = await adopted_resume(db)
    proof = ready_witness(
        await retained_ready_candidate(
            db, job_id=state["job_id"], identity=state["identity"]
        )
    )
    state["ready_proof"] = proof
    state["recovery"] = VMWorkspaceRecoveryStore(db)
    state["keep"] = await acquire_cancel_retention(
        state["recovery"],
        job_id=state["job_id"],
        identity=state["identity"],
        retention_preflight=proof,
    )
    assert state["keep"].allowed
    return state


async def settled_ready_keep(db):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from shared.vm_cancel_retention import retained_rootdisk_from_preflight
    from orchestrator.services.vm_workspace_recovery_store import (
        prepare_vm_cleanup_resource,
        complete_vm_cleanup_permit,
    )

    state = await ready_keep(db)
    candidate = await prepare_vm_cleanup_resource(state["recovery"], state["keep"])
    assert candidate["retention_preflight"] == state["ready_proof"]
    # This PG test supplies the existing SSH process-zero receipt; transport
    # tests separately prove that Ready retirement runs the pinned SSH path.
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"retiring_process_zero\"') WHERE id=$1",
        UUID(state["job_id"]),
    )
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts(owner_kind,owner_id,scope,provisioner,runtime_incarnation) VALUES('job',$1,'vm','vm',$2)",
        UUID(state["job_id"]),
        state["identity"].provision_generation,
    )
    evidence = {
        "version": 1,
        "kind": "vm_cleanup_physical_stop",
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
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "pvc_disposition": "retained",
        "controller_authenticated": True,
        "retained_rootdisk": retained_rootdisk_from_preflight(state["ready_proof"]),
    }
    provisioner = SimpleNamespace(
        attest_vm_cleanup_stop=AsyncMock(return_value=evidence)
    )
    await complete_vm_cleanup_permit(
        state["recovery"], state["keep"], outcome="completed", provisioner=provisioner
    )
    assert await db.fetchval(
        "SELECT public.vm_job_cancel_retention_settled($1)", state["keep"].admission_id
    )
    assert await db.complete_stateless_cancel_cleanup(state["job_id"])
    terminal = await db.fetchrow(
        "SELECT * FROM vm_job_retained_resume_terminals WHERE resume_id=$1",
        state["resume"]["id"],
    )
    assert terminal["terminal_kind"] == "kept_compute"
    assert terminal["physical_cleanup_admission_id"] == state["keep"].admission_id
    return state


@pytest.mark.asyncio
async def test_ready_keep_releases_exact_compute_then_next_resume_uses_new_physical_keep(
    db,
):
    state = await settled_ready_keep(db)
    terminal = await db.fetchrow(
        "SELECT * FROM vm_job_retained_resume_terminals WHERE resume_id=$1",
        state["resume"]["id"],
    )
    assert await db.prepare_stateless_job_for_workspace_resume(
        state["job_id"],
        "vm",
        expected_status="cancelled",
        owner_resume_user_id=state["resume"]["requested_by"],
    )
    state["resume"] = await db.fetchrow(
        "SELECT * FROM vm_job_retained_resumes WHERE predecessor_terminal_id=$1",
        terminal["id"],
    )
    assert (
        state["resume"]["physical_cleanup_admission_id"] == state["keep"].admission_id
    )
    assert (await begin_resume(db, state))["predecessor_cleanup_admission_id"] == str(
        state["keep"].admission_id
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("with_parent", [False, True])
async def test_admitted_never_issued_cancel_records_honest_tail_and_next_resume(
    db, with_parent
):
    from orchestrator.services.vm_job_retained_resume import settle_retained_no_compute

    if with_parent:
        state = await claimed_resume(db)
        await authorize_resume(db, state)
    else:
        state = await accepted_resume(db)
        state["retry"] = await admit_resume(db, state)
    assert (await db.cancel_stateless_job(state["job_id"]))[0]
    assert await settle_retained_no_compute(db, state["job_id"]) is True
    assert await db.complete_stateless_cancel_cleanup(state["job_id"])
    terminal = await db.fetchrow(
        "SELECT * FROM vm_job_retained_resume_terminals WHERE resume_id=$1",
        state["resume"]["id"],
    )
    assert terminal["terminal_kind"] == "never_issued"
    assert terminal["source_request_id"] == state["resume"]["request_id"]
    assert terminal["physical_cleanup_admission_id"] == state["retention"].admission_id
    retry = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1",
        state["resume"]["request_id"],
    )
    assert retry["state"] == "settled" and retry["reason"] == "creation_never_issued"
    assert retry["observed_vm_uid"] is None
    assert not await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts WHERE owner_id=$1 AND runtime_incarnation=$2)",
        UUID(state["job_id"]),
        str(state["resume"]["provision_generation"]),
    )
    assert await db.prepare_stateless_job_for_workspace_resume(
        state["job_id"],
        "vm",
        expected_status="cancelled",
        owner_resume_user_id=state["resume"]["requested_by"],
    )
    successor = await db.fetchrow(
        "SELECT * FROM vm_job_retained_resumes WHERE predecessor_terminal_id=$1",
        terminal["id"],
    )
    assert successor["physical_cleanup_admission_id"] == state["retention"].admission_id
    state["resume"] = successor
    assert (await begin_resume(db, state))["expected_pvc_uid"] == state["frozen"][
        "pvc_uid"
    ]


@pytest.mark.asyncio
async def test_native_effect_waits_for_cancel_owner_lock_before_issuance(db):
    import asyncio

    state = await claimed_resume(db)
    await authorize_resume(db, state)
    retry = state["retry"]
    async with db.acquire() as conn:
        transaction = conn.transaction()
        await transaction.start()
        task = None
        try:
            await conn.fetchrow(
                "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE",
                retry["job_id"],
            )
            await conn.fetchrow(
                "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", retry["job_id"]
            )
            task = asyncio.create_task(
                db.execute(
                    "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,effect_kind,carrier_uid,carrier_namespace,carrier_intent) "
                    "VALUES($1,$2,1,'rootdisk',$3,'workers','{}'::jsonb)",
                    uuid4(),
                    retry["request_id"],
                    uuid4(),
                )
            )
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), timeout=0.15)
            await conn.execute(
                "UPDATE jobs SET status='cancelled',context=context||'{\"_stateless_cancel_cleanup_pending\":true}'::jsonb WHERE id=$1",
                retry["job_id"],
            )
            await transaction.commit()
            transaction = None
            with pytest.raises(
                asyncpg.CheckViolationError, match="committed live authority"
            ):
                await task
        finally:
            if transaction is not None:
                await transaction.rollback()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_inherited_disposition_root_is_exact_kept_dv_not_a_fabricated_c_effect(
    db, monkeypatch
):
    from orchestrator.services.vm_creation_disposition_store import (
        VMCreationDispositionStore,
    )
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
    from tests import test_vm_job_cancel_retention_real_postgres as retention_fixture

    original_preflight = retention_fixture.preflight

    def canonical_name(state):
        return {
            **original_preflight(state),
            "pvc_name": f"agent-vm-{state['job_id']}-rootdisk",
        }

    # The old SQL-only fixture permits an arbitrary witnessed PVC name. The
    # production disposition actuator additionally requires the canonical name.
    monkeypatch.setattr(retention_fixture, "preflight", canonical_name)
    state = await claimed_resume(db)
    await authorize_resume(db, state)
    assert (await db.cancel_stateless_job(state["job_id"]))[0]
    async with db.acquire() as conn:
        retry = dict(
            await conn.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                state["retry"]["request_id"],
            )
        )
        inherited = json.loads(
            await conn.fetchval(
                "SELECT public.vm_job_retained_inherited_rootdisk(r) FROM vm_creation_retries r WHERE request_id=$1",
                retry["request_id"],
            )
        )
        assert inherited == {
            "name": state["preflight"]["pvc_name"],
            "namespace": state["preflight"]["namespace"],
            "uid": state["preflight"]["dv_uid"],
            "pvc_uid": state["frozen"]["pvc_uid"],
        }
        retry["canonical_request"] = json.loads(retry["canonical_request"])
        disposition = {
            "disposition_id": str(uuid4()),
            "namespace": inherited["namespace"],
            "disk_policy": "retain",
            "objects": {},
            "effects": [],
        }
        grant = await VMCreationDispositionStore(VMCreationRetryStore(db))._grant(
            conn, retry, disposition, "rootdisk", create_child=False
        )
        assert grant["operation"] == "retain_inherited_rootdisk"
        assert grant["resource"] == inherited
        assert disposition["objects"] == {}
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_creation_effects WHERE request_id=$1)",
            retry["request_id"],
        )

        for evidence, expected in (
            (grant["completion"], True),
            ({**grant["completion"], "uid": str(uuid4())}, False),
            ({**grant["completion"], "extra": True}, False),
        ):
            assert (
                await conn.fetchval(
                    "SELECT public.valid_vm_creation_resource_completion(jsonb_populate_record(r,"
                    "jsonb_build_object('cancellation_disposition',$2::jsonb,'cancellation_progress',"
                    "jsonb_build_object('rootdisk',$3::jsonb))),'rootdisk',$3::jsonb) "
                    "FROM vm_creation_retries r WHERE request_id=$1",
                    retry["request_id"],
                    json.dumps(disposition),
                    json.dumps(evidence),
                )
                is expected
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("tail", ["source_absent", "never_issued"])
async def test_explicit_delete_purges_physical_disk_and_audits_logical_noeffect_tail(
    db, tail
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from orchestrator.services.vm_job_retained_resume import settle_retained_no_compute
    from orchestrator.services.vm_job_retained_disk_purge import (
        acquire_job_retained_disk_purge,
        complete_job_retained_disk_purge,
        read_job_retained_disk_purge_candidate,
    )
    from orchestrator.services.vm_workspace_recovery_store import (
        VMWorkspaceRecoveryStore,
    )

    state = await accepted_resume(db)
    if tail == "never_issued":
        await admit_resume(db, state)
    assert (await db.cancel_stateless_job(state["job_id"]))[0]
    assert await settle_retained_no_compute(db, state["job_id"])
    assert await db.complete_stateless_cancel_cleanup(state["job_id"])
    assert await db.prepare_stateless_job_for_delete(state["job_id"])
    store = VMWorkspaceRecoveryStore(db)
    purge = await acquire_job_retained_disk_purge(
        store, job_id=state["job_id"], identity=state["identity"]
    )
    assert purge.allowed, purge.reason
    candidate = await read_job_retained_disk_purge_candidate(store, purge)
    evidence = {
        "version": 1,
        "kind": "vm_cleanup_physical_stop",
        **{
            key: candidate[key]
            for key in (
                "job_id",
                "provision_generation",
                "vm_uid",
                "vmi_uid",
                "launcher_uid",
                "pvc_uid",
                "controller_scope",
            )
        },
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "pvc_disposition": "purged",
        "controller_authenticated": True,
    }
    await complete_job_retained_disk_purge(
        store,
        purge,
        outcome="completed",
        provisioner=SimpleNamespace(
            attest_vm_cleanup_stop=AsyncMock(return_value=evidence)
        ),
    )
    assert await db.fetchval(
        "SELECT public.vm_job_cancel_retention_discharged($1)",
        state["retention"].admission_id,
    )
    assert (
        await acquire_job_retained_disk_purge(
            store, job_id=state["job_id"], identity=state["identity"]
        )
    ).completed_outcome == "completed"
    if tail == "never_issued":
        claimant = f"{uuid4()}:{uuid4()}"
        no_effect_tab = await db.fetchval(
            "INSERT INTO vm_idle_access_leases "
            "(owner_kind,owner_id,provision_generation,vm_uid,kind,claimed_by,"
            "acquired_at,expires_at,max_expires_at) VALUES "
            "('job',$1,$2,$3,'ide',$4,clock_timestamp()-interval '5 minutes',"
            "clock_timestamp()-interval '3 minutes',"
            "clock_timestamp()-interval '1 minute') RETURNING id",
            UUID(state["job_id"]),
            state["resume"]["provision_generation"],
            uuid4(),
            claimant,
        )
        with pytest.raises(JobVMAuditNotReady, match="cleanup or access remains open"):
            await db.delete_job(state["job_id"], prepared_stateless=True)
        assert (
            await db.fetchval(
                "SELECT closed_at IS NULL FROM vm_idle_access_leases WHERE id=$1",
                no_effect_tab,
            )
            is True
        )
        assert await VMIdleAccessStore(db).close(
            str(no_effect_tab),
            owner_kind="job",
            owner_id=state["job_id"],
            kind="ide",
            claimant=claimant,
        )
    assert await db.delete_job(state["job_id"], prepared_stateless=True)
    assert (
        await db.fetchval(
            "SELECT public.vm_job_retained_disk_purge_chain_digest($1)",
            purge.admission_id,
        )
        == candidate["chain_digest"]
    )
    assert await db.fetchval(
        "SELECT public.vm_job_cancel_retention_discharged($1)",
        state["retention"].admission_id,
    )
    if tail == "never_issued":
        packet = await db.fetchrow(
            "SELECT * FROM vm_job_creation_terminal_packets WHERE request_id=$1",
            state["resume"]["request_id"],
        )
        assert packet["terminal_kind"] == "retained_noeffect"


@pytest.mark.asyncio
@pytest.mark.parametrize("proof_available", [True, False])
async def test_shared_ready_admission_probes_before_authority_and_replays_without_io(
    db, proof_available
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from orchestrator.services import vm_job_retained_resume as service
    from orchestrator.services.vm_workspace_recovery_store import (
        VMWorkspaceRecoveryStore,
    )

    state = await adopted_resume(db)

    async def qualify(candidate):
        assert not await db.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_job_cancel_retention_authorities WHERE job_retained_resume_id=$1)",
            state["resume"]["id"],
        )
        return ready_witness(candidate) if proof_available else None

    provisioner = SimpleNamespace(
        qualify_retained_ready_stop=AsyncMock(side_effect=qualify)
    )
    permit = await service.acquire_retained_terminal_cleanup(
        VMWorkspaceRecoveryStore(db),
        provisioner,
        job_id=state["job_id"],
        identity=state["identity"],
    )
    assert permit.allowed is proof_available
    provisioner.qualify_retained_ready_stop.assert_awaited_once()
    if proof_available:
        replay = await service.acquire_retained_terminal_cleanup(
            VMWorkspaceRecoveryStore(db),
            provisioner,
            job_id=state["job_id"],
            identity=state["identity"],
        )
        assert replay.admission_id == permit.admission_id
        provisioner.qualify_retained_ready_stop.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tail", "cancel_first"),
    [
        ("source_absent", False),
        ("source_absent", True),
        ("never_issued", False),
        ("never_issued", True),
        ("ready", True),
    ],
)
async def test_public_job_delete_resolves_retained_disk_before_null_vm_skip(
    db, tail, cancel_first
):
    import logging
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from orchestrator.services.job_mutation_controls import JobControlOperations
    from orchestrator.services.thread_retirement import archive_and_cleanup_workspace
    from orchestrator.services.vm_job_retained_resume import settle_retained_no_compute
    from orchestrator.services.vm_provisioner import VMTeardownResult
    from orchestrator.services.vm_workspace_policy import vm_needs_release

    state = (
        await settled_ready_keep(db) if tail == "ready" else await accepted_resume(db)
    )
    if tail == "never_issued":
        await admit_resume(db, state)
    if cancel_first and tail != "ready":
        assert (await db.cancel_stateless_job(state["job_id"]))[0]
        assert await settle_retained_no_compute(db, state["job_id"])
        assert await db.complete_stateless_cancel_cleanup(state["job_id"])
        await db.execute(
            "UPDATE jobs SET context=context-'vm' WHERE id=$1", UUID(state["job_id"])
        )

    async def attest(candidate):
        return {
            "version": 1,
            "kind": "vm_cleanup_physical_stop",
            **{
                key: candidate[key]
                for key in (
                    "job_id",
                    "provision_generation",
                    "vm_uid",
                    "vmi_uid",
                    "launcher_uid",
                    "pvc_uid",
                    "controller_scope",
                )
            },
            "vm_absent": True,
            "vmi_absent": True,
            "launcher_absent": True,
            "same_generation_replacement": False,
            "pvc_disposition": "purged",
            "controller_authenticated": True,
        }

    provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(
            side_effect=AssertionError("logical C has no VM")
        ),
        delete_vm_captured=AsyncMock(return_value=VMTeardownResult("completed", True)),
        attest_vm_cleanup_stop=AsyncMock(side_effect=attest),
    )
    archive_dependencies = SimpleNamespace(
        store=db,
        recovery_store=state["recovery"],
        vm_provisioner=provisioner,
        container_provisioner=None,
        docker_provisioner=None,
        get_container_context=lambda _: {},
        get_vm_context=lambda job: (
            json.loads(job["context"])
            if isinstance(job["context"], str)
            else job["context"]
        ).get("vm", {}),
        vm_needs_release=vm_needs_release,
    )

    async def archive(job_id):
        return await archive_and_cleanup_workspace(
            job_id, dependencies=archive_dependencies
        )

    @asynccontextmanager
    async def vector_connection():
        yield SimpleNamespace(execute=AsyncMock())

    controls = JobControlOperations(
        SimpleNamespace(
            store=db,
            logger=logging.getLogger(__name__),
            archive_and_cleanup_workspace=archive,
            snapshot_service=SimpleNamespace(is_available=False),
            vector_db=SimpleNamespace(acquire=vector_connection),
            resolve_job_notifications=AsyncMock(),
        )
    )
    result = await controls.delete(
        state["job_id"],
        caller={"id": str(state["resume"]["requested_by"])},
        job=await db.get_job(state["job_id"]),
    )
    assert result["status"] == "deleted"
    provisioner.capture_vm_teardown_identity.assert_not_awaited()
    call = provisioner.delete_vm_captured.await_args
    assert call.args[1] == state["identity"] and call.kwargs["purge_disk"] is True


async def authorized_completed_resume(db):
    from tests.test_completion_teardown_authority_real_postgres import (
        _command,
        _effect,
        _status_effect,
    )
    from orchestrator.services.completion_teardown_authority import (
        authorize_workspace_teardown,
    )

    state = await adopted_resume(db)
    owner = UUID(state["job_id"])
    await db.execute(
        "UPDATE jobs SET status='completed', completion_seq_hwm=1,context=context-'_stateless_cancel_cleanup_pending' WHERE id=$1",
        owner,
    )
    assert not await db.fetchval(
        "SELECT public.vm_job_retained_terminal_authorized($1)", owner
    )
    command = await _command(db, job_id=owner, report_seq=1)
    await _effect(db, command_id=command, job_id=owner)
    await _status_effect(db, command_id=command, job_id=owner)
    decision = await authorize_workspace_teardown(
        db, job_id=state["job_id"], command_id=str(command), owner="owner-1"
    )
    assert decision.authorized
    state["completion_command"] = command
    return state


@pytest.mark.asyncio
async def test_completed_continuation_uses_durable_s36_status_authority(db):
    from orchestrator.services.vm_job_retained_resume import retained_ready_candidate

    state = await authorized_completed_resume(db)
    assert await db.fetchval(
        "SELECT public.vm_job_retained_terminal_authorized($1)", UUID(state["job_id"])
    )
    assert await retained_ready_candidate(
        db, job_id=state["job_id"], identity=state["identity"]
    )
    await db.execute(
        "UPDATE completion_effects SET detail=jsonb_set(detail,'{output,new_status}','\"failed\"') WHERE producer_id=$1 AND effect_name='main_status_write'",
        state["completion_command"],
    )
    assert not await db.fetchval(
        "SELECT public.vm_job_retained_terminal_authorized($1)", UUID(state["job_id"])
    )


@pytest.mark.asyncio
async def test_ready_reader_allows_only_committed_exact_typed_permanent_delete(db):
    from orchestrator.services.vm_job_retained_resume import (
        read_current_ready_preflight,
    )
    from orchestrator.services.vm_job_retained_disk_purge import (
        acquire_job_retained_disk_purge,
    )
    from shared.vm_resource_admission import ResourceAdmissionError

    state = await settled_ready_keep(db)
    assert await db.prepare_stateless_job_for_delete(state["job_id"])
    purge = await acquire_job_retained_disk_purge(
        state["recovery"], job_id=state["job_id"], identity=state["identity"]
    )
    assert purge.allowed
    assert (
        await read_current_ready_preflight(
            db,
            purge.parent_cleanup,
            job_id=state["job_id"],
            generation=state["identity"].provision_generation,
        )
        is None
    )
    for malformed in (
        None,
        state["keep"].parent_cleanup,
        {
            **purge.parent_cleanup,
            "intent": {**purge.parent_cleanup["intent"], "vm_uid": str(uuid4())},
        },
    ):
        with pytest.raises(ResourceAdmissionError):
            await read_current_ready_preflight(
                db,
                malformed,
                job_id=state["job_id"],
                generation=state["identity"].provision_generation,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["no_delete", "wrong_current", "active_queue", "native_source_changed"]
)
async def test_typed_purge_refuses_missing_request_or_changed_logical_tail(db, fault):
    from orchestrator.services.vm_job_retained_resume import settle_retained_no_compute
    from orchestrator.services.vm_job_retained_disk_purge import (
        acquire_job_retained_disk_purge,
    )
    from shared.vm_resource_admission import ResourceAdmissionError

    state = await accepted_resume(db)
    await admit_resume(db, state)
    assert (await db.cancel_stateless_job(state["job_id"]))[0]
    assert await settle_retained_no_compute(db, state["job_id"])
    assert await db.complete_stateless_cancel_cleanup(state["job_id"])
    if fault != "no_delete":
        assert await db.prepare_stateless_job_for_delete(state["job_id"])
    if fault == "wrong_current":
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{_vm_job_retained_resume}',to_jsonb($2::text)) WHERE id=$1",
            UUID(state["job_id"]),
            str(uuid4()),
        )
    elif fault == "active_queue":
        await db.execute(
            "UPDATE run_queue SET state='ready' WHERE unit_id=$1", UUID(state["job_id"])
        )
    elif fault == "native_source_changed":
        await db.execute(
            "UPDATE vm_creation_retries SET resolved_at=resolved_at+interval '1 second' WHERE request_id=$1",
            state["resume"]["request_id"],
        )
    with pytest.raises((asyncpg.CheckViolationError, ResourceAdmissionError)):
        await acquire_job_retained_disk_purge(
            state["recovery"], job_id=state["job_id"], identity=state["identity"]
        )
    assert not await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM vm_job_retained_disk_purge_authorities WHERE job_id=$1)",
        UUID(state["job_id"]),
    )


@pytest.mark.asyncio
async def test_missing_typed_marker_cannot_fall_back_to_legacy_physical_purge(db):
    from orchestrator.services.vm_job_retained_disk_purge import (
        acquire_job_retained_disk_purge,
    )
    from shared.vm_resource_admission import ResourceAdmissionError

    state = await settled_ready_keep(db)
    assert await db.prepare_stateless_job_for_delete(state["job_id"])
    await db.execute(
        "UPDATE jobs SET context=context-'_vm_job_retained_resume' WHERE id=$1",
        UUID(state["job_id"]),
    )
    with pytest.raises((asyncpg.CheckViolationError, ResourceAdmissionError)):
        await acquire_job_retained_disk_purge(
            state["recovery"], job_id=state["job_id"], identity=state["identity"]
        )
