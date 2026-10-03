"""Native non-quota End must preserve an exact retained-Resume predecessor."""

import json
from pathlib import Path

import asyncpg
import pytest_asyncio

import pytest
from tests.test_vm_nonquota_readiness_real_postgres import prepared
from testcontainers.postgres import PostgresContainer

from orchestrator.services.vm_provisioner import VMTeardownIdentity
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    acquire_pinned_thread_retirement_cleanup_permit,
    prepare_vm_cleanup_resource,
)
from tests.test_vm_thread_adopted_without_quotas_delete_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    adopted_source,
    cleaned_retirement,
    db,  # noqa: F401
    setup,  # noqa: F401
    thread_schema as _thread_schema,  # noqa: F401 - pytest fixture registration
)


@pytest.fixture(scope="module")
def pg_dsn():
    container = (
        PostgresContainer("postgres:15")
        .with_kwargs(mem_limit="1g", nano_cpus=2_000_000_000, pids_limit=128)
        .with_command(
            "postgres -c fsync=off -c synchronous_commit=off "
            "-c full_page_writes=off -c max_connections=32"
        )
    )
    container.start()
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def thread_schema(pg_dsn, _thread_schema):  # noqa: F811 - fixture dependency
    migration = (
        Path(__file__).resolve().parents[1]
        / "src/orchestrator/database/migrations/app/0323_nonquota_retained_vm_resume.sql"
    )
    if migration.exists():
        conn = await asyncpg.connect(pg_dsn)
        try:
            if not await conn.fetchval(
                "SELECT to_regprocedure('public.valid_vm_thread_nonquota_creation(public.vm_creation_retries)') IS NOT NULL"
            ):
                await conn.execute(migration.read_text())
        finally:
            await conn.close()


@pytest.mark.asyncio
async def test_nonquota_soft_end_requires_exact_controller_stop_before_completion(
    db,  # noqa: F811 - imported fixture
    setup,  # noqa: F811 - imported fixture
    monkeypatch,  # noqa: F811 - imported PostgreSQL/controller fixtures
):
    current, source = await adopted_source(db, setup, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(
        str(current["id"]), permanent=False
    )
    assert retirement["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        str(current["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    vm = retirement["context"]["vm"]
    recovery = VMWorkspaceRecoveryStore(db)
    permit = await acquire_pinned_thread_retirement_cleanup_permit(
        recovery,
        thread_id=current["id"],
        identity=VMTeardownIdentity(
            provision_generation=vm["provision_generation"],
            vm_uid=vm["vm_uid"],
            rootdisk_pvc_uid=vm["rootdisk_pvc_uid"],
        ),
        purge_disk=False,
    )
    assert permit.allowed
    candidate = await prepare_vm_cleanup_resource(recovery, permit)
    assert await db.fetchval("SELECT count(*) FROM vm_resource_reservations") == 0
    assert await db.fetchval(
        "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
        permit.admission_id,
    )
    assert candidate is not None, (
        "non-quota soft End bypasses exact controller stop capture"
    )
    assert candidate["vm_uid"] == str(source["observed_vm_uid"])
    assert candidate["pvc_uid"] == str(source["observed_pvc_uid"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rotated", [False, True], ids=["original-actor", "confirmed-successor"]
)
async def test_native_nonquota_end_resume_records_retained_operation_without_kept_marker(
    db,  # noqa: F811 - imported fixture
    setup,  # noqa: F811 - imported fixture
    monkeypatch,
    rotated,
):
    current, source, _, _ = await prepared(db, setup, monkeypatch, rotated)
    retirement = await cleaned_retirement(db, current, permanent=False)
    assert await db.settle_pinned_thread_retirement(
        str(current["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        final_status="ended",
    )
    # The shared fake stops at the CAS's retiring projection. Production's
    # completed VM delete then publishes deleted under the same generation.
    assert await db.merge_thread_vm_context_if_provision_generation(
        str(current["id"]), str(source["provision_generation"]), {"status": "deleted"}
    )
    ended = await db.get_thread(str(current["id"]))
    vm = json.loads(ended["metadata"])["vm"]
    assert vm["status"] == "deleted" and vm.get("rootdisk") is None
    assert await db.resume_thread(str(current["id"]))
    resumed = await db.get_thread(str(current["id"]))
    assert resumed["runtime_generation"] != ended["runtime_generation"]
    assert resumed["agent_id"] is None and resumed["runtime_attach_token"] is None
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
        )
        == source
    )
    assert await db.fetchval("SELECT count(*) FROM vm_resource_reservations") == 0
    operation = await db.fetchrow(
        "SELECT * FROM vm_thread_retained_resumes WHERE thread_id=$1 AND runtime_generation=$2",
        current["id"],
        resumed["runtime_generation"],
    )
    assert operation is not None, (
        "accepted non-quota Resume has no durable retained-disk operation"
    )
    assert operation["predecessor_runtime_generation"] == ended["runtime_generation"]


async def ended_nonquota(store, controller_setup, monkeypatch):
    current, source = await adopted_source(store, controller_setup, monkeypatch)
    retirement = await cleaned_retirement(store, current, permanent=False)
    assert await store.settle_pinned_thread_retirement(
        str(current["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        final_status="ended",
    )
    return await store.get_thread(str(current["id"])), source


@pytest.mark.asyncio
async def test_nonquota_retained_resume_waits_for_actor_then_admits_one_exact_source(
    db,  # noqa: F811 - imported fixture
    setup,  # noqa: F811 - imported fixture
    monkeypatch,  # noqa: F811
):
    from orchestrator.services.vm_thread_retained_resume import (
        ensure_retained_thread_vm,
    )
    from orchestrator.services.vm_provisioner import VMProvisioner
    from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent
    from tests.test_vm_creation_actuation import SECRET

    ended, old_source = await ended_nonquota(db, setup, monkeypatch)
    assert await db.resume_thread(str(ended["id"]))
    resumed = await db.get_thread(str(ended["id"]))
    operation = await db.fetchrow(
        "SELECT * FROM vm_thread_retained_resumes WHERE thread_id=$1", ended["id"]
    )
    assert operation is not None
    calls = []
    configuration = json.loads(old_source["controller_configuration"])

    async def resolve(_client, request, *, secret):
        assert secret == SECRET
        calls.append(request)
        return {"request": request, "controller_configuration": configuration}

    monkeypatch.setattr(
        "orchestrator.services.vm_creation_transport.resolve_vm_creation_configuration",
        resolve,
    )
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "false")
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._controller_url = "http://controller.test"
    provisioner._http_client = object()
    provisioner._lifecycle_hmac_secret = SECRET
    assert (
        await ensure_retained_thread_vm(resumed, store=db, provisioner=provisioner)
        is False
    )
    assert calls == []
    assert await db.fetchval("SELECT count(*) FROM vm_creation_retries") == 1
    from uuid import uuid4

    current = await _bind_cold_agent(
        db, ended["id"], pod_name="srw-agent-s-" + uuid4().hex[:8]
    )
    assert await ensure_retained_thread_vm(current, store=db, provisioner=provisioner)
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", operation["request_id"]
    )
    assert source is not None
    assert source["thread_runtime_generation"] == current["runtime_generation"]
    assert source["thread_agent_id"] == current["agent_id"]
    assert source["thread_attach_token"] == current["runtime_attach_token"]
    assert source["expected_pvc_uid"] == old_source["observed_pvc_uid"]
    assert source["provision_generation"] == operation["provision_generation"]
    assert source["thread_retained_resume_id"] == operation["id"]
    assert await ensure_retained_thread_vm(
        await db.get_thread(str(ended["id"])), store=db, provisioner=provisioner
    )
    assert dict(
        await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1",
            operation["request_id"],
        )
    ) == dict(source)
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                old_source["request_id"],
            )
        )
        == old_source
    )
    assert await db.fetchval("SELECT count(*) FROM vm_creation_retries") == 2
    assert await db.fetchval("SELECT count(*) FROM vm_resource_reservations") == 0
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "runtime_generation",
        "agent_id",
        "attach_token",
        "vm_uid",
        "pvc_uid",
        "reservation_id",
        "vmi_uid",
        "missing_vmi",
        "missing_launcher",
        "missing_both_processes",
    ],
)
async def test_nonquota_cleanup_authority_refuses_changed_exact_tuple(
    db,  # noqa: F811 - imported fixture
    setup,  # noqa: F811 - imported fixture
    monkeypatch,
    fault,  # noqa: F811
):
    from uuid import uuid4

    current, source, _, _ = await prepared(db, setup, monkeypatch, False)
    retirement = await db.begin_pinned_thread_retirement(
        str(current["id"]), permanent=False
    )
    assert await db.authorize_pinned_thread_retirement(
        str(current["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    vm = retirement["context"]["vm"]
    permit = await acquire_pinned_thread_retirement_cleanup_permit(
        VMWorkspaceRecoveryStore(db),
        thread_id=current["id"],
        identity=VMTeardownIdentity(
            provision_generation=vm["provision_generation"],
            vm_uid=vm["vm_uid"],
            rootdisk_pvc_uid=vm["rootdisk_pvc_uid"],
        ),
        purge_disk=False,
    )
    changed = {fault: str(uuid4())}
    if fault.startswith("missing_"):
        changed = {
            key: None
            for key in ("vmi_uid", "launcher_uid")
            if fault == "missing_both_processes"
            or key == ("vmi_uid" if fault == "missing_vmi" else "launcher_uid")
        }
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.fetchval(
                "SELECT public.validate_vm_thread_cleanup_authority(json_populate_record(NULL::public.vm_resource_thread_cleanup_authorities, (to_jsonb(a)||$2::jsonb)::json)) FROM vm_resource_thread_cleanup_authorities a WHERE cleanup_admission_id=$1",
                permit.admission_id,
                json.dumps(changed),
            )
    assert (
        await db.fetchval("SELECT count(*) FROM vm_resource_thread_cleanup_stops") == 0
    )
    assert await db.fetchval(
        "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
        permit.admission_id,
    )
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
        )
        == source
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        None,
        "vm_uid",
        "pvc_uid",
        "controller_authenticated",
        "vm_absent",
        "pvc_disposition",
        "missing_zero",
    ],
)
async def test_nonquota_end_records_only_exact_authenticated_stop(
    db,  # noqa: F811 - imported fixture
    setup,  # noqa: F811 - imported fixture
    monkeypatch,
    fault,  # noqa: F811
):
    from uuid import uuid4
    from unittest.mock import AsyncMock
    from types import SimpleNamespace
    from shared.vm_resource_admission import ResourceAdmissionError
    from orchestrator.services.vm_workspace_recovery_store import (
        complete_vm_cleanup_permit,
    )

    current, source = await adopted_source(db, setup, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(
        str(current["id"]), permanent=False
    )
    assert await db.authorize_pinned_thread_retirement(
        str(current["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    vm = retirement["context"]["vm"]
    recovery = VMWorkspaceRecoveryStore(db)
    permit = await acquire_pinned_thread_retirement_cleanup_permit(
        recovery,
        thread_id=current["id"],
        identity=VMTeardownIdentity(
            provision_generation=vm["provision_generation"],
            vm_uid=vm["vm_uid"],
            rootdisk_pvc_uid=vm["rootdisk_pvc_uid"],
        ),
        purge_disk=False,
    )
    candidate = await prepare_vm_cleanup_resource(recovery, permit)
    assert candidate is not None
    proof = {
        "version": 1,
        "kind": "vm_cleanup_physical_stop",
        **{k: v for k, v in candidate.items() if k != "purge_disk"},
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "controller_authenticated": True,
        "pvc_disposition": "retained",
    }
    if fault in {"vm_uid", "pvc_uid"}:
        proof[fault] = str(uuid4())
    elif fault in {"controller_authenticated", "vm_absent"}:
        proof[fault] = False
    elif fault == "pvc_disposition":
        proof[fault] = "purged"
    if fault != "missing_zero":
        assert await db.record_managed_repository_workspace_process_zero(
            str(current["id"]),
            owner_kind="thread",
            scope="vm",
            provisioner="vm",
            runtime_incarnation=str(source["provision_generation"]),
        )
    physical = SimpleNamespace(attest_vm_cleanup_stop=AsyncMock(return_value=proof))
    if fault:
        with pytest.raises(ResourceAdmissionError):
            await complete_vm_cleanup_permit(
                recovery, permit, outcome="completed", provisioner=physical
            )
        assert (
            await db.fetchval("SELECT count(*) FROM vm_resource_thread_cleanup_stops")
            == 0
        )
        assert await db.fetchval(
            "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
            permit.admission_id,
        )
    else:
        await complete_vm_cleanup_permit(
            recovery, permit, outcome="completed", provisioner=physical
        )
        await complete_vm_cleanup_permit(
            recovery, permit, outcome="completed", provisioner=physical
        )
        assert (
            await db.fetchval("SELECT count(*) FROM vm_resource_thread_cleanup_stops")
            == 1
        )
        assert (
            await db.fetchval(
                "SELECT outcome FROM vm_workspace_cleanup_admissions WHERE id=$1",
                permit.admission_id,
            )
            == "completed"
        )
        assert physical.attest_vm_cleanup_stop.await_count == 1
    assert await db.fetchval("SELECT count(*) FROM vm_resource_reservations") == 0
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
        )
        == source
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "missing_stop",
        "wrong_stop",
        "missing_zero",
        "unfinished_cleanup",
        "wrong_outcome",
    ],
)
async def test_nonquota_resume_refuses_damaged_retained_proof_before_rotation(
    db,  # noqa: F811 - imported fixture
    setup,  # noqa: F811 - imported fixture
    monkeypatch,
    fault,  # noqa: F811
):
    ended, _ = await ended_nonquota(db, setup, monkeypatch)
    # Model damaged historical evidence in a disposable database only. Every
    # production authority table remains append-only; no live ledger is changed.
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role=replica")
        if fault == "missing_stop":
            await conn.execute("DELETE FROM vm_resource_thread_cleanup_stops")
        elif fault == "wrong_stop":
            await conn.execute(
                "UPDATE vm_resource_thread_cleanup_stops SET stop_evidence=jsonb_set(stop_evidence,'{vm_absent}','false')"
            )
        elif fault == "missing_zero":
            await conn.execute(
                "DELETE FROM managed_repository_process_zero_receipts WHERE scope='vm'"
            )
        elif fault == "unfinished_cleanup":
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET completed_at=NULL,outcome=NULL "
                "WHERE source='pinned_thread_retirement' AND owner_id=$1 AND completed_at IS NOT NULL",
                ended["id"],
            )
        elif fault == "wrong_outcome":
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET outcome='refused' "
                "WHERE source='pinned_thread_retirement' AND owner_id=$1 AND completed_at IS NOT NULL",
                ended["id"],
            )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.resume_thread(str(ended["id"]))
    current = await db.get_thread(str(ended["id"]))
    assert (
        current["status"] == "ended"
        and current["runtime_generation"] == ended["runtime_generation"]
    )
    assert await db.fetchval("SELECT count(*) FROM vm_thread_retained_resumes") == 0
    assert await db.fetchval("SELECT count(*) FROM vm_creation_retries") == 1


@pytest.mark.asyncio
async def test_nonquota_retained_resume_actuates_same_disk_and_permanently_deletes(
    db,  # noqa: F811
    setup,  # noqa: F811
    monkeypatch,
):
    from uuid import UUID, uuid4
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
    from orchestrator.services.vm_thread_retained_resume import (
        ensure_retained_thread_vm,
    )
    from orchestrator.services.vm_provisioner import VMProvisioner
    from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent
    from tests.test_vm_creation_actuation import SECRET
    from vm_controller.creation_actuation import CreationActuator
    from vm_controller.creation_configuration import resolve_creation_configuration

    controller, api, _, _ = setup
    ended, original = await ended_nonquota(db, setup, monkeypatch)
    # The external adapter models the completed old End: exact compute gone,
    # original DV/PVC survive. Durable authority was established through SQL.
    for key in list(api.objects):
        if key[0] not in {"DataVolume", "PersistentVolumeClaim", "Lease"}:
            del api.objects[key]
    disk_uids = {
        key: obj["metadata"]["uid"]
        for key, obj in api.objects.items()
        if key[0] in {"DataVolume", "PersistentVolumeClaim"}
    }
    assert await db.resume_thread(str(ended["id"]))
    current = await _bind_cold_agent(
        db, ended["id"], pod_name="srw-agent-s-" + uuid4().hex[:8]
    )

    async def resolve(_client, request, *, secret):
        assert secret == SECRET
        return resolve_creation_configuration(controller, request)

    monkeypatch.setattr(
        "orchestrator.services.vm_creation_transport.resolve_vm_creation_configuration",
        resolve,
    )
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._controller_url = "http://controller.test"
    provisioner._http_client = object()
    provisioner._lifecycle_hmac_secret = SECRET
    assert await ensure_retained_thread_vm(current, store=db, provisioner=provisioner)
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1 AND request_id<>$2",
        ended["id"],
        original["request_id"],
    )
    retry = VMCreationRetryStore(db)
    claim = (await retry.claim_due(limit=1))[0]

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[1].replace("-", "_")
        assert operation == "creation_retry_" + method
        return await getattr(
            retry, "authorize_controller" if method == "authorize" else method
        )(**body)

    controller._workspace_cleanup_authority_request = authority
    payload = {
        **json.loads(source["canonical_request"]),
        "creation_retry": {
            "version": 1,
            "request_id": str(source["request_id"]),
            "claim_token": str(claim["claim_token"]),
            "request_digest": source["request_digest"],
            "controller_configuration_digest": source[
                "controller_configuration_digest"
            ],
        },
    }
    for _ in range(8):
        result = await CreationActuator(controller)._run(payload)
        if result["status"] == "created":
            break
    assert result["status"] == "created", result
    adopted = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", source["request_id"]
    )
    assert adopted["state"] == "succeeded" and adopted["reason"] == "creation_adopted"
    assert (
        adopted["expected_pvc_uid"]
        == original["observed_pvc_uid"]
        == adopted["observed_pvc_uid"]
    )
    assert adopted["observed_vm_uid"] != original["observed_vm_uid"]
    assert {
        key: obj["metadata"]["uid"]
        for key, obj in api.objects.items()
        if key[0] in {"DataVolume", "PersistentVolumeClaim"}
    } == disk_uids
    assert await db.fetchval("SELECT count(*) FROM vm_resource_reservations") == 0
    permanent = await cleaned_retirement(
        db, await db.get_thread(str(ended["id"])), permanent=True
    )
    await db.delete_thread(
        str(ended["id"]),
        expected_runtime_generation=permanent["generation"],
        expected_runtime_retirement_token=permanent["token"],
    )
    assert await db.get_thread(str(ended["id"])) is None
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                original["request_id"],
            )
        )
        == original
    )
    assert UUID(permanent["generation"]) == current["runtime_generation"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "permanent", [False, True], ids=["soft-again", "permanent-before-actor"]
)
async def test_nonquota_retained_resume_end_before_actor_or_creation_settles(
    db,  # noqa: F811
    setup,  # noqa: F811
    monkeypatch,
    permanent,
):
    ended, source = await ended_nonquota(db, setup, monkeypatch)
    assert await db.resume_thread(str(ended["id"]))
    current = await db.get_thread(str(ended["id"]))
    assert current["agent_id"] is None
    assert await db.fetchval("SELECT count(*) FROM vm_creation_retries") == 1
    retirement = await cleaned_retirement(db, current, permanent=permanent)
    # Begin captures the proven retained predecessor, never a new VM identity.
    assert retirement["context"]["vm"]["vm_uid"] == str(source["observed_vm_uid"])
    assert retirement["context"]["vm"]["provision_generation"] == str(
        source["provision_generation"]
    )
    assert await db.fetchval("SELECT count(*) FROM vm_creation_retries") == 1
    assert await db.fetchval("SELECT count(*) FROM vm_resource_reservations") == 0
    if permanent:
        await db.delete_thread(
            str(ended["id"]),
            expected_runtime_generation=retirement["generation"],
            expected_runtime_retirement_token=retirement["token"],
        )
        assert await db.get_thread(str(ended["id"])) is None
    else:
        assert await db.settle_pinned_thread_retirement(
            str(ended["id"]),
            token=retirement["token"],
            generation=retirement["generation"],
            final_status="ended",
        )
        assert await db.resume_thread(str(ended["id"]))
        resumed = await db.get_thread(str(ended["id"]))
        assert resumed["runtime_generation"] != current["runtime_generation"]
        assert resumed["agent_id"] is None and resumed["runtime_attach_token"] is None
        assert await db.fetchval("SELECT count(*) FROM vm_thread_retained_resumes") == 2
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
        )
        == source
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "missing_operation",
        "changed_capture",
        "new_source",
        "exposed",
        "captured_exposed",
        "captured_control",
        "old_stop",
        "old_zero",
    ],
)
async def test_nonquota_uncreated_end_refuses_changed_predecessor_or_current_work(
    db,  # noqa: F811
    setup,  # noqa: F811
    monkeypatch,
    fault,
):
    ended, source = await ended_nonquota(db, setup, monkeypatch)
    assert await db.resume_thread(str(ended["id"]))
    current = await db.get_thread(str(ended["id"]))
    retirement = await db.begin_pinned_thread_retirement(
        str(ended["id"]), permanent=False
    )
    assert await db.authorize_pinned_thread_retirement(
        str(ended["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role=replica")
        if fault == "missing_operation":
            await conn.execute(
                "DELETE FROM vm_thread_retained_resumes WHERE thread_id=$1", ended["id"]
            )
        elif fault == "changed_capture":
            await conn.execute(
                "UPDATE vm_thread_retained_resumes SET retained_vm=jsonb_set(retained_vm,'{vm_uid}',to_jsonb(gen_random_uuid()::text)) WHERE thread_id=$1",
                ended["id"],
            )
        elif fault == "new_source":
            await conn.execute(
                "INSERT INTO vm_creation_retries SELECT changed.* FROM vm_creation_retries r "
                "JOIN vm_thread_retained_resumes op ON op.thread_id=r.thread_id "
                "CROSS JOIN LATERAL json_populate_record(NULL::vm_creation_retries, "
                "(to_jsonb(r)||jsonb_build_object('request_id',op.request_id,'provision_generation',op.provision_generation,'thread_runtime_generation',op.runtime_generation))::json) changed "
                "WHERE r.request_id=$1",
                source["request_id"],
            )
        elif fault == "exposed":
            await conn.execute(
                "UPDATE threads SET runtime_authority_exposed=true WHERE id=$1",
                ended["id"],
            )
        elif fault == "captured_exposed":
            await conn.execute(
                "UPDATE threads SET runtime_retirement_context=jsonb_set(runtime_retirement_context,'{runtime_authority_exposed}','true') WHERE id=$1",
                ended["id"],
            )
        elif fault == "captured_control":
            await conn.execute(
                "UPDATE threads SET runtime_retirement_context=jsonb_set(runtime_retirement_context,'{control_admission_agent_id}',to_jsonb(gen_random_uuid()::text)) WHERE id=$1",
                ended["id"],
            )
        elif fault == "old_stop":
            await conn.execute(
                "UPDATE vm_resource_thread_cleanup_stops SET stop_evidence=jsonb_set(stop_evidence,'{vm_absent}','false')"
            )
        elif fault == "old_zero":
            await conn.execute(
                "DELETE FROM managed_repository_process_zero_receipts WHERE owner_id=$1 AND scope='vm'",
                ended["id"],
            )
    vm = retirement["context"]["vm"]
    with pytest.raises(asyncpg.CheckViolationError):
        await acquire_pinned_thread_retirement_cleanup_permit(
            VMWorkspaceRecoveryStore(db),
            thread_id=ended["id"],
            identity=VMTeardownIdentity(
                provision_generation=vm["provision_generation"],
                vm_uid=vm["vm_uid"],
                rootdisk_pvc_uid=vm["rootdisk_pvc_uid"],
            ),
            purge_disk=False,
        )
    after = await db.get_thread(str(ended["id"]))
    assert after["runtime_generation"] == current["runtime_generation"]
    assert after["runtime_retirement_token"] is not None
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
        )
        == source
    )
