"""Job resource reservation before the actual creation-retry transport."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from fastapi import HTTPException

from orchestrator.services.vm_creation_retry import VMCreationRetryService
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
from orchestrator.services.vm_creation_disposition_store import VMCreationDispositionStore
from orchestrator.services.vm_resource_job_runtime import installed_job_resource_store
from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
from orchestrator.services.vm_provisioner import VMTeardownIdentity, VMTeardownResult
from orchestrator.services.job_controls import JobControlOperations
from shared.vm_creation_issuance import seal_creation_carrier
from shared.vm_lifecycle_auth import AUTH_FIELD, sign_payload
from shared.vm_resource_admission import ResourceAdmissionError
from shared.vm_resource_effect_node import fresh_resource_effect_node
from shared.worker_queue import claim_worker_batch
from tests.test_vm_resource_whole_store_real_postgres import (
    db as _db_fixture,  # noqa: F401
    whole_schema,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    environment,
    waiter,
)
from tests.test_vm_resource_inventory_real_postgres import publish
from tests.test_vm_resource_policy import whole_launcher_policy
from tests.test_vm_creation_actuation import (
    SECRET as ACTUATION_SECRET,
    setup as _controller_setup,  # noqa: F401
)
from tests.test_vm_workspace_recovery_real_postgres import (
    admission_kwargs,
    recovery_guest_network,
)
from vm_controller import controller as controller_settings
from vm_controller.creation_disposition import CreationDisposer
from shared.vm_creation_disposition import disposition_identity
from shared.vm_network_profile import NETWORK_PROFILE
from shared.workspace_idle_policy import IdleEpisode, RuntimeIdentity, episode_document
from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore
from shared.pinned_session_identity import PinnedJobRecipient

controller_setup = _controller_setup

PROFILED_IMAGE = "registry.example/charged-idle@sha256:" + "a" * 64


async def charged_idle_wait(db, monkeypatch, *, lane="stateless"):
    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
        "VM_NETWORK_PROFILE_IMAGE_ALLOWLIST": PROFILED_IMAGE,
    }.items():
        monkeypatch.setenv(key, value)
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(
        db, policy, inventory, lane=lane,
        request_options={
            "vm_image": PROFILED_IMAGE,
            "network_profile": dict(NETWORK_PROFILE),
        },
    )
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    assert admitted["action"] == "admitted"
    vm_uid, vmi_uid, launcher_uid, pvc_uid = (uuid4() for _ in range(4))
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    authorized = await VMCreationRetryStore(db).authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(claim["job_id"]),
            "provision_generation": str(claim["provision_generation"]),
            "request_digest": claim["request_digest"],
            "controller_configuration_digest": claim["controller_configuration_digest"],
            "expected_pvc_uid": None,
        },
    )
    assert authorized["allowed"]
    await db.execute(
        "UPDATE vm_creation_retries SET state='succeeded',revision=revision+1,"
        "observed_vm_uid=$2,observed_pvc_uid=$3,resolved_at=clock_timestamp(),"
        "claim_token=NULL,claim_expires_at=NULL WHERE request_id=$1",
        retry["request_id"], vm_uid, pvc_uid,
    )
    await db.execute(
        "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),"
        "outcome='completed' WHERE id=(SELECT creation_admission_id "
        "FROM vm_creation_retries WHERE request_id=$1)",
        retry["request_id"],
    )
    await db.execute(
        "UPDATE vm_resource_reservations SET state='active',vm_uid=$2,"
        "vmi_uid=$3,launcher_uid=$4 WHERE id=$1",
        UUID(admitted["reservation_id"]), vm_uid, vmi_uid, launcher_uid,
    )
    execution = await db.fetchrow(
        "SELECT id,revision,generation,created_at+interval '1 hour' AS deadline "
        "FROM srw_execution_specs WHERE work_kind='Job' AND work_id=$1",
        retry["job_id"],
    )
    route_id = uuid4()
    identity = {
        "generation": str(retry["provision_generation"]),
        "vm_uid": str(vm_uid), "vmi_uid": str(vmi_uid),
        "launcher_uid": str(launcher_uid), "pvc_uid": str(pvc_uid),
    }
    episode = IdleEpisode(
        str(uuid4()), 1, "human_message", str(route_id),
        datetime.now(timezone.utc) - timedelta(minutes=16),
        None, 0, RuntimeIdentity(
            "job", str(retry["job_id"]), "vm",
            identity["generation"], identity["vm_uid"],
        ),
    )
    context = json.loads(await db.fetchval(
        "SELECT context FROM jobs WHERE id=$1", retry["job_id"],
    ))
    vm = context["vm"]
    vm.update({
        "status": "ready", "provision_generation": identity["generation"],
        "identity_authenticated": True,
        "identity_provision_generation": identity["generation"],
        "vm_uid": identity["vm_uid"], "vmi_uid": identity["vmi_uid"],
        "active_pod_uid": identity["launcher_uid"],
        "rootdisk_pvc_uid": identity["pvc_uid"],
        "ssh_host": "10.42.0.91", "ssh_port": 22,
        "ssh_ready_source": "provisioner_probe",
        "ssh_host_key_fingerprint": "SHA256:" + "A" * 43,
        "creation_preflight": {
            "version": 1, "request_id": str(retry["request_id"]),
            "job_id": str(retry["job_id"]),
            "request": retry["canonical_request"],
            "request_digest": retry["request_digest"],
            "revision": 1, "attempt": 0, "state": "admitted",
            "execution_id": str(execution["id"]),
            "execution_revision": execution["revision"],
            "execution_generation": execution["generation"],
            "admission_deadline": execution["deadline"].isoformat(),
            "expected_pvc_uid": None,
        },
        "creation_request_id": str(retry["request_id"]),
        "provision_attempts": 0,
        "network_profile_evidence": {
            "profile": NETWORK_PROFILE,
            "provision_generation": identity["generation"],
            "vm_uid": identity["vm_uid"], "pvc_uid": identity["pvc_uid"],
            "vmi_uid": identity["vmi_uid"],
            "launcher_uid": identity["launcher_uid"],
            "guest_boot_id": str(uuid4()),
            "cloud_init_instance_id": "i-charged-idle",
            "cloud_init_cached_instance_id": "i-charged-idle",
            "network_file_sha256": "a" * 64,
            "name_only_dhcp": True,
        },
    })
    context["vm"] = vm
    context.pop("_vm_creation_pending", None)
    await db.execute(
        "UPDATE jobs SET status='waiting_for_reply',execution_lane=$2,"
        "context=$3::jsonb,config_override=$6::jsonb,freeze_data=$4::jsonb,"
        "workspace_idle_revision=1,workspace_idle_episode=$5::jsonb "
        "WHERE id=$1", retry["job_id"], lane, json.dumps(context),
        json.dumps({"route_id": str(route_id)}),
        json.dumps(episode_document(episode)),
        json.dumps({"workspace": {"backend": "vm"}}),
    )
    if lane == "stateless":
        await db.execute(
            "INSERT INTO run_queue(unit_id,unit_kind,state) "
            "VALUES($1,'worker_batch','done') "
            "ON CONFLICT (unit_id) DO UPDATE "
            "SET state='done',leased_by=NULL,leased_until=NULL",
            retry["job_id"],
        )
    return policy, retry, admitted, episode, identity


async def charged_pinned_idle_wait(db, monkeypatch):
    from orchestrator.services.pinned_job_delivery import (
        accept_pinned_report_on_conn, record_pinned_wait_receipt_on_conn,
    )
    from shared.pinned_job_delivery import pinned_job_delivery_proof

    policy, retry, admitted, episode, identity = await charged_idle_wait(
        db, monkeypatch, lane="pinned",
    )
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", "x" * 64)
    owner = retry["job_id"]
    agent_id, process_generation, pod_uid = uuid4(), uuid4(), uuid4()
    await db.execute("DELETE FROM run_queue WHERE unit_id=$1", owner)
    await db.execute(
        "INSERT INTO agents(id,config_name,hostname,status,pod_uid,metadata) "
        "VALUES($1,'worker_base','charged-pinned-agent','ready',$2,$3::jsonb)",
        agent_id, str(pod_uid),
        json.dumps({"dispatch_process_generation": str(process_generation)}),
    )
    await db.execute(
        "UPDATE jobs SET status='created',freeze_data=NULL WHERE id=$1", owner,
    )
    assert await db.claim_job_for_agent(str(owner), str(agent_id))
    recipient = PinnedJobRecipient(
        expected_agent_id=str(agent_id), expected_pod_uid=str(pod_uid),
        expected_process_generation=str(process_generation),
        expected_job_id=str(owner),
    )
    digest = "sha256:" + "a" * 64
    intent = await db.prepare_pinned_job_delivery(
        str(owner), str(agent_id), recipient=recipient,
        projection_digest=digest,
    )
    assert intent is not None
    proof = pinned_job_delivery_proof(
        b"x" * 64, delivery_id=str(intent["id"]), agent_id=str(agent_id),
        process_generation=str(process_generation), pod_uid=str(pod_uid),
        projection_digest=digest,
    )
    route_id = UUID(episode.wait_key)
    async with db.acquire() as conn, conn.transaction():
        accepted = await accept_pinned_report_on_conn(
            conn, job_id=owner, agent_id=agent_id, delivery_id=intent["id"],
            projection_digest=digest, process_generation=str(process_generation),
            pod_uid=str(pod_uid), delivery_proof=proof, source_kind="route",
        )
        assert accepted is not None
        await conn.execute(
            "INSERT INTO job_message_routes(route_id,job_id,thread_id,state,blocking) "
            "VALUES($1,$2,'charged-pinned-route','user_direct',true)",
            route_id, owner,
        )
        await conn.execute(
            "UPDATE jobs SET status='waiting_for_reply',"
            "freeze_data=jsonb_build_object('route_id',$2::text) WHERE id=$1",
            owner, str(route_id),
        )
        assert await record_pinned_wait_receipt_on_conn(
            conn, delivery=accepted, source_kind="route", source_id=route_id,
        ) is not None
    await db.execute(
        "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 hour' "
        "WHERE id=$1", owner,
    )
    return policy, retry, admitted, episode, identity


async def charged_final_review(db, monkeypatch):
    from orchestrator.services.workspace_idle_completion_events import (
        ACCEPTED_IDLE_WAIT_SOURCE_KEY, completion_runtime_evidence,
    )
    from shared.workspace_idle_completion import classify_completion_wait

    policy, retry, admitted, previous, identity = await charged_idle_wait(
        db, monkeypatch,
    )
    command_id, decision_id = uuid4(), "charged-final-review"
    freeze = {
        "job_id": str(retry["job_id"]), "status": "pending_review",
        "freeze_type": "job_complete", "summary": "reviewed",
    }
    context = json.loads(await db.fetchval(
        "SELECT context FROM jobs WHERE id=$1", retry["job_id"],
    ))
    context["completion_decision"] = {"tool_call_id": decision_id}
    await db.execute(
        "UPDATE jobs SET status='pending_review',context=$2::jsonb,"
        "freeze_data=$3::jsonb,resolved_config=$4::jsonb,completion_seq_hwm=1 "
        "WHERE id=$1",
        retry["job_id"], json.dumps(context), json.dumps(freeze),
        json.dumps({"agent": {"autonomy": "review", "verification": {"enabled": False}}}),
    )
    job = dict(await db.fetchrow("SELECT * FROM jobs WHERE id=$1", retry["job_id"]))
    report = {
        "should_stop": True, "goal_achieved": False,
        "error": None, "freeze_data": freeze,
    }
    semantics = classify_completion_wait(
        job=job, report=report, decision_tool_call_id=decision_id,
    )
    runtime = completion_runtime_evidence(job)
    assert semantics and runtime
    await db.execute(
        "INSERT INTO job_completion_commands "
        "(id,job_id,report_seq,client_report_id,payload,payload_digest,"
        "accepted_lease_token,requested_by,state,outcome,finalized_at,deadline_at,"
        "code_version) VALUES($1,$2,1,$3,$4::jsonb,$5,71,'charged-terminal',"
        "'done','{}'::jsonb,clock_timestamp(),"
        "clock_timestamp()+interval '1 hour','charged-terminal')",
        command_id, retry["job_id"], uuid4(),
        json.dumps({**report, ACCEPTED_IDLE_WAIT_SOURCE_KEY: {
            "version": 1, "semantics": semantics, **runtime,
        }}),
        "sha256:" + "a" * 64,
    )
    await db.execute(
        "INSERT INTO completion_effects "
        "(producer_kind,producer_id,scope_id,effect_name,effect_group,state,completed_at) "
        "VALUES('job_completion',$1,$2,'main_status_write','status','done',clock_timestamp())",
        command_id, retry["job_id"],
    )
    episode = IdleEpisode(
        str(uuid4()), previous.revision + 1, "human_review", str(command_id),
        previous.entered_at, None, 0, previous.runtime_identity,
    )
    await db.execute(
        "UPDATE jobs SET workspace_idle_revision=$2,"
        "workspace_idle_episode=$3::jsonb WHERE id=$1",
        retry["job_id"], episode.revision,
        json.dumps(episode_document(episode)),
    )
    return policy, retry, admitted, episode, identity


@pytest.mark.asyncio
async def test_charged_final_approval_marks_teardown_before_terminal_release(
    db, monkeypatch, tmp_path,
):
    from tests.test_vm_idle_terminal_review_real_postgres import controls_for

    _, retry, admitted, _, identity = await charged_final_review(db, monkeypatch)
    controls = await controls_for(db, monkeypatch, tmp_path)
    controls.dependencies.forge.is_initialized = False
    (tmp_path / "output").mkdir()
    result = await controls.approve_job(
        str(retry["job_id"]), user={"id": "reviewer"},
        job=await db.get_job(str(retry["job_id"])), request=None,
    )
    assert result["status"] == "approved"
    operation = await VMIdleLifecycleStore(db).get_open_for_owner(str(retry["job_id"]))
    assert operation is not None and operation["terminal_source_command_id"] is not None
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "teardown"
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)",
        retry["job_id"], identity["generation"],
    )
    physical = {
        "version": 1, "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]), **identity,
        "vm_absent": True, "vmi_absent": True,
        "launcher_absent": True, "retained_pvc": True,
        "controller_authenticated": True,
        "same_generation_replacement": False,
    }
    assert await VMIdleLifecycleStore(db).complete_release(
        str(operation["id"]), evidence=physical,
    )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "released"


@pytest.mark.asyncio
async def test_charged_idle_admission_marks_current_compute_teardown(db, monkeypatch):
    _, retry, admitted, episode, identity = await charged_idle_wait(db, monkeypatch)
    operation = await VMIdleLifecycleStore(db).admit_release(
        str(retry["job_id"]), episode_id=episode.episode_id,
        revision=episode.revision, identity=identity,
    )
    assert operation is not None
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "teardown"


@pytest.mark.asyncio
async def test_charged_idle_admission_rolls_back_if_charge_cannot_enter_teardown(
    db, monkeypatch,
):
    from orchestrator.services.vm_resource_reservation_store import (
        VMResourceReservationStore,
    )

    _, retry, admitted, episode, identity = await charged_idle_wait(db, monkeypatch)

    async def fail_teardown(self, conn, *, retry, operation):
        raise ResourceAdmissionError("test_teardown_unavailable")

    monkeypatch.setattr(
        VMResourceReservationStore, "mark_idle_teardown_on_conn", fail_teardown,
    )
    with pytest.raises(ResourceAdmissionError, match="test_teardown_unavailable"):
        await VMIdleLifecycleStore(db).admit_release(
            str(retry["job_id"]), episode_id=episode.episode_id,
            revision=episode.revision, identity=identity,
        )
    assert await db.fetchval(
        "SELECT count(*) FROM vm_idle_operations WHERE owner_id=$1",
        retry["job_id"],
    ) == 0
    assert await db.fetchval(
        "SELECT context->'vm'->>'status' FROM jobs WHERE id=$1",
        retry["job_id"],
    ) == "ready"
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "active"


@pytest.mark.asyncio
async def test_charged_idle_new_operation_refuses_preexisting_teardown_charge(
    db, monkeypatch,
):
    _, retry, admitted, episode, identity = await charged_idle_wait(db, monkeypatch)
    await db.execute(
        "UPDATE vm_resource_reservations SET state='teardown' WHERE id=$1",
        UUID(admitted["reservation_id"]),
    )
    with pytest.raises(ResourceAdmissionError, match="resource_idle_charge_changed"):
        await VMIdleLifecycleStore(db).admit_release(
            str(retry["job_id"]), episode_id=episode.episode_id,
            revision=episode.revision, identity=identity,
        )
    assert await db.fetchval(
        "SELECT count(*) FROM vm_idle_operations WHERE owner_id=$1",
        retry["job_id"],
    ) == 0
    assert await db.fetchval(
        "SELECT context->'vm'->>'status' FROM jobs WHERE id=$1",
        retry["job_id"],
    ) == "ready"


@pytest.mark.asyncio
async def test_charged_idle_replays_persisted_suspension_with_owed_debit(
    db, monkeypatch,
):
    _, retry, admitted, episode, identity = await charged_idle_wait(db, monkeypatch)
    store = VMIdleLifecycleStore(db)
    operation = await store.admit_release(
        str(retry["job_id"]), episode_id=episode.episode_id,
        revision=episode.revision, identity=identity,
    )
    assert operation is not None
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)",
        retry["job_id"], identity["generation"],
    )
    physical = {
        "version": 1, "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]), **identity,
        "vm_absent": True, "vmi_absent": True,
        "launcher_absent": True, "retained_pvc": True,
        "controller_authenticated": True,
        "same_generation_replacement": False,
    }
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',"
        "'\"suspended\"'::jsonb) WHERE id=$1",
        retry["job_id"],
    )
    await db.execute(
        "UPDATE vm_idle_operations SET phase='suspended',"
        "stop_evidence=$2::jsonb,stop_verified_at=clock_timestamp() WHERE id=$1",
        operation["id"], json.dumps(physical),
    )
    assert await store.complete_release(str(operation["id"]), evidence=physical)
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "released"


@pytest.mark.asyncio
@pytest.mark.parametrize("case", [
    "exact", "flag_off", "missing_process_zero", "wrong_successor",
    "settlement_rollback",
])
async def test_charged_idle_actual_release_debits_only_after_c_stop(
    db, monkeypatch, case,
):
    _, retry, admitted, episode, identity = await charged_idle_wait(db, monkeypatch)
    store = VMIdleLifecycleStore(db)
    operation = await store.admit_release(
        str(retry["job_id"]), episode_id=episode.episode_id,
        revision=episode.revision, identity=identity,
    )
    assert operation is not None
    if case == "flag_off":
        monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
        await db.execute(
            "UPDATE vm_resource_admission_policy SET mode='off',revision=revision+1 "
            "WHERE cluster_id=(SELECT cluster_id FROM vm_resource_reservations "
            "WHERE id=$1)", UUID(admitted["reservation_id"]),
        )
    if case != "missing_process_zero":
        await db.execute(
            "INSERT INTO managed_repository_process_zero_receipts "
            "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
            "VALUES('job',$1,'vm','vm',$2)",
            retry["job_id"], identity["generation"],
        )
    evidence = {
        "version": 1, "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]), **identity,
        "vm_absent": True, "vmi_absent": True,
        "launcher_absent": True, "retained_pvc": True,
        "controller_authenticated": True,
        "same_generation_replacement": False,
    }
    if case == "wrong_successor":
        evidence["launcher_uid"] = str(uuid4())
    if case == "settlement_rollback":
        from orchestrator.services.vm_resource_reservation_store import (
            VMResourceReservationStore,
        )

        async def fail_settlement(self, conn, *, retry, operation):
            raise ResourceAdmissionError("test_resource_settlement_unavailable")

        monkeypatch.setattr(
            VMResourceReservationStore, "release_idle_compute_on_conn",
            fail_settlement,
        )
        with pytest.raises(
            ResourceAdmissionError, match="test_resource_settlement_unavailable"
        ):
            await store.complete_release(str(operation["id"]), evidence=evidence)
    elif case in {"missing_process_zero", "wrong_successor"}:
        assert not await store.complete_release(
            str(operation["id"]), evidence=evidence,
        )
    else:
        assert await store.complete_release(str(operation["id"]), evidence=evidence)
        assert await store.complete_release(str(operation["id"]), evidence=evidence)
    charge = await db.fetchrow(
        "SELECT state,release_evidence FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    )
    projected = await db.fetchval(
        "SELECT context->'vm'->>'status' FROM jobs WHERE id=$1",
        retry["job_id"],
    )
    phase = await db.fetchval(
        "SELECT phase FROM vm_idle_operations WHERE id=$1", operation["id"],
    )
    if case in {"exact", "flag_off"}:
        assert (charge["state"], projected, phase) == (
            "released", "suspended", "suspended",
        )
        assert json.loads(charge["release_evidence"])["operation_id"] == str(
            operation["id"]
        )
    else:
        assert (charge["state"], projected, phase) == (
            "teardown", "suspending", "releasing",
        )


@pytest.mark.asyncio
async def test_charged_pinned_idle_requires_agent_stop_before_compute_debit(
    db, monkeypatch,
):
    _, retry, admitted, episode, identity = await charged_pinned_idle_wait(
        db, monkeypatch,
    )
    store = VMIdleLifecycleStore(db)
    operation = await store.admit_release(
        str(retry["job_id"]), episode_id=episode.episode_id,
        revision=episode.revision, identity=identity,
    )
    assert operation is not None and operation["release_kind"] == "pinned_job"
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "teardown"
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)",
        retry["job_id"], identity["generation"],
    )
    physical = {
        "version": 1, "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]), **identity,
        "vm_absent": True, "vmi_absent": True,
        "launcher_absent": True, "retained_pvc": True,
        "controller_authenticated": True,
        "same_generation_replacement": False,
    }
    assert not await store.complete_release(str(operation["id"]), evidence=physical)
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "teardown"
    claimed = await store.claim(str(operation["id"]), claimant="charged-pinned-stop")
    assert await store.record_pinned_terminal(claimed, claimant="charged-pinned-stop")
    assert await store.record_pinned_stop(
        claimed, claimant="charged-pinned-stop", absence="exact_absent",
    )
    await store.release_claim(
        str(operation["id"]), token=claimed["claim_token"],
        claimant="charged-pinned-stop",
    )
    assert await store.complete_release(str(operation["id"]), evidence=physical)
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "released"

@pytest_asyncio.fixture(scope="module")
async def runtime_schema(whole_schema, pg_dsn):  # noqa: F811
    conn = await asyncpg.connect(pg_dsn)
    try:
        if not await conn.fetchval(
            "SELECT to_regclass('public.vm_resource_recovery_successors') IS NOT NULL"
        ):
            migration = (
                Path(__file__).resolve().parents[1]
                / "src/orchestrator/database/migrations/app"
                / "0276_vm_resource_job_runtime.sql"
            )
            await conn.execute(migration.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(runtime_schema, _db_fixture):  # noqa: F811
    # The base fixture truncates jobs; run_queue is an independent substrate
    # and can retain a previous test's unclaimed unit after that truncation.
    await _db_fixture.execute("TRUNCATE run_queue CASCADE")
    yield _db_fixture


@pytest.mark.asyncio
@pytest.mark.parametrize("entity_type", ["job", "thread"])
async def test_installed_enforcement_refuses_legacy_controller_create_for_any_owner(
    monkeypatch, entity_type,
):
    from vm_controller.controller import VMController

    document = whole_launcher_policy()
    document["policy"].update(shadowEnabled=True, enforcementEnabled=True)
    monkeypatch.setenv("VM_RESOURCE_ADMISSION_CONFIG", json.dumps(document))
    controller = VMController.__new__(VMController)
    with pytest.raises(ValueError, match="durable creation authority"):
        await controller._do_create_serialized({
            "job_id": str(uuid4()), "entity_type": entity_type,
        })


class UnexpectedCreate:
    def __init__(self):
        self.calls = 0

    async def post(self, *_args, **_kwargs):
        self.calls += 1
        raise AssertionError("unreserved controller create")


class SignedPendingCreate:
    def __init__(self, db, request_id):
        self.db = db
        self.request_id = request_id
        self.calls = 0

    async def post(self, path, *, json, timeout):
        assert path == "/vm-creation/create"
        assert timeout == 30.0
        assert await self.db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations "
            "WHERE request_id=$1 AND state='reserved'", self.request_id,
        ) == 1
        self.calls += 1
        result = sign_payload(
            {
                "job_id": json["job_id"],
                "provision_generation": json["provision_generation"],
                "status": "creation_pending",
                "reason": "creation_observation_pending",
            },
            direction="response", operation="creation_retry_create",
            secret=b"test-key",
            correlation_id=json[AUTH_FIELD]["request_id"],
        )
        return SimpleNamespace(
            status_code=200, json=lambda: result, raise_for_status=lambda: None,
        )


@pytest.mark.asyncio
async def test_retry_scan_preserves_waiter_and_never_posts_with_conflicted_inventory(db):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    await db.execute(
        "UPDATE vm_resource_inventory_heads SET observation_conflict=TRUE "
        "WHERE cluster_id=$1 AND policy_digest=$2",
        inventory.cluster_id, inventory.policy_digest,
    )
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    client = UnexpectedCreate()
    service = VMCreationRetryService(
        db,
        SimpleNamespace(_http_client=client, _lifecycle_hmac_secret=b"test-key"),
    )

    await service._replay(claim)

    row = await db.fetchrow(
        "SELECT state,reason,request_digest,provision_generation "
        "FROM vm_creation_retries WHERE request_id=$1", retry["request_id"],
    )
    assert (row["state"], row["reason"]) == ("queued", "capacity_wait")
    assert row["request_digest"] == retry["request_digest"]
    assert row["provision_generation"] == retry["provision_generation"]
    assert await db.fetchval(
        "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
        retry["request_id"],
    ) == 0
    assert client.calls == 0


@pytest.mark.asyncio
async def test_retry_scan_commits_held_reservation_before_signed_controller_create(db):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    client = SignedPendingCreate(db, retry["request_id"])
    service = VMCreationRetryService(
        db,
        SimpleNamespace(_http_client=client, _lifecycle_hmac_secret=b"test-key"),
    )

    await service._replay(claim)

    assert client.calls == 1
    row = await db.fetchrow(
        "SELECT id,revision,node_uid,node_name FROM vm_resource_reservations "
        "WHERE request_id=$1 AND state='reserved'", retry["request_id"],
    )
    assert row is not None and row["revision"] == 1


@pytest.mark.asyncio
async def test_issued_effect_replays_for_observation_after_installed_policy_is_off(db):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    assert admitted["action"] == "admitted"
    await db.execute(
        "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,"
        "effect_kind,carrier_uid,carrier_namespace,carrier_intent) "
        "VALUES($1,$2,1,'rootdisk',$3,'workers','{}'::jsonb)",
        uuid4(), retry["request_id"], uuid4(),
    )
    await db.execute(
        "UPDATE vm_resource_admission_policy SET mode='off',revision=revision+1 "
        "WHERE cluster_id=$1", inventory.cluster_id,
    )
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    client = SignedPendingCreate(db, retry["request_id"])
    service = VMCreationRetryService(
        db,
        SimpleNamespace(_http_client=client, _lifecycle_hmac_secret=b"test-key"),
    )

    await service._replay(claim)

    assert client.calls == 1
    row = await db.fetchrow(
        "SELECT state,reason FROM vm_creation_retries WHERE request_id=$1",
        retry["request_id"],
    )
    assert (row["state"], row["reason"]) == (
        "queued", "creation_observation_pending",
    )


@pytest.mark.asyncio
async def test_controller_authorize_refuses_v3_request_without_held_reservation(db):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    observed = {
        "job_id": str(claim["job_id"]),
        "provision_generation": str(claim["provision_generation"]),
        "request_digest": claim["request_digest"],
        "controller_configuration_digest": claim["controller_configuration_digest"],
        "expected_pvc_uid": None,
    }

    result = await VMCreationRetryStore(db).authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed=observed,
    )

    assert result == {"allowed": False, "reason": "resource_reservation_missing"}
    assert await db.fetchval(
        "SELECT creation_admission_id FROM vm_creation_retries WHERE request_id=$1",
        retry["request_id"],
    ) is None


@pytest.mark.asyncio
async def test_v3_authorize_and_begin_bind_exact_held_resource_grant(db, monkeypatch):
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", "resource-test-secret-at-least-32-bytes")
    policy, inventory, _, demand = await environment(db)
    retry = await waiter(db, policy, inventory)
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    assert admitted["action"] == "admitted"
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    store = VMCreationRetryStore(db)
    observed = {
        "job_id": str(claim["job_id"]),
        "provision_generation": str(claim["provision_generation"]),
        "request_digest": claim["request_digest"],
        "controller_configuration_digest": claim["controller_configuration_digest"],
        "expected_pvc_uid": None,
    }
    authorization = await store.authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]), observed=observed,
    )
    assert authorization["allowed"] is True
    grant = authorization["resource_grant"]
    assert grant["id"] == admitted["reservation_id"]
    assert grant["vector"] == demand.to_six_dict()
    values = {
        "version": 4, "resource_grant": grant,
        "rootdisk_source": {
            "kind": "registry", "image": claim["canonical_request"]["vm_image"],
        },
        "source": "controller_vm_create",
        "admission_id": str(authorization["admission_id"]),
        "reservation_request_id": authorization["request_id"],
        "intent_digest": authorization["intent_digest"],
        "retry_request_id": str(retry["request_id"]),
        "job_id": str(claim["job_id"]),
        "provision_generation": str(claim["provision_generation"]),
        "request_digest": claim["request_digest"],
        "controller_configuration_digest": claim["controller_configuration_digest"],
        "expected_pvc_uid": None, "retained_dv_uid": None,
        "current_dv_uid": None, "current_pvc_uid": None,
        "current_secret_uid": None, "effect_kind": "rootdisk",
        "effect_nonce": str(uuid4()),
        "object_name": "agent-vm-" + str(claim["job_id"]) + "-rootdisk",
    }

    def carrier(intent):
        return seal_creation_carrier(
            intent, namespace="workers", uid=str(uuid4()),
            resource_version="1", secret=b"resource-test-secret-at-least-32-bytes",
        )

    forged = {**values, "resource_grant": {**grant, "id": str(uuid4())}}
    with pytest.raises(VMCreationRetryConflict, match="resource_reservation_changed"):
        await store.begin_effect(
            request_id=str(retry["request_id"]),
            claim_token=str(claim["claim_token"]), carrier=carrier(forged),
        )
    assert await db.fetchval(
        "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
        retry["request_id"],
    ) == 0
    await db.execute(
        "UPDATE vm_resource_inventory_heads SET observation_conflict=TRUE "
        "WHERE cluster_id=$1 AND policy_digest=$2",
        inventory.cluster_id, inventory.policy_digest,
    )
    with pytest.raises(VMCreationRetryConflict, match="inventory_missing"):
        await store.begin_effect(
            request_id=str(retry["request_id"]),
            claim_token=str(claim["claim_token"]), carrier=carrier(values),
        )
    assert await db.fetchval(
        "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
        retry["request_id"],
    ) == 0
    await db.execute(
        "UPDATE vm_resource_inventory_heads SET observation_conflict=FALSE "
        "WHERE cluster_id=$1 AND policy_digest=$2",
        inventory.cluster_id, inventory.policy_digest,
    )
    result = await store.begin_effect(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]), carrier=carrier(values),
    )
    assert result["actuation_allowed"] is True


@pytest.mark.asyncio
async def test_fresh_controller_inventory_rechecks_selected_node_before_effect(db):
    policy, inventory, snapshot, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    authorization = await VMCreationRetryStore(db).authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(claim["job_id"]),
            "provision_generation": str(claim["provision_generation"]),
            "request_digest": claim["request_digest"],
            "controller_configuration_digest": claim["controller_configuration_digest"],
            "expected_pvc_uid": None,
        },
    )
    assert authorization["resource_grant"]["id"] == admitted["reservation_id"]
    collector = SimpleNamespace(collect=AsyncMock(return_value=snapshot))
    controller = SimpleNamespace(resource_inventory_collector=collector)
    row = {
        "controller_configuration": claim["controller_configuration"],
        "expected_pvc_uid": None,
    }
    grant = authorization["resource_grant"]
    await fresh_resource_effect_node(controller, row, grant)
    for change in ("uid", "ready", "hostname", "architecture", "storage_topology"):
        stale = deepcopy(snapshot)
        if change == "uid":
            stale["nodes"][0]["uid"] = str(uuid4())
        elif change == "ready":
            stale["nodes"][0]["ready"] = False
        elif change == "hostname":
            stale["nodes"][0]["labels"]["kubernetes.io/hostname"] = "other"
        elif change == "architecture":
            stale["nodes"][0]["labels"]["kubernetes.io/arch"] = "arm64"
        else:
            stale["storage_classes"][0]["allowed_topology"] = {
                "nodeSelectorTerms": [{"matchExpressions": [{
                    "key": "kubernetes.io/hostname", "operator": "In",
                    "values": ["other"],
                }]}],
            }
        collector.collect.return_value = stale
        with pytest.raises(ResourceAdmissionError, match="resource_node_changed"):
            await fresh_resource_effect_node(controller, row, grant)


@pytest.mark.asyncio
@pytest.mark.parametrize("initial_overbudget", [False, True])
async def test_ready_binding_uses_exact_pod_and_keeps_overreserve_high_water(db, initial_overbudget):
    policy, inventory, original, demand = await environment(db)
    retry = await waiter(db, policy, inventory)
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    vm_uid, vmi_uid, launcher_uid, pvc_uid = (str(uuid4()) for _ in range(4))
    await VMCreationRetryStore(db).authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(claim["job_id"]),
            "provision_generation": str(claim["provision_generation"]),
            "request_digest": claim["request_digest"],
            "controller_configuration_digest": claim["controller_configuration_digest"],
            "expected_pvc_uid": None,
        },
    )
    await db.execute(
        "UPDATE vm_creation_retries SET state='succeeded',revision=revision+1,"
        "observed_vm_uid=$2,observed_pvc_uid=$3,resolved_at=clock_timestamp(),"
        "claim_token=NULL,claim_expires_at=NULL WHERE request_id=$1",
        retry["request_id"], vm_uid, pvc_uid,
    )
    job_id, generation = str(claim["job_id"]), str(claim["provision_generation"])
    vm = {
        "vm_uid": vm_uid, "vmi_uid": vmi_uid,
        "active_pod_uid": launcher_uid, "rootdisk_pvc_uid": pvc_uid,
    }
    node_uid, node_name = admitted["node_uid"], admitted["node_name"]
    sample = deepcopy(original)
    sample["vms"] = [{
        "uid": vm_uid, "name": "agent-vm-" + job_id,
        "owner_kind": "job", "owner_id": job_id,
        "provision_generation": generation, "deleting": False,
    }]
    sample["vmis"] = [{
        "uid": vmi_uid, "name": "agent-vm-" + job_id,
        "vm_uid": vm_uid, "node_uid": node_uid,
        "node_name": node_name, "phase": "Running", "deleting": False,
    }]
    sample["pods"] = [{
        "uid": launcher_uid, "namespace": "workers", "name": "virt-launcher-test",
        "node_uid": node_uid, "node_name": node_name,
        "terminal": False, "deleting": False,
        "requests": demand.to_six_dict(), "vmi_uid": vmi_uid,
        "reservation_id": str(uuid4()),
        "provision_generation": generation,
    }]

    async def publish_sample(document):
        value = deepcopy(document)
        value["snapshot_id"] = str(uuid4())
        value["sequence"] += 1
        value["started_at"] = value["finished_at"] = datetime.now(
            timezone.utc
        ).isoformat()
        await publish(inventory, value)

    async def bind():
        async with db.acquire() as conn, conn.transaction():
            await conn.fetchrow(
                "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", claim["job_id"],
            )
            source = await conn.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
                retry["request_id"],
            )
            installed = await installed_job_resource_store(
                conn, db, source["controller_configuration"], fresh=False,
            )
            return await installed.bind_ready_on_conn(
                conn, retry=source, vm=vm, job_id=job_id, generation=generation,
            )

    await publish_sample(sample)
    assert not await bind()
    assert await db.fetchval(
        "SELECT vm_uid FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) is None
    sample["pods"][0]["reservation_id"] = admitted["reservation_id"]
    if initial_overbudget:
        sample["pods"][0]["requests"]["cpu_millicores"] += 1
    await publish_sample(sample)
    assert await bind() is (not initial_overbudget)
    row = await db.fetchrow(
        "SELECT state,vm_uid,vmi_uid,launcher_uid,observed_cpu_millicores "
        "FROM vm_resource_reservations WHERE id=$1", admitted["reservation_id"],
    )
    assert (row["state"], str(row["vm_uid"]), str(row["vmi_uid"]),
            str(row["launcher_uid"]), row["observed_cpu_millicores"]) == (
        "reserved" if initial_overbudget else "active", vm_uid, vmi_uid, launcher_uid,
        demand.cpu_millicores + int(initial_overbudget),
    )
    sample["pods"][0]["requests"]["cpu_millicores"] = demand.cpu_millicores + 1
    await publish_sample(sample)
    assert not await bind()
    assert await db.fetchval(
        "SELECT observed_cpu_millicores FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == demand.cpu_millicores + 1
    from orchestrator.services.vm_resource_capacity import vm_capacity_snapshot

    capacity = await vm_capacity_snapshot(db)
    cluster = next(c for c in capacity["clusters"] if c["cluster_id"] == inventory.cluster_id)
    expected = {**demand.to_six_dict(), "cpu_millicores": demand.cpu_millicores + 1}
    category = "bound_reserved" if initial_overbudget else "active"
    assert cluster["totals"][category] == cluster["held"][category] == expected
    assert cluster["totals"]["unbound"] == dict.fromkeys(expected, 0)
    assert cluster["totals"]["external"] == dict.fromkeys(expected, 0)
    sample["pods"][0]["requests"]["cpu_millicores"] = demand.cpu_millicores
    await publish_sample(sample)
    assert not await bind()


@pytest.mark.asyncio
async def test_never_issued_cancel_releases_held_reservation_in_same_settlement(db):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    assert admitted["action"] == "admitted"
    store = VMCreationRetryStore(db)
    assert await db.linearize_pinned_cancel(
        str(retry["job_id"]), expected_status="paused"
    )

    result = await store.settle_never_issued(request_id=str(retry["request_id"]))

    assert result == {"settled": True, "disposition": "never_issued"}
    row = await db.fetchrow(
        "SELECT state,release_evidence FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    )
    assert row["state"] == "released"
    assert json.loads(row["release_evidence"])["kind"] == "never_vm_issued"
    assert await db.fetchval(
        "SELECT state FROM vm_resource_waiters WHERE request_id=$1",
        retry["request_id"],
    ) == "released"


@pytest.mark.asyncio
async def test_rejected_partial_effect_disposition_releases_only_after_real_completion(
    db, monkeypatch, controller_setup,
):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    assert admitted["action"] == "admitted"
    store = VMCreationRetryStore(db)
    claim = (await store.claim_due(limit=1))[0]
    authorized = await store.authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(claim["job_id"]),
            "provision_generation": str(claim["provision_generation"]),
            "request_digest": claim["request_digest"],
            "controller_configuration_digest": claim["controller_configuration_digest"],
            "expected_pvc_uid": None,
        },
    )
    assert authorized["allowed"] is True
    intent = {
        "version": 4, "resource_grant": authorized["resource_grant"],
        "rootdisk_source": {
            "kind": "registry", "image": claim["canonical_request"]["vm_image"],
        },
        "source": "controller_vm_create",
        "admission_id": str(authorized["admission_id"]),
        "reservation_request_id": authorized["request_id"],
        "intent_digest": authorized["intent_digest"],
        "retry_request_id": str(retry["request_id"]),
        "job_id": str(claim["job_id"]),
        "provision_generation": str(claim["provision_generation"]),
        "request_digest": claim["request_digest"],
        "controller_configuration_digest": claim["controller_configuration_digest"],
        "expected_pvc_uid": None, "retained_dv_uid": None,
        "current_dv_uid": None, "current_pvc_uid": None,
        "current_secret_uid": None, "effect_kind": "rootdisk",
        "effect_nonce": str(uuid4()),
        "object_name": "agent-vm-" + str(claim["job_id"]) + "-rootdisk",
    }
    carrier = seal_creation_carrier(
        intent, namespace="workers", uid=str(uuid4()),
        resource_version="1", secret=ACTUATION_SECRET,
    )
    assert (await store.begin_effect(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]), carrier=carrier,
    ))["actuation_allowed"]
    assert await store.observe_effect(
        request_id=str(retry["request_id"]), carrier=carrier,
        observation={
            "outcome": "rejected",
            "api_status": {
                "kind": "Status", "apiVersion": "v1", "status": "Failure",
                "reason": "Invalid", "code": 422,
            },
        },
    ) == {"recorded": True, "effect_state": "rejected"}
    assert await db.linearize_pinned_cancel(
        str(retry["job_id"]), expected_status="paused"
    )
    service = VMCreationDispositionStore(store)
    frozen = await service.freeze(request_id=str(retry["request_id"]), carrier=carrier)
    assert frozen["frozen"] is True
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "reserved"
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError, match="never-issued release unproven"):
            async with conn.transaction():
                await conn.execute(
                    "UPDATE vm_resource_reservations SET state='released',"
                    "released_at=clock_timestamp(),release_evidence=$2::jsonb "
                    "WHERE id=$1", UUID(admitted["reservation_id"]),
                    json.dumps({
                        "kind": "never_vm_issued",
                        "request_id": str(retry["request_id"]),
                        "job_id": str(retry["job_id"]),
                        "provision_generation": str(retry["provision_generation"]),
                    }),
                )
    ctrl, api, _, _ = controller_setup
    monkeypatch.setattr(controller_settings, "VM_NAMESPACE", "workers")
    api.objects["Lease", carrier["metadata"]["name"]] = carrier
    ctrl.k8s_client.list_namespaced_custom_object = lambda **_: {
        "metadata": {"resourceVersion": "1"}, "items": [],
    }
    ctrl.core_api.list_namespaced_pod = lambda **_: {
        "metadata": {"resourceVersion": "1"}, "items": [],
    }

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[-1].replace("-", "_")
        return await getattr(store, method)(**body)

    ctrl._workspace_cleanup_authority_request = authority
    outcome = await CreationDisposer(ctrl).run(disposition_identity(retry))
    assert outcome["status"] == "creation_disposed"
    row = await db.fetchrow(
        "SELECT state,release_evidence FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    )
    assert row["state"] == "released"
    assert json.loads(row["release_evidence"])["disposition_id"] == (
        frozen["disposition"]["disposition_id"]
    )


@pytest.mark.asyncio
async def test_idle_charge_requires_persisted_exact_operation_before_teardown(db):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    vm_uid, vmi_uid, launcher_uid, pvc_uid = (uuid4() for _ in range(4))
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    authorized = await VMCreationRetryStore(db).authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(claim["job_id"]),
            "provision_generation": str(claim["provision_generation"]),
            "request_digest": claim["request_digest"],
            "controller_configuration_digest": claim["controller_configuration_digest"],
            "expected_pvc_uid": None,
        },
    )
    assert authorized["allowed"]
    await db.execute(
        "UPDATE vm_creation_retries SET state='succeeded',revision=revision+1,"
        "observed_vm_uid=$2,observed_pvc_uid=$3,resolved_at=clock_timestamp(),"
        "claim_token=NULL,claim_expires_at=NULL WHERE request_id=$1",
        retry["request_id"], vm_uid, pvc_uid,
    )
    await db.execute(
        "UPDATE vm_resource_reservations SET state='active',vm_uid=$2,"
        "vmi_uid=$3,launcher_uid=$4 WHERE id=$1",
        UUID(admitted["reservation_id"]), vm_uid, vmi_uid, launcher_uid,
    )
    fake = {
        "id": uuid4(), "owner_kind": "job", "owner_id": retry["job_id"],
        "provision_generation": retry["provision_generation"],
        "vm_uid": vm_uid, "vmi_uid": vmi_uid,
        "launcher_uid": launcher_uid, "pvc_uid": pvc_uid,
        "phase": "releasing", "stop_evidence": None, "stop_verified_at": None,
    }
    async with db.acquire() as conn:
        with pytest.raises(ResourceAdmissionError, match="resource_idle_operation_changed"):
            async with conn.transaction():
                source = await conn.fetchrow(
                    "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
                    retry["request_id"],
                )
                await policy.mark_idle_teardown_on_conn(
                    conn, retry=source, operation=fake,
                )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "active"
    operation = await db.fetchrow(
        "INSERT INTO vm_idle_operations "
        "(owner_kind,owner_id,phase,episode_id,episode_revision,"
        "provision_generation,vm_uid,vmi_uid,launcher_uid,pvc_uid,retained_kind) "
        "VALUES('job',$1,'releasing',$2,1,$3,$4,$5,$6,$7,'rootdisk') RETURNING *",
        retry["job_id"], uuid4(), retry["provision_generation"],
        vm_uid, vmi_uid, launcher_uid, pvc_uid,
    )
    async with db.acquire() as conn, conn.transaction():
        source = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
            retry["request_id"],
        )
        assert await policy.mark_idle_teardown_on_conn(
            conn, retry=source, operation=operation,
        )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "teardown"
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError, match="physical release proof changed"):
            await conn.execute(
                "UPDATE vm_resource_reservations SET state='released',"
                "released_at=clock_timestamp(),release_evidence=$2::jsonb "
                "WHERE id=$1", UUID(admitted["reservation_id"]),
                json.dumps({
                    "kind": "exact_compute_absent",
                    "operation_id": str(operation["id"]),
                    "job_id": str(retry["job_id"]),
                    "provision_generation": str(retry["provision_generation"]),
                    "vm_uid": str(vm_uid), "vmi_uid": str(vmi_uid),
                    "launcher_uid": str(launcher_uid), "pvc_uid": str(pvc_uid),
                    "stop_evidence_digest": "sha256:" + "0" * 64,
                }),
            )
    async with db.acquire() as conn:
        with pytest.raises(ResourceAdmissionError, match="resource_physical_release_unproven"):
            async with conn.transaction():
                source = await conn.fetchrow(
                    "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
                    retry["request_id"],
                )
                await policy.release_idle_compute_on_conn(
                    conn, retry=source, operation=operation,
                )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "teardown"
    evidence = {
        "version": 1, "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]),
        "generation": str(retry["provision_generation"]),
        "vm_uid": str(vm_uid), "vmi_uid": str(vmi_uid),
        "launcher_uid": str(launcher_uid), "pvc_uid": str(pvc_uid),
        "vm_absent": True, "vmi_absent": True, "launcher_absent": True,
        "retained_pvc": True, "controller_authenticated": True,
        "same_generation_replacement": False,
    }
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)",
        retry["job_id"], str(retry["provision_generation"]),
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_build_object('vm',$2::jsonb) WHERE id=$1",
        retry["job_id"], json.dumps({
            "status": "suspended", "_suspend_remote_io_closed": str(operation["id"]),
            "provision_generation": str(retry["provision_generation"]),
            "vm_uid": str(vm_uid), "rootdisk_pvc_uid": str(pvc_uid),
        }),
    )
    updated = await db.fetchrow(
        "UPDATE vm_idle_operations SET phase='suspended',"
        "stop_evidence=$2::jsonb,stop_verified_at=clock_timestamp() "
        "WHERE id=$1 RETURNING *", operation["id"], json.dumps(evidence),
    )
    async with db.acquire() as conn, conn.transaction():
        source = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
            retry["request_id"],
        )
        await conn.fetchrow(
            "SELECT * FROM vm_idle_operations WHERE id=$1 FOR UPDATE",
            operation["id"],
        )
        await policy.release_idle_compute_on_conn(
            conn, retry=source, operation=updated,
        )
    final = await db.fetchrow(
        "SELECT state,release_evidence FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    )
    assert final["state"] == "released"
    assert json.loads(final["release_evidence"])["operation_id"] == str(operation["id"])


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_case", [
    "exact", "flag_off", "partial_unknown", "wrong_successor", "logical_delete",
    "lost_response", "missing_process_zero", "native_release_without_receipt",
])
async def test_public_delete_settles_adopted_charge_only_with_exact_cleanup_stop(
    db, stop_case,
):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    vm_uid, vmi_uid, launcher_uid, pvc_uid = (uuid4() for _ in range(4))
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    authorized = await VMCreationRetryStore(db).authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(claim["job_id"]),
            "provision_generation": str(claim["provision_generation"]),
            "request_digest": claim["request_digest"],
            "controller_configuration_digest": claim["controller_configuration_digest"],
            "expected_pvc_uid": None,
        },
    )
    assert authorized["allowed"]
    await db.execute(
        "UPDATE vm_creation_retries SET state='succeeded',revision=revision+1,"
        "observed_vm_uid=$2,observed_pvc_uid=$3,resolved_at=clock_timestamp(),"
        "claim_token=NULL,claim_expires_at=NULL WHERE request_id=$1",
        retry["request_id"], vm_uid, pvc_uid,
    )
    await db.execute(
        "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),"
        "outcome='adopted' WHERE id=$1", authorized["admission_id"],
    )
    await db.execute(
        "UPDATE vm_resource_reservations SET state='active',vm_uid=$2,"
        "vmi_uid=$3,launcher_uid=$4 WHERE id=$1",
        UUID(admitted["reservation_id"]), vm_uid, vmi_uid, launcher_uid,
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_build_object('vm',$2::jsonb) WHERE id=$1",
        retry["job_id"], json.dumps({
            "provision_generation": str(retry["provision_generation"]),
            "vm_uid": str(vm_uid), "vmi_uid": str(vmi_uid),
            "active_pod_uid": str(launcher_uid),
            "rootdisk_pvc_uid": str(pvc_uid), "status": "ready",
        }),
    )
    if stop_case != "missing_process_zero":
        await db.execute(
            "INSERT INTO managed_repository_process_zero_receipts "
            "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
            "VALUES('job',$1,'vm','vm',$2)",
            retry["job_id"], str(retry["provision_generation"]),
        )

    identity = VMTeardownIdentity(
        provision_generation=str(retry["provision_generation"]),
        vm_uid=str(vm_uid), rootdisk_pvc_uid=str(pvc_uid),
    )
    proof = {
        "version": 1, "kind": "vm_cleanup_physical_stop",
        "job_id": str(retry["job_id"]),
        "provision_generation": str(retry["provision_generation"]),
        "vm_uid": str(vm_uid), "vmi_uid": str(vmi_uid),
        "launcher_uid": str(launcher_uid), "pvc_uid": str(pvc_uid),
        "vm_absent": True, "vmi_absent": True, "launcher_absent": True,
        "same_generation_replacement": False,
        "pvc_disposition": "purged", "controller_authenticated": True,
    }
    if stop_case == "flag_off":
        await db.execute(
            "UPDATE vm_resource_admission_policy SET mode='off' WHERE cluster_id=$1",
            inventory.cluster_id,
        )
    if stop_case == "wrong_successor":
        proof["vmi_uid"] = str(uuid4())
    if stop_case == "native_release_without_receipt":
        await db.execute(
            "UPDATE vm_resource_reservations SET state='teardown' WHERE id=$1",
            UUID(admitted["reservation_id"]),
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(
                "UPDATE vm_resource_reservations SET state='released',"
                "release_evidence=$2::jsonb WHERE id=$1",
                UUID(admitted["reservation_id"]), json.dumps({
                    "kind": "exact_cleanup_compute_absent",
                    "cleanup_admission_id": str(uuid4()),
                }),
            )
    provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(return_value=identity),
        release_vm_captured=AsyncMock(return_value=(
            VMTeardownResult("retry_pending", False) if stop_case == "logical_delete"
            else VMTeardownResult("completed", True)
        )),
        attest_vm_cleanup_stop=AsyncMock(
            side_effect=[None, proof] if stop_case == "lost_response" else None,
            return_value=None if stop_case == "partial_unknown" else proof,
        ),
    )
    controls = JobControlOperations(SimpleNamespace(
        vm_provisioner=provisioner,
        recovery_store=VMWorkspaceRecoveryStore(db),
    ))
    if stop_case in {
        "partial_unknown", "wrong_successor", "logical_delete",
        "lost_response", "missing_process_zero",
    }:
        with pytest.raises(HTTPException) as refused:
            await controls.delete_vm(str(retry["job_id"]))
        assert refused.value.status_code == (
            500 if stop_case == "logical_delete" else 409
        )
    else:
        assert (await controls.delete_vm(str(retry["job_id"])))["status"] == "deleting"
    charge = await db.fetchrow(
        "SELECT state,release_evidence FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    )
    if stop_case in {
        "partial_unknown", "wrong_successor", "logical_delete",
        "lost_response", "missing_process_zero",
    }:
        assert charge["state"] == "teardown"
        assert charge["release_evidence"] is None
        assert await db.fetchval(
            "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions "
            "WHERE source='public_vm_delete' AND owner_id=$1",
            retry["job_id"],
        ) is True
        assert provisioner.attest_vm_cleanup_stop.await_count == (
            0 if stop_case == "logical_delete" else 1
        )
        if stop_case != "lost_response":
            return
        # The controller delete already completed; a second public request
        # reuses its durable permit and settles from a fresh exact probe.
        assert (await controls.delete_vm(str(retry["job_id"]))) == {
            "status": "deleting", "job_id": str(retry["job_id"]),
        }
        assert provisioner.release_vm_captured.await_count == 2
        assert (await controls.delete_vm(str(retry["job_id"]))) == {
            "status": "deleting", "job_id": str(retry["job_id"]),
        }
        assert provisioner.attest_vm_cleanup_stop.await_count == 2
        charge = await db.fetchrow(
            "SELECT state,release_evidence FROM vm_resource_reservations WHERE id=$1",
            UUID(admitted["reservation_id"]),
        )
    assert charge["state"] == "released"
    assert json.loads(charge["release_evidence"])["kind"] == "exact_cleanup_compute_absent"
    assert await db.fetchval(
        "SELECT count(*) FROM vm_resource_cleanup_stop_receipts WHERE reservation_id=$1",
        UUID(admitted["reservation_id"]),
    ) == 1
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_resource_cleanup_stop_receipts SET "
            "stop_evidence='{}'::jsonb WHERE reservation_id=$1",
            UUID(admitted["reservation_id"]),
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "DELETE FROM vm_resource_cleanup_stop_receipts WHERE reservation_id=$1",
            UUID(admitted["reservation_id"]),
        )


@pytest.mark.asyncio
async def test_retry_service_runs_bounded_real_waiter_maintenance(db, monkeypatch):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    monkeypatch.setenv(
        "VM_RESOURCE_ADMISSION_CONFIG", json.dumps(policy.policy_document),
    )
    await db.execute(
        "UPDATE vm_resource_waiters SET state='parked',reason='worker_lease_active' "
        "WHERE request_id=$1", retry["request_id"],
    )
    before = await db.fetchval(
        "SELECT enqueued_at FROM vm_resource_waiters WHERE request_id=$1",
        retry["request_id"],
    )
    service = VMCreationRetryService(db, SimpleNamespace())
    service.preflight.settle_cancelled = AsyncMock()
    service.preflight.claim_due = AsyncMock(return_value=[])
    service.store.claim_due = AsyncMock(return_value=[])

    await service.reconcile_once()

    row = await db.fetchrow(
        "SELECT state,reason,enqueued_at FROM vm_resource_waiters WHERE request_id=$1",
        retry["request_id"],
    )
    assert (row["state"], row["reason"], row["enqueued_at"]) == (
        "waiting", None, before,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "durable_mode", ["shadow", "off", "missing", "unconfigured", "invalid_config"],
)
async def test_unready_resource_mode_does_not_abort_unrelated_retry_scans(
    db, monkeypatch, durable_mode,
):
    policy, inventory, _, _ = await environment(db)
    monkeypatch.setenv(
        "VM_RESOURCE_ADMISSION_CONFIG", json.dumps(policy.policy_document),
    )
    if durable_mode == "missing":
        await db.execute(
            "DELETE FROM vm_resource_admission_policy WHERE cluster_id=$1",
            inventory.cluster_id,
        )
    elif durable_mode in {"shadow", "off"}:
        await db.execute(
            "UPDATE vm_resource_admission_policy SET mode=$2,revision=revision+1 "
            "WHERE cluster_id=$1", inventory.cluster_id, durable_mode,
        )
    elif durable_mode == "unconfigured":
        monkeypatch.delenv("VM_RESOURCE_ADMISSION_CONFIG")
    else:
        monkeypatch.setenv("VM_RESOURCE_ADMISSION_CONFIG", "{")
    service = VMCreationRetryService(db, SimpleNamespace())
    service.preflight.settle_cancelled = AsyncMock()
    service.preflight.claim_due = AsyncMock(return_value=[])
    service.store.claim_due = AsyncMock(return_value=[])

    await service.reconcile_once()

    service.preflight.claim_due.assert_awaited_once()
    service.store.claim_due.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("durable_mode", ["enforce", "drain"])
async def test_changed_installed_policy_holds_v3_create_but_scans_retry(
    db, monkeypatch, durable_mode,
):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    if durable_mode == "drain":
        await db.execute(
            "UPDATE vm_resource_admission_policy SET mode='drain',revision=revision+1 "
            "WHERE cluster_id=$1", inventory.cluster_id,
        )
    changed = deepcopy(policy.policy_document)
    changed["policy"]["ownerBudget"]["cpuMillicores"] += 1
    monkeypatch.setenv("VM_RESOURCE_ADMISSION_CONFIG", json.dumps(changed))
    client = UnexpectedCreate()
    service = VMCreationRetryService(
        db,
        SimpleNamespace(_http_client=client, _lifecycle_hmac_secret=b"test-key"),
    )

    await service.reconcile_once()

    current = await db.fetchrow(
        "SELECT state,reason FROM vm_creation_retries WHERE request_id=$1",
        retry["request_id"],
    )
    assert tuple(current) == ("attention", "vm_creation_retry_blocked")
    assert client.calls == 0
    assert await db.fetchval(
        "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
        retry["request_id"],
    ) == 0


@pytest.mark.asyncio
async def test_enforcement_transition_refuses_unclassified_live_vm(db):
    from orchestrator.services.vm_resource_policy_lifecycle_store import (
        VMResourcePolicyLifecycleStore,
    )
    from shared.vm_resource_policy import validate_enforcement_resource_policy

    store, inventory, original, _ = await environment(db)
    await db.execute(
        "UPDATE vm_resource_admission_policy SET mode='shadow' WHERE cluster_id=$1",
        inventory.cluster_id,
    )
    lifecycle = VMResourcePolicyLifecycleStore(
        db, snapshot=validate_enforcement_resource_policy(store.policy_document),
    )
    current = await db.fetchrow(
        "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1",
        inventory.cluster_id,
    )
    shadow = lifecycle._receipt(current)
    marked = deepcopy(original)
    marked["vms"] = [{
        "uid": str(uuid4()), "name": "agent-vm-legacy",
        "owner_kind": "job", "owner_id": str(uuid4()),
        "provision_generation": str(uuid4()), "deleting": False,
    }]
    marked["snapshot_id"] = str(uuid4())
    marked["sequence"] += 1
    marked["started_at"] = marked["finished_at"] = datetime.now(
        timezone.utc
    ).isoformat()
    await publish(inventory, marked)

    with pytest.raises(ResourceAdmissionError, match="legacy_occupancy_unclassified"):
        await lifecycle.activate_enforce(expected=shadow)
    assert await db.fetchval(
        "SELECT mode FROM vm_resource_admission_policy WHERE cluster_id=$1",
        inventory.cluster_id,
    ) == "shadow"


@pytest.mark.asyncio
async def test_enforcement_transition_accepts_fresh_empty_srw_inventory(db):
    from orchestrator.services.vm_resource_policy_lifecycle_store import (
        VMResourcePolicyLifecycleStore,
    )
    from shared.vm_resource_policy import validate_enforcement_resource_policy

    store, inventory, _, _ = await environment(db)
    await db.execute(
        "UPDATE vm_resource_admission_policy SET mode='shadow' WHERE cluster_id=$1",
        inventory.cluster_id,
    )
    lifecycle = VMResourcePolicyLifecycleStore(
        db, snapshot=validate_enforcement_resource_policy(store.policy_document),
    )
    shadow = lifecycle._receipt(await db.fetchrow(
        "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1",
        inventory.cluster_id,
    ))
    enforced = await lifecycle.activate_enforce(expected=shadow)
    assert (enforced.mode, enforced.revision) == ("enforce", shadow.revision + 1)


@pytest.mark.asyncio
async def test_drain_cannot_finish_off_until_held_charge_is_released(db):
    from orchestrator.services.vm_resource_policy_lifecycle_store import (
        VMResourcePolicyLifecycleStore,
    )
    from shared.vm_resource_policy import validate_enforcement_resource_policy

    store, inventory, _, _ = await environment(db)
    lifecycle = VMResourcePolicyLifecycleStore(
        db, snapshot=validate_enforcement_resource_policy(store.policy_document),
    )
    enforce = lifecycle._receipt(await db.fetchrow(
        "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1",
        inventory.cluster_id,
    ))
    retry = await waiter(db, store, inventory)
    await store.admit(request_id=str(retry["request_id"]))
    drained = await lifecycle.begin_drain(expected=enforce)
    with pytest.raises(ResourceAdmissionError, match="resource_charge_unresolved"):
        await lifecycle.finalize_off(expected=drained)
    assert await db.linearize_pinned_cancel(
        str(retry["job_id"]), expected_status="paused"
    )
    await VMCreationRetryStore(db).settle_never_issued(
        request_id=str(retry["request_id"]),
    )
    off = await lifecycle.finalize_off(expected=drained)
    assert (off.mode, off.revision) == ("off", drained.revision + 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("teardown_before_release", [False, True])
async def test_genuine_recovery_release_appends_exact_charged_successor(
    db, teardown_before_release,
):
    from shared.workspace_recovery import WorkspaceRecoveryCode

    policy, inventory, original, demand = await environment(
        db, installation_count=2, owner_count=2,
    )
    retry = await waiter(db, policy, inventory, lane="stateless")
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    job_id, generation = retry["job_id"], retry["provision_generation"]
    vm_uid, vmi_uid, launcher_uid, pvc_uid = (uuid4() for _ in range(4))
    successor_vmi, successor_launcher = uuid4(), uuid4()
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    authorized = await VMCreationRetryStore(db).authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(job_id),
            "provision_generation": str(generation),
            "request_digest": claim["request_digest"],
            "controller_configuration_digest": claim["controller_configuration_digest"],
            "expected_pvc_uid": None,
        },
    )
    assert authorized["allowed"]
    await db.execute(
        "UPDATE vm_creation_retries SET state='succeeded',revision=revision+1,"
        "observed_vm_uid=$2,observed_pvc_uid=$3,resolved_at=clock_timestamp(),"
        "claim_token=NULL,claim_expires_at=NULL "
        "WHERE request_id=$1",
        retry["request_id"], vm_uid, pvc_uid,
    )
    # The predecessor creation's adoption has settled before a worker may
    # report recovery. Its full effect receipts are tested by the creation
    # suite; this fixture starts at the already-created runtime boundary.
    await db.execute(
        "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),"
        "outcome='adopted' WHERE id=$1",
        authorized["admission_id"],
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_build_object('vm',$2::jsonb) "
        "WHERE id=$1",
        job_id,
        json.dumps({
            "provision_generation": str(generation), "vm_uid": str(vm_uid),
            "vmi_uid": str(vmi_uid), "active_pod_uid": str(launcher_uid),
            "rootdisk_pvc_uid": str(pvc_uid), "status": "ready",
        }),
    )
    sample = deepcopy(original)
    sample["vms"] = [{
        "uid": str(vm_uid), "name": "agent-vm-" + str(job_id),
        "owner_kind": "job", "owner_id": str(job_id),
        "provision_generation": str(generation), "deleting": False,
    }]
    sample["vmis"] = [{
        "uid": str(vmi_uid), "name": "agent-vm-" + str(job_id),
        "vm_uid": str(vm_uid), "node_uid": admitted["node_uid"],
        "node_name": admitted["node_name"], "phase": "Running",
        "deleting": False,
    }]
    sample["pods"] = [{
        "uid": str(launcher_uid), "namespace": "workers", "name": "virt-launcher-old",
        "node_uid": admitted["node_uid"], "node_name": admitted["node_name"],
        "terminal": False, "deleting": False,
        "requests": demand.to_six_dict(), "vmi_uid": str(vmi_uid),
        "reservation_id": admitted["reservation_id"],
        "provision_generation": str(generation),
    }]
    sample["snapshot_id"] = str(uuid4())
    sample["sequence"] += 1
    sample["started_at"] = sample["finished_at"] = datetime.now(
        timezone.utc
    ).isoformat()
    await publish(inventory, sample)
    async with db.acquire() as conn, conn.transaction():
        await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job_id)
        source = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
            retry["request_id"],
        )
        assert await policy.bind_ready_on_conn(
            conn, retry=source,
            vm={
                "vm_uid": str(vm_uid), "vmi_uid": str(vmi_uid),
                "active_pod_uid": str(launcher_uid),
                "rootdisk_pvc_uid": str(pvc_uid),
            },
            job_id=str(job_id), generation=str(generation),
        )
    assert await db.queue_stateless_job_for_resume(
        str(job_id), expected_status="paused", void_completion_decision=False,
    )
    worker = await claim_worker_batch(db, pod_name="worker-a")
    assert worker is not None and worker.unit_id == job_id
    values = admission_kwargs(job_id, worker.lease_token) | {
        "provision_generation": generation, "vm_uid": vm_uid,
        "prior_vmi_uid": vmi_uid, "prior_launcher_uid": launcher_uid,
        "root_pvc_uid": pvc_uid,
        "code": WorkspaceRecoveryCode.RUNTIME_NOT_READY,
    }
    recovery = VMWorkspaceRecoveryStore(db, worker_id="resource-test")
    admitted_recovery = await recovery.admit_hold(**values)
    claim = await recovery.claim_due(admitted_recovery.operation_id)
    assert claim is not None
    observed_at = datetime.now(timezone.utc).isoformat()
    stop = {
        "protocol_version": 1, "vm_uid": str(vm_uid),
        "vmi_uid": str(vmi_uid), "launcher_uid": str(launcher_uid),
        "container_id": "containerd://old-compute", "root_pvc_uid": str(pvc_uid),
        "controller_identity": "controller/pod-1", "observed_at": observed_at,
        "containers": [{
            "name": "compute", "kind": "regular",
            "container_id": "containerd://old-compute",
            "terminated_container_id": "containerd://old-compute",
            "restart_count": 0, "state": "terminated", "last_state": None,
            "finished_at": observed_at, "reason": "Completed",
        }],
        "declared_containers": {"regular": ["compute"], "init": []},
        "pod_terminal": {"phase": "Succeeded", "restart_policy": "Never"},
    }
    stop_digest = await recovery.accept_stop_evidence(claim, stop)
    assert stop_digest is not None
    observation = {
        "ready": True, "authenticated": True, "ambiguous": False,
        "owner_kind": "job", "owner_id": str(job_id),
        "provision_generation": str(generation), "vm_uid": str(vm_uid),
        "root_pvc_uid": str(pvc_uid), "prior_runtime": "stopped",
        "stop_receipt_digest": stop_digest,
        "remote_operations": "settled", "continuation": "safe",
        "successor": {
            "vmi_uid": str(successor_vmi), "launcher_uid": str(successor_launcher),
            "node_uid": "node-8", "pod_ip": "10.42.0.90",
            "ssh_registration_id": "54" * 16,
            "guest_boot_id": "00000000-0000-4000-8000-000000000041",
            "guest_machine_id": "41" * 16,
            "interface_mac": "02:00:00:00:00:41",
            "guest_network": recovery_guest_network(),
        },
    }
    staged = await recovery.stage_observation(
        operation_id=claim.operation_id, version=claim.version,
        claim_token=claim.claim_token, phase="attesting", observation=observation,
    )
    assert staged is not None
    if teardown_before_release:
        await db.execute(
            "UPDATE vm_resource_reservations SET state='teardown' WHERE id=$1",
            UUID(admitted["reservation_id"]),
        )
        assert not await recovery.release_recovered(
            operation_id=staged.operation_id, version=staged.version,
            claim_token=staged.claim_token,
            initial_observation=observation,
            final_observation=observation.copy(),
            resume_receipt={"kind": "workspace_recovery"},
        )
        assert await db.fetchval(
            "SELECT count(*) FROM vm_resource_recovery_successors "
            "WHERE recovery_id=$1", staged.operation_id,
        ) == 0
        assert await db.fetchval(
            "SELECT resolved_at FROM vm_workspace_recoveries WHERE id=$1",
            staged.operation_id,
        ) is None
        queue = await db.fetchrow(
            "SELECT state,park_reason FROM run_queue WHERE unit_id=$1", job_id,
        )
        assert tuple(queue) == ("parked", "workspace_recovery")
        return
    assert await recovery.release_recovered(
        operation_id=staged.operation_id, version=staged.version,
        claim_token=staged.claim_token,
        initial_observation=observation, final_observation=observation.copy(),
        resume_receipt={"kind": "workspace_recovery"},
    )
    receipt = await db.fetchrow(
        "SELECT * FROM vm_resource_recovery_successors WHERE recovery_id=$1",
        staged.operation_id,
    )
    assert receipt is not None
    assert (receipt["reservation_id"], receipt["ordinal"],
            receipt["prior_vmi_uid"], receipt["successor_vmi_uid"],
            receipt["prior_launcher_uid"], receipt["successor_launcher_uid"]) == (
        UUID(admitted["reservation_id"]),
        1, vmi_uid, successor_vmi, launcher_uid, successor_launcher,
    )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        receipt["reservation_id"],
    ) == "active"
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError, match="append-only"):
            await conn.execute(
                "UPDATE vm_resource_recovery_successors SET successor_vmi_uid=$2 "
                "WHERE recovery_id=$1", staged.operation_id, uuid4(),
            )
        with pytest.raises(asyncpg.CheckViolationError, match="idle release missing"):
            async with conn.transaction():
                await conn.execute(
                    "UPDATE vm_resource_reservations SET state='teardown' WHERE id=$1",
                    receipt["reservation_id"],
                )
                await conn.execute(
                    "UPDATE vm_resource_reservations SET state='released',"
                    "released_at=clock_timestamp(),release_evidence=$2::jsonb "
                    "WHERE id=$1", receipt["reservation_id"],
                    json.dumps({"kind": "exact_compute_absent"}),
                )
    successor_sample = deepcopy(sample)
    successor_sample["vmis"][0]["uid"] = str(successor_vmi)
    successor_sample["pods"][0]["uid"] = str(successor_launcher)
    successor_sample["pods"][0]["vmi_uid"] = str(successor_vmi)
    successor_sample["snapshot_id"] = str(uuid4())
    successor_sample["sequence"] += 1
    successor_sample["started_at"] = successor_sample["finished_at"] = (
        datetime.now(timezone.utc).isoformat()
    )
    await publish(inventory, successor_sample)
    async with db.acquire() as conn, conn.transaction():
        await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job_id)
        source = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
            retry["request_id"],
        )
        assert await policy.bind_ready_on_conn(
            conn, retry=source,
            vm={
                "vm_uid": str(vm_uid), "vmi_uid": str(successor_vmi),
                "active_pod_uid": str(successor_launcher),
                "rootdisk_pvc_uid": str(pvc_uid),
            },
            job_id=str(job_id), generation=str(generation),
        )
    next_retry = await waiter(db, policy, inventory)
    next_admission = await policy.admit(request_id=str(next_retry["request_id"]))
    assert next_admission["action"] == "admitted", next_admission
    second_worker = await claim_worker_batch(db, pod_name="worker-a")
    assert second_worker is not None and second_worker.unit_id == job_id
    second_values = admission_kwargs(job_id, second_worker.lease_token) | {
        "provision_generation": generation, "vm_uid": vm_uid,
        "prior_vmi_uid": successor_vmi,
        "prior_launcher_uid": successor_launcher,
        "root_pvc_uid": pvc_uid,
        "code": WorkspaceRecoveryCode.RUNTIME_NOT_READY,
    }
    second = await recovery.admit_hold(**second_values)
    second_claim = await recovery.claim_due(second.operation_id)
    assert second_claim is not None
    second_stop = {
        **stop,
        "vmi_uid": str(successor_vmi),
        "launcher_uid": str(successor_launcher),
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }
    second_digest = await recovery.accept_stop_evidence(
        second_claim, second_stop,
    )
    assert second_digest is not None
    second_vmi, second_launcher = uuid4(), uuid4()
    second_observation = {
        **observation,
        "stop_receipt_digest": second_digest,
        "successor": {
            **observation["successor"],
            "vmi_uid": str(second_vmi),
            "launcher_uid": str(second_launcher),
        },
    }
    second_stage = await recovery.stage_observation(
        operation_id=second_claim.operation_id, version=second_claim.version,
        claim_token=second_claim.claim_token, phase="attesting",
        observation=second_observation,
    )
    assert second_stage is not None
    assert await recovery.release_recovered(
        operation_id=second_stage.operation_id, version=second_stage.version,
        claim_token=second_stage.claim_token,
        initial_observation=second_observation,
        final_observation=second_observation.copy(),
        resume_receipt={"kind": "workspace_recovery"},
    )
    lineage = await db.fetch(
        "SELECT ordinal,prior_vmi_uid,successor_vmi_uid,prior_launcher_uid,"
        "successor_launcher_uid FROM vm_resource_recovery_successors "
        "WHERE reservation_id=$1 ORDER BY ordinal", receipt["reservation_id"],
    )
    assert [tuple(row) for row in lineage] == [
        (1, vmi_uid, successor_vmi, launcher_uid, successor_launcher),
        (2, successor_vmi, second_vmi, successor_launcher, second_launcher),
    ]
    # Admin accounting follows the same append-only successor chain. The
    # replacement launcher is managed occupancy, never charged again as external.
    from orchestrator.services.vm_resource_capacity import vm_capacity_snapshot

    successor_sample["vmis"][0]["uid"] = str(second_vmi)
    successor_sample["pods"][0]["uid"] = str(second_launcher)
    successor_sample["pods"][0]["vmi_uid"] = str(second_vmi)
    successor_sample["snapshot_id"] = str(uuid4())
    successor_sample["sequence"] += 1
    successor_sample["started_at"] = successor_sample["finished_at"] = datetime.now(timezone.utc).isoformat()
    await publish(inventory, successor_sample)
    capacity = await vm_capacity_snapshot(db)
    cluster = next(c for c in capacity["clusters"] if c["cluster_id"] == inventory.cluster_id)
    assert cluster["available"] is True
    assert cluster["totals"]["active"] == demand.to_six_dict()
    assert cluster["totals"]["unbound"] == demand.to_six_dict()
    assert cluster["totals"]["external"] == dict.fromkeys(demand.to_six_dict(), 0)
