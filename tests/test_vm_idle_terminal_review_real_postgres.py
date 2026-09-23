"""Exact final S17 review, terminal no-wake and retained-rootdisk authority."""

import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
import asyncpg

from tests.test_vm_idle_lifecycle_real_postgres import (
    _schema, db as _db_fixture, pg_dsn,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    _schema_applied,  # noqa: F401
    seed_wait,
)
from shared.workspace_idle_policy import IdleEpisode, episode_document

db = _db_fixture


@pytest.fixture(autouse=True)
def terminal_env(monkeypatch):
    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)


async def seed_final_review(db, *, finalized=True):
    from orchestrator.services.workspace_idle_completion_events import (
        ACCEPTED_IDLE_WAIT_SOURCE_KEY, completion_runtime_evidence,
    )
    from shared.workspace_idle_completion import classify_completion_wait

    owner, previous, identity = await seed_wait(db)
    command_id, decision_id = uuid4(), "final-review-decision"
    freeze = {
        "job_id": str(owner), "status": "pending_review",
        "freeze_type": "job_complete", "summary": "reviewed",
    }
    context = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))
    context["completion_decision"] = {"tool_call_id": decision_id}
    await db.execute(
        "UPDATE jobs SET status='pending_review',context=$2::jsonb,freeze_data=$3::jsonb,"
        "resolved_config=$4::jsonb,completion_seq_hwm=1 WHERE id=$1",
        owner, json.dumps(context), json.dumps(freeze),
        json.dumps({"agent": {"autonomy": "review", "verification": {"enabled": False}}}),
    )
    job = dict(await db.fetchrow("SELECT * FROM jobs WHERE id=$1", owner))
    report = {"should_stop": True, "goal_achieved": False, "error": None,
              "freeze_data": freeze}
    semantics = classify_completion_wait(
        job=job, report=report, decision_tool_call_id=decision_id,
    )
    runtime = completion_runtime_evidence(job)
    assert semantics and runtime
    await db.execute(
        "INSERT INTO job_completion_commands "
        "(id,job_id,report_seq,client_report_id,payload,payload_digest,"
        "accepted_lease_token,requested_by,state,outcome,finalized_at,deadline_at,code_version) "
        "VALUES($1,$2,1,$3,$4::jsonb,$5,71,'terminal-test',$6,$7::jsonb,$8,"
        "clock_timestamp()+interval '1 hour','terminal-test')",
        command_id, owner, uuid4(),
        json.dumps({**report, ACCEPTED_IDLE_WAIT_SOURCE_KEY: {
            "version": 1, "semantics": semantics, **runtime,
        }}),
        "sha256:" + "a" * 64,
        "done" if finalized else "pending",
        json.dumps({}) if finalized else None,
        await db.fetchval("SELECT clock_timestamp()") if finalized else None,
    )
    if finalized:
        await db.execute(
            "INSERT INTO completion_effects "
            "(producer_kind,producer_id,scope_id,effect_name,effect_group,state,completed_at) "
            "VALUES('job_completion',$1,$2,'main_status_write','status','done',clock_timestamp())",
            command_id, owner,
        )
    episode = IdleEpisode(
        str(uuid4()), previous.revision + 1, "human_review", str(command_id),
        previous.entered_at, None, 0, previous.runtime_identity,
    )
    await db.execute(
        "UPDATE jobs SET workspace_idle_revision=$2,workspace_idle_episode=$3::jsonb "
        "WHERE id=$1", owner, episode.revision,
        json.dumps(episode_document(episode)),
    )
    return owner, episode, identity, command_id


async def controls_for(db, monkeypatch, tmp_path):
    from orchestrator.services.completion_control import CompletionControl
    from orchestrator.services.completion_runtime import CompletionControlBoundary
    from tests.test_job_control_operations import _operations

    real_control = CompletionControl(db, SimpleNamespace(enqueue_job=AsyncMock()))
    boundary = CompletionControlBoundary(SimpleNamespace(
        dependencies=SimpleNamespace(
            commands_enabled=lambda: True, logger=logging.getLogger(__name__),
        ),
        control=lambda: real_control,
    ))
    controls = _operations(tmp_path, store=db, completion_control=boundary)
    controls.dependencies.subjob_output.resolve_job_repo = AsyncMock(
        return_value=("repo", "job/branch"),
    )
    monkeypatch.setattr(type(controls), "_unmerged_pr_gate_reason", AsyncMock(return_value=None))
    return controls


@pytest.mark.asyncio
async def test_final_review_release_requires_finalized_s17_and_restart_proof(db):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    await _schema(db)
    owner, episode, identity, command_id = await seed_final_review(db, finalized=False)
    store = VMIdleLifecycleStore(db)

    async def admit():
        return await store.admit_release(
            str(owner), episode_id=episode.episode_id,
            revision=episode.revision, identity=identity,
        )

    assert await admit() is None
    await db.execute(
        "UPDATE job_completion_commands SET state='done',outcome='{}'::jsonb,"
        "finalized_at=clock_timestamp() WHERE id=$1", command_id,
    )
    assert await admit() is None
    await db.execute(
        "INSERT INTO completion_effects "
        "(producer_kind,producer_id,scope_id,effect_name,effect_group,state,completed_at) "
        "VALUES('job_completion',$1,$2,'main_status_write','status','done',clock_timestamp())",
        command_id, owner,
    )
    from orchestrator.services.vm_idle_phase_approval import finalized_review_source
    async with db.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", owner)
        assert await finalized_review_source(
            conn, job=row, episode=episode,
            generation=identity["generation"], vm_uid=identity["vm_uid"],
            launcher_uid=identity["launcher_uid"],
            pvc_uid=identity["pvc_uid"],
        ) is not None
    assert await admit() is not None


@pytest.mark.asyncio
async def test_approval_before_release_commits_no_wake_and_replays_publication(
    db, monkeypatch, tmp_path,
):
    from orchestrator.services.vm_idle_lifecycle import (
        VMIdleLifecycleService, VMIdleLifecycleStore,
    )

    await _schema(db)
    owner, episode, identity, command_id = await seed_final_review(db)
    controls = await controls_for(db, monkeypatch, tmp_path)
    controls.dependencies.forge.is_initialized = False
    (tmp_path / "output").mkdir()
    real_publish = type(controls).publish_terminal_review
    monkeypatch.setattr(
        type(controls), "publish_terminal_review",
        AsyncMock(side_effect=RuntimeError("offline")),
    )
    caller = await db.get_job(str(owner))
    result = await controls.approve_job(
        str(owner), user={"id": "reviewer"},
        job=caller, request=None,
    )
    assert result["status"] == "approved" and result["publication_pending"]
    job = await db.fetchrow(
        "SELECT status,workspace_idle_episode,workspace_idle_revision FROM jobs WHERE id=$1", owner,
    )
    assert job["status"] == "completed" and job["workspace_idle_episode"] is None
    assert job["workspace_idle_revision"] == episode.revision + 1
    operation = await VMIdleLifecycleStore(db).get_open_for_owner(str(owner))
    assert operation["terminal_source_command_id"] == command_id
    assert operation["storage_disposition"] == "retention_unknown"
    assert not operation["wake_requested"] and not operation["wake_execution_requested"]
    assert operation["terminal_published_at"] is None
    monkeypatch.setattr(type(controls), "publish_terminal_review", real_publish)
    controls.dependencies.forge.is_initialized = True
    controls.dependencies.forge.create_or_update_file = AsyncMock(return_value=True)
    controls.dependencies.forge.delete_file = AsyncMock(return_value=True)
    monkeypatch.setattr(
        "orchestrator.services.completion.apply_terminal_job_side_effects",
        AsyncMock(return_value={}),
    )
    await db.execute(
        "UPDATE vm_idle_operations SET phase='superseded',closed_at=clock_timestamp(),"
        "terminal_publication_retry_after=clock_timestamp()-interval '1 second' "
        "WHERE id=$1", operation["id"],
    )
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    service = VMIdleLifecycleService(
        db, SimpleNamespace(), None, claimant="terminal-replay",
        terminal_publication_handler=controls.publish_terminal_review,
    )
    assert await service.reconcile_once(limit=4) == 1
    frozen_payload = json.loads(operation["terminal_publication"])
    assert json.loads((tmp_path / "output" / "job_completion.json").read_text()) == frozen_payload[
        "completion_data"
    ]
    assert await db.fetchval(
        "SELECT terminal_published_at IS NOT NULL FROM vm_idle_operations WHERE id=$1",
        operation["id"],
    )
    assert not await VMIdleLifecycleStore(db).request_wake(
        str(owner), execution_requested=True,
    )
    assert await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner) == "done"
    from fastapi import HTTPException

    with pytest.raises(HTTPException):
        await controls.approve_job(
            str(owner), user={"id": "reviewer"}, job=caller, request=None,
        )
    assert await db.fetchval(
        "SELECT count(*) FROM vm_idle_operations WHERE owner_id=$1 "
        "AND terminal_source_command_id=$2", owner, command_id,
    ) == 1


@pytest.mark.asyncio
async def test_failed_frozen_delete_replays_without_repeating_legacy_merge(
    db, monkeypatch, tmp_path,
):
    from orchestrator.services.vm_idle_lifecycle import (
        VMIdleLifecycleService, VMIdleLifecycleStore,
    )

    await _schema(db)
    owner, _, _, _ = await seed_final_review(db)
    controls = await controls_for(db, monkeypatch, tmp_path)
    controls.dependencies.forge.is_initialized = True
    controls.dependencies.forge.create_or_update_file = AsyncMock(return_value=True)
    controls.dependencies.forge.delete_file = AsyncMock(side_effect=[False, True])
    merge_and_record = AsyncMock(return_value={})
    monkeypatch.setattr(
        "orchestrator.services.completion.apply_terminal_job_side_effects",
        merge_and_record,
    )
    (tmp_path / "output").mkdir()
    (tmp_path / "output" / "job_frozen.json").write_text("{}")
    result = await controls.approve_job(
        str(owner), user={"id": "reviewer"},
        job=await db.get_job(str(owner)), request=None,
    )
    assert result["status"] == "approved" and result["publication_pending"]
    operation = await VMIdleLifecycleStore(db).get_open_for_owner(str(owner))
    assert operation["terminal_published_at"] is None
    assert controls.dependencies.forge.delete_file.await_count == 1
    assert merge_and_record.await_count == 1
    assert (tmp_path / "output" / "job_frozen.json").exists()
    assert await db.get_job_change_record(str(owner)) is None
    await db.execute(
        "UPDATE vm_idle_operations SET terminal_publication_retry_after="
        "clock_timestamp()-interval '1 second' WHERE id=$1", operation["id"],
    )
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    service = VMIdleLifecycleService(
        db, SimpleNamespace(), None, claimant="terminal-delete-replay",
        terminal_publication_handler=controls.publish_terminal_review,
    )
    assert await service.reconcile_once(limit=4) == 1
    assert controls.dependencies.forge.delete_file.await_count == 2
    assert merge_and_record.await_count == 1
    assert not (tmp_path / "output" / "job_frozen.json").exists()
    assert await db.get_job_change_record(str(owner)) is not None
    assert await db.fetchval(
        "SELECT terminal_published_at IS NOT NULL FROM vm_idle_operations WHERE id=$1",
        operation["id"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["releasing", "suspended"])
async def test_approval_joins_existing_release_without_wake(db, monkeypatch, tmp_path, state):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    await _schema(db)
    owner, episode, identity, command_id = await seed_final_review(db)
    store = VMIdleLifecycleStore(db)
    original = await store.admit_release(
        str(owner), episode_id=episode.episode_id,
        revision=episode.revision, identity=identity,
    )
    assert original is not None
    if state == "suspended":
        await db.execute(
            "INSERT INTO managed_repository_process_zero_receipts "
            "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
            "VALUES('job',$1,'vm','vm',$2)", owner, identity["generation"],
        )
        evidence = {
            "version": 1, "kind": "vm_idle_physical_stop",
            "operation_id": str(original["id"]), **identity,
            "vm_absent": True, "vmi_absent": True, "launcher_absent": True,
            "same_generation_replacement": False, "retained_pvc": True,
            "controller_authenticated": True,
        }
        assert await store.complete_release(str(original["id"]), evidence=evidence)
    controls = await controls_for(db, monkeypatch, tmp_path)
    monkeypatch.setattr(type(controls), "publish_terminal_review", AsyncMock())
    result = await controls.approve_job(
        str(owner), user={"id": "reviewer"},
        job=await db.get_job(str(owner)), request=None,
    )
    assert result["status"] == "approved"
    operation = await store.get_open_for_owner(str(owner))
    assert operation["id"] == original["id"]
    assert operation["terminal_source_command_id"] == command_id
    assert operation["phase"] == state
    assert not operation["wake_requested"] and not operation["wake_execution_requested"]


@pytest.mark.asyncio
@pytest.mark.parametrize("hold", ["ide_active", "queue_queued", "queue_leased"])
async def test_immediate_terminal_approval_preserves_live_access_and_worker_holds(
    db, monkeypatch, tmp_path, hold,
):
    from fastapi import HTTPException

    await _schema(db)
    owner, _, _, _ = await seed_final_review(db)
    if hold == "ide_active":
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{ide_session}',"
            "'{\"status\":\"active\"}'::jsonb) WHERE id=$1", owner,
        )
    else:
        await db.execute(
            "UPDATE run_queue SET state=$2,leased_by=$3,"
            "leased_until=CASE WHEN $2='leased' THEN "
            "clock_timestamp()+interval '5 minutes' ELSE NULL END WHERE unit_id=$1",
            owner, "leased" if hold == "queue_leased" else "queued",
            "busy-worker" if hold == "queue_leased" else None,
        )
    controls = await controls_for(db, monkeypatch, tmp_path)
    (tmp_path / "output").mkdir()
    original_queue = await db.fetchrow(
        "SELECT state,lease_token,leased_by,leased_until FROM run_queue WHERE unit_id=$1",
        owner,
    )
    for _ in range(2 if hold.startswith("queue_") else 1):
        with pytest.raises(HTTPException) as raised:
            await controls.approve_job(
                str(owner), user={"id": "reviewer"},
                job=await db.get_job(str(owner)), request=None,
            )
        assert raised.value.status_code == 409
        if hold.startswith("queue_"):
            assert await db.fetchrow(
                "SELECT state,lease_token,leased_by,leased_until FROM run_queue "
                "WHERE unit_id=$1", owner,
            ) == original_queue
        assert await db.fetchval(
            "SELECT context ? '_completion_control_claim' FROM jobs WHERE id=$1",
            owner,
        ) is False
        assert await db.fetchval("SELECT status FROM jobs WHERE id=$1", owner) == "pending_review"
        assert await db.fetchval(
            "SELECT count(*) FROM vm_idle_operations WHERE owner_id=$1", owner,
        ) == 0
        assert not (tmp_path / "output" / "job_completion.json").exists()
        controls.dependencies.forge.create_or_update_file.assert_not_called()
        controls.dependencies.forge.delete_file.assert_not_called()
    if hold.startswith("queue_"):
        from shared.worker_queue import (
            cancel_queued_worker_batch, complete_worker_batch,
        )

        # Settle through the actual queue APIs, separately from approval.
        async with db.acquire() as conn, conn.transaction():
            if hold == "queue_queued":
                assert await cancel_queued_worker_batch(conn, job_id=owner)
            else:
                assert await complete_worker_batch(
                    conn, unit_id=owner,
                    lease_token=original_queue["lease_token"], consumed_seq=None,
                ) == "done"
        monkeypatch.setattr(type(controls), "publish_terminal_review", AsyncMock())
        result = await controls.approve_job(
            str(owner), user={"id": "reviewer"},
            job=await db.get_job(str(owner)), request=None,
        )
        assert result["status"] == "approved"
        assert await db.fetchval(
            "SELECT count(*) FROM vm_idle_operations WHERE owner_id=$1 "
            "AND terminal_source_command_id IS NOT NULL", owner,
        ) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["vmi_uid", "active_pod_uid", "access_wake"])
async def test_joined_terminal_approval_rejects_current_pod_drift_or_access_winner(
    db, monkeypatch, tmp_path, drift,
):
    from fastapi import HTTPException
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    await _schema(db)
    owner, episode, identity, _ = await seed_final_review(db)
    store = VMIdleLifecycleStore(db)
    operation = await store.admit_release(
        str(owner), episode_id=episode.episode_id,
        revision=episode.revision, identity=identity,
    )
    assert operation is not None
    if drift == "access_wake":
        wake = await store.request_wake(str(owner), execution_requested=False)
        assert wake is not None and wake["wake_requested"]
    else:
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,$2::text[],to_jsonb($3::text)) "
            "WHERE id=$1", owner, ["vm", drift], str(uuid4()),
        )
    controls = await controls_for(db, monkeypatch, tmp_path)
    with pytest.raises(HTTPException) as raised:
        await controls.approve_job(
            str(owner), user={"id": "reviewer"},
            job=await db.get_job(str(owner)), request=None,
        )
    assert raised.value.status_code == 409
    after = await db.fetchrow("SELECT * FROM vm_idle_operations WHERE id=$1", operation["id"])
    assert after["terminal_source_command_id"] is None
    assert await db.fetchval("SELECT status FROM jobs WHERE id=$1", owner) == "pending_review"


@pytest.mark.asyncio
async def test_terminal_approval_exempts_only_no_successor_from_network_and_budget(
    db, monkeypatch, tmp_path,
):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    await _schema(db)
    owner, episode, identity, _ = await seed_final_review(db)
    monkeypatch.setenv("VM_PROVISION_MAX_ATTEMPTS", "0")
    # The first-boot profile may be absent on a legacy disk. A waiting review
    # cannot stop it; after exact terminal approval no successor is admitted.
    context = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))
    context["vm"]["creation_preflight"]["request"].pop("network_profile")
    await db.execute("UPDATE jobs SET context=$2::jsonb WHERE id=$1", owner, json.dumps(context))
    store = VMIdleLifecycleStore(db)
    assert await store.admit_release(
        str(owner), episode_id=episode.episode_id,
        revision=episode.revision, identity=identity,
    ) is None
    # The frozen runtime source is independent of a successor profile.
    controls = await controls_for(db, monkeypatch, tmp_path)
    monkeypatch.setattr(type(controls), "publish_terminal_review", AsyncMock())
    result = await controls.approve_job(
        str(owner), user={"id": "reviewer"},
        job=await db.get_job(str(owner)), request=None,
    )
    assert result["status"] == "approved"
    assert (await store.get_open_for_owner(str(owner)))["storage_disposition"] == "retention_unknown"


@pytest.mark.asyncio
async def test_stale_caller_review_a_cannot_approve_b_or_write_artifacts(
    db, monkeypatch, tmp_path,
):
    from fastapi import HTTPException

    await _schema(db)
    owner, episode, identity, command_id = await seed_final_review(db)
    caller_a = await db.get_job(str(owner))
    command_b = uuid4()
    payload = await db.fetchval(
        "SELECT payload FROM job_completion_commands WHERE id=$1", command_id,
    )
    await db.execute(
        "INSERT INTO job_completion_commands "
        "(id,job_id,report_seq,client_report_id,payload,payload_digest,"
        "accepted_lease_token,requested_by,state,outcome,finalized_at,deadline_at,code_version) "
        "VALUES($1,$2,2,$3,$4::jsonb,$5,71,'terminal-test','done','{}'::jsonb,"
        "clock_timestamp(),clock_timestamp()+interval '1 hour','terminal-test')",
        command_b, owner, uuid4(), payload, "sha256:" + "b" * 64,
    )
    await db.execute(
        "INSERT INTO completion_effects "
        "(producer_kind,producer_id,scope_id,effect_name,effect_group,state,completed_at) "
        "VALUES('job_completion',$1,$2,'main_status_write','status','done',clock_timestamp())",
        command_b, owner,
    )
    successor = IdleEpisode(
        str(uuid4()), episode.revision + 1, "human_review", str(command_b),
        episode.entered_at, None, 0, episode.runtime_identity,
    )
    await db.execute(
        "UPDATE jobs SET completion_seq_hwm=2,workspace_idle_revision=$2,"
        "workspace_idle_episode=$3::jsonb WHERE id=$1",
        owner, successor.revision, json.dumps(episode_document(successor)),
    )
    controls = await controls_for(db, monkeypatch, tmp_path)
    controls.dependencies.forge.is_initialized = False
    (tmp_path / "output").mkdir()
    with pytest.raises(HTTPException) as raised:
        await controls.approve_job(
            str(owner), user={"id": "reviewer"}, job=caller_a, request=None,
        )
    assert raised.value.status_code == 409
    assert not (tmp_path / "output" / "job_completion.json").exists()
    assert await db.fetchval("SELECT status FROM jobs WHERE id=$1", owner) == "pending_review"
    assert await db.fetchval(
        "SELECT count(*) FROM vm_idle_operations WHERE owner_id=$1", owner,
    ) == 0


@pytest.mark.asyncio
async def test_terminal_release_requires_process_zero_and_exact_physical_stop(db, monkeypatch, tmp_path):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    await _schema(db)
    owner, episode, identity, _ = await seed_final_review(db)
    controls = await controls_for(db, monkeypatch, tmp_path)
    monkeypatch.setattr(type(controls), "publish_terminal_review", AsyncMock())
    await controls.approve_job(str(owner), user={"id": "reviewer"},
                               job=await db.get_job(str(owner)), request=None)
    store = VMIdleLifecycleStore(db)
    operation = await store.get_open_for_owner(str(owner))
    evidence = {
        "version": 1, "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]), **identity,
        "vm_absent": True, "vmi_absent": True, "launcher_absent": True,
        "same_generation_replacement": False, "retained_pvc": True,
        "controller_authenticated": True,
    }
    assert not await store.complete_release(str(operation["id"]), evidence=evidence)
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)", owner, identity["generation"],
    )
    assert not await store.complete_release(str(operation["id"]),
                                            evidence={**evidence, "vm_absent": False})
    assert await store.complete_release(str(operation["id"]), evidence=evidence)
    stopped = await store.get_open_for_owner(str(owner))
    assert stopped["phase"] == "suspended" and not stopped["wake_requested"]
    assert not await store.finish_wake(str(operation["id"]))


@pytest.mark.asyncio
async def test_retention_hold_survives_closed_operation_and_flag_off(
    db, monkeypatch, tmp_path,
):
    from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
    from orchestrator.services.lifecycle.vm_manager import VMInstanceManager

    await _schema(db)
    owner, _, identity, command_id = await seed_final_review(db)
    controls = await controls_for(db, monkeypatch, tmp_path)
    monkeypatch.setattr(type(controls), "publish_terminal_review", AsyncMock())
    await controls.approve_job(str(owner), user={"id": "reviewer"},
                               job=await db.get_job(str(owner)), request=None)
    operation = await db.fetchrow(
        "SELECT * FROM vm_idle_operations WHERE owner_id=$1", owner,
    )
    await db.execute(
        "UPDATE vm_idle_operations SET phase='superseded',closed_at=clock_timestamp() "
        "WHERE id=$1", operation["id"],
    )
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    recovery = VMWorkspaceRecoveryStore(db)
    for source in ("completion_workspace_teardown", "kept_disk"):
        for pvc_uid in (UUID(identity["pvc_uid"]), None):
            permit = await recovery.acquire_cleanup_permit(
                owner_kind="job", owner_id=owner,
                pvc_uid=pvc_uid, request_id=uuid4(),
                source=source, intent_digest="test",
            )
            assert not permit.allowed and permit.reason == "terminal_retention_unknown"
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "INSERT INTO vm_workspace_cleanup_admissions "
            "(owner_kind,owner_id,pvc_uid,source,request_id,intent_digest) "
            "VALUES('job',$1,$2,'kept_disk',$3,'test')",
            owner, UUID(identity["pvc_uid"]), uuid4(),
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "INSERT INTO vm_workspace_cleanup_admissions "
            "(owner_kind,owner_id,pvc_uid,source,request_id,intent_digest) "
            "VALUES('job',$1,NULL,'completion_workspace_teardown',$2,'test')",
            owner, uuid4(),
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_idle_operations SET pvc_uid=$2 WHERE id=$1",
            operation["id"], uuid4(),
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_idle_operations SET wake_requested=true WHERE id=$1",
            operation["id"],
        )
    vm_provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(
            side_effect=AssertionError("retained disk must never be captured for purge")
        ),
        release_vm_captured=AsyncMock(),
    )
    manager = VMInstanceManager(vm_provisioner, None, None, db)
    assert await manager.purge_kept_disks() == 0
    vm_provisioner.release_vm_captured.assert_not_awaited()


@pytest.mark.asyncio
async def test_actual_s36_replay_holds_retained_vm_before_destructive_permit(
    db, monkeypatch, tmp_path,
):
    from orchestrator.services.completion_effects import (
        CompletionEffectDependencies, run_completion_workspace_teardown,
    )
    from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore

    await _schema(db)
    owner, _, identity, _ = await seed_final_review(db)
    controls = await controls_for(db, monkeypatch, tmp_path)
    monkeypatch.setattr(type(controls), "publish_terminal_review", AsyncMock())
    await controls.approve_job(str(owner), user={"id": "reviewer"},
                               job=await db.get_job(str(owner)), request=None)
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    context = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))
    vm = context["vm"]
    intent = {
        "kind": "vm", "provision_generation": identity["generation"],
        "vm_uid": identity["vm_uid"], "rootdisk_pvc_uid": identity["pvc_uid"],
        "ssh_host": vm["ssh_host"], "ssh_port": vm["ssh_port"],
        "ssh_host_key_fingerprint": vm["ssh_host_key_fingerprint"],
    }

    class Runner:
        command_id = str(uuid4())

        async def authorize_workspace_teardown(self):
            return SimpleNamespace(authorized=True)

        async def capture_intent(self, name, value=None):
            assert name == "workspace_archive_teardown" and value is None
            return intent

        async def run(self, **kwargs):
            return await kwargs["callback"]()

    vm_provisioner = SimpleNamespace(
        release_vm_captured=AsyncMock(
            side_effect=AssertionError("retained disk must not be released")
        ),
    )
    dependencies = CompletionEffectDependencies(
        store=db, container_provisioner=SimpleNamespace(),
        vm_provisioner=vm_provisioner,
        get_container_context=lambda job: {},
        get_vm_context=lambda job: (
            json.loads(job["context"]) if isinstance(job["context"], str)
            else job["context"]
        ).get("vm", {}),
        archive_and_cleanup_workspace=AsyncMock(),
        s36_exact_absence_timeout_seconds=lambda: 30.0,
        logger=logging.getLogger(__name__),
        recovery_store=VMWorkspaceRecoveryStore(db),
    )
    output = await run_completion_workspace_teardown(
        str(owner), Runner(), dependencies=dependencies,
    )
    assert output["teardown_disposition"] == "retry_pending"
    assert "terminal_retention_unknown" in output["error"]
    legacy_output = await run_completion_workspace_teardown(
        str(owner), None, dependencies=dependencies,
    )
    assert legacy_output["teardown_disposition"] == "retry_pending"
    assert "terminal_retention_unknown" in legacy_output["error"]
    vm_provisioner.release_vm_captured.assert_not_awaited()


@pytest.mark.asyncio
async def test_preexisting_destructive_s36_intent_holds_approval(db, monkeypatch, tmp_path):
    from fastapi import HTTPException

    await _schema(db)
    owner, _, identity, command_id = await seed_final_review(db)
    await db.execute(
        "INSERT INTO completion_effects "
        "(producer_kind,producer_id,scope_id,effect_name,effect_group,state,intent_at) "
        "VALUES('job_completion',$1,$2,'workspace_archive_teardown',"
        "'workspace_teardown','pending',clock_timestamp())",
        command_id, owner,
    )
    controls = await controls_for(db, monkeypatch, tmp_path)
    (tmp_path / "output").mkdir()
    with pytest.raises(HTTPException) as raised:
        await controls.approve_job(str(owner), user={"id": "reviewer"},
                                   job=await db.get_job(str(owner)), request=None)
    assert raised.value.status_code == 409
    assert not (tmp_path / "output" / "job_completion.json").exists()
    assert await db.fetchval("SELECT status FROM jobs WHERE id=$1", owner) == "pending_review"


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["decision", "vm_uid", "pvc_uid", "command"])
async def test_exact_final_source_or_runtime_drift_holds_approval_without_artifacts(
    db, monkeypatch, tmp_path, drift,
):
    from fastapi import HTTPException

    await _schema(db)
    owner, _, identity, command_id = await seed_final_review(db)
    if drift == "command":
        await db.execute(
            "UPDATE job_completion_commands SET state='pending',finalized_at=NULL,"
            "outcome=NULL WHERE id=$1", command_id,
        )
    elif drift == "decision":
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,"
            "'{completion_decision,tool_call_id}',to_jsonb('later'::text)) WHERE id=$1",
            owner,
        )
    else:
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,$2::text[],to_jsonb($3::text)) "
            "WHERE id=$1", owner, ["vm", "vm_uid" if drift == "vm_uid" else "rootdisk_pvc_uid"],
            str(uuid4()),
        )
    controls = await controls_for(db, monkeypatch, tmp_path)
    (tmp_path / "output").mkdir()
    with pytest.raises(HTTPException) as raised:
        await controls.approve_job(
            str(owner), user={"id": "reviewer"},
            job=await db.get_job(str(owner)), request=None,
        )
    assert raised.value.status_code == 409
    assert not (tmp_path / "output" / "job_completion.json").exists()
    assert await db.fetchval(
        "SELECT count(*) FROM vm_idle_operations WHERE owner_id=$1", owner,
    ) == 0


@pytest.mark.asyncio
async def test_failed_publication_yields_to_later_terminal_decision(
    db, monkeypatch, tmp_path,
):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleService

    await _schema(db)
    controls = await controls_for(db, monkeypatch, tmp_path)
    monkeypatch.setattr(type(controls), "publish_terminal_review", AsyncMock(
        side_effect=RuntimeError("offline"),
    ))
    operations = []
    for _ in range(2):
        owner, _, _, _ = await seed_final_review(db)
        result = await controls.approve_job(
            str(owner), user={"id": "reviewer"},
            job=await db.get_job(str(owner)), request=None,
        )
        assert result["publication_pending"]
        operation = await db.fetchrow(
            "SELECT id FROM vm_idle_operations WHERE owner_id=$1", owner,
        )
        operations.append(operation["id"])
        await db.execute(
            "UPDATE vm_idle_operations SET phase='superseded',closed_at=clock_timestamp(),"
            "terminal_publication_retry_after=clock_timestamp()-interval '1 second' "
            "WHERE id=$1", operation["id"],
        )
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    seen = []

    async def publish(operation):
        seen.append(operation["id"])
        if operation["id"] == operations[0]:
            raise RuntimeError("first operation offline")

    service = VMIdleLifecycleService(
        db, SimpleNamespace(), None, claimant="publication-fairness",
        terminal_publication_handler=publish,
    )
    assert await service.reconcile_once(limit=2) == 1
    assert seen == operations
    first = await db.fetchrow(
        "SELECT terminal_published_at,terminal_publication_retry_after "
        "FROM vm_idle_operations WHERE id=$1", operations[0],
    )
    assert first["terminal_published_at"] is None
    assert first["terminal_publication_retry_after"] is not None
    assert await db.fetchval(
        "SELECT terminal_published_at IS NOT NULL FROM vm_idle_operations WHERE id=$1",
        operations[1],
    )
