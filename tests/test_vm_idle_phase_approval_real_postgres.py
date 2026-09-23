"""Completion-owned phase approval must share the exact VM idle operation."""

import asyncio
import json
from uuid import UUID, uuid4

import pytest

from tests.test_vm_idle_lifecycle_real_postgres import (
    _schema,
    db as _db_fixture,
    pg_dsn,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    _schema_applied,  # noqa: F401
    seed_wait,
)
from shared.workspace_idle_policy import IdleEpisode, episode_document


db = _db_fixture


async def seed_phase_wait(db, *, command_state="done"):
    from orchestrator.services.workspace_idle_completion_events import (
        ACCEPTED_IDLE_WAIT_SOURCE_KEY,
        completion_runtime_evidence,
    )
    from shared.workspace_idle_completion import classify_completion_wait

    owner, old, identity = await seed_wait(db)
    command_id = uuid4()
    freeze = {
        "job_id": str(owner),
        "status": "pending_review",
        "freeze_type": "phase_boundary",
        "phase_type": "strategic",
        "phase_number": 2,
    }
    await db.execute(
        "UPDATE jobs SET status='pending_review',freeze_data=$2::jsonb,"
        "resolved_config=$3::jsonb,completion_seq_hwm=1 WHERE id=$1",
        owner,
        json.dumps(freeze),
        json.dumps(
            {
                "agent": {
                    "autonomy": "guided",
                    "verification": {"enabled": True},
                }
            }
        ),
    )
    job = dict(await db.fetchrow("SELECT * FROM jobs WHERE id=$1", owner))
    report = {
        "should_stop": True,
        "goal_achieved": False,
        "error": None,
        "freeze_data": freeze,
    }
    semantics = classify_completion_wait(job=job, report=report)
    runtime = completion_runtime_evidence(job)
    assert semantics and runtime
    source = {"version": 1, "semantics": semantics, **runtime}
    await db.execute(
        "INSERT INTO job_completion_commands "
        "(id,job_id,report_seq,client_report_id,payload,payload_digest,"
        "accepted_lease_token,requested_by,state,outcome,finalized_at,"
        "deadline_at,code_version) "
        "VALUES($1,$2,1,$3,$4::jsonb,$5,71,'phase-test',$6,$7::jsonb,"
        "$8,clock_timestamp()+interval '1 hour','phase-test')",
        command_id,
        owner,
        uuid4(),
        json.dumps({**report, ACCEPTED_IDLE_WAIT_SOURCE_KEY: source}),
        "sha256:" + "a" * 64,
        command_state,
        json.dumps({}) if command_state == "done" else None,
        None
        if command_state != "done"
        else await db.fetchval("SELECT clock_timestamp()"),
    )
    if command_state == "done":
        await db.execute(
            "INSERT INTO completion_effects "
            "(producer_kind,producer_id,scope_id,effect_name,effect_group,state,completed_at) "
            "VALUES('job_completion',$1,$2,'main_status_write','status','done',clock_timestamp())",
            command_id,
            owner,
        )
    episode = IdleEpisode(
        str(uuid4()),
        old.revision + 1,
        "human_approval",
        str(command_id),
        old.entered_at,
        None,
        0,
        old.runtime_identity,
    )
    await db.execute(
        "UPDATE jobs SET workspace_idle_revision=$3,"
        "workspace_idle_episode=$2::jsonb WHERE id=$1",
        owner,
        json.dumps(episode_document(episode)),
        episode.revision,
    )
    return owner, episode, identity, command_id


async def suspend_phase_wait(db, owner, episode, identity):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    store = VMIdleLifecycleStore(db)
    operation = await store.admit_release(
        str(owner),
        episode_id=episode.episode_id,
        revision=episode.revision,
        identity=identity,
    )
    assert operation is not None
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)",
        owner,
        identity["generation"],
    )
    evidence = {
        "version": 1,
        "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]),
        **identity,
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "retained_pvc": True,
        "controller_authenticated": True,
    }
    assert await store.complete_release(str(operation["id"]), evidence=evidence)
    return store, operation


@pytest.mark.asyncio
async def test_phase_release_requires_finalized_exact_source_and_no_control_claim(
    db,
    monkeypatch,
):
    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)
    await _schema(db)
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    owner, episode, identity, command_id = await seed_phase_wait(
        db,
        command_state="pending",
    )
    store = VMIdleLifecycleStore(db)

    async def admit():
        return await store.admit_release(
            str(owner),
            episode_id=episode.episode_id,
            revision=episode.revision,
            identity=identity,
        )

    assert await admit() is None
    await db.execute(
        "UPDATE job_completion_commands SET state='done',outcome='{}'::jsonb,"
        "finalized_at=clock_timestamp() WHERE id=$1",
        command_id,
    )
    assert await admit() is None
    await db.execute(
        "INSERT INTO completion_effects "
        "(producer_kind,producer_id,scope_id,effect_name,effect_group,state,completed_at) "
        "VALUES('job_completion',$1,$2,'main_status_write','status','done',clock_timestamp())",
        command_id,
        owner,
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,"
        "'{_completion_control_claim}',"
        '\'{"version":1,"claim_id":"held","expires_epoch":9999999999}\'::jsonb) '
        "WHERE id=$1",
        owner,
    )
    assert await admit() is None
    await db.execute(
        "UPDATE jobs SET context=context-'_completion_control_claim' WHERE id=$1",
        owner,
    )
    operation = await admit()
    assert operation is not None
    assert operation["episode_id"] == UUID(episode.episode_id)
    assert (
        await db.fetchval("SELECT status FROM jobs WHERE id=$1", owner)
        == "pending_review"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "phase", "episode", "command"])
async def test_claimed_phase_approval_joins_access_wake_and_executes_only_at_ready(
    db,
    monkeypatch,
    drift,
):
    from types import SimpleNamespace
    from orchestrator.services.completion_control import CompletionControl
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)
    await _schema(db)
    owner, episode, identity, command_id = await seed_phase_wait(db)
    store = VMIdleLifecycleStore(db)
    operation = await store.admit_release(
        str(owner),
        episode_id=episode.episode_id,
        revision=episode.revision,
        identity=identity,
    )
    assert operation is not None
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)",
        owner,
        identity["generation"],
    )
    evidence = {
        "version": 1,
        "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]),
        **identity,
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "retained_pvc": True,
        "controller_authenticated": True,
    }
    assert await store.complete_release(str(operation["id"]), evidence=evidence)
    access = await store.request_wake(
        str(owner),
        execution_requested=False,
        access_kind="ide",
        access_claimant="reviewer",
    )
    assert access is not None and not access["wake_execution_requested"]
    control = CompletionControl(db, SimpleNamespace(enqueue_job=None))
    claim = await control.claim_job(
        str(owner),
        source="public_approve",
        expected_status="pending_review",
        expected_lane="stateless",
    )
    async def approve():
        async with control.finish_claim(claim) as (conn, _job):
            return await store.approve_phase_wake_on_conn(
                conn,
                job_id=str(owner),
                claim_id=claim.claim_id,
            )

    approved, concurrent_access = await asyncio.gather(
        approve(),
        store.request_wake(
            str(owner), execution_requested=False,
            access_kind="ide", access_claimant="reviewer",
        ),
    )
    assert approved is not None
    assert concurrent_access is not None
    assert approved["wake_id"] == access["wake_id"]
    assert concurrent_access["wake_id"] == access["wake_id"]
    assert approved["wake_request_id"] == access["wake_request_id"]
    assert approved["wake_execution_requested"]
    assert (
        await db.fetchval("SELECT status FROM jobs WHERE id=$1", owner)
        == "pending_review"
    )
    assert (
        await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner)
        == "done"
    )
    assert (
        json.loads(
            await db.fetchval("SELECT freeze_data FROM jobs WHERE id=$1", owner)
        )["phase_number"]
        == 2
    )
    if drift == "phase":
        freeze = json.loads(
            await db.fetchval("SELECT freeze_data FROM jobs WHERE id=$1", owner)
        )
        freeze["phase_number"] = 99
        await db.execute(
            "UPDATE jobs SET freeze_data=$2::jsonb WHERE id=$1",
            owner,
            json.dumps(freeze),
        )
    elif drift == "episode":
        replacement = IdleEpisode(
            str(uuid4()),
            episode.revision + 1,
            "human_approval",
            str(uuid4()),
            episode.entered_at,
            None,
            0,
            episode.runtime_identity,
        )
        await db.execute(
            "UPDATE jobs SET workspace_idle_revision=$2,"
            "workspace_idle_episode=$3::jsonb WHERE id=$1",
            owner,
            replacement.revision,
            json.dumps(episode_document(replacement)),
        )
    elif drift == "command":
        payload = json.loads(
            await db.fetchval(
                "SELECT payload FROM job_completion_commands WHERE id=$1",
                command_id,
            )
        )
        payload["_accepted_idle_wait_source"]["semantics"]["freeze"]["phase_number"] = (
            99
        )
        await db.execute(
            "UPDATE job_completion_commands SET payload=$2::jsonb WHERE id=$1",
            command_id,
            json.dumps(payload),
        )

    successor = {
        "generation": str(access["wake_generation"]),
        "vm_uid": str(uuid4()),
        "vmi_uid": str(uuid4()),
        "launcher_uid": str(uuid4()),
        "pvc_uid": identity["pvc_uid"],
    }
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
    )
    context["last_vm"] = context["vm"]
    context["vm"] = {
        "status": "ready",
        "idle_wake_operation_id": str(operation["id"]),
        "provision_generation": successor["generation"],
        "vm_uid": successor["vm_uid"],
        "vmi_uid": successor["vmi_uid"],
        "active_pod_uid": successor["launcher_uid"],
        "rootdisk_pvc_uid": successor["pvc_uid"],
    }
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1", owner, json.dumps(context)
    )
    assert await store.mark_wake_ready(str(operation["id"]), **successor)
    assert await store.finish_wake(str(operation["id"])) is (drift is None)
    if drift is not None:
        row = await db.fetchrow(
            "SELECT status,freeze_data,context FROM jobs WHERE id=$1",
            owner,
        )
        assert row["status"] == "pending_review"
        assert row["freeze_data"] is not None
        assert (
            await db.fetchval(
                "SELECT state FROM run_queue WHERE unit_id=$1",
                owner,
            )
            == "done"
        )
        return
    assert await store.finish_wake(str(operation["id"]))
    row = await db.fetchrow(
        "SELECT status,freeze_data,workspace_idle_episode,context FROM jobs WHERE id=$1",
        owner,
    )
    assert row["status"] == "paused"
    assert row["freeze_data"] is None and row["workspace_idle_episode"] is None
    context = json.loads(row["context"])
    assert context["last_freeze_data"]["phase_number"] == 2
    assert context["_vm_idle_last_phase_approval"]["command_id"] == str(command_id)
    assert (
        await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner)
        == "queued"
    )


@pytest.mark.asyncio
async def test_authorized_approval_entrypoint_defers_exact_phase_until_ready(
    db,
    monkeypatch,
    tmp_path,
):
    import logging
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from orchestrator.services.completion_control import CompletionControl
    from orchestrator.services.completion_runtime import CompletionControlBoundary
    from tests.test_job_control_operations import _operations

    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)
    await _schema(db)
    owner, episode, identity, command_id = await seed_phase_wait(db)
    store, operation = await suspend_phase_wait(db, owner, episode, identity)
    access = await store.request_wake(
        str(owner),
        execution_requested=False,
        access_kind="ide",
        access_claimant="reviewer",
    )
    assert access is not None and not access["wake_execution_requested"]
    # Admission may be disabled during rollback, but admitted operations must drain.
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    real_control = CompletionControl(db, SimpleNamespace(enqueue_job=None))
    boundary = CompletionControlBoundary(
        SimpleNamespace(
            dependencies=SimpleNamespace(
                commands_enabled=lambda: True,
                logger=logging.getLogger(__name__),
            ),
            control=lambda: real_control,
        )
    )
    controls = _operations(tmp_path, store=db, completion_control=boundary)
    controls.dependencies.subjob_output.resolve_job_repo = AsyncMock(
        return_value=("repo", "job/branch"),
    )
    monkeypatch.setattr(
        type(controls),
        "_unmerged_pr_gate_reason",
        AsyncMock(return_value=None),
    )
    job = await db.get_job(str(owner))
    result = await controls.approve_job(
        str(owner),
        user={"id": "reviewer"},
        job=job,
        request=None,
    )
    assert result["status"] == "waking"
    controls.dependencies.resolve_job_notifications.assert_awaited_once_with(
        str(owner), user={"id": "reviewer"}, hook="approve",
    )
    state = await store.get_operation(str(operation["id"]))
    assert state["wake_id"] == access["wake_id"]
    assert state["wake_request_id"] == access["wake_request_id"]
    assert state["wake_execution_requested"]
    assert json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))[
        "_vm_idle_phase_approval"
    ]["command_id"] == str(command_id)
    assert (
        await db.fetchval("SELECT status FROM jobs WHERE id=$1", owner)
        == "pending_review"
    )
    assert (
        await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner)
        == "done"
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_idle_access_leases WHERE owner_id=$1 AND closed_at IS NULL",
            owner,
        )
        == 1
    )
    successor = {
        "generation": str(state["wake_generation"]),
        "vm_uid": str(uuid4()),
        "vmi_uid": str(uuid4()),
        "launcher_uid": str(uuid4()),
        "pvc_uid": identity["pvc_uid"],
    }
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
    )
    context["last_vm"] = context["vm"]
    context["vm"] = {
        "status": "ready",
        "idle_wake_operation_id": str(operation["id"]),
        "provision_generation": successor["generation"],
        "vm_uid": successor["vm_uid"],
        "vmi_uid": successor["vmi_uid"],
        "active_pod_uid": successor["launcher_uid"],
        "rootdisk_pvc_uid": successor["pvc_uid"],
    }
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
        owner,
        json.dumps(context),
    )
    assert await store.mark_wake_ready(str(operation["id"]), **successor)
    assert await store.finish_wake(str(operation["id"]))
    approved_at = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
    )["_vm_idle_last_phase_approval"]["approved_at"]
    assert await store.finish_wake(str(operation["id"]))
    final = await db.fetchrow(
        "SELECT status,freeze_data,context FROM jobs WHERE id=$1",
        owner,
    )
    assert final["status"] == "paused" and final["freeze_data"] is None
    final_context = json.loads(final["context"])
    assert final_context["last_freeze_data"]["phase_type"] == "strategic"
    assert final_context["last_freeze_data"]["phase_number"] == 2
    assert final_context["_vm_idle_last_phase_approval"]["command_id"] == str(
        command_id
    )
    assert final_context["_vm_idle_last_phase_approval"]["approved_at"] == approved_at
    assert "_vm_idle_phase_approval" not in final_context
    assert (
        await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner)
        == "queued"
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM job_completion_commands WHERE job_id=$1 AND state='done'",
            owner,
        )
        == 1
    )


@pytest.mark.asyncio
async def test_approval_claim_wins_release_race_and_queues_without_stopping(
    db,
    monkeypatch,
    tmp_path,
):
    import logging
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from orchestrator.services.completion_control import CompletionControl
    from orchestrator.services.completion_runtime import CompletionControlBoundary
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore
    from tests.test_job_control_operations import _operations

    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)
    await _schema(db)
    owner, episode, identity, command_id = await seed_phase_wait(db)
    store = VMIdleLifecycleStore(db)
    real_control = CompletionControl(db, SimpleNamespace(enqueue_job=None))
    boundary = CompletionControlBoundary(
        SimpleNamespace(
            dependencies=SimpleNamespace(
                commands_enabled=lambda: True,
                logger=logging.getLogger(__name__),
            ),
            control=lambda: real_control,
        )
    )
    controls = _operations(tmp_path, store=db, completion_control=boundary)

    async def race_during_approval(*_args, **_kwargs):
        assert (
            await store.admit_release(
                str(owner),
                episode_id=episode.episode_id,
                revision=episode.revision,
                identity=identity,
            )
            is None
        )
        return "repo", "job/branch"

    controls.dependencies.subjob_output.resolve_job_repo = AsyncMock(
        side_effect=race_during_approval,
    )
    monkeypatch.setattr(
        type(controls),
        "_unmerged_pr_gate_reason",
        AsyncMock(return_value=None),
    )
    job = await db.get_job(str(owner))
    result = await controls.approve_job(
        str(owner),
        user={"id": "reviewer"},
        job=job,
        request=None,
    )
    assert result["status"] == "approved_continue"
    assert await store.get_open_for_owner(str(owner)) is None
    row = await db.fetchrow("SELECT context,status FROM jobs WHERE id=$1", owner)
    context = json.loads(row["context"])
    assert row["status"] == "paused"
    assert context["last_freeze_data"]["phase_number"] == 2
    assert "_vm_idle_phase_approval" not in context
    assert (
        await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner)
        == "queued"
    )
    assert (
        await db.fetchval(
            "SELECT state FROM job_completion_commands WHERE id=$1",
            command_id,
        )
        == "done"
    )
