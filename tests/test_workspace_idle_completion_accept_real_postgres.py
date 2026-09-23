"""Completion acceptance freezes server evidence once under the queue/Job fence."""

import asyncio
import json
from uuid import UUID, uuid4

import asyncpg

import pytest

from tests.test_completion_finalizer_real_postgres import (
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    pg as _pg_fixture,
)
from tests.test_vm_remote_operation_real_postgres import _vm_identity
from tests._previous_release_seed import seed_previous_release_row
from orchestrator.services.job_completion_commands import accept_completion_command
from shared.workspace_contract import workspace_runtime_authority_digest
from shared.worker_queue import record_worker_bundle_authorized


pg = _pg_fixture

KEY = "_accepted_idle_wait_source"


@pytest.fixture(autouse=True)
def tracking(monkeypatch):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")


async def seed(pg, *, proof=True, lane="stateless", manifest=None, repository=None):
    vm, _ = _vm_identity()
    vm["ssh_ready_source"] = "provisioner_probe"
    vm["rootdisk_pvc_uid"] = str(uuid4())
    vm["vmi_uid"] = str(uuid4())
    admitted_context = (
        {"required_deliverables": manifest} if manifest is not None else {}
    )
    reserved_job_id = uuid4()
    if repository is not None:
        from tests.test_managed_repository_authority_real_postgres import _reserve
        from tests.test_completion_finalizer_real_postgres import _pool_db

        repository_db = _pool_db(pg)
        authority = await _reserve(
            repository_db, repo_name=repository, authority_id=reserved_job_id
        )
        assert await repository_db.activate_managed_repository_authority(
            str(authority["id"]), forge_key_id=91, access_mode="write"
        )
        admitted_context["git_remote_url"] = authority["clean_repo_url"]
    context = {**admitted_context, "vm": vm}
    config = {"workspace": {"backend": "vm"}}
    policy = {"agent": {"autonomy": "guided", "verification": {"enabled": True}}}
    agent = None
    async with pg.acquire() as conn:
        if lane == "pinned":
            agent = await conn.fetchval(
                "INSERT INTO agents(config_name,hostname,status) VALUES('developer',$1,'working') RETURNING id",
                f"idle-completion-{uuid4()}",
            )
        job_id = await conn.fetchval(
            "INSERT INTO jobs(description,status,execution_lane,assigned_agent_id,resolved_config,context,id,repo_name) "
            "VALUES('idle acceptance','processing',$1,$2,$3::jsonb,$4::jsonb,$5,$6) RETURNING id",
            lane,
            agent,
            json.dumps(policy),
            json.dumps(admitted_context),
            reserved_job_id,
            repository,
        )
        await seed_previous_release_row(
            conn,
            "jobs",
            "UPDATE jobs SET context=$2::jsonb,config_override=$3::jsonb WHERE id=$1",
            job_id,
            json.dumps(context),
            json.dumps(config),
        )
        if lane == "stateless":
            await conn.execute(
                "INSERT INTO run_queue(unit_id,unit_kind,state,lease_token,leased_by,last_leased_by,"
                "leased_until,attempts_since_completion,input_seq,consumed_seq) "
                "VALUES($1,'worker_batch','leased',71,'idle-worker','idle-worker',"
                "clock_timestamp()+interval '5 minutes',1,4,3)",
                job_id,
            )
            await conn.execute(
                "INSERT INTO worker_batch_attempts(job_id,lease_token,claimed_attempt) VALUES($1,71,1)",
                job_id,
            )
            if proof:
                digest = workspace_runtime_authority_digest(
                    {"context": context, "config_override": config},
                    vm_mode="same-cluster",
                )
                assert digest
                assert await record_worker_bundle_authorized(
                    conn, job_id=job_id, lease_token=71, authority_digest=digest
                )
    report = {
        "should_stop": True,
        "goal_achieved": False,
        "error": None,
        "freeze_data": {
            "job_id": str(job_id),
            "status": "pending_review",
            "freeze_type": "phase_boundary",
            "phase_type": "strategic",
            "phase_number": 2,
        },
    }
    return job_id, report, vm, agent


async def accept(pg, job_id, report, *, report_id=None, agent=None):
    return await accept_completion_command(
        pg,
        job_id=str(job_id),
        payload=report,
        lease_token=None if agent else 71,
        agent_id=str(agent) if agent else None,
        client_report_id=str(report_id or uuid4()),
        requested_by="idle-acceptance-test",
    )


@pytest.mark.asyncio
async def test_fast_pinned_phase_report_freezes_delivered_source_before_202(pg, monkeypatch):
    from pathlib import Path
    from orchestrator.database.postgres import PostgresDB
    from tests.test_completion_finalizer_real_postgres import _pool_db
    from shared.pinned_session_identity import PinnedJobRecipient
    from shared.pinned_job_delivery import pinned_job_delivery_proof
    from shared.workspace_contract import pinned_dispatch_authority_jsonb_sql

    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", "x" * 64)
    migration = (
        Path(__file__).resolve().parents[1]
        / "src/orchestrator/database/migrations/app/0275_vm_idle_pinned_job.sql"
    )
    async with pg.acquire() as conn:
        if not await conn.fetchval(
            "SELECT to_regclass('public.pinned_job_deliveries') IS NOT NULL"
        ):
            await conn.execute(migration.read_text())
    job_id, report, vm, agent_id = await seed(pg, lane="pinned")
    process_generation, pod_uid = uuid4(), uuid4()
    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE agents SET status='ready',pod_uid=$2,"
            "metadata=jsonb_build_object('dispatch_process_generation',$3::text) "
            "WHERE id=$1", agent_id, str(pod_uid), str(process_generation),
        )
        lease = await conn.fetchval("SELECT clock_timestamp()+interval '1 hour'")
        marker = pinned_dispatch_authority_jsonb_sql(
            agent_expr="$2::uuid", lease_expr="$3::timestamptz",
        )
        await conn.execute(
            "UPDATE jobs SET lease_expires_at=$3,"
            "context=context||jsonb_build_object('_workspace_dispatch_authority',"
            + marker + ") WHERE id=$1", job_id, agent_id, lease,
        )
    db = _pool_db(pg)
    digest = "sha256:" + "a" * 64
    intent = await PostgresDB.prepare_pinned_job_delivery(
        db, str(job_id), str(agent_id),
        recipient=PinnedJobRecipient(
            expected_agent_id=str(agent_id), expected_pod_uid=str(pod_uid),
            expected_process_generation=str(process_generation),
            expected_job_id=str(job_id),
        ),
        projection_digest=digest,
    )
    assert intent is not None and intent["accepted_at"] is None
    proof = pinned_job_delivery_proof(
        b"x" * 64, delivery_id=str(intent["id"]), agent_id=str(agent_id),
        process_generation=str(process_generation), pod_uid=str(pod_uid),
        projection_digest=digest,
    )
    accepted = await accept_completion_command(
        db, job_id=str(job_id), payload=report, lease_token=None,
        agent_id=str(agent_id), client_report_id=str(uuid4()),
        requested_by="fast-agent-report",
        pinned_delivery_id=intent["id"], pinned_projection_digest=digest,
        pinned_delivery_proof=proof,
        pinned_process_generation=str(process_generation),
        pinned_pod_uid=str(pod_uid),
    )
    source = accepted.stored_payload.get(KEY)
    assert source and source["pinned_delivery_id"] == str(intent["id"])
    assert source["runtime_identity"]["runtime_uid"] == vm["vm_uid"]
    async with pg.acquire() as conn:
        receipt = await conn.fetchrow(
            "SELECT * FROM pinned_job_wait_receipts WHERE source_kind='completion' "
            "AND source_id=$1", UUID(accepted.command_id),
        )
    assert receipt and receipt["delivery_id"] == intent["id"]
    from tests.test_completion_finalizer_real_postgres import _claimed_runner
    from tests.test_workspace_idle_completion_publish_real_postgres import through_status

    runner = await _claimed_runner(db, accepted.command_id)
    await through_status(monkeypatch, db, runner, report, agent_id=agent_id)
    async with pg.acquire() as conn:
        source_row = await conn.fetchrow(
            "SELECT status,workspace_idle_revision,workspace_idle_episode "
            "FROM jobs WHERE id=$1", job_id,
        )
    assert source_row["status"] == "pending_review"
    assert source_row["workspace_idle_revision"] == 1
    episode = json.loads(source_row["workspace_idle_episode"])
    assert episode["wait_kind"] == "human_approval"
    assert episode["wait_key"] == accepted.command_id
    # The fixture stops deliberately after the real S17 transaction. Finish
    # only the command state that the ordinary finalizer writes afterward.
    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE job_completion_commands SET state='done',outcome='{}'::jsonb,"
            "finalized_at=clock_timestamp() WHERE id=$1",
            UUID(accepted.command_id),
        )
    from orchestrator.services.vm_idle_phase_approval import finalized_phase_source
    from shared.workspace_idle_policy import read_episode

    async with pg.acquire() as conn:
        source_job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", job_id)
        published = read_episode(
            json.loads(source_job["workspace_idle_episode"]),
            revision=source_job["workspace_idle_revision"],
        )
        finalized = await finalized_phase_source(
            conn, job=source_job, episode=published,
            generation=UUID(vm["provision_generation"]),
            vm_uid=UUID(vm["vm_uid"]), vmi_uid=UUID(vm["vmi_uid"]),
            launcher_uid=UUID(vm["active_pod_uid"]),
            pvc_uid=UUID(vm["rootdisk_pvc_uid"]),
        )
    assert finalized and finalized["command_id"] == accepted.command_id
    from orchestrator.services.completion_control import CompletionControl
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore
    from orchestrator.services.vm_idle_phase_approval import approval_source_snapshot
    from unittest.mock import AsyncMock

    async with pg.acquire() as conn:
        receipt = await conn.fetchrow(
            "SELECT * FROM pinned_job_wait_receipts WHERE source_kind='completion' "
            "AND source_id=$1", UUID(accepted.command_id),
        )
        delivery = await conn.fetchrow(
            "SELECT * FROM pinned_job_deliveries WHERE id=$1", receipt["delivery_id"],
        )
        operation_id = await conn.fetchval(
            "INSERT INTO vm_idle_operations "
            "(owner_kind,owner_id,phase,episode_id,episode_revision,"
            "provision_generation,vm_uid,vmi_uid,launcher_uid,pvc_uid,retained_kind,"
            "release_kind,pinned_delivery_id,pinned_wait_receipt_id,pinned_agent_id,"
            "pinned_process_generation,pinned_agent_pod_name,pinned_agent_pod_namespace,"
            "pinned_agent_pod_uid,pinned_original_dispatch_marker,"
            "pinned_lease_observed_at,pinned_lease_expires_at) "
            "VALUES('job',$1,'releasing',$2,$3,$4,$5,$6,$7,$8,'rootdisk',"
            "'pinned_job',$9,$10,$11,$12,$13,$14,$15,$16::jsonb,$17,$18) "
            "RETURNING id",
            job_id, UUID(episode["episode_id"]), source_row["workspace_idle_revision"],
            UUID(vm["provision_generation"]), UUID(vm["vm_uid"]),
            UUID(vm["vmi_uid"]), UUID(vm["active_pod_uid"]),
            UUID(vm["rootdisk_pvc_uid"]), delivery["id"], receipt["id"],
            agent_id, delivery["process_generation"], delivery["pod_name"],
            delivery["pod_namespace"], delivery["pod_uid"],
            delivery["original_dispatch_marker"], receipt["observed_at"],
            receipt["lease_expires_at"],
        )
        vm["status"] = "suspending"
        vm["_suspend_remote_io_closed"] = str(operation_id)
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm}',$2::jsonb,true) "
            "WHERE id=$1", job_id, json.dumps(vm),
        )
        source_job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", job_id)
    snapshot = approval_source_snapshot(dict(source_job))
    claim = await CompletionControl(db, AsyncMock()).claim_job(
        job_id, source="public_approve", expected_status="pending_review",
        expected_lane="pinned",
    )
    control = CompletionControl(db, AsyncMock())
    async with control.finish_claim(claim) as (conn, _):
        wake = await VMIdleLifecycleStore(db).approve_phase_wake_on_conn(
            conn, job_id=str(job_id), claim_id=str(claim.claim_id),
            expected_source=snapshot,
        )
        assert wake is not None
    assert wake["wake_execution_requested"] is True


@pytest.mark.asyncio
async def test_fast_pinned_final_review_approves_immediate_exact_no_wake_stop(pg, monkeypatch):
    from pathlib import Path
    from unittest.mock import AsyncMock
    from orchestrator.database.postgres import PostgresDB
    from orchestrator.services.completion_control import CompletionControl
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore
    from orchestrator.services.vm_idle_phase_approval import review_source_snapshot
    from shared.pinned_session_identity import PinnedJobRecipient
    from shared.pinned_job_delivery import pinned_job_delivery_proof
    from shared.workspace_contract import pinned_dispatch_authority_jsonb_sql
    from tests.test_completion_finalizer_real_postgres import _pool_db, _claimed_runner
    from tests.test_workspace_idle_completion_publish_real_postgres import through_status

    for key, value in {
        "VM_LIFECYCLE_HMAC_SECRET": "x" * 64,
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)
    migration = (Path(__file__).resolve().parents[1]
                 / "src/orchestrator/database/migrations/app/0275_vm_idle_pinned_job.sql")
    async with pg.acquire() as conn:
        if not await conn.fetchval("SELECT to_regclass('public.pinned_job_deliveries') IS NOT NULL"):
            await conn.execute(migration.read_text())
    job_id, report, vm, agent_id = await seed(pg, lane="pinned")
    report["freeze_data"]["freeze_type"] = "job_complete"
    process_generation, pod_uid = uuid4(), uuid4()
    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{completion_decision}',"
            "$2::jsonb,true),resolved_config=$3::jsonb WHERE id=$1",
            job_id, json.dumps({"tool_call_id": "final-decision"}),
            json.dumps({"agent": {"autonomy": "review", "verification": {"enabled": False}}}),
        )
        await conn.execute(
            "UPDATE agents SET status='ready',pod_uid=$2,"
            "metadata=jsonb_build_object('dispatch_process_generation',$3::text) "
            "WHERE id=$1", agent_id, str(pod_uid), str(process_generation),
        )
        lease = await conn.fetchval("SELECT clock_timestamp()+interval '1 hour'")
        marker = pinned_dispatch_authority_jsonb_sql(
            agent_expr="$2::uuid", lease_expr="$3::timestamptz",
        )
        await conn.execute(
            "UPDATE jobs SET lease_expires_at=$3,"
            "context=context||jsonb_build_object('_workspace_dispatch_authority',"
            + marker + ") WHERE id=$1", job_id, agent_id, lease,
        )
    db = _pool_db(pg)
    digest = "sha256:" + "a" * 64
    intent = await PostgresDB.prepare_pinned_job_delivery(
        db, str(job_id), str(agent_id),
        recipient=PinnedJobRecipient(
            expected_agent_id=str(agent_id), expected_pod_uid=str(pod_uid),
            expected_process_generation=str(process_generation),
            expected_job_id=str(job_id),
        ), projection_digest=digest,
    )
    proof = pinned_job_delivery_proof(
        b"x" * 64, delivery_id=str(intent["id"]), agent_id=str(agent_id),
        process_generation=str(process_generation), pod_uid=str(pod_uid),
        projection_digest=digest,
    )
    accepted = await accept_completion_command(
        db, job_id=str(job_id), payload=report, lease_token=None,
        agent_id=str(agent_id), client_report_id=str(uuid4()),
        requested_by="fast-final-review",
        pinned_delivery_id=intent["id"], pinned_projection_digest=digest,
        pinned_delivery_proof=proof, pinned_process_generation=str(process_generation),
        pinned_pod_uid=str(pod_uid),
    )
    runner = await _claimed_runner(db, accepted.command_id)
    await through_status(monkeypatch, db, runner, report, agent_id=agent_id)
    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE job_completion_commands SET state='done',outcome='{}'::jsonb,"
            "finalized_at=clock_timestamp() WHERE id=$1", UUID(accepted.command_id),
        )
        job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", job_id)
    snapshot = review_source_snapshot(dict(job))
    assert snapshot is not None
    control = CompletionControl(db, AsyncMock())
    claim = await control.claim_job(
        job_id, source="public_approve", expected_status="pending_review",
        expected_lane="pinned", terminal_review_source=snapshot,
    )
    async with control.finish_claim(claim) as (conn, _):
        operation = await VMIdleLifecycleStore(db).approve_terminal_review_on_conn(
            conn, job_id=str(job_id), claim_id=str(claim.claim_id),
            expected_source=snapshot, publication={"version": 1, "job_id": str(job_id)},
        )
        assert operation is not None
        await conn.execute(
            "UPDATE jobs SET status='completed',assigned_agent_id=NULL "
            "WHERE id=$1", job_id,
        )
    assert operation["release_kind"] == "pinned_job"
    assert operation["terminal_source_command_id"] == UUID(accepted.command_id)
    assert not operation["wake_requested"]


@pytest.mark.asyncio
async def test_capture_binds_runtime_and_replay_never_recaptures(pg):
    job_id, report, vm, _ = await seed(pg)
    report_id = uuid4()
    first = await accept(
        pg, job_id, {**report, KEY: {"poison": True}}, report_id=report_id
    )
    marker = first.stored_payload.get(KEY)
    assert (
        marker
        and marker["version"] == 1
        and marker["semantics"]["branch"] == "phase_approval"
    )
    assert marker["runtime_identity"]["runtime_uid"] == vm["vm_uid"]
    assert (
        marker["runtime_identity"]["runtime_generation"] == vm["provision_generation"]
    )
    assert marker["launcher_uid"] == vm["active_pod_uid"]
    assert "ssh_host" not in json.dumps(marker) and "poison" not in marker
    async with pg.acquire() as conn:
        vm["vm_uid"] = str(uuid4())
        await seed_previous_release_row(
            conn,
            "jobs",
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
            job_id,
            json.dumps({"vm": vm}),
        )
    from orchestrator.services.job_completion_commands import CompletionInProgress

    with pytest.raises(CompletionInProgress):
        await accept(pg, job_id, report, report_id=report_id)
    async with pg.acquire() as conn:
        assert (
            json.loads(
                await conn.fetchval(
                    "SELECT payload FROM job_completion_commands WHERE id=$1",
                    UUID(first.command_id),
                )
            )[KEY]
            == marker
        )
        await conn.execute(
            "UPDATE job_completion_commands SET state='done',outcome='{}'::jsonb,finalized_at=clock_timestamp() WHERE id=$1",
            UUID(first.command_id),
        )
    replay = await accept(pg, job_id, report, report_id=report_id)
    assert replay.replayed and replay.stored_payload[KEY] == marker
    assert replay.command_id == first.command_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "missing_bundle",
        "old_generation",
        "refunded",
        "pinned",
        "disabled",
        "legacy_context",
    ],
)
async def test_unproven_execution_accepts_completion_without_idle_authority(
    pg, monkeypatch, case
):
    job_id, report, vm, agent = await seed(
        pg,
        proof=case != "missing_bundle",
        lane="pinned" if case == "pinned" else "stateless",
    )
    if case == "disabled":
        monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    async with pg.acquire() as conn:
        if case == "refunded":
            await conn.execute(
                "UPDATE worker_batch_attempts SET refunded_at=clock_timestamp(),refund_reason='test' WHERE job_id=$1",
                job_id,
            )
        elif case == "old_generation":
            vm["provision_generation"] = str(uuid4())
            vm["identity_provision_generation"] = vm["provision_generation"]
            await seed_previous_release_row(
                conn,
                "jobs",
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                job_id,
                json.dumps({"vm": vm}),
            )
        elif case == "legacy_context":
            await seed_previous_release_row(
                conn, "jobs", "UPDATE jobs SET context='[]'::jsonb WHERE id=$1", job_id
            )
    accepted = await accept(pg, job_id, {**report, KEY: {"poison": True}}, agent=agent)
    assert accepted.disposition == "fresh" and KEY not in accepted.stored_payload


@pytest.mark.asyncio
async def test_restart_body_does_not_expose_server_marker():
    from types import SimpleNamespace
    from orchestrator.services.job_completion import (
        run_persisted_completion_workflow,
        PersistedCompletionDependencies,
    )

    seen = []

    async def legacy(request, job_id, body, **kwargs):
        seen.append(body.model_dump())
        return {"ok": True}

    runner = SimpleNamespace(
        command={
            "job_id": uuid4(),
            "payload": {
                "should_stop": True,
                "goal_achieved": False,
                KEY: {"version": 1},
            },
            "accepted_lease_token": 71,
            "accepted_agent_id": None,
            "client_report_id": uuid4(),
        }
    )
    assert await run_persisted_completion_workflow(
        runner, dependencies=PersistedCompletionDependencies(legacy_complete=legacy)
    ) == {"ok": True}
    assert KEY not in seen[0]


@pytest.mark.asyncio
async def test_acceptance_rechecks_generation_after_job_lock_wait(pg):
    job_id, report, vm, _ = await seed(pg)
    task = None
    async with pg.acquire() as blocker:
        async with blocker.transaction():
            pid = await blocker.fetchval("SELECT pg_backend_pid()")
            await blocker.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job_id)
            task = asyncio.create_task(accept(pg, job_id, report))
            for _ in range(500):
                await blocker.execute("SELECT pg_stat_clear_snapshot()")
                waiting = await blocker.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
                    pid,
                )
                if waiting:
                    break
                await asyncio.sleep(0.01)
            assert waiting and not task.done()
            vm["provision_generation"] = str(uuid4())
            vm["identity_provision_generation"] = vm["provision_generation"]
            await seed_previous_release_row(
                blocker,
                "jobs",
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                job_id,
                json.dumps({"vm": vm}),
            )
    accepted = await asyncio.wait_for(task, timeout=10)
    assert KEY not in accepted.stored_payload


@pytest.mark.asyncio
async def test_capture_and_acceptance_roll_back_on_command_insert_failure(pg):
    job_id, report, _, _ = await seed(pg)
    async with pg.acquire() as conn:
        await conn.execute("""
            CREATE FUNCTION test_reject_idle_completion() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected command insert failure' USING ERRCODE='23514'; END $$;
            CREATE TRIGGER test_reject_idle_completion BEFORE INSERT ON job_completion_commands
            FOR EACH ROW EXECUTE FUNCTION test_reject_idle_completion();
        """)
    try:
        with pytest.raises(asyncpg.CheckViolationError, match="injected"):
            await accept(pg, job_id, report)
        async with pg.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM job_completion_commands WHERE job_id=$1",
                    job_id,
                )
                == 0
            )
            assert (
                await conn.fetchval(
                    "SELECT completion_seq_hwm FROM jobs WHERE id=$1", job_id
                )
                == 0
            )
            queue = await conn.fetchrow(
                "SELECT state,lease_token FROM run_queue WHERE unit_id=$1", job_id
            )
            assert (queue["state"], queue["lease_token"]) == ("leased", 71)
    finally:
        async with pg.acquire() as conn:
            await conn.execute(
                "DROP TRIGGER test_reject_idle_completion ON job_completion_commands; DROP FUNCTION test_reject_idle_completion()"
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("decision", [{"bad": "historical"}, True])
async def test_final_review_never_invents_a_tool_identity_from_legacy_json(
    pg, decision
):
    job_id, report, vm, _ = await seed(pg)
    report["freeze_data"]["freeze_type"] = "job_complete"
    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=$2::jsonb,resolved_config=$3::jsonb WHERE id=$1",
            job_id,
            json.dumps({"vm": vm, "completion_decision": {"tool_call_id": decision}}),
            json.dumps(
                {"agent": {"autonomy": "review", "verification": {"enabled": False}}}
            ),
        )
    accepted = await accept(pg, job_id, report)
    assert KEY not in accepted.stored_payload
