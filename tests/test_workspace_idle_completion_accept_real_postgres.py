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
