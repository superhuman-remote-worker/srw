"""Exercise the actual completion workflow through its transactional S17 effect."""

from dataclasses import replace
import json
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from tests.test_workspace_idle_completion_accept_real_postgres import (
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    tracking,  # noqa: F401
    pg as _pg_fixture,
    seed,
    accept,
)
from tests.test_completion_finalizer_real_postgres import _pool_db, _claimed_runner
from orchestrator import main
from orchestrator.schemas.job_runtime import JobCompleteRequest
from orchestrator.services.legacy_job_completion import complete_job_legacy

pg = _pg_fixture


@pytest.fixture
def repository_crypto(monkeypatch):
    from orchestrator.security import crypto

    monkeypatch.setenv("APP_ENCRYPTION_KEY", "G" * 32)
    crypto.reset_cipher_cache()
    yield
    crypto.reset_cipher_cache()


class StatusCommitted(BaseException):
    """Stop the fixture after the real S17 transaction has committed."""


async def through_status(
    monkeypatch, db, runner, report, *, before_status=None, forge=None,
    agent_id=None,
):
    monkeypatch.setattr(main, "postgres_db", db)
    monkeypatch.setattr(
        main,
        "gitea_client",
        forge if forge is not None else SimpleNamespace(is_initialized=False),
    )
    monkeypatch.setattr(main, "vector_db", None)
    dependencies = main._legacy_completion_dependencies()
    original = dependencies.effects.run

    async def effects(effect_runner, name, group, callback, **kwargs):
        if name == "main_status_write" and before_status is not None:
            await before_status()
        result = await original(effect_runner, name, group, callback, **kwargs)
        if name == "main_status_write":
            raise StatusCommitted()
        return result

    dependencies = replace(
        dependencies, effects=replace(dependencies.effects, run=effects)
    )
    with pytest.raises(StatusCommitted):
        await complete_job_legacy(
            None,
            str(runner.command["job_id"]),
            JobCompleteRequest(
                **report, lease_token=None if agent_id else 71,
                agent_id=agent_id,
            ),
            dependencies=dependencies,
            _authorized=True,
            _effect_runner=runner,
        )


@pytest.mark.asyncio
async def test_actual_phase_completion_publishes_once_with_status_and_effect(
    pg, monkeypatch
):
    job_id, report, _, _ = await seed(pg)
    accepted = await accept(pg, job_id, report)
    db = _pool_db(pg)
    runner = await _claimed_runner(db, accepted.command_id)
    await through_status(monkeypatch, db, runner, report)
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status::text,workspace_idle_revision,workspace_idle_episode FROM jobs WHERE id=$1",
            job_id,
        )
        assert row["status"] == "pending_review"
        assert row["workspace_idle_revision"] == 1
        episode = json.loads(row["workspace_idle_episode"])
        assert (
            episode["wait_key"] == accepted.command_id
            and episode["wait_kind"] == "human_approval"
        )
        assert (
            await conn.fetchval(
                "SELECT state FROM completion_effects WHERE producer_id=$1 AND effect_name='main_status_write'",
                UUID(accepted.command_id),
            )
            == "done"
        )
    await through_status(monkeypatch, db, runner, report)
    async with pg.acquire() as conn:
        replay = await conn.fetchrow(
            "SELECT workspace_idle_revision,workspace_idle_episode FROM jobs WHERE id=$1",
            job_id,
        )
        assert replay["workspace_idle_revision"] == 1
        assert json.loads(replay["workspace_idle_episode"]) == episode


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_at", ["capture", "finalization"])
async def test_missing_retained_pvc_never_publishes_s17_wait(
    pg, monkeypatch, missing_at,
):
    from tests._previous_release_seed import seed_previous_release_row

    job_id, report, vm, _ = await seed(pg)
    if missing_at == "capture":
        vm["rootdisk_pvc_uid"] = None
        async with pg.acquire() as conn:
            await seed_previous_release_row(
                conn, "jobs", "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                job_id, json.dumps({"vm": vm}),
            )
    accepted = await accept(pg, job_id, report)
    if missing_at == "capture":
        assert "_accepted_idle_wait_source" not in accepted.stored_payload
    db = _pool_db(pg)
    runner = await _claimed_runner(db, accepted.command_id)

    async def lose_pvc():
        if missing_at == "finalization":
            vm["rootdisk_pvc_uid"] = None
            async with pg.acquire() as conn:
                await seed_previous_release_row(
                    conn, "jobs", "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                    job_id, json.dumps({"vm": vm}),
                )

    await through_status(monkeypatch, db, runner, report, before_status=lose_pvc)
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status::text,workspace_idle_revision,workspace_idle_episode "
            "FROM jobs WHERE id=$1", job_id,
        )
        assert row["status"] == "pending_review"
        assert row["workspace_idle_revision"] == 0
        assert row["workspace_idle_episode"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift",
    [
        "generation",
        "vm_uid",
        "launcher",
        "pin",
        "policy",
        "freeze",
        "disabled",
        "branch",
    ],
)
async def test_finalization_cannot_attach_old_completion_wait_to_changed_source(
    pg, monkeypatch, drift
):
    from tests._previous_release_seed import seed_previous_release_row
    from orchestrator.services import completion
    from orchestrator.services.deliverable_gate import DeliverableGateResult

    job_id, report, vm, _ = await seed(pg)
    accepted = await accept(pg, job_id, report)
    db = _pool_db(pg)
    runner = await _claimed_runner(db, accepted.command_id)
    if drift == "branch":

        async def gate(*args, **kwargs):
            return DeliverableGateResult(
                "pending_review", ["delivery failure fallback"], False
            )

        monkeypatch.setattr(completion, "apply_deliverable_gate", gate)

    async def mutate():
        if drift == "disabled":
            monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
            return
        async with pg.acquire() as conn:
            if drift in {"generation", "vm_uid", "launcher", "pin"}:
                key = {
                    "generation": "provision_generation",
                    "vm_uid": "vm_uid",
                    "launcher": "active_pod_uid",
                    "pin": "ssh_host_key_fingerprint",
                }[drift]
                vm[key] = "SHA256:" + "B" * 43 if drift == "pin" else str(uuid4())
                if drift == "generation":
                    vm["identity_provision_generation"] = vm[key]
                await seed_previous_release_row(
                    conn,
                    "jobs",
                    "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                    job_id,
                    json.dumps({"vm": vm}),
                )
            elif drift == "policy":
                await conn.execute(
                    "UPDATE jobs SET resolved_config=$2::jsonb WHERE id=$1",
                    job_id,
                    json.dumps(
                        {
                            "agent": {
                                "autonomy": "dependent",
                                "verification": {"enabled": True},
                            }
                        }
                    ),
                )
            elif drift == "freeze":
                await conn.execute(
                    "UPDATE jobs SET freeze_data=$2::jsonb WHERE id=$1",
                    job_id,
                    json.dumps({**report["freeze_data"], "phase_number": 99}),
                )

    await through_status(monkeypatch, db, runner, report, before_status=mutate)
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status::text,workspace_idle_revision,workspace_idle_episode FROM jobs WHERE id=$1",
            job_id,
        )
        assert row["status"] == "pending_review"
        assert (row["workspace_idle_revision"], row["workspace_idle_episode"]) == (
            0,
            None,
        )


@pytest.mark.asyncio
async def test_sql_failure_rolls_back_status_episode_and_effect_then_retry_enters_once(
    pg, monkeypatch
):
    from fastapi import HTTPException

    job_id, report, _, _ = await seed(pg)
    accepted = await accept(pg, job_id, report)
    db = _pool_db(pg)
    runner = await _claimed_runner(db, accepted.command_id)
    async with pg.acquire() as conn:
        await conn.execute("""
            CREATE FUNCTION test_fail_idle_publication() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.workspace_idle_revision <> OLD.workspace_idle_revision THEN
                RAISE EXCEPTION 'injected idle publication failure' USING ERRCODE='23514';
            END IF; RETURN NEW; END $$;
            CREATE TRIGGER test_fail_idle_publication BEFORE UPDATE ON jobs
            FOR EACH ROW EXECUTE FUNCTION test_fail_idle_publication();
        """)
    try:
        with pytest.raises(HTTPException, match="injected idle publication failure"):
            await through_status(monkeypatch, db, runner, report)
        async with pg.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT status::text,workspace_idle_revision,workspace_idle_episode FROM jobs WHERE id=$1",
                job_id,
            )
            assert row["status"] == "processing"
            assert (row["workspace_idle_revision"], row["workspace_idle_episode"]) == (
                0,
                None,
            )
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM completion_effects WHERE producer_id=$1 AND effect_name='main_status_write'",
                    UUID(accepted.command_id),
                )
                == 0
            )
    finally:
        async with pg.acquire() as conn:
            await conn.execute(
                "DROP TRIGGER test_fail_idle_publication ON jobs; DROP FUNCTION test_fail_idle_publication()"
            )
    await through_status(monkeypatch, db, runner, report)
    async with pg.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT workspace_idle_revision FROM jobs WHERE id=$1", job_id
            )
            == 1
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "decision_drift", [None, "different-decision", {"bad": "legacy"}]
)
async def test_actual_final_human_review_uses_accepted_decision(
    pg, monkeypatch, decision_drift
):
    job_id, report, vm, _ = await seed(pg)
    report["freeze_data"]["freeze_type"] = "job_complete"
    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=$2::jsonb,resolved_config=$3::jsonb WHERE id=$1",
            job_id,
            json.dumps(
                {"vm": vm, "completion_decision": {"tool_call_id": "original-decision"}}
            ),
            json.dumps(
                {"agent": {"autonomy": "review", "verification": {"enabled": False}}}
            ),
        )
    accepted = await accept(pg, job_id, report)
    db = _pool_db(pg)
    runner = await _claimed_runner(db, accepted.command_id)

    async def mutate_decision():
        if decision_drift is not None:
            async with pg.acquire() as conn:
                await conn.execute(
                    "UPDATE jobs SET context=jsonb_set(context,'{completion_decision}', $2::jsonb) WHERE id=$1",
                    job_id,
                    json.dumps({"tool_call_id": decision_drift}),
                )

    await through_status(monkeypatch, db, runner, report, before_status=mutate_decision)
    async with pg.acquire() as conn:
        raw = await conn.fetchval(
            "SELECT workspace_idle_episode FROM jobs WHERE id=$1", job_id
        )
        if decision_drift is not None:
            assert raw is None
        else:
            episode = json.loads(raw)
            assert (
                episode["wait_kind"] == "human_review"
                and episode["wait_key"] == accepted.command_id
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["verified", "unreadable", "unverified"])
async def test_actual_delivery_gate_preserves_only_verified_original_human_review(
    pg, monkeypatch, case, repository_crypto
):
    from tests.test_deliverable_gate import make_gitea

    manifest = ["kb:answer"] if case == "unverified" else ["answer.txt"]
    job_id, report, vm, _ = await seed(
        pg, manifest=manifest, repository=f"idle-gate-{uuid4()}"
    )
    report["freeze_data"]["freeze_type"] = "job_complete"
    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=context||$2::jsonb,resolved_config=$3::jsonb WHERE id=$1",
            job_id,
            json.dumps(
                {
                    "vm": vm,
                    "completion_decision": {"tool_call_id": "decision-original"},
                    "required_deliverables": manifest,
                }
            ),
            json.dumps(
                {"agent": {"autonomy": "review", "verification": {"enabled": False}}}
            ),
        )
    accepted = await accept(pg, job_id, report)
    db = _pool_db(pg)
    runner = await _claimed_runner(db, accepted.command_id)
    await through_status(
        monkeypatch,
        db,
        runner,
        report,
        forge=make_gitea(None if case == "unreadable" else ["answer.txt"]),
    )
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status::text,workspace_idle_episode FROM jobs WHERE id=$1", job_id
        )
        assert row["status"] == "pending_review"
        assert (row["workspace_idle_episode"] is not None) is (case == "verified")
        detail = json.loads(
            await conn.fetchval(
                "SELECT detail FROM completion_effects WHERE producer_id=$1 AND effect_name='deliverable_contract_gate'",
                UUID(accepted.command_id),
            )
        )
        assert detail["output"]["preserves_human_wait"] is (case == "verified")


@pytest.mark.asyncio
@pytest.mark.parametrize("replace_pin", [False, True])
async def test_s17_rechecks_identity_and_samples_clock_after_job_lock_wait(
    pg, monkeypatch, replace_pin
):
    import asyncio
    from datetime import datetime

    job_id, report, vm, _ = await seed(pg)
    accepted = await accept(pg, job_id, report)
    db = _pool_db(pg)
    runner = await _claimed_runner(db, accepted.command_id)
    arrived, release = asyncio.Event(), asyncio.Event()

    async def before_status():
        arrived.set()
        await release.wait()

    task = asyncio.create_task(
        through_status(monkeypatch, db, runner, report, before_status=before_status)
    )
    try:
        await asyncio.wait_for(arrived.wait(), 15)
        async with pg.acquire() as blocker:
            async with blocker.transaction():
                pid = await blocker.fetchval("SELECT pg_backend_pid()")
                await blocker.fetchrow(
                    "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job_id
                )
                release.set()
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
                if replace_pin:
                    vm["ssh_host_key_fingerprint"] = "SHA256:" + "C" * 43
                    await blocker.execute(
                        "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                        job_id,
                        json.dumps({"vm": vm}),
                    )
                released_after = await blocker.fetchval("SELECT clock_timestamp()")
        await asyncio.wait_for(task, 15)
        async with pg.acquire() as conn:
            raw = await conn.fetchval(
                "SELECT workspace_idle_episode FROM jobs WHERE id=$1", job_id
            )
            if replace_pin:
                assert raw is None
            else:
                assert (
                    datetime.fromisoformat(json.loads(raw)["entered_at"])
                    >= released_after
                )
    finally:
        release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
