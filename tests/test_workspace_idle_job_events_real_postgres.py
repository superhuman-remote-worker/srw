"""Real human-route publication and execution Resume own the episode clocks."""

import json
from uuid import UUID, uuid4

import pytest

from tests.test_vm_creation_retry_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
)
from tests.test_completion_control_real_postgres import _agent
from tests.test_officer_message_routing_real_postgres import (
    _seed,
    _route_dict,
    _freeze,
    _MESSAGE_ENTRY,
)
from tests.test_vm_remote_operation_real_postgres import _vm_identity

db = _db_fixture


async def seeded(db, *, delivered=False, monkeypatch=None):
    seed = await _seed(db)
    vm, identities = _vm_identity()
    vm["ssh_ready_source"] = "provisioner_probe"
    vm["vmi_uid"] = str(uuid4())
    vm["rootdisk_pvc_uid"] = str(uuid4())
    async with db.acquire() as conn:
        agent = await _agent(conn)
        # Installing fixture authority follows the established prior-release
        # seed helper; the actual producer still proves this exact owner.
        from tests._previous_release_seed import seed_previous_release_row

        await seed_previous_release_row(
            conn,
            "jobs",
            "UPDATE jobs SET context=$2::jsonb,config_override=$3::jsonb WHERE id=$1",
            UUID(seed["job_id"]),
            json.dumps({"vm": vm}),
            json.dumps({"workspace": {"backend": "vm"}}),
        )
        from shared.workspace_contract import pinned_dispatch_authority_jsonb_sql

        authority = pinned_dispatch_authority_jsonb_sql(
            agent_expr="$2::uuid", lease_expr="$3::timestamptz"
        )
        lease = await conn.fetchval("SELECT clock_timestamp()+interval '1 hour'")
        await conn.execute(
            "UPDATE jobs SET assigned_agent_id=$2,lease_expires_at=$3,context=context||jsonb_build_object('_workspace_dispatch_authority',"
            + authority
            + ") WHERE id=$1",
            UUID(seed["job_id"]),
            agent,
            lease,
        )
    seed["agent_id"] = str(agent)
    if delivered:
        from pathlib import Path
        from shared.pinned_session_identity import PinnedJobRecipient
        from shared.pinned_job_delivery import pinned_job_delivery_proof

        assert monkeypatch is not None
        monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", "x" * 64)
        migration = (
            Path(__file__).resolve().parents[1]
            / "src/orchestrator/database/migrations/app/0275_vm_idle_pinned_job.sql"
        )
        if not await db.fetchval(
            "SELECT to_regclass('public.pinned_job_deliveries') IS NOT NULL"
        ):
            await db.execute(migration.read_text())
        process_generation, pod_uid = uuid4(), uuid4()
        await db.execute(
            "UPDATE agents SET status='ready',pod_uid=$2,"
            "metadata=jsonb_build_object('dispatch_process_generation',$3::text) "
            "WHERE id=$1", agent, str(pod_uid), str(process_generation),
        )
        digest = "sha256:" + "a" * 64
        intent = await db.prepare_pinned_job_delivery(
            seed["job_id"], str(agent),
            recipient=PinnedJobRecipient(
                expected_agent_id=str(agent), expected_pod_uid=str(pod_uid),
                expected_process_generation=str(process_generation),
                expected_job_id=seed["job_id"],
            ),
            projection_digest=digest,
        )
        assert intent is not None
        seed["delivery"] = {
            "id": intent["id"], "digest": digest,
            "process_generation": str(process_generation),
            "pod_uid": str(pod_uid),
            "proof": pinned_job_delivery_proof(
                b"x" * 64, delivery_id=str(intent["id"]),
                agent_id=str(agent), process_generation=str(process_generation),
                pod_uid=str(pod_uid), projection_digest=digest,
            ),
        }
    return seed, identities


async def publish(db, seed, *, state="user_direct", agent=None, lease_token=None):
    route = _route_dict(seed, state=state)
    result = await db.create_routed_blocking_freeze(
        seed["job_id"],
        _freeze(route),
        route=route,
        message_entry=_MESSAGE_ENTRY,
        expected_lane="stateless" if lease_token is not None else "pinned",
        lease_token=lease_token,
        agent_id=agent or seed["agent_id"],
        pinned_delivery_id=seed.get("delivery", {}).get("id"),
        pinned_projection_digest=seed.get("delivery", {}).get("digest"),
        pinned_delivery_proof=seed.get("delivery", {}).get("proof"),
        pinned_process_generation=seed.get("delivery", {}).get("process_generation"),
        pinned_pod_uid=seed.get("delivery", {}).get("pod_uid"),
        completion_commands_enabled=True,
    )
    return result, route


async def episode(db, job_id):
    row = await db.fetchrow(
        "SELECT workspace_idle_revision,workspace_idle_episode FROM jobs WHERE id=$1",
        UUID(job_id),
    )
    return row["workspace_idle_revision"], json.loads(
        row["workspace_idle_episode"]
    ) if row["workspace_idle_episode"] else None


@pytest.mark.asyncio
async def test_human_route_records_episode_with_exact_vm_and_reply_closes_it(
    db, monkeypatch
):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    seed, identities = await seeded(db, delivered=True, monkeypatch=monkeypatch)
    result, route = await publish(db, seed)
    assert result is not None
    revision, stored = await episode(db, seed["job_id"])
    assert revision == 1 and stored["wait_key"] == route["route_id"]
    assert stored["runtime_identity"]["runtime_uid"] == identities["vm_uid"]
    assert stored["runtime_identity"]["runtime_generation"] == identities["generation"]
    # A delayed different question must not consume the current episode.
    assert not await db.queue_job_for_resume(
        seed["job_id"],
        {},
        expected_status="waiting_for_reply",
        expected_route_id=str(uuid4()),
    )
    assert await episode(db, seed["job_id"]) == (revision, stored)
    assert await db.queue_job_for_resume(
        seed["job_id"],
        {},
        expected_status="waiting_for_reply",
        expected_route_id=route["route_id"],
    )
    assert await episode(db, seed["job_id"]) == (2, None)


@pytest.mark.asyncio
async def test_route_report_before_202_confirmation_joins_without_restamping(db, monkeypatch):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    seed, _ = await seeded(db, delivered=True, monkeypatch=monkeypatch)
    route_result, route = await publish(db, seed)
    assert route_result is not None
    before = await episode(db, seed["job_id"])
    assert before[1]["wait_key"] == route["route_id"]
    assert await db.confirm_pinned_job_dispatch(
        seed["job_id"], seed["agent_id"],
        pinned_delivery_id=str(seed["delivery"]["id"]),
        pinned_projection_digest=seed["delivery"]["digest"],
    )
    assert await episode(db, seed["job_id"]) == before
    assert not await db.confirm_pinned_job_dispatch(
        seed["job_id"], seed["agent_id"],
        pinned_delivery_id=str(uuid4()),
        pinned_projection_digest=seed["delivery"]["digest"],
    )


@pytest.mark.asyncio
async def test_202_confirmation_before_route_keeps_same_delivery(db, monkeypatch):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    seed, _ = await seeded(db, delivered=True, monkeypatch=monkeypatch)
    assert await db.confirm_pinned_job_dispatch(
        seed["job_id"], seed["agent_id"],
        pinned_delivery_id=str(seed["delivery"]["id"]),
        pinned_projection_digest=seed["delivery"]["digest"],
    )
    first = await db.fetchrow(
        "SELECT accepted_at,accepted_via FROM pinned_job_deliveries WHERE id=$1",
        seed["delivery"]["id"],
    )
    assert first["accepted_at"] is not None and first["accepted_via"] == "post"
    result, route = await publish(db, seed)
    assert result is not None
    assert (await episode(db, seed["job_id"]))[1]["wait_key"] == route["route_id"]
    second = await db.fetchrow(
        "SELECT accepted_at,accepted_via FROM pinned_job_deliveries WHERE id=$1",
        seed["delivery"]["id"],
    )
    assert second == first


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "disabled",
        "officer",
        "wrong_agent",
        "unattested",
        "missing_probe",
        "legacy_uid",
        "shared_owner",
        "list_status",
        "object_status",
    ],
)
async def test_only_authorized_proven_human_wait_enters_policy(db, monkeypatch, case):
    monkeypatch.setenv(
        "WORKSPACE_IDLE_RELEASE_ENABLED", "false" if case == "disabled" else "true"
    )
    monkeypatch.setenv("VM_MODE", "same-cluster")
    seed, _ = await seeded(db, delivered=True, monkeypatch=monkeypatch)
    if case == "unattested":
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,identity_authenticated}','false') WHERE id=$1",
            UUID(seed["job_id"]),
        )
    if case in {
        "missing_probe",
        "legacy_uid",
        "shared_owner",
        "list_status",
        "object_status",
    }:
        context = json.loads(
            await db.fetchval(
                "SELECT context FROM jobs WHERE id=$1", UUID(seed["job_id"])
            )
        )
        if case == "missing_probe":
            context["vm"].pop("ssh_ready_source")
        elif case == "legacy_uid":
            context["vm"]["vm_uid"] = "legacy-name-only"
        elif case in {"list_status", "object_status"}:
            context["vm"]["status"] = [] if case == "list_status" else {}
        else:
            context["inherits_parent_workspace"] = True
        await db.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
            UUID(seed["job_id"]),
            json.dumps(context),
        )
    result, _ = await publish(
        db,
        seed,
        state="pending_officer" if case == "officer" else "user_direct",
        agent=str(uuid4()) if case == "wrong_agent" else None,
    )
    assert (result is None) is (case == "wrong_agent")
    assert await episode(db, seed["job_id"]) == (0, None)


@pytest.mark.asyncio
async def test_disabling_tracking_does_not_keep_old_episode_after_authorized_resume(
    db, monkeypatch
):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    seed, _ = await seeded(db, delivered=True, monkeypatch=monkeypatch)
    _, route = await publish(db, seed)
    assert (await episode(db, seed["job_id"]))[0] == 1
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    assert await db.queue_job_for_resume(
        seed["job_id"],
        {},
        expected_status="waiting_for_reply",
        expected_route_id=route["route_id"],
    )
    assert await episode(db, seed["job_id"]) == (2, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("stale", [False, True])
async def test_stateless_route_requires_the_current_queue_token(db, monkeypatch, stale):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    seed, _ = await seeded(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='paused',execution_lane='stateless',assigned_agent_id=NULL WHERE id=$1",
            UUID(seed["job_id"]),
        )
        await conn.execute(
            "INSERT INTO run_queue (unit_id,unit_kind,state,lease_token,leased_by,leased_until,input_seq,consumed_seq) "
            "VALUES ($1,'worker_batch','leased',7,'fixture-worker',clock_timestamp()+interval '5 minutes',1,0)",
            UUID(seed["job_id"]),
        )
        from shared.worker_queue import _CAS_JOB_SQL

        leased_until = await conn.fetchval(
            "SELECT leased_until FROM run_queue WHERE unit_id=$1", UUID(seed["job_id"])
        )
        assert await conn.fetchval(
            _CAS_JOB_SQL,
            UUID(seed["job_id"]),
            "paused",
            "fixture-worker",
            7,
            leased_until,
        ) == UUID(seed["job_id"])
    result, route = await publish(db, seed, lease_token=6 if stale else 7)
    if stale:
        assert result is None and await episode(db, seed["job_id"]) == (0, None)
    else:
        assert result is not None
        revision, stored = await episode(db, seed["job_id"])
        assert revision == 1 and stored["wait_key"] == route["route_id"]


@pytest.mark.asyncio
async def test_episode_and_message_publication_roll_back_together(db, monkeypatch):
    from orchestrator.services import workspace_idle_events

    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    seed, _ = await seeded(db, delivered=True, monkeypatch=monkeypatch)
    real = workspace_idle_events.record_human_route_wait_on_conn

    async def fault_after_episode(conn, **kwargs):
        assert await real(conn, **kwargs)
        raise RuntimeError("fixture commit failure")

    monkeypatch.setattr(
        workspace_idle_events, "record_human_route_wait_on_conn", fault_after_episode
    )
    with pytest.raises(RuntimeError, match="fixture commit failure"):
        await publish(db, seed)
    assert await episode(db, seed["job_id"]) == (0, None)
    row = await db.fetchrow(
        "SELECT status,freeze_data FROM jobs WHERE id=$1", UUID(seed["job_id"])
    )
    assert row["status"] == "processing" and row["freeze_data"] is None
    for table in ("message_log", "job_message_routes"):
        assert await db.fetchval("SELECT count(*) FROM " + table) == 0


@pytest.mark.asyncio
async def test_reprovisioning_does_not_close_pending_human_episode(db, monkeypatch):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    seed, _ = await seeded(db, delivered=True, monkeypatch=monkeypatch)
    _, route = await publish(db, seed)
    previous = await episode(db, seed["job_id"])
    assert await db.queue_job_for_resume(
        seed["job_id"],
        {},
        void_completion_decision=False,
        expected_status="waiting_for_reply",
        expected_route_id=route["route_id"],
    )
    assert await episode(db, seed["job_id"]) == previous
