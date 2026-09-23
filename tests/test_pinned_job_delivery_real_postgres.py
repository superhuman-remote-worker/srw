"""A pinned VM Job receives one durable, non-authorizing pre-POST intent."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from shared.pinned_session_identity import PinnedJobRecipient
from tests.test_b05_lane_j_job_preparation import (
    READY_VM, _bundle_job, _start_bundle_deps, bundle_env,  # noqa: F401
)
from tests.test_vm_idle_lifecycle_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    profiled_idle_image_policy,  # noqa: F401
    seed_wait,
)

db = _db_fixture


MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "src/orchestrator/database/migrations/app/0275_vm_idle_pinned_job.sql"
)


async def _schema(db):
    if not await db.fetchval("SELECT to_regclass('public.pinned_job_deliveries') IS NOT NULL"):
        await db.execute(MIGRATION.read_text())


async def _pinned_claim(db):
    owner, _, identity = await seed_wait(db)
    agent_id, process_generation, pod_uid = uuid4(), uuid4(), uuid4()
    await db.execute(
        "INSERT INTO agents(id,config_name,hostname,status,pod_uid,metadata) "
        "VALUES($1,'worker_base','pinned-agent','ready',$2,$3::jsonb)",
        agent_id, str(pod_uid),
        json.dumps({"dispatch_process_generation": str(process_generation)}),
    )
    await db.execute(
        "UPDATE jobs SET status='created',execution_lane='pinned',"
        "freeze_data=NULL "
        "WHERE id=$1", owner,
    )
    assert await db.claim_job_for_agent(str(owner), str(agent_id))
    return owner, agent_id, process_generation, pod_uid, identity


@pytest.mark.asyncio
async def test_vm_start_wire_accepts_current_delivery_intent(db, monkeypatch):
    """A VM bundle must reach the exact wire receipt, not the sandbox discriminator."""
    from agent.api.models import JobStartRequest
    from agent.api.pinned_delivery import accepted_pinned_job_delivery
    from orchestrator.services.job_control_delivery import _pinned_vm_delivery_intent

    await _schema(db)
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", "x" * 64)
    owner, agent_id, generation, pod_uid, _ = await _pinned_claim(db)
    recipient = PinnedJobRecipient(
        expected_agent_id=str(agent_id), expected_pod_uid=str(pod_uid),
        expected_process_generation=str(generation), expected_job_id=str(owner),
    )
    bundle = JobStartRequest(
        job_id=str(owner), description="real VM dispatch",
        workspace_runtime={"effective_backend": "vm", "assigned_backend": "vm", "state": "ready"},
        workspace_provisioner="vm", recipient=recipient,
    )
    payload, intent = await _pinned_vm_delivery_intent(
        SimpleNamespace(store=db), job_id=str(owner), agent_id=str(agent_id),
        recipient=recipient, payload=bundle.model_dump(mode="json", exclude_none=True),
    )
    assert intent is not None
    wire = JobStartRequest.model_validate(payload)
    acknowledgement = accepted_pinned_job_delivery(
        wire, SimpleNamespace(), retry=False,
    )
    assert acknowledgement["pinned_delivery_id"] == str(intent["id"])
    assert await db.confirm_pinned_job_dispatch(
        str(owner), str(agent_id), pinned_delivery_id=acknowledgement["pinned_delivery_id"],
        pinned_projection_digest=acknowledgement["pinned_projection_digest"],
    )
    changed_payload, changed_intent = await _pinned_vm_delivery_intent(
        SimpleNamespace(store=db), job_id=str(owner), agent_id=str(agent_id),
        recipient=recipient, payload={**payload, "description": "changed projection"},
    )
    assert changed_payload is None and changed_intent is None


@pytest.mark.asyncio
async def test_real_vm_start_dispatch_posts_accepted_receipt(db, monkeypatch, bundle_env):  # noqa: F811
    from agent.api.models import JobStartRequest
    from agent.api.pinned_delivery import accepted_pinned_job_delivery
    from orchestrator.services.job_control_delivery import (
        JobDeliveryDependencies, dispatch_job_to_agent,
    )
    from orchestrator.services.job_start_bundle import build_job_start_request

    await _schema(db)
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", "x" * 64)
    owner, agent_id, generation, pod_uid, _ = await _pinned_claim(db)
    recipient = PinnedJobRecipient(
        expected_agent_id=str(agent_id), expected_pod_uid=str(pod_uid),
        expected_process_generation=str(generation), expected_job_id=str(owner),
    )
    job = _bundle_job(
        backend="vm", id=str(owner), runtime_kind="srw", execution_lane="pinned",
    )
    job["context"]["vm"] = {
        **READY_VM,
        "provision_generation": "22222222-2222-2222-2222-222222222222",
        "ssh_ready_source": "provisioner_probe",
    }
    bundle = await build_job_start_request(job, dependencies=_start_bundle_deps())
    assert bundle is not None
    assert bundle.workspace_provisioner == "kubevirt"
    assert bundle.workspace_runtime["effective_backend"] == "vm"
    client_state = SimpleNamespace()

    class AcceptedAgent:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, url, json):
            assert url.endswith("/job/start")
            receipt = accepted_pinned_job_delivery(
                JobStartRequest.model_validate(json), client_state, retry=False,
            )
            return SimpleNamespace(status_code=202, json=lambda: receipt)

    store = SimpleNamespace(
        prepare_pinned_job_delivery=db.prepare_pinned_job_delivery,
        confirm_pinned_job_dispatch=db.confirm_pinned_job_dispatch,
        managed_repository_authorities_are_current=AsyncMock(return_value=True),
        heartbeat=db.heartbeat,
    )
    values = {
        field: MagicMock() for field in JobDeliveryDependencies.__dataclass_fields__
    }
    values.update(
        store=store, logger=logging.getLogger(__name__),
        completion_commands_enabled=lambda: True,
        pause_pending_job_ids=set(),
        prepare_job_workspace_runtime=AsyncMock(side_effect=lambda job: ("proceed", job, None)),
        attest_pinned_k8s_job_workspace=AsyncMock(side_effect=lambda job: (job, None)),
        build_job_start_request=AsyncMock(return_value=bundle),
        pinned_k8s_job_workspace_authority_is_current=AsyncMock(return_value=True),
        prepare_pinned_job_mutation_target=AsyncMock(return_value=SimpleNamespace(
            agent={"pod_ip": "10.0.0.2", "pod_port": 8001}, recipient=recipient,
        )),
        redispatch_livelock_trip=lambda _job: None,
        bind_log_context=lambda **_kwargs: None,
        reset_log_context=lambda _token: None,
        http_client_factory=lambda **_kwargs: AcceptedAgent(),
    )
    assert await dispatch_job_to_agent(
        job,
        {"id": str(agent_id), "pod_ip": "10.0.0.2"},
        dependencies=JobDeliveryDependencies(**values),
    )
    assert client_state.pinned_delivery_id == str(await db.fetchval(
        "SELECT id FROM pinned_job_deliveries WHERE job_id=$1", owner,
    ))


@pytest.mark.asyncio
@pytest.mark.parametrize("newer_group", [None, "feedback", "delegation", "both"])
async def test_vm_resume_wire_accepts_exact_current_claim(
    db, monkeypatch, newer_group,
):
    """A checkpoint resume gets a new receipt after the dispatch marker rotates."""
    from agent.api.models import JobResumeRequest
    from agent.api.pinned_delivery import accepted_pinned_job_delivery
    from orchestrator.services.job_control_delivery import _pinned_vm_delivery_intent

    await _schema(db)
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", "x" * 64)
    owner, agent_id, generation, pod_uid, _ = await _pinned_claim(db)
    recipient = PinnedJobRecipient(
        expected_agent_id=str(agent_id), expected_pod_uid=str(pod_uid),
        expected_process_generation=str(generation), expected_job_id=str(owner),
    )
    base = JobResumeRequest(
        job_id=str(owner), previous_status="paused",
        workspace_runtime={"effective_backend": "vm", "assigned_backend": "vm", "state": "ready"},
        workspace_provisioner="vm", recipient=recipient,
    ).model_dump(mode="json", exclude_none=True)
    first_payload, first = await _pinned_vm_delivery_intent(
        SimpleNamespace(store=db), job_id=str(owner), agent_id=str(agent_id),
        recipient=recipient, payload=base,
    )
    assert first is not None
    assert accepted_pinned_job_delivery(
        JobResumeRequest.model_validate(first_payload), SimpleNamespace(), retry=False,
    )["pinned_delivery_id"] == str(first["id"])
    await db.execute(
        "UPDATE jobs SET status='paused',assigned_agent_id=NULL WHERE id=$1", owner,
    )
    await db.execute(
        "UPDATE agents SET status='ready',current_job_id=NULL WHERE id=$1", agent_id,
    )
    assert await db.claim_job_for_agent(str(owner), str(agent_id))
    delivered = {
        "queued_feedback": "new reply",
        "queued_feedback_reason": "human reply",
        "queued_feedback_delivery_id": str(uuid4()),
        "delegation_results": [{"job_id": "child-1", "status": "completed"}],
        "delegation_results_delivery_id": str(uuid4()),
    }
    await db.execute(
        "UPDATE jobs SET context=context || $2::jsonb WHERE id=$1",
        owner, json.dumps(delivered),
    )
    changed = {
        **base, "feedback": "new reply", "feedback_reason": "human reply",
        "delegation_results": delivered["delegation_results"],
    }
    second_payload, second = await _pinned_vm_delivery_intent(
        SimpleNamespace(store=db), job_id=str(owner), agent_id=str(agent_id),
        recipient=recipient, payload=changed, consumed_context=delivered,
    )
    assert second is not None and second["id"] != first["id"]
    client = SimpleNamespace()
    acknowledgement = accepted_pinned_job_delivery(
        JobResumeRequest.model_validate(second_payload), client, retry=False,
    )
    assert acknowledgement["pinned_delivery_id"] == str(second["id"])
    newer = {
        "queued_feedback": "new reply",  # Even identical content is a new delivery.
        "queued_feedback_reason": "human reply",
        "queued_feedback_delivery_id": str(uuid4()),
        "delegation_results": delivered["delegation_results"],
        "delegation_results_delivery_id": str(uuid4()),
    }
    concurrent = {
        key: value for key, value in newer.items()
        if newer_group == "both"
        or (newer_group == "feedback" and key.startswith("queued_feedback"))
        or (newer_group == "delegation" and key.startswith("delegation_results"))
    }
    if concurrent:
        assert await db.merge_job_context(
            str(owner), {
                key: value for key, value in concurrent.items()
                if not key.endswith("_delivery_id")
            },
        )
        produced = json.loads(await db.fetchval(
            "SELECT context FROM jobs WHERE id=$1", owner,
        ))
        concurrent = {key: produced[key] for key in concurrent}
        for key in concurrent:
            if key.endswith("_delivery_id"):
                assert concurrent[key] != delivered[key]
    route_id = UUID(json.loads(await db.fetchval(
        "SELECT workspace_idle_episode FROM jobs WHERE id=$1", owner,
    ))["wait_key"])
    assert await _publish_fast_route(
        db, owner=owner, agent_id=agent_id, intent=second,
        process_generation=generation, pod_uid=pod_uid,
        proof=second_payload["pinned_delivery_proof"], route_id=route_id,
    )
    context_at_receipt = json.loads(await db.fetchval(
        "SELECT context FROM jobs WHERE id=$1", owner,
    ))
    assert context_at_receipt.items() >= concurrent.items()
    assert all(key not in context_at_receipt for key in delivered if key not in concurrent)
    assert await db.confirm_pinned_job_dispatch(
        str(owner), str(agent_id), pinned_delivery_id=acknowledgement["pinned_delivery_id"],
        pinned_projection_digest=acknowledgement["pinned_projection_digest"],
        consumed_context=delivered,
    )
    context_after = json.loads(await db.fetchval(
        "SELECT context FROM jobs WHERE id=$1", owner,
    ))
    assert context_after.items() >= concurrent.items()
    assert all(key not in context_after for key in delivered if key not in concurrent)
    assert await db.fetchval(
        "SELECT delivery_id FROM pinned_job_wait_receipts WHERE source_kind='route' "
        "AND source_id=$1", route_id,
    ) == second["id"]
    later = {
        "queued_feedback": "later reply",
        "queued_feedback_reason": "later reason",
        "queued_feedback_delivery_id": str(uuid4()),
        "delegation_results": [{"job_id": "child-2", "status": "completed"}],
        "delegation_results_delivery_id": str(uuid4()),
    }
    await db.execute(
        "UPDATE jobs SET context=context || $2::jsonb WHERE id=$1",
        owner, json.dumps(later),
    )
    assert await db.confirm_pinned_job_dispatch(
        str(owner), str(agent_id), pinned_delivery_id=acknowledgement["pinned_delivery_id"],
        pinned_projection_digest=acknowledgement["pinned_projection_digest"],
        consumed_context=delivered,
    )
    assert json.loads(await db.fetchval(
        "SELECT context FROM jobs WHERE id=$1", owner,
    )).items() >= later.items()
    assert not await db.confirm_pinned_job_dispatch(
        str(owner), str(agent_id), pinned_delivery_id=acknowledgement["pinned_delivery_id"],
        pinned_projection_digest="sha256:" + "0" * 64,
        consumed_context=later,
    )
    changed_payload, changed_intent = await _pinned_vm_delivery_intent(
        SimpleNamespace(store=db), job_id=str(owner), agent_id=str(agent_id),
        recipient=recipient, payload={**changed, "feedback": "different second projection"},
    )
    assert changed_payload is None and changed_intent is None


@pytest.mark.asyncio
async def test_pre_post_intent_is_exact_and_not_a_delivery_receipt(db):
    await _schema(db)
    owner, agent_id, process_generation, pod_uid, identity = await _pinned_claim(db)
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
    assert intent["accepted_at"] is None
    assert intent["agent_id"] == agent_id
    assert str(intent["vm_uid"]) == identity["vm_uid"]
    replay = await db.prepare_pinned_job_delivery(
        str(owner), str(agent_id), recipient=recipient,
        projection_digest=digest,
    )
    assert replay["id"] == intent["id"]
    assert await db.prepare_pinned_job_delivery(
        str(owner), str(agent_id), recipient=recipient,
        projection_digest="sha256:" + "b" * 64,
    ) is None


@pytest.mark.asyncio
async def test_fast_authenticated_report_accepts_intent_before_post_reply(db, monkeypatch):
    from orchestrator.services.pinned_job_delivery import accept_pinned_report_on_conn
    from shared.pinned_job_delivery import pinned_job_delivery_proof

    await _schema(db)
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", "x" * 64)
    owner, agent_id, process_generation, pod_uid, _ = await _pinned_claim(db)
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
    assert intent["accepted_at"] is None
    proof = pinned_job_delivery_proof(
        b"x" * 64, delivery_id=str(intent["id"]), agent_id=str(agent_id),
        process_generation=str(process_generation), pod_uid=str(pod_uid),
        projection_digest=digest,
    )
    async with db.acquire() as conn, conn.transaction():
        assert await accept_pinned_report_on_conn(
            conn, job_id=owner, agent_id=agent_id,
            delivery_id=intent["id"], projection_digest=digest,
            process_generation=str(process_generation), pod_uid=str(pod_uid),
            delivery_proof="0" * 64, source_kind="route",
        ) is None
        accepted = await accept_pinned_report_on_conn(
            conn, job_id=owner, agent_id=agent_id,
            delivery_id=intent["id"], projection_digest=digest,
            process_generation=str(process_generation), pod_uid=str(pod_uid),
            delivery_proof=proof, source_kind="route",
        )
        assert accepted is not None
        assert accepted["accepted_via"] == "route"
    assert await db.fetchval(
        "SELECT accepted_at IS NOT NULL FROM pinned_job_deliveries WHERE id=$1",
        intent["id"],
    )


async def _publish_fast_route(db, *, owner, agent_id, intent, process_generation,
                              pod_uid, proof, route_id):
    from orchestrator.services.pinned_job_delivery import (
        accept_pinned_report_on_conn, record_pinned_wait_receipt_on_conn,
    )

    async with db.acquire() as conn, conn.transaction():
        await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", owner)
        delivery = await accept_pinned_report_on_conn(
            conn, job_id=owner, agent_id=agent_id,
            delivery_id=intent["id"], projection_digest=intent["projection_digest"],
            process_generation=str(process_generation), pod_uid=str(pod_uid),
            delivery_proof=proof, source_kind="route",
        )
        if delivery is None:
            return False
        await conn.execute(
            "INSERT INTO job_message_routes(route_id,job_id,thread_id,state,blocking) "
            "VALUES($1,$2,'fast-pinned-route','user_direct',true)", route_id, owner,
        )
        await conn.execute(
            "UPDATE jobs SET status='waiting_for_reply',"
            "freeze_data=jsonb_build_object('route_id',$2::text) WHERE id=$1",
            owner, str(route_id),
        )
        return await record_pinned_wait_receipt_on_conn(
            conn, delivery=delivery, source_kind="route", source_id=route_id,
        ) is not None


@pytest.mark.asyncio
async def test_two_connection_fast_report_precedes_late_post_confirmation(db, monkeypatch):
    from shared.pinned_job_delivery import pinned_job_delivery_proof

    await _schema(db)
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", "x" * 64)
    owner, agent_id, generation, pod_uid, _ = await _pinned_claim(db)
    recipient = PinnedJobRecipient(
        expected_agent_id=str(agent_id), expected_pod_uid=str(pod_uid),
        expected_process_generation=str(generation), expected_job_id=str(owner),
    )
    intent = await db.prepare_pinned_job_delivery(
        str(owner), str(agent_id), recipient=recipient,
        projection_digest="sha256:" + "a" * 64,
    )
    assert intent is not None
    proof = pinned_job_delivery_proof(
        b"x" * 64, delivery_id=str(intent["id"]), agent_id=str(agent_id),
        process_generation=str(generation), pod_uid=str(pod_uid),
        projection_digest=intent["projection_digest"],
    )
    episode_before = await db.fetchval(
        "SELECT workspace_idle_episode FROM jobs WHERE id=$1", owner,
    )
    route_id = UUID(json.loads(episode_before)["wait_key"])
    async with db.acquire() as blocker, blocker.transaction():
        await blocker.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", owner)
        report = asyncio.create_task(_publish_fast_route(
            db, owner=owner, agent_id=agent_id, intent=intent,
            process_generation=generation, pod_uid=pod_uid,
            proof=proof, route_id=route_id,
        ))
        await asyncio.sleep(0.05)
        confirmation = asyncio.create_task(db.confirm_pinned_job_dispatch(
            str(owner), str(agent_id), pinned_delivery_id=str(intent["id"]),
            pinned_projection_digest=intent["projection_digest"],
        ))
        await asyncio.sleep(0.05)
        assert not report.done() and not confirmation.done()
    reported, confirmed = await asyncio.wait_for(
        asyncio.gather(report, confirmation), timeout=5,
    )
    assert reported and confirmed
    row = await db.fetchrow(
        "SELECT status,freeze_data,workspace_idle_episode FROM jobs WHERE id=$1", owner,
    )
    assert row["status"] == "waiting_for_reply"
    assert json.loads(row["freeze_data"])["route_id"] == str(route_id)
    assert row["workspace_idle_episode"] == episode_before
    assert await db.fetchval(
        "SELECT accepted_via FROM pinned_job_deliveries WHERE id=$1", intent["id"],
    ) == "route"


@pytest.mark.asyncio
async def test_two_connection_renewed_heartbeat_and_report_keep_original_marker(
    db, monkeypatch,
):
    from shared.pinned_job_delivery import pinned_job_delivery_proof

    await _schema(db)
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", "x" * 64)
    owner, agent_id, generation, pod_uid, _ = await _pinned_claim(db)
    recipient = PinnedJobRecipient(
        expected_agent_id=str(agent_id), expected_pod_uid=str(pod_uid),
        expected_process_generation=str(generation), expected_job_id=str(owner),
    )
    intent = await db.prepare_pinned_job_delivery(
        str(owner), str(agent_id), recipient=recipient,
        projection_digest="sha256:" + "a" * 64,
    )
    proof = pinned_job_delivery_proof(
        b"x" * 64, delivery_id=str(intent["id"]), agent_id=str(agent_id),
        process_generation=str(generation), pod_uid=str(pod_uid),
        projection_digest=intent["projection_digest"],
    )
    route_id = UUID(json.loads(await db.fetchval(
        "SELECT workspace_idle_episode FROM jobs WHERE id=$1", owner,
    ))["wait_key"])
    original_marker = await db.fetchval(
        "SELECT context->'_workspace_dispatch_authority' FROM jobs WHERE id=$1", owner,
    )
    await db.execute(
        "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 hour' WHERE id=$1", owner,
    )
    assert await db.heartbeat(
        str(agent_id), "working", current_job_id=str(owner),
    )
    assert await db.fetchval(
        "SELECT lease_expires_at > clock_timestamp() FROM jobs WHERE id=$1", owner,
    )
    await db.execute(
        "UPDATE jobs SET lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE id=$1",
        owner,
    )
    async with db.acquire() as blocker, blocker.transaction():
        await blocker.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", owner)
        report = asyncio.create_task(_publish_fast_route(
            db, owner=owner, agent_id=agent_id, intent=intent,
            process_generation=generation, pod_uid=pod_uid,
            proof=proof, route_id=route_id,
        ))
        await asyncio.sleep(0.05)
        heartbeat = asyncio.create_task(db.heartbeat(
            str(agent_id), "working", current_job_id=str(owner),
        ))
        await asyncio.sleep(0.05)
        assert not heartbeat.done() and not report.done()
    beat, reported = await asyncio.wait_for(
        asyncio.gather(heartbeat, report), timeout=5,
    )
    assert beat is not None and reported
    assert await db.fetchval(
        "SELECT context->'_workspace_dispatch_authority' FROM jobs WHERE id=$1", owner,
    ) == original_marker
    lease_proof = await db.fetchrow(
        "SELECT accepted_lease_expires_at > clock_timestamp() AS accepted_live,"
        "accepted_lease_expires_at <> original_lease_expires_at AS renewed,"
        "accepted_via FROM pinned_job_deliveries WHERE id=$1", intent["id"],
    )
    assert lease_proof["accepted_live"] and lease_proof["renewed"], dict(lease_proof)
    assert await db.fetchval(
        "SELECT status FROM jobs WHERE id=$1", owner,
    ) == "waiting_for_reply"


@pytest.mark.asyncio
async def test_two_connection_heartbeat_cannot_undo_pinned_nomination(db, monkeypatch):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    owner, agent_id, _, revision, episode, identity = await _due_pinned_route(
        db, monkeypatch,
    )
    before = await db.fetchrow(
        "SELECT context,workspace_idle_episode,lease_expires_at FROM jobs WHERE id=$1", owner,
    )
    store = VMIdleLifecycleStore(db)
    async with db.acquire() as blocker, blocker.transaction():
        await blocker.fetchrow("SELECT id FROM agents WHERE id=$1 FOR UPDATE", agent_id)
        nominee = asyncio.create_task(store.admit_release(
            str(owner), episode_id=episode["episode_id"],
            revision=revision, identity=identity,
        ))
        await asyncio.sleep(0.05)
        heartbeat = asyncio.create_task(db.heartbeat(
            str(agent_id), "working", current_job_id=str(owner),
        ))
        await asyncio.sleep(0.05)
        assert not nominee.done() and not heartbeat.done()
    admitted, beat = await asyncio.wait_for(
        asyncio.gather(nominee, heartbeat), timeout=5,
    )
    assert admitted is not None and beat is not None
    assert await db.fetchval("SELECT status FROM agents WHERE id=$1", agent_id) == "draining"
    after = await db.fetchrow(
        "SELECT context,workspace_idle_episode,lease_expires_at FROM jobs WHERE id=$1", owner,
    )
    before_context = json.loads(before["context"])
    after_context = json.loads(after["context"])
    assert after_context["_workspace_dispatch_authority"] == (
        before_context["_workspace_dispatch_authority"]
    )
    for key in (
        "provision_generation", "vm_uid", "vmi_uid", "active_pod_uid", "rootdisk_pvc_uid",
    ):
        assert after_context["vm"][key] == before_context["vm"][key]
    assert after["workspace_idle_episode"] == before["workspace_idle_episode"]
    assert after["lease_expires_at"] == before["lease_expires_at"]


@pytest.mark.asyncio
async def test_two_connection_lease_recovery_loses_to_fast_route_report(db, monkeypatch):
    from shared.pinned_job_delivery import pinned_job_delivery_proof

    await _schema(db)
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", "x" * 64)
    owner, agent_id, generation, pod_uid, _ = await _pinned_claim(db)
    recipient = PinnedJobRecipient(
        expected_agent_id=str(agent_id), expected_pod_uid=str(pod_uid),
        expected_process_generation=str(generation), expected_job_id=str(owner),
    )
    intent = await db.prepare_pinned_job_delivery(
        str(owner), str(agent_id), recipient=recipient,
        projection_digest="sha256:" + "a" * 64,
    )
    proof = pinned_job_delivery_proof(
        b"x" * 64, delivery_id=str(intent["id"]), agent_id=str(agent_id),
        process_generation=str(generation), pod_uid=str(pod_uid),
        projection_digest=intent["projection_digest"],
    )
    route_id = UUID(json.loads(await db.fetchval(
        "SELECT workspace_idle_episode FROM jobs WHERE id=$1", owner,
    ))["wait_key"])
    await db.execute(
        "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 hour' WHERE id=$1", owner,
    )
    async with db.acquire() as blocker, blocker.transaction():
        await blocker.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", owner)
        report = asyncio.create_task(_publish_fast_route(
            db, owner=owner, agent_id=agent_id, intent=intent,
            process_generation=generation, pod_uid=pod_uid,
            proof=proof, route_id=route_id,
        ))
        await asyncio.sleep(0.05)
        recovery = asyncio.create_task(db.recover_expired_lease_jobs(
            completion_commands_enabled=True,
        ))
        await asyncio.sleep(0.05)
        assert not report.done()
    reported, _ = await asyncio.wait_for(
        asyncio.gather(report, recovery), timeout=10,
    )
    assert reported
    row = await db.fetchrow(
        "SELECT status,assigned_agent_id,context FROM jobs WHERE id=$1", owner,
    )
    assert row["status"] == "waiting_for_reply"
    assert row["assigned_agent_id"] == agent_id
    assert await db.fetchval(
        "SELECT count(*) FROM pinned_job_wait_receipts WHERE delivery_id=$1", intent["id"],
    ) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("resume_kind", ["in_process", "queued"])
async def test_two_connection_resume_and_nomination_have_one_owner(
    db, monkeypatch, resume_kind,
):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    owner, agent_id, _, revision, episode, identity = await _due_pinned_route(
        db, monkeypatch,
    )
    before = await db.fetchrow(
        "SELECT context,workspace_idle_episode FROM jobs WHERE id=$1", owner,
    )
    store = VMIdleLifecycleStore(db)
    async with db.acquire() as blocker, blocker.transaction():
        await blocker.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", owner)
        nomination = asyncio.create_task(store.admit_release(
            str(owner), episode_id=episode["episode_id"],
            revision=revision, identity=identity,
        ))
        await asyncio.sleep(0.05)
        if resume_kind == "in_process":
            resume = asyncio.create_task(db.resume_pinned_job_in_process(str(owner)))
        else:
            resume = asyncio.create_task(db.queue_job_for_resume(
                str(owner), expected_status="waiting_for_reply",
                expected_route_id=episode["wait_key"],
                completion_commands_enabled=True,
            ))
        await asyncio.sleep(0.05)
        assert not nomination.done() and not resume.done()
    admitted, resumed = await asyncio.wait_for(
        asyncio.gather(nomination, resume), timeout=5,
    )
    assert (admitted is not None) != resumed
    after = await db.fetchrow(
        "SELECT status,context,workspace_idle_episode FROM jobs WHERE id=$1", owner,
    )
    if admitted is not None:
        assert after["status"] == "waiting_for_reply"
        assert after["workspace_idle_episode"] == before["workspace_idle_episode"]
        assert json.loads(after["context"])["_workspace_dispatch_authority"] == (
            json.loads(before["context"])["_workspace_dispatch_authority"]
        )
        assert await db.fetchval(
            "SELECT count(*) FROM vm_idle_operations WHERE owner_kind='job' "
            "AND owner_id=$1 AND closed_at IS NULL", owner,
        ) == 1
    else:
        assert after["status"] in {"processing", "paused"}
        assert await db.fetchval(
            "SELECT count(*) FROM vm_idle_operations WHERE owner_kind='job' "
            "AND owner_id=$1 AND closed_at IS NULL", owner,
        ) == 0


async def _due_pinned_route(db, monkeypatch):
    from orchestrator.services.pinned_job_delivery import (
        accept_pinned_report_on_conn, record_pinned_wait_receipt_on_conn,
    )
    from shared.pinned_job_delivery import pinned_job_delivery_proof

    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_PERSISTENT_ROOTDISK": "true",
        "VM_MODE": "same-cluster",
        "VM_LIFECYCLE_HMAC_SECRET": "x" * 64,
    }.items():
        monkeypatch.setenv(key, value)
    await _schema(db)
    owner, agent_id, process_generation, pod_uid, identity = await _pinned_claim(db)
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
    source = await db.fetchrow(
        "SELECT workspace_idle_episode,workspace_idle_revision FROM jobs WHERE id=$1",
        owner,
    )
    assert source["workspace_idle_episode"] is not None
    episode = json.loads(source["workspace_idle_episode"])
    route_id = UUID(episode["wait_key"])
    async with db.acquire() as conn, conn.transaction():
        accepted = await accept_pinned_report_on_conn(
            conn, job_id=owner, agent_id=agent_id, delivery_id=intent["id"],
            projection_digest=digest, process_generation=str(process_generation),
            pod_uid=str(pod_uid), delivery_proof=proof, source_kind="route",
        )
        assert accepted is not None
        await conn.execute(
            "INSERT INTO job_message_routes(route_id,job_id,thread_id,state,blocking) "
            "VALUES($1,$2,'pinned-route','user_direct',true)",
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
    await db.execute("DELETE FROM run_queue WHERE unit_id=$1", owner)
    await db.execute(
        "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 hour' "
        "WHERE id=$1", owner,
    )
    return owner, agent_id, intent, source["workspace_idle_revision"], episode, identity


@pytest.mark.asyncio
async def test_due_pinned_wait_admits_exact_operation_after_lease_expiry(db, monkeypatch):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    owner, agent_id, intent, revision, episode, identity = await _due_pinned_route(
        db, monkeypatch,
    )
    store = VMIdleLifecycleStore(db)
    assert str(owner) in await store.due_job_ids()
    operation = await store.admit_release(
        str(owner), episode_id=episode["episode_id"],
        revision=revision, identity=identity,
    )
    assert operation is not None
    assert operation["release_kind"] == "pinned_job"
    assert operation["pinned_delivery_id"] == intent["id"]
    assert operation["pinned_agent_id"] == agent_id
    assert operation["pinned_agent_pod_uid"] == intent["pod_uid"]
    assert await db.fetchval("SELECT status FROM agents WHERE id=$1", agent_id) == "draining"


async def _second_pinned_job(db):
    other = uuid4()
    await db.execute(
        "INSERT INTO jobs(id,description,status,execution_lane,config_override,context) "
        "VALUES($1,'second pinned claim','created','pinned',$2::jsonb,$3::jsonb)",
        other, json.dumps({"workspace": {"backend": "vm"}}),
        json.dumps({"vm": {"requested": True}}),
    )
    return other


@pytest.mark.asyncio
async def test_open_pinned_idle_operation_fences_new_agent_claim(db, monkeypatch):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    owner, agent_id, _, revision, episode, identity = await _due_pinned_route(
        db, monkeypatch,
    )
    store = VMIdleLifecycleStore(db)
    assert await store.admit_release(
        str(owner), episode_id=episode["episode_id"],
        revision=revision, identity=identity,
    ) is not None
    other = await _second_pinned_job(db)
    assert not await db.claim_job_for_agent(str(other), str(agent_id))
    assert await db.fetchval(
        "SELECT assigned_agent_id FROM jobs WHERE id=$1", other,
    ) is None


@pytest.mark.asyncio
async def test_admitted_pinned_fence_survives_feature_flag_off(db, monkeypatch):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    owner, agent_id, _, revision, episode, identity = await _due_pinned_route(
        db, monkeypatch,
    )
    operation = await VMIdleLifecycleStore(db).admit_release(
        str(owner), episode_id=episode["episode_id"],
        revision=revision, identity=identity,
    )
    assert operation is not None
    other = await _second_pinned_job(db)
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    # Even a misleading ready heartbeat and a disabled new-admission flag
    # cannot let the old process receive a second Job.
    await db.execute("UPDATE agents SET status='ready' WHERE id=$1", agent_id)
    assert not await db.claim_job_for_agent(str(other), str(agent_id))


@pytest.mark.asyncio
async def test_stale_cross_job_claim_waits_for_agent_first_nomination(db, monkeypatch):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    owner, agent_id, _, revision, episode, identity = await _due_pinned_route(
        db, monkeypatch,
    )
    other = await _second_pinned_job(db)
    store = VMIdleLifecycleStore(db)
    async with db.acquire() as blocker:
        async with blocker.transaction():
            await blocker.fetchrow("SELECT id FROM agents WHERE id=$1 FOR UPDATE", agent_id)
            nominee = asyncio.create_task(store.admit_release(
                str(owner), episode_id=episode["episode_id"],
                revision=revision, identity=identity,
            ))
            await asyncio.sleep(0.05)
            stale_claim = asyncio.create_task(
                db.claim_job_for_agent(str(other), str(agent_id))
            )
            await asyncio.sleep(0.05)
            assert not nominee.done() and not stale_claim.done()
        admitted, claimed = await asyncio.wait_for(
            asyncio.gather(nominee, stale_claim), timeout=5,
        )
    assert admitted is not None
    assert not claimed
    assert await db.fetchval(
        "SELECT assigned_agent_id FROM jobs WHERE id=$1", other,
    ) is None


@pytest.mark.asyncio
async def test_open_pinned_operation_refuses_legacy_resume_writers(db, monkeypatch):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    owner, _, _, revision, episode, identity = await _due_pinned_route(db, monkeypatch)
    assert await VMIdleLifecycleStore(db).admit_release(
        str(owner), episode_id=episode["episode_id"],
        revision=revision, identity=identity,
    ) is not None
    assert not await db.resume_pinned_job_in_process(str(owner))
    assert not await db.queue_job_for_resume(
        str(owner), expected_status="waiting_for_reply",
    )
    row = await db.fetchrow(
        "SELECT status,assigned_agent_id FROM jobs WHERE id=$1", owner,
    )
    assert row["status"] == "waiting_for_reply"
    assert row["assigned_agent_id"] is not None


@pytest.mark.asyncio
async def test_pinned_access_and_execution_join_one_wake_without_queueing(db, monkeypatch):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    owner, _, _, revision, episode, identity = await _due_pinned_route(db, monkeypatch)
    store = VMIdleLifecycleStore(db)
    operation = await store.admit_release(
        str(owner), episode_id=episode["episode_id"],
        revision=revision, identity=identity,
    )
    assert operation is not None
    access = await store.request_wake(
        str(owner), execution_requested=False,
        access_kind="ide", access_claimant="person-a",
    )
    assert access is not None
    execution = await store.request_wake(str(owner), execution_requested=True)
    assert execution is not None
    assert execution["wake_id"] == access["wake_id"]
    assert execution["wake_generation"] == access["wake_generation"]
    assert execution["wake_execution_requested"] is True
    job = await db.fetchrow(
        "SELECT status,assigned_agent_id FROM jobs WHERE id=$1", owner,
    )
    assert job["status"] == "waiting_for_reply"
    assert job["assigned_agent_id"] is not None


@pytest.mark.asyncio
async def test_pinned_execution_dispatches_fresh_only_after_successor_ready(db, monkeypatch):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    owner, agent_id, _, revision, episode, identity = await _due_pinned_route(
        db, monkeypatch,
    )
    store = VMIdleLifecycleStore(db)
    operation = await store.admit_release(
        str(owner), episode_id=episode["episode_id"],
        revision=revision, identity=identity,
    )
    claimed = await store.claim(str(operation["id"]), claimant="wake-test")
    assert await store.record_pinned_terminal(claimed, claimant="wake-test")
    assert await store.record_pinned_stop(
        claimed, claimant="wake-test", absence="exact_absent",
    )
    await store.release_claim(str(operation["id"]), token=claimed["claim_token"],
                              claimant="wake-test")
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)", owner, identity["generation"],
    )
    physical = {
        "version": 1, "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]), **identity,
        "vm_absent": True, "vmi_absent": True, "launcher_absent": True,
        "same_generation_replacement": False, "retained_pvc": True,
        "controller_authenticated": True,
    }
    assert await store.complete_release(str(operation["id"]), evidence=physical)
    wake = await store.request_wake(str(owner), execution_requested=True)
    assert wake is not None
    assert await db.fetchval("SELECT status FROM jobs WHERE id=$1", owner) == "waiting_for_reply"
    assert not await store.finish_wake(str(operation["id"]))

    successor = {
        "generation": str(wake["wake_generation"]),
        "vm_uid": str(uuid4()), "vmi_uid": str(uuid4()),
        "launcher_uid": str(uuid4()), "pvc_uid": identity["pvc_uid"],
    }
    raw = await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
    context = json.loads(raw)
    context["last_vm"] = context["vm"]
    context["vm"] = {
        "status": "ready", "idle_wake_operation_id": str(operation["id"]),
        "provision_generation": successor["generation"],
        "vm_uid": successor["vm_uid"], "vmi_uid": successor["vmi_uid"],
        "active_pod_uid": successor["launcher_uid"],
        "rootdisk_pvc_uid": successor["pvc_uid"],
    }
    await db.execute("UPDATE jobs SET context=$2::jsonb WHERE id=$1", owner,
                     json.dumps(context))
    assert await store.mark_wake_ready(str(operation["id"]), **successor)
    assert await store.finish_wake(str(operation["id"]))
    row = await db.fetchrow(
        "SELECT status,assigned_agent_id,workspace_idle_episode FROM jobs WHERE id=$1",
        owner,
    )
    assert row["status"] == "paused"
    assert row["assigned_agent_id"] is None
    assert row["workspace_idle_episode"] is None
    assert agent_id is not None
    assert await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner) is None


@pytest.mark.asyncio
async def test_pinned_idle_public_approval_claim_preserves_captured_assignment(
    db, monkeypatch,
):
    from orchestrator.services.completion_control import CompletionControl
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore
    from unittest.mock import AsyncMock

    owner, agent_id, _, revision, episode, identity = await _due_pinned_route(
        db, monkeypatch,
    )
    operation = await VMIdleLifecycleStore(db).admit_release(
        str(owner), episode_id=episode["episode_id"],
        revision=revision, identity=identity,
    )
    assert operation is not None
    # A phase/review approval must retain the old agent only as captured stop
    # authority until its exact Pod is physically gone.
    await db.execute("UPDATE jobs SET status='pending_review' WHERE id=$1", owner)
    control = CompletionControl(db, AsyncMock())
    claim = await control.claim_job(
        owner, source="public_approve", expected_status="pending_review",
        expected_lane="pinned",
    )
    assert claim.fence_value == str(agent_id)
    assert await db.fetchval(
        "SELECT assigned_agent_id FROM jobs WHERE id=$1", owner,
    ) == agent_id


@pytest.mark.asyncio
async def test_pinned_access_only_reuses_proven_agent_stop_across_two_cycles(
    db, monkeypatch,
):
    from shared.workspace_idle_policy import read_episode
    from tests.test_vm_idle_access_wake_lineage_real_postgres import access_only_cycle

    owner, agent_id, _, revision, document, identity = await _due_pinned_route(
        db, monkeypatch,
    )
    episode = read_episode(document, revision=revision)
    assert episode is not None
    original_wait = (episode.episode_id, episode.wait_key, episode.entered_at)
    for index in range(2):
        operation, episode, identity = await access_only_cycle(
            db, owner, episode, identity, pinned=True,
        )
        assert episode is not None
        assert (episode.episode_id, episode.wait_key, episode.entered_at) == original_wait
        if index == 1:
            assert json.loads(operation["pinned_stop_evidence"])["kind"] == "pinned_job_agent_stop_reuse"
            assert operation["pinned_agent_id"] == agent_id


@pytest.mark.asyncio
async def test_other_unresolved_claim_wins_before_nomination(db, monkeypatch):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    owner, agent_id, _, revision, episode, identity = await _due_pinned_route(
        db, monkeypatch,
    )
    other = await _second_pinned_job(db)
    assert await db.claim_job_for_agent(str(other), str(agent_id))
    store = VMIdleLifecycleStore(db)
    assert await store.admit_release(
        str(owner), episode_id=episode["episode_id"],
        revision=revision, identity=identity,
    ) is None
    assert await db.fetchval("SELECT status FROM agents WHERE id=$1", agent_id) == "ready"


@pytest.mark.asyncio
async def test_pinned_release_never_acquires_vm_permit_without_exact_agent_stop(
    db, monkeypatch,
):
    from orchestrator.services import vm_idle_lifecycle as module
    from orchestrator.services.vm_provisioner import VMTeardownIdentity

    owner, _, _, revision, episode, identity = await _due_pinned_route(db, monkeypatch)
    store = module.VMIdleLifecycleStore(db)
    admitted = await store.admit_release(
        str(owner), episode_id=episode["episode_id"],
        revision=revision, identity=identity,
    )
    assert admitted is not None
    operation = await store.claim(str(admitted["id"]), claimant="pinned-test")
    assert operation is not None

    class Provisioner:
        async def capture_vm_teardown_identity(self, job_id):
            assert job_id == str(owner)
            return VMTeardownIdentity(
                identity["generation"], identity["vm_uid"], identity["pvc_uid"],
            )

    async def forbidden_permit(*args, **kwargs):
        pytest.fail("VM permit acquired before pinned agent stop proof")

    monkeypatch.setattr(module, "acquire_vm_cleanup_permit", forbidden_permit)
    service = module.VMIdleLifecycleService(db, Provisioner(), object())
    assert not await service._release(operation, current=lambda: True)
    assert await db.fetchval(
        "SELECT pinned_stop_evidence IS NULL FROM vm_idle_operations WHERE id=$1",
        operation["id"],
    )


@pytest.mark.asyncio
async def test_pinned_release_records_terminal_then_absence_before_vm_permit(
    db, monkeypatch,
):
    from orchestrator.services import vm_idle_lifecycle as module
    from orchestrator.services.vm_provisioner import VMTeardownIdentity
    from types import SimpleNamespace

    owner, _, _, revision, episode, identity = await _due_pinned_route(db, monkeypatch)
    store = module.VMIdleLifecycleStore(db)
    admitted = await store.admit_release(
        str(owner), episode_id=episode["episode_id"],
        revision=revision, identity=identity,
    )
    operation = await store.claim(str(admitted["id"]), claimant="pinned-test")
    assert operation is not None

    class Provisioner:
        async def capture_vm_teardown_identity(self, job_id):
            return VMTeardownIdentity(
                identity["generation"], identity["vm_uid"], identity["pvc_uid"],
            )

    class AgentPods:
        is_available = True
        observations = iter(("exact_live", "exact_terminal", "exact_absent"))
        deleted = False
        finalizer_released = False

        async def observe_agent_pod_exact(self, name, *, expected_pod_uid, namespace):
            assert name == operation["pinned_agent_pod_name"]
            assert expected_pod_uid == operation["pinned_agent_pod_uid"]
            assert namespace == operation["pinned_agent_pod_namespace"]
            state = next(self.observations)
            if state == "exact_terminal":
                from types import SimpleNamespace

                return state, SimpleNamespace(
                    spec=SimpleNamespace(containers=[SimpleNamespace(name="agent")]),
                    status=SimpleNamespace(container_statuses=[SimpleNamespace(
                        name="agent", state=SimpleNamespace(
                            terminated=SimpleNamespace(exit_code=0),
                        ),
                    )]),
                )
            return state, None

        async def delete_agent_pod_exact(self, name, *, expected_pod_uid, namespace):
            self.deleted = True
            return True

        async def release_agent_pod_finalizer_exact(
            self, name, *, expected_pod_uid, namespace, terminal_required,
        ):
            assert terminal_required
            self.finalizer_released = True
            return True

    actor = AgentPods()

    async def permit_after_stop(*args, **kwargs):
        assert actor.deleted and actor.finalizer_released
        stored = await store.get_operation(str(operation["id"]))
        assert stored["pinned_terminal_observed_at"] is not None
        assert stored["pinned_stop_verified_at"] is not None
        assert json.loads(stored["pinned_stop_evidence"])["pod_uid"] == operation["pinned_agent_pod_uid"]
        return SimpleNamespace(allowed=False)

    monkeypatch.setattr(module, "acquire_vm_cleanup_permit", permit_after_stop)
    service = module.VMIdleLifecycleService(
        db, Provisioner(), object(), claimant="pinned-test", agent_provisioner=actor,
    )
    assert not await service._release(operation, current=lambda: True)


@pytest.mark.asyncio
async def test_ordinary_lifecycle_cleanup_stands_down_for_open_pinned_release(
    db, monkeypatch,
):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore
    from orchestrator.services.vm_workspace_recovery_store import (
        VMWorkspaceRecoveryStore, acquire_vm_cleanup_permit,
    )
    from orchestrator.services.vm_provisioner import VMTeardownIdentity

    owner, _, _, revision, episode, identity = await _due_pinned_route(db, monkeypatch)
    operation = await VMIdleLifecycleStore(db).admit_release(
        str(owner), episode_id=episode["episode_id"],
        revision=revision, identity=identity,
    )
    assert operation is not None
    recovery = VMWorkspaceRecoveryStore(db)
    captured = VMTeardownIdentity(
        identity["generation"], identity["vm_uid"], identity["pvc_uid"],
    )
    ordinary = await acquire_vm_cleanup_permit(
        recovery, owner_kind="job", owner_id=str(owner), identity=captured,
        source="lifecycle_vm_delete", purge_disk=False,
    )
    assert not ordinary.allowed
    assert ordinary.reason == "pinned_job_idle_owned"
    assert await db.fetchval(
        "SELECT count(*) FROM vm_workspace_cleanup_admissions "
        "WHERE owner_kind='job' AND owner_id=$1 AND source='lifecycle_vm_delete'",
        owner,
    ) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["terminal_then_absent", "replacement", "deleting",
                                      "partial_terminal", "read_timeout"])
async def test_pinned_stop_uses_actual_uid_bound_pod_client_and_terminal_graph(
    db, monkeypatch, scenario,
):
    from types import SimpleNamespace as NS
    from unittest.mock import MagicMock
    from orchestrator.services.agent_provisioner import AgentProvisioner
    from orchestrator.services.vm_idle_lifecycle import (
        VMIdleLifecycleService, VMIdleLifecycleStore,
    )

    owner, _, _, revision, episode, identity = await _due_pinned_route(db, monkeypatch)
    store = VMIdleLifecycleStore(db)
    admitted = await store.admit_release(
        str(owner), episode_id=episode["episode_id"],
        revision=revision, identity=identity,
    )
    operation = await store.claim(str(admitted["id"]), claimant="actual-pod-api")
    assert operation is not None
    uid = operation["pinned_agent_pod_uid"]

    def pod(*, actual_uid=uid, phase="Running", deleting=False, partial=False):
        terminated = NS(terminated=NS(exit_code=0))
        running = NS(terminated=None)
        return NS(
            metadata=NS(uid=actual_uid, deletion_timestamp="now" if deleting else None,
                        resource_version="11", finalizers=["srw.io/pinned-authority-protection"]),
            spec=NS(containers=[NS(name="agent")], init_containers=[NS(name="init")],
                    ephemeral_containers=[NS(name="debug")]),
            status=NS(
                phase=phase,
                container_statuses=[NS(name="agent", state=terminated if phase == "Failed" else running)],
                init_container_statuses=[] if partial else [NS(name="init", state=terminated)],
                ephemeral_container_statuses=[NS(name="debug", state=terminated)],
            ),
        )

    class NotFound(Exception):
        status = 404

    core = MagicMock()
    if scenario == "terminal_then_absent":
        core.read_namespaced_pod.side_effect = [
            pod(), pod(phase="Failed"), pod(phase="Failed"), NotFound(),
        ]
    elif scenario == "replacement":
        core.read_namespaced_pod.return_value = pod(actual_uid="replacement")
    elif scenario == "deleting":
        core.read_namespaced_pod.return_value = pod(deleting=True)
    elif scenario == "partial_terminal":
        core.read_namespaced_pod.return_value = pod(phase="Failed", partial=True)
    else:
        core.read_namespaced_pod.side_effect = TimeoutError("read timeout")
    actor = AgentProvisioner()
    actor._k8s_available = True
    actor._namespace = operation["pinned_agent_pod_namespace"]
    actor._core_api = core
    service = VMIdleLifecycleService(
        db, object(), object(), claimant="actual-pod-api", agent_provisioner=actor,
    )
    stopped = await service._stop_pinned_agent(operation, current=lambda: True)
    assert stopped is (scenario == "terminal_then_absent")
    stored = await store.get_operation(str(operation["id"]))
    assert (stored["pinned_stop_verified_at"] is not None) == stopped
    if stopped:
        core.delete_namespaced_pod.assert_called_once()
        assert core.delete_namespaced_pod.call_args.kwargs["body"] == {
            "preconditions": {"uid": uid},
        }
        core.patch_namespaced_pod.assert_called_once()
        patch = core.patch_namespaced_pod.call_args.kwargs["body"]
        assert {"op": "test", "path": "/metadata/uid", "value": uid} in patch
    else:
        core.delete_namespaced_pod.assert_not_called()
        core.patch_namespaced_pod.assert_not_called()
