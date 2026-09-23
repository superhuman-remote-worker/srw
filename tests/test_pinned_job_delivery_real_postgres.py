"""A pinned VM Job receives one durable, non-authorizing pre-POST intent."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from shared.pinned_session_identity import PinnedJobRecipient
from tests.test_vm_idle_lifecycle_real_postgres import seed_wait

pytest_plugins = ("tests.test_vm_idle_lifecycle_real_postgres",)


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
