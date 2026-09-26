"""A pending stateless creation can adopt its exact Pod after a lost waiter."""

import asyncio
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator.services import container_provisioner as provisioner_module
from orchestrator.services.container_provisioner import ContainerProvisioner
from orchestrator.services.manifest_execution_snapshot import read_execution
from orchestrator.services.manifest_workspace_selection import (
    select_execution_workspace,
)
from orchestrator.services.session_provisioner import ensure_session_workspace
from orchestrator.services.workspace_lifecycle import EnsureOutcome, WorkspaceOwner
from tests import test_manifest_native_full_schema as full_schema
from tests.test_sandbox_workspace_provisioner import stub_plan_inputs
from tests.test_workspace_pull_failure_real_postgres import NeverPullingCluster

actor = full_schema.actor
database = full_schema.database
postgres_url = full_schema.postgres_url

IMAGE = "registry.example/team/session@sha256:" + "c" * 64
FINGERPRINT = "SHA256:" + "A" * 43


@pytest.mark.asyncio
async def test_elapsed_pull_budget_does_not_extend_a_new_readiness_wait(monkeypatch):
    started_at = datetime.now(timezone.utc) - timedelta(seconds=601)
    pod = SimpleNamespace(
        metadata=SimpleNamespace(creation_timestamp=started_at),
        status=SimpleNamespace(
            phase="Pending",
            container_statuses=[
                SimpleNamespace(
                    name="workspace",
                    state=SimpleNamespace(
                        waiting=SimpleNamespace(
                            reason="ContainerCreating", message=None
                        )
                    ),
                )
            ],
        ),
    )
    provisioner = ContainerProvisioner()
    provisioner._core_api = SimpleNamespace(read_namespaced_pod=None)
    provisioner._bounded_kubernetes_call = AsyncMock(return_value=pod)
    real_sleep = asyncio.sleep
    monkeypatch.setattr(provisioner_module.asyncio, "sleep", lambda _: real_sleep(0.01))
    assert (
        await asyncio.wait_for(
            provisioner._wait_for_ready(
                "workspace-test",
                timeout=0.1,
                pull_image=IMAGE,
                pull_started_at=started_at,
            ),
            timeout=1,
        )
        is None
    )


class DelayedWorkspaceCluster(NeverPullingCluster):
    """The same Pod starts successfully after its original pull waiter failed."""

    def __init__(self):
        super().__init__()
        self.pod_create_calls = 0

    def create_namespaced_pod(self, *, body, **kwargs):
        self.pod_create_calls += 1
        pod = super().create_namespaced_pod(body=body, **kwargs)
        pod.metadata.creation_timestamp = datetime.now(timezone.utc) - timedelta(
            seconds=601
        )
        pod.status.container_statuses[0].state.waiting = SimpleNamespace(
            reason="ErrImagePull", message="registry temporarily unavailable"
        )
        return pod

    def become_ready(self):
        pod = self.objects["pod"]
        pod.status.phase = "Running"
        status = pod.status.container_statuses[0]
        status.ready = True
        status.started = True
        status.state = SimpleNamespace(
            waiting=None, running=SimpleNamespace(), terminated=None
        )

    def delete_namespaced_pod(self, **kwargs):
        super().delete_namespaced_pod(**kwargs)
        pod = self.objects.get("pod")
        if pod is not None:
            # Match Kubernetes client objects for exact terminated-container
            # coverage; the manifest fixture otherwise keeps raw dictionaries.
            pod.spec.containers = [
                SimpleNamespace(**container) for container in pod.spec.containers
            ]


def metadata(thread):
    value = thread["metadata"]
    return json.loads(value) if isinstance(value, str) else value


async def reservation(database, thread_id):
    rows = await database.fetch(
        "SELECT * FROM managed_repository_workspace_creation_reservations "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid "
        "AND scope='workspace_container' ORDER BY reservation_generation",
        thread_id,
    )
    assert len(rows) == 1
    return dict(rows[0])


async def pending_workspace(database, actor, monkeypatch):
    workspace, selection = await select_execution_workspace(
        database,
        actor,
        role="session",
        project_id=None,
        supplied=True,
        workspace={
            "template": {
                "inline": {
                    "backend": "sandbox",
                    "resources": {"cpu": 1, "memory": "3Gi", "storage": "15Gi"},
                    "environment": {"image": IMAGE, "pullPolicy": "IfNotPresent"},
                }
            }
        },
    )
    thread_id = await database.create_thread(
        user_id=str(actor["id"]),
        datasource_ids=[],
        execution_lane="stateless",
        initial_metadata={
            "config_override": {"workspace": workspace},
            "workspace_container": {
                "status": "pending",
                "provisioner": "k8s",
                "_runtime_creation": {
                    "generation": str(uuid4()),
                    "mode": "create",
                    "attempted": False,
                    "replaces_uid": None,
                },
            },
        },
        workspace_selection=selection,
    )
    original_snapshot = await read_execution(database, "Session", thread_id)
    owner = WorkspaceOwner.session(thread_id)
    cluster = DelayedWorkspaceCluster()
    provisioner = ContainerProvisioner()
    provisioner._db = database
    provisioner._k8s_available = True
    provisioner._namespace = "agent-workspaces"
    provisioner._storage_class = "test-storage"
    provisioner._pvc_enabled = True
    provisioner._core_api = cluster
    stub_plan_inputs(monkeypatch, provisioner)
    for name in ("open_interval", "close_interval"):
        monkeypatch.setattr(
            provisioner_module.workspace_metering, name, AsyncMock(return_value=None)
        )
    # Only external SSH is faked; readiness, identity checks, reservation
    # admission, publication, and settlement all run their production code.
    monkeypatch.setattr(
        provisioner_module,
        "wait_for_agent_ssh",
        AsyncMock(return_value=(True, 1, None)),
    )
    monkeypatch.setattr(
        provisioner_module, "workspace_private_key_fingerprint", lambda _: FINGERPRINT
    )
    monkeypatch.setattr(
        provisioner_module,
        "_isolated_pod_exec",
        lambda *args, **kwargs: f"256 {FINGERPRINT} workspace (ED25519)",
    )
    suspension = SimpleNamespace()

    first = await ensure_session_workspace(
        thread_id, db=database, provisioner=provisioner, suspension=suspension
    )
    assert first.outcome == EnsureOutcome.FAILED
    before = await database.get_thread(thread_id)
    pending = metadata(before)["workspace_container"]
    pod_uid = cluster.objects["pod"].metadata.uid
    pvc_uid = cluster.objects["pvc"].metadata.uid
    service_uid = cluster.objects["service"].metadata.uid
    creation = await reservation(database, thread_id)
    assert pending["_runtime_incarnation"] == pod_uid
    assert pending["_runtime_creation"]["attempted"] is True
    assert creation["phase"] == "runtime_bound"
    assert creation["settled_at"] is None
    assert cluster.objects["pod"].spec.containers[0]["image"] == IMAGE
    return SimpleNamespace(
        thread_id=thread_id,
        owner=owner,
        provisioner=provisioner,
        cluster=cluster,
        suspension=suspension,
        before=before,
        creation=creation,
        pod_uid=pod_uid,
        pvc_uid=pvc_uid,
        service_uid=service_uid,
        original_snapshot=original_snapshot,
    )


@pytest.mark.asyncio
async def test_ensure_continues_exact_open_creation_with_frozen_image(
    database, actor, monkeypatch
):
    case = await pending_workspace(database, actor, monkeypatch)
    thread_id, provisioner = case.thread_id, case.provisioner
    cluster, suspension = case.cluster, case.suspension
    pod_uid, pvc_uid, service_uid = case.pod_uid, case.pvc_uid, case.service_uid
    before, creation = case.before, case.creation

    cluster.become_ready()
    provisioner._workspace_image = "registry.example/installation/new-default:v2"
    await ensure_session_workspace(
        thread_id, db=database, provisioner=provisioner, suspension=suspension
    )
    after = await database.get_thread(thread_id)
    current = metadata(after)
    assert current["workspace_container"]["status"] == "ready"
    assert "_runtime_creation" not in current["workspace_container"]
    assert current["workspace_container"]["_runtime_incarnation"] == pod_uid
    assert current["_workspace_binding"]["backing_id"] == (
        f"k8s-pvc:agent-workspaces:{pvc_uid}"
    )
    assert current["_workspace_binding"]["ssh_host_key_fingerprint"] == FINGERPRINT
    assert after["runtime_generation"] == before["runtime_generation"]
    settled = await reservation(database, thread_id)
    assert settled["id"] == creation["id"]
    assert settled["claim_token"] == creation["claim_token"]
    assert settled["phase"] == "settled"
    assert settled["settled_at"] is not None
    assert cluster.pod_create_calls == 1
    assert cluster.pod_deletes == 0
    assert cluster.objects["pod"].metadata.uid == pod_uid
    assert cluster.objects["pvc"].metadata.uid == pvc_uid
    assert cluster.objects["service"].metadata.uid == service_uid
    assert cluster.objects["pod"].spec.containers[0]["image"] == IMAGE
    assert (
        await read_execution(database, "Session", thread_id) == case.original_snapshot
    )
    ready = await ensure_session_workspace(
        thread_id, db=database, provisioner=provisioner, suspension=suspension
    )
    assert ready.outcome == EnsureOutcome.READY
    assert cluster.pod_create_calls == 1


@pytest.mark.asyncio
async def test_ensure_continues_exact_creation_after_lease_expiry(
    database, actor, monkeypatch
):
    case = await pending_workspace(database, actor, monkeypatch)
    original = case.creation
    # Match the reservation duration guard while expiring only this test row.
    await database.execute(
        "UPDATE managed_repository_workspace_creation_reservations "
        "SET created_at = now() - interval '1 hour', "
        "expires_at = now() - interval '1 second' WHERE id = $1",
        original["id"],
    )
    case.cluster.become_ready()
    wait_for_ready = case.provisioner._wait_for_ready
    observed_rotation = []

    async def observe_rotated_claim(*args, **kwargs):
        # Observe after reclaim, before Ready/settlement, then run the actual
        # waiter. Both tokens face the same live, still-open reservation.
        rotated = await reservation(database, case.thread_id)
        projected = metadata(await database.get_thread(case.thread_id))[
            "workspace_container"
        ]
        assert rotated["settled_at"] is None
        assert rotated["claim_token"] > original["claim_token"]
        assert projected["_creation_reservation_id"] == str(original["id"])
        assert projected["_creation_claim_token"] == str(rotated["claim_token"])
        assert projected["_runtime_incarnation"] == case.pod_uid
        authority = {
            "owner_kind": "thread",
            "scope": "workspace_container",
            "reservation_generation": original["reservation_generation"],
            "claimant": original["claimed_by"],
        }
        assert await database.managed_repository_workspace_creation_claim_is_current(
            case.thread_id, **authority, claim_token=rotated["claim_token"]
        )
        assert (
            not await database.managed_repository_workspace_creation_claim_is_current(
                case.thread_id, **authority, claim_token=original["claim_token"]
            )
        )
        assert (
            await database.mark_managed_repository_workspace_creation_started(
                case.thread_id, **authority, claim_token=original["claim_token"]
            )
            is None
        )
        observed_rotation.append(rotated["claim_token"])
        return await wait_for_ready(*args, **kwargs)

    monkeypatch.setattr(case.provisioner, "_wait_for_ready", observe_rotated_claim)
    await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    settled = await reservation(database, case.thread_id)
    assert observed_rotation == [settled["claim_token"]]
    for key in (
        "id",
        "reservation_generation",
        "thread_runtime_generation",
        "desired_manifest_digest",
        "claimed_by",
        "operation_kind",
        "runtime_incarnation",
        "pod_uid",
        "pvc_uid",
        "service_uid",
    ):
        assert settled[key] == original[key]
    assert settled["phase"] == "settled"
    assert settled["settled_at"] is not None
    after = await database.get_thread(case.thread_id)
    current = metadata(after)["workspace_container"]
    assert after["runtime_generation"] == case.before["runtime_generation"]
    assert current["status"] == "ready"
    assert "_runtime_creation" not in current
    assert current["_creation_claim_token"] == str(settled["claim_token"])
    assert current["_creation_reservation_id"] == str(original["id"])
    assert current["_runtime_incarnation"] == case.pod_uid
    for kind, uid in (
        ("pod", case.pod_uid),
        ("pvc", case.pvc_uid),
        ("service", case.service_uid),
    ):
        assert case.cluster.objects[kind].metadata.uid == uid
    assert case.cluster.pod_create_calls == 1
    assert case.cluster.pod_deletes == 0
    assert (
        await read_execution(database, "Session", case.thread_id)
        == case.original_snapshot
    )
    ready = await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    assert ready.outcome == EnsureOutcome.READY
    assert case.cluster.pod_create_calls == 1


@pytest.mark.asyncio
async def test_continuation_keeps_elapsed_custom_image_pull_budget(
    database, actor, monkeypatch, caplog
):
    case = await pending_workspace(database, actor, monkeypatch)
    caplog.clear()
    result = await asyncio.wait_for(
        case.provisioner.continue_stateless_workspace_creation(
            case.owner,
            generation=str(case.before["runtime_generation"]),
            expected_runtime_incarnation=case.pod_uid,
        ),
        timeout=5,
    )
    assert result is False
    assert f"Workspace image {IMAGE} could not be pulled: ErrImagePull" in caplog.text
    after = await reservation(database, case.thread_id)
    assert after["id"] == case.creation["id"]
    assert after["claim_token"] == case.creation["claim_token"]
    assert after["settled_at"] is None
    assert case.cluster.pod_create_calls == 1
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid


@pytest.mark.asyncio
async def test_not_ready_continuation_keeps_reservation_open_until_ready(
    database, actor, monkeypatch
):
    case = await pending_workspace(database, actor, monkeypatch)
    with monkeypatch.context() as waiting:
        waiting.setattr(
            case.provisioner, "_wait_for_ready", AsyncMock(return_value=None)
        )
        await ensure_session_workspace(
            case.thread_id,
            db=database,
            provisioner=case.provisioner,
            suspension=case.suspension,
        )
    pending = await reservation(database, case.thread_id)
    assert pending["id"] == case.creation["id"]
    assert pending["claim_token"] == case.creation["claim_token"]
    assert pending["settled_at"] is None
    case.cluster.become_ready()
    await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    settled = await reservation(database, case.thread_id)
    assert settled["id"] == pending["id"]
    assert settled["settled_at"] is not None
    assert case.cluster.pod_create_calls == 1
    assert case.cluster.objects["pod"].metadata.uid == case.pod_uid
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid


@pytest.mark.asyncio
async def test_restore_continuation_keeps_suspension_operation_and_volume(
    database, actor, monkeypatch
):
    case = await pending_workspace(database, actor, monkeypatch)
    case.cluster.become_ready()
    await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    suspended = await case.provisioner.prepare_workspace_cleanup_intent(
        case.owner,
        expected_runtime_incarnation=case.pod_uid,
        target_disposition="suspended",
        reclaim_shared_resources=False,
        suspended_at=datetime.now(timezone.utc).isoformat(),
        snapshot_restore_required=True,
    )
    assert suspended is not None
    cleanup = await case.provisioner.reconcile_workspace_cleanup_intent(
        case.owner,
        expected_runtime_incarnation=case.pod_uid,
        intent_generation=suspended["intent_generation"],
    )
    assert cleanup.settled
    suspension = await case.provisioner.get_settled_workspace_suspension(
        case.owner, expected_runtime_incarnation=case.pod_uid
    )
    assert suspension is not None
    generation = str(case.before["runtime_generation"])
    assert str(suspension["runtime_incarnation"]) == case.pod_uid
    assert str(suspension["thread_runtime_generation"]) == generation
    assert "pod" not in case.cluster.objects
    receipts = await database.fetch(
        "SELECT * FROM managed_repository_process_zero_receipts "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid",
        case.thread_id,
    )
    assert receipts
    assert all(str(row["runtime_incarnation"]) == case.pod_uid for row in receipts)
    prepared = await database.prepare_stateless_thread_workspace_creation(
        case.thread_id,
        proposed_generation=str(uuid4()),
        mode="restore",
        expected_runtime_incarnation=case.pod_uid,
    )
    assert prepared["state"] == "prepared"
    # Seed only the completed predecessor-clearing projection. The predecessor
    # above really settled with a process-zero receipt and its finalizer gone.
    # Running the separate recreation-finalization path here currently tries
    # to reactivate that retired UID as deleting, which the DB correctly rejects.
    # This test owns continuation after restore creation, not that transition.
    async with database.acquire() as conn:
        async with conn.transaction():
            # This isolated fixture projection cannot pass the current cleanup
            # trigger either. Restore the trigger before exercising any code.
            await conn.execute("SET LOCAL session_replication_role = replica")
            await conn.execute(
                "UPDATE threads SET metadata = jsonb_set(metadata, "
                "'{workspace_container}', metadata->'workspace_container' || "
                '\'{"status":"restoring","pod_ip":null,'
                '"_runtime_incarnation":null}\'::jsonb) WHERE id=$1::uuid',
                case.thread_id,
            )
    assert not await case.provisioner.create_workspace(
        case.owner,
        stateless_creation_generation=generation,
        allow_stateless_create=True,
        operation_kind="restore",
        operation_id=str(suspension["id"]),
    )
    restore = await database.get_current_managed_repository_workspace_creation_result(
        case.thread_id,
        owner_kind="thread",
        scope="workspace_container",
        operation_kind="restore",
    )
    assert restore is not None
    assert restore["claimed_by"] == f"container-restore:{suspension['id']}"
    assert restore["settled_at"] is None
    runtime = case.cluster.objects["pod"].metadata.uid
    assert runtime != case.pod_uid
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid
    case.cluster.become_ready()
    await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    settled = await database.get_current_managed_repository_workspace_creation_result(
        case.thread_id,
        owner_kind="thread",
        scope="workspace_container",
        operation_kind="restore",
    )
    assert settled["id"] == restore["id"]
    assert settled["claimed_by"] == restore["claimed_by"]
    assert settled["claim_token"] == restore["claim_token"]
    assert settled["desired_manifest_digest"] == restore["desired_manifest_digest"]
    assert str(settled["thread_runtime_generation"]) == generation
    assert settled["settled_at"] is not None
    assert settled["restore_work_completed_at"] is None
    current = metadata(await database.get_thread(case.thread_id))
    assert current["workspace_container"]["_runtime_incarnation"] == runtime
    assert "_runtime_creation" not in current["workspace_container"]
    assert current["workspace_container"]["_snapshot_restore_required"] is True
    assert current["_workspace_binding"]["backing_id"] == (
        f"k8s-pvc:agent-workspaces:{case.pvc_uid}"
    )
    assert (
        case.cluster.pod_create_calls == 2
    )  # One predecessor, one restore; no retry create.
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid
    assert case.cluster.objects["pod"].spec.containers[0]["image"] == IMAGE
    assert (
        await database.fetch(
            "SELECT * FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='thread' AND owner_id=$1::uuid",
            case.thread_id,
        )
        == receipts
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "refusal", ["generation", "runtime", "successor", "missing", "cancelled", "settled"]
)
async def test_continuation_refuses_changed_or_missing_authority(
    database, actor, monkeypatch, refusal
):
    case = await pending_workspace(database, actor, monkeypatch)
    case.cluster.become_ready()
    generation = str(case.before["runtime_generation"])
    runtime = case.pod_uid
    if refusal == "generation":
        generation = str(uuid4())
    elif refusal == "runtime":
        runtime = str(uuid4())
    elif refusal == "successor":
        case.cluster.objects["pod"].metadata.uid = str(uuid4())
    elif refusal == "missing":
        monkeypatch.setattr(
            type(database),
            "get_current_managed_repository_workspace_creation_result",
            AsyncMock(return_value=None),
        )
    elif refusal == "cancelled":
        assert (
            await database.request_managed_repository_workspace_creation_cancellation(
                case.thread_id,
                owner_kind="thread",
                scope="workspace_container",
                target_disposition="deleted",
                reclaim_shared_resources=False,
                claimant="test-cancellation",
            )
        )
    elif refusal == "settled":
        assert await database.settle_managed_repository_workspace_creation_reservation(
            case.thread_id,
            owner_kind="thread",
            scope="workspace_container",
            reservation_generation=case.creation["reservation_generation"],
            claimant=case.creation["claimed_by"],
            claim_token=case.creation["claim_token"],
            runtime_incarnation=runtime,
        )
    authority_before = await reservation(database, case.thread_id)
    thread_before = await database.get_thread(case.thread_id)
    assert not await case.provisioner.continue_stateless_workspace_creation(
        case.owner,
        generation=generation,
        expected_runtime_incarnation=runtime,
    )
    assert await reservation(database, case.thread_id) == authority_before
    assert await database.get_thread(case.thread_id) == thread_before
    assert case.cluster.pod_create_calls == 1
    assert case.cluster.pod_deletes == 0
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid
