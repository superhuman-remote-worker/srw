"""Replay terminal cleanup after the exact Pod has already disappeared."""

from __future__ import annotations

from tests import _b09_control_seams as control_seams

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
import pytest_asyncio
import asyncpg
from kubernetes.client.exceptions import ApiException

from orchestrator.services.container_provisioner import ContainerProvisioner
from orchestrator.database.migrate import run_migrations
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests import test_non_pinned_workspace_lifecycle_real_postgres as lifecycle_tests
from tests.test_non_pinned_workspace_lifecycle_real_postgres import (
    _create_settled_authoritative_runtime,
    _execute_pre_0195,
)
from orchestrator.services import container_provisioner as container_provisioner_module


pg_dsn = lifecycle_tests.pg_dsn
db = lifecycle_tests.db


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    async with asyncpg.create_pool(pg_dsn, min_size=1, max_size=2) as pool:
        await run_migrations(
            pool,
            Path(__file__).resolve().parents[1]
            / "src/orchestrator/database/migrations/app",
        )


async def _capture(db, owner_id, runtime, *, owner_kind, pvc, service, reclaim=True):
    intent = await db.prepare_managed_repository_workspace_cleanup_intent(
        str(owner_id),
        owner_kind=owner_kind,
        scope="workspace_container",
        runtime_incarnation=str(runtime),
        target_disposition="deleted",
        reclaim_shared_resources=reclaim,
    )
    assert intent is not None
    claimed = await db.claim_managed_repository_workspace_cleanup_intent(
        str(intent["id"]),
        claimant="cleanup-retry-test",
    )
    assert claimed is not None
    captured = await db.record_managed_repository_workspace_cleanup_resources(
        str(intent["id"]),
        claimant=claimed["claimed_by"],
        claim_token=claimed["claim_token"],
        pod_uid=str(runtime),
        seed_configmap_uid=None,
        pvc_uid=str(pvc),
        service_uid=str(service),
    )
    assert captured is not None
    return captured


async def _soft_settled_reclaim(db):
    """Seed the prior release's soft-End result, then use real reclaim APIs."""
    thread, runtime, pvc, service, generation = (uuid4() for _ in range(5))
    metadata = {
        "workspace_container": {
            "provisioner": "k8s",
            "status": "deleted",
            "_runtime_incarnation": None,
            "_snapshot_restore_required": True,
        },
        "_stateless_workspace_retirement_settled": {
            "terminal_token": 8,
            "cleanup_complete": True,
            "permanent": True,
            "runtime_incarnation": str(runtime),
            "backing_id": f"k8s-pvc:agent-workspaces:{pvc}",
            "snapshot_restore_required": True,
        },
    }
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "INSERT INTO threads (id, status, execution_lane, runtime_generation, metadata) "
            "VALUES ($1, 'ended', 'stateless', $2, $3::jsonb)",
            thread,
            generation,
            json.dumps(metadata),
        )
        await conn.execute(
            "INSERT INTO run_queue (unit_id, unit_kind, state, lease_token) "
            "VALUES ($1, 'session_turn', 'done', 8)",
            thread,
        )
        await conn.execute(
            "INSERT INTO managed_repository_process_zero_receipts "
            "(owner_kind, owner_id, scope, provisioner, runtime_incarnation) "
            "VALUES ('thread', $1, 'workspace_container', 'k8s', $2)",
            thread,
            str(runtime),
        )
        prior = await conn.fetchrow(
            "INSERT INTO managed_repository_workspace_cleanup_intents ("
            "owner_kind, owner_id, thread_runtime_generation, scope, runtime_incarnation, "
            "target_disposition, resource_policy, reclaim_shared_resources, "
            "pod_uid, pvc_uid, service_uid, capture_complete, resources_captured_at, "
            "phase, cleanup_completed_at, settled_at, result_kind, projection_transaction_id) "
            "VALUES ('thread', $1, $3, 'workspace_container', $2, 'deleted', 'preserve', "
            "FALSE, $2, $4, $5, TRUE, now(), 'settled', now(), now(), 'settled', txid_current()) "
            "RETURNING *",
            thread,
            runtime,
            generation,
            pvc,
            service,
        )
    captured = await _capture(
        db,
        thread,
        runtime,
        owner_kind="thread",
        pvc=pvc,
        service=service,
    )
    return thread, runtime, prior, captured


def _absent_pod_provisioner(db, owner, intent):
    """Only Kubernetes transport is mocked; real receipt and intent APIs run."""
    p = ContainerProvisioner()
    p._db = db
    p._k8s_available = True
    p._namespace = "agent-workspaces"
    p._storage_class = "test-storage"
    p._core_api = MagicMock()
    p._core_api.read_namespaced_pod.side_effect = ApiException(status=404)
    p._core_api.read_namespaced_config_map.side_effect = ApiException(status=404)
    resources = {}
    for resource, uid, name, component in (
        ("pvc", intent["pvc_uid"], "pvc-" + owner.pod_name, "workspace-pvc"),
        ("service", intent["service_uid"], owner.pod_name, "workspace-svc"),
    ):
        metadata = SimpleNamespace(
            uid=str(uid),
            name=name,
            namespace=p._namespace,
            deletion_timestamp=None,
            labels={
                "app": "srw-workspace",
                "srw/component": component,
                "srw.io/component": "agent-workspace",
                owner.label_key: owner.id,
            },
        )
        if resource == "pvc":
            spec = SimpleNamespace(
                access_modes=["ReadWriteOnce"],
                storage_class_name=p._storage_class,
                volume_mode="Filesystem",
                selector=None,
                data_source=None,
                data_source_ref=None,
            )
        else:
            spec = SimpleNamespace(
                cluster_ip="None",
                type="ClusterIP",
                selector={"app": "srw-workspace", owner.label_key: owner.id},
                ports=[
                    SimpleNamespace(name=n, port=v, target_port=v, protocol="TCP")
                    for n, v in (("ssh", 30022), ("code-server", 38080), ("cdp", 9222))
                ],
            )
        resources[resource] = SimpleNamespace(metadata=metadata, spec=spec)

    def read(resource, **kwargs):
        if resource not in resources:
            raise ApiException(status=404)
        return resources[resource]

    def delete(resource, **kwargs):
        current = resources.get(resource)
        assert current is not None
        assert kwargs["body"]["preconditions"]["uid"] == current.metadata.uid
        del resources[resource]

    p._core_api.read_namespaced_persistent_volume_claim.side_effect = (
        lambda **kwargs: read("pvc", **kwargs)
    )
    p._core_api.read_namespaced_service.side_effect = lambda **kwargs: read(
        "service", **kwargs
    )
    p._core_api.delete_namespaced_persistent_volume_claim.side_effect = (
        lambda **kwargs: delete("pvc", **kwargs)
    )
    p._core_api.delete_namespaced_service.side_effect = lambda **kwargs: delete(
        "service", **kwargs
    )
    return p, resources


async def _receipts(db, owner):
    async with db.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM managed_repository_process_zero_receipts WHERE owner_id=$1 ORDER BY id",
            owner,
        )


@pytest.mark.asyncio
async def test_soft_end_then_permanent_cleanup_replays_exact_retired_pod(db):
    thread, runtime, prior, intent = await _soft_settled_reclaim(db)
    before = await _receipts(db, thread)
    owner = WorkspaceOwner.session(str(thread))
    p, resources = _absent_pod_provisioner(db, owner, intent)
    outcome = await p.reconcile_workspace_cleanup_intent(
        owner,
        expected_runtime_incarnation=str(runtime),
        intent_generation=intent["intent_generation"],
    )
    assert outcome.settled
    assert resources == {}
    assert await _receipts(db, thread) == before
    replay = await p.reconcile_workspace_cleanup_intent(
        owner,
        expected_runtime_incarnation=str(runtime),
        intent_generation=intent["intent_generation"],
    )
    assert replay.settled
    async with db.acquire() as conn:
        assert dict(
            await conn.fetchrow(
                "SELECT * FROM managed_repository_workspace_cleanup_intents WHERE id=$1",
                prior["id"],
            )
        ) == dict(prior)
    assert p._core_api.delete_namespaced_persistent_volume_claim.call_count == 1
    assert p._core_api.delete_namespaced_service.call_count == 1
    # Resource settlement is only complete when the normal owner lifecycle can
    # finish. Soft End previously left a null UID that the delete guard refused.
    await db.delete_thread(str(thread))
    assert await db.get_thread(str(thread)) is None


@pytest.mark.asyncio
async def test_absent_soft_cleanup_settles_intent_before_clearing_projection(db):
    thread, runtime, _reservation, _state = await _create_settled_authoritative_runtime(
        db, owner_kind="thread", scope="workspace_container"
    )
    intent = await _capture(
        db,
        thread,
        runtime,
        owner_kind="thread",
        pvc=uuid4(),
        service=uuid4(),
        reclaim=False,
    )
    assert await db.record_managed_repository_workspace_process_zero(
        str(thread),
        owner_kind="thread",
        scope="workspace_container",
        provisioner="k8s",
        runtime_incarnation=str(runtime),
    )
    owner = WorkspaceOwner.session(str(thread))
    p, resources = _absent_pod_provisioner(db, owner, intent)
    assert await p.release_absent_workspace(
        owner,
        expected_runtime_incarnation=str(runtime),
        reclaim_volume=False,
        strict=True,
    )
    stored = await db.get_thread(str(thread))
    metadata = stored["metadata"]
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    assert metadata["workspace_container"]["_runtime_incarnation"] is None
    assert metadata["workspace_container"]["status"] == "deleted"
    # The volume is deliberately kept for the resume window; the headless
    # Service is not — it is 409-idempotent to recreate and a Service that
    # outlives its Pod is a stale selector. Previously BOTH survived here,
    # because this soft path settles through the durable intent and the
    # Service delete sat inside the reclaim gate. See
    # knowledge-base/knowledge/issues/session_workspace_service_and_pvc_survive_end.md
    assert set(resources) == {"pvc"}
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT result_kind FROM managed_repository_workspace_cleanup_intents WHERE id=$1",
                intent["id"],
            )
            == "settled"
        )
    # Replay: the settled intent is idempotent, so a second pass repeats no
    # Kubernetes mutation. The Service delete the first pass performed is not
    # repeated and the retained volume is still not touched.
    service_deletes = p._core_api.delete_namespaced_service.call_count
    assert await p.release_absent_workspace(
        owner,
        expected_runtime_incarnation=str(runtime),
        reclaim_volume=False,
        strict=True,
    )
    assert set(resources) == {"pvc"}
    p._core_api.delete_namespaced_persistent_volume_claim.assert_not_called()
    assert p._core_api.delete_namespaced_service.call_count == service_deletes


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed",
    [
        None,
        "generation",
        "runtime",
        "queue_token",
        "queue_live",
        "queue_kind",
        "soft_only",
        "pending_overlap",
        "missing_receipt",
        "intent_generation",
        "intent_runtime_generation",
        "intent_token",
        "capture_incomplete",
        "unsettled",
        "active_owner",
    ],
)
async def test_previously_settled_null_projection_replays_only_exact_authority(
    db, changed
):
    thread, runtime, prior, intent = await _soft_settled_reclaim(db)
    owner = WorkspaceOwner.session(str(thread))
    p, resources = _absent_pod_provisioner(db, owner, intent)
    assert (
        await p.reconcile_workspace_cleanup_intent(
            owner,
            expected_runtime_incarnation=str(runtime),
            intent_generation=int(intent["intent_generation"]),
        )
    ).settled
    assert resources == {}
    async with db.acquire() as conn:
        # Reproduce the exact persisted shape left by the previous release.
        await _execute_pre_0195(
            conn,
            "UPDATE threads SET metadata=jsonb_set(metadata,"
            "'{workspace_container,_runtime_incarnation}','null'::jsonb) WHERE id=$1",
            thread,
        )
        statements = {
            "generation": (
                "UPDATE threads SET runtime_generation=$2 WHERE id=$1",
                thread,
                uuid4(),
            ),
            "runtime": (
                "UPDATE threads SET metadata=jsonb_set(metadata,"
                "'{workspace_container,_runtime_incarnation}',$2::jsonb) WHERE id=$1",
                thread,
                json.dumps(str(uuid4())),
            ),
            "queue_token": (
                "UPDATE run_queue SET lease_token=9 WHERE unit_id=$1",
                thread,
            ),
            "queue_live": (
                "UPDATE run_queue SET state='leased',leased_by='successor' WHERE unit_id=$1",
                thread,
            ),
            "queue_kind": (
                "UPDATE run_queue SET unit_kind='worker_batch' WHERE unit_id=$1",
                thread,
            ),
            "soft_only": (
                "UPDATE threads SET metadata=jsonb_set(metadata,"
                "'{_stateless_workspace_retirement_settled,permanent}','false'::jsonb) WHERE id=$1",
                thread,
            ),
            "pending_overlap": (
                "UPDATE threads SET metadata=jsonb_set(metadata,"
                "'{_stateless_workspace_retirement_pending}','true'::jsonb) WHERE id=$1",
                thread,
            ),
            "missing_receipt": (
                "DELETE FROM managed_repository_process_zero_receipts WHERE owner_id=$1",
                thread,
            ),
            "intent_runtime_generation": (
                "UPDATE managed_repository_workspace_cleanup_intents SET thread_runtime_generation=$2 WHERE id=$1",
                intent["id"],
                uuid4(),
            ),
            "intent_token": (
                "UPDATE managed_repository_workspace_cleanup_intents SET terminal_queue_token=9 WHERE id=$1",
                intent["id"],
            ),
            "capture_incomplete": (
                "UPDATE managed_repository_workspace_cleanup_intents SET "
                "capture_complete=false,resources_captured_at=NULL,seed_configmap_uid=NULL,"
                "pvc_uid=NULL,service_uid=NULL,result_kind=NULL,cleanup_completed_at=NULL,"
                "projection_transaction_id=NULL,settled_at=NULL,phase='prepared' WHERE id=$1",
                intent["id"],
            ),
            "unsettled": (
                "UPDATE managed_repository_workspace_cleanup_intents SET "
                "result_kind=NULL,cleanup_completed_at=NULL,projection_transaction_id=NULL,"
                "settled_at=NULL,phase='captured' WHERE id=$1",
                intent["id"],
            ),
            "active_owner": ("UPDATE threads SET status='active' WHERE id=$1", thread),
        }
        if changed in statements:
            await _execute_pre_0195(conn, *statements[changed])
        before = await conn.fetchval("SELECT metadata FROM threads WHERE id=$1", thread)
        receipts = await conn.fetch(
            "SELECT * FROM managed_repository_workspace_cleanup_intents WHERE owner_id=$1 ORDER BY id",
            thread,
        )
    generation = int(intent["intent_generation"])
    if changed == "intent_generation":
        generation = int(prior["intent_generation"])
    repaired = await db.restore_settled_thread_workspace_cleanup_projection(
        str(thread), runtime_incarnation=str(runtime), intent_generation=generation
    )
    assert repaired is (changed is None)
    async with db.acquire() as conn:
        assert (
            await conn.fetch(
                "SELECT * FROM managed_repository_workspace_cleanup_intents WHERE owner_id=$1 ORDER BY id",
                thread,
            )
            == receipts
        )
        if changed is not None:
            assert (
                await conn.fetchval("SELECT metadata FROM threads WHERE id=$1", thread)
                == before
            )
    if changed is None:
        # The normal reconciler also replays recovery without reissuing DELETE.
        assert (
            await p.reconcile_workspace_cleanup_intent(
                owner,
                expected_runtime_incarnation=str(runtime),
                intent_generation=generation,
            )
        ).settled
        await db.delete_thread(str(thread))
        assert await db.get_thread(str(thread)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [None, "missing_receipt", "replacement_during_replay"]
)
async def test_completed_job_cleanup_replays_captured_generation_after_pod_loss(
    db, monkeypatch, failure
):
    import orchestrator.main as main

    job, runtime, _reservation, _state = await _create_settled_authoritative_runtime(
        db,
        owner_kind="job",
        scope="workspace_container",
    )
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", job)
    assert await db.record_managed_repository_workspace_process_zero(
        str(job),
        owner_kind="job",
        scope="workspace_container",
        provisioner="k8s",
        runtime_incarnation=runtime,
    )
    intent = await _capture(
        db, job, runtime, owner_kind="job", pvc=uuid4(), service=uuid4()
    )
    p, resources = _absent_pod_provisioner(db, WorkspaceOwner.job(str(job)), intent)
    del resources["pvc"]  # First attempt already removed Pod, seed and PVC.
    if failure == "missing_receipt":
        async with db.acquire() as conn:
            await _execute_pre_0195(
                conn,
                "DELETE FROM managed_repository_process_zero_receipts WHERE owner_id=$1",
                job,
            )
    elif failure == "replacement_during_replay":
        owner = WorkspaceOwner.job(str(job))
        p._core_api.read_namespaced_pod.side_effect = [
            ApiException(status=404),
            SimpleNamespace(
                metadata=SimpleNamespace(
                    name=owner.pod_name,
                    namespace=p._namespace,
                    uid=str(uuid4()),
                    deletion_timestamp=None,
                    labels={
                        "app": "srw-workspace",
                        "srw/component": owner.component_label,
                        "srw.io/component": "agent-workspace",
                        owner.label_key: owner.id,
                    },
                )
            ),
        ]
    before = await _receipts(db, job)
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(container_provisioner_module, "container_provisioner", p)
    if failure is None:
        actions = await control_seams.archive_and_cleanup_workspace(str(job))
        assert actions == ["k8s workspace released"]
        assert resources == {}
    else:
        with pytest.raises(RuntimeError, match="exact teardown is incomplete"):
            await control_seams.archive_and_cleanup_workspace(str(job))
        assert set(resources) == {"service"}
        p._core_api.delete_namespaced_service.assert_not_called()
    assert await _receipts(db, job) == before
    async with db.acquire() as conn:
        saved = await conn.fetchrow(
            "SELECT id,result_kind FROM managed_repository_workspace_cleanup_intents WHERE owner_id=$1",
            job,
        )
    assert saved["id"] == intent["id"]
    assert saved["result_kind"] == ("settled" if failure is None else None)
    p._core_api.delete_namespaced_pod.assert_not_called()
    p._core_api.delete_namespaced_persistent_volume_claim.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed",
    [
        "missing_receipt",
        "wrong_receipt_runtime",
        "changed_queue_token",
        "wrong_owner",
        "wrong_intent",
        "preserve_only",
        "wrong_claimant",
        "wrong_claim_token",
        "expired_claim",
        "successor_runtime",
        "changed_thread_generation",
        "soft_only",
        "missing_prior",
        "wrong_prior_resource",
        "unexpected_seed",
        "superseding_marker",
        "future_preserve",
        "cross_owner_intent",
    ],
)
async def test_terminal_reclaim_receipt_rejects_changed_authority(db, changed):
    thread, runtime, prior, intent = await _soft_settled_reclaim(db)
    args = dict(
        runtime_incarnation=str(runtime),
        intent_id=str(intent["id"]),
        claimant=intent["claimed_by"],
        claim_token=intent["claim_token"],
    )
    if changed == "cross_owner_intent":
        thread, runtime, _other_prior, _other_intent = await _soft_settled_reclaim(db)
        args["runtime_incarnation"] = str(runtime)
    async with db.acquire() as conn:
        if changed in {"missing_receipt", "wrong_receipt_runtime"}:
            await _execute_pre_0195(
                conn,
                "DELETE FROM managed_repository_process_zero_receipts WHERE owner_id=$1",
                thread,
            )
            if changed == "wrong_receipt_runtime":
                await _execute_pre_0195(
                    conn,
                    "INSERT INTO managed_repository_process_zero_receipts (owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
                    "VALUES ('thread',$1,'workspace_container','k8s',$2)",
                    thread,
                    str(uuid4()),
                )
        elif changed == "changed_queue_token":
            await _execute_pre_0195(
                conn, "UPDATE run_queue SET lease_token=9 WHERE unit_id=$1", thread
            )
        elif changed == "expired_claim":
            await _execute_pre_0195(
                conn,
                "UPDATE managed_repository_workspace_cleanup_intents SET claim_expires_at=now()-interval '1 second' WHERE id=$1",
                intent["id"],
            )
        elif changed == "successor_runtime":
            await _execute_pre_0195(
                conn,
                "UPDATE threads SET metadata=jsonb_set(metadata,'{workspace_container,_runtime_incarnation}',$2::jsonb) WHERE id=$1",
                thread,
                json.dumps(str(uuid4())),
            )
        elif changed == "changed_thread_generation":
            await _execute_pre_0195(
                conn,
                "UPDATE threads SET runtime_generation=$2 WHERE id=$1",
                thread,
                uuid4(),
            )
        elif changed == "soft_only":
            await _execute_pre_0195(
                conn,
                "UPDATE threads SET metadata=jsonb_set(metadata,'{_stateless_workspace_retirement_settled,permanent}','false'::jsonb) WHERE id=$1",
                thread,
            )
        elif changed == "missing_prior":
            await _execute_pre_0195(
                conn,
                "DELETE FROM managed_repository_workspace_cleanup_intents WHERE id=$1",
                prior["id"],
            )
        elif changed == "wrong_prior_resource":
            await _execute_pre_0195(
                conn,
                "UPDATE managed_repository_workspace_cleanup_intents SET service_uid=$2 WHERE id=$1",
                prior["id"],
                uuid4(),
            )
        elif changed == "unexpected_seed":
            await _execute_pre_0195(
                conn,
                "UPDATE managed_repository_workspace_cleanup_intents SET seed_configmap_uid=$2 WHERE id=$1",
                intent["id"],
                uuid4(),
            )
        elif changed == "superseding_marker":
            await _execute_pre_0195(
                conn,
                "UPDATE threads SET metadata=jsonb_set(metadata,'{_stateless_claim_retirement}',$2::jsonb) WHERE id=$1",
                thread,
                json.dumps({"permanent": True, "terminal_token": 9}),
            )
            await _execute_pre_0195(
                conn, "UPDATE run_queue SET lease_token=9 WHERE unit_id=$1", thread
            )
            await _execute_pre_0195(
                conn,
                "UPDATE managed_repository_workspace_cleanup_intents SET terminal_queue_token=9 WHERE id=$1",
                intent["id"],
            )
        elif changed == "future_preserve":
            await _execute_pre_0195(
                conn,
                "UPDATE managed_repository_workspace_cleanup_intents SET intent_generation=$2 WHERE id=$1",
                prior["id"],
                intent["intent_generation"] + 10000000,
            )
    if changed == "wrong_owner":
        thread = uuid4()
    elif changed == "wrong_intent":
        args["intent_id"] = str(uuid4())
    elif changed == "preserve_only":
        args["intent_id"] = str(prior["id"])
    elif changed == "wrong_claimant":
        args["claimant"] = "other-worker"
    elif changed == "wrong_claim_token":
        args["claim_token"] += 1
    assert not await db.terminal_workspace_cleanup_process_zero_is_current(
        str(thread), **args
    )


@pytest.mark.asyncio
async def test_terminal_reclaim_preserves_a_same_name_successor(db):
    thread, runtime, _prior, intent = await _soft_settled_reclaim(db)
    owner = WorkspaceOwner.session(str(thread))
    p, resources = _absent_pod_provisioner(db, owner, intent)
    p._core_api.read_namespaced_pod.side_effect = None
    p._core_api.read_namespaced_pod.return_value = SimpleNamespace(
        metadata=SimpleNamespace(
            name=owner.pod_name,
            namespace=p._namespace,
            uid=str(uuid4()),
            labels={
                "app": "srw-workspace",
                "srw/component": owner.component_label,
                "srw.io/component": "agent-workspace",
                owner.label_key: owner.id,
            },
            deletion_timestamp=None,
        ),
        status=SimpleNamespace(phase="Running"),
    )
    before = await _receipts(db, thread)
    outcome = await p.reconcile_workspace_cleanup_intent(
        owner,
        expected_runtime_incarnation=str(runtime),
        intent_generation=intent["intent_generation"],
    )
    assert not outcome.settled
    assert set(resources) == {"pvc", "service"}
    p._core_api.delete_namespaced_pod.assert_not_called()
    p._core_api.delete_namespaced_persistent_volume_claim.assert_not_called()
    p._core_api.delete_namespaced_service.assert_not_called()
    assert await _receipts(db, thread) == before


@pytest.mark.asyncio
async def test_terminal_reclaim_locks_owner_before_rechecking_queue_token(db):
    thread, runtime, _prior, intent = await _soft_settled_reclaim(db)
    task = None
    try:
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.fetchrow(
                    "SELECT id FROM threads WHERE id=$1 FOR UPDATE", thread
                )
                task = asyncio.create_task(
                    db.terminal_workspace_cleanup_process_zero_is_current(
                        str(thread),
                        runtime_incarnation=str(runtime),
                        intent_id=str(intent["id"]),
                        claimant=intent["claimed_by"],
                        claim_token=intent["claim_token"],
                    )
                )
                await asyncio.sleep(0.05)
                assert not task.done()
                await _execute_pre_0195(
                    conn,
                    "UPDATE run_queue SET lease_token=9 WHERE unit_id=$1",
                    thread,
                )
        assert not await asyncio.wait_for(task, timeout=5)
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
