"""Exact retirement of a cancelled CREATE whose accepted reply was lost.

All PostgreSQL authorities come from the canonical migration chain. The first
regression catches a missing SQL permission: publishing only the retiring UID
must work without granting the cancelled creator normal creation authority.
"""

from __future__ import annotations

import asyncio
import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from orchestrator.database.migrate import run_migrations
from orchestrator.services.container_provisioner import (
    STATELESS_WORKSPACE_PROCESS_ZERO_FINALIZER,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests import test_non_pinned_workspace_lifecycle_real_postgres as lifecycle_tests
from tests import test_workspace_pull_failure_real_postgres as pull_failure_tests
from tests.test_container_provisioner import _pod_from_manifest, _pvc_from_manifest

db = lifecycle_tests.db
pg_dsn = lifecycle_tests.pg_dsn


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    async with asyncpg.create_pool(pg_dsn, min_size=1, max_size=2) as pool:
        await run_migrations(
            pool,
            Path(__file__).resolve().parents[1]
            / "src/orchestrator/database/migrations/app",
        )


async def _cancelled_unbound(
    db, *, workspace_extra=None, workspace_shape="nonempty", accepted_effect=None
):
    job, pod, pvc = uuid4(), uuid4(), uuid4()
    initial = {"retained": {"audit": "unchanged"}}
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs(id,description,status,execution_lane,context) "
            "VALUES($1,'accepted reply lost','created','stateless',$2::jsonb)",
            job,
            json.dumps(initial),
        )
    creator = f"container-create:{uuid4()}"
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job),
        owner_kind="job",
        scope="workspace_container",
        claimant=creator,
        desired_manifest_digest="a" * 64,
    )
    assert reservation is not None
    exact = {
        "owner_kind": "job",
        "scope": "workspace_container",
        "reservation_generation": int(reservation["reservation_generation"]),
        "claimant": creator,
        "claim_token": int(reservation["claim_token"]),
    }
    for resource, uid in (("pvc", pvc), ("pod", pod)):
        assert (
            await db.begin_managed_repository_workspace_creation_effect(
                str(job), **exact, resource_kind=resource
            )
            is not None
        )
        if accepted_effect is not None:
            accepted_effect(resource, uid, job, reservation["id"])
        if resource == "pvc":
            assert await db.record_managed_repository_workspace_creation_resource(
                str(job), **exact, resource_kind=resource, resource_uid=str(uid)
            )
    # The accepted Pod exists, but its reply never reached normal UID publication.
    # The actual retained shape is nonempty with all five authority keys absent.
    if workspace_shape != "absent":
        initial["workspace_container"] = (
            {}
            if workspace_shape == "empty"
            else {
                "startup_diagnostics": {"accepted_reply": "lost"},
                **(workspace_extra or {}),
            }
        )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1", job, json.dumps(initial)
        )
    await db.update_job_status(
        str(job), status="failed", error_message="Workspace CREATE reply lost"
    )
    assert await db.cancel_job(str(job))
    claimant = f"creation-reconciler:{uuid4()}"
    reservation = await db.request_managed_repository_workspace_creation_cancellation(
        str(job),
        owner_kind="job",
        scope="workspace_container",
        target_disposition="deleted",
        reclaim_shared_resources=True,
        claimant=claimant,
    )
    assert reservation is not None
    exact.update(claimant=claimant, claim_token=int(reservation["claim_token"]))
    assert await db.authorize_cancelled_workspace_creation_runtime_for_reconciliation(
        str(job), **exact, runtime_incarnation=str(pod)
    )
    assert await db.record_cancelled_workspace_creation_resource_for_reconciliation(
        str(job), **exact, resource_kind="pvc", resource_uid=str(pvc)
    )
    return job, pod, pvc, reservation, exact, initial


async def _projection(db, job):
    async with db.acquire() as conn:
        value = await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job)
    return json.loads(value) if isinstance(value, str) else value


async def _rows(db, job):
    async with db.acquire() as conn:
        return dict(
            await conn.fetchrow(
                "SELECT (SELECT row_to_json(j)::text FROM jobs j WHERE id=$1) AS owner, "
                "(SELECT json_agg(r ORDER BY id)::text FROM "
                "managed_repository_workspace_creation_reservations r WHERE owner_id=$1) AS creation, "
                "(SELECT json_agg(i ORDER BY id)::text FROM "
                "managed_repository_workspace_cleanup_intents i WHERE owner_id=$1) AS cleanup",
                job,
            )
        )


async def _convert(db, job, pod, exact):
    return await db.convert_cancelled_workspace_creation_to_cleanup_intent(
        str(job),
        **exact,
        runtime_incarnation=str(pod),
        allow_existing_terminal_intent=True,
    )


def _retirement(initial, pod, reservation, exact):
    return {
        **copy.deepcopy(initial),
        "workspace_container": {
            **copy.deepcopy(initial.get("workspace_container", {})),
            "status": "retiring_process_zero",
            "provisioner": "k8s",
            "_runtime_incarnation": str(pod),
            "_creation_reservation_id": str(reservation["id"]),
            "_creation_claim_token": str(exact["claim_token"]),
        },
    }


async def _proof(db, job, initial, candidate):
    async with db.acquire() as conn:
        return await conn.fetchval(
            "SELECT managed_repo_cancelled_creation_retirement_is_authorized_now("
            "'job',$1,'workspace_container',$2::jsonb,$3::jsonb)",
            job,
            json.dumps(initial),
            json.dumps(candidate),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["nonempty", "absent", "empty"])
async def test_cancelled_accepted_reply_loss_binds_only_exact_retirement(db, shape):
    job, pod, pvc, reservation, exact, initial = await _cancelled_unbound(
        db, workspace_shape=shape
    )
    async with db.workspace_runtime_mutation_lock(
        str(job), owner_kind="job", scope="workspace_container", wait=False
    ) as acquired:
        assert acquired
        try:
            intent = await db.convert_cancelled_workspace_creation_to_cleanup_intent(
                str(job),
                **exact,
                runtime_incarnation=str(pod),
                allow_existing_terminal_intent=True,
            )
        except asyncpg.CheckViolationError as error:
            pytest.fail(
                "exact cancelled retirement rejected by "
                f"{error.constraint_name}: {error.message}",
                pytrace=False,
            )
    assert intent is not None
    assert intent["target_disposition"] == "deleted"
    assert intent["resource_policy"] == "terminal_reclaim"
    assert intent["capture_complete"] is True
    assert str(intent["pod_uid"]) == str(pod)
    assert str(intent["pvc_uid"]) == str(pvc)
    expected = _retirement(initial, pod, reservation, exact)
    assert await _projection(db, job) == expected
    async with db.acquire() as conn:
        creation = await conn.fetchrow(
            "SELECT phase,result_kind,settled_at FROM "
            "managed_repository_workspace_creation_reservations WHERE id=$1",
            reservation["id"],
        )
        assert dict(creation) == {
            "phase": "aborted",
            "result_kind": "aborted",
            "settled_at": creation["settled_at"],
        }
        assert creation["settled_at"] is not None
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM managed_repository_workspace_cleanup_intents WHERE owner_id=$1",
                job,
            )
            == 1
        )
        assert not await conn.fetchval(
            "SELECT managed_repository_workspace_creation_is_authorized($1,$2,$3,$4,$5,$6)",
            "job",
            job,
            "workspace_container",
            str(pod),
            str(reservation["id"]),
            str(exact["claim_token"]),
        )
    before = await _rows(db, job)
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context,'{workspace_container,status}',"
                "'\"ready\"'::jsonb) WHERE id=$1",
                job,
            )
    assert await _rows(db, job) == before


@pytest.mark.asyncio
async def test_background_sweep_retires_one_accepted_unpublished_pod(db, monkeypatch):
    """Actual DB Cancel and background cleanup; only the Kubernetes API is fake.

    The fixture issues the production reservation/effect protocol, persists one
    accepted Pod, and loses its CREATE reply. It does not call the public HTTP
    route or the full workspace creator, which has separate regression coverage.
    """
    cluster = pull_failure_tests.NeverPullingCluster()
    provisioner = pull_failure_tests._provisioner(monkeypatch, db, cluster)
    creates = []

    def accepted_effect(resource, uid, job, reservation_id):
        owner = WorkspaceOwner.job(str(job))
        metadata = {
            "name": (f"pvc-{owner.pod_name}" if resource == "pvc" else owner.pod_name),
            "namespace": provisioner._namespace,
            "labels": {
                "app": "srw-workspace",
                "srw/job-id": str(job),
                "srw/component": "workspace-pvc" if resource == "pvc" else "workspace",
                "srw.io/component": "agent-workspace",
            },
            "annotations": {
                "srw.io/workspace-creation-reservation": str(reservation_id)
            },
        }
        if resource == "pvc":
            cluster.objects["pvc"] = _pvc_from_manifest(
                {
                    "metadata": metadata,
                    "spec": {
                        "accessModes": ["ReadWriteOnce"],
                        "storageClassName": "test-storage",
                    },
                },
                uid=str(uid),
            )
            return

        def create_and_lose_reply():
            assert "pod" not in cluster.objects
            creates.append(str(uid))
            metadata["finalizers"] = [STATELESS_WORKSPACE_PROCESS_ZERO_FINALIZER]
            pod = _pod_from_manifest(
                {
                    "metadata": metadata,
                    "spec": {
                        "volumes": [
                            {
                                "name": "workspace-data",
                                "persistentVolumeClaim": {
                                    "claimName": f"pvc-{owner.pod_name}"
                                },
                            }
                        ],
                        "containers": [
                            {
                                "name": "workspace",
                                "volumeMounts": [
                                    {
                                        "name": "workspace-data",
                                        "mountPath": "/home/agent-host",
                                    }
                                ],
                            }
                        ],
                    },
                },
                uid=str(uid),
            )
            pod.spec.containers = [
                SimpleNamespace(
                    name="workspace",
                    volume_mounts=[
                        SimpleNamespace(
                            name="workspace-data", mount_path="/home/agent-host"
                        )
                    ],
                )
            ]
            pod.spec.node_name = "test-node"
            pod.spec.restart_policy = "Never"
            pod.metadata.finalizers = list(metadata["finalizers"])
            pod.metadata.creation_timestamp = datetime.now(timezone.utc)
            pod.status.container_statuses[0].state = SimpleNamespace(
                running=SimpleNamespace(started_at=datetime.now(timezone.utc)),
                waiting=None,
                terminated=None,
            )
            cluster.objects["pod"] = pod
            raise TimeoutError("accepted CREATE reply lost")

        with pytest.raises(TimeoutError, match="accepted CREATE reply lost"):
            create_and_lose_reply()

    job, pod, _, reservation, _, initial = await _cancelled_unbound(
        db, accepted_effect=accepted_effect
    )
    assert await _projection(db, job) == initial

    # A process restart leaves the old reconciler lease expired. The ordinary
    # sweep must claim a fresh token and preserve the already accepted Pod UID.
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET expires_at=created_at+interval '1 microsecond' WHERE id=$1",
            reservation["id"],
        )
    counts = await provisioner.reconcile_pending_workspace_cleanup_intents(limit=25)
    # This module retains other owners' negative fixtures. Their unresolved
    # intents may be refused by this one-owner fake; only this owner must settle.
    assert counts["settled"] == 1
    assert counts["superseded"] == 0
    assert creates == [str(pod)]
    assert cluster.pod_deletes == 1
    assert cluster.objects == {}
    final = await _projection(db, job)
    assert final["retained"] == initial["retained"]
    assert (
        final["workspace_container"]["startup_diagnostics"]
        == initial["workspace_container"]["startup_diagnostics"]
    )
    assert final["workspace_container"]["status"] == "deleted"
    assert final["workspace_container"]["provisioner"] == "k8s"
    assert final["workspace_container"]["_runtime_incarnation"] == str(pod)
    assert final["workspace_container"]["_creation_reservation_id"] == str(
        reservation["id"]
    )
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM managed_repository_workspace_cleanup_intents "
                "WHERE owner_id=$1 AND runtime_incarnation=$2 AND result_kind='settled'",
                job,
                pod,
            )
            == 1
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM managed_repository_process_zero_receipts "
                "WHERE owner_id=$1 AND runtime_incarnation=$2",
                job,
                str(pod),
            )
            == 1
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM managed_repository_workspace_creation_reservations "
                "WHERE owner_id=$1 AND settled_at IS NULL",
                job,
            )
            == 0
        )
    replay = await provisioner.reconcile_pending_workspace_cleanup_intents(limit=25)
    assert replay["settled"] == 0
    assert replay["superseded"] == 0
    assert creates == [str(pod)]
    assert cluster.pod_deletes == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "extra", [{"pod_name": "workspace-other"}, {"endpoint": {"host": "other"}}]
)
async def test_old_runtime_authority_cannot_be_preserved_under_new_retirement_uid(
    db, extra
):
    job, pod, _pvc, _reservation, exact, _initial = await _cancelled_unbound(
        db, workspace_extra=extra
    )
    before = await _rows(db, job)
    assert await _convert(db, job, pod, exact) is None
    assert await _rows(db, job) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "extra",
    [
        {"status": None},
        {"provisioner": None},
        {"_runtime_incarnation": None},
        {"_creation_reservation_id": None},
        {"_creation_claim_token": None},
        {"status": False},
        {"provisioner": False},
        {"_creation_claim_token": False},
        {"pod_ip": False},
        {"container_id": None},
    ],
)
async def test_present_null_or_false_authority_is_not_an_unbound_diagnostic(db, extra):
    job, pod, _pvc, _reservation, exact, _initial = await _cancelled_unbound(
        db, workspace_extra=extra
    )
    before = await _rows(db, job)
    assert await _convert(db, job, pod, exact) is None
    assert await _rows(db, job) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "ready",
        "provisioner",
        "token",
        "reservation",
        "runtime",
        "outer-content",
        "drop-diagnostic",
        "extra-endpoint",
        "missing-token",
        "null-runtime",
    ],
)
async def test_generic_json_update_cannot_use_retirement_as_creation_authority(
    db, fault
):
    job, pod, _pvc, reservation, exact, initial = await _cancelled_unbound(db)
    candidate = _retirement(initial, pod, reservation, exact)
    workspace = candidate["workspace_container"]
    if fault == "ready":
        workspace["status"] = "ready"
    elif fault == "provisioner":
        workspace["provisioner"] = "docker"
    elif fault == "token":
        workspace["_creation_claim_token"] = str(exact["claim_token"] - 1)
    elif fault == "reservation":
        workspace["_creation_reservation_id"] = str(uuid4())
    elif fault == "runtime":
        workspace["_runtime_incarnation"] = str(uuid4())
    elif fault == "outer-content":
        candidate["retained"]["audit"] = "changed"
    elif fault == "drop-diagnostic":
        del workspace["startup_diagnostics"]
    elif fault == "extra-endpoint":
        workspace["endpoint"] = {"host": "other"}
    elif fault == "missing-token":
        del workspace["_creation_claim_token"]
    elif fault == "null-runtime":
        workspace["_runtime_incarnation"] = None
    before = await _rows(db, job)
    assert await _proof(db, job, initial, candidate) is False
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                job,
                json.dumps(candidate),
            )
    assert await _rows(db, job) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "stale-token",
        "wrong-claimant",
        "wrong-runtime",
        "wrong-owner-kind",
        "wrong-scope",
        "expired",
        "preserve",
        "wrong-capture",
        "missing-issued",
        "missing-observed",
        "bool-effect-token",
        "future-observed",
        "unresolved-effect",
        "pinned",
        "failed",
        "retired",
        "missing-storage-opt-in",
    ],
)
async def test_conversion_false_proof_leaves_all_authority_rows_unchanged(db, fault):
    job, pod, _pvc, reservation, exact, initial = await _cancelled_unbound(db)
    requested = dict(exact)
    requested_pod = pod
    async with db.acquire() as conn:
        if fault == "stale-token":
            requested["claim_token"] -= 1
        elif fault == "wrong-claimant":
            requested["claimant"] = "different-cleaner"
        elif fault == "wrong-runtime":
            requested_pod = uuid4()
        elif fault == "wrong-owner-kind":
            requested["owner_kind"] = "thread"
        elif fault == "wrong-scope":
            requested["scope"] = "ide"
        elif fault == "expired":
            await conn.execute(
                "UPDATE managed_repository_workspace_creation_reservations "
                "SET expires_at=now()-interval '1 millisecond' WHERE id=$1",
                reservation["id"],
            )
        elif fault == "preserve":
            await conn.execute(
                "UPDATE managed_repository_workspace_creation_reservations "
                "SET cancel_resource_policy='preserve' WHERE id=$1",
                reservation["id"],
            )
        elif fault in {
            "wrong-capture",
            "missing-issued",
            "missing-observed",
            "bool-effect-token",
            "future-observed",
            "unresolved-effect",
        }:
            value = await conn.fetchval(
                "SELECT external_effects FROM managed_repository_workspace_creation_reservations "
                "WHERE id=$1",
                reservation["id"],
            )
            effects = json.loads(value) if isinstance(value, str) else value
            if fault == "wrong-capture":
                effects["pvc"]["observed_uid"] = str(uuid4())
            elif fault == "missing-issued":
                effects["pod"].pop("issued_at")
            elif fault == "missing-observed":
                effects["pod"].pop("observed_at")
            elif fault == "bool-effect-token":
                effects["pod"]["claim_token"] = True
            elif fault == "future-observed":
                effects["pod"]["observed_at"] = "2099-01-01T00:00:00Z"
            elif fault == "unresolved-effect":
                effects["seed"] = {
                    **effects["pod"],
                    "observed_uid": None,
                    "ambiguity_until": "2099-01-01T00:00:00Z",
                }
            await conn.execute(
                "UPDATE managed_repository_workspace_creation_reservations "
                "SET external_effects=$2::jsonb WHERE id=$1",
                reservation["id"],
                json.dumps(effects),
            )
        elif fault == "pinned":
            await conn.execute(
                "UPDATE jobs SET execution_lane='pinned' WHERE id=$1", job
            )
        elif fault == "retired":
            await conn.execute(
                "INSERT INTO managed_repository_process_zero_receipts "
                "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
                "VALUES('job',$1,'workspace_container','k8s',$2)",
                job,
                str(pod),
            )
    if fault == "failed":
        await db.update_job_status(
            str(job), status="failed", error_message="still failed"
        )
        refreshed = await db.request_managed_repository_workspace_creation_cancellation(
            str(job),
            owner_kind="job",
            scope="workspace_container",
            target_disposition="deleted",
            reclaim_shared_resources=True,
            claimant=exact["claimant"],
        )
        requested["claim_token"] = int(refreshed["claim_token"])
    before = await _rows(db, job)
    result = await db.convert_cancelled_workspace_creation_to_cleanup_intent(
        str(job),
        **requested,
        runtime_incarnation=str(requested_pod),
        allow_existing_terminal_intent=fault != "missing-storage-opt-in",
    )
    assert result is None
    assert await _rows(db, job) == before


@pytest.mark.asyncio
async def test_two_current_conversions_admit_one_cleanup_generation(db):
    job, pod, _pvc, _reservation, exact, _initial = await _cancelled_unbound(db)
    results = await asyncio.gather(
        _convert(db, job, pod, exact), _convert(db, job, pod, exact)
    )
    assert sum(isinstance(result, dict) for result in results) == 1
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM managed_repository_workspace_cleanup_intents WHERE owner_id=$1",
                job,
            )
            == 1
        )


@pytest.mark.asyncio
async def test_conversion_waiting_on_owner_rechecks_rotated_claim(db):
    job, pod, _pvc, reservation, exact, _initial = await _cancelled_unbound(db)
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.fetchval("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job)
            conversion = asyncio.create_task(_convert(db, job, pod, exact))
            for _ in range(100):
                waiting = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM pg_locks waiter JOIN pg_locks holder "
                    "ON holder.transactionid=waiter.transactionid "
                    "WHERE NOT waiter.granted AND waiter.locktype='transactionid' "
                    "AND holder.pid=pg_backend_pid() AND holder.granted)"
                )
                if waiting:
                    break
                await asyncio.sleep(0.01)
            assert waiting
            await conn.execute(
                "UPDATE managed_repository_workspace_creation_reservations "
                "SET claimed_by='replacement-cleaner',claim_token=nextval("
                "'managed_repository_workspace_creation_claim_seq'),expires_at=now()+interval '5 minutes' "
                "WHERE id=$1",
                reservation["id"],
            )
    before = await _rows(db, job)
    assert await asyncio.wait_for(conversion, timeout=5) is None
    assert await _rows(db, job) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["status", "lane", "id", "assignment"])
async def test_retirement_json_cannot_be_combined_with_owner_reactivation(db, change):
    job, pod, _pvc, reservation, exact, initial = await _cancelled_unbound(db)
    candidate = _retirement(initial, pod, reservation, exact)
    before = await _rows(db, job)
    async with db.acquire() as conn:
        agent = uuid4()
        if change == "assignment":
            await conn.execute(
                "INSERT INTO agents(id,config_name) VALUES($1,'retirement-negative')",
                agent,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            if change == "status":
                await conn.execute(
                    "UPDATE jobs SET context=$2::jsonb,status='created' WHERE id=$1",
                    job,
                    json.dumps(candidate),
                )
            elif change == "lane":
                await conn.execute(
                    "UPDATE jobs SET context=$2::jsonb,execution_lane='pinned' WHERE id=$1",
                    job,
                    json.dumps(candidate),
                )
            elif change == "id":
                await conn.execute(
                    "UPDATE jobs SET context=$2::jsonb,id=$3 WHERE id=$1",
                    job,
                    json.dumps(candidate),
                    uuid4(),
                )
            else:
                await conn.execute(
                    "UPDATE jobs SET context=$2::jsonb,assigned_agent_id=$3 WHERE id=$1",
                    job,
                    json.dumps(candidate),
                    agent,
                )
    assert await _rows(db, job) == before


@pytest.mark.asyncio
async def test_prior_settled_cleanup_for_same_uid_cannot_be_rebound(db):
    job, pod, pvc, _reservation, exact, _initial = await _cancelled_unbound(db)
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO managed_repository_workspace_cleanup_intents "
            "(owner_kind,owner_id,scope,runtime_incarnation,pod_uid,pvc_uid,"
            "target_disposition,resource_policy,capture_complete,resources_captured_at,"
            "cleanup_completed_at,settled_at,result_kind,phase) "
            "VALUES('job',$1,'workspace_container',$2,$2,$3,'deleted','preserve',"
            "TRUE,now(),now(),now(),'superseded','superseded')",
            job,
            pod,
            pvc,
        )
    before = await _rows(db, job)
    assert await _convert(db, job, pod, exact) is None
    assert await _rows(db, job) == before


@pytest.mark.asyncio
async def test_retirement_rechecks_lease_after_transaction_started(db):
    job, pod, _pvc, reservation, exact, initial = await _cancelled_unbound(db)
    candidate = _retirement(initial, pod, reservation, exact)
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "UPDATE managed_repository_workspace_creation_reservations "
                "SET expires_at=clock_timestamp()+interval '30 milliseconds' WHERE id=$1",
                reservation["id"],
            )
            await conn.execute("SELECT pg_sleep(0.05)")
            assert await conn.fetchval(
                "SELECT expires_at<clock_timestamp() AND expires_at>now() "
                "FROM managed_repository_workspace_creation_reservations WHERE id=$1",
                reservation["id"],
            )
            assert not await conn.fetchval(
                "SELECT managed_repo_cancelled_creation_retirement_is_authorized_now("
                "'job',$1,'workspace_container',$2::jsonb,$3::jsonb)",
                job,
                json.dumps(initial),
                json.dumps(candidate),
            )
            with pytest.raises(asyncpg.CheckViolationError):
                async with conn.transaction():
                    await conn.execute(
                        "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                        job,
                        json.dumps(candidate),
                    )
    assert await _projection(db, job) == initial


@pytest.mark.asyncio
async def test_expiry_during_intent_insert_rolls_back_complete_handoff(db):
    job, pod, _pvc, reservation, exact, _initial = await _cancelled_unbound(db)
    async with db.acquire() as conn:
        await conn.execute(
            "CREATE FUNCTION pause_retirement_test_insert() RETURNS trigger "
            "LANGUAGE plpgsql AS $$ BEGIN "
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET expires_at=clock_timestamp()+interval '30 milliseconds' "
            "WHERE owner_id=NEW.owner_id AND scope='workspace_container' "
            "AND settled_at IS NULL; "
            "PERFORM pg_sleep(0.05); RETURN NEW; END; $$"
        )
        await conn.execute(
            "CREATE TRIGGER pause_retirement_test_insert BEFORE INSERT ON "
            "managed_repository_workspace_cleanup_intents FOR EACH ROW "
            f"WHEN (NEW.owner_id='{job}'::uuid) "
            "EXECUTE FUNCTION pause_retirement_test_insert()"
        )
        try:
            # Shorten the lease only inside the insertion trigger, after both
            # projection predicates have succeeded. This deterministic test
            # clock avoids expiring while queued behind a busy test database.
            before = await _rows(db, job)
            with pytest.raises(
                RuntimeError, match="cancelled creation handoff was lost"
            ):
                await _convert(db, job, pod, exact)
            assert await _rows(db, job) == before
        finally:
            await conn.execute(
                "DROP TRIGGER pause_retirement_test_insert ON "
                "managed_repository_workspace_cleanup_intents"
            )
            await conn.execute("DROP FUNCTION pause_retirement_test_insert()")
