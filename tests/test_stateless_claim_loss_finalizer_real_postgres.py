"""Real-PostgreSQL proof that a stolen stateless claim's hold settles.

stateless_claim_loss_hold_never_settles_without_pod_finalizer: a reaper steal
parks the turn behind a claimant-loss hold whose only automatic settlement is
observing the exact claimant UID with every container terminated. A
finalizer-free executor Pod object disappears ~0.7 s after its containers stop,
the reconciler ticks every 15 s and — correctly — never treats a 404 as
process-zero proof, so the hold never settled and the user saw "generating"
forever.

These tests drive the production reaper cycle against the real schema and an
in-memory apiserver whose executor Pods carry exactly the finalizers the Helm
chart renders, so the chart and the reconciler are proven together.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
import yaml
from fastapi import HTTPException
from testcontainers.postgres import PostgresContainer

from orchestrator.services import agent_provisioner as provisioner_module
from orchestrator.services import run_queue_admin
from orchestrator.services import run_queue_reaper as reaper
from orchestrator.services.agent_provisioner import AgentProvisioner, agent_provisioner
from tests._fake_executor_k8s import (
    EXECUTOR_NAMESPACE,
    POOL_INSTANCE,
    POOL_NAME,
    FakeExecutorCoreApi,
    executor_pod,
)

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"
SCHEMA_FILE = ROOT / "src" / "orchestrator" / "database" / "schema_current.sql"

POD = "srw-agent-stateless-68d8c97ff6-w97cz"
POD_UID = "072ccef4-0000-4000-8000-000000000001"


def _rendered_executor_finalizers() -> list[str] | None:
    """Finalizers the chart stamps on every stateless executor Pod."""

    if shutil.which("helm") is None:
        pytest.skip("Helm is not installed")
    rendered = subprocess.run(
        [
            "helm",
            "template",
            "claim-loss-finalizer-test",
            str(CHART),
            "-f",
            str(CHART / "ci/test-values.yaml"),
            "--set",
            "agent.stateless.enabled=true",
            "--set",
            "agent.stateless.autoscaling.enabled=false",
            "--show-only",
            "templates/agent/stateless-deployment.yaml",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    deployment = next(
        document
        for document in yaml.safe_load_all(rendered)
        if document and document.get("kind") == "Deployment"
    )
    return deployment["spec"]["template"]["metadata"].get("finalizers")


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:
        pytest.skip(f"local Postgres container unavailable: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA_FILE.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def pool(pg_dsn, _schema_applied):
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE run_queue, thread_events, threads, security_events CASCADE"
        )
    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
def fake_k8s(monkeypatch):
    """Point the orchestrator's provisioner singleton at the fake apiserver."""

    api = FakeExecutorCoreApi()
    monkeypatch.setattr(agent_provisioner, "_core_api", api)
    monkeypatch.setattr(agent_provisioner, "_k8s_available", True)
    monkeypatch.setattr(agent_provisioner, "_namespace", EXECUTOR_NAMESPACE)
    monkeypatch.setenv("AGENT_LABEL_NAME", POOL_NAME)
    monkeypatch.setenv("AGENT_LABEL_INSTANCE", POOL_INSTANCE)
    monkeypatch.setattr(reaper, "_ABSENT_CLAIMANT_ANOMALIES", set(), raising=False)
    monkeypatch.setattr(reaper, "_CLAIM_LOSS_RECONCILE_CURSOR", None, raising=False)
    return api


def _fresh_provisioner(api: FakeExecutorCoreApi) -> AgentProvisioner:
    provisioner = AgentProvisioner()
    provisioner._core_api = api
    provisioner._k8s_available = True
    provisioner._namespace = EXECUTOR_NAMESPACE
    return provisioner


async def _insert_thread(conn, thread_id, metadata) -> None:
    await conn.execute(
        "INSERT INTO threads (id, status, execution_lane, metadata) "
        "VALUES ($1, 'active', 'stateless', $2::jsonb)",
        thread_id,
        json.dumps(metadata),
    )


async def _insert_expired_claim(conn, thread_id, *, pod=POD, pod_uid=POD_UID):
    """The doc's timeline at 05:39:48: bundle stamped, renewals stopped."""

    await _insert_thread(
        conn,
        thread_id,
        {
            "_stateless_active_claim": {
                "lease_token": 1,
                "pod": pod,
                "pod_uid": pod_uid,
                "credential_bound_at": "2026-09-03T05:38:12+00:00",
            }
        },
    )
    await conn.execute(
        """
        INSERT INTO run_queue (
            unit_id, unit_kind, state, lease_token, leased_by, last_leased_by,
            leased_until, input_seq, consumed_seq, attempts_since_completion,
            input_delivery_capable_lease_token
        ) VALUES ($1, 'session_turn', 'leased', 1, $2, $2,
                  now() - interval '5 minutes', 70804, 70803, 1, 1)
        """,
        thread_id,
        pod,
    )


async def _insert_held_unit(conn, thread_id, *, pod=POD, pod_uid=POD_UID):
    """A thread already parked behind an unsettled claimant-loss hold."""

    await _insert_thread(
        conn,
        thread_id,
        {
            "_stateless_claim_losses": {
                "1": {
                    "pod": pod,
                    "pod_uid": pod_uid,
                    "quiesced": False,
                    "eviction_requested_at": "2026-09-03T05:39:48+00:00",
                }
            },
            "_stateless_claim_loss_hold": {
                "lease_token": 2,
                "intended_state": "queued",
                "attempts_since_completion": 1,
                "queued_at": "2026-09-03T05:39:48+00:00",
                "run_after": "2026-09-03T05:39:48+00:00",
            },
        },
    )
    await conn.execute(
        """
        INSERT INTO run_queue (
            unit_id, unit_kind, state, lease_token, input_seq, consumed_seq,
            attempts_since_completion, park_reason, parked_at
        ) VALUES ($1, 'session_turn', 'parked', 2, 70804, 70803, 1,
                  'claim_loss_hold', now())
        """,
        thread_id,
    )


async def _queue(conn, thread_id):
    return await conn.fetchrow(
        "SELECT state, park_reason, lease_token, leased_by FROM run_queue "
        "WHERE unit_id = $1",
        thread_id,
    )


async def _metadata(conn, thread_id) -> dict:
    raw = await conn.fetchval("SELECT metadata FROM threads WHERE id = $1", thread_id)
    return json.loads(raw) if isinstance(raw, str) else dict(raw)


@pytest.mark.asyncio
async def test_reaper_steal_hold_settles_after_the_retained_claimant_terminates(
    pool, fake_k8s, monkeypatch
):
    """Steal -> evict -> kubelet stops containers -> settle -> finalizer release."""

    fake_k8s.pods[POD] = executor_pod(
        name=POD, uid=POD_UID, finalizers=_rendered_executor_finalizers()
    )
    thread_id = uuid4()
    async with pool.acquire() as conn:
        await _insert_expired_claim(conn, thread_id)

        assert await reaper.reap_cycle(conn, grace_seconds=30) == 1
        queue = await _queue(conn, thread_id)
        assert (queue["state"], queue["park_reason"]) == ("parked", "claim_loss_hold")
        assert fake_k8s.deletes == [(POD, {"preconditions": {"uid": POD_UID}}, 180)]
        assert POD in fake_k8s.pods, "a deleting claimant is retained, never released"

        # The claimant never ACKs (the 0c1aeea7 shape); the kubelet SIGKILLs it.
        fake_k8s.kubelet_terminates(POD)
        assert POD in fake_k8s.pods, (
            "the chart's finalizer must keep the exact terminal UID readable"
        )

        # The orchestrator restarts mid-way: no in-process state survives.
        monkeypatch.setattr(
            provisioner_module, "agent_provisioner", _fresh_provisioner(fake_k8s)
        )
        await reaper.reap_cycle(conn, grace_seconds=30)

        queue = await _queue(conn, thread_id)
        assert queue["state"] == "queued"
        assert queue["leased_by"] is None
        metadata = await _metadata(conn, thread_id)
        assert "_stateless_claim_losses" not in metadata
        assert "_stateless_claim_loss_hold" not in metadata
        [receipt] = metadata["_stateless_claim_loss_receipts"]
        assert receipt["lease_token"] == 1
        assert (receipt["pod"], receipt["pod_uid"]) == (POD, POD_UID)
        assert receipt["quiesced_by"] == "pod_terminal"
        assert receipt["evidence"] == "exact_terminal"

    # Proof was durable before the finalizer went; only then may the object go.
    assert fake_k8s.removed == [POD]
    name, patch = fake_k8s.patches[-1]
    assert name == POD
    assert {"op": "test", "path": "/metadata/uid", "value": POD_UID} in patch


@pytest.mark.asyncio
async def test_rollout_killed_claimant_is_retained_until_its_steal_settles(
    pool, fake_k8s
):
    """A rollout SIGKILLs a busy executor before its lease expires.

    The kubelet finishes before the reaper can steal, so for a while the Pod
    is process-zero yet still named by a live lease. Releasing it then would
    recreate the deadlock the moment the steal records its debt.
    """

    fake_k8s.pods[POD] = executor_pod(
        name=POD, uid=POD_UID, finalizers=_rendered_executor_finalizers()
    )
    thread_id = uuid4()
    async with pool.acquire() as conn:
        await _insert_expired_claim(conn, thread_id)
        await conn.execute(
            "UPDATE run_queue SET leased_until = now() + interval '1 minute' "
            "WHERE unit_id = $1",
            thread_id,
        )
        fake_k8s.delete_namespaced_pod(POD, EXECUTOR_NAMESPACE)  # ReplicaSet
        fake_k8s.kubelet_terminates(POD)

        assert await reaper.reap_cycle(conn, grace_seconds=30) == 0
        assert POD in fake_k8s.pods, "a live lease still names the dead claimant"

        await conn.execute(
            "UPDATE run_queue SET leased_until = now() - interval '5 minutes' "
            "WHERE unit_id = $1",
            thread_id,
        )
        # One tick: steal records the debt, the reconciler observes the
        # retained exact terminal UID and settles it, then the release.
        assert await reaper.reap_cycle(conn, grace_seconds=30) == 1

        assert (await _queue(conn, thread_id))["state"] == "queued"
        assert "_stateless_claim_losses" not in await _metadata(conn, thread_id)
    assert fake_k8s.removed == [POD]
    assert fake_k8s.deletes == [(POD, None, None)], "no second eviction needed"


@pytest.mark.asyncio
async def test_retained_pod_waits_for_every_reference_to_its_identity(pool, fake_k8s):
    referenced_by_ledger = executor_pod(
        name="exec-ledger", uid=str(uuid4()), finalizers=_rendered_executor_finalizers()
    )
    holding_session_lease = executor_pod(
        name="exec-session",
        uid=str(uuid4()),
        finalizers=_rendered_executor_finalizers(),
    )
    holding_worker_lease = executor_pod(
        name="exec-worker", uid=str(uuid4()), finalizers=_rendered_executor_finalizers()
    )
    never_claimed = executor_pod(
        name="exec-idle", uid=str(uuid4()), finalizers=_rendered_executor_finalizers()
    )
    malformed_ledger = executor_pod(
        name="exec-malformed",
        uid=str(uuid4()),
        finalizers=_rendered_executor_finalizers(),
    )
    for pod in (
        referenced_by_ledger,
        holding_session_lease,
        holding_worker_lease,
        never_claimed,
        malformed_ledger,
    ):
        fake_k8s.pods[pod.metadata.name] = pod
        fake_k8s.delete_namespaced_pod(pod.metadata.name, EXECUTOR_NAMESPACE)
        fake_k8s.kubelet_terminates(pod.metadata.name)

    async with pool.acquire() as conn:
        # An unsettled debt elsewhere (e.g. another thread's stuck steal).
        await _insert_thread(
            conn,
            uuid4(),
            {
                "_stateless_claim_losses": {
                    "4": {
                        "pod": "exec-ledger",
                        "pod_uid": referenced_by_ledger.metadata.uid,
                        "quiesced": False,
                    }
                }
            },
        )
        # A ledger nobody can parse still names the UID: fail closed.
        await _insert_thread(
            conn,
            uuid4(),
            {"_stateless_claim_losses": f"corrupt {malformed_ledger.metadata.uid}"},
        )
        # A lease not yet stolen can still become ledger debt.
        await conn.execute(
            "INSERT INTO run_queue (unit_id, unit_kind, state, lease_token, "
            "leased_by, leased_until) VALUES ($1, 'session_turn', 'leased', 3, "
            "'exec-session', now() + interval '1 minute')",
            uuid4(),
        )
        # Worker leases never create claimant-loss debt.
        await conn.execute(
            "INSERT INTO run_queue (unit_id, unit_kind, state, lease_token, "
            "leased_by, leased_until) VALUES ($1, 'worker_batch', 'leased', 3, "
            "'exec-worker', now() + interval '1 minute')",
            uuid4(),
        )

        assert await reaper.release_retained_executor_pods(conn) == 2

    assert sorted(fake_k8s.removed) == ["exec-idle", "exec-worker"]
    assert {"exec-ledger", "exec-session", "exec-malformed"} <= set(fake_k8s.pods)


@pytest.mark.asyncio
async def test_pre_finalizer_404_stays_held_until_an_operator_attests(
    pool, fake_k8s, caplog
):
    thread_id = uuid4()
    async with pool.acquire() as conn:
        await _insert_held_unit(conn, thread_id)

        with caplog.at_level(logging.ERROR, logger=reaper.logger.name):
            await reaper.reap_cycle(conn, grace_seconds=30)
            await reaper.reap_cycle(conn, grace_seconds=30)
        assert (await _queue(conn, thread_id))["state"] == "parked"
        anomalies = [
            record
            for record in caplog.records
            if "attest-claimant-gone" in record.message
        ]
        assert len(anomalies) == 1, "the 404 anomaly is loud but logged once"

    dependencies = run_queue_admin.RunQueueAdminDependencies(
        db=pool,
        require_admin=AsyncMock(return_value={"id": "admin-7"}),
        completion_commands_enabled=lambda: False,
        get_completion_command_resolution=lambda: None,
    )
    with pytest.raises(HTTPException) as unpark:
        await run_queue_admin.unpark_run_queue_unit(
            str(thread_id), dependencies=dependencies
        )
    assert unpark.value.status_code == 409
    assert "attest-claimant-gone" in unpark.value.detail

    with pytest.raises(HTTPException) as wrong_uid:
        await run_queue_admin.attest_claimant_gone(
            str(thread_id),
            pod=POD,
            pod_uid=str(uuid4()),
            reason="wrong pod",
            admin={"id": "admin-7"},
            dependencies=dependencies,
        )
    assert wrong_uid.value.status_code == 404

    result = await run_queue_admin.attest_claimant_gone(
        str(thread_id),
        pod=POD,
        pod_uid=POD_UID,
        reason="node-3 powered off; pod 404 since 09-03",
        admin={"id": "admin-7"},
        dependencies=dependencies,
    )
    assert result["settled_lease_tokens"] == [1]
    assert result["kubernetes_authority"] == "exact_absent"
    assert result["hold_released"] is True

    async with pool.acquire() as conn:
        assert (await _queue(conn, thread_id))["state"] == "queued"
        metadata = await _metadata(conn, thread_id)
    assert "_stateless_claim_losses" not in metadata
    [receipt] = metadata["_stateless_claim_loss_receipts"]
    assert receipt["quiesced_by"] == "operator:admin-7"
    assert receipt["evidence"] == "operator_attestation"
    assert receipt["reason"] == "node-3 powered off; pod 404 since 09-03"
    assert (receipt["pod"], receipt["pod_uid"]) == (POD, POD_UID)


@pytest.mark.asyncio
async def test_attest_refuses_a_claimant_the_api_still_shows_running(pool, fake_k8s):
    thread_id = uuid4()
    fake_k8s.pods[POD] = executor_pod(
        name=POD, uid=POD_UID, finalizers=_rendered_executor_finalizers()
    )
    async with pool.acquire() as conn:
        await _insert_held_unit(conn, thread_id)
    dependencies = run_queue_admin.RunQueueAdminDependencies(
        db=pool,
        require_admin=AsyncMock(return_value={"id": "admin-7"}),
        completion_commands_enabled=lambda: False,
        get_completion_command_resolution=lambda: None,
    )

    with pytest.raises(HTTPException) as live:
        await run_queue_admin.attest_claimant_gone(
            str(thread_id),
            pod=POD,
            pod_uid=POD_UID,
            reason="premature",
            admin={"id": "admin-7"},
            dependencies=dependencies,
        )
    assert live.value.status_code == 409

    # Deleting but still inside its termination grace: the kubelet may yet
    # report terminal containers, so a human assertion is premature.
    fake_k8s.delete_namespaced_pod(POD, EXECUTOR_NAMESPACE, grace_period_seconds=180)
    with pytest.raises(HTTPException) as in_grace:
        await run_queue_admin.attest_claimant_gone(
            str(thread_id),
            pod=POD,
            pod_uid=POD_UID,
            reason="premature",
            admin={"id": "admin-7"},
            dependencies=dependencies,
        )
    assert in_grace.value.status_code == 409
    async with pool.acquire() as conn:
        assert (await _queue(conn, thread_id))["state"] == "parked"
        assert "_stateless_claim_losses" in await _metadata(conn, thread_id)


@pytest.mark.asyncio
async def test_attest_releases_a_lost_node_claimant_the_kubelet_never_finished(
    pool, fake_k8s
):
    """Grace long past, statuses frozen at running: only a human can settle."""

    thread_id = uuid4()
    lost = executor_pod(
        name=POD,
        uid=POD_UID,
        finalizers=_rendered_executor_finalizers(),
        deleting=True,
    )
    lost.metadata.deletion_timestamp = datetime.now(timezone.utc) - timedelta(hours=2)
    fake_k8s.pods[POD] = lost
    async with pool.acquire() as conn:
        await _insert_held_unit(conn, thread_id)
        await reaper.reap_cycle(conn, grace_seconds=30)
        assert (await _queue(conn, thread_id))["state"] == "parked"
    assert POD in fake_k8s.pods, "running statuses are never process-zero evidence"

    dependencies = run_queue_admin.RunQueueAdminDependencies(
        db=pool,
        require_admin=AsyncMock(return_value={"id": "admin-7"}),
        completion_commands_enabled=lambda: False,
        get_completion_command_resolution=lambda: None,
    )
    result = await run_queue_admin.attest_claimant_gone(
        str(thread_id),
        pod=POD,
        pod_uid=POD_UID,
        reason="node-3 out-of-service",
        admin={"id": "admin-7"},
        dependencies=dependencies,
    )

    assert result["kubernetes_authority"] == "unknown"
    assert result["finalizer_released"] is True
    assert fake_k8s.removed == [POD]
    async with pool.acquire() as conn:
        assert (await _queue(conn, thread_id))["state"] == "queued"


def _admin_dependencies(pool, admin_id):
    return run_queue_admin.RunQueueAdminDependencies(
        db=pool,
        require_admin=AsyncMock(return_value={"id": admin_id}),
        completion_commands_enabled=lambda: False,
        get_completion_command_resolution=lambda: None,
    )


def _frozen_on_a_live_node() -> object:
    """Statuses frozen at running on a node nobody tainted or deleted."""

    pod = executor_pod(
        name=POD, uid=POD_UID, finalizers=_rendered_executor_finalizers(), deleting=True
    )
    pod.metadata.deletion_timestamp = datetime.now(timezone.utc) - timedelta(hours=2)
    return pod


@pytest.mark.asyncio
async def test_release_only_attestation_frees_an_unowed_frozen_executor(pool, fake_k8s):
    fake_k8s.pods[POD] = _frozen_on_a_live_node()
    admin_id = str(uuid4())
    async with pool.acquire() as conn:
        await reaper.reap_cycle(conn, grace_seconds=30)
    assert POD in fake_k8s.pods, "no evidence the reaper trusts ever arrives"

    result = await run_queue_admin.attest_executor_pod_gone(
        POD,
        pod_uid=POD_UID,
        reason="node-3 hardware replaced; kubelet never came back",
        admin={"id": admin_id},
        dependencies=_admin_dependencies(pool, admin_id),
    )

    assert result == {
        "pod": POD,
        "pod_uid": POD_UID,
        "kubernetes_authority": "unknown",
        "finalizer_released": True,
    }
    assert fake_k8s.removed == [POD]
    async with pool.acquire() as conn:
        receipt = await conn.fetchrow(
            "SELECT event_type, user_id, resource_type, resource_id, path, detail "
            "FROM security_events"
        )
    assert receipt["event_type"] == "operator_attestation"
    assert str(receipt["user_id"]) == admin_id
    assert (receipt["resource_type"], receipt["resource_id"]) == (
        "stateless_executor_pod",
        POD_UID,
    )
    assert json.loads(receipt["detail"])["reason"] == (
        "node-3 hardware replaced; kubelet never came back"
    )


@pytest.mark.asyncio
async def test_release_only_attestation_refuses_owed_live_or_unretained_pods(
    pool, fake_k8s
):
    admin_id = str(uuid4())
    dependencies = _admin_dependencies(pool, admin_id)

    async def attest():
        with pytest.raises(HTTPException) as refused:
            await run_queue_admin.attest_executor_pod_gone(
                POD,
                pod_uid=POD_UID,
                reason="probe",
                admin={"id": admin_id},
                dependencies=dependencies,
            )
        return refused.value.status_code

    assert await attest() == 404  # nothing retained under that name
    fake_k8s.pods[POD] = executor_pod(
        name=POD, uid=POD_UID, finalizers=_rendered_executor_finalizers()
    )
    assert await attest() == 409  # live and serving
    fake_k8s.delete_namespaced_pod(POD, EXECUTOR_NAMESPACE, grace_period_seconds=180)
    assert await attest() == 409  # inside its termination grace
    fake_k8s.pods[POD] = _frozen_on_a_live_node()
    async with pool.acquire() as conn:
        await _insert_held_unit(conn, uuid4())
    assert await attest() == 409  # a claim still owes it: attest-claimant-gone
    fake_k8s.pod_read_error = 500
    assert await attest() == 503
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM security_events") == 0
    assert POD in fake_k8s.pods


@pytest.mark.asyncio
async def test_reconciler_page_rotates_through_every_held_thread(pool, fake_k8s):
    """25+ unsettleable 404 holds must not starve a newer hold forever."""

    held = [uuid4() for _ in range(5)]
    async with pool.acquire() as conn:
        for index, thread_id in enumerate(held):
            await _insert_held_unit(
                conn, thread_id, pod=f"gone-{index}", pod_uid=str(uuid4())
            )
    seen: set[str] = set()
    original = fake_k8s.read_namespaced_pod

    def read(name, namespace, **kwargs):
        seen.add(name)
        return original(name, namespace, **kwargs)

    fake_k8s.read_namespaced_pod = read
    async with pool.acquire() as conn:
        for _ in range(3):
            await reaper.reconcile_claim_loss_holds(conn, max_threads=2)
    assert seen == {f"gone-{index}" for index in range(5)}
