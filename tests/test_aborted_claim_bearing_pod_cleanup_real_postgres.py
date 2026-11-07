"""A claim-bearing agent that released before setup leaves no stuck Pod.

Repro of k3d Session ``7b007750`` (2026-11-03): a dedicated agent whose VM had
failed released its attach with ``agent_attach_not_started_v1`` and exited 0.
Its Pod mounted the Session's agent workspace PVC, so neither cleanup path
accepted the abort; the Pod stayed on the pinned-authority finalizer and a
permanent Delete answered 503 forever.
"""

from __future__ import annotations

import json
from types import SimpleNamespace as NS
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from orchestrator import main
from orchestrator.application import controls as controls_composition
from orchestrator.services import agent_provisioner as agent_provisioner_module
from orchestrator.services.agent_provisioner import AgentProvisioner
from orchestrator.services.historical_agent_pod_cleanup import (
    retire_aborted_agent_pod,
)
from orchestrator.services.pinned_k8s_effect import PINNED_AUTHORITY_FINALIZER
from orchestrator.services.session_attach_binding import release_session_attach_binding
from orchestrator.services.session_router import SessionRouterService
from tests import test_persistent_recycler_real_postgres as authority_fixtures
from tests.test_historical_agent_pod_cleanup_real_postgres import ClaimantK8sApi

db = authority_fixtures.db
pg_dsn = authority_fixtures.pg_dsn
_schema_applied = authority_fixtures._schema_applied

PVC_PROTECTION = "kubernetes.io/pvc-protection"


async def _aborted_claimant(db, monkeypatch):
    """G1 binds with a claim, releases before setup; owner End settles G2."""

    ids = {key: str(uuid4()) for key in ("user", "thread")}
    await db.execute(
        "INSERT INTO users (id,display_name,email) VALUES ($1::uuid,'owner',$2)",
        ids["user"],
        f"{ids['user']}@example.test",
    )
    await db.execute(
        "INSERT INTO threads (id,user_id,status,execution_lane,config_name,metadata) "
        "VALUES ($1::uuid,$2::uuid,'created','pinned','session_base',$3::jsonb)",
        ids["thread"],
        ids["user"],
        json.dumps({"config_override": {"workspace": {"backend": "none"}}}),
    )
    generation = str((await db.get_thread(ids["thread"]))["runtime_generation"])
    old = {
        **ids,
        "generation": generation,
        "agent": str(uuid4()),
        "attach_token": str(uuid4()),
        "attempt": str(uuid4()),
        "pod_name": f"srw-agent-s-{uuid4().hex[:8]}",
        "pod_uid": str(uuid4()),
        "namespace": "agents-a",
    }
    reserved = await db.reserve_pinned_agent_pod_provision_intent(
        ids["thread"],
        expected_runtime_generation=generation,
        attempt_id=old["attempt"],
        pod_name=old["pod_name"],
        provisioner="agent",
        namespace="agents-a",
        pvc_name=f"pvc-agent-s-{ids['thread'][:12]}",
    )
    assert reserved
    claim = reserved["workspace_claim"]
    assert await db.publish_pinned_agent_workspace_claim(
        ids["thread"],
        expected_runtime_generation=generation,
        claim_id=str(claim["claim_id"]),
        pvc_name=claim["pvc_name"],
        pvc_uid=f"pvc-{ids['thread']}",
        namespace="agents-a",
    )
    assert await db.publish_pinned_agent_pod_provision_intent(
        ids["thread"],
        expected_runtime_generation=generation,
        attempt_id=old["attempt"],
        pod_name=old["pod_name"],
        pod_uid=old["pod_uid"],
        namespace="agents-a",
    )
    async with db.acquire() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO agents (id,config_name,hostname,pod_uid,status,agent_mode) "
            "VALUES ($1::uuid,'session_base',$2,$3,'session','persistent')",
            old["agent"],
            old["pod_name"],
            old["pod_uid"],
        )
        await conn.execute(
            "UPDATE threads SET agent_id=$2::uuid,control_admission_agent_id=$2::uuid,"
            "runtime_attach_token=$3::uuid WHERE id=$1::uuid",
            ids["thread"],
            old["agent"],
            old["attach_token"],
        )
        await conn.execute(
            "UPDATE agents SET thread_id=$2::uuid WHERE id=$1::uuid",
            old["agent"],
            ids["thread"],
        )
    # The workspace never became ready: the agent releases before setup.
    assert (
        await release_session_attach_binding(
            old["agent"],
            ids["thread"],
            expected_runtime_generation=generation,
            expected_attach_token=old["attach_token"],
            expected_agent_pod_uid=old["pod_uid"],
            local_runtime_quiesced=True,
            local_quiescence_protocol="agent_attach_not_started_v1",
            dependencies=NS(store=db),
        )
        == "released"
    )
    # Normal deregistration follows the confirmed release; the Pod exits 0.
    assert await db.delete_agent(old["agent"])

    k8s = ClaimantK8sApi()
    pod = k8s.install_claimant(old, claim)
    pod.metadata.name = old["pod_name"]
    pod.spec.restart_policy = "Never"
    k8s.mark_terminal("agents-a", old["pod_name"])
    k8s.install_pvc(
        namespace="agents-a",
        name=claim["pvc_name"],
        uid=f"pvc-{ids['thread']}",
        labels={
            "srw.io/thread-id": ids["thread"],
            "srw.io/runtime-generation": str(claim["created_runtime_generation"]),
            "srw.io/workspace-claim": str(claim["claim_id"]),
            "srw.io/provision-attempt": str(claim["create_attempt"]),
            "srw.io/claim-provisioner": "agent",
        },
    )
    k8s.list_namespaced_pod = lambda **_kwargs: NS(items=list(k8s.pods.values()))
    provider = AgentProvisioner()
    provider._k8s_available = True
    provider._core_api = k8s
    provider._db = db
    provider._namespace = "agents-a"
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(agent_provisioner_module, "agent_provisioner", provider)
    core_api = MagicMock()
    networking_api = MagicMock()
    core_api.read_namespaced_service.side_effect = authority_fixtures._K8sError(404)
    networking_api.read_namespaced_ingress.side_effect = authority_fixtures._K8sError(
        404
    )
    monkeypatch.setattr(
        main.app.state.resources,
        "session_router",
        SessionRouterService(
            namespace="agents-a",
            ingress_host="unused.example",
            core_api=core_api,
            networking_api=networking_api,
        ),
    )
    ops = controls_composition.pinned_retirement_operations(main.app.state.resources)

    # Owner End of the successor generation captures no agent Pod.
    soft = await db.begin_pinned_thread_retirement(ids["thread"], permanent=False)
    assert soft["state"] == "pending"
    assert soft["generation"] != generation
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=soft["token"],
        generation=soft["generation"],
        settle_status="ended",
    )
    await ops.cleanup_pinned_thread_retirement(soft, cleanup_agent_pod=False)
    assert await db.settle_pinned_thread_retirement(
        ids["thread"],
        token=soft["token"],
        generation=soft["generation"],
        final_status="ended",
    )
    # The orchestrator already asked Kubernetes to delete the exited Pod.
    pod.metadata.deletion_timestamp = "now"
    assert pod.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
    return old, claim, k8s, provider, ops


async def _begin_permanent(db, old):
    permanent = await db.begin_pinned_thread_retirement(old["thread"], permanent=True)
    assert permanent["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        old["thread"],
        token=permanent["token"],
        generation=permanent["generation"],
        settle_status="ended",
    )
    return permanent


async def _claim_status(db, claim):
    return await db.fetchval(
        "SELECT status FROM thread_agent_workspace_claims WHERE claim_id=$1::uuid",
        str(claim["claim_id"]),
    )


async def _reap(provider, old):
    return await retire_aborted_agent_pod(
        provider._db,
        pod_name=old["pod_name"],
        pod_uid=old["pod_uid"],
        namespace="agents-a",
        agent_provisioner=provider,
    )


@pytest.mark.asyncio
async def test_reaper_releases_the_aborted_claimant_and_keeps_its_pvc(db, monkeypatch):
    old, claim, k8s, provider, _ = await _aborted_claimant(db, monkeypatch)

    assert (await provider.reap_pods())["completed"] == 1

    assert ("agents-a", old["pod_name"]) not in k8s.pods
    assert k8s.removed_pods == [old["pod_uid"]]
    # A soft-ended Session keeps its workspace: the claim and PVC stay.
    assert ("agents-a", claim["pvc_name"]) in k8s.pvcs
    assert k8s.deleted_pvcs == []
    assert await _claim_status(db, claim) == "ready"
    assert (await db.get_thread(old["thread"]))["status"] == "ended"


@pytest.mark.asyncio
async def test_permanent_delete_retires_the_aborted_claimant_then_fences_the_pvc(
    db, monkeypatch
):
    old, claim, k8s, _, ops = await _aborted_claimant(db, monkeypatch)
    permanent = await _begin_permanent(db, old)

    await ops.cleanup_pinned_thread_retirement(permanent, cleanup_agent_pod=False)

    assert k8s.removed_pods == [old["pod_uid"]]
    assert not k8s.pods
    assert k8s.deleted_pvcs == [f"pvc-{old['thread']}"]
    fence = next(iter(k8s.pvcs.values()))
    assert fence.metadata.labels["srw.io/workspace-claim-fence"] == "true"
    assert await _claim_status(db, claim) == "fenced"
    await db.delete_thread(
        old["thread"],
        expected_runtime_retirement_token=permanent["token"],
        expected_runtime_generation=permanent["generation"],
    )
    assert await db.get_thread(old["thread"]) is None


@pytest.mark.asyncio
async def test_permanent_delete_survives_the_pvc_protection_controller_race(
    db, monkeypatch
):
    """Dev 2026-11-05: the first permanent Delete of ``c78ac031`` answered 503."""

    old, claim, k8s, _, ops = await _aborted_claimant(db, monkeypatch)
    pvc = k8s.pvcs[("agents-a", claim["pvc_name"])]
    pvc.metadata.finalizers = [PINNED_AUTHORITY_FINALIZER, PVC_PROTECTION]
    patch_pvc = k8s.patch_namespaced_persistent_volume_claim
    raced = []

    def controller_drops_protection_first(**kwargs):
        # Kubernetes drops pvc-protection once it observes the delete, between
        # our read and our patch.
        target = k8s.pvcs.get((kwargs["namespace"], kwargs["name"]))
        if not raced and target is not None and target.metadata.deletion_timestamp:
            raced.append(target.metadata.resource_version)
            target.metadata.finalizers = [PINNED_AUTHORITY_FINALIZER]
            target.metadata.resource_version = str(
                int(target.metadata.resource_version) + 1
            )
        return patch_pvc(**kwargs)

    k8s.patch_namespaced_persistent_volume_claim = controller_drops_protection_first
    permanent = await _begin_permanent(db, old)

    # One call, no "agent workspace finalizer release is retryable" 503.
    await ops.cleanup_pinned_thread_retirement(permanent, cleanup_agent_pod=False)

    assert raced
    assert k8s.deleted_pvcs == [f"pvc-{old['thread']}"]
    assert await _claim_status(db, claim) == "fenced"


async def _corrupt_history(db, old, statement, *args):
    # Corrupt only disposable test history; valid positive paths keep triggers.
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        await conn.execute(statement, old["thread"], *args)


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["reaper", "permanent"])
@pytest.mark.parametrize(
    "fault",
    [
        "missing-abort",
        "setup-exposed",
        "wrong-pod-proof",
        "wrong-generation",
        "live-actor",
        "restartable",
        "live-container",
        "extra-pvc",
        "wrong-label",
    ],
)
async def test_aborted_claimant_cleanup_refuses_unproven_or_changed_life(
    db, monkeypatch, path, fault
):
    old, claim, k8s, provider, ops = await _aborted_claimant(db, monkeypatch)
    pod = k8s.pods[("agents-a", old["pod_name"])]
    if fault == "missing-abort":
        await _corrupt_history(
            db,
            old,
            "DELETE FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1::uuid",
        )
    elif fault == "setup-exposed":
        await _corrupt_history(
            db,
            old,
            "UPDATE thread_runtime_attach_abort_outcomes "
            "SET quiescence_protocol='agent_runtime_zero_v1' WHERE thread_id=$1::uuid",
        )
    elif fault == "wrong-pod-proof":
        await _corrupt_history(
            db,
            old,
            "UPDATE thread_runtime_attach_abort_outcomes SET agent_pod_uid=$2 "
            "WHERE thread_id=$1::uuid",
            str(uuid4()),
        )
    elif fault == "wrong-generation":
        await _corrupt_history(
            db,
            old,
            "UPDATE thread_runtime_attach_abort_outcomes "
            "SET runtime_generation=$2::uuid WHERE thread_id=$1::uuid",
            str(uuid4()),
        )
    elif fault == "live-actor":
        await db.execute(
            "INSERT INTO agents (id,config_name,hostname,pod_uid,status,agent_mode) "
            "VALUES ($1::uuid,'session_base',$2,$3,'ready','persistent')",
            old["agent"],
            old["pod_name"],
            old["pod_uid"],
        )
    elif fault == "restartable":
        pod.spec.restart_policy = "Always"
    elif fault == "live-container":
        pod.status.container_statuses[0].state.terminated = None
    elif fault == "extra-pvc":
        pod.spec.volumes.append(NS(persistent_volume_claim=NS(claim_name="other")))
    elif fault == "wrong-label":
        pod.metadata.labels["srw.io/runtime-generation"] = str(uuid4())

    if path == "reaper":
        assert not await _reap(provider, old)
    else:
        permanent = await _begin_permanent(db, old)
        with pytest.raises(RuntimeError):
            await ops.cleanup_pinned_thread_retirement(
                permanent, cleanup_agent_pod=False
            )
    assert k8s.pods[("agents-a", old["pod_name"])] is pod
    assert pod.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
    assert not k8s.removed_pods
    assert ("agents-a", claim["pvc_name"]) in k8s.pvcs
    assert not k8s.deleted_pvcs


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["foreign-claim", "claim-revoking", "claim-moved"])
async def test_aborted_claimant_reaper_refuses_a_claim_it_cannot_match(
    db, monkeypatch, fault
):
    old, claim, k8s, provider, _ = await _aborted_claimant(db, monkeypatch)
    if fault == "foreign-claim":
        await _corrupt_history(
            db,
            old,
            "UPDATE thread_agent_pod_provision_intents SET workspace_claim_id=$2::uuid "
            "WHERE thread_id=$1::uuid",
            str(uuid4()),
        )
    elif fault == "claim-revoking":
        await _corrupt_history(
            db,
            old,
            "UPDATE thread_agent_workspace_claims SET status='revoking',"
            "resolved_at=NULL WHERE thread_id=$1::uuid",
        )
    elif fault == "claim-moved":
        await _corrupt_history(
            db,
            old,
            "UPDATE thread_agent_workspace_claims SET namespace='agents-b' "
            "WHERE thread_id=$1::uuid",
        )

    assert not await _reap(provider, old)
    assert (await provider.reap_pods())["completed"] == 0
    assert ("agents-a", old["pod_name"]) in k8s.pods
    assert not k8s.removed_pods
    assert ("agents-a", claim["pvc_name"]) in k8s.pvcs


@pytest.mark.asyncio
async def test_aborted_claimant_reaper_rechecks_the_claim_before_its_effect(
    db, monkeypatch
):
    old, claim, k8s, provider, _ = await _aborted_claimant(db, monkeypatch)
    fetchrow = db.fetchrow
    claim_reads = []

    async def claim_changes_after_first_read(query, *args):
        row = await fetchrow(query, *args)
        if "FROM thread_agent_workspace_claims" in query and row is not None:
            claim_reads.append(query)
            if len(claim_reads) > 1:
                row = {**row, "pvc_uid": "replacement-pvc-uid"}
        return row

    monkeypatch.setattr(db, "fetchrow", claim_changes_after_first_read)
    assert not await _reap(provider, old)
    assert len(claim_reads) >= 2
    assert ("agents-a", old["pod_name"]) in k8s.pods
    assert not k8s.removed_pods
