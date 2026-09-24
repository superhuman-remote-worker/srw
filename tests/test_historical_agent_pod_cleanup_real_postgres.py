"""Soft G1 settlement, used G2 Resume and permanent PVC cleanup on real SQL."""

from __future__ import annotations

import json
from types import SimpleNamespace as NS
from unittest.mock import MagicMock
from uuid import uuid4

import asyncpg
import pytest

from orchestrator import main
from orchestrator.services.agent_provisioner import AgentProvisioner
from orchestrator.services.pinned_k8s_effect import PINNED_AUTHORITY_FINALIZER
from orchestrator.services.session_router import SessionRouterService
from shared.persistent_input_delivery import (
    mark_input_delivery_queued,
    persist_input_delivery,
    transition_input_delivery,
)
from tests import test_persistent_recycler_real_postgres as authority_fixtures
from tests.test_persistent_recycler_real_postgres import (
    StatefulPinnedK8sApi,
    _authorize_and_ack,
    _json,
)
from orchestrator.application import controls as controls_composition
from orchestrator.services import agent_provisioner as agent_provisioner_module

db = authority_fixtures.db
pg_dsn = authority_fixtures.pg_dsn
_schema_applied = authority_fixtures._schema_applied


class ClaimantK8sApi(StatefulPinnedK8sApi):
    """Keep a deleting PVC while any Pod still references it, as Kubernetes does."""

    def __init__(self):
        super().__init__()
        self.removed_pods = []
        self.deleted_pvcs = []

    def install_claimant(self, identity, claim):
        self.install_old_pod(
            namespace="agents-a",
            name=identity["pod_name"],
            uid=identity["pod_uid"],
            labels={
                "srw/managed-by": "agent-provisioner",
                "srw/purpose": "session",
                "srw.io/thread-id": identity["thread"],
                "srw.io/runtime-generation": identity["generation"],
                "srw.io/provision-attempt": identity["attempt"],
            },
        )
        pod = self.pods[("agents-a", identity["pod_name"])]
        pod.spec = NS(
            containers=[NS(name="agent")],
            init_containers=[],
            ephemeral_containers=[],
            volumes=[NS(persistent_volume_claim=NS(claim_name=claim["pvc_name"]))],
        )
        pod.status.container_statuses[0].name = "agent"
        return pod

    def mark_terminal(self, namespace, name):
        super().mark_terminal(namespace, name)
        self.pods[(namespace, name)].status.container_statuses[0].name = "agent"

    def patch_namespaced_pod(self, *, name, namespace, **kwargs):
        uid = self.pods[(namespace, name)].metadata.uid
        result = super().patch_namespaced_pod(name=name, namespace=namespace, **kwargs)
        if (namespace, name) not in self.pods:
            self.removed_pods.append(uid)
        return result

    def _collect_pvc(self, namespace, name):
        claim = self.pvcs.get((namespace, name))
        if claim is None or not claim.metadata.deletion_timestamp:
            return
        mounted = any(
            ns == namespace
            and any(
                getattr(getattr(v, "persistent_volume_claim", None), "claim_name", None)
                == name
                for v in getattr(getattr(pod, "spec", None), "volumes", [])
            )
            for (ns, _), pod in self.pods.items()
        )
        if not claim.metadata.finalizers and not mounted:
            self.pvcs.pop((namespace, name))

    def delete_namespaced_persistent_volume_claim(
        self, *, name, namespace, body, **kwargs
    ):
        claim = self.read_namespaced_persistent_volume_claim(
            name=name, namespace=namespace
        )
        assert body["preconditions"]["uid"] == claim.metadata.uid
        self.deleted_pvcs.append(claim.metadata.uid)
        claim.metadata.deletion_timestamp = "now"
        self._collect_pvc(namespace, name)

    def patch_namespaced_persistent_volume_claim(self, *, name, namespace, **kwargs):
        result = super().patch_namespaced_persistent_volume_claim(
            name=name, namespace=namespace, **kwargs
        )
        self._collect_pvc(namespace, name)
        return result


async def _bind_used_generation(db, ids):
    generation = str((await db.get_thread(ids["thread"]))["runtime_generation"])
    identity = {
        **ids,
        "generation": generation,
        "agent": str(uuid4()),
        "attach_token": str(uuid4()),
        "attempt": str(uuid4()),
        "pod_name": f"agent-{uuid4().hex[:12]}",
        "pod_uid": str(uuid4()),
    }
    reserved = await db.reserve_pinned_agent_pod_provision_intent(
        ids["thread"],
        expected_runtime_generation=generation,
        attempt_id=identity["attempt"],
        pod_name=identity["pod_name"],
        provisioner="agent",
        namespace="agents-a",
        pvc_name=f"pvc-agent-{ids['thread'][:12]}",
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
        attempt_id=identity["attempt"],
        pod_name=identity["pod_name"],
        pod_uid=identity["pod_uid"],
        namespace="agents-a",
    )
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO agents (id,config_name,hostname,pod_uid,status,agent_mode) "
                "VALUES ($1::uuid,'session_base',$2,$3,'session','persistent')",
                identity["agent"],
                identity["pod_name"],
                identity["pod_uid"],
            )
            await conn.execute(
                "UPDATE threads SET status='active',agent_id=$2::uuid,"
                "control_admission_agent_id=$2::uuid,runtime_attach_token=$3::uuid "
                "WHERE id=$1::uuid",
                ids["thread"],
                identity["agent"],
                identity["attach_token"],
            )
            await conn.execute(
                "UPDATE agents SET thread_id=$2::uuid WHERE id=$1::uuid",
                identity["agent"],
                ids["thread"],
            )
            delivery_id = uuid4()
            authority = dict(
                agent_id=identity["agent"],
                pod_uid=identity["pod_uid"],
                runtime_generation=generation,
                runtime_attach_token=identity["attach_token"],
            )
            delivery = await persist_input_delivery(
                conn,
                thread_id=ids["thread"],
                delivery_id=delivery_id,
                role="human",
                content="Use this pinned generation",
                source="direct_human",
                turn_number=1,
                **authority,
            )
            transition_args = dict(
                delivery_id=delivery_id,
                claim_generation=int(delivery["claim_generation"]),
                **authority,
            )
            assert await mark_input_delivery_queued(conn, **transition_args)
            assert await transition_input_delivery(
                conn, transition="admitted", turn_number=1, **transition_args
            )
            assert await transition_input_delivery(
                conn, transition="settled", **transition_args
            )
    return identity, claim


async def _scenario(
    db, monkeypatch, *, retain_old_agent=False, acknowledge_current=True
):
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
    old, claim = await _bind_used_generation(db, ids)
    k8s = ClaimantK8sApi()
    old_pod = k8s.install_claimant(old, claim)
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
    provider = AgentProvisioner()
    provider._k8s_available = True
    provider._core_api = k8s
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(agent_provisioner_module, "agent_provisioner", provider)
    # The stopped actor's route is already absent. Keep route teardown on
    # injected APIs so this SQL/Kubernetes model never reads ambient kubeconfig.
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

    soft = await db.begin_pinned_thread_retirement(ids["thread"], permanent=False)
    await _authorize_and_ack(db, old, soft)
    await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).reconcile_agent_workspace_claim_for_retirement(soft)
    assert await db.settle_pinned_thread_retirement(
        ids["thread"],
        token=soft["token"],
        generation=soft["generation"],
        final_status="ended",
    )
    proof = await db.fetchrow(
        "SELECT * FROM thread_runtime_retirement_outcomes WHERE thread_id=$1::uuid",
        ids["thread"],
    )
    assert _json(proof["retired_agent_pod"])["pod_uid"] == old["pod_uid"]
    if not retain_old_agent:
        assert await db.delete_agent(old["agent"])
    k8s.mark_terminal("agents-a", old["pod_name"])
    old_pod.metadata.deletion_timestamp = "now"

    assert await db.resume_thread(ids["thread"])
    current, reused_claim = await _bind_used_generation(db, ids)
    assert current["generation"] != old["generation"]
    assert reused_claim["claim_id"] == claim["claim_id"]
    k8s.install_claimant(current, claim)
    permanent = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    if acknowledge_current:
        await _authorize_and_ack(db, current, permanent)
    else:
        assert await db.authorize_pinned_thread_retirement(
            ids["thread"],
            token=permanent["token"],
            generation=permanent["generation"],
            settle_status="ended",
        )
    k8s.mark_terminal("agents-a", current["pod_name"])
    await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).stop_captured_retirement_agent(permanent)
    assert old_pod.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
    assert k8s.removed_pods == [current["pod_uid"]]
    return old, current, permanent, k8s, provider


@pytest.mark.asyncio
@pytest.mark.parametrize("retain_old_agent", [False, True])
async def test_used_resumed_generation_reclaims_historical_claimant(
    db, monkeypatch, retain_old_agent
):
    old, current, permanent, k8s, _ = await _scenario(
        db,
        monkeypatch,
        retain_old_agent=retain_old_agent,
    )
    await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).cleanup_pinned_thread_retirement(permanent)
    assert k8s.removed_pods == [current["pod_uid"], old["pod_uid"]]
    assert not k8s.pods
    assert k8s.deleted_pvcs == [f"pvc-{old['thread']}"]
    # The remaining claim is a deliberate causal name fence, not the old PVC.
    fence = next(iter(k8s.pvcs.values()))
    assert fence.metadata.labels["srw.io/workspace-claim-fence"] == "true"
    await db.delete_thread(
        old["thread"],
        expected_runtime_retirement_token=permanent["token"],
        expected_runtime_generation=permanent["generation"],
    )
    assert await db.get_thread(old["thread"]) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "stale_token",
        "missing_history",
        "foreign_uid",
        "wrong_label",
        "wrong_pvc",
        "running",
        "init_running",
        "ephemeral_running",
        "missing_container_status",
        "missing_local_quiescence",
        "rebound_old_actor",
    ],
)
async def test_historical_claimant_refuses_incomplete_or_changed_authority(
    db, monkeypatch, fault
):
    old, _, permanent, k8s, _ = await _scenario(
        db,
        monkeypatch,
        retain_old_agent=fault == "rebound_old_actor",
        acknowledge_current=fault != "missing_local_quiescence",
    )
    pod = k8s.pods[("agents-a", old["pod_name"])]
    if fault == "stale_token":
        permanent = {**permanent, "token": str(uuid4())}
    elif fault == "missing_history":
        # Model an outcome written before 0224; production never backfills it.
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL session_replication_role='replica'")
                await conn.execute(
                    "UPDATE thread_runtime_retirement_outcomes SET retired_agent_pod=NULL "
                    "WHERE thread_id=$1::uuid",
                    old["thread"],
                )
    elif fault == "foreign_uid":
        pod.metadata.uid = str(uuid4())
    elif fault == "wrong_label":
        pod.metadata.labels["srw.io/provision-attempt"] = str(uuid4())
    elif fault == "wrong_pvc":
        pod.spec.volumes[0].persistent_volume_claim.claim_name = "another-claim"
    elif fault == "running":
        pod.status.container_statuses[0].state.terminated = None
    elif fault in {"init_running", "ephemeral_running"}:
        prefix = "init" if fault == "init_running" else "ephemeral"
        setattr(pod.spec, prefix + "_containers", [NS(name="sidecar")])
        setattr(
            pod.status,
            prefix + "_container_statuses",
            [NS(name="sidecar", state=NS(terminated=None))],
        )
    elif fault == "missing_container_status":
        pod.spec.ephemeral_containers = [NS(name="unobserved")]
    elif fault == "rebound_old_actor":
        await db.execute(
            "UPDATE agents SET status='ready' WHERE id=$1::uuid", old["agent"]
        )
    with pytest.raises(RuntimeError):
        await controls_composition.pinned_retirement_operations(
            main.app.state.resources
        ).reconcile_agent_workspace_claim_for_retirement(permanent)
    assert pod.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
    assert not k8s.deleted_pvcs


@pytest.mark.asyncio
async def test_retired_pod_identity_remains_append_only(db, monkeypatch):
    old, _, _, _, _ = await _scenario(db, monkeypatch)
    with pytest.raises(asyncpg.CheckViolationError) as refused:
        await db.execute(
            "UPDATE thread_runtime_retirement_outcomes SET retired_agent_pod='{}'::jsonb "
            "WHERE thread_id=$1::uuid",
            old["thread"],
        )
    assert refused.value.constraint_name == "thread_runtime_outcomes_append_only"


@pytest.mark.asyncio
async def test_lost_historical_finalizer_response_retries_without_touching_successor(
    db, monkeypatch
):
    old, current, permanent, k8s, _ = await _scenario(db, monkeypatch)
    k8s.lose_next_pod_patch_response = True
    with pytest.raises(RuntimeError, match="historical claimant Pod retirement"):
        await controls_composition.pinned_retirement_operations(
            main.app.state.resources
        ).reconcile_agent_workspace_claim_for_retirement(permanent)
    assert not k8s.pods
    assert not k8s.deleted_pvcs
    await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).cleanup_pinned_thread_retirement(permanent)
    assert k8s.removed_pods == [current["pod_uid"]]
    assert k8s.deleted_pvcs == [f"pvc-{old['thread']}"]
    assert await db.pinned_retirement_external_cleanup_complete(
        old["thread"],
        runtime_generation=permanent["generation"],
        retirement_token=permanent["token"],
    )


@pytest.mark.asyncio
async def test_historical_patch_refuses_a_status_change_at_its_resource_version(
    db, monkeypatch
):
    old, _, permanent, k8s, _ = await _scenario(db, monkeypatch)
    original_patch = k8s.patch_namespaced_pod
    pod = k8s.pods[("agents-a", old["pod_name"])]

    def changed_before_patch(**kwargs):
        pod.status.ephemeral_container_statuses = [
            NS(name="debug", state=NS(terminated=None))
        ]
        pod.spec.ephemeral_containers = [NS(name="debug")]
        pod.metadata.resource_version = str(int(pod.metadata.resource_version) + 1)
        return original_patch(**kwargs)

    monkeypatch.setattr(k8s, "patch_namespaced_pod", changed_before_patch)
    with pytest.raises(RuntimeError, match="historical claimant Pod retirement"):
        await controls_composition.pinned_retirement_operations(
            main.app.state.resources
        ).reconcile_agent_workspace_claim_for_retirement(permanent)
    assert pod.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
    assert not k8s.deleted_pvcs
