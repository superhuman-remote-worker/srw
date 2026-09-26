"""Agent-initiated End of a dedicated pinned life, then permanent Delete.

The R3.2 live gate found ended pinned threads that could never be deleted:
the agent settled its own soft End (idle timeout or the socket ``archive``
verb) and its Pod kept running, so historical claimant retirement refused it
forever. These tests drive the same database/Kubernetes contract through the
public End funnel (``end_thread_flow``) on migrated PostgreSQL and a stateful
Kubernetes model:

* the agent's own settlement goes through ``end_thread_flow`` exactly as
  ``agent_thread_status`` calls it (agent Begin, local-quiescence ACK, final
  soft settlement with the Pod left to exit itself);
* the permanent Delete is the owner's ``end_thread_flow(permanent=True)``,
  retried the way a client retries a 503.

Both agent PVC configurations are covered: a claim-bearing life mounts the
thread's agent workspace claim; a claim-less life (``workspace.pvcEnabled``
false) mounts none. Neither an offline agent row, an ended thread nor Pod
absence is treated as authority: every retirement joins the exact settled
outcome to the exact published Pod intent and re-attests the live object.

See knowledge-base/knowledge/issues/agent_initiated_pinned_end_wedges_permanent_delete.md.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from orchestrator import main
from orchestrator.application import controls as controls_composition
from orchestrator.services import agent_provisioner as agent_provisioner_module
from orchestrator.services.agent_provisioner import AgentProvisioner
from orchestrator.services.pinned_k8s_effect import PINNED_AUTHORITY_FINALIZER
from orchestrator.services.session_router import SessionRouterService
from shared.persistent_input_delivery import (
    mark_input_delivery_queued,
    persist_input_delivery,
    transition_input_delivery,
)
from tests import test_persistent_recycler_real_postgres as authority_fixtures
from tests.test_historical_agent_pod_cleanup_real_postgres import ClaimantK8sApi

db = authority_fixtures.db
pg_dsn = authority_fixtures.pg_dsn
_schema_applied = authority_fixtures._schema_applied

NAMESPACE = "agents-a"
LOW_ATTEMPT = "00000000-0000-4000-8000-000000000001"
HIGH_ATTEMPT = "ffffffff-ffff-4fff-bfff-fffffffffff1"


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


class SelfEndK8sApi(ClaimantK8sApi):
    """Claimant model that also installs Pods without an agent PVC."""

    def install_life(self, life):
        self.install_old_pod(
            namespace=NAMESPACE,
            name=life["pod_name"],
            uid=life["pod_uid"],
            labels={
                "srw/managed-by": "agent-provisioner",
                "srw/purpose": "session",
                "srw.io/thread-id": life["thread"],
                "srw.io/runtime-generation": life["generation"],
                "srw.io/provision-attempt": life["attempt"],
            },
        )
        pod = self.pods[(NAMESPACE, life["pod_name"])]
        volumes = (
            [NS(persistent_volume_claim=NS(claim_name=life["claim"]["pvc_name"]))]
            if life["claim"]
            else [NS(persistent_volume_claim=None)]
        )
        pod.spec = NS(
            containers=[NS(name="agent")],
            init_containers=[],
            ephemeral_containers=[],
            volumes=volumes,
        )
        pod.status.container_statuses[0].name = "agent"
        return pod

    def exit_and_reap(self, life):
        """The process exits; ``reap_pods`` then requests exact deletion."""

        self.mark_terminal(NAMESPACE, life["pod_name"])
        pod = self.pods[(NAMESPACE, life["pod_name"])]
        pod.metadata.deletion_timestamp = "now"
        return pod


class Stack:
    def __init__(self, db, k8s):
        self.db = db
        self.k8s = k8s
        self.retirement = controls_composition.thread_retirement_operations(
            main.app.state.resources
        )


@pytest.fixture
def stack(db, monkeypatch):
    k8s = SelfEndK8sApi()
    provider = AgentProvisioner()
    provider._k8s_available = True
    provider._core_api = k8s
    provider._namespace = NAMESPACE
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
            namespace=NAMESPACE,
            ingress_host="unused.example",
            core_api=core_api,
            networking_api=networking_api,
        ),
    )
    return Stack(db, k8s)


async def _thread(db):
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
    return ids


async def _bind_life(stack, ids, *, with_claim: bool, attempt: str | None = None):
    """Provision, publish and use one dedicated life of ``ids['thread']``."""

    db = stack.db
    generation = str((await db.get_thread(ids["thread"]))["runtime_generation"])
    life = {
        **ids,
        "generation": generation,
        "agent": str(uuid4()),
        "attach_token": str(uuid4()),
        "attempt": attempt or str(uuid4()),
        "pod_name": f"srw-agent-s-{uuid4().hex[:8]}",
        "pod_uid": str(uuid4()),
    }
    reserved = await db.reserve_pinned_agent_pod_provision_intent(
        ids["thread"],
        expected_runtime_generation=generation,
        attempt_id=life["attempt"],
        pod_name=life["pod_name"],
        provisioner="agent",
        namespace=NAMESPACE,
        pvc_name=f"pvc-agent-{ids['thread'][:12]}" if with_claim else None,
    )
    assert reserved
    claim = reserved.get("workspace_claim") if with_claim else None
    if with_claim:
        assert claim
        assert await db.publish_pinned_agent_workspace_claim(
            ids["thread"],
            expected_runtime_generation=generation,
            claim_id=str(claim["claim_id"]),
            pvc_name=claim["pvc_name"],
            pvc_uid=f"pvc-{ids['thread']}",
            namespace=NAMESPACE,
        )
        if (NAMESPACE, claim["pvc_name"]) not in stack.k8s.pvcs:
            stack.k8s.install_pvc(
                namespace=NAMESPACE,
                name=claim["pvc_name"],
                uid=f"pvc-{ids['thread']}",
                labels={
                    "srw.io/thread-id": ids["thread"],
                    "srw.io/runtime-generation": str(
                        claim["created_runtime_generation"]
                    ),
                    "srw.io/workspace-claim": str(claim["claim_id"]),
                    "srw.io/provision-attempt": str(claim["create_attempt"]),
                    "srw.io/claim-provisioner": "agent",
                },
            )
    life["claim"] = claim
    assert await db.publish_pinned_agent_pod_provision_intent(
        ids["thread"],
        expected_runtime_generation=generation,
        attempt_id=life["attempt"],
        pod_name=life["pod_name"],
        pod_uid=life["pod_uid"],
        namespace=NAMESPACE,
    )
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO agents (id,config_name,hostname,pod_uid,status,agent_mode) "
                "VALUES ($1::uuid,'session_base',$2,$3,'session','persistent')",
                life["agent"],
                life["pod_name"],
                life["pod_uid"],
            )
            await conn.execute(
                "UPDATE threads SET status='active',agent_id=$2::uuid,"
                "control_admission_agent_id=$2::uuid,runtime_attach_token=$3::uuid "
                "WHERE id=$1::uuid",
                ids["thread"],
                life["agent"],
                life["attach_token"],
            )
            await conn.execute(
                "UPDATE agents SET thread_id=$2::uuid WHERE id=$1::uuid",
                life["agent"],
                ids["thread"],
            )
            authority = dict(
                agent_id=life["agent"],
                pod_uid=life["pod_uid"],
                runtime_generation=generation,
                runtime_attach_token=life["attach_token"],
            )
            delivery_id = uuid4()
            delivery = await persist_input_delivery(
                conn,
                thread_id=ids["thread"],
                delivery_id=delivery_id,
                role="human",
                content="One turn on this dedicated life",
                source="direct_human",
                turn_number=1,
                **authority,
            )
            transition = dict(
                delivery_id=delivery_id,
                claim_generation=int(delivery["claim_generation"]),
                **authority,
            )
            assert await mark_input_delivery_queued(conn, **transition)
            assert await transition_input_delivery(
                conn, transition="admitted", turn_number=1, **transition
            )
            assert await transition_input_delivery(
                conn, transition="settled", **transition
            )
    stack.k8s.install_life(life)
    return life


async def _ack_local_quiescence(db, life, retirement):
    receipt = await db.acknowledge_pinned_thread_local_quiescence(
        life["thread"],
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_agent_id=life["agent"],
        expected_attach_token=life["attach_token"],
        expected_settle_status="ended",
        expected_quiescence_protocol="agent_runtime_zero_v1",
        expected_workspace_generation=None,
        expected_workspace_runtime_incarnation=None,
    )
    assert receipt is not None


async def _agent_settles_soft_end(stack, life, *, initiator: str = "agent"):
    """What the Pod's own teardown does for idle timeout / ``archive``.

    ``initiator='owner'`` models an owner End whose authorized marker the
    agent's watchdog observed; the agent settles it the same way.
    """

    db = stack.db
    if initiator == "agent":
        retirement = await db.begin_pinned_thread_retirement(
            life["thread"],
            permanent=False,
            settle_status="ended",
            initiator="agent",
            expected_runtime_generation=life["generation"],
            expected_agent_id=life["agent"],
            expected_attach_token=life["attach_token"],
            authorize_immediately=True,
        )
    else:
        retirement = await db.begin_pinned_thread_retirement(
            life["thread"], permanent=False
        )
        assert await db.authorize_pinned_thread_retirement(
            life["thread"],
            token=retirement["token"],
            generation=retirement["generation"],
            settle_status="ended",
        )
    assert retirement["state"] == "pending"
    await _ack_local_quiescence(db, life, retirement)
    result = await stack.retirement.end_thread_flow(
        life["thread"],
        await db.get_thread(life["thread"]),
        permanent=False,
        force=True,
        expected_runtime_generation=life["generation"],
        expected_agent_id=life["agent"],
        expected_attach_token=life["attach_token"],
        settle_status="ended",
        local_runtime_quiesced=True,
        retiring_agent_response_pending=False,
    )
    assert result == {"status": "ended"}
    thread = await db.get_thread(life["thread"])
    assert thread["status"] == "ended"
    agent = await db.fetchrow(
        "SELECT status,thread_id FROM agents WHERE id=$1::uuid", life["agent"]
    )
    assert agent["status"] == "offline" and agent["thread_id"] is None
    # Settlement leaves the Pod to exit by itself: its own request is the
    # caller, so the orchestrator never deletes it on this path.
    pod = stack.k8s.pods[(NAMESPACE, life["pod_name"])]
    assert pod.metadata.deletion_timestamp is None
    assert pod.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
    return retirement


async def _permanent_delete(stack, thread_id):
    """One owner DELETE ?permanent=true; returns the status or the 503."""

    thread = await stack.db.get_thread(thread_id)
    if thread is None:
        return "gone"
    try:
        result = await stack.retirement.end_thread_flow(
            thread_id, thread, permanent=True, force=False
        )
    except HTTPException as exc:
        return exc.status_code, exc.detail
    return result.get("status")


async def _delete_until_settled(stack, thread_id, attempts: int = 4):
    outcomes = []
    for _ in range(attempts):
        outcome = await _permanent_delete(stack, thread_id)
        outcomes.append(outcome)
        if isinstance(outcome, str) and outcome in {"deleted", "gone"}:
            break
    return outcomes


def _pod(stack, life):
    return stack.k8s.pods.get((NAMESPACE, life["pod_name"]))


async def _outcome_proof(db, life):
    row = await db.fetchrow(
        "SELECT retired_agent_pod FROM thread_runtime_retirement_outcomes "
        "WHERE thread_id=$1::uuid AND runtime_generation=$2::uuid AND NOT permanent",
        life["thread"],
        life["generation"],
    )
    return _json(row["retired_agent_pod"]) if row else None


# ---------------------------------------------------------------------------
# Settlement records the exact retired Pod for every dedicated life
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_self_ended_claimless_life_records_its_exact_retired_pod(stack):
    ids = await _thread(stack.db)
    life = await _bind_life(stack, ids, with_claim=False)

    await _agent_settles_soft_end(stack, life)

    assert await _outcome_proof(stack.db, life) == {
        "version": 1,
        "pod_name": life["pod_name"],
        "pod_uid": life["pod_uid"],
        "namespace": NAMESPACE,
        "provisioner": "agent",
        "provision_attempt": life["attempt"],
        "protection_protocol": "finalizer_v1",
    }


@pytest.mark.asyncio
async def test_self_ended_claim_bearing_proof_is_unchanged(stack):
    ids = await _thread(stack.db)
    life = await _bind_life(stack, ids, with_claim=True)

    await _agent_settles_soft_end(stack, life)

    proof = await _outcome_proof(stack.db, life)
    assert proof["pod_uid"] == life["pod_uid"]
    assert proof["workspace_claim_id"] == str(life["claim"]["claim_id"])
    assert proof["pvc_name"] == life["claim"]["pvc_name"]


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["actor_pod_uid", "pool_marker", "warm_marker"])
async def test_claimless_capture_requires_every_exact_relation(stack, fault):
    """An incomplete claim-less shape records nothing and never blocks End."""

    ids = await _thread(stack.db)
    life = await _bind_life(stack, ids, with_claim=False)
    async with stack.db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            if fault == "actor_pod_uid":
                await conn.execute(
                    "UPDATE thread_agent_pod_provision_intents SET pod_uid=$2 "
                    "WHERE attempt_id=$1::uuid",
                    life["attempt"],
                    str(uuid4()),
                )
            elif fault == "pool_marker":
                await conn.execute(
                    "UPDATE threads SET metadata=metadata #- "
                    "'{agent_pod,provision_attempt}' WHERE id=$1::uuid",
                    ids["thread"],
                )
            else:
                await conn.execute(
                    "UPDATE threads SET metadata=jsonb_set(metadata,"
                    "'{agent_pod,warm_binding_protection}',to_jsonb($2::text)) "
                    "WHERE id=$1::uuid",
                    ids["thread"],
                    str(uuid4()),
                )

    retirement = await stack.db.begin_pinned_thread_retirement(
        life["thread"],
        permanent=False,
        settle_status="ended",
        initiator="agent",
        expected_runtime_generation=life["generation"],
        expected_agent_id=life["agent"],
        expected_attach_token=life["attach_token"],
        authorize_immediately=True,
    )
    if retirement.get("state") != "pending":
        # The shape is refused before any settlement; nothing can be captured.
        assert await _outcome_proof(stack.db, life) is None
        return
    await _ack_local_quiescence(stack.db, life, retirement)
    settled = await stack.db.settle_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        final_status="ended",
    )
    # A warm marker without its bound protection row is refused outright;
    # the other shapes settle normally but capture no Pod relation.
    assert settled is (fault != "warm_marker")
    assert await _outcome_proof(stack.db, life) is None


# ---------------------------------------------------------------------------
# Self-ended life → Pod exits → permanent Delete reaches 404
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("with_claim", [True, False], ids=["pvc", "no-pvc"])
async def test_self_ended_life_deletes_after_its_pod_exits(stack, with_claim):
    ids = await _thread(stack.db)
    life = await _bind_life(stack, ids, with_claim=with_claim)
    await _agent_settles_soft_end(stack, life)
    stack.k8s.exit_and_reap(life)

    outcomes = await _delete_until_settled(stack, ids["thread"])

    assert outcomes[-1] == "deleted", outcomes
    assert await stack.db.get_thread(ids["thread"]) is None
    assert _pod(stack, life) is None
    assert stack.k8s.removed_pods == [life["pod_uid"]]
    if with_claim:
        assert stack.k8s.deleted_pvcs == [f"pvc-{ids['thread']}"]


@pytest.mark.asyncio
@pytest.mark.parametrize("with_claim", [True, False], ids=["pvc", "no-pvc"])
async def test_self_ended_running_pod_keeps_delete_retryable_until_it_exits(
    stack, with_claim
):
    """A Pod whose process never exited is not cleanup authority."""

    ids = await _thread(stack.db)
    life = await _bind_life(stack, ids, with_claim=with_claim)
    await _agent_settles_soft_end(stack, life)

    outcomes = await _delete_until_settled(stack, ids["thread"], attempts=3)

    assert all(
        isinstance(item, tuple)
        and item[0] == 503
        and item[1]["code"] == "pinned_retirement_retry_pending"
        for item in outcomes
    ), outcomes
    pod = _pod(stack, life)
    assert pod is not None and pod.metadata.deletion_timestamp is None
    assert pod.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
    assert await stack.db.get_thread(ids["thread"]) is not None
    assert not stack.k8s.deleted_pvcs

    stack.k8s.exit_and_reap(life)
    outcomes = await _delete_until_settled(stack, ids["thread"])
    assert outcomes[-1] == "deleted", outcomes
    assert _pod(stack, life) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("with_claim", [True, False], ids=["pvc", "no-pvc"])
async def test_soft_end_predecessor_resume_self_end_then_permanent_delete(
    stack, with_claim
):
    ids = await _thread(stack.db)
    first = await _bind_life(stack, ids, with_claim=with_claim)
    await _agent_settles_soft_end(stack, first, initiator="owner")
    # The owner End's agent exited; reap_pods left it Terminating under the
    # protection finalizer — the retained predecessor.
    stack.k8s.exit_and_reap(first)
    assert await stack.db.resume_thread(ids["thread"])
    second = await _bind_life(stack, ids, with_claim=with_claim)
    assert second["generation"] != first["generation"]
    if with_claim:
        assert second["claim"]["claim_id"] == first["claim"]["claim_id"]
    await _agent_settles_soft_end(stack, second)
    stack.k8s.exit_and_reap(second)

    outcomes = await _delete_until_settled(stack, ids["thread"])

    assert outcomes[-1] == "deleted", outcomes
    assert _pod(stack, first) is None and _pod(stack, second) is None
    assert sorted(stack.k8s.removed_pods) == sorted(
        [first["pod_uid"], second["pod_uid"]]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("with_claim", [True, False], ids=["pvc", "no-pvc"])
async def test_terminal_predecessor_is_retired_while_a_successor_still_runs(
    stack, with_claim
):
    """One live historical claimant must not hold an exited one hostage."""

    ids = await _thread(stack.db)
    first = await _bind_life(stack, ids, with_claim=with_claim, attempt=HIGH_ATTEMPT)
    await _agent_settles_soft_end(stack, first, initiator="owner")
    stack.k8s.exit_and_reap(first)
    assert await stack.db.resume_thread(ids["thread"])
    # The successor's attempt sorts first; it has not exited.
    second = await _bind_life(stack, ids, with_claim=with_claim, attempt=LOW_ATTEMPT)
    await _agent_settles_soft_end(stack, second)

    outcome = await _permanent_delete(stack, ids["thread"])
    outcome = await _permanent_delete(stack, ids["thread"])

    assert isinstance(outcome, tuple) and outcome[0] == 503, outcome
    assert _pod(stack, first) is None
    running = _pod(stack, second)
    assert running is not None and running.metadata.deletion_timestamp is None
    assert running.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
    assert not stack.k8s.deleted_pvcs

    stack.k8s.exit_and_reap(second)
    outcomes = await _delete_until_settled(stack, ids["thread"])
    assert outcomes[-1] == "deleted", outcomes


# ---------------------------------------------------------------------------
# Refusals: changed or incomplete authority never mutates a Pod
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "replacement_pod",
        "wrong_generation_label",
        "wrong_attempt_label",
        "reused_agent",
        "rebound_agent",
        "init_running",
        "changed_proof",
    ],
)
@pytest.mark.parametrize("with_claim", [True, False], ids=["pvc", "no-pvc"])
async def test_self_ended_retirement_refuses_changed_authority(
    stack, with_claim, fault
):
    ids = await _thread(stack.db)
    life = await _bind_life(stack, ids, with_claim=with_claim)
    await _agent_settles_soft_end(stack, life)
    pod = stack.k8s.exit_and_reap(life)
    original_uid = life["pod_uid"]
    if fault == "replacement_pod":
        pod.metadata.uid = str(uuid4())
    elif fault == "wrong_generation_label":
        pod.metadata.labels["srw.io/runtime-generation"] = str(uuid4())
    elif fault == "wrong_attempt_label":
        pod.metadata.labels["srw.io/provision-attempt"] = str(uuid4())
    elif fault == "reused_agent":
        await stack.db.execute(
            "UPDATE agents SET status='ready' WHERE id=$1::uuid", life["agent"]
        )
    elif fault == "rebound_agent":
        # The old actor row now serves another thread (reciprocal binding
        # modelled without the attach path's triggers).
        other = await _thread(stack.db)
        async with stack.db.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL session_replication_role='replica'")
                await conn.execute(
                    "UPDATE agents SET thread_id=$2::uuid,status='session' "
                    "WHERE id=$1::uuid",
                    life["agent"],
                    other["thread"],
                )
    elif fault == "init_running":
        pod.spec.init_containers = [NS(name="sidecar")]
        pod.status.init_container_statuses = [
            NS(name="sidecar", state=NS(terminated=None))
        ]
    elif fault == "changed_proof":
        async with stack.db.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL session_replication_role='replica'")
                await conn.execute(
                    "UPDATE thread_runtime_retirement_outcomes SET "
                    "retired_agent_pod=jsonb_set(retired_agent_pod,'{namespace}',"
                    "'\"agents-b\"') WHERE thread_id=$1::uuid",
                    ids["thread"],
                )

    outcomes = await _delete_until_settled(stack, ids["thread"], attempts=2)

    assert all(isinstance(item, tuple) and item[0] == 503 for item in outcomes), (
        outcomes
    )
    assert await stack.db.get_thread(ids["thread"]) is not None
    current = _pod(stack, life)
    assert current is not None
    assert current.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
    assert original_uid not in stack.k8s.removed_pods
    assert not stack.k8s.deleted_pvcs


@pytest.mark.asyncio
async def test_claimless_outcome_without_a_captured_pod_stays_untouched(stack):
    """A pre-migration outcome carries no Pod relation: never infer one.

    The permanent Delete behaves as it always did for a claim-less life (the
    thread is deleted); the Pod is not deleted or unprotected by name,
    generation or absence of its actor.
    """

    ids = await _thread(stack.db)
    life = await _bind_life(stack, ids, with_claim=False)
    await _agent_settles_soft_end(stack, life)
    async with stack.db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(
                "UPDATE thread_runtime_retirement_outcomes SET retired_agent_pod=NULL "
                "WHERE thread_id=$1::uuid",
                ids["thread"],
            )
    stack.k8s.exit_and_reap(life)

    outcomes = await _delete_until_settled(stack, ids["thread"])

    assert outcomes[-1] == "deleted", outcomes
    pod = _pod(stack, life)
    assert pod is not None and pod.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
    assert not stack.k8s.removed_pods


# ---------------------------------------------------------------------------
# Repeated and concurrent deletion converge exactly once
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("with_claim", [True, False], ids=["pvc", "no-pvc"])
async def test_concurrent_and_repeated_permanent_deletes_converge_once(
    stack, with_claim
):
    ids = await _thread(stack.db)
    life = await _bind_life(stack, ids, with_claim=with_claim)
    await _agent_settles_soft_end(stack, life)
    stack.k8s.exit_and_reap(life)

    first = await asyncio.gather(
        _permanent_delete(stack, ids["thread"]),
        _permanent_delete(stack, ids["thread"]),
        return_exceptions=True,
    )
    assert not any(isinstance(item, BaseException) for item in first), first
    outcomes = await _delete_until_settled(stack, ids["thread"])

    assert outcomes[-1] in ("deleted", "gone"), (first, outcomes)
    assert await stack.db.get_thread(ids["thread"]) is None
    assert stack.k8s.removed_pods == [life["pod_uid"]]
    assert await _permanent_delete(stack, ids["thread"]) == "gone"
    if with_claim:
        assert stack.k8s.deleted_pvcs == [f"pvc-{ids['thread']}"]


# ---------------------------------------------------------------------------
# Active-session deletion keeps its captured-actor path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("with_claim", [True, False], ids=["pvc", "no-pvc"])
async def test_active_resumed_life_deletion_still_stops_only_its_captured_pod(
    stack, with_claim
):
    ids = await _thread(stack.db)
    first = await _bind_life(stack, ids, with_claim=with_claim)
    await _agent_settles_soft_end(stack, first, initiator="owner")
    stack.k8s.exit_and_reap(first)
    assert await stack.db.resume_thread(ids["thread"])
    current = await _bind_life(stack, ids, with_claim=with_claim)

    permanent = await stack.db.begin_pinned_thread_retirement(
        ids["thread"], permanent=True
    )
    assert await stack.db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=permanent["token"],
        generation=permanent["generation"],
        settle_status="ended",
    )
    await _ack_local_quiescence(stack.db, current, permanent)
    stack.k8s.mark_terminal(NAMESPACE, current["pod_name"])
    operations = controls_composition.pinned_retirement_operations(
        main.app.state.resources
    )
    await operations.cleanup_pinned_thread_retirement(permanent)

    assert _pod(stack, current) is None and _pod(stack, first) is None
    assert stack.k8s.removed_pods[0] == current["pod_uid"]
    assert sorted(stack.k8s.removed_pods) == sorted(
        [first["pod_uid"], current["pod_uid"]]
    )


# ---------------------------------------------------------------------------
# Active permanent Delete: the agent's own final ACK hands the exact Pod to
# the durable retry for every finalizer-protected Pod, not only claim-bearing
# ones — otherwise the thread is deleted while its exited Pod stays
# Terminating under SRW's finalizer with no owner left.
# ---------------------------------------------------------------------------


async def _owner_permanent_then_agent_ack(stack, life):
    """Owner DELETE while live, then the agent's exact final ACK."""

    owner = await stack.retirement.end_thread_flow(
        life["thread"],
        await stack.db.get_thread(life["thread"]),
        permanent=True,
        force=True,
    )
    assert owner["status"] == "ending"
    pending = await stack.db.get_thread(life["thread"])
    retirement = {
        "token": str(pending["runtime_retirement_token"]),
        "generation": life["generation"],
        "context": _json(pending["runtime_retirement_context"]),
    }
    await _ack_local_quiescence(stack.db, life, retirement)
    return await stack.retirement.end_thread_flow(
        life["thread"],
        await stack.db.get_thread(life["thread"]),
        permanent=True,
        force=True,
        expected_runtime_generation=life["generation"],
        expected_agent_id=life["agent"],
        expected_attach_token=life["attach_token"],
        local_runtime_quiesced=True,
        retiring_agent_response_pending=True,
    )


async def _durable_retry(stack, life):
    """What ``retry_pending_pinned_retirement`` runs once the actor is gone."""

    thread = await stack.db.get_thread(life["thread"])
    if thread is None:
        return {"status": "deleted"}
    return await stack.retirement.end_thread_flow(
        life["thread"],
        thread,
        permanent=True,
        force=True,
        expected_runtime_generation=life["generation"],
        expected_agent_id=life["agent"],
        expected_attach_token=life["attach_token"],
        require_expected_agent_offline=False,
        settle_status="ended",
        local_runtime_quiesced=True,
    )


@pytest.mark.asyncio
async def test_active_claimless_permanent_ack_hands_the_exact_pod_to_retry(stack):
    ids = await _thread(stack.db)
    life = await _bind_life(stack, ids, with_claim=False)

    handoff = await _owner_permanent_then_agent_ack(stack, life)

    assert handoff.get("status") == "ending", handoff
    assert handoff.get("retiring_agent_exit_authorized") is True
    assert await stack.db.get_thread(ids["thread"]) is not None
    pod = _pod(stack, life)
    assert pod is not None and pod.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
    assert pod.metadata.deletion_timestamp is None

    # The authorized agent exits; once its heartbeats age out the durable
    # retry (proven receipt) finishes the same token.
    stack.k8s.exit_and_reap(life)
    result = await _durable_retry(stack, life)

    assert result.get("status") == "deleted", result
    assert await stack.db.get_thread(ids["thread"]) is None
    assert _pod(stack, life) is None
    assert stack.k8s.removed_pods == [life["pod_uid"]]


@pytest.mark.asyncio
async def test_active_claimless_retry_refuses_a_pod_that_did_not_exit(stack):
    """The retry stops only the exact captured Pod; a live one is deleted and
    waited for, never unprotected while its containers run."""

    ids = await _thread(stack.db)
    life = await _bind_life(stack, ids, with_claim=False)
    handoff = await _owner_permanent_then_agent_ack(stack, life)
    assert handoff.get("retiring_agent_exit_authorized") is True

    with pytest.raises(HTTPException) as retry:
        await _durable_retry(stack, life)

    assert retry.value.status_code == 503
    pod = _pod(stack, life)
    assert pod is not None
    assert pod.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
    assert await stack.db.get_thread(ids["thread"]) is not None


@pytest.mark.asyncio
async def test_active_warm_pool_permanent_ack_hands_the_exact_pod_to_retry(
    db, monkeypatch
):
    """A warm dual Pod bound to the session is protected the same way."""

    from orchestrator.services.pinned_agent_authority import (
        reserve_pinned_warm_agent_binding,
    )

    ids = await authority_fixtures._seed_warm_pool_binding(db, bound=False)
    api = authority_fixtures.StatefulPinnedK8sApi()
    authority_fixtures._install_warm_pool_pod(api, ids)
    monkeypatch.setenv("PINNED_LEGACY_AGENT_NAMESPACES", "agents-a")
    provisioner = authority_fixtures._production_warm_provisioner(
        db, api, namespace="agents-a"
    )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(agent_provisioner_module, "agent_provisioner", provisioner)
    monkeypatch.setattr(
        main.app.state.resources.session_router,
        "teardown_route",
        AsyncMock(return_value=True),
    )
    async with db.acquire() as conn:
        metadata = _json(
            await conn.fetchval(
                "SELECT metadata FROM threads WHERE id=$1::uuid", ids["thread"]
            )
        )
        metadata["config_override"]["officer"]["enabled"] = False
        await conn.execute(
            "DELETE FROM project_officers WHERE thread_id=$1::uuid", ids["thread"]
        )
        await conn.execute(
            "UPDATE threads SET metadata=$2::jsonb WHERE id=$1::uuid",
            ids["thread"],
            json.dumps(metadata),
        )
    bound = await reserve_pinned_warm_agent_binding(
        db,
        agent_provisioner=provisioner,
        persistent_provisioner=None,
        thread_id=ids["thread"],
        agent_id=ids["agent"],
        expected_runtime_generation=ids["runtime_generation"],
    )
    assert bound.bound
    life = {
        **ids,
        "generation": ids["runtime_generation"],
        "attach_token": bound.attach_token,
    }
    pod = api.pods[("agents-a", ids["pod_name"])]
    assert pod.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
    retirement_ops = controls_composition.thread_retirement_operations(
        main.app.state.resources
    )
    stack = NS(db=db, retirement=retirement_ops)

    handoff = await _owner_permanent_then_agent_ack(stack, life)

    assert handoff.get("status") == "ending", handoff
    assert handoff.get("retiring_agent_exit_authorized") is True
    assert pod.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]

    api.mark_terminal("agents-a", ids["pod_name"])
    result = await _durable_retry(stack, life)

    assert result.get("status") == "deleted", result
    assert await db.get_thread(ids["thread"]) is None
    assert ("agents-a", ids["pod_name"]) not in api.pods


# ---------------------------------------------------------------------------
# Warm-pool protection ledger settles with the life it protected
# ---------------------------------------------------------------------------


async def _bind_warm_life(db, monkeypatch):
    """Bind a warm dual pool Pod to a pinned session through the attach path."""

    from orchestrator.services.pinned_agent_authority import (
        reserve_pinned_warm_agent_binding,
    )

    ids = await authority_fixtures._seed_warm_pool_binding(db, bound=False)
    api = authority_fixtures.StatefulPinnedK8sApi()
    authority_fixtures._install_warm_pool_pod(api, ids)
    monkeypatch.setenv("PINNED_LEGACY_AGENT_NAMESPACES", "agents-a")
    provisioner = authority_fixtures._production_warm_provisioner(
        db, api, namespace="agents-a"
    )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(agent_provisioner_module, "agent_provisioner", provisioner)
    monkeypatch.setattr(
        main.app.state.resources.session_router,
        "teardown_route",
        AsyncMock(return_value=True),
    )
    async with db.acquire() as conn:
        metadata = _json(
            await conn.fetchval(
                "SELECT metadata FROM threads WHERE id=$1::uuid", ids["thread"]
            )
        )
        metadata["config_override"]["officer"]["enabled"] = False
        await conn.execute(
            "DELETE FROM project_officers WHERE thread_id=$1::uuid", ids["thread"]
        )
        await conn.execute(
            "UPDATE threads SET metadata=$2::jsonb WHERE id=$1::uuid",
            ids["thread"],
            json.dumps(metadata),
        )
    bound = await reserve_pinned_warm_agent_binding(
        db,
        agent_provisioner=provisioner,
        persistent_provisioner=None,
        thread_id=ids["thread"],
        agent_id=ids["agent"],
        expected_runtime_generation=ids["runtime_generation"],
    )
    assert bound.bound
    life = {
        **ids,
        "generation": ids["runtime_generation"],
        "attach_token": bound.attach_token,
    }
    stack = NS(
        db=db,
        retirement=controls_composition.thread_retirement_operations(
            main.app.state.resources
        ),
    )
    return life, api, provisioner, stack


async def _warm_agent_self_end(stack, life):
    """The warm Pod's own teardown (idle timeout / ``archive``)."""

    db = stack.db
    retirement = await db.begin_pinned_thread_retirement(
        life["thread"],
        permanent=False,
        settle_status="ended",
        initiator="agent",
        expected_runtime_generation=life["generation"],
        expected_agent_id=life["agent"],
        expected_attach_token=life["attach_token"],
        authorize_immediately=True,
    )
    assert retirement["state"] == "pending"
    await _ack_local_quiescence(db, life, retirement)
    ended = await stack.retirement.end_thread_flow(
        life["thread"],
        await db.get_thread(life["thread"]),
        permanent=False,
        force=True,
        expected_runtime_generation=life["generation"],
        expected_agent_id=life["agent"],
        expected_attach_token=life["attach_token"],
        settle_status="ended",
        local_runtime_quiesced=True,
        retiring_agent_response_pending=False,
    )
    assert ended == {"status": "ended"}


async def _warm_protections(db, life):
    rows = await db.fetch(
        "SELECT protection_id,status,release_outcome,released_at,agent_id,pod_uid "
        "FROM thread_agent_warm_binding_protections WHERE thread_id=$1::uuid",
        life["thread"],
    )
    return [dict(row) for row in rows]


async def _assert_warm_ledger_settled(db, life, *, outcome):
    rows = await _warm_protections(db, life)
    assert len(rows) == 1, rows
    row = rows[0]
    assert (row["status"], row["release_outcome"]) == ("released", outcome), row
    assert row["released_at"] is not None
    assert str(row["agent_id"]) == life["agent"]
    assert row["pod_uid"] == life["pod_uid"]
    # Nothing actionable is left for this life: no bound/releasing record, and
    # the agent-active uniqueness slot is free again.
    assert not await db.fetchval(
        "SELECT count(*) FROM thread_agent_warm_binding_protections "
        "WHERE agent_id=$1::uuid "
        "AND status IN ('planned','protecting','protected','bound','releasing')",
        life["agent"],
    )


@pytest.mark.asyncio
async def test_active_warm_permanent_delete_settles_its_warm_protection(
    db, monkeypatch
):
    """The exact stop of a warm Pod must also settle its durable protection.

    A ``bound`` row whose thread row is gone can never move again (``bound``
    only leaves through ``releasing``, which the reciprocity check fences on
    the thread row), so the settlement has to happen with the delete.
    """

    life, api, _, stack = await _bind_warm_life(db, monkeypatch)
    handoff = await _owner_permanent_then_agent_ack(stack, life)
    assert handoff.get("retiring_agent_exit_authorized") is True
    api.mark_terminal("agents-a", life["pod_name"])

    result = await _durable_retry(stack, life)

    assert result.get("status") == "deleted", result
    assert await db.get_thread(life["thread"]) is None
    assert ("agents-a", life["pod_name"]) not in api.pods
    await _assert_warm_ledger_settled(db, life, outcome="exact_absent_v1")
    agent = await db.fetchrow(
        "SELECT status,thread_id FROM agents WHERE id=$1::uuid", life["agent"]
    )
    assert agent is None or (agent["status"], agent["thread_id"]) == ("offline", None)


@pytest.mark.asyncio
async def test_warm_self_end_releases_its_protection_then_deletes(db, monkeypatch):
    """A warm Pod's own End returns it to the pool; Delete leaves history."""

    life, api, _, stack = await _bind_warm_life(db, monkeypatch)
    await _warm_agent_self_end(stack, life)
    pod = api.pods[("agents-a", life["pod_name"])]
    assert pod.metadata.finalizers == []
    await _assert_warm_ledger_settled(db, life, outcome="exact_live_unprotected_v1")

    outcomes = await _delete_until_settled(stack, life["thread"])

    assert outcomes[-1] == "deleted", outcomes
    assert await db.get_thread(life["thread"]) is None
    # The pool owns its Pod: the session's Delete never stops it.
    assert ("agents-a", life["pod_name"]) in api.pods
    await _assert_warm_ledger_settled(db, life, outcome="exact_live_unprotected_v1")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "no_outcome",
        "soft_outcome",
        "other_attach_token",
        "other_generation",
        "agent_not_draining",
        None,
    ],
)
async def test_absent_thread_warm_release_needs_its_exact_permanent_outcome(
    db, monkeypatch, fault
):
    """``releasing`` without a thread row is fenced only by this life's
    permanent ``deleted`` outcome and a detached, draining actor."""

    import asyncpg

    life, _, _, _ = await _bind_warm_life(db, monkeypatch)
    protection = (await _warm_protections(db, life))[0]["protection_id"]
    async with db.acquire() as conn:
        async with conn.transaction():
            # Stage the post-delete shape without the application path.
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(
                "UPDATE agents SET thread_id=NULL,status=$2 WHERE id=$1::uuid",
                life["agent"],
                "session" if fault == "agent_not_draining" else "draining",
            )
            await conn.execute("DELETE FROM threads WHERE id=$1::uuid", life["thread"])
            if fault != "no_outcome":
                await conn.execute(
                    "INSERT INTO thread_runtime_retirement_outcomes (thread_id,"
                    "runtime_generation,retirement_token,agent_id,"
                    "runtime_attach_token,disposition,permanent,outcome) "
                    "VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,$5::uuid,"
                    "'ended',$6,$7)",
                    life["thread"],
                    str(uuid4()) if fault == "other_generation" else life["generation"],
                    str(uuid4()),
                    life["agent"],
                    (
                        str(uuid4())
                        if fault == "other_attach_token"
                        else life["attach_token"]
                    ),
                    fault != "soft_outcome",
                    "settled" if fault == "soft_outcome" else "deleted",
                )

    async def release():
        async with db.acquire() as conn:
            async with conn.transaction():
                return await conn.execute(
                    "UPDATE thread_agent_warm_binding_protections SET "
                    "status='releasing',release_started_at=transaction_timestamp() "
                    "WHERE protection_id=$1::uuid AND status='bound'",
                    protection,
                )

    if fault is None:
        assert await release() == "UPDATE 1"
        assert (await _warm_protections(db, life))[0]["status"] == "releasing"
    else:
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await release()
        assert (await _warm_protections(db, life))[0]["status"] == "bound"
