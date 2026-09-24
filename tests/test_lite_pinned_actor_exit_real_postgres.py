"""Lite-tier (`workspace.backend: none`) pinned actor crash recovery with real SQL.

Officers and their conferences run on the pinned lite tier: one agent Pod,
no workspace, no binding. When that agent stops answering after Begin, the
durable retirement can only be finished by the orchestrator proving the exact
Pod is gone and receipting `agent_runtime_zero_v1`. Two contracts cover the
two lives such an actor can have had:

* a *created* life — registered, never admitted an input — settles through the
  zero-admission receipt the receipt trigger has accepted for backend `none`
  all along; only the Python recovery gate refused to reach it;
* a *used* life — inputs admitted and settled — settles through the settled
  actor exit (0225 for virtual, 0226 for lite), which refuses any unfinished
  input/child/control work so the anti-ABA fence stays intact.

Before 0226 every such retirement stayed pending forever while the sweep
retried it every minute in silence.
"""

from orchestrator.services import stale_agent_detector as stale_agent_detector_service
from types import SimpleNamespace as NS
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from orchestrator import main
from orchestrator.services.agent_provisioner import AgentProvisioner
from orchestrator.services.pinned_agent_authority import (
    reserve_pinned_warm_agent_binding,
)
from orchestrator.services.session_router import SessionRouterService
from shared.persistent_input_delivery import (
    mark_input_delivery_queued,
    persist_input_delivery,
    transition_input_delivery,
)
from tests import test_persistent_recycler_real_postgres as fixtures
from orchestrator.application import controls as controls_composition
from orchestrator.services import agent_provisioner as agent_provisioner_module

db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied


async def _retired_lite_actor(
    db, monkeypatch, *, permanent=False, status="active", input_state=None
):
    """One lite actor after Begin+authorize, its exact Pod already terminal.

    ``input_state`` seeds a single pinned input owned by that Pod and drives it
    to ``queued``/``admitted``/``settled``; ``None`` seeds no input at all.
    """
    ids = await fixtures._seed(db, protected_agent_pod=True, workspace_claim=False)
    ids.pop("old_access")
    await db.execute(
        "UPDATE threads SET status=$2 WHERE id=$1::uuid", ids["thread"], status
    )
    generation = str((await db.get_thread(ids["thread"]))["runtime_generation"])
    ids["generation"] = generation
    ids["process_generation"] = str(uuid4())
    ids["pod_uid"] = "old-pod"

    if input_state is not None:
        ids["delivery_id"] = uuid4()
        async with db.acquire() as conn:
            async with conn.transaction():
                authority = dict(
                    agent_id=ids["agent"],
                    pod_uid=ids["pod_uid"],
                    runtime_generation=ids["process_generation"],
                    session_runtime_generation=generation,
                    runtime_attach_token=ids["attach_token"],
                )
                delivery = await persist_input_delivery(
                    conn,
                    thread_id=ids["thread"],
                    delivery_id=ids["delivery_id"],
                    role="human",
                    content="A real admitted lite turn",
                    source="direct_human",
                    turn_number=1,
                    **authority,
                )
                args = dict(
                    delivery_id=ids["delivery_id"],
                    claim_generation=int(delivery["claim_generation"]),
                    **authority,
                )
                assert await mark_input_delivery_queued(conn, **args)
                if input_state in {"admitted", "settled"}:
                    assert await transition_input_delivery(
                        conn, transition="admitted", turn_number=1, **args
                    )
                if input_state == "settled":
                    assert await transition_input_delivery(
                        conn, transition="settled", **args
                    )

    k8s = fixtures.StatefulPinnedK8sApi()
    pod_name = f"persistent-{ids['thread'][:12]}"
    k8s.install_old_pod(
        namespace="agents-a",
        name=pod_name,
        uid=ids["pod_uid"],
        labels={
            "srw/component": "persistent-agent",
            "srw/thread-id": ids["thread"],
            "srw.io/runtime-generation": generation,
            "srw.io/provision-attempt": ids["provision_attempt"],
        },
    )
    k8s.mark_terminal("agents-a", pod_name)
    pod = k8s.pods[("agents-a", pod_name)]
    pod.spec = NS(
        containers=[NS(name="agent")],
        init_containers=[],
        ephemeral_containers=[],
        volumes=[],
    )
    pod.status.container_statuses[0].name = "agent"
    pod.metadata.deletion_timestamp = "now"
    provider = AgentProvisioner()
    provider._k8s_available = True
    provider._core_api = k8s
    monkeypatch.setattr(agent_provisioner_module, "agent_provisioner", provider)
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    # The captured route died with the stopped Pod. Exercise the real teardown's
    # 404 handling through injected APIs -- never the ambient kubeconfig, which
    # would make this test pass only on a machine with a reachable cluster.
    core_api = MagicMock()
    networking_api = MagicMock()
    core_api.read_namespaced_service.side_effect = fixtures._K8sError(404)
    networking_api.read_namespaced_ingress.side_effect = fixtures._K8sError(404)
    ids["route_core_api"] = core_api
    ids["route_networking_api"] = networking_api
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

    retirement = await db.begin_pinned_thread_retirement(
        ids["thread"], permanent=permanent
    )
    assert retirement["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=generation,
        settle_status="ended",
    )
    thread = await db.get_thread(ids["thread"])
    assert thread["runtime_retirement_local_quiescence"] is None
    # Begin captured the seeded lite tier truthfully — this is the shape the
    # orchestrator sees for every officer and conference on dev.
    context = fixtures._json(thread["runtime_retirement_context"])
    assert context["workspace_backend"] == "none"
    assert context.get("workspace_container") in (None, {})
    assert context.get("workspace_binding") in (None, {})
    return ids, retirement, k8s


def _receipt(thread):
    return fixtures._json(thread["runtime_retirement_local_quiescence"])


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
async def test_created_lite_actor_exit_settles_through_zero_admission(
    db, monkeypatch, permanent
):
    ids, retirement, k8s = await _retired_lite_actor(
        db, monkeypatch, permanent=permanent, status="created"
    )
    assert await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    assert not k8s.pods
    thread = await db.get_thread(ids["thread"])
    receipt = _receipt(thread)
    assert receipt["quiescence_protocol"] == "agent_runtime_zero_v1"
    assert receipt["quiescence_actor"] == "orchestrator"
    assert receipt["workspace_generation"] is None
    assert receipt["workspace_runtime_incarnation"] is None
    assert "recovery_protocol" not in receipt
    assert controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).retirement_has_exact_local_quiescence(retirement, thread)
    # Replay is idempotent: the receipt stands and nothing is re-actuated.
    assert await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
async def test_used_lite_actor_exit_settles_after_exact_pod_stop(
    db, monkeypatch, permanent
):
    ids, retirement, k8s = await _retired_lite_actor(
        db, monkeypatch, permanent=permanent, input_state="settled"
    )
    assert ids["process_generation"] != ids["generation"]
    assert await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    assert not k8s.pods
    thread = await db.get_thread(ids["thread"])
    receipt = _receipt(thread)
    assert receipt["quiescence_protocol"] == "agent_runtime_zero_v1"
    assert receipt["quiescence_actor"] == "orchestrator"
    assert receipt["recovery_protocol"] == "settled_lite_actor_exit_v1"
    assert receipt["agent_pod_uid"] == ids["pod_uid"]
    assert receipt["settled_input_count"] == 1
    assert (
        await db.acknowledge_settled_virtual_actor_exit(
            ids["thread"],
            runtime_generation=ids["generation"],
            retirement_token=retirement["token"],
            agent_id=ids["agent"],
            attach_token=ids["attach_token"],
            stopped_pod_uid=ids["pod_uid"],
        )
        == receipt
    )
    assert controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).retirement_has_exact_local_quiescence(retirement, thread)
    assert await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)


@pytest.mark.asyncio
@pytest.mark.parametrize("input_state", ["queued", "admitted"])
async def test_used_lite_actor_with_unfinished_input_stays_pending(
    db, monkeypatch, input_state
):
    """The fence holds: an admitted, unsettled turn is not settled work.

    A registered life with a queued-but-never-admitted input is the existing
    created-life exception; an admitted one from a distinct process UUID is
    never zero admission, and the settled-work contract refuses it too.
    """
    ids, retirement, _ = await _retired_lite_actor(
        db, monkeypatch, status="active", input_state=input_state
    )
    assert not await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    assert (await db.get_thread(ids["thread"]))[
        "runtime_retirement_local_quiescence"
    ] is None


@pytest.mark.asyncio
async def test_lite_actor_exit_lets_the_durable_retry_finish_the_thread(
    db, monkeypatch
):
    """The sweep's own path — nominate, recover, settle — ends the row."""
    ids, retirement, _ = await _retired_lite_actor(
        db, monkeypatch, input_state="settled"
    )
    await db.execute(
        "UPDATE agents SET status='offline' WHERE id=$1::uuid", ids["agent"]
    )
    candidates = await db.list_retryable_pinned_retirements(grace_seconds=0)
    assert [str(c["id"]) for c in candidates] == [ids["thread"]]
    assert await stale_agent_detector_service.retry_pending_pinned_retirement(
        candidates[0],
        dependencies=controls_composition.stale_agent_detector_dependencies(
            main.app.state.resources
        ),
    )
    for read in (
        ids["route_core_api"].read_namespaced_service,
        ids["route_networking_api"].read_namespaced_ingress,
    ):
        assert read.call_count == 1
        assert read.call_args.kwargs["namespace"] == "agents-a"
        assert read.call_args.kwargs["name"] == f"session-{ids['thread']}"
    ids["route_core_api"].delete_namespaced_service.assert_not_called()
    ids["route_networking_api"].delete_namespaced_ingress.assert_not_called()
    thread = await db.get_thread(ids["thread"])
    assert thread["status"] == "ended"
    assert thread["runtime_retirement_token"] is None
    assert await db.list_retryable_pinned_retirements(grace_seconds=0) == []


def _route_died_with_the_pod(monkeypatch, ids):
    """The captured route is gone; exercise teardown's 404 path via injected APIs."""
    core_api = MagicMock()
    networking_api = MagicMock()
    core_api.read_namespaced_service.side_effect = fixtures._K8sError(404)
    networking_api.read_namespaced_ingress.side_effect = fixtures._K8sError(404)
    ids["route_core_api"] = core_api
    ids["route_networking_api"] = networking_api
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


async def _retired_warm_lite_actor(
    db, monkeypatch, *, input_state="settled", vouched=True
):
    """A lite actor on a warm-pool Pod bound through the real 0200 protocol.

    This is the shape every conference takes on dev: no provision intent, a
    ``warm_binding_protection`` marker backed by a ``bound`` row. ``vouched``
    False re-points the marker at a protection that does not exist before
    Begin captures it, so only the warm authority can refuse the receipt.
    """
    ids = await fixtures._seed_warm_pool_binding(db, bound=False)
    api = fixtures.StatefulPinnedK8sApi()
    fixtures._install_warm_pool_pod(api, ids)
    monkeypatch.setenv("PINNED_LEGACY_AGENT_NAMESPACES", "agents-a")
    provisioner = fixtures._production_warm_provisioner(db, api)
    reservation = await reserve_pinned_warm_agent_binding(
        db,
        agent_provisioner=provisioner,
        persistent_provisioner=None,
        thread_id=ids["thread"],
        agent_id=ids["agent"],
        expected_runtime_generation=ids["runtime_generation"],
    )
    assert reservation.bound
    thread = await db.get_thread(ids["thread"])
    marker = fixtures._json(thread["metadata"])["agent_pod"]
    assert marker["pod_uid"] == ids["pod_uid"]
    assert marker["warm_binding_protection"]
    assert "provision_attempt" not in marker
    ids["attach_token"] = str(thread["runtime_attach_token"])
    ids["generation"] = ids["runtime_generation"]
    ids["process_generation"] = str(uuid4())
    if not vouched:
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL session_replication_role='replica'")
                await conn.execute(
                    "UPDATE threads SET metadata=jsonb_set(metadata,"
                    "'{agent_pod,warm_binding_protection}',to_jsonb($2::text)) "
                    "WHERE id=$1::uuid",
                    ids["thread"],
                    str(uuid4()),
                )

    ids["delivery_id"] = uuid4()
    async with db.acquire() as conn:
        async with conn.transaction():
            authority = dict(
                agent_id=ids["agent"],
                pod_uid=ids["pod_uid"],
                runtime_generation=ids["process_generation"],
                session_runtime_generation=ids["generation"],
                runtime_attach_token=ids["attach_token"],
            )
            delivery = await persist_input_delivery(
                conn,
                thread_id=ids["thread"],
                delivery_id=ids["delivery_id"],
                role="human",
                content="A real admitted conference turn",
                source="direct_human",
                turn_number=1,
                **authority,
            )
            args = dict(
                delivery_id=ids["delivery_id"],
                claim_generation=int(delivery["claim_generation"]),
                **authority,
            )
            assert await mark_input_delivery_queued(conn, **args)
            if input_state in {"admitted", "settled"}:
                assert await transition_input_delivery(
                    conn, transition="admitted", turn_number=1, **args
                )
            if input_state == "settled":
                assert await transition_input_delivery(
                    conn, transition="settled", **args
                )

    api.mark_terminal("agents-a", ids["pod_name"])
    pod = api.pods[("agents-a", ids["pod_name"])]
    pod.spec = NS(
        containers=[NS(name="agent")],
        init_containers=[],
        ephemeral_containers=[],
        volumes=[],
    )
    pod.status.container_statuses[0].name = "agent"
    pod.metadata.deletion_timestamp = "now"
    monkeypatch.setattr(agent_provisioner_module, "agent_provisioner", provisioner)
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    _route_died_with_the_pod(monkeypatch, ids)

    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=False)
    assert retirement["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=ids["generation"],
        settle_status="ended",
    )
    thread = await db.get_thread(ids["thread"])
    assert thread["runtime_retirement_local_quiescence"] is None
    context = fixtures._json(thread["runtime_retirement_context"])
    assert context["workspace_backend"] == "none"
    assert context["agent_pod"] == fixtures._json(thread["metadata"])["agent_pod"]
    return ids, retirement, api


@pytest.mark.asyncio
async def test_used_warm_actor_exit_settles_after_exact_pod_stop(db, monkeypatch):
    ids, retirement, api = await _retired_warm_lite_actor(db, monkeypatch)
    assert await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    assert ("agents-a", ids["pod_name"]) not in api.pods
    thread = await db.get_thread(ids["thread"])
    receipt = _receipt(thread)
    assert receipt["recovery_protocol"] == "settled_lite_actor_exit_v1"
    assert receipt["quiescence_protocol"] == "agent_runtime_zero_v1"
    assert receipt["agent_pod_uid"] == ids["pod_uid"]
    assert receipt["settled_input_count"] == 1
    assert controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).retirement_has_exact_local_quiescence(retirement, thread)
    assert await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)


@pytest.mark.asyncio
async def test_warm_actor_exit_lets_the_durable_retry_finish_the_thread(
    db, monkeypatch
):
    ids, _, _ = await _retired_warm_lite_actor(db, monkeypatch)
    await db.execute(
        "UPDATE agents SET status='offline' WHERE id=$1::uuid", ids["agent"]
    )
    candidates = await db.list_retryable_pinned_retirements(grace_seconds=0)
    assert [str(c["id"]) for c in candidates] == [ids["thread"]]
    assert await stale_agent_detector_service.retry_pending_pinned_retirement(
        candidates[0],
        dependencies=controls_composition.stale_agent_detector_dependencies(
            main.app.state.resources
        ),
    )
    thread = await db.get_thread(ids["thread"])
    assert thread["status"] == "ended"
    assert thread["runtime_retirement_token"] is None


@pytest.mark.asyncio
async def test_warm_actor_exit_requires_the_bound_protection(db, monkeypatch):
    """A warm marker nobody vouches for is not an exact Pod authority."""
    ids, retirement, _ = await _retired_warm_lite_actor(db, monkeypatch, vouched=False)
    assert not await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    assert (await db.get_thread(ids["thread"]))[
        "runtime_retirement_local_quiescence"
    ] is None
