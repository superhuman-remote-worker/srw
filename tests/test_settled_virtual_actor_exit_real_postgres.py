"""Used pinned virtual actor crash recovery with actual retirement/admission SQL."""

from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import asyncpg
import pytest

from orchestrator import main
from orchestrator.services.agent_provisioner import AgentProvisioner
from orchestrator.services.session_router import SessionRouterService
from shared.persistent_input_delivery import (
    InputDeliveryAuthorityLost,
    lock_runtime_authority,
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


async def _used_virtual(
    db,
    monkeypatch,
    *,
    input_state="settled",
    status="active",
    permanent=False,
    workspace_claim=False,
):
    ids = await fixtures._seed(
        db, protected_agent_pod=True, workspace_claim=workspace_claim
    )
    ids.pop("old_access")
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{config_override,workspace,backend}',"
        "'\"virtual\"'::jsonb),status=$2 WHERE id=$1::uuid",
        ids["thread"],
        status,
    )
    assert await db.bind_thread_workspace_backing(
        ids["thread"],
        backing_kind="virtual",
        backing_id=f"rclone:{'a' * 64}",
    )
    generation = str((await db.get_thread(ids["thread"]))["runtime_generation"])
    ids["process_generation"] = str(uuid4())
    ids["pod_uid"] = "old-pod"
    ids["generation"] = generation
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
                content="A real admitted virtual turn",
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
    assert (await db.get_thread(ids["thread"]))[
        "runtime_retirement_local_quiescence"
    ] is None
    return ids, retirement, k8s


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
async def test_used_virtual_actor_exit_settles_after_exact_pod_stop(
    db, monkeypatch, permanent
):
    ids, retirement, k8s = await _used_virtual(db, monkeypatch, permanent=permanent)
    assert ids["process_generation"] != ids["generation"]
    assert await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    assert not k8s.pods
    thread = await db.get_thread(ids["thread"])
    receipt = fixtures._json(thread["runtime_retirement_local_quiescence"])
    assert receipt["recovery_protocol"] == "settled_virtual_actor_exit_v1"
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
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE threads SET runtime_retirement_local_quiescence=NULL WHERE id=$1::uuid",
            ids["thread"],
        )
    # End's existing admission fence remains closed after recovery. This
    # receipt cannot grant the stopped actor another provider/tool boundary.
    async with db.acquire() as conn:
        async with conn.transaction():
            with pytest.raises(InputDeliveryAuthorityLost):
                await lock_runtime_authority(
                    conn,
                    thread_id=ids["thread"],
                    agent_id=ids["agent"],
                    pod_uid=ids["pod_uid"],
                    session_runtime_generation=ids["generation"],
                    runtime_attach_token=ids["attach_token"],
                )
    if not permanent:
        assert await db.settle_pinned_thread_retirement(
            ids["thread"],
            token=retirement["token"],
            generation=retirement["generation"],
            final_status="ended",
        )
        assert await db.resume_thread(ids["thread"])
        assert (
            str((await db.get_thread(ids["thread"]))["runtime_generation"])
            != ids["generation"]
        )
        assert (
            await db.acknowledge_settled_virtual_actor_exit(
                ids["thread"],
                runtime_generation=ids["generation"],
                retirement_token=retirement["token"],
                agent_id=ids["agent"],
                attach_token=ids["attach_token"],
                stopped_pod_uid=ids["pod_uid"],
            )
            is None
        )
        retirement = await db.begin_pinned_thread_retirement(
            ids["thread"], permanent=True
        )
        assert await db.authorize_pinned_thread_retirement(
            ids["thread"],
            token=retirement["token"],
            generation=retirement["generation"],
            settle_status="ended",
        )
    from orchestrator.services import thread_uploads

    purge = AsyncMock(return_value=True)
    monkeypatch.setattr(
        thread_uploads, "purge_attested_pinned_virtual_workspace", purge
    )
    # The captured route has disappeared with the stopped Pod. Exercise the
    # real teardown's 404 handling using injected APIs, never a local cluster.
    core_api = MagicMock()
    networking_api = MagicMock()
    core_api.read_namespaced_service.side_effect = fixtures._K8sError(404)
    networking_api.read_namespaced_ingress.side_effect = fixtures._K8sError(404)
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
    await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).cleanup_pinned_thread_retirement(retirement)
    for read in (
        core_api.read_namespaced_service,
        networking_api.read_namespaced_ingress,
    ):
        if permanent:
            assert read.call_count == 1
            assert read.call_args.kwargs["namespace"] == "agents-a"
            assert read.call_args.kwargs["name"] == f"session-{ids['thread']}"
        else:
            read.assert_not_called()
    core_api.delete_namespaced_service.assert_not_called()
    networking_api.delete_namespaced_ingress.assert_not_called()
    await db.delete_thread(
        ids["thread"],
        expected_runtime_retirement_token=retirement["token"],
        expected_runtime_generation=retirement["generation"],
    )
    assert await db.get_thread(ids["thread"]) is None
    purge.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("input_state", ["queued", "admitted"])
@pytest.mark.parametrize("status", ["created", "active"])
async def test_process_generation_difference_never_turns_used_life_into_zero_admission(
    db, monkeypatch, input_state, status
):
    ids, retirement, _ = await _used_virtual(
        db, monkeypatch, input_state=input_state, status=status
    )
    recovered = await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    # Queued work was never admitted; the existing created-life protocol may
    # still settle it. An admitted input from a distinct process UUID cannot.
    if status == "created" and input_state == "queued":
        assert recovered
    else:
        assert not recovered
        assert (await db.get_thread(ids["thread"]))[
            "runtime_retirement_local_quiescence"
        ] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "generation",
        "token",
        "agent",
        "attach",
        "pod",
        "child",
        "pending_effect",
        "dead_effect",
        "pending_permission",
        "missing_intent",
        "external_claim",
    ],
)
async def test_settled_virtual_exit_refuses_other_authority_or_work(
    db, monkeypatch, fault
):
    ids, retirement, _ = await _used_virtual(
        db, monkeypatch, workspace_claim=fault == "external_claim"
    )
    args = dict(
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
        agent_id=ids["agent"],
        attach_token=ids["attach_token"],
        stopped_pod_uid=ids["pod_uid"],
    )
    field = {
        "generation": "runtime_generation",
        "token": "retirement_token",
        "agent": "agent_id",
        "attach": "attach_token",
        "pod": "stopped_pod_uid",
    }.get(fault)
    if field:
        args[field] = str(uuid4())
    elif fault == "child":
        # Seed pre-retirement child state without claiming an exited actor can
        # create another child after Begin. The recovery gate must refuse it.
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL session_replication_role='replica'")
                await conn.execute(
                    "INSERT INTO threads (id,user_id,kind,parent_thread_id,status,execution_lane,"
                    "subagent_status) VALUES ($1::uuid,$2::uuid,'subagent',$3::uuid,'active','pinned','running')",
                    uuid4(),
                    ids["user"],
                    ids["thread"],
                )
    elif fault in {"pending_effect", "dead_effect"}:
        await db.execute(
            "INSERT INTO completion_effects (producer_kind,producer_id,scope_id,effect_name,effect_group,state) "
            "VALUES ('session_turn',$1::uuid,$2::uuid,'memory','optional',$3)",
            uuid4(),
            ids["thread"],
            "pending" if fault == "pending_effect" else "dead",
        )
    elif fault == "pending_permission":
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL session_replication_role='replica'")
                await conn.execute(
                    "INSERT INTO thread_permission_requests (thread_id,tool_call_id,tool_name) "
                    "VALUES ($1::uuid,'pending-tool','run_command')",
                    ids["thread"],
                )
    elif fault == "missing_intent":
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL session_replication_role='replica'")
                await conn.execute(
                    "DELETE FROM thread_agent_pod_provision_intents WHERE attempt_id=$1::uuid",
                    ids["provision_attempt"],
                )
    assert (
        await db.acknowledge_settled_virtual_actor_exit(ids["thread"], **args) is None
    )
    assert (await db.get_thread(ids["thread"]))[
        "runtime_retirement_local_quiescence"
    ] is None
