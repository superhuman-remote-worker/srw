"""A released pre-setup life still owes exact terminal Pod finalizer cleanup."""

import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator import main
from orchestrator.application import controls as controls_composition
from orchestrator.services import agent_provisioner as provisioner_module
from orchestrator.services.agent_provisioner import AgentProvisioner
from orchestrator.services.session_attach_binding import release_session_attach_binding
from tests import test_persistent_recycler_real_postgres as authority
from tests.test_historical_agent_pod_cleanup_real_postgres import ClaimantK8sApi

db = authority.db
pg_dsn = authority.pg_dsn
_schema_applied = authority._schema_applied


async def _aborted_life(db, monkeypatch):
    ids = await authority._seed(db, bind_agent=False, publish_agent_pod=False)
    await db.execute(
        "DELETE FROM project_officers WHERE thread_id=$1::uuid", ids["thread"]
    )
    await db.execute(
        "UPDATE threads SET status='created',config_name='session_base',metadata=$2::jsonb WHERE id=$1::uuid",
        ids["thread"],
        json.dumps(
            {
                "config_override": {
                    "workspace": {"backend": "none"},
                    "officer": {"enabled": False},
                }
            }
        ),
    )
    generation = str((await db.get_thread(ids["thread"]))["runtime_generation"])
    old = dict(
        thread=ids["thread"],
        generation=generation,
        attempt=str(uuid4()),
        pod_name="srw-agent-s-" + uuid4().hex[:8],
        pod_uid=str(uuid4()),
        namespace="agents-a",
        agent=ids["agent"],
    )
    assert await db.reserve_pinned_agent_pod_provision_intent(
        ids["thread"],
        expected_runtime_generation=generation,
        attempt_id=old["attempt"],
        pod_name=old["pod_name"],
        provisioner="agent",
        namespace=old["namespace"],
    )
    assert await db.publish_pinned_agent_pod_provision_intent(
        ids["thread"],
        expected_runtime_generation=generation,
        attempt_id=old["attempt"],
        pod_name=old["pod_name"],
        pod_uid=old["pod_uid"],
        namespace=old["namespace"],
    )
    await db.execute(
        "INSERT INTO agents(id,config_name,hostname,pod_uid,status,agent_mode) VALUES($1::uuid,'session_base',$2,$3,'session','persistent')",
        ids["agent"],
        old["pod_name"],
        old["pod_uid"],
    )
    async with db.acquire() as conn, conn.transaction():
        await conn.execute(
            "UPDATE threads SET agent_id=$2::uuid,control_admission_agent_id=$2::uuid,runtime_attach_token=$3::uuid WHERE id=$1::uuid",
            ids["thread"],
            ids["agent"],
            ids["attach_token"],
        )
        await conn.execute(
            "UPDATE agents SET thread_id=$2::uuid WHERE id=$1::uuid",
            ids["agent"],
            ids["thread"],
        )
    assert (
        await release_session_attach_binding(
            ids["agent"],
            ids["thread"],
            expected_runtime_generation=generation,
            expected_attach_token=ids["attach_token"],
            expected_agent_pod_uid=old["pod_uid"],
            local_runtime_quiesced=True,
            local_quiescence_protocol="agent_attach_not_started_v1",
            dependencies=NS(store=db),
        )
        == "released"
    )
    assert (
        await db.fetchval(
            "SELECT status::text FROM agents WHERE id=$1::uuid", ids["agent"]
        )
        == "ready"
    )
    # Normal deregistration follows its confirmed release; no forged offline proof.
    await db.delete_agent(ids["agent"])
    current_agent, _ = await authority._bind_replacement_agent(
        db,
        thread_id=ids["thread"],
        pod_uid=str(uuid4()),
        pod_name="successor-" + uuid4().hex[:8],
    )
    ids["agent"] = current_agent
    ids["attach_token"] = str(
        (await db.get_thread(ids["thread"]))["runtime_attach_token"]
    )
    k8s = ClaimantK8sApi()
    k8s.install_old_pod(
        namespace=old["namespace"],
        name=old["pod_name"],
        uid=old["pod_uid"],
        labels={
            "srw/managed-by": "agent-provisioner",
            "srw/purpose": "session",
            "srw.io/thread-id": old["thread"],
            "srw.io/runtime-generation": old["generation"],
            "srw.io/provision-attempt": old["attempt"],
        },
    )
    pod = k8s.pods[(old["namespace"], old["pod_name"])]
    pod.metadata.name, pod.metadata.namespace = old["pod_name"], old["namespace"]
    pod.spec = NS(
        containers=[NS(name="agent")],
        init_containers=[],
        ephemeral_containers=[],
        volumes=[],
        restart_policy="Never",
    )
    k8s.mark_terminal(old["namespace"], old["pod_name"])
    k8s.list_namespaced_pod = lambda **kwargs: NS(items=list(k8s.pods.values()))
    provider = AgentProvisioner()
    provider._k8s_available, provider._core_api, provider._db, provider._namespace = (
        True,
        k8s,
        db,
        old["namespace"],
    )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(provisioner_module, "agent_provisioner", provider)
    monkeypatch.setattr(
        main.app.state.resources.session_router,
        "teardown_route",
        AsyncMock(return_value=True),
    )
    soft = await db.begin_pinned_thread_retirement(ids["thread"], permanent=False)
    await authority._authorize_and_ack(db, ids, soft)
    ops = controls_composition.pinned_retirement_operations(main.app.state.resources)
    await ops.cleanup_pinned_thread_retirement(soft, cleanup_agent_pod=False)
    assert await db.settle_pinned_thread_retirement(
        ids["thread"],
        token=soft["token"],
        generation=soft["generation"],
        final_status="ended",
    )
    permanent = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    assert permanent["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=permanent["token"],
        generation=permanent["generation"],
        settle_status="ended",
    )
    return old, k8s, provider, ops, permanent


@pytest.mark.asyncio
async def test_permanent_cleanup_removes_exact_pre_setup_aborted_pod_before_thread_delete(
    db, monkeypatch
):
    old, k8s, _, ops, permanent = await _aborted_life(db, monkeypatch)
    await ops.cleanup_pinned_thread_retirement(permanent, cleanup_agent_pod=False)
    assert (old["namespace"], old["pod_name"]) not in k8s.pods
    assert await db.get_thread(old["thread"]) is not None


@pytest.mark.asyncio
async def test_normal_reaper_recovers_pre_setup_aborted_pod_after_thread_was_deleted(
    db, monkeypatch
):
    old, k8s, provider, ops, permanent = await _aborted_life(db, monkeypatch)
    # Reproduce a previous version's omitted historical Pod before upgrading.
    monkeypatch.setattr(
        type(ops), "_retire_historical_unclaimed_agent_pods_for_retirement", AsyncMock()
    )
    await ops.cleanup_pinned_thread_retirement(permanent, cleanup_agent_pod=False)
    await db.delete_thread(
        old["thread"],
        expected_runtime_retirement_token=permanent["token"],
        expected_runtime_generation=permanent["generation"],
    )
    assert await db.get_thread(old["thread"]) is None
    assert (old["namespace"], old["pod_name"]) in k8s.pods
    await provider.reap_pods()
    assert (old["namespace"], old["pod_name"]) not in k8s.pods
