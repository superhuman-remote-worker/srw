"""A zero-floor upgrade must not refill the just-drained durable warm pool."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from orchestrator.services.agent_provisioner import AgentProvisioner
from tests import test_persistent_recycler_real_postgres as fixtures
from tests.test_agent_pool_settings_rollout_helm import POOL_FIELDS, render_pool

pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied
db = fixtures.db


@pytest.mark.asyncio
async def test_zero_floor_rollout_prevents_recreation_after_real_pool_drain(
    db, monkeypatch
):
    before, old_deployment = render_pool()
    after, new_deployment = render_pool(changed="minAgents")
    for environment_key, _, _ in POOL_FIELDS.values():
        monkeypatch.setenv(environment_key, before["data"][environment_key])

    created = []

    async def create_and_register_pod(*, purpose):
        # The Kubernetes transport and first registration are substituted;
        # pool decisions, startup snapshots and idle-agent queries are real.
        assert purpose == "job"
        actor_id = uuid4()
        name = "srw-agent-pool-rollout-" + actor_id.hex[:8]
        async with db.acquire() as conn:
            await conn.execute(
                "INSERT INTO agents (id,config_name,hostname,pod_uid,status,agent_mode) "
                "VALUES ($1,'persistent_defaults',$2,$3,'ready','dual')",
                actor_id,
                name,
                str(uuid4()),
            )
        created.append(actor_id)
        return name

    def start_process():
        owner = AgentProvisioner()
        owner._db = db
        owner._k8s_available = True
        owner._core_api = SimpleNamespace(
            list_namespaced_pod=lambda **kwargs: SimpleNamespace(items=[])
        )
        owner.provision_agent = create_and_register_pod
        return owner

    process = start_process()
    assert await process.ensure_warm_pool() == 1
    assert await process._count_idle_agents() == 1
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM agents WHERE id=$1", created.pop())
    assert await process._count_idle_agents() == 0

    for environment_key, _, _ in POOL_FIELDS.values():
        monkeypatch.setenv(environment_key, after["data"][environment_key])
    # Kubernetes only replaces the process when the rendered Pod template
    # changes. Updating ConfigMap-backed environment does not mutate a process.
    if old_deployment["spec"]["template"] != new_deployment["spec"]["template"]:
        process = start_process()
    recreated = await process.ensure_warm_pool()
    async with db.acquire() as conn:
        remaining = await conn.fetchval(
            "SELECT count(*) FROM agents WHERE hostname LIKE 'srw-agent-pool-rollout-%'"
        )
    assert (recreated, remaining) == (0, 0), (
        "zero-floor ConfigMap upgrade left the old process recreating durable actors"
    )
