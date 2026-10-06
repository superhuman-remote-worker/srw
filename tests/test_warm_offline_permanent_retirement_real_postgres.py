"""A stale warm actor remains exact retirement authority, never a pool slot."""

from uuid import UUID
import pytest
from tests import test_self_ended_pinned_retirement_real_postgres as flow

pg_dsn = flow.pg_dsn
_schema_applied = flow._schema_applied
db = flow.db


@pytest.mark.asyncio
async def test_exact_offline_warm_actor_permanent_retirement_settles(db, monkeypatch):
    life, api, provisioner, stack = await flow._bind_warm_life(db, monkeypatch)
    handoff = await flow._owner_permanent_then_agent_ack(stack, life)
    assert handoff["retiring_agent_exit_authorized"] is True
    api.mark_terminal("agents-a", life["pod_name"])
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE agents SET last_heartbeat=now()-interval '1 day' WHERE id=$1::uuid",
            UUID(life["agent"]),
        )
    assert await db.mark_stale_agents_offline(timeout_minutes=3)
    actor = await db.fetchrow(
        "SELECT status,thread_id FROM agents WHERE id=$1::uuid", life["agent"]
    )
    assert actor["status"] == "offline" and str(actor["thread_id"]) == life["thread"]
    result = await flow._durable_retry(stack, life)
    assert result["status"] == "deleted"
    assert await db.get_thread(life["thread"]) is None
    await flow._reconcile_expired_warm_release(db, life, provisioner)
    await flow._assert_warm_ledger_settled(db, life, outcome="exact_absent_v1")
    actor = await db.fetchrow(
        "SELECT status,thread_id FROM agents WHERE id=$1::uuid", life["agent"]
    )
    assert actor is None or (actor["status"], actor["thread_id"]) == ("offline", None)
