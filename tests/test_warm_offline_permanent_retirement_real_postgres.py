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


@pytest.mark.asyncio
@pytest.mark.parametrize("local_receipt", [False, True])
async def test_offline_warm_status_never_supplies_missing_physical_proof(
    db, monkeypatch, local_receipt
):
    from tests import test_pinned_permanent_warm_release_real_postgres as warm

    life, _, _ = await warm._bound_warm_thread(db, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(life["thread"], permanent=True)
    if local_receipt:
        await warm.fixtures._authorize_and_ack(db, life, retirement)
    else:
        assert await db.authorize_pinned_thread_retirement(
            life["thread"],
            token=retirement["token"],
            generation=retirement["generation"],
            settle_status="ended",
        )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE agents SET last_heartbeat=now()-interval '1 day' WHERE id=$1::uuid",
            UUID(life["agent"]),
        )
    assert await db.mark_stale_agents_offline(timeout_minutes=3)
    with pytest.raises(
        RuntimeError, match="permanent pinned delete lacks physical quiescence"
    ):
        await db.delete_thread(
            life["thread"],
            expected_runtime_retirement_token=retirement["token"],
            expected_runtime_generation=retirement["generation"],
        )
    assert await db.get_thread(life["thread"]) is not None
