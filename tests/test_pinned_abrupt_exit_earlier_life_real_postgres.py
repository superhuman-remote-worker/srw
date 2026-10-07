"""A second abrupt death after Resume still receipts despite the first's leftovers.

An abrupt exit leaves its admitted partial turn ``admitted`` for good (0313:
admitted partial turns are immutable receipts and are never replayed) and
its queued/owned input owned by the dead actor until a successor reclaims it.
Before 0335 ``acknowledge_abrupt_pinned_actor_exit`` refused on any such
non-terminal delivery owned by another agent or Pod, so a session killed
mid-turn, resumed and killed again could never retire (pinned-lane plan P0,
vault ``features/parallel_subagents.md`` §14.2).

The relaxed predicate accepts a leftover only when its owner is a different
agent that was the receipted actor of an earlier, settled life of this same
thread, and claimed the input before that life settled. Anything else - an
owner with no settled life, a settled life of another thread or of the
current generation, the same agent on another Pod, a claim after the earlier
settlement, or a stateless claim - keeps refusing.
"""

from types import SimpleNamespace as NS
from uuid import UUID, uuid4

import pytest

from orchestrator import main
from orchestrator.application import controls
from shared.persistent_input_delivery import (
    claim_pending_input_deliveries,
    mark_input_delivery_queued,
    persist_input_delivery,
    transition_input_delivery,
)
from tests import test_persistent_recycler_real_postgres as fixtures
from tests.test_pinned_abrupt_death_real_postgres import killed_life
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent

pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied
db = fixtures.db


def _operations():
    return controls.pinned_retirement_operations(main.app.state.resources)


async def _end_first_life(db, ids, retirement):
    assert await _operations().recover_captured_process_zero(retirement)
    current = await db.get_thread(ids["thread"])
    result = await controls.thread_retirement_operations(
        main.app.state.resources
    ).end_thread_flow(
        ids["thread"],
        current,
        permanent=False,
        force=True,
        expected_runtime_generation=retirement["generation"],
        expected_agent_id=ids["agent"],
        expected_attach_token=ids["attach_token"],
        local_runtime_quiesced=True,
    )
    assert result["status"] == "ended"


async def _second_life(db, ids):
    """Resume and bind a fresh dedicated actor (a new agent row and Pod UID)."""

    assert await db.resume_thread(ids["thread"])
    successor = await _bind_cold_agent(db, ids["thread"])
    actor = await db.get_agent(str(successor["agent_id"]))
    marker = fixtures._json(successor["metadata"])["agent_pod"]
    return {
        "thread": ids["thread"],
        "agent": str(successor["agent_id"]),
        "pod_uid": str(actor["pod_uid"]),
        "pod_name": str(marker["pod_name"]),
        "generation": str(successor["runtime_generation"]),
        "attach_token": str(successor["runtime_attach_token"]),
        "process_generation": str(uuid4()),
        "provision_attempt": str(marker["provision_attempt"]),
    }


def _authority(life, delivery_id, claim_generation):
    return dict(
        delivery_id=delivery_id,
        agent_id=life["agent"],
        pod_uid=life["pod_uid"],
        runtime_generation=life["process_generation"],
        session_runtime_generation=life["generation"],
        runtime_attach_token=life["attach_token"],
        claim_generation=int(claim_generation),
    )


async def _serve_stranded_input(db, life):
    """The successor claims, admits and settles every pending input once."""

    async with db.acquire() as conn:
        async with conn.transaction():
            recovered = await claim_pending_input_deliveries(
                conn,
                thread_id=life["thread"],
                agent_id=life["agent"],
                pod_uid=life["pod_uid"],
                runtime_generation=life["process_generation"],
                session_runtime_generation=life["generation"],
                runtime_attach_token=life["attach_token"],
            )
            for row in recovered:
                authority = _authority(
                    life, row["delivery_id"], row["claim_generation"]
                )
                assert await mark_input_delivery_queued(conn, **authority)
                assert await transition_input_delivery(
                    conn, **authority, transition="admitted", turn_number=2
                )
                assert await transition_input_delivery(
                    conn, **authority, transition="settled", turn_number=2
                )
    return [row["delivery_id"] for row in recovered]


async def _new_input(db, life, *, state):
    delivery_id = uuid4()
    async with db.acquire() as conn:
        async with conn.transaction():
            row = await persist_input_delivery(
                conn,
                thread_id=life["thread"],
                delivery_id=delivery_id,
                role="human",
                content=f"second life input {state}",
                source="direct_human",
                turn_number=2,
                agent_id=life["agent"],
                pod_uid=life["pod_uid"],
                runtime_generation=life["process_generation"],
                session_runtime_generation=life["generation"],
                runtime_attach_token=life["attach_token"],
            )
            authority = _authority(life, delivery_id, row["claim_generation"])
            assert await mark_input_delivery_queued(conn, **authority)
            assert await transition_input_delivery(
                conn, **authority, transition="admitted", turn_number=2
            )
            if state == "settled":
                assert await transition_input_delivery(
                    conn, **authority, transition="settled", turn_number=2
                )
    return delivery_id


async def _kill(db, api, life):
    """SIGKILL the second life's exact Pod after Begin + authorize."""

    retirement = await db.begin_pinned_thread_retirement(
        life["thread"], permanent=False
    )
    assert retirement["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        life["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    api.install_old_pod(
        namespace="agents-a",
        name=life["pod_name"],
        uid=life["pod_uid"],
        labels={
            "srw/thread-id": life["thread"],
            "srw.io/runtime-generation": life["generation"],
            "srw.io/provision-attempt": life["provision_attempt"],
            "srw/component": "persistent-agent",
        },
    )
    api.mark_terminal("agents-a", life["pod_name"])
    pod = api.pods[("agents-a", life["pod_name"])]
    pod.spec = NS(
        containers=[NS(name="agent")],
        init_containers=[],
        ephemeral_containers=[],
        volumes=[],
        restart_policy="Never",
    )
    pod.status.phase = "Failed"
    pod.status.container_statuses[0].name = "agent"
    pod.status.container_statuses[0].state.terminated.exit_code = 137
    pod.status.container_statuses[0].state.terminated.signal = 9
    return retirement


def _receipt_args(life, retirement):
    return dict(
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
        agent_id=life["agent"],
        attach_token=life["attach_token"],
        stopped_pod_uid=life["pod_uid"],
    )


async def _states(db, delivery_ids):
    rows = await db.fetch(
        "SELECT delivery_id,state,owner_agent_id,settled_at "
        "FROM thread_input_deliveries WHERE delivery_id=ANY($1::uuid[])",
        delivery_ids,
    )
    by_id = {row["delivery_id"]: row for row in rows}
    return [dict(by_id[delivery_id]) for delivery_id in delivery_ids]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend,binding", [("none", True), ("virtual", False)], ids=["lite", "virtual"]
)
@pytest.mark.parametrize(
    "successor_served", [True, False], ids=["served", "killed_before_serving"]
)
async def test_second_abrupt_death_receipts_with_the_first_lifes_leftovers(
    db, monkeypatch, backend, binding, successor_served
):
    ids, retirement, deliveries, api, _ = await killed_life(
        db, monkeypatch, backend=backend, partial=True, with_virtual_binding=binding
    )
    first_agent = UUID(ids["agent"])
    await _end_first_life(db, ids, retirement)
    life = await _second_life(db, ids)
    assert life["agent"] != ids["agent"] and life["pod_uid"] != ids["pod_uid"]
    if successor_served:
        assert await _serve_stranded_input(db, life) == deliveries[1:]
        own = [await _new_input(db, life, state="admitted")]
    else:
        # Killed before its claim reached the first life's stranded input.
        own = [await _new_input(db, life, state="settled")]
    leftovers = await _states(db, deliveries)
    assert leftovers[0]["state"] == "admitted"
    assert leftovers[0]["owner_agent_id"] == first_agent
    retirement2 = await _kill(db, api, life)

    receipt = await db.acknowledge_abrupt_pinned_actor_exit(
        life["thread"], **_receipt_args(life, retirement2)
    )
    assert receipt is not None, "a dead earlier life's leftovers must not block"
    # This life's receipt counts only this life's input.
    assert receipt["captured_input_count"] == (3 if successor_served else 1)
    assert receipt["partial_admission_count"] == int(successor_served)
    assert receipt["stranded_input_count"] == 0
    assert await _operations().recover_captured_process_zero(retirement2)
    # Nothing is mutated: the first life's partial turn stays immutable
    # evidence, and input it never served stays owed to the next successor.
    assert await _states(db, deliveries) == leftovers
    assert [row["state"] for row in await _states(db, own)] == [
        "admitted" if successor_served else "settled"
    ]
    if not successor_served:
        assert [row["state"] for row in leftovers] == ["admitted", "queued", "owned"]
        assert {row["owner_agent_id"] for row in leftovers} == {first_agent}


async def _replica(db, sql, *args):
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(sql, *args)


async def _foreign_delivery(db, ids, *, owner_agent, owner_pod, state, lane, owned_at):
    """One non-terminal delivery claimed by another runtime (seeded pre-Begin)."""

    message = uuid4()
    await _replica(
        db,
        "INSERT INTO thread_messages (id,thread_id,role,content,turn_number) "
        "VALUES ($1,$2::uuid,'human','claimed by another runtime',1)",
        message,
        ids["thread"],
    )
    admitted = state == "admitted"
    if lane == "pinned":
        await _replica(
            db,
            "INSERT INTO thread_input_deliveries (delivery_id,thread_id,message_id,"
            "source,execution_lane,state,claim_generation,owner_agent_id,"
            "owner_pod_uid,owner_runtime_generation,owned_at,admitted_at,"
            "admitted_turn_number) VALUES ($1,$2::uuid,$3,'direct_human','pinned',"
            "$4,1,$5,$6,$7,$8::timestamptz,CASE WHEN $9 THEN $8::timestamptz END,CASE WHEN $9 THEN 1 END)",
            uuid4(),
            ids["thread"],
            message,
            state,
            owner_agent,
            owner_pod,
            uuid4(),
            owned_at,
            admitted,
        )
    else:
        await _replica(
            db,
            "INSERT INTO thread_input_deliveries (delivery_id,thread_id,message_id,"
            "source,execution_lane,state,claim_generation,owner_run_queue_lease_token,"
            "owner_executor,owner_executor_pod_uid,owned_at,admitted_at,"
            "admitted_turn_number) VALUES ($1,$2::uuid,$3,'direct_human','stateless',"
            "$4,1,7,'executor-a','executor-pod',$5::timestamptz,CASE WHEN $6 THEN $5::timestamptz END,"
            "CASE WHEN $6 THEN 1 END)",
            uuid4(),
            ids["thread"],
            message,
            state,
            owned_at,
            admitted,
        )


async def _earlier_outcome(db, *, thread, generation, agent, settled_at):
    """An append-only settled retirement outcome, as an earlier End wrote it."""

    await _replica(
        db,
        "INSERT INTO thread_runtime_retirement_outcomes (thread_id,"
        "runtime_generation,retirement_token,agent_id,runtime_attach_token,"
        "disposition,permanent,outcome,settled_at) VALUES ($1::uuid,$2::uuid,$3,"
        "$4,$5,'ended',false,'settled',$6)",
        thread,
        generation,
        uuid4(),
        agent,
        uuid4(),
        settled_at,
    )


async def _other_thread(db, ids):
    other = uuid4()
    await _replica(
        db,
        "INSERT INTO threads (id,user_id,status,execution_lane,metadata) "
        "VALUES ($1,$2::uuid,'ended','pinned','{}'::jsonb)",
        other,
        ids["user"],
    )
    return str(other)


LEFTOVERS = {
    # The predicate's positive case, seeded the same way as every refusal
    # below so that each refusal is attributable to its one difference.
    "earlier_life_admitted": True,
    "earlier_life_queued": True,
    "earlier_life_owned": True,
    # An owner with no receipted life: e.g. the actor this life replaced
    # after it went offline, never proven stopped.
    "unreceipted_owner": False,
    # The owner's settled life belongs to another thread (a pooled agent).
    "outcome_of_another_thread": False,
    # The settled outcome names a different agent than the owner.
    "outcome_of_another_agent": False,
    # The owner's outcome is for the generation being retired.
    "outcome_of_current_generation": False,
    # The owner claimed the input after its earlier life had settled: the
    # claim belongs to a later life that was never receipted.
    "claimed_after_settlement": False,
    # The same agent row on another Pod: never accepted as an earlier life.
    "same_agent_other_pod": False,
    # A stateless executor's claim is never this pinned life's leftover.
    "stateless_claim": False,
}


@pytest.mark.asyncio
@pytest.mark.parametrize("case", sorted(LEFTOVERS))
async def test_abrupt_receipt_accepts_only_a_receipted_earlier_lifes_leftover(
    db, monkeypatch, case
):
    async def before_retirement(ids, deliveries):
        now = await db.fetchval("SELECT statement_timestamp()")
        owner = ids["agent"] if case == "same_agent_other_pod" else str(uuid4())
        claimed_at = now
        settled_at = now
        if case == "claimed_after_settlement":
            settled_at = await db.fetchval(
                "SELECT $1::timestamptz - interval '1 hour'", now
            )
        outcome_thread = ids["thread"]
        if case == "outcome_of_another_thread":
            outcome_thread = await _other_thread(db, ids)
        outcome_generation = (
            ids["generation"] if case == "outcome_of_current_generation" else uuid4()
        )
        outcome_agent = str(uuid4()) if case == "outcome_of_another_agent" else owner
        if case != "unreceipted_owner":
            await _earlier_outcome(
                db,
                thread=outcome_thread,
                generation=outcome_generation,
                agent=outcome_agent,
                settled_at=settled_at,
            )
        state = {"earlier_life_queued": "queued", "earlier_life_owned": "owned"}.get(
            case, "admitted"
        )
        await _foreign_delivery(
            db,
            ids,
            owner_agent=UUID(owner),
            owner_pod=str(uuid4()),
            state=state,
            lane="stateless" if case == "stateless_claim" else "pinned",
            owned_at=claimed_at,
        )

    ids, retirement, _, _, _ = await killed_life(
        db, monkeypatch, backend="none", before_retirement=before_retirement
    )
    receipt = await db.acknowledge_abrupt_pinned_actor_exit(
        ids["thread"],
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
        agent_id=ids["agent"],
        attach_token=ids["attach_token"],
        stopped_pod_uid=ids["pod_uid"],
    )
    assert (receipt is not None) is LEFTOVERS[case], receipt
    assert (
        await _operations().recover_captured_process_zero(retirement)
        is (LEFTOVERS[case])
    )
    if receipt is not None:
        # The leftover is not this life's input and is not counted as such.
        assert receipt["captured_input_count"] == 2
        assert receipt["stranded_input_count"] == 2
