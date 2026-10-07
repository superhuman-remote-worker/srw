"""A pinned event admitted by a runtime that died is served again, boundedly.

Design: knowledge-base/knowledge/features/parallel_subagents.md §14.1 ("an
admitted but unsettled delivery is never served again", "a bound on repeated
recovery") and §14.2 P3; the stateless twin is K4
(``test_stateless_stale_event_admission_pg``).

The pinned claim took only unadmitted rows, so an event (a session or officer
wake, a job event, a subagent continuation) whose runtime died after provider
admission stayed ``admitted`` and no successor served it. Attach now runs
``reserve_stale_pinned_admissions`` between the subagent recovery and restore:
an answered admission is settled, an owed one handed back to the current
runtime. Each provider admission counts, summed along the
``supersedes_input_seq`` chain; at five the input is parked with one notice
until its owner retries.

Everything below runs the real SQL on a migrated Postgres. A process restart
inside one life is a new process generation of the same agent, Pod and attach
token (the attach mints one per process); a new life is End, Resume and a new
agent through the R3.3c retirement machinery.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from orchestrator.routers import thread_transport
from shared.persistent_input_delivery import (
    PINNED_RECOVERY_ADMISSION_LIMIT,
    PINNED_RECOVERY_PARK_REASON,
    PINNED_RECOVERY_RETRY_REASON,
    claim_pending_input_deliveries,
    mark_input_delivery_queued,
    message_row_id,
    persist_input_delivery,
    recovery_chain_admissions,
    reserve_stale_pinned_admissions,
    transition_input_delivery,
)
from tests import test_persistent_recycler_real_postgres as fixtures
from tests.test_b10_session_queries_real_postgres import _request, _transport
from tests.test_pinned_abrupt_death_real_postgres import killed_life
from tests.test_pinned_abrupt_exit_earlier_life_real_postgres import (
    _end_first_life,
    _kill,
    _operations,
    _receipt_args,
    _second_life,
)
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent
from tests.test_subagent_thread_migration import _agent_db

pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied
db = fixtures.db

WAKE = "[wake] the nightly report job finished"
ANSWER = "The nightly report is ready: 3 failures, all in the import step."
NOTHING = {"settled": [], "reserved": [], "parked": []}


# ---------------------------------------------------------------------------
# A pinned runtime, as the agent's input owner drives it
# ---------------------------------------------------------------------------


async def _live(db) -> dict[str, str]:
    """One live pinned life (the recycler fixture's reciprocal binding)."""

    pod_uid = str(uuid4())
    ids = await fixtures._seed(db, pod_uid=pod_uid)
    thread = await db.get_thread(ids["thread"])
    return {
        "thread": ids["thread"],
        "agent": ids["agent"],
        "pod_uid": pod_uid,
        "generation": str(thread["runtime_generation"]),
        "attach_token": ids["attach_token"],
        "process_generation": str(uuid4()),
        "user": ids["user"],
    }


def _restart(life: dict[str, str]) -> dict[str, str]:
    """The same life's next process: a new process generation only."""

    return {**life, "process_generation": str(uuid4())}


def _identity(life, *, generation=None):
    return dict(
        agent_id=life["agent"],
        pod_uid=life["pod_uid"],
        runtime_generation=generation or life["process_generation"],
        session_runtime_generation=life["generation"],
        runtime_attach_token=life["attach_token"],
    )


async def _persist(
    db,
    life,
    *,
    content=WAKE,
    role="event",
    source="officer_wake",
    turn=1,
    supersedes=None,
    delivery_id=None,
    owner_generation=None,
):
    async with db.acquire() as conn:
        async with conn.transaction():
            return await persist_input_delivery(
                conn,
                thread_id=life["thread"],
                delivery_id=delivery_id or uuid4(),
                role=role,
                content=content,
                source=source,
                turn_number=turn,
                supersedes_input_seq=supersedes,
                **_identity(life, generation=owner_generation),
            )


async def _transition(db, life, row, transition, *, turn=None):
    async with db.acquire() as conn:
        async with conn.transaction():
            return await transition_input_delivery(
                conn,
                delivery_id=row["delivery_id"],
                claim_generation=int(row["claim_generation"]),
                transition=transition,
                turn_number=turn,
                **_identity(life),
            )


async def _admit(db, life, row, *, turn):
    """The loop's queue publication and provider admission of one claim."""

    async with db.acquire() as conn:
        async with conn.transaction():
            assert await mark_input_delivery_queued(
                conn,
                delivery_id=row["delivery_id"],
                claim_generation=int(row["claim_generation"]),
                **_identity(life),
            )
    assert await _transition(db, life, row, "admitted", turn=turn)


async def _claim(db, life):
    async with db.acquire() as conn:
        async with conn.transaction():
            return await claim_pending_input_deliveries(
                conn, thread_id=life["thread"], **_identity(life)
            )


async def _reserve(db, life):
    async with db.acquire() as conn:
        async with conn.transaction():
            return await reserve_stale_pinned_admissions(
                conn, thread_id=life["thread"], **_identity(life)
            )


async def _ai(db, life, *, turn, content=ANSWER, tool_call=False):
    """One AI transcript row of a turn: a final answer, or a tool call."""

    await db.execute(
        "INSERT INTO thread_messages (id, thread_id, role, content, tool_calls, "
        "turn_number) VALUES ($1, $2::uuid, 'ai', $3, $4::jsonb, $5)",
        uuid4(),
        life["thread"],
        "" if tool_call else content,
        '[{"id": "call_1", "name": "shell_execute", "args": {}}]'
        if tool_call
        else None,
        turn,
    )


async def _delivery(db, delivery_id):
    return await db.fetchrow(
        "SELECT delivery.*, message.seq FROM thread_input_deliveries AS delivery "
        "JOIN thread_messages AS message ON message.id = delivery.message_id "
        "WHERE delivery.delivery_id = $1::uuid",
        delivery_id,
    )


async def _errors(db, life):
    return await db.fetch(
        "SELECT id, content FROM thread_messages WHERE thread_id=$1::uuid "
        "AND role='error' ORDER BY seq",
        life["thread"],
    )


async def _answers(db, life):
    return await db.fetchval(
        "SELECT count(*) FROM thread_messages WHERE thread_id=$1::uuid "
        "AND role='ai' AND content=$2",
        life["thread"],
        ANSWER,
    )


async def _restored(db, life) -> list[str]:
    """The agent's restore query: the ids the model would read on resume."""

    rows = await _agent_db(db.pool).get_thread_messages_history(
        life["thread"], limit=None
    )
    return [row["id"] for row in rows]


async def _settle_source(db, row):
    """The batch recovery's settle of the input it supersedes."""

    assert await db.fetchval(
        "UPDATE thread_input_deliveries SET state='settled', "
        "settled_at=statement_timestamp() WHERE delivery_id=$1::uuid "
        "AND state='admitted' RETURNING true",
        row["delivery_id"],
    )


async def _continuation(db, life, source, *, n):
    """The pinned batch settle's continuation: owned by the life's runtime
    under its session generation, superseding the abandoned input."""

    source_row = await _delivery(db, source["delivery_id"])
    return await _persist(
        db,
        life,
        content=f"[delegate_agent recovered: continuation {n}]",
        source="subagent",
        turn=1,
        supersedes=int(source_row["seq"]),
        owner_generation=life["generation"],
    )


async def _next_life(db, life):
    """Resume and bind one more dedicated actor, on a Pod name of its own
    (the second life's published intent keeps the default name)."""

    assert await db.resume_thread(life["thread"])
    bound = await _bind_cold_agent(
        db, life["thread"], pod_name=f"persistent-life-{uuid4().hex[:12]}"
    )
    actor = await db.get_agent(str(bound["agent_id"]))
    return {
        "thread": life["thread"],
        "agent": str(bound["agent_id"]),
        "pod_uid": str(actor["pod_uid"]),
        "generation": str(bound["runtime_generation"]),
        "attach_token": str(bound["runtime_attach_token"]),
        "process_generation": str(uuid4()),
    }


async def _killed_admitted_event(db, *, turn=1):
    """A live life whose process died with one event admitted mid-turn."""

    life = await _live(db)
    event = await _persist(db, life, turn=turn)
    await _admit(db, life, event, turn=turn)
    await _ai(db, life, turn=turn, tool_call=True)
    return life, event


async def _drive_to_park(db):
    """Five admissions of one event, each ended by its process's death."""

    life, event = await _killed_admitted_event(db)
    current = life
    for turn in range(2, PINNED_RECOVERY_ADMISSION_LIMIT + 1):
        current = _restart(current)
        assert await _reserve(db, current) == {
            **NOTHING,
            "reserved": [str(event["delivery_id"])],
        }
        (claimed,) = await _claim(db, current)
        assert claimed["delivery_id"] == event["delivery_id"]
        await _admit(db, current, claimed, turn=turn)
        await _ai(db, current, turn=turn, tool_call=True)
    current = _restart(current)
    assert await _reserve(db, current) == {
        **NOTHING,
        "parked": [str(event["delivery_id"])],
    }
    return current, event


# ---------------------------------------------------------------------------
# Serve again
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_owed_event_is_served_again_once_and_answered_once(db):
    life, event = await _killed_admitted_event(db)
    successor = _restart(life)

    assert await _reserve(db, successor) == {
        **NOTHING,
        "reserved": [str(event["delivery_id"])],
    }
    row = await _delivery(db, event["delivery_id"])
    assert row["state"] == "owned"
    assert str(row["owner_runtime_generation"]) == successor["process_generation"]
    assert row["claim_generation"] == event["claim_generation"] + 1
    assert row["admitted_at"] is None and row["admitted_turn_number"] is None
    assert row["admission_count"] == 1
    # Restore leaves out the handed-back copy: the loop sees it once, as input.
    assert str(message_row_id(event["delivery_id"])) not in await _restored(
        db, successor
    )
    # A second pass of the same process selects nothing more.
    assert await _reserve(db, successor) == NOTHING

    (claimed,) = await _claim(db, successor)
    assert claimed["delivery_id"] == event["delivery_id"]
    assert claimed["content"] == WAKE
    await _admit(db, successor, claimed, turn=2)
    await _ai(db, successor, turn=2)
    assert await _transition(db, successor, claimed, "settled")

    third = _restart(successor)
    assert await _reserve(db, third) == NOTHING
    assert await _claim(db, third) == []
    row = await _delivery(db, event["delivery_id"])
    assert (row["state"], row["admitted_turn_number"], row["admission_count"]) == (
        "settled",
        2,
        2,
    )
    assert await _answers(db, life) == 1
    assert str(message_row_id(event["delivery_id"])) in await _restored(db, third)


@pytest.mark.asyncio
async def test_answered_event_is_settled_not_served_again(db):
    life, event = await _killed_admitted_event(db)
    # The turn's final answer is durable; the process died before the settle.
    await _ai(db, life, turn=1)
    successor = _restart(life)

    assert await _reserve(db, successor) == {
        **NOTHING,
        "settled": [str(event["delivery_id"])],
    }
    row = await _delivery(db, event["delivery_id"])
    assert row["state"] == "settled" and row["settled_at"] is not None
    assert row["claim_generation"] == event["claim_generation"]
    assert await _claim(db, successor) == []
    assert await _answers(db, life) == 1


@pytest.mark.asyncio
async def test_event_superseded_by_a_continuation_is_left_to_the_recovery(db):
    life, event = await _killed_admitted_event(db)
    successor = _restart(life)
    continuation = await _continuation(db, successor, event, n=1)

    assert await _reserve(db, successor) == NOTHING
    row = await _delivery(db, event["delivery_id"])
    assert row["state"] == "admitted"
    assert str(row["owner_runtime_generation"]) == life["process_generation"]
    # Only the continuation is served; its chain holds the event's admission.
    claimed = await _claim(db, successor)
    assert [r["delivery_id"] for r in claimed] == [continuation["delivery_id"]]
    assert await recovery_chain_admissions(
        db.pool, delivery_id=continuation["delivery_id"]
    ) == (1, [continuation["delivery_id"], event["delivery_id"]])


@pytest.mark.asyncio
@pytest.mark.parametrize("answered", [False, True], ids=["owed", "answered"])
async def test_event_with_a_later_admission_is_history(db, answered):
    """A later turn ran without it: never replayed into the newer
    conversation; settled only if its own turn had answered it."""

    life, event = await _killed_admitted_event(db)
    if answered:
        await _ai(db, life, turn=1)
    later = _restart(life)
    human = await _persist(
        db, later, content="and the weekly one?", role="human", source="direct_human"
    )
    (claimed,) = [
        row
        for row in await _claim(db, later)
        if row["delivery_id"] == human["delivery_id"]
    ]
    await _admit(db, later, claimed, turn=2)
    assert await _transition(db, later, claimed, "settled")

    successor = _restart(later)
    outcome = await _reserve(db, successor)
    row = await _delivery(db, event["delivery_id"])
    if answered:
        assert outcome == {**NOTHING, "settled": [str(event["delivery_id"])]}
        assert row["state"] == "settled"
    else:
        assert outcome == NOTHING
        assert row["state"] == "admitted"
        assert str(row["owner_runtime_generation"]) == life["process_generation"]
    assert await _claim(db, successor) == []


@pytest.mark.asyncio
async def test_direct_human_partial_turn_keeps_its_receipt(db):
    life = await _live(db)
    human = await _persist(
        db, life, content="compare the offers", role="human", source="direct_human"
    )
    await _admit(db, life, human, turn=1)
    await _ai(db, life, turn=1, tool_call=True)
    successor = _restart(life)

    assert await _reserve(db, successor) == NOTHING
    assert await _claim(db, successor) == []
    row = await _delivery(db, human["delivery_id"])
    assert (row["state"], row["admitted_turn_number"]) == ("admitted", 1)
    assert str(row["owner_runtime_generation"]) == life["process_generation"]
    # Its admitted row is passive context, restored as before.
    assert str(message_row_id(human["delivery_id"])) in await _restored(db, successor)


@pytest.mark.asyncio
async def test_other_threads_and_earlier_history_are_untouched(db):
    life, event = await _killed_admitted_event(db, turn=3)
    # Earlier history of the same thread: a settled event and a settled human
    # input of turns 1 and 2, written before the admission that died.
    settled = []
    for turn, (role, source) in enumerate(
        [("event", "officer_wake"), ("human", "direct_human")], start=1
    ):
        row = await _persist(db, life, role=role, source=source, turn=turn)
        settled.append(row["delivery_id"])
    await db.execute(
        "UPDATE thread_input_deliveries SET state='settled', "
        "admitted_at=statement_timestamp() - interval '1 hour', "
        "admitted_turn_number=1, settled_at=statement_timestamp() "
        "WHERE delivery_id = ANY($1::uuid[])",
        settled,
    )
    other, other_event = await _killed_admitted_event(db)
    successor = _restart(life)

    assert await _reserve(db, successor) == {
        **NOTHING,
        "reserved": [str(event["delivery_id"])],
    }
    for delivery_id in settled:
        assert (await _delivery(db, delivery_id))["state"] == "settled"
    other_row = await _delivery(db, other_event["delivery_id"])
    assert other_row["state"] == "admitted"
    assert str(other_row["owner_runtime_generation"]) == other["process_generation"]


@pytest.mark.asyncio
async def test_unadmitted_attempt_does_not_count(db):
    life = await _live(db)
    event = await _persist(db, life)
    await _admit(db, life, event, turn=1)
    assert (await _delivery(db, event["delivery_id"]))["admission_count"] == 1
    # The termination fence closed in the admission race: no provider call.
    assert await _transition(db, life, event, "unadmit", turn=1)
    assert (await _delivery(db, event["delivery_id"]))["admission_count"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("served", ["admitted", "owned", "parked"])
async def test_a_second_abrupt_death_after_the_re_serve_still_receipts(
    db, monkeypatch, served
):
    """0335: re-serving never leaves a delivery a later receipt refuses."""

    events = []

    async def admit_event(ids, deliveries):
        first = {
            "thread": ids["thread"],
            "agent": ids["agent"],
            "pod_uid": ids["pod_uid"],
            "generation": ids["generation"],
            "attach_token": ids["attach_token"],
            "process_generation": ids["process_generation"],
        }
        row = await _persist(db, first, turn=2)
        await _admit(db, first, row, turn=2)
        if served == "parked":
            # Four earlier deaths of this event: the next one parks it.
            await db.execute(
                "UPDATE thread_input_deliveries SET admission_count=$2 "
                "WHERE delivery_id=$1::uuid",
                row["delivery_id"],
                PINNED_RECOVERY_ADMISSION_LIMIT,
            )
        events.append(row["delivery_id"])

    ids, retirement, deliveries, api, _ = await killed_life(
        db,
        monkeypatch,
        backend="virtual",
        with_virtual_binding=False,
        before_retirement=admit_event,
    )
    (event_id,) = events
    await _end_first_life(db, ids, retirement)
    life = await _second_life(db, ids)

    outcome = await _reserve(db, life)
    key = "parked" if served == "parked" else "reserved"
    assert outcome == {**NOTHING, key: [str(event_id)]}
    claimed = await _claim(db, life)
    assert [row["delivery_id"] for row in claimed] == deliveries + (
        [] if served == "parked" else [event_id]
    )
    if served == "admitted":
        await _admit(db, life, claimed[-1], turn=3)

    retirement2 = await _kill(db, api, life)
    receipt = await db.acknowledge_abrupt_pinned_actor_exit(
        life["thread"], **_receipt_args(life, retirement2)
    )
    assert receipt is not None, "the re-served event must not block the receipt"
    assert receipt["partial_admission_count"] == int(served == "admitted")
    assert receipt["captured_input_count"] == 3
    row = await _delivery(db, event_id)
    assert UUID(str(row["owner_agent_id"])) == UUID(life["agent"])
    if served != "admitted":
        assert await _operations().recover_captured_process_zero(retirement2)
    else:
        # The second life retires; a third serves it again, counting both
        # deaths.
        await _end_first_life(db, life, retirement2)
        third = await _next_life(db, life)
        assert await _reserve(db, third) == {**NOTHING, "reserved": [str(event_id)]}
        assert (await _delivery(db, event_id))["admission_count"] == 2


# ---------------------------------------------------------------------------
# The bound
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repeated_deaths_park_the_event_at_the_bound_with_one_notice(db):
    current, event = await _drive_to_park(db)

    row = await _delivery(db, event["delivery_id"])
    assert (row["state"], row["deferred_reason"]) == (
        "deferred",
        PINNED_RECOVERY_PARK_REASON,
    )
    assert row["admission_count"] == PINNED_RECOVERY_ADMISSION_LIMIT
    assert row["admitted_at"] is None
    # This life's runtime holds it, so this life's exit receipt counts it.
    assert str(row["owner_runtime_generation"]) == current["process_generation"]
    errors = await _errors(db, current)
    assert len(errors) == 1
    assert f"after {PINNED_RECOVERY_ADMISSION_LIMIT} attempts" in errors[0]["content"]
    # Neither the model nor the loop sees it again.
    assert str(message_row_id(event["delivery_id"])) not in await _restored(db, current)
    assert await _claim(db, current) == []
    later = _restart(current)
    assert await _reserve(db, later) == NOTHING
    assert await _claim(db, later) == []
    # A stable-identity retry of the same delivery (an outbox wake) leaves it
    # parked.
    again = await _persist(db, later, delivery_id=event["delivery_id"])
    assert (again["state"], again["deferred_reason"]) == (
        "deferred",
        PINNED_RECOVERY_PARK_REASON,
    )
    assert await _claim(db, later) == []
    assert len(await _errors(db, later)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "deaths",
    [
        ("delegated", "delegated", "delegated", "delegated"),
        ("delegated", "plain", "delegated", "plain"),
    ],
    ids=["continuations", "mixed"],
)
async def test_a_chain_of_recovery_turns_parks_at_the_bound(db, deaths):
    """D3: each recovery turn that delegates and dies supersedes the last
    continuation with a new one, so the bound counts the whole chain."""

    life = await _live(db)
    human = await _persist(
        db, life, content="compare the offers", role="human", source="direct_human"
    )
    await _admit(db, life, human, turn=1)
    # Each death ends the turn just admitted: "delegated" leaves a batch whose
    # recovery settles the input and writes a continuation, "plain" leaves
    # the input admitted for the re-serve. The fifth admission's death parks.
    current, source, turn = life, human, 1
    death = "delegated"
    for step in (*deaths, "final"):
        current = _restart(current)
        if death == "delegated":
            await _settle_source(db, source)
            source = await _continuation(db, current, source, n=turn)
            outcome = await _reserve(db, current)
            assert outcome == NOTHING
        else:
            outcome = await _reserve(db, current)
        claimed = await _claim(db, current)
        if turn == PINNED_RECOVERY_ADMISSION_LIMIT:
            break
        if death == "plain":
            assert outcome == {**NOTHING, "reserved": [str(source["delivery_id"])]}
        assert [row["delivery_id"] for row in claimed] == [source["delivery_id"]]
        turn += 1
        await _admit(db, current, claimed[0], turn=turn)
        death = step

    assert turn == PINNED_RECOVERY_ADMISSION_LIMIT
    assert claimed == []
    if death == "plain":
        assert outcome == {**NOTHING, "parked": [str(source["delivery_id"])]}
    row = await _delivery(db, source["delivery_id"])
    assert (row["state"], row["deferred_reason"]) == (
        "deferred",
        PINNED_RECOVERY_PARK_REASON,
    )
    assert (
        await recovery_chain_admissions(db.pool, delivery_id=source["delivery_id"])
    )[0] == PINNED_RECOVERY_ADMISSION_LIMIT
    assert len(await _errors(db, current)) == 1
    # The person's own input is never parked; its receipt stands.
    assert (await _delivery(db, human["delivery_id"]))["state"] == "settled"


@pytest.mark.asyncio
async def test_human_input_is_never_parked(db):
    current, event = await _drive_to_park(db)
    typed = await _persist(
        db, current, content="are you there?", role="human", source="direct_human"
    )
    # Even a human row carrying an inflated count is no recovery chain.
    await db.execute(
        "UPDATE thread_input_deliveries SET admission_count=$2 "
        "WHERE delivery_id=$1::uuid",
        typed["delivery_id"],
        PINNED_RECOVERY_ADMISSION_LIMIT + 2,
    )
    later = _restart(current)
    claimed = await _claim(db, later)
    assert [row["delivery_id"] for row in claimed] == [typed["delivery_id"]]
    assert (await _delivery(db, typed["delivery_id"]))["state"] == "owned"
    assert len(await _errors(db, later)) == 1


@pytest.mark.asyncio
async def test_owner_retry_clears_the_park_and_serves_it_again(db):
    current, event = await _drive_to_park(db)
    deps = _transport(db, UUID(current["user"]), {"id": current["thread"]})

    revived = await thread_transport.thread_queue_retry(
        current["thread"], _request(), dependencies=deps
    )
    assert (revived["state"], revived["park_reason"]) == (
        "queued",
        PINNED_RECOVERY_PARK_REASON,
    )
    row = await _delivery(db, event["delivery_id"])
    assert (row["state"], row["deferred_reason"], row["admission_count"]) == (
        "deferred",
        PINNED_RECOVERY_RETRY_REASON,
        0,
    )
    with pytest.raises(HTTPException) as again:
        await thread_transport.thread_queue_retry(
            current["thread"], _request(), dependencies=deps
        )
    assert (again.value.status_code, again.value.detail) == (404, "Unit is not parked")

    # The live runtime's inbox poll serves it, answered once.
    (claimed,) = await _claim(db, current)
    assert claimed["delivery_id"] == event["delivery_id"]
    await _admit(db, current, claimed, turn=7)
    await _ai(db, current, turn=7)
    assert await _transition(db, current, claimed, "settled")
    assert await _answers(db, current) == 1
    assert (await _delivery(db, event["delivery_id"]))["admission_count"] == 1


@pytest.mark.asyncio
async def test_owner_retry_resets_the_whole_chain(db):
    life = await _live(db)
    human = await _persist(
        db, life, content="compare the offers", role="human", source="direct_human"
    )
    await _admit(db, life, human, turn=1)
    current, source = life, human
    for n in range(1, PINNED_RECOVERY_ADMISSION_LIMIT + 1):
        current = _restart(current)
        await _settle_source(db, source)
        source = await _continuation(db, current, source, n=n)
        claimed = await _claim(db, current)
        if n == PINNED_RECOVERY_ADMISSION_LIMIT:
            assert claimed == []
            break
        await _admit(db, current, claimed[0], turn=n + 1)
    deps = _transport(db, UUID(current["user"]), {"id": current["thread"]})
    await thread_transport.thread_queue_retry(
        current["thread"], _request(), dependencies=deps
    )
    admissions, members = await recovery_chain_admissions(
        db.pool, delivery_id=source["delivery_id"]
    )
    assert admissions == 0 and len(members) == PINNED_RECOVERY_ADMISSION_LIMIT + 1
    (claimed,) = await _claim(db, current)
    assert claimed["delivery_id"] == source["delivery_id"]
