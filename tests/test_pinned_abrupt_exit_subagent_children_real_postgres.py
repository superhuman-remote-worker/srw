"""A killed pinned life that delegated still receipts its abrupt exit.

Session subagents run in-process inside the parent's agent Pod: a child is a
``threads`` row (``parent_thread_id`` = the session) plus a ledger, with no
runtime, executor, inbox or workspace of its own. Once the orchestrator has
proven every container of that Pod stopped, such a child can produce nothing
more; the receipt leaves its ``running``/``queued`` row as the crash left it
(the retirement settle then ends it ``cancelled:parent_retired``). Before 0335 ``acknowledge_abrupt_pinned_actor_exit`` refused on any
child row at all, so a pinned virtual or lite session that had ever delegated
could not retire after a SIGKILL (pinned-lane plan P0, vault
``features/parallel_subagents.md`` §14.2). A child that does carry a producer
of its own must keep the receipt refused.
"""

import json
from uuid import UUID, uuid4

import pytest

from orchestrator import main
from orchestrator.application import controls
from shared.persistent_input_delivery import message_row_id, transition_input_delivery
from tests import test_lite_pinned_actor_exit_real_postgres as lite
from tests import test_persistent_recycler_real_postgres as fixtures
from orchestrator.services import stale_agent_detector as detector
from tests.test_pinned_abrupt_death_real_postgres import killed_life
from tests.test_pinned_retirement_retry_parity_real_postgres import (
    _run_one_detector_pass,
)
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent

pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied
db = fixtures.db

# backend, with_virtual_binding: the lite tier, virtual with its rclone
# backing, and virtual without one. All three reach the abrupt receipt.
TIERS = [("none", True), ("virtual", True), ("virtual", False)]
BATCHES = {
    # SIGKILL in the middle of a fan-out: two children running, one queued
    # behind the cap, the source input admitted.
    "interrupted_batch": ["running", "running", "queued"],
    # One delegation long ago: the child ended and its turn settled.
    "delegated_once": ["completed"],
    # A pinned background child still queued in the dead process.
    "background": ["background"],
    # The P0b shape: two running, one queued behind the cap, and one that
    # finished but whose report never reached the parent.
    "mixed_batch": ["running", "running", "queued", "completed"],
}
# Not in the matrices: one child its own runtime already ended interrupted
# (what P4's graceful hand-over will leave), one still running.
HANDED_OVER = ["agent_interrupted", "running"]
#: Who retired the dead life: a person's End (no cause) or the stale-agent
#: detector's orphan path (``cause: runtime_lost``).
CAUSES = ["person", "runtime_lost"]


def _pinned_authority(ids):
    return {
        "version": 1,
        "execution_lane": "pinned",
        "parent_thread_id": ids["thread"],
        "agent_id": ids["agent"],
        "pod_uid": ids["pod_uid"],
        "session_runtime_generation": ids["generation"],
        "runtime_attach_token": ids["attach_token"],
    }


async def _delegate(db, ids, source_delivery_id, states):
    """Turn 1 delegated once per state, through the real child writers."""

    ai_id = uuid4()
    calls = [f"call_delegate_{index}_{uuid4().hex[:8]}" for index in range(len(states))]
    await db.execute(
        "INSERT INTO thread_messages "
        "(id, thread_id, role, content, tool_calls, turn_number) "
        "VALUES ($1::uuid, $2::uuid, 'ai', '', $3::jsonb, 1)",
        ai_id,
        ids["thread"],
        json.dumps(
            [
                {
                    "id": call_id,
                    "name": "delegate_agent",
                    "args": {
                        "subagent_type": "reader",
                        "description": f"brief {index}",
                        "prompt": f"brief {index}",
                    },
                }
                for index, call_id in enumerate(calls)
            ]
        ),
    )
    authority = _pinned_authority(ids)
    children = []
    for index, (call_id, state) in enumerate(zip(calls, states)):
        handle = f"reader-{index:04x}"
        child = await db.create_session_subagent_thread(
            parent_thread_id=ids["thread"],
            parent_authority=authority,
            handle=handle,
            subagent_type="reader",
            parent_tool_call_id=call_id,
            parent_input_message_id=str(message_row_id(source_delivery_id)),
            parent_ai_message_id=str(ai_id),
            parent_iteration=1,
            brief_description=f"brief {index}",
            run_in_background=state == "background",
            initial_status="queued" if state in {"queued", "background"} else "running",
        )
        assert child is not None
        if state == "agent_interrupted":
            ended = await db.terminalize_session_subagent_thread(
                parent_thread_id=ids["thread"],
                parent_authority=authority,
                thread_id=child["thread_id"],
                runtime_generation=child["runtime_generation"],
                subagent_status="interrupted",
                outcome="interrupted:parent_restart",
                turns=1,
                tokens=200,
                error="handed over at shutdown",
            )
            assert ended is not None and ended["result"] == "applied"
        if state == "completed":
            ended = await db.terminalize_session_subagent_thread(
                parent_thread_id=ids["thread"],
                parent_authority=authority,
                thread_id=child["thread_id"],
                runtime_generation=child["runtime_generation"],
                subagent_status="completed",
                outcome="completed",
                turns=3,
                tokens=900,
                report_path=f".subagents/{handle}/report.md",
            )
            assert ended is not None and ended["result"] == "applied"
        children.append(child["thread_id"])
    return children


async def _settle_source(db, ids, delivery_id):
    claim = await db.fetchval(
        "SELECT claim_generation FROM thread_input_deliveries WHERE delivery_id=$1::uuid",
        delivery_id,
    )
    async with db.acquire() as conn:
        async with conn.transaction():
            assert await transition_input_delivery(
                conn,
                delivery_id=delivery_id,
                agent_id=ids["agent"],
                pod_uid=ids["pod_uid"],
                runtime_generation=ids["process_generation"],
                session_runtime_generation=ids["generation"],
                runtime_attach_token=ids["attach_token"],
                claim_generation=int(claim),
                transition="settled",
                turn_number=1,
            )


def _delegating(db, batch, children):
    """A pre-Begin hook for the shared killed-life helpers."""

    states = BATCHES[batch] if isinstance(batch, str) else list(batch)

    async def before_retirement(ids, deliveries):
        children.extend(await _delegate(db, ids, deliveries[0], states))
        if batch == "delegated_once":
            await _settle_source(db, ids, deliveries[0])

    return before_retirement


async def _children(db, thread_id):
    return [
        dict(row)
        for row in await db.fetch(
            "SELECT id, status, subagent_status, subagent_outcome FROM threads "
            "WHERE parent_thread_id=$1::uuid ORDER BY created_at, id",
            thread_id,
        )
    ]


async def _killed_delegating_life(
    db, monkeypatch, *, batch, backend, binding, cause="person"
):
    """A SIGKILLed life that delegated.

    ``cause="person"``: an owner End began its retirement (no cause).
    ``cause="runtime_lost"``: nothing began it; the stale-agent detector's own
    orphan path will (``_retire_through_detector``), and ``retirement`` is None.
    """

    children: list[str] = []
    ids, retirement, deliveries, api, _ = await killed_life(
        db,
        monkeypatch,
        backend=backend,
        partial=True,
        with_virtual_binding=binding,
        before_retirement=_delegating(db, batch, children),
        begin=cause == "person",
    )
    states = BATCHES[batch] if isinstance(batch, str) else batch
    assert len(children) == len(states)
    ids["k8s"] = api
    return ids, retirement, deliveries, children


async def _retire_through_detector(db, monkeypatch, ids):
    """Heartbeats stopped, the Pod is proven killed: run the real detector."""

    for pod in ids["k8s"].pods.values():
        pod.spec.restart_policy = "Never"
    monkeypatch.setattr(detector, "PINNED_RETIREMENT_TERMINAL_PROBE_GRACE_SECONDS", 0)
    await db.execute(
        "UPDATE agents SET last_heartbeat=now()-interval '5 minutes' WHERE id=$1::uuid",
        ids["agent"],
    )
    # One pass marks the agent offline, begins (orphan path) and retries the
    # pending retirement; a second pass is allowed for a Begin that lands
    # after the pass's retry listing.
    for _ in range(2):
        await _run_one_detector_pass()
        if (await db.get_thread(ids["thread"]))["status"] == "ended":
            return
    raise AssertionError("the detector did not settle the killed life")


def _receipt_args(ids, retirement):
    return dict(
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
        agent_id=ids["agent"],
        attach_token=ids["attach_token"],
        stopped_pod_uid=ids["pod_uid"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("backend,binding", TIERS)
@pytest.mark.parametrize("batch", sorted(BATCHES))
async def test_killed_life_that_delegated_receipts_its_abrupt_exit(
    db, monkeypatch, batch, backend, binding
):
    ids, retirement, deliveries, children = await _killed_delegating_life(
        db, monkeypatch, batch=batch, backend=backend, binding=binding
    )
    before = await _children(db, ids["thread"])

    receipt = await db.acknowledge_abrupt_pinned_actor_exit(
        ids["thread"], **_receipt_args(ids, retirement)
    )
    assert receipt is not None, "an in-process child must not block the receipt"
    assert receipt["recovery_protocol"] == (
        "abrupt_lite_actor_exit_v1"
        if backend == "none"
        else "abrupt_virtual_actor_exit_v1"
    )
    # The receipt neither settles input nor touches the ledger: the source
    # stays admitted (or settled) and every child row stays as the crash left
    # it.
    assert receipt["partial_admission_count"] == int(batch != "delegated_once")
    assert receipt["stranded_input_count"] == 2
    assert await _children(db, ids["thread"]) == before
    assert await controls.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    assert await _children(db, ids["thread"]) == before


@pytest.mark.asyncio
async def test_killed_warm_conference_that_delegated_receipts_its_abrupt_exit(
    db, monkeypatch
):
    """Every conference runs on a warm-pool Pod (0227); delegate there too."""

    children: list[str] = []
    ids, retirement, _ = await lite._retired_warm_lite_actor(
        db,
        monkeypatch,
        input_state="admitted",
        before_retirement=_delegating(db, "interrupted_batch", children),
    )
    assert len(children) == 3
    assert await controls.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    receipt = lite._receipt(await db.get_thread(ids["thread"]))
    assert receipt["recovery_protocol"] == "abrupt_lite_actor_exit_v1"
    assert receipt["partial_admission_count"] == 1
    assert {row["subagent_status"] for row in await _children(db, ids["thread"])} == {
        "running",
        "queued",
    }


@pytest.mark.asyncio
async def test_settled_exit_still_refuses_a_running_child(db, monkeypatch):
    """The settled-work receipt keeps its contract; the abrupt one answers."""

    ids, retirement, _, _ = await _killed_delegating_life(
        db, monkeypatch, batch="interrupted_batch", backend="none", binding=True
    )
    args = _receipt_args(ids, retirement)
    assert (
        await db.acknowledge_settled_virtual_actor_exit(ids["thread"], **args) is None
    )
    assert await db.acknowledge_abrupt_pinned_actor_exit(ids["thread"], **args)


async def _subagent_events(db, thread_id):
    return [
        dict(row)
        for row in await db.fetch(
            "SELECT m.content, d.state FROM thread_input_deliveries d "
            "JOIN thread_messages m ON m.id=d.message_id "
            "WHERE d.thread_id=$1::uuid AND d.source='subagent' ORDER BY m.seq",
            thread_id,
        )
    ]


def _retired_state(state, cause):
    """How the settle leaves one child the dead runtime held."""

    if state == "completed":
        return ("ended", "completed", "completed")
    if state == "agent_interrupted":
        return ("ended", "interrupted", "interrupted:parent_restart")
    if state in {"running", "queued"} and cause == "runtime_lost":
        return ("ended", "interrupted", "interrupted:parent_restart")
    return ("ended", "cancelled", "cancelled:parent_retired")


def _assert_retired_children(rows, batch, events, cause="person"):
    """What the settle leaves: per cause for live foreground children;
    finished ones kept; a background child cancelled with its event."""

    states = BATCHES[batch] if isinstance(batch, str) else batch
    assert [
        (row["status"], row["subagent_status"], row["subagent_outcome"]) for row in rows
    ] == [_retired_state(state, cause) for state in states]
    # Only a pinned background child leaves an event; it is owed to Resume.
    assert len(events) == states.count("background")
    for event in events:
        assert "cancelled:parent_retired" in event["content"]
        assert event["state"] == "persisted"


@pytest.mark.asyncio
@pytest.mark.parametrize("backend,binding", TIERS)
@pytest.mark.parametrize("batch", sorted(BATCHES))
@pytest.mark.parametrize("cause", CAUSES)
async def test_detector_retires_a_killed_life_that_delegated(
    db, monkeypatch, cause, batch, backend, binding
):
    """The real durable retry after a SIGKILL: receipt, End funnel, settle.

    A person's End cancels the live children (``cancelled:parent_retired``);
    the detector's own retirement of a lost runtime ends live foreground
    children ``interrupted:parent_restart`` so the successor recovers them.
    A background child is cancelled with its retirement event either way.
    """

    ids, _, _, children = await _killed_delegating_life(
        db, monkeypatch, batch=batch, backend=backend, binding=binding, cause=cause
    )
    # Children made by the real writers never look like producers.
    for child in children:
        assert not await db.fetchval(
            "SELECT public.pinned_session_child_may_produce($1::uuid)", child
        )
    before = (await db.get_thread(ids["thread"]))["last_activity"]

    await _retire_through_detector(db, monkeypatch, ids)

    thread = await db.get_thread(ids["thread"])
    # A retirement event is new transcript activity; nothing else is.
    assert (thread["last_activity"] > before) is (batch == "background")
    assert thread["status"] == "ended"
    assert thread["runtime_retirement_token"] is None
    assert thread["agent_id"] is None
    rows = await _children(db, ids["thread"])
    assert [str(row["id"]) for row in rows] == children
    _assert_retired_children(
        rows, batch, await _subagent_events(db, ids["thread"]), cause
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("backend,binding", TIERS)
@pytest.mark.parametrize("disposition", ["ended", "suspended"])
async def test_soft_retirement_with_a_live_background_child_settles(
    db, monkeypatch, disposition, backend, binding
):
    """The agent's own End or suspend, its local cleanup acknowledged."""

    children: list[str] = []
    ids, retirement, _, _, _ = await killed_life(
        db,
        monkeypatch,
        backend=backend,
        partial=True,
        with_virtual_binding=binding,
        settle_status=disposition,
        before_retirement=_delegating(db, "background", children),
    )
    assert await db.acknowledge_pinned_thread_local_quiescence(
        ids["thread"],
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_agent_id=ids["agent"],
        expected_attach_token=ids["attach_token"],
        expected_settle_status=disposition,
        expected_quiescence_protocol="agent_runtime_zero_v1",
        expected_workspace_generation=None,
        expected_workspace_runtime_incarnation=None,
    )
    before = (await db.get_thread(ids["thread"]))["last_activity"]
    result = await controls.thread_retirement_operations(
        main.app.state.resources
    ).end_thread_flow(
        ids["thread"],
        await db.get_thread(ids["thread"]),
        permanent=False,
        force=True,
        expected_runtime_generation=retirement["generation"],
        expected_agent_id=ids["agent"],
        expected_attach_token=ids["attach_token"],
        settle_status=disposition,
        local_runtime_quiesced=True,
    )
    assert result["status"] == disposition
    thread = await db.get_thread(ids["thread"])
    assert thread["status"] == disposition
    assert thread["last_activity"] > before
    assert thread["runtime_retirement_token"] is None
    rows = await _children(db, ids["thread"])
    assert [str(row["id"]) for row in rows] == children
    _assert_retired_children(
        rows, "background", await _subagent_events(db, ids["thread"])
    )


async def _successor_authority(db, thread_id):
    """Resume the ended session and bind a fresh dedicated actor (new life)."""

    assert await db.resume_thread(thread_id)
    successor = await _bind_cold_agent(db, thread_id)
    actor = await db.get_agent(str(successor["agent_id"]))
    return {
        "version": 1,
        "execution_lane": "pinned",
        "parent_thread_id": thread_id,
        "agent_id": str(successor["agent_id"]),
        "pod_uid": str(actor["pod_uid"]),
        "session_runtime_generation": str(successor["runtime_generation"]),
        "runtime_attach_token": str(successor["runtime_attach_token"]),
    }


def _listed(listing):
    return [str(row.get("thread_id") or row.get("id")) for row in listing["subagents"]]


async def _tool_results(db, thread_id):
    return [
        dict(row)
        for row in await db.fetch(
            "SELECT tool_call_id, content FROM thread_messages "
            "WHERE thread_id=$1::uuid AND role='tool' AND rewound_at IS NULL "
            "ORDER BY seq",
            thread_id,
        )
    ]


async def _continuations(db, thread_id):
    return [
        dict(row)
        for row in await db.fetch(
            "SELECT delivery_id, state, supersedes_input_seq "
            "FROM thread_input_deliveries "
            "WHERE thread_id=$1::uuid AND source='subagent'",
            thread_id,
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("batch", ["interrupted_batch", "mixed_batch"])
async def test_successor_recovers_the_batch_its_lost_runtime_left(
    db, monkeypatch, batch
):
    """P0b: SIGKILL mid-batch, the detector retires the lost runtime, Resume.

    The interrupted foreground children end ``interrupted:parent_restart``, so
    the successor's listing names the turn and its batch settle answers every
    call once, with one continuation, and settles the abandoned source input.
    """

    ids, _, deliveries, children = await _killed_delegating_life(
        db, monkeypatch, batch=batch, backend="none", binding=True, cause="runtime_lost"
    )
    await _retire_through_detector(db, monkeypatch, ids)
    _assert_retired_children(
        await _children(db, ids["thread"]),
        batch,
        await _subagent_events(db, ids["thread"]),
        "runtime_lost",
    )

    authority = await _successor_authority(db, ids["thread"])
    listing = await db.list_live_session_subagent_recovery(
        ids["thread"], parent_authority=authority
    )
    assert sorted(_listed(listing)) == sorted(children)
    [plan] = listing["recovery_turns"]
    assert plan.get("error") is None
    source_seq = await db.fetchval(
        "SELECT seq FROM thread_messages WHERE id=$1::uuid",
        message_row_id(deliveries[0]),
    )
    assert plan["supersedes_input_seq"] == source_seq
    # Every call has a child that ended without a result reaching the parent.
    assert [call["class"] for call in plan["calls"]] == ["ended"] * len(children)
    assert [call["thread_id"] for call in plan["calls"]] == children
    assert all(call["needs_entry"] and call["needs_message"] for call in plan["calls"])
    assert [call["outcome"] for call in plan["calls"]] == [
        _retired_state(state, "runtime_lost")[2] for state in BATCHES[batch]
    ]

    members = [
        {
            "thread_id": call["thread_id"],
            "runtime_generation": call["runtime_generation"],
            "subagent_status": call["subagent_status"],
            "outcome": call["outcome"],
            "message": f"replayed result of {call['handle']}",
        }
        for call in plan["calls"]
    ]
    settle = dict(
        parent_thread_id=ids["thread"],
        parent_authority=authority,
        parent_input_message_id=plan["parent_input_message_id"],
        parent_iteration=plan["parent_iteration"],
        members=members,
    )
    result = await db.settle_session_subagent_batch(**settle)
    assert result["result"] == "applied"
    results = await _tool_results(db, ids["thread"])
    assert [row["tool_call_id"] for row in results] == [
        call["tool_call_id"] for call in plan["calls"]
    ]
    assert [row["content"] for row in results] == [m["message"] for m in members]
    [continuation] = await _continuations(db, ids["thread"])
    assert str(continuation["delivery_id"]) == plan["delivery_id"]
    assert continuation["supersedes_input_seq"] == source_seq
    assert (
        await db.fetchval(
            "SELECT state FROM thread_input_deliveries WHERE delivery_id=$1::uuid",
            deliveries[0],
        )
        == "settled"
    )

    # A retry (a lost response, a second recoverer) writes nothing more.
    again = await db.settle_session_subagent_batch(**settle)
    assert again["result"] == "idempotent"
    assert await _tool_results(db, ids["thread"]) == results
    assert len(await _continuations(db, ids["thread"])) == 1
    relisted = await db.list_live_session_subagent_recovery(
        ids["thread"], parent_authority=authority
    )
    assert _listed(relisted) == [] and relisted["recovery_turns"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("batch", ["interrupted_batch", "mixed_batch"])
async def test_a_persons_end_of_a_killed_batch_still_cancels_it(db, monkeypatch, batch):
    """D4: a person's End of the same dead batch cancels it (unchanged).

    The live list excludes ``cancelled:parent_retired``: with nothing finished
    there is no plan; a finished child's plan reports the others retired.
    """

    ids, _, deliveries, children = await _killed_delegating_life(
        db, monkeypatch, batch=batch, backend="none", binding=True, cause="person"
    )
    await _retire_through_detector(db, monkeypatch, ids)
    authority = await _successor_authority(db, ids["thread"])
    listing = await db.list_live_session_subagent_recovery(
        ids["thread"], parent_authority=authority
    )
    if batch == "interrupted_batch":
        assert _listed(listing) == []
        assert listing["recovery_turns"] == []
    else:
        assert _listed(listing) == [children[3]]
        [plan] = listing["recovery_turns"]
        assert [call["class"] for call in plan["calls"]] == [
            "retired",
            "retired",
            "retired",
            "ended",
        ]
    assert (
        await db.fetchval(
            "SELECT state FROM thread_input_deliveries WHERE delivery_id=$1::uuid",
            deliveries[0],
        )
        == "admitted"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("cause", CAUSES)
async def test_the_settle_keeps_a_child_its_runtime_already_ended(
    db, monkeypatch, cause
):
    """Only created/active children are retired; terminal ones keep their facts.

    A child its own runtime ended ``interrupted:parent_restart`` before the
    retirement (P4's graceful hand-over) stays so under either cause, and
    the successor's live list still offers it until its result is delivered.
    """

    ids, _, _, children = await _killed_delegating_life(
        db, monkeypatch, batch=HANDED_OVER, backend="none", binding=True, cause=cause
    )
    await _retire_through_detector(db, monkeypatch, ids)
    rows = await _children(db, ids["thread"])
    _assert_retired_children(rows, HANDED_OVER, [], cause)
    assert await db.fetchval(
        "SELECT subagent_error FROM threads WHERE id=$1::uuid", children[0]
    ) == ("handed over at shutdown")

    authority = await _successor_authority(db, ids["thread"])
    listing = await db.list_live_session_subagent_recovery(
        ids["thread"], parent_authority=authority
    )
    expected = children if cause == "runtime_lost" else children[:1]
    assert sorted(_listed(listing)) == sorted(expected)
    [plan] = listing["recovery_turns"]
    assert [call["class"] for call in plan["calls"]] == [
        "ended",
        "ended" if cause == "runtime_lost" else "retired",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["detector", "person"])
async def test_only_the_detectors_begin_names_the_lost_runtime(db, monkeypatch, first):
    """The cause is written once, by the first Begin, into the context."""

    ids, retirement, _, _ = await _killed_delegating_life(
        db,
        monkeypatch,
        batch="interrupted_batch",
        backend="none",
        binding=True,
        cause="runtime_lost" if first == "detector" else "person",
    )
    # The Pod still runs: recovery cannot prove process zero, so the
    # retirement stays pending and its immutable context can be read.
    for pod in ids["k8s"].pods.values():
        pod.spec.restart_policy = "Never"
    ids["k8s"].mark_ready(*next(iter(ids["k8s"].pods)))
    monkeypatch.setattr(detector, "PINNED_RETIREMENT_TERMINAL_PROBE_GRACE_SECONDS", 0)
    await db.execute(
        "UPDATE agents SET last_heartbeat=now()-interval '5 minutes' WHERE id=$1::uuid",
        ids["agent"],
    )
    await _run_one_detector_pass()

    thread = await db.get_thread(ids["thread"])
    assert thread["status"] == "active"
    assert thread["runtime_retirement_token"] is not None
    if retirement is not None:
        assert str(thread["runtime_retirement_token"]) == retirement["token"]
    context = fixtures._json(thread["runtime_retirement_context"])
    assert context["initiator"] == "owner"
    if first == "detector":
        assert context["cause"] == "runtime_lost"
    else:
        # A person's End began it: no cause, and the detector's later Begin
        # reused that token without writing one.
        assert "cause" not in context


@pytest.mark.asyncio
async def test_a_lost_runtime_cancels_a_child_that_answers_no_call(db, monkeypatch):
    """Only a child answering a parent tool call can be recovered as a result."""

    orphan = uuid4()

    async def before_retirement(ids, deliveries):
        await _replica(
            db,
            "INSERT INTO threads (id,user_id,kind,parent_thread_id,status,"
            "execution_lane,subagent_status,metadata) VALUES ($1,$2::uuid,"
            "'subagent',$3::uuid,'active','pinned','running','{}'::jsonb)",
            orphan,
            ids["user"],
            ids["thread"],
        )

    ids, _, _, api, _ = await killed_life(
        db,
        monkeypatch,
        backend="none",
        partial=True,
        before_retirement=before_retirement,
        begin=False,
    )
    ids["k8s"] = api
    await _retire_through_detector(db, monkeypatch, ids)
    [row] = await _children(db, ids["thread"])
    assert row["id"] == orphan
    assert (row["status"], row["subagent_status"], row["subagent_outcome"]) == (
        "ended",
        "cancelled",
        "cancelled:parent_retired",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "initiator,cause", [("owner", "person"), ("agent", "runtime_lost")]
)
async def test_begin_accepts_only_an_orchestrator_runtime_lost_cause(
    db, initiator, cause
):
    with pytest.raises(ValueError):
        await db.begin_pinned_thread_retirement(
            str(uuid4()), permanent=False, initiator=initiator, cause=cause
        )


async def _replica(db, sql, *args):
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(sql, *args)


async def _seed_child_producer(db, ids, child_id, fault):
    """Give one in-process child a producer of its own (seeded pre-Begin state)."""

    child = UUID(child_id)
    if fault == "bound_agent":
        await _replica(
            db,
            "INSERT INTO agents (id,config_name,hostname,pod_ip,pod_uid,status,"
            "agent_mode,thread_id,last_heartbeat) VALUES ($1,'reader',$2,"
            "'127.0.0.2',$3,'session','persistent',$4,now())",
            uuid4(),
            f"child-{child_id[:12]}",
            str(uuid4()),
            child,
        )
    elif fault in {
        "agent_pointer",
        "control_admission_pointer",
        "attach_token",
        "authority_exposed",
        "retiring",
        "agent_pod",
        "vm",
        "workspace_binding",
    }:
        assignment = {
            "agent_pointer": "agent_id=$2::uuid",
            "control_admission_pointer": "control_admission_agent_id=$2::uuid",
            "attach_token": "runtime_attach_token=$2::uuid",
            "authority_exposed": "runtime_authority_exposed=true",
            "retiring": (
                "runtime_retirement_token=$2::uuid,runtime_retirement_permanent=false,"
                "runtime_retirement_started_at=now(),"
                "runtime_retirement_context='{}'::jsonb"
            ),
            "agent_pod": (
                "metadata=jsonb_set(metadata,'{agent_pod}',"
                "jsonb_build_object('pod_name','child-pod','pod_uid',$2::text))"
            ),
            "vm": (
                "metadata=jsonb_set(metadata,'{vm}',"
                "jsonb_build_object('vm_uid',$2::text))"
            ),
            "workspace_binding": (
                "metadata=jsonb_set(metadata,'{_workspace_binding}',"
                "jsonb_build_object('kind','sandbox','generation',$2::text))"
            ),
        }[fault]
        sql = f"UPDATE threads SET {assignment} WHERE id=$1::uuid"
        if "$2::uuid" in assignment:
            await _replica(db, sql, child, uuid4())
        elif "$2::text" in assignment:
            await _replica(db, sql, child, str(uuid4()))
        else:
            await _replica(db, sql, child)
    elif fault == "grandchild":
        await _replica(
            db,
            "INSERT INTO threads (id,user_id,kind,parent_thread_id,status,"
            "execution_lane,subagent_status) VALUES ($1,$2::uuid,'subagent',$3,"
            "'active','pinned','running')",
            uuid4(),
            ids["user"],
            child,
        )
    elif fault == "pending_input":
        message = uuid4()
        await _replica(
            db,
            "INSERT INTO thread_messages (id,thread_id,role,content,turn_number) "
            "VALUES ($1,$2,'human','an input for the child itself',1)",
            message,
            child,
        )
        await _replica(
            db,
            "INSERT INTO thread_input_deliveries "
            "(delivery_id,thread_id,message_id,source,execution_lane) "
            "VALUES ($1,$2,$3,'direct_human','pinned')",
            uuid4(),
            child,
            message,
        )
    elif fault == "pending_control":
        await _replica(
            db,
            "INSERT INTO thread_control_requests "
            "(thread_id,request_seq,client_request_id,verb,requested_by) "
            "VALUES ($1,1,$2,'stop','owner')",
            child,
            uuid4(),
        )
    elif fault == "pending_permission":
        await _replica(
            db,
            "INSERT INTO thread_permission_requests (thread_id,tool_call_id,tool_name) "
            "VALUES ($1,'child-tool','run_command')",
            child,
        )
    elif fault == "pending_interrupt":
        await _replica(
            db,
            "INSERT INTO thread_interrupt_requests (thread_id,client_request_id,"
            "target_turn_id,accepted_lease_token,accepted_leased_by,requested_by) "
            "VALUES ($1,$2,1,1,'executor-a','owner')",
            child,
            uuid4(),
        )
    elif fault == "run_queue":
        await _replica(
            db,
            "INSERT INTO run_queue (unit_id,unit_kind) VALUES ($1,'session_turn')",
            child,
        )
    elif fault == "pending_effect":
        await _replica(
            db,
            "INSERT INTO completion_effects "
            "(producer_kind,producer_id,scope_id,effect_name,effect_group,state) "
            "VALUES ('session_turn',$1,$2,'memory','optional','pending')",
            uuid4(),
            child,
        )
    elif fault == "producer_effect":
        await _replica(
            db,
            "INSERT INTO completion_effects "
            "(producer_kind,producer_id,scope_id,effect_name,effect_group,state) "
            "VALUES ('session_turn',$1,$2,'memory','optional','pending')",
            child,
            uuid4(),
        )
    elif fault == "workspace_claim":
        await _replica(
            db,
            "INSERT INTO thread_agent_workspace_claims (claim_id,thread_id,"
            "created_runtime_generation,create_attempt,provisioner,pvc_name) "
            "VALUES ($1,$2,$3,$4,'persistent','pvc-child')",
            uuid4(),
            child,
            uuid4(),
            uuid4(),
        )
    elif fault == "workspace_intent":
        await _replica(
            db,
            "INSERT INTO thread_workspace_provision_intents (attempt_id,thread_id,"
            "runtime_generation,namespace,pod_name,network_tier,manifest_fingerprint) "
            "VALUES ($1,$2,$3,'agents-a','ws-child','restricted',$4)",
            uuid4(),
            child,
            uuid4(),
            "f" * 64,
        )
    elif fault == "repository_reservation":
        await _replica(
            db,
            "INSERT INTO managed_repository_workspace_creation_reservations "
            "(owner_kind,owner_id,scope,claimed_by,desired_manifest_digest,expires_at,"
            "thread_runtime_generation) VALUES ('thread',$1,'workspace_container',"
            "'orchestrator-a',$2,now()+interval '1 hour',$3)",
            child,
            "d" * 64,
            uuid4(),
        )
    elif fault == "cloud_mount":
        await _replica(
            db,
            "INSERT INTO cloud_ro_mounts (thread_id,user_id,backend,reader_id,"
            "grant_handle,credentials,webdav_url,auth_kind,status,"
            "runtime_generation,engage_attempt,backend_instance_id,grant_group_id,"
            "grant_handle_sha256,source_binding,source_binding_sha256,"
            "selected_mount_id) VALUES ($1,$2::uuid,'nextcloud','reader-a',"
            "'grant-a','secret','https://cloud.example/dav','basic','active',"
            "$3,$4,$5,'group-a',$6,'{}'::jsonb,$6,$7)",
            child,
            ids["user"],
            uuid4(),
            uuid4(),
            uuid4(),
            "c" * 64,
            uuid4(),
        )
    elif fault in DONE_BUT_HELD:
        base, held = DONE_BUT_HELD[fault]
        await _seed_child_producer(db, ids, child_id, base)
        await _replica(db, held, child)
    else:  # pragma: no cover - a typo in the parametrization
        raise AssertionError(fault)


# Terminal-looking rows that still hold a producer: an interrupt applied but
# never consumed, a finished queue unit still leased, a finished effect still
# claimed by a worker.
DONE_BUT_HELD = {
    "unconsumed_interrupt": (
        "pending_interrupt",
        "UPDATE thread_interrupt_requests SET outcome='applied',"
        "result='{}'::jsonb,applied_mode='hard',applied_at=now(),"
        "applied_lease_token=accepted_lease_token,journal_epoch=1,"
        "journal_seq=1,acknowledged_at=now() WHERE thread_id=$1",
    ),
    "leased_queue": (
        "run_queue",
        "UPDATE run_queue SET state='done',leased_by='executor-a' WHERE unit_id=$1",
    ),
    "claimed_effect": (
        "pending_effect",
        "UPDATE completion_effects SET state='done',claimed_by=gen_random_uuid() "
        "WHERE scope_id=$1",
    ),
}

CHILD_PRODUCERS = [
    "bound_agent",
    "agent_pointer",
    "control_admission_pointer",
    "attach_token",
    "authority_exposed",
    "retiring",
    "agent_pod",
    "vm",
    "workspace_binding",
    "grandchild",
    "pending_input",
    "pending_control",
    "pending_permission",
    "pending_interrupt",
    "run_queue",
    "pending_effect",
    "producer_effect",
    *DONE_BUT_HELD,
    "workspace_claim",
    "workspace_intent",
    "repository_reservation",
    "cloud_mount",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", CHILD_PRODUCERS)
async def test_abrupt_receipt_refuses_a_child_with_a_producer_of_its_own(
    db, monkeypatch, fault
):
    children: list[str] = []

    async def before_retirement(ids, deliveries):
        children.extend(
            await _delegate(db, ids, deliveries[0], BATCHES["interrupted_batch"])
        )
        await _seed_child_producer(db, ids, children[-1], fault)

    ids, retirement, _, _, _ = await killed_life(
        db,
        monkeypatch,
        backend="none",
        partial=True,
        before_retirement=before_retirement,
    )
    assert (
        await db.acknowledge_abrupt_pinned_actor_exit(
            ids["thread"], **_receipt_args(ids, retirement)
        )
        is None
    )
    assert not await controls.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
    assert (await db.get_thread(ids["thread"]))[
        "runtime_retirement_local_quiescence"
    ] is None


# The same rows as the producers above, driven to a terminal state.
FINISHED = {
    "pending_input": (
        "UPDATE thread_input_deliveries SET state='cancelled',"
        "cancelled_at=now(),cancelled_turn_number=1,"
        "cancelled_reason='rewound' WHERE thread_id=$1"
    ),
    "pending_control": (
        "UPDATE thread_control_requests SET outcome='rejected',"
        "result='{}'::jsonb,applied_at=now(),applied_agent_id=$2,"
        "journal_epoch=1,journal_seq=1,acknowledged_at=now() "
        "WHERE thread_id=$1"
    ),
    "pending_permission": (
        "UPDATE thread_permission_requests SET status='denied',"
        "decided_at=now(),decided_by='owner' WHERE thread_id=$1"
    ),
    "pending_interrupt": (
        "UPDATE thread_interrupt_requests SET outcome='applied',"
        "result=jsonb_build_object('consumed_input_seq',1),"
        "applied_mode='hard',applied_at=now(),"
        "applied_lease_token=accepted_lease_token,journal_epoch=1,"
        "journal_seq=1,acknowledged_at=now() WHERE thread_id=$1"
    ),
    "run_queue": "UPDATE run_queue SET state='done' WHERE unit_id=$1",
    "pending_effect": "UPDATE completion_effects SET state='done' WHERE scope_id=$1",
    "workspace_claim": (
        "UPDATE thread_agent_workspace_claims SET status='reclaimed',"
        "pvc_uid='pvc-uid',fenced_at=now(),gc_after=now()+interval '10 minutes',"
        "resolved_at=now() WHERE thread_id=$1"
    ),
    "cloud_mount": (
        "UPDATE cloud_ro_mounts SET status='revoked',revoked_at=now() "
        "WHERE thread_id=$1"
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", sorted(FINISHED))
async def test_a_childs_finished_work_does_not_count_as_a_producer(
    db, monkeypatch, fault
):
    """Only unfinished work refuses: the same rows, terminal, are history."""

    children: list[str] = []

    async def before_retirement(ids, deliveries):
        children.extend(
            await _delegate(db, ids, deliveries[0], BATCHES["interrupted_batch"])
        )
        await _seed_child_producer(db, ids, children[-1], fault)
        child = UUID(children[-1])
        finish = FINISHED[fault]
        if "$2" in finish:
            await _replica(db, finish, child, uuid4())
        else:
            await _replica(db, finish, child)

    ids, retirement, _, _, _ = await killed_life(
        db,
        monkeypatch,
        backend="none",
        partial=True,
        before_retirement=before_retirement,
    )
    assert await db.acknowledge_abrupt_pinned_actor_exit(
        ids["thread"], **_receipt_args(ids, retirement)
    )
