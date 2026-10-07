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
from tests.test_pinned_abrupt_death_real_postgres import killed_life

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
}


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

    async def before_retirement(ids, deliveries):
        children.extend(await _delegate(db, ids, deliveries[0], BATCHES[batch]))
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


async def _killed_delegating_life(db, monkeypatch, *, batch, backend, binding):
    children: list[str] = []
    ids, retirement, deliveries, _, _ = await killed_life(
        db,
        monkeypatch,
        backend=backend,
        partial=True,
        with_virtual_binding=binding,
        before_retirement=_delegating(db, batch, children),
    )
    assert len(children) == len(BATCHES[batch])
    return ids, retirement, deliveries, children


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


@pytest.mark.asyncio
async def test_killed_life_that_delegated_ends_and_its_children_are_retired(
    db, monkeypatch
):
    """The whole End after a SIGKILL mid-batch now finishes.

    What the retirement settle then does to the live children is existing
    behaviour (``_terminalize_live_session_subagents_for_retirement``): it
    ends them ``cancelled:parent_retired``, which the successor's batch settle
    reports as retired. Pinned here so a later package changes it on purpose.
    """

    ids, retirement, _, children = await _killed_delegating_life(
        db, monkeypatch, batch="interrupted_batch", backend="none", binding=True
    )
    assert await controls.pinned_retirement_operations(
        main.app.state.resources
    ).recover_captured_process_zero(retirement)
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
    thread = await db.get_thread(ids["thread"])
    assert thread["status"] == "ended"
    assert thread["runtime_retirement_token"] is None
    rows = await _children(db, ids["thread"])
    assert [str(row["id"]) for row in rows] == children
    assert {
        (row["status"], row["subagent_status"], row["subagent_outcome"]) for row in rows
    } == {("ended", "cancelled", "cancelled:parent_retired")}


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
