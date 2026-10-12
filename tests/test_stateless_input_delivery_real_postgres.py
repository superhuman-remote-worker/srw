"""Real-PostgreSQL proofs for durable event input on stateless sessions."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from agent.api.turn_executor import _PENDING_INPUT_SQL
from shared.persistent_input_delivery import (
    InputDeliveryAuthorityLost,
    claim_pending_input_deliveries,
    claim_stateless_input_delivery,
    transition_input_delivery,
    transition_stateless_input_delivery,
)
from shared.run_queue import (
    UNIT_KIND_SESSION_TURN,
    claim_unit,
    complete_unit,
    record_input_seq,
    release_unit,
)


SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:
        pytest.skip(f"local PostgreSQL container unavailable: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA_FILE.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied):
    store = PostgresDB(
        connection_string=pg_dsn,
        min_connections=1,
        max_connections=12,
    )
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE run_queue, session_wake_events, "
            "thread_input_deliveries, thread_messages, threads, users CASCADE"
        )
    try:
        yield store
    finally:
        await store.close()


def _retirement_metadata() -> dict:
    return {
        "_stateless_workspace_retirement_settled": {
            "terminal_token": 1,
            "cleanup_complete": True,
            "permanent": False,
            "backing_id": None,
            "runtime_incarnation": None,
            "snapshot_restore_required": False,
            "workspace_absence_proven": False,
        }
    }


async def _seed_thread(
    db: PostgresDB,
    *,
    status: str = "active",
    metadata: dict | None = None,
) -> tuple[UUID, UUID]:
    user_id = uuid4()
    thread_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, display_name, email) "
            "VALUES ($1, 'stateless input owner', $2)",
            user_id,
            f"{user_id}@example.test",
        )
        await conn.execute(
            "INSERT INTO threads "
            "(id, user_id, status, execution_lane, config_name, metadata) "
            "VALUES ($1, $2, $3, 'stateless', 'default', $4::jsonb)",
            thread_id,
            user_id,
            status,
            json.dumps(metadata or {"config_override": {"officer": {"enabled": True}}}),
        )
    return user_id, thread_id


async def _seed_pinned_thread(
    db: PostgresDB, *, pod_uid: str = "pod-pinned"
) -> tuple[UUID, UUID, UUID]:
    user_id = uuid4()
    thread_id = uuid4()
    agent_id = uuid4()
    provision_attempt = uuid4()
    runtime_attach_token = uuid4()
    pod_name = "pinned-input-pod"
    namespace = "agents-a"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id,display_name,email) "
            "VALUES ($1,'pinned input owner',$2)",
            user_id,
            f"{user_id}@example.test",
        )
        await conn.execute(
            "INSERT INTO threads "
            "(id,user_id,status,execution_lane,config_name,metadata) "
            "VALUES ($1,$2,'active','pinned','default',$3::jsonb)",
            thread_id,
            user_id,
            json.dumps({"config_override": {"officer": {"enabled": True}}}),
        )
        runtime_generation = await conn.fetchval(
            "SELECT runtime_generation FROM threads WHERE id=$1",
            thread_id,
        )

    reserved = await db.reserve_pinned_agent_pod_provision_intent(
        str(thread_id),
        expected_runtime_generation=str(runtime_generation),
        attempt_id=str(provision_attempt),
        pod_name=pod_name,
        provisioner="persistent",
        namespace=namespace,
    )
    assert reserved is not None
    assert await db.publish_pinned_agent_pod_provision_intent(
        str(thread_id),
        expected_runtime_generation=str(runtime_generation),
        attempt_id=str(provision_attempt),
        pod_name=pod_name,
        pod_uid=pod_uid,
        namespace=namespace,
    )

    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO agents "
            "(id,config_name,hostname,pod_ip,pod_uid,status,agent_mode,"
            "last_heartbeat) "
            "VALUES ($1,'default',$2,'127.0.0.1',$3,'session','persistent',now())",
            agent_id,
            pod_name,
            pod_uid,
        )
        async with conn.transaction():
            await conn.execute(
                "UPDATE threads SET agent_id=$2,control_admission_agent_id=$2,"
                "runtime_attach_token=$3 WHERE id=$1",
                thread_id,
                agent_id,
                runtime_attach_token,
            )
            await conn.execute(
                "UPDATE agents SET thread_id=$2 WHERE id=$1",
                agent_id,
                thread_id,
            )
    return user_id, thread_id, agent_id


async def _persist_event(
    db: PostgresDB,
    thread_id: UUID,
    delivery_id: UUID,
    content: str = "[wake] inspect the completed job",
):
    return await db.persist_thread_input_delivery(
        thread_id=str(thread_id),
        delivery_id=str(delivery_id),
        role="event",
        content=content,
        source="officer_wake",
    )


async def _claim_delivery(db: PostgresDB, **kwargs):
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE threads
                   SET metadata = jsonb_set(
                       COALESCE(metadata, '{}'::jsonb),
                       '{_stateless_active_claim}',
                       jsonb_build_object(
                           'lease_token', $2::bigint,
                           'pod', $3::text,
                           'pod_uid', $4::text
                       ),
                       true
                   )
                 WHERE id = $1
                """,
                UUID(str(kwargs["thread_id"])),
                int(kwargs["lease_token"]),
                str(kwargs["executor_id"]),
                str(kwargs["pod_uid"]),
            )
            return await claim_stateless_input_delivery(conn, **kwargs)


async def _transition_delivery(db: PostgresDB, **kwargs):
    async with db.acquire() as conn:
        async with conn.transaction():
            return await transition_stateless_input_delivery(conn, **kwargs)


@pytest.mark.asyncio
async def test_stateless_persist_is_atomic_stable_and_role_truthful(db):
    _, thread_id = await _seed_thread(db)
    delivery_id = uuid4()

    first, retry = await asyncio.gather(
        _persist_event(db, thread_id, delivery_id),
        _persist_event(db, thread_id, delivery_id),
    )
    assert sorted((first["transcript_inserted"], retry["transcript_inserted"])) == [
        False,
        True,
    ]
    assert first["message_id"] == retry["message_id"]
    assert first["seq"] == retry["seq"]
    assert first["state"] == retry["state"] == "queued"

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT message.role, message.turn_number, delivery.execution_lane, "
            "delivery.state, queue.state AS queue_state, queue.input_seq, "
            "queue.consumed_seq FROM thread_input_deliveries AS delivery "
            "JOIN thread_messages AS message ON message.id=delivery.message_id "
            "JOIN run_queue AS queue ON queue.unit_id=delivery.thread_id "
            "WHERE delivery.delivery_id=$1",
            delivery_id,
        )
    assert dict(row) == {
        "role": "event",
        "turn_number": 1,
        "execution_lane": "stateless",
        "state": "queued",
        "queue_state": "queued",
        "input_seq": first["seq"],
        "consumed_seq": first["seq"] - 1,
    }


@pytest.mark.asyncio
async def test_delivery_insert_failure_rolls_back_transcript_and_queue(db):
    _, thread_id = await _seed_thread(db)
    delivery_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "CREATE FUNCTION reject_test_delivery() RETURNS trigger "
            "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fault seam'; END $$"
        )
        await conn.execute(
            "CREATE TRIGGER trg_reject_test_delivery BEFORE INSERT "
            "ON thread_input_deliveries FOR EACH ROW "
            "EXECUTE FUNCTION reject_test_delivery()"
        )
    try:
        with pytest.raises(asyncpg.RaiseError, match="fault seam"):
            await _persist_event(db, thread_id, delivery_id)
    finally:
        async with db.acquire() as conn:
            await conn.execute(
                "DROP TRIGGER trg_reject_test_delivery ON thread_input_deliveries"
            )
            await conn.execute("DROP FUNCTION reject_test_delivery()")

    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM thread_messages WHERE thread_id=$1", thread_id
            )
            == 0
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM run_queue WHERE unit_id=$1", thread_id
            )
            == 0
        )


@pytest.mark.asyncio
async def test_human_and_event_fifo_then_multiple_events_requeue(db):
    user_id, thread_id = await _seed_thread(db)
    human_id = uuid4()
    async with db.acquire() as conn:
        async with conn.transaction():
            human_seq = await conn.fetchval(
                "INSERT INTO thread_messages "
                "(id,thread_id,role,content,turn_number) "
                "VALUES ($1,$2,'human','first human',1) RETURNING seq",
                human_id,
                thread_id,
            )
            await conn.execute(
                "UPDATE threads SET total_turns=1 WHERE id=$1", thread_id
            )
            await record_input_seq(
                conn,
                unit_id=thread_id,
                unit_kind=UNIT_KIND_SESSION_TURN,
                input_seq=human_seq,
                fair_key=str(user_id),
            )
    event_a = await _persist_event(db, thread_id, uuid4(), "event a")
    event_b = await _persist_event(db, thread_id, uuid4(), "event b")

    async with db.acquire() as conn:
        rows = await conn.fetch(_PENDING_INPUT_SQL, thread_id, -1, 10)
        assert [(row["role"], row["seq"]) for row in rows] == [
            ("human", human_seq),
            ("event", event_a["seq"]),
            ("event", event_b["seq"]),
        ]
        first_claim = await claim_unit(
            conn, unit_kind=UNIT_KIND_SESSION_TURN, pod_name="executor-a"
        )
        assert first_claim is not None
        assert (
            await complete_unit(
                conn,
                unit_id=thread_id,
                lease_token=first_claim.lease_token,
                consumed_seq=human_seq,
            )
            == "queued"
        )

        event_claim = await claim_unit(
            conn,
            unit_kind=UNIT_KIND_SESSION_TURN,
            pod_name="executor-a",
            prefer_unit_id=thread_id,
        )
    assert event_claim is not None
    claimed = await _claim_delivery(
        db,
        thread_id=str(thread_id),
        delivery_id=str(event_a["delivery_id"]),
        lease_token=event_claim.lease_token,
        executor_id="executor-a",
        pod_uid="pod-a",
    )
    assert claimed is not None
    assert await _transition_delivery(
        db,
        thread_id=str(thread_id),
        delivery_id=str(event_a["delivery_id"]),
        lease_token=event_claim.lease_token,
        executor_id="executor-a",
        pod_uid="pod-a",
        claim_generation=claimed["claim_generation"],
        transition="admitted",
        turn_number=2,
    )
    assert await _transition_delivery(
        db,
        thread_id=str(thread_id),
        delivery_id=str(event_a["delivery_id"]),
        lease_token=event_claim.lease_token,
        executor_id="executor-a",
        pod_uid="pod-a",
        claim_generation=claimed["claim_generation"],
        transition="settled",
    )
    async with db.acquire() as conn:
        # event_b is still pending even though its seq is below the monotonic
        # watermark we deliberately preserve on historical-hole recovery.
        state = await complete_unit(
            conn,
            unit_id=thread_id,
            lease_token=event_claim.lease_token,
            consumed_seq=event_a["seq"],
        )
        assert state == "queued"


@pytest.mark.asyncio
async def test_exact_lease_fences_stale_claim_and_outbox_settlement(db):
    _, thread_id = await _seed_thread(db)
    assert await db.enqueue_session_wake_event(
        str(thread_id),
        source="timer",
        dedup_key="stateless-gate",
        payload={"reason": "prove admission"},
    )
    wake = await db.claim_pending_session_wake_events(
        debounce_seconds_by_source={"timer": 0}
    )
    assigned = await db.assign_session_wake_delivery_groups([wake[0]["id"]])
    delivery_id = UUID(str(assigned[0]["delivery_id"]))
    delivery = await _persist_event(db, thread_id, delivery_id)

    with pytest.raises(asyncpg.CheckViolationError):
        await db.finish_session_wake_events([wake[0]["id"]])

    async with db.acquire() as conn:
        first = await claim_unit(
            conn, unit_kind=UNIT_KIND_SESSION_TURN, pod_name="executor-a"
        )
    assert first is not None
    first_delivery = await _claim_delivery(
        db,
        thread_id=str(thread_id),
        delivery_id=str(delivery_id),
        lease_token=first.lease_token,
        executor_id="executor-a",
        pod_uid="pod-a",
    )
    assert first_delivery is not None
    # Pod names are reusable.  A replacement incarnation with the same name
    # and queue token cannot borrow the credential-bound UID stamped by the
    # bundle response.
    async with db.acquire() as conn:
        async with conn.transaction():
            with pytest.raises(InputDeliveryAuthorityLost):
                await claim_stateless_input_delivery(
                    conn,
                    thread_id=str(thread_id),
                    delivery_id=str(delivery_id),
                    lease_token=first.lease_token,
                    executor_id="executor-a",
                    pod_uid="replacement-pod-uid",
                )
    async with db.acquire() as conn:
        assert (
            await release_unit(
                conn,
                unit_id=thread_id,
                lease_token=first.lease_token,
            )
            == "queued"
        )
        second = await claim_unit(
            conn, unit_kind=UNIT_KIND_SESSION_TURN, pod_name="executor-b"
        )
    assert second is not None
    second_delivery = await _claim_delivery(
        db,
        thread_id=str(thread_id),
        delivery_id=str(delivery_id),
        lease_token=second.lease_token,
        executor_id="executor-b",
        pod_uid="pod-b",
    )
    assert second_delivery is not None
    assert second_delivery["claim_generation"] == (
        first_delivery["claim_generation"] + 1
    )
    with pytest.raises(InputDeliveryAuthorityLost):
        await _transition_delivery(
            db,
            thread_id=str(thread_id),
            delivery_id=str(delivery_id),
            lease_token=first.lease_token,
            executor_id="executor-a",
            pod_uid="pod-a",
            claim_generation=first_delivery["claim_generation"],
            transition="admitted",
            turn_number=1,
        )
    assert await _transition_delivery(
        db,
        thread_id=str(thread_id),
        delivery_id=str(delivery_id),
        lease_token=second.lease_token,
        executor_id="executor-b",
        pod_uid="pod-b",
        claim_generation=second_delivery["claim_generation"],
        transition="admitted",
        turn_number=1,
    )
    await db.finish_session_wake_events([wake[0]["id"]])
    # A committed response may be lost. Stable persistence and settlement
    # retries observe the same admitted identity and cannot buy a second turn.
    replay = await _persist_event(db, thread_id, delivery_id, "new render ignored")
    assert replay["state"] == "admitted"
    assert replay["content"] == delivery["content"]
    await db.finish_session_wake_events([wake[0]["id"]])


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_state", ["admitted", "settled"])
async def test_terminal_stateless_replay_survives_lane_change_and_finishes_outbox(
    db,
    terminal_state,
):
    _, thread_id = await _seed_thread(db)
    assert await db.enqueue_session_wake_event(
        str(thread_id),
        source="timer",
        dedup_key=f"terminal-lane-replay:{terminal_state}",
        payload={"reason": "prove stable terminal replay"},
    )
    wake = await db.claim_pending_session_wake_events(
        debounce_seconds_by_source={"timer": 0}
    )
    assigned = await db.assign_session_wake_delivery_groups([wake[0]["id"]])
    delivery_id = UUID(str(assigned[0]["delivery_id"]))
    first = await _persist_event(db, thread_id, delivery_id, "original wake")

    async with db.acquire() as conn:
        claim = await claim_unit(
            conn, unit_kind=UNIT_KIND_SESSION_TURN, pod_name="executor-a"
        )
    assert claim is not None
    owned = await _claim_delivery(
        db,
        thread_id=str(thread_id),
        delivery_id=str(delivery_id),
        lease_token=claim.lease_token,
        executor_id="executor-a",
        pod_uid="pod-a",
    )
    assert owned is not None
    assert await _transition_delivery(
        db,
        thread_id=str(thread_id),
        delivery_id=str(delivery_id),
        lease_token=claim.lease_token,
        executor_id="executor-a",
        pod_uid="pod-a",
        claim_generation=owned["claim_generation"],
        transition="admitted",
        turn_number=1,
    )
    if terminal_state == "settled":
        assert await _transition_delivery(
            db,
            thread_id=str(thread_id),
            delivery_id=str(delivery_id),
            lease_token=claim.lease_token,
            executor_id="executor-a",
            pod_uid="pod-a",
            claim_generation=owned["claim_generation"],
            transition="settled",
        )

    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET execution_lane='pinned' WHERE id=$1", thread_id
        )

    replay = await _persist_event(db, thread_id, delivery_id, "new render ignored")
    assert replay["state"] == terminal_state
    assert replay["execution_lane"] == "stateless"
    assert replay["transcript_inserted"] is False
    assert replay["content"] == first["content"]

    await db.finish_session_wake_events([wake[0]["id"]])
    await db.finish_session_wake_events([wake[0]["id"]])
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT state FROM session_wake_events WHERE id=$1", wake[0]["id"]
            )
            == "sent"
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM thread_messages WHERE thread_id=$1",
                thread_id,
            )
            == 1
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM thread_input_deliveries WHERE delivery_id=$1",
                delivery_id,
            )
            == 1
        )


@pytest.mark.asyncio
async def test_terminal_pinned_replay_survives_change_to_stateless(db):
    _, thread_id, agent_id = await _seed_pinned_thread(db)
    assert await db.enqueue_session_wake_event(
        str(thread_id),
        source="timer",
        dedup_key="terminal-pinned-lane-replay",
        payload={"reason": "prove reverse stable terminal replay"},
    )
    wake = await db.claim_pending_session_wake_events(
        debounce_seconds_by_source={"timer": 0}
    )
    assigned = await db.assign_session_wake_delivery_groups([wake[0]["id"]])
    delivery_id = UUID(str(assigned[0]["delivery_id"]))
    first = await _persist_event(db, thread_id, delivery_id, "pinned wake")
    process_generation = uuid4()
    async with db.acquire() as conn:
        async with conn.transaction():
            runtime_identity = await conn.fetchrow(
                "UPDATE threads SET runtime_attach_token=COALESCE("
                "runtime_attach_token,gen_random_uuid()) WHERE id=$1 "
                "RETURNING runtime_generation,runtime_attach_token",
                thread_id,
            )
            rows = await claim_pending_input_deliveries(
                conn,
                thread_id=thread_id,
                agent_id=agent_id,
                pod_uid="pod-pinned",
                runtime_generation=process_generation,
                session_runtime_generation=runtime_identity["runtime_generation"],
                runtime_attach_token=runtime_identity["runtime_attach_token"],
            )
            assert len(rows) == 1
            assert await transition_input_delivery(
                conn,
                delivery_id=delivery_id,
                agent_id=agent_id,
                pod_uid="pod-pinned",
                runtime_generation=process_generation,
                session_runtime_generation=runtime_identity["runtime_generation"],
                runtime_attach_token=runtime_identity["runtime_attach_token"],
                claim_generation=rows[0]["claim_generation"],
                transition="admitted",
                turn_number=1,
            )
        await conn.execute(
            "UPDATE threads SET execution_lane='stateless' WHERE id=$1",
            thread_id,
        )

    replay = await _persist_event(db, thread_id, delivery_id, "changed render ignored")
    assert replay["state"] == "admitted"
    assert replay["execution_lane"] == "pinned"
    assert replay["transcript_inserted"] is False
    assert replay["content"] == first["content"]
    await db.finish_session_wake_events([wake[0]["id"]])
    await db.finish_session_wake_events([wake[0]["id"]])
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT state FROM session_wake_events WHERE id=$1", wake[0]["id"]
            )
            == "sent"
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM thread_messages WHERE thread_id=$1", thread_id
            )
            == 1
        )


@pytest.mark.asyncio
async def test_old_writers_and_claimants_fail_closed(db):
    _, thread_id = await _seed_thread(db)
    delivery = await _persist_event(db, thread_id, uuid4())
    async with db.acquire() as conn:
        with pytest.raises(
            asyncpg.CheckViolationError,
            match="lane-aware executor claim",
        ):
            await conn.execute(
                "UPDATE run_queue SET state='leased', "
                "lease_token=lease_token+1, leased_by='old', "
                "leased_until=now()+interval '1 minute' WHERE unit_id=$1",
                thread_id,
            )
        foreign_message = uuid4()
        await conn.execute(
            "INSERT INTO thread_messages "
            "(id,thread_id,role,content,turn_number) "
            "VALUES ($1,$2,'event','old writer',2)",
            foreign_message,
            thread_id,
        )
        with pytest.raises(asyncpg.CheckViolationError, match="owning thread"):
            await conn.execute(
                "INSERT INTO thread_input_deliveries "
                "(delivery_id,thread_id,message_id,source) "
                "VALUES ($1,$2,$3,'officer_wake')",
                uuid4(),
                thread_id,
                foreign_message,
            )
        assert (
            await conn.fetchval(
                "SELECT state FROM thread_input_deliveries WHERE delivery_id=$1",
                UUID(str(delivery["delivery_id"])),
            )
            == "queued"
        )


@pytest.mark.asyncio
async def test_suspended_wakes_and_ended_waits_for_supported_resume(db):
    _, suspended = await _seed_thread(db, status="suspended")
    suspended_delivery = await _persist_event(db, suspended, uuid4())
    async with db.acquire() as conn:
        assert (
            await conn.fetchval("SELECT status FROM threads WHERE id=$1", suspended)
            == "created"
        )
        assert (
            await conn.fetchval(
                "SELECT state FROM run_queue WHERE unit_id=$1", suspended
            )
            == "queued"
        )
    assert suspended_delivery["state"] == "queued"

    _, ended = await _seed_thread(db, status="ended", metadata=_retirement_metadata())
    ended_delivery = await _persist_event(db, ended, uuid4())
    assert ended_delivery["state"] == "persisted"
    async with db.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM run_queue WHERE unit_id=$1)", ended
        )
    assert await db.resume_thread(str(ended))
    async with db.acquire() as conn:
        resumed = await conn.fetchrow(
            "SELECT thread.status, queue.state, queue.input_seq, "
            "queue.consumed_seq FROM threads AS thread "
            "JOIN run_queue AS queue ON queue.unit_id=thread.id "
            "WHERE thread.id=$1",
            ended,
        )
    assert dict(resumed) == {
        "status": "created",
        "state": "queued",
        "input_seq": ended_delivery["seq"],
        "consumed_seq": ended_delivery["seq"] - 1,
    }


@pytest.mark.asyncio
async def test_lane_change_serializes_with_pending_delivery(db):
    _, thread_id = await _seed_thread(db)
    delivery = await _persist_event(db, thread_id, uuid4())
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError, match="durable input"):
            await conn.execute(
                "UPDATE threads SET execution_lane='pinned' WHERE id=$1", thread_id
            )
        claim = await claim_unit(
            conn, unit_kind=UNIT_KIND_SESSION_TURN, pod_name="executor"
        )
    assert claim is not None
    owned = await _claim_delivery(
        db,
        thread_id=str(thread_id),
        delivery_id=str(delivery["delivery_id"]),
        lease_token=claim.lease_token,
        executor_id="executor",
        pod_uid="pod",
    )
    assert owned is not None
    assert await _transition_delivery(
        db,
        thread_id=str(thread_id),
        delivery_id=str(delivery["delivery_id"]),
        lease_token=claim.lease_token,
        executor_id="executor",
        pod_uid="pod",
        claim_generation=owned["claim_generation"],
        transition="admitted",
        turn_number=1,
    )
    assert await _transition_delivery(
        db,
        thread_id=str(thread_id),
        delivery_id=str(delivery["delivery_id"]),
        lease_token=claim.lease_token,
        executor_id="executor",
        pod_uid="pod",
        claim_generation=owned["claim_generation"],
        transition="settled",
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET execution_lane='pinned' WHERE id=$1", thread_id
        )
        assert (
            await conn.fetchval(
                "SELECT execution_lane FROM threads WHERE id=$1", thread_id
            )
            == "pinned"
        )


@pytest.mark.asyncio
async def test_old_insert_and_lane_change_have_only_two_safe_outcomes(db):
    """The trigger locks the thread row, closing the old-writer MVCC gap."""

    # Insert wins: its pending pinned delivery prevents the later lane change.
    user_id = uuid4()
    insert_wins = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id,display_name,email) VALUES ($1,'race',$2)",
            user_id,
            f"{user_id}@example.test",
        )
        await conn.execute(
            "INSERT INTO threads (id,user_id,status,execution_lane) "
            "VALUES ($1,$2,'active','pinned')",
            insert_wins,
            user_id,
        )
    insert_conn = await asyncpg.connect(db._connection_string)
    update_conn = await asyncpg.connect(db._connection_string)
    try:
        tx = insert_conn.transaction()
        await tx.start()
        message_id = uuid4()
        await insert_conn.execute(
            "INSERT INTO thread_messages "
            "(id,thread_id,role,content,turn_number) "
            "VALUES ($1,$2,'event','insert wins',1)",
            message_id,
            insert_wins,
        )
        await insert_conn.execute(
            "INSERT INTO thread_input_deliveries "
            "(delivery_id,thread_id,message_id,source) "
            "VALUES ($1,$2,$3,'officer_wake')",
            uuid4(),
            insert_wins,
            message_id,
        )
        blocked_update = asyncio.create_task(
            update_conn.execute(
                "UPDATE threads SET execution_lane='stateless' WHERE id=$1",
                insert_wins,
            )
        )
        await asyncio.sleep(0.05)
        assert not blocked_update.done()
        await tx.commit()
        with pytest.raises(asyncpg.CheckViolationError, match="durable input"):
            await blocked_update

        # Lane change wins: the blocked rolling-old/default-pinned insert sees
        # the new stateless lane and is rejected before either row commits.
        update_wins = uuid4()
        await update_conn.execute(
            "INSERT INTO threads (id,user_id,status,execution_lane) "
            "VALUES ($1,$2,'active','pinned')",
            update_wins,
            user_id,
        )
        lane_tx = update_conn.transaction()
        await lane_tx.start()
        await update_conn.execute(
            "UPDATE threads SET execution_lane='stateless' WHERE id=$1",
            update_wins,
        )

        async def old_insert():
            async with insert_conn.transaction():
                old_message = uuid4()
                await insert_conn.execute(
                    "INSERT INTO thread_messages "
                    "(id,thread_id,role,content,turn_number) "
                    "VALUES ($1,$2,'event','lane wins',1)",
                    old_message,
                    update_wins,
                )
                await insert_conn.execute(
                    "INSERT INTO thread_input_deliveries "
                    "(delivery_id,thread_id,message_id,source) "
                    "VALUES ($1,$2,$3,'officer_wake')",
                    uuid4(),
                    update_wins,
                    old_message,
                )

        blocked_insert = asyncio.create_task(old_insert())
        await asyncio.sleep(0.05)
        assert not blocked_insert.done()
        await lane_tx.commit()
        with pytest.raises(asyncpg.CheckViolationError, match="owning thread"):
            await blocked_insert
    finally:
        await insert_conn.close()
        await update_conn.close()

    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT execution_lane FROM threads WHERE id=$1", insert_wins
            )
            == "pinned"
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM thread_input_deliveries WHERE thread_id=$1",
                insert_wins,
            )
            == 1
        )
        assert (
            await conn.fetchval(
                "SELECT execution_lane FROM threads WHERE id=$1", update_wins
            )
            == "stateless"
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM thread_input_deliveries WHERE thread_id=$1",
                update_wins,
            )
            == 0
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM thread_messages WHERE thread_id=$1", update_wins
            )
            == 0
        )
