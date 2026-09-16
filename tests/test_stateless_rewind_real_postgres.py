"""Real-PostgreSQL acceptance for idle stateless conversation rewind."""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.schemas.thread_rewind import StatelessRewindRequest
from orchestrator.services.thread_rewind import RewindFailure, ThreadRewindService


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
        max_connections=8,
    )
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE completion_effects, thread_rewinds, run_queue, "
            "session_wake_events, "
            "thread_input_deliveries, thread_messages, threads, users CASCADE"
        )
    try:
        yield store
    finally:
        await store.close()


async def _seed_idle_thread(db: PostgresDB) -> tuple[dict, UUID, list[UUID]]:
    user_id = uuid4()
    thread_id = uuid4()
    message_ids = [uuid4() for _ in range(4)]
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, display_name, email) VALUES ($1,'owner',$2)",
            user_id,
            f"{user_id}@example.test",
        )
        await conn.execute(
            "INSERT INTO threads "
            "(id,user_id,status,execution_lane,config_name,total_turns,metadata) "
            "VALUES ($1,$2,'active','stateless','default',2,'{}'::jsonb)",
            thread_id,
            user_id,
        )
        rows = []
        for message_id, role, content, turn in (
            (message_ids[0], "human", "first prompt", 1),
            (message_ids[1], "ai", "first answer", 1),
            (message_ids[2], "human", "second prompt", 2),
            (message_ids[3], "ai", "second answer", 2),
        ):
            rows.append(
                await conn.fetchrow(
                    "INSERT INTO thread_messages "
                    "(id,thread_id,role,content,turn_number) "
                    "VALUES ($1,$2,$3,$4,$5) RETURNING seq",
                    message_id,
                    thread_id,
                    role,
                    content,
                    turn,
                )
            )
        second_input_seq = int(rows[2]["seq"])
        await conn.execute(
            "INSERT INTO run_queue "
            "(unit_id,unit_kind,state,input_seq,consumed_seq) "
            "VALUES ($1,'session_turn','done',$2,$2)",
            thread_id,
            second_input_seq,
        )
    return {"id": str(user_id), "is_admin": False}, thread_id, message_ids


async def _preview_request(
    service: ThreadRewindService,
    user: dict,
    thread_id: UUID,
    message_id: UUID,
    *,
    client_request_id: UUID | None = None,
) -> StatelessRewindRequest:
    preview = await service.preview(str(thread_id), message_id, user)
    assert preview.eligible is True
    assert preview.expected is not None
    return StatelessRewindRequest(
        client_request_id=client_request_id or uuid4(),
        message_id=message_id,
        mode="conversation",
        expected=preview.expected,
    )


@pytest.mark.asyncio
async def test_rewind_commits_effect_receipt_and_preserves_queue(db):
    user, thread_id, message_ids = await _seed_idle_thread(db)
    service = ThreadRewindService(db, lambda: True)
    body = await _preview_request(service, user, thread_id, message_ids[2])

    result, duplicate = await service.apply(str(thread_id), body, user)

    assert duplicate is False
    assert result.prompt == "second prompt"
    assert result.swept_count == 2
    assert result.surviving_turn == 1
    assert result.conversation_revision == 1
    assert result.events_epoch == 1
    assert result.event_seq == "1"
    async with db.acquire() as conn:
        state = await conn.fetchrow(
            "SELECT conversation_revision,events_epoch,total_turns FROM threads "
            "WHERE id=$1",
            thread_id,
        )
        queue = await conn.fetchrow(
            "SELECT state,lease_token,input_seq,consumed_seq,control_input_seq,"
            "control_consumed_seq FROM run_queue WHERE unit_id=$1",
            thread_id,
        )
        hidden = await conn.fetchval(
            "SELECT COUNT(*) FROM thread_messages WHERE thread_id=$1 "
            "AND rewound_at IS NOT NULL",
            thread_id,
        )
        ledger = await conn.fetchrow(
            "SELECT request_payload,result_payload,event_epoch,event_seq "
            "FROM thread_rewinds WHERE id=$1",
            result.rewind_id,
        )
    assert dict(state) == {
        "conversation_revision": 1,
        "events_epoch": 1,
        "total_turns": 1,
    }
    assert queue["state"] == "done"
    assert queue["lease_token"] == 0
    assert queue["input_seq"] == queue["consumed_seq"]
    assert queue["control_input_seq"] == queue["control_consumed_seq"]
    assert hidden == 2
    assert ledger["event_epoch"] == 1
    assert ledger["event_seq"] == 1


@pytest.mark.asyncio
async def test_same_idempotency_key_replays_and_conflicting_body_refuses(db):
    user, thread_id, message_ids = await _seed_idle_thread(db)
    service = ThreadRewindService(db, lambda: True)
    body = await _preview_request(service, user, thread_id, message_ids[2])
    first, first_duplicate = await service.apply(str(thread_id), body, user)
    replay, replay_duplicate = await service.apply(str(thread_id), body, user)
    assert first_duplicate is False
    assert replay_duplicate is True
    assert replay == first

    conflict = body.model_copy(update={"message_id": message_ids[0]})
    with pytest.raises(RewindFailure) as caught:
        await service.apply(str(thread_id), conflict, user)
    assert caught.value.code == "rewind_idempotency_conflict"
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT COUNT(*) FROM thread_rewinds WHERE thread_id=$1", thread_id
            )
            == 1
        )


@pytest.mark.asyncio
async def test_two_distinct_requests_from_one_preview_apply_once(db):
    user, thread_id, message_ids = await _seed_idle_thread(db)
    service = ThreadRewindService(db, lambda: True)
    first = await _preview_request(service, user, thread_id, message_ids[2])
    second = first.model_copy(update={"client_request_id": uuid4()})

    outcomes = await asyncio.gather(
        service.apply(str(thread_id), first, user),
        service.apply(str(thread_id), second, user),
        return_exceptions=True,
    )
    applied = [outcome for outcome in outcomes if isinstance(outcome, tuple)]
    refused = [outcome for outcome in outcomes if isinstance(outcome, RewindFailure)]
    assert len(applied) == 1
    assert len(refused) == 1
    assert refused[0].code == "rewind_stale"
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT conversation_revision FROM threads WHERE id=$1", thread_id
            )
            == 1
        )


@pytest.mark.asyncio
async def test_busy_queue_and_pending_delivery_fail_without_effect(db):
    user, thread_id, message_ids = await _seed_idle_thread(db)
    service = ThreadRewindService(db, lambda: True)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE run_queue SET state='queued' WHERE unit_id=$1", thread_id
        )
    preview = await service.preview(str(thread_id), message_ids[2], user)
    assert preview.eligible is False
    assert preview.refusal_code == "queue_not_idle"
    assert preview.expected is None

    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE run_queue SET state='done' WHERE unit_id=$1", thread_id
        )
        delivery_id = uuid4()
        event_message_id = uuid4()
        await conn.execute(
            "INSERT INTO thread_messages "
            "(id,thread_id,role,content,turn_number) "
            "VALUES ($1,$2,'event','pending wake',3)",
            event_message_id,
            thread_id,
        )
        await conn.execute(
            "INSERT INTO thread_input_deliveries "
            "(delivery_id,thread_id,message_id,source,execution_lane,"
            "conversation_revision) VALUES ($1,$2,$3,'officer_wake','stateless',0)",
            delivery_id,
            thread_id,
            event_message_id,
        )
    preview = await service.preview(str(thread_id), message_ids[2], user)
    assert preview.eligible is False
    assert preview.refusal_code == "pending_input_delivery"
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT COUNT(*) FROM thread_rewinds WHERE thread_id=$1", thread_id
            )
            == 0
        )
        assert (
            await conn.fetchval(
                "SELECT COUNT(*) FROM thread_messages WHERE thread_id=$1 "
                "AND rewound_at IS NOT NULL",
                thread_id,
            )
            == 0
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("producer", "expected_reason"),
    [
        ("control", "pending_control"),
        ("interrupt", "pending_interrupt"),
        ("permission", "pending_permission"),
        ("active_job", "active_job"),
        ("job_wake", "pending_job_wake"),
        ("scheduled_wake", "pending_scheduled_wake"),
        ("child", "pending_child"),
        ("final_memory", "pending_final_memory"),
    ],
)
async def test_each_unsettled_producer_blocks_locked_apply(
    db, producer, expected_reason
):
    user, thread_id, message_ids = await _seed_idle_thread(db)
    service = ThreadRewindService(db, lambda: True)
    body = await _preview_request(service, user, thread_id, message_ids[2])

    async with db.acquire() as conn:
        if producer == "control":
            await conn.execute(
                "INSERT INTO thread_control_requests "
                "(thread_id,request_seq,client_request_id,verb,requested_by) "
                "VALUES ($1,1,$2,'mode.set',$3)",
                thread_id,
                uuid4(),
                user["id"],
            )
        elif producer == "interrupt":
            await conn.execute(
                "INSERT INTO thread_interrupt_requests "
                "(thread_id,client_request_id,target_turn_id,accepted_lease_token,"
                "accepted_leased_by,requested_by) VALUES ($1,$2,2,1,'pod-a',$3)",
                thread_id,
                uuid4(),
                user["id"],
            )
        elif producer == "permission":
            await conn.execute(
                "INSERT INTO thread_permission_requests "
                "(thread_id,tool_call_id,tool_name) VALUES ($1,$2,'write_file')",
                thread_id,
                f"tool-{uuid4()}",
            )
        elif producer in {"active_job", "job_wake"}:
            await conn.execute(
                "INSERT INTO jobs "
                "(description,status,user_id,created_by_thread_id,wake_on_complete,"
                "wake_state,wake_notified_status) VALUES ($1,$2,$3,$4,true,$5,$6)",
                producer,
                "processing" if producer == "active_job" else "completed",
                UUID(user["id"]),
                thread_id,
                "none" if producer == "active_job" else "pending",
                None,
            )
        elif producer == "scheduled_wake":
            await conn.execute(
                "INSERT INTO session_wake_events "
                "(thread_id,source,dedup_key,state) VALUES ($1,'timer',$2,'pending')",
                thread_id,
                str(uuid4()),
            )
        elif producer == "child":
            await conn.execute(
                "INSERT INTO threads "
                "(id,user_id,kind,parent_thread_id,status,execution_lane,"
                "subagent_status) VALUES "
                "($1,$2,'subagent',$3,'active','pinned','running')",
                uuid4(),
                UUID(user["id"]),
                thread_id,
            )
        elif producer == "final_memory":
            producer_id = uuid4()
            await conn.execute(
                "UPDATE thread_messages SET turn_execution_id=$2 WHERE id=$1",
                message_ids[2],
                producer_id,
            )
            await conn.execute(
                "INSERT INTO completion_effects "
                "(producer_kind,producer_id,scope_id,effect_name,effect_group,state) "
                "VALUES ('session_turn',$1,$2,'final_memory_extraction',"
                "'memory_extraction','pending')",
                producer_id,
                thread_id,
            )

    with pytest.raises(RewindFailure) as blocked:
        await service.apply(str(thread_id), body, user)
    assert blocked.value.code == "rewind_busy"
    assert blocked.value.reason == expected_reason

    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT COUNT(*) FROM thread_rewinds WHERE thread_id=$1", thread_id
            )
            == 0
        )
        assert (
            await conn.fetchval(
                "SELECT COUNT(*) FROM thread_messages WHERE thread_id=$1 "
                "AND rewound_at IS NOT NULL",
                thread_id,
            )
            == 0
        )


@pytest.mark.asyncio
async def test_disabled_gate_hides_new_admission_but_receipt_remains_readable(db):
    user, thread_id, message_ids = await _seed_idle_thread(db)
    enabled = ThreadRewindService(db, lambda: True)
    body = await _preview_request(enabled, user, thread_id, message_ids[2])
    result, _ = await enabled.apply(str(thread_id), body, user)

    disabled = ThreadRewindService(db, lambda: False)
    receipt = await disabled.receipt(str(thread_id), body.client_request_id, user)
    replay, duplicate = await disabled.apply(str(thread_id), body, user)
    assert receipt == result
    assert replay == result
    assert duplicate is True


@pytest.mark.asyncio
async def test_stale_preview_owner_and_runtime_transition_fail_without_effect(db):
    user, thread_id, message_ids = await _seed_idle_thread(db)
    service = ThreadRewindService(db, lambda: True)

    with pytest.raises(RewindFailure) as forbidden:
        await service.preview(
            str(thread_id),
            message_ids[2],
            {"id": str(uuid4()), "is_admin": False},
        )
    assert forbidden.value.status_code == 403

    body = await _preview_request(service, user, thread_id, message_ids[2])
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET events_epoch=events_epoch+1 WHERE id=$1", thread_id
        )
    with pytest.raises(RewindFailure) as stale:
        await service.apply(str(thread_id), body, user)
    assert stale.value.code == "rewind_stale"

    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET runtime_attach_token=$2 WHERE id=$1",
            thread_id,
            uuid4(),
        )
    preview = await service.preview(str(thread_id), message_ids[2], user)
    assert preview.eligible is False
    assert preview.refusal_code == "runtime_transition"

    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT COUNT(*) FROM thread_rewinds WHERE thread_id=$1", thread_id
            )
            == 0
        )
        assert (
            await conn.fetchval(
                "SELECT COUNT(*) FROM thread_messages WHERE thread_id=$1 "
                "AND rewound_at IS NOT NULL",
                thread_id,
            )
            == 0
        )


@pytest.mark.asyncio
async def test_malformed_or_officer_metadata_is_not_advertised_as_eligible(db):
    user, thread_id, message_ids = await _seed_idle_thread(db)
    service = ThreadRewindService(db, lambda: True)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET metadata='[]'::jsonb WHERE id=$1", thread_id
        )
    malformed = await service.preview(str(thread_id), message_ids[2], user)
    assert malformed.eligible is False
    assert malformed.refusal_code == "malformed_thread_metadata"

    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET metadata="
            '\'{"config_override":{"officer":{"enabled":true}}}\'::jsonb '
            "WHERE id=$1",
            thread_id,
        )
    officer = await service.preview(str(thread_id), message_ids[2], user)
    assert officer.eligible is False
    assert officer.refusal_code == "unsupported_session_class"
