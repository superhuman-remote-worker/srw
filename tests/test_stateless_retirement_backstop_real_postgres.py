"""Real-PostgreSQL proof that the cleanup backstop defers to stateless End.

Begin's terminal transition (migration 0198) admits a cleanup intent for the
exact live runtime in the same transaction that closes the queue and records
the retirement marker.  The lifecycle reconciler's backstop lists pending
intents every tick.  While the explicit End/Delete/Resume owner still owes the
resident drain and shell retirement for that runtime, the backstop must not
claim the intent: deleting the Pod first destroys the only object that can
produce those proofs (R1.B12 thread ``bff692dc``: backstop at +58 s, then a
permanent ``503 runtime authority changed or is ambiguous``).

The same file pins the recovery path for a row whose Pod the backstop already
deleted: the finalizer release's exact ``stateless_workspace`` process-zero
receipt, and only that receipt, may stand in for the observed terminal Pod.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from shared.session_retirement import stateless_retirement_authority


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
        pytest.skip(f"local Postgres container unavailable: {exc}")
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
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=4)
    await store.connect()
    try:
        yield store
    finally:
        await store.close()


async def _live_stateless_thread(db: PostgresDB, *, queue_token: int = 4) -> dict:
    """A settled, Ready stateless sandbox workspace after one completed turn."""

    thread_id = uuid4()
    runtime_uid = str(uuid4())
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id, status, execution_lane) "
            "VALUES ($1, 'awaiting_user', 'stateless')",
            thread_id,
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(thread_id),
        owner_kind="thread",
        scope="workspace_container",
        claimant="stateless-ensure",
        desired_manifest_digest="0" * 64,
    )
    reservation = await db.mark_managed_repository_workspace_creation_started(
        str(thread_id),
        owner_kind="thread",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="stateless-ensure",
        claim_token=int(reservation["claim_token"]),
    )
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(thread_id),
        owner_kind="thread",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="stateless-ensure",
        claim_token=int(reservation["claim_token"]),
        runtime_incarnation=runtime_uid,
    )
    generation = str(uuid4())
    state = {
        "workspace_container": {
            "provisioner": "k8s",
            "status": "ready",
            "_runtime_incarnation": runtime_uid,
            "_creation_reservation_id": str(reservation["id"]),
            "_creation_claim_token": str(reservation["claim_token"]),
            "pod_name": f"ws-thread-{str(thread_id)[:12]}",
            "namespace": "srw",
            "pod_ip": "10.42.0.31",
            "host": "10.42.0.31",
            "port": 30022,
            "_canvas_workspace_generation": generation,
        },
        "_workspace_binding": {
            "generation": generation,
            "kind": "remote",
            "backing_id": f"k8s-pvc:srw:pvc-ws-thread-{str(thread_id)[:12]}",
            "ssh_host_key_fingerprint": "SHA256:successor-host-key",
        },
    }
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET metadata = $2::jsonb WHERE id = $1",
            thread_id,
            json.dumps(state),
        )
        await conn.execute(
            "INSERT INTO run_queue (unit_id, unit_kind, state, lease_token) "
            "VALUES ($1, 'session_turn', 'done', $2)",
            thread_id,
            queue_token,
        )
    assert await db.settle_managed_repository_workspace_creation_reservation(
        str(thread_id),
        owner_kind="thread",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="stateless-ensure",
        claim_token=int(reservation["claim_token"]),
        runtime_incarnation=runtime_uid,
    )
    return {
        "thread_id": str(thread_id),
        "runtime": runtime_uid,
        "generation": generation,
        "fingerprint": "SHA256:successor-host-key",
    }


def _backstop_ids(rows: list[dict]) -> set[str]:
    return {str(row["id"]) for row in rows}


async def _terminal_intent(db: PostgresDB, thread: dict) -> dict:
    intent = await db.get_managed_repository_workspace_cleanup_intent(
        thread["thread_id"],
        owner_kind="thread",
        scope="workspace_container",
        runtime_incarnation=thread["runtime"],
    )
    assert isinstance(intent, dict)
    return intent


async def _begin(db: PostgresDB, thread: dict, *, permanent: bool) -> dict:
    closure = await db.begin_stateless_thread_workspace_retirement(
        thread["thread_id"], force=False, permanent=permanent
    )
    assert closure["state"] == "closed"
    assert closure["resident_cleanup_required"] is True
    assert closure["resident_acknowledged"] is False
    return closure


class TestBackstopDefersToTheStatelessRetirementOwner:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("permanent", [False, True])
    async def test_begin_admits_an_intent_the_backstop_must_not_list(
        self, db, permanent
    ):
        thread = await _live_stateless_thread(db)
        await _begin(db, thread, permanent=permanent)
        intent = await _terminal_intent(db, thread)
        assert intent["settled_at"] is None

        listed = await db.list_pending_managed_repository_workspace_cleanup_intents(
            limit=500
        )

        assert str(intent["id"]) not in _backstop_ids(listed)

    @pytest.mark.asyncio
    async def test_resident_proof_alone_keeps_the_backstop_out(self, db):
        thread = await _live_stateless_thread(db)
        closure = await _begin(db, thread, permanent=True)
        assert await db.acknowledge_stateless_thread_resident_retirement(
            thread["thread_id"],
            terminal_token=int(closure["terminal_token"]),
            workspace_generation=thread["generation"],
            endpoint_generation=thread["generation"],
            runtime_incarnation=thread["runtime"],
            host_key_fingerprint=thread["fingerprint"],
            proof={"browser_processes": 0},
        )
        intent = await _terminal_intent(db, thread)

        listed = await db.list_pending_managed_repository_workspace_cleanup_intents(
            limit=500
        )

        assert str(intent["id"]) not in _backstop_ids(listed)

    @pytest.mark.asyncio
    async def test_both_protocol_proofs_return_the_intent_to_the_backstop(self, db):
        thread = await _live_stateless_thread(db)
        closure = await _begin(db, thread, permanent=True)
        token = int(closure["terminal_token"])
        assert await db.acknowledge_stateless_thread_resident_retirement(
            thread["thread_id"],
            terminal_token=token,
            workspace_generation=thread["generation"],
            endpoint_generation=thread["generation"],
            runtime_incarnation=thread["runtime"],
            host_key_fingerprint=thread["fingerprint"],
            proof={"browser_processes": 0},
        )
        assert await db.acknowledge_stateless_thread_shell_retirement(
            thread["thread_id"],
            terminal_token=token,
            workspace_generation=thread["generation"],
            endpoint_generation=thread["generation"],
            runtime_incarnation=thread["runtime"],
            host_key_fingerprint=thread["fingerprint"],
        )
        intent = await _terminal_intent(db, thread)

        listed = await db.list_pending_managed_repository_workspace_cleanup_intents(
            limit=500
        )

        assert str(intent["id"]) in _backstop_ids(listed)

    @pytest.mark.asyncio
    async def test_observed_terminal_runtime_returns_the_intent_to_the_backstop(
        self, db
    ):
        thread = await _live_stateless_thread(db)
        closure = await _begin(db, thread, permanent=False)
        assert await db.acknowledge_stateless_thread_shell_absent(
            thread["thread_id"],
            terminal_token=int(closure["terminal_token"]),
            runtime_incarnation=thread["runtime"],
        )
        intent = await _terminal_intent(db, thread)

        listed = await db.list_pending_managed_repository_workspace_cleanup_intents(
            limit=500
        )

        assert str(intent["id"]) in _backstop_ids(listed)

    @pytest.mark.asyncio
    async def test_the_explicit_owner_can_still_claim_its_deferred_intent(self, db):
        thread = await _live_stateless_thread(db)
        await _begin(db, thread, permanent=True)
        intent = await _terminal_intent(db, thread)

        claimed = await db.claim_managed_repository_workspace_cleanup_intent(
            str(intent["id"]), claimant="explicit-end-owner", lease_seconds=300
        )

        assert isinstance(claimed, dict)
        assert claimed["claimed_by"] == "explicit-end-owner"

    @pytest.mark.asyncio
    async def test_deferral_is_bounded_to_the_marker_runtime_and_lane(self, db):
        """Job and ownerless intents keep their existing backstop behaviour."""

        thread = await _live_stateless_thread(db)
        await _begin(db, thread, permanent=True)
        deferred = await _terminal_intent(db, thread)
        job_id = uuid4()
        async with db.acquire() as conn:
            await conn.execute(
                "INSERT INTO jobs (id, description, status) "
                "VALUES ($1, 'job workspace', 'paused')",
                job_id,
            )
        job_runtime = str(uuid4())
        job_intent = await db.prepare_managed_repository_workspace_cleanup_intent(
            str(job_id),
            owner_kind="job",
            scope="workspace_container",
            runtime_incarnation=job_runtime,
            target_disposition="deleted",
            reclaim_shared_resources=False,
            allow_orphan=True,
            admission_source="explicit",
        )
        assert isinstance(job_intent, dict)

        listed = _backstop_ids(
            await db.list_pending_managed_repository_workspace_cleanup_intents(
                limit=500
            )
        )

        assert str(deferred["id"]) not in listed
        assert str(job_intent["id"]) in listed


class TestAbsentRuntimeRecoveryRequiresTheExactReceipt:
    """Recovery for a row whose Pod was deleted before End's proofs."""

    @pytest.mark.asyncio
    async def test_exact_receipt_acknowledges_both_stages(self, db):
        thread = await _live_stateless_thread(db)
        closure = await _begin(db, thread, permanent=True)
        # The finalizer release records this only after it observed every
        # container of the exact Pod UID terminated.
        assert await db.record_stateless_thread_workspace_process_zero(
            thread["thread_id"], runtime_incarnation=thread["runtime"]
        )

        assert await db.acknowledge_stateless_thread_runtime_process_zero(
            thread["thread_id"],
            terminal_token=int(closure["terminal_token"]),
            runtime_incarnation=thread["runtime"],
        )

        row = await db.get_thread(thread["thread_id"])
        metadata = row["metadata"]
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        marker = stateless_retirement_authority(metadata)
        assert marker["residents_retired"] is True
        assert marker["remote_retired"] is True
        assert marker["residents_retired_by"] == "workspace_runtime_terminal"
        assert metadata["_stateless_resident_retirement_ack"]["evidence"] == (
            "process_zero_receipt"
        )

    @pytest.mark.asyncio
    async def test_absence_without_a_receipt_is_not_proof(self, db):
        thread = await _live_stateless_thread(db)
        closure = await _begin(db, thread, permanent=True)

        assert not await db.acknowledge_stateless_thread_runtime_process_zero(
            thread["thread_id"],
            terminal_token=int(closure["terminal_token"]),
            runtime_incarnation=thread["runtime"],
        )

    @pytest.mark.asyncio
    async def test_a_predecessor_receipt_cannot_satisfy_the_successor(self, db):
        thread = await _live_stateless_thread(db)
        closure = await _begin(db, thread, permanent=True)
        predecessor = str(uuid4())
        async with db.acquire() as conn:
            # A's receipt from its own earlier End, exactly as its finalizer
            # release left it: same thread and scope, different runtime UID.
            await conn.execute(
                "INSERT INTO managed_repository_process_zero_receipts "
                "(owner_kind, owner_id, scope, provisioner, runtime_incarnation) "
                "VALUES ('thread', $1, 'stateless_workspace', 'k8s', $2)",
                UUID(thread["thread_id"]),
                predecessor,
            )

        assert not await db.acknowledge_stateless_thread_runtime_process_zero(
            thread["thread_id"],
            terminal_token=int(closure["terminal_token"]),
            runtime_incarnation=thread["runtime"],
        )

    @pytest.mark.asyncio
    async def test_a_generic_workspace_receipt_is_not_container_termination(self, db):
        """The resident SSH proof writes this scope; it is not process zero."""

        thread = await _live_stateless_thread(db)
        closure = await _begin(db, thread, permanent=True)
        assert await db.record_managed_repository_workspace_process_zero(
            thread["thread_id"],
            owner_kind="thread",
            scope="workspace_container",
            provisioner="k8s",
            runtime_incarnation=thread["runtime"],
        )

        assert not await db.acknowledge_stateless_thread_runtime_process_zero(
            thread["thread_id"],
            terminal_token=int(closure["terminal_token"]),
            runtime_incarnation=thread["runtime"],
        )

    @pytest.mark.asyncio
    async def test_a_stale_terminal_token_cannot_use_the_receipt(self, db):
        thread = await _live_stateless_thread(db)
        closure = await _begin(db, thread, permanent=True)
        assert await db.record_stateless_thread_workspace_process_zero(
            thread["thread_id"], runtime_incarnation=thread["runtime"]
        )

        assert not await db.acknowledge_stateless_thread_runtime_process_zero(
            thread["thread_id"],
            terminal_token=int(closure["terminal_token"]) - 1,
            runtime_incarnation=thread["runtime"],
        )
