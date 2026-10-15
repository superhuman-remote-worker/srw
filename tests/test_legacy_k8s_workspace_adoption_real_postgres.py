"""Real-PostgreSQL proof for the bounded legacy Kubernetes adoption bridge.

Migrations 0197-0200 give every Kubernetes workspace runtime a durable
reservation and an exact Pod UID.  Rows the previous release wrote have
neither: a ready endpoint and, for a session, a create marker under its older
name.  This module proves the one path that converts such a row -- an `adopt`
generation on the 0198 reservation ledger, published only from an external
Kubernetes attestation -- and proves that everything else about it stays fail
closed.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.session_workspace_adoption import (
    LEGACY_CREATION_MARKER_KEY,
    ensure_legacy_k8s_thread_runtime_authority,
)
from orchestrator.services.job_workspace_adoption import (
    LegacyK8sAdoptionOutcome,
    ensure_legacy_k8s_job_runtime_authority,
)
from shared.workspace_contract import LEGACY_K8S_RUNTIME_ADOPTION_KEY
from tests._previous_release_seed import seed_previous_release_row

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)

HISTORICAL_ENDPOINT = {
    "status": "ready",
    "provisioner": "k8s",
    "host": "ws-thread.internal",
    "pod_ip": "10.42.3.21",
    "port": 30022,
}


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
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=6)
    await store.connect()
    try:
        yield store
    finally:
        await store.close()


def _attestation(*, runtime_incarnation=None, host=None, pod_ip=None):
    from orchestrator.services.container_provisioner import WorkspaceRuntimeAttestation

    backing = str(uuid4())
    return WorkspaceRuntimeAttestation(
        backing_id=f"k8s-pvc:test:{backing}",
        workspace_generation=backing,
        runtime_incarnation=runtime_incarnation or str(uuid4()),
        ssh_host_key_fingerprint="SHA256:" + ("b" * 43),
        host=host or HISTORICAL_ENDPOINT["host"],
        pod_ip=pod_ip or HISTORICAL_ENDPOINT["pod_ip"],
        port=HISTORICAL_ENDPOINT["port"],
    )


async def _seed_previous_release_session(db, *, marker=True, extra=None, status=None):
    """Persist the exact session workspace JSON the prior release emitted."""

    thread_id = await db.create_thread()
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET execution_lane='stateless' WHERE id=$1",
            UUID(thread_id),
        )
        generation = await conn.fetchval(
            "SELECT runtime_generation FROM threads WHERE id=$1", UUID(thread_id)
        )
        workspace = dict(HISTORICAL_ENDPOINT)
        if marker:
            # The name the previous release used for the same four fields.
            workspace[LEGACY_CREATION_MARKER_KEY] = {
                "generation": str(generation),
                "mode": "create",
                "attempted": True,
                "replaces_uid": None,
            }
        metadata = {"workspace_container": workspace}
        if extra:
            metadata.update(extra)
        await seed_previous_release_row(
            conn,
            "threads",
            "UPDATE threads SET metadata=$2::jsonb"
            + (", status=$3" if status else "")
            + " WHERE id=$1",
            *(
                (UUID(thread_id), json.dumps(metadata), status)
                if status
                else (UUID(thread_id), json.dumps(metadata))
            ),
        )
    return await db.get_thread(thread_id)


async def _sole_reservation(db, owner_kind, owner_id):
    async with db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM managed_repository_workspace_creation_reservations "
            "WHERE owner_kind=$1 AND owner_id=$2 ORDER BY reservation_generation",
            owner_kind,
            owner_id,
        )
    assert len(rows) == 1, [dict(row) for row in rows]
    return dict(rows[0])


def _workspace_of(thread):
    metadata = thread["metadata"]
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    return metadata["workspace_container"]


class _PausingAdoptionDB:
    """Pause one lagging adopter at a selected ledger boundary."""

    def __init__(self, delegate, pause_at):
        self._delegate = delegate
        self._pause_at = pause_at
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    def __getattr__(self, name):
        return getattr(self._delegate, name)

    async def _pause(self, boundary):
        if self._pause_at == boundary:
            self.entered.set()
            await self.release.wait()

    async def reserve_managed_repository_workspace_creation(self, *args, **kwargs):
        await self._pause("reservation")
        return await self._delegate.reserve_managed_repository_workspace_creation(
            *args, **kwargs
        )

    async def authorize_managed_repository_workspace_creation_runtime(
        self, *args, **kwargs
    ):
        await self._pause("runtime-authorization")
        return await self._delegate.authorize_managed_repository_workspace_creation_runtime(
            *args, **kwargs
        )


@pytest.mark.asyncio
async def test_previous_release_session_adopts_once_from_live_attestation(db):
    thread = await _seed_previous_release_session(db)
    inventory = await db.list_uidless_k8s_thread_workspace_rows()
    assert [str(row["id"]) for row in inventory] == [str(thread["id"])]
    attestation = _attestation()
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(return_value=attestation)
    )

    result = await ensure_legacy_k8s_thread_runtime_authority(db, provisioner, thread)

    assert result.outcome is LegacyK8sAdoptionOutcome.ADOPTED
    workspace = _workspace_of(await db.get_thread(str(thread["id"])))
    assert workspace["_runtime_incarnation"] == attestation.runtime_incarnation
    assert workspace[LEGACY_K8S_RUNTIME_ADOPTION_KEY] == {
        "version": 1,
        "runtime_incarnation": attestation.runtime_incarnation,
        "workspace_generation": attestation.workspace_generation,
        "ssh_host_key_fingerprint": attestation.ssh_host_key_fingerprint,
    }
    # The historical create marker is gone: an old replica must not be able to
    # read it back as live create authority for the adopted generation.
    assert LEGACY_CREATION_MARKER_KEY not in workspace
    assert await db.list_uidless_k8s_thread_workspace_rows() == []

    reservation = await _sole_reservation(db, "thread", thread["id"])
    assert reservation["operation_kind"] == "adopt"
    assert reservation["result_kind"] == "settled"
    assert reservation["external_mutation_started_at"] is None
    assert str(reservation["pod_uid"]) == attestation.runtime_incarnation


@pytest.mark.asyncio
async def test_identical_and_lost_response_retries_converge_on_one_authority(db):
    thread = await _seed_previous_release_session(db)
    attestation = _attestation()
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(return_value=attestation)
    )

    first = await ensure_legacy_k8s_thread_runtime_authority(db, provisioner, thread)
    # A lost response is indistinguishable from never having called: the exact
    # same request runs again against the row it already wrote.
    replay = await ensure_legacy_k8s_thread_runtime_authority(db, provisioner, thread)
    fresh = await ensure_legacy_k8s_thread_runtime_authority(
        db, provisioner, await db.get_thread(str(thread["id"]))
    )

    assert first.outcome is LegacyK8sAdoptionOutcome.ADOPTED
    assert replay.outcome is LegacyK8sAdoptionOutcome.CONVERGED
    assert fresh.outcome is LegacyK8sAdoptionOutcome.CONVERGED
    workspace = _workspace_of(await db.get_thread(str(thread["id"])))
    assert workspace["_runtime_incarnation"] == attestation.runtime_incarnation
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM "
                "managed_repository_workspace_creation_reservations "
                "WHERE owner_kind='thread' AND owner_id=$1",
                thread["id"],
            )
            == 1
        )


@pytest.mark.asyncio
async def test_concurrent_adopters_mint_one_generation_and_one_stamp(db):
    thread = await _seed_previous_release_session(db)
    attestation = _attestation()
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(return_value=attestation)
    )

    outcomes = await asyncio.gather(
        ensure_legacy_k8s_thread_runtime_authority(db, provisioner, thread),
        ensure_legacy_k8s_thread_runtime_authority(db, provisioner, thread),
    )

    assert {outcome.outcome for outcome in outcomes} <= {
        LegacyK8sAdoptionOutcome.ADOPTED,
        LegacyK8sAdoptionOutcome.CONVERGED,
    }
    assert LegacyK8sAdoptionOutcome.ADOPTED in {o.outcome for o in outcomes}
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM "
                "managed_repository_workspace_creation_reservations "
                "WHERE owner_kind='thread' AND owner_id=$1",
                thread["id"],
            )
            == 1
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pause_at",
    ["reservation", "runtime-authorization"],
)
async def test_lagging_concurrent_adopter_converges_after_winner_settles(db, pause_at):
    """A ledger refusal after an equal winner settles is not a fresh retry."""

    thread = await _seed_previous_release_session(db)
    attestation = _attestation()
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(return_value=attestation)
    )
    lagging_db = _PausingAdoptionDB(db, pause_at)
    lagging = asyncio.create_task(
        ensure_legacy_k8s_thread_runtime_authority(lagging_db, provisioner, thread)
    )
    await asyncio.wait_for(lagging_db.entered.wait(), timeout=2)

    winner = await ensure_legacy_k8s_thread_runtime_authority(db, provisioner, thread)
    lagging_db.release.set()
    lagged = await asyncio.wait_for(lagging, timeout=2)

    assert winner.outcome is LegacyK8sAdoptionOutcome.ADOPTED
    assert lagged.outcome is LegacyK8sAdoptionOutcome.CONVERGED
    reservation = await _sole_reservation(db, "thread", thread["id"])
    assert reservation["result_kind"] == "settled"


@pytest.mark.asyncio
async def test_pod_replaced_after_persistence_withdraws_the_tentative_stamp(db):
    thread = await _seed_previous_release_session(db)
    predecessor = _attestation()
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(
            side_effect=[predecessor, predecessor, _attestation()]
        )
    )

    result = await ensure_legacy_k8s_thread_runtime_authority(db, provisioner, thread)

    assert result.outcome is LegacyK8sAdoptionOutcome.RETRY
    assert result.reason == "kubernetes_runtime_changed_after_persistence"
    workspace = _workspace_of(await db.get_thread(str(thread["id"])))
    assert "_runtime_incarnation" not in workspace
    assert "_creation_reservation_id" not in workspace
    assert LEGACY_K8S_RUNTIME_ADOPTION_KEY not in workspace
    # The withdrawn generation is closed, so the next round starts clean
    # instead of colliding with a stale runtime binding.
    assert (await _sole_reservation(db, "thread", thread["id"]))[
        "result_kind"
    ] == "aborted"

    replacement = _attestation()
    provisioner.attest_workspace_runtime = AsyncMock(return_value=replacement)
    retry = await ensure_legacy_k8s_thread_runtime_authority(
        db, provisioner, await db.get_thread(str(thread["id"]))
    )
    assert retry.outcome is LegacyK8sAdoptionOutcome.ADOPTED
    assert (
        _workspace_of(await db.get_thread(str(thread["id"])))["_runtime_incarnation"]
        == replacement.runtime_incarnation
    )


@pytest.mark.asyncio
async def test_absent_pod_leaves_the_historical_row_untouched(db):
    thread = await _seed_previous_release_session(db)
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(side_effect=RuntimeError("no such Pod"))
    )

    result = await ensure_legacy_k8s_thread_runtime_authority(db, provisioner, thread)

    assert result.outcome is LegacyK8sAdoptionOutcome.RETRY
    assert result.reason == "kubernetes_attestation_unavailable"
    workspace = _workspace_of(await db.get_thread(str(thread["id"])))
    assert workspace == _workspace_of(thread)
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM "
                "managed_repository_workspace_creation_reservations "
                "WHERE owner_kind='thread' AND owner_id=$1",
                thread["id"],
            )
            == 0
        )


@pytest.mark.asyncio
async def test_disagreeing_proofs_never_publish_a_uid(db):
    thread = await _seed_previous_release_session(db)
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(side_effect=[_attestation(), _attestation()])
    )

    result = await ensure_legacy_k8s_thread_runtime_authority(db, provisioner, thread)

    assert result.outcome is LegacyK8sAdoptionOutcome.RETRY
    assert result.reason == "kubernetes_runtime_changed"
    assert "_runtime_incarnation" not in _workspace_of(
        await db.get_thread(str(thread["id"]))
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param({"execution_lane": "pinned"}, id="pinned-lane"),
        pytest.param({"status": "ended"}, id="terminal-owner"),
    ],
)
async def test_foreign_lane_or_terminal_owner_is_never_adopted(db, mutation):
    thread = await _seed_previous_release_session(db)
    async with db.acquire() as conn:
        for column, value in mutation.items():
            await seed_previous_release_row(
                conn,
                "threads",
                f"UPDATE threads SET {column}=$2 WHERE id=$1",
                thread["id"],
                value,
            )
    provisioner = SimpleNamespace(attest_workspace_runtime=AsyncMock())

    result = await ensure_legacy_k8s_thread_runtime_authority(
        db, provisioner, await db.get_thread(str(thread["id"]))
    )

    assert result.outcome is LegacyK8sAdoptionOutcome.NOT_NEEDED
    provisioner.attest_workspace_runtime.assert_not_awaited()


@pytest.mark.asyncio
async def test_retiring_session_is_never_adopted(db):
    thread = await _seed_previous_release_session(
        db, extra={"_stateless_workspace_retirement_pending": True}
    )
    provisioner = SimpleNamespace(attest_workspace_runtime=AsyncMock())

    result = await ensure_legacy_k8s_thread_runtime_authority(db, provisioner, thread)

    assert result.outcome is LegacyK8sAdoptionOutcome.NOT_NEEDED
    provisioner.attest_workspace_runtime.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_foreign_generation_or_snapshot_cannot_win_the_adoption_cas(db):
    """The CAS is the fence a retirement/recycle race actually runs into."""

    thread = await _seed_previous_release_session(db)
    workspace = _workspace_of(thread)
    adopted = dict(workspace)
    adopted["_runtime_incarnation"] = str(uuid4())

    assert not await db.adopt_legacy_k8s_thread_workspace_runtime(
        str(thread["id"]),
        expected_status=str(thread["status"]),
        expected_execution_lane="stateless",
        expected_runtime_generation=str(uuid4()),
        expected_workspace=workspace,
        adopted_workspace=adopted,
    )
    assert not await db.adopt_legacy_k8s_thread_workspace_runtime(
        str(thread["id"]),
        expected_status="ended",
        expected_execution_lane="stateless",
        expected_runtime_generation=str(thread["runtime_generation"]),
        expected_workspace=workspace,
        adopted_workspace=adopted,
    )
    assert not await db.adopt_legacy_k8s_thread_workspace_runtime(
        str(thread["id"]),
        expected_status=str(thread["status"]),
        expected_execution_lane="pinned",
        expected_runtime_generation=str(thread["runtime_generation"]),
        expected_workspace=workspace,
        adopted_workspace=adopted,
    )
    assert not await db.adopt_legacy_k8s_thread_workspace_runtime(
        str(thread["id"]),
        expected_status=str(thread["status"]),
        expected_execution_lane="stateless",
        expected_runtime_generation=str(thread["runtime_generation"]),
        expected_workspace={**workspace, "pod_ip": "10.42.9.9"},
        adopted_workspace=adopted,
    )
    assert "_runtime_incarnation" not in _workspace_of(
        await db.get_thread(str(thread["id"]))
    )


@pytest.mark.asyncio
async def test_the_historical_marker_cannot_be_restored_after_conversion(db):
    thread = await _seed_previous_release_session(db)
    attestation = _attestation()
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(return_value=attestation)
    )
    adopted = await ensure_legacy_k8s_thread_runtime_authority(db, provisioner, thread)
    assert adopted.outcome is LegacyK8sAdoptionOutcome.ADOPTED

    # An old replica writing its create marker back onto the adopted row is a
    # resurrection attempt: the reader refuses to progress on any row that
    # carries the historical key alongside current authority.
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET metadata = jsonb_set(metadata, "
            "'{workspace_container," + LEGACY_CREATION_MARKER_KEY + "}', $2::jsonb) "
            "WHERE id = $1",
            thread["id"],
            json.dumps(
                {
                    "generation": str(thread["runtime_generation"]),
                    "mode": "create",
                    "attempted": False,
                    "replaces_uid": None,
                }
            ),
        )
    state = await db.prepare_stateless_thread_workspace_creation(
        str(thread["id"]),
        proposed_generation=str(thread["runtime_generation"]),
        mode="create",
    )
    assert state["state"] == "blocked"
    assert "contradictory" in state["reason"] or "predates" in state["reason"]


@pytest.mark.asyncio
async def test_owner_delete_stays_fenced_while_adoption_authority_is_active(db):
    thread = await _seed_previous_release_session(db)
    attestation = _attestation()
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(return_value=attestation)
    )
    assert (
        await ensure_legacy_k8s_thread_runtime_authority(db, provisioner, thread)
    ).outcome is LegacyK8sAdoptionOutcome.ADOPTED

    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as raised:
            await conn.execute("DELETE FROM threads WHERE id=$1", thread["id"])
    assert "cleanup" in (raised.value.constraint_name or "")


@pytest.mark.asyncio
async def test_adopt_is_refused_for_anything_but_the_historical_shape(db):
    """`adopt` is not a general escape hatch from the create fence."""

    thread = await _seed_previous_release_session(db)
    digest = "c" * 64
    # A current, UID-bearing row.
    attestation = _attestation()
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(return_value=attestation)
    )
    assert (
        await ensure_legacy_k8s_thread_runtime_authority(db, provisioner, thread)
    ).outcome is LegacyK8sAdoptionOutcome.ADOPTED

    assert (
        await db.reserve_managed_repository_workspace_creation(
            str(thread["id"]),
            owner_kind="thread",
            scope="workspace_container",
            claimant="probe",
            operation_kind="adopt",
            desired_manifest_digest=digest,
        )
        is None
    )

    # A brand new session with no projection at all is a create, not an adopt.
    empty = await db.create_thread()
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET execution_lane='stateless' WHERE id=$1", UUID(empty)
        )
    assert (
        await db.reserve_managed_repository_workspace_creation(
            empty,
            owner_kind="thread",
            scope="workspace_container",
            claimant="probe",
            operation_kind="adopt",
            desired_manifest_digest=digest,
        )
        is None
    )


@pytest.mark.asyncio
async def test_job_and_session_adoption_share_one_ledger_contract(db):
    """The job bridge writes the same generation shape as the session one."""

    job = await db.create_job(
        description="pre-0197 Kubernetes job runtime",
        config_override={"workspace": {"backend": "sandbox"}},
    )
    async with db.acquire() as conn:
        await seed_previous_release_row(
            conn,
            "jobs",
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
            job["id"],
            json.dumps({"workspace_container": dict(HISTORICAL_ENDPOINT)}),
        )
    attestation = _attestation()
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(return_value=attestation)
    )

    result = await ensure_legacy_k8s_job_runtime_authority(
        db, provisioner, await db.get_job(str(job["id"]))
    )

    assert result.outcome is LegacyK8sAdoptionOutcome.ADOPTED
    reservation = await _sole_reservation(db, "job", job["id"])
    assert reservation["operation_kind"] == "adopt"
    assert reservation["result_kind"] == "settled"
    assert reservation["external_mutation_started_at"] is None
    assert str(reservation["pod_uid"]) == attestation.runtime_incarnation


@pytest.mark.asyncio
async def test_per_owner_annotation_classifies_like_the_activation_inventory(db):
    """The per-owner cleanup gate agrees with the inventory, row by row."""

    before = await db.managed_repository_workspace_cleanup_activation_inventory()
    job = await db.create_job(
        description="pre-0197 Kubernetes job runtime",
        config_override={"workspace": {"backend": "sandbox"}},
    )
    job_id = str(job["id"])
    async with db.acquire() as conn:
        await seed_previous_release_row(
            conn,
            "jobs",
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
            job["id"],
            json.dumps({"workspace_container": dict(HISTORICAL_ENDPOINT)}),
        )

    assert (
        await db.managed_repository_workspace_runtime_is_unannotated(
            job_id, owner_kind="job", scope="workspace_container"
        )
        is True
    )
    seeded = await db.managed_repository_workspace_cleanup_activation_inventory()
    assert (
        seeded["unannotated_runtime_count"] == before["unannotated_runtime_count"] + 1
    )
    # The same job has no IDE projection, so that scope has nothing to guard.
    assert (
        await db.managed_repository_workspace_runtime_is_unannotated(
            job_id, owner_kind="job", scope="ide"
        )
        is False
    )

    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(return_value=_attestation())
    )
    result = await ensure_legacy_k8s_job_runtime_authority(
        db, provisioner, await db.get_job(job_id)
    )
    assert result.outcome is LegacyK8sAdoptionOutcome.ADOPTED

    assert (
        await db.managed_repository_workspace_runtime_is_unannotated(
            job_id, owner_kind="job", scope="workspace_container"
        )
        is False
    )
    adopted = await db.managed_repository_workspace_cleanup_activation_inventory()
    assert adopted["unannotated_runtime_count"] == before["unannotated_runtime_count"]


@pytest.mark.asyncio
async def test_per_owner_annotation_without_a_managed_runtime(db):
    thread = await _seed_previous_release_session(db)
    thread_id = str(thread["id"])
    assert (
        await db.managed_repository_workspace_runtime_is_unannotated(
            thread_id, owner_kind="thread", scope="workspace_container"
        )
        is True
    )
    # An absent owner keeps orphan admission's own process-zero requirement.
    assert (
        await db.managed_repository_workspace_runtime_is_unannotated(
            str(uuid4()), owner_kind="job", scope="workspace_container"
        )
        is False
    )
    plain = await db.create_job(description="no workspace runtime yet")
    assert (
        await db.managed_repository_workspace_runtime_is_unannotated(
            str(plain["id"]), owner_kind="job", scope="workspace_container"
        )
        is False
    )
    # Unknown shapes fail closed.
    for owner_id, owner_kind, scope in (
        (thread_id, "thread", "ide"),
        (thread_id, "session", "workspace_container"),
        ("not-a-uuid", "job", "workspace_container"),
    ):
        assert (
            await db.managed_repository_workspace_runtime_is_unannotated(
                owner_id, owner_kind=owner_kind, scope=scope
            )
            is True
        )
