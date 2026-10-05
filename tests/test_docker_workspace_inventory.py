"""Durable static-Docker workspace inventory and quarantine regressions."""

from __future__ import annotations

import asyncio
import copy
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from orchestrator.database.postgres import PostgresDB


JOB_A = "11111111-1111-4111-8111-111111111111"
JOB_B = "22222222-2222-4222-8222-222222222222"
THREAD_A = "33333333-3333-4333-8333-333333333333"
THREAD_B = "44444444-4444-4444-8444-444444444444"
FINGERPRINT = "SHA256:durable-inventory-test"
RUNTIME_GENERATION = "55555555-5555-4555-8555-555555555555"


def _compact(query: str) -> str:
    return " ".join(query.split())


class _InventoryConnection:
    """Small transactional asyncpg seam for the lease helper contract."""

    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, Any]] = {}
        self.threads: dict[str, dict[str, Any]] = {}
        self.authorized_thread_deletes: set[str] = set()
        self.inventory: dict[tuple[str, int], dict[str, Any]] = {}
        self.process_zero_receipts: set[tuple[str, str, str]] = set()
        self.lock = asyncio.Lock()
        self.fail_owner_update_once = False

    @asynccontextmanager
    async def transaction(self):
        async with self.lock:
            before = (
                copy.deepcopy(self.jobs),
                copy.deepcopy(self.threads),
                copy.deepcopy(self.inventory),
                copy.deepcopy(self.process_zero_receipts),
            )
            try:
                yield
            except BaseException:
                (
                    self.jobs,
                    self.threads,
                    self.inventory,
                    self.process_zero_receipts,
                ) = before
                raise

    async def fetchrow(self, query: str, *args):
        sql = _compact(query)
        if (
            sql.startswith(
                "SELECT execution_lane, status::text AS status, metadata, agent_id"
            )
            and "FROM threads WHERE id = $1::uuid FOR UPDATE" in sql
        ):
            thread_id = str(args[0])
            if thread_id not in self.threads:
                return None
            if thread_id not in self.authorized_thread_deletes:
                raise AssertionError(
                    "thread deletion fixture lacks terminal retirement authority"
                )
            return {
                "execution_lane": "stateless",
                "status": "ended",
                "metadata": {
                    "_stateless_workspace_retirement_settled": {
                        "terminal_token": 0,
                        "cleanup_complete": True,
                        "permanent": True,
                        "backing_id": None,
                        "runtime_incarnation": None,
                        "snapshot_restore_required": False,
                    }
                },
                "agent_id": None,
                "control_admission_agent_id": None,
                "runtime_attach_token": None,
                "runtime_generation": RUNTIME_GENERATION,
                "runtime_retirement_token": None,
                "runtime_retirement_permanent": None,
                "runtime_retirement_authorized_at": None,
                "runtime_retirement_context": None,
                "runtime_retirement_local_quiescence": None,
                "runtime_retirement_external_cleanup": None,
                "runtime_authority_exposed": False,
                "live_docker_workspace_lease": any(
                    row.get("owner_kind") == "thread"
                    and str(row.get("owner_id")) == thread_id
                    and row["status"] in {"ready", "releasing"}
                    for row in self.inventory.values()
                ),
                "live_protected_ro": False,
                "unresolved_agent_pod_provision_intent": False,
                "unresolved_agent_workspace_claim": False,
            }
        if sql.startswith("SELECT unit_kind, state, lease_token FROM run_queue"):
            return None
        if (
            "SELECT execution_lane, status::text AS status, metadata FROM threads"
            in sql
        ):
            workspace = self.threads.get(str(args[0]))
            return (
                None
                if workspace is None
                else {
                    "execution_lane": "pinned",
                    "status": "active",
                    "metadata": {
                        "workspace_container": copy.deepcopy(workspace),
                    },
                }
            )
        if "AS workspace FROM jobs" in sql:
            workspace = self.jobs.get(str(args[0]))
            return (
                None if workspace is None else {"workspace": copy.deepcopy(workspace)}
            )
        if sql.startswith("SELECT status, completion_outcome_kind,"):
            # Job deletion reads the pre-delete status for the Officer claim
            # audit. This seam models workspace ownership only, so every
            # represented job reports the same terminal status.
            return (
                None
                if str(args[0]) not in self.jobs
                else {
                    "status": "completed",
                    "completion_outcome_kind": None,
                }
            )
        if sql.startswith("SELECT * FROM vm_job_creation_owners"):
            # The inventory fixture represents Docker workspaces only.
            return None
        if "AS workspace FROM threads" in sql:
            workspace = self.threads.get(str(args[0]))
            return (
                None if workspace is None else {"workspace": copy.deepcopy(workspace)}
            )
        if "FROM docker_workspace_leases" not in sql:
            raise AssertionError(f"unexpected fetchrow: {sql}")

        rows = list(self.inventory.values())
        if "owner_kind = $1 AND owner_id = $2 AND lease_id = $3" in sql:
            kind, owner_id, lease_id = args
            rows = [
                row
                for row in rows
                if row.get("owner_kind") == kind
                and row.get("owner_id") == owner_id
                and row.get("lease_id") == lease_id
            ]
        elif "owner_kind = $1 AND owner_id = $2" in sql:
            kind, owner_id = args
            rows = [
                row
                for row in rows
                if row.get("owner_kind") == kind
                and row.get("owner_id") == owner_id
                and row["status"] in {"ready", "releasing"}
            ]
        elif "owner_kind = $3 AND owner_id = $4" in sql:
            host, port, kind, owner_id = args
            rows = [
                row
                for row in rows
                if row["host"] == host
                and row["port"] == port
                and row.get("owner_kind") == kind
                and row.get("owner_id") == owner_id
                and row.get("lease_id") is None
            ]
        else:
            host, port = args[:2]
            rows = [row for row in rows if row["host"] == host and row["port"] == port]
        return copy.deepcopy(rows[0]) if rows else None

    async def fetch(self, query: str, *args):
        sql = _compact(query)
        if "FROM completion_effects" in sql or sql.startswith(
            "SELECT id FROM agents WHERE thread_id="
        ):
            return []
        raise AssertionError(f"unexpected fetch: {sql}")

    async def fetchval(self, query: str, *args):
        sql = _compact(query)
        if sql.startswith("SELECT EXISTS(SELECT 1 FROM vm_job_creation_owners"):
            return False
        if sql.startswith("SELECT jsonb_typeof(context->'vm')='object'"):
            return False
        if "FROM managed_repository_process_zero_receipts" in sql:
            kind, owner_id, lease_id = args
            return (str(kind), str(owner_id), str(lease_id)) in (
                self.process_zero_receipts
            )
        if sql.startswith("UPDATE officer_ticket_claims"):
            # No represented job holds a durable backlog-ticket claim, so the
            # deletion audit stamps nothing and retains nothing.
            return None
        raise AssertionError(f"unexpected fetchval: {sql}")

    async def execute(self, query: str, *args):
        sql = _compact(query)
        if "pg_advisory_xact_lock" in sql:
            return "SELECT 1"

        if sql.startswith("INSERT INTO docker_workspace_leases"):
            host, port = str(args[0]), int(args[1])
            endpoint = (host, port)
            if endpoint in self.inventory:
                return "INSERT 0 0"
            if len(args) == 6:
                status, trust, fingerprint, reason = args[2:]
            else:
                status, trust, fingerprint, reason = (
                    "quarantined",
                    "unattested",
                    None,
                    "owner_inventory_missing",
                )
            self.inventory[endpoint] = {
                "host": host,
                "port": port,
                "status": status,
                "lease_id": None,
                "owner_kind": None,
                "owner_id": None,
                "trust_mode": trust,
                "host_key_fingerprint": fingerprint,
                "quarantine_reason": reason,
            }
            return "INSERT 0 1"

        if sql.startswith("INSERT INTO managed_repository_process_zero_receipts"):
            kind, owner_id, lease_id = args
            receipt = (str(kind), str(owner_id), str(lease_id))
            existed = receipt in self.process_zero_receipts
            self.process_zero_receipts.add(receipt)
            return "INSERT 0 0" if existed else "INSERT 0 1"

        if sql.startswith("UPDATE docker_workspace_leases"):
            if "SET status = 'ready'" in sql:
                host, port, lease_id, kind, owner_id = args
                row = self.inventory[(host, port)]
                if row["status"] != "released":
                    return "UPDATE 0"
                if any(
                    other["status"] in {"ready", "releasing"}
                    and other.get("owner_kind") == kind
                    and other.get("owner_id") == owner_id
                    for other in self.inventory.values()
                ):
                    return "UPDATE 0"
                row.update(
                    status="ready",
                    lease_id=lease_id,
                    owner_kind=kind,
                    owner_id=owner_id,
                    quarantine_reason=None,
                )
                return "UPDATE 1"
            if "SET status = $4" in sql:
                host, port, lease_id, status, trust, fingerprint, reason = args
                row = self.inventory[(host, port)]
                if row.get("lease_id") != lease_id:
                    return "UPDATE 0"
                row.update(
                    status=status,
                    trust_mode=trust,
                    host_key_fingerprint=fingerprint,
                    quarantine_reason=reason,
                )
                return "UPDATE 1"
            if "owner_kind = 'job'" in sql or "owner_kind = 'thread'" in sql:
                kind = "job" if "owner_kind = 'job'" in sql else "thread"
                owner_id = args[0]
                count = 0
                for row in self.inventory.values():
                    if (
                        row.get("owner_kind") == kind
                        and row.get("owner_id") == owner_id
                        and row["status"] in {"ready", "releasing"}
                    ):
                        row["status"] = "quarantined"
                        row["quarantine_reason"] = "owner_deleted_before_recreation"
                        count += 1
                return f"UPDATE {count}"

            host, port = str(args[0]), int(args[1])
            row = self.inventory[(host, port)]
            if "owner_mirror_mismatch" in sql:
                expected_lease = args[2]
                if row.get("lease_id") != expected_lease:
                    return "UPDATE 0"
                reason = "owner_mirror_mismatch"
            elif "owner_deleted_during_transition" in sql:
                expected_lease = args[2]
                if row.get("lease_id") != expected_lease:
                    return "UPDATE 0"
                reason = "owner_deleted_during_transition"
            elif "host_fingerprint_inventory_mismatch" in sql:
                reason = "host_fingerprint_inventory_mismatch"
            elif "trusted_dev_inventory_not_enabled" in sql:
                reason = "trusted_dev_inventory_not_enabled"
            else:
                reason = "inventory_trust_invalid"
            row["status"] = "quarantined"
            row["quarantine_reason"] = reason
            return "UPDATE 1"

        if sql.startswith("UPDATE jobs") and "workspace_container" in sql:
            if self.fail_owner_update_once:
                self.fail_owner_update_once = False
                return "UPDATE 0"
            owner_id, raw = str(args[0]), args[1]
            if owner_id not in self.jobs:
                return "UPDATE 0"
            self.jobs[owner_id] = json.loads(raw)
            return "UPDATE 1"
        if sql.startswith("UPDATE jobs") and "wake_state = 'undeliverable'" in sql:
            # This seam models only workspace ownership. No represented job is
            # a session wake, but permanent thread deletion still issues the
            # production atomic wake-retirement statement.
            return "UPDATE 0"
        if sql.startswith("UPDATE threads") and "kind = 'subagent'" in sql:
            # 0206: delete_job ends subagent children before the jobs DELETE.
            return "UPDATE 0"
        if sql.startswith("UPDATE threads") and "workspace_container" in sql:
            if self.fail_owner_update_once:
                self.fail_owner_update_once = False
                return "UPDATE 0"
            owner_id, raw = str(args[0]), args[1]
            if owner_id not in self.threads:
                return "UPDATE 0"
            self.threads[owner_id] = json.loads(raw)
            return "UPDATE 1"
        if sql.startswith(
            (
                "DELETE FROM thread_turn_commits",
                "DELETE FROM thread_rewinds",
                "DELETE FROM thread_messages",
            )
        ):
            return "DELETE 0"
        if sql.startswith("DELETE FROM run_queue"):
            return "DELETE 0"
        if sql.startswith("DELETE FROM jobs"):
            return (
                "DELETE 1"
                if self.jobs.pop(str(args[0]), None) is not None
                else "DELETE 0"
            )
        if sql.startswith("DELETE FROM threads"):
            return (
                "DELETE 1"
                if self.threads.pop(str(args[0]), None) is not None
                else "DELETE 0"
            )
        raise AssertionError(f"unexpected execute: {sql}")


def _db(connection: _InventoryConnection) -> PostgresDB:
    db = PostgresDB.__new__(PostgresDB)

    @asynccontextmanager
    async def acquire():
        yield connection

    db.acquire = acquire
    return db


def _candidate(
    *,
    bootstrap: bool = False,
    trusted_dev: bool = False,
    fingerprint: str | None = FINGERPRINT,
) -> dict[str, Any]:
    return {
        "host": "workspace-1",
        "port": 30022,
        "host_key_fingerprint": fingerprint,
        "bootstrap_attested": bootstrap,
        "trusted_dev_reuse": trusted_dev,
    }


def _owner_table(
    connection: _InventoryConnection, kind: str
) -> dict[str, dict[str, Any]]:
    return connection.jobs if kind == "job" else connection.threads


@pytest.mark.asyncio
async def test_unknown_endpoint_is_quarantined_without_bootstrap() -> None:
    connection = _InventoryConnection()
    connection.jobs[JOB_A] = {}
    db = _db(connection)

    result = await db.acquire_docker_workspace_lease(
        owner_kind="job", owner_id=JOB_A, candidates=[_candidate()]
    )

    assert result is None
    row = connection.inventory[("workspace-1", 30022)]
    assert row["status"] == "quarantined"
    assert row["trust_mode"] == "unattested"
    assert row["quarantine_reason"] == "bootstrap_attestation_required"


@pytest.mark.asyncio
async def test_attested_bootstrap_claims_inventory_and_owner_mirror_atomically() -> (
    None
):
    connection = _InventoryConnection()
    connection.threads[THREAD_A] = {
        "git_remote_url": "ssh://git/thread.git",
        "snapshot_attempts": 99,
        "host": "stale-host",
    }
    db = _db(connection)

    result = await db.acquire_docker_workspace_lease(
        owner_kind="thread",
        owner_id=THREAD_A,
        candidates=[_candidate(bootstrap=True)],
    )

    assert result is not None
    assert result["status"] == "ready"
    assert result["_docker_workspace_attested"] is True
    assert result["_docker_workspace_trust_mode"] == "attested"
    assert result["_canvas_workspace_generation"] is None
    assert result["git_remote_url"] == "ssh://git/thread.git"
    assert "snapshot_attempts" not in result
    assert result == connection.threads[THREAD_A]
    row = connection.inventory[("workspace-1", 30022)]
    assert row["owner_kind"] == "thread"
    assert row["owner_id"] == UUID(THREAD_A)
    assert str(row["lease_id"]) == result["_docker_workspace_lease_id"]


@pytest.mark.asyncio
async def test_owner_mirror_failure_rolls_back_first_inventory_claim() -> None:
    connection = _InventoryConnection()
    connection.jobs[JOB_A] = {}
    connection.fail_owner_update_once = True
    db = _db(connection)

    with pytest.raises(RuntimeError, match="owner mirror disappeared"):
        await db.acquire_docker_workspace_lease(
            owner_kind="job",
            owner_id=JOB_A,
            candidates=[_candidate(bootstrap=True)],
        )

    assert connection.inventory == {}
    assert connection.jobs[JOB_A] == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "first_id", "second_id"),
    [("job", JOB_A, JOB_B), ("thread", THREAD_A, THREAD_B)],
)
async def test_quarantine_survives_owner_deletion_and_blocks_reallocation(
    kind: str, first_id: str, second_id: str
) -> None:
    connection = _InventoryConnection()
    owners = _owner_table(connection, kind)
    owners[first_id] = {}
    db = _db(connection)
    lease = await db.acquire_docker_workspace_lease(
        owner_kind=kind,
        owner_id=first_id,
        candidates=[_candidate(bootstrap=True)],
    )
    assert lease is not None

    releasing = await db.transition_docker_workspace_lease(
        owner_kind=kind,
        owner_id=first_id,
        expected_lease_id=lease["_docker_workspace_lease_id"],
        expected_statuses={"ready"},
        updates={"status": "releasing"},
    )
    assert releasing is not None
    quarantined = await db.transition_docker_workspace_lease(
        owner_kind=kind,
        owner_id=first_id,
        expected_lease_id=lease["_docker_workspace_lease_id"],
        expected_statuses={"releasing"},
        updates={
            "status": "quarantined",
            "quarantine_reason": "container_recreation_required",
        },
    )
    assert quarantined is not None

    if kind == "job":
        assert await db.delete_job(first_id) is True
    else:
        connection.authorized_thread_deletes.add(first_id)
        await db.delete_thread(first_id)
    assert first_id not in owners
    durable = connection.inventory[("workspace-1", 30022)]
    assert durable["status"] == "quarantined"

    owners[second_id] = {}
    assert (
        await db.acquire_docker_workspace_lease(
            owner_kind=kind,
            owner_id=second_id,
            # A lingering bootstrap flag must never re-release an existing row.
            candidates=[_candidate(bootstrap=True)],
        )
        is None
    )
    assert durable["status"] == "quarantined"


@pytest.mark.asyncio
@pytest.mark.parametrize(("kind", "owner_id"), [("job", JOB_A), ("thread", THREAD_A)])
async def test_deleting_live_owner_conservatively_quarantines_inventory(
    kind: str, owner_id: str
) -> None:
    connection = _InventoryConnection()
    owners = _owner_table(connection, kind)
    owners[owner_id] = {}
    db = _db(connection)
    assert await db.acquire_docker_workspace_lease(
        owner_kind=kind,
        owner_id=owner_id,
        candidates=[_candidate(bootstrap=True)],
    )

    if kind == "job":
        await db.delete_job(owner_id)
    else:
        connection.authorized_thread_deletes.add(owner_id)
        await db.delete_thread(owner_id)

    row = connection.inventory[("workspace-1", 30022)]
    assert row["status"] == "quarantined"
    assert row["quarantine_reason"] == "owner_deleted_before_recreation"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "first_id", "second_id"),
    [("job", JOB_A, JOB_B), ("thread", THREAD_A, THREAD_B)],
)
async def test_delete_during_release_blocks_stale_finalizer_and_reallocation(
    kind: str, first_id: str, second_id: str
) -> None:
    connection = _InventoryConnection()
    owners = _owner_table(connection, kind)
    owners[first_id] = {}
    db = _db(connection)
    lease = await db.acquire_docker_workspace_lease(
        owner_kind=kind,
        owner_id=first_id,
        candidates=[_candidate(bootstrap=True)],
    )
    assert lease is not None
    releasing = await db.transition_docker_workspace_lease(
        owner_kind=kind,
        owner_id=first_id,
        expected_lease_id=lease["_docker_workspace_lease_id"],
        expected_statuses={"ready"},
        updates={"status": "releasing"},
    )
    assert releasing is not None

    if kind == "job":
        assert await db.delete_job(first_id) is True
    else:
        connection.authorized_thread_deletes.add(first_id)
        await db.delete_thread(first_id)

    row = connection.inventory[("workspace-1", 30022)]
    assert row["status"] == "quarantined"
    assert row["quarantine_reason"] == "owner_deleted_before_recreation"
    stale_finalizer = await db.transition_docker_workspace_lease(
        owner_kind=kind,
        owner_id=first_id,
        expected_lease_id=lease["_docker_workspace_lease_id"],
        expected_statuses={"releasing"},
        updates={
            "status": "released",
            "_docker_workspace_trust_mode": "attested",
            "_docker_workspace_attested": True,
        },
    )
    assert stale_finalizer is None
    assert row["status"] == "quarantined"

    owners[second_id] = {}
    assert (
        await db.acquire_docker_workspace_lease(
            owner_kind=kind,
            owner_id=second_id,
            candidates=[_candidate(bootstrap=True)],
        )
        is None
    )
    assert row["status"] == "quarantined"


@pytest.mark.asyncio
async def test_stale_lease_cannot_mutate_reassigned_inventory() -> None:
    connection = _InventoryConnection()
    connection.jobs[JOB_A] = {}
    connection.jobs[JOB_B] = {}
    db = _db(connection)
    first = await db.acquire_docker_workspace_lease(
        owner_kind="job", owner_id=JOB_A, candidates=[_candidate(bootstrap=True)]
    )
    assert first is not None
    await db.transition_docker_workspace_lease(
        owner_kind="job",
        owner_id=JOB_A,
        expected_lease_id=first["_docker_workspace_lease_id"],
        expected_statuses={"ready"},
        updates={
            "status": "releasing",
            "quarantine_reason": "managed_repository_agent_retirement_claimed",
        },
    )
    assert await db.record_docker_workspace_process_zero(
        JOB_A,
        owner_kind="job",
        lease_id=first["_docker_workspace_lease_id"],
    )
    released = await db.transition_docker_workspace_lease(
        owner_kind="job",
        owner_id=JOB_A,
        expected_lease_id=first["_docker_workspace_lease_id"],
        expected_statuses={"releasing"},
        updates={
            "status": "released",
            "_docker_workspace_trust_mode": "attested",
            "_docker_workspace_attested": True,
        },
    )
    assert released is not None
    second = await db.acquire_docker_workspace_lease(
        owner_kind="job", owner_id=JOB_B, candidates=[_candidate()]
    )
    assert second is not None
    assert second["_docker_workspace_lease_id"] != first["_docker_workspace_lease_id"]

    assert (
        await db.transition_docker_workspace_lease(
            owner_kind="job",
            owner_id=JOB_A,
            expected_lease_id=first["_docker_workspace_lease_id"],
            expected_statuses={"released"},
            updates={"status": "quarantined"},
        )
        is None
    )
    row = connection.inventory[("workspace-1", 30022)]
    assert str(row["lease_id"]) == second["_docker_workspace_lease_id"]
    assert row["owner_id"] == UUID(JOB_B)
    assert row["status"] == "ready"


@pytest.mark.asyncio
async def test_authoritative_mirror_strips_stale_endpoint_aliases() -> None:
    connection = _InventoryConnection()
    lease_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    connection.threads[THREAD_A] = {
        "host": "workspace-1",
        "port": 30022,
        "ssh_host": "evil-alias",
        "ssh_port": 22,
        "pod_ip": "203.0.113.10",
        "status": "ready",
        "provisioner": "docker",
        "_docker_workspace_lease_id": str(lease_id),
        "_canvas_workspace_generation": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "repo_name": "thread-repo",
    }
    connection.inventory[("workspace-1", 30022)] = {
        "host": "workspace-1",
        "port": 30022,
        "status": "ready",
        "lease_id": lease_id,
        "owner_kind": "thread",
        "owner_id": UUID(THREAD_A),
        "trust_mode": "attested",
        "host_key_fingerprint": FINGERPRINT,
        "quarantine_reason": None,
    }
    result = await _db(connection).acquire_docker_workspace_lease(
        owner_kind="thread", owner_id=THREAD_A, candidates=[_candidate()]
    )

    assert result is not None
    assert result["host"] == "workspace-1"
    assert result["port"] == 30022
    assert result["repo_name"] == "thread-repo"
    assert result["_canvas_workspace_generation"].startswith("bbbb")
    for stale in ("ssh_host", "ssh_port", "pod_ip"):
        assert stale not in result


@pytest.mark.asyncio
async def test_same_owner_lease_disagreement_quarantines_both_mirrors() -> None:
    connection = _InventoryConnection()
    inventory_lease = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    connection.threads[THREAD_A] = {
        "host": "workspace-1",
        "port": 30022,
        "status": "ready",
        "provisioner": "docker",
        "_docker_workspace_lease_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "_docker_workspace_trust_mode": "attested",
        "_docker_workspace_attested": True,
        "_canvas_workspace_generation": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
    }
    connection.inventory[("workspace-1", 30022)] = {
        "host": "workspace-1",
        "port": 30022,
        "status": "ready",
        "lease_id": inventory_lease,
        "owner_kind": "thread",
        "owner_id": UUID(THREAD_A),
        "trust_mode": "attested",
        "host_key_fingerprint": FINGERPRINT,
        "quarantine_reason": None,
    }

    result = await _db(connection).acquire_docker_workspace_lease(
        owner_kind="thread", owner_id=THREAD_A, candidates=[_candidate()]
    )

    assert result is not None
    assert result["status"] == "quarantined"
    assert result["_docker_workspace_lease_id"] is None
    assert result["_docker_workspace_attested"] is False
    assert result["_canvas_workspace_generation"] is None
    inventory = connection.inventory[("workspace-1", 30022)]
    assert inventory["status"] == "quarantined"
    assert inventory["lease_id"] == inventory_lease
    assert inventory["quarantine_reason"] == "owner_mirror_mismatch"


@pytest.mark.asyncio
async def test_transition_requires_exact_owner_and_inventory_status_match() -> None:
    connection = _InventoryConnection()
    lease_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    connection.jobs[JOB_A] = {
        "host": "workspace-1",
        "port": 30022,
        "status": "ready",
        "provisioner": "docker",
        "_docker_workspace_lease_id": str(lease_id),
        "_docker_workspace_trust_mode": "attested",
        "_docker_workspace_attested": True,
    }
    connection.inventory[("workspace-1", 30022)] = {
        "host": "workspace-1",
        "port": 30022,
        "status": "releasing",
        "lease_id": lease_id,
        "owner_kind": "job",
        "owner_id": UUID(JOB_A),
        "trust_mode": "attested",
        "host_key_fingerprint": FINGERPRINT,
        "quarantine_reason": None,
    }

    result = await _db(connection).transition_docker_workspace_lease(
        owner_kind="job",
        owner_id=JOB_A,
        expected_lease_id=str(lease_id),
        expected_statuses={"ready", "releasing"},
        updates={"status": "quarantined"},
    )

    assert result is None
    inventory = connection.inventory[("workspace-1", 30022)]
    assert inventory["status"] == "quarantined"
    assert inventory["quarantine_reason"] == "owner_mirror_mismatch"
    assert connection.jobs[JOB_A]["status"] == "quarantined"
    assert connection.jobs[JOB_A]["_docker_workspace_lease_id"] is None


@pytest.mark.asyncio
async def test_other_owner_inventory_is_not_copied_into_stale_mirror() -> None:
    connection = _InventoryConnection()
    lease_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    connection.jobs[JOB_B] = {
        "host": "workspace-1",
        "port": 30022,
        "status": "ready",
        "provisioner": "docker",
        "_docker_workspace_lease_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    }
    connection.inventory[("workspace-1", 30022)] = {
        "host": "workspace-1",
        "port": 30022,
        "status": "ready",
        "lease_id": lease_id,
        "owner_kind": "job",
        "owner_id": UUID(JOB_A),
        "trust_mode": "attested",
        "host_key_fingerprint": FINGERPRINT,
        "quarantine_reason": None,
    }

    result = await _db(connection).acquire_docker_workspace_lease(
        owner_kind="job", owner_id=JOB_B, candidates=[_candidate()]
    )

    assert result is not None
    assert result["status"] == "quarantined"
    assert result["_docker_workspace_lease_id"] is None
    assert result["_docker_workspace_attested"] is False
    authoritative = connection.inventory[("workspace-1", 30022)]
    assert authoritative["lease_id"] == lease_id
    assert authoritative["owner_id"] == UUID(JOB_A)
    assert authoritative["status"] == "ready"


@pytest.mark.asyncio
async def test_dev_provenance_cannot_be_consumed_or_promoted_by_default_mode() -> None:
    connection = _InventoryConnection()
    connection.jobs[JOB_A] = {}
    connection.jobs[JOB_B] = {}
    db = _db(connection)
    dev = await db.acquire_docker_workspace_lease(
        owner_kind="job",
        owner_id=JOB_A,
        candidates=[_candidate(trusted_dev=True, fingerprint=None)],
    )
    assert dev is not None
    assert dev["_docker_workspace_attested"] is False
    await db.transition_docker_workspace_lease(
        owner_kind="job",
        owner_id=JOB_A,
        expected_lease_id=dev["_docker_workspace_lease_id"],
        expected_statuses={"ready"},
        updates={
            "status": "releasing",
            "quarantine_reason": "managed_repository_agent_retirement_claimed",
        },
    )
    assert await db.record_docker_workspace_process_zero(
        JOB_A,
        owner_kind="job",
        lease_id=dev["_docker_workspace_lease_id"],
    )
    await db.transition_docker_workspace_lease(
        owner_kind="job",
        owner_id=JOB_A,
        expected_lease_id=dev["_docker_workspace_lease_id"],
        expected_statuses={"releasing"},
        updates={
            "status": "released",
            "_docker_workspace_trust_mode": "trusted_dev",
            "_docker_workspace_attested": False,
        },
    )

    assert (
        await db.acquire_docker_workspace_lease(
            owner_kind="job",
            owner_id=JOB_B,
            # Fingerprint + bootstrap cannot promote an existing dev row.
            candidates=[_candidate(bootstrap=True, trusted_dev=False)],
        )
        is None
    )
    row = connection.inventory[("workspace-1", 30022)]
    assert row["status"] == "released"
    assert row["trust_mode"] == "trusted_dev"
    assert row["host_key_fingerprint"] is None


@pytest.mark.asyncio
async def test_transition_rejects_cleanup_bypassing_lifecycle_edges() -> None:
    connection = _InventoryConnection()
    connection.jobs[JOB_A] = {}
    db = _db(connection)
    lease = await db.acquire_docker_workspace_lease(
        owner_kind="job", owner_id=JOB_A, candidates=[_candidate(bootstrap=True)]
    )
    assert lease is not None

    with pytest.raises(ValueError, match="lifecycle transition"):
        await db.transition_docker_workspace_lease(
            owner_kind="job",
            owner_id=JOB_A,
            expected_lease_id=lease["_docker_workspace_lease_id"],
            expected_statuses={"ready"},
            updates={
                "status": "released",
                "_docker_workspace_trust_mode": "attested",
                "_docker_workspace_attested": True,
            },
        )
    assert connection.inventory[("workspace-1", 30022)]["status"] == "ready"
    assert connection.jobs[JOB_A]["status"] == "ready"

    quarantined = await db.transition_docker_workspace_lease(
        owner_kind="job",
        owner_id=JOB_A,
        expected_lease_id=lease["_docker_workspace_lease_id"],
        expected_statuses={"ready"},
        updates={"status": "quarantined"},
    )
    assert quarantined is not None

    with pytest.raises(ValueError, match="lifecycle transition"):
        await db.transition_docker_workspace_lease(
            owner_kind="job",
            owner_id=JOB_A,
            expected_lease_id=lease["_docker_workspace_lease_id"],
            expected_statuses={"quarantined"},
            updates={
                "status": "released",
                "_docker_workspace_trust_mode": "attested",
                "_docker_workspace_attested": True,
            },
        )
    assert connection.inventory[("workspace-1", 30022)]["status"] == ("quarantined")
    assert connection.jobs[JOB_A]["status"] == "quarantined"


def test_inventory_migration_is_owner_independent_and_conservative() -> None:
    migration = Path(
        "src/orchestrator/database/migrations/app/0059_docker_workspace_leases.sql"
    ).read_text()

    assert "PRIMARY KEY (host, port)" in migration
    assert "REFERENCES jobs" not in migration
    assert "REFERENCES threads" not in migration
    assert "'legacy_recreation_attestation_required'" in migration
    assert "'legacy_conflicting_owners'" in migration
    assert "'quarantined'" in migration
    assert "uq_docker_workspace_lease_id" in migration
    assert "status IN ('ready', 'releasing')" in migration
