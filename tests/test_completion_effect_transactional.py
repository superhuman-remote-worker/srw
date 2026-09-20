"""Atomicity contracts for Postgres-only completion effects."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.completion_finalizer import CompletionEffectRunner


COMMAND_ID = UUID("22222222-bbbb-4222-8222-222222222222")
JOB_ID = "11111111-aaaa-4111-8111-111111111111"


def _normalized(sql: str) -> str:
    return " ".join(sql.split()).lower()


@dataclass
class _Ledger:
    committed_mutations: list[str] = field(default_factory=list)
    pool_events: list[str] = field(default_factory=list)


class _Transaction:
    def __init__(self, connection: "_Connection") -> None:
        self.connection = connection
        self.snapshot = 0

    async def __aenter__(self):
        self.snapshot = len(self.connection.pending_mutations)
        self.connection.transaction_depth += 1
        self.connection.transaction_events.append(
            f"begin:{self.connection.transaction_depth}"
        )
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        depth = self.connection.transaction_depth
        if exc_type is not None:
            del self.connection.pending_mutations[self.snapshot :]
            outcome = "rollback"
        else:
            outcome = "commit"
        self.connection.transaction_events.append(f"{outcome}:{depth}")
        self.connection.transaction_depth -= 1
        if self.connection.transaction_depth == 0:
            if exc_type is None:
                self.connection.ledger.committed_mutations.extend(
                    self.connection.pending_mutations
                )
            self.connection.pending_mutations.clear()
        return False


class _Connection:
    """Scripted asyncpg connection that records task, connection, and tx depth."""

    def __init__(self, name: str, ledger: _Ledger) -> None:
        self.name = name
        self.ledger = ledger
        self.transaction_depth = 0
        self.transaction_events: list[str] = []
        self.pending_mutations: list[str] = []
        self.calls: list[tuple[str, str, int, asyncio.Task[Any] | None]] = []
        self.settled_states: list[str] = []

    def transaction(self) -> _Transaction:
        return _Transaction(self)

    def _record(self, operation: str, sql: str) -> str:
        normalized = _normalized(sql)
        self.calls.append(
            (operation, normalized, self.transaction_depth, asyncio.current_task())
        )
        return normalized

    def _mutation(self, normalized: str) -> None:
        if self.transaction_depth:
            self.pending_mutations.append(normalized)
        else:
            self.ledger.committed_mutations.append(normalized)

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        normalized = self._record("fetchrow", sql)
        if normalized.startswith("insert into completion_effects"):
            self._mutation(normalized)
            return {
                "effect_group": "recovery",
                "state": "pending",
                "attempts": 1,
                "max_attempts": 5,
                "run_after": datetime.now(UTC),
                "complete_by": datetime.now(UTC) + timedelta(seconds=30),
                "detail": {},
                "deferred": False,
                "remaining_seconds": 30.0,
            }
        if normalized.startswith("select effect_name, effect_group, state"):
            return {
                "effect_name": "atomic_counter",
                "effect_group": "recovery",
                "state": "pending",
                "attempts": 1,
                "max_attempts": 5,
                "run_after": datetime.now(UTC),
                "intent_at": datetime.now(UTC),
                "complete_by": datetime.now(UTC) + timedelta(seconds=30),
                "completed_at": None,
                "detail": {},
                "error_code": None,
                "deferred": False,
            }
        raise AssertionError(f"unexpected fetchrow SQL: {normalized}")

    async def fetchval(self, sql: str, *args: Any) -> Any:
        normalized = self._record("fetchval", sql)
        if normalized.startswith("update job_completion_commands set lease_expires_at"):
            self._mutation(normalized)
            return 1
        if normalized.startswith("with completed_effect as"):
            self._mutation(normalized)
            self.settled_states.append(str(args[5]))
            return 1
        raise AssertionError(f"unexpected fetchval SQL: {normalized}")

    async def execute(self, sql: str, *args: Any) -> str:
        normalized = self._record("execute", sql)
        if normalized.startswith("update jobs set context =") or normalized.startswith(
            "update completion_effects as effect set error_code"
        ):
            self._mutation(normalized)
            return "UPDATE 1"
        raise AssertionError(f"unexpected execute SQL: {normalized}")


class _PoolAcquire:
    def __init__(self, pool: "_Pool", connection: _Connection) -> None:
        self.pool = pool
        self.connection = connection

    async def __aenter__(self) -> _Connection:
        self.pool.ledger.pool_events.append(f"acquire:{self.connection.name}")
        return self.connection

    async def __aexit__(self, exc_type, exc, traceback):
        self.pool.ledger.pool_events.append(f"release:{self.connection.name}")


class _Pool:
    def __init__(self, ledger: _Ledger, *connections: _Connection) -> None:
        self.ledger = ledger
        self.connections = list(connections)

    def acquire(self) -> _PoolAcquire:
        if not self.connections:
            raise AssertionError("unexpected second connection acquisition")
        return _PoolAcquire(self, self.connections.pop(0))


def _runner_database() -> tuple[
    CompletionEffectRunner, PostgresDB, _Ledger, _Connection, _Connection
]:
    ledger = _Ledger()
    parent_connection = _Connection("parent", ledger)
    child_connection = _Connection("child", ledger)
    database = PostgresDB.__new__(PostgresDB)
    database._pool = _Pool(ledger, parent_connection, child_connection)
    command = {
        "id": COMMAND_ID,
        "job_id": UUID(JOB_ID),
        "state": "finalizing",
        "finalizing_by": "owner-a",
    }
    runner = CompletionEffectRunner(
        database,
        command=command,
        owner="owner-a",
        effect_lease_seconds=30,
    )
    return runner, database, ledger, parent_connection, child_connection


@pytest.mark.asyncio
async def test_transactional_effect_domain_write_and_marker_share_transaction():
    runner, database, ledger, parent, child = _runner_database()

    async def update_counter() -> dict[str, int]:
        assert await database.merge_job_context(JOB_ID, {"counter": 1})
        return {"counter": 1}

    result = await runner.run_transactional(
        name="atomic_counter",
        group="recovery",
        callback=update_counter,
    )

    assert result == {"counter": 1}
    domain_write = next(
        call for call in parent.calls if call[1].startswith("update jobs set context =")
    )
    completion_marker = next(
        call for call in parent.calls if call[1].startswith("with completed_effect as")
    )
    assert domain_write[2] > 0
    assert completion_marker[2] > 0
    assert domain_write[3] is completion_marker[3] is asyncio.current_task()
    # Preparation and the callback's database helper each use a savepoint
    # inside the task-scoped transaction. Both settle before the outer domain
    # transaction, which also owns the completion marker.
    assert parent.transaction_events == [
        "begin:1",
        "begin:2",
        "commit:2",
        "begin:2",
        "commit:2",
        "commit:1",
    ]
    assert child.calls == []
    assert ledger.pool_events == ["acquire:parent", "release:parent"]
    assert any(
        sql.startswith("update jobs set context =")
        for sql in ledger.committed_mutations
    )
    assert any(
        sql.startswith("with completed_effect as") for sql in ledger.committed_mutations
    )


@pytest.mark.asyncio
async def test_transactional_effect_rejects_effect_group_retry_modes():
    runner, _, ledger, parent, child = _runner_database()

    async def callback() -> str:
        return "unused"

    with pytest.raises(ValueError, match="command-level retry"):
        await runner.run_transactional(
            name="atomic_counter",
            group="recovery",
            callback=callback,
            retry_on_error=True,
            error_output=lambda exc: {"error": str(exc)},
        )

    with pytest.raises(ValueError, match="command-level retry"):
        await runner.run_transactional(
            name="atomic_counter",
            group="recovery",
            callback=callback,
            retry_if=lambda output: output == "retry",
        )

    assert ledger.committed_mutations == []
    assert ledger.pool_events == []
    assert parent.calls == []
    assert child.calls == []


@pytest.mark.asyncio
async def test_transactional_world_cas_miss_and_supersede_marker_share_transaction():
    runner, database, ledger, parent, child = _runner_database()

    async def lose_world_cas() -> dict[str, Any]:
        assert await database.merge_job_context(JOB_ID, {"synthesizer": "lost"})
        return {"won": False}

    result = await runner.run_transactional(
        name="atomic_counter",
        group="recovery",
        callback=lose_world_cas,
        supersede_if=lambda output: output["won"] is False,
    )

    assert result == {"won": False}
    assert parent.settled_states == ["superseded"]
    domain_write = next(
        call for call in parent.calls if call[1].startswith("update jobs set context =")
    )
    completion_marker = next(
        call for call in parent.calls if call[1].startswith("with completed_effect as")
    )
    assert domain_write[2] > 0
    assert completion_marker[2] > 0
    assert domain_write[3] is completion_marker[3] is asyncio.current_task()
    assert child.calls == []
    assert ledger.pool_events == ["acquire:parent", "release:parent"]


@pytest.mark.asyncio
async def test_transactional_callback_error_rolls_back_without_shield_child_write():
    runner, database, ledger, parent, child = _runner_database()

    async def update_then_fail() -> None:
        assert await database.merge_job_context(JOB_ID, {"counter": 1})
        raise RuntimeError("counter write failed")

    with pytest.raises(RuntimeError, match="counter write failed"):
        await runner.run_transactional(
            name="atomic_counter",
            group="recovery",
            callback=update_then_fail,
        )

    # The intent and domain write belong to the rolled-back parent transaction.
    # Transactional mode must not invoke run()'s shielded diagnostic arm: that
    # would run in a child task, escape the task-scoped connection, and commit a
    # marker independently of the rolled-back effect.
    assert ledger.committed_mutations == []
    assert parent.transaction_events == [
        "begin:1",
        "begin:2",
        "commit:2",
        "begin:2",
        "commit:2",
        "rollback:1",
    ]
    assert child.calls == []
    assert ledger.pool_events == ["acquire:parent", "release:parent"]
