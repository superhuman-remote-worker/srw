"""A completion that read processing cannot overwrite a later pinned cancel."""

from __future__ import annotations

from tests import b08_completion_helpers as b08_helpers

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

from fastapi import HTTPException
import pytest

from orchestrator import main
from orchestrator.database.postgres import PostgresDB
from orchestrator.services import completion
from tests import test_completion_control_real_postgres as postgres_fixtures
from tests.test_job_completion_endpoint_wrapper import (
    _patch_normal_route_dependencies,
    _route_job,
)


pg_dsn = postgres_fixtures.pg_dsn
_schema_applied = postgres_fixtures._schema_applied
pg = postgres_fixtures.pg


class _RacingPostgres:
    """Use real status writes; cancel immediately before a selected mutation."""

    _resolve_workspace_recovery_cancel_participant = staticmethod(
        PostgresDB._resolve_workspace_recovery_cancel_participant
    )
    _emit_workspace_recovery_cancel = staticmethod(
        PostgresDB._emit_workspace_recovery_cancel
    )
    _queue_job_for_resume_on_conn = PostgresDB._queue_job_for_resume_on_conn

    def __init__(self, pool, job, *, race_at="update_job_status"):
        self.pool = pool
        self.job = job
        self.race_at = race_at
        self.cancelled = False
        self.read_statuses = []
        self.status_write_attempts = []
        self.delete_checkpoint_thread = AsyncMock()
        self.merge_job_context = AsyncMock()
        self.merge_workspace_container_context = AsyncMock(return_value=True)
        self.increment_job_memory_retry = AsyncMock(return_value=1)
        self.increment_job_llm_outage_attempt = AsyncMock(return_value={"attempt": 1})

    def acquire(self):
        return self.pool.acquire()

    async def get_job(self, job_id):
        async with self.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT status, assigned_agent_id FROM jobs WHERE id=$1", UUID(job_id)
            )
        self.read_statuses.append(row["status"])
        return {**self.job, **dict(row)}

    async def _cancel_before(self, method):
        if method != self.race_at or self.cancelled:
            return
        assert self.read_statuses[0] == "processing"
        assert await PostgresDB.linearize_pinned_cancel(
            self,
            self.job["id"],
            expected_status="processing",
            completion_commands_enabled=False,
        )
        self.cancelled = True

    async def update_job_status(self, job_id, **kwargs):
        self.status_write_attempts.append(kwargs)
        await self._cancel_before("update_job_status")
        return await PostgresDB.update_job_status(self, job_id, **kwargs)

    async def pause_job(self, job_id, **kwargs):
        await self._cancel_before("pause_job")
        return await PostgresDB.pause_job(self, job_id, **kwargs)

    async def pause_job_shed_freeze(self, job_id, **kwargs):
        await self._cancel_before("pause_job_shed_freeze")
        return await PostgresDB.pause_job_shed_freeze(self, job_id, **kwargs)

    async def queue_job_for_resume(self, job_id, *args, **kwargs):
        await self._cancel_before("queue_job_for_resume")
        return await PostgresDB.queue_job_for_resume(self, job_id, *args, **kwargs)


async def _database(pg, *, context=None, race_at="update_job_status"):
    async with pg.acquire() as conn:
        job_id = await conn.fetchval(
            "INSERT INTO jobs (description,status,execution_lane) "
            "VALUES ('pinned cancellation completion race','processing','pinned') "
            "RETURNING id"
        )
    job = {**_route_job(), "id": str(job_id), "context": context or {}}
    return _RacingPostgres(pg, job, race_at=race_at)


def _isolate(monkeypatch, db):
    terminal = AsyncMock(return_value={"actions": []})
    cleanup = AsyncMock()
    _patch_normal_route_dependencies(
        monkeypatch, database=db, terminal_effects=terminal, workspace_cleanup=cleanup
    )
    monkeypatch.setattr(main, "COMPLETION_COMMANDS_ENABLED", False)
    return terminal, cleanup


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["completed", "paused", "infra_exhausted"])
async def test_real_postgres_inflight_completion_cannot_overwrite_cancel(
    monkeypatch, pg, outcome
):
    context = (
        {"infra_transient": {"attempts": completion.INFRA_TRANSIENT_MAX_ATTEMPTS}}
        if outcome == "infra_exhausted"
        else {}
    )
    db = await _database(pg, context=context)
    terminal, cleanup = _isolate(monkeypatch, db)
    body = main.JobCompleteRequest(
        should_stop=True,
        goal_achieved=outcome == "completed",
        error=(
            {"type": "infra_transient", "message": "test outage"}
            if outcome == "infra_exhausted"
            else None
        ),
        freeze_data=(
            {"freeze_type": "version_upgrade"} if outcome == "paused" else None
        ),
    )
    with pytest.raises(HTTPException) as exc:
        await b08_helpers.complete_job_legacy(
            None, db.job["id"], body, _authorized=True
        )
    assert exc.value.status_code == 409
    assert db.cancelled
    assert (await db.get_job(db.job["id"]))["status"] == "cancelled"
    db.delete_checkpoint_thread.assert_not_awaited()
    terminal.assert_not_awaited()
    cleanup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("recovery", ["infra", "memory", "llm", "vm", "pod"])
async def test_real_postgres_recovery_pause_loss_stops_completion_tail(
    monkeypatch, pg, recovery
):
    db = await _database(
        pg,
        context={"vm": {"requested": True}} if recovery == "vm" else {},
        race_at="pause_job_shed_freeze" if recovery == "pod" else "pause_job",
    )
    terminal, cleanup = _isolate(monkeypatch, db)
    monkeypatch.setattr(main, "_job_needs_vm", lambda _: recovery == "vm")
    vm_capture = AsyncMock()
    monkeypatch.setattr(main.vm_provisioner, "capture_vm_teardown_identity", vm_capture)
    error = None
    freeze = None
    if recovery in {"infra", "vm", "pod"}:
        error = {
            "type": "infra_transient"
            if recovery == "infra"
            else "workspace_unavailable",
            "message": "test recovery",
        }
    else:
        freeze = {
            "freeze_type": "memory_unavailable"
            if recovery == "memory"
            else "llm_unavailable"
        }
    with pytest.raises(HTTPException) as exc:
        await b08_helpers.complete_job_legacy(
            None,
            db.job["id"],
            main.JobCompleteRequest(should_stop=True, error=error, freeze_data=freeze),
            _authorized=True,
        )
    assert exc.value.status_code == 409
    assert db.cancelled
    assert (await db.get_job(db.job["id"]))["status"] == "cancelled"
    db.delete_checkpoint_thread.assert_not_awaited()
    terminal.assert_not_awaited()
    cleanup.assert_not_awaited()
    vm_capture.assert_not_awaited()
    main._trigger_dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_real_postgres_pod_recovery_exhaustion_loses_before_delete(
    monkeypatch, pg
):
    monkeypatch.setenv("WORKSPACE_RECOVERY_MAX_ATTEMPTS", "3")
    db = await _database(pg, context={"workspace_container": {"recovery_attempts": 3}})
    job = await db.get_job(db.job["id"])
    delete = AsyncMock(return_value=True)
    dispatch = MagicMock()
    outcome = await completion.handle_pod_workspace_recovery(
        job,
        job["id"],
        {"message": "test outage"},
        db=db,
        delete_workspace=delete,
        trigger_dispatch=dispatch,
        expected_status="processing",
    )
    assert outcome["paused"] is False
    assert db.cancelled
    assert (await db.get_job(job["id"]))["status"] == "cancelled"
    db.delete_checkpoint_thread.assert_not_awaited()
    delete.assert_not_awaited()
    dispatch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("pod_alive", [False, True])
async def test_real_postgres_pod_recovery_loses_before_context_or_delete(pg, pod_alive):
    db = await _database(
        pg,
        context={"workspace_container": {"host": "test-workspace", "port": 22}},
        race_at="pause_job_shed_freeze",
    )
    job = await db.get_job(db.job["id"])
    delete = AsyncMock(return_value=True)
    dispatch = MagicMock()
    outcome = await completion.handle_pod_workspace_recovery(
        job,
        job["id"],
        {"message": "test outage"},
        db=db,
        delete_workspace=delete,
        trigger_dispatch=dispatch,
        probe=AsyncMock(return_value=pod_alive),
        expected_status="processing",
    )
    assert outcome["paused"] is False
    assert db.cancelled
    assert (await db.get_job(job["id"]))["status"] == "cancelled"
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT context->'workspace_container' FROM jobs WHERE id=$1",
                UUID(job["id"]),
            )
            is None
        )
    db.merge_workspace_container_context.assert_not_awaited()
    db.delete_checkpoint_thread.assert_not_awaited()
    delete.assert_not_awaited()
    dispatch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "freeze_type,expected", [(None, "completed"), ("version_upgrade", "paused")]
)
async def test_real_postgres_uncontested_legacy_disposition_still_commits(
    monkeypatch, pg, freeze_type, expected
):
    db = await _database(pg, race_at=None)
    _isolate(monkeypatch, db)
    body = main.JobCompleteRequest(
        should_stop=True,
        goal_achieved=freeze_type is None,
        freeze_data={"freeze_type": freeze_type} if freeze_type else None,
    )
    outcome = await b08_helpers.complete_job_legacy(
        None, db.job["id"], body, _authorized=True
    )
    assert outcome["new_status"] == expected
    assert (await db.get_job(db.job["id"]))["status"] == expected
    assert not db.cancelled


@pytest.mark.asyncio
async def test_real_deliverable_gate_cannot_swallow_cancelled_resume_race(
    monkeypatch, pg
):
    from orchestrator.services import job_evidence
    from tests.test_deliverable_gate import make_gitea

    db = await _database(
        pg,
        context={"required_deliverables": ["output/missing.md"]},
        race_at="queue_job_for_resume",
    )
    db.job["repo_name"] = "test-completion-race"
    real_gate = completion.apply_deliverable_gate
    terminal, cleanup = _isolate(monkeypatch, db)
    monkeypatch.setattr(completion, "apply_deliverable_gate", real_gate)
    monkeypatch.setattr(main, "gitea_client", make_gitea([]))
    evidence = AsyncMock()
    monkeypatch.setattr(job_evidence, "build_evidence_manifest", evidence)
    body = main.JobCompleteRequest(
        should_stop=True,
        goal_achieved=True,
        freeze_data={"freeze_type": "job_complete", "summary": "test completion"},
    )
    with pytest.raises(HTTPException) as exc:
        await b08_helpers.complete_job_legacy(
            None, db.job["id"], body, _authorized=True
        )
    assert exc.value.status_code == 409
    assert db.cancelled
    assert (await db.get_job(db.job["id"]))["status"] == "cancelled"
    assert db.status_write_attempts == []
    db.delete_checkpoint_thread.assert_not_awaited()
    evidence.assert_not_awaited()
    terminal.assert_not_awaited()
    cleanup.assert_not_awaited()
    main._trigger_dispatch.assert_not_called()
