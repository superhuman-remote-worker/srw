"""HTTP contracts for the Gate-3 ``/complete`` admission wrapper.

These tests deliberately stub the legacy completion body.  Its side effects
remain covered by the existing endpoint suites; this file proves only the dark
gate, admission ordering, durable outcome handoff, and replay response matrix.
"""

from __future__ import annotations

from tests import b08_completion_helpers as b08_helpers

import builtins
import copy
import dataclasses
import inspect
import json
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from fastapi import HTTPException
from starlette.responses import JSONResponse

import orchestrator.main
from orchestrator.services import job_completion_commands as commands
from orchestrator.services import completion as completion_service
from orchestrator.services.completion_effect_policy import COMPLETION_EFFECT_PLAN
from orchestrator.services.completion_finalizer import CompletionDispositionSuperseded
from orchestrator.services.container_provisioner import WorkspaceCleanupOutcome


JOB_ID = "11111111-2222-3333-4444-555555555555"
AGENT_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
REPORT_ID = UUID("99999999-8888-7777-6666-555555555555")
COMMAND_ID = "12345678-1234-5678-9abc-123456789abc"
CURATOR_ID = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"


class _AllowCleanupStore:
    async def acquire_cleanup_permit(self, **_kwargs):
        return SimpleNamespace(allowed=True)


@pytest.fixture(autouse=True)
def _isolate_workspace_cleanup_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep legacy completion tests focused on their existing collaborators."""

    original = orchestrator.main._completion_effect_dependencies

    def dependencies():
        return dataclasses.replace(original(), recovery_store=_AllowCleanupStore())

    monkeypatch.setattr(
        orchestrator.main, "_completion_effect_dependencies", dependencies
    )


class _RecordingRunner:
    """Small in-memory model of the durable effect-runner route contract."""

    def __init__(self) -> None:
        self.command_id = COMMAND_ID
        self.command: dict[str, object] = {}
        self.owner = "recording-finalizer-owner"
        self.started: set[str] = set()
        self.details: dict[str, object] = {}
        self.pending: dict[str, object] = {}
        self.intents: dict[str, dict[str, object]] = {}
        self.higher_report_seq = False
        self.teardown_authority_disposition: str | None = None
        self.teardown_handoff = False
        self.entry_authority_checks = 0
        self.entry_authority_hook = None
        self.delivery_control_acquisitions = 0
        self.delivery_control_checks = 0
        self.delivery_control_hook = None
        self.disposition_authority_checks = 0
        self.disposition_authority_hook = None
        self.probe_order: list[str] = []
        self.order: list[tuple[str, str]] = []
        self.callback_counts: Counter[str] = Counter()
        self.retry_if_names: set[str] = set()
        self.transactional_names: set[str] = set()
        self.superseded_names: set[str] = set()

    async def run(
        self,
        *,
        name: str,
        group: str,
        callback,
        retry_on_error: bool = False,
        error_output=None,
        retry_if=None,
        supersede_if=None,
        depends_on_groups=(),
        effect_timeout_seconds=None,
        command_lease_seconds=None,
    ):
        del depends_on_groups
        self.started.add(name)
        if name in self.details:
            return copy.deepcopy(self.details[name])
        if retry_if is not None:
            self.retry_if_names.add(name)
        if name == "workspace_archive_teardown":
            assert effect_timeout_seconds == 890.0
            assert command_lease_seconds == 900.0
        else:
            assert effect_timeout_seconds is None
            assert command_lease_seconds is None
        self.order.append((name, group))
        self.callback_counts[name] += 1
        try:
            detail = await callback()
        except Exception as exc:
            if isinstance(exc, CompletionDispositionSuperseded):
                raise
            if not retry_on_error or error_output is None:
                raise
            detail = error_output(exc)
            if inspect.isawaitable(detail):
                detail = await detail
            self.pending[name] = copy.deepcopy(detail)
            return copy.deepcopy(detail)
        if retry_if is not None and retry_if(detail):
            self.pending[name] = copy.deepcopy(detail)
            return copy.deepcopy(detail)
        if supersede_if is not None and supersede_if(detail):
            self.pending.pop(name, None)
            self.superseded_names.add(name)
            self.details[name] = copy.deepcopy(detail)
            return copy.deepcopy(detail)
        self.pending.pop(name, None)
        self.details[name] = copy.deepcopy(detail)
        return copy.deepcopy(detail)

    async def run_transactional(self, **kwargs):
        self.transactional_names.add(str(kwargs["name"]))
        return await self.run(**kwargs)

    async def has_started(self, name: str) -> bool:
        return name in self.started

    async def has_completed(self, name: str) -> bool:
        return name in self.details and name not in self.superseded_names

    async def completed_detail(self, name: str):
        if name in self.superseded_names:
            return None
        detail = self.details.get(name)
        return copy.deepcopy(detail)

    async def terminal_detail(self, name: str):
        detail = self.details.get(name)
        return copy.deepcopy(detail)

    async def has_pending_group(self, group: str) -> bool:
        return any(
            effect.name in self.pending and effect.group == group
            for effect in COMPLETION_EFFECT_PLAN
        )

    async def capture_intent(self, name: str, detail=None):
        self.probe_order.append("intent_write" if detail is not None else "intent_read")
        existing = self.intents.get(name)
        if existing is not None:
            if detail is not None and existing != detail:
                raise RuntimeError("completion effect intent identity drifted")
            return copy.deepcopy(existing)
        if detail is None:
            return None
        self.intents[name] = copy.deepcopy(detail)
        return copy.deepcopy(detail)

    async def authorize_workspace_teardown(self):
        disposition = self.teardown_authority_disposition or (
            "deferred" if self.higher_report_seq else "authorized"
        )
        return SimpleNamespace(
            authorized=disposition == "authorized",
            higher_report_seq=(2 if disposition == "deferred" else None),
            superseded=disposition == "world_state_superseded",
            operator_hold=disposition == "operator_hold",
            observed_status=(
                "cancelled"
                if disposition in {"world_state_superseded", "operator_hold"}
                else "completed"
            ),
            expected_status="completed",
        )

    async def workspace_teardown_handoff(self):
        return SimpleNamespace(required=self.teardown_handoff)

    async def assert_entry_authority(self) -> None:
        self.entry_authority_checks += 1
        if self.entry_authority_hook is not None:
            await self.entry_authority_hook()

    async def acquire_delivery_control(self, expected_status: str) -> str:
        assert expected_status == self.command.get("resolved_entry_status")
        self.delivery_control_acquisitions += 1
        if self.delivery_control_hook is not None:
            await self.delivery_control_hook()
        return self.command_id

    async def assert_delivery_control(self, expected_status: str) -> None:
        assert expected_status == self.command.get("resolved_entry_status")
        self.delivery_control_checks += 1
        if self.delivery_control_hook is not None:
            await self.delivery_control_hook()

    async def assert_disposition_authority(self) -> None:
        self.disposition_authority_checks += 1
        if self.disposition_authority_hook is not None:
            await self.disposition_authority_hook()


class _RouteDB:
    """Minimum DB surface for exercising the real legacy route with a runner."""

    def __init__(self, job: dict) -> None:
        self.job = job
        self.execute_count = 0
        self.status_write_count = 0
        self.status_writes: list[dict[str, object]] = []

    async def get_job(self, _job_id: str) -> dict:
        return copy.deepcopy(self.job)

    @asynccontextmanager
    async def acquire(self):
        database = self

        class _Connection:
            async def fetchrow(self, _sql: str, *_args):
                row = copy.deepcopy(database.job)
                row["db_now_epoch"] = datetime.now(timezone.utc).timestamp()
                return row

            async def execute(self, _sql: str, *_args):
                database.execute_count += 1
                return "UPDATE 1"

        yield _Connection()

    async def update_job_status(self, _job_id: str, **updates) -> bool:
        expected = updates.get("expected_status")
        if expected is not None and self.job["status"] != expected:
            return False
        self.status_write_count += 1
        self.status_writes.append(copy.deepcopy(updates))
        self.job["status"] = updates["status"]
        if updates["status"] == "completed":
            self.job["completed_at"] = "set"
        if "assigned_agent_id" in updates:
            self.job["assigned_agent_id"] = None
        return True


def _route_job(*, status: str = "processing") -> dict:
    return {
        "id": JOB_ID,
        "status": status,
        "execution_lane": "pinned",
        "assigned_agent_id": str(AGENT_ID),
        "freeze_data": None,
        "context": {},
        "parent_job_id": None,
        "project_id": None,
        "user_id": None,
        "config_name": "defaults",
        "config_override": None,
        "resolved_config": {
            "agent": {
                "autonomy": "full",
                "verification": {"enabled": False},
                "curator": {"enabled": False},
            }
        },
        "cloud_diff_baseline_commit": None,
        "merge_status": None,
        "repo_name": None,
        "branch_name": None,
    }


def _body() -> orchestrator.main.JobCompleteRequest:
    return orchestrator.main.JobCompleteRequest(
        should_stop=True,
        goal_achieved=True,
        error=None,
        freeze_data={"freeze_type": "job_complete", "summary": "done"},
        lease_token=17,
        agent_id=AGENT_ID,
        client_report_id=REPORT_ID,
    )


def _accepted(
    disposition: str,
    *,
    state: str = "pending",
    outcome: dict | None = None,
    winning_report_seq: int | None = None,
    abandoned_effects: tuple[str, ...] = (),
    queue_terminalized: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        disposition=disposition,
        command_id=COMMAND_ID,
        job_id=JOB_ID,
        report_seq=3,
        state=state,
        stored_payload={},
        outcome=outcome,
        winning_report_seq=winning_report_seq,
        abandoned_effects=abandoned_effects,
        client_report_id=str(REPORT_ID),
        queue_terminalized=queue_terminalized,
        accepted_job_status="processing",
    )


def _response_json(response: JSONResponse) -> dict:
    return json.loads(response.body.decode("utf-8"))


def _journaled_entry(
    *,
    resolution: str | None,
    infra_attempts: int = 0,
    memory_retries: int = 0,
    llm_outage: dict[str, object] | None = None,
) -> dict[str, object]:
    """Fixed-cardinality S1 decision snapshot used by crash-resume tests."""

    return {
        "entry_status": "processing",
        "entry_assigned_agent_id": str(AGENT_ID),
        "entry_updated_at": None,
        "matched": False,
        "entry_needs_vm": False,
        "entry_parent_status": None,
        "entry_resolution": resolution,
        "entry_infra_transient_attempts": infra_attempts,
        "entry_memory_retry_count": memory_retries,
        "entry_llm_outage": llm_outage
        or {
            "attempt": 0,
            "first_failed_at": None,
            "last_failed_at": None,
            "next_retry_at": None,
            "fingerprint": None,
            "repeat_key": None,
            "repeats": 0,
            "shape_nudge_attempted": False,
        },
    }


def _forbid_finalizer(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    getter = MagicMock(side_effect=AssertionError("finalizer must not be accessed"))
    monkeypatch.setattr(orchestrator.main._completion_runtime, "finalizer", getter)
    return getter


def _patch_normal_route_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    *,
    database: _RouteDB,
    terminal_effects: AsyncMock,
    workspace_cleanup: AsyncMock,
) -> None:
    """Isolate the two independently retryable tail groups under test."""

    monkeypatch.setattr(orchestrator.main, "postgres_db", database)
    monkeypatch.setattr(
        orchestrator.main, "gitea_client", SimpleNamespace(is_initialized=False)
    )
    monkeypatch.setattr(orchestrator.main, "vector_db", None)
    monkeypatch.setattr(
        orchestrator.main.thread_retirement_operations.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        workspace_cleanup,
    )
    monkeypatch.setattr(orchestrator.main, "maybe_wake_session", AsyncMock())
    monkeypatch.setattr(orchestrator.main, "_trigger_dispatch", MagicMock())
    monkeypatch.setattr(orchestrator.main, "_kick_session_wake_drain", MagicMock())
    for owner, helper in (
        (
            orchestrator.main.verification_operations,
            "handle_critic_verdict_on_complete",
        ),
        (orchestrator.main.subjob_completion_operations, "handle_scholar_completion"),
        (
            orchestrator.main.subjob_completion_operations,
            "handle_delegation_child_completion",
        ),
        (orchestrator.main.verification_operations, "trigger_verification_on_complete"),
        (orchestrator.main.project_loop_advance_service, "advance_project_loop"),
    ):
        monkeypatch.setattr(owner, helper, AsyncMock())

    from orchestrator.services import completion as completion_service

    monkeypatch.setattr(
        completion_service,
        "apply_deliverable_gate",
        AsyncMock(
            side_effect=lambda _job, _result, status, **_kwargs: (status, [], False)
        ),
    )
    monkeypatch.setattr(
        completion_service,
        "apply_terminal_job_side_effects",
        terminal_effects,
    )


@pytest.mark.asyncio
async def test_effect_runner_replays_early_return_without_reentering_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job(status="completed"))
    runner = _RecordingRunner()
    monkeypatch.setattr(orchestrator.main, "postgres_db", database)

    first = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )
    replay = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    expected = {
        "status": "handled",
        "job_id": JOB_ID,
        "new_status": "completed",
        "actions": ["late callback ignored; job already completed"],
    }
    assert first == expected
    assert replay == expected
    assert runner.order == [("late_callback_guard", "entry")]
    assert runner.callback_counts == {"late_callback_guard": 1}
    assert await runner.has_completed("late_callback_guard")
    assert await runner.completed_detail("late_callback_guard") == {
        "entry_status": "completed",
        "entry_assigned_agent_id": str(AGENT_ID),
        "entry_updated_at": None,
        "matched": True,
        "entry_needs_vm": False,
        "entry_parent_status": None,
        "entry_resolution": "completed",
        "entry_infra_transient_attempts": 0,
        "entry_memory_retry_count": 0,
        "entry_llm_outage": {
            "attempt": 0,
            "first_failed_at": None,
            "last_failed_at": None,
            "next_retry_at": None,
            "fingerprint": None,
            "repeat_key": None,
            "repeats": 0,
            "shape_nudge_attempted": False,
        },
    }
    assert database.status_write_count == 0


@pytest.mark.parametrize("late_status", ["completed", "pending_review", "reviewing"])
@pytest.mark.asyncio
async def test_late_noop_runs_only_deferred_s36_handoff(
    monkeypatch: pytest.MonkeyPatch,
    late_status: str,
) -> None:
    database = _RouteDB(_route_job(status=late_status))
    runner = _RecordingRunner()
    runner.teardown_handoff = True
    terminal_effects = AsyncMock(return_value={"actions": ["must not run"]})
    workspace_cleanup = AsyncMock(return_value=["workspace archived"])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    assert result == {
        "status": "handled",
        "job_id": JOB_ID,
        "new_status": late_status,
        "actions": [
            f"late callback ignored; job already {late_status}",
            "workspace archived",
        ],
    }
    assert runner.order == [
        ("late_callback_guard", "entry"),
        ("workspace_archive_teardown", "workspace_teardown"),
    ]
    assert runner.details["workspace_archive_teardown"] == {
        "actions": ["workspace archived"],
        "teardown_disposition": "completed",
    }
    assert database.status_write_count == 0
    terminal_effects.assert_not_awaited()
    workspace_cleanup.assert_awaited_once_with(JOB_ID)


@pytest.mark.asyncio
async def test_review_late_noop_without_handoff_does_not_run_s36(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job(status="pending_review"))
    runner = _RecordingRunner()
    workspace_cleanup = AsyncMock(return_value=["must not run"])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=AsyncMock(return_value={"actions": ["must not run"]}),
        workspace_cleanup=workspace_cleanup,
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    assert result["new_status"] == "pending_review"
    assert result["actions"] == ["late callback ignored; job already pending_review"]
    assert runner.order == [("late_callback_guard", "entry")]
    workspace_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_intermediate_late_noop_defers_s36_to_still_higher_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job(status="completed"))
    runner = _RecordingRunner()
    runner.teardown_handoff = True
    runner.higher_report_seq = True
    terminal_effects = AsyncMock(return_value={"actions": ["must not run"]})
    workspace_cleanup = AsyncMock(return_value=["must not run"])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    assert result["actions"] == ["late callback ignored; job already completed"]
    assert runner.details["workspace_archive_teardown"] == {
        "actions": [],
        "teardown_disposition": "deferred",
        "higher_report_seq": 2,
    }
    assert database.status_write_count == 0
    terminal_effects.assert_not_awaited()
    workspace_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_effect_runner_reconstructs_normal_result_without_repeating_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job())
    runner = _RecordingRunner()
    runner.command["payload"] = {
        "_accepted_completion_decision": {"tool_call_id": "round-2-tool"}
    }
    terminal_effects = AsyncMock(return_value={"actions": ["terminal durable"]})
    workspace_cleanup = AsyncMock(return_value=["workspace archived"])
    wake = AsyncMock()
    dispatch = MagicMock()
    wake_drain = MagicMock()

    monkeypatch.setattr(orchestrator.main, "postgres_db", database)
    monkeypatch.setattr(
        orchestrator.main, "gitea_client", SimpleNamespace(is_initialized=False)
    )
    monkeypatch.setattr(orchestrator.main, "vector_db", None)
    monkeypatch.setattr(
        orchestrator.main.thread_retirement_operations.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        workspace_cleanup,
    )
    monkeypatch.setattr(orchestrator.main, "maybe_wake_session", wake)
    monkeypatch.setattr(orchestrator.main, "_trigger_dispatch", dispatch)
    monkeypatch.setattr(orchestrator.main, "_kick_session_wake_drain", wake_drain)
    for owner, helper in (
        (
            orchestrator.main.verification_operations,
            "handle_critic_verdict_on_complete",
        ),
        (orchestrator.main.subjob_completion_operations, "handle_scholar_completion"),
        (
            orchestrator.main.subjob_completion_operations,
            "handle_delegation_child_completion",
        ),
        (orchestrator.main.verification_operations, "trigger_verification_on_complete"),
        (orchestrator.main.project_loop_advance_service, "advance_project_loop"),
    ):
        monkeypatch.setattr(owner, helper, AsyncMock())

    from orchestrator.services import completion as completion_service

    monkeypatch.setattr(
        completion_service,
        "apply_deliverable_gate",
        AsyncMock(
            side_effect=lambda _job, _result, status, **_kwargs: (status, [], False)
        ),
    )
    monkeypatch.setattr(
        completion_service,
        "apply_terminal_job_side_effects",
        terminal_effects,
    )

    body = orchestrator.main.JobCompleteRequest(should_stop=True, goal_achieved=True)
    first = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        body,
        _authorized=True,
        _effect_runner=runner,
    )
    replay = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        body,
        _authorized=True,
        _effect_runner=runner,
    )

    expected = {
        "status": "handled",
        "job_id": JOB_ID,
        "new_status": "completed",
        "actions": [
            "status -> completed",
            "terminal durable",
            "workspace archived",
        ],
    }
    assert first == expected
    assert replay == expected
    assert runner.order[0] == ("late_callback_guard", "entry")
    assert runner.order[-1] == ("session_wake_drain_kick", "session_wake_kick")
    assert len(runner.order) == len({name for name, _group in runner.order})
    assert set(runner.callback_counts.values()) == {1}
    assert await runner.has_completed("main_status_write")
    assert "main_status_write" in runner.transactional_names
    assert await runner.completed_detail("workspace_archive_teardown") == {
        "actions": ["workspace archived"],
        "teardown_disposition": "completed",
    }
    assert database.status_write_count == 1
    assert database.status_writes[0]["consume_completion_decision_tool_call_id"] == (
        "round-2-tool"
    )
    assert database.job["status"] == "completed"
    terminal_effects.assert_awaited_once()
    workspace_cleanup.assert_awaited_once_with(JOB_ID)
    wake.assert_awaited_once_with(database, JOB_ID, "completed")
    dispatch.assert_called_once_with()
    wake_drain.assert_called_once_with(database)


@pytest.mark.asyncio
async def test_persisted_reorder_runs_class_b_and_product_delivery_before_s17(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _route_job()
    job["parent_job_id"] = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    database = _RouteDB(job)
    runner = _RecordingRunner()
    runner.command.update(
        status_reorder_enabled=True,
        resolved_entry_status="processing",
    )
    terminal_effects = AsyncMock(return_value={"actions": ["terminal durable"]})
    workspace_cleanup = AsyncMock(return_value=["workspace archived"])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )
    monkeypatch.setattr(
        orchestrator.main.subjob_output_operations,
        "maybe_graft_completed_subjob",
        AsyncMock(
            return_value={
                "status": "grafted",
                "output_path": "outputs/001-worker",
            }
        ),
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    names = [name for name, _group in runner.order]
    assert names.index("critic_verdict") < names.index("subjob_output_graft")
    assert names.index("subjob_output_graft") < names.index(
        "terminal_merge_change_record"
    )
    assert names.index("terminal_merge_change_record") < names.index(
        "main_status_write"
    )
    assert names.index("main_status_write") < names.index("scholar_parent_unblock")
    assert result["new_status"] == "completed"
    assert result["actions"] == [
        "status -> completed",
        "subjob output grafted to outputs/001-worker",
        "terminal durable",
        "workspace archived",
    ]
    assert database.job["status"] == "completed"
    assert runner.delivery_control_acquisitions == 1
    assert runner.delivery_control_checks == 2
    assert runner.entry_authority_checks == 0
    assert runner.disposition_authority_checks == 1


@pytest.mark.asyncio
async def test_reordered_restart_after_s17_skips_pre_status_phase_and_resumes_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _route_job(status="completed")
    job["parent_job_id"] = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    database = _RouteDB(job)
    runner = _RecordingRunner()
    runner.command.update(
        status_reorder_enabled=True,
        resolved_entry_status="processing",
    )
    runner.details.update(
        late_callback_guard=_journaled_entry(resolution="completed"),
        main_status_write={
            "new_status": "completed",
            "had_assigned_agent": True,
            "stash_and_clear_freeze": False,
        },
        critic_verdict={
            "applicable": False,
            "world_cas_won": False,
            "actions": [],
        },
        subjob_output_graft={
            "graft_result": {
                "status": "grafted",
                "output_path": "outputs/001-worker",
            }
        },
        terminal_merge_change_record={"actions": ["terminal durable"]},
    )
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=AsyncMock(
            side_effect=AssertionError("pre-status delivery reran after S17")
        ),
        workspace_cleanup=AsyncMock(return_value=[]),
    )
    graft = AsyncMock(side_effect=AssertionError("pre-status graft reran after S17"))
    monkeypatch.setattr(
        orchestrator.main.subjob_output_operations,
        "maybe_graft_completed_subjob",
        graft,
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    assert runner.delivery_control_acquisitions == 0
    assert runner.delivery_control_checks == 0
    assert database.status_write_count == 0
    graft.assert_not_awaited()
    assert result["actions"][:3] == [
        "status -> completed",
        "subjob output grafted to outputs/001-worker",
        "terminal durable",
    ]


@pytest.mark.asyncio
async def test_persisted_false_preserves_exact_status_first_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _route_job()
    job["parent_job_id"] = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    database = _RouteDB(job)
    runner = _RecordingRunner()
    runner.command.update(
        status_reorder_enabled=False,
        resolved_entry_status="processing",
    )
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=AsyncMock(return_value={"actions": []}),
        workspace_cleanup=AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        orchestrator.main.subjob_output_operations,
        "maybe_graft_completed_subjob",
        AsyncMock(return_value={"status": "skipped", "reason": "test"}),
    )

    await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    names = [name for name, _group in runner.order]
    assert names.index("main_status_write") < names.index("subjob_output_graft")
    assert names.index("subjob_output_graft") < names.index("critic_verdict")
    assert names.index("critic_verdict") < names.index("terminal_merge_change_record")
    assert runner.entry_authority_checks == 0


@pytest.mark.asyncio
async def test_persisted_true_nonterminal_path_preserves_status_first_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _route_job()
    job["parent_job_id"] = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    database = _RouteDB(job)
    runner = _RecordingRunner()
    runner.command.update(
        status_reorder_enabled=True,
        resolved_entry_status="processing",
    )
    runner.details["late_callback_guard"] = _journaled_entry(
        resolution="pending_review"
    )
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=AsyncMock(return_value={"actions": []}),
        workspace_cleanup=AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        orchestrator.main.subjob_output_operations,
        "maybe_graft_completed_subjob",
        AsyncMock(return_value={"status": "skipped", "reason": "test"}),
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    names = [name for name, _group in runner.order]
    assert result["new_status"] == "pending_review"
    assert names.index("main_status_write") < names.index("subjob_output_graft")
    assert names.index("subjob_output_graft") < names.index("critic_verdict")
    assert runner.entry_authority_checks == 0


@pytest.mark.asyncio
async def test_reordered_pending_delivery_withholds_s17_and_all_tail_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _route_job()
    job["parent_job_id"] = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    database = _RouteDB(job)
    runner = _RecordingRunner()
    runner.command.update(
        status_reorder_enabled=True,
        resolved_entry_status="processing",
    )
    terminal_effects = AsyncMock(return_value={"actions": ["terminal durable"]})
    workspace_cleanup = AsyncMock(return_value=["must not run"])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )
    monkeypatch.setattr(
        orchestrator.main.subjob_output_operations,
        "maybe_graft_completed_subjob",
        AsyncMock(side_effect=RuntimeError("graft transport ambiguous")),
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    assert result["new_status"] == "processing"
    assert database.job["status"] == "processing"
    assert runner.pending["subjob_output_graft"] == {
        "graft_result": {
            "status": "error",
            "reason": "graft transport ambiguous",
        }
    }
    assert runner.details["terminal_merge_change_record"] == {
        "actions": ["terminal durable"]
    }
    assert "main_status_write" not in runner.started
    assert not runner.started.intersection(
        {
            "critic_verdict_followup",
            "scholar_parent_unblock",
            "delegation_parent_unblock",
            "verification_critic_spawn",
            "project_loop_advance",
            "session_wake_enqueue",
            "dispatch_trigger",
            "workspace_archive_teardown",
        }
    )
    assert runner.disposition_authority_checks == 0
    workspace_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_reordered_preexisting_s15_delivery_pending_withholds_s17(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job())
    runner = _RecordingRunner()
    runner.command.update(
        status_reorder_enabled=True,
        resolved_entry_status="processing",
    )
    runner.pending["loop_project_cloud_delivery"] = {
        "new_status": "completed",
        "delivery_status": "cloud-applied",
    }
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=AsyncMock(return_value={"actions": ["terminal durable"]}),
        workspace_cleanup=AsyncMock(return_value=["must not run"]),
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    assert result["new_status"] == "processing"
    assert database.job["status"] == "processing"
    assert "main_status_write" not in runner.started
    assert "terminal_merge_change_record" in runner.details


@pytest.mark.asyncio
async def test_reordered_s15_runs_only_after_exact_delivery_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _route_job()
    job["context"] = {"loop_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}
    job["project_id"] = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    database = _RouteDB(job)
    database.get_project = AsyncMock(return_value={"id": job["project_id"]})
    database.update_job_merge_status = AsyncMock(return_value=True)
    database.merge_job_context = AsyncMock(return_value=True)
    runner = _RecordingRunner()
    runner.command.update(
        status_reorder_enabled=True,
        resolved_entry_status="processing",
    )
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=AsyncMock(return_value={"actions": []}),
        workspace_cleanup=AsyncMock(return_value=[]),
    )

    async def deliver(**_kwargs) -> dict[str, object]:
        assert runner.delivery_control_acquisitions == 1
        assert runner.delivery_control_checks == 1
        assert database.job["status"] == "processing"
        return {
            "delivery_status": "cloud-applied",
            "needs_review": False,
            "delivery_sha": "abc123",
            "notes": [],
        }

    from orchestrator.services import job_cloud_baseline

    delivery = AsyncMock(side_effect=deliver)
    monkeypatch.setattr(job_cloud_baseline, "deliver_loop_diff_to_cloud", delivery)
    monkeypatch.setattr(
        orchestrator.main.project_loop_advance_service,
        "prepare_atomic_project_loop_advance",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        orchestrator.main.project_loop_advance_service,
        "materialize_prepared_project_loop_advance",
        AsyncMock(return_value={"applicable": False, "won": False, "actions": []}),
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    delivery.assert_awaited_once()
    names = [name for name, _group in runner.order]
    assert names.index("loop_project_cloud_delivery") < names.index("main_status_write")
    assert result["new_status"] == "completed"
    assert "loop cloud delivery -> cloud-applied" in result["actions"]


@pytest.mark.asyncio
async def test_reordered_entry_authority_loss_prevents_class_b_and_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job())
    runner = _RecordingRunner()
    runner.command.update(
        status_reorder_enabled=True,
        resolved_entry_status="processing",
    )
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=AsyncMock(return_value={"actions": ["must not run"]}),
        workspace_cleanup=AsyncMock(return_value=["must not run"]),
    )

    async def lose_entry_authority() -> None:
        database.job["status"] = "cancelled"
        raise CompletionDispositionSuperseded(
            observed_status="cancelled",
            expected_statuses=("processing",),
        )

    runner.delivery_control_hook = lose_entry_authority

    with pytest.raises(CompletionDispositionSuperseded):
        await b08_helpers.complete_job_legacy(
            MagicMock(),
            JOB_ID,
            _body(),
            _authorized=True,
            _effect_runner=runner,
        )

    assert runner.delivery_control_acquisitions == 1
    assert runner.entry_authority_checks == 0
    assert not runner.started.intersection(
        {
            "critic_verdict",
            "subjob_output_graft",
            "terminal_merge_change_record",
            "main_status_write",
        }
    )


@pytest.mark.asyncio
async def test_reordered_s27_uses_logical_terminal_job_but_followup_stays_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _route_job()
    job["parent_job_id"] = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    job["context"] = {"verification_target": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}
    database = _RouteDB(job)
    runner = _RecordingRunner()
    runner.command.update(
        status_reorder_enabled=True,
        resolved_entry_status="processing",
    )
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=AsyncMock(return_value={"actions": []}),
        workspace_cleanup=AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        orchestrator.main.subjob_output_operations,
        "maybe_graft_completed_subjob",
        AsyncMock(return_value={"status": "skipped", "reason": "critic"}),
    )
    observed_jobs: list[dict] = []

    async def materialize(logical_job: dict, **_kwargs) -> dict:
        observed_jobs.append(copy.deepcopy(logical_job))
        assert database.job["status"] == "processing"
        return {
            "applicable": True,
            "world_cas_won": True,
            "outcome": "approved",
            "target_job_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            "critic_job_id": JOB_ID,
            "new_status": "completed",
            "actions": [],
        }

    followup = AsyncMock(return_value={"actions": ["critic followup"]})
    monkeypatch.setattr(
        orchestrator.main.verification_operations,
        "materialize_critic_verdict_transactional",
        AsyncMock(side_effect=materialize),
    )
    monkeypatch.setattr(
        orchestrator.main.verification_operations,
        "run_critic_verdict_followups",
        followup,
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    names = [name for name, _group in runner.order]
    assert observed_jobs[0]["status"] == "completed"
    assert names.index("critic_verdict") < names.index("main_status_write")
    assert names.index("main_status_write") < names.index("critic_verdict_followup")
    assert result["actions"].index("status -> completed") < result["actions"].index(
        "critic followup"
    )
    followup.assert_awaited_once()


@pytest.mark.asyncio
async def test_pre_m3_terminal_loop_effect_never_synthesizes_new_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A shipped S32 output already includes every legacy consequence."""

    database = _RouteDB(_route_job())
    runner = _RecordingRunner()
    runner.details["project_loop_advance"] = {
        "actions": ["legacy project loop consequence"]
    }
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=AsyncMock(return_value={"actions": []}),
        workspace_cleanup=AsyncMock(return_value=[]),
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    assert "legacy project loop consequence" in result["actions"]
    assert "project_loop_advance_handoff" not in runner.started


@pytest.mark.asyncio
async def test_resume_rejects_same_status_without_this_commands_completed_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job(status="completed"))
    runner = _RecordingRunner()
    runner.details["late_callback_guard"] = {
        "entry_status": "processing",
        "entry_assigned_agent_id": str(AGENT_ID),
        "entry_updated_at": None,
        "matched": False,
    }
    # An intent is not ownership proof.  A human/concurrent writer may have
    # reached the same status while this callback was in flight.
    runner.started.add("main_status_write")
    terminal_effects = AsyncMock(return_value={"actions": []})
    workspace_cleanup = AsyncMock(return_value=[])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )

    with pytest.raises(CompletionDispositionSuperseded) as raised:
        await b08_helpers.complete_job_legacy(
            MagicMock(),
            JOB_ID,
            _body(),
            _authorized=True,
            _effect_runner=runner,
        )

    assert raised.value.observed_status == "completed"
    assert raised.value.expected_statuses == ("processing",)
    assert database.status_write_count == 0
    terminal_effects.assert_not_awaited()
    workspace_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_s17_cas_miss_raises_typed_whole_command_supersede(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job())
    runner = _RecordingRunner()
    runner.command["resolved_entry_status"] = "processing"
    terminal_effects = AsyncMock(return_value={"actions": ["must not run"]})
    workspace_cleanup = AsyncMock(return_value=["must not run"])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )

    async def lose_s17(_job_id: str, **_updates) -> bool:
        database.job["status"] = "cancelled"
        return False

    monkeypatch.setattr(database, "update_job_status", lose_s17)

    with pytest.raises(CompletionDispositionSuperseded) as raised:
        await b08_helpers.complete_job_legacy(
            MagicMock(),
            JOB_ID,
            _body(),
            _authorized=True,
            _effect_runner=runner,
        )

    assert raised.value.observed_status == "cancelled"
    assert raised.value.expected_statuses == ("processing",)
    assert "main_status_write" in runner.started
    assert "main_status_write" not in runner.details
    terminal_effects.assert_not_awaited()
    workspace_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_after_s17_is_fenced_before_any_class_c_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job())
    runner = _RecordingRunner()
    runner.command["resolved_entry_status"] = "processing"
    terminal_effects = AsyncMock(return_value={"actions": ["must not run"]})
    workspace_cleanup = AsyncMock(return_value=["must not run"])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )

    async def cancel_at_class_c_boundary() -> None:
        assert runner.details["main_status_write"]["new_status"] == "completed"
        database.job["status"] = "cancelled"
        raise CompletionDispositionSuperseded(
            observed_status="cancelled",
            expected_statuses=("completed",),
        )

    runner.disposition_authority_hook = cancel_at_class_c_boundary

    with pytest.raises(CompletionDispositionSuperseded) as raised:
        await b08_helpers.complete_job_legacy(
            MagicMock(),
            JOB_ID,
            _body(),
            _authorized=True,
            _effect_runner=runner,
        )

    assert raised.value.observed_status == "cancelled"
    assert raised.value.expected_statuses == ("completed",)
    assert runner.disposition_authority_checks == 1
    assert not runner.started.intersection(
        {
            "subjob_output_graft",
            "critic_verdict",
            "scholar_parent_unblock",
            "delegation_parent_unblock",
            "verification_critic_spawn",
            "curation_final_pass",
            "project_loop_advance",
            "terminal_merge_change_record",
            "session_wake_enqueue",
            "dispatch_trigger",
            "workspace_archive_teardown",
        }
    )
    terminal_effects.assert_not_awaited()
    workspace_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_flag_on_s23_auto_deny_uses_exact_finalizer_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job())
    runner = _RecordingRunner()
    runner.command["resolved_entry_status"] = "processing"
    terminal_effects = AsyncMock(return_value={"actions": []})
    workspace_cleanup = AsyncMock(return_value=[])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )
    monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(
        orchestrator.main,
        "_check_vm_permission",
        AsyncMock(side_effect=HTTPException(status_code=403, detail="not allowed")),
    )
    monkeypatch.setattr(
        orchestrator.main.sudo_gate,
        "insert_vm_upgrade_request",
        AsyncMock(return_value="sudo-request-1"),
    )
    resume = AsyncMock(return_value={"status": "denied_vm_upgrade"})
    monkeypatch.setattr(
        orchestrator.main.job_control_operations.JobControlOperations,
        "resume_job_without_vm_internal",
        resume,
    )
    body = orchestrator.main.JobCompleteRequest(
        should_stop=True,
        goal_achieved=False,
        freeze_data={
            "freeze_type": "vm_upgrade_required",
            "command": "sudo apt install example",
            "reason": "package required",
        },
        lease_token=17,
        agent_id=AGENT_ID,
        client_report_id=REPORT_ID,
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        body,
        _authorized=True,
        _effect_runner=runner,
    )

    resume.assert_awaited_once_with(
        JOB_ID,
        decided_by="system",
        reason="not allowed",
        denied=True,
        completion_owner_command_id=COMMAND_ID,
        completion_owner=runner.owner,
    )
    assert runner.details["auto_deny_resume"] == {"auto_denied": True}
    assert runner.disposition_authority_checks == 1
    assert result["new_status"] == "paused"
    assert "vm upgrade auto-denied" in result["actions"][-1]


@pytest.mark.asyncio
async def test_completed_pre_status_pause_replays_exact_early_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job(status="paused"))
    runner = _RecordingRunner()
    runner.details["late_callback_guard"] = {
        "entry_status": "processing",
        "entry_assigned_agent_id": str(AGENT_ID),
        "entry_updated_at": None,
        "matched": False,
    }
    expected = {
        "status": "handled",
        "job_id": JOB_ID,
        "new_status": "paused",
        "actions": [
            "infra_transient: paused for retry (attempt 1/5, next retry in 5s, "
            "workspace kept)"
        ],
    }
    runner.details["infra_transient_pause"] = copy.deepcopy(expected)
    terminal_effects = AsyncMock(return_value={"actions": []})
    workspace_cleanup = AsyncMock(return_value=[])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )
    body = orchestrator.main.JobCompleteRequest(
        should_stop=True,
        goal_achieved=False,
        error={"type": "infra_transient", "message": "database unavailable"},
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        body,
        _authorized=True,
        _effect_runner=runner,
    )

    assert result == expected
    assert runner.callback_counts["infra_transient_pause"] == 0
    assert database.status_write_count == 0
    terminal_effects.assert_not_awaited()
    workspace_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_infra_retry_ceiling_replay_uses_journaled_entry_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from orchestrator.services.completion import INFRA_TRANSIENT_MAX_ATTEMPTS

    job = _route_job(status="paused")
    job["context"] = {"infra_transient": {"attempts": INFRA_TRANSIENT_MAX_ATTEMPTS}}
    database = _RouteDB(job)
    runner = _RecordingRunner()
    runner.details["late_callback_guard"] = _journaled_entry(
        resolution="failed",
        infra_attempts=INFRA_TRANSIENT_MAX_ATTEMPTS - 1,
    )
    expected = {
        "status": "handled",
        "job_id": JOB_ID,
        "new_status": "paused",
        "actions": ["journaled final infra retry"],
    }
    runner.details["infra_transient_pause"] = copy.deepcopy(expected)
    monkeypatch.setattr(orchestrator.main, "postgres_db", database)

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=False,
            error={
                "type": "infra_transient",
                "message": "database unavailable",
            },
        ),
        _authorized=True,
        _effect_runner=runner,
    )

    assert result == expected
    assert runner.callback_counts["infra_transient_pause"] == 0
    assert "infra_transient_give_up" not in runner.started
    assert database.status_write_count == 0


@pytest.mark.asyncio
async def test_memory_retry_ceiling_replay_uses_journaled_entry_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from orchestrator.services.completion import MEMORY_RETRY_CAP

    job = _route_job(status="paused")
    job["context"] = {"memory_retry_count": MEMORY_RETRY_CAP}
    database = _RouteDB(job)
    runner = _RecordingRunner()
    runner.details["late_callback_guard"] = _journaled_entry(
        resolution="paused",
        memory_retries=MEMORY_RETRY_CAP - 1,
    )
    runner.details["memory_kb_retry_pause"] = {
        "paused": True,
        "retry_count": MEMORY_RETRY_CAP,
    }
    terminal_effects = AsyncMock(return_value={"actions": []})
    workspace_cleanup = AsyncMock(return_value=[])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=False,
            freeze_data={
                "freeze_type": "memory_unavailable",
                "reason": "embedding endpoint unavailable",
            },
        ),
        _authorized=True,
        _effect_runner=runner,
    )

    assert result["new_status"] == "paused"
    assert result["actions"] == [
        "memory_unavailable: re-queued for retry "
        f"(memory_retry_count -> {MEMORY_RETRY_CAP})"
    ]
    assert runner.callback_counts["memory_kb_retry_pause"] == 0
    assert "main_status_write" not in runner.started
    terminal_effects.assert_not_awaited()
    workspace_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_llm_outage_attempt_ceiling_replay_uses_journaled_entry_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from orchestrator.services import completion as completion_service

    max_attempts = 3
    monkeypatch.setattr(completion_service, "LLM_OUTAGE_MAX_ATTEMPTS", max_attempts)
    monkeypatch.setattr(completion_service, "LLM_OUTAGE_CEILING_SECONDS", 43_200)
    now = datetime.now(timezone.utc).isoformat()
    advanced_outage = {
        "attempt": max_attempts,
        "first_failed_at": now,
        "last_failed_at": now,
        "next_retry_at": now,
        "fingerprint": None,
        "repeat_key": None,
        "repeats": 0,
        "shape_nudge_attempted": False,
    }
    entry_outage = {**advanced_outage, "attempt": max_attempts - 1}
    job = _route_job(status="paused")
    job["context"] = {"llm_outage": advanced_outage}
    database = _RouteDB(job)
    runner = _RecordingRunner()
    runner.details["late_callback_guard"] = _journaled_entry(
        resolution="paused",
        llm_outage=entry_outage,
    )
    runner.details["llm_outage_retry_pause"] = {
        "paused": True,
        "attempt": max_attempts,
        "delay": 30.0,
        "next_retry_at": now,
    }
    terminal_effects = AsyncMock(return_value={"actions": []})
    workspace_cleanup = AsyncMock(return_value=[])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=False,
            freeze_data={
                "freeze_type": "llm_unavailable",
                "classification": "provider_unavailable",
                "error_summary": "provider unavailable",
                "model": "test-model",
            },
        ),
        _authorized=True,
        _effect_runner=runner,
    )

    assert result["new_status"] == "paused"
    assert result["actions"] == [
        "llm_unavailable: paused for backoff re-dispatch "
        f"(attempt {max_attempts}, next retry in 30s)"
    ]
    assert runner.callback_counts["llm_outage_retry_pause"] == 0
    assert "llm_give_up_operator_alert" not in runner.started
    assert "main_status_write" not in runner.started
    terminal_effects.assert_not_awaited()
    workspace_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_main_status_effect_omits_large_freeze_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job())
    database.merge_job_context = AsyncMock()
    runner = _RecordingRunner()
    terminal_effects = AsyncMock(return_value={"actions": []})
    workspace_cleanup = AsyncMock(return_value=[])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )
    large_value = "x" * 32_000

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=False,
            freeze_data={
                "freeze_type": "batch_boundary",
                "phase_number": 7,
                "opaque_checkpoint": large_value,
            },
        ),
        _authorized=True,
        _effect_runner=runner,
    )

    assert result["new_status"] == "paused"
    status_detail = runner.details["main_status_write"]
    assert status_detail == {
        "new_status": "paused",
        "had_assigned_agent": True,
        "stash_and_clear_freeze": True,
    }
    assert "fd_row" not in status_detail
    assert large_value not in json.dumps(status_detail)
    assert len(json.dumps(status_detail)) < 1024


@pytest.mark.asyncio
async def test_durable_drain_stall_counter_requires_domain_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job())
    database.merge_job_context = AsyncMock(return_value=False)
    runner = _RecordingRunner()
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=AsyncMock(return_value={"actions": []}),
        workspace_cleanup=AsyncMock(return_value=[]),
    )

    with pytest.raises(HTTPException) as raised:
        await b08_helpers.complete_job_legacy(
            MagicMock(),
            JOB_ID,
            orchestrator.main.JobCompleteRequest(
                should_stop=True,
                goal_achieved=False,
                freeze_data={
                    "freeze_type": "batch_boundary",
                    "phase_number": 7,
                },
            ),
            _authorized=True,
            _effect_runner=runner,
        )

    assert raised.value.status_code == 500
    assert raised.value.detail == "drain-stall counter update did not commit"
    assert isinstance(raised.value.__cause__, RuntimeError)
    assert "drain_stall_counter_alert" not in runner.details
    assert runner.callback_counts["drain_stall_counter_alert"] == 1
    assert "drain_stall_counter_alert" in runner.transactional_names


@pytest.mark.asyncio
async def test_legacy_drain_stall_counter_keeps_best_effort_false_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job())
    database.merge_job_context = AsyncMock(return_value=False)
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=AsyncMock(return_value={"actions": []}),
        workspace_cleanup=AsyncMock(return_value=[]),
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=False,
            freeze_data={
                "freeze_type": "batch_boundary",
                "phase_number": 7,
            },
        ),
        _authorized=True,
    )

    assert result["new_status"] == "paused"
    assert result["actions"] == [
        "status -> paused",
        "cleared agent on paused job (re-dispatchable)",
        "freeze stashed to context (auto-redispatch)",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cleanup_state", "expected_result"),
    [("settled", True), ("superseded", True), ("retryable", False)],
)
async def test_durable_recovery_delete_uses_exact_cleanup_intent(
    monkeypatch: pytest.MonkeyPatch,
    cleanup_state: str,
    expected_result: bool,
) -> None:
    runtime_uid = "55555555-5555-4555-8555-555555555555"
    job = _route_job()
    job["context"] = {
        "workspace_container": {
            "status": "ready",
            "host": "workspace.example",
            "_runtime_incarnation": runtime_uid,
        }
    }
    database = _RouteDB(job)
    runner = _RecordingRunner()
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=AsyncMock(return_value={"actions": []}),
        workspace_cleanup=AsyncMock(return_value=[]),
    )
    prepare_cleanup = AsyncMock(return_value={"intent_generation": 7})
    reconcile_cleanup = AsyncMock(
        return_value=WorkspaceCleanupOutcome(cleanup_state, 7)
    )
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "prepare_workspace_cleanup_intent",
        prepare_cleanup,
    )
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "reconcile_workspace_cleanup_intent",
        reconcile_cleanup,
    )

    async def recovery(_job, job_id, _error, *, delete_workspace, **_kwargs):
        assert await delete_workspace(job_id) is expected_result
        return {
            "status": "handled",
            "job_id": job_id,
            "new_status": "paused",
            "paused": True,
            "actions": ["workspace recovery tested"],
        }

    monkeypatch.setattr(completion_service, "handle_pod_workspace_recovery", recovery)

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=False,
            error={"type": "workspace_unavailable", "message": "sshd gone"},
        ),
        _authorized=True,
        _effect_runner=runner,
    )

    assert result["actions"] == ["workspace recovery tested"]
    prepare_cleanup.assert_awaited_once_with(
        orchestrator.main.WorkspaceOwner.job(JOB_ID),
        expected_runtime_incarnation=runtime_uid,
        target_disposition="deleted",
        reclaim_shared_resources=False,
    )
    reconcile_cleanup.assert_awaited_once_with(
        orchestrator.main.WorkspaceOwner.job(JOB_ID),
        expected_runtime_incarnation=runtime_uid,
        intent_generation=7,
    )


@pytest.mark.asyncio
async def test_recovery_delete_without_runtime_identity_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _route_job()
    job["context"] = {"workspace_container": {"status": "ready"}}
    database = _RouteDB(job)
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=AsyncMock(return_value={"actions": []}),
        workspace_cleanup=AsyncMock(return_value=[]),
    )
    prepare_cleanup = AsyncMock()
    reconcile_cleanup = AsyncMock()
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "prepare_workspace_cleanup_intent",
        prepare_cleanup,
    )
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "reconcile_workspace_cleanup_intent",
        reconcile_cleanup,
    )

    async def recovery(_job, job_id, _error, *, delete_workspace, **_kwargs):
        # A deterministic Pod name is not authority to delete a replacement.
        assert await delete_workspace(job_id) is False
        return {
            "status": "handled",
            "job_id": job_id,
            "new_status": "paused",
            "actions": ["workspace recovery tested"],
        }

    monkeypatch.setattr(completion_service, "handle_pod_workspace_recovery", recovery)

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=False,
            error={"type": "workspace_unavailable", "message": "sshd gone"},
        ),
        _authorized=True,
    )

    assert result == {
        "status": "handled",
        "job_id": JOB_ID,
        "new_status": "paused",
        "actions": ["workspace recovery tested"],
    }
    prepare_cleanup.assert_not_awaited()
    reconcile_cleanup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("release_disposition", "expected_reset"),
    (("completed", True), ("process_zero_unproven", False)),
)
async def test_vm_recovery_retires_exact_runtime_before_context_reset(
    monkeypatch: pytest.MonkeyPatch,
    release_disposition: str,
    expected_reset: bool,
) -> None:
    from orchestrator.services.vm_provisioner import (
        VMTeardownIdentity,
        VMTeardownResult,
    )

    generation = "00000000-0000-4000-8000-000000000001"
    job = _route_job()
    job["context"] = {
        "vm": {
            "requested": True,
            "status": "ready",
            "provision_generation": generation,
            "vm_uid": "vm-uid-a",
            "rootdisk_pvc_uid": "root-uid-a",
            "ssh_host": "vm.internal",
            "ssh_port": 22,
            "ssh_host_key_fingerprint": "SHA256:" + ("A" * 43),
        }
    }
    database = _RouteDB(job)
    database.merge_job_context = AsyncMock(return_value=True)
    database.pause_job = AsyncMock(return_value=True)
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=AsyncMock(return_value={"actions": []}),
        workspace_cleanup=AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", False)
    identity = VMTeardownIdentity(
        generation,
        "vm-uid-a",
        "root-uid-a",
        ssh_host="vm.internal",
        ssh_port=22,
        ssh_host_key_fingerprint="SHA256:" + ("A" * 43),
    )
    calls: list[str] = []

    async def capture(*_args, **_kwargs):
        calls.append("capture")
        return identity

    async def release(*_args, **_kwargs):
        calls.append("release")
        return VMTeardownResult(
            release_disposition,
            release_disposition == "completed",
        )

    async def merge(*_args, **_kwargs):
        calls.append("reset")
        return True

    monkeypatch.setattr(
        orchestrator.main.vm_provisioner,
        "capture_vm_teardown_identity",
        AsyncMock(side_effect=capture),
    )
    monkeypatch.setattr(
        orchestrator.main.vm_provisioner,
        "release_vm_captured",
        AsyncMock(side_effect=release),
    )
    database.merge_job_context.side_effect = merge

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=False,
            error={"type": "workspace_unavailable", "message": "sshd gone"},
        ),
        _authorized=True,
    )

    assert calls == (
        ["capture", "release", "reset"] if expected_reset else ["capture", "release"]
    )
    orchestrator.main.vm_provisioner.capture_vm_teardown_identity.assert_awaited_once_with(
        JOB_ID,
        entity_type="job",
    )
    orchestrator.main.vm_provisioner.release_vm_captured.assert_awaited_once_with(
        JOB_ID,
        identity,
        purge_disk=False,
        entity_type="job",
        capture_snapshot=False,
    )
    if expected_reset:
        database.merge_job_context.assert_awaited_once_with(
            JOB_ID,
            {
                "vm": {
                    "requested": True,
                    "recovering": True,
                    "previous_error": "workspace_unavailable",
                    "rootdisk": "kept",
                }
            },
        )
        orchestrator.main._trigger_dispatch.assert_called_once_with()
    else:
        database.merge_job_context.assert_not_awaited()
        orchestrator.main._trigger_dispatch.assert_not_called()
    assert result["actions"] == [
        (
            "vm recovery: old VM deleted, new VM will be provisioned, job re-queued"
            if expected_reset
            else "vm recovery: old VM delete REFUSED — recreate will likely fail on the stale cloud-init Secret"
        )
    ]


@pytest.mark.asyncio
async def test_terminal_delivery_failure_does_not_block_teardown_and_replays_only_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job())
    runner = _RecordingRunner()
    terminal_effects = AsyncMock(
        side_effect=[
            RuntimeError("gitea unavailable"),
            {"actions": ["terminal durable"]},
        ]
    )
    workspace_cleanup = AsyncMock(return_value=["workspace archived"])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )
    body = orchestrator.main.JobCompleteRequest(should_stop=True, goal_achieved=True)

    first = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        body,
        _authorized=True,
        _effect_runner=runner,
    )

    assert first["actions"] == ["status -> completed", "workspace archived"]
    assert runner.pending["terminal_merge_change_record"] == {
        "actions": [],
        "error": "gitea unavailable",
    }
    assert "terminal_merge_change_record" not in runner.details
    assert runner.details["workspace_archive_teardown"] == {
        "actions": ["workspace archived"],
        "teardown_disposition": "completed",
    }
    assert {
        "terminal_merge_change_record",
        "workspace_archive_teardown",
    }.issubset(runner.retry_if_names)
    terminal_effects.assert_awaited_once()
    workspace_cleanup.assert_awaited_once_with(JOB_ID)

    replay = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        body,
        _authorized=True,
        _effect_runner=runner,
    )

    assert replay["actions"] == [
        "status -> completed",
        "terminal durable",
        "workspace archived",
    ]
    assert "terminal_merge_change_record" not in runner.pending
    assert runner.details["terminal_merge_change_record"] == {
        "actions": ["terminal durable"]
    }
    assert runner.callback_counts["terminal_merge_change_record"] == 2
    assert runner.callback_counts["workspace_archive_teardown"] == 1
    assert terminal_effects.await_count == 2
    workspace_cleanup.assert_awaited_once_with(JOB_ID)


@pytest.mark.asyncio
async def test_session_wake_failure_does_not_block_independent_teardown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job())
    runner = _RecordingRunner()
    terminal_effects = AsyncMock(return_value={"actions": ["terminal durable"]})
    workspace_cleanup = AsyncMock(return_value=["workspace archived"])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )
    wake = AsyncMock(side_effect=RuntimeError("session wake store unavailable"))
    monkeypatch.setattr(orchestrator.main, "maybe_wake_session", wake)

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        orchestrator.main.JobCompleteRequest(should_stop=True, goal_achieved=True),
        _authorized=True,
        _effect_runner=runner,
    )

    assert result["actions"] == [
        "status -> completed",
        "terminal durable",
        "workspace archived",
    ]
    assert runner.pending["session_wake_enqueue"] == {
        "enqueued": False,
        "error": "session wake store unavailable",
    }
    assert "session_wake_enqueue" not in runner.details
    assert runner.details["workspace_archive_teardown"] == {
        "actions": ["workspace archived"],
        "teardown_disposition": "completed",
    }
    assert runner.order.index(("session_wake_enqueue", "session_wake_enqueue")) < (
        runner.order.index(("workspace_archive_teardown", "workspace_teardown"))
    )
    wake.assert_awaited_once_with(database, JOB_ID, "completed")
    workspace_cleanup.assert_awaited_once_with(JOB_ID)


@pytest.mark.asyncio
async def test_session_wake_retry_policy_is_dark_without_effect_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job())
    terminal_effects = AsyncMock(return_value={"actions": ["terminal durable"]})
    workspace_cleanup = AsyncMock(return_value=["workspace archived"])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )
    failure = RuntimeError("legacy session wake failure")
    wake = AsyncMock(side_effect=failure)
    monkeypatch.setattr(orchestrator.main, "maybe_wake_session", wake)

    with pytest.raises(HTTPException) as caught:
        await b08_helpers.complete_job_legacy(
            MagicMock(),
            JOB_ID,
            orchestrator.main.JobCompleteRequest(should_stop=True, goal_achieved=True),
            _authorized=True,
            _effect_runner=None,
        )

    assert caught.value.status_code == 500
    assert caught.value.detail == "legacy session wake failure"
    assert caught.value.__cause__ is failure
    wake.assert_awaited_once_with(database, JOB_ID, "completed")
    workspace_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_teardown_failure_stays_pending_and_replay_skips_done_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job())
    runner = _RecordingRunner()
    terminal_effects = AsyncMock(return_value={"actions": ["terminal durable"]})
    workspace_cleanup = AsyncMock(
        side_effect=[RuntimeError("kubernetes unavailable"), ["workspace archived"]]
    )
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=workspace_cleanup,
    )
    body = orchestrator.main.JobCompleteRequest(should_stop=True, goal_achieved=True)

    first = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        body,
        _authorized=True,
        _effect_runner=runner,
    )

    assert first["actions"] == [
        "status -> completed",
        "terminal durable",
        "workspace cleanup failed: kubernetes unavailable",
    ]
    assert runner.details["terminal_merge_change_record"] == {
        "actions": ["terminal durable"]
    }
    assert runner.pending["workspace_archive_teardown"] == {
        "actions": ["workspace cleanup failed: kubernetes unavailable"],
        "error": "kubernetes unavailable",
        "teardown_disposition": "retry_pending",
    }
    terminal_effects.assert_awaited_once()
    workspace_cleanup.assert_awaited_once_with(JOB_ID)

    replay = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        body,
        _authorized=True,
        _effect_runner=runner,
    )

    assert replay["actions"] == [
        "status -> completed",
        "terminal durable",
        "workspace archived",
    ]
    assert "workspace_archive_teardown" not in runner.pending
    assert runner.details["workspace_archive_teardown"] == {
        "actions": ["workspace archived"],
        "teardown_disposition": "completed",
    }
    assert runner.callback_counts["terminal_merge_change_record"] == 1
    assert runner.callback_counts["workspace_archive_teardown"] == 2
    terminal_effects.assert_awaited_once()
    assert workspace_cleanup.await_count == 2


@pytest.mark.asyncio
async def test_cancel_after_s17_supersedes_before_s36_without_settling_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job(status="cancelled"))
    runner = _RecordingRunner()
    runner.details["main_status_write"] = {
        "new_status": "completed",
        "had_assigned_agent": True,
        "stash_and_clear_freeze": False,
    }
    runner.teardown_authority_disposition = "world_state_superseded"
    workspace_cleanup = AsyncMock(return_value=["must not release"])
    monkeypatch.setattr(orchestrator.main, "postgres_db", database)
    monkeypatch.setattr(
        orchestrator.main.thread_retirement_operations.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        workspace_cleanup,
    )

    with pytest.raises(CompletionDispositionSuperseded) as raised:
        await b08_helpers.run_completion_workspace_teardown(JOB_ID, runner)

    assert raised.value.observed_status == "cancelled"
    assert raised.value.expected_statuses == ("completed",)
    assert raised.value.reason == "workspace_teardown_status_superseded"
    assert runner.pending["workspace_archive_teardown"] == {
        "actions": [],
        "error": "jobs status changed before workspace teardown authorization",
        "teardown_disposition": "world_state_superseded",
        "observed_status": "cancelled",
        "expected_status": "completed",
    }
    assert "workspace_archive_teardown" not in runner.details
    assert runner.probe_order == []
    workspace_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_active_s36_marker_status_drift_parks_without_clearing_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job(status="cancelled"))
    runner = _RecordingRunner()
    runner.teardown_authority_disposition = "operator_hold"
    workspace_cleanup = AsyncMock(return_value=["must not release"])
    monkeypatch.setattr(orchestrator.main, "postgres_db", database)
    monkeypatch.setattr(
        orchestrator.main.thread_retirement_operations.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        workspace_cleanup,
    )

    output = await b08_helpers.run_completion_workspace_teardown(JOB_ID, runner)

    assert output == {
        "actions": [],
        "error": (
            "workspace teardown authorization marker conflicts with current jobs status"
        ),
        "teardown_disposition": "operator_hold",
        "observed_status": "cancelled",
        "expected_status": "completed",
    }
    assert runner.pending["workspace_archive_teardown"] == output
    assert "workspace_archive_teardown" not in runner.details
    assert runner.probe_order == []
    workspace_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_workspace_recovery_hold_blocks_completion_cleanup_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _RouteDB(_route_job(status="completed"))
    runner = _RecordingRunner()
    workspace_cleanup = AsyncMock(return_value=["must not release"])
    deny_store = MagicMock()
    deny_store.acquire_cleanup_permit = AsyncMock(
        return_value=SimpleNamespace(allowed=False)
    )
    prior_dependencies = orchestrator.main._completion_effect_dependencies

    def dependencies():
        return dataclasses.replace(prior_dependencies(), recovery_store=deny_store)

    monkeypatch.setattr(orchestrator.main, "postgres_db", database)
    monkeypatch.setattr(
        orchestrator.main, "_completion_effect_dependencies", dependencies
    )
    monkeypatch.setattr(
        orchestrator.main.thread_retirement_operations.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        workspace_cleanup,
    )

    output = await b08_helpers.run_completion_workspace_teardown(JOB_ID, runner)

    assert output["teardown_disposition"] == "retry_pending"
    assert "held for unresolved workspace recovery" in output["error"]
    deny_store.acquire_cleanup_permit.assert_awaited_once()
    workspace_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_flagged_vm_teardown_captures_replays_and_archives_exact_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from orchestrator.services.vm_provisioner import (
        VMTeardownIdentity,
        VMTeardownResult,
    )

    host_key = "SHA256:" + ("A" * 43)
    job = _route_job()
    job["context"] = {
        "vm": {
            "status": "ready",
            "provision_generation": "00000000-0000-4000-8000-000000000001",
            "vm_uid": "vm-uid-a",
            "rootdisk_pvc_uid": "root-uid-a",
            "ssh_host": "100.64.0.8",
            "ssh_port": 22,
            "ssh_host_key_fingerprint": host_key,
        }
    }
    database = _RouteDB(job)
    runner = _RecordingRunner()
    identity = VMTeardownIdentity(
        "00000000-0000-4000-8000-000000000001",
        "vm-uid-a",
        "root-uid-a",
        ssh_host="100.64.0.8",
        ssh_port=22,
        ssh_host_key_fingerprint=host_key,
    )
    capture = AsyncMock(return_value=identity)
    release = AsyncMock(return_value=VMTeardownResult("completed", True))
    legacy_cleanup = AsyncMock(return_value=["legacy cleanup"])
    monkeypatch.setattr(orchestrator.main, "postgres_db", database)
    monkeypatch.setattr(
        orchestrator.main.vm_provisioner, "capture_vm_teardown_identity", capture
    )
    monkeypatch.setattr(
        orchestrator.main.vm_provisioner, "release_vm_captured", release
    )
    monkeypatch.setattr(
        orchestrator.main.thread_retirement_operations.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        legacy_cleanup,
    )

    first = await b08_helpers.run_completion_workspace_teardown(JOB_ID, runner)
    replay = await b08_helpers.run_completion_workspace_teardown(JOB_ID, runner)

    assert (
        first
        == replay
        == {
            "actions": ["vm released"],
            "teardown_disposition": "completed",
        }
    )
    assert runner.intents["workspace_archive_teardown"] == {
        "kind": "vm",
        "provision_generation": identity.provision_generation,
        "vm_uid": "vm-uid-a",
        "rootdisk_pvc_uid": "root-uid-a",
        "ssh_host": "100.64.0.8",
        "ssh_port": 22,
        "ssh_host_key_fingerprint": host_key,
    }
    capture.assert_awaited_once_with(JOB_ID)
    release.assert_awaited_once_with(
        JOB_ID,
        identity,
        ssh_host="100.64.0.8",
        ssh_port=22,
    )
    legacy_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_vm_identity_mismatch_supersedes_only_s36_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from orchestrator.services.vm_provisioner import (
        VMTeardownIdentity,
        VMTeardownResult,
    )

    generation = "00000000-0000-4000-8000-000000000001"
    host_key = "SHA256:" + ("A" * 43)
    job = _route_job()
    job["context"] = {"vm": {"status": "ready"}}
    database = _RouteDB(job)
    runner = _RecordingRunner()
    runner.intents["workspace_archive_teardown"] = {
        "kind": "vm",
        "provision_generation": generation,
        "vm_uid": "old-vm-uid",
        "rootdisk_pvc_uid": "old-root-uid",
        "ssh_host": "100.64.0.8",
        "ssh_port": 22,
        "ssh_host_key_fingerprint": host_key,
    }
    release = AsyncMock(return_value=VMTeardownResult("identity_superseded", False))
    monkeypatch.setattr(orchestrator.main, "postgres_db", database)
    monkeypatch.setattr(
        orchestrator.main.vm_provisioner, "release_vm_captured", release
    )
    monkeypatch.setattr(
        orchestrator.main.vm_provisioner,
        "capture_vm_teardown_identity",
        AsyncMock(side_effect=AssertionError("must replay captured intent")),
    )

    output = await b08_helpers.run_completion_workspace_teardown(JOB_ID, runner)

    assert output["teardown_disposition"] == "identity_superseded"
    assert runner.superseded_names == {"workspace_archive_teardown"}
    release.assert_awaited_once_with(
        JOB_ID,
        VMTeardownIdentity(
            generation,
            "old-vm-uid",
            "old-root-uid",
            ssh_host="100.64.0.8",
            ssh_port=22,
            ssh_host_key_fingerprint=host_key,
        ),
        ssh_host="100.64.0.8",
        ssh_port=22,
    )


@pytest.mark.asyncio
async def test_docker_vm_s36_keeps_durable_legacy_cleanup_without_identity_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _route_job()
    job["context"] = {
        "vm": {
            "status": "ready",
            "provisioner": "docker",
            "ssh_host": "127.0.0.1",
            "ssh_port": 2222,
        }
    }
    runner = _RecordingRunner()
    legacy_cleanup = AsyncMock(return_value=["docker vm released"])
    monkeypatch.setattr(orchestrator.main, "postgres_db", _RouteDB(job))
    monkeypatch.setattr(
        orchestrator.main.thread_retirement_operations.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        legacy_cleanup,
    )
    monkeypatch.setattr(
        orchestrator.main.vm_provisioner,
        "capture_vm_teardown_identity",
        AsyncMock(side_effect=AssertionError("Docker has no KubeVirt identity")),
    )

    first = await b08_helpers.run_completion_workspace_teardown(JOB_ID, runner)
    replay = await b08_helpers.run_completion_workspace_teardown(JOB_ID, runner)

    assert (
        first
        == replay
        == {
            "actions": ["docker vm released"],
            "teardown_disposition": "completed",
        }
    )
    assert "workspace_archive_teardown" not in runner.intents
    assert runner.callback_counts["workspace_archive_teardown"] == 1
    legacy_cleanup.assert_awaited_once_with(JOB_ID)


@pytest.mark.asyncio
async def test_hybrid_vm_and_kubernetes_s36_captures_and_releases_both(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from orchestrator.services.vm_provisioner import (
        VMTeardownIdentity,
        VMTeardownResult,
    )

    generation = "00000000-0000-4000-8000-000000000001"
    host_key = "SHA256:" + ("A" * 43)
    workspace_identity = orchestrator.main.WorkspaceTeardownIdentity(
        pod_uid="pod-uid-a",
        pvc_uid="pvc-uid-a",
        service_uid="service-uid-a",
        pod_ip="10.0.0.8",
        ssh_host_key_fingerprint=host_key,
    )
    vm_identity = VMTeardownIdentity(
        generation,
        "vm-uid-a",
        "root-uid-a",
        ssh_host="100.64.0.8",
        ssh_port=22,
        ssh_host_key_fingerprint=host_key,
    )
    job = _route_job()
    job["context"] = {
        "workspace_container": {"status": "ready", "provisioner": "k8s"},
        "vm": {
            "status": "ready",
            "provisioner": "kubevirt",
            "ssh_host": "100.64.0.8",
            "ssh_port": 22,
            "ssh_host_key_fingerprint": host_key,
        },
    }
    runner = _RecordingRunner()
    release_order: list[str] = []

    async def release_vm(*_args, **_kwargs):
        release_order.append("vm")
        return VMTeardownResult("completed", True)

    async def release_kubernetes(*_args, **_kwargs):
        release_order.append("kubernetes")
        return True

    monkeypatch.setattr(orchestrator.main, "postgres_db", _RouteDB(job))
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "capture_terminal_workspace_identity",
        AsyncMock(return_value=workspace_identity),
    )
    monkeypatch.setattr(
        orchestrator.main.vm_provisioner,
        "capture_vm_teardown_identity",
        AsyncMock(return_value=vm_identity),
    )
    release_vm_mock = AsyncMock(side_effect=release_vm)
    release_kubernetes_mock = AsyncMock(side_effect=release_kubernetes)
    monkeypatch.setattr(
        orchestrator.main.vm_provisioner, "release_vm_captured", release_vm_mock
    )
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "release_workspace",
        release_kubernetes_mock,
    )
    legacy_cleanup = AsyncMock(side_effect=AssertionError("must stay UID fenced"))
    monkeypatch.setattr(
        orchestrator.main.thread_retirement_operations.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        legacy_cleanup,
    )

    first = await b08_helpers.run_completion_workspace_teardown(JOB_ID, runner)
    replay = await b08_helpers.run_completion_workspace_teardown(JOB_ID, runner)

    assert (
        first
        == replay
        == {
            "actions": ["vm released", "k8s workspace released"],
            "teardown_disposition": "completed",
        }
    )
    intent = runner.intents["workspace_archive_teardown"]
    assert intent["kind"] == "vm_and_kubernetes"
    assert intent["vm"] == {
        "provision_generation": generation,
        "vm_uid": "vm-uid-a",
        "rootdisk_pvc_uid": "root-uid-a",
        "ssh_host": "100.64.0.8",
        "ssh_port": 22,
        "ssh_host_key_fingerprint": host_key,
    }
    assert intent["kubernetes"] == {
        "pod_uid": "pod-uid-a",
        "pvc_uid": "pvc-uid-a",
        "service_uid": "service-uid-a",
        "pod_ip": "10.0.0.8",
        "ssh_host_key_fingerprint": host_key,
        "ssh_port": 30022,
        "snapshot_generation": COMMAND_ID,
        "snapshot_created_at": intent["kubernetes"]["snapshot_created_at"],
    }
    assert release_order == ["vm", "kubernetes"]
    assert runner.callback_counts["workspace_archive_teardown"] == 1
    legacy_cleanup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", ["vm", "kubernetes"])
async def test_hybrid_s36_replacement_supersedes_and_preserves_other_names(
    monkeypatch: pytest.MonkeyPatch,
    replacement: str,
) -> None:
    from orchestrator.services.vm_provisioner import (
        VMTeardownIdentity,
        VMTeardownResult,
    )

    generation = "00000000-0000-4000-8000-000000000001"
    host_key = "SHA256:" + ("A" * 43)
    workspace_identity = orchestrator.main.WorkspaceTeardownIdentity(
        pod_uid="old-pod-uid",
        pvc_uid="old-pvc-uid",
        service_uid="old-service-uid",
        pod_ip="10.0.0.8",
        ssh_host_key_fingerprint=host_key,
    )
    job = _route_job()
    job["context"] = {
        "workspace_container": {"status": "ready", "provisioner": "k8s"},
        "vm": {
            "status": "ready",
            "provisioner": "kubevirt",
            "ssh_host": "100.64.0.8",
            "ssh_port": 22,
            "ssh_host_key_fingerprint": host_key,
        },
    }
    runner = _RecordingRunner()
    monkeypatch.setattr(orchestrator.main, "postgres_db", _RouteDB(job))
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "capture_terminal_workspace_identity",
        AsyncMock(return_value=workspace_identity),
    )
    monkeypatch.setattr(
        orchestrator.main.vm_provisioner,
        "capture_vm_teardown_identity",
        AsyncMock(
            return_value=VMTeardownIdentity(
                generation,
                "old-vm-uid",
                "old-root-uid",
                ssh_host="100.64.0.8",
                ssh_port=22,
                ssh_host_key_fingerprint=host_key,
            )
        ),
    )
    release_vm = AsyncMock(
        return_value=VMTeardownResult(
            "identity_superseded" if replacement == "vm" else "completed",
            False if replacement == "vm" else True,
        )
    )
    release_kubernetes = AsyncMock(return_value=replacement == "vm")
    classify_kubernetes = AsyncMock(return_value="identity_superseded")
    monkeypatch.setattr(
        orchestrator.main.vm_provisioner, "release_vm_captured", release_vm
    )
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "release_workspace",
        release_kubernetes,
    )
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "classify_workspace_teardown_identity",
        classify_kubernetes,
    )
    legacy_cleanup = AsyncMock(side_effect=AssertionError("must preserve successors"))
    monkeypatch.setattr(
        orchestrator.main.thread_retirement_operations.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        legacy_cleanup,
    )

    output = await b08_helpers.run_completion_workspace_teardown(JOB_ID, runner)

    assert output["teardown_disposition"] == "identity_superseded"
    assert runner.superseded_names == {"workspace_archive_teardown"}
    legacy_cleanup.assert_not_awaited()
    release_kubernetes.assert_awaited_once()
    if replacement == "vm":
        classify_kubernetes.assert_not_awaited()
        assert output["actions"] == ["k8s workspace released"]
    else:
        classify_kubernetes.assert_awaited_once_with(
            orchestrator.main.WorkspaceOwner.job(JOB_ID), workspace_identity
        )
        assert output["actions"] == ["vm released"]


@pytest.mark.asyncio
async def test_hybrid_s36_retry_precedes_replacement_supersede(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from orchestrator.services.vm_provisioner import VMTeardownResult

    generation = "00000000-0000-4000-8000-000000000001"
    host_key = "SHA256:" + ("A" * 43)
    runner = _RecordingRunner()
    runner.intents["workspace_archive_teardown"] = {
        "kind": "vm_and_kubernetes",
        "vm": {
            "provision_generation": generation,
            "vm_uid": "old-vm-uid",
            "rootdisk_pvc_uid": "old-root-uid",
            "ssh_host": "100.64.0.8",
            "ssh_port": 22,
        },
        "kubernetes": {
            "pod_uid": "old-pod-uid",
            "pvc_uid": "old-pvc-uid",
            "service_uid": "old-service-uid",
            "pod_ip": "10.0.0.8",
            "ssh_host_key_fingerprint": host_key,
            "ssh_port": 30022,
            "snapshot_generation": COMMAND_ID,
            "snapshot_created_at": "2026-08-13T01:02:03+00:00",
        },
    }
    monkeypatch.setattr(orchestrator.main, "postgres_db", _RouteDB(_route_job()))
    monkeypatch.setattr(
        orchestrator.main.vm_provisioner,
        "release_vm_captured",
        AsyncMock(return_value=VMTeardownResult("identity_superseded", False)),
    )
    release_kubernetes = AsyncMock(return_value=False)
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "release_workspace",
        release_kubernetes,
    )
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "classify_workspace_teardown_identity",
        AsyncMock(return_value="unknown"),
    )

    output = await b08_helpers.run_completion_workspace_teardown(JOB_ID, runner)

    assert output["teardown_disposition"] == "retry_pending"
    assert "captured Kubernetes teardown remains unknown" in output["error"]
    assert "workspace_archive_teardown" in runner.pending
    assert not runner.superseded_names
    release_kubernetes.assert_awaited_once()


@pytest.mark.asyncio
async def test_flagged_kubernetes_teardown_captures_and_uses_exact_uids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _route_job()
    job["context"] = {"workspace_container": {"status": "ready", "provisioner": "k8s"}}
    database = _RouteDB(job)
    runner = _RecordingRunner()
    terminal_effects = AsyncMock(return_value={"actions": ["terminal durable"]})
    legacy_cleanup = AsyncMock(return_value=["legacy cleanup"])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=legacy_cleanup,
    )
    host_key = "SHA256:" + ("A" * 43)
    identity = orchestrator.main.WorkspaceTeardownIdentity(
        pod_uid="pod-uid-a",
        pvc_uid="pvc-uid-a",
        service_uid="service-uid-a",
        pod_ip="10.0.0.8",
        ssh_host_key_fingerprint=host_key,
    )

    async def capture(owner):
        assert owner == orchestrator.main.WorkspaceOwner.job(JOB_ID)
        runner.probe_order.append("capture_resource")
        return identity

    async def release(owner, **kwargs):
        assert owner == orchestrator.main.WorkspaceOwner.job(JOB_ID)
        assert kwargs == {
            "teardown_identity": identity,
            "require_snapshot": True,
            "expected_runtime_incarnation": "pod-uid-a",
            "expected_host_key_fingerprint": host_key,
            "strict_terminal_snapshot": True,
            "terminal_snapshot_generation": COMMAND_ID,
            "terminal_snapshot_created_at": runner.intents[
                "workspace_archive_teardown"
            ]["snapshot_created_at"],
            "strict": True,
            "exact_absence_timeout_seconds": 45.0,
        }
        runner.probe_order.append("release")
        return True

    capture_mock = AsyncMock(side_effect=capture)
    release_mock = AsyncMock(side_effect=release)
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "capture_terminal_workspace_identity",
        capture_mock,
    )
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "release_workspace",
        release_mock,
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    assert "k8s workspace released" in result["actions"]
    assert runner.intents["workspace_archive_teardown"] == {
        "kind": "kubernetes",
        "pod_uid": "pod-uid-a",
        "pvc_uid": "pvc-uid-a",
        "service_uid": "service-uid-a",
        "pod_ip": "10.0.0.8",
        "ssh_host_key_fingerprint": host_key,
        "ssh_port": 30022,
        "snapshot_generation": COMMAND_ID,
        "snapshot_created_at": runner.intents["workspace_archive_teardown"][
            "snapshot_created_at"
        ],
    }
    assert runner.probe_order == [
        "intent_read",
        "capture_resource",
        "intent_write",
        "release",
    ]
    assert runner.callback_counts["workspace_archive_teardown"] == 1
    assert "workspace_archive_teardown" not in runner.pending
    legacy_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_kubernetes_teardown_resume_reuses_intent_after_pod_disappears(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _route_job()
    job["context"] = {"workspace_container": {"status": "ready", "provisioner": "k8s"}}
    database = _RouteDB(job)
    runner = _RecordingRunner()
    host_key = "SHA256:" + ("A" * 43)
    snapshot_created_at = "2026-08-13T01:02:03+00:00"
    runner.intents["workspace_archive_teardown"] = {
        "kind": "kubernetes",
        "pod_uid": "old-pod-uid",
        "pvc_uid": "old-pvc-uid",
        "service_uid": "old-service-uid",
        "pod_ip": "10.0.0.9",
        "ssh_host_key_fingerprint": host_key,
        "ssh_port": 30022,
        "snapshot_generation": COMMAND_ID,
        "snapshot_created_at": snapshot_created_at,
    }
    terminal_effects = AsyncMock(return_value={"actions": ["terminal durable"]})
    legacy_cleanup = AsyncMock(return_value=["legacy cleanup"])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=legacy_cleanup,
    )
    capture_mock = AsyncMock(
        side_effect=AssertionError("a captured teardown must not recapture by name")
    )
    release_mock = AsyncMock(side_effect=[False, True])
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "capture_terminal_workspace_identity",
        capture_mock,
    )
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "release_workspace",
        release_mock,
    )

    first = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )
    replay = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    assert first["actions"][-1].startswith("workspace cleanup failed:")
    assert replay["actions"][-1] == "k8s workspace released"
    assert release_mock.await_count == 2
    expected_identity = orchestrator.main.WorkspaceTeardownIdentity(
        pod_uid="old-pod-uid",
        pvc_uid="old-pvc-uid",
        service_uid="old-service-uid",
        pod_ip="10.0.0.9",
        ssh_host_key_fingerprint=host_key,
    )
    assert all(
        call.kwargs
        == {
            "teardown_identity": expected_identity,
            "require_snapshot": True,
            "expected_runtime_incarnation": "old-pod-uid",
            "expected_host_key_fingerprint": host_key,
            "strict_terminal_snapshot": True,
            "terminal_snapshot_generation": COMMAND_ID,
            "terminal_snapshot_created_at": snapshot_created_at,
            "strict": True,
            "exact_absence_timeout_seconds": 45.0,
        }
        for call in release_mock.await_args_list
    )
    capture_mock.assert_not_awaited()
    legacy_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_kubernetes_uid_teardown_stays_off_for_default_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _route_job()
    job["context"] = {
        "workspace_container": {"status": "ready", "provisioner": "k8s"},
    }
    database = _RouteDB(job)
    terminal_effects = AsyncMock(return_value={"actions": ["terminal durable"]})
    legacy_cleanup = AsyncMock(return_value=["legacy cleanup"])
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal_effects,
        workspace_cleanup=legacy_cleanup,
    )
    capture_mock = AsyncMock()
    release_mock = AsyncMock()
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "capture_workspace_teardown_identity",
        capture_mock,
    )
    monkeypatch.setattr(
        orchestrator.main.container_provisioner,
        "release_workspace",
        release_mock,
    )

    result = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=None,
    )

    assert result["actions"][-1] == "legacy cleanup"
    legacy_cleanup.assert_awaited_once_with(JOB_ID)
    capture_mock.assert_not_awaited()
    release_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_default_off_bypasses_command_module_and_preserves_legacy_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The closed gate must be byte-for-byte legacy at the wrapper boundary."""

    request = MagicMock()
    body = _body()
    legacy_result = {
        "status": "success",
        "job_id": JOB_ID,
        "new_status": "completed",
        "actions": ["legacy action"],
    }
    legacy = AsyncMock(return_value=legacy_result)
    accept = AsyncMock(side_effect=AssertionError("accept must remain dark"))
    auth = AsyncMock()
    finalizer_getter = _forbid_finalizer(monkeypatch)

    monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", False)
    monkeypatch.setattr(orchestrator.main, "require_internal", auth)
    monkeypatch.setattr(orchestrator.main, "_run_legacy_completion", legacy)
    monkeypatch.setattr(commands, "accept_completion_command", accept)

    original_import = builtins.__import__

    def reject_command_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in {
            "orchestrator.services.job_completion_commands",
            "orchestrator.services.completion_finalizer",
        }:
            raise AssertionError("closed gate attempted completion-service import")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", reject_command_import)

    handled = await b08_helpers.complete_job(request, JOB_ID, body)

    assert handled is legacy_result
    auth.assert_awaited_once_with(request)
    legacy.assert_awaited_once_with(request, JOB_ID, body, _authorized=True)
    accept.assert_not_awaited()
    finalizer_getter.assert_not_called()


@pytest.mark.asyncio
async def test_accepted_stateless_command_does_not_recheck_terminalized_worker_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The persisted workflow owns the accepted fence; legacy checks the lease."""

    from shared import worker_queue

    job = _route_job(status="completed")
    job["execution_lane"] = "stateless"
    database = _RouteDB(job)
    runner = _RecordingRunner()
    stale_lease = AsyncMock(return_value=False)
    monkeypatch.setattr(orchestrator.main, "postgres_db", database)
    monkeypatch.setattr(orchestrator.main, "require_internal", AsyncMock())
    monkeypatch.setattr(worker_queue, "worker_lease_is_current", stale_lease)

    # Command admission already checked token 17 and terminalized that queue
    # unit. Invoke the persisted workflow directly: the HTTP route now returns
    # 202 for this case and the background drain supplies the effect runner.
    handled = await b08_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        _body(),
        _authorized=True,
        _effect_runner=runner,
    )

    assert handled == {
        "status": "handled",
        "job_id": JOB_ID,
        "new_status": "completed",
        "actions": ["late callback ignored; job already completed"],
    }
    stale_lease.assert_not_awaited()

    # With the gate closed there is no accepted command fence, so the same
    # stale token must still fail the historical thin entry check.
    monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", False)
    with pytest.raises(HTTPException) as rejected:
        await b08_helpers.complete_job(MagicMock(), JOB_ID, _body())

    assert rejected.value.status_code == 409
    assert rejected.value.detail == (
        "Completion report does not hold the current worker lease"
    )
    stale_lease.assert_awaited_once()
    assert stale_lease.await_args.kwargs == {
        "job_id": JOB_ID,
        "lease_token": 17,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("admission_enabled", [False, True])
async def test_fresh_stateless_accept_returns_exact_background_handoff(
    monkeypatch: pytest.MonkeyPatch,
    admission_enabled: bool,
) -> None:
    """Durable queue closure, not the current admission flag, selects 202."""

    request = MagicMock()
    body = _body()
    database = object()
    accepted = _accepted("fresh", queue_terminalized=True)
    accept = AsyncMock(return_value=accepted)
    legacy = AsyncMock(side_effect=AssertionError("stateless route must not inline"))
    finalizer_getter = _forbid_finalizer(monkeypatch)

    monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(orchestrator.main, "COMPLETION_STATUS_REORDER_ENABLED", False)
    monkeypatch.setattr(
        orchestrator.main, "STATELESS_WORKER_ENABLED", admission_enabled
    )
    monkeypatch.setattr(orchestrator.main, "postgres_db", database)
    monkeypatch.setattr(orchestrator.main, "require_internal", AsyncMock())
    monkeypatch.setattr(orchestrator.main, "_run_legacy_completion", legacy)
    monkeypatch.setattr(commands, "accept_completion_command", accept)

    response = await b08_helpers.complete_job(request, JOB_ID, body)

    assert isinstance(response, JSONResponse)
    assert response.status_code == 202
    assert _response_json(response) == {
        "status": "accepted_pending",
        "job_id": JOB_ID,
        "command_id": COMMAND_ID,
        "command_state": "pending",
    }
    assert "Idempotent-Replayed" not in response.headers
    assert "Retry-After" not in response.headers
    legacy.assert_not_awaited()
    finalizer_getter.assert_not_called()
    accept.assert_awaited_once_with(
        database,
        job_id=JOB_ID,
        payload={
            "should_stop": True,
            "goal_achieved": True,
            "error": None,
            "freeze_data": {
                "freeze_type": "job_complete",
                "summary": "done",
            },
        },
        status_reorder_enabled=False,
        lease_token=17,
        agent_id=str(AGENT_ID),
        client_report_id=str(REPORT_ID),
        requested_by=f"agent:{AGENT_ID}",
    )


@pytest.mark.asyncio
async def test_fresh_pinned_admission_preserves_exact_inline_response_and_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = MagicMock()
    body = _body()
    database = object()
    runner = _RecordingRunner()
    events: list[str] = []
    legacy_result = {
        "status": "success",
        "job_id": JOB_ID,
        "new_status": "completed",
        "actions": ["effect-a", "effect-b"],
    }

    async def auth(_request) -> None:
        events.append("authenticate")

    async def accept(*args, **kwargs):
        events.append("accept")
        return _accepted("fresh", queue_terminalized=False)

    async def legacy(*args, **kwargs):
        events.append("legacy")
        return legacy_result

    async def finalize(command_id, *, callback, inline):
        events.append("finalize")
        assert command_id == COMMAND_ID
        assert inline is True
        outcome = await callback(runner)
        events.append("terminal")
        return SimpleNamespace(
            disposition="done",
            state="done",
            outcome=outcome,
        )

    accept_mock = AsyncMock(side_effect=accept)
    legacy_mock = AsyncMock(side_effect=legacy)
    finalize_mock = AsyncMock(side_effect=finalize)
    finalizer = SimpleNamespace(finalize_command=finalize_mock)
    finalizer_getter = MagicMock(return_value=finalizer)
    delay = AsyncMock()
    monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(orchestrator.main, "STATELESS_WORKER_ENABLED", True)
    monkeypatch.setattr(orchestrator.main, "COMPLETION_STATUS_REORDER_ENABLED", False)
    monkeypatch.setattr(
        orchestrator.main, "COMPLETION_FINALIZER_INLINE_DELAY_SECONDS", 0.0
    )
    monkeypatch.setattr(orchestrator.main.asyncio, "sleep", delay)
    monkeypatch.setattr(orchestrator.main, "postgres_db", database)
    monkeypatch.setattr(
        orchestrator.main, "require_internal", AsyncMock(side_effect=auth)
    )
    monkeypatch.setattr(orchestrator.main, "_run_legacy_completion", legacy_mock)
    monkeypatch.setattr(
        orchestrator.main._completion_runtime, "finalizer", finalizer_getter
    )
    monkeypatch.setattr(commands, "accept_completion_command", accept_mock)

    handled = await b08_helpers.complete_job(request, JOB_ID, body)

    assert handled is legacy_result
    assert events == ["authenticate", "accept", "finalize", "legacy", "terminal"]
    legacy_mock.assert_awaited_once_with(
        request,
        JOB_ID,
        body,
        _authorized=True,
        _effect_runner=runner,
    )
    finalizer_getter.assert_called_once_with()
    delay.assert_not_awaited()
    finalize_mock.assert_awaited_once()
    accept_mock.assert_awaited_once_with(
        database,
        job_id=JOB_ID,
        payload={
            "should_stop": True,
            "goal_achieved": True,
            "error": None,
            "freeze_data": {
                "freeze_type": "job_complete",
                "summary": "done",
            },
        },
        status_reorder_enabled=False,
        lease_token=17,
        agent_id=str(AGENT_ID),
        client_report_id=str(REPORT_ID),
        requested_by=f"agent:{AGENT_ID}",
    )


@pytest.mark.asyncio
async def test_fresh_superseded_finalization_returns_terminal_outcome_not_202(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcome = {
        "status": "superseded",
        "job_id": JOB_ID,
        "report_seq": 3,
        "reason": "entry_status_superseded",
        "accepted_job_status": "processing",
        "expected_entry_statuses": ["processing"],
        "observed_status": "cancelled",
        "winning_report_seq": None,
        "abandoned_effects": [],
    }
    finalize = AsyncMock(
        return_value=SimpleNamespace(
            disposition="superseded",
            state="superseded",
            outcome=outcome,
        )
    )
    monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(orchestrator.main, "require_internal", AsyncMock())
    monkeypatch.setattr(
        commands,
        "accept_completion_command",
        AsyncMock(return_value=_accepted("fresh")),
    )
    monkeypatch.setattr(
        orchestrator.main._completion_runtime,
        "finalizer",
        MagicMock(return_value=SimpleNamespace(finalize_command=finalize)),
    )
    legacy = AsyncMock(side_effect=AssertionError("superseded workflow must not run"))
    monkeypatch.setattr(orchestrator.main, "_run_legacy_completion", legacy)

    handled = await b08_helpers.complete_job(MagicMock(), JOB_ID, _body())

    assert handled == outcome
    legacy.assert_not_awaited()
    finalize.assert_awaited_once()


@pytest.mark.asyncio
async def test_fresh_admission_exposes_local_force_delete_window(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    events: list[str] = []

    async def accept(*_args, **_kwargs):
        events.append("accept")
        return _accepted("fresh")

    async def delay(seconds: float) -> None:
        events.append(f"delay:{seconds}")

    async def finalize(command_id, *, callback, inline):
        assert command_id == COMMAND_ID
        assert inline is True
        events.append("finalize")
        outcome = await callback(_RecordingRunner())
        return SimpleNamespace(
            disposition="done",
            state="done",
            outcome=outcome,
        )

    async def legacy(*_args, **_kwargs):
        events.append("legacy")
        return {"status": "success", "job_id": JOB_ID}

    monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(
        orchestrator.main, "COMPLETION_FINALIZER_INLINE_DELAY_SECONDS", 15.0
    )
    monkeypatch.setattr(orchestrator.main, "require_internal", AsyncMock())
    monkeypatch.setattr(
        orchestrator.main, "_run_legacy_completion", AsyncMock(side_effect=legacy)
    )
    monkeypatch.setattr(
        commands, "accept_completion_command", AsyncMock(side_effect=accept)
    )
    monkeypatch.setattr(
        orchestrator.main.asyncio, "sleep", AsyncMock(side_effect=delay)
    )
    monkeypatch.setattr(
        orchestrator.main._completion_runtime,
        "finalizer",
        MagicMock(
            return_value=SimpleNamespace(
                finalize_command=AsyncMock(side_effect=finalize)
            )
        ),
    )

    with caplog.at_level("INFO"):
        handled = await b08_helpers.complete_job(MagicMock(), JOB_ID, _body())

    assert handled == {"status": "success", "job_id": JOB_ID}
    assert events == ["accept", "finalize", "delay:15.0", "legacy"]
    accepted_records = [
        record.getMessage()
        for record in caplog.records
        if "Completion command" in record.getMessage()
    ]
    assert accepted_records == [
        f"Completion command {COMMAND_ID} accepted for job {JOB_ID}",
        f"Completion command {COMMAND_ID} claimed for job {JOB_ID}; "
        "inline delay 15.000s",
    ]
    assert all(str(AGENT_ID) not in record for record in accepted_records)
    assert all("lease" not in record.lower() for record in accepted_records)


@pytest.mark.asyncio
async def test_deterministic_http_guard_is_terminal_and_replays_exactly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = MagicMock()
    body = _body()
    runner = _RecordingRunner()
    stored: dict[str, dict] = {}

    async def accept(*args, **kwargs):
        if "outcome" not in stored:
            return _accepted("fresh")
        return _accepted("replay_done", state="done", outcome=stored["outcome"])

    async def finalize(command_id, *, callback, inline):
        assert command_id == COMMAND_ID
        assert inline is True
        stored["outcome"] = await callback(runner)
        return SimpleNamespace(
            disposition="done",
            state="done",
            outcome=stored["outcome"],
        )

    error = HTTPException(
        status_code=422,
        detail={"reason": "deterministic completion guard"},
        headers={"X-Completion-Guard": "true"},
    )
    legacy = AsyncMock(side_effect=error)
    finalize_mock = AsyncMock(side_effect=finalize)
    monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(orchestrator.main, "require_internal", AsyncMock())
    monkeypatch.setattr(orchestrator.main, "_run_legacy_completion", legacy)
    monkeypatch.setattr(
        commands, "accept_completion_command", AsyncMock(side_effect=accept)
    )
    monkeypatch.setattr(
        orchestrator.main._completion_runtime,
        "finalizer",
        MagicMock(return_value=SimpleNamespace(finalize_command=finalize_mock)),
    )

    for _attempt in range(2):
        with pytest.raises(HTTPException) as caught:
            await b08_helpers.complete_job(request, JOB_ID, body)
        assert caught.value.status_code == 422
        assert caught.value.detail == {"reason": "deterministic completion guard"}
        assert caught.value.headers == {"X-Completion-Guard": "true"}

    assert stored["outcome"] == {
        "_completion_http_error": {
            "status_code": 422,
            "detail": {"reason": "deterministic completion guard"},
            "headers": {"X-Completion-Guard": "true"},
        }
    }
    legacy.assert_awaited_once_with(
        request,
        JOB_ID,
        body,
        _authorized=True,
        _effect_runner=runner,
    )
    finalize_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_done_replay_returns_stored_outcome_with_idempotency_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcome = {
        "status": "success",
        "job_id": JOB_ID,
        "new_status": "completed",
        "actions": ["already finalized"],
    }
    accept = AsyncMock(
        return_value=_accepted(
            "replay_done",
            state="done",
            outcome=outcome,
            queue_terminalized=True,
        )
    )
    legacy = AsyncMock()
    finalizer_getter = _forbid_finalizer(monkeypatch)
    monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(orchestrator.main, "require_internal", AsyncMock())
    monkeypatch.setattr(orchestrator.main, "_run_legacy_completion", legacy)
    monkeypatch.setattr(commands, "accept_completion_command", accept)

    response = await b08_helpers.complete_job(MagicMock(), JOB_ID, _body())

    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    assert response.headers["Idempotent-Replayed"] == "true"
    assert _response_json(response) == outcome
    legacy.assert_not_awaited()
    finalizer_getter.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("command_state", ["pending", "finalizing"])
async def test_pending_or_finalizing_replay_is_retryable_conflict(
    monkeypatch: pytest.MonkeyPatch,
    command_state: str,
) -> None:
    accept = AsyncMock(
        side_effect=commands.CompletionInProgress(COMMAND_ID, command_state)
    )
    legacy = AsyncMock()
    finalizer_getter = _forbid_finalizer(monkeypatch)
    monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(orchestrator.main, "require_internal", AsyncMock())
    monkeypatch.setattr(orchestrator.main, "_run_legacy_completion", legacy)
    monkeypatch.setattr(commands, "accept_completion_command", accept)

    with pytest.raises(HTTPException) as caught:
        await b08_helpers.complete_job(MagicMock(), JOB_ID, _body())

    assert caught.value.status_code == 409
    assert caught.value.headers == {"Retry-After": "1"}
    assert command_state in str(caught.value.detail)
    legacy.assert_not_awaited()
    finalizer_getter.assert_not_called()


@pytest.mark.asyncio
async def test_divergent_replay_is_unprocessable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    accept = AsyncMock(
        side_effect=commands.CompletionPayloadMismatch(
            "client_report_id was reused with a different payload"
        )
    )
    legacy = AsyncMock()
    finalizer_getter = _forbid_finalizer(monkeypatch)
    monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(orchestrator.main, "require_internal", AsyncMock())
    monkeypatch.setattr(orchestrator.main, "_run_legacy_completion", legacy)
    monkeypatch.setattr(commands, "accept_completion_command", accept)

    with pytest.raises(HTTPException) as caught:
        await b08_helpers.complete_job(MagicMock(), JOB_ID, _body())

    assert caught.value.status_code == 422
    assert caught.value.headers is None
    assert "different payload" in str(caught.value.detail)
    legacy.assert_not_awaited()
    finalizer_getter.assert_not_called()


@pytest.mark.asyncio
async def test_nonterminal_stateless_report_has_machine_coded_422(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message = "stateless completion requires should_stop=true"
    accept = AsyncMock(side_effect=commands.CompletionNonTerminalReport(message))
    legacy = AsyncMock()
    finalizer_getter = _forbid_finalizer(monkeypatch)
    monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(orchestrator.main, "require_internal", AsyncMock())
    monkeypatch.setattr(orchestrator.main, "_run_legacy_completion", legacy)
    monkeypatch.setattr(commands, "accept_completion_command", accept)

    with pytest.raises(HTTPException) as caught:
        await b08_helpers.complete_job(MagicMock(), JOB_ID, _body())

    assert caught.value.status_code == 422
    assert caught.value.detail == {
        "code": "completion_non_terminal_report",
        "message": message,
    }
    assert caught.value.headers is None
    legacy.assert_not_awaited()
    finalizer_getter.assert_not_called()


@pytest.mark.asyncio
async def test_parked_replay_is_accepted_without_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    accept = AsyncMock(
        return_value=_accepted("replay_parked", state="parked", queue_terminalized=True)
    )
    legacy = AsyncMock()
    finalizer_getter = _forbid_finalizer(monkeypatch)
    monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(orchestrator.main, "require_internal", AsyncMock())
    monkeypatch.setattr(orchestrator.main, "_run_legacy_completion", legacy)
    monkeypatch.setattr(commands, "accept_completion_command", accept)

    response = await b08_helpers.complete_job(MagicMock(), JOB_ID, _body())

    assert isinstance(response, JSONResponse)
    assert response.status_code == 202
    assert "Retry-After" not in response.headers
    assert response.headers["Idempotent-Replayed"] == "true"
    assert _response_json(response) == {
        "status": "still_pending",
        "job_id": JOB_ID,
        "command_id": COMMAND_ID,
        "command_state": "parked",
    }
    legacy.assert_not_awaited()
    finalizer_getter.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("disposition", "state", "outcome", "winning_seq", "abandoned"),
    [
        (
            "replay_superseded",
            "superseded",
            {
                "status": "superseded",
                "job_id": JOB_ID,
                "winning_report_seq": 2,
            },
            2,
            (),
        ),
        (
            "replay_force_resolved",
            "force_resolved",
            {
                "status": "force_resolved",
                "job_id": JOB_ID,
                "abandoned_effects": ["workspace_cleanup"],
            },
            None,
            ("workspace_cleanup",),
        ),
    ],
)
async def test_operator_terminal_replays_return_their_durable_outcome(
    monkeypatch: pytest.MonkeyPatch,
    disposition: str,
    state: str,
    outcome: dict,
    winning_seq: int | None,
    abandoned: tuple[str, ...],
) -> None:
    accept = AsyncMock(
        return_value=_accepted(
            disposition,
            state=state,
            outcome=outcome,
            winning_report_seq=winning_seq,
            abandoned_effects=abandoned,
            queue_terminalized=True,
        )
    )
    legacy = AsyncMock()
    finalizer_getter = _forbid_finalizer(monkeypatch)
    monkeypatch.setattr(orchestrator.main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(orchestrator.main, "require_internal", AsyncMock())
    monkeypatch.setattr(orchestrator.main, "_run_legacy_completion", legacy)
    monkeypatch.setattr(commands, "accept_completion_command", accept)

    response = await b08_helpers.complete_job(MagicMock(), JOB_ID, _body())

    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    assert response.headers["Idempotent-Replayed"] == "true"
    assert _response_json(response) == outcome
    assert "Retry-After" not in response.headers
    legacy.assert_not_awaited()
    finalizer_getter.assert_not_called()


@pytest.mark.asyncio
async def test_curation_handoff_is_keyed_to_completion_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Conn:
        async def fetchrow(self, *_args, **_kwargs):
            return {"id": CURATOR_ID, "status": "waiting"}

    @asynccontextmanager
    async def acquire():
        yield _Conn()

    queue = AsyncMock(return_value=True)
    monkeypatch.setattr(completion_service, "is_curation_enabled", lambda _job: True)
    monkeypatch.setattr(
        completion_service,
        "get_curation_config",
        lambda _job: {"curator_config": "curator"},
    )
    monkeypatch.setattr(orchestrator.main.postgres_db, "acquire", acquire)
    monkeypatch.setattr(
        orchestrator.main.postgres_db,
        "get_job",
        AsyncMock(return_value={"id": CURATOR_ID, "context": {}}),
    )
    monkeypatch.setattr(
        orchestrator.main.job_control_operations.JobControlOperations,
        "internal_resume_job",
        queue,
    )

    await b08_helpers.trigger_curation_final_pass(
        JOB_ID,
        {"id": JOB_ID, "resolved_config": {}},
        completion_command_id=COMMAND_ID,
    )

    assert queue.await_args.kwargs["additional_context"] == {
        "curation_final_pass_completion_command_id": COMMAND_ID
    }


@pytest.mark.asyncio
async def test_curation_handoff_reconciles_exact_command_without_requeue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Conn:
        async def fetchrow(self, *_args, **_kwargs):
            return {"id": CURATOR_ID, "status": "paused"}

    @asynccontextmanager
    async def acquire():
        yield _Conn()

    queue = AsyncMock()
    dispatch = MagicMock()
    monkeypatch.setattr(completion_service, "is_curation_enabled", lambda _job: True)
    monkeypatch.setattr(
        completion_service,
        "get_curation_config",
        lambda _job: {"curator_config": "curator"},
    )
    monkeypatch.setattr(orchestrator.main.postgres_db, "acquire", acquire)
    monkeypatch.setattr(
        orchestrator.main.postgres_db,
        "get_job",
        AsyncMock(
            return_value={
                "id": CURATOR_ID,
                "context": {"curation_final_pass_completion_command_id": COMMAND_ID},
            }
        ),
    )
    monkeypatch.setattr(
        orchestrator.main.job_control_operations.JobControlOperations,
        "internal_resume_job",
        queue,
    )
    monkeypatch.setattr(orchestrator.main, "_trigger_dispatch", dispatch)

    await b08_helpers.trigger_curation_final_pass(
        JOB_ID,
        {"id": JOB_ID, "resolved_config": {}},
        completion_command_id=COMMAND_ID,
    )

    queue.assert_not_awaited()
    dispatch.assert_called_once_with()
