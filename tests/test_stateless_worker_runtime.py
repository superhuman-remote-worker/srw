"""Adversarial driver tests for stateless worker batch dispositions."""

from __future__ import annotations

from dataclasses import replace
import asyncio
from datetime import datetime, timedelta, timezone
import os
import threading
from types import SimpleNamespace
from typing import Annotated
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
import pytest
from typing_extensions import TypedDict

import agent.api.persistent_app as pa
import agent.api.turn_executor as turn_executor
from agent.api.orchestrator_client import (
    ClaimBundleError,
    CompletionNonTerminalReportError,
)
from shared.workspace_recovery import (
    WorkspaceRecoveryCode,
    WorkspaceRecoveryDisposition,
)
from agent.agent import UniversalAgent, _stateless_worker_remote_authority
from shared.runtime.core.workspace_backend import WorkspaceUnavailableError
from shared.runtime.core.backends.remote import RemoteBackend
from agent.graph import route_entry
from shared.run_queue import ClaimedUnit, EnqueueResult
from shared.subagent_lifecycle import (
    SubagentLifecycleError,
    SubagentQuiescenceError,
)
from shared.job_steering import CheckpointSteeringAcker, context_delivery_key
from shared.worker_queue import (
    WorkerClaim,
    WorkerCompletionAcceptance,
    WorkerRenewal,
    WorkerRotation,
    get_worker_completion_acceptance,
)

WORKSPACE_GENERATION = "11111111-1111-4111-8111-111111111111"
WORKSPACE_RUNTIME = "22222222-2222-4222-8222-222222222222"
WORKSPACE_FINGERPRINT = "SHA256:" + ("A" * 43)


class _WorkerFrontierState(TypedDict, total=False):
    initialized: bool
    messages: Annotated[list[BaseMessage], add_messages]
    should_stop: bool
    goal_achieved: bool
    is_final_phase: bool
    freeze_data: dict | None
    error: dict | None
    iteration: int
    resume_feedback: str | None
    resume_reason: str | None
    delivered_feedback_keys: list[str]
    delivered_delegation_keys: list[str]
    client_report_id: str | None
    completion_report_payload: dict | None
    worker_batch_started_at: float | None
    worker_batch_start_iteration: int | None
    worker_batch_target_wall_seconds: float | None
    worker_batch_min_wall_seconds: float | None
    worker_batch_iteration_cap: int | None
    worker_resume_id: str | None


def _claim(
    *,
    token=7,
    input_seq=None,
    prior="created",
    attempts=1,
    max_attempts=5,
) -> WorkerClaim:
    unit = ClaimedUnit(
        unit_id=uuid4(),
        unit_kind="worker_batch",
        fair_key="user-a",
        lease_token=token,
        input_seq=input_seq,
        consumed_seq=None,
        attempts_since_completion=attempts,
        leased_until=datetime.now(timezone.utc),
    )
    return WorkerClaim(
        unit=unit,
        prior_job_status=prior,
        resume=prior != "created",
        max_attempts=max_attempts,
    )


def _renewal(status="processing") -> WorkerRenewal:
    return WorkerRenewal(
        leased_until=datetime.now(timezone.utc),
        job_status=status,
        job_context={},
        pending_guidance=(),
        queued_replies=(),
    )


def _recovery_receipt(claim):
    return WorkspaceRecoveryDisposition.hold_committed(
        code=WorkspaceRecoveryCode.TRANSPORT_UNAVAILABLE,
        operation_id=uuid4(),
        accepted_lease_token=claim.lease_token,
        hold_lease_token=claim.lease_token + 1,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", [1, 5])
@pytest.mark.parametrize(
    "seam",
    [
        "bundle",
        "bundle_response_lost",
        "preflight",
        "graph",
        "shutdown_tail",
        "heartbeat",
        "heartbeat_error",
    ],
)
async def test_workspace_refusal_hands_off_without_terminal_report(
    worker_runtime, monkeypatch, attempt, seam
):
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "true")
    claim = _claim(attempts=attempt)
    final = {"should_stop": True, "goal_achieved": True, "error": None}
    executor, agent, client, renew, rotate, complete, release = _install(
        monkeypatch, claim, final
    )
    client.backend = "vm"
    receipt = _recovery_receipt(claim)
    client.report_workspace_recovery.return_value = receipt
    if seam == "bundle":
        client.get_claim_bundle = AsyncMock(
            side_effect=ClaimBundleError(409, recovery=receipt)
        )
    elif seam == "bundle_response_lost":
        client.get_claim_bundle = AsyncMock(
            side_effect=TimeoutError("response lost after commit")
        )
        client.get_workspace_recovery_disposition.return_value = receipt
    elif seam == "preflight":
        renew.return_value = None
        client.get_workspace_recovery_disposition.return_value = receipt
    else:

        async def stream():
            if seam in {"heartbeat", "heartbeat_error"}:
                await asyncio.Event().wait()
            if seam == "graph":
                raise WorkspaceUnavailableError("transport disconnected")
            yield final
            if seam == "shutdown_tail":
                raise WorkspaceUnavailableError("tail transport disconnected")

        agent.process_job = AsyncMock(return_value=stream())
        if seam in {"heartbeat", "heartbeat_error"}:
            monkeypatch.setattr(turn_executor, "HEARTBEAT_INTERVAL_SECONDS", 0.001)
            renew.side_effect = [
                _renewal(),
                None if seam == "heartbeat" else OSError("renewal response lost"),
            ]
            client.get_workspace_recovery_disposition.return_value = receipt

    await asyncio.wait_for(executor._serve_worker_claim(claim), 1)

    client.report_completion.assert_not_awaited()
    complete.assert_not_awaited()
    rotate.assert_not_awaited()
    release.assert_not_awaited()
    assert agent.cleanup_calls == [True]
    assert not executor._stop.is_set()
    if seam in {"graph", "shutdown_tail"}:
        client.report_workspace_recovery.assert_awaited_once()
    if seam in {"preflight", "heartbeat", "heartbeat_error", "bundle_response_lost"}:
        client.get_workspace_recovery_disposition.assert_awaited()


@pytest.mark.asyncio
async def test_recovery_report_response_loss_replays_exact_disposition(
    worker_runtime, monkeypatch
):
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "true")
    claim = _claim(attempts=5)
    final = {
        "should_stop": True,
        "error": {"type": "workspace_unavailable", "recoverable": True},
    }
    executor, agent, client, _, _, complete, release = _install(
        monkeypatch, claim, final
    )
    client.backend = "vm"
    receipt = _recovery_receipt(claim)
    client.report_workspace_recovery.side_effect = TimeoutError(
        "committed response lost"
    )
    client.get_workspace_recovery_disposition.side_effect = [None, receipt]
    await executor._serve_worker_claim(claim)
    client.report_completion.assert_not_awaited()
    release.assert_not_awaited()
    complete.assert_not_awaited()
    assert agent.cleanup_calls == [True]


@pytest.mark.asyncio
async def test_recovery_quiescence_failure_quarantines_executor(
    worker_runtime, monkeypatch
):
    claim = _claim(attempts=5)
    executor, agent, client, _, _, complete, release = _install(monkeypatch, claim, {})
    client.get_claim_bundle = AsyncMock(
        side_effect=ClaimBundleError(409, recovery=_recovery_receipt(claim))
    )
    agent.cleanup_worker_claim = AsyncMock(
        side_effect=RuntimeError("resource calls still active")
    )
    await executor._serve_worker_claim(claim)
    assert executor._stop.is_set()
    assert executor._worker_quarantined is True
    client.report_completion.assert_not_awaited()
    release.assert_not_awaited()
    complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancelled_bundle_recovery_still_quarantines_failed_cleanup(
    worker_runtime, monkeypatch
):
    claim = _claim(attempts=5)
    executor, agent, client, _, _, complete, release = _install(monkeypatch, claim, {})
    client.get_claim_bundle = AsyncMock(side_effect=asyncio.CancelledError)
    client.get_workspace_recovery_disposition.return_value = _recovery_receipt(claim)
    agent.cleanup_worker_claim = AsyncMock(side_effect=RuntimeError("still active"))
    with pytest.raises(asyncio.CancelledError):
        await executor._serve_worker_claim(claim)
    assert executor._worker_quarantined and executor._stop.is_set()
    client.report_completion.assert_not_awaited()
    complete.assert_not_awaited()
    release.assert_not_awaited()


@pytest.mark.asyncio
async def test_recovery_without_strict_runtime_gate_quarantines_executor(
    worker_runtime, monkeypatch
):
    claim = _claim(attempts=5)
    executor, agent, client, _, _, complete, release = _install(monkeypatch, claim, {})
    client.get_claim_bundle = AsyncMock(
        side_effect=ClaimBundleError(409, recovery=_recovery_receipt(claim))
    )
    agent.quiesce_worker_workspace_recovery = None
    await executor._serve_worker_claim(claim)
    assert executor._worker_quarantined and executor._stop.is_set()
    assert agent.cleanup_calls == []
    client.report_completion.assert_not_awaited()
    complete.assert_not_awaited()
    release.assert_not_awaited()


@pytest.mark.asyncio
async def test_recovery_unknown_response_leaves_exact_claim_for_reaper(
    worker_runtime, monkeypatch
):
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "true")
    claim = _claim(attempts=5)
    executor, agent, client, _, _, complete, release = _install(
        monkeypatch,
        claim,
        {
            "should_stop": True,
            "error": {"type": "workspace_unavailable"},
        },
    )
    client.report_workspace_recovery.side_effect = TimeoutError("response lost")
    client.backend = "vm"
    client.get_workspace_recovery_disposition.return_value = None
    await executor._serve_worker_claim(claim)
    client.report_completion.assert_not_awaited()
    release.assert_not_awaited()
    complete.assert_not_awaited()
    assert agent.cleanup_calls == [True]


@pytest.mark.asyncio
async def test_agent_recovery_cleanup_does_not_swallow_transport_retirement_failure():
    agent = UniversalAgent.__new__(UniversalAgent)
    backend = SimpleNamespace(
        retire=MagicMock(side_effect=RuntimeError("still active"))
    )
    agent._workspace_manager = SimpleNamespace(backend=backend)
    agent._tool_context = None
    agent._retire_worker_shell_admission = MagicMock()
    agent._scrub_worker_claim_locals = AsyncMock()
    with pytest.raises(RuntimeError, match="still active"):
        await agent.quiesce_worker_workspace_recovery()
    assert agent._workspace_manager.backend is backend
    agent._scrub_worker_claim_locals.assert_not_awaited()


@pytest.mark.asyncio
async def test_workspace_error_survives_child_failure_in_graph_shutdown_tail(
    worker_runtime, monkeypatch
):
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "true")
    claim = _claim(attempts=5)
    executor, agent, client, _, _, complete, release = _install(monkeypatch, claim, {})
    client.backend = "vm"
    client.report_workspace_recovery.return_value = _recovery_receipt(claim)

    async def stream():
        yield {"should_stop": True, "error": {"type": "workspace_unavailable"}}
        raise SubagentQuiescenceError("child finalizer failed after workspace loss")

    agent.process_job = AsyncMock(return_value=stream())
    await executor._serve_worker_claim(claim)
    client.report_workspace_recovery.assert_awaited_once()
    client.report_completion.assert_not_awaited()
    release.assert_not_awaited()
    complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_agent_recovery_cleanup_does_not_swallow_shell_admission_failure():
    agent = UniversalAgent.__new__(UniversalAgent)
    backend = SimpleNamespace(
        retire_shell_owner=MagicMock(side_effect=RuntimeError("shell still admitted")),
        retire=MagicMock(),
    )
    agent._workspace_manager = SimpleNamespace(backend=backend)
    agent._tool_context = None
    agent._scrub_worker_claim_locals = AsyncMock()
    with pytest.raises(RuntimeError, match="shell still admitted"):
        await agent.quiesce_worker_workspace_recovery()
    backend.retire.assert_not_called()


@pytest.mark.asyncio
async def test_recovery_cleanup_timeout_quarantines_and_refuses_next_claim(
    worker_runtime, monkeypatch
):
    claim = _claim(attempts=5)
    executor, agent, client, _, _, _, release = _install(monkeypatch, claim, {})
    client.get_claim_bundle = AsyncMock(
        side_effect=ClaimBundleError(409, recovery=_recovery_receipt(claim))
    )
    admitted = asyncio.Event()
    retire = asyncio.Event()

    async def cleanup(*, preserve_shell):
        admitted.set()
        await retire.wait()

    agent.cleanup_worker_claim = cleanup
    monkeypatch.setattr(turn_executor, "WORKER_RECOVERY_QUIESCE_SECONDS", 0.01)
    try:
        await executor._serve_worker_claim(claim)
        assert admitted.is_set()
        assert executor._worker_quarantined and executor._stop.is_set()
        with pytest.raises(turn_executor._ClaimQuiescenceError):
            await executor._serve_worker_claim(_claim())
        client.report_completion.assert_not_awaited()
        release.assert_not_awaited()
    finally:
        retire.set()
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_disabled_recovery_admission_still_consumes_accepted_receipt(
    worker_runtime, monkeypatch
):
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")
    claim = _claim(attempts=5)
    executor, agent, client, _, _, complete, release = _install(monkeypatch, claim, {})
    client.get_claim_bundle = AsyncMock(side_effect=TimeoutError("hold response lost"))
    client.get_workspace_recovery_disposition.return_value = _recovery_receipt(claim)
    await executor._serve_worker_claim(claim)
    client.report_completion.assert_not_awaited()
    release.assert_not_awaited()
    complete.assert_not_awaited()
    assert agent.cleanup_calls == [True]


@pytest.mark.asyncio
async def test_enabled_vm_recovery_preserves_sandbox_exhaustion_behavior(
    worker_runtime, monkeypatch
):
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "true")
    claim = _claim(attempts=5)
    executor, agent, client, _, _, complete, release = _install(
        monkeypatch,
        claim,
        {
            "should_stop": True,
            "error": {"type": "workspace_unavailable", "recoverable": True},
        },
    )
    await executor._serve_worker_claim(claim)
    client.report_workspace_recovery.assert_not_awaited()
    client.report_completion.assert_awaited_once()
    complete.assert_awaited_once()
    release.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("http_result", [False, True])
async def test_recovery_accepted_during_completion_response_uses_strict_handoff(
    worker_runtime, monkeypatch, http_result
):
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "true")
    claim = _claim(attempts=5)
    executor, agent, client, renew, _, complete, release = _install(
        monkeypatch,
        claim,
        {
            "should_stop": True,
            "goal_achieved": True,
        },
    )
    receipt = _recovery_receipt(claim)

    async def report(*args, **kwargs):
        client.get_workspace_recovery_disposition.return_value = receipt
        renew.return_value = None
        return http_result

    client.report_completion.side_effect = report
    await executor._serve_worker_claim(claim)
    assert executor._worker_workspace_recovery.disposition == receipt
    assert agent.cleanup_calls == [True]
    complete.assert_not_awaited()
    release.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_type",
    [
        "workspace_unavailable",
        "workspace_transport_unavailable",
        "workspace_identity_conflict",
    ],
)
async def test_workspace_error_closes_graph_before_another_step(
    worker_runtime, monkeypatch, error_type
):
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "true")
    claim = _claim(attempts=5)
    executor, agent, client, _, _, _, release = _install(monkeypatch, claim, {})
    client.backend = "vm"
    client.report_workspace_recovery.return_value = _recovery_receipt(claim)
    events = []

    async def stream():
        try:
            yield {"should_stop": True, "error": {"type": error_type}}
            events.append("another_step")
        finally:
            events.append(("closed", executor._lease.lost.is_set()))

    agent.process_job = AsyncMock(return_value=stream())
    await executor._serve_worker_claim(claim)
    assert events == [("closed", True)]
    client.report_completion.assert_not_awaited()
    release.assert_not_awaited()


def _acceptance(
    *,
    command_id=None,
    command_state="done",
    job_status="completed",
    outcome=None,
    deadline_expired=False,
    deadline_remaining_seconds=60.0,
    lease_remaining_seconds=30.0,
    run_after_remaining_seconds=0.0,
) -> WorkerCompletionAcceptance:
    now = datetime.now(timezone.utc)
    return WorkerCompletionAcceptance(
        job_status=job_status,
        queue_state="done",
        command_state=command_state,
        command_id=command_id or uuid4(),
        command_outcome=(
            {"new_status": job_status} if outcome is None else dict(outcome)
        ),
        deadline_at=now + timedelta(seconds=deadline_remaining_seconds),
        lease_expires_at=(
            now + timedelta(seconds=lease_remaining_seconds)
            if lease_remaining_seconds is not None
            else None
        ),
        run_after=now + timedelta(seconds=run_after_remaining_seconds),
        deadline_expired=deadline_expired,
        deadline_remaining_seconds=deadline_remaining_seconds,
        lease_remaining_seconds=lease_remaining_seconds,
        run_after_remaining_seconds=run_after_remaining_seconds,
    )


def _bundle(claim: WorkerClaim) -> dict:
    job_id = str(claim.unit_id)
    return {
        "unit_id": job_id,
        "job_id": job_id,
        "unit_kind": "worker_batch",
        "execution_lane": "stateless",
        "job": {
            "job_id": job_id,
            "description": "continue the task",
            "config_name": "worker_base",
            "config_override": {
                "workspace": {
                    "backend": "sandbox",
                    "remote": {"host": "workspace.internal"},
                }
            },
            "context": {},
            "managed_repository_credentials": [
                {
                    "authority_id": "11111111-1111-4111-8111-111111111111",
                    "generation": 1,
                    "repo_name": "job-stateless",
                    "private_key": "hidden-runtime-bearer",
                }
            ],
            "workspace_runtime": {
                "requested_backend": "sandbox",
                "assigned_backend": "sandbox",
                "effective_backend": "sandbox",
                "state": "ready",
            },
            "workspace_generation": WORKSPACE_GENERATION,
            "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
            "workspace_ssh_host_key_fingerprint": WORKSPACE_FINGERPRINT,
            "workspace_owner_kind": "job",
            "workspace_owner_id": job_id,
        },
        "batch": {
            "target_wall_seconds": 60.0,
            "min_wall_seconds": 0.0,
            "iteration_cap": 3,
        },
    }


def test_worker_bundle_preserves_exact_workspace_authority_in_metadata():
    claim = _claim()
    request, _ = turn_executor.StatelessTurnExecutor._parse_worker_bundle(
        _bundle(claim), claim
    )

    metadata = turn_executor.StatelessTurnExecutor._worker_job_metadata(request)

    assert metadata["workspace_generation"] == WORKSPACE_GENERATION
    assert metadata["workspace_runtime_incarnation"] == WORKSPACE_RUNTIME
    assert metadata["workspace_ssh_host_key_fingerprint"] == WORKSPACE_FINGERPRINT
    assert metadata["workspace_owner_kind"] == "job"
    assert metadata["workspace_owner_id"] == str(claim.unit.unit_id)
    assert metadata["managed_repository_credentials"] == [
        {
            "authority_id": "11111111-1111-4111-8111-111111111111",
            "generation": 1,
            "repo_name": "job-stateless",
            "private_key": "hidden-runtime-bearer",
        }
    ]
    assert metadata["workspace_runtime"] == {
        "requested_backend": "sandbox",
        "assigned_backend": "sandbox",
        "effective_backend": "sandbox",
        "state": "ready",
    }


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("workspace_generation", None),
        ("workspace_runtime_incarnation", "not-a-uuid"),
        ("workspace_ssh_host_key_fingerprint", "SHA256:short"),
        ("workspace_owner_kind", "session"),
        ("workspace_owner_id", "NOT-CANONICAL"),
    ],
)
def test_worker_bundle_rejects_missing_or_malformed_workspace_authority(
    field, replacement
):
    claim = _claim()
    bundle = _bundle(claim)
    if replacement is None:
        bundle["job"].pop(field)
    else:
        bundle["job"][field] = replacement

    with pytest.raises(ValueError):
        turn_executor.StatelessTurnExecutor._parse_worker_bundle(bundle, claim)


def test_agent_worker_authority_maps_all_remote_backend_fields():
    metadata = {
        "workspace_provisioner": "k8s",
        "workspace_generation": WORKSPACE_GENERATION,
        "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
        "workspace_ssh_host_key_fingerprint": WORKSPACE_FINGERPRINT,
        "workspace_owner_kind": "job",
        "workspace_owner_id": "33333333-3333-4333-8333-333333333333",
    }

    assert _stateless_worker_remote_authority(metadata, 7) == {
        "workspace_generation": WORKSPACE_GENERATION,
        "runtime_incarnation": WORKSPACE_RUNTIME,
        "expected_host_key_fingerprint": WORKSPACE_FINGERPRINT,
        "workspace_owner_kind": "job",
        "workspace_owner_id": "33333333-3333-4333-8333-333333333333",
        "require_host_key_fingerprint": True,
    }
    assert _stateless_worker_remote_authority({}, None) == {}


@pytest.mark.parametrize(
    "mutation",
    [
        {"workspace_generation": None},
        {"workspace_runtime_incarnation": None},
        {"workspace_ssh_host_key_fingerprint": None},
        {"workspace_owner_kind": "session"},
        {"workspace_owner_id": None},
    ],
)
def test_agent_worker_authority_fails_closed_on_incomplete_or_non_job_owner(mutation):
    metadata = {
        "workspace_provisioner": "k8s",
        "workspace_generation": WORKSPACE_GENERATION,
        "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
        "workspace_ssh_host_key_fingerprint": WORKSPACE_FINGERPRINT,
        "workspace_owner_kind": "job",
        "workspace_owner_id": "33333333-3333-4333-8333-333333333333",
    }
    metadata.update(mutation)

    with pytest.raises(WorkspaceUnavailableError):
        _stateless_worker_remote_authority(metadata, 7)


class _FakeAgent:
    def __init__(self, final_state):
        self.postgres_conn = object()
        self.final_state = final_state
        self.process_calls = []
        self.cleanup_calls = []
        self.hold_calls = 0
        self.hold_event = asyncio.Event()
        self._orchestrator_client = None

    async def process_job(self, *args, **kwargs):
        self.process_calls.append((args, kwargs))
        final_state = self.final_state

        async def stream():
            yield final_state

        return stream()

    async def cleanup_worker_claim(self, *, preserve_shell):
        self.cleanup_calls.append(preserve_shell)

    async def quiesce_worker_workspace_recovery(self):
        await self.cleanup_worker_claim(preserve_shell=True)

    async def hold_worker_finalization(self):
        self.hold_calls += 1
        self.hold_event.set()


class _FakeClient:
    def __init__(self, claim, *, report_result=True):
        self.claim = claim
        self.report_result = report_result
        self.report_completion = AsyncMock(return_value=report_result)
        self.get_workspace_recovery_disposition = AsyncMock(return_value=None)
        self.report_workspace_recovery = AsyncMock()
        self.backend = "sandbox"

    async def get_claim_bundle(self, unit_id, lease_token):
        assert (unit_id, lease_token) == (
            str(self.claim.unit_id),
            self.claim.lease_token,
        )
        bundle = _bundle(self.claim)
        bundle["job"]["config_override"]["workspace"]["backend"] = self.backend
        for name in ("requested_backend", "assigned_backend", "effective_backend"):
            bundle["job"]["workspace_runtime"][name] = self.backend
        return bundle


class _FakeAuditWriter:
    def __init__(self, *, ready=True):
        self.ready = ready
        self.rows = []

    def insert_audit_pre(self, row):
        self.rows.append(row)
        return len(self.rows) if self.ready else None


def _claim_timing_payload(writer: _FakeAuditWriter) -> dict:
    assert len(writer.rows) == 1
    row = writer.rows[0]
    assert row["step_type"] == "claim_timing"
    assert row["node_name"] == "worker_claim"
    assert row["agent_type"] == "worker"
    payload = row["payload"]
    assert payload["claimed_at"] == row["timestamp"].isoformat().replace("+00:00", "Z")
    assert (
        datetime.fromisoformat(payload["released_at"].replace("Z", "+00:00"))
        >= (row["timestamp"])
    )
    assert all(
        isinstance(payload[name], float) and payload[name] >= 0
        for name in ("bundle", "preflight", "agent_start", "stream", "finish")
    )
    return payload


@pytest.fixture
def worker_runtime(monkeypatch):
    saved = {
        "agent": pa._agent,
        "client": pa._orchestrator_client,
        "session": pa._session,
        "thread_id": pa._thread_id,
    }
    pa._session = None
    pa._thread_id = None
    monkeypatch.setattr(
        turn_executor.StatelessTurnExecutor,
        "_scrub_process_residue",
        lambda _self: None,
    )
    try:
        yield
    finally:
        pa._agent = saved["agent"]
        pa._orchestrator_client = saved["client"]
        pa._session = saved["session"]
        pa._thread_id = saved["thread_id"]


def _install(
    monkeypatch,
    claim,
    final_state,
    *,
    report_result=True,
    audit_writer=None,
):
    agent = _FakeAgent(final_state)
    client = _FakeClient(claim, report_result=report_result)
    pa._agent = agent
    pa._orchestrator_client = client
    renew = AsyncMock(return_value=_renewal())
    rotate = AsyncMock(
        return_value=WorkerRotation(
            completed_state="queued",
            enqueue=EnqueueResult(status="input_recorded", state="leased"),
            prior_input_seq=claim.unit.input_seq,
            next_input_seq=int(claim.unit.input_seq or 0) + 1,
        )
    )
    complete = AsyncMock(return_value="done")
    release = AsyncMock(return_value="queued")
    monkeypatch.setattr(turn_executor, "renew_worker_batch", renew)
    monkeypatch.setattr(turn_executor, "rotate_worker_batch", rotate)
    monkeypatch.setattr(turn_executor, "complete_worker_batch", complete)
    monkeypatch.setattr(turn_executor, "release_worker_batch", release)
    executor = turn_executor.StatelessTurnExecutor(
        pod_name="worker-pod",
        worker_enabled=True,
        audit_writer=audit_writer,
    )
    return executor, agent, client, renew, rotate, complete, release


@pytest.mark.asyncio
async def test_stateless_executor_drains_worker_residue_when_admission_is_off(
    worker_runtime,
    monkeypatch,
):
    """Admission-off stops enqueueing; executor mode keeps existing work live."""

    claim = _claim(input_seq=1, prior="processing")
    monkeypatch.setenv("STATELESS_EXECUTOR", "1")
    monkeypatch.setenv("STATELESS_WORKER_ENABLED", "false")
    monkeypatch.setenv("COMPLETION_COMMANDS_ENABLED", "false")
    pa._agent = SimpleNamespace(postgres_conn=object())
    session_claim = AsyncMock(return_value=None)
    worker_claim = AsyncMock(return_value=claim)
    monkeypatch.setattr(turn_executor, "claim_unit", session_claim)
    monkeypatch.setattr(turn_executor, "claim_worker_batch", worker_claim)
    executor = turn_executor.StatelessTurnExecutor(
        pod_name="rollback-worker",
        idle_poll_seconds=0.001,
        jitter=0,
    )

    async def serve(_claim):
        assert _claim is claim
        executor._stop.set()

    serve_worker = AsyncMock(side_effect=serve)
    executor._serve_worker_claim = serve_worker

    await executor.run()

    assert executor._worker_enabled is True
    assert executor._completion_commands_enabled is False
    session_claim.assert_awaited_once()
    worker_claim.assert_awaited_once_with(
        executor._db,
        pod_name="rollback-worker",
        completion_commands_enabled=False,
    )
    serve_worker.assert_awaited_once_with(claim)


@pytest.mark.asyncio
async def test_rotation_uses_complete_and_requeue_and_zero_complete_calls(
    worker_runtime, monkeypatch, caplog
):
    claim = _claim(input_seq=12, prior="processing")
    final = {
        "should_stop": True,
        "freeze_data": {"freeze_type": "batch_boundary"},
        "error": None,
    }
    executor, agent, client, _, rotate, complete, release = _install(
        monkeypatch, claim, final
    )

    with caplog.at_level("INFO"):
        await executor._serve_worker_claim(claim)

    client.report_completion.assert_not_awaited()
    rotate.assert_awaited_once_with(
        executor._db,
        unit_id=claim.unit_id,
        lease_token=claim.lease_token,
        input_seq=12,
        fair_key="user-a",
    )
    complete.assert_not_awaited()
    release.assert_not_awaited()
    assert agent.cleanup_calls == [True]
    post_commit = agent.process_calls[0][1]["worker_checkpoint_post_commit"]
    assert isinstance(post_commit, CheckpointSteeringAcker)
    assert post_commit.job_id == str(claim.unit_id)
    assert post_commit.client is client
    assert "queue_verb=complete_and_requeue" in caplog.text
    assert "complete_calls=0" in caplog.text
    assert "http_complete_calls=0" in caplog.text


@pytest.mark.asyncio
async def test_rotated_worker_claim_emits_one_mcp_split_timing_row(
    worker_runtime, monkeypatch
):
    claim = _claim(input_seq=12, prior="processing")
    writer = _FakeAuditWriter()
    executor, _, client, _, _, _, _ = _install(
        monkeypatch,
        claim,
        {
            "should_stop": True,
            "freeze_data": {"freeze_type": "batch_boundary"},
            "error": None,
        },
        audit_writer=writer,
    )
    bundle = _bundle(claim)
    bundle["job"]["datasources"] = [{"type": "mcp", "name": "bench-server"}]
    client.get_claim_bundle = AsyncMock(return_value=bundle)

    await executor._serve_worker_claim(claim)

    payload = _claim_timing_payload(writer)
    assert payload["outcome"] == "rotated"
    assert payload["lease_token"] == claim.lease_token
    assert payload["pod_name"] == "worker-pod"
    assert payload["mcp_attached"] is True


@pytest.mark.asyncio
async def test_terminal_worker_claim_emits_one_timing_row(worker_runtime, monkeypatch):
    claim = _claim(input_seq=31, prior="processing")
    writer = _FakeAuditWriter()
    executor, _, _, _, _, _, _ = _install(
        monkeypatch,
        claim,
        {
            "should_stop": True,
            "goal_achieved": True,
            "freeze_data": None,
            "error": None,
        },
        audit_writer=writer,
    )

    await executor._serve_worker_claim(claim)

    payload = _claim_timing_payload(writer)
    assert payload["outcome"] == "terminal:completed"
    assert payload["mcp_attached"] is False


@pytest.mark.asyncio
async def test_preempted_worker_claim_emits_one_timing_row(worker_runtime, monkeypatch):
    claim = _claim(input_seq=41, prior="processing")
    writer = _FakeAuditWriter()
    executor, agent, _, renew, _, complete, _ = _install(
        monkeypatch,
        claim,
        {"should_stop": True},
        audit_writer=writer,
    )
    renew.return_value = _renewal("paused")

    await executor._serve_worker_claim(claim)

    assert _claim_timing_payload(writer)["outcome"] == "preempted"
    assert agent.process_calls == []
    complete.assert_awaited_once()


@pytest.mark.asyncio
async def test_worker_claim_audit_absence_and_unready_writer_are_nonfatal(
    worker_runtime, monkeypatch, caplog
):
    claim = _claim(input_seq=12, prior="processing")
    final = {
        "should_stop": True,
        "freeze_data": {"freeze_type": "batch_boundary"},
        "error": None,
    }
    executor, _, _, _, rotate, _, _ = _install(
        monkeypatch,
        claim,
        final,
        audit_writer=None,
    )

    with caplog.at_level("WARNING"):
        await executor._serve_worker_claim(claim)

    rotate.assert_awaited_once()
    assert caplog.text.count("worker claim timing audit unavailable") == 1

    successor = replace(claim, unit=replace(claim.unit, lease_token=8))
    unready = _FakeAuditWriter(ready=False)
    executor, _, client, _, rotate, _, _ = _install(
        monkeypatch,
        successor,
        final,
        audit_writer=unready,
    )
    client.claim = successor

    await executor._serve_worker_claim(successor)

    rotate.assert_awaited_once()
    assert len(unready.rows) == 1


@pytest.mark.asyncio
async def test_terminal_reports_once_then_closes_exact_watermark(
    worker_runtime, monkeypatch
):
    claim = _claim(input_seq=31, prior="processing")
    final = {
        "should_stop": True,
        "goal_achieved": True,
        "freeze_data": None,
        "error": None,
    }
    executor, agent, client, _, rotate, complete, release = _install(
        monkeypatch, claim, final
    )

    await executor._serve_worker_claim(claim)

    client.report_completion.assert_awaited_once_with(
        str(claim.unit_id), final, lease_token=claim.lease_token
    )
    complete.assert_awaited_once_with(
        executor._db,
        unit_id=claim.unit_id,
        lease_token=claim.lease_token,
        consumed_seq=31,
    )
    rotate.assert_not_awaited()
    release.assert_not_awaited()
    assert agent.cleanup_calls == [False]


@pytest.mark.asyncio
async def test_terminal_report_waits_for_stream_generator_close(
    worker_runtime, monkeypatch
):
    claim = _claim(input_seq=32, prior="processing")
    final = {
        "should_stop": True,
        "goal_achieved": True,
        "freeze_data": None,
        "error": None,
    }
    executor, agent, client, _, _, _, _ = _install(monkeypatch, claim, final)
    events = []

    async def stream():
        try:
            yield final
        finally:
            events.append("stream_closed")

    async def report(*args, **kwargs):
        del args, kwargs
        events.append("reported")
        return True

    agent.process_job = AsyncMock(return_value=stream())
    client.report_completion.side_effect = report

    await executor._serve_worker_claim(claim)

    assert events[:2] == ["stream_closed", "reported"]


@pytest.mark.asyncio
async def test_child_quiescence_failure_retries_cleanup_then_releases_without_report(
    worker_runtime, monkeypatch
):
    claim = _claim(input_seq=33, prior="processing", attempts=5, max_attempts=5)
    final = {
        "should_stop": True,
        "goal_achieved": True,
        "freeze_data": None,
        "error": None,
    }
    executor, agent, client, _, _, _, release = _install(monkeypatch, claim, final)

    async def stream():
        yield final
        raise SubagentQuiescenceError("terminal delivery unavailable")

    agent.process_job = AsyncMock(return_value=stream())

    await executor._serve_worker_claim(claim)

    client.report_completion.assert_not_awaited()
    assert agent.cleanup_calls == [True]
    release.assert_awaited_once_with(
        executor._db,
        unit_id=claim.unit_id,
        lease_token=claim.lease_token,
        park_on_exhaustion=False,
    )


@pytest.mark.asyncio
async def test_repeated_child_quiescence_failure_escapes_without_report_or_release(
    worker_runtime, monkeypatch
):
    claim = _claim(input_seq=34, prior="processing", attempts=5, max_attempts=5)
    final = {
        "should_stop": True,
        "goal_achieved": True,
        "freeze_data": None,
        "error": None,
    }
    executor, agent, client, _, _, _, release = _install(monkeypatch, claim, final)

    async def stream():
        yield final
        raise SubagentQuiescenceError("first failure")

    agent.process_job = AsyncMock(return_value=stream())
    agent.cleanup_worker_claim = AsyncMock(
        side_effect=SubagentQuiescenceError("retry failure")
    )

    with pytest.raises(SubagentLifecycleError, match="did not fully clean"):
        await executor._serve_worker_claim(claim)

    client.report_completion.assert_not_awaited()
    release.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("final", "payload_source"),
    [
        (
            {
                "should_stop": False,
                "goal_achieved": False,
                "freeze_data": None,
                "error": None,
            },
            "live_state",
        ),
        (
            {
                "should_stop": True,
                "goal_achieved": True,
                "freeze_data": None,
                "error": None,
                "completion_report_payload": {
                    "should_stop": False,
                    "goal_achieved": False,
                    "freeze_data": None,
                    "error": None,
                },
            },
            "checkpoint_envelope",
        ),
        (
            {
                "should_stop": "true",
                "goal_achieved": False,
                "freeze_data": None,
                "error": None,
            },
            "live_state",
        ),
    ],
    ids=["live-false", "checkpoint-envelope-false", "live-truthy-non-bool"],
)
async def test_nonterminal_effective_wire_payload_fails_closed_before_report(
    worker_runtime,
    monkeypatch,
    caplog,
    final,
    payload_source,
):
    claim = _claim(input_seq=32, prior="processing")
    executor, agent, client, _, rotate, complete, release = _install(
        monkeypatch, claim, final
    )

    with caplog.at_level("ERROR"):
        await executor._serve_worker_claim(claim)

    client.report_completion.assert_not_awaited()
    complete.assert_not_awaited()
    rotate.assert_not_awaited()
    release.assert_awaited_once_with(
        executor._db,
        unit_id=claim.unit_id,
        lease_token=claim.lease_token,
        park_on_exhaustion=True,
    )
    assert agent.cleanup_calls == [True]
    assert agent.hold_calls == 0
    assert executor._worker_terminal_report_generation is None
    assert payload_source in caplog.text
    assert "effective_should_stop_not_true" in caplog.text
    assert "without /complete" in caplog.text


@pytest.mark.asyncio
async def test_terminal_checkpoint_envelope_is_wire_authority_over_live_false(
    worker_runtime,
    monkeypatch,
):
    claim = _claim(input_seq=33, prior="processing")
    final = {
        "should_stop": False,
        "goal_achieved": False,
        "freeze_data": None,
        "error": None,
        "completion_report_payload": {
            "should_stop": True,
            "goal_achieved": True,
            "freeze_data": None,
            "error": None,
        },
    }
    executor, agent, client, _, rotate, complete, release = _install(
        monkeypatch, claim, final
    )

    await executor._serve_worker_claim(claim)

    client.report_completion.assert_awaited_once_with(
        str(claim.unit_id), final, lease_token=claim.lease_token
    )
    complete.assert_awaited_once()
    rotate.assert_not_awaited()
    release.assert_not_awaited()
    assert agent.cleanup_calls == [False]


@pytest.mark.asyncio
async def test_coded_nonterminal_422_releases_normally_then_successor_reclaims(
    worker_runtime,
    monkeypatch,
    caplog,
):
    first = _claim(token=41, input_seq=34, prior="processing")
    final = {
        "should_stop": True,
        "goal_achieved": True,
        "freeze_data": None,
        "error": None,
    }
    executor, agent, client, renew, rotate, complete, release = _install(
        monkeypatch, first, final
    )
    client.report_completion.side_effect = [
        CompletionNonTerminalReportError(
            "stateless completion requires should_stop=true"
        ),
        True,
    ]
    accepted_lookup = AsyncMock()
    monkeypatch.setattr(
        turn_executor,
        "get_worker_completion_acceptance",
        accepted_lookup,
    )

    with caplog.at_level("ERROR"):
        await executor._serve_worker_claim(first)

    client.report_completion.assert_awaited_once_with(
        str(first.unit_id), final, lease_token=41
    )
    assert renew.await_count == 1  # pre-graph fence only; no ambiguity lookup
    accepted_lookup.assert_not_awaited()
    complete.assert_not_awaited()
    rotate.assert_not_awaited()
    release.assert_awaited_once_with(
        executor._db,
        unit_id=first.unit_id,
        lease_token=41,
        park_on_exhaustion=True,
    )
    assert agent.cleanup_calls == [True]
    assert agent.hold_calls == 0
    assert executor._worker_terminal_report_generation is None
    assert executor._worker_completion_accepted_generation is None
    assert (
        "unit=" + str(first.unit_id) + " token=41 code=completion_non_terminal_report"
    ) in caplog.text
    assert "stateless completion requires should_stop=true" not in caplog.text

    successor = WorkerClaim(
        unit=replace(first.unit, lease_token=42, attempts_since_completion=2),
        prior_job_status="processing",
        resume=True,
        max_attempts=first.max_attempts,
    )
    client.claim = successor
    await executor._serve_worker_claim(successor)

    assert [
        call.kwargs["lease_token"] for call in client.report_completion.await_args_list
    ] == [41, 42]
    complete.assert_awaited_once_with(
        executor._db,
        unit_id=successor.unit_id,
        lease_token=42,
        consumed_seq=34,
    )
    assert agent.cleanup_calls == [True, False]
    assert agent.hold_calls == 0


@pytest.mark.asyncio
async def test_coded_nonterminal_422_cleanup_fault_at_cap_never_rereports(
    worker_runtime,
    monkeypatch,
    caplog,
):
    claim = _claim(
        token=43,
        input_seq=35,
        prior="processing",
        attempts=5,
        max_attempts=5,
    )
    final = {"should_stop": True, "goal_achieved": True, "error": None}
    executor, agent, client, renew, rotate, complete, release = _install(
        monkeypatch, claim, final
    )
    client.report_completion.side_effect = CompletionNonTerminalReportError(
        "body must not be logged"
    )
    cleanup = AsyncMock(side_effect=RuntimeError("cleanup detail must not leak"))
    agent.cleanup_worker_claim = cleanup
    accepted_lookup = AsyncMock()
    monkeypatch.setattr(
        turn_executor,
        "get_worker_completion_acceptance",
        accepted_lookup,
    )

    with caplog.at_level("ERROR"):
        await executor._serve_worker_claim(claim)

    client.report_completion.assert_awaited_once_with(
        str(claim.unit_id), final, lease_token=43
    )
    cleanup.assert_awaited_once_with(preserve_shell=True)
    assert renew.await_count == 1  # pre-graph fence only
    accepted_lookup.assert_not_awaited()
    complete.assert_not_awaited()
    rotate.assert_not_awaited()
    release.assert_awaited_once_with(
        executor._db,
        unit_id=claim.unit_id,
        lease_token=43,
        park_on_exhaustion=True,
    )
    assert agent.hold_calls == 0
    assert executor._worker_terminal_report_generation is None
    assert executor._worker_completion_accepted_generation is None
    assert "worker completion refusal cleanup failed" in caplog.text
    assert "body must not be logged" not in caplog.text
    assert "cleanup detail must not leak" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("report_result", [True, False])
@pytest.mark.parametrize("workspace_recovery", [False, True])
async def test_command_accept_queue_closure_is_not_misclassified_as_lease_loss(
    worker_runtime,
    monkeypatch,
    report_result,
    workspace_recovery,
):
    """B4 closes the queue inside accept, before the HTTP response returns."""

    claim = _claim(input_seq=31, prior="processing")
    final = {
        "should_stop": True,
        "goal_achieved": True,
        "freeze_data": None,
        "error": None,
    }
    executor, agent, client, renew, rotate, complete, release = _install(
        monkeypatch,
        claim,
        final,
        report_result=report_result,
    )
    executor._completion_commands_enabled = True
    renew.side_effect = [_renewal("processing"), None]
    acceptance = _acceptance()
    accepted_lookup = AsyncMock(return_value=acceptance)
    monkeypatch.setattr(
        turn_executor,
        "get_worker_completion_acceptance",
        accepted_lookup,
    )
    if workspace_recovery:
        monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "true")
        client.get_workspace_recovery_disposition.return_value = _recovery_receipt(
            claim
        )

    await executor._serve_worker_claim(claim)

    if workspace_recovery:
        client.report_completion.assert_not_awaited()
        accepted_lookup.assert_not_awaited()
        release.assert_not_awaited()
        complete.assert_not_awaited()
        assert agent.cleanup_calls == [True]
        return

    client.report_completion.assert_awaited_once()
    accepted_lookup.assert_awaited_once_with(
        executor._db,
        unit_id=claim.unit_id,
        lease_token=claim.lease_token,
    )
    complete.assert_not_awaited()
    release.assert_not_awaited()
    rotate.assert_not_awaited()
    assert executor._lease.lost.is_set() is False
    assert agent.hold_calls == 0
    assert agent.cleanup_calls == [False]


@pytest.mark.asyncio
@pytest.mark.parametrize("command_state", ["pending", "finalizing"])
@pytest.mark.parametrize("terminal_status", ["completed", "failed", "cancelled"])
async def test_accepted_unfinished_command_holds_then_retires_terminal_shell_once(
    worker_runtime,
    monkeypatch,
    command_state,
    terminal_status,
):
    claim = _claim(input_seq=31, prior="processing")
    final = {"should_stop": True, "goal_achieved": True, "error": None}
    executor, agent, client, renew, rotate, complete, release = _install(
        monkeypatch,
        claim,
        final,
    )
    executor._completion_commands_enabled = True
    executor._sleep_worker_finalization_poll = AsyncMock()
    renew.side_effect = [_renewal("processing"), None]
    command_id = uuid4()
    accepted_lookup = AsyncMock(
        side_effect=[
            _acceptance(
                command_id=command_id,
                command_state=command_state,
                job_status="processing",
                outcome={},
            ),
            _acceptance(
                command_id=command_id,
                command_state="done",
                job_status=terminal_status,
                outcome={"new_status": terminal_status},
            ),
        ]
    )
    monkeypatch.setattr(
        turn_executor,
        "get_worker_completion_acceptance",
        accepted_lookup,
    )

    await executor._serve_worker_claim(claim)

    assert agent.hold_calls == 1
    assert agent.cleanup_calls == [False]
    assert accepted_lookup.await_args_list[1].kwargs["command_id"] == command_id
    complete.assert_not_awaited()
    release.assert_not_awaited()
    rotate.assert_not_awaited()
    client.report_completion.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stored_status",
    [
        "processing",
        "reviewing",
        "pending_review",
        "paused",
        "waiting",
        "waiting_for_reply",
    ],
)
async def test_accepted_unfinished_command_preserves_all_nonterminal_outcomes(
    worker_runtime,
    monkeypatch,
    stored_status,
):
    claim = _claim(input_seq=31, prior="processing")
    executor, agent, client, renew, rotate, complete, release = _install(
        monkeypatch,
        claim,
        {"should_stop": True, "goal_achieved": True},
    )
    executor._completion_commands_enabled = True
    executor._sleep_worker_finalization_poll = AsyncMock()
    renew.side_effect = [_renewal("processing"), None]
    command_id = uuid4()
    accepted_lookup = AsyncMock(
        side_effect=[
            _acceptance(
                command_id=command_id,
                command_state="pending",
                job_status="processing",
                outcome={},
            ),
            _acceptance(
                command_id=command_id,
                command_state="done",
                job_status=stored_status,
                outcome={"new_status": stored_status},
            ),
        ]
    )
    monkeypatch.setattr(
        turn_executor,
        "get_worker_completion_acceptance",
        accepted_lookup,
    )

    await executor._serve_worker_claim(claim)

    assert agent.hold_calls == 1
    assert agent.cleanup_calls == [True]
    assert accepted_lookup.await_args_list[1].kwargs["command_id"] == command_id
    complete.assert_not_awaited()
    release.assert_not_awaited()
    rotate.assert_not_awaited()
    client.report_completion.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("command_state", "outcome", "preserve_shell"),
    [
        ("superseded", {"observed_status": "cancelled"}, False),
        ("superseded", {"observed_job_status": "failed"}, False),
        ("superseded", {"observed_status": "paused"}, True),
        ("force_resolved", {"terminal_status": "completed"}, False),
        ("force_resolved", {"terminal_status": "failed"}, False),
        ("force_resolved", {"terminal_status": "cancelled"}, False),
    ],
)
async def test_accepted_command_uses_finalized_outcome_not_fixture_job_status(
    worker_runtime,
    monkeypatch,
    command_state,
    outcome,
    preserve_shell,
):
    claim = _claim(input_seq=31, prior="processing")
    executor, agent, client, renew, rotate, complete, release = _install(
        monkeypatch,
        claim,
        {"should_stop": True, "goal_achieved": True},
    )
    executor._completion_commands_enabled = True
    renew.side_effect = [_renewal("processing"), None]
    acceptance = _acceptance(
        command_state=command_state,
        # Deliberately contradictory: disposition must come from outcome.
        job_status="processing" if not preserve_shell else "completed",
        outcome=outcome,
    )
    accepted_lookup = AsyncMock(return_value=acceptance)
    monkeypatch.setattr(
        turn_executor,
        "get_worker_completion_acceptance",
        accepted_lookup,
    )

    await executor._serve_worker_claim(claim)

    assert agent.hold_calls == 0
    assert agent.cleanup_calls == [preserve_shell]
    complete.assert_not_awaited()
    release.assert_not_awaited()
    rotate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tail",
    [
        _acceptance(command_state="parked", job_status="processing", outcome={}),
        _acceptance(
            command_state="pending",
            job_status="processing",
            outcome={},
            deadline_expired=True,
            deadline_remaining_seconds=0.0,
        ),
        None,
    ],
    ids=["parked", "deadline", "lookup-loss"],
)
async def test_accepted_hold_hands_back_nonfinalized_command_without_queue_verb(
    worker_runtime,
    monkeypatch,
    tail,
):
    claim = _claim(input_seq=31, prior="processing")
    executor, agent, client, renew, rotate, complete, release = _install(
        monkeypatch,
        claim,
        {"should_stop": True, "goal_achieved": True},
    )
    executor._completion_commands_enabled = True
    executor._sleep_worker_finalization_poll = AsyncMock()
    renew.side_effect = [_renewal("processing"), None]
    command_id = uuid4()
    first = _acceptance(
        command_id=command_id,
        command_state="pending",
        job_status="processing",
        outcome={},
    )
    if tail is not None:
        tail = replace(tail, command_id=command_id)
    accepted_lookup = AsyncMock(side_effect=[first, tail])
    monkeypatch.setattr(
        turn_executor,
        "get_worker_completion_acceptance",
        accepted_lookup,
    )

    await executor._serve_worker_claim(claim)

    assert agent.hold_calls == 1
    assert agent.cleanup_calls == [True]
    assert accepted_lookup.await_args_list[1].kwargs["command_id"] == command_id
    complete.assert_not_awaited()
    release.assert_not_awaited()
    rotate.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancelled_accepted_hold_preserves_shell_without_requeue(
    worker_runtime,
    monkeypatch,
):
    claim = _claim(input_seq=31, prior="processing")
    executor, agent, client, renew, rotate, complete, release = _install(
        monkeypatch,
        claim,
        {"should_stop": True, "goal_achieved": True},
    )
    executor._completion_commands_enabled = True
    renew.side_effect = [_renewal("processing"), None]
    command_id = uuid4()
    accepted_lookup = AsyncMock(
        return_value=_acceptance(
            command_id=command_id,
            command_state="pending",
            job_status="processing",
            outcome={},
        )
    )
    monkeypatch.setattr(
        turn_executor,
        "get_worker_completion_acceptance",
        accepted_lookup,
    )
    sleep_started = asyncio.Event()

    async def block_poll(_seconds):
        sleep_started.set()
        await asyncio.Event().wait()

    executor._sleep_worker_finalization_poll = block_poll

    task = asyncio.create_task(executor._serve_worker_claim(claim))
    await sleep_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert agent.hold_calls == 1
    assert agent.cleanup_calls == [True]
    complete.assert_not_awaited()
    release.assert_not_awaited()
    rotate.assert_not_awaited()


@pytest.mark.asyncio
async def test_crashed_accepted_hold_defers_to_command_backstop_without_requeue(
    worker_runtime,
    monkeypatch,
):
    claim = _claim(input_seq=31, prior="processing")
    executor, agent, client, renew, rotate, complete, release = _install(
        monkeypatch,
        claim,
        {"should_stop": True, "goal_achieved": True},
    )
    executor._completion_commands_enabled = True
    renew.side_effect = [_renewal("processing"), None]
    accepted_lookup = AsyncMock(
        return_value=_acceptance(
            command_state="pending",
            job_status="processing",
            outcome={},
        )
    )
    monkeypatch.setattr(
        turn_executor,
        "get_worker_completion_acceptance",
        accepted_lookup,
    )
    executor._sleep_worker_finalization_poll = AsyncMock(
        side_effect=RuntimeError("driver crashed after B4 accept")
    )

    await executor._serve_worker_claim(claim)

    assert agent.hold_calls == 1
    assert agent.cleanup_calls == [True]
    complete.assert_not_awaited()
    release.assert_not_awaited()
    rotate.assert_not_awaited()


@pytest.mark.asyncio
async def test_completion_acceptance_lookup_returns_db_clock_bounds_and_exact_command():
    command_id = uuid4()
    unit_id = uuid4()
    now = datetime.now(timezone.utc)
    row = {
        "job_status": "processing",
        "queue_state": "done",
        "command_state": "finalizing",
        "command_id": command_id,
        "command_outcome": {"attempt": 2},
        "deadline_at": now + timedelta(minutes=2),
        "lease_expires_at": now + timedelta(seconds=20),
        "run_after": now,
        "deadline_expired": False,
        "deadline_remaining_seconds": 120.0,
        "lease_remaining_seconds": 20.0,
        "run_after_remaining_seconds": 0.0,
    }
    conn = SimpleNamespace(fetchrow=AsyncMock(return_value=row))

    accepted = await get_worker_completion_acceptance(
        conn,
        unit_id=unit_id,
        lease_token=7,
        command_id=command_id,
    )

    assert accepted is not None
    assert accepted.command_id == command_id
    assert accepted.command_outcome == {"attempt": 2}
    assert accepted.deadline_remaining_seconds == 120.0
    assert accepted.lease_remaining_seconds == 20.0
    assert accepted.deadline_expired is False
    assert conn.fetchrow.await_args.args[1:] == (unit_id, 7, command_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("preserve_shell", [False, True])
async def test_agent_finalization_hold_retires_admission_then_disposes_shell_once(
    preserve_shell,
):
    events: list[str] = []
    backend = SimpleNamespace(
        retire_shell_owner=MagicMock(
            side_effect=lambda: events.append("retire_admission")
        ),
        make_terminal_shell_cleanup_capability=MagicMock(),
        retire=MagicMock(side_effect=lambda: events.append("retire_backend")),
    )
    terminal_cleanup = MagicMock(side_effect=lambda: events.append("terminal_cleanup"))
    backend.make_terminal_shell_cleanup_capability.side_effect = lambda: (
        events.append("fork_cleanup") or terminal_cleanup
    )
    shell = SimpleNamespace(
        cleanup=MagicMock(side_effect=lambda: events.append("shell_cleanup"))
    )
    tool_context = SimpleNamespace(
        shell_manager=shell,
        citation_verdict_callback=object(),
    )
    agent = UniversalAgent.__new__(UniversalAgent)
    agent._current_job_id = "job-a"
    agent._worker_lease_token = 7
    agent._workspace_manager = SimpleNamespace(backend=backend)
    agent._shell_manager = shell
    agent._tool_context = tool_context
    agent._doc_registration_task = None
    agent._knowledge_graph = None
    agent._datasource_connections = {
        "postgresql": SimpleNamespace(
            close=MagicMock(side_effect=lambda: events.append("scrub"))
        )
    }
    agent._datasource_clients = {}
    agent._datasource_files_manifest = None
    agent._checkpoint_conn = None
    agent._checkpointer = object()
    agent._worker_env_restore = {}
    agent._job_metadata = {"secret": "scrub-me"}
    agent._todo_manager = object()
    agent._tools = [object()]
    agent._graph = object()
    agent._worker_checkpoint_post_commit = object()
    agent._defer_job_cleanup = True

    await agent.hold_worker_finalization()

    assert events == [
        "retire_admission",
        "fork_cleanup",
        "retire_backend",
        "scrub",
    ]
    assert agent._shell_manager is None
    assert agent._worker_finalization_backend is backend
    assert agent._worker_terminal_shell_cleanup is terminal_cleanup
    assert agent._worker_finalization_held is True
    assert tool_context.shell_manager is None
    assert tool_context.citation_verdict_callback is None
    assert agent._tool_context is None
    assert agent._job_metadata is None
    assert agent._checkpointer is None
    assert agent._graph is None

    # Idempotent hold must neither re-retire admission nor scrub/disconnect twice.
    await agent.hold_worker_finalization()
    assert events == [
        "retire_admission",
        "fork_cleanup",
        "retire_backend",
        "scrub",
    ]

    await agent.cleanup_worker_claim(preserve_shell=preserve_shell)

    assert backend.retire_shell_owner.call_count == 1
    shell.cleanup.assert_not_called()
    assert terminal_cleanup.call_count == (0 if preserve_shell else 1)
    assert backend.retire.call_count == 1
    assert events == (
        [
            "retire_admission",
            "fork_cleanup",
            "retire_backend",
            "scrub",
        ]
        if preserve_shell
        else [
            "retire_admission",
            "fork_cleanup",
            "retire_backend",
            "scrub",
            "terminal_cleanup",
        ]
    )
    assert agent._shell_manager is None
    assert agent._worker_finalization_backend is None
    assert agent._worker_finalization_held is False


@pytest.mark.asyncio
@pytest.mark.parametrize("workspace_recovery", [False, True])
@pytest.mark.parametrize("io_kind", ["resource", "sftp"])
async def test_agent_hold_drains_admitted_resource_io_and_retires_original_backend(
    workspace_recovery, io_kind
):
    job_id = str(uuid4())
    workspace_generation = str(uuid4())
    runtime_incarnation = str(uuid4())
    backend = RemoteBackend(
        host="workspace.invalid",
        key_path="/unused/test-key",
        job_id=job_id,
        workspace_generation=workspace_generation,
        runtime_incarnation=runtime_incarnation,
        workspace_owner_kind="job",
        workspace_owner_id=job_id,
    )
    backend.set_shell_owner_token(7)
    backend._shell_generation = "a" * 32
    admitted = threading.Event()
    release = threading.Event()

    def admitted_exec(*_args, **_kwargs):
        admitted.set()
        assert release.wait(timeout=2)
        return "finished", 0

    backend._exec_with_status = MagicMock(side_effect=admitted_exec)
    agent = UniversalAgent.__new__(UniversalAgent)
    agent._current_job_id = job_id
    agent._worker_lease_token = 7
    agent._workspace_manager = SimpleNamespace(backend=backend)
    agent._shell_manager = object()
    agent._tool_context = None
    agent._doc_registration_task = None
    agent._knowledge_graph = None
    agent._datasource_connections = {}
    agent._datasource_clients = {}
    agent._datasource_files_manifest = None
    agent._checkpoint_conn = None
    agent._checkpointer = None
    agent._worker_env_restore = {}
    agent._job_metadata = {}
    agent._todo_manager = None
    agent._tools = []
    agent._graph = object()
    agent._worker_checkpoint_post_commit = None
    agent._defer_job_cleanup = True
    child = SimpleNamespace(abandon=AsyncMock())
    if workspace_recovery:
        agent._tool_context = SimpleNamespace(subagent_runtime=child)
    shared_pool = SimpleNamespace(close=AsyncMock())
    agent._checkpointer = SimpleNamespace(conn=shared_pool)

    def sftp_call():
        with backend._sftp_lock:
            return admitted_exec()[0]

    resource_task = asyncio.create_task(
        asyncio.to_thread(
            sftp_call if io_kind == "sftp" else lambda: backend.exec_claim_resource(":")
        )
    )
    assert await asyncio.to_thread(admitted.wait, 1)
    hold_task = asyncio.create_task(
        agent.quiesce_worker_workspace_recovery()
        if workspace_recovery
        else agent.hold_worker_finalization()
    )
    await asyncio.sleep(0.02)

    # retire() cannot publish the hold until the admitted resource mutation
    # leaves its local admission lock.
    assert hold_task.done() is False
    release.set()
    assert await resource_task == "finished"
    await hold_task

    assert backend._retired is True
    assert backend._claim_resource_retired is True
    with pytest.raises(WorkspaceUnavailableError):
        backend.exec_claim_resource(":")
    with pytest.raises(WorkspaceUnavailableError):
        backend.write_file("late.txt", "must fail")
    shared_pool.close.assert_not_awaited()
    assert agent._graph is None and agent._checkpointer is None
    if workspace_recovery:
        assert agent._subagent_abandoned_runtime is None
        assert agent._workspace_manager is None
        assert agent._shell_manager is None
        assert not getattr(agent, "_worker_terminal_shell_cleanup", None)
        return
    capability = agent._worker_terminal_shell_cleanup
    assert callable(capability)
    assert not hasattr(capability, "exec_command")
    assert not hasattr(capability, "write_file")
    assert not hasattr(capability, "shell_run")

    await agent.cleanup_worker_claim(preserve_shell=True)


def test_remote_terminal_cleanup_capability_has_exact_fence_and_only_cleanup(
    monkeypatch,
):
    job_id = str(uuid4())
    workspace_generation = str(uuid4())
    runtime_incarnation = str(uuid4())
    backend = RemoteBackend(
        host="workspace.invalid",
        key_path="/unused/test-key",
        job_id=job_id,
        workspace_generation=workspace_generation,
        runtime_incarnation=runtime_incarnation,
        workspace_owner_kind="job",
        workspace_owner_id=job_id,
    )
    backend.set_shell_owner_token(19)
    observed: list[tuple[str, str, str, int | None, bool, bool]] = []

    def terminal_cleanup(cleanup_backend):
        observed.append(
            (
                cleanup_backend._job_id,
                cleanup_backend._workspace_generation,
                cleanup_backend._runtime_incarnation,
                cleanup_backend._shell_owner_token,
                cleanup_backend._shell_retired,
                cleanup_backend._claim_resource_retired,
            )
        )

    retired: list[RemoteBackend] = []
    monkeypatch.setattr(RemoteBackend, "shell_cleanup", terminal_cleanup)
    monkeypatch.setattr(
        RemoteBackend,
        "retire",
        lambda cleanup_backend: retired.append(cleanup_backend),
    )

    capability = backend.make_terminal_shell_cleanup_capability()

    assert callable(capability)
    assert not hasattr(capability, "exec_command")
    assert not hasattr(capability, "write_file")
    assert not hasattr(capability, "shell_run")
    with pytest.raises(AttributeError):
        capability.shell_owner_token = 20
    capability()
    assert observed == [
        (
            job_id,
            workspace_generation,
            runtime_incarnation,
            19,
            True,
            True,
        )
    ]
    assert len(retired) == 1


@pytest.mark.asyncio
async def test_recoverable_end_releases_without_reporting(worker_runtime, monkeypatch):
    claim = _claim(input_seq=4, prior="processing")
    final = {
        "should_stop": True,
        "freeze_data": None,
        "error": {"type": "infra_transient", "recoverable": True},
    }
    executor, agent, client, _, rotate, complete, release = _install(
        monkeypatch, claim, final
    )

    await executor._serve_worker_claim(claim)

    client.report_completion.assert_not_awaited()
    release.assert_awaited_once_with(
        executor._db,
        unit_id=claim.unit_id,
        lease_token=claim.lease_token,
        park_on_exhaustion=True,
    )
    rotate.assert_not_awaited()
    complete.assert_not_awaited()
    assert agent.cleanup_calls == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize("workspace_recovery", [False, True])
async def test_last_recoverable_attempt_reports_visible_terminal_give_up(
    worker_runtime, monkeypatch, workspace_recovery
):
    claim = _claim(input_seq=4, prior="processing", attempts=5, max_attempts=5)
    final = {
        "should_stop": True,
        "freeze_data": {
            "freeze_type": "llm_unavailable",
            "reason": "endpoint offline",
        },
        "error": {"type": "llm_unavailable", "recoverable": True},
    }
    executor, agent, client, _, rotate, complete, release = _install(
        monkeypatch, claim, final
    )
    if workspace_recovery:
        monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "true")
        final["error"]["type"] = "workspace_unavailable"
        client.backend = "vm"
        client.report_workspace_recovery.return_value = _recovery_receipt(claim)

    await executor._serve_worker_claim(claim)

    if workspace_recovery:
        client.report_completion.assert_not_awaited()
        client.report_workspace_recovery.assert_awaited_once()
        release.assert_not_awaited()
        complete.assert_not_awaited()
        assert agent.cleanup_calls == [True]
        return

    client.report_completion.assert_awaited_once()
    reported = client.report_completion.await_args.args[1]
    assert reported["error"]["type"] == "worker_retry_exhausted"
    assert reported["error"]["recoverable"] is False
    assert reported["freeze_data"]["freeze_type"] == "worker_retry_exhausted"
    assert reported["freeze_data"]["attempts"] == 5
    complete.assert_awaited_once()
    rotate.assert_not_awaited()
    release.assert_not_awaited()
    assert agent.cleanup_calls == [False]


@pytest.mark.asyncio
@pytest.mark.parametrize("workspace_recovery", [False, True])
async def test_last_pregraph_driver_failure_reports_visible_terminal_give_up(
    worker_runtime, monkeypatch, workspace_recovery
):
    claim = _claim(input_seq=4, prior="processing", attempts=5, max_attempts=5)
    executor, agent, client, _, rotate, complete, release = _install(
        monkeypatch,
        claim,
        {
            "should_stop": True,
            "error": {"type": "infra_transient", "recoverable": True},
        },
    )
    client.get_claim_bundle = AsyncMock(
        side_effect=RuntimeError("bundle credentials were revoked")
    )
    if workspace_recovery:
        client.get_claim_bundle.side_effect = ClaimBundleError(
            409,
            recovery=_recovery_receipt(claim),
            code=WorkspaceRecoveryCode.RUNTIME_NOT_READY,
        )

    await executor._serve_worker_claim(claim)

    if workspace_recovery:
        client.report_completion.assert_not_awaited()
        complete.assert_not_awaited()
        release.assert_not_awaited()
        assert agent.cleanup_calls == [True]
        return

    client.report_completion.assert_awaited_once()
    reported = client.report_completion.await_args.args[1]
    assert reported["error"]["type"] == "worker_retry_exhausted"
    assert reported["error"]["recoverable"] is False
    assert reported["freeze_data"]["prior_freeze"] is None
    complete.assert_awaited_once_with(
        executor._db,
        unit_id=claim.unit_id,
        lease_token=claim.lease_token,
        consumed_seq=4,
    )
    rotate.assert_not_awaited()
    release.assert_not_awaited()
    assert agent.cleanup_calls == [False]


@pytest.mark.asyncio
async def test_failed_max_give_up_successor_re_reports_without_running_work(
    worker_runtime, monkeypatch
):
    first = _claim(input_seq=4, prior="processing", attempts=5, max_attempts=5)
    recoverable = {
        "should_stop": True,
        "freeze_data": {"freeze_type": "llm_unavailable"},
        "error": {"type": "llm_unavailable", "recoverable": True},
    }
    executor, agent, client, _, rotate, complete, release = _install(
        monkeypatch,
        first,
        recoverable,
    )
    client.report_completion.side_effect = [False, True]
    client.get_claim_bundle = AsyncMock(return_value=_bundle(first))

    await executor._serve_worker_claim(first)
    successor = WorkerClaim(
        unit=replace(first.unit, lease_token=8, attempts_since_completion=6),
        prior_job_status="processing",
        resume=True,
        max_attempts=5,
    )
    client.claim = successor
    await executor._serve_worker_claim(successor)

    assert client.report_completion.await_count == 2
    assert client.get_claim_bundle.await_count == 2
    assert len(agent.process_calls) == 2
    assert agent.process_calls[1][1]["worker_retry_exhausted"] is True
    for call in client.report_completion.await_args_list:
        assert call.args[1]["error"]["type"] == "worker_retry_exhausted"
    release.assert_awaited_once_with(
        executor._db,
        unit_id=first.unit_id,
        lease_token=first.lease_token,
        park_on_exhaustion=False,
    )
    complete.assert_awaited_once_with(
        executor._db,
        unit_id=successor.unit_id,
        lease_token=successor.lease_token,
        consumed_seq=4,
    )
    rotate.assert_not_awaited()


@pytest.mark.asyncio
async def test_over_budget_claim_detaches_warm_session_before_terminal_report(
    worker_runtime, monkeypatch
):
    claim = _claim(input_seq=4, prior="processing", attempts=6, max_attempts=5)
    executor, agent, client, _, rotate, complete, release = _install(
        monkeypatch,
        claim,
        {
            "should_stop": True,
            "error": {"type": "infra_transient", "recoverable": True},
        },
    )
    events: list[str] = []
    pa._session = object()

    async def detach(reason):
        assert reason == "worker_claim_switch"
        events.append("detached")
        pa._session = None

    async def report(*args, **kwargs):
        events.append("reported")
        return True

    executor._detach_cached_session = AsyncMock(side_effect=detach)
    client.report_completion.side_effect = report
    client.get_claim_bundle = AsyncMock(return_value=_bundle(claim))

    await executor._serve_worker_claim(claim)

    assert events[:2] == ["detached", "reported"]
    executor._detach_cached_session.assert_awaited_once_with("worker_claim_switch")
    client.get_claim_bundle.assert_awaited_once()
    assert len(agent.process_calls) == 1
    assert agent.process_calls[0][1]["worker_retry_exhausted"] is True
    client.report_completion.assert_awaited_once()
    complete.assert_awaited_once()
    rotate.assert_not_awaited()
    release.assert_not_awaited()


@pytest.mark.asyncio
async def test_over_budget_successor_re_reports_genuine_end_unchanged(
    worker_runtime, monkeypatch
):
    first = _claim(input_seq=11, prior="processing", attempts=5, max_attempts=5)
    terminal = {
        "should_stop": True,
        "goal_achieved": True,
        "freeze_data": None,
        "error": None,
    }
    executor, agent, client, _, rotate, complete, release = _install(
        monkeypatch,
        first,
        terminal,
    )
    client.report_completion.side_effect = [False, True]

    await executor._serve_worker_claim(first)
    successor = WorkerClaim(
        unit=replace(first.unit, lease_token=8, attempts_since_completion=6),
        prior_job_status="processing",
        resume=True,
        max_attempts=5,
    )
    client.claim = successor
    await executor._serve_worker_claim(successor)

    assert client.report_completion.await_count == 2
    assert [call.args[1] for call in client.report_completion.await_args_list] == [
        terminal,
        terminal,
    ]
    assert len(agent.process_calls) == 2
    assert agent.process_calls[1][1]["worker_retry_exhausted"] is True
    release.assert_awaited_once_with(
        executor._db,
        unit_id=first.unit_id,
        lease_token=first.lease_token,
        park_on_exhaustion=False,
    )
    complete.assert_awaited_once_with(
        executor._db,
        unit_id=successor.unit_id,
        lease_token=successor.lease_token,
        consumed_seq=11,
    )
    rotate.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_stream_at_max_reports_give_up_instead_of_parking(
    worker_runtime, monkeypatch
):
    claim = _claim(input_seq=3, prior="processing", attempts=5, max_attempts=5)
    executor, agent, client, _, rotate, complete, release = _install(
        monkeypatch,
        claim,
        {"should_stop": False},
    )

    async def empty_stream():
        if False:  # pragma: no cover - makes this an async generator
            yield {}

    agent.process_job = AsyncMock(return_value=empty_stream())

    await executor._serve_worker_claim(claim)

    client.report_completion.assert_awaited_once()
    reported = client.report_completion.await_args.args[1]
    assert reported["error"]["type"] == "worker_retry_exhausted"
    assert "without a durable terminal state" in reported["error"]["message"]
    complete.assert_awaited_once()
    rotate.assert_not_awaited()
    release.assert_not_awaited()


@pytest.mark.asyncio
async def test_post_report_tail_failure_never_reports_twice_or_parks_at_max(
    worker_runtime, monkeypatch
):
    claim = _claim(input_seq=6, prior="processing", attempts=5, max_attempts=5)
    final = {"should_stop": True, "goal_achieved": True}
    executor, agent, client, renew, rotate, complete, release = _install(
        monkeypatch,
        claim,
        final,
    )
    renew.side_effect = [
        _renewal("processing"),
        RuntimeError("post-report renewal transient"),
    ]

    await executor._serve_worker_claim(claim)

    client.report_completion.assert_awaited_once_with(
        str(claim.unit_id), final, lease_token=claim.lease_token
    )
    complete.assert_not_awaited()
    rotate.assert_not_awaited()
    release.assert_awaited_once_with(
        executor._db,
        unit_id=claim.unit_id,
        lease_token=claim.lease_token,
        park_on_exhaustion=False,
    )
    assert agent.cleanup_calls == [True]


@pytest.mark.asyncio
async def test_terminal_report_failure_preserves_shell_and_error_releases(
    worker_runtime, monkeypatch
):
    claim = _claim(input_seq=9, prior="processing")
    final = {"should_stop": True, "goal_achieved": True}
    executor, agent, client, _, _, complete, release = _install(
        monkeypatch,
        claim,
        final,
        report_result=False,
    )

    await executor._serve_worker_claim(claim)

    client.report_completion.assert_awaited_once()
    assert agent.cleanup_calls == [True]
    complete.assert_not_awaited()
    release.assert_awaited_once_with(
        executor._db,
        unit_id=claim.unit_id,
        lease_token=claim.lease_token,
        park_on_exhaustion=False,
    )


@pytest.mark.asyncio
async def test_cancel_winning_during_terminal_report_closes_without_error_release(
    worker_runtime,
    monkeypatch,
    caplog,
):
    claim = _claim(input_seq=10, prior="processing")
    final = {"should_stop": True, "goal_achieved": True}
    executor, agent, client, renew, _, complete, release = _install(
        monkeypatch,
        claim,
        final,
        report_result=False,
    )
    renew.side_effect = [_renewal("processing"), _renewal("cancelled")]

    with caplog.at_level("INFO"):
        await executor._serve_worker_claim(claim)

    client.report_completion.assert_awaited_once()
    complete.assert_awaited_once_with(
        executor._db,
        unit_id=claim.unit_id,
        lease_token=claim.lease_token,
        consumed_seq=10,
    )
    release.assert_not_awaited()
    assert agent.cleanup_calls == [False]
    assert "status=cancelled" in caplog.text
    assert "http_complete_calls=1" in caplog.text


@pytest.mark.asyncio
async def test_terminal_report_failure_reclaims_end_and_second_report_closes_queue(
    worker_runtime, monkeypatch
):
    first = _claim(token=21, input_seq=9, prior="processing")
    final = {"should_stop": True, "goal_achieved": True, "error": None}
    executor, agent, client, _, _, complete, release = _install(
        monkeypatch,
        first,
        final,
    )
    client.report_completion.side_effect = [False, True]

    await executor._serve_worker_claim(first)
    successor = WorkerClaim(
        unit=replace(first.unit, lease_token=22, attempts_since_completion=2),
        prior_job_status="processing",
        resume=True,
    )
    client.claim = successor
    await executor._serve_worker_claim(successor)

    assert client.report_completion.await_count == 2
    assert [
        call.kwargs["lease_token"] for call in client.report_completion.await_args_list
    ] == [
        21,
        22,
    ]
    release.assert_awaited_once_with(
        executor._db,
        unit_id=first.unit_id,
        lease_token=21,
        park_on_exhaustion=False,
    )
    complete.assert_awaited_once_with(
        executor._db,
        unit_id=successor.unit_id,
        lease_token=22,
        consumed_seq=9,
    )
    assert agent.cleanup_calls == [True, False]


@pytest.mark.asyncio
async def test_claim_time_pause_preempts_with_zero_report_calls(
    worker_runtime, monkeypatch
):
    claim = _claim(input_seq=5, prior="processing")
    executor, agent, client, renew, _, complete, release = _install(
        monkeypatch,
        claim,
        {"should_stop": False},
    )
    renew.return_value = _renewal("paused")

    await executor._serve_worker_claim(claim)

    assert agent.process_calls == []
    client.report_completion.assert_not_awaited()
    assert agent.cleanup_calls == [True]
    complete.assert_awaited_once_with(
        executor._db,
        unit_id=claim.unit_id,
        lease_token=claim.lease_token,
        consumed_seq=5,
    )
    release.assert_not_awaited()


def _build_worker_frontier_graph():
    """Real LangGraph END/re-entry shape used by worker resume regressions."""
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import END, START, StateGraph

    ran: list[str] = []
    armed_targets: list[float | None] = []

    # Leave router/node arguments unannotated so branch schema inference does
    # not need to resolve a local callable annotation.
    def route(state):
        if not state.get("initialized"):
            return "init_workspace"
        if state.get("resume_feedback"):
            return "restore_from_feedback"
        return "restore_todo_state"

    def init_workspace(_state):
        ran.append("init_workspace")
        return {"initialized": True}

    def restore_todo_state(_state):
        ran.append("restore_todo_state")
        return {"should_stop": False}

    def restore_from_feedback(_state):
        ran.append("restore_from_feedback")
        return {"should_stop": False, "resume_feedback": None}

    def execute(state):
        ran.append("execute")
        armed_targets.append(state.get("worker_batch_target_wall_seconds"))
        return {
            "iteration": int(state.get("iteration") or 0) + 1,
            "should_stop": True,
            "goal_achieved": False,
            "freeze_data": {"freeze_type": "batch_boundary"},
        }

    def checkpoint_completion_report(_state):
        ran.append("checkpoint_completion_report")
        return {}

    workflow = StateGraph(_WorkerFrontierState)
    workflow.add_node("init_workspace", init_workspace)
    workflow.add_node("restore_todo_state", restore_todo_state)
    workflow.add_node("restore_from_feedback", restore_from_feedback)
    workflow.add_node("execute", execute)
    workflow.add_node("checkpoint_completion_report", checkpoint_completion_report)
    workflow.add_conditional_edges(
        START,
        route,
        {
            "init_workspace": "init_workspace",
            "restore_todo_state": "restore_todo_state",
            "restore_from_feedback": "restore_from_feedback",
        },
    )
    workflow.add_edge("init_workspace", "execute")
    workflow.add_edge("restore_todo_state", "execute")
    workflow.add_edge("restore_from_feedback", "execute")
    workflow.add_edge("execute", "checkpoint_completion_report")
    workflow.add_edge("checkpoint_completion_report", END)
    return workflow.compile(checkpointer=InMemorySaver()), ran, armed_targets


class TestWorkerBatchArming:
    def test_worker_claim_hydrates_checkpointed_instruction_reads(self):
        context = SimpleNamespace(
            restore_instruction_read_receipts=MagicMock(return_value=1)
        )
        agent = UniversalAgent.__new__(UniversalAgent)
        agent._tool_context = context
        receipts = {
            "skills/verify-before-done/SKILL.md": {
                "phase": "tactical",
                "phase_number": 1,
                "turn_count": 4,
            }
        }

        assert (
            agent._restore_worker_instruction_reads(
                {"instruction_read_receipts": receipts}
            )
            == 1
        )
        context.restore_instruction_read_receipts.assert_called_once_with(receipts)

    @pytest.mark.asyncio
    async def test_fresh_stateless_resume_injects_feedback_before_first_checkpoint(
        self,
    ):
        graph = SimpleNamespace(aupdate_state=AsyncMock())
        agent = UniversalAgent.__new__(UniversalAgent)
        agent._graph = graph
        graph_input = {"messages": [], "should_stop": True}
        checkpoint_values = {}
        delivery_id = "b5426cab-66d8-48e6-bf30-9027fe4602b4"

        await agent._inject_resume_feedback(
            job_id=str(uuid4()),
            stateless_worker=True,
            graph_input=graph_input,
            thread_config={"configurable": {"thread_id": "job"}},
            checkpoint_values=checkpoint_values,
            feedback="continue from the durable request",
            feedback_reason="reviewer resumed",
            metadata={"queued_feedback_delivery_id": delivery_id},
        )

        expected_key = context_delivery_key(
            "feedback",
            "continue from the durable request",
            delivery_id=delivery_id,
            companion="reviewer resumed",
        )
        assert graph_input["delivered_feedback_keys"] == [expected_key]
        assert graph_input["should_stop"] is False
        assert graph_input["client_report_id"] is None
        assert graph_input["completion_report_payload"] is None
        assert route_entry(graph_input) == "init_workspace"
        assert len(graph_input["messages"]) == 1
        assert graph_input["client_report_id"] is None
        assert graph_input["completion_report_payload"] is None
        assert "continue from the durable request" in graph_input["messages"][0].content
        assert "reviewer resumed" in graph_input["messages"][0].content
        assert checkpoint_values["delivered_feedback_keys"] == [expected_key]
        graph.aupdate_state.assert_not_awaited()

        client = AsyncMock()
        client.ack_job_guidance.return_value = True
        acker = CheckpointSteeringAcker("job", client)
        assert await acker.reconcile_values(graph_input, checkpoint_id="cp-first")
        client.ack_job_guidance.assert_awaited_once_with(
            "job",
            guidance_ids=[],
            reply_keys=[],
            feedback_keys=[expected_key],
            delegation_keys=[],
            checkpoint_id="cp-first",
        )

    @pytest.mark.asyncio
    async def test_mid_loop_update_preserves_pending_frontier(self):
        graph = SimpleNamespace(
            aget_state=AsyncMock(
                return_value=SimpleNamespace(
                    values={"iteration": 8, "should_stop": False},
                    next=("audited_tools",),
                    metadata={"source": "loop"},
                )
            ),
            aupdate_state=AsyncMock(),
        )
        agent = UniversalAgent.__new__(UniversalAgent)
        agent._graph = graph

        terminal = await agent._arm_worker_batch(
            job_id=str(uuid4()),
            graph_input=None,
            thread_config={"configurable": {"thread_id": "job"}},
            target_wall_seconds=60,
            min_wall_seconds=0,
            iteration_cap=10,
        )

        graph.aupdate_state.assert_awaited_once()
        updates = graph.aupdate_state.await_args.args[1]
        assert graph.aupdate_state.await_args.kwargs == {}
        assert updates["worker_batch_start_iteration"] == 8
        assert updates["worker_batch_target_wall_seconds"] == 60
        assert terminal is None

    @pytest.mark.asyncio
    async def test_real_langgraph_atomic_arm_survives_crash_and_reclaim(self):
        app, ran, armed_targets = _build_worker_frontier_graph()
        initial = {
            "initialized": False,
            "messages": [],
            "should_stop": False,
            "goal_achieved": False,
            "iteration": 0,
        }

        # Dependency witness: on installed LangGraph, a second inferred-node
        # update consumes the START-selected restore task and runs zero nodes.
        bad_config = {"configurable": {"thread_id": "double-update"}}
        await app.ainvoke(initial, bad_config)
        await app.aupdate_state(
            bad_config,
            {
                "should_stop": False,
                "freeze_data": None,
                "error": None,
            },
            as_node="__start__",
        )
        assert app.get_state(bad_config).next == ("restore_todo_state",)
        await app.aupdate_state(
            bad_config,
            {"worker_batch_target_wall_seconds": 60.0},
        )
        assert app.get_state(bad_config).next == ()
        ran.clear()
        await app.ainvoke(None, bad_config)
        assert ran == []

        # Production path stages clear+nudge, then commits them with the arm in
        # one START update. A successor adopts that durable update byte-for-byte.
        config = {"configurable": {"thread_id": "atomic-update"}}
        await app.ainvoke(initial, config)
        before_prepare = await app.aget_state(config)
        agent = UniversalAgent.__new__(UniversalAgent)
        agent._graph = app
        resume_updates = {}

        resume_as_node = await agent._prepare_auto_continue_resume(
            job_id=str(uuid4()),
            thread_config=config,
            updated_metadata={"llm_outage": {"pending_shape_nudge": True}},
            stateless_worker=True,
            deferred_updates=resume_updates,
        )

        assert resume_as_node == "__start__"
        assert (await app.aget_state(config)).metadata[
            "step"
        ] == before_prepare.metadata["step"]

        terminal = await agent._arm_worker_batch(
            job_id=str(uuid4()),
            graph_input=None,
            thread_config=config,
            target_wall_seconds=60,
            min_wall_seconds=0,
            iteration_cap=10,
            resume_updates=resume_updates,
            resume_as_node=resume_as_node,
        )

        after_arm = await app.aget_state(config)
        assert terminal is None
        assert after_arm.metadata["step"] == before_prepare.metadata["step"] + 1
        assert after_arm.next == ("restore_todo_state",)
        assert after_arm.metadata["source"] == "update"
        assert after_arm.values["should_stop"] is False
        assert after_arm.values["freeze_data"] is None
        assert after_arm.values["worker_batch_target_wall_seconds"] == 60
        assert "Transport Notice" in after_arm.values["messages"][-1].content

        # Crash point: route+arm is durable but no graph node has run. The next
        # claim sees no auto-stop to restage and performs zero checkpoint writes.
        successor_updates = {}
        successor_as_node = await agent._prepare_auto_continue_resume(
            job_id=str(uuid4()),
            thread_config=config,
            updated_metadata={"llm_outage": {"pending_shape_nudge": True}},
            stateless_worker=True,
            deferred_updates=successor_updates,
        )
        assert successor_updates == {}
        assert successor_as_node is None
        successor_terminal = await agent._arm_worker_batch(
            job_id=str(uuid4()),
            graph_input=None,
            thread_config=config,
            target_wall_seconds=60,
            min_wall_seconds=0,
            iteration_cap=10,
            resume_updates=successor_updates,
            resume_as_node=successor_as_node,
        )
        adopted = await app.aget_state(config)
        assert successor_terminal is None
        assert adopted.metadata["step"] == after_arm.metadata["step"]
        assert adopted.next == ("restore_todo_state",)

        ran.clear()
        armed_targets.clear()
        await app.ainvoke(None, config)
        assert ran == [
            "restore_todo_state",
            "execute",
            "checkpoint_completion_report",
        ]
        assert armed_targets == [60.0]

    @pytest.mark.asyncio
    async def test_unarmed_update_frontier_fails_closed_without_second_update(self):
        app, _, _ = _build_worker_frontier_graph()
        config = {"configurable": {"thread_id": "unarmed-update"}}
        await app.ainvoke(
            {
                "initialized": False,
                "messages": [],
                "should_stop": False,
                "iteration": 0,
            },
            config,
        )
        await app.aupdate_state(
            config,
            {"should_stop": False, "freeze_data": None, "error": None},
            as_node="__start__",
        )
        before = await app.aget_state(config)
        assert before.metadata["source"] == "update"
        assert before.next == ("restore_todo_state",)

        agent = UniversalAgent.__new__(UniversalAgent)
        agent._graph = app
        terminal = await agent._arm_worker_batch(
            job_id=str(uuid4()),
            graph_input=None,
            thread_config=config,
            target_wall_seconds=60,
            min_wall_seconds=0,
            iteration_cap=10,
        )

        after = await app.aget_state(config)
        assert terminal["should_stop"] is True
        assert terminal["error"] == {
            "type": "worker_resume_frontier_unarmed",
            "recoverable": True,
            "message": "pending worker resume frontier has no valid durable batch arm",
        }
        assert after.metadata["step"] == before.metadata["step"]
        assert after.next == before.next

    @pytest.mark.asyncio
    async def test_feedback_crash_reclaim_arms_without_consuming_frontier(self):
        app, ran, armed_targets = _build_worker_frontier_graph()
        config = {"configurable": {"thread_id": "feedback-reclaim"}}
        await app.ainvoke(
            {
                "initialized": False,
                "messages": [],
                "should_stop": False,
                "iteration": 0,
            },
            config,
        )
        agent = UniversalAgent.__new__(UniversalAgent)
        agent._graph = app
        delivery_id = "b5426cab-66d-48e6-bf30-9027fe4602b4"
        checkpoint_values = dict((await app.aget_state(config)).values)
        resume_updates = {}

        resume_as_node = await agent._inject_resume_feedback(
            job_id=str(uuid4()),
            stateless_worker=True,
            graph_input=None,
            thread_config=config,
            checkpoint_values=checkpoint_values,
            feedback="continue from the durable request",
            feedback_reason="reviewer resumed",
            metadata={"queued_feedback_delivery_id": delivery_id},
            deferred_updates=resume_updates,
        )
        assert resume_as_node == "__start__"
        await agent._arm_worker_batch(
            job_id=str(uuid4()),
            graph_input=None,
            thread_config=config,
            target_wall_seconds=60,
            min_wall_seconds=0,
            iteration_cap=10,
            resume_updates=resume_updates,
            resume_as_node=resume_as_node,
        )
        routed = await app.aget_state(config)
        assert routed.next == ("restore_from_feedback",)
        assert routed.metadata["source"] == "update"

        # A successor sees the delivery receipt, suppresses reinjection, and
        # must still preserve the predecessor's already-selected task.
        successor_updates = {}
        await agent._inject_resume_feedback(
            job_id=str(uuid4()),
            stateless_worker=True,
            graph_input=None,
            thread_config=config,
            checkpoint_values=dict(routed.values),
            feedback="continue from the durable request",
            feedback_reason="reviewer resumed",
            metadata={"queued_feedback_delivery_id": delivery_id},
            deferred_updates=successor_updates,
        )
        assert successor_updates == {}

        terminal = await agent._arm_worker_batch(
            job_id=str(uuid4()),
            graph_input=None,
            thread_config=config,
            target_wall_seconds=60,
            min_wall_seconds=0,
            iteration_cap=10,
            resume_updates=successor_updates,
        )
        assert terminal is None
        assert (await app.aget_state(config)).metadata["step"] == routed.metadata[
            "step"
        ]
        assert (await app.aget_state(config)).next == ("restore_from_feedback",)

        ran.clear()
        armed_targets.clear()
        await app.ainvoke(None, config)
        assert ran == [
            "restore_from_feedback",
            "execute",
            "checkpoint_completion_report",
        ]
        assert armed_targets == [60.0]

    @pytest.mark.asyncio
    async def test_recoverable_end_reenters_through_start_and_clears_error(self):
        graph = SimpleNamespace(
            aget_state=AsyncMock(
                return_value=SimpleNamespace(
                    values={
                        "iteration": 8,
                        "should_stop": True,
                        "error": {"recoverable": True},
                        "freeze_data": None,
                        "client_report_id": "11111111-1111-4111-8111-111111111111",
                        "completion_report_payload": {
                            "should_stop": True,
                            "goal_achieved": False,
                            "error": {"recoverable": True},
                            "freeze_data": None,
                        },
                    },
                    next=(),
                )
            ),
            aupdate_state=AsyncMock(),
        )
        agent = UniversalAgent.__new__(UniversalAgent)
        agent._graph = graph

        terminal = await agent._arm_worker_batch(
            job_id=str(uuid4()),
            graph_input=None,
            thread_config={"configurable": {"thread_id": "job"}},
            target_wall_seconds=60,
            min_wall_seconds=0,
            iteration_cap=10,
        )

        updates = graph.aupdate_state.await_args.args[1]
        assert graph.aupdate_state.await_args.kwargs == {"as_node": "__start__"}
        assert updates["error"] is None
        assert updates["should_stop"] is False
        assert updates["client_report_id"] is None
        assert updates["completion_report_payload"] is None
        assert terminal is None

    @pytest.mark.asyncio
    async def test_terminal_end_is_not_rearmed_or_reentered(self):
        graph = SimpleNamespace(
            aget_state=AsyncMock(
                return_value=SimpleNamespace(
                    values={
                        "iteration": 8,
                        "should_stop": True,
                        "goal_achieved": True,
                        "error": None,
                        "client_report_id": "11111111-1111-4111-8111-111111111111",
                        "completion_report_payload": {
                            "should_stop": True,
                            "goal_achieved": True,
                            "error": None,
                            "freeze_data": None,
                        },
                    },
                    next=(),
                )
            ),
            aupdate_state=AsyncMock(),
        )
        agent = UniversalAgent.__new__(UniversalAgent)
        agent._graph = graph

        terminal = await agent._arm_worker_batch(
            job_id=str(uuid4()),
            graph_input=None,
            thread_config={"configurable": {"thread_id": "job"}},
            target_wall_seconds=60,
            min_wall_seconds=0,
            iteration_cap=10,
        )

        graph.aupdate_state.assert_not_awaited()
        assert terminal["goal_achieved"] is True
        assert terminal["client_report_id"] == ("11111111-1111-4111-8111-111111111111")

    @pytest.mark.asyncio
    async def test_exhausted_probe_preserves_terminal_end_without_graph_update(self):
        values = {
            "iteration": 8,
            "should_stop": True,
            "goal_achieved": True,
            "freeze_data": None,
            "error": None,
        }
        graph = SimpleNamespace(
            aget_state=AsyncMock(return_value=SimpleNamespace(values=values, next=())),
            aupdate_state=AsyncMock(),
        )
        agent = UniversalAgent.__new__(UniversalAgent)
        agent._graph = graph

        terminal = await agent._arm_worker_batch(
            job_id=str(uuid4()),
            graph_input=None,
            thread_config={"configurable": {"thread_id": "job"}},
            target_wall_seconds=60,
            min_wall_seconds=0,
            iteration_cap=10,
            retry_exhausted=True,
        )

        graph.aupdate_state.assert_not_awaited()
        assert terminal == values

    @pytest.mark.asyncio
    async def test_exhausted_probe_suppresses_recoverable_end_reentry(self):
        graph = SimpleNamespace(
            aget_state=AsyncMock(
                return_value=SimpleNamespace(
                    values={
                        "iteration": 8,
                        "should_stop": True,
                        "freeze_data": {"freeze_type": "llm_unavailable"},
                        "error": {"recoverable": True},
                    },
                    next=(),
                )
            ),
            aupdate_state=AsyncMock(),
        )
        agent = UniversalAgent.__new__(UniversalAgent)
        agent._graph = graph

        terminal = await agent._arm_worker_batch(
            job_id=str(uuid4()),
            graph_input=None,
            thread_config={"configurable": {"thread_id": "job"}},
            target_wall_seconds=60,
            min_wall_seconds=0,
            iteration_cap=10,
            retry_exhausted=True,
        )

        graph.aupdate_state.assert_not_awaited()
        assert terminal["error"]["type"] == "worker_retry_budget_exhausted"

    @pytest.mark.asyncio
    async def test_explicit_resume_generation_reenters_human_end_through_start(self):
        graph = SimpleNamespace(
            aget_state=AsyncMock(
                return_value=SimpleNamespace(
                    values={
                        "iteration": 8,
                        "should_stop": True,
                        "goal_achieved": False,
                        "freeze_data": {"freeze_type": "phase_boundary"},
                        "error": None,
                    },
                    next=(),
                )
            ),
            aupdate_state=AsyncMock(),
        )
        agent = UniversalAgent.__new__(UniversalAgent)
        agent._graph = graph

        terminal = await agent._arm_worker_batch(
            job_id=str(uuid4()),
            graph_input=None,
            thread_config={"configurable": {"thread_id": "job"}},
            target_wall_seconds=60,
            min_wall_seconds=0,
            iteration_cap=10,
            resume_id="resume-generation-2",
        )

        updates = graph.aupdate_state.await_args.args[1]
        assert graph.aupdate_state.await_args.kwargs == {"as_node": "__start__"}
        assert updates["should_stop"] is False
        assert updates["freeze_data"] is None
        assert updates["worker_resume_id"] == "resume-generation-2"
        assert terminal is None

    @pytest.mark.asyncio
    async def test_applied_resume_generation_does_not_reopen_ambiguous_end(self):
        values = {
            "iteration": 9,
            "should_stop": True,
            "goal_achieved": False,
            "freeze_data": {"freeze_type": "phase_boundary"},
            "error": None,
            "worker_resume_id": "resume-generation-2",
        }
        graph = SimpleNamespace(
            aget_state=AsyncMock(return_value=SimpleNamespace(values=values, next=())),
            aupdate_state=AsyncMock(),
        )
        agent = UniversalAgent.__new__(UniversalAgent)
        agent._graph = graph

        terminal = await agent._arm_worker_batch(
            job_id=str(uuid4()),
            graph_input=None,
            thread_config={"configurable": {"thread_id": "job"}},
            target_wall_seconds=60,
            min_wall_seconds=0,
            iteration_cap=10,
            resume_id="resume-generation-2",
        )

        graph.aupdate_state.assert_not_awaited()
        assert terminal == values


def test_worker_environment_is_restored_between_claims(monkeypatch):
    agent = UniversalAgent.__new__(UniversalAgent)
    agent._worker_env_restore = {}
    monkeypatch.setenv("TENANT_ONLY_KEY", "pod-baseline")
    monkeypatch.delenv("PGPASSWORD", raising=False)

    agent._capture_worker_environment(
        {
            "resolved_config": {"agent": {"env_keys": {"TENANT_ONLY_KEY": "secret"}}},
            "datasources": [{"type": "postgresql"}],
        }
    )
    os.environ["TENANT_ONLY_KEY"] = "secret"
    os.environ["PGPASSWORD"] = "database-secret"

    agent._restore_worker_environment()

    assert os.environ["TENANT_ONLY_KEY"] == "pod-baseline"
    assert "PGPASSWORD" not in os.environ
