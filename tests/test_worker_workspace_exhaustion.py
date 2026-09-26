"""Typed workspace causes survive the worker budget's reporting envelope."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from agent.agent import UniversalAgent
from agent.api.turn_executor import StatelessTurnExecutor
from agent.api.orchestrator_client import ClaimBundleError
from orchestrator.services.completion import (
    is_container_worker_workspace_exhaustion,
    should_persist_completion_freeze,
    should_reset_recovery_counter,
)
from shared.worker_errors import worker_workspace_exhaustion_cause
from shared.workspace_recovery import WorkspaceRecoveryCode
from shared.runtime.core.workspace_backend import WorkspaceUnavailableError
from tests.test_stateless_worker_runtime import (
    _claim,
    _install,
)

from tests import test_stateless_worker_runtime as worker_helpers

worker_runtime = worker_helpers.worker_runtime


@pytest.mark.asyncio
async def test_last_container_attempt_preserves_typed_workspace_cause(
    worker_runtime, monkeypatch
):
    claim = _claim(input_seq=4, prior="processing", attempts=5, max_attempts=5)
    cause = {
        "type": "workspace_unavailable",
        "recoverable": True,
        "message": "transport failed after a command was admitted",
    }
    final = {"should_stop": True, "goal_achieved": False, "error": cause}
    executor, agent, client, _, rotate, complete, release = _install(
        monkeypatch, claim, final
    )
    client.backend = "sandbox"
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")

    await executor._serve_worker_claim(claim)

    reported = client.report_completion.await_args.args[1]
    assert reported["error"]["type"] == "worker_retry_exhausted"
    assert reported["error"]["cause"] == cause
    assert final["error"] == cause
    complete.assert_awaited_once()
    release.assert_not_awaited()
    rotate.assert_not_awaited()
    assert len(agent.process_calls) == 1


@pytest.mark.asyncio
async def test_over_budget_checkpoint_probe_preserves_workspace_cause():
    cause = {"type": "workspace_unavailable", "recoverable": True}
    graph = SimpleNamespace(
        aget_state=AsyncMock(
            return_value=SimpleNamespace(
                values={"should_stop": True, "error": cause}, next=()
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
    reported = StatelessTurnExecutor._worker_retry_exhausted_state(
        terminal, attempts=6, max_attempts=5
    )
    assert reported["error"]["cause"] == cause
    graph.aupdate_state.assert_not_awaited()


def test_workspace_exhaustion_does_not_replace_freeze_or_reset_recovery_counter():
    report = {
        "error": {
            "type": "worker_retry_exhausted",
            "recoverable": False,
            "cause": {"type": "workspace_unavailable", "recoverable": True},
        },
        "freeze_data": {"freeze_type": "worker_retry_exhausted"},
    }
    job = {
        "execution_lane": "stateless",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": {"workspace_container": {"provisioner": "k8s"}},
    }
    assert not should_persist_completion_freeze(report, job=job)
    assert not should_reset_recovery_counter(
        {"recovery_attempts": 2}, report["error"], job=job
    )


@pytest.mark.parametrize(
    "backend,provisioner,lane",
    [
        ("vm", "k8s", "stateless"),
        ("virtual", "k8s", "stateless"),
        ("sandbox", "docker", "stateless"),
        ("sandbox", "k8s", "pinned"),
    ],
)
def test_other_backends_and_lanes_keep_their_completion_contract(
    backend, provisioner, lane
):
    error = {
        "type": "worker_retry_exhausted",
        "cause": {"type": "workspace_unavailable"},
    }
    job = {
        "execution_lane": lane,
        "config_override": {"workspace": {"backend": backend}},
        "context": {"workspace_container": {"provisioner": provisioner}},
    }
    assert not is_container_worker_workspace_exhaustion(job, error)
    assert should_persist_completion_freeze({"error": error}, job=job)
    assert should_reset_recovery_counter({"recovery_attempts": 2}, error, job=job)


@pytest.mark.parametrize(
    "error",
    [
        None,
        [],
        {"type": []},
        {"type": "worker_retry_exhausted", "cause": []},
        {"type": "worker_retry_exhausted", "cause": {"type": []}},
        {"type": "worker_retry_exhausted", "message": "workspace_unavailable"},
        {"type": "worker_retry_exhausted", "cause": {"type": "llm_unavailable"}},
        {
            "type": "worker_retry_exhausted",
            "cause": {
                "type": "worker_retry_exhausted",
                "cause": {"type": "workspace_unavailable"},
            },
        },
    ],
)
def test_exhaustion_cause_requires_a_known_flat_typed_cause(error):
    assert worker_workspace_exhaustion_cause(error) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["typed_bundle", "typed_transport", "untyped_text"])
async def test_last_setup_failure_preserves_only_typed_workspace_cause(
    worker_runtime, monkeypatch, kind
):
    claim = _claim(input_seq=4, prior="processing", attempts=5, max_attempts=5)
    executor, agent, client, _, _, complete, release = _install(
        monkeypatch, claim, {"should_stop": True}
    )
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")
    failures = {
        "typed_bundle": ClaimBundleError(
            409, code=WorkspaceRecoveryCode.RUNTIME_NOT_READY
        ),
        "typed_transport": WorkspaceUnavailableError("lost transport"),
        "untyped_text": RuntimeError(
            "workspace_unavailable workspace_runtime_not_ready"
        ),
    }
    client.get_claim_bundle = AsyncMock(side_effect=failures[kind])
    await executor._serve_worker_claim(claim)
    reported = client.report_completion.await_args.args[1]
    cause = worker_workspace_exhaustion_cause(reported["error"])
    if kind == "untyped_text":
        assert cause is None
    else:
        assert cause["type"] == (
            "workspace_runtime_not_ready"
            if kind == "typed_bundle"
            else "workspace_unavailable"
        )
    assert agent.process_calls == []
    complete.assert_awaited_once()
    release.assert_not_awaited()
