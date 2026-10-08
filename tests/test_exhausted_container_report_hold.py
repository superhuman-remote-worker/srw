"""Native exhausted workspace reports retain the restrictive worker hold."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent.agent import UniversalAgent
from agent.api.orchestrator_client import CompletionNonTerminalReportError
from agent.api.turn_executor import StatelessTurnExecutor
from shared.runtime.core.workspace_backend import WorkspaceUnavailableError
from shared.workspace_recovery import WorkspaceRecoveryCode
from tests import test_stateless_worker_runtime as worker
from tests.test_container_workspace_unknown_outcome import configure_bundle

worker_runtime = worker.worker_runtime


def exhausted(error=None):
    return StatelessTurnExecutor._worker_retry_exhausted_state(
        {
            "should_stop": True,
            "goal_achieved": False,
            "error": error or {"type": "workspace_unavailable", "recoverable": True},
        },
        attempts=6,
        max_attempts=5,
    )


def install(monkeypatch, claim, final, *, report_result=False):
    runtime = worker._install(monkeypatch, claim, final, report_result=report_result)
    executor, _, client, *_ = runtime
    configure_bundle(client)
    executor._completion_commands_enabled = True
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")
    hold = AsyncMock(return_value="held")
    monkeypatch.setattr(
        worker.turn_executor, "hold_failed_container_worker_report", hold
    )
    return (*runtime, hold)


@pytest.mark.asyncio
@pytest.mark.parametrize("report", ["false", "exception"])
@pytest.mark.parametrize(
    "path", ["direct", "driver_at_cap", "exhausted_checkpoint", "checkpoint_budget"]
)
async def test_typed_container_report_loss_holds_all_native_envelopes(
    worker_runtime, monkeypatch, path, report
):
    claim = worker._claim(
        prior="processing",
        attempts=6 if path in {"exhausted_checkpoint", "checkpoint_budget"} else 5,
        max_attempts=5,
    )
    final = {
        "should_stop": True,
        "goal_achieved": False,
        "error": {"type": "workspace_unavailable", "recoverable": True},
    }
    graph = None
    if path == "exhausted_checkpoint":
        final = StatelessTurnExecutor._worker_retry_exhausted_state(
            final, attempts=6, max_attempts=5
        )
    elif path == "checkpoint_budget":
        graph = SimpleNamespace(
            aget_state=AsyncMock(return_value=SimpleNamespace(values=final, next=())),
            aupdate_state=AsyncMock(),
        )
        checkpoint_agent = UniversalAgent.__new__(UniversalAgent)
        checkpoint_agent._graph = graph
        final = await checkpoint_agent._arm_worker_batch(
            job_id=str(claim.unit_id),
            graph_input=None,
            thread_config={"configurable": {"thread_id": str(claim.unit_id)}},
            target_wall_seconds=60,
            min_wall_seconds=0,
            iteration_cap=10,
            retry_exhausted=True,
        )
        assert final["error"]["type"] == "worker_retry_budget_exhausted"
    executor, agent, client, _, rotate, complete, release, hold = install(
        monkeypatch, claim, final
    )
    if path == "driver_at_cap":
        agent.process_job = AsyncMock(
            side_effect=WorkspaceUnavailableError("typed transport failed after bundle")
        )
    if report == "exception":
        client.report_completion.side_effect = TimeoutError("completion response lost")

    await executor._serve_worker_claim(claim)

    client.report_completion.assert_awaited_once()
    reported = client.report_completion.await_args.args[1]
    if path != "direct":
        assert reported["error"]["type"] == "worker_retry_exhausted"
        assert reported["error"]["cause"]["type"] == "workspace_unavailable"
    hold.assert_awaited_once_with(
        executor._db, unit_id=claim.unit_id, lease_token=claim.lease_token
    )
    release.assert_not_awaited()
    rotate.assert_not_awaited()
    complete.assert_not_awaited()
    if graph is not None:
        graph.aupdate_state.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("code", list(WorkspaceRecoveryCode))
async def test_known_typed_exhaustion_cause_uses_existing_hold(
    worker_runtime, monkeypatch, code
):
    claim = worker._claim(prior="processing", attempts=6, max_attempts=5)
    executor, _, client, _, _, _, release, hold = install(
        monkeypatch, claim, exhausted({"type": code.value})
    )
    await executor._serve_worker_claim(claim)
    client.report_completion.assert_awaited_once()
    hold.assert_awaited_once()
    release.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        {"type": "worker_retry_exhausted"},
        {"type": "worker_retry_exhausted", "cause": "workspace_unavailable"},
        {"type": "worker_retry_exhausted", "cause": {"type": "generic_error"}},
        {
            "type": "worker_retry_exhausted",
            "cause": {"message": "workspace_unavailable"},
        },
        {
            "type": "worker_retry_exhausted",
            "cause": {
                "type": "worker_retry_exhausted",
                "cause": {"type": "workspace_unavailable"},
            },
        },
        {
            "type": "worker_driver_error",
            "cause": {"type": "workspace_unavailable"},
        },
        {"type": "tool_outcome_unknown"},
    ],
)
async def test_untyped_nested_and_other_top_level_errors_do_not_gain_hold(
    worker_runtime, monkeypatch, error
):
    claim = worker._claim(prior="processing", attempts=5, max_attempts=5)
    final = {"should_stop": True, "goal_achieved": False, "error": deepcopy(error)}
    executor, _, client, _, _, _, release, hold = install(monkeypatch, claim, final)
    await executor._serve_worker_claim(claim)
    assert client.report_completion.await_args.args[1]["error"] == error
    hold.assert_not_awaited()
    release.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend,provisioner,commands,vm_recovery",
    [
        ("vm", "k8s", True, True),
        ("sandbox", "docker", True, False),
        ("sandbox", None, True, False),
        ("sandbox", "k8s", False, False),
    ],
)
async def test_exhaustion_hold_preserves_existing_authority_gates(
    worker_runtime, monkeypatch, backend, provisioner, commands, vm_recovery
):
    claim = worker._claim(prior="processing", attempts=6, max_attempts=5)
    executor, _, client, _, _, _, release, hold = install(
        monkeypatch, claim, exhausted()
    )
    client.backend = backend
    configure_bundle(client, provisioner=provisioner)
    executor._completion_commands_enabled = commands
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", str(vm_recovery).lower())
    await executor._serve_worker_claim(claim)
    client.report_completion.assert_awaited_once()
    hold.assert_not_awaited()
    release.assert_awaited_once()


@pytest.mark.asyncio
async def test_vm_recovery_off_typed_exhaustion_report_loss_holds_worker_claim(
    worker_runtime, monkeypatch
):
    claim = worker._claim(prior="processing", attempts=6, max_attempts=5)
    executor, agent, client, _, rotate, complete, release, hold = install(
        monkeypatch, claim, exhausted()
    )
    client.backend = "vm"
    configure_bundle(client, provisioner=None)
    client.report_completion.side_effect = TimeoutError("completion response lost")

    await executor._serve_worker_claim(claim)

    client.report_completion.assert_awaited_once()
    reported = client.report_completion.await_args.args[1]
    assert reported["error"]["type"] == "worker_retry_exhausted"
    assert reported["error"]["cause"]["type"] == "workspace_unavailable"
    assert executor._worker_workspace_backend == "vm"
    hold.assert_awaited_once_with(
        executor._db, unit_id=claim.unit_id, lease_token=claim.lease_token
    )
    assert agent.cleanup_calls and all(agent.cleanup_calls)
    release.assert_not_awaited()
    rotate.assert_not_awaited()
    complete.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("goal", [None, True])
async def test_goal_must_be_exact_false_for_exhaustion_hold(
    worker_runtime, monkeypatch, goal
):
    claim = worker._claim(prior="processing", attempts=6, max_attempts=5)
    final = exhausted()
    final["goal_achieved"] = goal
    executor, _, client, _, _, _, release, hold = install(monkeypatch, claim, final)
    await executor._serve_worker_claim(claim)
    client.report_completion.assert_awaited_once()
    hold.assert_not_awaited()
    release.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_accepted_exhaustion_report_never_adds_unknown_outcome_hold(
    worker_runtime, monkeypatch, lost_reply
):
    claim = worker._claim(prior="processing", attempts=6, max_attempts=5)
    executor, _, client, renew, _, _, release, hold = install(
        monkeypatch, claim, exhausted(), report_result=not lost_reply
    )
    if lost_reply:
        renew.side_effect = [worker._renewal(), None]
        monkeypatch.setattr(
            worker.turn_executor,
            "get_worker_completion_acceptance",
            AsyncMock(
                return_value=worker._acceptance(
                    job_status="paused", outcome={"new_status": "paused"}
                )
            ),
        )
    await executor._serve_worker_claim(claim)
    client.report_completion.assert_awaited_once()
    hold.assert_not_awaited()
    release.assert_not_awaited()
    assert executor._worker_container_report_uncertain is False


@pytest.mark.asyncio
async def test_definitive_prewrite_refusal_clears_exhaustion_report_uncertainty(
    worker_runtime, monkeypatch
):
    claim = worker._claim(prior="processing", attempts=6, max_attempts=5)
    executor, _, client, _, _, _, release, hold = install(
        monkeypatch, claim, exhausted()
    )
    client.report_completion.side_effect = CompletionNonTerminalReportError(
        "stateless completion requires should_stop=true"
    )
    await executor._serve_worker_claim(claim)
    hold.assert_not_awaited()
    release.assert_awaited_once()
    assert executor._worker_container_report_uncertain is False
    assert executor._worker_terminal_report_generation is None


@pytest.mark.asyncio
async def test_failure_before_valid_bundle_does_not_inherit_container_authority(
    worker_runtime, monkeypatch
):
    claim = worker._claim(prior="processing", attempts=5, max_attempts=5)
    executor, agent, client, _, _, _, release, hold = install(
        monkeypatch, claim, exhausted()
    )
    client.get_claim_bundle = AsyncMock(
        side_effect=WorkspaceUnavailableError("failure before a valid bundle")
    )
    await executor._serve_worker_claim(claim)
    assert not agent.process_calls
    assert executor._worker_workspace_backend is None
    assert executor._worker_workspace_provisioner is None
    hold.assert_not_awaited()
    release.assert_awaited_once()
