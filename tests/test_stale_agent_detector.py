"""Unit tests for stale-agent background sweeps."""

import asyncio
import logging
from contextlib import asynccontextmanager

from tests._workspace_recovery_fakes import idle_recovery_store

import pytest
from fastapi import HTTPException
from unittest.mock import AsyncMock, MagicMock, patch

import orchestrator.main as main
from orchestrator.database.postgres import (
    LeaseRecoveryBatch,
    LeaseRecoveryCircuitTrip,
    OrphanRecoveryBatch,
    RecoveredJob,
)
from orchestrator.services.pinned_retirement import PinnedRetirementOperations


def _mock_db(shutdown_event: asyncio.Event, stall_return: int = 0):
    """Build a db mock matching the call surface of stale_agent_detector."""
    db = AsyncMock()

    def _stop(event):
        # Ensure the loop exits immediately after one full sweep.
        event.set()
        return []

    db.mark_stale_agents_offline = AsyncMock(
        side_effect=lambda *args, **kwargs: _stop(shutdown_event)
    )
    db.mark_stuck_working_agents_ready = AsyncMock(return_value=0)
    db.mark_stalled_working_agents_by_graph_progress = AsyncMock(
        return_value=stall_return
    )
    db.mark_stuck_session_agents_ready = AsyncMock(return_value=0)
    db.reap_orphaned_session_agents = AsyncMock(return_value=[])
    db.list_retryable_thread_attach_abort_successors = AsyncMock(return_value=[])
    db.mark_orphaned_threads_ended = AsyncMock(return_value=[])
    db.mark_orphaned_threads_suspended = AsyncMock(return_value=[])
    db.abort_stale_pinned_retirement_preflights = AsyncMock(return_value=[])
    db.list_retryable_pinned_retirements = AsyncMock(return_value=[])
    db.recover_orphaned_jobs = AsyncMock(return_value=OrphanRecoveryBatch())
    db.recover_expired_lease_jobs = AsyncMock(return_value=LeaseRecoveryBatch())
    db.gc_offline_agents = AsyncMock(return_value=0)
    return db


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("workspace_status", "workspace_authority", "expected_recovery"),
    [
        ("ready", "exact_absent", True),
        ("suspending", "exact_absent", True),
        ("suspending", "exact_live", False),
    ],
)
async def test_permanent_retirement_recovers_from_exact_absent_sandbox_pod(
    workspace_status,
    workspace_authority,
    expected_recovery,
):
    """A deleted U1 is process-zero; recovery must not require SSH to U1."""

    thread_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
    generation = "11111111-1111-4111-8111-111111111111"
    token = "22222222-2222-4222-8222-222222222222"
    agent_id = "33333333-3333-4333-8333-333333333333"
    attach_token = "44444444-4444-4444-8444-444444444444"
    workspace_generation = "55555555-5555-4555-8555-555555555555"
    workspace_runtime = "66666666-6666-4666-8666-666666666666"
    context = {
        "thread_id": thread_id,
        "generation": generation,
        "settle_status": "ended",
        "runtime_authority_exposed": True,
        "agent_id": agent_id,
        "runtime_attach_token": attach_token,
        "agent": {"hostname": "agent-retired", "pod_uid": "agent-pod-uid"},
        "agent_pod": {
            "pod_name": "agent-retired",
            "pod_uid": "agent-pod-uid",
            "namespace": "agents-a",
            "protection_protocol": "finalizer_v1",
        },
        "workspace_backend": "sandbox",
        "workspace_container": {
            "status": workspace_status,
            "pod_ip": "10.42.0.8",
            "port": 30022,
            "_canvas_workspace_generation": workspace_generation,
            "_runtime_incarnation": workspace_runtime,
        },
        "workspace_binding": {
            "kind": "remote",
            "generation": workspace_generation,
            "ssh_host_key_fingerprint": "SHA256:test",
        },
    }
    retirement = {
        "generation": generation,
        "token": token,
        "permanent": True,
        "context": context,
    }
    current = {
        "runtime_generation": generation,
        "runtime_retirement_token": token,
        "runtime_retirement_local_quiescence": None,
    }
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value=current)
    db.acknowledge_pinned_thread_local_quiescence = AsyncMock(
        return_value={"version": 1}
    )

    @asynccontextmanager
    async def lifecycle_lock(_thread_id):
        yield True

    db.try_thread_advisory_lock = MagicMock(side_effect=lifecycle_lock)
    agent_provisioner = MagicMock(is_available=True)
    agent_provisioner.delete_agent_pod_exact = AsyncMock(return_value=True)
    agent_provisioner.release_agent_pod_finalizer_exact = AsyncMock(return_value=True)
    agent_provisioner.agent_pod_authority = AsyncMock(
        side_effect=["exact_terminal", "exact_absent"]
    )
    container_provisioner = MagicMock(is_available=True)
    container_provisioner.workspace_pod_authority = AsyncMock(
        return_value=workspace_authority
    )

    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "agent_provisioner", agent_provisioner),
        patch.object(main, "container_provisioner", container_provisioner),
    ):
        assert (
            await main._recover_captured_sandbox_process_zero(retirement)
            is expected_recovery
        )

    if expected_recovery:
        db.acknowledge_pinned_thread_local_quiescence.assert_awaited_once_with(
            thread_id,
            expected_runtime_generation=generation,
            expected_retirement_token=token,
            expected_agent_id=agent_id,
            expected_attach_token=attach_token,
            expected_settle_status="ended",
            expected_quiescence_protocol="sandbox_actuator_zero_v1",
            expected_workspace_generation=workspace_generation,
            expected_workspace_runtime_incarnation=workspace_runtime,
            quiescence_actor="orchestrator",
        )
    else:
        db.acknowledge_pinned_thread_local_quiescence.assert_not_awaited()


@pytest.mark.asyncio
async def test_soft_retirement_recovers_never_delivered_warm_runtime():
    thread_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
    generation = "11111111-1111-4111-8111-111111111111"
    token = "22222222-2222-4222-8222-222222222222"
    agent_id = "33333333-3333-4333-8333-333333333333"
    attach_token = "44444444-4444-4444-8444-444444444444"
    context = {
        "thread_id": thread_id,
        "generation": generation,
        "entry_status": "created",
        "settle_status": "ended",
        "runtime_authority_exposed": True,
        "agent_id": agent_id,
        "runtime_attach_token": attach_token,
        "agent": {"hostname": "warm-agent", "pod_uid": "warm-pod-uid"},
        "agent_pod": {
            "pod_name": "warm-agent",
            "pod_uid": "warm-pod-uid",
            "namespace": "agents-a",
            "protection_protocol": "finalizer_v1",
            "warm_binding_protection": "55555555-5555-4555-8555-555555555555",
        },
        "workspace_backend": "virtual",
        "workspace_container": None,
        "workspace_binding": {
            "generation": "66666666-6666-4666-8666-666666666666",
            "kind": "virtual",
            "backing_id": f"rclone:{'a' * 64}",
            "ssh_host_key_fingerprint": None,
        },
    }
    retirement = {
        "generation": generation,
        "token": token,
        "permanent": False,
        "context": context,
    }
    current = {
        "status": "created",
        "runtime_generation": generation,
        "runtime_retirement_token": token,
        "runtime_retirement_local_quiescence": None,
    }
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value=current)
    db.acknowledge_pinned_thread_local_quiescence = AsyncMock(
        return_value={"version": 1}
    )

    @asynccontextmanager
    async def lifecycle_lock(_thread_id):
        yield True

    db.try_thread_advisory_lock = MagicMock(side_effect=lifecycle_lock)
    provisioner = MagicMock(is_available=True)
    provisioner.delete_agent_pod_exact = AsyncMock(return_value=True)
    provisioner.release_agent_pod_finalizer_exact = AsyncMock(return_value=True)
    provisioner.agent_pod_authority = AsyncMock(
        side_effect=["exact_terminal", "exact_absent"]
    )

    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "agent_provisioner", provisioner),
    ):
        assert await main._recover_captured_sandbox_process_zero(retirement)

    db.acknowledge_pinned_thread_local_quiescence.assert_awaited_once_with(
        thread_id,
        expected_runtime_generation=generation,
        expected_retirement_token=token,
        expected_agent_id=agent_id,
        expected_attach_token=attach_token,
        expected_settle_status="ended",
        expected_quiescence_protocol="agent_runtime_zero_v1",
        expected_workspace_generation=None,
        expected_workspace_runtime_incarnation=None,
        quiescence_actor="orchestrator",
        expected_agent_pod_uid="warm-pod-uid",
        require_zero_admission=True,
    )


@pytest.mark.parametrize(
    ("context_patch", "workspace_patch", "binding_patch"),
    [
        ({"workspace_backend": "sandbox"}, {}, {}),
        ({"vm": {"status": "ready"}}, {}, {}),
        ({}, {"status": "ready"}, {}),
        ({}, {}, {"backing_id": "rclone:not-a-digest"}),
        ({}, {}, {"ssh_host_key_fingerprint": "SHA256:unexpected"}),
        ({}, {}, {"unexpected": "field"}),
    ],
)
def test_virtual_binding_agent_zero_requires_exact_nonphysical_shape(
    context_patch, workspace_patch, binding_patch
):
    context = {"workspace_backend": "virtual", **context_patch}
    workspace = {**workspace_patch}
    binding = {
        "generation": "66666666-6666-4666-8666-666666666666",
        "kind": "virtual",
        "backing_id": f"rclone:{'a' * 64}",
        "ssh_host_key_fingerprint": None,
        **binding_patch,
    }
    assert not main._pinned_retirement_operations().captured_virtual_binding_agent_zero_only(
        context, workspace, binding
    )


def test_virtual_binding_agent_zero_accepts_exact_project_cloud_shape():
    assert (
        main._pinned_retirement_operations().captured_virtual_binding_agent_zero_only(
            {"workspace_backend": "virtual"},
            {},
            {
                "generation": "66666666-6666-4666-8666-666666666666",
                "kind": "virtual",
                "backing_id": f"rclone:{'a' * 64}",
                "ssh_host_key_fingerprint": None,
            },
        )
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("first_authority", ["exact_terminal", "exact_absent"])
async def test_captured_agent_stop_retries_after_exact_pod_disappeared(
    monkeypatch, first_authority
):
    retirement = {
        "context": {
            "agent_pod": {
                "pod_name": "captured-agent",
                "pod_uid": "captured-uid",
                "namespace": "captured-namespace",
                "protection_protocol": "finalizer_v1",
            }
        }
    }
    provisioner = MagicMock(is_available=True)
    provisioner.delete_agent_pod_exact = AsyncMock(return_value=True)
    provisioner.agent_pod_authority = AsyncMock(
        side_effect=[first_authority, "exact_absent"]
    )
    provisioner.release_agent_pod_finalizer_exact = AsyncMock(return_value=True)

    async def immediate_observation(
        _self, pod_name, pod_uid, *, namespace, allowed, **_kwargs
    ):
        state = await provisioner.agent_pod_authority(
            pod_name, expected_pod_uid=pod_uid, namespace=namespace
        )
        return state if state in allowed else None

    monkeypatch.setattr(main, "agent_provisioner", provisioner)
    monkeypatch.setattr(
        PinnedRetirementOperations,
        "_wait_for_captured_agent_pod_retired",
        immediate_observation,
    )
    await main._pinned_retirement_operations().stop_captured_retirement_agent(
        retirement
    )
    provisioner.delete_agent_pod_exact.assert_awaited_once_with(
        "captured-agent",
        expected_pod_uid="captured-uid",
        namespace="captured-namespace",
    )
    if first_authority == "exact_terminal":
        provisioner.release_agent_pod_finalizer_exact.assert_awaited_once_with(
            "captured-agent",
            expected_pod_uid="captured-uid",
            namespace="captured-namespace",
            terminal_required=True,
        )
    else:
        provisioner.release_agent_pod_finalizer_exact.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("authority", ["exact_live", "unknown", "replacement"])
async def test_captured_agent_stop_refuses_unproven_initial_absence(
    monkeypatch, authority
):
    retirement = {
        "context": {
            "agent_pod": {
                "pod_name": "captured-agent",
                "pod_uid": "captured-uid",
                "namespace": "captured-namespace",
                "protection_protocol": "finalizer_v1",
            }
        }
    }
    provisioner = MagicMock(is_available=True)
    provisioner.delete_agent_pod_exact = AsyncMock(return_value=True)
    provisioner.agent_pod_authority = AsyncMock(return_value=authority)
    provisioner.release_agent_pod_finalizer_exact = AsyncMock(return_value=True)

    async def immediate_observation(
        _self, pod_name, pod_uid, *, namespace, allowed, **_kwargs
    ):
        state = await provisioner.agent_pod_authority(
            pod_name, expected_pod_uid=pod_uid, namespace=namespace
        )
        return state if state in allowed else None

    monkeypatch.setattr(main, "agent_provisioner", provisioner)
    monkeypatch.setattr(
        PinnedRetirementOperations,
        "_wait_for_captured_agent_pod_retired",
        immediate_observation,
    )
    with pytest.raises(RuntimeError, match="exact agent Pod termination is retryable"):
        await main._pinned_retirement_operations().stop_captured_retirement_agent(
            retirement
        )
    provisioner.release_agent_pod_finalizer_exact.assert_not_awaited()


@pytest.mark.asyncio
async def test_detector_retries_durable_attach_abort_after_request_task_failure():
    """The leader/startup sweep owns G2 even if the request-local task dies."""

    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    candidate = {
        "thread_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1",
        "retired_runtime_generation": "11111111-1111-4111-8111-111111111111",
        "retired_attach_token": "22222222-2222-4222-8222-222222222222",
        "retired_agent_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2",
        "successor_generation": "55555555-5555-4555-8555-555555555555",
        "quiescence_protocol": "agent_attach_not_started_v1",
        "workspace_generation": None,
        "workspace_runtime_incarnation": None,
    }
    db.list_retryable_thread_attach_abort_successors = AsyncMock(
        return_value=[candidate]
    )
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=candidate)

    @asynccontextmanager
    async def acquire():
        yield conn

    db.acquire = MagicMock(side_effect=acquire)
    reconcile = AsyncMock(side_effect=[RuntimeError("transient"), True])
    main._attach_abort_successor_tasks.clear()
    original_schedule = main._schedule_attach_abort_successor

    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "_reconcile_attach_abort_successor", reconcile),
    ):
        first = original_schedule(
            candidate["thread_id"],
            retired_runtime_generation=candidate["retired_runtime_generation"],
            retired_attach_token=candidate["retired_attach_token"],
            retired_agent_id=candidate["retired_agent_id"],
        )
        await first
        assert not main._attach_abort_successor_tasks

        scheduled = []

        def schedule_from_sweep(*args, **kwargs):
            task = original_schedule(*args, **kwargs)
            scheduled.append(task)
            return task

        with (
            patch.object(main, "_schedule_attach_abort_successor", schedule_from_sweep),
            patch.object(main, "_trigger_dispatch", MagicMock()),
            patch.object(
                main.thread_retirement_operations,
                "release_thread_resources",
                AsyncMock(),
            ),
            patch.object(
                main.thread_retirement_operations,
                "suspend_thread_resources",
                AsyncMock(),
            ),
        ):
            await main.stale_agent_detector(shutdown_event)
        assert len(scheduled) == 1
        await scheduled[0]

    assert reconcile.await_count == 2
    db.list_retryable_thread_attach_abort_successors.assert_awaited_once_with(limit=25)
    # The same detector pass continues through unrelated recovery work.
    db.recover_orphaned_jobs.assert_awaited_once()
    db.gc_offline_agents.assert_awaited_once_with(retention_hours=24)


@pytest.mark.asyncio
async def test_stale_detector_uses_graph_progress_stall_window():
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event, stall_return=0)

    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "_trigger_dispatch", MagicMock()),
        patch.object(
            main.thread_retirement_operations, "release_thread_resources", AsyncMock()
        ),
        patch.object(
            main.thread_retirement_operations, "suspend_thread_resources", AsyncMock()
        ),
    ):
        await main.stale_agent_detector(shutdown_event)

    db.mark_stalled_working_agents_by_graph_progress.assert_awaited_once_with(
        stall_minutes=10
    )
    method_calls = [call[0] for call in db.method_calls]
    assert method_calls.index("mark_stale_agents_offline") < method_calls.index(
        "mark_stuck_working_agents_ready"
    )
    assert method_calls.index("mark_stuck_working_agents_ready") < method_calls.index(
        "mark_stalled_working_agents_by_graph_progress"
    )
    assert method_calls.index(
        "mark_stalled_working_agents_by_graph_progress"
    ) < method_calls.index("mark_stuck_session_agents_ready")
    assert method_calls.index("mark_stuck_session_agents_ready") < method_calls.index(
        "reap_orphaned_session_agents"
    )


@pytest.mark.asyncio
async def test_stale_detector_triggers_dispatch_on_graph_progress_stall():
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event, stall_return=3)

    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "_trigger_dispatch") as trigger_dispatch,
        patch.object(
            main.thread_retirement_operations, "release_thread_resources", AsyncMock()
        ),
        patch.object(
            main.thread_retirement_operations, "suspend_thread_resources", AsyncMock()
        ),
    ):
        await main.stale_agent_detector(shutdown_event)

    trigger_dispatch.assert_called_once()


@pytest.mark.asyncio
async def test_step_failure_does_not_block_downstream_recovery():
    """One broken sweep must degrade only itself.

    Regression for the 2026-07-11 incident: a bind-type crash in the
    graph-progress sweep aborted the shared try block and silently disabled
    recover_orphaned_jobs (and every other downstream step) for ~36h. See
    knowledge-history/done/stale_agent_detector_sql_crash_disables_recovery_sweeps.md.
    """
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    db.mark_stalled_working_agents_by_graph_progress = AsyncMock(
        side_effect=TypeError(
            "invalid input for query argument $1: 10 (expected str, got int)"
        )
    )

    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "_trigger_dispatch", MagicMock()),
        patch.object(
            main.thread_retirement_operations, "release_thread_resources", AsyncMock()
        ),
        patch.object(
            main.thread_retirement_operations, "suspend_thread_resources", AsyncMock()
        ),
    ):
        await main.stale_agent_detector(shutdown_event)

    # Everything downstream of the crashing step still ran.
    db.mark_stuck_session_agents_ready.assert_awaited_once()
    db.reap_orphaned_session_agents.assert_awaited_once()
    db.mark_orphaned_threads_ended.assert_awaited_once()
    db.mark_orphaned_threads_suspended.assert_awaited_once()
    db.recover_orphaned_jobs.assert_awaited_once_with(
        completion_commands_enabled=main.COMPLETION_COMMANDS_ENABLED
    )
    db.recover_expired_lease_jobs.assert_awaited_once_with(
        completion_commands_enabled=main.COMPLETION_COMMANDS_ENABLED
    )
    db.gc_offline_agents.assert_awaited_once()


@pytest.mark.asyncio
async def test_lease_expiry_recovery_runs_and_triggers_dispatch():
    """Expired-lease jobs are recovered and re-dispatched, independent of the
    agents-table sweeps (knowledge-base/knowledge/features/job_execution_lease.md).
    The wake goes to the owning project's officer only — never the fleet."""
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    db.recover_expired_lease_jobs = AsyncMock(
        return_value=LeaseRecoveryBatch(
            recovered_jobs=(
                RecoveredJob(job_id="job-a", project_id="proj-1"),
                RecoveredJob(job_id="job-b", project_id="proj-1"),
            )
        )
    )

    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "_trigger_dispatch") as trigger_dispatch,
        patch.object(main, "_kick_officer_event_drain") as kick_wake_drain,
        patch.object(main, "notify_all_officers", AsyncMock()) as notify_all,
        patch.object(main, "notify_owning_officers", AsyncMock()) as notify_owning,
        patch.object(
            main.thread_retirement_operations, "release_thread_resources", AsyncMock()
        ),
        patch.object(
            main.thread_retirement_operations, "suspend_thread_resources", AsyncMock()
        ),
    ):
        await main.stale_agent_detector(shutdown_event)

    db.recover_expired_lease_jobs.assert_awaited_once_with(
        completion_commands_enabled=main.COMPLETION_COMMANDS_ENABLED
    )
    notify_owning.assert_awaited_once_with(
        db,
        {"proj-1": {"summary": "2 job(s) recovered by lease expiry: job-a, job-b"}},
        source="fleet",
        dedup_key="fleet:lease_recovered",
    )
    notify_all.assert_not_awaited()
    kick_wake_drain.assert_called_once_with(db)
    trigger_dispatch.assert_called_once()


@pytest.mark.asyncio
async def test_lease_recovery_groups_wakes_per_owning_project():
    """A batch spanning projects sends each officer only its own jobs' ids;
    a job with no project notifies nobody (owner ruling: job-derived fleet
    events are scoped, not broadcast)."""
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    db.recover_expired_lease_jobs = AsyncMock(
        return_value=LeaseRecoveryBatch(
            recovered_jobs=(
                RecoveredJob(job_id="job-a", project_id="proj-1"),
                RecoveredJob(job_id="job-b", project_id="proj-2"),
                RecoveredJob(job_id="job-c", project_id=None),
            )
        )
    )

    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "_trigger_dispatch"),
        patch.object(main, "_kick_officer_event_drain"),
        patch.object(main, "notify_all_officers", AsyncMock()) as notify_all,
        patch.object(main, "notify_owning_officers", AsyncMock()) as notify_owning,
        patch.object(
            main.thread_retirement_operations, "release_thread_resources", AsyncMock()
        ),
        patch.object(
            main.thread_retirement_operations, "suspend_thread_resources", AsyncMock()
        ),
    ):
        await main.stale_agent_detector(shutdown_event)

    notify_owning.assert_awaited_once_with(
        db,
        {
            "proj-1": {"summary": "1 job(s) recovered by lease expiry: job-a"},
            "proj-2": {"summary": "1 job(s) recovered by lease expiry: job-b"},
        },
        source="fleet",
        dedup_key="fleet:lease_recovered",
    )
    notify_all.assert_not_awaited()


@pytest.mark.asyncio
async def test_lease_recovery_of_projectless_jobs_notifies_nobody():
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    db.recover_expired_lease_jobs = AsyncMock(
        return_value=LeaseRecoveryBatch(
            recovered_jobs=(RecoveredJob(job_id="job-a", project_id=None),)
        )
    )

    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "_trigger_dispatch") as trigger_dispatch,
        patch.object(main, "_kick_officer_event_drain") as kick_wake_drain,
        patch.object(main, "notify_all_officers", AsyncMock()) as notify_all,
        patch.object(main, "notify_owning_officers", AsyncMock()) as notify_owning,
        patch.object(
            main.thread_retirement_operations, "release_thread_resources", AsyncMock()
        ),
        patch.object(
            main.thread_retirement_operations, "suspend_thread_resources", AsyncMock()
        ),
    ):
        await main.stale_agent_detector(shutdown_event)

    notify_owning.assert_not_awaited()
    notify_all.assert_not_awaited()
    kick_wake_drain.assert_not_called()
    # The job still goes back to the dispatcher — scoping affects wakes only.
    trigger_dispatch.assert_called_once()


@pytest.mark.asyncio
async def test_orphan_recovery_wakes_only_the_owning_projects_officer():
    """fleet:orphans_recovered is scoped: the owning project's officer hears
    about its own jobs; projectless jobs notify nobody; the fleet fan-out is
    never used for job-derived events."""
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    db.recover_orphaned_jobs = AsyncMock(
        return_value=OrphanRecoveryBatch(
            count=3,
            recovered_jobs=(
                RecoveredJob(job_id="aaaa1111-dead-beef", project_id="proj-a"),
                RecoveredJob(job_id="bbbb2222-dead-beef", project_id="proj-a"),
                RecoveredJob(job_id="cccc3333-dead-beef", project_id=None),
            ),
        )
    )

    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "_trigger_dispatch") as trigger_dispatch,
        patch.object(main, "_kick_officer_event_drain"),
        patch.object(main, "notify_all_officers", AsyncMock()) as notify_all,
        patch.object(main, "notify_owning_officers", AsyncMock()) as notify_owning,
        patch.object(
            main.thread_retirement_operations, "release_thread_resources", AsyncMock()
        ),
        patch.object(
            main.thread_retirement_operations, "suspend_thread_resources", AsyncMock()
        ),
    ):
        await main.stale_agent_detector(shutdown_event)

    notify_owning.assert_awaited_once_with(
        db,
        {
            "proj-a": {
                "summary": (
                    "2 orphaned job(s) auto-paused for re-dispatch "
                    "(agent offline): aaaa1111, bbbb2222"
                )
            }
        },
        source="fleet",
        dedup_key="fleet:orphans_recovered",
    )
    notify_all.assert_not_awaited()
    trigger_dispatch.assert_called_once()


@pytest.mark.asyncio
async def test_agents_offline_scopes_to_derived_projects_and_falls_back_global():
    """Dead agents whose project is derivable (from their assigned/last job)
    wake that project's officer; only the underivable remainder keeps the
    historical fleet-wide fan-out."""
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)

    def _mark(*args, **kwargs):
        shutdown_event.set()
        return [
            {"agent_id": "agent-1", "project_id": "proj-a"},
            {"agent_id": "agent-2", "project_id": "proj-a"},
            {"agent_id": "agent-3", "project_id": None},
        ]

    db.mark_stale_agents_offline = AsyncMock(side_effect=_mark)

    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "_trigger_dispatch"),
        patch.object(main, "_kick_officer_event_drain") as kick_wake_drain,
        patch.object(main, "notify_all_officers", AsyncMock()) as notify_all,
        patch.object(main, "notify_owning_officers", AsyncMock()) as notify_owning,
        patch.object(
            main.thread_retirement_operations, "release_thread_resources", AsyncMock()
        ),
        patch.object(
            main.thread_retirement_operations, "suspend_thread_resources", AsyncMock()
        ),
    ):
        await main.stale_agent_detector(shutdown_event)

    notify_owning.assert_awaited_once_with(
        db,
        {"proj-a": {"summary": "2 agent(s) marked offline (missed heartbeats)"}},
        source="fleet",
        dedup_key="fleet:agents_offline",
    )
    notify_all.assert_awaited_once_with(
        db,
        source="fleet",
        dedup_key="fleet:agents_offline",
        payload={"summary": "1 agent(s) marked offline (missed heartbeats)"},
    )
    kick_wake_drain.assert_called_once_with(db)


@pytest.mark.asyncio
async def test_agents_offline_fully_derivable_skips_the_fleet_fanout():
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)

    def _mark(*args, **kwargs):
        shutdown_event.set()
        return [{"agent_id": "agent-1", "project_id": "proj-a"}]

    db.mark_stale_agents_offline = AsyncMock(side_effect=_mark)

    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "_trigger_dispatch"),
        patch.object(main, "_kick_officer_event_drain"),
        patch.object(main, "notify_all_officers", AsyncMock()) as notify_all,
        patch.object(main, "notify_owning_officers", AsyncMock()) as notify_owning,
        patch.object(
            main.thread_retirement_operations, "release_thread_resources", AsyncMock()
        ),
        patch.object(
            main.thread_retirement_operations, "suspend_thread_resources", AsyncMock()
        ),
    ):
        await main.stale_agent_detector(shutdown_event)

    notify_owning.assert_awaited_once()
    notify_all.assert_not_awaited()


@pytest.mark.asyncio
async def test_lease_recovery_uses_strict_audit_fingerprint_reader():
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    strict_counts = AsyncMock(return_value={})
    reader = MagicMock(is_available=True, get_audit_counts_strict=strict_counts)

    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "audit_reader", reader),
        patch.object(main, "_trigger_dispatch", MagicMock()),
        patch.object(
            main.thread_retirement_operations, "release_thread_resources", AsyncMock()
        ),
        patch.object(
            main.thread_retirement_operations, "suspend_thread_resources", AsyncMock()
        ),
    ):
        await main.stale_agent_detector(shutdown_event)

    db.recover_expired_lease_jobs.assert_awaited_once_with(
        completion_commands_enabled=main.COMPLETION_COMMANDS_ENABLED,
        audit_fingerprint_provider=strict_counts,
    )


@pytest.mark.asyncio
async def test_lease_circuit_trip_kicks_only_durable_wake_drain_not_dispatch():
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    db.recover_expired_lease_jobs = AsyncMock(
        return_value=LeaseRecoveryBatch(
            circuit_trips=(
                LeaseRecoveryCircuitTrip(
                    job_id="job-a",
                    project_id="project-a",
                    unchanged_recoveries=3,
                    officer_destination="wake",
                    officer_thread_id="thread-a",
                    notification_queued=True,
                ),
            )
        )
    )

    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "_trigger_dispatch") as trigger_dispatch,
        patch.object(main, "_kick_officer_event_drain") as kick_wake_drain,
        patch.object(main, "notify_all_officers", AsyncMock()) as notify_all,
        patch.object(
            main.thread_retirement_operations, "release_thread_resources", AsyncMock()
        ),
        patch.object(
            main.thread_retirement_operations, "suspend_thread_resources", AsyncMock()
        ),
    ):
        await main.stale_agent_detector(shutdown_event)

    trigger_dispatch.assert_not_called()
    kick_wake_drain.assert_called_once_with(db)
    notify_all.assert_not_awaited()


@pytest.mark.asyncio
async def test_lease_recovery_survives_orphan_recovery_failure():
    """The lease sweep is the PRIMARY recovery path — a failure in the legacy
    agents-join sweep must not take it down (per-step isolation)."""
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    db.recover_orphaned_jobs = AsyncMock(side_effect=RuntimeError("boom"))

    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "_trigger_dispatch", MagicMock()),
        patch.object(
            main.thread_retirement_operations, "release_thread_resources", AsyncMock()
        ),
        patch.object(
            main.thread_retirement_operations, "suspend_thread_resources", AsyncMock()
        ),
    ):
        await main.stale_agent_detector(shutdown_event)

    db.recover_expired_lease_jobs.assert_awaited_once_with(
        completion_commands_enabled=main.COMPLETION_COMMANDS_ENABLED
    )
    db.gc_offline_agents.assert_awaited_once()


def _lite_retirement(*, backend="none", permanent=False):
    """A retired pinned lite-tier actor: agent Pod, no workspace, no binding."""

    thread_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2"
    generation = "11111111-1111-4111-8111-111111111112"
    token = "22222222-2222-4222-8222-222222222223"
    agent_id = "33333333-3333-4333-8333-333333333334"
    attach_token = "44444444-4444-4444-8444-444444444445"
    context = {
        "thread_id": thread_id,
        "generation": generation,
        "settle_status": "ended",
        "runtime_authority_exposed": True,
        "agent_id": agent_id,
        "runtime_attach_token": attach_token,
        "agent_pod": {
            "pod_name": "persistent-lite",
            "pod_uid": "lite-pod-uid",
            "namespace": "agents-a",
            "protection_protocol": "finalizer_v1",
        },
        "workspace_backend": backend,
        "workspace_container": None,
        "workspace_binding": None,
    }
    retirement = {
        "generation": generation,
        "token": token,
        "permanent": permanent,
        "context": context,
    }
    current = {
        "id": thread_id,
        "runtime_generation": generation,
        "runtime_retirement_token": token,
        "runtime_retirement_local_quiescence": None,
        "metadata": {},
    }
    return retirement, current


def _lite_recovery_mocks(current):
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value=current)
    db.acknowledge_pinned_thread_local_quiescence = AsyncMock(
        return_value={"version": 1}
    )

    @asynccontextmanager
    async def lifecycle_lock(_thread_id):
        yield True

    db.try_thread_advisory_lock = MagicMock(side_effect=lifecycle_lock)
    provisioner = MagicMock(is_available=True)
    provisioner.delete_agent_pod_exact = AsyncMock(return_value=True)
    provisioner.release_agent_pod_finalizer_exact = AsyncMock(return_value=True)
    provisioner.agent_pod_authority = AsyncMock(
        side_effect=["exact_terminal", "exact_absent"]
    )
    return db, provisioner


def _vm_retirement(*, backend="vm"):
    retirement, current = _lite_retirement(backend=backend, permanent=True)
    generation = "55555555-5555-4555-8555-555555555556"
    vm_uid = "vm-incarnation-uid"
    retirement["context"]["vm"] = {
        "status": "ready",
        "provision_generation": generation,
        "identity_provision_generation": generation,
        "identity_authenticated": True,
        "vm_uid": vm_uid,
        "_runtime_incarnation": vm_uid,
        "rootdisk_pvc_uid": "rootdisk-pvc-uid",
        "ssh_host": "192.0.2.44",
        "ssh_port": 22,
        "ssh_host_key_fingerprint": "SHA256:" + "A" * 43,
        "credential_runtime_started": True,
    }
    return retirement, current


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
async def test_lite_backend_retirement_recovers_through_agent_runtime_zero(
    monkeypatch, permanent
):
    """A `none`-backend pinned actor is process-zero once its exact Pod is gone.

    Officers and conferences run on the lite tier: an agent Pod and no
    workspace at all. The design names their proof `agent_runtime_zero_v1`
    and the receipt trigger already accepts it for backend `none`; only the
    Python recovery gate refused everything but `sandbox`/`virtual`, so every
    lite retirement whose agent stopped answering stayed pending forever.
    """
    retirement, current = _lite_retirement(permanent=permanent)
    db, provisioner = _lite_recovery_mocks(current)

    async def immediate_observation(
        _self, pod_name, pod_uid, *, namespace, allowed, **_kwargs
    ):
        state = await provisioner.agent_pod_authority(
            pod_name, expected_pod_uid=pod_uid, namespace=namespace
        )
        return state if state in allowed else None

    monkeypatch.setattr(
        PinnedRetirementOperations,
        "_wait_for_captured_agent_pod_retired",
        immediate_observation,
    )
    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "agent_provisioner", provisioner),
    ):
        assert await main._recover_captured_sandbox_process_zero(retirement)

    provisioner.delete_agent_pod_exact.assert_awaited_once_with(
        "persistent-lite", expected_pod_uid="lite-pod-uid", namespace="agents-a"
    )
    db.acknowledge_pinned_thread_local_quiescence.assert_awaited_once_with(
        retirement["context"]["thread_id"],
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_agent_id=retirement["context"]["agent_id"],
        expected_attach_token=retirement["context"]["runtime_attach_token"],
        expected_settle_status="ended",
        expected_quiescence_protocol="agent_runtime_zero_v1",
        expected_workspace_generation=None,
        expected_workspace_runtime_incarnation=None,
        quiescence_actor="orchestrator",
        expected_agent_pod_uid="lite-pod-uid",
        require_zero_admission=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["vm", "remote"])
async def test_non_sandbox_recovery_uses_the_captured_vm_actuator(monkeypatch, backend):
    """Leader recovery must break the VM receipt/End retry dependency cycle."""
    import orchestrator.services.vm_workspace_recovery_store as recovery
    from orchestrator.services.vm_provisioner import VMTeardownResult

    monkeypatch.setattr(
        main, "VMWorkspaceRecoveryStore", lambda db: idle_recovery_store()
    )
    # This pinned-thread owner has no v3 Job creation/retry resource charge.
    monkeypatch.setattr(
        recovery, "prepare_vm_cleanup_resource", AsyncMock(return_value=None)
    )
    retirement, current = _vm_retirement(backend=backend)
    db, provisioner = _lite_recovery_mocks(current)
    vm_provisioner = MagicMock(lifecycle_available=True)
    vm_provisioner.release_vm_captured = AsyncMock(
        return_value=VMTeardownResult("completed", True)
    )

    async def immediate_observation(
        _self, pod_name, pod_uid, *, namespace, allowed, **_kwargs
    ):
        state = await provisioner.agent_pod_authority(
            pod_name, expected_pod_uid=pod_uid, namespace=namespace
        )
        return state if state in allowed else None

    monkeypatch.setattr(
        PinnedRetirementOperations,
        "_wait_for_captured_agent_pod_retired",
        immediate_observation,
    )
    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "agent_provisioner", provisioner),
        patch.object(main, "vm_provisioner", vm_provisioner),
    ):
        assert await main._recover_captured_sandbox_process_zero(retirement)

    provisioner.delete_agent_pod_exact.assert_awaited_once_with(
        "persistent-lite", expected_pod_uid="lite-pod-uid", namespace="agents-a"
    )
    vm_provisioner.release_vm_captured.assert_awaited_once()
    call = vm_provisioner.release_vm_captured.await_args
    assert call.args[0] == retirement["context"]["thread_id"]
    assert (
        call.args[1].provision_generation
        == retirement["context"]["vm"]["provision_generation"]
    )
    assert call.args[1].vm_uid == retirement["context"]["vm"]["vm_uid"]
    assert call.args[1].rootdisk_pvc_uid == "rootdisk-pvc-uid"
    parent_cleanup = call.kwargs["parent_cleanup"]
    assert parent_cleanup["intent"]["owner_id"] == call.args[0]
    assert parent_cleanup["intent"]["vm_uid"] == call.args[1].vm_uid
    assert call.kwargs == {
        "parent_cleanup": parent_cleanup,
        "ssh_host": "192.0.2.44",
        "ssh_port": 22,
        "purge_disk": True,
        "entity_type": "thread",
        "capture_snapshot": False,
    }
    db.acknowledge_pinned_thread_local_quiescence.assert_awaited_once_with(
        retirement["context"]["thread_id"],
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_agent_id=retirement["context"]["agent_id"],
        expected_attach_token=retirement["context"]["runtime_attach_token"],
        expected_settle_status="ended",
        expected_quiescence_protocol="workspace_actuator_zero_v1",
        expected_workspace_generation=retirement["context"]["vm"][
            "provision_generation"
        ],
        expected_workspace_runtime_incarnation=retirement["context"]["vm"]["vm_uid"],
        quiescence_actor="orchestrator",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["vm", "remote"])
async def test_unactuated_backend_recovery_refusal_is_logged(
    monkeypatch, caplog, backend
):
    """A backend with no crash-recovery actuator must say so, not go quiet."""
    retirement, current = _lite_retirement(backend=backend)
    db, provisioner = _lite_recovery_mocks(current)
    caplog.set_level(logging.WARNING)
    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "agent_provisioner", provisioner),
    ):
        assert not await main._recover_captured_sandbox_process_zero(retirement)

    provisioner.delete_agent_pod_exact.assert_not_awaited()
    db.acknowledge_pinned_thread_local_quiescence.assert_not_awaited()
    refusals = [
        r for r in caplog.records if "no process-zero actuator" in r.getMessage()
    ]
    assert len(refusals) == 1
    assert backend in refusals[0].getMessage()
    assert retirement["context"]["thread_id"] in refusals[0].getMessage()


@pytest.mark.asyncio
async def test_pending_retirement_retry_refusal_is_logged(monkeypatch, caplog):
    """A 409/503 refusal is a retry outcome, not silence.

    Before this, a retirement that could never finish retried every sweep
    with zero log output: the refusal was swallowed into `False`, and the
    caller only logged successes. Fourteen hours of that on dev looked
    exactly like nothing being wrong.
    """
    thread_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3"
    generation = "11111111-1111-4111-8111-111111111113"
    token = "22222222-2222-4222-8222-222222222224"
    candidate = {
        "id": thread_id,
        "runtime_generation": generation,
        "runtime_retirement_token": token,
        "runtime_retirement_permanent": False,
        "runtime_retirement_context": {
            "thread_id": thread_id,
            "generation": generation,
            "settle_status": "ended",
            "runtime_authority_exposed": False,
        },
    }
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value={"id": thread_id})
    refused = HTTPException(
        status_code=409, detail={"code": "pinned_runtime_identity_mismatch"}
    )
    caplog.set_level(logging.WARNING)
    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "_end_thread_flow", AsyncMock(side_effect=refused)),
    ):
        assert await main._retry_pending_pinned_retirement(candidate) is False

    refusals = [r for r in caplog.records if thread_id in r.getMessage()]
    assert len(refusals) == 1
    assert "409" in refusals[0].getMessage()
    assert "pinned_runtime_identity_mismatch" in refusals[0].getMessage()


@pytest.mark.asyncio
async def test_sweep_reports_unresolved_pinned_retirements(caplog):
    """An all-failing retry pass must leave a trace in the log."""
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    db.list_retryable_pinned_retirements = AsyncMock(
        return_value=[{"id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4"}]
    )
    caplog.set_level(logging.WARNING)
    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "_trigger_dispatch", MagicMock()),
        patch.object(
            main.thread_retirement_operations, "release_thread_resources", AsyncMock()
        ),
        patch.object(
            main.thread_retirement_operations, "suspend_thread_resources", AsyncMock()
        ),
        patch.object(
            main, "_retry_pending_pinned_retirement", AsyncMock(return_value=False)
        ),
    ):
        await main.stale_agent_detector(shutdown_event)

    unresolved = [r for r in caplog.records if "remain unresolved" in r.getMessage()]
    assert len(unresolved) == 1
    assert unresolved[0].getMessage().startswith("1 ")


@pytest.mark.asyncio
async def test_pending_retirement_retry_logs_when_recovery_cannot_prove_zero(
    caplog,
):
    """Recovery returning False is a refusal too, and must say so."""
    retirement, current = _lite_retirement()
    context = dict(retirement["context"])
    candidate = {
        "id": context["thread_id"],
        "runtime_generation": retirement["generation"],
        "runtime_retirement_token": retirement["token"],
        "runtime_retirement_permanent": False,
        "runtime_retirement_context": context,
    }
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value=current)
    caplog.set_level(logging.WARNING)
    with (
        patch.object(main, "postgres_db", db),
        patch.object(
            main,
            "_recover_captured_sandbox_process_zero",
            AsyncMock(return_value=False),
        ),
        patch.object(main, "_end_thread_flow", AsyncMock()) as end_flow,
    ):
        assert await main._retry_pending_pinned_retirement(candidate) is False

    end_flow.assert_not_awaited()
    refusals = [
        r for r in caplog.records if "could not prove process zero" in r.getMessage()
    ]
    assert len(refusals) == 1
    assert context["thread_id"] in refusals[0].getMessage()
    assert "'none'" in refusals[0].getMessage()


@pytest.mark.asyncio
async def test_recovery_logs_when_the_receipt_is_refused_after_the_pod_stop(
    monkeypatch, caplog
):
    """The Pod is gone but the DB refused the receipt: name it, don't hide it."""
    retirement, current = _lite_retirement()
    db, provisioner = _lite_recovery_mocks(current)
    db.acknowledge_pinned_thread_local_quiescence = AsyncMock(return_value=None)
    db.acknowledge_settled_virtual_actor_exit = AsyncMock(return_value=None)

    async def immediate_observation(
        _self, pod_name, pod_uid, *, namespace, allowed, **_kwargs
    ):
        state = await provisioner.agent_pod_authority(
            pod_name, expected_pod_uid=pod_uid, namespace=namespace
        )
        return state if state in allowed else None

    monkeypatch.setattr(
        PinnedRetirementOperations,
        "_wait_for_captured_agent_pod_retired",
        immediate_observation,
    )
    caplog.set_level(logging.WARNING)
    with (
        patch.object(main, "postgres_db", db),
        patch.object(main, "agent_provisioner", provisioner),
    ):
        assert not await main._recover_captured_sandbox_process_zero(retirement)

    # Both contracts were consulted for this used-or-created lite life.
    db.acknowledge_pinned_thread_local_quiescence.assert_awaited_once()
    db.acknowledge_settled_virtual_actor_exit.assert_awaited_once()
    refusals = [r for r in caplog.records if "receipt refused" in r.getMessage()]
    assert len(refusals) == 1
    assert retirement["context"]["thread_id"] in refusals[0].getMessage()
