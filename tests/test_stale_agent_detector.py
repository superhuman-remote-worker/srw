"""Unit tests for stale-agent background sweeps."""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from types import SimpleNamespace

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
from orchestrator.services import stale_agent_detector as detector
from orchestrator.services.pinned_retirement import (
    PinnedRetirementDependencies,
    PinnedRetirementOperations,
)
from orchestrator.services.stale_agent_detector import StaleAgentDetectorDependencies


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


class _ThreadRetirementFake:
    """The composed thread retirement surface the detector asks for End."""

    def __init__(self, end_thread_flow=None):
        self.end_thread_flow = end_thread_flow or AsyncMock(
            return_value={"status": "ended"}
        )


def _pinned_operations(*, recover=None):
    """Real retirement-context validators with a fake process-zero recovery.

    ``retirement_context_runtime_exposed`` and
    ``retirement_has_exact_local_quiescence`` are pure validators of the
    captured context, so the detector's retry runs them for real; only the
    external crash-recovery actuator is replaced.
    """

    real = PinnedRetirementOperations(
        PinnedRetirementDependencies(
            store=MagicMock(name="unused_store"),
            agent_provisioner=MagicMock(name="unused_agent_provisioner"),
            persistent_provisioner=MagicMock(name="unused_persistent_provisioner"),
            container_provisioner=MagicMock(name="unused_container_provisioner"),
            docker_provisioner=MagicMock(name="unused_docker_provisioner"),
            vm_provisioner=MagicMock(name="unused_vm_provisioner"),
            recovery_store=MagicMock(name="unused_recovery_store"),
            session_router=MagicMock(name="unused_session_router"),
            resolve_protected_reader_backend=AsyncMock(),
            resolve_ssh_key_path=MagicMock(),
            logger=logging.getLogger("tests.pinned_retirement"),
        )
    )
    return SimpleNamespace(
        retirement_context_runtime_exposed=real.retirement_context_runtime_exposed,
        retirement_has_exact_local_quiescence=(
            real.retirement_has_exact_local_quiescence
        ),
        recover_captured_process_zero=(
            recover
            if recover is not None
            else AsyncMock(side_effect=AssertionError("unexpected crash recovery"))
        ),
    )


def _detector_dependencies(db, **overrides) -> StaleAgentDetectorDependencies:
    """Build the detector's explicit collaborators from a test's fakes."""

    thread_ops = _ThreadRetirementFake()
    pinned_ops = _pinned_operations()
    fields = {
        "store": db,
        "agent_provisioner": MagicMock(name="agent_provisioner"),
        "docker_provisioner": MagicMock(name="docker_provisioner"),
        # main's audit reader is unavailable until the lifespan connects it.
        "audit_reader": None,
        "completion_commands_enabled": False,
        "trigger_dispatch": MagicMock(name="trigger_dispatch"),
        "schedule_attach_abort_successor": MagicMock(
            name="schedule_attach_abort_successor"
        ),
        "thread_retirement_operations": lambda: thread_ops,
        "pinned_retirement_operations": lambda: pinned_ops,
    }
    fields.update(overrides)
    return StaleAgentDetectorDependencies(**fields)


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
            await main._pinned_retirement_operations().recover_captured_process_zero(
                retirement
            )
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
        assert await main._pinned_retirement_operations().recover_captured_process_zero(
            retirement
        )

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
    # The scheduler is the application's collaborator; the detector only
    # receives it through its dependencies.
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
        scheduled_calls = []

        def schedule_from_sweep(*args, **kwargs):
            scheduled_calls.append((args, kwargs))
            task = original_schedule(*args, **kwargs)
            scheduled.append(task)
            return task

        await detector.stale_agent_detector(
            shutdown_event,
            dependencies=_detector_dependencies(
                db, schedule_attach_abort_successor=schedule_from_sweep
            ),
        )
        assert len(scheduled) == 1
        await scheduled[0]

    assert scheduled_calls == [
        (
            (candidate["thread_id"],),
            {
                "retired_runtime_generation": candidate["retired_runtime_generation"],
                "retired_attach_token": candidate["retired_attach_token"],
                "retired_agent_id": candidate["retired_agent_id"],
            },
        )
    ]
    assert reconcile.await_count == 2
    db.list_retryable_thread_attach_abort_successors.assert_awaited_once_with(limit=25)
    # The same detector pass continues through unrelated recovery work.
    db.recover_orphaned_jobs.assert_awaited_once()
    db.gc_offline_agents.assert_awaited_once_with(retention_hours=24)


@pytest.mark.asyncio
async def test_stale_detector_uses_graph_progress_stall_window():
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event, stall_return=0)

    await detector.stale_agent_detector(
        shutdown_event, dependencies=_detector_dependencies(db)
    )

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
    trigger_dispatch = MagicMock()

    await detector.stale_agent_detector(
        shutdown_event,
        dependencies=_detector_dependencies(db, trigger_dispatch=trigger_dispatch),
    )

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

    await detector.stale_agent_detector(
        shutdown_event,
        dependencies=_detector_dependencies(db, completion_commands_enabled=True),
    )

    # Everything downstream of the crashing step still ran.
    db.mark_stuck_session_agents_ready.assert_awaited_once()
    db.reap_orphaned_session_agents.assert_awaited_once()
    db.mark_orphaned_threads_ended.assert_awaited_once()
    db.mark_orphaned_threads_suspended.assert_awaited_once()
    db.recover_orphaned_jobs.assert_awaited_once_with(completion_commands_enabled=True)
    db.recover_expired_lease_jobs.assert_awaited_once_with(
        completion_commands_enabled=True
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
    trigger_dispatch = MagicMock()

    with (
        patch.object(detector, "_kick_officer_event_drain") as kick_wake_drain,
        patch.object(detector, "notify_all_officers", AsyncMock()) as notify_all,
        patch.object(detector, "notify_owning_officers", AsyncMock()) as notify_owning,
    ):
        await detector.stale_agent_detector(
            shutdown_event,
            dependencies=_detector_dependencies(db, trigger_dispatch=trigger_dispatch),
        )

    db.recover_expired_lease_jobs.assert_awaited_once_with(
        completion_commands_enabled=False
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
        patch.object(detector, "_kick_officer_event_drain"),
        patch.object(detector, "notify_all_officers", AsyncMock()) as notify_all,
        patch.object(detector, "notify_owning_officers", AsyncMock()) as notify_owning,
    ):
        await detector.stale_agent_detector(
            shutdown_event, dependencies=_detector_dependencies(db)
        )

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
    trigger_dispatch = MagicMock()

    with (
        patch.object(detector, "_kick_officer_event_drain") as kick_wake_drain,
        patch.object(detector, "notify_all_officers", AsyncMock()) as notify_all,
        patch.object(detector, "notify_owning_officers", AsyncMock()) as notify_owning,
    ):
        await detector.stale_agent_detector(
            shutdown_event,
            dependencies=_detector_dependencies(db, trigger_dispatch=trigger_dispatch),
        )

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
    trigger_dispatch = MagicMock()

    with (
        patch.object(detector, "_kick_officer_event_drain"),
        patch.object(detector, "notify_all_officers", AsyncMock()) as notify_all,
        patch.object(detector, "notify_owning_officers", AsyncMock()) as notify_owning,
    ):
        await detector.stale_agent_detector(
            shutdown_event,
            dependencies=_detector_dependencies(db, trigger_dispatch=trigger_dispatch),
        )

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
        patch.object(detector, "_kick_officer_event_drain") as kick_wake_drain,
        patch.object(detector, "notify_all_officers", AsyncMock()) as notify_all,
        patch.object(detector, "notify_owning_officers", AsyncMock()) as notify_owning,
    ):
        await detector.stale_agent_detector(
            shutdown_event, dependencies=_detector_dependencies(db)
        )

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
        patch.object(detector, "_kick_officer_event_drain"),
        patch.object(detector, "notify_all_officers", AsyncMock()) as notify_all,
        patch.object(detector, "notify_owning_officers", AsyncMock()) as notify_owning,
    ):
        await detector.stale_agent_detector(
            shutdown_event, dependencies=_detector_dependencies(db)
        )

    notify_owning.assert_awaited_once()
    notify_all.assert_not_awaited()


@pytest.mark.asyncio
async def test_lease_recovery_uses_strict_audit_fingerprint_reader():
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    strict_counts = AsyncMock(return_value={})
    reader = MagicMock(is_available=True, get_audit_counts_strict=strict_counts)

    await detector.stale_agent_detector(
        shutdown_event, dependencies=_detector_dependencies(db, audit_reader=reader)
    )

    db.recover_expired_lease_jobs.assert_awaited_once_with(
        completion_commands_enabled=False,
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
    trigger_dispatch = MagicMock()

    with (
        patch.object(detector, "_kick_officer_event_drain") as kick_wake_drain,
        patch.object(detector, "notify_all_officers", AsyncMock()) as notify_all,
    ):
        await detector.stale_agent_detector(
            shutdown_event,
            dependencies=_detector_dependencies(db, trigger_dispatch=trigger_dispatch),
        )

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

    await detector.stale_agent_detector(
        shutdown_event, dependencies=_detector_dependencies(db)
    )

    db.recover_expired_lease_jobs.assert_awaited_once_with(
        completion_commands_enabled=False
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
        assert await main._pinned_retirement_operations().recover_captured_process_zero(
            retirement
        )

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
        assert await main._pinned_retirement_operations().recover_captured_process_zero(
            retirement
        )

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
        assert not await main._pinned_retirement_operations().recover_captured_process_zero(
            retirement
        )

    provisioner.delete_agent_pod_exact.assert_not_awaited()
    db.acknowledge_pinned_thread_local_quiescence.assert_not_awaited()
    refusals = [
        r for r in caplog.records if "no process-zero actuator" in r.getMessage()
    ]
    assert len(refusals) == 1
    assert backend in refusals[0].getMessage()
    assert retirement["context"]["thread_id"] in refusals[0].getMessage()


@pytest.mark.asyncio
async def test_pending_retirement_retry_refusal_is_logged(caplog):
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
    # The refusal comes from the End funnel the retry actually calls.
    thread_ops = _ThreadRetirementFake(AsyncMock(side_effect=refused))
    caplog.set_level(logging.WARNING)
    assert (
        await detector.retry_pending_pinned_retirement(
            candidate,
            dependencies=_detector_dependencies(
                db, thread_retirement_operations=lambda: thread_ops
            ),
        )
        is False
    )

    thread_ops.end_thread_flow.assert_awaited_once_with(
        thread_id,
        {"id": thread_id},
        permanent=False,
        force=True,
        expected_runtime_generation=generation,
        expected_agent_id=None,
        expected_attach_token=None,
        require_expected_agent_offline=False,
        settle_status="ended",
        local_runtime_quiesced=False,
    )
    refusals = [r for r in caplog.records if thread_id in r.getMessage()]
    assert len(refusals) == 1
    assert "409" in refusals[0].getMessage()
    assert "pinned_runtime_identity_mismatch" in refusals[0].getMessage()


@pytest.mark.asyncio
async def test_sweep_reports_unresolved_pinned_retirements(caplog):
    """An all-failing retry pass must leave a trace in the log."""
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    candidate = {"id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4"}
    db.list_retryable_pinned_retirements = AsyncMock(return_value=[candidate])
    dependencies = _detector_dependencies(db)
    caplog.set_level(logging.WARNING)
    # Step 3d calls the owner module's retry by name.
    with patch.object(
        detector, "retry_pending_pinned_retirement", AsyncMock(return_value=False)
    ) as retry:
        await detector.stale_agent_detector(shutdown_event, dependencies=dependencies)

    retry.assert_awaited_once_with(candidate, dependencies=dependencies)
    db.list_retryable_pinned_retirements.assert_awaited_once_with(
        grace_seconds=detector.PINNED_RETIREMENT_RETRY_GRACE_SECONDS,
        limit=25,
        proven_grace_seconds=detector.PINNED_RETIREMENT_PROVEN_RETRY_GRACE_SECONDS,
    )
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
    recover = AsyncMock(return_value=False)
    # Both fakes sit on the seams the retry actually calls, so the
    # not-awaited assertion below can fail.
    thread_ops = _ThreadRetirementFake()
    pinned_ops = _pinned_operations(recover=recover)
    caplog.set_level(logging.WARNING)
    assert (
        await detector.retry_pending_pinned_retirement(
            candidate,
            dependencies=_detector_dependencies(
                db,
                thread_retirement_operations=lambda: thread_ops,
                pinned_retirement_operations=lambda: pinned_ops,
            ),
        )
        is False
    )

    recover.assert_awaited_once_with(
        {
            "generation": retirement["generation"],
            "token": retirement["token"],
            "permanent": False,
            "context": context,
        }
    )
    thread_ops.end_thread_flow.assert_not_awaited()
    refusals = [
        r for r in caplog.records if "could not prove process zero" in r.getMessage()
    ]
    assert len(refusals) == 1
    assert context["thread_id"] in refusals[0].getMessage()
    assert "'none'" in refusals[0].getMessage()


@pytest.mark.asyncio
async def test_early_nominated_retry_never_reaches_crash_recovery():
    """A row nominated before the live-drain grace must carry its proof.

    Step 3d admits already-proven rows before the full grace. If the exact
    receipt no longer validates, the row waits for the full grace instead of
    being handed to crash recovery early.
    """
    retirement, current = _lite_retirement()
    context = dict(retirement["context"])
    candidate = {
        "id": context["thread_id"],
        "runtime_generation": retirement["generation"],
        "runtime_retirement_token": retirement["token"],
        "runtime_retirement_permanent": False,
        "runtime_retirement_context": context,
        "nominated_before_grace": True,
    }
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value=current)
    recover = AsyncMock(return_value=True)
    thread_ops = _ThreadRetirementFake()
    pinned_ops = _pinned_operations(recover=recover)
    dependencies = _detector_dependencies(
        db,
        thread_retirement_operations=lambda: thread_ops,
        pinned_retirement_operations=lambda: pinned_ops,
    )
    assert (
        await detector.retry_pending_pinned_retirement(
            candidate, dependencies=dependencies
        )
        is False
    )
    recover.assert_not_awaited()
    thread_ops.end_thread_flow.assert_not_awaited()

    candidate["nominated_before_grace"] = False
    recover.return_value = False
    assert (
        await detector.retry_pending_pinned_retirement(
            candidate, dependencies=dependencies
        )
        is False
    )
    recover.assert_awaited_once()
    thread_ops.end_thread_flow.assert_not_awaited()


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
        assert not await main._pinned_retirement_operations().recover_captured_process_zero(
            retirement
        )

    # Both contracts were consulted for this used-or-created lite life.
    db.acknowledge_pinned_thread_local_quiescence.assert_awaited_once()
    db.acknowledge_settled_virtual_actor_exit.assert_awaited_once()
    refusals = [r for r in caplog.records if "receipt refused" in r.getMessage()]
    assert len(refusals) == 1
    assert retirement["context"]["thread_id"] in refusals[0].getMessage()


# ---------------------------------------------------------------------------
# R1.B11 owner guards: bodies moved to services/stale_agent_detector.py.
# ---------------------------------------------------------------------------

_OFFLINE_THREAD_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaab1"
_OFFLINE_GENERATION = "11111111-1111-4111-8111-11111111ab11"
_OFFLINE_AGENT_ID = "33333333-3333-4333-8333-33333333ab33"
_OFFLINE_ATTACH_TOKEN = "44444444-4444-4444-8444-44444444ab44"


def _offline_candidate(**overrides):
    candidate = {
        "id": _OFFLINE_THREAD_ID,
        "runtime_generation": _OFFLINE_GENERATION,
        "agent_id": _OFFLINE_AGENT_ID,
        "runtime_attach_token": _OFFLINE_ATTACH_TOKEN,
        "status": "active",
    }
    candidate.update(overrides)
    return candidate


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "settle_status"),
    [
        ("active", "ended"),
        ("created", "ended"),
        ("awaiting_user", "suspended"),
        ("suspended", "suspended"),
    ],
)
async def test_retire_orphaned_runtime_ends_the_exact_offline_incarnation(
    status, settle_status
):
    thread = {"id": _OFFLINE_THREAD_ID, "status": status}
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value=thread)
    thread_ops = _ThreadRetirementFake()

    assert await detector.retire_orphaned_pinned_runtime(
        _offline_candidate(status=status),
        dependencies=_detector_dependencies(
            db, thread_retirement_operations=lambda: thread_ops
        ),
    )

    db.get_thread.assert_awaited_once_with(_OFFLINE_THREAD_ID)
    thread_ops.end_thread_flow.assert_awaited_once_with(
        _OFFLINE_THREAD_ID,
        thread,
        permanent=False,
        force=True,
        expected_runtime_generation=_OFFLINE_GENERATION,
        expected_agent_id=_OFFLINE_AGENT_ID,
        expected_attach_token=_OFFLINE_ATTACH_TOKEN,
        require_expected_agent_offline=True,
        settle_status=settle_status,
    )


@pytest.mark.asyncio
async def test_retire_orphaned_runtime_passes_a_missing_attach_token_as_none():
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value={"id": _OFFLINE_THREAD_ID})
    thread_ops = _ThreadRetirementFake()

    assert await detector.retire_orphaned_pinned_runtime(
        _offline_candidate(runtime_attach_token=None),
        dependencies=_detector_dependencies(
            db, thread_retirement_operations=lambda: thread_ops
        ),
    )

    assert thread_ops.end_thread_flow.await_args.kwargs["expected_attach_token"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["id", "runtime_generation", "agent_id"])
async def test_retire_orphaned_runtime_requires_the_complete_identity(missing):
    db = AsyncMock()
    thread_ops = _ThreadRetirementFake()

    assert not await detector.retire_orphaned_pinned_runtime(
        _offline_candidate(**{missing: None}),
        dependencies=_detector_dependencies(
            db, thread_retirement_operations=lambda: thread_ops
        ),
    )

    db.get_thread.assert_not_awaited()
    thread_ops.end_thread_flow.assert_not_awaited()


@pytest.mark.asyncio
async def test_retire_orphaned_runtime_skips_a_vanished_thread():
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value=None)
    thread_ops = _ThreadRetirementFake()

    assert not await detector.retire_orphaned_pinned_runtime(
        _offline_candidate(),
        dependencies=_detector_dependencies(
            db, thread_retirement_operations=lambda: thread_ops
        ),
    )

    thread_ops.end_thread_flow.assert_not_awaited()


@pytest.mark.asyncio
async def test_retire_orphaned_runtime_preserves_a_runtime_that_won_authority(
    caplog,
):
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value={"id": _OFFLINE_THREAD_ID})
    thread_ops = _ThreadRetirementFake(
        AsyncMock(side_effect=HTTPException(status_code=409, detail="rebound"))
    )
    caplog.set_level(logging.INFO, logger=detector.__name__)

    assert not await detector.retire_orphaned_pinned_runtime(
        _offline_candidate(),
        dependencies=_detector_dependencies(
            db, thread_retirement_operations=lambda: thread_ops
        ),
    )

    lost = [r for r in caplog.records if "lost authority" in r.getMessage()]
    assert len(lost) == 1
    assert lost[0].levelno == logging.INFO
    assert _OFFLINE_THREAD_ID in lost[0].getMessage()


@pytest.mark.asyncio
async def test_retire_orphaned_runtime_propagates_a_non_authority_failure():
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value={"id": _OFFLINE_THREAD_ID})
    thread_ops = _ThreadRetirementFake(
        AsyncMock(side_effect=HTTPException(status_code=503, detail="retry"))
    )

    with pytest.raises(HTTPException) as raised:
        await detector.retire_orphaned_pinned_runtime(
            _offline_candidate(),
            dependencies=_detector_dependencies(
                db, thread_retirement_operations=lambda: thread_ops
            ),
        )
    assert raised.value.status_code == 503


@pytest.mark.asyncio
async def test_detector_keeps_retiring_candidates_after_one_fails(caplog):
    """Step 3/3b isolate each candidate: one failing End must not stop the
    next candidate, the paused sweep, or anything downstream."""

    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    failing = _offline_candidate(id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaac1")
    healthy = _offline_candidate(id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaac2")
    paused = _offline_candidate(
        id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaac3", status="awaiting_user"
    )
    db.mark_orphaned_threads_ended = AsyncMock(
        return_value=[failing, "not-a-row", healthy]
    )
    db.mark_orphaned_threads_suspended = AsyncMock(return_value=[paused])
    db.get_thread = AsyncMock(side_effect=lambda thread_id: {"id": thread_id})

    async def end_thread_flow(thread_id, *_args, **_kwargs):
        if thread_id == failing["id"]:
            raise RuntimeError("cleanup exploded")
        return {"status": "ended"}

    thread_ops = _ThreadRetirementFake(AsyncMock(side_effect=end_thread_flow))
    caplog.set_level(logging.INFO, logger=detector.__name__)

    await detector.stale_agent_detector(
        shutdown_event,
        dependencies=_detector_dependencies(
            db, thread_retirement_operations=lambda: thread_ops
        ),
    )

    ended = [call.args[0] for call in thread_ops.end_thread_flow.await_args_list]
    assert ended == [failing["id"], healthy["id"], paused["id"]]
    assert (
        thread_ops.end_thread_flow.await_args_list[2].kwargs["settle_status"]
        == "suspended"
    )
    messages = [r.getMessage() for r in caplog.records]
    assert (
        "Stale agent detector step 'retire_orphaned_pinned_runtime' failed: "
        "cleanup exploded"
    ) in messages
    assert "Retired 1 offline pinned runtime(s) through exact End" in messages
    assert "Retired 1 paused offline pinned runtime(s) through exact End" in messages
    db.recover_orphaned_jobs.assert_awaited_once()
    db.gc_offline_agents.assert_awaited_once_with(retention_hours=24)


@pytest.mark.asyncio
async def test_unresolved_warning_counts_every_unretired_nomination(caplog):
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    done = {"id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaad1"}
    refused = {"id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaad2"}
    db.list_retryable_pinned_retirements = AsyncMock(
        return_value=[done, refused, "not-a-row"]
    )
    caplog.set_level(logging.INFO, logger=detector.__name__)

    async def retry(candidate, *, dependencies):
        return candidate is done

    with patch.object(detector, "retry_pending_pinned_retirement", retry):
        await detector.stale_agent_detector(
            shutdown_event, dependencies=_detector_dependencies(db)
        )

    messages = [r.getMessage() for r in caplog.records]
    assert "Completed 1 durable pinned retirement retry(s)" in messages
    unresolved = [r for r in caplog.records if "remain unresolved" in r.getMessage()]
    assert len(unresolved) == 1
    assert unresolved[0].levelno == logging.WARNING
    assert unresolved[0].getMessage().startswith("2 durable pinned retirement(s)")


@pytest.mark.asyncio
async def test_fully_resolved_retry_pass_logs_no_unresolved_warning(caplog):
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    db.list_retryable_pinned_retirements = AsyncMock(
        return_value=[{"id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaae1"}]
    )
    caplog.set_level(logging.INFO, logger=detector.__name__)

    with patch.object(
        detector, "retry_pending_pinned_retirement", AsyncMock(return_value=True)
    ):
        await detector.stale_agent_detector(
            shutdown_event, dependencies=_detector_dependencies(db)
        )

    messages = [r.getMessage() for r in caplog.records]
    assert "Completed 1 durable pinned retirement retry(s)" in messages
    assert not [m for m in messages if "remain unresolved" in m]


@pytest.mark.asyncio
async def test_retry_step_isolates_a_raising_retry(caplog):
    """A retry that raises is one failed step, counted as unresolved."""

    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    db.list_retryable_pinned_retirements = AsyncMock(
        return_value=[
            {"id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaf1"},
            {"id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaf2"},
        ]
    )
    caplog.set_level(logging.INFO, logger=detector.__name__)
    retry = AsyncMock(side_effect=[RuntimeError("retry exploded"), True])

    with patch.object(detector, "retry_pending_pinned_retirement", retry):
        await detector.stale_agent_detector(
            shutdown_event, dependencies=_detector_dependencies(db)
        )

    assert retry.await_count == 2
    messages = [r.getMessage() for r in caplog.records]
    assert (
        "Stale agent detector step 'retry_pending_pinned_retirement' failed: "
        "retry exploded"
    ) in messages
    assert "Completed 1 durable pinned retirement retry(s)" in messages
    assert [m for m in messages if "remain unresolved" in m] == [
        "1 durable pinned retirement(s) remain unresolved after this pass; "
        "each refusal is logged above"
    ]
    db.gc_offline_agents.assert_awaited_once()


@pytest.mark.asyncio
async def test_detector_reopens_abandoned_preflights_behind_their_grace(caplog):
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    db.abort_stale_pinned_retirement_preflights = AsyncMock(
        return_value=[{"id": "t1"}, {"id": "t2"}]
    )
    caplog.set_level(logging.WARNING, logger=detector.__name__)

    await detector.stale_agent_detector(
        shutdown_event, dependencies=_detector_dependencies(db)
    )

    db.abort_stale_pinned_retirement_preflights.assert_awaited_once_with(
        grace_seconds=detector.PINNED_RETIREMENT_PREFLIGHT_GRACE_SECONDS,
        limit=25,
    )
    assert "Reopened 2 abandoned pinned retirement preflight(s)" in [
        r.getMessage() for r in caplog.records
    ]


@pytest.mark.asyncio
async def test_detector_reaps_orphaned_session_pods_by_exact_uid(caplog):
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    db.reap_orphaned_session_agents = AsyncMock(
        return_value=[
            {"id": "agent-1", "hostname": "pod-1", "pod_uid": "uid-1"},
            {"id": "agent-2", "hostname": "pod-2", "pod_uid": None},
        ]
    )
    agent_provisioner = MagicMock()
    agent_provisioner.delete_agent_pod = AsyncMock(
        side_effect=[RuntimeError("api down"), True]
    )
    caplog.set_level(logging.WARNING, logger=detector.__name__)

    await detector.stale_agent_detector(
        shutdown_event,
        dependencies=_detector_dependencies(db, agent_provisioner=agent_provisioner),
    )

    db.reap_orphaned_session_agents.assert_awaited_once_with(grace_minutes=5)
    assert [
        (call.args, call.kwargs)
        for call in agent_provisioner.delete_agent_pod.await_args_list
    ] == [
        (("pod-1",), {"expected_pod_uid": "uid-1"}),
        (("pod-2",), {"expected_pod_uid": ""}),
    ]
    reaped = [r.getMessage() for r in caplog.records if "Reaped" in r.getMessage()]
    assert reaped == [
        "Reaped orphaned session agent agent-1 (pod=pod-1, deleted=None): "
        "'session' with no thread/job past grace",
        "Reaped orphaned session agent agent-2 (pod=pod-2, deleted=True): "
        "'session' with no thread/job past grace",
    ]


@pytest.mark.asyncio
async def test_detector_settles_each_claimed_docker_workspace_retirement():
    shutdown_event = asyncio.Event()
    db = _mock_db(shutdown_event)
    claims = [{"lease": "a"}, {"lease": "b"}]
    db.claim_terminal_docker_workspace_retirements = AsyncMock(return_value=claims)
    docker_provisioner = MagicMock()
    docker_provisioner.settle_claimed_terminal_workspace_retirement = AsyncMock(
        side_effect=[RuntimeError("ssh down"), True]
    )

    await detector.stale_agent_detector(
        shutdown_event,
        dependencies=_detector_dependencies(db, docker_provisioner=docker_provisioner),
    )

    assert [
        call.args
        for call in (
            docker_provisioner.settle_claimed_terminal_workspace_retirement.await_args_list
        )
    ] == [(claims[0],), (claims[1],)]
    db.recover_orphaned_jobs.assert_awaited_once()


def _retry_candidate(**overrides):
    retirement, current = _lite_retirement()
    context = dict(retirement["context"])
    candidate = {
        "id": context["thread_id"],
        "runtime_generation": retirement["generation"],
        "runtime_retirement_token": retirement["token"],
        "runtime_retirement_permanent": False,
        "runtime_retirement_context": context,
    }
    candidate.update(overrides)
    return candidate, context, current


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "malform",
    [
        "context_bad_json",
        "context_not_mapping",
        "missing_id",
        "missing_generation",
        "missing_token",
        "bad_settle_status",
        "generation_mismatch",
        "permanent_suspend",
    ],
)
async def test_retry_refuses_a_malformed_durable_marker(malform):
    candidate, context, _current = _retry_candidate()
    if malform == "context_bad_json":
        candidate["runtime_retirement_context"] = "{not json"
    elif malform == "context_not_mapping":
        candidate["runtime_retirement_context"] = ["ended"]
    elif malform == "missing_id":
        candidate["id"] = None
    elif malform == "missing_generation":
        candidate["runtime_generation"] = ""
    elif malform == "missing_token":
        candidate["runtime_retirement_token"] = None
    elif malform == "bad_settle_status":
        context["settle_status"] = "deleted"
    elif malform == "generation_mismatch":
        context["generation"] = "99999999-9999-4999-8999-999999999999"
    elif malform == "permanent_suspend":
        context["settle_status"] = "suspended"
        candidate["runtime_retirement_permanent"] = True
    db = AsyncMock()
    thread_ops = _ThreadRetirementFake()

    assert not await detector.retry_pending_pinned_retirement(
        candidate,
        dependencies=_detector_dependencies(
            db, thread_retirement_operations=lambda: thread_ops
        ),
    )

    db.get_thread.assert_not_awaited()
    thread_ops.end_thread_flow.assert_not_awaited()


@pytest.mark.asyncio
async def test_retry_decodes_a_json_context_and_ends_with_the_receipt():
    """A string context is decoded; an exact receipt skips crash recovery."""

    candidate, context, current = _retry_candidate()
    candidate["runtime_retirement_context"] = json.dumps(context)
    receipt_thread = dict(current)
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value=receipt_thread)
    recover = AsyncMock(return_value=True)
    thread_ops = _ThreadRetirementFake()
    pinned_ops = _pinned_operations(recover=recover)
    pinned_ops.retirement_has_exact_local_quiescence = MagicMock(return_value=True)

    assert await detector.retry_pending_pinned_retirement(
        candidate,
        dependencies=_detector_dependencies(
            db,
            thread_retirement_operations=lambda: thread_ops,
            pinned_retirement_operations=lambda: pinned_ops,
        ),
    )

    recover.assert_not_awaited()
    pinned_ops.retirement_has_exact_local_quiescence.assert_called_once_with(
        {
            "generation": candidate["runtime_generation"],
            "token": candidate["runtime_retirement_token"],
            "permanent": False,
            "context": context,
        },
        receipt_thread,
    )
    thread_ops.end_thread_flow.assert_awaited_once_with(
        context["thread_id"],
        receipt_thread,
        permanent=False,
        force=True,
        expected_runtime_generation=candidate["runtime_generation"],
        expected_agent_id=context["agent_id"],
        expected_attach_token=context["runtime_attach_token"],
        require_expected_agent_offline=False,
        settle_status="ended",
        local_runtime_quiesced=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("permanent", "status", "expected"),
    [
        (False, "ended", True),
        (False, "deleted", False),
        (False, "suspended", False),
        (True, "deleted", True),
        (True, "ended", False),
    ],
)
async def test_retry_success_requires_the_exact_disposition(
    permanent, status, expected
):
    candidate, context, current = _retry_candidate(
        runtime_retirement_permanent=permanent
    )
    context["runtime_authority_exposed"] = False
    for key in ("agent_id", "runtime_attach_token", "agent_pod"):
        context.pop(key)
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value=current)
    thread_ops = _ThreadRetirementFake(AsyncMock(return_value={"status": status}))

    assert (
        await detector.retry_pending_pinned_retirement(
            candidate,
            dependencies=_detector_dependencies(
                db, thread_retirement_operations=lambda: thread_ops
            ),
        )
        is expected
    )
    kwargs = thread_ops.end_thread_flow.await_args.kwargs
    assert kwargs["permanent"] is permanent
    assert kwargs["local_runtime_quiesced"] is False
    assert kwargs["expected_agent_id"] is None
    assert kwargs["expected_attach_token"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_soft_settlement", [True, False])
async def test_permanent_retry_reuses_a_prior_soft_settlement(prior_soft_settlement):
    candidate, context, current = _retry_candidate(runtime_retirement_permanent=True)
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value=current)
    db.pinned_thread_has_prior_soft_settlement = AsyncMock(
        return_value=prior_soft_settlement
    )
    recover = AsyncMock(return_value=False)
    thread_ops = _ThreadRetirementFake(AsyncMock(return_value={"status": "deleted"}))
    pinned_ops = _pinned_operations(recover=recover)

    result = await detector.retry_pending_pinned_retirement(
        candidate,
        dependencies=_detector_dependencies(
            db,
            thread_retirement_operations=lambda: thread_ops,
            pinned_retirement_operations=lambda: pinned_ops,
        ),
    )

    db.pinned_thread_has_prior_soft_settlement.assert_awaited_once_with(
        context["thread_id"],
        runtime_generation=candidate["runtime_generation"],
        retirement_token=candidate["runtime_retirement_token"],
    )
    if prior_soft_settlement:
        assert result is True
        recover.assert_not_awaited()
        assert thread_ops.end_thread_flow.await_args.kwargs["permanent"] is True
    else:
        assert result is False
        recover.assert_awaited_once()
        thread_ops.end_thread_flow.assert_not_awaited()


@pytest.mark.asyncio
async def test_soft_retry_never_consults_the_prior_soft_settlement():
    candidate, _context, current = _retry_candidate()
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value=current)
    db.pinned_thread_has_prior_soft_settlement = AsyncMock(return_value=True)
    recover = AsyncMock(return_value=False)
    pinned_ops = _pinned_operations(recover=recover)

    assert not await detector.retry_pending_pinned_retirement(
        candidate,
        dependencies=_detector_dependencies(
            db, pinned_retirement_operations=lambda: pinned_ops
        ),
    )

    db.pinned_thread_has_prior_soft_settlement.assert_not_awaited()
    recover.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("after_recovery", "expected"),
    [
        (None, True),
        ({"runtime_retirement_token": "other", "status": "ended"}, True),
        ({"runtime_retirement_token": "other", "status": "suspended"}, True),
        ({"runtime_retirement_token": "other", "status": "active"}, False),
    ],
)
async def test_retry_after_recovery_reads_the_settled_row(after_recovery, expected):
    """Crash recovery may itself settle the row; the retry then reports it
    without a second End."""

    candidate, _context, current = _retry_candidate()
    db = AsyncMock()
    db.get_thread = AsyncMock(side_effect=[current, after_recovery])
    thread_ops = _ThreadRetirementFake()
    pinned_ops = _pinned_operations(recover=AsyncMock(return_value=True))

    assert (
        await detector.retry_pending_pinned_retirement(
            candidate,
            dependencies=_detector_dependencies(
                db,
                thread_retirement_operations=lambda: thread_ops,
                pinned_retirement_operations=lambda: pinned_ops,
            ),
        )
        is expected
    )
    assert db.get_thread.await_count == 2
    thread_ops.end_thread_flow.assert_not_awaited()


@pytest.mark.asyncio
async def test_retry_after_recovery_ends_the_reread_row():
    candidate, context, current = _retry_candidate()
    reread = {**current, "status": "ending"}
    db = AsyncMock()
    db.get_thread = AsyncMock(side_effect=[current, reread])
    thread_ops = _ThreadRetirementFake()
    pinned_ops = _pinned_operations(recover=AsyncMock(return_value=True))

    assert await detector.retry_pending_pinned_retirement(
        candidate,
        dependencies=_detector_dependencies(
            db,
            thread_retirement_operations=lambda: thread_ops,
            pinned_retirement_operations=lambda: pinned_ops,
        ),
    )

    assert thread_ops.end_thread_flow.await_args.args == (context["thread_id"], reread)
    assert thread_ops.end_thread_flow.await_args.kwargs["local_runtime_quiesced"]


@pytest.mark.asyncio
async def test_retry_treats_503_as_refusal_and_raises_other_http_errors(caplog):
    candidate, _context, current = _retry_candidate()
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value=current)
    pinned_ops = _pinned_operations()
    pinned_ops.retirement_has_exact_local_quiescence = MagicMock(return_value=True)
    caplog.set_level(logging.WARNING, logger=detector.__name__)

    retryable = _ThreadRetirementFake(
        AsyncMock(side_effect=HTTPException(status_code=503, detail="cleanup busy"))
    )
    assert not await detector.retry_pending_pinned_retirement(
        candidate,
        dependencies=_detector_dependencies(
            db,
            thread_retirement_operations=lambda: retryable,
            pinned_retirement_operations=lambda: pinned_ops,
        ),
    )
    assert [r.getMessage() for r in caplog.records] == [
        "Durable pinned retirement retry refused for thread "
        f"{candidate['id']} (HTTP 503): cleanup busy"
    ]

    broken = _ThreadRetirementFake(
        AsyncMock(side_effect=HTTPException(status_code=500, detail="bug"))
    )
    with pytest.raises(HTTPException) as raised:
        await detector.retry_pending_pinned_retirement(
            candidate,
            dependencies=_detector_dependencies(
                db,
                thread_retirement_operations=lambda: broken,
                pinned_retirement_operations=lambda: pinned_ops,
            ),
        )
    assert raised.value.status_code == 500


def test_pinned_retirement_graces_keep_their_env_names_defaults_and_floors(
    monkeypatch,
):
    import importlib.util
    import sys

    def load():
        spec = importlib.util.spec_from_file_location(
            "_stale_agent_detector_env_probe", detector.__file__
        )
        module = importlib.util.module_from_spec(spec)
        # A dataclass resolves its string annotations through sys.modules.
        monkeypatch.setitem(sys.modules, spec.name, module)
        spec.loader.exec_module(module)
        return module

    for name in (
        "PINNED_RETIREMENT_RETRY_GRACE_SECONDS",
        "PINNED_RETIREMENT_PREFLIGHT_GRACE_SECONDS",
        "PINNED_RETIREMENT_PROVEN_RETRY_GRACE_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)
    defaults = load()
    assert defaults.PINNED_RETIREMENT_RETRY_GRACE_SECONDS == 900
    assert defaults.PINNED_RETIREMENT_PREFLIGHT_GRACE_SECONDS == 300
    assert defaults.PINNED_RETIREMENT_PROVEN_RETRY_GRACE_SECONDS == 60

    monkeypatch.setenv("PINNED_RETIREMENT_RETRY_GRACE_SECONDS", "-5")
    monkeypatch.setenv("PINNED_RETIREMENT_PREFLIGHT_GRACE_SECONDS", "0")
    monkeypatch.setenv("PINNED_RETIREMENT_PROVEN_RETRY_GRACE_SECONDS", "-1")
    floored = load()
    assert floored.PINNED_RETIREMENT_RETRY_GRACE_SECONDS == 0
    assert floored.PINNED_RETIREMENT_PREFLIGHT_GRACE_SECONDS == 1
    assert floored.PINNED_RETIREMENT_PROVEN_RETRY_GRACE_SECONDS == 0

    monkeypatch.setenv("PINNED_RETIREMENT_RETRY_GRACE_SECONDS", "1200")
    monkeypatch.setenv("PINNED_RETIREMENT_PREFLIGHT_GRACE_SECONDS", "45")
    monkeypatch.setenv("PINNED_RETIREMENT_PROVEN_RETRY_GRACE_SECONDS", "7")
    configured = load()
    assert configured.PINNED_RETIREMENT_RETRY_GRACE_SECONDS == 1200
    assert configured.PINNED_RETIREMENT_PREFLIGHT_GRACE_SECONDS == 45
    assert configured.PINNED_RETIREMENT_PROVEN_RETRY_GRACE_SECONDS == 7


def test_detector_dependencies_are_a_frozen_explicit_port():
    import dataclasses

    fields = [
        field.name for field in dataclasses.fields(StaleAgentDetectorDependencies)
    ]
    assert fields == [
        "store",
        "agent_provisioner",
        "docker_provisioner",
        "audit_reader",
        "completion_commands_enabled",
        "trigger_dispatch",
        "schedule_attach_abort_successor",
        "thread_retirement_operations",
        "pinned_retirement_operations",
    ]
    dependencies = _detector_dependencies(AsyncMock())
    with pytest.raises(dataclasses.FrozenInstanceError):
        dependencies.store = None
    assert not hasattr(dependencies, "__dict__")
