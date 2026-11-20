"""A workspace Pod killed for memory is held as one, in either order.

knowledge-base/knowledge/issues/workspace_out_of_memory_is_reported_as_transport_lost.md:
the cleanup path reads the dead Pod's ``workspace`` container and settles the
record onto the job row (``workspace_container.last_termination``). A recorded
``OOMKilled`` words the hold as a memory kill, with the limit and what the
next Resume does: it runs with twice the limit once (``memory_growth``, read
by the Pod creator), up to ``WORKSPACE_MEMORY_GROW_MAX``. A bare exit 137 keeps
the transport-lost wording.

Real PostgreSQL with the full migration chain; only Kubernetes is faked. The
report-first order runs through the recovery arm, its Pod read and its
cleanup; the reconciler-first order through the production reconciler tick.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock

import pytest
from kubernetes import client

from orchestrator.services.completion import handle_pod_workspace_recovery
from orchestrator.services.completion_finalizer import CompletionFinalizer
from shared.orch_surface.formatters import format_job_detail
from shared.workspace_disk import (
    MORE_MEMORY_HINT,
    WORKSPACE_OUT_OF_MEMORY_REASON,
    WorkspaceTermination,
)
from tests import test_workspace_report_after_automatic_cleanup_real_postgres as cases

pg_dsn = cases.pg_dsn
_schema_applied = cases._schema_applied
db = cases.db
current_job = cases.current_job

FINISHED = datetime(2026, 11, 20, 20, 18, 50, tzinfo=timezone.utc)
KEPT = (
    "Files under /home/agent-host are kept; running processes and other paths "
    "such as /tmp are gone. The job's saved progress is kept."
)
OOM_ACTION = (
    "workspace ran out of memory (limit 512 MiB) and was stopped; interrupted "
    "command outcome is unknown; explicit Resume required. Resume raises the "
    f"job's memory limit to 1 GiB. {KEPT}"
)


def _kill_pod(
    resources,
    *,
    reason="OOMKilled",
    exit_code=137,
    memory="512Mi",
    deleted_before=False,
):
    """The fake cluster's Failed Pod, ended the way the case needs."""

    pod = resources["pod"]
    pod.spec.containers[0].resources = client.V1ResourceRequirements(
        limits={"memory": memory, "cpu": "2"},
        requests={"memory": memory, "cpu": "500m"},
    )
    pod.status.container_statuses[0].state = client.V1ContainerState(
        terminated=client.V1ContainerStateTerminated(
            exit_code=exit_code, reason=reason, finished_at=FINISHED
        )
    )
    if deleted_before:
        pod.metadata.deletion_timestamp = FINISHED - timedelta(seconds=10)
    return pod


def _recover(db, job, runner, provisioner, *, read=True):
    owner_runtime = job["context"]["workspace_container"]["_runtime_incarnation"]

    async def read_termination():
        return await provisioner.read_workspace_termination(
            cases.WorkspaceOwner.job(str(job["id"])),
            expected_runtime_incarnation=owner_runtime,
        )

    return handle_pod_workspace_recovery(
        job,
        str(job["id"]),
        cases.REPORT,
        db=db,
        delete_workspace=AsyncMock(side_effect=AssertionError("untyped deletion")),
        cleanup_service=provisioner,
        trigger_dispatch=Mock(),
        probe=AsyncMock(return_value=False),
        completion_command_id=runner.command_id,
        completion_finalizing_by=runner.owner,
        read_termination=read_termination if read else AsyncMock(return_value=None),
    )


def _assert_memory_hold(held, runtime, *, action=OOM_ACTION, limit=512 << 20):
    assert held["status"] == "paused"
    assert held["error_message"] == action
    assert "transport lost" not in held["error_message"]
    hold = held["context"]["_operator_pause_hold"]
    # The source stays: the cleanup receipt and the Resume storage check match it.
    assert hold["source"] == "workspace_recovery_unavailable"
    assert hold["attention"]["reason"] == WORKSPACE_OUT_OF_MEMORY_REASON
    assert hold["attention"]["runtime_replaced"] is True
    assert hold["attention"]["volume_kept"] is True
    workspace = held["context"]["workspace_container"]
    record = WorkspaceTermination.from_dict(workspace["last_termination"])
    assert record.pod_uid == runtime
    assert record.cause == "out_of_memory"
    assert record.memory_limit_bytes == limit
    return hold, workspace


@pytest.mark.asyncio
async def test_report_first_names_the_memory_kill_from_the_pod_read(db, monkeypatch):
    monkeypatch.delenv("WORKSPACE_MEMORY_GROW_MAX", raising=False)
    original, owner, runtime, provisioner, resources = await cases._stateless_job(
        db, monkeypatch
    )
    _kill_pod(resources)
    runner = await cases._accept_report(db, original["id"])

    outcome = await _recover(db, original, runner, provisioner)

    assert outcome["actions"] == [OOM_ACTION]
    assert outcome["held_for_resume"] is True and outcome["cleanup_pending"] is False
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    held = await current_job(db, original["id"])
    hold, workspace = _assert_memory_hold(held, runtime)
    assert hold["attention"]["memory"] == {
        "limit_bytes": 512 << 20,
        "exit_code": 137,
        "container_reason": "OOMKilled",
        "growth": {"outcome": "granted", "limit_bytes": 1 << 30},
    }
    assert workspace["memory_growth"]["limit"] == "1Gi"
    assert workspace["memory_growth"]["from_bytes"] == 512 << 20
    assert workspace["memory_growth"]["runtime_incarnation"] == runtime
    assert workspace["status"] == "deleted" and "pod" not in resources
    assert workspace["recovery_completion_outcome"]["actions"] == [OOM_ACTION]
    detail = format_job_detail(held)
    assert detail.count(KEPT) == 1
    assert "transport" not in detail


@pytest.mark.asyncio
async def test_report_first_upgrades_the_hold_from_the_settled_record(
    db, monkeypatch, tmp_path
):
    """The report came before the kubelet published the Pod's end: the hold
    starts as transport lost and the cleanup's own record upgrades it. Resume
    then starts the new Pod with the raised limit."""

    monkeypatch.delenv("WORKSPACE_MEMORY_GROW_MAX", raising=False)
    original, owner, runtime, provisioner, resources = await cases._stateless_job(
        db, monkeypatch
    )
    # The fixture job has no template: its Pods get the default 4Gi limit.
    _kill_pod(resources, memory="4Gi")
    runner = await cases._accept_report(db, original["id"])

    outcome = await _recover(db, original, runner, provisioner, read=False)

    action = OOM_ACTION.replace("512 MiB", "4 GiB").replace("to 1 GiB", "to 8 GiB")
    assert outcome["actions"] == [action]
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    held = await current_job(db, original["id"])
    _, workspace = _assert_memory_hold(held, runtime, action=action, limit=4 << 30)
    assert workspace["memory_growth"]["limit"] == "8Gi"

    # Resume, then the dispatcher's creator: the record survives both, and
    # the new Pod runs with the raised limit and the old request.
    successor, cluster = await cases._resume_on_a_new_pod(
        db, monkeypatch, tmp_path, owner, held, resources["pvc"]
    )
    new_workspace = successor["context"]["workspace_container"]
    assert new_workspace["status"] == "ready"
    assert new_workspace["_runtime_incarnation"] != runtime
    assert new_workspace["memory_growth"] == workspace["memory_growth"]
    assert new_workspace["last_termination"] == workspace["last_termination"]
    pod = cluster.objects["pod"]
    (container,) = [c for c in pod.spec.containers if c.name == "workspace"]
    resources_spec = container.resources
    if not isinstance(resources_spec, dict):
        resources_spec = resources_spec.to_dict()
    # The limit doubled from the killed 4Gi; the request stays the default 1Gi.
    assert resources_spec["limits"]["memory"] == "8Gi"
    assert resources_spec["requests"]["memory"] == "1Gi"


@pytest.mark.asyncio
async def test_reconciler_first_holds_as_a_memory_kill(db, monkeypatch):
    monkeypatch.delenv("WORKSPACE_MEMORY_GROW_MAX", raising=False)
    original, owner, runtime, provisioner, resources = await cases._stateless_job(
        db, monkeypatch
    )
    _kill_pod(resources)

    stats = await cases._reconciler(db, provisioner).tick()
    assert stats["workspace"]["unhealthy"] == 1
    retired = await current_job(db, original["id"])
    assert retired["context"]["workspace_container"]["status"] == "deleted"
    # Settlement recorded the end; the job is not held yet.
    assert (
        WorkspaceTermination.from_dict(
            retired["context"]["workspace_container"]["last_termination"]
        ).cause
        == "out_of_memory"
    )
    assert "memory_growth" not in retired["context"]["workspace_container"]

    runner = await cases._accept_report(db, original["id"])
    outcome = await _recover(db, retired, runner, provisioner)

    assert outcome["actions"] == [OOM_ACTION]
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    held = await current_job(db, original["id"])
    _, workspace = _assert_memory_hold(held, runtime)
    assert workspace["memory_growth"]["limit"] == "1Gi"
    assert len(await cases._intents(db, original["id"])) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("order", ["report_first", "reconciler_first"])
async def test_a_bare_exit_137_keeps_the_transport_wording(db, monkeypatch, order):
    """Error/137 is also a liveness kill or a lost runtime event: no cause."""

    original, owner, runtime, provisioner, resources = await cases._stateless_job(
        db, monkeypatch
    )
    _kill_pod(resources, reason="Error")
    job = original
    if order == "reconciler_first":
        await cases._reconciler(db, provisioner).tick()
        job = await current_job(db, original["id"])
    runner = await cases._accept_report(db, original["id"])

    outcome = await _recover(db, job, runner, provisioner)

    assert outcome["actions"] == [cases.DEAD_ARM_ACTION]
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    held = await current_job(db, original["id"])
    assert held["error_message"] == cases.DEAD_ARM_ACTION
    attention = held["context"]["_operator_pause_hold"]["attention"]
    assert attention["reason"] == "interrupted_command_outcome_unknown"
    assert "memory" not in attention
    workspace = held["context"]["workspace_container"]
    record = WorkspaceTermination.from_dict(workspace["last_termination"])
    # The raw facts are kept: a memory kill and a network loss can be told apart.
    assert (record.container_reason, record.exit_code) == ("Error", 137)
    assert record.cause == "not_recorded"
    assert "memory_growth" not in workspace


@pytest.mark.asyncio
async def test_a_container_srw_stopped_is_not_a_memory_kill(db, monkeypatch):
    original, owner, runtime, provisioner, resources = await cases._stateless_job(
        db, monkeypatch
    )
    _kill_pod(resources, deleted_before=True)
    runner = await cases._accept_report(db, original["id"])

    outcome = await _recover(db, original, runner, provisioner)

    assert outcome["actions"] == [cases.DEAD_ARM_ACTION]
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    workspace = (await current_job(db, original["id"]))["context"][
        "workspace_container"
    ]
    assert "memory_growth" not in workspace


@pytest.mark.asyncio
async def test_a_second_kill_does_not_grow_again(db, monkeypatch):
    monkeypatch.delenv("WORKSPACE_MEMORY_GROW_MAX", raising=False)
    original, owner, runtime, provisioner, resources = await cases._stateless_job(
        db, monkeypatch
    )
    earlier = {
        "limit": "512Mi",
        "limit_bytes": 512 << 20,
        "from_bytes": 256 << 20,
        "runtime_incarnation": "00000000-0000-4000-8000-000000000001",
        "at": "2026-11-20T19:00:00+00:00",
    }
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{workspace_container,memory_growth}',"
        "$2::jsonb) WHERE id=$1",
        original["id"],
        __import__("json").dumps(earlier),
    )
    original = await current_job(db, original["id"])
    _kill_pod(resources)
    runner = await cases._accept_report(db, original["id"])

    outcome = await _recover(db, original, runner, provisioner)

    action = (
        "workspace ran out of memory again (limit 512 MiB, already raised once) "
        "and was stopped; interrupted command outcome is unknown; explicit Resume "
        f"required. {MORE_MEMORY_HINT} {KEPT}"
    )
    assert outcome["actions"] == [action]
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    held = await current_job(db, original["id"])
    hold, workspace = _assert_memory_hold(held, runtime, action=action)
    assert hold["attention"]["memory"]["growth"]["outcome"] == "used"
    assert workspace["memory_growth"] == earlier


@pytest.mark.asyncio
async def test_growth_off_gives_the_hint(db, monkeypatch):
    monkeypatch.setenv("WORKSPACE_MEMORY_GROW_MAX", "")
    original, owner, runtime, provisioner, resources = await cases._stateless_job(
        db, monkeypatch
    )
    _kill_pod(resources)
    runner = await cases._accept_report(db, original["id"])

    outcome = await _recover(db, original, runner, provisioner)

    action = (
        "workspace ran out of memory (limit 512 MiB) and was stopped; interrupted "
        "command outcome is unknown; explicit Resume required. "
        f"{MORE_MEMORY_HINT} {KEPT}"
    )
    assert outcome["actions"] == [action]
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    held = await current_job(db, original["id"])
    _, workspace = _assert_memory_hold(held, runtime, action=action)
    assert "memory_growth" not in workspace


@pytest.mark.asyncio
async def test_a_resume_with_feedback_tells_the_agent_why_and_its_new_limit(
    db, monkeypatch, tmp_path
):
    """WP4, first half: the feedback banner is worded from the lifted hold."""

    from orchestrator.schemas.job_controls import JobResumeRequest
    from tests import test_workspace_pull_failure_real_postgres as creator_cases

    monkeypatch.delenv("WORKSPACE_MEMORY_GROW_MAX", raising=False)
    original, owner, runtime, provisioner, resources = await cases._stateless_job(
        db, monkeypatch
    )
    _kill_pod(resources, memory="4Gi")
    runner = await cases._accept_report(db, original["id"])
    outcome = await _recover(db, original, runner, provisioner)
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    held = await current_job(db, original["id"])

    cluster = creator_cases.NeverPullingCluster()
    cluster.objects["pvc"] = resources["pvc"]
    provider = creator_cases._provisioner(monkeypatch, db, cluster)
    operations = cases._job_controls(db, tmp_path, provider)
    await operations.resume_job_internal(
        owner.id,
        user={"id": "00000000-0000-4000-8000-0000000000aa"},
        job=held,
        request=JobResumeRequest(feedback="Use the small model this time."),
    )

    queued = (await current_job(db, original["id"]))["context"]
    assert "_operator_pause_hold" not in queued
    assert queued["queued_feedback"] == "Use the small model this time."
    assert queued["queued_feedback_reason"] == (
        "This job's workspace ran out of memory (limit 4 GiB) and was stopped; the "
        "outcome of the command that was running is unknown. Its memory limit is "
        f"now 8 GiB. {KEPT} An operator resumed it with the feedback below."
    )
