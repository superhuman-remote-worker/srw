"""The stateless executor's process-zero retention finalizer.

Claimant-loss debt settles only on the exact claimant UID observed with every
container terminated; the finalizer keeps that object readable until the
orchestrator has recorded the proof. These tests pin the release predicate,
the exact patch, and the reaper's ordering: a finalizer nobody removes would
block every rollout and scale-down, and one removed too early reopens the
deadlock (stateless_claim_loss_hold_never_settles_without_pod_finalizer).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from orchestrator.routers import run_queue_admin as run_queue_admin_router
from orchestrator.services import agent_provisioner as provisioner_module
from orchestrator.services import run_queue_admin
from orchestrator.services import run_queue_reaper as reaper
from orchestrator.services.agent_provisioner import (
    STATELESS_EXECUTOR_PROCESS_ZERO_FINALIZER as FINALIZER,
    AgentProvisioner,
    stateless_executor_process_zero,
)
from orchestrator.services.pinned_k8s_effect import (
    PINNED_AUTHORITY_FINALIZER,
    finalizer_release_patch,
)
from shared.session_retirement import ClaimantAuthority
from tests._fake_executor_k8s import (
    EXECUTOR_NAMESPACE,
    POOL_INSTANCE,
    POOL_NAME,
    FakeExecutorCoreApi,
    container_status,
    executor_pod,
)


@pytest.fixture
def pool_identity(monkeypatch):
    monkeypatch.setenv("AGENT_LABEL_NAME", POOL_NAME)
    monkeypatch.setenv("AGENT_LABEL_INSTANCE", POOL_INSTANCE)


def _provisioner(api: FakeExecutorCoreApi) -> AgentProvisioner:
    provisioner = AgentProvisioner()
    provisioner._core_api = api
    provisioner._k8s_available = True
    provisioner._namespace = EXECUTOR_NAMESPACE
    return provisioner


def _terminated(pod: SimpleNamespace) -> SimpleNamespace:
    api = FakeExecutorCoreApi(pod)
    api.delete_namespaced_pod(pod.metadata.name, EXECUTOR_NAMESPACE)
    api.kubelet_terminates(pod.metadata.name)
    return api.pods[pod.metadata.name]


# --------------------------------------------------------------------------- #
# Release predicate
# --------------------------------------------------------------------------- #


def test_release_patch_default_stays_the_pinned_finalizer():
    assert finalizer_release_patch(
        uid="u", resource_version="7", finalizers=[PINNED_AUTHORITY_FINALIZER]
    )[-1] == {"op": "replace", "path": "/metadata/finalizers", "value": []}
    assert (
        finalizer_release_patch(uid="u", resource_version="7", finalizers=[FINALIZER])
        is None
    )


def test_terminal_deleting_executor_is_process_zero():
    assert stateless_executor_process_zero(
        _terminated(executor_pod(finalizers=[FINALIZER]))
    )


@pytest.mark.parametrize(
    "pod",
    [
        # Live: the finalizer must never be released from a serving executor.
        executor_pod(finalizers=[FINALIZER]),
        # Deleting but still draining: SIGTERM ACK may still be in flight.
        executor_pod(finalizers=[FINALIZER], deleting=True),
        # A lost node: the node controller flips phase, statuses stay frozen.
        executor_pod(finalizers=[FINALIZER], deleting=True, phase="Unknown"),
        # Even a terminal phase is not proof while any container shows running.
        executor_pod(finalizers=[FINALIZER], deleting=True, phase="Failed"),
    ],
)
def test_running_or_live_executor_is_never_process_zero(pod):
    assert not stateless_executor_process_zero(pod)


def test_terminated_status_without_deletion_is_not_release_evidence():
    # restartPolicy Always: a crashed container restarts in the same Pod.
    pod = executor_pod(
        finalizers=[FINALIZER], containers=(("agent", "terminated"),), phase="Running"
    )
    assert not stateless_executor_process_zero(pod)


def test_running_ephemeral_debug_container_blocks_release():
    pod = _terminated(executor_pod(finalizers=[FINALIZER]))
    pod.spec.ephemeral_containers = [SimpleNamespace(name="debugger")]
    pod.status.ephemeral_container_statuses = [container_status("debugger", "running")]
    assert not stateless_executor_process_zero(pod)


def test_deleting_unscheduled_executor_never_ran_a_process():
    pod = executor_pod(
        finalizers=[FINALIZER],
        deleting=True,
        phase="Pending",
        node_name=None,
        init=(),
        containers=(("agent", "waiting"),),
    )
    pod.status.init_container_statuses = []
    pod.status.container_statuses = []
    assert stateless_executor_process_zero(pod)
    pod.spec.node_name = "node-a"
    assert not stateless_executor_process_zero(pod)


def test_unscheduled_executor_stays_releasable_after_podgc_flips_its_phase():
    # KEDA scale-down / a rollout deletes an Unschedulable surge Pod; PodGC's
    # unscheduled-terminating sweep sets phase Failed before its force delete.
    pod = executor_pod(
        finalizers=[FINALIZER], deleting=True, phase="Failed", node_name=None
    )
    pod.status.init_container_statuses = None
    pod.status.container_statuses = None
    pod.status.conditions = [
        SimpleNamespace(type="PodScheduled", reason="Unschedulable")
    ]
    assert stateless_executor_process_zero(pod)


def test_debug_container_that_never_started_does_not_retain_the_pod():
    # kubelet's terminal rewrite never touches ephemeral statuses.
    pod = _terminated(executor_pod(finalizers=[FINALIZER]))
    pod.spec.ephemeral_containers = [SimpleNamespace(name="debugger")]
    pod.status.ephemeral_container_statuses = [
        container_status("debugger", "waiting", reason="ImagePullBackOff")
    ]
    assert stateless_executor_process_zero(pod)
    pod.status.ephemeral_container_statuses[0].restart_count = 1
    assert not stateless_executor_process_zero(pod)


def _killed_during_init() -> SimpleNamespace:
    """The kubelet's view of a Pod deleted inside wait-for-orchestrator."""

    return executor_pod(
        finalizers=[FINALIZER],
        deleting=True,
        phase="Failed",
        init=(("wait-for-orchestrator", "terminated", 137),),
        containers=(("agent", "waiting"),),
    )


def test_executor_killed_during_init_never_started_the_claim_loop():
    assert stateless_executor_process_zero(_killed_during_init())


def test_podgc_terminal_phase_is_not_kubelet_evidence():
    pod = _killed_during_init()
    pod.status.conditions = [
        SimpleNamespace(type="DisruptionTarget", reason="DeletionByPodGC")
    ]
    assert not stateless_executor_process_zero(pod)


def test_waiting_container_after_a_successful_init_is_ambiguous():
    pod = _killed_during_init()
    pod.status.init_container_statuses[0].state.terminated.exit_code = 0
    assert not stateless_executor_process_zero(pod)


def test_waiting_container_with_start_evidence_is_ambiguous():
    pod = _killed_during_init()
    pod.status.container_statuses[0].restart_count = 1
    assert not stateless_executor_process_zero(pod)
    pod = _killed_during_init()
    pod.status.container_statuses[0].state.waiting.reason = "CrashLoopBackOff"
    assert not stateless_executor_process_zero(pod)
    pod = _killed_during_init()
    pod.status.phase = "Pending"
    assert not stateless_executor_process_zero(pod)


# --------------------------------------------------------------------------- #
# AgentProvisioner list / release
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_retained_list_is_pool_scoped_and_fails_closed(
    pool_identity, monkeypatch
):
    retained = executor_pod(name="a", uid="uid-a", finalizers=[FINALIZER])
    unprotected = executor_pod(name="b", uid="uid-b", finalizers=None)
    live = executor_pod(name="c", uid="uid-c", finalizers=[FINALIZER])
    other_release = executor_pod(
        name="d",
        uid="uid-d",
        finalizers=[FINALIZER],
        labels={"app.kubernetes.io/instance": "other"},
    )
    # Created by this release before a chart rename changed its name label.
    renamed_chart = executor_pod(
        name="e",
        uid="uid-e",
        finalizers=[FINALIZER],
        labels={"app.kubernetes.io/name": "superhuman-remote-worker-old"},
    )
    api = FakeExecutorCoreApi(retained, unprotected, live, other_release, renamed_chart)
    for name in ("a", "b", "d", "e"):
        api.delete_namespaced_pod(name, EXECUTOR_NAMESPACE)
    provisioner = _provisioner(api)

    pods = await provisioner.list_retained_stateless_executor_pods()
    assert [pod.metadata.name for pod in pods] == ["a", "e"]

    monkeypatch.delenv("AGENT_LABEL_INSTANCE")
    assert await provisioner.list_retained_stateless_executor_pods() is None
    provisioner._k8s_available = False
    assert await provisioner.list_retained_stateless_executor_pods() is None


@pytest.mark.asyncio
async def test_retention_blindness_is_warned_once_per_reason(
    pool_identity, monkeypatch, caplog
):
    provisioner = _provisioner(FakeExecutorCoreApi())
    provisioner._k8s_available = False
    with caplog.at_level(logging.WARNING, logger=provisioner_module.logger.name):
        await provisioner.list_retained_stateless_executor_pods()
        await provisioner.list_retained_stateless_executor_pods()
        provisioner._k8s_available = True
        monkeypatch.delenv("AGENT_LABEL_INSTANCE")
        await provisioner.list_retained_stateless_executor_pods()
        await provisioner.list_retained_stateless_executor_pods()
    messages = [record.getMessage() for record in caplog.records]
    assert len(messages) == 2
    assert "no Kubernetes client" in messages[0]
    assert "AGENT_LABEL_INSTANCE" in messages[1]


@pytest.mark.asyncio
async def test_claimant_reads_are_bounded(pool_identity):
    api = FakeExecutorCoreApi(executor_pod(finalizers=[FINALIZER]))
    seen: list[dict] = []
    original = api.read_namespaced_pod

    def read(name, namespace, **kwargs):
        seen.append(kwargs)
        return original(name, namespace, **kwargs)

    api.read_namespaced_pod = read
    provisioner = _provisioner(api)
    await provisioner.agent_pod_authority(
        "srw-agent-stateless-68d8c97ff6-w97cz",
        expected_pod_uid="072ccef4-0000-4000-8000-000000000001",
    )
    assert (
        seen[0]["_request_timeout"]
        == provisioner_module.STATELESS_EXECUTOR_READ_TIMEOUT
    )


@pytest.mark.asyncio
async def test_release_removes_only_our_finalizer_from_the_observed_object(
    pool_identity,
):
    pod = executor_pod(finalizers=["example.com/keep", FINALIZER])
    api = FakeExecutorCoreApi(pod)
    api.delete_namespaced_pod(pod.metadata.name, EXECUTOR_NAMESPACE)
    api.kubelet_terminates(pod.metadata.name)
    provisioner = _provisioner(api)
    [observed] = await provisioner.list_retained_stateless_executor_pods()

    assert await provisioner.release_stateless_executor_finalizer_exact(observed)

    [(_, patch)] = api.patches
    assert patch == [
        {"op": "test", "path": "/metadata/uid", "value": observed.metadata.uid},
        {
            "op": "test",
            "path": "/metadata/resourceVersion",
            "value": observed.metadata.resource_version,
        },
        {
            "op": "test",
            "path": "/metadata/finalizers",
            "value": ["example.com/keep", FINALIZER],
        },
        {
            "op": "replace",
            "path": "/metadata/finalizers",
            "value": ["example.com/keep"],
        },
    ]
    assert api.pods[pod.metadata.name].metadata.finalizers == ["example.com/keep"]


@pytest.mark.asyncio
async def test_release_refuses_a_stale_observation_or_a_live_executor(
    pool_identity,
):
    pod = executor_pod(finalizers=[FINALIZER])
    api = FakeExecutorCoreApi(pod)
    provisioner = _provisioner(api)
    live = await provisioner.read_stateless_executor_pod(
        pod.metadata.name, expected_pod_uid=pod.metadata.uid
    )
    assert not await provisioner.release_stateless_executor_finalizer_exact(
        live, require_process_zero=False
    )

    api.delete_namespaced_pod(pod.metadata.name, EXECUTOR_NAMESPACE)
    api.kubelet_terminates(pod.metadata.name)
    [observed] = await provisioner.list_retained_stateless_executor_pods()
    api.pods[pod.metadata.name].metadata.resource_version = "999"  # a later write

    assert not await provisioner.release_stateless_executor_finalizer_exact(observed)
    assert pod.metadata.name in api.pods

    assert (
        await provisioner.read_stateless_executor_pod(
            pod.metadata.name, expected_pod_uid="someone-else"
        )
        is None
    )


@pytest.mark.asyncio
async def test_only_an_attestation_releases_a_running_deleting_executor(
    pool_identity,
):
    pod = executor_pod(finalizers=[FINALIZER], deleting=True)
    provisioner = _provisioner(FakeExecutorCoreApi(pod))
    [observed] = await provisioner.list_retained_stateless_executor_pods()

    assert not await provisioner.release_stateless_executor_finalizer_exact(observed)
    assert await provisioner.release_stateless_executor_finalizer_exact(
        observed, require_process_zero=False
    )


# --------------------------------------------------------------------------- #
# Reaper
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_reaper_asks_the_database_only_about_process_zero_pods(
    pool_identity, monkeypatch
):
    settled = _terminated(
        executor_pod(name="settled", uid="uid-s", finalizers=[FINALIZER])
    )
    owed = _terminated(executor_pod(name="owed", uid="uid-o", finalizers=[FINALIZER]))
    draining = executor_pod(
        name="draining", uid="uid-d", finalizers=[FINALIZER], deleting=True
    )
    api = FakeExecutorCoreApi(settled, owed, draining)
    monkeypatch.setattr(provisioner_module, "agent_provisioner", _provisioner(api))
    conn = MagicMock()
    conn.fetch = AsyncMock(return_value=[{"pod_uid": "uid-s"}])

    assert await reaper.release_retained_executor_pods(conn) == 1

    sql, uids, names = conn.fetch.await_args.args
    assert sql == reaper._UNREFERENCED_EXECUTOR_UIDS_SQL
    assert (uids, names) == (["uid-o", "uid-s"], ["owed", "settled"])
    assert api.removed == ["settled"]
    assert {"owed", "draining"} <= set(api.pods)


@pytest.mark.asyncio
async def test_reaper_skips_the_database_when_nothing_is_retained(
    pool_identity, monkeypatch
):
    api = FakeExecutorCoreApi(executor_pod(finalizers=[FINALIZER]))
    monkeypatch.setattr(provisioner_module, "agent_provisioner", _provisioner(api))
    conn = MagicMock()
    conn.fetch = AsyncMock(side_effect=AssertionError("no DB read"))

    assert await reaper.release_retained_executor_pods(conn) == 0
    conn.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_reaper_release_failure_is_contained(pool_identity, monkeypatch):
    api = FakeExecutorCoreApi(_terminated(executor_pod(finalizers=[FINALIZER])))
    monkeypatch.setattr(provisioner_module, "agent_provisioner", _provisioner(api))
    conn = MagicMock()
    conn.fetch = AsyncMock(side_effect=RuntimeError("db blip"))

    assert await reaper.release_retained_executor_pods(conn) == 0
    assert api.removed == []


def _unreferenced_except(referenced: set[str]):
    async def fake(_conn, candidates):
        return {uid for uid in candidates if uid not in referenced}

    return fake


@pytest.mark.asyncio
async def test_referenced_pods_cannot_starve_the_release_cap(
    pool_identity, monkeypatch
):
    pods = [
        _terminated(
            executor_pod(
                name=f"srw-agent-stateless-aaaa-{i:05d}",
                uid=f"00000000-0000-4000-8000-{i:012d}",
                finalizers=[FINALIZER],
            )
        )
        for i in range(reaper.RETAINED_EXECUTOR_RELEASE_MAX_PODS + 3)
    ]
    api = FakeExecutorCoreApi(*pods)
    monkeypatch.setattr(provisioner_module, "agent_provisioner", _provisioner(api))
    # Everything LISTed first is still owed; only the tail is releasable.
    owed = {pod.metadata.uid for pod in pods[:-3]}
    monkeypatch.setattr(
        reaper, "unreferenced_executor_uids", _unreferenced_except(owed)
    )

    assert await reaper.release_retained_executor_pods(object()) == 3
    assert sorted(api.removed) == sorted(pod.metadata.name for pod in pods[-3:])

    # The cap bounds patches per pass, not candidates.
    more = [
        _terminated(
            executor_pod(name=f"x-{i}", uid=f"uid-x-{i}", finalizers=[FINALIZER])
        )
        for i in range(3)
    ]
    api = FakeExecutorCoreApi(*more)
    monkeypatch.setattr(provisioner_module, "agent_provisioner", _provisioner(api))
    monkeypatch.setattr(
        reaper, "unreferenced_executor_uids", _unreferenced_except(set())
    )
    assert await reaper.release_retained_executor_pods(object(), max_pods=2) == 2
    assert len(api.pods) == 1


def _lost_node_executor(**kwargs) -> SimpleNamespace:
    """Force-deleted by PodGC long ago; its kubelet never reported again."""

    pod = executor_pod(finalizers=[FINALIZER], deleting=True, phase="Failed", **kwargs)
    pod.metadata.deletion_timestamp = datetime.now(timezone.utc) - timedelta(hours=1)
    return pod


@pytest.mark.asyncio
@pytest.mark.parametrize("loss", ["node_deleted", "out_of_service"])
async def test_unowed_executor_on_a_lost_node_is_released(
    pool_identity, monkeypatch, caplog, loss
):
    pod = _lost_node_executor()
    api = FakeExecutorCoreApi(pod)
    if loss == "node_deleted":
        api.missing_nodes.add("node-a")
    else:
        api.out_of_service_nodes.add("node-a")
    monkeypatch.setattr(provisioner_module, "agent_provisioner", _provisioner(api))
    monkeypatch.setattr(
        reaper, "unreferenced_executor_uids", _unreferenced_except(set())
    )

    with caplog.at_level(logging.WARNING, logger=reaper.logger.name):
        assert await reaper.release_retained_executor_pods(object()) == 1
    assert api.removed == [pod.metadata.name]
    assert "lost-node evidence" in caplog.text


@pytest.mark.asyncio
async def test_lost_node_evidence_never_overrides_an_owed_claim(
    pool_identity, monkeypatch
):
    pod = _lost_node_executor()
    api = FakeExecutorCoreApi(pod)
    api.missing_nodes.add("node-a")
    monkeypatch.setattr(provisioner_module, "agent_provisioner", _provisioner(api))
    monkeypatch.setattr(
        reaper,
        "unreferenced_executor_uids",
        _unreferenced_except({pod.metadata.uid}),
    )

    assert await reaper.release_retained_executor_pods(object()) == 0
    assert pod.metadata.name in api.pods


@pytest.mark.asyncio
async def test_healthy_or_unreadable_node_keeps_a_frozen_executor(
    pool_identity, monkeypatch, caplog
):
    pod = _lost_node_executor()
    api = FakeExecutorCoreApi(pod)
    monkeypatch.setattr(provisioner_module, "agent_provisioner", _provisioner(api))
    monkeypatch.setattr(
        reaper, "unreferenced_executor_uids", _unreferenced_except(set())
    )

    assert await reaper.release_retained_executor_pods(object()) == 0  # healthy node
    api.node_read_error = 403
    with caplog.at_level(logging.WARNING, logger=provisioner_module.logger.name):
        assert await reaper.release_retained_executor_pods(object()) == 0
        assert await reaper.release_retained_executor_pods(object()) == 0
    assert caplog.text.count("Node reads are forbidden") == 1
    api.node_read_error = 500
    assert await reaper.release_retained_executor_pods(object()) == 0
    assert pod.metadata.name in api.pods


@pytest.mark.asyncio
async def test_node_is_not_consulted_inside_the_termination_grace(
    pool_identity, monkeypatch
):
    pod = executor_pod(finalizers=[FINALIZER], deleting=True)
    pod.metadata.deletion_timestamp = datetime.now(timezone.utc) + timedelta(minutes=3)
    api = FakeExecutorCoreApi(pod)
    api.missing_nodes.add("node-a")
    monkeypatch.setattr(provisioner_module, "agent_provisioner", _provisioner(api))
    monkeypatch.setattr(
        reaper, "unreferenced_executor_uids", _unreferenced_except(set())
    )

    assert await reaper.release_retained_executor_pods(object()) == 0
    assert api.node_reads == []


@pytest.mark.asyncio
async def test_reconciler_rotates_so_wedged_holds_cannot_starve_new_ones(monkeypatch):
    held = [f"{i:08d}-0000-4000-8000-000000000000" for i in range(5)]

    async def fetch(sql, limit, cursor):
        assert sql == reaper._CLAIM_LOSS_HOLD_CANDIDATES_SQL
        ids = [tid for tid in held if cursor is None or tid > cursor][:limit]
        return [
            {
                "id": tid,
                "metadata": {
                    "_stateless_claim_losses": {
                        "1": {"pod": f"pod-{tid}", "pod_uid": tid, "quiesced": False}
                    }
                },
            }
            for tid in ids
        ]

    conn = MagicMock()
    conn.fetch = AsyncMock(side_effect=fetch)
    # Every hold is a pre-finalizer 404: nothing ever settles.
    authority = AsyncMock(return_value="exact_absent")
    monkeypatch.setattr(
        provisioner_module.agent_provisioner, "agent_pod_authority", authority
    )
    monkeypatch.setattr(reaper, "_CLAIM_LOSS_RECONCILE_CURSOR", None)
    monkeypatch.setattr(reaper, "_ABSENT_CLAIMANT_ANOMALIES", set())

    for _ in range(3):
        await reaper.reconcile_claim_loss_holds(conn, max_threads=2)

    visited = [call.kwargs["expected_pod_uid"] for call in authority.await_args_list]
    assert set(visited) == set(held)
    assert visited[:2] == held[:2] and visited[2:4] == held[2:4]


@pytest.mark.asyncio
async def test_reap_cycle_releases_only_after_settlement(monkeypatch):
    order: list[str] = []
    monkeypatch.setattr(reaper, "reap_expired", AsyncMock(return_value=[]))
    monkeypatch.setattr(reaper, "retry_stale_interrupt_requests", AsyncMock())
    monkeypatch.setattr(reaper, "retry_stale_permission_requests", AsyncMock())
    monkeypatch.setattr(
        reaper,
        "reconcile_claim_loss_holds",
        AsyncMock(side_effect=lambda _conn: order.append("settle")),
    )
    monkeypatch.setattr(
        reaper,
        "release_retained_executor_pods",
        AsyncMock(side_effect=lambda _conn: order.append("release")),
    )

    await reaper.reap_cycle(MagicMock())

    assert order == ["settle", "release"]


@pytest.mark.asyncio
async def test_exact_terminal_settlement_records_its_evidence(monkeypatch):
    import shared.session_retirement as retirement

    conn = MagicMock()
    conn.fetch = AsyncMock(
        return_value=[
            {
                "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "metadata": {
                    "_stateless_claim_losses": {
                        "8": {"pod": "pod-1", "pod_uid": "uid-old", "quiesced": False}
                    }
                },
            }
        ]
    )
    monkeypatch.setattr(
        provisioner_module.agent_provisioner,
        "agent_pod_authority",
        AsyncMock(return_value="exact_terminal"),
    )
    ack = AsyncMock(return_value=True)
    monkeypatch.setattr(retirement, "acknowledge_session_claim_quiesced", ack)

    assert await reaper.reconcile_claim_loss_holds(conn) == 1
    assert ack.await_args.kwargs["quiesced_by"] == "pod_terminal"
    assert ack.await_args.kwargs["receipt"] == {"evidence": "exact_terminal"}


@pytest.mark.asyncio
async def test_absent_claimant_anomaly_is_reported_once(monkeypatch, caplog):
    monkeypatch.setattr(reaper, "_ABSENT_CLAIMANT_ANOMALIES", set())
    authority = ClaimantAuthority("pod-1", "uid-old")
    with caplog.at_level(logging.ERROR, logger=reaper.logger.name):
        reaper._report_absent_claimant("t-1", 8, authority, "exact_absent")
        reaper._report_absent_claimant("t-1", 8, authority, "exact_absent")
        reaper._report_absent_claimant("t-2", 3, authority, "replacement")
    messages = [record.getMessage() for record in caplog.records]
    assert len(messages) == 2
    assert "/api/admin/run-queue/t-1/attest-claimant-gone" in messages[0]


# --------------------------------------------------------------------------- #
# Admin route
# --------------------------------------------------------------------------- #


def test_attest_route_is_admin_gated_and_forwards_the_exact_identity(monkeypatch):
    admin = {"id": "admin-7"}
    require_admin = AsyncMock(return_value=admin)
    dependencies = run_queue_admin.RunQueueAdminDependencies(
        db=MagicMock(),
        require_admin=require_admin,
        completion_commands_enabled=lambda: False,
        get_completion_command_resolution=lambda: None,
    )
    attest = AsyncMock(return_value={"settled_lease_tokens": [1]})
    monkeypatch.setattr(run_queue_admin, "attest_claimant_gone", attest)
    app = FastAPI()
    app.include_router(run_queue_admin_router.router)
    app.state.run_queue_admin_dependencies_factory = lambda: dependencies
    client = TestClient(app)

    response = client.post(
        "/api/admin/run-queue/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/attest-claimant-gone",
        json={"pod": "srw-agent-stateless-x", "pod_uid": "uid-1", "reason": "gone"},
    )

    assert response.status_code == 200
    require_admin.assert_awaited_once()
    assert attest.await_args.kwargs == {
        "pod": "srw-agent-stateless-x",
        "pod_uid": "uid-1",
        "reason": "gone",
        "admin": admin,
        "dependencies": dependencies,
    }
    missing_reason = client.post(
        "/api/admin/run-queue/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/attest-claimant-gone",
        json={"pod": "p", "pod_uid": "u", "reason": ""},
    )
    assert missing_reason.status_code == 422


def test_release_only_route_is_admin_gated(monkeypatch):
    admin = {"id": "admin-7"}
    require_admin = AsyncMock(return_value=admin)
    dependencies = run_queue_admin.RunQueueAdminDependencies(
        db=MagicMock(),
        require_admin=require_admin,
        completion_commands_enabled=lambda: False,
        get_completion_command_resolution=lambda: None,
    )
    attest = AsyncMock(return_value={"finalizer_released": True})
    monkeypatch.setattr(run_queue_admin, "attest_executor_pod_gone", attest)
    app = FastAPI()
    app.include_router(run_queue_admin_router.router)
    app.state.run_queue_admin_dependencies_factory = lambda: dependencies
    client = TestClient(app)

    response = client.post(
        "/api/admin/run-queue/executor-pods/srw-agent-stateless-x/attest-gone",
        json={"pod_uid": "uid-1", "reason": "node-3 scrapped"},
    )

    assert response.status_code == 200
    require_admin.assert_awaited_once()
    assert attest.await_args.args == ("srw-agent-stateless-x",)
    assert attest.await_args.kwargs["pod_uid"] == "uid-1"
    assert attest.await_args.kwargs["admin"] == admin


# --------------------------------------------------------------------------- #
# Attestation fails closed on an unanswered Kubernetes read
# --------------------------------------------------------------------------- #

_UNIT = "11111111-1111-4111-8111-111111111111"


def _ledger_db(pod: SimpleNamespace) -> SimpleNamespace:
    metadata = {
        "_stateless_claim_losses": {
            "3": {
                "pod": pod.metadata.name,
                "pod_uid": pod.metadata.uid,
                "quiesced": False,
            }
        }
    }

    class _Conn:
        async def fetchrow(self, *_args):
            return {"execution_lane": "stateless", "metadata": metadata}

        async def fetchval(self, *_args):
            return {}

    class _Acquire:
        async def __aenter__(self):
            return _Conn()

        async def __aexit__(self, *_exc):
            return False

    return SimpleNamespace(acquire=lambda: _Acquire())


async def _refused_attestation(pod, provisioner, monkeypatch) -> HTTPException:
    monkeypatch.setattr(provisioner_module, "agent_provisioner", provisioner)
    ack = AsyncMock(return_value=True)
    monkeypatch.setattr(
        "shared.session_retirement.acknowledge_session_claim_quiesced", ack
    )
    with pytest.raises(HTTPException) as refused:
        await run_queue_admin.attest_claimant_gone(
            _UNIT,
            pod=pod.metadata.name,
            pod_uid=pod.metadata.uid,
            reason="probe",
            admin={"id": "admin-1"},
            dependencies=SimpleNamespace(db=_ledger_db(pod)),
        )
    ack.assert_not_awaited()
    return refused.value


def _in_grace() -> SimpleNamespace:
    pod = executor_pod(finalizers=[FINALIZER], deleting=True)
    pod.metadata.deletion_timestamp = datetime.now(timezone.utc) + timedelta(
        seconds=170
    )
    return pod


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [500, 403, 504])
async def test_attest_refuses_when_kubernetes_does_not_answer(
    pool_identity, monkeypatch, status
):
    pod = _in_grace()
    api = FakeExecutorCoreApi(pod)
    api.pod_read_error = status
    refused = await _refused_attestation(pod, _provisioner(api), monkeypatch)
    assert refused.status_code == 503


@pytest.mark.asyncio
async def test_attest_refuses_without_a_kubernetes_client(pool_identity, monkeypatch):
    pod = _in_grace()
    provisioner = _provisioner(FakeExecutorCoreApi(pod))
    provisioner._k8s_available = False
    refused = await _refused_attestation(pod, provisioner, monkeypatch)
    assert refused.status_code == 503


@pytest.mark.asyncio
async def test_attest_grace_check_ignores_pool_label_drift(pool_identity, monkeypatch):
    pod = _in_grace()
    pod.metadata.labels["app.kubernetes.io/name"] = "old-chart-name"
    pod.metadata.labels["app.kubernetes.io/instance"] = "old-release"
    refused = await _refused_attestation(
        pod, _provisioner(FakeExecutorCoreApi(pod)), monkeypatch
    )
    assert refused.status_code == 409
    assert "termination grace" in refused.detail
