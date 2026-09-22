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
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services import agent_provisioner as provisioner_module
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
    api = FakeExecutorCoreApi(retained, unprotected, live, other_release)
    for name in ("a", "b", "d"):
        api.delete_namespaced_pod(name, EXECUTOR_NAMESPACE)
    provisioner = _provisioner(api)

    pods = await provisioner.list_retained_stateless_executor_pods()
    assert [pod.metadata.name for pod in pods] == ["a"]

    monkeypatch.delenv("AGENT_LABEL_INSTANCE")
    assert await provisioner.list_retained_stateless_executor_pods() is None
    provisioner._k8s_available = False
    assert await provisioner.list_retained_stateless_executor_pods() is None


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
