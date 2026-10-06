"""Tests for orchestrator/services/agent_provisioner.py.

Covers the new pool management and reservation-aware provisioning:
  - reap_pods(): unified GC of completed / stale / unstartable pods
  - scale_down_idle(): terminate excess idle pods above MIN_AGENTS
  - _try_evict_for_reservation(): evict idle other-purpose pod at capacity
  - provision_agent(): reservation-aware capacity check with eviction
  - Pool fallback paths are tested indirectly via the provisioner methods
"""

import shlex
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from orchestrator.services.agent_pod_entrypoint import InvalidConfigNameError
from orchestrator.services.agent_provisioner import AgentProvisioner
from shared.runtime.core.loader import canonical_config_name
from orchestrator.services.pinned_k8s_effect import (
    K8S_MUTATION_REQUEST_TIMEOUT,
    PINNED_AUTHORITY_FINALIZER,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_provisioner(
    max_agents=10,
    min_agents=2,
    reserved_session_slots=0,
    reserved_job_slots=0,
    k8s_available=True,
):
    """Create an AgentProvisioner with mocked K8s API and DB."""
    p = AgentProvisioner()
    p._k8s_available = k8s_available
    p._namespace = "test-ns"
    p._max_agents = max_agents
    p._min_agents = min_agents
    p._agent_buffer = 0
    p._reserved_session_slots = reserved_session_slots
    p._reserved_job_slots = reserved_job_slots
    p._core_api = MagicMock()
    p._agent_image = "test-image:latest"
    p._configmap_name = "srw-config"
    p._secret_name = "srw-secrets"

    p._db = AsyncMock()
    p._db.acquire = MagicMock()
    mock_conn = AsyncMock()
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)
    p._db.acquire.return_value = mock_ctx
    mock_conn.execute.return_value = "UPDATE 1"
    runtime_generation = "22222222-2222-4222-8222-222222222222"

    async def _thread(thread_id):
        try:
            canonical = str(UUID(str(thread_id)))
        except (TypeError, ValueError):
            return None
        return {
            "id": canonical,
            "status": "created",
            "runtime_generation": runtime_generation,
            "runtime_retirement_token": None,
        }

    async def _reserve(
        thread_id,
        *,
        expected_runtime_generation,
        attempt_id,
        pod_name,
        provisioner,
        namespace,
        protection_protocol="finalizer_v1",
        pvc_name=None,
    ):
        claim = (
            {
                "claim_id": "33333333-3333-4333-8333-333333333333",
                "thread_id": str(thread_id),
                "created_runtime_generation": str(expected_runtime_generation),
                "create_attempt": str(attempt_id),
                "provisioner": provisioner,
                "pvc_name": pvc_name,
                "status": "planned",
                "pvc_uid": None,
                "namespace": namespace,
                "protection_protocol": protection_protocol,
            }
            if pvc_name
            else None
        )
        return {
            "attempt_id": attempt_id,
            "thread_id": thread_id,
            "runtime_generation": expected_runtime_generation,
            "provisioner": provisioner,
            "pod_name": pod_name,
            "status": "planned",
            "pod_uid": None,
            "namespace": namespace,
            "protection_protocol": protection_protocol,
            "workspace_claim": claim,
        }

    p._db.get_thread.side_effect = _thread
    p._db.reserve_pinned_agent_pod_provision_intent.side_effect = _reserve
    p._db.publish_pinned_agent_workspace_claim.return_value = True
    p._db.publish_pinned_agent_pod_provision_intent.return_value = True
    return p, mock_conn


def _make_pod(
    name,
    phase="Running",
    purpose="job",
    thread_id=None,
    age_seconds=10,
    waiting_reason=None,
    agent_terminated_at=None,
    tailscale_terminated_at=None,
):
    """Create a mock K8s pod object.

    ``age_seconds`` controls ``metadata.creation_timestamp`` (now minus that).
    ``waiting_reason`` attaches a single container_status with
    ``state.waiting.reason``. When None, container_statuses is empty — which
    matches the shape of pods that haven't progressed to container creation.
    ``agent_terminated_at`` attaches a container_status named ``"agent"``
    with ``state.terminated.finished_at`` set to (now - that many seconds),
    simulating an agent container that has crashed while a sidecar keeps
    the pod in phase=Running.
    """
    pod = MagicMock()
    pod.metadata.name = name
    pod.metadata.uid = f"uid-{name}"
    pod.metadata.labels = {
        "srw/managed-by": "agent-provisioner",
        "srw/purpose": purpose,
    }
    if thread_id:
        pod.metadata.labels["srw/thread-id"] = thread_id[:12]
    pod.metadata.creation_timestamp = datetime.now(timezone.utc) - timedelta(
        seconds=age_seconds
    )
    pod.status.phase = phase
    pod.status.pod_ip = "10.0.0.1"
    statuses = []
    if waiting_reason is not None:
        cs = MagicMock()
        cs.name = "agent"
        cs.state.waiting.reason = waiting_reason
        cs.state.terminated = None
        statuses.append(cs)
    if agent_terminated_at is not None:
        cs = MagicMock()
        cs.name = "agent"
        cs.state.waiting = None
        cs.state.terminated.finished_at = datetime.now(timezone.utc) - timedelta(
            seconds=agent_terminated_at
        )
        statuses.append(cs)
    if tailscale_terminated_at is not None:
        cs = MagicMock()
        cs.name = "tailscale"
        cs.state.waiting = None
        cs.state.terminated.finished_at = datetime.now(timezone.utc) - timedelta(
            seconds=tailscale_terminated_at
        )
        statuses.append(cs)
    pod.status.container_statuses = statuses
    pod.status.init_container_statuses = []
    return pod


async def _fake_to_thread(fn, *args, **kwargs):
    """Execute a function synchronously (replaces asyncio.to_thread in tests)."""
    return fn(*args, **kwargs)


def _ready_recipient_pod(
    *,
    purpose: str,
    thread_id: str | None = None,
    runtime_generation: str | None = None,
    finalizers: list[str] | None = None,
) -> SimpleNamespace:
    labels = {
        "srw/component": "agent",
        "srw/managed-by": "agent-provisioner",
        "srw/purpose": purpose,
    }
    if thread_id is not None:
        labels["srw.io/thread-id"] = thread_id
    if runtime_generation is not None:
        labels["srw.io/runtime-generation"] = runtime_generation
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name="agent-a",
            namespace="test-ns",
            uid="pod-a",
            deletion_timestamp=None,
            labels=labels,
            finalizers=list(finalizers or []),
        ),
        status=SimpleNamespace(
            phase="Running",
            pod_ip="10.42.0.17",
            container_statuses=[SimpleNamespace(name="agent", ready=True)],
        ),
    )


@pytest.mark.asyncio
async def test_pinned_session_recipient_accepts_exact_provisioned_session_pod():
    provisioner, _ = _make_provisioner()
    generation = "22222222-2222-4222-8222-222222222222"
    thread_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    provisioner._core_api.read_namespaced_pod.return_value = _ready_recipient_pod(
        purpose="session",
        thread_id=thread_id,
        runtime_generation=generation,
    )

    assert await provisioner.attest_pinned_session_recipient(
        "agent-a",
        thread_id=thread_id,
        expected_runtime_generation=generation,
        expected_pod_uid="pod-a",
        expected_pod_ip="10.42.0.17",
        authority_kind="provisioned",
        namespace="test-ns",
    )


@pytest.mark.asyncio
async def test_pinned_session_recipient_accepts_only_protected_warm_pool_pod():
    provisioner, _ = _make_provisioner()
    generation = "22222222-2222-4222-8222-222222222222"
    thread_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    pod = _ready_recipient_pod(
        purpose="job",
        finalizers=[PINNED_AUTHORITY_FINALIZER],
    )
    provisioner._core_api.read_namespaced_pod.return_value = pod

    assert await provisioner.attest_pinned_session_recipient(
        "agent-a",
        thread_id=thread_id,
        expected_runtime_generation=generation,
        expected_pod_uid="pod-a",
        expected_pod_ip="10.42.0.17",
        authority_kind="warm_pool",
        namespace="test-ns",
    )

    pod.metadata.finalizers = []
    assert not await provisioner.attest_pinned_session_recipient(
        "agent-a",
        thread_id=thread_id,
        expected_runtime_generation=generation,
        expected_pod_uid="pod-a",
        expected_pod_ip="10.42.0.17",
        authority_kind="warm_pool",
        namespace="test-ns",
    )


@pytest.mark.asyncio
async def test_pinned_session_recipient_accepts_protected_routed_warm_pool_pod():
    provisioner, _ = _make_provisioner()
    generation = "22222222-2222-4222-8222-222222222222"
    thread_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    provisioner._core_api.read_namespaced_pod.return_value = _ready_recipient_pod(
        purpose="session",
        thread_id=thread_id,
        runtime_generation=generation,
        finalizers=[PINNED_AUTHORITY_FINALIZER],
    )

    assert await provisioner.attest_pinned_session_recipient(
        "agent-a",
        thread_id=thread_id,
        expected_runtime_generation=generation,
        expected_pod_uid="pod-a",
        expected_pod_ip="10.42.0.17",
        authority_kind="warm_pool",
        namespace="test-ns",
    )

    provisioner._core_api.read_namespaced_pod.return_value.metadata.labels[
        "srw.io/runtime-generation"
    ] = "33333333-3333-4333-8333-333333333333"
    assert not await provisioner.attest_pinned_session_recipient(
        "agent-a",
        thread_id=thread_id,
        expected_runtime_generation=generation,
        expected_pod_uid="pod-a",
        expected_pod_ip="10.42.0.17",
        authority_kind="warm_pool",
        namespace="test-ns",
    )


@pytest.mark.asyncio
async def test_warm_release_recognizes_only_exact_routed_session_identity():
    provisioner, _ = _make_provisioner()
    authority = {
        "thread_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "runtime_generation": "22222222-2222-4222-8222-222222222222",
        "namespace": "test-ns",
        "pod_name": "agent-a",
        "pod_uid": "pod-a",
    }
    outcome = {"outcome": "exact_absent_v1", "agent_present": False}

    with patch(
        "orchestrator.services.agent_provisioner.release_planned_pinned_pod_authority",
        AsyncMock(side_effect=[None, outcome]),
    ) as release:
        assert (
            await provisioner.release_planned_pinned_warm_agent_authority(authority)
            == outcome
        )

    assert release.await_args_list[0].kwargs["expected_labels"] == {
        "srw/managed-by": "agent-provisioner",
        "srw/purpose": "job",
    }
    assert release.await_args_list[1].kwargs["expected_labels"] == {
        "srw/managed-by": "agent-provisioner",
        "srw/purpose": "session",
        "srw.io/thread-id": authority["thread_id"],
        "srw.io/runtime-generation": authority["runtime_generation"],
    }


@pytest.mark.asyncio
async def test_pinned_session_recipient_never_crosses_authority_shapes():
    provisioner, _ = _make_provisioner()
    generation = "22222222-2222-4222-8222-222222222222"
    thread_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    warm_pod = _ready_recipient_pod(
        purpose="job",
        finalizers=[PINNED_AUTHORITY_FINALIZER],
    )
    provisioner._core_api.read_namespaced_pod.return_value = warm_pod
    assert not await provisioner.attest_pinned_session_recipient(
        "agent-a",
        thread_id=thread_id,
        expected_runtime_generation=generation,
        expected_pod_uid="pod-a",
        expected_pod_ip="10.42.0.17",
        authority_kind="provisioned",
        namespace="test-ns",
    )

    session_pod = _ready_recipient_pod(
        purpose="session",
        thread_id=thread_id,
        runtime_generation=generation,
        finalizers=[],
    )
    provisioner._core_api.read_namespaced_pod.return_value = session_pod
    assert not await provisioner.attest_pinned_session_recipient(
        "agent-a",
        thread_id=thread_id,
        expected_runtime_generation=generation,
        expected_pod_uid="pod-a",
        expected_pod_ip="10.42.0.17",
        authority_kind="warm_pool",
        namespace="test-ns",
    )


# =============================================================================
# TestReapPods
# =============================================================================


class TestReapPods:
    """Tests for reap_pods() — unified completed/stale/unstartable dispatcher."""

    @pytest.mark.asyncio
    async def test_noop_when_k8s_not_available(self):
        p, _ = _make_provisioner(k8s_available=False)
        result = await p.reap_pods()
        assert result == {
            "completed": 0,
            "crashed": 0,
            "tunnel_dark": 0,
            "stale": 0,
            "drained": 0,
            "unstartable": 0,
        }

    @pytest.mark.asyncio
    async def test_noop_when_no_reapable_pods(self):
        p, conn = _make_provisioner()
        conn.fetch.return_value = []
        pods_result = MagicMock()
        pods_result.items = [_make_pod("srw-agent-j-healthy")]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result == {
            "completed": 0,
            "crashed": 0,
            "tunnel_dark": 0,
            "stale": 0,
            "drained": 0,
            "unstartable": 0,
        }
        assert p._core_api.delete_namespaced_pod.call_count == 0

    @pytest.mark.asyncio
    async def test_reaps_completed_pods(self):
        p, conn = _make_provisioner()
        conn.fetch.return_value = []
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod("srw-agent-j-ok", phase="Succeeded"),
            _make_pod("srw-agent-j-bad", phase="Failed"),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result["completed"] == 2
        assert result["stale"] == 0
        assert result["unstartable"] == 0
        assert p._core_api.delete_namespaced_pod.call_count == 2

    @pytest.mark.asyncio
    async def test_protected_completed_pod_waits_when_database_is_unavailable(self):
        p, _ = _make_provisioner()
        p._db = None
        pod = _make_pod("srw-agent-s-protected", phase="Succeeded", purpose="session")
        pod.metadata.finalizers = [PINNED_AUTHORITY_FINALIZER]
        p._core_api.list_namespaced_pod.return_value.items = [pod]
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result["completed"] == 0
        p._core_api.delete_namespaced_pod.assert_not_called()

    @pytest.mark.asyncio
    async def test_reaps_stale_running_pods(self):
        p, conn = _make_provisioner()
        conn.fetch.return_value = [
            {"hostname": "srw-agent-j-stale"},
        ]
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod("srw-agent-j-stale"),
            _make_pod("srw-agent-j-healthy"),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result["stale"] == 1
        assert result["completed"] == 0
        assert result["unstartable"] == 0
        assert p._core_api.delete_namespaced_pod.call_count == 1

    @pytest.mark.asyncio
    async def test_reaps_drained_running_pods(self):
        # Phase 0 stopgap: when _drain_stale_image_agents marks an agent
        # 'draining', reap_pods must force-delete the pod. Without this the
        # status flicker has no actuation.
        p, conn = _make_provisioner()
        # Two queries fire: offline (returns empty) and draining (returns
        # the target hostname). Use side_effect to differentiate.
        conn.fetch.side_effect = [
            [],
            [{"hostname": "srw-agent-j-drained"}],
        ]
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod("srw-agent-j-drained"),
            _make_pod("srw-agent-j-healthy"),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result["drained"] == 1
        assert result["stale"] == 0
        assert result["completed"] == 0
        assert result["unstartable"] == 0
        assert p._core_api.delete_namespaced_pod.call_count == 1

    @pytest.mark.asyncio
    async def test_drained_skipped_for_pending_pods(self):
        # A pod marked draining but stuck in Pending isn't a Running pod
        # and shouldn't get the drained categorization (would shadow
        # _is_unstartable). Pending stale-image pods are caught by the
        # unstartable path if they hit a terminal waiting reason; otherwise
        # they're left to start up.
        p, conn = _make_provisioner()
        conn.fetch.side_effect = [
            [],
            [{"hostname": "srw-agent-j-pending-drained"}],
        ]
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod("srw-agent-j-pending-drained", phase="Pending", age_seconds=10),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result["drained"] == 0
        assert p._core_api.delete_namespaced_pod.call_count == 0

    @pytest.mark.asyncio
    async def test_stale_takes_precedence_over_drained(self):
        # Defensive ordering: if an agent somehow appears in both the
        # offline and draining hostname sets (mock conflation, race
        # condition), the stale category wins. The pod gets deleted
        # either way; the category just needs to be deterministic.
        p, conn = _make_provisioner()
        conn.fetch.side_effect = [
            [{"hostname": "srw-agent-j-both"}],
            [{"hostname": "srw-agent-j-both"}],
        ]
        pods_result = MagicMock()
        pods_result.items = [_make_pod("srw-agent-j-both")]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result["stale"] == 1
        assert result["drained"] == 0
        assert p._core_api.delete_namespaced_pod.call_count == 1

    @pytest.mark.asyncio
    async def test_reaps_unstartable_pending_pods_past_grace(self):
        p, conn = _make_provisioner()
        conn.fetch.return_value = []
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod(
                "srw-agent-j-stuck",
                phase="Pending",
                age_seconds=600,
                waiting_reason="CreateContainerConfigError",
            ),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result["unstartable"] == 1
        assert p._core_api.delete_namespaced_pod.call_count == 1

    @pytest.mark.asyncio
    async def test_preserves_unstartable_pending_pods_within_grace(self):
        p, conn = _make_provisioner()
        conn.fetch.return_value = []
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod(
                "srw-agent-j-young",
                phase="Pending",
                age_seconds=30,
                waiting_reason="CreateContainerConfigError",
            ),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result["unstartable"] == 0
        assert p._core_api.delete_namespaced_pod.call_count == 0

    @pytest.mark.asyncio
    async def test_preserves_pending_pods_with_benign_waiting_reason(self):
        p, conn = _make_provisioner()
        conn.fetch.return_value = []
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod(
                "srw-agent-j-pulling",
                phase="Pending",
                age_seconds=600,
                waiting_reason="ContainerCreating",
            ),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result == {
            "completed": 0,
            "crashed": 0,
            "tunnel_dark": 0,
            "stale": 0,
            "drained": 0,
            "unstartable": 0,
        }
        assert p._core_api.delete_namespaced_pod.call_count == 0

    @pytest.mark.asyncio
    async def test_reaps_crashed_running_pod_past_grace(self):
        # Sidecar keeps the pod in phase=Running even though agent died —
        # this is the case the original sidecar-pinned bug exposed.
        p, conn = _make_provisioner()
        conn.fetch.return_value = []
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod(
                "srw-agent-j-crashed",
                phase="Running",
                agent_terminated_at=120,
            ),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result["crashed"] == 1
        assert p._core_api.delete_namespaced_pod.call_count == 1

    @pytest.mark.asyncio
    async def test_preserves_crashed_pod_within_grace(self):
        p, conn = _make_provisioner()
        conn.fetch.return_value = []
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod(
                "srw-agent-j-just-crashed",
                phase="Running",
                agent_terminated_at=10,
            ),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result["crashed"] == 0
        assert p._core_api.delete_namespaced_pod.call_count == 0

    @pytest.mark.asyncio
    async def test_mixed_pod_set_reaps_all_three_categories(self):
        p, conn = _make_provisioner()
        conn.fetch.return_value = [{"hostname": "srw-agent-j-zombie"}]
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod("srw-agent-j-done", phase="Succeeded"),
            _make_pod("srw-agent-j-zombie"),  # Running, in offline DB list
            _make_pod(
                "srw-agent-j-stuck",
                phase="Pending",
                age_seconds=600,
                waiting_reason="ImagePullBackOff",
            ),
            _make_pod("srw-agent-j-fine"),  # Running, healthy — skip
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result == {
            "completed": 1,
            "crashed": 0,
            "tunnel_dark": 0,
            "stale": 1,
            "drained": 0,
            "unstartable": 1,
        }
        assert p._core_api.delete_namespaced_pod.call_count == 3

    @pytest.mark.asyncio
    async def test_reaps_tunnel_dark_pod_past_grace(self):
        # Agent container alive, but the kubelet killed the tailscale sidecar
        # after a sustained dark window. The pod is useless — recycle it.
        p, conn = _make_provisioner()
        conn.fetch.return_value = []
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod("srw-agent-j-dark", phase="Running", tailscale_terminated_at=120),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result["tunnel_dark"] == 1
        assert p._core_api.delete_namespaced_pod.call_count == 1

    @pytest.mark.asyncio
    async def test_preserves_tunnel_dark_pod_within_grace(self):
        p, conn = _make_provisioner()
        conn.fetch.return_value = []
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod("srw-agent-j-dark", phase="Running", tailscale_terminated_at=5),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result["tunnel_dark"] == 0
        assert p._core_api.delete_namespaced_pod.call_count == 0

    @pytest.mark.asyncio
    async def test_dead_agent_takes_precedence_over_tunnel_dark(self):
        # Both containers terminated → "crashed" (the agent dying is the more
        # fundamental failure), not tunnel_dark.
        p, conn = _make_provisioner()
        conn.fetch.return_value = []
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod(
                "srw-agent-j-both-dead",
                phase="Running",
                agent_terminated_at=120,
                tailscale_terminated_at=120,
            ),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.reap_pods()
        assert result["crashed"] == 1
        assert result["tunnel_dark"] == 0


# =============================================================================
# TestScaleDownIdle
# =============================================================================


class TestScaleDownIdle:
    """Tests for scale_down_idle()."""

    @pytest.mark.asyncio
    async def test_noop_when_k8s_not_available(self):
        p, _ = _make_provisioner(k8s_available=False)
        assert await p.scale_down_idle() == 0

    @pytest.mark.asyncio
    async def test_noop_when_min_agents_zero(self):
        p, _ = _make_provisioner(min_agents=0)
        assert await p.scale_down_idle() == 0

    @pytest.mark.asyncio
    async def test_noop_when_at_or_below_min(self):
        p, conn = _make_provisioner(min_agents=2)

        # active_count returns 2 (at floor)
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod("pod-1"),
            _make_pod("pod-2"),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.scale_down_idle()

        assert result == 0

    @pytest.mark.asyncio
    async def test_scales_down_excess_idle(self):
        p, conn = _make_provisioner(min_agents=2)

        # active_count returns 6 (4 excess)
        pods_result = MagicMock()
        pods_result.items = [_make_pod(f"pod-{i}") for i in range(6)]
        p._core_api.list_namespaced_pod.return_value = pods_result

        # DB has 4 idle agents
        conn.fetch.return_value = [
            {
                "id": f"agent-{i}",
                "hostname": f"pod-{i}",
                "pod_uid": f"uid-pod-{i}",
            }
            for i in range(4)
        ]
        p._count_idle_agents = AsyncMock(return_value=4)

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.scale_down_idle(max_terminate=2)

        # Should terminate max 2 (max_terminate cap)
        assert result == 2
        assert p._core_api.delete_namespaced_pod.call_count == 2

    @pytest.mark.asyncio
    async def test_respects_max_terminate(self):
        p, conn = _make_provisioner(min_agents=2)

        pods_result = MagicMock()
        pods_result.items = [_make_pod(f"pod-{i}") for i in range(10)]
        p._core_api.list_namespaced_pod.return_value = pods_result

        conn.fetch.return_value = [
            {
                "id": f"agent-{i}",
                "hostname": f"pod-{i}",
                "pod_uid": f"uid-pod-{i}",
            }
            for i in range(8)
        ]
        p._count_idle_agents = AsyncMock(return_value=8)

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.scale_down_idle(max_terminate=1)

        assert result == 1

    @pytest.mark.asyncio
    async def test_leaves_buffer_idle_pods_alone(self):
        """Idle pods within AGENT_BUFFER are warm-pool inventory, not excess.

        Regression for the warm-pool/scale-down thrash: with 3 busy + 1 idle
        and buffer=1, the old active-vs-min check (4 > 2) killed the idle pod
        that ensure_warm_pool had just created — one pod/minute churn for
        hours (knowledge-base/knowledge/issues/session_silent_failure_audit.md #12).
        """
        p, conn = _make_provisioner(min_agents=2)
        p._agent_buffer = 1

        # active_count returns 4 (3 busy + 1 idle)
        pods_result = MagicMock()
        pods_result.items = [_make_pod(f"pod-{i}") for i in range(4)]
        p._core_api.list_namespaced_pod.return_value = pods_result

        conn.fetch.return_value = [
            {"id": "agent-0", "hostname": "pod-0", "pod_uid": "uid-pod-0"}
        ]
        p._count_idle_agents = AsyncMock(return_value=1)

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.scale_down_idle(max_terminate=2)

        assert result == 0
        p._core_api.delete_namespaced_pod.assert_not_called()

    @pytest.mark.asyncio
    async def test_noop_when_no_idle_agents(self):
        p, conn = _make_provisioner(min_agents=2)

        # 6 active pods but no idle agents in DB
        pods_result = MagicMock()
        pods_result.items = [_make_pod(f"pod-{i}") for i in range(6)]
        p._core_api.list_namespaced_pod.return_value = pods_result
        conn.fetch.return_value = []

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.scale_down_idle()

        assert result == 0


# =============================================================================
# TestTryEvictForReservation
# =============================================================================


class TestTryEvictForReservation:
    """Tests for _try_evict_for_reservation()."""

    @pytest.mark.asyncio
    async def test_returns_false_without_reservation(self):
        p, conn = _make_provisioner(reserved_session_slots=0, reserved_job_slots=0)
        result = await p._try_evict_for_reservation(
            "session", {"job": 8, "session": 2, "total": 10}
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_evicts_idle_job_for_session(self):
        p, conn = _make_provisioner(reserved_session_slots=2)

        # DB: one idle job agent
        conn.fetch.return_value = [
            {
                "id": "agent-idle-job",
                "hostname": "srw-agent-j-idle",
                "pod_uid": "uid-srw-agent-j-idle",
            },
        ]

        # K8s: one job pod matching the idle agent
        pods_result = MagicMock()
        pods_result.items = [_make_pod("srw-agent-j-idle", purpose="job")]
        p._core_api.list_namespaced_pod.return_value = pods_result

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p._try_evict_for_reservation(
                "session", {"job": 8, "session": 2, "total": 10}
            )

        assert result is True
        p._core_api.delete_namespaced_pod.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_false_when_no_idle_other_type(self):
        p, conn = _make_provisioner(reserved_session_slots=2)

        # No idle agents in DB
        conn.fetch.return_value = []

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p._try_evict_for_reservation(
                "session", {"job": 8, "session": 2, "total": 10}
            )

        assert result is False

    @pytest.mark.asyncio
    async def test_evicts_idle_session_for_job(self):
        p, conn = _make_provisioner(reserved_job_slots=1)

        conn.fetch.return_value = [
            {
                "id": "agent-idle-session",
                "hostname": "srw-agent-s-idle",
                "pod_uid": "uid-srw-agent-s-idle",
            },
        ]

        pods_result = MagicMock()
        pods_result.items = [_make_pod("srw-agent-s-idle", purpose="session")]
        p._core_api.list_namespaced_pod.return_value = pods_result

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p._try_evict_for_reservation(
                "job", {"job": 2, "session": 8, "total": 10}
            )

        assert result is True


# =============================================================================
# TestProvisionWithEviction
# =============================================================================


class TestProvisionWithEviction:
    """Tests for provision_agent() with reservation-aware eviction."""

    @pytest.mark.asyncio
    async def test_at_capacity_evicts_for_session(self):
        p, conn = _make_provisioner(max_agents=10, reserved_session_slots=2)

        # active_counts: at capacity (10/10)
        pods_list = MagicMock()
        pods_list.items = [_make_pod(f"pod-{i}") for i in range(10)]

        # For eviction: one idle job agent matching a pod
        conn.fetch.side_effect = [
            # First fetch: idle agents for eviction
            [
                {
                    "id": "agent-idle",
                    "hostname": "pod-0",
                    "pod_uid": "uid-pod-0",
                }
            ],
        ]

        # After eviction, need to list pods again for eviction check
        eviction_pods = MagicMock()
        eviction_pods.items = [_make_pod("pod-0", purpose="job")]

        def list_pods_side_effect(**kwargs):
            selector = kwargs.get("label_selector", "")
            if "srw/purpose=job" in selector:
                return eviction_pods
            return pods_list

        p._core_api.list_namespaced_pod.side_effect = list_pods_side_effect
        p._core_api.create_namespaced_pod.return_value = MagicMock()

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.provision_agent(
                purpose="session",
                thread_id="11111111-2222-4333-8444-555555555555",
            )

        # Should have evicted one pod and created a new one
        assert result is not None
        p._core_api.delete_namespaced_pod.assert_called_once()
        p._core_api.create_namespaced_pod.assert_called_once()

    @pytest.mark.asyncio
    async def test_at_capacity_no_eviction_without_reservation(self):
        p, conn = _make_provisioner(max_agents=10, reserved_session_slots=0)

        pods_list = MagicMock()
        pods_list.items = [_make_pod(f"pod-{i}") for i in range(10)]
        p._core_api.list_namespaced_pod.return_value = pods_list

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.provision_agent(
                purpose="session",
                thread_id="11111111-2222-4333-8444-555555555555",
            )

        assert result is None
        p._core_api.create_namespaced_pod.assert_not_called()

    @pytest.mark.asyncio
    async def test_under_capacity_no_eviction(self):
        p, conn = _make_provisioner(max_agents=10, reserved_session_slots=2)

        # Only 5 active pods — well under capacity
        pods_list = MagicMock()
        pods_list.items = [_make_pod(f"pod-{i}") for i in range(5)]
        p._core_api.list_namespaced_pod.return_value = pods_list
        p._core_api.create_namespaced_pod.return_value = MagicMock()

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.provision_agent(
                purpose="session",
                thread_id="11111111-2222-4333-8444-555555555555",
            )

        assert result is not None
        # delete should NOT have been called (no eviction needed)
        p._core_api.delete_namespaced_pod.assert_not_called()


# =============================================================================
# TestProvisionBasic
# =============================================================================


class TestProvisionBasic:
    """Tests for basic provision_agent() behaviour."""

    @pytest.mark.asyncio
    async def test_returns_none_when_k8s_not_available(self):
        p, _ = _make_provisioner(k8s_available=False)
        result = await p.provision_agent(purpose="job")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_pod_name_on_success(self):
        p, conn = _make_provisioner()

        pods_list = MagicMock()
        pods_list.items = []
        p._core_api.list_namespaced_pod.return_value = pods_list
        p._core_api.create_namespaced_pod.return_value = MagicMock()

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.provision_agent(purpose="job")

        assert result is not None
        assert result.startswith("srw-agent-j-")

    @pytest.mark.asyncio
    async def test_reservation_blocks_job_at_ceiling(self):
        p, conn = _make_provisioner(max_agents=10, reserved_session_slots=2)

        # 8 job pods, 0 session → job at ceiling (10-2=8)
        pods_list = MagicMock()
        pods_list.items = [_make_pod(f"pod-{i}", purpose="job") for i in range(8)]
        p._core_api.list_namespaced_pod.return_value = pods_list

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.provision_agent(purpose="job")

        assert result is None

    @pytest.mark.asyncio
    async def test_handles_409_conflict(self):
        p, conn = _make_provisioner()

        pods_list = MagicMock()
        pods_list.items = []
        p._core_api.list_namespaced_pod.return_value = pods_list

        exc = Exception("Conflict")
        exc.status = 409
        p._core_api.create_namespaced_pod.side_effect = exc

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.provision_agent(purpose="job")

        # 409 is treated as success (pod already exists)
        assert result is not None


# =============================================================================
# TestSessionAgentWorkspacePvc
# =============================================================================


class TestSessionAgentWorkspacePvc:
    """Durable ``/workspace`` for SESSION agent pods (WORKSPACE_PVC_ENABLED).

    The agent pod's ``/workspace`` was framed as pure scratch — the real tree
    lives in the separate workspace pod it reaches over SSH — but that framing
    is incomplete: ``backend: none`` / lite sessions have no workspace pod at
    all, so agent-local state written there is the only copy and died with every
    pod recycle (drift drain, crash, node loss, version upgrade). Since the pod
    name is random per provision, the volume identity has to come from the
    thread, which is what makes a recycled pod reattach rather than boot onto
    empty scratch.

    Job agent pods stay emptyDir: they are stateless dispatch runners whose
    durable state is in the workspace pod, the job repo and Postgres.
    """

    _TID = "11111111-2222-3333-4444-555555555555"
    _PVC = "pvc-agent-s-11111111-222"

    def _provisioner(self, pvc_enabled=True):
        p, conn = _make_provisioner()
        p._pvc_enabled = pvc_enabled
        p._pvc_size = "10Gi"
        p._storage_class = "longhorn-ephemeral"
        pods_list = MagicMock()
        pods_list.items = []
        p._core_api.list_namespaced_pod.return_value = pods_list
        return p, conn

    @staticmethod
    def _capture(p, pod_body, pvc_body=None):
        def _pod_create(**kw):
            pod_body.update(kw.get("body", {}))
            created = MagicMock()
            created.metadata.uid = "pod-uid-created"
            return created

        p._core_api.create_namespaced_pod = _pod_create
        if pvc_body is not None:

            def _pvc_create(**kw):
                pvc_body.update(kw.get("body", {}))
                created = MagicMock()
                created.metadata.uid = "pvc-uid-created"
                return created

            p._core_api.create_namespaced_persistent_volume_claim = _pvc_create

    @pytest.mark.asyncio
    async def test_session_pod_gets_a_thread_keyed_pvc(self):
        p, _conn = self._provisioner()
        pod_body, pvc_body = {}, {}
        self._capture(p, pod_body, pvc_body)

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            name = await p.provision_agent(purpose="session", thread_id=self._TID)

        assert name is not None and name.startswith("srw-agent-s-")
        # Keyed on the thread, not the (random) pod name — that is the whole point.
        assert pvc_body["metadata"]["name"] == self._PVC
        assert pvc_body["spec"]["accessModes"] == ["ReadWriteOnce"]
        # The pod mounts the claim, not scratch.
        vols = {v["name"]: v for v in pod_body["spec"]["volumes"]}
        assert vols["workspace"]["persistentVolumeClaim"]["claimName"] == self._PVC
        assert "emptyDir" not in vols["workspace"]

    @pytest.mark.asyncio
    async def test_pvc_carries_the_labels_gc_selects_on(self):
        """Without ``srw.io/component: agent-workspace`` the claim is invisible
        to the lifecycle reaper's label selector and leaks storage forever once
        its thread is gone — the bug the legacy PersistentProvisioner helper has,
        which is why this path does not reuse it. The thread id must be the FULL
        uuid: the reaper resolves PVC → thread row by that value, and the pod's
        12-char ``srw/thread-id`` label is not a thread key.
        """
        p, _conn = self._provisioner()
        pod_body, pvc_body = {}, {}
        self._capture(p, pod_body, pvc_body)

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            await p.provision_agent(purpose="session", thread_id=self._TID)

        labels = pvc_body["metadata"]["labels"]
        assert labels["srw.io/component"] == "agent-workspace"
        assert labels["srw/thread-id"] == self._TID

    @pytest.mark.asyncio
    async def test_job_agent_pod_stays_emptydir(self):
        p, _conn = self._provisioner()
        pod_body = {}
        self._capture(p, pod_body)
        p._core_api.create_namespaced_persistent_volume_claim = MagicMock()

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            name = await p.provision_agent(purpose="job")

        assert name is not None
        p._core_api.create_namespaced_persistent_volume_claim.assert_not_called()
        vols = {v["name"]: v for v in pod_body["spec"]["volumes"]}
        assert vols["workspace"]["emptyDir"]["sizeLimit"] == "10Gi"
        assert "persistentVolumeClaim" not in vols["workspace"]

    @pytest.mark.asyncio
    async def test_flag_off_keeps_session_pods_on_emptydir(self):
        """Mixed-fleet safety: same switch as workspace PVCs, and a cluster that
        hasn't flipped it behaves exactly as before."""
        p, _conn = self._provisioner(pvc_enabled=False)
        pod_body = {}
        self._capture(p, pod_body)
        p._core_api.create_namespaced_persistent_volume_claim = MagicMock()

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            name = await p.provision_agent(purpose="session", thread_id=self._TID)

        assert name is not None
        p._core_api.create_namespaced_persistent_volume_claim.assert_not_called()
        vols = {v["name"]: v for v in pod_body["spec"]["volumes"]}
        assert "emptyDir" in vols["workspace"]

    @pytest.mark.asyncio
    async def test_pvc_failure_fails_closed_without_creating_a_pod(self):
        """An emptyDir fallback would hand the user a session that looks healthy
        and then loses its agent-local state on the next recycle — the exact
        failure the PVC exists to prevent, with nothing in the UI to explain it.
        A visible provision failure the caller can retry is strictly better.
        """
        p, conn = self._provisioner()
        p._core_api.create_namespaced_pod = MagicMock()
        p._core_api.create_namespaced_persistent_volume_claim = MagicMock(
            side_effect=Exception("PVC API down")
        )

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            name = await p.provision_agent(purpose="session", thread_id=self._TID)

        assert name is None
        p._core_api.create_namespaced_pod.assert_not_called()
        # A partial metadata marker would masquerade as deletion authority.
        # The durable planned claim is the retry/recovery signal instead.
        p._db.reserve_pinned_agent_pod_provision_intent.assert_awaited_once()
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_quota_403_also_fails_closed(self):
        """Capacity exhaustion surfaces as a 403 from the namespace
        ResourceQuota. Never silently drop durability because the cluster is
        full."""
        p, _conn = self._provisioner()

        class _QuotaExc(Exception):
            status = 403
            body = "exceeded quota: srw-workspace-storage"

        p._core_api.create_namespaced_pod = MagicMock()
        p._core_api.create_namespaced_persistent_volume_claim = MagicMock(
            side_effect=_QuotaExc()
        )

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            name = await p.provision_agent(purpose="session", thread_id=self._TID)

        assert name is None
        p._core_api.create_namespaced_pod.assert_not_called()

    @pytest.mark.asyncio
    async def test_existing_claim_is_reused_not_recreated(self):
        """409 is the reattach path every pod recycle takes — it must read as
        success, or a recycled session agent could never come back to its data.
        """
        p, _conn = self._provisioner()
        pod_body = {}
        self._capture(p, pod_body)

        conflict = type("ApiErr", (Exception,), {"status": 409})()
        p._core_api.create_namespaced_persistent_volume_claim = MagicMock(
            side_effect=conflict
        )
        existing = MagicMock()
        existing.metadata.uid = "pvc-existing-uid"
        existing.metadata.labels = {
            "srw.io/thread-id": self._TID,
            "srw.io/runtime-generation": ("22222222-2222-4222-8222-222222222222"),
            "srw.io/workspace-claim": "33333333-3333-4333-8333-333333333333",
            "srw.io/provision-attempt": (
                p._db.reserve_pinned_agent_pod_provision_intent.side_effect and "unused"
            ),
            "srw.io/claim-provisioner": "agent",
        }

        async def _reserve_with_known_attempt(*args, **kwargs):
            result = await _make_reserved_intent_for_test(*args, **kwargs)
            existing.metadata.labels["srw.io/provision-attempt"] = str(
                result["attempt_id"]
            )
            return result

        async def _make_reserved_intent_for_test(
            thread_id,
            *,
            expected_runtime_generation,
            attempt_id,
            pod_name,
            provisioner,
            namespace,
            protection_protocol="finalizer_v1",
            pvc_name=None,
        ):
            return {
                "attempt_id": attempt_id,
                "thread_id": thread_id,
                "runtime_generation": expected_runtime_generation,
                "provisioner": provisioner,
                "pod_name": pod_name,
                "status": "planned",
                "pod_uid": None,
                "namespace": namespace,
                "protection_protocol": protection_protocol,
                "workspace_claim": {
                    "claim_id": "33333333-3333-4333-8333-333333333333",
                    "thread_id": thread_id,
                    "created_runtime_generation": expected_runtime_generation,
                    "create_attempt": attempt_id,
                    "provisioner": provisioner,
                    "pvc_name": pvc_name,
                    "status": "planned",
                    "pvc_uid": None,
                    "namespace": namespace,
                    "protection_protocol": protection_protocol,
                },
            }

        p._db.reserve_pinned_agent_pod_provision_intent.side_effect = (
            _reserve_with_known_attempt
        )
        p._core_api.read_namespaced_persistent_volume_claim.return_value = existing

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            name = await p.provision_agent(purpose="session", thread_id=self._TID)

        assert name is not None
        vols = {v["name"]: v for v in pod_body["spec"]["volumes"]}
        assert vols["workspace"]["persistentVolumeClaim"]["claimName"] == self._PVC

    @pytest.mark.asyncio
    async def test_pvc_name_never_collides_with_the_legacy_persistent_claim(self):
        """``pvc-persistent-<id>`` belongs to the legacy PersistentProvisioner
        pod path. Sharing one RWO claim between both paths would wedge whichever
        pod attached second if the two ever coexist for a thread."""
        p, _conn = self._provisioner()
        pod_body, pvc_body = {}, {}
        self._capture(p, pod_body, pvc_body)

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            await p.provision_agent(purpose="session", thread_id=self._TID)

        assert not pvc_body["metadata"]["name"].startswith("pvc-persistent-")

    @pytest.mark.asyncio
    async def test_effect_then_timeout_recovers_only_exact_claim_labels(self):
        p, _ = self._provisioner()
        timeout = TimeoutError("create response lost")
        p._core_api.create_namespaced_persistent_volume_claim.side_effect = timeout
        observed = MagicMock()
        observed.metadata.uid = "pvc-uid-after-timeout"
        observed.metadata.labels = {
            "srw.io/thread-id": self._TID,
            "srw.io/runtime-generation": ("22222222-2222-4222-8222-222222222222"),
            "srw.io/workspace-claim": "33333333-3333-4333-8333-333333333333",
            "srw.io/provision-attempt": "44444444-4444-4444-8444-444444444444",
            "srw.io/claim-provisioner": "agent",
        }
        p._core_api.read_namespaced_persistent_volume_claim.return_value = observed
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            uid = await p._ensure_pinned_agent_pvc(
                self._PVC,
                thread_id=self._TID,
                runtime_generation="22222222-2222-4222-8222-222222222222",
                claim_id="33333333-3333-4333-8333-333333333333",
                create_attempt="44444444-4444-4444-8444-444444444444",
                expected_pvc_uid=None,
                namespace="test-ns",
            )
        assert uid == "pvc-uid-after-timeout"

        observed.metadata.labels["srw.io/thread-id"] = (
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        )
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            assert (
                await p._ensure_pinned_agent_pvc(
                    self._PVC,
                    thread_id=self._TID,
                    runtime_generation="22222222-2222-4222-8222-222222222222",
                    claim_id="33333333-3333-4333-8333-333333333333",
                    create_attempt="44444444-4444-4444-8444-444444444444",
                    expected_pvc_uid=None,
                    namespace="test-ns",
                )
                is None
            )

    @pytest.mark.asyncio
    async def test_permanent_claim_fence_is_inert_and_classifies_original(self):
        p, _ = self._provisioner()
        incumbent = MagicMock()
        incumbent.metadata.uid = "original-pvc-uid"
        incumbent.metadata.labels = {
            "srw.io/thread-id": self._TID,
            "srw.io/runtime-generation": ("22222222-2222-4222-8222-222222222222"),
            "srw.io/workspace-claim": "33333333-3333-4333-8333-333333333333",
            "srw.io/provision-attempt": "44444444-4444-4444-8444-444444444444",
            "srw.io/claim-provisioner": "agent",
        }
        conflict = type("ApiErr", (Exception,), {"status": 409})()
        p._core_api.create_namespaced_persistent_volume_claim.side_effect = conflict
        p._core_api.read_namespaced_persistent_volume_claim.return_value = incumbent
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.fence_agent_workspace_claim(
                self._PVC,
                expected_thread_id=self._TID,
                expected_runtime_generation=("22222222-2222-4222-8222-222222222222"),
                expected_claim_id="33333333-3333-4333-8333-333333333333",
                expected_create_attempt="44444444-4444-4444-8444-444444444444",
                namespace="test-ns",
            )
        assert result == {"state": "exact_original", "pvc_uid": "original-pvc-uid"}

        p._core_api.create_namespaced_persistent_volume_claim.side_effect = None
        created_fence = MagicMock()
        created_fence.metadata.uid = "fence-pvc-uid"
        created_fence.metadata.labels = {
            **incumbent.metadata.labels,
            "srw.io/workspace-claim-fence": "true",
        }
        p._core_api.create_namespaced_persistent_volume_claim.return_value = (
            created_fence
        )
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.fence_agent_workspace_claim(
                self._PVC,
                expected_thread_id=self._TID,
                expected_runtime_generation=("22222222-2222-4222-8222-222222222222"),
                expected_claim_id="33333333-3333-4333-8333-333333333333",
                expected_create_attempt="44444444-4444-4444-8444-444444444444",
                namespace="test-ns",
            )
        assert result == {"state": "exact_fence", "pvc_uid": "fence-pvc-uid"}
        manifest = (
            p._core_api.create_namespaced_persistent_volume_claim.call_args.kwargs[
                "body"
            ]
        )
        assert manifest["spec"]["storageClassName"] == ""
        assert manifest["spec"]["resources"]["requests"]["storage"] == "1Mi"


# =============================================================================
# TestActiveCountsByPurpose
# =============================================================================


class TestActiveCountsByPurpose:
    """Tests for active_counts_by_purpose()."""

    @pytest.mark.asyncio
    async def test_counts_running_pods_by_purpose(self):
        p, _ = _make_provisioner()

        pods_result = MagicMock()
        pods_result.items = [
            _make_pod("j1", purpose="job"),
            _make_pod("j2", purpose="job"),
            _make_pod("s1", purpose="session"),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.active_counts_by_purpose()

        assert result == {"job": 2, "session": 1, "total": 3}

    @pytest.mark.asyncio
    async def test_excludes_succeeded_and_failed(self):
        p, _ = _make_provisioner()

        pods_result = MagicMock()
        pods_result.items = [
            _make_pod("j1", purpose="job"),
            _make_pod("j2", phase="Succeeded", purpose="job"),
            _make_pod("s1", phase="Failed", purpose="session"),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.active_counts_by_purpose()

        assert result == {"job": 1, "session": 0, "total": 1}

    @pytest.mark.asyncio
    async def test_returns_zeros_when_k8s_not_available(self):
        p, _ = _make_provisioner(k8s_available=False)
        result = await p.active_counts_by_purpose()
        assert result == {"job": 0, "session": 0, "total": 0}

    @pytest.mark.asyncio
    async def test_excludes_pods_with_terminated_agent_container(self):
        # Sidecar-pinned crashed pods stay in phase=Running but must not
        # count against MAX_AGENTS, otherwise the ceiling locks up.
        p, _ = _make_provisioner()
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod("j1", purpose="job"),
            _make_pod("j2", purpose="job", agent_terminated_at=5),
            _make_pod("s1", purpose="session", agent_terminated_at=200),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            result = await p.active_counts_by_purpose()
        assert result == {"job": 1, "session": 0, "total": 1}

    @pytest.mark.asyncio
    async def test_excludes_tunnel_dark_pods(self):
        p, _ = _make_provisioner()
        pods_result = MagicMock()
        pods_result.items = [
            _make_pod("j-ok", purpose="job"),
            _make_pod("j-dark", purpose="job", tailscale_terminated_at=120),
        ]
        p._core_api.list_namespaced_pod.return_value = pods_result
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            counts = await p.active_counts_by_purpose()
        assert counts["job"] == 1  # the tunnel-dark pod is not counted
        assert counts["total"] == 1


# =============================================================================
# Pod manifest — labels & downward-API env
# =============================================================================


def _bare_provisioner_for_manifest():
    """Build an AgentProvisioner with only the attributes _build_pod_manifest reads.

    Bypasses __init__ so we don't depend on K8s client initialisation,
    config discovery, or env-var defaults.
    """
    p = AgentProvisioner.__new__(AgentProvisioner)
    p._namespace = "srw"
    p._configmap_name = "srw-config"
    p._secret_name = "srw-secret"
    p._agent_image = "srw-agent:latest"
    p._chart_label_name = ""
    p._chart_label_instance = ""
    p._ssh_secret_name = "srw-vm-ssh-key"
    p._orchestrator_host = "srw-orchestrator"
    p._orchestrator_port = 8085
    # Disable the optional tailscale sidecar — its branch reads _headscale_url.
    p._tailscale_enabled = False
    p._headscale_url = ""
    return p


def test_pod_manifest_includes_full_thread_id_label():
    """Persistent agent pods carry srw.io/thread-id={thread_id} (full value)
    so the session router Service selector can match them.

    The legacy srw/thread-id label is truncated to 12 chars and is kept for
    backwards-compat with the existing lifecycle reconciler; it's not the
    selector the session router uses.
    """
    p = _bare_provisioner_for_manifest()

    manifest = p._build_pod_manifest(
        pod_name="srw-agent-s-abc12345",
        purpose="session",
        thread_id="11111111-2222-3333-4444-555555555555",
        config_name="persistent_defaults",
        cpu_request="100m",
        memory_request="256Mi",
        cpu_limit="1",
        memory_limit="2Gi",
        session_runtime_generation="22222222-2222-4222-8222-222222222222",
    )

    labels = manifest["metadata"]["labels"]
    assert labels["srw.io/thread-id"] == "11111111-2222-3333-4444-555555555555"
    # legacy label still present (truncated)
    assert labels["srw/thread-id"] == "11111111-222"


def test_pod_manifest_omits_thread_id_label_for_worker():
    """Worker pods have no thread affinity, so neither thread-id label is set.

    Otherwise the session router's Service selector would accidentally match
    arbitrary worker pods.
    """
    p = _bare_provisioner_for_manifest()

    manifest = p._build_pod_manifest(
        pod_name="srw-agent-w-deadbeef",
        purpose="worker",
        thread_id=None,
        config_name="defaults",
        cpu_request="100m",
        memory_request="256Mi",
        cpu_limit="1",
        memory_limit="2Gi",
    )

    labels = manifest["metadata"]["labels"]
    assert "srw.io/thread-id" not in labels
    assert "srw/thread-id" not in labels


def test_pod_manifest_checks_readiness_immediately_after_startup_probe():
    p = _bare_provisioner_for_manifest()

    manifest = p._build_pod_manifest(
        pod_name="srw-agent-j-deadbeef",
        purpose="job",
        thread_id=None,
        config_name="developer",
        cpu_request="100m",
        memory_request="256Mi",
        cpu_limit="1",
        memory_limit="2Gi",
    )

    container = manifest["spec"]["containers"][0]
    assert container["startupProbe"]["httpGet"]["path"] == "/health"
    assert container["startupProbe"]["periodSeconds"] == 1
    assert container["startupProbe"]["failureThreshold"] == 100
    assert container["readinessProbe"]["httpGet"]["path"] == "/ready"
    assert container["readinessProbe"]["initialDelaySeconds"] == 0


@pytest.mark.parametrize(
    ("purpose", "thread_id", "expected_mode"),
    [
        ("job", None, "--config developer"),
        (
            "session",
            "11111111-2222-3333-4444-555555555555",
            "--mode persistent",
        ),
    ],
)
def test_pod_manifest_execs_python_as_pid_one(purpose, thread_id, expected_mode):
    """Kubelet SIGTERM must reach the agent's graceful-drain handler."""
    p = _bare_provisioner_for_manifest()

    manifest = p._build_pod_manifest(
        pod_name=f"srw-agent-{purpose}-deadbeef",
        purpose=purpose,
        thread_id=thread_id,
        config_name="developer",
        cpu_request="100m",
        memory_request="256Mi",
        cpu_limit="1",
        memory_limit="2Gi",
    )

    command = manifest["spec"]["containers"][0]["command"]
    assert command[:2] == ["sh", "-c"]
    assert command[2].startswith("exec python agent.py ")
    assert expected_mode in command[2]


def test_pod_manifest_injects_pod_uid_via_downward_api():
    """Each agent pod has POD_UID env populated from the K8s downward API
    so the agent can report its own metadata.uid back to the orchestrator
    at registration time."""
    p = _bare_provisioner_for_manifest()

    manifest = p._build_pod_manifest(
        pod_name="srw-agent-w-deadbeef",
        purpose="worker",
        thread_id=None,
        config_name="defaults",
        cpu_request="100m",
        memory_request="256Mi",
        cpu_limit="1",
        memory_limit="2Gi",
    )

    container_env = manifest["spec"]["containers"][0].get("env", [])
    pod_uid_entry = next((e for e in container_env if e.get("name") == "POD_UID"), None)
    assert pod_uid_entry is not None, "POD_UID env not present"
    assert pod_uid_entry["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.uid"


def test_pod_manifest_injects_internal_key_for_canvas_tools():
    p = _bare_provisioner_for_manifest()
    manifest = p._build_pod_manifest(
        pod_name="srw-agent-s-canvas",
        purpose="session",
        thread_id="11111111-2222-3333-4444-555555555555",
        config_name="persistent_defaults",
        cpu_request="100m",
        memory_request="256Mi",
        cpu_limit="1",
        memory_limit="2Gi",
    )
    env = manifest["spec"]["containers"][0]["env"]
    internal = next(e for e in env if e["name"] == "MCP_INTERNAL_KEY")
    assert internal["valueFrom"]["secretKeyRef"] == {
        "name": "srw-secret",
        "key": "MCP_INTERNAL_KEY",
        "optional": True,
    }


def test_pod_manifest_injects_session_bound_thread_id_env():
    """Session pods carry SESSION_BOUND_THREAD_ID env so the pod's JWT
    validator can check the `tid` claim matches its own thread."""
    from orchestrator.services.agent_provisioner import AgentProvisioner

    p = AgentProvisioner.__new__(AgentProvisioner)
    p._namespace = "srw"
    p._configmap_name = "srw-config"
    p._secret_name = "srw-secret"
    p._agent_image = "srw-agent:latest"
    p._chart_label_name = ""
    p._chart_label_instance = ""
    # Additional attributes _build_pod_manifest reads beyond the JWT-env
    # assertions this test cares about. Kept minimal: SSH secret name for the
    # vm-ssh-key volume, and the tailscale sidecar gate disabled so we don't
    # need a headscale URL.
    p._ssh_secret_name = "srw-vm-ssh-key"
    p._orchestrator_host = "srw-orchestrator"
    p._orchestrator_port = 8085
    p._tailscale_enabled = False
    p._headscale_url = ""

    manifest = p._build_pod_manifest(
        pod_name="srw-agent-s-deadbeef",
        purpose="session",
        thread_id="11111111-2222-3333-4444-555555555555",
        config_name="persistent_defaults",
        cpu_request="100m",
        memory_request="256Mi",
        cpu_limit="1",
        memory_limit="2Gi",
        runtime_actor_bootstrap="srb_session_only",
    )

    env = manifest["spec"]["containers"][0].get("env", [])
    tid_entry = next(
        (e for e in env if e.get("name") == "SESSION_BOUND_THREAD_ID"), None
    )
    assert tid_entry is not None
    assert tid_entry["value"] == "11111111-2222-3333-4444-555555555555"
    actor_entry = next(
        (e for e in env if e.get("name") == "SRW_RUNTIME_ACTOR_BOOTSTRAP"), None
    )
    assert actor_entry == {
        "name": "SRW_RUNTIME_ACTOR_BOOTSTRAP",
        "value": "srb_session_only",
    }


def test_pod_manifest_injects_session_jwt_secret_env_from_secretref(monkeypatch):
    """Session pods reference the JWT Secret by name (via SESSION_JWT_SECRET_NAME
    env on the orchestrator) so handshake validation has the shared key."""
    monkeypatch.setenv("SESSION_JWT_SECRET_NAME", "my-release-session-jwt")
    from orchestrator.services.agent_provisioner import AgentProvisioner

    p = AgentProvisioner.__new__(AgentProvisioner)
    p._namespace = "srw"
    p._configmap_name = "srw-config"
    p._secret_name = "srw-secret"
    p._agent_image = "srw-agent:latest"
    p._chart_label_name = ""
    p._chart_label_instance = ""
    # See test above for why these extra attributes are set.
    p._ssh_secret_name = "srw-vm-ssh-key"
    p._orchestrator_host = "srw-orchestrator"
    p._orchestrator_port = 8085
    p._tailscale_enabled = False
    p._headscale_url = ""

    manifest = p._build_pod_manifest(
        pod_name="srw-agent-s-abc",
        purpose="session",
        thread_id="t1",
        config_name="persistent_defaults",
        cpu_request="100m",
        memory_request="256Mi",
        cpu_limit="1",
        memory_limit="2Gi",
    )

    env = manifest["spec"]["containers"][0].get("env", [])
    jwt_entry = next((e for e in env if e.get("name") == "SESSION_JWT_SECRET"), None)
    assert jwt_entry is not None
    assert jwt_entry["valueFrom"]["secretKeyRef"]["name"] == "my-release-session-jwt"
    assert jwt_entry["valueFrom"]["secretKeyRef"]["key"] == "jwt-secret"
    assert jwt_entry["valueFrom"]["secretKeyRef"]["optional"] is True


# =============================================================================
# TestTailscaleSidecar
# =============================================================================


class TestTailscaleSidecar:
    """Tests for the tailscale sidecar built by _build_pod_manifest()."""

    def _manifest_with_tailscale(self, dark_timeout=600):
        p, _ = _make_provisioner()
        p._tailscale_enabled = True
        p._headscale_url = "https://headscale.test"
        p._tailscale_dark_timeout = dark_timeout
        manifest = p._build_pod_manifest(
            pod_name="srw-agent-test",
            purpose="job",
            thread_id=None,
            config_name="srw-config",
            cpu_request="100m",
            memory_request="256Mi",
            cpu_limit="1000m",
            memory_limit="2Gi",
        )
        return next(
            c for c in manifest["spec"]["containers"] if c["name"] == "tailscale"
        )

    def test_sidecar_has_self_heal_loop(self):
        ts = self._manifest_with_tailscale()
        args = ts["args"][0]
        assert "kill -0" in args, "supervision loop must watch tailscaled"
        assert "BackendState" in args, "must re-up based on backend state"
        assert "--peers=false" in args, (
            "status check must skip the peer dump (slow on large tailnets)"
        )
        assert args.count("tailscale up") >= 2, "initial auth + re-up"
        assert "wait $TSPID" not in args, "blocking tail must be replaced"

    def test_sidecar_liveness_probe_default_threshold(self):
        ts = self._manifest_with_tailscale(dark_timeout=600)
        probe = ts["livenessProbe"]
        assert "BackendState" in probe["exec"]["command"][-1]
        assert "--peers=false" in probe["exec"]["command"][-1], (
            "probe must skip the peer dump"
        )
        assert probe["timeoutSeconds"] >= 5, (
            "1s default is too short for `tailscale status`"
        )
        assert probe["periodSeconds"] == 30
        assert probe["failureThreshold"] == 20  # ceil(600 / 30)
        assert probe["initialDelaySeconds"] >= 90  # startup auth must finish first

    def test_sidecar_liveness_threshold_scales_with_timeout(self):
        ts = self._manifest_with_tailscale(dark_timeout=300)
        assert ts["livenessProbe"]["failureThreshold"] == 10  # ceil(300 / 30)


# =============================================================================
# _archive_pod_logs — post-mortem log archive (knowledge-base/knowledge/features/job_log_archive.md)
# =============================================================================


def _make_snapshot_mock(available=True, put_ok=True):
    snap = MagicMock()
    snap.is_available = available
    snap.put_blob = AsyncMock(return_value=put_ok)
    return snap


class TestArchivePodLogs:
    """Deletion paths never read logs through a mutable Pod name."""

    @pytest.mark.asyncio
    async def test_archive_helper_refuses_name_addressed_read(self):
        p, conn = _make_provisioner()
        await p._archive_pod_logs("srw-agent-s-reused")
        p._core_api.read_namespaced_pod.assert_not_called()
        p._core_api.read_namespaced_pod_log.assert_not_called()
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_exact_delete_does_not_read_successor_logs(self):
        p, _ = _make_provisioner()
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            assert await p.delete_agent_pod("srw-agent-j-xyz", expected_pod_uid="uid-a")
        p._core_api.delete_namespaced_pod.assert_called_once()
        p._core_api.read_namespaced_pod_log.assert_not_called()


class TestExactClaimantPodAuthority:
    @staticmethod
    def _pod(*, uid="uid-a", phase="Running", deleting=False, terminated=False):
        pod = MagicMock()
        pod.metadata.uid = uid
        pod.metadata.deletion_timestamp = "now" if deleting else None
        pod.status.phase = phase
        state = MagicMock()
        state.terminated = object() if terminated else None
        status = MagicMock()
        status.state = state
        pod.status.container_statuses = [status]
        return pod

    @pytest.mark.asyncio
    async def test_same_name_uid_replacement_is_not_old_claimant(self):
        p, _ = _make_provisioner()
        p._core_api.read_namespaced_pod.return_value = self._pod(uid="uid-new")
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            assert (
                await p.agent_pod_authority("pod-a", expected_pod_uid="uid-old")
                == "replacement"
            )

    @pytest.mark.asyncio
    async def test_deleting_and_unknown_are_not_quiescence_proof(self):
        p, _ = _make_provisioner()
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            p._core_api.read_namespaced_pod.return_value = self._pod(deleting=True)
            assert (
                await p.agent_pod_authority("pod-a", expected_pod_uid="uid-a")
                == "unknown"
            )
            p._core_api.read_namespaced_pod.return_value = self._pod(phase="Unknown")
            assert (
                await p.agent_pod_authority("pod-a", expected_pod_uid="uid-a")
                == "unknown"
            )

    @pytest.mark.asyncio
    async def test_exact_terminal_containers_are_positive_proof(self):
        p, _ = _make_provisioner()
        p._core_api.read_namespaced_pod.return_value = self._pod(
            phase="Failed", terminated=True
        )
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            assert (
                await p.agent_pod_authority("pod-a", expected_pod_uid="uid-a")
                == "exact_terminal"
            )

    @pytest.mark.asyncio
    async def test_exact_delete_is_graceful_and_uid_preconditioned(self):
        p, _ = _make_provisioner()
        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            assert await p.delete_agent_pod_exact("pod-a", expected_pod_uid="uid-a")
        p._core_api.delete_namespaced_pod.assert_called_once_with(
            name="pod-a",
            namespace="test-ns",
            grace_period_seconds=180,
            body={"preconditions": {"uid": "uid-a"}},
            _request_timeout=K8S_MUTATION_REQUEST_TIMEOUT,
        )


# =============================================================================
# config_name at the provisioner boundary
# (security audit 2026-08-27, finding #3: caller-controlled config_name was
#  f-spliced unquoted into the agent pod's ``sh -c`` entrypoint, in a pod
#  carrying the platform Secret via envFrom)
# =============================================================================

_HOSTILE_CONFIG_NAMES = [
    "worker_base; touch /tmp/pwned",
    "$(id)",
    "`id`",
    "a b",
    "../x",
    "x" * 1000,
]

# Bare names, a relative YAML path, and the compatibility aliases in both the
# name and the path form. The alias mapping itself belongs to
# canonical_config_name() (covered by test_unified_expert_selection.py); here
# the point is that every one of these survives the validator and boots with
# the alias layer's answer on ``--config``.
_VALID_CONFIG_NAMES = [
    "worker_base",
    "session_base",
    "scholar",
    "config/experts/scholar/config.yaml",
    "default",
    "defaults",
    "persistent_default",
    "persistent_defaults",
    "experts/default.yaml",
]


def _expected_agent_argv(config_name, thread_id=None):
    argv = ["exec", "python", "agent.py"]
    if thread_id:
        argv += ["--mode", "persistent", "--thread-id", thread_id]
    return argv + ["--config", config_name, "--port", "8001", "--host", "0.0.0.0"]


def _manifest(p, purpose, thread_id, config_name):
    return p._build_pod_manifest(
        pod_name=f"srw-agent-{purpose[0]}-deadbeef",
        purpose=purpose,
        thread_id=thread_id,
        config_name=config_name,
        cpu_request="100m",
        memory_request="256Mi",
        cpu_limit="1",
        memory_limit="2Gi",
    )


class TestConfigNameBoundary:
    """A hostile name never reaches Kubernetes or a pod spec; valid ones boot."""

    _TID = "11111111-2222-3333-4444-555555555555"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("hostile", _HOSTILE_CONFIG_NAMES)
    async def test_job_rejects_hostile_name_before_any_kubernetes_call(self, hostile):
        p, _conn = _make_provisioner()
        with patch.object(p, "_build_pod_manifest") as build:
            with pytest.raises(InvalidConfigNameError):
                await p.provision_agent(purpose="job", config_name=hostile)
        build.assert_not_called()
        assert p._core_api.mock_calls == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("hostile", _HOSTILE_CONFIG_NAMES)
    async def test_session_rejects_hostile_name_before_any_kubernetes_or_db_call(
        self, hostile
    ):
        p, _conn = _make_provisioner()
        with patch.object(p, "_build_pod_manifest") as build:
            with pytest.raises(InvalidConfigNameError):
                await p.provision_agent(
                    purpose="session", thread_id=self._TID, config_name=hostile
                )
        build.assert_not_called()
        assert p._core_api.mock_calls == []
        p._db.get_thread.assert_not_awaited()
        p._db.reserve_pinned_agent_pod_provision_intent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_hostile_name_is_rejected_even_without_kubernetes(self):
        """The check precedes the availability short-circuit: a rejected name
        is loud everywhere, not silently swallowed into ``None``."""
        p, _ = _make_provisioner(k8s_available=False)
        with pytest.raises(InvalidConfigNameError):
            await p.provision_agent(purpose="job", config_name="$(id)")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("given", _VALID_CONFIG_NAMES)
    async def test_valid_names_still_boot_a_job_pod(self, given):
        booted = canonical_config_name(given)
        p, _conn = _make_provisioner()
        pods_list = MagicMock()
        pods_list.items = []
        p._core_api.list_namespaced_pod.return_value = pods_list
        bodies = []

        def _create(**kw):
            bodies.append(kw["body"])
            created = MagicMock()
            created.metadata.uid = "pod-uid-created"
            return created

        p._core_api.create_namespaced_pod.side_effect = _create

        with patch(
            "orchestrator.services.agent_provisioner.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            name = await p.provision_agent(purpose="job", config_name=given)

        assert name is not None and name.startswith("srw-agent-j-")
        assert len(bodies) == 1
        container = bodies[0]["spec"]["containers"][0]
        assert container["command"][:2] == ["sh", "-c"]
        assert shlex.split(container["command"][2]) == _expected_agent_argv(booted)
        env = {e["name"]: e.get("value") for e in container["env"]}
        assert env["AGENT_CONFIG"] == booted

    @pytest.mark.parametrize("hostile", _HOSTILE_CONFIG_NAMES)
    def test_manifest_builder_itself_refuses_a_hostile_name(self, hostile):
        """The sink re-checks: no path to a pod spec bypasses the allow-list."""
        p = _bare_provisioner_for_manifest()
        with pytest.raises(InvalidConfigNameError):
            _manifest(p, "job", None, hostile)

    @pytest.mark.parametrize(
        ("purpose", "thread_id"),
        [("job", None), ("session", _TID)],
    )
    def test_manifest_command_parses_back_to_exactly_the_intended_argv(
        self, purpose, thread_id
    ):
        p = _bare_provisioner_for_manifest()
        name = "config/experts/scholar/config.yaml"
        manifest = _manifest(p, purpose, thread_id, name)
        command = manifest["spec"]["containers"][0]["command"]
        assert command[:2] == ["sh", "-c"]
        assert shlex.split(command[2]) == _expected_agent_argv(name, thread_id)
        init = manifest["spec"]["initContainers"][0]["command"]
        assert init[:2] == ["sh", "-c"]
        assert "nc -z srw-orchestrator 8085" in init[2]

    def test_manifest_quotes_every_argv_word_not_just_config_name(self):
        """``thread_id`` is a DB UUID today; the sink still quotes it as one
        word so no future caller can turn it into shell syntax."""
        p = _bare_provisioner_for_manifest()
        hostile_thread = "a b;$(id)"
        manifest = _manifest(p, "session", hostile_thread, "scholar")
        command = manifest["spec"]["containers"][0]["command"]
        assert shlex.split(command[2]) == _expected_agent_argv(
            "scholar", hostile_thread
        )
