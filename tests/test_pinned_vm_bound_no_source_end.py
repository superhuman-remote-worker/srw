"""A bound, never-issued VM life needs exact actor zero before End can settle."""

from contextlib import asynccontextmanager
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services.pinned_retirement import (
    PinnedRetirementDependencies,
    PinnedRetirementOperations,
)
from orchestrator.services.pinned_k8s_effect import PINNED_AUTHORITY_FINALIZER
from orchestrator.services.agent_provisioner import AgentProvisioner
from shared import pinned_vm_creation_retirement


THREAD = "11111111-1111-4111-8111-111111111111"
GENERATION = "22222222-2222-4222-8222-222222222222"
TOKEN = "33333333-3333-4333-8333-333333333333"
AGENT = "44444444-4444-4444-8444-444444444444"
ATTACH = "55555555-5555-4555-8555-555555555555"
POD_UID = "66666666-6666-4666-8666-666666666666"
POD_NAME = "srw-agent-j-44444444"
NAMESPACE = "agents-a"


def captured_context():
    return {
        "version": 1,
        "thread_id": THREAD,
        "generation": GENERATION,
        "entry_status": "created",
        "settle_status": "ended",
        "workspace_backend": "vm",
        "runtime_authority_exposed": True,
        "agent_id": AGENT,
        "control_admission_agent_id": AGENT,
        "runtime_attach_token": ATTACH,
        "agent": {
            "id": AGENT,
            "thread_id": THREAD,
            "hostname": POD_NAME,
            "pod_uid": POD_UID,
        },
        "agent_pod": {
            "pod_name": POD_NAME,
            "pod_uid": POD_UID,
            "namespace": NAMESPACE,
            "runtime_generation": GENERATION,
            "protection_protocol": "finalizer_v1",
        },
        "agent_pod_provision_intent": None,
        "agent_workspace_claim": None,
        "workspace_provision_intent": None,
        "workspace_container": {"repo_name": "example"},
        "workspace_binding": None,
        "vm": None,
        "vm_creation_source": None,
    }


def retirement(context=None):
    return {
        "generation": GENERATION,
        "token": TOKEN,
        "permanent": False,
        "context": captured_context() if context is None else context,
    }


def local_receipt():
    return {
        "version": 1,
        "runtime_generation": GENERATION,
        "retirement_token": TOKEN,
        "agent_id": AGENT,
        "runtime_attach_token": ATTACH,
        "settle_status": "ended",
        "quiescence_protocol": "agent_runtime_zero_v1",
        "quiescence_actor": "orchestrator",
        "workspace_generation": None,
        "workspace_runtime_incarnation": None,
        "agent_pod_name": POD_NAME,
        "agent_pod_uid": POD_UID,
    }


def owner_row(context=None, receipt=None):
    return {
        "id": THREAD,
        "status": "created",
        "runtime_generation": GENERATION,
        "runtime_retirement_token": TOKEN,
        "runtime_retirement_authorized_at": object(),
        "runtime_retirement_permanent": False,
        "runtime_authority_exposed": True,
        "agent_id": AGENT,
        "control_admission_agent_id": AGENT,
        "runtime_attach_token": ATTACH,
        "runtime_retirement_context": captured_context()
        if context is None
        else context,
        "runtime_retirement_local_quiescence": receipt,
        "metadata": {"agent_pod": captured_context()["agent_pod"]},
    }


def terminal_pod():
    def status(name):
        return SimpleNamespace(name=name, state=SimpleNamespace(terminated=object()))

    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=POD_UID,
            name=POD_NAME,
            namespace=NAMESPACE,
            finalizers=[PINNED_AUTHORITY_FINALIZER],
        ),
        spec=SimpleNamespace(
            restart_policy="Never",
            containers=[SimpleNamespace(name="agent")],
            init_containers=[SimpleNamespace(name="wait-for-orchestrator")],
            ephemeral_containers=[SimpleNamespace(name="debug")],
        ),
        status=SimpleNamespace(
            container_statuses=[status("agent")],
            init_container_statuses=[status("wait-for-orchestrator")],
            ephemeral_container_statuses=[status("debug")],
            conditions=[],
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing_status,restart_policy,expected",
    [
        (None, "Never", True),
        ("container_statuses", "Never", False),
        ("init_container_statuses", "Never", False),
        ("ephemeral_container_statuses", "Never", False),
        (None, "Always", False),
    ],
)
async def test_existing_exact_pod_proof_requires_every_declared_container(
    missing_status, restart_policy, expected
):
    pod = terminal_pod()
    pod.spec.restart_policy = restart_policy
    if missing_status is not None:
        setattr(pod.status, missing_status, [])
    provisioner = AgentProvisioner.__new__(AgentProvisioner)
    provisioner.observe_agent_pod_exact = AsyncMock(
        return_value=("exact_terminal", pod)
    )
    assert (
        await provisioner.agent_pod_terminal_process_zero(
            POD_NAME, expected_pod_uid=POD_UID, namespace=NAMESPACE
        )
        is expected
    )


@pytest.mark.asyncio
async def test_bound_actor_proof_excludes_never_started_and_missing_finalizer():
    pod = terminal_pod()
    pod.metadata.deletion_timestamp = object()
    pod.status.container_statuses = []
    pod.status.init_container_statuses = []
    pod.status.ephemeral_container_statuses = []
    provisioner = AgentProvisioner.__new__(AgentProvisioner)
    provisioner.observe_agent_pod_exact = AsyncMock(
        return_value=("exact_terminal", pod)
    )
    assert await provisioner.agent_pod_terminal_process_zero(
        POD_NAME, expected_pod_uid=POD_UID, namespace=NAMESPACE
    )
    assert not await provisioner.agent_pod_terminal_process_zero(
        POD_NAME,
        expected_pod_uid=POD_UID,
        namespace=NAMESPACE,
        require_protected_full_terminal=True,
    )

    pod = terminal_pod()
    pod.metadata.finalizers = []
    provisioner.observe_agent_pod_exact = AsyncMock(
        return_value=("exact_terminal", pod)
    )
    assert not await provisioner.agent_pod_terminal_process_zero(
        POD_NAME,
        expected_pod_uid=POD_UID,
        namespace=NAMESPACE,
        require_protected_full_terminal=True,
    )


def operations(store, provisioner):
    return PinnedRetirementOperations(
        PinnedRetirementDependencies(
            store=store,
            agent_provisioner=provisioner,
            persistent_provisioner=MagicMock(),
            container_provisioner=MagicMock(),
            docker_provisioner=MagicMock(),
            vm_provisioner=MagicMock(),
            recovery_store=MagicMock(),
            session_router=MagicMock(),
            resolve_protected_reader_backend=AsyncMock(),
            resolve_ssh_key_path=lambda: None,
            logger=logging.getLogger(__name__),
        )
    )


def test_frozen_bound_source_free_shape_is_recognized():
    assert pinned_vm_creation_retirement.bound_vm_without_source(captured_context())


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", True),
        ("agent_id", None),
        ("runtime_attach_token", None),
        ("vm", {"status": "waiting_capacity"}),
        ("vm_creation_source", {"request_id": "issued"}),
        ("workspace_binding", {"generation": GENERATION}),
        ("workspace_container", {"pod_ip": "10.0.0.1"}),
        ("workspace_provision_intent", {"attempt_id": TOKEN}),
        ("entry_status", "active"),
        ("settle_status", "suspended"),
        ("protected_cloud_state", "malformed"),
        ("retained_soft_workspace", {}),
    ],
)
def test_frozen_bound_shape_refuses_missing_actor_or_external_authority(field, value):
    context = captured_context()
    context[field] = value
    assert not pinned_vm_creation_retirement.bound_vm_without_source(context)


@pytest.mark.parametrize(
    "field,value",
    [
        (
            "agent",
            {"id": AGENT, "thread_id": THREAD, "hostname": POD_NAME, "pod_uid": TOKEN},
        ),
        ("agent_pod", {"pod_name": POD_NAME, "pod_uid": POD_UID}),
        (
            "agent_pod",
            {
                "pod_name": POD_NAME,
                "pod_uid": POD_UID,
                "namespace": NAMESPACE,
                "runtime_generation": TOKEN,
                "protection_protocol": "finalizer_v1",
            },
        ),
    ],
)
def test_frozen_bound_shape_refuses_mismatched_or_partial_pod(field, value):
    context = captured_context()
    context[field] = value
    assert not pinned_vm_creation_retirement.bound_vm_without_source(context)


@pytest.mark.asyncio
async def test_owner_exact_terminal_receipts_before_finalizer_release():
    events = []
    context = captured_context()
    current = owner_row(context)
    receipt = local_receipt()
    store = MagicMock()
    store.get_thread = AsyncMock(return_value=current)
    store.fetchval = AsyncMock(return_value=True)
    store.acknowledge_pinned_thread_local_quiescence = AsyncMock(
        side_effect=lambda *_args, **_kwargs: events.append("receipt") or receipt
    )

    @asynccontextmanager
    async def lock(_thread):
        yield True

    store.try_thread_advisory_lock = lock
    provisioner = MagicMock(is_available=True)
    provisioner.delete_agent_pod_exact = AsyncMock(
        side_effect=lambda *_args, **_kwargs: events.append("delete") or True
    )
    provisioner.observe_agent_pod_exact = AsyncMock(
        return_value=("exact_terminal", terminal_pod())
    )
    provisioner.agent_pod_terminal_process_zero = AsyncMock(
        side_effect=lambda *_args, **_kwargs: events.append("full_zero") or True
    )
    provisioner.agent_pod_authority = AsyncMock(
        side_effect=["exact_terminal", "exact_absent"]
    )
    provisioner.release_agent_pod_finalizer_exact = AsyncMock(
        side_effect=lambda *_args, **_kwargs: events.append("release_finalizer") or True
    )
    owner = operations(store, provisioner)

    assert await owner._recover_bound_vm_no_source_agent_zero(
        retirement(context), current
    )
    assert events.index("delete") < events.index("full_zero")
    assert events.index("full_zero") < events.index("receipt")
    assert events.index("receipt") < events.index("release_finalizer")
    assert (
        provisioner.agent_pod_terminal_process_zero.await_args.kwargs[
            "require_protected_full_terminal"
        ]
        is True
    )
    assert (
        store.acknowledge_pinned_thread_local_quiescence.await_args.kwargs[
            "expected_agent_pod_uid"
        ]
        == POD_UID
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("observation", ["exact_absent", "replacement", "incomplete"])
async def test_owner_refuses_absence_replacement_or_partial_container_status(
    observation,
):
    context = captured_context()
    current = owner_row(context)
    store = MagicMock()
    store.get_thread = AsyncMock(return_value=current)
    store.fetchval = AsyncMock(return_value=True)
    store.acknowledge_pinned_thread_local_quiescence = AsyncMock()
    provisioner = MagicMock(is_available=True)
    provisioner.delete_agent_pod_exact = AsyncMock(return_value=True)
    provisioner.agent_pod_authority = AsyncMock(
        return_value="exact_terminal" if observation == "incomplete" else observation
    )
    provisioner.agent_pod_terminal_process_zero = AsyncMock(return_value=False)
    provisioner.release_agent_pod_finalizer_exact = AsyncMock(return_value=True)
    owner = operations(store, provisioner)

    assert not await owner._recover_bound_vm_no_source_agent_zero(
        retirement(context), current
    )
    store.acknowledge_pinned_thread_local_quiescence.assert_not_awaited()
    provisioner.release_agent_pod_finalizer_exact.assert_not_awaited()


@pytest.mark.asyncio
async def test_existing_exact_receipt_replays_after_pod_404():
    context = captured_context()
    receipt = local_receipt()
    current = owner_row(context, receipt)
    store = MagicMock()
    store.get_thread = AsyncMock(return_value=current)
    store.fetchval = AsyncMock(return_value=True)
    store.acknowledge_pinned_thread_local_quiescence = AsyncMock(return_value=receipt)
    provisioner = MagicMock(is_available=True)
    provisioner.agent_pod_authority = AsyncMock(return_value="exact_absent")
    provisioner.delete_agent_pod_exact = AsyncMock(return_value=True)
    provisioner.agent_pod_terminal_process_zero = AsyncMock(return_value=False)
    provisioner.release_agent_pod_finalizer_exact = AsyncMock(return_value=True)
    owner = operations(store, provisioner)

    assert await owner._recover_bound_vm_no_source_agent_zero(
        retirement(context), current
    )
    provisioner.agent_pod_terminal_process_zero.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("version", 2),
        ("runtime_generation", TOKEN),
        ("agent_id", TOKEN),
        ("runtime_attach_token", TOKEN),
        ("quiescence_actor", "agent"),
        ("workspace_generation", GENERATION),
        ("agent_pod_uid", TOKEN),
        ("vm_creation_request_id", TOKEN),
    ],
)
async def test_wrong_existing_receipt_never_releases_finalizer(field, value):
    context = captured_context()
    receipt = local_receipt()
    receipt[field] = value
    current = owner_row(context, receipt)
    store = MagicMock()
    store.get_thread = AsyncMock(return_value=current)
    store.fetchval = AsyncMock(return_value=True)
    provisioner = MagicMock(is_available=True)
    provisioner.delete_agent_pod_exact = AsyncMock(return_value=True)
    provisioner.release_agent_pod_finalizer_exact = AsyncMock(return_value=True)
    owner = operations(store, provisioner)

    assert not await owner._recover_bound_vm_no_source_agent_zero(
        retirement(context), current
    )
    provisioner.delete_agent_pod_exact.assert_not_awaited()
    provisioner.release_agent_pod_finalizer_exact.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("runtime_generation", TOKEN),
        ("runtime_retirement_token", GENERATION),
        ("agent_id", TOKEN),
        ("runtime_attach_token", TOKEN),
        ("status", "active"),
        ("metadata", {"agent_pod": {"pod_uid": TOKEN}}),
    ],
)
async def test_changed_current_actor_or_pod_refuses_before_delete(field, value):
    current = owner_row()
    current[field] = value
    store = MagicMock()
    store.get_thread = AsyncMock(return_value=current)
    store.fetchval = AsyncMock(return_value=True)
    provisioner = MagicMock(is_available=True)
    provisioner.delete_agent_pod_exact = AsyncMock(return_value=True)
    owner = operations(store, provisioner)

    assert not await owner._recover_bound_vm_no_source_agent_zero(retirement(), current)
    provisioner.delete_agent_pod_exact.assert_not_awaited()
