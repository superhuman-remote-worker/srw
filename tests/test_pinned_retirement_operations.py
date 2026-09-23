"""Focused contracts for the shared pinned-retirement operation owner."""

from contextlib import asynccontextmanager
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.services.pinned_retirement import (
    PinnedRetirementDependencies,
    PinnedRetirementOperations,
)
from orchestrator.services.vm_provisioner import (
    VMTeardownIdentity,
    VMTeardownResult,
)


THREAD_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
AGENT_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2"
RUNTIME_GENERATION = "11111111-1111-4111-8111-111111111111"
ATTACH_TOKEN = "22222222-2222-4222-8222-222222222222"
RETIREMENT_TOKEN = "33333333-3333-4333-8333-333333333333"
VM_GENERATION = "44444444-4444-4444-8444-444444444444"
CLAIM_ID = "66666666-6666-4666-8666-666666666666"
CLAIM_ATTEMPT = "77777777-7777-4777-8777-777777777777"
CLAIM_GENERATION = "88888888-8888-4888-8888-888888888888"


def _operations(
    *,
    store: object | None = None,
    agent_provisioner: object | None = None,
    persistent_provisioner: object | None = None,
    vm_provisioner: object | None = None,
    recovery_store: object | None = None,
) -> PinnedRetirementOperations:
    if recovery_store is None:
        recovery_store = MagicMock()
        recovery_store.acquire_cleanup_permit = AsyncMock(
            return_value=SimpleNamespace(allowed=True, admission_id=None)
        )
    return PinnedRetirementOperations(
        PinnedRetirementDependencies(
            store=store or MagicMock(),
            agent_provisioner=agent_provisioner or MagicMock(),
            persistent_provisioner=persistent_provisioner or MagicMock(),
            container_provisioner=MagicMock(),
            docker_provisioner=MagicMock(),
            vm_provisioner=vm_provisioner or MagicMock(),
            recovery_store=recovery_store,
            session_router=MagicMock(),
            resolve_protected_reader_backend=AsyncMock(),
            resolve_ssh_key_path=lambda: None,
            logger=logging.getLogger(__name__),
        )
    )


@pytest.mark.asyncio
async def test_begin_adopts_legacy_authority_before_freezing_retirement() -> None:
    store = MagicMock()
    store.begin_pinned_thread_retirement = AsyncMock(
        return_value={"state": "pending", "token": RETIREMENT_TOKEN}
    )
    agent_provisioner = MagicMock()
    persistent_provisioner = MagicMock()
    operations = _operations(
        store=store,
        agent_provisioner=agent_provisioner,
        persistent_provisioner=persistent_provisioner,
    )

    with patch(
        "orchestrator.services.pinned_retirement."
        "reconcile_legacy_pinned_agent_authority",
        AsyncMock(return_value=SimpleNamespace(complete=True)),
    ) as adopt:
        result = await operations.begin_pinned_thread_retirement(
            THREAD_ID,
            permanent=True,
            settle_status="ended",
        )

    assert result == {"state": "pending", "token": RETIREMENT_TOKEN}
    adopt.assert_awaited_once_with(
        store,
        agent_provisioner=agent_provisioner,
        persistent_provisioner=persistent_provisioner,
        thread_id=THREAD_ID,
        limit=2,
    )
    store.begin_pinned_thread_retirement.assert_awaited_once_with(
        THREAD_ID,
        permanent=True,
        settle_status="ended",
    )


@pytest.mark.asyncio
async def test_begin_refuses_when_legacy_authority_remains_unresolved() -> None:
    store = MagicMock()
    store.begin_pinned_thread_retirement = AsyncMock()
    operations = _operations(store=store)

    with patch(
        "orchestrator.services.pinned_retirement."
        "reconcile_legacy_pinned_agent_authority",
        AsyncMock(return_value=SimpleNamespace(complete=False)),
    ):
        result = await operations.begin_pinned_thread_retirement(
            THREAD_ID,
            permanent=True,
        )

    assert result == {
        "state": "malformed",
        "reason": "agent_k8s_authority_adoption_unresolved",
    }
    store.begin_pinned_thread_retirement.assert_not_awaited()


def _claim_retirement(*, permanent: bool, status: str, pvc_uid: str | None):
    return {
        "generation": RUNTIME_GENERATION,
        "token": RETIREMENT_TOKEN,
        "permanent": permanent,
        "context": {
            "thread_id": THREAD_ID,
            "generation": RUNTIME_GENERATION,
            "agent_workspace_claim": {
                "claim_id": CLAIM_ID,
                "thread_id": THREAD_ID,
                "created_runtime_generation": CLAIM_GENERATION,
                "create_attempt": CLAIM_ATTEMPT,
                "provisioner": "agent",
                "pvc_name": "pvc-agent-s-aaaaaaaa-aaa",
                "status": status,
                "pvc_uid": pvc_uid,
                "namespace": "agents-a",
                "protection_protocol": "finalizer_v1",
            },
        },
    }


@pytest.mark.asyncio
async def test_soft_retirement_retain_publishes_exact_claim_uid() -> None:
    retirement = _claim_retirement(permanent=False, status="planned", pvc_uid=None)
    store = MagicMock()
    store.publish_pinned_agent_workspace_claim = AsyncMock(return_value=True)
    provider = MagicMock()
    provider.is_available = True
    provider.ensure_agent_workspace_claim = AsyncMock(return_value="retained-pvc-uid")
    operations = _operations(store=store, agent_provisioner=provider)

    await operations.reconcile_agent_workspace_claim_for_retirement(retirement)

    provider.ensure_agent_workspace_claim.assert_awaited_once_with(
        "pvc-agent-s-aaaaaaaa-aaa",
        expected_thread_id=THREAD_ID,
        expected_runtime_generation=CLAIM_GENERATION,
        expected_claim_id=CLAIM_ID,
        expected_create_attempt=CLAIM_ATTEMPT,
        namespace="agents-a",
        expected_pvc_uid=None,
    )
    store.publish_pinned_agent_workspace_claim.assert_awaited_once_with(
        THREAD_ID,
        expected_runtime_generation=RUNTIME_GENERATION,
        expected_retirement_token=RETIREMENT_TOKEN,
        claim_id=CLAIM_ID,
        pvc_name="pvc-agent-s-aaaaaaaa-aaa",
        pvc_uid="retained-pvc-uid",
        namespace="agents-a",
    )


@pytest.mark.asyncio
async def test_permanent_retirement_deletes_original_before_pvc_fence() -> None:
    retirement = _claim_retirement(
        permanent=True,
        status="ready",
        pvc_uid="original-pvc-uid",
    )
    provider = MagicMock()
    provider.is_available = True
    provider.fence_agent_workspace_claim = AsyncMock(
        side_effect=[
            {"state": "exact_original", "pvc_uid": "original-pvc-uid"},
            {"state": "exact_fence", "pvc_uid": "fence-pvc-uid"},
        ]
    )
    provider.delete_agent_workspace_claim_exact = AsyncMock(return_value=True)
    provider.release_agent_workspace_claim_finalizer_exact = AsyncMock(
        return_value=True
    )
    store = MagicMock()
    store.revoke_pinned_agent_workspace_claim = AsyncMock(return_value=True)
    store.fetchrow = AsyncMock(return_value={"status": "revoking", "pvc_uid": None})
    store.fence_pinned_agent_workspace_claim = AsyncMock(return_value=True)
    store.get_thread = AsyncMock(
        return_value={
            "runtime_generation": RUNTIME_GENERATION,
            "runtime_retirement_token": RETIREMENT_TOKEN,
            "runtime_retirement_permanent": True,
            "runtime_retirement_authorized_at": "authorized",
        }
    )
    store.fetch = AsyncMock(return_value=[])
    operations = _operations(store=store, agent_provisioner=provider)

    with patch(
        "orchestrator.services.pinned_retirement.asyncio.sleep",
        AsyncMock(),
    ):
        await operations.reconcile_agent_workspace_claim_for_retirement(retirement)

    provider.delete_agent_workspace_claim_exact.assert_awaited_once_with(
        "pvc-agent-s-aaaaaaaa-aaa",
        expected_pvc_uid="original-pvc-uid",
        namespace="agents-a",
    )
    store.fence_pinned_agent_workspace_claim.assert_awaited_once_with(
        THREAD_ID,
        expected_runtime_generation=RUNTIME_GENERATION,
        expected_retirement_token=RETIREMENT_TOKEN,
        expected_claim_id=CLAIM_ID,
        expected_pvc_name="pvc-agent-s-aaaaaaaa-aaa",
        fence_pvc_uid="fence-pvc-uid",
    )


def _vm_retirement() -> dict[str, object]:
    pod_name = "srw-agent-pinned"
    pod_uid = "55555555-5555-4555-8555-555555555555"
    return {
        "generation": RUNTIME_GENERATION,
        "token": RETIREMENT_TOKEN,
        "permanent": True,
        "context": {
            "thread_id": THREAD_ID,
            "entry_status": "active",
            "settle_status": "ended",
            "runtime_authority_exposed": True,
            "workspace_backend": "vm",
            "agent_id": AGENT_ID,
            "runtime_attach_token": ATTACH_TOKEN,
            "agent": {"hostname": pod_name, "pod_uid": pod_uid},
            "agent_pod": {
                "pod_name": pod_name,
                "pod_uid": pod_uid,
                "namespace": "agents-a",
                "protection_protocol": "finalizer_v1",
            },
            "workspace_container": {},
            "workspace_binding": {},
            "workspace_provision_intent": {},
            "vm": {
                "provision_generation": VM_GENERATION,
                "identity_provision_generation": VM_GENERATION,
                "identity_authenticated": True,
                "vm_uid": "vm-uid-a",
                "_runtime_incarnation": "vm-uid-a",
                "rootdisk_pvc_uid": "rootdisk-uid-a",
                "ssh_host": "vm.internal",
                "ssh_port": 22,
                "ssh_host_key_fingerprint": "SHA256:" + ("A" * 43),
                "credential_runtime_started": True,
            },
        },
    }


def _current_vm_thread() -> dict[str, object]:
    return {
        "id": THREAD_ID,
        "status": "active",
        "runtime_generation": RUNTIME_GENERATION,
        "runtime_retirement_token": RETIREMENT_TOKEN,
        "runtime_retirement_permanent": True,
        "runtime_retirement_authorized_at": "authorized",
        "runtime_retirement_local_quiescence": None,
        "metadata": {},
    }


@pytest.mark.asyncio
async def test_vm_recovery_stops_exact_pod_before_releasing_captured_vm() -> None:
    events: list[str] = []
    store = MagicMock()
    store.get_thread = AsyncMock(
        side_effect=[_current_vm_thread(), _current_vm_thread()]
    )

    @asynccontextmanager
    async def lifecycle_lock(_thread_id: str):
        yield True

    store.try_thread_advisory_lock = lifecycle_lock

    async def acknowledge(*_args, **_kwargs):
        events.append("acknowledge")
        return {"version": 1}

    store.acknowledge_pinned_thread_local_quiescence = AsyncMock(
        side_effect=acknowledge
    )

    agent_provisioner = MagicMock()
    agent_provisioner.is_available = True

    async def delete_pod(*_args, **_kwargs):
        events.append("delete_pod")
        return True

    async def release_pod_finalizer(*_args, **_kwargs):
        events.append("release_pod_finalizer")
        return True

    agent_provisioner.delete_agent_pod_exact = AsyncMock(side_effect=delete_pod)
    agent_provisioner.agent_pod_authority = AsyncMock(
        side_effect=["exact_terminal", "exact_absent"]
    )
    agent_provisioner.release_agent_pod_finalizer_exact = AsyncMock(
        side_effect=release_pod_finalizer
    )

    vm_provisioner = MagicMock()
    vm_provisioner.lifecycle_available = True

    async def release_vm(*_args, **_kwargs):
        events.append("release_vm")
        return VMTeardownResult("completed", True)

    vm_provisioner.release_vm_captured = AsyncMock(side_effect=release_vm)
    operations = _operations(
        store=store,
        agent_provisioner=agent_provisioner,
        vm_provisioner=vm_provisioner,
    )

    assert await operations.recover_captured_process_zero(_vm_retirement())

    assert events == [
        "delete_pod",
        "release_pod_finalizer",
        "release_vm",
        "acknowledge",
    ]
    identity = VMTeardownIdentity(
        provision_generation=VM_GENERATION,
        vm_uid="vm-uid-a",
        rootdisk_pvc_uid="rootdisk-uid-a",
        ssh_host="vm.internal",
        ssh_port=22,
        ssh_host_key_fingerprint="SHA256:" + ("A" * 43),
        credential_runtime_started=True,
    )
    vm_provisioner.release_vm_captured.assert_awaited_once_with(
        THREAD_ID,
        identity,
        ssh_host="vm.internal",
        ssh_port=22,
        purge_disk=True,
        entity_type="thread",
        capture_snapshot=False,
    )
    store.acknowledge_pinned_thread_local_quiescence.assert_awaited_once_with(
        THREAD_ID,
        expected_runtime_generation=RUNTIME_GENERATION,
        expected_retirement_token=RETIREMENT_TOKEN,
        expected_agent_id=AGENT_ID,
        expected_attach_token=ATTACH_TOKEN,
        expected_settle_status="ended",
        expected_quiescence_protocol="workspace_actuator_zero_v1",
        expected_workspace_generation=VM_GENERATION,
        expected_workspace_runtime_incarnation="vm-uid-a",
        quiescence_actor="orchestrator",
    )


@pytest.mark.asyncio
async def test_idle_soft_retirement_stops_captured_pod_before_vm_effect() -> None:
    events: list[str] = []
    retirement = _vm_retirement()
    retirement["permanent"] = False
    retirement["context"]["generation"] = RUNTIME_GENERATION
    retirement["context"]["settle_status"] = "suspended"
    store = MagicMock()
    store.get_thread = AsyncMock(return_value={
        **_current_vm_thread(), "runtime_retirement_permanent": False,
    })
    actor = MagicMock()
    actor.is_available = True

    async def delete_pod(*_args, **_kwargs):
        events.append("agent_stop")
        return True

    async def release_vm(*_args, **_kwargs):
        events.append("vm_stop")
        return VMTeardownResult("completed", True)

    actor.delete_agent_pod_exact = AsyncMock(side_effect=delete_pod)
    actor.agent_pod_authority = AsyncMock(
        side_effect=["exact_terminal", "exact_absent"]
    )
    actor.release_agent_pod_finalizer_exact = AsyncMock(return_value=True)
    vm = MagicMock()
    vm.lifecycle_available = True
    vm.release_vm_captured = AsyncMock(side_effect=release_vm)
    operations = _operations(
        store=store, agent_provisioner=actor, vm_provisioner=vm,
    )
    operations.dependencies.session_router.teardown_route = AsyncMock(
        return_value=True,
    )
    with (
        patch.object(
            PinnedRetirementOperations,
            "_reconcile_workspace_provision_intent_for_retirement",
            AsyncMock(return_value=False),
        ),
        patch.object(
            PinnedRetirementOperations, "_admit_vm_cleanup",
            AsyncMock(return_value=SimpleNamespace(allowed=True)),
        ),
        patch.object(
            PinnedRetirementOperations, "_complete_vm_cleanup", AsyncMock(),
        ),
        patch.object(
            PinnedRetirementOperations,
            "_reconcile_agent_workspace_claim_for_retirement", AsyncMock(),
        ),
    ):
        await operations.cleanup_pinned_thread_retirement(
            retirement, cleanup_agent_pod=True,
            stop_agent_before_workspace=True,
        )
    assert events == ["agent_stop", "vm_stop"]
    actor.delete_agent_pod_exact.assert_awaited_once_with(
        "srw-agent-pinned",
        expected_pod_uid="55555555-5555-4555-8555-555555555555",
        namespace="agents-a",
    )


@pytest.mark.asyncio
async def test_vm_recovery_hold_blocks_pinned_retirement_vm_release() -> None:
    store = MagicMock()
    store.get_thread = AsyncMock(
        side_effect=[_current_vm_thread(), _current_vm_thread()]
    )

    @asynccontextmanager
    async def lifecycle_lock(_thread_id: str):
        yield True

    store.try_thread_advisory_lock = lifecycle_lock
    agent_provisioner = MagicMock()
    agent_provisioner.is_available = True
    agent_provisioner.delete_agent_pod_exact = AsyncMock(return_value=True)
    agent_provisioner.agent_pod_authority = AsyncMock(
        side_effect=["exact_terminal", "exact_absent"]
    )
    agent_provisioner.release_agent_pod_finalizer_exact = AsyncMock(return_value=True)
    vm_provisioner = MagicMock()
    vm_provisioner.lifecycle_available = True
    vm_provisioner.release_vm_captured = AsyncMock()
    recovery_store = MagicMock()
    recovery_store.acquire_cleanup_permit = AsyncMock(
        return_value=SimpleNamespace(
            allowed=False,
            reason="workspace_recovery_unresolved",
        )
    )
    operations = _operations(
        store=store,
        agent_provisioner=agent_provisioner,
        vm_provisioner=vm_provisioner,
        recovery_store=recovery_store,
    )

    assert not await operations.recover_captured_process_zero(_vm_retirement())

    recovery_store.acquire_cleanup_permit.assert_awaited_once()
    vm_provisioner.release_vm_captured.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("identity_authenticated", False),
        ("identity_provision_generation", RUNTIME_GENERATION),
        ("rootdisk_pvc_uid", ""),
    ],
)
async def test_vm_recovery_refuses_incomplete_or_contradictory_authority(
    field: str,
    value: object,
) -> None:
    retirement = _vm_retirement()
    retirement["context"]["vm"][field] = value
    store = MagicMock()
    store.get_thread = AsyncMock(return_value=_current_vm_thread())
    store.acknowledge_pinned_thread_local_quiescence = AsyncMock()
    agent_provisioner = MagicMock()
    agent_provisioner.is_available = True
    agent_provisioner.delete_agent_pod_exact = AsyncMock()
    vm_provisioner = MagicMock()
    vm_provisioner.lifecycle_available = True
    vm_provisioner.release_vm_captured = AsyncMock()
    operations = _operations(
        store=store,
        agent_provisioner=agent_provisioner,
        vm_provisioner=vm_provisioner,
    )

    assert not await operations.recover_captured_process_zero(retirement)

    agent_provisioner.delete_agent_pod_exact.assert_not_awaited()
    vm_provisioner.release_vm_captured.assert_not_awaited()
    store.acknowledge_pinned_thread_local_quiescence.assert_not_awaited()
