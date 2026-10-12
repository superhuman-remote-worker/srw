"""Guards of the pinned Kubernetes leader loops at their R1.B11 owner.

``pinned_agent_create_intent_reconciler`` may only publish an exact,
finalizer-protected create it can attribute to one row; the create-fence GC
may only retire a fence row once the exact Kubernetes name is proven absent.
The end-to-end promotion/fence scenarios stay in
``tests/test_pinned_retirement_lifecycle.py``; these cover the skip guards.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.services import pinned_k8s_reconciliation as reconciliation
from orchestrator.services.pinned_k8s_reconciliation import (
    PinnedK8sReconciliationDependencies,
)

THREAD_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
RUNTIME_GENERATION = "11111111-1111-4111-8111-111111111111"
CLAIM_ID = "44444444-4444-4444-8444-444444444444"
CLAIM_ATTEMPT = "55555555-5555-4555-8555-555555555555"
CLAIM_GENERATION = "66666666-6666-4666-8666-666666666666"


def _dependencies(db, **provisioners) -> PinnedK8sReconciliationDependencies:
    """Explicit collaborators; unnamed provisioners are present but unavailable."""

    fields = {
        "agent_provisioner": MagicMock(name="agent_provisioner", is_available=False),
        "persistent_provisioner": MagicMock(
            name="persistent_provisioner", is_available=False
        ),
        "container_provisioner": MagicMock(
            name="container_provisioner", is_available=False
        ),
    }
    fields.update(provisioners)
    return PinnedK8sReconciliationDependencies(store=db, **fields)


def _provider():
    provider = MagicMock()
    provider.is_available = True
    provider.agent_workspace_claim_authority = AsyncMock(
        return_value={"state": "exact_present", "pvc_uid": "observed-pvc-uid"}
    )
    provider.agent_pod_provision_intent_authority = AsyncMock(
        return_value={"state": "exact_present", "pod_uid": "observed-pod-uid"}
    )
    return provider


def _intent_row(**overrides):
    row = {
        "attempt_id": CLAIM_ATTEMPT,
        "thread_id": THREAD_ID,
        "runtime_generation": RUNTIME_GENERATION,
        "provisioner": "agent",
        "pod_name": "srw-agent-s-fence",
        "namespace": "agents-a",
        "protection_protocol": "finalizer_v1",
        "workspace_claim": None,
    }
    row.update(overrides)
    return row


def _claim(**overrides):
    claim = {
        "claim_id": CLAIM_ID,
        "created_runtime_generation": CLAIM_GENERATION,
        "create_attempt": CLAIM_ATTEMPT,
        "pvc_name": "pvc-agent-s-aaaaaaaa-aaa",
        "status": "planned",
        "pvc_uid": None,
        "namespace": "agents-a",
        "protection_protocol": "finalizer_v1",
    }
    claim.update(overrides)
    return claim


def _intent_db(shutdown, rows):
    db = MagicMock()

    async def _list(**_kwargs):
        shutdown.set()
        return rows

    db.list_pinned_agent_create_intents_for_reconcile = AsyncMock(side_effect=_list)
    db.publish_pinned_agent_workspace_claim = AsyncMock(return_value=True)
    db.publish_pinned_agent_pod_provision_intent = AsyncMock(return_value=True)
    return db


async def _reconcile_once(db, **provisioners):
    shutdown = db._shutdown
    legacy = AsyncMock(return_value=SimpleNamespace(unresolved=0))
    with patch.object(
        reconciliation, "reconcile_legacy_pinned_agent_authority", legacy
    ):
        await asyncio.wait_for(
            reconciliation.pinned_agent_create_intent_reconciler(
                shutdown, dependencies=_dependencies(db, **provisioners)
            ),
            timeout=2,
        )
    return legacy


def _bind_shutdown(rows):
    shutdown = asyncio.Event()
    db = _intent_db(shutdown, rows)
    db._shutdown = shutdown
    return db


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"protection_protocol": None},
        {"protection_protocol": "legacy_v0"},
        {"thread_id": None},
        {"runtime_generation": ""},
        {"attempt_id": None},
        {"pod_name": ""},
        {"namespace": None},
        {"provisioner": "vm"},
        {"provisioner": None},
    ],
)
async def test_reconciler_skips_unprotected_or_unattributable_intents(overrides):
    provider = _provider()
    db = _bind_shutdown([_intent_row(**overrides)])

    await _reconcile_once(db, agent_provisioner=provider)

    provider.agent_workspace_claim_authority.assert_not_awaited()
    provider.agent_pod_provision_intent_authority.assert_not_awaited()
    db.publish_pinned_agent_workspace_claim.assert_not_awaited()
    db.publish_pinned_agent_pod_provision_intent.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconciler_skips_an_unavailable_provisioner():
    provider = _provider()
    provider.is_available = False
    db = _bind_shutdown([_intent_row()])

    await _reconcile_once(db, agent_provisioner=provider)

    provider.agent_pod_provision_intent_authority.assert_not_awaited()
    db.publish_pinned_agent_pod_provision_intent.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconciler_publishes_an_exact_claimless_pod_intent_once():
    provider = _provider()
    persistent = _provider()
    db = _bind_shutdown([_intent_row(provisioner="persistent")])

    await _reconcile_once(
        db, agent_provisioner=provider, persistent_provisioner=persistent
    )

    provider.agent_pod_provision_intent_authority.assert_not_awaited()
    persistent.agent_pod_provision_intent_authority.assert_awaited_once_with(
        "srw-agent-s-fence",
        expected_thread_id=THREAD_ID,
        expected_runtime_generation=RUNTIME_GENERATION,
        expected_attempt_id=CLAIM_ATTEMPT,
        namespace="agents-a",
    )
    db.publish_pinned_agent_workspace_claim.assert_not_awaited()
    db.publish_pinned_agent_pod_provision_intent.assert_awaited_once_with(
        THREAD_ID,
        expected_runtime_generation=RUNTIME_GENERATION,
        attempt_id=CLAIM_ATTEMPT,
        pod_name="srw-agent-s-fence",
        pod_uid="observed-pod-uid",
        namespace="agents-a",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "claim",
    [
        ["not", "a", "mapping"],
        _claim(claim_id=None),
        _claim(created_runtime_generation=""),
        _claim(create_attempt=None),
        _claim(pvc_name=""),
        _claim(namespace=None),
        _claim(status="released"),
        _claim(namespace="agents-b"),
        _claim(protection_protocol="legacy_v0"),
    ],
)
async def test_reconciler_skips_a_row_whose_claim_is_not_exact(claim):
    provider = _provider()
    db = _bind_shutdown([_intent_row(workspace_claim=claim)])

    await _reconcile_once(db, agent_provisioner=provider)

    provider.agent_workspace_claim_authority.assert_not_awaited()
    provider.agent_pod_provision_intent_authority.assert_not_awaited()
    db.publish_pinned_agent_workspace_claim.assert_not_awaited()
    db.publish_pinned_agent_pod_provision_intent.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "observed",
    [
        None,
        {"state": "exact_present", "pvc_uid": None},
        {"state": "exact_fence", "pvc_uid": "recorded-pvc-uid"},
        {"state": "exact_present", "pvc_uid": "some-other-uid"},
    ],
)
async def test_reconciler_requires_the_exact_present_claim_uid(observed):
    provider = _provider()
    provider.agent_workspace_claim_authority = AsyncMock(return_value=observed)
    db = _bind_shutdown(
        [
            _intent_row(
                workspace_claim=_claim(status="ready", pvc_uid="recorded-pvc-uid")
            )
        ]
    )

    await _reconcile_once(db, agent_provisioner=provider)

    provider.agent_workspace_claim_authority.assert_awaited_once_with(
        "pvc-agent-s-aaaaaaaa-aaa",
        expected_thread_id=THREAD_ID,
        expected_runtime_generation=CLAIM_GENERATION,
        expected_claim_id=CLAIM_ID,
        expected_create_attempt=CLAIM_ATTEMPT,
        namespace="agents-a",
        expected_pvc_uid="recorded-pvc-uid",
    )
    provider.agent_pod_provision_intent_authority.assert_not_awaited()
    db.publish_pinned_agent_pod_provision_intent.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconciler_does_not_republish_a_ready_claim():
    provider = _provider()
    provider.agent_workspace_claim_authority = AsyncMock(
        return_value={"state": "exact_present", "pvc_uid": "recorded-pvc-uid"}
    )
    db = _bind_shutdown(
        [
            _intent_row(
                workspace_claim=_claim(status="ready", pvc_uid="recorded-pvc-uid")
            )
        ]
    )

    await _reconcile_once(db, agent_provisioner=provider)

    db.publish_pinned_agent_workspace_claim.assert_not_awaited()
    db.publish_pinned_agent_pod_provision_intent.assert_awaited_once()


@pytest.mark.asyncio
async def test_reconciler_stops_the_row_when_claim_publication_loses_the_cas():
    provider = _provider()
    db = _bind_shutdown([_intent_row(workspace_claim=_claim())])
    db.publish_pinned_agent_workspace_claim = AsyncMock(return_value=False)

    await _reconcile_once(db, agent_provisioner=provider)

    db.publish_pinned_agent_workspace_claim.assert_awaited_once()
    provider.agent_pod_provision_intent_authority.assert_not_awaited()
    db.publish_pinned_agent_pod_provision_intent.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "observed",
    [
        None,
        {"state": "exact_present", "pod_uid": None},
        {"state": "exact_fence", "pod_uid": "fence-uid"},
        {"state": "replacement", "pod_uid": "foreign-uid"},
    ],
)
async def test_reconciler_publishes_only_an_exact_present_pod_uid(observed):
    provider = _provider()
    provider.agent_pod_provision_intent_authority = AsyncMock(return_value=observed)
    db = _bind_shutdown([_intent_row()])

    await _reconcile_once(db, agent_provisioner=provider)

    provider.agent_pod_provision_intent_authority.assert_awaited_once()
    db.publish_pinned_agent_pod_provision_intent.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconciler_isolates_a_failing_row(caplog):
    provider = _provider()
    provider.agent_pod_provision_intent_authority = AsyncMock(
        side_effect=[
            RuntimeError("apiserver hiccup"),
            {"state": "exact_present", "pod_uid": "observed-pod-uid"},
        ]
    )
    second_attempt = "77777777-7777-4777-8777-777777777777"
    db = _bind_shutdown([_intent_row(), _intent_row(attempt_id=second_attempt)])
    caplog.set_level(logging.ERROR, logger=reconciliation.__name__)

    await _reconcile_once(db, agent_provisioner=provider)

    assert provider.agent_pod_provision_intent_authority.await_count == 2
    db.publish_pinned_agent_pod_provision_intent.assert_awaited_once()
    assert (
        db.publish_pinned_agent_pod_provision_intent.await_args.kwargs["attempt_id"]
        == second_attempt
    )
    failures = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert [r.getMessage() for r in failures] == [
        f"Pinned agent create-intent reconciliation failed for {CLAIM_ATTEMPT}"
    ]
    assert failures[0].exc_info is not None


@pytest.mark.asyncio
async def test_reconciler_warns_on_unresolved_legacy_authority_first(caplog):
    shutdown = asyncio.Event()
    db = _intent_db(shutdown, [])
    agent = MagicMock(name="agent", is_available=True)
    persistent = MagicMock(name="persistent", is_available=True)
    order = []

    async def legacy(store, **kwargs):
        order.append("legacy")
        return SimpleNamespace(unresolved=3)

    async def listing(**kwargs):
        order.append("list")
        shutdown.set()
        return []

    db.list_pinned_agent_create_intents_for_reconcile = AsyncMock(side_effect=listing)
    legacy_mock = AsyncMock(side_effect=legacy)
    caplog.set_level(logging.WARNING, logger=reconciliation.__name__)
    with patch.object(
        reconciliation, "reconcile_legacy_pinned_agent_authority", legacy_mock
    ):
        await reconciliation.pinned_agent_create_intent_reconciler(
            shutdown,
            dependencies=_dependencies(
                db, agent_provisioner=agent, persistent_provisioner=persistent
            ),
        )

    assert order == ["legacy", "list"]
    legacy_mock.assert_awaited_once_with(
        db, agent_provisioner=agent, persistent_provisioner=persistent, limit=50
    )
    db.list_pinned_agent_create_intents_for_reconcile.assert_awaited_once_with(limit=50)
    assert [r.getMessage() for r in caplog.records] == [
        "Pinned legacy Kubernetes authority remains unresolved for 3 row(s)"
    ]


@pytest.mark.asyncio
async def test_reconciler_survives_a_failed_pass_and_runs_the_next(monkeypatch, caplog):
    """A pass-level failure is logged and the loop keeps its cadence."""

    monkeypatch.setenv("PINNED_AGENT_CREATE_RECONCILE_INTERVAL_SECONDS", "0")
    shutdown = asyncio.Event()
    db = _intent_db(shutdown, [])
    calls = 0

    async def legacy(store, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("legacy scan exploded")
        shutdown.set()
        return SimpleNamespace(unresolved=0)

    waits = []
    real_wait_for = asyncio.wait_for

    async def fast_wait_for(awaitable, timeout):
        waits.append(timeout)
        return await real_wait_for(awaitable, timeout=0.01)

    caplog.set_level(logging.ERROR, logger=reconciliation.__name__)
    with (
        patch.object(
            reconciliation,
            "reconcile_legacy_pinned_agent_authority",
            AsyncMock(side_effect=legacy),
        ),
        patch.object(reconciliation.asyncio, "wait_for", fast_wait_for),
    ):
        await real_wait_for(
            reconciliation.pinned_agent_create_intent_reconciler(
                shutdown, dependencies=_dependencies(db)
            ),
            timeout=2,
        )

    assert calls == 2
    # The interval is floored at five seconds whatever the environment says.
    assert waits and set(waits) == {5}
    assert [r.getMessage() for r in caplog.records] == [
        "Pinned agent create-intent reconciliation pass failed"
    ]


# ---------------------------------------------------------------------------
# pinned_k8s_create_fence_gc_sweeper
# ---------------------------------------------------------------------------


def _fence_row(**overrides):
    row = {
        "resource_kind": "pod",
        "authority_id": CLAIM_ATTEMPT,
        "thread_id": THREAD_ID,
        "runtime_generation": RUNTIME_GENERATION,
        "create_attempt": CLAIM_ATTEMPT,
        "provisioner": "agent",
        "resource_name": "srw-agent-s-fence",
        "resource_uid": "fence-uid",
        "namespace": "agents-a",
        "protection_protocol": "finalizer_v1",
    }
    row.update(overrides)
    return row


def _fence_provider(*, pod_states=(), pvc_states=()):
    provider = MagicMock()
    provider.is_available = True
    provider.agent_pod_provision_intent_authority = AsyncMock(
        side_effect=[{"state": state, "pod_uid": uid} for state, uid in pod_states]
    )
    provider.agent_workspace_claim_authority = AsyncMock(
        side_effect=[{"state": state, "pvc_uid": uid} for state, uid in pvc_states]
    )
    provider.delete_agent_pod_exact = AsyncMock(return_value=True)
    provider.release_agent_pod_finalizer_exact = AsyncMock(return_value=True)
    provider.delete_agent_workspace_claim_exact = AsyncMock(return_value=True)
    provider.release_agent_workspace_claim_finalizer_exact = AsyncMock(
        return_value=True
    )
    return provider


async def _sweep_once(rows, **provisioners):
    shutdown = asyncio.Event()
    db = MagicMock()

    async def _list(**_kwargs):
        shutdown.set()
        return rows

    db.list_due_pinned_k8s_create_fences = AsyncMock(side_effect=_list)
    db.complete_pinned_k8s_create_fence_gc = AsyncMock(return_value=True)
    db.list_pinned_thread_workspace_provision_fences_for_gc = AsyncMock(return_value=[])
    db.retire_pinned_thread_workspace_provision_fence = AsyncMock(return_value=True)
    await asyncio.wait_for(
        reconciliation.pinned_k8s_create_fence_gc_sweeper(
            shutdown, dependencies=_dependencies(db, **provisioners)
        ),
        timeout=2,
    )
    return db


@pytest.mark.asyncio
async def test_fence_gc_completes_a_pod_fence_only_after_exact_absence():
    provider = _fence_provider(
        pod_states=[("exact_fence", "fence-uid"), ("exact_fence", "fence-uid")]
    )

    db = await _sweep_once([_fence_row()], agent_provisioner=provider)

    provider.delete_agent_pod_exact.assert_awaited_once()
    provider.release_agent_pod_finalizer_exact.assert_awaited_once_with(
        "srw-agent-s-fence",
        expected_pod_uid="fence-uid",
        namespace="agents-a",
        terminal_required=False,
    )
    assert provider.agent_pod_provision_intent_authority.await_count == 2
    db.complete_pinned_k8s_create_fence_gc.assert_not_awaited()


@pytest.mark.asyncio
async def test_fence_gc_completes_an_already_absent_pod_without_deleting():
    provider = _fence_provider(pod_states=[("exact_absent", None)])

    db = await _sweep_once([_fence_row()], agent_provisioner=provider)

    provider.delete_agent_pod_exact.assert_not_awaited()
    db.complete_pinned_k8s_create_fence_gc.assert_awaited_once_with(
        resource_kind="pod",
        authority_id=CLAIM_ATTEMPT,
        expected_resource_uid="fence-uid",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_step", ["delete", "release"])
async def test_fence_gc_stops_a_pod_fence_when_an_exact_step_fails(failing_step):
    provider = _fence_provider(
        pod_states=[("exact_fence", "fence-uid"), ("exact_absent", None)]
    )
    if failing_step == "delete":
        provider.delete_agent_pod_exact = AsyncMock(return_value=False)
    else:
        provider.release_agent_pod_finalizer_exact = AsyncMock(return_value=False)

    db = await _sweep_once([_fence_row()], agent_provisioner=provider)

    if failing_step == "delete":
        provider.release_agent_pod_finalizer_exact.assert_not_awaited()
    assert provider.agent_pod_provision_intent_authority.await_count == 1
    db.complete_pinned_k8s_create_fence_gc.assert_not_awaited()


@pytest.mark.asyncio
async def test_fence_gc_never_deletes_a_pod_whose_uid_moved():
    provider = _fence_provider(pod_states=[("exact_fence", "successor-uid")])

    db = await _sweep_once([_fence_row()], agent_provisioner=provider)

    provider.delete_agent_pod_exact.assert_not_awaited()
    db.complete_pinned_k8s_create_fence_gc.assert_not_awaited()


@pytest.mark.asyncio
async def test_fence_gc_deletes_a_persistent_pod_by_thread_identity():
    persistent = _fence_provider(
        pod_states=[("exact_fence", "fence-uid"), ("exact_absent", None)]
    )
    agent = _fence_provider()

    db = await _sweep_once(
        [_fence_row(provisioner="persistent")],
        agent_provisioner=agent,
        persistent_provisioner=persistent,
    )

    agent.delete_agent_pod_exact.assert_not_awaited()
    persistent.delete_agent_pod_exact.assert_awaited_once_with(
        THREAD_ID, expected_pod_uid="fence-uid", namespace="agents-a"
    )
    persistent.release_agent_pod_finalizer_exact.assert_awaited_once_with(
        THREAD_ID,
        expected_pod_uid="fence-uid",
        namespace="agents-a",
        terminal_required=False,
    )
    db.complete_pinned_k8s_create_fence_gc.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_step", [None, "delete", "release"])
async def test_fence_gc_completes_a_pvc_fence_only_after_exact_absence(failing_step):
    provider = _fence_provider(
        pvc_states=[("exact_fence", "fence-uid"), ("exact_fence", "fence-uid")]
    )
    if failing_step == "delete":
        provider.delete_agent_workspace_claim_exact = AsyncMock(return_value=False)
    elif failing_step == "release":
        provider.release_agent_workspace_claim_finalizer_exact = AsyncMock(
            return_value=False
        )

    db = await _sweep_once(
        [_fence_row(resource_kind="pvc", authority_id=CLAIM_ID)],
        agent_provisioner=provider,
    )

    provider.delete_agent_workspace_claim_exact.assert_awaited_once_with(
        "srw-agent-s-fence", expected_pvc_uid="fence-uid", namespace="agents-a"
    )
    if failing_step == "delete":
        provider.release_agent_workspace_claim_finalizer_exact.assert_not_awaited()
    expected_observations = 2 if failing_step is None else 1
    assert provider.agent_workspace_claim_authority.await_count == (
        expected_observations
    )
    db.complete_pinned_k8s_create_fence_gc.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"protection_protocol": "legacy_v0"},
        {"protection_protocol": None},
        {"resource_name": ""},
        {"resource_uid": None},
        {"thread_id": None},
        {"runtime_generation": ""},
        {"create_attempt": None},
        {"authority_id": ""},
        {"namespace": None},
        {"resource_kind": "configmap"},
        {"provisioner": "vm"},
    ],
)
async def test_fence_gc_skips_unprotected_or_unattributable_rows(overrides):
    provider = _fence_provider(
        pod_states=[("exact_absent", None)], pvc_states=[("exact_absent", None)]
    )

    db = await _sweep_once([_fence_row(**overrides)], agent_provisioner=provider)

    provider.agent_pod_provision_intent_authority.assert_not_awaited()
    provider.agent_workspace_claim_authority.assert_not_awaited()
    provider.delete_agent_pod_exact.assert_not_awaited()
    db.complete_pinned_k8s_create_fence_gc.assert_not_awaited()


@pytest.mark.asyncio
async def test_fence_gc_skips_an_unavailable_provisioner():
    provider = _fence_provider(pod_states=[("exact_absent", None)])
    provider.is_available = False

    db = await _sweep_once([_fence_row()], agent_provisioner=provider)

    provider.agent_pod_provision_intent_authority.assert_not_awaited()
    db.complete_pinned_k8s_create_fence_gc.assert_not_awaited()


@pytest.mark.asyncio
async def test_fence_gc_leaves_workspace_fences_alone_without_a_container_provisioner():
    db = await _sweep_once([])

    db.list_pinned_thread_workspace_provision_fences_for_gc.assert_not_awaited()


@pytest.mark.asyncio
async def test_fence_gc_retires_a_workspace_fence_with_absent_optional_uids():
    container = MagicMock(is_available=True)
    container.delete_pinned_workspace_provision_fences_exact = AsyncMock(
        return_value=True
    )
    workspace_row = {
        "attempt_id": CLAIM_ATTEMPT,
        "fence_pod_uid": "fence-pod-uid",
        "fence_pvc_uid": "",
        "fence_configmap_uid": None,
        "fence_service_uid": None,
    }
    shutdown = asyncio.Event()
    db = MagicMock()
    db.list_due_pinned_k8s_create_fences = AsyncMock(return_value=[])

    async def _list_workspace(**_kwargs):
        shutdown.set()
        return [workspace_row]

    db.list_pinned_thread_workspace_provision_fences_for_gc = AsyncMock(
        side_effect=_list_workspace
    )
    db.retire_pinned_thread_workspace_provision_fence = AsyncMock(return_value=True)

    await reconciliation.pinned_k8s_create_fence_gc_sweeper(
        shutdown, dependencies=_dependencies(db, container_provisioner=container)
    )

    db.list_pinned_thread_workspace_provision_fences_for_gc.assert_awaited_once_with(
        limit=50
    )
    db.retire_pinned_thread_workspace_provision_fence.assert_awaited_once_with(
        CLAIM_ATTEMPT,
        expected_fence_pod_uid="fence-pod-uid",
        expected_fence_pvc_uid=None,
        expected_fence_configmap_uid=None,
        expected_fence_service_uid=None,
    )


@pytest.mark.asyncio
async def test_fence_gc_logs_a_failed_pass_and_keeps_its_floored_cadence(
    monkeypatch, caplog
):
    monkeypatch.setenv("PINNED_K8S_CREATE_FENCE_GC_INTERVAL_SECONDS", "1")
    shutdown = asyncio.Event()
    db = MagicMock()
    calls = 0

    async def _list(**_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("fence listing exploded")
        shutdown.set()
        return []

    db.list_due_pinned_k8s_create_fences = AsyncMock(side_effect=_list)
    real_wait_for = asyncio.wait_for

    async def fast_wait_for(awaitable, timeout):
        return await real_wait_for(awaitable, timeout=0.01)

    caplog.set_level(logging.INFO, logger=reconciliation.__name__)
    with patch.object(reconciliation.asyncio, "wait_for", fast_wait_for):
        await real_wait_for(
            reconciliation.pinned_k8s_create_fence_gc_sweeper(
                shutdown, dependencies=_dependencies(db)
            ),
            timeout=2,
        )

    assert calls == 2
    assert [r.getMessage() for r in caplog.records] == [
        "Pinned Kubernetes create-fence GC started (interval=5s)",
        "Pinned Kubernetes create-fence GC pass failed",
        "Pinned Kubernetes create-fence GC stopped",
    ]


def test_reconciliation_dependencies_are_a_frozen_explicit_port():
    fields = [
        field.name for field in dataclasses.fields(PinnedK8sReconciliationDependencies)
    ]
    assert fields == [
        "store",
        "agent_provisioner",
        "persistent_provisioner",
        "container_provisioner",
    ]
    dependencies = _dependencies(MagicMock())
    with pytest.raises(dataclasses.FrozenInstanceError):
        dependencies.store = None
    assert not hasattr(dependencies, "__dict__")
