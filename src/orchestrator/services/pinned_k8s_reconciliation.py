"""Pinned agent Kubernetes create-intent reconciliation and create-fence GC.

R1.B11 home of the two pinned Kubernetes leader loops that used to live in
``orchestrator.main``. The application owns task creation, leader gating
(``run_when_leader``), cadence ownership and shutdown; the bodies here receive
their store and provisioners explicitly through
:class:`PinnedK8sReconciliationDependencies` and never reach back into
application globals.

* :func:`pinned_agent_create_intent_reconciler` promotes exact response-lost
  Pod/PVC creates (and adopts exact legacy authority) after a restart.
* :func:`pinned_k8s_create_fence_gc_sweeper` exact-deletes post-horizon
  Pod/PVC/workspace name fences and retires their rows only after the
  Kubernetes name is proven absent.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from orchestrator.services.pinned_agent_authority import (
    reconcile_legacy_pinned_agent_authority,
)

logger = logging.getLogger(__name__)

__all__ = [
    "PinnedK8sReconciliationDependencies",
    "pinned_agent_create_intent_reconciler",
    "pinned_k8s_create_fence_gc_sweeper",
]


@dataclass(frozen=True, slots=True)
class PinnedK8sReconciliationDependencies:
    """Collaborators the application hands both loops for one task tenure."""

    #: The application's Postgres store (``main.postgres_db``).
    store: Any
    agent_provisioner: Any
    persistent_provisioner: Any
    container_provisioner: Any


async def pinned_agent_create_intent_reconciler(
    shutdown_event: asyncio.Event,
    *,
    dependencies: PinnedK8sReconciliationDependencies,
) -> None:
    """Promote exact response-lost Pod/PVC creates after process restart.

    This leader never originates a credential-bearing Kubernetes effect.  It
    only observes the immutable labels/UID of an already-committed object and
    lets the row-locked publication CAS adopt it while T/G remains open.
    Retirement owns the complementary revoke/fence path.
    """

    interval_s = max(
        5,
        int(os.getenv("PINNED_AGENT_CREATE_RECONCILE_INTERVAL_SECONDS", "15")),
    )
    logger.info("Pinned agent create-intent reconciler started")
    while not shutdown_event.is_set():
        try:
            legacy = await reconcile_legacy_pinned_agent_authority(
                dependencies.store,
                agent_provisioner=dependencies.agent_provisioner,
                persistent_provisioner=dependencies.persistent_provisioner,
                limit=50,
            )
            if legacy.unresolved:
                logger.warning(
                    "Pinned legacy Kubernetes authority remains unresolved "
                    "for %d row(s)",
                    legacy.unresolved,
                )
            rows = (
                await dependencies.store.list_pinned_agent_create_intents_for_reconcile(
                    limit=50
                )
            )
            for row in rows:
                try:
                    provisioner = str(row.get("provisioner") or "")
                    provider = (
                        dependencies.persistent_provisioner
                        if provisioner == "persistent"
                        else dependencies.agent_provisioner
                        if provisioner == "agent"
                        else None
                    )
                    if provider is None or not provider.is_available:
                        continue
                    thread_id = str(row.get("thread_id") or "")
                    generation = str(row.get("runtime_generation") or "")
                    attempt_id = str(row.get("attempt_id") or "")
                    pod_name = str(row.get("pod_name") or "")
                    namespace = str(row.get("namespace") or "")
                    if (
                        not all(
                            (thread_id, generation, attempt_id, pod_name, namespace)
                        )
                        or str(row.get("protection_protocol") or "") != "finalizer_v1"
                    ):
                        continue

                    claim = row.get("workspace_claim")
                    if claim is not None:
                        if not isinstance(claim, Mapping):
                            continue
                        claim_id = str(claim.get("claim_id") or "")
                        claim_generation = str(
                            claim.get("created_runtime_generation") or ""
                        )
                        claim_attempt = str(claim.get("create_attempt") or "")
                        claim_name = str(claim.get("pvc_name") or "")
                        claim_status = str(claim.get("status") or "")
                        claim_uid = str(claim.get("pvc_uid") or "")
                        claim_namespace = str(claim.get("namespace") or "")
                        if (
                            not all(
                                (
                                    claim_id,
                                    claim_generation,
                                    claim_attempt,
                                    claim_name,
                                    claim_namespace,
                                )
                            )
                            or claim_status not in {"planned", "ready"}
                            or not (
                                claim_namespace == namespace
                                and str(claim.get("protection_protocol") or "")
                                == "finalizer_v1"
                            )
                        ):
                            continue
                        observed_claim = await provider.agent_workspace_claim_authority(
                            claim_name,
                            expected_thread_id=thread_id,
                            expected_runtime_generation=claim_generation,
                            expected_claim_id=claim_id,
                            expected_create_attempt=claim_attempt,
                            namespace=claim_namespace,
                            expected_pvc_uid=claim_uid or None,
                        )
                        observed_claim_uid = str(
                            (observed_claim or {}).get("pvc_uid") or ""
                        )
                        if not (
                            str((observed_claim or {}).get("state") or "")
                            == "exact_present"
                            and observed_claim_uid
                            and (not claim_uid or observed_claim_uid == claim_uid)
                        ):
                            continue
                        if (
                            claim_status == "planned"
                            and not await dependencies.store.publish_pinned_agent_workspace_claim(
                                thread_id,
                                expected_runtime_generation=generation,
                                claim_id=claim_id,
                                pvc_name=claim_name,
                                pvc_uid=observed_claim_uid,
                                namespace=claim_namespace,
                            )
                        ):
                            continue

                    observed_pod = await provider.agent_pod_provision_intent_authority(
                        pod_name,
                        expected_thread_id=thread_id,
                        expected_runtime_generation=generation,
                        expected_attempt_id=attempt_id,
                        namespace=namespace,
                    )
                    pod_uid = str((observed_pod or {}).get("pod_uid") or "")
                    if not (
                        str((observed_pod or {}).get("state") or "") == "exact_present"
                        and pod_uid
                    ):
                        continue
                    await dependencies.store.publish_pinned_agent_pod_provision_intent(
                        thread_id,
                        expected_runtime_generation=generation,
                        attempt_id=attempt_id,
                        pod_name=pod_name,
                        pod_uid=pod_uid,
                        namespace=namespace,
                    )
                except Exception:
                    logger.exception(
                        "Pinned agent create-intent reconciliation failed for %s",
                        row.get("attempt_id"),
                    )
        except Exception:
            logger.exception("Pinned agent create-intent reconciliation pass failed")
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval_s)
        except TimeoutError:
            pass
    logger.info("Pinned agent create-intent reconciler stopped")


async def pinned_k8s_create_fence_gc_sweeper(
    shutdown_event: asyncio.Event,
    *,
    dependencies: PinnedK8sReconciliationDependencies,
) -> None:
    """Exact-delete post-horizon Pod/PVC name fences and retire their rows.

    Fence rows deliberately survive thread deletion and process restart. A
    due timestamp is necessary but not sufficient: the sweeper reattests the
    immutable labels and recorded UID, issues a UID-preconditioned delete, and
    marks the work item terminal only after the Kubernetes name is absent.
    """

    interval_s = max(
        5,
        int(os.getenv("PINNED_K8S_CREATE_FENCE_GC_INTERVAL_SECONDS", "30")),
    )
    logger.info("Pinned Kubernetes create-fence GC started (interval=%ds)", interval_s)
    while not shutdown_event.is_set():
        try:
            rows = await dependencies.store.list_due_pinned_k8s_create_fences(limit=50)
            for row in rows:
                provisioner = str(row.get("provisioner") or "")
                provider = (
                    dependencies.persistent_provisioner
                    if provisioner == "persistent"
                    else dependencies.agent_provisioner
                    if provisioner == "agent"
                    else None
                )
                if provider is None or not provider.is_available:
                    continue
                resource_kind = str(row.get("resource_kind") or "")
                resource_name = str(row.get("resource_name") or "")
                resource_uid = str(row.get("resource_uid") or "")
                thread_id = str(row.get("thread_id") or "")
                generation = str(row.get("runtime_generation") or "")
                create_attempt = str(row.get("create_attempt") or "")
                authority_id = str(row.get("authority_id") or "")
                namespace = str(row.get("namespace") or "")
                if (
                    not all(
                        (
                            resource_name,
                            resource_uid,
                            thread_id,
                            generation,
                            create_attempt,
                            authority_id,
                            namespace,
                        )
                    )
                    or str(row.get("protection_protocol") or "") != "finalizer_v1"
                ):
                    continue
                if resource_kind == "pod":
                    observed = await provider.agent_pod_provision_intent_authority(
                        resource_name,
                        expected_thread_id=thread_id,
                        expected_runtime_generation=generation,
                        expected_attempt_id=create_attempt,
                        namespace=namespace,
                    )
                    state = str((observed or {}).get("state") or "")
                    observed_uid = str((observed or {}).get("pod_uid") or "")
                    if state == "exact_fence" and observed_uid == resource_uid:
                        deleted = (
                            await dependencies.persistent_provisioner.delete_agent_pod_exact(
                                thread_id,
                                expected_pod_uid=resource_uid,
                                namespace=namespace,
                            )
                            if provisioner == "persistent"
                            else await dependencies.agent_provisioner.delete_agent_pod_exact(
                                resource_name,
                                expected_pod_uid=resource_uid,
                                namespace=namespace,
                            )
                        )
                        if not deleted:
                            continue
                        released = (
                            await dependencies.persistent_provisioner.release_agent_pod_finalizer_exact(
                                thread_id,
                                expected_pod_uid=resource_uid,
                                namespace=namespace,
                                terminal_required=False,
                            )
                            if provisioner == "persistent"
                            else await dependencies.agent_provisioner.release_agent_pod_finalizer_exact(
                                resource_name,
                                expected_pod_uid=resource_uid,
                                namespace=namespace,
                                terminal_required=False,
                            )
                        )
                        if not released:
                            continue
                        observed = await provider.agent_pod_provision_intent_authority(
                            resource_name,
                            expected_thread_id=thread_id,
                            expected_runtime_generation=generation,
                            expected_attempt_id=create_attempt,
                            namespace=namespace,
                        )
                        state = str((observed or {}).get("state") or "")
                    if state != "exact_absent":
                        continue
                elif resource_kind == "pvc":
                    observed = await provider.agent_workspace_claim_authority(
                        resource_name,
                        expected_thread_id=thread_id,
                        expected_runtime_generation=generation,
                        expected_claim_id=authority_id,
                        expected_create_attempt=create_attempt,
                        namespace=namespace,
                        expected_pvc_uid=resource_uid,
                    )
                    state = str((observed or {}).get("state") or "")
                    observed_uid = str((observed or {}).get("pvc_uid") or "")
                    if state == "exact_fence" and observed_uid == resource_uid:
                        if not await provider.delete_agent_workspace_claim_exact(
                            resource_name,
                            expected_pvc_uid=resource_uid,
                            namespace=namespace,
                        ):
                            continue
                        if not await provider.release_agent_workspace_claim_finalizer_exact(
                            resource_name,
                            expected_pvc_uid=resource_uid,
                            namespace=namespace,
                        ):
                            continue
                        observed = await provider.agent_workspace_claim_authority(
                            resource_name,
                            expected_thread_id=thread_id,
                            expected_runtime_generation=generation,
                            expected_claim_id=authority_id,
                            expected_create_attempt=create_attempt,
                            namespace=namespace,
                            expected_pvc_uid=resource_uid,
                        )
                        state = str((observed or {}).get("state") or "")
                    if state != "exact_absent":
                        continue
                else:
                    continue
                await dependencies.store.complete_pinned_k8s_create_fence_gc(
                    resource_kind=resource_kind,
                    authority_id=authority_id,
                    expected_resource_uid=resource_uid,
                )
            if dependencies.container_provisioner.is_available:
                workspace_rows = await dependencies.store.list_pinned_thread_workspace_provision_fences_for_gc(
                    limit=50
                )
                for workspace_row in workspace_rows:
                    if not await dependencies.container_provisioner.delete_pinned_workspace_provision_fences_exact(
                        workspace_row
                    ):
                        continue
                    await dependencies.store.retire_pinned_thread_workspace_provision_fence(
                        str(workspace_row.get("attempt_id") or ""),
                        expected_fence_pod_uid=str(
                            workspace_row.get("fence_pod_uid") or ""
                        ),
                        expected_fence_pvc_uid=(
                            str(workspace_row.get("fence_pvc_uid") or "") or None
                        ),
                        expected_fence_configmap_uid=(
                            str(workspace_row.get("fence_configmap_uid") or "") or None
                        ),
                        expected_fence_service_uid=(
                            str(workspace_row.get("fence_service_uid") or "") or None
                        ),
                    )
        except Exception:
            logger.exception("Pinned Kubernetes create-fence GC pass failed")
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval_s)
        except TimeoutError:
            pass
    logger.info("Pinned Kubernetes create-fence GC stopped")
