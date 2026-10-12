"""Rollout reconciliation for pre-0200 pinned Kubernetes authority.

0185 persisted exact create attempts and object UIDs but did not persist the
Kubernetes namespace or protect objects with an SRW finalizer.  This module is
the only grandfathering path: a configured provider must first protect and
re-read the exact labelled Pod/PVC tuple, then Postgres atomically publishes the
coordinates with an append-only evidence receipt.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any
from uuid import uuid4


@dataclass(frozen=True, slots=True)
class LegacyPinnedAuthorityReconcileResult:
    scanned: int
    adopted: int
    unresolved: int

    @property
    def complete(self) -> bool:
        return self.unresolved == 0


@dataclass(frozen=True, slots=True)
class WarmBindingReservationResult:
    state: str
    attach_token: str | None = None

    @property
    def bound(self) -> bool:
        return self.state == "bound" and self.attach_token is not None


def _warm_provider(
    row: dict[str, Any],
    *,
    agent_provisioner: Any | None,
    persistent_provisioner: Any | None,
) -> Any | None:
    return {
        "agent": agent_provisioner,
        "persistent": persistent_provisioner,
    }.get(str(row.get("provisioner") or ""))


async def reserve_pinned_warm_agent_binding(
    db: Any,
    *,
    agent_provisioner: Any | None,
    persistent_provisioner: Any | None,
    thread_id: str,
    agent_id: str,
    expected_runtime_generation: str,
) -> WarmBindingReservationResult:
    """Protect an idle pool Pod, then atomically publish its binding.

    Kubernetes discovery is read-only.  The exact plan and attach token are
    durable before the first patch, so cancellation/crash can only leave a
    restart-visible finalizer obligation—not an orphan external effect.
    """

    get_candidate = getattr(db, "get_pinned_warm_binding_candidate", None)
    plan = getattr(db, "plan_pinned_warm_binding_protection", None)
    claim = getattr(db, "claim_pinned_warm_binding_effect", None)
    publish = getattr(db, "publish_pinned_warm_binding_protection", None)
    bind = getattr(db, "bind_pinned_warm_agent", None)
    if any(method is None for method in (get_candidate, plan, claim, publish, bind)):
        return WarmBindingReservationResult("unsupported")
    candidate = await get_candidate(
        thread_id,
        agent_id,
        expected_runtime_generation=expected_runtime_generation,
    )
    if not isinstance(candidate, dict):
        return WarmBindingReservationResult("refused")
    provider = _warm_provider(
        candidate,
        agent_provisioner=agent_provisioner,
        persistent_provisioner=persistent_provisioner,
    )
    discover = getattr(provider, "discover_pinned_warm_agent_authority", None)
    protect = getattr(provider, "protect_planned_pinned_warm_agent_authority", None)
    if (
        provider is None
        or not bool(getattr(provider, "is_available", False))
        or discover is None
        or protect is None
    ):
        return WarmBindingReservationResult("refused")
    discovery = await discover(candidate)
    if not isinstance(discovery, dict):
        return WarmBindingReservationResult("refused")
    attach_token = str(uuid4())
    protection_id = str(uuid4())
    planned = await plan(
        thread_id,
        expected_runtime_generation=expected_runtime_generation,
        runtime_attach_token=attach_token,
        agent_id=agent_id,
        protection_id=protection_id,
        source="attach",
        provisioner=str(candidate.get("provisioner") or ""),
        namespace=str(discovery.get("namespace") or ""),
        pod_name=str(candidate.get("pod_name") or ""),
        pod_uid=str(discovery.get("pod_uid") or ""),
        discovered_resource_version=str(discovery.get("pod_resource_version") or ""),
    )
    if not isinstance(planned, dict):
        return WarmBindingReservationResult("refused")
    if not bool(planned.get("owned")):
        # Another request has already reserved this exact tuple.  Its lease or
        # reconciler must settle before callers may provision a second runtime.
        return WarmBindingReservationResult("pending")
    effect_token = str(uuid4())
    claimed = await claim(protection_id, effect_token=effect_token)
    if not isinstance(claimed, dict):
        # An expired-plan abort may have won this exact CAS.  No Kubernetes
        # mutation is legal unless the protecting row names our token.
        return WarmBindingReservationResult("pending")
    try:
        evidence = await protect(claimed)
    except asyncio.CancelledError:
        # The bounded Kubernetes helper has joined its sync worker.  Keep the
        # durable plan for restart reconciliation, then honor cancellation.
        raise
    if not isinstance(evidence, dict):
        return WarmBindingReservationResult("pending")
    protected = await publish(
        protection_id,
        effect_token=effect_token,
        expected_pod_uid=str(evidence.get("pod_uid") or ""),
        protection_resource_version=str(evidence.get("pod_resource_version") or ""),
        evidence_protocol=str(evidence.get("evidence_protocol") or ""),
    )
    if not isinstance(protected, dict):
        return WarmBindingReservationResult("pending")
    bound = await bind(protection_id)
    if not isinstance(bound, dict) or str(bound.get("status") or "") != "bound":
        return WarmBindingReservationResult("pending")
    return WarmBindingReservationResult(
        "bound", str(bound.get("runtime_attach_token") or "") or None
    )


async def reconcile_pinned_warm_binding_protections(
    db: Any,
    *,
    agent_provisioner: Any | None,
    persistent_provisioner: Any | None,
    thread_id: str | None = None,
    limit: int = 50,
) -> LegacyPinnedAuthorityReconcileResult:
    """Settle expired attach plans and pre-0200 reciprocal warm bindings."""

    list_legacy = getattr(db, "list_legacy_pinned_warm_binding_candidates", None)
    list_pending = getattr(db, "list_expired_pinned_warm_binding_protections", None)
    plan = getattr(db, "plan_pinned_warm_binding_protection", None)
    claim = getattr(db, "claim_pinned_warm_binding_effect", None)
    publish = getattr(db, "publish_pinned_warm_binding_protection", None)
    bind = getattr(db, "bind_pinned_warm_agent", None)
    begin_release = getattr(db, "begin_pinned_warm_binding_release", None)
    begin_deleted_release = getattr(
        db, "begin_deleted_pinned_warm_binding_release", None
    )
    complete_release = getattr(db, "complete_pinned_warm_binding_release", None)
    abort = getattr(db, "abort_unmodified_pinned_warm_binding", None)
    if any(
        method is None
        for method in (
            list_legacy,
            list_pending,
            plan,
            claim,
            publish,
            bind,
            begin_release,
            complete_release,
            abort,
        )
    ):
        return LegacyPinnedAuthorityReconcileResult(0, 0, 0)

    scanned = 0
    adopted = 0
    unresolved = 0
    legacy_rows = await list_legacy(thread_id=thread_id, limit=limit)
    for candidate in legacy_rows:
        scanned += 1
        provider = _warm_provider(
            candidate,
            agent_provisioner=agent_provisioner,
            persistent_provisioner=persistent_provisioner,
        )
        discover = getattr(provider, "discover_pinned_warm_agent_authority", None)
        protect = getattr(provider, "protect_planned_pinned_warm_agent_authority", None)
        if (
            provider is None
            or not bool(getattr(provider, "is_available", False))
            or discover is None
            or protect is None
        ):
            unresolved += 1
            continue
        discovery = await discover(candidate)
        if not isinstance(discovery, dict):
            unresolved += 1
            continue
        protection_id = str(uuid4())
        planned = await plan(
            str(candidate.get("thread_id") or ""),
            expected_runtime_generation=str(candidate.get("runtime_generation") or ""),
            runtime_attach_token=str(candidate.get("runtime_attach_token") or ""),
            agent_id=str(candidate.get("agent_id") or ""),
            protection_id=protection_id,
            source="legacy_binding",
            provisioner=str(candidate.get("provisioner") or ""),
            namespace=str(discovery.get("namespace") or ""),
            pod_name=str(candidate.get("pod_name") or ""),
            pod_uid=str(discovery.get("pod_uid") or ""),
            discovered_resource_version=str(
                discovery.get("pod_resource_version") or ""
            ),
        )
        if not isinstance(planned, dict):
            unresolved += 1
            continue
        protection_id = str(planned.get("protection_id") or "")
        effect_token = str(uuid4())
        claimed = await claim(protection_id, effect_token=effect_token)
        if not isinstance(claimed, dict):
            unresolved += 1
            continue
        evidence = await protect(claimed)
        if not isinstance(evidence, dict):
            unresolved += 1
            continue
        protected = await publish(
            protection_id,
            effect_token=effect_token,
            expected_pod_uid=str(evidence.get("pod_uid") or ""),
            protection_resource_version=str(evidence.get("pod_resource_version") or ""),
            evidence_protocol=str(evidence.get("evidence_protocol") or ""),
        )
        settled = await bind(protection_id) if protected is not None else None
        if isinstance(settled, dict) and str(settled.get("status") or "") == "bound":
            adopted += 1
        else:
            unresolved += 1

    pending_rows = await list_pending(thread_id=thread_id, limit=limit)
    for row in pending_rows:
        scanned += 1
        provider = _warm_provider(
            row,
            agent_provisioner=agent_provisioner,
            persistent_provisioner=persistent_provisioner,
        )
        observe = getattr(provider, "observe_planned_pinned_warm_agent_authority", None)
        protect = getattr(provider, "protect_planned_pinned_warm_agent_authority", None)
        fence = getattr(provider, "fence_expired_pinned_warm_agent_authority", None)
        release = getattr(provider, "release_planned_pinned_warm_agent_authority", None)
        if (
            provider is None
            or not bool(getattr(provider, "is_available", False))
            or observe is None
            or protect is None
            or fence is None
            or release is None
        ):
            unresolved += 1
            continue
        status = str(row.get("status") or "")
        source = str(row.get("source") or "")
        protection_id = str(row.get("protection_id") or "")
        if status == "planned" and source == "legacy_binding":
            effect_token = str(uuid4())
            claimed = await claim(protection_id, effect_token=effect_token)
            if not isinstance(claimed, dict):
                unresolved += 1
                continue
            evidence = await protect(claimed)
            if not isinstance(evidence, dict):
                unresolved += 1
                continue
            protected = await publish(
                protection_id,
                effect_token=effect_token,
                expected_pod_uid=str(evidence.get("pod_uid") or ""),
                protection_resource_version=str(
                    evidence.get("pod_resource_version") or ""
                ),
                evidence_protocol=str(evidence.get("evidence_protocol") or ""),
            )
            settled = await bind(protection_id) if protected is not None else None
            if (
                isinstance(settled, dict)
                and str(settled.get("status") or "") == "bound"
            ):
                adopted += 1
            else:
                unresolved += 1
            continue
        if status == "protecting":
            effect_token = str(row.get("effect_token") or "")
            observation = await observe(row)
            state = str(observation.get("state") or "")
            if observation.get("finalizer_present") is True:
                protected = await publish(
                    protection_id,
                    effect_token=effect_token,
                    expected_pod_uid=str(row.get("pod_uid") or ""),
                    protection_resource_version=str(
                        observation.get("pod_resource_version") or ""
                    ),
                    evidence_protocol="exact_live_finalizer_v1",
                )
                if not isinstance(protected, dict):
                    unresolved += 1
                    continue
                status = "protected"
            elif source == "legacy_binding":
                # The reciprocal pre-0200 binding is itself durable ownership,
                # so its reconciler can retry protection at the current exact
                # UID/resourceVersion without manufacturing an orphan owner.
                evidence = await protect(row)
                if not isinstance(evidence, dict):
                    unresolved += 1
                    continue
                protected = await publish(
                    protection_id,
                    effect_token=effect_token,
                    expected_pod_uid=str(evidence.get("pod_uid") or ""),
                    protection_resource_version=str(
                        evidence.get("pod_resource_version") or ""
                    ),
                    evidence_protocol=str(evidence.get("evidence_protocol") or ""),
                )
                if not isinstance(protected, dict):
                    unresolved += 1
                    continue
                status = "protected"
            elif source == "attach":
                fenced = await fence(row)
                fence_state = str(fenced.get("state") or "")
                if fence_state == "finalizer_won":
                    protected = await publish(
                        protection_id,
                        effect_token=effect_token,
                        expected_pod_uid=str(row.get("pod_uid") or ""),
                        protection_resource_version=str(
                            fenced.get("pod_resource_version") or ""
                        ),
                        evidence_protocol="exact_live_finalizer_v1",
                    )
                    if not isinstance(protected, dict):
                        unresolved += 1
                        continue
                    status = "protected"
                elif fence_state == "fence_won":
                    if await abort(
                        protection_id,
                        release_outcome="exact_live_unprotected_v1",
                        agent_present=True,
                        effect_token=effect_token,
                        abort_fence_protocol=str(
                            fenced.get("abort_fence_protocol") or ""
                        ),
                        abort_fence_resource_version=str(
                            fenced.get("abort_fence_resource_version") or ""
                        ),
                        abort_fence_value=str(fenced.get("abort_fence_value") or ""),
                    ):
                        adopted += 1
                    else:
                        unresolved += 1
                    continue
                elif fence_state in {"exact_absent", "replacement"}:
                    if await abort(
                        protection_id,
                        release_outcome={
                            "exact_absent": "exact_absent_v1",
                            "replacement": "exact_replacement_v1",
                        }[fence_state],
                        agent_present=False,
                        effect_token=effect_token,
                        abort_fence_protocol="exact_object_gone_v1",
                    ):
                        adopted += 1
                    else:
                        unresolved += 1
                    continue
                else:
                    unresolved += 1
                    continue
            else:
                unresolved += 1
                continue
        if status == "protected" and source == "legacy_binding":
            settled = await bind(protection_id)
            if (
                isinstance(settled, dict)
                and str(settled.get("status") or "") == "bound"
            ):
                adopted += 1
            else:
                unresolved += 1
            continue
        if source != "attach" and not row.get("deleted_owner_release"):
            unresolved += 1
            continue
        if status == "bound":
            if (
                not row.get("deleted_owner_release")
                or begin_deleted_release is None
                or not await begin_deleted_release(protection_id)
            ):
                unresolved += 1
                continue
            status = "terminal_release"
        if status == "planned":
            observation = await observe(row)
            state = str(observation.get("state") or "")
            if observation.get("finalizer_present") is not True and state in {
                "exact_live",
                "exact_absent",
                "replacement",
            }:
                outcome = {
                    "exact_live": "exact_live_unprotected_v1",
                    "exact_absent": "exact_absent_v1",
                    "replacement": "exact_replacement_v1",
                }[state]
                if await abort(
                    protection_id,
                    release_outcome=outcome,
                    agent_present=state == "exact_live",
                    abort_fence_protocol="unclaimed_plan_v1",
                ):
                    adopted += 1
                else:
                    unresolved += 1
                continue
            else:
                unresolved += 1
                continue
        if status == "protected" and not await begin_release(protection_id):
            unresolved += 1
            continue
        # R3.2's unpublished writer (superseded by 0301) left an ordinary
        # release at an owner deleted under this life's exact permanent
        # receipt when its follow-up release failed. That state alone grants
        # no Pod proof and cannot become terminal_release, so it settles only
        # through the terminal-only release: the exact Pod must be terminal or
        # absent, and it never returns to the pool.
        deleted_owner_releasing = status == "releasing" and bool(
            row.get("deleted_owner_release")
        )
        if status == "terminal_release" and (
            not row.get("deleted_owner_release")
            or begin_deleted_release is None
            or not await begin_deleted_release(protection_id)
        ):
            unresolved += 1
            continue
        release_result = (
            await release(row, terminal_only=True)
            if status == "terminal_release" or deleted_owner_releasing
            else await release(row)
        )
        if not isinstance(release_result, dict):
            unresolved += 1
            continue
        if deleted_owner_releasing and (
            release_result.get("outcome") != "exact_absent_v1"
            or release_result.get("agent_present")
        ):
            unresolved += 1
            continue
        if await complete_release(
            protection_id,
            release_outcome=str(release_result.get("outcome") or ""),
            agent_present=bool(release_result.get("agent_present")),
        ):
            adopted += 1
        else:
            unresolved += 1
    return LegacyPinnedAuthorityReconcileResult(scanned, adopted, unresolved)


async def release_pinned_warm_binding_protection(
    db: Any,
    *,
    protection_id: str,
    agent_provisioner: Any | None,
    persistent_provisioner: Any | None,
) -> bool:
    """Complete an explicit DB-fenced attach-abort release immediately.

    Unlike background/End reconciliation this does not wait for lease expiry:
    the caller already changed ``bound -> releasing`` atomically with the
    reciprocal detach, so no patch owner can still race this finalizer removal.
    """

    get_protection = getattr(db, "get_pinned_warm_binding_protection", None)
    complete = getattr(db, "complete_pinned_warm_binding_release", None)
    if get_protection is None or complete is None:
        return False
    row = await get_protection(protection_id)
    if not isinstance(row, dict):
        return False
    if str(row.get("status") or "") == "released":
        return True
    if str(row.get("status") or "") != "releasing":
        return False
    provider = _warm_provider(
        row,
        agent_provisioner=agent_provisioner,
        persistent_provisioner=persistent_provisioner,
    )
    release = getattr(provider, "release_planned_pinned_warm_agent_authority", None)
    if (
        provider is None
        or not bool(getattr(provider, "is_available", False))
        or release is None
    ):
        return False
    result = await release(row)
    if not isinstance(result, dict):
        return False
    return bool(
        await complete(
            protection_id,
            release_outcome=str(result.get("outcome") or ""),
            agent_present=bool(result.get("agent_present")),
        )
    )


async def reconcile_legacy_pinned_agent_authority(
    db: Any,
    *,
    agent_provisioner: Any | None = None,
    persistent_provisioner: Any | None = None,
    thread_id: str | None = None,
    limit: int = 50,
) -> LegacyPinnedAuthorityReconcileResult:
    """Adopt exact live legacy objects; never infer an absent namespace."""

    warm = await reconcile_pinned_warm_binding_protections(
        db,
        agent_provisioner=agent_provisioner,
        persistent_provisioner=persistent_provisioner,
        thread_id=thread_id,
        limit=limit,
    )

    list_candidates = getattr(
        db, "list_legacy_pinned_agent_k8s_authority_candidates", None
    )
    publish_adoption = getattr(db, "adopt_legacy_pinned_agent_k8s_authority", None)
    if list_candidates is None or publish_adoption is None:
        # Unit fakes and non-Postgres deployments predate the rollout ledger.
        return warm

    rows = await list_candidates(thread_id=thread_id, limit=limit)
    adopted = 0
    unresolved = 0
    providers = {
        "agent": agent_provisioner,
        "persistent": persistent_provisioner,
    }
    for row in rows:
        provider = providers.get(str(row.get("provisioner") or ""))
        protect = getattr(provider, "protect_legacy_pinned_agent_authority", None)
        if provider is None or not bool(getattr(provider, "is_available", False)):
            unresolved += 1
            continue
        if protect is None:
            unresolved += 1
            continue
        evidence = await protect(row)
        if not isinstance(evidence, dict):
            unresolved += 1
            continue
        accepted = await publish_adoption(
            str(row.get("thread_id") or ""),
            expected_runtime_generation=str(row.get("runtime_generation") or ""),
            attempt_id=str(row.get("attempt_id") or ""),
            namespace=str(evidence.get("namespace") or ""),
            pod_uid=str(evidence.get("pod_uid") or ""),
            pod_resource_version=str(evidence.get("pod_resource_version") or ""),
            pvc_uid=(str(evidence.get("pvc_uid")) if evidence.get("pvc_uid") else None),
            pvc_resource_version=(
                str(evidence.get("pvc_resource_version"))
                if evidence.get("pvc_resource_version")
                else None
            ),
            protection_finalizer=str(evidence.get("protection_finalizer") or ""),
            evidence_protocol=str(evidence.get("evidence_protocol") or ""),
            observed_at=evidence.get("observed_at"),
        )
        if accepted:
            adopted += 1
        else:
            # A sibling replica may have committed the same receipt after our
            # Kubernetes re-read.  Re-list rather than turning that harmless
            # CAS loss into a one-request rollout failure.
            remaining = await list_candidates(
                thread_id=str(row.get("thread_id") or ""), limit=2
            )
            if any(
                str(candidate.get("attempt_id") or "")
                == str(row.get("attempt_id") or "")
                for candidate in remaining
            ):
                unresolved += 1
            else:
                adopted += 1
    return LegacyPinnedAuthorityReconcileResult(
        warm.scanned + len(rows),
        warm.adopted + adopted,
        warm.unresolved + unresolved,
    )
