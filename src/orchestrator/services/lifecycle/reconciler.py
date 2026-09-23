"""Generic lifecycle reconciler — Phase 1a skeleton.

The reconciler iterates registered managers on each tick and reconciles
each managed instance against the manager's expected version and health
predicates. This module defines the shape; the actual tick loop and the
wiring into ``orchestrator.main.lifespan`` land in Phase 1b alongside
the first real ``InstanceLifecycleManager`` implementation
(``AgentInstanceManager``).

Disruption budget is the rate limiter that prevents a rollout from
draining too many instances of one kind at once. Karpenter and the K8s
Eviction API both rate-limit for the same reason — without it,
drift-detection fires N concurrent drains the moment a new image lands.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator

from orchestrator.services.lifecycle.types import (
    Instance,
    InstanceLifecycleManager,
    ReapableInstanceManager,
    StatefulInstanceManager,
)

logger = logging.getLogger(__name__)


@dataclass
class DisruptionBudget:
    """Caps concurrent drains per instance kind.

    Default per-kind cap is ``max(1, total // 4)`` of the kind's current
    instance count, mirroring Karpenter's "max 25% concurrent disruption"
    convention. Configurable via Helm values; the reconciler asks the
    budget whether it can act on a kind at the start of each candidate's
    drain decision.

    Phase 1a: shape only. The actual concurrency tracking lands in 1b
    when the reconciler is wired into the event loop.
    """

    overrides: dict[str, int] | None = None

    def cap_for(self, kind: str, total: int) -> int:
        if self.overrides and kind in self.overrides:
            return self.overrides[kind]
        return max(1, total // 4)

    def allow(self, kind: str) -> bool:
        # Phase 1a: always allow. 1b adds in-flight accounting.
        return True


class InstanceLifecycleReconciler:
    """Generic lifecycle reconciler.

    Phase 1a: skeleton. Owns the manager registry, exposes ``tick()``
    as a no-op. Phase 1b implements the reconciliation algorithm and
    wires startup reconciliation into the orchestrator lifespan.

    The reconciler does NOT own:
      - Heartbeat handling (lives on the agents heartbeat endpoint).
      - Warm pool maintenance (kind-specific; agent pool reconciler).
      - Adoption / job binding (lives in the dispatcher).
      - Snapshot bucket management (delegated to SnapshotService).
    """

    def __init__(
        self,
        managers: list[InstanceLifecycleManager] | None = None,
        budget: DisruptionBudget | None = None,
    ):
        self._managers: list[InstanceLifecycleManager] = managers or []
        self._budget = budget or DisruptionBudget()

    def register(self, manager: InstanceLifecycleManager) -> None:
        """Add a manager to the reconciler's registry.

        Order does not matter for correctness — each manager handles its
        own kind — but registration order determines log ordering on
        each tick, which may matter for debuggability.
        """
        self._managers.append(manager)

    @property
    def managers(self) -> list[InstanceLifecycleManager]:
        return list(self._managers)

    async def tick(self) -> dict[str, dict[str, int]]:
        """Run one reconciliation pass over all registered managers.

        For each registered manager:
          1. Compute the set of acceptable versions.
          2. Enumerate live instances.
          3. For each instance failing ``is_healthy``, call ``delete``
             with grace=0 — the crash-recovery path. Closes the gap
             where ``Unknown``/``Failed`` workspace pods sat forever
             (``knowledge-base/knowledge/issues/stuck_thread_workspace_pods.md``).
          4. For each drifted instance that is currently idle, call
             ``drain``. Drift on a busy instance is recorded in stats
             but not actuated — the in-pod drain-intent path (Phase 1c)
             handles busy agents at their next safe boundary.
          5. Respect the disruption budget: skip drains for a kind once
             the per-tick cap is hit (only applies to drift drains;
             unhealthy deletes always proceed since they free capacity
             rather than consume it).

        Returns a per-kind stats dict for observability/tests:
        ``{"agent": {"listed": N, "unhealthy": N, "drift": N,
                     "drained": N, "skipped_busy": N}}``.
        """
        report: dict[str, dict[str, int]] = {}
        for manager in self._managers:
            kind = manager.kind
            stats = {
                "listed": 0,
                "unhealthy": 0,
                "drift": 0,
                "drained": 0,
                "skipped_busy": 0,
                "reaped": 0,
                "reap_attempts": 0,
                "reap_forced": 0,
                "orphans_reaped": 0,
            }
            try:
                expected = await manager.expected_versions()
                instances = await manager.list_instances()
            except Exception:
                logger.exception("Lifecycle list failed for kind=%s", kind)
                report[kind] = stats
                continue
            stats["listed"] = len(instances)
            cap = self._budget.cap_for(kind, len(instances))
            drained = 0
            for inst in instances:
                # Crash recovery: unhealthy instances get force-deleted
                # before drift consideration. The manager decides what
                # 'unhealthy' means (pod phase, heartbeat freshness,
                # backend ping). Idempotent — delete on a missing pod
                # is a no-op.
                try:
                    healthy = await manager.is_healthy(inst)
                except Exception:
                    logger.exception(
                        "is_healthy raised for kind=%s id=%s — "
                        "treating as healthy to avoid mass-delete",
                        kind,
                        inst.id,
                    )
                    healthy = True
                if not healthy:
                    stats["unhealthy"] += 1
                    try:
                        async with self._lifecycle_action(
                            manager, inst, source="unhealthy_delete"
                        ) as permit:
                            if permit.local:
                                await manager.delete(inst, grace_s=0)
                    except Exception:
                        logger.exception(
                            "Unhealthy-delete failed for kind=%s id=%s",
                            kind,
                            inst.id,
                        )
                    continue

                if self.is_drift(inst, expected):
                    stats["drift"] += 1

                    # Soft signal: write the drain-pending hint on every
                    # drift, idle or busy. Cheap and idempotent. For agents
                    # this is the trigger that lets a busy worker react at
                    # its next phase boundary (Continue-as-New). For
                    # workspaces and VMs this is a no-op — they get drained
                    # the natural way once the bound work pauses.
                    try:
                        await manager.signal_drain_pending(inst)
                    except Exception:
                        logger.exception(
                            "signal_drain_pending failed for kind=%s id=%s",
                            kind,
                            inst.id,
                        )

                    if not await manager.is_idle(inst):
                        stats["skipped_busy"] += 1
                        # Not drained on drift, but may still be reapable
                        # below if its bound work has finished/paused.
                    elif isinstance(manager, ReapableInstanceManager):
                        # Drifted + idle + reapable: never bare-drain. For
                        # workspaces/VMs drain() is a no-snapshot delete(), so
                        # draining a dirty instance here would lose state. Fall
                        # through (no continue) to the snapshot-aware reap path
                        # below. is_idle ⊆ is_reapable for both managers, so
                        # _reap won't early-return; it is uncapped by design
                        # (mirrors the every-tick reap of finished work), so the
                        # disruption cap correctly does not gate it.
                        pass
                    elif drained < cap and self._budget.allow(kind):
                        try:
                            if isinstance(manager, StatefulInstanceManager):
                                # Stateful-but-not-reapable (no such manager ships
                                # today): honor the StatefulInstanceManager contract
                                # that a snapshot precedes any state-losing drain.
                                # Reapable kinds took the branch above.
                                await manager.snapshot(inst)
                            await manager.drain(inst, grace_s=0)
                            stats["drained"] += 1
                            drained += 1
                            continue  # drained — nothing left to reap
                        except Exception:
                            logger.exception(
                                "Drain failed for kind=%s id=%s",
                                kind,
                                inst.id,
                            )

                # Reap path: teardown-eligible stateful instances whose bound
                # work has finished or gone idle (clean → delete; dirty+reachable
                # → snapshot then delete; dirty+unreachable → bounded retry /
                # give_up). Replaces the old keep-alive-on-snapshot-failure loop.
                # Gated on ReapableInstanceManager: BOTH WorkspaceInstanceManager
                # and VMInstanceManager implement the reap predicates and qualify.
                # A StatefulInstanceManager that is NOT reapable (none ship today)
                # is intentionally excluded so _reap never AttributeErrors on a
                # missing predicate (see test_stateful_non_reapable_manager_is_skipped).
                if isinstance(manager, ReapableInstanceManager):
                    try:
                        await self._reap(manager, inst, stats)
                    except Exception:
                        logger.exception("Reap failed for kind=%s id=%s", kind, inst.id)

            # Once-per-tick orphan sweep — optional manager capability for
            # detached resources that never surface as a live Instance (e.g. a
            # workspace PVC whose pod is already gone). Managers without the
            # method are unaffected; guarded like the other optional hooks
            # (cf. ensure_workspace's workspace_pod_live probe).
            reap_orphans = getattr(manager, "reap_orphans", None)
            if reap_orphans is not None:
                try:
                    stats["orphans_reaped"] = await reap_orphans()
                except Exception:
                    logger.exception("Orphan sweep failed for kind=%s", kind)

            report[kind] = stats
            if any(v for k, v in stats.items() if k != "listed"):
                logger.info(
                    "Lifecycle tick kind=%s %s",
                    kind,
                    {k: v for k, v in stats.items() if v},
                )
        return report

    async def _reap(self, manager, inst, stats) -> None:
        """Decision flow for tearing down a teardown-eligible stateful instance.

        clean            -> delete now (no probe)
        dirty+reachable  -> snapshot; delete if captured else record attempt
        dirty+unreach    -> give_up if exhausted, else record attempt

        Replaces the old keep-alive-on-snapshot-failure loop: an instance that
        can never be snapshotted (gone/unreachable pod) is force-deleted after
        a bounded number of attempts rather than retried forever.
        """
        # Taking ownership can already probe external teardown identity. Do
        # not claim a dispatch-owned VM that is still preparing: an unrelated
        # probe failure would otherwise fence dispatch until the claim expires.
        if not await manager.is_reapable(inst):
            return
        async with self._lifecycle_action(manager, inst, source="reap") as permit:
            if not permit.local:
                return
            if not await manager.is_reapable(inst):
                permit.complete()
                return
            if not await manager.is_dirty(inst):
                await manager.delete(inst, grace_s=0)
                if permit.local:
                    stats["reaped"] += 1
                return
            if await manager.is_reachable(inst):
                ref = await manager.snapshot(inst)
                if not permit.local:
                    return
                if ref:
                    await manager.delete(inst, grace_s=0)
                    if permit.local:
                        stats["reaped"] += 1
                else:
                    await manager.record_attempt(inst)
                    if permit.local:
                        stats["reap_attempts"] += 1
                return
            if await manager.attempts_exhausted(inst):
                await manager.give_up(inst, grace_s=0)
                if not permit.local:
                    return
                stats["reap_forced"] += 1
                # Data-loss signal: a dirty instance we could never snapshot.
                # Logged (not a Prometheus counter — codebase has none) so
                # log-based alerting can fire on it. Applies to any reapable kind
                # (workspace, vm); kind-specific detail stays out of the message.
                logger.warning(
                    "Lifecycle reaper force-deleted dirty unreachable instance "
                    "kind=%s id=%s bound=%s — state not captured "
                    "(snapshot attempts exhausted)",
                    manager.kind,
                    inst.id,
                    inst.bound_to,
                )
            else:
                await manager.record_attempt(inst)
                if permit.local:
                    stats["reap_attempts"] += 1

    @staticmethod
    @asynccontextmanager
    async def _lifecycle_action(
        manager: InstanceLifecycleManager,
        inst: Instance,
        *,
        source: str,
    ) -> AsyncIterator[Any]:
        """Use a manager's optional cross-domain ownership section.

        Agents and default-off workspace/VM managers have no hook and retain
        the exact historical call sequence.  Completion-aware managers keep a
        single jobs-row claim across snapshot -> delete, closing the former
        read-before-I/O window rather than merely checking twice.
        """

        action = getattr(manager, "lifecycle_action", None)
        if action is None or not bool(
            getattr(manager, "completion_lifecycle_ownership_enabled", False)
        ):

            class _LegacyPermit:
                local = True

                @staticmethod
                def complete() -> None:
                    return None

            yield _LegacyPermit()
            return
        async with action(inst, source=source) as permit:
            yield permit

    @staticmethod
    def is_stateful(manager: InstanceLifecycleManager) -> bool:
        """Type-narrowing helper for snapshot-capable managers.

        The reconciler will use this to gate snapshot calls before
        drain/delete on stateful instances (workspaces, VMs).
        """
        return isinstance(manager, StatefulInstanceManager)

    @staticmethod
    def is_drift(inst: Instance, expected: set[str]) -> bool:
        """Drift predicate: instance version is not in the expected set.

        Returns False when ``expected`` is empty (no SHA-pinned image —
        local dev) or when the instance has no recorded version yet
        (in flight). Drift detection is opt-in by configuration.
        """
        if not expected or inst.version is None:
            return False
        return inst.version not in expected


async def lifecycle_reconciler_loop(
    shutdown_event: asyncio.Event,
    reconciler: InstanceLifecycleReconciler,
) -> None:
    """Background task driving the unified instance lifecycle reconciler.

    Runs every 60 seconds. The reconciler delegates to per-kind
    managers (``AgentInstanceManager`` etc.) for drift detection and
    drain. Crash detection still flows through ``reap_pods`` in the
    sibling ``agent_pool_reconciler`` for now; consolidation is a
    follow-up.
    """
    logger.info("Lifecycle reconciler loop started")
    while not shutdown_event.is_set():
        try:
            await reconciler.tick()
        except Exception:
            logger.exception("Lifecycle reconciler tick failed")

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=60.0)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Lifecycle reconciler loop stopped")
