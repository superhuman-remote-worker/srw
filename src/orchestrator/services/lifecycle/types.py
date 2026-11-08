"""Type and Protocol definitions for unified instance lifecycle management.

The reconciler operates on a ``list[InstanceLifecycleManager]`` registry —
one manager per instance kind (agent, workspace, vm) — and uses only the
methods declared here. Anything not on these Protocols stays inside the
respective manager's implementation.

The kind hierarchy:

    InstanceLifecycleManager       — agents (no per-instance state)
        StatefulInstanceManager    — workspaces, VMs (snapshot-able state)

Type narrowing in the reconciler (``isinstance(mgr, StatefulInstanceManager)``)
gates snapshot/restore calls, so the agent path doesn't need a no-op
snapshot method.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass(slots=True)
class Instance:
    """One managed instance, observable by the reconciler.

    ``id`` is the provisioner-native identifier (K8s pod name, VM uuid,
    Compose container hostname). ``version`` is the build SHA (or other
    versioning token) that the reconciler compares against the manager's
    ``expected_version()``. ``bound_to`` is the job/thread id when the
    instance is attached to specific work, or ``None`` for unbound or
    warm-pool instances.

    ``metadata`` carries kind-specific extras the reconciler doesn't
    interpret (pod labels, ssh host:port, NATS subject, etc.). It exists
    so manager implementations can stash whatever they need on the
    Instance without requiring round-trips to look it up later.
    """

    kind: str
    id: str
    version: str | None = None
    bound_to: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class InstanceLifecycleManager(Protocol):
    """Contract every instance kind implements.

    Stateless instances (agents) implement this directly. Stateful
    instances (workspaces, VMs) implement ``StatefulInstanceManager``,
    which extends this with snapshot/restore.

    Implementations are expected to be idempotent: ``drain`` and
    ``delete`` on an already-gone instance must not raise.
    """

    kind: str

    async def list_instances(self) -> list[Instance]: ...

    async def expected_versions(self) -> set[str]:
        """Build SHAs the orchestrator currently considers acceptable.

        A set rather than a single value because some kinds have multiple
        valid pinned images at once (e.g. agent has separate ``AGENT_IMAGE``
        and ``PERSISTENT_AGENT_IMAGE`` env vars). Returns an empty set
        when no SHA-pinned image is configured (local dev with
        ``:latest``); the reconciler then skips drift checks.
        """
        ...

    async def is_healthy(self, inst: Instance) -> bool:
        """Liveness predicate appropriate to the kind.

        Agents: heartbeat freshness + pod phase Running.
        Workspaces: pod phase Running + SSH ping.
        VMs: backend-specific probe.
        """
        ...

    async def is_idle(self, inst: Instance) -> bool:
        """True when the instance has no active work attached.

        Idle instances can be drained without interrupting a user. Busy
        instances get the ``drain_pending`` intent and drain on the next
        idle transition.
        """
        ...

    async def signal_drain_pending(self, inst: Instance) -> None:
        """Soft drain signal — fired on every drifted instance regardless
        of idle/busy state.

        Cheap, idempotent. For agents this writes ``intents.should_drain``
        so the in-pod heartbeat callback (Phase 1c) can react at the
        next safe phase boundary (version_upgrade Continue-as-New).
        For kinds without an in-pod drain hook (workspaces, VMs) this
        is a no-op — the reconciler picks them up on a future tick once
        the bound work becomes idle, and ``drain`` actuates immediately.
        """
        ...

    async def drain(self, inst: Instance, grace_s: int) -> None:
        """Hard drain — actuates immediately on an idle instance.

        Distinct from ``signal_drain_pending``: drain is the
        delete/snapshot path, called only for instances the reconciler
        has determined are safe to terminate now.
        """
        ...

    async def delete(self, inst: Instance, grace_s: int) -> None:
        """Force termination. Bypasses graceful drain semantics."""
        ...


@runtime_checkable
class StatefulInstanceManager(InstanceLifecycleManager, Protocol):
    """Adds snapshot/restore for instances with persistent state.

    The reconciler ensures snapshot runs before any drain/delete that
    would lose state. Implementations should make snapshot idempotent
    and tolerate a stale snapshot reference on restore.
    """

    async def snapshot(self, inst: Instance) -> str | None:
        """Capture instance state to durable storage. Returns a reference
        the manager (or its caller) can later pass to ``restore``. Returns
        ``None`` if the instance has no state worth snapshotting (empty
        workspace, freshly-booted VM)."""
        ...

    async def restore(self, inst: Instance, snapshot_ref: str) -> None:
        """Hydrate ``inst`` from the snapshot at ``snapshot_ref``."""
        ...


@runtime_checkable
class ReapableInstanceManager(StatefulInstanceManager, Protocol):
    """A stateful manager that also supports the reaper's teardown decision
    flow (clean/dirty gate + bounded snapshot-retry escape hatch).

    The reconciler's reap branch gates on THIS protocol, not on
    ``StatefulInstanceManager`` — VMs are stateful (snapshot/restore) but do
    not implement the reap predicates, so they must not be routed through
    ``_reap``. ``WorkspaceInstanceManager`` implements this; structural typing
    means it qualifies automatically once all methods below exist.
    """

    async def is_reapable(self, inst: Instance) -> bool:
        """True when the bound work no longer needs the instance."""
        ...

    async def is_dirty(self, inst: Instance) -> bool:
        """True when there may be un-snapshotted state worth preserving."""
        ...

    async def is_reachable(self, inst: Instance) -> bool:
        """True when the instance can currently be reached for a snapshot."""
        ...

    async def attempts_exhausted(self, inst: Instance) -> bool:
        """True when bounded snapshot retries have been used up."""
        ...

    async def record_attempt(self, inst: Instance) -> None:
        """Persist one more failed snapshot attempt."""
        ...

    async def give_up(self, inst: Instance, grace_s: int) -> bool | None:
        """Terminal action when a dirty instance can never be snapshotted.

        False means nothing was deleted; True or None (a manager that does
        not report) lets the caller record a forced delete.
        """
        ...
