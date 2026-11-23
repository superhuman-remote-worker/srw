"""Stateless executor capacity — the numbers KEDA sizes the pool from.

Design: knowledge-base/knowledge/features/capacity_ux_and_queue_autoscaling.md
§2. Demand is read straight off ``run_queue`` with the claim's own predicates
(``src/shared/run_queue/queries.py`` ``_CLAIM_SQL``): a unit is *busy* while
it holds a live lease and *runnable* while it is queued and past ``run_after``.
The desired replica count is ``GREATEST(min_replicas, busy + runnable +
reserve)``.

KEEP IN SYNC: the Helm ``ScaledObject`` query in
``helm/templates/agent/stateless-scaledobject.yaml`` mirrors ``BUSY_PREDICATE``
and ``RUNNABLE_PREDICATE`` literally so the scaler and this endpoint compute the
same ``desired``. Change both together, and the claim predicate they mirror.

Kubernetes access is in-cluster only by default: this module never loads an
ambient kubeconfig implicitly (a stale local kubeconfig makes a green local
run and a red CI run). Set ``STATELESS_CAPACITY_KUBECONFIG_FALLBACK=1`` for an
off-cluster developer run. Every read tolerates an unavailable API by
reporting ``None`` for the executor inventory — it never raises.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Callable

from orchestrator.services.pinned_k8s_effect import run_bounded_k8s_call
from shared.run_queue import list_parked

logger = logging.getLogger(__name__)

STATELESS_POD_LABEL_SELECTOR = "srw/class=agent-stateless"
DELETION_COST_ANNOTATION = "controller.kubernetes.io/pod-deletion-cost"

# Claim-predicate mirrors (see module docstring). A leased row past
# ``leased_until`` is reaper territory, not capacity; a queued row before
# ``run_after`` is deliberate backoff, not demand.
BUSY_PREDICATE = "state = 'leased' AND leased_until > now()"
RUNNABLE_PREDICATE = "state = 'queued' AND run_after <= now()"

K8S_READ_REQUEST_TIMEOUT = (5.0, 15.0)

_DEMAND_SQL = f"""
SELECT clock_timestamp() AS observed_at,
       (SELECT count(*)::int FROM run_queue
         WHERE {BUSY_PREDICATE}) AS busy,
       (SELECT count(*)::int FROM run_queue
         WHERE {RUNNABLE_PREDICATE}
           AND unit_kind = 'session_turn') AS runnable_session_turn,
       (SELECT count(*)::int FROM run_queue
         WHERE {RUNNABLE_PREDICATE}
           AND unit_kind = 'worker_batch') AS runnable_worker_batch,
       (SELECT count(*)::int FROM run_queue
         WHERE {RUNNABLE_PREDICATE}
           AND unit_kind = 'bg_task') AS runnable_bg_task,
       (SELECT count(*)::int FROM run_queue
         WHERE {RUNNABLE_PREDICATE}) AS runnable_total,
       (SELECT GREATEST(
                   0.0,
                   extract(epoch FROM (
                       clock_timestamp() - min(GREATEST(queued_at, run_after))
                   ))
               )::float8
          FROM run_queue
         WHERE {RUNNABLE_PREDICATE}) AS oldest_queued_age_s,
       (SELECT min(GREATEST(queued_at, run_after))
          FROM run_queue
         WHERE {RUNNABLE_PREDICATE}
           AND unit_kind = 'session_turn') AS oldest_session_turn_runnable_at,
       (SELECT GREATEST(
                   0.0,
                   extract(epoch FROM (
                       clock_timestamp() - min(GREATEST(queued_at, run_after))
                   ))
               )::float8
          FROM run_queue
         WHERE {RUNNABLE_PREDICATE}
           AND unit_kind = 'session_turn') AS oldest_session_turn_age_s
"""

_BUSY_PODS_SQL = f"""
SELECT DISTINCT leased_by
FROM run_queue
WHERE {BUSY_PREDICATE}
  AND leased_by IS NOT NULL
"""


def _env_int(name: str, default: int, *, minimum: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return max(minimum, int(raw.strip()))
    except ValueError:
        logger.warning("%s=%r is not an integer; using %d", name, raw, default)
        return default


def env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def namespace() -> str:
    """The namespace the stateless pool runs in (same env the provisioners read)."""
    return os.environ.get("WORKSPACE_NAMESPACE", "superhuman-remote-worker")


@dataclass(frozen=True, slots=True)
class CapacityParams:
    min_replicas: int
    reserve: int

    @classmethod
    def from_env(cls) -> "CapacityParams":
        return cls(
            min_replicas=_env_int("STATELESS_AUTOSCALE_MIN_REPLICAS", 2, minimum=0),
            reserve=_env_int("STATELESS_AUTOSCALE_RESERVE", 1, minimum=0),
        )


@dataclass(frozen=True, slots=True)
class QueueDemand:
    observed_at: datetime | None
    busy: int
    runnable_session_turn: int
    runnable_worker_batch: int
    runnable_total: int
    oldest_queued_age_s: float | None
    runnable_bg_task: int = 0
    oldest_session_turn_runnable_at: datetime | None = None
    oldest_session_turn_age_s: float | None = None


@dataclass(frozen=True, slots=True)
class ExecutorInventory:
    total: int
    ready: int


def desired_replicas(demand: QueueDemand, params: CapacityParams) -> int:
    """``GREATEST(min_replicas, busy + runnable + reserve)`` — the scaler's value."""
    return max(
        params.min_replicas, demand.busy + demand.runnable_total + params.reserve
    )


@asynccontextmanager
async def acquire_connection(source: Any) -> AsyncIterator[Any]:
    acquire = getattr(source, "acquire", None)
    if acquire is None:
        yield source
        return
    async with acquire() as conn:
        yield conn


async def read_demand(conn: Any) -> QueueDemand:
    row = await conn.fetchrow(_DEMAND_SQL)
    if row is None:
        return QueueDemand(None, 0, 0, 0, 0, None)
    age = row["oldest_queued_age_s"]
    turn_age = row.get("oldest_session_turn_age_s")
    return QueueDemand(
        observed_at=row["observed_at"],
        busy=int(row["busy"] or 0),
        runnable_session_turn=int(row["runnable_session_turn"] or 0),
        runnable_worker_batch=int(row["runnable_worker_batch"] or 0),
        runnable_bg_task=int(row.get("runnable_bg_task") or 0),
        runnable_total=int(row["runnable_total"] or 0),
        oldest_queued_age_s=float(age) if age is not None else None,
        oldest_session_turn_runnable_at=row.get("oldest_session_turn_runnable_at"),
        oldest_session_turn_age_s=float(turn_age) if turn_age is not None else None,
    )


async def busy_pod_names(conn: Any) -> set[str]:
    """Pod names holding a live lease (``leased_by`` is the claimant pod name)."""
    rows = await conn.fetch(_BUSY_PODS_SQL)
    return {str(r["leased_by"]) for r in rows if r["leased_by"]}


# --------------------------------------------------------------------------- #
# Kubernetes
# --------------------------------------------------------------------------- #


def load_core_api() -> Any | None:
    """In-cluster CoreV1Api, or ``None`` when Kubernetes is not reachable.

    Never loads an ambient kubeconfig unless
    ``STATELESS_CAPACITY_KUBECONFIG_FALLBACK`` is set (developer opt-in).
    """
    try:
        from kubernetes import client as k8s_client, config as k8s_config
    except ImportError:
        return None
    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        if not env_flag("STATELESS_CAPACITY_KUBECONFIG_FALLBACK", False):
            return None
        try:
            k8s_config.load_kube_config()
        except Exception:
            return None
    except Exception:
        return None
    return k8s_client.CoreV1Api()


def _pod_is_terminating(pod: Any) -> bool:
    return (
        getattr(getattr(pod, "metadata", None), "deletion_timestamp", None) is not None
    )


def _pod_is_ready(pod: Any) -> bool:
    conditions = getattr(getattr(pod, "status", None), "conditions", None) or ()
    for condition in conditions:
        if getattr(condition, "type", None) == "Ready":
            return str(getattr(condition, "status", "")).lower() == "true"
    return False


async def list_stateless_pods(core_api: Any, *, namespace: str) -> list[Any]:
    result = await run_bounded_k8s_call(
        core_api.list_namespaced_pod,
        namespace,
        label_selector=STATELESS_POD_LABEL_SELECTOR,
        _request_timeout=K8S_READ_REQUEST_TIMEOUT,
    )
    return list(getattr(result, "items", None) or [])


async def read_inventory(core_api: Any, *, namespace: str) -> ExecutorInventory:
    pods = [
        pod
        for pod in await list_stateless_pods(core_api, namespace=namespace)
        if not _pod_is_terminating(pod)
    ]
    return ExecutorInventory(
        total=len(pods), ready=sum(1 for pod in pods if _pod_is_ready(pod))
    )


async def capacity_snapshot(
    db: Any,
    *,
    core_api_factory: Callable[[], Any | None] = load_core_api,
    params: CapacityParams | None = None,
    pod_namespace: str | None = None,
    parked_limit: int = 50,
) -> dict[str, Any]:
    """The admin payload: what the scaler sees, plus the pool it is sizing,
    plus the parked worklist (stateless_turn_resilience.md step 2)."""
    # chat_pool_stall imports this module, so import it lazily.
    from orchestrator.services.chat_pool_stall import (
        chat_pool_stalled,
        stall_threshold_s,
    )

    params = params or CapacityParams.from_env()
    async with acquire_connection(db) as conn:
        demand = await read_demand(conn)
        parked_rows = await list_parked(conn, limit=parked_limit)
    inventory: ExecutorInventory | None = None
    try:
        core_api = await asyncio.to_thread(core_api_factory)
        if core_api is not None:
            inventory = await read_inventory(
                core_api, namespace=pod_namespace or namespace()
            )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.info("stateless capacity: executor inventory unavailable: %s", exc)
    threshold_s = stall_threshold_s()
    return {
        "observed_at": (demand.observed_at.isoformat() if demand.observed_at else None),
        "executors": {
            "total": inventory.total if inventory else None,
            "ready": inventory.ready if inventory else None,
            "busy": demand.busy,
        },
        "queued": {
            "session_turn": demand.runnable_session_turn,
            "worker_batch": demand.runnable_worker_batch,
            "bg_task": demand.runnable_bg_task,
            "total": demand.runnable_total,
        },
        "oldest_queued_age_s": demand.oldest_queued_age_s,
        "oldest_session_turn_age_s": demand.oldest_session_turn_age_s,
        "chat_pool_stalled": chat_pool_stalled(demand, threshold_s=threshold_s),
        "stall_threshold_s": threshold_s,
        "desired": desired_replicas(demand, params),
        "params": {"min_replicas": params.min_replicas, "reserve": params.reserve},
        "parked": [parked_entry(row) for row in parked_rows],
    }


def parked_entry(row: dict[str, Any]) -> dict[str, Any]:
    """One parked unit for the admin list; ids and times as strings."""

    def _text(value: Any) -> str | None:
        return None if value is None else str(value)

    parked_at = row.get("parked_at")
    return {
        "unit_id": _text(row.get("unit_id")),
        "unit_kind": row.get("unit_kind"),
        "thread_id": _text(row.get("thread_id")),
        "title": row.get("title"),
        "owner": row.get("owner"),
        "park_reason": row.get("park_reason"),
        "parked_at": parked_at.isoformat() if hasattr(parked_at, "isoformat") else None,
        "attempts": int(row.get("attempts_since_completion") or 0),
        "attach_failures": int(row.get("attach_failures") or 0),
        "last_error": row.get("last_error"),
        "pending_input": bool(row.get("pending_input")),
    }


__all__ = [
    "BUSY_PREDICATE",
    "DELETION_COST_ANNOTATION",
    "RUNNABLE_PREDICATE",
    "STATELESS_POD_LABEL_SELECTOR",
    "CapacityParams",
    "ExecutorInventory",
    "QueueDemand",
    "busy_pod_names",
    "capacity_snapshot",
    "desired_replicas",
    "env_flag",
    "list_stateless_pods",
    "load_core_api",
    "namespace",
    "parked_entry",
    "read_demand",
    "read_inventory",
]
