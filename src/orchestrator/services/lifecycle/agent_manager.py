"""AgentInstanceManager — first concrete InstanceLifecycleManager.

Wraps the existing ``AgentProvisioner`` and ``agents`` table to surface
agent pods to the unified lifecycle reconciler. Owns the SHA-drift
decision for agents; replaces the standalone
``_drain_stale_image_agents`` reconciler in ``orchestrator.main`` and
the inline drain-marking that used to live at the dispatch sites.

Phase 1b scope: drift detection. Crash detection still flows through
``AgentProvisioner.reap_pods`` for now — folding it into the manager
is a follow-up once both code paths are exercised in production.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

from orchestrator.services.lifecycle.types import Instance

logger = logging.getLogger(__name__)


def expected_agent_shas() -> set[str]:
    """Return SHAs from configured agent image tags.

    Reads ``AGENT_IMAGE`` and ``PERSISTENT_AGENT_IMAGE`` env vars and
    extracts the SHA suffix from tags formatted as ``...:sha-<hash>``.

    Public so the lifespan wiring can pre-compute expected SHAs at
    startup (cheap, but lets the manager be constructed without
    immediately hitting env vars).
    """
    shas: set[str] = set()
    for var in ("AGENT_IMAGE", "PERSISTENT_AGENT_IMAGE"):
        tag = os.environ.get(var, "")
        if ":sha-" in tag:
            shas.add(tag.rsplit(":sha-", 1)[-1])
    return shas


class AgentInstanceManager:
    """Lifecycle manager for the agent kind."""

    kind = "agent"

    def __init__(self, provisioner: Any, db: Any, label_selector: str | None = None):
        # Typing intentionally loose — production wiring passes the
        # singleton ``agent_provisioner`` and ``postgres_db`` modules,
        # tests pass mocks. Hard-typing them would force test code
        # through the actual module-init paths.
        self._provisioner = provisioner
        self._db = db
        self._label_selector = label_selector or "srw/managed-by=agent-provisioner"

    # -------------------------------------------------------------------------
    # Protocol implementation
    # -------------------------------------------------------------------------

    async def expected_versions(self) -> set[str]:
        return expected_agent_shas()

    async def list_instances(self) -> list[Instance]:
        """Cross-reference K8s pod listing with the agents DB table.

        Each returned ``Instance`` carries enough metadata for the
        downstream ``is_idle``/``is_healthy`` predicates to answer
        without further DB roundtrips.
        """
        if not self._provisioner_ready():
            return []

        pods, agent_rows = await asyncio.gather(
            self._list_pods(),
            self._fetch_agent_rows(),
        )
        agents_by_hostname = {
            row["hostname"]: row for row in agent_rows if row.get("hostname")
        }

        instances: list[Instance] = []
        for pod in pods:
            name = pod.metadata.name
            row = agents_by_hostname.get(name)
            metadata: dict[str, Any] = {
                "pod_phase": pod.status.phase,
                "row_present": row is not None,
                "pod_uid": str(getattr(pod.metadata, "uid", "") or ""),
            }
            version: str | None = None
            bound_to: str | None = None
            if row is not None:
                meta = row.get("metadata") or {}
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except (json.JSONDecodeError, ValueError):
                        meta = {}
                version = meta.get("build_sha") or None
                metadata["agent_id"] = str(row["id"])
                metadata["status"] = row.get("status")
                metadata["current_job_id"] = (
                    str(row["current_job_id"]) if row.get("current_job_id") else None
                )
                metadata["thread_id"] = (
                    str(row["thread_id"]) if row.get("thread_id") else None
                )
                bound_to = metadata["thread_id"] or metadata["current_job_id"]
            else:
                # Pod without DB row — likely registered too recently or
                # heartbeat hasn't landed. Read SHA from the pod label
                # instead (Phase 1a put it there for this case).
                labels = pod.metadata.labels or {}
                version = labels.get("srw/build-sha")
            instances.append(
                Instance(
                    kind=self.kind,
                    id=name,
                    version=version,
                    bound_to=bound_to,
                    metadata=metadata,
                )
            )
        return instances

    async def is_healthy(self, inst: Instance) -> bool:
        # Phase 1b: defer to reap_pods for crash detection. The
        # reconciler doesn't act on health yet for agents — it acts on
        # drift. is_healthy returns True except for the obvious cases.
        phase = inst.metadata.get("pod_phase")
        return phase in (None, "Running", "Pending")

    async def is_idle(self, inst: Instance) -> bool:
        if not inst.metadata.get("row_present"):
            # No DB row → can't claim idle (and we don't have signal to
            # drain anything we can't fully describe).
            return False
        return (
            inst.metadata.get("status") == "ready"
            and inst.metadata.get("current_job_id") is None
            and inst.metadata.get("thread_id") is None
        )

    async def signal_drain_pending(self, inst: Instance) -> None:
        """Write ``intents.should_drain=true`` without changing status.

        Fired on every drift detection (idle and busy). The Phase 1c
        in-pod heartbeat callback reads this on the next 5s tick and
        either self-exits (idle worker, persistent session) or sets a
        flag the graph picks up at the next phase boundary
        (version_upgrade Continue-as-New for busy worker).

        Idempotent and cheap — JSONB merge produces the same result on
        repeat ticks. Logged at debug to avoid spam every 60s while
        a stale agent waits for its phase boundary.
        """
        agent_id = inst.metadata.get("agent_id")
        if not agent_id:
            return
        intent_payload = json.dumps(
            {"should_drain": True, "drain_reason": "stale_image"}
        )
        async with self._db.acquire() as conn:
            await conn.execute(
                "UPDATE agents SET intents = intents || $1::jsonb WHERE id = $2",
                intent_payload,
                agent_id,
            )
        logger.debug(
            "Drain intent set for agent %s (build_sha=%s)",
            agent_id,
            inst.version,
        )

    async def drain(self, inst: Instance, grace_s: int) -> None:
        """Hard drain for an idle stale-SHA agent.

        Sets the intent (idempotent with ``signal_drain_pending``) and
        flips ``status`` to ``draining`` only when currently ``ready``,
        so the existing ``reap_pods`` ``drained`` category force-deletes
        the pod on the next agent_pool_reconciler pass. CASE preserves
        the status for agents that aren't actually idle in the DB
        (defense in depth — the reconciler already gates on is_idle).
        """
        agent_id = inst.metadata.get("agent_id")
        if not agent_id:
            logger.debug("drain skipped: no agent_id on instance %s", inst.id)
            return
        intent_payload = json.dumps(
            {"should_drain": True, "drain_reason": "stale_image"}
        )
        async with self._db.acquire() as conn:
            await conn.execute(
                """
                UPDATE agents
                SET intents = intents || $1::jsonb,
                    status  = CASE WHEN status IN ('ready') THEN 'draining' ELSE status END
                WHERE id = $2
                """,
                intent_payload,
                agent_id,
            )
        logger.info(
            "Drained agent %s (build_sha=%s, expected mismatch)",
            agent_id,
            inst.version,
        )

    async def delete(self, inst: Instance, grace_s: int) -> None:
        """Force-delete the underlying pod via the provisioner."""
        if not self._provisioner_ready():
            return
        await self._provisioner.delete_agent_pod(
            inst.id,
            expected_pod_uid=str(inst.metadata.get("pod_uid") or ""),
        )

    # -------------------------------------------------------------------------
    # Startup reconciliation (called from main.lifespan)
    # -------------------------------------------------------------------------

    async def list_pods(self) -> list[dict[str, Any]]:
        """Lightweight pod summary, used at orchestrator startup to rebuild
        the in-memory view from K8s reality before accepting heartbeats.

        Returns a list of {name, phase, build_sha, thread_id} dicts.
        """
        if not self._provisioner_ready():
            return []
        pods = await self._list_pods()
        out: list[dict[str, Any]] = []
        for pod in pods:
            labels = pod.metadata.labels or {}
            out.append(
                {
                    "name": pod.metadata.name,
                    "phase": pod.status.phase,
                    "build_sha": labels.get("srw/build-sha"),
                    "thread_id": labels.get("srw/thread-id"),
                    "purpose": labels.get("srw/purpose"),
                }
            )
        return out

    # -------------------------------------------------------------------------
    # Internals
    # -------------------------------------------------------------------------

    def _provisioner_ready(self) -> bool:
        # The provisioner sets this attribute on connect(); guard against
        # being called before the lifespan finishes wiring (or in tests
        # where it isn't wired at all).
        return bool(getattr(self._provisioner, "_k8s_available", False))

    async def _list_pods(self) -> list[Any]:
        try:
            result = await asyncio.to_thread(
                self._provisioner._core_api.list_namespaced_pod,
                namespace=self._provisioner._namespace,
                label_selector=self._label_selector,
            )
        except Exception:
            logger.exception("Failed to list agent pods for lifecycle")
            return []
        return list(result.items)

    async def _fetch_agent_rows(self) -> list[dict[str, Any]]:
        if self._db is None:
            return []
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, hostname, status, current_job_id, thread_id,
                           metadata, agent_mode, last_heartbeat, image
                    FROM agents
                    WHERE hostname IS NOT NULL
                    """
                )
            return [dict(row) for row in rows]
        except Exception:
            logger.exception("Failed to query agents table for lifecycle")
            return []
