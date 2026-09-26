"""Read-only hints for continuing an already-issued initial Session workspace.

Neither a hint nor its metadata pointer grants effect authority. Callers reread
under the existing owner guard, then use the ordinary source admission helpers.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class SessionCreationCursor:
    created_at: datetime
    thread_id: str
    source_id: str


@dataclass(frozen=True)
class SessionCreationCandidate:
    cursor: SessionCreationCursor
    lane: str
    runtime_generation: str
    agent_id: str | None
    attach_token: str | None
    claim_token: int | None
    namespace: str
    pod_uid: str
    pvc_uid: str | None
    seed_configmap_uid: str | None
    service_uid: str | None
    owner_fingerprint: str
    source_fingerprint: str

    @property
    def thread_id(self) -> str:
        return self.cursor.thread_id

    @property
    def source_id(self) -> str:
        return self.cursor.source_id


@dataclass(frozen=True)
class SessionCreationPage:
    candidates: tuple[SessionCreationCandidate, ...]
    cursor: SessionCreationCursor | None
    exhausted: bool


# Page over durable open sources, including hints later held by the strict
# parser. Advancing the raw cursor prevents an ineligible first page from
# hiding later work. Existing partial open-source indexes bound the history
# scan; the all-history owner exclusion uses the concurrent 0294 index.
SESSION_CREATION_SCAN_SQL = """
WITH sources AS (
    SELECT i.thread_id, i.attempt_id AS source_id, i.created_at,
           'pinned'::text AS lane, to_jsonb(i) AS source
    FROM thread_workspace_provision_intents i
    WHERE i.status='planned' AND i.pod_uid IS NOT NULL
      AND ($1::uuid IS NULL OR i.thread_id=$1)
    UNION ALL
    SELECT r.owner_id, r.id, r.created_at, 'stateless', to_jsonb(r)
    FROM managed_repository_workspace_creation_reservations r
    WHERE r.owner_kind='thread' AND r.scope='workspace_container'
      AND r.settled_at IS NULL AND r.phase='runtime_bound'
      AND r.operation_kind='create' AND r.cancel_requested_at IS NULL
      AND ($1::uuid IS NULL OR r.owner_id=$1)
)
SELECT s.*, to_jsonb(t) AS owner,
       NOT EXISTS (
           SELECT 1 FROM thread_workspace_provision_intents h
           WHERE h.thread_id=s.thread_id AND h.attempt_id<>s.source_id
       ) AND NOT EXISTS (
           SELECT 1 FROM managed_repository_workspace_creation_reservations h
           WHERE h.owner_kind='thread' AND h.owner_id=s.thread_id
             AND h.scope='workspace_container' AND h.id<>s.source_id
       ) AND NOT EXISTS (
           SELECT 1 FROM managed_repository_workspace_cleanup_intents h
           WHERE h.owner_kind='thread' AND h.owner_id=s.thread_id
             AND h.scope='workspace_container'
       ) AS initial_source
FROM sources s JOIN threads t ON t.id=s.thread_id
WHERE t.status IN ('created','active') AND t.execution_lane=s.lane
  AND ($2::timestamptz IS NULL OR
       (s.created_at,s.thread_id,s.source_id)>($2,$3::uuid,$4::uuid))
ORDER BY s.created_at,s.thread_id,s.source_id LIMIT $5
"""


def _object(value: Any) -> dict:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("not an object")
    return value


def _uuid(value: Any, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or str(UUID(value)) != value:
        raise ValueError("noncanonical identity")
    return value


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def candidate_from_record(row: Any) -> SessionCreationCandidate | None:
    """Fail closed on malformed, retired, restored or historical source hints."""
    try:
        owner, source = _object(row["owner"]), _object(row["source"])
        metadata = _object(owner["metadata"])
        workspace = _object(metadata["workspace_container"])
        config = _object(metadata["config_override"])
        tid = _uuid(str(row["thread_id"]))
        sid = _uuid(str(row["source_id"]))
        generation = _uuid(owner["runtime_generation"])
        agent = _uuid(owner.get("agent_id"), optional=True)
        attach = _uuid(owner.get("runtime_attach_token"), optional=True)
        lane = row["lane"]
        if (
            row["initial_source"] is not True
            or owner.get("status") not in {"created", "active"}
            or owner.get("execution_lane") != lane
            or owner.get("runtime_retirement_token") is not None
            or (agent is None) != (attach is None)
            or _object(config.get("workspace")).get("backend") != "sandbox"
            or workspace.get("provisioner") != "k8s"
            or workspace.get("status")
            not in {"pending", "created", "creating", "failed"}
            or workspace.get("_snapshot_restore_required", False) is not False
            or any(
                metadata.get(k) not in (None, {}) for k in ("vm", "_workspace_binding")
            )
            or any(
                k in metadata
                for k in (
                    "_stateless_workspace_retirement_pending",
                    "_stateless_claim_retirement",
                    "_stateless_workspace_retirement_settled",
                    "_stateless_claim_loss_hold",
                    "_stateless_claim_losses",
                    "_pinned_retained_creation_attempt",
                )
            )
            or any(
                workspace.get(k) is not None
                for k in (
                    "_canvas_workspace_generation",
                    "_docker_workspace_lease_id",
                    "suspended_at",
                    "snapshot_key",
                )
            )
        ):
            return None
        uids = tuple(
            _uuid(source.get(k), optional=(k != "pod_uid"))
            for k in ("pod_uid", "pvc_uid", "seed_configmap_uid", "service_uid")
        )
        if (uids[1] is None) != (uids[3] is None):
            return None
        if lane == "pinned":
            if (
                source.get("status") != "planned"
                or source.get("runtime_generation") != generation
                or source.get("created_agent_id") != agent
                or source.get("created_attach_token") != attach
                or source.get("previous_binding") != {}
                or any(
                    source.get(k) is not None
                    for k in (
                        "retained_binding_generation",
                        "retained_pvc_uid",
                        "retained_service_uid",
                        "retained_source_attempt_id",
                    )
                )
                or workspace.get("_workspace_provision_attempt") != sid
                or workspace.get("_workspace_provision_generation") != generation
                or workspace.get("_runtime_incarnation") is not None
                or any(
                    (source.get(name) is None) != (uid is None)
                    for name, uid in zip(
                        ("pvc_name", "seed_configmap_name", "service_name"), uids[1:]
                    )
                )
            ):
                return None
            namespace = source["namespace"]
            token = None
            source_keys = (
                "attempt_id",
                "runtime_generation",
                "created_agent_id",
                "created_attach_token",
                "namespace",
                "pod_name",
                "pvc_name",
                "seed_configmap_name",
                "service_name",
                "network_tier",
                "manifest_fingerprint",
                "previous_binding",
                "status",
            )
        elif lane == "stateless":
            marker = _object(workspace.get("_runtime_creation"))
            token = source.get("claim_token")
            if (
                source.get("thread_runtime_generation") != generation
                or source.get("phase") != "runtime_bound"
                or source.get("settled_at") is not None
                or source.get("cancel_requested_at") is not None
                or source.get("operation_kind") != "create"
                or marker
                != {
                    "generation": generation,
                    "mode": "create",
                    "attempted": True,
                    "replaces_uid": None,
                }
                or workspace.get("_runtime_incarnation") != uids[0]
                or source.get("runtime_incarnation") != uids[0]
                or workspace.get("_creation_reservation_id") != sid
                or type(token) is not int
                or token <= 0
                or workspace.get("_creation_claim_token") != str(token)
                or (
                    "protected_cloud" in metadata
                    and metadata["protected_cloud"] is not False
                )
            ):
                return None
            namespace = workspace["namespace"]
            source_keys = (
                "id",
                "thread_runtime_generation",
                "reservation_generation",
                "claim_token",
                "claimed_by",
                "operation_kind",
                "desired_manifest_digest",
                "lifecycle_fingerprint",
                "phase",
                "settled_at",
                "cancel_requested_at",
            )
        else:
            return None
        if not isinstance(namespace, str) or not namespace:
            return None
        return SessionCreationCandidate(
            cursor=SessionCreationCursor(row["created_at"], tid, sid),
            lane=lane,
            runtime_generation=generation,
            agent_id=agent,
            attach_token=attach,
            claim_token=token,
            namespace=namespace,
            pod_uid=uids[0],
            pvc_uid=uids[1],
            seed_configmap_uid=uids[2],
            service_uid=uids[3],
            owner_fingerprint=_digest(
                {"workspace": workspace, "config": config, "status": owner["status"]}
            ),
            source_fingerprint=_digest({k: source.get(k) for k in source_keys}),
        )
    except (KeyError, TypeError, ValueError):
        return None
