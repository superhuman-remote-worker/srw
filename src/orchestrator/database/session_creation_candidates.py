"""Read-only hints for continuing already-issued Session workspace creates.

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
    route: str
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
# hiding later work. The retained branch checks the immediately preceding
# create plus cleanup/process-zero; any pinned or restore history remains held.
# Existing partial open-source indexes bound the scan, and the concurrent 0294
# owner-history index bounds the per-owner lineage lookups.
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
       ) AS initial_source,
       (SELECT jsonb_agg(to_jsonb(p)) FROM (
           SELECT p.* FROM managed_repository_workspace_creation_reservations p
           WHERE p.owner_kind='thread' AND p.owner_id=s.thread_id
             AND p.scope='workspace_container' AND p.id<>s.source_id
             AND p.reservation_generation < (s.source->>'reservation_generation')::bigint
           ORDER BY p.reservation_generation DESC LIMIT 1
       ) p) AS retained_predecessors,
       (SELECT jsonb_agg(to_jsonb(c)) FROM (
           SELECT c.* FROM managed_repository_workspace_cleanup_intents c
           WHERE c.owner_kind='thread' AND c.owner_id=s.thread_id
             AND c.scope='workspace_container'
             AND c.thread_runtime_generation=(
                 SELECT p.thread_runtime_generation
                 FROM managed_repository_workspace_creation_reservations p
                 WHERE p.owner_kind='thread' AND p.owner_id=s.thread_id
                   AND p.scope='workspace_container' AND p.id<>s.source_id
                   AND p.reservation_generation < (s.source->>'reservation_generation')::bigint
                 ORDER BY p.reservation_generation DESC LIMIT 1)
           ORDER BY c.intent_generation DESC LIMIT 2
       ) c) AS retained_cleanups,
       (SELECT jsonb_agg(to_jsonb(z)) FROM (
           SELECT z.* FROM managed_repository_process_zero_receipts z
           JOIN managed_repository_workspace_creation_reservations p
             ON p.owner_kind='thread' AND p.owner_id=s.thread_id
             AND p.scope='workspace_container' AND p.id<>s.source_id
             AND p.id=(
                 SELECT q.id FROM managed_repository_workspace_creation_reservations q
                 WHERE q.owner_kind='thread' AND q.owner_id=s.thread_id
                   AND q.scope='workspace_container' AND q.id<>s.source_id
                   AND q.reservation_generation < (s.source->>'reservation_generation')::bigint
                 ORDER BY q.reservation_generation DESC LIMIT 1)
             AND p.runtime_incarnation::text=z.runtime_incarnation
           WHERE z.owner_kind='thread' AND z.owner_id=s.thread_id
             AND z.scope IN ('workspace_container','stateless_workspace')
             AND z.provisioner='k8s'
           ORDER BY z.observed_at DESC LIMIT 1
       ) z) AS retained_process_zeroes,
       EXISTS (
           SELECT 1 FROM managed_repository_workspace_creation_reservations h
           WHERE h.owner_kind='thread' AND h.owner_id=s.thread_id
             AND h.scope='workspace_container' AND h.id<>s.source_id
             AND (h.settled_at IS NULL OR h.reservation_generation >
                  (s.source->>'reservation_generation')::bigint)
       ) OR EXISTS (
           SELECT 1 FROM managed_repository_workspace_cleanup_intents h
           WHERE h.owner_kind='thread' AND h.owner_id=s.thread_id
             AND h.scope='workspace_container' AND h.settled_at IS NULL
       ) OR EXISTS (
           SELECT 1 FROM thread_workspace_provision_intents h
           WHERE h.thread_id=s.thread_id
       ) OR EXISTS (
           SELECT 1 FROM managed_repository_workspace_creation_reservations h
           WHERE h.owner_kind='thread' AND h.owner_id=s.thread_id
             AND h.scope='workspace_container' AND h.operation_kind<>'create'
       ) OR EXISTS (
           SELECT 1 FROM managed_repository_workspace_cleanup_intents h
           WHERE h.owner_kind='thread' AND h.owner_id=s.thread_id
             AND h.scope='workspace_container'
             AND (h.target_disposition<>'deleted' OR h.snapshot_restore_required)
       ) AS conflicting_authority
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


def _one_object(value: Any) -> dict:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, list) or len(value) != 1:
        raise ValueError("missing or ambiguous retained lineage")
    return _object(value[0])


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("missing physical clock")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("unqualified physical clock")
    return parsed


def _retained_create_lineage(
    row: Any, metadata: dict, source: dict, generation: str
) -> dict:
    """Admit only the completed End whose exact PVC this v1 create retained."""
    if row["initial_source"] is not False or row["conflicting_authority"] is not False:
        raise ValueError("retained authority is not exclusive")
    binding = _object(metadata.get("_workspace_binding"))
    workspace = _object(metadata["workspace_container"])
    predecessor = _one_object(row["retained_predecessors"])
    cleanup = _one_object(row["retained_cleanups"])
    process_zero = _one_object(row["retained_process_zeroes"])
    thread_id = _uuid(str(row["thread_id"]))
    binding_generation = _uuid(binding.get("generation"))
    predecessor_generation = _uuid(predecessor.get("thread_runtime_generation"))
    predecessor_pod = _uuid(predecessor.get("pod_uid"))
    pvc_uid = _uuid(source.get("pvc_uid"))
    namespace = workspace.get("namespace")
    if (
        source.get("startup_protocol_version") != 1
        or source.get("owner_kind") != "thread"
        or source.get("owner_id") != thread_id
        or source.get("scope") != "workspace_container"
        or not isinstance(namespace, str)
        or not namespace
        or binding.get("kind") != "remote"
        or binding.get("backing_id") != f"k8s-pvc:{namespace}:{pvc_uid}"
        or binding_generation == generation
        or binding.get("runtime_incarnation") is not None
        or not isinstance(binding.get("ssh_host_key_fingerprint"), str)
        or not binding["ssh_host_key_fingerprint"]
        or not {"kind", "backing_id", "generation", "ssh_host_key_fingerprint"}
        <= set(binding)
        or bool(
            set(binding)
            - {
                "kind",
                "backing_id",
                "generation",
                "runtime_incarnation",
                "ssh_host_key_fingerprint",
            }
        )
        or predecessor_generation == generation
        or predecessor.get("operation_kind") != "create"
        or predecessor.get("owner_kind") != "thread"
        or predecessor.get("owner_id") != thread_id
        or predecessor.get("scope") != "workspace_container"
        or predecessor.get("phase") != "settled"
        or predecessor.get("result_kind") != "settled"
        or predecessor.get("settled_at") is None
        or predecessor.get("cancel_requested_at") is not None
        or predecessor.get("startup_protocol_version") not in (None, 1)
        or (
            predecessor.get("startup_protocol_version") == 1
            and (
                predecessor.get("startup_stage") != "readiness"
                or predecessor.get("startup_state") != "starting"
                or predecessor.get("startup_reason_code") != "scheduled"
                or predecessor.get("startup_attention_at") is not None
                or not _timestamp(predecessor.get("scheduled_at"))
                <= _timestamp(predecessor.get("startup_first_ready_at"))
                <= _timestamp(predecessor.get("settled_at"))
            )
        )
        or predecessor.get("thread_runtime_generation") != predecessor_generation
        or predecessor.get("runtime_incarnation") != predecessor_pod
        or predecessor.get("pvc_uid") != pvc_uid
        or predecessor.get("service_uid") is None
        or cleanup.get("owner_kind") != "thread"
        or cleanup.get("owner_id") != thread_id
        or cleanup.get("scope") != "workspace_container"
        or cleanup.get("thread_runtime_generation") != predecessor_generation
        or cleanup.get("runtime_incarnation") != predecessor_pod
        or cleanup.get("pod_uid") != predecessor_pod
        or cleanup.get("pvc_uid") != pvc_uid
        or cleanup.get("service_uid") != predecessor.get("service_uid")
        or (
            cleanup.get("seed_configmap_uid") is not None
            and cleanup.get("seed_configmap_uid")
            != predecessor.get("seed_configmap_uid")
        )
        or _object(cleanup.get("resource_location")).get("namespace") != namespace
        or cleanup.get("target_disposition") != "deleted"
        or cleanup.get("resource_policy") != "preserve"
        or _object(cleanup.get("lifecycle_fingerprint")).get("runtime_status")
        != "ready"
        or cleanup.get("reclaim_shared_resources") is not False
        or cleanup.get("snapshot_restore_required") is not False
        or cleanup.get("capture_complete") is not True
        or cleanup.get("phase") != "settled"
        or cleanup.get("result_kind") != "settled"
        or any(
            cleanup.get(k) is None
            for k in ("resources_captured_at", "cleanup_completed_at", "settled_at")
        )
        or process_zero.get("runtime_incarnation") != predecessor_pod
        or process_zero.get("owner_kind") != "thread"
        or process_zero.get("owner_id") != thread_id
        or process_zero.get("scope")
        not in {"workspace_container", "stateless_workspace"}
        or process_zero.get("provisioner") != "k8s"
    ):
        raise ValueError("retained predecessor is not complete and exact")
    return {
        "predecessor": predecessor,
        "cleanup": cleanup,
        "process_zero": process_zero,
        "binding": binding,
        "conflicting_authority": row["conflicting_authority"],
    }


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
        retained = lane == "stateless" and row["initial_source"] is False
        lineage = (
            _retained_create_lineage(row, metadata, source, generation)
            if retained
            else None
        )
        if (
            (not retained and row["initial_source"] is not True)
            or owner.get("status") not in {"created", "active"}
            or owner.get("execution_lane") != lane
            or owner.get("runtime_retirement_token") is not None
            or (agent is None) != (attach is None)
            or _object(config.get("workspace")).get("backend") != "sandbox"
            or workspace.get("provisioner") != "k8s"
            or workspace.get("status")
            not in {"pending", "created", "creating", "failed"}
            or workspace.get("_snapshot_restore_required", False) is not False
            or metadata.get("vm") not in (None, {})
            or (not retained and metadata.get("_workspace_binding") not in (None, {}))
            or (retained and "_stateless_workspace_retirement_settled" in metadata)
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
            route="retained_create" if retained else "initial",
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
                {
                    "metadata": metadata,
                    "status": owner["status"],
                    "generation": generation,
                    "agent": agent,
                    "attach": attach,
                }
                if retained
                else {
                    "workspace": workspace,
                    "config": config,
                    "status": owner["status"],
                }
            ),
            source_fingerprint=_digest(
                {"source": source, "lineage": lineage}
                if retained
                else {k: source.get(k) for k in source_keys}
            ),
        )
    except (KeyError, TypeError, ValueError):
        return None
