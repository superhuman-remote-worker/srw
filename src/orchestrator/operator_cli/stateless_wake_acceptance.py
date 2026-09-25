"""Bounded k3d acceptance gate for stateless durable wake execution.

This operator is intentionally inert unless both ``--execute`` (or
``--cleanup-only``) and the exact confirmation phrase are supplied.  It is
designed to run inside one orchestrator container, where it can use the same
database helpers and Kubernetes service account as production without ever
printing a token, credential, repository coordinate, message body, or DSN.

The host-side wrapper at ``scripts/stateless-durable-wake-k3d-gate.sh`` binds
execution to context ``k3d-srw`` and namespace ``srw``.  Direct module use must
also set ``SRW_WAKE_GATE_CONTEXT=k3d-srw``; this is a second guard, not a
substitute for the wrapper's real kube-context check.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID


EXPECTED_CONTEXT = "k3d-srw"
EXPECTED_NAMESPACE = "srw"
CONFIRMATION = "k3d-srw-disposable-stateless-wake"
GATE_KIND = "stateless_durable_wake_k3d"
RUN_ID_RE = re.compile(r"[a-z0-9][a-z0-9-]{7,47}\Z")
TERMINAL_DELIVERY_STATES = frozenset({"admitted", "settled"})


class GateError(RuntimeError):
    """A safe, pre-classified acceptance failure."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass
class GateState:
    run_id: str
    owner_user_id: str | None = None
    thread_id: str | None = None
    job_ids: list[str] = field(default_factory=list)
    deleted_pods: list[dict[str, str]] = field(default_factory=list)


def _canonical_uuid(value: str) -> str:
    text = str(value).strip()
    try:
        parsed = UUID(text)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("must be a canonical UUID") from exc
    if text.lower() != str(parsed):
        raise argparse.ArgumentTypeError("must be a canonical UUID")
    return str(parsed)


def _run_id(value: str) -> str:
    text = str(value).strip()
    if not RUN_ID_RE.fullmatch(text):
        raise argparse.ArgumentTypeError(
            "must be 8-48 lowercase letters, digits, or hyphens"
        )
    return text


def _positive_seconds(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not 30.0 <= parsed <= 1800.0:
        raise argparse.ArgumentTypeError("must be between 30 and 1800 seconds")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect by default; explicitly execute or clean one disposable "
            "stateless durable-wake k3d fixture."
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--execute",
        action="store_true",
        help="run the bounded acceptance fixture after strict preflight",
    )
    mode.add_argument(
        "--cleanup-only",
        action="store_true",
        help="clean the exact fixture selected by --run-id",
    )
    parser.add_argument("--run-id", type=_run_id)
    parser.add_argument("--owner-user-id", type=_canonical_uuid)
    parser.add_argument("--config-name", default="session_base")
    parser.add_argument("--timeout-seconds", type=_positive_seconds, default=600.0)
    parser.add_argument("--confirm")
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    mutating = args.execute or args.cleanup_only
    if mutating and args.confirm != CONFIRMATION:
        parser.error(f"mutation requires --confirm {CONFIRMATION}")
    if mutating and not args.run_id:
        parser.error("mutation requires --run-id")
    if args.execute and not args.owner_user_id:
        parser.error("--execute requires --owner-user-id")
    if not mutating and args.confirm is not None:
        parser.error("--confirm is valid only with a mutating mode")
    if not mutating and args.run_id is not None:
        parser.error("--run-id is valid only with a mutating mode")
    if not mutating and args.owner_user_id is not None:
        parser.error("--owner-user-id is valid only with --execute")
    if not str(args.config_name or "").strip():
        parser.error("--config-name must be non-empty")
    return args


def _emit(event: str, **values: Any) -> None:
    """Emit one secret-free, machine-readable observation."""

    print(
        json.dumps(
            {"event": event, **values},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ),
        flush=True,
    )


def _marker(run_id: str) -> dict[str, str]:
    return {"kind": GATE_KIND, "run_id": run_id}


def _row_dict(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _safe_delivery(row: Mapping[str, Any]) -> dict[str, Any]:
    """Project only non-content execution authority for operator output."""

    return {
        "delivery_id": str(row.get("delivery_id") or "") or None,
        "state": str(row.get("state") or "") or None,
        "execution_lane": str(row.get("execution_lane") or "") or None,
        "claim_generation": int(row.get("claim_generation") or 0),
        "lease_token": (
            int(row["owner_run_queue_lease_token"])
            if row.get("owner_run_queue_lease_token") is not None
            else None
        ),
        "executor": str(row.get("owner_executor") or "") or None,
        "executor_pod_uid": str(row.get("owner_executor_pod_uid") or "") or None,
        "admitted_turn_number": (
            int(row["admitted_turn_number"])
            if row.get("admitted_turn_number") is not None
            else None
        ),
    }


async def _wait_for(
    probe: Callable[[], Awaitable[Any]],
    predicate: Callable[[Any], bool],
    *,
    timeout: float,
    label: str,
    interval: float = 0.2,
) -> Any:
    deadline = asyncio.get_running_loop().time() + timeout
    last: Any = None
    while asyncio.get_running_loop().time() < deadline:
        last = await probe()
        if predicate(last):
            return last
        await asyncio.sleep(interval)
    raise GateError("timeout", f"timed out waiting for {label}")


class StatelessWakeGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.state = GateState(
            run_id=str(args.run_id or "inspect"),
            owner_user_id=args.owner_user_id,
        )
        self.resources: Any = None
        self.session_wake: Any = None
        self.db: Any = None
        self.provisioner: Any = None
        self.namespace = ""

    async def connect(self) -> None:
        # The supported composition boundary for a process that needs the
        # application's operations without serving HTTP: build one
        # application's resources (no connection, no task) and reach each
        # operation through the composition function that owns it.
        from orchestrator.application import build_application_resources
        from orchestrator.services import agent_provisioner as agent_provisioner_module
        from orchestrator.services import session_wake

        self.resources = build_application_resources()
        self.session_wake = session_wake
        self.db = self.resources.postgres_db
        await self.db.connect()
        self.provisioner = agent_provisioner_module.agent_provisioner
        self.provisioner.connect(self.db)
        self.namespace = str(getattr(self.provisioner, "_namespace", "") or "")

    async def close(self) -> None:
        if self.db is not None:
            await self.db.close()

    async def inspect(
        self, *, require_owner: bool, require_capacity: bool = True
    ) -> dict[str, Any]:
        if self.namespace != EXPECTED_NAMESPACE:
            raise GateError(
                "wrong_namespace",
                "orchestrator agent namespace is not the disposable k3d namespace",
            )
        if not bool(getattr(self.provisioner, "is_available", False)):
            raise GateError("kubernetes_unavailable", "Kubernetes client unavailable")

        owner = None
        async with self.db.acquire() as conn:
            migrations = await conn.fetch(
                "SELECT filename, success FROM schema_migrations "
                "WHERE filename = ANY($1::text[]) ORDER BY filename",
                [
                    "0191_stateless_input_deliveries.sql",
                    "0192_stateless_input_delivery_validate.sql",
                    "0197_non_pinned_workspace_process_zero.sql",
                    "0198_non_pinned_workspace_lifecycle_authority.sql",
                ],
            )
            dirty_migrations = await conn.fetchval(
                "SELECT count(*) FROM schema_migrations WHERE success = FALSE"
            )
            constraints = await conn.fetch(
                "SELECT conname, convalidated FROM pg_constraint "
                "WHERE conname = ANY($1::text[]) ORDER BY conname",
                [
                    "thread_input_deliveries_lane_check",
                    "thread_input_deliveries_owner_shape",
                    "thread_input_deliveries_claim_shape",
                ],
            )
            triggers = await conn.fetchval(
                "SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal "
                "AND tgname = ANY($1::text[])",
                [
                    "trg_input_delivery_lane_authority",
                    "trg_thread_lane_without_pending_input",
                    "trg_stateless_input_delivery_claim",
                ],
            )
            auto_pull = await conn.fetchrow(
                "SELECT "
                "(SELECT count(*) FROM project_officers "
                " WHERE config_override #> '{officer,auto_pull}' = 'true'::jsonb) "
                "AS post_enabled, "
                "(SELECT count(*) FROM threads "
                " WHERE metadata #> '{config_override,officer,auto_pull}' "
                "       = 'true'::jsonb) AS thread_enabled"
            )
            active_wakes = await conn.fetchval(
                "SELECT count(*) FROM jobs WHERE wake_on_complete "
                "AND wake_state IN ('pending','sending')"
            )
            if self.args.owner_user_id:
                owner = await conn.fetchrow(
                    "SELECT id, is_approved FROM users WHERE id=$1::uuid",
                    self.args.owner_user_id,
                )

        migration_map = {
            str(row["filename"]): bool(row["success"]) for row in migrations
        }
        expected_migrations = {
            "0191_stateless_input_deliveries.sql": True,
            "0192_stateless_input_delivery_validate.sql": True,
            "0197_non_pinned_workspace_process_zero.sql": True,
            "0198_non_pinned_workspace_lifecycle_authority.sql": True,
        }
        if migration_map != expected_migrations:
            raise GateError(
                "migration_unavailable", "0191/0192/0197/0198 are not cleanly applied"
            )
        if int(dirty_migrations or 0) != 0:
            raise GateError("dirty_migration", "a failed migration row is present")
        if len(constraints) != 3 or not all(row["convalidated"] for row in constraints):
            raise GateError(
                "constraint_unvalidated", "delivery constraints are not valid"
            )
        if int(triggers or 0) != 3:
            raise GateError("trigger_unavailable", "delivery trigger set is incomplete")
        if int(auto_pull["post_enabled"] or 0) or int(auto_pull["thread_enabled"] or 0):
            raise GateError(
                "auto_pull_enabled", "auto-pull must be disabled fleet-wide"
            )
        if require_owner and (owner is None or owner["is_approved"] is not True):
            raise GateError(
                "owner_unavailable", "fixture owner must exist and be approved"
            )

        pods = await self._stateless_pods()
        ready_pods = [pod for pod in pods if pod["ready"]]
        if require_capacity and len(ready_pods) < 2:
            raise GateError(
                "insufficient_stateless_capacity",
                "gate requires at least two Ready stateless executors",
            )
        result = {
            "namespace": self.namespace,
            "migrations": migration_map,
            "constraints_validated": len(constraints),
            "delivery_triggers": int(triggers or 0),
            "auto_pull_posts": int(auto_pull["post_enabled"] or 0),
            "auto_pull_threads": int(auto_pull["thread_enabled"] or 0),
            "preexisting_active_wakes": int(active_wakes or 0),
            "stateless_pods": len(pods),
            "ready_stateless_pods": len(ready_pods),
            "owner_approved": bool(owner and owner["is_approved"]),
        }
        _emit("preflight", status="pass", **result)
        return result

    async def _stateless_pods(self) -> list[dict[str, Any]]:
        api = getattr(self.provisioner, "_core_api", None)
        if api is None:
            return []
        response = await asyncio.to_thread(
            api.list_namespaced_pod,
            namespace=self.namespace,
            label_selector="app.kubernetes.io/component=agent-stateless",
        )
        result: list[dict[str, Any]] = []
        for pod in getattr(response, "items", None) or []:
            metadata = getattr(pod, "metadata", None)
            status = getattr(pod, "status", None)
            conditions = getattr(status, "conditions", None) or []
            ready = any(
                str(getattr(condition, "type", "")) == "Ready"
                and str(getattr(condition, "status", "")) == "True"
                for condition in conditions
            )
            result.append(
                {
                    "name": str(getattr(metadata, "name", "") or ""),
                    "uid": str(getattr(metadata, "uid", "") or ""),
                    "phase": str(getattr(status, "phase", "") or ""),
                    "ready": ready,
                }
            )
        return result

    async def _pod(self, name: str) -> dict[str, Any] | None:
        api = getattr(self.provisioner, "_core_api", None)
        if api is None:
            return None
        try:
            pod = await asyncio.to_thread(
                api.read_namespaced_pod,
                name=name,
                namespace=self.namespace,
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return None
            raise GateError(
                "pod_read_failed", "exact executor pod read failed"
            ) from None
        metadata = getattr(pod, "metadata", None)
        return {
            "name": str(getattr(metadata, "name", "") or ""),
            "uid": str(getattr(metadata, "uid", "") or ""),
        }

    async def _delete_exact_executor(self, name: str, uid: str) -> None:
        async with self.db.acquire() as conn:
            foreign = await conn.fetchval(
                "SELECT count(*) FROM run_queue "
                "WHERE state IN ('queued','leased') "
                "AND leased_by=$1 AND unit_id<>$2::uuid",
                name,
                self.state.thread_id,
            )
            fleet_work = await conn.fetchval(
                "SELECT count(*) FROM run_queue "
                "WHERE unit_id<>$1::uuid AND state IN ('queued','leased')",
                self.state.thread_id,
            )
        if int(foreign or 0) != 0:
            raise GateError(
                "executor_not_disposable",
                "selected executor owns non-fixture work",
            )
        if int(fleet_work or 0) != 0:
            raise GateError(
                "stateless_pool_not_quiet",
                "non-fixture queue work appeared during fault injection",
            )
        observed = await self._pod(name)
        if observed is None or observed["uid"] != uid:
            raise GateError("pod_authority_changed", "executor Pod UID changed")
        api = getattr(self.provisioner, "_core_api", None)
        try:
            await asyncio.to_thread(
                api.delete_namespaced_pod,
                name=name,
                namespace=self.namespace,
                grace_period_seconds=0,
                propagation_policy="Background",
                body={"preconditions": {"uid": uid}},
            )
        except Exception:
            raise GateError(
                "pod_delete_failed", "exact executor fault injection failed"
            ) from None
        self.state.deleted_pods.append({"name": name, "uid": uid})
        _emit("executor_deleted", pod=name, pod_uid=uid)

    async def _thread_snapshot(self) -> dict[str, Any]:
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT thread.id, thread.status::text AS status, "
                "thread.execution_lane, thread.agent_id, thread.total_turns, "
                "queue.state AS queue_state, queue.lease_token, "
                "queue.leased_by, queue.last_leased_by, queue.input_seq, "
                "queue.consumed_seq FROM threads AS thread "
                "LEFT JOIN run_queue AS queue ON queue.unit_id=thread.id "
                "WHERE thread.id=$1::uuid",
                self.state.thread_id,
            )
        return _row_dict(row)

    async def _delivery(self, delivery_id: str) -> dict[str, Any]:
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT delivery.*, message.seq, message.role, "
                "message.turn_number FROM thread_input_deliveries AS delivery "
                "JOIN thread_messages AS message ON message.id=delivery.message_id "
                "WHERE delivery.delivery_id=$1::uuid",
                delivery_id,
            )
        return _row_dict(row)

    async def _job_outbox(self, job_id: str) -> dict[str, Any]:
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, status::text AS status, wake_state, wake_attempts, "
                "wake_delivery_id, wake_delivery_claim_attempt, "
                "wake_notified_status FROM jobs WHERE id=$1::uuid",
                job_id,
            )
        return _row_dict(row)

    async def _turn_rows(self, turn_number: int) -> dict[str, Any]:
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT count(*) FILTER (WHERE role='human') AS human_rows, "
                "count(*) FILTER (WHERE role='event') AS event_rows, "
                "count(*) FILTER (WHERE role='ai') AS ai_rows, "
                "min(seq) FILTER (WHERE role='ai') AS first_ai_seq "
                "FROM thread_messages WHERE thread_id=$1::uuid "
                "AND turn_number=$2 AND rewound_at IS NULL",
                self.state.thread_id,
                int(turn_number),
            )
        return _row_dict(row)

    async def _wait_turn(self, turn_number: int) -> dict[str, Any]:
        async def probe() -> dict[str, Any]:
            rows = await self._turn_rows(turn_number)
            rows["thread"] = await self._thread_snapshot()
            return rows

        result = await _wait_for(
            probe,
            lambda value: int(value.get("ai_rows") or 0) >= 1
            and value.get("thread", {}).get("queue_state") == "done",
            timeout=self.args.timeout_seconds,
            label=f"turn {turn_number} settlement",
        )
        return result

    async def _wait_delivery(self, delivery_id: str) -> dict[str, Any]:
        return await _wait_for(
            lambda: self._delivery(delivery_id),
            lambda row: str(row.get("state") or "") == "settled",
            timeout=self.args.timeout_seconds,
            label="durable event settlement",
        )

    async def _create_job(self, label: str) -> str:
        row = await self.db.create_job(
            description=(
                f"Acceptance gate {self.state.run_id} {label}. "
                "No investigation or tool use is needed; acknowledge briefly."
            ),
            config_name="worker_base",
            context={"acceptance_gate": _marker(self.state.run_id), "phase": label},
            user_id=self.state.owner_user_id,
            origin="bench",
            status="paused",
            freeze_data={"reason": "acceptance_gate_fixture"},
            created_by_thread_id=self.state.thread_id,
            wake_on_complete=True,
            execution_lane="pinned",
        )
        job_id = str(row["id"])
        self.state.job_ids.append(job_id)
        return job_id

    async def _claim_job(self, job_id: str) -> tuple[dict[str, Any], str]:
        delivery_id = self.session_wake._job_wake_delivery_id(
            {"id": job_id, "status": "completed"}
        )
        async with self.db.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT status::text AS status, wake_state, wake_attempts "
                    "FROM jobs WHERE id=$1::uuid FOR UPDATE",
                    job_id,
                )
                if row is None:
                    raise GateError("fixture_missing", "fixture job disappeared")
                if str(row["wake_state"]) not in {"none", "pending", "sending"}:
                    raise GateError(
                        "wake_claim_conflict", "fixture wake is not claimable"
                    )
                attempts = int(row["wake_attempts"] or 0) + (
                    0 if str(row["wake_state"]) == "sending" else 1
                )
                await conn.execute(
                    "UPDATE jobs SET status='completed', wake_state='sending', "
                    "wake_claimed_at=now(), wake_attempts=$2, "
                    "wake_delivery_id=$3::uuid, "
                    "wake_delivery_claim_attempt=$2, updated_at=now() "
                    "WHERE id=$1::uuid",
                    job_id,
                    attempts,
                    delivery_id,
                )
        wake = await self.db.get_job(job_id)
        if wake is None:
            raise GateError("fixture_missing", "fixture job disappeared after claim")
        return wake, delivery_id

    async def _settle_outbox(self, job_id: str) -> None:
        current = await self._job_outbox(job_id)
        if current.get("wake_state") == "sent":
            return
        wake, _delivery_id = await self._claim_job(job_id)
        settled = await self.session_wake._deliver_and_settle(self.db, wake)
        if settled is not True:
            raise GateError("outbox_not_sent", "terminal replay did not settle outbox")
        row = await self._job_outbox(job_id)
        if row.get("wake_state") != "sent":
            raise GateError("outbox_not_sent", "job wake did not reach sent")

    async def _persist_wake(self, label: str) -> tuple[str, str, dict[str, Any]]:
        job_id = await self._create_job(label)
        wake, delivery_id = await self._claim_job(job_id)
        delivered = await self.session_wake._deliver_and_settle(self.db, wake)
        if delivered is True:
            raise GateError(
                "premature_execution",
                "new stateless wake reported executed before queue admission",
            )
        delivery = await self._delivery(delivery_id)
        if not delivery or delivery.get("role") != "event":
            raise GateError(
                "event_not_persisted", "truthful event row was not persisted"
            )
        outbox = await self._job_outbox(job_id)
        if outbox.get("wake_state") not in {"pending", "sending"}:
            raise GateError(
                "outbox_settled_early",
                "outbox settled before provider admission",
            )
        return job_id, delivery_id, delivery

    async def _input(self, label: str) -> tuple[int, int]:
        thread = await self.db.get_thread(str(self.state.thread_id))
        if thread is None:
            raise GateError("fixture_missing", "fixture thread disappeared")
        from orchestrator.application import transport as transport_composition
        from orchestrator.services import stateless_input_admission

        result = await stateless_input_admission.admit_stateless_input(
            thread,
            f"Reply exactly GATE-{self.state.run_id}-{label}; do not use tools.",
            dependencies=transport_composition.stateless_input_dependencies(
                self.resources
            ),
        )
        return int(result["turn_id"]), int(result["queue"]["input_seq"])

    async def _wait_lease_before_admission(
        self, delivery_id: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        async def probe() -> tuple[dict[str, Any], dict[str, Any]]:
            return await self._thread_snapshot(), await self._delivery(delivery_id)

        def claimed(value: tuple[dict[str, Any], dict[str, Any]]) -> bool:
            thread, delivery = value
            return bool(
                thread.get("queue_state") == "leased"
                and thread.get("leased_by")
                and delivery.get("state") not in TERMINAL_DELIVERY_STATES
            )

        return await _wait_for(
            probe,
            claimed,
            timeout=min(30.0, self.args.timeout_seconds),
            label="pre-admission stateless lease",
            interval=0.02,
        )

    async def run(self) -> dict[str, Any]:
        await self.inspect(require_owner=True)
        existing = await self._fixture_ids()
        if existing["threads"] or existing["jobs"]:
            raise GateError(
                "fixture_exists",
                "run-id already has residue; use --cleanup-only first",
            )

        self.state.thread_id = await self.db.create_thread(
            user_id=self.state.owner_user_id,
            config_name=str(self.args.config_name),
            permission_mode="autonomous",
            narration_mode="minimal",
            title=f"SRW stateless wake gate {self.state.run_id}",
            execution_lane="stateless",
            initial_metadata={
                "config_override": {
                    "workspace": {"backend": "none"},
                    "officer": {"enabled": False, "conference": False},
                },
                "acceptance_gate": _marker(self.state.run_id),
            },
        )
        _emit(
            "fixture_created", run_id=self.state.run_id, thread_id=self.state.thread_id
        )

        # Warm one real attached session, then prove a human row ordered before
        # an event is consumed first and that the event stays on the warm pod.
        warm_turn, _warm_seq = await self._input("WARM")
        await self._wait_turn(warm_turn)
        warm_queue = await self._thread_snapshot()
        warm_pod = str(warm_queue.get("last_leased_by") or "")
        if not warm_pod:
            raise GateError("warm_owner_missing", "warm turn has no executor identity")
        warm_identity = await self._pod(warm_pod)
        if warm_identity is None:
            raise GateError("warm_owner_missing", "warm executor Pod is absent")

        ordered_turn, ordered_human_seq = await self._input("ORDER")
        ordered_job, ordered_delivery_id, ordered_delivery = await self._persist_wake(
            "ordered-warm-event"
        )
        if int(ordered_delivery["seq"]) <= ordered_human_seq:
            raise GateError(
                "fifo_violation", "event transcript order precedes human input"
            )
        ordered_done = await self._wait_delivery(ordered_delivery_id)
        await self._wait_turn(ordered_turn)
        event_turn = int(ordered_done["admitted_turn_number"])
        event_rows = await self._wait_turn(event_turn)
        human_rows = await self._turn_rows(ordered_turn)
        if int(human_rows.get("first_ai_seq") or 0) <= 0 or int(
            event_rows.get("first_ai_seq") or 0
        ) <= int(human_rows.get("first_ai_seq") or 0):
            raise GateError(
                "fifo_violation", "event provider result preceded human result"
            )
        if str(ordered_done.get("owner_executor") or "") != warm_pod:
            raise GateError("warm_affinity_lost", "event did not use the warm executor")
        await self._settle_outbox(ordered_job)
        _emit(
            "warm_fifo",
            status="pass",
            warm_executor=warm_pod,
            human_turn=ordered_turn,
            event_turn=event_turn,
            delivery=_safe_delivery(ordered_done),
        )

        # Remove the idle warm process and prove the next event is restored and
        # executed exactly once by a process with no cached copy of this thread.
        queue = await self._thread_snapshot()
        if queue.get("queue_state") != "done":
            raise GateError("queue_not_idle", "warm queue is not idle before recycle")
        await self._delete_exact_executor(warm_pod, str(warm_identity["uid"]))
        await _wait_for(
            lambda: self._pod(warm_pod),
            lambda pod: pod is None
            or str(pod.get("uid") or "") != warm_identity["uid"],
            timeout=self.args.timeout_seconds,
            label="warm executor replacement",
        )
        fresh_job, fresh_delivery_id, _ = await self._persist_wake("fresh-attach")
        fresh_done = await self._wait_delivery(fresh_delivery_id)
        if str(fresh_done.get("owner_executor") or "") == warm_pod and str(
            fresh_done.get("owner_executor_pod_uid") or ""
        ) == str(warm_identity["uid"]):
            raise GateError(
                "fresh_attach_not_proven", "deleted warm runtime executed wake"
            )
        await self._wait_turn(int(fresh_done["admitted_turn_number"]))
        await self._settle_outbox(fresh_job)
        _emit("fresh_attach", status="pass", delivery=_safe_delivery(fresh_done))

        # Claim one more event, delete exactly that claimant before provider
        # admission, and require a higher queue lease plus a different Pod UID
        # to settle the same delivery identity.
        # First remove the now-idle fresh executor.  That forces the handoff
        # case through an uncached attach path and gives the harness a bounded
        # window in which to observe the lease before provider admission.
        fresh_executor = str(fresh_done.get("owner_executor") or "")
        fresh_executor_uid = str(fresh_done.get("owner_executor_pod_uid") or "")
        if not fresh_executor or not fresh_executor_uid:
            raise GateError("fresh_owner_missing", "fresh delivery has no executor")
        await self._delete_exact_executor(fresh_executor, fresh_executor_uid)
        await _wait_for(
            lambda: self._pod(fresh_executor),
            lambda pod: pod is None or str(pod.get("uid") or "") != fresh_executor_uid,
            timeout=self.args.timeout_seconds,
            label="fresh executor replacement",
        )
        handoff_job, handoff_delivery_id, _ = await self._persist_wake("lease-handoff")
        leased, pre_handoff = await self._wait_lease_before_admission(
            handoff_delivery_id
        )
        leased_pod = str(leased["leased_by"])
        leased_identity = await self._pod(leased_pod)
        if leased_identity is None:
            raise GateError("lease_owner_missing", "leased executor Pod is absent")
        old_token = int(leased["lease_token"] or 0)
        old_claim_generation = int(pre_handoff.get("claim_generation") or 0)
        await self._delete_exact_executor(leased_pod, str(leased_identity["uid"]))
        handoff_done = await self._wait_delivery(handoff_delivery_id)
        final_queue = await self._thread_snapshot()
        if int(handoff_done.get("owner_run_queue_lease_token") or 0) <= old_token:
            raise GateError("lease_not_advanced", "successor did not use a newer lease")
        if str(handoff_done.get("owner_executor_pod_uid") or "") == str(
            leased_identity["uid"]
        ):
            raise GateError(
                "stale_executor_settled", "deleted claimant settled delivery"
            )
        if int(handoff_done.get("claim_generation") or 0) <= old_claim_generation:
            raise GateError(
                "claim_not_advanced", "delivery claim generation did not advance"
            )
        await self._wait_turn(int(handoff_done["admitted_turn_number"]))
        if final_queue.get("queue_state") != "done":
            final_queue = await _wait_for(
                self._thread_snapshot,
                lambda row: row.get("queue_state") == "done",
                timeout=self.args.timeout_seconds,
                label="handoff queue settlement",
            )
        await self._settle_outbox(handoff_job)
        _emit(
            "lease_handoff",
            status="pass",
            predecessor=leased_pod,
            predecessor_pod_uid=leased_identity["uid"],
            predecessor_lease_token=old_token,
            successor_lease_token=int(
                handoff_done.get("owner_run_queue_lease_token") or 0
            ),
            delivery=_safe_delivery(handoff_done),
        )

        # Simulate a committed delivery with a lost response: keep the exact
        # job outbox in sending, let the stateless execution settle, change the
        # thread lane, then replay the production delivery+settle path.  The
        # immutable ledger lane must win and no second queue entry/turn may run.
        lost_job = await self._create_job("lost-response-historical-lane")
        lost_wake, lost_delivery_id = await self._claim_job(lost_job)
        outcome = await self.session_wake._deliver(self.db, lost_wake)
        if outcome != self.session_wake.WakeDeliveryResult.PERSISTED:
            raise GateError(
                "lost_response_seam_failed", "first delivery was not pending"
            )
        lost_done = await self._wait_delivery(lost_delivery_id)
        lost_turn = int(lost_done["admitted_turn_number"])
        await self._wait_turn(lost_turn)
        before_messages = await self._fixture_counts()
        lost_outbox = await self._job_outbox(lost_job)
        if lost_outbox.get("wake_state") != "sending":
            raise GateError(
                "lost_response_not_preserved", "outbox did not stay sending"
            )
        async with self.db.acquire() as conn:
            async with conn.transaction():
                thread = await conn.fetchrow(
                    "SELECT execution_lane, agent_id FROM threads "
                    "WHERE id=$1::uuid FOR UPDATE",
                    self.state.thread_id,
                )
                queue = await conn.fetchrow(
                    "SELECT state FROM run_queue WHERE unit_id=$1::uuid FOR UPDATE",
                    self.state.thread_id,
                )
                if (
                    thread is None
                    or thread["execution_lane"] != "stateless"
                    or thread["agent_id"] is not None
                    or queue is None
                    or queue["state"] != "done"
                ):
                    raise GateError("lane_change_unsafe", "fixture lane is not idle")
                await conn.execute(
                    "UPDATE threads SET execution_lane='pinned' WHERE id=$1::uuid",
                    self.state.thread_id,
                )
        replayed = await self.session_wake._deliver_and_settle(self.db, lost_wake)
        if replayed is not True:
            raise GateError("terminal_replay_failed", "historical-lane replay failed")
        replay_delivery = await self._delivery(lost_delivery_id)
        after_messages = await self._fixture_counts()
        if replay_delivery.get("execution_lane") != "stateless":
            raise GateError("historical_lane_lost", "terminal receipt changed lanes")
        if before_messages != after_messages:
            raise GateError("terminal_replay_duplicated", "terminal replay added work")
        if (await self._job_outbox(lost_job)).get("wake_state") != "sent":
            raise GateError("outbox_not_sent", "lost-response outbox did not converge")
        async with self.db.acquire() as conn:
            await conn.execute(
                "UPDATE threads SET execution_lane='stateless' "
                "WHERE id=$1::uuid AND execution_lane='pinned' AND agent_id IS NULL",
                self.state.thread_id,
            )
        _emit(
            "lost_response_historical_lane",
            status="pass",
            delivery=_safe_delivery(replay_delivery),
            outbox_state="sent",
        )

        await asyncio.sleep(2.0)
        await self._assert_exactly_once()
        result = {
            "status": "pass",
            "run_id": self.state.run_id,
            "thread_id": self.state.thread_id,
            "jobs": len(self.state.job_ids),
            "deleted_executors": len(self.state.deleted_pods),
        }
        _emit("scorecard", **result)
        return result

    async def _fixture_counts(self) -> dict[str, int]:
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT "
                "(SELECT count(*) FROM thread_messages WHERE thread_id=$1::uuid) "
                "AS messages, "
                "(SELECT count(*) FROM thread_input_deliveries "
                " WHERE thread_id=$1::uuid) AS deliveries, "
                "(SELECT count(*) FROM run_queue WHERE unit_id=$1::uuid) AS queues",
                self.state.thread_id,
            )
        return {key: int(row[key] or 0) for key in ("messages", "deliveries", "queues")}

    async def _assert_exactly_once(self) -> None:
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT delivery.delivery_id, delivery.state, delivery.execution_lane, "
                "delivery.source, message.role, "
                "delivery.admitted_turn_number, count(message.id) AS transcript_rows "
                "FROM thread_input_deliveries AS delivery "
                "JOIN thread_messages AS message ON message.id=delivery.message_id "
                "WHERE delivery.thread_id=$1::uuid "
                "GROUP BY delivery.delivery_id, delivery.state, "
                "delivery.execution_lane, delivery.source, message.role, "
                "delivery.admitted_turn_number "
                "ORDER BY delivery.delivery_id",
                self.state.thread_id,
            )
            outboxes = await conn.fetch(
                "SELECT wake_state, count(*) AS n FROM jobs "
                "WHERE id=ANY($1::uuid[]) GROUP BY wake_state",
                [UUID(job_id) for job_id in self.state.job_ids],
            )
        if len(rows) != len(self.state.job_ids):
            raise GateError("delivery_count_mismatch", "fixture delivery count differs")
        if any(
            row["state"] != "settled"
            or row["execution_lane"] != "stateless"
            or row["source"] != "officer_wake"
            or row["role"] != "event"
            or row["admitted_turn_number"] is None
            or int(row["transcript_rows"] or 0) != 1
            for row in rows
        ):
            raise GateError("delivery_not_exact", "fixture delivery is not exact-once")
        if {str(row["wake_state"]): int(row["n"]) for row in outboxes} != {
            "sent": len(self.state.job_ids)
        }:
            raise GateError("outbox_not_exact", "fixture outboxes are not all sent")

    async def _fixture_ids(self) -> dict[str, list[str]]:
        async with self.db.acquire() as conn:
            threads = await conn.fetch(
                "SELECT id FROM threads WHERE metadata @> $1::jsonb ORDER BY id",
                json.dumps({"acceptance_gate": _marker(self.state.run_id)}),
            )
            jobs = await conn.fetch(
                "SELECT id FROM jobs WHERE context @> $1::jsonb ORDER BY id",
                json.dumps({"acceptance_gate": _marker(self.state.run_id)}),
            )
        return {
            "threads": [str(row["id"]) for row in threads],
            "jobs": [str(row["id"]) for row in jobs],
        }

    async def cleanup(self) -> dict[str, Any]:
        ids = await self._fixture_ids()
        self.state.job_ids = list(ids["jobs"])
        if len(ids["threads"]) > 1:
            raise GateError("ambiguous_fixture", "run-id selects multiple threads")
        self.state.thread_id = ids["threads"][0] if ids["threads"] else None

        for job_id in self.state.job_ids:
            await self.db.delete_job(job_id, deletion_reason="acceptance_gate_cleanup")

        if self.state.thread_id:
            # A failure may land after the historical-replay seam changed the
            # idle fixture to pinned but before it restored stateless.  Return
            # that exact no-agent/no-pending-delivery shape to its creation
            # lane before invoking the supported End funnel.  No live product
            # row can match the globally unique run-id marker selected above.
            async with self.db.acquire() as conn:
                async with conn.transaction():
                    thread = await conn.fetchrow(
                        "SELECT execution_lane, agent_id FROM threads "
                        "WHERE id=$1::uuid FOR UPDATE",
                        self.state.thread_id,
                    )
                    if (
                        thread is not None
                        and thread["execution_lane"] == "pinned"
                        and thread["agent_id"] is None
                    ):
                        pending = await conn.fetchval(
                            "SELECT count(*) FROM thread_input_deliveries "
                            "WHERE thread_id=$1::uuid "
                            "AND state IN ('persisted','owned','queued','deferred')",
                            self.state.thread_id,
                        )
                        if int(pending or 0) != 0:
                            raise GateError(
                                "cleanup_lane_blocked",
                                "fixture has pending input after lane-change exercise",
                            )
                        await conn.execute(
                            "UPDATE threads SET execution_lane='stateless' "
                            "WHERE id=$1::uuid",
                            self.state.thread_id,
                        )
            for _attempt in range(8):
                thread = await self.db.get_thread(self.state.thread_id)
                if thread is None:
                    break
                try:
                    from orchestrator.application import (
                        controls as controls_composition,
                    )

                    await controls_composition.thread_retirement_operations(
                        self.resources
                    ).end_thread_flow(
                        self.state.thread_id,
                        thread,
                        permanent=True,
                        force=True,
                    )
                except Exception:
                    await asyncio.sleep(2.0)
                    continue
                if await self.db.get_thread(self.state.thread_id) is None:
                    break
                await asyncio.sleep(1.0)

        residue = await self._fixture_ids()
        run_queue_rows = 0
        delivery_rows = 0
        message_rows = 0
        if self.state.thread_id:
            async with self.db.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT "
                    "(SELECT count(*) FROM run_queue WHERE unit_id=$1::uuid) AS q, "
                    "(SELECT count(*) FROM thread_input_deliveries "
                    " WHERE thread_id=$1::uuid) AS d, "
                    "(SELECT count(*) FROM thread_messages "
                    " WHERE thread_id=$1::uuid) AS m",
                    self.state.thread_id,
                )
            run_queue_rows = int(row["q"] or 0)
            delivery_rows = int(row["d"] or 0)
            message_rows = int(row["m"] or 0)
        if (
            residue["threads"]
            or residue["jobs"]
            or run_queue_rows
            or delivery_rows
            or message_rows
        ):
            raise GateError("cleanup_incomplete", "fixture residue remains")

        # The lite fixture owns no Pod or PVC.  Check both full and historical
        # short thread labels so cleanup evidence cannot miss an accidental
        # provisioning regression.
        k8s_pods = 0
        k8s_pvcs = 0
        if self.state.thread_id:
            api = getattr(self.provisioner, "_core_api", None)
            for label in (
                f"srw.io/thread-id={self.state.thread_id}",
                f"srw/thread-id={self.state.thread_id[:12]}",
            ):
                pods = await asyncio.to_thread(
                    api.list_namespaced_pod,
                    namespace=self.namespace,
                    label_selector=label,
                )
                pvcs = await asyncio.to_thread(
                    api.list_namespaced_persistent_volume_claim,
                    namespace=self.namespace,
                    label_selector=label,
                )
                k8s_pods += len(getattr(pods, "items", None) or [])
                k8s_pvcs += len(getattr(pvcs, "items", None) or [])
        if k8s_pods or k8s_pvcs:
            raise GateError("cleanup_incomplete", "fixture Kubernetes residue remains")

        async with self.db.acquire() as conn:
            auto_pull = await conn.fetchval(
                "SELECT "
                "(SELECT count(*) FROM project_officers "
                " WHERE config_override #> '{officer,auto_pull}' = 'true'::jsonb) "
                "+ (SELECT count(*) FROM threads "
                " WHERE metadata #> '{config_override,officer,auto_pull}' "
                "       = 'true'::jsonb)"
            )
        if int(auto_pull or 0) != 0:
            raise GateError("auto_pull_enabled", "auto-pull changed during gate")
        result = {
            "status": "pass",
            "run_id": self.state.run_id,
            "threads": 0,
            "jobs": 0,
            "run_queue": 0,
            "deliveries": 0,
            "messages": 0,
            "pods": 0,
            "pvcs": 0,
            "auto_pull_enabled": 0,
        }
        _emit("cleanup", **result)
        return result


async def _async_main(args: argparse.Namespace) -> int:
    mutating = bool(args.execute or args.cleanup_only)
    if mutating and os.environ.get("SRW_WAKE_GATE_CONTEXT") != EXPECTED_CONTEXT:
        raise GateError(
            "wrong_context",
            "host wrapper did not attest the disposable k3d context",
        )
    gate = StatelessWakeGate(args)
    await gate.connect()
    success = False
    try:
        if args.cleanup_only:
            await gate.inspect(require_owner=False, require_capacity=False)
            await gate.cleanup()
        elif args.execute:
            try:
                await gate.run()
                success = True
            finally:
                # A reused run ID is deliberately not adopted by --execute.
                # Only clean a thread this invocation created/loaded into its
                # state; pre-existing residue requires explicit cleanup-only.
                if gate.state.thread_id is not None:
                    await gate.cleanup()
        else:
            await gate.inspect(require_owner=False)
        return 0 if success or not args.execute else 1
    finally:
        await gate.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return asyncio.run(_async_main(args))
    except GateError as exc:
        _emit("gate_failed", status="fail", code=exc.code, detail=exc.detail)
        return 2
    except Exception as exc:  # Never print a possibly credential-bearing message.
        _emit(
            "gate_failed",
            status="error",
            code="unexpected_exception",
            exception_class=type(exc).__name__,
        )
        return 3


if __name__ == "__main__":  # pragma: no cover - module entrypoint
    sys.exit(main())
