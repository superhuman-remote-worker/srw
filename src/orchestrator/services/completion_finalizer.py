"""Durable execution for accepted job-completion commands.

The accept transaction in :mod:`services.job_completion_commands` owns the
short admission fence.  This module owns everything after that boundary:

* a River-style, expiring leader row for background drain work;
* a renewable, uniquely-owned lease on each command;
* stable-name effect intents written before their callback;
* exact-term CAS for effect completion, command completion, retry and parking.

Leader election reduces duplicate work, but is deliberately not a correctness
lock.  Two leaders may overlap during a partition; the per-command owner UUID
is the fencing term that prevents the stale executor from writing progress.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import random
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Awaitable, Callable, Mapping, Sequence
from uuid import UUID, uuid4

from orchestrator.services.completion_control import (
    COMPLETION_CONTROL_CLAIM_KEY,
    COMPLETION_CONTROL_CLAIM_SECONDS,
    COMPLETION_CONTROL_CLAIM_VERSION,
    COMPLETION_DELIVERY_CONTROL_SOURCE,
    completion_control_claim_active,
    completion_delivery_control_claim_owned_active,
)
from orchestrator.services.job_completion_commands import (
    COMPLETION_CODE_VERSION,
    COMPLETION_STATUS_REORDER_CODE_VERSION,
    COMPLETION_SUPPORTED_CODE_VERSIONS,
    accepted_completion_decision_tool_call_id,
)

logger = logging.getLogger(__name__)

COMMAND_LEASE_SECONDS = 120.0
COMMAND_HEARTBEAT_SECONDS = 10.0
EFFECT_LEASE_SECONDS = 90.0
LEADER_LEASE_SECONDS = 120.0
LEADER_HEARTBEAT_SECONDS = 10.0
IDLE_POLL_SECONDS = 1.0
BUSY_POLL_SECONDS = 0.1
EFFECT_DETAIL_LIMIT_BYTES = 8 * 1024
LEADER_LEASE_NAME = "job_completion"
EFFECT_WRITE_MARGIN_SECONDS = 1.0
EFFECT_COMMAND_LEASE_GAP_SECONDS = 5.0
RETRY_BUCKET_CAPACITY = 10.0
RETRY_BUCKET_REFILL_PER_SECOND = 1.0
DELIVERY_CONTROL_SOURCE = COMPLETION_DELIVERY_CONTROL_SOURCE
_RETRY_BUCKET_TOKENS = RETRY_BUCKET_CAPACITY
_RETRY_BUCKET_UPDATED = time.monotonic()
_RETRY_BUCKET_LOCKS: dict[int, asyncio.Lock] = {}

Workflow = Callable[["CompletionEffectRunner"], Awaitable[Mapping[str, Any]]]
EffectCallback = Callable[[], Awaitable[Any]]
EffectErrorOutput = Callable[[BaseException], Any]
EffectRetryPredicate = Callable[[Any], bool]
EffectSupersedePredicate = Callable[[Any], bool]
AlertCallback = Callable[[str], Any]
PreclaimCallback = Callable[[str], Awaitable[Any]]


class CompletionFinalizerError(RuntimeError):
    """Base class for durable-finalizer failures."""


class CompletionLeaseLost(CompletionFinalizerError):
    """The exact command owner no longer has authority to write."""


class CompletionDispositionSuperseded(CompletionFinalizerError):
    """A legitimate jobs-row writer displaced this command's disposition."""

    def __init__(
        self,
        *,
        observed_status: str,
        expected_statuses: Sequence[str],
        reason: str = "entry_status_superseded",
    ) -> None:
        self.observed_status = str(observed_status or "unknown")
        self.expected_statuses = tuple(
            dict.fromkeys(
                status
                for value in expected_statuses
                if (status := str(value or "").strip())
            )
        )
        normalized_reason = str(reason or "entry_status_superseded").strip()
        self.reason = (normalized_reason or "entry_status_superseded")[:128]
        super().__init__(
            "completion disposition superseded: observed "
            f"{self.observed_status!r}, expected one of "
            f"{self.expected_statuses!r}"
        )


class CompletionTeardownSupersedeBlocked(CompletionFinalizerError):
    """An authorized S36 callback may still be performing external work."""


class CompletionEffectInFlight(CompletionFinalizerError):
    """An earlier effect attempt is still inside its ambiguity window."""


class CompletionEffectBudgetExhausted(CompletionFinalizerError):
    """An effect exhausted its independently stored retry budget."""


class CompletionEffectVersionError(CompletionFinalizerError):
    """A stable effect name was reused with an incompatible group."""


class CompletionEffectGroupBlocked(CompletionFinalizerError):
    """An effect's own group or one of its declared dependencies is pending."""

    def __init__(self, group: str, blocked_by: str) -> None:
        super().__init__(
            f"completion effect group {group!r} is blocked by pending "
            f"group {blocked_by!r}"
        )
        self.group = group
        self.blocked_by = blocked_by


class CompletionEffectRetryRequested(CompletionFinalizerError):
    """An explicitly classified callback result requires a group retry."""


@dataclass(frozen=True, slots=True)
class _GroupBlock:
    reason: BaseException
    run_after: datetime | None = None
    exhausted: bool = False


@dataclass(frozen=True, slots=True)
class LeaderTerm:
    """The immutable identity of one finalizer leadership tenure."""

    leader_id: str
    elected_at: datetime


@dataclass(frozen=True, slots=True)
class FinalizationResult:
    """Result of one exact-ID finalization attempt."""

    command_id: str
    state: str
    disposition: str
    outcome: dict[str, Any] | None = None
    run_after: datetime | None = None
    error_code: str | None = None

    @property
    def done(self) -> bool:
        return self.state in {"done", "superseded", "force_resolved"}


@asynccontextmanager
async def _connection(source: Any) -> AsyncIterator[Any]:
    acquire = getattr(source, "acquire", None)
    if acquire is None:
        yield source
        return
    async with acquire() as conn:
        yield conn


def _json_object(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, str):
        parsed = json.loads(value)
        return dict(parsed) if isinstance(parsed, Mapping) else None
    return dict(value) if isinstance(value, Mapping) else None


def _command_dict(row: Any) -> dict[str, Any]:
    result = dict(row)
    result["id"] = str(result["id"])
    result["job_id"] = str(result["job_id"])
    result["payload"] = _json_object(result.get("payload")) or {}
    result["outcome"] = _json_object(result.get("outcome"))
    if result.get("accepted_agent_id") is not None:
        result["accepted_agent_id"] = str(result["accepted_agent_id"])
    return result


def _error_code(exc: BaseException) -> str:
    name = type(exc).__name__ or "completion_error"
    return name[:128]


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    """Read an optional field from asyncpg records and small test doubles."""

    try:
        return row[key]
    except (KeyError, TypeError):
        return default


_NO_EFFECT_INTENT = object()

# Only these fields are presentation/diagnostic data.  Every sibling field in
# the same output remains exact because the route uses it to reconstruct a
# branch after a crash (status, booleans, UIDs, SHAs, request IDs, counters,
# etc.).  Keeping this allowlist keyed by the stable effect name prevents a
# coincidentally named ``error`` or ``actions`` field in a future effect from
# being silently treated as non-authoritative.
_DIAGNOSTIC_OUTPUT_PATHS: dict[str, tuple[tuple[str, ...], ...]] = {
    "persist_reported_freeze": (("error",),),
    "drop_queued_replies": (("error",),),
    "infra_transient_give_up": (("actions",),),
    "infra_transient_pause": (("actions",),),
    "pod_workspace_recovery": (("actions",),),
    "vm_workspace_recovery": (("actions",),),
    "reset_recovery_strikes": (("error",),),
    "llm_give_up_operator_alert": (("error",),),
    "deliverable_contract_gate": (("actions",),),
    "loop_project_cloud_delivery": (("action",),),
    "mode_a_diff_capture": (("error",),),
    "job_outbox_delivery": (("error",),),
    "drain_stall_counter_alert": (("error",),),
    "drain_stall_operator_alert": (("error",),),
    "sudo_approval_request": (("error",),),
    "auto_deny_resume": (("error",),),
    "freeze_notification": (("error",),),
    "subjob_output_graft": (("graft_result", "reason"),),
    "critic_verdict": (("actions",), ("error",)),
    "critic_verdict_followup": (("actions",), ("error",)),
    "scholar_parent_unblock": (("actions",), ("error",)),
    "delegation_parent_unblock": (("actions",), ("error",)),
    "verification_critic_spawn": (("actions",), ("error",)),
    "verification_critic_handoff": (("actions",), ("error",)),
    "curation_final_pass": (("error",),),
    "project_loop_advance": (("actions",), ("error",)),
    "project_loop_advance_handoff": (("actions",), ("error",)),
    "terminal_merge_change_record": (("actions",), ("error",)),
    "session_wake_enqueue": (("error",),),
    "workspace_archive_teardown": (("actions",), ("error",)),
}
_DIAGNOSTIC_STRING_LIMIT_BYTES = 1024
_DIAGNOSTIC_LIST_LIMIT = 8
_DIAGNOSTIC_LIST_ITEM_LIMIT_BYTES = 256


def _bounded_detail(detail: Mapping[str, Any]) -> str:
    """Serialize one complete effect detail object under the row-size cap."""

    encoded = json.dumps(
        dict(detail),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
        allow_nan=False,
    )
    if len(encoded.encode("utf-8")) > EFFECT_DETAIL_LIMIT_BYTES:
        raise CompletionFinalizerError(
            "completion effect replay detail exceeds the 8 KiB correctness cap"
        )
    return encoded


def _json_size(value: Any) -> int:
    return len(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
            allow_nan=False,
        ).encode("utf-8")
    )


def _utf8_prefix(value: str, limit: int) -> str:
    """Return a valid-UTF-8 diagnostic prefix no larger than ``limit`` bytes."""

    raw = value.encode("utf-8")
    if len(raw) <= limit:
        return value
    suffix = "…"
    prefix_limit = max(0, limit - len(suffix.encode("utf-8")))
    return raw[:prefix_limit].decode("utf-8", errors="ignore") + suffix


def _compact_diagnostic_value(value: Any, *, minimal: bool) -> Any | None:
    """Compact one allowlisted value without changing its JSON value kind."""

    if isinstance(value, str):
        compacted = (
            "…" if minimal else _utf8_prefix(value, _DIAGNOSTIC_STRING_LIMIT_BYTES)
        )
        return compacted if compacted != value else None
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        if minimal:
            compacted_list = ["…"] if value else []
        else:
            compacted_list = [
                _utf8_prefix(item, _DIAGNOSTIC_LIST_ITEM_LIMIT_BYTES)
                for item in value[:_DIAGNOSTIC_LIST_LIMIT]
            ]
            if len(value) > _DIAGNOSTIC_LIST_LIMIT:
                compacted_list.append(
                    f"… {len(value) - _DIAGNOSTIC_LIST_LIMIT} more diagnostic item(s)"
                )
        return compacted_list if compacted_list != value else None
    return None


def _compact_effect_diagnostics(
    effect_name: str,
    detail: Mapping[str, Any],
    *,
    minimal: bool,
) -> dict[str, Any] | None:
    """Compact only explicitly non-authoritative output fields.

    The returned metadata records both the original serialized size and the
    retained size. Its field count is bounded by the static allowlist, never by
    caller-controlled output cardinality.
    """

    paths = _DIAGNOSTIC_OUTPUT_PATHS.get(effect_name, ())
    if not paths or not isinstance(detail.get("output"), Mapping):
        return None
    candidate = copy.deepcopy(dict(detail))
    output = candidate["output"]
    if not isinstance(output, dict):
        output = dict(output)
        candidate["output"] = output
    truncations: dict[str, dict[str, int]] = {}
    for path in paths:
        parent: Any = output
        for component in path[:-1]:
            if not isinstance(parent, dict) or component not in parent:
                parent = None
                break
            parent = parent[component]
        if not isinstance(parent, dict) or path[-1] not in parent:
            continue
        original = parent[path[-1]]
        compacted = _compact_diagnostic_value(original, minimal=minimal)
        if compacted is None:
            continue
        parent[path[-1]] = compacted
        truncation = {
            "original_bytes": _json_size(original),
            "stored_bytes": _json_size(compacted),
        }
        if isinstance(original, list) and isinstance(compacted, list):
            truncation.update(
                original_items=len(original),
                stored_items=len(compacted),
            )
        truncations["output." + ".".join(path)] = truncation
    if not truncations:
        return None
    candidate["diagnostic_truncation"] = {"fields": truncations}
    return candidate


def _bounded_effect_detail(
    effect_name: str,
    output: Any,
    *,
    intent: Any = _NO_EFFECT_INTENT,
    teardown_authorization: Any = _NO_EFFECT_INTENT,
) -> str:
    """Serialize replay data while enforcing the design's 8 KiB row bound.

    Outputs used to steer the legacy tail must be replayable in full. Only the
    stable-effect/path pairs audited as presentation diagnostics may be
    compacted; oversized identity or branch data fails loudly. Call sites with
    naturally large correctness data must persist it in their existing domain
    table and return a bounded summary.
    """

    detail = {"output": output}
    if intent is not _NO_EFFECT_INTENT:
        # Keep the destructive-effect identity in the same bounded row when
        # the replay output is written.  Replacing it here would reopen the ABA
        # hole after a crash following Kubernetes deletion.
        detail = {"intent": intent, **detail}
    if teardown_authorization is not _NO_EFFECT_INTENT:
        # S36's jobs-lock authorization is the admission barrier for an
        # external callback that may outlive every finalizer clock. Preserve
        # it across pending->pending retry rewrites; only the same UPDATE that
        # settles the effect to done may remove it.
        detail = {"teardown_authorization": teardown_authorization, **detail}
    try:
        return _bounded_detail(detail)
    except CompletionFinalizerError:
        # Never truncate by field name alone. Only stable-effect/path pairs
        # audited above are eligible, and intents are never eligible.
        for minimal in (False, True):
            compacted = _compact_effect_diagnostics(
                effect_name, detail, minimal=minimal
            )
            if compacted is None:
                continue
            try:
                return _bounded_detail(compacted)
            except CompletionFinalizerError:
                pass
        raise CompletionFinalizerError(
            "completion effect replay-critical detail exceeds the 8 KiB correctness cap"
        )


class CompletionEffectRunner:
    """Stable-effect journal bound to one exact command-lease term."""

    def __init__(
        self,
        db: Any,
        *,
        command: Mapping[str, Any],
        owner: str,
        effect_lease_seconds: float = EFFECT_LEASE_SECONDS,
        command_lease_seconds: float = COMMAND_LEASE_SECONDS,
        random_source: Callable[[], float] = random.random,
    ) -> None:
        self._db = db
        self.command = dict(command)
        self.owner = owner
        self._effect_lease_seconds = float(effect_lease_seconds)
        self._command_lease_seconds = float(command_lease_seconds)
        self._random_source = random_source
        self._blocked_groups: dict[str, _GroupBlock] = {}

    @property
    def requires_retry(self) -> bool:
        """Whether this workflow pass left at least one required group pending."""

        return bool(self._blocked_groups)

    @property
    def command_id(self) -> str:
        return str(self.command["id"])

    async def _effect_row(self, name: str) -> Any:
        async with _connection(self._db) as conn:
            return await conn.fetchrow(
                """
                SELECT effect_name, effect_group, state, attempts, max_attempts,
                       run_after, intent_at, complete_by, completed_at, detail,
                       error_code, run_after > now() AS deferred
                FROM completion_effects
                WHERE producer_kind = 'job_completion'
                  AND producer_id = $1::uuid
                  AND effect_name = $2::text
                  AND EXISTS (
                      SELECT 1 FROM job_completion_commands AS command
                      WHERE command.id = $1::uuid
                        AND command.state = 'finalizing'
                        AND command.finalizing_by = $3::text
                        AND command.lease_expires_at > now()
                  )
                """,
                UUID(self.command_id),
                name,
                self.owner,
            )

    async def has_started(self, name: str) -> bool:
        row = await self._effect_row(name)
        return bool(row is not None and row["intent_at"] is not None)

    async def has_completed(self, name: str) -> bool:
        row = await self._effect_row(name)
        return bool(row is not None and str(row["state"]) == "done")

    async def completed_detail(self, name: str) -> Any:
        row = await self._effect_row(name)
        if row is None or str(row["state"]) != "done":
            return None
        detail = _json_object(row["detail"]) or {}
        return detail.get("output")

    async def terminal_detail(self, name: str) -> Any:
        """Return replay output for ``done|superseded``, never authority proof."""

        row = await self._effect_row(name)
        if row is None or str(row["state"]) not in {"done", "superseded"}:
            return None
        detail = _json_object(row["detail"]) or {}
        return detail.get("output")

    async def capture_intent(
        self,
        name: str,
        detail: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Durably preserve one destructive-effect identity before acting.

        This is valid only from inside ``run``'s callback, after ``_prepare``
        has created the live effect intent.  A resumed attempt must offer the
        exact same identity: silently recapturing a same-name Kubernetes
        replacement would turn retry into an ABA delete.
        """

        incoming: dict[str, Any] | None = None
        if detail is not None:
            incoming_json = _bounded_detail({"intent": dict(detail)})
            incoming = json.loads(incoming_json)["intent"]
        async with _connection(self._db) as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT effect.detail
                    FROM completion_effects AS effect
                    WHERE effect.producer_kind = 'job_completion'
                      AND effect.producer_id = $1::uuid
                      AND effect.effect_name = $3::text
                      AND effect.state = 'pending'
                      AND effect.complete_by > now()
                      AND EXISTS (
                          SELECT 1
                          FROM job_completion_commands AS command
                          WHERE command.id = $1::uuid
                            AND command.state = 'finalizing'
                            AND command.finalizing_by = $2::text
                            AND command.lease_expires_at > now()
                      )
                    FOR UPDATE
                    """,
                    UUID(self.command_id),
                    self.owner,
                    name,
                )
                if row is None:
                    raise CompletionLeaseLost(
                        f"completion effect {name!r} lost its intent-capture term"
                    )

                current = _json_object(row["detail"]) or {}
                if "intent" in current:
                    captured = current["intent"]
                    if incoming is not None and captured != incoming:
                        raise CompletionEffectVersionError(
                            f"completion effect {name!r} intent identity drifted"
                        )
                    return dict(captured)

                # Read-before-capture is how a resumed destructive effect
                # distinguishes "the old Pod is already gone" from "this is
                # the first attempt and no identity was ever persisted".
                if incoming is None:
                    return None

                merged = {**current, "intent": incoming}
                merged_json = _bounded_detail(merged)
                captured = await conn.fetchrow(
                    """
                    UPDATE completion_effects AS effect
                    SET detail = $4::jsonb
                    WHERE effect.producer_kind = 'job_completion'
                      AND effect.producer_id = $1::uuid
                      AND effect.effect_name = $3::text
                      AND effect.state = 'pending'
                      AND effect.complete_by > now()
                      AND EXISTS (
                          SELECT 1
                          FROM job_completion_commands AS command
                          WHERE command.id = $1::uuid
                            AND command.state = 'finalizing'
                            AND command.finalizing_by = $2::text
                            AND command.lease_expires_at > now()
                      )
                    RETURNING effect.detail
                    """,
                    UUID(self.command_id),
                    self.owner,
                    name,
                    merged_json,
                )
        if captured is None:
            raise CompletionLeaseLost(
                f"completion effect {name!r} lost its intent-capture term"
            )
        persisted = _json_object(captured["detail"]) or {}
        return dict(persisted["intent"])

    async def authorize_workspace_teardown(self) -> Any:
        """Install S36's durable admission barrier under the jobs-row lock."""

        from orchestrator.services.completion_teardown_authority import (
            authorize_workspace_teardown,
        )

        return await authorize_workspace_teardown(
            self._db,
            job_id=str(self.command["job_id"]),
            command_id=self.command_id,
            owner=self.owner,
        )

    async def workspace_teardown_handoff(self) -> Any:
        """Return whether the newest lower report deferred S36 to this one."""

        from orchestrator.services.completion_teardown_authority import (
            workspace_teardown_handoff,
        )

        return await workspace_teardown_handoff(
            self._db,
            job_id=str(self.command["job_id"]),
            before_report_seq=int(self.command["report_seq"]),
        )

    async def assert_entry_authority(self) -> None:
        """Fence pre-S17 Class B/delivery against a concurrent control writer.

        Reordered product delivery necessarily runs before the command-owned
        disposition marker exists.  The jobs row remains the serialization
        point: require the exact entry status resolved by the finalizer and no
        live out-of-band control claim before any of those effects can begin.
        S17 repeats the same checks in its own transaction after delivery.
        """

        expected_status = str(self.command.get("resolved_entry_status") or "").strip()
        if not expected_status:
            raise CompletionFinalizerError(
                f"completion command {self.command_id} has no resolved entry status"
            )
        async with _connection(self._db) as conn:
            async with conn.transaction():
                job = await conn.fetchrow(
                    """
                    SELECT status::text AS status, context,
                           extract(epoch FROM clock_timestamp())::float8
                               AS db_now_epoch
                    FROM jobs
                    WHERE id = $1::uuid
                    FOR UPDATE
                    """,
                    UUID(str(self.command["job_id"])),
                )
                if job is None:
                    raise CompletionLeaseLost(
                        f"completion command {self.command_id} lost its jobs row"
                    )
                exact = await conn.fetchval(
                    """
                    SELECT 1
                    FROM job_completion_commands
                    WHERE id = $1::uuid
                      AND job_id = $2::uuid
                      AND state = 'finalizing'
                      AND finalizing_by = $3::text
                      AND lease_expires_at > now()
                      AND deadline_at > now()
                    """,
                    UUID(self.command_id),
                    UUID(str(self.command["job_id"])),
                    self.owner,
                )
                if exact is None:
                    raise CompletionLeaseLost(
                        f"completion command {self.command_id} lost its entry-authority term"
                    )

                current_status = str(job["status"] or "")
                from orchestrator.services.completion_control import (
                    completion_control_claim_active,
                )

                control_claimed = completion_control_claim_active(
                    _row_value(job, "context"),
                    now_epoch=float(_row_value(job, "db_now_epoch", 0.0)),
                )
                if current_status != expected_status or control_claimed:
                    observed = (
                        f"{current_status}:control_claimed"
                        if control_claimed
                        else current_status
                    )
                    raise CompletionDispositionSuperseded(
                        observed_status=observed,
                        expected_statuses=(expected_status,),
                    )

    async def acquire_delivery_control(self, expected_status: str) -> str:
        """Install or renew this command's durable pre-S17 delivery barrier.

        Product delivery cannot share one transaction with WebDAV/Gitea I/O.
        The jobs row therefore carries the same reserved control marker used by
        human verbs.  Its stable owner is the command id (so a crash can adopt
        it), while every read/write also requires this runner's exact ephemeral
        command lease (so a stale process cannot use the adopted marker).
        """

        expected = str(expected_status or "").strip()
        if not expected:
            raise CompletionFinalizerError(
                f"completion command {self.command_id} has no delivery entry status"
            )
        command_uuid = UUID(self.command_id)
        job_uuid = UUID(str(self.command["job_id"]))
        async with _connection(self._db) as conn:
            async with conn.transaction():
                job = await conn.fetchrow(
                    """
                    SELECT status::text AS status, execution_lane, context,
                           extract(epoch FROM clock_timestamp())::float8
                               AS db_now_epoch
                    FROM jobs
                    WHERE id = $1::uuid
                    FOR UPDATE
                    """,
                    job_uuid,
                )
                if job is None:
                    raise CompletionLeaseLost(
                        f"completion command {self.command_id} lost its jobs row"
                    )
                exact = await conn.fetchval(
                    """
                    SELECT 1
                    FROM job_completion_commands
                    WHERE id = $1::uuid
                      AND job_id = $2::uuid
                      AND state = 'finalizing'
                      AND finalizing_by = $3::text
                      AND lease_expires_at > now()
                      AND deadline_at > now()
                    """,
                    command_uuid,
                    job_uuid,
                    self.owner,
                )
                if exact is None:
                    raise CompletionLeaseLost(
                        f"completion command {self.command_id} lost its delivery-control term"
                    )

                current_status = str(job["status"] or "")
                context = _row_value(job, "context")
                now_epoch = float(_row_value(job, "db_now_epoch", 0.0))
                owned = completion_delivery_control_claim_owned_active(
                    context,
                    self.command_id,
                    now_epoch=now_epoch,
                )
                if current_status != expected or (
                    completion_control_claim_active(context, now_epoch=now_epoch)
                    and not owned
                ):
                    observed = (
                        f"{current_status}:control_claimed"
                        if current_status == expected
                        else current_status
                    )
                    raise CompletionDispositionSuperseded(
                        observed_status=observed,
                        expected_statuses=(expected,),
                        reason="delivery_control_superseded",
                    )

                installed = await conn.fetchval(
                    f"""
                    UPDATE jobs
                    SET context = jsonb_set(
                            COALESCE(context, '{{}}'::jsonb),
                            '{{{COMPLETION_CONTROL_CLAIM_KEY}}}',
                            jsonb_build_object(
                                'version', $4::int,
                                'claim_id', $1::text,
                                'source', $5::text,
                                'expected_status', $6::text,
                                'expected_lane', $7::text,
                                'fence_kind', 'completion_command',
                                'fence_value', $1::text,
                                'claimed_at', to_jsonb(now()),
                                'expires_epoch', to_jsonb(
                                    extract(epoch FROM clock_timestamp())
                                    + $8::float8
                                )
                            ),
                            true
                        ),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = $2::uuid
                      AND status::text = $6::text
                      AND EXISTS (
                          SELECT 1
                          FROM job_completion_commands AS command
                          WHERE command.id = $1::uuid
                            AND command.job_id = jobs.id
                            AND command.state = 'finalizing'
                            AND command.finalizing_by = $3::text
                            AND command.lease_expires_at > now()
                            AND command.deadline_at > now()
                      )
                    RETURNING 1
                    """,
                    command_uuid,
                    job_uuid,
                    self.owner,
                    COMPLETION_CONTROL_CLAIM_VERSION,
                    DELIVERY_CONTROL_SOURCE,
                    expected,
                    str(_row_value(job, "execution_lane", "pinned") or "pinned"),
                    float(COMPLETION_CONTROL_CLAIM_SECONDS),
                )
                if installed is None:
                    raise CompletionLeaseLost(
                        f"completion command {self.command_id} lost its delivery-control install term"
                    )
        return self.command_id

    async def assert_delivery_control(self, expected_status: str) -> None:
        """Fence one delivery callback to the exact live command + marker."""

        expected = str(expected_status or "").strip()
        if not expected:
            raise CompletionFinalizerError(
                f"completion command {self.command_id} has no delivery entry status"
            )
        command_uuid = UUID(self.command_id)
        job_uuid = UUID(str(self.command["job_id"]))
        async with _connection(self._db) as conn:
            async with conn.transaction():
                job = await conn.fetchrow(
                    """
                    SELECT status::text AS status, context,
                           extract(epoch FROM clock_timestamp())::float8
                               AS db_now_epoch
                    FROM jobs
                    WHERE id = $1::uuid
                    FOR UPDATE
                    """,
                    job_uuid,
                )
                if job is None:
                    raise CompletionLeaseLost(
                        f"completion command {self.command_id} lost its jobs row"
                    )
                exact = await conn.fetchval(
                    """
                    SELECT 1
                    FROM job_completion_commands
                    WHERE id = $1::uuid
                      AND job_id = $2::uuid
                      AND state = 'finalizing'
                      AND finalizing_by = $3::text
                      AND lease_expires_at > now()
                      AND deadline_at > now()
                    """,
                    command_uuid,
                    job_uuid,
                    self.owner,
                )
                if exact is None:
                    raise CompletionLeaseLost(
                        f"completion command {self.command_id} lost its delivery-control term"
                    )

                current_status = str(job["status"] or "")
                context = _row_value(job, "context")
                owned = completion_delivery_control_claim_owned_active(
                    context,
                    self.command_id,
                    now_epoch=float(_row_value(job, "db_now_epoch", 0.0)),
                )
                if current_status != expected or not owned:
                    observed = (
                        f"{current_status}:delivery_control_lost"
                        if current_status == expected
                        else current_status
                    )
                    raise CompletionDispositionSuperseded(
                        observed_status=observed,
                        expected_statuses=(expected,),
                        reason="delivery_control_superseded",
                    )

    async def assert_disposition_authority(self) -> None:
        """Fence Class C against a jobs-row writer that won after S17.

        S17's completed effect output is the command-owned proof of the status
        this workflow wrote.  Taking the jobs-row lock makes this check order
        with cancel, preemption, drain, and human-control updates.  A workflow
        with no S17 has no disposition to defend and remains unchanged.

        An already-authorized S36 is deliberately non-abandonable: its
        external callback may outlive every database lease.  A mismatched
        status in that state fails closed instead of emitting a whole-command
        supersede signal.
        """

        async with _connection(self._db) as conn:
            async with conn.transaction():
                job = await conn.fetchrow(
                    """
                    SELECT status::text AS status
                    FROM jobs
                    WHERE id = $1::uuid
                    FOR UPDATE
                    """,
                    UUID(str(self.command["job_id"])),
                )
                if job is None:
                    raise CompletionLeaseLost(
                        f"completion command {self.command_id} lost its jobs row"
                    )
                authority = await conn.fetchrow(
                    """
                    SELECT command.id,
                           disposition.state AS disposition_state,
                           disposition.detail AS disposition_detail,
                           auto_deny.state AS auto_deny_state,
                           auto_deny.detail AS auto_deny_detail,
                           teardown.state AS teardown_state,
                           teardown.detail AS teardown_detail
                    FROM job_completion_commands AS command
                    LEFT JOIN completion_effects AS disposition
                      ON disposition.producer_kind = 'job_completion'
                     AND disposition.producer_id = command.id
                     AND disposition.effect_name = 'main_status_write'
                     AND disposition.effect_group = 'job_disposition'
                    LEFT JOIN completion_effects AS auto_deny
                      ON auto_deny.producer_kind = 'job_completion'
                     AND auto_deny.producer_id = command.id
                     AND auto_deny.effect_name = 'auto_deny_resume'
                     AND auto_deny.effect_group = 'auto_deny_resume'
                    LEFT JOIN completion_effects AS teardown
                      ON teardown.producer_kind = 'job_completion'
                     AND teardown.producer_id = command.id
                     AND teardown.effect_name = 'workspace_archive_teardown'
                     AND teardown.effect_group = 'workspace_teardown'
                    WHERE command.id = $1::uuid
                      AND command.job_id = $2::uuid
                      AND command.state = 'finalizing'
                      AND command.finalizing_by = $3::text
                      AND command.lease_expires_at > now()
                      AND command.deadline_at > now()
                    """,
                    UUID(self.command_id),
                    UUID(str(self.command["job_id"])),
                    self.owner,
                )
                if authority is None:
                    raise CompletionLeaseLost(
                        f"completion command {self.command_id} lost its "
                        "disposition-authority term"
                    )

                disposition_state = str(
                    _row_value(authority, "disposition_state", "") or ""
                )
                if not disposition_state:
                    return
                if disposition_state != "done":
                    raise CompletionFinalizerError(
                        f"completion command {self.command_id} reached Class C "
                        "before S17 settled"
                    )
                disposition_detail = (
                    _json_object(_row_value(authority, "disposition_detail")) or {}
                )
                disposition_output = disposition_detail.get("output")
                expected_status = (
                    str(disposition_output.get("new_status") or "").strip()
                    if isinstance(disposition_output, Mapping)
                    else ""
                )
                if not expected_status:
                    raise CompletionFinalizerError(
                        f"completion command {self.command_id} has no proven S17 status"
                    )

                expected_statuses = [expected_status]
                auto_deny_detail = (
                    _json_object(_row_value(authority, "auto_deny_detail")) or {}
                )
                auto_deny_output = auto_deny_detail.get("output")
                if (
                    str(_row_value(authority, "auto_deny_state", "")) == "done"
                    and isinstance(auto_deny_output, Mapping)
                    and auto_deny_output.get("auto_denied") is True
                ):
                    expected_statuses.append("paused")

                current_status = str(job["status"] or "")
                if current_status in expected_statuses:
                    return

                teardown_detail = (
                    _json_object(_row_value(authority, "teardown_detail")) or {}
                )
                teardown_authorization = teardown_detail.get("teardown_authorization")
                if (
                    str(_row_value(authority, "teardown_state", "")) == "pending"
                    and isinstance(teardown_authorization, Mapping)
                    and teardown_authorization.get("active") is True
                ):
                    raise CompletionTeardownSupersedeBlocked(
                        f"completion command {self.command_id} cannot abandon "
                        "an authorized workspace teardown"
                    )
                raise CompletionDispositionSuperseded(
                    observed_status=current_status,
                    expected_statuses=expected_statuses,
                )

    async def _prepare(
        self,
        *,
        name: str,
        group: str,
        effect_timeout_seconds: float,
        command_lease_seconds: float,
    ) -> tuple[bool, Any, float | None, bool]:
        """Write intent before action, or return a previously completed output."""

        async with _connection(self._db) as conn:
            async with conn.transaction():
                extended = await conn.fetchval(
                    """
                    UPDATE job_completion_commands
                    SET lease_expires_at = LEAST(
                        deadline_at,
                        GREATEST(
                            lease_expires_at,
                            now() + make_interval(secs => $3::float8)
                        )
                    )
                    WHERE id = $1::uuid
                      AND state = 'finalizing'
                      AND finalizing_by = $2::text
                      AND lease_expires_at > now()
                      AND deadline_at > now() + make_interval(
                          secs => $4::float8
                      )
                    RETURNING 1
                    """,
                    UUID(self.command_id),
                    self.owner,
                    command_lease_seconds,
                    effect_timeout_seconds + EFFECT_COMMAND_LEASE_GAP_SECONDS,
                )
                if extended is None:
                    raise CompletionLeaseLost(
                        f"completion effect {name!r} has insufficient fenced budget"
                    )
                # The owner EXISTS makes even the initial intent an exact-term
                # write.  A stale executor cannot introduce a new effect row.
                inserted = await conn.fetchrow(
                    """
                    INSERT INTO completion_effects (
                        producer_kind, producer_id, scope_id, effect_name,
                        effect_group, state, attempts, intent_at, complete_by,
                        detail
                    )
                    SELECT 'job_completion', command.id, command.job_id,
                           $3::text, $4::text, 'pending', 1, now(),
                           LEAST(
                               command.lease_expires_at - interval '5 seconds',
                               now() + make_interval(secs => $5::float8)
                           ),
                           '{}'::jsonb
                    FROM job_completion_commands AS command
                    WHERE command.id = $1::uuid
                      AND command.state = 'finalizing'
                      AND command.finalizing_by = $2::text
                      AND command.lease_expires_at > now() + interval '5 seconds'
                    ON CONFLICT (producer_kind, producer_id, effect_name)
                    DO NOTHING
                    RETURNING effect_group, state, attempts, max_attempts,
                              run_after, complete_by, detail,
                              FALSE AS deferred,
                              GREATEST(
                                  0.0,
                                  EXTRACT(EPOCH FROM complete_by - now())
                              )::float8 AS remaining_seconds
                    """,
                    UUID(self.command_id),
                    self.owner,
                    name,
                    group,
                    effect_timeout_seconds,
                )
                row = inserted
                if row is None:
                    row = await conn.fetchrow(
                        """
                        SELECT effect_group, state, attempts, max_attempts,
                               run_after, complete_by, detail,
                               run_after > now() AS deferred
                        FROM completion_effects
                        WHERE producer_kind = 'job_completion'
                          AND producer_id = $1::uuid
                          AND effect_name = $2::text
                          AND EXISTS (
                              SELECT 1
                              FROM job_completion_commands AS command
                              WHERE command.id = $1::uuid
                                AND command.state = 'finalizing'
                                AND command.finalizing_by = $3::text
                                AND command.lease_expires_at > now()
                          )
                        FOR UPDATE
                        """,
                        UUID(self.command_id),
                        name,
                        self.owner,
                    )
                    if row is None:
                        raise CompletionLeaseLost(
                            f"completion command {self.command_id} lost its lease"
                        )

                if str(row["effect_group"]) != group:
                    raise CompletionEffectVersionError(
                        f"effect {name!r} changed group across a resumable command"
                    )
                if str(row["state"]) in {"done", "superseded"}:
                    detail = _json_object(row["detail"]) or {}
                    return True, detail.get("output"), None, False
                if inserted is not None:
                    return False, None, float(inserted["remaining_seconds"]), False
                if int(row["attempts"]) >= int(row["max_attempts"]):
                    raise CompletionEffectBudgetExhausted(
                        f"completion effect {name!r} exhausted its retry budget"
                    )
                if bool(_row_value(row, "deferred", False)):
                    detail = _json_object(row["detail"]) or {}
                    return True, detail.get("output"), None, True

                renewed = await conn.fetchrow(
                    """
                    UPDATE completion_effects AS effect
                    SET attempts = effect.attempts + 1,
                        intent_at = now(),
                        complete_by = LEAST(
                            command.lease_expires_at - interval '5 seconds',
                            now() + make_interval(secs => $4::float8)
                        ),
                        error_code = NULL
                    FROM job_completion_commands AS command
                    WHERE effect.producer_kind = 'job_completion'
                      AND effect.producer_id = $1::uuid
                      AND effect.effect_name = $3::text
                      AND effect.state = 'pending'
                      AND effect.run_after <= now()
                      AND (effect.complete_by IS NULL OR effect.complete_by <= now())
                      AND command.id = $1::uuid
                      AND command.state = 'finalizing'
                      AND command.finalizing_by = $2::text
                      AND command.lease_expires_at > now() + interval '5 seconds'
                    RETURNING effect.attempts, effect.complete_by,
                              GREATEST(
                                  0.0,
                                  EXTRACT(EPOCH FROM effect.complete_by - now())
                              )::float8 AS remaining_seconds
                """,
                    UUID(self.command_id),
                    self.owner,
                    name,
                    effect_timeout_seconds,
                )
                if renewed is None:
                    # Distinguish a live ambiguity window from lost command
                    # ownership; both must prevent the callback from running.
                    live = await conn.fetchval(
                        """
                        SELECT complete_by > now()
                        FROM completion_effects
                        WHERE producer_kind = 'job_completion'
                          AND producer_id = $1::uuid
                          AND effect_name = $2::text
                          AND state = 'pending'
                        """,
                        UUID(self.command_id),
                        name,
                    )
                    if live:
                        raise CompletionEffectInFlight(
                            f"completion effect {name!r} may still be running"
                        )
                    current = await self._effect_row(name)
                    if current is not None and bool(
                        _row_value(current, "deferred", False)
                    ):
                        detail = _json_object(current["detail"]) or {}
                        return True, detail.get("output"), None, True
                    raise CompletionLeaseLost(
                        f"completion command {self.command_id} lost its lease"
                    )
                return False, None, float(renewed["remaining_seconds"]), False

    async def _record_error(self, name: str, exc: BaseException) -> None:
        async with _connection(self._db) as conn:
            await conn.execute(
                """
                UPDATE completion_effects AS effect
                SET error_code = $4::text
                WHERE effect.producer_kind = 'job_completion'
                  AND effect.producer_id = $1::uuid
                  AND effect.effect_name = $3::text
                  AND effect.state = 'pending'
                  AND effect.complete_by > now()
                  AND EXISTS (
                      SELECT 1 FROM job_completion_commands AS command
                      WHERE command.id = $1::uuid
                        AND command.state = 'finalizing'
                        AND command.finalizing_by = $2::text
                        AND command.lease_expires_at > now()
                  )
                """,
                UUID(self.command_id),
                self.owner,
                name,
                _error_code(exc),
            )

    async def _settle(self, name: str, output: Any, *, state: str) -> None:
        """Settle one exact-term effect to a replayable terminal state."""

        if state not in {"done", "superseded"}:
            raise ValueError(f"invalid completion effect terminal state {state!r}")
        prior_row = await self._effect_row(name)
        if prior_row is None:
            raise CompletionLeaseLost(
                f"completion effect {name!r} lost its completion term"
            )
        prior_detail = _json_object(prior_row["detail"]) or {}
        detail_json = _bounded_effect_detail(
            name,
            output,
            intent=prior_detail.get("intent", _NO_EFFECT_INTENT),
        )
        async with _connection(self._db) as conn:
            updated = await conn.fetchval(
                """
                WITH completed_effect AS (
                    UPDATE completion_effects AS effect
                    SET state = $6::text, completed_at = now(),
                        complete_by = NULL, detail = $4::jsonb,
                        error_code = NULL
                    WHERE effect.producer_kind = 'job_completion'
                      AND effect.producer_id = $1::uuid
                      AND effect.effect_name = $3::text
                      AND effect.state = 'pending'
                      AND effect.complete_by > now()
                      AND EXISTS (
                          SELECT 1 FROM job_completion_commands AS command
                          WHERE command.id = $1::uuid
                            AND command.state = 'finalizing'
                            AND command.finalizing_by = $2::text
                            AND command.lease_expires_at > now()
                      )
                    RETURNING 1
                ), shortened_command AS (
                    UPDATE job_completion_commands AS command
                    SET lease_expires_at = LEAST(
                        command.deadline_at,
                        now() + make_interval(secs => $5::float8)
                    )
                    WHERE command.id = $1::uuid
                      AND command.state = 'finalizing'
                      AND command.finalizing_by = $2::text
                      AND command.lease_expires_at > now()
                      AND EXISTS (SELECT 1 FROM completed_effect)
                    RETURNING 1
                )
                SELECT 1 FROM completed_effect, shortened_command
                """,
                UUID(self.command_id),
                self.owner,
                name,
                detail_json,
                self._command_lease_seconds,
                state,
            )
        if updated is None:
            raise CompletionLeaseLost(
                f"completion effect {name!r} finished after its fenced deadline"
            )

    async def _complete(self, name: str, output: Any) -> None:
        await self._settle(name, output, state="done")

    async def _supersede_effect(self, name: str, output: Any) -> None:
        await self._settle(name, output, state="superseded")

    async def _pending_dependency(self, group: str) -> _GroupBlock | None:
        """Return one durable prerequisite block under this exact command term."""

        async with _connection(self._db) as conn:
            row = await conn.fetchrow(
                """
                SELECT MIN(effect.run_after) AS run_after,
                       BOOL_OR(effect.attempts >= effect.max_attempts) AS exhausted
                FROM completion_effects AS effect
                WHERE effect.producer_kind = 'job_completion'
                  AND effect.producer_id = $1::uuid
                  AND effect.effect_group = $3::text
                  AND effect.state = 'pending'
                  AND EXISTS (
                      SELECT 1 FROM job_completion_commands AS command
                      WHERE command.id = $1::uuid
                        AND command.state = 'finalizing'
                        AND command.finalizing_by = $2::text
                        AND command.lease_expires_at > now()
                  )
                HAVING COUNT(*) > 0
                """,
                UUID(self.command_id),
                self.owner,
                group,
            )
        if row is None:
            return None
        return _GroupBlock(
            CompletionEffectGroupBlocked(group, group),
            run_after=_row_value(row, "run_after"),
            exhausted=bool(_row_value(row, "exhausted", False)),
        )

    async def has_pending_group(self, group: str) -> bool:
        """Whether this exact command term still has a pending group member.

        Step 4 uses this after attempting every independently runnable product
        delivery.  Returning to the finalizer while any gated group is pending
        lets its existing release/park machinery own the retry; the workflow
        must not cross S17 first.
        """

        if group in self._blocked_groups:
            return True
        durable = await self._pending_dependency(group)
        if durable is None:
            return False
        self._blocked_groups[group] = durable
        return True

    async def _dependency_block(
        self, group: str, depends_on_groups: Sequence[str]
    ) -> _GroupBlock | None:
        """Resolve explicit dependencies without imposing a global chain."""

        for dependency in depends_on_groups:
            if dependency == group:
                raise CompletionEffectVersionError(
                    f"effect group {group!r} cannot depend on itself"
                )
            local = self._blocked_groups.get(dependency)
            if local is not None:
                return _GroupBlock(
                    CompletionEffectGroupBlocked(group, dependency),
                    run_after=local.run_after,
                    exhausted=local.exhausted,
                )
            durable = await self._pending_dependency(dependency)
            if durable is not None:
                return _GroupBlock(
                    CompletionEffectGroupBlocked(group, dependency),
                    run_after=durable.run_after,
                    exhausted=durable.exhausted,
                )
        return None

    @staticmethod
    async def _resolve_error_output(
        factory: EffectErrorOutput, exc: BaseException
    ) -> Any:
        output = factory(exc)
        if isinstance(output, Awaitable):
            output = await output
        return output

    async def _schedule_group_retry(
        self,
        *,
        name: str,
        group: str,
        exc: BaseException,
        output: Any,
    ) -> _GroupBlock:
        """Release every pending member of a group onto one retry clock.

        The callback's explicitly supplied fallback is retained on its own row
        so a pre-``run_after`` replay reconstructs the legacy branch result.
        Attempts remain per-row, while the delay and exhaustion decision use
        the group's maximum attempt number and the most restrictive budget.
        """

        prior_row = await self._effect_row(name)
        if prior_row is None:
            raise CompletionLeaseLost(f"completion effect {name!r} lost its retry term")
        prior_detail = _json_object(prior_row["detail"]) or {}
        detail_json = _bounded_effect_detail(
            name,
            output,
            intent=prior_detail.get("intent", _NO_EFFECT_INTENT),
            teardown_authorization=prior_detail.get(
                "teardown_authorization", _NO_EFFECT_INTENT
            ),
        )
        jitter = max(0.0, min(0.2, self._random_source() * 0.2))
        async with _connection(self._db) as conn:
            row = await conn.fetchrow(
                """
                WITH group_budget AS (
                    SELECT GREATEST(1, COALESCE(MAX(attempts), 1)) AS attempt
                    FROM completion_effects
                    WHERE producer_kind = 'job_completion'
                      AND producer_id = $1::uuid
                      AND effect_group = $4::text
                ), updated AS (
                    UPDATE completion_effects AS effect
                    SET attempts = GREATEST(effect.attempts, budget.attempt),
                        run_after = now() + make_interval(
                            secs => 5.0 * budget.attempt * (1.0 + $7::float8)
                        ),
                        complete_by = NULL,
                        detail = CASE
                            WHEN effect.effect_name = $3::text THEN $6::jsonb
                            ELSE effect.detail
                        END,
                        error_code = $5::text
                    FROM group_budget AS budget
                    WHERE effect.producer_kind = 'job_completion'
                      AND effect.producer_id = $1::uuid
                      AND effect.effect_group = $4::text
                      AND effect.state = 'pending'
                      AND EXISTS (
                          SELECT 1 FROM completion_effects AS current_effect
                          WHERE current_effect.producer_kind = 'job_completion'
                            AND current_effect.producer_id = $1::uuid
                            AND current_effect.effect_name = $3::text
                            AND current_effect.state = 'pending'
                            AND current_effect.complete_by > now()
                      )
                      AND EXISTS (
                          SELECT 1 FROM job_completion_commands AS command
                          WHERE command.id = $1::uuid
                            AND command.state = 'finalizing'
                            AND command.finalizing_by = $2::text
                            AND command.lease_expires_at > now()
                      )
                    RETURNING effect.run_after, effect.attempts,
                              effect.max_attempts
                )
                SELECT MIN(run_after) AS run_after,
                       BOOL_OR(attempts >= max_attempts) AS exhausted
                FROM updated
                HAVING COUNT(*) > 0
                """,
                UUID(self.command_id),
                self.owner,
                name,
                group,
                _error_code(exc),
                detail_json,
                jitter,
            )
        if row is None:
            raise CompletionLeaseLost(
                f"completion effect group {group!r} lost its retry term"
            )
        return _GroupBlock(
            exc,
            run_after=_row_value(row, "run_after"),
            exhausted=bool(_row_value(row, "exhausted", False)),
        )

    async def run(
        self,
        *,
        name: str,
        group: str,
        callback: EffectCallback,
        retry_on_error: bool = False,
        error_output: EffectErrorOutput | None = None,
        retry_if: EffectRetryPredicate | None = None,
        supersede_if: EffectSupersedePredicate | None = None,
        depends_on_groups: Sequence[str] = (),
        effect_timeout_seconds: float | None = None,
        command_lease_seconds: float | None = None,
        _record_error_on_failure: bool = True,
    ) -> Any:
        """Execute one stable effect, replaying its bounded result after restart."""

        if retry_on_error and error_output is None:
            raise ValueError("retryable completion effects require error_output")
        requested_effect_timeout = float(
            self._effect_lease_seconds
            if effect_timeout_seconds is None
            else effect_timeout_seconds
        )
        requested_command_lease = float(
            self._command_lease_seconds
            if command_lease_seconds is None
            else command_lease_seconds
        )
        if requested_effect_timeout <= EFFECT_WRITE_MARGIN_SECONDS:
            raise ValueError("completion effect timeout is too short")
        if requested_command_lease < (
            requested_effect_timeout + EFFECT_COMMAND_LEASE_GAP_SECONDS
        ):
            raise ValueError("completion command lease must outlive its effect timeout")

        # A failed member gates the rest of its own group.  Explicit cross-group
        # dependencies add only the domain ordering a caller declares; every
        # other group remains runnable during this workflow pass.
        block = self._blocked_groups.get(group)
        if block is None and depends_on_groups:
            block = await self._dependency_block(group, depends_on_groups)
        if block is not None:
            self._blocked_groups.setdefault(group, block)
            if not retry_on_error or error_output is None:
                raise block.reason
            return await self._resolve_error_output(error_output, block.reason)

        try:
            replayed, output, remaining_seconds, deferred = await self._prepare(
                name=name,
                group=group,
                effect_timeout_seconds=requested_effect_timeout,
                command_lease_seconds=requested_command_lease,
            )
        except CompletionEffectBudgetExhausted as exc:
            block = _GroupBlock(exc, exhausted=True)
            self._blocked_groups[group] = block
            if not retry_on_error or error_output is None:
                raise
            prior_row = await self._effect_row(name)
            prior_detail = (
                _json_object(prior_row["detail"]) if prior_row is not None else None
            ) or {}
            if "output" in prior_detail:
                return prior_detail["output"]
            return await self._resolve_error_output(error_output, exc)
        if replayed:
            if deferred:
                block = _GroupBlock(
                    CompletionEffectGroupBlocked(group, group),
                    run_after=_row_value(await self._effect_row(name), "run_after"),
                )
                self._blocked_groups[group] = block
            return output
        if remaining_seconds is None:
            raise CompletionLeaseLost(
                f"completion effect {name!r} has no fenced completion deadline"
            )
        # The DB computes this duration on the same clock that enforces the CAS;
        # app/DB wall-clock skew therefore cannot let an external call overrun
        # complete_by.  Leave a write margin for the terminal marker.
        timeout_seconds = float(remaining_seconds) - EFFECT_WRITE_MARGIN_SECONDS
        if timeout_seconds <= 0:
            raise CompletionLeaseLost(
                f"completion effect {name!r} has no safe execution interval"
            )
        try:
            async with asyncio.timeout(timeout_seconds):
                output = await callback()
        except BaseException as exc:
            # A transactional effect must let its outer transaction roll back
            # the domain write and intent together. Shielding a diagnostic here
            # creates a child task, which intentionally cannot inherit the
            # task-bound Postgres connection and would autocommit an orphan
            # error marker.
            if not _record_error_on_failure:
                raise
            # An authority loss is a whole-command disposition signal, never
            # a retryable product-delivery error.  Converting it into a group
            # retry would retain a stale marker/command and could publish S17
            # after a legitimate control writer won.
            if isinstance(exc, CompletionDispositionSuperseded):
                with suppress(Exception):
                    await asyncio.shield(self._record_error(name, exc))
                raise
            # Cancellation deliberately leaves intent pending.  Recording the
            # diagnostic is exact-term best effort and never grants a retry.
            if retry_on_error and isinstance(exc, Exception):
                assert error_output is not None
                output = await self._resolve_error_output(error_output, exc)
                block = await asyncio.shield(
                    self._schedule_group_retry(
                        name=name,
                        group=group,
                        exc=exc,
                        output=output,
                    )
                )
                self._blocked_groups[group] = block
                return output
            with suppress(Exception):
                await asyncio.shield(self._record_error(name, exc))
            raise
        if retry_if is not None and retry_if(output):
            exc = CompletionEffectRetryRequested(
                f"completion effect {name!r} returned a retryable result"
            )
            block = await self._schedule_group_retry(
                name=name,
                group=group,
                exc=exc,
                output=output,
            )
            self._blocked_groups[group] = block
            return output
        if supersede_if is not None and supersede_if(output):
            await self._supersede_effect(name, output)
            return output
        await self._complete(name, output)
        return output

    async def run_transactional(
        self,
        *,
        name: str,
        group: str,
        callback: EffectCallback,
        retry_on_error: bool = False,
        error_output: EffectErrorOutput | None = None,
        retry_if: EffectRetryPredicate | None = None,
        supersede_if: EffectSupersedePredicate | None = None,
        depends_on_groups: Sequence[str] = (),
        effect_timeout_seconds: float | None = None,
        command_lease_seconds: float | None = None,
    ) -> Any:
        """Run a Postgres-only effect and its progress marker atomically.

        ``PostgresDB.transaction_scope`` makes the existing database helpers
        called by ``callback`` reuse the same task-bound connection as the
        effect intent/completion writes. External I/O must never use this arm:
        a network call inside a database transaction would hold the command
        term and row locks for an unbounded interval.
        """

        if retry_on_error or error_output is not None or retry_if is not None:
            raise ValueError(
                "transactional completion effects cannot schedule effect-group "
                "retries; propagate the error for command-level retry"
            )

        transaction_scope = getattr(self._db, "transaction_scope", None)
        if transaction_scope is None:
            # Lightweight fakes and connection-level callers already execute
            # against one explicit connection; preserve their test contract.
            return await self.run(
                name=name,
                group=group,
                callback=callback,
                supersede_if=supersede_if,
                depends_on_groups=depends_on_groups,
                effect_timeout_seconds=effect_timeout_seconds,
                command_lease_seconds=command_lease_seconds,
                _record_error_on_failure=False,
            )
        async with transaction_scope():
            return await self.run(
                name=name,
                group=group,
                callback=callback,
                supersede_if=supersede_if,
                depends_on_groups=depends_on_groups,
                effect_timeout_seconds=effect_timeout_seconds,
                command_lease_seconds=command_lease_seconds,
                _record_error_on_failure=False,
            )


class CompletionFinalizer:
    """Claim, execute, retry and drain durable completion commands."""

    def __init__(
        self,
        db: Any,
        *,
        workflow: Workflow | None = None,
        code_version: str | None = None,
        leader_id: str | None = None,
        alert: AlertCallback | None = None,
        random_source: Callable[[], float] = random.random,
        command_lease_seconds: float = COMMAND_LEASE_SECONDS,
        heartbeat_seconds: float = COMMAND_HEARTBEAT_SECONDS,
        effect_lease_seconds: float = EFFECT_LEASE_SECONDS,
        preclaim: PreclaimCallback | None = None,
    ) -> None:
        self.db = db
        self.workflow = workflow
        self.code_version = code_version or COMPLETION_CODE_VERSION
        self.supported_code_versions = (
            (str(code_version),)
            if code_version is not None
            else COMPLETION_SUPPORTED_CODE_VERSIONS
        )
        self.leader_id = leader_id or f"completion-finalizer-{uuid4()}"
        self.alert = alert
        self.random_source = random_source
        self.command_lease_seconds = float(command_lease_seconds)
        self.heartbeat_seconds = float(heartbeat_seconds)
        self.effect_lease_seconds = float(effect_lease_seconds)
        self.preclaim = preclaim

    async def _take_retry_token(self) -> bool:
        """Process-global admission bound for background retry work.

        Per-command run_after prevents one row from spinning.  This bucket
        bounds aggregate external pressure after a fleet-wide dependency
        outage.  Fresh exact-ID inline work does not consume retry capacity;
        every background drain claim does.
        """

        global _RETRY_BUCKET_TOKENS, _RETRY_BUCKET_UPDATED

        loop = asyncio.get_running_loop()
        lock = _RETRY_BUCKET_LOCKS.setdefault(id(loop), asyncio.Lock())
        async with lock:
            now = time.monotonic()
            elapsed = max(0.0, now - _RETRY_BUCKET_UPDATED)
            _RETRY_BUCKET_TOKENS = min(
                RETRY_BUCKET_CAPACITY,
                _RETRY_BUCKET_TOKENS + elapsed * RETRY_BUCKET_REFILL_PER_SECOND,
            )
            _RETRY_BUCKET_UPDATED = now
            if _RETRY_BUCKET_TOKENS < 1.0:
                return False
            _RETRY_BUCKET_TOKENS -= 1.0
            return True

    async def _alert(self, message: str) -> None:
        logger.error(message)
        if self.alert is None:
            return
        result = self.alert(message)
        if isinstance(result, Awaitable):
            await result

    async def _fetch_command(self, command_id: str) -> dict[str, Any] | None:
        async with _connection(self.db) as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM job_completion_commands WHERE id = $1::uuid
                """,
                UUID(str(command_id)),
            )
        return _command_dict(row) if row is not None else None

    async def _park_unclaimable(
        self,
        command_id: str,
        *,
        error_code: str,
        version_mismatch: bool = False,
        capability_mismatch: bool = False,
    ) -> dict[str, Any] | None:
        async with _connection(self.db) as conn:
            row = await conn.fetchrow(
                """
                UPDATE job_completion_commands AS command
                SET state = 'parked', error_code = $2::text,
                    finalizing_by = NULL, lease_expires_at = NULL
                WHERE command.id = $1::uuid
                  AND (
                      command.state = 'pending'
                      OR (command.state = 'finalizing'
                          AND (command.lease_expires_at IS NULL
                               OR command.lease_expires_at < now()))
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM job_completion_commands AS predecessor
                      WHERE predecessor.job_id = command.job_id
                        AND predecessor.report_seq < command.report_seq
                        AND predecessor.state IN ('pending', 'finalizing', 'parked')
                  )
                  AND (
                      ($3::boolean AND (
                          NOT (command.code_version = ANY($4::text[]))
                          OR ($5::boolean AND NOT (
                              (command.code_version = $6::text
                               AND command.status_reorder_enabled = false)
                              OR
                              (command.code_version = $7::text
                               AND command.status_reorder_enabled = true)
                          ))
                      ))
                      OR (NOT $3::boolean AND (
                          command.deadline_at <= now()
                          OR command.attempts >= command.max_attempts
                      ))
                  )
                RETURNING command.*
                """,
                UUID(str(command_id)),
                error_code[:128],
                version_mismatch,
                list(self.supported_code_versions),
                capability_mismatch,
                COMPLETION_CODE_VERSION,
                COMPLETION_STATUS_REORDER_CODE_VERSION,
            )
        return _command_dict(row) if row is not None else None

    async def _claim(
        self, command_id: str, *, inline: bool
    ) -> tuple[dict[str, Any] | None, str | None]:
        command = await self._fetch_command(command_id)
        if command is None:
            return None, None
        state = str(command["state"])
        if state in {"done", "parked", "superseded", "force_resolved"}:
            return command, None

        version_supported = str(command["code_version"]) in self.supported_code_versions
        capability_matches = (
            str(command["code_version"]) == COMPLETION_STATUS_REORDER_CODE_VERSION
        ) == bool(command.get("status_reorder_enabled", False))
        if not version_supported or not capability_matches:
            parked = await self._park_unclaimable(
                command_id,
                error_code="code_version_mismatch",
                version_mismatch=True,
                capability_mismatch=not capability_matches,
            )
            if parked is not None:
                await self._alert(
                    "completion command "
                    f"{command_id} parked: stored code version "
                    f"{command['code_version']!r} not in "
                    f"{self.supported_code_versions!r}"
                )
                return parked, None
            return await self._fetch_command(command_id), None

        parked = await self._park_unclaimable(
            command_id, error_code="deadline_or_attempts_exhausted"
        )
        if parked is not None:
            await self._alert(
                f"completion command {command_id} parked at deadline/retry cap"
            )
            return parked, None

        owner = str(uuid4())
        async with _connection(self.db) as conn:
            row = await conn.fetchrow(
                """
                UPDATE job_completion_commands AS command
                SET state = 'finalizing', attempts = command.attempts + 1,
                    finalizing_by = $2::text,
                    lease_expires_at = LEAST(
                        command.deadline_at,
                        now() + make_interval(secs => $4::float8)
                    )
                WHERE command.id = $1::uuid
                  AND command.code_version = ANY($3::text[])
                  AND (
                      (command.code_version = $6::text
                       AND command.status_reorder_enabled = false)
                      OR
                      (command.code_version = $7::text
                       AND command.status_reorder_enabled = true)
                  )
                  AND command.deadline_at > now()
                  AND command.attempts < command.max_attempts
                  AND (
                      command.state = 'pending'
                      OR (command.state = 'finalizing'
                          AND (command.lease_expires_at IS NULL
                               OR command.lease_expires_at < now()))
                  )
                  AND ($5::boolean OR command.run_after <= now())
                  AND NOT EXISTS (
                      SELECT 1 FROM job_completion_commands AS predecessor
                      WHERE predecessor.job_id = command.job_id
                        AND predecessor.report_seq < command.report_seq
                        AND predecessor.state IN ('pending', 'finalizing', 'parked')
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM completion_effects AS effect
                      WHERE effect.producer_kind = 'job_completion'
                        AND effect.producer_id = command.id
                        AND effect.state = 'pending'
                        AND effect.complete_by > now()
                  )
                RETURNING command.*
                """,
                UUID(str(command_id)),
                owner,
                list(self.supported_code_versions),
                self.command_lease_seconds,
                inline,
                COMPLETION_CODE_VERSION,
                COMPLETION_STATUS_REORDER_CODE_VERSION,
            )
        if row is None:
            return await self._fetch_command(command_id), None
        return _command_dict(row), owner

    async def _supersede_locked(
        self,
        conn: Any,
        *,
        command: Mapping[str, Any],
        owner: str,
        signal: CompletionDispositionSuperseded,
    ) -> FinalizationResult:
        """Terminalize one whole command while its jobs row is locked."""

        command_id = str(command["id"])
        job_id = str(command["job_id"])
        job = await conn.fetchrow(
            """
            SELECT status::text AS status, context
            FROM jobs
            WHERE id = $1::uuid
            """,
            UUID(job_id),
        )
        if job is None:
            raise CompletionLeaseLost(
                f"completion command {command_id} lost its jobs row"
            )
        higher_report_seq = await conn.fetchval(
            """
            SELECT MIN(report_seq)
            FROM job_completion_commands
            WHERE job_id = $1::uuid
              AND report_seq > $2::bigint
              AND state <> 'superseded'
            """,
            UUID(job_id),
            int(command["report_seq"]),
        )
        job_context = _json_object(_row_value(job, "context")) or {}
        live_decision = job_context.get("completion_decision")
        current_status = str(_row_value(job, "status", "") or "")
        accepted_decision_id = accepted_completion_decision_tool_call_id(
            _json_object(command.get("payload")) or {}
        )
        decision_disposition = "not_applicable"
        if (
            higher_report_seq is None
            and current_status not in {"completed", "failed", "cancelled"}
            and isinstance(live_decision, Mapping)
        ):
            live_decision_id = str(live_decision.get("tool_call_id") or "").strip()
            if accepted_decision_id and live_decision_id == accepted_decision_id:
                cleared = await conn.execute(
                    """
                    UPDATE jobs
                    SET context = COALESCE(context, '{}'::jsonb)
                                  - 'completion_decision',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = $1::uuid
                      AND status::text = $2::text
                      AND context #>> '{completion_decision,tool_call_id}' = $3::text
                    """,
                    UUID(job_id),
                    current_status,
                    accepted_decision_id,
                )
                if cleared != "UPDATE 1":
                    raise CompletionLeaseLost(
                        f"completion command {command_id} lost its decision-void term"
                    )
                decision_disposition = "voided_exact_acceptance"
            else:
                # A live decision that cannot be tied to this command may be a
                # newer legitimate completion.  Parking preserves both the
                # command and decision and leaves the existing completion
                # monitor/operator path in charge; guessing here would discard
                # the only durable statement of completed work.
                parked = await conn.fetchrow(
                    """
                    UPDATE job_completion_commands
                    SET state = 'parked',
                        error_code = 'completion_decision_authority_unresolved',
                        finalizing_by = NULL, lease_expires_at = NULL
                    WHERE id = $1::uuid
                      AND state = 'finalizing'
                      AND finalizing_by = $2::text
                      AND lease_expires_at > now()
                      AND deadline_at > now()
                    RETURNING *
                    """,
                    UUID(command_id),
                    owner,
                )
                if parked is None:
                    raise CompletionLeaseLost(
                        f"completion command {command_id} lost its decision-park term"
                    )
                return FinalizationResult(
                    command_id=command_id,
                    state="parked",
                    disposition="parked",
                    error_code="completion_decision_authority_unresolved",
                )

        abandoned_rows = await conn.fetch(
            """
            SELECT effect_name, state, detail
            FROM completion_effects
            WHERE producer_kind = 'job_completion'
              AND producer_id = $1::uuid
              AND state NOT IN ('done', 'superseded')
            ORDER BY effect_name
            """,
            UUID(command_id),
        )
        for effect in abandoned_rows:
            if str(effect["effect_name"]) != "workspace_archive_teardown":
                continue
            detail = _json_object(_row_value(effect, "detail")) or {}
            authorization = detail.get("teardown_authorization")
            if (
                str(_row_value(effect, "state", "")) == "pending"
                and isinstance(authorization, Mapping)
                and authorization.get("active") is True
            ):
                raise CompletionTeardownSupersedeBlocked(
                    f"completion command {command_id} cannot abandon an "
                    "authorized workspace teardown"
                )
        abandoned_effects = [str(row["effect_name"]) for row in abandoned_rows]
        outcome = {
            "status": "superseded",
            "job_id": str(command["job_id"]),
            "report_seq": int(command["report_seq"]),
            "reason": signal.reason,
            "accepted_job_status": command.get("accepted_job_status"),
            "expected_entry_statuses": list(signal.expected_statuses),
            "observed_status": signal.observed_status,
            "winning_report_seq": (
                int(higher_report_seq) if higher_report_seq is not None else None
            ),
            "completion_decision_disposition": decision_disposition,
            "abandoned_effects": abandoned_effects,
        }
        outcome_json = json.dumps(
            outcome,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
            allow_nan=False,
        )
        row = await conn.fetchrow(
            """
            UPDATE job_completion_commands
            SET state = 'superseded', outcome = $3::jsonb,
                finalized_at = now(), error_code = $4::text,
                finalizing_by = NULL, lease_expires_at = NULL
            WHERE id = $1::uuid
              AND state = 'finalizing'
              AND finalizing_by = $2::text
              AND lease_expires_at > now()
              AND deadline_at > now()
            RETURNING *
            """,
            UUID(command_id),
            owner,
            outcome_json,
            signal.reason,
        )
        if row is None:
            raise CompletionLeaseLost(
                f"completion command {command_id} lost its supersede term"
            )
        return FinalizationResult(
            command_id=command_id,
            state="superseded",
            disposition="superseded",
            outcome=outcome,
            error_code=signal.reason,
        )

    async def _resolve_entry_authority(
        self,
        command: Mapping[str, Any],
        owner: str,
    ) -> str | FinalizationResult:
        """Resolve the jobs status this ordered command is allowed to apply.

        Fresh work uses the status captured under the admission lock. A later
        report may instead consume the immediate predecessor's proven output.
        A retry that already completed S1 keeps that command-owned snapshot and
        lets the legacy workflow's exact-effect proof reconcile later writes.
        """

        command_id = str(command["id"])
        job_id = str(command["job_id"])
        async with _connection(self.db) as conn:
            async with conn.transaction():
                job = await conn.fetchrow(
                    """
                    SELECT status::text AS status
                    FROM jobs
                    WHERE id = $1::uuid
                    FOR UPDATE
                    """,
                    UUID(job_id),
                )
                if job is None:
                    raise CompletionLeaseLost(
                        f"completion command {command_id} lost its jobs row"
                    )
                authority = await conn.fetchrow(
                    """
                    SELECT command.id, command.job_id, command.report_seq,
                           command.accepted_job_status, command.payload,
                           s1.state AS s1_state, s1.detail AS s1_detail,
                           predecessor.report_seq AS predecessor_report_seq,
                           predecessor.state AS predecessor_state,
                           predecessor.outcome AS predecessor_outcome
                    FROM job_completion_commands AS command
                    LEFT JOIN completion_effects AS s1
                      ON s1.producer_kind = 'job_completion'
                     AND s1.producer_id = command.id
                     AND s1.effect_name = 'late_callback_guard'
                     AND s1.effect_group = 'entry'
                    LEFT JOIN LATERAL (
                        SELECT prior.report_seq, prior.state, prior.outcome
                        FROM job_completion_commands AS prior
                        WHERE prior.job_id = command.job_id
                          AND prior.report_seq < command.report_seq
                        ORDER BY prior.report_seq DESC
                        LIMIT 1
                    ) AS predecessor ON TRUE
                    WHERE command.id = $1::uuid
                      AND command.job_id = $2::uuid
                      AND command.state = 'finalizing'
                      AND command.finalizing_by = $3::text
                      AND command.lease_expires_at > now()
                      AND command.deadline_at > now()
                    """,
                    UUID(command_id),
                    UUID(job_id),
                    owner,
                )
                if authority is None:
                    raise CompletionLeaseLost(
                        f"completion command {command_id} lost its authority term"
                    )

                current_status = str(job["status"] or "")
                accepted_status = str(
                    _row_value(authority, "accepted_job_status", "") or ""
                ).strip()
                s1_detail = _json_object(_row_value(authority, "s1_detail")) or {}
                s1_output = s1_detail.get("output")
                s1_entry_status = (
                    str(s1_output.get("entry_status") or "").strip()
                    if isinstance(s1_output, Mapping)
                    else ""
                )
                if (
                    str(_row_value(authority, "s1_state", "")) == "done"
                    and s1_entry_status
                ):
                    return s1_entry_status

                predecessor_outcome = (
                    _json_object(_row_value(authority, "predecessor_outcome")) or {}
                )
                predecessor_status = str(
                    predecessor_outcome.get("new_status") or ""
                ).strip()
                predecessor_proved = (
                    str(_row_value(authority, "predecessor_state", "")) == "done"
                    and predecessor_status
                )
                predecessor_exists = (
                    _row_value(authority, "predecessor_report_seq") is not None
                )
                if accepted_status and current_status == accepted_status:
                    # Admission captured this command's own status while the
                    # jobs row was locked.  That proof remains authoritative
                    # when the job still has the same status, including the
                    # feedback-round topology where a completed predecessor
                    # legitimately returned the job to processing before this
                    # report was accepted.
                    return accepted_status
                if predecessor_exists:
                    # If this command's own snapshot no longer matches,
                    # acceptance may have happened before the lower report
                    # finalized. A proven predecessor output can then supply
                    # sequential authority; an unproved output cannot.
                    if predecessor_proved and current_status == predecessor_status:
                        return predecessor_status

                expected_statuses = [accepted_status]
                if predecessor_proved:
                    expected_statuses.append(predecessor_status)
                signal = CompletionDispositionSuperseded(
                    observed_status=current_status,
                    expected_statuses=expected_statuses,
                )
                authoritative_command = {
                    "id": str(authority["id"]),
                    "job_id": str(authority["job_id"]),
                    "report_seq": int(authority["report_seq"]),
                    "accepted_job_status": _row_value(authority, "accepted_job_status"),
                    "payload": _json_object(_row_value(authority, "payload")) or {},
                }
                return await self._supersede_locked(
                    conn,
                    command=authoritative_command,
                    owner=owner,
                    signal=signal,
                )

    async def _supersede(
        self,
        command: Mapping[str, Any],
        owner: str,
        signal: CompletionDispositionSuperseded,
    ) -> FinalizationResult:
        """Settle a workflow-detected status race under the exact live term."""

        command_id = str(command["id"])
        job_id = str(command["job_id"])
        async with _connection(self.db) as conn:
            async with conn.transaction():
                job = await conn.fetchrow(
                    """
                    SELECT status::text AS status
                    FROM jobs
                    WHERE id = $1::uuid
                    FOR UPDATE
                    """,
                    UUID(job_id),
                )
                if job is None:
                    raise CompletionLeaseLost(
                        f"completion command {command_id} lost its jobs row"
                    )
                exact = await conn.fetchrow(
                    """
                    SELECT id, job_id, report_seq, accepted_job_status, payload
                    FROM job_completion_commands
                    WHERE id = $1::uuid
                      AND job_id = $2::uuid
                      AND state = 'finalizing'
                      AND finalizing_by = $3::text
                      AND lease_expires_at > now()
                      AND deadline_at > now()
                    """,
                    UUID(command_id),
                    UUID(job_id),
                    owner,
                )
                if exact is None:
                    raise CompletionLeaseLost(
                        f"completion command {command_id} lost its supersede term"
                    )
                locked_signal = CompletionDispositionSuperseded(
                    observed_status=str(job["status"] or ""),
                    expected_statuses=signal.expected_statuses,
                    reason=signal.reason,
                )
                return await self._supersede_locked(
                    conn,
                    command=exact,
                    owner=owner,
                    signal=locked_signal,
                )

    async def _renew_command(self, command_id: str, owner: str) -> bool:
        async with _connection(self.db) as conn:
            renewed = await conn.fetchval(
                """
                UPDATE job_completion_commands
                SET lease_expires_at = LEAST(
                    deadline_at,
                    GREATEST(
                        lease_expires_at,
                        now() + make_interval(secs => $3::float8)
                    )
                )
                WHERE id = $1::uuid
                  AND state = 'finalizing'
                  AND finalizing_by = $2::text
                  AND lease_expires_at >= now()
                  AND deadline_at > now()
                RETURNING 1
                """,
                UUID(str(command_id)),
                owner,
                self.command_lease_seconds,
            )
        return renewed is not None

    async def _heartbeat_command(self, command_id: str, owner: str) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_seconds)
            if not await self._renew_command(command_id, owner):
                raise CompletionLeaseLost(
                    f"completion command {command_id} lost its renewable lease"
                )

    async def _run_owned_workflow(
        self,
        runner: CompletionEffectRunner,
        workflow: Workflow,
    ) -> dict[str, Any]:
        work = asyncio.create_task(workflow(runner))
        heartbeat = asyncio.create_task(
            self._heartbeat_command(runner.command_id, runner.owner)
        )
        try:
            done, _ = await asyncio.wait(
                {work, heartbeat}, return_when=asyncio.FIRST_COMPLETED
            )
            if heartbeat in done:
                exc = heartbeat.exception()
                if exc is not None:
                    work.cancel()
                    with suppress(asyncio.CancelledError):
                        await work
                    raise exc
                raise CompletionLeaseLost(
                    f"completion command {runner.command_id} heartbeat stopped"
                )
            outcome = work.result()
            if not isinstance(outcome, Mapping):
                raise CompletionFinalizerError(
                    "completion workflow must return a JSON-object outcome"
                )
            return dict(outcome)
        finally:
            for task in (work, heartbeat):
                if not task.done():
                    task.cancel()
            await asyncio.gather(work, heartbeat, return_exceptions=True)

    async def _finish(
        self, command_id: str, owner: str, outcome: Mapping[str, Any]
    ) -> dict[str, Any]:
        outcome_json = json.dumps(
            dict(outcome),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
            allow_nan=False,
        )
        async with _connection(self.db) as conn:
            row = await conn.fetchrow(
                """
                UPDATE job_completion_commands
                SET state = 'done', outcome = $3::jsonb, finalized_at = now(),
                    error_code = NULL, finalizing_by = NULL,
                    lease_expires_at = NULL
                WHERE id = $1::uuid
                  AND state = 'finalizing'
                  AND finalizing_by = $2::text
                  AND lease_expires_at > now()
                  AND deadline_at > now()
                  AND NOT EXISTS (
                      SELECT 1 FROM completion_effects AS effect
                      WHERE effect.producer_kind = 'job_completion'
                        AND effect.producer_id = job_completion_commands.id
                        AND effect.state NOT IN ('done', 'superseded')
                  )
                RETURNING *
                """,
                UUID(str(command_id)),
                owner,
                outcome_json,
            )
        if row is None:
            raise CompletionLeaseLost(
                f"completion command {command_id} lost its finish term"
            )
        return _command_dict(row)

    async def _release_for_pending_effects(
        self,
        command_id: str,
        owner: str,
        outcome: Mapping[str, Any],
    ) -> FinalizationResult:
        """Release a healthy command while independent effect groups retry.

        A soft group retry is progress, not a failed command execution, so the
        command-attempt increment taken by the claim is returned here.  The
        command-level budget remains reserved for crashes/fatal workflow
        failures, while each effect group consumes its own row budget.
        """

        async with _connection(self.db) as conn:
            async with conn.transaction():
                summary = await conn.fetchrow(
                    """
                    SELECT MIN(effect.run_after) AS run_after,
                           BOOL_OR(
                               effect.attempts >= effect.max_attempts
                           ) AS exhausted
                    FROM completion_effects AS effect
                    WHERE effect.producer_kind = 'job_completion'
                      AND effect.producer_id = $1::uuid
                      AND effect.state = 'pending'
                      AND EXISTS (
                          SELECT 1 FROM job_completion_commands AS command
                          WHERE command.id = $1::uuid
                            AND command.state = 'finalizing'
                            AND command.finalizing_by = $2::text
                            AND command.lease_expires_at > now()
                      )
                    HAVING COUNT(*) > 0
                    """,
                    UUID(str(command_id)),
                    owner,
                )
                if summary is None:
                    raise CompletionLeaseLost(
                        f"completion command {command_id} has no pending effect "
                        "under its retry term"
                    )

                exhausted = bool(_row_value(summary, "exhausted", False))
                if exhausted:
                    row = await conn.fetchrow(
                        """
                        UPDATE job_completion_commands
                        SET state = 'parked',
                            error_code = 'effect_group_attempts_exhausted',
                            finalizing_by = NULL,
                            lease_expires_at = NULL
                        WHERE id = $1::uuid
                          AND state = 'finalizing'
                          AND finalizing_by = $2::text
                          AND lease_expires_at > now()
                          AND EXISTS (
                              SELECT 1 FROM completion_effects AS effect
                              WHERE effect.producer_kind = 'job_completion'
                                AND effect.producer_id = $1::uuid
                                AND effect.state = 'pending'
                                AND effect.attempts >= effect.max_attempts
                          )
                        RETURNING *
                        """,
                        UUID(str(command_id)),
                        owner,
                    )
                else:
                    row = await conn.fetchrow(
                        """
                        UPDATE job_completion_commands
                        SET state = 'pending',
                            attempts = GREATEST(attempts - 1, 0),
                            run_after = $3::timestamptz,
                            error_code = NULL,
                            finalizing_by = NULL,
                            lease_expires_at = NULL
                        WHERE id = $1::uuid
                          AND state = 'finalizing'
                          AND finalizing_by = $2::text
                          AND lease_expires_at > now()
                          AND deadline_at > now()
                          AND EXISTS (
                              SELECT 1 FROM completion_effects AS effect
                              WHERE effect.producer_kind = 'job_completion'
                                AND effect.producer_id = $1::uuid
                                AND effect.state = 'pending'
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM completion_effects AS effect
                              WHERE effect.producer_kind = 'job_completion'
                                AND effect.producer_id = $1::uuid
                                AND effect.state = 'pending'
                                AND effect.attempts >= effect.max_attempts
                          )
                        RETURNING *
                        """,
                        UUID(str(command_id)),
                        owner,
                        summary["run_after"],
                    )
                    if row is None:
                        # Crossing the absolute deadline parks rather than
                        # turning a successful group pass into an owner-loss.
                        row = await conn.fetchrow(
                            """
                            UPDATE job_completion_commands
                            SET state = 'parked',
                                error_code = 'completion_deadline_exhausted',
                                finalizing_by = NULL,
                                lease_expires_at = NULL
                            WHERE id = $1::uuid
                              AND state = 'finalizing'
                              AND finalizing_by = $2::text
                              AND lease_expires_at > now()
                              AND deadline_at <= now()
                            RETURNING *
                            """,
                            UUID(str(command_id)),
                            owner,
                        )
                        exhausted = row is not None

        if row is None:
            raise CompletionLeaseLost(
                f"completion command {command_id} lost its effect-group term"
            )
        command = _command_dict(row)
        if exhausted or command["state"] == "parked":
            code = str(command.get("error_code") or "effect_groups_parked")
            await self._alert(f"completion command {command_id} parked after {code}")
            return FinalizationResult(
                command_id=command_id,
                state="parked",
                disposition="parked",
                error_code=code,
            )
        return FinalizationResult(
            command_id=command_id,
            state="pending",
            disposition="effects_pending",
            outcome=dict(outcome),
            run_after=command.get("run_after"),
        )

    async def _retry_or_park(
        self, command_id: str, owner: str, exc: BaseException
    ) -> FinalizationResult:
        code = _error_code(exc)
        async with _connection(self.db) as conn:
            parked = await conn.fetchrow(
                """
                UPDATE job_completion_commands
                SET state = 'parked', error_code = $3::text,
                    finalizing_by = NULL, lease_expires_at = NULL
                WHERE id = $1::uuid
                  AND state = 'finalizing'
                  AND finalizing_by = $2::text
                  AND lease_expires_at > now()
                  AND (attempts >= max_attempts OR deadline_at <= now())
                RETURNING *
                """,
                UUID(str(command_id)),
                owner,
                code,
            )
            command = None
            if parked is None:
                command = await conn.fetchrow(
                    """
                UPDATE job_completion_commands
                SET state = 'pending',
                    run_after = now() + make_interval(
                        secs => 5.0 * attempts * (1.0 + $3::float8)
                    ),
                    error_code = NULL, finalizing_by = NULL,
                    lease_expires_at = NULL
                WHERE id = $1::uuid
                  AND state = 'finalizing'
                  AND finalizing_by = $2::text
                  AND lease_expires_at > now()
                  AND attempts < max_attempts
                  AND deadline_at > now()
                RETURNING *
                """,
                    UUID(str(command_id)),
                    owner,
                    max(0.0, min(0.2, self.random_source() * 0.2)),
                )
        if parked is not None:
            await self._alert(f"completion command {command_id} parked after {code}")
            return FinalizationResult(
                command_id=command_id,
                state="parked",
                disposition="parked",
                error_code=code,
            )
        if command is None:
            raise CompletionLeaseLost(
                f"completion command {command_id} lost its retry term"
            )
        command_dict = _command_dict(command)
        logger.warning(
            "completion command %s released for retry after %s",
            command_id,
            code,
        )
        return FinalizationResult(
            command_id=command_id,
            state="pending",
            disposition="retry",
            run_after=command_dict.get("run_after"),
            error_code=code,
        )

    async def finalize_command(
        self,
        command_id: str,
        *,
        callback: Workflow | None = None,
        inline: bool = True,
    ) -> FinalizationResult:
        """Finalize an accepted command or report why it was not claimable."""

        command_id = str(UUID(str(command_id)))
        if self.preclaim is not None:
            preclaim_result = await self.preclaim(command_id)
            if getattr(preclaim_result, "disposition", None) == "superseded":
                command = await self._fetch_command(command_id)
                if command is None:
                    return FinalizationResult(
                        command_id=command_id,
                        state="missing",
                        disposition="missing",
                    )
                state = str(command["state"])
                return FinalizationResult(
                    command_id=command_id,
                    state=state,
                    disposition="terminal",
                    outcome=_json_object(command.get("outcome")),
                    run_after=command.get("run_after"),
                    error_code=command.get("error_code"),
                )
        command, owner = await self._claim(command_id, inline=inline)
        if command is None:
            return FinalizationResult(
                command_id=command_id,
                state="missing",
                disposition="missing",
            )
        state = str(command["state"])
        if owner is None:
            return FinalizationResult(
                command_id=command_id,
                state=state,
                disposition=(
                    "terminal"
                    if state in {"done", "superseded", "force_resolved"}
                    else "busy"
                ),
                outcome=_json_object(command.get("outcome")),
                run_after=command.get("run_after"),
                error_code=command.get("error_code"),
            )

        entry_authority = await self._resolve_entry_authority(command, owner)
        if isinstance(entry_authority, FinalizationResult):
            return entry_authority
        command["resolved_entry_status"] = entry_authority

        workflow = callback or self.workflow
        if workflow is None:
            raise CompletionFinalizerError("no completion workflow is configured")
        runner = CompletionEffectRunner(
            self.db,
            command=command,
            owner=owner,
            effect_lease_seconds=self.effect_lease_seconds,
            command_lease_seconds=self.command_lease_seconds,
            random_source=self.random_source,
        )
        try:
            outcome = await self._run_owned_workflow(runner, workflow)
            if runner.requires_retry:
                return await self._release_for_pending_effects(
                    command_id, owner, outcome
                )
            finished = await self._finish(command_id, owner, outcome)
        except asyncio.CancelledError:
            # Leave the exact claim live.  The successor waits for expiry and
            # the effect complete_by fence before probing/retrying.
            logger.warning(
                "completion command %s cancelled while finalizing", command_id
            )
            raise
        except CompletionDispositionSuperseded as signal:
            try:
                return await self._supersede(command, owner, signal)
            except CompletionTeardownSupersedeBlocked as exc:
                # S36's durable authorization may outlive both the effect and
                # command clocks. Never terminalize its command while that
                # external callback could still be reconciling.
                return await self._retry_or_park(command_id, owner, exc)
        except CompletionTeardownSupersedeBlocked as exc:
            return await self._retry_or_park(command_id, owner, exc)
        except CompletionLeaseLost:
            raise
        except Exception as exc:
            return await self._retry_or_park(command_id, owner, exc)
        return FinalizationResult(
            command_id=command_id,
            state="done",
            disposition="done",
            outcome=_json_object(finished.get("outcome")) or dict(outcome),
        )

    async def _candidate_id(self) -> str | None:
        async with _connection(self.db) as conn:
            value = await conn.fetchval(
                """
                SELECT command.id
                FROM job_completion_commands AS command
                WHERE (
                    (command.state = 'pending' AND command.run_after <= now())
                    OR (command.state = 'finalizing'
                        AND (command.lease_expires_at IS NULL
                             OR command.lease_expires_at < now()))
                    OR (command.state IN ('pending', 'finalizing')
                        AND command.deadline_at <= now())
                )
                  AND NOT EXISTS (
                      SELECT 1 FROM job_completion_commands AS predecessor
                      WHERE predecessor.job_id = command.job_id
                        AND predecessor.report_seq < command.report_seq
                        AND predecessor.state IN ('pending', 'finalizing', 'parked')
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM completion_effects AS effect
                      WHERE effect.producer_kind = 'job_completion'
                        AND effect.producer_id = command.id
                        AND effect.state = 'pending'
                        AND effect.complete_by > now()
                  )
                ORDER BY command.run_after, command.reported_at,
                         command.job_id, command.report_seq
                LIMIT 1
                """
            )
        return str(value) if value is not None else None

    async def acquire_leader(self) -> LeaderTerm | None:
        async with _connection(self.db) as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    DELETE FROM completion_finalizer_leases
                    WHERE lease_name = $1::text AND expires_at < now()
                    """,
                    LEADER_LEASE_NAME,
                )
                row = await conn.fetchrow(
                    """
                    INSERT INTO completion_finalizer_leases (
                        lease_name, leader_id, elected_at, expires_at
                    ) VALUES (
                        $1::text, $2::text, now(),
                        now() + make_interval(secs => $3::float8)
                    )
                    ON CONFLICT (lease_name) DO NOTHING
                    RETURNING elected_at
                    """,
                    LEADER_LEASE_NAME,
                    self.leader_id,
                    LEADER_LEASE_SECONDS,
                )
        if row is None:
            return None
        return LeaderTerm(self.leader_id, row["elected_at"])

    async def renew_leader(self, term: LeaderTerm) -> bool:
        async with _connection(self.db) as conn:
            renewed = await conn.fetchval(
                """
                UPDATE completion_finalizer_leases
                SET expires_at = now() + make_interval(secs => $4::float8)
                WHERE lease_name = $1::text
                  AND leader_id = $2::text
                  AND elected_at = $3::timestamptz
                  AND expires_at >= now()
                RETURNING 1
                """,
                LEADER_LEASE_NAME,
                term.leader_id,
                term.elected_at,
                LEADER_LEASE_SECONDS,
            )
        return renewed is not None

    async def release_leader(self, term: LeaderTerm) -> bool:
        async with _connection(self.db) as conn:
            result = await conn.execute(
                """
                DELETE FROM completion_finalizer_leases
                WHERE lease_name = $1::text
                  AND leader_id = $2::text
                  AND elected_at = $3::timestamptz
                """,
                LEADER_LEASE_NAME,
                term.leader_id,
                term.elected_at,
            )
        return str(result).endswith(" 1")

    async def _heartbeat_leader(
        self, term: LeaderTerm, lost: asyncio.Event, shutdown: asyncio.Event
    ) -> None:
        while not shutdown.is_set() and not lost.is_set():
            try:
                await asyncio.wait_for(
                    shutdown.wait(), timeout=LEADER_HEARTBEAT_SECONDS
                )
                return
            except TimeoutError:
                pass
            try:
                renewed = await self.renew_leader(term)
            except Exception:
                # Renewal uncertainty is leadership loss.  Continuing to scan
                # after a database error would let this process act on a term
                # that may already have expired and been replaced.
                logger.exception(
                    "completion finalizer leader renewal failed; abandoning "
                    "leader=%s elected_at=%s",
                    term.leader_id,
                    term.elected_at.isoformat(),
                )
                lost.set()
                return
            if not renewed:
                lost.set()
                return

    async def run_drain(
        self,
        shutdown_event: asyncio.Event,
        *,
        drain_commands: bool = True,
        background_sweep: Callable[[], Awaitable[Any]] | None = None,
    ) -> None:
        """Elect and drain until shutdown; safe under a dual-leader window."""

        while not shutdown_event.is_set():
            try:
                term = await self.acquire_leader()
            except Exception:
                logger.exception(
                    "completion finalizer leader election failed; retrying"
                )
                try:
                    await asyncio.wait_for(
                        shutdown_event.wait(), timeout=IDLE_POLL_SECONDS
                    )
                except TimeoutError:
                    pass
                continue
            if term is None:
                try:
                    await asyncio.wait_for(
                        shutdown_event.wait(), timeout=IDLE_POLL_SECONDS
                    )
                except TimeoutError:
                    pass
                continue

            logger.info(
                "completion finalizer elected leader=%s elected_at=%s",
                term.leader_id,
                term.elected_at.isoformat(),
            )
            lost = asyncio.Event()
            heartbeat = asyncio.create_task(
                self._heartbeat_leader(term, lost, shutdown_event)
            )
            try:
                next_sweep = 0.0
                while not shutdown_event.is_set() and not lost.is_set():
                    if background_sweep is not None and time.monotonic() >= next_sweep:
                        try:
                            await background_sweep()
                        except Exception:
                            logger.exception(
                                "completion background sweep failed; retrying"
                            )
                        next_sweep = time.monotonic() + 15.0
                    if shutdown_event.is_set() or lost.is_set():
                        break
                    try:
                        command_id = (
                            await self._candidate_id() if drain_commands else None
                        )
                    except Exception:
                        logger.exception(
                            "completion finalizer candidate scan failed; retrying"
                        )
                        try:
                            await asyncio.wait_for(
                                shutdown_event.wait(), timeout=IDLE_POLL_SECONDS
                            )
                        except TimeoutError:
                            pass
                        continue
                    if command_id is None:
                        try:
                            await asyncio.wait_for(
                                shutdown_event.wait(), timeout=IDLE_POLL_SECONDS
                            )
                        except TimeoutError:
                            pass
                        continue
                    if not await self._take_retry_token():
                        await asyncio.sleep(BUSY_POLL_SECONDS)
                        continue
                    try:
                        result = await self.finalize_command(command_id, inline=False)
                    except CompletionLeaseLost:
                        logger.info(
                            "completion command %s changed owner during drain",
                            command_id,
                        )
                    except Exception:
                        logger.exception(
                            "completion finalizer drain failed for command %s",
                            command_id,
                        )
                    else:
                        if result.disposition == "busy":
                            await asyncio.sleep(BUSY_POLL_SECONDS)
            finally:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
                if not lost.is_set():
                    try:
                        await self.release_leader(term)
                    except Exception:
                        logger.exception(
                            "completion finalizer leader release failed for "
                            "leader=%s elected_at=%s",
                            term.leader_id,
                            term.elected_at.isoformat(),
                        )


__all__ = [
    "COMMAND_HEARTBEAT_SECONDS",
    "COMMAND_LEASE_SECONDS",
    "CompletionEffectBudgetExhausted",
    "CompletionEffectGroupBlocked",
    "CompletionEffectInFlight",
    "CompletionEffectRetryRequested",
    "CompletionEffectRunner",
    "CompletionEffectVersionError",
    "CompletionDispositionSuperseded",
    "CompletionFinalizer",
    "CompletionFinalizerError",
    "CompletionLeaseLost",
    "CompletionTeardownSupersedeBlocked",
    "EFFECT_DETAIL_LIMIT_BYTES",
    "EFFECT_LEASE_SECONDS",
    "FinalizationResult",
    "LEADER_LEASE_NAME",
    "LeaderTerm",
]
