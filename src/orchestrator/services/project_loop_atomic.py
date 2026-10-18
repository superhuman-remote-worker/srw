"""Atomic Class-C materialization for project-loop advancement.

The legacy loop engine drains ``current_stage_jobs`` before it creates the
successor jobs, then points the loop at those rows in a later write.  A parked
completion finalizer turns that normally-short gap into an unbounded window in
which the loop healer can restore the old membership and create the successor
twice.

With durable completion commands enabled, advancement uses this module instead:
the loop row and its member jobs are locked, the exact observed world is
validated, the successor job rows are inserted, and the loop pointers/counters
(including a campaign cursor handoff) are written in one app-Postgres
transaction.  The callback performs no Gitea, vector-store, cloud,
notification, or dispatch I/O.  Those operations replay after commit from the
persisted job IDs returned here.

The returned ``won`` bit is deliberately data rather than an exception.  A
durable completion effect uses it as its effect-level ``supersede_if`` world
CAS; a competing sweeper simply treats ``won=False`` as a benign lost race.
"""

from __future__ import annotations

import copy
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from orchestrator.services.project_loops import (
    LOOP_CAMPAIGN_HISTORY_LIMIT,
    create_loop_job,
    normalize_stage,
    planner_slots,
    resolve_campaign_caps,
    validate_loop_plan,
)

_TERMINAL_JOB_STATES = frozenset({"completed", "failed", "cancelled"})

# May return None: create_loop_job skips (and logs) rather than raising when
# the loop's project has been archived.
CreateLoopJob = Callable[..., Awaitable[dict[str, Any] | None]]


class ProjectLoopHandoffAuthorityLost(RuntimeError):
    """The exact DB-clock handoff claim is no longer owned by this task."""


def _json_value(value: Any) -> Any:
    """Return a comparison-stable copy of a JSON-ish asyncpg value."""

    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _bounded_diagnostic(
    value: str | None, *, limit_bytes: int = 1024
) -> tuple[str | None, dict[str, int] | None]:
    """Bound a replay-only string while recording what was truncated."""

    if value is None:
        return None, None
    raw = str(value).encode("utf-8")
    if len(raw) <= limit_bytes:
        return str(value), None
    suffix = "…"
    budget = max(0, limit_bytes - len(suffix.encode("utf-8")))
    bounded = raw[:budget].decode("utf-8", errors="ignore") + suffix
    return bounded, {
        "original_bytes": len(raw),
        "retained_bytes": len(bounded.encode("utf-8")),
    }


def bounded_replay_diagnostic(
    value: str | None, *, limit_bytes: int = 1024
) -> tuple[str | None, dict[str, int] | None]:
    """Public projection for diagnostics stored in an S32 replay payload.

    The authoritative error remains on the completion command/job.  This copy
    exists only so the post-commit handoff can describe what happened without
    letting a provider-sized error overflow the effect row's 8 KiB cap.
    """

    return _bounded_diagnostic(value, limit_bytes=limit_bytes)


def bounded_replay_text(value: Any, *, limit_bytes: int = 512) -> str:
    """UTF-8-safe presentation text for replay output and final actions."""

    return _bounded_diagnostic(str(value), limit_bytes=limit_bytes)[0] or ""


def _bounded_replay_stage(stage: Any) -> Any:
    if isinstance(stage, (list, tuple)):
        return [bounded_replay_text(role, limit_bytes=128) for role in stage]
    return bounded_replay_text(stage, limit_bytes=128)


@dataclass(frozen=True, slots=True)
class LoopAdvanceExpectation:
    """The world observed while the next turn was planned outside the lock.

    Only advancement-authority fields participate.  Human edits to prose such
    as ``goal`` do not strand a completion, while membership, counters,
    schedule grammar, budgets, and campaign state must still be exactly the
    world for which the plan was derived.
    """

    stage_job_ids: tuple[str, ...]
    current_job_id: str | None
    member_states: tuple[tuple[str, str], ...]
    scheduling: str
    role_sequence: Any
    seq_index: int
    total_jobs_run: int
    remaining_iterations: int | None
    max_iterations: int | None
    run_until: datetime | None
    max_consecutive_failures: int
    consecutive_failures: int
    campaign: Any
    campaign_history: Any
    campaign_caps: Any

    @classmethod
    def from_rows(
        cls,
        loop: Mapping[str, Any],
        member_states: Mapping[str, str],
    ) -> "LoopAdvanceExpectation":
        stage_job_ids = tuple(
            str(job_id) for job_id in (loop.get("current_stage_jobs") or [])
        )
        return cls(
            stage_job_ids=stage_job_ids,
            current_job_id=(
                str(loop["current_job_id"])
                if loop.get("current_job_id") is not None
                else None
            ),
            member_states=tuple(
                (job_id, str(member_states.get(job_id, "missing")))
                for job_id in stage_job_ids
            ),
            scheduling=str(loop.get("scheduling") or "standard"),
            role_sequence=_json_value(loop.get("role_sequence") or []),
            seq_index=int(loop.get("seq_index") or 0),
            total_jobs_run=int(loop.get("total_jobs_run") or 0),
            remaining_iterations=(
                int(loop["remaining_iterations"])
                if loop.get("remaining_iterations") is not None
                else None
            ),
            max_iterations=(
                int(loop["max_iterations"])
                if loop.get("max_iterations") is not None
                else None
            ),
            run_until=loop.get("run_until"),
            max_consecutive_failures=int(loop.get("max_consecutive_failures") or 3),
            consecutive_failures=int(loop.get("consecutive_failures") or 0),
            campaign=_json_value(loop.get("campaign")),
            campaign_history=_json_value(loop.get("campaign_history") or []),
            campaign_caps=_json_value(loop.get("campaign_caps")),
        )

    def matches_loop(self, loop: Mapping[str, Any]) -> bool:
        """Whether a locked row still represents this planned turn."""

        if str(loop.get("status") or "") != "running":
            return False
        observed_stage = tuple(
            str(job_id) for job_id in (loop.get("current_stage_jobs") or [])
        )
        return (
            observed_stage == self.stage_job_ids
            and (
                str(loop["current_job_id"])
                if loop.get("current_job_id") is not None
                else None
            )
            == self.current_job_id
            and str(loop.get("scheduling") or "standard") == self.scheduling
            and _json_value(loop.get("role_sequence") or []) == self.role_sequence
            and int(loop.get("seq_index") or 0) == self.seq_index
            and int(loop.get("total_jobs_run") or 0) == self.total_jobs_run
            and loop.get("remaining_iterations") == self.remaining_iterations
            and loop.get("max_iterations") == self.max_iterations
            and loop.get("run_until") == self.run_until
            and int(loop.get("max_consecutive_failures") or 3)
            == self.max_consecutive_failures
            and int(loop.get("consecutive_failures") or 0) == self.consecutive_failures
            and _json_value(loop.get("campaign")) == self.campaign
            and _json_value(loop.get("campaign_history") or []) == self.campaign_history
            and _json_value(loop.get("campaign_caps")) == self.campaign_caps
        )


@dataclass(frozen=True, slots=True)
class LoopAdvanceMutation:
    """One already-planned mutation to commit with the barrier claim.

    ``stage`` is ``None`` for a stop/officer outcome.  A non-None stage is the
    role-sequence entry to materialize; all of its job rows are born in the
    transaction.  Campaign/history booleans distinguish "unchanged" from the
    meaningful JSON NULL/empty values.
    """

    stage: Any | None
    seq_index: int
    remaining_iterations: int | None
    consecutive_failures: int
    last_error: str | None
    status: str = "running"
    stop_reason: str | None = None
    campaign_changed: bool = False
    campaign: Any = None
    campaign_history_changed: bool = False
    campaign_history: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    extra_context: Mapping[str, Any] = field(default_factory=dict)
    park_until: datetime | None = None
    replay: Mapping[str, Any] = field(default_factory=dict)


def _loop_notification(
    *, event_type: str, subject: str, message: str
) -> dict[str, str]:
    return {
        "event_type": bounded_replay_text(event_type, limit_bytes=96),
        "subject": bounded_replay_text(subject, limit_bytes=256),
        "message": bounded_replay_text(message, limit_bytes=1024),
    }


def _rotation_mutation(
    loop: Mapping[str, Any],
    *,
    next_remaining: int | None,
    consecutive: int,
    last_error: str | None,
    campaign_changed: bool,
    campaign: Any,
    campaign_history_changed: bool,
    campaign_history: Sequence[Mapping[str, Any]],
    park_until: datetime | None,
    replay: dict[str, Any],
) -> LoopAdvanceMutation:
    roles = list(loop.get("role_sequence") or ["scholar", "critic", "developer"])
    next_index = (int(loop.get("seq_index") or 0) + 1) % len(roles)
    replay["kb_ttl_decrement"] = next_index == 0
    replay["action"] = {
        "kind": "rotation",
        "stage": _bounded_replay_stage(roles[next_index]),
    }
    return LoopAdvanceMutation(
        stage=copy.deepcopy(roles[next_index]),
        seq_index=next_index,
        remaining_iterations=next_remaining,
        consecutive_failures=consecutive,
        last_error=last_error,
        campaign_changed=campaign_changed,
        campaign=copy.deepcopy(campaign),
        campaign_history_changed=campaign_history_changed,
        campaign_history=tuple(copy.deepcopy(list(campaign_history))),
        park_until=park_until,
        replay=replay,
    )


def plan_loop_advance(
    loop: Mapping[str, Any],
    *,
    completed_job: Mapping[str, Any],
    completed_context: Mapping[str, Any],
    member_states: Mapping[str, str],
    failed: bool,
    member_error: str | None,
    deadline_passed: bool,
    park_until: datetime | None = None,
) -> LoopAdvanceMutation:
    """Purely plan the mutation and post-commit handoff for one turn.

    The function performs no database or network work.  Campaign decisions are
    derived from persisted loop/member stamps and encoded in the mutation so
    the transaction can compare the exact expectation before using them.
    """

    loop_id = str(loop["id"])
    stage_ids = [str(job_id) for job_id in (loop.get("current_stage_jobs") or [])]
    member_values = [str(member_states.get(job_id, "missing")) for job_id in stage_ids]
    all_failed = bool(member_values) and all(
        state == "failed" for state in member_values
    )
    consecutive = int(loop.get("consecutive_failures") or 0) + 1 if all_failed else 0
    last_error = (
        (member_error if len(stage_ids) == 1 else "all stage jobs failed")
        if all_failed
        else None
    )
    replay_error, replay_error_truncation = bounded_replay_diagnostic(member_error)
    replay_record: dict[str, Any] = {
        "failed": bool(failed),
        "last_error": replay_error,
    }
    if replay_error_truncation is not None:
        replay_record["last_error_truncation"] = replay_error_truncation
    replay: dict[str, Any] = {
        "record_member": replay_record,
        "notify_user_questions": True,
        "notifications": [],
        "close_ticket": None,
        "kb_ttl_decrement": False,
        "officer": None,
        "pre_actions": [],
        "action": None,
    }

    if str(loop.get("scheduling") or "standard") == "officer":
        replay["officer"] = {
            "dedup_key": f"{loop_id[:8]}:{int(loop.get('seq_index') or 0)}",
            "turn_all_failed": all_failed,
            "consecutive_failures": consecutive,
        }
        replay["action"] = {"kind": "officer"}
        return LoopAdvanceMutation(
            stage=None,
            seq_index=int(loop.get("seq_index") or 0),
            remaining_iterations=loop.get("remaining_iterations"),
            consecutive_failures=consecutive,
            last_error=last_error,
            replay=replay,
        )

    remaining = loop.get("remaining_iterations")
    next_remaining = int(remaining) - 1 if remaining is not None else None
    if next_remaining is not None and next_remaining <= 0:
        stop_reason = "budget"
    elif deadline_passed:
        stop_reason = "deadline"
    elif consecutive >= int(loop.get("max_consecutive_failures") or 3):
        stop_reason = "failures"
    else:
        stop_reason = None
    if stop_reason is not None:
        replay["action"] = {"kind": "stop", "reason": stop_reason}
        return LoopAdvanceMutation(
            stage=None,
            seq_index=int(loop.get("seq_index") or 0),
            remaining_iterations=next_remaining,
            consecutive_failures=consecutive,
            last_error=last_error,
            status="failed" if stop_reason == "failures" else "completed",
            stop_reason=stop_reason,
            replay=replay,
        )

    if park_until is not None:
        park_iso = park_until.isoformat()
        replay["pre_actions"].append({"kind": "cooldown_park", "park_until": park_iso})
        replay["notifications"].append(
            _loop_notification(
                event_type="loop_cooldown_park",
                subject="Loop waiting for model cooldown",
                message=(
                    "A loop member failed because model "
                    f"'{loop.get('model') or 'the pinned model'}' is in a quota "
                    "cooldown. The next member was created parked and will "
                    f"dispatch automatically at {park_iso}."
                ),
            )
        )

    campaign_changed = False
    campaign_value: Any = loop.get("campaign")
    history_changed = False
    history_value: list[Mapping[str, Any]] = list(loop.get("campaign_history") or [])

    if str(loop.get("scheduling") or "standard") == "campaign":
        try:
            critic_slot, execution_slot = planner_slots(
                list(loop.get("role_sequence") or [])
            )
        except ValueError:
            # Historical behavior for a malformed persisted planner grammar is
            # plain rotation.  In particular, never materialize a campaign job
            # with the sentinel index -1.
            return _rotation_mutation(
                loop,
                next_remaining=next_remaining,
                consecutive=consecutive,
                last_error=last_error,
                campaign_changed=False,
                campaign=loop.get("campaign"),
                campaign_history_changed=False,
                campaign_history=list(loop.get("campaign_history") or []),
                park_until=park_until,
                replay=replay,
            )
        campaign = copy.deepcopy(loop.get("campaign") or None)
        member_campaign_id = completed_context.get("loop_campaign_id")

        if member_campaign_id and (
            not campaign or str(campaign.get("id")) != str(member_campaign_id)
        ):
            # A late member of an already-disposed campaign must not be
            # reinterpreted as a checkpoint critic with permission to file a
            # fresh plan.  It simply rejoins normal rotation, as legacy did.
            return _rotation_mutation(
                loop,
                next_remaining=next_remaining,
                consecutive=consecutive,
                last_error=last_error,
                campaign_changed=False,
                campaign=loop.get("campaign"),
                campaign_history_changed=False,
                campaign_history=list(loop.get("campaign_history") or []),
                park_until=park_until,
                replay=replay,
            )

        if (
            member_campaign_id
            and campaign
            and str(campaign.get("id")) == str(member_campaign_id)
        ):
            try:
                member_index = int(completed_context.get("loop_campaign_index"))
            except (TypeError, ValueError):
                member_index = -1
            stages = list(campaign.get("stages") or [])
            if 0 <= member_index < len(stages):
                stages_done = max(
                    int(campaign.get("stages_done") or 0), member_index + 1
                )
                member_failures = (
                    int(campaign.get("member_failures") or 0) + 1 if failed else 0
                )
                label = (
                    campaign.get("title") or campaign.get("initiative_note_id") or "?"
                )
                replay_label = bounded_replay_text(label, limit_bytes=256)
                caps = resolve_campaign_caps(dict(loop))
                if failed and member_failures >= caps["abort_failures"]:
                    replay["pre_actions"].append(
                        {
                            "kind": "campaign_aborted",
                            "label": replay_label,
                            "member_failures": member_failures,
                        }
                    )
                    campaign_changed = True
                    campaign_value = {
                        **campaign,
                        "status": "aborted",
                        "member_failures": member_failures,
                        "stages_done": stages_done,
                    }
                    replay["notifications"].append(
                        _loop_notification(
                            event_type="loop_campaign_disposition",
                            subject=f"Loop campaign aborted: {replay_label}",
                            message=(
                                f"Campaign '{replay_label}' aborted after "
                                f"{member_failures} consecutive stage failures "
                                f"({stages_done} of {len(stages)} stages done). "
                                "The loop is returning to the critic checkpoint "
                                "for a disposition."
                            ),
                        )
                    )
                    return _rotation_mutation(
                        loop,
                        next_remaining=next_remaining,
                        consecutive=consecutive,
                        last_error=last_error,
                        campaign_changed=True,
                        campaign=campaign_value,
                        campaign_history_changed=False,
                        campaign_history=history_value,
                        park_until=park_until,
                        replay=replay,
                    )
                elif member_index + 1 >= len(stages):
                    replay["pre_actions"].append(
                        {
                            "kind": "campaign_complete",
                            "label": replay_label,
                            "stage_count": len(stages),
                        }
                    )
                    campaign_changed = True
                    campaign_value = {
                        **campaign,
                        "status": "review",
                        "member_failures": member_failures,
                        "stages_done": len(stages),
                        "cursor": len(stages),
                    }
                    return _rotation_mutation(
                        loop,
                        next_remaining=next_remaining,
                        consecutive=consecutive,
                        last_error=last_error,
                        campaign_changed=True,
                        campaign=campaign_value,
                        campaign_history_changed=False,
                        campaign_history=history_value,
                        park_until=park_until,
                        replay=replay,
                    )
                else:
                    next_index = member_index + 1
                    next_campaign = {
                        **campaign,
                        "member_failures": member_failures,
                        "stages_done": stages_done,
                        "cursor": next_index + 1,
                    }
                    entry = stages[next_index]
                    role = str(
                        entry.get("role") if isinstance(entry, Mapping) else entry
                    )
                    replay["action"] = {
                        "kind": "campaign_member",
                        "label": replay_label,
                        "stage_index": next_index,
                        "stage_count": len(stages),
                        "role": bounded_replay_text(role, limit_bytes=128),
                    }
                    return LoopAdvanceMutation(
                        stage=role,
                        seq_index=execution_slot,
                        remaining_iterations=next_remaining,
                        consecutive_failures=consecutive,
                        last_error=last_error,
                        campaign_changed=True,
                        campaign=next_campaign,
                        extra_context={
                            "loop_campaign_id": str(campaign["id"]),
                            "loop_campaign_index": next_index,
                        },
                        park_until=park_until,
                        replay=replay,
                    )

            # Legacy treats a malformed campaign-member cursor as an ordinary
            # rotation, not as a checkpoint critic eligible to file a plan.
            return _rotation_mutation(
                loop,
                next_remaining=next_remaining,
                consecutive=consecutive,
                last_error=last_error,
                campaign_changed=False,
                campaign=loop.get("campaign"),
                campaign_history_changed=False,
                campaign_history=history_value,
                park_until=park_until,
                replay=replay,
            )

        stamped_seq = completed_context.get("loop_seq_index")
        checkpoint_critic = completed_context.get("loop_role") == "critic" and (
            stamped_seq is None or int(stamped_seq) == critic_slot
        )
        if checkpoint_critic:
            plan = completed_context.get("loop_plan")
            if not isinstance(plan, Mapping):
                if campaign and campaign.get("status") in ("review", "aborted"):
                    label = (
                        campaign.get("title")
                        or campaign.get("initiative_note_id")
                        or "?"
                    )
                    replay_label = bounded_replay_text(label, limit_bytes=256)
                    replay["notifications"].append(
                        _loop_notification(
                            event_type="loop_campaign_review_skipped",
                            subject=f"Loop campaign review skipped: {replay_label}",
                            message=(
                                "The checkpoint critic completed without "
                                f"disposing campaign '{replay_label}' (status "
                                f"{campaign.get('status')}, "
                                f"{campaign.get('stages_done', '?')} of "
                                f"{len(campaign.get('stages') or [])} stages "
                                "done). The campaign stays parked until a "
                                "critic files a disposition — ship/kill may be "
                                "filed without opening a new campaign."
                            ),
                        )
                    )
                    replay["pre_actions"].append(
                        {
                            "kind": "campaign_review_skipped",
                            "label": replay_label,
                        }
                    )
            elif campaign and str(campaign.get("plan_job_id")) == str(
                completed_job["id"]
            ):
                cursor = int(campaign.get("cursor") or 0)
                stages = list(campaign.get("stages") or [])
                if cursor < len(stages):
                    entry = stages[cursor]
                    role = str(
                        entry.get("role") if isinstance(entry, Mapping) else entry
                    )
                    next_campaign = {**campaign, "cursor": cursor + 1}
                    replay["action"] = {
                        "kind": "campaign_member",
                        "label": bounded_replay_text(
                            campaign.get("title") or "?", limit_bytes=256
                        ),
                        "stage_index": cursor,
                        "stage_count": len(stages),
                        "role": bounded_replay_text(role, limit_bytes=128),
                    }
                    return LoopAdvanceMutation(
                        stage=role,
                        seq_index=execution_slot,
                        remaining_iterations=next_remaining,
                        consecutive_failures=consecutive,
                        last_error=last_error,
                        campaign_changed=True,
                        campaign=next_campaign,
                        extra_context={
                            "loop_campaign_id": str(campaign["id"]),
                            "loop_campaign_index": cursor,
                        },
                        park_until=park_until,
                        replay=replay,
                    )
            else:
                try:
                    normalized = validate_loop_plan(dict(plan), dict(loop))
                except ValueError as exc:
                    rejected, rejected_truncation = bounded_replay_diagnostic(str(exc))
                    replay["plan_rejected"] = rejected
                    if rejected_truncation is not None:
                        replay["plan_rejected_truncation"] = rejected_truncation
                    replay["pre_actions"].append(
                        {"kind": "plan_rejected", "error": rejected}
                    )
                else:
                    extensions_used = 0
                    disposition = normalized.get("disposition")
                    if campaign and disposition:
                        outcome = disposition["outcome"]
                        if outcome == "extend":
                            extensions_used = (
                                int(campaign.get("extensions_used") or 0) + 1
                            )
                        history_value.append(
                            {
                                "id": campaign.get("id"),
                                "initiative_note_id": campaign.get(
                                    "initiative_note_id"
                                ),
                                "title": campaign.get("title"),
                                "stages_total": len(campaign.get("stages") or []),
                                "stages_done": campaign.get("stages_done"),
                                "extensions_used": campaign.get("extensions_used"),
                                "status_at_close": campaign.get("status"),
                                "outcome": outcome,
                                "notes": disposition.get("notes"),
                                "disposed_by": str(completed_job["id"]),
                            }
                        )
                        history_value = history_value[-LOOP_CAMPAIGN_HISTORY_LIMIT:]
                        history_changed = True
                        label = (
                            campaign.get("title")
                            or campaign.get("initiative_note_id")
                            or "?"
                        )
                        replay_label = bounded_replay_text(label, limit_bytes=256)
                        replay["pre_actions"].append(
                            {
                                "kind": "campaign_disposed",
                                "label": replay_label,
                                "outcome": outcome,
                            }
                        )
                        replay["notifications"].append(
                            _loop_notification(
                                event_type="loop_campaign_disposition",
                                subject=f"Loop campaign {outcome}: {replay_label}",
                                message=(
                                    f"The critic disposed campaign '{replay_label}' as "
                                    f"{outcome.upper()} "
                                    f"({campaign.get('stages_done', '?')} of "
                                    f"{len(campaign.get('stages') or [])} stages "
                                    "done"
                                    f"{', extending it' if outcome == 'extend' else ''})."
                                    + (
                                        f" Notes: {disposition.get('notes')}"
                                        if disposition.get("notes")
                                        else ""
                                    )
                                ),
                            )
                        )
                        ticket_status = {"ship": "resolved", "kill": "archived"}.get(
                            outcome
                        )
                        if ticket_status and campaign.get("initiative_note_id"):
                            replay["close_ticket"] = {
                                "note_id": str(campaign["initiative_note_id"]),
                                "status": ticket_status,
                            }

                    if not normalized["stages"]:
                        campaign_changed = True
                        campaign_value = None
                        replay["pre_actions"].append({"kind": "campaign_dispose_only"})
                    else:
                        new_campaign = {
                            "id": str(completed_job["id"]),
                            "plan_job_id": str(completed_job["id"]),
                            "initiative_note_id": normalized["initiative"][
                                "kb_note_id"
                            ],
                            "title": normalized["initiative"]["title"],
                            "stages": normalized["stages"],
                            "acceptance": normalized["acceptance"],
                            "cursor": 1,
                            "stages_done": 0,
                            "member_failures": 0,
                            "extensions_used": extensions_used,
                            "status": "active",
                        }
                        first = normalized["stages"][0]
                        role = str(first["role"])
                        replay["action"] = {
                            "kind": "campaign_member",
                            "label": bounded_replay_text(
                                new_campaign.get("title") or "?", limit_bytes=256
                            ),
                            "stage_index": 0,
                            "stage_count": len(normalized["stages"]),
                            "role": bounded_replay_text(role, limit_bytes=128),
                        }
                        return LoopAdvanceMutation(
                            stage=role,
                            seq_index=execution_slot,
                            remaining_iterations=next_remaining,
                            consecutive_failures=consecutive,
                            last_error=last_error,
                            campaign_changed=True,
                            campaign=new_campaign,
                            campaign_history_changed=history_changed,
                            campaign_history=tuple(history_value),
                            extra_context={
                                "loop_campaign_id": str(new_campaign["id"]),
                                "loop_campaign_index": 0,
                            },
                            park_until=park_until,
                            replay=replay,
                        )

    return _rotation_mutation(
        loop,
        next_remaining=next_remaining,
        consecutive=consecutive,
        last_error=last_error,
        campaign_changed=campaign_changed,
        campaign=campaign_value,
        campaign_history_changed=history_changed,
        campaign_history=history_value,
        park_until=park_until,
        replay=replay,
    )


def _locked_member_states(
    expected: LoopAdvanceExpectation,
    rows: Mapping[str, str],
) -> tuple[tuple[str, str], ...]:
    return tuple(
        (job_id, str(rows.get(job_id, "missing"))) for job_id in expected.stage_job_ids
    )


def _lost_result(*, loop_id: str, member_job_id: str, reason: str) -> dict[str, Any]:
    return {
        "won": False,
        "reason": reason,
        "loop_id": str(loop_id),
        "completed_member_id": str(member_job_id),
        "spawned_job_ids": [],
        "replay": {},
    }


async def materialize_loop_advance_atomic(
    db: Any,
    *,
    loop_id: str,
    member_job_id: str,
    expected: LoopAdvanceExpectation,
    mutation: LoopAdvanceMutation,
    backlog_block: str | None = None,
    history_block: str | None = None,
    create_job_fn: CreateLoopJob = create_loop_job,
) -> dict[str, Any]:
    """Claim one loop turn and materialize its successor atomically.

    The only callbacks allowed here are app-DB job materialization helpers.
    Connector/expert policy reads intentionally share the transaction through
    :meth:`PostgresDB.transaction_scope`; caller-prepared vector/backlog text is
    passed as inert input.  Any exception rolls back both jobs and pointers.
    """

    loop_id = str(loop_id)
    member_job_id = str(member_job_id)
    if member_job_id not in expected.stage_job_ids:
        return _lost_result(
            loop_id=loop_id,
            member_job_id=member_job_id,
            reason="member_not_in_expected_turn",
        )

    async with db.transaction_scope():
        loop = await db.lock_project_loop_for_advance(loop_id)
        if loop is None or not expected.matches_loop(loop):
            return _lost_result(
                loop_id=loop_id,
                member_job_id=member_job_id,
                reason="loop_world_changed",
            )

        member_states = await db.lock_loop_stage_member_statuses(
            list(expected.stage_job_ids)
        )
        if _locked_member_states(expected, member_states) != expected.member_states:
            return _lost_result(
                loop_id=loop_id,
                member_job_id=member_job_id,
                reason="member_world_changed",
            )
        if member_states.get(member_job_id) not in _TERMINAL_JOB_STATES:
            return _lost_result(
                loop_id=loop_id,
                member_job_id=member_job_id,
                reason="member_not_terminal",
            )
        if any(state not in _TERMINAL_JOB_STATES for state in member_states.values()):
            return _lost_result(
                loop_id=loop_id,
                member_job_id=member_job_id,
                reason="turn_not_terminal",
            )

        update_fields: dict[str, Any] = {
            "status": mutation.status,
            "seq_index": int(mutation.seq_index),
            "remaining_iterations": mutation.remaining_iterations,
            "consecutive_failures": int(mutation.consecutive_failures),
            "last_error": mutation.last_error,
            "stop_reason": mutation.stop_reason,
        }
        if mutation.campaign_changed:
            update_fields["campaign"] = copy.deepcopy(mutation.campaign)
        if mutation.campaign_history_changed:
            update_fields["campaign_history"] = copy.deepcopy(
                list(mutation.campaign_history)
            )

        jobs: list[dict[str, Any]] = []
        roles: list[str] = []
        if mutation.stage is not None:
            roles = normalize_stage(mutation.stage)
            new_total = expected.total_jobs_run + len(roles)
            loop_for_spawn = dict(loop)
            loop_for_spawn["remaining_iterations"] = mutation.remaining_iterations
            if mutation.campaign_changed:
                loop_for_spawn["campaign"] = copy.deepcopy(mutation.campaign)
            for role in roles:
                job = await create_job_fn(
                    db,
                    loop_for_spawn,
                    role=role,
                    iteration=new_total,
                    seq_index=int(mutation.seq_index),
                    remaining_iterations=mutation.remaining_iterations,
                    extra_context=dict(mutation.extra_context),
                    backlog_block=backlog_block,
                    history_block=history_block,
                    park_until=mutation.park_until,
                )
                if job is not None:
                    jobs.append(job)
            if roles and not jobs:
                # create_loop_job skipped every role (archived project). Do
                # not advance: return the same "did not win" shape a changed
                # world produces, so the transaction commits nothing and the
                # loop keeps pointing at the turn that just finished rather
                # than at an empty stage it can never complete.
                return _lost_result(
                    loop_id=loop_id,
                    member_job_id=member_job_id,
                    reason="project_archived",
                )
            ids = [str(job["id"]) for job in jobs]
            update_fields.update(
                current_job_id=(ids[0] if len(ids) == 1 else None),
                current_stage_jobs=ids,
                total_jobs_run=new_total,
            )
        else:
            ids = []
            update_fields.update(
                current_job_id=None,
                current_stage_jobs=[],
                total_jobs_run=expected.total_jobs_run,
            )

        updated = await db.update_project_loop(loop_id, **update_fields)
        if updated is None:  # row was locked, so disappearance is corruption
            raise RuntimeError(f"project loop {loop_id} disappeared while locked")

        output = {
            "won": True,
            "reason": "materialized" if ids else "settled_without_successor",
            "loop_id": loop_id,
            "completed_member_id": member_job_id,
            "spawned_job_ids": ids,
            "spawned_roles": [
                bounded_replay_text(role, limit_bytes=128) for role in roles
            ],
            "loop_status": str(updated.get("status") or mutation.status),
            "seq_index": int(mutation.seq_index),
            "remaining_iterations": mutation.remaining_iterations,
            "total_jobs_run": int(update_fields["total_jobs_run"]),
            "replay": copy.deepcopy(dict(mutation.replay)),
        }
        command_id = mutation.extra_context.get("_loop_advance_completion_command_id")
        marker_written = await db.merge_job_context(
            member_job_id,
            {
                "_project_loop_advance_handoff": {
                    "state": "pending",
                    "command_id": str(command_id) if command_id else None,
                    "output": {"applicable": True, **copy.deepcopy(output)},
                }
            },
        )
        if not marker_written:
            raise RuntimeError(
                f"project loop {loop_id}: predecessor disappeared before handoff marker"
            )
        return output
