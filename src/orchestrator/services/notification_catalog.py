"""Pure catalog for the unified notification system.

Everything here is data or pure functions: the category → (severity, steps,
actions) table, the severity classes, deterministic id minting, row
serialisation, and the three registries ``main.py`` fills at startup (action
handlers, source loaders, source probes). No I/O and no ``main`` imports, so
``main.py`` can register into it without an import cycle and tests can use it
without a database.

Design: knowledge-base/knowledge/features/unified_notification_system.md —
D1 (callers ``record``, never ``send``), D5 (workflow-as-data), D7 (typed
actions, server-side handlers), D8 (severity classes), D11 (recipients are
parties). Shapes deliberately mirror Knock's workflow/step/message model (D12)
so a later buy is a mapping exercise.
"""

from __future__ import annotations

import json
import logging
import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------

NAMESPACE = uuid.NAMESPACE_URL
ID_PREFIX = "srw-notification-v1"

RECIPIENT_KINDS: tuple[str, ...] = ("user", "officer")
SEVERITIES: tuple[str, ...] = ("low", "normal", "high", "critical")
SEVERITY_RANK: dict[str, int] = {"low": 0, "normal": 1, "high": 2, "critical": 3}
CHANNELS: tuple[str, ...] = (
    "in_app",
    "email",
    "ntfy",
    "slack_webhook",
    "discord_webhook",
    "push",
)
WEBHOOK_CHANNELS: tuple[str, ...] = ("ntfy", "slack_webhook", "discord_webhook")
EXTERNAL_CHANNELS: tuple[str, ...] = ("email",) + WEBHOOK_CHANNELS
ACTION_STYLES: tuple[str, ...] = ("default", "primary", "danger")
ACTION_INPUTS: tuple[str | None, ...] = (None, "text", "textarea")

# Step conditions (D5/D6) — evaluated by the sweeper AT DUE TIME, never when
# the step is written. ``severity_at_least:<level>`` is the one parametrised
# form. ``not_resolved`` consults the row's ``resolved_at`` *and* the live
# source through the registered probe, so a writer the resolve hooks never
# enumerated can still never cause a stale mail.
CONDITION_NAMES: tuple[str, ...] = ("not_seen", "not_read", "not_resolved")
CONDITION_SEVERITY_PREFIX = "severity_at_least:"


def validate_condition(condition: str) -> None:
    if condition in CONDITION_NAMES:
        return
    if condition.startswith(CONDITION_SEVERITY_PREFIX):
        level = condition[len(CONDITION_SEVERITY_PREFIX) :]
        if level in SEVERITIES:
            return
    raise ValueError(f"unknown step condition {condition!r}")


# Symbolic delay: resolved per notification from the source job's project
# officer policy (``communication_policy.officer_response_minutes``) when a
# live, un-held officer is commissioned; otherwise the recipient's
# ``communication.escalation_minutes`` or NO_OFFICER_DELAY_MINUTES.
DELAY_OFFICER_RESPONSE = "officer_response_minutes"
NO_OFFICER_DELAY_MINUTES = 5
ESCALATION_MINUTES_BOUNDS = (1, 24 * 60)

# Steps written by quiet-hours deferral of an *immediate* step use indexes from
# here up so they never collide with the class's own declared step indexes.
DEFERRED_STEP_INDEX_BASE = 100


def notification_id(
    recipient_kind: str, recipient_id: str, dedup_key: str
) -> uuid.UUID:
    """Deterministic feed-row id: the same (recipient, dedup_key) always maps
    to the same uuid, which is what makes ``record()`` idempotent under
    journal replay and dual-leader windows. Postgres can reproduce it with
    ``uuid_generate_v5(uuid_ns_url(), 'srw-notification-v1:…')`` for backfills."""
    return uuid.uuid5(
        NAMESPACE, f"{ID_PREFIX}:{recipient_kind}:{recipient_id}:{dedup_key}"
    )


# ---------------------------------------------------------------------------
# Specs
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ActionSpec:
    """One server-declared action on a notification.

    The cockpit renders it from ``label_key``/``style`` and, for ``input``
    actions, collects ``params[input_name]`` before POSTing
    ``{action_type, params}`` back. It never learns what the action *does* —
    that is the registered handler's business (D7).
    """

    type: str
    label_key: str
    style: str = "default"
    input: str | None = None
    input_name: str | None = None

    def __post_init__(self) -> None:
        if self.style not in ACTION_STYLES:
            raise ValueError(f"action {self.type}: unknown style {self.style!r}")
        if self.input not in ACTION_INPUTS:
            raise ValueError(f"action {self.type}: unknown input {self.input!r}")
        if (self.input is None) != (self.input_name is None):
            raise ValueError(f"action {self.type}: input and input_name go together")
        if not self.label_key.startswith("notifications.actions."):
            raise ValueError(
                f"action {self.type}: label_key must be a notifications.actions.* key"
            )

    def serialize(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return {
            "type": self.type,
            "label_key": self.label_key,
            "style": self.style,
            "input": self.input,
            "input_name": self.input_name,
            "params": dict(params or {}),
        }


@dataclass(frozen=True, slots=True)
class StepSpec:
    """One channel step of a category's workflow (Knock: delay + channel).

    ``delay`` is minutes, or the symbolic ``'officer_response_minutes'`` which
    slice 2 resolves per project. A delay is not its own row — it becomes the
    ``due_at`` of this step. Zero-delay steps run inline in ``record()``.
    """

    channel: str
    delay: int | str = 0
    conditions: tuple[str, ...] = ()
    batch_key: str | None = None
    batch_window_minutes: int | None = None

    def __post_init__(self) -> None:
        if self.channel not in CHANNELS or self.channel == "in_app":
            raise ValueError(f"step: unknown channel {self.channel!r}")
        if isinstance(self.delay, bool) or not (
            (isinstance(self.delay, int) and self.delay >= 0)
            or self.delay == DELAY_OFFICER_RESPONSE
        ):
            raise ValueError(f"step {self.channel}: bad delay {self.delay!r}")
        for condition in self.conditions:
            validate_condition(condition)
        if (self.batch_key is None) != (self.batch_window_minutes is None):
            raise ValueError(
                f"step {self.channel}: batch_key and batch_window_minutes go together"
            )
        if self.batch_window_minutes is not None and self.batch_window_minutes <= 0:
            raise ValueError(f"step {self.channel}: batch window must be positive")
        if self.immediate and (self.conditions or self.batch_key):
            # An immediate step runs inline inside record(); there is no due
            # time at which a condition could be re-evaluated or a batch
            # collected. Such a step is a design error, not a runtime one.
            raise ValueError(
                f"step {self.channel}: conditions/batching need a non-zero delay"
            )

    @property
    def immediate(self) -> bool:
        return self.delay == 0


@dataclass(frozen=True, slots=True)
class CategorySpec:
    """What a category means: its default severity, its actions, and — unless
    overridden — the severity class that decides its channel steps."""

    name: str
    severity: str
    actions: tuple[ActionSpec, ...] = ()
    steps: tuple[StepSpec, ...] | None = None
    bypass_quiet_hours: bool | None = None

    def __post_init__(self) -> None:
        if self.severity not in SEVERITIES:
            raise ValueError(
                f"category {self.name}: unknown severity {self.severity!r}"
            )
        types = [a.type for a in self.actions]
        if len(types) != len(set(types)):
            raise ValueError(f"category {self.name}: duplicate action types")


def _immediate(*channels: str) -> tuple[StepSpec, ...]:
    return tuple(StepSpec(channel) for channel in channels)


# Slice 1 severity classes: behaviour parity — every class that mails today
# mails immediately. Kept as the documented "before" and for the parity test.
SEVERITY_CLASSES_V1: dict[str, tuple[StepSpec, ...]] = {
    "critical": _immediate("email", "ntfy", "slack_webhook", "discord_webhook"),
    "high": _immediate("email", "ntfy", "slack_webhook", "discord_webhook"),
    "normal": _immediate("email", "ntfy", "slack_webhook", "discord_webhook"),
    "low": (),
}


def _escalating(*channels: str) -> tuple[StepSpec, ...]:
    """The D5 workflow: in_app now (the durable row), wait the officer's
    response window, then reach out only if nobody looked and nobody settled
    it. Batched per category in 15-minute buckets so three jobs finishing
    together produce one "3 jobs awaiting review" mail, not three."""
    return tuple(
        StepSpec(
            channel,
            delay=DELAY_OFFICER_RESPONSE,
            conditions=("not_seen", "not_resolved"),
            batch_key="{category}",
            batch_window_minutes=15,
        )
        for channel in channels
    )


# Slice 2 severity classes (D8): immediate delivery is reserved for classes
# where latency costs something real. `normal` — a review-queue item — is
# what the issue doc is about: it waits, and a mail that survives the wait
# carries real information (the automated tier did not settle it). `low`
# stays in-app only; a daily digest is one StepSpec away if ever wanted.
SEVERITY_CLASSES_V2: dict[str, tuple[StepSpec, ...]] = {
    "critical": _immediate("email", "ntfy", "slack_webhook", "discord_webhook"),
    "high": _immediate("email", "ntfy", "slack_webhook", "discord_webhook"),
    "normal": _escalating("email", "ntfy", "slack_webhook", "discord_webhook"),
    "low": (),
}
SEVERITY_CLASSES: dict[str, tuple[StepSpec, ...]] = SEVERITY_CLASSES_V2


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------

A = "notifications.actions."
ACTION_APPROVE = ActionSpec("approve", A + "approve", "primary")
ACTION_RESUME_WITH_FEEDBACK = ActionSpec(
    "resume", A + "resumeWithFeedback", "default", "textarea", "feedback"
)
ACTION_RESUME = ActionSpec("resume", A + "resume", "primary", "textarea", "feedback")
ACTION_DENY = ActionSpec("deny", A + "deny", "danger", "text", "reason")
ACTION_UPGRADE_TO_VM = ActionSpec("approve_upgrade", A + "upgradeToVm", "primary")
ACTION_RESUME_WITHOUT_VM = ActionSpec("resume_without_vm", A + "resumeWithoutVm")
ACTION_REPLY = ActionSpec("reply", A + "reply", "primary", "textarea", "message")
ACTION_OPEN_CONFERENCE = ActionSpec("open_conference", A + "openConference")
ACTION_DENY_PLAIN = ActionSpec("deny", A + "deny", "danger")
ACTION_OPEN_JOB = ActionSpec("open", A + "openJob")
ACTION_OPEN_THREAD = ActionSpec("open", A + "openThread")
ACTION_OPEN_SESSION = ActionSpec("open_session", A + "openSession")
ACTION_OPEN_PROJECT = ActionSpec("open", A + "openProject")
ACTION_OPEN_AUTOMATIONS = ActionSpec("open", A + "openAutomations")
ACTION_OPEN_ADMIN_USERS = ActionSpec("open", A + "openAdminUsers")
ACTION_OPEN_ADMIN_CAPACITY = ActionSpec("open", A + "openAdminCapacity")
ACTION_OPEN_SOURCE = ActionSpec("open", A + "open")


# ---------------------------------------------------------------------------
# Categories — the whole vocabulary after the slice-3 cutover
# ---------------------------------------------------------------------------

CATEGORIES: dict[str, CategorySpec] = {
    spec.name: spec
    for spec in (
        # A job waiting on a human decision (autonomy `review`, or automated
        # verification that ended without approving).
        CategorySpec(
            "review_queue",
            "normal",
            (ACTION_APPROVE, ACTION_RESUME_WITH_FEEDBACK, ACTION_OPEN_JOB),
        ),
        # 24 h TTL — miss it and the job dies.
        CategorySpec(
            "vm_upgrade",
            "high",
            (ACTION_UPGRADE_TO_VM, ACTION_RESUME_WITHOUT_VM, ACTION_DENY),
        ),
        CategorySpec("budget_exceeded", "high", (ACTION_RESUME, ACTION_OPEN_JOB)),
        # The job already failed (LLM give-up, drain stall): latency costs.
        CategorySpec("incident", "critical", (ACTION_OPEN_JOB,)),
        CategorySpec(
            "officer_question", "high", (ACTION_REPLY, ACTION_OPEN_CONFERENCE)
        ),
        CategorySpec("officer_runtime", "high", (ACTION_OPEN_CONFERENCE,)),
        # A worker's message to its owner (blocking sends record as `high`).
        CategorySpec("agent_message", "normal", (ACTION_REPLY, ACTION_OPEN_JOB)),
        # A job launched from a session finished while the tab was closed.
        CategorySpec("session_wake", "normal", (ACTION_OPEN_SESSION,)),
        # Loop questions/dispositions: in-app only (loop_campaign_scheduling
        # Q3 — no email, no push).
        CategorySpec("loop_event", "low", (ACTION_OPEN_PROJECT,)),
        CategorySpec("automation_disabled", "normal", (ACTION_OPEN_AUTOMATIONS,)),
        # Per admin; resolved when the user is approved.
        CategorySpec("user_registered", "normal", (ACTION_OPEN_ADMIN_USERS,)),
        # Per admin; resolved by the chat-pool stall watch when the stall ends.
        CategorySpec(
            "chat_executors_unavailable", "high", (ACTION_OPEN_ADMIN_CAPACITY,)
        ),
        # A headless session's permission gate waiting on its owner. The mail
        # carries magic links, so `high` keeps the "approve from your phone"
        # affordance the old thread_notifications email had.
        CategorySpec(
            "session_permission",
            "high",
            (ACTION_APPROVE, ACTION_DENY_PLAIN, ACTION_OPEN_SESSION),
        ),
        # A sudo request has a 300 s TTL: push channels now, never email.
        CategorySpec(
            "sudo_request",
            "critical",
            (ACTION_APPROVE, ACTION_DENY, ACTION_OPEN_SOURCE),
            steps=_immediate("ntfy", "slack_webhook", "discord_webhook"),
        ),
        # An SSH key was registered on the account (workspace_ssh_access.md
        # §6.3): the standard account-security signal, so a key added by
        # someone else (a stolen session, a shared account) is visible to
        # its owner — the same reason a "new sign-in" mail is loud.
        # The row DOES carry source_kind="ssh_key"/source_id (main.py's
        # ssh_key_added notify call) — but no source_loader/source_probe is
        # ever registered for that kind, so nothing can compute
        # source_resolved for it (M-2: this comment used to say "no
        # source_kind", which is wrong — the row has one, it's just
        # unprobed). ACTION_OPEN_SOURCE's handler is therefore the ONLY way
        # one of these ever leaves `pending` (ruling P-12, fix round 1: a
        # zero-action version of this category could never resolve and would
        # inflate the inbox forever) — it also happens to be the right
        # destination: exactly where a user revokes a key they didn't add.
        CategorySpec("ssh_key_added", "high", (ACTION_OPEN_SOURCE,)),
    )
}


def category_spec(name: str) -> CategorySpec:
    """Loud on an unknown category — a typo must never silently become a
    notification that reaches nobody."""
    try:
        return CATEGORIES[name]
    except KeyError:
        raise ValueError(f"unknown notification category {name!r}") from None


def normalize_severity(spec: CategorySpec, severity: str | None) -> str:
    resolved = severity or spec.severity
    if resolved not in SEVERITIES:
        raise ValueError(f"unknown severity {resolved!r}")
    return resolved


def steps_for(spec: CategorySpec, severity: str) -> tuple[StepSpec, ...]:
    if spec.steps is not None:
        return spec.steps
    return SEVERITY_CLASSES[severity]


def bypasses_quiet_hours(spec: CategorySpec, severity: str) -> bool:
    if spec.bypass_quiet_hours is not None:
        return spec.bypass_quiet_hours
    return severity == "critical"


def serialize_actions(
    spec: CategorySpec, action_params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """The declared action set for a new row, with the call's params (job id,
    request id, …) merged into every action so the cockpit can POST them back."""
    return [action.serialize(action_params) for action in spec.actions]


# ---------------------------------------------------------------------------
# Step conditions, batching, preferences (pure — the engine's vocabulary)
# ---------------------------------------------------------------------------


def evaluate_condition(
    condition: str, row: dict[str, Any], *, source_resolved: bool = False
) -> bool:
    """Does ``row`` still satisfy ``condition`` right now? Pure: the caller
    supplies the live probe verdict for the source."""
    validate_condition(condition)
    if condition == "not_seen":
        return row.get("seen_at") is None
    if condition == "not_read":
        return row.get("read_at") is None
    if condition == "not_resolved":
        return row.get("resolved_at") is None and not source_resolved
    level = condition[len(CONDITION_SEVERITY_PREFIX) :]
    return SEVERITY_RANK.get(str(row.get("severity")), -1) >= SEVERITY_RANK[level]


def first_failing_condition(
    conditions: Any, row: dict[str, Any], *, source_resolved: bool = False
) -> str | None:
    """The first condition that no longer holds, or ``None`` when the step
    should go ahead. Unknown names fail closed (the step is skipped, loudly
    named) rather than mailing on a condition nobody can read."""
    for condition in list(conditions or ()):
        try:
            if not evaluate_condition(
                str(condition), row, source_resolved=source_resolved
            ):
                return str(condition)
        except ValueError:
            return f"invalid:{condition}"
    return None


def batch_key_for(step: StepSpec, row: dict[str, Any]) -> str | None:
    """Rows sharing (recipient, channel, batch key, due bucket) become one
    message. The template sees the row's category and severity."""
    if step.batch_key is None:
        return None
    return step.batch_key.format(
        category=row.get("category") or "", severity=row.get("severity") or ""
    )


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def bucket_due_at(due: datetime, window_minutes: int) -> datetime:
    """Round ``due`` UP to the next epoch-aligned window boundary, so rows
    whose delay ends inside the same window share one due instant and the
    sweeper claims them together (Knock: batch window). Never earlier than
    ``due`` — the delay is a floor, the bucket only adds up to one window."""
    seconds = int(window_minutes) * 60
    elapsed = (due - _EPOCH).total_seconds()
    return _EPOCH + timedelta(seconds=math.ceil(elapsed / seconds) * seconds)


def quiet_hours_window(
    settings: dict[str, Any] | None, now: datetime | None = None
) -> tuple[bool, datetime | None]:
    """``(inside, end)`` for the recipient's ``communication.quiet_hours``:
    whether ``now`` falls in the window and, if so, when it ends (UTC). An
    unusable configuration (missing bounds, unknown zone) is "not quiet" —
    silence must be opted into, never stumbled into."""
    qh = ((settings or {}).get("communication") or {}).get("quiet_hours") or {}
    if not qh.get("enabled"):
        return False, None
    start_str = qh.get("start") or ""
    end_str = qh.get("end") or ""
    tz_str = qh.get("timezone") or "UTC"
    if not start_str or not end_str:
        return False, None
    try:
        tz = ZoneInfo(tz_str)
    except (ZoneInfoNotFoundError, KeyError, ValueError):
        logger.warning("Invalid timezone in quiet hours: %s", tz_str)
        return False, None
    try:
        start = time.fromisoformat(start_str)
        end = time.fromisoformat(end_str)
    except ValueError:
        return False, None

    local = (now or datetime.now(timezone.utc)).astimezone(tz)
    current = local.time()
    if start <= end:
        inside = start <= current <= end
        end_date = local.date()
    else:  # overnight, e.g. 22:00 – 08:00
        inside = current >= start or current <= end
        end_date = (
            local.date() + timedelta(days=1) if current >= start else local.date()
        )
    if not inside:
        return False, None
    end_local = datetime.combine(end_date, end, tzinfo=tz)
    return True, end_local.astimezone(timezone.utc)


def channel_enabled(
    channels: dict[str, Any] | None,
    categories: dict[str, Any] | None,
    category: str,
    channel: str,
) -> bool:
    """The D9 preference matrix, degenerate form: a per-category cell
    overrides the channel-type default; both default to on. Only a real
    ``False`` switches anything off — the settings validator guarantees the
    stored values are booleans, and this stays defensive for older rows."""
    cell = ((categories or {}).get(category) or {}).get(channel)
    if isinstance(cell, bool):
        return cell
    default = (channels or {}).get(channel)
    if isinstance(default, bool):
        return default
    return True


def serialize_step(row: dict[str, Any]) -> dict[str, Any]:
    """DB step row → the wire shape (detail pane: "email in 12 min unless…")."""
    conditions = row.get("conditions")
    if isinstance(conditions, str):
        try:
            conditions = json.loads(conditions)
        except ValueError:
            conditions = []
    return {
        "id": int(row["id"]) if row.get("id") is not None else None,
        "step_index": row.get("step_index"),
        "channel": row.get("step_kind"),
        "due_at": _iso(row.get("due_at")),
        "conditions": list(conditions or []),
        "batch_key": row.get("batch_key"),
        "state": row.get("state"),
        "attempt": int(row.get("attempt") or 0),
        "settled_at": _iso(row.get("settled_at")),
        "detail": row.get("detail"),
    }


# ---------------------------------------------------------------------------
# Row serialisation (one shape for the API and the SSE frame)
# ---------------------------------------------------------------------------

_TS_FIELDS = (
    "created_at",
    "seen_at",
    "read_at",
    "interacted_at",
    "resolved_at",
    "archived_at",
)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def serialize_notification(row: dict[str, Any]) -> dict[str, Any]:
    """DB row → the wire shape (`Notification` in the cockpit)."""
    source_kind = row.get("source_kind")
    return {
        "id": str(row["id"]),
        "category": row.get("category"),
        "severity": row.get("severity"),
        "subject": row.get("subject") or "",
        "body": row.get("body") or "",
        "source_ref": (
            {"kind": source_kind, "id": str(row.get("source_id"))}
            if source_kind
            else None
        ),
        "actions": list(row.get("actions") or []),
        "payload": dict(row.get("payload") or {}),
        "resolved_by": row.get("resolved_by"),
        **{name: _iso(row.get(name)) for name in _TS_FIELDS},
    }


def cursor_for(row: dict[str, Any]) -> str:
    """Keyset cursor: ``{created_at_iso}|{id}``."""
    return f"{_iso(row['created_at'])}|{row['id']}"


# ---------------------------------------------------------------------------
# Registries — filled by main.py at startup
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ActionContext:
    notification: dict[str, Any]
    user: dict[str, Any]
    params: dict[str, Any] = field(default_factory=dict)
    db: Any = None


@dataclass(slots=True)
class ActionResult:
    result: dict[str, Any] = field(default_factory=dict)
    # For source-less categories (an officer question has no job to probe),
    # the handler resolves the row itself.
    resolve: bool = False
    resolved_by: str | None = None


ActionHandler = Callable[[ActionContext], Awaitable[ActionResult]]
SourceLoader = Callable[[Any, str, dict[str, Any]], Awaitable[dict[str, Any] | None]]
SourceProbe = Callable[[Any, str], Awaitable[bool]]

_ACTION_HANDLERS: dict[tuple[str, str], ActionHandler] = {}
_SOURCE_LOADERS: dict[str, SourceLoader] = {}
_SOURCE_PROBES: dict[str, SourceProbe] = {}


def register_action(
    category: str, action_type: str
) -> Callable[[ActionHandler], ActionHandler]:
    """Bind a server-side effect to ``(category, action_type)``. The action must
    be declared on the category, so a handler can never exist for an action the
    cockpit could not have been offered."""
    spec = category_spec(category)
    if action_type not in {a.type for a in spec.actions}:
        raise ValueError(f"category {category!r} declares no action {action_type!r}")

    def decorator(handler: ActionHandler) -> ActionHandler:
        _ACTION_HANDLERS[(category, action_type)] = handler
        return handler

    return decorator


def action_handler(category: str, action_type: str) -> ActionHandler | None:
    return _ACTION_HANDLERS.get((category, action_type))


def register_source_loader(source_kind: str) -> Callable[[SourceLoader], SourceLoader]:
    def decorator(loader: SourceLoader) -> SourceLoader:
        _SOURCE_LOADERS[source_kind] = loader
        return loader

    return decorator


def source_loader(source_kind: str | None) -> SourceLoader | None:
    return _SOURCE_LOADERS.get(source_kind) if source_kind else None


def register_source_probe(source_kind: str) -> Callable[[SourceProbe], SourceProbe]:
    def decorator(probe: SourceProbe) -> SourceProbe:
        _SOURCE_PROBES[source_kind] = probe
        return probe

    return decorator


def source_probe(source_kind: str | None) -> SourceProbe | None:
    return _SOURCE_PROBES.get(source_kind) if source_kind else None


def clear_registries() -> None:
    """Test isolation only."""
    _ACTION_HANDLERS.clear()
    _SOURCE_LOADERS.clear()
    _SOURCE_PROBES.clear()


__all__ = [
    "ACTION_STYLES",
    "CATEGORIES",
    "CHANNELS",
    "CONDITION_NAMES",
    "DEFERRED_STEP_INDEX_BASE",
    "DELAY_OFFICER_RESPONSE",
    "ESCALATION_MINUTES_BOUNDS",
    "EXTERNAL_CHANNELS",
    "NO_OFFICER_DELAY_MINUTES",
    "RECIPIENT_KINDS",
    "SEVERITIES",
    "SEVERITY_CLASSES",
    "SEVERITY_CLASSES_V1",
    "SEVERITY_CLASSES_V2",
    "SEVERITY_RANK",
    "WEBHOOK_CHANNELS",
    "ActionContext",
    "ActionHandler",
    "ActionResult",
    "ActionSpec",
    "CategorySpec",
    "SourceLoader",
    "SourceProbe",
    "StepSpec",
    "action_handler",
    "batch_key_for",
    "bucket_due_at",
    "bypasses_quiet_hours",
    "category_spec",
    "channel_enabled",
    "clear_registries",
    "cursor_for",
    "evaluate_condition",
    "first_failing_condition",
    "normalize_severity",
    "notification_id",
    "quiet_hours_window",
    "register_action",
    "register_source_loader",
    "register_source_probe",
    "serialize_actions",
    "serialize_notification",
    "serialize_step",
    "source_loader",
    "source_probe",
    "steps_for",
    "validate_condition",
]
