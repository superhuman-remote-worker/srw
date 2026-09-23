"""Unified notification service
(knowledge-base/knowledge/features/unified_notification_system.md).

* ``record()`` — the one front door (D1: callers *record* what happened and
  who has a stake; they never choose a channel). It writes one durable feed
  row per recipient (D2/D3), broadcasts the ``notification`` SSE frame once,
  performs the zero-delay channel deliveries of the row's severity class
  with a claim-before-send ledger so a replayed completion effect or a
  dual-leader retry can never send twice (D10), and writes the class's
  *deferred* steps to ``notification_steps`` in the same transaction as the
  row (D5/D6: "wait the officer's window, then mail unless somebody looked
  or somebody settled it"). ``services/notification_steps.py`` runs those
  when due; :meth:`send_step_group` is the send half it calls back into.

* The ``record_*`` helpers (agent message, review returned, automation
  disabled, user registered) are thin shapes over ``record()`` for the
  producers with a fixed subject/body — never a second delivery path. The
  legacy fan-out (``dispatch()``, the ``notify_*`` helpers, the quiet-hours
  digest queue) was deleted in slice 3; ``scripts/check_notification_producers.py``
  keeps ``policy/notification_producers.txt`` at zero legacy call sites.

Follows the NatsBridge graceful degradation pattern: fully optional, all
operations are no-ops when unconfigured.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from orchestrator.services.notification_catalog import (
    DEFERRED_STEP_INDEX_BASE,
    DELAY_OFFICER_RESPONSE,
    ESCALATION_MINUTES_BOUNDS,
    NO_OFFICER_DELAY_MINUTES,
    RECIPIENT_KINDS,
    WEBHOOK_CHANNELS,
    ActionContext,
    action_handler,
    batch_key_for,
    bucket_due_at,
    bypasses_quiet_hours,
    category_spec,
    channel_enabled,
    normalize_severity,
    notification_id as mint_notification_id,
    quiet_hours_window,
    serialize_actions,
    serialize_notification,
    serialize_step,
    source_probe,
    steps_for,
)
from orchestrator.services.webhook_transports import (
    DiscordWebhookTransport,
    NotificationPayload,
    NtfyTransport,
    SlackWebhookTransport,
)
from shared.content_redaction import sanitize, sanitize_text

logger = logging.getLogger(__name__)


class NotificationNotFound(LookupError):
    """No such row for this recipient (the endpoint answers 404)."""


class ActionNotDeclared(ValueError):
    """The row does not carry that action (400)."""


class ActionUnregistered(RuntimeError):
    """The category declares the action but nothing handles it — a wiring bug
    that must be loud, like ``_run_completion_effect``'s registry gate (500)."""


@dataclass(frozen=True, slots=True)
class RecordResult:
    notification_id: str
    inserted: bool
    deliveries: dict[str, Any]

    def as_dispatch(self) -> dict[str, Any]:
        """The channel-outcome dict the message ledger callers settle on
        (``services.message_routing.classify_dispatch`` reads ``in_app`` /
        ``email`` / ``email_message_id``). The durable feed row is itself an
        accepted delivery; the email, when immediate, rides along."""
        return {**self.deliveries, "notification_id": self.notification_id}


class NotificationService:
    """The notification feed and its channel deliveries.

    ``record()`` writes the row and runs its severity class; the recipient's
    ``users.settings.communication`` (channel booleans, the per-category
    matrix, quiet hours, escalation minutes) is applied at every channel
    step, never by the caller.
    """

    def __init__(self) -> None:
        self._db: Any = None
        self._email_service: Any = None
        self._notification_feed: Any = None
        self._available = False

        self._cockpit_url = os.getenv(
            "COCKPIT_EXTERNAL_URL", "http://localhost:4200"
        ).rstrip("/")

        # Initialize transports (each checks its own env vars)
        self._transports = {
            "ntfy": NtfyTransport(),
            "slack_webhook": SlackWebhookTransport(),
            "discord_webhook": DiscordWebhookTransport(),
        }

        configured = [name for name, t in self._transports.items() if t.is_configured]
        if configured:
            logger.info("Webhook transports configured: %s", ", ".join(configured))
        else:
            logger.info("No webhook transports configured. Email-only notifications.")

    @property
    def is_available(self) -> bool:
        return self._available

    @property
    def configured_transports(self) -> list[str]:
        """Names of transports that have valid configuration."""
        return [name for name, t in self._transports.items() if t.is_configured]

    def connect(
        self,
        db: Any,
        email_service: Any,
        notification_feed: Any = None,
    ) -> None:
        """Store references to collaborating services.

        Args:
            db: PostgresDB instance
            email_service: EmailService instance for SMTP delivery
            notification_feed: NotificationFeedService for SSE broadcast (optional)
        """
        self._db = db
        self._email_service = email_service
        self._notification_feed = notification_feed
        self._available = True
        logger.info("NotificationService initialized")

    # =========================================================================
    # The unified feed (record / act / engagement / resolution)
    # =========================================================================

    async def record(
        self,
        *,
        recipient_id: str,
        category: str,
        dedup_key: str,
        subject: str,
        body: str = "",
        recipient_kind: str = "user",
        source_kind: str | None = None,
        source_id: str | None = None,
        severity: str | None = None,
        actions: list[dict[str, Any]] | None = None,
        action_params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> RecordResult:
        """Record that something happened that ``recipient`` has a stake in.

        Idempotent on ``(recipient_kind, recipient_id, dedup_key)``: a replay
        finds the existing row, broadcasts nothing, and re-attempts only the
        channel deliveries that never got a ``sent`` claim. Callers never see
        or influence delivery beyond the returned outcome dict (D1).

        The severity class decides what happens beyond the feed row: its
        immediate steps run here; its deferred steps (``normal``: wait the
        officer's response window, then mail only if ``not_seen`` and
        ``not_resolved``) are written with the row and run by the sweeper.
        """
        if not self._available:
            raise RuntimeError("NotificationService not initialized")
        if recipient_kind not in RECIPIENT_KINDS:
            raise ValueError(f"unknown recipient_kind {recipient_kind!r}")
        if not dedup_key:
            raise ValueError("dedup_key is required")
        if (source_kind is None) != (source_id is None):
            raise ValueError("source_kind and source_id go together")
        spec = category_spec(category)
        resolved_severity = normalize_severity(spec, severity)

        nid = str(mint_notification_id(recipient_kind, str(recipient_id), dedup_key))
        row: dict[str, Any] = {
            "id": nid,
            "recipient_kind": recipient_kind,
            "recipient_id": str(recipient_id),
            "category": category,
            "severity": resolved_severity,
            "subject": subject,
            "body": body or "",
            "source_kind": source_kind,
            "source_id": str(source_id) if source_id is not None else None,
            "dedup_key": dedup_key,
            "actions": (
                list(actions)
                if actions is not None
                else serialize_actions(spec, action_params)
            ),
            "payload": dict(payload or {}),
            # Provisional: the DB default is authoritative; the cockpit upserts
            # by id and the next feed load corrects the millisecond drift.
            "created_at": datetime.now(timezone.utc),
            "seen_at": None,
            "read_at": None,
            "interacted_at": None,
            "resolved_at": None,
            "resolved_by": None,
            "archived_at": None,
        }

        steps = steps_for(spec, resolved_severity)
        planned: list[dict[str, Any]] = []
        if recipient_kind == "user" and any(not s.immediate for s in steps):
            planned = await self._plan_deferred_steps(row, steps)

        stored_id, inserted = await self._persist_notification(
            row, steps=planned or None
        )
        row["id"] = stored_id
        if inserted:
            self._broadcast_notification(row)
        deliveries = await self._deliver_immediate(row, spec, inserted=inserted)
        if planned:
            deliveries["scheduled"] = {
                p["step_kind"]: p["due_at"].isoformat() for p in planned
            }
        logger.info(
            "notification %s %s for %s:%s (%s/%s)%s",
            stored_id[:8],
            "recorded" if inserted else "replayed",
            recipient_kind,
            str(recipient_id)[:8],
            category,
            resolved_severity,
            (
                f" — {len(planned)} step(s) due {planned[0]['due_at'].isoformat()}"
                if planned
                else ""
            ),
        )
        return RecordResult(stored_id, inserted, deliveries)

    async def act(
        self,
        *,
        notification_id: str,
        user: dict[str, Any],
        action_type: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run a declared action through its registered handler and stamp the
        row. The center posts ``{action_type, params}`` and knows nothing
        else; category meaning lives entirely in the handler (D7)."""
        row = await self._db.get_notification(notification_id)
        if (
            not row
            or row.get("recipient_kind") != "user"
            or str(row.get("recipient_id")) != str(user.get("id"))
        ):
            raise NotificationNotFound(notification_id)
        declared = next(
            (a for a in row.get("actions") or [] if a.get("type") == action_type),
            None,
        )
        if declared is None:
            raise ActionNotDeclared(action_type)
        handler = action_handler(row["category"], action_type)
        if handler is None:
            raise ActionUnregistered(f"{row['category']}/{action_type}")

        # Server-declared params win over anything the client sends; the
        # client contributes only the collected input (feedback, reason, …).
        merged = dict(params or {})
        merged.update(declared.get("params") or {})
        context = ActionContext(notification=row, user=user, params=merged, db=self._db)
        result = await handler(context)

        updated = await self._db.stamp_notification_interacted(row["id"])
        if result.resolve:
            updated = await self._db.resolve_notification(
                row["id"], resolved_by=result.resolved_by or f"user:{user.get('id')}"
            )
        updated = updated or row
        self._broadcast_update(
            str(updated["recipient_id"]),
            {
                "id": str(updated["id"]),
                **{
                    k: serialize_notification(updated)[k]
                    for k in (
                        "seen_at",
                        "read_at",
                        "interacted_at",
                        "resolved_at",
                        "resolved_by",
                    )
                },
            },
        )
        return {
            "result": result.result,
            "notification": serialize_notification(updated),
        }

    async def mark_seen(
        self, *, recipient_kind: str, recipient_id: str, ids: list[str]
    ) -> list[str]:
        stamped = await self._db.mark_notifications_seen(
            recipient_kind=recipient_kind, recipient_id=recipient_id, ids=ids
        )
        for entry in stamped:
            seen_at = entry.get("seen_at")
            self._broadcast_update(
                str(recipient_id),
                {
                    "id": str(entry["id"]),
                    "seen_at": seen_at.isoformat() if seen_at else None,
                },
            )
        return [str(entry["id"]) for entry in stamped]

    async def mark_read(
        self, *, recipient_kind: str, recipient_id: str, notification_id: str
    ) -> dict[str, Any] | None:
        row = await self._db.mark_notification_read_v2(
            recipient_kind=recipient_kind,
            recipient_id=recipient_id,
            notification_id=notification_id,
        )
        if row is None:
            return None
        wire = serialize_notification(row)
        self._broadcast_update(
            str(recipient_id),
            {"id": wire["id"], "seen_at": wire["seen_at"], "read_at": wire["read_at"]},
        )
        return wire

    async def archive(
        self, *, recipient_kind: str, recipient_id: str, notification_id: str
    ) -> dict[str, Any] | None:
        row = await self._db.archive_notification(
            recipient_kind=recipient_kind,
            recipient_id=recipient_id,
            notification_id=notification_id,
        )
        if row is None:
            return None
        wire = serialize_notification(row)
        self._broadcast_update(
            str(recipient_id), {"id": wire["id"], "archived_at": wire["archived_at"]}
        )
        return wire

    async def resolve_source(
        self, source_kind: str, source_id: str, *, resolved_by: str
    ) -> list[str]:
        """The underlying thing was settled — by a user, the officer, or a
        sweeper. Stamp every open row about it, whoever it belongs to (D6).
        Best-effort by design: called from state-change hooks that must not
        fail because the feed did."""
        if not self._available or not self._db:
            return []
        try:
            rows = await self._db.resolve_notifications_by_source(
                source_kind=source_kind,
                source_id=str(source_id),
                resolved_by=resolved_by,
            )
        except Exception as e:
            logger.warning(
                "resolve_source(%s, %s) failed: %s", source_kind, source_id, e
            )
            return []
        for row in rows:
            wire = serialize_notification(row)
            self._broadcast_update(
                str(row["recipient_id"]),
                {
                    "id": wire["id"],
                    "resolved_at": wire["resolved_at"],
                    "resolved_by": wire["resolved_by"],
                },
            )
        return [str(row["id"]) for row in rows]

    async def get_feed_page(
        self,
        *,
        recipient_kind: str,
        recipient_id: str,
        before: str | None = None,
        limit: int = 50,
        categories: list[str] | None = None,
        status: str = "all",
        source_kind: str | None = None,
        source_id: str | None = None,
    ) -> dict[str, Any]:
        rows, next_before = await self._db.list_notifications_page(
            recipient_kind=recipient_kind,
            recipient_id=recipient_id,
            before=before,
            limit=limit,
            categories=categories,
            status=status,
            source_kind=source_kind,
            source_id=source_id,
        )
        counts = await self._db.get_notification_counts(
            recipient_kind=recipient_kind, recipient_id=recipient_id
        )
        return {
            "items": [serialize_notification(r) for r in rows],
            "next_before": next_before,
            "counts": counts,
        }

    async def get_counts(
        self, *, recipient_kind: str, recipient_id: str
    ) -> dict[str, Any]:
        return await self._db.get_notification_counts(
            recipient_kind=recipient_kind, recipient_id=recipient_id
        )

    # --- stubbable seams (tests build NotificationService.__new__ and replace these) ---

    async def _persist_notification(
        self, row: dict[str, Any], *, steps: list[dict[str, Any]] | None = None
    ) -> tuple[str, bool]:
        inserted = await self._db.insert_notification_once(
            notification_id=row["id"],
            recipient_kind=row["recipient_kind"],
            recipient_id=row["recipient_id"],
            category=row["category"],
            severity=row["severity"],
            subject=row["subject"],
            body=row["body"],
            source_kind=row["source_kind"],
            source_id=row["source_id"],
            dedup_key=row["dedup_key"],
            actions=row["actions"],
            payload=row["payload"],
            steps=steps or None,
        )
        return row["id"], bool(inserted)

    async def _defer_steps(
        self, notification_id: str, steps: list[dict[str, Any]]
    ) -> int:
        """Quiet hours: park the immediate steps until the window ends. The
        insert is idempotent per step index, so a replay cannot stack a
        second promise. Failure here is logged, not raised — the feed row
        already exists and the in-app signal is intact."""
        try:
            return int(await self._db.insert_notification_steps(notification_id, steps))
        except Exception as e:
            logger.warning(
                "could not defer notification %s steps: %s", notification_id[:8], e
            )
            return 0

    async def _claim_delivery(
        self,
        notification_id: str,
        channel: str,
        *,
        address: str | None,
        step_index: int | None = None,
        batch_id: str | None = None,
    ) -> str | None:
        return await self._db.claim_notification_delivery(
            notification_id=notification_id,
            channel=channel,
            recipient_address=address,
            step_index=step_index,
            batch_id=batch_id,
        )

    async def _settle_delivery(
        self,
        delivery_id: str,
        *,
        state: str,
        provider_msg_id: str | None = None,
        error: str | None = None,
    ) -> None:
        await self._db.settle_notification_delivery(
            delivery_id, state=state, provider_msg_id=provider_msg_id, error=error
        )

    async def _record_suppressed(
        self, notification_id: str, channel: str, reason: str
    ) -> None:
        """A channel that was deliberately not attempted still gets a row, so
        "why did no mail go out" is answerable. A suppressed row does not hold
        the claim slot."""
        try:
            claim = await self._claim_delivery(notification_id, channel, address=None)
            if claim:
                await self._settle_delivery(claim, state="suppressed", error=reason)
        except Exception as e:
            logger.debug("suppressed-delivery row failed (%s): %s", channel, e)

    async def _get_user(self, user_id: str) -> dict[str, Any] | None:
        if not self._db:
            return None
        try:
            return await self._db.get_user(str(user_id))
        except Exception:
            return None

    def _broadcast_notification(self, row: dict[str, Any]) -> None:
        if not self._notification_feed or row.get("recipient_kind") != "user":
            return
        try:
            self._notification_feed.broadcast(
                user_id=str(row["recipient_id"]),
                event_type="notification",
                data={"notification": serialize_notification(row)},
            )
        except Exception as e:
            logger.debug("notification SSE broadcast failed: %s", e)

    def _broadcast_update(self, recipient_id: str, patch: dict[str, Any]) -> None:
        if not self._notification_feed:
            return
        try:
            self._notification_feed.broadcast(
                user_id=str(recipient_id),
                event_type="notification.updated",
                data=patch,
            )
        except Exception as e:
            logger.debug("notification.updated SSE broadcast failed: %s", e)

    def _channel_deliverable(self, channel: str) -> bool:
        """Is there any transport behind this channel at all? Channels with
        nothing behind them get no delivery rows — a suppressed row answers
        "why did this not go out", and "the operator never configured Slack"
        is not a per-notification question."""
        if channel == "email":
            return self._email_service is not None and bool(
                getattr(self._email_service, "is_configured", True)
            )
        if channel in WEBHOOK_CHANNELS:
            transport = self._transports.get(channel)
            return bool(transport and transport.is_configured)
        return False  # 'push' has no v1 transport (non-goal)

    async def _deliver_immediate(
        self, row: dict[str, Any], spec: Any, *, inserted: bool
    ) -> dict[str, Any]:
        """Run the zero-delay channel steps of the row's severity class.

        Always runs — including on replay — because a crash between a send
        and the completion journal's mark replays the callback; the claim
        ledger decides per channel whether anything is actually sent. Rows
        that were deliberately not attempted (preference off, no address)
        are recorded as ``suppressed`` only by the inserting call so a replay
        does not pile up duplicates. Quiet hours never drop a step: they
        defer it to the window's end as a ``not_resolved``-gated step row.
        """
        results: dict[str, Any] = {"in_app": True}
        steps = steps_for(spec, row["severity"])
        immediate = [step for step in steps if step.immediate]
        if not immediate or row["recipient_kind"] != "user":
            return results

        nid = row["id"]
        recipient_id = row["recipient_id"]
        deliverable = [s for s in immediate if self._channel_deliverable(s.channel)]
        if not deliverable:
            return results
        channels = await self._get_user_channels(recipient_id)
        settings = await self._get_user_settings(recipient_id)
        categories = ((settings or {}).get("communication") or {}).get("categories")
        wanted = [
            s
            for s in deliverable
            if channel_enabled(channels, categories, row["category"], s.channel)
        ]
        if inserted:
            for step in deliverable:
                if step not in wanted:
                    await self._record_suppressed(nid, step.channel, "preference")
        if not wanted:
            return results

        if not bypasses_quiet_hours(spec, row["severity"]) and self._is_in_quiet_hours(
            settings
        ):
            resume_at = self.next_quiet_hours_end(settings) or (
                datetime.now(timezone.utc) + timedelta(hours=1)
            )
            await self._defer_steps(
                nid,
                [
                    {
                        "step_index": DEFERRED_STEP_INDEX_BASE + steps.index(step),
                        "step_kind": step.channel,
                        "due_at": resume_at,
                        "conditions": ["not_resolved"],
                        "batch_key": None,
                    }
                    for step in wanted
                ],
            )
            results["deferred_until"] = resume_at.isoformat()
            return results

        user = await self._get_user(recipient_id)
        for step in wanted:
            results.update(
                await self._send_one(
                    row,
                    step.channel,
                    user=user,
                    subject=row["subject"],
                    body=row["body"],
                    cockpit_path=f"/inbox?n={nid}",
                    inserted=inserted,
                )
            )
        return results

    async def _send_one(
        self,
        row: dict[str, Any],
        channel: str,
        *,
        user: dict[str, Any] | None,
        subject: str,
        body: str,
        cockpit_path: str,
        inserted: bool,
    ) -> dict[str, Any]:
        """Claim, send, settle — one channel of one row."""
        nid = row["id"]
        address = None
        user = self._mail_recipient(row.get("payload") or {}, user)
        if channel == "email":
            address = (user or {}).get("email")
            if not address:
                if inserted:
                    await self._record_suppressed(nid, "email", "no_email")
                return {}
        claim = await self._claim_delivery(nid, channel, address=address)
        if claim is None:
            return {channel: "already_delivered"}
        ok, msg_id, error = await self._send_channel(
            channel,
            user=user,
            subject=subject,
            body=body,
            cockpit_path=cockpit_path,
            payload=row.get("payload") or {},
            source_id=row.get("source_id"),
        )
        await self._settle_delivery(
            claim,
            state="sent" if ok else "failed",
            provider_msg_id=msg_id,
            error=error,
        )
        out: dict[str, Any] = {channel: bool(ok)}
        if msg_id:
            out["email_message_id"] = msg_id
        return out

    async def _send_channel(
        self,
        channel: str,
        *,
        user: dict[str, Any] | None,
        subject: str,
        body: str,
        cockpit_path: str,
        payload: dict[str, Any],
        source_id: str | None,
    ) -> tuple[bool, str | None, str | None]:
        """The provider call for one channel: ``(ok, provider_msg_id, error)``.
        Never raises — a channel failure is a settled ``failed`` delivery,
        not a lost notification."""
        if channel == "email":
            try:
                ok, msg_id = await self._email_service.send_notification_email(
                    to=(user or {}).get("email") or "",
                    to_name=(user or {}).get("display_name") or "User",
                    subject=subject,
                    body_md=body,
                    cockpit_path=cockpit_path,
                    reply_to=self._reply_address(payload),
                )
                if ok and msg_id:
                    await self._stamp_ledger(payload, msg_id)
                return bool(ok), msg_id, None if ok else "send returned False"
            except Exception as e:
                logger.warning("notification email failed: %s", e)
                return False, None, str(e)
        transport = self._transports.get(channel)
        if transport is None:
            return False, None, f"no transport for {channel}"
        try:
            ok = bool(
                await transport.send(
                    NotificationPayload(
                        subject=subject,
                        body_text=body,
                        job_id=str(payload.get("job_id") or source_id or ""),
                        job_description=str(payload.get("job_description") or ""),
                        config_name=str(payload.get("config_name") or ""),
                        thread_id=payload.get("thread_id"),
                        cockpit_url=f"{self._cockpit_url}{cockpit_path}",
                    )
                )
            )
            return ok, None, None if ok else "send returned False"
        except Exception as e:
            logger.warning("notification %s transport failed: %s", channel, e)
            return False, None, str(e)

    @staticmethod
    def _mail_recipient(
        payload: dict[str, Any], user: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        """The address a row's mail goes to: the recipient's own, unless the
        row carries a ``deliver_to`` override (an agent message addressed to
        a project contact without a user row)."""
        override = (payload or {}).get("deliver_to") or {}
        if override.get("email"):
            return {
                **(user or {}),
                "email": override["email"],
                "display_name": override.get("name")
                or (user or {}).get("display_name"),
            }
        return user

    def _reply_address(self, payload: dict[str, Any]) -> str | None:
        """Agent messages are answerable by mail: the row's ``reply_routing``
        (job + thread) becomes the ``+job+thread`` sub-addressed Reply-To the
        IMAP poller routes on. Every other category has no reply lane."""
        routing = (payload or {}).get("reply_routing") or {}
        if not routing or not self._email_service:
            return None
        helper = getattr(self._email_service, "reply_address", None)
        if helper is None:
            return None
        try:
            return helper(
                str(routing.get("job_id") or ""), str(routing.get("thread_id") or "")
            )
        except Exception:
            return None

    async def _stamp_ledger(self, payload: dict[str, Any], msg_id: str) -> None:
        """A sent agent-message email stamps its Message-ID onto the ledger
        row (``message_log.email_message_id``) so an ``In-Reply-To`` reply
        resolves to the thread. Immediate sends and the sweeper's deferred
        sends both pass through here. Best-effort."""
        ledger_id = (payload or {}).get("message_log_id")
        if not ledger_id or not self._db:
            return
        try:
            await self._db.set_message_email_id(str(ledger_id), msg_id)
        except Exception as e:
            logger.debug("ledger Message-ID stamp failed (%s): %s", ledger_id, e)

    # --- deferred steps (slice 2) ----------------------------------------------

    async def _plan_deferred_steps(
        self, row: dict[str, Any], steps: tuple[Any, ...]
    ) -> list[dict[str, Any]]:
        """Turn the class's non-immediate steps into rows for
        ``notification_steps``: the delay becomes ``due_at`` (D5 — a delay is
        not its own row), batched steps round up to their window bucket, and
        the conditions ride along to be evaluated when due, not now."""
        settings = await self._get_user_settings(row["recipient_id"])
        now = datetime.now(timezone.utc)
        officer_minutes: int | None = None
        planned: list[dict[str, Any]] = []
        for index, step in enumerate(steps):
            if step.immediate:
                continue
            if step.delay == DELAY_OFFICER_RESPONSE:
                if officer_minutes is None:
                    officer_minutes = await self._resolve_delay_minutes(row, settings)
                minutes = officer_minutes
            else:
                minutes = int(step.delay)
            due = now + timedelta(minutes=minutes)
            if step.batch_window_minutes:
                due = bucket_due_at(due, step.batch_window_minutes)
            planned.append(
                {
                    "step_index": index,
                    "step_kind": step.channel,
                    "due_at": due,
                    "conditions": list(step.conditions),
                    "batch_key": batch_key_for(step, row),
                }
            )
        return planned

    async def _resolve_delay_minutes(
        self, row: dict[str, Any], settings: dict[str, Any] | None
    ) -> int:
        """D6: wait as long as the project's officer is allowed to take — the
        mail that survives that window says the automated tier did not
        settle it. Without a live, un-held officer the recipient's own
        ``communication.escalation_minutes`` applies, default 5."""
        if row.get("source_kind") == "job" and self._db:
            try:
                minutes = await self._officer_response_minutes(str(row["source_id"]))
            except Exception as e:
                logger.warning(
                    "officer window lookup failed for job %s: %s",
                    str(row.get("source_id"))[:8],
                    e,
                )
                minutes = None
            if minutes is not None:
                return minutes
        configured = ((settings or {}).get("communication") or {}).get(
            "escalation_minutes"
        )
        lo, hi = ESCALATION_MINUTES_BOUNDS
        if (
            isinstance(configured, int)
            and not isinstance(configured, bool)
            and lo <= configured <= hi
        ):
            return configured
        return NO_OFFICER_DELAY_MINUTES

    async def _officer_response_minutes(self, job_id: str) -> int | None:
        """The commissioned, un-held officer's response window for the job's
        project, or ``None`` when nobody but the human can settle it. Deliberately
        independent of the worker-message policy: an officer reviews jobs even
        for a project whose messages go user-direct."""
        from orchestrator.services import message_routing as routing_svc

        job = await self._db.get_job(job_id)
        project_id = (job or {}).get("project_id")
        if not project_id:
            return None
        post = await self._db.get_project_officer(str(project_id))
        if not post:
            return None
        officer = await self._db.get_officer_thread_for_project(str(project_id))
        if not officer or routing_svc.officer_hold(officer) is not None:
            return None
        policy = post.get("communication_policy") or {}
        if isinstance(policy, str):
            try:
                policy = json.loads(policy)
            except ValueError:
                policy = {}
        lo, hi = routing_svc.OFFICER_RESPONSE_MINUTES_BOUNDS
        try:
            value = int(
                policy.get(
                    "officer_response_minutes",
                    routing_svc.DEFAULT_OFFICER_RESPONSE_MINUTES,
                )
            )
        except (TypeError, ValueError):
            value = routing_svc.DEFAULT_OFFICER_RESPONSE_MINUTES
        return min(max(value, lo), hi)

    async def _source_resolved(self, source_kind: str | None, source_id: Any) -> bool:
        """Ask the registered probe whether the source is settled. Unknown
        kinds and probe failures read as *not* resolved: the failure mode
        stays "occasionally mails about something just settled", never
        "silently never mails"."""
        probe = source_probe(source_kind)
        if probe is None or not self._db or source_id is None:
            return False
        try:
            return bool(await probe(self._db, str(source_id)))
        except Exception as e:
            logger.warning("source probe %s/%s failed: %s", source_kind, source_id, e)
            return False

    async def describe_steps(self, notification_id: str) -> list[dict[str, Any]]:
        if not self._db:
            return []
        rows = await self._db.list_notification_steps(notification_id)
        return [serialize_step(r) for r in rows]

    @staticmethod
    def render_step_message(
        members: list[dict[str, Any]], *, cockpit_url: str
    ) -> tuple[str, str, str]:
        """``(subject, body_md, cockpit_path)`` for a due group. One member is
        the row itself; several become one digest ("3 review queue items
        waiting") that links each row's own deep link (D8 batching)."""
        if len(members) == 1:
            row = members[0]
            return (
                str(row.get("subject") or ""),
                str(row.get("body") or ""),
                f"/inbox?n={row['notification_id']}",
            )
        category = str(members[0].get("category") or "notification")
        label = category.replace("_", " ")
        subject = f"{len(members)} {label} items waiting for you"
        lines = [f"You have **{len(members)}** {label} items nobody has settled yet:\n"]
        for row in members:
            excerpt = str(row.get("body") or "").strip().splitlines()
            first = excerpt[0].strip() if excerpt else ""
            if len(first) > 160:
                first = first[:157] + "…"
            lines.append(
                f"- **{row.get('subject') or '(no subject)'}** — "
                f"[open]({cockpit_url}/inbox?n={row['notification_id']})"
            )
            if first:
                lines.append(f"  {first}")
        return subject, "\n".join(lines), "/inbox"

    async def send_step_group(
        self, members: list[dict[str, Any]], *, channel: str
    ) -> dict[str, Any]:
        """The send half of a due step group (called by the sweeper).

        Claims one delivery per member BEFORE sending (D10) so a member whose
        channel already went out — an earlier attempt that crashed after the
        provider call — is left out rather than mailed twice; renders one
        message for the survivors; settles every claim the same way.
        """
        batch_id = str(uuid.uuid4())
        recipient_id = str(members[0]["recipient_id"])
        owner = await self._get_user(recipient_id)
        outcome: dict[str, Any] = {
            "batch_id": batch_id,
            "attempted": [],
            "already": [],
            "unaddressed": [],
            "ok": True,
            "error": None,
        }
        # A `deliver_to` override (a message the agent addressed to a project
        # contact) mails that address; everything else goes to the recipient.
        # One message per distinct address, so a batch never leaks one
        # contact's message into another's digest.
        by_address: dict[
            str | None, tuple[dict[str, Any] | None, list[dict[str, Any]]]
        ] = {}
        for member in members:
            user = self._mail_recipient(member.get("payload") or {}, owner)
            address = (user or {}).get("email") if channel == "email" else None
            if channel == "email" and not address:
                outcome["unaddressed"].append(member["id"])
                continue
            by_address.setdefault(address, (user, []))[1].append(member)
        if not by_address:
            return outcome

        errors: list[str] = []
        for address, (user, group) in by_address.items():
            attempted: list[dict[str, Any]] = []
            claims: list[str] = []
            for member in group:
                claim = await self._claim_delivery(
                    str(member["notification_id"]),
                    channel,
                    address=address,
                    step_index=int(member["step_index"]),
                    batch_id=batch_id,
                )
                if claim is None:
                    outcome["already"].append(member["id"])
                    continue
                attempted.append(member)
                claims.append(claim)
            if not attempted:
                continue

            subject, body, cockpit_path = self.render_step_message(
                attempted, cockpit_url=self._cockpit_url
            )
            first = attempted[0]
            ok, msg_id, error = await self._send_channel(
                channel,
                user=user,
                subject=subject,
                body=body,
                cockpit_path=cockpit_path,
                payload=first.get("payload") or {},
                source_id=first.get("source_id"),
            )
            for claim in claims:
                await self._settle_delivery(
                    claim,
                    state="sent" if ok else "failed",
                    provider_msg_id=msg_id,
                    error=error,
                )
            outcome["attempted"].extend(m["id"] for m in attempted)
            if not ok:
                outcome["ok"] = False
                errors.append(error or "send failed")
        outcome["error"] = "; ".join(errors) if errors else None
        return outcome

    # =========================================================================
    # Producer helpers — every one is a thin shape over record() (D1)
    # =========================================================================

    async def record_agent_message(
        self,
        *,
        user_id: str,
        job: dict[str, Any],
        job_id: str,
        thread_id: str,
        sequence: int | None,
        subject: str,
        message_md: str,
        blocking: bool = False,
        message_log_id: str | None = None,
        dedup_key: str | None = None,
        severity: str | None = None,
        reason_line: str | None = None,
        deliver_to: tuple[str | None, str | None] | None = None,
    ) -> RecordResult:
        """A worker's message to its owner. ``normal`` by default (the mail
        waits for the officer's window and goes only if nobody looked and
        nobody answered); a *blocking* message parks the job, so it is
        ``high``. The row carries the ledger row id so the deferred email's
        Message-ID can be stamped onto ``message_log`` for reply routing.
        ``deliver_to`` (address, name) overrides the mail's recipient when the
        agent addressed a project contact who has no user row — the feed row
        still belongs to the owner, the party with the stake."""
        key = dedup_key or (
            f"message:{thread_id}:{sequence}"
            if sequence is not None
            else f"message:{thread_id}:{uuid.uuid4()}"
        )
        # Worker text (audit OC-05): the subject and message are the worker's
        # own words, and can carry a credential it or a page it read supplied.
        # Redacted here, where the untrusted part is known — never over the
        # assembled body, which would also strip the links the server built.
        # An escalation arrives already sanitized and counts nothing twice.
        subject = sanitize_text(subject)
        clean_message = sanitize(message_md)
        message_md = clean_message.text
        if clean_message.redacted:
            message_md += (
                f"\n\n_({clean_message.count} secret-shaped value(s) redacted "
                "from the worker's message.)_"
            )
        payload: dict[str, Any] = {
            "job_id": str(job_id),
            "thread_id": str(thread_id),
            "sequence": sequence,
            "job_description": sanitize_text(job.get("description") or "")[:100],
            "config_name": str(job.get("config_name") or "worker_base"),
            "blocking": bool(blocking),
            "reply_routing": {"job_id": str(job_id), "thread_id": str(thread_id)},
        }
        if message_log_id:
            payload["message_log_id"] = str(message_log_id)
        if reason_line:
            payload["reason_line"] = reason_line
        if deliver_to and deliver_to[0]:
            payload["deliver_to"] = {"email": deliver_to[0], "name": deliver_to[1]}
        return await self.record(
            recipient_id=str(user_id),
            category="agent_message",
            severity=severity or ("high" if blocking else None),
            dedup_key=key,
            subject=subject,
            body=message_md,
            source_kind="message_thread",
            source_id=str(thread_id),
            action_params={"job_id": str(job_id), "thread_id": str(thread_id)},
            payload=payload,
        )

    async def record_review_returned(
        self,
        *,
        user_id: str,
        job_id: str,
        config_name: str,
        reason: str | None = None,
    ) -> RecordResult:
        """Automated review ended without approving; a human must decide.

        Two callers, both meaning "nothing was approved, it is yours now":
        the stale-verification sweeper (no ``reason`` — the cause is the
        pipeline itself) and the verification gate's escalation (``reason``
        is the same text written to the job's ``error_message``).
        """
        label = config_name or "job"
        subject = "Job needs manual review — automated verification failed"
        # The reviewer's text is agent-written: it rides both the body and the
        # payload the feed row and SSE frame carry.
        raw_reason = reason
        reason = sanitize_text(reason) if reason else reason
        if reason:
            body_md = (
                f"Automated verification for your **{label}** job stopped without "
                f"approving it, so it needs **manual review**.\n\n"
                f"> {reason}\n\n"
                f"Approve it or send it back with feedback."
            )
        else:
            body_md = (
                f"Automated verification for your **{label}** job did not complete "
                f"(the review pipeline died), so it has been returned to **manual "
                f"review**. Approve it or send it back with feedback."
            )
        digest = hashlib.sha1((raw_reason or "pipeline").encode("utf-8")).hexdigest()[
            :8
        ]
        return await self.record(
            recipient_id=str(user_id),
            category="review_queue",
            dedup_key=f"review_returned:{job_id}:{digest}",
            subject=subject,
            body=body_md,
            source_kind="job",
            source_id=str(job_id),
            action_params={"job_id": str(job_id)},
            payload={
                "job_id": str(job_id),
                "config_name": config_name,
                "reason": reason or "",
                "returned_to_manual": True,
            },
        )

    async def record_automation_disabled(
        self,
        *,
        user_id: str,
        automation_id: str,
        automation_name: str,
        reason: str,
    ) -> RecordResult:
        """An automation tripped a safety guard and was paused. One row per
        automation per day — the same guard trips on every tick until fixed."""
        display_name = automation_name or "(unnamed)"
        today = datetime.now(timezone.utc).date().isoformat()
        # A guard's reason can quote the failure it tripped on (a job error
        # echoing a credential-bearing remote); body and payload both carry it.
        reason = sanitize_text(reason)
        return await self.record(
            recipient_id=str(user_id),
            category="automation_disabled",
            dedup_key=f"automation_disabled:{automation_id}:{today}",
            subject=f"Automation '{display_name}' was auto-paused",
            body=(
                f"Your automation **{display_name}** was paused automatically "
                f"because: {reason}\n\n"
                f"Edit the automation to fix the underlying issue and re-enable it."
            ),
            source_kind="automation",
            source_id=str(automation_id),
            action_params={"automation_id": str(automation_id)},
            payload={
                "automation_id": str(automation_id),
                "automation_name": automation_name,
                "reason": reason,
            },
        )

    async def record_user_registered(
        self,
        *,
        new_user_id: str,
        display_name: str | None = None,
        email: str | None = None,
    ) -> dict[str, Any]:
        """A new user registered and awaits approval: one row per admin
        (app-side admission — an admin decides who gets in). The legacy
        ``user_registered`` SSE frame keeps flowing so the admin Users page
        refreshes live; it is not a notification, it is a page-refresh
        signal. Best-effort; failures are logged, never raised."""
        if not self._available or not self._db:
            return {"error": "NotificationService not initialized"}
        try:
            admin_ids = await self._db.list_admin_user_ids()
        except Exception as e:
            logger.warning("Could not list admins for registration notify: %s", e)
            return {"error": "admin lookup failed"}

        label = display_name or email or "A new user"
        suffix = f" ({email})" if email else ""
        results: dict[str, Any] = {"notified": 0, "notification_ids": []}
        for admin_id in admin_ids:
            if str(admin_id) == str(new_user_id):
                continue  # a self-registered admin isn't pending anyway
            try:
                outcome = await self.record(
                    recipient_id=str(admin_id),
                    category="user_registered",
                    dedup_key=f"user_registered:{new_user_id}",
                    subject=f"New user pending approval: {label}",
                    body=(
                        f"**{label}**{suffix} just registered and is awaiting "
                        "approval. Review pending users on the Users page."
                    ),
                    source_kind="user",
                    source_id=str(new_user_id),
                    action_params={"user_id": str(new_user_id)},
                    payload={
                        "user_id": str(new_user_id),
                        "display_name": display_name,
                        "email": email,
                    },
                )
                results["notified"] += 1
                results["notification_ids"].append(outcome.notification_id)
            except Exception as e:
                logger.warning(
                    "Registration notify for admin %s failed: %s", admin_id, e
                )
            if self._notification_feed:
                try:
                    self._notification_feed.broadcast(
                        user_id=str(admin_id),
                        event_type="user_registered",
                        data={
                            "user_id": str(new_user_id),
                            "display_name": display_name,
                            "email": email,
                            "cockpit_url": f"{self._cockpit_url}/admin/users",
                        },
                    )
                except Exception as e:
                    logger.warning("SSE broadcast failed for user_registered: %s", e)
        return results

    # --- recipient preferences -------------------------------------------------

    async def _get_user_channels(self, user_id: str) -> dict[str, bool]:
        """Load user's channel preferences from settings JSONB."""
        defaults = {"email": True, "cockpit": True}
        if not self._db:
            return defaults
        try:
            settings = await self._db.get_user_settings(user_id)
            channels = (settings or {}).get("communication", {}).get("channels", {})
            return {**defaults, **channels}
        except Exception:
            return defaults

    async def _get_user_settings(self, user_id: str) -> dict:
        """Load full user settings."""
        if not self._db:
            return {}
        try:
            return await self._db.get_user_settings(user_id) or {}
        except Exception:
            return {}

    @staticmethod
    def _is_in_quiet_hours(user_settings: dict) -> bool:
        """Check if current time falls within user's quiet hours."""
        return quiet_hours_window(user_settings)[0]

    @staticmethod
    def next_quiet_hours_end(user_settings: dict) -> datetime | None:
        """When the current quiet-hours window ends (UTC), or ``None`` when
        the recipient is not in one."""
        return quiet_hours_window(user_settings)[1]


# Module-level singleton
notification_service = NotificationService()
