"""Email magic-link generation + outbound notification dispatch for
headless persistent sessions.

Phase 4 of knowledge-base/knowledge/features/headless_persistent_sessions.md. When the agent's
permission_check inserts a row into thread_permission_requests and nobody
has decided it for >30s, the permission-pending sweeper
(src/orchestrator/main.py) calls record_permission_pending() here. We:

  1. Generate two magic-link tokens — one for "approve", one for "deny".
     Plaintext lives only in the notification body; the DB stores SHA-256
     hashes.
  2. Record one `session_permission` row on the owner's feed (unified
     notification system). The feed delivers it — mail now, with the links —
     dedups on the request id, and resolves it when the gate is decided.
     (`thread_notifications` is retired; the delivery ledger is
     `notification_deliveries`.)

Magic-link click flow (HTTP routes in src/orchestrator/main.py):

  GET /magic/approve/{token}
    → hash + look up the token row → render confirmation HTML showing
      the tool, args, and a single POST button. We deliberately do NOT
      execute the decision on GET because email-link prefetchers (Outlook
      Safe Links, Gmail link preview) auto-fetch every link.

  POST /magic/approve/{token}
    → validate_magic_link → consume_magic_link (CAS UPDATE used_at) →
      UPDATE thread_permission_requests via the same DB trigger that the
      cockpit and REST paths use → render success HTML.

Token security:
  - Opaque random 32-byte token (secrets.token_urlsafe). NOT a JWT —
    single-use enforcement forces server state anyway, and JWT adds
    algorithm-confusion footguns without compensating benefit.
  - DB stores SHA-256(token) hex. A DB leak does not yield usable tokens.
  - 30-minute expiry. AWS Step Functions allows up to 30 days for slow
    approval flows, but persistent-session permission requests are
    expected to be acted on quickly.
  - Bound to a specific approval_id. A leaked token cannot be replayed
    against a different request.
  - Single-use via CAS: UPDATE ... WHERE used_at IS NULL. Double-clicks
    land on the "already used" path with a friendly response.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from shared.content_redaction import sanitize_data, sanitize_text


logger = logging.getLogger(__name__)


# Module-level config knobs. Overridable via env for ops tuning without
# changing call sites.
MAGIC_LINK_TTL_SECONDS: int = int(os.environ.get("MAGIC_LINK_TTL_S", "1800"))  # 30 min


def _hash_token(raw_token: str) -> str:
    """SHA-256 of the raw token, hex-encoded. The only form stored in DB."""
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


async def generate_magic_link_token(
    db: Any,
    *,
    purpose: str,
    user_id: Optional[str],
    approval_id: Optional[str],
    thread_id: Optional[str],
    intended_decision: Optional[str] = None,
    ttl_seconds: int = MAGIC_LINK_TTL_SECONDS,
) -> tuple[str, str]:
    """Generate a fresh opaque token, store its hash, return (raw, token_id).

    The raw token is what we embed in the email href. token_id is the row
    UUID, useful for logging / observability. The raw token is never
    persisted in plaintext — only its hash is.
    """
    if intended_decision is not None and intended_decision not in (
        "approved",
        "denied",
    ):
        raise ValueError(
            f"intended_decision must be 'approved'/'denied'/None, got "
            f"{intended_decision!r}"
        )
    raw = secrets.token_urlsafe(32)
    token_hash = _hash_token(raw)
    expires_at = _now_utc() + timedelta(seconds=ttl_seconds)

    async with db.acquire() as conn:
        row_id = await conn.fetchval(
            "INSERT INTO magic_link_tokens "
            "(token_hash, purpose, user_id, approval_id, thread_id, "
            " intended_decision, expires_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7) "
            "RETURNING id",
            token_hash,
            purpose,
            user_id,
            approval_id,
            thread_id,
            intended_decision,
            expires_at,
        )
    return raw, str(row_id)


async def validate_magic_link(db: Any, raw_token: str) -> Optional[dict[str, Any]]:
    """Hash the incoming token, look up the row. Returns the row dict if
    the token is valid (exists, not expired, not used) — else None.

    Does NOT consume the token. Callers should call consume_magic_link()
    only on POST, never GET (email-link prefetch protection).
    """
    if not raw_token:
        return None
    token_hash = _hash_token(raw_token)
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, purpose, user_id, approval_id, thread_id, "
            "       intended_decision, expires_at, used_at, consumed_decision "
            "FROM magic_link_tokens "
            "WHERE token_hash = $1",
            token_hash,
        )
    if row is None:
        return None
    if row["used_at"] is not None:
        return None  # already consumed
    if row["expires_at"] < _now_utc():
        return None  # expired
    return dict(row)


async def consume_magic_link(
    db: Any, token_id: str, decision: str
) -> Optional[dict[str, Any]]:
    """CAS UPDATE: marks the token consumed iff still unused. Returns the
    updated row dict on success, None on contention (already consumed by
    a racing click — also the bot-prefetch case where the link was hit
    twice in close succession).

    Single-use enforcement: WHERE used_at IS NULL. Even if the same human
    clicks twice in 50ms, only one POST wins.
    """
    if decision not in ("approved", "denied"):
        return None
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE magic_link_tokens "
            "SET used_at = now(), consumed_decision = $2 "
            "WHERE id = $1 AND used_at IS NULL "
            "RETURNING id, purpose, user_id, approval_id, thread_id, "
            "          intended_decision, consumed_decision",
            token_id,
            decision,
        )
    return dict(row) if row else None


def _build_magic_link_url(cockpit_url: str, raw_token: str) -> str:
    """Compose the user-visible URL we embed in the email. The cockpit
    serves /magic/approve/{token} via the orchestrator proxy in
    production; locally it lands directly on the orchestrator port.
    """
    return f"{cockpit_url.rstrip('/')}/magic/approve/{urllib.parse.quote(raw_token, safe='')}"


def _truncate_args_for_email(tool_args: dict[str, Any], max_chars: int = 600) -> str:
    """Build a human-readable args preview, bounded for email body.

    The arguments are the agent's (audit OC-05): a command can carry a token
    it read. Redacted per value, before the cut, so a truncation cannot strand
    a fragment and the rendering stays valid JSON. The approve/deny links are
    the server's and are never passed through here.
    """
    tool_args = sanitize_data(tool_args).value
    try:
        rendered = json.dumps(tool_args, indent=2, default=str)
    except Exception:
        rendered = str(tool_args)
    if len(rendered) > max_chars:
        rendered = rendered[:max_chars] + "\n… (truncated)"
    return rendered


async def record_permission_pending(
    db: Any,
    notifier: Any,
    *,
    row: dict[str, Any],
    cockpit_external_url: str,
) -> dict[str, Any]:
    """One ``session_permission`` feed row for a pending gate the owner has
    not answered in-session (unified notification system).

    ``high``: the mail goes out now and carries the two magic links, so the
    owner can decide from a phone without signing in — the affordance the
    old ``thread_notifications`` email had. The row resolves when the gate is
    decided by any path (cockpit, magic link, the notification's own
    approve/deny action, the agent's LISTEN); a step that comes due later
    asks the live request before mailing.

    ``row`` is the sweeper's join of ``thread_permission_requests`` with the
    thread's ``user_id``/``title``. Returns ``{"status": recorded | replayed |
    skipped_no_owner, "notification_id"?}``.
    """
    thread_id = str(row["thread_id"])
    approval_id = str(row["id"])
    user_id = row.get("user_id")
    if not user_id:
        return {"status": "skipped_no_owner"}

    # Two tokens — one per decision. Both bound to the same approval_id and
    # expire on the same clock.
    approve_token, _ = await generate_magic_link_token(
        db,
        purpose="approve_permission",
        user_id=str(user_id),
        approval_id=approval_id,
        thread_id=thread_id,
        intended_decision="approved",
    )
    deny_token, _ = await generate_magic_link_token(
        db,
        purpose="approve_permission",
        user_id=str(user_id),
        approval_id=approval_id,
        thread_id=thread_id,
        intended_decision="denied",
    )

    requested_at = row.get("requested_at")
    if isinstance(requested_at, datetime):
        if requested_at.tzinfo is None:
            requested_at = requested_at.replace(tzinfo=timezone.utc)
        age_min = max(0, int((_now_utc() - requested_at).total_seconds() // 60))
    else:
        age_min = 0

    tool_args = row.get("tool_args")
    if isinstance(tool_args, str):
        try:
            tool_args = json.loads(tool_args)
        except Exception:
            tool_args = {}
    elif tool_args is None:
        tool_args = {}
    preview = _truncate_args_for_email(tool_args)
    tool_name = str(row.get("tool_name") or "a tool")
    # LLM-generated (auto-titled sessions); shown and stored in the payload.
    title = sanitize_text(str(row.get("title") or thread_id[:8]))
    approve_url = _build_magic_link_url(cockpit_external_url, approve_token)
    deny_url = _build_magic_link_url(cockpit_external_url, deny_token)
    session_link = f"{cockpit_external_url.rstrip('/')}/sessions/{thread_id}"

    # Labeled bare URLs, not markdown links: the email leg escapes the body
    # and ntfy/Slack get raw text — a bare URL is clickable in every leg.
    body = (
        f"**{tool_name}** is waiting for your approval in session **{title}** "
        f"(requested {age_min} min ago).\n\n"
        f"```\n{preview}\n```\n\n"
        f"Approve: {approve_url}\n"
        f"Deny: {deny_url}\n\n"
        f"These links need no sign-in and expire in "
        f"{MAGIC_LINK_TTL_SECONDS // 60} minutes. Session: {session_link}"
    )
    result = await notifier.record(
        recipient_id=str(user_id),
        category="session_permission",
        dedup_key=f"session_permission:{approval_id}",
        subject=f"Approval needed: {tool_name}",
        body=body,
        source_kind="permission_request",
        source_id=approval_id,
        action_params={"thread_id": thread_id, "request_id": approval_id},
        payload={
            "thread_id": thread_id,
            "request_id": approval_id,
            "tool_name": tool_name,
            "tool_args_preview": preview,
            "requested_at": (
                requested_at.isoformat() if isinstance(requested_at, datetime) else None
            ),
            "title": title,
        },
    )
    return {
        "status": "recorded" if result.inserted else "replayed",
        "notification_id": result.notification_id,
    }
