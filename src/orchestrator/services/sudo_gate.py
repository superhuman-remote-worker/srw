"""Sudo Approval Gate — Orchestrator-side service.

Handles:
  1. NATS subscription for sudo.request.> — receives approval requests from daemons
  2. Auto-approval rule evaluation (fnmatch, shell metacharacter check)
  3. Expiration sweeper — denies timed-out requests
  4. SSE event broadcasting — pushes new requests to cockpit clients
  5. Database operations for sudo_approval_requests and sudo_auto_rules

This module is optional — if NATS is unavailable, the gate simply doesn't exist
and agents operate with unrestricted sudo (the pre-gate default).
"""

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from fnmatch import fnmatch
from typing import Any, Optional
from uuid import UUID

from shared.content_redaction import sanitize_data, sanitize_text
from shared.sudo_command_line import render_sudo_command_line

logger = logging.getLogger(__name__)

# Shell metacharacters that prevent auto-approval.
# Commands containing these are always forwarded to human review.
_SHELL_META_RE = re.compile(r"[|;&`$><]|\$\(|\|\||&&")


def _ttl_seconds_from_env(name: str, default: int) -> int:
    """Read a positive TTL from the environment, falling back to ``default``."""
    try:
        value = int(os.environ[name])
    except (KeyError, TypeError, ValueError):
        return default
    return value if value > 0 else default


# How long a pending sudo command request stays decidable. At 300 s every
# request an unattended VM job raised expired before an operator could look at
# it, and the agent simply re-issued the command; VM-tier work waits for a
# human, so the default is 30 minutes. Other request types carry their own TTL
# (vm_upgrade: 24 h).
SUDO_COMMAND_TTL_SECONDS = _ttl_seconds_from_env("SUDO_COMMAND_TTL_SECONDS", 1800)


class SudoRequestConflict(ValueError):
    """The HTTP idempotency key is already bound to a different payload."""


class SudoEntityUnavailable(ValueError):
    """The VM entity disappeared or changed generation during authentication."""


@dataclass(frozen=True, slots=True)
class SudoOpenResult:
    request_id: str
    status: str
    reason: str | None
    expires_at: Any
    created: bool


def _public_status(value: object) -> str:
    return {
        "auto_approved": "approved",
        "auto_denied": "denied",
    }.get(str(value), str(value))


def _expires_at(ttl_seconds: int) -> str:
    """The expiry an approver is shown for a request just inserted.

    The row's authoritative ``expires_at`` is ``NOW() + ttl`` in the database;
    this mirrors it for the event payload without a second round trip (the
    insert and this call are milliseconds apart).
    """
    return (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()


def _object(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


class SudoGateService:
    """Manages sudo approval requests and auto-approval rules."""

    def __init__(self):
        self._db: Optional[Any] = None
        self._nc: Optional[Any] = None  # NATS connection (from NatsBridge)
        self._sse_queues: list[asyncio.Queue] = []
        self._lock = asyncio.Lock()
        self._pending_msgs: dict[str, Any] = {}  # request_id → NATS msg for respond()

    def connect(self, db: Any, nc: Optional[Any] = None) -> None:
        """Bind to database and optional NATS connection."""
        self._db = db
        self._nc = nc

    async def open_request(self, identity: Any, body: Any) -> SudoOpenResult:
        """Idempotently create and evaluate an HTTP guest sudo request."""

        if not self._db:
            raise RuntimeError("sudo gate is not connected")
        payload = (
            body.model_dump(mode="json") if hasattr(body, "model_dump") else dict(body)
        )
        payload["provision_generation"] = identity.provision_generation
        client_request_id = str(payload["request_id"])
        reply_subject = f"http:{client_request_id}"
        entity = (
            await self._db.get_thread(identity.entity_id)
            if identity.entity_type == "thread"
            else await self._db.get_job(identity.entity_id)
        )
        if not isinstance(entity, Mapping):
            raise SudoEntityUnavailable("VM entity is no longer available")
        container = (
            entity.get("metadata")
            if identity.entity_type == "thread"
            else entity.get("context")
        )
        vm = _object(_object(container).get("vm"))
        if vm.get("provision_generation") != identity.provision_generation:
            raise SudoEntityUnavailable("VM generation is no longer current")

        existing = await self._get_request_by_reply_subject(reply_subject)
        if existing:
            self._assert_http_entity(existing, identity)
            self._assert_same_http_payload(existing, payload)
            return self._open_result(existing, created=False)

        existing_id = await self._get_request(client_request_id)
        if existing_id:
            if existing_id.get("nats_reply_subject") != reply_subject:
                raise SudoRequestConflict(
                    "request_id is already bound to a different request"
                )
            self._assert_http_entity(existing_id, identity)
            self._assert_same_http_payload(existing_id, payload)
            return self._open_result(existing_id, created=False)

        vm_name = vm.get("vm_name") or "vm"
        job_id = identity.entity_id if identity.entity_type == "job" else None
        thread_id = identity.entity_id if identity.entity_type == "thread" else None
        request_id = await self._insert_request(
            job_id=job_id,
            thread_id=thread_id,
            request_id=client_request_id,
            vm_name=vm_name,
            command=payload["command"],
            arguments=payload["argv"],
            cwd=payload["cwd"],
            requesting_user=payload["user"],
            target_user=payload["runas_user"],
            nats_reply_subject=reply_subject,
            metadata=payload,
        )
        if request_id is None:
            existing = await self._get_request_by_reply_subject(reply_subject)
            if existing:
                self._assert_http_entity(existing, identity)
                self._assert_same_http_payload(existing, payload)
                return self._open_result(existing, created=False)
            existing_id = await self._get_request(client_request_id)
            if existing_id:
                raise SudoRequestConflict(
                    "request_id is already bound to a different request"
                )
            raise RuntimeError("sudo request claim disappeared")

        command_string = (
            " ".join(payload["argv"]) if payload["argv"] else payload["command"]
        )
        auto_result = await self._evaluate_auto_rules(
            command_string,
            requesting_user=payload["user"],
            target_user=payload["runas_user"],
        )
        decided_status: str | None = None
        reason: str | None = None
        if auto_result in {"approve", "deny"}:
            decided_status = (
                "auto_approved" if auto_result == "approve" else "auto_denied"
            )
            reason = (
                "auto-approval rule" if auto_result == "approve" else "auto-denial rule"
            )
            await self._finalize_request(
                request_id, decided_status, reason, "system", reply_subject
            )
        else:
            event = {
                "id": request_id,
                "job_id": job_id,
                "thread_id": thread_id,
                "vm_name": vm_name,
                "command": payload["command"],
                "arguments": payload["argv"],
                "requesting_user": payload["user"],
                "target_user": payload["runas_user"],
                "working_directory": payload["cwd"],
                "requested_at": datetime.now(timezone.utc).isoformat(),
                "expires_at": _expires_at(SUDO_COMMAND_TTL_SECONDS),
                "request_type": "sudo_command",
            }
            await self._broadcast_sse("new_request", event)
            await self._record_owner_notification(
                request_id, job_id=job_id, thread_id=thread_id, event=event
            )
            await self._notify_project_officer(
                request_id,
                job_id,
                payload["command"],
                "sudo_command",
                thread_id=thread_id,
            )

        row = await self._get_request(request_id)
        if row:
            return self._open_result(row, created=True)
        return SudoOpenResult(
            request_id,
            "pending",
            None,
            None,
            True,
        )

    async def wait_for_decision(
        self,
        request_id: str,
        max_wait: float,
        *,
        entity_type: str | None = None,
        entity_id: str | None = None,
        provision_generation: str | None = None,
    ) -> dict[str, Any] | None:
        """Poll row truth without retaining a pool connection during sleeps."""

        deadline = asyncio.get_running_loop().time() + min(max(float(max_wait), 0), 30)
        while True:
            row = await self._get_scoped_request(
                request_id,
                entity_type=entity_type,
                entity_id=entity_id,
                provision_generation=provision_generation,
            )
            if not row:
                return None
            result = {
                "request_id": str(row["id"]),
                "status": _public_status(row["status"]),
                "reason": row.get("decision_reason"),
            }
            remaining = deadline - asyncio.get_running_loop().time()
            if result["status"] != "pending" or remaining <= 0:
                return result
            await asyncio.sleep(min(1.0, remaining))

    async def count_pending_for_entity(self, identity: Any) -> int:
        if not self._db:
            return 0
        column = "thread_id" if identity.entity_type == "thread" else "job_id"
        async with self._db.acquire() as conn:
            return int(
                await conn.fetchval(
                    f"SELECT count(*) FROM sudo_approval_requests "
                    f"WHERE {column} = $1 AND status = 'pending' "
                    "AND expires_at > NOW()",
                    identity.entity_id,
                )
                or 0
            )

    async def http_request_exists(self, identity: Any, request_id: str) -> bool:
        row = await self._get_request_by_reply_subject(f"http:{request_id}")
        if not row:
            return False
        self._assert_http_entity(row, identity)
        return True

    @staticmethod
    def _assert_http_entity(row: Mapping[str, Any], identity: Any) -> None:
        column = "thread_id" if identity.entity_type == "thread" else "job_id"
        metadata = _object(row.get("metadata"))
        if (
            str(row.get(column) or "") != identity.entity_id
            or metadata.get("provision_generation") != identity.provision_generation
        ):
            raise SudoRequestConflict("request_id is already bound to another entity")

    @staticmethod
    def _assert_same_http_payload(
        row: Mapping[str, Any], payload: Mapping[str, Any]
    ) -> None:
        if (
            row.get("command") != payload.get("command")
            or list(row.get("arguments") or []) != list(payload.get("argv") or [])
            or (row.get("working_directory") or "") != (payload.get("cwd") or "")
        ):
            raise SudoRequestConflict("request_id is already bound to another payload")

    @staticmethod
    def _open_result(row: Mapping[str, Any], *, created: bool) -> SudoOpenResult:
        return SudoOpenResult(
            str(row["id"]),
            _public_status(row["status"]),
            row.get("decision_reason"),
            row.get("expires_at"),
            created,
        )

    # =========================================================================
    # NATS subscription handler
    # =========================================================================

    async def on_sudo_request(self, msg) -> None:
        """Handle sudo.request.> — a daemon is requesting approval.

        The handler:
          1. Parses the request payload
          2. Inserts a row in sudo_approval_requests with msg.reply stored
          3. Evaluates auto-approval rules
          4. If auto-match: publishes response immediately
          5. If no match: pushes to SSE, waits for human decision

        This handler returns immediately — it does NOT block. The NATS reply
        subject is persisted in the DB for async response later.
        """
        try:
            data = json.loads(msg.data.decode())
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error("Malformed sudo request payload: %s", e)
            return

        entity_id = data.get("job_id", "")
        vm_id = data.get("vm_id", "")
        command = data.get("command", "")
        argv = data.get("argv", [])
        user = data.get("user", "unknown")
        runas_user = data.get("runas_user", "root")
        cwd = data.get("cwd", "")

        logger.info(
            "Sudo request: job=%s vm=%s user=%s cmd=%s",
            entity_id,
            vm_id,
            command,
            " ".join(argv),
        )

        # Store the request with the NATS reply subject. This is also the claim:
        # the NATS sudo subject fans out to every replica (no queue group), so
        # both run this handler; _insert_request claims the request on its unique
        # reply subject (migration 0040) and only the winner proceeds. (HA / M2-L4)
        reply_subject = msg.reply if hasattr(msg, "reply") else None
        try:
            from orchestrator.services.vm_guest_events import resolve_vm_entity

            identity = await resolve_vm_entity(self._db, entity_id)
            if identity is None:
                raise SudoEntityUnavailable("unknown VM entity")
            job_id = identity.entity_id if identity.entity_type == "job" else None
            thread_id = identity.entity_id if identity.entity_type == "thread" else None
            request_id = await self._insert_request(
                job_id=job_id,
                thread_id=thread_id,
                vm_name=vm_id,
                command=command,
                arguments=argv,
                cwd=cwd,
                requesting_user=user,
                target_user=runas_user,
                nats_reply_subject=reply_subject,
                metadata={
                    **data,
                    "provision_generation": identity.provision_generation,
                },
            )
        except Exception as e:
            # Genuine DB failure — deny so the daemon doesn't hang.
            logger.error("Sudo request insert failed: %s", e)
            await self._nats_reply(reply_subject, False, "internal error")
            return

        if request_id is None:
            # Lost the claim to the other replica — it owns this request; drop
            # silently (do NOT deny: the winner responds).
            logger.debug("Sudo request already claimed by another replica; dropping")
            return

        # Evaluate auto-approval rules.
        cmd_string = " ".join(argv) if argv else command
        auto_result = await self._evaluate_auto_rules(
            cmd_string, requesting_user=user, target_user=runas_user
        )

        if auto_result == "approve":
            logger.info("Auto-approved: %s (request %s)", cmd_string, request_id)
            await self._finalize_request(
                request_id,
                "auto_approved",
                "auto-approval rule",
                "system",
                reply_subject,
            )
            # Also respond directly via msg for immediate delivery
            try:
                await msg.respond(
                    json.dumps(
                        {"approved": True, "reason": "auto-approval rule"}
                    ).encode()
                )
            except Exception:
                pass
            return

        if auto_result == "deny":
            logger.info("Auto-denied: %s (request %s)", cmd_string, request_id)
            await self._finalize_request(
                request_id,
                "auto_denied",
                "auto-denial rule",
                "system",
                reply_subject,
            )
            try:
                await msg.respond(
                    json.dumps(
                        {"approved": False, "reason": "auto-denial rule"}
                    ).encode()
                )
            except Exception:
                pass
            return

        # No auto-match — store msg for later respond() and push to SSE.
        if request_id:
            self._pending_msgs[request_id] = msg

        event = {
            "id": str(request_id),
            "job_id": job_id,
            "thread_id": thread_id,
            "vm_name": vm_id,
            "command": command,
            "arguments": argv,
            "requesting_user": user,
            "target_user": runas_user,
            "working_directory": cwd,
            "requested_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": _expires_at(SUDO_COMMAND_TTL_SECONDS),
        }
        await self._broadcast_sse("new_request", event)
        await self._notify_project_officer(
            str(request_id),
            job_id,
            render_sudo_command_line(command, argv),
            "sudo_command",
            thread_id=thread_id,
        )

    # =========================================================================
    # Approval / Denial (from REST endpoints)
    # =========================================================================

    async def approve_request(
        self, request_id: str, reason: str = "", decided_by: str = "operator"
    ) -> Optional[dict]:
        """Approve a pending sudo request."""
        row = await self._get_request(request_id)
        if not row:
            return None
        if row["status"] != "pending":
            return {"error": f"Request status is '{row['status']}', not 'pending'"}

        reply_subject = row.get("nats_reply_subject")
        await self._finalize_request(
            request_id,
            "approved",
            reason,
            decided_by,
            reply_subject,
        )

        # Respond via the stored NATS msg object (most reliable for cross-leaf)
        msg = self._pending_msgs.pop(request_id, None)
        if msg:
            try:
                await msg.respond(
                    json.dumps({"approved": True, "reason": reason}).encode()
                )
                logger.info("Responded via msg.respond() for request %s", request_id)
            except Exception as e:
                logger.warning("msg.respond() failed for %s: %s", request_id, e)

        await self._broadcast_sse(
            "request_decided",
            {
                "id": str(request_id),
                "job_id": str(row["job_id"]) if row.get("job_id") else None,
                "thread_id": str(row["thread_id"]) if row.get("thread_id") else None,
                "status": "approved",
                "decided_by": decided_by,
            },
        )

        return {
            "id": str(request_id),
            "status": "approved",
            "job_id": str(row["job_id"]) if row.get("job_id") else None,
            "thread_id": str(row["thread_id"]) if row.get("thread_id") else None,
        }

    async def deny_request(
        self, request_id: str, reason: str = "", decided_by: str = "operator"
    ) -> Optional[dict]:
        """Deny a pending sudo request."""
        row = await self._get_request(request_id)
        if not row:
            return None
        if row["status"] != "pending":
            return {"error": f"Request status is '{row['status']}', not 'pending'"}

        reply_subject = row.get("nats_reply_subject")
        await self._finalize_request(
            request_id,
            "denied",
            reason,
            decided_by,
            reply_subject,
        )

        # Respond via the stored NATS msg object
        msg = self._pending_msgs.pop(request_id, None)
        if msg:
            try:
                await msg.respond(
                    json.dumps({"approved": False, "reason": reason}).encode()
                )
                logger.info("Responded via msg.respond() for request %s", request_id)
            except Exception as e:
                logger.warning("msg.respond() failed for %s: %s", request_id, e)

        await self._broadcast_sse(
            "request_decided",
            {
                "id": str(request_id),
                "job_id": str(row["job_id"]) if row.get("job_id") else None,
                "thread_id": str(row["thread_id"]) if row.get("thread_id") else None,
                "status": "denied",
                "decided_by": decided_by,
                "reason": reason,
            },
        )

        return {
            "id": str(request_id),
            "status": "denied",
            "job_id": str(row["job_id"]) if row.get("job_id") else None,
            "thread_id": str(row["thread_id"]) if row.get("thread_id") else None,
        }

    # =========================================================================
    # Expiration sweeper
    # =========================================================================

    async def sweep_expired(self) -> int:
        """Deny all pending requests past their TTL.

        Returns the number of expired requests processed.
        """
        if not self._db:
            return 0

        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    UPDATE sudo_approval_requests
                    SET status = 'expired',
                        decided_at = NOW(),
                        decided_by = 'system',
                        decision_reason = 'TTL expired'
                    WHERE status = 'pending' AND expires_at < NOW()
                    RETURNING id, job_id, thread_id, nats_reply_subject
                    """
                )

            count = 0
            for row in rows:
                req_id = str(row["id"])
                reply_subject = row.get("nats_reply_subject")
                # Try msg.respond() first, fallback to publish
                msg = self._pending_msgs.pop(req_id, None)
                if msg:
                    try:
                        await msg.respond(
                            json.dumps(
                                {"approved": False, "reason": "approval timed out"}
                            ).encode()
                        )
                    except Exception:
                        await self._nats_reply(
                            reply_subject, False, "approval timed out"
                        )
                else:
                    await self._nats_reply(reply_subject, False, "approval timed out")
                count += 1

                await self._resolve_notification_source(
                    req_id, resolved_by="system:sudo_expired"
                )
                await self._broadcast_sse(
                    "request_decided",
                    {
                        "id": str(row["id"]),
                        "job_id": str(row["job_id"]) if row.get("job_id") else None,
                        "thread_id": (
                            str(row["thread_id"]) if row.get("thread_id") else None
                        ),
                        "status": "expired",
                        "decided_by": "system",
                    },
                )

            if count > 0:
                logger.info("Expired %d sudo approval request(s)", count)
            return count
        except Exception as e:
            logger.error("Expiration sweep failed: %s", e)
            return 0

    # =========================================================================
    # Auto-approval rules
    # =========================================================================

    async def _evaluate_auto_rules(
        self,
        command: str,
        *,
        requesting_user: Optional[str] = None,
        target_user: Optional[str] = None,
    ) -> Optional[str]:
        """Evaluate auto-approval rules against a command string.

        Returns "approve", "deny", "review", or None (no match).
        """
        if not self._db:
            return None

        # Shell metacharacter check — require human review, except when the
        # target user IS the requesting user: `sudo -u <self>` grants nothing
        # the caller does not already have, so there is no escalation for a
        # human to judge (an agent re-entering a login shell to pick up a group
        # it was just added to). The rules table still decides.
        self_targeted = bool(requesting_user) and requesting_user == target_user
        if not self_targeted and _SHELL_META_RE.search(command):
            logger.debug(
                "Shell metacharacters in '%s' — skipping auto-approval", command
            )
            return None

        try:
            async with self._db.acquire() as conn:
                rules = await conn.fetch(
                    """
                    SELECT pattern, action FROM sudo_auto_rules
                    WHERE enabled = TRUE
                    ORDER BY priority ASC
                    """
                )

            for rule in rules:
                if fnmatch(command, rule["pattern"]):
                    return rule["action"]

            return None
        except Exception as e:
            logger.error("Auto-rule evaluation failed: %s", e)
            return None

    async def list_rules(self) -> list[dict]:
        """List all auto-approval rules."""
        if not self._db:
            return []
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM sudo_auto_rules ORDER BY priority ASC"
                )
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("Failed to list auto-rules: %s", e)
            return []

    async def create_rule(
        self,
        pattern: str,
        action: str,
        priority: int = 100,
        description: str = "",
        created_by: str = "operator",
    ) -> Optional[dict]:
        """Create an auto-approval rule."""
        if action not in ("approve", "deny", "review"):
            return None
        if not self._db:
            return None

        try:
            async with self._db.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    INSERT INTO sudo_auto_rules (pattern, action, priority, description, created_by)
                    VALUES ($1, $2, $3, $4, $5)
                    RETURNING *
                    """,
                    pattern,
                    action,
                    priority,
                    description,
                    created_by,
                )
            return dict(row) if row else None
        except Exception as e:
            logger.error("Failed to create auto-rule: %s", e)
            return None

    async def delete_rule(self, rule_id: str) -> bool:
        """Delete an auto-approval rule."""
        if not self._db:
            return False
        try:
            async with self._db.acquire() as conn:
                result = await conn.execute(
                    "DELETE FROM sudo_auto_rules WHERE id = $1",
                    rule_id,
                )
            return "DELETE 1" in result
        except Exception as e:
            logger.error("Failed to delete auto-rule %s: %s", rule_id, e)
            return False

    # =========================================================================
    # Query
    # =========================================================================

    async def list_requests(
        self,
        job_id: Optional[str] = None,
        status: Optional[str] = None,
        request_type: Optional[str] = None,
        limit: int = 50,
    ) -> list[dict]:
        """List sudo approval requests with optional filters."""
        if not self._db:
            return []

        conditions = []
        params = []
        idx = 1

        if job_id:
            conditions.append(f"job_id = ${idx}")
            params.append(job_id)
            idx += 1
        if status:
            conditions.append(f"status = ${idx}")
            params.append(status)
            idx += 1
        if request_type:
            conditions.append(f"request_type = ${idx}")
            params.append(request_type)
            idx += 1

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    f"""
                    SELECT id, job_id, thread_id, vm_name, command, arguments,
                           working_directory, requesting_user, target_user,
                           status, requested_at, decided_at, decided_by,
                           decision_reason, ttl_seconds, expires_at, metadata,
                           request_type
                    FROM sudo_approval_requests
                    {where}
                    ORDER BY requested_at DESC
                    LIMIT ${idx}
                    """,
                    *params,
                    limit,
                )
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("Failed to list sudo requests: %s", e)
            return []

    async def get_request(self, request_id: str) -> Optional[dict]:
        """Get a single sudo approval request."""
        row = await self._get_request(request_id)
        return dict(row) if row else None

    # =========================================================================
    # SSE broadcasting
    # =========================================================================

    def subscribe_sse(self) -> asyncio.Queue:
        """Create a new SSE subscription queue.

        Returns a Queue that receives (event_type, data_dict) tuples.
        """
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._sse_queues.append(q)
        return q

    def unsubscribe_sse(self, q: asyncio.Queue) -> None:
        """Remove an SSE subscription queue."""
        try:
            self._sse_queues.remove(q)
        except ValueError:
            pass

    async def _notify_project_officer(
        self,
        request_id: str,
        job_id: Any,
        command: str,
        request_type: str,
        *,
        thread_id: Any = None,
    ) -> None:
        """Wake the project's officer about a pending approval. Never raises.

        The sudo TTL (300s) usually expires before a DOWN officer can be
        respawned, so this is a latency win for a LIVE officer, not a promise
        of coverage — the human notification path remains the fallback
        (centurion implementation notes, S4).
        """
        if not self._db or not (job_id or thread_id):
            return
        try:
            from orchestrator.services import session_wake

            entity = (
                await self._db.get_thread(str(thread_id))
                if thread_id
                else await self._db.get_job(str(job_id))
            )
            project_id = (entity or {}).get("project_id")
            if not project_id:
                return
            enqueued = await session_wake.notify_officer(
                self._db,
                str(project_id),
                source="sudo_request",
                dedup_key=f"sudo:{request_id}",
                payload={
                    "request_id": str(request_id),
                    "job_id": str(job_id) if job_id else None,
                    "thread_id": str(thread_id) if thread_id else None,
                    "request_type": request_type,
                    "summary": (command or "")[:160],
                },
            )
            if enqueued:
                session_wake.kick_event_drain(self._db)
        except Exception:
            logger.warning(
                "sudo gate: officer notify failed for request %s (non-fatal)",
                request_id,
                exc_info=True,
            )

    async def _broadcast_sse(self, event_type: str, data: dict) -> None:
        """Push an event to all connected SSE clients."""
        dead = []
        for q in self._sse_queues:
            try:
                q.put_nowait((event_type, data))
            except asyncio.QueueFull:
                dead.append(q)
        for q in dead:
            self._sse_queues.remove(q)

    # =========================================================================
    # Internal helpers
    # =========================================================================

    async def _insert_request(
        self,
        job_id: Optional[str],
        vm_name: str,
        command: str,
        arguments: list,
        cwd: str,
        requesting_user: str,
        target_user: str,
        nats_reply_subject: Optional[str],
        metadata: dict,
        *,
        thread_id: Optional[str] = None,
        request_id: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
    ) -> Optional[str]:
        """Claim-and-insert a sudo approval request.

        Returns the new request id on success, or None when another replica
        already claimed this request. The NATS sudo subject fans out to every
        replica (no queue group), so both run this; the unique reply subject is
        the per-request claim key (migration 0040 / uq_sudo_request_reply_subject).
        Raises on a genuine DB error so the caller can deny rather than silently
        drop — a None must mean "someone else owns it", never "the insert failed".
        """
        if not self._db:
            return None
        async with self._db.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO sudo_approval_requests
                    (id, job_id, thread_id, vm_name, command, arguments, working_directory,
                     requesting_user, target_user, nats_reply_subject, metadata,
                     ttl_seconds, expires_at)
                VALUES (COALESCE($1::uuid, gen_random_uuid()), $2, $3, $4, $5,
                        $6, $7, $8, $9, $10, $11, $12::integer,
                        NOW() + ($12::integer * INTERVAL '1 second'))
                ON CONFLICT DO NOTHING
                RETURNING id
                """,
                request_id,
                job_id,
                thread_id,
                vm_name,
                command,
                arguments,
                cwd,
                requesting_user,
                target_user,
                nats_reply_subject,
                json.dumps(metadata),
                int(ttl_seconds or SUDO_COMMAND_TTL_SECONDS),
            )
        return str(row["id"]) if row else None

    async def insert_vm_upgrade_request(
        self,
        job_id: str,
        command: str,
        reason: str = "",
        config_name: str = "",
        status: str = "pending",
        decision_reason: str = "",
    ) -> Optional[str]:
        """Insert a vm_upgrade request into sudo_approval_requests.

        Called when a job freezes with freeze_type='vm_upgrade_required'.
        Unlike NATS sudo requests, these have no reply subject and use
        a long TTL (24h — operator decides in their own time).

        ``status`` other than ``pending`` (e.g. ``auto_denied`` when the job
        owner can never satisfy the request) records an already-decided row
        for audit parity — decided by ``system``, no ``new_request`` SSE
        (there is nothing for an operator to act on).

        Returns the request ID, or None on failure.
        """
        if not self._db:
            return None
        try:
            metadata = json.dumps(
                {
                    "freeze_type": "vm_upgrade_required",
                    "reason": reason,
                }
            )
            async with self._db.acquire() as conn:
                if status == "pending":
                    row = await conn.fetchrow(
                        """
                        INSERT INTO sudo_approval_requests
                            (job_id, vm_name, command, arguments, working_directory,
                             requesting_user, target_user, nats_reply_subject, metadata,
                             request_type, ttl_seconds, expires_at)
                        VALUES ($1, $2, $3, '{}', '', 'agent', 'root', NULL, $4,
                                'vm_upgrade', 86400,
                                NOW() + INTERVAL '86400 seconds')
                        RETURNING id
                        """,
                        job_id,
                        config_name or "container",
                        command,
                        metadata,
                    )
                else:
                    row = await conn.fetchrow(
                        """
                        INSERT INTO sudo_approval_requests
                            (job_id, vm_name, command, arguments, working_directory,
                             requesting_user, target_user, nats_reply_subject, metadata,
                             request_type, ttl_seconds, expires_at,
                             status, decided_at, decided_by, decision_reason)
                        VALUES ($1, $2, $3, '{}', '', 'agent', 'root', NULL, $4,
                                'vm_upgrade', 86400,
                                NOW() + INTERVAL '86400 seconds',
                                $5::sudo_request_status, NOW(), 'system', $6)
                        RETURNING id
                        """,
                        job_id,
                        config_name or "container",
                        command,
                        metadata,
                        status,
                        decision_reason,
                    )
            request_id = str(row["id"]) if row else None

            if request_id and status == "pending":
                event = {
                    "id": request_id,
                    "job_id": job_id,
                    "vm_name": config_name or "container",
                    "command": command,
                    "arguments": [],
                    "requesting_user": "agent",
                    "target_user": "root",
                    "request_type": "vm_upgrade",
                    "requested_at": datetime.now(timezone.utc).isoformat(),
                }
                await self._broadcast_sse("new_request", event)
                await self._notify_project_officer(
                    request_id, job_id, command, "vm_upgrade"
                )

            return request_id
        except Exception as e:
            logger.error("Failed to insert vm_upgrade request: %s", e)
            return None

    async def _record_owner_notification(
        self,
        request_id: str,
        *,
        job_id: Optional[str],
        thread_id: Optional[str],
        event: dict,
    ) -> None:
        """The owner's feed row for a pending request (unified notification
        system). ``sudo_request`` is ``critical`` with push-only steps — a
        300 s TTL makes mail pointless. Resolved by ``_finalize_request`` and
        the expiry sweep. Best-effort: the request itself is already durable."""
        if not self._db:
            return
        try:
            from orchestrator.services.notification_service import notification_service

            owner = None
            if job_id:
                job = await self._db.get_job(str(job_id))
                owner = (job or {}).get("user_id")
            elif thread_id:
                thread = await self._db.get_thread(str(thread_id))
                owner = (thread or {}).get("user_id")
            if not owner:
                return
            # The command line is the agent's (audit OC-05) and can carry a
            # token it read: redacted for the body AND the payload the feed
            # row and SSE frame ship. Approval acts on the request row, whose
            # command stays exact — only this notice is a view.
            full = sanitize_text(
                render_sudo_command_line(event.get("command"), event.get("arguments"))
            )
            event = sanitize_data(dict(event)).value
            expires_at = event.get("expires_at")
            await notification_service.record(
                recipient_id=str(owner),
                category="sudo_request",
                dedup_key=f"sudo_request:{request_id}",
                subject=f"Sudo approval needed: {full[:60]}",
                body=(
                    f"`{full}` on **{event.get('vm_name') or 'vm'}** as "
                    f"`{event.get('target_user') or 'root'}` (requested by "
                    f"`{event.get('requesting_user') or 'agent'}` in "
                    f"`{event.get('working_directory') or '/'}`)."
                    + (f" Expires {expires_at}." if expires_at else "")
                ),
                source_kind="sudo_request",
                source_id=str(request_id),
                action_params={
                    "request_id": str(request_id),
                    "job_id": str(job_id) if job_id else None,
                    "thread_id": str(thread_id) if thread_id else None,
                },
                payload=dict(event),
            )
        except Exception as e:
            logger.warning(
                "sudo request %s: owner notification failed (non-fatal): %s",
                str(request_id)[:8],
                e,
            )

    async def _get_request(self, request_id: str):
        """Fetch a single request row."""
        if not self._db:
            return None
        try:
            async with self._db.acquire() as conn:
                return await conn.fetchrow(
                    "SELECT * FROM sudo_approval_requests WHERE id = $1",
                    request_id,
                )
        except Exception as e:
            logger.error("Failed to get sudo request %s: %s", request_id, e)
            return None

    async def _get_request_by_reply_subject(self, reply_subject: str):
        if not self._db:
            return None
        async with self._db.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM sudo_approval_requests WHERE nats_reply_subject = $1",
                reply_subject,
            )

    async def _get_scoped_request(
        self,
        request_id: str,
        *,
        entity_type: str | None,
        entity_id: str | None,
        provision_generation: str | None = None,
    ):
        if not self._db:
            return None
        try:
            request_uuid = UUID(str(request_id))
        except (TypeError, ValueError):
            return None
        if entity_type not in {"job", "thread"} or not entity_id:
            return await self._get_request(str(request_uuid))
        column = "thread_id" if entity_type == "thread" else "job_id"
        async with self._db.acquire() as conn:
            if provision_generation is not None:
                return await conn.fetchrow(
                    f"SELECT * FROM sudo_approval_requests "
                    f"WHERE id = $1 AND {column} = $2 "
                    "AND metadata->>'provision_generation' = $3",
                    request_uuid,
                    entity_id,
                    provision_generation,
                )
            return await conn.fetchrow(
                f"SELECT * FROM sudo_approval_requests WHERE id = $1 AND {column} = $2",
                request_uuid,
                entity_id,
            )

    async def _finalize_request(
        self,
        request_id: str,
        status: str,
        reason: str,
        decided_by: str,
        nats_reply_subject: Optional[str],
    ) -> None:
        """Set the final status and send the NATS reply."""
        if self._db:
            try:
                async with self._db.acquire() as conn:
                    await conn.execute(
                        """
                        UPDATE sudo_approval_requests
                        SET status = $2, decided_at = NOW(),
                            decided_by = $3, decision_reason = $4
                        WHERE id = $1 AND status = 'pending'
                        """,
                        request_id,
                        status,
                        decided_by,
                        reason,
                    )
            except Exception as e:
                logger.error("Failed to finalize sudo request %s: %s", request_id, e)

        await self._resolve_notification_source(
            request_id, resolved_by=f"sudo_{status}:{decided_by}"
        )
        approved = status in ("approved", "auto_approved")
        await self._nats_reply(nats_reply_subject, approved, reason)

    @staticmethod
    async def _resolve_notification_source(
        request_id: str, *, resolved_by: str
    ) -> None:
        """Settle every feed row about this request (unified notification
        system, D6). Best-effort — the decision is already committed."""
        try:
            from orchestrator.services.notification_service import notification_service

            if notification_service.is_available:
                await notification_service.resolve_source(
                    "sudo_request", str(request_id), resolved_by=resolved_by
                )
        except Exception as e:
            logger.warning(
                "notification resolve failed for sudo request %s: %s", request_id, e
            )

    async def _nats_reply(
        self, reply_subject: Optional[str], approved: bool, reason: str = ""
    ) -> None:
        """Publish an approval/denial to the daemon via the stored reply subject."""
        if not reply_subject or reply_subject.startswith("http:") or not self._nc:
            return

        response = {"approved": approved}
        if reason:
            response["reason"] = reason

        try:
            logger.info("Publishing NATS reply to %s: %s", reply_subject, response)
            await self._nc.publish(reply_subject, json.dumps(response).encode())
            await self._nc.flush()
            logger.info("NATS reply published and flushed to %s", reply_subject)
        except Exception as e:
            logger.error("Failed to publish NATS reply to %s: %s", reply_subject, e)


# Singleton instance
sudo_gate = SudoGateService()
