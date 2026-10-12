"""Opaque runtime-actor credential minting and authorization.

``MCP_INTERNAL_KEY`` authenticates transport only.  This module is the single
actor boundary for OC-02 officer message actions and BP-09 sensitive knowledge
writes: identity is derived at dispatch/attach, stored behind opaque tokens,
and revalidated against current durable state on every authorization.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException

from orchestrator.services.thread_mount_rows import durable_project_ids
from shared.runtime_actor import (
    RUNTIME_ACTOR_BOOTSTRAP_HEADER,
    RUNTIME_ACTOR_HEADER,
    RUNTIME_ACTOR_MAINTENANCE_PHASE_HEADER,
    RUNTIME_ACTOR_MAINTENANCE_PHASE_PRE_TURN,
    RUNTIME_ACTOR_REFRESH_HEADER,
    SENSITIVE_KNOWLEDGE_HUMAN_ROLE_POLICY,
    RuntimeActorContext,
)


ACCESS_TTL_SECONDS = int(os.environ.get("RUNTIME_ACTOR_ACCESS_TTL_SECONDS", "300"))
REFRESH_TTL_SECONDS = int(os.environ.get("RUNTIME_ACTOR_REFRESH_TTL_SECONDS", "86400"))
BOOTSTRAP_TTL_SECONDS = int(
    os.environ.get("RUNTIME_ACTOR_BOOTSTRAP_TTL_SECONDS", "900")
)
# A dedicated session pod exchanges its bootstrap within seconds of booting, so
# 15 minutes is generous. A warm pool pod holds its bootstrap for as long as it
# sits idle waiting for a session, which is a pod lifetime — a short TTL there
# would silently turn identity into a function of how busy the cluster is,
# which is exactly the nondeterminism this credential exists to remove.
POD_BOOTSTRAP_TTL_SECONDS = int(
    os.environ.get("RUNTIME_ACTOR_POD_BOOTSTRAP_TTL_SECONDS", str(7 * 24 * 3600))
)
# Heartbeat-driven slide throttle: a grant is only pushed forward once it is
# inside the *second half* of its window. A 60s heartbeat therefore costs one
# UPDATE per grant per half-TTL (two a day at the default 24h) instead of one
# per beat, while still leaving a full half-TTL of runway to recover from an
# orchestrator outage before anything expires.
LIVENESS_SLIDE_BELOW_SECONDS = int(
    os.environ.get(
        "RUNTIME_ACTOR_LIVENESS_SLIDE_BELOW_SECONDS", str(REFRESH_TTL_SECONDS // 2)
    )
)
OFFICER_RENEW_BEFORE_SECONDS = int(
    os.environ.get("RUNTIME_ACTOR_OFFICER_RENEW_BEFORE_SECONDS", "21600")
)
OFFICER_AGENT_LIVE_SECONDS = int(
    os.environ.get("RUNTIME_ACTOR_OFFICER_AGENT_LIVE_SECONDS", "180")
)
REFRESH_ROTATION_OVERLAP_SECONDS = int(
    os.environ.get("RUNTIME_ACTOR_REFRESH_ROTATION_OVERLAP_SECONDS", "120")
)
INCIDENT_RETRY_BASE_SECONDS = int(
    os.environ.get("RUNTIME_ACTOR_INCIDENT_RETRY_BASE_SECONDS", "60")
)
INCIDENT_RETRY_MAX_SECONDS = int(
    os.environ.get("RUNTIME_ACTOR_INCIDENT_RETRY_MAX_SECONDS", "900")
)
INCIDENT_NOTIFICATION_CLAIM_SECONDS = int(
    os.environ.get("RUNTIME_ACTOR_INCIDENT_NOTIFICATION_CLAIM_SECONDS", "300")
)

_TOKEN_RE = re.compile(r"^sr(?:a|r|b)_[A-Za-z0-9_-]{32,128}$")
_SENSITIVE_ACTIONS = frozenset({"machine_tags", "charter"})
_LIVE_PINNED_THREAD_STATUSES = frozenset(
    {"created", "active", "awaiting_user", "suspended"}
)


class RuntimeActorCredentialError(Exception):
    """Closed-vocabulary credential/identity refusal."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        actor: RuntimeActorContext | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.actor = actor
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class OfficerRuntimeMaintenance:
    """Credential-independent watchdog result with no secret-bearing fields."""

    authorized: bool
    state: str
    project_id: str | None = None
    thread_id: str | None = None
    officer_incarnation: int | None = None
    failure_code: str | None = None
    retry_at: datetime | None = None
    notification_due: bool = False
    notification_claim_id: str | None = None
    incident_changed: bool = False


@dataclass(frozen=True, slots=True)
class RuntimeActorRefreshExchange:
    """Private refresh-route result, including a bearer-delivery fault decision."""

    actor: RuntimeActorContext | None
    response_lost: bool = False
    retryable_failure_code: str | None = None
    verification_plan_id: str | None = None


def _token(prefix: str) -> str:
    return f"{prefix}_{secrets.token_urlsafe(32)}"


def _digest(token: str) -> bytes:
    return hashlib.sha256(token.encode("utf-8")).digest()


def _encrypt_refresh_handoff(token: str) -> str:
    """Encrypt one rotation bearer with the existing orchestrator key."""

    from orchestrator.security.crypto import encrypt

    return encrypt(token)


def _decrypt_refresh_handoff(ciphertext: Any) -> str:
    """Recover a pending bearer without ever persisting or logging plaintext."""

    if not isinstance(ciphertext, str) or not ciphertext:
        raise RuntimeActorCredentialError(
            "invalid_credential", "Runtime actor refresh handoff is unavailable."
        )
    from orchestrator.security.crypto import DecryptionError, decrypt

    try:
        token = decrypt(ciphertext)
    except (DecryptionError, RuntimeError, ValueError, TypeError) as exc:
        raise RuntimeActorCredentialError(
            "invalid_credential", "Runtime actor refresh handoff is unavailable."
        ) from exc
    if not _valid_token(token, "srr"):
        raise RuntimeActorCredentialError(
            "invalid_credential", "Runtime actor refresh handoff is unavailable."
        )
    return token


def _as_utc(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _metadata(thread: dict[str, Any]) -> dict[str, Any]:
    value = thread.get("metadata") or {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        import json

        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _exact_live_pinned_runtime(thread: Any) -> bool:
    """Return whether a thread still owns one usable pinned runtime life.

    A retirement Begin deliberately leaves the reciprocal thread/agent binding
    in place until physical quiescence and settlement finish.  That pointer is
    therefore insufficient credential authority: every Officer boundary must
    also observe the exact generation/attachment and absence of the immutable
    retirement token in the same locked or joined snapshot.
    """

    return bool(
        thread
        and str(thread.get("execution_lane") or "") == "pinned"
        and str(thread.get("status") or "") in _LIVE_PINNED_THREAD_STATUSES
        and thread.get("runtime_generation") is not None
        and thread.get("agent_id") is not None
        and thread.get("runtime_attach_token") is not None
        and thread.get("runtime_retirement_token") is None
    )


async def _thread_project_ids(db: Any, thread: dict[str, Any]) -> list[str]:
    """The same project scope as the attach boundary (``thread_project_ids``):
    the column, or a legacy multi-project Session's mount rows."""
    legacy_mounts = await db.list_thread_mounts(str(thread["id"]))
    return durable_project_ids(thread, legacy_mounts=legacy_mounts)


async def derive_runtime_actor(
    db: Any,
    *,
    thread_id: str,
    project_ids: list[str] | tuple[str, ...] | None = None,
) -> RuntimeActorContext:
    """Derive a persistent runtime's actor solely from durable server state."""

    thread = await db.get_thread(str(thread_id))
    if not thread or str(thread.get("status") or "") == "ended":
        raise RuntimeActorCredentialError(
            "runtime_not_current", "The bound session is not current."
        )
    lane = str(thread.get("execution_lane") or "")
    if lane not in {"pinned", "stateless"} or (
        lane == "pinned" and not _exact_live_pinned_runtime(thread)
    ):
        raise RuntimeActorCredentialError(
            "runtime_not_current", "The bound runtime is not current."
        )
    durable_projects = await _thread_project_ids(db, thread)
    if project_ids is not None:
        requested = list(dict.fromkeys(str(value) for value in project_ids if value))
        if any(value not in durable_projects for value in requested):
            raise RuntimeActorCredentialError(
                "runtime_scope_changed",
                "The delivered project scope is not attached to the thread.",
            )
    else:
        requested = durable_projects
    project_id = requested[0] if requested else None
    user_id = str(thread["user_id"]) if thread.get("user_id") else None

    project_role: str | None = None
    if user_id and project_id:
        user = await db.get_user(user_id)
        if user and bool(user.get("is_admin")):
            project_role = "admin"
        elif user:
            raw_role = await db.get_user_role_in_project(project_id, user_id)
            project_role = str(raw_role) if raw_role else None

    caller_kind = "human"
    officer_incarnation: int | None = None
    if project_id:
        post = await db.get_project_officer(project_id)
        if post and str(post.get("thread_id") or "") == str(thread_id):
            caller_kind = "officer"
            incarnations = post.get("incarnations") or []
            officer_incarnation = (
                len(incarnations) if isinstance(incarnations, list) else 0
            )
        else:
            config_override = _metadata(thread).get("config_override") or {}
            officer_config = (
                config_override.get("officer")
                if isinstance(config_override, dict)
                else None
            )
            if (
                isinstance(officer_config, dict)
                and officer_config.get("conference") is True
            ):
                caller_kind = "conference"

    return RuntimeActorContext(
        caller_kind=caller_kind,
        project_id=project_id,
        project_role=project_role,
        thread_id=str(thread_id),
        officer_incarnation=officer_incarnation,
        user_id=user_id,
    )


def worker_runtime_actor(
    *, project_id: str | None, user_id: str | None = None
) -> RuntimeActorContext:
    """Build the server-side worker identity placed in a job start bundle."""

    return RuntimeActorContext(
        caller_kind="worker",
        project_id=str(project_id) if project_id else None,
        project_role=None,
        thread_id=None,
        officer_incarnation=None,
        user_id=str(user_id) if user_id else None,
    )


async def _insert_access_token(
    conn: Any, grant_id: Any, *, now: datetime
) -> tuple[str, datetime]:
    access_token = _token("sra")
    access_expires_at = now + timedelta(seconds=ACCESS_TTL_SECONDS)
    await conn.execute(
        """
        INSERT INTO runtime_actor_access_tokens
            (token_hash, grant_id, expires_at)
        VALUES ($1, $2, $3)
        """,
        _digest(access_token),
        grant_id,
        access_expires_at,
    )
    return access_token, access_expires_at


async def _lock_officer_mint_authority(
    conn: Any,
    actor: RuntimeActorContext,
    *,
    agent_id: str,
    now: datetime,
) -> Any:
    """Revalidate Post -> thread -> agent before minting Officer authority."""

    post = await conn.fetchrow(
        "SELECT project_id, thread_id, incarnations, state FROM project_officers "
        "WHERE project_id = $1::uuid FOR UPDATE",
        actor.project_id,
    )
    if post is None or str(post.get("thread_id") or "") != str(actor.thread_id):
        raise RuntimeActorCredentialError(
            "runtime_not_current", "The Officer post incarnation changed.", actor=actor
        )
    thread = await conn.fetchrow(
        "SELECT id, project_id, user_id, status, metadata, execution_lane, "
        "runtime_generation, runtime_attach_token, runtime_retirement_token, "
        "agent_id FROM threads WHERE id = $1::uuid FOR UPDATE",
        actor.thread_id,
    )
    bound = await conn.fetchrow(
        "SELECT id, thread_id, status, last_heartbeat FROM agents "
        "WHERE id = $1::uuid FOR UPDATE",
        agent_id,
    )
    incarnations = post.get("incarnations") or []
    if isinstance(incarnations, str):
        try:
            incarnations = json.loads(incarnations)
        except (TypeError, ValueError):
            incarnations = []
    incarnation = len(incarnations) if isinstance(incarnations, list) else 0
    metadata = _json_object(thread.get("metadata")) if thread else {}
    officer = _json_object(_json_object(metadata.get("config_override")).get("officer"))
    heartbeat = _as_utc(bound.get("last_heartbeat")) if bound else None
    if (
        thread is None
        or str(thread.get("project_id") or "") != str(post["project_id"])
        or str(thread.get("user_id") or "") != str(actor.user_id)
        or str(thread.get("agent_id") or "") != str(agent_id)
        or not _exact_live_pinned_runtime(thread)
        or officer.get("enabled") not in {True, "true", "True", 1}
        or bound is None
        or str(bound.get("thread_id") or "") != str(thread["id"])
        or str(bound.get("status") or "") in {"offline", "failed", "draining"}
        or heartbeat is None
        or heartbeat <= now - timedelta(seconds=max(1, OFFICER_AGENT_LIVE_SECONDS))
        or int(actor.officer_incarnation or 0) != incarnation
    ):
        raise RuntimeActorCredentialError(
            "runtime_not_current",
            "The Officer grant does not match the current live runtime.",
            actor=actor,
        )
    role_row = await conn.fetchrow(
        "SELECT u.is_admin, pm.role FROM users u LEFT JOIN project_members pm "
        "ON pm.user_id = u.id AND pm.project_id = $2::uuid "
        "WHERE u.id = $1::uuid",
        actor.user_id,
        actor.project_id,
    )
    role = (
        "admin"
        if role_row and bool(role_row.get("is_admin"))
        else str(role_row.get("role"))
        if role_row and role_row.get("role")
        else ""
    )
    if role != str(actor.project_role or ""):
        raise RuntimeActorCredentialError(
            "runtime_not_current", "The Officer project authority changed.", actor=actor
        )
    return post


async def mint_runtime_actor(
    db: Any,
    actor: RuntimeActorContext,
    *,
    agent_id: str | None = None,
) -> RuntimeActorContext:
    """Mint opaque access/refresh credentials for a derived actor."""

    refresh_token = _token("srr")
    now = datetime.now(timezone.utc)
    refresh_expires_at = now + timedelta(seconds=REFRESH_TTL_SECONDS)
    if actor.caller_kind == "officer" and not agent_id:
        raise RuntimeActorCredentialError(
            "runtime_not_current",
            "Officer runtime grants require the authoritative agent binding.",
            actor=actor,
        )
    async with db.acquire() as conn:
        async with conn.transaction():
            officer_post = None
            if actor.caller_kind == "officer" and agent_id:
                officer_post = await _lock_officer_mint_authority(
                    conn, actor, agent_id=agent_id, now=now
                )
                # One durable authority per Officer incarnation, including
                # across pod replacement. Revoking every predecessor here is
                # also the rolling-upgrade fence: an older replica still sees
                # revoked_at and cannot refresh the losing pod's grant.
                await conn.execute(
                    """
                    UPDATE runtime_actor_grants
                       SET revoked_at = COALESCE(revoked_at, $3)
                     WHERE caller_kind = 'officer'
                       AND thread_id = $1::uuid
                       AND officer_incarnation = $2
                       AND revoked_at IS NULL
                    """,
                    actor.thread_id,
                    actor.officer_incarnation,
                    now,
                )
            row = await conn.fetchrow(
                """
                INSERT INTO runtime_actor_grants (
                    refresh_token_hash, caller_kind, user_id, project_id,
                    project_role, thread_id, officer_incarnation, agent_id,
                    refresh_expires_at
                )
                VALUES ($1, $2, $3::uuid, $4::uuid, $5, $6::uuid, $7,
                        $8::uuid, $9)
                RETURNING id
                """,
                _digest(refresh_token),
                actor.caller_kind,
                actor.user_id,
                actor.project_id,
                actor.project_role,
                actor.thread_id,
                actor.officer_incarnation,
                agent_id,
                refresh_expires_at,
            )
            access_token, access_expires_at = await _insert_access_token(
                conn, row["id"], now=now
            )
            if officer_post is not None:
                await _resolve_runtime_incident_on_conn(
                    conn,
                    officer_post,
                    thread_id=str(actor.thread_id),
                    incarnation=int(actor.officer_incarnation or 0),
                    now=now,
                )
    actor.access_credential = access_token
    actor.refresh_credential = refresh_token
    actor.access_expires_at = access_expires_at
    actor.refresh_expires_at = refresh_expires_at
    return actor


async def mint_thread_runtime_actor(
    db: Any,
    *,
    thread_id: str,
    project_ids: list[str] | tuple[str, ...] | None = None,
    agent_id: str | None = None,
) -> RuntimeActorContext:
    actor = await derive_runtime_actor(db, thread_id=thread_id, project_ids=project_ids)
    return await mint_runtime_actor(db, actor, agent_id=agent_id)


async def mint_worker_runtime_actor(
    db: Any, *, project_id: str | None, user_id: str | None = None
) -> RuntimeActorContext:
    return await mint_runtime_actor(
        db, worker_runtime_actor(project_id=project_id, user_id=user_id)
    )


async def issue_runtime_actor_bootstrap(db: Any, thread_id: str) -> str:
    """Create one unique, short-lived workload bootstrap for a session pod."""

    token = _token("srb")
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=BOOTSTRAP_TTL_SECONDS)
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO runtime_actor_bootstraps (token_hash, thread_id, expires_at)
            VALUES ($1, $2::uuid, $3)
            """,
            _digest(token),
            str(thread_id),
            expires_at,
        )
    return token


async def issue_runtime_actor_pod_bootstrap(db: Any) -> str:
    """Create one pod-scoped bootstrap for a warm pool agent.

    Thread-less by construction: the pod is provisioned before any session
    exists, so there is nothing to bind to yet. The binding happens in
    ``exchange_runtime_actor_pod_bootstrap``, which reads it from durable
    server state instead of accepting it from the caller.
    """

    token = _token("srb")
    expires_at = datetime.now(timezone.utc) + timedelta(
        seconds=POD_BOOTSTRAP_TTL_SECONDS
    )
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO runtime_actor_bootstraps (token_hash, thread_id, expires_at)
            VALUES ($1, NULL, $2)
            """,
            _digest(token),
            expires_at,
        )
    return token


async def exchange_runtime_actor_pod_bootstrap(
    db: Any, *, agent_id: str, thread_id: str, bootstrap_token: str
) -> RuntimeActorContext:
    """Bind a warm pool pod's bootstrap to the session it was just given.

    Two independent facts must hold, and neither is taken from the caller's
    word: the presenter holds a secret that only exists inside one provisioned
    pod, and the orchestrator's own ``agents`` row already says that pod is
    the agent serving this thread. A pod that lies about ``thread_id`` gets
    nothing, because the binding is read from the row, not the request.

    Unlike the dedicated-pod exchange this is deliberately repeatable: one pool
    pod serves a succession of sessions over its life, and each attach must
    mint a fresh, correctly-scoped actor.
    """

    if not _valid_token(bootstrap_token, "srb"):
        raise RuntimeActorCredentialError(
            "malformed_bootstrap", "Runtime actor bootstrap is malformed."
        )
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE runtime_actor_bootstraps
               SET last_used_at = now()
             WHERE token_hash = $1
               AND thread_id IS NULL
               AND expires_at > now()
            RETURNING token_hash
            """,
            _digest(bootstrap_token),
        )
        if row is None:
            raise RuntimeActorCredentialError(
                "invalid_bootstrap",
                "Runtime actor pod bootstrap is invalid or expired.",
            )
        bound = await conn.fetchrow(
            """
            SELECT 1
              FROM agents
             WHERE id = $1::uuid
               AND thread_id = $2::uuid
            """,
            str(agent_id),
            str(thread_id),
        )
    if bound is None:
        raise RuntimeActorCredentialError(
            "runtime_not_current",
            "This agent is not the bound agent for the requested session.",
        )
    return await mint_thread_runtime_actor(
        db, thread_id=str(thread_id), agent_id=str(agent_id)
    )


async def exchange_runtime_actor_bootstrap(
    db: Any,
    *,
    thread_id: str,
    bootstrap_token: str,
    agent_id: str | None = None,
) -> RuntimeActorContext:
    """Exchange a pod-unique bootstrap after the registration bind succeeds."""

    await validate_thread_runtime_actor_bootstrap(
        db, thread_id=thread_id, bootstrap_token=bootstrap_token
    )
    return await mint_thread_runtime_actor(
        db,
        thread_id=str(thread_id),
        agent_id=str(agent_id) if agent_id else None,
    )


async def validate_thread_runtime_actor_bootstrap(
    db: Any, *, thread_id: str, bootstrap_token: str
) -> None:
    """Validate a dedicated pod secret before mutating its agent binding."""

    if not _valid_token(bootstrap_token, "srb"):
        raise RuntimeActorCredentialError(
            "malformed_bootstrap", "Runtime actor bootstrap is malformed."
        )
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE runtime_actor_bootstraps
               SET last_used_at = now()
             WHERE token_hash = $1
               AND thread_id = $2::uuid
               AND expires_at > now()
            RETURNING thread_id
            """,
            _digest(bootstrap_token),
            str(thread_id),
        )
    if row is None:
        raise RuntimeActorCredentialError(
            "invalid_bootstrap", "Runtime actor bootstrap is invalid or expired."
        )


def _valid_token(token: Any, prefix: str) -> bool:
    return (
        isinstance(token, str)
        and token.startswith(f"{prefix}_")
        and _TOKEN_RE.fullmatch(token) is not None
    )


def _header_values(request: Any, name: str) -> list[str]:
    headers = getattr(request, "headers", None)
    if headers is None:
        return []
    getter = getattr(headers, "getlist", None)
    if callable(getter):
        raw_values = getter(name)
        return [part.strip() for value in raw_values for part in str(value).split(",")]
    try:
        value = headers.get(name)
    except Exception:
        return []
    if not isinstance(value, str):
        return []
    # A combined duplicate header is just as ambiguous as two physical fields.
    return [part.strip() for part in value.split(",")]


def request_bootstrap_token(request: Any) -> str | None:
    values = _header_values(request, RUNTIME_ACTOR_BOOTSTRAP_HEADER)
    if not values:
        return None
    if len(values) != 1:
        raise RuntimeActorCredentialError(
            "duplicate_bootstrap",
            "Duplicate runtime actor bootstraps are refused.",
        )
    value = values[0]
    if not _valid_token(value, "srb"):
        raise RuntimeActorCredentialError(
            "malformed_bootstrap", "Runtime actor bootstrap is malformed."
        )
    return value


def _required_request_token(request: Any, name: str, prefix: str) -> str:
    values = _header_values(request, name)
    if not values:
        raise RuntimeActorCredentialError(
            "missing_credential", "Runtime actor credential is required."
        )
    if len(values) != 1:
        raise RuntimeActorCredentialError(
            "duplicate_credential", "Duplicate runtime actor credentials are refused."
        )
    value = values[0]
    if not _valid_token(value, prefix):
        raise RuntimeActorCredentialError(
            "malformed_credential", "Runtime actor credential is malformed."
        )
    return value


def _actor_from_row(row: Any) -> RuntimeActorContext:
    return RuntimeActorContext(
        caller_kind=str(row["caller_kind"]),
        project_id=str(row["project_id"]) if row.get("project_id") else None,
        project_role=str(row["project_role"]) if row.get("project_role") else None,
        thread_id=str(row["thread_id"]) if row.get("thread_id") else None,
        officer_incarnation=row.get("officer_incarnation"),
        user_id=str(row["user_id"]) if row.get("user_id") else None,
        access_expires_at=_as_utc(row.get("access_expires_at")),
        refresh_expires_at=_as_utc(row.get("refresh_expires_at")),
    )


async def _actor_for_access(db: Any, token: str) -> RuntimeActorContext:
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT g.caller_kind, g.user_id, g.project_id, g.project_role,
                   g.thread_id, g.officer_incarnation, g.refresh_expires_at,
                   g.revoked_at, g.agent_id,
                   a.expires_at AS access_expires_at,
                   t.agent_id AS current_thread_agent_id,
                   t.execution_lane AS current_thread_execution_lane,
                   t.status AS current_thread_status,
                   t.runtime_generation AS current_thread_runtime_generation,
                   t.runtime_attach_token AS current_thread_runtime_attach_token,
                   t.runtime_retirement_token AS current_thread_retirement_token,
                   bound.thread_id AS current_agent_thread_id,
                   bound.status AS current_agent_status,
                   bound.last_heartbeat AS current_agent_heartbeat
              FROM runtime_actor_access_tokens a
              JOIN runtime_actor_grants g ON g.id = a.grant_id
              LEFT JOIN threads t ON t.id = g.thread_id
              LEFT JOIN agents bound ON bound.id = g.agent_id
             WHERE a.token_hash = $1
            """,
            _digest(token),
        )
        if row is not None:
            await conn.execute(
                "UPDATE runtime_actor_access_tokens SET last_used_at = now() "
                "WHERE token_hash = $1",
                _digest(token),
            )
    if row is None:
        raise RuntimeActorCredentialError(
            "invalid_credential", "Runtime actor credential is not recognized."
        )
    actor = _actor_from_row(row)
    now = datetime.now(timezone.utc)
    if row.get("revoked_at") is not None:
        raise RuntimeActorCredentialError(
            "revoked_credential", "Runtime actor credential was revoked.", actor=actor
        )
    if actor.access_expires_at is None or actor.access_expires_at <= now:
        raise RuntimeActorCredentialError(
            "expired_credential", "Runtime actor credential has expired.", actor=actor
        )
    if actor.refresh_expires_at is None or actor.refresh_expires_at <= now:
        raise RuntimeActorCredentialError(
            "expired_credential", "Runtime actor grant has expired.", actor=actor
        )
    if actor.caller_kind == "officer":
        heartbeat = _as_utc(row.get("current_agent_heartbeat"))
        exact_thread = {
            "execution_lane": row.get("current_thread_execution_lane"),
            "status": row.get("current_thread_status"),
            "runtime_generation": row.get("current_thread_runtime_generation"),
            "agent_id": row.get("current_thread_agent_id"),
            "runtime_attach_token": row.get("current_thread_runtime_attach_token"),
            "runtime_retirement_token": row.get("current_thread_retirement_token"),
        }
        binding_current = (
            row.get("agent_id") is not None
            and _exact_live_pinned_runtime(exact_thread)
            and row.get("current_thread_agent_id") == row.get("agent_id")
            and row.get("current_agent_thread_id") == row.get("thread_id")
            and str(row.get("current_agent_status") or "")
            not in {"offline", "failed", "draining"}
            and heartbeat is not None
            and heartbeat > now - timedelta(seconds=max(1, OFFICER_AGENT_LIVE_SECONDS))
        )
        if not binding_current:
            raise RuntimeActorCredentialError(
                "runtime_not_current",
                "The Officer credential is not bound to the current live runtime.",
                actor=actor,
            )
    return actor


async def _current_actor(db: Any, actor: RuntimeActorContext) -> RuntimeActorContext:
    if actor.caller_kind == "worker":
        return worker_runtime_actor(
            project_id=actor.project_id,
            user_id=actor.user_id,
        )
    if not actor.thread_id:
        raise RuntimeActorCredentialError(
            "runtime_not_current", "Runtime actor has no bound session.", actor=actor
        )
    try:
        current = await derive_runtime_actor(
            db,
            thread_id=actor.thread_id,
            project_ids=[actor.project_id] if actor.project_id else [],
        )
    except RuntimeActorCredentialError as exc:
        exc.actor = actor
        raise
    if current.audit_payload() != actor.audit_payload():
        raise RuntimeActorCredentialError(
            "runtime_not_current",
            "The runtime actor no longer matches current post or membership state.",
            actor=actor,
        )
    return current


async def _audit_denial(
    db: Any,
    request: Any,
    error: RuntimeActorCredentialError,
    *,
    action: str,
    project_id: str | None,
) -> None:
    from orchestrator.security.access import log_security_event

    actor = error.actor
    user = {
        "id": actor.user_id if actor else None,
        "auth_method": "runtime_actor",
        "is_admin": bool(actor and actor.project_role == "admin"),
    }
    actor_bits = actor.audit_payload() if actor else {"caller_kind": "unresolved"}
    await log_security_event(
        db,
        request=request,
        event_type="runtime_actor_denied",
        resource_type="runtime_actor",
        resource_id=project_id or (actor.project_id if actor else None),
        user=user,
        detail=f"{error.code}: action={action}; actor={actor_bits}",
    )


def _http_denial(error: RuntimeActorCredentialError, *, action: str) -> HTTPException:
    actor = (
        error.actor.audit_payload() if error.actor else {"caller_kind": "unresolved"}
    )
    return HTTPException(
        status_code=403,
        detail={
            "authorized": False,
            "code": error.code,
            "action": action,
            "actor": actor,
            "message": error.message,
        },
    )


async def authorize_runtime_actor_request(
    db: Any,
    request: Any,
    *,
    action: str,
    project_id: str | None,
) -> RuntimeActorContext:
    """Authorize one route or knowledge mutation from the hidden credential."""

    try:
        token = _required_request_token(request, RUNTIME_ACTOR_HEADER, "sra")
        actor = await _actor_for_access(db, token)
        if project_id is None or actor.project_id != str(project_id):
            raise RuntimeActorCredentialError(
                "project_scope_mismatch",
                "Runtime actor project does not match the target project.",
                actor=actor,
            )
        await _current_actor(db, actor)
        if action in {"officer_message", "redispatch_livelock_ack"}:
            if actor.caller_kind != "officer":
                raise RuntimeActorCredentialError(
                    "officer_required",
                    "Only the commissioned background officer may perform this action.",
                    actor=actor,
                )
        elif action in _SENSITIVE_ACTIONS:
            if actor.caller_kind == "officer":
                pass
            elif actor.caller_kind in {"human", "conference"} and bool(
                SENSITIVE_KNOWLEDGE_HUMAN_ROLE_POLICY.get(
                    str(actor.project_role or ""), False
                )
            ):
                pass
            else:
                raise RuntimeActorCredentialError(
                    "project_role_denied",
                    "The current caller kind/project role may not perform this write.",
                    actor=actor,
                )
        else:
            raise RuntimeActorCredentialError(
                "unknown_action", "Runtime actor action is not recognized.", actor=actor
            )
        return actor
    except RuntimeActorCredentialError as error:
        await _audit_denial(db, request, error, action=action, project_id=project_id)
        raise _http_denial(error, action=action) from error


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _officer_incident_key(thread_id: Any, incarnation: Any) -> str:
    return f"runtime_actor:{thread_id}:{int(incarnation)}"


def _incident_retry_at(now: datetime, attempt: int) -> datetime:
    delay = min(
        max(1, INCIDENT_RETRY_MAX_SECONDS),
        max(1, INCIDENT_RETRY_BASE_SECONDS) * (2 ** max(0, min(attempt - 1, 8))),
    )
    return now + timedelta(seconds=delay)


def _incident_notification_due(incident: dict[str, Any], now: datetime) -> bool:
    notification = _json_object(incident.get("notification"))
    state = str(notification.get("state") or "pending")
    retry_at = _parse_incident_time(notification.get("next_retry_at"))
    claim_expires = _parse_incident_time(notification.get("claim_expires_at"))
    return (
        state == "pending"
        or (state == "failed" and (retry_at is None or retry_at <= now))
        or (state == "sending" and (claim_expires is None or claim_expires <= now))
    )


async def _claim_runtime_incident_notification_on_conn(
    conn: Any,
    post: Any,
    incident: dict[str, Any],
    *,
    now: datetime,
) -> str | None:
    """Lease one project-scoped page attempt under the locked Post row."""

    if not _incident_notification_due(incident, now):
        return None
    claim_id = secrets.token_urlsafe(24)
    notification = _json_object(incident.get("notification"))
    notification.update(
        {
            "state": "sending",
            "claim_id": claim_id,
            "claimed_at": now.isoformat(),
            "claim_expires_at": (
                now + timedelta(seconds=max(1, INCIDENT_NOTIFICATION_CLAIM_SECONDS))
            ).isoformat(),
        }
    )
    incident["notification"] = notification
    await conn.execute(
        """
        UPDATE project_officers
           SET state = jsonb_set(COALESCE(state, '{}'::jsonb),
                                 '{runtime_actor_incident}', $2::jsonb, true),
               updated_at = now()
         WHERE project_id = $1
        """,
        post["project_id"],
        json.dumps(incident),
    )
    return claim_id


async def _write_runtime_incident_on_conn(
    conn: Any,
    post: Any,
    *,
    thread_id: str,
    incarnation: int,
    failure_code: str,
    now: datetime,
) -> tuple[dict[str, Any], bool, bool]:
    """Open/update one deduplicated Post incident while its row is locked."""

    state = _json_object(post.get("state"))
    existing = _json_object(state.get("runtime_actor_incident"))
    key = _officer_incident_key(thread_id, incarnation)
    same_open = existing.get("key") == key and existing.get("status") == "open"
    attempt = int(existing.get("attempt_count") or 0) + 1 if same_open else 1
    notification = _json_object(existing.get("notification")) if same_open else {}
    notification_state = str(notification.get("state") or "pending")
    incident = {
        "key": key,
        "status": "open",
        "failure_class": str(failure_code)[:128],
        "summary": (
            "The commissioned Officer runtime authorization could not be "
            "maintained. Autonomous planning is suppressed until the exact "
            "current runtime recovers."
        ),
        "thread_id": str(thread_id),
        "officer_incarnation": int(incarnation),
        "first_failed_at": (
            existing.get("first_failed_at") if same_open else now.isoformat()
        ),
        "last_failed_at": now.isoformat(),
        "attempt_count": attempt,
        "next_retry_at": _incident_retry_at(now, attempt).isoformat(),
        "recovery_probe_at": existing.get("recovery_probe_at") if same_open else None,
        "notification": {
            "state": notification_state,
            "attempt_count": int(notification.get("attempt_count") or 0),
            "last_attempted_at": notification.get("last_attempted_at"),
            "delivered_at": notification.get("delivered_at"),
            "failure_class": notification.get("failure_class"),
            "next_retry_at": notification.get("next_retry_at"),
            "claim_id": notification.get("claim_id"),
            "claimed_at": notification.get("claimed_at"),
            "claim_expires_at": notification.get("claim_expires_at"),
        },
    }
    await conn.execute(
        """
        UPDATE project_officers
           SET state = jsonb_set(COALESCE(state, '{}'::jsonb),
                                 '{runtime_actor_incident}', $2::jsonb, true),
               updated_at = now()
         WHERE project_id = $1
        """,
        post["project_id"],
        json.dumps(incident),
    )
    return incident, not same_open, False


async def _resolve_runtime_incident_on_conn(
    conn: Any,
    post: Any,
    *,
    thread_id: str,
    incarnation: int,
    now: datetime,
) -> bool:
    state = _json_object(post.get("state"))
    existing = _json_object(state.get("runtime_actor_incident"))
    if existing.get("status") != "open":
        return False
    current_key = _officer_incident_key(thread_id, incarnation)
    same_incarnation = existing.get("key") == current_key
    resolved = {
        **existing,
        "status": "resolved" if same_incarnation else "superseded",
        "resolved_at": now.isoformat(),
        "next_retry_at": None,
        "resolution": "recovered" if same_incarnation else "incarnation_changed",
    }
    await conn.execute(
        """
        UPDATE project_officers
           SET state = jsonb_set(COALESCE(state, '{}'::jsonb),
                                 '{runtime_actor_incident}', $2::jsonb, true),
               updated_at = now()
         WHERE project_id = $1
        """,
        post["project_id"],
        json.dumps(resolved),
    )
    # An authorization incident may have deferred already-durable wake rows.
    # Re-arm them without creating a duplicate intent.
    await conn.execute(
        """
        UPDATE session_wake_events
           SET fire_at = LEAST(COALESCE(fire_at, $2), $2)
         WHERE thread_id = $1
           AND state = 'pending'
        """,
        str(thread_id),
        now,
    )
    return True


async def _lock_officer_authority_for_grant(
    conn: Any,
    grant_hint: Any,
    *,
    now: datetime,
) -> tuple[Any, Any, Any, Any, str]:
    """Lock Post -> thread -> agent -> grant and rederive exact authority."""

    project_id = grant_hint.get("project_id")
    thread_id = grant_hint.get("thread_id")
    if not project_id or not thread_id:
        raise RuntimeActorCredentialError(
            "runtime_not_current", "The Officer grant has no durable binding."
        )
    post = await conn.fetchrow(
        "SELECT project_id, thread_id, incarnations, state "
        "FROM project_officers WHERE project_id = $1 FOR UPDATE",
        project_id,
    )
    if post is None or post.get("thread_id") != thread_id:
        raise RuntimeActorCredentialError(
            "runtime_not_current", "The Officer post incarnation changed."
        )
    thread = await conn.fetchrow(
        """
        SELECT id, project_id, user_id, status, metadata, execution_lane,
               runtime_generation, runtime_attach_token,
               runtime_retirement_token, agent_id
          FROM threads
         WHERE id = $1
         FOR UPDATE
        """,
        thread_id,
    )
    if thread is None:
        raise RuntimeActorCredentialError(
            "runtime_not_current", "The Officer thread is no longer current."
        )
    agent_id = thread.get("agent_id")
    agent = (
        await conn.fetchrow(
            """
            SELECT id, thread_id, status, last_heartbeat
              FROM agents
             WHERE id = $1
             FOR UPDATE
            """,
            agent_id,
        )
        if agent_id
        else None
    )
    grant = await conn.fetchrow(
        """
        SELECT id, refresh_token_hash, previous_refresh_token_hash,
               previous_refresh_valid_until, refresh_handoff_ciphertext,
               refresh_handoff_acknowledged_at,
               caller_kind, user_id, project_id,
               project_role, thread_id, officer_incarnation, agent_id,
               credential_generation, refresh_rotation_required,
               refresh_expires_at, revoked_at, created_at
          FROM runtime_actor_grants
         WHERE id = $1
         FOR UPDATE
        """,
        grant_hint["id"],
    )
    if grant is None or grant.get("revoked_at") is not None:
        raise RuntimeActorCredentialError(
            "revoked_credential", "The Officer grant is no longer current."
        )

    metadata = _json_object(thread.get("metadata"))
    officer = _json_object(_json_object(metadata.get("config_override")).get("officer"))
    incarnations = post.get("incarnations") or []
    if isinstance(incarnations, str):
        try:
            incarnations = json.loads(incarnations)
        except (TypeError, ValueError):
            incarnations = []
    current_incarnation = len(incarnations) if isinstance(incarnations, list) else 0
    heartbeat = _as_utc(agent.get("last_heartbeat")) if agent else None
    live_agent = (
        agent is not None
        and agent.get("thread_id") == thread.get("id")
        and str(agent.get("status") or "") not in {"offline", "failed", "draining"}
        and heartbeat is not None
        and heartbeat > now - timedelta(seconds=max(1, OFFICER_AGENT_LIVE_SECONDS))
    )
    shape_current = (
        grant.get("caller_kind") == "officer"
        and grant.get("project_id") == post.get("project_id")
        and grant.get("thread_id") == thread.get("id")
        and grant.get("user_id") == thread.get("user_id")
        and int(grant.get("officer_incarnation") or 0) == current_incarnation
        and thread.get("project_id") == post.get("project_id")
        and _exact_live_pinned_runtime(thread)
        and officer.get("enabled") in {True, "true", "True", 1}
        and live_agent
        and (grant.get("agent_id") is None or grant.get("agent_id") == agent.get("id"))
    )
    if not shape_current:
        raise RuntimeActorCredentialError(
            "runtime_not_current",
            "The Officer grant does not match the current live incarnation.",
            actor=_actor_from_row(grant),
        )

    if grant.get("agent_id") is None:
        # A pre-0171 grant has no agent provenance. One otherwise-current
        # candidate can be adopted under the exact Post/thread/live-agent
        # locks. More than one is ambiguous: timestamp order cannot prove
        # which bearer the live pod holds, so touch none of them.
        legacy_candidates = await conn.fetch(
            """
            SELECT id
              FROM runtime_actor_grants
             WHERE caller_kind = 'officer'
               AND project_id = $1
               AND thread_id = $2
               AND officer_incarnation = $3
               AND agent_id IS NULL
               AND revoked_at IS NULL
             ORDER BY id
             LIMIT 2
             FOR UPDATE
            """,
            post["project_id"],
            thread["id"],
            current_incarnation,
        )
        if len(legacy_candidates) != 1 or legacy_candidates[0]["id"] != grant["id"]:
            raise RuntimeActorCredentialError(
                "ambiguous_legacy_grants",
                "The Officer runtime grant cannot be adopted unambiguously.",
                actor=_actor_from_row(grant),
            )

    role_row = await conn.fetchrow(
        """
        SELECT u.is_admin, pm.role
          FROM users u
          LEFT JOIN project_members pm
            ON pm.user_id = u.id AND pm.project_id = $2
         WHERE u.id = $1
        """,
        thread["user_id"],
        post["project_id"],
    )
    current_role = (
        "admin"
        if role_row and bool(role_row.get("is_admin"))
        else str(role_row.get("role"))
        if role_row and role_row.get("role")
        else ""
    )
    if current_role != str(grant.get("project_role") or ""):
        raise RuntimeActorCredentialError(
            "runtime_not_current",
            "The Officer project authority changed.",
            actor=_actor_from_row(grant),
        )
    return post, thread, agent, grant, current_role


async def lock_current_officer_runtime_grant(
    conn: Any,
    *,
    post: Any,
    thread: Any,
    agent: Any,
    now: datetime | None = None,
) -> Any | None:
    """Lock and return the one exact live grant for a locked Officer runtime.

    Callers must already hold the Post -> thread -> agent locks.  This helper
    centralizes the same incarnation, owner-role, liveness, and grant-shape
    authority used by refresh/maintenance so lifecycle replacement cannot
    accept a merely unrevoked thread/agent-shaped row.
    """

    observed_at = now or datetime.now(timezone.utc)
    incarnations = post.get("incarnations") or []
    if isinstance(incarnations, str):
        try:
            incarnations = json.loads(incarnations)
        except (TypeError, ValueError):
            return None
    incarnation = len(incarnations) if isinstance(incarnations, list) else 0
    metadata = _json_object(thread.get("metadata"))
    officer = _json_object(_json_object(metadata.get("config_override")).get("officer"))
    heartbeat = _as_utc(agent.get("last_heartbeat")) if agent else None
    if (
        post.get("thread_id") != thread.get("id")
        or post.get("project_id") != thread.get("project_id")
        or not _exact_live_pinned_runtime(thread)
        or officer.get("enabled") not in {True, "true", "True", 1}
        or agent is None
        or agent.get("id") != thread.get("agent_id")
        or agent.get("thread_id") != thread.get("id")
        or str(agent.get("status") or "") in {"offline", "failed", "draining"}
        or heartbeat is None
        or heartbeat
        <= observed_at - timedelta(seconds=max(1, OFFICER_AGENT_LIVE_SECONDS))
    ):
        return None

    role_row = await conn.fetchrow(
        "SELECT u.is_admin, pm.role FROM users u LEFT JOIN project_members pm "
        "ON pm.user_id = u.id AND pm.project_id = $2 "
        "WHERE u.id = $1",
        thread.get("user_id"),
        post.get("project_id"),
    )
    current_role = (
        "admin"
        if role_row and bool(role_row.get("is_admin"))
        else str(role_row.get("role"))
        if role_row and role_row.get("role")
        else ""
    )
    grants = await conn.fetch(
        """
        SELECT id, caller_kind, user_id, project_id, project_role,
               thread_id, officer_incarnation, agent_id,
               refresh_expires_at, revoked_at
          FROM runtime_actor_grants
         WHERE caller_kind = 'officer'
           AND project_id = $1
           AND thread_id = $2
           AND officer_incarnation = $3
           AND agent_id = $4
           AND revoked_at IS NULL
           AND refresh_expires_at > $5
         ORDER BY id
         LIMIT 2
         FOR UPDATE
        """,
        post.get("project_id"),
        thread.get("id"),
        incarnation,
        agent.get("id"),
        observed_at,
    )
    if len(grants) != 1:
        return None
    grant = grants[0]
    if (
        grant.get("caller_kind") != "officer"
        or grant.get("user_id") != thread.get("user_id")
        or grant.get("project_id") != post.get("project_id")
        or grant.get("thread_id") != thread.get("id")
        or int(grant.get("officer_incarnation") or 0) != incarnation
        or grant.get("agent_id") != agent.get("id")
        or str(grant.get("project_role") or "") != current_role
    ):
        return None
    return grant


def _refresh_digest_matches(grant: Any, presented_digest: bytes, now: datetime) -> bool:
    if grant.get("refresh_token_hash") == presented_digest:
        return True
    previous_until = _as_utc(grant.get("previous_refresh_valid_until"))
    acknowledged_at = _as_utc(grant.get("refresh_handoff_acknowledged_at"))
    return (
        grant.get("previous_refresh_token_hash") == presented_digest
        and grant.get("refresh_handoff_ciphertext") is not None
        and (
            acknowledged_at is None
            or (previous_until is not None and previous_until > now)
        )
    )


async def refresh_runtime_actor_exchange(
    db: Any,
    request: Any,
    *,
    verification_enabled: bool = False,
    now: datetime | None = None,
) -> RuntimeActorRefreshExchange:
    """Maintain one actor and mint a fresh short-lived access credential.

    Workers retain 0161's fixed refresh lifetime. A current Officer may renew
    before expiry or recover after expiry, but the exceptional recovery is
    accepted only after Post -> thread -> live-agent -> grant revalidation and
    rotates the refresh bearer. Rotation is an acknowledged handoff: until
    the new bearer is presented, predecessor retries re-deliver the same
    encrypted generation even across response loss or orchestrator restart.
    Acknowledgement starts the bounded predecessor overlap.
    """

    action = "refresh"
    try:
        token = _required_request_token(request, RUNTIME_ACTOR_REFRESH_HEADER, "srr")
        presented_digest = _digest(token)
        verification_pre_turn = (
            str(request.headers.get(RUNTIME_ACTOR_MAINTENANCE_PHASE_HEADER, ""))
            == RUNTIME_ACTOR_MAINTENANCE_PHASE_PRE_TURN
        )
        async with db.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, refresh_token_hash, previous_refresh_token_hash,
                       previous_refresh_valid_until, refresh_handoff_ciphertext,
                       refresh_handoff_acknowledged_at, caller_kind, user_id,
                       project_id, project_role, thread_id,
                       officer_incarnation, agent_id, credential_generation,
                       refresh_rotation_required, refresh_expires_at,
                       revoked_at, created_at
                  FROM runtime_actor_grants
                 WHERE refresh_token_hash = $1
                    OR (previous_refresh_token_hash = $1
                        AND refresh_handoff_ciphertext IS NOT NULL
                        AND (refresh_handoff_acknowledged_at IS NULL
                             OR previous_refresh_valid_until > now()))
                 ORDER BY (refresh_token_hash = $1) DESC
                 LIMIT 1
                """,
                presented_digest,
            )
        if row is None:
            raise RuntimeActorCredentialError(
                "invalid_credential", "Runtime actor refresh is not recognized."
            )
        actor = _actor_from_row(row)
        observed_at = now or datetime.now(timezone.utc)
        if row.get("revoked_at") is not None:
            raise RuntimeActorCredentialError(
                "revoked_credential", "Runtime actor grant was revoked.", actor=actor
            )
        if actor.caller_kind == "officer":
            async with db.acquire() as conn:
                async with conn.transaction():
                    (
                        post,
                        _thread,
                        agent,
                        grant,
                        _role,
                    ) = await _lock_officer_authority_for_grant(
                        conn, row, now=observed_at
                    )
                    if not _refresh_digest_matches(
                        grant, presented_digest, observed_at
                    ):
                        raise RuntimeActorCredentialError(
                            "invalid_credential",
                            "Runtime actor refresh is no longer current.",
                            actor=_actor_from_row(grant),
                        )
                    expired = (
                        _as_utc(grant.get("refresh_expires_at")) is None
                        or _as_utc(grant.get("refresh_expires_at")) <= observed_at
                    )
                    using_previous = grant.get("refresh_token_hash") != presented_digest
                    verification_decision = None
                    if verification_enabled:
                        from orchestrator.services.runtime_actor_verification import (
                            prepare_refresh_on_conn,
                        )

                        verification_decision = await prepare_refresh_on_conn(
                            conn,
                            post=post,
                            thread=_thread,
                            agent=agent,
                            grant=grant,
                            pre_turn=verification_pre_turn,
                            now=observed_at,
                        )
                        if verification_decision.block_code:
                            return RuntimeActorRefreshExchange(
                                actor=None,
                                retryable_failure_code=(
                                    verification_decision.block_code
                                ),
                                verification_plan_id=(verification_decision.plan_id),
                            )
                        if verification_decision.inject_maintenance_failure:
                            await _write_runtime_incident_on_conn(
                                conn,
                                post,
                                thread_id=str(grant["thread_id"]),
                                incarnation=int(grant["officer_incarnation"]),
                                failure_code="verification_maintenance_failure",
                                now=observed_at,
                            )
                            return RuntimeActorRefreshExchange(
                                actor=None,
                                retryable_failure_code=(
                                    "verification_maintenance_failure"
                                ),
                                verification_plan_id=(verification_decision.plan_id),
                            )

                    next_refresh = observed_at + timedelta(seconds=REFRESH_TTL_SECONDS)
                    refresh_token = token
                    generation = int(grant.get("credential_generation") or 1)
                    previous_hash = grant.get("previous_refresh_token_hash")
                    previous_until = grant.get("previous_refresh_valid_until")
                    handoff_ciphertext = grant.get("refresh_handoff_ciphertext")
                    handoff_acknowledged_at = grant.get(
                        "refresh_handoff_acknowledged_at"
                    )

                    if using_previous:
                        # The predecessor is a recovery receipt for this one
                        # pending generation, never authority to rotate again.
                        # Decrypt only after exact lifecycle revalidation and
                        # verify the ciphertext against the authoritative hash.
                        refresh_token = _decrypt_refresh_handoff(handoff_ciphertext)
                        if _digest(refresh_token) != grant.get("refresh_token_hash"):
                            raise RuntimeActorCredentialError(
                                "invalid_credential",
                                "Runtime actor refresh handoff is unavailable.",
                                actor=_actor_from_row(grant),
                            )
                    elif (
                        expired
                        or bool(grant.get("refresh_rotation_required"))
                        or bool(
                            verification_decision
                            and verification_decision.force_rotation
                        )
                    ):
                        refresh_token = _token("srr")
                        previous_hash = grant.get("refresh_token_hash")
                        # No deadline runs while delivery is ambiguous. The
                        # first request using ``refresh_token`` acknowledges
                        # receipt and replaces infinity with the 120s overlap.
                        previous_until = datetime.max.replace(tzinfo=timezone.utc)
                        handoff_ciphertext = _encrypt_refresh_handoff(refresh_token)
                        handoff_acknowledged_at = None
                        generation += 1
                    elif handoff_ciphertext is not None:
                        acknowledged = _as_utc(handoff_acknowledged_at)
                        overlap_until = _as_utc(previous_until)
                        if acknowledged is None:
                            handoff_acknowledged_at = observed_at
                            previous_until = observed_at + timedelta(
                                seconds=max(1, REFRESH_ROTATION_OVERLAP_SECONDS)
                            )
                        elif overlap_until is None or overlap_until <= observed_at:
                            previous_hash = None
                            previous_until = None
                            handoff_ciphertext = None
                            handoff_acknowledged_at = None
                    await conn.execute(
                        """
                        UPDATE runtime_actor_grants
                           SET refresh_token_hash = $2,
                               previous_refresh_token_hash = $3,
                               previous_refresh_valid_until = $4,
                               refresh_handoff_ciphertext = $5,
                               refresh_handoff_acknowledged_at = $6,
                               agent_id = $7,
                               credential_generation = $8,
                               last_refreshed_at = $9,
                               last_maintenance_at = $9,
                               refresh_expires_at = $10,
                               refresh_rotation_required = FALSE
                         WHERE id = $1
                        """,
                        grant["id"],
                        _digest(refresh_token),
                        previous_hash,
                        previous_until,
                        handoff_ciphertext,
                        handoff_acknowledged_at,
                        agent["id"],
                        generation,
                        observed_at,
                        next_refresh,
                    )
                    # Access credentials never straddle a renewal/recovery.
                    # Concurrent calls serialize here; the last committed
                    # response is the sole live access authority.
                    await conn.execute(
                        "DELETE FROM runtime_actor_access_tokens WHERE grant_id = $1",
                        grant["id"],
                    )
                    access_token, access_expires_at = await _insert_access_token(
                        conn, grant["id"], now=observed_at
                    )
                    await _resolve_runtime_incident_on_conn(
                        conn,
                        post,
                        thread_id=str(grant["thread_id"]),
                        incarnation=int(grant["officer_incarnation"]),
                        now=observed_at,
                    )
                    response_lost = False
                    if verification_enabled and verification_decision is not None:
                        from orchestrator.services.runtime_actor_verification import (
                            finish_refresh_on_conn,
                        )

                        response_lost = await finish_refresh_on_conn(
                            conn,
                            post=post,
                            thread=_thread,
                            agent=agent,
                            grant=grant,
                            decision=verification_decision,
                            resulting_generation=generation,
                            using_previous=using_previous,
                            now=observed_at,
                        )
            actor.access_credential = access_token
            actor.refresh_credential = refresh_token
            actor.access_expires_at = access_expires_at
            actor.refresh_expires_at = next_refresh
            return RuntimeActorRefreshExchange(
                actor=actor,
                response_lost=response_lost,
                verification_plan_id=(
                    verification_decision.plan_id
                    if verification_decision is not None
                    else None
                ),
            )

        # Non-Officer actors keep their existing authority and expiry rules.
        if actor.refresh_expires_at is None or actor.refresh_expires_at <= observed_at:
            raise RuntimeActorCredentialError(
                "expired_credential", "Runtime actor refresh has expired.", actor=actor
            )
        await _current_actor(db, actor)
        slides = actor.caller_kind != "worker" and bool(row["thread_id"])
        next_refresh = (
            observed_at + timedelta(seconds=REFRESH_TTL_SECONDS) if slides else None
        )
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "DELETE FROM runtime_actor_access_tokens WHERE grant_id = $1",
                    row["id"],
                )
                access_token, access_expires_at = await _insert_access_token(
                    conn, row["id"], now=observed_at
                )
                if next_refresh is not None:
                    await conn.execute(
                        "UPDATE runtime_actor_grants SET last_refreshed_at = $2, "
                        "last_maintenance_at = $2, refresh_expires_at = $3 "
                        "WHERE id = $1",
                        row["id"],
                        observed_at,
                        next_refresh,
                    )
                else:
                    await conn.execute(
                        "UPDATE runtime_actor_grants SET last_refreshed_at = $2, "
                        "last_maintenance_at = $2 WHERE id = $1",
                        row["id"],
                        observed_at,
                    )
        actor.access_credential = access_token
        actor.refresh_credential = token
        actor.access_expires_at = access_expires_at
        if next_refresh is not None:
            actor.refresh_expires_at = next_refresh
        return RuntimeActorRefreshExchange(actor=actor)
    except RuntimeActorCredentialError as error:
        await _audit_denial(
            db,
            request,
            error,
            action=action,
            project_id=error.actor.project_id if error.actor else None,
        )
        raise _http_denial(error, action=action) from error


async def refresh_runtime_actor_request(db: Any, request: Any) -> RuntimeActorContext:
    """Compatibility wrapper for callers that do not enable verification."""

    exchange = await refresh_runtime_actor_exchange(
        db, request, verification_enabled=False
    )
    assert exchange.actor is not None
    return exchange.actor


async def slide_thread_grant_on_liveness(
    db: Any,
    thread_id: str,
    *,
    agent_id: str | None = None,
    session_runtime_generation: str | None = None,
    session_runtime_attach_token: str | None = None,
) -> bool:
    """Slide a live thread's grants forward because the thread is still ALIVE.

    ``refresh_runtime_actor_request`` already slides the window, but only from
    inside a refresh — and the runtime only refreshes when it needs an access
    token for a PRIVILEGED call. That keys an IDLE timeout on MUTATIONS rather
    than on liveness, which inverts the risk: a busy officer is safe and a quiet
    one starves. Officer 6ce5bc4c woke every 10 minutes for 24h reading SITREPs,
    made no privileged call, never refreshed, and so walked into the wall while
    demonstrably alive (knowledge/issues/
    officer_runtime_grant_expires_after_24h_and_dies_silently.md).

    An agent heartbeat from a thread-bound runtime is exactly the liveness
    signal the refresh-path comment describes, so it licenses the same
    extension. This is deliberately server-side: persistent thread pods are
    bare pods no rollout ever updates, so an agent-side fix would never reach
    an officer that is already running.

    Nothing here widens the documented threat model. The window bounds only how
    long a *stolen* refresh token stays useful, and the refresh path already
    licenses "no absolute cap while its thread lives"; authority itself is never
    cached — it is recomputed on every access and every refresh.

    Returns True when at least one grant was slid.
    """

    # A heartbeat only licenses a grant extension when it proves the complete
    # identity of the exact pinned runtime that produced it.  Older or partial
    # heartbeat payloads remain valid liveness reports, but cannot extend
    # runtime-actor authority.
    if not (agent_id and session_runtime_generation and session_runtime_attach_token):
        return False

    now = datetime.now(timezone.utc)
    async with db.acquire() as conn:
        # Everything that must NOT be slid is excluded in SQL, so the common
        # case (a grant nowhere near its wall) costs one indexed read and no
        # write at all:
        #   * workers — job-scoped, no thread liveness to claim (the refresh
        #     path excludes them identically);
        #   * revoked grants;
        #   * ALREADY-EXPIRED grants: expiry stays terminal. This fix prevents
        #     reaching the wall, it never resurrects a credential past it;
        #   * grants still in the first half of their window — the throttle.
        rows = await conn.fetch(
            """
            SELECT id, caller_kind, user_id, project_id, project_role,
                   thread_id, officer_incarnation, refresh_expires_at,
                   revoked_at
              FROM runtime_actor_grants
             WHERE thread_id = $1::uuid
               AND revoked_at IS NULL
               AND caller_kind <> 'worker'
               AND refresh_expires_at > now()
               AND refresh_expires_at < now() + make_interval(secs => $2::int)
            """,
            str(thread_id),
            LIVENESS_SLIDE_BELOW_SECONDS,
        )
    if not rows:
        return False

    next_refresh_expires_at = now + timedelta(seconds=REFRESH_TTL_SECONDS)
    slid = False
    for row in rows:
        actor = _actor_from_row(row)
        try:
            # The SAME authority the refresh path applies before it slides:
            # _current_actor re-derives from durable state via
            # derive_runtime_actor, which refuses an `ended` thread and any
            # grant that no longer matches current post/membership. A heartbeat
            # is evidence of a live POD; this is what makes it evidence of a
            # live, still-authorized THREAD.
            await _current_actor(db, actor)
        except RuntimeActorCredentialError:
            # Fail closed: a grant the refresh path would refuse is a grant
            # this path must leave to expire.
            continue
        async with db.acquire() as conn:
            updated = await conn.execute(
                """
                UPDATE runtime_actor_grants AS grant
                   SET last_refreshed_at = now(),
                       refresh_expires_at = $2
                  FROM threads AS thread
                  JOIN agents AS agent
                    ON agent.id = thread.agent_id
                   AND agent.thread_id = thread.id
                 WHERE grant.id = $1
                   AND grant.thread_id = thread.id
                   AND grant.agent_id = thread.agent_id
                   AND thread.id = $3::uuid
                   AND thread.execution_lane = 'pinned'
                   AND thread.status IN ('created', 'active', 'awaiting_user')
                   AND thread.runtime_retirement_token IS NULL
                   AND thread.agent_id = $4::uuid
                   AND thread.runtime_generation = $5::uuid
                   AND thread.runtime_attach_token
                       IS NOT DISTINCT FROM $6::uuid
                   AND grant.revoked_at IS NULL
                   AND grant.refresh_expires_at > now()
                """,
                row["id"],
                next_refresh_expires_at,
                thread_id,
                agent_id,
                session_runtime_generation,
                session_runtime_attach_token,
            )
        slid = updated == "UPDATE 1" or slid
    return slid


async def maintain_current_officer_runtime(
    db: Any,
    *,
    project_id: str,
    thread_id: str,
    now: datetime | None = None,
    verification_enabled: bool = False,
) -> OfficerRuntimeMaintenance:
    """Renew or recover the exact current live Officer grant.

    This credential-independent liveness point re-derives and locks the current
    Post, thread, live agent binding, project authority, incarnation, and grant.
    It may recover an expired grant only when every one of those server-owned
    facts still matches. Recovery restores the existing bearer just long
    enough to keep an already-running pod usable and marks it for mandatory
    rotation on its next refresh. Historical Officers and workers never enter
    this path. Other failures become one durable Post incident.
    """

    observed_at = now or datetime.now(timezone.utc)
    try:
        async with db.acquire() as conn:
            async with conn.transaction():
                post = await conn.fetchrow(
                    "SELECT project_id, thread_id, incarnations, state "
                    "FROM project_officers WHERE project_id = $1::uuid FOR UPDATE",
                    str(project_id),
                )
                if post is None or str(post.get("thread_id") or "") != str(thread_id):
                    return OfficerRuntimeMaintenance(False, "not_current")
                thread = await conn.fetchrow(
                    "SELECT id, project_id, user_id, status, metadata, "
                    "execution_lane, runtime_generation, runtime_attach_token, "
                    "runtime_retirement_token, agent_id FROM threads "
                    "WHERE id = $1::uuid FOR UPDATE",
                    str(thread_id),
                )
                incarnations = post.get("incarnations") or []
                if isinstance(incarnations, str):
                    try:
                        incarnations = json.loads(incarnations)
                    except (TypeError, ValueError):
                        incarnations = []
                incarnation = len(incarnations) if isinstance(incarnations, list) else 0
                incident = _json_object(
                    _json_object(post.get("state")).get("runtime_actor_incident")
                )
                incident_retry = _parse_incident_time(incident.get("next_retry_at"))
                if (
                    incident.get("status") == "open"
                    and incident.get("key")
                    == _officer_incident_key(thread_id, incarnation)
                    and incident_retry is not None
                    and incident_retry > observed_at
                ):
                    # The 60-second watchdog cadence must not silently defeat
                    # the durable exponential retry policy. Credential-bearing
                    # recovery can still resolve this incident at any moment.
                    notification_claim_id = (
                        await _claim_runtime_incident_notification_on_conn(
                            conn, post, incident, now=observed_at
                        )
                    )
                    return OfficerRuntimeMaintenance(
                        False,
                        "backoff",
                        project_id=str(post["project_id"]),
                        thread_id=str(thread_id),
                        officer_incarnation=incarnation,
                        failure_code=str(
                            incident.get("failure_class") or "authorization"
                        ),
                        retry_at=incident_retry,
                        notification_due=notification_claim_id is not None,
                        notification_claim_id=notification_claim_id,
                    )
                metadata = _json_object(thread.get("metadata")) if thread else {}
                officer = _json_object(
                    _json_object(metadata.get("config_override")).get("officer")
                )
                if (
                    thread is None
                    or thread.get("project_id") != post.get("project_id")
                    or str(thread.get("status") or "") == "ended"
                    or officer.get("enabled") not in {True, "true", "True", 1}
                ):
                    return OfficerRuntimeMaintenance(False, "not_current")
                if not _exact_live_pinned_runtime(thread):
                    return OfficerRuntimeMaintenance(
                        False,
                        "lifecycle_pending",
                        project_id=str(post["project_id"]),
                        thread_id=str(thread["id"]),
                        officer_incarnation=incarnation,
                        failure_code="exact_runtime_authority_missing",
                    )
                agent = (
                    await conn.fetchrow(
                        "SELECT id, thread_id, status, last_heartbeat FROM agents "
                        "WHERE id = $1 FOR UPDATE",
                        thread.get("agent_id"),
                    )
                    if thread.get("agent_id")
                    else None
                )
                heartbeat = _as_utc(agent.get("last_heartbeat")) if agent else None
                live_agent = (
                    agent is not None
                    and agent.get("thread_id") == thread.get("id")
                    and str(agent.get("status") or "")
                    not in {"offline", "failed", "draining"}
                    and heartbeat is not None
                    and heartbeat
                    > observed_at
                    - timedelta(seconds=max(1, OFFICER_AGENT_LIVE_SECONDS))
                )
                if not live_agent:
                    # Pod drain, deletion, suspension and replacement are
                    # runtime lifecycle states owned by the watchdog respawn
                    # path. They suppress authorization/spend, but must not
                    # create a misleading credential incident during the
                    # expected no-agent interval.
                    return OfficerRuntimeMaintenance(
                        False,
                        "lifecycle_pending",
                        project_id=str(post["project_id"]),
                        thread_id=str(thread["id"]),
                        officer_incarnation=incarnation,
                        failure_code="live_agent_binding_missing",
                    )

                grant = await conn.fetchrow(
                    """
                    SELECT id, refresh_token_hash, previous_refresh_token_hash,
                           previous_refresh_valid_until,
                           refresh_handoff_ciphertext,
                           refresh_handoff_acknowledged_at,
                           caller_kind, user_id, project_id, project_role,
                           thread_id, officer_incarnation, agent_id,
                           credential_generation, refresh_rotation_required,
                           refresh_expires_at, revoked_at, created_at
                      FROM runtime_actor_grants
                     WHERE caller_kind = 'officer'
                       AND project_id = $1
                       AND thread_id = $2
                       AND officer_incarnation = $3
                       AND revoked_at IS NULL
                       AND agent_id = $4
                     FOR UPDATE
                    """,
                    post["project_id"],
                    thread["id"],
                    incarnation,
                    agent["id"],
                )
                legacy_ambiguous = False
                if grant is None:
                    legacy_candidates = await conn.fetch(
                        """
                        SELECT id, refresh_token_hash, previous_refresh_token_hash,
                               previous_refresh_valid_until,
                               refresh_handoff_ciphertext,
                               refresh_handoff_acknowledged_at,
                               caller_kind, user_id, project_id, project_role, thread_id,
                               officer_incarnation, agent_id,
                               credential_generation, refresh_rotation_required,
                               refresh_expires_at, revoked_at, created_at
                          FROM runtime_actor_grants
                         WHERE caller_kind = 'officer'
                           AND project_id = $1
                           AND thread_id = $2
                           AND officer_incarnation = $3
                           AND revoked_at IS NULL
                           AND agent_id IS NULL
                         ORDER BY id
                         LIMIT 2
                         FOR UPDATE
                        """,
                        post["project_id"],
                        thread["id"],
                        incarnation,
                    )
                    if len(legacy_candidates) == 1:
                        grant = legacy_candidates[0]
                    elif len(legacy_candidates) > 1:
                        legacy_ambiguous = True
                failure_code = None
                if legacy_ambiguous:
                    failure_code = "ambiguous_legacy_grants"
                elif grant is None:
                    failure_code = "current_grant_missing"
                elif grant.get("user_id") != thread.get("user_id"):
                    failure_code = "grant_owner_mismatch"
                else:
                    role_row = await conn.fetchrow(
                        """
                        SELECT u.is_admin, pm.role
                          FROM users u
                          LEFT JOIN project_members pm
                            ON pm.user_id = u.id AND pm.project_id = $2
                         WHERE u.id = $1
                        """,
                        thread["user_id"],
                        post["project_id"],
                    )
                    current_role = (
                        "admin"
                        if role_row and bool(role_row.get("is_admin"))
                        else str(role_row.get("role"))
                        if role_row and role_row.get("role")
                        else ""
                    )
                    if current_role != str(grant.get("project_role") or ""):
                        failure_code = "grant_authority_changed"
                expiry = _as_utc(grant.get("refresh_expires_at")) if grant else None
                expired = expiry is None or expiry <= observed_at

                if failure_code is not None:
                    incident, changed, _ = await _write_runtime_incident_on_conn(
                        conn,
                        post,
                        thread_id=str(thread["id"]),
                        incarnation=incarnation,
                        failure_code=failure_code,
                        now=observed_at,
                    )
                    notification_claim_id = (
                        await _claim_runtime_incident_notification_on_conn(
                            conn, post, incident, now=observed_at
                        )
                    )
                    return OfficerRuntimeMaintenance(
                        False,
                        "failed",
                        project_id=str(post["project_id"]),
                        thread_id=str(thread["id"]),
                        officer_incarnation=incarnation,
                        failure_code=failure_code,
                        retry_at=_parse_incident_time(incident.get("next_retry_at")),
                        notification_due=notification_claim_id is not None,
                        notification_claim_id=notification_claim_id,
                        incident_changed=changed,
                    )

                assert grant is not None and agent is not None
                verification_decision = None
                if verification_enabled:
                    from orchestrator.services.runtime_actor_verification import (
                        observe_maintenance_on_conn,
                    )

                    verification_decision = await observe_maintenance_on_conn(
                        conn,
                        post=post,
                        thread=thread,
                        agent=agent,
                        grant=grant,
                        now=observed_at,
                    )
                    if verification_decision.inject_maintenance_failure:
                        incident, changed, _ = await _write_runtime_incident_on_conn(
                            conn,
                            post,
                            thread_id=str(thread["id"]),
                            incarnation=incarnation,
                            failure_code="verification_maintenance_failure",
                            now=observed_at,
                        )
                        notification_claim_id = (
                            await _claim_runtime_incident_notification_on_conn(
                                conn, post, incident, now=observed_at
                            )
                        )
                        return OfficerRuntimeMaintenance(
                            False,
                            "failed",
                            project_id=str(post["project_id"]),
                            thread_id=str(thread["id"]),
                            officer_incarnation=incarnation,
                            failure_code="verification_maintenance_failure",
                            retry_at=_parse_incident_time(
                                incident.get("next_retry_at")
                            ),
                            notification_due=notification_claim_id is not None,
                            notification_claim_id=notification_claim_id,
                            incident_changed=changed,
                        )
                # Make every losing grant for this immutable incarnation
                # unusable even to a pre-0171 replica. Never invent agent
                # provenance for unbound legacy losers: NULL truthfully means
                # the old schema did not record which pod held that bearer.
                await conn.execute(
                    """
                    UPDATE runtime_actor_grants
                       SET revoked_at = $2
                     WHERE caller_kind = 'officer'
                       AND project_id = $1
                       AND thread_id = $3
                       AND officer_incarnation = $4
                       AND id <> $5
                       AND revoked_at IS NULL
                    """,
                    post["project_id"],
                    observed_at,
                    thread["id"],
                    incarnation,
                    grant["id"],
                )
                recovered = expired
                renewed = (
                    recovered
                    or bool(
                        verification_decision and verification_decision.force_renewal
                    )
                    or expiry
                    <= observed_at
                    + timedelta(seconds=max(1, OFFICER_RENEW_BEFORE_SECONDS))
                )
                next_expiry = (
                    observed_at + timedelta(seconds=REFRESH_TTL_SECONDS)
                    if renewed
                    else expiry
                )
                await conn.execute(
                    """
                    UPDATE runtime_actor_grants
                           SET agent_id = $2,
                               refresh_expires_at = $3,
                               last_maintenance_at = $4,
                               refresh_rotation_required =
                                   refresh_rotation_required OR $5
                     WHERE id = $1
                    """,
                    grant["id"],
                    agent["id"],
                    next_expiry,
                    observed_at,
                    recovered,
                )
                resolved = await _resolve_runtime_incident_on_conn(
                    conn,
                    post,
                    thread_id=str(thread["id"]),
                    incarnation=incarnation,
                    now=observed_at,
                )
                return OfficerRuntimeMaintenance(
                    True,
                    "recovered" if recovered else "renewed" if renewed else "current",
                    project_id=str(post["project_id"]),
                    thread_id=str(thread["id"]),
                    officer_incarnation=incarnation,
                    incident_changed=resolved,
                )
    except RuntimeActorCredentialError as exc:
        return OfficerRuntimeMaintenance(
            False,
            "failed",
            project_id=str(project_id),
            thread_id=str(thread_id),
            failure_code=exc.code,
        )


def _parse_incident_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, str):
        try:
            return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            return None
    return None


async def admit_officer_wake_for_runtime(
    db: Any, *, project_id: str, thread_id: str, now: datetime | None = None
) -> tuple[bool, datetime | None]:
    """Allow one compatibility recovery wake, then suppress repeated spend."""

    observed_at = now or datetime.now(timezone.utc)
    async with db.acquire() as conn:
        async with conn.transaction():
            post = await conn.fetchrow(
                "SELECT project_id, thread_id, incarnations, state "
                "FROM project_officers WHERE project_id = $1::uuid FOR UPDATE",
                str(project_id),
            )
            if post is None or str(post.get("thread_id") or "") != str(thread_id):
                return False, None
            incident = _json_object(
                _json_object(post.get("state")).get("runtime_actor_incident")
            )
            if incident.get("status") != "open":
                return True, None
            incarnations = post.get("incarnations") or []
            if isinstance(incarnations, str):
                try:
                    incarnations = json.loads(incarnations)
                except (TypeError, ValueError):
                    incarnations = []
            incarnation = len(incarnations) if isinstance(incarnations, list) else 0
            if incident.get("key") != _officer_incident_key(thread_id, incarnation):
                # An incident belongs to one immutable incarnation. It remains
                # historical/operator-visible but cannot suppress the
                # successor's commission wake or credential maintenance.
                return True, None
            if incident.get("recovery_probe_at") is None:
                incident["recovery_probe_at"] = observed_at.isoformat()
                await conn.execute(
                    """
                    UPDATE project_officers
                       SET state = jsonb_set(COALESCE(state, '{}'::jsonb),
                                             '{runtime_actor_incident}',
                                             $2::jsonb, true),
                           updated_at = now()
                     WHERE project_id = $1
                    """,
                    post["project_id"],
                    json.dumps(incident),
                )
                return True, None
            return False, _parse_incident_time(incident.get("next_retry_at"))


async def settle_officer_runtime_incident_notification(
    db: Any,
    *,
    project_id: str,
    thread_id: str,
    officer_incarnation: int,
    notification_claim_id: str,
    delivered: bool,
    failure_class: str | None = None,
    now: datetime | None = None,
) -> bool:
    """CAS one out-of-band page outcome into the durable incident."""

    observed_at = now or datetime.now(timezone.utc)
    async with db.acquire() as conn:
        async with conn.transaction():
            post = await conn.fetchrow(
                "SELECT project_id, thread_id, state FROM project_officers "
                "WHERE project_id = $1::uuid FOR UPDATE",
                str(project_id),
            )
            if post is None or str(post.get("thread_id") or "") != str(thread_id):
                return False
            incident = _json_object(
                _json_object(post.get("state")).get("runtime_actor_incident")
            )
            if incident.get("status") != "open" or incident.get(
                "key"
            ) != _officer_incident_key(thread_id, officer_incarnation):
                return False
            notification = _json_object(incident.get("notification"))
            if (
                notification.get("state") != "sending"
                or notification.get("claim_id") != notification_claim_id
            ):
                return False
            attempts = int(notification.get("attempt_count") or 0) + 1
            notification.update(
                {
                    "state": "delivered" if delivered else "failed",
                    "attempt_count": attempts,
                    "last_attempted_at": observed_at.isoformat(),
                    "delivered_at": observed_at.isoformat() if delivered else None,
                    "failure_class": None
                    if delivered
                    else str(failure_class or "delivery"),
                    "next_retry_at": (
                        None
                        if delivered
                        else _incident_retry_at(observed_at, attempts).isoformat()
                    ),
                    "claim_id": None,
                    "claimed_at": None,
                    "claim_expires_at": None,
                }
            )
            incident["notification"] = notification
            await conn.execute(
                """
                UPDATE project_officers
                   SET state = jsonb_set(COALESCE(state, '{}'::jsonb),
                                         '{runtime_actor_incident}', $2::jsonb, true),
                       updated_at = now()
                 WHERE project_id = $1
                """,
                post["project_id"],
                json.dumps(incident),
            )
            return True
