"""Per-resource access checks for the orchestrator API.

This module is the home for the "can user U see / act on resource R?"
question. ``security/auth.py`` resolves *who* the caller is; this module
resolves *what* they're allowed to touch. They're deliberately split:

* auth.py loads identity from a session cookie / Bearer / MCP header.
* access.py applies the visibility model: a user can see a resource iff
  (a) they own it, (b) they're a member of a project that owns it,
  (c) they're an admin. MCP tokens further restrict by scope.

Helpers are direct async functions (no FastAPI ``Depends`` factory),
matching the inline ``await require_approved_user(request, db)`` style
already used across ~86 endpoints in main.py. Each function returns the
loaded resource on success so callers don't refetch.

Status code policy: 404 when the resource doesn't exist, 403 when it
does but the caller lacks access. Same shape as H1-H5, decided in
``knowledge-base/knowledge/multi_tenancy.md`` open-question #1. Every 403 raised here is
additionally recorded as a security event (structured log line + a
best-effort ``security_events`` row) via :func:`log_security_event` —
see ``knowledge-base/knowledge/features/security_event_log.md``.

The H1-H5 hotfix helpers (``user_can_access_ide_entity``,
``require_project_owner``, ``require_sudo_request_authority``) moved
here from main.py without behavior changes — F2-F7 will add the new
helpers (``require_job_access``, ``require_project_member``, etc.) to
new endpoints. F1 is a pure foundation: no endpoint behavior changes,
just the home for the next four bundles of work.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import re
from typing import Any, Awaitable, Callable, Literal
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import UUID

from fastapi import HTTPException, Request

from orchestrator.security.auth import require_approved_user
from orchestrator.services.project_status import (
    PROJECT_ARCHIVED_DETAIL as PROJECT_ARCHIVED_DETAIL,
    project_is_archived as project_is_archived,
)

logger = logging.getLogger(__name__)

# Shared bootstrap secret for agent ↔ orchestrator and MCP-bridge ↔ orchestrator
# traffic. Distributed via Helm as the ``MCP_INTERNAL_KEY`` env var (already
# set on orchestrator + MCP pods; P4b adds it to agent pods too). Read once at
# import time — the env doesn't change between requests.
_INTERNAL_KEY = os.environ.get("MCP_INTERNAL_KEY", "")


Role = Literal["viewer", "editor", "owner"]
_ROLE_RANK: dict[str, int] = {"viewer": 0, "editor": 1, "owner": 2}


def _role_satisfies(actual: str | None, minimum: Role) -> bool:
    """Whether ``actual`` (a row from project_members) clears ``minimum``."""
    if actual is None:
        return False
    actual_rank = _ROLE_RANK.get(actual)
    if actual_rank is None:
        return False
    return actual_rank >= _ROLE_RANK[minimum]


# =============================================================================
# Denied-access audit — every 403 below leaves a trace
# =============================================================================
#
# Before this, every gate denied silently: 1000 UUID-probe attempts against
# another user's resources produced 1000 identical 403s and zero detection
# signal. Each deny now emits a structured WARNING line (survives in pod
# logs even if the DB is down) plus a best-effort row in the
# ``security_events`` table (queryable forensics, pruned on retention).
# Closes M1.B #4; design in knowledge-base/knowledge/features/security_event_log.md.


def _request_meta(request: Any) -> tuple[str | None, str | None, str | None]:
    """Best-effort ``(method, path, client_ip)`` from a Request/WebSocket.

    Deliberately paranoid: tests pass bare MagicMocks, the WS handshake
    has no ``method`` attribute, and proxies may omit forwarding headers.
    Anything that isn't a real string degrades to None rather than
    raising — the event row is still worth writing without it.
    """
    if request is None:
        return None, None, None
    method = getattr(request, "method", None)
    if not isinstance(method, str):
        method = None
    path = getattr(getattr(request, "url", None), "path", None)
    if not isinstance(path, str):
        path = None
    client_ip: str | None = None
    try:
        fwd = request.headers.get("x-forwarded-for", "")
        if isinstance(fwd, str) and fwd:
            client_ip = fwd.split(",")[0].strip()
    except Exception:
        client_ip = None
    if not client_ip:
        host = getattr(getattr(request, "client", None), "host", None)
        client_ip = host if isinstance(host, str) else None
    return method, path, client_ip


async def log_security_event(
    db,
    *,
    resource_type: str,
    event_type: str = "access_denied",
    user: dict[str, Any] | None = None,
    resource_id: str | None = None,
    detail: str = "",
    request: Any = None,
    method: str | None = None,
    path: str | None = None,
) -> None:
    """Record a denied-access event. Never raises.

    Emits the structured log line first, then the DB row, so a database
    outage still leaves a trace in pod logs. A failed insert is loud
    (``logger.error``) but never blocks the 403 it documents — a broken
    audit trail must not turn a deny into a 500.

    ``request`` may be a FastAPI ``Request``, a Starlette ``WebSocket``
    (pass ``method='WS'`` explicitly), or None. ``user`` is the resolved
    auth dict; ``real_is_admin``/``is_admin`` are compared to record
    whether the admin "view as user" shadow was on (an admin exercising
    the toggle is distinguishable from a genuine cross-user attempt).
    """
    user = user or {}
    user_id = str(user["id"]) if user.get("id") else None
    raw_auth = user.get("auth_method")
    auth_method = raw_auth if isinstance(raw_auth, str) else None
    real_is_admin = bool(user.get("real_is_admin", user.get("is_admin", False)))
    view_as = real_is_admin and not bool(user.get("is_admin"))
    req_method, req_path, client_ip = _request_meta(request)
    method = method or req_method
    path = path or req_path
    logger.warning(
        "security-event %s: user=%s auth=%s resource=%s/%s method=%s "
        "path=%s view_as=%s ip=%s detail=%s",
        event_type,
        user_id,
        auth_method,
        resource_type,
        resource_id,
        method,
        path,
        view_as,
        client_ip,
        detail,
    )
    if db is None:
        return
    try:
        await db.record_security_event(
            event_type=event_type,
            user_id=user_id,
            auth_method=auth_method,
            real_is_admin=real_is_admin,
            view_as=view_as,
            resource_type=resource_type,
            resource_id=str(resource_id) if resource_id is not None else None,
            method=method,
            path=path,
            detail=detail,
            client_ip=client_ip,
        )
    except Exception as exc:
        logger.error("security-event DB write failed (deny proceeds): %s", exc)


async def require_admin(
    request: Request,
    db: Any,
    *,
    resolve_user: Callable[[Request, Any], Awaitable[dict[str, Any]]] | None = None,
    audit: Callable[..., Awaitable[None]] | None = None,
) -> dict[str, Any]:
    """Require an approved admin against the caller's database.

    Use the un-shadowed privilege flag so an admin keeps access while viewing
    as a user. The optional collaborators preserve the existing composition
    adapter's call-time bindings; ordinary callers use these module defaults.
    """
    if resolve_user is None:
        resolve_user = require_approved_user
    if audit is None:
        audit = log_security_event
    user = await resolve_user(request, db)
    if not user.get("real_is_admin", False):
        await audit(
            db,
            event_type="admin_denied",
            user=user,
            resource_type="admin_endpoint",
            resource_id=getattr(getattr(request, "url", None), "path", None),
            detail="Admin access required",
            request=request,
        )
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


async def _denied(
    request: Any,
    db,
    user: dict[str, Any] | None,
    *,
    resource_type: str,
    resource_id: str | None,
    detail: str,
) -> HTTPException:
    """Log a denied-access event and return the 403 to raise.

    Usage: ``raise await _denied(...)`` — the log and the raise live in
    one expression so no future gate can do one without the other.
    """
    await log_security_event(
        db,
        user=user,
        resource_type=resource_type,
        resource_id=resource_id,
        detail=detail,
        request=request,
    )
    return HTTPException(status_code=403, detail=detail)


# =============================================================================
# Project lifecycle — archived projects refuse new work
# =============================================================================
#
# ``archived`` is a real lifecycle state, not a badge: reads stay open, new
# work is refused, teardown stays possible. Design + the full WRITE/ALLOW/READ
# call-site classification live in
# ``knowledge-base/knowledge/features/project_and_job_list_filtering.md`` §4.3.

#: The only two statuses this product writes. ``paused``/``completed`` are
#: permitted by the DB CHECK but nothing sets them, and the column is nullable
#: (a CHECK passes on NULL). Anything outside this tuple is *unclassifiable*,
#: not archived — see :func:`project_is_archived` and
#: :func:`project_status_filter_sql`, which both fail toward showing.
KNOWN_PROJECT_STATUSES: tuple[str, ...] = ("active", "archived")

#: What ``GET /api/projects`` returns when the caller passes no ``?status=``.
DEFAULT_PROJECT_STATUSES: tuple[str, ...] = ("active",)


def normalize_project_statuses(values: Any) -> list[str]:
    """Normalise a repeatable ``?status=`` query param.

    Lower-cases, trims, drops blanks and dedupes while preserving order (the
    ``dict.fromkeys`` idiom ``GET /api/datasources/eligible`` already uses).
    ``None`` — and a list that normalises to nothing — yields the default
    (``['active']``); omission must never widen to "everything".

    ``values`` is typed loosely on purpose. FastAPI hands the handler a list
    or ``None``; anything else means an in-process caller invoked the handler
    directly without supplying the parameter, so it still holds the unresolved
    ``Query(...)`` default. Half this repo's endpoint tests call handlers that
    way, and a total function beats making every one of them pass the param.
    """
    if not isinstance(values, (list, tuple, set)):
        values = []
    normalized = list(
        dict.fromkeys(
            str(value).strip().lower() for value in values if str(value).strip()
        )
    )
    return normalized or list(DEFAULT_PROJECT_STATUSES)


def project_status_filter_sql(param: str, *, column: str = "status") -> str:
    """``WHERE`` fragment keeping rows whose status was requested — or unknown.

    ``param`` is the caller's placeholder for the requested status array
    (e.g. ``"$1"``); ``column`` is the (already qualified) status column.

    Fail toward showing (§4.2): NULL collapses to ``active``, and a status
    outside :data:`KNOWN_PROJECT_STATUSES` always survives the filter. The
    default filter therefore hides exactly one thing — ``archived`` — instead
    of quietly swallowing every row whose state we cannot classify. The known
    vocabulary is interpolated from a module constant of literal identifiers,
    never from caller input.
    """
    known = ", ".join(f"'{value}'" for value in KNOWN_PROJECT_STATUSES)
    return (
        f"(COALESCE({column}, 'active') = ANY({param}::text[]) "
        f"OR COALESCE({column}, 'active') NOT IN ({known}))"
    )


async def _archived_conflict(
    request: Any,
    db,
    user: dict[str, Any] | None,
    *,
    resource_id: str | None,
    detail: str = PROJECT_ARCHIVED_DETAIL,
) -> HTTPException:
    """Log an archived-write refusal and return the 409 to raise.

    Sibling of :func:`_denied`, deliberately NOT a reuse of it. ``_denied``
    hardcodes 403 and writes an ``access_denied`` row into the table that
    exists to detect UUID-probing; a refusal handed to an *authorized* member
    because the project is archived is a lifecycle conflict, not an intrusion
    signal, and polluting that table would blunt the detector. Same
    raise-and-log-in-one-expression discipline: ``raise await
    _archived_conflict(...)``.

    409 rather than the 403 GitHub/GitLab use for archived-repo writes: the
    closest in-house analogue (the Officer Post admission gate — durable row
    state blocking new work) already returns 409, and 403 here would collide
    with the authorization meaning ``_denied`` owns.
    """
    await log_security_event(
        db,
        event_type="project_archived_write",
        user=user,
        resource_type="project",
        resource_id=resource_id,
        detail=detail,
        request=request,
    )
    return HTTPException(status_code=409, detail=detail)


# =============================================================================
# MCP scope guards — applied on top of identity-based visibility
# =============================================================================
#
# MCP tokens carry a legacy scope string in ``user['scopes'][0]``. F7 plumbs
# it through ``_get_user_from_mcp_headers`` (in security/auth.py) into the
# resolved user dict, so the helpers below can narrow further:
#
#   'user'           — no narrowing. Identity already restricts to the
#                       caller's own data.
#   'all'            — no narrowing at this layer. Admin-equivalent for the
#                       *user* (resolved from realm role), not for the token.
#                       A non-admin holding an 'all' token still only sees
#                       their own data — they don't gain admin powers.
#   'project:<uuid>' — caller can only see resources tied to that one
#                       project. Personal resources (threads, builder
#                       sessions) become inaccessible since they have no
#                       project. See open-question #3 in
#                       ``knowledge-base/knowledge/multi_tenancy.md``.
#
# Cookie / OIDC / PAT auth paths leave ``scopes`` empty or carry non-legacy
# entries; the guards below short-circuit to "no narrowing" for anything
# that doesn't look like ``project:<uuid>``.


def mcp_scope_project_id(user: dict[str, Any]) -> UUID | None:
    """Public accessor for the caller's MCP ``project:<uuid>`` scope.

    Thin re-export of :func:`_scope_project_id`. Useful for endpoints
    that need to pass the scope as an explicit AND-filter param to the
    DB layer (rather than relying on the visibility helpers' implicit
    narrowing). Returns ``None`` for cookie/OIDC/PAT auth and for
    non-``project:`` MCP scopes (``user`` / ``all``).
    """
    return _scope_project_id(user)


def _scope_project_id(user: dict[str, Any]) -> UUID | None:
    """Return the UUID a token is project-scoped to, or None.

    Returns None for cookie/OIDC/PAT auth, for legacy MCP scopes ``user``
    or ``all``, and for malformed values. Malformed ``project:<bad>``
    returns ``None`` here; :func:`_scope_permits_project` below treats
    that as "deny everything" so callers fail closed.
    """
    scopes = user.get("scopes") or []
    if not scopes:
        return None
    scope = scopes[0]
    if not isinstance(scope, str) or not scope.startswith("project:"):
        return None
    try:
        return UUID(scope.split(":", 1)[1])
    except (ValueError, IndexError):
        # Sentinel: a project-scoped token with an unparseable UUID.
        # Treat as a permanently-empty scope by returning a constant
        # all-zero UUID that no project will ever match.
        return UUID("00000000-0000-0000-0000-000000000000")


def _scope_permits_project(user: dict[str, Any], project_id: str | UUID | None) -> bool:
    """Whether the caller's MCP scope (if any) allows access to ``project_id``.

    No scope → always True. ``project:<uuid>`` scope → True iff the project
    matches. Anything else → True (no project-shape restriction). A None
    ``project_id`` against a project-scoped token returns False — there's
    no project to match.
    """
    scope_pid = _scope_project_id(user)
    if scope_pid is None:
        return True
    if project_id is None:
        return False
    try:
        target = project_id if isinstance(project_id, UUID) else UUID(str(project_id))
    except (ValueError, TypeError):
        return False
    return target == scope_pid


def _scope_permits_personal(user: dict[str, Any]) -> bool:
    """Whether the caller's scope allows resources with no project link.

    Threads and builder sessions have no project. A project-scoped MCP
    token shouldn't be able to read or mutate them. ``user`` / ``all`` /
    no-scope tokens always pass.
    """
    return _scope_project_id(user) is None


async def require_personal_scope(
    request: Any,
    db,
    user: dict[str, Any],
    *,
    resource_type: str,
    resource_id: str | None = None,
) -> None:
    """Refuse a ``project:<uuid>``-scoped MCP token on a personal resource.

    The public form of :func:`_scope_permits_personal`, matching how
    :func:`require_thread_owner` and :func:`user_can_access_job_or_thread`
    already apply it: log the denial and raise 403, in one expression so no
    call site can do one without the other.

    Exists because scope has to be checked where a personal credential is
    *minted*, not only where one is used. SSH keys are the case that forced
    it: ``user_can_access_job_or_thread`` denies a project-scoped token every
    thread — including the IDE — but the SSH key it registers authenticates by
    fingerprint, so by the time authorization runs the token's scope is gone.
    Registration therefore had to be gated itself, or a project-scoped token
    could mint a credential opening a shell on every session its owner has.
    """
    if _scope_permits_personal(user):
        return
    raise await _denied(
        request,
        db,
        user,
        resource_type=resource_type,
        resource_id=resource_id,
        detail="Access denied by MCP token scope",
    )


# =============================================================================
# Visibility — set-shaped + SQL-shaped
# =============================================================================


async def user_visible_project_ids(
    user: dict[str, Any], db
) -> set[UUID] | Literal["all"]:
    """Project IDs the user can see via ``project_members``.

    Admins return the sentinel string ``"all"`` so callers can short-circuit
    the WHERE clause instead of materializing every project ID. An MCP
    token with a ``project:<uuid>`` scope narrows the result to that one
    project (admin powers are restricted by the token's scope).
    """
    scope_pid = _scope_project_id(user)
    if user.get("is_admin"):
        return {scope_pid} if scope_pid else "all"
    rows = await db.get_projects_for_user(str(user["id"]))
    visible = {row["id"] for row in rows}
    if scope_pid:
        return visible & {scope_pid}
    return visible


def user_visible_jobs_clause(
    user: dict[str, Any],
    *,
    table_alias: str = "jobs",
    user_param: str = "uid",
    projects_param: str = "projects",
) -> tuple[str, dict[str, Any]]:
    """SQL fragment for ``WHERE`` restricting jobs to the caller's visibility.

    For non-admins: ``(jobs.user_id = $uid OR jobs.project_id = ANY($projects))``.
    For admins: ``TRUE`` (and the params dict is empty).

    Callers are expected to format param placeholders to match their query
    driver. We return a dict instead of a positional tuple so callers can
    splice the fragment into a larger query without numbering collisions.

    The ``projects`` value is left empty — callers must resolve the actual
    project ID list via :func:`user_visible_project_ids` and pass it
    through their own bind path. This keeps the helper synchronous and
    side-effect-free.
    """
    if user.get("is_admin"):
        return "TRUE", {}
    fragment = (
        f"({table_alias}.user_id = :{user_param} "
        f"OR {table_alias}.project_id = ANY(:{projects_param}))"
    )
    return fragment, {user_param: user["id"], projects_param: []}


# =============================================================================
# Per-resource dependencies — call inline from endpoint bodies
# =============================================================================


async def require_project_member(
    request: Request,
    db,
    project_id: str,
    *,
    min_role: Role = "viewer",
    allow_archived: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Require caller to be a project member at ``min_role`` or higher.

    Returns ``(user, project)``. Admins bypass the role check. An MCP
    token with a ``project:<uuid>`` scope must match ``project_id`` or
    the call is denied. Raises 404 if the project doesn't exist, 403
    otherwise.

    ``allow_archived=False`` additionally refuses (409) when the project is
    archived — pass it from endpoints that create new work. It defaults to
    True so the ~30 read endpoints on this guard keep serving archived
    projects: an archive you cannot open is a trap, not a lifecycle state.
    The lifecycle assertion runs LAST, after authorization, so it can never
    tell a non-member that the project exists.
    """
    user = await require_approved_user(request, db)
    project = await db.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found")
    if not _scope_permits_project(user, project_id):
        raise await _denied(
            request,
            db,
            user,
            resource_type="project",
            resource_id=project_id,
            detail="Access denied by MCP token scope",
        )
    if not user.get("is_admin"):
        role = await db.get_user_role_in_project(project_id, str(user["id"]))
        if not _role_satisfies(role, min_role):
            raise await _denied(
                request,
                db,
                user,
                resource_type="project",
                resource_id=project_id,
                detail=f"Project role '{min_role}' or higher required",
            )
    if not allow_archived and project_is_archived(project):
        raise await _archived_conflict(request, db, user, resource_id=project_id)
    return user, project


async def require_project_owner(
    request: Request,
    db,
    project_id: str,
    *,
    allow_archived: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Owner-or-admin gate on a project. Returns ``(user, project)``.

    Convenience wrapper for the common owner check (member mutations,
    project mutations). Equivalent to
    ``require_project_member(min_role='owner')`` but with a more specific
    error string. MCP scope is enforced like ``require_project_member``.

    ``allow_archived`` behaves exactly as on :func:`require_project_member`:
    default True, so teardown (DELETE, detach, decommission) and the
    unarchive PATCH keep working on an archived project.
    """
    user = await require_approved_user(request, db)
    project = await db.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found")
    if not _scope_permits_project(user, project_id):
        raise await _denied(
            request,
            db,
            user,
            resource_type="project",
            resource_id=project_id,
            detail="Access denied by MCP token scope",
        )
    if not user.get("is_admin"):
        role = await db.get_user_role_in_project(project_id, str(user["id"]))
        if role != "owner":
            raise await _denied(
                request,
                db,
                user,
                resource_type="project",
                resource_id=project_id,
                detail="Project owner role required",
            )
    if not allow_archived and project_is_archived(project):
        raise await _archived_conflict(request, db, user, resource_id=project_id)
    return user, project


async def require_job_access(
    request: Request,
    db,
    job_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Require caller to be able to see ``job_id``. Returns ``(user, job)``.

    Visible = caller owns the job, OR is a member of the job's project,
    OR is admin. An MCP token with a ``project:<uuid>`` scope additionally
    requires the job's ``project_id`` to match. Raises 404 if the job
    doesn't exist, 403 otherwise.
    """
    user = await require_approved_user(request, db)
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    if not _scope_permits_project(user, job.get("project_id")):
        raise await _denied(
            request,
            db,
            user,
            resource_type="job",
            resource_id=job_id,
            detail="Access denied by MCP token scope",
        )
    if user.get("is_admin"):
        return user, job
    if str(job.get("user_id") or "") == str(user["id"]):
        return user, job
    project_id = job.get("project_id")
    if project_id:
        role = await db.get_user_role_in_project(str(project_id), str(user["id"]))
        if role:
            return user, job
    raise await _denied(
        request,
        db,
        user,
        resource_type="job",
        resource_id=job_id,
        detail="Not authorized to access this job",
    )


async def user_can_access_any_job(
    user: dict[str, Any], db, job_ids: list[str] | list[UUID]
) -> bool:
    """True if the caller can access at least one of ``job_ids``.

    Convenience over a loop of :func:`user_can_access_job`. Used by
    endpoints that resolve a resource through a M:N join (sources via
    ``job_sources``; future: any artifact reachable by multiple jobs).
    An empty ``job_ids`` returns False — fail closed for non-admins.
    Admins with no project: scope pass without hitting the DB.
    """
    if user.get("is_admin") and _scope_project_id(user) is None:
        return True
    for jid in job_ids:
        if await user_can_access_job(user, db, str(jid)):
            return True
    return False


async def user_can_access_job(user: dict[str, Any], db, job_id: str | None) -> bool:
    """Bool variant of :func:`require_job_access` for non-HTTP call sites.

    Used by the SSE event filter in `/api/sudo/events` and similar streaming
    paths where a missing or unauthorized job means "drop this event," not
    "raise an exception." Admin short-circuits to True UNLESS the token is
    project-scoped to a different project. Orphan job_ids (None / empty /
    unknown) return False — fail closed for non-admins.
    """
    # Fast path: admins with no project: scope see every event regardless
    # of whether the underlying job still exists (the SSE filter relies on
    # this for deleted-job race conditions).
    if user.get("is_admin") and _scope_project_id(user) is None:
        return True
    if not job_id:
        return False
    job = await db.get_job(job_id)
    if not job:
        return False
    if not _scope_permits_project(user, job.get("project_id")):
        return False
    if user.get("is_admin"):
        return True
    if str(job.get("user_id") or "") == str(user["id"]):
        return True
    project_id = job.get("project_id")
    if project_id:
        role = await db.get_user_role_in_project(str(project_id), str(user["id"]))
        if role:
            return True
    return False


async def require_thread_owner(
    request: Request,
    db,
    thread_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Require caller to own the persistent thread. Returns ``(user, thread)``.

    Persistent chat threads are personal — there's no project sharing.
    Admins bypass. A ``project:<uuid>``-scoped MCP token is refused since
    the thread has no project to bind to. **Fail-closed for orphans:**
    threads with ``user_id IS NULL`` (left behind by deleted users) are
    admin-only — the pre-G3 inline checks silently allowed any caller
    through for orphan threads, which let attackers enumerate them by
    UUID. Raises 404 if the thread doesn't exist, 403 otherwise.
    """
    user = await require_approved_user(request, db)
    thread = await db.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    if not _scope_permits_personal(user):
        raise await _denied(
            request,
            db,
            user,
            resource_type="thread",
            resource_id=thread_id,
            detail="Access denied by MCP token scope",
        )
    if user.get("is_admin"):
        return user, thread
    if str(thread.get("user_id") or "") != str(user["id"]):
        raise await _denied(
            request,
            db,
            user,
            resource_type="thread",
            resource_id=thread_id,
            detail="Not your thread",
        )
    return user, thread


# =============================================================================
# Specialized helpers — multi-table or service-backed
# =============================================================================


async def user_can_access_job_or_thread(
    user: dict[str, Any], db, entity_id: str | None
) -> bool:
    """True if the caller can access ``entity_id`` as a job OR owns it as a thread.

    Some artifacts carry a ``job_id`` that, for persistent sessions, is actually
    a *thread* id (``job_id == thread_id``) with no row in ``jobs`` — citations
    are the motivating case. A pure job check (:func:`user_can_access_any_job`)
    therefore 404s for every session citation. This resolver tries the job table
    first, then the thread table (the same job-or-thread shape the embedded IDE
    uses), so worker-job and session artifacts are both authorized.

    Returns a bool — callers decide whether a miss is 404 (don't leak existence)
    or 403. Admins pass subject to scope; for jobs: owner or any project member,
    narrowed by a ``project:<uuid>`` MCP scope; for threads: owner only (a
    project-scoped token can't reach a personal thread). Orphan/unknown
    ``entity_id`` → False (fail closed for non-admins).
    """
    # Fast path: unscoped admin sees every entity, even one whose row was
    # already deleted (matches the IDE-proxy race behavior the tests assert).
    if user.get("is_admin") and _scope_project_id(user) is None:
        return True
    if not entity_id:
        return False
    # Callers hand us ids straight out of asyncpg rows, where a uuid column
    # arrives as asyncpg.pgproto.UUID rather than str. uuid.UUID() calls
    # .replace() on its argument, so an unstringified value raises
    # AttributeError — not the ValueError get_job guards — and escapes as a
    # 500. That took out every /api/sudo/requests route (list, get, approve,
    # deny) once sudo rows carried a thread_id. Normalize once, here.
    entity_id = str(entity_id)
    job = await db.get_job(entity_id)
    if job:
        if not _scope_permits_project(user, job.get("project_id")):
            return False
        if user.get("is_admin"):
            return True
        if str(job.get("user_id") or "") == str(user["id"]):
            return True
        project_id = job.get("project_id")
        if project_id:
            role = await db.get_user_role_in_project(str(project_id), str(user["id"]))
            if role:
                return True
        return False
    thread = await db.get_thread(entity_id)
    if thread:
        if not _scope_permits_personal(user):
            return False
        if user.get("is_admin"):
            return True
        return str(thread.get("user_id") or "") == str(user["id"])
    return False


async def user_can_access_ide_entity(user: dict[str, Any], db, entity_id: str) -> bool:
    """Whether ``user`` can open the embedded IDE for ``entity_id``.

    The IDE proxy accepts either a job UUID or a thread UUID in the URL
    (see ``ide_proxy_service._load_context``). Thin alias over
    :func:`user_can_access_job_or_thread` (job-first, then thread), kept as a
    named seam for the ``ide_proxy_http`` / ``ide_proxy_ws`` call sites and the
    tests that assert the IDE-proxy access behavior.
    """
    return await user_can_access_job_or_thread(user, db, entity_id)


async def require_sudo_request_authority(
    request: Request,
    db,
    request_id: str,
) -> dict[str, Any]:
    """Require caller to be allowed to approve/deny a sudo request.

    Authority = admin, project-owner of the related job, OR owner of the
    related persistent thread. Job owners CANNOT self-approve their own sudo
    requests — that would defeat the gate. Orphan requests are admin-only.

    Returns the sudo request dict. Raises 404 if unknown, 403 otherwise.

    Imports ``sudo_gate`` lazily so this module stays importable in tests
    that don't bring up the gate service (and to avoid cycles via
    ``main.py``).
    """
    from orchestrator.services.sudo_gate import sudo_gate  # noqa: PLC0415

    user = await require_approved_user(request, db)
    sudo_req = await sudo_gate.get_request(request_id)
    if not sudo_req:
        raise HTTPException(
            status_code=404, detail=f"Sudo request '{request_id}' not found"
        )
    thread_id = sudo_req.get("thread_id")
    job_id = sudo_req.get("job_id")
    entity_id = thread_id or job_id
    if not await user_can_access_job_or_thread(user, db, entity_id):
        raise await _denied(
            request,
            db,
            user,
            resource_type="sudo_request",
            resource_id=request_id,
            detail="Not authorized to act on this sudo request",
        )

    # A personal thread has no project authority tier: its owner decides.
    # ``user_can_access_job_or_thread`` already enforces owner/admin + scope.
    if thread_id:
        return sudo_req

    # Resolve the underlying job's project so the existing project-owner
    # authority rule remains intact for batch jobs.
    job_project_id = None
    if job_id:
        job = await db.get_job(str(job_id))
        if job:
            job_project_id = job.get("project_id")
    if not _scope_permits_project(user, job_project_id):
        raise await _denied(
            request,
            db,
            user,
            resource_type="sudo_request",
            resource_id=request_id,
            detail="Access denied by MCP token scope",
        )
    if user.get("is_admin"):
        return sudo_req
    if job_project_id:
        role = await db.get_user_role_in_project(str(job_project_id), str(user["id"]))
        if role == "owner":
            return sudo_req
    raise await _denied(
        request,
        db,
        user,
        resource_type="sudo_request",
        resource_id=request_id,
        detail="Not authorized to act on this sudo request",
    )


# =============================================================================
# Datasource visibility — credentials are NEVER returned via REST
# =============================================================================


_CONNECTION_ASSIGNMENT_RE = re.compile(
    r"(?P<prefix>(?:^|[;\s,&]))(?P<key>[A-Za-z][A-Za-z0-9_.-]*)"
    r"(?P<separator>\s*=\s*)(?P<value>\"[^\"]*\"|'[^']*'|[^;\s,&]*)"
)
_SECRET_CONNECTION_KEYS = frozenset(
    {
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "apikey",
        "accesskey",
        "privatekey",
        "clientsecret",
        "credential",
        "credentials",
        "signature",
        "sig",
        "auth",
        "authorization",
        "key",
        "securitytoken",
    }
)
_SECRET_CONNECTION_SUFFIXES = (
    "password",
    "passwd",
    "secret",
    "token",
    "apikey",
    "accesskey",
    "privatekey",
    "clientsecret",
    "credential",
    "credentials",
    "signature",
)


def _is_secret_connection_key(key: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", key.lower())
    return normalized in _SECRET_CONNECTION_KEYS or normalized.endswith(
        _SECRET_CONNECTION_SUFFIXES
    )


def _sanitize_datasource_connection_url(value: Any) -> tuple[str | None, bool]:
    """Strip URL/DSN credential material before a datasource reaches REST."""
    if value is None:
        return None, False
    if not isinstance(value, str):
        return None, True

    try:
        parts = urlsplit(value)
    except (TypeError, ValueError):
        # An unparsable URL cannot be proven credential-free.  Hiding it is
        # safer than returning a malformed DSN that may contain userinfo.
        return None, True

    changed = False
    netloc = parts.netloc
    if "@" in netloc:
        netloc = netloc.rsplit("@", 1)[1]
        changed = True

    query_items = parse_qsl(parts.query, keep_blank_values=True)
    safe_query_items = [
        (key, item_value)
        for key, item_value in query_items
        if not _is_secret_connection_key(key)
    ]
    if len(safe_query_items) != len(query_items):
        changed = True
        query = urlencode(safe_query_items, doseq=True)
    else:
        # Do not normalize harmless encoding (for example %20 -> +): the
        # redaction marker is also the edit form's "preserve existing" signal.
        query = parts.query

    fragment = parts.fragment
    if fragment:
        # Fragments are client-side opaque and frequently carry bearer tokens;
        # they have no role in an agent's server-side connection target.
        fragment = ""
        changed = True

    sanitized = urlunsplit((parts.scheme, netloc, parts.path, query, fragment))

    # Also cover key=value DSNs and JDBC-style semicolon properties, which
    # urllib intentionally treats as an opaque path rather than URL fields.
    def _strip_assignment(match: re.Match[str]) -> str:
        nonlocal changed
        if not _is_secret_connection_key(match.group("key")):
            return match.group(0)
        changed = True
        return f"{match.group('prefix')}{match.group('key')}{match.group('separator')}"

    sanitized = _CONNECTION_ASSIGNMENT_RE.sub(_strip_assignment, sanitized)
    return sanitized, changed or sanitized != value


def redact_datasource(ds: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of ``ds`` with the credentials field stripped.

    F3 policy: credentials never leave the orchestrator over REST. The
    agent reads them via internal dispatch; the cockpit's edit form is
    expected to use a "leave blank to keep existing" UX. See
    ``knowledge-base/knowledge/multi_tenancy.md`` open-question #2 (decided: strip always).
    """
    if not ds:
        return ds
    out = dict(ds)
    if ds.get("type") == "credentials":
        out["env_var_names"] = sorted((ds.get("credentials") or {}).get("env_vars", {}))
    out.pop("credentials", None)
    out.pop("connection_url_redacted", None)
    if "connection_url" in out:
        safe_url, changed = _sanitize_datasource_connection_url(
            out.get("connection_url")
        )
        out["connection_url"] = safe_url
        if changed:
            out["connection_url_redacted"] = True
    return out


def redact_datasources(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """List variant of :func:`redact_datasource`."""
    return [redact_datasource(row) for row in rows]


# =============================================================================
# Project repository URLs — strip embedded credentials, rewrite internal host
# =============================================================================


def vm_workspaces_on_pod_network() -> bool:
    """True when VM workspaces run on this cluster (``vm.mode=same-cluster``).

    Such a VM sits on the pod network like a workspace container, so it reaches
    the cluster-internal Gitea HTTP and SSH endpoints directly — and the
    externally-routed ones may not exist at all (k3d) or not resolve from
    inside the guest. Only the cross-cluster topology (``external``, VMs on a
    tailnet) needs the externalised addresses.
    """
    return os.environ.get("VM_MODE", "").strip().lower() == "same-cluster"


def externalize_gitea_url(url: str | None) -> str | None:
    """Rewrite a cluster-internal Gitea URL to its externally-reachable form.

    Project ``repo_url``\\ s are minted from ``GITEA_INTERNAL_URL`` (e.g.
    ``http://srw-gitea:3000``) so in-cluster workspace pods can reach the host.
    That address is unroutable from a VM (a tailnet node) or a browser — which
    is both the F29 clone/push failure *and* the reason the Repos tab shows an
    unusable link. Replace the exact internal base with ``GITEA_URL`` (including
    its public subpath) and discard legacy credentials from rewritten links.
    Managed runtime delivery uses internal endpoints, not this public projection.

    No-op when the URL doesn't point at the internal host, or when either env
    var is unset or already equal — so external repos and dev setups (where the
    two collapse to one address) pass through untouched.
    """
    if not url:
        return url
    internal = os.environ.get("GITEA_INTERNAL_URL", "").rstrip("/")
    external = os.environ.get("GITEA_URL", "").rstrip("/")
    if not internal or not external or internal == external:
        return url

    from urllib.parse import urlparse, urlunparse

    int_p = urlparse(internal)
    ext_p = urlparse(external)
    u = urlparse(url)
    if not int_p.hostname or not ext_p.hostname:
        return url
    # Match the whole origin and a path boundary, not merely a hostname.
    # Another service on the same host/port or a sibling path is not Gitea.
    if (u.scheme, u.hostname, u.port) != (int_p.scheme, int_p.hostname, int_p.port):
        return url
    internal_path = int_p.path.rstrip("/")
    if u.path != internal_path and not u.path.startswith(internal_path + "/"):
        return url
    public_path = ext_p.path.rstrip("/") + u.path[len(internal_path) :]
    netloc = ext_p.netloc.rsplit("@", 1)[-1]
    return urlunparse(
        u._replace(scheme=ext_p.scheme or u.scheme, netloc=netloc, path=public_path)
    )


def redact_repository(repo: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of a project-repository row safe to return over REST.

    Two problems with the raw row, both visible on the Repos tab:

    * historical ``repo_url`` values may embed the shared Gitea admin
      ``user:password@`` — a credential leak to every project *member* (the
      endpoint is member-gated, not owner-gated). Migration 0176 and scoped
      deploy keys make new managed values clean, but this redaction remains the
      compatibility boundary until legacy inventory is zero. Same F3 policy
      datasources already follow: credentials never leave the orchestrator.
    * the host is the internal ``srw-gitea:3000``, unusable from a browser.

    Externalize the host, then strip any userinfo, so the displayed URL is both
    safe and clickable. Drop the ``credentials`` blob too if present.
    """
    if not repo:
        return repo
    out = dict(repo)
    out.pop("credentials", None)
    raw = out.get("repo_url")
    if raw:
        ext = externalize_gitea_url(raw)
        # Strip userinfo unconditionally — even when externalization was a no-op
        # (dev single-URL setups, external repos), credentials must not be sent.
        from urllib.parse import urlparse, urlunparse

        p = urlparse(ext)
        if p.username or p.password:
            netloc = p.hostname or ""
            if p.port:
                netloc += f":{p.port}"
            ext = urlunparse(p._replace(netloc=netloc))
        out["repo_url"] = ext
    return out


def redact_repositories(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """List variant of :func:`redact_repository`."""
    return [redact_repository(row) for row in rows]


# Key names (case-insensitive) whose VALUE is always a credential. The suffix
# match covers the ``env_keys`` block (EMBEDDING_API_KEY, OPENROUTER_API_KEY,
# VISION_API_KEY, WHISPER_API_KEY, TTS_API_KEY, CITATION_LLM_API_KEY, ...).
_SECRET_KEY_NAMES = frozenset(
    {"api_key", "password", "secret", "token", "private_key", "rclone_spec"}
)
_SECRET_KEY_SUFFIX = "_api_key"


def _is_secret_key(key: str) -> bool:
    k = key.lower()
    return k in _SECRET_KEY_NAMES or k.endswith(_SECRET_KEY_SUFFIX)


def redact_config_override(co: Any) -> Any:
    """Return a deep copy of a ``config_override`` with credential fields removed.

    Non-mutating and recursive (walks dicts and lists). Removes any key whose
    name (case-insensitive) is one of api_key/password/secret/token/private_key/
    rclone_spec, or ends with ``_api_key`` — i.e. ``llm.api_key``, phase overrides
    ``llm.{strategic,tactical,summarization}.api_key``, ``auxiliary.api_key``,
    every ``env_keys.*_API_KEY``, and ``workspace.mounts[].rclone_spec``.

    Non-secret fields are preserved verbatim (``llm.model``/``provider``/
    ``base_url``/``temperature``, ``env_keys.*_MODEL``/``*_BASE_URL``/``*_PROVIDER``,
    ``workspace.backend``, ...).

    Used at two boundaries:
    - the user-facing GET endpoints (redact before returning), and
    - persistence of ``threads.metadata.config_override`` (secrets are injected
      in-flight only — see ``_inject_thread_dispatch_credentials`` in main.py).

    Keep this pure: no DB access, no logging of values (it handles secrets).
    """
    if isinstance(co, dict):
        return {
            k: redact_config_override(v) for k, v in co.items() if not _is_secret_key(k)
        }
    if isinstance(co, list):
        return [redact_config_override(v) for v in co]
    return co


def _hidden_config_key(path: tuple[str, ...], key: str) -> bool:
    """A key :func:`redact_public_config_override` removes at ``path``.

    ``remote`` is hidden under ANY ``workspace`` dict, not only the override's
    root one: the same override also rides nested — as a manifest layer, a
    merged expert config — and the transport block is the same thing there.
    """
    return _is_secret_key(key) or (key == "remote" and path[-1:] == ("workspace",))


def _public_config_view(value: Any, path: tuple[str, ...] = ()) -> Any:
    if isinstance(value, dict):
        return {
            k: _public_config_view(v, (*path, k))
            for k, v in value.items()
            if not _hidden_config_key(path, k)
        }
    if isinstance(value, list):
        return [_public_config_view(v, (*path, "[]")) for v in value]
    return value


def redact_public_config_override(co: Any) -> Any:
    """The browser-facing view of a stored config override.

    The job API's policy (``job_projection.redact_job_config_override``):
    :func:`redact_config_override` plus the ``workspace.remote`` transport block
    (SSH coordinates injected at dispatch). JSONB arrives from asyncpg as text,
    so a string is parsed and returned as an object; one that does not parse is
    dropped rather than risk returning a raw secret.

    It walks the whole value, so it also serves for a document that EMBEDS
    overrides — a Project manifest carries the project's override verbatim in
    each Expert's ``runtime.config.layers`` — and is the identity on one with
    nothing to hide.
    """
    if isinstance(co, str):
        try:
            co = json.loads(co)
        except (json.JSONDecodeError, TypeError):
            return None
    return _public_config_view(co)


# Name parts that say WHERE a request goes (``base_url``, ``EMBEDDING_BASE_URL``,
# ``endpoint_id``, ``provider``, ``http_proxy``, ``mcp_servers`` ...). A restored
# key must never ride to an endpoint its writer could not see it bound to, so a
# change to any such key anywhere in an override restores nothing. Deliberately
# broad: a false match only means a secret has to be re-entered.
_ENDPOINT_KEY_TOKENS = frozenset(
    {
        "url",
        "urls",
        "uri",
        "host",
        "hostname",
        "endpoint",
        "endpoints",
        "proxy",
        "provider",
        "server",
        "servers",
        "address",
    }
)


def _is_endpoint_key(key: str) -> bool:
    k = key.lower()
    return not _ENDPOINT_KEY_TOKENS.isdisjoint(
        re.split(r"[^a-z0-9]+", k)
    ) or k.endswith(("url", "uri", "host", "endpoint", "api_base"))


def _endpoint_values(value: Any, path: tuple[Any, ...] = ()) -> dict[Any, Any]:
    """Every endpoint-shaped key in ``value``, by full path (list index
    included, so a reordered list of endpoints counts as a change)."""
    found: dict[Any, Any] = {}
    if isinstance(value, dict):
        for k, v in value.items():
            if _is_endpoint_key(k):
                found[(*path, k)] = v
            found.update(_endpoint_values(v, (*path, k)))
    elif isinstance(value, list):
        for i, v in enumerate(value):
            found.update(_endpoint_values(v, (*path, i)))
    return found


def restore_hidden_config_values(incoming: Any, stored: Any) -> Any:
    """Put back what :func:`redact_public_config_override` hid, for a client
    that writes the redacted view back whole (read, flip one key, PATCH the
    override — the cockpit's project-memory toggle does exactly this).

    What it guarantees, and nothing more:

    * A hidden value is only ever put back at the exact path it was stored at.
    * Nothing is restored anywhere if any endpoint-shaped key (a name with a
      ``url`` / ``host`` / ``endpoint`` / ``provider`` / ``proxy`` / ``server``
      / ``address`` part, e.g. ``base_url``, ``EMBEDDING_BASE_URL``,
      ``endpoint_id``) anywhere in the written override differs from the stored
      one — added, removed or changed, including inside an explicitly sent
      ``workspace.remote``. The writer re-enters its secrets.
    * Otherwise a hidden value comes back only into a dict whose public view is
      unchanged, or a list element equal to a stored element's public view.
    * A hidden key the write sends itself, in any letter case and ``null``
      included, is never overwritten.

    It does not make a restored key safe against every edit: a change that is
    not an endpoint (a model, a tool list) keeps the keys of sections it did not
    touch. A project override has several writers — any co-owner — so the guard
    is what stops one who cannot read a key from re-pointing it at a host they
    control. ``stored`` may be JSONB text.
    """
    if isinstance(stored, str):
        try:
            stored = json.loads(stored)
        except (json.JSONDecodeError, TypeError):
            return incoming
    if _endpoint_values(incoming) != _endpoint_values(_public_config_view(stored)):
        return incoming
    return _restore_hidden(incoming, stored, ())


def _restore_hidden(incoming: Any, stored: Any, path: tuple[str, ...]) -> Any:
    if isinstance(incoming, dict) and isinstance(stored, dict):
        out = {
            k: _restore_hidden(v, stored[k], (*path, k)) if k in stored else v
            for k, v in incoming.items()
        }
        if _public_config_view(incoming, path) == _public_config_view(stored, path):
            sent = {k.lower() for k in incoming}
            for k, v in stored.items():
                if k.lower() not in sent and _hidden_config_key(path, k):
                    out[k] = copy.deepcopy(v)
        return out
    if isinstance(incoming, list) and isinstance(stored, list):
        element_path = (*path, "[]")
        unmatched = list(stored)
        out_list = []
        for item in incoming:
            match = next(
                (
                    i
                    for i, candidate in enumerate(unmatched)
                    if _public_config_view(candidate, element_path) == item
                ),
                None,
            )
            out_list.append(
                item if match is None else copy.deepcopy(unmatched.pop(match))
            )
        return out_list
    return incoming


async def user_can_access_datasource(
    user: dict[str, Any], db, ds: dict[str, Any]
) -> bool:
    """Whether ``user`` can see ``ds`` in list/get responses.

    Visible = admin, OR the caller created the datasource, OR the caller
    is a member of any project the datasource is linked to. A
    ``project:<uuid>`` MCP scope narrows the result: the datasource must
    be linked to the scoped project (creator-only access doesn't survive
    a project-scope mismatch).

    Returns False for everything else, including "global" datasources
    (`is_global=true, no project link`) the caller didn't create — those
    are admin-only by design. Agents access globals via internal dispatch,
    not through this gate.
    """
    scope_pid = _scope_project_id(user)
    project_ids = await db.list_datasource_projects(str(ds["id"]))
    if scope_pid is not None:
        # Project-scoped tokens see only datasources linked to that project.
        if scope_pid not in {UUID(str(pid)) for pid in project_ids}:
            return False
        # ... and only if the user is also a project member (or admin).
        if user.get("is_admin"):
            return True
        role = await db.get_user_role_in_project(str(scope_pid), str(user["id"]))
        return bool(role)
    if user.get("is_admin"):
        return True
    if str(ds.get("created_by") or "") == str(user["id"]):
        return True
    for pid in project_ids:
        role = await db.get_user_role_in_project(str(pid), str(user["id"]))
        if role:
            return True
    return False


async def filter_visible_datasources(
    user: dict[str, Any], db, rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Batch equivalent of :func:`user_can_access_datasource` for list views.

    Resolves the caller's visible projects ONCE (:func:`user_visible_project_ids`)
    and fetches every datasource→project link in a single query, replacing the
    per-row ``N × (1 + M)`` ``list_datasource_projects`` +
    ``get_user_role_in_project`` fan-out. Preserves the single-row semantics
    exactly:

    * admin (unscoped) → all rows;
    * creator → own rows (unscoped only — creator access does NOT survive a
      ``project:<uuid>`` token scope);
    * member → rows linked to a project the caller can see;
    * project-scoped token → rows linked to the scoped project the caller sees.

    Membership is sourced from the same ``project_members`` table as the per-row
    path (via ``user_visible_project_ids`` → ``get_projects_for_user``), so the
    visible set matches ``get_user_role_in_project`` truthiness.
    """
    if not rows:
        return []
    scope_pid = _scope_project_id(user)
    visible = await user_visible_project_ids(user, db)
    if scope_pid is None and visible == "all":
        # Unscoped admin sees every row regardless of project links — skip the
        # link fetch entirely.
        return list(rows)
    links = await db.list_datasource_projects_bulk([str(ds["id"]) for ds in rows])

    def _linked(ds: dict[str, Any]) -> set[UUID]:
        out: set[UUID] = set()
        for pid in links.get(str(ds["id"]), ()):
            try:
                out.add(UUID(str(pid)))
            except (ValueError, TypeError):
                continue
        return out

    result: list[dict[str, Any]] = []
    for ds in rows:
        linked = _linked(ds)
        if scope_pid is not None:
            # user_visible_project_ids collapses admin+scope to {scope_pid}, so
            # `visible` is always a set here (never "all"). Creator access does
            # not survive a scope mismatch — the link + visibility are required.
            if scope_pid in linked and scope_pid in visible:
                result.append(ds)
            continue
        if visible == "all":  # admin, unscoped
            result.append(ds)
            continue
        if str(ds.get("created_by") or "") == str(user["id"]):
            result.append(ds)
            continue
        if linked & visible:  # member of any linked project
            result.append(ds)
    return result


async def require_datasource_access(
    request: Request, db, datasource_id: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Require caller to be able to see ``datasource_id``.

    Returns ``(user, datasource)``. The datasource dict still contains
    the raw ``credentials`` field — callers MUST run it through
    :func:`redact_datasource` before returning it to the client. Keeping
    them in the loaded dict lets callers like ``test_datasource`` use the
    creds internally without a second DB round-trip.

    Raises 404 if missing, 403 otherwise.
    """
    user = await require_approved_user(request, db)
    ds = await db.get_datasource(datasource_id)
    if not ds:
        raise HTTPException(
            status_code=404, detail=f"Connector '{datasource_id}' not found"
        )
    if not await user_can_access_datasource(user, db, ds):
        raise await _denied(
            request,
            db,
            user,
            resource_type="datasource",
            resource_id=datasource_id,
            detail="Not authorized to access this connector",
        )
    return user, ds


async def require_datasource_owner(
    request: Request, db, datasource_id: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Require caller to be the creator of ``datasource_id``, or admin.

    Used for mutations (PUT, DELETE) and the connectivity test. Returns
    ``(user, datasource)``. The datasource dict still contains raw
    credentials — redact before returning to the client. A
    ``project:<uuid>`` MCP scope restricts mutation to datasources
    linked to that project (the token can't reach a creator's other
    datasources).

    Raises 404 if missing, 403 if caller is neither creator nor admin
    or if the scope rejects.
    """
    user = await require_approved_user(request, db)
    ds = await db.get_datasource(datasource_id)
    if not ds:
        raise HTTPException(
            status_code=404, detail=f"Connector '{datasource_id}' not found"
        )
    scope_pid = _scope_project_id(user)
    if scope_pid is not None:
        project_ids = await db.list_datasource_projects(str(datasource_id))
        if scope_pid not in {UUID(str(pid)) for pid in project_ids}:
            raise await _denied(
                request,
                db,
                user,
                resource_type="datasource",
                resource_id=datasource_id,
                detail="Access denied by MCP token scope",
            )
    if user.get("is_admin"):
        return user, ds
    if str(ds.get("created_by") or "") == str(user["id"]):
        return user, ds
    raise await _denied(
        request,
        db,
        user,
        resource_type="datasource",
        resource_id=datasource_id,
        detail="Only the connector creator or an admin can do this",
    )


# =============================================================================
# MCP scope — additional filter on top of user visibility
# =============================================================================


def apply_mcp_scope(
    user: dict[str, Any],
    *,
    table_alias: str = "jobs",
    scope_project_param: str = "scope_project",
) -> tuple[str, dict[str, Any]]:
    """Restrict a query further by the caller's MCP token scope.

    Per open-question #3 (doc): ``scope='all'`` means "admin-equivalent
    for this token's user". A non-admin holding an ``'all'`` token does
    NOT get global access — they get their own visibility set, same as
    a session cookie. A ``'user'`` scope is the explicit form of that
    (same effect). ``'project:<uuid>'`` narrows to one project.

    Returns ``(fragment, params)`` to AND into the visibility WHERE. An
    empty fragment ``""`` means "no further restriction".

    Only applies when ``user['auth_method'] == 'mcp'`` (or the future
    PAT path that carries ``scopes=['<one-mcp-scope>']``). For session
    cookies and OIDC Bearer paths, returns ``("", {})``.
    """
    scopes = user.get("scopes") or []
    if not scopes:
        return "", {}
    # Legacy MCP rows carry exactly one scope string; PAT rows have a list
    # of action scopes (not the legacy 'user'/'all'/'project:<uuid>' shape).
    # Treat anything that doesn't look like a legacy MCP scope as a no-op
    # here — PAT action scopes are enforced per route when the token is
    # resolved (security.token_scopes via auth._resolve_pat), not by
    # row-level visibility.
    scope = scopes[0]
    if scope in ("", "all", "user"):
        # 'all' = admin-equivalent for this user, but actual admin-bypass
        # is gated on the realm role, not the scope. So 'all' for a non-
        # admin still respects user visibility. Both are no-ops here.
        return "", {}
    if scope.startswith("project:"):
        project_id_str = scope.split(":", 1)[1]
        try:
            project_uuid = UUID(project_id_str)
        except ValueError:
            # Malformed scope — fail closed by intersecting with the empty
            # set. The caller's outer query becomes unsatisfiable, which
            # is what we want for a bad token.
            return f"{table_alias}.project_id = :{scope_project_param}", {
                scope_project_param: None,
            }
        return f"{table_alias}.project_id = :{scope_project_param}", {
            scope_project_param: project_uuid,
        }
    return "", {}


# =============================================================================
# Track B (P4b) — agent ↔ orchestrator shared-secret authentication
# =============================================================================
#
# The agent runs in the same cluster as the orchestrator and reaches it via
# the in-cluster Service DNS (no ingress). Public ingress traffic, however,
# was routing every path to the orchestrator without any auth on these
# agent-internal endpoints. Two complementary defenses:
#
#   1. Ingress path strip (helm/templates/ingress.yaml) — pure-agent paths
#      return 403 at the edge so external attackers can't even reach the
#      handler. In-cluster Service calls bypass the ingress and so bypass
#      the strip.
#   2. ``X-Internal-Key`` header — the agent reads ``MCP_INTERNAL_KEY``
#      from its env and sends it on every call. The helpers below check
#      it. This closes the in-cluster lateral-movement vector (a
#      compromised pod in the same namespace can reach the Service but
#      can't forge the key).
#
# For pure-internal endpoints (``require_internal``) the key is mandatory.
# For dual-callable endpoints (``require_internal_or_job_access``) the key
# acts as an agent-side bypass: with key → skip user auth; without key →
# normal ``require_job_access``. Cockpit Bearer flows keep working.


def is_internal_call(request: Request) -> bool:
    """True iff the caller presented a valid ``X-Internal-Key`` header.

    Returns False when no key is configured (``MCP_INTERNAL_KEY`` empty)
    so a misconfigured cluster fails closed — better to break in-cluster
    agent traffic loudly than to silently let anyone through.
    """
    if not _INTERNAL_KEY:
        return False
    return request.headers.get("X-Internal-Key", "") == _INTERNAL_KEY


async def require_internal(request: Request) -> None:
    """Pure-internal endpoint guard. Raises 401 without a valid X-Internal-Key.

    Use for endpoints with zero legitimate external/cockpit caller (agent
    bootstrap, heartbeat, job-complete callback, internal MCP token
    bridge, persistent-thread bookkeeping called from the agent runtime).
    """
    if not is_internal_call(request):
        raise HTTPException(status_code=401, detail="Invalid internal key")


async def require_internal_or_job_access(
    request: Request,
    db,
    job_id: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Dual-callable endpoint guard. Returns ``(user, job)`` for user calls,
    ``(None, job)`` for internal calls (job still loaded for the handler).

    Internal calls skip the user resolution entirely (the agent runtime
    has no Keycloak session) but still get the job dict — handlers
    frequently use it to look up project_id, status, etc. We pay the
    extra ``get_job`` cost (one query) to keep handler bodies identical
    across the two paths.

    Raises 404 if the job doesn't exist (both paths).
    """
    if is_internal_call(request):
        job = await db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        return None, job
    # Local import to avoid the forward-reference dance — require_job_access
    # is defined earlier in this module.
    return await require_job_access(request, db, job_id)
