"""Personal access token (PAT) action scopes: the route policy and its check.

A PAT (``Authorization: Bearer ak_…``) carries a list of action scopes from the
closed vocabulary in ``orchestrator.schemas.tokens.VALID_PAT_SCOPES``. This
module decides which scope each route needs and refuses a PAT that lacks it.
Every other credential — the ``srw_session`` cookie, a Keycloak Bearer, a
legacy ``srw_…`` MCP token (``''``/``user``/``all``/``project:<uuid>``) and the
MCP server's forwarded headers — is governed by role and row visibility alone
and is never checked here.

Why a table instead of a per-route decorator: handlers resolve their caller
inline (``await require_approved_user(request, db)``), not through dependency
injection, so a per-route check could only ever be opt-in — and an opt-in check
leaves every route nobody annotated at the owner's full reach, which is the
defect this module closes. Instead the PAT resolver
(``security.auth._resolve_pat``) looks the matched route's template up here on
every PAT request. A route no rule covers refuses PATs, and
``tests/test_pat_scope_policy.py`` fails until it is classified; the endpoint
inventory records each route's decision in its ``pat=`` column.

The mapping follows the vocabulary the Cockpit advertises
(knowledge-base/knowledge/features/auth_bff_and_api_tokens.md §3.3): a family
of routes has a ``:read`` scope for GET/HEAD/OPTIONS and a ``:write`` scope for
everything else. Surfaces outside jobs/chat/knowledge need ``admin``, which is
also a superset of every other scope. Internal agent routes, credential
minting, browser-only surfaces and WebSockets refuse PATs outright.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from orchestrator.schemas.tokens import VALID_PAT_SCOPES

PAT_AUTH_METHOD = "pat"
ADMIN_SCOPE = "admin"
INSUFFICIENT_SCOPE = "Insufficient token scope"

#: Route decisions that are not a scope. ``ANY`` admits a PAT holding at least
#: one scope (identity introspection); ``REFUSED`` admits no PAT; ``UNMAPPED``
#: is the deny-by-default answer for a route no rule covers.
ANY = "any"
REFUSED = "refused"
UNMAPPED = "unmapped"

_READ_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


@dataclass(frozen=True)
class _Family:
    """A read scope for safe methods and a write scope for the rest."""

    read: str
    write: str


_JOBS = _Family("jobs:read", "jobs:write")
_CHAT = _Family("chat:read", "chat:write")
_KNOWLEDGE = _Family("knowledge:read", "knowledge:write")
# POST-shaped reads: a search or bulk export changes nothing.
_KNOWLEDGE_READ = _Family("knowledge:read", "knowledge:read")
# What a job is created against (projects, experts, skills, models,
# datasources, manifests): readable by a job reader, changed only as the
# design's "user/project admin, datasource admin".
_CATALOG = _Family("jobs:read", ADMIN_SCOPE)
_ADMIN = _Family(ADMIN_SCOPE, ADMIN_SCOPE)
_ANY = _Family(ANY, ANY)
_REFUSED = _Family(REFUSED, REFUSED)


@dataclass(frozen=True)
class _Rule:
    path: str
    policy: _Family
    exact: bool = False
    methods: frozenset[str] | None = None

    def matches(self, method: str, path: str) -> bool:
        if self.methods is not None and method not in self.methods:
            return False
        if self.exact:
            return path == self.path
        return path == self.path or path.startswith(self.path + "/")


def _exact(path: str, policy: _Family, *methods: str) -> _Rule:
    return _Rule(path, policy, exact=True, methods=frozenset(methods) or None)


def _prefix(path: str, policy: _Family, *methods: str) -> _Rule:
    return _Rule(path, policy, methods=frozenset(methods) or None)


_JOB = "/api/jobs/{job_id}"
_PROJECT = "/api/projects/{project_id}"
_THREAD = "/api/agents/threads/{thread_id}"

# Agent/runtime callbacks that share a user-facing prefix. They are gated on
# the internal key before any user is resolved, so a PAT never reaches them;
# listing them keeps the table honest should one ever grow a user fallback.
_INTERNAL_ROUTES = (
    f"{_JOB}/agent-release",
    f"{_JOB}/brief",
    f"{_JOB}/complete",
    f"{_JOB}/completion-decision",
    f"{_JOB}/guidance/ack",
    f"{_JOB}/loop-plan",
    f"{_JOB}/messages/send",
    f"{_JOB}/messages/{{thread_id}}/officer-ack",
    f"{_JOB}/messages/{{thread_id}}/officer-escalate",
    f"{_JOB}/messages/{{thread_id}}/officer-reply",
    f"{_JOB}/provision-workspace",
    f"{_JOB}/subjob-merge",
    f"{_JOB}/workspace-status",
    "/api/jobs/{target_job_id}/verification/rounds",
    f"{_PROJECT}/knowledge/delete",
    f"{_PROJECT}/knowledge/materialize",
    "/api/citations/snapshot",
    "/api/contacts/internal/list",
)

# First match wins: exact and narrow rules precede the prefix they refine.
_RULES: tuple[_Rule, ...] = (
    # -- PATs never ---------------------------------------------------------
    *(_prefix(path, _REFUSED) for path in _INTERNAL_ROUTES),
    _prefix("/api/internal", _REFUSED),
    _prefix("/api/runtime-actors", _REFUSED),
    # Credentials that authenticate to SRW: a PAT must not mint its successor.
    _prefix("/api/api-keys", _REFUSED),
    _prefix("/api/mcp-tokens", _REFUSED),
    _prefix("/api/ssh-keys", _REFUSED),
    _exact("/api/ssh/host-keys", _ANY),  # public host-key pinning
    _prefix("/api/ssh", _REFUSED),
    # Browser-only: code-server proxy, BFF cookie flow, WOPI office tokens.
    _prefix("/api/ide", _REFUSED),
    _exact("/auth/me", _ANY),
    _prefix("/auth", _REFUSED),
    _prefix("/wopi", _REFUSED),
    # -- Identity and health ----------------------------------------------
    _exact("/api/auth/me", _ANY),
    _exact("/api/health", _ANY),
    _exact("/api/system/readiness", _ANY),
    # -- Admin-only routes outside /api/admin ------------------------------
    _prefix("/api/admin", _ADMIN),
    _exact(f"{_JOB}/assign/{{agent_id}}", _ADMIN),
    _exact("/api/agents", _ADMIN),
    _exact("/api/agents/{agent_id}", _ADMIN),
    _exact("/api/agents/{agent_id}/system-info", _ADMIN),
    _exact("/api/vms", _ADMIN, "GET"),
    _exact("/api/stats/agents", _ADMIN),
    _exact("/api/stats/session-wakes", _ADMIN),
    _exact("/api/experts/reload", _ADMIN),
    _exact("/api/models/reload", _ADMIN),
    _exact("/api/skills/reload", _ADMIN),
    _prefix("/api/sudo/rules", _ADMIN),
    # Sudo decisions are the design's first `admin` grant.
    _prefix("/api/sudo/requests/{request_id}", _ADMIN, "POST"),
    # -- Knowledge ----------------------------------------------------------
    _exact(f"{_PROJECT}/knowledge/search", _KNOWLEDGE_READ),
    _exact(f"{_PROJECT}/knowledge/export", _KNOWLEDGE_READ),
    _prefix(f"{_PROJECT}/knowledge", _KNOWLEDGE),
    _prefix(f"{_PROJECT}/memory", _KNOWLEDGE),
    _prefix(f"{_JOB}/citations", _KNOWLEDGE),
    _prefix(f"{_JOB}/sources", _KNOWLEDGE),
    _prefix(f"{_JOB}/memories", _KNOWLEDGE),
    _prefix(f"{_JOB}/memory", _KNOWLEDGE),
    _prefix("/api/citations", _KNOWLEDGE),
    _prefix("/api/sources", _KNOWLEDGE),
    _prefix("/api/graph", _KNOWLEDGE),
    # -- Chat (persistent threads) -----------------------------------------
    _prefix("/api/persistent", _CHAT),
    _prefix("/api/sessions", _CHAT),
    _prefix(f"{_THREAD}/cloud-diff", _CHAT),
    _exact(f"{_THREAD}/rewind", _CHAT),
    _prefix("/api/voice", _CHAT),
    # Everything else under /api/agents is the agent runtime's.
    _prefix("/api/agents", _REFUSED),
    # -- Jobs ---------------------------------------------------------------
    _prefix("/api/jobs", _JOBS),
    _prefix(f"{_PROJECT}/jobs", _JOBS),
    _prefix(f"{_PROJECT}/job-records", _JOBS),
    _prefix(f"{_PROJECT}/backlog", _JOBS),
    _prefix(f"{_PROJECT}/loop", _JOBS),
    _prefix("/api/automations", _JOBS),
    _prefix("/api/bench", _JOBS),
    _prefix("/api/uploads", _JOBS),
    _prefix("/api/sudo", _JOBS),
    _prefix("/api/vms", _JOBS),
    _prefix("/api/me/active-jobs", _JOBS),
    _prefix("/api/actions", _JOBS),
    _prefix("/api/stats", _JOBS),
    _prefix("/api/usage", _JOBS),
    _prefix("/api/requests", _JOBS),
    _prefix("/api/officers", _JOBS),
    # -- Catalog: read as a job reader, change as admin ---------------------
    _prefix(f"{_PROJECT}/api-keys", _ADMIN),
    _prefix(f"{_PROJECT}/contacts", _ADMIN),
    _prefix("/api/projects", _CATALOG),
    _prefix("/api/datasources", _CATALOG),
    _prefix("/api/experts", _CATALOG),
    _prefix("/api/expert-defaults", _CATALOG),
    _prefix("/api/skills", _CATALOG),
    _prefix("/api/models", _CATALOG),
    _prefix("/api/manifests", _CATALOG),
    _prefix("/api/resources", _CATALOG),
    _prefix("/api/resource-secrets", _CATALOG),
    _prefix("/api/workspace-cache", _CATALOG),
    _prefix("/api/workspace-instances", _CATALOG),
    # -- Account and operator surfaces the vocabulary does not name ---------
    _prefix("/api/users", _ADMIN),
    _prefix("/api/settings", _ADMIN),
    _prefix("/api/contacts", _ADMIN),
    _prefix("/api/notifications", _ADMIN),
    _prefix("/api/media", _ADMIN),
    _prefix("/api/codex", _ADMIN),
    _prefix("/api/subscriptions", _ADMIN),
    _prefix("/api/tables", _ADMIN),
    _prefix("/api/snapshots", _ADMIN),
    _prefix("/api/workspace", _ADMIN),
)


def classify_route(method: str, path: str) -> str:
    """The scope a PAT needs for ``method path`` (a route *template*).

    Returns one scope from ``VALID_PAT_SCOPES``, or :data:`ANY`,
    :data:`REFUSED`, or :data:`UNMAPPED` (deny-by-default). ``method`` is an
    HTTP method, or ``WS`` for a WebSocket handshake — those authenticate by
    cookie only, so a PAT never has business there.
    """
    method = method.upper()
    if method == "WS":
        return REFUSED
    for rule in _RULES:
        if rule.matches(method, path):
            family = rule.policy
            return family.read if method in _READ_METHODS else family.write
    return UNMAPPED


def require_scopes(user: dict[str, Any], *needed: str) -> None:
    """Raise 403 unless a PAT caller holds every scope in ``needed``.

    A no-op for every credential other than a PAT: cookie/OIDC sessions and
    legacy MCP tokens are governed by role and row visibility, not by action
    scopes. ``admin`` satisfies any requirement. A PAT with no scopes is
    refused even when nothing is needed — an empty list must not read as
    "unrestricted".
    """
    if user.get("auth_method") != PAT_AUTH_METHOD:
        return
    held = {scope for scope in user.get("scopes") or [] if isinstance(scope, str)}
    if not held:
        raise HTTPException(status_code=403, detail=f"{INSUFFICIENT_SCOPE}: none")
    if ADMIN_SCOPE in held:
        return
    missing = sorted(set(needed) - held)
    if missing:
        raise HTTPException(
            status_code=403,
            detail=f"{INSUFFICIENT_SCOPE}: requires {', '.join(missing)}",
        )


def route_identity(request: Any) -> tuple[str, str] | None:
    """``(method, template)`` of the route FastAPI matched for ``request``.

    FastAPI releases that keep included routers intact (the orchestrator lock
    pins one) point ``scope["route"]`` at the router-local route and carry
    the include-prefixed template on the effective route context; older ones
    copy the route with its full path. Either way the full template wins, and
    a context that does not belong to the matched route is ignored.
    """
    scope = getattr(request, "scope", None)
    if not isinstance(scope, dict):
        return None
    route = scope.get("route")
    fastapi_scope = scope.get("fastapi")
    context = (
        fastapi_scope.get("effective_route_context")
        if isinstance(fastapi_scope, dict)
        else None
    )
    if context is not None and getattr(context, "original_route", None) is route:
        path = getattr(context, "path", None)
    else:
        path = getattr(route, "path", None)
    if not isinstance(path, str):
        return None
    if scope.get("type") == "websocket":
        return "WS", path
    method = scope.get("method")
    return (method, path) if isinstance(method, str) else None


def enforce_route_scopes(request: Any, user: dict[str, Any]) -> str:
    """Refuse a PAT the matched route's policy does not admit.

    Returns the route's decision for audit. Raises 403 for an unmatched or
    refused route and for missing scopes. Callers only reach this for PATs.
    """
    identity = route_identity(request)
    decision = classify_route(*identity) if identity else UNMAPPED
    if decision == ANY:
        require_scopes(user)
    elif decision in VALID_PAT_SCOPES:
        require_scopes(user, decision)
    else:
        raise HTTPException(
            status_code=403,
            detail=f"{INSUFFICIENT_SCOPE}: personal access tokens cannot use "
            "this endpoint",
        )
    return decision


__all__ = [
    "ADMIN_SCOPE",
    "ANY",
    "INSUFFICIENT_SCOPE",
    "PAT_AUTH_METHOD",
    "REFUSED",
    "UNMAPPED",
    "classify_route",
    "enforce_route_scopes",
    "require_scopes",
    "route_identity",
]
