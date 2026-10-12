"""The PAT route-scope table covers every mounted route, and says what it means.

``security.token_scopes`` refuses a personal access token on any route no rule
covers. That default is only safe if nothing real falls into it by accident,
so this suite walks the same mounted-route inventory the endpoint snapshot uses
(``scripts/check_endpoint_auth.py``) and pins three agreements:

* every route has a decision — a new route fails here until someone decides
  which scope it needs (``token_scopes._RULES``), instead of silently refusing
  every PAT;
* every admin-gated route needs ``admin``, and every internal-only route
  refuses PATs, so the table never advertises less than the gate enforces;
* the vocabulary the table uses is exactly the one tokens are minted with and
  the Cockpit offers;
* a GET that writes or mints is gated as its write (``READS_THAT_WRITE``) or
  acknowledged here as bookkeeping — a new GET handler that visibly calls a
  write- or mint-shaped function fails until someone decides which.

Behaviour through real requests: ``tests/test_pat_scope_enforcement.py``.
"""

from __future__ import annotations

import ast
import importlib.machinery
import importlib.util
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from orchestrator.schemas.tokens import VALID_PAT_SCOPES
from orchestrator.security.token_scopes import (
    ANY,
    READS_THAT_WRITE,
    REFUSED,
    UNMAPPED,
    classify_route,
    enforce_route_scopes,
    holds_scopes,
    require_scopes,
    tethers_session,
)
from tests.test_endpoint_inventory import _load_script

REPO_ROOT = Path(__file__).resolve().parent.parent
API_KEYS_PAGE = (
    REPO_ROOT / "cockpit/src/app/views/settings/api-keys/api-keys-page.component.ts"
)

# Gates that authenticate a service, never a user: a PAT cannot get past them,
# and the table must not suggest otherwise. ``is_internal_call`` is excluded —
# those routes fall back to user auth when the internal key is absent.
_SERVICE_ONLY_GATES = (
    "internal:require_internal",
    "internal:require_vm_guest",
    "internal:require_vm_cleanup_authority",
    "internal:require_vm_inventory_authority",
    "internal:_dispatch_infrastructure_ingestion",
)


@pytest.fixture(scope="module")
def endpoints():
    return _load_script().collect_endpoints()


def _render(endpoint) -> str:
    return f"{endpoint.method} {endpoint.path} ({endpoint.classification})"


class TestEveryMountedRouteIsClassified:
    def test_no_route_falls_to_the_deny_by_default(self, endpoints):
        unmapped = [
            _render(e)
            for e in endpoints
            if classify_route(e.method, e.path) == UNMAPPED
        ]
        assert not unmapped, (
            "Route(s) with no PAT scope decision — personal access tokens are "
            "refused there. Add a rule to orchestrator/security/token_scopes.py "
            "_RULES, then `python scripts/check_endpoint_auth.py --write`:\n  "
            + "\n  ".join(unmapped)
        )

    def test_admin_gated_routes_need_the_admin_scope(self, endpoints):
        weaker = [
            f"{_render(e)} -> {classify_route(e.method, e.path)}"
            for e in endpoints
            if e.classification.startswith("admin:")
            and classify_route(e.method, e.path) != "admin"
        ]
        assert not weaker, "Admin-only route(s) not mapped to `admin`:\n  " + (
            "\n  ".join(weaker)
        )

    def test_service_only_routes_refuse_pats(self, endpoints):
        admitted = [
            f"{_render(e)} -> {classify_route(e.method, e.path)}"
            for e in endpoints
            if e.classification.startswith(_SERVICE_ONLY_GATES)
            and classify_route(e.method, e.path) != REFUSED
        ]
        assert not admitted, (
            "Internal-only route(s) admit PATs; add them to "
            "token_scopes._INTERNAL_ROUTES:\n  " + "\n  ".join(admitted)
        )

    def test_decisions_use_exactly_the_minted_vocabulary(self, endpoints):
        decisions = {classify_route(e.method, e.path) for e in endpoints}
        assert decisions - {ANY, REFUSED} == VALID_PAT_SCOPES


# GET routes a `:read` token keeps although serving them writes. Every GET a
# `:read` scope reaches was traced into its services (2026-09-22); these are
# the ones that write, and each writes only bookkeeping or server-side repair
# the caller cannot steer. A GET that mints a credential, or performs a write
# whose content the caller chooses, belongs in token_scopes.READS_THAT_WRITE.
_BOOKKEEPING_READS = {
    "/api/persistent/threads/{thread_id}": (
        "ensure_thread_ssh_handle backfills the thread's opaque SSH routing "
        "handle; connecting still needs a registered key and an attach token"
    ),
    "/api/persistent/threads/{thread_id}/stream": (
        "records viewer presence on the stateless lane — which holds permission "
        "prompts (and the turn's executor slot) open, blocks the natural pause "
        "and flips awaiting_user to active — but only for a caller who could "
        "answer (token_scopes.tethers_session: chat:write or a non-PAT); a "
        "read-only token streams without tethering"
    ),
    "/api/persistent/threads/{thread_id}/canvases/main": (
        "re-pins an unchanged canvas to the current workspace generation"
    ),
    "/api/persistent/threads/{thread_id}/canvases/main/awareness/stream": (
        "sweeps expired editor-awareness rows"
    ),
    "/api/persistent/threads/{thread_id}/canvases/main/content": (
        "purges a stored snapshot whose bytes fail their hash"
    ),
    "/api/projects/{project_id}": (
        "kicks the hourly, service-authored cloud/Keycloak reconciliation any "
        "project view triggers; the caller supplies no content"
    ),
    "/api/projects/{project_id}/officer": (
        "materialises the vacant default officer row (INSERT … ON CONFLICT DO NOTHING)"
    ),
    "/api/citations/{citation_id}/drift": (
        "caches the caller's own cloud account id on a lookup miss"
    ),
    "/api/resources": (
        "audits each stored resource it skips as invisible (security_events)"
    ),
}

# Call names that, made directly from a GET handler, look like a write or a
# mint. Deliberately broad: a false hit costs one line above.
_WRITE_VERBS = re.compile(
    r"^(create|update|delete|mint|sign|insert|ensure|start|stop|cancel|revoke|"
    r"approve|deny|set|write|put|post|patch|record|grant|issue|provision|spawn|"
    r"dispatch|remove|upsert|rotate|attach|detach|enqueue|submit)(_|$)"
)


def _safe_method_handler_verbs():
    """``(route, {verb calls})`` for every mounted GET/HEAD handler."""
    script = _load_script()
    functions: dict[Path, dict[int, ast.AST]] = {}
    for route in script.discover_routes():
        if not script.in_inventory_scope(route):
            continue
        if route.method.upper() not in {"GET", "HEAD"}:
            continue
        if route.source_path not in functions:
            tree = ast.parse(route.source_path.read_text())
            functions[route.source_path] = {
                node.lineno: node
                for node in ast.walk(tree)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
        handler = functions[route.source_path][route.function_lineno]
        verbs = set()
        for node in ast.walk(handler):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = getattr(func, "attr", None) or getattr(func, "id", None)
            if name and _WRITE_VERBS.match(name):
                verbs.add(name)
        yield route, verbs


class TestReadsThatWrite:
    """A read method is not proof of a read: a GET that mints a credential or
    writes would hand a `:read` token write power."""

    def test_each_listed_read_is_gated_as_its_write(self, endpoints):
        mounted = {(e.method, e.path) for e in endpoints}
        for path, scope in READS_THAT_WRITE.items():
            assert ("GET", path) in mounted, f"stale entry: {path}"
            assert scope in VALID_PAT_SCOPES and not scope.endswith(":read")
            assert classify_route("GET", path) == scope
            assert classify_route("HEAD", path) == scope

    def test_bookkeeping_reads_are_still_mounted_reads(self, endpoints):
        mounted = {(e.method, e.path) for e in endpoints}
        for path in _BOOKKEEPING_READS:
            assert ("GET", path) in mounted, f"stale entry: {path}"
            assert classify_route("GET", path).endswith(":read")
            assert path not in READS_THAT_WRITE

    def test_a_get_that_visibly_writes_is_classified_consciously(self):
        unexplained = [
            f"{route.method} {route.path}: {sorted(verbs)}"
            for route, verbs in _safe_method_handler_verbs()
            if verbs
            and (
                classify_route(route.method, route.path).endswith(":read")
                or classify_route(route.method, route.path) == ANY
            )
            and route.path not in READS_THAT_WRITE
            and route.path not in _BOOKKEEPING_READS
        ]
        assert not unexplained, (
            "GET handler(s) that call a write/mint-shaped function while a "
            "`:read` token may call them. Gate the route as its write in "
            "token_scopes.READS_THAT_WRITE, or — if the write is bookkeeping "
            "the caller cannot steer — acknowledge it in _BOOKKEEPING_READS "
            "here:\n  " + "\n  ".join(unexplained)
        )


class TestClassifyRoute:
    @pytest.mark.parametrize(
        ("method", "path", "expected"),
        [
            ("GET", "/api/jobs/{job_id}", "jobs:read"),
            ("HEAD", "/api/jobs/{job_id}", "jobs:read"),
            ("OPTIONS", "/api/jobs/{job_id}", "jobs:read"),
            ("DELETE", "/api/jobs/{job_id}", "jobs:write"),
            # Segment boundaries: `/complete` is the agent callback, the
            # report beside it is an ordinary job read.
            ("POST", "/api/jobs/{job_id}/complete", REFUSED),
            ("GET", "/api/jobs/{job_id}/completion-report", "jobs:read"),
            ("GET", "/api/jobsearch", UNMAPPED),
            ("GET", "/api/workspace-cache", "jobs:read"),
            ("GET", "/api/workspace/status", "admin"),
            # POST-shaped reads stay reads.
            ("POST", "/api/projects/{project_id}/knowledge/search", "knowledge:read"),
            ("POST", "/api/projects/{project_id}/knowledge/reindex", "knowledge:write"),
            ("GET", "/api/projects/{project_id}", "jobs:read"),
            ("PATCH", "/api/projects/{project_id}", "admin"),
            ("POST", "/api/sudo/requests/{request_id}/approve", "admin"),
            ("GET", "/api/sudo/requests/{request_id}", "jobs:read"),
            ("WS", "/api/persistent/threads/{thread_id}/browser/stream", REFUSED),
            ("GET", "/api/persistent/threads/{thread_id}/stream", "chat:read"),
            ("POST", "/api/api-keys", REFUSED),
            ("GET", "/api/auth/me", ANY),
            ("get", "/api/jobs", "jobs:read"),
        ],
    )
    def test_decision(self, method, path, expected):
        assert classify_route(method, path) == expected


@pytest.mark.parametrize(
    ("user", "tethers"),
    [
        ({"id": "cookie-or-oidc"}, True),
        ({"auth_method": "mcp", "scopes": ["project:x"]}, True),
        ({"auth_method": "pat", "scopes": ["chat:write"]}, True),
        ({"auth_method": "pat", "scopes": ["admin"]}, True),
        ({"auth_method": "pat", "scopes": ["chat:read", "jobs:write"]}, False),
        ({"auth_method": "pat", "scopes": []}, False),
    ],
)
def test_only_a_caller_who_could_answer_tethers_a_session(user, tethers):
    assert tethers_session(user) is tethers
    assert holds_scopes(user, "chat:write") is tethers


class TestRequireScopes:
    @pytest.mark.parametrize(
        "user",
        [
            {"id": "cookie-or-oidc", "is_admin": False},
            {"auth_method": "mcp", "scopes": ["user"]},
            {"auth_method": "mcp", "scopes": []},
            # A forwarded X-MCP-Scope is an MCP credential, not a PAT.
            {"auth_method": "mcp", "scopes": ["jobs:read"]},
            {"auth_method": "runtime_actor", "scopes": []},
        ],
    )
    def test_non_pat_callers_are_never_checked(self, user):
        require_scopes(user, "admin")

    def test_admin_satisfies_everything(self):
        require_scopes({"auth_method": "pat", "scopes": ["admin"]}, "jobs:write")

    def test_write_does_not_imply_read(self):
        with pytest.raises(HTTPException) as exc:
            require_scopes(
                {"auth_method": "pat", "scopes": ["jobs:write"]}, "jobs:read"
            )
        assert exc.value.status_code == 403

    @pytest.mark.parametrize("scopes", [[], None, [None], ["user"]])
    def test_a_pat_with_no_usable_scope_is_refused(self, scopes):
        with pytest.raises(HTTPException) as exc:
            require_scopes({"auth_method": "pat", "scopes": scopes}, "jobs:read")
        assert exc.value.status_code == 403


class TestEnforceRouteScopes:
    PAT = {"auth_method": "pat", "scopes": ["admin"]}

    @pytest.mark.parametrize(
        "request_",
        [
            SimpleNamespace(),
            SimpleNamespace(scope={"type": "http", "method": "GET"}),
            SimpleNamespace(
                scope={"type": "http", "route": SimpleNamespace(path="/api/jobs")}
            ),
        ],
    )
    def test_an_unidentifiable_route_refuses(self, request_):
        with pytest.raises(HTTPException) as exc:
            enforce_route_scopes(request_, self.PAT)
        assert exc.value.status_code == 403

    def test_the_effective_template_wins_over_a_router_local_path(self):
        # FastAPI releases that keep included routers intact report the
        # router-local path on scope["route"].
        route = SimpleNamespace(path="/{project_id}")
        request = SimpleNamespace(
            scope={
                "type": "http",
                "method": "PATCH",
                "route": route,
                "fastapi": {
                    "effective_route_context": SimpleNamespace(
                        original_route=route, path="/api/projects/{project_id}"
                    )
                },
            }
        )
        assert enforce_route_scopes(request, self.PAT) == "admin"

    def test_a_context_for_another_route_is_ignored(self):
        request = SimpleNamespace(
            scope={
                "type": "http",
                "method": "GET",
                "route": SimpleNamespace(path="/api/jobs"),
                "fastapi": {
                    "effective_route_context": SimpleNamespace(
                        original_route=object(), path="/api/admin/capacity"
                    )
                },
            }
        )
        assert enforce_route_scopes(request, self.PAT) == "jobs:read"


def test_cockpit_offers_exactly_the_enforced_vocabulary():
    source = API_KEYS_PAGE.read_text()
    block = re.search(r"const ALL_SCOPES[^=]*=\s*\[(.*?)\];", source, re.S)
    assert block, "ALL_SCOPES not found in the API keys page"
    offered = set(re.findall(r"value:\s*'([^']+)'", block.group(1)))
    assert offered == VALID_PAT_SCOPES
    assert re.search(r"\{[^}]*value:\s*'admin'[^}]*adminOnly:\s*true", block.group(1))


def test_the_ssh_attach_exchange_needs_chat_write_and_the_helper_says_so():
    """``scripts/srw-ssh-proxy`` exchanges a PAT at this route on every
    connection. It must be admitted for chat:write (ssh-access.md), stay
    refused for the key-management routes beside it, and the helper must turn
    the resolver's refusal into the scope it names — a contract between the
    403 detail format here and the parser there."""
    assert classify_route("POST", "/api/ssh/attach-token") == "chat:write"
    assert classify_route("POST", "/api/ssh-keys") == REFUSED
    assert classify_route("POST", "/api/ssh-keys/challenge") == REFUSED

    with pytest.raises(HTTPException) as exc:
        require_scopes({"auth_method": "pat", "scopes": ["chat:read"]}, "chat:write")

    loader = importlib.machinery.SourceFileLoader(
        "srw_ssh_proxy_contract", str(REPO_ROOT / "scripts" / "srw-ssh-proxy")
    )
    spec = importlib.util.spec_from_loader(loader.name, loader)
    proxy = importlib.util.module_from_spec(spec)
    loader.exec_module(proxy)
    reason = proxy._refusal_reason(json.dumps({"detail": exc.value.detail}).encode())
    assert reason.startswith("PAT lacks the chat:write scope")
