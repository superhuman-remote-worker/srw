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
  the Cockpit offers.

Behaviour through real requests: ``tests/test_pat_scope_enforcement.py``.
"""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from orchestrator.schemas.tokens import VALID_PAT_SCOPES
from orchestrator.security.token_scopes import (
    ANY,
    REFUSED,
    UNMAPPED,
    classify_route,
    enforce_route_scopes,
    require_scopes,
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
