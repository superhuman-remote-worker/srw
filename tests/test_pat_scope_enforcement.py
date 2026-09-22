"""PAT action scopes are enforced per route, end to end through FastAPI routing.

A personal access token (``Bearer ak_…``) used to carry its owner's full reach
whatever scopes it was minted with: the list was validated and stored, and no
route read it. These tests drive real ASGI requests through the real resolvers
(``security.auth``, ``security.access.require_admin``) on route templates the
orchestrator actually serves, so the route policy is exercised exactly as the
resolver sees it — including an include-prefixed router, whose template FastAPI
reports differently across releases.

Only the store is faked. The policy table itself is covered for every mounted
route by ``tests/test_pat_scope_policy.py``.
"""

from __future__ import annotations

import hashlib
from typing import Any
from uuid import uuid4

import httpx
import pytest
from fastapi import APIRouter, FastAPI, Request

from orchestrator.security import auth
from orchestrator.security.access import require_admin
from orchestrator.security.auth import get_current_user, require_approved_user

OWNER_ID = str(uuid4())
OWNER_SUB = "kc-owner"
INTERNAL_KEY = "test-internal-key"


def _owner_row() -> dict[str, Any]:
    # An admin owner: the admin flag must not leak into a token that was not
    # granted the ``admin`` scope.
    return {
        "id": OWNER_ID,
        "display_name": "owner",
        "preferred_username": "owner",
        "email": "owner@example.test",
        "keycloak_sub": OWNER_SUB,
        "is_admin": True,
        "is_approved": True,
    }


class _Store:
    """The handful of store methods the auth resolvers reach."""

    def __init__(self) -> None:
        self.tokens: dict[str, dict[str, Any]] = {}
        self.security_events: list[dict[str, Any]] = []

    def mint(self, kind: str, *, scopes=None, scope: str | None = None) -> str:
        token = ("ak_" if kind == "api" else "srw_") + uuid4().hex
        digest = hashlib.sha256(token.encode("ascii")).hexdigest()
        self.tokens[digest] = {
            "id": uuid4(),
            "user_id": OWNER_ID,
            "kind": kind,
            "scopes": scopes,
            "scope": scope,
        }
        return token

    async def get_auth_token_by_hash(self, digest: str):
        return self.tokens.get(digest)

    async def get_user(self, user_id: str):
        return _owner_row() if user_id == OWNER_ID else None

    async def get_user_by_keycloak_sub(self, sub: str):
        return _owner_row() if sub == OWNER_SUB else None

    async def touch_auth_token(self, token_id: str, ip) -> None:
        return None

    async def record_security_event(self, **event: Any) -> None:
        self.security_events.append(event)


def _build_app(store: _Store) -> FastAPI:
    app = FastAPI()

    async def _as_user(request: Request) -> dict[str, Any]:
        user = await require_approved_user(request, store)
        return {"is_admin": user["is_admin"], "real_is_admin": user["real_is_admin"]}

    @app.get("/api/jobs")
    async def list_jobs(request: Request):
        return await _as_user(request)

    @app.post("/api/jobs")
    async def create_job(request: Request):
        return await _as_user(request)

    @app.get("/api/persistent/threads")
    async def list_threads(request: Request):
        return await _as_user(request)

    @app.post("/api/persistent/threads")
    async def create_thread(request: Request):
        return await _as_user(request)

    @app.get("/api/projects/{project_id}/knowledge")
    async def list_notes(request: Request, project_id: str):
        return await _as_user(request)

    @app.patch("/api/projects/{project_id}/knowledge/{note_id}")
    async def update_note(request: Request, project_id: str, note_id: str):
        return await _as_user(request)

    @app.get("/api/admin/capacity")
    async def admin_capacity(request: Request):
        await require_admin(request, store)
        return {"ok": True}

    @app.post("/api/api-keys")
    async def mint_key(request: Request):
        return await _as_user(request)

    @app.get("/api/auth/me")
    async def whoami(request: Request):
        user = await get_current_user(request, store)
        return {"is_admin": user["is_admin"]}

    # Declared nowhere in the policy: stands in for a route added tomorrow.
    @app.get("/api/brand-new-surface")
    async def unclassified(request: Request):
        return await _as_user(request)

    # Include-prefixed: newer FastAPI hands the resolver the router-local
    # route ("/{job_id}") and keeps the prefixed template elsewhere.
    jobs = APIRouter()

    @jobs.get("/{job_id}")
    async def get_job(request: Request, job_id: str):
        return await _as_user(request)

    @jobs.post("/{job_id}/resume")
    async def resume_job(request: Request, job_id: str):
        return await _as_user(request)

    app.include_router(jobs, prefix="/api/jobs")
    return app


@pytest.fixture
def store() -> _Store:
    return _Store()


@pytest.fixture
def client(store):
    transport = httpx.ASGITransport(app=_build_app(store))
    return httpx.AsyncClient(transport=transport, base_url="http://test")


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


JOBS = "/api/jobs"
JOB = "/api/jobs/job-1"
RESUME = "/api/jobs/job-1/resume"
THREADS = "/api/persistent/threads"
NOTES = "/api/projects/p-1/knowledge"
NOTE = "/api/projects/p-1/knowledge/n-1"
ADMIN = "/api/admin/capacity"


class TestEachScopeReachesItsRoutesOnly:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("scope", "allowed", "neighbour"),
        [
            ("jobs:read", ("GET", JOBS), ("POST", JOBS)),
            ("jobs:read", ("GET", JOB), ("POST", RESUME)),
            ("jobs:write", ("POST", JOBS), ("POST", THREADS)),
            ("jobs:write", ("POST", RESUME), ("GET", JOB)),
            ("chat:read", ("GET", THREADS), ("POST", THREADS)),
            ("chat:write", ("POST", THREADS), ("GET", JOBS)),
            ("knowledge:read", ("GET", NOTES), ("PATCH", NOTE)),
            ("knowledge:write", ("PATCH", NOTE), ("GET", JOBS)),
            ("jobs:write", ("POST", JOBS), ("GET", ADMIN)),
        ],
    )
    async def test_allowed_route_passes_and_neighbour_is_refused(
        self, store, client, scope, allowed, neighbour
    ):
        token = store.mint("api", scopes=[scope])
        async with client:
            ok = await client.request(*allowed, headers=_bearer(token))
            refused = await client.request(*neighbour, headers=_bearer(token))
        assert ok.status_code == 200, ok.text
        assert refused.status_code == 403
        assert refused.json()["detail"].startswith("Insufficient token scope")

    @pytest.mark.asyncio
    async def test_admin_scope_is_a_superset(self, store, client):
        token = store.mint("api", scopes=["admin"])
        async with client:
            for method, path in [
                ("GET", ADMIN),
                ("POST", JOBS),
                ("GET", THREADS),
                ("PATCH", NOTE),
            ]:
                response = await client.request(method, path, headers=_bearer(token))
                assert response.status_code == 200, (method, path, response.text)

    @pytest.mark.asyncio
    async def test_scope_without_admin_drops_the_owners_admin_reach(
        self, store, client
    ):
        # The owner is an admin; a jobs:read token must not carry admin
        # visibility onto the routes it is allowed to call.
        token = store.mint("api", scopes=["jobs:read"])
        async with client:
            response = await client.get(JOBS, headers=_bearer(token))
            whoami = await client.get("/api/auth/me", headers=_bearer(token))
        assert response.json() == {"is_admin": False, "real_is_admin": False}
        assert whoami.json() == {"is_admin": False}

    @pytest.mark.asyncio
    async def test_denial_is_recorded_as_a_security_event(self, store, client):
        token = store.mint("api", scopes=["jobs:read"])
        async with client:
            await client.post(JOBS, headers=_bearer(token))
        assert [e["event_type"] for e in store.security_events] == [
            "token_scope_denied"
        ]
        assert store.security_events[0]["path"] == JOBS
        assert store.security_events[0]["auth_method"] == "pat"


class TestFailClosed:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("scopes", [[], None])
    async def test_empty_scope_pat_is_refused_everywhere(self, store, client, scopes):
        token = store.mint("api", scopes=scopes)
        async with client:
            for method, path in [("GET", JOBS), ("GET", "/api/auth/me")]:
                response = await client.request(method, path, headers=_bearer(token))
                assert response.status_code == 403, (method, path, response.text)

    @pytest.mark.asyncio
    async def test_a_malformed_scope_row_is_refused_not_a_500(self, store, client):
        # TEXT[] can hold NULLs; the denial (and its audit line) must survive.
        token = store.mint("api", scopes=[None, "chat:read"])
        async with client:
            response = await client.get(JOBS, headers=_bearer(token))
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_unclassified_route_refuses_even_an_admin_pat(self, store, client):
        token = store.mint("api", scopes=["admin"])
        async with client:
            response = await client.get(
                "/api/brand-new-surface", headers=_bearer(token)
            )
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_a_pat_cannot_mint_credentials(self, store, client):
        token = store.mint("api", scopes=sorted(["admin", "jobs:write"]))
        async with client:
            response = await client.post("/api/api-keys", headers=_bearer(token))
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_any_scoped_pat_may_read_its_identity(self, store, client):
        token = store.mint("api", scopes=["chat:read"])
        async with client:
            response = await client.get("/api/auth/me", headers=_bearer(token))
        assert response.status_code == 200


ALL_ROUTES = [
    ("GET", JOBS),
    ("POST", JOBS),
    ("GET", JOB),
    ("POST", RESUME),
    ("POST", THREADS),
    ("PATCH", NOTE),
    ("GET", ADMIN),
    ("POST", "/api/api-keys"),
    ("GET", "/api/brand-new-surface"),
]


class TestOtherCredentialsAreUnaffected:
    @pytest.mark.asyncio
    async def test_oidc_bearer_reaches_every_route(self, store, client, monkeypatch):
        monkeypatch.setattr(
            auth.oidc_validator,
            "validate_token",
            lambda token: {
                "sub": OWNER_SUB,
                "email": "owner@example.test",
                "preferred_username": "owner",
                "realm_access": {"roles": ["user", "admin"]},
            },
        )
        async with client:
            for method, path in ALL_ROUTES:
                response = await client.request(
                    method, path, headers=_bearer("header.payload.signature")
                )
                assert response.status_code == 200, (method, path, response.text)

    @pytest.mark.asyncio
    async def test_cookie_session_reaches_every_route(self, store, client, monkeypatch):
        async def _session_user(session_id, db):
            return _owner_row()

        monkeypatch.setattr(auth, "_resolve_from_cookie", _session_user)
        async with client:
            client.cookies.set(auth.SESSION_COOKIE, "session-1")
            for method, path in ALL_ROUTES:
                response = await client.request(method, path)
                assert response.status_code == 200, (method, path, response.text)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "scope", ["user", "all", "project:6d4c1f7e-2b8a-4d3e-9f10-5a6b7c8d9e0f"]
    )
    async def test_legacy_mcp_token_reaches_every_route(self, store, client, scope):
        token = store.mint("mcp", scope=scope)
        async with client:
            for method, path in ALL_ROUTES:
                response = await client.request(method, path, headers=_bearer(token))
                assert response.status_code == 200, (method, path, response.text)

    @pytest.mark.asyncio
    async def test_mcp_server_forwarded_headers_reach_every_route(
        self, store, client, monkeypatch
    ):
        monkeypatch.setenv("MCP_INTERNAL_KEY", INTERNAL_KEY)
        headers = {
            "X-MCP-User-Id": OWNER_ID,
            "X-Internal-Key": INTERNAL_KEY,
            "X-MCP-Scope": "user",
        }
        async with client:
            for method, path in ALL_ROUTES:
                response = await client.request(method, path, headers=headers)
                assert response.status_code == 200, (method, path, response.text)
