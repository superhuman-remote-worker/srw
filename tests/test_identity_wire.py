"""``GET /api/auth/me`` and the one public projection of an authenticated user.

The route is deliberately classified ``public:auth-bootstrap`` in
``policy/endpoint_inventory.txt``: it still requires a Bearer token (its gate
is ``get_current_user``, which 401s without one), but it intentionally answers
for a user who has NOT been approved yet, so the cockpit can render "pending
approval" instead of a blank screen. Both halves of that sentence are pinned
below — an approval check added here would break sign-up, and dropping the
identity check would make it genuinely public.

``user_dict`` is an allowlist, not a passthrough of the ``users`` row. The
row carries the Keycloak subject and other internal columns; a projection
that grew into ``dict(user)`` would publish them. That is the property the
last case guards.

Supersedes ``TestUserDictHelper`` in tests/test_user_management.py, which
re-implemented the projection inline (with the comment "we can't easily
import from main.py due to side effects") and therefore asserted nothing
about the shipped code.
"""

from datetime import datetime, timezone
from uuid import UUID

import httpx
import pytest
from fastapi import HTTPException

from orchestrator.routers import identity as identity_routes
from orchestrator.services import identity
from tests._mounted_router import mount_router

USER_ID = UUID("00000000-0000-0000-0000-000000000001")
PROJECT_ID = UUID("00000000-0000-0000-0000-0000000000aa")
CREATED = datetime(2026, 1, 1, tzinfo=timezone.utc)


def user_row(**over):
    row = {
        "id": USER_ID,
        "display_name": "Alice",
        "avatar_color": "#123456",
        "email": "alice@example.com",
        "default_project_id": PROJECT_ID,
        "is_admin": True,
        "is_approved": True,
        "can_use_vm": True,
        "created_at": CREATED,
        # Internal columns a passthrough projection would publish:
        "keycloak_sub": "kc-sub-not-for-the-wire",
        "mcp_token_hash": "hash-not-for-the-wire",
        "is_active": True,
    }
    row.update(over)
    return row


class Harness:
    def __init__(self):
        self.user = user_row()
        self.error: HTTPException | None = None

    async def get_current_user(self, request, db):
        if self.error is not None:
            raise self.error
        return self.user

    @property
    def dependencies(self):
        return identity_routes.IdentityDependencies(
            store=object(), get_current_user=self.get_current_user
        )

    def app(self):
        return mount_router(
            identity_routes.router,
            factories={"identity_dependencies_factory": lambda: self.dependencies},
        )


@pytest.fixture
def harness():
    return Harness()


# =============================================================================
# The projection
# =============================================================================


def test_user_dict_projects_exactly_the_public_fields():
    assert identity.user_dict(user_row()) == {
        "id": str(USER_ID),
        "display_name": "Alice",
        "avatar_color": "#123456",
        "email": "alice@example.com",
        "default_project_id": str(PROJECT_ID),
        "is_admin": True,
        "is_approved": True,
        "can_use_vm": True,
        "created_at": CREATED,
    }


def test_user_dict_never_publishes_internal_columns():
    """An allowlist, and it has to stay one: ``dict(user)`` would ship the
    Keycloak subject and the stored token hash to every cockpit session."""
    projected = identity.user_dict(user_row())
    assert "keycloak_sub" not in projected
    assert "mcp_token_hash" not in projected
    assert "is_active" not in projected
    assert "kc-sub-not-for-the-wire" not in str(projected)
    assert "hash-not-for-the-wire" not in str(projected)


@pytest.mark.parametrize("flag", ["is_admin", "is_approved", "can_use_vm"])
def test_missing_boolean_flags_default_to_false(flag):
    """A row that predates a column must read as "not granted", never as
    truthy-because-absent."""
    row = user_row()
    del row[flag]
    assert identity.user_dict(row)[flag] is False


def test_can_use_vm_is_coerced_to_a_real_bool():
    """The column is nullable; ``None`` must reach the wire as ``False``,
    not as JSON ``null`` a cockpit conditional would read as unknown."""
    assert identity.user_dict(user_row(can_use_vm=None))["can_use_vm"] is False


def test_absent_default_project_is_null_not_the_string_none():
    assert (
        identity.user_dict(user_row(default_project_id=None))["default_project_id"]
        is None
    )


def test_email_is_optional():
    row = user_row()
    del row["email"]
    assert identity.user_dict(row)["email"] is None


def test_ids_are_stringified_for_json():
    projected = identity.user_dict(user_row())
    assert isinstance(projected["id"], str)
    assert isinstance(projected["default_project_id"], str)


# =============================================================================
# The endpoint
# =============================================================================


@pytest.mark.asyncio
async def test_auth_me_wraps_the_projection(harness):
    result = await identity_routes.auth_me(
        request=object(), dependencies=harness.dependencies
    )
    assert result == {"user": identity.user_dict(harness.user)}


@pytest.mark.asyncio
async def test_auth_me_answers_for_an_unapproved_user(harness):
    """The reason this route is classified ``public:auth-bootstrap``: it must
    NOT gate on approval, or a newly registered user gets a blank cockpit
    instead of "pending approval"."""
    harness.user = user_row(is_approved=False, is_admin=False)
    result = await identity_routes.auth_me(
        request=object(), dependencies=harness.dependencies
    )
    assert result["user"]["is_approved"] is False


@pytest.mark.asyncio
async def test_auth_me_still_requires_an_identity(harness):
    """ "Public" here means "no approval check", not "no authentication":
    ``get_current_user``'s 401 must propagate untouched."""
    harness.error = HTTPException(status_code=401, detail="Not authenticated")
    with pytest.raises(HTTPException) as excinfo:
        await identity_routes.auth_me(
            request=object(), dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 401


@pytest.mark.asyncio
async def test_auth_me_over_http(harness):
    """Through real routing and this application's own factory — no lifespan,
    no database, no Keycloak."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=harness.app()), base_url="http://test"
    ) as client:
        response = await client.get("/api/auth/me")

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["user"]["id"] == str(USER_ID)
    # The application's encoder renders UTC as a trailing "Z", not "+00:00".
    assert body["user"]["created_at"].startswith("2026-01-01T00:00:00")
    assert "keycloak_sub" not in body["user"]
    assert "kc-sub-not-for-the-wire" not in response.text


@pytest.mark.asyncio
async def test_auth_me_over_http_propagates_the_401(harness):
    harness.error = HTTPException(status_code=401, detail="Not authenticated")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=harness.app()), base_url="http://test"
    ) as client:
        response = await client.get("/api/auth/me")

    assert response.status_code == 401


def test_auth_me_is_the_only_route_on_the_identity_router():
    """A domain router's surface is part of its contract: anything else
    arriving here would inherit this route's public classification by
    proximity in review, which is exactly how a gate goes missing."""
    assert {
        (route.path, tuple(sorted(route.methods)))
        for route in identity_routes.router.routes
    } == {("/api/auth/me", ("GET",))}


# =============================================================================
# A validated Bearer token without a usable subject
# =============================================================================


def _bearer_app(db):
    """A bare app whose one route runs the real ``get_current_user``."""
    from fastapi import FastAPI, Request

    from orchestrator.security.auth import get_current_user

    app = FastAPI()

    @app.get("/whoami")
    async def whoami(request: Request):
        user = await get_current_user(request, db)
        return {"id": str(user["id"])}

    return app


async def _get_whoami(db):
    # raise_app_exceptions=False turns an unhandled error into the 500 a
    # client would actually see, instead of re-raising it into the test.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_bearer_app(db), raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        return await client.get("/whoami", headers={"Authorization": "Bearer a.b.c"})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "claims",
    [
        {"email": "a@example.com"},
        {"sub": None},
        {"sub": ""},
        {"sub": "   "},
        {"sub": 123},
    ],
    ids=["absent", "null", "empty", "blank", "non-string"],
)
async def test_bearer_without_usable_subject_is_401_before_any_lookup(
    claims, monkeypatch
):
    from unittest.mock import AsyncMock, MagicMock

    from orchestrator.security.oidc import oidc_validator

    monkeypatch.setattr(oidc_validator, "validate_token", lambda token: claims)
    db = MagicMock()
    db.get_user_by_keycloak_sub = AsyncMock()

    response = await _get_whoami(db)

    assert response.status_code == 401, response.text
    db.get_user_by_keycloak_sub.assert_not_called()


@pytest.mark.asyncio
async def test_bearer_with_usable_subject_still_resolves(monkeypatch):
    from unittest.mock import AsyncMock, MagicMock

    from orchestrator.security.oidc import oidc_validator

    monkeypatch.setattr(
        oidc_validator,
        "validate_token",
        lambda token: {"sub": "kc-sub-1", "email": "alice@example.com"},
    )
    db = MagicMock()
    db.get_user_by_keycloak_sub = AsyncMock(
        return_value=user_row(
            keycloak_sub="kc-sub-1", display_name="alice", is_admin=False
        )
    )
    db.acquire = MagicMock()

    response = await _get_whoami(db)

    assert response.status_code == 200, response.text
    assert response.json() == {"id": str(USER_ID)}
    db.get_user_by_keycloak_sub.assert_awaited_once_with("kc-sub-1")
