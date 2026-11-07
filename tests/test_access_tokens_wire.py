"""MCP tokens and personal access tokens: scope policy, secrets, revocation.

Both credentials live in one ``auth_tokens`` table and share one validator
path, so they share one module — and one set of invariants worth stating
plainly, because every one of them is a way to hand out more access than the
caller asked for:

* **The plaintext appears exactly once, on the response that mints it.** Never
  on a list, never on a verify, and never on the internal create — that
  endpoint is handed a hash the OAuth bridge already minted and has never seen
  the secret at all. Each is asserted below, positively and negatively.
* **The stored value is a SHA-256 of the plaintext**, so a stolen database row
  is not a usable credential.
* **Scope escalation is gated on the real admin flag**, not on the effective
  one a view-as-user session carries: ``real_is_admin`` is the field both
  ``all``-scope MCP tokens and ``admin``-scope PATs check.
* **A project-scoped MCP token requires membership of that project**, checked
  against the authenticated caller, not asserted by the body.
* **Revocation and rotation are owner-scoped**: the store is always called
  with the authenticated user's id, and a miss is a 404, never a silent
  success.

These cases were first recorded against the original handlers in ``main`` and
reproduced byte-for-byte against ``orchestrator.routers.access_tokens`` before
this file was committed.
"""

import hashlib
from datetime import datetime, timezone
from uuid import UUID

import httpx
import pytest
from fastapi import HTTPException

from orchestrator.routers import access_tokens as access_token_routes
from orchestrator.schemas.tokens import (
    VALID_PAT_SCOPES,
    ApiKeyCreate,
    McpTokenCreate,
    McpTokenCreateInternal,
    McpTokenVerifyRequest,
)
from orchestrator.services import access_tokens as access_token_operations
from tests._mounted_router import mount_router

USER_ID = UUID("00000000-0000-0000-0000-000000000001")
OTHER_ID = UUID("00000000-0000-0000-0000-0000000000ff")
PROJECT_ID = "00000000-0000-0000-0000-0000000000bb"
CREATED = datetime(2026, 1, 1, tzinfo=timezone.utc)


class FakeStore:
    def __init__(self, **methods):
        for name, method in methods.items():
            setattr(self, name, method)

    def set(self, name, method):
        setattr(self, name, method)
        return self

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)

        async def _unexpected(*a, **k):
            raise AssertionError(f"unexpected store call: {name}")

        return _unexpected


class Harness:
    """One application's worth of token collaborators, resolved per call."""

    def __init__(self):
        self.store = FakeStore()
        self.user = {"id": USER_ID, "real_is_admin": False}
        self.internal_error: HTTPException | None = None
        self.service_gate_calls: list[str] = []

    async def require_approved_user(self, request, db):
        return self.user

    async def require_internal(self, request):
        if self.internal_error is not None:
            raise self.internal_error
        return None

    async def require_internal_service(self, request):
        self.service_gate_calls.append(request.url.path)
        if self.internal_error is not None:
            raise self.internal_error
        return None

    @property
    def dependencies(self):
        return access_token_routes.AccessTokenDependencies(
            store=self.store,
            tokens=access_token_operations.AccessTokenDependencies(store=self.store),
            require_approved_user=self.require_approved_user,
            require_internal=self.require_internal,
            require_internal_service=self.require_internal_service,
        )

    def app(self):
        return mount_router(
            access_token_routes.router,
            factories={"access_token_dependencies_factory": lambda: self.dependencies},
        )


@pytest.fixture
def harness():
    return Harness()


@pytest.fixture
def pat_token_vector(monkeypatch):
    """Synthetic known-answer vector; production must still request 256 bits.

    PATs are random bearer tokens, not human passwords. Keep an exact SHA-256
    assertion without treating a response field as password input to a hash.
    """
    requested_sizes = []

    def random_token(size):
        requested_sizes.append(size)
        return "A" * 43  # Base64url encoding of 32 zero bytes, without padding.

    monkeypatch.setattr(access_token_operations.secrets, "token_urlsafe", random_token)
    yield (
        "ak_" + "A" * 43,
        "8207331e1808af9d67b4814f60f30bd2e84fe7b51766fb6296296c732c489907",
    )
    assert requested_sizes == [32]


def _mcp_row(**over):
    row = {
        "id": UUID("00000000-0000-0000-0000-0000000000c1"),
        "name": "n",
        "scope": "user",
        "token_prefix": "srw_prefix1",
        "expires_at": None,
        "created_at": CREATED,
    }
    row.update(over)
    return row


def _pat_row(**over):
    row = {
        "id": UUID("00000000-0000-0000-0000-0000000000e1"),
        "name": "k",
        "scopes": ["jobs:read"],
        "token_prefix": "ak_prefix12",
        "last_four": "abcd",
        "expires_at": None,
        "created_at": CREATED,
    }
    row.update(over)
    return row


# =============================================================================
# MCP tokens — scope policy
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["nope", "project", "user:extra", ""])
async def test_an_unrecognised_scope_is_refused(harness, scope):
    with pytest.raises(HTTPException) as excinfo:
        await access_token_routes.create_mcp_token(
            request=object(),
            body=McpTokenCreate(name="n", scope=scope),
            dependencies=harness.dependencies,
        )
    assert excinfo.value.status_code == 400
    assert (
        excinfo.value.detail == "Invalid scope. Use 'user', 'all', or 'project:<uuid>'"
    )


@pytest.mark.asyncio
async def test_full_access_scope_needs_the_real_admin_flag(harness):
    """``real_is_admin``, not ``is_admin``: an admin using View-as-User
    carries a demoted ``is_admin`` and must not be able to mint a token that
    outlives the impersonation with full access.
    """
    harness.user = {"id": USER_ID, "is_admin": True, "real_is_admin": False}
    with pytest.raises(HTTPException) as excinfo:
        await access_token_routes.create_mcp_token(
            request=object(),
            body=McpTokenCreate(name="n", scope="all"),
            dependencies=harness.dependencies,
        )
    assert excinfo.value.status_code == 403
    assert excinfo.value.detail == "Only admins can create full-access tokens"


@pytest.mark.asyncio
async def test_a_real_admin_may_mint_a_full_access_token(harness):
    harness.user = {"id": USER_ID, "real_is_admin": True}
    recorded = {}

    async def _create(**kwargs):
        recorded.update(kwargs)
        return _mcp_row(scope=kwargs["scope"])

    harness.store.set("create_mcp_token", _create)
    result = await access_token_routes.create_mcp_token(
        request=object(),
        body=McpTokenCreate(name="n", scope="all"),
        dependencies=harness.dependencies,
    )
    assert recorded["scope"] == "all"
    assert result["token"].startswith("srw_")


@pytest.mark.asyncio
async def test_a_project_scope_requires_membership_of_that_project(harness):
    asked = []

    async def _members(project_id):
        asked.append(project_id)
        return [{"user_id": OTHER_ID}]

    harness.store.set("get_project_members", _members)
    with pytest.raises(HTTPException) as excinfo:
        await access_token_routes.create_mcp_token(
            request=object(),
            body=McpTokenCreate(name="n", scope=f"project:{PROJECT_ID}"),
            dependencies=harness.dependencies,
        )
    assert excinfo.value.status_code == 403
    assert excinfo.value.detail == "Not a member of this project"
    # The project id comes off the requested scope, not off the body's name.
    assert asked == [PROJECT_ID]


@pytest.mark.asyncio
async def test_a_member_may_mint_a_project_scoped_token(harness):
    async def _members(project_id):
        return [{"user_id": OTHER_ID}, {"user_id": USER_ID}]

    recorded = {}

    async def _create(**kwargs):
        recorded.update(kwargs)
        return _mcp_row(scope=kwargs["scope"])

    harness.store.set("get_project_members", _members)
    harness.store.set("create_mcp_token", _create)
    result = await access_token_routes.create_mcp_token(
        request=object(),
        body=McpTokenCreate(name="n", scope=f"project:{PROJECT_ID}"),
        dependencies=harness.dependencies,
    )
    assert recorded["scope"] == f"project:{PROJECT_ID}"
    assert recorded["user_id"] == str(USER_ID)
    assert result["scope"] == f"project:{PROJECT_ID}"


@pytest.mark.asyncio
async def test_the_scope_is_stripped_before_it_is_validated_and_stored(harness):
    """Whitespace around a scope must not be able to smuggle an unvalidated
    value into the stored row: the same stripped string is what gets checked
    and what gets persisted."""
    recorded = {}

    async def _create(**kwargs):
        recorded.update(kwargs)
        return _mcp_row(scope=kwargs["scope"])

    harness.store.set("create_mcp_token", _create)
    await access_token_routes.create_mcp_token(
        request=object(),
        body=McpTokenCreate(name="n", scope="  user  "),
        dependencies=harness.dependencies,
    )
    assert recorded["scope"] == "user"


# =============================================================================
# MCP tokens — the secret
# =============================================================================


@pytest.mark.asyncio
async def test_creation_returns_the_plaintext_once_and_stores_only_its_hash(harness):
    recorded = {}

    async def _create(**kwargs):
        recorded.update(kwargs)
        return _mcp_row(token_prefix=kwargs["token_prefix"])

    harness.store.set("create_mcp_token", _create)
    result = await access_token_routes.create_mcp_token(
        request=object(),
        body=McpTokenCreate(name="n", scope="user"),
        dependencies=harness.dependencies,
    )

    token = result["token"]
    assert token.startswith("srw_")
    assert len(token) > 32
    # What is stored is a digest of what was returned, never the token itself.
    assert recorded["token_hash"] == hashlib.sha256(token.encode()).hexdigest()
    assert recorded["token_hash"] != token
    assert recorded["token_prefix"] == token[:12]
    # ...and the hash never travels back out on the response.
    assert "token_hash" not in result


@pytest.mark.asyncio
async def test_two_tokens_are_never_the_same(harness):
    async def _create(**kwargs):
        return _mcp_row()

    harness.store.set("create_mcp_token", _create)
    body = McpTokenCreate(name="n", scope="user")
    first = await access_token_routes.create_mcp_token(
        request=object(), body=body, dependencies=harness.dependencies
    )
    second = await access_token_routes.create_mcp_token(
        request=object(), body=body, dependencies=harness.dependencies
    )
    assert first["token"] != second["token"]


@pytest.mark.asyncio
async def test_an_expiry_in_days_becomes_a_future_timestamp(harness):
    recorded = {}

    async def _create(**kwargs):
        recorded.update(kwargs)
        return _mcp_row(expires_at=kwargs["expires_at"])

    harness.store.set("create_mcp_token", _create)
    await access_token_routes.create_mcp_token(
        request=object(),
        body=McpTokenCreate(name="n", scope="user", expires_in_days=7),
        dependencies=harness.dependencies,
    )
    assert recorded["expires_at"] > datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_no_expiry_means_no_expiry(harness):
    recorded = {}

    async def _create(**kwargs):
        recorded.update(kwargs)
        return _mcp_row()

    harness.store.set("create_mcp_token", _create)
    await access_token_routes.create_mcp_token(
        request=object(),
        body=McpTokenCreate(name="n", scope="user"),
        dependencies=harness.dependencies,
    )
    assert recorded["expires_at"] is None


@pytest.mark.asyncio
async def test_listing_carries_no_secret_and_is_owner_scoped(harness):
    asked = []

    async def _list(user_id):
        asked.append(user_id)
        return [_mcp_row()]

    harness.store.set("list_mcp_tokens", _list)
    rows = await access_token_routes.list_mcp_tokens(
        request=object(), dependencies=harness.dependencies
    )
    assert asked == [str(USER_ID)]
    assert rows == [
        {
            "id": "00000000-0000-0000-0000-0000000000c1",
            "name": "n",
            "scope": "user",
            "token_prefix": "srw_prefix1",
            "expires_at": None,
            "created_at": str(CREATED),
        }
    ]
    assert "token" not in rows[0]


# =============================================================================
# MCP tokens — revocation
# =============================================================================


@pytest.mark.asyncio
async def test_revoking_someone_elses_token_is_a_404_not_a_success(harness):
    asked = []

    async def _revoke(token_id, user_id):
        asked.append((token_id, user_id))
        return False

    harness.store.set("revoke_mcp_token", _revoke)
    with pytest.raises(HTTPException) as excinfo:
        await access_token_routes.revoke_mcp_token(
            request=object(), token_id="t1", dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == "Token not found or already revoked"
    # Ownership is enforced in the predicate, so the caller's own id must
    # reach the store — a revoke keyed only on token_id would be global.
    assert asked == [("t1", str(USER_ID))]


@pytest.mark.asyncio
async def test_revoking_your_own_token_reports_revoked(harness):
    async def _revoke(token_id, user_id):
        return True

    harness.store.set("revoke_mcp_token", _revoke)
    assert await access_token_routes.revoke_mcp_token(
        request=object(), token_id="t1", dependencies=harness.dependencies
    ) == {"status": "revoked"}


# =============================================================================
# The internal boundary
# =============================================================================


@pytest.mark.asyncio
async def test_verify_requires_the_internal_key(harness):
    harness.internal_error = HTTPException(401, "Invalid internal key")

    async def _tripwire(token_hash):
        raise AssertionError("must refuse before reaching the store")

    harness.store.set("get_mcp_token_by_hash", _tripwire)
    with pytest.raises(HTTPException) as excinfo:
        await access_token_routes.internal_mcp_token_verify(
            request=object(),
            body=McpTokenVerifyRequest(token_hash="h"),
            dependencies=harness.dependencies,
        )
    assert excinfo.value.status_code == 401


@pytest.mark.asyncio
async def test_internal_create_requires_the_internal_key(harness):
    harness.internal_error = HTTPException(401, "Invalid internal key")

    async def _tripwire(sub):
        raise AssertionError("must refuse before reaching the store")

    harness.store.set("get_user_by_keycloak_sub", _tripwire)
    with pytest.raises(HTTPException) as excinfo:
        await access_token_routes.internal_mcp_token_create(
            request=object(),
            body=McpTokenCreateInternal(
                user_sub="kc", name="n", token_hash="h", token_prefix="p"
            ),
            dependencies=harness.dependencies,
        )
    assert excinfo.value.status_code == 401


@pytest.mark.asyncio
async def test_verify_of_an_unknown_hash_is_401_and_bumps_nothing(harness):
    async def _by_hash(token_hash):
        return None

    async def _tripwire(token_hash):
        raise AssertionError("last_used_at must not move for an unknown token")

    harness.store.set("get_mcp_token_by_hash", _by_hash)
    harness.store.set("update_mcp_token_last_used", _tripwire)
    with pytest.raises(HTTPException) as excinfo:
        await access_token_routes.internal_mcp_token_verify(
            request=object(),
            body=McpTokenVerifyRequest(token_hash="h"),
            dependencies=harness.dependencies,
        )
    assert excinfo.value.status_code == 401
    assert excinfo.value.detail == "Invalid or expired token"


@pytest.mark.asyncio
async def test_verify_answers_identity_and_scope_only(harness):
    """The verify response is an allowlist: the stored row carries the hash
    and prefix, and neither has any business travelling back to a caller that
    already holds the secret it presented."""
    bumped = []

    async def _by_hash(token_hash):
        return {
            "user_id": USER_ID,
            "scope": "user",
            "display_name": "Alice",
            "token_hash": "STORED-HASH",
            "token_prefix": "srw_prefix1",
            "revoked_at": None,
        }

    async def _bump(token_hash):
        bumped.append(token_hash)

    harness.store.set("get_mcp_token_by_hash", _by_hash)
    harness.store.set("update_mcp_token_last_used", _bump)
    result = await access_token_routes.internal_mcp_token_verify(
        request=object(),
        body=McpTokenVerifyRequest(token_hash="presented-hash"),
        dependencies=harness.dependencies,
    )
    assert result == {
        "user_id": str(USER_ID),
        "scope": "user",
        "display_name": "Alice",
    }
    assert "STORED-HASH" not in str(result)
    assert bumped == ["presented-hash"]


@pytest.mark.asyncio
async def test_internal_create_never_returns_a_plaintext_token(harness):
    """This endpoint is handed a hash the OAuth bridge minted; it has never
    seen the secret, so a ``token`` field here could only be a fabrication."""

    async def _by_sub(sub):
        return {"id": USER_ID}

    recorded = {}

    async def _create(**kwargs):
        recorded.update(kwargs)
        return _mcp_row(token_prefix=kwargs["token_prefix"])

    harness.store.set("get_user_by_keycloak_sub", _by_sub)
    harness.store.set("create_mcp_token", _create)
    result = await access_token_routes.internal_mcp_token_create(
        request=object(),
        body=McpTokenCreateInternal(
            user_sub="kc-1",
            name="oauth",
            token_hash="bridge-hash",
            token_prefix="srw_bridge1",
            scope="user",
            origin="oauth",
        ),
        dependencies=harness.dependencies,
    )
    assert "token" not in result
    assert recorded["token_hash"] == "bridge-hash"
    assert recorded["origin"] == "oauth"


@pytest.mark.asyncio
async def test_internal_create_jit_provisions_by_keycloak_subject(harness):
    """A first OAuth login has no user row yet; the display name is derived
    from the email's local part, and falls back when there is no email."""
    upserts = []

    async def _by_sub(sub):
        return None

    async def _upsert(**kwargs):
        upserts.append(kwargs)
        return {"id": USER_ID}

    async def _create(**kwargs):
        return _mcp_row()

    harness.store.set("get_user_by_keycloak_sub", _by_sub)
    harness.store.set("upsert_user_from_oidc", _upsert)
    harness.store.set("create_mcp_token", _create)

    await access_token_routes.internal_mcp_token_create(
        request=object(),
        body=McpTokenCreateInternal(
            user_sub="kc-1",
            user_email="bob@example.com",
            name="n",
            token_hash="h",
            token_prefix="p",
        ),
        dependencies=harness.dependencies,
    )
    await access_token_routes.internal_mcp_token_create(
        request=object(),
        body=McpTokenCreateInternal(
            user_sub="kc-2", name="n", token_hash="h", token_prefix="p"
        ),
        dependencies=harness.dependencies,
    )
    assert [u["display_name"] for u in upserts] == ["bob", "OAuth User"]
    assert [u["sub"] for u in upserts] == ["kc-1", "kc-2"]


@pytest.mark.asyncio
async def test_internal_create_refuses_when_the_user_cannot_be_resolved(harness):
    async def _by_sub(sub):
        return None

    async def _upsert(**kwargs):
        return None

    async def _tripwire(**kwargs):
        raise AssertionError("no token may be minted without an owner")

    harness.store.set("get_user_by_keycloak_sub", _by_sub)
    harness.store.set("upsert_user_from_oidc", _upsert)
    harness.store.set("create_mcp_token", _tripwire)
    with pytest.raises(HTTPException) as excinfo:
        await access_token_routes.internal_mcp_token_create(
            request=object(),
            body=McpTokenCreateInternal(
                user_sub="kc", name="n", token_hash="h", token_prefix="p"
            ),
            dependencies=harness.dependencies,
        )
    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "Could not resolve user"


@pytest.mark.asyncio
async def test_internal_create_parses_an_iso_expiry(harness):
    async def _by_sub(sub):
        return {"id": USER_ID}

    recorded = {}

    async def _create(**kwargs):
        recorded.update(kwargs)
        return _mcp_row()

    harness.store.set("get_user_by_keycloak_sub", _by_sub)
    harness.store.set("create_mcp_token", _create)
    await access_token_routes.internal_mcp_token_create(
        request=object(),
        body=McpTokenCreateInternal(
            user_sub="kc",
            name="n",
            token_hash="h",
            token_prefix="p",
            expires_at="2027-01-01T00:00:00+00:00",
        ),
        dependencies=harness.dependencies,
    )
    assert recorded["expires_at"] == datetime(2027, 1, 1, tzinfo=timezone.utc)


# =============================================================================
# Personal access tokens
# =============================================================================


@pytest.mark.asyncio
async def test_a_pat_needs_at_least_one_scope(harness):
    with pytest.raises(HTTPException) as excinfo:
        await access_token_routes.create_api_key(
            request=object(),
            body=ApiKeyCreate(name="k", scopes=[]),
            dependencies=harness.dependencies,
        )
    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "At least one scope required"


@pytest.mark.asyncio
async def test_unknown_pat_scopes_are_named_in_the_refusal(harness):
    with pytest.raises(HTTPException) as excinfo:
        await access_token_routes.create_api_key(
            request=object(),
            body=ApiKeyCreate(name="k", scopes=["zz", "aa", "jobs:read"]),
            dependencies=harness.dependencies,
        )
    assert excinfo.value.status_code == 400
    # Sorted, so the message is stable regardless of set iteration order.
    assert excinfo.value.detail == "Unknown scopes: ['aa', 'zz']"


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", sorted(VALID_PAT_SCOPES - {"admin"}))
async def test_every_non_admin_scope_is_accepted(harness, scope):
    """Pins the vocabulary itself: a scope silently dropped from
    ``VALID_PAT_SCOPES`` would start 400-ing tokens that used to work."""

    async def _create(**kwargs):
        return _pat_row(scopes=kwargs["scopes"])

    harness.store.set("create_api_key", _create)
    result = await access_token_routes.create_api_key(
        request=object(),
        body=ApiKeyCreate(name="k", scopes=[scope]),
        dependencies=harness.dependencies,
    )
    assert result["scopes"] == [scope]


@pytest.mark.asyncio
async def test_the_admin_scope_needs_the_real_admin_flag(harness):
    harness.user = {"id": USER_ID, "is_admin": True, "real_is_admin": False}
    with pytest.raises(HTTPException) as excinfo:
        await access_token_routes.create_api_key(
            request=object(),
            body=ApiKeyCreate(name="k", scopes=["admin"]),
            dependencies=harness.dependencies,
        )
    assert excinfo.value.status_code == 403
    assert excinfo.value.detail == "Only admins can issue admin-scoped tokens"


@pytest.mark.asyncio
async def test_a_real_admin_may_issue_an_admin_scoped_pat(harness):
    harness.user = {"id": USER_ID, "real_is_admin": True}
    recorded = {}

    async def _create(**kwargs):
        recorded.update(kwargs)
        return _pat_row(scopes=kwargs["scopes"])

    harness.store.set("create_api_key", _create)
    await access_token_routes.create_api_key(
        request=object(),
        body=ApiKeyCreate(name="k", scopes=["admin", "jobs:read"]),
        dependencies=harness.dependencies,
    )
    assert recorded["scopes"] == ["admin", "jobs:read"]


@pytest.mark.asyncio
async def test_pat_scopes_are_deduplicated_and_sorted_before_storage(harness):
    recorded = {}

    async def _create(**kwargs):
        recorded.update(kwargs)
        return _pat_row(scopes=kwargs["scopes"])

    harness.store.set("create_api_key", _create)
    await access_token_routes.create_api_key(
        request=object(),
        body=ApiKeyCreate(name="k", scopes=["jobs:read", "chat:read", "jobs:read"]),
        dependencies=harness.dependencies,
    )
    assert recorded["scopes"] == ["chat:read", "jobs:read"]


@pytest.mark.asyncio
async def test_pat_creation_returns_the_plaintext_once_and_stores_its_hash(
    harness, pat_token_vector
):
    recorded = {}

    async def _create(**kwargs):
        recorded.update(kwargs)
        return _pat_row(
            token_prefix=kwargs["token_prefix"], last_four=kwargs["last_four"]
        )

    harness.store.set("create_api_key", _create)
    result = await access_token_routes.create_api_key(
        request=object(),
        body=ApiKeyCreate(name="k", scopes=["jobs:read"]),
        dependencies=harness.dependencies,
    )

    token = result["token"]
    expected_token, expected_digest = pat_token_vector
    assert token == expected_token
    assert recorded["token_hash"] == expected_digest
    assert recorded["token_prefix"] == token[:12]
    assert recorded["last_four"] == token[-4:]
    assert "token_hash" not in result


@pytest.mark.asyncio
async def test_pat_listing_carries_no_secret(harness):
    asked = []

    async def _list(user_id):
        asked.append(user_id)
        return [_pat_row()]

    harness.store.set("list_api_keys", _list)
    rows = await access_token_routes.list_api_keys(
        request=object(), dependencies=harness.dependencies
    )
    assert asked == [str(USER_ID)]
    assert "token" not in rows[0]
    assert rows[0]["id"] == "00000000-0000-0000-0000-0000000000e1"
    assert rows[0]["last_four"] == "abcd"


@pytest.mark.asyncio
async def test_revoking_a_pat_that_is_not_yours_is_a_404(harness):
    asked = []

    async def _revoke(token_id, user_id):
        asked.append((token_id, user_id))
        return False

    harness.store.set("revoke_api_key", _revoke)
    with pytest.raises(HTTPException) as excinfo:
        await access_token_routes.revoke_api_key(
            request=object(), token_id="t1", dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 404
    assert asked == [("t1", str(USER_ID))]


@pytest.mark.asyncio
async def test_rotation_mints_a_new_secret_and_is_owner_scoped(
    harness, pat_token_vector
):
    recorded = {}

    async def _rotate(**kwargs):
        recorded.update(kwargs)
        return _pat_row(id=UUID("00000000-0000-0000-0000-0000000000e2"))

    harness.store.set("rotate_api_key", _rotate)
    result = await access_token_routes.rotate_api_key(
        request=object(), token_id="old-id", dependencies=harness.dependencies
    )

    token = result["token"]
    expected_token, expected_digest = pat_token_vector
    assert token == expected_token
    assert recorded["old_id"] == "old-id"
    assert recorded["user_id"] == str(USER_ID)
    assert recorded["token_hash"] == expected_digest
    # Name, scopes and expiry are copied by the store from the source row —
    # a rotation that re-derived them here could silently widen a token.
    assert "scopes" not in recorded
    assert "expires_at" not in recorded


@pytest.mark.asyncio
async def test_rotating_an_unknown_or_foreign_token_is_a_404(harness):
    async def _rotate(**kwargs):
        return None

    harness.store.set("rotate_api_key", _rotate)
    with pytest.raises(HTTPException) as excinfo:
        await access_token_routes.rotate_api_key(
            request=object(), token_id="t1", dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == "Token not found, already revoked, or not yours"


# =============================================================================
# Wire
# =============================================================================


@pytest.mark.asyncio
async def test_the_mint_and_list_pair_over_http(harness):
    """Through real routing, with this application's own factory: the mint
    response carries the plaintext, the very next list does not."""
    minted = {}

    async def _create(**kwargs):
        minted.update(kwargs)
        return _pat_row(
            token_prefix=kwargs["token_prefix"], last_four=kwargs["last_four"]
        )

    async def _list(user_id):
        return [_pat_row(token_prefix=minted["token_prefix"])]

    harness.store.set("create_api_key", _create)
    harness.store.set("list_api_keys", _list)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=harness.app()), base_url="http://test"
    ) as client:
        created = await client.post(
            "/api/api-keys", json={"name": "k", "scopes": ["jobs:read"]}
        )
        listed = await client.get("/api/api-keys")

    assert created.status_code == 200, created.text
    token = created.json()["token"]
    assert token.startswith("ak_")

    assert listed.status_code == 200, listed.text
    assert token not in listed.text
    assert minted["token_hash"] not in listed.text
    assert listed.json()[0]["token_prefix"] == token[:12]


@pytest.mark.asyncio
async def test_a_refused_scope_reaches_the_wire_as_a_400(harness):
    async def _tripwire(**kwargs):
        raise AssertionError("no row may be written for a refused scope")

    harness.store.set("create_api_key", _tripwire)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=harness.app()), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/api-keys", json={"name": "k", "scopes": ["not-a-scope"]}
        )
    assert response.status_code == 400
    assert response.json()["detail"] == "Unknown scopes: ['not-a-scope']"


def test_the_access_token_router_serves_exactly_its_own_surface():
    assert {
        (route.path, tuple(sorted(route.methods)))
        for route in access_token_routes.router.routes
    } == {
        ("/api/mcp-tokens", ("POST",)),
        ("/api/mcp-tokens", ("GET",)),
        ("/api/mcp-tokens/{token_id}", ("DELETE",)),
        ("/api/internal/mcp-token-verify", ("POST",)),
        ("/api/internal/mcp-token-create", ("POST",)),
        ("/api/internal/mcp-oauth-grant", ("POST",)),
        ("/api/internal/mcp-oauth-refresh/lookup", ("POST",)),
        ("/api/internal/mcp-oauth-refresh/rotate", ("POST",)),
        ("/api/internal/mcp-oauth-refresh/revoke", ("POST",)),
        ("/api/api-keys", ("POST",)),
        ("/api/api-keys", ("GET",)),
        ("/api/api-keys/{token_id}", ("DELETE",)),
        ("/api/api-keys/{token_id}/rotate", ("POST",)),
    }


# =============================================================================
# MCP OAuth grants (G6): the bridge's refresh tokens. The SQL semantics run on
# real PostgreSQL in tests/test_mcp_oauth_grants_real_postgres.py; these pin
# the HTTP boundary.
# =============================================================================

H64 = "a" * 64
GRANT_ROUTES = [
    (
        "/api/internal/mcp-oauth-grant",
        {
            "user_sub": "kc-1",
            "name": "OAuth: Claude",
            "oauth_client_id": "dcr-1",
            "origin": "oauth:Claude",
            "access_token_hash": H64,
            "access_token_prefix": "srw_abc",
            "access_expires_at": "2026-11-05T12:00:00+00:00",
            "refresh_token_hash": "b" * 64,
            "grant_expires_at": "2026-12-05T11:00:00+00:00",
        },
    ),
    ("/api/internal/mcp-oauth-refresh/lookup", {"refresh_token_hash": H64}),
    (
        "/api/internal/mcp-oauth-refresh/rotate",
        {
            "refresh_token_hash": H64,
            "new_refresh_token_hash": "b" * 64,
            "new_access_token_hash": "c" * 64,
            "new_access_token_prefix": "srw_new",
            "access_ttl_seconds": 3600,
        },
    ),
    ("/api/internal/mcp-oauth-refresh/revoke", {"refresh_token_hash": H64}),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("path,body", GRANT_ROUTES)
async def test_grant_routes_are_internal_service_only(harness, path, body):
    """A delegated MCP user never reaches them; the store is not touched."""
    harness.internal_error = HTTPException(status_code=403, detail="no delegated user")

    async with _client(harness) as client:
        response = await client.post(path, json=body)

    assert response.status_code == 403
    assert harness.service_gate_calls == [path]


@pytest.mark.asyncio
async def test_grant_create_resolves_the_user_and_stores_hashes_only(harness):
    stored = {}

    async def create(**fields):
        stored.update(fields)
        return {"grant_id": "g-1", "access_token_id": "t-1", "grant_expires_at": None}

    harness.store.set(
        "get_user_by_keycloak_sub", _async({"id": USER_ID, "is_approved": True})
    ).set("create_mcp_oauth_grant", create)

    async with _client(harness) as client:
        path, body = _route_body("/api/internal/mcp-oauth-grant")
        response = await client.post(path, json=body)

    assert response.status_code == 200, response.text
    assert stored["user_id"] == str(USER_ID)
    assert stored["access_token_hash"] == H64
    assert stored["access_expires_at"] == datetime(2026, 11, 5, 12, tzinfo=timezone.utc)
    assert "token" not in response.json()


@pytest.mark.asyncio
async def test_lookup_of_an_unknown_refresh_token_is_404_after_the_replay_check(
    harness,
):
    replay_checked = []

    async def on_replay(refresh_hash):
        replay_checked.append(refresh_hash)
        return True

    harness.store.set("get_mcp_oauth_grant_by_refresh_hash", _async(None)).set(
        "revoke_mcp_oauth_grant_on_replay", on_replay
    )

    async with _client(harness) as client:
        path, body = _route_body("/api/internal/mcp-oauth-refresh/lookup")
        response = await client.post(path, json=body)

    assert response.status_code == 404
    assert replay_checked == [H64]


@pytest.mark.asyncio
async def test_lookup_answers_client_scope_and_expiry(harness):
    harness.store.set(
        "get_mcp_oauth_grant_by_refresh_hash",
        _async(
            {
                "id": UUID(int=7),
                "user_id": USER_ID,
                "oauth_client_id": "dcr-1",
                "scope": "user",
                "expires_at": CREATED,
            }
        ),
    )

    async with _client(harness) as client:
        path, body = _route_body("/api/internal/mcp-oauth-refresh/lookup")
        response = await client.post(path, json=body)

    assert response.status_code == 200
    assert response.json()["oauth_client_id"] == "dcr-1"
    assert response.json()["grant_expires_at"] == str(CREATED)


@pytest.mark.asyncio
async def test_rotation_of_a_dead_refresh_token_is_404(harness):
    harness.store.set("rotate_mcp_oauth_grant", _async(None))

    async with _client(harness) as client:
        path, body = _route_body("/api/internal/mcp-oauth-refresh/rotate")
        response = await client.post(path, json=body)

    assert response.status_code == 404


def _route_body(path):
    return path, dict(GRANT_ROUTES)[path]


def _client(harness):
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=harness.app()), base_url="http://test"
    )


def _async(value):
    async def _method(*args, **kwargs):
        return value

    return _method
