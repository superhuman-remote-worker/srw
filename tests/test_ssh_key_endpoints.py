"""Tests for the SSH key registration REST surface (``/api/ssh-keys*``).

Registration requires proving possession of the private key before a public
key is accepted. Without that check, anyone could claim a public key they
merely read (e.g. published at ``github.com/<user>.keys``), and because
fingerprints are globally unique that would deny the rightful owner the
ability to register their own key.

Possession challenges are STATELESS — an HMAC-signed token over a nonce, the
authenticated user's id and an expiry, verified with ``SESSION_JWT_SECRET`` —
not an in-process dict. The orchestrator runs multiple replicas behind one
Service with no session affinity (the dev values pin
``orchestrator.replicas: 2`` deliberately), so the pod that issues a
challenge is frequently not the pod that redeems it; an in-process store
would reject roughly half of all registrations with "unknown challenge".
See ruling F24 in
``.superpowers/sdd/2026-08-28-workspace-ssh-access-foundation/progress.md``.

Because the token is stateless rather than single-use, ``test_challenge_*``
below documents the actual anti-replay property: a token is bound to the
user id that requested it (rejected cross-account, ``test_challenge_minted_
for_one_user_is_rejected_for_another``) and to a five-minute window
(``test_expired_challenge_is_rejected``), and integrity-checked
(``test_tampered_challenge_is_rejected``); a same-user replay is allowed at
the token layer and is instead caught by the fingerprint uniqueness
constraint on ``user_ssh_keys`` (409, not a token-layer 400).

Fix round 1 (review findings) adds: the token carries a fifth, MAC-covered
but display-only identity clause so a signer can see whose account they're
about to bind (``test_identity_*`` / ``test_minted_token_contains_the_
identity_clause``) — closing a confused-deputy phishing case a bare UUID
enabled; a ``str.isascii()`` guard so a non-ASCII or lone-surrogate challenge
is rejected rather than crashing ``hmac.compare_digest`` into an unhandled
500 (``test_*non_ascii*`` / ``test_*surrogate*``); fail-closed checks moved
into the helpers themselves, not just the endpoints (``test_mint_raises_
when_secret_is_empty`` / ``test_verify_returns_false_when_secret_is_empty``);
a malformed ``key_id`` on delete folded into the existing 404
(``test_delete_ssh_key_malformed_id_is_404_not_500``); and wiring tests
distinct from logic tests — route registration, the previously-untested GET,
the delete happy path, both branches of ``serialize_ssh_key_row``'s
``.isoformat()`` calls, and pinned arguments into ``verify_possession`` and
the delete store call.

These cases first passed against the original handlers in ``main`` and now
exercise ``orchestrator.routers.ssh_access`` over
``orchestrator.services.ssh_access``. The session secret is a *dependency
value* rather than a module global, so the fail-closed cases set
``harness.secret = ""`` instead of monkeypatching an import-time binding.
"""

import json
import time
from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

import orchestrator.main
from orchestrator.routers import ssh_access as ssh_access_routes
from orchestrator.services import ssh_access as ssh_access_operations
from orchestrator.services.ssh_public_keys import SshKeyRejected
from tests._ssh_access_harness import SECRET, SshAccessHarness
from tests._route_inventory import mounted_routes
from orchestrator.application import workflows as workflows_composition


class _Body:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _parsed(fingerprint="SHA256:" + "A" * 43, public_key=None):
    def _parse(text):
        return _Body(
            key_type="ssh-ed25519",
            public_key=public_key if public_key is not None else text,
            fingerprint_sha256=fingerprint,
            comment="",
        )

    return _parse


@pytest.fixture
def harness():
    return SshAccessHarness()


@pytest.fixture
def approved_user(harness):
    harness.user = {"id": "00000000-0000-0000-0000-000000000001", "is_approved": True}
    return harness.user


@pytest.mark.asyncio
async def test_challenge_is_reusable_but_duplicate_key_is_rejected_by_fingerprint(
    harness, approved_user, monkeypatch
):
    """Not single-use at the token layer, by design (ruling F24).

    Binding the token to the caller's user id is what carries the security
    property a stateful single-use store would have provided: a captured
    (challenge, signature) pair can't be replayed to register someone else's
    key, because the embedded user id is checked against the authenticated
    caller (see the cross-user test below). Replaying your OWN token just
    re-registers your own key, which the database's fingerprint-uniqueness
    constraint rejects — surfaced as 409 here, not the 400 "unknown
    challenge" a dict-backed single-use store would have produced on the
    second call. If this starts asserting 400, someone put the nonce store
    back — see the comment on ``mint_ssh_key_challenge``.
    """
    from orchestrator.database.postgres import SshKeyAlreadyRegistered

    challenge = await ssh_access_routes.create_ssh_key_challenge(
        request=object(), dependencies=harness.dependencies
    )
    assert challenge["namespace"]
    assert len(challenge["challenge"]) >= 32

    monkeypatch.setattr(ssh_access_operations, "parse_public_key", _parsed())
    monkeypatch.setattr(
        ssh_access_operations, "verify_possession", lambda *a, **k: True
    )

    calls = {"n": 0}

    async def _create(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return {
                "id": "k1",
                "name": kwargs["name"],
                "key_type": "ssh-ed25519",
                "fingerprint_sha256": "SHA256:" + "A" * 43,
                "created_at": None,
                "last_used_at": None,
                "disabled_at": None,
            }
        raise SshKeyAlreadyRegistered("SHA256:" + "A" * 43)

    harness.store.set("create_user_ssh_key", _create)

    body = _Body(
        name="laptop",
        public_key="ssh-ed25519 AAAA",
        challenge=challenge["challenge"],
        signature="-----BEGIN SSH SIGNATURE-----",
    )
    first = await ssh_access_routes.create_ssh_key(
        request=object(), body=body, dependencies=harness.dependencies
    )
    assert first["id"] == "k1"

    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.create_ssh_key(
            request=object(), body=body, dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 409
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_challenge_minted_for_one_user_is_rejected_for_another(
    harness, monkeypatch
):
    """The exact lockout attack the possession check exists to prevent:
    replaying a captured (challenge, signature) pair to register a key under
    a *different* account. A stateless token only carries this property if
    the embedded user id is checked against the authenticated caller, not
    just the signature — this pins that check.
    """
    harness.user = {"id": "00000000-0000-0000-0000-0000000000aa", "is_approved": True}
    challenge = await ssh_access_routes.create_ssh_key_challenge(
        request=object(), dependencies=harness.dependencies
    )

    harness.user = {"id": "00000000-0000-0000-0000-0000000000bb", "is_approved": True}
    monkeypatch.setattr(
        ssh_access_operations, "parse_public_key", _parsed("SHA256:" + "B" * 43)
    )
    monkeypatch.setattr(
        ssh_access_operations, "verify_possession", lambda *a, **k: True
    )

    body = _Body(
        name="stolen",
        public_key="ssh-ed25519 AAAA",
        challenge=challenge["challenge"],
        signature="sig",
    )
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.create_ssh_key(
            request=object(), body=body, dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 400


def test_expired_challenge_is_rejected():
    """Five-minute window, enforced from the embedded (HMAC-protected)
    expiry rather than any external state.
    """
    user_id = "00000000-0000-0000-0000-000000000042"
    past = time.time() - 10_000
    token, expires_at = ssh_access_operations.mint_ssh_key_challenge(
        user_id, now=past, secret=SECRET
    )
    assert expires_at <= time.time()
    assert not ssh_access_operations.verify_ssh_key_challenge(
        token, user_id, secret=SECRET
    )


def test_tampered_challenge_is_rejected():
    """Flipping one character anywhere in the token must invalidate the
    HMAC — this is what makes the embedded user id and expiry trustworthy.
    """
    user_id = "00000000-0000-0000-0000-000000000042"
    token, _ = ssh_access_operations.mint_ssh_key_challenge(user_id, secret=SECRET)
    tampered = token[:-1] + ("0" if token[-1] != "0" else "1")
    assert tampered != token
    assert not ssh_access_operations.verify_ssh_key_challenge(
        tampered, user_id, secret=SECRET
    )


@pytest.mark.asyncio
async def test_tampered_challenge_is_rejected_through_the_endpoint(
    harness, approved_user, monkeypatch
):
    challenge = await ssh_access_routes.create_ssh_key_challenge(
        request=object(), dependencies=harness.dependencies
    )
    original = challenge["challenge"]
    tampered = original[:-1] + ("0" if original[-1] != "0" else "1")

    monkeypatch.setattr(ssh_access_operations, "parse_public_key", _parsed())
    monkeypatch.setattr(
        ssh_access_operations, "verify_possession", lambda *a, **k: True
    )
    body = _Body(
        name="laptop",
        public_key="ssh-ed25519 AAAA",
        challenge=tampered,
        signature="sig",
    )
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.create_ssh_key(
            request=object(), body=body, dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 400


@pytest.mark.asyncio
async def test_challenge_endpoint_503_when_secret_is_empty(harness, approved_user):
    """Fail closed: application startup only logs a warning for an empty
    SESSION_JWT_SECRET today. The challenge endpoint must not rely on that —
    an empty secret is a well-known HMAC key, so every replica would sign
    forgeable tokens.
    """
    harness.secret = ""
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.create_ssh_key_challenge(
            request=object(), dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 503


@pytest.mark.asyncio
async def test_create_ssh_key_503_when_secret_is_empty(harness, approved_user):
    """Same fail-closed guard on the redemption side: if the secret goes
    empty between mint and redeem, verifying the challenge would be
    meaningless, so refuse outright rather than accept.
    """
    challenge = await ssh_access_routes.create_ssh_key_challenge(
        request=object(), dependencies=harness.dependencies
    )
    harness.secret = ""
    body = _Body(
        name="laptop",
        public_key="ssh-ed25519 AAAA",
        challenge=challenge["challenge"],
        signature="sig",
    )
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.create_ssh_key(
            request=object(), body=body, dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 503


@pytest.mark.asyncio
async def test_rejects_unproven_key(harness, approved_user, monkeypatch):
    challenge = await ssh_access_routes.create_ssh_key_challenge(
        request=object(), dependencies=harness.dependencies
    )
    monkeypatch.setattr(ssh_access_operations, "parse_public_key", _parsed())
    monkeypatch.setattr(
        ssh_access_operations, "verify_possession", lambda *a, **k: False
    )
    body = _Body(
        name="laptop",
        public_key="ssh-ed25519 AAAA",
        challenge=challenge["challenge"],
        signature="bogus",
    )
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.create_ssh_key(
            request=object(), body=body, dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 400
    assert "possession" in excinfo.value.detail.lower()


@pytest.mark.asyncio
async def test_rejects_bad_key_with_its_reason(harness, approved_user, monkeypatch):
    challenge = await ssh_access_routes.create_ssh_key_challenge(
        request=object(), dependencies=harness.dependencies
    )

    def _reject(text):
        raise SshKeyRejected("RSA keys must be at least 3072 bits; this one is 2048.")

    monkeypatch.setattr(ssh_access_operations, "parse_public_key", _reject)
    body = _Body(
        name="old",
        public_key="ssh-rsa AAAA",
        challenge=challenge["challenge"],
        signature="x",
    )
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.create_ssh_key(
            request=object(), body=body, dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 400
    assert "3072" in excinfo.value.detail


@pytest.mark.asyncio
async def test_duplicate_fingerprint_is_409_with_a_recovery_path(
    harness, approved_user, monkeypatch
):
    from orchestrator.database.postgres import SshKeyAlreadyRegistered

    challenge = await ssh_access_routes.create_ssh_key_challenge(
        request=object(), dependencies=harness.dependencies
    )
    monkeypatch.setattr(ssh_access_operations, "parse_public_key", _parsed())
    monkeypatch.setattr(
        ssh_access_operations, "verify_possession", lambda *a, **k: True
    )

    async def _boom(**kwargs):
        raise SshKeyAlreadyRegistered("SHA256:" + "A" * 43)

    harness.store.set("create_user_ssh_key", _boom)
    body = _Body(
        name="laptop",
        public_key="ssh-ed25519 AAAA",
        challenge=challenge["challenge"],
        signature="x",
    )
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.create_ssh_key(
            request=object(), body=body, dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 409
    assert "support" in excinfo.value.detail.lower()


@pytest.mark.asyncio
async def test_key_cap_is_409_naming_the_cap(harness, approved_user, monkeypatch):
    """Spec §4.1 caps registrations at ``MAX_SSH_KEYS_PER_USER``.

    Also 409, like the duplicate above, but a different recovery path — this
    one the user can fix alone, which is why the number has to be in the
    message. The store raises; this pins the translation.
    """
    from orchestrator.database.postgres import MAX_SSH_KEYS_PER_USER, SshKeyLimitReached

    challenge = await ssh_access_routes.create_ssh_key_challenge(
        request=object(), dependencies=harness.dependencies
    )
    monkeypatch.setattr(
        ssh_access_operations, "parse_public_key", _parsed("SHA256:" + "B" * 43)
    )
    monkeypatch.setattr(
        ssh_access_operations, "verify_possession", lambda *a, **k: True
    )

    async def _capped(**kwargs):
        raise SshKeyLimitReached(MAX_SSH_KEYS_PER_USER)

    harness.store.set("create_user_ssh_key", _capped)
    body = _Body(
        name="laptop",
        public_key="ssh-ed25519 AAAA",
        challenge=challenge["challenge"],
        signature="x",
    )
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.create_ssh_key(
            request=object(), body=body, dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 409
    assert str(MAX_SSH_KEYS_PER_USER) in excinfo.value.detail
    # Not the duplicate-fingerprint message, which points at support.
    assert "support" not in excinfo.value.detail.lower()


@pytest.mark.asyncio
async def test_delete_reports_miss(harness, approved_user):
    async def _delete(key_id, user_id):
        return False

    harness.store.set("delete_user_ssh_key", _delete)
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.delete_ssh_key(
            request=object(), key_id="k1", dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 404


# =============================================================================
# Final review, Important 6 — a project-scoped MCP token must not mint or
# revoke a personal SSH credential.
# =============================================================================


@pytest.fixture
def project_scoped_user(harness):
    """An approved user authenticated by a legacy ``project:<uuid>``-scoped
    MCP token. ``user_can_access_job_or_thread`` denies this principal every
    thread — the IDE included — via ``_scope_permits_personal``.

    The real ``require_personal_scope`` runs (the harness default), so its
    denial audit write has to land somewhere: the store answers it.
    """
    user = {
        "id": "00000000-0000-0000-0000-000000000001",
        "is_approved": True,
        "scopes": ["project:11111111-1111-1111-1111-111111111111"],
    }

    async def _no_audit(**kwargs):
        return None

    harness.user = user
    harness.store.set("record_security_event", _no_audit)
    return user


def test_the_scope_helper_actually_denies_this_principal(project_scoped_user):
    """Guards the fixture, not the endpoint: if the scope field were named
    something ``_scope_project_id`` does not read, the two tests below would
    pass for the wrong reason — the principal would simply be unscoped.
    """
    from orchestrator.security import access

    assert access._scope_project_id(project_scoped_user) is not None
    assert not access._scope_permits_personal(project_scoped_user)


@pytest.mark.asyncio
async def test_project_scoped_token_cannot_register_a_key(
    harness, project_scoped_user, monkeypatch
):
    """Registration composes with resolution into something neither is alone:
    an SSH key authenticates by fingerprint, so the token's scope is gone by
    the time authorization runs. The gate therefore has to be at minting."""

    def _tripwire(*a, **k):
        raise AssertionError("must refuse before parsing the key")

    monkeypatch.setattr(ssh_access_operations, "parse_public_key", _tripwire)
    body = _Body(
        name="laptop",
        public_key="ssh-ed25519 AAAA",
        challenge="whatever",
        signature="x",
    )
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.create_ssh_key(
            request=object(), body=body, dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 403


@pytest.mark.asyncio
async def test_project_scoped_token_cannot_list_keys(harness, project_scoped_user):
    """Listing is read-only and leaks no id the token could act on, so this is
    the weakest of the three gates. It exists for contract consistency:
    _scope_permits_personal's own docstring says such a token "shouldn't be
    able to read or mutate" a personal resource, and leaving one of the three
    call sites open would make them disagree with the helper they cite."""

    async def _tripwire(user_id):
        raise AssertionError("must refuse before reaching the store")

    harness.store.set("list_user_ssh_keys", _tripwire)
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.list_ssh_keys(
            request=object(), dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 403


@pytest.mark.asyncio
async def test_project_scoped_token_cannot_delete_a_key(harness, project_scoped_user):
    """Deletion is the only revocation this feature has, so leaving it open
    while gating create would let a project-scoped token strip its owner's
    access."""

    async def _tripwire(key_id, user_id):
        raise AssertionError("must refuse before reaching the store")

    harness.store.set("delete_user_ssh_key", _tripwire)
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.delete_ssh_key(
            request=object(),
            key_id="00000000-0000-0000-0000-0000000000aa",
            dependencies=harness.dependencies,
        )
    assert excinfo.value.status_code == 403


@pytest.mark.asyncio
async def test_project_scoped_token_cannot_mint_an_attach_token(
    harness, project_scoped_user
):
    """The attach token opens a transport into every workspace its holder's
    registered keys reach, and the SSH layer authorizes by fingerprint — by
    which point the MCP token's scope no longer exists to check."""
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.create_ssh_attach_token(
            request=object(), dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 403


# =============================================================================
# Fix round 1 — wiring tests distinct from logic tests (review Minor 6)
# =============================================================================


def test_ssh_key_routes_are_mounted():
    """Every test in this file calls handlers directly, so none of them
    prove FastAPI actually serves these paths at these methods — a typo'd
    decorator path or a route registered under the wrong verb would pass
    every other test here and still 404 in production.
    """
    routes = mounted_routes(orchestrator.main.app)
    assert ("POST", "/api/ssh-keys/challenge") in routes
    assert ("POST", "/api/ssh-keys") in routes
    assert ("GET", "/api/ssh-keys") in routes
    assert ("DELETE", "/api/ssh-keys/{key_id}") in routes


@pytest.mark.asyncio
async def test_list_ssh_keys(harness, approved_user):
    """No test exercised GET /api/ssh-keys at all before this."""

    async def _list(user_id):
        assert user_id == approved_user["id"]
        return [
            {
                "id": "k1",
                "name": "laptop",
                "key_type": "ssh-ed25519",
                "fingerprint_sha256": "SHA256:" + "A" * 43,
                "created_at": None,
                "last_used_at": None,
                "disabled_at": None,
            }
        ]

    harness.store.set("list_user_ssh_keys", _list)
    result = await ssh_access_routes.list_ssh_keys(
        request=object(), dependencies=harness.dependencies
    )
    assert result == [
        {
            "id": "k1",
            "name": "laptop",
            "key_type": "ssh-ed25519",
            "fingerprint": "SHA256:" + "A" * 43,
            "created_at": None,
            "last_used_at": None,
            "disabled": False,
        }
    ]


@pytest.mark.asyncio
async def test_delete_ssh_key_happy_path(harness, approved_user):
    """The delete happy path, including its response body, plus argument
    pinning: the user_id that reaches the store must be the authenticated
    caller's, not something derived from the path or body.
    """
    captured = {}

    async def _delete(key_id, user_id):
        captured["key_id"] = key_id
        captured["user_id"] = user_id
        return True

    harness.store.set("delete_user_ssh_key", _delete)
    result = await ssh_access_routes.delete_ssh_key(
        request=object(), key_id="k1", dependencies=harness.dependencies
    )
    assert result == {"status": "deleted"}
    assert captured == {"key_id": "k1", "user_id": approved_user["id"]}


@pytest.mark.asyncio
async def test_delete_ssh_key_malformed_id_is_404_not_500(harness, approved_user):
    """Review Minor 3: the store's ``UUID(key_id)`` raises ``ValueError`` on
    a malformed id. That must fold into the existing "not found" outcome,
    not surface as an unhandled 500.
    """

    async def _delete(key_id, user_id):
        raise ValueError("badly formed hexadecimal UUID string")

    harness.store.set("delete_user_ssh_key", _delete)
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.delete_ssh_key(
            request=object(), key_id="not-a-uuid", dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 404


def test_serialize_ssh_key_row_with_timestamps():
    """The truthy branch of both ``.isoformat()`` calls — every other
    fixture in this file passes ``created_at: None``.
    """
    created = datetime(2026, 1, 1, tzinfo=timezone.utc)
    used = datetime(2026, 1, 2, tzinfo=timezone.utc)
    row = {
        "id": "k1",
        "name": "laptop",
        "key_type": "ssh-ed25519",
        "fingerprint_sha256": "SHA256:" + "A" * 43,
        "created_at": created,
        "last_used_at": used,
        "disabled_at": None,
    }
    result = ssh_access_operations.serialize_ssh_key_row(row)
    assert result["created_at"] == created.isoformat()
    assert result["last_used_at"] == used.isoformat()
    assert result["disabled"] is False


def test_serialize_ssh_key_row_without_timestamps():
    row = {
        "id": "k1",
        "name": "laptop",
        "key_type": "ssh-ed25519",
        "fingerprint_sha256": "SHA256:" + "A" * 43,
        "created_at": None,
        "last_used_at": None,
        "disabled_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    result = ssh_access_operations.serialize_ssh_key_row(row)
    assert result["created_at"] is None
    assert result["last_used_at"] is None
    assert result["disabled"] is True


def test_serialized_key_row_never_carries_the_public_key_material():
    """The projection is an allowlist, and it must stay one: the store row
    carries ``public_key`` (and could grow more columns), none of which the
    key list has any reason to hand back.
    """
    row = {
        "id": "k1",
        "name": "laptop",
        "key_type": "ssh-ed25519",
        "public_key": "ssh-ed25519 AAAA secret-comment",
        "fingerprint_sha256": "SHA256:" + "A" * 43,
        "created_at": None,
        "last_used_at": None,
        "disabled_at": None,
        "user_id": "00000000-0000-0000-0000-000000000001",
    }
    result = ssh_access_operations.serialize_ssh_key_row(row)
    assert set(result) == {
        "id",
        "name",
        "key_type",
        "fingerprint",
        "created_at",
        "last_used_at",
        "disabled",
    }
    assert "ssh-ed25519 AAAA secret-comment" not in json.dumps(result)


@pytest.mark.asyncio
async def test_verify_possession_receives_parsed_key_namespace_and_challenge(
    harness, approved_user, monkeypatch
):
    """Argument pinning on verify_possession: the PARSED (normalized)
    public key — not the raw request body string — the module's
    SIGNATURE_NAMESPACE, and the challenge string (encoded) as the signed
    payload, plus the signature verbatim.
    """
    challenge = await ssh_access_routes.create_ssh_key_challenge(
        request=object(), dependencies=harness.dependencies
    )

    monkeypatch.setattr(
        ssh_access_operations,
        "parse_public_key",
        _parsed(public_key="ssh-ed25519 AAAA-normalized comment"),
    )

    captured = {}

    def _verify(public_key, namespace, payload, signature):
        captured["public_key"] = public_key
        captured["namespace"] = namespace
        captured["payload"] = payload
        captured["signature"] = signature
        return True

    monkeypatch.setattr(ssh_access_operations, "verify_possession", _verify)

    async def _create(**kwargs):
        return {
            "id": "k1",
            "name": kwargs["name"],
            "key_type": "ssh-ed25519",
            "fingerprint_sha256": "SHA256:" + "A" * 43,
            "created_at": None,
            "last_used_at": None,
            "disabled_at": None,
        }

    harness.store.set("create_user_ssh_key", _create)

    body = _Body(
        name="laptop",
        public_key="ssh-ed25519 AAAA raw-comment",
        challenge=challenge["challenge"],
        signature="the-signature",
    )
    await ssh_access_routes.create_ssh_key(
        request=object(), body=body, dependencies=harness.dependencies
    )

    assert captured["public_key"] == "ssh-ed25519 AAAA-normalized comment"
    assert captured["namespace"] == ssh_access_operations.SIGNATURE_NAMESPACE
    assert captured["payload"] == challenge["challenge"].encode("utf-8")
    assert captured["signature"] == "the-signature"


# =============================================================================
# Fix round 1 — Important 1: non-ASCII challenge must reject, never raise
# =============================================================================


def test_verify_ssh_key_challenge_rejects_non_ascii_without_raising():
    """Reproduces the review finding directly: ``hmac.compare_digest``
    raises ``TypeError`` on a non-ASCII ``str`` argument, reachable
    pre-authentication since the raw signature field is compared before its
    validity is known. Before the ``isascii()`` guard this token would have
    raised out of ``verify_ssh_key_challenge`` instead of returning False.
    """
    token = "srw-ssh1:a:b:c:d:\u00e9"
    assert (
        ssh_access_operations.verify_ssh_key_challenge(token, "b", secret=SECRET)
        is False
    )


def test_verify_ssh_key_challenge_rejects_lone_surrogate():
    """The specific case the review flagged as the wrong direction to fix
    this in: a lone UTF-16 surrogate is reachable through ``json.loads`` on
    a hostile request body and is non-ASCII (so ``isascii()`` catches it
    too), but would raise ``UnicodeEncodeError`` — not ``TypeError`` — if
    this were "fixed" by encoding to bytes and comparing instead, which
    just moves the crash rather than closing it.
    """
    token = json.loads('{"c": "srw-ssh1:a:b:c:d:\\udcff"}')["c"]
    with pytest.raises(UnicodeEncodeError):
        token.encode("utf-8")  # documents why encode-first is not the fix
    assert (
        ssh_access_operations.verify_ssh_key_challenge(token, "b", secret=SECRET)
        is False
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_challenge",
    [
        "srw-ssh1:a:b:c:d:\u00e9",
        # A lone surrogate is the case that pins the ORDER, not just the guard:
        # it encodes to UTF-8 with a UnicodeEncodeError rather than succeeding,
        # so this input fails only if verification runs BEFORE
        # body.challenge.encode("utf-8"). The `\u00e9` case above passes either
        # way, because it encodes cleanly.
        "srw-ssh1:a:b:c:d:\udcff",
    ],
)
async def test_non_ascii_challenge_is_rejected_not_raised_through_endpoint(
    harness, approved_user, monkeypatch, bad_challenge
):
    """End-to-end: a non-ASCII challenge in the request body must come back
    as a 4xx HTTPException, never an unhandled exception an authenticated
    caller could loop to produce logged 500s.
    """
    monkeypatch.setattr(ssh_access_operations, "parse_public_key", _parsed())
    body = _Body(
        name="laptop",
        public_key="ssh-ed25519 AAAA",
        challenge=bad_challenge,
        signature="sig",
    )
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.create_ssh_key(
            request=object(), body=body, dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 400


# =============================================================================
# Fix round 1 — Important 2: identity clause, MAC-covered, display-only
# =============================================================================


def _mint(user_id, identity=None):
    token, _ = ssh_access_operations.mint_ssh_key_challenge(
        user_id, identity, secret=SECRET
    )
    return token


def _verify(token, user_id):
    return ssh_access_operations.verify_ssh_key_challenge(token, user_id, secret=SECRET)


def test_minted_token_contains_the_identity_clause():
    token = _mint("user-a-id", "alice@example.com")
    assert "alice@example.com" in token.split(":")


def test_identity_label_is_covered_by_the_mac():
    """Flipping the identity clause after minting must invalidate the token
    — otherwise the label wouldn't actually be trustworthy to a signer."""
    token = _mint("user-a-id", "alice")
    assert _verify(token, "user-a-id") is True
    tampered = token.replace(":alice:", ":mallory:")
    assert tampered != token
    assert _verify(tampered, "user-a-id") is False


def test_identity_label_is_never_consulted_for_authorization():
    """The confused-deputy fix itself (review Important 2): the identity
    clause is display-only. A token minted for ``user-a-id`` whose label
    happens to name a different account must still authorize ONLY
    ``user-a-id`` — never the account the label names.
    """
    token = _mint("user-a-id", "looks-like-user-b")
    assert _verify(token, "user-a-id") is True
    assert _verify(token, "looks-like-user-b") is False


def test_identity_with_whitespace_falls_back_to_user_id():
    token = _mint("user-a-id", "alice smith")
    assert "alice smith" not in token
    assert "user-a-id" in token.split(":")


def test_non_ascii_identity_falls_back_to_user_id():
    token = _mint("user-a-id", "\u00c9tienne")
    assert "\u00c9tienne" not in token
    assert token.isascii()
    assert "user-a-id" in token.split(":")


def test_overlong_identity_falls_back_to_user_id():
    long_label = "x" * (ssh_access_operations.SSH_CHALLENGE_IDENTITY_MAX_LEN + 1)
    token = _mint("user-a-id", long_label)
    assert long_label not in token
    assert "user-a-id" in token.split(":")


@pytest.mark.parametrize(
    "label",
    [
        "vict\x1b[2Kmallory@srw.works",  # ESC: rewrites the line as it renders
        "alice\x08\x08\x08\x08\x08mallory",  # backspaces: erases what precedes
        "alice\x00mallory",  # NUL: truncates in C-string consumers
        "alice\x7f",  # DEL
    ],
)
def test_control_character_identity_falls_back_to_user_id(label):
    """A control character is ASCII and is not whitespace, so it slipped past
    the other three guards. It defeats the exact property the identity clause
    exists to provide: a signer inspecting the token to see whose account it
    binds can have that display rewritten by terminal escapes, putting them
    back in the phished state the label was added to prevent. The MAC covering
    the label does not help — the label is authentic, it just does not render
    as what it is.
    """
    token = _mint("user-a-id", label)
    assert label not in token
    assert token.isprintable()
    assert "user-a-id" in token.split(":")


def test_empty_identity_falls_back_to_user_id():
    token = _mint("user-a-id", None)
    assert "user-a-id" in token.split(":")


@pytest.mark.asyncio
async def test_challenge_endpoint_labels_the_token_with_the_caller_identity(harness):
    """The label source, pinned at the endpoint: ``preferred_username`` is
    preferred over ``email``, and the fallback is the user id — the whole
    anti-phishing property depends on the label naming the signer's account.
    """
    harness.user = {
        "id": "00000000-0000-0000-0000-0000000000aa",
        "preferred_username": "alice",
        "email": "alice@example.com",
    }
    named = await ssh_access_routes.create_ssh_key_challenge(
        request=object(), dependencies=harness.dependencies
    )
    assert "alice" in named["challenge"].split(":")

    harness.user = {
        "id": "00000000-0000-0000-0000-0000000000bb",
        "email": "bob@example.com",
    }
    by_email = await ssh_access_routes.create_ssh_key_challenge(
        request=object(), dependencies=harness.dependencies
    )
    assert "bob@example.com" in by_email["challenge"].split(":")

    harness.user = {"id": "00000000-0000-0000-0000-0000000000cc"}
    bare = await ssh_access_routes.create_ssh_key_challenge(
        request=object(), dependencies=harness.dependencies
    )
    assert "00000000-0000-0000-0000-0000000000cc" in bare["challenge"].split(":")


# =============================================================================
# Fix round 1 — Minor 5: fail-closed lives on the helpers, not just callers
# =============================================================================


def test_mint_raises_when_secret_is_empty():
    with pytest.raises(RuntimeError):
        ssh_access_operations.mint_ssh_key_challenge("user-a-id", "alice", secret="")


def test_verify_returns_false_when_secret_is_empty():
    token = _mint("user-a-id", "alice")
    assert (
        ssh_access_operations.verify_ssh_key_challenge(token, "user-a-id", secret="")
        is False
    )


# ---------------------------------------------------------------------------
# §6.3 account-security notification (workspace_ssh_access.md): a key added
# by someone else — a stolen session, a shared account — must stay visible
# to its owner. Best-effort (the service wraps the call in try/except), so
# these pin the happy-path call, not failure handling — a broken notify path
# already can't fail registration (see the wrapping try/except itself).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_adding_a_key_records_exactly_one_notification(
    harness, approved_user, monkeypatch
):
    challenge = await ssh_access_routes.create_ssh_key_challenge(
        request=object(), dependencies=harness.dependencies
    )
    monkeypatch.setattr(
        ssh_access_operations, "parse_public_key", _parsed("SHA256:" + "C" * 43)
    )
    monkeypatch.setattr(
        ssh_access_operations, "verify_possession", lambda *a, **k: True
    )

    async def _create(**kwargs):
        return {
            "id": "k-notify-1",
            "name": kwargs["name"],
            "key_type": "ssh-ed25519",
            "fingerprint_sha256": "SHA256:" + "C" * 43,
            "created_at": None,
            "last_used_at": None,
            "disabled_at": None,
        }

    harness.store.set("create_user_ssh_key", _create)

    body = _Body(
        name="laptop",
        public_key="ssh-ed25519 AAAA",
        challenge=challenge["challenge"],
        signature="-----BEGIN SSH SIGNATURE-----",
    )
    await ssh_access_routes.create_ssh_key(
        request=object(), body=body, dependencies=harness.dependencies
    )

    assert len(harness.notifier.records) == 1
    kwargs = harness.notifier.records[0]
    assert kwargs["category"] == "ssh_key_added"
    assert kwargs["recipient_id"] == approved_user["id"]
    assert kwargs["dedup_key"] == "ssh_key_added:k-notify-1"


@pytest.mark.asyncio
async def test_adding_the_same_key_twice_does_not_record_two_notifications(
    harness, approved_user, monkeypatch
):
    """The second attempt never reaches the notify call at all: the store's
    fingerprint-uniqueness constraint rejects it first (409, same as
    ``test_challenge_is_reusable_but_duplicate_key_is_rejected_by_fingerprint``),
    so there is only ever one row to notify about."""
    from orchestrator.database.postgres import SshKeyAlreadyRegistered

    challenge = await ssh_access_routes.create_ssh_key_challenge(
        request=object(), dependencies=harness.dependencies
    )
    monkeypatch.setattr(
        ssh_access_operations, "parse_public_key", _parsed("SHA256:" + "D" * 43)
    )
    monkeypatch.setattr(
        ssh_access_operations, "verify_possession", lambda *a, **k: True
    )

    calls = {"n": 0}

    async def _create(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return {
                "id": "k-notify-2",
                "name": kwargs["name"],
                "key_type": "ssh-ed25519",
                "fingerprint_sha256": "SHA256:" + "D" * 43,
                "created_at": None,
                "last_used_at": None,
                "disabled_at": None,
            }
        raise SshKeyAlreadyRegistered("SHA256:" + "D" * 43)

    harness.store.set("create_user_ssh_key", _create)

    body = _Body(
        name="laptop",
        public_key="ssh-ed25519 AAAA",
        challenge=challenge["challenge"],
        signature="-----BEGIN SSH SIGNATURE-----",
    )
    await ssh_access_routes.create_ssh_key(
        request=object(), body=body, dependencies=harness.dependencies
    )
    with pytest.raises(HTTPException) as excinfo:
        await ssh_access_routes.create_ssh_key(
            request=object(), body=body, dependencies=harness.dependencies
        )
    assert excinfo.value.status_code == 409

    assert len(harness.notifier.records) == 1


@pytest.mark.asyncio
async def test_ssh_key_added_category_is_registered_high_severity():
    """Registered in the catalog (not just called ad hoc), and `high` —
    the account-security class, same reason a "new sign-in" mail is loud."""
    from orchestrator.services import notification_catalog as cat

    spec = cat.category_spec("ssh_key_added")
    assert spec.severity == "high"
    # Ruling P-12 (fix round 1): source-less categories need a resolving
    # action or the row can never leave `pending`, so this declares
    # ACTION_OPEN_SOURCE rather than shipping with none.
    assert [a.type for a in spec.actions] == ["open"]


@pytest.mark.asyncio
async def test_ssh_key_added_open_action_navigates_and_resolves():
    """The declared `open` action is the ONLY way an `ssh_key_added` row
    ever leaves `pending` — no source_kind is registered for it, so nothing
    else can resolve it. Also pins the destination: Settings → SSH Keys is
    where a user revokes a key they didn't add."""
    from orchestrator.services.notification_actions import (
        register_notification_actions,
    )
    from orchestrator.services.notification_catalog import ActionContext, action_handler

    register_notification_actions(
        dependencies=workflows_composition.notification_action_dependencies(
            orchestrator.main.app.state.resources
        )
    )
    handler = action_handler("ssh_key_added", "open")
    assert handler is not None

    result = await handler(ActionContext(notification={}, user={"id": "u1"}, params={}))
    assert result.resolve is True
    assert result.result == {"navigate": "/settings/ssh-keys"}


@pytest.mark.asyncio
async def test_ssh_key_notification_failure_does_not_fail_registration(
    harness, approved_user, monkeypatch
):
    """Best-effort: the key is already durably written by the time this
    runs, so a notify-path exception must never surface as a failed
    registration."""
    challenge = await ssh_access_routes.create_ssh_key_challenge(
        request=object(), dependencies=harness.dependencies
    )
    monkeypatch.setattr(
        ssh_access_operations, "parse_public_key", _parsed("SHA256:" + "E" * 43)
    )
    monkeypatch.setattr(
        ssh_access_operations, "verify_possession", lambda *a, **k: True
    )

    async def _create(**kwargs):
        return {
            "id": "k-notify-3",
            "name": kwargs["name"],
            "key_type": "ssh-ed25519",
            "fingerprint_sha256": "SHA256:" + "E" * 43,
            "created_at": None,
            "last_used_at": None,
            "disabled_at": None,
        }

    harness.store.set("create_user_ssh_key", _create)

    class _BrokenNotifier:
        async def record(self, **kwargs):
            raise RuntimeError("notification service unavailable")

    harness.notifier = _BrokenNotifier()

    body = _Body(
        name="laptop",
        public_key="ssh-ed25519 AAAA",
        challenge=challenge["challenge"],
        signature="-----BEGIN SSH SIGNATURE-----",
    )
    result = await ssh_access_routes.create_ssh_key(
        request=object(), body=body, dependencies=harness.dependencies
    )
    assert result["id"] == "k-notify-3"
