"""Possession-verified SSH key registration, target resolution and attach audit.

Registration requires proving possession: without it, anyone could claim a
public key they merely read (e.g. published at github.com/<user>.keys), and
because fingerprints are globally unique that would deny the rightful owner
the ability to register their own key.

The possession challenge is a STATELESS, HMAC-signed token, not a lookup in
an in-process dict. The orchestrator runs multiple replicas behind one
Service with no session affinity (deployment/values-experimental.yaml
`orchestrator.replicas: 2` on dev, deliberately — it's where the HA posture
gets exercised), so the pod that issues a challenge is frequently not the
pod that redeems it. An in-process store would reject roughly half of all
registrations with "unknown challenge" — a coin flip, not a rare race. See
ruling F24 in
.superpowers/sdd/2026-08-28-workspace-ssh-access-foundation/progress.md.

AUTHORIZATION IS NOT HERE. Every entry point takes an already-authenticated
``user``, or an already-resolved ``(thread_id, user)`` pair, because the HTTP
adapter owns the gate and — for the internal gateway endpoints — owns the
opaque-404 identity resolution whose *sameness across failure modes* is the
security property. What this module owns is the challenge MAC, the response
projections, the host-key cache and the store writes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import functools
import hashlib
import hmac
import logging
import os
import secrets
import time
from collections.abc import Callable
from typing import Any, Protocol

import asyncpg
from fastapi import HTTPException

from orchestrator.database.postgres import (
    SshKeyAlreadyRegistered,
    SshKeyLimitReached,
)
from orchestrator.services.canvas_ssh import (
    CanvasSSHError,
    RemoteWorkspaceTarget,
    bound_workspace_generation,
    remote_target_is_vm_backed,
    resolve_remote_workspace_target,
)
from orchestrator.services.ssh_gateway_targets import (
    STATE_LIVE,
    STATE_STALE_BINDING,
    STATE_VM_UNSUPPORTED,
    resolve_workspace_state,
)
from orchestrator.services.ssh_gateway_token import mint_attach_token
from orchestrator.services.ssh_public_keys import (
    SIGNATURE_NAMESPACE,
    SshKeyRejected,
    parse_public_key,
    verify_possession,
)
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from orchestrator.schemas.ssh_access import SshKeyCreate


class SshAccessStore(Protocol):
    """The store methods this domain reaches, and nothing else."""

    async def create_user_ssh_key(
        self,
        user_id: str,
        name: str,
        key_type: str,
        public_key: str,
        fingerprint_sha256: str,
    ) -> dict[str, Any]: ...

    async def list_user_ssh_keys(self, user_id: str) -> list[dict[str, Any]]: ...

    async def delete_user_ssh_key(self, key_id: str, user_id: str) -> bool: ...

    async def get_thread(self, thread_id: str) -> dict[str, Any] | None: ...

    async def mark_ssh_key_used(self, key_id: str, fingerprint: str) -> None: ...

    async def record_ssh_attachment(
        self,
        thread_id: str,
        user_id: str,
        ssh_key_id: str | None,
        client_ip: str | None,
        handle: str,
    ) -> str: ...

    async def close_ssh_attachment(
        self, attachment_id: str, channels: list[str]
    ) -> int: ...


class SshKeyNotifier(Protocol):
    async def record(self, **kwargs: Any) -> Any: ...


# =============================================================================
# Possession challenge
# =============================================================================

SSH_CHALLENGE_TTL_SECONDS = 300
SSH_CHALLENGE_VERSION = "srw-ssh1"


SSH_CHALLENGE_IDENTITY_MAX_LEN = 255


def mint_ssh_key_challenge(
    user_id: str,
    identity: str | None = None,
    *,
    secret: str,
    now: float | None = None,
) -> tuple[str, float]:
    """Mint a possession challenge bound to ``user_id``.

    Returns ``(token, expires_at_unix_ts)``. The token is the exact string
    the caller signs with ``ssh-keygen -Y sign``, so its wire format —
    version tag, nonce, user id, expiry, a human-readable identity label and
    an HMAC-SHA256 signature, colon-joined — is deliberately restricted to
    printable ASCII with no whitespace. The separator is ``:``, not ``.``:
    emails contain dots, and a dot-separated identity would make the field
    split in ``verify_ssh_key_challenge`` ambiguous. ``identity`` may itself
    contain colons (an email never does, but nothing here depends on that);
    parsing uses ``rsplit``/``split`` with explicit counts so the identity
    field absorbs anything after its position instead of being cut short.

    ``identity`` (pass ``preferred_username`` or ``email``; the caller
    chooses) exists so a signer who actually reads what ``ssh-keygen -Y
    sign`` is about to sign can see whose account the token binds — a bare
    UUID gives a phishing target no such visibility (Mallory mints a
    challenge for her own account, gets Victoria to sign *that* token via a
    page imitating the registration flow, then posts Victoria's public key
    with her own token and Victoria's signature: every check passes and the
    row lands on Mallory's account). The label is covered by the HMAC below
    (so it can't be swapped after minting without invalidating the token)
    but is DISPLAY-ONLY: authorization in ``verify_ssh_key_challenge`` is
    decided entirely by ``user_id``, never by ``identity`` — flipping the
    label to name a different account does not move who the token
    authenticates as. Sanitized here — non-empty, ASCII, no whitespace,
    bounded length — with a fall back to the raw user id, since a
    malicious or malformed label must not be able to break the token's
    single-line-printable-ASCII contract (that contract is what lets
    ``verify_ssh_key_challenge`` use ``str.isascii()`` as its first,
    cheap line of defense).

    **The label's anti-phishing property has one undocumented dependency:
    ``preferred_username`` and ``email`` are unique per Keycloak realm.** The
    mitigation is "the signer recognises the name as their own and not
    someone else's", which only works while a label names exactly one
    account. A deployment that ever admits duplicate usernames — a second
    identity provider federated in, a realm merge, a source change to a
    non-unique field like ``display_name`` — reverts this to a bare UUID's
    worth of protection, silently: no test here can see it, because every
    test constructs its own label. If the label source changes, re-derive
    that uniqueness claim first.

    Not single-use, deliberately (ruling F24). A nonce store would give
    literal single-use, but that isn't the property this token needs:
    binding it to the caller's user id is what carries the actual security
    guarantee. The attack single-use would prevent is replaying a captured
    (challenge, signature) pair to register someone else's public key under
    the attacker's account — the lockout risk this whole possession check
    exists to prevent, since SSH key fingerprints are globally unique. A
    token minted for user A can never redeem for user B (checked in
    ``verify_ssh_key_challenge``), and a user replaying their own token just
    re-registers their own key, which the fingerprint's uniqueness
    constraint already rejects (409, see ``register_key``). Do not "fix"
    this back into a stateful nonce store: every orchestrator replica shares
    SESSION_JWT_SECRET via one Kubernetes Secret, but replicas do not share
    memory, which is the whole reason this token is stateless.

    Fails closed: raises if ``secret`` is empty rather than signing with a
    well-known-empty key. Both HTTP callers already guard this before
    calling in (and keep their own 503), but the precondition lives here
    too, on the reusable unit itself, since the ssh-gateway is expected to
    call this directly, outside any HTTP request/response cycle where a 503
    would even make sense.
    """
    if not secret:
        raise RuntimeError(
            "SESSION_JWT_SECRET is empty; refusing to mint a forgeable SSH "
            "possession challenge."
        )
    if now is None:
        now = time.time()
    label = (identity or "").strip()
    if (
        not label
        or len(label) > SSH_CHALLENGE_IDENTITY_MAX_LEN
        or not label.isascii()
        or not label.isprintable()
        or any(ch.isspace() for ch in label)
    ):
        label = user_id
    expires_at = now + SSH_CHALLENGE_TTL_SECONDS
    nonce = secrets.token_urlsafe(24)
    head = f"{SSH_CHALLENGE_VERSION}:{nonce}:{user_id}:{int(expires_at)}:{label}"
    signature = hmac.new(
        secret.encode("utf-8"),
        head.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{head}:{signature}", expires_at


def verify_ssh_key_challenge(
    token: str, user_id: str, *, secret: str, now: float | None = None
) -> bool:
    """True iff ``token`` was minted by us, for ``user_id``, and is unexpired.

    Check order is deliberate and load-bearing:

    1. ``token.isascii()``. ``hmac.compare_digest`` raises ``TypeError`` on
       a non-ASCII ``str`` argument — reachable pre-authentication, since
       the raw signature field is compared before its validity is known —
       so a body like ``{"challenge": "srw-ssh1:...:é", ...}`` would
       otherwise turn a 400 into an unhandled 500 an authenticated caller
       could loop. A lone UTF-16 surrogate (reachable via ``json.loads`` on
       a hostile body) isn't even ``str``-comparable by ``compare_digest``
       and raises ``UnicodeEncodeError`` on ``.encode()`` instead, so this
       must be a rejection, not a switch to byte comparison.
    2. SESSION_JWT_SECRET must be configured — verifying against an empty
       key would accept a forgery anyone can compute.
    3. The token parses into the expected fields at all.
    4. The HMAC, via ``hmac.compare_digest``, before any field the token
       carries (expiry, embedded user id, identity) is trusted — until the
       MAC checks out those fields are attacker-controlled input, not fact.
    5. Expiry.
    6. The embedded user id against the authenticated caller, also via
       ``hmac.compare_digest``. The identity label is parsed out (it has to
       be, to locate the other fields) but never compared against
       anything: it is a MAC-covered, display-only annotation, not an
       authorization input — only ``user_id`` decides who a token is for.
    """
    if not token.isascii():
        return False
    if not secret:
        return False
    if now is None:
        now = time.time()
    try:
        head, signature = token.rsplit(":", 1)
        version, nonce, token_user_id, expires_at_raw, _identity = head.split(":", 4)
    except ValueError:
        return False
    if version != SSH_CHALLENGE_VERSION:
        return False
    expected_signature = hmac.new(
        secret.encode("utf-8"),
        head.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected_signature, signature):
        return False
    try:
        expires_at = float(expires_at_raw)
    except ValueError:
        return False
    if expires_at <= now:
        return False
    return hmac.compare_digest(token_user_id, str(user_id))


def serialize_ssh_key_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "name": row["name"],
        "key_type": row["key_type"],
        "fingerprint": row["fingerprint_sha256"],
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        "last_used_at": (
            row["last_used_at"].isoformat() if row.get("last_used_at") else None
        ),
        "disabled": row.get("disabled_at") is not None,
    }


# =============================================================================
# Gateway host-key publication
# =============================================================================


class PartialHostKeyRead(Exception):
    """Carries a partial host-key read past ``functools.lru_cache``.

    ``lru_cache`` does not memoize calls that raise, which is exactly the
    behavior wanted here: see ``SshGatewayHostKeyCache.load``.
    """

    def __init__(self, entries: tuple[dict[str, str], ...]) -> None:
        super().__init__("one or more ssh gateway host key paths were unreadable")
        self.entries = entries


# Cap on how much of an operator-supplied path is read. A public key file is
# a few hundred bytes; anything larger is a mis-pointed path, and there is no
# reason to slurp it into memory to fail parsing it.
SSH_HOST_KEY_READ_LIMIT = 65536


def parse_ssh_gateway_host_keys(
    paths_value: str,
    *,
    logger: logging.Logger,
) -> tuple[tuple[dict[str, str], ...], bool]:
    """Read and fingerprint every path in ``paths_value`` (uncached).

    Returns ``(entries, complete)``. ``complete`` is False when any configured
    path failed to read or parse. A bad entry is skipped rather than raising —
    one typo in the list must not take down discovery for the rest — but the
    caller needs to know it happened so it does not memoize a partial answer.
    """
    import asyncssh

    entries: list[dict[str, str]] = []
    complete = True
    for path in (p.strip() for p in paths_value.split(",")):
        if not path:
            continue
        try:
            with open(path, "r", encoding="utf-8") as handle:
                raw = handle.read(SSH_HOST_KEY_READ_LIMIT)
            key = asyncssh.import_public_key(raw)
        except Exception:
            logger.warning("ssh gateway host key unreadable at %s", path)
            complete = False
            continue
        entries.append(
            {
                "type": key.get_algorithm(),
                "public_key": key.export_public_key().decode().strip(),
                "fingerprint": key.get_fingerprint("sha256"),
            }
        )
    return tuple(entries), complete


class SshGatewayHostKeyCache:
    """One gateway host-key parse cache per application.

    Deliberately an *object* rather than a module-level ``lru_cache``. The
    memo it holds is keyed on operator configuration and is read by an
    unauthenticated endpoint, so it must have exactly one owner whose
    lifetime is the application's — a module-level cache would be shared by
    every application constructed in a process (tests mount several) and
    would outlive the composition that configured it. Application
    composition builds one of these and hands it to the dependency factory;
    nothing else may build a second.
    """

    def __init__(
        self,
        *,
        logger: logging.Logger,
        parse: Callable[[str], tuple[tuple[dict[str, str], ...], bool]] | None = None,
        maxsize: int = 8,
    ) -> None:
        self._parse = (
            parse
            if parse is not None
            else functools.partial(parse_ssh_gateway_host_keys, logger=logger)
        )

        @functools.lru_cache(maxsize=maxsize)
        def memoized(paths_value: str) -> tuple[dict[str, str], ...]:
            """Memoize a *complete* parse only; raise on a partial one.

            ``functools.lru_cache`` never caches a call that raises, so
            signalling a partial read with ``PartialHostKeyRead`` is what
            makes the next request retry instead of inheriting the failure.
            """
            entries, complete = self._parse(paths_value)
            if not complete:
                raise PartialHostKeyRead(entries)
            return entries

        self._memoized = memoized

    def load(self, paths_value: str) -> tuple[dict[str, str], ...]:
        """Parse and fingerprint the gateway's public host keys.

        A fully-successful parse is cached on ``paths_value`` itself — the raw
        ``SSH_GATEWAY_PUBLIC_HOST_KEYS`` string, not on nothing.
        ``host_keys`` below is unauthenticated by design, so without that
        cache every anonymous request would drive a fresh blocking ``open()``
        plus asyncssh parse per configured key, on the event loop, with no
        rate limit in front of it. Keying on the env value (rather than
        calling with no arguments) means a changed value — a real config
        update, or a different value monkeypatched in per-test — gets a fresh
        parse instead of a stale hit.

        A partial or failed read is deliberately *not* cached. These files
        arrive on a projected Secret volume, so contrary to what one might
        assume they very much can be absent or in flux while the pod runs: the
        volume may not be projected yet when the first request lands, and a
        read can fall in the window where kubelet swaps the ``..data`` symlink
        during a Secret update. Memoizing that outcome would publish
        ``host_keys: []`` — a hard stop for a pinning client — until the pod
        restarted. Retrying costs one re-read per request, and only for as
        long as the underlying problem lasts.
        """
        try:
            return self._memoized(paths_value)
        except PartialHostKeyRead as partial:
            return partial.entries

    def cache_clear(self) -> None:
        """Drop the memo. Exists for tests and for an operator-facing reload."""
        self._memoized.cache_clear()


@dataclass(frozen=True)
class SshAccessDependencies:
    """Application collaborators, resolved per invocation by composition.

    ``session_jwt_secret`` is a value, not a getter, precisely because the
    factory that builds this dataclass is what re-reads it: a module that
    cached the secret at import would keep signing with a stale one.
    """

    store: SshAccessStore
    session_jwt_secret: str
    notifier: SshKeyNotifier
    logger: logging.Logger
    host_keys: SshGatewayHostKeyCache
    # Injected rather than imported: its home module drags the container/VM
    # provisioner import chain (and therefore the Kubernetes SDK) behind it,
    # which has no business inside a credential-registration leaf module.
    thread_is_vm_tier: Callable[[dict, dict, dict], bool]


# =============================================================================
# Key registration
# =============================================================================


async def create_challenge(
    *, user: dict[str, Any], dependencies: SshAccessDependencies
) -> dict[str, Any]:
    """Issue the nonce the caller must sign with the private half of its key."""
    if not dependencies.session_jwt_secret:
        # Fail closed: an empty secret is a well-known HMAC key, so every
        # replica would sign forgeable tokens. Application startup only logs a
        # warning for this today — do not rely on that here.
        raise HTTPException(
            status_code=503,
            detail="SSH key registration is temporarily unavailable.",
        )
    identity = user.get("preferred_username") or user.get("email")
    token, expires_at = mint_ssh_key_challenge(
        str(user["id"]), identity, secret=dependencies.session_jwt_secret
    )
    return {
        "challenge": token,
        "namespace": SIGNATURE_NAMESPACE,
        "expires_at": datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat(),
    }


async def register_key(
    *,
    user: dict[str, Any],
    body: SshKeyCreate,
    dependencies: SshAccessDependencies,
) -> dict[str, Any]:
    """Verify possession, then durably register the public key."""
    if not dependencies.session_jwt_secret:
        raise HTTPException(
            status_code=503,
            detail="SSH key registration is temporarily unavailable.",
        )
    if not verify_ssh_key_challenge(
        body.challenge, str(user["id"]), secret=dependencies.session_jwt_secret
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Challenge is unknown, expired, or was issued to a "
                "different account. Request a new one."
            ),
        )

    try:
        parsed = parse_public_key(body.public_key)
    except SshKeyRejected as exc:
        raise HTTPException(status_code=400, detail=exc.reason) from exc

    if not verify_possession(
        parsed.public_key,
        SIGNATURE_NAMESPACE,
        body.challenge.encode("utf-8"),
        body.signature,
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Could not verify possession of this key. Sign the challenge "
                "with the matching private key and paste the signature."
            ),
        )

    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Give the key a name.")

    try:
        row = await dependencies.store.create_user_ssh_key(
            user_id=str(user["id"]),
            name=name,
            key_type=parsed.key_type,
            public_key=parsed.public_key,
            fingerprint_sha256=parsed.fingerprint_sha256,
        )
    except SshKeyAlreadyRegistered as exc:
        raise HTTPException(
            status_code=409,
            detail=(
                "This key is already registered, possibly on another account. "
                "Register a different key, or contact support to have the "
                "existing registration released."
            ),
        ) from exc
    except SshKeyLimitReached as exc:
        # Also 409, but a different recovery path from the duplicate above:
        # this one you can fix yourself. Name the cap — a refusal that does
        # not say what the limit is reads as a bug.
        raise HTTPException(
            status_code=409,
            detail=(
                f"You already have the maximum of {exc.limit} registered SSH "
                "keys. Delete one before adding another."
            ),
        ) from exc

    # Account-security signal (workspace_ssh_access.md §6.3): a key added by
    # someone else — a stolen session, a shared account — must stay visible
    # to its owner, the same reason a "new sign-in" mail is loud. Best-effort
    # and non-fatal, like every other notify-after-write call site: the key is
    # already durably registered, so a feed-write hiccup must never turn into
    # a failed registration.
    try:
        await dependencies.notifier.record(
            recipient_id=str(user["id"]),
            category="ssh_key_added",
            # Per-key, not per-user-per-day: a replayed request must collapse
            # onto the same row, but a second, distinct key added minutes
            # later is its own event.
            dedup_key=f"ssh_key_added:{row['id']}",
            subject=f"New SSH key added: {name}",
            body=(
                f"The SSH key **{name}** ({parsed.key_type}, "
                f"`{parsed.fingerprint_sha256}`) was added to your account. "
                "If this wasn't you, remove it from Settings → SSH Keys "
                "immediately and rotate anything it may have reached."
            ),
            source_kind="ssh_key",
            source_id=str(row["id"]),
            payload={
                "ssh_key_id": str(row["id"]),
                "name": name,
                "fingerprint": parsed.fingerprint_sha256,
                "key_type": parsed.key_type,
            },
        )
    except Exception:
        dependencies.logger.warning(
            "ssh_key_added notification for key %s failed (non-fatal)",
            str(row["id"])[:8],
            exc_info=True,
        )
    return serialize_ssh_key_row(row)


async def list_keys(
    *, user: dict[str, Any], dependencies: SshAccessDependencies
) -> list[dict[str, Any]]:
    """The current user's registered SSH public keys."""
    rows = await dependencies.store.list_user_ssh_keys(str(user["id"]))
    return [serialize_ssh_key_row(r) for r in rows]


async def delete_key(
    *, user: dict[str, Any], key_id: str, dependencies: SshAccessDependencies
) -> dict[str, str]:
    """Remove one of the current user's keys."""
    try:
        deleted = await dependencies.store.delete_user_ssh_key(key_id, str(user["id"]))
    except ValueError:
        # key_id isn't a well-formed UUID — the store's UUID(key_id) raises.
        # Same "no such key" outcome as a well-formed id matching no row;
        # not a 500.
        deleted = False
    if not deleted:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"status": "deleted"}


async def create_attach_token(
    *, user: dict[str, Any], dependencies: SshAccessDependencies
) -> dict[str, Any]:
    """Mint the short-lived credential the gateway's WSS front door wants."""
    if not dependencies.session_jwt_secret:
        raise HTTPException(
            status_code=503,
            detail="SSH access is temporarily unavailable.",
        )
    token, expires_at = mint_attach_token(
        str(user["id"]), dependencies.session_jwt_secret
    )
    return {
        "token": token,
        "expires_at": datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat(),
    }


async def host_keys(*, dependencies: SshAccessDependencies) -> dict[str, Any]:
    """The gateway's public host keys plus its hostname, or empty when unset."""
    entries = dependencies.host_keys.load(
        os.environ.get("SSH_GATEWAY_PUBLIC_HOST_KEYS") or ""
    )
    return {
        "host_keys": [dict(entry) for entry in entries],
        "hostname": os.environ.get("SSH_GATEWAY_HOSTNAME", ""),
    }


# =============================================================================
# Target resolution and attachment audit
# =============================================================================


def ssh_target_response(
    thread_id: str,
    user: dict[str, Any],
    state: str,
    target: RemoteWorkspaceTarget | None = None,
) -> dict[str, Any]:
    """The one response shape every ``resolve_target`` branch returns, so a
    new field can't be added to three branches out of four.

    ``pod_ip`` is a misnomer kept on purpose. The value is
    ``RemoteWorkspaceTarget.host``, which comes from ``workspace_container.
    ssh_host`` — a Kubernetes Service DNS name, not a pod IP. Dialing a pod IP
    was an explicitly rejected earlier design (it breaks the moment the pod is
    recreated, and the provisioner's attested identity is bound to the
    Service). The name stays because the gateway plan already reads this
    field by it; do not "fix" it into something that suggests the gateway
    should resolve or trust an IP.

    ``ssh_key_id`` comes from ``user`` because ``resolve_user_by_ssh_
    fingerprint`` already resolves it (it's the matched ``user_ssh_keys.id``,
    joined in for exactly this reason). ``.get`` rather than a required key:
    every real caller passes the dict that resolver returns, but tests and
    any future caller that only has a plain ``{"id": ...}`` must not crash
    this helper for it.

    NOT load-bearing for ``POST /api/internal/ssh-attachments``: that
    endpoint re-resolves ``ssh_key_id`` server-side itself rather than
    trusting a value the gateway echoes back (fix round 1, Important 1 —
    accepting it from the caller would be exactly the "internal key plus an
    asserted identity" this domain's own docstring rules out). This field
    stays because it is already built and tested, leaks nothing beyond the
    ``user_id`` already on this response, and is useful for gateway-side
    logging — but nothing should come to depend on it for anything other
    than display/logging.
    """
    ssh_key_id = user.get("ssh_key_id")
    return {
        "thread_id": thread_id,
        "user_id": str(user["id"]),
        "ssh_key_id": str(ssh_key_id) if ssh_key_id else None,
        "pod_ip": target.host if target else None,
        "pod_port": target.port if target else None,
        "host_key_fingerprint": target.fingerprint if target else None,
        "state": state,
    }


async def resolve_target(
    *,
    thread_id: str,
    user: dict[str, Any],
    dependencies: SshAccessDependencies,
) -> dict[str, Any]:
    """Project an already-authorized ``(thread, user)`` pair to a gateway target.

    The caller has already resolved and authorized ``thread_id`` — including
    its opaque 404 for every unauthorized outcome — so the missing-thread
    branch here reuses that same opaque 404 rather than inventing a
    distinguishable one.

    A resolvable-but-not-live workspace returns a non-live ``state`` and
    ``pod_ip: None``, so the gateway can print a readable reason.
    """
    thread = await dependencies.store.get_thread(thread_id)
    return resolve_target_from_thread(
        thread=thread, thread_id=thread_id, user=user, dependencies=dependencies
    )


def resolve_target_from_thread(
    *,
    thread: dict[str, Any] | None,
    thread_id: str,
    user: dict[str, Any],
    dependencies: SshAccessDependencies,
) -> dict[str, Any]:
    """Project the provided Thread snapshot; the native route also describes it."""
    opaque = HTTPException(status_code=404, detail="No such workspace")

    if not thread:
        raise opaque
    metadata = thread_metadata_object(thread)

    ws_ctx = metadata.get("workspace_container") or {}
    vm_ctx = metadata.get("vm") or {}
    if dependencies.thread_is_vm_tier(metadata, ws_ctx, vm_ctx):
        return ssh_target_response(thread_id, user, STATE_VM_UNSUPPORTED)

    state = resolve_workspace_state(thread, metadata)
    if state != STATE_LIVE:
        return ssh_target_response(thread_id, user, state)

    try:
        target = resolve_remote_workspace_target(
            thread, bound_workspace_generation(thread)
        )
    except CanvasSSHError as exc:
        # workspace_container.status IS "ready" here — resolve_workspace_
        # state already proved that via STATE_LIVE above — so this is a
        # provisioned workspace with an unusable SSH attestation (missing/
        # stale _workspace_binding, bad host key), not an absent workspace.
        # STATE_STALE_BINDING says so; folding it into STATE_NEVER_
        # PROVISIONED would send an operator after the wrong problem.
        dependencies.logger.info("ssh-target unresolvable for %s: %s", thread_id, exc)
        return ssh_target_response(thread_id, user, STATE_STALE_BINDING)

    # The guard above and the resolver disagree about what "VM tier" means,
    # and the disagreement hands out the wrong host. thread_is_vm_tier reads
    # the DECLARED backend once a container status is present, while
    # resolve_remote_workspace_target prefers metadata.vm whenever its status
    # is "ready" regardless of workspace_container. Upgrade-to-VM writes
    # metadata.vm without rewriting config_override.workspace.backend (that
    # method's own docstring says so), so a thread with both contexts ready
    # and a stale non-VM backend passes the guard and is then handed the VM's
    # host and port — which v1 must refuse. Ask the resolver's own question,
    # after the fact, rather than re-deriving the tier a third way.
    if remote_target_is_vm_backed(thread):
        return ssh_target_response(thread_id, user, STATE_VM_UNSUPPORTED)

    return ssh_target_response(thread_id, user, STATE_LIVE, target)


async def mark_key_used(
    *,
    user: dict[str, Any] | None,
    fingerprint: str,
    dependencies: SshAccessDependencies,
) -> dict[str, str]:
    """Stamp ``last_used_at`` on the key behind an already-resolved fingerprint.

    An unresolved fingerprint is a quiet no-op success, not a 404: the caller
    has already authenticated against *some* key by the time this fires, and
    a 404 here would turn this endpoint into an existence oracle for
    registered keys (identical reasoning to ``resolve_target``'s opaque
    404s).
    """
    key_id = user.get("ssh_key_id") if user else None
    if key_id:
        try:
            await dependencies.store.mark_ssh_key_used(str(key_id), fingerprint)
        except Exception:
            # mark_ssh_key_used's own docstring: "a failed bump must not
            # discard an authentication that already succeeded." The
            # gateway calls this after key.verify has already passed, so a
            # transient DB hiccup on the bump itself must not turn into a
            # 500 for what is, from the caller's side, a fire-and-forget
            # bookkeeping call.
            dependencies.logger.warning(
                "mark_ssh_key_used failed for a resolved key; last_used_at not bumped",
                exc_info=True,
            )
    return {"status": "ok"}


async def record_attachment(
    *,
    thread_id: str,
    user: dict[str, Any],
    handle: str,
    client_ip: str | None,
    dependencies: SshAccessDependencies,
) -> dict[str, str]:
    """Open an SSH-attachment audit row for an already-authorized pair.

    A foreign-key violation on the insert (thread, user or key deleted
    between the caller's resolution and the write below — a real race, e.g. a
    thread torn down mid-session, not just a probe) and a malformed
    ``handle`` surfacing from ``record_ssh_attachment`` both map to 400
    rather than an unhandled 500.
    """
    ssh_key_id = user.get("ssh_key_id")
    try:
        attachment_id = await dependencies.store.record_ssh_attachment(
            thread_id,
            str(user["id"]),
            str(ssh_key_id) if ssh_key_id else None,
            client_ip,
            handle,
        )
    except (ValueError, asyncpg.ForeignKeyViolationError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"attachment_id": attachment_id}


async def close_attachment(
    *,
    attachment_id: str,
    channels: list[str],
    dependencies: SshAccessDependencies,
) -> dict[str, int]:
    """Stamp detach time on an already-authorized attachment row."""
    try:
        closed = await dependencies.store.close_ssh_attachment(attachment_id, channels)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"closed": closed}
