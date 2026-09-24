"""HTTP adapters for SSH key registration, target resolution and attach audit.

Identity resolution for the three internal gateway endpoints deliberately
lives HERE, inline, rather than behind a shared helper in the service: the
security property is that unknown handle, unknown key and "not yours" are
indistinguishable, and that only holds while the resolution and the opaque
404 sit in the same place a reviewer reads. The duplication is the point.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
import os
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request

from orchestrator.schemas.ssh_access import (
    SshAttachmentClose,
    SshAttachmentCreate,
    SshKeyCreate,
    SshKeyUsedRequest,
    VMGatewayAccessRequest,
)
from orchestrator.security.access import (
    require_internal,
    require_personal_scope,
    user_can_access_ide_entity,
)
from orchestrator.security.auth import require_approved_user
from orchestrator.services import ssh_access
from orchestrator.services.ssh_handles import is_valid_handle
from orchestrator.services.ssh_gateway_vm_access_proof import verify_vm_access_proof
from orchestrator.services.vm_ssh_access_binding import vm_binding_digest

router = APIRouter()


@dataclass(frozen=True)
class SshAccessDependencies:
    store: Any
    operations: ssh_access.SshAccessDependencies
    require_approved_user: Callable[..., Awaitable[Any]] = require_approved_user
    require_internal: Callable[..., Awaitable[Any]] = require_internal
    require_personal_scope: Callable[..., Awaitable[None]] = require_personal_scope
    user_can_access_ide_entity: Callable[..., Awaitable[bool]] = (
        user_can_access_ide_entity
    )
    vm_access_store: Any = None
    vm_provisioner: Any = None


def get_ssh_access_dependencies(request: Request) -> SshAccessDependencies:
    return request.app.state.ssh_access_dependencies_factory()


# =============================================================================
# SSH Key Endpoints — possession-verified public key registration
# =============================================================================


@router.post("/api/ssh-keys/challenge")
async def create_ssh_key_challenge(
    request: Request,
    *,
    dependencies: SshAccessDependencies = Depends(get_ssh_access_dependencies),
) -> dict[str, Any]:
    """Issue a nonce the caller must sign with the private half of the key they
    are registering.

    Without proof of possession, anyone could claim a public key they merely
    read — and since fingerprints are globally unique, that denies the
    rightful owner the ability to register their own key.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await ssh_access.create_challenge(
        user=user, dependencies=dependencies.operations
    )


@router.post("/api/ssh-keys")
async def create_ssh_key(
    request: Request,
    body: SshKeyCreate,
    *,
    dependencies: SshAccessDependencies = Depends(get_ssh_access_dependencies),
) -> dict[str, Any]:
    """Register an SSH public key after verifying possession.

    Scope-gated: a ``project:<uuid>``-scoped MCP token is refused here even
    though it authenticates a real, approved user. That token is already
    denied every thread by ``user_can_access_job_or_thread`` — the IDE
    included — but an SSH key authenticates by fingerprint, so resolution
    never sees the token that registered it. Without this gate the token
    could mint a credential opening a shell on every session its owner has,
    and one that outlives the token: revoking the PAT does not revoke the key.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    await dependencies.require_personal_scope(
        request, dependencies.store, user, resource_type="ssh_key"
    )
    return await ssh_access.register_key(
        user=user, body=body, dependencies=dependencies.operations
    )


@router.get("/api/ssh-keys")
async def list_ssh_keys(
    request: Request,
    *,
    dependencies: SshAccessDependencies = Depends(get_ssh_access_dependencies),
) -> list[dict[str, Any]]:
    """The current user's registered SSH public keys.

    Scope-gated like its create/delete siblings. The delta a project-scoped
    MCP token gains here is only read access — names, fingerprints and
    last_used_at, with no id it can act on, since delete is gated too. It is
    gated anyway because ``_scope_permits_personal`` says such a token
    "shouldn't be able to read or mutate" a personal resource, and
    ``user_can_access_job_or_thread`` enforces exactly that for reads. Leaving
    one of the three open would make this helper's own call sites disagree
    with the contract they cite.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    await dependencies.require_personal_scope(
        request, dependencies.store, user, resource_type="ssh_key", resource_id=None
    )
    return await ssh_access.list_keys(user=user, dependencies=dependencies.operations)


@router.delete("/api/ssh-keys/{key_id}")
async def delete_ssh_key(
    request: Request,
    key_id: str,
    *,
    dependencies: SshAccessDependencies = Depends(get_ssh_access_dependencies),
) -> dict[str, str]:
    """Remove one of the current user's keys.

    Scope-gated for the same reason as ``create_ssh_key``: a project-scoped
    MCP token has no business reaching a personal credential, in either
    direction. Revocation is the only control a user has over a key (there is
    no disable endpoint), so leaving delete open while gating create would let
    a project-scoped token strip its owner's access.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    await dependencies.require_personal_scope(
        request, dependencies.store, user, resource_type="ssh_key", resource_id=key_id
    )
    return await ssh_access.delete_key(
        user=user, key_id=key_id, dependencies=dependencies.operations
    )


@router.post("/api/ssh/attach-token")
async def create_ssh_attach_token(
    request: Request,
    *,
    dependencies: SshAccessDependencies = Depends(get_ssh_access_dependencies),
) -> dict[str, Any]:
    """Mint the short-lived credential the SSH gateway's WSS front door wants.

    This is the ONLY thing a user should ever present to
    ``wss://.../api/ssh/attach``. It is emphatically not ``MCP_INTERNAL_KEY``:
    that value is the platform's service-to-service credential, guarding ~50
    ``require_internal`` endpoints, and an earlier draft of the gateway
    compared the user's bearer token against it — which would have put the
    master key in every SSH user's ``~/.config/srw/token`` (ruling G38; see
    ``services/ssh_gateway_token.py`` for the full account).

    Stateless and user-bound, the same shape as the SSH-key registration
    challenge above and for the same reason: the gateway is a separate
    Deployment from this one, and this one runs ``replicas: 2`` with no
    session affinity, so there is nowhere to keep a nonce that both sides can
    see. The two tokens are minted with the same secret and kept apart by a
    version clause inside the MAC.

    Scope-gated like ``create_ssh_key``: a ``project:<uuid>``-scoped MCP token
    is refused. The token opens a transport into every workspace its holder's
    registered keys can reach, and by the time the SSH layer authorizes (by
    key fingerprint) the MCP token's scope no longer exists to check.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    await dependencies.require_personal_scope(
        request, dependencies.store, user, resource_type="ssh_attach_token"
    )
    return await ssh_access.create_attach_token(
        user=user, dependencies=dependencies.operations
    )


# nosec: public ssh-host-key-pinning (host keys are public material; client needs them before it can authenticate anything)
@router.get("/api/ssh/host-keys")
async def get_ssh_host_keys(
    request: Request,
    *,
    dependencies: SshAccessDependencies = Depends(get_ssh_access_dependencies),
) -> dict[str, Any]:
    """Publish the SSH gateway's host keys so client tooling can pin them
    without a human comparing fingerprints by eye (Gitpod's
    ``/_ssh/host_keys``, Tailscale's control-plane distribution).

    Deliberately unauthenticated: host keys are public, and the client needs
    this response before it has anything to authenticate against yet. This
    runs in the orchestrator, not the gateway process — it reads
    ``SSH_GATEWAY_PUBLIC_HOST_KEYS`` (comma-separated public-key file paths)
    and ``SSH_GATEWAY_HOSTNAME`` from its own environment, which the gateway
    Deployment's operator is responsible for keeping in sync with the
    gateway's actual ``SSH_GATEWAY_HOST_KEYS`` (private-key) configuration.

    A path that doesn't parse as a key (missing, unreadable, garbage) is
    logged and skipped rather than raising — one bad entry in the list
    should not take down discovery for the rest. Pointing an entry at a
    private key instead of its ``.pub`` file does not raise either: asyncssh
    parses OpenSSH private-key material leniently and returns only its
    public component (verified by reading ``asyncssh.public_key._decode_public``,
    not assumed) — so this is not a rejection path, it is quietly correct.
    Either way, only ``export_public_key()`` output ever reaches the
    response; nothing here ever calls an export-private path.

    Returns ``{"host_keys": [], "hostname": ...}`` when unconfigured, never
    an error — an SSH client probing this before any gateway exists is a
    normal, not exceptional, state.
    """
    return await ssh_access.host_keys(dependencies=dependencies.operations)


@router.get("/api/internal/ssh-targets/{handle}")
async def get_ssh_target(
    request: Request,
    handle: str,
    fingerprint: str | None = None,
    *,
    dependencies: SshAccessDependencies = Depends(get_ssh_access_dependencies),
) -> dict[str, Any]:
    """Resolve an SSH handle plus a presented key fingerprint to a workspace
    target. **Internal** — requires ``X-Internal-Key``.

    The gateway sends the FINGERPRINT, never a user id: it holds no database
    credentials, and this codebase does not accept an internal key plus an
    asserted user identity. Key-to-user mapping stays here so disabled_at and
    approval are enforced server-side.

    **Resolution here is not proof of possession**, so this endpoint must
    never call ``mark_ssh_key_used``: the caller has offered a fingerprint,
    not a signature. Every agent pod holds ``X-Internal-Key``, and
    fingerprints are derivable from published public keys, so a write on this
    path would be attacker-controlled. The gateway bumps ``last_used_at``
    itself, after ``key.verify`` succeeds.

    Unknown handle, unknown key and unauthorized all return an identical 404.
    A resolvable-but-not-live workspace returns 200 with a non-live ``state``
    and ``pod_ip: None``, so the gateway can print a readable reason.

    ``fingerprint`` is Optional so a request that omits it still reaches
    ``require_internal`` first: a required query param would make FastAPI
    422 before any auth check runs, disclosing the route and its parameter
    name to an unauthenticated caller.
    """
    await dependencies.require_internal(request)

    opaque = HTTPException(status_code=404, detail="No such workspace")

    if not fingerprint or not is_valid_handle(handle):
        raise opaque

    # Both lookups run unconditionally, even when the handle is already
    # known to be unknown. The original reason — resolve_user_by_ssh_
    # fingerprint bumped user_ssh_keys.last_used_at, which the caller could
    # read back through GET /api/ssh-keys, turning "did my key's last_used_at
    # just move?" into a confirmation oracle — is gone: that resolver is now a
    # pure read (see its docstring). Keep the unconditional call anyway. It
    # costs one indexed lookup on an already-failing request, it keeps the two
    # 404 paths issuing the same number of round trips, and it means
    # re-introducing any write inside the resolver cannot silently re-open the
    # oracle from here. Pinned by test_unknown_handle_still_reaches_the_
    # fingerprint_resolver.
    #
    # Note the timing argument is a bonus, not the boundary: the not-yours
    # path still does more work (get_thread plus user_can_access_ide_entity's
    # own get_job/get_thread) than the unknown-handle path.
    thread_id = await dependencies.store.get_thread_id_by_ssh_handle(handle)
    user = await dependencies.store.resolve_user_by_ssh_fingerprint(fingerprint)
    if not thread_id or not user:
        raise opaque
    if not await dependencies.user_can_access_ide_entity(
        user, dependencies.store, thread_id
    ):
        raise opaque

    return await ssh_access.resolve_target(
        thread_id=thread_id, user=user, dependencies=dependencies.operations
    )


@router.post("/api/internal/ssh-vm-access/{action}")
async def internal_vm_ssh_access(
    action: str,
    request: Request,
    body: VMGatewayAccessRequest,
    *,
    dependencies: SshAccessDependencies = Depends(get_ssh_access_dependencies),
) -> dict[str, Any]:
    """One verified gateway SSH connection, after key.verify, owns one lease.

    InternalKey alone is deliberately insufficient.  The gateway signs the
    exact connection/action with its host private key, whose public half is
    mounted in the orchestrator.  The asserted handle/key are re-resolved to
    an approved owner here on every action.
    """
    await dependencies.require_internal(request)
    opaque = HTTPException(status_code=404, detail="No such workspace")
    if action not in {"admit", "renew", "close"} or not dependencies.vm_access_store:
        raise opaque
    payload = body.proof
    entries = dependencies.operations.host_keys.load(
        os.environ.get("SSH_GATEWAY_PUBLIC_HOST_KEYS", "")
    )
    if not verify_vm_access_proof(
        payload,
        [entry["public_key"] for entry in entries],
        action=action,
    ):
        raise opaque
    handle = str(payload["handle"])
    fingerprint = str(payload["fingerprint"])
    thread_id = await dependencies.store.get_thread_id_by_ssh_handle(handle)
    user = await dependencies.store.resolve_user_by_ssh_fingerprint(fingerprint)
    if (
        not thread_id
        or not user
        or not await dependencies.user_can_access_ide_entity(
            user,
            dependencies.store,
            thread_id,
        )
    ):
        raise opaque
    thread = await dependencies.store.get_thread(thread_id)
    if not thread:
        raise opaque
    from orchestrator.services.ssh_access import thread_metadata_object

    metadata = thread_metadata_object(thread)
    if (
        not dependencies.operations.thread_is_vm_tier(
            metadata,
            metadata.get("workspace_container") or {},
            metadata.get("vm") or {},
        )
        or thread.get("execution_lane") != "pinned"
    ):
        raise opaque
    access = dependencies.vm_access_store
    claimant = f"{user['id']}:{payload['connection_id']}"
    if action == "close":
        return {
            "closed": await access.close(
                payload["lease_id"],
                owner_kind="thread",
                owner_id=thread_id,
                kind="ssh",
                claimant=claimant,
            )
        }
    if action == "renew":
        if not dependencies.vm_provisioner:
            return {"renewed": False}
        try:
            proof = await dependencies.vm_provisioner.attest_workspace_runtime(
                thread_id,
                entity_type="thread",
            )
            if vm_binding_digest(proof) != payload["binding"]:
                return {"renewed": False}
        except Exception:
            return {"renewed": False}
        return {
            "renewed": await access.renew(
                payload["lease_id"],
                owner_kind="thread",
                owner_id=thread_id,
                kind="ssh",
                claimant=claimant,
            )
        }
    lease = await access.request(
        owner_kind="thread",
        owner_id=thread_id,
        kind="ssh",
        user_id=str(user["id"]),
        connection_id=str(payload["connection_id"]),
    )
    if lease is None:
        return {"state": "restoring"}
    current = await access.inspect(
        str(lease["id"]),
        owner_kind="thread",
        owner_id=thread_id,
        kind="ssh",
        claimant=claimant,
    )
    if current is None:
        await access.close(
            str(lease["id"]),
            owner_kind="thread",
            owner_id=thread_id,
            kind="ssh",
            claimant=claimant,
        )
        return {"state": "restoring"}
    try:
        proof = await dependencies.vm_provisioner.attest_workspace_runtime(
            thread_id,
            entity_type="thread",
        )
        if (
            UUID(str(proof.vm_uid)) != lease["vm_uid"]
            or UUID(str(proof.workspace_generation)) != lease["provision_generation"]
        ):
            raise ValueError("VM SSH binding changed")
        binding = vm_binding_digest(proof)
    except Exception:
        await access.close(
            str(lease["id"]),
            owner_kind="thread",
            owner_id=thread_id,
            kind="ssh",
            claimant=claimant,
        )
        return {"state": "stale_binding"}
    return {
        "state": "live",
        "thread_id": thread_id,
        "user_id": str(user["id"]),
        "pod_ip": proof.host,
        "pod_port": proof.port,
        "host_key_fingerprint": proof.ssh_host_key_fingerprint,
        "lease_id": str(lease["id"]),
        "binding": binding,
    }


@router.post("/api/internal/ssh-keys/used")
async def internal_mark_ssh_key_used(
    request: Request,
    body: SshKeyUsedRequest,
    *,
    dependencies: SshAccessDependencies = Depends(get_ssh_access_dependencies),
) -> dict[str, str]:
    """Stamp ``last_used_at`` on the key behind a presented fingerprint.
    **Internal** — requires ``X-Internal-Key``.

    Keyed by FINGERPRINT, not ``key_id``, and that is not arbitrary: the
    gateway calls this from asyncssh's ``auth_completed()``, which fires
    immediately after ``key.verify`` succeeds — but the gateway resolves its
    target (and therefore a key id) lazily, at first channel open. At the
    only moment this call may legitimately fire, it holds a fingerprint and
    nothing else. An endpoint taking ``key_id`` would be uncallable then.

    Resolution happens server-side via ``resolve_user_by_ssh_fingerprint``
    (the same pure-read resolver ``get_ssh_target`` uses) so the actual
    write goes through ``mark_ssh_key_used``, which additionally requires
    the fingerprint to match the row being stamped — see that method's
    docstring for why this endpoint is safe to call with only a fingerprint
    even though ``get_ssh_target`` itself must never call it.

    An unknown fingerprint is a quiet no-op success, not a 404: the caller
    has already authenticated against *some* key by the time this fires, and
    a 404 here would turn this endpoint into an existence oracle for
    registered keys (identical reasoning to ``get_ssh_target``'s opaque
    404s).
    """
    await dependencies.require_internal(request)
    user = await dependencies.store.resolve_user_by_ssh_fingerprint(body.fingerprint)
    return await ssh_access.mark_key_used(
        user=user,
        fingerprint=body.fingerprint,
        dependencies=dependencies.operations,
    )


@router.post("/api/internal/ssh-attachments")
async def internal_create_ssh_attachment(
    request: Request,
    body: SshAttachmentCreate,
    *,
    dependencies: SshAccessDependencies = Depends(get_ssh_access_dependencies),
) -> dict[str, str]:
    """Open an SSH-attachment audit row. **Internal** — requires
    ``X-Internal-Key``.

    Resolves ``thread_id``, ``user_id`` and ``ssh_key_id`` server-side from
    ``fingerprint`` and ``handle`` — see ``SshAttachmentCreate`` for why an
    asserted identity is never accepted here. The resolution is the
    identical lookup ``get_ssh_target`` performs (``get_thread_id_by_ssh_
    handle`` + ``resolve_user_by_ssh_fingerprint``, gated by
    ``user_can_access_ide_entity``), reusing that endpoint's opaque 404 for
    every failure mode — unknown handle, unknown/unapproved key, and "not
    your thread" all come back identical, so this endpoint cannot be used
    to probe for handle or key existence either. In real operation this is
    not expected to fail: the gateway only calls this after ``get_ssh_
    target`` already resolved the same handle/fingerprint pair to open the
    SSH session in the first place.

    A foreign-key violation on the insert (thread, user or key deleted
    between the resolution above and the write below — a real race, e.g. a
    thread torn down mid-session, not just a probe) and a malformed
    ``handle`` surfacing from ``record_ssh_attachment`` both map to 400
    rather than an unhandled 500.
    """
    await dependencies.require_internal(request)

    opaque = HTTPException(status_code=404, detail="No such workspace")
    if not is_valid_handle(body.handle):
        raise opaque

    thread_id = await dependencies.store.get_thread_id_by_ssh_handle(body.handle)
    user = await dependencies.store.resolve_user_by_ssh_fingerprint(body.fingerprint)
    if not thread_id or not user:
        raise opaque
    if not await dependencies.user_can_access_ide_entity(
        user, dependencies.store, thread_id
    ):
        raise opaque

    return await ssh_access.record_attachment(
        thread_id=thread_id,
        user=user,
        handle=body.handle,
        client_ip=body.client_ip,
        dependencies=dependencies.operations,
    )


@router.post("/api/internal/ssh-attachments/{attachment_id}/close")
async def internal_close_ssh_attachment(
    request: Request,
    attachment_id: str,
    body: SshAttachmentClose,
    *,
    dependencies: SshAccessDependencies = Depends(get_ssh_access_dependencies),
) -> dict[str, int]:
    """Stamp detach time on an SSH-attachment row. **Internal** — requires
    ``X-Internal-Key``.

    Authorizes the close server-side rather than trusting the path-param
    UUID alone: resolves ``attachment_id`` to the thread it was opened
    against (``get_ssh_attachment_thread_id``), resolves ``body.fingerprint``
    to a user (``resolve_user_by_ssh_fingerprint``), and requires
    ``user_can_access_ide_entity`` to hold between them — the identical two
    building blocks ``internal_create_ssh_attachment`` composes, reused
    rather than re-invented. Both lookups run unconditionally regardless of
    whether the first already failed, mirroring ``get_ssh_target``'s own
    comment on why: skipping the fingerprint resolution whenever
    ``attachment_id`` is already known-bad would make "was the id real" a
    timing oracle.

    Every authorization failure — unknown id, a thread whose FK already went
    NULL (``get_ssh_attachment_thread_id``'s docstring), unresolvable
    fingerprint, and "not your thread" — collapses into the SAME
    ``{"closed": 0}`` the pre-existing "unknown or already-closed id" case
    returns, deliberately: a caller must not be able to distinguish "no such
    attachment" from "not yours" (opaque, matching create's contract), and
    the gateway already treats ``{"closed": 0}`` as a normal best-effort
    outcome, so this changes no caller's error handling. ``close_ssh_
    attachment`` itself is never reached in any of these cases.

    A malformed ``attachment_id`` is a distinct, earlier failure: it 400s
    (via ``get_ssh_attachment_thread_id``'s own ``UUID()`` parse, before any
    connection is acquired) rather than folding into the opaque envelope
    above, because the caller already knows whether the string it sent is a
    syntactically valid UUID — that check discloses nothing about which real
    ids exist.
    """
    await dependencies.require_internal(request)

    try:
        thread_id = await dependencies.store.get_ssh_attachment_thread_id(attachment_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    user = await dependencies.store.resolve_user_by_ssh_fingerprint(body.fingerprint)

    if (
        thread_id is not None
        and user is not None
        and await dependencies.user_can_access_ide_entity(
            user, dependencies.store, thread_id
        )
    ):
        return await ssh_access.close_attachment(
            attachment_id=attachment_id,
            channels=body.channels,
            dependencies=dependencies.operations,
        )

    return {"closed": 0}
