"""The gateway's only view of platform state.

The gateway holds no database credentials. It presents the fingerprint of the
key the client just authenticated with, and the orchestrator maps that to a
user and decides authorization. Sending a user id instead would be rejected:
this codebase does not treat an internal key plus an asserted identity as
equivalent to an authenticated user.

Two kinds of call live here and they fail differently:

``resolve_target`` is an AUTHORIZATION read. It fails closed, turning every
control-plane problem into a readable refusal, because the alternative is
either an open door or a hang.

``mark_key_used`` / ``record_attachment`` / ``close_attachment`` are AUDIT
writes against Task 6A's internal endpoints. They raise ``AuditWriteFailed``
rather than inventing a refusal, and every caller is expected to swallow it:
a bookkeeping failure must never tear down a session that already
authenticated. They are here rather than in the server module because this
module is the gateway's only view of platform state -- there is exactly one
place that knows how to reach the orchestrator, and it is this file.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx

from orchestrator.services.ssh_handles import is_valid_handle
from orchestrator.services.ssh_gateway_vm_access_proof import mint_vm_access_proof

# state -> (message shown on stderr, process exit code)
# 75 EX_TEMPFAIL    -- genuinely retryable without anything else changing:
#                      suspended, reclaimed, ending, restoring,
#                      stale_binding (a resume/re-provision plausibly
#                      rewrites the binding, so this is user-recoverable
#                      the same way suspended/reclaimed are -- not the
#                      same class as failed/deleted, where retrying is
#                      pointless).
# 69 EX_UNAVAILABLE -- broken or gone, not fixed by retrying alone: failed,
#                      deleted, ended, never_provisioned, unreachable.
# 77 EX_NOPERM      -- policy refusals: denied, vm_unsupported.
# Pinned per state, not just to "some legal code" -- an automated retry
# wrapper keying on the exit code would otherwise loop forever against a
# target that can never become live, or give up on one genuinely worth
# retrying. See test_exit_codes_are_pinned_per_state_not_just_in_the_legal_set.
REFUSAL_MESSAGES: dict[str, tuple[str, int]] = {
    "suspended": (
        "workspace suspended - resume the session in cockpit, then reconnect",
        75,
    ),
    "reclaimed": (
        "workspace was reclaimed while idle - resume the session in cockpit to "
        "restore it from its snapshot, then reconnect",
        75,
    ),
    "ending": ("workspace is shutting down - try again once it has stopped", 75),
    "ended": ("this session has ended", 69),
    "restoring": ("workspace is still starting - reconnect in a moment", 75),
    "never_provisioned": ("this session has no workspace yet", 69),
    "vm_unsupported": ("this workspace is VM-tier - SSH access is not supported", 77),
    # These three are real, currently-written workspace_container statuses
    # (see services.ssh_gateway_targets's module docstring and its
    # STATE_FAILED / STATE_DELETED / STATE_STALE_BINDING comments) -- not
    # hypothetical. That module added dedicated constants for them precisely
    # so a distinct, accurate reason reaches whoever reads the refusal;
    # falling back to "unreachable" or "never_provisioned" here would send
    # them after the wrong problem, undoing that.
    "failed": (
        "workspace failed to provision and cannot be recovered - start a new session",
        69,
    ),
    "deleted": (
        "workspace was deleted - start a new session to get a new workspace",
        69,
    ),
    "stale_binding": (
        "workspace is ready but its SSH access is not valid right now - "
        "reconnect via cockpit, or start a new session if this persists",
        75,
    ),
    "unreachable": ("workspace is unreachable right now", 69),
    "denied": ("no such workspace, or you do not have access to it", 77),
}


@dataclass(frozen=True)
class SshTarget:
    thread_id: str
    user_id: str
    pod_ip: str
    pod_port: int
    host_key_fingerprint: str
    state: str
    backend: str = "container"
    lease_id: str | None = None
    binding: str | None = None


class TargetDenied(Exception):
    """Unknown handle, unknown key, or not authorized — deliberately one case."""


class TargetUnavailable(Exception):
    def __init__(self, state: str):
        super().__init__(state)
        self.state = state


class AuditWriteFailed(Exception):
    """An audit bookkeeping write did not land.

    Deliberately NOT a ``TargetDenied``/``TargetUnavailable``: those two are
    authorization outcomes the gateway turns into a refusal the user reads,
    and an audit write must never be able to manufacture one. Every caller
    catches this and carries on.
    """


async def _http_get(url: str, headers: dict, params: dict, timeout: float) -> Any:
    async with httpx.AsyncClient(timeout=timeout) as client:
        return await client.get(url, headers=headers, params=params)


async def _http_post(url: str, headers: dict, json: dict, timeout: float) -> Any:
    async with httpx.AsyncClient(timeout=timeout) as client:
        return await client.post(url, headers=headers, json=json)


def _audit_headers(config) -> dict:
    return {"X-Internal-Key": config.internal_key}


# The ``last_used_at`` bump gets its own, much shorter budget than
# ``orchestrator_request_timeout``. asyncssh awaits ``auth_completed``'s
# coroutine from inside packet processing, and the async branch of
# ``_finish_recv_packet`` sets ``self._recv_handler = lambda: False``
# (``connection.py:1719-1725``) for the duration -- so every further inbound
# packet on that connection is buffered until the bump resolves, and the
# user's first channel open hangs rather than failing. (Note what this is
# NOT: userauth-success is already on the wire by then --
# ``connection.py:2101`` sends ``MSG_USERAUTH_SUCCESS`` and ``:2107`` flushes
# deferred packets, both before the ``:2113`` await -- so authentication
# latency is unaffected. An earlier version of this file's docstrings claimed
# otherwise and was wrong.)
#
# Awaiting is still right, and this constant is why: it caps the stall at ~2s
# instead of 10s while keeping the two properties fire-and-forget would give
# up -- one in-flight bump per connection (``_http_post`` builds a fresh
# unpooled ``httpx.AsyncClient`` per call, and the post-auth path has no
# admission cap of its own, so unbounded scheduling would amplify exactly the
# control-plane degradation it is meant to tolerate), and the bump being on
# record before any channel opens.
KEY_USE_BUMP_TIMEOUT_SECONDS = 2.0


async def _post_audit(
    config, path: str, payload: dict, timeout: float | None = None
) -> Any:
    """POST one audit write and return its parsed body, or raise.

    Every failure mode collapses into ``AuditWriteFailed``: a transport
    error, a non-200, and a body that is not a JSON object are all the same
    thing to a caller whose only reaction is to log and carry on. Chaining
    with ``from exc`` keeps the original in the traceback the caller logs --
    "the audit write failed" with no cause attached is not an operable log
    line.

    ``timeout`` defaults to ``config.orchestrator_request_timeout``. A caller
    passing one gets the SHORTER of the two, never a longer one: an operator
    who tightens the global budget is stating a ceiling, and a per-call
    override must not quietly raise it.
    """
    url = f"{config.orchestrator_url}{path}"
    budget = config.orchestrator_request_timeout
    if timeout is not None:
        budget = min(timeout, budget)
    try:
        response = await _http_post(
            url,
            headers=_audit_headers(config),
            json=payload,
            timeout=budget,
        )
    except Exception as exc:
        raise AuditWriteFailed(f"POST {path} failed: {exc!r}") from exc

    if response.status_code != 200:
        raise AuditWriteFailed(f"POST {path} returned {response.status_code}")

    try:
        body = response.json()
    except Exception as exc:
        raise AuditWriteFailed(f"POST {path} returned a non-JSON body") from exc

    if not isinstance(body, dict):
        raise AuditWriteFailed(
            f"POST {path} returned {type(body).__name__}, not an object"
        )
    return body


async def mark_key_used(config, fingerprint: str) -> None:
    """Stamp ``last_used_at`` on the key behind ``fingerprint``.

    Keyed by fingerprint rather than key id because the gateway's only
    legitimate call site is asyncssh's ``auth_completed()``, which fires
    immediately after ``key.verify`` succeeds -- while target resolution
    (and therefore any key id) is lazy, at first channel open. At that
    instant the gateway holds a fingerprint and nothing else.

    An unknown fingerprint is a 200 no-op server-side, not a 404, so this
    function cannot be used to probe which keys are registered.

    Runs on a ``KEY_USE_BUMP_TIMEOUT_SECONDS`` budget rather than the full
    ``orchestrator_request_timeout`` -- see that constant for why this one
    call is the one that needs a tighter one.
    """
    await _post_audit(
        config,
        "/api/internal/ssh-keys/used",
        {"fingerprint": fingerprint},
        timeout=KEY_USE_BUMP_TIMEOUT_SECONDS,
    )


async def record_attachment(
    config, fingerprint: str, handle: str, client_ip: str | None = None
) -> str:
    """Open the audit row for one SSH attachment, returning its id.

    Sends only ``fingerprint``/``handle``/``client_ip``: ``thread_id``,
    ``user_id`` and ``ssh_key_id`` are resolved server-side, because this
    codebase does not accept an internal key plus an asserted identity (see
    the module docstring, and ``SshAttachmentCreate``'s). The endpoint
    re-runs the identical authorization ``resolve_target`` already passed,
    and returns the same opaque 404 for every failure -- unknown handle,
    unknown key, and "not your thread" are indistinguishable here too.

    ``handle`` is validated before the request rather than after: it is the
    SSH username, fully attacker-controlled, and ``resolve_target``'s own
    comment explains why an unvalidated one must never reach a request
    carrying ``config.internal_key``. It travels in the JSON body here
    rather than the URL path, so this is defence in depth rather than the
    traversal fix it is there -- but a handle this module would refuse to
    resolve has no business opening an audit row either.
    """
    if not is_valid_handle(handle):
        raise AuditWriteFailed("refusing to record an attachment for an invalid handle")

    payload: dict[str, Any] = {"fingerprint": fingerprint, "handle": handle}
    if client_ip:
        payload["client_ip"] = client_ip

    body = await _post_audit(config, "/api/internal/ssh-attachments", payload)
    attachment_id = body.get("attachment_id")
    if not isinstance(attachment_id, str) or not attachment_id:
        # Without this the gateway would hold a None/int "id" and later POST
        # it into a URL path, turning one bad response into a second bad
        # request instead of one logged failure.
        raise AuditWriteFailed("ssh-attachments returned no usable attachment_id")
    return attachment_id


async def close_attachment(
    config, attachment_id: str, fingerprint: str, channels: Sequence[str] = ()
) -> int:
    """Stamp detach time on an attachment row, returning how many rows closed.

    ``fingerprint`` is in the body because the endpoint authorizes the close
    against the named attachment's own thread -- an internal-key holder must
    not be able to silently close (or fabricate ``channels`` on) an audit row
    belonging to someone else. ``{"closed": 0}`` is the endpoint's opaque
    answer for every unauthorized or unknown id and is a normal, non-raising
    outcome here: the row may already have been closed by a previous
    best-effort attempt.

    ``attachment_id`` is percent-encoded even though it comes from this
    module's own ``record_attachment``: it is the one value here that lands
    in a URL path, and ``resolve_target``'s handle encoding sets the
    precedent that a path segment is encoded at the point it is built, not
    trusted because of where it came from.
    """
    body = await _post_audit(
        config,
        f"/api/internal/ssh-attachments/{quote(attachment_id, safe='')}/close",
        {"fingerprint": fingerprint, "channels": list(channels)},
    )
    closed = body.get("closed")
    if not isinstance(closed, int) or isinstance(closed, bool):
        raise AuditWriteFailed("ssh-attachments close returned no usable count")
    return closed


def _is_valid_port(value: Any) -> bool:
    """True if ``value`` is a genuine int (never a bool) in 1..65535.

    ``isinstance(True, int)`` is ``True`` (bool subclasses int) and
    ``int(3.9) == 3`` silently truncates, so a bare ``isinstance(x, int)`` or
    ``int(x)`` conversion alone would let a JSON boolean or a fractional
    number through as a "valid" port. Neither is ever a real port, so both
    are excluded explicitly rather than coerced -- no ``int()`` call happens
    anywhere in this path.
    """
    return (
        isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= 65535
    )


def _is_valid_identifier(value: Any) -> bool:
    """True if ``value`` is a non-empty string.

    Used for ``pod_ip``, ``host_key_fingerprint``, and ``thread_id``: a
    frozen dataclass performs no runtime type validation, so a "live"
    response with a null, empty, or non-string value for any of them would
    otherwise construct a usable-looking ``SshTarget`` that a caller then
    dials or pins against garbage instead of getting the readable refusal
    this module promises. ``thread_id`` earns the same check as the other
    two now that ``connect_upstream`` mints each certificate's principal
    from it directly -- ``SshUserCa.mint`` raising on an empty principal is
    a fail-closed backstop, not validation, and a null/non-string value
    would reach it unchecked otherwise.
    """
    return isinstance(value, str) and bool(value)


async def resolve_target(config, handle: str, fingerprint: str) -> SshTarget:
    """Resolve handle + fingerprint to a live workspace, or raise."""
    if not is_valid_handle(handle):
        # handle is the SSH USERNAME (services.ssh_handles's own docstring)
        # and is therefore fully attacker-controlled at the point a caller
        # reaches this function. It is interpolated into the URL path below,
        # and httpx normalises ".." path segments when building a request --
        # an unvalidated handle is an authenticated GET-request-forgery
        # primitive carrying config.internal_key into arbitrary orchestrator
        # routes that never run the orchestrator's own is_valid_handle
        # check, because a traversed URL never reaches the ssh-targets route
        # at all. Refusing here, before any URL is built or any request is
        # sent, also preserves the deliberate handle/key/authz
        # indistinguishability: an invalid handle now produces exactly the
        # same TargetDenied as an unknown one.
        raise TargetDenied()

    # Belt-and-braces beyond the is_valid_handle gate above: percent-encode
    # so a handle can never reintroduce a "/" or ".." into the path even if
    # that check's charset were ever loosened.
    url = f"{config.orchestrator_url}/api/internal/ssh-targets/{quote(handle, safe='')}"
    try:
        response = await _http_get(
            url,
            headers={"X-Internal-Key": config.internal_key},
            params={"fingerprint": fingerprint},
            timeout=config.orchestrator_request_timeout,
        )
    except Exception:
        # Fail closed: a control-plane outage must not become an open door or a
        # hang, it must become a refusal the user can read.
        raise TargetUnavailable("unreachable") from None

    if response.status_code == 404:
        raise TargetDenied()
    if response.status_code != 200:
        raise TargetUnavailable("unreachable")

    try:
        payload = response.json()
        state = payload.get("state")
        if state != "live":
            raise TargetUnavailable(
                state if state in REFUSAL_MESSAGES else "unreachable"
            )

        pod_ip = payload["pod_ip"]
        pod_port = payload["pod_port"]
        host_key_fingerprint = payload["host_key_fingerprint"]
        thread_id = payload["thread_id"]
        if not (
            _is_valid_identifier(pod_ip)
            and _is_valid_port(pod_port)
            and _is_valid_identifier(host_key_fingerprint)
            and _is_valid_identifier(thread_id)
        ):
            raise TargetUnavailable("unreachable")

        return SshTarget(
            thread_id=thread_id,
            user_id=payload["user_id"],
            pod_ip=pod_ip,
            pod_port=pod_port,
            host_key_fingerprint=host_key_fingerprint,
            state="live",
        )
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        # A "live" state, parseable JSON, and a dict payload are still not
        # proof the rest of the payload is well-formed. A non-JSON body
        # (json.JSONDecodeError, a ValueError subclass), a non-dict payload
        # (AttributeError from .get()/[] on a list/str/None), a missing
        # field (KeyError), or an unhashable state (TypeError from `state in
        # REFUSAL_MESSAGES`) must all become the same readable refusal as
        # any other bad control-plane response, not an unhandled exception
        # surfacing mid-authentication in the SSH server.
        raise TargetUnavailable("unreachable") from exc


async def vm_access(
    config,
    signer,
    *,
    action: str,
    connection_id: str,
    handle: str,
    fingerprint: str,
    target: SshTarget | None = None,
) -> SshTarget | bool:
    """A post-key.verify, signed gateway action; never a fingerprint GET."""
    if signer is None or not is_valid_handle(handle):
        raise TargetUnavailable("vm_unsupported")
    proof = mint_vm_access_proof(
        signer,
        action=action,
        connection_id=connection_id,
        handle=handle,
        fingerprint=fingerprint,
        lease_id=target.lease_id if target and action != "admit" else "",
        binding=target.binding if target and action != "admit" else "",
    )
    try:
        response = await _http_post(
            f"{config.orchestrator_url}/api/internal/ssh-vm-access/{action}",
            headers=_audit_headers(config),
            json={"proof": proof},
            timeout=config.orchestrator_request_timeout,
        )
    except Exception:
        raise TargetUnavailable("unreachable") from None
    if response.status_code == 404:
        raise TargetDenied()
    if response.status_code != 200:
        raise TargetUnavailable("unreachable")
    try:
        payload = response.json()
        if action != "admit":
            return payload["renewed" if action == "renew" else "closed"] is True
        if payload.get("state") != "live":
            raise TargetUnavailable(
                payload["state"]
                if payload.get("state") in REFUSAL_MESSAGES
                else "unreachable"
            )
        if not (
            _is_valid_identifier(payload["pod_ip"])
            and _is_valid_port(payload["pod_port"])
            and _is_valid_identifier(payload["host_key_fingerprint"])
            and _is_valid_identifier(payload["thread_id"])
            and _is_valid_identifier(payload["user_id"])
            and isinstance(payload["binding"], str)
            and len(payload["binding"]) == 64
        ):
            raise TargetUnavailable("unreachable")
        UUID(payload["lease_id"])
        int(payload["binding"], 16)
        return SshTarget(
            thread_id=payload["thread_id"],
            user_id=payload["user_id"],
            pod_ip=payload["pod_ip"],
            pod_port=payload["pod_port"],
            host_key_fingerprint=payload["host_key_fingerprint"],
            state="live",
            backend="vm",
            lease_id=payload["lease_id"],
            binding=payload["binding"],
        )
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise TargetUnavailable("unreachable") from exc
