"""The SSH server half of the gateway.

Ordering rule that shapes this whole file: asyncssh calls ``begin_auth``
before ``validate_public_key``, and neither is allowed to touch the
workspace. The target is resolved lazily at first channel open, once an
identity exists. Resolving earlier means an unauthenticated peer can make us
dial upstream -- measured at 100 upstream dials for 100 failed
authentications.

Authentication therefore establishes IDENTITY only; AUTHORIZATION happens at
channel open, where a refusal can carry a readable reason on stderr instead
of asyncssh's bare ``Permission denied (publickey)``.

Reply strategy is deliberately split, because asyncssh's channel-level
callbacks are synchronous and must answer before the upstream is known good:

  shell / exec  -> optimistic SUCCESS, then stderr + exit code, so the user
                   can read why they were refused.
  subsystem     -> defer the reply, then CHANNEL_FAILURE, because SFTP and
                   JetBrains clients never render stderr.

Both halves are wired now that the dial exists. The subsystem half works by
returning ``None`` from ``subsystem_requested``, which asyncssh reads as "no
answer yet" (``channel.py``'s ``_service_next_request`` only reports a
response when the handler returns something other than ``None`` -- the same
mechanism asyncssh itself uses to defer x11 and agent-forwarding replies).
The answer is then delivered from a task once the upstream is known good.
The ordering matters and is the reason this could not land before the dial
did: ``_report_response(True)`` is what CALLS ``session_started`` and so
starts ``_session_factory``, meaning the deferred reply has to resolve the
upstream itself rather than leaving it to the session.

Getting this wrong is not cosmetic. An optimistic SUCCESS followed by a
dying channel is what an sftp client reports as ``SFTPConnectionLost`` (and
OpenSSH's ``sftp`` as a bare "Connection closed"); a deferred
CHANNEL_FAILURE is one clean ``ChannelOpenError`` naming the channel.

THREE PIECES OF WIRING HERE ARE LOAD-BEARING AND HAVE NO OTHER ENFORCER.
Each is a sibling module's capability that is inert until this file calls
it, so nothing in that sibling's own test suite can catch its removal:

1. ``narrow_signature_algorithms(key)`` in ``validate_public_key``.
   ``signature_algs`` is advisory on asyncssh's server side --
   ``SSHServerConnection.validate_public_key`` calls ``key.verify()``
   (``connection.py:6217``) and ``SSHKey.verify()`` gates on the KEY's own
   ``all_sig_algorithms`` (``public_key.py:587``), never on the listener's
   policy list. Registered RSA user keys are accepted down to 3072 bits, so
   without this call a client that ignores the advisory ``server-sig-algs``
   hint authenticates with SHA-1 ``ssh-rsa``.
2. ``auth_completed`` bumping ``last_used_at``. That column is the
   stolen-key detection signal, and splitting its write out of the resolver
   (so that merely *offering* a key no longer stamps it) only pays off if
   something stamps it after the key actually verifies. This is that
   something.
3. ``try_open_channel``/``close_channel``. asyncssh ships no ``MaxSessions``
   equivalent -- 300 concurrent session channels on one connection were
   measured on 2.24.0 -- and 300 channels would exhaust the workspace
   sshd's ``MaxSessions 16`` and present as workspace death.
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Optional

import asyncssh
from asyncssh.constants import (
    OPEN_ADMINISTRATIVELY_PROHIBITED,
    OPEN_CONNECT_FAILED,
)
from asyncssh.sftp import MIN_SFTP_VERSION

from orchestrator.services.ssh_gateway_client import (
    REFUSAL_MESSAGES,
    SshTarget,
    TargetDenied,
    TargetUnavailable,
)
from orchestrator.services.ssh_gateway_config import narrow_signature_algorithms
from orchestrator.services.ssh_gateway_proxy import ProxyProcess, proxy_session
from orchestrator.services.ssh_handles import is_valid_handle

logger = logging.getLogger(__name__)

_LOOPBACK_DESTINATIONS = frozenset({"127.0.0.1", "localhost"})

# The two values that may ever reach the audit row's ``channels`` text[]
# column. The client's own subsystem string is MAPPED into this vocabulary,
# never echoed, so no attacker-controlled text reaches the column and the
# recorded list cannot outgrow the endpoint's 8-entry cap however many
# channels one connection opens.
SESSION_CHANNEL_TYPE = "session"
SFTP_CHANNEL_TYPE = "sftp"

# Refusal for the gateway's OWN per-workspace attachment cap. Deliberately
# not an entry in ``REFUSAL_MESSAGES``: that table is keyed by workspace
# state as reported by the orchestrator, and a client-side resource guard is
# not a workspace state. (``test_exit_codes_are_pinned_per_state_not_just_
# in_the_legal_set`` asserts that table's keys exactly, so adding one there
# would also be a lie about where the refusal came from.)
# 75 EX_TEMPFAIL, matching the "suspended"/"reclaimed" class: closing one of
# your other sessions makes this succeed, so an automated retry is right.
ATTACHMENT_LIMIT_MESSAGE = (
    "too many SSH connections to this workspace already - close one and reconnect"
)
ATTACHMENT_LIMIT_EXIT_CODE = 75

# Strong references to fire-and-forget audit closes. ``connection_lost`` is a
# synchronous asyncssh callback, so the close has to be scheduled; without a
# reference held here the event loop is free to garbage-collect the task
# mid-flight and the audit row silently stays open.
_BACKGROUND_TASKS: set[asyncio.Task] = set()

# Default ceiling for drain_background_tasks. Comfortably above one audit
# close's own budget so an in-flight close normally finishes, and well inside
# a Kubernetes ``terminationGracePeriodSeconds`` so a rollout is not held up.
DRAIN_TIMEOUT_SECONDS = 5.0


async def drain_background_tasks(timeout: float = DRAIN_TIMEOUT_SECONDS) -> int:
    """Await in-flight audit closes, returning how many did NOT settle.

    ``timeout`` is a required-in-practice ``float``, deliberately NOT
    ``Optional``: ``asyncio.wait(timeout=None)`` waits forever, which is the
    exact opposite of what a shutdown drain is for -- one wedged audit POST
    would hang the whole termination until Kubernetes SIGKILLs the pod.
    Nothing may pass ``None`` here.

    Task 8's shutdown should call this. ``connection_lost`` schedules the
    attachment close rather than awaiting it (the callback is synchronous),
    so without a drain a rollout that tears the process down mid-flight
    leaves those rows open in the audit table -- silently, because the loop
    simply stops.

    Anything still running past ``timeout`` is cancelled rather than waited
    on forever: a shutdown that hangs on bookkeeping is worse than an
    unclosed audit row, and the return value gives the caller a number to
    log rather than making the loss invisible.
    """
    pending = [task for task in _BACKGROUND_TASKS if not task.done()]
    if not pending:
        return 0
    _, still_pending = await asyncio.wait(pending, timeout=timeout)
    for task in still_pending:
        task.cancel()
    if still_pending:
        logger.warning(
            "ssh gateway: %d audit close(s) cancelled at shutdown", len(still_pending)
        )
    return len(still_pending)


# How long a closing inner hop is waited on before it is abandoned. Well
# inside DRAIN_TIMEOUT_SECONDS so a shutdown drain is bounded by its own
# ceiling rather than by this one, and by a Kubernetes
# terminationGracePeriodSeconds either way.
UPSTREAM_CLOSE_TIMEOUT_SECONDS = 3.0
VM_RENEW_INTERVAL_SECONDS = 30.0


async def _await_upstream_closed(connection) -> None:
    """Wait, briefly, for an already-close()d inner hop to finish closing.

    Never raises: this runs as a background task, where an exception would
    reach the loop's handler as an unretrieved-exception warning and tell
    the operator nothing they can act on. A teardown that outlives its
    budget is abandoned rather than waited on forever -- the workspace
    sshd's own timeout is the backstop.
    """
    try:
        async with asyncio.timeout(UPSTREAM_CLOSE_TIMEOUT_SECONDS):
            await connection.wait_closed()
    except TimeoutError:
        logger.warning("ssh gateway: an upstream connection did not close in time")
    except Exception:
        logger.debug("ssh gateway: upstream close failed", exc_info=True)


def clamp_direct_tcpip(dest_host: str) -> bool:
    """True iff a direct-tcpip destination is permitted.

    The workspace sshd already enforces ``PermitOpen 127.0.0.1:*``; refusing
    here as well keeps the gateway's posture legible and produces a better
    error. Note the refusal surfaces at first connect, not at ``ssh -L``
    setup. ``::1`` is refused because the workspace listens on IPv4 loopback
    only.
    """
    return dest_host in _LOOPBACK_DESTINATIONS


MISCONFIGURED_MESSAGE = (
    "this gateway is not correctly configured - report this to an administrator"
)
# 69 EX_UNAVAILABLE, not 75: reconnecting cannot fix a half-wired listener.
MISCONFIGURED_EXIT_CODE = 69


class AttachmentLimitReached(Exception):
    """This workspace already has as many SSH attachments as it allows."""


class GatewayMisconfigured(Exception):
    """A required ``GatewayContext`` field was never bound."""


@dataclass
class GatewayContext:
    """Everything one listener shares across its connections.

    The callables are pre-bound to ``config`` by whoever builds this (Task 8
    does it with ``functools.partial``), which is why none of them takes a
    config argument: this class is what a ``GatewaySSHServer`` is allowed to
    reach, and a server that could re-derive its own orchestrator client
    could also reach past the seam the tests inject at.
    """

    config: Any
    ca: Any
    limiter: Any
    resolve: Optional[Callable]
    record_attach: Optional[Callable]
    close_attach: Optional[Callable]
    # Defaulted, unlike its five siblings, only because it was added after
    # the field order above was already being constructed positionally.
    mark_key_used: Optional[Callable] = None
    vm_admit: Optional[Callable] = None
    vm_renew: Optional[Callable] = None
    vm_close: Optional[Callable] = None

    # Not every field is optional in the same sense, and the uniform
    # ``Optional[Callable]`` typing hides that. ``resolve``, ``limiter`` and
    # ``ca`` are REQUIRED -- a connection cannot authorize, bound, or reach
    # a workspace without them. ``ca`` joined the list when the upstream
    # dial landed: ``connect_upstream`` mints this connection's certificate
    # off it, so an unbound one used to surface as an AttributeError
    # escaping ``_session_factory`` into asyncssh's task reaper, which tears
    # the connection down with nothing for the user or the operator to read.
    # The three audit callables are genuinely optional (audit is best effort
    # by design, and every call site guards them), and ``config`` is not
    # touched by this module at all.
    REQUIRED_FIELDS = ("resolve", "limiter", "ca")

    def missing_required(self) -> tuple[str, ...]:
        """Names of required fields this context never had bound."""
        return tuple(
            name for name in self.REQUIRED_FIELDS if getattr(self, name) is None
        )


class _GatewayProxyProcess(ProxyProcess):
    """A ``ProxyProcess`` that reports its own channel's close, and that
    defers a subsystem reply until the upstream is known good.

    asyncssh's channel cleanup calls ``connection_lost`` on the session
    exactly once (``channel.py``'s ``_cleanup``), and wraps it in its own
    try/except -- so the slot release runs BEFORE ``super()``, where an
    exception in asyncssh's teardown cannot cost us the release.
    """

    def __init__(
        self,
        process_factory,
        sftp_factory,
        sftp_version,
        allow_scp,
        *,
        on_channel_closed: Callable[[], None],
        open_upstream: Callable,
        note_channel_type: Callable,
    ):
        super().__init__(process_factory, sftp_factory, sftp_version, allow_scp)
        self._on_channel_closed = on_channel_closed
        self._open_upstream = open_upstream
        self._note_channel_type = note_channel_type
        # Serves two purposes deliberately: it makes the slot release
        # idempotent, and it is how the deferred subsystem reply below knows
        # there is no longer anyone to answer.
        self._channel_closed = False

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if not self._channel_closed:
            self._channel_closed = True
            try:
                self._on_channel_closed()
            except Exception:  # pragma: no cover - defensive
                logger.exception("ssh gateway: failed to release a channel slot")
        super().connection_lost(exc)

    def subsystem_requested(self, subsystem: str) -> Optional[bool]:
        """Defer the reply until the workspace has actually answered.

        Returning ``None`` tells asyncssh "no answer yet"
        (``channel.py``'s ``_service_next_request`` reports a response only
        for a non-``None`` result), and ``_settle_subsystem`` delivers the
        real one later. sftp and JetBrains clients never render stderr, so
        the shell path's optimistic SUCCESS would leave them with a channel
        that simply dies -- ``SFTPConnectionLost``, with no reason anywhere.
        """
        if not super().subsystem_requested(subsystem):
            return False
        # Recorded even though the session may never start: 'they asked for
        # sftp and were turned away' is exactly what the audit table's
        # ``channels`` column is for, and on a refusal ``_session_factory``
        # -- which does the same recording for the shell path -- is never
        # reached at all.
        self._note_channel_type(self)
        report = getattr(self._chan, "_report_response", None)
        if report is None:
            # Degrade to the shell path's strategy rather than hanging: a
            # deferral nobody can ever answer would leave every sftp client
            # waiting on a reply that never comes. Loud, because the only
            # way to get here is an asyncssh that renamed the mechanism out
            # from under us -- the whole mechanism is exercised against the
            # real library by
            # test_a_refused_workspace_fails_sftp_with_a_channel_open_failure.
            logger.error(
                "ssh gateway: cannot defer a subsystem reply; answering "
                "optimistically instead, so a refusal will reach sftp "
                "clients as a dropped channel"
            )
            return True
        self._conn.create_task(self._settle_subsystem(report), self._chan.logger)
        return None

    async def _settle_subsystem(self, report: Callable[[bool], None]) -> None:
        """Answer a deferred subsystem request once the upstream is known.

        ``report(True)`` is what calls ``session_started``, which starts
        ``_session_factory`` -- which resolves the same, now cached,
        upstream. ``report(False)`` sends CHANNEL_FAILURE and starts
        nothing.
        """
        try:
            await self._open_upstream()
        except Exception as exc:
            logger.info("ssh gateway: refusing a subsystem channel: %r", exc)
            allowed = False
        else:
            allowed = True
        if self._channel_closed:
            # The peer went away mid-dial. Reporting True here would ask
            # asyncssh to start a session on a channel it has already torn
            # down; reporting anything at all is pointless.
            return
        try:
            report(allowed)
        except Exception:
            # THIS GUARD IS WHAT MAKES DEPENDING ON A PRIVATE METHOD
            # ACCEPTABLE. ``_report_response`` is called from a task, so an
            # exception here would reach asyncssh's ``_reap_task`` ->
            # ``internal_error()``, which tears down the ENTIRE inbound
            # connection -- every other channel on it included -- rather
            # than this one channel. The branch above handles the method
            # being renamed away; this one handles the likelier upgrade
            # failure, "same name, different contract".
            #
            # Closing the channel is the degradation, not a tidy-up:
            # ``_cleanup`` resolves the peer's pending request waiters with
            # ``False`` (channel.py:218-226), so the client gets the same
            # clean "Session request failed" it would have got from an
            # honest CHANNEL_FAILURE, and the connection survives.
            logger.exception(
                "ssh gateway: could not answer a deferred subsystem request; "
                "refusing this channel"
            )
            try:
                self._chan.close()
            except Exception:  # pragma: no cover - defensive
                logger.exception("ssh gateway: could not close a stuck channel")


class GatewaySSHServer(asyncssh.SSHServer):
    """One inbound SSH connection's policy and lifecycle."""

    def __init__(self, context: GatewayContext, client_ip: str):
        self._context = context
        self._client_ip = client_ip
        # A fresh id rather than ``id(self)``: CPython recycles object ids
        # after collection, and a recycled one would alias a live
        # connection's entry in the limiter's per-connection channel dict.
        self._connection_id = uuid.uuid4().hex
        self.handle: Optional[str] = None
        self.presented_fingerprint: Optional[str] = None

        self._target: Optional[SshTarget] = None
        self._resolve_lock = asyncio.Lock()
        # The INBOUND connection, kept because ``forward_tunneled_connection``
        # is a method on ``SSHServerConnection`` (connection.py:7255), not on
        # the upstream client connection.
        self._conn = None
        self._upstream_conn = None
        self._upstream_lock = asyncio.Lock()
        self._attached_workspace: Optional[str] = None
        self._attachment_id: Optional[str] = None
        self._channel_types: list[str] = []
        self._open_channels = 0
        # Set by connection_lost, read by _open_attachment_record. See the
        # latter for the window this closes.
        self._connection_closed = False
        self._authenticated = False
        self._vm_renew_task: asyncio.Task | None = None
        self._vm_access_closed = False

    # --- observable state (read by tests and by Task 8's logging) ---------

    @property
    def attachment_id(self) -> Optional[str]:
        return self._attachment_id

    @property
    def channel_types(self) -> list[str]:
        return list(self._channel_types)

    # --- authentication ---------------------------------------------------

    def connection_made(self, conn) -> None:
        """Keep the inbound connection, as asyncssh's own docs instruct.

        ``direct-tcpip`` is forwarded with
        ``SSHServerConnection.forward_tunneled_connection(upstream, host,
        port)`` -- a method on THIS connection that takes the upstream as an
        argument, not a method on the upstream. Getting that backwards is an
        AttributeError at the first ``ssh -L``.
        """
        self._conn = conn

    def begin_auth(self, username: str) -> bool:
        """Always require authentication. Records the handle only; resolving
        a target here would dial the workspace for unauthenticated peers."""
        self.handle = username
        return True

    def public_key_auth_supported(self) -> bool:
        return True

    def password_auth_supported(self) -> bool:
        return False

    def kbdint_auth_supported(self) -> bool:
        return False

    def validate_public_key(self, username: str, key) -> bool:
        """Accept any well-formed key, narrow it, and remember its fingerprint.

        Identity is established here; *authorization* happens at channel
        open, where a refusal can carry a readable reason. Rejecting here
        would emit only 'Permission denied (publickey)'.

        This runs in asyncssh's QUERY phase, before any signature exists, so
        it must not write anything or call anything with a side effect --
        which is why the ``last_used_at`` bump lives in ``auth_completed``
        and not here. Narrowing ``key`` is not such a side effect: it
        mutates only this connection's own key object (the same instance
        asyncssh then hands to ``key.verify()``), shadowing the class
        attribute, and it must happen here because there is no later
        callback that still holds the key.

        ``presented_fingerprint`` is assigned only after narrowing succeeds,
        and is the fingerprint of the key that actually AUTHENTICATED by the
        time ``auth_completed`` reads it: asyncssh calls this method again
        for the signed attempt (``_validate_client_public_key``, then
        ``key.verify`` on the returned object), and a successful signed
        attempt ends authentication immediately -- so no later query can
        overwrite it.
        """
        if not is_valid_handle(username or ""):
            return False
        try:
            fingerprint = key.get_fingerprint("sha256")
            narrow_signature_algorithms(key)
        except Exception:
            # narrow_signature_algorithms raises AssertionError if a future
            # asyncssh stops honouring the assignment. Fail closed and loud
            # in the log rather than authenticating a key that still speaks
            # SHA-1, and never let the exception escape into asyncssh's auth
            # path, where it would surface as a protocol error rather than a
            # denied key.
            logger.exception(
                "ssh gateway: refusing a presented key that could not be validated"
            )
            return False
        self.presented_fingerprint = fingerprint
        return True

    async def auth_completed(self) -> None:
        """Bump ``last_used_at`` for the key that just authenticated.

        Post-``key.verify`` by construction: asyncssh calls this only after
        userauth succeeds. Keyed by fingerprint because target resolution is
        lazy -- at this instant there is no key id to send.

        Awaited rather than fired and forgotten (asyncssh awaits a coroutine
        returned from here, ``connection.py:2113``), so the bump is on record
        before any channel opens.

        CORRECTION: an earlier version of this docstring said awaiting
        "delays userauth-success". It does not. ``connection.py:2101`` sends
        ``MSG_USERAUTH_SUCCESS`` and ``:2107`` flushes deferred packets,
        both BEFORE the ``:2113`` await -- authentication latency is
        untouched. What awaiting really costs is that the async branch of
        ``_finish_recv_packet`` sets ``self._recv_handler = lambda: False``
        (``connection.py:1719-1725``), buffering this connection's further
        inbound packets until the bump resolves, so a degraded orchestrator
        makes the first channel open hang. That is bounded by
        ``ssh_gateway_client.KEY_USE_BUMP_TIMEOUT_SECONDS`` rather than the
        full request timeout, which is the right fix; scheduling instead
        would drop the backpressure exactly when the control plane is
        degraded (see that constant's own comment).

        Every failure is swallowed: a bookkeeping write must never tear down
        a session that already authenticated.
        """
        self._authenticated = True
        bump = self._context.mark_key_used
        if bump is None or not self.presented_fingerprint:
            return
        try:
            await bump(self.presented_fingerprint)
        except Exception:
            logger.warning(
                "ssh gateway: last_used_at not bumped for the authenticated key",
                exc_info=True,
            )

    # --- channel policy ---------------------------------------------------

    def server_requested(self, listen_host: str, listen_port: int) -> bool:
        """Refuse remote forwarding (``ssh -R``). Mirrors the workspace sshd's
        ``AllowTcpForwarding local``."""
        return False

    def connection_requested(self, dest_host, dest_port, orig_host, orig_port):
        """direct-tcpip. Clamped to workspace loopback.

        Not charged against the per-connection channel cap: that cap exists
        to mirror the workspace sshd's ``MaxSessions``, which counts session
        channels only. A direct-tcpip flood is a different problem with a
        different bound.
        """
        if not clamp_direct_tcpip(dest_host):
            return False
        return self._forward_connection(dest_host, dest_port)

    def session_requested(self):
        """Open one session channel, subject to the per-connection cap.

        Raises ``ChannelOpenError`` rather than returning ``False`` so the
        client is told *why*; ``False`` sends a bare "Session refused".
        The slot is charged here and released in
        ``_GatewayProxyProcess.connection_lost``, with
        ``GatewaySSHServer.connection_lost`` sweeping anything still held (a
        channel that is opened and then abandoned without ever being closed
        individually).
        """
        limiter = self._context.limiter
        if limiter is None:
            # Fail closed rather than skipping the cap: a listener with no
            # limiter would otherwise accept unbounded channels. Refused
            # here rather than deferred to the stderr path below because
            # without a limiter there is no slot to charge in the first
            # place. logger.error, not warning -- this is an operator bug,
            # not a user one, and it makes every connection useless.
            logger.error(
                "ssh gateway: GatewayContext is missing %s; refusing every channel",
                ", ".join(self._context.missing_required()),
            )
            raise asyncssh.ChannelOpenError(
                OPEN_ADMINISTRATIVELY_PROHIBITED, MISCONFIGURED_MESSAGE
            )
        if not limiter.try_open_channel(self._connection_id):
            raise asyncssh.ChannelOpenError(
                OPEN_ADMINISTRATIVELY_PROHIBITED,
                "too many concurrent channels on this connection",
            )
        self._open_channels += 1
        # MIN_SFTP_VERSION comes from asyncssh.sftp; asyncssh.constants does
        # not export it (importing it from there raises ImportError).
        return _GatewayProxyProcess(
            self._session_factory,
            None,
            MIN_SFTP_VERSION,
            False,
            on_channel_closed=self._channel_closed,
            open_upstream=self._upstream,
            note_channel_type=self._note_channel_type,
        )

    def _channel_closed(self) -> None:
        """Return one channel slot, never more than this connection holds."""
        if self._open_channels <= 0:
            return
        self._open_channels -= 1
        self._context.limiter.close_channel(self._connection_id)

    # --- connection lifecycle ---------------------------------------------

    def connection_lost(self, exc: Optional[Exception]) -> None:
        """Release everything this connection still holds.

        asyncssh closes every channel (and so fires every per-channel
        ``connection_lost``) before calling this, so the sweep below normally
        finds nothing -- it exists for the channel that never reached a
        session at all.
        """
        self._connection_closed = True
        while self._open_channels > 0:
            self._channel_closed()
        self._release_attachment()
        self._close_upstream()
        if self._vm_renew_task is not None:
            self._vm_renew_task.cancel()
        self._close_vm_access()

    def _close_vm_access(self) -> None:
        target = self._target
        if (
            self._vm_access_closed
            or target is None
            or target.backend != "vm"
            or self._context.vm_close is None
        ):
            return
        self._vm_access_closed = True
        self._schedule(
            self._context.vm_close(
                connection_id=self._connection_id,
                handle=self.handle,
                fingerprint=self.presented_fingerprint,
                target=target,
            )
        )

    async def _renew_vm_access(self, target: SshTarget) -> None:
        """Only a still-attached upstream may keep its own bounded lease."""
        while not self._connection_closed and self._upstream_conn is not None:
            await asyncio.sleep(VM_RENEW_INTERVAL_SECONDS)
            if self._connection_closed or self._upstream_conn is None:
                return
            try:
                valid = await self._context.vm_renew(
                    connection_id=self._connection_id,
                    handle=self.handle,
                    fingerprint=self.presented_fingerprint,
                    target=target,
                )
            except Exception:
                valid = False
            if not valid:
                self._close_upstream()
                self._close_vm_access()
                return

    def _close_upstream(self) -> None:
        """Drop this connection's SSH connection to the workspace.

        Nothing else ever closes it: without this, every gateway connection
        leaves a live inner-hop connection holding one of the workspace
        sshd's ``MaxSessions`` slots until the gateway pod restarts.

        ``close()`` is synchronous because ``connection_lost`` is, but it
        only REQUESTS the close -- the disconnect and socket teardown finish
        on the loop afterwards. The bounded ``wait_closed()`` is therefore
        scheduled through ``_schedule``, which is what puts it in
        ``_BACKGROUND_TASKS`` and so under ``drain_background_tasks``: a
        rollout that stops the loop between the close request and its
        completion would otherwise drop the inner hop mid-teardown, leaving
        the workspace sshd to time the half-closed connection out itself.
        """
        connection, self._upstream_conn = self._upstream_conn, None
        if connection is None:
            return
        try:
            connection.close()
        except Exception:  # pragma: no cover - defensive
            logger.exception("ssh gateway: failed to close an upstream connection")
            return
        self._schedule(_await_upstream_closed(connection))

    def _release_attachment(self) -> None:
        workspace_id, self._attached_workspace = self._attached_workspace, None
        if workspace_id is not None:
            self._context.limiter.detach(workspace_id)

        attachment_id, self._attachment_id = self._attachment_id, None
        close = self._context.close_attach
        if attachment_id is None or close is None or not self.presented_fingerprint:
            return
        self._schedule(
            self._close_attachment_record(
                close, attachment_id, self.presented_fingerprint, self.channel_types
            )
        )

    async def _close_attachment_record(
        self, close, attachment_id: str, fingerprint: str, channels: list[str]
    ) -> None:
        try:
            await close(attachment_id, fingerprint, channels)
        except Exception:
            logger.warning(
                "ssh gateway: attachment %s left open in the audit table",
                attachment_id,
                exc_info=True,
            )

    def _schedule(self, coro) -> Optional[asyncio.Task]:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No loop means nothing can run the close; closing the coroutine
            # explicitly avoids a "never awaited" warning that would point at
            # this line instead of at whatever called us off-loop.
            coro.close()
            logger.warning("ssh gateway: no running loop to close an audit row on")
            return None
        task = loop.create_task(coro)
        _BACKGROUND_TASKS.add(task)
        task.add_done_callback(_BACKGROUND_TASKS.discard)
        return task

    # --- wiring -------------------------------------------------------------

    async def _session_factory(self, process) -> None:
        """Open the upstream session, or refuse with a readable reason."""
        self._note_channel_type(process)
        try:
            upstream = await self._upstream()
        except TargetDenied:
            self._refuse(process, *REFUSAL_MESSAGES["denied"])
            return
        except TargetUnavailable as exc:
            self._refuse(
                process,
                *REFUSAL_MESSAGES.get(exc.state, REFUSAL_MESSAGES["unreachable"]),
            )
            return
        except AttachmentLimitReached:
            self._refuse(process, ATTACHMENT_LIMIT_MESSAGE, ATTACHMENT_LIMIT_EXIT_CODE)
            return
        except GatewayMisconfigured:
            self._refuse(process, MISCONFIGURED_MESSAGE, MISCONFIGURED_EXIT_CODE)
            return
        await proxy_session(process, upstream)

    def _refuse(self, process, message: str, code: int) -> None:
        """Optimistic-SUCCESS refusal: stderr, then an exit code.

        The channel is binary (``encoding=None`` on the listener), so stderr
        takes bytes.
        """
        try:
            process.stderr.write(f"srw: {message}\n".encode())
        except Exception:
            # The client can disconnect in this same window; what matters is
            # that exit() below still runs and the channel closes.
            logger.debug("ssh gateway: could not write a refusal to stderr")
        process.exit(code)

    def _note_channel_type(self, process) -> None:
        """Record what kind of channel this was, for the audit row.

        Recorded BEFORE the upstream is tried, so a refused attach still
        leaves 'they asked for sftp and were turned away' in the table --
        which is what the table is for. The value is mapped into a
        two-word vocabulary rather than echoed; see SESSION_CHANNEL_TYPE.
        """
        try:
            subsystem = getattr(process, "subsystem", None)
        except Exception:  # pragma: no cover - defensive
            subsystem = None
        channel_type = (
            SFTP_CHANNEL_TYPE
            if subsystem == SFTP_CHANNEL_TYPE
            else SESSION_CHANNEL_TYPE
        )
        if channel_type not in self._channel_types:
            self._channel_types.append(channel_type)

    async def _attached_target(self) -> SshTarget:
        """Resolve this connection's workspace once, and attach to it.

        Once per CONNECTION, not per channel: a JetBrains client opens
        several channels at once, and resolving per channel would charge the
        workspace attachment cap several times over and write an audit row
        for each. The lock is what makes "once" true under the concurrent
        first opens that client produces.

        Only success is cached. A failed resolution is left uncached so a
        workspace that finishes restoring is reachable on the next channel
        instead of poisoning the whole connection; the per-connection channel
        cap is what bounds how much re-resolution that can cost.
        """
        if self._target is not None:
            return self._target

        missing = self._context.missing_required()
        if missing:
            # A readable stderr refusal beats a TypeError/AttributeError
            # escaping into asyncssh, where it becomes an opaque internal
            # error with nothing for either the user or the operator to act
            # on. The log line names the field; the user is told only that
            # the gateway is misconfigured.
            logger.error(
                "ssh gateway: GatewayContext is missing %s; cannot resolve a target",
                ", ".join(missing),
            )
            raise GatewayMisconfigured(missing)

        async with self._resolve_lock:
            if self._target is not None:
                return self._target

            try:
                target = await self._context.resolve(
                    self.handle, self.presented_fingerprint
                )
            except TargetUnavailable as exc:
                if exc.state != "vm_unsupported" or not self._authenticated:
                    raise
                if self._context.vm_admit is None:
                    raise
                target = await self._context.vm_admit(
                    connection_id=self._connection_id,
                    handle=self.handle,
                    fingerprint=self.presented_fingerprint,
                )
            if not self._context.limiter.try_attach(target.thread_id):
                raise AttachmentLimitReached(target.thread_id)
            self._attached_workspace = target.thread_id
            self._target = target

        try:
            await self._open_attachment_record()
        finally:
            # THE CHARGE ABOVE MUST NOT OUTLIVE A PEER THAT IS ALREADY GONE,
            # whatever happened to the audit row. ``connection_lost`` can
            # fire while ``resolve`` is still awaiting, and it then finds
            # ``_attached_workspace`` still None and detaches nothing -- so
            # this is the only place the charge can be given back.
            #
            # It is a ``finally``, not a line after the call, because
            # ``_open_attachment_record`` has early returns (no
            # ``record_attach`` bound at all, and the audit write failing),
            # and each of those used to skip the release and leak a slot
            # permanently. ``_attachments`` only ever climbs, and the
            # default cap is 4, so four such leaks make the workspace
            # refuse SSH until the gateway pod restarts. Worse, the two
            # triggering conditions CORRELATE: a degraded orchestrator makes
            # both the lost peer and the failed audit write likelier at the
            # same moment.
            if self._connection_closed:
                self._release_attachment()
                self._close_vm_access()
        return self._target

    async def _open_attachment_record(self) -> None:
        """Open the audit row. Opening it is ALL this does.

        It deliberately owns no lifecycle: its caller's ``finally`` re-checks
        ``_connection_closed`` on every path out of here, which is what
        releases both the workspace charge and this row if the peer went
        away meanwhile. That re-check used to live at the bottom of this
        method, where the two early returns below skipped it -- one place
        that always runs beats three places that must each remember to.

        Why a re-check at all: ``connection_lost`` is synchronous and reads
        ``_attachment_id`` directly, so a peer lost while this POST is in
        flight sees ``None`` and schedules no close, leaving the row open
        forever. Note this is NOT fixed by moving the call inside
        ``_resolve_lock`` -- ``connection_lost`` never takes that lock, so
        the same in-flight POST loses the same race one line earlier. The
        race is between an await and a synchronous callback, not between two
        awaits, and only re-checking after the await can see it.
        """
        record = self._context.record_attach
        if record is None:
            return
        try:
            self._attachment_id = await record(
                self.presented_fingerprint, self.handle, self._client_ip
            )
        except Exception:
            # Audit is bookkeeping. A control-plane hiccup here must not cost
            # the user a session that already authenticated AND resolved.
            logger.warning(
                "ssh gateway: no audit row opened for this attachment",
                exc_info=True,
            )

    async def _upstream(self):
        """Resolve, attach, and connect -- once per CONNECTION.

        One dial, not one per channel: each dial mints a certificate and
        costs a full SSH handshake against the workspace, and a JetBrains
        client opens several channels at once. ``_attached_target`` already
        makes resolution and the attachment charge once-per-connection; this
        lock does the same for the dial itself.

        A dial failure becomes ``TargetUnavailable("unreachable")`` rather
        than escaping: an exception out of here reaches ``_session_factory``,
        which does not catch it, and from there asyncssh's task reaper,
        which tears the whole connection down with an opaque internal error.
        The reason survives in the log instead -- at ``error`` for a host-key
        mismatch, which is a security signal (the fingerprint the
        provisioner attested through the Kubernetes API no longer matches
        the pod answering) rather than an outage.
        """
        connection = self._upstream_conn
        if self._vm_access_closed:
            raise TargetUnavailable("stale_binding")
        if connection is not None:
            return connection
        if self._connection_closed:
            # A deferred subsystem reply can still be in flight after the
            # peer has gone. Dialling a workspace for a client that no
            # longer exists is pure waste.
            raise TargetUnavailable("unreachable")

        target = await self._attached_target()

        async with self._upstream_lock:
            if self._upstream_conn is not None:
                return self._upstream_conn
            try:
                connection = await connect_upstream(self._context, target)
            except asyncssh.HostKeyNotVerifiable as exc:
                # Deliberately does not assert WHICH way the pin failed:
                # ``_PinnedClient`` refuses both a genuine mismatch and a
                # malformed attested fingerprint, and logs its own line
                # naming which of the two happened immediately before this
                # one. Both are operator signals, hence ``error``.
                logger.error(
                    "ssh gateway: could not verify workspace %s's host key "
                    "against the attested fingerprint; refusing the inner "
                    "hop: %r",
                    target.thread_id,
                    exc,
                )
                self._close_vm_access()
                raise TargetUnavailable("unreachable") from exc
            except Exception as exc:
                logger.warning(
                    "ssh gateway: could not open the inner hop to workspace %s: %r",
                    target.thread_id,
                    exc,
                )
                self._close_vm_access()
                raise TargetUnavailable("unreachable") from exc

            if self._connection_closed:
                # Same shape as the attach-slot leak: ``connection_lost``
                # ran while the dial was in flight, found ``_upstream_conn``
                # still None, and closed nothing. This is the only place
                # that connection can be given back.
                #
                # Published and immediately reclaimed, rather than closed
                # inline, so that EVERY upstream close goes through the one
                # method -- same bounded wait_closed, same drain coverage,
                # nothing to keep in sync between two teardown paths.
                self._upstream_conn = connection
                self._close_upstream()
                raise TargetUnavailable("unreachable")

            self._upstream_conn = connection
            if target.backend == "vm" and self._context.vm_renew is not None:
                self._vm_renew_task = asyncio.create_task(self._renew_vm_access(target))
            return connection

    def _forward_connection(self, dest_host: str, dest_port: int):
        """direct-tcpip, returned as an AWAITABLE rather than a callable.

        asyncssh's ``_process_direct_tcpip_open`` (connection.py:6394-6417)
        inspects what ``connection_requested`` returned before awaiting
        anything: a callable is wrapped in ``SSHTCPStreamSession`` and later
        invoked as ``handler(reader, writer)``, so returning a function here
        never dials at all. A coroutine falls through to
        ``chan.process_open``, which awaits it (channel.py:485) before the
        channel-open confirmation is sent -- exactly the ordering this needs.
        """
        return self._open_forwarded_connection(dest_host, dest_port)

    async def _open_forwarded_connection(self, dest_host: str, dest_port: int):
        """Dial upstream, then splice the forwarded channel onto it.

        Every refusal is converted to ``ChannelOpenError``:
        ``_finish_open_request`` catches that and nothing else
        (channel.py:511), so any other exception kills the whole connection
        instead of the one channel. The reason travels in the failure, which
        is where an ``ssh -L`` client prints it.
        """
        try:
            upstream = await self._upstream()
        except TargetDenied:
            raise asyncssh.ChannelOpenError(
                OPEN_ADMINISTRATIVELY_PROHIBITED, REFUSAL_MESSAGES["denied"][0]
            ) from None
        except TargetUnavailable as exc:
            message, _ = REFUSAL_MESSAGES.get(
                exc.state, REFUSAL_MESSAGES["unreachable"]
            )
            raise asyncssh.ChannelOpenError(OPEN_CONNECT_FAILED, message) from None
        except AttachmentLimitReached:
            raise asyncssh.ChannelOpenError(
                OPEN_ADMINISTRATIVELY_PROHIBITED, ATTACHMENT_LIMIT_MESSAGE
            ) from None
        except GatewayMisconfigured:
            raise asyncssh.ChannelOpenError(
                OPEN_ADMINISTRATIVELY_PROHIBITED, MISCONFIGURED_MESSAGE
            ) from None

        if self._conn is None:  # pragma: no cover - defensive
            # connection_made is asyncssh's own contract; a listener that
            # never fired it cannot forward anything.
            raise asyncssh.ChannelOpenError(
                OPEN_ADMINISTRATIVELY_PROHIBITED, MISCONFIGURED_MESSAGE
            )
        return await self._conn.forward_tunneled_connection(
            upstream, dest_host, dest_port
        )


# --- the inner hop --------------------------------------------------------

# The workspace's host key is pinned by fingerprint, so asyncssh must be given
# an EMPTY known-hosts tuple rather than None. Passing None disables validation
# entirely and ``validate_host_public_key`` is never called at all -- verified
# on 2.24.0 against a real loopback server, and pinned by
# ``test_known_hosts_none_is_the_negative_control``, which connects with a
# deliberately WRONG fingerprint and succeeds once this constant is None.
# ``canvas_ssh.py`` and ``docker_provisioner.py`` carry the same constant for
# the same reason.
EMPTY_KNOWN_HOSTS = ((), (), (), (), (), (), ())

# The single Unix user every workspace image bakes in
# (docker/Dockerfile.workspace). This is ONLY the SSH login name.
#
# It is deliberately NOT the certificate's principal (Task 9 / ruling
# G3-G44): every workspace shares this same login user and trusts the same
# CA, so a certificate whose principal was this fixed constant would
# authenticate to every workspace in the fleet for its whole validity
# window. connect_upstream() below mints each certificate's principal from
# the resolved target's thread id instead, and every workspace's
# AuthorizedPrincipalsFile (populated at boot from SRW_WORKSPACE_OWNER_ID --
# see docker/workspace-entrypoint.sh) checks that principal against its own
# identity rather than against this login name, per ``AuthorizedPrincipalsFile``
# semantics (OpenSSH matches certificate principals against that file's
# contents, not the login name, once it is set). See ssh_gateway_ca's module
# docstring for the fuller "what a single principal does and does not scope"
# discussion.
WORKSPACE_PRINCIPAL = "agent-host"

# Bounds on the inner hop. Without them a pod that black-holes packets hangs
# the user's channel forever -- and, on the subsystem path, hangs a DEFERRED
# reply, so the client waits on an answer that never comes rather than
# failing. Sized like canvas_ssh.py's pinned transport, which dials the same
# workspaces.
UPSTREAM_CONNECT_TIMEOUT_SECONDS = 10
UPSTREAM_LOGIN_TIMEOUT_SECONDS = 15


class _PinnedClient(asyncssh.SSHClient):
    """Validates the workspace host key against the fingerprint the
    provisioner attested through the Kubernetes API -- not an SSH scan.

    Deliberately omits ``super().__init__()``: ``SSHClient`` defines none,
    and asyncssh only ever calls the callbacks below.
    """

    def __init__(self, expected_fingerprint: str):
        self._expected = expected_fingerprint

    def validate_host_public_key(self, host, addr, port, key) -> bool:
        del host, addr, port
        try:
            return secrets.compare_digest(key.get_fingerprint("sha256"), self._expected)
        except TypeError:
            # ``compare_digest`` raises TypeError for None, bytes, or a
            # non-ASCII str. It already failed CLOSED before this guard --
            # the exception escaped the callback and asyncssh refused the
            # connection -- but it surfaced through ``_upstream``'s generic
            # handler as "could not open the inner hop", at ``warning``, as
            # though the workspace were unreachable. It is not: OUR OWN
            # attested data is malformed, which is an operator problem and a
            # different thing to go looking at. ``resolve_target``'s
            # ``_is_valid_identifier`` check makes this unreachable today,
            # so this is defence in depth for a future path that skips it.
            logger.error(
                "ssh gateway: the attested host-key fingerprint is not a "
                "usable string (got %s); refusing the inner hop",
                type(self._expected).__name__,
            )
            return False


async def connect_upstream(context: GatewayContext, target: SshTarget):
    """Open the inner hop with a freshly minted, short-lived certificate.

    Nothing here is cached or reused: the gateway holds a CA, not a standing
    credential, so every dial mints its own keypair and certificate and the
    blast radius of a compromised gateway is "can mint until the CA is
    rotated" rather than "holds a key that opens every workspace forever".

    ``target.pod_ip`` is a misnomer carried from the orchestrator's API: it
    holds a Kubernetes Service DNS name, and must not be validated as an IP.
    """
    # The certificate's PRINCIPAL is this specific workspace's thread id --
    # not WORKSPACE_PRINCIPAL, which names only the shared Unix login user
    # passed as `username` below. See WORKSPACE_PRINCIPAL's docstring.
    expected = target.host_key_fingerprint
    if target.backend == "vm":
        key_path = os.environ.get("SSH_KEY_PATH", "")
        if not key_path or not os.path.isfile(key_path):
            raise TargetUnavailable("unreachable")
        return await asyncssh.connect(
            target.pod_ip,
            port=target.pod_port,
            username=WORKSPACE_PRINCIPAL,
            client_keys=[key_path],
            known_hosts=EMPTY_KNOWN_HOSTS,
            server_host_key_algs=["ssh-ed25519"],
            client_factory=lambda: _PinnedClient(expected),
            encoding=None,
            connect_timeout=UPSTREAM_CONNECT_TIMEOUT_SECONDS,
            login_timeout=UPSTREAM_LOGIN_TIMEOUT_SECONDS,
        )
    key, cert = context.ca.mint(target.thread_id)
    return await asyncssh.connect(
        target.pod_ip,
        port=target.pod_port,
        username=WORKSPACE_PRINCIPAL,
        client_keys=[(key, cert)],
        known_hosts=EMPTY_KNOWN_HOSTS,
        # Client-side option, unlike on a listener: "host-key algorithms
        # this client will accept FROM a server". Workspaces also generate
        # an RSA host key, and negotiating it would fail a pin recorded
        # against the ed25519 one. See ssh_gateway_config.SERVER_HOST_KEY_ALGS
        # for the server-side half of this distinction.
        server_host_key_algs=["ssh-ed25519"],
        # DO NOT "tidy" this lambda into a direct reference or a partial.
        # asyncssh calls it when the connection is built, so ``_PinnedClient``
        # is resolved from module globals at that moment -- which is the seam
        # ``test_known_hosts_none_is_the_negative_control`` and its siblings
        # patch to prove the pin callback fires (or, with known_hosts=None,
        # never fires). Binding the class here instead would leave those
        # controls green while testing nothing.
        client_factory=lambda: _PinnedClient(expected),
        encoding=None,
        connect_timeout=UPSTREAM_CONNECT_TIMEOUT_SECONDS,
        login_timeout=UPSTREAM_LOGIN_TIMEOUT_SECONDS,
    )
