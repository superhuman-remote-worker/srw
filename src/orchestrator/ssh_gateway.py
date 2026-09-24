"""Dedicated front door for workspace SSH.

This process contains no platform API, no cockpit routes and no database
credentials. It terminates SSH and proxies into workspaces, and it is deployed
separately from the orchestrator so that control-plane rollouts — which happen
on nearly every commit — do not drop live interactive sessions.

TWO TRANSPORTS, AND THEY ARE NOT EQUALLY PROTECTED. Read this before assuming
a control applies to both:

* **WSS** — ``/api/ssh/attach``. The WebSocket is pumped byte-for-byte into
  one half of a ``socket.socketpair()``; the other half is handed to
  ``asyncssh.run_server``, so there is no loopback TCP listener on this path
  and therefore no port-to-IP map and no reuse race. Guarded by an
  exact-match **Origin** check and a short-lived, user-bound **bearer token**,
  both applied before the upgrade.
* **TCP** — ``:2222`` (``GatewayConfig.ssh_listen_port``). A plain listening
  socket; each accepted socket goes to the same ``GatewaySSHServer`` factory
  and the same ``GatewayLimiter``. **Neither the Origin check nor the bearer
  token applies here, and neither can**: a raw SSH client sends no Origin
  header and has nowhere to put a bearer token before the SSH banner. This
  transport's controls are the LoadBalancer's ``allowedClientCIDRs``
  (Task 11 refuses to render an unscoped SSH Service) plus SSH public-key
  authentication, which is the same posture as any internet-facing sshd.
  Do not "harden" the WSS path and record the result as a property of the
  gateway; state which transport you mean.

Both transports charge the same pre-auth limiter, and on both the slot is
released exactly once, on every exit path — including an ``accept()`` that
raises, and asyncssh's own ``login_timeout`` disconnect. This seam has leaked
three times in this plan already (``detach`` never called, an early return
skipping a release, an upstream connection never closed), and a leak here is
not subtle: the counter only climbs, so ``max_preauth_connections`` leaks
turn into a gateway that refuses everyone until the pod restarts.

WHAT THAT SLOT ACTUALLY BOUNDS, because the name understates it: the slot is
held for the whole connection, not just until authentication completes, so
``max_preauth_connections`` (default 64) and ``max_preauth_connections_per_
source`` (default 16) are in practice caps on CONCURRENT CONNECTIONS. That is
deliberate — it is the only global concurrency bound this process has, and
releasing at auth would leave an authenticated client free to hold unbounded
sockets — but it has two sizing consequences an operator must know. A NAT'd
office reaching the TCP listener shares one source bucket, so the 17th
simultaneous session from that office is refused; and on the WSS path, if
``SSH_GATEWAY_TRUSTED_PROXIES`` is not set to the ingress hop, EVERY WSS
client is bucketed under the ingress pod's own IP and the whole fleet shares
16 slots. Task 11 must set that variable, and raise these caps if sessions,
rather than handshakes, are what needs bounding.

That variable stopped being advisory when refused handshakes started being
metered (``_refuse_handshake``): refusals share the source's 60-second rate
window, so with every WSS client bucketed under one ingress IP, an
unauthenticated flood from anywhere burns the budget everyone else is
admitted from. Bucketed by real client address, it burns only the flooder's
own. It is therefore a BOOT CONDITION, not a recommendation: ``load_config``
refuses to start without it, and a deployment with nothing in front of the
gateway must say ``SSH_GATEWAY_TRUSTED_PROXIES=none`` out loud rather than
inherit the vulnerable state by omission.

The credential the USER presents is NOT ``MCP_INTERNAL_KEY``. See
``services/ssh_gateway_token.py`` for the full account (ruling G38); in short,
that value is the platform's service-to-service key for ~50
``require_internal`` endpoints, and handing it to every SSH user's laptop
would have been the seam design §6.3 warns about twice.

Three asyncssh gotchas, all hit in testing and all still true on 2.24.0:
``run_server`` returns an ``_ACMWrapper``, not a coroutine, so it cannot be
handed straight to ``create_task``; it does not return until authentication
completes (``wait='auth'``), so awaiting it before the client connects
deadlocks; and ``get_extra_info('peername')`` on a socketpair is ``''``, so
the real client IP has to travel in the ``server_factory`` closure.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import functools
import ipaddress
import logging
import os
import socket
from typing import Optional, Sequence

import asyncssh
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route, WebSocketRoute

from orchestrator.services.ssh_gateway_ca import load_user_ca
from orchestrator.services.ssh_gateway_client import (
    close_attachment,
    mark_key_used,
    record_attachment,
    resolve_target,
    vm_access,
)
from orchestrator.services.ssh_gateway_config import load_config, server_options
from orchestrator.services.ssh_gateway_limits import GatewayLimiter
from orchestrator.services.ssh_gateway_server import (
    GatewayContext,
    GatewaySSHServer,
    drain_background_tasks,
)
from orchestrator.services.ssh_gateway_token import verify_attach_token

logger = logging.getLogger(__name__)

# Application-defined close codes (4000-4999 is the private range), distinct
# per refusal.
#
# WHAT THE CLIENT ACTUALLY SEES, measured against a real uvicorn rather than
# assumed: nothing. All three of these fire BEFORE ``accept()``, and a close
# before accept is an ASGI handshake denial, which uvicorn renders as a bare
# ``HTTP 403`` -- identical for all three (verified live: no-token, bad-origin
# and internal-key-as-token all came back 403). The codes are therefore a
# GATEWAY-SIDE distinction, useful in logs and in the tests, not something
# plan 3's helper can branch on. If that helper needs to tell "your token
# expired, fetch another" from "wrong host", it needs a real denial response
# (Starlette's ``send_denial_response``, which depends on the ASGI server
# advertising the ``websocket.http.response`` extension) -- do not assume
# these numbers reach it.
WS_TOKEN_REFUSED = 4401
WS_ORIGIN_REFUSED = 4403
WS_RATE_REFUSED = 4429

# How much of one WebSocket frame / socket read is moved at a time.
PUMP_CHUNK_BYTES = 65536

# Backlog for the TCP listener. Above the default pre-auth cap (64) so the
# kernel queue is never the thing that refuses a burst -- GatewayLimiter is,
# and it can say so.
TCP_LISTEN_BACKLOG = 128

# accept() errors that mean THE LISTENING SOCKET is gone, not that one
# connection failed. Everything else is survivable and must not end the loop.
_FATAL_ACCEPT_ERRNOS = frozenset({errno.EBADF, errno.EINVAL, errno.ENOTSOCK})

# ...and the subset that means "the process is out of resources", where
# retrying immediately just burns CPU. asyncio's own accept loop pauses
# ACCEPT_RETRY_DELAY = 1s on exactly these; half that keeps a recovered
# gateway responsive without spinning.
_RESOURCE_ACCEPT_ERRNOS = frozenset(
    {errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM}
)
ACCEPT_RETRY_DELAY_SECONDS = 0.5


def origin_allowed(origin: Optional[str], allowed: Sequence[str]) -> bool:
    """Exact-match Origin check. **WSS transport only.**

    Starlette does no same-origin validation of its own, and this endpoint
    shares a hostname with cockpit's cookie-authenticated API — the adjacency
    that made Gitpod's CVE-2023-0957 end in permanent SSH persistence.
    Prefix matching would accept ``cockpit.srw.works.evil.example``.
    """
    if not origin or not allowed:
        return False
    return origin in tuple(allowed)


def client_ip(websocket, trusted_proxies: Sequence[str] = ()) -> str:
    """The source address this connection is rate-limited and audited as.

    ``X-Forwarded-For`` is believed only when the socket peer is one of
    ``trusted_proxies``, and then read RIGHT to LEFT, skipping entries that
    are themselves trusted proxies; the first untrusted entry is the client.
    Every clause matters:

    * Trusting it unconditionally (the plan's original ``_client_ip``) lets
      any client send a fresh header per connection and mint a fresh
      rate-limit bucket each time, which nullifies every per-source control
      in ``GatewayLimiter`` — the module that exists precisely because
      asyncssh ships no MaxStartups or PerSourceMaxStartups.
    * Taking the FIRST entry (also the original) reads the attacker's half of
      the header. Proxies APPEND the real peer to whatever the client already
      sent, so everything left of the last untrusted entry is client-supplied
      fiction.
    * Taking the RIGHTMOST entry unconditionally (this function until
      2026-09-05) is only correct behind exactly one trusted hop. Behind
      cloudflared → Traefik the ingress appends the *tunnel connector's* pod
      address, so the rightmost entry is the second proxy: every external
      user collapsed into one per-source bucket keyed by cloudflared's pod
      IP, and the audit trail recorded that constant instead of the client.
      Skipping trusted entries handles any chain depth and degrades to the
      old behaviour for the one-hop case.

    When every entry is trusted, the leftmost is returned: the originator
    itself lives inside the trusted range (an in-cluster caller through the
    ingress), and falling back to the peer would collapse it into the
    ingress hop's bucket — the defect this walk exists to fix.

    The TCP transport does not come through here at all: it has a real socket
    peer and no headers.
    """
    peer = getattr(getattr(websocket, "client", None), "host", "") or ""
    if trusted_proxies and _address_matches(peer, trusted_proxies):
        forwarded = websocket.headers.get("x-forwarded-for", "")
        entries = [e.strip() for e in forwarded.split(",") if e.strip()]
        for entry in reversed(entries):
            if not _address_matches(entry, trusted_proxies):
                return entry
        if entries:
            return entries[0]
    return peer or "unknown"


def _address_matches(address: str, networks: Sequence[str]) -> bool:
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        return False
    for entry in networks:
        try:
            network = ipaddress.ip_network(entry, strict=False)
        except ValueError:
            # load_config already refuses these at boot; skipping rather than
            # raising keeps one bad entry from failing open OR taking the
            # whole listener down mid-connection.
            continue
        if parsed in network:
            return True
    return False


def attach_principal(websocket, config) -> Optional[str]:
    """The user id behind this WebSocket's bearer token, or ``None``.

    **WSS transport only** — HTTP-layer authentication, before the upgrade.
    Nobody in this product category ships an unauthenticated SSH-speaking
    WebSocket: Gitpod authenticates before the upgrade, Codespaces sends a
    tunnel JWT, SSM uses SigV4, Coder requires a session token.

    The token is a short-lived HMAC minted by the orchestrator for a specific
    user (``POST /api/ssh/attach-token``), verified here with
    ``SESSION_JWT_SECRET``. It is deliberately NOT ``config.internal_key``:
    that is the gateway's own service credential for the orchestrator's
    internal API, and comparing a user-presented string against it would put
    the platform's master key in every user's home directory.

    Header-only, deliberately: a query parameter lands in every access log and
    in browser history, and the only intended client is plan 3's helper, which
    can set a header. Returns the id rather than a bool so the caller has to
    hold the answer to "who?".
    """
    presented = websocket.headers.get("authorization") or ""
    if presented[:7].lower() != "bearer ":
        return None
    return verify_attach_token(presented[7:].strip(), config.session_jwt_secret)


async def attach_endpoint(websocket) -> None:
    """Pump one authenticated WebSocket into an SSH server on a socketpair."""
    state = websocket.app.state
    config = state.config
    source = client_ip(websocket, config.trusted_proxies)

    if not origin_allowed(websocket.headers.get("origin"), config.allowed_origins):
        await _refuse_handshake(websocket, state, source, WS_ORIGIN_REFUSED, "origin")
        return

    principal = None
    if config.require_wss_token:
        principal = attach_principal(websocket, config)
        if principal is None:
            await _refuse_handshake(
                websocket, state, source, WS_TOKEN_REFUSED, "attach token"
            )
            return

    if not state.limiter.try_admit(source):
        # NOT re-metered: try_admit already charged this source's own rate
        # window on its way to refusing, and counting it twice would halve
        # the effective budget of a client that is merely at its limit.
        await websocket.close(code=WS_RATE_REFUSED)
        return

    # THE TRY OPENS ON THE LINE AFTER A SUCCESSFUL ADMIT, WITH NOTHING
    # BETWEEN. The plan's original put ``await websocket.accept()`` here,
    # outside the try, so an accept that raised -- a client that vanishes
    # mid-handshake, which is routine on an internet-facing listener -- leaked
    # its pre-auth slot permanently.
    try:
        logger.info("ssh gateway: wss attach from %s for user %s", source, principal)
        await _attach_over_socketpair(websocket, state, source)
    finally:
        state.limiter.release(source)


async def _refuse_handshake(websocket, state, source: str, code: int, why: str) -> None:
    """Close a handshake we turned away, and meter it against its source.

    Before this, Origin and token refusals returned ahead of ``try_admit``, so
    an unauthenticated handshake flood was not metered at all -- only a client
    that got PAST both checks was ever rate limited, the inverse of the usual
    ordering (review finding 5).

    It charges the per-source RATE window and deliberately not a concurrency
    slot: a slot charged here belongs to a connection that was refused and
    will never reach a release path, so a bad-Origin flood would drain the
    global pool and lock every legitimate user out -- a self-inflicted denial
    strictly worse than the gap it closes. Over budget therefore changes the
    log level and nothing else; the handshake was already refused.
    """
    within_budget = state.limiter.note_handshake_refusal(source)
    if within_budget:
        logger.info("ssh gateway: refused wss handshake from %s (%s)", source, why)
    else:
        logger.warning(
            "ssh gateway: %s over its handshake budget, still refusing (%s)",
            source,
            why,
        )
    await websocket.close(code=code)


async def _attach_over_socketpair(websocket, state, source: str) -> None:
    await websocket.accept()

    srv_sock, ws_sock = socket.socketpair()
    ws_sock.setblocking(False)
    connection = None
    try:
        # run_server is an async context manager wrapper, not a coroutine, so
        # it is wrapped before create_task; and it does not return until
        # authentication completes, so it cannot be awaited before the pumps
        # start moving bytes.
        server_task = asyncio.create_task(_run_ssh_on_socket(srv_sock, state, source))
        try:
            await _pump(websocket, ws_sock)
        finally:
            server_task.cancel()
            # Awaited, not just cancelled: an unretrieved exception from
            # _serve() surfaces as an "exception was never retrieved" warning
            # at garbage-collection time instead of an error anyone reads.
            results = await asyncio.gather(server_task, return_exceptions=True)
            connection = _completed_connection(results[0], source)
    finally:
        if connection is not None:
            connection.abort()
        for sock in (srv_sock, ws_sock):
            with contextlib.suppress(OSError):
                sock.close()
        # Close it ourselves rather than letting the ASGI server tear it down
        # on return: the client otherwise observes an abnormal close. Errors
        # are expected here (the peer is usually gone already, and Starlette
        # raises on a socket that has finished closing) and carry nothing the
        # operator can act on.
        with contextlib.suppress(Exception):
            await websocket.close()


def _completed_connection(result, source: str):
    """The SSHServerConnection ``_run_ssh_on_socket`` produced, if any."""
    if isinstance(result, asyncio.CancelledError):
        # Cancelled before authentication finished: normal, and the common
        # case (a user closes the terminal before typing anything).
        return None
    if isinstance(result, BaseException):
        logger.info(
            "ssh gateway: attach from %s ended without authenticating: %s",
            source,
            result,
        )
        return None
    return result


async def _run_ssh_on_socket(sock, state, source: str):
    """Speak SSH on ``sock``. The one place both transports meet.

    The real client IP travels in this closure because
    ``get_extra_info('peername')`` is ``''`` on a socketpair -- and on the TCP
    path it is passed in from ``accept()`` for the same reason the WSS path
    cannot read it off the socket: one code path, one source of truth.
    """

    def _server_factory():
        return GatewaySSHServer(state.context, source)

    return await asyncssh.run_server(
        sock, server_factory=_server_factory, **server_options(state.config)
    )


async def _pump(websocket, sock) -> None:
    """Move bytes both ways until either direction ends.

    ``asyncio.wait`` rather than ``gather``: gather leaves the sibling
    running when one side raises, and a pump left reading a closed socket
    is a task that never finishes and a connection that never releases its
    slot. Every task's exception is retrieved here, not left for the loop's
    handler to complain about at collection time.
    """
    loop = asyncio.get_running_loop()
    tasks = {
        asyncio.create_task(_ws_to_sock(websocket, sock, loop)),
        asyncio.create_task(_sock_to_ws(websocket, sock, loop)),
    }
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if task.cancelled():
                continue
            if task.exception() is not None:
                logger.debug("ssh attach closed", exc_info=task.exception())
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _ws_to_sock(websocket, sock, loop) -> None:
    while True:
        data = await websocket.receive_bytes()
        if not data:
            return
        await loop.sock_sendall(sock, data)


async def _sock_to_ws(websocket, sock, loop) -> None:
    while True:
        data = await loop.sock_recv(sock, PUMP_CHUNK_BYTES)
        if not data:
            return
        await websocket.send_bytes(data)


class SshTcpListener:
    """The raw SSH listener on 2222.

    Task 11 ships ``containerPort: 2222`` and an optional LoadBalancer on it,
    and every step of Task 12's live gate is an ``ssh -p 2222`` command, so
    without this the chart fronts a dead port and the plan cannot gate itself.
    A raw ``accept()`` loop rather than ``asyncio.start_server`` because
    ``asyncssh.run_server`` wants the socket itself, and because the peer
    address is needed at factory-construction time.

    NO ORIGIN CHECK AND NO BEARER TOKEN HERE -- see the module docstring. The
    controls on this path are the LoadBalancer's ``allowedClientCIDRs`` and
    SSH public-key auth.
    """

    def __init__(self, sock: socket.socket, state):
        self._sock = sock
        self._state = state
        self._accept_task: Optional[asyncio.Task] = None
        self._connections: set[asyncio.Task] = set()
        self._serving = False
        self._closing = False

    @property
    def port(self) -> int:
        return self._sock.getsockname()[1]

    def start(self) -> None:
        self._serving = True
        self._accept_task = asyncio.create_task(self._accept_forever())

    def is_serving(self) -> bool:
        """True only while the accept loop is actually alive.

        ``healthz`` reads this. Before it existed, an accept loop that died
        left ``/healthz`` returning ok forever, so Kubernetes kept the pod
        Ready with a dead port 2222 and nothing anywhere said so.
        """
        return (
            self._serving
            and self._accept_task is not None
            and not self._accept_task.done()
        )

    async def _accept_forever(self) -> None:
        """Accept until told to stop, surviving anything survivable.

        ONE OSError USED TO END THIS LOOP FOR GOOD. EMFILE/ENFILE/ENOBUFS
        under fd pressure and a transient ECONNABORTED all arrive here, and
        asyncio's own ``_accept_connection`` -- the battle-tested loop this
        raw one replaced -- logs and continues on exactly those, pausing
        ``ACCEPT_RETRY_DELAY`` (1s there) when the cause is exhaustion rather
        than one bad connection. Doing otherwise produced the worst failure
        shape available in an orchestrated environment: a healthy-looking pod
        with SSH silently gone (review finding 1).

        Whatever does end this loop, ``is_serving()`` goes false in the
        ``finally`` and ``/healthz`` starts failing, so the pod stops
        advertising a port it is not serving.
        """
        loop = asyncio.get_running_loop()
        try:
            while True:
                try:
                    sock, address = await loop.sock_accept(self._sock)
                except asyncio.CancelledError:
                    raise
                except OSError as exc:
                    if self._closing or exc.errno in _FATAL_ACCEPT_ERRNOS:
                        # The listening socket was closed under us: shutdown,
                        # not an error worth a stack trace.
                        logger.info("ssh gateway: tcp listener stopped accepting")
                        return
                    logger.warning(
                        "ssh gateway: transient accept error on port %d (%s); "
                        "still listening",
                        self.port,
                        exc,
                    )
                    if exc.errno in _RESOURCE_ACCEPT_ERRNOS:
                        # Spinning on EMFILE just burns CPU while the fds are
                        # still gone; give the process a moment to recover.
                        await asyncio.sleep(ACCEPT_RETRY_DELAY_SECONDS)
                    continue
                except Exception:
                    # Nothing above covers this, so the loop is over -- but it
                    # ends LOUDLY and with the health check going down, not in
                    # silence (review finding 3).
                    logger.exception(
                        "ssh gateway: tcp accept loop failed; port %d is no "
                        "longer being served",
                        self.port,
                    )
                    return

                source = address[0] if address else "unknown"
                if not self._state.limiter.try_admit(source):
                    # Closed before a single SSH byte is written. asyncssh
                    # ships no MaxStartups at all (measured: 1000 silent
                    # pre-auth connections accepted in 0.3s), which is why
                    # this gate is ahead of run_server rather than inside it.
                    logger.info("ssh gateway: refused tcp connection from %s", source)
                    with contextlib.suppress(OSError):
                        sock.close()
                    continue
                self._spawn(sock, source)
        finally:
            self._serving = False

    def _spawn(self, sock: socket.socket, source: str) -> asyncio.Task:
        """Serve one admitted socket, releasing its slot exactly once.

        THE RELEASE IS A DONE CALLBACK, not a ``finally`` inside ``_serve``: a
        task cancelled before its first scheduling never executes a line of
        its coroutine, so an inner ``finally`` never runs and that slot leaks
        (review finding 4 -- shutdown-only, but this seam has leaked three
        times on this plan and uniformity is the point). A done callback runs
        exactly once for every task, including one cancelled before it starts.
        """
        try:
            task = asyncio.create_task(self._serve(sock, source))
        except BaseException:
            # The slot was charged by the caller and nothing else can give it
            # back if the task never exists.
            self._state.limiter.release(source)
            with contextlib.suppress(OSError):
                sock.close()
            raise
        self._connections.add(task)
        task.add_done_callback(self._connections.discard)
        task.add_done_callback(functools.partial(self._release_slot, source))
        return task

    def _release_slot(self, source: str, _task: asyncio.Task) -> None:
        self._state.limiter.release(source)

    async def _serve(self, sock: socket.socket, source: str) -> None:
        """Speak SSH on one accepted socket. Owns the socket, not the slot."""
        try:
            connection = await _run_ssh_on_socket(sock, self._state, source)
        except asyncio.CancelledError:
            with contextlib.suppress(OSError):
                sock.close()
            raise
        except Exception as exc:
            # Never authenticated: a scan, a login_timeout, a client that hung
            # up. asyncssh's own teardown aborts the connection when it got
            # that far, but a failure BEFORE the transport exists leaves this
            # socket to us.
            logger.info(
                "ssh gateway: tcp connection from %s ended without authenticating: %s",
                source,
                exc,
            )
            with contextlib.suppress(OSError):
                sock.close()
            return
        try:
            await connection.wait_closed()
        finally:
            connection.abort()

    async def close(self) -> None:
        self._closing = True
        if self._accept_task is not None:
            self._accept_task.cancel()
            await asyncio.gather(self._accept_task, return_exceptions=True)
            self._accept_task = None
        with contextlib.suppress(OSError):
            self._sock.close()
        connections = list(self._connections)
        for task in connections:
            task.cancel()
        if connections:
            await asyncio.gather(*connections, return_exceptions=True)


def _listen_socket(host: str, port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((host, port))
        sock.listen(TCP_LISTEN_BACKLOG)
    except OSError:
        sock.close()
        raise
    sock.setblocking(False)
    return sock


@contextlib.asynccontextmanager
async def lifespan(app):
    """Start the TCP listener, and drain the audit writes on the way out."""
    config = app.state.config
    listener = SshTcpListener(
        _listen_socket(config.ssh_listen_host, config.ssh_listen_port), app.state
    )
    listener.start()
    app.state.ssh_listener = listener
    logger.info(
        "ssh gateway: listening for ssh on %s:%d", config.ssh_listen_host, listener.port
    )
    try:
        yield
    finally:
        app.state.ssh_listener = None
        await listener.close()
        # Carried from Task 6: connection_lost SCHEDULES the attachment close
        # rather than awaiting it (the asyncssh callback is synchronous), so
        # without this a rollout mid-flight leaves those rows open forever --
        # silently, because the loop simply stops.
        undrained = await drain_background_tasks()
        if undrained:
            logger.warning(
                "ssh gateway: %d audit close(s) abandoned at shutdown", undrained
            )


async def healthz(request):
    """Liveness/readiness — and it must fail when the SSH listener is down.

    The route this process exists to serve is port 2222, not this one. A
    gateway whose accept loop has died answers here perfectly well, which is
    exactly the failure that must not be hidden: Kubernetes would keep the pod
    Ready, restart nothing, and SSH would simply be gone with no signal
    (review finding 1). Also reports down before startup and during shutdown,
    which is correct in both cases.
    """
    listener = getattr(request.app.state, "ssh_listener", None)
    if listener is None or not listener.is_serving():
        return JSONResponse(
            {"status": "degraded", "ssh_listener": "down"}, status_code=503
        )
    return JSONResponse({"status": "ok"})


def _build_app() -> Starlette:
    # Without this the gateway's INFO records are dropped on the floor. Nothing
    # in this process configures the root logger: uvicorn's dictConfig sets up
    # only its own `uvicorn.*` loggers, leaving root with no handler, so Python
    # falls back to `logging.lastResort` -- which is WARNING-level. Warnings,
    # errors and tracebacks still surface, which is why this is easy to miss;
    # what disappears is every refusal reason ("refused wss handshake from %s
    # (%s)", "refused tcp connection from %s"), i.e. exactly what an operator
    # needs when a user reports that SSH will not connect. Deliberately NOT the
    # orchestrator's configure_logging(): reaching it from this flattened image
    # means a try/except ModuleNotFoundError import dance, and getting that
    # wrong crash-loops the gateway to gain a log format. basicConfig is a
    # no-op when a handler already exists, so it cannot fight a real config.
    logging.basicConfig(
        level=os.environ.get("SSH_GATEWAY_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    # Every outbound hop dies without this, and the cause is nowhere near the
    # symptom. asyncssh's SSHClientConnectionOptions.prepare() calls
    # getpass.getuser() UNCONDITIONALLY (connection.py:8186) -- before it looks
    # at `username=` -- purely to locate ~/.ssh/config. getuser() consults
    # LOGNAME/USER/LNAME/USERNAME and only then falls back to a passwd lookup
    # for the current uid. This pod runs as uid 999 (chart securityContext) on
    # an image whose /etc/passwd has no such user, so the fallback raises and
    # asyncssh re-raises "Unknown local username". Every attach then fails with
    # "workspace is unreachable right now" and a single WARNING line.
    #
    # Passing `username=` does NOT help: that is the REMOTE login name, chosen
    # after this call. No asyncssh option reaches it -- the fix has to be
    # environmental, which is why it lives here rather than in connect_upstream.
    #
    # setdefault, not assignment: a real deployment that sets LOGNAME keeps it.
    # The value is only used to expand ~/.ssh/config, which this image does not
    # ship, so it never affects authentication.
    #
    # Found by the Task 12 live gate. It is invisible in production only by
    # accident: Dockerfile.orchestrator:60 creates uid 999, while the dev image
    # Tilt builds does not -- and any runAsUser override (OpenShift arbitrary
    # UIDs, a PSA/Kyverno mutation) reintroduces it on any image.
    os.environ.setdefault("LOGNAME", "srw-ssh-gateway")

    config = load_config()
    application = Starlette(
        routes=[
            WebSocketRoute("/api/ssh/attach", attach_endpoint),
            Route("/healthz", healthz),
        ],
        lifespan=lifespan,
    )
    application.state.config = config
    application.state.ssh_listener = None
    application.state.limiter = GatewayLimiter(
        max_preauth_connections=config.max_preauth_connections,
        preauth_rate_per_minute=config.preauth_rate_per_minute,
        max_channels_per_connection=config.max_channels_per_connection,
        max_attachments_per_workspace=config.max_attachments_per_workspace,
    )
    # Every callable pre-bound to the config with functools.partial, which is
    # the arrangement GatewayContext documents: a server that could re-derive
    # its own orchestrator client could also reach past the seam the tests
    # inject at. mark_key_used is bound HERE and not left defaulted to None --
    # left unbound, last_used_at never moves in production and the
    # stolen-key signal the whole column exists for is dead (ruling G1).
    # The gateway's host private key is the dedicated VM admission signer.
    # The orchestrator mounts only its public half; no extra fleet-wide HMAC
    # secret is distributed to agent pods for this capability.
    import asyncssh

    vm_signer = asyncssh.read_private_key(config.host_key_paths[0])
    application.state.context = GatewayContext(
        config=config,
        ca=load_user_ca(config.user_ca_path),
        limiter=application.state.limiter,
        resolve=functools.partial(resolve_target, config),
        record_attach=functools.partial(record_attachment, config),
        close_attach=functools.partial(close_attachment, config),
        mark_key_used=functools.partial(mark_key_used, config),
        vm_admit=functools.partial(vm_access, config, vm_signer, action="admit"),
        vm_renew=functools.partial(vm_access, config, vm_signer, action="renew"),
        vm_close=functools.partial(vm_access, config, vm_signer, action="close"),
    )
    return application


def create_app() -> Starlette:
    """Factory, so importing this module needs no environment.

    The Deployment runs ``uvicorn ssh_gateway:create_app --factory`` (Task
    11). There is deliberately no module-level ``app = _build_app()``: it
    would demand host keys, a CA and a secret at import time, which makes the
    module untestable and turns any config error into an import traceback.
    """
    return _build_app()
