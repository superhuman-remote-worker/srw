"""Tests for the gateway's inner hop: the dial into the workspace.

RULING G36 SHAPES THIS FILE. The brief's own tests all monkeypatch
``asyncssh.connect`` with a recorder, which means every one of them would
still pass with ``known_hosts="garbage"``, none of them exercises
``_PinnedClient.validate_host_public_key`` at all, and none of them proves
real asyncssh even accepts the kwarg set -- a bogus kwarg would sail through
a recorder and fail only in production. Those recorder tests are kept below
as cheap *shape* checks and are labelled as such; the security property is
tested against a real loopback asyncssh server instead, with the
``known_hosts=None`` trap wired up as an explicit negative control.

The full-stack section at the bottom stands up BOTH hops for real -- a
client, the gateway, and a workspace sshd that trusts the gateway's CA --
because three of this task's properties are invisible to any unit test:

* sftp must survive the proxy (asyncssh's own ``forward_tunneled_session``
  refuses the subsystem outright; see ssh_gateway_proxy's docstring),
* a refused workspace must fail sftp with a CHANNEL_FAILURE the client
  reports as ``ChannelOpenError`` -- not an optimistic SUCCESS followed by a
  dropped channel, which is what an sftp client renders as
  ``SFTPConnectionLost`` and what Task 12's live gate checks,
* ``direct-tcpip`` must reach the workspace's loopback through the upstream
  connection. The brief's ``_forward_connection`` returned a *callable*,
  which asyncssh interprets as an ``SSHTCPStreamSession`` handler taking
  ``(reader, writer)`` -- see ``test_forward_connection_is_awaited_not_called``.
"""

import asyncio
import contextlib
import inspect
import logging
from dataclasses import dataclass

import asyncssh
import pytest

import orchestrator.services.ssh_gateway_server as mod
from orchestrator.services.ssh_gateway_ca import SshUserCa
from orchestrator.services.ssh_gateway_client import (
    REFUSAL_MESSAGES,
    SshTarget,
    TargetUnavailable,
)
from orchestrator.services.ssh_gateway_limits import GatewayLimiter
from orchestrator.services.ssh_gateway_server import (
    GatewayContext,
    GatewaySSHServer,
    connect_upstream,
)

HANDLE = "s-7f3a91c2"
CLIENT_IP = "203.0.113.9"
WRONG_FINGERPRINT = "SHA256:" + "A" * 43


class Recorder:
    """Stand-in for ``asyncssh.connect`` that records what it was handed."""

    def __init__(self):
        self.calls = []

    async def __call__(self, host, port=None, **kwargs):
        self.calls.append(dict(kwargs, host=host, port=port))
        return object()

    @property
    def kwargs(self):
        return self.calls[-1]


class FakeCa:
    def __init__(self):
        self.mint_calls = []

    def mint(self, principal, **kw):
        self.mint_calls.append(principal)
        return object(), object()


def _target(**overrides) -> SshTarget:
    base = dict(
        thread_id="t1",
        user_id="u1",
        pod_ip="10.1.2.3",
        pod_port=30022,
        host_key_fingerprint="SHA256:" + "B" * 43,
        state="live",
        execution_lane="stateless",
        no_boot_watchdog=True,
    )
    base.update(overrides)
    return SshTarget(**base)


def _limiter(**overrides) -> GatewayLimiter:
    base = dict(
        max_preauth_connections=64,
        preauth_rate_per_minute=60,
        max_channels_per_connection=8,
        max_attachments_per_workspace=4,
    )
    base.update(overrides)
    return GatewayLimiter(**base)


def _context(**overrides) -> GatewayContext:
    base = dict(
        config=None,
        ca=FakeCa(),
        limiter=None,
        resolve=None,
        record_attach=None,
        close_attach=None,
    )
    base.update(overrides)
    return GatewayContext(**base)


class FakeProcess:
    """The shape ``_session_factory`` reads off an SSHServerProcess."""

    def __init__(self, subsystem=None):
        self.subsystem = subsystem
        self.command = None
        self.stderr = self
        self.written = []
        self.exit_code = None

    def write(self, data):
        self.written.append(data)

    def exit(self, code):
        self.exit_code = code

    @property
    def stderr_text(self) -> str:
        return b"".join(self.written).decode()


def _authenticated(server: GatewaySSHServer) -> GatewaySSHServer:
    server.handle = HANDLE
    server.presented_fingerprint = "SHA256:abc"
    return server


# =====================================================================
# Cheap shape checks. These prove the call is SPELLED right; they prove
# nothing at all about whether pinning works -- see the loopback section.
# =====================================================================


@pytest.mark.asyncio
async def test_never_passes_known_hosts_none(monkeypatch):
    """Shape check only. ``known_hosts=None`` disables validation entirely
    and the callback is never called; the property this asserts is really
    tested by ``test_known_hosts_none_is_the_negative_control``."""
    recorder = Recorder()
    monkeypatch.setattr(mod.asyncssh, "connect", recorder)
    await connect_upstream(_context(), _target())
    assert recorder.kwargs["known_hosts"] is not None


@pytest.mark.asyncio
async def test_pins_ed25519_only(monkeypatch):
    """Workspaces also generate an RSA host key; negotiating it would fail a
    pin recorded against the ed25519 key."""
    recorder = Recorder()
    monkeypatch.setattr(mod.asyncssh, "connect", recorder)
    await connect_upstream(_context(), _target())
    assert recorder.kwargs["server_host_key_algs"] == ["ssh-ed25519"]


@pytest.mark.asyncio
async def test_uses_a_minted_certificate_not_a_static_key(monkeypatch):
    recorder = Recorder()
    monkeypatch.setattr(mod.asyncssh, "connect", recorder)
    context = _context()
    target = _target()
    await connect_upstream(context, target)
    assert recorder.kwargs["client_keys"], "a minted keypair+cert must be passed"
    # Ruling G3/G44: the certificate's PRINCIPAL is minted per-workspace
    # (the resolved thread id), not the fixed WORKSPACE_PRINCIPAL constant --
    # that constant names the shared Unix LOGIN user only (asserted below).
    # A cert minted with the fixed constant would authenticate to every
    # workspace for its whole validity window, which is exactly what
    # AuthorizedPrincipalsFile plus this per-workspace principal closes.
    assert context.ca.mint_calls == [target.thread_id]


@pytest.mark.asyncio
async def test_login_username_is_the_fixed_unix_account_not_the_principal(
    monkeypatch,
):
    """The certificate's principal is per-workspace (previous test), but the
    Unix account every workspace image bakes in is not -- ``ssh-keygen(1)``'s
    principal and the SSH login username are different things, and
    ``AuthorizedPrincipalsFile`` is what lets them diverge: OpenSSH matches
    the certificate's principals against that file's contents rather than
    against the login name once it is set (docker/Dockerfile.workspace)."""
    recorder = Recorder()
    monkeypatch.setattr(mod.asyncssh, "connect", recorder)
    await connect_upstream(_context(), _target(thread_id="some-other-thread"))
    assert recorder.kwargs["username"] == mod.WORKSPACE_PRINCIPAL
    assert mod.WORKSPACE_PRINCIPAL == "agent-host"


@pytest.mark.asyncio
async def test_every_dial_mints_fresh_key_material(monkeypatch):
    """The gateway holds a CA, not a standing credential: two dials must not
    reuse one keypair. A recorder alone cannot see this unless the two calls
    are compared, which is why the recorder keeps every call."""
    recorder = Recorder()
    monkeypatch.setattr(mod.asyncssh, "connect", recorder)
    ca_key = asyncssh.generate_private_key("ssh-ed25519")
    context = _context(ca=SshUserCa(ca_key.export_private_key()))

    await connect_upstream(context, _target())
    await connect_upstream(context, _target())

    first, second = (call["client_keys"][0][0] for call in recorder.calls)
    assert first.convert_to_public().get_fingerprint(
        "sha256"
    ) != second.convert_to_public().get_fingerprint("sha256")


@pytest.mark.asyncio
async def test_connects_to_the_resolved_host_and_port(monkeypatch):
    recorder = Recorder()
    monkeypatch.setattr(mod.asyncssh, "connect", recorder)
    await connect_upstream(_context(), _target())
    assert recorder.kwargs["host"] == "10.1.2.3"
    assert recorder.kwargs["port"] == 30022


# =====================================================================
# The security property, against a real asyncssh server.
# =====================================================================


@dataclass
class _Workspace:
    port: int
    fingerprint: str
    ca: SshUserCa
    root: str


class _WorkspaceServer(asyncssh.SSHServer):
    """Stands in for a workspace pod's sshd.

    ``connection_requested`` returning True is what the real workspace's
    ``AllowTcpForwarding local`` grants, and is what the direct-tcpip test
    below forwards through.
    """

    def connection_requested(self, dest_host, dest_port, orig_host, orig_port):
        return True


async def _workspace_session(process) -> None:
    command = process.command or "shell"
    process.stdout.write(b"workspace:" + command.encode() + b"\n")
    process.exit(0)


@contextlib.asynccontextmanager
async def _workspace(tmp_path, principal="t1"):
    """A real sshd that trusts a fresh CA, exactly as a provisioned
    workspace trusts the gateway's via ``TrustedUserCAKeys`` -- AND scopes
    accepted certificates to one principal, exactly as a provisioned
    workspace's ``AuthorizedPrincipalsFile`` (populated at boot from
    ``SRW_WORKSPACE_OWNER_ID``) does. asyncssh's authorized-keys
    ``principals="..."`` option on the ``cert-authority`` line is its
    equivalent of that file: it makes validation check the certificate's
    principal list against this value instead of against the login
    username -- see ``connection.py``'s ``_validate_openssh_certificate``,
    where a ``principals`` key option sets ``cert_user = None`` rather than
    the username. Defaults to ``"t1"``, matching ``_target()``'s default
    ``thread_id``, so existing callers that never mint for a different
    workspace need no change.
    """
    ca_key = asyncssh.generate_private_key("ssh-ed25519")
    ca = SshUserCa(ca_key.export_private_key())
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    authorized = asyncssh.import_authorized_keys(
        f'cert-authority,principals="{principal}" ' + ca.public_key_line + "\n"
    )

    server = await asyncssh.create_server(
        _WorkspaceServer,
        "127.0.0.1",
        0,
        server_host_keys=[host_key],
        authorized_client_keys=authorized,
        process_factory=_workspace_session,
        sftp_factory=lambda chan: asyncssh.SFTPServer(chan, chroot=str(tmp_path)),
        encoding=None,
        line_editor=False,
    )
    try:
        yield _Workspace(
            port=server.sockets[0].getsockname()[1],
            fingerprint=host_key.convert_to_public().get_fingerprint("sha256"),
            ca=ca,
            root=str(tmp_path),
        )
    finally:
        await _shut_down(server)


async def _shut_down(server) -> None:
    """Close a listener without letting a leaked connection wedge the suite.

    ``asyncio.Server.wait_closed`` waits for every active connection on
    Python 3.12+, so a test that fails while still holding one would
    otherwise hang here forever instead of reporting its assertion.
    """
    server.close()
    with contextlib.suppress(TimeoutError):
        async with asyncio.timeout(10):
            await server.wait_closed()


@contextlib.asynccontextmanager
async def _connected(connection):
    try:
        yield connection
    finally:
        connection.close()
        await connection.wait_closed()


def _recording_pin(monkeypatch) -> list:
    """Patch in a ``_PinnedClient`` that records whether asyncssh ever asked
    it to validate a host key. ``known_hosts=None`` never asks -- that is
    the whole trap."""
    fired = []

    class _Recording(mod._PinnedClient):
        def validate_host_public_key(self, host, addr, port, key):
            result = super().validate_host_public_key(host, addr, port, key)
            fired.append(result)
            return result

    monkeypatch.setattr(mod, "_PinnedClient", _Recording)
    return fired


def test_pinned_client_accepts_only_the_attested_fingerprint():
    """The actual security function, which no recorder test reaches."""
    key = asyncssh.generate_private_key("ssh-ed25519").convert_to_public()
    fingerprint = key.get_fingerprint("sha256")

    assert (
        mod._PinnedClient(fingerprint).validate_host_public_key(
            "workspace", "127.0.0.1", 22, key
        )
        is True
    )
    assert (
        mod._PinnedClient(WRONG_FINGERPRINT).validate_host_public_key(
            "workspace", "127.0.0.1", 22, key
        )
        is False
    )


@pytest.mark.parametrize("malformed", [None, b"SHA256:" + b"A" * 43, "SHA256:é"])
def test_a_malformed_pin_is_refused_and_named_as_our_own_data(malformed, caplog):
    """``compare_digest`` raises TypeError for None, bytes, or non-ASCII.

    It already failed CLOSED without the guard -- the exception escaped the
    callback and asyncssh refused the connection -- but it surfaced through
    ``_upstream``'s generic handler at ``warning``, as "could not open the
    inner hop", i.e. as though the WORKSPACE were the problem. It is our own
    attested data that is malformed, which is a different thing to go
    looking at, so it is named and logged at ``error``.

    ``resolve_target``'s ``_is_valid_identifier`` makes this unreachable
    today; this is defence in depth for a future path that skips it.
    """
    key = asyncssh.generate_private_key("ssh-ed25519").convert_to_public()

    with caplog.at_level(
        logging.ERROR, logger="orchestrator.services.ssh_gateway_server"
    ):
        allowed = mod._PinnedClient(malformed).validate_host_public_key(
            "workspace", "127.0.0.1", 22, key
        )

    assert allowed is False
    assert "fingerprint is not a usable string" in caplog.text
    assert [r.levelname for r in caplog.records] == ["ERROR"]


@pytest.mark.asyncio
async def test_the_real_kwarg_set_connects_and_authenticates(tmp_path, monkeypatch):
    """Unmocked end to end: a bogus kwarg, a rejected certificate, or a
    principal the workspace will not accept all fail here and nowhere else.
    ``test_server_options_are_accepted_by_asyncssh`` set this precedent for
    the listener side."""
    fired = _recording_pin(monkeypatch)
    async with _workspace(tmp_path) as workspace:
        target = _target(
            pod_ip="127.0.0.1",
            pod_port=workspace.port,
            host_key_fingerprint=workspace.fingerprint,
        )
        async with _connected(
            await connect_upstream(_context(ca=workspace.ca), target)
        ) as upstream:
            result = await upstream.run("id -un")
            assert result.stdout == b"workspace:id -un\n"

    assert fired == [True], "the pin callback must have been consulted"


@pytest.mark.asyncio
async def test_a_wrong_pin_is_refused_by_a_real_server(tmp_path, monkeypatch):
    fired = _recording_pin(monkeypatch)
    async with _workspace(tmp_path) as workspace:
        target = _target(
            pod_ip="127.0.0.1",
            pod_port=workspace.port,
            host_key_fingerprint=WRONG_FINGERPRINT,
        )
        with pytest.raises(asyncssh.HostKeyNotVerifiable):
            await connect_upstream(_context(ca=workspace.ca), target)

    assert fired == [False]


@pytest.mark.asyncio
async def test_a_certificate_is_refused_by_a_workspace_it_was_not_minted_for(
    tmp_path,
):
    """The WORKSPACE side of Ruling G3/G44's fix, against a real asyncssh
    server rather than a recorder. ``test_uses_a_minted_certificate_not_a_
    static_key`` proves the GATEWAY mints per-workspace (``target.thread_
    id``, not the fixed constant); this proves the other half actually
    matters -- that a workspace scoped via ``AuthorizedPrincipalsFile`` to
    one principal really does reject a certificate carrying a different
    one, via asyncssh's ``principals="..."`` authorized-keys option (its
    analogue of that OpenSSH directive).

    This test alone would still pass if the gateway regressed to minting
    every certificate with the fixed ``WORKSPACE_PRINCIPAL`` constant --
    ``"agent-host"`` mismatches ``"some-other-thread"`` exactly as ``"t1"``
    does. That regression is what ``test_uses_a_minted_certificate_not_a_
    static_key`` (the mint-argument assertion) and the successful full-stack
    dials below (workspace scoped to ``"t1"``, matching ``_target()``'s
    default) exist to catch instead.
    """
    async with _workspace(tmp_path, principal="some-other-thread") as workspace:
        target = _target(
            pod_ip="127.0.0.1",
            pod_port=workspace.port,
            host_key_fingerprint=workspace.fingerprint,
            thread_id="t1",
        )
        with pytest.raises(asyncssh.PermissionDenied):
            await connect_upstream(_context(ca=workspace.ca), target)


@pytest.mark.asyncio
async def test_known_hosts_none_is_the_negative_control(tmp_path, monkeypatch):
    """THE reason EMPTY_KNOWN_HOSTS exists, and the control that makes the
    test above discriminating rather than a fact about asyncssh in general.

    Only ``EMPTY_KNOWN_HOSTS`` changes here. With ``None`` the identical
    wrong pin CONNECTS and the callback is never consulted at all -- so a
    future edit that "simplifies" the constant away silently disables host
    verification on the inner hop rather than failing any test.
    """
    monkeypatch.setattr(mod, "EMPTY_KNOWN_HOSTS", None)
    fired = _recording_pin(monkeypatch)

    async with _workspace(tmp_path) as workspace:
        target = _target(
            pod_ip="127.0.0.1",
            pod_port=workspace.port,
            host_key_fingerprint=WRONG_FINGERPRINT,
        )
        async with _connected(
            await connect_upstream(_context(ca=workspace.ca), target)
        ) as upstream:
            assert upstream is not None

    assert fired == [], "known_hosts=None never consults the pin at all"


# =====================================================================
# _upstream: one dial per connection, and no dial that outlives its peer
# =====================================================================


def _server_with_target(**context_overrides) -> GatewaySSHServer:
    async def _resolve(handle, fingerprint):
        return _target()

    base = dict(limiter=_limiter(), resolve=_resolve)
    base.update(context_overrides)
    return _authenticated(GatewaySSHServer(_context(**base), CLIENT_IP))


class FakeUpstream:
    def __init__(self, hangs_on_close=False):
        self.closed = 0
        self.awaited = 0
        self._hangs_on_close = hangs_on_close

    def close(self):
        self.closed += 1

    async def wait_closed(self):
        self.awaited += 1
        if self._hangs_on_close:
            await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_the_upstream_is_dialled_once_per_connection(monkeypatch):
    """A JetBrains client opens several channels at once. Each dial mints a
    certificate and costs a full SSH handshake against the workspace."""
    dials = []

    async def _connect(context, target):
        await asyncio.sleep(0)
        dials.append(target.thread_id)
        return FakeUpstream()

    monkeypatch.setattr(mod, "connect_upstream", _connect)
    server = _server_with_target()

    first, second = await asyncio.gather(server._upstream(), server._upstream())

    assert dials == ["t1"]
    assert first is second


@pytest.mark.asyncio
async def test_a_failed_dial_becomes_a_readable_refusal(monkeypatch):
    """An exception escaping ``_session_factory`` reaches asyncssh's task
    reaper, which tears the whole connection down with an opaque internal
    error instead of telling the user anything."""

    async def _connect(context, target):
        raise asyncssh.HostKeyNotVerifiable("host key mismatch")

    monkeypatch.setattr(mod, "connect_upstream", _connect)
    server = _server_with_target()
    process = FakeProcess()

    await server._session_factory(process)

    message, code = REFUSAL_MESSAGES["unreachable"]
    assert process.stderr_text == f"srw: {message}\n"
    assert process.exit_code == code


@pytest.mark.asyncio
async def test_connection_lost_closes_the_upstream(monkeypatch):
    """Without this every gateway connection leaves a live SSH connection to
    the workspace, holding one of its sshd's MaxSessions slots until the
    gateway pod restarts."""
    upstream = FakeUpstream()

    async def _connect(context, target):
        return upstream

    monkeypatch.setattr(mod, "connect_upstream", _connect)
    server = _server_with_target()
    await server._upstream()

    server.connection_lost(None)
    await mod.drain_background_tasks(timeout=5.0)

    assert upstream.closed == 1


@pytest.mark.asyncio
async def test_the_upstream_teardown_is_awaited_and_covered_by_the_drain(monkeypatch):
    """``close()`` only REQUESTS the close; the disconnect and socket
    teardown finish on the loop afterwards. Scheduling the bounded
    ``wait_closed`` through ``_schedule`` is what puts it in
    ``_BACKGROUND_TASKS``, so Task 8's shutdown drain covers it instead of
    stopping the loop mid-teardown."""
    upstream = FakeUpstream()

    async def _connect(context, target):
        return upstream

    monkeypatch.setattr(mod, "connect_upstream", _connect)
    server = _server_with_target()
    await server._upstream()

    server.connection_lost(None)
    assert upstream.awaited == 0, "a synchronous callback cannot await"

    assert await mod.drain_background_tasks(timeout=5.0) == 0
    assert upstream.awaited == 1


@pytest.mark.asyncio
async def test_a_wedged_upstream_teardown_is_abandoned_not_waited_on(monkeypatch):
    """A shutdown that hangs on an inner hop that will not close is worse
    than an abandoned teardown -- the workspace sshd times the half-closed
    connection out on its own."""
    upstream = FakeUpstream(hangs_on_close=True)

    async def _connect(context, target):
        return upstream

    monkeypatch.setattr(mod, "connect_upstream", _connect)
    monkeypatch.setattr(mod, "UPSTREAM_CLOSE_TIMEOUT_SECONDS", 0.01)
    server = _server_with_target()
    await server._upstream()

    server.connection_lost(None)

    # Settles on its own budget rather than the drain's, and never raises
    # out of the background task.
    assert await mod.drain_background_tasks(timeout=5.0) == 0
    assert upstream.closed == 1


@pytest.mark.asyncio
async def test_a_peer_lost_during_the_dial_does_not_leak_the_upstream(monkeypatch):
    """Same shape as the attach-slot leak fixed in 39702139:
    ``connection_lost`` runs while the dial is still in flight, so it finds
    ``_upstream_conn`` still None and closes nothing."""
    release = asyncio.Event()
    upstream = FakeUpstream()

    async def _connect(context, target):
        await release.wait()
        return upstream

    monkeypatch.setattr(mod, "connect_upstream", _connect)
    server = _server_with_target()

    dialing = asyncio.ensure_future(server._upstream())
    for _ in range(4):
        await asyncio.sleep(0)

    server.connection_lost(None)
    assert upstream.closed == 0

    release.set()
    with pytest.raises(TargetUnavailable):
        await dialing
    await mod.drain_background_tasks(timeout=5.0)

    # Closed through the same one method as every other teardown path, so it
    # gets the same bounded wait and the same drain coverage.
    assert upstream.closed == 1
    assert upstream.awaited == 1


@pytest.mark.asyncio
async def test_a_context_without_a_ca_refuses_readably(monkeypatch):
    """``ca`` became required the moment this task started minting with it.
    Before, ``missing_required`` did not name it and a half-wired listener
    raised AttributeError into asyncssh instead of refusing."""
    server = _server_with_target(ca=None)
    process = FakeProcess()

    await server._session_factory(process)

    assert process.stderr_text == f"srw: {mod.MISCONFIGURED_MESSAGE}\n"
    assert process.exit_code == mod.MISCONFIGURED_EXIT_CODE


# =====================================================================
# The deferred subsystem reply, at the seam
#
# The full-stack tests at the bottom prove this works against real
# asyncssh; these prove the individual moving parts, including two the
# full-stack tests cannot reach at all (a peer that leaves mid-dial, and an
# asyncssh with no deferral mechanism left to use).
# =====================================================================


class _FakeChannel:
    def __init__(self, subsystem="sftp", report_raises=None):
        self.reported = []
        self.closed = 0
        self.logger = logging.getLogger("test.channel")
        self._subsystem = subsystem
        self._report_raises = report_raises

    def get_subsystem(self):
        return self._subsystem

    def close(self):
        self.closed += 1

    def _report_response(self, result):
        if self._report_raises is not None:
            raise self._report_raises
        self.reported.append(result)


class _NoDeferChannel(_FakeChannel):
    """An asyncssh that renamed the deferral mechanism out from under us."""

    _report_response = None


class _FakeConn:
    def __init__(self):
        self.tasks = []

    def create_task(self, coro, logger=None):
        task = asyncio.ensure_future(coro)
        self.tasks.append(task)
        return task

    async def drain(self):
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)


def _subsystem_process(server: GatewaySSHServer, channel=None):
    """A real ``_GatewayProxyProcess`` with the real wiring, on fake plumbing."""
    process = server.session_requested()
    process._chan = channel if channel is not None else _FakeChannel()
    process._conn = _FakeConn()
    return process


@pytest.mark.asyncio
async def test_a_subsystem_reply_is_deferred_until_the_upstream_answers(monkeypatch):
    """``None`` means "no answer yet" to asyncssh. Returning True here
    instead is the pre-Task-7 behaviour, and it is what makes a refusal
    arrive as ``SFTPConnectionLost``."""
    release = asyncio.Event()

    async def _connect(context, target):
        await release.wait()
        return FakeUpstream()

    monkeypatch.setattr(mod, "connect_upstream", _connect)
    server = _server_with_target()
    process = _subsystem_process(server)

    assert process.subsystem_requested("sftp") is None
    await asyncio.sleep(0)
    assert process._chan.reported == [], "answered before the upstream did"

    release.set()
    await process._conn.drain()

    assert process._chan.reported == [True]


@pytest.mark.asyncio
async def test_a_refused_subsystem_is_answered_with_a_channel_failure():
    async def _resolve(handle, fingerprint):
        raise TargetUnavailable("suspended")

    server = _authenticated(
        GatewaySSHServer(_context(limiter=_limiter(), resolve=_resolve), CLIENT_IP)
    )
    process = _subsystem_process(server)

    assert process.subsystem_requested("sftp") is None
    await process._conn.drain()

    assert process._chan.reported == [False]


@pytest.mark.asyncio
async def test_a_refused_subsystem_is_still_recorded_in_the_audit_row():
    """'They asked for sftp and were turned away' is what the ``channels``
    column exists for. On the deferred path ``_session_factory`` -- which
    does this recording for the shell path -- is never reached at all, so
    without a record here a refused sftp attempt leaves no trace."""

    async def _resolve(handle, fingerprint):
        raise TargetUnavailable("suspended")

    server = _authenticated(
        GatewaySSHServer(_context(limiter=_limiter(), resolve=_resolve), CLIENT_IP)
    )
    process = _subsystem_process(server)

    process.subsystem_requested("sftp")
    await process._conn.drain()

    assert server.channel_types == [mod.SFTP_CHANNEL_TYPE]


@pytest.mark.asyncio
async def test_an_unsupported_subsystem_is_refused_without_deferring():
    """Only sftp is proxied. Anything else is a plain False, and must not
    reach the workspace at all -- a deferral would dial for it first."""
    dials = []

    async def _resolve(handle, fingerprint):
        dials.append(handle)
        return _target()

    server = _authenticated(
        GatewaySSHServer(_context(limiter=_limiter(), resolve=_resolve), CLIENT_IP)
    )
    process = _subsystem_process(server, _FakeChannel(subsystem="netconf"))

    assert process.subsystem_requested("netconf") is False
    assert process._conn.tasks == []
    assert dials == []
    assert server.channel_types == []


@pytest.mark.asyncio
async def test_a_deferred_reply_is_dropped_when_the_peer_has_already_left(monkeypatch):
    """``_report_response(True)`` asks asyncssh to start a session on the
    channel. On a channel it has already torn down that trips an assertion
    inside asyncssh, from a task, which takes the whole connection with it."""
    release = asyncio.Event()

    async def _connect(context, target):
        await release.wait()
        return FakeUpstream()

    monkeypatch.setattr(mod, "connect_upstream", _connect)
    server = _server_with_target()
    process = _subsystem_process(server)

    process.subsystem_requested("sftp")
    await asyncio.sleep(0)
    process.connection_lost(None)

    release.set()
    await process._conn.drain()

    assert process._chan.reported == []


@pytest.mark.asyncio
async def test_a_failing_deferred_reply_refuses_only_its_own_channel(monkeypatch):
    """``_report_response`` is asyncssh-private and called FROM A TASK, so an
    exception out of it reaches ``_reap_task`` -> ``internal_error()``, which
    tears down the whole inbound connection. The degradation branch above
    covers the method being renamed away; this covers "same name, different
    contract", which is the likelier way private API breaks on an upgrade.

    Closing the channel is the degradation: asyncssh's ``_cleanup`` resolves
    the peer's pending request waiters with ``False``, so the client gets the
    same clean failure an honest CHANNEL_FAILURE would have produced."""

    async def _connect(context, target):
        return FakeUpstream()

    monkeypatch.setattr(mod, "connect_upstream", _connect)
    server = _server_with_target()
    channel = _FakeChannel(report_raises=RuntimeError("asyncssh changed"))
    process = _subsystem_process(server, channel)

    process.subsystem_requested("sftp")
    await process._conn.drain()

    # Not raised out of the task, and the one channel was refused.
    assert [task.exception() for task in process._conn.tasks] == [None]
    assert channel.closed == 1


@pytest.mark.asyncio
async def test_a_missing_defer_mechanism_degrades_instead_of_hanging():
    """``_report_response`` is asyncssh-private. If a future release renames
    it, answering optimistically is a bad UX; deferring an answer nobody can
    ever deliver hangs every sftp client instead. Loud, and degraded."""
    server = _server_with_target()
    process = _subsystem_process(server, _NoDeferChannel())

    assert process.subsystem_requested("sftp") is True
    assert process._conn.tasks == []


# =====================================================================
# direct-tcpip
# =====================================================================


@pytest.mark.asyncio
async def test_forward_connection_is_awaited_not_called(monkeypatch):
    """asyncssh's ``_process_direct_tcpip_open`` does ``if callable(result):
    session = SSHTCPStreamSession(result)`` BEFORE it awaits anything, so a
    returned *function* is treated as a ``(reader, writer)`` stream handler
    and the dial never happens. It must return an awaitable instead."""
    forwarded = []

    class _FakeInbound:
        async def forward_tunneled_connection(self, conn, dest_host, dest_port):
            forwarded.append((conn, dest_host, dest_port))
            return "forwarder"

    upstream = FakeUpstream()

    async def _connect(context, target):
        return upstream

    monkeypatch.setattr(mod, "connect_upstream", _connect)
    server = _server_with_target()
    server.connection_made(_FakeInbound())

    result = server._forward_connection("127.0.0.1", 8080)
    assert not callable(result)
    assert inspect.isawaitable(result)

    assert await result == "forwarder"
    assert forwarded == [(upstream, "127.0.0.1", 8080)]


@pytest.mark.asyncio
async def test_a_refused_target_makes_direct_tcpip_a_clean_channel_failure():
    """``_finish_open_request`` catches ChannelOpenError and nothing else: a
    TargetUnavailable escaping here would reach asyncssh's task reaper and
    kill the whole connection rather than the one channel."""

    async def _resolve(handle, fingerprint):
        raise TargetUnavailable("suspended")

    server = _authenticated(
        GatewaySSHServer(_context(limiter=_limiter(), resolve=_resolve), CLIENT_IP)
    )

    with pytest.raises(asyncssh.ChannelOpenError) as excinfo:
        await server._forward_connection("127.0.0.1", 8080)

    assert REFUSAL_MESSAGES["suspended"][0] in excinfo.value.reason


# =====================================================================
# Full stack: client -> gateway -> workspace, both hops real.
# =====================================================================


@contextlib.asynccontextmanager
async def _gateway(context):
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    server = await asyncssh.create_server(
        lambda: GatewaySSHServer(context, CLIENT_IP),
        "127.0.0.1",
        0,
        server_host_keys=[host_key],
        encoding=None,
        line_editor=False,
    )
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        await _shut_down(server)


@contextlib.asynccontextmanager
async def _client(port):
    # known_hosts=None on THIS hop only: the client->gateway host key is not
    # what this file tests, and the gateway's own listener is Task 8's.
    connection = await asyncssh.connect(
        "127.0.0.1",
        port=port,
        username=HANDLE,
        client_keys=[asyncssh.generate_private_key("ssh-ed25519")],
        known_hosts=None,
        encoding=None,
    )
    async with _connected(connection):
        yield connection


def _live_context(workspace: _Workspace, **overrides) -> GatewayContext:
    async def _resolve(handle, fingerprint):
        return _target(
            pod_ip="127.0.0.1",
            pod_port=workspace.port,
            host_key_fingerprint=workspace.fingerprint,
        )

    base = dict(ca=workspace.ca, limiter=_limiter(), resolve=_resolve)
    base.update(overrides)
    return _context(**base)


@pytest.mark.asyncio
async def test_exec_is_proxied_end_to_end(tmp_path):
    async with _workspace(tmp_path) as workspace:
        async with _gateway(_live_context(workspace)) as port:
            async with _client(port) as connection:
                async with asyncio.timeout(20):
                    result = await connection.run("hostname")

    assert result.stdout == b"workspace:hostname\n"
    assert result.exit_status == 0


@pytest.mark.asyncio
async def test_sftp_is_proxied_end_to_end(tmp_path):
    """asyncssh's own ``forward_tunneled_session`` refuses the sftp subsystem
    outright, which is why ``ssh_gateway_proxy`` exists at all."""
    (tmp_path / "hello.txt").write_text("workspace file")

    async with _workspace(tmp_path) as workspace:
        async with _gateway(_live_context(workspace)) as port:
            async with _client(port) as connection:
                async with asyncio.timeout(20):
                    async with connection.start_sftp_client() as sftp:
                        names = await sftp.listdir(".")

    assert "hello.txt" in names


@pytest.mark.asyncio
async def test_a_refused_workspace_fails_sftp_with_a_channel_open_failure(tmp_path):
    """TASK 12'S LIVE GATE, in-process.

    The optimistic-SUCCESS strategy the shell path uses is wrong for sftp:
    the client would get MSG_CHANNEL_SUCCESS, start its handshake, and see
    the channel die -- which asyncssh's own client surfaces as
    ``SFTPConnectionLost`` and OpenSSH's ``sftp`` as 'Connection closed'.
    Deferring the reply until the upstream is known good turns that into one
    clean ``ChannelOpenError``.
    """

    async def _resolve(handle, fingerprint):
        raise TargetUnavailable("suspended")

    async with _workspace(tmp_path) as workspace:
        context = _live_context(workspace, resolve=_resolve)
        async with _gateway(context) as port:
            async with _client(port) as connection:
                async with asyncio.timeout(20):
                    with pytest.raises(asyncssh.ChannelOpenError):
                        await connection.start_sftp_client()

                    # The connection itself must survive: only the channel
                    # was refused.
                    result = await connection.run("still alive")

    assert result.exit_status == REFUSAL_MESSAGES["suspended"][1]


@pytest.mark.asyncio
async def test_a_refused_workspace_still_explains_itself_on_a_shell(tmp_path):
    """The other half of the split: shell/exec DOES get the optimistic
    SUCCESS, because stderr is the only place a human reads a reason."""

    async def _resolve(handle, fingerprint):
        raise TargetUnavailable("suspended")

    async with _workspace(tmp_path) as workspace:
        context = _live_context(workspace, resolve=_resolve)
        async with _gateway(context) as port:
            async with _client(port) as connection:
                async with asyncio.timeout(20):
                    result = await connection.run("whoami")

    message, code = REFUSAL_MESSAGES["suspended"]
    assert result.stderr == f"srw: {message}\n".encode()
    assert result.exit_status == code


@pytest.mark.asyncio
async def test_a_broken_report_response_costs_one_channel_not_the_connection(
    tmp_path, monkeypatch
):
    """Against REAL asyncssh, with the private method made to raise.

    This is the negative control for the guard around ``report(allowed)``.
    Unguarded, the exception reaches asyncssh's ``_reap_task`` ->
    ``internal_error()``, which disconnects the whole inbound connection --
    so the ``connection.run`` at the end fails too, and every other channel a
    JetBrains client had open dies with it. Guarded, sftp fails alone and the
    connection is still usable.
    """
    original = asyncssh.channel.SSHChannel._report_response

    def _broken(self, result):
        if self._request_queue and self._request_queue[0][0] == "subsystem":
            raise RuntimeError("a future asyncssh changed this contract")
        return original(self, result)

    monkeypatch.setattr(asyncssh.channel.SSHChannel, "_report_response", _broken)

    async with _workspace(tmp_path) as workspace:
        async with _gateway(_live_context(workspace)) as port:
            async with _client(port) as connection:
                async with asyncio.timeout(20):
                    with pytest.raises(asyncssh.ChannelOpenError):
                        await connection.start_sftp_client()

                    result = await connection.run("still alive")

    assert result.stdout == b"workspace:still alive\n"


@pytest.mark.asyncio
async def test_direct_tcpip_reaches_the_workspace_loopback(tmp_path):
    """JetBrains Gateway and ``ProxyJump`` both ride this channel."""

    async def _echo(reader, writer):
        writer.write(await reader.read(64))
        await writer.drain()
        writer.close()

    echo = await asyncio.start_server(_echo, "127.0.0.1", 0)
    echo_port = echo.sockets[0].getsockname()[1]

    try:
        async with _workspace(tmp_path) as workspace:
            async with _gateway(_live_context(workspace)) as port:
                async with _client(port) as connection:
                    async with asyncio.timeout(20):
                        reader, writer = await connection.open_connection(
                            "127.0.0.1", echo_port
                        )
                        writer.write(b"through the tunnel")
                        received = await reader.read(64)
                        writer.close()
    finally:
        echo.close()
        await echo.wait_closed()

    assert received == b"through the tunnel"


@pytest.mark.asyncio
async def test_a_non_loopback_direct_tcpip_never_reaches_the_workspace(tmp_path):
    """``clamp_direct_tcpip`` guards the forwarder; with the forwarder now
    real, 'returns False' has to still mean 'refused' rather than 'dialled
    and failed'."""
    async with _workspace(tmp_path) as workspace:
        async with _gateway(_live_context(workspace)) as port:
            async with _client(port) as connection:
                async with asyncio.timeout(20):
                    with pytest.raises(asyncssh.ChannelOpenError):
                        await connection.open_connection("169.254.169.254", 80)
