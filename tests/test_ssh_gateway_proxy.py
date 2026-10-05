"""Tests for the SSH gateway's channel proxy.

asyncssh's own ``forward_tunneled_session`` can't be reused for two reasons,
both reproduced against the installed 2.24.0:

1. It constructs ``SSHServerProcess(process_factory, None, MIN_SFTP_VERSION,
   False)`` -- that ``None`` is ``sftp_factory``, and
   ``SSHServerStreamSession.subsystem_requested`` returns
   ``bool(self._sftp_factory)``, so sftp (and JetBrains Gateway, which needs
   it) is refused outright.
2. It never calls ``process.exit()``, so a proxied ``ssh gw cmd`` hangs
   forever with ``exit_status=None``.

``ProxyProcess`` fixes both. Most of the tests below exercise ``proxy_session``
(the process-factory body) against fakes; a smaller set (Ruling G14) pins the
shape of the private asyncssh surface ``ProxyProcess.session_started`` reaches
into, since nothing else here calls that override at all.
"""

import asyncio
import inspect
import logging

import asyncssh
import pytest
from asyncssh.constants import EXTENDED_DATA_STDERR
from asyncssh.stream import SSHReader, SSHWriter

from orchestrator.services.ssh_gateway_proxy import (
    ALLOWED_SUBSYSTEMS,
    UPSTREAM_FAILURE_EXIT_CODE,
    ProxyProcess,
    proxy_session,
)


def _inert_factory(process):
    """Stand-in for the ``self._session_factory`` Task 6 passes as
    ``ProxyProcess``'s first positional arg. Never invoked by the tests that
    use it (they only exercise ``subsystem_requested`` / the private
    attributes ``__init__`` sets up) -- its only job is to have the same
    one-argument shape asyncssh's own ``_start_process`` calls it with."""
    return None


class FakeUpstreamProcess:
    def __init__(self, exit_status=0, exit_signal=None):
        self.exit_status = exit_status
        self.exit_signal = exit_signal
        self.closed = False
        self.close_requested = False

    def close(self):
        self.close_requested = True

    async def wait_closed(self):
        self.closed = True


class FakeUpstream:
    def __init__(self, process):
        self.process = process
        self.create_kwargs = None

    async def create_process(self, **kwargs):
        self.create_kwargs = kwargs
        return self.process


class FailingUpstream:
    """Ruling G16: upstream sshd refusing the session, or the workspace
    dying mid-connect -- ``create_process`` raises before an upstream
    process ever exists."""

    def __init__(self, exc):
        self._exc = exc

    async def create_process(self, **kwargs):
        raise self._exc


class RecordingWriter:
    """Stand-in for the SSHWriter bound to a process's stderr. The real one
    takes ``bytes`` here specifically because ``ProxyProcess.session_started``
    forces the downstream channel binary (Ruling G17) before this is ever
    written to."""

    def __init__(self):
        self.written = []

    def write(self, data):
        self.written.append(data)


class BrokenWriter:
    """A stderr writer whose channel is already gone.

    Mirrors ``asyncssh.channel.SSHChannel.write`` (verified against 2.24.0),
    which raises ``BrokenPipeError`` once the channel has left the 'open'
    state -- e.g. the downstream client disconnected in the same window the
    upstream failed in."""

    def write(self, data):
        raise BrokenPipeError("Channel not open for sending")


class FakeProcess:
    def __init__(self, command="ls", subsystem=None):
        self.command = command
        self.subsystem = subsystem
        self.env = {"JB_IDE": "2026.2"}
        self.term_type = "xterm-256color"
        self.term_size = (120, 40)
        self.term_modes = {}
        self.stdin, self.stdout = object(), object()
        self.stderr = RecordingWriter()
        self.exited_with = None
        self.exited_signal = None

    def exit(self, status):
        self.exited_with = status

    def exit_with_signal(self, *args):
        self.exited_signal = args


@pytest.mark.asyncio
async def test_native_notice_occurs_only_after_upstream_process_exists():
    process = FakeProcess()
    upstream_process = FakeUpstreamProcess()
    upstream = FakeUpstream(upstream_process)
    events = []

    async def notify():
        assert upstream.create_kwargs["command"] == "ls"
        events.append("notified")
        return True

    await proxy_session(process, upstream, on_first_use=notify)
    assert events == ["notified"]
    assert process.exited_with == 0


@pytest.mark.asyncio
async def test_unacknowledged_native_notice_closes_started_process_without_replay():
    process = FakeProcess(command="touch /tmp/once")
    upstream_process = FakeUpstreamProcess()
    upstream = FakeUpstream(upstream_process)

    async def refuse():
        return False

    await proxy_session(process, upstream, on_first_use=refuse)
    assert upstream_process.close_requested
    assert upstream_process.closed
    assert process.exited_with == 75
    assert upstream.create_kwargs["command"] == "touch /tmp/once"


@pytest.mark.asyncio
async def test_peer_cancellation_after_notice_closes_exact_started_process():
    class WaitingProcess(FakeUpstreamProcess):
        def __init__(self):
            super().__init__()
            self.waiting = asyncio.Event()

        async def wait_closed(self):
            self.waiting.set()
            if not self.close_requested:
                await asyncio.Event().wait()
            self.closed = True

    inner = WaitingProcess()
    upstream = FakeUpstream(inner)

    async def notify():
        return True

    task = asyncio.create_task(proxy_session(FakeProcess(), upstream, on_first_use=notify))
    await inner.waiting.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert inner.close_requested


def test_only_sftp_is_allowed():
    assert ALLOWED_SUBSYSTEMS == frozenset({"sftp"})


# ---------------------------------------------------------------------------
# Ruling G15: the constant check above is a change-detector -- it would pass
# just as happily if ALLOWED_SUBSYSTEMS were renamed and never consulted by
# anything. These exercise the actual policy seam, ProxyProcess itself.
# ---------------------------------------------------------------------------


def test_subsystem_requested_allows_sftp():
    process = ProxyProcess(_inert_factory, None, 3, False)
    assert process.subsystem_requested("sftp") is True


def test_subsystem_requested_denies_other_subsystems():
    process = ProxyProcess(_inert_factory, None, 3, False)
    assert process.subsystem_requested("netconf") is False


# ---------------------------------------------------------------------------
# Ruling G14: session_started -- the most intricate code in this module --
# reaches into four private asyncssh attributes (_chan, _conn, _encoding,
# _start_process) and, through them, three more asyncssh methods
# (set_encoding, logger, create_task). Nothing above calls session_started at
# all. An asyncssh upgrade that renames or changes any of this breaks
# ProxyProcess silently in production; these pin the seam so a dependency
# bump fails here, in CI, instead.
# ---------------------------------------------------------------------------


def test_session_started_instance_attributes_still_exist():
    """The four private attributes session_started reads or writes."""
    process = ProxyProcess(_inert_factory, None, 3, False)
    assert hasattr(process, "_chan")
    assert hasattr(process, "_conn")
    assert hasattr(process, "_encoding")
    assert hasattr(process, "_start_process")
    assert callable(process._start_process)
    # session_started calls this with exactly three positional args
    # (stdin, stdout, stderr) -- an arity change is exactly the kind of
    # silent break this canary exists to catch.
    assert len(inspect.signature(process._start_process).parameters) == 3


def test_session_started_channel_and_connection_surface_still_exist():
    """The public methods session_started reaches through _chan/_conn.

    Checked at the class level: _chan/_conn are None until a real channel
    opens (there is no live instance to introspect without a network
    connection). SSHServerChannel and SSHServerConnection are the documented
    server-side types asyncssh assigns to them -- see process.py's own
    ``_chan: SSHServerChannel[AnyStr]`` class annotation.
    """
    assert hasattr(asyncssh.SSHServerChannel, "set_encoding")
    assert hasattr(asyncssh.SSHServerChannel, "logger")
    assert hasattr(asyncssh.SSHServerConnection, "create_task")


def test_session_started_stream_wrappers_still_construct():
    """The exact SSHReader/SSHWriter construction calls session_started
    makes, against the real asyncssh classes rather than mocks.

    Both constructors just store their arguments (verified against 2.24.0),
    so succeeding here is a meaningful check: it proves the two-arg
    (stdin/stdout) and three-arg (stderr's extra EXTENDED_DATA_STDERR
    datatype) constructor shapes are both still there.
    """
    process = ProxyProcess(_inert_factory, None, 3, False)
    stdin = SSHReader(process, object())
    stdout = SSHWriter(process, object())
    stderr = SSHWriter(process, object(), EXTENDED_DATA_STDERR)
    assert stdin is not None
    assert stdout is not None
    assert stderr is not None


@pytest.mark.asyncio
async def test_exit_status_is_mirrored():
    """asyncssh's built-in forwarder omits this, so `ssh gw cmd` hangs forever."""
    proc = FakeProcess()
    upstream = FakeUpstream(FakeUpstreamProcess(exit_status=42))
    await proxy_session(proc, upstream)
    assert proc.exited_with == 42


@pytest.mark.asyncio
async def test_missing_exit_status_becomes_zero():
    proc = FakeProcess()
    upstream = FakeUpstream(FakeUpstreamProcess(exit_status=None))
    await proxy_session(proc, upstream)
    assert proc.exited_with == 0


@pytest.mark.asyncio
async def test_exit_signal_is_mirrored():
    proc = FakeProcess()
    signal = ("TERM", False, "killed", "en-US")
    upstream = FakeUpstream(FakeUpstreamProcess(exit_status=None, exit_signal=signal))
    await proxy_session(proc, upstream)
    assert proc.exited_signal == signal
    assert proc.exited_with is None


@pytest.mark.asyncio
async def test_terminal_and_env_are_forwarded():
    """JetBrains sends env and a pty; dropping either breaks it."""
    proc = FakeProcess()
    upstream = FakeUpstream(FakeUpstreamProcess())
    await proxy_session(proc, upstream)
    kwargs = upstream.create_kwargs
    assert kwargs["term_type"] == "xterm-256color"
    assert kwargs["term_size"] == (120, 40)
    assert kwargs["env"] == {"JB_IDE": "2026.2"}
    assert kwargs["encoding"] is None


@pytest.mark.asyncio
async def test_subsystem_is_forwarded():
    proc = FakeProcess(command=None, subsystem="sftp")
    upstream = FakeUpstream(FakeUpstreamProcess())
    await proxy_session(proc, upstream)
    assert upstream.create_kwargs["subsystem"] == "sftp"


# ---------------------------------------------------------------------------
# Ruling G16: proxy_session had no failure path. If upstream.create_process
# raises -- upstream sshd refusing the session, or the workspace dying
# mid-connect -- the exception used to escape the process factory
# uncaught, and the downstream channel closed with no exit status: exactly
# failure mode #2 this module exists to fix (see module docstring), brought
# back through the error path instead of the happy path.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upstream_failure_is_reported_and_exits_nonzero():
    proc = FakeProcess()
    upstream = FailingUpstream(ConnectionError("upstream refused the connection"))

    await proxy_session(proc, upstream)  # must not raise

    assert proc.exited_with == UPSTREAM_FAILURE_EXIT_CODE
    assert proc.exited_with != 0
    assert proc.exited_signal is None
    assert proc.stderr.written == [
        b"srw: failed to start the session on the workspace\n"
    ]


@pytest.mark.asyncio
async def test_upstream_failure_is_logged_with_traceback(caplog):
    """Fix round 1, Important #1: the except clause used to discard the
    exception outright -- no bind, no log -- so a failed attach left only a
    generic stderr line and an exit code. Before this override existed at
    all, the escaping exception still reached asyncssh's own
    internal_error() logging path (connection.py's _reap_task); losing that
    on the way to fixing the hang would have been a regression, not a wash.
    Checked against caplog.text (the fully rendered output), not just the
    bare message, because "with traceback" is the actual requirement --
    logger.error(msg) with no exc_info would pass a check that only looked
    at record.getMessage()."""
    proc = FakeProcess()
    upstream = FailingUpstream(ConnectionError("upstream refused the connection"))

    await proxy_session(proc, upstream)

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert errors[0].exc_info is not None
    assert "ConnectionError" in caplog.text
    assert "upstream refused the connection" in caplog.text


@pytest.mark.asyncio
async def test_upstream_success_still_works_after_failure_handling_was_added():
    """Negative control for the try/except above: a well-behaved upstream
    must still reach the ordinary exit-status path, proving the new except
    clause doesn't swallow the success case too."""
    proc = FakeProcess()
    upstream = FakeUpstream(FakeUpstreamProcess(exit_status=7))
    await proxy_session(proc, upstream)
    assert proc.exited_with == 7
    assert proc.stderr.written == []


@pytest.mark.asyncio
async def test_upstream_failure_still_exits_if_stderr_is_already_gone():
    """A fifth defect found while implementing G16, not asked for by any
    ruling: the downstream client can disconnect in the very window the
    upstream fails in. asyncssh's real stderr writer raises BrokenPipeError
    once the channel has left the 'open' state (confirmed against 2.24.0),
    which -- unguarded -- would escape proxy_session exactly like the
    original create_process failure did, undoing the G16 fix via its own
    error-handling code. process.exit() is safe to call regardless (asyncssh
    no-ops it once the channel is closing/closed), so it must still run."""
    proc = FakeProcess()
    proc.stderr = BrokenWriter()
    upstream = FailingUpstream(ConnectionError("upstream refused the connection"))

    await proxy_session(proc, upstream)  # must not raise

    assert proc.exited_with == UPSTREAM_FAILURE_EXIT_CODE
