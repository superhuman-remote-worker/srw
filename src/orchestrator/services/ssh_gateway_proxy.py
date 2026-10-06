"""Channel proxying for the SSH gateway.

Why this exists rather than asyncssh's own proxy mode, both reproduced on 2.24.0:

1. ``SSHClientConnection.forward_tunneled_session`` constructs
   ``SSHServerProcess(process_factory, None, MIN_SFTP_VERSION, False)``. That
   ``None`` is ``sftp_factory``, and ``SSHServerStreamSession.subsystem_requested``
   returns ``bool(self._sftp_factory)`` -- so the sftp subsystem is refused, and
   JetBrains Gateway cannot work through it at all.
2. It never calls ``process.exit()``. The downstream channel is never closed and
   ``ssh gateway some-command`` hangs forever with ``exit_status=None``.

``session_started`` must also be overridden: the stock implementation
special-cases sftp and runs a *local* sftp server instead of forwarding.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable

import asyncssh
from asyncssh.constants import EXTENDED_DATA_STDERR
from asyncssh.stream import SSHReader, SSHWriter

logger = logging.getLogger(__name__)

ALLOWED_SUBSYSTEMS = frozenset({"sftp"})

# Ruling G16: this isn't a resolved-target refusal (ssh_gateway_client's
# REFUSAL_MESSAGES table owns those, keyed by workspace state) -- it's a
# generic proxy-layer failure, so there is no per-state message to look up.
# 75 EX_TEMPFAIL, not 69 EX_UNAVAILABLE (fix round 1, Minor #2): a
# create_process failure on an already-established upstream connection --
# the workspace sshd's MaxSessions exhausted, a transient hiccup -- is
# generally retryable. Mirrors e16cdc13's own reasoning for reclassifying
# stale_binding the same way in ssh_gateway_client.REFUSAL_MESSAGES.
UPSTREAM_FAILURE_EXIT_CODE = 75


async def _close_started_process(upstream_process) -> None:
    """Bound cleanup of the exact process; never start or replay a command."""

    try:
        upstream_process.close()
        await asyncio.wait_for(upstream_process.wait_closed(), timeout=3.0)
    except Exception:
        logger.warning("ssh gateway: started upstream process did not close promptly")


class ProxyProcess(asyncssh.SSHServerProcess):
    """A server process whose streams are spliced to an upstream connection."""

    def subsystem_requested(self, subsystem: str) -> bool:
        return subsystem in ALLOWED_SUBSYSTEMS

    def session_started(self) -> None:
        # Binary, not UTF-8: session data is arbitrary bytes. This is *not*
        # what keeps asyncssh's line editor out of the way (fix round 1,
        # Minor #1 -- the previous comment claiming that was false): the
        # listener already passes line_editor=False
        # (ssh_gateway_config.py's server_options), so asyncssh's
        # SSHServerChannel._wrap_session (asyncssh/channel.py) never installs
        # SSHLineEditorSession around this session at all. Even if it were
        # installed, its session_started calls create_editor() *before*
        # delegating to this one (asyncssh/editor.py), so a set_encoding()
        # here would be too late to matter either way. The listener's own
        # encoding=None also already makes the channel binary before this
        # method ever runs -- restating it below matches asyncssh's own
        # _init_sftp_server idiom (asyncssh/stream.py) as defense in depth,
        # not the mechanism that makes this safe.
        self._chan.set_encoding(None)
        self._encoding = None
        handler = self._start_process(
            SSHReader(self, self._chan),
            SSHWriter(self, self._chan),
            SSHWriter(self, self._chan, EXTENDED_DATA_STDERR),
        )
        if inspect.isawaitable(handler):
            self._conn.create_task(handler, self._chan.logger)


async def proxy_session(
    process,
    upstream,
    *,
    on_first_use: Callable[[], Awaitable[bool]] | None = None,
) -> None:
    """Open the matching upstream process and mirror it back down.

    Exit status and exit signal are mirrored explicitly because asyncssh omits
    both when forwarding; without this the client never sees the channel close.
    """
    try:
        upstream_process = await upstream.create_process(
            command=process.command,
            subsystem=process.subsystem,
            env=process.env,
            term_type=process.term_type,
            term_size=process.term_size,
            term_modes=process.term_modes,
            # `errors` is meaningless once encoding is forced to None, and is
            # deliberately not passed (Ruling G17). That's only correct
            # *because* ProxyProcess.session_started above forces the
            # downstream channel binary first -- if a future edit ever stops
            # doing that, this hardcoded None must be revisited alongside it.
            encoding=None,
            stdin=process.stdin,
            stdout=process.stdout,
            stderr=process.stderr,
        )
    except Exception:
        # Ruling G16: asyncssh's own forwarder lets this escape uncaught, so
        # the downstream channel never closes and `ssh gw cmd` hangs forever
        # -- the exact bug item 2 above describes, reintroduced through the
        # error path instead of the happy path. The channel is binary
        # (encoding=None, forced above), so stderr takes bytes, not str.
        #
        # Fix round 1, Important #1: log before doing anything else. Before
        # this override existed, an escaping exception here still reached
        # asyncssh's own internal_error() logging path (connection.py's
        # _reap_task); a bare `except Exception:` with no bind and no log
        # would have thrown that trace away and left only a generic stderr
        # line and an exit code -- an operability regression, not a wash.
        # Not deferred to Task 8's module logger: wiring this now is the
        # pattern this plan already had to un-defer elsewhere.
        logger.exception("ssh gateway: failed to start upstream session")
        try:
            process.stderr.write(b"srw: failed to start the session on the workspace\n")
        except Exception:
            # The downstream client can disconnect in this same window --
            # SSHWriter.write raises BrokenPipeError once the channel has
            # left the 'open' state. Nobody is left to read this message
            # either way; what matters is that exit() below still runs.
            pass
        process.exit(UPSTREAM_FAILURE_EXIT_CODE)
        return

    if on_first_use is not None:
        try:
            acknowledged = await on_first_use()
        except asyncio.CancelledError:
            await _close_started_process(upstream_process)
            raise
        except Exception:
            logger.warning(
                "ssh gateway: native first-use acknowledgement failed", exc_info=True
            )
            acknowledged = False
        if not acknowledged:
            # create_process may already have started a command. Close this
            # exact process and report uncertainty; never replay its command.
            await _close_started_process(upstream_process)
            try:
                process.stderr.write(
                    b"srw: native session confirmation unavailable; command may already have started. Check its outcome before retrying\n"
                )
            except Exception:
                pass
            process.exit(UPSTREAM_FAILURE_EXIT_CODE)
            return

    try:
        await upstream_process.wait_closed()
    except asyncio.CancelledError:
        await _close_started_process(upstream_process)
        raise

    if upstream_process.exit_signal:
        process.exit_with_signal(*upstream_process.exit_signal)
    else:
        status = upstream_process.exit_status
        process.exit(status if status is not None else 0)
