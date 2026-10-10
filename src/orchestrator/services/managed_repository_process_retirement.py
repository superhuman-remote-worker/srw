"""Exact remote retirement for managed-repository credential agents.

Control-plane deletion is not process-zero: a partitioned Kubernetes node or
VM guest can keep running after the API reports an accepted delete or 404. The
terminal owners call this module against a server-attested endpoint and host
key before they delete compute. No credential material enters the command,
logs, or durable state; the command only classifies and retires the private
managed ssh-agent namespace.

The same pinned transport scrubs SRW's credential material from a workspace
a refused resume keeps (connector drivers decision 34): see
:func:`scrub_workspace_credentials`.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
import logging
import secrets
import shlex

try:
    import asyncssh
except ImportError:  # pragma: no cover - deployment dependency guard
    asyncssh = None  # type: ignore[assignment]

from orchestrator.services import resolve_ssh_key_path
from shared.runtime.core.credential_env import (
    INSTALL_CREDENTIAL_FILES,
    WORKSPACE_PYTHON,
)
from shared.runtime.core.legacy_ssh_keys import (
    LEGACY_KEY_MARKER,
    RETIRE_LEGACY_KEYS_PROGRAM,
    list_legacy_keys_command,
)
from shared.runtime.core.managed_repository import (
    managed_repository_agent_retirement_command,
    managed_repository_agent_zero_command,
)

logger = logging.getLogger(__name__)

_EMPTY_KNOWN_HOSTS = ((), (), (), (), (), (), ())

#: The workspace image's home for its ``agent-host`` user, on Kubernetes and
#: VM workspaces alike. No shared constant names it: the remote backend
#: spells the same default (``RemoteBackend`` ``workspace_path``), as do the
#: other terminal owners.
WORKSPACE_HOME = "/home/agent-host"


if asyncssh is not None:

    class _PinnedWorkspaceSSHClient(asyncssh.SSHClient):
        def __init__(self, expected_fingerprint: str):
            self._expected_fingerprint = expected_fingerprint

        def validate_host_public_key(self, host, addr, port, key):  # noqa: ANN001
            del host, addr, port
            return secrets.compare_digest(
                key.get_fingerprint("sha256"), self._expected_fingerprint
            )


def _whole_workspace_retirement(home_path: str) -> str:
    # The whole-workspace retirement form deliberately ends each generated
    # cleanup loop with ``;``. Appending another separator verbatim produces
    # ``done; ; set -eu``, which both bash and dash reject. Normalize only the
    # generated trailing delimiter at this composition boundary.
    return (
        managed_repository_agent_retirement_command(
            home_path=home_path,
            authority_ids=None,
            remove_configs=True,
        )
        .rstrip()
        .rstrip(";")
    )


async def retire_managed_repository_processes(
    *,
    host: str,
    port: int,
    host_key_fingerprint: str,
    home_path: str = WORKSPACE_HOME,
    operation: str = "managed repository process retirement",
) -> bool:
    """Retire and independently prove zero on one pinned runtime endpoint."""

    command = (
        _whole_workspace_retirement(home_path)
        + "; "
        + managed_repository_agent_zero_command(home_path=home_path)
    )
    return await _run_pinned(
        host=host,
        port=port,
        host_key_fingerprint=host_key_fingerprint,
        command=command,
        operation=operation,
    )


#: Strips the userinfo SRW's token-in-URL clones write (``oauth2:<token>@``,
#: or ``x-access-token:<token>@`` for a minted GitHub App token; see
#: ``agent.connectors.checkout.token_username``) from the remotes of the
#: checkouts SRW clones under ``<workspace>/repos/``. ``argv``: that
#: directory. Each checkout's own ``.git/config`` is read and written with
#: ``git config --file`` (never followed through a symlink); a remote value
#: without such userinfo is left as it is, and so is every other key. It
#: prints one JSON line naming the checkouts and keys it changed, never a
#: URL, and exits non-zero when a rewrite failed.
STRIP_CHECKOUT_TOKENS_PROGRAM = r"""
import json, os, subprocess, sys
from urllib.parse import urlsplit, urlunsplit

repos = sys.argv[1]
USERS = ('oauth2', 'x-access-token')
env = {
    'PATH': os.environ.get('PATH') or '/usr/bin:/bin',
    'HOME': os.environ.get('HOME') or '/',
    'GIT_CONFIG_NOSYSTEM': '1',
    'GIT_TERMINAL_PROMPT': '0',
    'LC_ALL': 'C',
}


def git(*args):
    return subprocess.run(
        ('git',) + args, capture_output=True, text=True, env=env, timeout=30
    )


def clean(url):
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if parts.scheme.lower() not in ('http', 'https') or '@' not in parts.netloc:
        return None
    userinfo, _, host = parts.netloc.rpartition('@')
    user, colon, secret = userinfo.partition(':')
    if user not in USERS or not colon or not secret or not host:
        return None
    return urlunsplit(parts._replace(netloc=host))


failed, stripped = 0, []
try:
    names = sorted(os.listdir(repos))
except OSError:
    names = []
for name in names:
    checkout = os.path.join(repos, name)
    dot_git = os.path.join(checkout, '.git')
    config = os.path.join(dot_git, 'config')
    if any(os.path.islink(path) for path in (checkout, dot_git, config)):
        continue
    if not os.path.isfile(config):
        continue
    listed = git(
        'config', '--file', config, '--null', '--get-regexp',
        r'^remote\..*\.(url|pushurl)$',
    )
    if listed.returncode == 1:
        continue
    if listed.returncode != 0:
        failed += 1
        continue
    values = {}
    for item in listed.stdout.split('\0'):
        if item:
            key, _, value = item.partition('\n')
            values.setdefault(key, []).append(value)
    for key, current in values.items():
        new = [clean(value) or value for value in current]
        if new == current:
            continue
        ok = git('config', '--file', config, '--unset-all', key).returncode == 0
        for value in new:
            ok = ok and git('config', '--file', config, '--add', key, value).returncode == 0
        if ok:
            stripped.append(name + ':' + key)
        else:
            failed += 1
print(json.dumps({'stripped': stripped, 'failed': failed}))
sys.exit(1 if failed else 0)
"""


def workspace_credential_scrub_command(home_path: str = WORKSPACE_HOME) -> str:
    """Remove SRW's connector credential material from one workspace home.

    It removes, and nothing else:

    - each credential-file store, through its own ``retire`` program
      (:data:`INSTALL_CREDENTIAL_FILES`): the store, the links it placed (the
      merged kubeconfig's ``~/.kube/config`` among them) and the work item's
      environment file; a link it did not place, or a file the user owns, is
      never touched;
    - the rest of ``~/.srw-credentials/``: environment connectors' files,
      lease tokens and the git swap driver's wiring; a symlink standing
      there is removed as a link, never followed;
    - pre-agent repository keys ``~/.ssh/repo_<slug>``, only those an exact
      SRW block in ``~/.ssh/config`` names, by the agent's own rule and
      program (:mod:`shared.runtime.core.legacy_ssh_keys`), with those
      blocks; a user's ``~/.ssh/repo_deploy`` is never touched;
    - the token SRW's token-in-URL clones put in their remotes, in the
      checkouts under ``~/workspace/repos/``
      (:data:`STRIP_CHECKOUT_TOKENS_PROGRAM`);
    - the managed ssh-agent namespace ``~/.ssh/srw-managed/``, after its
      agents are retired exactly as a terminal teardown retires them.

    What stays: the ``include.path`` line the git swap wiring added to
    ``~/.gitconfig`` and the ``Include`` line for the managed namespace in
    ``~/.ssh/config``; both are lines in the user's own files, and both name
    nothing once the scrub ran. Live shells are not this command's: the
    caller retires the job's tmux session first.

    Every step runs even when an earlier one failed. The command exits
    non-zero when any failed, or when ``~/.srw-credentials`` or
    ``~/.ssh/srw-managed`` is still there afterwards.
    """

    home = str(home_path).rstrip("/")
    root = shlex.quote(f"{home}/.srw-credentials")
    managed = shlex.quote(f"{home}/.ssh/srw-managed")
    ssh_dir = f"{home}/.ssh"
    retire_store = (
        f"{WORKSPACE_PYTHON} -c {shlex.quote(INSTALL_CREDENTIAL_FILES)} "
        f'{shlex.quote(home)} "${{_srw_store##*/}}" retire '
        "</dev/null >/dev/null 2>&1 || true"
    )
    retire_keys = (
        f"python3 -c {shlex.quote(RETIRE_LEGACY_KEYS_PROGRAM)} "
        f"{shlex.quote(ssh_dir)} $_srw_keys"
    )
    return (
        "_srw_rc=0; "
        f"if [ -L {root} ]; then rm -f -- {root} || _srw_rc=1; else "
        f"for _srw_store in {root}/files-*; do "
        'test -d "$_srw_store" || continue; '
        f"{retire_store}; done; "
        f"rm -rf -- {root} || _srw_rc=1; fi; "
        # A space after ``$(``: the listing opens with its own subshell, and
        # ``$((`` would read as arithmetic.
        f"_srw_keys=$( {list_legacy_keys_command(ssh_dir)} ) || _srw_keys=''; "
        f"_srw_keys=${{_srw_keys##*{LEGACY_KEY_MARKER}}}; "
        f'if [ -n "$_srw_keys" ]; then {retire_keys} || _srw_rc=1; fi; '
        f"{WORKSPACE_PYTHON} -c {shlex.quote(STRIP_CHECKOUT_TOKENS_PROGRAM)} "
        f"{shlex.quote(home + '/workspace/repos')} >/dev/null || _srw_rc=1; "
        f"( {_whole_workspace_retirement(home)} ) || _srw_rc=1; "
        f"rm -rf -- {managed} || _srw_rc=1; "
        f"test ! -e {root} || _srw_rc=1; "
        f"test ! -e {managed} || _srw_rc=1; "
        'exit "$_srw_rc"'
    )


async def scrub_workspace_credentials(
    *,
    host: str,
    port: int,
    host_key_fingerprint: str,
    home_path: str = WORKSPACE_HOME,
    operation: str = "refused-resume credential scrub",
) -> bool:
    """Scrub SRW's credential material on one pinned workspace endpoint."""

    return await _run_pinned(
        host=host,
        port=port,
        host_key_fingerprint=host_key_fingerprint,
        command=workspace_credential_scrub_command(home_path),
        operation=operation,
    )


async def _run_pinned(
    *,
    host: str,
    port: int,
    host_key_fingerprint: str,
    command: str,
    operation: str,
) -> bool:
    """Run one credential-free command on an endpoint whose host key is pinned."""

    key_path = resolve_ssh_key_path()
    if (
        asyncssh is None
        or not key_path
        or not isinstance(host, str)
        or not host
        or isinstance(port, bool)
        or not isinstance(port, int)
        or not 1 <= port <= 65535
        or not isinstance(host_key_fingerprint, str)
        or not host_key_fingerprint.startswith("SHA256:")
    ):
        logger.warning("%s refused without exact SSH authority", operation)
        return False

    connection = None
    try:
        async with asyncio.timeout(30):
            connection = await asyncssh.connect(
                host,
                port=port,
                username="agent-host",
                client_keys=[str(key_path)],
                known_hosts=_EMPTY_KNOWN_HOSTS,
                client_factory=lambda: _PinnedWorkspaceSSHClient(host_key_fingerprint),
                server_host_key_algs=["ssh-ed25519"],
                connect_timeout=10,
                login_timeout=15,
            )
            result = await connection.run(command, check=False, timeout=20)
        if result.exit_status == 0:
            return True
        logger.warning("%s failed with rc=%d", operation, result.exit_status)
        return False
    except TimeoutError:
        logger.warning("%s timed out", operation)
        return False
    except Exception:
        logger.warning("%s failed", operation, exc_info=True)
        return False
    finally:
        if connection is not None:
            connection.close()
            with suppress(Exception):
                await connection.wait_closed()


__all__ = [
    "retire_managed_repository_processes",
    "scrub_workspace_credentials",
    "workspace_credential_scrub_command",
]
