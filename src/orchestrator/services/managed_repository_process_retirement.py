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
from shared.runtime.core.managed_repository import (
    managed_repository_agent_retirement_command,
    managed_repository_agent_zero_command,
)

logger = logging.getLogger(__name__)

_EMPTY_KNOWN_HOSTS = ((), (), (), (), (), (), ())


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
    home_path: str = "/home/agent-host",
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


def workspace_credential_scrub_command(home_path: str = "/home/agent-host") -> str:
    """Remove SRW's connector credential material from one workspace home.

    What an agent's terminal shell retirement and its attach syncs remove,
    for every work item at once, and nothing of the user's:

    - each credential-file store, through its own ``retire`` program
      (:data:`INSTALL_CREDENTIAL_FILES`), which removes the store, the links
      it placed (the merged kubeconfig's ``~/.kube/config`` among them) and
      the work item's environment file; a link it did not place, or a file
      the user owns, is never touched;
    - the rest of ``~/.srw-credentials/``: environment connectors' files,
      lease tokens and the git swap driver's wiring (the ``include.path``
      line it added to ``~/.gitconfig`` then names nothing, so git skips it);
    - the legacy repository keys ``~/.ssh/repo_*``;
    - the managed ssh-agent namespace ``~/.ssh/srw-managed/``, after its
      agents are retired exactly as a terminal teardown retires them.

    The files go first: when the agent classifier refuses (an ambiguous
    process), the command stops with a non-zero status after them.
    """

    home = str(home_path).rstrip("/")
    root = f"{home}/.srw-credentials"
    managed = f"{home}/.ssh/srw-managed"
    retire_store = (
        f"{WORKSPACE_PYTHON} -c {shlex.quote(INSTALL_CREDENTIAL_FILES)} "
        f'{shlex.quote(home)} "${{_srw_store##*/}}" retire '
        "</dev/null >/dev/null 2>&1 || true"
    )
    return (
        f"for _srw_store in {shlex.quote(root)}/files-*; do "
        'test -d "$_srw_store" || continue; '
        f"{retire_store}; done; "
        f"rm -rf -- {shlex.quote(root)}; "
        f"rm -f -- {shlex.quote(home)}/.ssh/repo_*; "
        + _whole_workspace_retirement(home)
        + f"; rm -rf -- {shlex.quote(managed)}; "
        f"test ! -e {shlex.quote(root)}; "
        f"test ! -e {shlex.quote(managed)}"
    )


async def scrub_workspace_credentials(
    *,
    host: str,
    port: int,
    host_key_fingerprint: str,
    home_path: str = "/home/agent-host",
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
