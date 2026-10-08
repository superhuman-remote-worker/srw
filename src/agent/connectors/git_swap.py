"""The workspace side of the git swap driver (connector drivers C3).

A token repository bound through ``srw.git-swap/v1`` keeps its clean
upstream URL as its remote. What makes its git reach the driver instead is
SRW's wiring under ``~/.srw-credentials/git/`` (written over the secret stdin
channel, never tmux; no snapshot captures it), which ``~/.gitconfig``
includes, so the agent's git, IDE terminals and ssh-gateway sessions all
read it. One include per binding:

* ``url.<driver URL>.insteadOf = <upstream base>``: the clean remote is
  rewritten to ``https://<endpoint>/<connector>/<repository>`` on use;
* ``credential.<driver origin>``: the list of helpers reset, then SRW's
  helper, which answers with the connector's lease token from
  ``~/.srw-credentials/leases/<connector>`` (``useHttpPath`` keeps two
  connectors apart);
* ``http.<driver origin>/.sslCAInfo``: SRW's certificate authority, trusted
  for the driver's URL only, never globally.

A checkout of such a repository also gets ``transfer.credentialsInUrl =
die``, so a token in its remote URL fails loudly instead of working.

Design: knowledge-base/knowledge/features/connector_drivers.md, "The git
swap driver" (workspace wiring).
"""

from __future__ import annotations

import logging
import posixpath
import re
import shlex
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from shared.connectors.git_swap import (
    WIRING_DIR,
    UnservedUpstream,
    connector_path_id,
    swap_upstream,
)

logger = logging.getLogger(__name__)

HELPER_FILE = f"{WIRING_DIR}/credential-helper"
#: How often the first clone retries a driver that is not serving yet.
_WAIT_INTERVAL_SECONDS = 5.0
#: One reachability try (a dropped SYN before the binding's ingress policy
#: lands would otherwise hang for curl's default).
_TRY_SECONDS = 20
#: Paths and URLs written into git config: nothing that needs quoting.
_SAFE_PATH = re.compile(r"[A-Za-z0-9._/-]+")
_SAFE_URL = re.compile(r"https://[A-Za-z0-9._:/~-]+")
#: Git's report of an HTTP answer: once the driver answers at all, its pod is
#: serving and its answer stands (the lease refused, the repository not
#: served, the upstream refusing or redirecting), except a 503 (the driver
#: stopping, the lease exchange unavailable), which may pass.
_HTTP_ANSWER = re.compile(r"returned error: (\d{3})")
_FINAL_FAILURES = ("Authentication failed",)


@dataclass(frozen=True)
class SwapBinding:
    """One repository connector bound through the git swap driver."""

    connector_id: str
    clean_url: str
    base: str
    driver_url: str
    origin: str
    ca: str
    wait_seconds: float


def swap_binding(entry: Mapping[str, Any]) -> tuple[SwapBinding | None, str]:
    """The binding a payload entry describes, or ``None`` and why not."""
    block = entry.get("git_swap")
    if not isinstance(block, Mapping):
        return None, "the entry is not bound through the git swap driver"
    if "unavailable" in block:
        return None, str(block["unavailable"])
    lease = (entry.get("credentials") or {}).get("lease")
    if not isinstance(lease, Mapping) or not lease.get("token"):
        return None, "the git swap driver's lease was not delivered"
    try:
        connector_id = connector_path_id(entry.get("datasource_id"))
        upstream = swap_upstream(entry.get("connection_url"))
    except (UnservedUpstream, ValueError) as exc:
        return None, f"the binding is malformed ({exc})"
    driver_url = str(block.get("url") or "")
    parts = urlsplit(driver_url)
    origin = f"https://{parts.netloc}"
    if (
        not _SAFE_URL.fullmatch(driver_url)
        or parts.scheme != "https"
        or not parts.hostname
        or parts.path != f"/{connector_id}/{upstream.path}"
        or "@" in parts.netloc
    ):
        return None, "the driver's URL is not this connector's repository"
    ca = str(block.get("ca") or "")
    if "-----BEGIN CERTIFICATE-----" not in ca:
        return None, "the binding carries no certificate authority"
    try:
        wait = max(0.0, float(block.get("wait_seconds") or 0))
    except (TypeError, ValueError):
        wait = 0.0
    return (
        SwapBinding(
            connector_id=connector_id,
            clean_url=upstream.url,
            base=upstream.base,
            driver_url=driver_url,
            origin=origin,
            ca=ca,
            wait_seconds=wait,
        ),
        "",
    )


def render_include(binding: SwapBinding, *, home: str) -> str:
    """The git config include one binding installs."""
    if not _SAFE_PATH.fullmatch(home) or not home.startswith("/"):
        raise ValueError("the workspace home cannot be written into git config")
    if not _SAFE_URL.fullmatch(binding.base):
        raise ValueError("the upstream URL cannot be written into git config")
    helper = f"{home}/{HELPER_FILE}"
    ca = f"{home}/{WIRING_DIR}/bindings/{binding.connector_id}.ca.pem"
    return (
        f"# SRW git swap driver: connector {binding.connector_id}. Written by SRW\n"
        "# on every attach; edits are overwritten.\n"
        f'[url "{binding.driver_url}"]\n'
        f"\tinsteadOf = {binding.base}\n"
        f'[credential "{binding.origin}"]\n'
        "\thelper =\n"
        f"\thelper = \"!/usr/bin/python3 -I '{helper}' {binding.connector_id}\"\n"
        "\tuseHttpPath = true\n"
        f'[http "{binding.origin}/"]\n'
        f"\tsslCAInfo = {ca}\n"
        "\tfollowRedirects = false\n"
    )


def install_wiring(
    backend: Any,
    bindings: Iterable[SwapBinding],
    *,
    remove: Iterable[str] = (),
    prune: bool = False,
) -> dict[str, Any]:
    """Write the wiring of ``bindings`` (and drop ``remove``) on the workspace."""
    home = posixpath.dirname(backend.resolve_home_path(".srw-credentials"))
    return backend.install_git_swap_wiring(
        [
            {
                "id": binding.connector_id,
                "include": render_include(binding, home=home),
                "ca": binding.ca,
            }
            for binding in bindings
        ],
        remove=list(remove),
        prune=prune,
    )


def wait_for_driver(
    backend: Any,
    binding: SwapBinding,
    *,
    sleep: Callable[[float], None] | None = None,
    clock: Callable[[], float] | None = None,
) -> str | None:
    """Wait until the workspace's git reaches the repository through the
    driver; ``None`` once it does, else why not.

    A new binding's pod starts on the reconciler's next pass, and its
    ingress policy for this workspace lands after it, so the first clone
    retries a connection failure for up to the binding's ``wait_seconds``.
    A refusal (the lease, the repository) is final at once.
    """
    sleep = sleep or time.sleep
    clock = clock or time.monotonic
    deadline = clock() + binding.wait_seconds
    command = (
        f"timeout {_TRY_SECONDS} git ls-remote --quiet "
        f"{shlex.quote(binding.clean_url)} HEAD"
    )
    last = ""
    while True:
        output = str(
            backend.shell_run(command, timeout=_TRY_SECONDS + 10, tab_name="git")
        )
        first = output.split("\n", 1)[0].strip()
        if first.startswith("Exit code: 0"):
            return None
        last = output
        answered = _HTTP_ANSWER.search(output)
        if any(marker in output for marker in _FINAL_FAILURES) or (
            answered is not None and answered.group(1) != "503"
        ):
            break
        if clock() + _WAIT_INTERVAL_SECONDS > deadline:
            break
        sleep(_WAIT_INTERVAL_SECONDS)
    detail = " ".join(last.split())[-300:]
    return f"the git swap driver did not serve the repository ({detail})"


def swap_note(entry: Mapping[str, Any]) -> str:
    """The README's note on a repository bound (or refused) through the driver."""
    block = entry.get("git_swap")
    if not isinstance(block, Mapping):
        return ""
    if "unavailable" in block:
        return f" — NOT cloned: {block['unavailable']}"
    return (
        " — fetches and pushes go through SRW's git swap driver with a lease "
        "(the forge token never enters the workspace): branch pushes only, "
        "no ref deletes or tags, no Git LFS"
    )


__all__ = [
    "HELPER_FILE",
    "SwapBinding",
    "install_wiring",
    "render_include",
    "swap_binding",
    "swap_note",
    "wait_for_driver",
]
