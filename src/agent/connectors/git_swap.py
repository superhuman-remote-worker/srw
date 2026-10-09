"""The workspace side of the git swap driver (connector drivers C3).

A token repository bound through ``srw.git-swap/v1`` keeps its clean
upstream URL as its remote, normalised to ``<repository>.git``. What makes
its git reach the driver instead is SRW's wiring under
``~/.srw-credentials/git/`` (written over the secret stdin channel, never
tmux; no snapshot captures it), which ``~/.gitconfig`` includes. One rule
file per binding (``bindings/<connector>.gitconfig``):

* ``url.<driver URL>.git.insteadOf = <clean remote>``: the exact clean
  remote is rewritten to ``https://<endpoint>/<connector>/<repository>.git``
  on use. Git rewrites by prefix, and ``o/r.git`` is no prefix of
  ``o/r-docs.git``;
* ``credential.<driver origin>``: the list of helpers reset, then SRW's
  helper, which answers with the connector's lease token from
  ``~/.srw-credentials/leases/<connector>`` (``useHttpPath`` keeps two
  connectors apart);
* ``http.<driver origin>/.sslCAInfo``: SRW's certificate authority, trusted
  for the driver's URL only, never globally.

A rule file applies only inside its own checkout: the include git reads
names it under ``[includeIf "gitdir:<workspace>/repos/<clone>/"]``. Two
connectors may name one upstream (a ReadWrite and a ReadOnly binding of the
same repository): each checkout gets its own connector's driver URL and
lease, whatever the order of their ids. The condition is git's own, so it
holds for every git that runs in the checkout: the agent's, an IDE
terminal's and an ssh-gateway session's (they share the workspace home, and
so ``~/.gitconfig``). Outside SRW's checkouts nothing is rewritten: a clone
made by hand of the same URL reaches the forge directly, without the
connector's credential. The clone itself, and the wait for a starting
driver, run before the checkout exists; they name the binding's rule file
with ``git -c include.path=<file>``.

A checkout of such a repository also gets ``transfer.credentialsInUrl =
die``, so a token in its remote URL fails loudly instead of working.

While a first clone waits for a starting driver pod, the agent asks the
orchestrator between tries whether the reconciler refused that pod
(:func:`driver_refusal`, with the binding's own lease token): a refused pod
never starts, so the wait ends at once with the refusal's fixed reason.

Design: knowledge-base/knowledge/features/connector_drivers.md, "The git
swap driver" (workspace wiring).
"""

from __future__ import annotations

import logging
import math
import os
import posixpath
import re
import shlex
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import httpx

from shared.connectors.git_swap import (
    DRIVER_REFUSED,
    DRIVER_STATE_PATH,
    WIRING_DIR,
    UnservedUpstream,
    connector_path_id,
    swap_upstream,
)

logger = logging.getLogger(__name__)

HELPER_FILE = f"{WIRING_DIR}/credential-helper"
BINDINGS_DIR = f"{WIRING_DIR}/bindings"
#: How often the first clone retries a driver that is not serving yet.
_WAIT_INTERVAL_SECONDS = 5.0
#: One reachability try (a dropped SYN before the binding's ingress policy
#: lands would otherwise hang for curl's default).
_TRY_SECONDS = 20
#: Paths and URLs written into git config: nothing that needs quoting and no
#: glob character (a checkout path is a ``gitdir:`` pattern).
_SAFE_PATH = re.compile(r"[A-Za-z0-9._/-]+")
_SAFE_URL = re.compile(r"https://[A-Za-z0-9._:/~-]+")
#: A checkout's ``gitdir:`` pattern (the wiring program checks the same).
_GITDIR = re.compile(r"/(?:[A-Za-z0-9_-][A-Za-z0-9._-]*/)+")
#: Git's report of an HTTP answer: once the driver answers at all, its pod is
#: serving and its answer stands (the lease refused, the repository not
#: served, the upstream refusing or redirecting), except a 503 (the driver
#: stopping, the lease exchange unavailable), which may pass.
_HTTP_ANSWER = re.compile(r"returned error: (\d{3})")
_FINAL_FAILURES = ("Authentication failed",)
#: One question to the orchestrator about a binding's driver pod.
_STATE_SECONDS = 5.0
#: The longest refusal reason a README line takes (the orchestrator sends
#: one of its fixed reasons, which are shorter).
_MAX_REASON = 200


@dataclass(frozen=True)
class SwapBinding:
    """One repository connector bound through the git swap driver.

    ``clean_url`` is the checkout's remote (``<repository>.git``), the exact
    string the binding's ``insteadOf`` rewrites; ``driver_url`` is the
    driver's URL for the repository, without ``.git``.
    """

    connector_id: str
    clean_url: str
    driver_url: str
    origin: str
    ca: str
    wait_seconds: float
    #: The binding's lease token: what the agent asks the orchestrator about
    #: the binding's pod with (:func:`driver_refusal`). Never in a repr.
    lease_token: str = field(default="", repr=False, compare=False)


def swap_binding(entry: Mapping[str, Any]) -> tuple[SwapBinding | None, str]:
    """The binding a payload entry describes, or ``None`` and why not."""
    block = entry.get("git_swap")
    if not isinstance(block, Mapping):
        return None, "the entry is not bound through the git swap driver"
    if "unavailable" in block:
        return None, str(block["unavailable"])
    if "url" not in block:
        return None, "the entry is not bound through the git swap driver"
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
            clean_url=upstream.remote,
            driver_url=driver_url,
            origin=origin,
            ca=ca,
            wait_seconds=wait,
            lease_token=str(lease["token"]),
        ),
        "",
    )


def _safe_home(home: str) -> str:
    if not _SAFE_PATH.fullmatch(home) or not home.startswith("/"):
        raise ValueError("the workspace home cannot be written into git config")
    return home


def rule_file(binding: SwapBinding, *, home: str) -> str:
    """Where a binding's rules live in the workspace (absolute)."""
    return f"{_safe_home(home)}/{BINDINGS_DIR}/{binding.connector_id}.gitconfig"


def render_include(binding: SwapBinding, *, home: str) -> str:
    """The git config rules one binding installs."""
    home = _safe_home(home)
    if not _SAFE_URL.fullmatch(binding.clean_url):
        raise ValueError("the upstream URL cannot be written into git config")
    helper = f"{home}/{HELPER_FILE}"
    ca = f"{home}/{BINDINGS_DIR}/{binding.connector_id}.ca.pem"
    return (
        f"# SRW git swap driver: connector {binding.connector_id}. Written by SRW\n"
        "# on every attach; edits are overwritten.\n"
        f'[url "{binding.driver_url}.git"]\n'
        f"\tinsteadOf = {binding.clean_url}\n"
        f'[credential "{binding.origin}"]\n'
        "\thelper =\n"
        f"\thelper = \"!/usr/bin/python3 -I '{helper}' {binding.connector_id}\"\n"
        "\tuseHttpPath = true\n"
        f'[http "{binding.origin}/"]\n'
        f"\tsslCAInfo = {ca}\n"
        "\tfollowRedirects = false\n"
    )


def checkout_gitdir(backend: Any, clone_name: str) -> str:
    """The ``gitdir:`` pattern of a checkout: its absolute directory and a
    trailing slash (the checkout's ``.git`` and everything under it, its
    worktrees included)."""
    gitdir = str(backend.resolve_path(f"repos/{clone_name}")).rstrip("/") + "/"
    if not _GITDIR.fullmatch(gitdir):
        raise ValueError("the checkout path cannot be written into git config")
    return gitdir


def install_wiring(
    backend: Any,
    bindings: Iterable[SwapBinding],
    *,
    checkouts: Mapping[str, str] | None = None,
    remove: Iterable[str] = (),
    prune: bool = False,
) -> dict[str, Any]:
    """Write the wiring of ``bindings`` (and drop ``remove``) on the workspace.

    ``checkouts`` maps a connector id to its checkout's clone name: the
    binding's rules apply in that checkout only. A binding without one keeps
    the checkout an earlier write recorded.
    """
    home = posixpath.dirname(backend.resolve_home_path(".srw-credentials"))
    checkouts = checkouts or {}
    items = []
    for binding in bindings:
        item = {
            "id": binding.connector_id,
            "include": render_include(binding, home=home),
            "ca": binding.ca,
        }
        clone_name = checkouts.get(binding.connector_id)
        if clone_name:
            item["gitdir"] = checkout_gitdir(backend, clone_name)
        items.append(item)
    return backend.install_git_swap_wiring(items, remove=list(remove), prune=prune)


def binding_options(backend: Any, binding: SwapBinding) -> list[str]:
    """``git -c`` options that apply a binding's rules where no checkout
    exists yet (the clone, the wait for a starting driver)."""
    home = posixpath.dirname(backend.resolve_home_path(".srw-credentials"))
    return [f"include.path={rule_file(binding, home=home)}"]


def driver_refusal(binding: SwapBinding, remaining: float = math.inf) -> str | None:
    """Why the orchestrator refused to start the binding's driver pod, or
    ``None`` while that pod may still serve (not started yet, starting,
    serving) or nothing is known.

    Asks the orchestrator's internal route (:data:`DRIVER_STATE_PATH`) with
    the binding's own lease token, in the body: the agent learns about its
    own bindings only, and the answer is one of the orchestrator's fixed
    reasons. A refusal whose back-off ends within the ``remaining`` wait is
    no answer yet (the reconciler may start the pod again by then). Never
    raises: without an answer the wait goes on as before. Blocking; it runs
    where the wait runs, in a worker thread.
    """
    base = os.getenv("ORCHESTRATOR_URL", "").strip().rstrip("/")
    if not base or not binding.lease_token:
        return None
    headers: dict[str, str] = {}
    internal_key = os.getenv("MCP_INTERNAL_KEY", "")
    if internal_key:
        headers["X-Internal-Key"] = internal_key
    try:
        response = httpx.post(
            f"{base}{DRIVER_STATE_PATH}",
            json={"lease_token": binding.lease_token},
            headers=headers,
            timeout=_STATE_SECONDS,
        )
        body = response.json() if response.status_code == 200 else None
    except (httpx.HTTPError, ValueError) as exc:
        logger.debug(
            "Could not ask about the git swap driver of connector %s (%s)",
            binding.connector_id,
            type(exc).__name__,
        )
        return None
    if not isinstance(body, dict) or body.get("state") != DRIVER_REFUSED:
        return None
    retry_in = body.get("retry_in_seconds")
    if (
        isinstance(retry_in, (int, float))
        and not isinstance(retry_in, bool)
        and retry_in < remaining
    ):
        return None
    reason = " ".join(str(body.get("reason") or "").split())[:_MAX_REASON]
    return reason or "its driver pod did not start"


def wait_for_driver(
    backend: Any,
    binding: SwapBinding,
    *,
    sleep: Callable[[float], None] | None = None,
    clock: Callable[[], float] | None = None,
    refusal: Callable[[SwapBinding, float], str | None] | None = None,
) -> str | None:
    """Wait until the workspace's git reaches the repository through the
    driver; ``None`` once it does, else why not.

    A new binding's pod starts on the reconciler's next pass, and its
    ingress policy for this workspace lands after it, so the first clone
    retries a connection failure for up to the binding's ``wait_seconds``.
    A refusal (the lease, the repository) is final at once, and so is the
    reconciler's refusal of the pod itself, which ``refusal`` (by default
    :func:`driver_refusal`, given the seconds left of the wait) reports
    after each failed try: that pod does not start within the wait, so its
    fixed reason is the answer, not the timeout.
    """
    sleep = sleep or time.sleep
    clock = clock or time.monotonic
    refusal = refusal or driver_refusal
    deadline = clock() + binding.wait_seconds
    options = " ".join(
        f"-c {shlex.quote(option)}" for option in binding_options(backend, binding)
    )
    command = (
        f"timeout {_TRY_SECONDS} git {options} ls-remote --quiet "
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
        try:
            refused = refusal(binding, max(0.0, deadline - clock()))
        except Exception:  # a question, never a reason to stop waiting
            refused = None
        if refused:
            logger.info(
                "The git swap driver of connector %s was refused: %s",
                binding.connector_id,
                refused,
            )
            return f"the git swap driver could not serve the repository: {refused}"
        if clock() + _WAIT_INTERVAL_SECONDS > deadline:
            break
        sleep(_WAIT_INTERVAL_SECONDS)
    detail = " ".join(last.split())[-300:]
    return f"the git swap driver did not serve the repository ({detail})"


def swap_note(entry: Mapping[str, Any], *, cloned: bool = True) -> str:
    """The README's note on how a token repository is reached: through the
    driver, refused, or on the installation's fallback (and why). For a
    repository that was not cloned (``cloned=False``), how it was to be."""
    block = entry.get("git_swap")
    if not isinstance(block, Mapping):
        return ""
    if "unavailable" in block:
        return f" — NOT cloned: {block['unavailable']}"
    if "fallback" in block:
        how = "cloned" if cloned else "to be cloned"
        return (
            f" — {how} with the forge token in its remote URL, NOT through "
            f"SRW's git swap driver: {block['fallback']}"
        )
    return (
        " — fetches and pushes go through SRW's git swap driver with a lease "
        "(the forge token never enters the workspace): branch pushes only, "
        "no ref deletes or tags, no Git LFS"
    )


__all__ = [
    "BINDINGS_DIR",
    "HELPER_FILE",
    "SwapBinding",
    "binding_options",
    "checkout_gitdir",
    "driver_refusal",
    "install_wiring",
    "render_include",
    "rule_file",
    "swap_binding",
    "swap_note",
    "wait_for_driver",
]
