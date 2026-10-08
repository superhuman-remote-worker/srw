"""``checkout``: repository connectors, cloned onto the workspace.

Every operation runs on the workspace backend; there is no agent-local clone
(knowledge-base/knowledge/features/no_workspace_agent_mode.md §9.4). A token
repository bound through the git swap driver (C3) clones its clean upstream
URL (``<repository>.git``): SRW's wiring (``agent.connectors.git_swap``,
installed before the clones, scoped to each binding's own checkout) points
it at the driver, a reused checkout's origin is reset to the clean URL
(dropping any ``oauth2:`` token an earlier clone left; a checkout whose
origin cannot be reset is not used), and the checkout refuses credentials
in a URL from then on. Where the installation falls back, a token
repository clones with the token in its URL as before, and the README says
so and why. A repository that is not cloned says why in the README too.
An SSH-key repository clones through the opaque alias of its workspace
``ssh-agent`` identity (``agent.connectors.ssh_identity``, slice C1), and a
key file a pre-agent clone left in ``~/.ssh`` is retired once the repository
is proven to work through that alias.

The clone keeps C1's rules on the payload entry
(``clone_repository_datasources`` reads it exactly as before); the
materializer routes by form and supplies the identity status and the
legacy-key mode from the runtime context.
"""

from __future__ import annotations

import logging
import re
import shlex
from collections.abc import Sequence
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from agent.connectors.base import (
    Delivery,
    FactsLines,
    RuntimeContext,
    declared_read_only_note,
)
from agent.connectors.git_swap import (
    SwapBinding,
    binding_options,
    install_wiring,
    swap_binding,
    swap_note,
    wait_for_driver,
)
from agent.connectors.legacy import checkout_auth
from agent.connectors.ssh_identity import ssh_clone_target, ssh_identity_note
from shared.datasource_policy import resolve_repo_clone_names

logger = logging.getLogger(__name__)


#: A pre-agent clone wrote its key to ``~/.ssh/repo_<datasource-name-slug>``
#: and appended, to ``~/.ssh/config``, exactly
#: ``\nHost <host>\n  IdentityFile <abs key path>\n  StrictHostKeyChecking
#: accept-new\n``. Only a key that such an exact block names, and that no
#: other config line names, is ours to delete: an ``IdentityFile`` line a user
#: wrote (absolute path or not) never causes a delete, and neither does a key
#: no line names (``ssh-keygen -f ~/.ssh/repo_deploy``).
_LEGACY_KEY_NAME = re.compile(r"repo_[a-z0-9-]+")
_LEGACY_KEY_MARKER = "__srw_legacy_repo_keys__"
#: How far a clone call may retire those files: ``sweep`` (the workspace
#: owner's attach, which sees every repository),
#: ``own`` (a live add: only each proven repository's own file) or ``keep``
#: (a child job on its parent's workspace, which a parent still on a pre-agent
#: image may need for its next fetch or push).
LEGACY_KEY_FILE_MODES = frozenset({"sweep", "own", "keep"})

# One awk pass over config per candidate (``path`` is its absolute path).
# Every line naming the file must be the IdentityFile line of an exact
# pre-agent block (``Host <host>`` before it, ``  StrictHostKeyChecking
# accept-new`` after it, nothing indented after that), and one must be.
# The delete program applies the same rule again.
_EXACT_BLOCK_AWK = (
    "'{ line[NR] = $0 } END { exact = 0; other = 0; "
    'for (i = 1; i <= NR; i++) { s = line[i]; sub(/^[ \\t]+/, "", s); '
    'sub(/[ \\t\\r]+$/, "", s); if (s != "IdentityFile " path) continue; '
    'if (line[i] == "  IdentityFile " path && i > 1 '
    "&& line[i - 1] ~ /^Host [^ \\t\\r]+$/ && i < NR "
    '&& line[i + 1] == "  StrictHostKeyChecking accept-new" '
    "&& (i + 2 > NR || line[i + 2] !~ /^[ \\t]/)) exact++; else other++ } "
    "exit !(exact > 0 && other == 0) }'"
)


def _list_legacy_keys_command(ssh_dir: str) -> str:
    """List pre-agent key files: one marker line on stdout.

    Regular files only, slug-shaped names only, a PEM key header that is not
    a public key, and named in ``config`` only by exact pre-agent blocks
    (:data:`_EXACT_BLOCK_AWK`). Runs in a subshell to leave the persistent
    ``git`` tab where it was; the marker is split in the command so an echoed
    or wrapped command line can never read as the answer. (The pattern avoids
    spelling a private-key header: every command stays clear of what the
    key-residue checks scan for.)
    """

    prefix = shlex.quote(f"{ssh_dir}/")
    return (
        f"( cd {shlex.quote(ssh_dir)} 2>/dev/null || exit 0; "
        "test -f config || exit 0; _srw_names=''; "
        "for _srw_f in repo_*; do "
        '[ -f "$_srw_f" ] && [ ! -L "$_srw_f" ] || continue; '
        'case "${_srw_f#repo_}" in ""|*[!a-z0-9-]*) continue;; esac; '
        'IFS= read -r _srw_first < "$_srw_f" || continue; '
        'case "$_srw_first" in *"PUBLIC KEY-----") continue;; '
        '"-----BEGIN "*" KEY-----") ;; *) continue;; esac; '
        f'awk -v path={prefix}"$_srw_f" {_EXACT_BLOCK_AWK} config || continue; '
        '_srw_names="$_srw_names $_srw_f"; '
        "done; "
        f"printf '%s%s\\n' {_LEGACY_KEY_MARKER[:13]} "
        f'{_LEGACY_KEY_MARKER[13:]}"$_srw_names" )'
    )


# Runs on the workspace (argv: ssh dir, names). Re-checks every name exactly
# as the listing did (the same exact-block rule), deletes it, then removes
# each deleted key's exact pre-agent blocks from config (and the blank line
# before each). Since no other line may name a deleted key, nothing in config
# is left pointing at it; a block a user edited never qualified.
_RETIRE_LEGACY_KEYS_PROGRAM = r"""
import os, re, stat, sys, tempfile
ssh_dir, names = sys.argv[1], sys.argv[2:]
config = os.path.join(ssh_dir, "config")
try:
    with open(config, encoding="utf-8", errors="surrogateescape") as handle:
        lines = handle.read().split("\n")
except OSError:
    sys.exit(0)
HOST = re.compile(r"Host [^ \t\r]+")
TRUST = "  StrictHostKeyChecking accept-new"
def exact_block_at(index, path):
    following = lines[index + 2] if index + 2 < len(lines) else ""
    return (
        lines[index] == "  IdentityFile " + path
        and index > 0
        and HOST.fullmatch(lines[index - 1]) is not None
        and index + 1 < len(lines)
        and lines[index + 1] == TRUST
        and not following[:1].isspace()
    )
def only_exact_blocks_name(path):
    exact = other = 0
    for index, line in enumerate(lines):
        if line.strip() != "IdentityFile " + path:
            continue
        if exact_block_at(index, path):
            exact += 1
        else:
            other += 1
    return exact > 0 and other == 0
def key_file(path):
    try:
        if not stat.S_ISREG(os.lstat(path).st_mode):
            return False
        with open(path, "rb") as handle:
            first = handle.readline(4096).rstrip(b"\r\n")
    except OSError:
        return False
    return (
        first.startswith(b"-----BEGIN ")
        and first.endswith(b" KEY-----")
        and not first.endswith(b"PUBLIC KEY-----")
    )
removed = set()
for name in names:
    path = os.path.join(ssh_dir, name)
    if (
        re.fullmatch(r"repo_[a-z0-9-]+", name)
        and key_file(path)
        and only_exact_blocks_name(path)
    ):
        os.unlink(path)
        removed.add(path)
kept, index, changed = [], 0, False
while index < len(lines):
    if (
        index + 1 < len(lines)
        and lines[index + 1].startswith("  IdentityFile ")
        and lines[index + 1][len("  IdentityFile "):] in removed
        and exact_block_at(index + 1, lines[index + 1][len("  IdentityFile "):])
    ):
        if kept and kept[-1] == "":
            kept.pop()
        index += 3
        changed = True
        continue
    kept.append(lines[index])
    index += 1
if changed and not os.path.islink(config):
    mode = stat.S_IMODE(os.stat(config).st_mode)
    descriptor, temporary = tempfile.mkstemp(dir=ssh_dir, prefix=".config.srw-")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", errors="surrogateescape") as handle:
            handle.write("\n".join(kept))
        os.chmod(temporary, mode)
        os.replace(temporary, config)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
"""


def _legacy_ssh_key_files(backend: Any, ssh_dir: str) -> set[str]:
    """Names of pre-agent ``~/.ssh/repo_*`` key files; empty when unsure."""

    output = str(
        backend.shell_run(
            _list_legacy_keys_command(ssh_dir), timeout=10, tab_name="git"
        )
    )
    answers = [
        line.strip()[len(_LEGACY_KEY_MARKER) :]
        for line in output.splitlines()
        if line.strip().startswith(_LEGACY_KEY_MARKER)
    ]
    if not answers:
        return set()
    return {name for name in answers[-1].split() if _LEGACY_KEY_NAME.fullmatch(name)}


def _retire_legacy_ssh_key_files(
    backend: Any, outcomes: List[Tuple[str, Any]], *, mode: str
) -> None:
    """Delete pre-agent key files whose repository works through its alias.

    ``outcomes`` holds one ``(name slug, outcome)`` per SSH repository:
    ``True`` (cloned through its alias), ``False`` (skipped or failed) or the
    reused checkout's ``GitManager``, re-proven with ``ls-remote`` only when a
    key file is actually there. A repository's own ``repo_<slug>`` goes once
    it is proven. In ``sweep`` mode, with every SSH repository proven, every
    listed file goes, including those a renamed or detached connector left
    behind. ``keep`` touches nothing. Only files that exact pre-agent
    ``Host`` blocks, and nothing else in ``~/.ssh/config``, name are ever
    listed; those blocks go with the key.
    """

    if mode == "keep":
        return
    try:
        from shared.runtime.core.managed_repository import (
            _execute_managed_secret_command,
        )

        ssh_dir = backend.resolve_home_path(".ssh")
        found = _legacy_ssh_key_files(backend, ssh_dir)
        if not found:
            return
        can_sweep = mode == "sweep" and all(
            outcome is not False for _, outcome in outcomes
        )
        delete: set[str] = set()
        for ds_name, outcome in outcomes:
            own = f"repo_{ds_name}"
            if outcome is False or (own not in found and not can_sweep):
                continue
            if outcome is True or outcome.remote_reachable():
                if own in found:
                    delete.add(own)
            else:
                can_sweep = False
        if can_sweep:
            delete |= found
        if not delete:
            return
        command = " ".join(
            [
                "python3",
                "-c",
                shlex.quote(_RETIRE_LEGACY_KEYS_PROGRAM),
                shlex.quote(ssh_dir),
                *(shlex.quote(name) for name in sorted(delete)),
            ]
        )
        # Off tmux, under the claim fence a stateless backend provides.
        if _execute_managed_secret_command(
            backend,
            command,
            b"",
            timeout=30,
            operation="pre-agent SSH key retirement",
        ):
            logger.info("Retired %d pre-agent SSH key file(s)", len(delete))
        else:
            logger.warning("Could not retire pre-agent SSH key files")
    except Exception as exc:  # cleanup only: never fails a clone
        logger.warning(
            "Could not retire pre-agent SSH key files: %s", type(exc).__name__
        )


def _wire_swap_repositories(
    backend: Any,
    repo_datasources: List[Dict[str, Any]],
    clone_names: List[str],
    *,
    prune: bool,
) -> Tuple[Dict[int, SwapBinding], Dict[int, str]]:
    """Install the git swap wiring of every swap repository in the batch.

    Returns the bindings wired, by position, and why the others are not.
    Runs before any clone: the clone of a clean URL needs the wiring to
    reach the driver. Each binding's rules apply in its own checkout
    (``clone_names``). ``prune`` (the workspace owner's full set) also drops
    bindings an earlier attach left that this one no longer has.
    """
    wired: Dict[int, SwapBinding] = {}
    reasons: Dict[int, str] = {}
    for index, ds in enumerate(repo_datasources):
        if checkout_auth(ds) != "swap":
            continue
        binding, reason = swap_binding(ds)
        if binding is None:
            reasons[index] = reason
        else:
            wired[index] = binding
    if not wired:
        if prune:
            # The owner's set has no binding now (every repository fell
            # back or was refused, or the driver is off and no entry says
            # anything about it): an earlier attach's wiring goes, or its
            # rewrite would keep sending a checkout to the driver. A
            # workspace that never had any wiring is left as it is.
            try:
                install_wiring(backend, (), prune=True)
            except Exception as exc:
                logger.warning(
                    "Could not prune the git swap wiring: %s", type(exc).__name__
                )
        return wired, reasons
    try:
        install_wiring(
            backend,
            wired.values(),
            checkouts={
                binding.connector_id: clone_names[index]
                for index, binding in wired.items()
            },
            prune=prune,
        )
    except Exception as exc:  # the repositories are skipped, never cloned bare
        reason = (
            "the workspace's git wiring for the git swap driver could not be "
            f"installed ({type(exc).__name__})"
        )
        reasons.update({index: reason for index in wired})
        wired = {}
    return wired, reasons


def _secure_swap_checkout(
    git_mgr: Any, binding: SwapBinding, *, reused: bool
) -> Optional[str]:
    """Keep a swap checkout's remote the clean upstream URL and make a token
    in any of its URLs fail loudly (``transfer.credentialsInUrl``).

    ``None`` when the checkout is safe to use, else why not: a reused
    checkout whose origin cannot be reset still holds whatever URL an
    earlier clone left (an ``oauth2:<token>@`` one before C3), so it is not
    used at all.
    """
    if reused:
        reset = git_mgr.add_remote("origin", binding.clean_url)
        found = git_mgr._run_git(["config", "--get", "remote.origin.url"])
        current = str(getattr(found, "stdout", "") or "").strip()
        if (
            not reset
            or getattr(found, "returncode", 1) != 0
            or current != (binding.clean_url)
        ):
            return (
                "its existing checkout's origin could not be reset to the clean "
                "URL (it may still hold a token from an earlier clone); remove "
                "the directory to clone it again"
            )
    git_mgr._run_git(["config", "transfer.credentialsInUrl", "die"])
    return None


def _falls_back(ds: Dict[str, Any]) -> bool:
    block = ds.get("git_swap")
    return isinstance(block, dict) and "fallback" in block


def _swap_era(git_mgr: Any) -> bool:
    """Whether a reused checkout was the git swap driver's: it refuses
    credentials in URLs (``transfer.credentialsInUrl=die``), which only a
    swap checkout is set to. A pre-C3 token checkout is not. Only the
    checkout's own config counts (``--local``): a global or system setting
    says nothing about how this checkout was made."""
    found = git_mgr._run_git(
        ["config", "--local", "--get", "transfer.credentialsInUrl"]
    )
    return (
        getattr(found, "returncode", 1) == 0
        and str(getattr(found, "stdout", "") or "").strip() == "die"
    )


def _fallback_checkout(git_mgr: Any, token_url: str) -> Optional[str]:
    """Point a checkout the git swap driver served before at its token URL
    (the installation's fallback now); ``None`` when done, else why not."""
    git_mgr._run_git(["config", "--unset-all", "transfer.credentialsInUrl"])
    if not git_mgr.add_remote("origin", token_url):
        return "its existing checkout's origin could not be set for the fallback"
    return None


def _repository_identity(url: Any) -> Optional[Tuple[Optional[str], str]]:
    """``(host, path)`` of a repository URL, credentials and ``.git``
    dropped, for telling two repositories apart; the host is ``None`` for an
    SSH alias SRW wrote (``srw-repo-*``), which names no real host."""
    text = str(url or "").strip()
    if not text:
        return None
    if "://" not in text:
        # scp-like: [user@]host:path
        head, sep, path = text.partition(":")
        if not sep:
            return None
        host = head.rsplit("@", 1)[-1]
    else:
        parsed = urlparse(text)
        host = parsed.hostname or ""
        path = parsed.path
    path = path.strip("/").removesuffix(".git").strip("/").lower()
    host = host.lower()
    if not path:
        return None
    return (None if host.startswith("srw-repo-") else host or None), path


def _other_repository(git_mgr: Any, expected_url: str) -> Optional[str]:
    """The repository a reused checkout's origin names when it is not the
    one expected (credentials masked), else ``None``; an origin that cannot
    be read decides nothing."""
    found = git_mgr._run_git(["config", "--get", "remote.origin.url"])
    current = getattr(found, "stdout", None)
    if not isinstance(current, str) or getattr(found, "returncode", None) != 0:
        return None
    have = _repository_identity(current.strip())
    want = _repository_identity(expected_url)
    if have is None or want is None:
        return None
    same_path = have[1] == want[1]
    same_host = have[0] is None or want[0] is None or have[0] == want[0]
    if same_path and same_host:
        return None
    return f"{have[0] or 'an SSH alias'}/{have[1]}"


def _note_skipped(workspace_manager: Any, clone_name: str, reason: str) -> None:
    """Record why a repository was not cloned, for the README."""
    skipped = getattr(workspace_manager, "source_repo_skipped", None)
    if not isinstance(skipped, dict):
        skipped = {}
        try:
            workspace_manager.source_repo_skipped = skipped
        except AttributeError:
            return
    skipped[clone_name] = " ".join(str(reason).split())[:400]


def clone_repository_datasources(
    repo_datasources: List[Dict[str, Any]],
    workspace_manager: Any,
    *,
    ssh_identity_status: Optional[Dict[str, str]] = None,
    legacy_key_files: str = "sweep",
    clone_names: Optional[List[str]] = None,
) -> None:
    """Clone repository datasources onto the workspace backend.

    Every operation runs on the workspace and the clone itself is
    ``GitManager.clone(backend=...)`` (git on the workspace over SSH).

    An SSH-key repository is cloned from ``ssh://srw-repo-<slug>/<path>`` (or
    ``srw-repo-<slug>:<path>`` for a relative scp-style path), an opaque alias
    whose key a dedicated workspace ``ssh-agent`` holds
    (``agent.connectors.ssh_identity``); no key file is written
    and no ``Host`` block is appended. ``ssh_identity_status`` is the
    materializer's ``{authority_id: status}``: a connector whose identity did
    not load is skipped with a warning, never cloned without its key. A
    reused checkout gets its origin reset to the alias. A pre-agent
    ``~/.ssh/repo_<slug>`` key file (one only its exact pre-agent
    ``~/.ssh/config`` block names) is deleted only once its repository is
    proven to work through the alias;
    ``legacy_key_files`` says how far that goes.

    There is deliberately NO agent-local fallback: without a shell-capable
    backend the datasources are skipped with an error. Repository
    datasources require a full workspace — lite tiers reject them at
    dispatch (knowledge-base/knowledge/features/no_workspace_agent_mode.md §4/§7).

    Args:
        repo_datasources: Datasource config dicts of type "repository".
        workspace_manager: WorkspaceManager whose backend hosts the clones;
            successful clones are registered in its ``source_repos``.
        ssh_identity_status: Which SSH identities the workspace agent holds;
            ``None`` when the caller did not materialize any.
        legacy_key_files: ``sweep`` when the workspace owner passes its
            full set (key files no listed repository owns may go too),
            ``own`` for a partial set (a live add), ``keep`` when the
            workspace is someone else's (a child job on its parent's).
    """
    if not isinstance(ssh_identity_status, dict):
        ssh_identity_status = None
    if legacy_key_files not in LEGACY_KEY_FILE_MODES:
        raise ValueError(f"unknown legacy_key_files mode {legacy_key_files!r}")
    if not repo_datasources:
        return

    backend = getattr(workspace_manager, "backend", None)
    if backend is None or not getattr(backend, "supports_shell", False):
        logger.error(
            "Repository datasources require a workspace backend with shell "
            "support; skipping %d repository datasource(s), no local clone "
            "fallback: %s",
            len(repo_datasources),
            ", ".join(ds.get("name", "unnamed") for ds in repo_datasources),
        )
        return

    from agent.managers.git_manager import GitManager

    # The workspace root is itself a durable Git repository. Without this
    # exclusion, its next checkpoint records each nested checkout as a
    # contentless gitlink; a fallback restore then recreates only an empty
    # directory. Keeping the clone root ignored means every attach can either
    # reuse the PVC copy below or re-clone it from the connector.
    try:
        if backend.exists(".gitignore"):
            content = backend.read_file(".gitignore")
            ignored = any(
                line.strip() == "repos/" for line in str(content).splitlines()
            )
            if not ignored:
                separator = "" if str(content).endswith("\n") else "\n"
                backend.append_file(
                    ".gitignore",
                    f"{separator}\n# Cloned repository datasources\nrepos/\n",
                )
        else:
            backend.write_file(
                ".gitignore", "# Cloned repository datasources\nrepos/\n"
            )
    except Exception as exc:
        logger.warning(
            "Could not exclude repository datasource clones from workspace "
            "versioning: %s",
            exc,
        )

    if clone_names is None or len(clone_names) != len(repo_datasources):
        clone_names = resolve_repo_clone_names(repo_datasources)
    swap_wired, swap_reasons = _wire_swap_repositories(
        backend, repo_datasources, clone_names, prune=legacy_key_files == "sweep"
    )
    ssh_outcomes: List[List[Any]] = []
    for index, (ds, repo_name) in enumerate(zip(repo_datasources, clone_names)):
        # ds_name is the safe form of the user-supplied datasource label.
        ds_name = (
            re.sub(r"[^a-z0-9]+", "-", ds.get("name", "repo").lower()).strip("-")
            or "repo"
        )
        try:
            repo_url = ds.get("connection_url", "")
            branch = ds.get("default_branch")
            creds = ds.get("credentials") or {}
            # The checkout entry's auth: an explicit auth_method, or inferred
            # from the delivered SSH identity / credentials keys.
            auth = checkout_auth(ds)

            ssh_clone_url: Optional[str] = None
            ssh_outcome: Optional[List[Any]] = None
            swap: Optional[SwapBinding] = None
            if auth == "swap":
                swap = swap_wired.get(index)
                if swap is None:
                    reason = swap_reasons.get(
                        index, "not bound through the git swap driver"
                    )
                    logger.warning(
                        "Skipping repository datasource %r: %s", ds_name, reason
                    )
                    _note_skipped(workspace_manager, repo_name, reason)
                    continue
                # The clean upstream URL: the wiring sends it to the driver.
                repo_url = swap.clean_url

            elif auth == "ssh_agent":
                ssh_clone_url, reason = ssh_clone_target(ds, ssh_identity_status)
                # A key file a pre-agent clone wrote goes only once this
                # repository is proven through its alias (after the loop).
                ssh_outcome = [ds_name, False]
                ssh_outcomes.append(ssh_outcome)
                if ssh_clone_url is None:
                    logger.warning(
                        "Skipping SSH repository datasource %r: %s",
                        ds_name,
                        reason,
                    )
                    _note_skipped(workspace_manager, repo_name, reason)
                    continue
                repo_url = ssh_clone_url

            elif auth == "token_in_url":
                parsed = urlparse(repo_url)
                repo_url = parsed._replace(
                    netloc=f"oauth2:{creds['token']}@{parsed.hostname}"
                    + (f":{parsed.port}" if parsed.port else "")
                ).geturl()

            target = workspace_manager.path / "repos" / repo_name
            remote_cwd = f"repos/{repo_name}"
            reused = backend.exists(f"{remote_cwd}/.git")
            if reused:
                # A session workspace may outlive its agent pod (PVC hot tier)
                # or be restored from the thread repository. Re-register the
                # managed checkout instead of attempting a second clone into
                # the existing directory, which git correctly refuses.
                git_mgr = GitManager(
                    target,
                    backend=backend,
                    remote_cwd=remote_cwd,
                )
                other = _other_repository(git_mgr, ds.get("connection_url", ""))
                if other is not None:
                    # Another connector's checkout (a live add whose name
                    # collides, an order that changed): re-pointing it would
                    # send that checkout's commits to this repository.
                    reason = (
                        f"`./repos/{repo_name}/` is the checkout of another "
                        f"repository ({other}); remove or rename it to clone this one"
                    )
                    logger.warning(
                        "Skipping repository datasource %r: %s", ds_name, reason
                    )
                    _note_skipped(workspace_manager, repo_name, reason)
                    continue
                if ssh_clone_url is not None:
                    if git_mgr.add_remote("origin", ssh_clone_url):
                        # Proven lazily: ls-remote only if a key file is left.
                        ssh_outcome[1] = git_mgr
                    else:
                        # A pre-agent checkout points at the real host and
                        # its key file; leaving it would fail every fetch.
                        logger.warning(
                            "Could not point reused repos/%s at its SSH identity",
                            repo_name,
                        )
                if swap is not None:
                    # A checkout an earlier token-in-URL clone left carries
                    # oauth2:<token>@ in its origin: reset to the clean URL,
                    # or leave the checkout unused.
                    unsafe = _secure_swap_checkout(git_mgr, swap, reused=True)
                    if unsafe is not None:
                        logger.warning(
                            "Skipping repository datasource %r: %s", ds_name, unsafe
                        )
                        _note_skipped(workspace_manager, repo_name, unsafe)
                        continue
                if (
                    auth == "token_in_url"
                    and legacy_key_files != "keep"
                    and (_falls_back(ds) or _swap_era(git_mgr))
                ):
                    # The driver served this checkout in an earlier attach:
                    # its remote is the clean URL and it refuses credentials
                    # in URLs. On the fallback (or with the driver turned
                    # off since: no entry says anything about it) it clones
                    # as before C3 did, with the token in its remote URL.
                    unusable = _fallback_checkout(git_mgr, repo_url)
                    if unusable is not None:
                        logger.warning(
                            "Skipping repository datasource %r: %s", ds_name, unusable
                        )
                        _note_skipped(workspace_manager, repo_name, unusable)
                        continue
                logger.info(
                    "Reusing repository datasource %r from repos/%s",
                    ds_name,
                    repo_name,
                )
            else:
                if swap is not None:
                    # A new binding's pod may still be starting.
                    unserved = wait_for_driver(backend, swap)
                    if unserved is not None:
                        logger.warning(
                            "Skipping repository datasource %r: %s",
                            ds_name,
                            unserved,
                        )
                        _note_skipped(workspace_manager, repo_name, unserved)
                        continue
                git_mgr = GitManager.clone(
                    repo_url,
                    target,
                    backend=backend,
                    remote_cwd=remote_cwd,
                    # The checkout does not exist yet, so its gitdir-scoped
                    # rules do not apply to the clone: name them.
                    **(
                        {"config": binding_options(backend, swap)}
                        if swap is not None
                        else {}
                    ),
                )
                if ssh_outcome is not None and git_mgr:
                    ssh_outcome[1] = True
                if swap is not None and git_mgr:
                    _secure_swap_checkout(git_mgr, swap, reused=False)
            if git_mgr:
                branch_ready = True
                if branch and (not reused or ds.get("require_default_branch")):
                    branch_ready = git_mgr.checkout_branch(branch)
                elif branch:
                    # Reused checkout without require_default_branch: the
                    # worker may have moved HEAD (e.g. onto a job branch)
                    # before this re-attach; re-running checkout here silently
                    # reverted that on every resume (job 12a0e92c). Only a
                    # review session pins the branch on reuse — its entire
                    # point is that this exact delivery is checked out
                    # (orchestrator/services/job_delivery.py sets
                    # require_default_branch).
                    logger.debug(
                        "Reused repos/%s keeps its checked-out branch %r "
                        "(pinned default %r not re-applied on re-attach)",
                        repo_name,
                        git_mgr.current_branch(),
                        branch,
                    )
                if ds.get("require_default_branch") and not branch_ready:
                    logger.error(
                        "Repository datasource %r could not check out required "
                        "branch %r; refusing to register the review source",
                        ds_name,
                        branch,
                    )
                    continue
                workspace_manager.source_repos[repo_name] = git_mgr
                skipped = getattr(workspace_manager, "source_repo_skipped", None)
                if isinstance(skipped, dict):
                    skipped.pop(repo_name, None)
                try:
                    from shared.runtime.services.forge import (
                        parse_owner_repo,
                        resolve_api_base,
                    )

                    forge = str((ds.get("config") or {}).get("forge") or "").lower()
                    raw_url = ds.get("connection_url", "")
                    owner, repo_slug = parse_owner_repo(raw_url)
                    repo_meta = {
                        "forge": forge,
                        "api_base": resolve_api_base(raw_url, forge),
                        "owner": owner,
                        "repo": repo_slug,
                        "token": (creds or {}).get("token", ""),
                        # The agent payload carries the project link flag as
                        # `project_read_only`; `read_only` is the publisher's
                        # declared flag on public datasources. Either one
                        # forbids writes, and reading only the latter made
                        # every read-only repository record read_only=False.
                        "read_only": bool(
                            ds.get("project_read_only") or ds.get("read_only")
                        ),
                        "default_branch": branch,
                    }
                    # Current orchestrators deliberately omit the raw DB
                    # ``id`` and send the resolved, server-owned identity as
                    # ``datasource_id``.  The ``id`` fallback is retained only
                    # for older in-process/internal callers that passed a
                    # resolved row directly to this shared clone helper.
                    datasource_id = str(
                        ds.get("datasource_id") or ds.get("id") or ""
                    ).strip()
                    if datasource_id:
                        repo_meta["datasource_id"] = datasource_id
                    workspace_manager.source_repo_meta[repo_name] = repo_meta
                except Exception as e:
                    # A metadata failure must not fail the clone; the repo is
                    # still usable through the shell and the read-only git tools.
                    logger.warning(
                        "Could not record forge metadata for repos/%s: %s",
                        repo_name,
                        e,
                    )
                logger.info(
                    "Cloned repository datasource %r into repos/%s",
                    ds_name,
                    repo_name,
                )
            else:
                logger.warning(
                    "Failed to clone repository datasource %r (target repos/%s)",
                    ds_name,
                    repo_name,
                )
                _note_skipped(workspace_manager, repo_name, "the clone failed")
        except Exception as e:
            logger.warning(
                "Failed to clone repository datasource %s: %s",
                ds.get("name", "unnamed"),
                e,
            )
            _note_skipped(
                workspace_manager, repo_name, f"the clone failed ({type(e).__name__})"
            )

    if ssh_outcomes:
        _retire_legacy_ssh_key_files(
            backend,
            [(name, outcome) for name, outcome in ssh_outcomes],
            mode=legacy_key_files,
        )


def _skip_reason(workspace_manager: Any, clone_name: str) -> Optional[str]:
    """Why the last clone call left a repository uncloned, if it did."""
    skipped = getattr(workspace_manager, "source_repo_skipped", None)
    if not isinstance(skipped, dict):
        return None
    reason = skipped.get(clone_name)
    return reason if isinstance(reason, str) and reason else None


def _repo_meta(workspace_manager: Any, clone_name: str) -> Dict[str, Any]:
    """Forge metadata recorded by clone_repository_datasources(), or {}."""
    meta = getattr(workspace_manager, "source_repo_meta", None)
    if not isinstance(meta, dict):
        return {}
    entry = meta.get(clone_name)
    return entry if isinstance(entry, dict) else {}


def _key(delivery: Delivery) -> str:
    # The internal payload strips datasource ids, so a live change identifies
    # a connector by (type, name), as the transcript summary does.
    return f"{delivery.entry.get('type')}:{delivery.entry.get('name')}"


class CheckoutMaterializer:
    form = "checkout"

    def materialize(self, deliveries: Sequence[Delivery], rt: RuntimeContext) -> None:
        if not deliveries:
            return
        if rt.workspace_manager is None and rt.execution == "session":
            # A session without a workspace has nothing to clone into (lite
            # tiers refuse repositories at dispatch); a worker says so loudly.
            logger.debug("No session workspace; repository checkouts skipped")
            return
        clone_repository_datasources(
            [delivery.entry for delivery in deliveries],
            rt.workspace_manager,
            ssh_identity_status=rt.ssh_identity_status,
            legacy_key_files=rt.legacy_key_files,
        )

    def replace(
        self,
        old: Sequence[Delivery],
        new: Sequence[Delivery],
        rt: RuntimeContext,
    ) -> None:
        """Clone added repositories; unregister removed ones.

        A removed repository keeps its clone on the workspace (cheap honesty:
        scrubbing is not a security boundary) but loses its ``source_repos``
        registration and its forge metadata, which holds its token, and a
        git swap binding loses its wiring (its lease was revoked with the
        detach). An added repository's clone name is resolved over the full
        new list: the name a resume resolves and the README lists. A
        checkout already there whose origin is another repository (an order
        that changed) is never re-pointed: that one repository is skipped
        and the README says why.
        """
        old_keys = {_key(delivery) for delivery in old}
        new_keys = {_key(delivery) for delivery in new}
        added = [delivery for delivery in new if _key(delivery) not in old_keys]
        removed = old_keys - new_keys
        workspace_manager = rt.workspace_manager
        if added and workspace_manager:
            names = resolve_repo_clone_names([delivery.entry for delivery in new])
            named = {
                id(delivery): name for delivery, name in zip(new, names, strict=True)
            }
            try:
                clone_repository_datasources(
                    [delivery.entry for delivery in added],
                    workspace_manager,
                    ssh_identity_status=rt.ssh_identity_status,
                    # Only the added ones: another repository's key file is
                    # not this batch's to sweep.
                    legacy_key_files="own",
                    clone_names=[named[id(delivery)] for delivery in added],
                )
            except Exception as e:
                logger.warning("Live repository clone failed: %s", e)
        if removed and workspace_manager:
            # Resolve clone names over the OLD full repo list (payload order)
            # so collision suffixes match what attach actually registered.
            old_repos = [delivery.entry for delivery in old]
            for delivery, clone_name in zip(old, resolve_repo_clone_names(old_repos)):
                if _key(delivery) in removed:
                    workspace_manager.source_repos.pop(clone_name, None)
                    # source_repo_meta holds the repository's plaintext token;
                    # leaving it behind keeps a detached credential live on
                    # the workspace manager for the rest of the session.
                    workspace_manager.source_repo_meta.pop(clone_name, None)
            # A detached swap repository's wiring goes too (housekeeping: its
            # lease is already revoked). The clean remote stays.
            detached = [
                binding.connector_id
                for delivery in old
                if _key(delivery) in removed
                for binding in (swap_binding(delivery.entry)[0],)
                if binding is not None
            ]
            backend = rt.workspace_backend
            if detached and backend is not None:
                try:
                    install_wiring(backend, (), remove=detached)
                except Exception as e:
                    logger.warning(
                        "Could not remove a detached repository's git swap wiring: %s",
                        type(e).__name__,
                    )

    def on_backend_swap(self, deliveries: Sequence[Delivery], backend: Any) -> None:
        """The git swap wiring lives under ``~/.srw-credentials``, which no
        snapshot carries: write it again on the new workspace (the clones
        and their clean remotes moved with it), each scoped to its checkout
        on the new workspace."""
        entries = [delivery.entry for delivery in deliveries]
        bindings = []
        checkouts: Dict[str, str] = {}
        for entry, clone_name in zip(entries, resolve_repo_clone_names(entries)):
            binding = swap_binding(entry)[0]
            if binding is not None:
                bindings.append(binding)
                checkouts[binding.connector_id] = clone_name
        if not bindings:
            return
        try:
            install_wiring(backend, bindings, checkouts=checkouts)
        except Exception as e:
            logger.warning(
                "Could not write the git swap wiring on the new workspace: %s",
                type(e).__name__,
            )

    def facts(
        self, deliveries: Sequence[Delivery], rt: RuntimeContext
    ) -> list[FactsLines]:
        repos = [delivery.entry for delivery in deliveries]
        out: list[FactsLines] = []
        # Same name resolution as clone_repository_datasources() — the list
        # must point at the directories the clones actually land in.
        for delivery, clone_name in zip(deliveries, resolve_repo_clone_names(repos)):
            ds = delivery.entry
            meta = _repo_meta(rt.workspace_manager, clone_name)
            default_branch = str(
                meta.get("default_branch") or ds.get("default_branch") or ""
            ).strip()
            branch_clause = (
                f"; base branch `{default_branch}`" if default_branch else ""
            )
            read_only = bool(
                meta.get("read_only")
                if "read_only" in meta
                else (ds.get("project_read_only") or ds.get("read_only"))
            )
            access = (
                "read-only — only repo_pull/repo_pr_status"
                if read_only
                else "writable — pull requests opened with repo_open_pr are "
                "recorded for this job"
            )
            skipped = _skip_reason(rt.workspace_manager, clone_name)
            if skipped is not None:
                block = ds.get("git_swap")
                line = (
                    f"- **{ds.get('name')}** — repository NOT cloned "
                    f"(`./repos/{clone_name}/` is not usable): {skipped}"
                    # How it was to be reached says why it may have failed:
                    # the SSH alias (and its key's state) or the swap route.
                    + ssh_identity_note(ds, rt.ssh_identity_status)
                    + (
                        swap_note(ds)
                        if isinstance(block, dict) and "fallback" in block
                        else ""
                    )
                )
                out.append(FactsLines("Repositories", delivery.index, [line]))
                continue
            line = (
                f"- **{ds.get('name')}** — repository cloned at "
                f'`./repos/{clone_name}/` (use `repo="{clone_name}"` with the '
                f"repo_* tools){branch_clause}; {access}{declared_read_only_note(ds)}"
                + ssh_identity_note(ds, rt.ssh_identity_status)
                + swap_note(ds)
            )
            out.append(FactsLines("Repositories", delivery.index, [line]))
        return out
