"""Where a credential file may land in the workspace home (an allowlist).

A kubeconfig or generic-file connector, and a registered image driver's
bind, write files into the home of the workspace the agent's shell runs in,
and a connector may be shared: its owner is not the person whose agent uses
it. This module decides *where* a file may land, for every connector alike
(connector drivers decision 26; the variable a file sets follows
:mod:`.env_names`): places tools read credentials from, never a place whose
files the shell, git, Python, an editor or the session manager execute,
source or import by path, and never SRW's own namespaces. Anything not
listed here is refused; the orchestrator refuses it when the connector is
saved or a driver's binding is checked (with this module's reason) and the
agent skips it at delivery, for rows saved before the rule.

It does not decide what a file *says*. Several allowed formats run a command
by design when their CLI is used: a kubeconfig ``exec`` credential plugin, an
AWS ``credential_process``, a Docker or Helm registry ``credsStore`` /
``credHelpers`` (a ``docker-credential-<name>`` program on ``PATH``), an Azure
CLI ``extension.dev_sources`` directory. Whether a shared connector may carry
such content is a trust decision about the connector, not a location rule.
What this module does guarantee: no file lands executable, on ``PATH``, in a
start-up or plugin directory, or where another tool's config would make it
code.

Allowed (home-relative):

* ``.kube/``, ``.aws/``, ``.azure/``, ``.docker/`` — the cloud and cluster
  CLIs' credential directories, except the two subtrees those CLIs load code
  from: ``.docker/cli-plugins/`` (Docker CLI plugins) and
  ``.azure/cliextensions/`` (Python packages the Azure CLI imports);
* ``.config/<app>/...`` for the applications in :data:`ALLOWED_CONFIG_APPS`
  only — an explicit list, each checked for a config key that runs a command:

  - ``gcloud``: credentials, access tokens and named configurations; an
    ``external_account`` credential's ``executable`` source runs only when
    ``GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1`` is set;
  - ``helm``: ``repositories.yaml`` (repository URLs and logins) and
    ``registry/config.json``; Helm's plugins live in ``~/.local/share``;
  - ``doctl``: ``config.yaml`` (access tokens and contexts);
  - ``hcloud``: ``cli.toml`` (tokens and contexts);
  - ``sops``: ``age/keys.txt`` (age identities; ``SOPS_AGE_KEY_CMD`` is an
    environment variable, never read from this file).

  Left out on purpose: ``gh`` (an alias starting with ``!`` runs a shell
  command), ``rclone`` (the sftp backend's ``ssh`` and the webdav backend's
  ``bearer_token_command`` run commands), ``pip`` and ``uv`` (an index URL is
  package code at the next install), ``pnpm`` (``node-options`` with
  ``--require``), ``containers`` (systemd quadlets run ``ExecStartPre``),
  ``user-tmpfiles.d`` (writes any file in the home), ``mypy`` and
  ``python_keyring`` (import a module by path), and every shell, editor,
  git, systemd or session directory. A file directly in ``.config/`` is
  refused too (``mimeapps.list``, ``user-dirs.dirs`` steer what runs);
* ``.netrc`` and ``.pgpass`` — read by curl, git, psql and libpq for logins;
* ``.srw-files/`` — a neutral directory, the default for a generic file
  without a target.

Everything in ``.ssh/`` is refused: SRW's managed repositories and SSH
identities own ``~/.ssh/config`` and their namespace, and an ssh config can
run commands (``ProxyCommand``). So are the shell start-up files, ``bin``
directories and Python's site directories, ``~/workspace`` (cloud sync
uploads what is there), and every other path. A target holds only letters,
digits and ``._/ -`` (no control characters reach a log or the README), and
a credential file never carries an execute bit.
"""

from __future__ import annotations

import posixpath
import re

#: The home the orchestrator resolves ``~`` against when it stores a target.
STORED_HOME = "/home/srw"

#: Directories any file under which may be a target.
ALLOWED_DIRECTORIES: tuple[str, ...] = (
    ".kube",
    ".aws",
    ".azure",
    ".docker",
    ".srw-files",
)
#: Single files that may be a target.
ALLOWED_FILES: tuple[str, ...] = (".netrc", ".pgpass")
#: Subtrees of an allowed directory that a CLI loads code from.
REFUSED_SUBTREES: tuple[str, ...] = (".docker/cli-plugins", ".azure/cliextensions")
#: ``.config/<app>/`` directories a target may be in (see the module
#: docstring for why each, and why the others are out).
ALLOWED_CONFIG_APPS: frozenset[str] = frozenset(
    {"doctl", "gcloud", "hcloud", "helm", "sops"}
)
#: Where a generic file without a target goes: ``<dir>/<slug>-<token>/``.
DEFAULT_DIRECTORY = ".srw-files"
#: The characters a target may hold.
_TARGET_CHARACTERS = re.compile(r"[A-Za-z0-9._/ -]+\Z")

OUTSIDE_HOME = "outside the home"
NOT_ALLOWED = "not a credential-file location"
BAD_CHARACTERS = "has a character other than letters, digits and ._/ -"


def allowed_targets_text() -> str:
    """The allowlist as the refusal message names it."""
    return (
        ", ".join(f"~/{name}/" for name in ALLOWED_DIRECTORIES)
        + ", "
        + ", ".join(f"~/.config/{app}/" for app in sorted(ALLOWED_CONFIG_APPS))
        + ", "
        + ", ".join(f"~/{name}" for name in ALLOWED_FILES)
    )


def home_relative(path: str) -> str | None:
    """``path`` (``~/...`` or under :data:`STORED_HOME`) relative to the home.

    ``None`` when it is not under the home, or is the home itself.
    """
    if not isinstance(path, str):
        return None
    if path.startswith("~/"):
        relative = path[2:]
    elif path.startswith(STORED_HOME + "/"):
        relative = path[len(STORED_HOME) + 1 :]
    else:
        return None
    relative = posixpath.normpath(relative)
    if relative in ("", ".") or relative == ".." or relative.startswith("../"):
        return None
    return relative


def _under(relative: str, directory: str) -> bool:
    return relative.startswith(directory + "/")


def relative_target_problem(relative: str) -> str | None:
    """Why a home-relative target is refused, or ``None`` if it is allowed."""
    if not _TARGET_CHARACTERS.fullmatch(relative):
        return BAD_CHARACTERS
    if relative in ALLOWED_FILES:
        return None
    if any(
        _under(relative, subtree) or relative == subtree for subtree in REFUSED_SUBTREES
    ):
        return NOT_ALLOWED
    if any(_under(relative, directory) for directory in ALLOWED_DIRECTORIES):
        return None
    parts = relative.split("/")
    if len(parts) >= 3 and parts[0] == ".config" and parts[1] in ALLOWED_CONFIG_APPS:
        return None
    return NOT_ALLOWED


def target_problem(path: str) -> tuple[str | None, str | None]:
    """``(home-relative target, None)``, or ``(None, why it is refused)``."""
    if isinstance(path, str) and not _TARGET_CHARACTERS.fullmatch(
        path.replace("~", "", 1) or "x"
    ):
        return None, BAD_CHARACTERS
    relative = home_relative(path)
    if relative is None:
        return None, OUTSIDE_HOME
    problem = relative_target_problem(relative)
    if problem is not None:
        return None, problem
    return relative, None


def mode_problem(mode: int) -> str | None:
    """Why a file mode is refused: a credential file is never executable."""
    if mode & 0o111:
        return "a credential file is never executable"
    if mode & ~0o777:
        return "no setuid, setgid or sticky bit"
    return None


def safe_mode(mode: int) -> int:
    """The mode a credential file is written with."""
    return mode & 0o666
