"""Where a credential file may land in the workspace home (an allowlist).

A kubeconfig or generic-file connector writes files into the home of the
workspace the agent's shell runs in, and a connector may be shared: its
owner is not the person whose agent uses it. So a target is a place that
tools read *data* from, never a place whose files the shell, git, an editor,
Python or the session manager execute or source, and never SRW's own
namespaces. Anything not listed here is refused; the orchestrator refuses it
when the connector is saved (with this module's reason) and the agent skips
it at delivery, for rows saved before the rule.

Allowed (home-relative):

* ``.kube/``, ``.aws/``, ``.azure/``, ``.docker/`` — the cloud and cluster
  CLIs' credential directories, except the two subtrees those CLIs load code
  from: ``.docker/cli-plugins/`` (Docker CLI plugins) and
  ``.azure/cliextensions/`` (Python packages the Azure CLI imports);
* ``.config/<app>/...`` — an application's own config directory (``gcloud``,
  ``gh``, ``rclone``...), except the applications in
  :data:`REFUSED_CONFIG_APPS`, whose files are executed or sourced. A file
  directly in ``.config/`` is refused (``mimeapps.list``, ``user-dirs.dirs``
  and the like steer what the session runs);
* ``.netrc`` and ``.pgpass`` — read by curl, git, psql and libpq for logins;
* ``.srw-files/`` — a neutral directory, the default for a generic file
  without a target.

Everything in ``.ssh/`` is refused: SRW's managed repositories and SSH
identities own ``~/.ssh/config`` and their namespace, and an ssh config can
run commands (``ProxyCommand``). So are the shell start-up files, ``bin``
directories and Python's site directories, ``~/workspace`` (cloud sync
uploads what is there), and every other path.

What a file says is the connector's business: a kubeconfig ``exec`` plugin or
an AWS ``credential_process`` runs a command when the CLI is used, which is
how those formats work. A credential file never carries an execute bit.
"""

from __future__ import annotations

import posixpath

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
#: ``.config/<app>/`` directories whose files are executed or sourced:
#: shells and their rc files (bash, zsh, fish, nushell, xonsh, elvish,
#: powershell), git (hooks, ``core.fsmonitor``, ``core.sshCommand``,
#: aliases), the session manager (systemd user units, ``environment.d``,
#: XDG ``autostart``), editors and IDEs that run their init files or tasks
#: (vim, nvim, emacs, Code, VSCodium, code-server — also SRW's IDE config),
#: tmux (SRW's shell runs in it), direnv (``direnvrc`` is sourced), starship
#: (custom prompt commands), IPython and Jupyter (startup and config files
#: are Python), browser profiles (Chromium, Chrome: extensions run code) and
#: SRW's own ``srw``.
REFUSED_CONFIG_APPS: frozenset[str] = frozenset(
    {
        "autostart",
        "bash",
        "chromium",
        "code-server",
        "Code",
        "direnv",
        "elvish",
        "emacs",
        "environment.d",
        "fish",
        "git",
        "google-chrome",
        "ipython",
        "jupyter",
        "nushell",
        "nvim",
        "powershell",
        "srw",
        "starship",
        "systemd",
        "tmux",
        "vim",
        "VSCodium",
        "xonsh",
        "zsh",
    }
)
#: Where a generic file without a target goes: ``<dir>/<connector slug>/``.
DEFAULT_DIRECTORY = ".srw-files"

OUTSIDE_HOME = "outside the home"
NOT_ALLOWED = "not a credential-file location"


def allowed_targets_text() -> str:
    """The allowlist as the refusal message names it."""
    return (
        ", ".join(f"~/{name}/" for name in ALLOWED_DIRECTORIES)
        + ", ~/.config/<app>/, "
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
    if relative in ALLOWED_FILES:
        return None
    if any(
        _under(relative, subtree) or relative == subtree for subtree in REFUSED_SUBTREES
    ):
        return NOT_ALLOWED
    if any(_under(relative, directory) for directory in ALLOWED_DIRECTORIES):
        return None
    parts = relative.split("/")
    if (
        len(parts) >= 3
        and parts[0] == ".config"
        and parts[1] not in REFUSED_CONFIG_APPS
        and parts[1] not in ("", ".", "..")
    ):
        return None
    return NOT_ALLOWED


def target_problem(path: str) -> tuple[str | None, str | None]:
    """``(home-relative target, None)``, or ``(None, why it is refused)``."""
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
