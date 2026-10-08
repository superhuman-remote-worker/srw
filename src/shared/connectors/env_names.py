"""Environment variable names a connector may not set in a workspace.

Three layers, from the loosest:

* **The workspace's own names** (:func:`workspace_name_problem`): what every
  environment connector has always been refused
  (``shared.credential_connectors.normalize_credential_env``): the shell's
  and SRW's variables, the dynamic loader's and Python's.
* **Variables that point a tool at a file** (:func:`points_a_tool_at_code`):
  a config, start-up or code file git, a shell, an editor, a pager, a
  language runtime or a package manager reads. A credential file's
  ``env_var`` names its stored file, so it may name none of these (slice
  D1d).
* **What a driver may set** (:func:`driver_env_problem`): the strict list
  for variables a *driver* puts in a workspace's environment, a registered
  image driver's bind (D6) or a managed MCP server's stdio template. A
  driver may be someone else's image and its connector may be shared, so
  nothing it names may run code in the consumer's workspace, redirect its
  traffic or loosen its TLS: the two layers above, plus the whole ``GIT_*``
  and ``SSH_*`` families, every ``*_PROXY`` in any case, the CA bundles,
  ``*ASKPASS``, ``*PAGER`` and ``*EDITOR``, the package managers' option
  families and the runtimes' option variables.

Environment connectors keep the first layer only: whether they adopt the
strict list is the owner's decision. Standard library only.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

#: What an environment variable name may look like.
ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
#: The longest value one variable may hold.
MAX_ENV_VALUE_BYTES = 65536

# ---------------------------------------------------------------------------
# The workspace's own names
# ---------------------------------------------------------------------------

#: Names the workspace sets for itself.
WORKSPACE_RESERVED_NAMES: frozenset[str] = frozenset(
    {
        "PATH",
        "HOME",
        "SHELL",
        "USER",
        "LOGNAME",
        "ENV",
        "BASH_ENV",
        "IFS",
        "SHELLOPTS",
        "BASHOPTS",
        "CDPATH",
        "PROMPT_COMMAND",
        "TMUX",
        "TMUX_PANE",
    }
)
#: Families the workspace reserves (case-sensitive, as they always were).
WORKSPACE_RESERVED_PREFIXES: tuple[str, ...] = ("SRW_", "LD_", "DYLD_", "PYTHON")


def workspace_name_problem(name: object) -> str | None:
    """Why ``name`` is refused as any connector's variable (``None``: it
    is not). The messages are an environment connector's own."""
    if not isinstance(name, str) or not ENV_NAME.fullmatch(name):
        return (
            "Environment names must contain letters, digits or underscores "
            "and cannot start with a digit"
        )
    if name in WORKSPACE_RESERVED_NAMES or name.startswith(WORKSPACE_RESERVED_PREFIXES):
        return f"Environment name {name} is reserved by the workspace"
    return None


# ---------------------------------------------------------------------------
# Variables that point a tool at a config, start-up or code file
# ---------------------------------------------------------------------------
#
# Naming a variable below would make a file a config git, pip, npm, a shell,
# an editor, a pager or the dynamic loader reads, or code Python, Node, Perl,
# Ruby or the JVM loads.
CONFIG_POINTER_ENV_NAMES: frozenset[str] = frozenset(
    {
        # git
        "GIT_CONFIG",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_SYSTEM",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_PARAMETERS",
        "GIT_EXEC_PATH",
        "GIT_TEMPLATE_DIR",
        "GIT_EXTERNAL_DIFF",
        "GIT_EDITOR",
        "GIT_SEQUENCE_EDITOR",
        "GIT_SSH",
        "GIT_SSH_COMMAND",
        "GIT_ASKPASS",
        "GIT_PAGER",
        "GIT_PROXY_COMMAND",
        # ssh and sudo prompts
        "SSH_ASKPASS",
        "SUDO_ASKPASS",
        # shells and readline
        "BASH_ENV",
        "ENV",
        "ZDOTDIR",
        "INPUTRC",
        "PS0",
        "PS1",
        "PS2",
        "PS3",
        "PS4",
        "PROMPT_COMMAND",
        "HISTFILE",
        # editors, pagers, browsers
        "EDITOR",
        "VISUAL",
        "PAGER",
        "MANPAGER",
        "LESSOPEN",
        "LESSCLOSE",
        "LESSKEY",
        "BROWSER",
        # language runtimes, package managers and their configs
        "PSQLRC",
        "PYTHONSTARTUP",
        "PYTHONPATH",
        "PYTHONHOME",
        "PYTHONUSERBASE",
        "NODE_OPTIONS",
        "NODE_PATH",
        "PERL5LIB",
        "PERL5OPT",
        "PERLLIB",
        "RUBYLIB",
        "RUBYOPT",
        "JAVA_TOOL_OPTIONS",
        "JDK_JAVA_OPTIONS",
        "_JAVA_OPTIONS",
        "CLASSPATH",
        "CARGO_HOME",
        "RUSTC_WRAPPER",
        "GOENV",
        "GOFLAGS",
        "CURL_HOME",
        "WGETRC",
        "DOCKER_CONFIG",
        "HELM_PLUGINS",
        # the dynamic loader
        "LD_PRELOAD",
        "LD_LIBRARY_PATH",
        "LD_AUDIT",
        # where every config is looked for
        "XDG_CONFIG_HOME",
        "XDG_CONFIG_DIRS",
        "XDG_DATA_HOME",
        "XDG_DATA_DIRS",
    }
)
#: Families whose every member points a tool at a config or changes how it
#: runs: ``GIT_CONFIG_KEY_<n>``/``GIT_CONFIG_VALUE_<n>``, any pip, uv or npm
#: option (``npm_config_*`` is read in either case), the loader's and
#: Python's own variables.
CONFIG_POINTER_ENV_PREFIXES: tuple[str, ...] = (
    "GIT_CONFIG_",
    "PIP_",
    "UV_",
    "NPM_CONFIG_",
    "LD_",
    "DYLD_",
    "PYTHON",
)


def points_a_tool_at_code(name: str) -> bool:
    """Whether ``name`` makes a file a config, start-up or code file."""
    upper = name.upper()
    return upper in CONFIG_POINTER_ENV_NAMES or upper.startswith(
        CONFIG_POINTER_ENV_PREFIXES
    )


# ---------------------------------------------------------------------------
# What a driver may set
# ---------------------------------------------------------------------------
#
# Compared in upper case: a tool that reads ``https_proxy`` reads it in
# lower case, and a driver must not slip one past in either.
DRIVER_DENIED_NAMES: frozenset[str] = frozenset(
    {
        # A config file or directory a tool reads, which can run a command.
        "KUBECONFIG",
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_CERT_PATH",
        "DOCKER_TLS_VERIFY",
        "AWS_CONFIG_FILE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "CLOUDSDK_CONFIG",
        "AZURE_CONFIG_DIR",
        "GNUPGHOME",
        "GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES",
        # Where temporary and history files go.
        "TMPDIR",
        "TMP",
        "TEMP",
        "HISTFILE",
        # Editors, start-up files and loaders of more runtimes.
        "VIMINIT",
        "EXINIT",
        "MYVIMRC",
        "VIMRUNTIME",
        "EMACSLOADPATH",
        "FPATH",
        "PHPRC",
        "PHP_INI_SCAN_DIR",
        "R_PROFILE",
        "R_PROFILE_USER",
        "R_ENVIRON",
        "R_ENVIRON_USER",
        "R_LIBS",
        "R_LIBS_USER",
        "TCLLIBPATH",
        "GCONV_PATH",
        "GLIBC_TUNABLES",
        "OPENSSL_CONF",
        "OPENSSL_MODULES",
        "OPENSSL_ENGINES",
        "MAKEFILES",
        "MAKEFLAGS",
        "CC",
        "CXX",
        "CPP",
        "LD",
        "AR",
        "RUSTC",
        "RUSTDOC",
        "RUSTFLAGS",
        "RUSTDOCFLAGS",
        # Package sources and toolchains (the next install is their code).
        "GOPROXY",
        "GONOPROXY",
        "GOPRIVATE",
        "GOSUMDB",
        "GONOSUMDB",
        "GONOSUMCHECK",
        "GOINSECURE",
        "GOTOOLCHAIN",
        # CA bundles and TLS checks: whoever names one can read the traffic.
        "REQUESTS_CA_BUNDLE",
        "CURL_CA_BUNDLE",
        "NODE_EXTRA_CA_CERTS",
        "NODE_TLS_REJECT_UNAUTHORIZED",
        "AWS_CA_BUNDLE",
        "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH",
        "HTTPLIB2_CA_CERTS",
        "NIX_SSL_CERT_FILE",
        "HOSTALIASES",
        "RES_OPTIONS",
        "LOCALDOMAIN",
    }
)
#: Whole families: git's and ssh's own, the CA and TLS variables of OpenSSL
#: (``SSL_CERT_FILE``, ``SSL_CERT_DIR``, ``SSLKEYLOGFILE``), every runtime's
#: and package manager's options, the XDG directories.
DRIVER_DENIED_PREFIXES: tuple[str, ...] = (
    "GIT_",
    "SSH_",
    "SSL",
    "XDG_",
    "NPM_CONFIG_",
    "PIP_",
    "UV_",
    "PERL5",
    "PERL_",
    "RUBY",
    "GEM_",
    "BUNDLE_",
    "DOTNET_",
    "BUN_",
    "CARGO_",
    "RUSTUP_",
    "LUA_",
    "JAVA_TOOL",
    "JDK_JAVA",
    "_JAVA",
)
#: Suffixes that name a proxy, a password prompt, a pager or an editor in
#: any tool (``HTTPS_PROXY``, ``GH_PAGER``, ``KUBE_EDITOR``...).
DRIVER_DENIED_SUFFIXES: tuple[str, ...] = ("_PROXY", "ASKPASS", "PAGER", "EDITOR")


def driver_env_problem(name: object) -> str | None:
    """Why a driver may not set ``name`` in a workspace (``None``: it may)."""
    problem = workspace_name_problem(name)
    if problem is not None:
        return problem
    assert isinstance(name, str)
    upper = name.upper()
    if (
        points_a_tool_at_code(name)
        or upper in WORKSPACE_RESERVED_NAMES
        or upper.startswith(WORKSPACE_RESERVED_PREFIXES)
        or upper in DRIVER_DENIED_NAMES
        or upper.startswith(DRIVER_DENIED_PREFIXES)
        or upper.endswith(DRIVER_DENIED_SUFFIXES)
    ):
        return (
            f"{name} is not a variable a driver may set: it can run code, "
            "redirect traffic or loosen TLS in the workspace"
        )
    return None


def driver_env_problems(names: Iterable[object]) -> list[str]:
    """:func:`driver_env_problem` for each name, in order."""
    return [problem for name in names if (problem := driver_env_problem(name))]


def env_value_problem(name: str, value: object) -> str | None:
    """Why ``value`` cannot be variable ``name``'s value (never shows it)."""
    if not isinstance(value, str) or "\x00" in value:
        return f"Environment variable {name} must be a string without NUL bytes"
    if len(value.encode("utf-8")) > MAX_ENV_VALUE_BYTES:
        return f"Environment variable {name} exceeds 64 KiB"
    return None


__all__ = [
    "CONFIG_POINTER_ENV_NAMES",
    "CONFIG_POINTER_ENV_PREFIXES",
    "DRIVER_DENIED_NAMES",
    "DRIVER_DENIED_PREFIXES",
    "DRIVER_DENIED_SUFFIXES",
    "ENV_NAME",
    "MAX_ENV_VALUE_BYTES",
    "WORKSPACE_RESERVED_NAMES",
    "WORKSPACE_RESERVED_PREFIXES",
    "driver_env_problem",
    "driver_env_problems",
    "env_value_problem",
    "points_a_tool_at_code",
    "workspace_name_problem",
]
