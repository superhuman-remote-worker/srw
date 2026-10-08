"""Environment variable names a connector may not set in a workspace.

The ONE list of variable names SRW keeps from connectors and drivers, in
three layers, from the loosest:

* **The workspace's own names** (:func:`workspace_name_problem`): what every
  environment connector has always been refused
  (``shared.credential_connectors.normalize_credential_env``): the shell's
  and SRW's variables, the dynamic loader's and Python's.
* **Variables that point a tool at a file** (:func:`points_a_tool_at_code`):
  a config, start-up or code file git, a shell, an editor, a pager, a
  language runtime or a package manager reads. A credential file's
  ``env_var`` names its stored file, so it may name none of these (slice
  D1d).
* **What a driver may not set** (:func:`driver_env_problem`,
  :func:`loads_code`): for variables a registered image driver's bind puts
  in a *workspace* (D6): the two layers above, the runtime hooks the MCP
  bridge refuses on its own (:data:`CODE_ENV`), the whole ``GIT_*`` and
  ``SSH_*`` families, every ``*_PROXY`` in any case, the CA bundles, the rc
  files, and the option, flag, command, config and home variables of the
  common runtimes, build tools and package managers. Within a prefix
  family a credential-shaped name (``NODE_AUTH_TOKEN``,
  ``CARGO_REGISTRY_TOKEN``: :data:`CREDENTIAL_SUFFIXES`) stays a driver's to
  set; a name the list spells out never does.

A managed MCP server's own process (D5b's templated variables, its stdio
credential variable, a ``server.json`` import) is checked against
:data:`CODE_ENV` and :data:`CODE_ENV_PREFIXES` only, the list its bridge
refuses: those variables never reach a workspace, and a server's
``NODE_ENV`` or ``JAVA_HOME`` is its own business.

This is a **best-effort lint against known tool hooks**, never a sandbox: a
tool this list does not know may read a variable it does not name, and a
value a driver delivers is data the workspace's own programs then read. The
author of a driver image is the trust boundary, as the author of a workspace
image is: register only images whose authors you trust with the connector's
credentials and with what reaches the workspace. A driver's declared names
(``env_names``) are shown before anyone attaches its connector.

Environment connectors keep the first layer only: whether they adopt the
driver list is the owner's decision. Standard library only.
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

#: The variables a runtime reads code, its search path, a command to run or
#: a package index from, which the managed MCP bridge refuses on its own in
#: the stdio process it starts (``drivers/mcp-bridge`` ``codeEnv`` mirrors
#: this list and :data:`CODE_ENV_PREFIXES`; a test keeps them equal). Part
#: of what a driver may not set.
CODE_ENV: frozenset[str] = frozenset(
    {
        "BASH_ENV",
        "BASHOPTS",
        "BROWSER",
        "BUN_OPTIONS",
        "CLASSPATH",
        "DOTNET_STARTUP_HOOKS",
        "EDITOR",
        "ELECTRON_RUN_AS_NODE",
        "ENV",
        "GCONV_PATH",
        "GEM_HOME",
        "GEM_PATH",
        "GLIBC_TUNABLES",
        "HOME",
        "IFS",
        "JAVA_OPTS",
        "JAVA_TOOL_OPTIONS",
        "JDK_JAVA_OPTIONS",
        "_JAVA_OPTIONS",
        "LESSOPEN",
        "NODE_OPTIONS",
        "NODE_PATH",
        "NODE_REPL_EXTERNAL_MODULE",
        "OPENSSL_CONF",
        "OPENSSL_MODULES",
        "PAGER",
        "PATH",
        "PERL5DB",
        "PERL5LIB",
        "PERL5OPT",
        "PERLLIB",
        "PROMPT_COMMAND",
        "PS4",
        "PYTHONBREAKPOINT",
        "PYTHONHOME",
        "PYTHONINSPECT",
        "PYTHONPATH",
        "PYTHONSTARTUP",
        "PYTHONUSERBASE",
        "PYTHONWARNINGS",
        "RUBYGEMS_GEMDEPS",
        "RUBYLIB",
        "RUBYOPT",
        "SHELLOPTS",
        "SSH_ASKPASS",
        "SUDO_ASKPASS",
        # A stdio process's HOME and TMPDIR are its private directory.
        "TMPDIR",
        "VISUAL",
        "ZDOTDIR",
    }
)
#: Prefixes of whole families of such variables (git's commands and config,
#: npm's, pip's and uv's settings, a package index among them).
CODE_ENV_PREFIXES: tuple[str, ...] = ("GIT_", "NPM_CONFIG_", "PIP_", "UV_")

DRIVER_DENIED_NAMES: frozenset[str] = CODE_ENV | frozenset(
    {
        # A config file or directory a tool reads, which can run a command.
        "KUBECONFIG",
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_CERT_PATH",
        "DOCKER_TLS_VERIFY",
        "AWS_CONFIG_FILE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "GNUPGHOME",
        "GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES",
        "IPYTHONDIR",
        "VIM",
        "HGRCPATH",
        "MAILCAPS",
        "CONFIG_SITE",
        "CONFIG_SHELL",
        "COMPOSER",
        # rc files a tool reads from the variable that names them.
        "INPUTRC",
        "CONDARC",
        "WGETRC",
        "SCREENRC",
        "MAILRC",
        "PSQLRC",
        "IRBRC",
        "GEMRC",
        "NETRC",
        "NPMRC",
        "TIGRC_USER",
        "TIGRC_SYSTEM",
        "LYNX_CFG",
        # A remote shell, a merge tool, an editor or a runtime a tool starts,
        # a runtime's debug switches, and libpq's TLS settings.
        "RSYNC_RSH",
        "CVS_RSH",
        "SVN_SSH",
        "FCEDIT",
        "HGMERGE",
        "PIPX_DEFAULT_PYTHON",
        "GODEBUG",
        "PGSSLMODE",
        "PGSSLROOTCERT",
        # Where temporary and history files go.
        "TMP",
        "TEMP",
        "HISTFILE",
        # A shell's prompts expand commands; less and man run a command
        # from their options; a shell's own settings.
        "PS0",
        "PS1",
        "PS2",
        "PS3",
        "PROMPT",
        "RPROMPT",
        "LESS",
        "LESSKEY",
        "LESSKEYIN",
        "LESSKEY_SYSTEM",
        "LESSCLOSE",
        "MANOPT",
        "GLOBIGNORE",
        "TMOUT",
        # Editors, start-up files, loaders and locales of more runtimes.
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
        "OPENSSL_ENGINES",
        "LOCPATH",
        "NLSPATH",
        "TERMINFO",
        "TERMINFO_DIRS",
        "MAKEFILES",
        "CC",
        "CXX",
        "CPP",
        "LD",
        "AR",
        "RUSTC",
        "RUSTDOC",
        "JAVA_HOME",
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
#: (``SSL_CERT_FILE``, ``SSL_CERT_DIR``, ``SSLKEYLOGFILE``), every runtime's,
#: build tool's and package manager's settings, the XDG directories.
DRIVER_DENIED_PREFIXES: tuple[str, ...] = CODE_ENV_PREFIXES + (
    "SSH_",
    "SSL",
    "XDG_",
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
    "ERL_",
    "ELIXIR_",
    "GRADLE_",
    "MAVEN_",
    "YARN_",
    "COREPACK_",
    "CLOUDSDK_",
    "ANSIBLE_",
    "TF_",
    "CMAKE_",
    "JULIA_",
    "JUPYTER_",
    "BASH_",
    "NODE_",
    "DENO_",
    "POETRY_",
    "PIPENV_",
    "COMPOSER_",
    "CONDA_",
)
#: Suffixes that name a proxy, a password prompt, a pager, an editor, a
#: browser, options or flags handed to a program, a command, a config file
#: or directory, or a tool's home, in any tool (``HTTPS_PROXY``,
#: ``GH_PAGER``, ``MAVEN_OPTS``, ``CFLAGS``, ``FZF_DEFAULT_COMMAND``,
#: ``HGRCPATH``, ``BOTO_CONFIG``, ``GH_CONFIG_DIR``, ``HELM_DATA_HOME``...).
#: An rc file is named, never matched by ``RC`` (``DATA_SRC`` is data).
DRIVER_DENIED_SUFFIXES: tuple[str, ...] = (
    "_PROXY",
    "ASKPASS",
    "PAGER",
    "EDITOR",
    "BROWSER",
    "_OPTS",
    "_OPTIONS",
    "FLAGS",
    "_COMMAND",
    "_ARGS",
    "RCPATH",
    "_CONFIG",
    "_CONFIG_FILE",
    "_CONFIG_PATH",
    "_CONFIG_DIR",
    "_HOME",
)
#: The longest name a driver may set.
MAX_DRIVER_ENV_NAME = 128
#: A credential's name: within a prefix family (``NODE_*``, ``CARGO_*``,
#: ``UV_*``...), a name ending so is the credential a tool sends upstream
#: (``NODE_AUTH_TOKEN``, ``CARGO_REGISTRY_TOKEN``, ``UV_PUBLISH_TOKEN``,
#: ``GEM_HOST_API_KEY``), never a hook: a driver may set it. A name the list
#: spells out is refused whatever it ends with.
CREDENTIAL_SUFFIXES: tuple[str, ...] = (
    "_TOKEN",
    "_API_KEY",
    "_PASSWORD",
    "_SECRET",
    "_ACCESS_KEY",
    "_SECRET_KEY",
)
#: Prefix families no credential-shaped name escapes: the dynamic loader's,
#: Python's and git's config entries (the workspace's own, or code).
_NEVER_EXEMPT_PREFIXES: tuple[str, ...] = ("LD_", "DYLD_", "PYTHON", "GIT_CONFIG_")


def credential_shaped(name: str) -> bool:
    """Whether ``name`` ends like a credential (:data:`CREDENTIAL_SUFFIXES`)."""
    return name.upper().endswith(CREDENTIAL_SUFFIXES)


def loads_code(name: str) -> bool:
    """Whether ``name`` is on the list of variables a driver may not set (a
    known tool hook, compared in upper case). Every name the list spells
    out and every suffix family is refused; a prefix family spares a
    credential-shaped name (:data:`CREDENTIAL_SUFFIXES`). The workspace's
    own names are :func:`workspace_name_problem`'s."""
    upper = name.upper()
    if (
        upper in WORKSPACE_RESERVED_NAMES
        or upper in CONFIG_POINTER_ENV_NAMES
        or upper in DRIVER_DENIED_NAMES
        or upper.startswith(_NEVER_EXEMPT_PREFIXES)
        or upper.startswith(WORKSPACE_RESERVED_PREFIXES)
        or upper.endswith(DRIVER_DENIED_SUFFIXES)
    ):
        return True
    if upper.startswith(DRIVER_DENIED_PREFIXES) or upper.startswith(
        CONFIG_POINTER_ENV_PREFIXES
    ):
        return not credential_shaped(upper)
    return False


def driver_env_problem(name: object) -> str | None:
    """Why a driver may not set ``name`` in a workspace (``None``: it may)."""
    problem = workspace_name_problem(name)
    if problem is not None:
        return problem
    assert isinstance(name, str)
    if len(name) > MAX_DRIVER_ENV_NAME:
        return f"A driver's variable name is at most {MAX_DRIVER_ENV_NAME} characters"
    if loads_code(name):
        return (
            f"{name} is not a variable a driver may set: tools read it to run "
            "code, redirect traffic or loosen TLS"
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
    "CODE_ENV",
    "CODE_ENV_PREFIXES",
    "CREDENTIAL_SUFFIXES",
    "CONFIG_POINTER_ENV_NAMES",
    "CONFIG_POINTER_ENV_PREFIXES",
    "DRIVER_DENIED_NAMES",
    "DRIVER_DENIED_PREFIXES",
    "DRIVER_DENIED_SUFFIXES",
    "ENV_NAME",
    "MAX_DRIVER_ENV_NAME",
    "MAX_ENV_VALUE_BYTES",
    "WORKSPACE_RESERVED_NAMES",
    "WORKSPACE_RESERVED_PREFIXES",
    "driver_env_problem",
    "driver_env_problems",
    "credential_shaped",
    "env_value_problem",
    "loads_code",
    "points_a_tool_at_code",
    "workspace_name_problem",
]
