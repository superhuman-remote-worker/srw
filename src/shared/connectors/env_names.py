"""The environment variable names a connector may set in a workspace: one rule.

Credential connectors can't execute commands in the workspace; anything more
is for the SSH entry point a driver will declare (connector drivers,
decisions 26, 27 and 36). That is one small rule for every connector, SRW's
own and a registered image driver's alike: SRW's connectors are trusted for
being reputable and open source, not held to other rules.
:func:`connector_env_problem` says whether a connector may set a name. It
refuses:

* **SRW's reserved names** (decision 24): ``PATH``, ``HOME``, the shell's
  own variables (``SHELL``, ``ENV``, ``BASH_ENV``, ``IFS``,
  ``PROMPT_COMMAND``...), the ``SRW_``, ``LD_``, ``DYLD_`` and ``PYTHON``
  families, and ``KUBECONFIG``, which names the kubeconfig SRW's kubeconfig
  connectors merge into.
* **Code hooks**, the known variables that make a tool run code: a command
  or code it runs (``GIT_SSH_COMMAND``, ``EDITOR``, ``VISUAL``, ``PAGER``,
  ``SSH_ASKPASS``, ``CC``, every ``*_COMMAND``), options or flags it hands a
  runtime (``NODE_OPTIONS``, ``JAVA_TOOL_OPTIONS``, ``RUBYOPT``, every
  ``*_OPTS``, ``*_OPTIONS``, ``*FLAGS``, ``*_ARGS``), a file or directory it
  reads config, start-up code, plugins or modules from (``GIT_CONFIG_*``,
  ``GIT_EXEC_PATH``, the rc files, ``XDG_CONFIG_HOME``, ``NODE_PATH``, every
  ``*_CONFIG`` and ``*_HOME``), or where its next install fetches code
  (``PIP_INDEX_URL``, ``GOPROXY``, ``GOTOOLCHAIN``). The settings of the
  common runtimes, build tools and package managers are refused as whole
  families (``NODE_*``, ``CARGO_*``, ``NPM_CONFIG_*``, ``PIP_*``...).

Allowed for everyone, because they change where a tool connects and which
certificates or checksums it trusts, never what it runs: proxies (every
``*_PROXY``, ``NO_PROXY`` included), CA bundles and TLS checks
(``SSL_CERT_FILE``, ``REQUESTS_CA_BUNDLE``, ``NODE_EXTRA_CA_CERTS``,
``GIT_SSL_CAINFO``, ``PGSSLMODE``...), name resolution (``HOSTALIASES``,
``RES_OPTIONS``), Docker's daemon address, and Go's private-module and
checksum settings (``GOPRIVATE``, ``GONOSUMDB``...). So are the variables
that name a credential file whose format the file rule already accepts at
the tool's own location (``AWS_CONFIG_FILE``, ``AWS_SHARED_CREDENTIALS_FILE``,
``NETRC``, ``PGPASSFILE``, ``GOOGLE_APPLICATION_CREDENTIALS``; see
:mod:`.file_targets`), and every name no list matches. Within a prefix
family a credential-shaped name (``NODE_AUTH_TOKEN``,
``CARGO_REGISTRY_TOKEN``: :data:`CREDENTIAL_SUFFIXES`) stays a connector's to
set; a name the list spells out never does. Names are compared in upper
case: a tool that reads ``https_proxy`` reads it in lower case.

The two families SRW's own work runs on, name by name:

* ``GIT_*`` is refused as a family. Git reads its members as a command
  (``GIT_SSH``, ``GIT_SSH_COMMAND``, ``GIT_ASKPASS``, ``GIT_EDITOR``,
  ``GIT_SEQUENCE_EDITOR``, ``GIT_PAGER``, ``GIT_EXTERNAL_DIFF``,
  ``GIT_PROXY_COMMAND``), as a place it loads programs, hooks or config from
  (``GIT_EXEC_PATH``, ``GIT_TEMPLATE_DIR``, ``GIT_DIR``, ``GIT_COMMON_DIR``,
  ``GIT_CONFIG_GLOBAL``, ``GIT_CONFIG_SYSTEM``, ``GIT_CONFIG_NOSYSTEM``,
  ``GIT_CONFIG_PARAMETERS``, ``GIT_CONFIG_COUNT`` with
  ``GIT_CONFIG_KEY_<n>`` and ``GIT_CONFIG_VALUE_<n>``: a config sets
  ``core.sshCommand`` or ``core.fsmonitor``), as a switch for a remote helper
  that runs commands (``GIT_ALLOW_PROTOCOL``, ``GIT_PROTOCOL_FROM_USER``:
  ``ext::``), or as SRW's own non-interactive settings
  (``GIT_TERMINAL_PROMPT``); where it reads objects, writes checkouts and
  traces (``GIT_OBJECT_DIRECTORY``, ``GIT_WORK_TREE``, ``GIT_INDEX_FILE``,
  ``GIT_TRACE*`` with ``GIT_TRACE_REDACT``) is no credential's business
  either. Allowed: the commit identity (``GIT_AUTHOR_NAME``,
  ``GIT_AUTHOR_EMAIL``, ``GIT_AUTHOR_DATE`` and the three ``GIT_COMMITTER_``
  names), which git only records, and git's CA bundle and TLS check
  (``GIT_SSL_CAINFO``, ``GIT_SSL_CAPATH``, ``GIT_SSL_NO_VERIFY``), as for
  every tool. ``GIT_CONFIG_*`` spares no credential-shaped name (any key is
  config).
* ``SSH_*`` is refused as a family, and no known member is harmless:
  ``SSH_ASKPASS`` and ``SSH_ASKPASS_REQUIRE`` run a program,
  ``SSH_SK_PROVIDER`` loads a library, ``SSH_AUTH_SOCK`` and
  ``SSH_AGENT_PID`` are the ssh-agent SRW's managed repositories and SSH
  identities sign with, and ``SSH_CONNECTION``, ``SSH_CLIENT``, ``SSH_TTY``
  and ``SSH_ORIGINAL_COMMAND`` belong to the session SRW reaches the
  workspace through. (``SUDO_ASKPASS`` runs a program too.)

The rule applies to an environment connector's variables
(``shared.credential_connectors``), to a credential file's ``env_var``
(which names the stored file), and to what a registered image driver's bind
returns and the names its spec declares (``env_names``, shown before anyone
attaches its connector: :mod:`.registration`). The orchestrator refuses a
name when a connector is saved, with this module's reason. A row saved
before the rule keeps working without it: the agent skips the refused
variable at delivery, logs it and says why in the connector's README, as it
skips a credential file outside the file allowlist.

A managed MCP server's own process (D5b's templated variables, its stdio
credential variable, a ``server.json`` import) is checked against
:data:`CODE_ENV` and :data:`CODE_ENV_PREFIXES` only, the list its bridge
refuses: those variables never reach a workspace, and a server's
``NODE_ENV`` or ``JAVA_HOME`` is its own business.

This is a **best-effort lint against known tool hooks**, never a sandbox: a
tool the list does not know may read a variable it does not name, and a
value a connector delivers is data the workspace's own programs then read.
What a connector does is its user's responsibility (decision 17), as with a
workspace image: the author of a driver image is the trust boundary, so
register only images whose authors you trust with the connector's
credentials and with what reaches the workspace. Standard library only.
"""

from __future__ import annotations

import re

#: What an environment variable name may look like.
ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
#: The longest name a connector may set.
MAX_ENV_NAME = 128
#: The longest value one variable may hold.
MAX_ENV_VALUE_BYTES = 65536

# ---------------------------------------------------------------------------
# SRW's reserved names
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
#: Families the workspace reserves.
WORKSPACE_RESERVED_PREFIXES: tuple[str, ...] = ("SRW_", "LD_", "DYLD_", "PYTHON")
#: The variable SRW's kubeconfig connectors merge their kubeconfigs into.
KUBECONFIG = "KUBECONFIG"

# ---------------------------------------------------------------------------
# Code hooks
# ---------------------------------------------------------------------------

#: The variables a runtime reads code, its search path, a command to run or
#: a package index from, which the managed MCP bridge refuses on its own in
#: the stdio process it starts (``drivers/mcp-bridge`` ``codeEnv`` mirrors
#: this list and :data:`CODE_ENV_PREFIXES`; a test keeps them equal). Part
#: of what no connector may set.
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

#: Every name the rule spells out: refused whatever it ends with.
CODE_HOOK_NAMES: frozenset[str] = CODE_ENV | frozenset(
    {
        # git: commands, and where it loads programs, hooks and config from.
        "GIT_ALLOW_PROTOCOL",
        "GIT_ASKPASS",
        "GIT_COMMON_DIR",
        "GIT_CONFIG",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_NOSYSTEM",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_SYSTEM",
        "GIT_DIR",
        "GIT_EDITOR",
        "GIT_EXEC_PATH",
        "GIT_EXTERNAL_DIFF",
        "GIT_PAGER",
        "GIT_PROTOCOL_FROM_USER",
        "GIT_PROXY_COMMAND",
        "GIT_SEQUENCE_EDITOR",
        "GIT_SSH",
        "GIT_SSH_COMMAND",
        "GIT_TEMPLATE_DIR",
        # ssh: a program, a library, SRW's ssh-agent.
        "SSH_ASKPASS_REQUIRE",
        "SSH_AUTH_SOCK",
        "SSH_SK_PROVIDER",
        # A shell's prompts expand commands; its start-up and function
        # files, history, editor and own settings.
        "FCEDIT",
        "FPATH",
        "GLOBIGNORE",
        "HISTFILE",
        "PROMPT",
        "PS0",
        "PS1",
        "PS2",
        "PS3",
        "RPROMPT",
        "TMOUT",
        # Editors, pagers and their start-up files: less and man run a
        # command from their options.
        "EMACSLOADPATH",
        "EXINIT",
        "LESS",
        "LESSCLOSE",
        "LESSKEY",
        "LESSKEYIN",
        "LESSKEY_SYSTEM",
        "MANOPT",
        "MANPAGER",
        "MYVIMRC",
        "VIM",
        "VIMINIT",
        "VIMRUNTIME",
        # rc files a tool reads from the variable that names them.
        "CONDARC",
        "CURL_HOME",
        "GEMRC",
        "HGRCPATH",
        "INPUTRC",
        "IRBRC",
        "LYNX_CFG",
        "MAILCAPS",
        "MAILRC",
        "NPMRC",
        "PSQLRC",
        "SCREENRC",
        "TIGRC_SYSTEM",
        "TIGRC_USER",
        "WGETRC",
        # Config files and directories whose keys run a command, or which
        # turn one on.
        "COMPOSER",
        "CONFIG_SHELL",
        "CONFIG_SITE",
        "DOCKER_CONFIG",
        "GNUPGHOME",
        "GOENV",
        "GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES",
        "HELM_PLUGINS",
        "IPYTHONDIR",
        # Runtimes: start-up code, search paths, homes and debug switches.
        "GODEBUG",
        "JAVA_HOME",
        "PHPRC",
        "PHP_INI_SCAN_DIR",
        "PIPX_DEFAULT_PYTHON",
        "R_ENVIRON",
        "R_ENVIRON_USER",
        "R_LIBS",
        "R_LIBS_USER",
        "R_PROFILE",
        "R_PROFILE_USER",
        "TCLLIBPATH",
        # Toolchains and the commands a build runs.
        "AR",
        "CARGO_HOME",
        "CC",
        "CPP",
        "CXX",
        "GOFLAGS",
        "GOTOOLCHAIN",
        "LD",
        "MAKEFILES",
        "RUSTC",
        "RUSTC_WRAPPER",
        "RUSTDOC",
        # A remote shell or merge tool another tool starts.
        "CVS_RSH",
        "HGMERGE",
        "RSYNC_RSH",
        "SVN_SSH",
        # Where the next install fetches code from.
        "GOPROXY",
        # OpenSSL's engines, and the locale, message catalogue and terminal
        # data libraries load at start-up (their parsers have run code from
        # crafted files).
        "LOCPATH",
        "NLSPATH",
        "OPENSSL_ENGINES",
        "TERMINFO",
        "TERMINFO_DIRS",
        # Where tools write the files they then run (build trees, scripts).
        "TEMP",
        "TMP",
        # Where every config is looked for.
        "XDG_CONFIG_DIRS",
        "XDG_CONFIG_HOME",
        "XDG_DATA_DIRS",
        "XDG_DATA_HOME",
    }
)
#: Whole families: git's (but for :data:`ALLOWED_NAMES`) and ssh's, the XDG
#: directories, and every runtime's, build tool's and package manager's
#: settings.
CODE_HOOK_PREFIXES: tuple[str, ...] = CODE_ENV_PREFIXES + (
    "SSH_",
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
#: Families no credential-shaped name escapes: git's config entries (any
#: key is config).
NEVER_EXEMPT_PREFIXES: tuple[str, ...] = ("GIT_CONFIG_",)
#: Suffixes that name a password prompt, a pager, an editor, a browser,
#: options or flags handed to a program, a command, a config file or
#: directory, or a tool's home, in any tool (``GH_PAGER``, ``MAVEN_OPTS``,
#: ``CFLAGS``, ``FZF_DEFAULT_COMMAND``, ``HGRCPATH``, ``BOTO_CONFIG``,
#: ``GH_CONFIG_DIR``, ``HELM_DATA_HOME``...). An rc file is named, never
#: matched by ``RC`` (``DATA_SRC`` is data).
CODE_HOOK_SUFFIXES: tuple[str, ...] = (
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
#: A credential's name: within a prefix family (``NODE_*``, ``CARGO_*``,
#: ``UV_*``...), a name ending so is the credential a tool sends upstream
#: (``NODE_AUTH_TOKEN``, ``CARGO_REGISTRY_TOKEN``, ``UV_PUBLISH_TOKEN``,
#: ``GEM_HOST_API_KEY``), never a hook: a connector may set it. A name the
#: list spells out is refused whatever it ends with.
CREDENTIAL_SUFFIXES: tuple[str, ...] = (
    "_TOKEN",
    "_API_KEY",
    "_PASSWORD",
    "_SECRET",
    "_ACCESS_KEY",
    "_SECRET_KEY",
)

# ---------------------------------------------------------------------------
# Allowed for everyone
# ---------------------------------------------------------------------------

#: Allowed whatever family or suffix they share with a hook. Every
#: ``*_PROXY`` is allowed too.
ALLOWED_NAMES: frozenset[str] = frozenset(
    {
        # CA bundles and certificate directories.
        "AWS_CA_BUNDLE",
        "BUNDLE_SSL_CA_CERT",
        "CARGO_HTTP_CAINFO",
        "CLOUDSDK_CORE_CUSTOM_CA_CERTS_FILE",
        "CURL_CA_BUNDLE",
        "DENO_CERT",
        "DOCKER_CERT_PATH",
        "GIT_SSL_CAINFO",
        "GIT_SSL_CAPATH",
        "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH",
        "HTTPLIB2_CA_CERTS",
        "NIX_SSL_CERT_FILE",
        "NODE_EXTRA_CA_CERTS",
        "NPM_CONFIG_CAFILE",
        "PGSSLROOTCERT",
        "PIP_CERT",
        "REQUESTS_CA_BUNDLE",
        "SSL_CERT_DIR",
        "SSL_CERT_FILE",
        # TLS checks.
        "DOCKER_TLS_VERIFY",
        "GIT_SSL_NO_VERIFY",
        "NODE_TLS_REJECT_UNAUTHORIZED",
        "PGSSLMODE",
        # Where traffic goes: name resolution and Docker's daemon.
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "HOSTALIASES",
        "LOCALDOMAIN",
        "RES_OPTIONS",
        # Go's private modules and checksum database.
        "GOINSECURE",
        "GONOPROXY",
        "GONOSUMCHECK",
        "GONOSUMDB",
        "GOPRIVATE",
        "GOSUMDB",
        # A credential file in a format the file rule accepts at the tool's
        # own location (~/.aws/, ~/.netrc, ~/.pgpass, ~/.config/gcloud/).
        "AWS_CONFIG_FILE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "NETRC",
        "PGPASSFILE",
        # git's commit identity: recorded, never run.
        "GIT_AUTHOR_DATE",
        "GIT_AUTHOR_EMAIL",
        "GIT_AUTHOR_NAME",
        "GIT_COMMITTER_DATE",
        "GIT_COMMITTER_EMAIL",
        "GIT_COMMITTER_NAME",
    }
)
#: The suffix of every proxy variable (``HTTPS_PROXY``, ``no_proxy``,
#: ``npm_config_https_proxy``...).
PROXY_SUFFIX = "_PROXY"


def _code_hook(upper: str) -> bool:
    """Whether the upper-cased name is a known code hook."""
    if upper in CODE_HOOK_NAMES or upper.startswith(NEVER_EXEMPT_PREFIXES):
        return True
    if upper in ALLOWED_NAMES or upper.endswith(PROXY_SUFFIX):
        return False
    if upper.endswith(CODE_HOOK_SUFFIXES):
        return True
    return upper.startswith(CODE_HOOK_PREFIXES) and not upper.endswith(
        CREDENTIAL_SUFFIXES
    )


def connector_env_problem(name: object) -> str | None:
    """Why no connector may set ``name`` in a workspace (``None``: any may)."""
    if not isinstance(name, str) or not ENV_NAME.fullmatch(name):
        return (
            "Environment names must contain letters, digits or underscores "
            "and cannot start with a digit"
        )
    if len(name) > MAX_ENV_NAME:
        return f"Environment names are at most {MAX_ENV_NAME} characters"
    upper = name.upper()
    if upper in WORKSPACE_RESERVED_NAMES or upper.startswith(
        WORKSPACE_RESERVED_PREFIXES
    ):
        return f"Environment name {name} is reserved by the workspace"
    if upper == KUBECONFIG:
        return f"{name} is reserved: it names the merged kubeconfig"
    if _code_hook(upper):
        return (
            f"{name} is not a variable a connector may set: tools read it to "
            "run code or load their config"
        )
    return None


def env_value_problem(name: str, value: object) -> str | None:
    """Why ``value`` cannot be variable ``name``'s value (never shows it)."""
    if not isinstance(value, str) or "\x00" in value:
        return f"Environment variable {name} must be a string without NUL bytes"
    if len(value.encode("utf-8")) > MAX_ENV_VALUE_BYTES:
        return f"Environment variable {name} exceeds 64 KiB"
    return None


__all__ = [
    "ALLOWED_NAMES",
    "CODE_ENV",
    "CODE_ENV_PREFIXES",
    "CODE_HOOK_NAMES",
    "CODE_HOOK_PREFIXES",
    "CODE_HOOK_SUFFIXES",
    "CREDENTIAL_SUFFIXES",
    "ENV_NAME",
    "KUBECONFIG",
    "MAX_ENV_NAME",
    "MAX_ENV_VALUE_BYTES",
    "NEVER_EXEMPT_PREFIXES",
    "PROXY_SUFFIX",
    "WORKSPACE_RESERVED_NAMES",
    "WORKSPACE_RESERVED_PREFIXES",
    "connector_env_problem",
    "env_value_problem",
]
