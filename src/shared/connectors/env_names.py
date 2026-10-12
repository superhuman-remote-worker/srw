"""The environment variable names a connector may set in a workspace: one rule.

Credential connectors can't execute commands in the workspace; anything more
is for the SSH entry point a driver will declare (connector drivers,
decisions 26, 27 and 36). That is one small rule for every connector, SRW's
own and a registered image driver's alike: SRW's connectors are trusted for
being reputable and open source, not held to other rules.
:func:`connector_env_problem` says whether a connector may set a name. It
refuses only names that make a tool run code or load a config or code file:

* **SRW's reserved names** (decision 24): ``PATH``, ``HOME``, the shell's
  own variables (``SHELL``, ``ENV``, ``BASH_ENV``, ``IFS``,
  ``PROMPT_COMMAND``...), tmux's (``TMUX``, ``TMUX_PANE``, ``TMUX_TMPDIR``),
  ``DEBIAN_FRONTEND`` (SRW's shells are non-interactive), the ``SRW_``,
  ``LD_``, ``DYLD_`` and ``PYTHON`` families, and ``KUBECONFIG``, which names
  the kubeconfig SRW's kubeconfig connectors merge into.
* **Code hooks**, the known variables that make a tool run code. A name a
  tool reads as a command or code to run (``GIT_SSH_COMMAND``, ``EDITOR``,
  ``VISUAL``, ``PAGER``, ``SSH_ASKPASS``, ``CC``, ``MAKE``,
  ``KUBECTL_EXTERNAL_DIFF``, ``GOCACHEPROG``, a build's ``<tool>-config``
  program, ``PKG_CONFIG``, ``LLVM_CONFIG`` or ``PG_CONFIG``, every
  ``*_COMMAND``, ``*ASKPASS``, ``*PAGER``, ``*EDITOR`` and ``*BROWSER``); as
  options or flags it hands a runtime or a
  build (``NODE_OPTIONS``, ``JAVA_TOOL_OPTIONS``, ``RUBYOPT``,
  ``PYTEST_ADDOPTS``, every ``*_OPTS``, ``*_OPTIONS`` and ``*_ARGS``, and
  ``CFLAGS``, ``MAKEFLAGS``, ``RUSTFLAGS``: a ``FLAGS`` ending not after an
  underscore, so ``FEATURE_FLAGS`` is data); as a profiler or start-up hook a
  runtime loads (``DOTNET_STARTUP_HOOKS``, ``CORECLR_PROFILER``); as a file
  or directory it reads config, start-up code, plugins or modules from
  (``GIT_CONFIG_*``, ``GIT_EXEC_PATH``, the rc files, ``XDG_CONFIG_HOME``,
  ``NODE_PATH``, ``BOTO_CONFIG``, every ``*_CONFIG_FILE``, ``*_CONFIG_PATH``,
  ``*_CONFIG_DIR`` and ``*_CONFIG_HOME``, and the tool homes spelled out,
  ``GEM_HOME``, ``CARGO_HOME``, ``RUSTUP_HOME``, ``GOROOT``, ``JAVA_HOME``...;
  a bare ``*_CONFIG`` or ``*_HOME`` is data, ``APP_CONFIG`` or
  ``PROJECT_HOME``); or as where and how its next install fetches code
  (``PIP_INDEX_URL``, npm's and uv's registries, ``GOPROXY``,
  ``GOTOOLCHAIN``, and Go's checksum switches ``GOSUMDB``, ``GONOSUMDB``,
  ``GONOSUMCHECK`` and ``GOINSECURE``). A family is refused whole only where
  a tool maps its entire configuration, or a runtime its settings, onto the
  environment, so that any member may be a hook: ``GIT_*``,
  ``NPM_CONFIG_*``, ``PIP_*``, ``UV_*``, ``CARGO_*``, ``BUNDLE_*``,
  ``YARN_*``, ``POETRY_*``, ``PIPENV_*``, ``COMPOSER_*``, ``CONDA_*``,
  ``ANSIBLE_*``, ``CMAKE_*``, ``COREPACK_*``, ``BUN_CONFIG_*``, ``CCACHE_*``,
  ``DOTNET_*`` (with ``CORECLR_*`` and ``COMPlus_*``, the same runtime's),
  ``PERL5*`` and ``BASH_*``. Elsewhere only the hooks are named:
  ``NODE_OPTIONS`` but not ``NODE_ENV``, ``TF_CLI_ARGS*`` but not
  ``TF_VAR_*``, ``CLOUDSDK_PYTHON*`` but not ``CLOUDSDK_CORE_PROJECT``,
  OpenSSH's variables but not ``SSH_HOST``.

Allowed for everyone, because they change where a tool connects and which
certificates it trusts, never what it runs: proxies (every ``*_PROXY``,
``NO_PROXY`` included), CA bundles, client certificates and TLS checks, in a
refused family too (``SSL_CERT_FILE``, ``REQUESTS_CA_BUNDLE``,
``NODE_EXTRA_CA_CERTS``, ``GIT_SSL_CAINFO``, ``GIT_SSL_CERT``,
``NPM_CONFIG_CA``, ``NPM_CONFIG_STRICT_SSL``, ``PIP_CLIENT_CERT``,
``PIP_TRUSTED_HOST``, ``YARN_HTTPS_CA_FILE_PATH``, ``UV_NATIVE_TLS``,
``POETRY_CERTIFICATES_<REPO>_CERT``, ``PGSSLMODE``...), name resolution
(``HOSTALIASES``, ``RES_OPTIONS``),
Docker's daemon address, and ``GOPRIVATE`` and ``GONOPROXY``, which
private-module credentials need. An allowed proxy with an allowed CA bundle
can redirect what a tool fetches too; that residual is accepted, because the
rule allows proxies. Allowed as well: the variables that name a credential
file whose format the file rule already accepts at the tool's own location
(``AWS_CONFIG_FILE``, ``AWS_SHARED_CREDENTIALS_FILE``, ``NETRC``,
``PGPASSFILE``, ``GOOGLE_APPLICATION_CREDENTIALS``; see :mod:`.file_targets`),
git's commit identity, the settings of a refused family that run nothing
(``DOTNET_ENVIRONMENT``, ``CARGO_TERM_*``, ``CMAKE_BUILD_TYPE``,
``CONDA_DEFAULT_ENV``, ``ANSIBLE_HOST_KEY_CHECKING``...), and every name no
list matches.

Within a refused family a credential-shaped name stays a connector's to set:
one ending in ``_TOKEN``, ``_TOKENS``, ``_AUTH``, ``_AUTH_IDENT``,
``_USERNAME``, ``_USER``, ``_KEY``, ``_PASSWORD`` or ``_SECRET``
(:data:`CREDENTIAL_SUFFIXES`: ``NPM_CONFIG__AUTH``, ``CARGO_REGISTRY_TOKEN``,
``YARN_NPM_AUTH_IDENT``, ``COMPOSER_AUTH``), Poetry's
``POETRY_PYPI_TOKEN_<repository>``, and Bundler's credentials for a gem
server, ``BUNDLE_<HOST>__<TLD>`` (but for its ``MIRROR__``, ``BUILD__``
and ``LOCAL__`` settings). A name the list spells out, and the variants of
one hook (``GIT_CONFIG_*``, ``TF_CLI_ARGS*``, ``LUA_PATH*``,
``PKG_CONFIG*``, ``CARGO_ALIAS_*``, ``CCACHE_*``...:
:data:`NEVER_EXEMPT_PREFIXES`), never are. Names are compared in upper case:
a tool that reads ``https_proxy`` reads it in lower case.

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
  every tool.
* ``SSH_*`` is not a family: the ``SSH_`` variables OpenSSH is known to
  read as a program, a library or SRW's own are refused by name.
  ``SSH_ASKPASS`` and ``SSH_ASKPASS_REQUIRE`` run a program,
  ``SSH_SK_PROVIDER`` loads a library, ``SSH_SK_HELPER`` and
  ``SSH_PKCS11_HELPER`` name the helper programs it starts, ``SSH_AUTH_SOCK``
  and ``SSH_AGENT_PID`` are the ssh-agent SRW's managed repositories and SSH
  identities sign with, and ``SSH_CONNECTION``, ``SSH_CLIENT``, ``SSH_TTY``
  and ``SSH_ORIGINAL_COMMAND`` belong to the session SRW reaches the
  workspace through. A script's own ``SSH_HOST``, ``SSH_USER`` or
  ``SSH_PRIVATE_KEY`` is data. (``SUDO_ASKPASS`` runs a program too.)

The rule applies to an environment connector's variables
(``shared.credential_connectors``), to a credential file's ``env_var``
(which names the stored file), and to what a registered image driver's bind
returns and the names its spec declares (``env_names``, shown before anyone
attaches its connector: :mod:`.registration`). The orchestrator refuses a
name when a connector is saved, with this module's reason. A row saved
before the rule keeps working without it: the agent skips the refused
variable at delivery, logs it and says why in the connector's README, as it
skips a credential file outside the file allowlist, and an edit of the row
drops it.

A managed MCP server's own process (D5b's templated variables, its stdio
credential variable, a ``server.json`` import) is checked against
:data:`CODE_ENV` and :data:`CODE_ENV_PREFIXES` only, the list its bridge
refuses: those variables never reach a workspace, and a server's
``NODE_ENV`` or ``JAVA_HOME`` is its own business.

This is a **best-effort lint against known tool hooks**, never a sandbox: a
tool the list does not know may read a variable it does not name (MSBuild,
for one, reads every variable as a property: only its known import hooks
are named), and a value a connector delivers is data the workspace's own
programs then read.
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
        "TMUX_TMPDIR",
        # SRW's shells are non-interactive (shell_protocol).
        "DEBIAN_FRONTEND",
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
        # OpenSSH: a program, a library, SRW's ssh-agent, the session's own.
        "SSH_AGENT_PID",
        "SSH_ASKPASS_REQUIRE",
        "SSH_AUTH_SOCK",
        "SSH_CLIENT",
        "SSH_CONNECTION",
        "SSH_ORIGINAL_COMMAND",
        "SSH_PKCS11_HELPER",
        "SSH_SK_HELPER",
        "SSH_SK_PROVIDER",
        "SSH_TTY",
        # A shell's prompts and mail checks expand commands (MAILPATH's at
        # every prompt of SRW's interactive shell); its start-up and
        # function files, history, editor and own settings.
        "FCEDIT",
        "FPATH",
        "GLOBIGNORE",
        "HISTFILE",
        "MAIL",
        "MAILCHECK",
        "MAILPATH",
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
        # Config files and directories whose keys run a command or load a
        # plugin, or which turn one on.
        "BOTO_CONFIG",
        "CABAL_CONFIG",
        "CLOUDSDK_CONFIG",
        "COMPOSER",
        "CONFIG_SHELL",
        "CONFIG_SITE",
        "DOCKER_CONFIG",
        "GNUPGHOME",
        "GOENV",
        "GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES",
        "HELM_PLUGINS",
        "IPYTHONDIR",
        "JUPYTER_DATA_DIR",
        "JUPYTER_PATH",
        "KRB5_CONFIG",
        "MAVEN_CONFIG",
        "PGSYSCONFDIR",
        "RCLONE_CONFIG",
        "STARSHIP_CONFIG",
        "TERRAFORM_CONFIG",
        "TF_CLI_CONFIG_FILE",
        "TF_DATA_DIR",
        "TF_PLUGIN_CACHE_DIR",
        "XDG_CACHE_HOME",
        "XDG_CONFIG_DIRS",
        "XDG_CONFIG_HOME",
        "XDG_DATA_DIRS",
        "XDG_DATA_HOME",
        # Tool homes: where a runtime, toolchain or version manager keeps
        # the code it runs and its plugins.
        "ANDROID_HOME",
        "ANDROID_SDK_ROOT",
        "CARGO_HOME",
        "DENO_DIR",
        "GOMODCACHE",
        "GOPATH",
        "GOROOT",
        "GRADLE_USER_HOME",
        "HELM_DATA_HOME",
        "JAVA_HOME",
        "M2_HOME",
        "MARIADB_HOME",
        "MAVEN_HOME",
        "MIX_HOME",
        "MYSQL_HOME",
        "NUGET_FALLBACK_PACKAGES",
        "NUGET_PACKAGES",
        "NVM_DIR",
        "PNPM_HOME",
        "PYENV_ROOT",
        "RBENV_ROOT",
        "RUSTUP_HOME",
        "VOLTA_HOME",
        # Runtimes: code, search paths, options, profilers, plugins, debug
        # switches.
        "CLOUDSDK_BQ_PYTHON",
        "CLOUDSDK_GSUTIL_PYTHON",
        "CORECLR_ENABLE_PROFILING",
        "CORECLR_PROFILER",
        "CORECLR_PROFILER_PATH",
        "DENO_V8_FLAGS",
        "ERL_AFLAGS",
        "ERL_FLAGS",
        "ERL_LIBS",
        "ERL_ZFLAGS",
        "GODEBUG",
        "JULIA_BINDIR",
        "JULIA_DEPOT_PATH",
        "JULIA_LOAD_PATH",
        "JULIA_PROJECT",
        "NODE_COMPILE_CACHE",
        "NUGET_NETCORE_PLUGIN_PATHS",
        "NUGET_NETFX_PLUGIN_PATHS",
        "NUGET_PLUGIN_PATHS",
        "PERL_CPANM_OPT",
        "PERL_MB_OPT",
        "PERL_MM_OPT",
        "PHPRC",
        "PHP_INI_SCAN_DIR",
        "PIPX_DEFAULT_PYTHON",
        "PYTEST_ADDOPTS",
        "PYTEST_PLUGINS",
        "R_ENVIRON",
        "R_ENVIRON_USER",
        "R_LIBS",
        "R_LIBS_USER",
        "R_PROFILE",
        "R_PROFILE_USER",
        "RUBYPATH",
        "RUBYSHELL",
        "TCLLIBPATH",
        # Toolchains and the commands a build or a tool runs.
        "AR",
        "AS",
        "CC",
        "CCACHE_PREFIX",
        "CPP",
        "CXX",
        "FC",
        "GCCGO",
        "GOAUTH",
        "GOCACHEPROG",
        "GOFLAGS",
        "GOTMPDIR",
        "GOVCS",
        "KUBECTL_EXTERNAL_DIFF",
        "LD",
        "MAKE",
        "MAKEFILES",
        "NM",
        "OBJCOPY",
        "RANLIB",
        "RUSTC",
        "RUSTC_WORKSPACE_WRAPPER",
        "RUSTC_WRAPPER",
        "RUSTDOC",
        "STRIP",
        # The flags cgo lets a module's own #cgo lines pass (-fplugin=...).
        "CGO_CFLAGS_ALLOW",
        "CGO_CPPFLAGS_ALLOW",
        "CGO_CXXFLAGS_ALLOW",
        "CGO_FFLAGS_ALLOW",
        "CGO_LDFLAGS_ALLOW",
        # A build's *-config program (autoconf runs $PKG_CONFIG unquoted):
        # the <tool>-config convention, the known ones named.
        "CURL_CONFIG",
        "FREETYPE_CONFIG",
        "GDAL_CONFIG",
        "GEOS_CONFIG",
        "GPGME_CONFIG",
        "GPG_ERROR_CONFIG",
        "ICU_CONFIG",
        "LIBGCRYPT_CONFIG",
        "LLVM_CONFIG",
        "MARIADB_CONFIG",
        "MYSQL_CONFIG",
        "NC_CONFIG",
        "PCRE2_CONFIG",
        "PCRE_CONFIG",
        "PG_CONFIG",
        "SDL2_CONFIG",
        "SDL_CONFIG",
        "XML2_CONFIG",
        "XSLT_CONFIG",
        # MSBuild reads every variable as a property; these import a
        # project file before or after the common targets and props.
        "CUSTOMAFTERMICROSOFTCOMMONPROPS",
        "CUSTOMAFTERMICROSOFTCOMMONTARGETS",
        "CUSTOMAFTERMICROSOFTCSHARPTARGETS",
        "CUSTOMBEFOREMICROSOFTCOMMONPROPS",
        "CUSTOMBEFOREMICROSOFTCOMMONTARGETS",
        "CUSTOMBEFOREMICROSOFTCSHARPTARGETS",
        # A remote shell, connection program or merge tool another tool
        # starts.
        "CVS_RSH",
        "HGMERGE",
        "RSYNC_CONNECT_PROG",
        "RSYNC_RSH",
        "SVN_MERGE",
        "SVN_SSH",
        # Where and how the next install fetches code: its source, its
        # toolchain, and the checksum checks on what it fetched.
        "GOINSECURE",
        "GONOSUMCHECK",
        "GONOSUMDB",
        "GOPROXY",
        "GOSUMDB",
        "GOTOOLCHAIN",
        "JULIA_PKG_SERVER",
        "RUSTUP_DIST_ROOT",
        "RUSTUP_DIST_SERVER",
        "RUSTUP_TOOLCHAIN",
        "RUSTUP_UPDATE_ROOT",
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
    }
)
#: Whole families: a tool maps its entire configuration, or a runtime its
#: settings, onto them, so any member may be a hook. Elsewhere only the hooks
#: are named (``NODE_OPTIONS``, not ``NODE_ENV``).
CODE_HOOK_PREFIXES: tuple[str, ...] = CODE_ENV_PREFIXES + (
    "ANSIBLE_",
    "BASH_",
    "BUNDLE_",
    "BUN_CONFIG_",
    "CARGO_",
    "CMAKE_",
    "COMPLUS_",
    "COMPOSER_",
    "CONDA_",
    "COREPACK_",
    "CORECLR_",
    "DOTNET_",
    "PERL5",
    "PIPENV_",
    "POETRY_",
    "YARN_",
)
#: The variants of one hook, git's config entries (any key is config),
#: cargo's command and credential-provider aliases, and ccache's whole
#: configuration (no credential in it): no credential-shaped name escapes
#: them.
NEVER_EXEMPT_PREFIXES: tuple[str, ...] = (
    "BUN_INSTALL",
    "CARGO_ALIAS_",
    "CARGO_CREDENTIAL_ALIAS_",
    "CCACHE_",
    "CLOUDSDK_COMPONENT_MANAGER_",
    "CLOUDSDK_PYTHON",
    "GIT_CONFIG_",
    "LUA_CPATH",
    "LUA_INIT",
    "LUA_PATH",
    "PKG_CONFIG",
    "TF_CLI_ARGS",
)
#: Suffixes that name a password prompt, a pager, an editor, a browser,
#: options handed to a program, a command, or a config file or directory, in
#: any tool (``GH_PAGER``, ``MAVEN_OPTS``, ``FZF_DEFAULT_COMMAND``,
#: ``HGRCPATH``, ``GH_CONFIG_DIR``, ``HELM_CONFIG_HOME``...). A bare
#: ``_CONFIG`` or ``_HOME`` is not one (``APP_CONFIG``, ``PROJECT_HOME``: the
#: real ones are named), nor is an rc file matched by ``RC`` (``DATA_SRC`` is
#: data).
CODE_HOOK_SUFFIXES: tuple[str, ...] = (
    "ASKPASS",
    "PAGER",
    "EDITOR",
    "BROWSER",
    "_OPTS",
    "_OPTIONS",
    "_COMMAND",
    "_ARGS",
    "RCPATH",
    "_CONFIG_FILE",
    "_CONFIG_PATH",
    "_CONFIG_DIR",
    "_CONFIG_HOME",
)
#: Compiler and build flags end so, not after an underscore: ``CFLAGS``,
#: ``LDFLAGS``, ``MAKEFLAGS``, ``RUSTFLAGS``, ``CGO_CFLAGS``, but never
#: ``FEATURE_FLAGS``.
FLAGS_SUFFIX = "FLAGS"
#: A credential's name: within a refused family, a name ending so is the
#: credential a tool sends upstream (``NPM_CONFIG__AUTH``,
#: ``CARGO_REGISTRY_TOKEN``, ``YARN_NPM_AUTH_IDENT``,
#: ``POETRY_HTTP_BASIC_PYPI_USERNAME``, ``COMPOSER_AUTH``), never a hook: a
#: connector may set it. A name the list spells out is refused whatever it
#: ends with.
CREDENTIAL_SUFFIXES: tuple[str, ...] = (
    "_TOKEN",
    "_TOKENS",
    "_AUTH",
    "_AUTH_IDENT",
    "_USERNAME",
    "_USER",
    "_KEY",
    "_PASSWORD",
    "_SECRET",
)
#: Credentials named by a prefix: Poetry's API token for a repository.
CREDENTIAL_PREFIXES: tuple[str, ...] = ("POETRY_PYPI_TOKEN_",)
#: Bundler names a gem server's credentials after its host, its dots as
#: ``__`` (``BUNDLE_GEMS__EXAMPLE__COM``, Gemfury's ``BUNDLE_GEM__FURY__IO``);
#: these dotted settings are not credentials: a mirror (a fetch source), a
#: gem's build options, a local override of a gem's code.
BUNDLE_SETTINGS: tuple[str, ...] = (
    "BUNDLE_BUILD__",
    "BUNDLE_LOCAL__",
    "BUNDLE_MIRROR__",
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
        # The same inside a refused family: CA bundles, client
        # certificates, TLS checks and proxy exceptions of git, npm, pip,
        # Yarn, conda, Bundler, uv and Cargo (Poetry's are a pattern:
        # ALLOWED_PREFIX_SUFFIXES).
        "BUNDLE_SSL_CLIENT_CERT",
        "BUNDLE_SSL_VERIFY_MODE",
        "CARGO_HTTP_CHECK_REVOKE",
        "CONDA_CLIENT_SSL_CERT",
        "CONDA_SSL_VERIFY",
        "GIT_PROXY_SSL_CAINFO",
        "GIT_PROXY_SSL_CERT",
        "GIT_PROXY_SSL_CERT_PASSWORD_PROTECTED",
        "GIT_SSL_CERT",
        "GIT_SSL_CERT_PASSWORD_PROTECTED",
        "NPM_CONFIG_CA",
        "NPM_CONFIG_CERT",
        "NPM_CONFIG_NOPROXY",
        "NPM_CONFIG_STRICT_SSL",
        "PIP_CLIENT_CERT",
        "PIP_TRUSTED_HOST",
        "UV_INSECURE_HOST",
        "UV_NATIVE_TLS",
        "YARN_CA_FILE_PATH",
        "YARN_ENABLE_STRICT_SSL",
        "YARN_HTTPS_CA_FILE_PATH",
        "YARN_HTTPS_CERT_FILE_PATH",
        "YARN_HTTPS_KEY_FILE_PATH",
        # TLS checks.
        "DOCKER_TLS_VERIFY",
        "GIT_SSL_NO_VERIFY",
        "NODE_TLS_REJECT_UNAUTHORIZED",
        "PGSSLMODE",
        # Where traffic goes: name resolution, Docker's daemon, and the Go
        # modules fetched straight from their repositories (private-module
        # credentials need it).
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "GONOPROXY",
        "GOPRIVATE",
        "HOSTALIASES",
        "LOCALDOMAIN",
        "RES_OPTIONS",
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
        # Settings of a refused family that run nothing: an environment's
        # name, telemetry and banners, a build type, host key checking.
        "ANSIBLE_HOST_KEY_CHECKING",
        "CMAKE_BUILD_PARALLEL_LEVEL",
        "CMAKE_BUILD_TYPE",
        "CMAKE_COLOR_DIAGNOSTICS",
        "CMAKE_EXPORT_COMPILE_COMMANDS",
        "CMAKE_GENERATOR",
        "CONDA_DEFAULT_ENV",
        "DOTNET_CLI_TELEMETRY_OPTOUT",
        "DOTNET_ENVIRONMENT",
        "DOTNET_NOLOGO",
        # libuv's thread pool, not uv's.
        "UV_THREADPOOL_SIZE",
    }
)
#: Allowed prefixes within a refused family: cargo's terminal output.
ALLOWED_PREFIXES: tuple[str, ...] = ("CARGO_TERM_",)
#: Allowed by prefix and suffix: Poetry's CA bundle and client certificate
#: for a repository (``POETRY_CERTIFICATES_<REPO>_CERT``,
#: ``POETRY_CERTIFICATES_<REPO>_CLIENT_CERT``).
ALLOWED_PREFIX_SUFFIXES: tuple[tuple[str, str], ...] = (
    ("POETRY_CERTIFICATES_", "_CERT"),
)
#: The suffix of every proxy variable (``HTTPS_PROXY``, ``no_proxy``,
#: ``npm_config_https_proxy``...).
PROXY_SUFFIX = "_PROXY"


def _credential_shaped(upper: str) -> bool:
    """Whether the upper-cased name, in a refused family, is a credential."""
    if upper.endswith(CREDENTIAL_SUFFIXES) or upper.startswith(CREDENTIAL_PREFIXES):
        return True
    return (
        upper.startswith("BUNDLE_")
        and "__" in upper[len("BUNDLE_") :]
        and not upper.startswith(BUNDLE_SETTINGS)
    )


def _code_hook(upper: str) -> bool:
    """Whether the upper-cased name is a known code hook."""
    if upper in CODE_HOOK_NAMES or upper.startswith(NEVER_EXEMPT_PREFIXES):
        return True
    if (
        upper in ALLOWED_NAMES
        or upper.endswith(PROXY_SUFFIX)
        or upper.startswith(ALLOWED_PREFIXES)
        or any(
            upper.startswith(prefix) and upper.endswith(suffix)
            for prefix, suffix in ALLOWED_PREFIX_SUFFIXES
        )
    ):
        return False
    if upper.endswith(CODE_HOOK_SUFFIXES) or (
        upper.endswith(FLAGS_SUFFIX) and not upper.endswith("_" + FLAGS_SUFFIX)
    ):
        return True
    return upper.startswith(CODE_HOOK_PREFIXES) and not _credential_shaped(upper)


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
    "ALLOWED_PREFIXES",
    "ALLOWED_PREFIX_SUFFIXES",
    "BUNDLE_SETTINGS",
    "CODE_ENV",
    "CODE_ENV_PREFIXES",
    "CODE_HOOK_NAMES",
    "CODE_HOOK_PREFIXES",
    "CODE_HOOK_SUFFIXES",
    "CREDENTIAL_PREFIXES",
    "CREDENTIAL_SUFFIXES",
    "ENV_NAME",
    "FLAGS_SUFFIX",
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
