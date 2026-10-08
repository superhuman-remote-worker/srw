"""ENV credential contracts shared by the API and workspace runtime."""

from __future__ import annotations

import re
from typing import Any

from shared.connectors.builtin import legacy_types_with_form, spec_for_type

#: Stored types whose driver delivers an environment file to the workspace.
ENV_CONNECTOR_TYPES = legacy_types_with_form("env_file")
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESERVED = frozenset(
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


class CredentialConnectorAttachedError(ValueError):
    """Credentials already attached to work cannot be detached in v1."""


def normalize_credential_env(value: Any, *, required: bool = False) -> dict[str, str]:
    """Validate values without including any secret in error messages."""
    if not isinstance(value, dict):
        raise ValueError("Environment variables must be a name/value object")
    if required and not value:
        raise ValueError("Add at least one credential environment variable")
    if len(value) > 100:
        raise ValueError("A connector supports at most 100 environment variables")
    result: dict[str, str] = {}
    for name, secret in value.items():
        if not isinstance(name, str) or not _ENV_NAME.fullmatch(name):
            raise ValueError(
                "Environment names must contain letters, digits or underscores and cannot start with a digit"
            )
        if name in _RESERVED or name.startswith(("SRW_", "LD_", "DYLD_", "PYTHON")):
            raise ValueError(f"Environment name {name} is reserved by the workspace")
        if not isinstance(secret, str) or "\x00" in secret:
            raise ValueError(
                f"Environment variable {name} must be a string without NUL bytes"
            )
        if len(secret.encode("utf-8")) > 65536:
            raise ValueError(f"Environment variable {name} exceeds 64 KiB")
        result[name] = secret
    return result


def collect_credential_env(datasources: list[dict[str, Any]]) -> dict[str, str]:
    """Collect one unambiguous environment for the current work item."""
    result: dict[str, str] = {}
    for ds in datasources:
        if ds.get("type") not in ENV_CONNECTOR_TYPES:
            continue
        values = normalize_credential_env(
            (ds.get("credentials") or {}).get("env_vars", {}),
            required=_env_vars_required(ds.get("type")),
        )
        for name, secret in values.items():
            if name in result:
                raise ValueError(f"Multiple attached connectors define {name}")
            result[name] = secret
    return result


def _env_vars_required(ds_type: Any) -> bool:
    """Whether the type's driver requires its environment slot (credentials)."""
    spec = spec_for_type(ds_type)
    return spec is not None and any(
        slot.name == "env_vars" and slot.required for slot in spec.credential_slots
    )


# ---------------------------------------------------------------------------
# Variables that point a tool at a config, start-up or code file
# ---------------------------------------------------------------------------
#
# A credential file's ``env_var`` is set to the stored file's path. Naming a
# variable below would make that file a config git, pip, npm, a shell, an
# editor, a pager or the dynamic loader reads, or code Python, Node, Perl,
# Ruby or the JVM loads: a shared connector's file would become code. So a
# credential file may not name one.
#
# Shared on purpose: environment connectors still reserve only ``_RESERVED``
# and the prefixes in :func:`normalize_credential_env`; whether they adopt
# this list too is the owner's decision (slice D1d review). It lives here so
# both can use one list.
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


def credential_file_env_problem(name: str) -> str | None:
    """Why a credential file's ``env_var`` is refused (``None``: it is not).

    The workspace's reserved names (as an environment connector's), the
    kubeconfig merge's ``KUBECONFIG``, and every variable that points a tool
    at a config or code file (:data:`CONFIG_POINTER_ENV_NAMES`).
    """
    try:
        normalize_credential_env({name: ""})
    except ValueError as exc:
        return str(exc)
    if name == "KUBECONFIG":
        return "KUBECONFIG is reserved: it names the merged kubeconfig"
    if points_a_tool_at_code(name):
        return (
            f"{name} is reserved: it would point a tool at the file as a config or code"
        )
    return None
