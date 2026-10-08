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
