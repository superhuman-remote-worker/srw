"""ENV credential contracts shared by the API and workspace runtime."""

from __future__ import annotations

from typing import Any

from shared.connectors.builtin import legacy_types_with_form, spec_for_type
from shared.connectors.env_names import (
    CONFIG_POINTER_ENV_NAMES,
    CONFIG_POINTER_ENV_PREFIXES,
    env_value_problem,
    points_a_tool_at_code,
    workspace_name_problem,
)

#: Stored types whose driver delivers an environment file to the workspace.
ENV_CONNECTOR_TYPES = legacy_types_with_form("env_file")


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
        problem = workspace_name_problem(name) or env_value_problem(name, secret)
        if problem is not None:
            raise ValueError(problem)
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
# variable in ``shared.connectors.env_names.CONFIG_POINTER_ENV_NAMES`` would
# make that file a config or code: a shared connector's file would become
# code. So a credential file may not name one.
#
# Environment connectors still reserve only the workspace's own names
# (:func:`normalize_credential_env`); whether they adopt this list, or the
# stricter one drivers keep (``env_names.driver_env_problem``), is the
# owner's decision (slices D1d and D6). The lists live in
# ``shared.connectors.env_names`` so every caller uses one copy.


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


__all__ = [
    "CONFIG_POINTER_ENV_NAMES",
    "CONFIG_POINTER_ENV_PREFIXES",
    "ENV_CONNECTOR_TYPES",
    "CredentialConnectorAttachedError",
    "collect_credential_env",
    "credential_file_env_problem",
    "normalize_credential_env",
    "points_a_tool_at_code",
]
