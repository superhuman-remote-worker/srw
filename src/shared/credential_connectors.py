"""ENV credential contracts shared by the API and workspace runtime.

An environment connector's names follow the one rule every connector does
(``shared.connectors.env_names.connector_env_problem``): no SRW reserved
name and no known code hook. The orchestrator refuses a name when the
connector is saved (:func:`normalize_credential_env`); a row saved before the
rule is delivered without it, the refused names reported
(:func:`split_credential_env`).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from shared.connectors.builtin import legacy_types_with_form, spec_for_type
from shared.connectors.env_names import connector_env_problem, env_value_problem

#: Stored types whose driver delivers an environment file to the workspace.
ENV_CONNECTOR_TYPES = legacy_types_with_form("env_file")


class CredentialConnectorAttachedError(ValueError):
    """Credentials already attached to work cannot be detached in v1."""


def _variables(value: Any, *, required: bool) -> Mapping[Any, Any]:
    """``value`` as a set of variables, or ``ValueError`` for its shape."""
    if not isinstance(value, dict):
        raise ValueError("Environment variables must be a name/value object")
    if required and not value:
        raise ValueError("Add at least one credential environment variable")
    if len(value) > 100:
        raise ValueError("A connector supports at most 100 environment variables")
    return value


def normalize_credential_env(value: Any, *, required: bool = False) -> dict[str, str]:
    """Validate values without including any secret in error messages."""
    result: dict[str, str] = {}
    for name, secret in _variables(value, required=required).items():
        problem = connector_env_problem(name) or env_value_problem(name, secret)
        if problem is not None:
            raise ValueError(problem)
        result[name] = secret
    return result


def split_credential_env(
    value: Any, *, required: bool = False
) -> tuple[dict[str, str], dict[str, str]]:
    """What a stored set delivers: ``(values, refused)``.

    ``refused`` maps each name no connector may set to why, for a row saved
    before the rule; the caller skips and reports it. A shape no delivery
    can take, or a value the workspace refuses, is a ``ValueError`` as in
    :func:`normalize_credential_env`.
    """
    values: dict[str, str] = {}
    refused: dict[str, str] = {}
    for name, secret in _variables(value, required=required).items():
        problem = connector_env_problem(name)
        if problem is not None:
            refused[str(name)] = problem
            continue
        problem = env_value_problem(name, secret)
        if problem is not None:
            raise ValueError(problem)
        values[name] = secret
    return values, refused


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


__all__ = [
    "ENV_CONNECTOR_TYPES",
    "CredentialConnectorAttachedError",
    "collect_credential_env",
    "normalize_credential_env",
    "split_credential_env",
]
