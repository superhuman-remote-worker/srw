"""Configuration write validation and override merge semantics.

These are the existing HTTP-compatible helpers, independent of application
startup. Merge copies only traversed mappings; None deletes a key and a list
replaces its predecessor. Do not substitute the loader's deep-copy merge.

:func:`looks_like_uuid` was moved here from ``orchestrator.main`` (R1.B05 lane
P). Its three unrelated callers — session config resolution, the expert catalog
service and the two tool-groups endpoints — all ask exactly one question:
"is this ``config_name`` slot actually holding an expert UUID?" That is the
same question :func:`validated_config_name` answers from the other side, so the
config-name vocabulary lives in one module rather than being re-derived beside
each caller.
"""

from typing import Any
from uuid import UUID

from fastapi import HTTPException

from orchestrator.services.agent_pod_entrypoint import (
    InvalidConfigNameError,
    validate_config_name,
)
from shared.orch_surface.jobs._utils import transport_key_paths


def refuse_caller_transport_keys(config_override: Any) -> None:
    """422 if a caller-authored ``config_override`` pins transport/credentials.

    The single write-boundary vocabulary — shared by REST job create, its
    bypass layers (automations, bench) and any other funnel that hands a
    caller-authored override to ``db.create_job`` / ``admit_job`` — mirroring
    the MCP create tool (``shared.orch_surface.jobs.control``). Routing is
    resolved server-side from the model ID and credentials are injected only
    in-flight at dispatch, so a pinned ``base_url`` / ``api_key`` / ``env_keys``
    (or ``*_api_key``) would otherwise have the deployment's stored key sent to
    a caller-chosen endpoint. A self-hosted model is routed through its catalog
    endpoint (Admin -> Models), never an inline transport key.
    """
    offending = transport_key_paths(config_override)
    if offending:
        raise HTTPException(
            status_code=422,
            detail=(
                "config_override may not set credential or transport keys ("
                + ", ".join(sorted(offending))
                + "). Routing is resolved server-side from the model ID — pass "
                '{"llm": {"model": "<id>"}} and pin any custom endpoint in the '
                "model catalog (Admin -> Models)."
            ),
        )


def deep_merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep merge two dicts. Override wins for scalars/lists; dicts merge recursively."""
    result = base.copy()
    for key, value in override.items():
        if value is None:
            result.pop(key, None)
        elif isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def validated_config_name(config_name: str | None) -> str | None:
    """Return a caller-supplied ``config_name``, or 422 naming the rule it broke.

    ``config_name`` is the one caller-controlled word in the agent pod's
    ``sh -c`` entrypoint. Both provisioners re-check it at their own boundary
    (``services/agent_pod_entrypoint.validate_config_name``, security audit
    2026-08-27 finding #3), but they are reached from fire-and-forget tasks and
    from rows read back long after the request that wrote them — a hostile
    value that gets *persisted* explodes on every later resume, recycle and
    magic-link wake, with no request left to answer. So the allow-list also
    runs here, on WRITE, exactly like ``_with_validated_tool_overrides``: the
    caller gets one clean 422 and the row is never created.

    Deliberately NOT applied to values read back out of the database. A row
    poisoned before this guard existed must still be listable, resumable-to-a
    -clear-failure and deletable; it fails loudly at its provisioning attempt
    instead (see the fire-and-forget handlers in this module).
    """
    try:
        return validate_config_name(config_name)
    except InvalidConfigNameError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def looks_like_uuid(value: Any) -> bool:
    """True if ``value`` parses as a UUID (a cockpit-conflated expert id in the
    config_name slot, which must not be treated as a config file name)."""
    try:
        UUID(str(value))
        return True
    except (ValueError, TypeError, AttributeError):
        return False
