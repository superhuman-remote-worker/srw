"""A session's frozen configuration, as its owner's settings pane displays it.

Session creation freezes the complete rendered configuration in the session's
execution snapshot (``srw_execution_specs``, ``srw/resolved-config-v1``), and
every settings update publishes a new generation of it. This read serves that
current generation so the live pane can show the real values of settings that
can no longer change. It never re-resolves anything: a historical session
without a snapshot answers ``legacy`` with no configuration.

The value is the blob the next attach delivers, minus what attach adds in
flight (credentials, workspace endpoints, connector tool bindings). It gets
the same credential redaction as ``jobs.resolved_config`` and additionally
drops every transport key, so no endpoint or secret crosses this read.
"""

from __future__ import annotations

from typing import Any, Literal

from fastapi import HTTPException

from orchestrator.security.config_redaction import (
    redact_config_override,
    redact_public_config_override,
)
from orchestrator.services.manifest_execution_snapshot import (
    read_execution,
    srw_snapshot_config,
)
from orchestrator.services.stateless_workspace_gate import thread_metadata_object

ResolvedConfigSource = Literal["snapshot", "legacy", "unavailable"]

# Where a request is sent, rather than what it does: never shown, even unset.
_TRANSPORT_KEYS = frozenset({"base_url", "env_keys"})
_TRANSPORT_SUFFIX = "_base_url"


def _without_transport(value: Any, parent: str | None = None) -> Any:
    if isinstance(value, dict):
        return {
            key: _without_transport(child, str(key))
            for key, child in value.items()
            if str(key).lower() not in _TRANSPORT_KEYS
            and not str(key).lower().endswith(_TRANSPORT_SUFFIX)
            and not (parent == "workspace" and key in ("remote", "mounts"))
        }
    if isinstance(value, list):
        return [_without_transport(child, parent) for child in value]
    return value


def owner_config_view(blob: dict[str, Any]) -> dict[str, Any]:
    """The frozen blob without credentials or transport, for its owner."""
    return _without_transport(
        redact_public_config_override(redact_config_override(blob))
    )


def _expert_based_on(metadata: dict[str, Any]) -> str | None:
    value = metadata.get("expert_based_on")
    return value if isinstance(value, str) and value else None


async def read_session_resolved_config(
    store: Any, thread: dict[str, Any]
) -> dict[str, Any]:
    """``{thread_id, source, config, expert_based_on}`` for an owner-gated thread.

    ``source`` is ``snapshot`` when the frozen generation was read,
    ``legacy`` for a session created before snapshots (``config`` is null and
    nothing is re-resolved), and ``unavailable`` when a snapshot exists but is
    not the reference harness's readable format (``config`` is null).
    """
    thread_id = str(thread["id"])
    metadata = thread_metadata_object(thread)
    result: dict[str, Any] = {
        "thread_id": thread_id,
        "source": "legacy",
        "config": None,
        "expert_based_on": _expert_based_on(metadata),
    }
    execution = await read_execution(store, "Session", thread_id)
    if execution is None:
        return result
    try:
        blob, _policy = srw_snapshot_config(execution)
    except (HTTPException, KeyError, TypeError, ValueError):
        result["source"] = "unavailable"
        return result
    # Ordered controls are materialized on the thread row and win at attach
    # (resolve_session_config); show the values the next attach delivers.
    interactive = {
        "permission_mode": str(thread.get("permission_mode") or "supervised")
    }
    if thread.get("narration_mode") is not None:
        interactive["narration_mode"] = str(thread["narration_mode"])
    agent = blob.get("agent")
    if isinstance(agent, dict):
        agent["interactive"] = {**(agent.get("interactive") or {}), **interactive}
    result["source"] = "snapshot"
    result["config"] = owner_config_view(blob)
    return result


__all__ = [
    "ResolvedConfigSource",
    "owner_config_view",
    "read_session_resolved_config",
]
