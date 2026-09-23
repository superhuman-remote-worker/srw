"""Owner-facing projection of persistent-thread rows (R1.B10).

A thread row carries credentials, internal lifecycle capabilities and immutable
cleanup evidence next to the fields a Cockpit renders. Every REST surface that
returns a thread row passes it through :func:`redact_thread_metadata`; replayed
history passes its stored tool calls through :func:`stamp_tool_categories`.

Both functions are pure: no database, network or application state.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from orchestrator.security.access import redact_config_override
from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_INCARNATION_KEY,
)
from orchestrator.services.job_projection import redact_nested_workspace_state
from shared.tool_catalog import TOOL_REGISTRY


def redact_thread_metadata(thread: dict[str, Any]) -> dict[str, Any]:
    """Parse and strip credential fields from a thread's ``metadata`` before
    it leaves over REST.

    ``metadata`` is a JSONB column asyncpg hands back as a JSON *string*.
    This helper used to "re-serialize to the original representation", which
    meant the owner-facing thread endpoints returned metadata as a string —
    silently breaking every Cockpit consumer typed against
    ``metadata?: Record<string, unknown>`` (settings-pane config/tools
    prefill, the attached-datasource default, and the REST model/temperature
    seeding — the long-standing "model shows the config name until the
    welcome frame" oddity). The contract is now: metadata always leaves as a
    parsed OBJECT (unparseable/absent → ``{}``).
    """
    raw_retirement_context = thread.get("runtime_retirement_context") or {}
    if isinstance(raw_retirement_context, str):
        try:
            raw_retirement_context = json.loads(raw_retirement_context)
        except (json.JSONDecodeError, TypeError):
            raw_retirement_context = {}
    # The token is installed before abortable turn/Officer preflight.  Only
    # the append-only authorized edge is a public `ending` state; exposing the
    # hidden preflight would make Cockpit retire control even when a non-force
    # End is about to abort as an observational no-op.
    retirement_pending = bool(
        thread.get("runtime_retirement_token") is not None
        and thread.get("runtime_retirement_authorized_at") is not None
    )
    retirement_disposition: str | None = None
    if retirement_pending and isinstance(raw_retirement_context, Mapping):
        candidate = str(raw_retirement_context.get("settle_status") or "")
        if candidate in {"ended", "suspended"}:
            retirement_disposition = candidate

    thread = redact_nested_workspace_state(
        thread,
        field="metadata",
        runtime_incarnation_key=WORKSPACE_RUNTIME_INCARNATION_KEY,
    )
    md = thread.get("metadata")
    if isinstance(md, str):
        try:
            md = json.loads(md)
        except (json.JSONDecodeError, TypeError):
            md = {}
    if not isinstance(md, dict):
        md = {}
    thread = dict(thread)
    md = dict(md)
    if "config_override" in md:
        md["config_override"] = redact_config_override(md["config_override"])
    md.pop("_workspace_binding", None)
    md.pop("_stateless_workspace_process_zero_observation", None)
    thread["metadata"] = md
    # These are internal capabilities or immutable physical cleanup evidence,
    # not owner API fields.  Never let a broad SELECT * list/detail response
    # leak them.  Cockpit gets only the durable, non-secret lifecycle shape.
    for internal_key in (
        "runtime_generation",
        "runtime_attach_token",
        "runtime_attach_abort_receipt",
        "runtime_authority_exposed",
        "runtime_retirement_token",
        "runtime_retirement_permanent",
        "runtime_retirement_started_at",
        "runtime_retirement_authorized_at",
        "runtime_retirement_context",
        "runtime_retirement_stage_receipt",
        "runtime_retirement_local_quiescence",
        "runtime_retirement_external_cleanup",
    ):
        thread.pop(internal_key, None)
    thread["runtime_retirement_pending"] = retirement_pending
    thread["retirement_disposition"] = retirement_disposition
    return thread


def stamp_tool_categories(messages: list[dict[str, Any]]) -> None:
    """Annotate replayed tool calls with their registry category, in place.

    The live SSE ``tool.started`` frame carries ``category`` (see graph.py's
    ``_get_tool_category``), but the stored ``thread_messages.tool_calls`` JSONB
    never did. Without this the cockpit's folded-chip summary buckets every
    replayed call as "other", so one turn reads
    "19× citations · 12× searches" while streaming and "38× steps" after a
    reload — same turn, same data, different answer.

    Derived at read time rather than persisted so that re-categorising a tool
    doesn't need a backfill of historical rows. Unknown tools (renamed, removed,
    or from another deployment) simply get no category and fall back to the
    cockpit's "other" bucket, which is the honest answer.
    """
    for m in messages:
        for tc in m.get("tool_calls") or []:
            category = TOOL_REGISTRY.get(tc.get("name") or "", {}).get("category")
            if category:
                tc["category"] = category


__all__ = ["redact_thread_metadata", "stamp_tool_categories"]
