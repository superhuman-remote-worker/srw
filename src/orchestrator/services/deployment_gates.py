"""Deployment feature gates read from the process environment.

Extracted verbatim from ``orchestrator.main`` (R1.B05 lane P, census group
``R_POLICY``). Every function here answers one question about how *this*
deployment is configured, from ``os.environ`` alone — no store, no logger, no
client. That purity is the point: they are read on request paths, in dispatch,
in admission gates and in the capabilities endpoint, and the answer must be the
live environment rather than a value frozen at import.

Two properties are load-bearing and moved unchanged:

* **Each gate keeps its own default.** ``EXPERTS_DB_ENABLED`` and
  ``REQUIRE_PINNED_STATUS_IDENTITY`` default to ``"true"``;
  ``DATASOURCE_DEFAULTS_ON_OMISSION`` and
  ``DATASOURCE_SCOPE_AUTO_ATTACH_V1_ENABLED`` default to ``"false"``; the rest
  default to the empty string, which is off. A uniform default would silently
  flip four deployments' behaviour.
* **The truthy vocabulary is exactly ``("true", "1", "yes")``** after
  ``.lower().strip()``, so ``"TRUE"`` and ``" yes "`` are on and ``"on"`` is
  not. The vocabulary is written out per function rather than shared, because
  that is how each was written and read.

``os.getenv`` is called on every invocation, so a test that sets the variable
with ``monkeypatch.setenv`` steers the next call. Nothing is cached.
"""

from __future__ import annotations

import os


def is_experts_db_enabled() -> bool:
    """True when DB-backed experts / orchestrator-resolved config is on (env).

    Gates whether the orchestrator resolves the full config at dispatch/attach
    and emits a ``resolved_config`` blob. Off → the agent uses its ``from_config``
    fallback (emergency compatibility path). Enabled by default because root
    creation depends on persisted application expert pointers.
    """
    return os.getenv("EXPERTS_DB_ENABLED", "true").lower().strip() in (
        "true",
        "1",
        "yes",
    )


def is_skills_db_enabled() -> bool:
    """True when DB-backed Agent Skills are on (env). Dev on / prod off (helm
    ``skillsDbEnabled``). Mirrors ``EXPERTS_DB_ENABLED``."""
    return os.getenv("SKILLS_DB_ENABLED", "").lower().strip() in ("true", "1", "yes")


def mcp_datasources_enabled() -> bool:
    """Whether user-added MCP server datasources are enabled."""
    return os.getenv("MCP_DATASOURCES_ENABLED", "").lower().strip() in (
        "true",
        "1",
        "yes",
    )


def datasource_defaults_on_omission() -> bool:
    """Temporary rollout gate for the root REST omission contract.

    Cockpit always submits a reviewed explicit array and internal schedulers
    call the default policy directly.  This gate protects older REST clients
    that historically encoded "none" by omitting ``datasource_ids``.
    """
    return os.getenv("DATASOURCE_DEFAULTS_ON_OMISSION", "false").lower().strip() in (
        "true",
        "1",
        "yes",
    )


def datasource_scope_auto_attach_v1_enabled() -> bool:
    """Coordinated rollout gate for the project-scope/auto-attach UI.

    Keep this default-off until every API replica understands the additive
    datasource contract; the Cockpit reads the corresponding capability bit
    and leaves the new management workflow hidden while a rollout is mixed.
    """
    return os.getenv(
        "DATASOURCE_SCOPE_AUTO_ATTACH_V1_ENABLED", "false"
    ).lower().strip() in ("true", "1", "yes")


def mcp_stdio_enabled() -> bool:
    """Whether MCP datasources may execute local stdio server commands."""
    return os.getenv("MCP_STDIO_ENABLED", "").lower().strip() in (
        "true",
        "1",
        "yes",
    )


def is_protected_cloud_mode_enabled() -> bool:
    """Whether protected cloud mode (RO-reader provisioning + capture overlay)
    is enabled for this deployment. Dev-ON / prod-OFF via the helm
    ``agent.protectedCloudModeEnabled`` flag
    (knowledge-base/knowledge/design/cloud_access_unification.md §8 Phase 1)."""
    return os.getenv("PROTECTED_CLOUD_MODE_ENABLED", "").lower().strip() in (
        "true",
        "1",
        "yes",
    )


def stateless_idle_conversation_rewind_enabled() -> bool:
    """Whether new idle stateless conversation rewinds may be admitted."""

    return os.getenv(
        "SESSION_REWIND_IDLE_CONVERSATION_ENABLED", "false"
    ).lower().strip() in ("true", "1", "yes")


def require_pinned_status_identity() -> bool:
    """Require exact pinned lifecycle identity unless explicitly disabled."""

    return os.getenv("REQUIRE_PINNED_STATUS_IDENTITY", "true").lower().strip() in (
        "true",
        "1",
        "yes",
    )


__all__ = [
    "datasource_defaults_on_omission",
    "datasource_scope_auto_attach_v1_enabled",
    "is_experts_db_enabled",
    "is_protected_cloud_mode_enabled",
    "is_skills_db_enabled",
    "mcp_datasources_enabled",
    "mcp_stdio_enabled",
    "require_pinned_status_identity",
    "stateless_idle_conversation_rewind_enabled",
]
