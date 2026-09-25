"""Deployment feature gates, read once when an application is built.

These were module-level flags in ``orchestrator.main``; every value keeps its
environment variable, default and parsing. ``create_app()`` reads them at the
same moment the former module did (application construction, which the
entrypoint performs at import), so a rolling restart is still the way to change
them. Nothing in the application writes to a built ``DeploymentSettings``; a
test that needs another gate builds its own settings or replaces one field on
its own application's settings.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _enabled(name: str, default: str = "false") -> bool:
    return os.environ.get(name, default).lower() in ("true", "1", "yes")


@dataclass
class DeploymentSettings:
    #: Auto-assignment toggle (default on).
    auto_assign_enabled: bool
    #: Session admission reads the same default-off gate that renders the
    #: generic stateless executor Deployment. Helm supplies this explicitly to
    #: the orchestrator; an unset/local process preserves pinned behavior.
    stateless_session_enabled: bool
    #: Worker jobs remain pinned unless this independent admission gate is
    #: opened. Session-pool enablement is intentionally not sufficient: worker
    #: rollout and rollback have different safety gates and capacity
    #: requirements.
    stateless_worker_enabled: bool
    #: Worker-lane defaulting is subordinate to admission. If this is enabled
    #: while worker admission remains disabled, omitted root jobs silently stay
    #: pinned.
    stateless_worker_default_enabled: bool
    #: Gate-3 completion commands ship dark. Helm wires this value explicitly;
    #: local/tests may opt in without changing the legacy path.
    completion_commands_enabled: bool
    #: Reorders only newly accepted completion commands. The admission decision
    #: is persisted on the command row so a config flip or rolling restart
    #: cannot change the ordering contract of work already in flight. Requires
    #: ``completion_commands_enabled`` (the lifespan refuses the combination).
    completion_status_reorder_enabled: bool
    #: First-rollout safety fence for the interim dedicated Officer-pod owner.
    #: Read-only drift observation and the authorized manual recycle stay
    #: available while automatic drift/missing-pod mutation is dark.
    persistent_agent_reconciliation_enabled: bool
    #: Dark-by-default, admin-only deployed verification seam for the exact
    #: current Officer runtime binding; no lookup or hot-path work unless
    #: explicitly enabled through Helm.
    officer_runtime_verification_enabled: bool
    #: BP-01 release fence. The owner-facing control may ship while the wider
    #: unattended-release scorecard is open, but a stored/manual JSON edit must
    #: not make the money-spending tick live. ``false`` always stays writable so
    #: an operator can stand a century down during rollback or an incident.
    officer_auto_pull_release_enabled: bool
    #: Local-only crash-recovery proof hook. Production/chart defaults keep
    #: this at zero; a positive value makes the accept -> force-delete window
    #: deterministic.
    completion_finalizer_inline_delay_seconds: float

    @classmethod
    def from_environment(cls) -> DeploymentSettings:
        return cls(
            auto_assign_enabled=_enabled("AUTO_ASSIGN_ENABLED", "true"),
            stateless_session_enabled=_enabled("STATELESS_SESSION_ENABLED"),
            stateless_worker_enabled=_enabled("STATELESS_WORKER_ENABLED"),
            stateless_worker_default_enabled=_enabled(
                "STATELESS_WORKER_DEFAULT_ENABLED"
            ),
            completion_commands_enabled=_enabled("COMPLETION_COMMANDS_ENABLED"),
            completion_status_reorder_enabled=_enabled(
                "COMPLETION_STATUS_REORDER_ENABLED"
            ),
            persistent_agent_reconciliation_enabled=_enabled(
                "PERSISTENT_AGENT_RECONCILIATION_ENABLED"
            ),
            officer_runtime_verification_enabled=_enabled(
                "OFFICER_RUNTIME_VERIFICATION_ENABLED"
            ),
            officer_auto_pull_release_enabled=_enabled(
                "OFFICER_AUTO_PULL_RELEASE_ENABLED"
            ),
            completion_finalizer_inline_delay_seconds=max(
                0.0,
                float(os.environ.get("COMPLETION_FINALIZER_INLINE_DELAY_SECONDS", "0")),
            ),
        )


__all__ = ["DeploymentSettings"]
