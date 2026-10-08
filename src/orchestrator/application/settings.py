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

import logging
import os
from dataclasses import dataclass

from orchestrator.services.connector_drivers.workspace_ssh import (
    WORKSPACE_SSH_KNOWN_HOSTS_ENV,
)
from shared.run_queue import LANE_PINNED, LANE_STATELESS

logger = logging.getLogger(__name__)

#: The session lanes whose parents may fan out (parallel_subagents.md §12).
SESSION_SUBAGENT_FANOUT_LANES_ENV = "SESSION_SUBAGENT_FANOUT_LANES"
_SESSION_SUBAGENT_FANOUT_LANE_NAMES = frozenset({LANE_STATELESS, LANE_PINNED})


def _enabled(name: str, default: str = "false") -> bool:
    return os.environ.get(name, default).lower() in ("true", "1", "yes")


def parse_session_subagent_fanout_lanes(raw: str | None) -> frozenset[str]:
    """The lanes named by ``SESSION_SUBAGENT_FANOUT_LANES``.

    A comma-separated subset of ``stateless`` and ``pinned``, case and
    whitespace insensitive; unset or empty is off. An unknown name is ignored
    with a warning and never widens the set, so a typo fails closed.
    """
    lanes: set[str] = set()
    for token in (raw or "").split(","):
        name = token.strip().lower()
        if not name:
            continue
        if name in _SESSION_SUBAGENT_FANOUT_LANE_NAMES:
            lanes.add(name)
        else:
            logger.warning(
                "%s names unknown lane %r; ignoring it",
                SESSION_SUBAGENT_FANOUT_LANES_ENV,
                name,
            )
    return frozenset(lanes)


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
    #: Session lanes whose parents may run several ``delegate_agent`` calls
    #: from one response (parallel_subagents.md §12, D5; default none). The
    #: orchestrator evaluates it for the session's lane at every stateless
    #: claim and pinned attach and advertises the boolean beside the
    #: batch-settle capability, so it is never frozen into a session: turning
    #: a lane off reaches every stateless session at its next claim, while
    #: batches already in flight are still settled by the recovery path.
    session_subagent_fanout_lanes: frozenset[str] = frozenset()
    #: The deployment's default SSH host-key pins
    #: (``orchestrator.workspaceSshKnownHosts``): known_hosts lines
    #: an SSH connector without a pin of its own is checked against.
    #: Empty means such connectors trust a host on first use.
    workspace_ssh_known_hosts: str = ""

    def session_subagent_fanout(self, lane: str | None) -> bool:
        """Whether a session on ``lane`` may fan out right now."""
        return lane in self.session_subagent_fanout_lanes

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
            session_subagent_fanout_lanes=parse_session_subagent_fanout_lanes(
                os.environ.get(SESSION_SUBAGENT_FANOUT_LANES_ENV)
            ),
            workspace_ssh_known_hosts=os.environ.get(WORKSPACE_SSH_KNOWN_HOSTS_ENV, ""),
        )


__all__ = [
    "SESSION_SUBAGENT_FANOUT_LANES_ENV",
    "DeploymentSettings",
    "parse_session_subagent_fanout_lanes",
]
