"""Allowed explicit-create session workspace tiers."""

# Session workspace tiers a caller may pick implicitly (request or, formerly,
# a saved default). Slice A2b retired the saved-default preference
# (settings.persistent_agent.workspace_backend): an unpinned Session's tier
# now comes from the workspace defaults chain
# (orchestrator.services.workspace_defaults_resolution), not from here. This
# tuple stays as the non-vm base for SESSION_CREATE_WORKSPACE_BACKENDS below.
SESSION_WORKSPACE_BACKENDS = ("sandbox", "virtual", "none")

# Backends a caller may *explicitly* select at session creation. ``vm`` is
# creatable (operator-gated + provisioned via KubeVirt, see create_thread) but
# deliberately NOT in SESSION_WORKSPACE_BACKENDS: a KubeVirt VM per session is
# expensive, so it is a per-session opt-in only, never implicit.
SESSION_CREATE_WORKSPACE_BACKENDS = SESSION_WORKSPACE_BACKENDS + ("vm",)


# ---------------------------------------------------------------------------
# Validating a session's tier at create time, moved verbatim from
# ``orchestrator.main`` (R1.B05 lane P). ``validated_session_workspace_override``
# reads the constants above, which is why it lives beside them rather than in
# ``workspace_tier_policy`` (the job/session-shared reader): it validates
# against the *create* set, which includes ``vm``.
#
# ``session_ready_timeout_s`` is here for the same reason: the budget is a
# per-tier property of a session, and the VM branch exists because a KubeVirt
# cold boot is minutes, not seconds.
# ---------------------------------------------------------------------------

import os  # noqa: E402
from typing import Any, Optional  # noqa: E402

from fastapi import HTTPException  # noqa: E402

from orchestrator.services.config_overrides import (  # noqa: E402
    refuse_execution_owned_workspace_keys,
)


def validated_session_workspace_override(
    config_override: Any,
) -> Optional[dict[str, Any]]:
    """Extract + validate the ``workspace`` sub-dict from a New Session request's
    ``config_override`` (the cockpit 'Backend' selector + Advanced→Workspace
    fragment). Returns the workspace dict for ``create_thread`` to merge, or
    ``None`` when no workspace fragment was sent.

    ``create_thread`` provisions a lite tier (no pod), a sandbox container, or —
    when the caller explicitly selects it and passes the operator gate — a
    KubeVirt ``vm`` (see the VM branch in ``create_thread``'s provisioning fork).
    ``vm`` is accepted here but validated against
    ``SESSION_CREATE_WORKSPACE_BACKENDS`` (not the default-chain set) so it stays
    a per-session opt-in; the operator gate (``_check_vm_permission``) and the
    ``vm_workspace`` PDP grant are enforced downstream in ``create_thread``.
    Unknown backends are rejected. A workspace fragment with no ``backend`` (e.g.
    word-limit tweaks only) passes through untouched, and the VM sizing sub-dict
    (``vm.{cpu_cores,memory}``) rides along via the caller's merge.

    Raises ``HTTPException(422)`` if the caller's ``workspace`` fragment sets
    ``container`` or ``sandbox`` directly — those are execution-owned, filled
    only from the selected WorkspaceTemplate. Raises ``HTTPException(400)`` on
    a disallowed/invalid backend.
    """
    refuse_execution_owned_workspace_keys(config_override)
    ws = config_override.get("workspace") if isinstance(config_override, dict) else None
    if not isinstance(ws, dict) or not ws:
        return None
    backend = ws.get("backend")
    if backend is not None and backend not in SESSION_CREATE_WORKSPACE_BACKENDS:
        raise HTTPException(
            status_code=400, detail=f"Invalid workspace backend '{backend}'"
        )
    return ws


def session_ready_timeout_s(
    backend: Optional[str], *, preparation: bool = False
) -> int:
    """Readiness-probe budget for the session-start paths (``provision_or_assign``
    and ``_do_prepare``'s ``wait_for_ready``).

    A ``vm`` tier pays a cold KubeVirt CDI import + guest boot (minutes) far
    beyond the sandbox-container default, so VM-backed sessions get a much larger
    budget; every other tier keeps the fast default. Both are env-tunable. Sized
    just above the agent's own VM attach-poll budget (``VM_UPGRADE_POLL_TIMEOUT``,
    900 s) so the agent gives up first with the truthful reason.
    """
    if backend == "vm":
        budget = int(os.environ.get("VM_WS_READY_TIMEOUT_S", "960"))
        if preparation:
            from shared.workspace_preparation_settings import PreparationSettings

            budget += PreparationSettings.from_environment().wait_budget
        return budget
    return int(os.environ.get("WS_READY_TIMEOUT_S", "180"))


# Every agent Pod's startup allowance before any session attach: process
# start, agent initialization and registration (the startup probe's 100 s).
AGENT_POD_STARTUP_ALLOWANCE_S = 100


def session_pod_startup_allowance_s(thread: Any) -> int:
    """Startup-probe allowance for an agent Pod bound to ``thread`` at creation.

    A dedicated agent now serves ``/health`` after bounded initialization and
    registration while its lifecycle-owned attach task waits for a VM. Keep
    this finite allowance as a process-start safety margin and for mixed-version
    agents; session readiness is observed separately with the exact source-aware
    budget (knowledge-base/knowledge/issues/
    dedicated_vm_session_ended_at_attach_by_startup_probe.md).
    """
    import json

    from orchestrator.services.stateless_workspace_gate import (
        declared_thread_workspace_backend,
        thread_metadata_object,
    )

    metadata = thread_metadata_object(thread)
    config_override = metadata.get("config_override") or {}
    if isinstance(config_override, str):
        try:
            config_override = json.loads(config_override)
        except (json.JSONDecodeError, TypeError):
            config_override = {}
    if not isinstance(config_override, dict):
        config_override = {}
    vm = metadata.get("vm")
    return AGENT_POD_STARTUP_ALLOWANCE_S + session_ready_timeout_s(
        declared_thread_workspace_backend(thread),
        preparation=bool(
            preparation_wait_budget(
                config_override, vm=vm if isinstance(vm, dict) else None
            )
        ),
    )


def preparation_wait_budget(config_override, vm=None):
    """Additional bounded startup time for an execution-owned preparation."""
    workspace = (config_override or {}).get("workspace") or {}
    if not (
        (vm or {}).get("preparation_request")
        or (workspace.get("vm") or {}).get("preparation")
    ):
        return 0
    from shared.workspace_preparation_settings import PreparationSettings

    return PreparationSettings.from_environment().wait_budget
