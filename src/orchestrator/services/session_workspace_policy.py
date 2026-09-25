"""Allowed saved/default and explicit-create session workspace tiers."""

# Session workspace tiers allowed as saved defaults, and
# the platform default applied when neither the request nor the owner's saved
# settings.persistent_agent.workspace_backend pick one. An S3 object store is
# an assumed platform prerequisite (knowledge-history/done/s3_object_store_bundled_fallback.md),
# so the default is the instant lite tier — see
# knowledge-base/knowledge/features/instant_landing_session.md.
SESSION_WORKSPACE_BACKENDS = ("sandbox", "virtual", "none")
SESSION_DEFAULT_WORKSPACE_BACKEND = "virtual"

# Backends a caller may *explicitly* select at session creation. ``vm`` is
# creatable (operator-gated + provisioned via KubeVirt, see create_thread) but
# deliberately NOT in SESSION_WORKSPACE_BACKENDS: it must never be an implicit
# or saved default (a KubeVirt VM per session is expensive), so it is a
# per-session opt-in only and is excluded from the default chain
# (_default_session_workspace_backend) and the settings-PATCH validator.
SESSION_CREATE_WORKSPACE_BACKENDS = SESSION_WORKSPACE_BACKENDS + ("vm",)


# ---------------------------------------------------------------------------
# Resolving and validating a session's tier, moved verbatim from
# ``orchestrator.main`` (R1.B05 lane P). Both functions read the constants
# above, which is why they live beside them rather than in
# ``workspace_tier_policy`` (the job/session-shared reader):
# ``default_session_workspace_backend`` walks the *default chain* that excludes
# ``vm``, and ``validated_session_workspace_override`` validates against the
# *create* set that includes it. Keeping the two sets and the two functions in
# one module is what stops ``vm`` from leaking into a saved default.
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


def default_session_workspace_backend(user_settings: dict[str, Any] | None) -> str:
    """The owner's saved default session tier, else the platform default.

    ``user_settings`` is the ``settings.persistent_agent`` sub-object. Unknown
    or absent values fall back to the platform default rather than erroring —
    the PATCH validator (``UserSettingsUpdate``) keeps stored values sane, this
    just guards legacy/hand-edited rows.
    """
    backend = (user_settings or {}).get("workspace_backend")
    if backend in SESSION_WORKSPACE_BACKENDS:
        return backend
    return SESSION_DEFAULT_WORKSPACE_BACKEND


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
