"""Pure raw-ingress sanitation shared by job validation and admission.

These fields can only be minted after authoritative repository/Officer/workspace
resolution. Transport authentication does not make submitted JSON authoritative.
Application state and public/internal caller policy remain with admission.

The two ``strip_*`` funnels below were moved verbatim from
``orchestrator.main`` (R1.B05 lane P, census group ``R_GRANTS``) so that the
three overlapping "a caller may not send this" sets live in one file and can be
read against each other: ``_SERVER_OWNED_RAW_CREATE_CONTEXT_KEYS`` (public AND
internal raw bodies), ``PUBLIC_JOB_CONTEXT_RESERVED_KEYS`` and
``PUBLIC_JOB_CONFIG_RESERVED_KEYS`` (public bodies only). They are deliberately
different sets, not one set with exceptions, and both funnels REBUILD the
dictionary rather than mutating it key by key — the pydantic model field is
reassigned, so a caller holding the original mapping cannot observe a
half-stripped state.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    # ``orchestrator.schemas.job_create`` imports THIS module, so the model is
    # referenced by name at runtime and imported only for type checking.
    from orchestrator.schemas.job_create import JobCreate

from shared.operator_pause_hold import (
    LAST_OPERATOR_PAUSE_HOLD_CONTEXT_KEY,
    OPERATOR_PAUSE_HOLD_CONTEXT_KEY,
)
from shared.workspace_contract import (
    WORKSPACE_CONTRACT_CONTEXT_KEY,
    WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY,
    WORKSPACE_RUNTIME_CONTEXT_KEY,
)


_SERVER_OWNED_OFFICER_CONTEXT_KEYS = {
    "ticket_note_id",
    "officer_admission",
    "ticket_ready_at",
    "ready_generation_at",
    "ticket_claim_source",
    "claim_source",
    "officer_thread_id",
    "officer_incarnation",
    "provisioning_preflight",
}
_SERVER_OWNED_REPOSITORY_CONTEXT_KEYS = {
    "git_remote_url",
    "repo_name",
    "managed_repository_credentials",
    "managed_repository_authority",
    "repository_auth",
    "repository_credentials",
    "_managed_repository_authority_pending",
    "_managed_repository_process_zero",
    "_stateless_workspace_process_zero_observation",
}
_SERVER_OWNED_RAW_CREATE_CONTEXT_KEYS = (
    _SERVER_OWNED_OFFICER_CONTEXT_KEYS
    | _SERVER_OWNED_REPOSITORY_CONTEXT_KEYS
    | {
        "evidence_manifest",
        "pull_request",
        "deliverable_contract_provenance",
        "prior_deliverable_contract",
        "required_pr_repositories",
        "required_deliverables",
        WORKSPACE_CONTRACT_CONTEXT_KEY,
        WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY,
        WORKSPACE_RUNTIME_CONTEXT_KEY,
        "workspace_backend",
        "vm",
        "workspace_container",
        # Only a public pause mints the hold and only an explicit resume lifts
        # it; a seeded one would park a job (or an agent's child) forever.
        OPERATOR_PAUSE_HOLD_CONTEXT_KEY,
        LAST_OPERATOR_PAUSE_HOLD_CONTEXT_KEY,
    }
)


def _strip_raw_repository_authority(value: Any) -> Any:
    """Recursively remove server-owned Git transport from request JSON."""

    if isinstance(value, dict):
        return {
            key: _strip_raw_repository_authority(item)
            for key, item in value.items()
            if key not in _SERVER_OWNED_REPOSITORY_CONTEXT_KEYS
        }
    if isinstance(value, list):
        return [_strip_raw_repository_authority(item) for item in value]
    return value


PUBLIC_JOB_CONTEXT_RESERVED_KEYS = {
    "automation_id",
    "automation_name",
    "automation_trigger",
    "cloud_baseline",
    "delegation_results",
    "datasource_selection",
    "delegation_timed_out",
    "git_remote_url",
    "graft_output_path",
    # Completion finalization is the only writer. Accepting this manifest at
    # creation turns the Gitea service into a confused deputy because its
    # repository/revision coordinates would otherwise arrive in caller data.
    "evidence_manifest",
    # Pull-request evidence and immutable contract provenance are minted only
    # by server-owned repository/admission paths.
    "pull_request",
    "deliverable_contract_provenance",
    "prior_deliverable_contract",
    "required_pr_repositories",
    "required_deliverables",
    "lifecycle_marker",
    "loop_campaign_id",
    "loop_campaign_index",
    "loop_id",
    "loop_iteration",
    "loop_remaining",
    "loop_role",
    "loop_seq_index",
    "parent_job_id",
    "runner_kind",
    "runner_source",
    "scholar_target",
    "snapshot",
    # The verification ledger is server-owned end to end: the server assigns
    # finding ids, computes the verdict from the open set, and reads the round
    # count for the cap. A caller-seeded ledger plants phantom findings into
    # the first critic's brief and can trip the cap/no-progress escalation on
    # round one, so it is stripped alongside its `verification_target` pair.
    "verification_rounds",
    "verification_target",
    "vm",
    "workspace_container",
    OPERATOR_PAUSE_HOLD_CONTEXT_KEY,
    LAST_OPERATOR_PAUSE_HOLD_CONTEXT_KEY,
}

PUBLIC_JOB_CONFIG_RESERVED_KEYS = {
    "lifecycle_marker",
    "parent_job_id",
    "runner_kind",
    "runner_source",
}


def strip_public_job_reserved_markers(job: "JobCreate") -> None:
    """Remove system-only job markers from public create payloads."""
    job.parent_job_id = None
    job.creation_order = None
    job.worktree_path = None
    job.delegation_context = None
    # thread_id is derived, never submitted. Only the internal path may set it,
    # and there it is authenticated: prepare_job_admission_scope
    # fetches the thread and 403s when it is missing or owned by someone else.
    # The public path never validated it — harmless while the value was merely
    # a datasource-inheritance hint whose lookup failures are swallowed, but
    # once it is PERSISTED as created_by_thread_id and woken on, an unchecked
    # body field lets a caller name a victim's live session and have a
    # completion payload POSTed into it (/api/input on the agent pod is
    # unauthenticated). Stripping is also what keeps a bogus-but-well-formed
    # UUID a no-op instead of a ForeignKeyViolationError → HTTP 500.
    job.thread_id = None
    if isinstance(job.context, dict):
        job.context = {
            key: value
            for key, value in job.context.items()
            if key not in PUBLIC_JOB_CONTEXT_RESERVED_KEYS
        }
    if isinstance(job.config_override, dict):
        job.config_override = {
            key: value
            for key, value in job.config_override.items()
            if key not in PUBLIC_JOB_CONFIG_RESERVED_KEYS
        }


def strip_raw_officer_claim_context(job: "JobCreate") -> None:
    """Remove server-owned context from public and internal raw bodies.

    ``ticket=`` is the sole caller-selectable claim input. The final Officer
    admission transaction writes these context keys after resolving the ticket
    and locking the post; internal transport authentication does not make a
    model-authored context dictionary authoritative. Completion finalization
    similarly records ``evidence_manifest`` later via ``merge_job_context``;
    no job-creation body can seed repository/revision authority.
    """

    if isinstance(job.context, dict):
        job.context = {
            key: value
            for key, value in job.context.items()
            if key not in _SERVER_OWNED_RAW_CREATE_CONTEXT_KEYS
        }
