"""Live session configuration edits and live workspace-tier upgrades.

Extracted verbatim from ``orchestrator.main`` (R1.B06 lane B, census group
``S_CONFIG``). Five route operations share one commit core and one strict
protected-cloud marker; this module owns both so the internal agent PATCH and
the owner-facing PATCH cannot drift apart.

The properties that make these a credential and provisioning boundary move
unchanged:

* **The strict marker never coerces.** ``protected_cloud_mutation_marker``
  deliberately does *not* use ``thread_metadata_object``'s legacy best-effort
  parse: corrupt JSON or list metadata would otherwise become an ordinary row
  and bypass the protected fixed-runtime contract. Malformed is
  ``409 protected_cloud_malformed``, and ``require_unprotected_workspace_upgrade``
  turns a live marker into ``409 protected_cloud_workspace_fixed`` *before* any
  provisioning effect.
* **The commit is serialized and generation-fenced.**
  :func:`apply_thread_config_update` takes
  ``thread_configuration_transaction`` and re-reads the row ``FOR UPDATE``, so a
  mixed before/after selection can never be rendered; a managed runtime must
  additionally present the exact snapshot generation or get
  ``409``. Ordered permission/narration scalars are refused here and must use
  the durable control inbox.
* **Every upgrade authorizes before it provisions.** Both upgrade routes run
  ``enforce_workspace_upgrade_grants`` (the ``vm_workspaces`` kill switch,
  per-user ``can_use_vm`` and the ``vm_workspace`` PDP grant) ahead of any
  effect, and the VM path re-reads under the advisory lock so a concurrent End
  wins cleanly.
* **An unproven VM teardown is a 503, not an "aborted" stamp.** An
  accepted/absent control-plane response is not process-zero for a partitioned
  guest; marking it aborted would hide a credential-capable VM from the
  lifecycle owner and from migration 0189.

:func:`apply_thread_config_update_locked` is the commit core both PATCH routes
share (validate → authorize → enrich → persist → audit). R1.B12 moved it here
verbatim from the application module: its effectful collaborators (connector
authorization and provenance, the datasource tool flip, grant enforcement,
model-credential injection, the audit writer) arrive through
:class:`ThreadConfigUpdateDependencies`, and its pure policy helpers are
imported from their owners. The late imports inside the function bodies are
kept where they were written, because tests steer those paths by patching the
attribute on the owning module.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal, Protocol
from uuid import UUID

from fastapi import HTTPException, Request

from orchestrator.database.postgres import (
    DatasourceMaterializationAuthorizationError,
    DatasourcePolicyConflictError,
)
from orchestrator.schemas.thread_config import (
    AgentThreadConfigUpdateRequest,
    ThreadConfigPatchRequest,
    ThreadWorkspaceUpgradeRequest,
)
from orchestrator.security.access import redact_config_override
from orchestrator.services.config_overrides import deep_merge_dicts
from orchestrator.services.manifest_runtime_ownership import require_srw_runtime
from orchestrator.services.session_class_policy import (
    require_stateless_workspace,
    session_class_pinned_refusal,
)
from orchestrator.services.session_create_overrides import (
    validated_session_officer_override,
)
from orchestrator.services.session_tool_policy import validated_tool_overrides
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from orchestrator.services.vm_workspace_config import vm_provisioning_options
from orchestrator.services.session_runtime_admission import (
    protected_cloud_marker_state,
    thread_runtime_authority,
)
from orchestrator.services.workspace_tier_policy import thread_workspace_backend
from orchestrator.services.vm_workspace_recovery_store import (
    acquire_vm_cleanup_permit,
    vm_cleanup_kwargs,
    completed_cleanup_outcome,
    complete_vm_cleanup_permit,
)
from shared.run_queue import LANE_PINNED
from shared.runtime.core.loader import canonical_config_name

logger = logging.getLogger(__name__)


class ThreadConfigStore(Protocol):
    async def get_thread(self, thread_id: str) -> dict[str, Any] | None: ...

    def thread_configuration_transaction(self, thread_id: str) -> Any: ...

    def thread_advisory_lock(self, thread_id: str) -> Any: ...

    async def refresh_session_execution(
        self, thread_id: str, *, conn: Any, config_override: dict[str, Any]
    ) -> dict[str, Any]: ...

    async def merge_thread_vm_context(
        self, thread_id: str, patch: dict[str, Any]
    ) -> Any: ...

    async def get_project_officer(self, project_id: str) -> dict[str, Any] | None: ...

    async def get_datasource_policy_rows(
        self, datasource_ids: list[str]
    ) -> list[dict[str, Any]]: ...

    async def get_user(self, user_id: str) -> dict[str, Any] | None: ...

    async def resolve_datasources_for_thread(
        self, *, datasource_ids: list[str], project_ids: list[str]
    ) -> list[dict[str, Any]]: ...

    async def resolve_api_keys_for_job(
        self, *, user_id: str | None, project_id: str | None
    ) -> dict[str, Any]: ...

    async def merge_thread_config_override(
        self, thread_id: str, config_override: dict[str, Any]
    ) -> Any: ...

    async def set_thread_datasource_ids(
        self,
        thread_id: str,
        datasource_ids: list[str],
        *,
        datasource_policy_revisions: dict[str, int] | None = None,
        datasource_selection_provenance: dict[str, Any] | None = None,
    ) -> bool: ...


@dataclass(frozen=True)
class ThreadConfigUpdateDependencies:
    """Per-invocation collaborators for session config and upgrade routes.

    ``store`` is main's ``postgres_db``; ``vm_provisioner`` and
    ``container_provisioner`` are its provisioner singletons. The callables
    are owned elsewhere and reach this module only through here:

    * ``enforce_workspace_upgrade_grants`` — B05 grant enforcement, bound to
      main's dependency object;
    * ``require_internal`` / ``require_thread_owner`` — the two auth guards, so
      a test can steer them without reaching into the security package;
    * the commit core's effectful collaborators, each bound to main's own
      wrapper so a patch of the application attribute still steers it:
      ``thread_project_ids``, ``authorize_thread_datasource_selection``,
      ``build_datasource_tool_override`` and
      ``datasource_selection_provenance`` (live connector selection),
      ``enforce_session_create_grants`` (the owner's capability grants),
      ``inject_model_credentials`` (model-swap transport enrichment) and
      ``log_security_event`` (the ``session_config_updated`` audit writer).
    """

    store: ThreadConfigStore
    vm_provisioner: Any
    container_provisioner: Any
    recovery_store: Any
    enforce_workspace_upgrade_grants: Callable[..., Awaitable[Any]]
    require_internal: Callable[[Request], Awaitable[Any]]
    require_thread_owner: Callable[..., Awaitable[Any]]
    thread_project_ids: Callable[[str], Awaitable[list[str]]]
    authorize_thread_datasource_selection: Callable[
        ..., Awaitable[tuple[list[str], dict[str, int]]]
    ]
    build_datasource_tool_override: Callable[..., Any]
    datasource_selection_provenance: Callable[..., Awaitable[dict[str, Any]]]
    enforce_session_create_grants: Callable[..., Awaitable[Any]]
    inject_model_credentials: Callable[..., Awaitable[Any]]
    log_security_event: Callable[..., Awaitable[Any]]


def config_change_summary(
    config_override: dict[str, Any], datasource_ids: list[str] | None
) -> str:
    """One-line audit summary of a config change: dotted KEY paths only.

    Values are deliberately omitted — the fragment can carry transport
    secrets after enrichment, and the security_events table must never
    hold credential material.
    """
    keys: list[str] = []
    for k, v in sorted(config_override.items()):
        if isinstance(v, dict) and v:
            keys.extend(f"{k}.{sub}" for sub in sorted(v))
        else:
            keys.append(k)
    parts = []
    if keys:
        parts.append("keys=" + ",".join(keys))
    if datasource_ids is not None:
        parts.append(f"datasource_ids={len(datasource_ids)}")
    return " ".join(parts) or "empty"


async def apply_thread_config_update(
    thread_id: str,
    thread_row: dict[str, Any] | None,
    config_override: dict[str, Any],
    datasource_ids: list[str] | None,
    *,
    request: Request,
    actor: dict[str, Any] | None,
    managed_runtime: bool = False,
    snapshot_patch_protocol: int | None = None,
    snapshot_generation: int | None = None,
    dependencies: ThreadConfigUpdateDependencies,
) -> tuple[dict[str, Any], list[str] | None]:
    """Commit accepted settings, connector selection and one spec generation."""
    if thread_row is not None:
        require_srw_runtime(thread_row)
    from shared.runtime.core.session_config_patch import validate_session_settings_patch

    try:
        validate_session_settings_patch(config_override)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc
    interactive = config_override.get("interactive")
    if isinstance(interactive, dict) and {
        "permission_mode",
        "narration_mode",
    }.intersection(interactive):
        raise HTTPException(
            409,
            "permission_mode and narration_mode must use the ordered session control endpoint",
        )
    async with dependencies.store.thread_configuration_transaction(thread_id) as conn:
        # Serialize config updates; never render a mixed before/after selection.
        current = await conn.fetchrow(
            "SELECT * FROM threads WHERE id=$1 FOR UPDATE", UUID(str(thread_id))
        )
        if current is None:
            raise HTTPException(404, "Thread not found")
        if managed_runtime:
            from orchestrator.services.manifest_execution_snapshot import read_execution

            execution = await read_execution(conn, "Session", thread_id)
            if execution is not None and (
                snapshot_patch_protocol != 1
                or snapshot_generation != execution["generation"]
            ):
                raise HTTPException(
                    409,
                    "Session configuration changed; reattach to load its current generation before editing settings.",
                )
        result = await apply_thread_config_update_locked(
            thread_id,
            dict(current),
            config_override,
            datasource_ids,
            request=request,
            actor=actor,
            dependencies=dependencies,
        )
        try:
            saved = await dependencies.store.refresh_session_execution(
                thread_id, conn=conn, config_override=result[0]
            )
        except DatasourceMaterializationAuthorizationError as exc:
            raise HTTPException(403, str(exc)) from exc
        return saved["delivery_override"], result[1]


async def apply_thread_config_update_locked(
    thread_id: str,
    thread_row: dict[str, Any] | None,
    config_override: dict[str, Any],
    datasource_ids: list[str] | None,
    *,
    request: Request,
    actor: dict[str, Any] | None,
    dependencies: ThreadConfigUpdateDependencies,
) -> tuple[dict[str, Any], list[str] | None]:
    """Validate → authorize → enrich → persist a thread config change.

    Shared core of the internal live-session PATCH
    (``agent_update_thread_config``) and the owner-facing
    disconnected-session PATCH (live_session_settings.md Slice C) — the two
    callers differ only in auth, connection gating, and response redaction.
    Authorization (datasource selection + capability grants) is keyed to the
    THREAD OWNER in both cases, so an API caller can never exceed what the
    live pane allows.

    Returns ``(config_override, selected_datasource_ids)`` where the fragment
    is enriched with resolved model transport (``base_url``/``api_key`` +
    explicit ``None`` sentinels) — the internal caller returns it verbatim to
    the agent; browser-facing callers MUST redact it. Persistence is always
    redacted. Emits a ``session_config_updated`` security event on success
    (``actor`` is the resolved caller for owner-facing requests, None for
    internal ones — the recorded path distinguishes the two).
    """
    protected_marker = protected_cloud_mutation_marker(thread_row)
    if protected_marker == "on" and {
        "workspace",
        "officer",
    }.intersection(config_override):
        # A protected session is safe only while its effective runtime remains
        # the supported Container/non-Officer class.  Generic live config
        # mutation is not an atomic protected-runtime transition protocol, so
        # reject both security-relevant blocks before grant resolution, audit,
        # or persistence.  Even a seemingly harmless partial/no-op block is
        # refused: defaults and expert inheritance make a fragment alone an
        # insufficient proof of the resulting class.
        raise HTTPException(
            status_code=409,
            detail={
                "code": "protected_cloud_runtime_class_fixed",
                "message": (
                    "Protected cloud sessions cannot change workspace tier or "
                    "Officer mode."
                ),
            },
        )
    if "officer" in config_override and thread_row and thread_row.get("project_id"):
        post = await dependencies.store.get_project_officer(
            str(thread_row["project_id"])
        )
        if post and str(post.get("thread_id") or "") == str(thread_id):
            raise HTTPException(
                status_code=409,
                detail=(
                    "The commissioned officer block is owned by the Officer "
                    "Post; use the project Officer Post endpoint so durable "
                    "and runtime configuration change atomically."
                ),
            )
    if thread_row and thread_row.get("execution_lane") == "stateless":
        # Generic config mutation is not the workspace-upgrade protocol. Refuse
        # an already-drifted row and allow only same-tier workspace tuning.
        current_backend = require_stateless_workspace(thread_row)
        if "officer" in config_override:
            # Runtime PATCH historically accepted this block without the
            # create-time validator. Normalize booleans and reject unknown
            # fields before evaluating the proposed immutable session class.
            normalized_officer = validated_session_officer_override(config_override)
            config_override["officer"] = normalized_officer or {}

        metadata = thread_metadata_object(thread_row)
        persisted_override = metadata.get("config_override") or {}
        if not isinstance(persisted_override, dict):
            persisted_override = {}
        proposed_override = deep_merge_dicts(
            persisted_override,
            config_override,
        )
        class_refusal = session_class_pinned_refusal(proposed_override)
        if class_refusal is not None:
            # Creation materializes the fully resolved class booleans into the
            # request layer, so this merge is authoritative even if the expert
            # or account default changes later. Never persist a fragment that
            # would move a live queue-served thread onto pinned-only wake
            # machinery without an atomic lane-transition protocol.
            raise HTTPException(
                status_code=409,
                detail=(
                    "A stateless session cannot enable pinned-only lifecycle "
                    f"behavior ({class_refusal})"
                ),
            )
        if "workspace" in config_override:
            workspace_patch = config_override.get("workspace")
            if not isinstance(workspace_patch, dict) or (
                "backend" in workspace_patch
                and workspace_patch.get("backend") != current_backend
            ):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "A stateless session cannot change its workspace tier "
                        "through generic config mutation"
                    ),
                )

    if "tools" in config_override:
        # Runtime updates use the same registry vocabulary as session creation
        # and job creation.  A fragment this boundary will not honour is a 400
        # from here, not a silent discard: the previous closed-group filter
        # replaced `tools` with the four accepted groups and dropped the rest,
        # so a live "turn research off" was acknowledged and never applied.
        # The names are also checked against their own category, because the
        # loader resolves a name against the global registry rather than the
        # key it arrived under.
        accepted_tools = validated_tool_overrides(config_override)
        if accepted_tools:
            config_override["tools"] = accepted_tools
        else:
            # `tools: {}` only — it asks for nothing, so there is nothing to
            # honour and nothing to report as changed.
            config_override.pop("tools", None)

    if "delegation" in config_override:
        # The gate half of the Delegation toggle (see create_thread). Same
        # validator as create, so the two write paths cannot disagree about
        # what the block means; malformed is a 400, never a silent drop.
        from orchestrator.services.session_create_overrides import (
            SessionOverrideError,
            validate_delegation_override,
        )

        try:
            accepted_delegation = validate_delegation_override(
                config_override["delegation"]
            )
        except SessionOverrideError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if accepted_delegation:
            config_override["delegation"] = accepted_delegation
        else:
            config_override.pop("delegation", None)

    # Audit summary is computed PRE-enrichment so it names only the keys the
    # caller actually sent (enrichment adds llm.api_key/base_url internally).
    change_summary = config_change_summary(config_override, datasource_ids)

    # Live datasource change (live_session_settings.md Slice B): authorize
    # the requested full selection exactly like create does — including the
    # lite-tier/repository rule against the thread's CURRENT workspace
    # backend (a live add is create-like; only the attach-time
    # revalidation deliberately passes None) — then fold the resulting
    # datasource tool-category flip into the grant-checked fragment so a
    # datasource_tools-denied principal fails HERE at the PATCH, not at
    # the next attach.
    selected_ds_ids: list[str] | None = None
    selected_ds_revisions: dict[str, int] | None = None
    datasource_selection_provenance: dict[str, Any] | None = None
    grant_fragment = config_override
    if datasource_ids is not None:
        if thread_row is None:
            raise HTTPException(status_code=404, detail="Thread not found")
        requested_ids = [str(v) for v in datasource_ids]
        current_metadata = thread_metadata_object(thread_row)
        try:
            canonical_requested = {str(UUID(value)) for value in requested_ids}
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=403,
                detail="One or more selected connectors are unavailable",
            ) from exc
        removed_ids = (
            set(current_metadata.get("datasource_ids") or []) - canonical_requested
        )
        if removed_ids:
            removed_rows = await dependencies.store.get_datasource_policy_rows(
                list(removed_ids)
            )
            if any(row.get("type") == "credentials" for row in removed_rows):
                raise HTTPException(
                    status_code=409,
                    detail="Credential connectors stay attached for the lifetime of the session",
                )
        target_project_ids = await dependencies.thread_project_ids(thread_id)
        if thread_row.get("user_id"):
            owner = await dependencies.store.get_user(str(thread_row["user_id"]))
            if owner is None:
                # Same generic denial as create — no enumeration oracle.
                raise HTTPException(
                    status_code=403,
                    detail="One or more selected connectors are unavailable",
                )
            (
                selected_ds_ids,
                selected_ds_revisions,
            ) = await dependencies.authorize_thread_datasource_selection(
                owner,
                requested_ids,
                workspace_backend=thread_workspace_backend(thread_row),
                target_project_ids=target_project_ids,
                effective_work_owner_id=str(thread_row["user_id"]),
            )
        else:
            # Ownerless/system threads have no ambient authority. A live edit
            # may narrow or preserve the already-materialized set, but cannot
            # use the trusted-inheritance seam to add an arbitrary UUID.
            metadata = thread_row.get("metadata") or {}
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except (json.JSONDecodeError, TypeError):
                    metadata = {}
            persisted_ids = (
                metadata.get("datasource_ids") if isinstance(metadata, dict) else []
            ) or []
            try:
                requested_set = {str(UUID(str(value))) for value in requested_ids}
                persisted_set = {str(UUID(str(value))) for value in persisted_ids}
            except (TypeError, ValueError) as exc:
                raise HTTPException(
                    status_code=403,
                    detail="One or more selected connectors are unavailable",
                ) from exc
            if not requested_set.issubset(persisted_set):
                raise HTTPException(
                    status_code=403,
                    detail="One or more selected connectors are unavailable",
                )
            (
                selected_ds_ids,
                selected_ds_revisions,
            ) = await dependencies.authorize_thread_datasource_selection(
                None,
                requested_ids,
                workspace_backend=thread_workspace_backend(thread_row),
                target_project_ids=target_project_ids,
                trusted_system_inheritance=True,
            )

        resolved_ds = await dependencies.store.resolve_datasources_for_thread(
            datasource_ids=selected_ds_ids,
            project_ids=target_project_ids,
        )
        flip = dependencies.build_datasource_tool_override(resolved_ds, None)
        # THE FLIP WINS, and the order is load-bearing. It used to be the other
        # way round, safe only because the request's tools were filtered down
        # to four non-connector groups first. Now that every category is
        # honoured, a request could send `tools.sql: []` and mask a
        # datasource_tools violation from the PDP below — while attach applies
        # the flip LAST anyway (_build_datasource_tool_override updates the
        # request's tools with the datasource categories), so the session would
        # get connector tools the grant check never saw. Modelling attach
        # exactly is what keeps this fragment honest.
        grant_fragment = {
            **config_override,
            "tools": {
                **(config_override.get("tools") or {}),
                **flip.get("tools", {}),
            },
        }
        datasource_selection_provenance = (
            await dependencies.datasource_selection_provenance(
                datasource_ids=selected_ds_ids,
                policy_revisions=selected_ds_revisions,
                origin="explicit",
                effective_work_owner_id=(
                    str(thread_row["user_id"]) if thread_row.get("user_id") else None
                ),
                actor=actor,
                project_ids=target_project_ids,
                creation_path=(
                    "live_thread_internal" if actor is None else "live_thread_rest"
                ),
            )
        )

    # Layer 2 (fail loud): a runtime config change must also fit the owner's
    # grants — reject a denied permission_mode/model with 422 instead of
    # persisting a config the session can't run (an API-direct or stale-UI
    # escalation past the user's ceiling; the cockpit greys these out
    # client-side). Ownerless/standalone threads (user_id NULL) aren't
    # subject to a user's grants — skip. Admin owner bypasses.
    # knowledge-base/knowledge/issues/session_permission_mode_grant_denied_ready_timeout.md
    if thread_row and thread_row.get("user_id"):
        await dependencies.enforce_session_create_grants(
            grant_fragment,
            user_id=str(thread_row["user_id"]),
            project_ids=(
                [str(thread_row["project_id"])] if thread_row.get("project_id") else []
            ),
        )

    # Enrich endpoint-backed model swaps with base_url + api_key so the
    # persisted override is complete. Without this, a hot-swap to a
    # custom-endpoint model leaves the next session attach pointing at
    # the default OpenAI base.
    llm_section = config_override.get("llm")
    if llm_section and llm_section.get("model"):
        if thread_row:
            user_id = str(thread_row["user_id"]) if thread_row.get("user_id") else None
            project_id = (
                str(thread_row["project_id"]) if thread_row.get("project_id") else None
            )
            resolved_keys = await dependencies.store.resolve_api_keys_for_job(
                user_id=user_id, project_id=project_id
            )
            llm_section = dict(llm_section)
            await dependencies.inject_model_credentials(
                section=llm_section,
                model_id=llm_section["model"],
                user_id=user_id,
                resolved_keys=resolved_keys,
            )
            # A model swap must fully determine its transport. Any field
            # resolution didn't set becomes an explicit None so the
            # agent-side deep_merge CLEARS the previous model's value
            # instead of inheriting it (e.g. swapping off an
            # endpoint-backed model must not keep its base_url).
            for transport_key in ("provider", "base_url", "api_key"):
                llm_section.setdefault(transport_key, None)
            config_override["llm"] = llm_section

    # Persist WITHOUT secrets — the agent rebuilds its LLM from the enriched
    # dict returned below, and resume re-injects from source. The explicit
    # None transport sentinels stay in the stored copy so the deep-merge
    # clears the previous model's transport; resume re-injection treats them
    # as absent (see _inject_thread_dispatch_credentials).
    ok = await dependencies.store.merge_thread_config_override(
        thread_id, redact_config_override(config_override)
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Thread not found")

    # Persist the accepted selection only after every check above passed.
    # The category flip is NOT merged into config_override — the closed
    # session tools vocabulary would drop it anyway; the agent re-fetches
    # GET /api/agents/threads/{id}/workspace and applies the categories
    # directly to its live session config, and every attach path re-derives
    # them from metadata.datasource_ids.
    if selected_ds_ids is not None:
        from shared.credential_connectors import CredentialConnectorAttachedError

        try:
            updated = await dependencies.store.set_thread_datasource_ids(
                thread_id,
                selected_ds_ids,
                datasource_policy_revisions=selected_ds_revisions,
                datasource_selection_provenance=datasource_selection_provenance,
            )
        except CredentialConnectorAttachedError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except DatasourcePolicyConflictError as exc:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Connector policy changed while updating the session; "
                    "retry the request"
                ),
            ) from exc
        if not updated:
            raise HTTPException(status_code=404, detail="Thread not found")

    # Config-change audit (live_session_settings.md Slice C): key paths only,
    # fired after every persist step succeeded. log_security_event never
    # raises, so a broken audit trail can't fail the update it documents.
    await dependencies.log_security_event(
        dependencies.store,
        resource_type="thread",
        event_type="session_config_updated",
        user=actor,
        resource_id=thread_id,
        detail=change_summary,
        request=request,
    )
    if selected_ds_ids is not None:
        # The snapshot policy and pinned merge use the same authorized derived
        # categories. Persisted author overrides still omit this live binding.
        config_override = {
            **config_override,
            "tools": grant_fragment.get("tools", {}),
        }
    return config_override, selected_ds_ids


def protected_cloud_mutation_marker(
    thread: dict[str, Any] | None,
) -> Literal["off", "on"]:
    """Strict protected marker for live runtime mutations.

    Runtime upgrade/config endpoints are credential and provisioning
    boundaries.  They must not use ``thread_metadata_object``'s legacy
    best-effort coercion: corrupt JSON/list metadata could otherwise become an
    ordinary row and bypass the protected fixed-runtime contract.
    """

    if thread is None:
        return "off"
    raw_metadata = thread.get("metadata")
    if raw_metadata is None:
        metadata: Any = {}
    elif isinstance(raw_metadata, str):
        try:
            metadata = json.loads(raw_metadata)
        except (json.JSONDecodeError, TypeError) as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "protected_cloud_malformed",
                    "message": "Protected cloud session state is invalid.",
                },
            ) from exc
    else:
        metadata = raw_metadata
    marker = protected_cloud_marker_state(metadata)
    if marker == "malformed":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "protected_cloud_malformed",
                "message": "Protected cloud session state is invalid.",
            },
        )
    return marker


def require_unprotected_workspace_upgrade(
    thread: dict[str, Any],
) -> dict[str, Any]:
    """Refuse every protected/malformed live workspace upgrade pre-effect."""

    marker = protected_cloud_mutation_marker(thread)
    if marker == "on":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "protected_cloud_workspace_fixed",
                "message": (
                    "Protected cloud sessions cannot upgrade or replace their "
                    "Container workspace."
                ),
            },
        )
    raw_metadata = thread.get("metadata")
    if raw_metadata is None:
        return {}
    if isinstance(raw_metadata, str):
        parsed = json.loads(raw_metadata)
        # The strict marker helper above has already proved this shape.
        assert isinstance(parsed, dict)
        return parsed
    assert isinstance(raw_metadata, dict)
    return raw_metadata


async def agent_update_thread_config(
    request: Request,
    thread_id: str,
    body: AgentThreadConfigUpdateRequest,
    *,
    dependencies: ThreadConfigUpdateDependencies,
) -> dict[str, Any]:
    """Body of ``PATCH /api/agents/threads/{thread_id}/config``."""
    await dependencies.require_internal(request)
    try:
        thread_row = await dependencies.store.get_thread(thread_id)
        config_override, selected_ds_ids = await apply_thread_config_update(
            thread_id,
            thread_row,
            dict(body.config_override or {}),
            body.datasource_ids,
            request=request,
            actor=None,
            managed_runtime=True,
            snapshot_patch_protocol=body.snapshot_patch_protocol,
            snapshot_generation=body.snapshot_generation,
            dependencies=dependencies,
        )
        return {
            "status": "updated",
            "config_override": config_override,
            "datasource_ids": selected_ds_ids,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def agent_upgrade_thread_to_vm(
    request: Request,
    thread_id: str,
    *,
    dependencies: ThreadConfigUpdateDependencies,
) -> dict[str, Any]:
    """Body of ``POST /api/agents/threads/{thread_id}/upgrade-to-vm``."""
    await dependencies.require_internal(request)
    vm_provisioner = dependencies.vm_provisioner
    thread = await dependencies.store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")

    if thread.get("execution_lane") != LANE_PINNED:
        raise HTTPException(
            status_code=409,
            detail="Workspace upgrades are not yet supported on the stateless lane",
        )

    metadata = require_unprotected_workspace_upgrade(thread)

    # Sec-1 — authorize BEFORE provisioning (fail-closed). This endpoint is the
    # target of both the sandbox→VM sudo path and the lite→vm delegation from
    # /upgrade-to-workspace; it previously ran ungated. The shared gate enforces
    # the global vm_workspaces kill-switch + per-user can_use_vm + the
    # vm_workspace PDP grant (workspace_tier_upgrade.md §4.4 Sec-1 / Phase 2).
    await dependencies.enforce_workspace_upgrade_grants(thread, target_tier="vm")

    if not vm_provisioner.is_available:
        raise HTTPException(
            status_code=503,
            detail="VM provisioning not available (no NATS or K8s)",
        )

    # The capability read above is advisory.  Serialize with End/protected
    # lifecycle work, then re-read and install the provision generation under
    # the exact current T/G/actor tuple before dispatch.  If End won after the
    # route read, the DB transition returns False and no VM request is sent.
    async with dependencies.store.thread_advisory_lock(thread_id):
        thread = await dependencies.store.get_thread(thread_id)
        if not thread:
            raise HTTPException(status_code=404, detail="Thread not found")
        metadata = require_unprotected_workspace_upgrade(thread)
        runtime_authority = thread_runtime_authority(thread)
        if runtime_authority is None:
            raise HTTPException(
                status_code=409,
                detail={"code": "pinned_runtime_identity_mismatch"},
            )
        raw_vm_ctx = metadata.get("vm")
        if raw_vm_ctx is not None and not isinstance(raw_vm_ctx, Mapping):
            raise HTTPException(status_code=409, detail="VM authority is malformed")
        vm_ctx = dict(raw_vm_ctx) if raw_vm_ctx is not None else None
        if (vm_ctx or {}).get("status") in (
            "provisioning",
            "created",
            "starting",
            "ssh_pending",
            "ready",
            "waiting_golden",
            "waiting_capacity",
            "waiting_headscale",
            "waiting_preparation",
        ):
            return {
                "status": vm_ctx["status"],
                "thread_id": thread_id,
                "message": "VM already provisioned or in progress",
            }

        options = await vm_provisioning_options(
            dependencies.store,
            "Session",
            thread,
            fallback=metadata.get("config_override"),
        )
        ok = await vm_provisioner.create_thread_vm(
            thread_id=thread_id,
            **options,
            agent_config=canonical_config_name(
                thread.get("config_name", "session_base")
            ),
            expected_runtime_generation=runtime_authority.generation,
            expected_agent_id=(
                str(thread["agent_id"]) if thread.get("agent_id") is not None else None
            ),
            expected_attach_token=(
                str(thread["runtime_attach_token"])
                if thread.get("runtime_attach_token") is not None
                else None
            ),
            expected_vm_context=vm_ctx,
        )
    if not ok:
        raise HTTPException(
            status_code=409,
            detail={"code": "vm_provision_authority_changed"},
        )

    return {
        "status": "provisioning",
        "thread_id": thread_id,
        "vm_provisioner_mode": vm_provisioner.mode,
    }


async def agent_abort_thread_vm_upgrade(
    request: Request,
    thread_id: str,
    *,
    dependencies: ThreadConfigUpdateDependencies,
) -> dict[str, Any]:
    """Body of ``POST /api/agents/threads/{thread_id}/abort-vm-upgrade``."""
    await dependencies.require_internal(request)
    vm_provisioner = dependencies.vm_provisioner
    thread = await dependencies.store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")

    deleted = False
    if vm_provisioner.lifecycle_available:
        try:
            identity = await vm_provisioner.capture_vm_teardown_identity(
                thread_id, entity_type="thread"
            )
            cleanup = await acquire_vm_cleanup_permit(
                dependencies.recovery_store,
                owner_kind="thread",
                owner_id=thread_id,
                identity=identity,
                source="abort_thread_vm_upgrade",
                purge_disk=True,
            )
            if not cleanup.allowed:
                raise RuntimeError("VM cleanup held for workspace recovery")
            disposition = completed_cleanup_outcome(cleanup)
            if disposition is None:
                outcome = await vm_provisioner.release_vm_captured(
                    thread_id,
                    identity,
                    entity_type="thread",
                    purge_disk=True,
                    capture_snapshot=False,
                    **vm_cleanup_kwargs(cleanup),
                )
                disposition = outcome.disposition
                if disposition in {"completed", "identity_superseded"}:
                    await complete_vm_cleanup_permit(
                        dependencies.recovery_store,
                        cleanup,
                        outcome=disposition,
                    )
            deleted = disposition == "completed"
        except Exception as e:
            logger.warning(
                "abort-vm-upgrade: delete_thread_vm failed for %s: %s", thread_id, e
            )
    if not deleted:
        # An accepted/absent control-plane response is not process-zero for a
        # partitioned guest. Preserve the exact generation and retry handle;
        # marking it aborted would hide a potentially credential-capable VM
        # from both the lifecycle owner and migration 0189.
        raise HTTPException(
            status_code=503,
            detail={
                "code": "vm_process_zero_unproven",
                "retryable": True,
            },
        )
    await dependencies.store.merge_thread_vm_context(thread_id, {"status": "aborted"})
    return {"status": "aborted", "thread_id": thread_id, "vm_deleted": deleted}


async def agent_upgrade_thread_to_workspace(
    request: Request,
    thread_id: str,
    body: ThreadWorkspaceUpgradeRequest | None = None,
    *,
    dependencies: ThreadConfigUpdateDependencies,
) -> dict[str, Any]:
    """Body of ``POST /api/agents/threads/{thread_id}/upgrade-to-workspace``."""
    await dependencies.require_internal(request)
    container_provisioner = dependencies.container_provisioner
    target_tier = (body.target_tier if body else "sandbox") or "sandbox"
    if target_tier not in ("sandbox", "vm"):
        raise HTTPException(
            status_code=400,
            detail=(
                f"upgrade-to-workspace supports target_tier 'sandbox' or 'vm'; "
                f"got {target_tier!r}"
            ),
        )

    # vm targets reuse the operator-gated VM provisioning path: it runs the same
    # enforce_workspace_upgrade_grants gate, provisions the VM, and records
    # metadata.vm. The agent then polls vm readiness and hot-swaps in place
    # exactly like the sandbox path — the swap handler (_handle_workspace_upgrade)
    # is tier-agnostic and sets sudo_action="allow" for a vm backend
    # (workspace_tier_upgrade.md Phase 2). Keeping a single client method +
    # endpoint means the agent stays uniform across tiers.
    if target_tier == "vm":
        return await agent_upgrade_thread_to_vm(
            request, thread_id, dependencies=dependencies
        )

    thread = await dependencies.store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")

    if thread.get("execution_lane") != LANE_PINNED:
        raise HTTPException(
            status_code=409,
            detail="Workspace upgrades are not yet supported on the stateless lane",
        )

    metadata = require_unprotected_workspace_upgrade(thread)

    # Sec-1 — authorize the upgrade against the owner's capability grants BEFORE
    # provisioning (fail-closed). sandbox passes by default; a shell-restricted
    # owner (or a vm target without vm_workspace) is refused with 403.
    await dependencies.enforce_workspace_upgrade_grants(thread, target_tier=target_tier)

    if not (container_provisioner.is_available and container_provisioner.in_cluster):
        raise HTTPException(
            status_code=503,
            detail="Workspace container provisioning not available (no in-cluster K8s)",
        )

    # Idempotency: short-circuit if a container is already in flight or ready.
    wc = metadata.get("workspace_container") or {}
    if wc.get("status") in ("pending", "creating", "created", "ready"):
        return {
            "status": wc["status"],
            "thread_id": thread_id,
            "target_tier": "sandbox",
            "message": "Workspace container already provisioned or in progress",
        }

    # The background owner installs the exact T/G/actor provision intent under
    # the lifecycle lock before its first Kubernetes effect.  Do not publish a
    # generic pending marker here: a stale route read must lose cleanly to End.
    asyncio.create_task(container_provisioner.create_pinned_thread_workspace(thread_id))

    return {
        "status": "provisioning",
        "thread_id": thread_id,
        "target_tier": "sandbox",
    }


async def update_thread_config(
    thread_id: str,
    body: ThreadConfigPatchRequest,
    request: Request,
    *,
    dependencies: ThreadConfigUpdateDependencies,
    owner: tuple[Any, Any] | None = None,
) -> dict[str, Any]:
    """Body of ``PATCH /api/persistent/threads/{thread_id}/config``.

    ``owner`` is the ``(user, thread)`` pair the route already proved. The route
    has to run the gate in its own body — ``scripts/check_endpoint_auth.py``
    reads the audited gate from the declaration and does not follow a call into
    a service — so accepting the result keeps this a single ownership read.
    """
    if owner is None:
        owner = await dependencies.require_thread_owner(
            request, dependencies.store, thread_id
        )
    user, thread = owner
    # Connected = an agent is bound AND the thread is in a live state. A
    # suspended/ended thread can carry a stale agent_id from a crash path
    # (drain-suspend clears it, a hard pod kill may not) — no live agent
    # serves those states, so they stay editable.
    if thread.get("agent_id") and thread.get("status") not in ("suspended", "ended"):
        raise HTTPException(
            status_code=409,
            detail=(
                "Session is connected to an agent — change settings from the "
                "session's settings pane; a server-side edit would not reach "
                "the running session until its next attach."
            ),
        )
    if not body.config_override and body.datasource_ids is None:
        raise HTTPException(status_code=400, detail="No changes provided")
    config_override, selected_ds_ids = await apply_thread_config_update(
        thread_id,
        thread,
        dict(body.config_override or {}),
        body.datasource_ids,
        request=request,
        actor=user,
        dependencies=dependencies,
    )
    return {
        "status": "updated",
        "config_override": redact_config_override(config_override),
        "datasource_ids": selected_ds_ids,
        "effective": (
            "next_turn"
            if thread.get("execution_lane") == "stateless"
            else "next_attach"
        ),
    }


__all__ = [
    "ThreadConfigStore",
    "ThreadConfigUpdateDependencies",
    "agent_abort_thread_vm_upgrade",
    "agent_update_thread_config",
    "agent_upgrade_thread_to_vm",
    "agent_upgrade_thread_to_workspace",
    "apply_thread_config_update",
    "apply_thread_config_update_locked",
    "config_change_summary",
    "protected_cloud_mutation_marker",
    "require_unprotected_workspace_upgrade",
    "update_thread_config",
]
