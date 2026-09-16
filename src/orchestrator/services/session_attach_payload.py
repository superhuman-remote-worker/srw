"""The delivery blob a persistent session's agent is attached with.

Extracted verbatim from ``orchestrator.main`` (R1.B05, root lane). It is the
session counterpart of the worker path's ``_build_job_start_request`` and
deliberately stays a separate operation: the two differ in identity, in
fencing, and in what the payload may contain, and merging them behind a flag
would hide exactly the differences that matter.

Three properties are load-bearing and moved unchanged:

* **Runtime authority is re-checked, not assumed.** The payload is built for
  one exact runtime generation and attach identity; a thread whose authority
  moved during assembly is refused rather than delivered to.
* **A protected thread never receives a live cloud mount.** The protected
  marker routes the payload through ``build_protected_cloud_mount``, and the
  selected read-only mount is matched against the thread's current protected
  selection before it is used.
* **Credentials are injected into the delivery blob and never persisted.** The
  thread row keeps the explicit per-session layer only; account fallback and
  expert fields are re-resolved here at attach time.

Collaborators arrive through :class:`SessionAttachPayloadDependencies`, rebuilt
per invocation by the application rather than captured at import.
"""

from __future__ import annotations

import asyncio  # noqa: F401  (used by the moved body)
import json
import logging
from dataclasses import dataclass
from typing import Any, Optional
from uuid import UUID

from fastapi import HTTPException

from orchestrator.services.config_overrides import deep_merge_dicts as _deep_merge_dicts
from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_INCARNATION_KEY,
)
from orchestrator.services.session_runtime_admission import (
    protected_cloud_marker_state,
    thread_runtime_authority,
)
from orchestrator.services.stateless_workspace_gate import (
    declared_thread_workspace_backend,
    thread_metadata_object,
)
from shared.backend_kinds import LITE_BACKENDS

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SessionAttachPayloadDependencies:
    """Collaborators for one attach-payload build, resolved per invocation.

    ``store`` is main's ``postgres_db``, rebound during ``lifespan``. The rest
    are operations owned elsewhere — B04's protected-cloud and cloud-mount
    operations, B06's runtime admission and project revalidation, and B05's own
    policy and job-preparation lanes — named here rather than imported so this
    module never reaches across a boundary it does not own.
    """

    GrantDenied: Any
    LiteWorkspaceConfigError: Any
    await_protected_cloud_runtime_ready: Any
    build_datasource_tool_override: Any
    build_datasources_payload: Any
    build_protected_cloud_mount: Any
    # Injected, not imported: several suites patch
    # ``orchestrator.main.mint_thread_runtime_actor``, and a direct import here
    # would resolve the real one while the stub sat unused — the exact hazard
    # §P3 names. The failure was loud (an unconnected store) rather than silent,
    # which is the safe half of it, but the patch still has to be reached.
    mint_thread_runtime_actor: Any
    inject_lite_workspace_config: Any
    protected_mount_selection_identity: Any
    require_pinned_status_identity: Any
    resolve_authorized_thread_datasources: Any
    resolve_session_config: Any
    revalidate_thread_project_ids: Any
    ro_mount_matches_protected_selection: Any
    thread_accepts_runtime: Any
    thread_project_ids: Any
    store: Any
    capture_session_config: Any = None


async def assemble_session_attach_payload(
    thread_id: str,
    *,
    config_override: Optional[dict] = None,
    config_name: Optional[str] = None,
    runtime_agent_id: str | None = None,
    dependencies: SessionAttachPayloadDependencies,
) -> Optional[dict[str, Any]]:
    """Assemble the session-attach payload for a thread — the ONE assembly.

    Factored out of ``_send_session_attach_locked`` so the stateless-lane
    claim bundle (``GET /internal/units/{unit_id}/claim-bundle``) and the
    legacy pinned-lane sender deliver identical attach payloads under
    identical fail-closed rules: lite workspace config injection, datasource
    reauthorization (the attach boundary owns the authoritative re-read),
    and ``_resolve_session_config`` grant/error handling.

    ``project_ids``/``datasources`` are deliberately NOT parameters: they are
    mutable authorization grants recomputed here from the thread's current
    state, never trusted from callers. Returns the payload dict, or ``None``
    on ANY refusal — callers must treat ``None`` as "do not attach" and must
    not distinguish refusal reasons (no enumeration oracle; details go to the
    server log only). Call under ``postgres_db.thread_datasource_lock`` to
    serialize with live connector-selection updates.
    """

    # Bind every collaborator to the name the moved body already uses, so
    # the body below is byte-for-byte what `main` ran.
    GrantDenied = dependencies.GrantDenied
    LiteWorkspaceConfigError = dependencies.LiteWorkspaceConfigError
    _await_protected_cloud_runtime_ready = (
        dependencies.await_protected_cloud_runtime_ready
    )
    _build_datasource_tool_override = dependencies.build_datasource_tool_override
    _build_datasources_payload = dependencies.build_datasources_payload
    _build_protected_cloud_mount = dependencies.build_protected_cloud_mount
    mint_thread_runtime_actor = dependencies.mint_thread_runtime_actor
    _inject_lite_workspace_config = dependencies.inject_lite_workspace_config
    _protected_mount_selection_identity = (
        dependencies.protected_mount_selection_identity
    )
    _require_pinned_status_identity = dependencies.require_pinned_status_identity
    _resolve_authorized_thread_datasources = (
        dependencies.resolve_authorized_thread_datasources
    )
    _resolve_session_config = dependencies.resolve_session_config
    _revalidate_thread_project_ids = dependencies.revalidate_thread_project_ids
    _ro_mount_matches_protected_selection = (
        dependencies.ro_mount_matches_protected_selection
    )
    _thread_accepts_runtime = dependencies.thread_accepts_runtime
    _thread_project_ids = dependencies.thread_project_ids
    postgres_db = dependencies.store
    # Lite tiers (virtual/none) carry no SSH endpoint. For `virtual` we attach
    # the object-store mounts here — deployment-sourced credentials, in-flight
    # only (never persisted to the thread row), keyed under threads/<id>/.
    # A misconfigured `virtual` deployment refuses the attach with a clear log.
    try:
        config_override = _inject_lite_workspace_config(
            config_override, prefix=f"threads/{thread_id}/"
        )
    except LiteWorkspaceConfigError as exc:
        logger.error("Session attach: thread %s lite-config error: %s", thread_id, exc)
        return None

    # Orchestrator-resolved config for the warm-pool agent: this is the expert
    # delivery channel the warm path lacked (the 3-minute-stall bug). Re-resolve
    # on every attach (no freeze). None when experts are off / resolve fails →
    # the agent uses the config_name + config_override fallback below.
    resolved_config: dict[str, Any] | None = None
    _sess_status: dict[str, Any] = {"_capture_manifest": True}
    try:
        _thread = await postgres_db.get_thread(thread_id)
    except Exception:
        logger.exception(
            "Session attach: failed to load thread %s; refusing (fail closed)",
            thread_id,
        )
        return None
    if not _thread:
        logger.warning("Session attach: thread %s vanished; refusing", thread_id)
        return None
    if not _thread_accepts_runtime(_thread):
        logger.info(
            "Session attach: thread %s lifecycle %r is not preparable; refusing",
            thread_id,
            _thread.get("status"),
        )
        return None

    _meta = _thread.get("metadata") or {}
    if isinstance(_meta, str):
        try:
            _meta = json.loads(_meta)
        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Session attach: malformed metadata for thread %s; refusing",
                thread_id,
            )
            return None
    protected_marker = protected_cloud_marker_state(_meta)
    if protected_marker == "malformed":
        logger.warning(
            "Session attach: malformed protected marker for thread %s; refusing",
            thread_id,
        )
        return None
    if protected_marker == "on" and not await _await_protected_cloud_runtime_ready(
        thread_id,
        timeout_s=0,
        allow_schedule=False,
    ):
        logger.info(
            "Session attach: thread %s refused — protected cloud runtime not ready",
            thread_id,
        )
        return None

    # Datasource selections are mutable authorization grants, not frozen
    # capabilities. The caller may have resolved a payload before this function
    # re-fetched the thread; a concurrent A -> B/[] settings update must not let
    # that stale payload survive. Reauthorize the current metadata selection,
    # resolve its credentials here, and rebuild connector tool categories from
    # that same result. The generic denial deliberately avoids an enumeration
    # oracle; datasource credentials never reach this log path.
    try:
        # Acknowledged-but-still-unavailable project drift is narrowed out
        # INSIDE _revalidate_thread_project_ids now (only while it stays
        # denied), so an already-acknowledged revoked/deleted project does
        # not spuriously 403 here — while a RECOVERED acknowledged project
        # returns automatically (spec §3.2).
        project_ids = await _revalidate_thread_project_ids(
            _thread, await _thread_project_ids(thread_id)
        )
        resolved_datasources = await _resolve_authorized_thread_datasources(
            _thread,
            _meta.get("datasource_ids"),
            target_project_ids=project_ids,
        )
        from orchestrator.services.job_delivery import apply_review_delivery_branch

        resolved_datasources = apply_review_delivery_branch(_meta, resolved_datasources)
        datasources = _build_datasources_payload(resolved_datasources)
        config_override = _build_datasource_tool_override(
            resolved_datasources, config_override
        )
    except HTTPException:
        logger.warning(
            "Session attach denied for thread %s: its current knowledge/data "
            "scope is no longer available",
            thread_id,
        )
        return None
    except Exception:
        logger.exception(
            "Session attach: access revalidation failed for thread %s; "
            "refusing (fail closed)",
            thread_id,
        )
        return None

    try:
        if _thread:
            resolved_config = await _resolve_session_config(
                _thread, _meta, config_override=config_override, status=_sess_status
            )
    except GrantDenied as gd:
        logger.warning("Session attach denied for thread %s: %s", thread_id, gd)
        return None
    except Exception:
        logger.exception(
            "Session attach: resolve failed for thread %s; using fallback", thread_id
        )
    # Fail closed: a resolution ERROR (experts on, resolve threw) must not deliver
    # the unvetted config_override — the grant check never ran. The 'disabled' state
    # (experts off) intentionally falls through to the legacy fallback below.
    if _sess_status.get("state") == "error":
        logger.warning(
            "Session attach: resolve errored for thread %s; refusing (fail closed)",
            thread_id,
        )
        return None

    # Control-inbox scalar persistence is first-class on ``threads``. Overlay
    # materialized values at the final delivery edge for BOTH resolution modes
    # so a handoff cannot resurrect an older config value. Narration remains
    # NULL for legacy rows whose value is inherited through an expert/account
    # layer that migration SQL cannot resolve; those rows keep the resolved
    # value until creation/control materializes one.
    interactive_scalars = {
        "permission_mode": str(_thread.get("permission_mode") or "supervised"),
    }
    if _thread.get("narration_mode") is not None:
        interactive_scalars["narration_mode"] = str(_thread["narration_mode"])
    if resolved_config is not None:
        resolved_config = dict(resolved_config)
        resolved_agent = dict(resolved_config.get("agent") or {})
        resolved_interactive = dict(resolved_agent.get("interactive") or {})
        resolved_interactive.update(interactive_scalars)
        resolved_agent["interactive"] = resolved_interactive
        resolved_config["agent"] = resolved_agent
    else:
        config_override = _deep_merge_dicts(
            dict(config_override or {}), {"interactive": interactive_scalars}
        )

    try:
        runtime_actor = await mint_thread_runtime_actor(
            postgres_db,
            thread_id=thread_id,
            project_ids=project_ids,
            agent_id=runtime_agent_id,
        )
    except Exception:
        logger.exception(
            "Session attach: runtime actor mint failed for thread %s; "
            "refusing (fail closed)",
            thread_id,
        )
        return None

    # Complete every protected reader/selection await before the final
    # lifecycle read.  Callers hold ``thread_datasource_lock`` across this
    # helper, so the selected thread_mount row cannot change between this
    # exact snapshot and the synchronous post-read checks below.
    prepared_protected_selection: tuple[str, ...] | None = None
    if protected_marker == "on":
        prepared_runtime_authority = thread_runtime_authority(_thread)
        if prepared_runtime_authority is None:
            logger.info(
                "Session attach: thread %s refused — no runtime authority at "
                "protected-selection prepare (status=%r generation=%r)",
                thread_id,
                (_thread or {}).get("status"),
                (_thread or {}).get("runtime_generation"),
            )
            return None
        ro_row, mount_rows = await asyncio.gather(
            postgres_db.get_ro_mount_by_thread(thread_id),
            postgres_db.list_thread_mounts(thread_id),
        )
        from orchestrator.services.cloud_staging import select_protected_mount

        prepared_protected_selection = _protected_mount_selection_identity(
            select_protected_mount(mount_rows)
        )
        if (
            prepared_protected_selection is None
            or not _ro_mount_matches_protected_selection(
                ro_row,
                mount_rows,
                thread_id=thread_id,
                user_id=str(_thread.get("user_id") or ""),
                runtime_generation=prepared_runtime_authority.generation,
            )
        ):
            logger.info(
                "Session attach: thread %s refused — ro-mount does not match the "
                "protected selection",
                thread_id,
            )
            return None
        if _build_protected_cloud_mount(ro_row, thread_id=thread_id) is None:
            logger.info(
                "Session attach: thread %s refused — protected cloud mount could "
                "not be built",
                thread_id,
            )
            return None

    if dependencies.capture_session_config is not None:
        try:
            resolved_config = await dependencies.capture_session_config(
                _thread, resolved_config, _sess_status, project_ids=project_ids
            )
        except HTTPException:
            logger.warning(
                "Session attach: configuration admission changed for %s; refusing",
                thread_id,
            )
            return None

    # Config/datasource/repository assembly crosses several awaits. End may
    # commit during any of them, so the response-side credential boundary owns
    # one authoritative final lifecycle read. Nothing after this read may
    # await before the payload is returned.
    final_thread = await postgres_db.get_thread(thread_id)
    if not _thread_accepts_runtime(final_thread):
        logger.info(
            "Session attach: lifecycle changed before payload delivery "
            "(thread=%s status=%r)",
            thread_id,
            (final_thread or {}).get("status"),
        )
        return None
    final_meta = thread_metadata_object(final_thread)
    final_marker = protected_cloud_marker_state(final_meta)
    if final_marker == "malformed":
        logger.info(
            "Session attach: thread %s refused — protected-cloud marker malformed",
            thread_id,
        )
        return None
    if final_marker != protected_marker:
        logger.info(
            "Session attach: thread %s refused — protected-cloud marker changed "
            "mid-assembly (%r -> %r)",
            thread_id,
            protected_marker,
            final_marker,
        )
        return None
    if final_marker == "on" and prepared_protected_selection is None:
        logger.info(
            "Session attach: thread %s refused — protected marker on but no "
            "prepared selection",
            thread_id,
        )
        return None
    final_runtime_authority = thread_runtime_authority(final_thread)
    if final_runtime_authority is None and (
        final_marker != "off" or _require_pinned_status_identity()
    ):
        logger.info(
            "Session attach: thread %s refused — no final runtime authority "
            "(status=%r generation=%r marker=%r require_pinned=%s)",
            thread_id,
            (final_thread or {}).get("status"),
            (final_thread or {}).get("runtime_generation"),
            final_marker,
            _require_pinned_status_identity(),
        )
        return None
    final_workspace = final_meta.get("workspace_container") or {}
    final_binding = final_meta.get("_workspace_binding") or {}
    if not isinstance(final_workspace, dict) or not isinstance(final_binding, dict):
        logger.info(
            "Session attach: thread %s refused — workspace_container/_workspace_binding "
            "are not objects (types %s/%s)",
            thread_id,
            type(final_workspace).__name__,
            type(final_binding).__name__,
        )
        return None
    final_workspace_generation = final_binding.get("generation")
    final_workspace_runtime = (
        final_workspace.get("_docker_workspace_lease_id")
        if final_workspace.get("provisioner") == "docker"
        else final_workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY)
    )
    final_workspace_backend = declared_thread_workspace_backend(final_thread)
    if (
        final_binding.get("kind") == "virtual"
        and final_workspace_backend in LITE_BACKENDS
    ):
        # A virtual binding generation fences the durable object-store
        # namespace; it is not a physical runtime identity.  The attach
        # contract deliberately represents lite workspaces as (None, None),
        # while sandbox/VM workspaces carry an exact generation+incarnation
        # pair.  Never let physical residue hide behind a lite declaration.
        if final_workspace_runtime:
            logger.info(
                "Session attach: thread %s refused — lite workspace (binding "
                "kind=virtual backend=%r) carries a physical "
                "runtime_incarnation=%r",
                thread_id,
                final_workspace_backend,
                final_workspace_runtime,
            )
            return None
        final_workspace_generation = None
    elif bool(final_workspace_generation) != bool(final_workspace_runtime):
        logger.info(
            "Session attach: thread %s refused — workspace identity pair "
            "incomplete (generation=%r from _workspace_binding, "
            "runtime_incarnation=%r from workspace_container, binding kind=%r "
            "backend=%r). NOTE: the two halves are read from DIFFERENT metadata "
            "objects — see the vault note on the stateless attach livelock "
            "before changing this gate.",
            thread_id,
            final_workspace_generation,
            final_workspace_runtime,
            final_binding.get("kind"),
            final_workspace_backend,
        )
        return None
    try:
        final_workspace_generation = (
            str(UUID(str(final_workspace_generation)))
            if final_workspace_generation
            else None
        )
        final_workspace_runtime = (
            str(UUID(str(final_workspace_runtime))) if final_workspace_runtime else None
        )
    except (TypeError, ValueError):
        logger.info(
            "Session attach: thread %s refused — workspace generation/incarnation "
            "not valid UUIDs (generation=%r runtime_incarnation=%r)",
            thread_id,
            final_workspace_generation,
            final_workspace_runtime,
        )
        return None

    return {
        "thread_id": thread_id,
        # Exact post-0185 contract. The maintenance gate drains old writers;
        # every admitted pinned lifecycle write must carry this identity.
        "pinned_status_identity_contract": 1,
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": (
            final_runtime_authority.generation
            if final_runtime_authority is not None
            else None
        ),
        # Transcript identity is distinct from the workspace/runtime
        # generation. A conversation-only rewind keeps the latter stable while
        # forcing every warm executor to discard its old model context.
        "conversation_revision": int(
            (final_thread or {}).get("conversation_revision") or 0
        ),
        "events_epoch": int((final_thread or {}).get("events_epoch") or 0),
        # Non-secret physical identity used by the dual agent's monotonic
        # pre-setup claim. If actor binding fails before workspace setup, it
        # can echo this exact tuple while proving that setup never began.
        "workspace_generation": final_workspace_generation,
        "workspace_runtime_incarnation": final_workspace_runtime,
        "config_override": None if resolved_config else config_override,
        "resolved_config": resolved_config,
        "project_ids": project_ids,
        "datasources": datasources,
        "config_name": config_name,
        "runtime_actor": runtime_actor.to_payload(),
    }


__all__ = [
    "SessionAttachPayloadDependencies",
    "assemble_session_attach_payload",
]
