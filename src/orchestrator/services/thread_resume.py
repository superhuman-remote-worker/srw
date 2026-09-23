"""Thread Resume, detached rewind, and late delivery operations."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, MutableMapping
from dataclasses import dataclass
from functools import partial
import json
import logging
from typing import Any

from fastapi import HTTPException

from orchestrator.schemas.thread_lifecycle import (
    ThreadResumeRequest,
    ThreadRewindRequest,
)
from orchestrator.services.cloud import SessionFolderHandle
from orchestrator.services.cloud.identity import resolve_user_identity_cached
from orchestrator.services.config_drift import (
    DriftItem,
    acknowledged_drift_ids,
    blocking_denials,
    collect_config_drift,
)
from orchestrator.services.datasource_policy import classify_datasource_selection
from orchestrator.services.grant_enforcement import GrantDenied
from orchestrator.services.manifest_runtime_ownership import require_srw_runtime
from orchestrator.services.session_provisioner import ensure_session_workspace
from orchestrator.services.session_runtime_admission import (
    protected_cloud_marker_state,
    same_thread_runtime_authority,
    thread_runtime_authority,
    thread_runtime_refusal_detail,
)
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from orchestrator.services.thread_retirement import ThreadRetirementOperations
from shared.runtime.core.loader import canonical_config_name


@dataclass(frozen=True, slots=True)
class ThreadResumeDependencies:
    """Application state and late-bound policy used by Resume."""

    store: Any
    agent_provisioner: Any
    persistent_provisioner: Any
    container_provisioner: Any
    workspace_suspension_service: Any
    main_cloud_router: Any
    officer_conference_service: Any
    retirement: ThreadRetirementOperations
    late_cloud_setup_tasks: MutableMapping[str, asyncio.Task[None]]
    late_cloud_setup_attach_timeout_s: Callable[[], float]
    classify_thread_project_ids: Callable[..., Awaitable[Any]]
    resolve_session_config: Callable[..., Awaitable[Any]]
    thread_project_ids: Callable[[str], Awaitable[list[str]]]
    require_stateless_workspace: Callable[..., Any]
    require_supported_protected_session_class: Callable[..., Awaitable[Any]]
    thread_workspace_backend: Callable[..., str]
    hold_officer_for_conference: Callable[..., Awaitable[None]]
    is_protected_cloud_mode_enabled: Callable[[], bool]
    schedule_protected_engage: Callable[..., Any]
    should_skip_session_folder: Callable[[list[dict[str, Any]]], bool]
    await_protected_cloud_runtime_ready: Callable[[str], Awaitable[bool]]
    find_idle_persistent_agent: Callable[[], Awaitable[Any]]
    thread_has_knowledge_scope: Callable[..., Awaitable[bool]]
    inject_thread_dispatch_credentials: Callable[..., Awaitable[dict[str, Any]]]
    send_session_attach: Callable[..., Awaitable[bool]]
    emit_session_provisioning_failure: Callable[..., Awaitable[None]]
    thread_uses_pinned_execution: Callable[[Any], bool]
    schedule_stateless_workspace_ensure: Callable[[str], Any]
    create_task: Callable[[Awaitable[Any]], asyncio.Task[Any]]
    agent_get_thread_workspace_locked: Callable[[str], Awaitable[dict[str, Any]]]
    inject_lite_workspace_config: Callable[..., dict[str, Any]]
    logger: logging.Logger


def register_late_cloud_setup(
    thread_id: str,
    task: asyncio.Task[None],
    *,
    dependencies: ThreadResumeDependencies,
) -> None:
    """Publish an in-flight session-folder provisioning task for the attach
    paths to await (see ``_await_late_cloud_setup``).

    Mirrors ``_schedule_protected_engage``'s slot discipline: the done-callback
    only clears the slot while it is still ours, so a newer registration for
    the same thread can't be clobbered by a stale callback.
    """

    _late_cloud_setup_tasks = dependencies.late_cloud_setup_tasks

    _late_cloud_setup_tasks[thread_id] = task

    def _done(finished: "asyncio.Task[None]") -> None:
        if _late_cloud_setup_tasks.get(thread_id) is finished:
            _late_cloud_setup_tasks.pop(thread_id, None)

    task.add_done_callback(_done)


async def await_late_cloud_setup(
    thread_id: str,
    *,
    dependencies: ThreadResumeDependencies,
) -> None:
    """Block until this thread's in-flight session-folder provisioning lands.

    Call this BEFORE binding an agent and OUTSIDE the thread advisory lock —
    it can take seconds, and holding the lock across it would stall the fresh
    pod's own ``POST /api/agents/register``.

    A no-op when nothing is registered for ``thread_id`` in this process:
    provisioning already finished, was never needed, or ran on the other HA
    replica. The cross-replica case doesn't strand the agent — the replica
    that schedules the task is also the one that attaches, so the handle is
    persisted before the attach POST goes out and every later reader (either
    replica) sees it through the DB.
    """

    _late_cloud_setup_tasks = dependencies.late_cloud_setup_tasks
    LATE_CLOUD_SETUP_ATTACH_TIMEOUT_S = dependencies.late_cloud_setup_attach_timeout_s()
    logger = dependencies.logger

    task = _late_cloud_setup_tasks.get(thread_id)
    if task is None:
        return
    try:
        await asyncio.wait_for(
            asyncio.shield(task), timeout=LATE_CLOUD_SETUP_ATTACH_TIMEOUT_S
        )
    except Exception as e:
        logger.warning(
            "Thread %s: awaiting in-flight cloud session-folder provisioning "
            "failed/timed out (%s); attaching anyway — the session may start "
            "with cloud sync degraded.",
            thread_id,
            e,
        )


async def thread_config_drift(
    thread: dict[str, Any],
    metadata: dict[str, Any],
    *,
    owner: dict[str, Any] | None,
    dependencies: ThreadResumeDependencies,
) -> list[DriftItem]:
    """Everything in this thread's stored config that is no longer usable.

    Runs the same classifiers the enforcers wrap, so the dialog can never
    promise something different from what attach will do.

    ``owner`` MUST be the thread's own owner row (looked up from
    ``thread["user_id"]``), never the caller. ``require_thread_owner`` lets
    admins act on threads they do not own, and admins pass every classify_*
    check — passing the caller through would silently report no drift for
    someone else's drifted thread, resume it anyway with no ack recorded,
    and leave the real owner permanently stuck (attach enforces against the
    true owner and refuses, and the cockpit only offers Resume while status
    is ``ended``). ``None`` means a userless internal/system thread: trusted
    exactly like :func:`_revalidate_thread_project_ids` /
    :func:`_revalidate_thread_datasource_selection` treat one, since there is
    no per-user membership or grant that could have drifted.
    """

    postgres_db = dependencies.store
    logger = dependencies.logger
    _thread_project_ids = dependencies.thread_project_ids
    _classify_thread_project_ids = dependencies.classify_thread_project_ids
    _resolve_session_config = dependencies.resolve_session_config

    thread_id = str(thread["id"])
    if owner is None:
        return []
    project_ids = await _thread_project_ids(thread_id)
    project_verdicts = await _classify_thread_project_ids(owner, project_ids)
    allowed_project_ids = [v.project_id for v in project_verdicts if not v.denied]

    datasource_verdicts, _revisions = await classify_datasource_selection(
        postgres_db,
        owner,
        str(owner["id"]),
        metadata.get("datasource_ids"),
        allowed_project_ids,
        None,
        allow_admin_explicit_override=True,
    )

    # workspace_tier / corrupt_revision cannot be acknowledged away (no user
    # action makes either safe), so collect_config_drift below never turns
    # either into a DriftItem — but both still deny at attach. Resuming past
    # them would return 200 and hang at attach, the exact failure shape this
    # feature exists to remove. Refuse here, before any grant probing or
    # status mutation, and without naming which item (same non-enumeration
    # reasoning as the generic denial raised elsewhere on this path).
    if blocking_denials(datasource_verdicts, project_verdicts):
        raise HTTPException(
            status_code=403,
            detail=(
                "This session's configuration cannot be verified and cannot "
                "be resumed. Its connector or project data is invalid rather "
                "than merely unavailable."
            ),
        )

    # Grants are enforced inside the session resolve; run it purely to harvest
    # the violations it would raise at attach.
    status: dict[str, Any] = {}
    try:
        await _resolve_session_config(thread, metadata, status=status)
    except GrantDenied:
        # Expected: this is exactly the signal we came here to harvest.
        pass
    except Exception as exc:
        # We could not determine whether grants drifted. Reporting "no drift"
        # would let the session resume on an unknown state; fail closed with
        # the same generic denial the endpoint used before this feature.
        logger.exception(
            "Thread %s: grant probe failed during drift collection; "
            "refusing to resume on an unknown state",
            thread_id,
        )
        raise HTTPException(
            status_code=403,
            detail="Session configuration could not be verified",
        ) from exc
    grant_violations = status.get("grant_violations") or []

    deleted_ids = [
        v.datasource_id
        for v in datasource_verdicts
        if v.denied and v.reason == "deleted"
    ]
    tombstones = await postgres_db.get_datasource_tombstones(deleted_ids)

    return await collect_config_drift(
        postgres_db,
        thread,
        owner=owner,
        project_ids=project_verdicts,
        datasource_ids=datasource_verdicts,
        grant_violations=grant_violations,
        tombstones=tombstones,
    )


async def resume_thread(
    thread_id: str,
    user: dict[str, Any],
    thread: dict[str, Any],
    body: ThreadResumeRequest | None = None,
    *,
    dependencies: ThreadResumeDependencies,
) -> dict[str, Any]:
    """Resume an ended thread (auth: owner only).

    Resets thread status to 'created' and clears the stale agent_id so that
    a new agent can pick it up. The frontend navigates to the chat page after
    calling this, where the orchestrator will provision or wait for an agent.

    Drifted config (deleted/revoked connectors or projects, withdrawn grants)
    is reported as 428 rather than silently denied; the caller re-POSTs with
    ``acknowledge`` naming the drift ids it accepts losing. See
    knowledge-history/done/session_config_drift_resume.md.
    """

    postgres_db = dependencies.store
    agent_provisioner = dependencies.agent_provisioner
    persistent_provisioner = dependencies.persistent_provisioner
    container_provisioner = dependencies.container_provisioner
    workspace_suspension_service = dependencies.workspace_suspension_service
    main_cloud_router = dependencies.main_cloud_router
    officer_conference_service = dependencies.officer_conference_service
    logger = dependencies.logger
    create_task = dependencies.create_task
    _require_stateless_workspace = dependencies.require_stateless_workspace
    _require_supported_protected_session_class = (
        dependencies.require_supported_protected_session_class
    )
    _thread_workspace_backend = dependencies.thread_workspace_backend
    _thread_config_drift = partial(thread_config_drift, dependencies=dependencies)
    _stateless_retirement_marker = dependencies.retirement.stateless_retirement_marker
    _reconcile_stateless_thread_retirement = (
        dependencies.retirement.reconcile_stateless_thread_retirement
    )
    _hold_officer_for_conference = dependencies.hold_officer_for_conference
    _is_protected_cloud_mode_enabled = dependencies.is_protected_cloud_mode_enabled
    _schedule_protected_engage = dependencies.schedule_protected_engage
    _should_skip_session_folder = dependencies.should_skip_session_folder
    _register_late_cloud_setup = partial(
        register_late_cloud_setup, dependencies=dependencies
    )
    _await_late_cloud_setup = partial(await_late_cloud_setup, dependencies=dependencies)
    _await_protected_cloud_runtime_ready = (
        dependencies.await_protected_cloud_runtime_ready
    )
    _find_idle_persistent_agent = dependencies.find_idle_persistent_agent
    _thread_project_ids = dependencies.thread_project_ids
    _thread_has_knowledge_scope = dependencies.thread_has_knowledge_scope
    _inject_thread_dispatch_credentials = (
        dependencies.inject_thread_dispatch_credentials
    )
    _send_session_attach = dependencies.send_session_attach
    _thread_uses_pinned_execution = dependencies.thread_uses_pinned_execution
    _emit_session_provisioning_failure = dependencies.emit_session_provisioning_failure
    _schedule_stateless_workspace_ensure = (
        dependencies.schedule_stateless_workspace_ensure
    )

    require_srw_runtime(thread)
    from shared.run_queue import LANE_PINNED, LANE_STATELESS

    execution_lane = thread.get("execution_lane")
    if execution_lane == LANE_STATELESS:
        # Resume is topology-neutral for a queue-served thread: validate the
        # workspace/class before mutating, then reset lifecycle state without
        # binding or provisioning a dedicated agent below.
        _require_stateless_workspace(thread)
    elif execution_lane != LANE_PINNED:
        # Unknown future lanes must never inherit either current control plane.
        raise HTTPException(
            status_code=409,
            detail="Session execution lane does not support resume",
        )
    if execution_lane == LANE_PINNED and thread.get("status") in {
        "awaiting_user", "suspended", "created",
    }:
        from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

        idle = VMIdleLifecycleStore(postgres_db)
        open_idle = await idle.get_open_for_thread(thread_id)
        continuation = await idle.get_pending_access_continuation(thread_id)
        if (
            open_idle is not None or continuation is not None
            or thread.get("status") == "suspended"
        ):
            wake = await idle.request_thread_wake(
                thread_id, execution_requested=True,
            )
            if wake is None and open_idle is not None:
                raise HTTPException(
                    status_code=409, detail={"code": "session_idle_wake_held"},
                )
            if wake is not None:
                return {
                    "status": "resuming", "wake_id": str(wake["wake_id"]),
                }
    if thread.get("status") != "ended":
        if (
            execution_lane == LANE_PINNED
            and thread.get("runtime_retirement_token") is not None
        ):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "session_ending",
                    "message": (
                        "This session is still finishing protected runtime "
                        "cleanup. Resume becomes available after settlement."
                    ),
                },
            )
        raise HTTPException(
            status_code=409,
            detail={
                "code": "session_not_ended",
                "message": "This session has already resumed.",
            },
        )

    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "session_metadata_malformed",
                    "message": "This session's stored state is invalid.",
                },
            )
    protected_marker = protected_cloud_marker_state(metadata)
    if protected_marker == "malformed":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "protected_cloud_malformed",
                "message": "Protected cloud session state is invalid.",
            },
        )
    if protected_marker == "on" and _thread_workspace_backend(thread) != "sandbox":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "protected_cloud_unsupported_workspace",
                "message": "Protected cloud sessions require the Container tier.",
            },
        )
    # A legacy ended row may predate create-time materialization of the
    # effective Officer class.  Resolve and reject it before any drift ACK or
    # lifecycle write so a failed resume remains exactly ended.
    await _require_supported_protected_session_class(thread, metadata)
    # Drift must be computed as the THREAD OWNER, never the caller (`user`):
    # require_thread_owner lets admins through for threads they do not own,
    # and admins pass every classify_* check — see _thread_config_drift's
    # docstring for the failure this produced. Same owner lookup and
    # fail-closed-on-missing-row as _revalidate_thread_project_ids.
    owner_id = thread.get("user_id")
    if owner_id:
        owner = await postgres_db.get_user(str(owner_id))
        if owner is None:
            raise HTTPException(
                status_code=403,
                detail="Session configuration could not be verified",
            )
    else:
        owner = None
    # Validation stays ahead of mutation: a thread that cannot resume must not
    # be left half-resumed. Compute drift and raise 428 BEFORE touching the
    # thread's status, same discipline the old revalidate-and-raise calls had.
    drift = await _thread_config_drift(thread, metadata, owner=owner)
    # The ack is durable (spec §3.2): a PRIOR resume already persisted it to
    # metadata.config_drift_ack (below), so it must count here too, not just
    # whatever this particular request body happens to carry — otherwise an
    # item the user already accepted losing is re-reported as outstanding on
    # every subsequent resume. Union, never replace: a genuinely NEW drift
    # id is in neither set, so it still blocks.
    stored_ack = acknowledged_drift_ids(thread.get("metadata"))
    acknowledged = (set(body.acknowledge or []) if body else set()) | stored_ack
    outstanding = {item.id for item in drift} - acknowledged
    if outstanding:
        # Subset, not equality: an item that RECOVERED between prompt and
        # confirm must not force a pointless re-prompt, while an item that
        # newly drifted is never silently acknowledged.
        raise HTTPException(
            status_code=428,
            detail={
                "code": "config_drift",
                "detail": (
                    "Parts of this session's configuration are no longer available"
                ),
                "drift": [
                    {
                        "id": item.id,
                        "kind": item.kind,
                        "reason": item.reason,
                        "label": item.label,
                    }
                    for item in drift
                ],
            },
        )

    if drift:
        await postgres_db.record_thread_config_drift_ack(
            thread_id, {item.id: item.reason for item in drift}
        )

    if execution_lane == LANE_STATELESS:
        try:
            async with postgres_db.stateless_session_workspace_ensure_lock(
                thread_id, wait=True
            ) as resume_owner:
                if not resume_owner:
                    raise HTTPException(
                        status_code=503,
                        detail="Stateless workspace lifecycle lock unavailable",
                    )
                locked_thread = await postgres_db.get_thread(thread_id)
                if not locked_thread:
                    raise HTTPException(status_code=404, detail="Thread not found")
                _require_stateless_workspace(locked_thread)
                locked_metadata = thread_metadata_object(locked_thread)
                if locked_thread.get("status") != "ended":
                    raise HTTPException(
                        status_code=409,
                        detail="Thread lifecycle changed before resume",
                    )
                if "_stateless_workspace_retirement_pending" in locked_metadata:
                    try:
                        marker = _stateless_retirement_marker(locked_thread)
                    except RuntimeError as exc:
                        raise HTTPException(
                            status_code=503,
                            detail="Stateless retirement authority is malformed",
                        ) from exc
                    if marker.get("permanent") is True:
                        raise HTTPException(
                            status_code=409,
                            detail="A permanent thread deletion is still in progress",
                        )
                # Reconcile every ended stateless row, not only explicit
                # in-progress markers. Legacy/pre-protocol rows may be ended
                # while a Ready workspace and shell still exist; reopening one
                # without a terminal ACK would bypass the lifecycle fence.
                await _reconcile_stateless_thread_retirement(
                    thread_id,
                    force=True,
                    permanent=False,
                )
                locked_thread = await postgres_db.get_thread(thread_id)
                if not locked_thread:
                    raise HTTPException(status_code=404, detail="Thread not found")
                locked_metadata = thread_metadata_object(locked_thread)
                if "_stateless_workspace_retirement_pending" in locked_metadata:
                    raise HTTPException(
                        status_code=503,
                        detail="Stateless workspace retirement remains incomplete",
                    )
                if not await postgres_db.resume_thread(thread_id):
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "Thread could not be resumed while workspace cleanup is active"
                        ),
                    )
                thread = await postgres_db.get_thread(thread_id) or locked_thread
                metadata = thread_metadata_object(thread)
        except asyncio.TimeoutError as exc:
            raise HTTPException(
                status_code=503,
                detail="Stateless workspace lifecycle lock acquisition timed out",
            ) from exc
    elif not await postgres_db.resume_thread(thread_id):
        raise HTTPException(
            status_code=409,
            detail="Thread could not be resumed while workspace cleanup is active",
        )

    # The pre-resume owner snapshot is terminal by construction. Every
    # provisioning decision below must use the exact row reopened by the
    # explicit ended->created transaction.
    thread = await postgres_db.get_thread(thread_id)
    resume_runtime_authority = thread_runtime_authority(thread)
    if resume_runtime_authority is None:
        raise HTTPException(
            status_code=409, detail=thread_runtime_refusal_detail(thread)
        )
    metadata = thread_metadata_object(thread)

    async def _resume_runtime_is_current() -> bool:
        return same_thread_runtime_authority(
            await postgres_db.get_thread(thread_id), resume_runtime_authority
        )

    # Reopening a conference re-establishes the officer's hold (centurion.md
    # §4) — the single-writer rule spans the meeting's whole lifetime, not
    # just its first sitting.
    if officer_conference_service.thread_is_conference(thread) and thread.get(
        "project_id"
    ):
        await _hold_officer_for_conference(str(thread["project_id"]), thread_id)

    # F-I2: the Slice A reconciler revokes cloud_ro_mounts grants of ended
    # threads, so an end -> resume cycle would otherwise leave a
    # protected-marked thread permanently mount-less. Re-engage here
    # whenever there's no ACTIVE grant on record — via the same registry
    # ``_schedule_protected_engage`` uses at create, so a concurrent attach
    # can await this task exactly like it would the create-time one.
    if (
        protected_cloud_marker_state(metadata) == "on"
        and _is_protected_cloud_mode_enabled()
    ):
        ro_row = await postgres_db.get_ro_mount_by_thread(thread_id)
        if not ro_row or ro_row.get("status") != "active":
            resume_mount_rows = await postgres_db.list_thread_mounts(thread_id)
            _schedule_protected_engage(
                thread_id,
                user_id=str(user["id"]),
                mount_rows=resume_mount_rows,
                runtime_generation=resume_runtime_authority.generation,
            )

    # Provision cloud session folder if missing (e.g. session was created
    # before the cloud backend was initialized), or retry the share alone
    # if the folder exists but no share_handle was recorded — this unstucks
    # threads where folder creation raced the user's first browser login.
    existing_session_handle = thread.get("main_cloud_session_handle") or thread.get(
        "nc_session_folder"
    )
    needs_share_only = bool(existing_session_handle) and not thread.get(
        "main_cloud_share_handle"
    )
    # Phase 4: if the thread already has a working mount, skip the
    # late-provision branch — the mount is the user-visible cloud surface
    # and a session folder on top would be redundant. The share-retry
    # branch is untouched: it only fires on existing folders, so it
    # remains the recovery path for old session folders that lost their
    # share record.
    try:
        existing_mounts = await postgres_db.list_thread_mounts(thread_id)
    except Exception as e:
        existing_mounts = []
        logger.warning(
            "Thread %s: failed to read thread_mounts during resume late-provision "
            "check (%s); proceeding with default policy.",
            thread_id,
            e,
        )
    needs_full_provision = (
        not existing_session_handle
        and not thread.get("nc_session_folder")
        and not _should_skip_session_folder(existing_mounts)
    )
    if protected_cloud_marker_state(metadata) == "on":
        # Protected sessions use the project reader + overlay exclusively.
        # Never provision/share the legacy live session folder on resume.
        needs_full_provision = False
        needs_share_only = False

    if needs_full_provision or needs_share_only:

        async def _late_cloud_setup(
            tid: str, usr: dict, existing_handle: str | None
        ) -> None:
            if not await _resume_runtime_is_current():
                return
            # This thread already exists and carries its origin backend in
            # main_cloud_backend. Dispatch via for_thread so a
            # later active-backend swap can't re-provision it on the wrong
            # cloud (Issue 16). Mirrors the delete path above (~:12339).
            backend = main_cloud_router.for_thread(thread)
            if not backend.is_initialized and backend.is_configured:
                await backend.ensure_initialized()
                if not await _resume_runtime_is_current():
                    return
            if not backend.is_initialized:
                return
            try:
                if existing_handle:
                    session_handle = SessionFolderHandle.from_db(
                        existing_handle, backend=backend.backend_id
                    )
                else:
                    session_handle = await backend.ensure_session_folder(
                        session_id=tid[:8]
                    )
                    if not await _resume_runtime_is_current():
                        return
                share_handle = None
                resolved_user_id = await resolve_user_identity_cached(
                    postgres_db, usr, backend
                )
                if not await _resume_runtime_is_current():
                    return
                if resolved_user_id:
                    share_handle = await backend.share_session_folder(
                        session_handle, resolved_user_id
                    )
                    if not await _resume_runtime_is_current():
                        return
                if not share_handle and existing_handle:
                    # Share still failed (user hasn't signed into cloud yet).
                    # Don't persist — leaving share_handle NULL lets the next
                    # resume retry once autoprovision has materialised them.
                    return
                if not await postgres_db.update_thread_main_cloud(
                    tid,
                    backend_id=backend.backend_id,
                    backend_instance_id=str(backend.backend_instance_id),
                    session_handle=session_handle.to_db(),
                    share_handle=share_handle.to_db() if share_handle else None,
                    expected_runtime_generation=resume_runtime_authority.generation,
                ):
                    return
                logger.info(
                    "Thread %s: %s cloud session folder",
                    tid,
                    "shared previously-unshared"
                    if existing_handle
                    else "late-provisioned",
                )
            except Exception as e:
                logger.warning(
                    "Thread %s: late cloud folder provisioning failed: %s", tid, e
                )

        _setup_task = create_task(
            _late_cloud_setup(thread_id, user, existing_session_handle)
        )
        if needs_full_provision:
            # Only a FULL provision decides whether the agent can resolve a
            # sync target at all, so only that case is worth making the attach
            # wait. The share-only retry leaves both handle columns untouched
            # — gating on it would add its cloud user-lookup cost to every
            # resume of a thread whose share never landed (i.e. every thread
            # whose owner has not signed into the cloud yet) for no change in
            # what the agent can resolve.
            _register_late_cloud_setup(thread_id, _setup_task)

    # Only the exact pinned lane receives a registered agent. Queue-served
    # sessions restore/recreate their workspace below and wait for user input
    # to create the next run_queue turn.
    if execution_lane == LANE_PINNED and agent_provisioner.is_available:
        config_name = canonical_config_name(thread.get("config_name", "session_base"))

        async def _reprovision(tid: str, cfg: str) -> None:
            """Bind an agent to the resumed thread. Fire-and-forget.

            The whole body is guarded because this runs as its own task after
            the handler returned 200. An exception here — a config_name the
            provisioner boundary refuses, a K8s outage — is otherwise dropped
            by asyncio and the session never becomes ready and never fails.
            Same recording shape as services/provision_or_assign.py.
            """
            try:
                # Let any in-flight session-folder provisioning land before an
                # agent is bound — the agent reads its cloud config within ~150ms
                # of attach and never re-reads. Outside the advisory lock below:
                # this can take seconds and the fresh pod's /register needs that
                # same lock.
                # knowledge-history/done/session_resume_cloud_sync_race_late_provision.md
                await _await_late_cloud_setup(tid)
                if not await _resume_runtime_is_current():
                    return
                if not await _await_protected_cloud_runtime_ready(tid):
                    return
                if not await _resume_runtime_is_current():
                    return

                # Serialise concurrent provisioning attempts for the same
                # thread (knowledge-base/knowledge/issues/persistent_thread_double_provisioning_race.md).
                # A concurrent /prepare or /resume on the same thread blocks
                # here; the second arrival observes the binding written by
                # the first and exits. Lifecycle SSE events for the cockpit's
                # resume progress card come from /api/sessions/{tid}/prepare,
                # which the cockpit drives in parallel with this endpoint.
                async with postgres_db.thread_advisory_lock(tid):
                    cur = await postgres_db.get_thread(tid)
                    if not _thread_uses_pinned_execution(
                        cur
                    ) or not same_thread_runtime_authority(
                        cur, resume_runtime_authority
                    ):
                        logger.warning(
                            "Thread %s: resume reprovision refused for execution lane %r",
                            tid,
                            cur.get("execution_lane") if cur else None,
                        )
                        return
                    if cur and cur.get("agent_id"):
                        logger.info(
                            "Thread %s: already bound to agent %s — "
                            "skipping duplicate reprovision.",
                            tid,
                            cur["agent_id"],
                        )
                        return

                    # Try idle pool agent first (instant attach, no pod boot).
                    idle_agent = await _find_idle_persistent_agent()
                    cur = await postgres_db.get_thread(tid)
                    if not same_thread_runtime_authority(cur, resume_runtime_authority):
                        return
                    if idle_agent:
                        # config_override lives in metadata (no top-level column) and
                        # is stripped of secrets at rest — re-inject from source so the
                        # attach payload carries the agent's keys. Needed in addition
                        # to the workspace-endpoint re-inject because datasource
                        # sessions make `co` truthy, suppressing the agent's
                        # fetch-fallback (persistent_app.py).
                        md = cur.get("metadata") or {}
                        if isinstance(md, str):
                            try:
                                md = json.loads(md)
                            except (json.JSONDecodeError, TypeError):
                                md = {}
                        co = (
                            (md.get("config_override") or {})
                            if isinstance(md, dict)
                            else {}
                        )
                        pids = await _thread_project_ids(tid)
                        include_kb_profile = await _thread_has_knowledge_scope(
                            project_ids=pids,
                            datasource_ids=(
                                md.get("datasource_ids")
                                if isinstance(md, dict)
                                else None
                            ),
                        )
                        co = await _inject_thread_dispatch_credentials(
                            co,
                            user_id=str(cur["user_id"]) if cur.get("user_id") else None,
                            project_id=str(cur["project_id"])
                            if cur.get("project_id")
                            else None,
                            include_kb_profile=include_kb_profile,
                        )
                        # The attach boundary owns the authoritative datasource
                        # re-read. Passing a pre-resolved payload here recreated
                        # credentials from a stale A selection after an A -> B/[]
                        # live edit. Canonical project IDs come from thread_mounts,
                        # never the obsolete thread.project_ids field.
                        ok = await _send_session_attach(
                            idle_agent,
                            tid,
                            co,
                            pids,
                            datasources=None,
                            config_name=cfg,
                            expected_runtime_generation=(
                                resume_runtime_authority.generation
                            ),
                        )
                        if ok:
                            logger.info(
                                "Thread %s: resumed via idle pool agent %s",
                                tid,
                                idle_agent["hostname"],
                            )
                            return

                        # The attach attempt crossed await points.  Do not trust
                        # the snapshot from before it: a lane transition or a
                        # sibling bind must suppress the dedicated-pod fallback.
                        cur = await postgres_db.get_thread(tid)
                        if not _thread_uses_pinned_execution(
                            cur
                        ) or not same_thread_runtime_authority(
                            cur, resume_runtime_authority
                        ):
                            logger.warning(
                                "Thread %s: resume pod fallback refused for "
                                "execution lane %r",
                                tid,
                                cur.get("execution_lane") if cur else None,
                            )
                            return
                        if cur.get("agent_id"):
                            logger.info(
                                "Thread %s: attach reservation lost to agent %s; "
                                "skipping duplicate pod fallback",
                                tid,
                                cur["agent_id"],
                            )
                            return

                    # No idle agent — create a dedicated session pod.
                    pod_name = await agent_provisioner.provision_agent(
                        purpose="session",
                        thread_id=tid,
                        config_name=cfg,
                        expected_runtime_generation=(
                            resume_runtime_authority.generation
                        ),
                    )
                    if pod_name:
                        return

                    logger.error(
                        "Thread %s: resume failed — no idle agents and pod provisioning failed",
                        tid,
                    )
            except Exception as exc:
                logger.exception("Thread %s: resume reprovision failed: %s", tid, exc)
                await _emit_session_provisioning_failure(
                    tid,
                    str(thread.get("user_id") or "") or None,
                    resume_runtime_authority,
                    str(exc),
                )

        create_task(_reprovision(thread_id, config_name))
    elif execution_lane == LANE_PINNED and persistent_provisioner.is_available:
        config_name = canonical_config_name(thread.get("config_name", "session_base"))

        async def _reprovision_legacy(tid: str, cfg: str) -> None:
            """Legacy dedicated-pod resume. Fire-and-forget, so guarded whole.

            Same reasoning as ``_reprovision`` above: a raise inside a detached
            task is swallowed and the session hangs in "provisioning" forever.
            """
            try:
                if not await _resume_runtime_is_current():
                    return
                if not await _await_protected_cloud_runtime_ready(tid):
                    return
                if not await _resume_runtime_is_current():
                    return
                cur = await postgres_db.get_thread(tid)
                if not _thread_uses_pinned_execution(
                    cur
                ) or not same_thread_runtime_authority(cur, resume_runtime_authority):
                    logger.warning(
                        "Thread %s: legacy resume provisioning refused for "
                        "execution lane %r",
                        tid,
                        cur.get("execution_lane") if cur else None,
                    )
                    return
                result = await persistent_provisioner.create_agent_pod(
                    tid,
                    config_name=cfg,
                    expected_runtime_generation=resume_runtime_authority.generation,
                )
                if not await _resume_runtime_is_current():
                    return
                if not result.usable:
                    logger.warning(
                        "Thread %s: legacy persistent resume is %s (%s)",
                        tid,
                        result.status.value,
                        result.failure_class or "no-detail",
                    )
                    await _emit_session_provisioning_failure(
                        tid,
                        str(thread.get("user_id") or "") or None,
                        resume_runtime_authority,
                        f"legacy persistent resume {result.status.value}"
                        f" ({result.failure_class or 'no-detail'})",
                    )
            except Exception as exc:
                logger.exception(
                    "Thread %s: legacy resume reprovision failed: %s", tid, exc
                )
                await _emit_session_provisioning_failure(
                    tid,
                    str(thread.get("user_id") or "") or None,
                    resume_runtime_authority,
                    str(exc),
                )

        create_task(_reprovision_legacy(thread_id, config_name))

    # Ensure the session workspace is provisioned/restored (idempotent): restores
    # a suspended workspace, recreates a failed/missing one. Fire-and-forget so
    # resume stays fast — the agent tolerates a not-yet-ready workspace and the
    # periodic reconcile retries on failure. Queue-served sessions share the
    # input/poll single-flight so a rapid resume + input cannot spawn competing
    # create/adopt operations.
    if execution_lane == LANE_STATELESS:
        _schedule_stateless_workspace_ensure(thread_id)
    else:

        async def _ensure_resumed_workspace() -> None:
            if not await _resume_runtime_is_current():
                return
            await ensure_session_workspace(
                thread_id,
                db=postgres_db,
                provisioner=container_provisioner,
                suspension=workspace_suspension_service,
                expected_runtime_generation=resume_runtime_authority.generation,
            )

        create_task(_ensure_resumed_workspace())

    return {"status": "created", "thread_id": thread_id}


async def rewind_thread_detached(
    thread_id: str,
    user: dict[str, Any],
    thread: dict[str, Any],
    body: ThreadRewindRequest,
    *,
    dependencies: ThreadResumeDependencies,
) -> dict[str, Any]:
    """Rewind a DETACHED session's transcript (auth: owner only).

    knowledge-base/knowledge/features/session_rewind.md §Flow — detached. Conversation mode
    only: file restore needs the agent that holds the workspace, so live
    sessions rewind through the session WebSocket instead, and code modes
    here answer 400 ("resume first"). A bound agent means the in-memory
    authority is live and a DB-only sweep would diverge it → 409.
    """

    postgres_db = dependencies.store

    from shared.run_queue import LANE_STATELESS

    if thread.get("execution_lane") == LANE_STATELESS:
        # Stateless threads intentionally have agent_id=NULL even while their
        # session_turn is queued/leased.  This detached DB-only workflow has no
        # queue/turn fence and can race a live claimant, so fail closed until
        # rewind is modeled as a durable stateless control request.
        raise HTTPException(
            status_code=409,
            detail="Stateless session rewind requires a connected session",
        )
    # mark_orphaned_threads_ended and agent_update_thread_status's ended
    # branch both leave a stale agent_id on real ended threads — status must
    # gate the check too, mirroring update_thread_config's established
    # "connected" predicate, or every ended thread would 409 forever.
    if thread.get("agent_id") and thread.get("status") not in ("suspended", "ended"):
        raise HTTPException(
            status_code=409,
            detail="Session is live — rewind from the session connection",
        )
    if body.mode != "conversation":
        raise HTTPException(
            status_code=400,
            detail="File restore needs a running session — resume it first, "
            "then rewind from the chat",
        )
    row = await postgres_db.get_live_thread_message(thread_id, body.message_id)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail="Message not found (it may already be rewound)",
        )
    if row["role"] != "human":
        raise HTTPException(
            status_code=400, detail="Rewind targets must be user messages"
        )
    result = await postgres_db.apply_thread_rewind(
        thread_id, from_seq=row["seq"], actor=str(user["id"])
    )
    return {
        "rewind_id": result["rewind_id"],
        "swept": result["swept"],
        "prompt": row["content"] or "",
    }


async def resolve_background_push_workspace(
    thread: dict[str, Any],
    *,
    dependencies: ThreadResumeDependencies,
) -> dict[str, Any]:
    """Reuse the existing attested workspace/cloud credential assembly."""

    _require_stateless_workspace = dependencies.require_stateless_workspace
    _agent_get_thread_workspace_locked = dependencies.agent_get_thread_workspace_locked
    _inject_lite_workspace_config = dependencies.inject_lite_workspace_config

    backend = _require_stateless_workspace(thread)
    thread_id = str(thread["id"])
    payload = await _agent_get_thread_workspace_locked(thread_id)
    generation = payload.get("workspace_generation")
    if not generation or payload.get("protected_cloud"):
        raise HTTPException(status_code=409, detail="Background workspace unavailable")
    if backend == "virtual":
        override = _inject_lite_workspace_config(
            {"workspace": {"backend": "virtual"}}, prefix=f"threads/{thread_id}/"
        )
        workspace = {**override["workspace"], "workspace_generation": generation}
    elif (
        backend == "sandbox"
        and payload.get("workspace_provisioner") == "k8s"
        and payload.get("status") == "ready"
    ):
        workspace = {
            "backend": backend,
            "host": payload.get("pod_ip"),
            "port": payload.get("pod_port"),
            "key_path": payload.get("ssh_key_path"),
            "workspace_generation": generation,
            "runtime_incarnation": payload.get("workspace_runtime_incarnation"),
            "host_key_fingerprint": payload.get("workspace_ssh_host_key_fingerprint"),
        }
        if not all(
            workspace.get(key)
            for key in ("host", "runtime_incarnation", "host_key_fingerprint")
        ):
            raise HTTPException(
                status_code=409, detail="Background workspace unavailable"
            )
    else:
        raise HTTPException(status_code=409, detail="Background workspace unavailable")
    return {"workspace": workspace, "cloud_sync": payload.get("cloud_sync")}


@dataclass(frozen=True, slots=True)
class ThreadResumeOperations:
    """Bound Resume and detached-control operations."""

    dependencies: ThreadResumeDependencies

    async def resume_thread(
        self,
        thread_id: str,
        user: dict[str, Any],
        thread: dict[str, Any],
        body: ThreadResumeRequest | None = None,
    ) -> dict[str, Any]:
        return await resume_thread(
            thread_id, user, thread, body, dependencies=self.dependencies
        )

    async def rewind_thread_detached(
        self,
        thread_id: str,
        user: dict[str, Any],
        thread: dict[str, Any],
        body: ThreadRewindRequest,
    ) -> dict[str, Any]:
        return await rewind_thread_detached(
            thread_id, user, thread, body, dependencies=self.dependencies
        )

    def register_late_cloud_setup(
        self, thread_id: str, task: asyncio.Task[None]
    ) -> None:
        register_late_cloud_setup(thread_id, task, dependencies=self.dependencies)

    async def await_late_cloud_setup(self, thread_id: str) -> None:
        await await_late_cloud_setup(thread_id, dependencies=self.dependencies)

    async def resolve_background_push_workspace(
        self, thread: dict[str, Any]
    ) -> dict[str, Any]:
        return await resolve_background_push_workspace(
            thread, dependencies=self.dependencies
        )


__all__ = [
    "ThreadResumeDependencies",
    "ThreadResumeOperations",
    "await_late_cloud_setup",
    "register_late_cloud_setup",
    "resolve_background_push_workspace",
    "resume_thread",
    "rewind_thread_detached",
    "thread_config_drift",
]
