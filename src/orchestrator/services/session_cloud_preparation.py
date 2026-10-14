"""What a session's cloud folders need before they are mounted.

Main cloud slices 3 and 6.1. Two things happen here, both outside a
thread's advisory lock (the provisioner's pre-plan step, and the planner
when no lock is held), so the planner itself only reads:

* **The one-time conversion** (:func:`convert_to_connector_rule`). A
  session created before cloud folders became connectors mounted its
  project's ``thread_mounts`` row and the session folder. At its next new
  Pod (or its next in-workspace delivery) it gets the connectors that stand
  for them: each project's own ``project-cloud:`` folder it mounted a row
  of and has not attached, and its owner's ``cloud-outbox:`` when it has a
  session folder and no outbox attached. Each is added only if the owner
  may attach it; only the additions are authorized, and they are appended
  to the selection as it is then (``add_thread_datasource_ids``, under the
  thread's datasource lock). The session is then stamped
  ``cloud_mount_rule: "connectors"`` so it never converts again, unless a
  connector that stands for one of them is not there yet (a project folder
  in the active installation whose connector the backfill has not made, an
  owner without an outbox connector) or no main cloud is active: it
  converts at a later pass. A protected or officer session is left alone
  (slice 5 moves protected).
* **The session's outbox folder** (:func:`create_session_outbox`,
  :func:`prepare_session_outbox`). Admission creates it only when the
  outbox is selected; a failure is recorded on the session
  (``cloud_outbox.state: "unavailable"`` with a closed reason) and the
  session starts anyway. Before each new Pod the folder is ensured (MKCOL
  is idempotent) and a share that never landed is retried, at most every
  :data:`SHARE_RETRY_SECONDS` (``cloud_outbox.share_retry_at``) and
  waiting at most :data:`SHARE_WAIT_SECONDS` on the provider's share rate
  limit (``cloud.retry.bounded_wait``).

Nothing here fails its caller: every failure is logged, and the next pass
retries it.

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md,
"Slice 3 plan (2026-10-10)" 3.5 and "Overnight run: slices 3 to 8".
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from orchestrator.services.cloud import (
    CloudBackendError,
    CloudBackendErrorKind,
    SessionFolderHandle,
)
from orchestrator.services.cloud.identity import with_cached_identity
from orchestrator.services.cloud.retry import BucketFull, bounded_wait
from orchestrator.services.cloud_connector_mounts import (
    CLOUD_MOUNT_RULE_KEY,
    CLOUD_OUTBOX_KEY,
    CONNECTOR_RULE,
    outbox_record,
    selected_cloud_connectors,
    selected_ids,
    uses_connector_rule,
)
from orchestrator.services.connector_drivers.cloud_outbox import CloudOutboxDriver
from orchestrator.services.officer_metadata import officer_meta_enabled
from orchestrator.services.session_runtime_admission import (
    protected_cloud_marker_state,
)
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from shared.connectors.platform import CLOUD_OUTBOX, managed_key, project_cloud_key

logger = logging.getLogger(__name__)

#: How often a session retries an outbox share that did not land.
SHARE_RETRY_SECONDS = 600
#: How long one attempt waits on the provider's share rate limit.
SHARE_WAIT_SECONDS = 5.0


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _officer(metadata: Mapping[str, Any]) -> bool:
    override = metadata.get("config_override")
    officer = override.get("officer") if isinstance(override, Mapping) else None
    return isinstance(officer, dict) and officer_meta_enabled(officer)


async def owner_outbox_id(store: Any, owner_id: Any) -> str | None:
    """The id of ``owner_id``'s outbox connector, or ``None``."""
    if not owner_id:
        return None
    try:
        row = await store.get_datasource_by_managed_key(
            managed_key(CLOUD_OUTBOX, owner_id)
        )
    except ValueError:
        return None
    return str(row["id"]) if row else None


async def outbox_selected(
    store: Any, owner_id: Any, datasource_ids: list[str] | None
) -> str | None:
    """The owner's outbox id when ``datasource_ids`` holds it, else ``None``:
    whether a session gets a session folder at all."""
    outbox = await owner_outbox_id(store, owner_id)
    ids = {str(value) for value in datasource_ids or []}
    return outbox if outbox is not None and outbox in ids else None


# --------------------------------------------------------------------------- #
# The one-time conversion
# --------------------------------------------------------------------------- #


async def _project_folder_pending(
    store: Any, project_id: str, *, cloud_router: Any
) -> bool:
    """Whether a project the session mounted a row of should have a folder
    connector that is not there yet: it names a folder in the active main
    cloud (the startup backfill or the project's provisioning has not made
    it). One that names none, or one in another installation, gets nothing
    in its place: no wait for it."""
    project = await store.get_project(project_id)
    if not project or not project.get("main_cloud_folder_handle"):
        return False
    active = getattr(cloud_router, "active_instance_id", None)
    if active is None:
        return True
    return str(project.get("main_cloud_backend_instance_id") or "") == str(active)


async def convert_to_connector_rule(
    thread: Mapping[str, Any],
    *,
    mount_rows: list[dict[str, Any]] | None,
    store: Any,
    cloud_router: Any = None,
) -> dict[str, Any] | None:
    """Give a session created before cloud connectors the connectors that
    stand for what it mounted, once; the thread as it now is, or ``None``
    when nothing was written.

    Only the additions are authorized and written, appended to the
    selection as it is when they land (``add_thread_datasource_ids``): a
    connector of the selection that was revoked since stays reported as
    before and never holds the conversion back, and a live edit is never
    undone. A connector its owner may not attach (a revoked membership) is
    not added, and nothing takes its place. The stamp waits while a
    connector that should stand for what the session mounted is not there
    yet (:func:`_conversion_pending`) or no main cloud is active
    (``cloud_router.active_instance_id``): the next pass converts it then.
    """
    metadata = thread_metadata_object(thread)
    if (
        uses_connector_rule(metadata)
        or protected_cloud_marker_state(metadata) != "off"
        or _officer(metadata)
    ):
        return None
    if cloud_router is not None and cloud_router.active_instance_id is None:
        # Which connectors stand for its folders is not known now.
        return None
    from orchestrator.services.datasource_policy import (
        DatasourceUnavailableError,
        authorize_datasource_selection,
    )
    from orchestrator.services.thread_mount_rows import durable_project_ids

    thread_id = str(thread["id"])
    owner_id = str(thread["user_id"]) if thread.get("user_id") else None
    ids = selected_ids(metadata)
    project_ids = durable_project_ids(dict(thread), legacy_mounts=mount_rows)
    attached = await selected_cloud_connectors(
        thread, datasource_ids=ids, project_ids=project_ids, store=store
    )
    candidates: list[str] = []
    pending: list[str] = []
    for row in mount_rows or []:
        if row.get("mount_kind") != "project" or not row.get("source_ref"):
            continue
        try:
            native = await store.get_datasource_by_managed_key(
                project_cloud_key(row["source_ref"])
            )
        except ValueError:
            continue
        if native is not None:
            candidates.append(str(native["id"]))
        elif await _project_folder_pending(
            store, str(row["source_ref"]), cloud_router=cloud_router
        ):
            pending.append(f"project-cloud:{row['source_ref']}")
    has_folder = bool(
        thread.get("main_cloud_session_handle") or thread.get("nc_session_folder")
    )
    outbox_id = await owner_outbox_id(store, owner_id) if has_folder else None
    if outbox_id and not any(isinstance(c.driver, CloudOutboxDriver) for c in attached):
        candidates.append(outbox_id)
    elif has_folder and owner_id and outbox_id is None:
        pending.append(f"cloud-outbox:{owner_id}")
    candidates = [cid for cid in dict.fromkeys(candidates) if cid not in ids]

    added: list[str] = []
    if candidates:
        allowed = await selected_cloud_connectors(
            thread, datasource_ids=candidates, project_ids=project_ids, store=store
        )
        added = [c.connector_id for c in allowed if c.eligible]
    if added:
        owner = await store.get_user(owner_id) if owner_id else None
        try:
            # The additions alone: the rest of the selection is the
            # session's, and attach reports what of it was revoked.
            added, revisions = await authorize_datasource_selection(
                store,
                owner,
                owner_id,
                added,
                project_ids,
                None,
                allow_admin_explicit_override=owner is not None,
                trusted_system_inheritance=owner is None,
            )
        except DatasourceUnavailableError:
            logger.info(
                "Session %s: what stands for its cloud folders no longer "
                "authorises; they convert at a later pass",
                thread_id,
            )
            return None
        written = await store.add_thread_datasource_ids(
            thread_id,
            added,
            datasource_policy_revisions=revisions,
            provenance={
                "cloud_connector_conversion": {
                    "effective_work_owner_id": owner_id,
                    "target_project_ids": project_ids,
                    "added": added,
                    "policy_revisions": revisions,
                    "resolved_at": _now().isoformat(),
                }
            },
        )
        if written is None:
            return None
    patch: dict[str, Any] = {}
    if outbox_id and has_folder:
        patch[CLOUD_OUTBOX_KEY] = {
            **outbox_record(metadata),
            "connector_id": outbox_id,
            "state": "ready",
        }
    if pending:
        logger.info(
            "Session %s: %s not there yet; its cloud folders convert at a later pass",
            thread_id,
            ", ".join(pending),
        )
    else:
        patch[CLOUD_MOUNT_RULE_KEY] = CONNECTOR_RULE
    if patch and not await store.merge_thread_cloud_metadata(thread_id, patch):
        return None
    if not added and not patch:
        return None
    logger.info(
        "Session %s: its cloud folders are its connectors%s (added %d: %s)",
        thread_id,
        "" if not pending else " in part",
        len(added),
        ", ".join(added) or "none",
    )
    return await store.get_thread(thread_id)


# --------------------------------------------------------------------------- #
# The session's outbox folder
# --------------------------------------------------------------------------- #


def _failure_reason(exc: BaseException) -> str:
    if isinstance(exc, CloudBackendError):
        return (
            "folder_exists"
            if exc.kind == CloudBackendErrorKind.ALREADY_EXISTS
            else "create_failed"
        )
    return "error"


def outbox_folder_id(thread_id: str) -> str:
    """The name of a new session's outbox folder under ``sessions/``: the
    session's full id. (Sessions before it used its first eight characters,
    32 random bits; they keep the folder recorded on them.)"""
    return str(thread_id)


async def ensure_new_outbox_folder(
    thread_id: str,
    *,
    connector_id: str,
    backend: Any,
    store: Any,
    recorded_folder: Any = None,
) -> Any:
    """Create a session's outbox folder, which must be new, and return its
    handle.

    A folder already at that name is taken only when this session recorded
    it as the one it was creating (``cloud_outbox.folder``: an earlier
    attempt made it and stopped before recording its handle); otherwise
    ``ALREADY_EXISTS`` is raised and nothing of another's is shared."""
    folder = outbox_folder_id(thread_id)
    claimed = str(recorded_folder or "") == folder
    if not claimed:
        await _record_outbox(
            store,
            thread_id,
            {"connector_id": connector_id, "state": "creating", "folder": folder},
        )
    return await backend.ensure_session_folder(session_id=folder, exclusive=not claimed)


async def _share(
    backend: Any, handle: Any, resolved_user_id: Any
) -> tuple[Any | None, bool]:
    """Share ``handle`` with the user, waiting at most
    :data:`SHARE_WAIT_SECONDS` on the share rate limit: ``(share handle or
    None, rate_limited)``."""
    try:
        with bounded_wait(SHARE_WAIT_SECONDS):
            return await backend.share_session_folder(handle, resolved_user_id), False
    except BucketFull:
        return None, True


async def create_session_outbox(
    thread_id: str,
    user: Mapping[str, Any],
    *,
    connector_id: str,
    backend: Any,
    store: Any,
    recorded_folder: Any = None,
) -> bool:
    """Create a new session's outbox folder, share it with its owner and
    record both on the session. True when the folder exists.

    The folder is ``sessions/<the session's id>`` and must be new
    (:func:`ensure_new_outbox_folder`; ``recorded_folder`` is the one an
    earlier attempt of this session recorded). The owner's cloud account
    id comes from the identity cache; on a miss it is ensured
    (``ensure_user``) and cached, so a user's very first session is shared
    too, and a cached id the cloud refuses is looked up again
    (``with_cached_identity``). A share that does not land (the rate limit,
    an account the cloud does not know yet) leaves the share empty for
    :func:`prepare_session_outbox` to retry. A failure to create the folder
    is recorded as ``cloud_outbox.state: "unavailable"`` with its reason
    (``folder_exists`` for a folder that was already there). Never raises.
    """
    try:
        session_handle = await ensure_new_outbox_folder(
            thread_id,
            connector_id=connector_id,
            backend=backend,
            store=store,
            recorded_folder=recorded_folder,
        )
    except Exception as exc:
        logger.warning(
            "Session %s: its outbox folder was not created: %s", thread_id, exc
        )
        reason = _failure_reason(exc)
        failed: dict[str, Any] = {
            "connector_id": connector_id,
            "state": "unavailable",
            "reason": reason,
        }
        if reason != "folder_exists":
            # Its MKCOL may have landed: the next attempt may take it.
            failed["folder"] = outbox_folder_id(thread_id)
        await _record_outbox(store, thread_id, failed)
        return False
    share_handle = None
    rate_limited = False

    async def ensure_owner() -> Any:
        return await backend.ensure_user(
            sub=user.get("keycloak_sub") or "",
            issuer=getattr(backend, "_keycloak_issuer", "") or "",
            email=user.get("email"),
            display_name=user.get("display_name"),
            preferred_username=user.get("preferred_username"),
        )

    try:
        shared = await with_cached_identity(
            store,
            dict(user),
            backend,
            lambda account: _share(backend, session_handle, account),
            lookup=ensure_owner,
        )
        if shared is not None:
            share_handle, rate_limited = shared
    except Exception as exc:
        logger.warning(
            "Session %s: its outbox folder was not shared yet: %s", thread_id, exc
        )
    try:
        await store.update_thread_main_cloud(
            thread_id,
            backend_id=backend.backend_id,
            backend_instance_id=str(backend.backend_instance_id),
            session_handle=session_handle.to_db(),
            share_handle=share_handle.to_db() if share_handle else None,
        )
    except Exception as exc:
        logger.warning(
            "Session %s: its outbox folder was not recorded: %s", thread_id, exc
        )
        await _record_outbox(
            store,
            thread_id,
            {
                "connector_id": connector_id,
                "state": "unavailable",
                "reason": "error",
                "folder": outbox_folder_id(thread_id),
            },
        )
        return False
    record: dict[str, Any] = {"connector_id": connector_id, "state": "ready"}
    if share_handle is None:
        record["share_retry_at"] = (
            _now() + timedelta(seconds=SHARE_RETRY_SECONDS if rate_limited else 0)
        ).isoformat()
    await _record_outbox(store, thread_id, record)
    return True


async def _record_outbox(store: Any, thread_id: str, record: dict[str, Any]) -> None:
    try:
        await store.merge_thread_cloud_metadata(thread_id, {CLOUD_OUTBOX_KEY: record})
    except Exception:
        logger.warning(
            "Session %s: its outbox state was not recorded", thread_id, exc_info=True
        )


def _retry_due(record: Mapping[str, Any], now: datetime) -> bool:
    raw = record.get("share_retry_at")
    if not raw:
        return True
    try:
        due = datetime.fromisoformat(str(raw))
    except ValueError:
        return True
    if due.tzinfo is None:
        due = due.replace(tzinfo=timezone.utc)
    return now >= due


async def prepare_session_outbox(
    thread: Mapping[str, Any],
    *,
    store: Any,
    cloud_router: Any,
    now: datetime | None = None,
) -> None:
    """Ensure an attached outbox's folder exists and is shared, before the
    session's next Pod is planned. Never raises.

    Only a session under the connector rule with its outbox attached and
    not protected; a folder that is there and shared costs nothing. A
    missing folder is created as admission would; a missing share is
    retried at most every :data:`SHARE_RETRY_SECONDS`.
    """
    metadata = thread_metadata_object(thread)
    if (
        not uses_connector_rule(metadata)
        or protected_cloud_marker_state(metadata) != "off"
    ):
        return
    thread_id = str(thread.get("id") or "")
    owner_id = thread.get("user_id")
    try:
        connector_id = await outbox_selected(store, owner_id, selected_ids(metadata))
        if connector_id is None:
            return
        handle_str = thread.get("main_cloud_session_handle") or thread.get(
            "nc_session_folder"
        )
        if handle_str and thread.get("main_cloud_share_handle"):
            return
        record = outbox_record(metadata)
        now = now or _now()
        if not _retry_due(record, now):
            return
        owner = await store.get_user(str(owner_id))
        if owner is None:
            return
        if handle_str:
            backend = cloud_router.for_thread_optional(dict(thread))
        elif cloud_router.active_instance_id is not None:
            backend = cloud_router.for_owner(owner)
        else:
            backend = None
        if backend is not None and not backend.is_initialized and backend.is_configured:
            await backend.ensure_initialized()
        if backend is None or not backend.is_initialized:
            await _record_outbox(
                store,
                thread_id,
                {
                    **record,
                    "connector_id": connector_id,
                    "state": "unavailable" if not handle_str else "ready",
                    "reason": "backend_unavailable",
                    "share_retry_at": (
                        now + timedelta(seconds=SHARE_RETRY_SECONDS)
                    ).isoformat(),
                },
            )
            return
        if not handle_str:
            await create_session_outbox(
                thread_id,
                owner,
                connector_id=connector_id,
                backend=backend,
                store=store,
                recorded_folder=record.get("folder"),
            )
            return
        await _retry_share(
            thread,
            owner,
            backend=backend,
            handle_str=str(handle_str),
            record={**record, "connector_id": connector_id},
            store=store,
            now=now,
        )
    except Exception:
        logger.warning(
            "Session %s: preparing its outbox failed; the next Pod retries it",
            thread_id,
            exc_info=True,
        )


async def _retry_share(
    thread: Mapping[str, Any],
    owner: Mapping[str, Any],
    *,
    backend: Any,
    handle_str: str,
    record: dict[str, Any],
    store: Any,
    now: datetime,
) -> None:
    thread_id = str(thread["id"])
    handle = SessionFolderHandle.from_db(handle_str, backend=backend.backend_id)
    # MKCOL answers 405 for a folder that is there: a folder someone removed
    # comes back where SRW recorded it.
    await backend.ensure_session_folder(session_id=handle.native_id.rsplit("/", 1)[-1])
    shared = await with_cached_identity(
        store, dict(owner), backend, lambda account: _share(backend, handle, account)
    )
    share_handle = shared[0] if shared is not None else None
    if share_handle is None:
        # The owner has not signed in to the cloud yet, or the rate limit
        # is full: try again later.
        await _record_outbox(
            store,
            thread_id,
            {
                **record,
                "state": "ready",
                "share_retry_at": (
                    now + timedelta(seconds=SHARE_RETRY_SECONDS)
                ).isoformat(),
            },
        )
        return
    await store.update_thread_main_cloud(
        thread_id,
        backend_id=backend.backend_id,
        backend_instance_id=str(backend.backend_instance_id),
        session_handle=handle_str,
        share_handle=share_handle.to_db(),
    )
    await _record_outbox(
        store,
        thread_id,
        {
            key: value
            for key, value in {**record, "state": "ready"}.items()
            if key not in {"share_retry_at", "reason"}
        },
    )
    logger.info("Session %s: shared its outbox folder with its owner", thread_id)


__all__ = [
    "SHARE_RETRY_SECONDS",
    "SHARE_WAIT_SECONDS",
    "convert_to_connector_rule",
    "create_session_outbox",
    "ensure_new_outbox_folder",
    "outbox_folder_id",
    "outbox_selected",
    "owner_outbox_id",
    "prepare_session_outbox",
]
