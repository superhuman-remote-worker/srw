"""What a session's cloud folders need before they are mounted.

Main cloud slices 3 and 6.1. Two things happen here, both outside a
thread's advisory lock (the provisioner's pre-plan step, and the planner
when no lock is held), so the planner itself only reads:

* **The one-time conversion** (:func:`convert_to_connector_rule`). A
  session created before cloud folders became connectors mounted its
  project's ``thread_mounts`` row and the session folder. At its next new
  Pod (or its next in-workspace delivery) it gets the connectors that stand
  for them: its project's own ``project-cloud:`` folder when it mounted a
  project row and has no cloud folder attached, and its owner's
  ``cloud-outbox:`` when it has a session folder and no outbox attached.
  Each is added only if the owner may attach it. The session is then
  stamped ``cloud_mount_rule: "connectors"`` so it never converts again; a
  protected or officer session is left alone (slice 5 moves protected).
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

from orchestrator.services.cloud import CloudBackendError, SessionFolderHandle
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
from orchestrator.services.connector_drivers.cloud_folder import CloudFolderDriver
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


async def convert_to_connector_rule(
    thread: Mapping[str, Any],
    *,
    mount_rows: list[dict[str, Any]] | None,
    store: Any,
) -> dict[str, Any] | None:
    """Give a session created before cloud connectors the connectors that
    stand for what it mounted, once; the thread as it now is, or ``None``
    when nothing was written.

    A connector its owner may not attach (a revoked membership, a project
    without its folder connector yet) is not added; the session is stamped
    anyway, since what it may attach is the connector rule's to report from
    now on. When the selection as a whole no longer authorises (another
    connector of it was revoked), nothing is written and the next pass
    tries again.
    """
    metadata = thread_metadata_object(thread)
    if (
        uses_connector_rule(metadata)
        or protected_cloud_marker_state(metadata) != "off"
        or _officer(metadata)
    ):
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
    if not any(isinstance(c.driver, CloudFolderDriver) for c in attached):
        for row in mount_rows or []:
            if row.get("mount_kind") != "project" or not row.get("source_ref"):
                continue
            try:
                native = await store.get_datasource_by_managed_key(
                    project_cloud_key(row["source_ref"])
                )
            except ValueError:
                native = None
            if native is not None:
                candidates.append(str(native["id"]))
    has_folder = bool(
        thread.get("main_cloud_session_handle") or thread.get("nc_session_folder")
    )
    outbox_id = await owner_outbox_id(store, owner_id) if has_folder else None
    if outbox_id and not any(isinstance(c.driver, CloudOutboxDriver) for c in attached):
        candidates.append(outbox_id)
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
            selection, revisions = await authorize_datasource_selection(
                store,
                owner,
                owner_id,
                ids + added,
                project_ids,
                None,
                allow_admin_explicit_override=owner is not None,
                trusted_system_inheritance=owner is None,
            )
        except DatasourceUnavailableError:
            logger.info(
                "Session %s: its connector selection no longer authorises; "
                "its cloud folders convert at a later pass",
                thread_id,
            )
            return None
        written = await store.set_thread_datasource_ids(
            thread_id,
            selection,
            datasource_policy_revisions=revisions,
            datasource_selection_provenance={
                "origin": "cloud_connector_conversion",
                "creation_path": "cloud_connector_conversion",
                "effective_work_owner_id": owner_id,
                "target_project_ids": project_ids,
                "datasource_ids": selection,
                "policy_revisions": revisions,
                "added": added,
                "resolved_at": _now().isoformat(),
            },
        )
        if not written:
            return None
    patch: dict[str, Any] = {CLOUD_MOUNT_RULE_KEY: CONNECTOR_RULE}
    if outbox_id and has_folder:
        patch[CLOUD_OUTBOX_KEY] = {
            **outbox_record(metadata),
            "connector_id": outbox_id,
            "state": "ready",
        }
    if not await store.merge_thread_cloud_metadata(thread_id, patch):
        return None
    logger.info(
        "Session %s: its cloud folders are its connectors now (added %d: %s)",
        thread_id,
        len(added),
        ", ".join(added) or "none",
    )
    return await store.get_thread(thread_id)


# --------------------------------------------------------------------------- #
# The session's outbox folder
# --------------------------------------------------------------------------- #


def _failure_reason(exc: BaseException) -> str:
    return "create_failed" if isinstance(exc, CloudBackendError) else "error"


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
) -> bool:
    """Create a new session's outbox folder, share it with its owner and
    record both on the session. True when the folder exists.

    The owner's cloud account is ensured first (``ensure_user``), so a
    user's very first session is shared too. A share that does not land
    (the rate limit, an account the cloud does not know yet) leaves the
    share empty for :func:`prepare_session_outbox` to retry. A failure to
    create the folder is recorded as ``cloud_outbox.state: "unavailable"``.
    Never raises.
    """
    try:
        session_handle = await backend.ensure_session_folder(session_id=thread_id[:8])
    except Exception as exc:
        logger.warning(
            "Session %s: its outbox folder was not created: %s", thread_id, exc
        )
        await _record_outbox(
            store,
            thread_id,
            {
                "connector_id": connector_id,
                "state": "unavailable",
                "reason": _failure_reason(exc),
            },
        )
        return False
    share_handle = None
    rate_limited = False
    try:
        resolved_user_id = await backend.ensure_user(
            sub=user.get("keycloak_sub") or "",
            issuer=getattr(backend, "_keycloak_issuer", "") or "",
            email=user.get("email"),
            display_name=user.get("display_name"),
            preferred_username=user.get("preferred_username"),
        )
        if resolved_user_id:
            share_handle, rate_limited = await _share(
                backend, session_handle, resolved_user_id
            )
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
            {"connector_id": connector_id, "state": "unavailable", "reason": "error"},
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
    from orchestrator.services.cloud.identity import resolve_user_identity_cached

    thread_id = str(thread["id"])
    handle = SessionFolderHandle.from_db(handle_str, backend=backend.backend_id)
    # MKCOL answers 405 for a folder that is there: a folder someone removed
    # comes back where SRW recorded it.
    await backend.ensure_session_folder(session_id=handle.native_id.rsplit("/", 1)[-1])
    resolved = await resolve_user_identity_cached(store, dict(owner), backend)
    share_handle = None
    if resolved:
        share_handle, _ = await _share(backend, handle, resolved)
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
    "outbox_selected",
    "owner_outbox_id",
    "prepare_session_outbox",
]
