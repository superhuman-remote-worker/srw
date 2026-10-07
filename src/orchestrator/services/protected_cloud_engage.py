"""Protected-cloud engagement: the fail-closed reader grant behind a session.

Extracted verbatim from ``orchestrator.main`` (R1.B04 lane M). This module is
the generation fence for protected cloud mode, and three properties are the
reason it exists in this shape:

* **Generation fencing.** Every write and every remote grant is bound to one
  ``(thread_id, runtime_generation)`` pair and to the exact selected mount's
  durable identity. ``_schedule_protected_engage`` takes the thread advisory
  lock — End acquires the same lock after closing admission — so no old
  engage can outlive retirement settlement and later revoke a *resumed*
  generation's stable remote grant key.
* **A closed set of error codes.** ``_PROTECTED_CLOUD_ERROR_CODES`` is what a
  credential endpoint is allowed to say. ``_record_protected_error`` refuses
  any other code with ``ValueError`` rather than persisting it, and
  ``_protected_workspace_wait_payload`` returns a deliberately
  credential-free body so a polling agent never learns workspace, repository,
  VM or cloud coordinates from a failure.
* **Fail closed, never fall back.** A refusal records metadata and returns; it
  never raises into the caller and never degrades to a live (agent-service
  credentialed) mount. The session boots with no cloud at all.

Collaborators arrive through :class:`ProtectedCloudEngageDependencies`, rebuilt
per invocation by the application. ``cloud_tasks`` is the application-owned
``CloudTaskRegistry`` that replaces main's ``_protected_engage_tasks`` dict:
holding the task reference fixes the asyncio "fire-and-forget task can be GC'd
mid-flight" hazard, and lets a concurrent reader (``_build_agent_cloud_mount``'s
protected branch, or a resume) await the SAME task instead of racing it with
its own poll loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import urllib.parse
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse
from uuid import UUID, uuid4

import httpx

from orchestrator.services.agent_cloud_mounts import _build_protected_cloud_mount
from orchestrator.services.cloud.protected_reader_authority import (
    ProtectedNextcloudReaderGrantPlan,
)
from orchestrator.services.cloud.ro_engage import (
    RoEngageCleanupPending,
    RoEngageRefused,
    engage_ro_mount,
    revoke_ro_mount_attempt,
)
from orchestrator.services.cloud_staging.source_identity import (
    ProtectedMountSourceIdentity,
)
from orchestrator.services.session_runtime_admission import (
    protected_cloud_marker_state,
    same_thread_runtime_authority,
    thread_runtime_authority,
)
from orchestrator.services.stateless_workspace_gate import thread_metadata_object

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProtectedCloudEngageDependencies:
    """Collaborators for one protected-cloud operation, resolved per invocation.

    ``store`` is main's ``postgres_db``, ``cloud_router`` its
    ``main_cloud_router`` and ``cloud_tasks`` the application-owned
    ``CloudTaskRegistry`` (``protected_engage_get`` /
    ``protected_engage_register``). The protected ``cloud_mount`` payload is
    built by the sibling :mod:`orchestrator.services.agent_cloud_mounts`
    builder, which is pure and therefore imported rather than injected.
    """

    store: Any
    cloud_router: Any
    cloud_tasks: Any
    is_protected_cloud_mode_enabled: Callable[[], bool]
    thread_workspace_backend: Callable[[Any], str | None]


_PROTECTED_CLOUD_ERROR_CODES = frozenset(
    {
        "feature_disabled",
        "malformed_protected_marker",
        "unsupported_workspace_tier",
        "no_protected_mount",
        "engage_refused",
        "engage_failed",
    }
)


def _protected_mount_selection_identity(
    row: Mapping[str, Any] | None,
) -> tuple[str, ...] | None:
    """Return the exact durable identity of one selected thread mount."""

    if not isinstance(row, Mapping):
        return None
    values = (
        row.get("id"),
        row.get("mount_kind"),
        row.get("backend_id"),
        row.get("source_ref"),
        row.get("cloud_handle"),
    )
    if any(value is None for value in values):
        return None
    return tuple(str(value) for value in values)


def _ro_mount_matches_protected_selection(
    ro_row: Mapping[str, Any] | None,
    mount_rows: list[dict[str, Any]] | None,
    *,
    thread_id: str,
    user_id: str,
    runtime_generation: str,
) -> bool:
    """Bind a deliverable grant to its exact selected source and attempt."""

    if not isinstance(ro_row, Mapping) or ro_row.get("status") != "active":
        return False
    try:
        UUID(str(ro_row.get("id")))
        UUID(str(ro_row.get("selected_mount_id")))
    except (TypeError, ValueError):
        return False
    if not isinstance(ro_row.get("etag_baseline"), dict):
        return False
    if str(ro_row.get("backend") or "") != "nextcloud":
        return False
    if str(ro_row.get("auth_kind") or "") != "basic":
        return False
    if not isinstance(ro_row.get("credentials"), str) or not ro_row["credentials"]:
        return False
    if not isinstance(ro_row.get("reader_id"), str) or not ro_row["reader_id"]:
        return False
    if not isinstance(ro_row.get("webdav_url"), str) or not ro_row["webdav_url"]:
        return False
    if str(ro_row.get("user_id") or "") != user_id:
        return False
    if str(ro_row.get("thread_id") or "") != thread_id:
        return False
    if str(ro_row.get("runtime_generation") or "") != runtime_generation:
        return False
    from orchestrator.services.cloud_staging import select_protected_mount

    selected = select_protected_mount(mount_rows or [])
    selected_source = ProtectedMountSourceIdentity.from_mount_row(selected)
    if selected_source is None:
        return False
    if str(ro_row.get("selected_mount_id") or "") != str(selected.get("id") or ""):
        return False
    plan = ProtectedNextcloudReaderGrantPlan.from_ro_mount_row(ro_row)
    if plan is None or plan.source != selected_source:
        return False
    try:
        parsed_url = urlparse(ro_row["webdav_url"])
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            return False
        delivered_segments = [
            urllib.parse.unquote(segment)
            for segment in parsed_url.path.rstrip("/").split("/")
            if segment
        ]
    except (TypeError, ValueError):
        return False
    return delivered_segments[-2:] == [plan.reader_id, plan.mountpoint]


async def _resolve_protected_reader_backend(
    plan: ProtectedNextcloudReaderGrantPlan,
    *,
    dependencies: ProtectedCloudEngageDependencies,
):
    """Resolve the immutable installation captured by a protected attempt."""

    try:
        return dependencies.cloud_router.for_backend_instance(
            plan.backend_instance_id,
            expected_backend_id="nextcloud",
        )
    except Exception:
        authority = await dependencies.store.get_main_cloud_backend_instance(
            plan.backend_instance_id,
            expected_backend_id="nextcloud",
        )
        if authority is None:
            raise RuntimeError("protected reader backend installation is unavailable")
        return await dependencies.cloud_router.resolve_backend_instance(authority)


def _protected_workspace_wait_payload(
    *, state: str, error_code: str | None = None
) -> dict[str, Any]:
    """Return a deliberately credential-free protected attach response.

    A dedicated/warm agent may poll this endpoint while the reader probe is in
    flight.  ``creating`` keeps the existing workspace poll alive, while a
    terminal ``failed`` state lets a new agent stop without ever learning the
    workspace, repository, VM, or cloud coordinates.
    """

    failed = state == "failed"
    return {
        "status": "failed" if failed else "creating",
        "protected_cloud": True,
        "protected_cloud_state": "failed" if failed else "engaging",
        "protected_cloud_error_code": error_code if failed else None,
        "pod_ip": None,
        "pod_name": None,
        "pod_port": None,
        "namespace": None,
        "vm_status": None,
        "vm_ssh_host": None,
        "vm_ssh_port": None,
        "vm_name": None,
        "ssh_key_path": None,
        "workspace_generation": None,
        "workspace_runtime_incarnation": None,
        "workspace_ssh_host_key_fingerprint": None,
        "git_remote_url": None,
        "managed_repository_credentials": None,
        "repositories": None,
        "config_override": None,
        "resolved_config": None,
        "project_ids": [],
        "datasources": None,
        "nc_session_folder": None,
        "cloud_sync": None,
        "cloud_mount": None,
        "cloud_sync_degraded": False,
        "canvas_presentation_available": False,
        "canvas_live_apps_available": False,
        "canvas_shared_browser_available": False,
    }


async def _protected_cloud_delivery_state(
    thread: dict[str, Any],
    metadata: dict[str, Any],
    *,
    dependencies: ProtectedCloudEngageDependencies,
) -> tuple[str, str | None]:
    """Classify whether protected workspace credentials may be delivered."""

    marker_state = protected_cloud_marker_state(metadata)
    if marker_state == "off":
        return "ready", None
    if marker_state == "malformed":
        return "failed", "malformed_protected_marker"
    if dependencies.thread_workspace_backend(thread) != "sandbox":
        return "failed", "unsupported_workspace_tier"
    if not dependencies.is_protected_cloud_mode_enabled():
        return "failed", "feature_disabled"
    stored_code = metadata.get("protected_cloud_error_code")
    if stored_code in _PROTECTED_CLOUD_ERROR_CODES:
        return "failed", str(stored_code)
    # Rows created by older orchestrators carry only the operator-facing raw
    # error.  Treat that as terminal too, but never return its contents over
    # the credential endpoint.
    if metadata.get("protected_cloud_error"):
        return "failed", "engage_failed"
    thread_id = str(thread["id"])
    runtime_authority = thread_runtime_authority(thread)
    if runtime_authority is None:
        return "engaging", None
    row, mount_rows = await asyncio.gather(
        dependencies.store.get_ro_mount_by_thread(thread_id),
        dependencies.store.list_thread_mounts(thread_id),
    )
    if not _ro_mount_matches_protected_selection(
        row,
        mount_rows,
        thread_id=thread_id,
        user_id=str(thread.get("user_id") or ""),
        runtime_generation=runtime_authority.generation,
    ):
        return "engaging", None
    mount = _build_protected_cloud_mount(row, thread_id=thread_id) if row else None
    if mount is None:
        return "engaging", None
    return "ready", None


async def _await_protected_cloud_runtime_ready(
    thread_id: str,
    *,
    timeout_s: float | None = None,
    allow_schedule: bool = True,
    dependencies: ProtectedCloudEngageDependencies,
) -> bool:
    """Wait for the protected reader grant before any runtime reservation.

    Ordinary sessions are a constant-time no-op.  Protected sessions poll the
    durable row as well as the local task registry so the gate works across
    orchestrator replicas.  Lifecycle is re-read on every pass; End cancels
    admission even when the Nextcloud call currently awaited by the engage
    task cannot itself be cancelled immediately.
    """

    if timeout_s is None:
        timeout_s = float(os.environ.get("PROTECTED_CLOUD_ENGAGE_TIMEOUT_S", "300"))
    from orchestrator.services.cloud_staging import select_protected_mount

    deadline = asyncio.get_running_loop().time() + max(0.0, timeout_s)
    scheduled_here = False
    entry_thread = await dependencies.store.get_thread(thread_id)
    runtime_authority = thread_runtime_authority(entry_thread)
    if runtime_authority is None:
        return False
    task_key = (thread_id, runtime_authority.generation)
    while True:
        thread = await dependencies.store.get_thread(thread_id)
        if not same_thread_runtime_authority(thread, runtime_authority):
            return False
        metadata = thread_metadata_object(thread)
        marker_state = protected_cloud_marker_state(metadata)
        if marker_state == "off":
            return True
        if marker_state == "malformed":
            return False
        state, _error_code = await _protected_cloud_delivery_state(
            thread, metadata, dependencies=dependencies
        )
        if state == "ready":
            # The delivery probe awaits the reader row and selected mounts.
            # End/marker mutation is independent of those reads, so prove the
            # lifecycle one last time before this admission helper reports
            # success to any present or future caller.
            current = await dependencies.store.get_thread(thread_id)
            if not same_thread_runtime_authority(current, runtime_authority):
                return False
            current_metadata = thread_metadata_object(current)
            return protected_cloud_marker_state(current_metadata) == "on"
        if state == "failed":
            return False

        # A resume handled by an older replica, or a direct /prepare after an
        # orchestrator restart, may have no local task reference.  Start the
        # same idempotent engage flow once on this replica rather than allowing
        # a mount-less runtime or polling forever with no producer.
        if (
            allow_schedule
            and not scheduled_here
            and dependencies.cloud_tasks.protected_engage_get(task_key) is None
        ):
            user_id = thread.get("user_id")
            if user_id:
                mount_rows = await dependencies.store.list_thread_mounts(thread_id)
                current = await dependencies.store.get_thread(thread_id)
                if not same_thread_runtime_authority(current, runtime_authority):
                    return False
                current_metadata = thread_metadata_object(current)
                if protected_cloud_marker_state(current_metadata) != "on":
                    return False
                current_mount_rows = await dependencies.store.list_thread_mounts(
                    thread_id
                )
                if _protected_mount_selection_identity(
                    select_protected_mount(mount_rows)
                ) != _protected_mount_selection_identity(
                    select_protected_mount(current_mount_rows)
                ):
                    # Selection changed across the scheduling awaits.  Loop
                    # from a new authoritative snapshot; never engage stale A.
                    await asyncio.sleep(0)
                    continue
                _schedule_protected_engage(
                    thread_id,
                    user_id=str(user_id),
                    mount_rows=current_mount_rows,
                    metadata=current_metadata,
                    runtime_generation=runtime_authority.generation,
                    dependencies=dependencies,
                )
                scheduled_here = True

        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return False
        task = dependencies.cloud_tasks.protected_engage_get(task_key)
        if task is not None:
            try:
                await asyncio.wait_for(
                    asyncio.shield(task), timeout=min(0.25, remaining)
                )
            except asyncio.TimeoutError:
                pass
            except Exception:
                # The engage owner records a sanitized terminal code.  Loop
                # once more so that durable state, not task exception shape,
                # decides admission.
                pass
        else:
            await asyncio.sleep(min(0.25, remaining))


def _schedule_protected_engage(
    thread_id: str,
    *,
    user_id: str,
    mount_rows: list[dict[str, Any]] | None,
    metadata: dict[str, Any] | None = None,
    runtime_generation: str,
    dependencies: ProtectedCloudEngageDependencies,
) -> "asyncio.Task[None]":
    """Fire-and-forget schedule of ``_engage_protected_cloud_for_thread``,
    registering the task in the application's protected-engage registry so
    callers racing the attach path (F-I1) or a re-engage on resume (F-I2) can
    await it instead of falling straight through to a bare poll. Shared by
    ``create_thread`` and ``resume_thread`` so both engage paths behave
    identically."""

    async def _run() -> None:
        # Cross-replica serialization. End acquires the same lock after it
        # closes admission, so no old engage can outlive retirement settlement
        # and later revoke a resumed generation's stable remote grant key.
        async with dependencies.store.thread_advisory_lock(thread_id):
            await _engage_protected_cloud_for_thread(
                thread_id,
                user_id=user_id,
                mount_rows=mount_rows,
                metadata=metadata or {},
                runtime_generation=runtime_generation,
                dependencies=dependencies,
            )

    task = asyncio.create_task(_run())
    task_key = (thread_id, runtime_generation)
    # The registry attaches the done-callback that clears the slot only if it
    # is still ours — a newer registration for the same thread_id (e.g. a
    # resume re-engage firing right after a create engage) must not be
    # clobbered by a stale callback.
    dependencies.cloud_tasks.protected_engage_register(task_key, task)
    return task


async def _engage_protected_cloud_for_thread(
    thread_id: str,
    *,
    user_id: str,
    mount_rows: list[dict[str, Any]] | None,
    metadata: dict[str, Any],
    runtime_generation: str,
    dependencies: ProtectedCloudEngageDependencies,
) -> None:
    """Engage protected cloud mode ONCE at thread create (design §3.3/§11.4).

    Picks the first Nextcloud-backed project mount, provisions an attempt-scoped
    reader + group, and runs the fail-closed probe via
    ``engage_ro_mount`` — persisting a ``cloud_ro_mounts`` row on success. On
    refusal, records ``metadata.protected_cloud_error`` so the session boots
    with NO cloud mount (never a live one) and the agent can say why."""
    postgres_db = dependencies.store
    thread = await postgres_db.get_thread(thread_id)
    runtime_authority = thread_runtime_authority(thread)
    if runtime_authority is None or runtime_authority.generation != runtime_generation:
        return
    if not dependencies.is_protected_cloud_mode_enabled():
        await _record_protected_error(
            thread_id,
            "protected cloud mode is disabled on this deployment",
            code="feature_disabled",
            expected_runtime_generation=runtime_generation,
            dependencies=dependencies,
        )
        return
    from orchestrator.services.cloud_staging import select_protected_mount

    row = select_protected_mount(mount_rows)
    if row is None:
        await _record_protected_error(
            thread_id,
            "no Nextcloud project mount to protect",
            code="no_protected_mount",
            expected_runtime_generation=runtime_generation,
            dependencies=dependencies,
        )
        return
    expected_selection = _protected_mount_selection_identity(row)
    expected_source = ProtectedMountSourceIdentity.from_mount_row(row)
    try:
        selected_mount_id = str(UUID(str(row.get("id"))))
    except (TypeError, ValueError):
        selected_mount_id = ""
    if expected_source is None or not selected_mount_id:
        await _record_protected_error(
            thread_id,
            "protected mount authority is malformed",
            code="engage_refused",
            expected_runtime_generation=runtime_generation,
            dependencies=dependencies,
        )
        return
    current_mount_rows = await postgres_db.list_thread_mounts(thread_id)
    current_selected = select_protected_mount(current_mount_rows)
    if (
        expected_selection is None
        or expected_selection != _protected_mount_selection_identity(current_selected)
        or expected_source
        != ProtectedMountSourceIdentity.from_mount_row(current_selected)
    ):
        logger.info(
            "Thread %s: protected mount selection changed before engage; refusing stale grant",
            thread_id,
        )
        return
    try:

        async def _still_admitted() -> bool:
            current = await postgres_db.get_thread(thread_id)
            if not same_thread_runtime_authority(current, runtime_authority):
                return False
            current_metadata = thread_metadata_object(current)
            if protected_cloud_marker_state(current_metadata) != "on":
                return False
            latest_mounts = await postgres_db.list_thread_mounts(thread_id)
            latest_selected = select_protected_mount(latest_mounts)
            return expected_selection == _protected_mount_selection_identity(
                latest_selected
            ) and expected_source == ProtectedMountSourceIdentity.from_mount_row(
                latest_selected
            )

        existing = await postgres_db.get_ro_mount_by_thread(thread_id)
        if _ro_mount_matches_protected_selection(
            existing,
            current_mount_rows,
            thread_id=thread_id,
            user_id=user_id,
            runtime_generation=runtime_generation,
        ):
            # A second replica can enter after the first atomically published
            # this exact active attempt. It is already the completed operation.
            return
        plan: ProtectedNextcloudReaderGrantPlan | None = None
        credentials: str | None = None
        backend = None
        if existing and existing.get("status") in {"engaging", "active", "revoking"}:
            existing_plan = ProtectedNextcloudReaderGrantPlan.from_ro_mount_row(
                existing
            )
            if existing_plan is None:
                raise RoEngageRefused(
                    "existing protected reader authority is malformed"
                )
            existing_backend = await _resolve_protected_reader_backend(
                existing_plan, dependencies=dependencies
            )
            can_continue = (
                existing.get("status") == "engaging"
                and str(existing.get("thread_id") or "") == thread_id
                and str(existing.get("user_id") or "") == user_id
                and str(existing.get("runtime_generation") or "") == runtime_generation
                and str(existing.get("selected_mount_id") or "") == selected_mount_id
                and existing_plan.source == expected_source
                and isinstance(existing.get("credentials"), str)
                and bool(existing.get("credentials"))
            )
            if can_continue:
                plan = existing_plan
                credentials = str(existing["credentials"])
                backend = existing_backend
            else:
                try:
                    settled = await revoke_ro_mount_attempt(
                        backend=existing_backend,
                        postgres_db=postgres_db,
                        row_id=str(existing["id"]),
                        thread_id=str(existing["thread_id"]),
                        runtime_generation=str(existing["runtime_generation"]),
                        plan=existing_plan,
                    )
                except BaseException as exc:
                    raise RoEngageCleanupPending(
                        "prior protected reader cleanup is still pending"
                    ) from exc
                if not settled:
                    raise RoEngageCleanupPending(
                        "prior protected reader effect horizon has not elapsed"
                    )
                if not await _still_admitted():
                    return

        if plan is None:
            plan = ProtectedNextcloudReaderGrantPlan(
                engage_attempt=str(uuid4()),
                backend_instance_id=expected_source.backend_instance_id,
                source=expected_source,
            )
            credentials = secrets.token_urlsafe(32)
            backend = await _resolve_protected_reader_backend(
                plan, dependencies=dependencies
            )
        if backend is None or credentials is None:
            raise RoEngageRefused("protected reader attempt could not be prepared")

        def _reader_client(reader_credentials: str | None, reader_id: str):
            return httpx.AsyncClient(
                base_url=backend._base_url,
                auth=(reader_id, reader_credentials or ""),
                timeout=30.0,
            )

        await engage_ro_mount(
            backend=backend,
            plan=plan,
            credentials=credentials,
            selected_mount_id=selected_mount_id,
            thread_id=thread_id,
            user_id=user_id,
            postgres_db=postgres_db,
            http_client_factory=_reader_client,
            admission_check=_still_admitted,
            expected_runtime_generation=runtime_generation,
        )
        if not await _still_admitted():
            # End can commit in the tiny interval after engage's final check.
            # Revoke the durable/remote grant before this task returns; runtime
            # admission independently remains closed even if cleanup itself is
            # temporarily unavailable.
            mounted = await postgres_db.get_ro_mount_by_thread(thread_id)
            mounted_plan = ProtectedNextcloudReaderGrantPlan.from_ro_mount_row(mounted)
            if mounted_plan == plan and mounted is not None:
                await revoke_ro_mount_attempt(
                    backend=backend,
                    postgres_db=postgres_db,
                    row_id=str(mounted["id"]),
                    thread_id=thread_id,
                    runtime_generation=runtime_generation,
                    plan=plan,
                )
            return
        # Success: clear any stale error from a prior refused/flag-off attempt
        # so the attach-time poll fallback isn't suppressed on other replicas.
        async with postgres_db.acquire() as conn:
            await conn.execute(
                "UPDATE threads SET metadata = COALESCE(metadata,'{}') "
                "- 'protected_cloud_error' - 'protected_cloud_error_code' "
                "WHERE id=$1 AND status IN "
                "('created','active','awaiting_user','suspended') "
                "AND execution_lane='pinned' "
                "AND runtime_generation=$2::uuid "
                "AND runtime_retirement_token IS NULL",
                thread_id,
                runtime_generation,
            )
    except RoEngageCleanupPending as e:
        logger.info("Thread %s: protected cleanup pending: %s", thread_id, e)
    except RoEngageRefused as e:
        await _record_protected_error(
            thread_id,
            f"protected mode refused: {e}",
            code="engage_refused",
            expected_runtime_generation=runtime_generation,
            dependencies=dependencies,
        )
    except Exception as e:  # provisioning error — fail closed, no mount
        logger.warning("Thread %s: protected engage failed: %s", thread_id, e)
        await _record_protected_error(
            thread_id,
            f"protected engage error: {e}",
            code="engage_failed",
            expected_runtime_generation=runtime_generation,
            dependencies=dependencies,
        )


async def _record_protected_error(
    thread_id: str,
    message: str,
    *,
    code: str = "engage_failed",
    expected_runtime_generation: str | None = None,
    dependencies: ProtectedCloudEngageDependencies,
) -> None:
    if code not in _PROTECTED_CLOUD_ERROR_CODES:
        raise ValueError(f"Unknown protected cloud error code: {code}")
    async with dependencies.store.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET metadata = COALESCE(metadata,'{}') || $2::jsonb "
            "WHERE id=$1 AND status IN "
            "('created','active','awaiting_user','suspended') "
            "AND runtime_retirement_token IS NULL "
            "AND ($3::uuid IS NULL OR runtime_generation=$3::uuid)",
            thread_id,
            json.dumps(
                {
                    "protected_cloud_error": message,
                    "protected_cloud_error_code": code,
                }
            ),
            expected_runtime_generation,
        )
