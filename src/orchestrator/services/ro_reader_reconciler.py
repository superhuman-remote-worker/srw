"""Restart-safe reconciliation for attempt-scoped protected readers.

Every authority-creating Nextcloud request has a durable signed pre-dispatch
intent.  A process loss leaves that intent ``planned``; the leader closes its
conservative horizon from PostgreSQL time before attempting remote cleanup.
Reader cleanup is serialized by the thread lifecycle lock and addresses only
the full attempt UUID's reader and group on the immutable backend instance.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Mapping
from typing import Any

from orchestrator.services.cloud.protected_effect_contract import (
    NextcloudEffectFenceIntent,
    NextcloudEffectHorizon,
)
from orchestrator.services.cloud.protected_reader_authority import (
    ProtectedNextcloudReaderGrantPlan,
)
from orchestrator.services.cloud.ro_engage import revoke_ro_mount_attempt
from orchestrator.services.cloud_staging import select_protected_mount
from orchestrator.services.cloud_staging.source_identity import (
    ProtectedMountSourceIdentity,
)
from orchestrator.services.session_runtime_admission import protected_cloud_marker_state

logger = logging.getLogger(__name__)

_LIVE_THREAD_STATUSES = {"created", "active", "awaiting_user", "suspended"}


async def _resolve_backend_instance(*, postgres_db, router, instance_id: str):
    """Resolve one retained installation without provider/active fallback."""

    try:
        return router.for_backend_instance(
            instance_id,
            expected_backend_id="nextcloud",
        )
    except Exception:
        authority = await postgres_db.get_main_cloud_backend_instance(
            instance_id,
            expected_backend_id="nextcloud",
        )
        if authority is None:
            raise RuntimeError("protected reader backend installation is unavailable")
        return await router.resolve_backend_instance(authority)


def _metadata_object(thread: Mapping | None) -> dict:
    if not isinstance(thread, Mapping):
        return {}
    value = thread.get("metadata")
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


async def _active_attempt_is_current(
    *,
    postgres_db,
    row: Mapping,
    plan: ProtectedNextcloudReaderGrantPlan,
) -> bool:
    """Prove an active row still belongs to the live selected T/G/source."""

    thread_id = str(row.get("thread_id") or "")
    thread = await postgres_db.get_thread(thread_id)
    if not isinstance(thread, Mapping):
        return False
    if not (
        str(thread.get("id") or "") == thread_id
        and str(thread.get("user_id") or "") == str(row.get("user_id") or "")
        and thread.get("execution_lane") == "pinned"
        and thread.get("status") in _LIVE_THREAD_STATUSES
        and str(thread.get("runtime_generation") or "")
        == str(row.get("runtime_generation") or "")
        and thread.get("runtime_retirement_token") is None
        and protected_cloud_marker_state(_metadata_object(thread)) == "on"
    ):
        return False
    selected = select_protected_mount(await postgres_db.list_thread_mounts(thread_id))
    return bool(
        isinstance(selected, Mapping)
        and str(selected.get("id") or "") == str(row.get("selected_mount_id") or "")
        and ProtectedMountSourceIdentity.from_mount_row(selected) == plan.source
    )


async def _close_abandoned_effect_intents(*, postgres_db, router) -> int:
    """Close signed intents whose dispatch owner disappeared mid-request."""

    closed = 0
    for row in await postgres_db.list_unclosed_cloud_ro_effect_intents():
        try:
            instance_id = str(row.get("backend_instance_id") or "")
            backend = await _resolve_backend_instance(
                postgres_db=postgres_db,
                router=router,
                instance_id=instance_id,
            )
            key = backend.protected_effect_hmac_key
            config_sha256 = str(row.get("config_sha256") or "")
            if (
                type(key) is not bytes
                or backend.protected_effect_config_sha256 != config_sha256
            ):
                raise RuntimeError(
                    "protected effect retained signing authority is unavailable"
                )
            intent = NextcloudEffectFenceIntent.from_binding(
                row.get("fence_intent"),
                key=key,
                expected_backend_instance_id=instance_id,
                expected_config_sha256=config_sha256,
                expected_engage_attempt=str(row.get("engage_attempt") or ""),
                expected_request_authority_sha256=str(
                    row.get("request_authority_sha256") or ""
                ),
            )
            if intent is None:
                raise RuntimeError("protected effect intent cannot be authenticated")
            db_now = await postgres_db.get_protected_effect_database_time()
            horizon = NextcloudEffectHorizon.capture(
                intent=intent,
                db_dispatch_closed_at=max(db_now, intent.db_dispatched_at),
            )
            if await postgres_db.close_cloud_ro_effect_intent(
                str(row.get("id") or ""),
                expected_thread_id=str(row.get("thread_id") or ""),
                expected_runtime_generation=str(row.get("runtime_generation") or ""),
                expected_engage_attempt=str(row.get("engage_attempt") or ""),
                horizon=horizon,
            ):
                closed += 1
        except BaseException:
            logger.exception(
                "failed to close protected effect intent %s", row.get("id")
            )
    return closed


async def reconcile_orphaned_ro_mounts(*, postgres_db, router) -> int:
    """Reconcile abandoned effects and exact nonterminal reader attempts.

    The return value remains the number of grant rows newly settled to
    ``revoked``. Effect-intent closures are logged separately and do not alter
    that public counter.
    """

    closed = await _close_abandoned_effect_intents(
        postgres_db=postgres_db,
        router=router,
    )
    if closed:
        logger.info("RO reader reconciler closed %d effect intent(s)", closed)

    revoked = 0
    for candidate in await postgres_db.list_unsettled_ro_mount_authorities():
        thread_id = str(candidate.get("thread_id") or "")
        try:
            async with postgres_db.try_thread_advisory_lock(thread_id) as acquired:
                if acquired is not True:
                    continue
                # The row is a one-per-thread projection and may have changed
                # while this sweep waited for the lifecycle lock. Never act on
                # the candidate snapshot without this exact re-read.
                row = await postgres_db.get_ro_mount_by_thread(thread_id)
                if not isinstance(row, Mapping) or str(row.get("id") or "") != str(
                    candidate.get("id") or ""
                ):
                    continue
                plan = ProtectedNextcloudReaderGrantPlan.from_ro_mount_row(row)
                if plan is None:
                    raise RuntimeError("protected reader authority is malformed")
                if row.get("status") == "active" and await _active_attempt_is_current(
                    postgres_db=postgres_db,
                    row=row,
                    plan=plan,
                ):
                    continue
                backend = await _resolve_backend_instance(
                    postgres_db=postgres_db,
                    router=router,
                    instance_id=plan.backend_instance_id,
                )
                if await revoke_ro_mount_attempt(
                    backend=backend,
                    postgres_db=postgres_db,
                    row_id=str(row["id"]),
                    thread_id=thread_id,
                    runtime_generation=str(row.get("runtime_generation") or ""),
                    plan=plan,
                ):
                    revoked += 1
        except BaseException:
            logger.exception(
                "failed to reconcile protected reader attempt %s",
                candidate.get("id"),
            )
    if revoked:
        logger.info("RO reader reconciler revoked %d grant attempt(s)", revoked)
    return revoked


async def ro_reader_reconciler_loop(
    shutdown_event: asyncio.Event, *, store, router: Callable[[], Any]
) -> None:
    """Leader-gated periodic sweep of orphaned protected-mode RO grants.

    Revoke-on-teardown alone is not enough (a crash/killed pod can skip it), so
    this independently revokes any active ``cloud_ro_mounts`` grant whose thread
    is gone/ended (design §8.1.4). Runs every 15 minutes.
    """
    logger.info("RO reader reconciler started")
    while not shutdown_event.is_set():
        try:
            await reconcile_orphaned_ro_mounts(postgres_db=store, router=router())
        except Exception as e:
            logger.error("Error in RO reader reconciler: %s", e)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=900.0)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("RO reader reconciler stopped")


__all__ = ["reconcile_orphaned_ro_mounts", "ro_reader_reconciler_loop"]
