"""A user's own session outbox connector (main cloud slice 6.1).

Every approved user has one ``cloud_outbox`` connector SRW keeps for them,
under the managed key ``cloud-outbox:<user>`` (``shared.connectors.platform``):
``Session outbox``, recorded as created by the user, scope ``all`` (only its
owner may attach it, ``native_user``), a default of the user's sessions
(``auto_attach``) and holding nothing (config ``{}``). Per session it stands
for one folder SRW creates in the main cloud and shares with the user
(``session_cloud_preparation``). The API refuses to create, edit, link,
unlink or delete it (``managed_refusal``); it goes with its user's account
(``manifest_user_retirement``).

:func:`ensure_user_outbox_connector` creates it when a user is admitted (the
first login of an approved user, an approval, an administrator's create);
:func:`backfill_cloud_outbox_connectors` gives every approved user theirs
at startup and logs one count line. Only while a main cloud is configured:
without one there is no folder for an outbox to stand for.

Nothing here fails its caller: a failure is logged and the next pass
retries it.

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md,
"Overnight run: slices 3 to 8" (session outbox).
"""

from __future__ import annotations

import functools
import logging
from typing import Any

from orchestrator.services.connector_drivers.platform import (
    validate_platform_connector,
)
from orchestrator.services.connector_drivers.registry import (
    ConnectorDriverRegistry,
    builtin_connector_drivers,
)
from shared.connectors.platform import CLOUD_OUTBOX, managed_key

logger = logging.getLogger(__name__)

#: The stored type of a user's outbox (``srw.cloud-outbox/v1``).
CLOUD_OUTBOX_TYPE = "cloud_outbox"
#: The name every user's outbox carries.
OUTBOX_NAME = "Session outbox"
#: What :func:`ensure_user_outbox_connector` did, as the backfill counts it.
OUTCOMES: tuple[str, ...] = ("created", "present", "deferred")
#: The most users one backfill pass reads.
_BACKFILL_LIMIT = 100_000


@functools.cache
def _builtin_registry() -> ConnectorDriverRegistry:
    return builtin_connector_drivers()


def _registry(store: Any) -> ConnectorDriverRegistry:
    registry = getattr(store, "connector_drivers", None)
    if isinstance(registry, ConnectorDriverRegistry):
        return registry
    return _builtin_registry()


async def _ensure(store: Any, user_id: str) -> str:
    key = managed_key(CLOUD_OUTBOX, user_id)
    if await store.get_datasource_by_managed_key(key) is not None:
        return "present"
    normalized = await validate_platform_connector(
        _registry(store), CLOUD_OUTBOX_TYPE, name=OUTBOX_NAME, connection_url=None
    )
    created = await store.create_datasource(
        name=OUTBOX_NAME,
        ds_type=CLOUD_OUTBOX_TYPE,
        connection_url=None,
        description=(
            "A folder SRW creates in the main cloud for each of your sessions "
            "and shares with you."
        ),
        credentials=normalized.credentials,
        config=normalized.config or {},
        created_by=user_id,
        read_only=None,
        scope_mode="all",
        auto_attach=True,
        managed_key=key,
    )
    # None: another pass (a replica, a rerun) created it first.
    return "created" if created else "present"


async def ensure_user_outbox_connector(
    store: Any, user_id: Any, *, log: Any = None
) -> str:
    """Create ``user_id``'s outbox connector unless it exists. Returns one
    of :data:`OUTCOMES`; a failure is logged and answers ``deferred``. Never
    raises."""
    log = log or logger
    try:
        return await _ensure(store, str(user_id))
    except Exception as exc:
        log.warning(
            "User %s: session outbox connector not ensured (%s); the next "
            "start retries it",
            user_id,
            exc,
        )
        return "deferred"


async def ensure_outbox_if_cloud(
    store: Any, cloud_router: Any, user_id: Any, *, log: Any = None
) -> str | None:
    """:func:`ensure_user_outbox_connector` when a main cloud is configured
    (``cloud_router.active_instance_id``), else ``None``. Never raises."""
    if getattr(cloud_router, "active_instance_id", None) is None or not user_id:
        return None
    return await ensure_user_outbox_connector(store, user_id, log=log)


async def backfill_cloud_outbox_connectors(
    store: Any, *, active_instance_id: Any, log: Any = None
) -> dict[str, int]:
    """Give every approved user their outbox connector while a main cloud is
    configured. Idempotent: a second pass finds every row ``present``. Logs
    one count line, an error when a user was ``deferred``."""
    log = log or logger
    counts = dict.fromkeys(OUTCOMES, 0)
    if active_instance_id is None:
        log.info("Session outbox connectors: no main cloud, nothing to do")
        return counts
    for user in await store.list_users(limit=_BACKFILL_LIMIT):
        if not user.get("is_approved"):
            continue
        outcome = await ensure_user_outbox_connector(store, user["id"], log=log)
        counts[outcome] += 1
    (log.error if counts["deferred"] else log.info)(
        "Session outbox connectors: %s", counts
    )
    return counts


__all__ = [
    "CLOUD_OUTBOX_TYPE",
    "OUTBOX_NAME",
    "OUTCOMES",
    "backfill_cloud_outbox_connectors",
    "ensure_outbox_if_cloud",
    "ensure_user_outbox_connector",
]
