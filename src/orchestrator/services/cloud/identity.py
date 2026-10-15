"""Cached cloud-identity resolution.

``resolve_user_identity`` against a main-cloud backend costs up to two
sequential user-search HTTP calls (~2.3 s measured on Nextcloud OCS), and its
result is a stable fact — the user's cloud account id. This module fronts it
with a per-backend cache persisted on ``users.cloud_identity`` (migration
0051) so the price is paid once per user per backend, not per page view.

Caching rules (knowledge-base/knowledge/issues/project_page_open_blocks_on_cloud_heal.md part 2):

* **Positive results persist indefinitely** — a cloud account id is stable.
  A backend switch is a cache miss (entries are keyed by ``backend_id``),
  never a wrong answer. An id the backend refuses as unknown (the account
  was recreated) is replaced by :func:`with_cached_identity`.
* **Negative results are never persisted** — "user hasn't logged into the
  cloud yet" is a valid transient state (see
  ``NextcloudBackend.resolve_user_identity``); the next caller retries.

The helpers take a plain user dict needing only ``id`` + ``email`` /
``display_name`` (a full ``users`` row or a ``project_members`` row both
qualify) and read the cache with a dedicated single-column query, so no
``SELECT`` list anywhere needs widening.
"""

import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any, Optional, TypeVar

from orchestrator.services.cloud.base import MainCloudBackend
from orchestrator.services.cloud.errors import CloudBackendError, CloudBackendErrorKind
from orchestrator.services.cloud.handles import UserId

T = TypeVar("T")

logger = logging.getLogger(__name__)


def _cache_entry(identity: dict[str, Any], backend_id: str) -> dict[str, Any]:
    entry = identity.get(backend_id)
    return entry if isinstance(entry, dict) else {}


async def resolve_user_identity_cached(
    db: Any,
    user: dict[str, Any],
    backend: MainCloudBackend,
) -> Optional[UserId]:
    """Resolve a user's cloud account id, cache-first.

    ``db`` is the app ``Database``; ``user`` needs ``id`` and, for the miss
    path, ``email`` / ``display_name``.
    """
    if not backend.is_initialized or not user.get("id"):
        return None
    user_id = str(user["id"])

    identity = await db.get_user_cloud_identity(user_id)
    cached = _cache_entry(identity, backend.backend_id).get("user_id")
    if cached:
        return UserId(cached)

    resolved = await backend.resolve_user_identity(
        user.get("email"), (user.get("display_name") or "").lower()
    )
    if resolved:
        await db.merge_user_cloud_identity(
            user_id,
            backend.backend_id,
            {
                "user_id": str(resolved),
                "resolved_at": datetime.now(timezone.utc).isoformat(),
            },
        )
    return resolved


async def peek_user_identity(
    db: Any, user_id: str, backend_id: str
) -> Optional[UserId]:
    """Cache-only identity lookup — pure DB, never calls the backend."""
    identity = await db.get_user_cloud_identity(user_id)
    cached = _cache_entry(identity, backend_id).get("user_id")
    return UserId(cached) if cached else None


async def remember_user_identity(
    db: Any,
    user_id: str,
    backend_id: str,
    resolved: UserId,
    *,
    replaces_stale: bool = False,
) -> None:
    """Persist a positive resolution. With ``replaces_stale`` the cached
    home URL, derived from the old id, is dropped too. Best effort: a failed
    write only costs the next caller a lookup."""
    entry: dict[str, Any] = {
        "user_id": str(resolved),
        "resolved_at": datetime.now(timezone.utc).isoformat(),
    }
    if replaces_stale:
        entry["home_browser_url"] = None
    try:
        await db.merge_user_cloud_identity(user_id, backend_id, entry)
    except Exception:
        logger.warning(
            "Cloud identity of user %s on %s was not cached",
            user_id,
            backend_id,
            exc_info=True,
        )


async def with_cached_identity(
    db: Any,
    user: dict[str, Any],
    backend: MainCloudBackend,
    action: Callable[[UserId], Awaitable[T]],
    *,
    lookup: Optional[Callable[[], Awaitable[Optional[UserId]]]] = None,
) -> Optional[T]:
    """Run ``action`` with the user's cloud account id, cache-first.

    On a cache hit the backend is not asked who the user is: ``action`` runs
    with the cached id. Should the backend refuse that id as ``NOT_FOUND``
    (the account was recreated, or the cache predates a reinstall), the user
    is looked up again: a different id replaces the cached one and ``action``
    runs once more with it; the same id, or none, re-raises the refusal.

    On a miss, ``lookup`` (default: ``backend.resolve_user_identity`` on the
    user's email and display name) resolves the id and a positive result is
    cached. Returns ``None`` when the user has no cloud account; ``action``'s
    errors propagate.
    """
    user_id = str(user.get("id") or "")

    async def fresh() -> Optional[UserId]:
        if lookup is not None:
            return await lookup()
        return await backend.resolve_user_identity(
            user.get("email"), (user.get("display_name") or "").lower()
        )

    cached = (
        await peek_user_identity(db, user_id, backend.backend_id) if user_id else None
    )
    if cached:
        try:
            return await action(cached)
        except CloudBackendError as exc:
            if exc.kind != CloudBackendErrorKind.NOT_FOUND:
                raise
            resolved = await fresh()
            if not resolved or str(resolved) == str(cached):
                raise
            logger.info(
                "User %s's cached %s account was refused; it is now %s",
                user_id,
                backend.backend_id,
                resolved,
            )
            await remember_user_identity(
                db, user_id, backend.backend_id, resolved, replaces_stale=True
            )
            return await action(resolved)
    resolved = await fresh()
    if not resolved:
        return None
    if user_id:
        await remember_user_identity(db, user_id, backend.backend_id, resolved)
    return await action(resolved)


async def peek_home_browser_url(
    db: Any, user_id: str, backend_id: str
) -> Optional[str]:
    """Cache-only home-URL lookup — pure DB, never calls the backend."""
    identity = await db.get_user_cloud_identity(user_id)
    return _cache_entry(identity, backend_id).get("home_browser_url") or None


async def get_home_browser_url_cached(
    db: Any,
    user: dict[str, Any],
    backend: MainCloudBackend,
) -> Optional[str]:
    """Resolve a user's personal home-Space browser URL, cache-first.

    On a miss this blocks on the backend (identity resolution + drive
    lookup) — callers on a latency-sensitive path should use
    ``peek_home_browser_url`` and run this in the background instead.
    """
    if not backend.is_initialized or not user.get("id"):
        return None
    user_id = str(user["id"])

    cached = await peek_home_browser_url(db, user_id, backend.backend_id)
    if cached:
        return cached

    resolved = await resolve_user_identity_cached(db, user, backend)
    if not resolved:
        return None
    home = await backend.get_user_home(resolved)
    if not (home and home.browser_url):
        return None
    await db.merge_user_cloud_identity(
        user_id,
        backend.backend_id,
        {"home_browser_url": home.browser_url},
    )
    return home.browser_url
