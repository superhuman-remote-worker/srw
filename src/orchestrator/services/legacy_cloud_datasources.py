"""The WebDAV rows holding the main cloud's own credential, retired at
startup (main cloud slice 8).

The old main-cloud wiring made two:

* **"Cloud Storage (Personal)"**: what an administrator's ``create_user``
  made for the new user's default project, the user's Nextcloud home reached
  with ``agent-service``'s password. The user's own cloud root
  (``cloud_user_root``) replaced it; nothing creates it any more.
* **The bundled ``Nextcloud`` row**: the chart's bundled-Nextcloud branch fed
  ``init.py`` a global, ownerless WebDAV row at Nextcloud admin's home
  (``dav/files/admin/``). The chart no longer describes it, and ``init.py``
  refuses that shape (:func:`is_nextcloud_admin_home_url`), so an old
  ConfigMap cannot seed it again.

Both carry a service account's credential to a whole cloud home, so they go
rather than wait for the table work. Each is retired in exactly the shape
SRW made it, whatever its login. The shape is the row's own columns and
links, never ``manifest_resource_id``: that is the Connector write-through
stamp (D3a), which ``create_datasource`` set on the personal row at birth
and ``migrate_stored_connectors`` sets on every scoped row at each start,
before this pass runs.

A renamed, re-described or relinked one still holds the same credential, so
a third match goes by the login: any other **ownerless** WebDAV row whose
URL is on the main cloud's host and whose username is one of the accounts
SRW itself signs in as there (``agent-service``, the cloud's admin;
``NextcloudBackend.service_account_logins``) goes too. A row someone owns
is theirs and stays, whatever it holds; so does an ownerless row with
another login or at another host.

Each goes through ``delete_datasource`` like any delete: links, leases and
driver identities go with it, a tombstone keeps its name and threads naming
it are scrubbed.

:func:`retire_legacy_cloud_datasources` runs at every startup, idempotent on
every replica, and logs one count line; once it reads 0 on every
installation for a release it can go.

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md,
slice 8 and "Overnight run: slices 3 to 8" (decisions: Slice 8).
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

#: Nextcloud admin's own WebDAV home, the bundled branch's seed URL.
_ADMIN_HOME_PATH = re.compile(r"/remote\.php/dav/files/admin/?$")

#: The descriptions ``create_user`` gave the personal row (the first one
#: until April 2026).
_PERSONAL_DESCRIPTIONS = ("Personal cloud storage", "Personal Nextcloud file storage")

# Ownerless, global-off, linked to exactly one project and that one a
# user's default project: the row an administrator's create made.
_PERSONAL_SQL = """
SELECT d.id
FROM datasources d
WHERE d.type = 'webdav'
  AND d.name = 'Cloud Storage (Personal)'
  AND d.description = ANY($1::text[])
  AND d.created_by IS NULL
  AND NOT d.is_global
  AND d.job_id IS NULL
  AND d.managed_key IS NULL
  AND (
      SELECT count(*) FROM project_datasources pd WHERE pd.datasource_id = d.id
  ) = 1
  AND EXISTS (
      SELECT 1
      FROM project_datasources pd
      JOIN projects p ON p.id = pd.project_id
      WHERE pd.datasource_id = d.id AND p.is_default
  )
ORDER BY d.id
"""

# Ownerless and global, named as the chart named it, at admin's home, with
# no description and no project link: ``upsert_default_datasource`` wrote
# neither.
_BUNDLED_NEXTCLOUD_SQL = """
SELECT d.id
FROM datasources d
WHERE d.type = 'webdav'
  AND d.name = 'Nextcloud'
  AND d.description IS NULL
  AND d.created_by IS NULL
  AND d.is_global
  AND d.job_id IS NULL
  AND d.managed_key IS NULL
  AND d.connection_url ~ '/remote\\.php/dav/files/admin/?$'
  AND NOT EXISTS (
      SELECT 1 FROM project_datasources pd WHERE pd.datasource_id = d.id
  )
ORDER BY d.id
"""


# Every other ownerless WebDAV row: the third match reads its login.
_OWNERLESS_WEBDAV_SQL = """
SELECT d.id
FROM datasources d
WHERE d.type = 'webdav'
  AND d.created_by IS NULL
  AND d.job_id IS NULL
  AND d.managed_key IS NULL
ORDER BY d.id
"""


def is_nextcloud_admin_home_url(url: str | None) -> bool:
    """Whether a WebDAV URL is Nextcloud admin's own home, the shape no
    seeded row may take."""
    if not url:
        return False
    try:
        path = urlsplit(url.strip()).path
    except ValueError:
        return False
    return bool(_ADMIN_HOME_PATH.search(path))


def main_cloud_service_logins(router: Any) -> frozenset[tuple[str, str]]:
    """The ``(host, username)`` logins of the main cloud's own accounts, from
    its configured backend; none without a router or for a provider that
    describes none."""
    backend = getattr(router, "active", None) if router is not None else None
    return frozenset(getattr(backend, "service_account_logins", None) or ())


def _holds_service_login(
    row: dict[str, Any], logins: frozenset[tuple[str, str]]
) -> bool:
    try:
        host = urlsplit(str(row.get("connection_url") or "").strip()).hostname
    except ValueError:
        return False
    username = (row.get("credentials") or {}).get("username")
    if not host or not isinstance(username, str):
        return False
    return (host.lower(), username.strip().casefold()) in logins


async def _candidate_ids(store: Any, sql: str, *args: Any) -> list[str]:
    async with store.acquire() as conn:
        return [str(row["id"]) for row in await conn.fetch(sql, *args)]


async def _delete(store: Any, ids: list[str]) -> int:
    retired = 0
    for datasource_id in ids:
        # Another replica may have taken it first: only a delete counts.
        if await store.delete_datasource(datasource_id):
            retired += 1
    return retired


async def _retire(store: Any, sql: str, *args: Any) -> int:
    return await _delete(store, await _candidate_ids(store, sql, *args))


async def _retire_service_logins(store: Any, logins: frozenset[tuple[str, str]]) -> int:
    if not logins:
        return 0
    held = []
    for datasource_id in await _candidate_ids(store, _OWNERLESS_WEBDAV_SQL):
        row = await store.get_datasource(datasource_id)
        if row and row.get("created_by") is None and _holds_service_login(row, logins):
            held.append(datasource_id)
    return await _delete(store, held)


async def retire_legacy_cloud_datasources(
    store: Any,
    *,
    service_logins: frozenset[tuple[str, str]] = frozenset(),
    log: Any = None,
) -> dict[str, int]:
    """Delete every "Cloud Storage (Personal)" row and the bundled
    ``Nextcloud`` admin-home row still in SRW's own shape, then every other
    ownerless WebDAV row holding one of ``service_logins``
    (:func:`main_cloud_service_logins`), and log one count line. Idempotent:
    a second pass retires nothing."""
    log = log or logger
    counts = {
        "personal": await _retire(store, _PERSONAL_SQL, list(_PERSONAL_DESCRIPTIONS)),
        "bundled_nextcloud": await _retire(store, _BUNDLED_NEXTCLOUD_SQL),
        "service_login": await _retire_service_logins(store, service_logins),
    }
    log.info(
        "Legacy cloud WebDAV connectors retired: %d Cloud Storage (Personal), "
        "%d bundled Nextcloud admin home, %d other ownerless holding the main "
        "cloud's own login",
        counts["personal"],
        counts["bundled_nextcloud"],
        counts["service_login"],
    )
    return counts


__all__ = [
    "is_nextcloud_admin_home_url",
    "main_cloud_service_logins",
    "retire_legacy_cloud_datasources",
]
