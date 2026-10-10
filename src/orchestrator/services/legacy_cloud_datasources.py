"""The two WebDAV rows the old main-cloud wiring made, retired at startup
(main cloud slice 8).

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
rather than wait for the table work. A row is retired only in exactly the
shape SRW made it: an administrator who renamed, re-described, linked or
re-owned one made it theirs, and it stays. The shape is the row's own
columns and links, never ``manifest_resource_id``: that is the Connector
write-through stamp (D3a), which ``create_datasource`` set on the personal
row at birth and ``migrate_stored_connectors`` sets on every scoped row at
each start, before this pass runs. Each goes through ``delete_datasource``
like any delete: links, leases and driver identities go with it, a tombstone
keeps its name and threads naming it are scrubbed.

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


async def _retire(store: Any, sql: str, *args: Any) -> int:
    async with store.acquire() as conn:
        ids = [str(row["id"]) for row in await conn.fetch(sql, *args)]
    retired = 0
    for datasource_id in ids:
        # Another replica may have taken it first: only a delete counts.
        if await store.delete_datasource(datasource_id):
            retired += 1
    return retired


async def retire_legacy_cloud_datasources(
    store: Any, *, log: Any = None
) -> dict[str, int]:
    """Delete every "Cloud Storage (Personal)" row and the bundled
    ``Nextcloud`` admin-home row still in SRW's own shape, and log one count
    line. Idempotent: a second pass retires nothing."""
    log = log or logger
    counts = {
        "personal": await _retire(store, _PERSONAL_SQL, list(_PERSONAL_DESCRIPTIONS)),
        "bundled_nextcloud": await _retire(store, _BUNDLED_NEXTCLOUD_SQL),
    }
    log.info(
        "Legacy cloud WebDAV connectors retired: %d Cloud Storage (Personal), "
        "%d bundled Nextcloud admin home",
        counts["personal"],
        counts["bundled_nextcloud"],
    )
    return counts


__all__ = ["is_nextcloud_admin_home_url", "retire_legacy_cloud_datasources"]
