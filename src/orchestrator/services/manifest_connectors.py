"""Datasources written through to manifest Connector resources (slices D3a
and D3b).

Datasources become Connectors on the Expert precedent
(``manifest_experts``), with one difference: the ``datasources`` row cannot
be emptied.  It keeps identity, sharing, policy and the columns SQL reads
(``type``, ``read_only``, ``policy_revision``, the native marker), and in
D3a every reader still reads it.  The Connector resource is kept in step by
the datasource store's writes, in their transaction, so slice D3c can move
readers onto it.

The mapping (connector_drivers.md, "Datasources become Connectors"):

* the resource uid is the datasource id, and so is ``linked_id``: every
  stored selection, PR authority and drift key already names the Connector;
* ``metadata.name`` is ``<slug>-<12 hex of the id>``, fixed when the resource
  is created, so one owner's ``prod`` Postgres and ``prod`` Neo4j never
  collide.  The free-text name is the ``srw.io/display-name`` annotation and
  the description ``srw.io/description``;
* the scope: a project's own knowledge base belongs to that project; any
  other row to its creator's Account; an ownerless row to its one linked
  project.  An ownerless row linked to no project or to several, when it is
  first written, has no resource and stays on the legacy path;
* once a resource exists it is kept.  Its scope changes in place only with
  the row's ownership (a connector adopted as a project's knowledge base
  moves from its creator's Account to the project); an ownerless row's
  resource stays in the project it was created in, because links are sharing
  and never scope.  Only deleting the row (or its project) retires it;
* ``spec.driver`` is the row's driver (``resource_driver``: ``srw.<type>/v1``,
  a remote MCP server ``srw.mcp-remote/v1``) and ``type`` stays on the row as
  a mirror;
* ``spec.config`` holds the driver-owned ``config`` and what the row holds
  that is not secret, as the driver's ``config_schema`` declares it: the
  ``endpoint`` (the connection URL cut to ``scheme://host[:port]``; userinfo,
  path, query and fragment can all carry a secret, so the full URL stays on
  the row), ``default_branch``, and the credential fields the driver names
  (``credential_config``).  ``cli_hint`` stays on the row (it is often a
  pasted command line), and so does the ``native_project_id`` marker: the
  resource carries the row's managed key in ``srw_resources.platform_managed``;
* ``spec.access`` is ``ReadOnly`` when the row is ``read_only``;
* ``spec.credentials`` names the keys of the Connector's resource secret,
  ``connector-<32 hex of the id>`` in the Connector's scope, which holds the
  row's secrets flattened per driver slot and its full connection URL
  (``connector_secrets``, slice D3b).  The row keeps its encrypted copy for
  rollback; a delivery and Test read the secret and fall back to the row.

The secret is written with the resource, in the same transaction, moves with
it when its scope changes, and is deleted when it is retired.

Sharing (``is_global``, ``scope_mode``, project links, ``auto_attach``) is
never written to a resource.  Nothing here writes ``policy_revision`` or
``project_datasources``: the junction's trigger would bump revisions and
enqueue reconciliation.
"""

from __future__ import annotations

import functools
import logging
import re
from copy import deepcopy
from typing import Any, Mapping
from urllib.parse import urlsplit
from uuid import UUID

from orchestrator.services.connector_secrets import (
    STALE_SECRETS,
    connector_secret_name,
    credential_refs,
    drop_connector_secret,
    secret_values,
    write_connector_secret,
)
from orchestrator.services.manifest_store import ManifestStore, decoded, resource_key
from shared.connectors.platform import (
    NATIVE_PROJECT_CONFIG_KEY,
    managed_key_for,
    native_kb_project,
)
from shared.manifests import API_VERSION, preview_documents
from shared.manifests.resolution import content_revision

logger = logging.getLogger(__name__)

KIND = "Connector"
DISPLAY_NAME = "srw.io/display-name"
DESCRIPTION = "srw.io/description"
#: Rows the startup backfill writes per transaction: one savepoint and an
#: identity lock each, so a batch stays under PostgreSQL's 64 cached
#: subtransactions and holds few advisory locks.
BACKFILL_BATCH = 50
#: Rows whose resource is missing or retired, or was written from another
#: version of the row. Every write-through ends by copying the row's exact
#: ``updated_at`` into the resource's ``linked_updated_at``, so a row in step
#: is skipped. A write that bypassed the write-through (an older orchestrator
#: during a rollout, a project delete's link cascade, a user deletion
#: detaching a knowledge base) gave the row another ``updated_at``. That value
#: is its transaction's start time, which can be *earlier* than the last sync
#: (an older replica's transaction began first, waited for the row lock, and
#: committed after the write-through), so the test is inequality, never
#: "newer". A resource written before D3b (no ``spec.credentials``), or one
#: whose secret is missing, needs its secret. Legacy job clones are never
#: Connectors and are not looked at.
_NEEDS_WORK = """
SELECT d.id FROM datasources d LEFT JOIN srw_resources r ON r.id = d.id
WHERE d.id > $1 AND d.job_id IS NULL
  AND (d.manifest_resource_id IS NULL OR r.id IS NULL
       OR r.deleted_at IS NOT NULL
       OR r.linked_updated_at IS DISTINCT FROM d.updated_at
       OR NOT (r.document->'spec' ? 'credentials')
       OR (r.document->'spec'->'credentials' <> '{}'::jsonb
           AND NOT EXISTS (
               SELECT 1 FROM srw_resource_secrets s
               WHERE s.scope_kind = r.scope_kind AND s.scope_name = r.scope_name
                 AND s.name = 'connector-' || replace(r.id::text, '-', ''))))
ORDER BY d.id
LIMIT $2
"""
#: A DNS name or an IP literal (``urlsplit`` lowercases it); anything else,
#: such as a multi-host list, has no single endpoint.
_HOST = re.compile(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?|[0-9a-f:.]+")


@functools.cache
def _builtin_registry():
    from orchestrator.services.connector_drivers import builtin_connector_drivers

    return builtin_connector_drivers()


def _registry(db, registry=None):
    """The application's drivers (lifecycle binds them to the store)."""
    return registry or getattr(db, "connector_drivers", None) or _builtin_registry()


# =============================================================================
# The mapping (pure)
# =============================================================================


def connector_resource_name(row: Mapping[str, Any], *, full_id: bool = False) -> str:
    """``<slug>-<12 hex of the id>``; ``full_id`` uses all 32 hex digits for
    the rare row whose short name another connector already took."""
    hex_id = str(row["id"]).replace("-", "")
    stem = re.sub(r"[^a-z0-9]+", "-", str(row.get("name") or "").lower()).strip("-")
    stem = stem or "connector"
    if full_id:
        return f"{stem[:30].rstrip('-')}-{hex_id}"
    return f"{stem[:50].rstrip('-')}-{hex_id[:12]}"


def connector_scope(
    row: Mapping[str, Any], project_ids: list[Any] | tuple[Any, ...]
) -> dict[str, str] | None:
    """The scope a row's new Connector lives in, or ``None`` for the legacy
    path."""
    project = native_kb_project(row)
    if project:
        return {"kind": "Project", "name": project}
    if row.get("created_by"):
        return {"kind": "Account", "name": str(row["created_by"])}
    linked = sorted({str(value) for value in project_ids})
    if len(linked) == 1:
        return {"kind": "Project", "name": linked[0]}
    return None


def connector_endpoint(url: Any) -> str | None:
    """``scheme://host[:port]`` of a connection URL, or ``None``.

    Everything else a URL holds can be a secret: the userinfo, a token in the
    path (``/api/mcp/s/<token>/mcp``), any query key, the fragment.  A
    ``jdbc:`` prefix is kept; a URL without a scheme and a host (an scp-style
    git address, a DSN) has no endpoint.
    """
    if not isinstance(url, str):
        return None
    prefix, rest = ("jdbc:", url[5:]) if url.lower().startswith("jdbc:") else ("", url)
    try:
        parts = urlsplit(rest.strip())
        port = parts.port
    except ValueError:
        return None
    host = parts.hostname
    if not parts.scheme or not host or not _HOST.fullmatch(host):
        return None
    host = f"[{host}]" if ":" in host else host
    return f"{prefix}{parts.scheme.lower()}://{host}{f':{port}' if port else ''}"


def connector_config(
    row: Mapping[str, Any],
    credential_config: Mapping[str, Any],
    *,
    declared: frozenset[str] = frozenset({"endpoint", "default_branch"}),
) -> dict[str, Any]:
    """The driver-owned config of a row's Connector; never a secret.

    ``declared`` is the resource driver's config properties: the row's
    ``endpoint`` and ``default_branch`` are mirrored only where its schema
    has them.
    """
    raw = row.get("config")
    config = {
        key: deepcopy(value)
        for key, value in (raw.items() if isinstance(raw, Mapping) else ())
        if key != NATIVE_PROJECT_CONFIG_KEY
    }
    endpoint = connector_endpoint(row.get("connection_url"))
    if endpoint and "endpoint" in declared:
        config["endpoint"] = endpoint
    if row.get("default_branch") and "default_branch" in declared:
        config["default_branch"] = str(row["default_branch"])
    for key, value in credential_config.items():
        config.setdefault(key, deepcopy(value))
    return config


def connector_document(
    row: Mapping[str, Any],
    *,
    scope: Mapping[str, str],
    driver: str,
    credential_config: Mapping[str, Any],
    existing: Mapping[str, Any] | None = None,
    declared: frozenset[str] = frozenset({"endpoint", "default_branch"}),
    credentials: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The Connector document of a row.

    ``existing`` is the resource's current document: its name stays (the
    identity), its other metadata is kept, and the spec is the row's.
    ``credentials`` is the spec's references to the Connector's secret
    (``connector_secrets.credential_refs``).
    """
    if existing is not None:
        document = deepcopy(dict(existing))
        metadata = document["metadata"]
        metadata["scope"] = dict(scope)
    else:
        metadata = {"name": connector_resource_name(row), "scope": dict(scope)}
        document = {"apiVersion": API_VERSION, "kind": KIND, "metadata": metadata}
    annotations = metadata.setdefault("annotations", {})
    annotations[DISPLAY_NAME] = str(row.get("name") or "")
    if row.get("description"):
        annotations[DESCRIPTION] = str(row["description"])
    else:
        annotations.pop(DESCRIPTION, None)
    spec: dict[str, Any] = {
        "driver": driver,
        "config": connector_config(row, credential_config, declared=declared),
    }
    if credentials is not None:
        spec["credentials"] = deepcopy(dict(credentials))
    if row.get("read_only") is True:
        spec["access"] = "ReadOnly"
    document["spec"] = spec
    return document


def declared_config(spec: Any) -> frozenset[str]:
    """The config properties a driver spec's schema names."""
    properties = (getattr(spec, "config_schema", None) or {}).get("properties")
    return frozenset(properties or ())


# =============================================================================
# Persistence (inside the datasource write's transaction)
# =============================================================================


async def _load_row(db, datasource_id: UUID) -> dict[str, Any] | None:
    from orchestrator.database.postgres import _datasource_row_to_dict

    raw = await db.fetchrow(
        "SELECT * FROM datasources WHERE id=$1 FOR UPDATE", datasource_id
    )
    return _datasource_row_to_dict(raw) if raw else None


async def _own_resource(db, datasource_id: UUID) -> dict[str, Any] | None:
    """The row's resource, live or retired (its uid is the datasource id)."""
    resource = decoded(
        await db.fetchrow(
            "SELECT * FROM srw_resources WHERE id=$1 FOR UPDATE", datasource_id
        )
    )
    if resource is not None and (
        resource["kind"] != KIND or str(resource.get("linked_id")) != str(datasource_id)
    ):
        raise RuntimeError(
            "A resource with a connector's uid is not that connector's; "
            "reconcile it before writing the connector."
        )
    return resource


async def _retire(db, resource: Mapping[str, Any] | None) -> bool:
    """Soft-delete a deleted row's live resource, without the
    execution-retirement wait, and delete its secret.

    Executions record their connectors by datasource id, never as resource
    dependencies, so nothing waits on a Connector (decision 12): a session
    that still names it recovers through the tombstone and drift flow.
    """
    if resource is None or resource.get("deleted_at") is not None:
        return False
    await drop_connector_secret(
        db, resource["id"], resource["document"]["metadata"]["scope"]
    )
    await db.execute(
        "UPDATE srw_resources SET deleted_at=now(),updated_at=now(),"
        "resource_version=resource_version+1 WHERE id=$1",
        resource["id"],
    )
    return True


async def _scope(db, row, live) -> dict[str, str] | None:
    """Where the row's resource lives: its owner decides, except that an
    ownerless row's resource keeps the scope it was created in."""
    if live is not None and not native_kb_project(row) and not row.get("created_by"):
        return dict(live["document"]["metadata"]["scope"])
    links = await db.fetch(
        "SELECT project_id FROM project_datasources WHERE datasource_id=$1",
        row["id"],
    )
    scope = connector_scope(row, [link["project_id"] for link in links])
    if scope and scope["kind"] == "Project":
        if not await db.fetchval(
            "SELECT EXISTS(SELECT 1 FROM projects WHERE id=$1)", UUID(scope["name"])
        ):
            return None
    return scope


async def _free_name(store: ManifestStore, row, scope, name: str, uid: UUID) -> str:
    occupant = await store.by_name(KIND, scope, name)
    if occupant is None or occupant["id"] == uid:
        return name
    return connector_resource_name(row, full_id=True)


async def persist_connector_resource(db, datasource_id: Any, *, registry=None) -> str:
    """Write a row's Connector resource and its secret from the row; return
    what happened.

    Call inside ``transaction_scope``: a failure here rolls the datasource
    write back with it.  Outcomes: ``created``, ``updated`` (the resource or
    its secret changed), ``unchanged``, ``legacy`` (no resource for this
    row) and ``deleted`` (the row is gone; its resource is retired and its
    secret deleted).
    """
    uid = UUID(str(datasource_id))
    store = ManifestStore(db)
    await store.lock_catalog()
    row = await _load_row(db, uid)
    current = await _own_resource(db, uid)
    if row is None:
        await _retire(db, current)
        return "deleted"
    drivers = _registry(db, registry)
    # A registered image driver's connector names its registration's driver
    # and keeps the config its spec declares (D6).
    from orchestrator.services.connector_driver_registrations import driver_for_row

    driver = await driver_for_row(db, drivers, row)
    if row.get("job_id") or driver is None:
        # A legacy clone-to-job row (frozen since 0083) or a type no driver
        # serves: never a Connector.
        return "legacy"
    live = current if current and current.get("deleted_at") is None else None
    scope = await _scope(db, row, live)
    if scope is None:
        return "legacy"

    credentials = row.get("credentials")
    credentials = credentials if isinstance(credentials, Mapping) else {}
    driver_name = driver.resource_driver(credentials)
    named = drivers.get(driver_name)
    secret = secret_values(driver, row)
    document = connector_document(
        row,
        scope=scope,
        driver=driver_name,
        credential_config=driver.credential_config(credentials),
        existing=live["document"] if live else None,
        declared=declared_config(named.spec if named else driver.spec),
        credentials=credential_refs(connector_secret_name(uid), secret),
    )
    metadata = document["metadata"]
    metadata["name"] = await _free_name(store, row, scope, metadata["name"], uid)
    owner_id = row.get("created_by")
    if scope["kind"] == "Account":
        owner_id, project_id = scope["name"], None
    else:
        project_id = scope["name"]
    key = managed_key_for(row)
    relocated = current is not None and (
        current.get("deleted_at") is not None
        or resource_key(current["document"]) != resource_key(document)
    )
    if relocated:
        # The secret follows the resource into its new scope.
        old_scope = current["document"]["metadata"]["scope"]
        if dict(old_scope) != dict(scope):
            await drop_connector_secret(db, uid, old_scope)
        current = await _relocate(db, store, current, document, owner_id, project_id)
    resolved = preview_documents([document])["resolved"][0]
    await store.lock_identity(document)
    resource, changed = await store.save(
        document,
        resolved,
        content_revision(resolved["spec"]),
        [],
        owner_id=owner_id,
        project_id=project_id,
        linked_id=uid,
        uid=uid,
        expected_version=current["resource_version"] if current else None,
        platform_managed=key,
    )
    secret_changed = await write_connector_secret(
        db, uid, scope, owner_id=owner_id, values=secret
    )
    if resource.get("platform_managed") != key:
        # A row that became platform-owned after its resource was written
        # (a connector adopted as a project's knowledge base).
        await db.execute(
            "UPDATE srw_resources SET platform_managed=$2 WHERE id=$1", uid, key
        )
    await db.execute(
        """UPDATE datasources SET manifest_resource_id=$2,
               managed_key=CASE
                   WHEN managed_key IS NOT NULL OR $3::text IS NULL THEN managed_key
                   WHEN EXISTS (SELECT 1 FROM datasources other
                                WHERE other.managed_key=$3::text AND other.id<>$1)
                   THEN NULL
                   ELSE $3::text
               END
           WHERE id=$1
             AND (manifest_resource_id IS DISTINCT FROM $2
                  OR (managed_key IS NULL AND $3::text IS NOT NULL
                      AND NOT EXISTS (SELECT 1 FROM datasources other
                                      WHERE other.managed_key=$3::text
                                        AND other.id<>$1)))""",
        uid,
        resource["id"],
        key,
    )
    # The resource is now in step with this version of the row (read after
    # the write above, which may itself have bumped it): record it, so the
    # startup backfill skips the row until its updated_at changes again.
    await db.execute(
        """UPDATE srw_resources r SET linked_updated_at = d.updated_at
           FROM datasources d
           WHERE r.id=$1 AND d.id=$1
             AND r.linked_updated_at IS DISTINCT FROM d.updated_at""",
        uid,
    )
    if current is None:
        return "created"
    return "updated" if changed or relocated or secret_changed else "unchanged"


async def _relocate(db, store, current, document, owner_id, project_id):
    """Move a row's resource to its new identity, reviving a retired one.

    The uid stays the datasource id: a connector adopted as a project's
    knowledge base leaves its creator's Account for the project, and an
    ownerless row whose project was deleted comes back in its new one.
    """
    await store.lock_identity(current["document"])
    metadata = document["metadata"]
    return decoded(
        await db.fetchrow(
            """UPDATE srw_resources SET scope_kind=$2,scope_name=$3,name=$4,
               owner_id=$5,project_id=$6,deleted_at=NULL,updated_at=now(),
               resource_version=resource_version+1
               WHERE id=$1 RETURNING *""",
            current["id"],
            metadata["scope"]["kind"],
            metadata["scope"]["name"],
            metadata["name"],
            UUID(str(owner_id)) if owner_id else None,
            UUID(str(project_id)) if project_id else None,
        )
    )


# =============================================================================
# The startup backfill
# =============================================================================


async def migrate_stored_connectors(
    db, *, registry=None, batch_size: int = BACKFILL_BATCH
) -> dict[str, int]:
    """Write the Connector and its secret of every row that needs them;
    idempotent.

    Runs at startup between ``migrate_stored_experts`` and
    ``migrate_projects``, on every replica.  It only touches rows whose
    resource is missing, retired, written from another version of the row
    or without its secret (``_NEEDS_WORK``), in
    batches of ``batch_size``, each its own transaction under the catalog
    lock with one savepoint per row.  That also reconciles what an older
    orchestrator wrote without the write-through during a rollout.  Then one
    statement retires the resources whose row is gone, and another deletes
    the secrets of retired Connectors and those left in a scope a Connector
    moved out of.  A row that cannot be
    written is logged and left for the next start; it does not stop this one.
    Rows on the legacy path are looked at again on every start: they have no
    resource to be in step with.
    """
    counts = dict.fromkeys(
        ("created", "updated", "unchanged", "legacy", "deleted", "deferred"), 0
    )
    after = UUID(int=0)
    while True:
        async with db.transaction_scope():
            await ManifestStore(db).lock_catalog()
            batch = [
                row["id"] for row in await db.fetch(_NEEDS_WORK, after, batch_size)
            ]
            for datasource_id in batch:
                try:
                    async with db.transaction_scope():
                        outcome = await persist_connector_resource(
                            db, datasource_id, registry=registry
                        )
                except Exception:
                    logger.exception(
                        "Connector resource for datasource %s not written; it "
                        "stays on the legacy path until the next write or start",
                        datasource_id,
                    )
                    outcome = "deferred"
                counts[outcome] += 1
        if len(batch) < batch_size:
            break
        after = batch[-1]
    async with db.transaction_scope():
        await ManifestStore(db).lock_catalog()
        retired = await db.execute(
            """UPDATE srw_resources r SET deleted_at=now(), updated_at=now(),
                   resource_version=r.resource_version+1
               WHERE r.kind=$1 AND r.linked_id IS NOT NULL AND r.deleted_at IS NULL
                 AND NOT EXISTS (SELECT 1 FROM datasources d WHERE d.id=r.linked_id)""",
            KIND,
        )
        # Their secrets, and any a write without D3b left behind.
        stale = await db.execute(STALE_SECRETS)
    counts["deleted"] += int(retired.split()[-1])
    if stale != "DELETE 0":
        logger.info("Connector resources: dropped stale secrets (%s)", stale)
    if counts["deferred"]:
        logger.error("Connector resources: %s", counts)
    else:
        logger.info("Connector resources: %s", counts)
    return counts
