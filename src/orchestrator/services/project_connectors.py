"""Project manifests list their connector links as Connector refs (slice D3c).

A Project's ``spec.resources.connectors`` lists the connectors linked to it
(``project_datasources``).  Each link is a ``ref`` to the connector's
Connector resource (``manifest_connectors``: its uid is the datasource id),
by the resource's name and scope, under the resource's name as alias.  A row
on the legacy path has no resource, so it keeps the old inline
``srw.datasource/v1`` entry: the row fallback.

**Refreshed with the links.**  The datasource store's write-through calls
``refresh_project_connectors`` for every Project a write linked or unlinked
(link, unlink, a policy edit of a connector's projects, a connector created
with links or deleted), in the write's transaction, naming the connector the
write unlinked from each.  A legacy-authored Project (one with a source
recipe) is rebuilt from its rows.  A natively authored one keeps what its
author wrote: it loses only the entries naming the connector this write
unlinked (and their aliases in every binding), and gains entries for links it
does not list.  An entry naming a connector that was never linked (before
D3c an apply linked nothing) is the author's, and stays.  A refresh never
fails the write that caused it: it runs in a savepoint, and a Project it
could not refresh is logged and brought in step by the startup heal.

**Applied with the links.**  Applying a Project manifest links the connectors
it newly names and unlinks those its previous revision named and it drops
(``sync_project_connector_links``), with the link API's own authority
checks; entries it carries over are left as they are.  The project's own
knowledge base is never unlinked.  Changing ``defaults.connectors`` needs the
authority the Settings API needs: a Project owner or an administrator.

**Older Projects.**  A stored legacy-authored Project that still holds inline
``datasource-<hex>`` children is read like any entry (``entry_datasource_ids``
understands the inline driver), and the startup heal
(``heal_project_connectors``) rebuilds it with refs and retires the children.
The heal never edits a natively authored Project: it reports the ones whose
entries differ from their links.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
import logging
from typing import Any, Iterable, Mapping
from uuid import UUID

from fastapi import HTTPException

from orchestrator.services.manifest_execution_retirement import (
    execution_references_block_retirement,
)
from orchestrator.services.manifest_store import ManifestStore, resource_key
from shared.connectors.builtin import spec_for_row
from shared.connectors.platform import platform_owned
from shared.manifests import API_VERSION, validate_documents
from shared.manifests.resolution import content_revision

logger = logging.getLogger(__name__)

KIND = "Connector"
DATASOURCE_DRIVER = "srw.datasource/v1"

#: The project's links with their Connector resources (uid = datasource id).
_LINKS = """
SELECT d.id, d.policy_revision, r.name AS resource_name,
       r.scope_kind, r.scope_name
FROM project_datasources pd
JOIN datasources d ON d.id = pd.datasource_id
LEFT JOIN srw_resources r
       ON r.id = d.id AND r.kind = 'Connector' AND r.linked_id = d.id
      AND r.deleted_at IS NULL
WHERE pd.project_id = $1
ORDER BY d.id
"""

_REFRESH_SUSPENDED: ContextVar[bool] = ContextVar(
    "srw_project_connector_refresh_suspended", default=False
)


def _hex(value: Any) -> str:
    return str(value).replace("-", "")


def datasource_binding(datasource_id: Any) -> dict:
    """How a datasource's Connector binds: the resolved form of a ref to it.

    The SRW adapter admits it by ``config.datasourceId`` under the connector
    policy, and the SRW snapshot records every selection in this form.
    """
    return {
        "inline": {
            "driver": DATASOURCE_DRIVER,
            "config": {"datasourceId": str(datasource_id)},
        }
    }


def legacy_alias(datasource_id: Any) -> str:
    """The alias of a link on the legacy path (the pre-D3c child name)."""
    return f"datasource-{_hex(datasource_id)}"


def link_entries(rows: Iterable[Mapping[str, Any]]) -> dict[str, tuple[str, dict]]:
    """``resources.connectors`` for a project's links: alias -> (id, entry).

    ``rows`` carry ``id`` and ``policy_revision``, and the Connector
    resource's ``resource_name``, ``scope_kind`` and ``scope_name`` when it
    has one.
    """
    entries: dict[str, tuple[str, dict]] = {}
    for row in rows:
        datasource_id = str(row["id"])
        if row.get("resource_name"):
            alias = row["resource_name"]
            if alias in entries:
                alias = f"connector-{_hex(datasource_id)}"
            entry = {
                "ref": {
                    "name": row["resource_name"],
                    "scope": {"kind": row["scope_kind"], "name": row["scope_name"]},
                }
            }
        else:
            alias = legacy_alias(datasource_id)
            entry = {
                "inline": {
                    "driver": DATASOURCE_DRIVER,
                    "config": {
                        "datasourceId": datasource_id,
                        "policyRevision": row["policy_revision"],
                    },
                }
            }
        entries[alias] = (datasource_id, entry)
    return entries


def drop_connector_aliases(spec: dict, aliases: Iterable[str]) -> None:
    """Remove connector aliases from a Project spec: the entries, and every
    binding that names them (``defaults``, ``team.officer``, each
    ``team.slots`` entry; the bindings validation checks)."""
    gone = set(aliases)
    connectors = spec.setdefault("resources", {}).setdefault("connectors", {})
    for alias in gone:
        connectors.pop(alias, None)
    team = spec.get("team") or {}
    for binding in (
        spec.get("defaults") or {},
        team.get("officer") or {},
        *(team.get("slots") or {}).values(),
    ):
        if "connectors" in binding:
            binding["connectors"] = [
                alias for alias in binding["connectors"] if alias not in gone
            ]


async def project_link_rows(db, project_id: Any) -> list:
    """The Project's links with their Connector resources, for ``link_entries``."""
    return await db.fetch(_LINKS, UUID(str(project_id)))


async def project_link_entries(db, project_id: Any) -> dict[str, tuple[str, dict]]:
    """The entries the Project's current links make (``link_entries``)."""
    return link_entries(await project_link_rows(db, project_id))


def _ref_scope(ref, *, project_id, account_id) -> dict[str, str] | None:
    scope = dict(ref.get("scope") or {"kind": "Project", "name": str(project_id)})
    if scope["kind"] == "Account" and scope["name"] in ("me", "personal"):
        if not account_id:
            return None
        scope["name"] = str(account_id)
    return scope


async def entry_datasource_id(
    store: ManifestStore,
    entry: Mapping[str, Any],
    *,
    project_id,
    account_id,
    include_retired: bool = False,
) -> str | None:
    """The connector id a ``resources.connectors`` entry names, or ``None``
    when it does not name a datasource's connector (an inline env or files
    connector, a Catalog Connector, a ref that resolves to nothing).

    An inline ``srw.datasource/v1`` entry names its ``config.datasourceId``;
    a ref names its resource's linked datasource.  An omitted ref scope is
    the Project's; ``me`` is ``account_id``.  With ``include_retired``, a ref
    whose Connector is retired (its connector was deleted) still names it.
    """
    if "inline" in entry:
        spec = entry.get("inline") or {}
        if spec.get("driver") != DATASOURCE_DRIVER:
            return None
        try:
            return str(UUID(str((spec.get("config") or {}).get("datasourceId"))))
        except (TypeError, ValueError):
            return None
    ref = entry.get("ref") or {}
    scope = _ref_scope(ref, project_id=project_id, account_id=account_id)
    if scope is None or not ref.get("name"):
        return None
    row = await store.by_name(KIND, scope, ref["name"])
    if row is None and include_retired:
        row = await store.db.fetchrow(
            """SELECT linked_id FROM srw_resources
               WHERE kind=$1 AND scope_kind=$2 AND scope_name=$3 AND name=$4
                 AND deleted_at IS NOT NULL AND linked_id IS NOT NULL
               ORDER BY deleted_at DESC LIMIT 1""",
            KIND,
            scope["kind"],
            scope["name"],
            ref["name"],
        )
    return str(row["linked_id"]) if row and row.get("linked_id") else None


async def entry_datasource_ids(
    db,
    connectors: Mapping[str, Any],
    *,
    project_id,
    account_id,
    include_retired: bool = False,
) -> dict[str, str | None]:
    """``entry_datasource_id`` for every alias of a connector map."""
    store = ManifestStore(db)
    return {
        alias: await entry_datasource_id(
            store,
            entry,
            project_id=project_id,
            account_id=account_id,
            include_retired=include_retired,
        )
        for alias, entry in (connectors or {}).items()
    }


#: The dependency entry that records what a person last applied to a natively
#: authored Project: its connector entries and its author. It tells the
#: entries the author wrote from those a refresh added, and an apply compares
#: against it, never against what refreshes saved since.
APPLIED_FORMAT = "srw/project-applied-v1"


def connectors_of(document: Mapping[str, Any] | None) -> dict[str, Any]:
    """A Project document's ``resources.connectors``."""
    if not document:
        return {}
    return (document["spec"].get("resources") or {}).get("connectors") or {}


def applied_record(resource: Mapping[str, Any] | None) -> dict | None:
    """The record of the last apply a person made (``APPLIED_FORMAT``)."""
    for item in (resource or {}).get("dependencies") or ():
        if isinstance(item, dict) and item.get("format") == APPLIED_FORMAT:
            return item
    return None


def applied_dependency(document: Mapping[str, Any], author: Any) -> dict:
    """The record of ``author`` applying ``document``, kept with the revision."""
    return {
        "format": APPLIED_FORMAT,
        "author": str(author) if author else None,
        "connectors": deepcopy(connectors_of(document)),
    }


def document_author(resource: Mapping[str, Any] | None) -> str | None:
    """Who ``me`` means in a stored Project document: the person who last
    applied it (its applied record), else the Account it lives in."""
    if not resource:
        return None
    record = applied_record(resource)
    if record and record.get("author"):
        return str(record["author"])
    owner = resource.get("owner_id")
    return str(owner) if owner else None


async def stale_connector_children(db, manager_id, keep: Iterable[str]) -> list[dict]:
    """The Project's managed Connector children no inline entry keeps."""
    rows = await db.fetch(
        """SELECT id, name FROM srw_resources
           WHERE managed_by=$1 AND kind=$2 AND deleted_at IS NULL
             AND NOT (name = ANY($3::text[]))
           ORDER BY name""",
        manager_id,
        KIND,
        sorted(set(keep)),
    )
    return [dict(row) for row in rows]


async def _child_held(db, child: Mapping[str, Any]) -> bool:
    """Whether unfinished work or another live resource still names a child."""
    if await execution_references_block_retirement(
        db, resource_ids=[child["id"]], dependency_ids=[str(child["id"])]
    ):
        return True
    return bool(
        await db.fetchval(
            """SELECT EXISTS(SELECT 1 FROM srw_resources r
               WHERE r.deleted_at IS NULL AND r.id <> $1 AND (r.managed_by = $1
                 OR EXISTS(SELECT 1 FROM jsonb_array_elements(r.dependencies) d
                           WHERE d->>'uid' = $2)))""",
            child["id"],
            str(child["id"]),
        )
    )


async def retirable_connector_children(
    db, manager_id, keep: Iterable[str]
) -> list[dict]:
    """The stale children (``stale_connector_children``) nothing holds."""
    return [
        child
        for child in await stale_connector_children(db, manager_id, keep)
        if not await _child_held(db, child)
    ]


async def retire_stale_connector_children(db, manager_id, keep: Iterable[str]) -> int:
    """Retire the Project's managed Connector children no inline entry keeps.

    Executions record their connectors by datasource id, so normally nothing
    waits on a child. One that unfinished work or a saved resource (a Job that
    names it, say) still names stays, logged, until a later refresh finds it
    free, and never fails the write that triggered this: the same blockers
    ``retire_removed_children`` refuses on.
    """
    retired = 0
    for child in await stale_connector_children(db, manager_id, keep):
        if await _child_held(db, child):
            logger.warning(
                "Connector child %s of Project resource %s is still named by "
                "unfinished work or a saved resource; it is retired by a later "
                "refresh",
                child["name"],
                manager_id,
            )
            continue
        await db.execute(
            "UPDATE srw_resources SET deleted_at=now(),updated_at=now(),"
            "resource_version=resource_version+1 WHERE id=$1",
            child["id"],
        )
        retired += 1
    return retired


@contextmanager
def project_refresh_suspended():
    """Hold refreshes while a Project apply changes its own links."""
    token = _REFRESH_SUSPENDED.set(True)
    try:
        yield
    finally:
        _REFRESH_SUSPENDED.reset(token)


async def refresh_project_connectors(
    db,
    project_ids: Iterable[Any],
    *,
    unlinked: Mapping[str, Iterable[str]] | None = None,
    deleted: Iterable[str] = (),
    heal: bool = False,
) -> dict[str, str]:
    """Bring each Project's connector entries in step with its links.

    Called by the datasource store's write-through inside its transaction,
    with ``unlinked``, the connectors the write unlinked, by project, and
    ``deleted``, the connector it deleted. Each Project gets a savepoint: a
    failure is logged as ``deferred`` and never raised into the write. The
    startup heal rebuilds a legacy-authored Project left behind; a natively
    authored one is only reported, and catches up at its next link write.
    """
    outcomes: dict[str, str] = {}
    if _REFRESH_SUSPENDED.get():
        return outcomes
    for project_id in sorted({str(value) for value in project_ids}):
        try:
            async with db.transaction_scope():
                outcome, _ = await refresh_project(
                    db,
                    project_id,
                    unlinked=(unlinked or {}).get(project_id, ()),
                    deleted=deleted,
                    heal=heal,
                )
        except Exception:
            logger.exception(
                "Project %s connector entries not refreshed; a legacy-authored "
                "Project is rebuilt by the startup heal, a natively authored "
                "one at its next link write",
                project_id,
            )
            outcome = "deferred"
        outcomes[project_id] = outcome
    return outcomes


async def refresh_project(
    db,
    project_id,
    *,
    unlinked: Iterable[str] = (),
    deleted: Iterable[str] = (),
    heal: bool = False,
    author: str | None = None,
) -> tuple[str, dict | None]:
    """Refresh one Project's entries; return the outcome and the saved row.

    ``unlinked``: the connectors the triggering write unlinked from this
    Project; a natively authored Project loses the entries a refresh added
    for them, never the ones its author wrote. ``deleted``: a connector the
    write deleted; every entry naming it goes, since a ref to it can no
    longer be applied. ``heal``: the startup pass, which never edits a
    natively authored Project: it only reports whether its entries differ
    from its links and writes the manifest's connector defaults row when the
    Project has none. ``author``: who ``me`` means in the document (the
    applier during an apply).

    Outcomes: ``none`` (no active Project manifest yet; it is built from the
    links when it is), ``unchanged``, ``rebuilt`` (legacy-authored),
    ``updated`` (natively authored), and in a heal ``native`` or
    ``native-drift``.  Call inside ``transaction_scope``.
    """
    from orchestrator.services.manifest_projects import (
        persist_project_resource,
        source_recipe,
    )
    from orchestrator.services.project_connector_defaults import (
        prune_unlinked_connector_defaults,
    )

    store = ManifestStore(db)
    await store.lock_catalog()
    resource = await store.by_link("Project", project_id)
    if resource is None or not resource.get("active_revision"):
        return "none", None
    desired = await project_link_entries(db, project_id)
    await prune_unlinked_connector_defaults(db, project_id)
    if source_recipe(resource) is not None:
        current = (resource["document"]["spec"].get("resources") or {}).get(
            "connectors", {}
        )
        wanted = {alias: entry for alias, (_, entry) in desired.items()}
        keep = [alias for alias, entry in wanted.items() if "inline" in entry]
        if current == wanted and not await retirable_connector_children(
            db, resource["id"], keep
        ):
            return "unchanged", None
        saved = await persist_project_resource(db, project_id)
        # None: the project row is gone (a delete racing this refresh).
        return ("rebuilt", saved) if saved else ("none", None)
    if heal:
        await _backfill_manifest_connector_defaults(db, project_id)
        drift = await _authored_drift(db, resource, desired)
        return ("native-drift" if drift else "native"), None
    saved = await _refresh_authored(
        db,
        store,
        resource,
        desired,
        set(unlinked),
        set(deleted),
        author=author or document_author(resource),
    )
    return ("updated", saved) if saved else ("unchanged", None)


async def _authored_drift(db, resource, desired) -> bool:
    """Whether a natively authored Project's datasource entries differ from
    its links (reported by the heal, never edited by it)."""
    authored = (resource["document"]["spec"].get("resources") or {}).get(
        "connectors", {}
    )
    named = await entry_datasource_ids(
        db,
        authored,
        project_id=str(resource["linked_id"]),
        account_id=document_author(resource),
    )
    listed = {value for value in named.values() if value}
    return listed != {datasource_id for datasource_id, _ in desired.values()}


async def _backfill_manifest_connector_defaults(db, project_id) -> None:
    """A natively authored Project whose manifest sets ``defaults.connectors``
    owns its connector defaults row; one written before the table existed
    gets it here, so the Settings API shows it as managed by the manifest and
    a Settings save cannot be overwritten by its next activation unseen. A
    Settings row is never taken over (as ``workspace_defaults_backfill``)."""
    from orchestrator.services.manifest_projects import active_project_resource
    from orchestrator.services.project_connector_defaults import (
        read_project_connector_defaults,
        sync_manifest_connector_defaults,
    )

    current = await read_project_connector_defaults(db, project_id)
    if current is not None and current.source != "manifest":
        return
    active = await active_project_resource(db, project_id)
    if active is None or "connectors" not in (
        active["document"]["spec"].get("defaults") or {}
    ):
        return
    if current is None or current.manifest_revision != active["revision"]:
        await sync_manifest_connector_defaults(
            db, {**active, "linked_id": str(project_id)}
        )


async def _refresh_authored(
    db,
    store,
    resource,
    desired,
    unlinked: set[str],
    deleted: set[str],
    *,
    author: str | None,
):
    """A natively authored Project: drop the entries a refresh added for a
    connector the triggering write unlinked, and every entry naming a
    connector it deleted; add entries for links it does not list; keep
    everything its author wrote. Returns the saved row, or None when nothing
    changed."""
    from orchestrator.services.project_connector_defaults import (
        sync_manifest_connector_defaults,
    )
    from orchestrator.services.project_workspace_defaults import (
        sync_manifest_defaults,
    )

    project_id = str(resource["linked_id"])
    owner = author
    authored = (resource["document"]["spec"].get("resources") or {}).get(
        "connectors", {}
    )
    # A deleted connector's Connector is retired before this runs: its ref
    # still names it.
    named = await entry_datasource_ids(
        db, authored, project_id=project_id, account_id=owner, include_retired=True
    )
    # What the author wrote: their last apply, or, before any refresh added
    # an entry, everything the document holds.
    record = applied_record(resource)
    written = (record.get("connectors") or {}) if record else authored
    linked = {datasource_id: alias for alias, (datasource_id, _) in desired.items()}
    removed = [
        alias
        for alias, value in named.items()
        if value
        and value not in linked
        and (
            value in deleted
            or (value in unlinked and written.get(alias) != authored[alias])
        )
    ]
    covered = {value for value in named.values() if value}
    added = [
        (alias, datasource_id)
        for datasource_id, alias in linked.items()
        if datasource_id not in covered
    ]
    if not removed and not added:
        return None

    document, resolved = deepcopy(resource["document"]), deepcopy(resource["resolved"])
    dependencies = deepcopy(resource["dependencies"])
    if record is None:
        # The first server edit of this document: record what the author
        # wrote, so the entries added from now on stay told apart.
        dependencies.append(applied_dependency(resource["document"], owner))
    child_scope = {"kind": "Project", "name": project_id}
    gone_keys: set[str] = set()
    gone_uids = {named[alias] for alias in removed}
    for alias in removed:
        entry = authored[alias]
        if "inline" in entry:
            gone_keys.add(f"{KIND}/Project/{project_id}/{alias}")
        else:
            scope = _ref_scope(entry["ref"], project_id=project_id, account_id=owner)
            gone_keys.add(
                f"{KIND}/{scope['kind']}/{scope['name']}/{entry['ref']['name']}"
            )
    children = []
    for item in (document, resolved):
        drop_connector_aliases(item["spec"], removed)
    connectors = document["spec"]["resources"]["connectors"]
    resolved_connectors = resolved["spec"]["resources"]["connectors"]
    for alias, datasource_id in added:
        entry = desired[alias][1]
        if alias in connectors:
            alias = f"connector-{_hex(datasource_id)}"
        connectors[alias] = deepcopy(entry)
        if "ref" in entry:
            resolved_connectors[alias] = datasource_binding(datasource_id)
        else:
            # The row fallback is a managed inline child, as an apply makes it.
            resolved_connectors[alias] = deepcopy(entry)
            child = {
                "apiVersion": API_VERSION,
                "kind": KIND,
                "metadata": {"name": alias, "scope": child_scope},
                "spec": deepcopy(entry["inline"]),
            }
            old_child = await store.by_name(KIND, child_scope, alias)
            children.append((child, old_child))
    dependencies = [
        item
        for item in dependencies
        if item.get("key") not in gone_keys and item.get("uid") not in gone_uids
    ]
    for child, old_child in children:
        await store.lock_identity(child)
        row, _ = await store.save(
            child,
            deepcopy(child),
            content_revision(child["spec"]),
            [],
            owner_id=resource["owner_id"],
            project_id=project_id,
            managed_by=resource["id"],
            expected_version=old_child["resource_version"] if old_child else None,
        )
        dependencies.append(
            {
                "uid": str(row["id"]),
                "revision": row["revision"],
                "key": resource_key(child),
            }
        )
    validate_documents([document])
    await store.lock_identity(document)
    saved, _ = await store.save(
        document,
        resolved,
        content_revision(resolved["spec"]),
        dependencies,
        owner_id=resource["owner_id"],
        project_id=project_id,
        linked_id=project_id,
        expected_version=resource["resource_version"],
    )
    await retire_stale_connector_children(
        db,
        saved["id"],
        [alias for alias, entry in connectors.items() if "inline" in entry],
    )
    await db.execute(
        "UPDATE srw_resources SET active_revision=$2 WHERE id=$1",
        saved["id"],
        saved["revision"],
    )
    saved["active_revision"] = saved["revision"]
    await sync_manifest_defaults(db, saved)
    await sync_manifest_connector_defaults(db, saved, author=author)
    return saved


async def sync_project_connector_links(db, project_row, user, *, previous=None) -> None:
    """Make an applied Project manifest's links: link every connector it newly
    names, unlink those the person's last apply named and it drops.

    "Last apply" is the applied record ``previous`` (the Project as saved
    before this apply) carries: what a person applied, never what refreshes
    added since, so re-applying an older copy never unlinks a link made in
    the meantime. Without a record, a natively authored document is its own
    (before D3c an apply linked nothing and refreshes added nothing); a
    legacy-authored one was written by the server, so nothing counts as
    named before. Entries carried over are left as they are, linked or not:
    re-applying a pre-D3c document changes no link.

    The link API's authority applies, checked by the store under its locks:
    adding a link needs the Project's owner and the connector's owner (or a
    public connector, or an administrator); a project's own knowledge base is
    never linked elsewhere and never unlinked.  Refreshes are held meanwhile;
    the caller refreshes the Project once afterwards.
    """
    from orchestrator.security.access import user_can_access_datasource
    from orchestrator.services.datasource_policy_errors import (
        DatasourceProjectAuthorizationError,
    )
    from orchestrator.services.project_connector_defaults import linked_connectors

    project_id = str(project_row["linked_id"])
    account = str(user["id"])
    admin = bool(user.get("is_admin"))

    from orchestrator.services.manifest_projects import source_recipe

    named = await entry_datasource_ids(
        db,
        connectors_of(project_row["document"]),
        project_id=project_id,
        account_id=account,
    )
    wanted = [value for value in dict.fromkeys(named.values()) if value]
    record = applied_record(previous)
    if record is not None:
        last_applied = record.get("connectors") or {}
    elif previous is not None and source_recipe(previous) is None:
        last_applied = connectors_of(previous["document"])
    else:
        last_applied = {}
    before = await entry_datasource_ids(
        db,
        last_applied,
        project_id=project_id,
        account_id=document_author(previous),
        include_retired=True,
    )
    named_before = {value for value in before.values() if value}
    dropped = named_before - set(wanted)
    links = {row["id"]: row for row in await linked_connectors(db, project_id)}
    with project_refresh_suspended():
        for datasource_id in wanted:
            if datasource_id in links or datasource_id in named_before:
                continue
            datasource = await db.get_datasource(datasource_id)
            if datasource is None:
                raise HTTPException(
                    422, "A connector this Project references no longer exists."
                )
            # Authority first, so a connector the caller cannot see answers
            # 403 like any other refused link, whatever kind it is.
            if not (
                datasource.get("is_global")
                or await user_can_access_datasource(user, db, datasource)
            ):
                raise HTTPException(
                    403, "Not authorized to add one or more project links"
                )
            if platform_owned(datasource):
                raise HTTPException(
                    409,
                    "The native knowledge connector cannot be linked to another project",
                )
            spec = spec_for_row(datasource)
            try:
                await db.link_datasource_to_project(
                    project_id,
                    datasource_id,
                    read_only=True
                    if spec is not None and spec.forced_read_only
                    else None,
                    description=None,
                    authority_user_id=account,
                    authority_is_admin=admin,
                )
            except DatasourceProjectAuthorizationError:
                raise HTTPException(
                    403, "Not authorized to add one or more project links"
                ) from None
        for datasource_id in sorted(dropped):
            link = links.get(datasource_id)
            if link is None or link["platform_owned"]:
                continue
            try:
                await db.unlink_datasource_from_project(
                    project_id,
                    datasource_id,
                    authority_user_id=account,
                    authority_is_admin=admin,
                )
            except DatasourceProjectAuthorizationError:
                raise HTTPException(
                    403, "Not authorized to modify this project connector link"
                ) from None


DEFAULTS_AUTHORITY_DETAIL = (
    "Only a Project owner or an administrator may change its connector defaults."
)


async def connector_default_ids(
    db, document: Mapping[str, Any], *, project_id, account_id
) -> list[str] | None:
    """The connector ids a Project document's ``defaults.connectors`` names,
    in order, or None when it does not set the field."""
    spec = document["spec"]
    defaults = spec.get("defaults") or {}
    if "connectors" not in defaults:
        return None
    connectors = (spec.get("resources") or {}).get("connectors") or {}
    named = await entry_datasource_ids(
        db,
        {
            alias: connectors[alias]
            for alias in defaults["connectors"]
            if alias in connectors
        },
        project_id=project_id,
        account_id=account_id,
    )
    return [named[alias] for alias in defaults["connectors"] if named.get(alias)]


async def require_connector_defaults_authority(
    db, document: Mapping[str, Any], user, *, project_id, previous=None
) -> None:
    """A manifest apply that changes the connectors ``defaults.connectors``
    names needs the authority ``PUT /api/projects/{id}/connector-defaults``
    needs: a Project owner or an administrator. Editors may apply a Project
    whose defaults stay as they are. (The workspace defaults have the same
    gap through ``defaults.workspace``; not changed here.)"""
    if user.get("is_admin"):
        return
    after = await connector_default_ids(
        db, document, project_id=project_id, account_id=str(user["id"])
    )
    before = (
        await connector_default_ids(
            db,
            previous["document"],
            project_id=project_id,
            account_id=document_author(previous),
        )
        if previous
        else None
    )
    if after == before:
        return
    role = await db.get_user_role_in_project(str(project_id), str(user["id"]))
    if role != "owner":
        raise HTTPException(403, DEFAULTS_AUTHORITY_DETAIL)


async def heal_project_connectors(db) -> dict[str, int]:
    """Startup: bring every legacy-authored Project's entries in step with its
    links, and report the natively authored ones that differ.

    Rebuilds legacy-authored Projects that still hold inline
    ``datasource-<hex>`` children or entries that drifted from their links
    before D3c; one in step is only compared.  A natively authored Project is
    its author's and is never edited here: one whose datasource entries differ
    from its links (an entry naming a connector an apply never linked, a link
    it does not list) is counted ``native-drift`` and named in a warning.
    Each Project is its own transaction; one that cannot be refreshed is logged
    and retried at the next start.
    """
    counts: dict[str, int] = {}
    drifted: list[str] = []
    rows = await db.fetch(
        "SELECT linked_id FROM srw_resources WHERE kind='Project' "
        "AND linked_id IS NOT NULL AND deleted_at IS NULL ORDER BY linked_id"
    )
    for row in rows:
        project_id = str(row["linked_id"])
        outcome = (await refresh_project_connectors(db, [project_id], heal=True)).get(
            project_id, "none"
        )
        counts[outcome] = counts.get(outcome, 0) + 1
        if outcome == "native-drift":
            drifted.append(project_id)
    if drifted:
        logger.warning(
            "%d natively authored Project manifests list connectors other than "
            "their links; left as their authors wrote them: %s",
            len(drifted),
            ", ".join(drifted),
        )
    log = logger.error if counts.get("deferred") else logger.info
    log("Project connector entries: %s", counts)
    return counts


__all__ = [
    "DATASOURCE_DRIVER",
    "datasource_binding",
    "drop_connector_aliases",
    "entry_datasource_id",
    "entry_datasource_ids",
    "heal_project_connectors",
    "legacy_alias",
    "link_entries",
    "project_link_entries",
    "project_link_rows",
    "project_refresh_suspended",
    "refresh_project",
    "refresh_project_connectors",
    "retire_stale_connector_children",
    "stale_connector_children",
    "sync_project_connector_links",
]
