"""Turn a request's ``execution.connectors`` refs into connector ids (slice D3c).

Job and session creation accept Connectors by manifest ref
(``schemas/execution_selection.py``).  This resolver runs at the admission
edge and only translates: each ref becomes the datasource id its Connector
resource is linked to, in alias order.  The ids then take the explicit
``datasource_ids`` branch of the unchanged funnels, so authorization (owner,
public, project-linked, native), scope, the lite-tier refusal, the policy
snapshot and the exact-resolution gate are the ones ``datasource_ids`` get.

**No enumeration.**  This module never authorizes, and it never tells a
caller more than the policy would.  A ref that names nothing, names a
resource that is not a datasource's Connector, or names another user's
connector all end in the policy's one refusal, ``403 "One or more selected
connectors are unavailable"``: here when the ref resolves to nothing, in the
policy when it resolves to an id the caller may not use.  Only request shape
(a malformed scope name) is a 422, which says nothing about what exists.

**Resource first, row fallback.**  A name ref is a resource lookup.  A ``uid``
ref is the connector's id (the uid of a datasource's Connector is its
datasource id), so a row that has no resource yet (the D3a legacy path)
still resolves, exactly as its id in ``datasource_ids`` would.
"""

from __future__ import annotations

from typing import Any, Mapping
from uuid import UUID

from fastapi import HTTPException

from orchestrator.services.datasource_policy import GENERIC_UNAVAILABLE_DETAIL
from orchestrator.services.manifest_store import ManifestStore

KIND = "Connector"
SELECTOR_CONFLICT_DETAIL = (
    "execution.connectors and {other} are mutually exclusive; select the "
    "connectors one way."
)


def refuse_connector_selector_conflict(command: Any) -> None:
    """400 when a request selects connectors by ref and another way too.

    ``datasource_ids`` (present, even ``[]``) and ``use_datasource_defaults``
    are the other selectors; presence, not truthiness, decides, as in the
    funnels.
    """
    if getattr(command, "execution", None) is None:
        return
    if "datasource_ids" in command.model_fields_set:
        raise HTTPException(
            400, SELECTOR_CONFLICT_DETAIL.format(other="datasource_ids")
        )
    if command.use_datasource_defaults:
        raise HTTPException(
            400, SELECTOR_CONFLICT_DETAIL.format(other="use_datasource_defaults")
        )


def connector_refs(execution: Any) -> dict[str, dict[str, Any]]:
    """An ``ExecutionSelection``'s connector refs by alias, as plain data."""
    return {
        alias: selection.ref.model_dump(exclude_none=True, mode="json")
        for alias, selection in execution.connectors.items()
    }


def _unavailable() -> HTTPException:
    return HTTPException(status_code=403, detail=GENERIC_UNAVAILABLE_DETAIL)


def _live_scope(
    scope: Mapping[str, str] | None,
    *,
    owner_id: str | None,
    project_id: str | None,
) -> dict[str, str] | None:
    """The concrete scope a name ref looks in; None when there is none."""
    if scope is None:
        if project_id:
            return {"kind": "Project", "name": str(project_id)}
        return {"kind": "Account", "name": str(owner_id)} if owner_id else None
    kind, name = scope["kind"], scope["name"]
    if kind == "Account" and name in ("me", "personal"):
        return {"kind": "Account", "name": str(owner_id)} if owner_id else None
    if kind in ("Account", "Project"):
        try:
            name = str(UUID(name))
        except ValueError:
            raise HTTPException(
                422,
                f"A connector ref's {kind} scope is named by its UUID"
                + (" or 'me'." if kind == "Account" else "."),
            ) from None
    return {"kind": kind, "name": name}


async def resolve_connector_refs(
    db,
    refs: Mapping[str, Mapping[str, Any]],
    *,
    owner_id: str | None,
    project_id: str | None,
) -> list[str]:
    """The connector ids ``refs`` name, in alias order, unauthorized.

    ``owner_id`` is the work's effective owner (``me`` and the default
    Account scope); ``project_id`` its project (the default scope when set).
    The caller passes the result to the explicit ``datasource_ids`` branch,
    which authorizes it.  Duplicates are kept: the policy collapses them as it
    does for ``datasource_ids``.
    """
    store = ManifestStore(db)
    ids: list[str] = []
    for ref in refs.values():
        if ref.get("uid"):
            uid = str(UUID(str(ref["uid"])))
            resource = await store.by_id(uid)
            if resource is not None and (
                resource["kind"] != KIND or str(resource.get("linked_id")) != uid
            ):
                # Another kind's resource, or a Connector with no datasource
                # (inline-authored, Catalog): nothing a job can bind.
                raise _unavailable()
            ids.append(uid)
            continue
        scope = _live_scope(ref.get("scope"), owner_id=owner_id, project_id=project_id)
        resource = (
            await store.by_name(KIND, scope, ref["name"]) if scope is not None else None
        )
        if resource is None or not resource.get("linked_id"):
            raise _unavailable()
        ids.append(str(resource["linked_id"]))
    return ids


async def resolve_execution_connectors(
    db,
    execution: Any,
    *,
    owner_id: str | None,
    project_id: str | None,
) -> list[str]:
    """``resolve_connector_refs`` for a request's ``execution`` block."""
    return await resolve_connector_refs(
        db, connector_refs(execution), owner_id=owner_id, project_id=project_id
    )


__all__ = [
    "SELECTOR_CONFLICT_DETAIL",
    "connector_refs",
    "refuse_connector_selector_conflict",
    "resolve_connector_refs",
    "resolve_execution_connectors",
]
