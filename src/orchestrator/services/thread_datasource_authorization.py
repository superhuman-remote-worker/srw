"""Thread-shaped connector authorization, revalidation and exact resolution.

Extracted verbatim from ``orchestrator.main`` (R1.B06 lane B, census group
``S_ADMISSION``). This is the *session* half of the connector contract whose
job-shaped counterpart is ``services.job_datasource_selection``; both are
policy over ``services.datasource_policy``, never a second copy of it.

The properties that make these five functions a security boundary move
unchanged:

* **One refusal shape for every denial.** ``DatasourceUnavailableError`` becomes
  ``403 "One or more selected connectors are unavailable"`` at each of the three
  translation points, and a tier violation becomes ``400`` carrying the policy's
  own sentence. The generic 403 is deliberate: a per-id reason would make this
  endpoint a connector-enumeration oracle.
* **A persisted selection is a record, not a grant.** Every attach/resume
  re-authorizes for the thread's current owner, so a revoked link or membership
  takes effect on the next attach. A non-system thread whose owner row vanished
  fails closed with the same generic detail.
* **An acknowledged id is narrowed out only while it is STILL denied.**
  ``strip_still_denied_ack`` classifies current status first and then narrows;
  it never strips on ack-map membership alone, so a recreated connector returns
  automatically with no repair step (spec §3.2). Anything denied and never
  acknowledged is left in place so the authorize call downstream still fails the
  whole selection closed on it. ``strip_acknowledged`` itself stays a pure,
  unconditional set difference — see ``tests/test_attach_honors_drift_ack.py``.
* **``resolve_authorized_thread_datasources`` ends in the exact-resolution
  gate.** A deleted connector, a changed policy revision, a duplicate resolver
  row or any silent reduction is one refusal rather than a reduced attach.

The late ``datasource_policy`` imports inside the function bodies are kept as
they were written: tests steer these paths by patching the attribute on
``orchestrator.services.datasource_policy``, which a module-level ``from``
import would bind past.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

from fastapi import HTTPException

from orchestrator.services.config_drift import (
    acknowledged_drift_ids,
    strip_acknowledged,
)
from orchestrator.services.datasource_policy import classify_datasource_selection
from orchestrator.services.job_datasource_selection import (
    require_exact_datasource_resolution,
)


class ThreadDatasourceStore(Protocol):
    async def get_user(self, user_id: str) -> dict[str, Any] | None: ...

    async def resolve_datasources_for_thread(
        self,
        *,
        datasource_ids: list[str] | None,
        project_ids: list[str] | None,
    ) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class ThreadDatasourceAuthorizationDependencies:
    """Collaborators for one thread connector authorization, per invocation.

    ``store`` is main's ``postgres_db``. ``thread_project_ids`` is main's
    bridge onto ``services.thread_mount_rows`` — it is taken as a callable
    rather than resolved here because that service needs a dependency object
    this module has no business building (port contract §P3).
    """

    store: ThreadDatasourceStore
    thread_project_ids: Callable[[str], Awaitable[list[str]]]
    #: Reads each authorized row's credentials from its Connector's resource
    #: secret, in place (``connector_secrets.read_connector_credentials``);
    #: ``None`` delivers the row's own.
    connector_credentials: Callable[..., Awaitable[None]] | None = None


async def authorize_thread_datasource_selection(
    user: dict[str, Any] | None,
    datasource_ids: list[str] | None,
    *,
    workspace_backend: str | None,
    target_project_ids: list[str] | None = None,
    effective_work_owner_id: str | None = None,
    trusted_system_inheritance: bool = False,
    legacy_job_id: str | None = None,
    dependencies: ThreadDatasourceAuthorizationDependencies,
) -> tuple[list[str], dict[str, int]]:
    """Resolve one complete selection and its exact policy snapshot."""
    from orchestrator.services.datasource_policy import (
        DatasourceUnavailableError,
        DatasourceWorkspaceTierError,
        authorize_datasource_selection,
    )

    owner_id = effective_work_owner_id
    if owner_id is None and user is not None:
        owner_id = str(user.get("id")) if user.get("id") else None
    try:
        return await authorize_datasource_selection(
            dependencies.store,
            user,
            owner_id,
            datasource_ids,
            target_project_ids,
            workspace_backend,
            allow_admin_explicit_override=True,
            trusted_system_inheritance=trusted_system_inheritance,
            legacy_job_id=legacy_job_id,
        )
    except DatasourceUnavailableError as exc:
        raise HTTPException(
            status_code=403,
            detail="One or more selected connectors are unavailable",
        ) from exc
    except DatasourceWorkspaceTierError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


async def authorize_thread_datasource_ids(
    user: dict[str, Any] | None,
    datasource_ids: list[str] | None,
    *,
    workspace_backend: str | None,
    target_project_ids: list[str] | None = None,
    effective_work_owner_id: str | None = None,
    trusted_system_inheritance: bool = False,
    legacy_job_id: str | None = None,
    dependencies: ThreadDatasourceAuthorizationDependencies,
) -> list[str]:
    """Compatibility wrapper for revalidation/live-update ID consumers."""
    selected, _revisions = await authorize_thread_datasource_selection(
        user,
        datasource_ids,
        workspace_backend=workspace_backend,
        target_project_ids=target_project_ids,
        effective_work_owner_id=effective_work_owner_id,
        trusted_system_inheritance=trusted_system_inheritance,
        legacy_job_id=legacy_job_id,
        dependencies=dependencies,
    )
    return selected


async def strip_still_denied_ack(
    thread: dict[str, Any],
    selected: list[str],
    *,
    actor: dict[str, Any] | None,
    effective_work_owner_id: str | None,
    project_ids: list[str],
    trusted_system_inheritance: bool = False,
    dependencies: ThreadDatasourceAuthorizationDependencies,
) -> list[str]:
    """Drop acknowledged connector ids that are CURRENTLY still unavailable.

    Spec §3.2: "If the connector is recreated or the grant re-issued, the
    item returns automatically on the next attach — no repair step." Dropping
    an id purely because its namespaced key is IN the ack map (as this used
    to do) breaks that promise forever, since nothing ever prunes the map.

    ``strip_acknowledged`` itself stays a pure, unconditional set-difference
    (see tests/test_attach_honors_drift_ack.py). The "only while it's still
    denied" condition lives here, at the call site, mirroring
    ``_strip_acknowledged_grants``'s re-evaluate-before-strip discipline:
    classify current status first, then narrow. A recreated/re-scoped
    connector keeps a clean verdict and is left in ``selected``, so it
    authorizes normally and is used again automatically; a still-denied
    acknowledged id is dropped exactly as before. Anything denied and NEVER
    acknowledged is also left in place, so the authorize call downstream
    still fails the whole selection closed on it — fail-closed is unchanged.

    A malformed stored id or a vanished/unapproved owner makes
    ``classify_datasource_selection`` raise ``DatasourceUnavailableError``
    directly — those checks run BEFORE its per-item loop, so no verdict list
    is ever returned. Translate that the same way the sibling authorizer
    (:func:`authorize_thread_datasource_selection`) does, so this call
    site's behavior is unchanged from before the ack feature existed: a 403,
    never an unhandled exception reaching the ASGI layer as a 500.
    """
    ack = acknowledged_drift_ids(thread.get("metadata"))
    if not ack:
        return selected
    from orchestrator.services.datasource_policy import DatasourceUnavailableError

    try:
        verdicts, _revisions = await classify_datasource_selection(
            dependencies.store,
            actor,
            effective_work_owner_id,
            selected,
            project_ids,
            None,
            trusted_system_inheritance=trusted_system_inheritance,
        )
    except DatasourceUnavailableError as exc:
        raise HTTPException(
            status_code=403,
            detail="One or more selected connectors are unavailable",
        ) from exc
    still_denied_ack = {
        f"connector:{v.datasource_id}"
        for v in verdicts
        if v.denied and f"connector:{v.datasource_id}" in ack
    }
    return strip_acknowledged(selected, still_denied_ack, prefix="connector")


async def revalidate_thread_datasource_selection(
    thread: dict[str, Any],
    datasource_ids: list[str] | None,
    *,
    target_project_ids: list[str] | None = None,
    dependencies: ThreadDatasourceAuthorizationDependencies,
) -> tuple[list[str], dict[str, int]]:
    """Re-check a persisted selection and return its current policy snapshot.

    Persistent metadata records which datasources were selected, not a durable
    authorization grant. A datasource link or project membership can be revoked
    after thread creation, so every attach/resume must resolve access again for
    the user who owns the thread. Global datasources retain their internal
    dispatch semantics through :func:`authorize_thread_datasource_ids`.

    Threads without a user are trusted internal/system threads. Preserve their
    historical behavior while still normalizing duplicate IDs; deleted IDs are
    naturally omitted by ``resolve_datasources_for_thread``. A non-system thread
    whose owner row vanished fails closed with the same generic detail used at
    create time, avoiding a datasource-enumeration oracle.

    Acknowledged ids are narrowed out only while still denied — see
    :func:`strip_still_denied_ack`.
    """
    selected = list(dict.fromkeys(str(value) for value in datasource_ids or []))
    if not selected:
        return [], {}

    project_ids = (
        list(target_project_ids)
        if target_project_ids is not None
        else await dependencies.thread_project_ids(str(thread["id"]))
    )

    owner_id = thread.get("user_id")
    if not owner_id:
        selected = await strip_still_denied_ack(
            thread,
            selected,
            actor=None,
            effective_work_owner_id=None,
            project_ids=project_ids,
            trusted_system_inheritance=True,
            dependencies=dependencies,
        )
        if not selected:
            return [], {}
        return await authorize_thread_datasource_selection(
            None,
            selected,
            workspace_backend=None,
            target_project_ids=project_ids,
            trusted_system_inheritance=True,
            dependencies=dependencies,
        )

    owner = await dependencies.store.get_user(str(owner_id))
    if owner is None:
        raise HTTPException(
            status_code=403,
            detail="One or more selected connectors are unavailable",
        )

    selected = await strip_still_denied_ack(
        thread,
        selected,
        actor=owner,
        effective_work_owner_id=str(owner_id),
        project_ids=project_ids,
        dependencies=dependencies,
    )
    if not selected:
        return [], {}

    # workspace_backend=None intentionally skips the create-time lite/repository
    # compatibility rule. Revalidation is only an access check and must not
    # retroactively change existing non-KB datasource behavior.
    return await authorize_thread_datasource_selection(
        owner,
        selected,
        workspace_backend=None,
        target_project_ids=project_ids,
        effective_work_owner_id=str(owner_id),
        dependencies=dependencies,
    )


async def resolve_authorized_thread_datasources(
    thread: dict[str, Any],
    datasource_ids: list[str] | None,
    *,
    target_project_ids: list[str] | None = None,
    dependencies: ThreadDatasourceAuthorizationDependencies,
) -> list[dict[str, Any]]:
    """Authorize and exactly resolve a thread connector snapshot.

    The credentials come from each Connector's resource secret, which the
    connector policy has just authorized this thread to use (decision 11),
    or from the row where the resource has none yet.
    """
    selected, policy_revisions = await revalidate_thread_datasource_selection(
        thread,
        datasource_ids,
        target_project_ids=target_project_ids,
        dependencies=dependencies,
    )
    resolved = await dependencies.store.resolve_datasources_for_thread(
        datasource_ids=selected,
        project_ids=target_project_ids,
    )
    rows = require_exact_datasource_resolution(selected, policy_revisions, resolved)
    if dependencies.connector_credentials is not None:
        await dependencies.connector_credentials(rows, authorized=selected)
    return rows


__all__ = [
    "ThreadDatasourceAuthorizationDependencies",
    "ThreadDatasourceStore",
    "authorize_thread_datasource_ids",
    "authorize_thread_datasource_selection",
    "resolve_authorized_thread_datasources",
    "revalidate_thread_datasource_selection",
    "strip_still_denied_ack",
]
