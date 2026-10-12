"""Project-attachment verdicts, authorization and knowledge scope for threads.

Extracted verbatim from ``orchestrator.main`` (R1.B06 lane B, census group
``S_ADMISSION``). Project mounts grant a session its native knowledge base and
repository scope, so this module is an authorization boundary and not a lookup
helper.

Five properties are load-bearing. The first four moved unchanged; the fifth
came with the one-project rule:

* **Classification ranks below authorization.** ``archived`` is a lifecycle
  verdict; a caller with no membership still learns only ``revoked``, never
  that the project happens to be archived.
* **Denial does not disclose which id failed** — one generic 403 for the whole
  selection, so the endpoint is not an existence oracle. The single exception is
  an all-``archived`` denial: the caller is by definition an authorized member
  of each one, so the 409 ``PROJECT_ARCHIVED_DETAIL`` leaks nothing and tells
  them which lever to pull.
* **An acknowledged id is dropped only while it is STILL denied**, mirroring
  the connector counterpart in
  ``services.thread_datasource_authorization.strip_still_denied_ack``: a
  restored membership returns automatically, with no repair step (spec §3.2),
  and anything denied but never acknowledged still fails the whole selection
  closed.
* **An MCP ``project:<uuid>`` token scope is a target binding, not another
  membership grant.** Omission means that project; naming a different or
  additional one is refused before project or connector policy could widen the
  request.
* **A new Session has one project or none.** More than one distinct project
  is a 422 (``ONE_PROJECT_PER_SESSION_DETAIL``), checked after the MCP token
  scope so a scoped token still gets its 403. Only creation checks it; older
  multi-project Sessions still read, resume and take config updates.

Every collaborator arrives per invocation through
:class:`ThreadProjectAuthorizationDependencies`; ``store`` is main's
``postgres_db``, which tests rebind on the application module.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from fastapi import HTTPException

from orchestrator.schemas.thread_admission import ThreadCreateRequest
from orchestrator.security.access import (
    PROJECT_ARCHIVED_DETAIL,
    mcp_scope_project_id,
    project_is_archived,
)
from orchestrator.services.config_drift import (
    acknowledged_drift_ids,
    strip_acknowledged,
)
from shared.connectors.builtin import needs_knowledge_profile

ONE_PROJECT_PER_SESSION_DETAIL = (
    "A Session can belong to one project at most. Pick one project, or none."
)


class ThreadProjectStore(Protocol):
    async def get_user(self, user_id: str) -> dict[str, Any] | None: ...

    async def get_project(self, project_id: str) -> dict[str, Any] | None: ...

    async def get_user_role_in_project(
        self, project_id: str, user_id: str
    ) -> str | None: ...

    async def get_datasource(self, datasource_id: str) -> dict[str, Any] | None: ...


@dataclass(frozen=True)
class ThreadProjectAuthorizationDependencies:
    """Per-invocation collaborators for thread project attachment policy."""

    store: ThreadProjectStore


@dataclass(frozen=True)
class ProjectVerdict:
    """One project attachment's availability decision."""

    project_id: str
    denied: bool
    reason: str | None = None


async def classify_thread_project_ids(
    user: dict[str, Any],
    project_ids: list[str] | None,
    *,
    dependencies: ThreadProjectAuthorizationDependencies,
) -> list[ProjectVerdict]:
    """Per-item project verdicts. Reporting half of
    :func:`authorize_thread_project_ids`, which wraps this.

    ``archived`` is the lifecycle verdict (§4.3 of
    knowledge-base/knowledge/features/project_and_job_list_filtering.md). This
    funnel is the only path thread creation takes, and — for free — the one
    ``prepare_job_admission_scope`` takes for agent-spawned subjobs,
    so extending it here covers both rather than inventing a parallel check.
    It ranks BELOW authorization: a caller with no membership still learns
    only ``revoked``, never that the project happens to be archived. The
    reason is acknowledgeable (``ACKNOWLEDGEABLE_REASONS``), so an existing
    session attached to a project that gets archived surfaces a drift item the
    owner can accept and resume without, instead of hard-failing at attach.
    """
    selected = list(dict.fromkeys(str(value) for value in project_ids or []))
    verdicts: list[ProjectVerdict] = []
    for project_id in selected:
        project = await dependencies.store.get_project(project_id)
        if not project:
            verdicts.append(ProjectVerdict(project_id, True, "deleted"))
            continue
        if not user.get("is_admin"):
            role = await dependencies.store.get_user_role_in_project(
                project_id, str(user["id"])
            )
            if not role:
                verdicts.append(ProjectVerdict(project_id, True, "revoked"))
                continue
        if project_is_archived(project):
            verdicts.append(ProjectVerdict(project_id, True, "archived"))
            continue
        verdicts.append(ProjectVerdict(project_id, False, None))
    return verdicts


async def authorize_thread_project_ids(
    user: dict[str, Any],
    project_ids: list[str] | None,
    *,
    dependencies: ThreadProjectAuthorizationDependencies,
) -> list[str]:
    """Authorize project attachments without disclosing which ID failed.

    One exception to the non-disclosure: when *every* denial is ``archived``
    the caller is by definition an authorized member of each one, so nothing
    is leaked by saying so — and the generic sentence would strand them with
    no idea which lever to pull. Any other denial in the mix keeps the
    original 403, which must not become an oracle for project existence.
    """
    selected = list(dict.fromkeys(str(value) for value in project_ids or []))
    if not selected:
        return []
    verdicts = await classify_thread_project_ids(
        user, selected, dependencies=dependencies
    )
    denials = [v for v in verdicts if v.denied]
    if denials:
        if all(v.reason == "archived" for v in denials):
            raise HTTPException(status_code=409, detail=PROJECT_ARCHIVED_DETAIL)
        raise HTTPException(
            status_code=403,
            detail="One or more attached projects are unavailable",
        )
    return selected


async def revalidate_thread_project_ids(
    thread: dict[str, Any],
    project_ids: list[str] | None,
    *,
    dependencies: ThreadProjectAuthorizationDependencies,
) -> list[str]:
    """Re-check persisted project mounts against the thread owner's membership.

    Project mounts grant the session its native knowledge base and repository
    scope. Like explicit datasource selections, they are not frozen grants: a
    revoked membership must take effect on the next attach/resume. Userless
    internal/system threads retain their existing trusted behavior, and admins
    retain the platform's normal all-project visibility.

    Acknowledged ids are dropped only while they remain unavailable (spec
    §3.2: a restored membership returns automatically, no repair step).
    Classify current status first, narrow out only the acknowledged ids that
    are STILL denied, and let :func:`authorize_thread_project_ids` fail the
    whole selection closed on anything denied that was never acknowledged —
    mirrors the connector counterpart, ``strip_still_denied_ack``.
    """
    owner_id = thread.get("user_id")
    if not owner_id:
        return list(dict.fromkeys(str(value) for value in project_ids or []))

    owner = await dependencies.store.get_user(str(owner_id))
    if owner is None:
        raise HTTPException(
            status_code=403,
            detail="One or more attached projects are unavailable",
        )

    selected = list(dict.fromkeys(str(value) for value in project_ids or []))
    ack = acknowledged_drift_ids(thread.get("metadata"))
    if ack and selected:
        verdicts = await classify_thread_project_ids(
            owner, selected, dependencies=dependencies
        )
        still_denied_ack = {
            f"project:{v.project_id}"
            for v in verdicts
            if v.denied and f"project:{v.project_id}" in ack
        }
        selected = strip_acknowledged(selected, still_denied_ack, prefix="project")

    return await authorize_thread_project_ids(
        owner, selected, dependencies=dependencies
    )


def thread_creation_project_ids(
    request_body: ThreadCreateRequest, user: dict[str, Any]
) -> list[str]:
    """Resolve requested thread projects under an MCP token's scope.

    A ``project:<uuid>`` MCP token is an authoritative target binding, not
    merely another membership grant. Omission therefore means that project,
    while an attempt to name a different or additional project fails before
    project or datasource policy resolution can widen the request.

    A new Session belongs to one project or none (owner decision 2026-09-24,
    knowledge-base/knowledge/features/single_project_sessions.md). More than
    one distinct project is a 422, checked after the token scope so a scoped
    token still learns only the scope's 403. Every create path (REST create
    and preview, MCP, review Sessions, Officers) goes through here; reads,
    resume and config updates of older multi-project Sessions do not.
    """
    requested = list(request_body.project_ids or [])
    if request_body.project_id and str(request_body.project_id) not in requested:
        requested.append(str(request_body.project_id))
    requested = list(dict.fromkeys(str(value) for value in requested))

    scoped_project = mcp_scope_project_id(user)
    if scoped_project is not None:
        scoped_project_id = str(scoped_project)
        if requested and requested != [scoped_project_id]:
            raise HTTPException(
                status_code=403,
                detail="Access denied by MCP token scope",
            )
        return [scoped_project_id]

    if len(requested) > 1:
        raise HTTPException(status_code=422, detail=ONE_PROJECT_PER_SESSION_DETAIL)
    return requested


async def thread_has_knowledge_scope(
    *,
    project_ids: list[str] | None,
    datasource_ids: list[str] | None,
    dependencies: ThreadProjectAuthorizationDependencies,
) -> bool:
    """Whether a persistent-session dispatch needs the system KB credential.

    Project scope exposes the native project knowledge base. For an
    external-only session, inspect only the already-authorized/persisted
    datasource IDs and opt in when one is an OKF KB datasource. This keeps the
    system embedding key out of unrelated database/cloud/repository sessions.
    """
    if project_ids:
        return True
    for datasource_id in datasource_ids or []:
        datasource = await dependencies.store.get_datasource(str(datasource_id))
        if datasource and needs_knowledge_profile(datasource):
            return True
    return False


__all__ = [
    "ONE_PROJECT_PER_SESSION_DETAIL",
    "ProjectVerdict",
    "ThreadProjectAuthorizationDependencies",
    "ThreadProjectStore",
    "authorize_thread_project_ids",
    "classify_thread_project_ids",
    "revalidate_thread_project_ids",
    "thread_creation_project_ids",
    "thread_has_knowledge_scope",
]
