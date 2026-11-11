"""``/api/officers`` and ``/api/projects/{project_id}/officer`` — the Post.

Extracted from ``orchestrator.main`` (R1.B07 lane O). Nine route declarations,
moved with their handler names, paths, methods, parameter order and docstrings
intact — the docstring is the published OpenAPI description, so it is part of
the route's identity and not editorial text.

None of the nine carried ``tags``, ``response_model``, ``status_code`` or a
``dependencies`` list, and none acquires one here. Each gate is called in the
declaration body exactly where it ran before: ``scripts/check_endpoint_auth.py``
reads the audited gate from the route it is declared on and does not follow a
call into a service module. The resolved principal (and, for commission, the
resolved project) is handed down so this is the same single membership read it
always was.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from orchestrator.schemas.officer_post import (
    OfficerDecommissionRequest,
    OfficerHoldRequest,
    OfficerNoteRequest,
)
from orchestrator.security.access import require_project_member, require_project_owner
from orchestrator.security.auth import require_approved_user
from orchestrator.services import officer_post_lifecycle, officer_post_views

# No `tags=` and no prefix: the nine declarations this replaces carried
# neither, and either would change the published OpenAPI operation.
router = APIRouter()


def get_officer_post_view_dependencies(
    request: Request,
) -> officer_post_views.OfficerPostViewDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.officer_post_view_dependencies_factory()


def get_officer_post_lifecycle_dependencies(
    request: Request,
) -> officer_post_lifecycle.OfficerPostLifecycleDependencies:
    return request.app.state.officer_post_lifecycle_dependencies_factory()


@router.get("/api/officers")
async def list_officers(request: Request) -> dict[str, Any]:
    """Every post the caller can see, vacant ones included — the roster.

    Discovery for a Legate (or an assistant holding his credentials) who has
    more projects than officers: one call answers which projects have an
    officer, whether he is awake, held or vacant, and whether anything is
    waiting on him. Per-slot kit utilization stays on the per-project card,
    which computes it lineage-aware; this read stays cheap.
    """
    dependencies = get_officer_post_view_dependencies(request)
    user = await require_approved_user(request, dependencies.store)
    return await officer_post_views.list_officers(
        request, dependencies=dependencies, user=user
    )


@router.get("/api/projects/{project_id}/officer")
async def get_project_officer_summary(
    request: Request, project_id: str
) -> dict[str, Any]:
    """The project's post at a glance — the cockpit's officer card.

    Always returns the post. ``commissioned`` / ``held`` / ``kit`` /
    ``incarnations`` / ``communication_policy`` / ``while_vacant`` come from
    the durable ``project_officers`` row; the ``officer`` block is ALWAYS
    present so the card's editor seeds from one place — live thread metadata
    when commissioned, the row's config when vacant (live-only fields null
    there). Kit utilization is lineage-aware: in-flight counts follow every
    incarnation on the post, not just the current thread.
    """
    # Design: knowledge-base/knowledge/features/officer_post.md §4/§8.
    dependencies = get_officer_post_view_dependencies(request)
    user, _project = await require_project_member(
        request, dependencies.store, project_id, min_role="viewer"
    )
    return await officer_post_views.get_project_officer_summary(
        request, project_id, dependencies=dependencies, user=user
    )


@router.post("/api/projects/{project_id}/officer/recycle")
async def recycle_project_officer(request: Request, project_id: str) -> dict[str, Any]:
    """Recycle only the commissioned Officer's disposable runtime pod.

    The existing project owner/admin policy is authoritative.  This is not an
    Officer tool and runtime actors cannot use it to recycle themselves.
    """
    dependencies = get_officer_post_lifecycle_dependencies(request)
    await require_project_owner(
        request, dependencies.store, project_id, allow_archived=False
    )
    return await officer_post_lifecycle.recycle_project_officer(
        request, project_id, dependencies=dependencies
    )


@router.post("/api/projects/{project_id}/officer/commission")
async def commission_project_officer(
    request: Request,
    project_id: str,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Raise an officer onto the project's post.

    Auth: project admin (owner or platform admin) — the kit belongs to the
    century, not to whoever clicked provision. Optional body: the same partial
    kit as PATCH; validated and merged into the row FIRST, so the thread is
    created from the durable record. A stale ended/disabled link completes the
    authoritative handoff before provisioning; the new thread then goes through
    the one create funnel, whose registration claim links it and 409s rivals.
    The continuity brief is his first wake: vacant-since/until, a pointer at
    his restored state + charter, and the while-vacant ledger.

    Project-binding invariant: a commissioned background officer has exactly
    one project — the sole native writable KB — and no override may replace
    that write target. This endpoint satisfies most of it by construction: the
    project comes from the URL, ``ThreadCreateRequest`` carries a single
    ``project_id``, and the kit patch vocabulary
    (``_SESSION_OFFICER_OVERRIDE_KEYS`` + workspace/tools/llm/interactive
    passthrough) has no project or datasource channel. The guard below rejects
    an unparseable project id (fail-closed 422 instead of a DB-layer 500); the
    agent-side attach re-checks the resolved bindings and refuses to boot a
    mis-bound officer.
    """
    # Design: knowledge-base/knowledge/features/officer_post.md §5 (auth:
    # §11 Q1, decided); project-binding invariant:
    # knowledge-base/knowledge/features/officer_knowledge_plane.md §3.1, K1.
    dependencies = get_officer_post_lifecycle_dependencies(request)
    # The project-id shape guard runs BEFORE the membership read, exactly as it
    # did in the moved handler: an unparseable id is a 422 here, not a 500 from
    # the gate's own UUID cast.
    officer_post_lifecycle.require_officer_commission_project_id(project_id)
    user, project = await require_project_owner(
        request, dependencies.store, project_id, allow_archived=False
    )
    return await officer_post_lifecycle.commission_project_officer(
        request,
        project_id,
        body,
        dependencies=dependencies,
        user=user,
        project=project,
    )


@router.post("/api/projects/{project_id}/officer/decommission")
async def decommission_project_officer(
    request: Request,
    project_id: str,
    body: OfficerDecommissionRequest | None = None,
) -> dict[str, Any]:
    """Stand the officer down and keep everything he had.

    Auth: project admin. Returns a 200 warning result listing in-flight jobs unless
    ``force`` — and force only acknowledges the warning: jobs are LEFT
    RUNNING either way; their completions land on the vacant post's ledger.
    The actual hygiene (harvest → queue fold → unlink → incarnation) runs
    inside the shared ``end_thread`` stand-down, so this endpoint and a
    direct thread DELETE are one funnel.
    """
    # Design: knowledge-base/knowledge/features/officer_post.md §5.
    dependencies = get_officer_post_lifecycle_dependencies(request)
    await require_project_owner(request, dependencies.store, project_id)
    return await officer_post_lifecycle.decommission_project_officer(
        request, project_id, body, dependencies=dependencies
    )


@router.post("/api/projects/{project_id}/officer/hold")
async def hold_project_officer(
    request: Request,
    project_id: str,
    body: OfficerHoldRequest | None = None,
) -> dict[str, Any]:
    """Maintenance hold — pause ≠ retire.

    Auth: project admin. Stamps ``officer.hold = {kind, since, note}`` on the
    THREAD (hold is runtime state; a vacant post 400s) with — critically —
    NO ``thread_id`` key: that absence is what keeps the watchdog's
    stale-conference-hold self-heal from ever releasing it. One key, four
    effects, all pre-wired by the conference machinery: drain skips him,
    dispatches 409, watchdog stands down, nothing self-heals.
    """
    # Design: knowledge-base/knowledge/features/officer_post.md §5 (decided 08-01).
    dependencies = get_officer_post_lifecycle_dependencies(request)
    await require_project_owner(request, dependencies.store, project_id)
    return await officer_post_lifecycle.hold_project_officer(
        request, project_id, body, dependencies=dependencies
    )


@router.post("/api/projects/{project_id}/officer/release")
async def release_project_officer(request: Request, project_id: str) -> dict[str, Any]:
    """Release the officer's hold. Auth: project admin.

    Clears via the established lever — deep-merge ``{"officer": {"hold":
    None}}`` → JSON null, which every reader (watchdog, wake claim, dispatch
    fence) treats as unheld. Queued events drain within one ~20s tick; the
    kick below just makes it immediate.
    """
    # Design: knowledge-base/knowledge/features/officer_post.md §5.
    dependencies = get_officer_post_lifecycle_dependencies(request)
    await require_project_owner(
        request, dependencies.store, project_id, allow_archived=False
    )
    return await officer_post_lifecycle.release_project_officer(
        request, project_id, dependencies=dependencies
    )


@router.post("/api/projects/{project_id}/officer/note")
async def send_project_officer_note(
    request: Request,
    project_id: str,
    body: OfficerNoteRequest,
) -> dict[str, Any]:
    """Send the project's officer a one-way note.

    Auth: project owner — a note carries command authority, the same bar as
    hold/release. The reply, if he has one, arrives in his log or as a page;
    this endpoint deliberately has no ask-and-wait leg.

    The response states which durable acceptance happened rather than a bare
    200: ``queued`` is a wake row whose stable identity is claimed by the
    exact current runtime, and ``held`` is fenced behind a hold that must lift
    first. Durable acceptance is never reported as provider admission.
    """
    # Design: knowledge-base/knowledge/features/officer_legate_channel.md.
    dependencies = get_officer_post_lifecycle_dependencies(request)
    user, _project = await require_project_owner(
        request, dependencies.store, project_id, allow_archived=False
    )
    return await officer_post_lifecycle.send_project_officer_note(
        request, project_id, body, dependencies=dependencies, user=user
    )


@router.patch("/api/projects/{project_id}/officer")
async def patch_project_officer(
    request: Request, project_id: str, body: dict[str, Any]
) -> dict[str, Any]:
    """Edit the post — the missing form. Auth: project owner or system admin
    (the kit belongs to the century).

    Writes the durable row always; when commissioned, merges the fragment into
    thread metadata (with an explicitly supplied ``slots`` map replacing the
    whole roster) and injects a one-line notice —
    deliberately NOT a wake (the next sitrep's capacity line carries the
    truth). Shrinking below in-flight is drain semantics, decided: the 409
    lives at the next dispatch, running jobs are untouched.
    ``communication_policy`` is row-only and never touches the thread.
    """
    # Design: knowledge-base/knowledge/features/officer_post.md §7
    # (auth: §11 decision 1).
    dependencies = get_officer_post_lifecycle_dependencies(request)
    await require_project_owner(
        request, dependencies.store, project_id, allow_archived=False
    )
    return await officer_post_lifecycle.patch_project_officer(
        request, project_id, body, dependencies=dependencies
    )


__all__ = [
    "commission_project_officer",
    "decommission_project_officer",
    "get_officer_post_lifecycle_dependencies",
    "get_officer_post_view_dependencies",
    "get_project_officer_summary",
    "hold_project_officer",
    "list_officers",
    "patch_project_officer",
    "recycle_project_officer",
    "release_project_officer",
    "router",
    "send_project_officer_note",
]
