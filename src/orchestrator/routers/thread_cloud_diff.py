"""HTTP adapters for the protected-cloud diff review surface (Slice C, Task 8).

Five routes (each also on a deprecated alias path, below), one gate.
``require_thread_owner`` is awaited first on every one of them — before the
protected-mode check, before any mount row is read — so the ordering of
refusals is unchanged: a non-owner never learns whether the thread is
protected, and a non-protected thread answers 404 rather than 403.

Route order is part of the contract: ``GET .../cloud-diff`` is declared before
``GET .../cloud-diff/{file_path:path}``, and the three POST verbs keep the
positions they had in ``main``.

The routes are served under ``/api/persistent/threads/{thread_id}/cloud-diff``.
They used to live under ``/api/agents/threads/...``, which is the
agent-internal prefix: an edge that blocks ``/api/agents/*`` (the dev tunnel
does) made the browser's review unreachable. Each handler still answers on
that old path through a second, ``deprecated=True`` decorator — the same
handler, gate and response — so a Cockpit that is already loaded or held by
the service worker keeps working for one release. **Remove the
``/api/agents/threads/...`` decorators in a later release.**

The owner gate returns ``(user, thread)``; only the thread row travels into the
operations, which re-derive metadata themselves.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Body, Depends, Request

from orchestrator.security.access import require_thread_owner
from orchestrator.services import thread_cloud_diff

router = APIRouter()


@dataclass(frozen=True)
class ThreadCloudDiffRouteDependencies:
    store: Any
    operations: thread_cloud_diff.ThreadCloudDiffDependencies
    require_thread_owner: Callable[..., Awaitable[Any]] = require_thread_owner


def get_thread_cloud_diff_dependencies(
    request: Request,
) -> ThreadCloudDiffRouteDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.thread_cloud_diff_dependencies_factory()


# =============================================================================
# Protected cloud mode (Slice C, Task 8): owner-facing cloud-diff review.
#
# Each route carries two decorators: the ``/api/persistent`` path, applied
# first, and above it the DEPRECATED ``/api/agents`` alias (see the module
# docstring; remove it in a later release).
#
# Three read/restage endpoints share one gate (``_require_protected``) and one
# resolver (``_thread_cloud_diff_source``) that builds a Task 7
# ``UpperdirDiffSource`` from the thread's ``cloud_ro_mounts`` row alone (its
# reader grant's immutable plan, never the current selection). See
# knowledge-base/knowledge/design/cloud_access_unification.md §5/§11 and
# .superpowers/sdd/task-8-brief.md for the response-shape contract Cockpit
# (Task 14) depends on.
# =============================================================================


@router.get("/api/agents/threads/{thread_id}/cloud-diff", deprecated=True)
@router.get("/api/persistent/threads/{thread_id}/cloud-diff")
async def get_thread_cloud_diff_summary(
    thread_id: str,
    request: Request,
    *,
    dependencies: ThreadCloudDiffRouteDependencies = Depends(
        get_thread_cloud_diff_dependencies
    ),
) -> dict[str, Any]:
    """Protected cloud mode diff summary — owner-facing review surface (Task 8).

    Reads work for ENDED threads too (mount row revoked, ``staged_summary``
    still present) — spec §11; only restage below needs a live workspace.
    Returns ``epoch=0``/empty ``files``/all-zero ``counts`` when nothing has
    been staged yet (no mount row, or a mount row with no staged_summary).
    """
    user, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    return await thread_cloud_diff.get_thread_cloud_diff_summary(
        thread_id=thread_id,
        thread=thread,
        dependencies=dependencies.operations,
    )


@router.get(
    "/api/agents/threads/{thread_id}/cloud-diff/{file_path:path}", deprecated=True
)
@router.get("/api/persistent/threads/{thread_id}/cloud-diff/{file_path:path}")
async def get_thread_cloud_diff_file(
    thread_id: str,
    file_path: str,
    request: Request,
    *,
    dependencies: ThreadCloudDiffRouteDependencies = Depends(
        get_thread_cloud_diff_dependencies
    ),
) -> dict[str, Any]:
    """Protected cloud mode per-file diff content (Task 8).

    404 when the path isn't in the staged diff, including "nothing staged at
    all" and "staged but unreadable" — ``UpperdirDiffSource.file()`` returns
    ``None`` for all three.

    The 404 body carries a ``code`` distinguishing the last case from the
    first two, because the review UI has to explain what happened and the
    three explanations are different: the path left the staged set (the
    session re-staged, or the diff was resolved elsewhere) versus the staged
    tar being missing or failing its content-binding check. Cockpit told every
    reviewer "the session has re-staged", which is wrong for a torn
    manifest/tar pair. The summary is memoized on the source, so the extra
    lookup below costs no I/O.
    """
    user, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    return await thread_cloud_diff.get_thread_cloud_diff_file(
        thread_id=thread_id,
        file_path=file_path,
        thread=thread,
        dependencies=dependencies.operations,
    )


@router.post("/api/agents/threads/{thread_id}/cloud-diff/restage", deprecated=True)
@router.post("/api/persistent/threads/{thread_id}/cloud-diff/restage")
async def restage_thread_cloud_diff(
    thread_id: str,
    request: Request,
    *,
    dependencies: ThreadCloudDiffRouteDependencies = Depends(
        get_thread_cloud_diff_dependencies
    ),
) -> dict[str, Any]:
    """Owner-triggered refresh of the staged protected-cloud diff (Task 8).

    Schedules the same ``stage_thread_cloud_diff`` background task the
    turn-end internal ping uses (``_cloud_stage_tasks`` registry, Task 5),
    fire-and-forget. Unlike the read endpoints above, restage needs a LIVE
    workspace: 409 ``{"code": "no_workspace"}`` when the thread's workspace
    host can't be resolved (ended thread, pod not yet ready, etc).
    """
    # ``_cloud_stage_tasks`` was main's module dict; R1.B04 made it the
    # ``stage`` half of the application-owned ``CloudTaskRegistry``. The
    # docstring keeps the old name verbatim because FastAPI publishes it
    # as this operation's OpenAPI description, and an extraction must not
    # change the API document.
    user, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    return await thread_cloud_diff.restage_thread_cloud_diff(
        thread_id=thread_id,
        thread=thread,
        dependencies=dependencies.operations,
    )


@router.post("/api/agents/threads/{thread_id}/cloud-diff/apply", deprecated=True)
@router.post("/api/persistent/threads/{thread_id}/cloud-diff/apply")
async def apply_thread_cloud_diff(
    request: Request,
    thread_id: str,
    body: dict = Body(...),
    *,
    dependencies: ThreadCloudDiffRouteDependencies = Depends(
        get_thread_cloud_diff_dependencies
    ),
) -> dict[str, Any]:
    """Owner-triggered apply of the staged protected-cloud diff (Task 10).

    Whole-diff, epoch-pinned write-back to the real cloud folder — see
    ``services.cloud_staging.apply`` module docstring for the full flow and
    its invariants (conflict gate, deletes-first, fail-soft partial writes,
    baseline re-capture on full success).
    """
    user, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    return await thread_cloud_diff.apply_thread_cloud_diff(
        thread_id=thread_id,
        thread=thread,
        body=body,
        dependencies=dependencies.operations,
        user=user,
    )


@router.post("/api/agents/threads/{thread_id}/cloud-diff/reject", deprecated=True)
@router.post("/api/persistent/threads/{thread_id}/cloud-diff/reject")
async def reject_thread_cloud_diff(
    request: Request,
    thread_id: str,
    body: dict = Body(...),
    *,
    dependencies: ThreadCloudDiffRouteDependencies = Depends(
        get_thread_cloud_diff_dependencies
    ),
) -> dict[str, Any]:
    """Owner-triggered rejection of the staged protected-cloud diff (Task 10).

    Same epoch pin as apply, but never touches the cloud — see
    ``services.cloud_staging.apply`` module docstring.
    """
    user, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    return await thread_cloud_diff.reject_thread_cloud_diff(
        thread_id=thread_id,
        thread=thread,
        body=body,
        dependencies=dependencies.operations,
    )
