"""Session permission decisions: owner REST and emailed magic links (R1.B10).

Headless persistent sessions — Phase 4 magic-link routes. Email magic-links land
at ``/magic/approve/{token}``. GET renders a confirmation page (read-only,
prefetch-safe). POST consumes the token and UPDATEs
``thread_permission_requests`` via the same trigger path as the cockpit WS
approve handler. ``/magic/extend/{token}`` bumps the attention-sleep window
without consuming the approval token.

The token is the credential for the magic routes: they carry no session
authentication, exactly as before. Collaborators come from the requesting
application's ``thread_permission_dependencies_factory``.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from orchestrator.services import headless_notifications, magic_link_pages
from orchestrator.services.thread_permissions import decide_permission_request

router = APIRouter()


class ThreadApproveRequest(BaseModel):
    """Body for POST /api/persistent/threads/{id}/approve/{approval_id}."""

    decision: str  # "approve" or "deny"


@dataclass(frozen=True, slots=True)
class ThreadPermissionDependencies:
    """Application-owned collaborators for one permission request."""

    store: Any
    require_thread_owner: Callable[
        [Request, Any, str], Awaitable[tuple[dict[str, Any], dict[str, Any]]]
    ]
    notification_service: Any
    cockpit_url: Callable[[], str]
    wake_after_permission_decision: Callable[..., Awaitable[None]]


def get_thread_permission_dependencies(
    request: Request,
) -> ThreadPermissionDependencies:
    return request.app.state.thread_permission_dependencies_factory()


@router.post("/api/persistent/threads/{thread_id}/approve/{approval_id}")
async def thread_approve(
    thread_id: str,
    approval_id: str,
    body: ThreadApproveRequest,
    request: Request,
    *,
    dependencies: ThreadPermissionDependencies = Depends(
        get_thread_permission_dependencies
    ),
) -> dict[str, Any]:
    """Resolve a pending permission gate by updating thread_permission_requests
    directly. The DB trigger fires NOTIFY → the agent's LISTEN wakes its
    permission_check. No agent forwarding hop — this endpoint is the
    canonical resolution path for magic-link approvals and MCP clients
    alike. The cockpit WS approve method does the same UPDATE inside the
    agent for back-compat.

    Returns:
        200 — request resolved (status flipped)
        400 — invalid decision
        403 — not thread owner
        404 — approval_id not found, or wrong thread, or no pending request
        409 — request already decided (idempotent re-clicks land here)
    """
    user, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    decided_by = str(user.get("id") or user.get("sub") or "rest_client")
    outcome = await decide_permission_request(
        dependencies.store,
        thread_id,
        approval_id,
        body.decision,
        decided_by=decided_by,
    )
    await dependencies.notification_service.resolve_source(
        "permission_request", approval_id, resolved_by=f"user:{decided_by}"
    )
    return outcome


def _tool_args_preview(tool_args: Any) -> str:
    if isinstance(tool_args, str):
        try:
            tool_args = json.loads(tool_args)
        except Exception:
            tool_args = {}
    elif tool_args is None:
        tool_args = {}
    args_preview = json.dumps(tool_args, indent=2, default=str)
    if len(args_preview) > 600:
        args_preview = args_preview[:600] + "\n… (truncated)"
    return args_preview


@router.get("/magic/approve/{token}")
async def magic_link_get(
    token: str,
    *,
    dependencies: ThreadPermissionDependencies = Depends(
        get_thread_permission_dependencies
    ),
) -> HTMLResponse:
    """Show a confirmation page for the magic-link token.

    Does NOT consume the token (POST does). This separation is critical:
    email link previewers (Outlook Safe Links, Gmail) auto-fetch URLs
    server-side; a GET-executes link would be consumed by a bot before
    the human ever clicks.
    """
    store = dependencies.store
    cockpit_external_url = dependencies.cockpit_url()

    row = await headless_notifications.validate_magic_link(store, token)
    if row is None:
        return HTMLResponse(
            magic_link_pages.magic_link_result_page(
                title="Link expired or already used",
                body=(
                    "This approval link is no longer valid. It may have "
                    "expired, been used already, or been invalidated by a "
                    "newer approval. Open the cockpit to see the current "
                    "state."
                ),
                cockpit_url=cockpit_external_url,
                is_error=True,
            ),
            status_code=404,
        )

    # Fetch tool details for the confirmation page.
    async with store.acquire() as conn:
        permission_row = await conn.fetchrow(
            "SELECT id, tool_name, tool_args, status "
            "FROM thread_permission_requests WHERE id = $1",
            row["approval_id"],
        )

    if permission_row is None or permission_row["status"] != "pending":
        return HTMLResponse(
            magic_link_pages.magic_link_result_page(
                title="Already decided",
                body=(
                    "The agent's request has already been resolved. No "
                    "further action is needed."
                ),
                cockpit_url=cockpit_external_url,
            ),
            status_code=409,
        )

    page = magic_link_pages.magic_link_confirmation_page(
        tool_name=permission_row["tool_name"],
        tool_args_preview=_tool_args_preview(permission_row["tool_args"]),
        intended_decision=row.get("intended_decision"),
        token=token,
    )
    return HTMLResponse(page)


@router.post("/magic/approve/{token}")
async def magic_link_post(
    token: str,
    *,
    dependencies: ThreadPermissionDependencies = Depends(
        get_thread_permission_dependencies
    ),
) -> HTMLResponse:
    """Consume the token and resolve the permission request.

    CAS UPDATE on magic_link_tokens (single-use) + a second UPDATE on
    thread_permission_requests (which the agent's LISTEN picks up via
    the existing trigger). Distinguishes 404 (invalid) from 409 (token
    already used or request already decided) for clean UX on double-clicks.
    """
    store = dependencies.store
    cockpit_external_url = dependencies.cockpit_url()

    row = await headless_notifications.validate_magic_link(store, token)
    if row is None:
        return HTMLResponse(
            magic_link_pages.magic_link_result_page(
                title="Link expired or already used",
                body=(
                    "This approval link is no longer valid. It may have "
                    "expired or been used already."
                ),
                cockpit_url=cockpit_external_url,
                is_error=True,
            ),
            status_code=404,
        )

    decision = row.get("intended_decision") or "approved"

    consumed = await headless_notifications.consume_magic_link(
        store, str(row["id"]), decision
    )
    if consumed is None:
        return HTMLResponse(
            magic_link_pages.magic_link_result_page(
                title="Already used",
                body=(
                    "This link has already been used. The agent's request "
                    "is being processed."
                ),
                cockpit_url=cockpit_external_url,
            ),
            status_code=409,
        )

    # Resolve the permission request. CAS-style UPDATE so we don't race
    # with the cockpit having already decided it.
    decided_by_label = "magic_link"
    if consumed.get("user_id"):
        decided_by_label = f"user:{consumed['user_id']}"
    async with store.acquire() as conn:
        permission_row = await conn.fetchrow(
            "UPDATE thread_permission_requests "
            "SET status = $2, decided_at = now(), decided_by = $3 "
            "WHERE id = $1 AND status = 'pending' "
            "RETURNING id, status, tool_call_id, tool_name, thread_id",
            consumed["approval_id"],
            decision,
            decided_by_label,
        )

    if permission_row is None:
        return HTMLResponse(
            magic_link_pages.magic_link_result_page(
                title="Already decided",
                body=(
                    "The agent's request was already resolved by another "
                    "approval path (cockpit click, REST, or expired). "
                    "Your action was not needed."
                ),
                cockpit_url=cockpit_external_url,
            ),
            status_code=409,
        )

    # Phase 5: if attention sleep fired since the email was sent, wake through
    # the thread's existing execution plane. Pinned sessions retain workspace
    # restore + agent-pod re-creation. Stateless sessions retain their exact
    # queued/leased turn and converge the workspace without binding a pod. The
    # permission-row id is the wake task's freshness fence.
    asyncio.create_task(
        dependencies.wake_after_permission_decision(
            str(permission_row["thread_id"]),
            permission_request_id=str(permission_row["id"]),
        ),
        name=f"phase5-wake-{str(permission_row['thread_id'])[:8]}",
    )

    pretty = "approved" if decision == "approved" else "denied"
    return HTMLResponse(
        magic_link_pages.magic_link_result_page(
            title=f"Tool {pretty}",
            body=(
                f"The agent's request to call "
                f"<code>{permission_row['tool_name']}</code> has been "
                f"{pretty}. The agent will resume shortly."
            ),
            cockpit_url=cockpit_external_url,
        )
    )


@router.post("/magic/extend/{token}")
async def magic_link_extend(
    token: str,
    *,
    dependencies: ThreadPermissionDependencies = Depends(
        get_thread_permission_dependencies
    ),
) -> HTMLResponse:
    """Extend the attention-sleep window for the thread bound to this token.

    Validates the token (same hash + expiry + single-use checks as
    /magic/approve) but does NOT consume it — the user is signaling
    "I'm still reviewing" without making the approve decision. Bumps
    threads.awaiting_user_since forward by 60 minutes per click, capped
    at HEADLESS_EXTEND_CAP (default 4 = 4h total ceiling).

    Re-renders the confirmation page with a toast so the user can still
    click approve/deny on the same screen. Status_code 200 throughout —
    the page itself carries the success/cap/not-awaiting signal.

    Why a separate route and not "extend ↔ approve same POST": the
    approve handler consumes the token (single-use CAS). If extend
    shared that path, every extend click would burn the approval token
    and the user couldn't approve afterward.
    """
    store = dependencies.store
    cockpit_external_url = dependencies.cockpit_url()
    extend_cap = magic_link_pages.MAGIC_EXTEND_CAP

    row = await headless_notifications.validate_magic_link(store, token)
    if row is None:
        return HTMLResponse(
            magic_link_pages.magic_link_result_page(
                title="Link expired or already used",
                body=(
                    "This link is no longer valid. Open the cockpit to "
                    "review the agent's current state."
                ),
                cockpit_url=cockpit_external_url,
                is_error=True,
            ),
            status_code=404,
        )

    thread_id = row.get("thread_id")
    if thread_id is None:
        return HTMLResponse(
            magic_link_pages.magic_link_result_page(
                title="Cannot extend",
                body="This link is not bound to a thread.",
                cockpit_url=cockpit_external_url,
                is_error=True,
            ),
            status_code=400,
        )

    # Bump awaiting_user_since iff the thread is still in awaiting_user
    # and extend_count < cap. The CAS UPDATE returns the new row state so
    # we can show the right banner. status='active' or 'suspended' means
    # there's nothing to extend — the agent has either woken up already
    # or moved beyond awaiting_user.
    async with store.acquire() as conn:
        updated = await conn.fetchrow(
            "UPDATE threads "
            "SET awaiting_user_since = now(), "
            "    extend_count = extend_count + 1 "
            "WHERE id = $1 "
            "  AND status = 'awaiting_user' "
            "  AND extend_count < $2 "
            "RETURNING extend_count",
            str(thread_id),
            extend_cap,
        )

    if updated is None:
        # Distinguish cap_reached from not_awaiting for the banner copy.
        async with store.acquire() as conn:
            row_state = await conn.fetchrow(
                "SELECT status, extend_count FROM threads WHERE id = $1",
                str(thread_id),
            )
        if row_state is None:
            extend_status = "not_awaiting"
        elif row_state["status"] != "awaiting_user":
            extend_status = "not_awaiting"
        elif row_state["extend_count"] >= extend_cap:
            extend_status = "cap_reached"
        else:
            # Edge case — concurrent change between our UPDATE and SELECT.
            # Render not_awaiting which is the gentler banner.
            extend_status = "not_awaiting"
        extends_remaining = None
    else:
        extend_status = "extended"
        extends_remaining = max(0, extend_cap - int(updated["extend_count"]))

    # Re-render the confirmation page with the banner. Load the permission
    # row again (status may have changed underneath us).
    approval_id = row.get("approval_id")
    if approval_id is not None:
        async with store.acquire() as conn:
            permission_row = await conn.fetchrow(
                "SELECT tool_name, tool_args, status FROM "
                "thread_permission_requests WHERE id = $1",
                approval_id,
            )
    else:
        permission_row = None

    if permission_row is None or permission_row["status"] != "pending":
        return HTMLResponse(
            magic_link_pages.magic_link_result_page(
                title="Already decided",
                body=(
                    "The agent's request has been resolved. No further "
                    "action is needed."
                ),
                cockpit_url=cockpit_external_url,
            ),
            status_code=200,
        )

    page = magic_link_pages.magic_link_confirmation_page(
        tool_name=permission_row["tool_name"],
        tool_args_preview=_tool_args_preview(permission_row["tool_args"]),
        intended_decision=row.get("intended_decision"),
        token=token,
        extend_status=extend_status,
        extends_remaining=extends_remaining,
    )
    return HTMLResponse(page)


__all__ = [
    "ThreadApproveRequest",
    "ThreadPermissionDependencies",
    "get_thread_permission_dependencies",
    "router",
]
