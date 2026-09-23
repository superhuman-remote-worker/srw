"""Durable decision of one pending session permission gate (R1.B10).

A permission gate waits on its ``thread_permission_requests`` row, not on any
transport: the DB trigger fires NOTIFY and the agent's LISTEN wakes its
``permission_check``. This module owns the one CAS UPDATE that decides it, shared
by the owner REST endpoint and the notification feed's approve/deny actions.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException


async def decide_permission_request(
    store: Any,
    thread_id: str,
    approval_id: str,
    decision: str,
    *,
    decided_by: str,
) -> dict[str, Any]:
    """The one UPDATE that decides a permission gate — shared by the REST
    endpoint and the notification's approve/deny actions. Raises the
    endpoint's HTTP errors: 400 bad decision, 404 unknown, 409 decided."""
    if decision == "approve":
        new_status = "approved"
    elif decision == "deny":
        new_status = "denied"
    else:
        raise HTTPException(
            status_code=400,
            detail="decision must be 'approve' or 'deny'",
        )

    async with store.acquire() as conn:
        # Lookup-then-update so we can distinguish 404 (wrong id/thread)
        # from 409 (already decided).
        existing = await conn.fetchrow(
            "SELECT id, status, tool_call_id FROM thread_permission_requests "
            "WHERE id = $1 AND thread_id = $2",
            approval_id,
            thread_id,
        )
        if existing is None:
            raise HTTPException(
                status_code=404,
                detail="Permission request not found for this thread",
            )
        if existing["status"] != "pending":
            raise HTTPException(
                status_code=409,
                detail=f"Already {existing['status']}",
            )
        row = await conn.fetchrow(
            "UPDATE thread_permission_requests "
            "SET status = $2, decided_at = now(), decided_by = $3 "
            "WHERE id = $1 AND status = 'pending' "
            "RETURNING id, status, tool_call_id",
            approval_id,
            new_status,
            decided_by,
        )
    if row is None:
        # Lost the race — somebody else just decided this. Idempotency.
        raise HTTPException(
            status_code=409,
            detail="Already decided (race lost)",
        )
    return {
        "accepted": True,
        "decision": decision,
        "approval_id": str(row["id"]),
        "status": row["status"],
        "tool_call_id": row["tool_call_id"],
    }


__all__ = ["decide_permission_request"]
