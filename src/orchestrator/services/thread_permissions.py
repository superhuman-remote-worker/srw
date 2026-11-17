"""Durable decision of one pending session permission gate (R1.B10).

A permission gate waits on its ``thread_permission_requests`` row, not on any
transport: the DB trigger fires NOTIFY and the agent's LISTEN wakes its
``permission_check``. This module owns the one CAS UPDATE that decides it, shared
by the owner REST endpoint and the notification feed's approve/deny actions.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException


_QUESTION_DECISIONS = {"approve": "approved", "deny": "denied", "cancel": "expired"}


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return {}
    return dict(value) if isinstance(value, dict) else {}


def _question_response(
    request: dict[str, Any], decision: str, content: Any, always_accept: bool
) -> dict[str, Any]:
    """The answer to an MCP server's question, checked against its form.
    A yes/no accepted without content sends the yes; any other accepted
    form must fit (422 with the reason, so the card can say what to fix).
    "Always accept" applies to a yes/no only."""
    from shared.mcp_questions import content_problem

    confirmation = request.get("confirmation")
    if always_accept and (decision != "approve" or not isinstance(confirmation, dict)):
        raise HTTPException(
            status_code=400,
            detail="always_accept applies only to accepting a yes/no question",
        )
    if decision != "approve":
        return {"content": None, "always_accept": False}
    if request.get("mode") == "url":
        return {"content": None, "always_accept": False}
    if content is None and isinstance(confirmation, dict):
        content = confirmation
    problem = content_problem(request.get("schema"), content)
    if problem is not None:
        raise HTTPException(status_code=422, detail=f"The answer {problem}.")
    return {"content": content, "always_accept": bool(always_accept)}


async def decide_permission_request(
    store: Any,
    thread_id: str,
    approval_id: str,
    decision: str,
    *,
    decided_by: str,
    content: Any = None,
    always_accept: bool = False,
) -> dict[str, Any]:
    """The one UPDATE that decides a permission gate — shared by the REST
    endpoint and the notification's approve/deny actions. Raises the
    endpoint's HTTP errors: 400 bad decision, 404 unknown, 409 decided,
    422 an answer that does not fit a server's form.

    A row may instead hold an MCP server's question (``kind = 'question'``):
    ``approve`` accepts it with ``content`` (``always_accept`` also turns the
    connector to accept for this Session), ``deny`` declines it, and
    ``cancel`` dismisses it unanswered (``expired``, never a refusal)."""
    if decision not in _QUESTION_DECISIONS:
        raise HTTPException(
            status_code=400,
            detail="decision must be 'approve', 'deny' or 'cancel'",
        )

    async with store.acquire() as conn:
        async with conn.transaction():
            # Lookup-then-update so we can distinguish 404 (wrong id/thread)
            # from 409 (already decided).
            existing = await conn.fetchrow(
                "SELECT id, status, tool_call_id, kind, request "
                "FROM thread_permission_requests "
                "WHERE id = $1 AND thread_id = $2",
                approval_id,
                thread_id,
            )
            if existing is None:
                raise HTTPException(
                    status_code=404,
                    detail="Permission request not found for this thread",
                )
            question = existing.get("kind") == "question"
            if not question and decision == "cancel":
                raise HTTPException(
                    status_code=400,
                    detail="decision must be 'approve' or 'deny'",
                )
            if existing["status"] != "pending":
                raise HTTPException(
                    status_code=409,
                    detail=f"Already {existing['status']}",
                )
            request = _json_object(existing.get("request")) if question else {}
            response = (
                _question_response(request, decision, content, always_accept)
                if question
                else None
            )
            row = await conn.fetchrow(
                "UPDATE thread_permission_requests "
                "SET status = $2, decided_at = now(), decided_by = $3, "
                "    response = $4::jsonb "
                "WHERE id = $1 AND status = 'pending' "
                "RETURNING id, status, tool_call_id",
                approval_id,
                _QUESTION_DECISIONS[decision],
                decided_by,
                json.dumps(response) if response is not None else None,
            )
            if row is None:
                # Lost the race — somebody else just decided this. Idempotency.
                raise HTTPException(
                    status_code=409,
                    detail="Already decided (race lost)",
                )
            datasource_id = request.get("datasource_id")
            if response and response.get("always_accept") and datasource_id:
                # "Always accept in this session": the next dispatch of this
                # Session delivers the connector as accept
                # (thread_datasource_authorization.stamp_session_server_questions).
                await conn.execute(
                    "UPDATE threads SET metadata = COALESCE(metadata, '{}'::jsonb) "
                    "|| jsonb_build_object('mcp_server_questions', "
                    "   COALESCE(metadata->'mcp_server_questions', '{}'::jsonb) "
                    "   || jsonb_build_object($2::text, 'accept')) "
                    "WHERE id = $1::uuid",
                    thread_id,
                    str(datasource_id),
                )
    return {
        "accepted": True,
        "decision": decision,
        "approval_id": str(row["id"]),
        "status": row["status"],
        "tool_call_id": row["tool_call_id"],
    }


__all__ = ["decide_permission_request"]
