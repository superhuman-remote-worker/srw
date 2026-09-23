"""Messaging tools for agent-human communication.

Provides the `send_message` tool that allows agents to send emails to
job owners (Phase 1) with messages stored as workspace files. Supports
async (fire-and-forget) and blocking (pause-until-reply) modes.

Messages are persisted in workspace/messages/<thread_id>/ as markdown
files with YAML frontmatter, making them visible via git_diff during
strategic review phases.
"""

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, List, Optional

import httpx
from langchain_core.tools import tool

from agent.tools.context import ToolContext

from shared.tool_catalog.definitions import (
    COMMUNICATION_TOOLS_METADATA as COMMUNICATION_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)


def _mask_email(email: str) -> str:
    """Mask an email address for display (a***@example.com)."""
    if not email or "@" not in email:
        return email
    local, domain = email.rsplit("@", 1)
    if len(local) <= 1:
        return f"*@{domain}"
    return f"{local[0]}***@{domain}"


def _build_message_content(
    direction: str,
    to: str,
    to_name: str,
    subject: str,
    thread_id: str,
    sequence: int,
    body: str,
    mode: str | None = None,
    status: str = "delivered",
    from_field: str = "agent",
    purpose: str | None = None,
) -> str:
    """Build a message file with YAML frontmatter."""
    frontmatter_lines = [
        "---",
        f"from: {from_field}",
        f"to: {to}",
        f"to_name: {to_name}",
        f"date: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}",
        f"subject: {subject}",
        f"thread: {thread_id}",
        f"sequence: {sequence}",
    ]
    if mode:
        frontmatter_lines.append(f"mode: {mode}")
    if purpose:
        frontmatter_lines.append(f"purpose: {purpose}")
    frontmatter_lines.append(f"status: {status}")
    frontmatter_lines.append("---")
    frontmatter_lines.append("")

    return "\n".join(frontmatter_lines) + body + "\n"


def create_communication_tools(context: ToolContext) -> List[Any]:
    """Create communication tools with injected context.

    Args:
        context: ToolContext with workspace_manager and job_id

    Returns:
        List of LangChain tool functions
    """

    @tool
    async def send_message(
        to: str,
        subject: str,
        message: str,
        mode: str = "async",
        thread_id: Optional[str] = None,
        purpose: Optional[str] = None,
    ) -> str:
        """Send a message to a human via email.

        Messages are stored as workspace files in messages/<thread_id>/ and
        delivered through the orchestrator. Use sparingly — depending on the
        project's routing policy the recipient is the owner (email) or the
        project's officer first, with automatic escalation/fallback to the
        owner.

        Args:
            to: Recipient. Use "user" for the job owner.
            subject: Subject line (max 200 chars). Carried from first message
                when replying to an existing thread.
            message: Message body in markdown (max 5000 chars).
            mode: "async" to continue working, or "blocking" to pause
                execution until the recipient replies.
            thread_id: Reply to an existing thread. Omit to start a new thread.
            purpose: Optional label improving triage/presentation:
                "question", "blocker", or "update". Routing never trusts it;
                mode="blocking" stays the mechanical wait signal.

        Returns:
            Confirmation with thread ID, delivery status, and file path.
        """
        # Validate inputs
        if not subject or not subject.strip():
            return "Error: subject is required."
        if len(subject) > 200:
            return f"Error: subject too long ({len(subject)} chars, max 200)."
        if not message or not message.strip():
            return "Error: message body is required."
        if len(message) > 5000:
            return f"Error: message too long ({len(message)} chars, max 5000)."
        if mode not in ("async", "blocking"):
            return f"Error: mode must be 'async' or 'blocking', got '{mode}'."
        if purpose is not None and purpose not in ("question", "blocker", "update"):
            return (
                "Error: purpose must be 'question', 'blocker', or 'update' "
                f"(got '{purpose}'). Omit it if unsure."
            )

        # Check config restriction on recipients
        if to != "user":
            allowed = "project"
            if hasattr(context, "config") and context.config:
                comm_cfg = getattr(context.config, "communication", None)
                if isinstance(comm_cfg, dict):
                    allowed = comm_cfg.get("allowed_recipients", "project")
            if allowed == "owner":
                return (
                    "Error: this agent config restricts messaging to the job owner only "
                    "(to='user'). Set allowed_recipients: project in config to enable "
                    "multi-recipient messaging."
                )

        job_id = context.job_id
        if not job_id:
            return "Error: no job_id available."

        # Generate or validate thread_id
        is_new_thread = thread_id is None
        if is_new_thread:
            thread_id = uuid.uuid4().hex[:6]

        # Determine sequence number from workspace files
        msg_dir = f"messages/{thread_id}"
        sequence = 1
        if context.has_workspace():
            try:
                existing = context.workspace_manager.list_directory(msg_dir)
                sequence = len(existing) + 1
            except Exception:
                sequence = 1  # Directory doesn't exist yet

        # Build and write message file
        file_name = f"{sequence:03d}_sent.md"
        file_path = f"{msg_dir}/{file_name}"

        file_content = _build_message_content(
            direction="outbound",
            to="(resolved by orchestrator)",
            to_name="(resolved by orchestrator)",
            subject=subject,
            thread_id=thread_id,
            sequence=sequence,
            body=message,
            mode=mode,
            status="pending",
            purpose=purpose,
        )

        if context.has_workspace():
            try:
                context.workspace_manager.write_file(file_path, file_content)
            except Exception as e:
                return f"Error: failed to write message file: {e}"

            # Git commit
            if context.has_git():
                try:
                    context.workspace_manager.git_manager.commit(
                        f"Message sent: {subject[:50]}"
                    )
                except Exception:
                    pass  # Non-critical

        # Call orchestrator to send email + log + handle blocking
        orchestrator_url = os.getenv("ORCHESTRATOR_URL", "http://localhost:8085")
        api_url = f"{orchestrator_url}/api/jobs/{job_id}/messages/send"

        # P4b: send X-Internal-Key so the orchestrator's require_internal gate
        # accepts the call. Without the key the endpoint 401s (it's pure
        # agent-internal — no cockpit caller). When the calling session
        # carries a user identity (persistent-thread sessions do; worker
        # mode does not), also forward X-MCP-User-Id so the orchestrator's
        # _get_user_from_mcp_headers can resolve the originating user for
        # audit and any future user-scoped checks on this endpoint.
        headers: dict[str, str] = {}
        internal_key = os.getenv("MCP_INTERNAL_KEY", "")
        if internal_key:
            headers["X-Internal-Key"] = internal_key
        if context.user_id:
            headers["X-MCP-User-Id"] = context.user_id

        try:
            routing_generation = str(uuid.uuid4())
            async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
                payload = {
                    "to": to,
                    "subject": subject,
                    "message": message,
                    "mode": mode,
                    "thread_id": thread_id,
                    "purpose": purpose,
                    "project_id": getattr(context, "project_id", None),
                    "lease_token": (
                        context._worker_lease_token
                        if context._stateless_worker
                        else None
                    ),
                    "agent_id": (
                        getattr(context.orchestrator_client, "agent_id", None)
                        if not context._stateless_worker
                        else None
                    ),
                    "routing_generation": routing_generation,
                }
                if not context._stateless_worker:
                    client = context.orchestrator_client
                    if getattr(client, "pinned_delivery_job_id", None) == job_id:
                        payload.update({
                            "pinned_delivery_id": getattr(client, "pinned_delivery_id", None),
                            "pinned_projection_digest": getattr(client, "pinned_projection_digest", None),
                            "pinned_delivery_proof": getattr(client, "pinned_delivery_proof", None),
                            "pinned_process_generation": getattr(client, "dispatch_process_generation", None),
                            "pinned_pod_uid": os.environ.get("POD_UID"),
                        })
                # One bounded transport retry covers response loss while the
                # same durable generation suppresses a second quota charge,
                # route, or provider delivery. A fresh model-authored send is
                # still a fresh generation.
                for send_attempt in range(2):
                    try:
                        resp = await client.post(api_url, json=payload)
                    except httpx.TransportError:
                        if send_attempt == 0:
                            continue
                        raise
                    if resp.status_code >= 500 and send_attempt == 0:
                        continue
                    break

            if resp.status_code == 429:
                data = resp.json()
                return (
                    f"Rate limit exceeded. {data.get('error', '')} "
                    f"Retry after {data.get('retry_after_seconds', 0)} seconds. "
                    f"Message saved to workspace: {file_path}"
                )

            if resp.status_code != 200:
                error_detail = resp.text[:200]
                logger.warning(
                    f"Orchestrator message send failed ({resp.status_code}): {error_detail}"
                )
                return (
                    f"Message saved to workspace ({file_path}) but email delivery "
                    f"failed: HTTP {resp.status_code}. The message is preserved in "
                    f"the workspace and can be retried."
                )

            data = resp.json()
            masked_recipient = data.get("recipient", "unknown")

            # Update message file status to delivered
            if context.has_workspace():
                try:
                    updated_content = file_content.replace(
                        "status: pending", "status: delivered"
                    )
                    if "to_name" in data:
                        updated_content = updated_content.replace(
                            "to_name: (resolved by orchestrator)",
                            f"to_name: {data['to_name']}",
                        )
                    updated_content = updated_content.replace(
                        "to: (resolved by orchestrator)",
                        f"to: {masked_recipient}",
                    )
                    context.workspace_manager.write_file(file_path, updated_content)
                except Exception:
                    pass  # Non-critical

        except httpx.RequestError as e:
            logger.warning(f"Failed to reach orchestrator for message send: {e}")
            return (
                f"Message saved to workspace ({file_path}) but orchestrator "
                f"unreachable: {e}. Email delivery skipped."
            )

        # Routing note (officer_message_routing): tell the worker WHO holds
        # its message so a blocking wait reads honestly in the transcript.
        routing_info = data.get("routing") or {}
        routing_note = ""
        if routing_info.get("applied") == "officer_first":
            routing_note = (
                "\nRouting: delivered to the project officer first; if the "
                "officer does not answer in time it escalates to the user "
                "automatically."
            )
        elif routing_info.get("applied") == "officer_and_user":
            routing_note = (
                "\nRouting: delivered to the project officer AND the user; "
                "the first answer counts."
            )

        # If blocking mode, request job freeze
        if mode == "blocking" and resp.status_code == 200:
            freeze_payload = {
                "status": "waiting_for_reply",
                "freeze_type": "blocking_message",
                "thread_id": thread_id,
                "subject": subject,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "job_id": job_id,
            }
            # The orchestrator's freeze (written in the send transaction) is
            # authoritative and carries the route generation; mirroring the
            # route_id here keeps the local job_frozen.json in agreement.
            if routing_info.get("route_id"):
                freeze_payload["route_id"] = routing_info["route_id"]
            context.request_freeze(freeze_payload)

            return (
                f"Message sent to {masked_recipient} (thread: {thread_id}).\n"
                f"File: {file_path}{routing_note}\n\n"
                f"Job execution is now paused. Waiting for reply. "
                f"Execution will resume automatically when the recipient responds."
            )

        # Async mode confirmation
        return (
            f"Message sent to {masked_recipient} (thread: {thread_id}).\n"
            f"File: {file_path}{routing_note}\n"
            f"Mode: async — continuing execution."
        )

    return [send_message]
