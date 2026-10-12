"""Agent-initiated workspace-tier-upgrade request tool.

A lite (``virtual``/``none``) agent has no shell to "attempt", so it can never
trip the sudo→VM freeze a sandbox agent uses to ask for more privilege. This
gives a lite agent an explicit, auditable request path:
``request_workspace_upgrade(reason)`` sets a ``workspace_upgrade_required``
freeze — it only REQUESTS, it never flips the tier. The transport turns that
freeze into a human-in-the-loop offer (``workspace_upgrade.needed``) the user
must approve before anything provisions (workspace_tier_upgrade.md §4.2 S5,
§4.4 Sec-4: the tier-control surface stays out of the agent's reach).

"Freeze" is a misnomer on the session path: ``request_freeze`` only sets a
one-shot slot the graph reads and clears mid-loop, then falls through to the
next LLM iteration (persistent_graph.py, "Continue the inner loop"). Sessions
have no should_stop/freeze_data state — the agent keeps talking and ends its
turn normally, and nothing resumes it. That is why the copy below refuses to
promise a continuation: only the human can grant one.

Category ``core`` (not an execution category), so it survives
``filter_tools_by_backend`` on the lite tiers where it actually matters; the
session only exposes it while the backend has no shell.

On the pinned lane the in-process container swap is refused
(persistent_app.py _handle_workspace_upgrade), so the offer asks for a VM, and
only after the orchestrator confirms a VM upgrade would be accepted
(GET /api/agents/threads/{id}/upgrade-availability). Otherwise no offer is
raised and the model is told why (stateless upgrade design, decision 1).
"""

import logging
from datetime import datetime, timezone
from typing import Any, List, Optional

from langchain_core.tools import tool

from agent.tools.context import ToolContext

from shared.tool_catalog.definitions import (
    WORKSPACE_UPGRADE_TOOLS_METADATA as WORKSPACE_UPGRADE_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)

_CHECK_FAILED = "SRW couldn't check whether an upgrade is available right now."


async def _vm_upgrade_unavailable_reason(context: ToolContext) -> Optional[str]:
    """None when a VM upgrade would be accepted now; otherwise why not."""
    # Imported at call time, not module level: the tests patch
    # agent.tools.orchestrator.jobs._get_client, which a module-level
    # `from` import would have already bound. (No import cycle exists.)
    from agent.tools.orchestrator.jobs import _get_client, _get_orchestrator_url

    url = (
        f"{_get_orchestrator_url()}/api/agents/threads/"
        f"{context.thread_id}/upgrade-availability"
    )
    try:
        async with _get_client(user_id=context.user_id) as client:
            resp = await client.get(url, timeout=10)
            resp.raise_for_status()
            vm = (resp.json() or {}).get("vm") or {}
    except Exception as exc:  # the offer is optional; never raise into the turn
        logger.warning("upgrade availability check failed: %s", exc)
        return _CHECK_FAILED
    if vm.get("available") is True:
        return None
    return str(vm.get("reason") or "SRW would refuse the upgrade.")


def create_workspace_upgrade_tools(context: ToolContext) -> List[Any]:
    """Create the agent-initiated workspace-upgrade request tool.

    No workspace/todo dependency — it only records a freeze request on the
    ToolContext, so it loads on the lite tiers (``todo_manager=None``).
    """

    @tool
    async def request_workspace_upgrade(reason: str) -> str:
        """Ask to upgrade from this lite workspace to one with a shell.

        Call this when the task needs capabilities this workspace lacks — a
        shell, git, running code or builds, or browser control. In a Session
        SRW offers a VM, and only when it would accept one; in a Job the
        workspace is upgraded to a container.

        You are only REQUESTING. A human is shown your request and decides;
        if they approve, the workspace is provisioned and your existing files
        carry over. If SRW can't offer an upgrade, nothing is offered and you
        are told why.

        This request does not provision a workspace or automatically resume
        the task. Explain what the upgrade enables, tell the user to send a
        follow-up message after it completes, and continue useful preparation
        with the tools currently available. Check the new tools when the user
        returns; approval alone is not proof that provisioning succeeded.

        Args:
            reason: A short, concrete explanation of why a real workspace is
                needed (e.g. "need to run pytest", "clone and build the repo").

        Returns:
            Whether the request was shown to the user, and if not, why.
        """
        if not context.thread_id:
            # Worker Job (no thread): the pre-existing sandbox freeze,
            # unchanged. No availability check.
            context.request_freeze(
                {
                    "freeze_type": "workspace_upgrade_required",
                    "target_tier": "sandbox",
                    "reason": reason or "The task needs a real workspace (shell/git).",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            logger.info("request_workspace_upgrade requested: reason=%r", reason)
            return (
                "Recorded your request for a sandbox workspace — a human will see "
                "it and decide. The request has not started a workspace or added "
                "tools. Explain what it enables and continue useful preparation. "
                "After an approved upgrade completes, the user can send a "
                "follow-up message to continue the task; check the newly available "
                "tools then. Existing files carry over during the upgrade."
            )
        unavailable = await _vm_upgrade_unavailable_reason(context)
        if unavailable is _CHECK_FAILED:
            logger.info("request_workspace_upgrade not offered: check failed")
            return (
                "SRW couldn't check whether an upgrade is available right now, "
                "so nothing was offered to the user. Tell them the task needs a "
                "shell; they can try /upgrade-workspace vm themselves, or start "
                "a new Session with a Container workspace. Meanwhile continue "
                "with the tools you have."
            )
        if unavailable is not None:
            logger.info("request_workspace_upgrade not offered: %s", unavailable)
            return (
                f"No workspace upgrade is available for this Session: {unavailable} "
                "Nothing was offered to the user. Tell them the task needs a "
                "shell, and that they can start a new Session with a Container "
                "workspace; meanwhile continue with the tools you have."
            )
        context.request_freeze(
            {
                "freeze_type": "workspace_upgrade_required",
                "target_tier": "vm",
                "reason": reason or "The task needs a real workspace (shell/git).",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
        logger.info("request_workspace_upgrade requested: reason=%r", reason)
        return (
            "Recorded your request for a VM workspace — a human will see it "
            "and decide. The request has not started a workspace or added "
            "tools. Explain what it enables and continue useful preparation. "
            "After an approved upgrade completes, the user can send a "
            "follow-up message to continue the task; check the newly available "
            "tools then. Existing files carry over during the upgrade."
        )

    return [request_workspace_upgrade]
