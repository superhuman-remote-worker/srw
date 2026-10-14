"""Model-facing controls for durable background subagents (U4).

The tools stay deliberately thin.  The per-parent :class:`SubagentRuntime`
owns addressability, fencing, durable revival, stop synthesis and bounded
status rendering; this module only validates the public arguments and calls
that runtime.  Importing the runtime remains lazy through ``ensure_runtime``
so the tools registry does not close the registry -> delegation -> subagents
-> persistent-graph import cycle.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from agent.tools.context import ToolContext

from shared.tool_catalog.definitions import (
    CONTROL_PLANE_METADATA as CONTROL_PLANE_METADATA,
)


CONTROL_TOOL_NAMES = (
    "wait_agent",
    "message_agent",
    "stop_agent",
    "list_agents",
)


def create_control_plane_tools(context: ToolContext) -> List[Any]:
    """Create all four controls when delegation is enabled.

    Membership is filtered against the expert's explicit grant by
    :func:`agent.tools.delegation.create_delegation_tools`; returning the full
    enabled family here keeps each StructuredTool factory uncomplicated.
    """
    from langchain_core.tools import StructuredTool
    from pydantic import BaseModel, Field

    from agent.tools.delegation.delegate_agent import (
        _delegation_settings,
        ensure_runtime,
    )

    if _delegation_settings(context).get("enabled") is not True:
        return []

    class WaitAgentInput(BaseModel):
        handle: Optional[str] = Field(
            default=None,
            description=(
                "One background subagent handle. Omit to wait for the next "
                "update from any child."
            ),
        )
        timeout_s: float = Field(
            default=30.0,
            ge=10.0,
            le=3600.0,
            description=(
                "How long to wait, in seconds (10-3600). Use one meaningful "
                "wait only when blocked; never call in a polling loop."
            ),
        )

    class MessageAgentInput(BaseModel):
        handle: str = Field(description="The target subagent handle.")
        message: str = Field(
            min_length=1,
            description=(
                "A concise follow-up or correction for the child. Include "
                "enough context to act without reading the parent transcript. "
                "For a live voice subagent: what the voice should tell the "
                "caller, in plain words."
            ),
        )
        reply_to: Optional[str] = Field(
            default=None,
            description=(
                "Live voice subagents only: the delegation_id of the question "
                "this message answers (from wait_agent). Omit to steer the "
                "voice instead."
            ),
        )
        call_action: Optional[Literal["transfer"]] = Field(
            default=None,
            description=(
                "Live voice subagents only: 'transfer' hands the caller to a "
                "person (the line's transfer target) after the message is "
                "spoken."
            ),
        )

    class StopAgentInput(BaseModel):
        handle: str = Field(description="The target subagent handle.")
        grace_s: float = Field(
            default=30.0,
            ge=0.1,
            le=300.0,
            description=(
                "Seconds allowed for a tool-less partial synthesis before a "
                "hard stop (0.1-300)."
            ),
        )

    class ListAgentsInput(BaseModel):
        pass

    async def _wait_agent(
        handle: Optional[str] = None,
        timeout_s: float = 30.0,
    ) -> Dict[str, Any]:
        runtime = ensure_runtime(context)
        return await runtime.wait_agent(
            str(handle).strip() if handle and str(handle).strip() else None,
            float(timeout_s),
        )

    async def _message_agent(
        handle: str,
        message: str,
        reply_to: Optional[str] = None,
        call_action: Optional[str] = None,
    ) -> Dict[str, Any]:
        runtime = ensure_runtime(context)
        if reply_to or call_action:
            return await runtime.message_agent(
                str(handle).strip(),
                str(message),
                reply_to=str(reply_to).strip() if reply_to else None,
                call_action=call_action,
            )
        return await runtime.message_agent(str(handle).strip(), str(message))

    async def _stop_agent(
        handle: str,
        grace_s: float = 30.0,
    ) -> Dict[str, Any]:
        runtime = ensure_runtime(context)
        return await runtime.stop_agent(str(handle).strip(), float(grace_s))

    async def _list_agents() -> List[Dict[str, Any]]:
        runtime = ensure_runtime(context)
        return await runtime.list_agents()

    return [
        StructuredTool.from_function(
            coroutine=_wait_agent,
            name="wait_agent",
            description=CONTROL_PLANE_METADATA["wait_agent"]["description"],
            args_schema=WaitAgentInput,
        ),
        StructuredTool.from_function(
            coroutine=_message_agent,
            name="message_agent",
            description=CONTROL_PLANE_METADATA["message_agent"]["description"],
            args_schema=MessageAgentInput,
        ),
        StructuredTool.from_function(
            coroutine=_stop_agent,
            name="stop_agent",
            description=CONTROL_PLANE_METADATA["stop_agent"]["description"],
            args_schema=StopAgentInput,
        ),
        StructuredTool.from_function(
            coroutine=_list_agents,
            name="list_agents",
            description=CONTROL_PLANE_METADATA["list_agents"]["description"],
            args_schema=ListAgentsInput,
        ),
    ]


__all__ = [
    "CONTROL_PLANE_METADATA",
    "CONTROL_TOOL_NAMES",
    "create_control_plane_tools",
]
