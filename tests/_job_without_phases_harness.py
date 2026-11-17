"""A compiled worker graph driven by a scripted model, for whole-job tests.

``build_job`` builds the real ``build_phase_alternation_graph`` on a
filesystem workspace (``tests/_fs_backend.py``), real tools from the registry,
a ``TodoManager`` and an ``InMemorySaver``. The model is ``ScriptedLLM``: each
request is recorded, and each answer is the next item of the script. The
system prompt is patched to a fixed string, so these tests see only the texts
the graph itself composes (the scaffolds are tested elsewhere).

The configuration stands for a job without phases: its frozen config has
one predefined to-do and, by default, a tool list without the phase tools
(``DIRECT_TASK_TOOLS``); the worker reads nothing else to run it.
``PHASED_TOOLS`` adds the phase tools, for the same job planned in phases.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
from unittest.mock import patch

from langchain_core.messages import AIMessage, BaseMessage, SystemMessage
from langgraph.checkpoint.memory import InMemorySaver

from agent.core.bound_tools import binds_todo_tool
from agent.core.state import create_initial_state
from agent.core.workspace import WorkspaceManager
from agent.graph import build_phase_alternation_graph
from agent.managers import TodoManager
from agent.tools.context import ToolContext
from agent.tools.registry import load_tools
from shared.runtime.core.loader import (
    InstructionFileEntry,
    load_agent_config,
    resolve_config_path,
)
from tests._fs_backend import FilesystemTestBackend

JOB_ID = "job-without-phases"
PHASE_SKILL = "test-mode-skill"
SYSTEM_PROMPT = "FIXED SYSTEM PROMPT"
TASK_BRIEF = "# Task Brief\n\n## Description\n\nWrite hello.txt with a greeting."

TODO_TEXT = "Do the task in the brief. When it is done, call job_complete."
RESUME_TODO_TEXT = (
    "Read the feedback above and finish the task. When it is done, call job_complete."
)
ONE_TODO_YAML = f"todos:\n  - id: 1\n    content: {TODO_TEXT}\n"
ONE_RESUME_TODO_YAML = f"todos:\n  - id: 1\n    content: {RESUME_TODO_TEXT}\n"

#: The lifecycle surface of a job without phases: no phase tools.
DIRECT_TASK_TOOLS = ("job_complete", "write_file")
#: The phased procedure's tools on top, for a job that plans in phases.
PHASED_TOOLS = DIRECT_TASK_TOOLS + ("todo_complete", "next_phase_todos", "todo_list")

#: Words no request of a job without the phase tools may contain.
PHASE_WORDS = ("todo_complete", "next_phase_todos", "strategic mode")


class ScriptExhausted(AssertionError):
    """The graph asked the model for more answers than the test scripted."""


class ScriptedLLM:
    """Bound-model stand-in: records each request, answers from a script."""

    def __init__(self, script: Sequence[AIMessage]) -> None:
        self.kwargs: Dict[str, Any] = {"tools": []}
        self.requests: List[List[BaseMessage]] = []
        self._script = list(script)

    async def ainvoke(self, prepared, **kwargs):
        self.requests.append(list(prepared))
        if not self._script:
            raise ScriptExhausted(f"model call {len(self.requests)} was not scripted")
        return self._script.pop(0)


def tool_call(name: str, args: Dict[str, Any], call_id: str) -> AIMessage:
    """A model reply that calls one tool."""
    return AIMessage(
        content="", tool_calls=[{"name": name, "args": args, "id": call_id}]
    )


def job_complete_call(call_id: str = "call_done", **args: Any) -> AIMessage:
    """A model reply that calls ``job_complete``."""
    payload = {"summary": "Wrote hello.txt.", "deliverables": []}
    payload.update(args)
    return tool_call("job_complete", payload, call_id)


def write_file_call(call_id: str, path: str = "hello.txt") -> AIMessage:
    """A model reply that writes a file."""
    return tool_call(
        "write_file", {"path": path, "content": f"Hello from {call_id}.\n"}, call_id
    )


def plain_reply(text: str) -> AIMessage:
    """A model reply without a tool call."""
    return AIMessage(content=text)


def request_text(request: Sequence[BaseMessage]) -> str:
    """Every message of one request but the system prompt, as one string."""
    parts = []
    for message in request:
        if isinstance(message, SystemMessage):
            continue
        content = message.content
        parts.append(content if isinstance(content, str) else str(content))
    return "\n".join(parts)


@dataclass
class Job:
    """One test job: its graph, model, managers and checkpoint."""

    tmp_path: Path
    workspace: WorkspaceManager
    config: Any
    todo: TodoManager
    ctx: ToolContext
    tools: List[Any]
    saver: InMemorySaver
    llm: ScriptedLLM
    graph: Any = None
    llms: List[ScriptedLLM] = field(default_factory=list)

    @property
    def thread(self) -> Dict[str, Any]:
        return {"configurable": {"thread_id": JOB_ID}, "recursion_limit": 500}

    @property
    def requests(self) -> List[List[BaseMessage]]:
        """Every request of every graph built for this job, in order."""
        return [r for llm in self.llms for r in llm.requests]

    def rebuild(self, script: Sequence[AIMessage]) -> None:
        """A new graph and a new model on the same checkpoint and managers,
        as the next stateless batch builds them."""
        self.llm = ScriptedLLM(script)
        self.llms.append(self.llm)
        self.graph = _compile(self)

    async def start(self, **state_extra: Any) -> Dict[str, Any]:
        """Run the job from its first node."""
        state = create_initial_state(JOB_ID, str(self.tmp_path))
        state.update(state_extra)
        with patch("agent.graph.get_phase_system_prompt", return_value=SYSTEM_PROMPT):
            return await self.graph.ainvoke(state, self.thread)

    async def resume(self, update: Dict[str, Any]) -> Dict[str, Any]:
        """Write ``update`` as a fresh entry (what the agent does before a
        resume or a new batch), then run the graph on."""
        await self.graph.aupdate_state(self.thread, update, as_node="__start__")
        with patch("agent.graph.get_phase_system_prompt", return_value=SYSTEM_PROMPT):
            return await self.graph.ainvoke(None, self.thread)


def _compile(job: Job) -> Any:
    return build_phase_alternation_graph(
        llm_with_tools=job.llm,
        tools=job.tools,
        config=job.config,
        workspace=job.workspace,
        todo_manager=job.todo,
        checkpointer=job.saver,
        tool_context=job.ctx,
    )


def build_job(
    tmp_path: Path,
    script: Sequence[AIMessage],
    *,
    autonomy: str = "full",
    tool_names: Sequence[str] = DIRECT_TASK_TOOLS,
    limits: Optional[Dict[str, int]] = None,
    phase_skill_body: Optional[str] = None,
) -> Job:
    """A job with one predefined to-do (and one resume to-do).

    ``phase_skill_body``: bind one skill (``PHASE_SKILL``) at the strategic
    phase start and seed its file, as the agent's instruction deployment
    would; by default no phase-start skill is bound.
    """
    workspace = WorkspaceManager(
        job_id=JOB_ID, base_path=tmp_path, backend=FilesystemTestBackend(tmp_path)
    )
    workspace.initialize()
    workspace.write_file("task_brief.md", TASK_BRIEF)

    config = load_agent_config(*resolve_config_path("worker_base"))
    config.autonomy = autonomy
    # The procedure skills are not this job's; the one that replaces them is
    # bound only when a test asks for it.
    config.instruction_files = []
    if phase_skill_body is not None:
        entry = InstructionFileEntry(
            trigger="phase_start:strategic", skill=PHASE_SKILL, enforce=False
        )
        config.instruction_files = [entry]
        workspace.write_file(
            entry.path,
            f"---\nname: {PHASE_SKILL}\ndescription: Test skill\n---\n\n"
            f"{phase_skill_body}",
        )
    config.memory.manager_enabled = False
    for key, value in (limits or {}).items():
        setattr(config.limits, key, value)
    config.extra = {
        **config.extra,
        "_resolved_instructions": {
            **(config.extra.get("_resolved_instructions") or {}),
            "strategic_todos_initial": ONE_TODO_YAML,
            "strategic_todos_resume": ONE_RESUME_TODO_YAML,
        },
    }

    todo = TodoManager(workspace, model_name=config.llm.model)
    ctx = ToolContext(workspace_manager=workspace, todo_manager=todo)
    ctx._instruction_files = list(config.instruction_files)
    tools = load_tools(list(tool_names), ctx)
    loaded = [t.name for t in tools]
    # What UniversalAgent._setup_job_tools does once the tools are loaded.
    ctx._resolved_tool_names = loaded
    todo.todo_tool_bound = binds_todo_tool(loaded)

    llm = ScriptedLLM(script)
    job = Job(
        tmp_path=tmp_path,
        workspace=workspace,
        config=config,
        todo=todo,
        ctx=ctx,
        tools=tools,
        saver=InMemorySaver(),
        llm=llm,
        llms=[llm],
    )
    job.graph = _compile(job)
    return job


def batch_arm(iteration: int, iteration_cap: int) -> Dict[str, Any]:
    """The arming update of one stateless batch that ends after
    ``iteration_cap`` model calls (no wall-clock floor)."""
    return {
        "worker_batch_started_at": time.time() - 10,
        "worker_batch_start_iteration": iteration,
        "worker_batch_target_wall_seconds": 1e9,
        "worker_batch_min_wall_seconds": 0.0,
        "worker_batch_iteration_cap": iteration_cap,
    }


#: The clean-END re-entry a new batch writes along with its arm.
BATCH_REENTRY = {
    "freeze_data": None,
    "should_stop": False,
    "goal_achieved": False,
    "error": None,
    "client_report_id": None,
    "completion_report_payload": None,
}
