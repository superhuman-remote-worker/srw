"""U2 WP4 — the worker's prepared request layout is pinned, not moved.

Acceptance for "cache prefix = system + tool schemas + history": every request
the execute node (src/graph.py) sends is laid out as

    [0]   SystemMessage — the ONE phase-agnostic system prompt
    [1..] summary SystemMessages ("[Summary of prior work]") from state, in
          state order; any other SystemMessage in state is never sent
    [..]  history — every non-System message of state, in state order,
          including the protected phase block delivered once per concrete
          phase (WP1), which is history, not tail
    tail  the transient block, rebuilt every turn and anchored after the last
          Human/Tool message: memory pair -> knowledge pair -> citation-feedback
          pair -> supervisor-guidance pair -> active-subagent status

The todo list is never part of the tail (append-only context injection, D17):
it is history, carried by the todo tool results and the phase-start messages,
so the execute node never renders it into a request itself.

"Prefix" is everything before the tail. Turn N+1's prefix is turn N's prefix
followed by turn N's new state messages, byte for byte, so provider prompt
caches reuse it and only the tail is re-billed. Nothing in the tail reaches
state.

WP3's corollary is pinned here as well: there is ONE tool
schema per job (``phase_tool_schemas["strategic"] is
phase_tool_schemas["tactical"]``) and the bound description set does not
change between turns — the "tool schemas" half of the cache-prefix claim.

Harness modelled on tests/test_memory_cutover.py::TestWorkerExecuteWiring: a
real AgentConfig, WorkspaceManager, TodoManager and ToolContext; a fake LLM
capturing every request; a recording MemoryManager seam with a fixed payload;
a real-typed fake ContextManager (the node does arithmetic on its values).
"""

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    RemoveMessage,
    SystemMessage,
    ToolMessage,
    message_to_dict,
)

import agent.api.dual_app as dual_app
from shared.runtime.citation_engine.models import Citation, VerificationStatus
from agent.core.citation_feedback_injection import (
    CITATION_FEEDBACK_TOOL_CALL_ID_PREFIX,
    format_failed_citations,
    is_citation_feedback_injection_message,
)
from agent.core.guidance_injection import (
    GUIDANCE_TOOL_CALL_ID_PREFIX,
    is_guidance_injection_message,
)
from agent.core.knowledge_injection import (
    create_knowledge_injection_messages,
    is_knowledge_injection_message,
)
from shared.runtime.core.loader import InstructionFileEntry, load_agent_config
from agent.core.memory_injection import (
    create_memory_injection_messages,
    is_memory_injection_message,
)
from shared.runtime.core.message_markers import (
    PERSIST_ROLE_EVENT,
    PERSIST_ROLE_KEY,
    is_protected_message,
    protected_phase_key,
)
from agent.core.workspace import WorkspaceManager
from shared.runtime.core.workspace_injection import (
    TODOS_INJECTION_CONTENT_PREFIX,
    is_workspace_injection_message,
)
from agent.graph import create_execute_node
from agent.managers import TodoManager
from agent.services.memory import AssembleStats, InjectionBlock, MemoryPayload
from agent.tools.context import ToolContext
from tests._fs_backend import FilesystemTestBackend

REPO_ROOT = Path(__file__).parent.parent
WORKER_CONFIG_PATH = str(REPO_ROOT / "config" / "worker_base.yaml")

JOB_ID = "layout-job-1"
SYSTEM_PROMPT = "ONE-SYS"
PHASE_SKILL_MD = (
    "---\n"
    "name: tactical-phase\n"
    "description: Tactical phase instructions\n"
    "catalog: hidden\n"
    "---\n\n"
    "# Tactical phase\n\n"
    "UNIQUE TACTICAL PHASE BODY " * 20
)
TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read a workspace file",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "job_complete",
            "description": "[strategic-phase tool] Declare the job complete",
            "parameters": {"type": "object", "properties": {}},
        },
    },
]

_EXECUTE_PATCHES = {
    "agent.graph.get_phase_system_prompt": SYSTEM_PROMPT,
}


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


class RecordingManager:
    """MemoryManager seam stub: fixed payload, records every call."""

    def __init__(self, payload: MemoryPayload) -> None:
        self.payload = payload
        self.assemble_requests: List[Any] = []
        self.captures: List[Any] = []

    async def assemble(self, req: Any) -> MemoryPayload:
        self.assemble_requests.append(req)
        return self.payload

    async def capture(self, event: Any) -> None:
        self.captures.append(event)

    def capture_nowait(self, event: Any) -> None:
        self.captures.append(event)


def make_payload(memory_text: str = "MEMBLOCK", knowledge_text: str = "KBBLOCK"):
    """A payload with the production injection pairs (memory, then knowledge)."""
    blocks = [
        InjectionBlock(
            kind="memory",
            content=memory_text,
            messages=list(create_memory_injection_messages(memory_text)),
            token_count=26,
            items=[{"record_id": "m1", "token_count": 26}],
        ),
        InjectionBlock(
            kind="knowledge",
            content=knowledge_text,
            messages=list(create_knowledge_injection_messages(knowledge_text)),
            token_count=0,
            items=[{"record_id": "k1", "token_count": 0}],
        ),
    ]
    return MemoryPayload(blocks=blocks, stats=AssembleStats(blocks=len(blocks)))


class FakeContextMgr:
    """Real-typed context manager fake with a char-based token counter.

    Records the thresholds in force while ``ensure_within_limits`` runs —
    that is where the execute node's lowered (overhead-adjusted) thresholds
    are observable.
    """

    ORIGINAL_THRESHOLD = 100_000

    def __init__(self) -> None:
        self.config = SimpleNamespace(
            compaction_threshold_tokens=self.ORIGINAL_THRESHOLD,
            summarization_threshold_tokens=self.ORIGINAL_THRESHOLD,
            keep_recent_messages=10,
        )
        self._state = SimpleNamespace(summaries=[])
        self.phase_key: Optional[str] = None
        self.thresholds_seen: List[tuple] = []

    def set_current_phase(self, phase: str, phase_key: Optional[str] = None) -> None:
        self.phase_key = phase_key

    def should_summarize(self, messages) -> bool:
        return False

    def get_token_count(self, messages: List[Any]) -> int:
        return sum(len(str(getattr(m, "content", ""))) // 4 for m in messages)

    async def ensure_within_limits(self, messages, *args, **kwargs):
        self.thresholds_seen.append(
            (
                self.config.compaction_threshold_tokens,
                self.config.summarization_threshold_tokens,
            )
        )
        return messages


class CapturingLLM:
    """Bound-LLM stand-in: captures each request, answers from a script."""

    def __init__(self, responses: Optional[List[AIMessage]] = None) -> None:
        self.kwargs = {"tools": copy.deepcopy(TOOL_SCHEMAS)}
        self.requests: List[List[BaseMessage]] = []
        self._responses = list(responses or [])

    async def ainvoke(self, prepared, **kwargs):
        self.requests.append(list(prepared))
        if self._responses:
            return self._responses.pop(0)
        return AIMessage(content="done")


@pytest.fixture
def env(tmp_path):
    ws = WorkspaceManager(
        job_id=JOB_ID, base_path=tmp_path, backend=FilesystemTestBackend(tmp_path)
    )
    ws.initialize()
    config = load_agent_config(WORKER_CONFIG_PATH)
    todo = TodoManager(ws)
    todo.add("Do the task")
    ctx = ToolContext(workspace_manager=ws)
    # Legacy-store spy: the manager path never touches it; its presence adds
    # the memory budget to the overhead estimate (as on a real worker).
    ctx.recall_store = AsyncMock()
    ctx.knowledge_store = None
    return {
        "config": config,
        "workspace": ws,
        "todo": todo,
        "ctx": ctx,
        "context": FakeContextMgr(),
        "service": RecordingManager(make_payload()),
    }


@pytest.fixture
def guidance_inbox():
    """Isolated dual-app guidance inbox (the heartbeat-fed steer lane)."""
    saved = dict(dual_app._guidance_inbox)
    dual_app._guidance_inbox.clear()
    try:
        yield dual_app._guidance_inbox
    finally:
        dual_app._guidance_inbox.clear()
        dual_app._guidance_inbox.update(saved)


def _bind_tactical_phase_skill(env) -> InstructionFileEntry:
    """Bind the tactical-phase skill at phase_start:tactical and write it."""
    entry = InstructionFileEntry(
        trigger="phase_start:tactical", skill="tactical-phase", enforce=False
    )
    env["config"].instruction_files = [entry]
    env["ctx"]._instruction_files = [entry]
    env["workspace"].write_file(entry.path, PHASE_SKILL_MD)
    return entry


def _make_node(env, llm, **overrides):
    kwargs = dict(
        llm_with_tools=llm,
        todo_manager=env["todo"],
        memory_manager=MagicMock(),  # vestigial workspace.md manager
        workspace_manager=env["workspace"],
        config=env["config"],
        context_mgr=env["context"],
        retry_manager=MagicMock(),
        auxiliary_llm=MagicMock(),
        summarization_prompt="summarize",
        memory_extraction_prompt="extract",
        memory_assembler_prompt="assemble",
        tool_context=env["ctx"],
        tool_names=None,
        memory_service=env["service"],
    )
    kwargs.update(overrides)
    return create_execute_node(**kwargs)


def _history() -> List[BaseMessage]:
    """A checkpointed mid-job history: one summary, one stale SystemMessage,
    a completed tool round trip, and the phase-transition notice."""
    return [
        SystemMessage(content="[Summary of prior work]\nPhase 0/1: read the brief."),
        SystemMessage(content="STALE SYSTEM MESSAGE from an older checkpoint"),
        HumanMessage(content="Start the task."),
        AIMessage(
            content="",
            tool_calls=[{"name": "read_file", "args": {"path": "a.md"}, "id": "c1"}],
        ),
        ToolMessage(content="contents of a.md", tool_call_id="c1", name="read_file"),
        HumanMessage(content="[PHASE_TRANSITION] Entering tactical phase 2."),
    ]


def _state(messages: Optional[List[BaseMessage]] = None, **extra) -> dict:
    state = {
        "job_id": JOB_ID,
        "iteration": 0,
        "messages": messages if messages is not None else _history(),
        "is_strategic_phase": False,
        "phase_number": 2,
        "turn_count": 0,
        "metadata": {},
    }
    state.update(extra)
    return state


def _apply_turn(state: dict, result: dict) -> None:
    """What the graph does between turns: the add_messages reducer (append
    everything that is not a RemoveMessage) plus the scalar updates."""
    state["messages"] = state["messages"] + [
        m for m in result["messages"] if not isinstance(m, RemoveMessage)
    ]
    state["iteration"] = result["iteration"]
    state["turn_count"] = result["turn_count"]
    if "phase_instruction_injections" in result:
        state["phase_instruction_injections"] = result["phase_instruction_injections"]


def _patches():
    return [
        patch(target, return_value=value) for target, value in _EXECUTE_PATCHES.items()
    ]


async def _run(node, state):
    """One execute turn under the harness patches (no archiver)."""
    with patch("agent.graph.get_archiver", return_value=None):
        patches = _patches()
        for p in patches:
            p.start()
        try:
            return await node(state)
        finally:
            for p in patches:
                p.stop()


# ---------------------------------------------------------------------------
# Layout helpers — the vocabulary the assertions are written in
# ---------------------------------------------------------------------------


def _is_todos(m: BaseMessage) -> bool:
    """The legacy per-turn ``<active_tasks>`` message, no longer produced."""
    return isinstance(m, HumanMessage) and str(m.content).startswith(
        TODOS_INJECTION_CONTENT_PREFIX
    )


def _is_transient(m: BaseMessage) -> bool:
    """Every kind the execute node splices into the tail."""
    return (
        is_workspace_injection_message(m)  # todos, memory, knowledge, citation
        or is_memory_injection_message(m)
        or is_knowledge_injection_message(m)
        or is_citation_feedback_injection_message(m)
        or is_guidance_injection_message(m)  # not covered by the predicate above
    )


def _kind(m: BaseMessage) -> str:
    if is_memory_injection_message(m):
        return "memory"
    if is_knowledge_injection_message(m):
        return "knowledge"
    if is_citation_feedback_injection_message(m):
        return "citation"
    if is_guidance_injection_message(m):
        return "guidance"
    if _is_todos(m):
        return "todos"
    if is_protected_message(m):
        return "phase_block"
    if isinstance(m, SystemMessage):
        return "summary" if "[Summary of prior work]" in m.content else "system"
    return type(m).__name__


def _tail_start(request: List[BaseMessage]) -> int:
    """Index of the first tail message: walk back over transient messages."""
    i = len(request)
    while i > 0 and _is_transient(request[i - 1]):
        i -= 1
    return i


def _split(request: List[BaseMessage]):
    cut = _tail_start(request)
    return request[:cut], request[cut:]


def _wire(m: BaseMessage) -> bytes:
    """Provider-facing bytes of one message (what a prompt cache hashes)."""
    return json.dumps(message_to_dict(m), sort_keys=True, default=str).encode("utf-8")


def _wire_all(messages: List[BaseMessage]) -> List[bytes]:
    return [_wire(m) for m in messages]


def _failed_citation(cid: int = 7) -> Citation:
    return Citation(
        id=cid,
        claim="The sky is green at noon",
        quote_context="The sky was a deep blue at noon.",
        source_id=1,
        locator={"page": 3},
        created_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
        verbatim_quote="the sky is green",
        verification_status=VerificationStatus.FAILED,
        verification_notes="quote not found in source",
        similarity_score=0.31,
    )


# ---------------------------------------------------------------------------
# The layout
# ---------------------------------------------------------------------------


class TestPreparedLayout:
    @pytest.mark.asyncio
    async def test_live_child_status_is_transient_and_last(self, env):
        _bind_tactical_phase_skill(env)
        block = (
            "<active_subagents>\n"
            "- probe-ab12: running (background)\n"
            "Reports push automatically; do not poll.\n"
            "</active_subagents>"
        )
        env["ctx"].subagent_runtime = SimpleNamespace(
            active_subagents_block=MagicMock(return_value=block)
        )
        llm = CapturingLLM()
        node = _make_node(env, llm)

        result = await _run(node, _state())

        (request,) = llm.requests
        active = next(
            m for m in request if str(m.content).startswith("<active_subagents>")
        )
        assert request[-1] is active
        assert not any(_is_todos(m) for m in request)
        assert active.additional_kwargs[PERSIST_ROLE_KEY] == PERSIST_ROLE_EVENT
        assert "do not poll" in active.content
        assert not any(
            "<active_subagents>" in str(m.content) for m in result["messages"]
        )
        env["ctx"].subagent_runtime.active_subagents_block.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_layout_is_system_summaries_history_block_then_tail(self, env):
        """One request, read top to bottom: the one system prompt, the
        summaries, the history in state order, the phase block as the last
        history message on its delivery turn, then the transient tail. There
        is no todo message: the list is history (D17). Nothing from the tail
        is returned to state."""
        _bind_tactical_phase_skill(env)
        # A ReAct turn: the reply carries a tool call (the graph goes to the
        # tools node next), so no todo-reminder nudge is appended to state.
        llm = CapturingLLM(
            responses=[
                AIMessage(
                    content="",
                    tool_calls=[
                        {"name": "read_file", "args": {"path": "b.md"}, "id": "c2"}
                    ],
                )
            ]
        )
        node = _make_node(env, llm)
        state = _state()

        result = await _run(node, state)

        (request,) = llm.requests
        assert [_kind(m) for m in request] == [
            "system",
            "summary",
            "HumanMessage",
            "AIMessage",
            "ToolMessage",
            "HumanMessage",
            "phase_block",
            "memory",
            "memory",
            "knowledge",
            "knowledge",
        ]

        # [0] is the ONE phase-agnostic prompt, rebuilt per turn;
        # a SystemMessage that sits in state is never forwarded unless it is a
        # summary — the stale one is dropped.
        assert isinstance(request[0], SystemMessage)
        assert request[0].content == SYSTEM_PROMPT
        assert [m for m in request if isinstance(m, SystemMessage)] == [
            request[0],
            state["messages"][0],
        ]
        assert not any("STALE SYSTEM MESSAGE" in str(m.content) for m in request)

        # Summaries come from state, right after the system prompt.
        assert request[1] is state["messages"][0]

        # History = every non-System state message, in state order, then the
        # block delivered this turn (it is appended to the working history,
        # so it is the newest history message, not part of the tail).
        prefix, tail = _split(request)
        expected_history = [
            m for m in state["messages"] if not isinstance(m, SystemMessage)
        ]
        assert _wire_all(prefix[2:-1]) == _wire_all(expected_history)
        block = prefix[-1]
        assert is_protected_message(block)
        assert protected_phase_key(block) == "2:tactical"
        assert "UNIQUE TACTICAL PHASE BODY" in block.content
        assert block is result["messages"][0]  # delivered into state this turn

        # The tail: memory pair, knowledge pair. Synthetic pairs are
        # well-formed (AI tool_call + ToolMessage with the same id).
        assert [_kind(m) for m in tail] == [
            "memory",
            "memory",
            "knowledge",
            "knowledge",
        ]
        for ai, tool in (tail[0:2], tail[2:4]):
            assert isinstance(ai, AIMessage) and isinstance(tool, ToolMessage)
            assert ai.tool_calls[0]["id"] == tool.tool_call_id
        # No todo message, and the node never renders the list itself: it is
        # in the history only where a todo tool or a phase start put it.
        assert not any(_is_todos(m) for m in request)
        assert not any("Do the task" in str(m.content) for m in request)

        # Transients never reach state; the delivery turn returns block + reply
        # (a no-tool-call reply with pending todos would add the reminder
        # nudge after them — a state message, not a tail message).
        assert [_kind(m) for m in result["messages"]] == ["phase_block", "AIMessage"]
        assert result["messages"][1].tool_calls[0]["id"] == "c2"
        assert not any(_is_transient(m) for m in result["messages"])
        assert env["context"].phase_key == "2:tactical"

    @pytest.mark.asyncio
    async def test_history_prefix_identical_across_two_turns(self, env):
        """Turn N+1's prefix is turn N's prefix followed by turn N's new state
        messages — byte for byte on the wire — and the tail is rebuilt after
        them. The request grows only by what state grew by."""
        _bind_tactical_phase_skill(env)
        llm = CapturingLLM(
            responses=[
                AIMessage(
                    content="",
                    tool_calls=[
                        {"name": "read_file", "args": {"path": "b.md"}, "id": "c2"}
                    ],
                ),
                AIMessage(content="done"),
            ]
        )
        node = _make_node(env, llm)
        state = _state()

        first = await _run(node, state)
        _apply_turn(state, first)
        # The tools node's contribution between the two execute turns.
        tool_reply = ToolMessage(
            content="contents of b.md", tool_call_id="c2", name="read_file"
        )
        state["messages"].append(tool_reply)
        second = await _run(node, state)

        req1, req2 = llm.requests
        prefix1, tail1 = _split(req1)
        prefix2, tail2 = _split(req2)

        # Byte-identical prefix: req2 starts with req1's whole prefix ...
        assert _wire_all(prefix2[: len(prefix1)]) == _wire_all(prefix1)
        # ... continued by exactly the messages state gained between the turns
        # (the block delivered on turn 1 is inside prefix1 already).
        new_messages = [m for m in first["messages"] if not is_protected_message(m)]
        new_messages.append(tool_reply)
        assert _wire_all(prefix2[len(prefix1) :]) == _wire_all(new_messages)
        assert len(req2) - len(req1) == len(new_messages)

        # Same tail shape, rebuilt after the new messages; no todo message.
        assert [_kind(m) for m in tail1] == [_kind(m) for m in tail2]
        assert not any(_is_todos(m) for m in req1 + req2)
        # Nothing changed between the turns, so the tail is byte-identical
        # too (deterministic injection ids) — but it sits at a new offset.
        assert _wire_all(tail1) == _wire_all(tail2)

        # The block was delivered once and is history on both turns.
        assert sum(is_protected_message(m) for m in req1) == 1
        assert sum(is_protected_message(m) for m in req2) == 1
        assert "phase_instruction_injections" not in second
        assert not any(is_protected_message(m) for m in second["messages"])
        assert not any(_is_transient(m) for m in second["messages"])

    @pytest.mark.asyncio
    async def test_supervisor_guidance_is_the_last_tail_pair(self, env, guidance_inbox):
        """The guidance pair is the last synthetic pair of the tail — after
        memory and knowledge — so mid-run steering is the freshest synthetic
        context. It is transient (never in state) and acked after the turn."""
        _bind_tactical_phase_skill(env)
        guidance_inbox[JOB_ID] = [
            {"id": "g1", "text": "stop retrying X", "source": "officer"},
            {"id": "g2", "text": "read file Z", "source": "officer"},
        ]
        llm = CapturingLLM()
        node = _make_node(env, llm)
        ack = MagicMock()

        with patch.object(dual_app, "ack_guidance", ack):
            result = await _run(node, _state())

        (request,) = llm.requests
        prefix, tail = _split(request)
        assert [_kind(m) for m in tail] == [
            "memory",
            "memory",
            "knowledge",
            "knowledge",
            "guidance",
            "guidance",
        ]
        guid_ai, guid_tool = tail[-2], tail[-1]
        assert isinstance(guid_ai, AIMessage) and isinstance(guid_tool, ToolMessage)
        assert guid_ai.tool_calls[0]["name"] == "supervisor_guidance"
        assert guid_ai.tool_calls[0]["id"].startswith(GUIDANCE_TOOL_CALL_ID_PREFIX)
        assert guid_tool.tool_call_id == guid_ai.tool_calls[0]["id"]
        assert guid_tool.content.startswith("[SUPERVISOR GUIDANCE]")
        assert "stop retrying X" in guid_tool.content
        assert "read file Z" in guid_tool.content
        assert not any(_is_todos(m) for m in request)
        # One block, after the whole history (the phase block included).
        assert sum("[SUPERVISOR GUIDANCE]" in str(m.content) for m in request) == 1
        assert request.index(guid_ai) > request.index(prefix[-1])
        assert is_protected_message(prefix[-1])

        assert not any(is_guidance_injection_message(m) for m in result["messages"])
        ack.assert_called_once_with(
            JOB_ID, guidance_ids=["g1", "g2"], reply_threads=None
        )

    @pytest.mark.asyncio
    async def test_citation_feedback_pair_in_tail(self, env, guidance_inbox):
        """Failed-citation feedback is a transient pair in the tail: after the
        memory and knowledge pairs, before the guidance pair — re-derived from
        the engine each turn, never written to state."""
        _bind_tactical_phase_skill(env)
        citation = _failed_citation()
        engine = SimpleNamespace(list_citations=AsyncMock(return_value=[citation]))
        env["ctx"].citation_engine = engine
        guidance_inbox[JOB_ID] = [
            {"id": "g1", "text": "fix the citation", "source": "officer"}
        ]
        llm = CapturingLLM(responses=[AIMessage(content="ok"), AIMessage(content="ok")])
        node = _make_node(env, llm)
        state = _state()

        with patch.object(dual_app, "ack_guidance", MagicMock()):
            first = await _run(node, state)
            _apply_turn(state, first)
            second = await _run(node, state)

        req1, req2 = llm.requests
        prefix, tail = _split(req1)
        assert [_kind(m) for m in tail] == [
            "memory",
            "memory",
            "knowledge",
            "knowledge",
            "citation",
            "citation",
            "guidance",
            "guidance",
        ]
        cit_ai, cit_tool = tail[4], tail[5]
        assert isinstance(cit_ai, AIMessage) and isinstance(cit_tool, ToolMessage)
        assert cit_ai.tool_calls[0]["name"] == "check_citation_verification"
        assert cit_ai.tool_calls[0]["id"].startswith(
            CITATION_FEEDBACK_TOOL_CALL_ID_PREFIX
        )
        assert cit_tool.tool_call_id == cit_ai.tool_calls[0]["id"]
        assert cit_tool.content == format_failed_citations([citation])
        assert "Automatic verification FAILED" in cit_tool.content
        assert (
            "[7]" in cit_tool.content
            and "quote not found in source" in cit_tool.content
        )
        engine.list_citations.assert_awaited_with(verification_status="failed")

        # After the whole history (phase block included); before the guidance.
        assert req1.index(cit_ai) > req1.index(prefix[-1])
        assert is_protected_message(prefix[-1])
        assert is_guidance_injection_message(req1[-1])

        # Transient: re-derived on the next turn, absent from state both turns.
        assert sum(is_citation_feedback_injection_message(m) for m in req2) == 2
        for result in (first, second):
            assert not any(
                is_citation_feedback_injection_message(m) for m in result["messages"]
            )


# ---------------------------------------------------------------------------
# One tool schema per job (WP3 corollary)
# ---------------------------------------------------------------------------


class TestOneSchemaPerJob:
    @pytest.mark.asyncio
    async def test_archived_tool_schemas_are_one_object_across_phases_and_turns(
        self, env
    ):
        """Observed through the archiver: a strategic turn and two tactical
        turns hand the SAME schema list to ``archive()`` — the bound
        description set is fixed at node creation and unchanged by running
        turns, so the "tool schemas" part of the cache prefix is stable."""
        _bind_tactical_phase_skill(env)
        llm = CapturingLLM()
        snapshot = copy.deepcopy(llm.kwargs["tools"])
        node = _make_node(env, llm)
        auditor = MagicMock()

        state = _state(is_strategic_phase=True, phase_number=1)
        with patch("agent.graph.get_archiver", return_value=auditor):
            patches = _patches()
            for p in patches:
                p.start()
            try:
                first = await node(state)
                _apply_turn(state, first)
                state["is_strategic_phase"] = False
                state["phase_number"] = 2
                second = await node(state)
                _apply_turn(state, second)
                await node(state)
            finally:
                for p in patches:
                    p.stop()

        archived = [c.kwargs for c in auditor.archive.call_args_list]
        assert [c["phase"] for c in archived] == ["strategic", "tactical", "tactical"]
        schemas = [c["tool_schemas"] for c in archived]
        assert all(s is schemas[0] for s in schemas)
        assert schemas[0] is llm.kwargs["tools"]
        assert schemas[0] == snapshot  # no per-turn or per-phase edits
        # Same binding answered every turn (one llm_with_tools per job).
        assert len(llm.requests) == 3


# ---------------------------------------------------------------------------
# Overhead accounting
# ---------------------------------------------------------------------------


class TestInjectionOverheadAccounting:
    @pytest.mark.asyncio
    async def test_delivery_turn_adds_the_phase_block_once(self, env):
        """The compaction thresholds are lowered by the request overhead that
        `messages` does not carry — system prompt, memory budget —
        plus, on the delivery turn only, the phase block just appended (the
        provider-anchored trigger predates it). The next turn drops the term;
        the originals are restored after every turn."""
        _bind_tactical_phase_skill(env)
        ctx = env["context"]
        llm = CapturingLLM(
            responses=[
                AIMessage(
                    content="",
                    tool_calls=[
                        {"name": "read_file", "args": {"path": "b.md"}, "id": "c2"}
                    ],
                ),
                AIMessage(content="done"),
            ]
        )
        node = _make_node(env, llm)
        state = _state()

        first = await _run(node, state)
        _apply_turn(state, first)
        state["messages"].append(
            ToolMessage(content="contents of b.md", tool_call_id="c2", name="read_file")
        )
        await _run(node, state)

        block = first["messages"][0]
        assert is_protected_message(block)
        block_tokens = ctx.get_token_count([block])
        assert block_tokens > 0

        common = (
            ctx.get_token_count([SystemMessage(content=SYSTEM_PROMPT)])
            + env["config"].memory.budget_tokens  # recall_store present
        )
        original = FakeContextMgr.ORIGINAL_THRESHOLD
        (compaction_1, summarization_1), (compaction_2, summarization_2) = (
            ctx.thresholds_seen
        )
        assert compaction_1 == original - common - block_tokens
        assert summarization_1 == original - common - block_tokens
        assert compaction_2 == original - common
        assert summarization_2 == original - common
        assert compaction_2 - compaction_1 == block_tokens

        # Restored after each turn.
        assert ctx.config.compaction_threshold_tokens == original
        assert ctx.config.summarization_threshold_tokens == original

    @pytest.mark.asyncio
    async def test_no_delivery_no_block_term(self, env):
        """A phase without a phase_start binding never adds a block term."""
        ctx = env["context"]
        llm = CapturingLLM()
        node = _make_node(env, llm)

        await _run(node, _state())

        common = (
            ctx.get_token_count([SystemMessage(content=SYSTEM_PROMPT)])
            + env["config"].memory.budget_tokens
        )
        assert ctx.thresholds_seen == [
            (
                FakeContextMgr.ORIGINAL_THRESHOLD - common,
                FakeContextMgr.ORIGINAL_THRESHOLD - common,
            )
        ]


# ---------------------------------------------------------------------------
# The todo list after compaction (D19)
# ---------------------------------------------------------------------------


class CompactingContextMgr(FakeContextMgr):
    """Every ``ensure_within_limits`` call compacts to summary + the newest
    message, seating whatever ``restate_after_summary`` returns in between —
    the contract of ``ContextManager.summarize_and_compact``. ``overflow_once``
    makes the first full-request count exceed every limit (the Layer-1 safety
    rebuild)."""

    def __init__(self, *, overflow_once: bool = False) -> None:
        super().__init__()
        self.hooks: List[Any] = []
        self._overflow = overflow_once

    def get_token_count(self, messages: List[Any]) -> int:
        if self._overflow and len(messages) > 1:
            self._overflow = False
            return 10**9
        return super().get_token_count(messages)

    async def ensure_within_limits(
        self, messages, *args, restate_after_summary=None, **kwargs
    ):
        await super().ensure_within_limits(messages, *args, **kwargs)
        self.hooks.append(restate_after_summary)
        summary = SystemMessage(content="[Summary of prior work]\nEarlier work.")
        kept = list(messages[-1:])
        restated = (
            list(restate_after_summary([summary, *kept]))
            if restate_after_summary
            else []
        )
        markers = [RemoveMessage(id=m.id) for m in messages if m.id]
        return markers + [summary, *restated, *kept]


class OverflowOnceLLM(CapturingLLM):
    """Raise ContextOverflowError once: the Layer-0 emergency compaction."""

    def __init__(self) -> None:
        super().__init__()
        self.raised = False

    async def ainvoke(self, prepared, **kwargs):
        from shared.runtime.llm.exceptions import ContextOverflowError

        if not self.raised:
            self.raised = True
            raise ContextOverflowError(token_count=10**9, limit=128_000)
        return await super().ainvoke(prepared, **kwargs)


class TestTodoListRestatedAfterCompaction:
    @pytest.mark.parametrize("path", ["threshold", "safety", "emergency"])
    @pytest.mark.asyncio
    async def test_every_execute_compaction_restates_the_list_once(self, env, path):
        """The threshold compaction, the Layer-1 safety rebuild and the Layer-0
        emergency rebuild all hand the todo-list hook to the compaction; the
        restatement lands right after the summary in the request and is
        returned to state, so it is history from then on (not a tail)."""
        ctx = CompactingContextMgr(overflow_once=path == "safety")
        env["context"] = ctx
        llm = OverflowOnceLLM() if path == "emergency" else CapturingLLM()
        node = _make_node(env, llm)
        history = [
            m.model_copy(update={"id": f"m{i}"}) for i, m in enumerate(_history())
        ]

        result = await _run(node, _state(history))

        expected_calls = 1 if path == "threshold" else 2
        assert len(ctx.hooks) == expected_calls
        assert all(hook is not None for hook in ctx.hooks)
        request = llm.requests[-1]
        rendering = env["todo"].format_for_injection()
        restated = [m for m in request if rendering in str(m.content)]
        assert len(restated) == 1
        assert isinstance(restated[0], HumanMessage)
        assert [_kind(m) for m in request[:4]] == [
            "system",
            "summary",
            "HumanMessage",  # the restated list
            "HumanMessage",  # the kept window
        ]
        assert request[2] is restated[0]
        state_messages = [
            m for m in result["messages"] if not isinstance(m, RemoveMessage)
        ]
        assert any(m is restated[0] for m in state_messages)
        assert not any(_is_todos(m) for m in request)
