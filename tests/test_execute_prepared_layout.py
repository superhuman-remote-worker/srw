"""U2 WP4 — the worker's prepared request layout is pinned, not moved.

Acceptance for "cache prefix = system + tool schemas + history": every request
the execute node (src/agent/graph.py) sends is laid out as

    [0]   SystemMessage — the ONE phase-agnostic system prompt
    [1..] summary SystemMessages ("[Summary of prior work]") from state, in
          state order; any other SystemMessage in state is never sent
    [..]  history — every non-System message of state, in state order,
          including the protected phase block delivered once per concrete
          phase (WP1) and the context entries earlier requests appended

There is no per-request tail (append-only context injection). The context
that is new or changed since the history last showed it — memory, knowledge,
citation feedback, supervisor guidance, active subagents — is appended as
typed entries after the history, folded into the last history message (its
carrier). The entries reach state between the history and the response, so
turn N+1's request is turn N's request followed by turn N's new state
messages, byte for byte, and provider prompt caches reuse all of it
(``TestAppendOnlyLayout``).

The execute node never renders the todo list into a request (D17): it is
history, carried by the todo tool results and the phase-start messages, and
restated after a compaction (D19).

WP3's corollary is pinned here as well: there is ONE tool
schema per job (``phase_tool_schemas["strategic"] is
phase_tool_schemas["tactical"]``) and the bound description set does not
change between turns — the "tool schemas" half of the cache-prefix claim.

Harness modelled on tests/test_memory_cutover.py::TestWorkerExecuteWiring: a
real AgentConfig, WorkspaceManager, TodoManager and ToolContext; a fake LLM
capturing every request; a recording MemoryManager seam with a fixed payload;
a real-typed fake ContextManager (the node does arithmetic on its values).
"""

import asyncio
import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List, Optional
from unittest.mock import AsyncMock, MagicMock, call, patch
from uuid import UUID

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
from shared.runtime.core.context_entries import (
    FOLDED_KEY,
    UPDATED_ITEM_MARKER,
    digest,
    entry_kind,
    entry_meta,
    is_context_entry,
    is_legacy_injection,
    make_context_entry,
    memory_handle,
)
from shared.runtime.core.loader import (
    InstructionFileEntry,
    MemoryRetrievalConfig,
    load_agent_config,
)
from shared.runtime.services.knowledge_store import KnowledgeRecord
from shared.runtime.services.recall_store import MemoryRecord
from shared.runtime.core.message_markers import (
    is_protected_message,
    protected_phase_key,
)
from agent.core.workspace import WorkspaceManager
from shared.runtime.core.workspace_injection import TODOS_INJECTION_CONTENT_PREFIX
from agent.graph import create_execute_node
from agent.managers import TodoManager
from agent.services.memory import (
    AssembleRequest,
    AssembleStats,
    InjectionBlock,
    MemoryPayload,
    RetrievalResult,
)
from agent.services.memory.same_conversation import ConversationTurns
from agent.tools.context import ToolContext
from tests._fs_backend import FilesystemTestBackend
from tests._memory_fixtures import (
    BrokenScorer,
    GatedRetriever,
    make_async_manager,
    settle_retrieval,
)

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

SUBAGENTS_BLOCK = (
    "<active_subagents>\n- probe-ab12 (explorer): running\n"
    "Reports push automatically as evidence; do not poll.\n</active_subagents>"
)
MEMORIES = [
    MemoryRecord(id=UUID(int=1), content="Deploys go through Fleet.", token_count=6),
    MemoryRecord(id=UUID(int=2), content="Never touch production data.", token_count=7),
]
NOTE = KnowledgeRecord(
    note_id="release",
    project_id=UUID(int=9),
    title="Release",
    note_type="process",
    content="Releases are cut on Fridays.",
)
ENTRY_KINDS = ["memory", "knowledge", "citation", "guidance", "subagents"]


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


class RecordingManager:
    """MemoryManager seam stub: fixed payload, records every call.

    The node retrieves asynchronously (WP3): ``start_retrieval`` finishes at
    once here, so the request that started a retrieval takes its result in
    (the node starts early and takes in late).
    """

    def __init__(self, payload: MemoryPayload) -> None:
        self.payload = payload
        self.assemble_requests: List[Any] = []
        self.captures: List[Any] = []
        self._finished: Optional[RetrievalResult] = None

    def _next_payload(self, req: Any) -> MemoryPayload:
        self.assemble_requests.append(req)
        return self.payload

    def start_retrieval(self, req: Any) -> bool:
        self._finished = RetrievalResult(payload=self._next_payload(req))
        return True

    def take_retrieval(self, conversation: Any = None) -> Optional[RetrievalResult]:
        result, self._finished = self._finished, None
        return result

    def retrieval_stats(self) -> dict:
        return {"started": len(self.assemble_requests)}

    async def capture(self, event: Any) -> None:
        self.captures.append(event)

    def capture_nowait(self, event: Any) -> None:
        self.captures.append(event)


def make_payload(memories=MEMORIES, notes=(NOTE,)) -> MemoryPayload:
    """A memory block, then a knowledge block, with the store records the
    node plans its entries from (``InjectionBlock.records``)."""
    blocks = [
        InjectionBlock(
            kind="memory",
            token_count=26,
            items=[{"record_id": "m1", "token_count": 26}],
            records=list(memories),
        ),
        InjectionBlock(
            kind="knowledge",
            token_count=0,
            items=[{"record_id": "k1", "token_count": 0}],
            records=list(notes),
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
    # Present as on a real worker: the manager path never touches it, and it
    # adds no memory-budget term to the overhead estimate (§D row 6).
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
    if "delivered_guidance_ids" in result:
        state["delivered_guidance_ids"] = result["delivered_guidance_ids"]


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


def _kind(m: BaseMessage) -> str:
    if _is_todos(m):
        return "todos"
    if is_protected_message(m):
        return "phase_block"
    if isinstance(m, SystemMessage):
        return "summary" if "[Summary of prior work]" in m.content else "system"
    return type(m).__name__


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


def _all_sources(env, guidance_inbox) -> None:
    """Citation feedback, a live child and one guidance entry, on top of the
    memory and knowledge payload."""
    env["ctx"].citation_engine = SimpleNamespace(
        list_citations=AsyncMock(return_value=[_failed_citation()])
    )
    env["ctx"].subagent_runtime = SimpleNamespace(
        active_subagents_block=MagicMock(return_value=SUBAGENTS_BLOCK)
    )
    guidance_inbox[JOB_ID] = [
        {"id": "g1", "text": "stop retrying X", "source": "officer"}
    ]


def _tool_call(call_id: str) -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[
            {"name": "read_file", "args": {"path": f"{call_id}.md"}, "id": call_id}
        ],
    )


def _srw_count(request: List[BaseMessage], kind: str) -> int:
    needle = f'<srw_context kind="{kind}">'
    return sum(str(m.content).count(needle) for m in request)


def _with_ids(messages: List[BaseMessage], prefix: str) -> List[BaseMessage]:
    """State copies with ids, as the add_messages reducer assigns them."""
    return [
        m if m.id else m.model_copy(update={"id": f"{prefix}{i}"})
        for i, m in enumerate(messages)
    ]


def _entries(messages: List[BaseMessage]) -> List[BaseMessage]:
    return [m for m in messages if is_context_entry(m)]


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
        `messages` does not carry — the system prompt — plus, on the delivery
        turn only, the phase block just appended (the provider-anchored
        trigger predates it). The next turn drops the term; the originals are
        restored after every turn. The entries appended on the first turn add
        no term."""
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

        assert _entries(first["messages"])  # this turn appended context
        common = ctx.get_token_count([SystemMessage(content=SYSTEM_PROMPT)])
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
    async def test_overhead_drops_the_memory_and_knowledge_terms(self, env):
        """§D row 6: there is no rebuilt tail, so only the system prompt (and
        the phase block on its delivery turn) lowers the thresholds."""
        env["ctx"].knowledge_store = MagicMock()
        env["ctx"].project_id = str(UUID(int=9))
        ctx = env["context"]
        node = _make_node(env, CapturingLLM())

        await _run(node, _state())

        system = ctx.get_token_count([SystemMessage(content=SYSTEM_PROMPT)])
        original = FakeContextMgr.ORIGINAL_THRESHOLD
        assert ctx.thresholds_seen == [(original - system, original - system)]


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
        returned to state, so it is history from then on (the node never
        renders the list into a request itself)."""
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


# ---------------------------------------------------------------------------
# The layout: entries appended once and folded into their carrier (WP2 2.4)
# ---------------------------------------------------------------------------


class SequenceManager(RecordingManager):
    """One payload per retrieval, in order; the last one repeats."""

    def __init__(self, payloads: List[MemoryPayload]) -> None:
        super().__init__(payloads[0])
        self.payloads = list(payloads)

    def _next_payload(self, req: Any) -> MemoryPayload:
        self.assemble_requests.append(req)
        index = min(len(self.assemble_requests), len(self.payloads)) - 1
        return self.payloads[index]


class ToggleCompactingContextMgr(CompactingContextMgr):
    """Compacts (like ``CompactingContextMgr``) only while ``compact`` is set."""

    def __init__(self) -> None:
        super().__init__()
        self.compact = False

    async def ensure_within_limits(
        self, messages, *args, restate_after_summary=None, **kwargs
    ):
        if not self.compact:
            return await FakeContextMgr.ensure_within_limits(
                self, messages, *args, **kwargs
            )
        return await super().ensure_within_limits(
            messages, *args, restate_after_summary=restate_after_summary, **kwargs
        )


class TestAppendOnlyLayout:
    @pytest.mark.asyncio
    async def test_entries_ride_the_carrier_and_reach_state_before_the_response(
        self, env, guidance_inbox
    ):
        """One request: system, summaries, the history in state order. The
        context comes after the history as typed entries in INJECTION_KINDS
        order, folded into the last history message (here the phase block
        delivered this turn); no synthetic pair, no extra turn, no tail. The
        entries are returned between the block and the response."""
        _bind_tactical_phase_skill(env)
        _all_sources(env, guidance_inbox)
        llm = CapturingLLM(responses=[_tool_call("c2")])
        node = _make_node(env, llm)
        state = _state()
        ack = MagicMock()

        with patch.object(dual_app, "ack_guidance", ack):
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
        ]
        assert not any(is_legacy_injection(m) for m in request)
        assert not any(is_context_entry(m) for m in request)
        expected_history = [
            m for m in state["messages"] if not isinstance(m, SystemMessage)
        ]
        assert _wire_all(request[2:-1]) == _wire_all(expected_history)

        # [0] is the ONE phase-agnostic prompt, rebuilt per turn; a
        # SystemMessage in state is sent only as a summary, right after it —
        # the stale one is dropped.
        assert request[0].content == SYSTEM_PROMPT
        assert request[1] is state["messages"][0]
        assert not any("STALE SYSTEM MESSAGE" in str(m.content) for m in request)
        # No todo message, and the node never renders the list itself: it is
        # in the history only where a todo tool or a phase start put it (D17).
        assert not any(_is_todos(m) for m in request)
        assert not any("Do the task" in str(m.content) for m in request)

        # State: the block, the entries, the response (routing reads [-1]).
        block, *entries, response = result["messages"]
        assert is_protected_message(block)
        assert protected_phase_key(block) == "2:tactical"
        assert "UNIQUE TACTICAL PHASE BODY" in block.content
        assert env["context"].phase_key == "2:tactical"
        assert [entry_kind(e) for e in entries] == ENTRY_KINDS
        assert response.tool_calls[0]["id"] == "c2"

        # The carrier: a copy of the block with the entries' stored text
        # appended; the stored block itself is untouched.
        carrier = request[-1]
        assert carrier.content == "\n\n".join(
            [block.content] + [e.content for e in entries]
        )
        assert carrier.additional_kwargs[FOLDED_KEY] == ENTRY_KINDS
        assert FOLDED_KEY not in block.additional_kwargs
        assert all(_srw_count(request, kind) == 1 for kind in ENTRY_KINDS)

        # Memory: shown by handle, the row ids only in the metadata (D30).
        memory = entries[0]
        assert [i["key"] for i in entry_meta(memory)["items"]] == [
            str(UUID(int=1)),
            str(UUID(int=2)),
        ]
        assert f"[{memory_handle(UUID(int=1))}]" in memory.content
        assert str(UUID(int=1)) not in memory.content

        # Citation feedback and the live children: read for this request.
        env["ctx"].citation_engine.list_citations.assert_awaited_with(
            verification_status="failed", include_withdrawn=False
        )
        assert "quote not found in source" in entries[2].content
        env["ctx"].subagent_runtime.active_subagents_block.assert_called_once_with()
        assert "do not poll" in entries[4].content

        # Guidance: once, without a "may repeat" notice; delivered.
        guidance = entries[3]
        assert "stop retrying X" in guidance.content
        assert "may repeat" not in guidance.content
        assert result["delivered_guidance_ids"] == ["g1"]
        ack.assert_called_once_with(JOB_ID, guidance_ids=["g1"], reply_threads=None)

    @pytest.mark.asyncio
    async def test_unchanged_context_is_not_sent_again(self, env, guidance_inbox):
        """Turn N+1 starts with turn N's whole request, entries included, and
        appends nothing while nothing changed. Guidance still pending in the
        inbox is filtered by the delivered ids, not shown again; the pinned
        lane re-acks it after the answered turn until the heartbeat drops
        it."""
        _all_sources(env, guidance_inbox)
        llm = CapturingLLM(responses=[_tool_call("c2"), _tool_call("c3")])
        node = _make_node(env, llm)
        state = _state()
        ack = MagicMock()

        with patch.object(dual_app, "ack_guidance", ack):
            first = await _run(node, state)
            _apply_turn(state, first)
            tool_reply = ToolMessage(
                content="contents of c2.md", tool_call_id="c2", name="read_file"
            )
            state["messages"].append(tool_reply)
            second = await _run(node, state)

        req1, req2 = llm.requests
        assert _wire_all(req2[: len(req1)]) == _wire_all(req1)
        assert _wire_all(req2[len(req1) :]) == _wire_all(
            [first["messages"][-1], tool_reply]
        )
        assert [entry_kind(e) for e in _entries(first["messages"])] == ENTRY_KINDS
        (response,) = second["messages"]
        assert response.tool_calls[0]["id"] == "c3"
        assert all(_srw_count(req2, kind) == 1 for kind in ENTRY_KINDS)
        assert second["delivered_guidance_ids"] == ["g1"]
        assert (
            ack.call_args_list
            == [call(JOB_ID, guidance_ids=["g1"], reply_threads=None)] * 2
        )

    @pytest.mark.asyncio
    async def test_history_prefix_identical_across_two_turns(self, env):
        """Turn N+1's request is turn N's request followed by turn N's new
        state messages — byte for byte on the wire. The phase block delivered
        on turn N carries that turn's entries and stays their carrier on turn
        N+1; the block is delivered once and is history on both turns."""
        _bind_tactical_phase_skill(env)
        llm = CapturingLLM(responses=[_tool_call("c2"), AIMessage(content="done")])
        node = _make_node(env, llm)
        state = _state()

        first = await _run(node, state)
        _apply_turn(state, first)
        # The tools node's contribution between the two execute turns.
        tool_reply = ToolMessage(
            content="contents of c2.md", tool_call_id="c2", name="read_file"
        )
        state["messages"].append(tool_reply)
        second = await _run(node, state)

        req1, req2 = llm.requests
        # Byte-identical prefix: req2 starts with the whole of req1 ...
        assert _wire_all(req2[: len(req1)]) == _wire_all(req1)
        # ... continued by exactly what state gained that req1 did not show
        # (the block and the entries are inside req1's carrier already).
        assert _wire_all(req2[len(req1) :]) == _wire_all(
            [first["messages"][-1], tool_reply]
        )
        carrier = req1[-1]
        assert is_protected_message(carrier)
        assert carrier.additional_kwargs[FOLDED_KEY] == ["memory", "knowledge"]
        assert [entry_kind(e) for e in _entries(first["messages"])] == [
            "memory",
            "knowledge",
        ]

        # The block was delivered once and is history on both turns; nothing
        # changed, so turn 2 appends no entry.
        assert sum(is_protected_message(m) for m in req1) == 1
        assert sum(is_protected_message(m) for m in req2) == 1
        assert "phase_instruction_injections" not in second
        assert not any(is_protected_message(m) for m in second["messages"])
        assert not _entries(second["messages"])

    @pytest.mark.asyncio
    async def test_new_and_changed_items_append_after_the_earlier_entries(self, env):
        """A changed memory (same row, new text) and a new one go out as one
        new entry on the newest carrier; the earlier entry stays where it
        was, so the earlier request is still the prefix (D1, D2, D5)."""
        revised = MemoryRecord(
            id=UUID(int=1), content="Deploys go through Fleet and Helm.", token_count=8
        )
        env["service"] = SequenceManager(
            [
                make_payload(memories=MEMORIES[:1]),
                make_payload(memories=[revised, MEMORIES[1]]),
            ]
        )
        llm = CapturingLLM(responses=[_tool_call("c2"), _tool_call("c3")])
        node = _make_node(env, llm)
        state = _state()

        first = await _run(node, state)
        _apply_turn(state, first)
        state["messages"].append(
            ToolMessage(
                content="contents of c2.md", tool_call_id="c2", name="read_file"
            )
        )
        second = await _run(node, state)

        req1, req2 = llm.requests
        assert _wire_all(req2[: len(req1)]) == _wire_all(req1)
        (memory,) = _entries(second["messages"])
        assert entry_kind(memory) == "memory"
        assert [i["key"] for i in entry_meta(memory)["items"]] == [
            str(UUID(int=1)),
            str(UUID(int=2)),
        ]
        assert memory.content.count(UPDATED_ITEM_MARKER) == 1
        assert "Fleet and Helm" in memory.content
        assert _srw_count(req2, "memory") == 2
        assert _srw_count(req2, "knowledge") == 1
        assert req2[-1].additional_kwargs[FOLDED_KEY] == ["memory"]

    @pytest.mark.asyncio
    async def test_layer1_rebuild_replans_against_the_forced_compaction(self, env):
        """The Layer-1 safety rebuild re-plans on the history its forced
        compaction left: the request carries one set of entries (not the
        normal path's plus the rebuild's), planned after compaction evicted
        the memory entry that sat in the history before."""
        ctx = CompactingContextMgr(overflow_once=True)
        env["context"] = ctx
        llm = CapturingLLM(responses=[_tool_call("c2")])
        node = _make_node(env, llm)
        prior = make_context_entry(
            "memory",
            "[m:older] Deploys go through Fleet.",
            section="memory",
            items=[
                {
                    "key": str(UUID(int=1)),
                    "hash": digest(MEMORIES[0].content),
                    "handle": memory_handle(UUID(int=1)),
                }
            ],
        )
        history = _history()
        history.insert(-1, prior)  # after the tool result, before the notice
        history = _with_ids(history, "m")
        prior_id = next(m.id for m in history if is_context_entry(m))

        result = await _run(node, _state(history))

        assert len(ctx.hooks) == 2  # the normal compaction + the forced one
        (request,) = llm.requests
        assert _srw_count(request, "memory") == 1
        assert _srw_count(request, "knowledge") == 1
        assert "[m:older]" not in "".join(str(m.content) for m in request)
        carrier = request[-1]
        assert carrier.content.startswith("[PHASE_TRANSITION]")
        assert carrier.additional_kwargs[FOLDED_KEY] == ["memory", "knowledge"]

        state_messages = [
            m for m in result["messages"] if not isinstance(m, RemoveMessage)
        ]
        assert [_kind(m) for m in state_messages] == [
            "summary",
            "HumanMessage",  # the restated todo list
            "HumanMessage",  # the kept window
            "HumanMessage",  # the memory entry
            "HumanMessage",  # the knowledge entry
            "AIMessage",
        ]
        memory, knowledge = _entries(state_messages)
        assert [entry_kind(memory), entry_kind(knowledge)] == ["memory", "knowledge"]
        # Both memories: the plan no longer saw the evicted prior entry.
        assert [i["key"] for i in entry_meta(memory)["items"]] == [
            str(UUID(int=1)),
            str(UUID(int=2)),
        ]
        removed = {m.id for m in result["messages"] if isinstance(m, RemoveMessage)}
        assert prior_id in removed

    @pytest.mark.asyncio
    async def test_layer0_rebuild_replans_against_the_emergency_compaction(self, env):
        env["context"] = CompactingContextMgr()
        llm = OverflowOnceLLM()
        llm._responses.append(_tool_call("c2"))
        node = _make_node(env, llm)

        result = await _run(node, _state(_with_ids(_history(), "m")))

        assert llm.raised
        (request,) = llm.requests  # the retry; the first attempt never left
        assert _srw_count(request, "memory") == 1
        assert request[-1].additional_kwargs[FOLDED_KEY] == ["memory", "knowledge"]
        state_messages = [
            m for m in result["messages"] if not isinstance(m, RemoveMessage)
        ]
        assert [entry_kind(m) for m in state_messages[-3:-1]] == [
            "memory",
            "knowledge",
        ]
        assert state_messages[-1].tool_calls[0]["id"] == "c2"
        assert len(_entries(state_messages)) == 2

    @pytest.mark.asyncio
    async def test_compaction_evicts_an_entry_and_it_is_appended_again(
        self, env, guidance_inbox
    ):
        """D4: a compaction evicts the entries with the summarized region
        (RemoveMessage); the next plan finds them absent and appends them
        again on the newest carrier. Delivered guidance is not appended
        again (the summary keeps it, O1)."""
        _all_sources(env, guidance_inbox)
        ctx = ToggleCompactingContextMgr()
        env["context"] = ctx
        llm = CapturingLLM(responses=[AIMessage(content="done"), _tool_call("c3")])
        node = _make_node(env, llm)
        state = _state(_with_ids(_history(), "m"))

        with patch.object(dual_app, "ack_guidance", MagicMock()):
            first = await _run(node, state)
            new = _with_ids(first["messages"], "t1-")
            _apply_turn(state, {**first, "messages": new})
            # A text-only answer with todos pending leaves the reminder last.
            reminder = state["messages"][-1]
            assert isinstance(reminder, HumanMessage)
            ctx.compact = True
            second = await _run(node, state)

        first_entries = _entries(new)
        assert [entry_kind(e) for e in first_entries] == ENTRY_KINDS
        removed = {m.id for m in second["messages"] if isinstance(m, RemoveMessage)}
        assert {e.id for e in first_entries} <= removed

        req2 = llm.requests[-1]
        assert [_kind(m) for m in req2] == [
            "system",
            "summary",
            "HumanMessage",  # the restated todo list
            "HumanMessage",  # the kept reminder, carrying the entries again
        ]
        assert req2[-1].content.startswith(reminder.content)
        again = ["memory", "knowledge", "citation", "subagents"]
        assert req2[-1].additional_kwargs[FOLDED_KEY] == again
        assert _srw_count(req2, "guidance") == 0
        state_messages = [
            m for m in second["messages"] if not isinstance(m, RemoveMessage)
        ]
        assert [entry_kind(e) for e in _entries(state_messages)] == again
        assert second["delivered_guidance_ids"] == ["g1"]

    @pytest.mark.asyncio
    async def test_archive_and_memory_inject_audit(self, env):
        """The archiver gets the folded request (llm_requests) and the
        unfolded history (the chat delta, §D 7c); memory_inject carries the
        appended / already-present counts."""
        import asyncio

        llm = CapturingLLM(responses=[_tool_call("c2"), _tool_call("c3")])
        node = _make_node(env, llm)
        auditor = MagicMock()
        state = _state()

        with patch("agent.graph.get_archiver", return_value=auditor):
            patches = _patches()
            for p in patches:
                p.start()
            try:
                first = await node(state)
                _apply_turn(state, first)
                state["messages"].append(
                    ToolMessage(
                        content="contents of c2.md",
                        tool_call_id="c2",
                        name="read_file",
                    )
                )
                await node(state)
                for _ in range(3):
                    await asyncio.sleep(0)
            finally:
                for p in patches:
                    p.stop()

        first_entries = _entries(first["messages"])
        archived = [c.kwargs for c in auditor.archive.call_args_list]
        assert [a["messages"] for a in archived] == llm.requests
        assert archived[0]["messages"][-1].additional_kwargs[FOLDED_KEY] == [
            "memory",
            "knowledge",
        ]
        # The unfolded history: the entries are their own messages, at the
        # end on the turn they were appended and in place after that.
        assert archived[0]["history_messages"][-2:] == first_entries
        assert not _entries(archived[0]["history_messages"][:-2])
        assert _entries(archived[1]["history_messages"]) == first_entries
        assert not is_context_entry(archived[1]["history_messages"][-1])

        injects = [
            c.kwargs["data"]
            for c in auditor.audit_step.call_args_list
            if c.kwargs.get("step_type") == "memory_inject"
        ]
        assert [(d["count"], d["appended"], d["present"]) for d in injects] == [
            (1, 2, 0),
            (1, 0, 2),
        ]


# ---------------------------------------------------------------------------
# Retrieval off the request path (WP3; D6-D8, D13)
# ---------------------------------------------------------------------------


class TestAsyncRetrieval:
    """The execute node starts the memory retrieval and never awaits it.

    A real MemoryManager with a gated retriever: the test decides when a
    retrieval finishes relative to the requests.
    """

    def _next_tool_result(self, state: dict, result: dict, call_id: str) -> None:
        _apply_turn(state, result)
        state["messages"].append(
            ToolMessage(
                content=f"contents of {call_id}.md",
                tool_call_id=call_id,
                name="read_file",
            )
        )

    @pytest.mark.asyncio
    async def test_a_result_that_finishes_late_reaches_the_next_request(self, env):
        retriever = GatedRetriever(MEMORIES)
        manager = env["service"] = make_async_manager(retriever)
        llm = CapturingLLM(
            responses=[_tool_call("c2"), _tool_call("c3"), _tool_call("c4")]
        )
        node = _make_node(env, llm)
        state = _state()

        # Request 1 starts the retrieval with its own query and goes out
        # while it is still running: no memory, and it never waited (D6).
        first = await _run(node, state)
        await asyncio.sleep(0)  # the provider call a real request awaits
        assert len(retriever.queries) == 1
        assert "Do the task" in retriever.queries[0]
        assert manager.retrieval_in_flight
        assert _srw_count(llm.requests[0], "memory") == 0
        self._next_tool_result(state, first, "c2")

        # It finishes after request 1 was sent; request 2 takes it in (D8)
        # and starts the next retrieval.
        retriever.release()
        await settle_retrieval(manager)
        second = await _run(node, state)
        await asyncio.sleep(0)
        assert _srw_count(llm.requests[1], "memory") == 1
        assert len(retriever.queries) == 2
        self._next_tool_result(state, second, "c3")

        # Request 3 while retrieval 2 runs: single flight starts nothing, no
        # new entry; the memory entry is history and request 3 extends
        # request 2 (D7, the cache rule).
        await _run(node, state)
        assert len(retriever.queries) == 2
        assert _srw_count(llm.requests[2], "memory") == 1
        previous = llm.requests[1]
        assert _wire_all(llm.requests[2][: len(previous) - 1]) == _wire_all(
            previous[:-1]
        )
        stats = manager.retrieval_stats()
        assert stats["started"] == 2
        assert stats["skipped_in_flight"] == 1
        assert stats["drained"] == 1

        await manager.close_background()
        assert retriever.cancelled == 1

    @pytest.mark.asyncio
    async def test_a_fast_retrieval_serves_the_request_that_started_it(self, env):
        """The node starts early and takes in late: a retrieval that finishes
        before the request is built serves that request."""
        manager = env["service"] = make_async_manager(
            GatedRetriever(MEMORIES, auto=True)
        )
        llm = CapturingLLM(responses=[_tool_call("c2")])

        async def no_failed_citations(**_kwargs):
            # The citation lookup between start and take-in yields to the
            # loop, as every real read there does.
            for _ in range(5):
                await asyncio.sleep(0)
            return []

        env["ctx"].citation_engine = SimpleNamespace(list_citations=no_failed_citations)
        node = _make_node(env, llm)

        await _run(node, _state())

        assert _srw_count(llm.requests[0], "memory") == 1
        await manager.close_background()

    @pytest.mark.asyncio
    async def test_the_memory_inject_audit_carries_the_async_run(self, env):
        retriever = GatedRetriever(MEMORIES)
        manager = env["service"] = make_async_manager(retriever)
        llm = CapturingLLM(responses=[_tool_call("c2"), _tool_call("c3")])
        node = _make_node(env, llm)
        state = _state()
        auditor = MagicMock()

        with patch("agent.graph.get_archiver", return_value=auditor):
            patches = _patches()
            for p in patches:
                p.start()
            try:
                first = await node(state)
                await asyncio.sleep(0)
                self._next_tool_result(state, first, "c2")
                retriever.release()
                await settle_retrieval(manager)
                await node(state)
            finally:
                for p in patches:
                    p.stop()

        injects = [
            c.kwargs["data"]
            for c in auditor.audit_step.call_args_list
            if c.kwargs.get("step_type") == "memory_inject"
        ]
        # Only the request that took a result in audits it.
        (data,) = injects
        assert (data["count"], data["appended"], data["present"]) == (2, 2, 0)
        assert data["stats"]["latency_ms"] >= 0
        assert data["retrieval"]["mode"] == "async"
        assert data["retrieval"]["seq"] == 1
        assert data["retrieval"]["started_this_request"] is True
        assert data["retrieval"]["degraded"] is None
        assert data["retrieval"]["counters"]["drained"] == 1
        await manager.close_background()

    @pytest.mark.asyncio
    async def test_the_take_in_gets_where_the_job_stands(self, env):
        """P4: the job's turns, and the last turn a compaction removed,
        derived from the responses its history still holds."""
        seen: List[Any] = []
        service = env["service"]
        take = service.take_retrieval

        def recording_take(conversation=None):
            seen.append(conversation)
            return take(conversation)

        service.take_retrieval = recording_take
        llm = CapturingLLM(responses=[_tool_call("c2")])

        await _run(_make_node(env, llm), _state(turn_count=10))

        # _history() holds one response: turns 1-9 were compacted away.
        assert seen == [ConversationTurns(compacted_through=9, current=10)]

    async def _same_conversation_turn(self, env, memories) -> tuple:
        """One request at turn 10 under skip_in_context, the retrieval
        finished before it; returns (request, memory_inject audit data)."""
        env["config"].memory.retrieval = MemoryRetrievalConfig(
            same_conversation="skip_in_context"
        )
        manager = env["service"] = make_async_manager(
            GatedRetriever(memories, auto=True), job_id=JOB_ID
        )
        manager.runtime.memory_config = env["config"].memory
        manager.start_retrieval(AssembleRequest(query_text="earlier request"))
        await settle_retrieval(manager)
        llm = CapturingLLM(responses=[_tool_call("c2")])
        auditor = MagicMock()

        with patch("agent.graph.get_archiver", return_value=auditor):
            patches = _patches()
            for p in patches:
                p.start()
            try:
                await _make_node(env, llm)(_state(turn_count=10))
            finally:
                for p in patches:
                    p.stop()
        await manager.close_background()

        (data,) = [
            c.kwargs["data"]
            for c in auditor.audit_step.call_args_list
            if c.kwargs.get("step_type") == "memory_inject"
        ]
        return llm.requests[0], data

    @pytest.mark.asyncio
    async def test_skip_in_context_drops_this_jobs_memory_and_audits_it(self, env):
        """P4 arm B: this job's memory from a turn still in the history is
        not appended; one a compaction removed and another job's are."""
        in_context, evicted, other = (
            MemoryRecord(
                id=UUID(int=n),
                job_id=job,
                content=f"Fact {n}.",
                token_count=3,
                source_turn_end=turn,
            )
            for n, job, turn in ((11, JOB_ID, 10), (12, JOB_ID, 9), (13, "other", 10))
        )

        request, data = await self._same_conversation_turn(
            env, [in_context, evicted, other]
        )

        text = "\n".join(str(m.content) for m in request)
        assert f"[{memory_handle(in_context.id)}]" not in text
        assert f"[{memory_handle(evicted.id)}]" in text
        assert f"[{memory_handle(other.id)}]" in text
        assert (data["count"], data["appended"]) == (2, 2)
        assert data["stats"]["extra"]["same_conversation"] == {
            "mode": "skip_in_context",
            "compacted_through": 9,
            "current_turn": 10,
            "same_conversation": 2,
            "skipped": 1,
        }
        assert data["retrieval"]["counters"]["same_conversation_skipped"] == 1

    @pytest.mark.asyncio
    async def test_a_request_whose_memories_were_all_skipped_is_audited(self, env):
        fresh = MemoryRecord(
            id=UUID(int=14), job_id=JOB_ID, content="Fresh.", source_turn_end=10
        )

        request, data = await self._same_conversation_turn(env, [fresh])

        assert _srw_count(request, "memory") == 0
        assert (data["count"], data["appended"]) == (0, 0)
        assert data["stats"]["extra"]["same_conversation"]["skipped"] == 1

    @pytest.mark.asyncio
    async def test_a_structural_reranker_failure_leaves_the_request_alive(self, env):
        """D13: the failure is audited and counted, the requests go out."""
        scorer = BrokenScorer(ValueError("rerank route returned HTML"))
        manager = env["service"] = make_async_manager(
            GatedRetriever(MEMORIES, auto=True), scorer=scorer, job_id=JOB_ID
        )
        llm = CapturingLLM(responses=[_tool_call("c2"), _tool_call("c3")])
        node = _make_node(env, llm)
        state = _state()
        archiver = MagicMock()

        with patch("agent.core.archiver.get_archiver", return_value=archiver):
            first = await _run(node, state)
            self._next_tool_result(state, first, "c2")
            await settle_retrieval(manager)
            second = await _run(node, state)
            await settle_retrieval(manager)

        assert not first.get("error") and not second.get("error")
        assert len(llm.requests) == 2
        assert all(_srw_count(r, "memory") == 0 for r in llm.requests)
        assert manager.retrieval_stats()["degraded"] == 2
        steps = [c.kwargs["step_type"] for c in archiver.audit_step.call_args_list]
        assert steps == ["memory_pipeline_degraded"]
        await manager.close_background()
