"""Cutover wiring tests for the MemoryManager seam (Phase 1, slice 5).

The equivalence suites (tests/test_memory_*_equivalence.py) pin the seam's
*logic* against verbatim reproductions of the legacy blocks. This suite pins
the *wiring* that slice 5 added to production code:

- the YAML pipeline defaults parse and every configured name binds,
- both graphs construct the manager behind memory.manager.enabled with the
  right MemoryRuntime (stores, prompts, per-mode retrieval timeout),
- flag-on call sites route through assemble()/capture() with the right
  request/event shapes and skip the legacy direct-store blocks,
- the B11 terminate capture fires for detach-style endings exactly once,
- B10: every injection message the manager produces is recognized by the
  summarization-exclusion predicate (is_workspace_injection_message), so
  compaction keeps stripping injected pairs after the cutover.
"""

import asyncio
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    RemoveMessage,
    SystemMessage,
    ToolMessage,
)

from shared.runtime.core.loader import (
    InstructionFileEntry,
    QueryConfig,
    load_agent_config,
)
from shared.runtime.core.message_markers import (
    is_protected_message,
    protected_phase_key,
)
from agent.core.workspace import WorkspaceManager
from shared.runtime.core.workspace_injection import (
    TODOS_INJECTION_CONTENT_PREFIX,
    is_workspace_injection_message,
)
from shared.runtime.llm.exceptions import ContextOverflowError
from agent.managers import TodoManager, PlanManager
from agent.services.memory import (
    AssembleStats,
    Candidate,
    InjectionBlock,
    MemoryManager,
    MemoryPayload,
    MemoryRuntime,
)
from agent.tools.context import ToolContext
from tests._fs_backend import FilesystemTestBackend
from tests._memory_fixtures import (
    PROJECT_ID,
    make_kb_mock,
    make_memories,
    make_notes,
    make_recall_mock,
)

REPO_ROOT = Path(__file__).parent.parent
WORKER_CONFIG_PATH = str(REPO_ROOT / "config" / "worker_base.yaml")
PERSISTENT_CONFIG_PATH = str(REPO_ROOT / "config" / "session_base.yaml")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class RecordingManager:
    """Manager stub for wiring tests — records calls, returns a fixed payload.

    The call sites only depend on the seam API (assemble/capture), so a
    recorder proves the wiring without re-testing seam internals.
    """

    def __init__(self, payload: Optional[MemoryPayload] = None) -> None:
        self.payload = payload or MemoryPayload()
        self.assemble_requests: List[Any] = []
        self.captures: List[Any] = []

    async def assemble(self, req: Any) -> MemoryPayload:
        self.assemble_requests.append(req)
        return self.payload

    async def capture(self, event: Any) -> None:
        self.captures.append(event)


def make_payload(
    memory_text: str = "MEMBLOCK", knowledge_text: str = "KBBLOCK"
) -> MemoryPayload:
    """A payload with real injection pairs (the production message shapes)."""
    from agent.core.knowledge_injection import create_knowledge_injection_messages
    from agent.core.memory_injection import create_memory_injection_messages

    blocks: List[InjectionBlock] = []
    if memory_text:
        blocks.append(
            InjectionBlock(
                kind="memory",
                content=memory_text,
                messages=list(create_memory_injection_messages(memory_text)),
                token_count=26,
                items=[
                    {"record_id": "m1", "token_count": 12},
                    {"record_id": "m2", "token_count": 14},
                ],
            )
        )
    if knowledge_text:
        blocks.append(
            InjectionBlock(
                kind="knowledge",
                content=knowledge_text,
                messages=list(create_knowledge_injection_messages(knowledge_text)),
                token_count=0,
                items=[{"record_id": "k1", "token_count": 0}],
            )
        )
    return MemoryPayload(blocks=blocks, stats=AssembleStats(blocks=len(blocks)))


class FakeContextMgr:
    """Minimal real-typed context manager for driving the execute node.

    Explicit class instead of MagicMock — the node does arithmetic and
    comparisons on these values (the B1 mock-config lesson).
    """

    def __init__(self, ensure_hook=None, should_summarize=False) -> None:
        self.config = SimpleNamespace(
            compaction_threshold_tokens=100_000,
            summarization_threshold_tokens=100_000,
            keep_recent_messages=10,
        )
        self._state = SimpleNamespace(summaries=[])
        self._ensure_hook = ensure_hook
        self._should_summarize = should_summarize
        self.phase_key: Optional[str] = None

    def set_current_phase(self, phase: str, phase_key: Optional[str] = None) -> None:
        self.phase_key = phase_key

    def should_summarize(self, messages) -> bool:
        # The pre_compaction emit gates on this; off by default so these
        # wiring tests stay focused on the read/write + compaction-capture path.
        return self._should_summarize

    def get_token_count(self, messages: List[Any]) -> int:
        return 10

    async def ensure_within_limits(self, messages, *args, **kwargs):
        if self._ensure_hook is not None:
            return self._ensure_hook(messages, self)
        return messages

    def clear_old_tool_results(self, messages):
        return messages


@pytest.fixture
def workspace_manager(tmp_path):
    ws = WorkspaceManager(
        job_id="cutover-job-1",
        base_path=tmp_path,
        backend=FilesystemTestBackend(tmp_path),
    )
    ws.initialize()
    return ws


@pytest.fixture
def worker_config():
    return load_agent_config(WORKER_CONFIG_PATH)


@pytest.fixture
def persistent_config():
    return load_agent_config(PERSISTENT_CONFIG_PATH)


# ---------------------------------------------------------------------------
# 1. YAML pipeline defaults
# ---------------------------------------------------------------------------


class TestPipelineDefaults:
    """The shipped YAML defaults: flag on (cutover), per-mode pipelines bind."""

    def test_worker_defaults(self, worker_config):
        m = worker_config.memory
        assert m.manager_enabled is True  # cutover flag on (Phase-1 closure)
        assert m.pipeline.retrievers == ["recall_two_tier", "kb_notes"]
        assert m.pipeline.writers == [
            "interval_extractor",
            "phase_boundary_extractor",
            "pre_compaction_extractor",
            "memory_assembler",
            "compaction_memory",
            "queued_memory",
        ]

    def test_persistent_defaults(self, persistent_config):
        m = persistent_config.memory
        assert m.manager_enabled is True
        assert m.pipeline.retrievers == ["recall_two_tier", "kb_notes"]
        assert m.pipeline.writers == [
            "persistent_interval_extractor",
            "pre_compaction_extractor",
            "teardown_extractor",
        ]

    def test_configured_names_bind(self, worker_config, persistent_config, monkeypatch):
        """Registry/YAML drift guard: every shipped name must resolve.

        Includes the GATE-B stack now in the defaults (scorers [reranker],
        policies [gate, bounded]) — their factories read runtime.memory_config,
        so bind with the config attached.
        """
        # The reranker rides the EMBEDDING endpoint's transport (EMBEDDING_BASE_URL),
        # injected at dispatch in production — stub it here so bind resolves.
        monkeypatch.setenv("EMBEDDING_BASE_URL", "https://embed.test/v1")
        aux_transport = SimpleNamespace(base_url="https://aux.test/v1", api_key="k")
        for cfg in (worker_config, persistent_config):
            manager = MemoryManager.from_config(
                cfg.memory,
                MemoryRuntime(memory_config=cfg.memory, auxiliary_config=aux_transport),
            )
            summary = manager.pipeline_summary()
            assert summary["retrievers"] == cfg.memory.pipeline.retrievers
            assert summary["scorers"] == cfg.memory.pipeline.scorers
            assert summary["policies"] == cfg.memory.pipeline.policies
            assert summary["writers"] == cfg.memory.pipeline.writers

    def test_manager_flag_survives_dispatch_round_trip(self, worker_config):
        """Dispatch paths re-parse a live config after dataclasses.asdict()
        + deep_merge (job config_override in src/agent.py, session config
        assembly and config.update in persistent_app). asdict emits the
        flat dataclass field name (``manager_enabled``), not the YAML
        nesting (``manager.enabled``) — the parser must accept both or the
        cutover flag silently resets to False on every dispatched
        job/session. Found live on k3d during Phase-1 closure step 1
        (scholar job built its graph with no bind log), 2026-06-11.
        """
        import dataclasses

        from shared.runtime.core.loader import deep_merge, load_agent_config_from_dict

        worker_config.memory.manager_enabled = True
        base = dataclasses.asdict(worker_config)
        merged = deep_merge(base, {"llm": {"model": "gemma-test"}})
        reparsed = load_agent_config_from_dict(merged)
        assert reparsed.memory.manager_enabled is True
        assert (
            reparsed.memory.pipeline.retrievers
            == worker_config.memory.pipeline.retrievers
        )
        assert reparsed.memory.pipeline.writers == worker_config.memory.pipeline.writers


# ---------------------------------------------------------------------------
# 2. Construction wiring
# ---------------------------------------------------------------------------


class TestWorkerConstruction:
    """build_phase_alternation_graph builds the manager behind the flag."""

    def _build(self, config, workspace_manager, tool_context):
        from agent.graph import build_phase_alternation_graph

        return build_phase_alternation_graph(
            llm_with_tools=MagicMock(),
            tools=[],
            config=config,
            workspace=workspace_manager,
            todo_manager=TodoManager(workspace_manager),
            tool_context=tool_context,
        )

    def test_one_binding_builds(self, worker_config, workspace_manager):
        worker_config.memory.manager_enabled = False
        self._build(worker_config, workspace_manager, None)

    def test_flag_on_constructs_with_worker_runtime(
        self, worker_config, workspace_manager
    ):
        worker_config.memory.manager_enabled = True
        # SimpleNamespace (not bare object()) so the now-default-on ingestion
        # verdict can attach (maybe_attach_ingestion_verdict sets
        # recall_store.ingestion_verdict); identity assertions still hold.
        marker_store = SimpleNamespace()
        ctx = ToolContext(workspace_manager=workspace_manager)
        ctx.recall_store = marker_store

        with patch(
            "agent.services.memory.MemoryManager.from_config",
            return_value=RecordingManager(),
        ) as from_config:
            self._build(worker_config, workspace_manager, ctx)

        assert from_config.call_count == 1
        cfg_arg, runtime = from_config.call_args[0]
        assert cfg_arg is worker_config.memory
        assert runtime.recall_store is marker_store
        # has_knowledge() is False (no graph connection) → same gate as the
        # legacy execute block: no knowledge store in the runtime.
        assert runtime.knowledge_store is None
        assert runtime.retrieval_timeout is None  # worker runs unbounded
        assert runtime.memory_config is worker_config.memory
        assert runtime.auxiliary_config is worker_config.auxiliary
        # Matrix-resolved prompts threaded from the graph factory
        assert isinstance(runtime.extraction_prompt, str)
        assert runtime.extraction_prompt
        assert isinstance(runtime.assembler_prompt, str)
        assert runtime.assembler_prompt
        assert runtime.job_id == workspace_manager.job_id
        # GATE B: ingestion is on in the defaults, so the verdict service
        # attaches to the store during construction.
        assert getattr(marker_store, "ingestion_verdict", None) is not None

    def test_exposes_memory_service_on_graph(self, worker_config, workspace_manager):
        """The run loop drains in-flight captures via this handle (OQ-C)."""
        worker_config.memory.manager_enabled = True
        ctx = ToolContext(workspace_manager=workspace_manager)
        ctx.recall_store = SimpleNamespace()
        mgr = RecordingManager()
        with patch("agent.services.memory.MemoryManager.from_config", return_value=mgr):
            graph = self._build(worker_config, workspace_manager, ctx)
        assert getattr(graph, "_srw_memory_service", None) is mgr

    def test_flag_off_exposes_none(self, worker_config, workspace_manager):
        worker_config.memory.manager_enabled = False
        ctx = ToolContext(workspace_manager=workspace_manager)
        ctx.recall_store = SimpleNamespace()
        graph = self._build(worker_config, workspace_manager, ctx)
        # Attribute present (so getattr in the run loop is a no-op), value None.
        assert getattr(graph, "_srw_memory_service", "MISSING") is None

    def test_flag_off_never_constructs(self, worker_config, workspace_manager):
        worker_config.memory.manager_enabled = False  # rollback-lever state
        ctx = ToolContext(workspace_manager=workspace_manager)

        with patch(
            "agent.services.memory.MemoryManager.from_config",
            return_value=RecordingManager(),
        ) as from_config:
            self._build(worker_config, workspace_manager, ctx)

        from_config.assert_not_called()


class TestPersistentConstruction:
    """PersistentSession._setup_memory builds the manager behind the flag."""

    def _make_session(self, persistent_config):
        from agent.api.persistent_session import PersistentSession

        return PersistentSession(thread_id=str(uuid.uuid4()), config=persistent_config)

    @pytest.mark.asyncio
    async def test_flag_on_constructs_with_persistent_runtime(self, persistent_config):
        persistent_config.memory.manager_enabled = True
        session = self._make_session(persistent_config)
        aux_marker = object()
        session.auxiliary_llm = aux_marker
        session.memory_extraction_prompt = "EXTRACT-PROMPT"

        recorder = RecordingManager()
        with (
            patch(
                "shared.runtime.services.embedding_service.get_embedding_service",
                return_value=MagicMock(verify_dimensions=AsyncMock()),
            ),
            patch(
                "agent.services.memory.MemoryManager.from_config",
                return_value=recorder,
            ) as from_config,
        ):
            session._setup_memory(None, vector_conn=MagicMock())

        assert from_config.call_count == 1
        cfg_arg, runtime = from_config.call_args[0]
        assert cfg_arg is persistent_config.memory
        assert runtime.recall_store is session.recall_store
        assert runtime.knowledge_store is session.knowledge_store
        assert runtime.auxiliary_llm is aux_marker
        assert runtime.extraction_prompt == "EXTRACT-PROMPT"
        assert runtime.assembler_prompt is None  # persistent: no assembler
        assert runtime.retrieval_timeout == 5.0  # legacy per-call guard
        assert runtime.job_id == session.thread_id
        assert runtime.project_ids == []
        assert session.memory_service is recorder

    @pytest.mark.asyncio
    async def test_flag_off_never_constructs(self, persistent_config):
        persistent_config.memory.manager_enabled = False  # rollback-lever state
        session = self._make_session(persistent_config)

        with (
            patch(
                "shared.runtime.services.embedding_service.get_embedding_service",
                return_value=MagicMock(verify_dimensions=AsyncMock()),
            ),
            patch("agent.services.memory.MemoryManager.from_config") as from_config,
        ):
            session._setup_memory(None, vector_conn=MagicMock())

        from_config.assert_not_called()
        assert session.memory_service is None


# ---------------------------------------------------------------------------
# 3. Worker execute node wiring (read swap + turn_end + compaction)
# ---------------------------------------------------------------------------


def _make_execute_node(config, workspace_manager, todo_manager, ctx, service, mgr):
    from agent.graph import create_execute_node

    return create_execute_node(
        llm_with_tools=mgr["llm"],
        todo_manager=todo_manager,
        memory_manager=MagicMock(),  # vestigial workspace.md manager
        workspace_manager=workspace_manager,
        config=config,
        context_mgr=mgr["context"],
        retry_manager=MagicMock(),
        auxiliary_llm=MagicMock(),
        summarization_prompt="summarize",
        memory_extraction_prompt="extract",
        memory_assembler_prompt="assemble",
        tool_context=ctx,
        tool_names=None,
        memory_service=service,
    )


def _worker_state():
    return {
        "job_id": "cutover-job-1",
        "iteration": 0,
        "messages": [HumanMessage(content="start")],
        "is_strategic_phase": False,
        "phase_number": 2,
        "turn_count": 0,
        "metadata": {},
    }


@pytest.fixture
def execute_env(worker_config, workspace_manager):
    """Real config/managers, mock LLM, recording manager, legacy-store spies."""
    todo_manager = TodoManager(workspace_manager)
    todo_manager.add("Do the task")

    ctx = ToolContext(workspace_manager=workspace_manager)
    # Legacy-store spies: the flag-on path must never touch them directly.
    ctx.recall_store = AsyncMock()
    ctx.knowledge_store = None

    llm = MagicMock()
    llm.ainvoke = AsyncMock(return_value=AIMessage(content="done"))

    service = RecordingManager(payload=make_payload())
    return {
        "config": worker_config,
        "workspace": workspace_manager,
        "todo": todo_manager,
        "ctx": ctx,
        "llm": llm,
        "service": service,
        "context": FakeContextMgr(),
    }


def _bind_phase_start(env, body: str, *, duplicate: bool = False):
    """Bind research-guide at phase_start:tactical and write ``body`` to it."""
    entry = InstructionFileEntry(
        trigger="phase_start:tactical",
        skill="research-guide",
        enforce=False,
    )
    entries = [entry, entry] if duplicate else [entry]
    env["config"].instruction_files = entries
    env["ctx"]._instruction_files = entries
    env["workspace"].write_file(entry.path, body)
    return entry


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


def _phase_blocks(request, phase_key: str):
    return [
        m
        for m in request
        if is_protected_message(m) and protected_phase_key(m) == phase_key
    ]


def _count_text(request, text: str) -> int:
    return sum(text in str(getattr(m, "content", "")) for m in request)


def _mock_aux():
    """AuxiliaryLLM whose structured summariser returns a fixed short summary."""
    from agent.core.context import ConversationSummary
    from shared.runtime.services.auxiliary import AuxiliaryLLM

    parsed = ConversationSummary(
        summary="Summary of the work so far.",
        tasks_completed="- read files",
        key_decisions="",
        current_state="mid-phase",
        blockers="",
    )
    structured = AsyncMock()
    structured.ainvoke = AsyncMock(
        return_value={
            "raw": AIMessage(content="s"),
            "parsed": parsed,
            "parsing_error": None,
        }
    )
    llm = MagicMock()
    llm.with_structured_output = MagicMock(return_value=structured)
    return AuxiliaryLLM(llm=llm, max_context_tokens=15_000)


_EXECUTE_PATCHES = (
    ("agent.graph.get_archiver", None),
    ("agent.graph.get_phase_system_prompt", "SYS"),
)


class TestWorkerExecuteWiring:
    @pytest.mark.asyncio
    async def test_phase_start_instruction_is_injected_once_per_phase_instance(
        self, execute_env
    ):
        """U2 WP1 durability: the body is delivered ONCE per concrete phase
        as a persistent, protected HumanMessage — present exactly once in
        EVERY request of the phase (from history, not the transient tail),
        returned in state on the delivery turn, and a new phase instance
        gets its own block while the old one stays in uncompacted history."""
        env = execute_env
        marker = "UNIQUE PHASE-START RESEARCH PROCEDURE"
        # Duplicate bindings to the same artifact must still deliver one body.
        _bind_phase_start(env, marker, duplicate=True)
        node = _make_execute_node(
            env["config"],
            env["workspace"],
            env["todo"],
            env["ctx"],
            env["service"],
            {"llm": env["llm"], "context": env["context"]},
        )
        state = _worker_state()

        with (
            patch("agent.graph.get_archiver", return_value=None),
            patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
        ):
            first = await node(state)
            _apply_turn(state, first)
            second = await node(state)
            _apply_turn(state, second)
            state["phase_number"] = 4
            third = await node(state)

        requests = [call.args[0] for call in env["llm"].ainvoke.call_args_list]

        # Delivery turn: the block is returned in state ahead of the response
        # and the ledger records the concrete phase instance.
        assert is_protected_message(first["messages"][0])
        assert marker in first["messages"][0].content
        assert isinstance(first["messages"][1], AIMessage)
        assert first["phase_instruction_injections"] == [
            "2:tactical:skills/research-guide/SKILL.md"
        ]
        assert env["context"].phase_key == "4:tactical"

        # Every request of the phase carries it exactly once.
        for request in requests[:2]:
            assert len(_phase_blocks(request, "2:tactical")) == 1
            assert _count_text(request, marker) == 1
        # Second turn: nothing re-delivered, ledger untouched.
        assert "phase_instruction_injections" not in second
        assert not any(is_protected_message(m) for m in second["messages"])
        # It is history: it sits before the transient tail (todos last).
        todo_idx = next(
            i
            for i, m in enumerate(requests[1])
            if isinstance(m, HumanMessage)
            and str(m.content).startswith(TODOS_INJECTION_CONTENT_PREFIX)
        )
        block_idx = next(
            i for i, m in enumerate(requests[1]) if is_protected_message(m)
        )
        assert block_idx < todo_idx
        assert requests[1][block_idx] is state["messages"][1]

        # Phase 4 gets its own block; phase 2's stays in uncompacted history.
        assert len(_phase_blocks(requests[2], "4:tactical")) == 1
        assert len(_phase_blocks(requests[2], "2:tactical")) == 1
        assert len(third["phase_instruction_injections"]) == 2

    @pytest.mark.asyncio
    async def test_ledger_present_but_block_missing_self_heals_once(self, execute_env):
        """A job resumed mid-phase from a pre-change checkpoint has the ledger
        entry but no block in history: deliver once more (logged), then the
        presence check takes over — no second delivery, ledger unchanged."""
        env = execute_env
        marker = "SELF-HEAL PHASE BODY"
        _bind_phase_start(env, marker)
        node = _make_execute_node(
            env["config"],
            env["workspace"],
            env["todo"],
            env["ctx"],
            env["service"],
            {"llm": env["llm"], "context": env["context"]},
        )
        key = "2:tactical:skills/research-guide/SKILL.md"
        state = _worker_state()
        state["phase_instruction_injections"] = [key]

        with (
            patch("agent.graph.get_archiver", return_value=None),
            patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
        ):
            first = await node(state)
            _apply_turn(state, first)
            second = await node(state)

        requests = [call.args[0] for call in env["llm"].ainvoke.call_args_list]
        assert len(_phase_blocks(requests[0], "2:tactical")) == 1
        assert _count_text(requests[0], marker) == 1
        assert is_protected_message(first["messages"][0])
        assert first["phase_instruction_injections"] == [key]
        # Healed: the next turn sees the block and delivers nothing.
        assert len(_phase_blocks(requests[1], "2:tactical")) == 1
        assert _count_text(requests[1], marker) == 1
        assert "phase_instruction_injections" not in second
        assert not any(is_protected_message(m) for m in second["messages"])

    @pytest.mark.asyncio
    async def test_prompt_tokens_grow_only_by_new_messages_across_two_tactical_turns(
        self, execute_env
    ):
        """Acceptance (b): the block is never re-billed. Across two consecutive
        tactical turns the request grows only by the new messages; the prefix
        up to and including the block is byte-identical, and the tail is the
        same block of transients."""
        env = execute_env
        body = "PHASE BODY " * 50
        _bind_phase_start(env, body)
        node = _make_execute_node(
            env["config"],
            env["workspace"],
            env["todo"],
            env["ctx"],
            env["service"],
            {"llm": env["llm"], "context": env["context"]},
        )
        state = _worker_state()

        with (
            patch("agent.graph.get_archiver", return_value=None),
            patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
        ):
            first = await node(state)
            _apply_turn(state, first)
            await node(state)

        req1, req2 = [call.args[0] for call in env["llm"].ainvoke.call_args_list]

        def chars(messages) -> int:
            return sum(len(str(getattr(m, "content", ""))) for m in messages)

        new_messages = first["messages"][1:]  # response (+ any reminder)
        assert chars(req2) - chars(req1) == chars(new_messages)
        assert _count_text(req1, body) == 1
        assert _count_text(req2, body) == 1

        # [system, task, block] is the stable prefix; then the new messages;
        # then the unchanged transient tail.
        prefix = 3
        assert [m.content for m in req2[:prefix]] == [m.content for m in req1[:prefix]]
        assert is_protected_message(req1[prefix - 1])
        assert [m.content for m in req2[prefix : prefix + len(new_messages)]] == [
            m.content for m in new_messages
        ]
        assert [m.content for m in req2[prefix + len(new_messages) :]] == [
            m.content for m in req1[prefix:]
        ]

    @pytest.mark.asyncio
    async def test_phase_block_is_present_exactly_once_after_each_strategy(
        self, execute_env
    ):
        """Acceptance (a): over the same history, tool-result clearing,
        trimming and summarisation each leave exactly one phase block —
        and summarisation seats it right after the summary, before the
        kept window."""
        from agent.core.context import ContextConfig, ContextManager

        env = execute_env
        body = "RESEARCH PROCEDURE " * 40
        _bind_phase_start(env, body)
        node = _make_execute_node(
            env["config"],
            env["workspace"],
            env["todo"],
            env["ctx"],
            env["service"],
            {"llm": env["llm"], "context": env["context"]},
        )
        with (
            patch("agent.graph.get_archiver", return_value=None),
            patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
        ):
            first = await node(_worker_state())
        block = first["messages"][0]
        assert is_protected_message(block)
        block.id = "blk"  # what the reducer assigns on append

        # Grow the history the way the tools node would, after the block.
        history = [HumanMessage(content="start", id="h0"), block]
        for i in range(6):
            history.append(
                AIMessage(
                    content=f"step {i}",
                    id=f"a{i}",
                    tool_calls=[
                        {"name": "read_file", "args": {"path": f"f{i}"}, "id": f"c{i}"}
                    ],
                )
            )
            history.append(
                ToolMessage(
                    content=f"result {i} " + "z" * 300, tool_call_id=f"c{i}", id=f"t{i}"
                )
            )
        history.append(HumanMessage(content="continue", id="h1"))

        mgr = ContextManager(
            config=ContextConfig(
                compaction_threshold_tokens=500,
                summarization_threshold_tokens=500,
                keep_recent_messages=3,
                keep_recent_tool_results=2,
                model_max_context_tokens=4000,
            )
        )
        mgr.set_current_phase("tactical", phase_key="2:tactical")

        def protected(messages):
            return [m for m in messages if is_protected_message(m)]

        cleared = mgr.clear_old_tool_results(history)
        assert protected(cleared) == [block]
        assert cleared[1] is block

        trimmed = mgr.trim_messages(history, keep_recent=3)
        assert protected(trimmed) == [block]
        assert trimmed[1] is block  # after the task, before the window

        summarised = await mgr.summarize_and_compact(history, _mock_aux())
        kept = [m for m in summarised if not isinstance(m, RemoveMessage)]
        summary_idx = next(
            i
            for i, m in enumerate(kept)
            if isinstance(m, SystemMessage) and "[Summary of prior work]" in m.content
        )
        blocks = protected(kept)
        assert len(blocks) == 1
        assert kept.index(blocks[0]) == summary_idx + 1
        assert blocks[0].content == block.content
        assert blocks[0].additional_kwargs == block.additional_kwargs
        assert blocks[0].id is None
        assert "blk" in {m.id for m in summarised if isinstance(m, RemoveMessage)}
        assert [m.content for m in kept[summary_idx + 2 :]] == [
            m.content for m in history[-3:]
        ]

    @pytest.mark.asyncio
    async def test_phase_start_instruction_survives_emergency_compaction_retry(
        self, execute_env
    ):
        env = execute_env
        entry = InstructionFileEntry(
            trigger="phase_start:tactical",
            skill="research-guide",
            enforce=False,
        )
        env["config"].instruction_files = [entry]
        env["ctx"]._instruction_files = [entry]
        marker = "PHASE GUIDANCE MUST REACH THE SUCCESSFUL RETRY"
        env["workspace"].write_file(entry.path, marker)
        env["llm"].ainvoke = AsyncMock(
            side_effect=[
                ContextOverflowError(token_count=101, limit=100),
                AIMessage(content="done"),
            ]
        )
        node = _make_execute_node(
            env["config"],
            env["workspace"],
            env["todo"],
            env["ctx"],
            env["service"],
            {"llm": env["llm"], "context": env["context"]},
        )

        with (
            patch("agent.graph.get_archiver", return_value=None),
            patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
        ):
            result = await node(_worker_state())

        requests = [call.args[0] for call in env["llm"].ainvoke.call_args_list]
        assert len(requests) == 2
        for request in requests:
            assert any(
                marker in str(getattr(message, "content", "")) for message in request
            )
        assert len(result["phase_instruction_injections"]) == 1

    @pytest.mark.asyncio
    async def test_flag_on_read_write_wiring(self, execute_env):
        env = execute_env
        auditor = MagicMock()
        node = _make_execute_node(
            env["config"],
            env["workspace"],
            env["todo"],
            env["ctx"],
            env["service"],
            {"llm": env["llm"], "context": env["context"]},
        )

        with (
            patch("agent.graph.get_archiver", return_value=auditor),
            # The real prompt templates aren't under test (their Jinja
            # blocks trip get_phase_system_prompt's .format with the
            # bare default model) — the wiring is.
            patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
        ):
            result = await node(_worker_state())
        await asyncio.sleep(0)  # let the fire-and-forget capture task run

        service = env["service"]

        # -- assemble request: legacy worker query formation
        assert len(service.assemble_requests) == 1
        req = service.assemble_requests[0]
        assert req.query_text == "Do the task phase 2 tactical"
        assert req.task_frame.top_todo == "Do the task"
        assert req.task_frame.phase_number == 2
        assert req.task_frame.is_strategic is False
        assert req.model == env["config"].llm.model

        # -- transient block spliced at the tail (after the conversation, for
        # prompt-cache prefix stability): payload pairs first, todos message
        # LAST (query-at-end)
        prepared = env["llm"].ainvoke.call_args[0][0]
        todo_idx = next(
            i
            for i, m in enumerate(prepared)
            if isinstance(m, HumanMessage)
            and str(m.content).startswith(TODOS_INJECTION_CONTENT_PREFIX)
        )
        payload_msgs = service.payload.messages()
        assert todo_idx == len(prepared) - 1
        assert prepared[todo_idx - len(payload_msgs) : todo_idx] == payload_msgs

        # -- legacy direct-store path skipped entirely
        env["ctx"].recall_store.decrement_ttl.assert_not_called()
        env["ctx"].recall_store.retrieve.assert_not_called()

        # -- audit fed from the payload's memory block (+ stats tap)
        audit_calls = [
            c
            for c in auditor.audit_step.call_args_list
            if c.kwargs.get("step_type") == "memory_inject"
        ]
        assert len(audit_calls) == 1
        data = audit_calls[0].kwargs["data"]
        assert data["count"] == 2
        assert data["total_tokens"] == 26
        assert "stats" in data

        # -- one turn_end capture with the legacy fields
        assert [e.kind for e in service.captures] == ["turn_end"]
        event = service.captures[0]
        assert event.turn_count == 1
        assert event.phase == 2
        assert event.extra["current_injection_text"] == "MEMBLOCK"

        # -- interval state stays writer-internal (no checkpointed keys)
        assert "last_observed_turn" not in result
        assert "last_assembled_turn" not in result
        assert result["turn_count"] == 1

    @pytest.mark.asyncio
    async def test_flag_on_compaction_capture(self, execute_env):
        env = execute_env

        def compact(messages, mgr):
            mgr._state.summaries.append("COMPACTION-SUMMARY")
            return [RemoveMessage(id="rm-1")] + list(messages)

        env["context"] = FakeContextMgr(ensure_hook=compact)
        node = _make_execute_node(
            env["config"],
            env["workspace"],
            env["todo"],
            env["ctx"],
            env["service"],
            {"llm": env["llm"], "context": env["context"]},
        )

        with (
            patch("agent.graph.get_archiver", return_value=None),
            patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
        ):
            await node(_worker_state())
        await asyncio.sleep(0)

        kinds = [e.kind for e in env["service"].captures]
        assert kinds == ["compaction", "turn_end"]
        event = env["service"].captures[0]
        assert event.extra["summary"] == "COMPACTION-SUMMARY"
        assert event.phase == 2
        # Legacy inline store skipped
        env["ctx"].recall_store.store.assert_not_called()


# ---------------------------------------------------------------------------
# 4. Worker archive + tools node wiring
# ---------------------------------------------------------------------------


class TestArchiveNodeWiring:
    @pytest.mark.asyncio
    async def test_phase_boundary_capture(self, workspace_manager):
        from agent.graph import create_archive_phase_node

        todo_manager = TodoManager(workspace_manager)
        plan_manager = PlanManager(workspace_manager)
        plan_manager.write("## Phase 1: Test\n\n- [x] Task 1")

        config = MagicMock()
        config.agent_id = "test-agent"
        config.extra = {}
        config.context_management.compact_on_archive = False
        config.llm.reasoning_level = None

        service = RecordingManager()
        recall_spy = AsyncMock()
        node = create_archive_phase_node(
            todo_manager,
            plan_manager,
            config,
            MagicMock(),
            MagicMock(),
            "summarize",
            recall_store=recall_spy,
            memory_service=service,
        )

        messages = [HumanMessage(content="work happened")]
        state = {
            "job_id": "cutover-job-1",
            "messages": messages,
            "phase_number": 3,
            "is_strategic_phase": False,
        }
        await node(state)
        await asyncio.sleep(0)

        assert [e.kind for e in service.captures] == ["phase_boundary"]
        event = service.captures[0]
        assert event.phase == 3
        assert event.messages is messages
        # Legacy direct extraction skipped
        recall_spy.assert_not_called()


class TestAuditedToolsWiring:
    @pytest.mark.asyncio
    async def test_todo_complete_capture_drains_queue(
        self, worker_config, workspace_manager
    ):
        from agent.graph import create_audited_tool_node

        ctx = ToolContext(workspace_manager=workspace_manager)
        ctx.queue_memory(content="Queued insight", importance=0.7, source="todo")
        recall_spy = AsyncMock()
        service = RecordingManager()

        fake_tool = MagicMock()
        fake_tool.name = "fake_tool"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [
                        ToolMessage(content="ok", tool_call_id="c1", name="fake_tool")
                    ]
                }
            )
            MockToolNode.return_value = mock_tn

            node = create_audited_tool_node(
                [fake_tool],
                worker_config,
                recall_store=recall_spy,
                tool_context=ctx,
                memory_service=service,
            )
            state = {
                "job_id": "cutover-job-1",
                "iteration": 1,
                "messages": [
                    AIMessage(
                        content="",
                        tool_calls=[{"name": "fake_tool", "id": "c1", "args": {}}],
                    )
                ],
                "is_strategic_phase": False,
                "phase_number": 2,
                "metadata": {},
            }
            with patch("agent.graph.get_archiver", return_value=None):
                await node(state)

        assert [e.kind for e in service.captures] == ["todo_complete"]
        queued = service.captures[0].extra["queued_memories"]
        assert len(queued) == 1
        assert queued[0]["content"] == "Queued insight"
        # Queue drained at the call site; store untouched directly
        assert ctx._pending_memories == []
        recall_spy.store.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_queue_emits_nothing(self, worker_config, workspace_manager):
        from agent.graph import create_audited_tool_node

        ctx = ToolContext(workspace_manager=workspace_manager)
        service = RecordingManager()
        fake_tool = MagicMock()
        fake_tool.name = "fake_tool"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [
                        ToolMessage(content="ok", tool_call_id="c1", name="fake_tool")
                    ]
                }
            )
            MockToolNode.return_value = mock_tn

            node = create_audited_tool_node(
                [fake_tool],
                worker_config,
                recall_store=None,
                tool_context=ctx,
                memory_service=service,
            )
            state = {
                "job_id": "cutover-job-1",
                "iteration": 1,
                "messages": [
                    AIMessage(
                        content="",
                        tool_calls=[{"name": "fake_tool", "id": "c1", "args": {}}],
                    )
                ],
                "is_strategic_phase": False,
                "phase_number": 2,
                "metadata": {},
            }
            with patch("agent.graph.get_archiver", return_value=None):
                await node(state)

        assert service.captures == []


# ---------------------------------------------------------------------------
# 5. Persistent turn wiring (read swap + insertion position)
# ---------------------------------------------------------------------------


def _persistent_callbacks():
    from agent.persistent_graph import PersistentLoopCallbacks

    return PersistentLoopCallbacks(
        get_user_input=AsyncMock(return_value="hello"),
        on_token=AsyncMock(),
        on_thinking=AsyncMock(),
        on_tool_start=AsyncMock(),
        on_tool_result=AsyncMock(),
        permission_check=AsyncMock(return_value=True),
        on_turn_start=AsyncMock(),
        on_turn_complete=AsyncMock(),
        on_error=AsyncMock(),
        check_interrupt=lambda: None,
    )


class TestPersistentTurnWiring:
    @pytest.mark.asyncio
    async def test_flag_on_assemble_and_insertion(self):
        from agent.persistent_graph import _execute_turn

        captured_prepared: List[List[Any]] = []

        async def _astream(msgs, **kwargs):
            captured_prepared.append(list(msgs))
            yield AIMessage(content="ok")

        llm = AsyncMock()
        llm.astream = _astream

        config = MagicMock()
        config.llm.timeout = 600
        config.llm.model = "test-model"
        config.context_management.max_summary_length = 10000
        # Real QueryConfig — a bare MagicMock fabricates a truthy
        # memory.query.digest and routes the turn into the digest branch.
        config.memory.query = QueryConfig()

        service = RecordingManager(payload=make_payload())
        recall_spy = AsyncMock()
        kb_spy = AsyncMock()
        messages = [SystemMessage(content="sys"), HumanMessage(content="user query")]

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m),
                should_summarize=lambda *a, **k: False,  # sync gate, pre_compaction off
            ),
            messages=messages,
            callbacks=_persistent_callbacks(),
            llm_timeout=600,
            auxiliary_llm=None,
            config=config,
            recall_store=recall_spy,
            knowledge_store=kb_spy,
            project_id=str(PROJECT_ID),
            memory_service=service,
        )
        assert result.interrupted is False

        # -- assemble request: latest HumanMessage, main model threaded
        assert len(service.assemble_requests) == 1
        req = service.assemble_requests[0]
        assert req.query_text == "user query"
        assert req.model == "test-model"

        # -- payload anchored at the tail (after the last Human/Tool message —
        # here the sole HumanMessage), so the synthetic function-call pairs
        # always follow a real user turn (Gemini's native API 400s on a leading
        # functionCall) and the stable history prefix stays cache-reusable —
        # see _injection_anchor_index.
        prepared = captured_prepared[0]
        payload_msgs = service.payload.messages()
        assert isinstance(prepared[0], SystemMessage)
        assert prepared[1].content == "user query"
        assert prepared[2 : 2 + len(payload_msgs)] == payload_msgs

        # -- legacy direct-store blocks skipped
        recall_spy.decrement_ttl.assert_not_called()
        recall_spy.retrieve.assert_not_called()
        kb_spy.hybrid_search.assert_not_called()

    @pytest.mark.asyncio
    async def test_digest_flag_switches_query_formation(self):
        from agent.persistent_graph import _execute_turn

        async def _astream(msgs, **kwargs):
            yield AIMessage(content="ok")

        llm = AsyncMock()
        llm.astream = _astream

        config = MagicMock()
        config.llm.timeout = 600
        config.llm.model = "test-model"
        config.context_management.max_summary_length = 10000
        config.memory.query = QueryConfig(digest=True, digest_window=4)

        service = RecordingManager(payload=make_payload())
        messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="first ask"),
            AIMessage(content="did things"),
            HumanMessage(content="follow-up"),
        ]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m),
                should_summarize=lambda *a, **k: False,  # sync gate, pre_compaction off
            ),
            messages=messages,
            callbacks=_persistent_callbacks(),
            llm_timeout=600,
            auxiliary_llm=None,
            config=config,
            recall_store=AsyncMock(),
            knowledge_store=AsyncMock(),
            project_id=str(PROJECT_ID),
            memory_service=service,
        )

        # Digest = windowed conversation, not the bare last user message
        # (which is what the legacy builder would have produced).
        req = service.assemble_requests[0]
        assert req.query_text == "first ask\ndid things\nfollow-up"


# ---------------------------------------------------------------------------
# 6. Persistent teardown + B11 terminate wiring
# ---------------------------------------------------------------------------


def _archive_session(service: RecordingManager) -> MagicMock:
    session = MagicMock()
    session.memory_service = service
    session.final_memory_extracted = False
    session.messages = [HumanMessage(content="hi")]
    session.tool_context.recall_store = AsyncMock()
    session.auxiliary_llm = None
    session.postgres_conn = None
    session.workspace_sync = None
    session.quiesce_subagents = AsyncMock()
    session.resume_subagents = AsyncMock()
    return session


class TestTeardownWiring:
    @pytest.mark.asyncio
    async def test_archive_captures_session_end(self):
        from agent.api.persistent_app import _handle_archive

        service = RecordingManager()
        session = _archive_session(service)
        ws = AsyncMock()

        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch(
                "agent.api.persistent_app._update_thread_status",
                new=AsyncMock(return_value=True),
            ),
            patch(
                "agent.api.persistent_app._session_termination.terminate",
                new=AsyncMock(),
            ),
        ):
            await _handle_archive(ws)

        assert [e.kind for e in service.captures] == ["session_end"]
        assert service.captures[0].messages is session.messages
        assert session.final_memory_extracted is True
        ended = [
            c[0][0]
            for c in ws.send_json.call_args_list
            if c[0][0].get("method") == "session.ended"
        ]
        assert len(ended) == 1

    @pytest.mark.asyncio
    async def test_idle_archive_captures_idle_kind(self):
        from agent.api.persistent_app import _handle_idle_archive

        service = RecordingManager()
        session = _archive_session(service)
        session.workspace_manager = None

        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch(
                "agent.api.persistent_app._update_thread_status",
                new=AsyncMock(return_value=True),
            ),
            patch(
                "agent.api.persistent_app._session_termination.terminate",
                new=AsyncMock(),
            ),
        ):
            await _handle_idle_archive(None)

        assert [e.kind for e in service.captures] == ["idle_archive"]
        assert session.final_memory_extracted is True

    @pytest.mark.asyncio
    async def test_terminate_captures_session_end_once(self):
        """B11: detach-style endings now capture — exactly once."""
        from agent.api import persistent_app

        service = RecordingManager()
        session = MagicMock()
        session.memory_service = service
        session.final_memory_extracted = False
        session.messages = [HumanMessage(content="hi")]
        session.workspace_sync = None
        session.workspace_manager = None
        session.cleanup = AsyncMock()
        session.quiesce_subagents = AsyncMock()
        session.resume_subagents = AsyncMock()
        persistent_app._session = session
        persistent_app._session_identity._thread_id = "tid-b11-1"
        persistent_app._session_termination.terminating = False
        persistent_app._loop_task = None
        persistent_app._session_termination.max_sessions_per_process = 0

        with (
            patch.object(persistent_app, "_update_thread_status", new=AsyncMock()),
            patch.object(persistent_app._session_termination, "stop_watchdogs"),
        ):
            await persistent_app._session_termination.terminate("rest_detach")

        assert [e.kind for e in service.captures] == ["session_end"]
        assert persistent_app._session is None

    @pytest.mark.asyncio
    async def test_terminate_skips_when_archive_already_captured(self):
        """B11 guard: archive → terminate must not double-extract."""
        from agent.api import persistent_app
        from agent.api.persistent_app import _handle_archive

        service = RecordingManager()
        session = _archive_session(service)
        session.workspace_manager = None
        session.cleanup = AsyncMock()
        ws = AsyncMock()

        persistent_app._session = session
        persistent_app._session_identity._thread_id = "tid-b11-2"
        persistent_app._session_termination.terminating = False
        persistent_app._loop_task = None
        persistent_app._session_termination.max_sessions_per_process = 0

        with (
            patch.object(persistent_app, "_update_thread_status", new=AsyncMock()),
            patch.object(persistent_app._session_termination, "stop_watchdogs"),
        ):
            # _handle_archive reads the patched-in module globals directly
            with patch(
                "agent.api.persistent_app._session_identity._thread_id", "tid-b11-2"
            ):
                await _handle_archive(ws)
            await persistent_app._session_termination.terminate("loop_complete")

        # One capture total: archive's. Terminate honoured the guard flag.
        assert [e.kind for e in service.captures] == ["session_end"]
        assert persistent_app._session is None


# ---------------------------------------------------------------------------
# 7. B10 — strip recognition for everything the manager injects
# ---------------------------------------------------------------------------


class _ExoticRetriever:
    """A future-shaped retriever with an unrenderable kind."""

    async def retrieve(self, req):
        return [Candidate(kind="graph_paths", text="A→B", record=object())]


class TestStripRecognitionB10:
    @pytest.mark.asyncio
    async def test_all_injected_messages_are_strip_recognized(self):
        """Every message assemble() emits must be recognized by the
        summarization-exclusion predicate — compaction strips injected
        pairs and re-injects fresh ones, so an unrecognized injection
        would survive into summaries and duplicate forever."""
        runtime = MemoryRuntime(
            recall_store=make_recall_mock(make_memories()),
            knowledge_store=make_kb_mock(make_notes()),
            project_id=str(PROJECT_ID),
        )
        manager = MemoryManager(
            runtime,
            retrievers=[
                ("recall_two_tier", _make_recall_retriever(runtime)),
                ("kb_notes", _make_kb_retriever(runtime)),
            ],
        )
        from agent.services.memory import AssembleRequest

        payload = await manager.assemble(
            AssembleRequest(query_text="anything", model=None)
        )

        msgs = payload.messages()
        assert len(msgs) == 4  # memory pair + knowledge pair
        for msg in msgs:
            assert is_workspace_injection_message(msg), (
                f"Injection message not strip-recognized: {msg!r}"
            )

    @pytest.mark.asyncio
    async def test_unrenderable_kinds_contribute_no_messages(self):
        """An exotic candidate kind yields a provenance-only block — the
        payload still contains only strip-recognized messages."""
        manager = MemoryManager(
            MemoryRuntime(), retrievers=[("exotic", _ExoticRetriever())]
        )
        from agent.services.memory import AssembleRequest

        payload = await manager.assemble(AssembleRequest(query_text="q"))
        assert payload.stats.candidates_total == 1
        assert payload.messages() == []


def _make_recall_retriever(runtime):
    from agent.services.memory.plugins.legacy import RecallTwoTierRetriever

    return RecallTwoTierRetriever(runtime.recall_store)


def _make_kb_retriever(runtime):
    from agent.services.memory.plugins.legacy import KbNotesRetriever

    return KbNotesRetriever(runtime.knowledge_store, project_id=runtime.project_id)


# ---------------------------------------------------------------------------
# U2 WP2: phase skills as phase_start blocks and the DB addendum
# ---------------------------------------------------------------------------

_TACTICAL_SKILL_MD = (
    "---\n"
    "name: tactical-phase\n"
    "description: test body\n"
    "catalog: hidden\n"
    "---\n\n"
    "# Tactical phase\n\n"
    "You are in TACTICAL mode. UNIQUE TACTICAL SKILL BODY\n"
)


def _bind_tactical_phase_skill(env, *, also_research_guide: bool = False):
    entries = [
        InstructionFileEntry(
            trigger="phase_start:tactical", skill="tactical-phase", enforce=False
        )
    ]
    env["workspace"].write_file("skills/tactical-phase/SKILL.md", _TACTICAL_SKILL_MD)
    if also_research_guide:
        guide = InstructionFileEntry(
            trigger="phase_start:tactical", skill="research-guide", enforce=False
        )
        env["workspace"].write_file(guide.path, "RESEARCH GUIDE BODY")
        entries.append(guide)
    env["config"].instruction_files = entries
    env["ctx"]._instruction_files = entries
    return entries


class TestPhaseSkillBlocks:
    @pytest.mark.asyncio
    async def test_bound_skill_block_delivers_the_body_without_frontmatter(
        self, execute_env
    ):
        """A bound skill delivers its instructions, not its catalog frontmatter;
        the factory's [phase: ...] header is the only header."""
        from shared.runtime.core.message_markers import protected_path

        env = execute_env
        _bind_tactical_phase_skill(env)
        node = _make_execute_node(
            env["config"],
            env["workspace"],
            env["todo"],
            env["ctx"],
            env["service"],
            {"llm": env["llm"], "context": env["context"]},
        )
        state = _worker_state()
        with (
            patch("agent.graph.get_archiver", return_value=None),
            patch("agent.graph.get_phase_system_prompt", return_value="ONE-SYS"),
        ):
            first = await node(state)

        block = first["messages"][0]
        assert is_protected_message(block)
        assert protected_path(block) == "skills/tactical-phase/SKILL.md"
        assert block.content.startswith(
            "[phase: tactical] Phase instructions (from skills/tactical-phase/SKILL.md)"
        )
        assert "UNIQUE TACTICAL SKILL BODY" in block.content
        assert "catalog: hidden" not in block.content
        assert "name: tactical-phase" not in block.content
        assert block.content.count("[phase:") == 1
        assert "<expert_workflow" not in block.content  # no DB addendum here
        # Skills mode: the ONE phase-agnostic prompt heads the request.
        request = env["llm"].ainvoke.call_args_list[0].args[0]
        assert request[0].content == "ONE-SYS"

    @pytest.mark.asyncio
    async def test_db_phase_addendum_rides_inside_the_phase_block(self, execute_env):
        """A DB expert's own tactical prompt is fenced (<expert_workflow>) and
        appended INSIDE the tactical-phase block — one protected identity per
        path, delivered once, brace-safe."""
        env = execute_env
        _bind_tactical_phase_skill(env)
        env["config"].extra["_db_prompt_keys"] = ["tactical"]
        env["config"].extra["_resolved_prompts"] = {
            "tactical": 'FORK TACTICAL RULE {"json": true}',
        }
        node = _make_execute_node(
            env["config"],
            env["workspace"],
            env["todo"],
            env["ctx"],
            env["service"],
            {"llm": env["llm"], "context": env["context"]},
        )
        state = _worker_state()
        with (
            patch("agent.graph.get_archiver", return_value=None),
            patch("agent.graph.get_phase_system_prompt", return_value="ONE-SYS"),
        ):
            first = await node(state)
            _apply_turn(state, first)
            second = await node(state)

        blocks = [m for m in first["messages"] if is_protected_message(m)]
        assert len(blocks) == 1
        content = blocks[0].content
        assert content.index("UNIQUE TACTICAL SKILL BODY") < content.index(
            "<expert_workflow"
        )
        assert "FORK TACTICAL RULE" in content
        assert (
            '"json": true' in content
            and "{" not in content.split("<expert_workflow")[1]
        )
        assert content.count("<expert_workflow") == 1
        assert first["phase_instruction_injections"] == [
            "2:tactical:skills/tactical-phase/SKILL.md"
        ]
        # Second turn: the addendum is in history, nothing is re-delivered.
        assert not any(is_protected_message(m) for m in second["messages"])
        requests = [c.args[0] for c in env["llm"].ainvoke.call_args_list]
        for request in requests:
            assert _count_text(request, "FORK TACTICAL RULE") == 1
            assert _count_text(request, "<expert_workflow") == 1
