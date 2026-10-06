"""The up-front memory summary (append-only context injection, D35).

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(D11, D21, D25, D34, D35). One small block goes in up front: how many
memories the project holds, by type, its most frequent topics and a one-line
hint to call ``memory_search``. It is computed once at conversation start,
never changes per turn and is given again only after a compaction removed
it. The planner side (append-if-absent) is pinned in
test_context_injection_planner.py; the SQL against a real server in
test_memory_summary_real_postgres.py; the prefix gate's non-vacuity in
test_prompt_cache_prefix_invariance.py. Here:

- the renderer (static text, the D35 contents);
- ``RecallStore.summary_stats`` against a recording database (two reads,
  nothing written);
- the per-conversation loader (only with ``memory_search`` bound, once per
  manager, nothing while the history holds it, failures never fatal);
- the worker and session wiring: in the first append_only request, folded
  into the carrier, exactly once across a run, re-appended after a
  compaction from the cached body, never re-read or re-sent after a resume
  or a restore (also through the real thread_messages round trip).
"""

from __future__ import annotations

import asyncio
import gc
import logging
import uuid
import weakref
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    RemoveMessage,
    SystemMessage,
    ToolMessage,
)

import agent.core.memory_summary as summary_module
from agent.core.context_injection import ContextSources, plan_context_entries
from agent.core.memory_summary import (
    conversation_memory_summary,
    memory_summary_enabled,
)
from shared.runtime.core.context_entries import (
    FOLDED_KEY,
    MEMORY_SUMMARY_KIND,
    entry_body,
    entry_kind,
    entry_meta,
    is_context_entry,
    make_context_entry,
)
from shared.runtime.services.recall_store import MemorySummaryStats, RecallStore
from shared.tool_catalog.names import MEMORY_SEARCH_TOOL_NAME
from tests import _prompt_cache_prefix_harness as harness
from tests._fake_chat_model import FakeChatModel, text_turn, tool_turn

STATS = MemorySummaryStats(
    total=42,
    by_type=(
        ("factual", 25),
        ("procedural", 9),
        ("error_solution", 5),
        ("vocabulary", 2),
        ("relational", 1),
    ),
    topics=("postgres", "helm", "k3d", "migrations", "keycloak", "tilt", "auth"),
)
BODY = (
    "Project memory overview (harness context, not a user message): 42 memories "
    "from earlier jobs and sessions.\n"
    "By type: 25 factual, 9 procedural, 5 error_solution, 2 vocabulary, "
    "1 relational.\n"
    "Frequent topics: postgres, helm, k3d, migrations, keycloak, tilt, auth.\n"
    "Relevant memories arrive on their own as context. To look up anything "
    "else from earlier work, such as a past decision, a fix or a preference, "
    "call memory_search with a short query."
)
GROWN = MemorySummaryStats(total=43, by_type=(("factual", 43),), topics=("grown",))
SRW_SUMMARY = f'<srw_context kind="{MEMORY_SUMMARY_KIND}">'
BOUND = ["read_file", MEMORY_SEARCH_TOOL_NAME]
MODEL = "gpt-5.6-sol"


class Store:
    """The summary read of a recall store: counted, optionally failing."""

    def __init__(
        self,
        stats: MemorySummaryStats = STATS,
        *,
        error: Optional[BaseException] = None,
        delay: float = 0.0,
    ) -> None:
        self.stats = stats
        self.error = error
        self.delay = delay
        self.calls = 0

    async def summary_stats(self, **_kwargs: Any) -> MemorySummaryStats:
        self.calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return self.stats


class Manager:
    """The part of a MemoryManager the loader reads: ``runtime.recall_store``."""

    def __init__(self, store: Any) -> None:
        self.runtime = SimpleNamespace(recall_store=store)


def _history() -> List[BaseMessage]:
    return [SystemMessage(content="system"), HumanMessage(content="Do the task.")]


def _summary_entry(body: str = BODY, turn: Optional[int] = None) -> HumanMessage:
    return make_context_entry(
        MEMORY_SUMMARY_KIND, body, section=MEMORY_SUMMARY_KIND, turn=turn
    )


async def _load(manager: Any, messages=None, tool_names: Any = BOUND) -> str:
    return await conversation_memory_summary(
        manager,
        _history() if messages is None else messages,
        tool_names=tool_names,
        model=None,
    )


def _count(request: List[BaseMessage]) -> int:
    return sum(str(m.content).count(SRW_SUMMARY) for m in request)


# --- The body -------------------------------------------------------------------


class TestRender:
    def test_counts_types_topics_and_the_hint(self):
        assert RecallStore.render_memory_summary(STATS) == BODY

    def test_the_same_stats_render_the_same_bytes(self):
        assert RecallStore.render_memory_summary(
            STATS
        ) == RecallStore.render_memory_summary(
            MemorySummaryStats(STATS.total, STATS.by_type, STATS.topics)
        )

    def test_one_memory_is_singular(self):
        body = RecallStore.render_memory_summary(
            MemorySummaryStats(total=1, by_type=(("factual", 1),), topics=("x",))
        )
        assert "): 1 memory from earlier jobs and sessions." in body

    def test_without_topics_the_topic_line_is_left_out(self):
        body = RecallStore.render_memory_summary(
            MemorySummaryStats(total=2, by_type=(("factual", 2),))
        )
        assert "Frequent topics" not in body
        assert body.splitlines()[1] == "By type: 2 factual."
        assert body.splitlines()[-1].startswith("Relevant memories arrive")

    def test_an_empty_scope_renders_nothing(self):
        assert RecallStore.render_memory_summary(MemorySummaryStats()) == ""


# --- The read ---------------------------------------------------------------------


class RecordingDB:
    """Answers the two aggregate reads; has no write methods at all."""

    def __init__(self, type_rows: List[dict], topic_rows: List[dict]) -> None:
        self.type_rows = type_rows
        self.topic_rows = topic_rows
        self.queries: List[tuple] = []

    async def fetch(self, query: str, *params: Any) -> List[dict]:
        self.queries.append((" ".join(query.split()), params))
        if "GROUP BY memory_type" in query:
            return self.type_rows
        return self.topic_rows


def _store(db: RecordingDB, project_id: uuid.UUID) -> RecallStore:
    return RecallStore(
        db=db,
        embedding_service=None,
        job_id=uuid.uuid4(),
        config=SimpleNamespace(project_scoped=True, retrieval_importance_floor=0.4),
        project_id=project_id,
    )


class TestSummaryStats:
    @pytest.mark.asyncio
    async def test_counts_by_type_and_topics(self):
        project = uuid.uuid4()
        db = RecordingDB(
            type_rows=[
                {"memory_type": "procedural", "n": 3},
                {"memory_type": "factual", "n": 2},
                {"memory_type": None, "n": 1},  # pre-constraint row: factual
                {"memory_type": "relational", "n": 1},
            ],
            topic_rows=[{"topic": "deploy", "n": 3}, {"topic": "helm", "n": 1}],
        )

        stats = await _store(db, project).summary_stats()

        assert stats == MemorySummaryStats(
            total=7,
            # Largest first; the 3-3 tie breaks by type name.
            by_type=(("factual", 3), ("procedural", 3), ("relational", 1)),
            topics=("deploy", "helm"),
        )
        (type_query, type_params), (topic_query, topic_params) = db.queries
        assert "project_id = $1" in type_query
        assert "valid_to IS NULL AND importance >= $2" in type_query
        assert type_params == (project, 0.4)
        assert "unnest(keywords)" in topic_query
        assert 'ORDER BY n DESC, topic COLLATE "C"' in topic_query
        assert topic_params == (project, 0.4, 8, 40)

    @pytest.mark.asyncio
    async def test_the_topic_count_is_a_parameter(self):
        db = RecordingDB([{"memory_type": "factual", "n": 1}], [])
        await _store(db, uuid.uuid4()).summary_stats(max_topics=5)
        assert db.queries[1][1][2] == 5

    @pytest.mark.asyncio
    async def test_an_empty_scope_reads_no_topics(self):
        db = RecordingDB(type_rows=[], topic_rows=[{"topic": "never", "n": 1}])
        assert await _store(db, uuid.uuid4()).summary_stats() == MemorySummaryStats()
        assert len(db.queries) == 1


# --- Loaded once per conversation ---------------------------------------------------


class TestLoader:
    @pytest.mark.asyncio
    async def test_loaded_once_per_manager(self):
        store = Store()
        manager = Manager(store)

        bodies = [await _load(manager) for _ in range(4)]

        assert bodies == [BODY] * 4
        assert store.calls == 1

    @pytest.mark.parametrize(
        "tool_names,expected",
        [
            (BOUND, True),
            ({"read_file": object(), MEMORY_SEARCH_TOOL_NAME: object()}, True),
            (["read_file"], False),
            ({}, False),
            (None, False),
        ],
        ids=["names", "tool-map", "unbound", "empty-map", "none"],
    )
    def test_only_with_memory_search_bound(self, tool_names, expected):
        assert memory_summary_enabled(Manager(Store()), tool_names) is expected

    @pytest.mark.asyncio
    async def test_no_tool_or_no_manager_means_no_summary_and_no_read(self):
        store = Store()
        assert await _load(Manager(store), tool_names=["read_file"]) == ""
        assert await _load(None) == ""
        assert store.calls == 0

    @pytest.mark.asyncio
    async def test_a_history_that_holds_it_reads_nothing(self):
        """A resumed worker or a restored session already has it: no query,
        and the planner appends nothing (append-if-absent)."""
        store = Store(GROWN)
        manager = Manager(store)
        history = _history() + [_summary_entry()]

        assert await _load(manager, history) == ""
        assert store.calls == 0
        planned = plan_context_entries(
            history, ContextSources(memory_summary=""), model=None, max_memories=5
        )
        assert planned.entries == []

    @pytest.mark.asyncio
    async def test_evicted_later_it_is_loaded_once_then(self):
        """That runtime loads a body only once a compaction removed it."""
        store = Store(GROWN)
        manager = Manager(store)
        assert await _load(manager, _history() + [_summary_entry()]) == ""

        body = await _load(manager, _history())
        assert body == RecallStore.render_memory_summary(GROWN)
        assert await _load(manager, _history()) == body
        assert store.calls == 1

    @pytest.mark.asyncio
    async def test_after_compaction_the_cached_body_comes_back_unread(self):
        store = Store()
        manager = Manager(store)
        history = _history()
        body = await _load(manager, history)
        planned = plan_context_entries(
            history, ContextSources(memory_summary=body), model=None, max_memories=5
        )
        history += planned.entries
        store.stats = GROWN  # memory grows mid-conversation

        assert await _load(manager, history) == BODY
        compacted = [history[0], HumanMessage(content="[summary] earlier work")]
        again = await _load(manager, compacted)

        assert again == BODY
        assert store.calls == 1
        (entry,) = plan_context_entries(
            compacted, ContextSources(memory_summary=again), model=None, max_memories=5
        ).entries
        assert entry_body(entry) == BODY

    @pytest.mark.asyncio
    async def test_a_failed_read_leaves_no_summary_and_is_not_retried(self, caplog):
        store = Store(error=RuntimeError("vector db down"))
        manager = Manager(store)

        with caplog.at_level(logging.WARNING, logger=summary_module.__name__):
            assert await _load(manager) == ""
            assert await _load(manager) == ""

        assert store.calls == 1
        assert "Memory summary unavailable" in caplog.text
        assert "RuntimeError: vector db down" in caplog.text

    @pytest.mark.asyncio
    async def test_a_slow_read_times_out(self, monkeypatch, caplog):
        monkeypatch.setattr(summary_module, "MEMORY_SUMMARY_TIMEOUT_S", 0.01)
        store = Store(delay=5)

        with caplog.at_level(logging.WARNING, logger=summary_module.__name__):
            assert await asyncio.wait_for(_load(Manager(store)), timeout=2) == ""

        assert "TimeoutError" in caplog.text

    @pytest.mark.asyncio
    async def test_an_empty_project_has_no_summary(self):
        store = Store(MemorySummaryStats())
        manager = Manager(store)
        assert await _load(manager) == ""
        assert await _load(manager) == ""
        assert store.calls == 1

    @pytest.mark.asyncio
    async def test_without_a_recall_store_there_is_none(self):
        assert await _load(Manager(None)) == ""

    @pytest.mark.asyncio
    async def test_the_cache_does_not_keep_the_manager_alive(self):
        store = Store()
        manager = Manager(store)
        await _load(manager)
        ref = weakref.ref(manager)

        del manager
        gc.collect()

        assert ref() is None
        assert await _load(Manager(store)) == BODY
        assert store.calls == 2  # a new runtime loads once more

    @pytest.mark.asyncio
    async def test_a_manager_without_weak_references_still_works(self):
        store = Store()
        manager = SimpleNamespace(runtime=SimpleNamespace(recall_store=store))
        assert await _load(manager) == BODY
        assert await _load(manager, _history() + [_summary_entry()]) == ""


# --- Worker wiring (graph.py execute) ---------------------------------------------------


class CapturingLLM:
    """Bound-LLM stand-in: captures each request, answers from a script."""

    def __init__(self, responses: List[AIMessage]) -> None:
        self.kwargs: Dict[str, Any] = {"tools": []}
        self.requests: List[List[BaseMessage]] = []
        self._responses = list(responses)

    async def ainvoke(self, prepared, **kwargs):
        self.requests.append(list(prepared))
        return self._responses.pop(0) if self._responses else AIMessage("done")


class CompactingContextMgr:
    """Pass-through, or (``compact``) summary + the newest call and result."""

    def __init__(self) -> None:
        self.config = SimpleNamespace(
            compaction_threshold_tokens=100_000,
            summarization_threshold_tokens=100_000,
            keep_recent_messages=10,
        )
        self._state = SimpleNamespace(summaries=[])
        self.compact = False

    def set_current_phase(self, phase: str, phase_key: Optional[str] = None) -> None:
        pass

    def should_summarize(self, messages) -> bool:
        return False

    def get_token_count(self, messages: List[Any]) -> int:
        return sum(len(str(getattr(m, "content", ""))) // 4 for m in messages)

    async def ensure_within_limits(self, messages, *args, **kwargs):
        if not self.compact:
            return messages
        # Keep the newest call and its result; summarize everything before.
        summary = SystemMessage(content="[Summary of prior work]\nEarlier work.")
        markers = [RemoveMessage(id=m.id) for m in messages[:-2] if m.id]
        return markers + [summary, *messages[-2:]]


def _worker_node(
    tmp_path,
    llm: CapturingLLM,
    *,
    memory_service: Any,
    tool_names: Optional[List[str]] = BOUND,
    mode: str = "append_only",
    context_mgr: Optional[CompactingContextMgr] = None,
):
    from agent.core.workspace import WorkspaceManager
    from agent.graph import create_execute_node
    from agent.managers import TodoManager
    from agent.tools.context import ToolContext
    from shared.runtime.core.loader import load_agent_config
    from tests._fs_backend import FilesystemTestBackend

    workspace = WorkspaceManager(
        job_id=harness.JOB_ID,
        base_path=tmp_path,
        backend=FilesystemTestBackend(tmp_path),
    )
    workspace.initialize()
    config = load_agent_config(harness.WORKER_CONFIG_PATH)
    config.context_management.injection_mode = mode
    todo = TodoManager(workspace)
    todo.add("Do the task")
    ctx = ToolContext(workspace_manager=workspace)
    ctx.knowledge_store = None
    return create_execute_node(
        llm_with_tools=llm,
        todo_manager=todo,
        memory_manager=MagicMock(),
        workspace_manager=workspace,
        config=config,
        context_mgr=context_mgr or CompactingContextMgr(),
        retry_manager=MagicMock(),
        auxiliary_llm=MagicMock(),
        summarization_prompt="summarize",
        tool_context=ctx,
        tool_names=tool_names,
        memory_service=memory_service,
    )


def _state(messages: List[BaseMessage]) -> Dict[str, Any]:
    return {
        "job_id": harness.JOB_ID,
        "iteration": 0,
        "messages": messages,
        "is_strategic_phase": False,
        "phase_number": 2,
        "turn_count": 0,
        "metadata": {},
    }


def _call(call_id: str) -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[{"name": "read_file", "args": {"path": "a.md"}, "id": call_id}],
    )


async def _execute(node, state: Dict[str, Any], prefix: str) -> List[BaseMessage]:
    """One execute turn plus what the graph does after it (the reducer)."""
    with (
        patch("agent.graph.get_phase_system_prompt", return_value="SYSTEM"),
        patch("agent.graph.get_archiver", return_value=None),
    ):
        result = await node(state)
    assert not result.get("error"), result.get("error")
    new = [
        m if m.id else m.model_copy(update={"id": f"{prefix}{i}"})
        for i, m in enumerate(result["messages"])
    ]
    removed = {m.id for m in new if isinstance(m, RemoveMessage)}
    state["messages"] = [m for m in state["messages"] if m.id not in removed] + [
        m for m in new if not isinstance(m, RemoveMessage)
    ]
    state["iteration"] = result["iteration"]
    state["turn_count"] = result["turn_count"]
    return new


def _with_ids(messages: List[BaseMessage]) -> List[BaseMessage]:
    return [m.model_copy(update={"id": f"h{i}"}) for i, m in enumerate(messages)]


class TestWorker:
    @pytest.mark.asyncio
    async def test_first_request_carries_it_once_and_later_ones_keep_it(self, tmp_path):
        memory = harness.RecordingMemoryManager()
        llm = CapturingLLM([_call("c1"), _call("c2"), AIMessage("done")])
        node = _worker_node(tmp_path, llm, memory_service=memory)
        state = _state(_with_ids([HumanMessage(content="Start the task.")]))

        first = await _execute(node, state, "t1-")
        state["messages"].append(ToolMessage(content="a", tool_call_id="c1"))
        await _execute(node, state, "t2-")
        state["messages"].append(ToolMessage(content="a", tool_call_id="c2"))
        await _execute(node, state, "t3-")

        assert [_count(r) for r in llm.requests] == [1, 1, 1]
        assert memory.runtime.recall_store.calls == 1
        # Folded into the carrier (the task message), right after the charter
        # slot and before the pushed memories; it is history from then on.
        carrier = llm.requests[0][-1]
        assert isinstance(carrier, HumanMessage)
        assert carrier.content.startswith("Start the task.")
        assert carrier.additional_kwargs[FOLDED_KEY][0] == MEMORY_SUMMARY_KIND
        entries = [m for m in first if is_context_entry(m)]
        assert entry_kind(entries[0]) == MEMORY_SUMMARY_KIND
        topics = ", ".join(harness.MEMORY_SUMMARY_TOPICS)
        assert f"Frequent topics: {topics}." in entry_body(entries[0])
        assert entry_meta(entries[0])["turn"] is None

    @pytest.mark.asyncio
    async def test_compaction_evicts_it_and_the_cached_body_goes_in_again(
        self, tmp_path
    ):
        memory = harness.RecordingMemoryManager()
        ctx = CompactingContextMgr()
        llm = CapturingLLM([_call("c1"), AIMessage("done")])
        node = _worker_node(tmp_path, llm, memory_service=memory, context_mgr=ctx)
        state = _state(_with_ids([HumanMessage(content="Start the task.")]))

        first = await _execute(node, state, "t1-")
        state["messages"].append(
            ToolMessage(content="a", tool_call_id="c1", id="tool-1")
        )
        memory.runtime.recall_store.stats = GROWN  # memory grew meanwhile
        ctx.compact = True
        second = await _execute(node, state, "t2-")

        (before,) = [m for m in first if entry_kind(m) == MEMORY_SUMMARY_KIND]
        removed = {m.id for m in second if isinstance(m, RemoveMessage)}
        assert before.id in removed
        (after,) = [m for m in second if entry_kind(m) == MEMORY_SUMMARY_KIND]
        assert after.content == before.content  # the cached body, not GROWN
        assert memory.runtime.recall_store.calls == 1
        assert _count(llm.requests[-1]) == 1

    @pytest.mark.asyncio
    async def test_a_resumed_job_neither_reads_nor_resends_it(self, tmp_path):
        """The checkpoint holds the entry: a new process (new manager) whose
        store would answer differently reads nothing and appends nothing."""
        memory = harness.RecordingMemoryManager()
        memory.runtime.recall_store.stats = GROWN
        llm = CapturingLLM([AIMessage("done")])
        node = _worker_node(tmp_path, llm, memory_service=memory)
        history = _with_ids(
            [HumanMessage(content="Start the task."), _summary_entry(), _call("c1")]
        ) + [ToolMessage(content="a", tool_call_id="c1", id="tool-1")]

        new = await _execute(node, _state(history), "t1-")

        assert memory.runtime.recall_store.calls == 0
        assert not any(entry_kind(m) == MEMORY_SUMMARY_KIND for m in new)
        assert _count(llm.requests[0]) == 1
        assert "Frequent topics: grown." not in str(llm.requests[0])

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "tool_names,mode",
        [(None, "append_only"), (["read_file"], "append_only"), (BOUND, "legacy")],
        ids=["no-tools", "memory_search-unbound", "legacy"],
    )
    async def test_none_without_the_tool_or_in_legacy(self, tmp_path, tool_names, mode):
        memory = harness.RecordingMemoryManager()
        llm = CapturingLLM([AIMessage("done")])
        node = _worker_node(
            tmp_path, llm, memory_service=memory, tool_names=tool_names, mode=mode
        )

        await _execute(node, _state(_with_ids([HumanMessage("Go.")])), "t1-")

        assert _count(llm.requests[0]) == 0
        assert memory.runtime.recall_store.calls == 0

    @pytest.mark.asyncio
    async def test_without_memory_there_is_none(self, tmp_path):
        llm = CapturingLLM([AIMessage("done")])
        node = _worker_node(tmp_path, llm, memory_service=None)
        await _execute(node, _state(_with_ids([HumanMessage("Go.")])), "t1-")
        assert _count(llm.requests[0]) == 0


@pytest.mark.filterwarnings(
    "ignore:Parameters .*reasoning_effort.* should be specified explicitly:UserWarning"
)
class TestWorkerOnTheWire:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("memory_search", [True, False])
    async def test_every_request_of_the_tool_loop(
        self, memory_search, tmp_path, monkeypatch
    ):
        """The prefix-gate worker run: exactly once in every request with the
        tool bound (the runner checks the single read), never without."""
        requests = await harness.run_worker_scenario(
            harness.FAMILIES["openai-chat"],
            sources=harness.VARIANTS["injected"],
            workdir=tmp_path,
            monkeypatch=monkeypatch,
            injection_mode="append_only",
            memory_search=memory_search,
        )
        expected = int(memory_search)
        assert [harness.text_occurrences(r.body, SRW_SUMMARY) for r in requests] == [
            expected
        ] * len(requests)


# --- Session wiring (persistent_graph.py _execute_turn) ---------------------------------


class Session:
    """The real session loop with charter, memory and the App Guide boundary."""

    def __init__(
        self,
        script: List[Any],
        *,
        memory_search: bool = True,
        mode: str = "append_only",
        compact_at: Optional[int] = None,
    ) -> None:
        self.llm = FakeChatModel(script)
        self.config = harness._session_config(
            injections=True, model=MODEL, injection_mode=mode
        )
        names = [t["function"]["name"] for t in harness.SESSION_TOOLS]
        if memory_search:
            names.append(MEMORY_SEARCH_TOOL_NAME)
        self.tools = [harness._session_tool(name) for name in names]
        self.memory = harness.RecordingMemoryManager()
        self.knowledge_store = MagicMock()
        self.knowledge_store.get_charter_note = AsyncMock(return_value=harness.CHARTER)
        self.persisted: List[BaseMessage] = []
        self.errors: List[Any] = []
        self.context_manager = AsyncMock()
        self.context_manager.should_summarize = MagicMock(return_value=False)
        self.context_manager.config.keep_recent_messages = 10
        self.context_manager.record_provider_usage = MagicMock()
        self.context_manager.compaction_runs = 0
        calls = {"n": 0}

        async def ensure(messages, *_args, **_kwargs):
            calls["n"] += 1
            if calls["n"] != compact_at:
                return messages
            # The summary replaces everything before the newest tool batch;
            # a counted run, so the loop adopts it into the durable history.
            self.context_manager.compaction_runs += 1
            return [
                messages[0],
                SystemMessage(content="[Summary of prior work]\nrecap"),
                *messages[-2:],
            ]

        self.context_manager.ensure_within_limits = ensure

    async def _persist(self, msg: Any) -> bool:
        self.persisted.append(msg)
        return True

    async def _on_error(self, *args: Any, **kwargs: Any) -> None:
        self.errors.append((args, kwargs))

    async def run(self, messages: List[BaseMessage], *inputs: str) -> None:
        from agent.persistent_graph import PersistentLoopCallbacks, run_persistent_loop

        callbacks = PersistentLoopCallbacks(
            get_user_input=AsyncMock(side_effect=[*inputs, asyncio.CancelledError()]),
            on_token=AsyncMock(),
            on_thinking=AsyncMock(),
            on_tool_start=AsyncMock(),
            on_tool_result=AsyncMock(),
            permission_check=AsyncMock(return_value=True),
            on_turn_start=AsyncMock(),
            on_turn_complete=AsyncMock(),
            on_error=self._on_error,
            check_interrupt=MagicMock(return_value=None),
            persist_message=self._persist,
        )
        await run_persistent_loop(
            llm_with_tools=self.llm,
            tools=self.tools,
            context_manager=self.context_manager,
            config=self.config,
            system_prompt="You are the SRW session under test.",
            callbacks=callbacks,
            messages=messages,
            knowledge_store=self.knowledge_store,
            project_ids=[harness.PROJECT_ID],
            tool_context=SimpleNamespace(knowledge_bindings=[], citation_engine=None),
            memory_service=self.memory,
        )
        assert self.errors == []

    @property
    def reads(self) -> int:
        return self.memory.runtime.recall_store.calls


class TestSession:
    @pytest.mark.asyncio
    async def test_first_turn_after_the_charter_then_never_again(self):
        session = Session(
            [
                tool_turn("read_file", {"path": "notes.md"}, "c1"),
                text_turn("Answer one."),
                text_turn("Answer two."),
            ]
        )
        messages: List[BaseMessage] = []

        await session.run(messages, "Turn one?", "Turn two?")

        entries = [m for m in messages if is_context_entry(m)]
        assert [entry_kind(e) for e in entries] == [
            "charter",
            MEMORY_SUMMARY_KIND,
            "memory",
            "knowledge",
            "turn_boundary",
            "turn_boundary",
        ]
        summary = entries[1]
        assert entry_meta(summary)["turn"] == 1
        # Persisted as a context row like every entry, before the provider.
        assert summary in session.persisted
        assert [_count(call) for call in session.llm.calls] == [1, 1, 1]
        assert session.reads == 1

    @pytest.mark.asyncio
    async def test_a_mid_turn_compaction_re_appends_the_cached_body(self):
        session = Session(
            [
                tool_turn("read_file", {"path": "a"}, "c1"),
                tool_turn("read_file", {"path": "b"}, "c2"),
                text_turn("done"),
            ],
            compact_at=2,
        )
        messages: List[BaseMessage] = []

        await session.run(messages, "Read both.")

        summaries = [
            m for m in session.persisted if entry_kind(m) == MEMORY_SUMMARY_KIND
        ]
        assert len(summaries) == 2
        assert summaries[0].content == summaries[1].content
        assert [_count(call) for call in session.llm.calls] == [1, 1, 1]
        assert session.reads == 1

    @pytest.mark.asyncio
    async def test_a_restored_history_neither_reads_nor_resends_it(self):
        session = Session([text_turn("Answer.")])
        session.memory.runtime.recall_store.stats = GROWN
        restored: List[BaseMessage] = [
            HumanMessage(content="Earlier question.", id="h1"),
            _summary_entry(turn=1).model_copy(update={"id": "ctx-1"}),
            AIMessage(content="Earlier answer.", id="a1"),
        ]

        await session.run(restored, "Next question?")

        assert session.reads == 0
        assert not any(entry_kind(m) == MEMORY_SUMMARY_KIND for m in session.persisted)
        (call,) = session.llm.calls
        assert _count(call) == 1
        assert BODY in "\n".join(str(m.content) for m in call)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "memory_search,mode",
        [(False, "append_only"), (True, "legacy")],
        ids=["memory_search-unbound", "legacy"],
    )
    async def test_none_without_the_tool_or_in_legacy(self, memory_search, mode):
        session = Session(
            [text_turn("Answer.")], memory_search=memory_search, mode=mode
        )
        messages: List[BaseMessage] = []

        await session.run(messages, "Question?")

        assert _count(session.llm.calls[0]) == 0
        assert session.reads == 0


@pytest.mark.filterwarnings(
    "ignore:Parameters .*reasoning_effort.* should be specified explicitly:UserWarning"
)
class TestSessionRestoreRoundTrip:
    """Through the real serializer, turn-end reconcile and resume reader.

    A live process runs two turns and stops; a fresh process restores its
    ``thread_messages`` rows and runs turn three with a manager whose store
    now reports more memories. The restored ``context`` row keeps the
    summary present: nothing is read, nothing is re-sent, and turn three is
    byte-identical to the uninterrupted run's turn three.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("lane", ["pinned", "stateless"])
    async def test_the_restored_summary_stays_the_one_sent(self, lane, monkeypatch):
        from tests import test_session_restore_round_trip as round_trip

        managers: List[Any] = []
        make_manager = harness.RecordingMemoryManager

        def manager(*args: Any, **kwargs: Any) -> Any:
            instance = make_manager(*args, **kwargs)
            if len(managers) == 2:  # the restored process
                instance.runtime.recall_store.stats = GROWN
            managers.append(instance)
            return instance

        monkeypatch.setattr(harness, "RecordingMemoryManager", manager)
        monkeypatch.setattr(
            harness,
            "SESSION_TOOLS",
            [
                *harness.SESSION_TOOLS,
                harness._function_tool(
                    MEMORY_SEARCH_TOOL_NAME,
                    "Search project memory",
                    {"query": harness._STR},
                ),
            ],
        )
        family = harness.FAMILIES["openai-chat"]
        inputs = round_trip.ROUND_TRIP_INPUTS

        warm = await round_trip._run_session(
            family,
            monkeypatch,
            rows=round_trip._ThreadRows(),
            inputs=inputs,
            messages=[],
            lane=lane,
        )
        live_rows = round_trip._ThreadRows()
        await round_trip._run_session(
            family,
            monkeypatch,
            rows=live_rows,
            inputs=inputs[:2],
            messages=[],
            lane=lane,
        )
        summary_rows = [
            row
            for row in live_rows.ordered()
            if row["role"] == "context"
            and row["additional_kwargs"]["srw_injection"]["kind"] == MEMORY_SUMMARY_KIND
        ]
        assert len(summary_rows) == 1

        restored, turn_count = await round_trip._restore_rows(
            live_rows, round_trip._round_trip_config(family)
        )
        assert [entry_kind(m) for m in restored].count(MEMORY_SUMMARY_KIND) == 1
        (resumed,) = await round_trip._run_session(
            family,
            monkeypatch,
            rows=live_rows,
            inputs=inputs[2:],
            messages=restored,
            lane=lane,
            initial_turn_count=turn_count,
        )

        assert [m.runtime.recall_store.calls for m in managers] == [1, 1, 0]
        assert harness.canonical(resumed.body) == harness.canonical(warm[-1].body)
        assert harness.text_occurrences(resumed.body, SRW_SUMMARY) == 1
        assert harness.text_occurrences(resumed.body, "Frequent topics: grown.") == 0
