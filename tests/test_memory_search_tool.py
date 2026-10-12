"""memory_search: the model's pull path into project memory (WP5a).

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(D3, D25, D30, D34); plan WP5.

- The tool runs the ``memory_search`` MemoryManager extension: hybrid search
  on the job's or session's own (project-scoped) RecallStore, then the
  configured scorers and policies, then ``limit``. Results are
  ``format_memory`` blocks labelled by display handle, static text with no
  TTL and no score (D11), plain tool output and never a pushed
  ``<srw_context>`` entry (D30). ``handle=`` fetches one memory.
- The bound tool is a front that resolves the extension through
  ``ToolContext.memory_service`` at call time (both runtimes bind tools before
  the manager exists).
- Dedupe (D3/D25): ``scan_presence`` reads the handles and content hashes of a
  ``memory_search`` result back from the history, worker form (the tool name
  on the ToolMessage) and session form (only the call id, matched to the
  assistant's call), so the push skips what the model fetched. That holds
  across a ``thread_messages`` persist/restore, because it reads the history
  itself, not metadata.
- Binding: ``tools.memory`` reaches the toolset only while memory is enabled,
  the manager seam is on and the extension is in the pipeline: workers and
  sessions yes, subagents no.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from types import SimpleNamespace
from typing import Any, List, Optional
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from agent.core.context_injection import (
    ContextSources,
    memory_item,
    plan_context_entries,
    scan_presence,
)
from agent.services.memory import (
    MEMORY_PLUGIN_REGISTRY,
    MemoryManager,
    MemoryPluginSpec,
    MemoryRuntime,
    TransientScorerError,
)
from agent.services.memory.plugins.memory_search import MemorySearchExtension
from agent.tools.context import ToolContext
from agent.tools.memory import (
    MAX_LIMIT,
    MEMORY_SEARCH_DESCRIPTION,
    MEMORY_UNAVAILABLE,
    create_memory_tools,
)
from agent.tools.registry import TOOL_REGISTRY, filter_tools_by_phase, load_tools
from shared.runtime.core.context_entries import (
    UPDATED_ITEM_MARKER,
    digest,
    entry_meta,
    make_context_entry,
    memory_handle,
    memory_list_items,
    normalize_memory_handle,
)
from shared.runtime.core.loader import (
    ROLE_ROOTS,
    get_all_tool_names,
    load_agent_config,
    memory_tool_names_bindable,
    render_instruction_content,
    resolve_config_path,
)
from shared.runtime.services.recall_store import MemoryRecord, RecallStore
from shared.tool_catalog.names import MEMORY_SEARCH_TOOL_NAME

PROJECT = uuid.UUID(int=0xA1)


def mem(n: int, content: Optional[str] = None, **kwargs) -> MemoryRecord:
    kwargs.setdefault("importance", 0.5)
    return MemoryRecord(
        id=uuid.UUID(int=n), content=content or f"Memory fact {n}.", **kwargs
    )


class FakeStore:
    """The RecallStore surface the extension uses, recording its calls."""

    def __init__(self, records: List[MemoryRecord], by_handle=None) -> None:
        self.records = list(records)
        self.by_handle = dict(by_handle or {})
        self.embedding_service = SimpleNamespace(embed=AsyncMock(return_value=[0.1]))
        self.searches: List[str] = []
        self.fetches: List[str] = []
        # The push path's TTL machinery: a search must never touch it.
        self.decrement_ttl = AsyncMock()
        self.get_ttl_active = AsyncMock(return_value=[])

    async def hybrid_search(self, query_text, query_embedding, **kwargs):
        self.searches.append(query_text)
        return list(self.records)

    async def get_by_handle(self, handle):
        self.fetches.append(handle)
        return self.by_handle.get(handle)


def _config(scorers=(), policies=(), *, gate=None, bounded=None) -> Any:
    return SimpleNamespace(
        pipeline=SimpleNamespace(
            retrievers=[],
            scorers=list(scorers),
            policies=list(policies),
            writers=[],
            extensions=[MEMORY_SEARCH_TOOL_NAME],
        ),
        gate=gate,
        bounded=bounded,
    )


def _extension(store, config=None, timeout=None) -> MemorySearchExtension:
    return MemorySearchExtension(
        MemoryRuntime(
            recall_store=store,
            memory_config=config or _config(),
            retrieval_timeout=timeout,
        )
    )


def _search(ext: MemorySearchExtension, **kwargs) -> str:
    return asyncio.run(ext.run(**kwargs))


@pytest.fixture
def scorer_plugin(monkeypatch):
    """Register a test scorer that ranks by a per-content score table."""

    def _register(name: str, scores=None, error: Optional[Exception] = None):
        class _Scorer:
            async def score(self, req, items):
                if error is not None:
                    raise error
                for item in items:
                    item.candidate.channel_scores["rerank"] = scores[
                        item.candidate.text
                    ]
                    item.score = scores[item.candidate.text]
                return sorted(items, key=lambda i: i.score, reverse=True)

        monkeypatch.setitem(
            MEMORY_PLUGIN_REGISTRY["scorer"],
            name,
            MemoryPluginSpec(kind="scorer", name=name, factory=lambda rt: _Scorer()),
        )
        return name

    return _register


# ---------------------------------------------------------------------------
# The tool
# ---------------------------------------------------------------------------


class TestSearch:
    def test_results_carry_handles_and_static_text(self):
        records = [
            mem(1, "The prod cluster runs in eu-central.", remaining_turns=7),
            mem(2, "Deploys go through Fleet.", memory_type="procedural"),
        ]
        store = FakeStore(records)
        out = _search(_extension(store), query="where does prod run")

        assert out.startswith('Memories matching "where does prod run"')
        assert f"[{memory_handle(records[0].id)}]" in out
        assert f"[{memory_handle(records[1].id)}]" in out
        assert "The prod cluster runs in eu-central." in out
        # D11: nothing per turn (no TTL, no pinned tier, no score) and D30:
        # not a pushed entry.
        for volatile in ("turns left", "pinned", "rerank", "score"):
            assert volatile not in out
        assert "<srw_context" not in out
        assert store.searches == ["where does prod run"]
        store.decrement_ttl.assert_not_awaited()
        store.get_ttl_active.assert_not_awaited()

    def test_the_same_rows_render_the_same_bytes(self):
        records = [mem(1), mem(2)]
        first = _search(_extension(FakeStore(records)), query="q")
        records[0].remaining_turns = 3  # a TTL tick between the calls
        assert _search(_extension(FakeStore(records)), query="q") == first

    def test_limit_is_respected_and_clamped(self):
        records = [mem(n) for n in range(1, 16)]
        assert (
            _search(_extension(FakeStore(records)), query="q", limit=2).count("[m:")
            == 2
        )
        assert (
            _search(_extension(FakeStore(records)), query="q", limit=50).count("[m:")
            == MAX_LIMIT
        )
        assert (
            _search(_extension(FakeStore(records)), query="q", limit=0).count("[m:")
            == 1
        )

    def test_empty_result_says_so(self):
        out = _search(_extension(FakeStore([])), query="nothing like this")
        assert out == 'No memories match "nothing like this".'

    def test_a_query_or_a_handle_is_required(self):
        store = FakeStore([mem(1)])
        assert _search(_extension(store), query="   ").startswith("Error:")
        assert store.searches == []

    def test_without_a_store_memory_is_unavailable(self):
        assert _search(_extension(None), query="q") == MEMORY_UNAVAILABLE

    def test_a_store_failure_is_reported_not_raised(self):
        store = FakeStore([mem(1)])
        store.hybrid_search = AsyncMock(side_effect=RuntimeError("db down"))
        out = _search(_extension(store), query="q")
        assert out.startswith("Memory search failed (RuntimeError)")

    def test_a_slow_store_times_out_inside_the_budget(self):
        store = FakeStore([mem(1)])

        async def _slow(*args, **kwargs):
            await asyncio.sleep(5)

        store.hybrid_search = _slow
        out = _search(_extension(store, timeout=0.01), query="q")
        assert out.startswith("Memory search timed out")


class TestPipeline:
    def test_configured_scorers_and_policies_rank_and_gate(self, scorer_plugin):
        from agent.services.memory.plugins.bounded import BoundedPolicy  # noqa: F401

        name = scorer_plugin(
            "wp5a_scorer",
            scores={"low": 0.001, "mid": 0.5, "top": 0.9, "also": 0.8},
        )
        records = [mem(1, "low"), mem(2, "mid"), mem(3, "top"), mem(4, "also")]
        config = _config(
            scorers=[name],
            policies=["gate", "bounded"],
            gate=SimpleNamespace(threshold=0.1, channel="rerank", mode="relative"),
            bounded=SimpleNamespace(
                max_items=2, max_tokens=None, include_knowledge=False
            ),
        )
        ext = _extension(FakeStore(records), config)
        found = asyncio.run(ext.search_records(ext.runtime.recall_store, "q", 5))
        # Reranked, the 0.001 tail gated out (floor 0.09), capped at 2.
        assert [r.content for r in found] == ["top", "also"]

    def test_a_transient_scorer_fault_keeps_hybrid_order(self, scorer_plugin):
        name = scorer_plugin("wp5a_flaky", error=TransientScorerError("blip"))
        records = [mem(1, "a"), mem(2, "b")]
        ext = _extension(FakeStore(records), _config(scorers=[name]))
        out = _search(ext, query="q")
        assert out.index("\na") < out.index("\nb")

    def test_a_structural_scorer_fault_is_reported_not_raised(self, scorer_plugin):
        name = scorer_plugin("wp5a_broken", error=ValueError("bad shape"))
        ext = _extension(FakeStore([mem(1), mem(2)]), _config(scorers=[name]))
        assert _search(ext, query="q") == (
            "Memory search failed (ValueError). Continue without it."
        )


class TestHandleFetch:
    def test_fetch_by_handle_in_every_spelling(self):
        record = mem(9, "The owner prefers bullet points.")
        handle = memory_handle(record.id)
        store = FakeStore([], by_handle={handle: record})
        for spelling in (handle, f"[{handle}]", handle[2:].upper()):
            out = _search(_extension(store), handle=spelling)
            assert out.startswith(f"Memory {handle}:")
            assert f"[{handle}]" in out and record.content in out
        assert store.fetches == [handle] * 3
        assert store.searches == []

    def test_an_unknown_handle_says_so(self):
        out = _search(_extension(FakeStore([])), handle="m:abcdef")
        assert out.startswith("No current memory has the handle m:abcdef")

    def test_a_malformed_handle_is_an_error(self):
        store = FakeStore([])
        assert _search(_extension(store), handle="memory 7").startswith("Error:")
        assert store.fetches == []

    def test_normalize_memory_handle(self):
        assert normalize_memory_handle("M:3F9A2C") == "m:3f9a2c"
        assert normalize_memory_handle(" [m:3f9a2c] ") == "m:3f9a2c"
        assert normalize_memory_handle("3f9a2c") == "m:3f9a2c"
        for bad in ("", None, "m:3f9a2", "m:3f9a2cc", "x:3f9a2c", "m:zzzzzz"):
            assert normalize_memory_handle(bad) is None


class TestProjectScoping:
    """The search and the fetch run on the store's own scope (like the push)."""

    class _DB:
        def __init__(self, rows):
            self.rows = rows
            self.calls: List[tuple] = []

        async def fetch(self, sql, *args):
            self.calls.append(("fetch", sql, args))
            return self.rows

        async def fetchrow(self, sql, *args):
            self.calls.append(("fetchrow", sql, args))
            return self.rows[0] if self.rows else None

        async def execute(self, sql, *args):
            self.calls.append(("execute", sql, args))

    def _store(self, rows):
        db = self._DB(rows)
        store = RecallStore(
            db=db,
            embedding_service=SimpleNamespace(embed=AsyncMock(return_value=[0.1])),
            job_id=uuid.uuid4(),
            config=SimpleNamespace(project_scoped=True),
            project_id=PROJECT,
        )
        return store, db

    def _row(self, n):
        return {"id": uuid.UUID(int=n), "content": f"fact {n}", "importance": 0.8}

    def test_search_is_project_scoped(self):
        store, db = self._store([self._row(1)])
        out = _search(_extension(store), query="q")
        kind, sql, args = db.calls[0]
        assert "memory_project_hybrid_search" in sql
        assert args[2] == PROJECT
        assert memory_handle(uuid.UUID(int=1)) in out

    def test_fetch_is_project_scoped(self):
        store, db = self._store([self._row(2)])
        out = _search(_extension(store), handle=memory_handle(uuid.UUID(int=2)))
        kind, sql, args = db.calls[0]
        assert kind == "fetchrow"
        assert "project_id = $1" in sql and "valid_to IS NULL" in sql
        assert args == (PROJECT, memory_handle(uuid.UUID(int=2))[2:])
        assert "fact 2" in out


# ---------------------------------------------------------------------------
# The bound front and the extension seam
# ---------------------------------------------------------------------------


def _worker_config():
    path, deployment_dir = resolve_config_path(ROLE_ROOTS["worker"])
    return load_agent_config(path, deployment_dir)


def _session_config():
    path, deployment_dir = resolve_config_path(ROLE_ROOTS["session"])
    return load_agent_config(path, deployment_dir)


def _subagent_config():
    path, deployment_dir = resolve_config_path(ROLE_ROOTS["subagent"])
    return load_agent_config(path, deployment_dir)


class TestFront:
    def test_unavailable_until_a_manager_is_published(self):
        ctx = ToolContext()
        (tool,) = load_tools([MEMORY_SEARCH_TOOL_NAME], ctx)
        assert asyncio.run(tool.ainvoke({"query": "q"})) == MEMORY_UNAVAILABLE

    def test_delegates_to_the_bound_extension(self, monkeypatch):
        # The shipped pipeline binds the reranker, which needs a transport.
        monkeypatch.setenv("EMBEDDING_BASE_URL", "http://embedding.invalid/v1")
        cfg = _worker_config()
        store = FakeStore([mem(1, "Use ruff.")])
        manager = MemoryManager.from_config(
            cfg.memory,
            MemoryRuntime(recall_store=store, memory_config=cfg.memory),
        )
        # The extension is bound from memory.pipeline.extensions.
        assert manager.pipeline_summary()["extensions"] == [MEMORY_SEARCH_TOOL_NAME]
        (ext_tool,) = manager.extension_tools()

        ctx = ToolContext()
        (front,) = create_memory_tools(ctx)
        assert front.name == ext_tool.name == MEMORY_SEARCH_TOOL_NAME
        assert front.description == ext_tool.description == MEMORY_SEARCH_DESCRIPTION
        assert front.args == ext_tool.args
        ctx.memory_service = manager
        # The real pipeline's reranker would call out; only one candidate, so
        # it passes through unscored (the scorer skips fewer than two).
        out = asyncio.run(front.ainvoke({"query": "lint"}))
        assert "Use ruff." in out and f"[{memory_handle(uuid.UUID(int=1))}]" in out

    def test_pull_and_push_have_different_names_and_wording(self):
        assert MEMORY_SEARCH_TOOL_NAME == "memory_search"
        assert "recall_memories" not in MEMORY_SEARCH_DESCRIPTION
        assert "<srw_context" not in MEMORY_SEARCH_DESCRIPTION

    def test_catalog_entry(self):
        meta = TOOL_REGISTRY[MEMORY_SEARCH_TOOL_NAME]
        assert meta["category"] == "memory"
        assert set(meta["phases"]) == {"strategic", "tactical"}
        assert "grant" not in meta  # config-granted by name or `memory: true`
        for phase in ("strategic", "tactical"):
            assert filter_tools_by_phase([MEMORY_SEARCH_TOOL_NAME], phase) == [
                MEMORY_SEARCH_TOOL_NAME
            ]

    def test_worker_publishes_the_graph_manager(self):
        from agent.agent import UniversalAgent

        marker = object()
        agent = SimpleNamespace(
            _tool_context=ToolContext(),
            _graph=SimpleNamespace(_srw_memory_service=marker),
        )
        UniversalAgent._publish_memory_service(agent)
        assert agent._tool_context.memory_service is marker

    def test_a_subagent_child_never_reads_the_parent_manager(self):
        import copy

        from agent.subagents.child import rebase_context

        parent = ToolContext()
        parent.memory_service = object()
        child = rebase_context(
            copy.copy(parent),
            cfg=SimpleNamespace(llm=None, limits=None, instruction_files=[]),
            tool_config={},
            workspace_manager=None,
            shell_manager=None,
        )
        assert child.memory_service is None
        assert parent.memory_service is not None


class TestBinding:
    def test_bound_for_workers_and_sessions(self):
        for cfg in (_worker_config(), _session_config()):
            assert MEMORY_SEARCH_TOOL_NAME in get_all_tool_names(cfg)

    def test_not_bound_for_subagents(self):
        cfg = _subagent_config()
        assert cfg.memory.enabled is False
        assert MEMORY_SEARCH_TOOL_NAME not in get_all_tool_names(cfg)

    @pytest.mark.parametrize(
        "switch_off",
        ["memory", "manager", "extension"],
    )
    def test_not_bound_when_memory_cannot_serve_it(self, switch_off):
        cfg = _worker_config()
        if switch_off == "memory":
            cfg.memory.enabled = False
        elif switch_off == "manager":
            cfg.memory.manager_enabled = False
        else:
            cfg.memory.pipeline.extensions = []
        assert memory_tool_names_bindable(cfg) == frozenset()
        assert MEMORY_SEARCH_TOOL_NAME not in get_all_tool_names(cfg)

    def test_the_prompt_hint_follows_the_binding(self):
        line = (
            "Relevant memories arrive as context"
            '{% if has_tool("memory_search") %}; call memory_search to look up '
            "anything else from earlier work{% endif %}."
        )
        with_tool = render_instruction_content(line, [MEMORY_SEARCH_TOOL_NAME])
        without = render_instruction_content(line, ["read_file"])
        assert "call memory_search" in with_tool
        assert without == "Relevant memories arrive as context."


# ---------------------------------------------------------------------------
# Dedupe between push and pull (D3, D25)
# ---------------------------------------------------------------------------


def _search_pair(records, *, call_id="ms1", named=True) -> List[BaseMessage]:
    """An assistant ``memory_search`` call and its result, as a runtime stores it.

    ``named``: the worker's ToolNode stamps the tool name on the result; the
    session loop stores only the call id.
    """
    text = (
        'Memories matching "q", best match first:\n\n'
        + RecallStore.render_memory_list(records)
    )
    call = AIMessage(
        content="",
        tool_calls=[
            {"name": MEMORY_SEARCH_TOOL_NAME, "args": {"query": "q"}, "id": call_id}
        ],
    )
    result = ToolMessage(
        content=text,
        tool_call_id=call_id,
        **({"name": MEMORY_SEARCH_TOOL_NAME} if named else {}),
    )
    return [call, result]


def _base() -> List[BaseMessage]:
    return [SystemMessage(content="system"), HumanMessage(content="Do the task.")]


def _plan(history, records, max_memories=5):
    return plan_context_entries(
        history,
        ContextSources(memory_records=records),
        model=None,
        max_memories=max_memories,
    )


class TestRoundTripOfTheRendering:
    def test_memory_list_items_reads_back_every_block(self):
        records = [
            mem(1, "Line one.\n\nLine two after a blank line."),
            mem(2, "Ends with a newline.\n", memory_type="procedural"),
            mem(3, "Mentions [m:abcdef] mid-line."),
        ]
        text = "Header line:\n\n" + RecallStore.render_memory_list(records)
        assert memory_list_items(text) == [
            (memory_handle(r.id), digest(r.content)) for r in records
        ]

    def test_text_without_handles_has_no_items(self):
        assert memory_list_items('No memories match "x".') == []
        assert memory_list_items(None) == []

    def test_a_pushed_entry_body_reads_back_too(self):
        records = [mem(4), mem(5)]
        body = RecallStore.render_memory_entry(
            [(r, memory_handle(r.id), False) for r in records]
        )
        assert [h for h, _ in memory_list_items(body)] == [
            memory_handle(r.id) for r in records
        ]


class TestDedupe:
    @pytest.mark.parametrize("named", [True, False], ids=["worker", "session"])
    def test_a_fetched_memory_is_not_pushed(self, named):
        fetched = [mem(1), mem(2)]
        history = _base() + _search_pair(fetched, named=named)

        presence = scan_presence(history)
        assert presence.memory_handles == {
            memory_handle(r.id): digest(r.content) for r in fetched
        }

        planned = _plan(history, fetched + [mem(3)])
        assert planned.memory_present == 2
        assert planned.memory_appended == 1
        (entry,) = planned.entries
        assert [i["key"] for i in entry_meta(entry)["items"]] == [str(uuid.UUID(int=3))]

    def test_nothing_is_pushed_when_the_fetch_covered_everything(self):
        fetched = [mem(1), mem(2)]
        planned = _plan(_base() + _search_pair(fetched), fetched)
        assert planned.entries == []

    def test_other_tool_output_with_handles_does_not_count(self):
        record = mem(1)
        text = RecallStore.render_memory_list([record])
        history = _base() + [
            AIMessage(
                content="",
                tool_calls=[{"name": "read_file", "args": {}, "id": "rf1"}],
            ),
            ToolMessage(content=text, tool_call_id="rf1", name="read_file"),
        ]
        assert scan_presence(history).memory_handles == {}
        assert _plan(history, [record]).memory_appended == 1

    def test_a_memory_changed_after_the_fetch_is_pushed_as_updated(self):
        history = _base() + _search_pair([mem(1, "v1")])
        planned = _plan(history, [mem(1, "v2")])
        (entry,) = planned.entries
        assert UPDATED_ITEM_MARKER in entry.content and "v2" in entry.content

    def test_pushed_then_fetched_is_fine(self):
        record = mem(1)
        pushed = _plan(_base(), [record]).entries
        assert len(pushed) == 1
        history = _base() + pushed + _search_pair([record])
        planned = _plan(history, [record])
        assert planned.entries == [] and planned.memory_present == 1

    def test_fetched_then_changed_then_fetched_again_is_present(self):
        history = (
            _base()
            + _search_pair([mem(1, "v1")], call_id="a")
            + _search_pair([mem(1, "v2")], call_id="b")
        )
        assert _plan(history, [mem(1, "v2")]).entries == []

    def test_compaction_makes_a_fetched_memory_eligible_again(self):
        record = mem(1)
        history = _base() + _search_pair([record])
        compacted = _base()  # the summary replaced the call and its result
        assert _plan(history, [record]).entries == []
        assert _plan(compacted, [record]).memory_appended == 1

    def test_a_handle_fetch_counts_too(self):
        record = mem(7)
        handle = memory_handle(record.id)
        out = _search(
            _extension(FakeStore([], by_handle={handle: record})), handle=handle
        )
        history = _base() + [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": MEMORY_SEARCH_TOOL_NAME,
                        "args": {"handle": handle},
                        "id": "h1",
                    }
                ],
            ),
            ToolMessage(content=out, tool_call_id="h1"),
        ]
        assert _plan(history, [record]).entries == []

    def test_entry_items_record_their_handles(self):
        entry = make_context_entry(
            "memory",
            "body",
            section="memory",
            items=[{"key": "k1", "hash": "h1", "handle": "m:abcdef"}],
        )
        presence = scan_presence([entry])
        assert presence.items == {("memory", "k1"): "h1"}
        assert presence.memory_handles == {"m:abcdef": "h1"}
        assert presence.item_hash("memory", memory_item(mem(1))) is None


class TestSessionRestore:
    """A pull survives a thread_messages persist + restore and still dedupes."""

    @pytest.mark.asyncio
    async def test_restored_history_keeps_the_pull_present(self):
        from unittest.mock import AsyncMock as _AsyncMock

        from agent.api.persistent_app import _db_rows_to_lc_messages
        from agent.core.thread_messages import _serialize_message_row
        from agent.database.postgres_db import PostgresDB

        fetched = [mem(1), mem(2)]
        live = [HumanMessage(content="What did we decide?", id=str(uuid.uuid4()))]
        live += _search_pair(fetched, named=False)
        for msg in live[1:]:
            msg.id = str(uuid.uuid4())
        rows = [_serialize_message_row(m, 1) for m in live]

        projected = []
        for row in rows:
            stored = json.loads(json.dumps(row))
            projected.append(
                {
                    "id": stored["id"],
                    "role": stored["role"],
                    "content": stored["content"],
                    "tool_calls": json.dumps(stored["tool_calls"])
                    if stored["tool_calls"] is not None
                    else None,
                    "tool_call_id": stored["tool_call_id"],
                    "turn_number": stored["turn_number"],
                    "admitted_turn_number": None,
                    "additional_kwargs": None,
                }
            )
        db = PostgresDB.__new__(PostgresDB)
        db.fetch = _AsyncMock(return_value=projected)
        history = await db.get_thread_messages_history("thread", order_by_seq=True)
        restored = _db_rows_to_lc_messages(history)

        assert [type(m) for m in restored] == [HumanMessage, AIMessage, ToolMessage]
        assert (
            scan_presence(restored).memory_handles == scan_presence(live).memory_handles
        )
        assert _plan(restored, fetched + [mem(3)]).memory_appended == 1
