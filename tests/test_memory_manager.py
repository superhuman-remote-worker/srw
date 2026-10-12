"""Tests for the MemoryManager kernel (memory overhaul Phase 1, slice 1).

Covers the seam's kernel only: registry, binder (from_config), assemble
orchestration + containment + stats, capture dispatch, extensions, and
the MemoryConfig plumbing. The transplanted current-behaviour plugins
get their own equivalence-fixture suites in the follow-up slices.
"""

from types import SimpleNamespace

import pytest
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

import agent.services.memory.registry as registry_module
from shared.runtime.core.loader import (
    MemoryConfig,
    MemoryPipelineConfig,
    _parse_memory_config,
)
from shared.runtime.services.knowledge_store import KnowledgeRecord
from shared.runtime.services.recall_store import MemoryRecord
from agent.services.memory import (
    AssembleRequest,
    AssembleStats,
    Candidate,
    CaptureEvent,
    InjectionBlock,
    MemoryManager,
    MemoryPayload,
    MemoryRuntime,
    Scored,
    UnknownMemoryPluginError,
    available_memory_plugins,
    register_memory_plugin,
)


@pytest.fixture(autouse=True)
def clean_registry():
    """Snapshot/restore the module-level registry around every test."""
    snapshot = {
        kind: dict(specs)
        for kind, specs in registry_module.MEMORY_PLUGIN_REGISTRY.items()
    }
    yield
    for kind, specs in registry_module.MEMORY_PLUGIN_REGISTRY.items():
        specs.clear()
        specs.update(snapshot[kind])


# ---------------------------------------------------------------------------
# Test plugins
# ---------------------------------------------------------------------------


class FakeRetriever:
    def __init__(self, candidates=None, error=None, log=None, label=""):
        self.candidates = candidates or []
        self.error = error
        self.log = log
        self.label = label

    async def retrieve(self, req):
        if self.log is not None:
            self.log.append(f"retrieve:{self.label}")
        if self.error:
            raise self.error
        return list(self.candidates)


class FakeScorer:
    def __init__(self, score=1.0, error=None, log=None, label=""):
        self.score_value = score
        self.error = error
        self.log = log
        self.label = label

    async def score(self, req, items):
        if self.log is not None:
            self.log.append(f"score:{self.label}")
        if self.error:
            raise self.error
        return [Scored(candidate=s.candidate, score=self.score_value) for s in items]


class FakePolicy:
    def __init__(self, keep=None, error=None, log=None, label=""):
        self.keep = keep  # None = pass through, int = head-truncate
        self.error = error
        self.log = log
        self.label = label

    async def apply(self, req, items):
        if self.log is not None:
            self.log.append(f"apply:{self.label}")
        if self.error:
            raise self.error
        return items if self.keep is None else items[: self.keep]


class FakeWriter:
    def __init__(self, kinds=("turn_end",), error=None):
        self.event_kinds = frozenset(kinds)
        self.error = error
        self.events = []

    async def on_event(self, event):
        if self.error:
            raise self.error
        self.events.append(event)


class FakeExtension:
    def __init__(self, tools=None, error=None):
        self._tools = tools or []
        self.error = error

    def tools(self):
        if self.error:
            raise self.error
        return list(self._tools)


def make_candidate(kind="memory", text="fact", tokens=10, record_id="r1"):
    """Candidate with a real store record so the real renderers work."""
    if kind == "memory":
        record = MemoryRecord(id=record_id, content=text, token_count=tokens)
    elif kind == "knowledge":
        record = KnowledgeRecord(id=record_id, note_id=str(record_id), content=text)
    else:
        record = SimpleNamespace(id=record_id)
    return Candidate(kind=kind, text=text, token_count=tokens, record=record)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_register_and_resolve(self):
        @register_memory_plugin("retriever", "test_dense", description="probe")
        def _build(runtime):
            return FakeRetriever()

        spec = registry_module.resolve_memory_plugin("retriever", "test_dense")
        assert spec.name == "test_dense"
        assert spec.kind == "retriever"
        assert spec.description == "probe"
        assert spec.factory is _build

    def test_duplicate_registration_raises(self):
        @register_memory_plugin("scorer", "test_dup")
        def _build_a(runtime):
            return FakeScorer()

        with pytest.raises(ValueError, match="already registered"):

            @register_memory_plugin("scorer", "test_dup")
            def _build_b(runtime):
                return FakeScorer()

    def test_same_factory_reregistration_is_idempotent(self):
        def _build(runtime):
            return FakeScorer()

        register_memory_plugin("scorer", "test_idem")(_build)
        # Module re-import re-runs decorators with the same function object.
        register_memory_plugin("scorer", "test_idem")(_build)

    def test_unknown_name_lists_available(self):
        @register_memory_plugin("policy", "test_budget")
        def _build(runtime):
            return FakePolicy()

        with pytest.raises(UnknownMemoryPluginError) as exc_info:
            registry_module.resolve_memory_plugin("policy", "nonexistent")
        assert "test_budget" in str(exc_info.value)
        assert "nonexistent" in str(exc_info.value)

    def test_unknown_kind_raises(self):
        with pytest.raises(ValueError, match="kind"):
            register_memory_plugin("frobnicator", "x")
        with pytest.raises(ValueError, match="kind"):
            registry_module.resolve_memory_plugin("frobnicator", "x")
        with pytest.raises(ValueError, match="kind"):
            available_memory_plugins("frobnicator")

    def test_available_filters_by_kind(self):
        @register_memory_plugin("writer", "test_writer")
        def _build(runtime):
            return FakeWriter()

        only_writers = available_memory_plugins("writer")
        assert list(only_writers.keys()) == ["writer"]
        assert "test_writer" in only_writers["writer"]
        everything = available_memory_plugins()
        assert set(everything.keys()) == set(registry_module.PLUGIN_KINDS)


# ---------------------------------------------------------------------------
# Binder (from_config)
# ---------------------------------------------------------------------------


class TestFromConfig:
    def test_binds_named_plugins_with_runtime(self):
        seen_runtimes = []

        @register_memory_plugin("retriever", "test_recall")
        def _build_retriever(runtime):
            seen_runtimes.append(runtime)
            return FakeRetriever()

        @register_memory_plugin("writer", "test_extractor")
        def _build_writer(runtime):
            seen_runtimes.append(runtime)
            return FakeWriter()

        cfg = MemoryConfig(
            pipeline=MemoryPipelineConfig(
                retrievers=["test_recall"], writers=["test_extractor"]
            )
        )
        runtime = MemoryRuntime(recall_store=object())
        manager = MemoryManager.from_config(cfg, runtime)

        assert manager.pipeline_summary() == {
            "retrievers": ["test_recall"],
            "scorers": [],
            "policies": [],
            "writers": ["test_extractor"],
            "extensions": [],
        }
        assert seen_runtimes == [runtime, runtime]

    def test_unknown_pipeline_name_fails_at_bind_time(self):
        cfg = MemoryConfig(pipeline=MemoryPipelineConfig(retrievers=["missing"]))
        with pytest.raises(UnknownMemoryPluginError, match="missing"):
            MemoryManager.from_config(cfg, MemoryRuntime())

    @pytest.mark.asyncio
    async def test_empty_pipeline_binds_noop_manager(self):
        manager = MemoryManager.from_config(MemoryConfig(), MemoryRuntime())
        payload = await manager.assemble(AssembleRequest(query_text="anything"))
        assert payload.blocks == []
        assert payload.stats.errors == []
        assert payload.messages() == []
        # capture is a no-op too — must not raise
        await manager.capture(CaptureEvent(kind="turn_end"))
        assert manager.extension_tools() == []

    def test_runtime_memory_config_backfilled(self):
        cfg = MemoryConfig()
        runtime = MemoryRuntime()
        MemoryManager.from_config(cfg, runtime)
        assert runtime.memory_config is cfg

    def test_runtime_memory_config_not_overwritten(self):
        preset = MemoryConfig(budget_tokens=42)
        runtime = MemoryRuntime(memory_config=preset)
        MemoryManager.from_config(MemoryConfig(), runtime)
        assert runtime.memory_config is preset


# ---------------------------------------------------------------------------
# Assemble orchestration
# ---------------------------------------------------------------------------


class TestAssemble:
    @pytest.mark.asyncio
    async def test_stage_order_retrievers_scorers_policies(self):
        log = []
        manager = MemoryManager(
            MemoryRuntime(),
            retrievers=[
                ("r1", FakeRetriever([make_candidate()], log=log, label="r1")),
                ("r2", FakeRetriever([make_candidate()], log=log, label="r2")),
            ],
            scorers=[("s1", FakeScorer(log=log, label="s1"))],
            policies=[("p1", FakePolicy(log=log, label="p1"))],
        )
        await manager.assemble(AssembleRequest(query_text="q"))
        assert log == ["retrieve:r1", "retrieve:r2", "score:s1", "apply:p1"]

    @pytest.mark.asyncio
    async def test_blocks_grouped_by_kind_with_provenance(self):
        mem = make_candidate(kind="memory", tokens=10, record_id="m1")
        kb1 = make_candidate(kind="knowledge", tokens=7, record_id="k1")
        kb2 = make_candidate(kind="knowledge", tokens=5, record_id="k2")
        manager = MemoryManager(
            MemoryRuntime(),
            retrievers=[
                ("recall", FakeRetriever([mem])),
                ("kb", FakeRetriever([kb1, kb2])),
            ],
        )
        payload = await manager.assemble(AssembleRequest(query_text="q"))

        assert payload.stats.candidates_total == 3
        assert payload.stats.per_retriever == {"recall": 1, "kb": 2}
        assert payload.stats.blocks == 2
        assert payload.stats.injected_total == 3
        assert payload.stats.tokens_injected == 22

        by_kind = {b.kind: b for b in payload.blocks}
        assert by_kind["memory"].token_count == 10
        assert by_kind["knowledge"].token_count == 12
        assert [i["record_id"] for i in by_kind["knowledge"].items] == ["k1", "k2"]
        # retriever name stamped as provenance
        assert by_kind["memory"].items[0]["retriever"] == "recall"

    @pytest.mark.asyncio
    async def test_blocks_carry_their_store_records_in_rank_order(self):
        # Append-only context injection renders its own entries from the
        # records; the legacy content/messages stay as they were.
        mem = make_candidate(kind="memory", text="m-fact", record_id="m1")
        kb1 = make_candidate(kind="knowledge", text="k-one", record_id="k1")
        kb2 = make_candidate(kind="knowledge", text="k-two", record_id="k2")
        manager = MemoryManager(
            MemoryRuntime(),
            retrievers=[
                ("recall", FakeRetriever([mem])),
                ("kb", FakeRetriever([kb1, kb2])),
            ],
        )
        payload = await manager.assemble(AssembleRequest(query_text="q"))

        by_kind = {b.kind: b for b in payload.blocks}
        assert by_kind["memory"].records == [mem.record]
        assert by_kind["knowledge"].records == [kb1.record, kb2.record]
        assert by_kind["memory"].content
        assert len(by_kind["memory"].messages) == 2

    def test_injection_block_records_default_empty(self):
        assert InjectionBlock(kind="memory").records == []

    @pytest.mark.asyncio
    async def test_retriever_failure_contained(self):
        good = make_candidate()
        manager = MemoryManager(
            MemoryRuntime(),
            retrievers=[
                ("bad", FakeRetriever(error=ValueError("boom"))),
                ("good", FakeRetriever([good])),
            ],
        )
        payload = await manager.assemble(AssembleRequest(query_text="q"))
        assert payload.stats.candidates_total == 1
        assert payload.stats.per_retriever == {"bad": 0, "good": 1}
        assert payload.stats.errors == ["retriever:bad: ValueError: boom"]
        assert payload.stats.blocks == 1

    @pytest.mark.asyncio
    async def test_scorer_failure_hard_fails(self):
        from agent.services.memory import MemoryPipelineError

        manager = MemoryManager(
            MemoryRuntime(),
            retrievers=[("r", FakeRetriever([make_candidate()]))],
            scorers=[("broken", FakeScorer(error=RuntimeError("nope")))],
        )
        # A configured scorer is required — its failure raises rather than
        # silently degrading to legacy order.
        with pytest.raises(MemoryPipelineError, match="broken"):
            await manager.assemble(AssembleRequest(query_text="q"))

    @pytest.mark.asyncio
    async def test_scorer_transient_exhaustion_degrades_turn(self):
        from agent.services.memory import TransientScorerError

        first = make_candidate(kind="memory", record_id="m1")
        second = make_candidate(kind="memory", record_id="m2")
        manager = MemoryManager(
            MemoryRuntime(),
            retrievers=[("r", FakeRetriever([first, second]))],
            scorers=[("reranker", FakeScorer(error=TransientScorerError("net blip")))],
        )
        # A transient transport fault that survived the scorer's bounded
        # retries degrades THIS turn to pre-scorer (hybrid) order — loud in
        # stats.errors, but the turn (and the job) stays alive.
        payload = await manager.assemble(AssembleRequest(query_text="q"))

        assert payload.stats.errors == [
            "scorer:reranker: TransientScorerError: net blip"
        ]
        block = payload.blocks[0]
        assert [i["record_id"] for i in block.items] == ["m1", "m2"]

    @pytest.mark.asyncio
    async def test_policy_filters_items(self):
        manager = MemoryManager(
            MemoryRuntime(),
            retrievers=[
                (
                    "r",
                    FakeRetriever(
                        [make_candidate(record_id=f"m{i}") for i in range(5)]
                    ),
                )
            ],
            policies=[("head3", FakePolicy(keep=3))],
        )
        payload = await manager.assemble(AssembleRequest(query_text="q"))
        assert payload.stats.candidates_total == 5
        assert payload.stats.injected_total == 3

    @pytest.mark.asyncio
    async def test_unknown_kind_renders_provenance_only_block(self):
        exotic = make_candidate(kind="exotic", record_id="x1")
        manager = MemoryManager(
            MemoryRuntime(), retrievers=[("r", FakeRetriever([exotic]))]
        )
        payload = await manager.assemble(AssembleRequest(query_text="q"))
        assert payload.stats.blocks == 1
        assert payload.blocks[0].kind == "exotic"
        assert payload.blocks[0].messages == []
        assert payload.blocks[0].items[0]["record_id"] == "x1"

    @pytest.mark.asyncio
    async def test_kernel_bug_backstop_returns_empty_payload(self, monkeypatch):
        manager = MemoryManager(
            MemoryRuntime(), retrievers=[("r", FakeRetriever([make_candidate()]))]
        )

        def _explode(req, items):
            raise RuntimeError("kernel bug")

        monkeypatch.setattr(manager, "_render_blocks", _explode)
        payload = await manager.assemble(AssembleRequest(query_text="q"))
        assert payload.blocks == []
        assert payload.stats.errors == ["assemble:kernel: RuntimeError: kernel bug"]

    @pytest.mark.asyncio
    async def test_latency_recorded(self):
        manager = MemoryManager(MemoryRuntime())
        payload = await manager.assemble(AssembleRequest(query_text="q"))
        assert payload.stats.latency_ms >= 0.0

    def test_payload_messages_flatten_in_block_order(self):
        ai = AIMessage(content="", tool_calls=[{"name": "x", "args": {}, "id": "1"}])
        tool = ToolMessage(content="block", tool_call_id="1")
        payload = MemoryPayload(
            blocks=[
                InjectionBlock(kind="memory", messages=[ai, tool]),
                InjectionBlock(kind="knowledge", messages=[]),
            ]
        )
        assert payload.messages() == [ai, tool]

    def test_stats_to_dict_round_trip(self):
        stats = AssembleStats(
            candidates_total=3,
            per_retriever={"r": 3},
            injected_total=2,
            tokens_injected=20,
            blocks=1,
            latency_ms=1.234,
            errors=["scorer:x: ValueError: y"],
        )
        d = stats.to_dict()
        assert d["candidates_total"] == 3
        assert d["per_retriever"] == {"r": 3}
        assert d["latency_ms"] == 1.23
        assert d["errors"] == ["scorer:x: ValueError: y"]
        assert "extra" not in d


# ---------------------------------------------------------------------------
# Capture dispatch
# ---------------------------------------------------------------------------


class TestCapture:
    @pytest.mark.asyncio
    async def test_dispatch_by_event_kind(self):
        turn_writer = FakeWriter(kinds=("turn_end",))
        end_writer = FakeWriter(kinds=("session_end", "idle_archive"))
        manager = MemoryManager(
            MemoryRuntime(),
            writers=[("turns", turn_writer), ("teardown", end_writer)],
        )

        await manager.capture(CaptureEvent(kind="turn_end", phase=2))
        assert len(turn_writer.events) == 1
        assert turn_writer.events[0].phase == 2
        assert end_writer.events == []

        await manager.capture(CaptureEvent(kind="session_end"))
        assert len(turn_writer.events) == 1
        assert len(end_writer.events) == 1

    @pytest.mark.asyncio
    async def test_writer_failure_contained_others_still_run(self):
        broken = FakeWriter(kinds=("turn_end",), error=RuntimeError("aux down"))
        healthy = FakeWriter(kinds=("turn_end",))
        manager = MemoryManager(
            MemoryRuntime(), writers=[("broken", broken), ("healthy", healthy)]
        )
        await manager.capture(CaptureEvent(kind="turn_end"))  # must not raise
        assert len(healthy.events) == 1

    @pytest.mark.asyncio
    async def test_broken_event_kinds_contained(self):
        class BrokenKinds:
            @property
            def event_kinds(self):
                raise AttributeError("misbuilt")

            async def on_event(self, event):
                raise AssertionError("must not be called")

        healthy = FakeWriter(kinds=("turn_end",))
        manager = MemoryManager(
            MemoryRuntime(), writers=[("broken", BrokenKinds()), ("ok", healthy)]
        )
        await manager.capture(CaptureEvent(kind="turn_end"))
        assert len(healthy.events) == 1

    def test_invalid_event_kind_rejected(self):
        with pytest.raises(ValueError, match="bogus"):
            CaptureEvent(kind="bogus")


# ---------------------------------------------------------------------------
# Extensions
# ---------------------------------------------------------------------------


class TestExtensions:
    def test_tools_aggregated_in_order(self):
        t1, t2, t3 = object(), object(), object()
        manager = MemoryManager(
            MemoryRuntime(),
            extensions=[("a", FakeExtension([t1, t2])), ("b", FakeExtension([t3]))],
        )
        assert manager.extension_tools() == [t1, t2, t3]

    def test_extension_failure_contained(self):
        t1 = object()
        manager = MemoryManager(
            MemoryRuntime(),
            extensions=[
                ("broken", FakeExtension(error=RuntimeError("no tools"))),
                ("ok", FakeExtension([t1])),
            ],
        )
        assert manager.extension_tools() == [t1]


# ---------------------------------------------------------------------------
# Config plumbing
# ---------------------------------------------------------------------------


class TestMemoryConfigPlumbing:
    def test_defaults_keep_manager_off_and_pipeline_empty(self):
        cfg = _parse_memory_config({})
        assert cfg.manager_enabled is False
        assert cfg.pipeline.retrievers == []
        assert cfg.pipeline.scorers == []
        assert cfg.pipeline.policies == []
        assert cfg.pipeline.writers == []
        assert cfg.pipeline.extensions == []

    def test_parse_manager_and_pipeline(self):
        cfg = _parse_memory_config(
            {
                "enabled": True,
                "manager": {"enabled": True},
                "pipeline": {
                    "retrievers": ["recall_two_tier", "kb_notes"],
                    "scorers": [],
                    "policies": ["token_budget"],
                    "writers": ["interval_extractor"],
                },
            }
        )
        assert cfg.manager_enabled is True
        assert cfg.pipeline.retrievers == ["recall_two_tier", "kb_notes"]
        assert cfg.pipeline.policies == ["token_budget"]
        assert cfg.pipeline.writers == ["interval_extractor"]
        assert cfg.pipeline.extensions == []

    def test_parse_tolerates_null_sections(self):
        cfg = _parse_memory_config({"manager": None, "pipeline": None})
        assert cfg.manager_enabled is False
        assert cfg.pipeline.retrievers == []

    def test_parse_manager_bool_shorthand(self):
        assert _parse_memory_config({"manager": True}).manager_enabled is True
        assert _parse_memory_config({"manager": False}).manager_enabled is False

    def test_legacy_fields_unchanged(self):
        cfg = _parse_memory_config({"budget_tokens": 5000, "observer_interval": 3})
        assert cfg.budget_tokens == 5000
        assert cfg.observer_interval == 3
        assert cfg.enabled is False


# ---------------------------------------------------------------------------
# Reranker scorer (Phase 3, slice 2)
# ---------------------------------------------------------------------------


class FakeRerankResponse:
    def __init__(self, results):
        self._results = results

    def raise_for_status(self):
        pass

    def json(self):
        return {"results": self._results}


class FakeRerankClient:
    """httpx-shaped client returning canned Cohere-style rerank results."""

    def __init__(self, scores=None, error=None, errors=None):
        #: text -> relevance score; results echo document order indices
        self.scores = scores or {}
        self.error = error  # raised on every call
        self.errors = list(errors or [])  # raised one per call, then success
        self.calls = []

    async def post(self, url, json=None):
        self.calls.append({"url": url, "json": json})
        if self.errors:
            raise self.errors.pop(0)
        if self.error:
            raise self.error
        docs = json["documents"]
        return FakeRerankResponse(
            [
                {"index": i, "relevance_score": self.scores.get(doc, 0.0)}
                for i, doc in enumerate(docs)
            ]
        )


def make_scored(text, *, pinned=False, kind="memory", record_id="r"):
    candidate = make_candidate(kind=kind, text=text, record_id=record_id)
    if kind == "memory" and pinned:
        candidate.record.remaining_turns = 3
    return Scored(candidate=candidate)


class TestRerankerScorer:
    def _scorer(self, client, **kwargs):
        from agent.services.memory.plugins.reranker import RerankerScorer

        defaults = dict(
            model="qwen3-reranker-8b",
            base_url="https://router/v1",
            api_key="k",
            client=client,
        )
        defaults.update(kwargs)
        return RerankerScorer(**defaults)

    @pytest.mark.asyncio
    async def test_reorders_by_relevance_and_records_channel_score(self):
        client = FakeRerankClient(scores={"tesla": 0.9, "hiking": 0.1, "civic": 0.6})
        scorer = self._scorer(client)
        items = [make_scored("hiking"), make_scored("civic"), make_scored("tesla")]

        out = await scorer.score(
            AssembleRequest(query_text="what car does the user drive"), items
        )

        assert [i.candidate.text for i in out] == ["tesla", "civic", "hiking"]
        assert out[0].score == 0.9
        assert out[0].candidate.channel_scores["rerank"] == 0.9
        assert client.calls[0]["url"] == "https://router/v1/rerank"
        assert client.calls[0]["json"]["model"] == "qwen3-reranker-8b"

    @pytest.mark.asyncio
    async def test_pinned_stay_first_and_knowledge_passes_through(self):
        client = FakeRerankClient(scores={"a": 0.2, "b": 0.8})
        scorer = self._scorer(client)
        pinned = make_scored("working-set note", pinned=True)
        kb = make_scored("kb note", kind="knowledge")
        items = [pinned, make_scored("a"), kb, make_scored("b")]

        out = await scorer.score(AssembleRequest(query_text="q"), items)

        assert [i.candidate.text for i in out] == [
            "working-set note",
            "b",
            "a",
            "kb note",
        ]
        # pinned item was not sent to the endpoint
        assert client.calls[0]["json"]["documents"] == ["a", "b"]

    @pytest.mark.asyncio
    async def test_top_k_tail_keeps_original_order(self):
        client = FakeRerankClient(scores={"x": 0.1, "y": 0.9})
        scorer = self._scorer(client, top_k=2)
        items = [
            make_scored("x"),
            make_scored("y"),
            make_scored("t1"),
            make_scored("t2"),
        ]

        out = await scorer.score(AssembleRequest(query_text="q"), items)

        assert [i.candidate.text for i in out] == ["y", "x", "t1", "t2"]

    @pytest.mark.asyncio
    async def test_failure_raises_without_partial_reorder(self):
        client = FakeRerankClient(error=RuntimeError("endpoint down"))
        scorer = self._scorer(client)
        items = [make_scored("a"), make_scored("b")]

        with pytest.raises(RuntimeError):
            await scorer.score(AssembleRequest(query_text="q"), items)
        assert "rerank" not in items[0].candidate.channel_scores
        # structural (non-transient) failures don't burn the retry budget
        assert len(client.calls) == 1

    @pytest.mark.asyncio
    async def test_transient_fault_retries_then_succeeds(self):
        import httpx

        client = FakeRerankClient(
            scores={"a": 0.1, "b": 0.9},
            errors=[httpx.ReadTimeout("slow rerank")],
        )
        scorer = self._scorer(client, retries=2, retry_backoff=0)
        items = [make_scored("a"), make_scored("b")]

        out = await scorer.score(AssembleRequest(query_text="q"), items)

        assert [i.candidate.text for i in out] == ["b", "a"]
        assert len(client.calls) == 2  # one timeout, one success

    @pytest.mark.asyncio
    async def test_transient_exhaustion_raises_transient_scorer_error(self):
        import httpx

        from agent.services.memory import TransientScorerError

        client = FakeRerankClient(error=httpx.ReadTimeout("still slow"))
        scorer = self._scorer(client, retries=2, retry_backoff=0)
        items = [make_scored("a"), make_scored("b")]

        with pytest.raises(TransientScorerError, match="3 attempts"):
            await scorer.score(AssembleRequest(query_text="q"), items)
        assert len(client.calls) == 3  # initial + 2 retries
        assert "rerank" not in items[0].candidate.channel_scores

    @pytest.mark.asyncio
    async def test_server_disconnect_and_5xx_are_transient(self):
        import httpx

        req = httpx.Request("POST", "https://router/v1/rerank")
        five_hundred = httpx.HTTPStatusError(
            "server error",
            request=req,
            response=httpx.Response(500, request=req),
        )
        client = FakeRerankClient(
            scores={"a": 0.5, "b": 0.6},
            errors=[
                httpx.RemoteProtocolError("Server disconnected"),
                five_hundred,
            ],
        )
        scorer = self._scorer(client, retries=2, retry_backoff=0)
        items = [make_scored("a"), make_scored("b")]

        out = await scorer.score(AssembleRequest(query_text="q"), items)

        assert [i.candidate.text for i in out] == ["b", "a"]
        assert len(client.calls) == 3

    @pytest.mark.asyncio
    async def test_4xx_is_structural_and_not_retried(self):
        import httpx

        req = httpx.Request("POST", "https://router/v1/rerank")
        client = FakeRerankClient(
            error=httpx.HTTPStatusError(
                "unauthorized",
                request=req,
                response=httpx.Response(401, request=req),
            )
        )
        scorer = self._scorer(client, retries=2, retry_backoff=0)
        items = [make_scored("a"), make_scored("b")]

        with pytest.raises(httpx.HTTPStatusError):
            await scorer.score(AssembleRequest(query_text="q"), items)
        assert len(client.calls) == 1

    @pytest.mark.asyncio
    async def test_short_or_queryless_passthrough(self):
        client = FakeRerankClient()
        scorer = self._scorer(client)
        single = [make_scored("only one")]
        assert await scorer.score(AssembleRequest(query_text="q"), single) == single
        items = [make_scored("a"), make_scored("b")]
        assert await scorer.score(AssembleRequest(query_text=""), items) == items
        assert client.calls == []

    @pytest.mark.asyncio
    async def test_manager_hard_fails_on_scorer_failure(self):
        # A configured scorer (reranker) is REQUIRED: a runtime failure raises
        # MemoryPipelineError (fails the turn loud) rather than silently
        # degrading to legacy order. Regression for
        # knowledge-base/knowledge/issues/openrouter_auxiliary_crashes_session_via_memory_reranker.md.
        from agent.services.memory import MemoryPipelineError
        from agent.services.memory.plugins.reranker import RerankerScorer

        scorer = RerankerScorer(
            model="m",
            base_url="https://router/v1",
            api_key=None,
            client=FakeRerankClient(error=RuntimeError("boom")),
        )
        manager = MemoryManager(
            MemoryRuntime(),
            retrievers=[
                (
                    "r",
                    FakeRetriever([make_candidate(text="a"), make_candidate(text="b")]),
                )
            ],
            scorers=[("reranker", scorer)],
        )
        with pytest.raises(MemoryPipelineError, match="reranker"):
            await manager.assemble(AssembleRequest(query_text="q"))

    @pytest.mark.asyncio
    async def test_manager_contains_retriever_failure(self):
        # Retriever failures are still contained (fewer candidates, not a dead
        # turn) — only the required scorer stage hard-fails.
        class _BoomRetriever:
            async def retrieve(self, req):
                raise RuntimeError("db blip")

        manager = MemoryManager(
            MemoryRuntime(),
            retrievers=[("recall", _BoomRetriever())],
        )
        payload = await manager.assemble(AssembleRequest(query_text="q"))
        assert payload.stats.errors and "recall" in payload.stats.errors[0]

    def test_factory_defaults_to_embedding_transport(self, monkeypatch):
        # The reranker rides the EMBEDDING endpoint, never the auxiliary — even
        # when the aux has a perfectly good base_url. Regression guard for
        # knowledge-base/knowledge/issues/openrouter_auxiliary_crashes_session_via_memory_reranker.md.
        from agent.services.memory.plugins.reranker import _build_reranker
        from shared.runtime.core.loader import RerankerConfig

        monkeypatch.setenv("EMBEDDING_BASE_URL", "https://ai.h4ll.app/v1")
        monkeypatch.setenv("EMBEDDING_API_KEY", "embed-key")
        runtime = MemoryRuntime(
            memory_config=SimpleNamespace(reranker=RerankerConfig()),
            # aux has a usable endpoint, but it must be ignored
            auxiliary_config=SimpleNamespace(
                base_url="https://aux/v1", api_key="aux-key"
            ),
        )
        scorer = _build_reranker(runtime)
        assert scorer.endpoint == "https://ai.h4ll.app/v1/rerank"
        assert scorer.api_key == "embed-key"
        assert scorer.top_k == 64

    def test_factory_explicit_base_url_wins(self, monkeypatch):
        from agent.services.memory.plugins.reranker import _build_reranker
        from shared.runtime.core.loader import RerankerConfig

        monkeypatch.setenv("EMBEDDING_BASE_URL", "https://ai.h4ll.app/v1")
        runtime = MemoryRuntime(
            memory_config=SimpleNamespace(
                reranker=RerankerConfig(
                    base_url="https://explicit/v1", api_key="explicit-key"
                )
            ),
            auxiliary_config=SimpleNamespace(base_url="https://aux/v1"),
        )
        scorer = _build_reranker(runtime)
        assert scorer.endpoint == "https://explicit/v1/rerank"
        assert scorer.api_key == "explicit-key"

    def test_factory_openrouter_aux_no_longer_crashes(self, monkeypatch):
        # The original bug: aux = OpenRouter-direct (no base_url). Previously
        # this raised at construction and killed agent startup. With the
        # embedding fallback it builds cleanly and ignores the aux entirely.
        from agent.services.memory.plugins.reranker import _build_reranker
        from shared.runtime.core.loader import RerankerConfig

        monkeypatch.setenv("EMBEDDING_BASE_URL", "https://ai.h4ll.app/v1")
        runtime = MemoryRuntime(
            memory_config=SimpleNamespace(reranker=RerankerConfig()),
            auxiliary_config=SimpleNamespace(base_url=None, api_key=None),
        )
        scorer = _build_reranker(runtime)
        assert scorer.endpoint == "https://ai.h4ll.app/v1/rerank"

    def test_factory_no_endpoint_anywhere_raises(self, monkeypatch):
        # Genuinely unserviceable (no explicit base_url, no EMBEDDING_BASE_URL):
        # still a hard error at the plugin layer — the orchestrator pre-flight
        # (Part 3) catches this before a pod ever spawns.
        from agent.services.memory.plugins.reranker import _build_reranker
        from shared.runtime.core.loader import RerankerConfig

        monkeypatch.delenv("EMBEDDING_BASE_URL", raising=False)
        runtime = MemoryRuntime(
            memory_config=SimpleNamespace(reranker=RerankerConfig()),
            auxiliary_config=SimpleNamespace(base_url=None, api_key=None),
        )
        with pytest.raises(ValueError, match="base_url"):
            _build_reranker(runtime)

    def _runtime(self, **cfg):
        from shared.runtime.core.loader import RerankerConfig

        return MemoryRuntime(
            memory_config=SimpleNamespace(reranker=RerankerConfig(**cfg)),
            auxiliary_config=SimpleNamespace(base_url=None, api_key=None),
        )

    def test_factory_no_pin_resolves_the_qwen_default_model(self, monkeypatch):
        from agent.services.memory.plugins.reranker import _build_reranker

        monkeypatch.delenv("RERANK_MODEL", raising=False)
        monkeypatch.delenv("RERANK_BASE_URL", raising=False)
        monkeypatch.setenv("EMBEDDING_BASE_URL", "https://ai.h4ll.app/v1")
        scorer = _build_reranker(self._runtime())
        assert scorer.model == "qwen3-reranker-8b"

    def test_factory_catalog_pin_wins_over_embedding_transport(self, monkeypatch):
        # The `rerank` catalog slot arrives as RERANK_* env at dispatch. It
        # selects the host AND the key as a pair — the embedding key must not
        # follow the request to a different host.
        from agent.services.memory.plugins.reranker import _build_reranker

        monkeypatch.setenv("EMBEDDING_BASE_URL", "https://ai.h4ll.app/v1")
        monkeypatch.setenv("EMBEDDING_API_KEY", "embed-key")
        monkeypatch.setenv("RERANK_MODEL", "rerank-english-v3.0")
        monkeypatch.setenv("RERANK_BASE_URL", "https://api.cohere.com/v2")
        monkeypatch.delenv("RERANK_API_KEY", raising=False)
        scorer = _build_reranker(self._runtime())
        assert scorer.model == "rerank-english-v3.0"
        assert scorer.endpoint == "https://api.cohere.com/v2/rerank"
        assert scorer.api_key is None
        monkeypatch.setenv("RERANK_API_KEY", "cohere-key")
        assert _build_reranker(self._runtime()).api_key == "cohere-key"

    def test_factory_rerank_model_alone_rides_the_embedding_endpoint(self, monkeypatch):
        # A system-provider rerank row delivers a model (and maybe a key) but
        # no base_url: the transport stays the embedding endpoint's pair.
        from agent.services.memory.plugins.reranker import _build_reranker

        monkeypatch.setenv("EMBEDDING_BASE_URL", "https://ai.h4ll.app/v1")
        monkeypatch.setenv("EMBEDDING_API_KEY", "embed-key")
        monkeypatch.setenv("RERANK_MODEL", "qwen3-reranker-8b")
        monkeypatch.delenv("RERANK_BASE_URL", raising=False)
        monkeypatch.setenv("RERANK_API_KEY", "stray-key")
        scorer = _build_reranker(self._runtime())
        assert scorer.model == "qwen3-reranker-8b"
        assert scorer.endpoint == "https://ai.h4ll.app/v1/rerank"
        assert scorer.api_key == "embed-key"

    def test_factory_explicit_config_beats_the_catalog_pin(self, monkeypatch):
        from agent.services.memory.plugins.reranker import _build_reranker

        monkeypatch.setenv("RERANK_MODEL", "pinned")
        monkeypatch.setenv("RERANK_BASE_URL", "https://pinned/v1")
        monkeypatch.setenv("RERANK_API_KEY", "pinned-key")
        scorer = _build_reranker(
            self._runtime(model="explicit", base_url="https://explicit/v1", api_key="k")
        )
        assert (scorer.model, scorer.endpoint, scorer.api_key) == (
            "explicit",
            "https://explicit/v1/rerank",
            "k",
        )

    def test_parse_reranker_config(self):
        cfg = _parse_memory_config(
            {
                "reranker": {
                    "model": "rerank-1",
                    "top_k": 32,
                    "timeout": 5,
                    "keep_pinned_first": False,
                }
            }
        )
        assert cfg.reranker.model == "rerank-1"
        assert cfg.reranker.top_k == 32
        assert cfg.reranker.timeout == 5.0
        assert cfg.reranker.keep_pinned_first is False
        assert cfg.reranker.base_url is None
        # defaults when section absent
        # null = the `rerank` catalog pin (RERANK_MODEL) resolves at bind time
        assert _parse_memory_config({}).reranker.model is None

    def test_reranker_is_registered(self):
        assert "reranker" in available_memory_plugins("scorer")["scorer"]


# ---------------------------------------------------------------------------
# BoundedPolicy (Phase 3 slice 3)
# ---------------------------------------------------------------------------


class TestBoundedPolicy:
    def _policy(self, **kwargs):
        from agent.services.memory.plugins.bounded import BoundedPolicy

        return BoundedPolicy(**kwargs)

    def _req(self):
        return AssembleRequest(query_text="what is my dog's name?")

    @pytest.mark.asyncio
    async def test_max_items_keeps_scored_order_and_passes_knowledge(self):
        items = [
            make_scored("m1", record_id="m1"),
            make_scored("kb1", kind="knowledge", record_id="k1"),
            make_scored("m2", record_id="m2"),
            make_scored("m3", record_id="m3"),
            make_scored("kb2", kind="knowledge", record_id="k2"),
        ]
        kept = await self._policy(max_items=2).apply(self._req(), items)
        assert [i.candidate.text for i in kept] == ["m1", "kb1", "m2", "kb2"]

    @pytest.mark.asyncio
    async def test_max_tokens_walks_token_counts(self):
        items = [
            Scored(candidate=make_candidate(text="a", tokens=40, record_id="a")),
            Scored(candidate=make_candidate(text="b", tokens=40, record_id="b")),
            Scored(candidate=make_candidate(text="c", tokens=40, record_id="c")),
        ]
        kept = await self._policy(max_tokens=80).apply(self._req(), items)
        assert [i.candidate.text for i in kept] == ["a", "b"]

    @pytest.mark.asyncio
    async def test_zero_token_count_falls_back_to_estimate(self):
        long_text = "x" * 400  # ~100 estimated tokens
        items = [
            Scored(candidate=make_candidate(text=long_text, tokens=0, record_id="a")),
            Scored(candidate=make_candidate(text=long_text, tokens=0, record_id="b")),
        ]
        kept = await self._policy(max_tokens=120).apply(self._req(), items)
        assert len(kept) == 1

    @pytest.mark.asyncio
    async def test_both_caps_whichever_bites_first(self):
        items = [
            Scored(candidate=make_candidate(text=t, tokens=10, record_id=t))
            for t in ("a", "b", "c", "d")
        ]
        kept = await self._policy(max_items=3, max_tokens=25).apply(self._req(), items)
        assert [i.candidate.text for i in kept] == ["a", "b"]

    @pytest.mark.asyncio
    async def test_under_cap_keeps_everything(self):
        items = [make_scored("m1"), make_scored("m2")]
        kept = await self._policy(max_items=10).apply(self._req(), items)
        assert kept == items

    def test_capless_construction_raises(self):
        with pytest.raises(ValueError, match="config theatre"):
            self._policy()
        with pytest.raises(ValueError, match=">= 1"):
            self._policy(max_items=0)
        with pytest.raises(ValueError, match=">= 1"):
            self._policy(max_tokens=0)

    def test_factory_reads_bounded_config(self):
        from agent.services.memory.plugins.bounded import _build_bounded

        runtime = SimpleNamespace(
            memory_config=SimpleNamespace(
                bounded=SimpleNamespace(max_items=10, max_tokens=None)
            )
        )
        policy = _build_bounded(runtime)
        assert policy.max_items == 10
        assert policy.max_tokens is None

    def test_factory_missing_section_raises(self):
        from agent.services.memory.plugins.bounded import _build_bounded

        runtime = SimpleNamespace(memory_config=SimpleNamespace(bounded=None))
        with pytest.raises(ValueError, match="config section missing"):
            _build_bounded(runtime)

    def test_parse_bounded_config(self):
        cfg = _parse_memory_config({"bounded": {"max_items": 10, "max_tokens": 2048}})
        assert cfg.bounded.max_items == 10
        assert cfg.bounded.max_tokens == 2048
        # defaults when section absent: capless (the policy refuses to bind)
        absent = _parse_memory_config({})
        assert absent.bounded.max_items is None
        assert absent.bounded.max_tokens is None

    def test_bounded_is_registered(self):
        assert "bounded" in available_memory_plugins("policy")["policy"]

    # --- B5: include_knowledge — one token budget across memory + KB ---

    @pytest.mark.asyncio
    async def test_include_knowledge_counts_kb_tokens_against_budget(self):
        items = [
            Scored(candidate=make_candidate(text="m1", tokens=40, record_id="m1")),
            Scored(
                candidate=make_candidate(
                    kind="knowledge", text="k1", tokens=40, record_id="k1"
                )
            ),
            Scored(
                candidate=make_candidate(
                    kind="knowledge", text="k2", tokens=40, record_id="k2"
                )
            ),
        ]
        kept = await self._policy(max_tokens=80, include_knowledge=True).apply(
            self._req(), items
        )
        assert [i.candidate.text for i in kept] == ["m1", "k1"]

    @pytest.mark.asyncio
    async def test_include_knowledge_max_items_stays_memory_only(self):
        # Knowledge never counts toward max_items — only the token budget.
        items = [
            Scored(
                candidate=make_candidate(
                    kind="knowledge", text="k1", tokens=10, record_id="k1"
                )
            ),
            Scored(candidate=make_candidate(text="m1", tokens=10, record_id="m1")),
            Scored(candidate=make_candidate(text="m2", tokens=10, record_id="m2")),
        ]
        kept = await self._policy(
            max_items=1, max_tokens=100, include_knowledge=True
        ).apply(self._req(), items)
        assert [i.candidate.text for i in kept] == ["k1", "m1"]

    @pytest.mark.asyncio
    async def test_include_knowledge_zero_token_kb_uses_estimate(self):
        # Production kb_notes candidates deliberately carry token_count=0
        # (legacy uncapped KB block) — the budget walk must estimate them.
        long_text = "x" * 400  # ~100 estimated tokens
        items = [
            Scored(
                candidate=make_candidate(
                    kind="knowledge", text=long_text, tokens=0, record_id="k1"
                )
            ),
            Scored(
                candidate=make_candidate(
                    kind="knowledge", text=long_text, tokens=0, record_id="k2"
                )
            ),
        ]
        kept = await self._policy(max_tokens=120, include_knowledge=True).apply(
            self._req(), items
        )
        assert len(kept) == 1

    def test_include_knowledge_without_max_tokens_raises(self):
        with pytest.raises(ValueError, match="include_knowledge needs max_tokens"):
            self._policy(max_items=10, include_knowledge=True)

    def test_parse_include_knowledge(self):
        cfg = _parse_memory_config(
            {"bounded": {"max_tokens": 2048, "include_knowledge": True}}
        )
        assert cfg.bounded.include_knowledge is True
        assert _parse_memory_config({}).bounded.include_knowledge is False


class TestGatePolicy:
    def _policy(self, **kwargs):
        from agent.services.memory.plugins.gate import GatePolicy

        defaults = dict(threshold=0.05)
        defaults.update(kwargs)
        return GatePolicy(**defaults)

    def _req(self):
        return AssembleRequest(query_text="what is my dog's name?")

    def _scored(self, text, rerank=None, kind="memory", record_id="r"):
        item = make_scored(text, kind=kind, record_id=record_id)
        if rerank is not None:
            item.candidate.channel_scores["rerank"] = rerank
            item.score = rerank
        return item

    @pytest.mark.asyncio
    async def test_drops_below_threshold_keeps_above(self):
        items = [
            self._scored("relevant", rerank=0.97, record_id="m1"),
            self._scored("borderline", rerank=0.24, record_id="m2"),
            self._scored("distractor", rerank=0.0001, record_id="m3"),
        ]
        kept = await self._policy().apply(self._req(), items)
        assert [i.candidate.text for i in kept] == ["relevant", "borderline"]

    @pytest.mark.asyncio
    async def test_unscored_items_pass_through(self):
        # Scorer outage (containment passes items unscored), top_k tail,
        # pinned head — absence of evidence never empties the injection.
        items = [
            self._scored("unscored-1", record_id="m1"),
            self._scored("distractor", rerank=0.0001, record_id="m2"),
            self._scored("unscored-2", record_id="m3"),
        ]
        kept = await self._policy().apply(self._req(), items)
        assert [i.candidate.text for i in kept] == ["unscored-1", "unscored-2"]

    @pytest.mark.asyncio
    async def test_knowledge_passes_through_even_scored_below(self):
        items = [
            self._scored("kb", rerank=0.0001, kind="knowledge", record_id="k1"),
            self._scored("mem", rerank=0.0001, record_id="m1"),
        ]
        kept = await self._policy().apply(self._req(), items)
        assert [i.candidate.text for i in kept] == ["kb"]

    @pytest.mark.asyncio
    async def test_nothing_qualifies_injects_nothing(self):
        # P4: below-threshold is not "inject the best of a bad lot".
        items = [
            self._scored("d1", rerank=0.001, record_id="m1"),
            self._scored("d2", rerank=0.002, record_id="m2"),
        ]
        kept = await self._policy().apply(self._req(), items)
        assert kept == []

    @pytest.mark.asyncio
    async def test_order_preserved_and_custom_channel(self):
        items = [
            self._scored("a", record_id="m1"),
            self._scored("b", record_id="m2"),
        ]
        items[0].candidate.channel_scores["ensemble"] = 0.9
        items[1].candidate.channel_scores["ensemble"] = 0.01
        kept = await self._policy(threshold=0.5, channel="ensemble").apply(
            self._req(), items
        )
        assert [i.candidate.text for i in kept] == ["a"]

    # --- relative mode: floor = threshold × the assemble's top score ---

    @pytest.mark.asyncio
    async def test_relative_mode_scales_floor_to_top_score(self):
        # The measured case: tiny absolute confidence, strong separation
        # (probed c8f1aeed: evidence 0.0325 vs distractor max 0.0008 —
        # absolute 0.05 deletes the evidence, relative 0.1 keeps it).
        items = [
            self._scored("evidence", rerank=0.0325, record_id="m1"),
            self._scored("distractor", rerank=0.0008, record_id="m2"),
        ]
        kept = await self._policy(threshold=0.1, mode="relative").apply(
            self._req(), items
        )
        assert [i.candidate.text for i in kept] == ["evidence"]

    @pytest.mark.asyncio
    async def test_relative_mode_top_item_always_passes(self):
        # Corollary: relative gating can never inject nothing.
        items = [self._scored("weak-top", rerank=0.0001, record_id="m1")]
        kept = await self._policy(threshold=0.5, mode="relative").apply(
            self._req(), items
        )
        assert len(kept) == 1

    @pytest.mark.asyncio
    async def test_relative_mode_all_unscored_passes_everything(self):
        items = [
            self._scored("u1", record_id="m1"),
            self._scored("u2", record_id="m2"),
        ]
        kept = await self._policy(threshold=0.5, mode="relative").apply(
            self._req(), items
        )
        assert len(kept) == 2

    def test_construction_raises_without_threshold_or_channel(self):
        with pytest.raises(ValueError, match="config theatre"):
            self._policy(threshold=None)
        with pytest.raises(ValueError, match="non-empty channel"):
            self._policy(channel="")
        with pytest.raises(ValueError, match="not in"):
            self._policy(mode="sigmoid")

    def test_factory_reads_gate_config(self):
        from agent.services.memory.plugins.gate import _build_gate

        runtime = SimpleNamespace(
            memory_config=SimpleNamespace(
                gate=SimpleNamespace(threshold=0.05, channel="rerank")
            )
        )
        policy = _build_gate(runtime)
        assert policy.threshold == 0.05
        assert policy.channel == "rerank"

    def test_factory_missing_threshold_or_section_raises(self):
        from agent.services.memory.plugins.gate import _build_gate

        with pytest.raises(ValueError, match="config section missing"):
            _build_gate(SimpleNamespace(memory_config=SimpleNamespace(gate=None)))
        runtime = SimpleNamespace(
            memory_config=SimpleNamespace(
                gate=SimpleNamespace(threshold=None, channel="rerank")
            )
        )
        with pytest.raises(ValueError, match="config theatre"):
            _build_gate(runtime)

    def test_parse_gate_config(self):
        cfg = _parse_memory_config(
            {"gate": {"threshold": 0.05, "channel": "ensemble", "mode": "relative"}}
        )
        assert cfg.gate.threshold == 0.05
        assert cfg.gate.channel == "ensemble"
        assert cfg.gate.mode == "relative"
        # defaults when section absent: no threshold (the policy refuses to
        # bind), rerank channel, absolute mode
        absent = _parse_memory_config({})
        assert absent.gate.threshold is None
        assert absent.gate.channel == "rerank"
        assert absent.gate.mode == "absolute"

    def test_gate_is_registered(self):
        assert "gate" in available_memory_plugins("policy")["policy"]


class TestDigestQueryText:
    def _build(self, messages, frame=None, **kwargs):
        from agent.services.memory.query import build_digest_query_text

        return build_digest_query_text(messages, frame, **kwargs)

    def test_empty_messages_no_frame_yields_empty(self):
        # Same contract as the legacy builders: retrieve with "", not skip.
        assert self._build([]) == ""

    def test_single_human_message_is_the_question(self):
        # The harness question-time equivalence: one HumanMessage deep,
        # digest == legacy == the question text.
        assert self._build([HumanMessage(content="where do I live?")]) == (
            "where do I live?"
        )

    def test_window_takes_trailing_conversational_messages_only(self):
        messages = [
            HumanMessage(content="h1"),
            AIMessage(content="a1"),
            SystemMessage(content="system noise"),
            HumanMessage(content="h2"),
            ToolMessage(content="tool payload", tool_call_id="t1"),
            AIMessage(content="a2"),
            HumanMessage(content="h3"),
        ]
        assert self._build(messages, window=3) == "h2\na2\nh3"

    def test_per_message_clip(self):
        text = self._build([HumanMessage(content="x" * 600)], max_chars_per_message=100)
        assert text == "x" * 100

    def test_frame_appended_as_focus(self):
        from agent.services.memory import TaskFrame

        frame = TaskFrame(top_todo="implement the parser", phase_number=2)
        text = self._build([HumanMessage(content="status?")], frame)
        assert text == "status?\nimplement the parser\nphase 2 tactical"

    def test_frame_only_matches_worker_legacy_shape(self):
        from agent.services.memory import TaskFrame
        from agent.services.memory.plugins.legacy import build_worker_query_text

        frame = TaskFrame(top_todo="write tests", phase_number=3, is_strategic=True)
        # With no conversation, the digest reduces to the legacy worker
        # query modulo the join character.
        assert self._build([], frame).replace("\n", " ") == (
            build_worker_query_text(frame)
        )

    def test_multimodal_content_str_coerced_and_empties_skipped(self):
        messages = [
            HumanMessage(content=[{"type": "text", "text": "look"}]),
            AIMessage(content="   "),
        ]
        text = self._build(messages)
        assert "look" in text
        assert text.count("\n") == 0  # blank AI message contributed nothing

    def test_parse_query_config(self):
        cfg = _parse_memory_config(
            {
                "query": {
                    "digest": True,
                    "digest_window": 6,
                    "digest_max_chars_per_message": 200,
                }
            }
        )
        assert cfg.query.digest is True
        assert cfg.query.digest_window == 6
        assert cfg.query.digest_max_chars_per_message == 200
        absent = _parse_memory_config({})
        assert absent.query.digest is False
        assert absent.query.digest_window == 4
        assert absent.query.digest_max_chars_per_message == 500
