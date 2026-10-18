"""Phase-1 slice-4 equivalence: capture writers ≡ legacy write-path call sites.

Each legacy extraction/curation call site is reproduced verbatim below
(gates + kwarg shapes + state-advance semantics) and run against the same
spied `extract_and_store_memories` / `recall_store.store` as the writer
driven by CaptureEvents — the await lists must be identical. Gates use the
REAL `_should_extract_memories` and REAL loader configs (the B1 lesson:
MagicMock-fabricated config attrs are how the persistent extraction bug
hid for months).

Site map and preserved per-mode asymmetries: see the module docstring of
src/services/memory/plugins/legacy_writers.py. The legacy call sites use
asyncio.create_task; the reproductions await directly — argument equality
is unaffected (args are computed at trigger time either way), and the
writers are awaited through capture() by design.
"""

import asyncio
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage

import shared.runtime.services.auxiliary as auxiliary_module
from shared.runtime.core.loader import (
    AuxiliaryConfig,
    AuxiliaryTaskConfig,
    MemoryConfig,
    MemoryPipelineConfig,
)
from shared.runtime.services.auxiliary import _should_extract_memories
from agent.services.memory import (
    CaptureEvent,
    MemoryManager,
    MemoryRuntime,
    available_memory_plugins,
)
from agent.services.memory.plugins.legacy_writers import (
    CompactionMemoryWriter,
    PersistentIntervalExtractor,
    PhaseBoundaryExtractor,
    PreCompactionExtractor,
    QueuedMemoryWriter,
    TeardownExtractor,
    WorkerIntervalExtractor,
)

EXTRACTION_PROMPT = "EXTRACTION PROMPT"


@pytest.fixture
def extract_spy(monkeypatch):
    spy = AsyncMock(return_value=3)
    monkeypatch.setattr(auxiliary_module, "extract_and_store_memories", spy)
    return spy


def make_aux_config(enabled=True, extract=True):
    return AuxiliaryConfig(
        enabled=enabled,
        tasks={
            "extract_memories": AuxiliaryTaskConfig(enabled=extract),
            "curate_knowledge": AuxiliaryTaskConfig(enabled=True),
        },
    )


def make_runtime(
    recall_store="store",
    auxiliary_llm="aux-llm",
    observer_interval=5,
    aux_enabled=True,
    extract_enabled=True,
    extraction_prompt=EXTRACTION_PROMPT,
):
    return MemoryRuntime(
        recall_store=recall_store,
        auxiliary_llm=auxiliary_llm,
        memory_config=MemoryConfig(
            enabled=True,
            observer_interval=observer_interval,
        ),
        auxiliary_config=make_aux_config(enabled=aux_enabled, extract=extract_enabled),
        extraction_prompt=extraction_prompt,
    )


def make_messages():
    return [HumanMessage(content="do the thing"), AIMessage(content="done")]


def turn_event(turn, messages, phase=2):
    return CaptureEvent(
        kind="turn_end",
        messages=messages,
        phase=phase,
        turn_count=turn,
    )


# ---------------------------------------------------------------------------
# Legacy reproductions (verbatim gates + kwarg shapes)
# ---------------------------------------------------------------------------


async def run_legacy_worker_turn(
    state,
    *,
    recall_store,
    auxiliary_llm,
    config_auxiliary,
    memory_config,
    messages,
    phase,
):
    """graph.py:1528-1606 — one execute-node turn's extraction."""
    new_turn_count = state.get("turn_count", 0) + 1
    extraction_triggered = False

    if (
        recall_store
        and config_auxiliary.enabled
        and config_auxiliary.tasks.get("extract_memories", None)
        and config_auxiliary.tasks["extract_memories"].enabled
    ):
        last_observed = state.get("last_observed_turn", 0)
        if _should_extract_memories(
            new_turn_count, memory_config.observer_interval, last_observed
        ):
            extraction_triggered = True
            await auxiliary_module.extract_and_store_memories(
                auxiliary_llm=auxiliary_llm,
                recall_store=recall_store,
                messages=messages,
                memory_extraction_prompt=EXTRACTION_PROMPT,
                phase=phase,
                source_turn_start=last_observed,
                source_turn_end=new_turn_count,
            )

    state["turn_count"] = new_turn_count
    if extraction_triggered:
        state["last_observed_turn"] = new_turn_count


async def run_legacy_persistent_turn(
    loop_state, *, recall_store, auxiliary_llm, extraction_interval, messages
):
    """persistent_graph.py:420-443 — one loop turn's extraction check."""
    turn_count = loop_state["turn_count"] + 1
    loop_state["turn_count"] = turn_count

    if (
        recall_store
        and auxiliary_llm
        and extraction_interval > 0
        and (turn_count - loop_state["last_extraction_turn"]) >= extraction_interval
    ):
        loop_state["last_extraction_turn"] = turn_count
        await auxiliary_module.extract_and_store_memories(
            auxiliary_llm=auxiliary_llm,
            recall_store=recall_store,
            messages=messages,
            memory_extraction_prompt=EXTRACTION_PROMPT,
            source_turn_start=turn_count - extraction_interval,
            source_turn_end=turn_count,
        )


async def run_legacy_phase_boundary(
    *, recall_store, auxiliary_llm, config_auxiliary, messages, phase
):
    """graph.py:2037-2054 — archive_phase extraction."""
    if (
        recall_store
        and config_auxiliary.enabled
        and config_auxiliary.tasks.get("extract_memories", None)
        and config_auxiliary.tasks["extract_memories"].enabled
    ):
        await auxiliary_module.extract_and_store_memories(
            auxiliary_llm=auxiliary_llm,
            recall_store=recall_store,
            messages=messages,
            memory_extraction_prompt=EXTRACTION_PROMPT,
            phase=phase,
        )


async def run_legacy_teardown(*, recall_store, auxiliary_llm, messages):
    """persistent_app.py _handle_archive / _handle_idle_archive extraction."""
    if recall_store and auxiliary_llm and messages:
        try:
            await auxiliary_module.extract_and_store_memories(
                auxiliary_llm=auxiliary_llm,
                recall_store=recall_store,
                messages=messages,
                memory_extraction_prompt=EXTRACTION_PROMPT,
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Worker in-loop parity
# ---------------------------------------------------------------------------


class TestWorkerIntervalParity:
    @pytest.mark.asyncio
    async def test_call_sequence_matches_legacy_over_twelve_turns(self, extract_spy):
        messages = make_messages()
        runtime = make_runtime()

        state = {"turn_count": 0}
        for _ in range(12):
            await run_legacy_worker_turn(
                state,
                recall_store=runtime.recall_store,
                auxiliary_llm=runtime.auxiliary_llm,
                config_auxiliary=runtime.auxiliary_config,
                memory_config=runtime.memory_config,
                messages=messages,
                phase=2,
            )
        legacy_calls = list(extract_spy.await_args_list)
        extract_spy.reset_mock()

        writer = WorkerIntervalExtractor(make_runtime())
        for turn in range(1, 13):
            await writer.on_event(turn_event(turn, messages))
        assert extract_spy.await_args_list == legacy_calls

        # and the sequence itself is what the legacy semantics promise:
        # modulo fires at 5 and 10 with gap windows
        assert len(legacy_calls) == 2
        assert legacy_calls[0].kwargs["source_turn_start"] == 0
        assert legacy_calls[0].kwargs["source_turn_end"] == 5
        assert legacy_calls[1].kwargs["source_turn_start"] == 5
        assert legacy_calls[1].kwargs["source_turn_end"] == 10
        assert legacy_calls[0].kwargs["phase"] == 2
        assert legacy_calls[0].kwargs["memory_extraction_prompt"] == EXTRACTION_PROMPT

    @pytest.mark.asyncio
    async def test_no_calls_when_task_disabled(self, extract_spy):
        writer = WorkerIntervalExtractor(make_runtime(extract_enabled=False))
        for turn in range(1, 11):
            await writer.on_event(turn_event(turn, make_messages()))
        extract_spy.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_calls_when_auxiliary_disabled(self, extract_spy):
        writer = WorkerIntervalExtractor(make_runtime(aux_enabled=False))
        await writer.on_event(turn_event(5, make_messages()))
        extract_spy.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_calls_without_store(self, extract_spy):
        writer = WorkerIntervalExtractor(make_runtime(recall_store=None))
        await writer.on_event(turn_event(5, make_messages()))
        extract_spy.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_state_advances_at_trigger_even_if_extraction_fails(
        self, extract_spy
    ):
        extract_spy.side_effect = RuntimeError("aux down")
        writer = WorkerIntervalExtractor(make_runtime())
        with pytest.raises(RuntimeError):
            await writer.on_event(turn_event(5, make_messages()))
        # B1 follow-up semantics: the trigger still advanced
        assert writer._last_observed_turn == 5


# ---------------------------------------------------------------------------
# Persistent in-loop parity
# ---------------------------------------------------------------------------


class TestPersistentIntervalParity:
    @pytest.mark.asyncio
    async def test_call_sequence_matches_legacy_over_twelve_turns(self, extract_spy):
        messages = make_messages()
        loop_state = {"turn_count": 0, "last_extraction_turn": 0}
        for _ in range(12):
            await run_legacy_persistent_turn(
                loop_state,
                recall_store="store",
                auxiliary_llm="aux-llm",
                extraction_interval=5,
                messages=messages,
            )
        legacy_calls = list(extract_spy.await_args_list)
        extract_spy.reset_mock()

        writer = PersistentIntervalExtractor(make_runtime())
        for turn in range(1, 13):
            await writer.on_event(turn_event(turn, messages))
        assert extract_spy.await_args_list == legacy_calls

        # elapsed gate fires at 5 and 10 with fixed-width windows
        assert len(legacy_calls) == 2
        assert legacy_calls[0].kwargs["source_turn_start"] == 0
        assert legacy_calls[0].kwargs["source_turn_end"] == 5
        assert legacy_calls[1].kwargs["source_turn_start"] == 5
        assert legacy_calls[1].kwargs["source_turn_end"] == 10
        # the legacy persistent call passes NO phase kwarg
        assert "phase" not in legacy_calls[0].kwargs

    @pytest.mark.asyncio
    async def test_no_calls_without_aux_llm_or_interval(self, extract_spy):
        writer = PersistentIntervalExtractor(make_runtime(auxiliary_llm=None))
        await writer.on_event(turn_event(5, make_messages()))
        writer_zero = PersistentIntervalExtractor(make_runtime(observer_interval=0))
        await writer_zero.on_event(turn_event(5, make_messages()))
        extract_spy.assert_not_awaited()


# ---------------------------------------------------------------------------
# Phase boundary parity
# ---------------------------------------------------------------------------


class TestPhaseBoundaryParity:
    @pytest.mark.asyncio
    async def test_call_matches_legacy(self, extract_spy):
        messages = make_messages()
        runtime = make_runtime()
        await run_legacy_phase_boundary(
            recall_store=runtime.recall_store,
            auxiliary_llm=runtime.auxiliary_llm,
            config_auxiliary=runtime.auxiliary_config,
            messages=messages,
            phase=4,
        )
        legacy_calls = list(extract_spy.await_args_list)
        extract_spy.reset_mock()

        writer = PhaseBoundaryExtractor(runtime)
        await writer.on_event(
            CaptureEvent(kind="phase_boundary", messages=messages, phase=4)
        )
        assert extract_spy.await_args_list == legacy_calls
        # no turn window on the boundary call — kwarg-shape pin
        kwargs = legacy_calls[0].kwargs
        assert kwargs["phase"] == 4
        assert "source_turn_start" not in kwargs
        assert "source_turn_end" not in kwargs

    @pytest.mark.asyncio
    async def test_disabled_task_no_call(self, extract_spy):
        writer = PhaseBoundaryExtractor(make_runtime(extract_enabled=False))
        await writer.on_event(CaptureEvent(kind="phase_boundary", phase=4))
        extract_spy.assert_not_awaited()


# ---------------------------------------------------------------------------
# Teardown parity (session_end / idle_archive)
# ---------------------------------------------------------------------------


class TestTeardownParity:
    @pytest.mark.asyncio
    async def test_session_end_matches_legacy(self, extract_spy, caplog):
        messages = make_messages()
        await run_legacy_teardown(
            recall_store="store", auxiliary_llm="aux-llm", messages=messages
        )
        legacy_calls = list(extract_spy.await_args_list)
        extract_spy.reset_mock()

        writer = TeardownExtractor(make_runtime())
        with caplog.at_level("INFO"):
            await writer.on_event(CaptureEvent(kind="session_end", messages=messages))
        assert extract_spy.await_args_list == legacy_calls
        # exactly the four legacy kwargs — no phase, no window
        assert sorted(legacy_calls[0].kwargs) == [
            "auxiliary_llm",
            "memory_extraction_prompt",
            "messages",
            "recall_store",
        ]
        # legacy log text preserved (the k3d B1 verification signal)
        assert "Final memory extraction complete" in caplog.text

    @pytest.mark.asyncio
    async def test_idle_archive_fires_with_legacy_log(self, extract_spy, caplog):
        writer = TeardownExtractor(make_runtime())
        with caplog.at_level("INFO"):
            await writer.on_event(
                CaptureEvent(kind="idle_archive", messages=make_messages())
            )
        extract_spy.assert_awaited_once()
        assert "Idle archive: memory extraction complete" in caplog.text

    @pytest.mark.asyncio
    async def test_empty_messages_or_missing_deps_skip(self, extract_spy):
        writer = TeardownExtractor(make_runtime())
        await writer.on_event(CaptureEvent(kind="session_end", messages=[]))
        writer_no_aux = TeardownExtractor(make_runtime(auxiliary_llm=None))
        await writer_no_aux.on_event(
            CaptureEvent(kind="session_end", messages=make_messages())
        )
        extract_spy.assert_not_awaited()


# ---------------------------------------------------------------------------
# Compaction + queued-memory writers (direct store sites)
# ---------------------------------------------------------------------------


class TestCompactionWriter:
    @pytest.mark.asyncio
    async def test_stores_truncated_summary_with_legacy_kwargs(self):
        store = AsyncMock()
        runtime = make_runtime(recall_store=store)
        writer = CompactionMemoryWriter(runtime)
        summary = "x" * 2500
        await writer.on_event(
            CaptureEvent(kind="compaction", phase=3, extra={"summary": summary})
        )
        store.store.assert_awaited_once_with(
            content=summary[:2000],
            summary="Context compaction summary",
            importance=0.6,
            source="compaction",
            memory_type="factual",
            source_phase=3,
        )

    @pytest.mark.asyncio
    async def test_no_summary_no_store(self):
        store = AsyncMock()
        writer = CompactionMemoryWriter(make_runtime(recall_store=store))
        await writer.on_event(CaptureEvent(kind="compaction", phase=3))
        store.store.assert_not_awaited()


class TestQueuedMemoryWriter:
    @pytest.mark.asyncio
    async def test_stores_each_queued_memory(self):
        store = AsyncMock()
        writer = QueuedMemoryWriter(make_runtime(recall_store=store))
        mems = [
            {
                "content": "Completed: fix the bug\nOutcome: patched",
                "importance": 0.7,
                "source": "todo",
                "memory_type": "procedural",
                "source_phase": 2,
            },
            {"content": "another", "source": "todo"},
        ]
        await writer.on_event(
            CaptureEvent(kind="todo_complete", extra={"queued_memories": mems})
        )
        assert store.store.await_count == 2
        assert store.store.await_args_list[0].kwargs == mems[0]
        assert store.store.await_args_list[1].kwargs == mems[1]

    @pytest.mark.asyncio
    async def test_per_item_containment_matches_legacy_drain(self):
        store = AsyncMock()
        store.store.side_effect = [RuntimeError("dup"), None]
        writer = QueuedMemoryWriter(make_runtime(recall_store=store))
        await writer.on_event(
            CaptureEvent(
                kind="todo_complete",
                extra={"queued_memories": [{"content": "a"}, {"content": "b"}]},
            )
        )
        # first failure contained, second still stored
        assert store.store.await_count == 2


# ---------------------------------------------------------------------------
# Registration + manager dispatch integration
# ---------------------------------------------------------------------------


class TestRegistrationAndDispatch:
    def test_all_legacy_writers_registered(self):
        writers = available_memory_plugins("writer")["writer"]
        for name in (
            "interval_extractor",
            "persistent_interval_extractor",
            "phase_boundary_extractor",
            "pre_compaction_extractor",
            "teardown_extractor",
            "compaction_memory",
            "queued_memory",
        ):
            assert name in writers

    @pytest.mark.asyncio
    async def test_manager_capture_routes_by_kind(self, extract_spy):
        cfg = MemoryConfig(
            enabled=True,
            observer_interval=5,
            pipeline=MemoryPipelineConfig(writers=["interval_extractor"]),
        )
        runtime = make_runtime()
        runtime.memory_config = cfg
        manager = MemoryManager.from_config(cfg, runtime)

        await manager.capture(turn_event(5, make_messages()))
        extract_spy.assert_awaited_once()

        # phase_boundary does not reach the interval writer
        extract_spy.reset_mock()
        await manager.capture(
            CaptureEvent(kind="phase_boundary", messages=make_messages(), phase=1)
        )
        extract_spy.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_retired_writer_name_is_skipped(self, extract_spy, caplog):
        # A frozen config (a resumed job's resolved_config) still lists the
        # removed memory assembler; binding skips it instead of failing.
        cfg = MemoryConfig(
            enabled=True,
            observer_interval=5,
            pipeline=MemoryPipelineConfig(
                writers=["interval_extractor", "memory_assembler"]
            ),
        )
        runtime = make_runtime()
        runtime.memory_config = cfg
        with caplog.at_level("WARNING"):
            manager = MemoryManager.from_config(cfg, runtime)

        assert manager.pipeline_summary()["writers"] == ["interval_extractor"]
        assert "memory_assembler" in caplog.text
        await manager.capture(turn_event(5, make_messages()))
        extract_spy.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_writer_failure_contained_by_manager(self, extract_spy):
        extract_spy.side_effect = RuntimeError("aux exploded")
        cfg = MemoryConfig(
            enabled=True,
            observer_interval=5,
            pipeline=MemoryPipelineConfig(writers=["interval_extractor"]),
        )
        runtime = make_runtime()
        runtime.memory_config = cfg
        manager = MemoryManager.from_config(cfg, runtime)
        # must not raise
        await manager.capture(turn_event(5, make_messages()))


# ---------------------------------------------------------------------------
# pre_compaction extractor writer (memory-extraction-before-compaction, Slice 3)
# ---------------------------------------------------------------------------


class _EngineSpy:
    """Stand-in for MemoryExtractionEngine — records construction + run()."""

    instances: list = []

    def __init__(self, *, auxiliary=None, recall_store=None, extraction_prompt=None):
        self.auxiliary = auxiliary
        self.recall_store = recall_store
        self.extraction_prompt = extraction_prompt
        self.run = AsyncMock(return_value=2)
        _EngineSpy.instances.append(self)


@pytest.fixture
def engine_spy(monkeypatch):
    import agent.services.memory.extraction_engine as ee

    _EngineSpy.instances = []
    monkeypatch.setattr(ee, "MemoryExtractionEngine", _EngineSpy)
    return _EngineSpy


def precompact_event(messages, phase=2):
    return CaptureEvent(kind="pre_compaction", messages=messages, phase=phase)


class TestPreCompactionExtractorWriter:
    """The writer runs MemoryExtractionEngine (NOT the 40-capped
    extract_and_store_memories) over the snapshot, gated like the worker
    extractors."""

    @pytest.mark.asyncio
    async def test_runs_engine_over_snapshot(self, engine_spy):
        writer = PreCompactionExtractor(make_runtime())
        msgs = make_messages()
        await writer.on_event(precompact_event(msgs, phase=4))

        assert len(engine_spy.instances) == 1
        eng = engine_spy.instances[0]
        assert eng.recall_store == "store"
        assert eng.auxiliary == "aux-llm"
        assert eng.extraction_prompt == EXTRACTION_PROMPT
        eng.run.assert_awaited_once()
        args, kwargs = eng.run.await_args
        assert args[0] is msgs  # full snapshot, no 40-cap
        assert kwargs["phase"] == 4

    @pytest.mark.asyncio
    async def test_uses_engine_not_legacy_helper(self, engine_spy, extract_spy):
        await PreCompactionExtractor(make_runtime()).on_event(
            precompact_event(make_messages())
        )
        # Must bypass the tail-capped helper entirely.
        extract_spy.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_gated_by_task_flag(self, engine_spy):
        await PreCompactionExtractor(make_runtime(extract_enabled=False)).on_event(
            precompact_event(make_messages())
        )
        assert engine_spy.instances == []

    @pytest.mark.asyncio
    async def test_gated_by_aux_disabled(self, engine_spy):
        await PreCompactionExtractor(make_runtime(aux_enabled=False)).on_event(
            precompact_event(make_messages())
        )
        assert engine_spy.instances == []

    @pytest.mark.asyncio
    async def test_no_store_skips(self, engine_spy):
        await PreCompactionExtractor(make_runtime(recall_store=None)).on_event(
            precompact_event(make_messages())
        )
        assert engine_spy.instances == []

    @pytest.mark.asyncio
    async def test_empty_snapshot_skips(self, engine_spy):
        await PreCompactionExtractor(make_runtime()).on_event(precompact_event([]))
        assert engine_spy.instances == []

    @pytest.mark.asyncio
    async def test_manager_capture_routes_pre_compaction(self, engine_spy):
        runtime = make_runtime()
        manager = MemoryManager(
            runtime,
            writers=[("pre_compaction_extractor", PreCompactionExtractor(runtime))],
        )
        await manager.capture(precompact_event(make_messages(), phase=1))
        assert len(engine_spy.instances) == 1
        # a non-matching kind does not dispatch to this writer
        _EngineSpy.instances = []
        await manager.capture(turn_event(3, make_messages()))
        assert engine_spy.instances == []


class TestCaptureNowaitAndDrain:
    """capture_nowait holds a ref to the detached task; drain_background awaits
    the in-flight set (OQ-C, bounded)."""

    @pytest.mark.asyncio
    async def test_capture_nowait_holds_ref_and_runs(self, engine_spy):
        runtime = make_runtime()
        manager = MemoryManager(
            runtime,
            writers=[("pre_compaction_extractor", PreCompactionExtractor(runtime))],
        )
        task = manager.capture_nowait(precompact_event(make_messages()))
        assert task in manager._bg_tasks  # strong ref held while running
        drained = await manager.drain_background(timeout=5.0)
        assert drained == 1
        assert engine_spy.instances  # the detached capture actually ran
        assert task.done()
        assert task not in manager._bg_tasks  # done-callback cleaned up

    @pytest.mark.asyncio
    async def test_drain_background_empty_is_zero(self):
        manager = MemoryManager(make_runtime(), writers=[])
        assert await manager.drain_background(timeout=1.0) == 0

    @pytest.mark.asyncio
    async def test_close_background_cancels_joins_and_closes_admission(self):
        started = asyncio.Event()
        cancelled = asyncio.Event()

        class BlockingWriter:
            event_kinds = frozenset({"pre_compaction"})

            async def on_event(self, _event):
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

        manager = MemoryManager(
            make_runtime(),
            writers=[("blocking", BlockingWriter())],
        )
        task = manager.capture_nowait(precompact_event(make_messages()))
        await started.wait()

        assert (
            await manager.close_background(
                drain_timeout=0.0,
                cancel_timeout=1.0,
            )
            == 1
        )
        assert task.done()
        assert cancelled.is_set()
        with pytest.raises(RuntimeError, match="background capture is closed"):
            manager.capture_nowait(precompact_event(make_messages()))
