"""Legacy capture-side writers — the Phase-1 write-path transplant.

Each writer reproduces one of the scattered extraction/curation call
sites exactly (gates, kwarg shapes, state-advance semantics), so that at
cutover the graphs emit plain CaptureEvents and the behaviour stays
byte-identical. The equivalence fixtures
(tests/test_memory_capture_equivalence.py) pin every writer against a
verbatim reproduction of its legacy call site.

Site → writer map (line refs as of 2026-06-11):
- graph.py:1528-1563 worker in-loop extraction      → interval_extractor
- graph.py:2037-2054 worker phase-boundary          → phase_boundary_extractor
- graph.py:842-860   worker compaction-summary      → compaction_memory
- graph.py:3438-3444 worker queued-memory drain     → queued_memory
  (the only queue source is the todo_complete tool — tools/core/todo.py:225)
- persistent_graph.py:420-443 loop interval         → persistent_interval_extractor
- persistent_app.py _handle_archive/_handle_idle_archive → teardown_extractor
  (B11: _terminate_session emits NO extraction today; at cutover it gains
  a capture(kind="session_end") so most real endings stop skipping it)

Deliberate per-mode asymmetries preserved from the legacy code:
- The worker gate is *modulo* (`turn % interval == 0`, window = since last
  trigger) and checks the auxiliary task flags; the persistent gate is
  *elapsed* (`turn - last >= interval`, fixed-width window) and does NOT
  check the task flags. Two registered writers rather than one
  mode-switched plugin — the per-mode YAML pipelines pick theirs.
- Interval state advances at trigger time regardless of task outcome
  (the documented B1 follow-up; capture() retry semantics are a Phase-1
  backlog item, teardown extraction is the safety net).
- Writers hold interval state in-memory. For the worker that state lived
  in checkpointed graph state (`last_observed_turn`); after a resume the
  writer restarts at 0, which can only *widen* one extraction window
  (completeness > precision, window capped by _MAX_OBSERVATION_WINDOW).

The §6 sketch also listed `trigger_phrase_gen` and `cosine_dedup` as
writers — both already ride inside the transplanted calls
(ExtractMemoriesTask generates retrieval_messages; RecallStore.store
dedups on write), so they are not separate plugins here.
"""

import logging
from typing import FrozenSet

from agent.services.memory.registry import register_memory_plugin
from agent.services.memory.types import CaptureEvent, MemoryRuntime

logger = logging.getLogger(__name__)


def _aux_task_enabled(runtime: MemoryRuntime, task_name: str) -> bool:
    """The worker call sites' gate: auxiliary.enabled + tasks[name].enabled."""
    aux = runtime.auxiliary_config
    if aux is None or not aux.enabled:
        return False
    task = aux.tasks.get(task_name, None)
    return bool(task and task.enabled)


class WorkerIntervalExtractor:
    """In-loop observer extraction, worker semantics (graph.py:1528-1563).

    Modulo gate via the real `_should_extract_memories`; window spans
    from the last trigger to the current turn; phase is threaded.
    """

    event_kinds: FrozenSet[str] = frozenset({"turn_end"})

    def __init__(self, runtime: MemoryRuntime) -> None:
        self.runtime = runtime
        self._last_observed_turn = 0

    async def on_event(self, event: CaptureEvent) -> None:
        runtime = self.runtime
        if runtime.recall_store is None or runtime.memory_config is None:
            return
        if not _aux_task_enabled(runtime, "extract_memories"):
            return

        from shared.runtime.services.auxiliary import (
            _should_extract_memories,
            extract_and_store_memories,
        )

        if not _should_extract_memories(
            event.turn_count,
            runtime.memory_config.observer_interval,
            self._last_observed_turn,
        ):
            return

        last_observed = self._last_observed_turn
        # Advance at trigger time, like the legacy state update — a failed
        # extraction does not rewind (B1 follow-up; teardown is the net).
        self._last_observed_turn = event.turn_count

        await extract_and_store_memories(
            auxiliary_llm=runtime.auxiliary_llm,
            recall_store=runtime.recall_store,
            messages=event.messages,
            memory_extraction_prompt=runtime.extraction_prompt or "",
            phase=event.phase,
            source_turn_start=last_observed,
            source_turn_end=event.turn_count,
        )


class PersistentIntervalExtractor:
    """In-loop observer extraction, persistent semantics
    (persistent_graph.py:420-443).

    Elapsed gate (`turn - last >= interval`), fixed-width window, no
    phase kwarg, no auxiliary task-flag gate — exactly the legacy loop.
    """

    event_kinds: FrozenSet[str] = frozenset({"turn_end"})

    def __init__(self, runtime: MemoryRuntime) -> None:
        self.runtime = runtime
        self._last_extraction_turn = 0

    async def on_event(self, event: CaptureEvent) -> None:
        runtime = self.runtime
        if runtime.memory_config is None:
            return
        interval = runtime.memory_config.observer_interval
        if not (runtime.recall_store and runtime.auxiliary_llm and interval > 0):
            return
        claim_interval = runtime.extra.get("claim_persistent_extraction_interval")
        if claim_interval is not None:
            # Migration 0133 makes this cursor authoritative across pod/claim
            # handoffs.  The claim advances before auxiliary work, matching the
            # historical local cursor's at-most-once scheduling semantics.
            if not await claim_interval(event.turn_count, interval):
                return
        else:
            if (event.turn_count - self._last_extraction_turn) < interval:
                return

        self._last_extraction_turn = event.turn_count

        from shared.runtime.services.auxiliary import extract_and_store_memories

        await extract_and_store_memories(
            auxiliary_llm=runtime.auxiliary_llm,
            recall_store=runtime.recall_store,
            messages=event.messages,
            memory_extraction_prompt=runtime.extraction_prompt or "",
            source_turn_start=event.turn_count - interval,
            source_turn_end=event.turn_count,
        )
        logger.debug("Memory extraction triggered at turn %s", event.turn_count)


class PhaseBoundaryExtractor:
    """Phase-boundary extraction (graph.py:2037-2054, archive_phase node).

    No turn window — the legacy call passes only the phase.
    """

    event_kinds: FrozenSet[str] = frozenset({"phase_boundary"})

    def __init__(self, runtime: MemoryRuntime) -> None:
        self.runtime = runtime

    async def on_event(self, event: CaptureEvent) -> None:
        runtime = self.runtime
        if runtime.recall_store is None:
            return
        if not _aux_task_enabled(runtime, "extract_memories"):
            return

        from shared.runtime.services.auxiliary import extract_and_store_memories

        await extract_and_store_memories(
            auxiliary_llm=runtime.auxiliary_llm,
            recall_store=runtime.recall_store,
            messages=event.messages,
            memory_extraction_prompt=runtime.extraction_prompt or "",
            phase=event.phase,
        )


class PreCompactionExtractor:
    """Chunked, full-coverage extraction of the about-to-be-evicted slice.

    memory-extraction-before-compaction feature (not a legacy transplant). The
    emit snapshots ``messages[:-keep_recent]`` and fires this *before*
    ``ensure_within_limits`` replaces the detail with a lossy summary. Unlike
    the interval / phase-boundary extractors it does NOT call the 40-capped
    ``extract_and_store_memories`` — it runs ``MemoryExtractionEngine`` over the
    whole snapshot, so the *oldest* (evicted) messages are mined, not the newest
    ones compaction keeps. Gated like the other worker extractors (aux enabled +
    ``extract_memories`` task; persistent config also enables that task).
    ``MemoryManager.capture`` isolates writer exceptions, so a failed run can't
    break the compaction that scheduled it. The engine is built per event so a
    persistent session's re-resolved ``extraction_prompt`` (B1) is honoured.
    """

    event_kinds: FrozenSet[str] = frozenset({"pre_compaction"})

    def __init__(self, runtime: MemoryRuntime) -> None:
        self.runtime = runtime

    async def on_event(self, event: CaptureEvent) -> None:
        runtime = self.runtime
        if runtime.recall_store is None or not event.messages:
            return
        if not _aux_task_enabled(runtime, "extract_memories"):
            return

        from agent.services.memory.extraction_engine import MemoryExtractionEngine

        engine = MemoryExtractionEngine(
            auxiliary=runtime.auxiliary_llm,
            recall_store=runtime.recall_store,
            extraction_prompt=runtime.extraction_prompt or "",
        )
        await engine.run(event.messages, phase=event.phase)


class TeardownExtractor:
    """Final extraction on session end / idle archive
    (persistent_app.py _handle_archive + _handle_idle_archive).

    Awaited by design — the legacy teardown blocks on it; capture() is
    awaited (not fire-and-forgotten) at those call sites. The log lines
    keep their legacy text so the k3d B1 verification signals stay
    greppable. B11's `_terminate_session` gains a session_end capture at
    cutover and lands on this same writer.
    """

    event_kinds: FrozenSet[str] = frozenset({"session_end", "idle_archive"})

    def __init__(self, runtime: MemoryRuntime) -> None:
        self.runtime = runtime

    async def on_event(self, event: CaptureEvent) -> None:
        runtime = self.runtime
        if not (runtime.recall_store and runtime.auxiliary_llm and event.messages):
            return

        from shared.runtime.services.auxiliary import extract_and_store_memories

        await extract_and_store_memories(
            auxiliary_llm=runtime.auxiliary_llm,
            recall_store=runtime.recall_store,
            messages=event.messages,
            memory_extraction_prompt=runtime.extraction_prompt or "",
        )
        if event.kind == "idle_archive":
            logger.info("Idle archive: memory extraction complete")
        else:
            logger.info("Final memory extraction complete")


class CompactionMemoryWriter:
    """Store the compaction summary as a free-source memory
    (graph.py:842-860). Expects event.extra["summary"].
    """

    event_kinds: FrozenSet[str] = frozenset({"compaction"})

    def __init__(self, runtime: MemoryRuntime) -> None:
        self.runtime = runtime

    async def on_event(self, event: CaptureEvent) -> None:
        runtime = self.runtime
        summary = event.extra.get("summary")
        if runtime.recall_store is None or not summary:
            return

        await runtime.recall_store.store(
            content=summary[:2000],
            summary="Context compaction summary",
            importance=0.6,
            source="compaction",
            memory_type="factual",
            source_phase=event.phase,
        )


class QueuedMemoryWriter:
    """Flush memories queued by sync tool functions (graph.py:3438-3444 —
    today only the todo_complete tool queues, hence the event kind).
    Expects event.extra["queued_memories"] = list of store() kwarg dicts;
    per-item containment like the legacy drain loop.
    """

    event_kinds: FrozenSet[str] = frozenset({"todo_complete"})

    def __init__(self, runtime: MemoryRuntime) -> None:
        self.runtime = runtime

    async def on_event(self, event: CaptureEvent) -> None:
        runtime = self.runtime
        if runtime.recall_store is None:
            return
        for mem in event.extra.get("queued_memories", []):
            try:
                await runtime.recall_store.store(**mem)
            except Exception as e:
                logger.warning(
                    "Failed to store queued memory: %s: %s", type(e).__name__, e
                )


@register_memory_plugin(
    "writer",
    "interval_extractor",
    description="Worker in-loop observer extraction (modulo gate, gap window)",
)
def _build_interval_extractor(runtime: MemoryRuntime) -> WorkerIntervalExtractor:
    return WorkerIntervalExtractor(runtime)


@register_memory_plugin(
    "writer",
    "persistent_interval_extractor",
    description="Persistent in-loop observer extraction (elapsed gate, fixed window)",
)
def _build_persistent_interval_extractor(
    runtime: MemoryRuntime,
) -> PersistentIntervalExtractor:
    return PersistentIntervalExtractor(runtime)


@register_memory_plugin(
    "writer",
    "phase_boundary_extractor",
    description="Worker phase-boundary observer extraction",
)
def _build_phase_boundary_extractor(runtime: MemoryRuntime) -> PhaseBoundaryExtractor:
    return PhaseBoundaryExtractor(runtime)


@register_memory_plugin(
    "writer",
    "pre_compaction_extractor",
    description="Chunked full-coverage extraction of the evicted slice before compaction",
)
def _build_pre_compaction_extractor(runtime: MemoryRuntime) -> PreCompactionExtractor:
    return PreCompactionExtractor(runtime)


@register_memory_plugin(
    "writer",
    "teardown_extractor",
    description="Final extraction on session_end / idle_archive",
)
def _build_teardown_extractor(runtime: MemoryRuntime) -> TeardownExtractor:
    return TeardownExtractor(runtime)


@register_memory_plugin(
    "writer",
    "compaction_memory",
    description="Store compaction summaries as free-source memories",
)
def _build_compaction_memory(runtime: MemoryRuntime) -> CompactionMemoryWriter:
    return CompactionMemoryWriter(runtime)


@register_memory_plugin(
    "writer",
    "queued_memory",
    description="Flush tool-queued memories (todo_complete)",
)
def _build_queued_memory(runtime: MemoryRuntime) -> QueuedMemoryWriter:
    return QueuedMemoryWriter(runtime)
