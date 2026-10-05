"""MemoryManager — the single seam for all agent memory.

Design: knowledge-base/knowledge/features/agent_memory_overhaul.md §2.1. Both graphs hold
exactly one of these; neither touches RecallStore/KnowledgeStore directly
(Phase-1 acceptance). The manager is a binder (P6): ``from_config``
resolves named plugins from the registry and wires the pipeline — it
holds no memory logic itself.

Error philosophy (the aux-outage lesson): assemble() and capture() never
raise into the graphs — memory is an enhancement, a broken plugin must
not kill a turn — but every contained failure is logged with the
exception type and recorded in AssembleStats.errors. Loud degradation,
never silent.

Asynchronous retrieval (``context_management.injection_mode: append_only``,
WP3 of knowledge-base/knowledge/plans/append_only_context_injection_plan_2026_10_05.md;
D6-D8, D10, D13, D32 of features/append_only_context_injection.md). The
manager owns one retrieval task per conversation: ``start_retrieval`` runs
``assemble`` off the request path (single flight: none starts while one is
in flight), the finished result waits as the pending result, and the next
request build takes it in with ``take_retrieval``. The pending result is the
latest finished retrieval only: a newer one replaces an older one that was
never taken in, because the planner checks every result against the history
at that moment anyway. It lives in memory only (the durable copy in
``threads.metadata`` is WP4). The task is cancelled and joined by
``close_background`` and ``drain_background``. A structural pipeline failure
no longer fails the turn there: it is logged at ERROR, audited
(``memory_pipeline_degraded``) and counted, and that retrieval serves nothing.
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.services.memory.registry import resolve_memory_plugin
from agent.services.memory.types import (
    AssembleRequest,
    AssembleStats,
    Candidate,
    CaptureEvent,
    InjectionBlock,
    MemoryPayload,
    MemoryPipelineError,
    MemoryRuntime,
    RetrievalResult,
    Scored,
    TransientScorerError,
    _StopWatch,
)

logger = logging.getLogger(__name__)

#: (name, instance) — names kept for stats, errors, and status surfaces.
NamedPlugin = Tuple[str, Any]

#: Upper bound on one asynchronous retrieval. Single flight means a hung
#: retrieval would stop every later one; past this it is cancelled and the
#: next request starts a fresh one. Far above the reranker's own budget (10 s
#: per attempt, 3 attempts, backoff) plus the store calls.
ASYNC_RETRIEVAL_DEADLINE_S = 120.0

#: Audit step written when an asynchronous retrieval meets a structural
#: pipeline failure (D13). Custom step types follow ``memory_unavailable`` /
#: ``kb_unavailable`` (agent.core.archiver.audit_unavailable).
PIPELINE_DEGRADED_STEP = "memory_pipeline_degraded"

#: Counters of :meth:`MemoryManager.retrieval_stats`.
RETRIEVAL_COUNTERS = (
    "started",
    "skipped_in_flight",
    "completed",
    "drained",
    "superseded",
    "degraded",
    "timed_out",
    "cancelled",
)


def pipeline_failure_signature(error: MemoryPipelineError) -> str:
    """``stage:plugin:CauseType`` of a structural failure (D13 dedupe key)."""
    cause = error.__cause__ or error
    return (
        f"{error.stage or 'pipeline'}:{error.plugin or 'unknown'}:"
        f"{type(cause).__name__}"
    )


class MemoryManager:
    """Single seam for all memory: assembly (read) + capture (write)."""

    def __init__(
        self,
        runtime: MemoryRuntime,
        *,
        retrievers: Optional[List[NamedPlugin]] = None,
        scorers: Optional[List[NamedPlugin]] = None,
        policies: Optional[List[NamedPlugin]] = None,
        writers: Optional[List[NamedPlugin]] = None,
        extensions: Optional[List[NamedPlugin]] = None,
    ) -> None:
        self.runtime = runtime
        self._retrievers = retrievers or []
        self._scorers = scorers or []
        self._policies = policies or []
        self._writers = writers or []
        self._extensions = extensions or []
        #: Strong refs to detached capture() tasks (capture_nowait). Without
        #: this the event loop only holds a weak ref and a long-running task
        #: (the chunked pre_compaction extraction) can be GC'd mid-flight.
        self._bg_tasks: Set[asyncio.Task] = set()
        # Once a persistent-session claimant begins teardown, no detached
        # memory writer may be admitted behind its quiescence barrier.
        self._background_closed = False
        # Asynchronous retrieval (append_only, WP3): at most one task in
        # flight, and the latest finished result waiting to be taken in.
        self._retrieval_task: Optional[asyncio.Task] = None
        self._pending_retrieval: Optional[RetrievalResult] = None
        self._retrieval_seq = 0
        self._retrieval_counts: Dict[str, int] = dict.fromkeys(RETRIEVAL_COUNTERS, 0)
        # Structural-failure signatures already audited in the current
        # degraded episode; a successful retrieval ends the episode.
        self._audited_degradations: Dict[str, int] = {}

    # ------------------------------------------------------------------
    # Binding
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, cfg: Any, runtime: MemoryRuntime) -> "MemoryManager":
        """Bind the pipeline declared in ``memory.pipeline`` (P6).

        Args:
            cfg: MemoryConfig (duck-typed: needs ``.pipeline`` with
                retrievers/scorers/policies/writers/extensions name lists;
                an absent/None pipeline binds an empty no-op manager).
            runtime: Shared dependency bundle handed to every factory.

        Raises:
            UnknownMemoryPluginError: a configured name isn't registered —
                misconfiguration fails at bind time, not mid-turn.
        """
        pipeline = getattr(cfg, "pipeline", None)
        if runtime.memory_config is None:
            runtime.memory_config = cfg

        def _bind(kind: str, names: List[str]) -> List[NamedPlugin]:
            bound: List[NamedPlugin] = []
            for name in names:
                spec = resolve_memory_plugin(kind, name)
                bound.append((name, spec.factory(runtime)))
            return bound

        manager = cls(
            runtime,
            retrievers=_bind("retriever", getattr(pipeline, "retrievers", []) or []),
            scorers=_bind("scorer", getattr(pipeline, "scorers", []) or []),
            policies=_bind("policy", getattr(pipeline, "policies", []) or []),
            writers=_bind("writer", getattr(pipeline, "writers", []) or []),
            extensions=_bind("extension", getattr(pipeline, "extensions", []) or []),
        )
        logger.info(
            "MemoryManager bound: %s",
            manager.pipeline_summary(),
        )
        return manager

    def pipeline_summary(self) -> dict:
        """Bound plugin names per stage — status surfaces + bind logging."""
        return {
            "retrievers": [name for name, _ in self._retrievers],
            "scorers": [name for name, _ in self._scorers],
            "policies": [name for name, _ in self._policies],
            "writers": [name for name, _ in self._writers],
            "extensions": [name for name, _ in self._extensions],
        }

    # ------------------------------------------------------------------
    # Read side
    # ------------------------------------------------------------------

    async def assemble(self, req: AssembleRequest) -> MemoryPayload:
        """Run the read pipeline: retrievers → scorers → policies → blocks.

        Order-preserving by default: with no scorers/policies bound, items
        flow through in retriever order (the legacy stores already return
        ranked results). Retriever/policy failures are contained per plugin — a
        failing retriever contributes nothing, a failing policy passes items
        through unchanged — and recorded in stats.errors.

        Scorer failures split by cause: a *transient* transport fault that
        outlasted the scorer's bounded retries (``TransientScorerError``)
        degrades that one turn to the pre-scorer order — loud, recorded in
        stats.errors, retried next turn. A *structural* failure (wrong
        route/auth/response shape) is NOT contained: a configured scorer (the
        reranker) is required, so it raises ``MemoryPipelineError`` rather
        than silently serving legacy order behind the user's back ("configured
        ⇒ required" — the caller fails the turn loud). See
        knowledge-base/knowledge/issues/openrouter_auxiliary_crashes_session_via_memory_reranker.md
        and knowledge-base/knowledge/issues/reranker_transient_fault_hard_fails_job.md.
        In append_only mode ``start_retrieval`` runs this off the request
        path and reports that error instead of raising it (D13).
        """
        watch = _StopWatch()
        stats = AssembleStats()

        try:
            candidates: List[Candidate] = []
            for name, retriever in self._retrievers:
                try:
                    found = await retriever.retrieve(req) or []
                    for candidate in found:
                        if candidate.retriever is None:
                            candidate.retriever = name
                    stats.per_retriever[name] = len(found)
                    candidates.extend(found)
                except Exception as e:
                    self._record_failure(stats, "retriever", name, e)
                    stats.per_retriever[name] = 0
            stats.candidates_total = len(candidates)

            items: List[Scored] = [Scored(candidate=c) for c in candidates]

            for name, scorer in self._scorers:
                try:
                    items = await scorer.score(req, items)
                except TransientScorerError as e:
                    # The network blipped and outlasted the scorer's bounded
                    # retries — serve THIS turn in pre-scorer (hybrid) order
                    # instead of killing the job over a transport fault.
                    # Loud: stats.errors + warning log; the next turn goes
                    # back through the scorer. Structural failures (wrong
                    # route/auth/shape) don't take this path.
                    self._record_failure(stats, "scorer", name, e)
                except Exception as e:
                    # Configured scorer (reranker) is required — a structural
                    # failure must fail loud, NOT degrade to legacy order
                    # silently. Escapes the kernel backstop below via
                    # MemoryPipelineError.
                    self._record_failure(stats, "scorer", name, e)
                    raise MemoryPipelineError(
                        f"required memory scorer '{name}' failed at runtime: "
                        f"{type(e).__name__}: {e}",
                        stage="scorer",
                        plugin=name,
                    ) from e

            for name, policy in self._policies:
                try:
                    items = await policy.apply(req, items)
                except Exception as e:
                    self._record_failure(stats, "policy", name, e)

            blocks = self._render_blocks(req, items)

            stats.injected_total = sum(len(b.items) for b in blocks)
            stats.tokens_injected = sum(b.token_count for b in blocks)
            stats.blocks = len(blocks)
            stats.latency_ms = watch.elapsed_ms()
            return MemoryPayload(blocks=blocks, stats=stats)

        except MemoryPipelineError:
            # Required-stage failure — propagate past the kernel backstop so the
            # caller fails the turn loud rather than serving half-working memory.
            raise
        except Exception as e:
            # Kernel bug backstop — a genuine kernel bug must never kill a turn.
            self._record_failure(stats, "assemble", "kernel", e)
            stats.latency_ms = watch.elapsed_ms()
            return MemoryPayload(blocks=[], stats=stats)

    def _render_blocks(
        self, req: AssembleRequest, items: List[Scored]
    ) -> List[InjectionBlock]:
        """Turn the final selection into ready-to-inject blocks.

        Injection message mechanics live here (manager-internal, not a
        plugin stage), transplanted unchanged from the legacy worker
        execute path: memory → RecallStore.assemble_memory_block + the
        synthetic ``recall_memories`` pair; knowledge → KnowledgeStore.
        assemble_knowledge_block + the synthetic ``kb_search`` pair. The
        exact call shapes matter for byte-equivalence: ``model=`` only —
        the legacy calls never pass a budget (the memory list is already
        budget-fit by the store; the KB block is uncapped, B5/Phase 3).
        Lazy imports match the legacy call-site style.
        """
        blocks: List[InjectionBlock] = []
        by_kind: dict = {}
        for scored in items:
            by_kind.setdefault(scored.candidate.kind, []).append(scored)

        for kind, group in by_kind.items():
            records = [
                s.candidate.record for s in group if s.candidate.record is not None
            ]
            content = ""
            messages: List[Any] = []

            if kind == "memory" and records:
                from agent.core.memory_injection import create_memory_injection_messages
                from shared.runtime.services.recall_store import RecallStore

                content = RecallStore.assemble_memory_block(records, model=req.model)
                if content:
                    messages = list(create_memory_injection_messages(content))
            elif kind == "knowledge" and records:
                from agent.core.knowledge_injection import (
                    create_knowledge_injection_messages,
                )
                from shared.runtime.services.knowledge_store import KnowledgeStore

                content = KnowledgeStore.assemble_knowledge_block(
                    records, model=req.model
                )
                if content:
                    messages = list(create_knowledge_injection_messages(content))
            elif kind not in ("memory", "knowledge"):
                logger.warning(
                    "No renderer for injection kind '%s' — "
                    "block carries provenance only",
                    kind,
                )

            blocks.append(
                InjectionBlock(
                    kind=kind,
                    content=content,
                    messages=messages,
                    token_count=sum(s.candidate.token_count for s in group),
                    items=[
                        {
                            "retriever": s.candidate.retriever,
                            "bucket": s.candidate.bucket,
                            "score": s.score,
                            "token_count": s.candidate.token_count,
                            "record_id": str(getattr(s.candidate.record, "id", None)),
                        }
                        for s in group
                    ],
                    records=records,
                )
            )
        return blocks

    # ------------------------------------------------------------------
    # Asynchronous retrieval (append_only, WP3)
    # ------------------------------------------------------------------

    @property
    def retrieval_in_flight(self) -> bool:
        """Whether a retrieval task is running."""
        task = self._retrieval_task
        return task is not None and not task.done()

    def start_retrieval(self, req: AssembleRequest) -> bool:
        """Start ``assemble(req)`` off the request path (D6, D7, D10).

        Never awaits. Returns True when a retrieval started; False when one
        is still in flight (single flight: this request's query is not run,
        the next request may start one) or when teardown closed the manager.
        The result waits for :meth:`take_retrieval`.
        """
        if self._background_closed:
            return False
        if self.retrieval_in_flight:
            self._retrieval_counts["skipped_in_flight"] += 1
            return False
        self._retrieval_seq += 1
        self._retrieval_counts["started"] += 1
        task = asyncio.create_task(
            self._run_retrieval(req, self._retrieval_seq),
            name=f"memory-retrieval-{self._retrieval_seq}",
        )
        self._retrieval_task = task
        task.add_done_callback(self._retrieval_settled)
        return True

    def take_retrieval(self) -> Optional[RetrievalResult]:
        """Take in the pending retrieval result, if one has finished (D8).

        Never awaits; the request build calls it. The result is the latest
        finished retrieval and is handed out once. The caller plans from its
        records against the history as it is now, so an older or repeated
        result is harmless.
        """
        result, self._pending_retrieval = self._pending_retrieval, None
        if result is not None:
            self._retrieval_counts["drained"] += 1
        return result

    def retrieval_stats(self) -> Dict[str, int]:
        """Counters of the asynchronous retrieval (status, audit rows)."""
        return dict(self._retrieval_counts)

    async def cancel_retrieval(self, timeout: float = 5.0) -> bool:
        """Cancel and join the running retrieval; True if one was running.

        Raises ``RuntimeError`` when the task ignores cancellation: it may
        still write (the TTL tick of ``recall_two_tier``), so a teardown
        barrier must not report quiescence.
        """
        task = self._retrieval_task
        if task is None or task.done():
            return False
        task.cancel()
        _, pending = await asyncio.wait({task}, timeout=max(0.0, timeout))
        if pending:
            raise RuntimeError("memory retrieval task ignored cancellation")
        return True

    async def _run_retrieval(self, req: AssembleRequest, seq: int) -> None:
        """One retrieval; its result replaces any result not yet taken in."""
        watch = _StopWatch()
        degraded: Optional[str] = None
        try:
            payload = await asyncio.wait_for(
                self.assemble(req), timeout=ASYNC_RETRIEVAL_DEADLINE_S
            )
        except MemoryPipelineError as e:
            # D13: a structural failure no longer fails the turn. Loud
            # (ERROR log, audit, counter); this retrieval serves nothing.
            degraded = pipeline_failure_signature(e)
            self._report_degraded(e, degraded)
            payload = self._empty_payload(watch, f"pipeline: {e}")
        except (asyncio.TimeoutError, TimeoutError):
            self._retrieval_counts["timed_out"] += 1
            logger.warning(
                "Memory retrieval %d exceeded %gs and was cancelled; the "
                "next request starts a new one",
                seq,
                ASYNC_RETRIEVAL_DEADLINE_S,
            )
            payload = self._empty_payload(watch, "retrieval: deadline exceeded")
        else:
            if self._audited_degradations:
                logger.info(
                    "Memory pipeline recovered after %s",
                    sorted(self._audited_degradations),
                )
                self._audited_degradations.clear()
        if self._pending_retrieval is not None:
            self._retrieval_counts["superseded"] += 1
        self._pending_retrieval = RetrievalResult(
            payload=payload,
            seq=seq,
            finished_at=time.monotonic(),
            degraded=degraded,
        )
        self._retrieval_counts["completed"] += 1

    @staticmethod
    def _empty_payload(watch: _StopWatch, error: str) -> MemoryPayload:
        stats = AssembleStats(errors=[error])
        stats.latency_ms = watch.elapsed_ms()
        return MemoryPayload(blocks=[], stats=stats)

    def _retrieval_settled(self, task: "asyncio.Task") -> None:
        """Done callback: count a cancellation, retrieve the outcome."""
        if self._retrieval_task is task:
            self._retrieval_task = None
        if task.cancelled():
            self._retrieval_counts["cancelled"] += 1
            return
        error = task.exception()
        if error is not None:
            # _run_retrieval contains everything assemble can raise; this
            # is a manager bug, never a reason to fail a turn.
            logger.error(
                "Memory retrieval task failed: %s: %s",
                type(error).__name__,
                error,
            )

    def _report_degraded(self, error: MemoryPipelineError, signature: str) -> None:
        """D13: log, count and audit a structural failure without raising.

        Every occurrence logs at ERROR and counts. The audit row is written
        once per signature per degraded episode (until a retrieval succeeds),
        so a broken reranker does not write one row per request.
        """
        self._retrieval_counts["degraded"] += 1
        occurrences = self._audited_degradations.get(signature, 0) + 1
        self._audited_degradations[signature] = occurrences
        logger.error(
            "Memory pipeline degraded (%s): %s — this retrieval serves no "
            "memory; the turn continues (D13, occurrence %d)",
            signature,
            error,
            occurrences,
        )
        if occurrences > 1:
            return
        job_id = self.runtime.job_id
        if not job_id:
            return
        try:
            from agent.core.archiver import get_archiver

            archiver = get_archiver()
            if archiver is None:
                return
            cause = error.__cause__ or error
            archiver.audit_step(
                job_id=str(job_id),
                agent_type=self.runtime.agent_type or "",
                step_type=PIPELINE_DEGRADED_STEP,
                node_name="memory_retrieval",
                iteration=0,
                data={
                    "component": f"{error.stage or 'pipeline'}:{error.plugin or 'unknown'}",
                    "signature": signature,
                    "error": str(cause),
                    "error_type": type(cause).__name__,
                    "retrieval": self.retrieval_stats(),
                },
            )
        except Exception as e:  # pragma: no cover - audit never breaks retrieval
            logger.debug(
                "memory_pipeline_degraded audit failed: %s: %s", type(e).__name__, e
            )

    # ------------------------------------------------------------------
    # Write side
    # ------------------------------------------------------------------

    async def capture(self, event: CaptureEvent) -> None:
        """Dispatch a lifecycle event to subscribed writers.

        Awaitable but safe to fire-and-forget (asyncio.create_task) —
        never raises; per-writer failures are contained and logged with
        the exception type.
        """
        for name, writer in self._writers:
            try:
                kinds = writer.event_kinds
                if event.kind not in kinds:
                    continue
            except Exception as e:
                logger.warning(
                    "Memory writer '%s' has a broken event_kinds: %s: %s",
                    name,
                    type(e).__name__,
                    e,
                )
                continue
            try:
                await writer.on_event(event)
            except Exception as e:
                logger.warning(
                    "Memory writer '%s' failed on %s (non-fatal): %s: %s",
                    name,
                    event.kind,
                    type(e).__name__,
                    e,
                )

    def capture_nowait(self, event: CaptureEvent) -> "asyncio.Task":
        """Fire ``capture(event)`` as a ref-held detached task.

        The graphs use this for the ``pre_compaction`` snapshot so a compaction
        proceeds immediately while the chunked extraction runs behind it. The
        task is kept in ``self._bg_tasks`` (and removed on completion) so the
        event loop can't GC a long-running extraction mid-flight — the bare
        ``create_task(capture(...))`` at the legacy call sites holds no ref.
        """
        if self._background_closed:
            raise RuntimeError("memory background capture is closed")
        task = asyncio.create_task(self.capture(event))
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        return task

    @property
    def background_tasks_inflight(self) -> int:
        """Number of detached captures not yet at their settlement boundary."""

        return sum(1 for task in self._bg_tasks if not task.done())

    async def drain_background(self, timeout: Optional[float] = None) -> int:
        """Await in-flight ``capture_nowait`` tasks; returns how many were pending.

        Called at worker job-end (OQ-C) so a ``pre_compaction`` extraction
        scheduled just before the job freezes gets a chance to persist. Bounded
        by ``timeout`` — a hung aux endpoint must not wedge job completion; on
        timeout the still-running tasks stay detached (best-effort) and the job
        proceeds. ``capture()`` never raises, so gathering is always clean.

        A running retrieval (append_only) is cancelled first: no later request
        takes its result in. It is not counted in the return value.
        """
        try:
            await self.cancel_retrieval()
        except RuntimeError as e:
            logger.warning("drain_background: %s; leaving it detached", e)
        pending = [t for t in self._bg_tasks if not t.done()]
        if not pending:
            return 0
        try:
            await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True), timeout=timeout
            )
        except (asyncio.TimeoutError, TimeoutError):
            logger.warning(
                "drain_background: %d memory task(s) still running after %ss; "
                "leaving detached",
                sum(1 for t in pending if not t.done()),
                timeout,
            )
        return len(pending)

    async def close_background(
        self,
        *,
        drain_timeout: float = 10.0,
        cancel_timeout: float = 5.0,
    ) -> int:
        """Terminally quiesce detached capture tasks.

        Unlike :meth:`drain_background`, this is an ownership boundary, not a
        best-effort job-end convenience.  It first closes admission, gives
        already-started writes a bounded chance to finish, then cancels and
        *joins* any remainder.  Returning therefore proves that no old session
        task can write RecallStore after a queue transition.  A task that
        suppresses cancellation is surfaced to the caller so the physical
        lease remains held for the reaper rather than exposing a successor.

        The retrieval task (append_only) is cancelled at once, not drained:
        it only reads, but its ``recall_two_tier`` TTL tick writes, so it is
        joined like the captures and a retrieval that ignores cancellation
        fails the barrier too. It is not counted in the return value.
        """

        self._background_closed = True
        retrieval_stuck: Optional[RuntimeError] = None
        try:
            await self.cancel_retrieval(timeout=cancel_timeout)
        except RuntimeError as e:
            retrieval_stuck = e
        pending = {task for task in self._bg_tasks if not task.done()}
        if not pending:
            if retrieval_stuck is not None:
                raise retrieval_stuck
            return 0
        count = len(pending)
        _, pending = await asyncio.wait(pending, timeout=max(0.0, drain_timeout))
        if pending:
            for task in pending:
                task.cancel()
            done, pending = await asyncio.wait(
                pending,
                timeout=max(0.0, cancel_timeout),
            )
            # Retrieve contained task results to avoid late warning emission.
            for task in done:
                try:
                    task.result()
                except (asyncio.CancelledError, Exception):
                    pass
        if pending:
            raise RuntimeError(
                f"{len(pending)} memory background task(s) ignored cancellation"
            )
        if retrieval_stuck is not None:
            raise retrieval_stuck
        return count

    # ------------------------------------------------------------------
    # Model-facing extensions
    # ------------------------------------------------------------------

    def extension_tools(self) -> List[Any]:
        """Tools contributed by bound extensions (may be empty — P0)."""
        tools: List[Any] = []
        for name, extension in self._extensions:
            try:
                tools.extend(extension.tools() or [])
            except Exception as e:
                logger.warning(
                    "Memory extension '%s' failed to provide tools: %s: %s",
                    name,
                    type(e).__name__,
                    e,
                )
        return tools

    # ------------------------------------------------------------------

    @staticmethod
    def _record_failure(
        stats: AssembleStats, stage: str, name: str, e: Exception
    ) -> None:
        stats.errors.append(f"{stage}:{name}: {type(e).__name__}: {e}")
        logger.warning(
            "Memory %s '%s' failed (contained): %s: %s",
            stage,
            name,
            type(e).__name__,
            e,
        )
