"""The MemoryManager's asynchronous retrieval (append_only, WP3).

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(D6 asynchronous retrieval, D7 single flight, D8 drain at request build, D10
the manager owns the task, D13 reranker failures stop killing turns, D32 the
in-memory pending set); plan:
knowledge-base/knowledge/plans/append_only_context_injection_plan_2026_10_05.md
(WP3).

The manager API: ``start_retrieval(req)`` runs ``assemble`` off the request
path unless one is in flight; ``take_retrieval()`` hands out the latest
finished result once; ``close_background`` / ``drain_background`` cancel and
join the task. The graph wiring is pinned in test_execute_prepared_layout.py
(worker) and test_persistent_append_only.py (session).
"""

import asyncio
import gc
import warnings
from unittest.mock import MagicMock, patch

import pytest

import agent.services.memory.manager as manager_module
from agent.services.memory import (
    AssembleRequest,
    MemoryPipelineError,
    RetrievalResult,
    TransientScorerError,
)
from agent.services.memory.manager import (
    PIPELINE_DEGRADED_STEP,
    pipeline_failure_signature,
)
from tests._memory_fixtures import (
    BrokenScorer,
    GatedRetriever,
    make_async_manager,
    make_memories,
    settle_retrieval,
)


def _req(text: str) -> AssembleRequest:
    return AssembleRequest(query_text=text)


def _memory_ids(result: RetrievalResult) -> list:
    return [
        record.id
        for block in result.payload.blocks
        if block.kind == "memory"
        for record in block.records
    ]


class TestSingleFlight:
    @pytest.mark.asyncio
    async def test_no_second_retrieval_starts_while_one_is_in_flight(self):
        retriever = GatedRetriever()
        manager = make_async_manager(retriever)

        assert manager.start_retrieval(_req("q1")) is True
        await asyncio.sleep(0)
        assert manager.retrieval_in_flight
        # Rapid requests while the first is running start nothing.
        assert manager.start_retrieval(_req("q2")) is False
        assert manager.start_retrieval(_req("q3")) is False
        await asyncio.sleep(0)
        assert retriever.queries == ["q1"]

        retriever.release()
        await settle_retrieval(manager)
        # The next request after it finished starts the next one.
        assert manager.start_retrieval(_req("q4")) is True
        await asyncio.sleep(0)
        assert retriever.queries == ["q1", "q4"]
        retriever.release()
        await settle_retrieval(manager)

        stats = manager.retrieval_stats()
        assert stats["started"] == 2
        assert stats["skipped_in_flight"] == 2
        assert stats["completed"] == 2

    @pytest.mark.asyncio
    async def test_concurrent_starts_from_several_tasks_run_one_retrieval(self):
        retriever = GatedRetriever()
        manager = make_async_manager(retriever)

        async def request(i: int) -> bool:
            await asyncio.sleep(0)
            return manager.start_retrieval(_req(f"q{i}"))

        started = await asyncio.gather(*(request(i) for i in range(5)))
        assert started.count(True) == 1
        await asyncio.sleep(0)
        assert len(retriever.queries) == 1
        retriever.release()
        await settle_retrieval(manager)


class TestDrain:
    @pytest.mark.asyncio
    async def test_take_never_waits_for_a_running_retrieval(self):
        retriever = GatedRetriever()
        manager = make_async_manager(retriever)

        manager.start_retrieval(_req("q1"))
        await asyncio.sleep(0)
        # Request N is built while the retrieval runs: nothing to take in.
        assert manager.take_retrieval() is None

        retriever.release()
        await settle_retrieval(manager)
        # Request N+1 takes the result in; it is handed out once.
        result = manager.take_retrieval()
        assert result is not None
        assert _memory_ids(result) == [m.id for m in make_memories()]
        assert result.payload.stats.latency_ms >= 0
        assert result.degraded is None
        assert result.seq == 1
        assert manager.take_retrieval() is None
        assert manager.retrieval_stats()["drained"] == 1

    @pytest.mark.asyncio
    async def test_the_latest_result_replaces_one_never_taken_in(self):
        retriever = GatedRetriever(auto=True)
        manager = make_async_manager(retriever)

        manager.start_retrieval(_req("q1"))
        await settle_retrieval(manager)
        manager.start_retrieval(_req("q2"))
        await settle_retrieval(manager)

        result = manager.take_retrieval()
        assert result.seq == 2
        assert manager.take_retrieval() is None
        stats = manager.retrieval_stats()
        assert stats["superseded"] == 1
        assert stats["drained"] == 1


class TestTeardown:
    @pytest.mark.asyncio
    async def test_close_background_cancels_and_joins_the_retrieval(self):
        retriever = GatedRetriever()
        manager = make_async_manager(retriever)
        manager.start_retrieval(_req("q1"))
        await asyncio.sleep(0)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            assert await manager.close_background() == 0
            gc.collect()

        assert retriever.cancelled == 1
        assert not manager.retrieval_in_flight
        assert manager.take_retrieval() is None
        assert manager.retrieval_stats()["cancelled"] == 1
        # Teardown closed admission: nothing starts behind the barrier.
        assert manager.start_retrieval(_req("q2")) is False
        assert retriever.queries == ["q1"]
        assert not [w for w in caught if "never" in str(w.message)]

    @pytest.mark.asyncio
    async def test_a_retrieval_that_ignores_cancellation_fails_the_barrier(self):
        release = asyncio.Event()

        class Stubborn:
            async def retrieve(self, req):
                while True:
                    try:
                        await release.wait()
                        return []
                    except asyncio.CancelledError:
                        continue

        manager = make_async_manager(Stubborn())
        manager.start_retrieval(_req("q1"))
        await asyncio.sleep(0)

        with pytest.raises(RuntimeError, match="ignored cancellation"):
            await manager.close_background(cancel_timeout=0.01)

        release.set()
        await settle_retrieval(manager)

    @pytest.mark.asyncio
    async def test_job_end_drain_cancels_the_retrieval(self):
        retriever = GatedRetriever()
        manager = make_async_manager(retriever)
        manager.start_retrieval(_req("q1"))
        await asyncio.sleep(0)

        assert await manager.drain_background(timeout=1.0) == 0

        assert retriever.cancelled == 1
        assert not manager.retrieval_in_flight

    @pytest.mark.asyncio
    async def test_a_hung_retrieval_is_cut_at_the_deadline(self, monkeypatch):
        monkeypatch.setattr(manager_module, "ASYNC_RETRIEVAL_DEADLINE_S", 0.01)
        retriever = GatedRetriever()
        manager = make_async_manager(retriever)

        manager.start_retrieval(_req("q1"))
        for _ in range(200):
            if not manager.retrieval_in_flight:
                break
            await asyncio.sleep(0.005)

        result = manager.take_retrieval()
        assert result is not None
        assert result.payload.blocks == []
        assert manager.retrieval_stats()["timed_out"] == 1
        assert retriever.cancelled == 1
        # Single flight is free again.
        assert manager.start_retrieval(_req("q2")) is True
        await asyncio.sleep(0)
        assert retriever.queries == ["q1", "q2"]
        retriever.release()
        await settle_retrieval(manager)


class TestReranker:
    @pytest.mark.asyncio
    async def test_a_transient_fault_degrades_that_retrieval_to_hybrid_order(self):
        scorer = BrokenScorer(TransientScorerError("ReadTimeout after 3 attempts"))
        manager = make_async_manager(GatedRetriever(auto=True), scorer=scorer)

        manager.start_retrieval(_req("q1"))
        await settle_retrieval(manager)

        result = manager.take_retrieval()
        assert result.degraded is None
        assert _memory_ids(result) == [m.id for m in make_memories()]
        assert any("scorer:reranker" in e for e in result.payload.stats.errors)
        assert manager.retrieval_stats()["degraded"] == 0

    @pytest.mark.asyncio
    async def test_a_structural_failure_is_reported_not_raised(self, caplog):
        scorer = BrokenScorer(ValueError("rerank route returned HTML"))
        manager = make_async_manager(
            GatedRetriever(auto=True), scorer=scorer, agent_type="writer"
        )
        archiver = MagicMock()

        with patch("agent.core.archiver.get_archiver", return_value=archiver):
            with caplog.at_level("ERROR", logger=manager_module.__name__):
                manager.start_retrieval(_req("q1"))
                await settle_retrieval(manager)

        result = manager.take_retrieval()
        assert result.payload.blocks == []
        assert result.degraded == "scorer:reranker:ValueError"
        assert manager.retrieval_stats()["degraded"] == 1
        assert any(r.levelname == "ERROR" for r in caplog.records)
        archiver.audit_step.assert_called_once()
        audit = archiver.audit_step.call_args.kwargs
        assert audit["step_type"] == PIPELINE_DEGRADED_STEP
        assert audit["job_id"] == "job-async"
        assert audit["agent_type"] == "writer"
        assert audit["data"]["signature"] == "scorer:reranker:ValueError"
        assert audit["data"]["error_type"] == "ValueError"
        assert audit["data"]["retrieval"]["degraded"] == 1

    @pytest.mark.asyncio
    async def test_one_audit_row_per_signature_per_degraded_episode(self):
        scorer = BrokenScorer(ValueError("rerank route returned HTML"))
        manager = make_async_manager(GatedRetriever(auto=True), scorer=scorer)
        archiver = MagicMock()

        async def retrieve_once() -> RetrievalResult:
            manager.start_retrieval(_req("q"))
            await settle_retrieval(manager)
            return manager.take_retrieval()

        with patch("agent.core.archiver.get_archiver", return_value=archiver):
            await retrieve_once()
            await retrieve_once()
            assert archiver.audit_step.call_count == 1
            # Recovery ends the episode; the next failure is audited again.
            scorer.error = None
            assert (await retrieve_once()).degraded is None
            scorer.error = ValueError("rerank route returned HTML")
            await retrieve_once()
            assert archiver.audit_step.call_count == 2
        assert manager.retrieval_stats()["degraded"] == 3

    @pytest.mark.asyncio
    async def test_the_synchronous_path_still_raises(self):
        """Legacy mode keeps "configured => required": assemble raises."""
        scorer = BrokenScorer(ValueError("rerank route returned HTML"))
        manager = make_async_manager(GatedRetriever(auto=True), scorer=scorer)

        with pytest.raises(MemoryPipelineError) as raised:
            await manager.assemble(_req("q"))
        assert pipeline_failure_signature(raised.value) == (
            "scorer:reranker:ValueError"
        )
        assert manager.retrieval_stats()["degraded"] == 0
