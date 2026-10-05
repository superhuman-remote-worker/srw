"""The session's idle-time memory prefetch and its durable pending set (WP4).

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(D24 retrieve in idle time, D32 one pending set per conversation stored with
it, D33 the agent pod runs it with a hard budget, no retries, cancelled by new
input; build notes B8, B9); plan:
knowledge-base/knowledge/plans/append_only_context_injection_plan_2026_10_05.md
(WP4).

This module pins the manager API (``prefetch``, the pending-set slot that
``take_retrieval`` merges in, ``seed_prefetch``), the no-retry / no-TTL-tick
request flags in the plugins, the stored JSON form of the set
(``agent.services.memory.pending_set``) and the exchange query. The loop
wiring is pinned in test_persistent_append_only.py (``TestIdlePrefetch``),
the executor and bundle seams in test_turn_executor.py and
test_claim_bundle.py, the SQL in test_session_pending_memory_pg.py.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from agent.core.context_injection import knowledge_item, memory_item
from agent.services.memory import AssembleRequest, RetrievalResult
from agent.services.memory.manager import (
    PREFETCH_STEP,
    merge_retrieval_results,
)
from agent.services.memory.pending_set import (
    deserialize_pending_set,
    serialize_pending_set,
)
from agent.services.memory.plugins.legacy import RecallTwoTierRetriever
from agent.services.memory.plugins.reranker import RerankerScorer
from agent.services.memory.query import build_exchange_query_text
from agent.services.memory.types import InjectionBlock, MemoryPayload, Scored
from shared.runtime.core.context_entries import make_context_entry
from shared.runtime.services.knowledge_store import KnowledgeStore
from shared.runtime.services.recall_store import MemoryRecord, RecallStore
from shared.session_pending_memory import (
    SESSION_PENDING_MEMORY_KEY,
    pending_memory_from_metadata,
    valid_pending_memory,
)
from tests._memory_fixtures import (
    BrokenScorer,
    GatedRetriever,
    make_async_manager,
    make_memories,
    make_notes,
    settle_retrieval,
)


def _prefetch_req(text: str = "q?\n\nanswer.") -> AssembleRequest:
    return AssembleRequest(query_text=text, retries=False, ttl_tick=False)


def _never() -> AsyncMock:
    return AsyncMock(return_value=False)


def _result(memories=(), notes=(), *, pending_id=None, seq=1) -> RetrievalResult:
    blocks = []
    if memories:
        blocks.append(InjectionBlock(kind="memory", records=list(memories)))
    if notes:
        blocks.append(InjectionBlock(kind="knowledge", records=list(notes)))
    return RetrievalResult(
        payload=MemoryPayload(blocks=blocks),
        seq=seq,
        source="prefetch" if pending_id else "request",
        pending_id=pending_id,
    )


# ---------------------------------------------------------------------------
# MemoryManager.prefetch
# ---------------------------------------------------------------------------


class TestPrefetch:
    @pytest.mark.asyncio
    async def test_a_finished_prefetch_becomes_the_pending_set(self):
        retriever = GatedRetriever(auto=True)
        manager = make_async_manager(retriever)

        outcome = await manager.prefetch(
            _prefetch_req(), budget_s=1.0, should_cancel=_never()
        )

        assert outcome.status == "ok"
        assert outcome.memories == len(make_memories())
        assert retriever.queries == ["q?\n\nanswer."]
        pending = manager.pending_prefetch
        assert pending is outcome.result
        assert pending.source == "prefetch" and pending.pending_id
        assert not manager.retrieval_in_flight
        # The next request takes it in, once.
        assert manager.take_retrieval() is pending
        assert manager.take_retrieval() is None
        stats = manager.retrieval_stats()
        assert stats["prefetch_started"] == 1
        assert stats["prefetch_ok"] == 1
        assert stats["prefetch_taken"] == 1
        assert stats["drained"] == 1

    @pytest.mark.asyncio
    async def test_the_budget_cuts_a_slow_retrieval_and_keeps_nothing(self):
        retriever = GatedRetriever()  # never released
        manager = make_async_manager(retriever)

        started = time.monotonic()
        outcome = await manager.prefetch(
            _prefetch_req(), budget_s=0.1, should_cancel=_never(), poll_interval_s=0.02
        )
        elapsed = time.monotonic() - started

        assert outcome.status == "timeout"
        assert 0.1 <= elapsed < 0.5
        assert 100 <= outcome.duration_ms < 500
        assert retriever.cancelled == 1
        assert manager.pending_prefetch is None
        assert not manager.retrieval_in_flight
        assert manager.retrieval_stats()["prefetch_timed_out"] == 1

    @pytest.mark.asyncio
    async def test_new_input_cancels_it_within_one_poll(self):
        retriever = GatedRetriever()  # never released
        manager = make_async_manager(retriever)
        checks = {"n": 0}
        arrived_at = {}

        async def input_arrives_on_third_check():
            checks["n"] += 1
            if checks["n"] == 3:
                arrived_at["t"] = time.monotonic()
                return True
            return False

        outcome = await manager.prefetch(
            _prefetch_req(),
            budget_s=5.0,
            should_cancel=input_arrives_on_third_check,
            poll_interval_s=0.02,
        )

        assert outcome.status == "cancelled"
        assert outcome.reason == "new_input"
        # One check before the start, then one per poll interval.
        assert checks["n"] == 3
        assert time.monotonic() - arrived_at["t"] < 0.1
        assert outcome.duration_ms < 1000
        assert retriever.cancelled == 1
        assert manager.pending_prefetch is None
        assert manager.retrieval_stats()["prefetch_cancelled"] == 1

    @pytest.mark.asyncio
    async def test_input_already_waiting_never_starts_it(self):
        retriever = GatedRetriever(auto=True)
        manager = make_async_manager(retriever)

        outcome = await manager.prefetch(
            _prefetch_req(), budget_s=1.0, should_cancel=AsyncMock(return_value=True)
        )

        assert outcome.status == "cancelled"
        assert retriever.queries == []
        assert manager.retrieval_stats()["prefetch_started"] == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("probe", "reason"),
        [
            (AsyncMock(side_effect=ConnectionError("db down")), "probe_failed"),
            (AsyncMock(return_value="interrupt"), "interrupt"),
        ],
    )
    async def test_a_failed_check_or_an_interrupt_cancels_it(self, probe, reason):
        manager = make_async_manager(GatedRetriever(auto=True))

        outcome = await manager.prefetch(
            _prefetch_req(), budget_s=1.0, should_cancel=probe
        )

        assert (outcome.status, outcome.reason) == ("cancelled", reason)

    @pytest.mark.asyncio
    async def test_a_hung_check_cancels_it(self, monkeypatch):
        import agent.services.memory.manager as manager_module

        monkeypatch.setattr(manager_module, "PREFETCH_PROBE_TIMEOUT_S", 0.05)
        manager = make_async_manager(GatedRetriever(auto=True))

        async def hang():
            await asyncio.sleep(10)

        outcome = await manager.prefetch(
            _prefetch_req(), budget_s=1.0, should_cancel=hang
        )

        assert (outcome.status, outcome.reason) == ("cancelled", "probe_timeout")

    @pytest.mark.asyncio
    async def test_single_flight_cancels_the_turns_retrieval_first(self):
        """D7: a request retrieval still running at turn end is cancelled;
        its query is older than the exchange."""
        retriever = GatedRetriever()
        manager = make_async_manager(retriever)
        manager.start_retrieval(AssembleRequest(query_text="turn query"))
        await asyncio.sleep(0)

        async def release_prefetch():
            await asyncio.sleep(0.01)
            retriever.release(1)

        releaser = asyncio.create_task(release_prefetch())
        outcome = await manager.prefetch(
            _prefetch_req("exchange"), budget_s=1.0, should_cancel=_never()
        )
        await releaser

        assert outcome.status == "ok"
        assert retriever.queries == ["turn query", "exchange"]
        assert retriever.cancelled == 1
        assert manager.retrieval_stats()["cancelled"] == 1

    @pytest.mark.asyncio
    async def test_teardown_cancels_a_running_prefetch(self):
        retriever = GatedRetriever()
        manager = make_async_manager(retriever)

        prefetch = asyncio.create_task(
            manager.prefetch(_prefetch_req(), budget_s=5.0, should_cancel=_never())
        )
        await asyncio.sleep(0.01)
        await manager.close_background()
        outcome = await prefetch

        assert outcome.status == "cancelled"
        assert retriever.cancelled == 1
        # A closed manager never starts another one.
        again = await manager.prefetch(
            _prefetch_req(), budget_s=1.0, should_cancel=_never()
        )
        assert (again.status, again.reason) == ("skipped", "closed")

    @pytest.mark.asyncio
    async def test_nothing_retrieved_keeps_no_set(self):
        manager = make_async_manager(GatedRetriever(memories=[], auto=True))

        outcome = await manager.prefetch(
            _prefetch_req(), budget_s=1.0, should_cancel=_never()
        )

        assert outcome.status == "empty"
        assert manager.pending_prefetch is None

    @pytest.mark.asyncio
    async def test_a_structural_failure_is_reported_and_serves_nothing(self):
        scorer = BrokenScorer(ValueError("rerank route returned HTML"))
        manager = make_async_manager(GatedRetriever(auto=True), scorer=scorer)
        archiver = MagicMock()

        with patch("agent.core.archiver.get_archiver", return_value=archiver):
            outcome = await manager.prefetch(
                _prefetch_req(), budget_s=1.0, should_cancel=_never()
            )

        assert (outcome.status, outcome.reason) == (
            "degraded",
            "scorer:reranker:ValueError",
        )
        assert manager.pending_prefetch is None
        assert manager.retrieval_stats()["degraded"] == 1

    @pytest.mark.asyncio
    async def test_a_zero_budget_turns_it_off(self):
        retriever = GatedRetriever(auto=True)
        manager = make_async_manager(retriever)

        outcome = await manager.prefetch(
            _prefetch_req(), budget_s=0, should_cancel=_never()
        )

        assert (outcome.status, outcome.reason) == ("skipped", "disabled")
        assert retriever.queries == []

    @pytest.mark.asyncio
    async def test_the_audit_row_carries_the_slot_time(self):
        manager = make_async_manager(GatedRetriever(auto=True), job_id="thread-9")
        outcome = await manager.prefetch(
            _prefetch_req(), budget_s=1.0, should_cancel=_never()
        )
        archiver = MagicMock()

        with patch("agent.core.archiver.get_archiver", return_value=archiver):
            manager.audit_prefetch(outcome, turn=4, slot_ms=12.5, pending_set="written")
            await asyncio.gather(*manager._audit_tasks)

        audit = archiver.audit_step.call_args.kwargs
        assert audit["step_type"] == PREFETCH_STEP
        assert audit["job_id"] == "thread-9"
        assert audit["iteration"] == 4
        assert audit["data"]["status"] == "ok"
        assert audit["data"]["slot_ms"] == 12.5
        assert audit["data"]["pending_set"] == "written"
        assert audit["data"]["retrieval"]["prefetch_ok"] == 1


class TestKeepFilter:
    @pytest.mark.asyncio
    async def test_records_the_history_already_holds_are_not_kept(self):
        memories = make_memories()
        manager = make_async_manager(GatedRetriever(auto=True))

        kept = await manager.prefetch(
            _prefetch_req(),
            budget_s=1.0,
            should_cancel=_never(),
            keep=lambda kind, record: record.id != memories[0].id,
        )
        assert kept.status == "ok" and kept.memories == 1
        assert [m.id for m in kept.result.records("memory")] == [memories[1].id]

        manager.take_retrieval()
        nothing = await manager.prefetch(
            _prefetch_req(),
            budget_s=1.0,
            should_cancel=_never(),
            keep=lambda kind, record: False,
        )
        assert (nothing.status, nothing.reason) == ("empty", "all_present")
        assert manager.pending_prefetch is None


class TestSessionSeams:
    """The session's two seams: the set a claim or setup hands over, and the
    persistent_app callbacks the loop's prefetch uses."""

    @staticmethod
    def _session(*, mode="append_only", manager=None, shell_owner_token=None):
        from agent.api.persistent_session import PersistentSession

        session = object.__new__(PersistentSession)
        session.thread_id = "thread-1"
        session.config = MagicMock()
        session.config.context_management.injection_mode = mode
        session.memory_service = manager
        session.postgres_conn = None
        session.shell_owner_token = shell_owner_token
        return session

    def test_apply_takes_a_stored_set_in_append_only_only(self):
        stored = serialize_pending_set(_result(make_memories(), pending_id="set-1"))
        manager = make_async_manager(GatedRetriever(auto=True))

        assert (
            self._session(mode="legacy", manager=manager).apply_pending_memory(stored)
            is False
        )
        assert self._session(manager=None).apply_pending_memory(stored) is False
        session = self._session(manager=manager)
        assert session.apply_pending_memory({"v": 1}) is False
        assert session.apply_pending_memory(stored) is True
        assert manager.pending_prefetch.pending_id == "set-1"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("shell_owner_token", [None, 7])
    async def test_pinned_setup_reads_the_set_and_stateless_does_not(
        self, shell_owner_token
    ):
        stored = serialize_pending_set(_result(make_memories(), pending_id="set-1"))
        manager = make_async_manager(GatedRetriever(auto=True))
        session = self._session(manager=manager, shell_owner_token=shell_owner_token)
        session.postgres_conn = MagicMock()
        session.postgres_conn.get_thread_pending_memory = AsyncMock(return_value=stored)

        await session._hydrate_pending_memory()

        pinned = shell_owner_token is None
        assert session.postgres_conn.get_thread_pending_memory.await_count == int(
            pinned
        )
        assert (manager.pending_prefetch is not None) is pinned

    @pytest.mark.asyncio
    async def test_a_failed_read_is_not_fatal(self):
        manager = make_async_manager(GatedRetriever(auto=True))
        session = self._session(manager=manager)
        session.postgres_conn = MagicMock()
        session.postgres_conn.get_thread_pending_memory = AsyncMock(
            side_effect=ConnectionError("db down")
        )

        await session._hydrate_pending_memory()

        assert manager.pending_prefetch is None

    @pytest.mark.asyncio
    async def test_the_input_check_per_lane(self, monkeypatch):
        import agent.api.persistent_app as pa

        monkeypatch.setattr(pa, "_loop_provider_admission_open", lambda: True)
        monkeypatch.setattr(pa, "_turn_idle_input_external_probe", None)
        queue: asyncio.Queue = asyncio.Queue()
        monkeypatch.setattr(pa._session_input, "_queue", queue)
        reclaim = AsyncMock(return_value=set())
        monkeypatch.setattr(pa._session_input, "reclaim_pending", reclaim)

        # Pinned: nothing queued, nothing claimable.
        monkeypatch.setenv("STATELESS_EXECUTOR", "0")
        assert await pa._loop_idle_input_arrived() is False
        reclaim.assert_awaited_once()
        # A durable delivery the reclaim queued.
        reclaim.side_effect = lambda: queue.put_nowait({"content": "next"})
        assert await pa._loop_idle_input_arrived() is True

        # Stateless: the executor's per-claim check decides; without one the
        # prefetch never holds the slot.
        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        assert await pa._loop_idle_input_arrived() is True
        probe = AsyncMock(return_value=False)
        monkeypatch.setattr(pa, "_turn_idle_input_external_probe", probe)
        assert await pa._loop_idle_input_arrived() is False
        probe.assert_awaited_once()

        # A closing admission (termination) stops it on either lane.
        monkeypatch.setattr(pa, "_loop_provider_admission_open", lambda: False)
        assert await pa._loop_idle_input_arrived() is True

    @pytest.mark.asyncio
    async def test_the_save_callback_goes_to_the_thread_row(self, monkeypatch):
        import agent.api.persistent_app as pa

        conn = MagicMock()
        conn.save_thread_pending_memory = AsyncMock(return_value=True)
        monkeypatch.setattr(pa, "_session", SimpleNamespace(postgres_conn=conn))
        monkeypatch.setattr(pa._session_identity, "_thread_id", "thread-1")

        assert await pa._loop_save_pending_memory({"v": 1, "id": "s"}, None) is True
        assert await pa._loop_save_pending_memory(None, "s") is True
        assert conn.save_thread_pending_memory.await_args_list[0].args == (
            "thread-1",
            {"v": 1, "id": "s"},
        )
        assert conn.save_thread_pending_memory.await_args_list[1].kwargs == {
            "expected_id": "s"
        }


class TestPendingSetSlot:
    @pytest.mark.asyncio
    async def test_a_request_retrieval_merges_with_the_prefetch(self):
        """One set: the request's records first, the prefetch's others after."""
        memories = make_memories()
        extra = MemoryRecord(id=uuid.UUID(int=9), content="Extra fact.")
        manager = make_async_manager(GatedRetriever(auto=True))
        manager.seed_prefetch(
            _result([memories[1], extra], make_notes()[:1], pending_id="set-1")
        )
        manager.start_retrieval(AssembleRequest(query_text="turn"))
        await settle_retrieval(manager)

        taken = manager.take_retrieval()

        assert taken.source == "merged"
        assert taken.pending_id == "set-1"
        assert [m.id for m in taken.records("memory")] == [
            memories[0].id,
            memories[1].id,
            extra.id,
        ]
        assert [n.note_id for n in taken.records("knowledge")] == ["n-001"]
        assert manager.take_retrieval() is None

    def test_merge_keeps_the_newer_ranking_and_dedupes_notes(self):
        notes = make_notes()
        newer = _result([], [notes[1]])
        prefetched = _result([], [notes[0], notes[1]], pending_id="p")

        merged = merge_retrieval_results(newer, prefetched)

        assert [n.note_id for n in merged.records("knowledge")] == ["n-002", "n-001"]

    def test_seeding_is_idempotent_and_never_replays_a_taken_set(self):
        manager = make_async_manager(GatedRetriever(auto=True))
        stored = _result(make_memories(), pending_id="set-1")

        assert manager.seed_prefetch(stored) is True
        assert manager.durable_pending_id == "set-1"
        # The same set again (a warm session at its next claim): held already.
        assert (
            manager.seed_prefetch(_result(make_memories(), pending_id="set-1")) is False
        )
        assert manager.take_retrieval() is stored
        # Taken in: the stored copy coming back (a clear that failed) is not
        # taken in a second time.
        assert (
            manager.seed_prefetch(_result(make_memories(), pending_id="set-1")) is False
        )
        assert manager.pending_prefetch is None
        # A newer stored set is.
        assert (
            manager.seed_prefetch(_result(make_memories(), pending_id="set-2")) is True
        )

    def test_a_held_set_wins_over_an_older_stored_one(self):
        manager = make_async_manager(GatedRetriever(auto=True))
        held = _result(make_memories(), pending_id="new")
        manager.seed_prefetch(held)
        manager.note_pending_saved(None)  # its write failed

        assert (
            manager.seed_prefetch(_result(make_memories(), pending_id="old")) is False
        )
        assert manager.pending_prefetch is held
        assert manager.durable_pending_id == "old"


# ---------------------------------------------------------------------------
# Plugins: no retries, no TTL tick
# ---------------------------------------------------------------------------


class _FlakyClient:
    """httpx-shaped client whose every POST times out."""

    def __init__(self):
        self.posts = 0

    async def post(self, url, json):
        self.posts += 1
        raise httpx.ReadTimeout("slow reranker")


class TestPluginFlags:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(("retries", "posts"), [(True, 3), (False, 1)])
    async def test_the_reranker_retries_only_when_asked(self, retries, posts):
        from agent.services.memory import Candidate, TransientScorerError

        client = _FlakyClient()
        scorer = RerankerScorer(
            model="m",
            base_url="http://rerank.test",
            api_key=None,
            retries=2,
            retry_backoff=0.0,
            client=client,
        )
        items = [
            Scored(candidate=Candidate(kind="memory", text=f"m{i}")) for i in range(3)
        ]

        with pytest.raises(TransientScorerError):
            await scorer.score(AssembleRequest(query_text="q", retries=retries), items)
        assert client.posts == posts

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("tick", "decrements"), [(True, 1), (False, 0)])
    async def test_a_read_only_retrieval_leaves_the_ttl_tier_alone(
        self, tick, decrements
    ):
        store = AsyncMock()
        store.retrieve.return_value = make_memories()
        retriever = RecallTwoTierRetriever(store)

        found = await retriever.retrieve(AssembleRequest(query_text="q", ttl_tick=tick))

        assert len(found) == 2
        assert store.decrement_ttl.await_count == decrements


# ---------------------------------------------------------------------------
# The stored form (threads.metadata)
# ---------------------------------------------------------------------------


class TestStoredForm:
    def test_round_trip_renders_and_keys_like_the_rows(self):
        memories, notes = make_memories(), make_notes()
        stored = serialize_pending_set(
            _result(memories, notes, pending_id="set-1"), turn=3
        )
        # What threads.metadata holds is plain JSON.
        stored = json.loads(json.dumps(stored))
        assert stored["v"] == 1 and stored["id"] == "set-1" and stored["turn"] == 3

        back = deserialize_pending_set(stored)

        assert back.source == "prefetch" and back.pending_id == "set-1"
        got_m, got_n = back.records("memory"), back.records("knowledge")
        for original, copy in zip(memories, got_m):
            assert memory_item(copy).key == memory_item(original).key
            assert memory_item(copy).hash == memory_item(original).hash
            assert memory_item(copy).handle == memory_item(original).handle
        for original, copy in zip(notes, got_n):
            assert knowledge_item(copy).key == knowledge_item(original).key
            assert knowledge_item(copy).hash == knowledge_item(original).hash
        rows = [(m, memory_item(m).handle, False) for m in memories]
        copies = [(m, memory_item(m).handle, False) for m in got_m]
        assert RecallStore.render_memory_entry(
            copies
        ) == RecallStore.render_memory_entry(rows)
        assert KnowledgeStore.assemble_knowledge_block(
            got_n
        ) == KnowledgeStore.assemble_knowledge_block(notes)

    def test_past_the_bound_low_ranked_records_go_whole(self):
        memories = [
            MemoryRecord(id=uuid.UUID(int=i), content=f"{i}:" + "x" * 300)
            for i in range(1, 6)
        ]
        notes = make_notes()

        stored = serialize_pending_set(
            _result(memories, notes, pending_id="set-1"), max_bytes=1200
        )

        assert stored["knowledge"] == []
        kept = stored["memory"]
        assert 0 < len(kept) < len(memories)
        assert [m["id"] for m in kept] == [str(m.id) for m in memories[: len(kept)]]
        assert all(m["content"] == memories[i].content for i, m in enumerate(kept))
        assert len(json.dumps(stored, separators=(",", ":"))) <= 1200

    def test_nothing_to_store(self):
        assert serialize_pending_set(_result(pending_id="set-1")) is None
        assert serialize_pending_set(_result(make_memories())) is None  # no id

    @pytest.mark.parametrize(
        "value",
        [
            None,
            "not json",
            {"v": 2, "id": "x", "memory": [{"content": "a"}]},
            {"v": 1, "memory": [{"content": "a"}]},
            {"v": 1, "id": "x", "memory": [{"id": "1"}]},
        ],
    )
    def test_an_unreadable_set_is_ignored(self, value):
        assert deserialize_pending_set(value) is None

    def test_metadata_helpers(self):
        stored = {"v": 1, "id": "set-1", "memory": []}
        assert (
            pending_memory_from_metadata({SESSION_PENDING_MEMORY_KEY: stored}) == stored
        )
        assert (
            pending_memory_from_metadata(
                json.dumps({SESSION_PENDING_MEMORY_KEY: stored})
            )
            == stored
        )
        assert pending_memory_from_metadata({"other": 1}) is None
        assert valid_pending_memory(json.dumps(stored)) == stored


# ---------------------------------------------------------------------------
# The exchange query
# ---------------------------------------------------------------------------


class TestExchangeQuery:
    def test_the_last_user_message_and_the_final_answer(self):
        messages = [
            HumanMessage(content="Earlier question"),
            AIMessage(content="Earlier answer"),
            HumanMessage(content="Where do releases live?"),
            make_context_entry("memory", "[m:abc123]\nold memory", section="memory"),
            AIMessage(
                content="",
                tool_calls=[{"name": "read_file", "args": {}, "id": "c1"}],
            ),
            ToolMessage(content="file body", tool_call_id="c1"),
            AIMessage(content=[{"type": "text", "text": "In docs/release.md."}]),
        ]

        assert build_exchange_query_text(messages) == (
            "Where do releases live?\n\nIn docs/release.md."
        )

    def test_each_part_is_capped_and_empty_history_is_empty(self):
        messages = [HumanMessage(content="q" * 50), AIMessage(content="a" * 50)]

        assert build_exchange_query_text(messages, max_chars_per_message=10) == (
            "q" * 10 + "\n\n" + "a" * 10
        )
        assert build_exchange_query_text([]) == ""
