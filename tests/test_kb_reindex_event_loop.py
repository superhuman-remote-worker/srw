"""KB indexing must never stall the orchestrator's event loop on its tokenizer.

Issue: kb_reindex_sync_dns_retries_stall_orchestrator_liveness. On a cold
tiktoken cache the chunker's ``get_encoding`` downloads its BPE vocab with
*synchronous* HTTP. Where that host does not resolve, every note paid ~4 s of
DNS retries on the event loop, one note after another: ``/api/health`` timed
out (kubelet killed the orchestrator) and unprocessed heartbeats got in-flight
jobs orphan-paused. A KB maintenance pass preempted running jobs.

These tests drive the real reindex and inline-index paths with a tokenizer
whose load blocks like that retry and then fails, while a heartbeat coroutine
measures how long the loop goes without running it. They pin three things:
the load runs off the loop, one failed load is not retried for every note, and
the pass degrades to the conservative estimate instead of failing each note.
"""

import asyncio
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from shared.runtime.core import chunk_planner
from orchestrator.services.kb_reindex import index_single_note, reindex_kb

# How long one simulated vocab load holds its thread. The loop's heartbeat must
# never go this long without running; the bound below leaves room for a busy CI
# box while staying far under a single blocked load.
BLOCK_SECONDS = 0.4
MAX_LOOP_GAP_SECONDS = BLOCK_SECONDS / 2


class _UnreachableTokenizer:
    """Stand-in for ``tiktoken`` on a pod whose vocab host never resolves."""

    def __init__(self) -> None:
        self.loads = 0
        self.block_seconds = BLOCK_SECONDS

    def get_encoding(self, _name):
        self.loads += 1
        # requests/urllib3 retrying NameResolutionError: synchronous, then fails.
        time.sleep(self.block_seconds)
        raise ConnectionError(
            "Max retries exceeded: NameResolutionError("
            "'openaipublic.blob.core.windows.net')"
        )

    def encoding_for_model(self, _model):
        return self.get_encoding("cl100k_base")


@pytest.fixture
def unreachable_tokenizer(monkeypatch):
    # Independent of whether tiktoken is installed or its cache is warm: a
    # cold, private encoding cache and a loader that blocks, then raises.
    fake = _UnreachableTokenizer()
    monkeypatch.setattr(chunk_planner, "TIKTOKEN_AVAILABLE", True)
    monkeypatch.setattr(chunk_planner, "tiktoken", fake, raising=False)
    monkeypatch.setattr(chunk_planner, "_ENCODING_CACHE", {})
    return fake


async def _run_with_heartbeat(work):
    """Await ``work`` while measuring the loop's longest stretch without a tick."""
    gaps = []
    stop = asyncio.Event()

    async def heartbeat():
        last = time.monotonic()
        while not stop.is_set():
            await asyncio.sleep(0.01)
            now = time.monotonic()
            gaps.append(now - last)
            last = now

    beat = asyncio.create_task(heartbeat())
    await asyncio.sleep(0)
    try:
        result = await work
    finally:
        stop.set()
        await beat
    return result, max(gaps, default=0.0)


def _note(slug: str) -> str:
    return (
        f"---\nid: {slug}\ntype: learning\nstatus: active\n---\n"
        f"# {slug}\n\nA body that the chunker has to count tokens for.\n"
    )


def _deps(notes):
    gitea = AsyncMock()
    gitea.get_branch_head_sha.return_value = "headsha"
    gitea.list_tree.return_value = [
        {"path": f"knowledge/{slug}.md", "type": "blob", "sha": f"sha-{slug}"}
        for slug in notes
    ]
    contents = {f"knowledge/{slug}.md": _note(slug) for slug in notes}
    gitea.get_file_content.side_effect = lambda repo, path, ref=None: contents.get(path)

    store = AsyncMock()
    store.get_watermark.return_value = None
    store.get_indexed_blob_shas.return_value = {}
    store.clear_note_stamps.return_value = 0
    store.adopt_legacy_row.return_value = None
    store.upsert_kb_note.return_value = uuid.uuid4()

    svc = MagicMock()
    svc.model = "qwen3-embedding-8b"
    svc.expected_dimensions = 4096

    async def _batch(texts):
        return [[0.1] for _ in texts]

    svc.embed_batch = AsyncMock(side_effect=_batch)
    return gitea, store, svc


class TestReindexKeepsTheLoopResponsive:
    @pytest.mark.asyncio
    async def test_unreachable_tokenizer_neither_blocks_the_loop_nor_fails_notes(
        self, unreachable_tokenizer
    ):
        notes = ["alpha", "beta", "gamma", "delta"]
        gitea, store, svc = _deps(notes)

        result, gap = await _run_with_heartbeat(
            reindex_kb(
                gitea_client=gitea,
                store=store,
                embedding_service=svc,
                kb_id=uuid.uuid4(),
                repo_name="r",
            )
        )

        # Liveness: the blocking load ran on a worker thread, not the loop
        # that serves /api/health and agent heartbeats.
        assert gap < MAX_LOOP_GAP_SECONDS, (
            f"event loop stalled {gap:.2f}s during the reindex"
        )
        # Fail fast: one unreachable vocab host predicts the next N notes.
        assert unreachable_tokenizer.loads == 1
        # Degrade, don't fail: every note is chunked on the estimate and indexed.
        assert result["status"] == "completed"
        assert result["upserted"] == len(notes)
        assert result["errors"] == 0
        assert svc.embed_batch.await_count == len(notes)

    @pytest.mark.asyncio
    async def test_inline_materialize_precount_does_not_block_the_loop(
        self, unreachable_tokenizer
    ):
        # The materialisation endpoint indexes inline, inside a request, and
        # counts chunks against its cap before embedding. That pre-count is the
        # first tokenizer call a freshly started orchestrator is likely to make.
        _, store, svc = _deps([])

        outcome, gap = await _run_with_heartbeat(
            index_single_note(
                store=store,
                embedding_service=svc,
                kb_id=uuid.uuid4(),
                path="knowledge/inline.md",
                text=_note("inline"),
                blob_sha="sha-inline",
                embedding_stamp="stamp",
                max_chunks=8,
            )
        )

        assert gap < MAX_LOOP_GAP_SECONDS, (
            f"event loop stalled {gap:.2f}s during an inline index"
        )
        assert unreachable_tokenizer.loads == 1
        assert outcome.status == "indexed"
        store.stamp_note_indexed.assert_awaited_once()


class TestCountTextTokensDegradation:
    def test_failed_load_is_remembered_then_retried_after_the_cooldown(
        self, unreachable_tokenizer, monkeypatch
    ):
        unreachable_tokenizer.block_seconds = 0
        monkeypatch.setattr(chunk_planner, "_ENCODING_RETRY_SECONDS", 60.0)
        clock = [1000.0]
        # Only the planner's view of the clock moves; the real module is untouched.
        monkeypatch.setattr(
            chunk_planner, "time", SimpleNamespace(monotonic=lambda: clock[0])
        )
        text = "x" * 35

        estimate = chunk_planner.count_text_tokens(text)
        assert estimate == 10  # ceil(35 / 3.5): the conservative estimate
        assert chunk_planner.count_text_tokens(text) == estimate
        assert unreachable_tokenizer.loads == 1

        # Inside the window the failure is served from the cache...
        clock[0] += 59.0
        chunk_planner.count_text_tokens(text)
        assert unreachable_tokenizer.loads == 1
        # ...and a blip is not permanent: past it, the load is tried again.
        clock[0] += 2.0
        chunk_planner.count_text_tokens(text)
        assert unreachable_tokenizer.loads == 2

    def test_concurrent_callers_share_one_failed_load(self, unreachable_tokenizer):
        # Distinct KBs index concurrently, each chunking on its own worker
        # thread. Callers that queued behind the first load see its failure
        # instead of each paying the same DNS retry in turn.
        unreachable_tokenizer.block_seconds = 0.2
        with ThreadPoolExecutor(max_workers=4) as pool:
            counts = list(
                pool.map(chunk_planner.count_text_tokens, ["x" * 35] * 4, timeout=10)
            )

        assert counts == [10, 10, 10, 10]
        assert unreachable_tokenizer.loads == 1

    def test_a_working_tokenizer_is_still_used_and_cached(self, monkeypatch):
        class _Encoding:
            def encode(self, text, disallowed_special=()):
                return list(text.split())

        class _Tokenizer:
            loads = 0

            def get_encoding(self, _name):
                _Tokenizer.loads += 1
                return _Encoding()

        monkeypatch.setattr(chunk_planner, "TIKTOKEN_AVAILABLE", True)
        monkeypatch.setattr(chunk_planner, "tiktoken", _Tokenizer(), raising=False)
        monkeypatch.setattr(chunk_planner, "_ENCODING_CACHE", {})

        assert chunk_planner.count_text_tokens("one two three") == 3
        assert chunk_planner.count_text_tokens("four five") == 2
        assert _Tokenizer.loads == 1
