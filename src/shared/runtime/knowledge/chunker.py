"""OKF KB chunker + embed pipeline — pure structural chunking (slice 3, PR2).

No DB: a heading-aware structural chunker, a breadcrumb embed-text builder, an
``embedding_version`` stamp, and the async embed pipeline that wires the bulk
``embed_batch`` into the chunk shape ``KnowledgeStore.replace_note_chunks``
(slice 3 PR1) consumes. The reindexer (PR3) parses a changed note with
``gardener.parse_note_md`` and feeds the body here.

Design: knowledge-base/knowledge/features/okf_knowledge_base.md §5.1.

Why structural, not semantic: heading-aware splitting benchmarks *better* than
semantic chunking on this corpus, and OKF notes are already sectioned by the
gardener's renderer. Short notes stay single-chunk naturally — the common case,
and the correct outcome. Overlap is applied *only* when a single section is too
big to fit and must be cut mid-section; clean heading boundaries need none.
"""

from __future__ import annotations

import asyncio
import math
import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from shared.runtime.core.chunk_planner import count_text_tokens

# Bump when the chunking algorithm changes — it is part of ``embedding_version``,
# so a bump invalidates every indexed row for a KB and forces a rebuild (the
# index is disposable; §5.1). Independent of the embedding model id/dims.
CHUNKER_VERSION = "c1"

# Target chunk size. The 2025-26 retrieval literature clusters at ~400-512
# tokens for heading-aware note chunks; 512 is the upper edge (fewer, richer
# chunks — agents can grep-and-read after search).
TARGET_TOKENS = 512

# Overlap fraction applied ONLY between forced mid-section pieces (10-15%).
OVERLAP_RATIO = 0.12

# Hard cap on inputs per embed_batch request. Embedding backends enforce their
# own limits (live: the self-hosted TEI endpoint 422s above 64 inputs — a 127 KB
# curator dump chunks into 95). 64 is the observed floor across our backends;
# raise via env for providers with roomier limits.
DEFAULT_MAX_EMBED_BATCH = int(os.getenv("EMBEDDING_MAX_BATCH", "64"))

# No trailing "$": it is redundant for the single lines _split_sections feeds in
# ("." already stops at a newline), and it was the only thing that could fail
# after "\s+" -- the ambiguity between that run and "(.*)" is what the scanner
# flagged.
_HEADING_LINE_RE = re.compile(r"^(#{1,6})\s+(.*)")
_PARAGRAPH_SPLIT_RE = re.compile(r"\n\s*\n")


@dataclass
class NoteChunk:
    """One heading-aware chunk of a note.

    ``heading_path`` is the breadcrumb of the section the chunk begins in
    (``"Design > Storage > Chunking"``), or ``None`` for preamble before the
    first heading. ``chunk_ix`` is the 0-based position within the note.
    """

    chunk_ix: int
    heading_path: Optional[str]
    content: str
    tokens: int


def _split_sections(body: str) -> List[Tuple[Optional[str], str]]:
    """Split a markdown body into ``(heading_path, section_text)`` pairs.

    A section runs from one ATX heading (inclusive) to the next; text before the
    first heading is a preamble section with ``heading_path=None``. The breadcrumb
    is built from a heading stack: a heading at level L pops every stacked heading
    at level >= L (siblings replace, deeper levels reset) before pushing itself.
    """
    sections: List[Tuple[Optional[str], str]] = []
    stack: List[Tuple[int, str]] = []
    cur_path: Optional[str] = None
    cur_lines: List[str] = []

    def flush() -> None:
        text = "\n".join(cur_lines).strip("\n")
        if text.strip():
            sections.append((cur_path, text))

    for line in body.split("\n"):
        m = _HEADING_LINE_RE.match(line)
        if m:
            flush()
            level = len(m.group(1))
            htext = m.group(2).strip()
            while stack and stack[-1][0] >= level:
                stack.pop()
            stack.append((level, htext))
            cur_path = " > ".join(t for _, t in stack)
            cur_lines = [line]
        else:
            cur_lines.append(line)
    flush()
    return sections


def _char_split(
    text: str, target_tokens: int, count: Callable[[str], int]
) -> List[str]:
    """Last-resort split of a single oversized paragraph into ~target pieces."""
    toks = count(text)
    n = math.ceil(toks / target_tokens) if target_tokens > 0 else 1
    if n <= 1:
        return [text]
    size = math.ceil(len(text) / n)
    pieces = [text[i * size : (i + 1) * size] for i in range(n)]
    return [p for p in pieces if p]


def _split_oversized(
    path: Optional[str],
    text: str,
    target_tokens: int,
    count: Callable[[str], int],
) -> List[Tuple[Optional[str], str]]:
    """Cut one section that exceeds ``target_tokens`` into overlapping pieces.

    Packs whole paragraphs (a paragraph that alone exceeds target is char-split
    first) into <= target pieces; each piece after the first is seeded with the
    previous piece's trailing paragraphs up to the overlap allowance, so a fact
    straddling a cut isn't lost.
    """
    units: List[str] = []
    for para in _PARAGRAPH_SPLIT_RE.split(text):
        if not para.strip():
            continue
        if count(para) <= target_tokens:
            units.append(para)
        else:
            units.extend(_char_split(para, target_tokens, count))

    overlap_tokens = max(1, int(target_tokens * OVERLAP_RATIO))
    pieces: List[str] = []
    cur: List[str] = []
    cur_tokens = 0
    for unit in units:
        ut = count(unit)
        if cur and cur_tokens + ut > target_tokens:
            pieces.append("\n\n".join(cur))
            seed: List[str] = []
            acc = 0
            for prev in reversed(cur):
                pt = count(prev)
                if acc + pt > overlap_tokens:
                    break
                seed.insert(0, prev)
                acc += pt
            cur = list(seed)
            cur_tokens = sum(count(x) for x in cur)
        cur.append(unit)
        cur_tokens += ut
    if cur:
        pieces.append("\n\n".join(cur))
    return [(path, pc) for pc in pieces]


def chunk_note(
    body: str,
    *,
    target_tokens: int = TARGET_TOKENS,
    token_counter: Optional[Callable[[str], int]] = None,
) -> List[NoteChunk]:
    """Split a note body into heading-aware chunks.

    Greedily merges consecutive sections up to ``target_tokens`` (so short notes
    become a single chunk), and hard-splits any single section that alone exceeds
    the target — the only place overlap is introduced. The breadcrumb of a merged
    chunk is the path of the first section it contains.
    """
    count = token_counter if token_counter is not None else count_text_tokens
    sections = _split_sections(body)

    raw: List[Tuple[Optional[str], str]] = []
    acc_path: Optional[str] = None
    acc: List[str] = []
    acc_tokens = 0
    for path, text in sections:
        stoks = count(text)
        if stoks > target_tokens:
            if acc:
                raw.append((acc_path, "\n\n".join(acc)))
                acc, acc_tokens, acc_path = [], 0, None
            raw.extend(_split_oversized(path, text, target_tokens, count))
            continue
        if acc and acc_tokens + stoks > target_tokens:
            raw.append((acc_path, "\n\n".join(acc)))
            acc, acc_tokens, acc_path = [], 0, None
        if not acc:
            acc_path = path
        acc.append(text)
        acc_tokens += stoks
    if acc:
        raw.append((acc_path, "\n\n".join(acc)))

    return [
        NoteChunk(chunk_ix=i, heading_path=p, content=c, tokens=count(c))
        for i, (p, c) in enumerate(raw)
    ]


def build_embed_text(
    content: str,
    heading_path: Optional[str],
    title: str,
    note_type: str,
    tags: List[str],
) -> str:
    """Prepend a natural-language breadcrumb to a chunk before embedding.

    The highest-ROI cheap trick in the 2025-26 retrieval literature (15-25pt
    QA-accuracy gains; Khoj ships exactly this): the embed text carries the note's
    title, type, tags, and the section heading path as prose — NEVER raw YAML —
    so a chunk retrieves on its context, not just its bare words. The chunk body
    follows verbatim after a blank line.
    """
    header: List[str] = []
    if title and note_type:
        header.append(f"{title} (a {note_type} note).")
    elif title:
        header.append(f"{title}.")
    elif note_type:
        header.append(f"A {note_type} note.")
    if tags:
        header.append(f"Tags: {', '.join(tags)}.")
    if heading_path:
        header.append(f"Section: {heading_path}.")
    if header:
        return f"{' '.join(header)}\n\n{content}"
    return content


def embedding_version(
    model: str,
    dimensions: int,
    chunker_version: str = CHUNKER_VERSION,
    profile_fingerprint: Optional[str] = None,
) -> str:
    """The pipeline stamp written on every embedded row.

    Retrieval filters ``WHERE embedding_version = current`` so mixed-model or
    mixed-chunker vectors can't silently drift into the same result set. The
    optional profile fingerprint distinguishes the same model label served by
    another provider/catalog endpoint. Any component changing means a per-KB
    rebuild.
    """
    stamp = f"{model}:{dimensions}:{chunker_version}"
    if profile_fingerprint:
        stamp = f"{stamp}:{profile_fingerprint}"
    return stamp


def embedding_version_for_service(
    embedding_service: Any, chunker_version: str = CHUNKER_VERSION
) -> str:
    """Build the row/query stamp from one effective embedding service.

    Keeping this resolution in one helper prevents central indexing and agent
    query filtering from accidentally using different profile identities.
    Test doubles and older service implementations without a fingerprint retain
    the legacy three-component stamp.
    """
    fingerprint = getattr(embedding_service, "profile_fingerprint", None)
    if not isinstance(fingerprint, str):
        fingerprint = None
    return embedding_version(
        embedding_service.model,
        embedding_service.expected_dimensions,
        chunker_version,
        fingerprint,
    )


def note_centroid(embeddings: List[List[float]]) -> Optional[List[float]]:
    """Mean of a note's chunk embeddings — its whole-note vector (PR4d).

    After the chunk cutover the note row's ``embedding`` is NULL (vectors live on
    ``knowledge_chunks``), so ``find_near_duplicate_pairs`` went blind. The
    reindexer averages the chunks it just embedded and stores the centroid back on
    the note row, restoring the note-vs-note near-duplicate self-join with a
    whole-note ("should a gardener merge these?") semantic. Cosine (pgvector
    ``<=>``) normalises magnitude, so a plain mean is a sound centroid. Returns
    ``None`` for a chunkless (empty-body) note — it simply doesn't participate.
    """
    if not embeddings:
        return None
    n = len(embeddings)
    return [sum(col) / n for col in zip(*embeddings)]


async def embed_note_chunks(
    body: str,
    title: str,
    note_type: str,
    tags: List[str],
    embedding_service: Any,
    *,
    chunker_version: str = CHUNKER_VERSION,
    target_tokens: int = TARGET_TOKENS,
    token_counter: Optional[Callable[[str], int]] = None,
    max_batch: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], str]:
    """Chunk a note, bulk-embed the chunks, return persistable rows.

    The end-to-end pipeline PR3 calls per changed note: it produces the exact dict
    shape ``KnowledgeStore.replace_note_chunks`` consumes (``chunk_ix``,
    ``heading_path``, ``content``, ``embedding``) plus the ``embedding_version``
    stamp for the note + chunk rows. Uses the bulk ``embed_batch`` (one request per
    ``max_batch`` chunks — most notes fit in one) rather than the legacy
    one-call-per-note upsert path. An empty body embeds nothing (no wasted API
    call) but still returns the version so the caller can stamp the — now
    chunk-less — note row consistently.
    """
    version = embedding_version_for_service(embedding_service, chunker_version)
    # Off the event loop: tokenizing is CPU work that scales with the note, and
    # a cold tiktoken cache makes the first count a synchronous vocab download.
    # The loop also answers health probes and agent heartbeats.
    chunks = await asyncio.to_thread(
        chunk_note, body, target_tokens=target_tokens, token_counter=token_counter
    )
    if not chunks:
        return [], version

    embed_texts = [
        build_embed_text(c.content, c.heading_path, title, note_type, tags)
        for c in chunks
    ]
    cap = max_batch or DEFAULT_MAX_EMBED_BATCH
    vectors: List[Any] = []
    for start in range(0, len(embed_texts), cap):
        vectors.extend(
            await embedding_service.embed_batch(embed_texts[start : start + cap])
        )
    rows = [
        {
            "chunk_ix": c.chunk_ix,
            "heading_path": c.heading_path,
            "content": c.content,
            "embedding": vec,
        }
        for c, vec in zip(chunks, vectors)
    ]
    return rows, version
