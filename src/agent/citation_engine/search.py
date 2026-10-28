"""
Unified Search for Citation Engine
====================================
Keyword search (Postgres FTS), semantic search (pgvector), hybrid retrieval
(RRF fusion), and explainable evidence labeling.

SRW-native: all queries run async on an acquired asyncpg connection from SRW's
vector pool, using ``$N`` placeholders. Search is scoped to a job via the
``job_sources`` join table.
"""

import logging
import uuid
from dataclasses import dataclass

from shared.runtime.citation_engine.models import SearchResult

log = logging.getLogger(__name__)

# RRF constant — standard value from the original RRF paper
RRF_K = 60

# Evidence label thresholds (on normalized RRF scores)
HIGH_THRESHOLD = 0.6
MEDIUM_THRESHOLD = 0.3


@dataclass
class _RankedHit:
    """Internal intermediate result before evidence labeling."""

    source_id: int
    source_name: str
    source_type: str
    chunk_text: str
    page_reference: str | None
    rrf_score: float
    in_keyword: bool
    in_semantic: bool


async def keyword_search(
    conn,
    query: str,
    job_id: uuid.UUID | None,
    top_k: int = 20,
    source_type: str | None = None,
    tags: list[str] | None = None,
    scope: str = "content",
) -> list[_RankedHit]:
    """Full-text search using PostgreSQL tsvector (job-scoped)."""
    results: list[_RankedHit] = []

    if scope in ("content", "all"):
        results.extend(
            await _pg_fts_content(conn, query, job_id, top_k, source_type, tags)
        )
    if scope in ("annotations", "all"):
        results.extend(
            await _pg_fts_annotations(conn, query, job_id, top_k, source_type, tags)
        )

    # Deduplicate by (source_id, chunk_text prefix) keeping highest score.
    seen: dict[tuple[int, str], _RankedHit] = {}
    for hit in results:
        key = (hit.source_id, hit.chunk_text[:200])
        if key not in seen or hit.rrf_score > seen[key].rrf_score:
            seen[key] = hit

    deduped = sorted(seen.values(), key=lambda h: h.rrf_score, reverse=True)
    return deduped[:top_k]


async def _pg_fts_content(
    conn,
    query: str,
    job_id: uuid.UUID | None,
    top_k: int,
    source_type: str | None,
    tags: list[str] | None,
) -> list[_RankedHit]:
    """PostgreSQL full-text search on source content.

    ``$1`` = query (reused), ``$2`` = job_id (reused for tag subqueries).
    """
    sql = """
        SELECT s.id AS source_id, COALESCE(js.name, s.name) AS source_name, s.type::text AS source_type,
               substring(s.content, 1, 1000) AS chunk_text,
               ts_rank(to_tsvector('simple', s.content), plainto_tsquery('simple', $1)) AS rank
        FROM sources s
        JOIN job_sources js ON s.id = js.source_id AND js.job_id = $2
        WHERE to_tsvector('simple', s.content) @@ plainto_tsquery('simple', $1)
    """
    params: list = [query, job_id]
    nxt = 3

    if source_type:
        sql += f" AND s.type = ${nxt}::source_type"
        params.append(source_type)
        nxt += 1

    if tags:
        for i, tag in enumerate(tags):
            alias = f"st{i}"
            sql += (
                f" AND EXISTS (SELECT 1 FROM source_tags {alias} "
                f"WHERE {alias}.source_id = s.id AND {alias}.job_id = $2 AND {alias}.tag = ${nxt})"
            )
            params.append(tag)
            nxt += 1

    sql += f" ORDER BY rank DESC LIMIT ${nxt}"
    params.append(top_k)

    rows = await conn.fetch(sql, *params)
    return [
        _RankedHit(
            source_id=row["source_id"],
            source_name=row["source_name"],
            source_type=row["source_type"],
            chunk_text=row["chunk_text"],
            page_reference=None,
            rrf_score=float(row["rank"]),
            in_keyword=True,
            in_semantic=False,
        )
        for row in rows
    ]


async def _pg_fts_annotations(
    conn,
    query: str,
    job_id: uuid.UUID | None,
    top_k: int,
    source_type: str | None,
    tags: list[str] | None,
) -> list[_RankedHit]:
    """PostgreSQL full-text search on annotation content (job-scoped)."""
    sql = """
        SELECT sa.source_id, COALESCE(js.name, s.name) AS source_name, s.type::text AS source_type,
               sa.content AS chunk_text, sa.page_reference,
               ts_rank(to_tsvector('simple', sa.content), plainto_tsquery('simple', $1)) AS rank
        FROM source_annotations sa
        JOIN sources s ON s.id = sa.source_id
        JOIN job_sources js ON s.id = js.source_id AND js.job_id = $2
        WHERE sa.job_id = $2
          AND to_tsvector('simple', sa.content) @@ plainto_tsquery('simple', $1)
    """
    params: list = [query, job_id]
    nxt = 3

    if source_type:
        sql += f" AND s.type = ${nxt}::source_type"
        params.append(source_type)
        nxt += 1

    sql += f" ORDER BY rank DESC LIMIT ${nxt}"
    params.append(top_k)

    rows = await conn.fetch(sql, *params)
    return [
        _RankedHit(
            source_id=row["source_id"],
            source_name=row["source_name"],
            source_type=row["source_type"],
            chunk_text=row["chunk_text"],
            page_reference=row["page_reference"],
            rrf_score=float(row["rank"]) if row["rank"] is not None else 1.0,
            in_keyword=True,
            in_semantic=False,
        )
        for row in rows
    ]


async def semantic_search(
    conn,
    query_embedding: list[float],
    job_id: uuid.UUID | None,
    top_k: int = 20,
    source_type: str | None = None,
    tags: list[str] | None = None,
) -> list[_RankedHit]:
    """Vector similarity search using pgvector cosine distance (job-scoped).

    ``$1`` = query embedding (List[float], encoded by the pgvector codec and
    reused in ORDER BY), ``$2`` = job_id (reused for tag subqueries).
    """
    sql = """
        SELECT se.source_id, COALESCE(js.name, s.name) AS source_name, s.type::text AS source_type,
               se.chunk_text,
               1 - (se.embedding <=> $1) AS similarity
        FROM source_embeddings se
        JOIN sources s ON s.id = se.source_id
        JOIN job_sources js ON s.id = js.source_id AND js.job_id = $2
        WHERE se.job_id = $2 AND se.embedding IS NOT NULL
    """
    params: list = [query_embedding, job_id]
    nxt = 3

    if source_type:
        sql += f" AND s.type = ${nxt}::source_type"
        params.append(source_type)
        nxt += 1

    if tags:
        for i, tag in enumerate(tags):
            alias = f"st{i}"
            sql += (
                f" AND EXISTS (SELECT 1 FROM source_tags {alias} "
                f"WHERE {alias}.source_id = s.id AND {alias}.job_id = $2 AND {alias}.tag = ${nxt})"
            )
            params.append(tag)
            nxt += 1

    sql += f" ORDER BY se.embedding <=> $1 LIMIT ${nxt}"
    params.append(top_k)

    rows = await conn.fetch(sql, *params)
    return [
        _RankedHit(
            source_id=row["source_id"],
            source_name=row["source_name"],
            source_type=row["source_type"],
            chunk_text=row["chunk_text"],
            page_reference=None,
            rrf_score=float(row["similarity"]),
            in_keyword=False,
            in_semantic=True,
        )
        for row in rows
    ]


def rrf_merge(
    keyword_hits: list[_RankedHit],
    semantic_hits: list[_RankedHit],
    k: int = RRF_K,
    top_k: int = 10,
) -> list[_RankedHit]:
    """Merge keyword and semantic results using Reciprocal Rank Fusion."""
    merged: dict[tuple[int, str], _RankedHit] = {}

    def _key(hit: _RankedHit) -> tuple[int, str]:
        return (hit.source_id, hit.chunk_text[:200])

    for rank, hit in enumerate(keyword_hits):
        key = _key(hit)
        rrf_score = 1.0 / (k + rank + 1)  # rank is 0-based, RRF uses 1-based
        if key in merged:
            merged[key].rrf_score += rrf_score
            merged[key].in_keyword = True
        else:
            merged[key] = _RankedHit(
                source_id=hit.source_id,
                source_name=hit.source_name,
                source_type=hit.source_type,
                chunk_text=hit.chunk_text,
                page_reference=hit.page_reference,
                rrf_score=rrf_score,
                in_keyword=True,
                in_semantic=False,
            )

    for rank, hit in enumerate(semantic_hits):
        key = _key(hit)
        rrf_score = 1.0 / (k + rank + 1)
        if key in merged:
            merged[key].rrf_score += rrf_score
            merged[key].in_semantic = True
        else:
            merged[key] = _RankedHit(
                source_id=hit.source_id,
                source_name=hit.source_name,
                source_type=hit.source_type,
                chunk_text=hit.chunk_text,
                page_reference=hit.page_reference,
                rrf_score=rrf_score,
                in_keyword=False,
                in_semantic=True,
            )

    sorted_hits = sorted(merged.values(), key=lambda h: h.rrf_score, reverse=True)
    return sorted_hits[:top_k]


def label_evidence(hits: list[_RankedHit]) -> list[SearchResult]:
    """Assign explainable evidence labels (HIGH/MEDIUM/LOW) to search results."""
    if not hits:
        return []

    max_score = max(h.rrf_score for h in hits)
    if max_score == 0:
        max_score = 1.0

    results = []
    for hit in hits:
        normalized = hit.rrf_score / max_score

        if hit.in_keyword and hit.in_semantic:
            label = "HIGH"
            reason = "matched by keyword and semantic search"
        elif normalized >= HIGH_THRESHOLD:
            label = "HIGH"
            reason = (
                "strong semantic similarity"
                if hit.in_semantic
                else "strong keyword match"
            )
        elif normalized >= MEDIUM_THRESHOLD:
            label = "MEDIUM"
            if hit.in_semantic:
                reason = "moderate semantic similarity"
            elif hit.in_keyword:
                reason = "partial keyword match"
            else:
                reason = "moderate relevance"
        else:
            label = "LOW"
            reason = "weak similarity"

        results.append(
            SearchResult(
                source_id=hit.source_id,
                source_name=hit.source_name,
                source_type=hit.source_type,
                chunk_text=hit.chunk_text,
                page_reference=hit.page_reference,
                evidence_label=label,
                evidence_reason=reason,
                score=hit.rrf_score,
            )
        )

    return results


def overall_label(results: list[SearchResult]) -> str:
    """Generate an aggregate evidence summary."""
    if not results:
        return "No results found"

    high_count = sum(1 for r in results if r.evidence_label == "HIGH")
    medium_count = sum(1 for r in results if r.evidence_label == "MEDIUM")
    total = len(results)

    best = results[0]  # Already sorted by score
    best_desc = f'strongest match from "{best.source_name}"'

    if high_count >= 2:
        return f"HIGH — {high_count} sources support this, {best_desc}"
    elif high_count == 1:
        return f"HIGH — 1 strong source, {best_desc}"
    elif medium_count >= 2:
        return f"MEDIUM — {medium_count} partial matches found"
    elif medium_count == 1:
        return f"MEDIUM — 1 source found, {results[0].evidence_reason}"
    else:
        return f"LOW — {total} weak match{'es' if total != 1 else ''} found"
