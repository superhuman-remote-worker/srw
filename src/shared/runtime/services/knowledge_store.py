"""KnowledgeStore — pgvector search index for project knowledge base.

Write-through companion to KnowledgeGraphDB. Every kb_write/kb_update
call writes to Neo4j (source of truth) first, then upserts into this
search index. Never edited directly — rebuilt from Neo4j if needed.

See knowledge-base/knowledge/features/project_knowledge_base.md for full architecture.

Usage:
    ```python
    from shared.runtime.services.knowledge_store import KnowledgeStore
    from shared.runtime.services.embedding_service import get_embedding_service

    store = KnowledgeStore(
        db=postgres_db,
        embedding_service=get_embedding_service(),
    )

    # Write-through after Neo4j write
    await store.upsert_note(
        note_id="chose-jwt-over-oauth",
        project_id=project_uuid,
        title="Chose JWT over OAuth",
        note_type="decision",
        content="After evaluating both...",
        tags=["authentication"],
        keywords=["JWT", "OAuth"],
        retrieval_messages=["What auth approach?"],
    )

    # Hybrid search
    results = await store.hybrid_search(
        project_id=project_uuid,
        query="authentication approach",
    )
    ```
"""

import hashlib
import logging
import re
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from shared.backlog_tags import strip_machine_tags
from shared.runtime.core.context_entries import (
    UPDATED_ITEM_MARKER,
    knowledge_item_key,
)

logger = logging.getLogger(__name__)


# Every stored note identifier is VARCHAR(100): knowledge_index.note_id and
# .superseded_by, knowledge_links.source_id and .target_id. Ingest clamps ids to
# this bound (kb_reindex._NOTE_ID_MAX), so anything longer could never resolve to
# a note and must not be handed to Postgres unclamped.
NOTE_ID_MAX = 100

# Consecutive `partial` sweeps carrying the same error fingerprint before the
# wedge detector alarms (spec WP3, H3). Bound as a parameter into the
# set_watermark_status UPSERT.
WEDGE_STREAK_THRESHOLD = 4


# TTL (in loop cycles) by note_type for KB convergence — see
# knowledge-base/knowledge/features/kb_convergence_ttl_reverification.md. ``None`` = no TTL (durable
# facts never expire on a clock; they're only retired by an explicit newer note).
# Moving-target governance notes get a short TTL and are re-verified by the
# knowledge-assembler aux pass when the counter runs out. Unknown types default to
# ``None`` (conservative: never auto-expire something we don't recognise).
KB_TTL_BY_NOTE_TYPE: Dict[str, Optional[int]] = {
    # moving-target — expire + re-verify
    "state": 2,
    "goal": 3,
    "plan": 3,
    "question": 3,
    # durable — no TTL
    "decision": None,
    "learning": None,
    "code": None,
    "retrospective": None,
    "source": None,
    # Backlog tickets — durable by design. A ticket is work that still needs
    # doing; expiring it on a clock would delete the queue.
    "feature": None,
    "issue": None,
    "idea": None,
    # Officer types (centurion.md §5). The charter is infinite by definition;
    # reports are retired by the officer's gardening (supersede), not a clock —
    # officer projects skip the loop's cycle-based TTL ticking anyway.
    "charter": None,
    "report": None,
}
KB_TTL_DEFAULT: Optional[int] = None


@dataclass
class KnowledgeRecord:
    """A single knowledge note from the search index."""

    id: Optional[uuid.UUID] = None
    note_id: str = ""
    project_id: Optional[uuid.UUID] = None
    kb_id: Optional[uuid.UUID] = None
    title: str = ""
    note_type: str = ""
    status: str = "active"
    priority: int = 1
    confidence: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)
    job_id: Optional[uuid.UUID] = None
    phase: Optional[int] = None
    content: str = ""
    retrieval_messages: List[str] = field(default_factory=list)
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None
    indexed_at: Optional[datetime] = None
    content_hash: Optional[str] = None
    # Which retrieval arms matched this note, in the multi-angle function's
    # declared order (dense, sparse, recency, exact, tag) — spec WP7. Attached
    # by search_chunks from the function's `arms` column; NOT a knowledge_index
    # column, so from_row never reads it and every other producer of a
    # KnowledgeRecord leaves it empty.
    matched_arms: List[str] = field(default_factory=list)

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "KnowledgeRecord":
        """Create a KnowledgeRecord from a database row dict."""
        return cls(
            id=row.get("id"),
            note_id=row.get("note_id", ""),
            project_id=row.get("project_id"),
            kb_id=row.get("kb_id") or row.get("project_id"),
            title=row.get("title", ""),
            note_type=row.get("note_type", ""),
            status=row.get("status", "active"),
            priority=row.get("priority", 1),
            confidence=row.get("confidence"),
            tags=row.get("tags") or [],
            keywords=row.get("keywords") or [],
            job_id=row.get("job_id"),
            phase=row.get("phase"),
            content=row.get("content", ""),
            retrieval_messages=row.get("retrieval_messages") or [],
            created_at=row.get("created_at"),
            modified_at=row.get("modified_at"),
            indexed_at=row.get("indexed_at"),
            content_hash=row.get("content_hash"),
        )


class KnowledgeSearchResults(list[KnowledgeRecord]):
    """List-compatible search results with per-call degradation metadata.

    Keep the notice even for zero hits, without mutable state on a store shared
    by concurrent tool calls and automatic context retrieval.
    """

    def __init__(
        self,
        records: Iterable[KnowledgeRecord] = (),
        *,
        lexical_fallback: bool = False,
    ):
        super().__init__(records)
        self.lexical_fallback = lexical_fallback

    @property
    def notice(self) -> str:
        if not self.lexical_fallback:
            return ""
        return (
            "⚠️ Embeddings unavailable — using lexical fallback (full-text and "
            "literal matching). Semantic matches may be missed. "
            "Use kb_grep(pattern=...) for matching lines."
        )


@dataclass
class KbWatermark:
    """The as-of git commit a KB's index was built from (slice 3, §5.1).

    One row per KB in ``kb_index_watermark``. The incremental reindexer (PR3)
    diffs the repo HEAD against ``indexed_commit`` and re-embeds only the changed
    blobs; ``pipeline_version`` (model id + dims + chunker version) invalidates the
    whole KB when it changes, forcing a full rebuild.
    """

    kb_id: Optional[uuid.UUID] = None
    repo_name: Optional[str] = None
    branch: Optional[str] = None
    indexed_commit: Optional[str] = None
    pipeline_version: Optional[str] = None
    source_head: Optional[str] = None
    status: str = "ready"
    last_attempt_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    last_error: Optional[str] = None
    updated_at: Optional[datetime] = None
    # Per-run progress (advisory; migration 0012) for a determinate Cockpit bar.
    notes_done: Optional[int] = None
    notes_total: Optional[int] = None
    # Wedge detector state (migration 0022, spec WP3/H3). error_fingerprint
    # identifies the run's per-note error set; error_streak counts consecutive
    # `partial` sweeps carrying that same fingerprint; wedged_since is when the
    # streak first crossed WEDGE_STREAK_THRESHOLD. advisory carries a line for
    # conditions that are skipped rather than failed (H2).
    error_fingerprint: Optional[str] = None
    error_streak: int = 0
    wedged_since: Optional[datetime] = None
    advisory: Optional[str] = None

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "KbWatermark":
        """Create a KbWatermark from a database row dict."""
        return cls(
            kb_id=row.get("kb_id"),
            repo_name=row.get("repo_name"),
            branch=row.get("branch"),
            indexed_commit=row.get("indexed_commit"),
            pipeline_version=row.get("pipeline_version"),
            source_head=row.get("source_head"),
            status=row.get("status") or "ready",
            last_attempt_at=row.get("last_attempt_at"),
            last_success_at=row.get("last_success_at"),
            last_error=row.get("last_error"),
            updated_at=row.get("updated_at"),
            notes_done=row.get("notes_done"),
            notes_total=row.get("notes_total"),
            error_fingerprint=row.get("error_fingerprint"),
            error_streak=row.get("error_streak") or 0,
            wedged_since=row.get("wedged_since"),
            advisory=row.get("advisory"),
        )


@dataclass
class KnowledgeChunk:
    """A single heading-aware chunk of a note (slice 3, §5.1).

    In the chunk-granular model the dense vector lives on chunk rows
    (``knowledge_chunks``), not the note row. ``heading_path`` is the breadcrumb
    (``"Design > Storage > Chunking"``) prepended to the embed text; ``chunk_ix``
    is the 0-based position within the note. The raw ``embedding`` is not carried
    on the dataclass (mirrors KnowledgeRecord — vectors stay in the DB).
    """

    id: Optional[uuid.UUID] = None
    note_row: Optional[uuid.UUID] = None
    kb_id: Optional[uuid.UUID] = None
    chunk_ix: int = 0
    heading_path: Optional[str] = None
    content: str = ""
    embedding_version: Optional[str] = None
    created_at: Optional[datetime] = None

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "KnowledgeChunk":
        """Create a KnowledgeChunk from a database row dict."""
        return cls(
            id=row.get("id"),
            note_row=row.get("note_row"),
            kb_id=row.get("kb_id"),
            chunk_ix=row.get("chunk_ix", 0),
            heading_path=row.get("heading_path"),
            content=row.get("content", ""),
            embedding_version=row.get("embedding_version"),
            created_at=row.get("created_at"),
        )


@dataclass
class GrepMatch:
    """One matched line from :meth:`KnowledgeStore.grep_notes` (spec WP7, D9).

    The lexical enumeration channel: a literal/regex hit an agent can act on by
    exact text, which stemmed full-text search cannot do for identifiers like
    ``sales_page_2026_09`` or a commit sha. ``line_no`` is 1-based within the
    note body; ``before``/``after`` are the surrounding context lines (their
    length is bounded by the caller's ``context`` argument, and can be shorter
    at a note's head/tail).
    """

    kb_id: uuid.UUID
    note_id: str
    title: str
    line_no: int
    line: str
    before: List[str]
    after: List[str]


# Grep pattern length cap (D9): both the ILIKE substring and the ~* regex are
# bound as a single query parameter, but an unbounded pattern is still an easy
# way to make Postgres do pathological work — reject before it ever reaches SQL.
_GREP_PATTERN_MAX = 256

# kb_id / status / path visibility gate shared by every grep_notes query
# (candidate fetch, candidate count) — same read-path rule as kb_read/kb_list
# (active + materialised only). $1 is always the kb_ids array in every query
# that includes this fragment.
_GREP_VISIBILITY = "kb_id = ANY($1::uuid[]) AND status = 'active' AND path IS NOT NULL"


def _escape_like(pattern: str) -> str:
    """Escape ``\\``, ``%`` and ``_`` so a literal substring search behaves
    literally under ``ILIKE ... ESCAPE '\\'`` (backslash must be escaped first,
    or escaping ``%``/``_`` would itself introduce new backslashes that get
    re-escaped)."""
    return pattern.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _grep_where_clause(regex: bool) -> Tuple[str, str]:
    """``(body_pred, title_pred)`` SQL fragments for one ``grep_notes`` call
    shape. Both are fixed strings chosen only by ``regex`` — never built from
    ``pattern`` or any other caller-supplied text; the pattern itself only
    ever reaches Postgres as the bound parameter ``$2``.

    The regex path prefixes the embedded ARE option ``(?n)`` (fix round 1,
    finding 2): Postgres's ``~*`` is newline-*insensitive* by default, so
    ``^``/``$`` anchor the whole note body (not each line) and ``.`` matches
    ``\\n`` — while line extraction happens in Python one line at a time.
    Left unaligned, ``^TODO`` silently misses a later line, and a pattern
    like ``1.l`` can pick a note as a SQL candidate by matching *across* a
    newline that no single extracted line then satisfies (``matches=[]``
    with ``total=1``). ``(?n)`` makes ``^``/``$`` match every line boundary
    and stops ``.`` crossing ``\\n``, so the SQL candidate filter agrees with
    the per-line Python extraction it feeds.
    """
    if regex:
        return "content ~* ('(?n)' || $2)", "title ~* ('(?n)' || $2)"
    return (
        "content ILIKE '%' || $2 || '%' ESCAPE '\\'",
        "title ILIKE '%' || $2 || '%' ESCAPE '\\'",
    )


def _grep_candidates_sql(*, regex: bool, include_titles: bool) -> str:
    """The exact candidate-fetch SQL ``grep_notes`` runs for one
    ``(regex, include_titles)`` shape — ``$1`` kb_ids, ``$2`` pattern, ``$3``
    ``max_matches`` (as a ``LIMIT``). Factored out (fix round 1, finding 1)
    so the real-Postgres EXPLAIN test asserts against the literal query
    ``grep_notes`` executes, not a hand-typed approximation of it.

    A single ``(content ILIKE … OR title ILIKE …)`` predicate defeats the
    trigram index: Postgres can't build a ``BitmapOr`` across an indexed
    column (``content``, via ``idx_knowledge_content_trgm``) and an
    unindexed one (``title``, H8), so the combined OR forces a seq scan —
    including on the *default* call (``include_titles=True``). UNIONing two
    independently plannable branches lets the ``content`` branch use the
    trigram index regardless of what the ``title`` branch does.

    ``ORDER BY kb_id, note_id`` (final review, M3) rather than ``note_id``
    alone: note ids are unique per ``(kb_id, note_id)``, not globally, and
    ``kb_grep`` routinely passes several ``kb_ids`` at once — ordering on the
    full identity keeps which rows survive ``LIMIT $3`` deterministic across
    knowledge bases instead of leaving same-slug ties to the plan.

    ``LIMIT $3`` bounds the number of candidate *notes* fetched (fix round
    1, finding 4): a note contributes at least one matched line, so at most
    ``max_matches`` notes can ever render a line, and this avoids pulling
    every matching body (up to ~250KB each) across the wire before the
    Python-side cap applies. :func:`_grep_count_sql` computes the *true*
    candidate count over the same predicate, uncapped, so a caller can still
    tell "there were more notes" even when the fetch itself was capped.
    """
    body_pred, title_pred = _grep_where_clause(regex)
    body_select = (
        "SELECT kb_id, note_id, title, content FROM knowledge_index "
        f"WHERE {_GREP_VISIBILITY} AND {body_pred}"
    )
    if not include_titles:
        return f"{body_select}\nORDER BY kb_id, note_id\nLIMIT $3"
    title_select = (
        "SELECT kb_id, note_id, title, content FROM knowledge_index "
        f"WHERE {_GREP_VISIBILITY} AND {title_pred}"
    )
    return f"{body_select}\nUNION\n{title_select}\nORDER BY kb_id, note_id\nLIMIT $3"


def _grep_count_sql(*, regex: bool, include_titles: bool) -> str:
    """Exact (uncapped) count of candidate notes for the same predicate
    :func:`_grep_candidates_sql` uses — ``$1`` kb_ids, ``$2`` pattern. Run
    alongside the capped fetch (fix round 1, finding 4) so
    ``matching_note_count`` stays accurate even when ``LIMIT`` truncated the
    fetch itself.

    Both branches select ``kb_id, note_id`` — not ``note_id`` alone (fix
    round 2). Note identity is per ``(project_id, note_id)``, not globally
    unique, and ``grep_notes`` is routinely called with several ``kb_ids`` at
    once (``kb_grep`` fans it out across every KB binding). Deduping the
    ``UNION`` on ``note_id`` alone would collapse two distinct notes in two
    different KBs that happen to share a ``note_id`` into one row —
    undercounting ``matching_note_count`` — while :func:`_grep_candidates_sql`
    (which already selects ``kb_id, note_id, …``) keeps them apart. Matching
    the dedup key to the candidate query's key fixes the mismatch: two notes
    still count once only when they're the *same* ``(kb_id, note_id)`` pair
    hit by both the body and title branches.
    """
    body_pred, title_pred = _grep_where_clause(regex)
    body_select = f"SELECT kb_id, note_id FROM knowledge_index WHERE {_GREP_VISIBILITY} AND {body_pred}"
    if not include_titles:
        return f"SELECT count(*) FROM ({body_select}) AS candidates"
    title_select = f"SELECT kb_id, note_id FROM knowledge_index WHERE {_GREP_VISIBILITY} AND {title_pred}"
    return f"SELECT count(*) FROM ({body_select} UNION {title_select}) AS candidates"


class KnowledgeStore:
    """pgvector search index for project knowledge base.

    Provides write-through upserts and hybrid search (RRF over
    dense vector + sparse keyword + recency).
    """

    def __init__(self, db, embedding_service):
        """Initialize KnowledgeStore.

        Args:
            db: PostgresDB instance (async, with execute/fetch/fetchval/fetchrow)
            embedding_service: EmbeddingService for generating vectors
        """
        self.db = db
        self.embedding_service = embedding_service

    @staticmethod
    def _ttl_for_note_type(note_type: str) -> Optional[int]:
        """Initial TTL (loop cycles) for a new note of this type.

        ``None`` = durable (no countdown). See KB_TTL_BY_NOTE_TYPE.
        """
        return KB_TTL_BY_NOTE_TYPE.get(
            (note_type or "").strip().lower(), KB_TTL_DEFAULT
        )

    @staticmethod
    def _content_hash(content: str) -> str:
        """SHA-256 hash of content for change detection."""
        return hashlib.sha256(content.encode()).hexdigest()

    @staticmethod
    def _prepare_embedding(embedding: Union[List[float], str]) -> List[float]:
        """Prepare embedding for asyncpg with pgvector codec.

        The pgvector asyncpg codec (registered via register_vector on
        connection init) expects List[float] or numpy arrays — not strings.
        If a string is passed (legacy), convert it back to a list.
        """
        if isinstance(embedding, list):
            return embedding
        if isinstance(embedding, str):
            # Legacy string format "[0.1,0.2,...]" — parse back to list
            cleaned = embedding.strip("[]")
            return [float(v) for v in cleaned.split(",") if v.strip()]
        return list(embedding)

    async def upsert_note(
        self,
        note_id: str,
        project_id: uuid.UUID,
        title: str,
        note_type: str,
        content: str,
        status: str = "active",
        confidence: Optional[str] = None,
        tags: Optional[List[str]] = None,
        keywords: Optional[List[str]] = None,
        job_id: Optional[uuid.UUID] = None,
        phase: Optional[int] = None,
        retrieval_messages: Optional[List[str]] = None,
        created_at: Optional[datetime] = None,
        modified_at: Optional[datetime] = None,
        priority: Optional[int] = 1,
        ready: Optional[bool] = None,
        ready_at: Optional[datetime] = None,
    ) -> uuid.UUID:
        """Upsert a note into the search index (write-through from Neo4j).

        Generates embedding only if content has changed (checked via content_hash).

        Args:
            note_id: Note slug from Neo4j
            project_id: Project UUID
            title: Note title
            note_type: Note type
            content: Full note body
            status: Note status
            confidence: Confidence level
            tags: Tag list
            keywords: Keyword list
            job_id: Creating job UUID
            phase: Phase number
            retrieval_messages: Retrieval query strings
            created_at: Original creation timestamp
            modified_at: Last modification timestamp
            priority: Backlog rank — 0=high, 1=normal (default), 2=low. A
                non-binding label; see PRIORITY_RANKS in knowledge_graph.py.
                ``None`` means "unknown, leave it as-is" (project-backlog-
                pipeline task 3 fix round 1): callers that could not
                determine a note's current priority (e.g. a failed lookback
                in the Neo4j-enabled kb_update path) pass ``None`` rather
                than guessing — both SQL branches below ``COALESCE`` it
                against the row's own existing value, and only fall back to
                1 for a genuinely new row, which has no prior value to
                clobber in the first place.
            ready: Whether THIS write authorizes the ticket for dispatch.
                Tri-state, and the distinction is load-bearing (see
                ``knowledge_index.ready_at``, vector migration 0020):
                ``True`` stamps ``ready_at = now`` — the officer explicitly
                (re-)armed the ticket in this call; ``False`` clears it — he
                withdrew the authorization; ``None`` leaves it alone.
                Every write that merely happens to carry a ``ready`` tag in
                its tag list MUST pass ``None``. Bumping the timestamp on an
                ordinary content edit would re-arm a ticket that is already
                claimed, and the tick would dispatch a second job for work
                already in flight.
            ready_at: Canonical READY timestamp to project when ``ready`` is
                true. Callers that just committed the canonical note pass the
                timestamp parsed from that file so projection cannot create a
                second logical authorization generation. Falls back to the
                database clock for legacy callers.

        Returns:
            UUID of the upserted row
        """
        new_hash = self._content_hash(content)
        tags_list = tags or []
        keywords_list = keywords or []
        retrieval_list = retrieval_messages or []

        # Check if content changed (skip re-embedding for metadata-only updates)
        existing_hash = await self.db.fetchval(
            """
            SELECT content_hash FROM knowledge_index
            WHERE project_id = $1 AND note_id = $2
            """,
            project_id,
            note_id,
        )

        if existing_hash == new_hash:
            # Content unchanged — update metadata only, skip embedding.
            # This is the branch a re-ready lands in: stamping `ready` on a
            # ticket touches tags and nothing else, so ready_at has to be
            # handled here or the officer's one re-arm action would be a no-op
            # and the ticket would stay parked forever.
            # priority = COALESCE($13, priority): this branch only ever runs
            # against a row that was just confirmed to exist (the
            # existing_hash lookup above), so a NULL $13 ("unknown, leave
            # unchanged") correctly falls back to the row's own current
            # value instead of overwriting it.
            row_id = await self.db.fetchval(
                """
                UPDATE knowledge_index
                SET title = $3, note_type = $4, status = $5, confidence = $6,
                    tags = $7, keywords = $8, job_id = $9, phase = $10,
                    retrieval_messages = $11, modified_at = $12,
                    indexed_at = NOW(), priority = COALESCE($13, priority),
                    ready_at = CASE
                        WHEN $14::boolean IS NULL THEN ready_at
                        WHEN $14::boolean THEN COALESCE($15, NOW())
                        ELSE NULL
                    END
                WHERE project_id = $1 AND note_id = $2
                RETURNING id
                """,
                project_id,
                note_id,
                title,
                note_type,
                status,
                confidence,
                tags_list,
                keywords_list,
                job_id,
                phase,
                retrieval_list,
                modified_at,
                priority,
                ready,
                ready_at,
            )
            logger.debug(f"Updated knowledge index (metadata only): {note_id}")
            return row_id

        # Content changed — generate new embedding
        # Embed content + retrieval messages together for better recall
        embed_text = content
        if retrieval_list:
            embed_text += "\n\n" + "\n".join(retrieval_list)

        embedding_raw = await self.embedding_service.embed(embed_text)
        embedding_vec = self._prepare_embedding(embedding_raw)

        # Build tsvector text from all searchable fields. Machine tags are
        # excluded: `category:researcher` is dispatch metadata, and a search for
        # "researcher" that surfaces every ticket in a pool buries the notes that
        # are actually about research. This is the only write path that puts tags
        # into search_doc at all (the reindexer indexes content alone), and only
        # on a content change — the metadata-only branch above leaves search_doc
        # untouched, so a row polluted before this landed stays polluted until
        # its content is edited or the vault is reindexed.
        search_text = " ".join(
            [
                title,
                content,
                " ".join(strip_machine_tags(tags_list)),
                " ".join(keywords_list),
                " ".join(retrieval_list),
            ]
        )

        # Initial TTL (loop cycles) by note_type — set only on INSERT. The
        # ON CONFLICT branch below deliberately omits remaining_cycles so an
        # existing note's countdown is preserved across content edits, and a
        # status-only change (kb_update → superseded) takes the metadata-only
        # path above which never touches it. See kb_convergence design doc.
        ttl_value = self._ttl_for_note_type(note_type)

        # priority is bound as $19 and referenced twice below, each with a
        # different COALESCE fallback: the VALUES list needs *some* concrete,
        # NOT-NULL value for a genuinely new row (nothing to preserve, so 1 —
        # DEFAULT_PRIORITY_RANK — is an honest default, not a guess), while
        # the ON CONFLICT branch is a real existing row and must fall back to
        # ITS current value, not 1, when $19 is NULL ("unknown, leave
        # unchanged"). Referencing EXCLUDED.priority there instead would be
        # wrong: it would already be the coalesced-to-1 VALUES-list result,
        # never NULL, so the "leave unchanged" case could never trigger.
        #
        # ready ($20) splits the same way for a different reason: a brand-new
        # row has no authorization to preserve, so NULL and False both mean
        # "not ready" there, while the ON CONFLICT branch must distinguish
        # them — NULL preserves an existing ready_at (this write said nothing
        # about readiness), False clears it (the officer withdrew it).
        row_id = await self.db.fetchval(
            """
            INSERT INTO knowledge_index (
                note_id, project_id, title, note_type, status, confidence,
                tags, keywords, job_id, phase, content, retrieval_messages,
                embedding, search_doc, created_at, modified_at, indexed_at,
                content_hash, remaining_cycles, priority, ready_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11, $12,
                $13, to_tsvector('english', $14), $15, $16, NOW(),
                $17, $18, COALESCE($19, 1),
                CASE WHEN $20::boolean THEN COALESCE($21, NOW()) ELSE NULL END
            )
            ON CONFLICT (project_id, note_id) DO UPDATE SET
                title = EXCLUDED.title,
                note_type = EXCLUDED.note_type,
                status = EXCLUDED.status,
                confidence = EXCLUDED.confidence,
                tags = EXCLUDED.tags,
                keywords = EXCLUDED.keywords,
                job_id = EXCLUDED.job_id,
                phase = EXCLUDED.phase,
                content = EXCLUDED.content,
                retrieval_messages = EXCLUDED.retrieval_messages,
                embedding = EXCLUDED.embedding,
                search_doc = EXCLUDED.search_doc,
                modified_at = EXCLUDED.modified_at,
                indexed_at = NOW(),
                content_hash = EXCLUDED.content_hash,
                priority = COALESCE($19, knowledge_index.priority),
                ready_at = CASE
                    WHEN $20::boolean IS NULL THEN knowledge_index.ready_at
                    WHEN $20::boolean THEN COALESCE($21, NOW())
                    ELSE NULL
                END
            RETURNING id
            """,
            note_id,
            project_id,
            title,
            note_type,
            status,
            confidence,
            tags_list,
            keywords_list,
            job_id,
            phase,
            content,
            retrieval_list,
            embedding_vec,
            search_text,
            created_at,
            modified_at,
            new_hash,
            ttl_value,
            priority,
            ready,
            ready_at,
        )

        logger.debug(f"Upserted knowledge index: {note_id} (content changed)")
        return row_id

    async def delete_note(self, project_id: uuid.UUID, note_id: str) -> bool:
        """Delete a note from the search index.

        Args:
            project_id: Project UUID
            note_id: Note slug

        Returns:
            True if a row was deleted
        """
        result = await self.db.fetchval(
            """
            DELETE FROM knowledge_index
            WHERE project_id = $1 AND note_id = $2
            RETURNING id
            """,
            project_id,
            note_id,
        )
        return result is not None

    async def get_note_revisions(
        self,
        project_id: uuid.UUID,
        note_id: str,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Prior versions of a note, newest change first (attributable history).

        Reads ``knowledge_note_revisions`` — rows cut by the ``knowledge_index``
        capture triggers (vector migration 0016, workspace_and_change_records.md
        §6.2), one per content/title-changing UPDATE and one per DELETE. Each
        dict is the note as it stood BEFORE that change, plus the change
        envelope: ``action`` ('update'|'delete'), ``replaced_by_job_id`` (the
        job whose write displaced this version; NULL for deletes) and
        ``changed_at``. Revisions carry no embedding/search_doc by design —
        a restore re-embeds through the normal upsert path.

        Args:
            project_id: Project UUID
            note_id: Note slug
            limit: Max revisions returned (newest first)

        Returns:
            List of revision dicts, most recent change first
        """
        rows = await self.db.fetch(
            """
            SELECT id, project_id, note_id, title, note_type, status,
                   confidence, tags, keywords, job_id, phase, content,
                   created_at, modified_at, action, replaced_by_job_id,
                   changed_at
            FROM knowledge_note_revisions
            WHERE project_id = $1 AND note_id = $2
            ORDER BY changed_at DESC
            LIMIT $3
            """,
            project_id,
            note_id,
            limit,
        )
        return [
            {
                "id": r["id"],
                "note_id": r["note_id"],
                "project_id": r["project_id"],
                "title": r["title"],
                "note_type": r["note_type"],
                "status": r["status"],
                "confidence": r["confidence"],
                "tags": r["tags"] or [],
                "keywords": r["keywords"] or [],
                "job_id": r["job_id"],
                "phase": r["phase"],
                "content": r["content"],
                "created_at": r["created_at"],
                "modified_at": r["modified_at"],
                "action": r["action"],
                "replaced_by_job_id": r["replaced_by_job_id"],
                "changed_at": r["changed_at"],
            }
            for r in rows
        ]

    # =========================================================================
    # Chunk-granular index — OKF files-canonical KB slice 3 (§5.1)
    #
    # Inert persistence primitives: the git-tree reindexer (PR3) writes these and
    # the retrieval cutover (PR4) reads them. Nothing in the running system calls
    # them yet — the schema (0008_kb_index_chunking.sql) and this surface land
    # together so PR3/PR4 are pure query code.
    # =========================================================================

    async def get_watermark(self, kb_id: uuid.UUID) -> Optional[KbWatermark]:
        """The index watermark for a KB, or ``None`` if never indexed.

        PR3 reads this to learn the commit the index was built as-of, then diffs
        the repo HEAD against it.
        """
        row = await self.db.fetchrow(
            "SELECT * FROM kb_index_watermark WHERE kb_id = $1",
            kb_id,
        )
        return KbWatermark.from_row(dict(row)) if row else None

    @asynccontextmanager
    async def try_reindex_lock(self, kb_id: uuid.UUID):
        """Claim a cross-replica session lock for one KB reindex run.

        The orchestrator already serializes runs inside one process. This lock
        closes the remaining HA race where an operator request reaches a
        different replica while the leader sweeper is rebuilding the same KB.
        It is deliberately a non-blocking claim: callers can report
        ``already-indexing`` instead of tying up a request or sweep worker.
        """
        lock_key = self._reindex_lock_key(kb_id)
        async with self.db.acquire() as conn:
            claimed = bool(
                await conn.fetchval("SELECT pg_try_advisory_lock($1)", lock_key)
            )
            try:
                yield claimed
            finally:
                if claimed:
                    await conn.fetchval("SELECT pg_advisory_unlock($1)", lock_key)

    @asynccontextmanager
    async def reindex_lock(self, kb_id: uuid.UUID):
        """Wait for exclusive ownership of one KB's disposable index.

        Reindex requests use :meth:`try_reindex_lock` so an API request or
        sweep never queues behind another replica. Destructive lifecycle work
        is different: datasource deletion must wait for an in-flight rebuild
        to finish before removing the final vector rows. Both contexts derive
        the same session advisory-lock key, so they coordinate across
        orchestrator replicas as well as within one process.
        """
        lock_key = self._reindex_lock_key(kb_id)
        async with self.db.acquire() as conn:
            await conn.fetchval("SELECT pg_advisory_lock($1)", lock_key)
            try:
                # Destructive callers must reuse this connection for their
                # vector transaction. Acquiring a second pool connection while
                # holding the session lock deadlocks when the pool size is one.
                yield conn
            finally:
                await conn.fetchval("SELECT pg_advisory_unlock($1)", lock_key)

    @staticmethod
    def _reindex_lock_key(kb_id: uuid.UUID) -> int:
        """Stable signed bigint key shared by reindex and deletion claims."""
        return int.from_bytes(
            hashlib.sha256(kb_id.bytes).digest()[:8], "big", signed=True
        )

    @staticmethod
    def _converge_lock_key(kb_id: uuid.UUID) -> int:
        """Advisory-lock key for the KB convergence pass — distinct from the
        reindex key on purpose: a converge run is minutes of aux-LLM time,
        and holding the *reindex* lock that long would defer every inline
        index in the project (``deferred:reindex-running``)."""
        return int.from_bytes(
            hashlib.sha256(kb_id.bytes + b":converge").digest()[:8],
            "big",
            signed=True,
        )

    @asynccontextmanager
    async def try_converge_lock(self, kb_id: uuid.UUID):
        """Non-blocking claim of the per-KB convergence lock (kb_gardening G4).

        Yields True when this caller is the single consolidator for the KB
        right now, False when another pass (another fan-out member, another
        replica) holds it — the caller then skips its run; the stale queue is
        still there next time. Session-scoped like the reindex lock, so it
        coordinates across orchestrator replicas and agent pods alike.
        """
        lock_key = self._converge_lock_key(kb_id)
        async with self.db.acquire() as conn:
            claimed = bool(
                await conn.fetchval("SELECT pg_try_advisory_lock($1)", lock_key)
            )
            try:
                yield claimed
            finally:
                if claimed:
                    await conn.fetchval("SELECT pg_advisory_unlock($1)", lock_key)

    async def upsert_watermark(
        self,
        kb_id: uuid.UUID,
        repo_name: Optional[str],
        branch: Optional[str],
        indexed_commit: Optional[str],
        pipeline_version: Optional[str],
        *,
        source_head: Optional[str] = None,
        status: str = "ready",
        last_error: Optional[str] = None,
        advisory: Optional[str] = None,
    ) -> None:
        """Record (or advance) the as-of commit for a KB's index.

        The as-of commit advances only at the END of a reindex, after every
        changed row is written, so an interrupted run leaves the old commit and
        the next run re-diffs from there (per-row ``blob_sha`` skips the
        already-current rows).

        A *rebuild* also calls this once up front, with ``indexed_commit`` left
        at its previous value and ``status='indexing'``, purely to record the
        ``pipeline_version`` it is rebuilding to. That write is what makes the
        rebuild resumable: the version is the flag the next run reads to decide
        whether to force a full pass, and recording it only on success meant an
        interrupted rebuild always started over (see
        :meth:`clear_note_stamps`). Because it never carries the new commit, the
        "a failed run must not advance the index" invariant is unchanged — but
        assert it on ``indexed_commit``, not on this method's call count.

        The ``::varchar`` casts on $4/$6 are load-bearing, not decoration. Both
        parameters feed ``VARCHAR(64)`` columns *and* appear inside
        ``COALESCE($6, $4)``. Left uncast, that COALESCE has two untyped
        arguments, so Postgres resolves it to its preferred unknown type
        ``text`` and records $4 as ``text``; the direct ``indexed_commit = $4``
        then wants ``character varying`` and the whole statement is rejected at
        parse time with ``inconsistent types deduced for parameter $4``. That
        killed *every* watermark write — the reindex wrote all its rows and then
        died on this last statement, so ``indexed_commit`` never advanced and
        each run re-embedded the whole vault
        (knowledge-history/done/kb_reindex_watermark_never_advances.md). Casting at every
        occurrence pins both parameters to the column type regardless of which
        use Postgres resolves first. ``::varchar`` (unqualified) rather than
        ``::varchar(64)`` deliberately: a cast to a length-qualified type
        silently truncates, whereas assignment to the column keeps its length
        check and errors loudly on an over-long commit id.
        ``set_watermark_status`` below needs no such cast — its COALESCE's
        second argument is a column, which pins the type on its own.

        ``advisory`` (migration 0022) is a line for conditions that were
        skipped rather than failed (spec H2, e.g. duplicate note ids). Like the
        wedge fields it is only *replaced* on a ``ready`` write (final review,
        M1). The same resumable-rebuild call that must not reset the streak
        passes no ``advisory`` at all, so an unconditional ``advisory = $9``
        blanked the last sweep's "1 note skipped (duplicate id)" line the
        moment a rebuild started — deleting exactly the explanation a reader
        needs for the note that is missing from the index. The INSERT branch
        keeps the plain ``$9``: there is no prior row to preserve.

        This method is also the wedge detector's reset, but only when ``status`` is
        ``'ready'``: that clears ``error_fingerprint``/``error_streak``/
        ``wedged_since``, because reaching a ``ready`` write means the run
        finished far enough to advance the watermark, which is not the "same
        failure repeating" state ``set_watermark_status`` tracks. A
        resumable-rebuild caller (``kb_reindex.py``) also calls this with
        ``status="indexing"`` up front, purely to record ``pipeline_version``
        before re-diffing — that write must NOT reset the streak, or a
        mid-rebuild wedge would be silently wiped every time the rebuild
        resumes (Controller addendum, S0 re-review 2026-09-04; same reset
        class the S0 review caught in ``set_watermark_status``).
        """
        await self.db.execute(
            """
            INSERT INTO kb_index_watermark
                (kb_id, repo_name, branch, indexed_commit, pipeline_version,
                 source_head, status, last_attempt_at, last_success_at,
                 last_error, updated_at, advisory)
            VALUES ($1, $2, $3, $4::varchar, $5,
                    COALESCE($6::varchar, $4::varchar), $7, NOW(),
                    CASE WHEN $7 = 'ready' THEN NOW() ELSE NULL END, $8, NOW(), $9)
            ON CONFLICT (kb_id) DO UPDATE
               SET repo_name = $2,
                   branch = $3,
                   indexed_commit = $4::varchar,
                   pipeline_version = $5,
                   source_head = COALESCE($6::varchar, $4::varchar),
                   status = $7,
                   last_attempt_at = NOW(),
                   last_success_at = CASE
                       WHEN $7 = 'ready' THEN NOW()
                       ELSE kb_index_watermark.last_success_at
                   END,
                   last_error = $8,
                   updated_at = NOW(),
                   advisory = CASE
                       WHEN $7 = 'ready' THEN $9
                       ELSE kb_index_watermark.advisory
                   END,
                   error_fingerprint = CASE
                       WHEN $7 = 'ready' THEN NULL
                       ELSE kb_index_watermark.error_fingerprint
                   END,
                   error_streak = CASE
                       WHEN $7 = 'ready' THEN 0
                       ELSE kb_index_watermark.error_streak
                   END,
                   wedged_since = CASE
                       WHEN $7 = 'ready' THEN NULL
                       ELSE kb_index_watermark.wedged_since
                   END
            """,
            kb_id,
            repo_name,
            branch,
            indexed_commit,
            pipeline_version,
            source_head,
            status,
            last_error,
            advisory,
        )

    async def set_watermark_status(
        self,
        kb_id: uuid.UUID,
        status: str,
        *,
        source_head: Optional[str] = None,
        last_error: Optional[str] = None,
        repo_name: Optional[str] = None,
        branch: Optional[str] = None,
        error_fingerprint: Optional[str] = None,
        conn: Any = None,
    ) -> None:
        """Record an index attempt without advancing ``indexed_commit``.

        Also the wedge detector's streak arithmetic (migration 0022, spec
        WP3/H3): a ``partial`` run whose ``error_fingerprint`` matches the row
        already on file is the *same* failure repeating, so the streak
        increments and ``wedged_since`` stamps the first time it crosses
        ``WEDGE_STREAK_THRESHOLD``; a different fingerprint starts a new
        streak at 1. Per spec H3 the three wedge fields reset ONLY on
        ``ready`` or a fingerprint change — every other status (``indexing``,
        ``failed``, ``pending``, ...) leaves them untouched. This matters
        because ``kb_reindex.py`` writes an ``indexing`` status at the start
        of every sweep, before that sweep's own terminal write; an earlier
        version reset the streak on any non-``partial`` status, so that
        ``indexing`` write silently wiped the streak every single run and the
        counter could never reach the threshold. All of this is computed
        inside the statement so two concurrent sweeps can't race the counter
        with a read-modify-write in Python.
        """
        executor = conn if conn is not None else self.db
        await executor.execute(
            """
            INSERT INTO kb_index_watermark
                (kb_id, repo_name, branch, source_head, status,
                 last_attempt_at, last_error, updated_at,
                 error_fingerprint, error_streak, wedged_since)
            VALUES ($1, $2, $3, $4, $5, NOW(), $6, NOW(),
                    CASE WHEN $5 = 'partial' THEN $7 ELSE NULL END,
                    CASE WHEN $5 = 'partial' THEN 1 ELSE 0 END, NULL)
            ON CONFLICT (kb_id) DO UPDATE
               SET repo_name = COALESCE($2, kb_index_watermark.repo_name),
                   branch = COALESCE($3, kb_index_watermark.branch),
                   source_head = COALESCE($4, kb_index_watermark.source_head),
                   status = $5,
                   last_attempt_at = NOW(),
                   last_error = $6,
                   updated_at = NOW(),
                   error_fingerprint = CASE
                       WHEN $5 = 'ready' THEN NULL
                       WHEN $5 = 'partial' THEN $7
                       ELSE kb_index_watermark.error_fingerprint END,
                   error_streak = CASE
                       WHEN $5 = 'ready' THEN 0
                       WHEN $5 = 'partial'
                            AND $7 IS NOT DISTINCT FROM kb_index_watermark.error_fingerprint
                           THEN kb_index_watermark.error_streak + 1
                       WHEN $5 = 'partial' THEN 1
                       ELSE kb_index_watermark.error_streak END,
                   wedged_since = CASE
                       WHEN $5 = 'ready' THEN NULL
                       WHEN $5 = 'partial'
                            AND $7 IS NOT DISTINCT FROM kb_index_watermark.error_fingerprint
                            AND kb_index_watermark.error_streak + 1 >= $8
                           THEN COALESCE(kb_index_watermark.wedged_since, NOW())
                       WHEN $5 = 'partial' THEN NULL
                       ELSE kb_index_watermark.wedged_since END
            """,
            kb_id,
            repo_name,
            branch,
            source_head,
            status,
            last_error,
            error_fingerprint,
            WEDGE_STREAK_THRESHOLD,
        )

    async def update_index_progress(
        self,
        kb_id: uuid.UUID,
        notes_done: int,
        notes_total: Optional[int] = None,
        *,
        conn: Any = None,
    ) -> None:
        """Update the current run's progress counters on an existing watermark row.

        Cockpit renders a determinate progress bar from ``notes_done``/``notes_total``
        while a KB indexes. This is a deliberately narrow UPDATE: it never creates a
        row and never touches ``status``/``indexed_commit`` (the status writers own
        those), so a stray progress write can neither resurrect a deleted KB nor
        advance the as-of commit. ``notes_total`` is only overwritten when provided.
        """
        executor = conn if conn is not None else self.db
        await executor.execute(
            """
            UPDATE kb_index_watermark
               SET notes_done = $2,
                   notes_total = COALESCE($3, notes_total),
                   updated_at = NOW()
             WHERE kb_id = $1
            """,
            kb_id,
            notes_done,
            notes_total,
        )

    async def delete_kb_index(self, kb_id: uuid.UUID, *, conn: Any = None) -> None:
        """Delete one disposable KB index; chunk/link rows cascade.

        ``conn`` is supplied by :meth:`reindex_lock` for coordinated datasource
        deletion so the session advisory lock and cleanup transaction share one
        pool connection. Standalone callers retain the normal acquire path.
        """

        async def delete_with_connection(active_conn: Any) -> None:
            async with active_conn.transaction():
                await active_conn.execute(
                    "DELETE FROM knowledge_index WHERE kb_id = $1",
                    kb_id,
                )
                await active_conn.execute(
                    "DELETE FROM kb_index_watermark WHERE kb_id = $1",
                    kb_id,
                )

        if conn is not None:
            await delete_with_connection(conn)
            return

        async with self.db.acquire() as acquired_conn:
            await delete_with_connection(acquired_conn)

    async def get_indexed_blob_shas(self, kb_id: uuid.UUID) -> Dict[str, str]:
        """``{path: blob_sha}`` for every indexed note in a KB.

        The tree-diff reindexer (PR3) compares this against the git tree's
        ``{path: blob_sha}`` map to decide which notes changed, were added, or were
        removed — the per-row self-heal that makes interrupted re-runs cheap.
        """
        rows = await self.db.fetch(
            """
            SELECT path, blob_sha FROM knowledge_index
            WHERE kb_id = $1 AND path IS NOT NULL
            """,
            kb_id,
        )
        return {r["path"]: r["blob_sha"] for r in rows}

    async def clear_note_stamps(
        self,
        kb_id: uuid.UUID,
        *,
        embedding_version: Optional[str] = None,
        batch_size: int = 200,
    ) -> int:
        """Drop note ``blob_sha`` stamps so a rebuild becomes resumable.

        A full rebuild used to exist only as ``plan_reindex(full=True)`` — a
        per-run instruction that had to survive to completion, because the fact
        driving it (the watermark's ``pipeline_version``) is written on success
        alone. Any interruption left that fact unrecorded, so the next run
        re-derived ``full=True`` and re-embedded the notes the dead run had
        already finished: a 2635-note vault restarted from note zero after every
        orchestrator rollout.

        Recording the invalidation on the rows instead makes it durable. An
        unstamped note stays in every subsequent diff until it is individually
        re-embedded and re-stamped, so a rebuild resumes exactly where it
        stopped — the per-row self-heal :func:`plan_reindex` was built around.

        Chunks are deliberately left in place: :meth:`replace_note_chunks` swaps
        them one note at a time, so search keeps serving the previous index for
        the duration of the rebuild rather than going dark.

        **Batched deliberately.** ``knowledge_index`` carries an HNSW index over
        the note centroid plus two GIN indexes, so each row touched costs an
        index insert on every one of them — measured at ~9ms/row on dev. As a
        single statement over a large KB that runs past the pool's 60s
        ``command_timeout``, and asyncpg cancels it with an *empty-message*
        ``asyncio.TimeoutError`` (Postgres side: ``canceling statement due to
        user request``). That is exactly how the first version of this shipped
        and silently fell back in production, so keep the batching. Each batch
        is its own statement, which also makes a half-finished invalidation
        harmless — the next run simply continues it.

        ``embedding_version`` narrows the sweep to rows stamped by a *different*
        embedding version. That is the form to use when nothing is known to be
        stale: it normally matches zero rows and costs one cheap statement,
        while still catching a KB whose embedding model moved under it.

        Returns the number of stamps cleared.
        """
        total = 0
        while True:
            rows = await self.db.fetch(
                """
                UPDATE knowledge_index
                   SET blob_sha = NULL
                 WHERE id IN (
                       SELECT id
                         FROM knowledge_index
                        WHERE kb_id = $1
                          AND blob_sha IS NOT NULL
                          AND ($2::text IS NULL
                               OR embedding_version IS DISTINCT FROM $2::text)
                        LIMIT $3
                 )
                RETURNING id
                """,
                kb_id,
                embedding_version,
                batch_size,
            )
            total += len(rows)
            # A short batch means the predicate is exhausted — every row this
            # statement touched no longer matches it.
            if len(rows) < batch_size:
                return total

    async def adopt_legacy_row(
        self,
        kb_id: uuid.UUID,
        note_id: str,
        path: str,
        *,
        movable_paths: Optional[List[str]] = None,
    ) -> Optional[uuid.UUID]:
        """Claim a pre-slice-3 row or move a stable note id to its new path.

        Legacy rows (written by the DB-first ``upsert_note`` path) carry
        ``(project_id, note_id)`` but no ``path``. A reindex INSERT for the same
        slug would violate ``uq_knowledge_project_note`` — an arbiter
        ``ON CONFLICT (kb_id, path)`` can't absorb a conflict on a *different*
        unique constraint. So the reindexer first adopts: set ``kb_id`` + ``path``
        on the pathless row (project-scoped KB: ``project_id`` doubles as the kb
        id), after which :meth:`upsert_kb_note` hits the ``(kb_id, path)`` arbiter
        normally.

        The same identity-first update also handles a Git rename whose OKF
        frontmatter keeps the same ``id`` when the old path is in this run's
        deletion set: move that existing row before the path-keyed upsert,
        otherwise the old row still owns the unique ``(project_id, note_id)``
        key and the first rename pass fails. Requiring an explicitly movable old
        path prevents a second file with a duplicate frontmatter id from
        stealing the still-canonical row. The destination guard likewise leaves
        path replacement/collision handling to the normal path-keyed upsert.
        Returns the adopted/moved row id, or ``None`` when no row was eligible.
        """
        movable_paths = movable_paths or []
        return await self.db.fetchval(
            """
            UPDATE knowledge_index AS note
            SET kb_id = $1, path = $3
            WHERE note.project_id = $1
              AND note.note_id = $2
              AND note.path IS DISTINCT FROM $3
              AND (note.path IS NULL OR note.path = ANY($4::text[]))
              AND NOT EXISTS (
                  SELECT 1
                  FROM knowledge_index AS destination
                  WHERE destination.kb_id = $1
                    AND destination.path = $3
                    AND destination.id <> note.id
              )
            RETURNING note.id
            """,
            kb_id,
            note_id,
            path,
            movable_paths,
        )

    async def find_note_id_owner(
        self,
        kb_id: uuid.UUID,
        note_id: str,
    ) -> Optional[Dict[str, Any]]:
        """The row that currently owns ``(project_id, note_id)``, if any.

        Purely diagnostic — the reindexer calls this *after* an INSERT has
        already failed, to turn an opaque ``uq_knowledge_project_note``
        duplicate-key error into a statement of what is actually wrong. The
        constraint fires when one note id exists at two paths (the classic
        cause: a vault where a note filename was used as a directory, so
        ``knowledge/<slug>.md`` and ``knowledge/<other>.md/<slug>.md`` both
        parse to the same OKF id). Neither :meth:`adopt_legacy_row` nor
        ``upsert_kb_note``'s ``(kb_id, path)`` arbiter can absorb that — one
        of the two paths must lose, every run, forever — and the raw Postgres
        error names only the id, never the path the index is holding.

        Returns ``{"id", "path", "status", "indexed_at"}`` for the incumbent
        row, or None when nothing owns the id (in which case the failure was
        something other than this condition — a genuine race, say).
        """
        row = await self.db.fetchrow(
            """
            SELECT id, path, status, indexed_at
              FROM knowledge_index
             WHERE project_id = $1 AND note_id = $2
             LIMIT 1
            """,
            kb_id,
            note_id,
        )
        return dict(row) if row else None

    async def upsert_kb_note(
        self,
        kb_id: uuid.UUID,
        note_id: str,
        path: str,
        title: str,
        note_type: str,
        content: str,
        blob_sha: Optional[str],
        embedding_version: Optional[str],
        *,
        status: str = "active",
        confidence: Optional[str] = None,
        tags: Optional[List[str]] = None,
        keywords: Optional[List[str]] = None,
        retrieval_messages: Optional[List[str]] = None,
        superseded_by: Optional[str] = None,
        invalidated_at: Optional[datetime] = None,
        job_id: Optional[uuid.UUID] = None,
        phase: Optional[int] = None,
        created_at: Optional[datetime] = None,
        modified_at: Optional[datetime] = None,
        priority: Optional[int] = 1,
        ready_at: Optional[datetime] = None,
    ) -> uuid.UUID:
        """Upsert a note-level row keyed by ``(kb_id, path)``.

        The metadata half of a reindex write — the dense vectors live on
        ``knowledge_chunks`` (see :meth:`replace_note_chunks`), so this INSERT never
        touches the ``embedding`` column. It does write ``search_doc``: that is the
        *note-level* sparse index the orchestrator's ``/knowledge/search`` endpoint
        queries, and it is separate from the chunk-level arm ``kb_search`` uses.
        Without it here, a vault seeded by pushing files indexes fine and stays
        invisible to note-level search — the agent finds it, a human browsing the
        cockpit does not. The reindexer passes ``blob_sha`` /
        ``embedding_version`` as None and stamps them via
        :meth:`stamp_note_indexed` only after the chunk write succeeds; the row
        also carries ``project_id = kb_id`` so legacy project-scoped queries keep
        working through the transition. ``priority`` is the backlog rank parsed
        by ``note_fields`` from frontmatter (0=high/1=normal/2=low) — this is
        the file-edit path onto ``knowledge_index.priority``, the counterpart to
        :meth:`upsert_note`'s agent-write path.

        ``priority=None`` means "the file had no ``priority:`` line, leave
        the stored rank as-is" (fix round 2, Finding 3) — the counterpart to
        :meth:`upsert_note`'s own sentinel. Every pre-existing note lacks
        that line, and this runs on every merge and via the sweeper, so
        writing a concrete default here on every reindex would silently
        overwrite any priority set through the agent-write path. Both
        ``COALESCE``s below fall back to the row's existing value on
        conflict, and to 1 only for a genuinely new row (nothing to
        preserve there).

        ``ready_at`` follows the same sentinel, and for a sharper reason. It
        is an ABSOLUTE timestamp here, not the action flag
        :meth:`upsert_note` takes: a file replay is not an authorization
        event, it is a replay of one that already happened, so the value
        comes off the frontmatter rather than the clock. ``None`` means the
        file carried no ``ready_at:`` line — leave the stored value alone.
        Stamping ``now()`` here instead would re-arm every ready ticket in
        the vault on the next reindex and re-dispatch work already done.

        ``retrieval_messages`` takes the same sentinel too, for a third
        reason again: OKF frontmatter has no ``retrieval_messages:`` field
        (:func:`_render_note_md` never emits one), so a reindex has no
        opinion of its own. ``None`` means "leave the stored value alone" —
        the counterpart to :meth:`upsert_note`'s own agent-write path, and
        the only way a value set through the inline materialize call
        (Slice A) survives the very next sweep instead of being wiped back
        to empty.

        ``job_id`` and ``phase`` take the same sentinel, for the sharpest
        version of the reason yet: the note file cannot carry either (OKF
        frontmatter has no job or phase field), so the *sweep* — which
        re-indexes notes it did not write — has no opinion and passed None,
        while this statement wrote both unconditionally. Every note therefore
        lost its provenance on the first sweep after it was written. That
        matters because ``job_id`` backs a shipped filter:
        :meth:`list_notes` builds a real ``job_id = $n`` WHERE clause behind
        ``kb_list(job_id=...)``, so a NULLed column is the difference between
        "which notes did job X write?" answering and answering nothing.
        ``None`` now means "leave the stored value alone"; only the inline
        materialize call, which knows the writing job, has a value to set.
        ``phase`` gets the sentinel too even though nothing supplies one yet —
        it costs one line and it is what stops the next writer's value from
        being swept away the same way.

        ``modified_at`` takes the sentinel too. It orders the search
        function's recency arm — a bare ``ORDER BY ki.modified_at DESC``
        (vector_schema_current.sql), with no ``NULLS LAST`` — and a file with
        no ``modified:`` line has no opinion about it, so a replay must leave
        the stored value alone rather than blank it. Blanking is worse than
        it sounds: Postgres sorts NULLs FIRST under ``DESC``, so a NULLed row
        does not drop to the bottom of that arm, it jumps to the top of it.
        ``created_at`` needs no sentinel for the opposite reason: it is
        absent from this branch entirely, which is what stops a second write
        from moving a note's creation time.

        ``remaining_cycles`` is seeded on INSERT only. After Slice A the
        file-canonical path is the only writer for a new note, so if it did
        not seed the TTL, convergence re-verification would never see the
        note. It is deliberately absent from the ON CONFLICT branch: a
        reindex is a replay, not a new authorization, and re-seeding would
        reset every note's TTL on every sweep.

        Returns the row id (for the chunk FK).
        """
        content_hash = self._content_hash(content)
        # Initial TTL (loop cycles) by note_type — set only on INSERT, the same
        # rule (and the same helper) as upsert_note. After Slice A this is the
        # only writer for a new note, so without it convergence never sees one.
        ttl_value = self._ttl_for_note_type(note_type)
        # priority ($21) is referenced twice below with two different
        # COALESCE fallbacks, mirroring upsert_note's INSERT branch (see the
        # comment there): the VALUES list needs a concrete NOT-NULL value for
        # a genuinely new row (1 — nothing to preserve), while ON CONFLICT is
        # a real existing row and must fall back to ITS current value, not 1,
        # when $21 is NULL. That fallback deliberately references the raw
        # $21 parameter, not EXCLUDED.priority: EXCLUDED.priority is already
        # the VALUES-list's coalesced-to-1 result and can never itself be
        # NULL, so using it here would silently reintroduce the clobber.
        #
        # retrieval_messages ($11) gets the identical treatment for the same
        # reason, against the column's own empty-array default — ``TEXT[]
        # DEFAULT '{}'``, vector/0001_initial.sql:495 — rather than a
        # literal 1. The explicit ``::text[]`` casts are load-bearing: both
        # sides of a bare ``COALESCE($11, '{}')`` are otherwise untyped, and
        # Postgres's fallback type for an all-unknown COALESCE is scalar
        # ``text``, not ``text[]`` (this mock-based suite cannot catch that
        # mistake — it never executes real SQL).
        #
        # remaining_cycles ($23) is new (task 5) and does NOT follow this
        # pattern — it has no ON CONFLICT branch at all (see the docstring).
        return await self.db.fetchval(
            """
            INSERT INTO knowledge_index
                (kb_id, project_id, note_id, path, title, note_type, content,
                 search_doc,
                 status, confidence, tags, keywords, retrieval_messages,
                 blob_sha, embedding_version, superseded_by, invalidated_at,
                 job_id, phase, content_hash, created_at, modified_at, indexed_at,
                 priority, ready_at, remaining_cycles)
            VALUES
                ($1, $1, $2, $3, $4, $5, $6,
                 to_tsvector('english', $6),
                 $7, $8, $9, $10, COALESCE($11::text[], '{}'::text[]),
                 $12, $13, $14, $15,
                 $16, $17, $18, $19, $20, NOW(),
                 COALESCE($21, 1), $22, $23)
            ON CONFLICT (kb_id, path) WHERE kb_id IS NOT NULL AND path IS NOT NULL
            DO UPDATE SET
                note_id = $2, title = $4, note_type = $5, content = $6,
                status = $7, confidence = $8, tags = $9, keywords = $10,
                retrieval_messages = COALESCE($11::text[], knowledge_index.retrieval_messages),
                blob_sha = $12, embedding_version = $13,
                superseded_by = $14, invalidated_at = $15,
                job_id = COALESCE($16, knowledge_index.job_id),
                phase = COALESCE($17, knowledge_index.phase),
                content_hash = $18,
                modified_at = COALESCE($20, knowledge_index.modified_at),
                indexed_at = NOW(),
                search_doc = EXCLUDED.search_doc,
                priority = COALESCE($21, knowledge_index.priority),
                ready_at = COALESCE($22, knowledge_index.ready_at)
            RETURNING id
            """,
            kb_id,
            note_id,
            path,
            title,
            note_type,
            content,
            status,
            confidence,
            tags or [],
            keywords or [],
            retrieval_messages,
            blob_sha,
            embedding_version,
            superseded_by,
            invalidated_at,
            job_id,
            phase,
            content_hash,
            created_at,
            modified_at,
            priority,
            ready_at,
            ttl_value,
        )

    async def stamp_note_indexed(
        self,
        note_row: uuid.UUID,
        blob_sha: str,
        embedding_version: str,
        centroid: Optional[List[float]] = None,
    ) -> None:
        """Mark a note's chunks as durably written (PR3.1).

        ``blob_sha``/``embedding_version`` on the note row mean "the chunk set for
        this git blob is fully persisted" — so the reindexer upserts the note
        UNSTAMPED, writes chunks, then stamps. A chunk-write failure leaves
        ``blob_sha`` NULL and the next tree-diff retries the note (a stamped-but-
        chunkless note would read as up-to-date forever — the live 2026-07-05
        zero-chunks gap).

        ``centroid`` (PR4d) is the note's whole-note vector — the mean of its
        chunk embeddings — written back to the ``embedding`` column *in the same
        UPDATE* so it lands atomically with the stamp (no stamped-without-centroid
        window). It re-arms ``find_near_duplicate_pairs``, which the chunk cutover
        left blind (the note row's ``embedding`` had gone NULL). Omitted for a
        chunkless note, in which case the embedding column is left untouched.
        """
        if centroid is not None:
            await self.db.execute(
                """
                UPDATE knowledge_index
                SET blob_sha = $2, embedding_version = $3, embedding = $4
                WHERE id = $1
                """,
                note_row,
                blob_sha,
                embedding_version,
                self._prepare_embedding(centroid),
            )
        else:
            await self.db.execute(
                """
                UPDATE knowledge_index
                SET blob_sha = $2, embedding_version = $3
                WHERE id = $1
                """,
                note_row,
                blob_sha,
                embedding_version,
            )

    async def replace_note_chunks(
        self,
        note_row: uuid.UUID,
        kb_id: uuid.UUID,
        chunks: List[Dict[str, Any]],
        embedding_version: str,
    ) -> int:
        """Replace a note's chunk rows wholesale — delete all, insert the new set.

        Called by the reindexer right after :meth:`upsert_kb_note`; the delete keeps
        chunk_ix contiguous when a note shrinks. Each chunk is a dict with
        ``chunk_ix``, ``content``, ``embedding`` (and optional ``heading_path``);
        ``search_doc`` is computed at write time so the sparse arm needs no trigger.
        Returns the number of chunks written.
        """
        await self.db.execute(
            "DELETE FROM knowledge_chunks WHERE note_row = $1",
            note_row,
        )
        for ch in chunks:
            await self.db.execute(
                """
                INSERT INTO knowledge_chunks
                    (note_row, kb_id, chunk_ix, heading_path, content,
                     embedding, search_doc, embedding_version)
                VALUES ($1, $2, $3, $4, $5, $6, to_tsvector('english', $5), $7)
                """,
                note_row,
                kb_id,
                ch["chunk_ix"],
                ch.get("heading_path"),
                ch["content"],
                self._prepare_embedding(ch["embedding"]),
                embedding_version,
            )
        return len(chunks)

    async def delete_kb_note(self, kb_id: uuid.UUID, path: str) -> bool:
        """Remove a note by ``(kb_id, path)``; its chunks cascade away.

        The reindexer calls this for notes that vanished from the git tree. The
        ``ON DELETE CASCADE`` on ``knowledge_chunks.note_row`` reaps the chunk rows.
        Returns True if a row was deleted.
        """
        result = await self.db.fetchval(
            """
            DELETE FROM knowledge_index
            WHERE kb_id = $1 AND path = $2
            RETURNING id
            """,
            kb_id,
            path,
        )
        return result is not None

    async def reconcile_orphans(
        self,
        project_id: uuid.UUID,
        tree_slugs: List[str],
        grace: timedelta = timedelta(hours=1),
    ) -> int:
        """Archive pathless active rows the git tree can never adopt (R-1).

        A row written by the agent write-through (:meth:`upsert_note`) carries
        ``(project_id, note_id)`` but no ``kb_id``/``path``; the reindexer adopts
        it once a file with ``note_id``'s slug appears in the tree
        (:meth:`adopt_legacy_row`). A pathless *active* row whose ``note_id``
        matches no current tree slug, and which has sat unadopted past the
        adoption ``grace``, is an orphan (failed/squashed commit, slug mismatch,
        create-then-delete). Soft-archive it — reversible, and it drops out of
        every functional surface (search/near-dup via status, list/read via the
        path guard); ``invalidated_at`` leaves an audit trail.

        Keyed on ``project_id``, **not** ``kb_id``: un-adopted ghosts have
        ``kb_id IS NULL``, so a ``kb_id`` filter would match none of them. The
        ``grace`` (anchored on ``indexed_at``, the last write) protects a
        just-written provisional row whose file is committed but not yet in the
        fetched HEAD. Returns the number of rows archived.
        """
        rows = await self.db.fetch(
            """
            UPDATE knowledge_index
               SET status = 'archived', invalidated_at = now()
             WHERE project_id = $1
               AND path IS NULL
               AND status = 'active'
               AND note_id <> ALL($2::text[])
               AND indexed_at < now() - $3::interval
            RETURNING id
            """,
            project_id,
            list(tree_slugs),
            grace,
        )
        return len(rows)

    async def replace_note_links(
        self,
        source_note_row: uuid.UUID,
        kb_id: uuid.UUID,
        source_id: str,
        targets: List[str],
        rel_type: str = "references",
    ) -> int:
        """Replace a note's outbound link edges wholesale (slice-3 PR4c).

        The files-canonical successor to the Neo4j relationship edges: the
        reindexer extracts body markdown link targets and rewrites this note's
        edge set (delete all + re-insert, mirroring :meth:`replace_note_chunks`),
        so the 1-hop ``kb_related`` degrade has a link table to query when Neo4j
        is absent. Targets are stored as slugs and resolved to note rows at read
        time — a link to a not-yet-indexed note is kept, and dead links simply
        fall out of the read-time join. Repeated targets collapse to one edge.
        Returns the number of edges written.
        """
        await self.db.execute(
            "DELETE FROM knowledge_links WHERE source_note_row = $1",
            source_note_row,
        )
        source_id = str(source_id)[:NOTE_ID_MAX]
        seen: set = set()
        written = 0
        for target in targets:
            # Clamp BEFORE the dedupe so two targets differing only past the
            # bound collapse into the single edge they would both resolve to.
            #
            # Unclamped, a long `[[wikilink]]` raised "value too long for type
            # character varying(100)" here. Because links are written *before*
            # stamp_note_indexed, that left the note unstamped, so every
            # subsequent sweep retried it and failed identically — 17 notes in
            # the Better Resavio archive looped this way indefinitely, holding
            # the whole KB at `partial` while the other 2618 were fine.
            #
            # Truncating costs no reachability: get_related_notes resolves
            # target_id against knowledge_index.note_id, which ingest already
            # clamps to the same bound, so an over-long target could never have
            # matched a note. It gains some — a target written out in full now
            # matches the note whose id was clamped on the way in.
            target = str(target)[:NOTE_ID_MAX]
            if target in seen:
                continue
            seen.add(target)
            await self.db.execute(
                """
                INSERT INTO knowledge_links
                    (source_note_row, kb_id, source_id, target_id, rel_type)
                VALUES ($1, $2, $3, $4, $5)
                """,
                source_note_row,
                kb_id,
                source_id,
                target,
                rel_type,
            )
            written += 1
        return written

    async def get_related_notes(
        self,
        kb_id: uuid.UUID,
        note_id: str,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """1-hop link neighbours of a note (the kg-less ``kb_related`` backend).

        Neighbours are notes ``note_id`` links to (outbound) or that link to it
        (inbound), resolved against ``knowledge_index`` for title/type/status and
        filtered to ``status = 'active'`` — dead links (targets with no active
        note) drop out of the join. Returns the same shape as the Neo4j
        ``get_related`` (``id``/``title``/``type``/``status``/``distance``/
        ``rel_types``) so the tool formats both worlds identically; every edge is
        a generic body-markdown ``references`` link, hence ``distance = 1`` and a
        constant ``rel_types``.
        """
        rows = await self.db.fetch(
            """
            SELECT DISTINCT ki.note_id AS id, ki.title AS title,
                   ki.note_type AS type, ki.status AS status
            FROM (
                SELECT target_id AS neighbour FROM knowledge_links
                WHERE kb_id = $1 AND source_id = $2
                UNION
                SELECT source_id AS neighbour FROM knowledge_links
                WHERE kb_id = $1 AND target_id = $2
            ) nbr
            JOIN knowledge_index ki
              ON ki.kb_id = $1 AND ki.note_id = nbr.neighbour
            WHERE ki.status = 'active'
            ORDER BY ki.title
            LIMIT $3
            """,
            kb_id,
            note_id,
            limit,
        )
        return [
            {
                "id": r["id"],
                "title": r["title"],
                "type": r["type"],
                "status": r["status"],
                "distance": 1,
                "rel_types": ["references"],
            }
            for r in rows
        ]

    async def get_gardening_health(
        self, kb_id: uuid.UUID, *, grace: timedelta = timedelta(days=14)
    ) -> Dict[str, Any]:
        """Corpus-level hygiene counters (kb_gardening G10).

        The numbers that turn "the KB is getting worse" from a feeling into a
        graph: how much is retired but still on disk, how much of the active
        corpus nothing durable reaches, how old the oldest tombstone is. One
        query; cheap enough for the summary endpoint. Near-duplicate density
        stays in ``kb_lint`` (it is an O(n²) self-join).
        """
        row = await self.db.fetchrow(
            """
            WITH scoped AS (
                SELECT * FROM knowledge_index WHERE kb_id = $1 AND path IS NOT NULL
            ),
            active_src AS (
                SELECT DISTINCT kl.target_id
                FROM knowledge_links kl
                JOIN scoped src ON src.note_id = kl.source_id
                WHERE kl.kb_id = $1 AND src.status = 'active'
            )
            SELECT
                COUNT(*) FILTER (WHERE status = 'active') AS active,
                COUNT(*) FILTER (WHERE status IN ('archived', 'superseded')) AS retired,
                COUNT(*) FILTER (
                    WHERE status IN ('archived', 'superseded')
                      AND COALESCE(invalidated_at, modified_at, indexed_at)
                            < now() - $2::interval
                ) AS retired_past_grace,
                COUNT(*) FILTER (
                    WHERE status = 'active'
                      AND note_type IN ('learning', 'retrospective', 'state')
                      AND note_id NOT IN (SELECT target_id FROM active_src)
                ) AS nursery_orphans,
                COUNT(*) FILTER (
                    WHERE status = 'active'
                      AND note_type IN ('learning', 'retrospective', 'state')
                ) AS nursery_active,
                EXTRACT(EPOCH FROM (
                    now() - MIN(COALESCE(invalidated_at, modified_at, indexed_at))
                        FILTER (WHERE status IN ('archived', 'superseded'))
                )) / 86400.0 AS oldest_retired_days
            FROM scoped
            """,
            kb_id,
            grace,
        )
        r = dict(row) if row else {}
        oldest = r.get("oldest_retired_days")
        return {
            "active": int(r.get("active") or 0),
            "retired": int(r.get("retired") or 0),
            "retired_past_grace": int(r.get("retired_past_grace") or 0),
            "nursery_active": int(r.get("nursery_active") or 0),
            "nursery_orphans": int(r.get("nursery_orphans") or 0),
            "oldest_retired_days": round(float(oldest), 1)
            if oldest is not None
            else None,
            "grace_days": grace.days,
        }

    async def list_unreachable_nursery(
        self,
        kb_id: uuid.UUID,
        *,
        root_types: List[str],
        nursery_types: List[str],
        protected_tags: List[str],
        min_age: timedelta,
        limit: int = 25,
    ) -> List[Dict[str, Any]]:
        """Active nursery notes no active durable note reaches, even transitively.

        kb_gardening G7 rule **R4** — the GC mark-and-sweep: roots are the
        active notes of ``root_types`` (decision/goal/plan/charter/tickets/…);
        marking follows links out of *active* notes only; an active note of a
        ``nursery_type`` that is never marked is a candidate. E1b on the Better
        Resavio dump: 644 of 1,719 active history notes, zero anchor
        violations. Protected tags and a minimum age are applied here so the
        caller never even sees a note the guard would refuse. Oldest first,
        bounded.
        """
        rows = await self.db.fetch(
            """
            WITH RECURSIVE roots AS (
                SELECT note_id
                FROM knowledge_index
                WHERE kb_id = $1 AND path IS NOT NULL AND status = 'active'
                  AND note_type = ANY($2::text[])
            ),
            reach AS (
                SELECT note_id FROM roots
                UNION
                SELECT kl.target_id
                FROM knowledge_links kl
                JOIN reach r ON r.note_id = kl.source_id
                JOIN knowledge_index src
                  ON src.kb_id = kl.kb_id AND src.note_id = kl.source_id
                WHERE kl.kb_id = $1 AND src.status = 'active'
            )
            SELECT ki.note_id, ki.path, ki.blob_sha, ki.note_type, ki.title,
                   ki.created_at, ki.modified_at
            FROM knowledge_index ki
            WHERE ki.kb_id = $1
              AND ki.path IS NOT NULL
              AND ki.status = 'active'
              AND ki.note_type = ANY($3::text[])
              AND NOT (COALESCE(ki.tags, '{}'::text[]) && $4::text[])
              AND ki.ready_at IS NULL
              AND COALESCE(ki.created_at, ki.indexed_at) < now() - $5::interval
              AND NOT EXISTS (SELECT 1 FROM reach r WHERE r.note_id = ki.note_id)
            ORDER BY COALESCE(ki.modified_at, ki.created_at, ki.indexed_at) ASC NULLS FIRST,
                     ki.note_id
            LIMIT $6
            """,
            kb_id,
            [str(t) for t in root_types],
            [str(t) for t in nursery_types],
            [str(t) for t in protected_tags],
            min_age,
            limit,
        )
        return [dict(r) for r in rows]

    async def set_note_status(
        self,
        kb_id: uuid.UUID,
        note_id: str,
        status: str,
        *,
        invalidated: bool = False,
    ) -> bool:
        """Flip a row's status after the canonical file already says so.

        The file is the truth and the sweep would converge the row anyway;
        this makes the projection agree immediately (search and injection
        stop returning the note now, not in 15 minutes). ``invalidated``
        stamps ``invalidated_at`` so the purge lane's grace period starts at
        the retirement, not at the note's last edit. Returns True if a row
        changed.
        """
        result = await self.db.execute(
            """
            UPDATE knowledge_index
               SET status = $3,
                   invalidated_at = CASE WHEN $4 THEN now() ELSE invalidated_at END
             WHERE kb_id = $1 AND note_id = $2 AND status <> $3
            """,
            kb_id,
            note_id,
            status,
            invalidated,
        )
        return str(result).endswith(" 1")

    async def list_purge_candidates(
        self,
        kb_id: uuid.UUID,
        *,
        grace: timedelta,
        limit: int = 25,
        excluded_types: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Retired notes that have earned physical removal (kb_gardening G2).

        Three signals must agree: the row has been non-active for at least
        ``grace`` (measured from ``invalidated_at`` when the retirement
        stamped it, else the file's ``modified_at``, else ``indexed_at``); no
        *active* note links to it; and its type is not excluded. Oldest first,
        bounded by ``limit`` so one tick cannot purge a whole vault. Only
        path-backed rows qualify — a pathless legacy row has no file to remove
        and is the reconciler's business.
        """
        excluded = [str(t) for t in (excluded_types or [])]
        rows = await self.db.fetch(
            """
            SELECT ki.note_id, ki.path, ki.blob_sha, ki.status, ki.note_type,
                   COALESCE(ki.invalidated_at, ki.modified_at, ki.indexed_at) AS retired_at
            FROM knowledge_index ki
            WHERE ki.kb_id = $1
              AND ki.path IS NOT NULL
              AND ki.status IN ('archived', 'superseded')
              AND NOT (ki.note_type = ANY($2::text[]))
              AND COALESCE(ki.invalidated_at, ki.modified_at, ki.indexed_at)
                    < now() - $3::interval
              AND NOT EXISTS (
                    SELECT 1
                    FROM knowledge_links kl
                    JOIN knowledge_index src
                      ON src.kb_id = kl.kb_id AND src.note_id = kl.source_id
                    WHERE kl.kb_id = ki.kb_id
                      AND kl.target_id = ki.note_id
                      AND src.status = 'active'
              )
            ORDER BY retired_at ASC NULLS LAST, ki.note_id
            LIMIT $4
            """,
            kb_id,
            excluded,
            grace,
            limit,
        )
        return [dict(r) for r in rows]

    async def get_inbound_links(
        self,
        kb_id: uuid.UUID,
        note_id: str,
        *,
        active_only: bool = True,
        note_types: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Notes that link *to* ``note_id`` (the retirement guard's evidence).

        kb_gardening G5: a note an active decision/goal/plan/ticket links to
        is load-bearing and must not be retired by an agent. This is the
        one-hop inbound side of :meth:`get_related_notes`, with the filters
        the guard needs — ``active_only`` drops retired sources (a link from
        an archived note protects nothing), ``note_types`` narrows to the
        durable root types. Returns ``[{id, title, type, status}]``.
        """
        clauses = ["kl.kb_id = $1", "kl.target_id = $2"]
        params: List[Any] = [kb_id, note_id]
        if active_only:
            clauses.append("ki.status = 'active'")
        if note_types:
            params.append([str(t) for t in note_types])
            clauses.append(f"ki.note_type = ANY(${len(params)}::text[])")
        rows = await self.db.fetch(
            f"""
            SELECT DISTINCT ki.note_id AS id, ki.title AS title,
                   ki.note_type AS type, ki.status AS status
            FROM knowledge_links kl
            JOIN knowledge_index ki
              ON ki.kb_id = kl.kb_id AND ki.note_id = kl.source_id
            WHERE {" AND ".join(clauses)}
            ORDER BY ki.title
            LIMIT 50
            """,
            *params,
        )
        return [
            {
                "id": r["id"],
                "title": r["title"],
                "type": r["type"],
                "status": r["status"],
            }
            for r in rows
        ]

    async def get_note_by_slug(
        self, kb_id: uuid.UUID, note_id: str
    ) -> Optional[Dict[str, Any]]:
        """A full note read from the index (the kg-less ``kb_read`` backend).

        Returns the note in the same dict shape ``KnowledgeGraphDB.read_note``
        produces (``id``/``type`` rather than ``note_id``/``note_type``) so the
        tool formats both worlds identically. Status-agnostic — a direct read
        must surface superseded/archived notes too, unlike search. Neo4j-only
        fields (``relationships``) are absent; 1-hop links come from
        :meth:`get_related_notes`. Returns None if the slug isn't in the KB.

        Files-canonical (symmetry with :meth:`list_notes`): only rows a file
        backs are read (``path IS NOT NULL``) — a pathless ghost row from the
        DELETE dual-write gap must never resolve. Status stays unfiltered.
        """
        row = await self.db.fetchrow(
            """
            SELECT note_id, title, note_type, status, content, confidence,
                   tags, keywords, job_id, phase, created_at, modified_at,
                   priority, ready_at, blob_sha
            FROM knowledge_index
            WHERE kb_id = $1 AND note_id = $2 AND path IS NOT NULL
            LIMIT 1
            """,
            kb_id,
            note_id,
        )
        if row is None:
            return None
        r = dict(row)
        return {
            "id": r.get("note_id"),
            "title": r.get("title", ""),
            "type": r.get("note_type", ""),
            "status": r.get("status", ""),
            "content": r.get("content", ""),
            "confidence": r.get("confidence"),
            "tags": r.get("tags") or [],
            "keywords": r.get("keywords") or [],
            "job_id": r.get("job_id"),
            "phase": r.get("phase"),
            "created": r.get("created_at"),
            "modified": r.get("modified_at"),
            # The compare-and-swap token for rewrites/deletes (kb_gardening
            # G3): the git blob the row was indexed from. NULL while an inline
            # index is still deferred — callers then write unconditionally.
            "blob_sha": r.get("blob_sha"),
            # Backlog rank (project-backlog-pipeline task 3) — read back so
            # kb_update can preserve an existing ticket's priority when the
            # caller doesn't specify a new one.
            "priority": r.get("priority", 1),
            # Dispatch authorization (B2) — read back for the same reason:
            # every kb_update rewrites the canonical OKF file, and a file that
            # silently dropped ready_at would lose the ticket's authorization
            # on the next full vault rebuild.
            "ready_at": r.get("ready_at"),
        }

    async def note_is_indexed(self, kb_id: uuid.UUID, note_id: str) -> bool:
        """Whether this note's chunks are durable — i.e. it is searchable.

        Reads the stamp, not the row. ``embedding_version`` is written **last**,
        after the chunk rows land (``index_single_note``), so a non-NULL stamp
        is the index's own promise that the note can be found; a row with a NULL
        stamp is precisely the deferred state a write reports as
        ``indexed=deferred:<reason>``. Distinguishing the two is the whole point
        — "the note exists" and "the note is findable" are different questions,
        and only this one answers the second.

        Files-canonical like :meth:`get_note_by_slug`: a pathless ghost row from
        the DELETE dual-write gap is not an indexed note. Returns False for an
        unknown slug, so callers can treat "absent" and "unindexed" alike.
        """
        row = await self.db.fetchrow(
            """
            SELECT embedding_version IS NOT NULL AS indexed
            FROM knowledge_index
            WHERE kb_id = $1 AND note_id = $2 AND path IS NOT NULL
            LIMIT 1
            """,
            kb_id,
            note_id,
        )
        return bool(row and row["indexed"])

    async def get_charter_note(self, project_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """The project's ACTIVE charter note, if one exists (centurion.md §5).

        Dedicated fetch by (project_id, note_type='charter') — the charter is
        injected unconditionally into officer/conference turns and must never
        depend on winning a relevance ranking. Keyed on ``project_id`` with NO
        path filter, deliberately unlike :meth:`get_note_by_slug`: officer and
        conference sessions run the lite (gitless) tier, so their charter is a
        pathless pgvector row — the one durable write they have.

        Returns the ``id``/``type`` dict shape shared with the other readers,
        or None when the project has no active charter. Newest-modified wins
        if bad data ever yields more than one.
        """
        row = await self.db.fetchrow(
            """
            SELECT note_id, title, note_type, status, content,
                   job_id, created_at, modified_at
            FROM knowledge_index
            WHERE project_id = $1 AND note_type = 'charter' AND status = 'active'
            ORDER BY modified_at DESC NULLS LAST
            LIMIT 1
            """,
            project_id,
        )
        if row is None:
            return None
        r = dict(row)
        return {
            "id": r.get("note_id"),
            "title": r.get("title", ""),
            "type": r.get("note_type", ""),
            "status": r.get("status", ""),
            "content": r.get("content", ""),
            "job_id": r.get("job_id"),
            "created": r.get("created_at"),
            "modified": r.get("modified_at"),
        }

    async def list_notes(
        self,
        kb_id: uuid.UUID,
        note_type: Optional[str] = None,
        tag: Optional[str] = None,
        status: Optional[str] = None,
        job_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """List note summaries in a KB (the kg-less ``kb_list`` backend).

        Same filter surface and summary-dict shape as
        ``KnowledgeGraphDB.list_notes`` (``id``/``type``); ``tag`` matches against
        the ``tags`` array column. No status filter is applied unless requested,
        so an unfiltered list spans every status like the Neo4j version.

        Files-canonical: only rows a file backs are listed (``path IS NOT NULL``).
        Pathless ghost rows from the DELETE dual-write gap are excluded — a
        durable guard independent of any one-off ghost cleanup.
        """
        conditions = ["kb_id = $1", "path IS NOT NULL"]
        params: List[Any] = [kb_id]
        if note_type:
            params.append(note_type)
            conditions.append(f"note_type = ${len(params)}")
        if status:
            params.append(status)
            conditions.append(f"status = ${len(params)}")
        if job_id:
            params.append(job_id)
            conditions.append(f"job_id = ${len(params)}")
        if tag:
            params.append(tag)
            conditions.append(f"${len(params)} = ANY(tags)")
        params.append(limit)
        where = " AND ".join(conditions)
        rows = await self.db.fetch(
            f"""
            SELECT note_id, title, note_type, status, confidence, priority
            FROM knowledge_index
            WHERE {where}
            ORDER BY modified_at DESC NULLS LAST
            LIMIT ${len(params)}
            """,
            *params,
        )
        return [
            {
                "id": r["note_id"],
                "title": r["title"],
                "type": r["note_type"],
                "status": r["status"],
                "confidence": r["confidence"],
                # dict(r) rather than r["priority"]: tolerates fake rows in
                # existing tests that predate this column (mock dicts without
                # a "priority" key), same as a real row's column default.
                "priority": dict(r).get("priority", 1),
            }
            for r in rows
        ]

    async def list_notes_full(
        self,
        kb_id: uuid.UUID,
        limit: int = 5000,
    ) -> List[Dict[str, Any]]:
        """Every row of a KB, bodies included — the gardener (kb_lint/kb_index) backend.

        Deliberately NOT ``list_notes`` with a bigger limit. Two differences,
        both load-bearing for a maintenance surface
        (knowledge-base/knowledge/features/knowledge_base_repo_separation.md §5a):

        1. **No ``path IS NOT NULL`` gate.** The read path (``kb_read``/
           ``kb_list``) is files-canonical and hides pathless rows on purpose.
           A linter must see exactly what the read path cannot: a row the agent
           wrote that no file backs is not "absent", it is a *defect* — invisible
           to every reader and a candidate for :meth:`reconcile_orphans`. Gating
           the linter the same way would let a KB whose materialisation is broken
           lint "clean" while being unreadable, which is the silent-empty failure
           this surface exists to catch. Callers report the pathless ones.
        2. **Ghost-inclusive scoping.** ``upsert_note`` (the agent write-through)
           sets ``project_id`` but never ``kb_id``/``path`` — those arrive when
           the reindexer adopts the row. Keying on ``kb_id`` alone would miss
           every just-written note, so un-adopted rows are matched through
           ``project_id`` as well (same reasoning as :meth:`reconcile_orphans`
           keying on ``project_id``). Safe for external KBs: their rows always
           carry a ``kb_id``, so they can only match the first branch.

        Returns the ``id``/``type`` dict shape the other readers use, plus
        ``path`` (None when unmaterialised) and ``superseded_by`` (which the
        lint supersede-chain rule needs). ``limit`` is a guard against pulling
        an unbounded number of note bodies into agent memory; callers must
        report truncation loudly rather than silently linting a prefix.
        """
        rows = await self.db.fetch(
            """
            SELECT note_id, path, title, note_type, status, confidence,
                   priority, tags, keywords, job_id, phase, content,
                   superseded_by, created_at, modified_at
            FROM knowledge_index
            WHERE (kb_id = $1 OR (kb_id IS NULL AND project_id = $1))
            ORDER BY note_id
            LIMIT $2
            """,
            kb_id,
            limit,
        )
        return [
            {
                "id": r["note_id"],
                "path": r["path"],
                "title": r["title"],
                "type": r["note_type"],
                "status": r["status"],
                "content": r["content"],
                "confidence": r["confidence"],
                # dict(r).get: tolerates fake rows in tests that predate a
                # column, same convention as list_notes' priority read.
                "priority": dict(r).get("priority", 1),
                "tags": r["tags"] or [],
                "keywords": r["keywords"] or [],
                "job_id": r["job_id"],
                "phase": r["phase"],
                "superseded_by": dict(r).get("superseded_by"),
                "created": r["created_at"],
                "modified": r["modified_at"],
            }
            for r in rows
        ]

    # =========================================================================
    # Lexical enumeration channel — grep + tag vocabulary (spec WP7, D9/D11)
    # =========================================================================

    async def grep_notes(
        self,
        kb_ids: List[uuid.UUID],
        pattern: str,
        *,
        regex: bool = False,
        max_matches: int = 50,
        context: int = 1,
        statement_timeout_ms: int = 5000,
        include_titles: bool = True,
    ) -> Tuple[List[GrepMatch], int]:
        """Deterministic literal/regex enumeration over note bodies (D9).

        The only lexical channel agents have into the knowledge base besides
        stemmed English full-text search, which cannot match an identifier
        like ``sales_page_2026_09`` or a commit sha. One indexed SQL predicate
        (S1's ``idx_knowledge_content_trgm`` trigram GIN index for the ILIKE
        path; a plain ``~*`` regex match otherwise) selects the *candidate*
        notes so Postgres never returns more bodies than actually match; line
        extraction — including the ``before``/``after`` context window — then
        happens here in Python, against exactly those bodies. The candidate
        fetch and its exact count run as two separate queries
        (:func:`_grep_candidates_sql` / :func:`_grep_count_sql`, fix round 1
        finding 4) so a broad pattern can't pull every matching body (up to
        ~250KB each) across the wire before ``max_matches`` applies — the
        fetch is itself ``LIMIT``-ed to ``max_matches`` candidate notes (a
        note contributes at least one line, so no more than that can ever
        render), while the count query still reports the true total.

        ``regex=False`` (the default) is a literal substring search: the
        pattern is escaped for ``ILIKE`` (backslash, ``%``, ``_``) so a
        pattern containing those characters matches itself, not a wildcard.
        ``regex=True`` passes the pattern straight to Postgres's ``~*``
        (case-insensitive POSIX regex, newline-sensitive via an ``(?n)``
        prefix — see :func:`_grep_where_clause`); a malformed pattern raises
        from Postgres and is left to propagate — the tool layer renders the
        error, this method does not swallow it.

        ``include_titles=True`` (the default) unions the ``content`` and
        ``title`` branches rather than OR-ing one predicate, so the trigram
        index still applies to the ``content`` branch (fix round 1, finding
        1 — see :func:`_grep_candidates_sql`).

        Only active, materialised (``path IS NOT NULL``) notes are candidates,
        matching the read path's visibility rule. Candidates are ordered by
        ``(kb_id, note_id)`` for determinism; ``max_matches`` caps the number of
        *matched lines* returned across all notes (not notes themselves), and
        the second return value is always the exact number of candidate notes
        — even when the capped fetch truncates before every candidate note is
        scanned, so a caller can tell "there were more notes" from "there
        were more lines in the notes we saw".
        """
        pattern = (pattern or "").strip()
        if not pattern:
            raise ValueError("pattern must not be empty")
        if len(pattern) > _GREP_PATTERN_MAX:
            raise ValueError(f"pattern longer than {_GREP_PATTERN_MAX} characters")
        if max_matches < 1:
            raise ValueError("max_matches must be at least 1")
        if not kb_ids:
            return [], 0

        arg = pattern if regex else _escape_like(pattern)
        candidates_sql = _grep_candidates_sql(
            regex=regex, include_titles=include_titles
        )
        count_sql = _grep_count_sql(regex=regex, include_titles=include_titles)

        async with self.db.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"SET LOCAL statement_timeout = {int(statement_timeout_ms)}"
                )
                rows = await conn.fetch(candidates_sql, list(kb_ids), arg, max_matches)
                total = await conn.fetchval(count_sql, list(kb_ids), arg)

        rx = re.compile(pattern if regex else re.escape(pattern), re.IGNORECASE)
        matches: List[GrepMatch] = []
        for row in rows:
            lines = (row["content"] or "").splitlines()
            for ix, line in enumerate(lines):
                if not rx.search(line):
                    continue
                matches.append(
                    GrepMatch(
                        kb_id=row["kb_id"],
                        note_id=row["note_id"],
                        title=row["title"] or "",
                        line_no=ix + 1,
                        line=line,
                        before=lines[max(0, ix - context) : ix],
                        after=lines[ix + 1 : ix + 1 + context],
                    )
                )
                if len(matches) >= max_matches:
                    return matches, total
        return matches, total

    async def tag_vocabulary(
        self, kb_id: uuid.UUID, limit: int = 20
    ) -> List[Tuple[str, int]]:
        """Distinct tags in a KB with usage counts, most-used first (D11).

        Backs ``kb_list``'s tag-vocabulary hint. Same visibility rule as
        :meth:`grep_notes` (active + materialised notes only); ties break
        alphabetically so the result is stable across calls.
        """
        rows = await self.db.fetch(
            """
            SELECT tag, COUNT(*) AS n
            FROM knowledge_index, LATERAL unnest(tags) AS tag
            WHERE kb_id = $1 AND status = 'active' AND path IS NOT NULL
            GROUP BY tag
            ORDER BY n DESC, tag ASC
            LIMIT $2
            """,
            kb_id,
            limit,
        )
        return [(r["tag"], int(r["n"])) for r in rows]

    # =========================================================================
    # TTL lifecycle — KB convergence
    # (knowledge-base/knowledge/features/kb_convergence_ttl_reverification.md)
    # =========================================================================

    async def decrement_ttl(self, project_id: uuid.UUID) -> int:
        """Decrement the cycle TTL of every active, TTL-bearing note in a project.

        Called once per loop cycle (at the developer→scholar wrap). Durable notes
        (``remaining_cycles IS NULL``) are untouched; only moving-target notes
        count down. A note reaching ``<= 0`` enters the stale queue
        (:meth:`get_stale_notes`) for re-verification. Returns rows decremented.

        NOTE: the orchestrator runs the equivalent UPDATE inline against the
        vector DB at the cycle boundary (it can't safely import this module). This
        method is the canonical agent-side / test form — keep the two in sync.
        """
        rows = await self.db.fetch(
            """
            UPDATE knowledge_index
            SET remaining_cycles = remaining_cycles - 1
            WHERE project_id = $1
              AND remaining_cycles IS NOT NULL
              AND status = 'active'
            RETURNING id
            """,
            project_id,
        )
        return len(rows)

    async def get_stale_notes(
        self, project_id: uuid.UUID, limit: int = 50
    ) -> List[KnowledgeRecord]:
        """Active notes whose TTL has run out (``remaining_cycles <= 0``).

        The stale queue the knowledge-assembler re-verifies. Oldest-modified
        first, so the most-likely-stale are handled first under a limit.
        """
        rows = await self.db.fetch(
            """
            SELECT * FROM knowledge_index
            WHERE project_id = $1
              AND remaining_cycles <= 0
              AND status = 'active'
            ORDER BY modified_at ASC NULLS FIRST
            LIMIT $2
            """,
            project_id,
            limit,
        )
        return [KnowledgeRecord.from_row(dict(r)) for r in rows]

    async def refresh_ttl(
        self,
        project_id: uuid.UUID,
        notes: List[KnowledgeRecord],
        current_cycle: Optional[int] = None,
    ) -> int:
        """Reset the TTL of notes that survived re-verification.

        Called by the assembler runner AFTER the agent pass. Only notes still
        ``active`` are refreshed (the WHERE clause skips any the agent superseded
        or archived), each back to its note_type's initial TTL. Durable notes are
        ignored. Returns the number of notes refreshed.
        """
        refreshed = 0
        for note in notes:
            ttl = self._ttl_for_note_type(note.note_type)
            if ttl is None:
                continue
            rows = await self.db.fetch(
                """
                UPDATE knowledge_index
                SET remaining_cycles = $1,
                    last_verified_cycle = $2
                WHERE project_id = $3 AND note_id = $4 AND status = 'active'
                RETURNING id
                """,
                ttl,
                current_cycle,
                project_id,
                note.note_id,
            )
            refreshed += len(rows)
        return refreshed

    # =========================================================================
    # Search
    # =========================================================================

    async def hybrid_search(
        self,
        project_id: Optional[uuid.UUID] = None,
        query: str = "",
        match_count: int = 10,
        dense_weight: float = 0.6,
        sparse_weight: float = 0.3,
        recency_weight: float = 0.1,
        project_ids: Optional[List[uuid.UUID]] = None,
    ) -> List[KnowledgeRecord]:
        """Execute hybrid search (RRF over dense + sparse + recency).

        Args:
            project_id: Single project UUID (backward compat)
            query: Search query text
            match_count: Max results
            dense_weight: Weight for vector similarity
            sparse_weight: Weight for keyword match
            recency_weight: Weight for recency
            project_ids: List of project UUIDs for multi-project search

        Returns:
            List of KnowledgeRecord objects ranked by RRF score
        """
        # Normalize: project_ids takes priority, fall back to project_id
        effective_ids = project_ids or ([project_id] if project_id else [])
        if not effective_ids:
            return []

        query_embedding = await self.embedding_service.embed(query)

        if len(effective_ids) > 1:
            func_name = "knowledge_multi_project_hybrid_search"
            scope_val = effective_ids
        else:
            func_name = "knowledge_hybrid_search"
            scope_val = effective_ids[0]

        rows = await self.db.fetch(
            f"""
            SELECT * FROM {func_name}(
                $1, $2, $3, $4, $5, $6, $7
            )
            """,
            query,
            query_embedding,
            scope_val,
            match_count,
            dense_weight,
            sparse_weight,
            recency_weight,
        )

        return [KnowledgeRecord.from_row(dict(row)) for row in rows]

    async def find_similar_many(
        self,
        project_id: uuid.UUID,
        embedding: Union[List[float], str],
        k: int = 5,
        min_similarity: float = 0.6,
    ) -> List[KnowledgeRecord]:
        """Return the top-``k`` active neighbours above ``min_similarity``.

        The KB analog of ``RecallStore.find_similar_many`` — the neighbour fetch
        that feeds the ingestion verdict (slice 2 PR2). Only ``status = 'active'``
        rows are candidates: a verdict never compares a new note against an
        already superseded/archived one. Each returned record carries a transient
        ``.similarity`` (cosine, closest first) for the verdict prompt; an empty
        list is the cost-guard signal (no neighbour above the floor → straight
        ADD, no LLM call).
        """
        embedding_vec = self._prepare_embedding(embedding)
        rows = await self.db.fetch(
            """
            SELECT *, 1 - (embedding <=> $1) AS similarity
            FROM knowledge_index
            WHERE project_id = $2
              AND embedding IS NOT NULL
              AND status = 'active'
              AND 1 - (embedding <=> $1) >= $3
            ORDER BY similarity DESC
            LIMIT $4
            """,
            embedding_vec,
            project_id,
            min_similarity,
            k,
        )
        records: List[KnowledgeRecord] = []
        for row in rows:
            row_dict = dict(row)
            rec = KnowledgeRecord.from_row(row_dict)
            rec.similarity = row_dict.get("similarity")
            records.append(rec)
        return records

    async def find_near_duplicate_pairs(
        self,
        project_id: uuid.UUID,
        min_similarity: float = 0.9,
        limit: int = 50,
    ) -> List[Tuple[str, str, float]]:
        """All active note pairs whose embeddings are near-duplicates.

        One self-join over the index (each unordered pair once, via
        ``a.note_id < b.note_id``), feeding ``kb_lint``'s near-duplicate rule —
        detection for the bare-writer twins the verdict gate never adjudicates.
        Most-similar pairs first. The 0.9 default floor is deliberately far
        above the verdict fetch floor (0.6): lint flags only what a gardener
        should merge, not everything topically related. Pairs are constrained to
        a matching ``embedding_version`` — cosine distance across embedding
        models is meaningless, so mixed-version rows are never compared.
        """
        rows = await self.db.fetch(
            """
            SELECT a.note_id AS note_a, b.note_id AS note_b,
                   1 - (a.embedding <=> b.embedding) AS similarity
            FROM knowledge_index a
            JOIN knowledge_index b
              ON b.project_id = a.project_id
             AND a.note_id < b.note_id
            WHERE a.project_id = $1
              AND a.status = 'active'
              AND b.status = 'active'
              AND a.embedding IS NOT NULL
              AND b.embedding IS NOT NULL
              AND a.embedding_version = b.embedding_version
              AND 1 - (a.embedding <=> b.embedding) >= $2
            ORDER BY similarity DESC
            LIMIT $3
            """,
            project_id,
            min_similarity,
            limit,
        )
        return [
            (str(r["note_a"]), str(r["note_b"]), float(r["similarity"])) for r in rows
        ]

    async def search_chunks(
        self,
        kb_ids: List[uuid.UUID],
        query: str = "",
        *,
        exact: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        exact_weight: float = 0.6,
        tag_weight: float = 0.2,
        embedding_version: Optional[str] = None,
        match_count: int = 15,
        over_fetch: int = 50,
        dense_weight: float = 0.6,
        sparse_weight: float = 0.3,
        recency_weight: float = 0.1,
        rrf_k: int = 60,
    ) -> List[KnowledgeRecord]:
        """Hybrid retrieval over the chunk index (slice-3 PR4, §5.1).

        The chunk-granular successor to :meth:`hybrid_search`. After the PR3
        reindexer the dense vector lives on ``knowledge_chunks`` (the note row's
        ``embedding`` is NULL for reindexed notes), so fusion runs over chunk
        rows — dense (chunk embedding) + sparse (chunk ``search_doc``) + recency
        (note ``modified_at``) — and the best-scoring chunk collapses back to its
        note. Returns note-level ``KnowledgeRecord``s so the ``kb_search`` tool
        signature is unchanged; the ``status = 'active'`` filter is preserved
        exactly (stricter than "not superseded").

        The SQL function over-fetches ``over_fetch`` fused note candidates; this
        method then runs the reranker slot (a no-op passthrough in v1 — a hosted
        or local reranker wires in here as a precision mode) and truncates to
        ``match_count``. ``embedding_version`` filters mixed-model vectors out so
        a model migration can't silently drift the index; ``None`` disables the
        filter (single-model deployments). An empty ``kb_ids`` is the cost guard.

        ``exact`` (literal substrings, trigram-ranked) and ``tags`` (a boost, not
        a filter) add two more arms — spec WP7. They are what selects the
        function: with neither supplied this method runs the *existing*
        ``knowledge_chunk_hybrid_search`` call unchanged, so no current caller's
        ranking moves (H6); supply either and it runs
        ``knowledge_chunk_multi_angle_search`` (migration 0025) instead, which
        returns per-note attribution the results carry back in
        ``KnowledgeRecord.matched_arms``.

        If query embedding fails or the service is absent, use that same
        multi-angle function with no vector or embedding-version filter. Add
        the query as a literal substring arm so materialised notes without
        chunks remain searchable, and omit recency-only hits. Healthy searches
        retain their existing ranking. The returned list carries a ``notice``
        on fallback, including when no notes match.
        """
        if not kb_ids:
            return []

        # Normalise the filter arms first — they decide which function runs.
        # `exact` terms are LIKE-escaped because the SQL function builds its
        # ILIKE pattern by plain concatenation and does not escape (0025's
        # header), so an unescaped `%`/`_` inside an identifier would silently
        # act as a wildcard. The escaped term is also what similarity() scores,
        # so a term containing `%`, `_` or `\` gets a slightly perturbed trigram
        # score; identifiers rarely contain those, and a wrong match SET is the
        # worse failure. Tags are matched with `&&` (array containment), which
        # has no pattern syntax to escape.
        exact_terms = [
            _escape_like(t.strip()) for t in (exact or []) if t and t.strip()
        ]
        tag_terms = [t.strip() for t in (tags or []) if t and t.strip()]

        query_embedding = None
        lexical_fallback = False
        if query.strip():
            if self.embedding_service is None:
                lexical_fallback = True
                logger.warning("KB embeddings unavailable; using lexical fallback")
            else:
                try:
                    query_embedding = await self.embedding_service.embed(query)
                except Exception as exc:
                    # Only embedding failures degrade. Database errors still
                    # propagate, and task cancellation must not start more work.
                    lexical_fallback = True
                    logger.warning(
                        "KB query embedding failed (%s); using lexical fallback",
                        type(exc).__name__,
                    )
        elif not exact_terms and not tag_terms:
            return []

        if not exact_terms and not tag_terms and not lexical_fallback:
            # H6: preserve the healthy plain query's SQL, parameters, ranking,
            # and return shape. Only an embedding failure takes the new path.

            rows = await self.db.fetch(
                """
                SELECT * FROM knowledge_chunk_hybrid_search(
                    $1, $2, $3, $4, $5, $6, $7, $8, $9
                )
                """,
                query,
                query_embedding,
                list(kb_ids),
                embedding_version,
                over_fetch,
                dense_weight,
                sparse_weight,
                recency_weight,
                rrf_k,
            )

            records = [KnowledgeRecord.from_row(dict(row)) for row in rows]
            return self._rerank_chunks(records)[:match_count]

        if lexical_fallback:
            # Text remains usable across model/profile changes. This filter
            # also gates sparse chunks, so retaining it would hide old content
            # precisely when a model is missing or misconfigured.
            embedding_version = None
            literal_query = _escape_like(query.strip())
            if literal_query not in exact_terms:
                exact_terms.append(literal_query)

        ranked = await self.db.fetch(
            """
            SELECT * FROM knowledge_chunk_multi_angle_search(
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
            )
            """,
            query,
            query_embedding,
            list(kb_ids),
            embedding_version,
            over_fetch,
            dense_weight,
            sparse_weight,
            recency_weight,
            exact_terms,
            exact_weight,
            tag_terms,
            tag_weight,
            rrf_k,
        )
        if lexical_fallback:
            # Recency may boost a text/tag match, but freshness alone is not a
            # lexical match. Do not report unrelated notes as fallback hits.
            ranked = [
                row
                for row in ranked
                if {"sparse", "exact", "tag"}.intersection(row["arms"] or [])
            ]
        if not ranked:
            return KnowledgeSearchResults(lexical_fallback=lexical_fallback)

        # The function returns (note_row, rrf_score, arms) already ordered by
        # score; the note bodies come from a second query whose row order is
        # arbitrary, so re-impose the function's order here. `exact` matches
        # note rows directly, so a note with no current-version chunks can rank
        # — and a row can vanish between the two queries: skip what's missing
        # rather than assuming the second query returns every id.
        #
        # The body fetch re-states the caller's scope (final review, M4):
        # `status = 'active'` and `kb_id = ANY($2)`. The ranking function
        # already applies both, but the two queries do not run in one snapshot
        # — a note archived (or re-homed) in between would otherwise be
        # returned to the caller as a live result on the strength of a stale
        # ranking row. Re-stating the predicate makes the second query
        # fail-closed instead of trusting the first.
        order = [r["note_row"] for r in ranked]
        arms = {r["note_row"]: list(r["arms"] or []) for r in ranked}
        rows = await self.db.fetch(
            "SELECT * FROM knowledge_index "
            "WHERE id = ANY($1) AND status = 'active' AND kb_id = ANY($2)",
            order,
            list(kb_ids),
        )
        by_id = {row["id"]: KnowledgeRecord.from_row(dict(row)) for row in rows}

        records = []
        for note_row in order:
            record = by_id.get(note_row)
            if record is None:
                continue
            record.matched_arms = arms.get(note_row, [])
            records.append(record)

        return KnowledgeSearchResults(
            self._rerank_chunks(records)[:match_count],
            lexical_fallback=lexical_fallback,
        )

    @staticmethod
    def _rerank_chunks(records: List[KnowledgeRecord]) -> List[KnowledgeRecord]:
        """Reranker slot between RRF fusion and return (§5.1).

        v1 is a no-op passthrough that preserves the fusion order — the single
        biggest post-hybrid quality lever, but agents can grep-and-read after
        search, so a hosted/local cross-encoder reranker is a later ``rerank:
        true`` precision mode that slots in here without touching callers.
        """
        return records

    async def get_summary(
        self,
        project_id: Optional[uuid.UUID] = None,
        project_ids: Optional[List[uuid.UUID]] = None,
    ) -> Dict[str, Any]:
        """Get knowledge base summary for context injection.

        Args:
            project_id: Single project UUID (backward compat)
            project_ids: List of project UUIDs for multi-project summary

        Returns:
            Dict with note counts by type, total count, recent notes
        """
        effective_ids = project_ids or ([project_id] if project_id else [])
        if not effective_ids:
            return {"total": 0}

        if len(effective_ids) > 1:
            scope_clause = "project_id = ANY($1)"
            scope_val = effective_ids
        else:
            scope_clause = "project_id = $1"
            scope_val = effective_ids[0]

        row = await self.db.fetchrow(
            f"""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status = 'active') AS active,
                COUNT(*) FILTER (WHERE note_type = 'decision') AS decisions,
                COUNT(*) FILTER (WHERE note_type = 'learning') AS learnings,
                COUNT(*) FILTER (WHERE note_type = 'question' AND status = 'active') AS open_questions,
                COUNT(*) FILTER (WHERE note_type = 'goal') AS goals,
                COUNT(*) FILTER (WHERE note_type = 'code') AS code_notes,
                COUNT(*) FILTER (WHERE note_type = 'state') AS state_notes,
                MAX(modified_at) AS last_modified
            FROM knowledge_index
            WHERE {scope_clause}
            """,
            scope_val,
        )

        summary = dict(row) if row else {"total": 0}

        # Get 5 most recent notes
        recent = await self.db.fetch(
            f"""
            SELECT note_id, title, note_type, status, modified_at
            FROM knowledge_index
            WHERE {scope_clause} AND status = 'active'
            ORDER BY modified_at DESC
            LIMIT 5
            """,
            scope_val,
        )
        summary["recent_notes"] = [dict(r) for r in recent]

        return summary

    # =========================================================================
    # Rebuild (Recovery)
    # =========================================================================

    async def rebuild_from_notes(
        self,
        project_id: uuid.UUID,
        notes: List[Dict[str, Any]],
    ) -> int:
        """Rebuild the search index from Neo4j note data.

        Used for cold start or recovery when pgvector drifts from Neo4j.

        Args:
            project_id: Project UUID
            notes: List of note dicts from KnowledgeGraphDB.get_all_notes_for_export()

        Returns:
            Number of notes indexed
        """
        # Clear existing index for this project
        await self.db.execute(
            "DELETE FROM knowledge_index WHERE project_id = $1",
            project_id,
        )

        count = 0
        for note in notes:
            try:
                # Convert Neo4j DateTime to Python datetime if needed
                created = note.get("created")
                modified = note.get("modified")
                if hasattr(created, "to_native"):
                    created = created.to_native()
                if hasattr(modified, "to_native"):
                    modified = modified.to_native()

                await self.upsert_note(
                    note_id=note.get("id", ""),
                    project_id=project_id,
                    title=note.get("title", ""),
                    note_type=note.get("type", "learning"),
                    content=note.get("content", ""),
                    status=note.get("status", "active"),
                    confidence=note.get("confidence"),
                    tags=note.get("tags", []),
                    keywords=note.get("keywords", []),
                    job_id=uuid.UUID(note["job_id"]) if note.get("job_id") else None,
                    phase=note.get("phase"),
                    retrieval_messages=note.get("retrieval_messages", []),
                    created_at=created,
                    modified_at=modified,
                )
                count += 1
            except Exception as e:
                logger.warning(f"Failed to index note {note.get('id')}: {e}")

        logger.info(f"Rebuilt knowledge index for project {project_id}: {count} notes")
        return count

    # =========================================================================
    # Formatting
    # =========================================================================

    @staticmethod
    def format_note(
        note: KnowledgeRecord,
        index: int,
        source_alias: Optional[str] = None,
        updated: bool = False,
    ) -> str:
        """Format a single note for context injection.

        Args:
            note: KnowledgeRecord to format
            index: Display index (1-based)
            source_alias: Optional qualified KB alias for multi-KB injection
            updated: Mark the note as a newer version of one already in the
                history (append-only injection, D5)

        Returns:
            Formatted note string
        """
        meta_parts = [note.note_type]
        if note.confidence:
            meta_parts.append(f"{note.confidence} confidence")
        if note.phase is not None:
            meta_parts.append(f"phase {note.phase}")

        meta = ", ".join(meta_parts)
        links = ""
        if note.tags:
            links = " Tags: " + ", ".join(note.tags)
        arms = getattr(note, "matched_arms", None)
        if isinstance(arms, list) and arms:
            links += f" ⟨{'+'.join(arms)}⟩"
        if updated:
            links += f" {UPDATED_ITEM_MARKER}"

        # Truncate content for injection
        content = note.content
        if len(content) > 500:
            content = content[:497] + "..."

        if source_alias:
            handle = f"{source_alias}:{note.note_id}"
            title = note.title or note.note_id
            return (
                f"[{index}] [{source_alias}] {handle} — {title} "
                f"({meta}){links}\n{content}"
            )
        return f"[{index}] ({meta}){links}\n{content}"

    @classmethod
    def assemble_knowledge_block(
        cls,
        notes: List[KnowledgeRecord],
        model: Optional[str] = None,
        bindings: Optional[List[Any]] = None,
        external_watermarks: Optional[Dict[str, Optional[str]]] = None,
        updated_keys: Optional[Iterable[str]] = None,
    ) -> str:
        """Assemble formatted knowledge block for context injection.

        Args:
            notes: List of KnowledgeRecord objects
            model: Model id used to resolve family-specific block headers
                / footer. Falls through to the default family if None.
            bindings: Optional authorized KB bindings used to source-label notes.
            external_watermarks: External alias → indexed commit used by retrieval.
            updated_keys: Presence keys (``knowledge_item_key``) of notes that
                replace an earlier version already in the history; they get
                the "(updated; ...)" marker (append-only injection, D5).

        Returns:
            Formatted knowledge block string
        """
        if not notes:
            return ""

        from shared.runtime.services.guardrails import format_nudge

        binding_by_id = {str(binding.kb_id): binding for binding in (bindings or [])}
        updated = set(updated_keys or ())
        lines = [format_nudge("knowledge_block_header", model=model), ""]
        for i, note in enumerate(notes, 1):
            kb_id = getattr(note, "kb_id", None) or getattr(note, "project_id", None)
            binding = binding_by_id.get(str(kb_id))
            source_alias = binding.alias if binding is not None else None
            lines.append(
                cls.format_note(
                    note,
                    i,
                    source_alias=source_alias,
                    updated=knowledge_item_key(note) in updated,
                )
            )
            lines.append("")

        tokens_est = sum(len(n.content) // 4 for n in notes)
        footer = format_nudge(
            "knowledge_block_footer",
            model=model,
            count=len(notes),
            tokens=f"{tokens_est:,}",
        )
        if external_watermarks:
            snapshots = ", ".join(
                f"[{alias}] {(commit or 'unavailable')}"
                for alias, commit in external_watermarks.items()
            )
            footer += f"\nExternal snapshots (as of): {snapshots}"
        lines.append(footer)
        return "\n".join(lines)


__all__ = ["KnowledgeStore", "KnowledgeRecord", "KnowledgeSearchResults"]
