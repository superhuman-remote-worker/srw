"""OKF KB tree-diff reindexer — files → chunk-granular pgvector index (slice 3 PR3).

The composition layer over slice-3 PR1 (schema + store surface) and PR2 (chunker
+ embed pipeline): read the KB's git tree at HEAD, diff it against what the index
already holds (per-row ``blob_sha``), parse the changed notes with the gardener's
``parse_note_md``, chunk + embed them, persist through ``KnowledgeStore``, and
advance the per-KB watermark — only at the end, and only on a clean run, so an
interrupted or failed reindex self-heals on the next pass (§5: "git is the
Merkle tree").

Design: knowledge-base/knowledge/features/okf_knowledge_base.md §5 / §5.1 / §11 slice-3 PR3.

Dependencies are injected (gitea client, KnowledgeStore, EmbeddingService) so the
flow is unit-testable and trigger sites (post-merge, job-start, leader-gated
sweeper, the MCP operator hatch) stay thin.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import posixpath
import re
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from shared.runtime.services.forge import parse_owner_repo
from shared.runtime.services.knowledge_store import WEDGE_STREAK_THRESHOLD
from shared.backlog_tags import normalize_tags

from shared.runtime.knowledge.chunker import (
    chunk_note,
    embed_note_chunks,
    embedding_version_for_service,
    note_centroid,
)

# _internal_link_targets is the same body-markdown link parser the dead-link lint
# rule uses (external URLs / anchors / images excluded, `.md` basename returned) —
# reused here so the link table and the linter agree on what a "link" is.
from shared.runtime.knowledge.gardener import (
    _internal_link_targets,
    frontmatter_link_targets,
    parse_note_md,
)

from orchestrator.services.kb_forge import kb_client_for_repo

logger = logging.getLogger(__name__)

# Sweeper cadence. Coarse by design: the post-merge trigger covers the loop's
# hot write path instantly; the sweep only catches out-of-band edits (human
# pushes, recovered partial reindexes), and the up-to-date short-circuit makes
# a no-op check one HEAD fetch + one watermark row per KB.
SWEEP_TICK_SECONDS = int(os.getenv("KB_REINDEX_SWEEP_SECONDS", "900"))

# Delay before the sweeper's FIRST pass, which doubles as crash recovery: an
# in-process index dies with its orchestrator, and nothing else restarts it.
# Short enough that a rollout costs seconds rather than a full tick, long enough
# that startup (leader election, embedding-catalog resolution) has settled.
FIRST_SWEEP_DELAY_SECONDS = int(os.getenv("KB_REINDEX_FIRST_SWEEP_SECONDS", "45"))

# How often (in successfully stamped notes) to persist a progress counter during
# a run. Coarse on purpose: bounds the extra watermark writes on a large first
# index while keeping the Cockpit progress bar visibly moving.
_PROGRESS_BUMP_EVERY = 25

# Batch size at which a snapshot's blob reads switch from per-file REST calls
# to one repo-archive download (sources that support prefetch — native Gitea).
# Break-even: an archive costs a few seconds of Gitea-side generation; a REST
# read costs ~130ms. Below the threshold the per-file path is cheaper.
_ARCHIVE_PREFETCH_THRESHOLD = int(os.getenv("KB_REINDEX_ARCHIVE_THRESHOLD", "25"))

# WEDGE_STREAK_THRESHOLD (consecutive `partial` sweeps carrying the same error
# fingerprint before the sweep logs a one-shot WEDGED warning, spec WP3/H3) is
# imported from KnowledgeStore above — that's what actually computes the
# streak in SQL; this module only decides when to log against it. A local
# copy would drift silently if the store's bound ever changed.

# The vault root within a project's KB repo (slice 1 dual-write target). Same
# path whichever repo resolve_kb_repo lands on.
KNOWLEDGE_PREFIX = "knowledge/"

# Repo roles that can host a project's KB vault, in resolution precedence
# order. Read by resolve_kb_repo (which repo a project's vault is in) and by
# kb_sweep_tick (which projects have a vault at all) — the two must agree on
# the vocabulary or the sweep would skip projects the resolver can serve.
_KB_REPO_ROLES: Tuple[str, ...] = ("knowledge", "jobs")


@dataclass(frozen=True)
class KbRepoRef:
    """Resolved location and credential handle for one project's live vault.

    ``credential_ref`` is a datasource UUID, never credential material. The
    URL is excluded from repr because legacy managed Gitea URLs can contain
    basic-auth userinfo; API responses already redact that separately.
    """

    forge: str
    repo_url: str = field(repr=False)
    owner: str
    repo: str
    branch: str
    credential_ref: Optional[str] = None


def _forge_from_repo_url(repo_url: str, override: Optional[str]) -> str:
    configured = str(override or "").strip().lower()
    if configured:
        return configured
    hostname = (urlparse(repo_url).hostname or "").lower().rstrip(".")
    if hostname in {"github.com", "www.github.com"}:
        return "github"
    if hostname in {"gitlab.com", "www.gitlab.com"}:
        return "gitlab"
    # Existing managed and legacy project repositories are Gitea. Keeping
    # unknown/self-hosted hosts on that default is the backwards-compatible
    # behavior; GitHub Enterprise is selected by the native datasource's
    # explicit forge override.
    return "gitea"


async def _native_kb_datasource_ref(
    postgres_db: Any, project_id: str
) -> Optional[dict[str, Any]]:
    getter = getattr(postgres_db, "get_native_project_kb_datasource_ref", None)
    if not callable(getter):
        return None
    row = await getter(project_id)
    return row if isinstance(row, dict) else None


# Bump when OKF parsing semantics change independently of the chunker. The
# watermark pipeline stamp also includes the normalized root, while chunk rows
# retain the embedding-only version used to filter query vectors.
OKF_PARSER_VERSION = "okf-parser-v1"

# OKF reserved filenames — generated navigation/history, never indexed
# (mirrors gardener._RESERVED; index.md carries no frontmatter by spec).
_RESERVED_BASENAMES = {"index.md", "log.md"}

# CHECK-constraint vocabularies from vector/0001 + vector/0013 + vector/0015 —
# frontmatter is
# human-editable, so unknown values map to safe defaults instead of failing the
# row INSERT. Canonical copy: src/services/knowledge_graph.py NOTE_TYPES (not
# importable here — the orchestrator image has no agent deps).
VALID_NOTE_TYPES = {
    "goal",
    "plan",
    "decision",
    "learning",
    "code",
    "source",
    "question",
    "state",
    "retrospective",
    "datasource",
    "feature",
    "issue",
    "idea",
    "charter",
    "report",
}
VALID_STATUSES = {"active", "resolved", "superseded", "archived"}
_DEFAULT_NOTE_TYPE = "learning"
_DEFAULT_STATUS = "active"

# Canonical copy: src/services/knowledge_graph.py PRIORITY_RANKS (not importable
# here — the orchestrator image has no agent deps).
PRIORITY_RANKS = {"high": 0, "normal": 1, "low": 2}
_DEFAULT_PRIORITY_RANK = 1

# knowledge_index.note_id is VARCHAR(100); superseded_by VARCHAR(100),
# confidence VARCHAR(20).
_NOTE_ID_MAX = 100
_CONFIDENCE_MAX = 20

# The trailing "\s*$" is gone: a lazy "(.+?)" followed by an optional whitespace
# run gave the engine a fresh way to split every trailing space on a hostile
# line. The final anchor is unnecessary too: "." already stops at a newline.
# Trim title whitespace at the call site instead.
_H1_RE = re.compile(r"^#\s+(.+)", re.MULTILINE)


def normalize_root_path(root_path: Optional[str]) -> str:
    """Normalize an already-validated repository-relative OKF root."""
    root = str(root_path or "").strip().replace("\\", "/").strip("/")
    return "/".join(part for part in root.split("/") if part not in ("", "."))


def reindex_pipeline_version(embedding_stamp: str, root_path: Optional[str]) -> str:
    """Watermark stamp for embedding, parser, and root-path semantics."""
    root = normalize_root_path(root_path)
    scope = hashlib.sha256(f"{OKF_PARSER_VERSION}\0{root}".encode()).hexdigest()[:12]
    return f"{embedding_stamp}:{OKF_PARSER_VERSION}:{scope}"


def knowledge_blob_map(
    tree: List[Dict[str, str]], root_path: Optional[str] = "knowledge"
) -> Dict[str, str]:
    """Filter a recursive ``list_tree`` result to indexable knowledge notes.

    Keeps ``knowledge/**/*.md`` blobs, drops the reserved generated files
    (``index.md`` / ``log.md``). Returns ``{path: blob_sha}`` — the same shape
    ``KnowledgeStore.get_indexed_blob_shas`` returns, so the two sides diff
    directly.
    """
    root = normalize_root_path(root_path)
    prefix = f"{root}/" if root else ""
    result: Dict[str, str] = {}
    for entry in tree:
        if entry.get("type") != "blob":
            continue
        path = str(entry.get("path", ""))
        if prefix and not path.startswith(prefix):
            continue
        if not path.endswith(".md"):
            continue
        if posixpath.basename(path) in _RESERVED_BASENAMES:
            continue
        result[path] = str(entry.get("sha", ""))
    return result


def plan_reindex(
    indexed: Dict[str, str],
    current: Dict[str, str],
    full: bool = False,
) -> Tuple[List[str], List[str]]:
    """Diff the indexed blob map against the git tree's.

    Returns ``(upsert_paths, delete_paths)``, both sorted for determinism.
    A path whose indexed ``blob_sha`` matches the tree is skipped — the per-row
    self-heal that makes re-runs after an interrupted reindex cheap. ``full``
    re-upserts everything in the tree (pipeline-version bump / operator rebuild);
    deletes are computed the same way in both modes.
    """
    upserts = sorted(
        path for path, sha in current.items() if full or indexed.get(path) != sha
    )
    deletes = sorted(path for path in indexed if path not in current)
    return upserts, deletes


def _parse_created_at(value: Any) -> Optional[datetime]:
    """Best-effort parse of a frontmatter timestamp (``created``, ``ready_at``).

    ``_render_note_md`` writes ``created:`` unquoted (knowledge_tools.py:449),
    so YAML's implicit timestamp resolver has usually already turned the
    common case into a native ``datetime``/``date`` by the time it reaches
    here — this only has real work to do for a quoted string or a stray
    type. Never raises: an unparseable ``created:`` line must still let the
    note index, just without a ``created_at`` (the pool query's
    ``NULLS LAST`` ordering already covers that — project_backlog.py).
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def note_fields(path: str, fm: Optional[Dict[str, Any]], body: str) -> Dict[str, Any]:
    """Map a parsed note file onto ``upsert_kb_note`` arguments.

    The inverse of ``_render_note_md``'s frontmatter (id/type/tags/keywords/
    confidence/status/priority/created/superseded_by), hardened for
    human-authored files: a missing frontmatter block derives the id from the
    filename stem and the title from the first H1; values outside the
    CHECK-constraint vocabularies (valid_note_type / valid_note_status) — and
    unrecognised ``priority`` words — fall back to safe defaults rather than
    failing the INSERT.

    ``priority`` is the one field that does NOT follow that "unknown ->
    default" rule (fix round 2, Finding 3): an *absent* ``priority`` key maps
    to ``None`` ("this file carries no opinion — leave the stored rank
    alone"), never to ``_DEFAULT_PRIORITY_RANK``. Every pre-existing note
    (and any human edit that just doesn't touch the line) lacks this key, and
    ``upsert_kb_note`` runs on every merge and via the sweeper — defaulting
    it would silently stamp "normal" over a real priority on the very next
    reindex. An *invalid* value (a typo) is different: the value is present,
    just unparseable, so it still falls back to ``_DEFAULT_PRIORITY_RANK``
    rather than propagating garbage or failing the row. The numeric rank
    itself (0/1/2, int or numeric string) is also accepted, not just the
    word: a user who reads ``knowledge_index.priority`` (0=high) elsewhere
    and writes it straight back (``priority: 0``) must round-trip rather than
    silently landing on "normal" via the word-only lookup.

    ``created`` maps to ``created_at`` (project-backlog-pipeline fix wave,
    finding B3): this is the only ingest path that can set it at all, since
    the reindexer INSERTs new rows with no ``created_at`` otherwise — the
    cockpit's backlog panel is read-only, so a hand-authored file is the only
    way a user files a ticket, and without this it always sorts as if newest
    (NULL), regardless of the ticket's real age. Since Slice A that is also
    true of every *agent*-written note: ``kb_write`` no longer writes the row
    itself, so its own ``created:`` line is the only thing standing between a
    new note and a NULL creation time.

    ``modified`` maps to ``modified_at`` for the same reason and one more: it
    orders the search function's recency arm, which contributes to the RRF
    score, so a NULL there degrades ranking silently rather than failing
    anything. The arm is a bare ``ORDER BY ki.modified_at DESC``
    (vector_schema_current.sql) — no ``NULLS LAST`` — and Postgres sorts NULLs
    FIRST under ``DESC``, so an unmapped ``modified_at`` did not push those
    rows to the bottom of the arm: it let every legacy row with a NULL there
    occupy the TOP of the recency window and crowd out genuinely recent
    notes. Mapping the field is what makes the arm mean anything once those
    rows are backfilled. (Adding ``NULLS LAST`` to the SQL needs a migration
    and is a separate change.)
    """
    fm = fm or {}

    stem = posixpath.basename(path)
    if stem.endswith(".md"):
        stem = stem[: -len(".md")]
    note_id = str(fm.get("id") or stem)[:_NOTE_ID_MAX]

    m = _H1_RE.search(body or "")
    title = m.group(1).rstrip() if m else note_id

    note_type = str(fm.get("type", "")).strip().lower()
    if note_type not in VALID_NOTE_TYPES:
        note_type = _DEFAULT_NOTE_TYPE

    status = str(fm.get("status", "")).strip().lower()
    if status not in VALID_STATUSES:
        status = _DEFAULT_STATUS

    raw_priority = fm.get("priority")
    if raw_priority is None:
        # Absent from frontmatter, or an explicit YAML null — not "unknown,
        # use the default" like type/status above. See the docstring note.
        priority: Optional[int] = None
    else:
        priority = None
        if not isinstance(raw_priority, bool):
            try:
                as_rank: Optional[int] = int(raw_priority)
            except (TypeError, ValueError):
                as_rank = None
            if as_rank in PRIORITY_RANKS.values():
                priority = as_rank
        if priority is None:
            priority = PRIORITY_RANKS.get(
                str(raw_priority).strip().lower(), _DEFAULT_PRIORITY_RANK
            )

    def _as_list(value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            return [str(v) for v in value]
        return [str(value)]

    confidence = fm.get("confidence")
    superseded_by = fm.get("superseded_by")

    return {
        "note_id": note_id,
        "title": title,
        "note_type": note_type,
        "status": status,
        "priority": priority,
        # Machine tags are folded to lowercase here so a hand-edited file that
        # writes `Ready` or `Category:Executor` still matches the tick's
        # containment queries. Human tags keep whatever case their author
        # chose — they are display text.
        "tags": normalize_tags(_as_list(fm.get("tags"))),
        "keywords": _as_list(fm.get("keywords")),
        "confidence": str(confidence)[:_CONFIDENCE_MAX] if confidence else None,
        "superseded_by": str(superseded_by)[:_NOTE_ID_MAX] if superseded_by else None,
        "created_at": _parse_created_at(fm.get("created")),
        # `modified` is the ordering key of the search function's recency arm
        # (a bare ORDER BY ki.modified_at DESC — no NULLS LAST), which carries
        # a real share of the RRF score. Postgres sorts NULLs FIRST under
        # DESC, so unmapped this did not rank notes last: every NULL-stamped
        # row the file path indexed sat at the TOP of that arm's window,
        # crowding out genuinely recent notes. A silent quality loss, never
        # an error.
        "modified_at": _parse_created_at(fm.get("modified")),
        # Absent means "this file carries no opinion, leave the stored value
        # alone" — the same sentinel as priority. A replay is not an
        # authorization event, so this never falls back to now().
        "ready_at": _parse_created_at(fm.get("ready_at")),
    }


@dataclass
class NoteIndexOutcome:
    """What one note's index attempt did.

    ``status`` is ``indexed`` (chunks + links durable, note stamped),
    ``malformed`` (unparseable frontmatter — lint's problem, not the
    indexer's; the caller removes any prior indexed version of the path), or
    ``oversized`` (over an inline caller's ``max_chunks``; ``chunks`` carries
    the planned count and the store was never touched — the sweep indexes it
    instead).
    """

    status: str
    note_id: Optional[str] = None
    chunks: int = 0
    detail: Optional[str] = None


class NoteIndexError(Exception):
    """A note failed to index.

    Carries the parsed OKF id so the caller can run the duplicate-id
    diagnostic, which needs the id from the frontmatter rather than a guess
    from the filename — they differ in exactly the cases that hit the
    ``uq_knowledge_project_note`` constraint.
    """

    def __init__(self, cause: Exception, note_id: Optional[str]) -> None:
        super().__init__(str(cause))
        self.cause = cause
        self.note_id = note_id


async def index_single_note(
    *,
    store: Any,
    embedding_service: Any,
    kb_id: uuid.UUID,
    path: str,
    text: str,
    blob_sha: str,
    embedding_stamp: str,
    movable_paths: Optional[List[str]] = None,
    max_chunks: Optional[int] = None,
    retrieval_messages: Optional[List[str]] = None,
    job_id: Optional[uuid.UUID] = None,
) -> NoteIndexOutcome:
    """Index ONE note into the disposable chunk index.

    The shared unit behind both writers: the sweep calls it once per changed
    path, and the materialisation endpoint calls it once, inline, for the note
    it has just committed. Both must agree, which is why the order lives here
    and not at either call site.

    That order — embed, adopt, upsert UNSTAMPED, chunks, links, stamp — is the
    durability contract. The stamp means "chunks and links are durable for this
    git blob", so any failure before it leaves ``blob_sha`` stale or NULL and
    the note stays in the next tree diff. A stamped-but-chunkless note would
    read as up-to-date forever (the live 2026-07-05 zero-chunks gap).

    Never advances the watermark, never deletes, never reconciles: those are
    whole-sweep concerns and an inline caller has no business with them.

    ``max_chunks`` caps how much work an *inline* caller will do in a request:
    over it, the note returns ``oversized`` and the sweep indexes it instead.
    The sweep itself passes ``None`` — it has all the time it needs, and a
    note no one indexes is worse than a slow one.

    ``retrieval_messages`` forwards straight to :meth:`KnowledgeStore.upsert_kb_note`.
    ``None`` (the sweep's every call, since OKF frontmatter carries no such
    field) means "leave the stored value alone" — only an inline materialize
    call that was actually given messages has an opinion to write.

    ``job_id`` is the same shape and the same contract: it is the writing job
    for ``knowledge_index.job_id``, the provenance behind
    ``kb_list(job_id=...)``, and the file cannot carry it either. The sweep
    passes ``None`` because it re-indexes notes it did not write.

    Raises ``NoteIndexError`` for a genuine failure; returns a ``malformed``
    outcome for unparseable frontmatter, which is not an error here.
    """
    try:
        fm, body = parse_note_md(text)
    except ValueError as exc:
        return NoteIndexOutcome(status="malformed", detail=str(exc))

    # ``note_fields`` is inside the wrapping try, not before it: it is the
    # step that derives ``note_id``, and a raise from it used to escape as a
    # raw exception while the docstring promised ``NoteIndexError``. Seeded
    # None first so the handler can still name the note it could not parse.
    note_id: Optional[str] = None
    try:
        fields = note_fields(path, fm, body)
        note_id = fields["note_id"]

        if max_chunks is not None:
            # Count before embedding, so an oversized note costs one cheap
            # structural pass and no embedding round-trip. Under the cap the
            # chunker runs twice; that is regex + token counting on a small
            # note, which is far cheaper than the API call it protects. Off the
            # loop for the same reason as embed_note_chunks' own chunking.
            planned = await asyncio.to_thread(chunk_note, body)
            if len(planned) > max_chunks:
                return NoteIndexOutcome(
                    status="oversized", note_id=note_id, chunks=len(planned)
                )

        # Embed BEFORE writing: a failed embed leaves the stale blob_sha in
        # place, so the next run retries this note.
        chunk_rows, _ = await embed_note_chunks(
            body,
            title=fields["title"],
            note_type=fields["note_type"],
            tags=fields["tags"],
            embedding_service=embedding_service,
        )
        # Claim a pre-slice-3 row or move the stable OKF id on a Git rename,
        # so the path-keyed upsert cannot collide with the old row's
        # uq_knowledge_project_note identity.
        await store.adopt_legacy_row(
            kb_id,
            note_id,
            path,
            movable_paths=movable_paths,
        )
        note_row = await store.upsert_kb_note(
            kb_id=kb_id,
            note_id=note_id,
            path=path,
            title=fields["title"],
            note_type=fields["note_type"],
            content=body,
            blob_sha=None,
            embedding_version=None,
            status=fields["status"],
            confidence=fields["confidence"],
            tags=fields["tags"],
            keywords=fields["keywords"],
            retrieval_messages=retrieval_messages,
            superseded_by=fields["superseded_by"],
            job_id=job_id,
            priority=fields["priority"],
            created_at=fields["created_at"],
            modified_at=fields["modified_at"],
            ready_at=fields["ready_at"],
        )
        await store.replace_note_chunks(
            note_row=note_row,
            kb_id=kb_id,
            chunks=chunk_rows,
            embedding_version=embedding_stamp,
        )
        await store.replace_note_links(
            source_note_row=note_row,
            kb_id=kb_id,
            source_id=note_id,
            targets=(_internal_link_targets(body) + frontmatter_link_targets(fm)),
        )
        centroid = note_centroid([c["embedding"] for c in chunk_rows])
        await store.stamp_note_indexed(
            note_row, blob_sha, embedding_stamp, centroid=centroid
        )
    except Exception as exc:
        raise NoteIndexError(exc, note_id) from exc

    return NoteIndexOutcome(status="indexed", note_id=note_id, chunks=len(chunk_rows))


# The project-scoped identity constraint on knowledge_index. A violation here
# is never a transient fault: it means one OKF note id exists at two paths in
# the same vault, and neither adopt_legacy_row (which only claims a pathless or
# being-deleted row) nor upsert_kb_note's `(kb_id, path)` arbiter can absorb a
# conflict raised by a *different* unique constraint. The INSERT dies once per
# affected note, on every run, forever.
_NOTE_ID_CONSTRAINT = "uq_knowledge_project_note"


def _violated_constraint(exc: BaseException) -> str:
    """Best-effort constraint name for a driver error.

    asyncpg puts it on ``constraint_name``; anything that wrapped or
    re-raised the error only has the rendered message. Both are checked so
    the diagnostic below survives a driver swap or a wrapping layer.
    """
    name = getattr(exc, "constraint_name", None)
    return str(name) if name else str(exc)


async def _log_duplicate_note_id(
    store: Any,
    kb_id: uuid.UUID,
    path: str,
    note_id: Optional[str],
    exc: BaseException,
) -> bool:
    """Turn a duplicate-key error into a statement of what is actually wrong.

    The raw Postgres error names the note id and nothing else — not the path
    being indexed, not the path the index already holds, and not the fact that
    those are two different files claiming one identity. Read literally it
    suggests the reindexer is retrying a write that should have succeeded; the
    truth is the opposite (the constraint is correct and the insert is
    impossible), which is why this shipped as a quiet per-note error for as
    long as it did.

    Emits ONE line carrying the note id, both paths, and which one the index
    holds, so a future occurrence is readable without a database session.

    Returns True when it recognised and logged the condition; False leaves the
    caller's generic error line in place. Never raises: this runs inside an
    ``except`` block and a failure to explain an error must not replace it.
    """
    if _NOTE_ID_CONSTRAINT not in _violated_constraint(exc):
        return False

    incumbent: Optional[Dict[str, Any]] = None
    if note_id:
        try:
            incumbent = await store.find_note_id_owner(kb_id, note_id)
        except Exception as lookup_exc:  # pragma: no cover - diagnostic only
            logger.debug(
                "kb_reindex[%s]: duplicate-id lookup failed for %s: %s",
                kb_id,
                note_id,
                lookup_exc,
            )

    held = (incumbent or {}).get("path")
    logger.error(
        "kb_reindex[%s]: note id %r exists at two paths — indexing %s but the "
        "index holds %s; the %s row wins and %s stays unindexed every run. "
        "One of the two files must be removed or given a distinct OKF id "
        "(a note filename used as a DIRECTORY is the usual cause).",
        kb_id,
        note_id or "<unknown>",
        path,
        held or "a pathless legacy row (no file)",
        "existing" if held else "pathless",
        path,
    )
    return True


async def _duplicate_holder_path(
    store: Any, kb_id: uuid.UUID, note_id: Optional[str]
) -> Optional[str]:
    """The path the index already holds for ``note_id``, for the advisory text.

    Best-effort and diagnostic only: a second lookup after
    ``_log_duplicate_note_id`` already ran one, but this runs once per skipped
    file per sweep, not per row, so the extra query is cheap. Never raises.
    """
    if not note_id:
        return None
    try:
        row = await store.find_note_id_owner(kb_id, note_id)
    except Exception:  # noqa: BLE001 — diagnostic only
        return None
    return (row or {}).get("path")


def _duplicate_advisory(
    skipped: List[Tuple[str, str, Optional[str]]],
) -> Optional[str]:
    """Render the skipped-duplicate list into the watermark's advisory text.

    ``None`` when nothing was skipped, so a clean run clears a stale advisory
    from a prior sweep instead of leaving it stuck.
    """
    if not skipped:
        return None
    parts = [
        f"'{nid}' at {path} (index holds {holder or 'another path'})"
        for nid, path, holder in skipped
    ]
    text = (
        f"{len(skipped)} note(s) skipped — duplicate note id: "
        + "; ".join(parts)
        + ". Give one file an explicit `id:` or run kb_lint."
    )
    return text[:2000]


_kb_locks: Dict[uuid.UUID, asyncio.Lock] = {}


def _kb_lock(kb_id: uuid.UUID) -> asyncio.Lock:
    """Per-KB serialization for reindex runs (PR3.1).

    Two post-merge triggers ~30s apart ran concurrent full rebuilds against the
    same KB on dev (interleaved chunk delete+insert batches). All in-process
    entry points (post-merge trigger, sweeper tick, operator endpoint) funnel
    through :func:`reindex_kb`, so an asyncio lock suffices — the sweeper is
    leader-gated and loop advances run on the leader. The second replica's
    operator endpoint can still race in theory; the up-to-date short-circuit
    under the lock makes the overlap window one HEAD read.
    """
    return _kb_locks.setdefault(kb_id, asyncio.Lock())


@asynccontextmanager
async def kb_index_lock(store: Any, kb_id: uuid.UUID, *, wait: bool = False):
    """Serialize all mutations of one KB index locally and across replicas.

    Routine reindex requests use the non-blocking advisory claim and report
    ``already-indexing`` when another replica owns it. Datasource deletion uses
    ``wait=True`` because it must run *after* any mutation already in flight and
    keep the claim until both vector cleanup and app-row deletion are complete.

    Narrow mock stores used by legacy unit tests have neither concrete lock
    method; they still receive the in-process serialization contract.
    """
    async with _kb_lock(kb_id):
        if wait:
            lock_method = getattr(type(store), "reindex_lock", None)
            if lock_method is not None:
                async with store.reindex_lock(kb_id) as conn:
                    yield conn
                return
            yield None
            return

        claim_method = getattr(type(store), "try_reindex_lock", None)
        if claim_method is not None:
            async with store.try_reindex_lock(kb_id) as claimed:
                yield bool(claimed)
            return
        yield True


def _empty_summary(
    status: str,
    *,
    errors: int = 0,
    indexed_commit: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "status": status,
        "indexed_commit": indexed_commit,
        "full": False,
        "upserted": 0,
        "deleted": 0,
        "skipped": 0,
        "skipped_duplicates": 0,
        "errors": errors,
    }


async def _set_reindex_status(
    store: Any,
    kb_id: uuid.UUID,
    status: str,
    *,
    source_label: str,
    branch: Optional[str],
    source_head: Optional[str] = None,
    last_error: Optional[str] = None,
    error_fingerprint: Optional[str] = None,
) -> None:
    """Best-effort operational state; never compromise index durability."""
    try:
        await store.set_watermark_status(
            kb_id,
            status,
            source_head=source_head,
            last_error=(last_error or "")[:2000] or None,
            repo_name=source_label[:255],
            branch=(branch or "")[:255] or None,
            error_fingerprint=error_fingerprint,
        )
    except Exception as exc:
        logger.warning("kb_reindex[%s]: status update skipped: %s", kb_id, exc)


def _error_fingerprint(note_errors: List[str]) -> Optional[str]:
    """Fingerprint a run's per-note error set (spec WP3/H3).

    Sorted so the same failure set fingerprints identically across runs
    regardless of tree-walk order; truncated to 16 hex chars — a log/DB label,
    not a security boundary.
    """
    if not note_errors:
        return None
    digest = hashlib.sha1("\n".join(sorted(note_errors)).encode("utf-8"))
    return digest.hexdigest()[:16]


async def _warn_if_wedged(
    store: Any,
    kb_id: uuid.UUID,
    errors: int,
    fingerprint: Optional[str],
) -> None:
    """Log a one-shot WEDGED warning once the streak crosses the threshold.

    Advisory only: the streak itself is computed durably in SQL
    (``KnowledgeStore.set_watermark_status``); this just reads it back and
    decides whether to say something. WARNING fires exactly once, at
    ``streak == WEDGE_STREAK_THRESHOLD``; every sweep after that logs at DEBUG
    so an operator who missed the warning can still find it without the log
    repeating a WARNING every 15 minutes forever.
    """
    try:
        wm = await store.get_watermark(kb_id)
    except Exception:  # noqa: BLE001 — advisory only
        return
    streak = int(getattr(wm, "error_streak", 0) or 0)
    if streak == WEDGE_STREAK_THRESHOLD:
        logger.warning(
            "kb_reindex[%s]: WEDGED — the same %d note error(s) for %d consecutive "
            "sweeps (fingerprint %s); retrying will not help. Last error: %s",
            kb_id,
            errors,
            streak,
            fingerprint,
            getattr(wm, "last_error", None),
        )
    elif streak > WEDGE_STREAK_THRESHOLD:
        logger.debug("kb_reindex[%s]: still wedged (%d sweeps)", kb_id, streak)


async def record_reindex_source_failure(
    *,
    store: Any,
    kb_id: uuid.UUID,
    source_label: str,
    branch: Optional[str],
    error: Exception,
    is_active: Optional[Callable[[], Awaitable[bool]]] = None,
) -> Dict[str, Any]:
    """Record a source-construction failure without advancing the watermark.

    External Git sources validate transport and credentials while they are
    constructed, before :func:`reindex_kb` can enter its normal watermark
    cycle. Keep that validation boundary, but give failures the same durable
    operator-visible state as a failed HEAD read. The index/delete claim and
    liveness recheck prevent a stale sweep row from resurrecting status after
    its datasource was deleted.
    """
    detail = str(error)[-2000:] or "Git source construction failed"
    async with kb_index_lock(store, kb_id) as claimed:
        if not claimed:
            return _empty_summary("already-indexing")
        if is_active is not None:
            try:
                active = await is_active()
            except Exception as exc:
                logger.warning(
                    "kb_reindex[%s]: source liveness check failed while "
                    "recording construction error: %s",
                    kb_id,
                    exc,
                )
                return _empty_summary("source-failed", errors=1)
            if not active:
                return _empty_summary("source-deleted")

        previous_indexed_commit: Optional[str] = None
        try:
            watermark = await store.get_watermark(kb_id)
            if watermark is not None:
                previous_indexed_commit = watermark.indexed_commit
        except Exception as exc:
            # Status is operational best effort. A failed read must not stop a
            # subsequent upsert from replacing a stale error if writes work.
            logger.warning(
                "kb_reindex[%s]: previous watermark unavailable while "
                "recording construction error: %s",
                kb_id,
                exc,
            )

        await _set_reindex_status(
            store,
            kb_id,
            "failed",
            source_label=source_label,
            branch=branch,
            last_error=detail,
        )
        logger.warning(
            "kb_reindex[%s]: source construction failed for %s: %s",
            kb_id,
            source_label,
            detail,
        )
        return _empty_summary(
            "source-failed",
            errors=1,
            indexed_commit=previous_indexed_commit,
        )


async def reindex_kb(
    *,
    store: Any,
    embedding_service: Any,
    kb_id: uuid.UUID,
    source: Any = None,
    root_path: Optional[str] = None,
    source_label: Optional[str] = None,
    # Legacy native-project arguments. They remain supported so post-merge,
    # MCP, and existing callers can migrate independently.
    gitea_client: Any = None,
    repo_name: Optional[str] = None,
    branch: Optional[str] = "main",
    force_full: bool = False,
    is_active: Optional[Callable[[], Awaitable[bool]]] = None,
) -> Dict[str, Any]:
    """Bring a KB's chunk-granular index up to the repo's HEAD.

    The watermark cycle (§5): read HEAD → short-circuit if the watermark already
    covers it under the current pipeline version → diff the git tree's
    ``{path: blob_sha}`` map against the index's → per changed note:
    fetch @HEAD, ``parse_note_md``, chunk + embed (PR2), adopt any legacy row,
    upsert the note row UNSTAMPED, replace its chunks (PR1), then stamp
    ``blob_sha``/``embedding_version`` → remove deleted notes → advance the
    watermark. Embed-before-write and stamp-after-chunks ordering per note: a
    note whose embedding OR chunk write failed keeps a stale/NULL ``blob_sha``,
    so the next run retries it.

    Full rebuild (``full=True`` in the plan) when the pipeline version changed
    (new model/dims/chunker), when there is no watermark yet (first index of a
    legacy corpus — also when legacy row adoption happens), or on ``force_full``
    (the ``kb reindex --full`` operator hatch).

    Honesty rules: the watermark advances ONLY on a zero-error run (``status:
    completed``); any fetch/embed/persist error leaves it untouched (``partial``)
    and the per-row ``blob_sha`` self-heal makes the retry cheap. Unparseable
    notes are *skipped*, not errors — a malformed file is ``kb_lint``'s problem
    and must not wedge the watermark forever. Legacy pathless rows whose file no
    longer exists are invisible to the diff and left for the offline frontmatter
    audit (§11 migration-hygiene gate).

    Runs are serialized per KB (see :func:`kb_index_lock`); distinct KBs proceed
    concurrently.

    Returns a summary dict: ``status`` (``no-head`` / ``up-to-date`` /
    ``tree-fetch-failed`` / ``completed`` / ``partial`` / ``source-deleted``),
    ``indexed_commit``, ``full``, ``upserted``, ``deleted``, ``skipped``,
    ``errors``.
    """
    if source is None:
        if gitea_client is None or not repo_name:
            raise ValueError("reindex_kb requires a git source")
        from orchestrator.services.kb_git_source import GiteaKnowledgeGitSource

        source = GiteaKnowledgeGitSource(
            gitea_client,
            repo_name,
            branch=branch or "main",
            label=source_label or repo_name,
        )
        if root_path is None:
            root_path = "knowledge"
    elif root_path is None:
        root_path = ""

    label = str(source_label or source.label or "knowledge-repository")
    normalized_root = normalize_root_path(root_path)

    async with kb_index_lock(store, kb_id) as claimed:
        if not claimed:
            return _empty_summary("already-indexing")
        # External datasource work lists can become stale while they wait for
        # an in-flight rebuild or deletion. Re-check application ownership only
        # after both mutation locks are held, before even writing a status row.
        if is_active is not None and not await is_active():
            return _empty_summary("source-deleted")
        return await _reindex_kb_unlocked(
            source=source,
            store=store,
            embedding_service=embedding_service,
            kb_id=kb_id,
            source_label=label,
            branch=branch,
            root_path=normalized_root,
            force_full=force_full,
        )


async def _reindex_kb_unlocked(
    *,
    source: Any,
    store: Any,
    embedding_service: Any,
    kb_id: uuid.UUID,
    source_label: str,
    branch: Optional[str],
    root_path: str,
    force_full: bool = False,
) -> Dict[str, Any]:
    """The reindex cycle body — call through :func:`reindex_kb` (per-KB lock)."""
    # Read the durable as-of marker before contacting the source. Every failure
    # summary must report the commit the index still covers, never the uncommitted
    # source HEAD (or a misleading None when a prior usable snapshot exists).
    wm = await store.get_watermark(kb_id)
    previous_indexed_commit = wm.indexed_commit if wm is not None else None
    try:
        head = await source.get_head()
    except Exception as exc:
        detail = str(exc)[-2000:] or "Git HEAD read failed"
        await _set_reindex_status(
            store,
            kb_id,
            "failed",
            source_label=source_label,
            branch=branch,
            last_error=detail,
        )
        logger.warning(
            "kb_reindex[%s]: HEAD read failed for %s: %s", kb_id, source_label, detail
        )
        return _empty_summary(
            "source-failed", errors=1, indexed_commit=previous_indexed_commit
        )
    if not head:
        await _set_reindex_status(
            store,
            kb_id,
            "failed",
            source_label=source_label,
            branch=branch,
            last_error="Repository branch has no resolvable HEAD",
        )
        logger.warning("kb_reindex[%s]: no HEAD for %s", kb_id, source_label)
        return _empty_summary("no-head", indexed_commit=previous_indexed_commit)

    embedding_stamp = embedding_version_for_service(embedding_service)
    current_version = reindex_pipeline_version(embedding_stamp, root_path)
    if (
        wm is not None
        and wm.indexed_commit == head
        and wm.pipeline_version == current_version
        and not force_full
    ):
        await _set_reindex_status(
            store,
            kb_id,
            "ready",
            source_label=source_label,
            branch=branch,
            source_head=head,
        )
        return {
            "status": "up-to-date",
            "indexed_commit": head,
            "full": False,
            "upserted": 0,
            "deleted": 0,
            "skipped": 0,
            "errors": 0,
        }

    full = force_full or wm is None or wm.pipeline_version != current_version

    # Turn the full-rebuild *decision* into durable per-row state before a single
    # note is embedded. Left as a per-run flag it has to survive to completion:
    # the fact behind it (pipeline_version) is written on success alone, so an
    # interrupted rebuild re-derives full=True next time and re-embeds
    # everything the dead run finished. Clearing the stamps and recording the
    # pipeline version up front hands the work list to the per-row diff instead,
    # which makes every subsequent run resume where this one stops.
    plan_full = full
    if full:
        # Only a *changed* pipeline leaves stale rows behind. A KB that merely
        # never finished has pipeline_version NULL, and its stamped notes are
        # perfectly current — clearing those would throw away precisely the work
        # this whole change exists to preserve, and on a large vault it is also
        # the expensive case. So invalidate wholesale only for a real version
        # change or an operator rebuild; otherwise sweep just the rows whose
        # embedding version moved, which is normally none of them.
        stale_pipeline = force_full or bool(
            wm is not None
            and wm.pipeline_version
            and wm.pipeline_version != current_version
        )
        try:
            cleared = await store.clear_note_stamps(
                kb_id,
                embedding_version=None if stale_pipeline else embedding_stamp,
            )
            await store.upsert_watermark(
                kb_id=kb_id,
                repo_name=source_label[:255],
                branch=branch,
                indexed_commit=previous_indexed_commit,
                pipeline_version=current_version,
                source_head=head,
                status="indexing",
                last_error=None,
            )
        except Exception as exc:
            # Resumability is an optimization; correctness is not. If either
            # write fails, fall back to the old per-run semantics rather than
            # entering the loop with a work list that skips unstamped notes.
            # %r, not %s: the first production failure here was an asyncpg
            # command timeout, whose str() is empty — the log read "skipped ()"
            # and named nothing at all.
            logger.warning(
                "kb_reindex[%s]: rebuild invalidation skipped (%r) — "
                "falling back to a non-resumable full pass",
                kb_id,
                exc,
            )
        else:
            plan_full = False
            logger.info(
                "kb_reindex[%s]: resumable rebuild (stale_pipeline=%s) — "
                "cleared %d note stamp(s)",
                kb_id,
                stale_pipeline,
                cleared,
            )

    await _set_reindex_status(
        store,
        kb_id,
        "indexing",
        source_label=source_label,
        branch=branch,
        source_head=head,
    )
    try:
        async with source.snapshot(head) as snapshot:
            return await _reindex_snapshot(
                snapshot=snapshot,
                store=store,
                embedding_service=embedding_service,
                kb_id=kb_id,
                source_label=source_label,
                branch=branch,
                root_path=root_path,
                head=head,
                embedding_stamp=embedding_stamp,
                pipeline_version=current_version,
                full=full,
                plan_full=plan_full,
                previous_indexed_commit=previous_indexed_commit,
            )
    except Exception as exc:
        detail = str(exc)[-2000:] or "Git snapshot failed"
        await _set_reindex_status(
            store,
            kb_id,
            "failed",
            source_label=source_label,
            branch=branch,
            source_head=head,
            last_error=detail,
        )
        logger.warning(
            "kb_reindex[%s]: snapshot failed for %s: %s",
            kb_id,
            source_label,
            detail,
        )
        return _empty_summary(
            "source-failed", errors=1, indexed_commit=previous_indexed_commit
        )


async def _reindex_snapshot(
    *,
    snapshot: Any,
    store: Any,
    embedding_service: Any,
    kb_id: uuid.UUID,
    source_label: str,
    branch: Optional[str],
    root_path: str,
    head: str,
    embedding_stamp: str,
    pipeline_version: str,
    full: bool,
    previous_indexed_commit: Optional[str],
    plan_full: Optional[bool] = None,
) -> Dict[str, Any]:
    """Index one immutable source snapshot, which owns all blob reads.

    ``full`` describes the run for the caller's summary and logs; ``plan_full``
    is the narrower question of whether the work list still has to be forced.
    They diverge on the normal rebuild path, where the caller has already
    cleared the note stamps: the per-row diff then selects the whole vault on
    its own *and* keeps selecting whatever is left after an interruption, so
    forcing it a second time would only discard that resumability. ``plan_full``
    defaults to ``full`` so callers that never invalidate keep the old behavior.
    """
    if plan_full is None:
        plan_full = full

    try:
        tree = await snapshot.list_tree()
    except Exception as exc:
        detail = str(exc)[-2000:] or "Git tree read failed"
        await _set_reindex_status(
            store,
            kb_id,
            "failed",
            source_label=source_label,
            branch=branch,
            source_head=head,
            last_error=detail,
        )
        logger.warning("kb_reindex[%s]: tree fetch failed at %s", kb_id, head)
        return {
            "status": "tree-fetch-failed",
            "indexed_commit": previous_indexed_commit,
            "full": full,
            "upserted": 0,
            "deleted": 0,
            "skipped": 0,
            "errors": 1,
        }

    current_map = knowledge_blob_map(tree, root_path)
    indexed_map = await store.get_indexed_blob_shas(kb_id)
    upsert_paths, delete_paths = plan_reindex(indexed_map, current_map, full=plan_full)

    # Bulk-fetch the snapshot as ONE archive download when the batch is big
    # enough to amortize it (full rebuilds, big backlogs). Small incremental
    # runs keep the cheap per-file path. Kills the sequential per-note REST
    # N+1 that was the measured dominant Gitea load. Best-effort: prefetch
    # never raises, and without it get_file falls back to per-file reads.
    prefetch = getattr(snapshot, "prefetch", None)
    if prefetch is not None and len(upsert_paths) >= _ARCHIVE_PREFETCH_THRESHOLD:
        await prefetch()

    # Reset determinate-progress counters for this run so Cockpit can render a
    # bar. notes_total is the per-run work set (changed notes on an incremental
    # run, the whole vault on a full rebuild). Best-effort: never fail on it.
    notes_total = len(upsert_paths)
    try:
        await store.update_index_progress(kb_id, 0, notes_total)
    except Exception as exc:
        logger.debug("kb_reindex[%s]: progress reset skipped: %s", kb_id, exc)

    upserted = skipped = errors = 0
    skipped_duplicates: List[Tuple[str, str, Optional[str]]] = []
    invalid_paths: List[str] = []
    note_errors: List[str] = []
    for path in upsert_paths:
        try:
            text = await snapshot.get_file(path)
            if text is None:
                logger.warning("kb_reindex[%s]: fetch failed for %s", kb_id, path)
                errors += 1
                note_errors.append(f"{path}: fetch failed")
                continue
            outcome = await index_single_note(
                store=store,
                embedding_service=embedding_service,
                kb_id=kb_id,
                path=path,
                text=text,
                blob_sha=current_map[path],
                embedding_stamp=embedding_stamp,
                movable_paths=delete_paths,
            )
        except NoteIndexError as err:
            if await _log_duplicate_note_id(store, kb_id, path, err.note_id, err.cause):
                # H2: a second file claiming a live id is a lint condition, not a
                # sweep failure. Skip it, say so once, and let the KB reach ready.
                holder = await _duplicate_holder_path(store, kb_id, err.note_id)
                skipped_duplicates.append((err.note_id or "?", path, holder))
                continue
            logger.warning("kb_reindex[%s]: error on %s: %s", kb_id, path, err.cause)
            errors += 1
            note_errors.append(f"{path}: {err.cause}")
            continue
        except Exception as exc:
            logger.warning("kb_reindex[%s]: error on %s: %s", kb_id, path, exc)
            errors += 1
            note_errors.append(f"{path}: {exc}")
            continue

        if outcome.status == "malformed":
            # Malformed frontmatter — lint's problem, not the reindexer's.
            # Remove any prior indexed version of this path: serving stale
            # content after the canonical file became invalid would be less
            # honest than omitting it until a later commit repairs the note.
            logger.warning(
                "kb_reindex[%s]: skipping %s: %s", kb_id, path, outcome.detail
            )
            invalid_paths.append(path)
            skipped += 1
            continue

        upserted += 1
        # Throttled determinate-progress bump (best-effort).
        if upserted % _PROGRESS_BUMP_EVERY == 0:
            try:
                await store.update_index_progress(kb_id, upserted)
            except Exception as exc:
                logger.debug("kb_reindex[%s]: progress bump skipped: %s", kb_id, exc)

    deleted = 0
    for path in sorted(set(delete_paths + invalid_paths)):
        try:
            if await store.delete_kb_note(kb_id, path):
                deleted += 1
        except Exception as exc:
            logger.warning("kb_reindex[%s]: delete error on %s: %s", kb_id, path, exc)
            errors += 1
            note_errors.append(f"{path}: delete failed: {exc}")

    # Reconcile orphaned provisional rows (R-1): pathless active rows the tree can
    # never adopt (failed/squashed commit, slug mismatch, create-then-delete).
    # current_map holds EVERY knowledge file in the tree, so the slug set is
    # complete even on an incremental run. Non-fatal and outside the error count —
    # a hygiene pass must never wedge the watermark or fail the reindex.
    reconciled = 0
    try:
        tree_slugs = [posixpath.basename(p)[: -len(".md")] for p in current_map]
        reconciled = await store.reconcile_orphans(
            project_id=kb_id, tree_slugs=tree_slugs
        )
    except Exception as exc:
        logger.warning("kb_reindex[%s]: reconcile pass skipped: %s", kb_id, exc)

    if errors == 0:
        await store.upsert_watermark(
            kb_id=kb_id,
            repo_name=source_label[:255],
            branch=branch,
            indexed_commit=head,
            pipeline_version=pipeline_version,
            source_head=head,
            status="ready",
            last_error=None,
            advisory=_duplicate_advisory(skipped_duplicates),
        )
        status = "completed"
    else:
        status = "partial"
        fingerprint = _error_fingerprint(note_errors)
        await _set_reindex_status(
            store,
            kb_id,
            "partial",
            source_label=source_label,
            branch=branch,
            source_head=head,
            last_error=f"{errors} note operation(s) failed; retry scheduled",
            error_fingerprint=fingerprint,
        )
        await _warn_if_wedged(store, kb_id, errors, fingerprint)

    # Final progress reflects what durably landed this run so the bar doesn't
    # stall between the last throttled bump and completion (best-effort).
    try:
        await store.update_index_progress(kb_id, upserted, notes_total)
    except Exception as exc:
        logger.debug("kb_reindex[%s]: final progress write skipped: %s", kb_id, exc)

    logger.info(
        "kb_reindex[%s]: %s at %s (full=%s upserted=%d deleted=%d "
        "reconciled=%d skipped=%d skipped_dupes=%d errors=%d)",
        kb_id,
        status,
        head[:12],
        full,
        upserted,
        deleted,
        reconciled,
        skipped,
        len(skipped_duplicates),
        errors,
    )
    return {
        "status": status,
        "indexed_commit": head if errors == 0 else previous_indexed_commit,
        "full": full,
        "upserted": upserted,
        "deleted": deleted,
        "reconciled": reconciled,
        "skipped": skipped,
        "skipped_duplicates": len(skipped_duplicates),
        "errors": errors,
    }


async def resolve_kb_repo(postgres_db: Any, project_id: str) -> Optional[KbRepoRef]:
    """Resolve a project's KB vault location and secret-free auth handle.

    Precedence (knowledge-base/knowledge/features/knowledge_base_repo_separation.md §5):

    1. the project's ``knowledge``-role repo, if it has one;
    2. otherwise its ``jobs`` repo — the vault's original home, and where
       every project created before the separation still keeps it;
    3. ``None`` when it has neither — nothing to index.

    Within a role the first (oldest) repo wins, matching
    ``get_project_repositories``' ``created_at ASC`` ordering. The vault root
    stays ``knowledge/`` either way, so nothing downstream of this needs to
    know which repo it was handed.

    This is deliberately the *only* place that rule is written down: every
    consumer — the sweep below, the backlog mirror, the write path — resolves
    through here, because a second copy is how the reader and the writer come
    to target different repos without anything failing loudly (§10).
    """
    for role in _KB_REPO_ROLES:
        repos = await postgres_db.get_project_repositories(project_id, role=role)
        if repos:
            first = repos[0]
            repo_url = str(first.get("repo_url") or "").strip()
            repo_name = str(first.get("name") or "").strip()
            datasource_ref = (
                await _native_kb_datasource_ref(postgres_db, project_id)
                if role == "knowledge"
                else None
            )
            config = (datasource_ref or {}).get("config") or {}
            override = config.get("forge") if isinstance(config, dict) else None
            forge = (
                _forge_from_repo_url(repo_url, override)
                if role == "knowledge"
                else "gitea"
            )
            owner = ""
            remote_repo = repo_name
            if repo_url:
                try:
                    owner, remote_repo = parse_owner_repo(repo_url)
                except Exception:
                    # Gitea's established client only needs the stored name.
                    # External clients validate owner/repo when selected.
                    if forge != "gitea":
                        raise
            return KbRepoRef(
                forge=forge,
                repo_url=repo_url,
                owner=owner,
                repo=remote_repo,
                branch=str(first.get("branch") or "main"),
                credential_ref=(
                    str(datasource_ref["id"])
                    if datasource_ref and datasource_ref.get("id")
                    else None
                ),
            )
    return None


def _default_purge_enabled() -> bool:
    from orchestrator.services.kb_purge import purge_enabled

    return purge_enabled()


async def _default_purge(**kwargs: Any) -> Dict[str, Any]:
    from orchestrator.services.kb_purge import purge_kb_tick

    return await purge_kb_tick(**kwargs)


def _default_prefilter_enabled() -> bool:
    from orchestrator.services.kb_prefilter import prefilter_enabled

    return prefilter_enabled()


async def _default_prefilter(**kwargs: Any) -> Dict[str, Any]:
    from orchestrator.services.kb_prefilter import prefilter_kb_tick

    return await prefilter_kb_tick(**kwargs)


async def kb_sweep_tick(
    *,
    postgres_db: Any,
    store: Any,
    gitea_client: Any,
    embedding_service: Any,
    reindex_fn: Callable[..., Awaitable[Dict[str, Any]]] = reindex_kb,
    purge_fn: Callable[..., Awaitable[Dict[str, Any]]] = _default_purge,
    purge_enabled_fn: Callable[[], bool] = _default_purge_enabled,
    prefilter_fn: Callable[..., Awaitable[Dict[str, Any]]] = _default_prefilter,
    prefilter_enabled_fn: Callable[[], bool] = _default_prefilter_enabled,
) -> int:
    """One sweep: refresh external datasource and active-project KBs.

    The native work list is every *active* project with a KB-capable repo, one
    KB each; which repo that is comes from ``resolve_kb_repo`` rather than a
    second query, so the sweep cannot read a different repo than the write path
    targets — the silent divergence behind
    ``kb_reindex_watermark_never_advances``. The query only enumerates eligible
    projects; it deliberately does not apply repository precedence itself.

    External sources run first so a long native rebuild cannot delay their
    first attempt. Enumeration and per-KB failures are logged and skipped — one
    broken phase or repo must not starve the rest. Returns the number of KBs
    that actually did work (``up-to-date`` checks don't count).
    """
    worked = 0

    try:
        from orchestrator.services.kb_materialize import (
            retry_knowledge_materialization_intent,
        )

        due = await postgres_db.claim_due_knowledge_materializations(limit=50)
    except Exception:
        logger.exception("kb_sweep: failed to enumerate materialization retries")
        due = []
    if not isinstance(due, (list, tuple)):
        due = []
    for intent in due:
        try:
            await retry_knowledge_materialization_intent(
                postgres_db=postgres_db,
                gitea_client=gitea_client,
                intent=intent,
            )
        except Exception:
            logger.exception(
                "kb_sweep: materialization retry failed for %s/%s",
                intent.get("project_id"),
                intent.get("note_id"),
            )

    # External KB datasources are indexed once under datasources.id, regardless
    # of how many projects/jobs select them. ``list_datasources`` decrypts the
    # credential field only inside this orchestrator process; the source keeps
    # those credentials out of argv, logs, status, and agent payloads.
    try:
        external_rows = await postgres_db.list_datasources(ds_type="kb", limit=10000)
    except Exception:
        logger.exception("kb_sweep: failed to enumerate external KB datasources")
        external_rows = []
    if not isinstance(external_rows, (list, tuple)):
        external_rows = []  # compatibility with narrow/mocked DB facades

    from orchestrator.services.kb_datasources import (
        native_kb_project_id,
        reindex_kb_datasource,
    )

    native_datasources_skipped = 0
    for datasource in external_rows:
        datasource_id = None
        try:
            datasource_id = datasource.get("id")
            # A project's own KB datasource is only a management surface over
            # notes swept below under ``kb_id = project_id``. Indexing it here
            # would duplicate every search hit under the datasource UUID.
            if native_kb_project_id(datasource):
                native_datasources_skipped += 1
                continue

            async def datasource_is_active(
                datasource_id: Any = datasource_id,
            ) -> bool:
                current = await postgres_db.get_datasource(str(datasource_id))
                # The marker is re-read here, not just at enumeration: a row
                # adopted as a project's vault mid-sweep must not have this
                # pass's chunks committed under its datasource id afterwards.
                # That is the double index, arrived at through a race.
                return bool(
                    current
                    and current.get("type") == "kb"
                    and not native_kb_project_id(current)
                )

            result = await reindex_kb_datasource(
                datasource,
                store=store,
                embedding_service=embedding_service,
                is_active=datasource_is_active,
                reindex_fn=reindex_fn,
            )
            if result.get("status") not in (
                "up-to-date",
                "no-head",
                "already-indexing",
                "source-deleted",
            ):
                worked += 1
        except Exception:
            logger.exception(
                "kb_sweep: reindex failed for datasource %s", datasource_id
            )
    if native_datasources_skipped:
        logger.debug(
            "kb_sweep: skipping %d native project KB datasource(s)",
            native_datasources_skipped,
        )

    try:
        rows = await postgres_db.fetch(
            """
            SELECT DISTINCT pr.project_id
            FROM project_repositories AS pr
            JOIN projects AS p ON p.id = pr.project_id
            WHERE pr.role = ANY($1::text[])
              AND p.status = 'active'
            ORDER BY pr.project_id
            """,
            list(_KB_REPO_ROLES),
        )
    except Exception:
        logger.exception("kb_sweep: failed to enumerate active project KBs")
        rows = []
    if not isinstance(rows, (list, tuple)):
        rows = []  # compatibility with narrow/mocked DB facades

    for row in rows:
        project_id = None
        try:
            project_id = row["project_id"]
            # One extra indexed lookup per project per tick (default 900s),
            # against a reindex that costs a Git HEAD fetch at minimum — cheap
            # enough to buy a single repository resolution rule.
            resolved = await resolve_kb_repo(postgres_db, str(project_id))
            if resolved is None:
                # Raced with a repo detach between the enumeration and here.
                continue
            repo_client = await kb_client_for_repo(postgres_db, gitea_client, resolved)
            result = await reindex_fn(
                gitea_client=repo_client,
                store=store,
                embedding_service=embedding_service,
                kb_id=project_id,
                repo_name=resolved.repo,
                branch=resolved.branch,
            )
            if result.get("status") in ("completed", "up-to-date"):
                try:
                    await postgres_db.mark_knowledge_projections_synced(str(project_id))
                except Exception:
                    logger.exception(
                        "kb_sweep: projection ledger update failed for project %s",
                        project_id,
                    )
                # Gardening lanes (kb_gardening G7/G2): only after this KB's
                # index is known to match its tree, so the candidate queries
                # see current statuses and links, and only when an operator
                # turned each lane on. Prefilter (archive unreachable nursery)
                # runs before purge (remove long-archived files); the purge
                # lane's own grace keeps the two from ever touching the same
                # note in one tick.
                if prefilter_enabled_fn():
                    try:
                        archived = await prefilter_fn(
                            postgres_db=postgres_db,
                            store=store,
                            gitea_client=repo_client,
                            kb_id=project_id,
                        )
                        if archived.get("archived"):
                            worked += 1
                    except Exception:
                        logger.exception(
                            "kb_sweep: prefilter failed for project %s", project_id
                        )
                if purge_enabled_fn():
                    try:
                        purged = await purge_fn(
                            postgres_db=postgres_db,
                            store=store,
                            gitea_client=repo_client,
                            kb_id=project_id,
                        )
                        if purged.get("purged"):
                            worked += 1
                    except Exception:
                        logger.exception(
                            "kb_sweep: purge failed for project %s", project_id
                        )
            if result.get("status") not in ("up-to-date", "no-head"):
                worked += 1
        except Exception:
            logger.exception("kb_sweep: reindex failed for project %s", project_id)
    return worked


async def kb_reindex_sweeper_loop(
    postgres_db: Any,
    store: Any,
    gitea_client: Any,
    shutdown_event: asyncio.Event,
    *,
    embedding_service_factory: Callable[[], Awaitable[Any]],
) -> None:
    """Periodic KB index freshness sweep (leader-gated by the caller).

    Mirrors ``project_loop_sweeper_loop``'s tick + shutdown-aware wait. The
    embedding service is re-resolved every tick via the injected factory so
    catalog changes (Admin → Models) take effect without a restart; a tick with
    no resolvable embedding service is skipped loudly — a keyless reindex could
    only write vectorless rows, and honesty beats coverage.
    """
    logger.info(
        "KB reindex sweeper started (tick=%ds, first sweep in %ds)",
        SWEEP_TICK_SECONDS,
        FIRST_SWEEP_DELAY_SECONDS,
    )
    # The first sweep is deliberately not a full tick away. Indexing is an
    # in-process task, so every orchestrator rollout kills whatever was running;
    # this loop is the only thing that restarts it, and at one tick of latency a
    # KB that a deploy interrupted stayed dead for 15 minutes. The short initial
    # delay still lets startup settle (leader election, embedding catalog) before
    # the first pass.
    delay = FIRST_SWEEP_DELAY_SECONDS
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=delay)
            break  # shutdown requested
        except asyncio.TimeoutError:
            pass  # tick due
        delay = SWEEP_TICK_SECONDS
        try:
            svc = await embedding_service_factory()
            if svc is None:
                logger.warning(
                    "kb_sweep: no embedding service resolvable (catalog or env) "
                    "— skipping tick"
                )
                continue
            worked = await kb_sweep_tick(
                postgres_db=postgres_db,
                store=store,
                gitea_client=gitea_client,
                embedding_service=svc,
            )
            if worked:
                logger.info("kb_sweep: %d KB(s) reindexed", worked)
        except Exception:
            logger.exception("kb_sweep: tick failed")
    logger.info("KB reindex sweeper stopped")
