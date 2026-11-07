"""Server-side materialisation of ONE knowledge note into a project's KB repo.

Step 3 of ``knowledge-base/knowledge/features/knowledge_base_repo_separation.md``. The agent used
to materialise every note as ``knowledge/<slug>.md`` inside its own workspace
checkout (``_dual_write_note``), which welded the vault to the jobs repo and,
because it was guarded on ``has_git()``, skipped entirely for anything without
git — persistent sessions, lite tiers, repo-less projects — leaving those
notes pathless and therefore invisible to ``kb_read`` / ``kb_search``. The
write moves here instead: one note, one Gitea commit, into whichever repo §5's
``resolve_kb_repo`` picks for the project.

Nothing downstream changes. Files stay canonical, the reindexer still ingests
``knowledge/**/*.md`` from git on its next sweep and sets ``path``, and reads
keep their ``path IS NOT NULL`` gate. Only the *writer* moved, from the agent's
workspace checkout to a commit the orchestrator makes.

By contract this **never raises for an expected condition**. Every outcome —
including "there is no repo" and "Gitea refused" — comes back as a status dict,
but mutation callers must fail closed unless that result proves the desired
bytes crossed the canonical repository boundary. The durable intent ledger
keeps a failed write visible and retryable instead of allowing an index-only
success.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

import yaml

from orchestrator.services.kb_forge import kb_client_for_repo
from shared.runtime.knowledge.gardener import parse_note_md

# The vault prefix and the repo resolution are both owned by the reindexer:
# the writer and the sweep MUST agree on repo and path or they silently
# diverge (that is the shape of kb_reindex_watermark_never_advances).
from orchestrator.services.kb_reindex import (
    KNOWLEDGE_PREFIX,
    index_single_note,
    resolve_kb_repo,
)
from shared.runtime.knowledge.chunker import embedding_version_for_service

logger = logging.getLogger(__name__)

# A slug becomes a repository path (``knowledge/<slug>.md``) and is authored by
# an LLM, so it is untrusted input to a path join. Anything with a separator,
# a leading dot or a traversal segment is refused rather than sanitised —
# silently rewriting a slug would make the file's stem disagree with the
# ``knowledge_index`` row's ``note_id``, and the reindexer matches them by
# stem. Underscores are common in live note ids, hence the permissive body.
_SAFE_SLUG = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._-]{0,199}$")

# OKF reserved basenames: generated navigation/history that the reindexer
# never indexes (``kb_reindex._RESERVED_BASENAMES``). A note written to one of
# these would clobber generated content AND be permanently invisible to every
# reader, so it is refused at the door instead.
_RESERVED_SLUGS = frozenset({"index", "log"})

_STATUS_COMMITTED = "committed"
_STATUS_SKIPPED = "skipped"
_STATUS_FAILED = "failed"

# Chunks a note may have and still be indexed inside the write request.
# 64 = DEFAULT_MAX_EMBED_BATCH (src/tools/knowledge/chunker.py): at or under
# it, embed_note_chunks sends exactly one embedding request, so the cap means
# "index inline costs one round-trip" — the real boundary this guard exists
# to hold. Measured against this repo's own 665-note OKF vault with the real
# chunker: median 11 chunks, mean 16.2, p75 21, p90 36, p95 47, p99 80, max
# 227 — so cap=64 defers only the pathological tail (17/665 notes, 2.6%; the
# 127 KB curator dump that chunks to ~95) rather than the majority of writes
# a lower cap deferred. Over it the note commits and the sweep indexes it.
_INLINE_MAX_CHUNKS_DEFAULT = 64


def _inline_max_chunks() -> int:
    """The chunk cap from the environment, never a boot failure.

    This runs at import. A typo in the Helm value (``"64 "``, ``"sixty-four"``,
    an accidental quote) would otherwise raise ``ValueError`` during module
    import and crash-loop the orchestrator — in the one module whose entire
    premise is that an index failure never fails a write. A bad value costs
    the deployment a tuned cap, which is a log line; it must not cost it a
    boot.

    Only an unparseable value falls back. A parseable but odd one (0, a
    negative) is left alone on purpose: it is expressible, its effect is
    "defer every write to the sweep", and overriding it here would take away
    an operator's kill switch on inline indexing.
    """
    raw = os.getenv("KB_INLINE_INDEX_MAX_CHUNKS")
    if raw is None or not str(raw).strip():
        return _INLINE_MAX_CHUNKS_DEFAULT
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        logger.warning(
            "kb-materialize: KB_INLINE_INDEX_MAX_CHUNKS=%r is not an integer "
            "— falling back to %d",
            raw,
            _INLINE_MAX_CHUNKS_DEFAULT,
        )
        return _INLINE_MAX_CHUNKS_DEFAULT


_INLINE_MAX_CHUNKS = _inline_max_chunks()


def note_repo_path(slug: str) -> str:
    """The vault-relative path a note materialises to."""
    return f"{KNOWLEDGE_PREFIX}{slug}.md"


def slug_error(slug: str) -> Optional[str]:
    """Operator-readable reason the slug is unusable as a path, else None."""
    candidate = str(slug or "").strip()
    if not candidate:
        return "empty note slug"
    if not _SAFE_SLUG.match(candidate):
        return (
            f"unsafe note slug {candidate!r} — a slug becomes the path "
            f"{KNOWLEDGE_PREFIX}<slug>.md and may only contain letters, "
            "digits, '.', '_' and '-' (no path separators, no leading dot)"
        )
    if candidate.lower() in _RESERVED_SLUGS:
        return (
            f"reserved note slug {candidate!r} — {KNOWLEDGE_PREFIX}"
            f"{candidate}.md is generated OKF navigation and is never indexed"
        )
    return None


def _git_blob_sha(data: bytes) -> str:
    """The git object id this content would have as a blob.

    Lets an unchanged rewrite be recognised from the tree listing we already
    fetched, with no extra API call. A wrong answer here can only ever produce
    a redundant commit (it degrades to "always write"), never a skipped one.
    """
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()  # noqa: S324


def _commit_message(slug: str, job_id: Optional[str]) -> str:
    """Commit text naming the note and the job that wrote it.

    §3's *"which job changed what note when"*: the subject carries the short
    id for ``git log --oneline`` readability, the body the full UUID so
    attribution stays machine-recoverable.
    """
    job = str(job_id or "").strip()
    if not job:
        return f"kb: {slug}"
    return f"kb: {slug} (job {job[:8]})\n\njob: {job}"


def _result(
    status: str,
    *,
    reason: Optional[str] = None,
    repo: Optional[str] = None,
    branch: Optional[str] = None,
    path: Optional[str] = None,
    operation: Optional[str] = None,
    canonical_state: Optional[str] = None,
    retry_state: Optional[str] = None,
    recorded: bool = True,
) -> dict[str, Any]:
    """Base result shape every materialiser outcome extends.

    ``canonical_state``/``retry_state`` default to ``None`` like the other
    optional fields — the durable-intent wrapper (``_finish_attempt``)
    overwrites both once an intent exists, so a caller that does not pass
    them keeps today's shape exactly. ``recorded`` defaults ``True`` because
    every existing caller of this function runs *after*
    ``begin_knowledge_materialization`` already opened an intent row; a
    caller returning before that call (a repo-less project can never satisfy
    the intent, so none is opened) must pass ``recorded=False`` explicitly.
    """
    return {
        "status": status,
        "reason": reason,
        "repo": repo,
        "branch": branch,
        "path": path,
        "operation": operation,
        "canonical_state": canonical_state,
        "retry_state": retry_state,
        "recorded": recorded,
    }


async def _path_exists(
    gitea_client: Any, repo_name: str, branch: str, path: str
) -> Tuple[Optional[bool], Optional[str]]:
    """``(exists, blob_sha)`` for ``path`` on ``branch``; ``(None, None)``
    when the tree could not be read at all.

    Same probe as the curated merge (``services.project_loops``): read the
    target ref's tree once and decide the ``change_files`` operation against
    it, because ``create`` on an existing path is a Gitea 422. ``None`` is
    deliberately distinct from ``False`` — an unreadable tree is a guess, and
    the caller flips the operation on refusal rather than pretending it knew.
    """
    try:
        tree = await gitea_client.list_tree(repo_name, branch)
    except Exception as e:  # noqa: BLE001 — a probe failure is not fatal
        logger.warning(
            "kb-materialize: tree read raised for %s@%s: %r", repo_name, branch, e
        )
        return None, None
    if tree is None:
        return None, None
    for entry in tree:
        if entry.get("type") == "blob" and str(entry.get("path")) == path:
            return True, entry.get("sha")
    return False, None


@dataclass(frozen=True)
class _NoteLocation:
    """Where a note lives on the branch, as far as the tree shows.

    ``exists`` is ``None`` when the tree could not be read (the path is then
    the flat guess); ``ambiguous`` lists the candidates when several files
    carry the note's name and nothing else tells them apart.
    """

    path: str
    exists: Optional[bool]
    blob_sha: Optional[str]
    ambiguous: Tuple[str, ...] = ()


def _note_stem(path: str) -> str:
    name = path.rsplit("/", 1)[-1]
    return name[:-3] if name.endswith(".md") else name


async def _resolve_note_path(
    repo_client: Any,
    repo_name: str,
    branch: str,
    slug: str,
    *,
    expected_blob_sha: Optional[str] = None,
    path_hint: Optional[str] = None,
) -> _NoteLocation:
    """Find the file that holds note ``slug`` anywhere under ``knowledge/``.

    Writes used to go to ``knowledge/<slug>.md`` unconditionally, but a vault
    keeps most hand-written notes in subfolders (86 % of the SRW design vault
    on 2026-11-04). An update of such a note was refused forever, or, with no
    compare-and-swap token, forked a second file with the same id beside it
    (G14). Resolution order, first match wins:

    1. ``path_hint``: the indexed path of the note, supplied by a server-side
       caller that read the row. Used only if the tree holds that file.
    2. The blob the caller read (``expected_blob_sha``): pins the exact file
       even when the note's id is not its filename.
    3. The flat path ``knowledge/<slug>.md``.
    4. The single file whose name is ``<slug>.md`` in any subfolder. Several
       such files with nothing else to choose between them are ``ambiguous``
       and the caller must refuse rather than guess.

    Nothing found means the note does not exist yet: the flat path, where a
    new note is created.
    """
    flat = note_repo_path(slug)
    try:
        tree = await repo_client.list_tree(repo_name, branch)
    except Exception as e:  # noqa: BLE001 — a probe failure is not fatal
        logger.warning(
            "kb-materialize: tree read raised for %s@%s: %r", repo_name, branch, e
        )
        return _NoteLocation(flat, None, None)
    if tree is None:
        return _NoteLocation(flat, None, None)

    blobs: dict[str, Optional[str]] = {}
    for entry in tree:
        entry_path = str(entry.get("path") or "")
        if (
            entry.get("type") == "blob"
            and entry_path.startswith(KNOWLEDGE_PREFIX)
            and entry_path.endswith(".md")
        ):
            blobs[entry_path] = entry.get("sha")

    hint = str(path_hint or "").strip()
    if hint and hint in blobs:
        return _NoteLocation(hint, True, blobs[hint])

    expected = str(expected_blob_sha or "").strip()
    if expected:
        same_blob = [path for path, sha in blobs.items() if sha == expected]
        if flat in same_blob:
            return _NoteLocation(flat, True, expected)
        named = [path for path in same_blob if _note_stem(path) == slug]
        if len(named) == 1:
            return _NoteLocation(named[0], True, expected)
        if len(same_blob) == 1:
            return _NoteLocation(same_blob[0], True, expected)

    if flat in blobs:
        return _NoteLocation(flat, True, blobs[flat])
    named = sorted(path for path in blobs if _note_stem(path) == slug)
    if len(named) == 1:
        return _NoteLocation(named[0], True, blobs[named[0]])
    if named:
        return _NoteLocation(flat, False, None, ambiguous=tuple(named))
    return _NoteLocation(flat, False, None)


def _ambiguous_result(slug: str, location: _NoteLocation, **fields: Any) -> dict:
    logger.error(
        "kb-materialize: refusing to change note %r — %d files carry its name "
        "(%s); give one an explicit frontmatter id or rename it",
        slug,
        len(location.ambiguous),
        ", ".join(location.ambiguous[:5]),
    )
    return _result(_STATUS_FAILED, reason="ambiguous-note-path", **fields)


async def _materialize_knowledge_note_once(
    *,
    postgres_db: Any,
    gitea_client: Any,
    project_id: str,
    slug: str,
    content: str,
    job_id: Optional[str] = None,
    resolved_repo: Any = None,
    repo_client: Any = None,
    expected_blob_sha: Optional[str] = None,
    mode: Optional[str] = None,
    path_hint: Optional[str] = None,
) -> dict[str, Any]:
    """Commit one rendered note to the project's KB repo, at the note's path.

    The path is the file that already holds the note, wherever it sits under
    ``knowledge/`` (:func:`_resolve_note_path`), or ``knowledge/<slug>.md``
    for a new note.

    Args:
        postgres_db: App-DB facade, passed straight to ``resolve_kb_repo``.
        gitea_client: ``GiteaClient`` (or any object with ``list_tree`` /
            ``change_files``).
        project_id: Project whose KB repo receives the note.
        slug: Note id — the file stem the reindexer matches rows on.
        content: Fully rendered OKF markdown. Rendering stays with the caller;
            the orchestrator does not know OKF's note format.
        job_id: Writing job, for per-job commit attribution (§3). Optional —
            persistent sessions have no job.
        expected_blob_sha: Compare-and-swap token (kb_gardening G3). The blob
            SHA the caller read before deciding to rewrite; when given, the
            write is refused with ``failed/precondition-failed`` if the tree
            holds a different blob at the path **or no blob at all** (the file
            was removed under the caller — without this check a stale
            ``kb_update`` silently re-creates a deleted note). A concurrent
            writer therefore fails loudly instead of being overwritten.
            ``None`` keeps the historical last-writer-wins behaviour.
        mode: ``"create"`` refuses (``failed/already-exists``, permanent) when
            the note already exists, so a create can never overwrite a note it
            did not read (G15). ``"update"`` refuses
            (``failed/precondition-failed``) when the note is gone instead of
            re-creating it. ``None`` is the historical upsert, kept for agents
            that predate the field.
        path_hint: The note's indexed path, from a server-side caller that read
            its row (see :func:`_resolve_note_path`).

    Returns:
        ``{status, reason, repo, branch, path, operation}``. ``status`` is one
        of:

        * ``committed`` — the note is on the branch as of this call.
        * ``skipped`` — nothing was written and nothing needed to be:
          ``unchanged`` (the identical bytes are already committed; writing
          again would add a no-op commit to the very history §3 wants to be
          readable).
        * ``failed`` — the note is NOT materialised. ``reason`` is one of
          ``invalid-slug``, ``empty-content``, ``resolve-error``,
          ``no-repo``, ``commit-refused``, ``commit-error``. ``no-repo`` (the
          project has no Gitea repo at all) is **permanent** — a repo-less
          project cannot grow one by retrying, so the caller
          (``_materialize_note_canonical``) resolves the repo *before*
          opening a durable intent and returns this outcome unrecorded; this
          branch only fires if that early resolution raised and a same-call
          retry then answers definitively "no repo".

        Never raises. A ``failed`` result means canonical durability was not
        established; callers must not mutate the searchable projection or
        report READY/updated/closed. The enclosing durable intent supplies the
        retry path.
    """
    bad_slug = slug_error(slug)
    if bad_slug:
        logger.error(
            "kb-materialize: refusing note for project %s — %s", project_id, bad_slug
        )
        return _result(_STATUS_FAILED, reason="invalid-slug")

    slug = str(slug).strip()
    path = note_repo_path(slug)

    if not str(content or "").strip():
        logger.error(
            "kb-materialize: refusing empty body for %s (project %s)", path, project_id
        )
        return _result(_STATUS_FAILED, reason="empty-content", path=path)

    resolved = resolved_repo
    if resolved is None:
        try:
            resolved = await resolve_kb_repo(postgres_db, project_id)
        except Exception as e:  # noqa: BLE001 — resolution is not the caller's problem
            logger.error(
                "kb-materialize: KB repo resolution failed for project %s: %r",
                project_id,
                e,
                exc_info=True,
            )
            return _result(_STATUS_FAILED, reason="resolve-error", path=path)

    if not resolved:
        # No jobs repo and no knowledge repo — a repo-less project can never
        # satisfy the intent by retrying. The common case never reaches this
        # line at all: ``_materialize_note_canonical`` resolves before
        # opening the intent and returns the unrecorded/permanent outcome
        # itself. This is the rare fallback where that early resolution
        # raised and this same-call retry (with an intent already open)
        # answers definitively "no repo" — permanent, but recorded, since an
        # intent row does exist by this point.
        logger.info(
            "kb-materialize: project %s has no KB repo — refusing %s permanently",
            project_id,
            path,
        )
        return _result(_STATUS_FAILED, reason="no-repo", path=path)

    repo_name, branch = resolved.repo, resolved.branch
    branch = branch or "main"
    body = str(content).encode("utf-8")

    if repo_client is None:
        try:
            repo_client = await kb_client_for_repo(postgres_db, gitea_client, resolved)
        except Exception:  # noqa: BLE001 — credential/config failures stay non-fatal
            logger.error(
                "kb-materialize: no usable %s client for project %s",
                resolved.forge,
                project_id,
                exc_info=True,
            )
            return _result(
                _STATUS_FAILED,
                reason="client-error",
                repo=repo_name,
                branch=branch,
                path=path,
            )

    location = await _resolve_note_path(
        repo_client,
        repo_name,
        branch,
        slug,
        expected_blob_sha=expected_blob_sha,
        path_hint=path_hint,
    )
    if location.ambiguous:
        return _ambiguous_result(
            slug, location, repo=repo_name, branch=branch, path=path
        )
    path, exists, blob_sha = location.path, location.exists, location.blob_sha
    expected = str(expected_blob_sha or "").strip() or None
    if mode == "create" and exists:
        logger.info(
            "kb-materialize: create of %s refused — the note already exists at %s",
            slug,
            path,
        )
        return _result(
            _STATUS_FAILED,
            reason="already-exists",
            repo=repo_name,
            branch=branch,
            path=path,
        )
    if mode == "update" and exists is False and not expected:
        return _result(
            _STATUS_FAILED,
            reason="precondition-failed",
            repo=repo_name,
            branch=branch,
            path=path,
        )
    if expected:
        if exists is None:
            # A guess is fine for an unconditional write (it costs one retry);
            # a conditional one must not proceed on a tree it could not read.
            return _result(
                _STATUS_FAILED,
                reason="tree-unreadable",
                repo=repo_name,
                branch=branch,
                path=path,
            )
        if not exists or (blob_sha and blob_sha != expected):
            logger.info(
                "kb-materialize: write of %s refused — caller read %s, tree holds %s",
                path,
                expected[:12],
                (str(blob_sha)[:12] if exists else "nothing"),
            )
            return _result(
                _STATUS_FAILED,
                reason="precondition-failed",
                repo=repo_name,
                branch=branch,
                path=path,
            )
    if exists and blob_sha and blob_sha == _git_blob_sha(body):
        logger.debug(
            "kb-materialize: %s@%s already holds %s byte-for-byte — no commit",
            repo_name,
            branch,
            path,
        )
        return _result(
            _STATUS_SKIPPED,
            reason="unchanged",
            repo=repo_name,
            branch=branch,
            path=path,
        )

    # ``create`` on an existing path is a 422 and ``update`` on a missing one
    # is refused too, so the operation is chosen per file against the target
    # tree — the curated merge's rule (services.project_loops). Two cases the
    # tree cannot settle: it was unreadable (exists is None), or a concurrent
    # writer changed the path between the probe and the commit. Both surface
    # identically as a refusal, so a refusal is retried ONCE with the opposite
    # operation. §10: a missed materialisation leaves the note invisible to
    # every reader, which is worth one extra call to avoid.
    operation = "update" if exists else "create"
    message = _commit_message(slug, job_id)
    payload = {"path": path, "content_b64": base64.b64encode(body).decode("ascii")}
    if blob_sha:
        # GitHub's contents API requires the current blob SHA for an update;
        # Gitea enforces it too when present (422 on mismatch). With an
        # ``expected_blob_sha`` this is what makes the check atomic: the tree
        # probe above catches a writer that read long ago, the forge catches
        # two writers whose probes both preceded either commit (E4, S2).
        payload["sha"] = blob_sha

    # A conditional write is never flipped to the opposite operation: the
    # refusal IS the answer (the file moved), and retrying as ``create`` on an
    # existing path would only manufacture a second refusal. Neither is an
    # explicit create or update: flipping a refused create into an update is
    # exactly how a create overwrites a note it never read.
    attempts = (
        (operation,)
        if expected or mode in ("create", "update")
        else (operation, "create" if operation == "update" else "update")
    )
    for attempt, op in enumerate(attempts):
        try:
            committed = await repo_client.change_files(
                repo_name, branch, [{**payload, "operation": op}], message=message
            )
        except Exception as e:  # noqa: BLE001 — non-fatal by contract
            logger.error(
                "kb-materialize: commit of %s to %s@%s raised: %r",
                path,
                repo_name,
                branch,
                e,
                exc_info=True,
            )
            return _result(
                _STATUS_FAILED,
                reason="commit-error",
                repo=repo_name,
                branch=branch,
                path=path,
                operation=op,
            )
        if committed:
            if attempt:
                logger.info(
                    "kb-materialize: %s landed on the '%s' retry (tree said %s)",
                    path,
                    op,
                    exists,
                )
            logger.info(
                "kb-materialize: %s → %s@%s (%s%s)",
                path,
                repo_name,
                branch,
                op,
                f", job {str(job_id)[:8]}" if job_id else "",
            )
            return _result(
                _STATUS_COMMITTED,
                repo=repo_name,
                branch=branch,
                path=path,
                operation=op,
            )

    if mode == "create" and not expected:
        return _result(
            _STATUS_FAILED,
            reason="already-exists",
            repo=repo_name,
            branch=branch,
            path=path,
            operation=operation,
        )
    if expected or mode == "update":
        logger.info(
            "kb-materialize: forge refused conditional write of %s@%s — the "
            "blob moved between the caller's read and its write",
            path,
            branch,
        )
        return _result(
            _STATUS_FAILED,
            reason="precondition-failed",
            repo=repo_name,
            branch=branch,
            path=path,
            operation=operation,
        )
    logger.error(
        "kb-materialize: Gitea refused %s on %s@%s as both '%s' and its "
        "opposite — the note is in the index but NOT readable until it is "
        "rewritten",
        path,
        repo_name,
        branch,
        operation,
    )
    return _result(
        _STATUS_FAILED,
        reason="commit-refused",
        repo=repo_name,
        branch=branch,
        path=path,
        operation=operation,
    )


#: ``skipped`` reasons that still prove the desired state is on the branch:
#: the identical bytes are already there (write), or the path is already gone
#: (delete). Both are "nothing needed to be done", not "nothing was done".
_CANONICAL_SKIP_REASONS = frozenset({"unchanged", "absent"})


async def _finish_attempt(
    *,
    postgres_db: Any,
    intent: dict[str, Any],
    outcome: dict[str, Any],
    permanent: bool,
) -> dict[str, Any]:
    canonical = outcome.get("status") == _STATUS_COMMITTED or (
        outcome.get("status") == _STATUS_SKIPPED
        and outcome.get("reason") in _CANONICAL_SKIP_REASONS
    )
    intent_id = str(intent["id"])
    token = intent.get("attempt_token")
    try:
        # Named for what it is — the ledger's post-write row — not "recorded",
        # which is a distinct boolean already carried on `outcome` (whether an
        # intent was ever opened at all; see `_result`).
        ledger_row = await postgres_db.finish_knowledge_materialization(
            intent_id,
            canonical=canonical,
            permanent=permanent,
            reason=str(outcome.get("reason") or "") or None,
            error=str(outcome.get("reason") or "") or None,
            repo=outcome.get("repo"),
            branch=outcome.get("branch"),
            path=outcome.get("path"),
            operation=outcome.get("operation"),
            attempt_token=str(token) if token else None,
        )
    except Exception as exc:
        logger.error(
            "kb-materialize: result ledger update failed for intent %s: %r",
            intent_id,
            exc,
            exc_info=True,
        )
        return {
            **outcome,
            "status": _STATUS_FAILED,
            "reason": "intent-result-store-error",
            "intent_id": intent_id,
            "canonical_state": "pending_sync",
            "projection_state": "projection_only",
            "retry_state": "retryable",
        }
    if ledger_row is None:
        return {
            **outcome,
            "status": _STATUS_FAILED,
            "reason": "intent-lease-lost",
            "intent_id": intent_id,
            "canonical_state": "pending_sync",
            "projection_state": "pending",
            "retry_state": "retryable",
        }
    return {
        **outcome,
        "intent_id": intent_id,
        "canonical_state": ledger_row.get("canonical_state")
        or ("canonical" if canonical else "pending_sync"),
        "projection_state": ledger_row.get("projection_state") or "pending",
        "retry_state": ledger_row.get("retry_state")
        or ("none" if canonical else "retryable"),
    }


async def _attempt_content_intent(
    *,
    postgres_db: Any,
    gitea_client: Any,
    intent: dict[str, Any],
    project_id: str,
    slug: str,
    content: str,
    job_id: Optional[str],
    expected_blob_sha: Optional[str] = None,
    resolved_repo: Any = None,
    mode: Optional[str] = None,
) -> dict[str, Any]:
    permanent_reason: str | None = None
    if slug_error(slug):
        permanent_reason = "invalid-slug"
    elif not content.strip():
        permanent_reason = "empty-content"
    else:
        try:
            frontmatter, _ = parse_note_md(content)
            if frontmatter is None:
                permanent_reason = "malformed-frontmatter"
            elif str(frontmatter.get("id") or "") != str(slug).strip():
                permanent_reason = "frontmatter-id-mismatch"
        except ValueError:
            permanent_reason = "malformed-frontmatter"

    if permanent_reason is not None:
        outcome = _result(_STATUS_FAILED, reason=permanent_reason)
    else:
        try:
            outcome = await _materialize_knowledge_note_once(
                postgres_db=postgres_db,
                gitea_client=gitea_client,
                project_id=project_id,
                slug=slug,
                content=content,
                job_id=job_id,
                expected_blob_sha=expected_blob_sha,
                resolved_repo=resolved_repo,
                mode=mode,
            )
        except Exception as exc:
            logger.error(
                "kb-materialize: unexpected materializer exception for %s/%s: %r",
                project_id,
                slug,
                exc,
                exc_info=True,
            )
            outcome = _result(_STATUS_FAILED, reason="materializer-exception")
    # ``precondition-failed`` is permanent by design: the caller decided on a
    # version that is no longer there, so replaying its bytes later would be
    # exactly the lost update the token exists to prevent. It must re-read.
    # ``no-repo`` is permanent for the same reason a repo-less project always
    # is (WP5.3) — reached here only via the rare fallback documented on
    # ``_materialize_knowledge_note_once``'s own no-repo branch.
    # ``already-exists`` and ``ambiguous-note-path`` are decisions about the
    # tree, not hiccups: replaying the same bytes cannot change either answer.
    permanent = permanent_reason is not None or outcome.get("reason") in {
        "invalid-slug",
        "empty-content",
        "precondition-failed",
        "no-repo",
        "already-exists",
        "ambiguous-note-path",
    }
    return await _finish_attempt(
        postgres_db=postgres_db,
        intent=intent,
        outcome=outcome,
        permanent=permanent,
    )


def _is_canonical(result: dict[str, Any]) -> bool:
    """Whether the bytes are proven to be on the canonical branch.

    The two ``skipped`` reasons are in here **deliberately**, and narrowing
    this to ``committed`` alone would be a regression, not a tightening. They
    are the retry-heals-a-deferred-index path: when a write commits but its
    inline index defers (lock held, oversized, index error), the note is
    canonical and unsearchable, and the natural repair is to write it again.
    That second write finds the identical bytes already on the branch
    (``skipped/unchanged``) or the intent already settled
    (``skipped/already-canonical``) — so if either were treated as
    non-canonical the retry would answer ``not-canonical`` and skip indexing,
    leaving the note invisible until the next sweep, which is exactly the
    900-second gap this slice exists to close. "Nothing was written" and
    "nothing needed to be written" are different facts; only the first is a
    reason to withhold the index.
    """
    if result.get("canonical_state") == "canonical":
        return True
    return result.get("status") == _STATUS_COMMITTED or (
        result.get("status") == _STATUS_SKIPPED
        and result.get("reason") in {"unchanged", "absent", "already-canonical"}
    )


async def _index_note_inline(
    *,
    postgres_db: Any,
    store: Any,
    embedding_service: Any,
    project_id: str,
    slug: str,
    content: str,
    intent_id: Optional[str],
    retrieval_messages: Optional[list[str]] = None,
    job_id: Optional[str] = None,
    path: Optional[str] = None,
) -> dict[str, Any]:
    """Index a just-committed note so it is searchable when the write returns.

    Slice A. Without this the row is pathless and chunkless until the next
    sweep, which is post-job or up to 900s away — so an agent could not read
    back the note it had just written.

    Deliberately narrow: one note, no deletes, no reconcile, and never a
    watermark write. The blob SHA stamped here is the one we just committed,
    so the next sweep's tree diff skips this note instead of re-embedding it.

    ``retrieval_messages`` forwards straight to :func:`index_single_note`;
    ``None`` leaves whatever is already stored alone (see that function and
    ``KnowledgeStore.upsert_kb_note``).

    ``job_id`` arrives as the request's string and becomes
    ``knowledge_index.job_id`` (a UUID column), so it is parsed here, at the
    boundary where strings stop. An unparseable one costs the note its
    provenance, not its index: the commit has already happened and a note
    nobody can find is far worse than one whose author is unknown.

    Never raises. Every failure degrades to "committed but not yet indexed",
    which is exactly the state the sweep already knows how to heal.

    ``path`` is where the commit landed (a nested note stays in its folder);
    omitted, the flat ``knowledge/<slug>.md``.
    """
    path = str(path or "").strip() or note_repo_path(slug)
    try:
        kb_id = uuid.UUID(str(project_id))
    except (TypeError, ValueError):
        return {"indexed": False, "index_reason": "index-error"}

    writing_job: Optional[uuid.UUID] = None
    if job_id:
        try:
            writing_job = uuid.UUID(str(job_id))
        except (TypeError, ValueError):
            logger.warning(
                "kb-materialize: %s has an unparseable job_id %r — indexing "
                "without provenance",
                path,
                job_id,
            )

    blob_sha = _git_blob_sha(str(content).encode("utf-8"))
    try:
        embedding_stamp = embedding_version_for_service(embedding_service)
        # Non-blocking: if the sweep owns this KB we defer rather than hold a
        # request open behind a full-vault rebuild. The sweep will index the
        # note in the pass that is already running.
        async with store.try_reindex_lock(kb_id) as claimed:
            if not claimed:
                # The reason token is interpolated verbatim so an operator can
                # grep the logs for the exact string the tool put in the
                # transcript (`indexed=deferred:reindex-running`).
                logger.info(
                    "kb-materialize: %s committed but deferred:%s — KB %s "
                    "index lock held (sweep or concurrent write)",
                    path,
                    "reindex-running",
                    kb_id,
                )
                return {"indexed": False, "index_reason": "reindex-running"}
            outcome = await index_single_note(
                store=store,
                embedding_service=embedding_service,
                kb_id=kb_id,
                path=path,
                text=content,
                blob_sha=blob_sha,
                embedding_stamp=embedding_stamp,
                max_chunks=_INLINE_MAX_CHUNKS,
                retrieval_messages=retrieval_messages,
                job_id=writing_job,
            )
    except Exception as e:  # noqa: BLE001 — non-fatal by contract
        logger.error(
            "kb-materialize: inline index of %s failed — deferred:%s: %r — "
            "committed but not searchable until the next sweep",
            path,
            "index-error",
            e,
            exc_info=True,
        )
        return {"indexed": False, "index_reason": "index-error"}

    if outcome.status != "indexed":
        logger.warning(
            "kb-materialize: %s committed but deferred:%s — not indexed "
            "inline (%d chunks)",
            path,
            outcome.status,
            outcome.chunks,
        )
        return {"indexed": False, "index_reason": outcome.status}

    if intent_id:
        try:
            await postgres_db.finish_knowledge_projection(
                intent_id, project_id=str(project_id), synced=True, error=None
            )
        except Exception as e:  # noqa: BLE001 — canonical truth already persisted
            logger.warning(
                "kb-materialize: projection ledger close failed for intent %s: %r",
                intent_id,
                e,
            )
    return {"indexed": True, "index_reason": None}


async def _materialize_note_canonical(
    *,
    postgres_db: Any,
    gitea_client: Any,
    project_id: str,
    slug: str,
    content: str,
    job_id: Optional[str] = None,
    expected_blob_sha: Optional[str] = None,
    mode: Optional[str] = None,
) -> dict[str, Any]:
    """Persist intent, cross canonical git, and return both truth legs.

    ``expected_blob_sha`` guards the *first* attempt only. The ledger does
    not carry it, so a sweep retry of a deferred intent runs unconditionally
    — the same replay behaviour as before this token existed. A refused first
    attempt is recorded as permanent, so it is never replayed at all.

    A repo-less project is resolved *before* any intent is opened (WP5.3): it
    can never satisfy the intent by retrying, so nothing should be durably
    recorded to retry. ``resolve_kb_repo`` raising here is left to the normal
    attempt below instead of being decided on the spot — that failure is
    transient (DB/forge hiccup, not "no repo"), and moving it in front of the
    intent would cost it the durable, ledger-tracked retry it has today.
    """
    content_text = str(content or "")
    content_hash = hashlib.sha256(content_text.encode("utf-8")).hexdigest()
    path = note_repo_path(str(slug or "").strip())

    resolved_repo: Any = None
    if slug_error(slug) is None and content_text.strip():
        try:
            resolved_repo = await resolve_kb_repo(postgres_db, project_id)
        except Exception:  # noqa: BLE001 — re-decided by the normal attempt below
            resolved_repo = None
        else:
            if not resolved_repo:
                logger.info(
                    "kb-materialize: project %s has no KB repo — refusing %s "
                    "permanently",
                    project_id,
                    path,
                )
                return _result(
                    _STATUS_FAILED,
                    reason="no-repo",
                    path=path,
                    canonical_state="failed",
                    retry_state="permanent",
                    recorded=False,
                )

    try:
        intent = await postgres_db.begin_knowledge_materialization(
            project_id=project_id,
            note_id=str(slug),
            content=content_text,
            content_hash=content_hash,
            job_id=job_id,
        )
    except Exception as exc:
        logger.error(
            "kb-materialize: could not persist retry intent for %s/%s: %r",
            project_id,
            slug,
            exc,
            exc_info=True,
        )
        return {
            **_result(_STATUS_FAILED, reason="intent-store-error"),
            "canonical_state": "failed",
            "projection_state": "projection_only",
            "retry_state": "retryable",
        }
    intent_id = str(intent["id"])
    if str(intent.get("canonical_state")) == "canonical":
        return {
            **_result(
                _STATUS_SKIPPED,
                reason="already-canonical",
                repo=intent.get("repo"),
                branch=intent.get("branch"),
                path=intent.get("path"),
                operation=intent.get("operation"),
            ),
            "intent_id": intent_id,
            "canonical_state": "canonical",
            "projection_state": intent.get("projection_state") or "pending",
            "retry_state": "none",
        }
    if not intent.get("attempt_claimed"):
        return {
            **_result(_STATUS_SKIPPED, reason="attempt-in-progress"),
            "intent_id": intent_id,
            "canonical_state": intent.get("canonical_state") or "pending_sync",
            "projection_state": intent.get("projection_state") or "pending",
            "retry_state": intent.get("retry_state") or "retryable",
        }
    return await _attempt_content_intent(
        postgres_db=postgres_db,
        gitea_client=gitea_client,
        intent=intent,
        project_id=project_id,
        slug=slug,
        content=content_text,
        job_id=job_id,
        expected_blob_sha=expected_blob_sha,
        resolved_repo=resolved_repo,
        mode=mode,
    )


async def materialize_knowledge_note(
    *,
    postgres_db: Any,
    gitea_client: Any,
    project_id: str,
    slug: str,
    content: str,
    job_id: Optional[str] = None,
    store: Any = None,
    embedding_service: Any = None,
    retrieval_messages: Optional[list[str]] = None,
    expected_blob_sha: Optional[str] = None,
    mode: Optional[str] = None,
) -> dict[str, Any]:
    """Commit one note and, when an indexer is supplied, index it inline.

    Two legs, in this order and no other: the commit is the write, the index
    is derived from it. A note is never indexed unless the bytes are proven
    canonical, so the searchable projection can never claim something git does
    not have.

    ``store``/``embedding_service`` are optional so the service stays testable
    without a vector DB, and so a deployment with no resolvable embedding
    service degrades to the pre-Slice-A behaviour (commit now, sweep indexes
    later) instead of failing the write.

    ``retrieval_messages`` forwards to the inline index call (``None`` leaves
    any already-stored value alone) — see :func:`_index_note_inline`. So does
    ``job_id``, which names the commit *and* stamps the row's provenance.

    ``mode`` (``"create"``/``"update"``/``None``) is the caller's intent; see
    :func:`_materialize_knowledge_note_once`.
    """
    result = await _materialize_note_canonical(
        postgres_db=postgres_db,
        gitea_client=gitea_client,
        project_id=project_id,
        slug=slug,
        content=content,
        job_id=job_id,
        expected_blob_sha=expected_blob_sha,
        mode=mode,
    )
    if not _is_canonical(result):
        return {**result, "indexed": False, "index_reason": "not-canonical"}
    if store is None or embedding_service is None:
        return {**result, "indexed": False, "index_reason": "no-indexer"}
    index_state = await _index_note_inline(
        postgres_db=postgres_db,
        store=store,
        embedding_service=embedding_service,
        project_id=project_id,
        slug=slug,
        content=str(content or ""),
        retrieval_messages=retrieval_messages,
        job_id=job_id,
        intent_id=result.get("intent_id"),
        path=result.get("path"),
    )
    return {**result, **index_state}


def metadata_mutation_content(
    *,
    status: str | None = None,
    add_tags: list[str] | None = None,
    remove_tags: list[str] | None = None,
    path: str | None = None,
) -> str:
    """The ledger payload of a metadata mutation.

    ``path`` (the note's indexed path) is included only when known, so a
    mutation without one keeps the content hash it always had.
    """
    mutation: dict[str, Any] = {
        "status": status,
        "add_tags": [str(tag) for tag in (add_tags or [])],
        "remove_tags": [str(tag) for tag in (remove_tags or [])],
    }
    if path:
        mutation["path"] = str(path)
    return json.dumps(mutation, sort_keys=True, separators=(",", ":"))


def _canonical_metadata_fields(content: str, slug: str) -> dict[str, Any]:
    """Return the exact projection metadata encoded in canonical markdown.

    Metadata mutation callers must project what Git contains, not reconstruct
    it from the request that happened to create an intent.  ``ready_at`` is
    normalized here so every projection consumer receives one typed contract
    and no database adapter is asked to infer an ISO string's timestamp type.
    """
    frontmatter, _ = parse_note_md(content)
    if frontmatter is None or str(frontmatter.get("id") or "") != slug:
        raise ValueError("canonical note has invalid frontmatter")

    raw_tags = frontmatter.get("tags") or []
    if not isinstance(raw_tags, list):
        raise ValueError("canonical note tags must be a list")

    raw_ready_at = frontmatter.get("ready_at")
    if raw_ready_at is None:
        ready_at = None
    elif isinstance(raw_ready_at, datetime):
        ready_at = raw_ready_at
    elif isinstance(raw_ready_at, str):
        try:
            ready_at = datetime.fromisoformat(raw_ready_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("canonical ready_at must be an ISO timestamp") from exc
    else:
        raise ValueError("canonical ready_at must be a timestamp")
    if ready_at is not None and ready_at.tzinfo is None:
        ready_at = ready_at.replace(tzinfo=timezone.utc)

    status = frontmatter.get("status")
    return {
        "canonical_status": str(status) if status is not None else None,
        "canonical_tags": [str(tag) for tag in raw_tags],
        "canonical_ready_at": ready_at,
        "canonical_metadata_complete": True,
    }


async def _read_canonical_metadata(
    *,
    postgres_db: Any,
    gitea_client: Any,
    project_id: str,
    slug: str,
    path_hint: Optional[str] = None,
) -> dict[str, Any]:
    """Reread canonical truth for an intent another actor already committed.

    A scheduled retry can win between a failed HTTP request and its client
    retry.  The durable intent then proves the mutation crossed Git, but it
    does not contain the complete, current tag set.  Reading the note avoids
    destructive defaults such as ``tags=[]`` or ``ready_at=NULL`` and also
    handles a later canonical mutation honestly.
    """
    try:
        resolved = await resolve_kb_repo(postgres_db, project_id)
        if resolved is None:
            return _result(_STATUS_FAILED, reason="no-repo")
        repo_client = await kb_client_for_repo(postgres_db, gitea_client, resolved)
        from orchestrator.services.project_backlog import _read_note_file

        location = await _resolve_note_path(
            repo_client,
            resolved.repo,
            resolved.branch or "main",
            slug,
            path_hint=path_hint,
        )
        if location.ambiguous:
            return _ambiguous_result(slug, location, operation="metadata-update")
        current = None
        if location.exists is not False:
            current, _ = await _read_note_file(repo_client, resolved, location.path)
        if current is None:
            return _result(
                _STATUS_FAILED,
                reason="canonical-file-missing",
                repo=resolved.repo,
                branch=resolved.branch,
                path=location.path,
                operation="metadata-update",
            )
        metadata = _canonical_metadata_fields(current, slug)
        return {
            **_result(
                _STATUS_SKIPPED,
                reason="already-canonical",
                repo=resolved.repo,
                branch=resolved.branch,
                path=location.path,
                operation="metadata-update",
            ),
            **metadata,
        }
    except ValueError:
        return _result(_STATUS_FAILED, reason="malformed-frontmatter")
    except Exception as exc:
        logger.error(
            "kb-materialize: canonical metadata reread failed for %s/%s: %r",
            project_id,
            slug,
            exc,
            exc_info=True,
        )
        return _result(_STATUS_FAILED, reason="canonical-read-error")


async def _attempt_metadata_intent(
    *,
    postgres_db: Any,
    gitea_client: Any,
    intent: dict[str, Any],
    project_id: str,
    slug: str,
    mutation: dict[str, Any],
) -> dict[str, Any]:
    status = mutation.get("status")
    add_tags = mutation.get("add_tags") or []
    remove_tags = mutation.get("remove_tags") or []
    permanent = False
    rendered: str | None = None
    try:
        resolved = await resolve_kb_repo(postgres_db, project_id)
        if resolved is None:
            outcome = _result(_STATUS_FAILED, reason="no-repo")
        else:
            repo_client = await kb_client_for_repo(postgres_db, gitea_client, resolved)
            from orchestrator.services.project_backlog import (
                _ALREADY_SET,
                _NOT_REWRITABLE,
                _read_note_file,
                _rewrite_status,
            )

            location = await _resolve_note_path(
                repo_client,
                resolved.repo,
                resolved.branch or "main",
                slug,
                path_hint=mutation.get("path"),
            )
            current = None
            if location.exists is not False and not location.ambiguous:
                current, _ = await _read_note_file(repo_client, resolved, location.path)
            if location.ambiguous:
                outcome = _ambiguous_result(slug, location, operation="metadata-update")
                permanent = True
            elif current is None:
                outcome = _result(_STATUS_FAILED, reason="canonical-file-missing")
                permanent = True
            else:
                frontmatter, body = parse_note_md(current)
                if frontmatter is None or str(frontmatter.get("id") or "") != slug:
                    outcome = _result(_STATUS_FAILED, reason="malformed-frontmatter")
                    permanent = True
                else:
                    if status is not None and not add_tags and not remove_tags:
                        rendered, rewrite = _rewrite_status(current, str(status))
                        if rewrite == _NOT_REWRITABLE:
                            outcome = _result(
                                _STATUS_FAILED, reason="malformed-frontmatter"
                            )
                            permanent = True
                            rendered = current
                        elif rewrite == _ALREADY_SET:
                            outcome = _result(
                                _STATUS_SKIPPED,
                                reason="unchanged",
                                repo=resolved.repo,
                                branch=resolved.branch,
                                path=location.path,
                                operation="metadata-update",
                            )
                            rendered = current
                        else:
                            outcome = {}
                    else:
                        desired = dict(frontmatter)
                        if status is not None:
                            desired["status"] = status
                        tags = [str(tag) for tag in (desired.get("tags") or [])]
                        remove = {str(tag).strip().lower() for tag in remove_tags}
                        tags = [
                            tag for tag in tags if tag.strip().lower() not in remove
                        ]
                        existing = {tag.strip().lower() for tag in tags}
                        for tag in add_tags:
                            normalized = str(tag).strip().lower()
                            if normalized and normalized not in existing:
                                tags.append(normalized)
                                existing.add(normalized)
                        desired["tags"] = tags
                        prior_tags = {
                            str(tag).strip().lower()
                            for tag in (frontmatter.get("tags") or [])
                        }
                        if "ready" in existing and "ready" not in prior_tags:
                            desired["ready_at"] = datetime.now(timezone.utc).isoformat()
                        elif "ready" not in existing:
                            desired.pop("ready_at", None)
                        rendered = (
                            "---\n"
                            + yaml.safe_dump(
                                desired, sort_keys=False, allow_unicode=True
                            ).rstrip()
                            + "\n---\n"
                            + body
                        )
                        outcome = {}
                    if not outcome:
                        # The blob just read pins the write to the same file
                        # (a nested note stays where it is) and makes the
                        # read-modify-write atomic: a concurrent change turns
                        # into a retryable miss, re-applied to the new text.
                        outcome = await _materialize_knowledge_note_once(
                            postgres_db=postgres_db,
                            gitea_client=gitea_client,
                            project_id=project_id,
                            slug=slug,
                            content=rendered,
                            resolved_repo=resolved,
                            repo_client=repo_client,
                            expected_blob_sha=location.blob_sha,
                            path_hint=location.path,
                        )
    except ValueError:
        outcome = _result(_STATUS_FAILED, reason="malformed-frontmatter")
        permanent = True
    except Exception as exc:
        logger.error(
            "kb-materialize: canonical metadata mutation failed for %s/%s: %r",
            project_id,
            slug,
            exc,
            exc_info=True,
        )
        outcome = _result(_STATUS_FAILED, reason="canonical-read-error")
    if rendered and (
        outcome.get("status") == _STATUS_COMMITTED
        or (
            outcome.get("status") == _STATUS_SKIPPED
            and outcome.get("reason") == "unchanged"
        )
    ):
        try:
            outcome.update(_canonical_metadata_fields(rendered, slug))
        except ValueError:
            # The Git boundary may already have been crossed.  Preserve that
            # canonical truth in the ledger, but never let a caller project
            # guessed metadata from an incomplete result.
            outcome["canonical_metadata_complete"] = False
    return await _finish_attempt(
        postgres_db=postgres_db,
        intent=intent,
        outcome=outcome,
        permanent=permanent,
    )


async def materialize_knowledge_metadata_update(
    *,
    postgres_db: Any,
    gitea_client: Any,
    project_id: str,
    slug: str,
    status: str | None = None,
    add_tags: list[str] | None = None,
    remove_tags: list[str] | None = None,
    path_hint: str | None = None,
) -> dict[str, Any]:
    """Apply a status/tag change to the note's canonical file, ledgered.

    ``path_hint`` is the note's indexed path when the caller read its row; it
    lets a note whose id is not its filename be found (see
    :func:`_resolve_note_path`).
    """
    payload = metadata_mutation_content(
        status=status, add_tags=add_tags, remove_tags=remove_tags, path=path_hint
    )
    content_hash = "metadata:" + hashlib.sha256(payload.encode()).hexdigest()
    try:
        intent = await postgres_db.begin_knowledge_materialization(
            project_id=project_id,
            note_id=slug,
            content=payload,
            content_hash=content_hash,
            operation="metadata-update",
        )
    except Exception as exc:
        logger.error(
            "kb-materialize: metadata retry intent failed for %s/%s: %r",
            project_id,
            slug,
            exc,
            exc_info=True,
        )
        return {
            **_result(_STATUS_FAILED, reason="intent-store-error"),
            "canonical_state": "failed",
            "projection_state": "projection_only",
            "retry_state": "retryable",
        }
    if str(intent.get("canonical_state")) == "canonical":
        canonical = await _read_canonical_metadata(
            postgres_db=postgres_db,
            gitea_client=gitea_client,
            project_id=project_id,
            slug=slug,
            path_hint=path_hint,
        )
        return {
            **canonical,
            "intent_id": str(intent["id"]),
            "canonical_state": "canonical",
            "projection_state": intent.get("projection_state") or "pending",
            "retry_state": "none",
        }
    if not intent.get("attempt_claimed"):
        return {
            **_result(_STATUS_SKIPPED, reason="attempt-in-progress"),
            "intent_id": str(intent["id"]),
            "canonical_state": intent.get("canonical_state") or "pending_sync",
            "projection_state": intent.get("projection_state") or "pending",
            "retry_state": intent.get("retry_state") or "retryable",
        }
    return await _attempt_metadata_intent(
        postgres_db=postgres_db,
        gitea_client=gitea_client,
        intent=intent,
        project_id=project_id,
        slug=slug,
        mutation=json.loads(payload),
    )


# =============================================================================
# Delete — the purge primitive (kb_gardening_retire_consolidate_purge.md G2)
# =============================================================================


def _delete_commit_message(
    slug: str, job_id: Optional[str], reason: Optional[str]
) -> str:
    """Commit text for a file removal: subject names the note, body carries
    the reason (the journal entry) and the full job id, like `_commit_message`."""
    job = str(job_id or "").strip()
    subject = f"kb: delete {slug}" + (f" (job {job[:8]})" if job else "")
    body_lines = []
    text = " ".join(str(reason or "").split())
    if text:
        body_lines.append(f"reason: {text[:500]}")
    if job:
        body_lines.append(f"job: {job}")
    return subject + ("\n\n" + "\n".join(body_lines) if body_lines else "")


async def _delete_note_once(
    *,
    postgres_db: Any,
    gitea_client: Any,
    project_id: str,
    slug: str,
    job_id: Optional[str],
    reason: Optional[str],
    expected_blob_sha: Optional[str],
    path_hint: Optional[str] = None,
) -> dict[str, Any]:
    """Remove the note's file from the project's KB repo — once.

    The file is found wherever it sits under ``knowledge/``
    (:func:`_resolve_note_path`); before that, a nested note's delete found
    nothing at the flat path, reported ``absent`` as success, and the next
    sweep resurrected the row from the untouched file (G16).

    Returns the same ``{status, reason, repo, branch, path, operation}`` shape
    as the write path. Outcomes:

    * ``committed`` / ``delete`` — the file-removal commit landed.
    * ``skipped`` / ``absent`` — the path is not on the branch. This is the
      desired state, so it counts as canonical (idempotent delete: RFC 9110).
    * ``skipped`` / ``no-repo`` — repo-less project; nothing in git to remove.
    * ``failed`` / ``precondition-failed`` — ``expected_blob_sha`` did not
      match the tree, or the forge refused the SHA because a concurrent
      writer moved the file. **Permanent**: the caller must re-read and
      decide again; a blind retry would delete something it never looked at.
    * ``failed`` / ``tree-unreadable`` | ``commit-error`` | ``client-error``
      | ``resolve-error`` — transient; retried by the intent sweep.

    Never raises.
    """
    bad_slug = slug_error(slug)
    if bad_slug:
        logger.error(
            "kb-materialize: refusing delete for project %s — %s", project_id, bad_slug
        )
        return _result(_STATUS_FAILED, reason="invalid-slug")
    slug = str(slug).strip()
    path = note_repo_path(slug)

    try:
        resolved = await resolve_kb_repo(postgres_db, project_id)
    except Exception as e:  # noqa: BLE001 — resolution is not the caller's problem
        logger.error(
            "kb-materialize: KB repo resolution failed for delete %s/%s: %r",
            project_id,
            slug,
            e,
            exc_info=True,
        )
        return _result(_STATUS_FAILED, reason="resolve-error", path=path)
    if not resolved:
        return _result(_STATUS_SKIPPED, reason="no-repo", path=path)

    repo_name, branch = resolved.repo, (resolved.branch or "main")
    try:
        repo_client = await kb_client_for_repo(postgres_db, gitea_client, resolved)
    except Exception:  # noqa: BLE001 — credential/config failures stay non-fatal
        logger.error(
            "kb-materialize: no usable %s client for delete in project %s",
            resolved.forge,
            project_id,
            exc_info=True,
        )
        return _result(
            _STATUS_FAILED,
            reason="client-error",
            repo=repo_name,
            branch=branch,
            path=path,
        )

    location = await _resolve_note_path(
        repo_client,
        repo_name,
        branch,
        slug,
        expected_blob_sha=expected_blob_sha,
        path_hint=path_hint,
    )
    if location.ambiguous:
        return _ambiguous_result(
            slug, location, repo=repo_name, branch=branch, path=path, operation="delete"
        )
    path, exists, tree_sha = location.path, location.exists, location.blob_sha
    if exists is None:
        # Unlike a write (where a guess degrades to one wasted call), a delete
        # against an unknown tree could remove a note nobody looked at. Refuse.
        return _result(
            _STATUS_FAILED,
            reason="tree-unreadable",
            repo=repo_name,
            branch=branch,
            path=path,
            operation="delete",
        )
    if not exists:
        return _result(
            _STATUS_SKIPPED,
            reason="absent",
            repo=repo_name,
            branch=branch,
            path=path,
            operation="delete",
        )
    expected = str(expected_blob_sha or "").strip() or None
    if expected and tree_sha and expected != tree_sha:
        logger.info(
            "kb-materialize: delete of %s refused — caller read %s, tree holds %s",
            path,
            expected[:12],
            str(tree_sha)[:12],
        )
        return _result(
            _STATUS_FAILED,
            reason="precondition-failed",
            repo=repo_name,
            branch=branch,
            path=path,
            operation="delete",
        )

    message = _delete_commit_message(slug, job_id, reason)
    try:
        verdict = await repo_client.delete_path(
            repo_name, branch, path, message, expected_sha=expected or tree_sha
        )
    except Exception as e:  # noqa: BLE001 — non-fatal by contract
        logger.error(
            "kb-materialize: delete of %s from %s@%s raised: %r",
            path,
            repo_name,
            branch,
            e,
            exc_info=True,
        )
        verdict = "error"

    common = dict(repo=repo_name, branch=branch, path=path, operation="delete")
    if verdict == "deleted":
        logger.info(
            "kb-materialize: %s removed from %s@%s%s",
            path,
            repo_name,
            branch,
            f", job {str(job_id)[:8]}" if job_id else "",
        )
        return _result(_STATUS_COMMITTED, **common)
    if verdict == "absent":
        return _result(_STATUS_SKIPPED, reason="absent", **common)
    if verdict == "conflict":
        return _result(_STATUS_FAILED, reason="precondition-failed", **common)
    return _result(_STATUS_FAILED, reason="commit-error", **common)


async def _attempt_delete_intent(
    *,
    postgres_db: Any,
    gitea_client: Any,
    intent: dict[str, Any],
    project_id: str,
    slug: str,
    job_id: Optional[str],
    mutation: dict[str, Any],
) -> dict[str, Any]:
    try:
        outcome = await _delete_note_once(
            postgres_db=postgres_db,
            gitea_client=gitea_client,
            project_id=project_id,
            slug=slug,
            job_id=job_id,
            reason=mutation.get("reason"),
            expected_blob_sha=mutation.get("expected_blob_sha"),
            path_hint=mutation.get("path"),
        )
    except Exception as exc:  # noqa: BLE001 — never let the ledger dangle
        logger.error(
            "kb-materialize: unexpected delete exception for %s/%s: %r",
            project_id,
            slug,
            exc,
            exc_info=True,
        )
        outcome = _result(_STATUS_FAILED, reason="materializer-exception")
    permanent = outcome.get("reason") in {
        "invalid-slug",
        "precondition-failed",
        "ambiguous-note-path",
    }
    return await _finish_attempt(
        postgres_db=postgres_db,
        intent=intent,
        outcome=outcome,
        permanent=permanent,
    )


async def _drop_note_rows(*, store: Any, project_id: str, slug: str, path: str) -> bool:
    """Remove the searchable projection of a note whose file is gone.

    Both keyings, because both row shapes exist: path-backed rows the sweep
    manages (``(kb_id, path)``; ``kb_id`` doubles as the project id for a
    native vault) and legacy born-pathless rows (``(project_id, note_id)``).
    The second must stay pathless-only: dropping every row with the note's id
    used to remove a row whose file was still on the branch (a nested note
    the delete never reached), and the next sweep put it back (G16).
    Best-effort: a failure here leaves a row the next sweep's tree-diff reaps.
    """
    try:
        project_uuid = uuid.UUID(str(project_id))
    except (TypeError, ValueError):
        return False
    dropped = False
    try:
        dropped = bool(await store.delete_kb_note(project_uuid, path)) or dropped
    except Exception:  # noqa: BLE001 — the sweep self-heals
        logger.warning(
            "kb-materialize: row cleanup by path failed for %s/%s (sweep will reap)",
            project_id,
            slug,
            exc_info=True,
        )
        return False
    try:
        dropped = bool(await store.delete_pathless_note(project_uuid, slug)) or dropped
    except Exception:  # noqa: BLE001
        logger.debug(
            "kb-materialize: legacy row cleanup skipped for %s/%s", project_id, slug
        )
    return dropped


async def materialize_knowledge_note_delete(
    *,
    postgres_db: Any,
    gitea_client: Any,
    project_id: str,
    slug: str,
    job_id: Optional[str] = None,
    reason: Optional[str] = None,
    expected_blob_sha: Optional[str] = None,
    store: Any = None,
    path_hint: Optional[str] = None,
) -> dict[str, Any]:
    """Purge one note: file-removal commit first, then drop its index rows.

    The git leg is the delete; the row drop is derived from it and only runs
    once the branch provably no longer holds the file (``committed`` or
    ``skipped/absent``). Like writes, the intent is persisted before the
    remote mutation so a crash mid-way is retried by the sweep — except a
    ``precondition-failed`` outcome, which is permanent by design (§G3):
    the caller must look again before asking again.

    ``expected_blob_sha`` is the compare-and-swap token: pass the SHA the
    caller read (``knowledge_index.blob_sha``) and the delete refuses if the
    tree holds anything else. Omit it only for a human-authorised purge.

    ``path_hint`` is the note's indexed path when the caller read its row
    (see :func:`_resolve_note_path`); it rides in the ledger payload so a
    sweep retry deletes the same file.

    Returns the write path's result shape plus ``row_deleted``.
    """
    mutation: dict[str, Any] = {
        "reason": " ".join(str(reason or "").split())[:500] or None,
        "expected_blob_sha": str(expected_blob_sha or "").strip() or None,
    }
    if path_hint:
        mutation["path"] = str(path_hint)
    payload = json.dumps(mutation, sort_keys=True)
    content_hash = "delete:" + hashlib.sha256(payload.encode()).hexdigest()
    try:
        intent = await postgres_db.begin_knowledge_materialization(
            project_id=project_id,
            note_id=slug,
            content=payload,
            content_hash=content_hash,
            job_id=job_id,
            operation="delete",
        )
    except Exception as exc:
        logger.error(
            "kb-materialize: delete retry intent failed for %s/%s: %r",
            project_id,
            slug,
            exc,
            exc_info=True,
        )
        return {
            **_result(_STATUS_FAILED, reason="intent-store-error"),
            "canonical_state": "failed",
            "projection_state": "projection_only",
            "retry_state": "retryable",
            "row_deleted": False,
        }
    intent_id = str(intent["id"])
    if str(intent.get("canonical_state")) == "canonical":
        result: dict[str, Any] = {
            **_result(
                _STATUS_SKIPPED,
                reason="already-canonical",
                repo=intent.get("repo"),
                branch=intent.get("branch"),
                path=intent.get("path") or note_repo_path(str(slug).strip()),
                operation="delete",
            ),
            "intent_id": intent_id,
            "canonical_state": "canonical",
            "projection_state": intent.get("projection_state") or "pending",
            "retry_state": "none",
        }
    elif not intent.get("attempt_claimed"):
        if str(intent.get("retry_state") or "") == "permanent":
            # The identical delete (same reason, same token) was already
            # refused for good — typically `precondition-failed` on a stale
            # token. Say so, instead of "in progress": the caller must
            # re-read (the sweep restamps the row's blob_sha) and ask again
            # with the current token, which is a different intent.
            prior = str(
                intent.get("last_error")
                or intent.get("last_error_class")
                or "permanent-failure"
            )
            return {
                **_result(
                    _STATUS_FAILED,
                    reason=prior,
                    repo=intent.get("repo"),
                    branch=intent.get("branch"),
                    path=intent.get("path"),
                    operation="delete",
                ),
                "intent_id": intent_id,
                "canonical_state": intent.get("canonical_state") or "failed",
                "projection_state": intent.get("projection_state") or "pending",
                "retry_state": "permanent",
                "row_deleted": False,
            }
        return {
            **_result(_STATUS_SKIPPED, reason="attempt-in-progress"),
            "intent_id": intent_id,
            "canonical_state": intent.get("canonical_state") or "pending_sync",
            "projection_state": intent.get("projection_state") or "pending",
            "retry_state": intent.get("retry_state") or "retryable",
            "row_deleted": False,
        }
    else:
        result = await _attempt_delete_intent(
            postgres_db=postgres_db,
            gitea_client=gitea_client,
            intent=intent,
            project_id=project_id,
            slug=slug,
            job_id=job_id,
            mutation=json.loads(payload),
        )

    row_deleted = False
    if store is not None and _is_canonical(result):
        row_deleted = await _drop_note_rows(
            store=store,
            project_id=project_id,
            slug=str(slug).strip(),
            path=str(result.get("path") or note_repo_path(str(slug).strip())),
        )
    return {**result, "row_deleted": row_deleted}


async def retry_knowledge_materialization_intent(
    *, postgres_db: Any, gitea_client: Any, intent: dict[str, Any]
) -> dict[str, Any]:
    operation = str(intent.get("operation") or "")
    if operation == "delete":
        try:
            mutation = json.loads(str(intent.get("content") or "{}"))
        except (TypeError, ValueError):
            mutation = {}
        return await _attempt_delete_intent(
            postgres_db=postgres_db,
            gitea_client=gitea_client,
            intent=intent,
            project_id=str(intent["project_id"]),
            slug=str(intent["note_id"]),
            job_id=str(intent["job_id"]) if intent.get("job_id") else None,
            mutation=mutation if isinstance(mutation, dict) else {},
        )
    if operation == "metadata-update":
        try:
            mutation = json.loads(str(intent.get("content") or "{}"))
        except (TypeError, ValueError):
            mutation = {}
        return await _attempt_metadata_intent(
            postgres_db=postgres_db,
            gitea_client=gitea_client,
            intent=intent,
            project_id=str(intent["project_id"]),
            slug=str(intent["note_id"]),
            mutation=mutation,
        )
    return await _attempt_content_intent(
        postgres_db=postgres_db,
        gitea_client=gitea_client,
        intent=intent,
        project_id=str(intent["project_id"]),
        slug=str(intent["note_id"]),
        content=str(intent["content"]),
        job_id=str(intent["job_id"]) if intent.get("job_id") else None,
    )
