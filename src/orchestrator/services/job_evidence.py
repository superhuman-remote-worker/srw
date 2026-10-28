"""Job evidence manifest — declared, bounded disposition material (E4).

officer_supervision_surface §3.3: at completion the orchestrator records a
typed, immutable manifest in ``jobs.context.evidence_manifest`` (JSONB — v1
deliberately mints no new table):

- server-created entries for the completion report and the deliverable-check
  result (deliverable_gate output);
- resolved worker-declared entries: the worker's ``freeze_data.evidence[]``
  declarations (kind, label, media_type, source path) are resolved inside the
  job's own Gitea repo, pinned to the exact completion commit, measured
  (byte_size + sha256), and only then published under an opaque ID.

A raw worker path is never copied into an officer tool call: reads resolve
the opaque ID server-side at the PINNED revision — the model cannot supply a
path, traverse directories, or switch revisions. Screenshot reads return one
bounded transient image attachment. Repository coordinates remain
server-private on both list and read surfaces.

Bounds (Legate-ratified §10 recommendation): 256 KiB paginated text per item,
five images per job, 2 000 diff/change-summary lines; oversize material is
recorded with honest ``availability`` metadata but is not readable — larger
material belongs in a KB report.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import logging
import re
import uuid
import warnings
from datetime import datetime, timezone
from typing import Any

from PIL import Image, UnidentifiedImageError

from orchestrator.services.deliverable_gate import normalize_deliverable_path
from shared.content_redaction import sanitize

logger = logging.getLogger(__name__)

CONTEXT_KEY = "evidence_manifest"

#: Allowed kinds v1. completion_report / deliverable_check are SERVER-created;
#: a worker declaring them is rejected (it would forge the server's stamp).
SERVER_KINDS = frozenset({"completion_report", "deliverable_check"})
WORKER_KINDS = frozenset({"test_report", "screenshot", "change_summary"})
ALLOWED_KINDS = SERVER_KINDS | WORKER_KINDS

TEXT_LIMIT_BYTES = 256 * 1024  #: per text item (test_report/change_summary/reports)
MAX_IMAGES_PER_JOB = 5
DIFF_LINE_LIMIT = 2000  #: change_summary entries
BINARY_LIMIT_BYTES = 8 * 1024 * 1024  #: sanity ceiling for screenshots
MAX_IMAGE_PIXELS = 40_000_000
IMAGE_MEDIA_TYPES = {
    "PNG": "image/png",
    "JPEG": "image/jpeg",
    "GIF": "image/gif",
    "WEBP": "image/webp",
}
READ_PAGE_CHARS = 16000  #: pagination window for read_job_evidence
MAX_WORKER_ENTRIES = 20

_TEXT_MEDIA_PREFIXES = ("text/", "application/json", "application/xml")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _opaque_id() -> str:
    return f"ev_{uuid.uuid4().hex[:12]}"


def _is_text_media(media_type: str) -> bool:
    return media_type.startswith(_TEXT_MEDIA_PREFIXES)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _actual_image_type(data: bytes) -> tuple[str, int, int] | None:
    """Validate raster bytes and return (MIME, width, height).

    Declared extensions/media types carry no authority. Pillow verifies the
    actual signature/decoder under a pixel ceiling; SVG and every other active
    or unsupported format remain outside the allowlist. Animated GIF/WebP is
    refused outright: screenshots are a static-evidence contract, so an
    attacker cannot hide an unbounded aggregate frame count behind a small
    first frame.
    """
    if not data or len(data) > BINARY_LIMIT_BYTES:
        return None
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(data)) as image:
                media_type = IMAGE_MEDIA_TYPES.get(str(image.format or "").upper())
                width, height = image.size
                if (
                    media_type is None
                    or width <= 0
                    or height <= 0
                    or width * height > MAX_IMAGE_PIXELS
                    or int(getattr(image, "n_frames", 1)) != 1
                ):
                    return None
                image.verify()
    except (
        UnidentifiedImageError,
        EOFError,
        OSError,
        SyntaxError,
        TypeError,
        ValueError,
        Image.DecompressionBombError,
        Image.DecompressionBombWarning,
    ):
        return None
    return media_type, width, height


def parse_manifest(job: dict[str, Any]) -> dict[str, Any] | None:
    """The recorded manifest bound to this exact job row, or ``None``.

    ``context`` was caller-writable before ES-01's ingress repair. Requiring
    the embedded job identity here makes every list/read/report surface reject
    a copied historical manifest instead of treating agreement inside the blob
    as authority.
    """
    context = job.get("context")
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (json.JSONDecodeError, ValueError):
            return None
    if not isinstance(context, dict):
        return None
    manifest = context.get(CONTEXT_KEY)
    if not isinstance(manifest, dict) or manifest.get("version") != 1:
        return None
    job_id = str(job.get("id") or "")
    if not job_id or str(manifest.get("job_id") or "") != job_id:
        return None
    return manifest


def _safe_repo_path(raw: Any) -> str | None:
    """Canonical repo-relative path, or None when the declaration is unsafe.

    Path-traversal gate: absolute paths, drive letters, backslashes, and any
    ``..`` segment are rejected before Gitea is ever consulted.
    """
    if not isinstance(raw, str):
        return None
    candidate = raw.strip()
    if not candidate or len(candidate) > 512:
        return None
    if "\\" in candidate or "\x00" in candidate:
        return None
    if candidate.startswith(("/", "~")) or ":" in candidate.split("/", 1)[0]:
        return None
    normalized = normalize_deliverable_path(candidate)
    if not normalized:
        return None
    if any(segment == ".." for segment in normalized.split("/")):
        return None
    return normalized


async def _resolve_repo_and_head(
    job: dict[str, Any], *, db: Any, gitea: Any
) -> tuple[str | None, str | None, str | None]:
    """(repo_name, ref, pinned head sha) for the job's pushed branch."""
    from orchestrator.services.deliverable_gate import _resolve_repo_ref

    if gitea is None or not getattr(gitea, "is_initialized", False):
        return None, None, None
    repo_name, ref = await _resolve_repo_ref(job, db)
    if not repo_name or not ref:
        return None, None, None
    try:
        sha = await gitea.get_branch_head_sha(repo_name, ref, redact_coordinates=True)
    except Exception:  # noqa: BLE001
        sha = None
    return repo_name, ref, sha


def _declared_evidence(result: dict[str, Any], job: dict[str, Any]) -> list[Any]:
    """The worker's evidence[] declaration from the completion payload.

    Preference order: the reported freeze_data (this completion), then the
    persisted job-row freeze_data (resume/replay paths).
    """
    for candidate in (result.get("freeze_data"), job.get("freeze_data")):
        if isinstance(candidate, str):
            try:
                candidate = json.loads(candidate)
            except (json.JSONDecodeError, ValueError):
                candidate = None
        if isinstance(candidate, dict):
            declared = candidate.get("evidence")
            if isinstance(declared, list) and declared:
                return declared
    return []


def _completion_report_payload(
    result: dict[str, Any], job: dict[str, Any]
) -> dict[str, Any] | None:
    """The worker's completion claim (summary/deliverables/confidence/notes/citations)."""
    for candidate in (result.get("freeze_data"), job.get("freeze_data")):
        if isinstance(candidate, str):
            try:
                candidate = json.loads(candidate)
            except (json.JSONDecodeError, ValueError):
                candidate = None
        if not isinstance(candidate, dict):
            continue
        if candidate.get("summary") or candidate.get("deliverables"):
            return {
                "summary": candidate.get("summary"),
                "deliverables": candidate.get("deliverables") or [],
                "confidence": candidate.get("confidence"),
                "notes": candidate.get("notes"),
                "reported_at": candidate.get("timestamp"),
                # Citation state at completion (unverified ones by handle), so
                # a reviewer sees them before approving.
                "citations": candidate.get("citations"),
            }
    return None


def _inline_entry(
    *,
    kind: str,
    label: str,
    payload: dict[str, Any],
    revision: str | None,
    now: str,
) -> dict[str, Any]:
    content = json.dumps(payload, indent=2, ensure_ascii=False, default=str)
    raw = content.encode("utf-8")
    return {
        "id": _opaque_id(),
        "kind": kind,
        "label": label,
        "media_type": "application/json",
        "byte_size": len(raw),
        "sha256": _sha256(raw),
        "source": {"type": "inline", "revision": revision},
        "captured_at": now,
        "producer": "server",
        "availability": "available",
        "inline_content": content,
    }


async def build_evidence_manifest(
    job: dict[str, Any],
    result: dict[str, Any],
    *,
    db: Any,
    gitea: Any,
    gate_stamp: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Assemble the typed manifest for one completion. Never raises.

    Every entry is measured and pinned before its opaque ID exists; a
    declaration that cannot be resolved is still RECORDED with an honest
    ``availability`` so the officer sees that evidence was claimed but not
    verifiable — silence would hide the gap.
    """
    now = _now_iso()
    job_id = str(job.get("id"))
    repo_name, ref, head_sha = await _resolve_repo_and_head(job, db=db, gitea=gitea)
    entries: list[dict[str, Any]] = []

    # --- server-created: completion report -------------------------------
    report = _completion_report_payload(result, job)
    if report is not None:
        entries.append(
            _inline_entry(
                kind="completion_report",
                label="Worker completion report",
                payload=report,
                revision=head_sha,
                now=now,
            )
        )

    # --- server-created: deliverable-check result ------------------------
    if gate_stamp is None:
        context = job.get("context")
        if isinstance(context, str):
            try:
                context = json.loads(context)
            except (json.JSONDecodeError, ValueError):
                context = {}
        gate_stamp = (
            context.get("deliverable_gate") if isinstance(context, dict) else None
        )
    if isinstance(gate_stamp, dict) and gate_stamp:
        entries.append(
            _inline_entry(
                kind="deliverable_check",
                label="Deliverable-contract gate result",
                payload=gate_stamp,
                revision=gate_stamp.get("commit_sha") or head_sha,
                now=now,
            )
        )

    # --- worker-declared entries ------------------------------------------
    declared = _declared_evidence(result, job)[:MAX_WORKER_ENTRIES]
    image_count = 0
    for declaration in declared:
        if not isinstance(declaration, dict):
            continue
        kind = str(declaration.get("kind") or "").strip()
        label = str(declaration.get("label") or "").strip()[:200] or "(unlabeled)"
        media_type = str(declaration.get("media_type") or "").strip()[:100]
        raw_source = declaration.get("source")
        if isinstance(raw_source, dict):
            raw_source = raw_source.get("path")

        entry: dict[str, Any] = {
            "id": _opaque_id(),
            "kind": kind if kind in ALLOWED_KINDS else "unknown",
            "label": label,
            "media_type": media_type or "application/octet-stream",
            "byte_size": None,
            "sha256": None,
            "source": None,
            "captured_at": now,
            "producer": "worker",
            "availability": "available",
        }

        if kind not in WORKER_KINDS:
            entry["availability"] = "rejected_kind"
            entry["availability_reason"] = (
                f"kind {kind or '(missing)'!r} is not a worker-declarable kind "
                f"(allowed: {', '.join(sorted(WORKER_KINDS))})"
            )
            entries.append(entry)
            continue

        path = _safe_repo_path(raw_source)
        if path is None:
            entry["availability"] = "rejected_path"
            entry["availability_reason"] = (
                "source path is missing, absolute, traversing, or malformed"
            )
            entries.append(entry)
            continue

        if kind == "screenshot":
            image_count += 1
            if image_count > MAX_IMAGES_PER_JOB:
                entry["availability"] = "rejected_image_cap"
                entry["availability_reason"] = (
                    f"more than {MAX_IMAGES_PER_JOB} screenshot entries declared"
                )
                entry["source"] = {"type": "job_repo", "path": path}
                entries.append(entry)
                continue
            if not media_type:
                entry["media_type"] = "image/png"

        if not repo_name or not head_sha:
            entry["availability"] = "unresolved"
            entry["availability_reason"] = (
                "job repository or completion revision unavailable"
            )
            entry["source"] = {"type": "job_repo", "path": path}
            entries.append(entry)
            continue

        content: bytes | None = None
        try:
            content = await gitea.get_file_bytes(
                repo_name, path, ref=head_sha, redact_coordinates=True
            )
            if content is None:
                # Repository checkouts may place files under repo/ — the same
                # dual spelling the deliverable gate accepts.
                content = await gitea.get_file_bytes(
                    repo_name,
                    f"repo/{path}",
                    ref=head_sha,
                    redact_coordinates=True,
                )
                if content is not None:
                    path = f"repo/{path}"
        except Exception:  # noqa: BLE001
            logger.warning(
                "evidence: private object resolution failed for job %s",
                job_id[:8],
            )
            content = None

        entry["source"] = {
            "type": "job_repo",
            "repo": repo_name,
            "path": path,
            "ref": ref,
            "revision": head_sha,
        }
        if content is None:
            entry["availability"] = "unresolved"
            entry["availability_reason"] = (
                f"declared file not found at the pinned completion revision "
                f"{head_sha[:12]}"
            )
            entries.append(entry)
            continue

        entry["byte_size"] = len(content)
        entry["sha256"] = _sha256(content)

        media = entry["media_type"]
        if kind == "screenshot":
            if len(content) > BINARY_LIMIT_BYTES:
                entry["availability"] = "oversize"
                entry["availability_reason"] = (
                    f"{len(content)} B exceeds the {BINARY_LIMIT_BYTES} B binary bound"
                )
            else:
                image_info = _actual_image_type(content)
                if image_info is None:
                    entry["availability"] = "bad_media"
                    entry["availability_reason"] = (
                        "declared screenshot is not an allowed, decodable raster image"
                    )
                elif media and image_info[0] != media:
                    entry["availability"] = "bad_media"
                    entry["availability_reason"] = (
                        "declared media type does not match the image signature"
                    )
                else:
                    entry["media_type"] = image_info[0]
                    entry["image_width"] = image_info[1]
                    entry["image_height"] = image_info[2]
        elif not _is_text_media(media):
            entry["availability"] = "bad_media"
            entry["availability_reason"] = (
                "binary evidence is unsupported unless kind=screenshot"
            )
        else:
            if len(content) > TEXT_LIMIT_BYTES:
                entry["availability"] = "oversize"
                entry["availability_reason"] = (
                    f"{len(content)} B exceeds the {TEXT_LIMIT_BYTES} B text bound — "
                    "publish a KB report for larger material"
                )
            elif kind == "change_summary":
                line_count = content.count(b"\n") + 1
                if line_count > DIFF_LINE_LIMIT:
                    entry["availability"] = "oversize"
                    entry["availability_reason"] = (
                        f"{line_count} lines exceed the {DIFF_LINE_LIMIT}-line "
                        "change-summary bound"
                    )
        entries.append(entry)

    return {
        "version": 1,
        "recorded_at": now,
        "job_id": job_id,
        "source_revision": head_sha,
        # Internal authority anchors used to revalidate every later opaque-ID
        # read. public_manifest() removes them from the list surface.
        "source_repository": repo_name,
        "source_ref": ref,
        "entries": entries,
    }


def _public_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Strip bytes and object-plane coordinates from an evidence record."""
    public = {
        key: value
        for key, value in entry.items()
        if key not in {"inline_content", "source"}
    }
    source = entry.get("source")
    if isinstance(source, dict):
        public_source = {
            key: source[key]
            for key in ("type", "revision")
            if source.get(key) is not None
        }
        if public_source:
            public["source"] = public_source
    return public


def public_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    """The manifest with bytes and object-plane coordinates stripped."""
    entries = [
        _public_entry(entry)
        for entry in manifest.get("entries") or []
        if isinstance(entry, dict)
    ]
    return {
        **{
            key: value
            for key, value in manifest.items()
            if key not in {"source_repository", "source_ref"}
        },
        "entries": entries,
    }


def find_entry(manifest: dict[str, Any], evidence_id: str) -> dict[str, Any] | None:
    for entry in manifest.get("entries") or []:
        if isinstance(entry, dict) and entry.get("id") == evidence_id:
            return entry
    return None


def _paginate(text: str, offset: int) -> dict[str, Any]:
    total = len(text)
    offset = max(0, min(offset, total))
    window = text[offset : offset + READ_PAGE_CHARS]
    return {
        "content": window,
        "offset": offset,
        "total_chars": total,
        "truncated": offset + len(window) < total,
    }


def _redaction_note(clean: Any) -> dict[str, Any]:
    """The machine-readable withheld-content signal (OC-05).

    Absent when nothing was removed, so a clean page stays byte-identical to
    what it was before redaction existed.
    """
    if not clean.redacted:
        return {}
    return {"redacted": True, "redacted_count": clean.count}


def _refused(public_entry: dict[str, Any]) -> dict[str, Any]:
    """One coordinate-free refusal for malformed or unauthoritative records."""
    return {
        "entry": public_entry,
        "note": "content REFUSED: evidence provenance is not authoritative",
    }


def _valid_revision(value: Any) -> bool:
    return (
        isinstance(value, str) and re.fullmatch(r"[0-9a-fA-F]{40}", value) is not None
    )


def _valid_sha256(value: Any) -> bool:
    return (
        isinstance(value, str) and re.fullmatch(r"[0-9a-fA-F]{64}", value) is not None
    )


def _valid_recorded_size(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


async def _canonical_repo_authority(
    job: dict[str, Any], db: Any
) -> tuple[str | None, str | None]:
    """Resolve repository/ref solely from job rows, never manifest fields."""
    from orchestrator.services.deliverable_gate import _resolve_repo_ref

    try:
        return await _resolve_repo_ref(job, db)
    except Exception:  # noqa: BLE001 -- missing authority is a safe refusal
        return None, None


def _worker_declared_repo_path(job: dict[str, Any], resolved_path: str) -> bool:
    """Confirm the accepted completion payload named this artifact path.

    The manifest is a server projection of ``freeze_data.evidence``. Historical
    context can be forged, but the accepted worker completion payload lives in
    the job's separate freeze column and is fenced by completion ownership.
    Checking it prevents a forged historical manifest from turning an
    arbitrary safe path in the otherwise-canonical job repository into an
    object-reader capability. ``repo/`` is the one server-generated fallback
    spelling used during manifest construction.
    """
    for declaration in _declared_evidence({}, job):
        if not isinstance(declaration, dict):
            continue
        raw_source = declaration.get("source")
        if isinstance(raw_source, dict):
            raw_source = raw_source.get("path")
        declared_path = _safe_repo_path(raw_source)
        if declared_path is None:
            continue
        if resolved_path in {declared_path, f"repo/{declared_path}"}:
            return True
    return False


async def read_evidence_entry(
    job: dict[str, Any],
    entry: dict[str, Any],
    *,
    offset: int = 0,
    db: Any = None,
    gitea: Any = None,
) -> dict[str, Any]:
    """Resolve one manifest entry for reading. Authorization happens upstream.

    Text entries return a bounded, secret-redacted page; screenshot entries
    return one transient bounded image attachment. All reads
    resolve at the PINNED revision recorded in the manifest — never a branch
    head — and verify the recorded sha256 before returning content.

    Redaction is reported, not silent (audit OC-05): a page that had secrets
    removed carries ``redacted: true`` and a count, so the officer judges a
    knowingly-incomplete artifact rather than a quietly-shortened one. The
    stored bytes and their sha256 are untouched — only this view is sanitized.
    """
    public_entry = _public_entry(entry)
    manifest = parse_manifest(job)
    if manifest is None:
        return _refused(public_entry)

    availability = entry.get("availability")
    if availability not in (None, "available"):
        return {
            "entry": public_entry,
            "note": (
                f"content not readable: availability={availability}"
                + (
                    f" — {entry.get('availability_reason')}"
                    if entry.get("availability_reason")
                    else ""
                )
            ),
        }

    inline = entry.get("inline_content")
    if isinstance(inline, str):
        raw_inline = inline.encode("utf-8")
        source = entry.get("source")
        if (
            entry.get("kind") not in SERVER_KINDS
            or entry.get("producer") != "server"
            or not isinstance(source, dict)
            or source.get("type") != "inline"
            or source.get("revision") != manifest.get("source_revision")
            or not _valid_recorded_size(entry.get("byte_size"))
            or entry.get("byte_size") != len(raw_inline)
            or not _valid_sha256(entry.get("sha256"))
            or entry.get("sha256") != _sha256(raw_inline)
        ):
            return _refused(public_entry)
        clean = sanitize(inline)
        page = _paginate(clean.text, offset)
        return {"entry": public_entry, **page, **_redaction_note(clean)}

    source = entry.get("source")
    if not isinstance(source, dict):
        return _refused(public_entry)
    repo = source.get("repo")
    path = source.get("path")
    revision = source.get("revision")
    if not (repo and path and revision):
        return {
            "entry": public_entry,
            "note": "content not readable: entry carries no pinned source",
        }
    pinned_revision = manifest.get("source_revision")
    pinned_repository = manifest.get("source_repository")
    pinned_ref = manifest.get("source_ref")
    canonical_repository, canonical_ref = await _canonical_repo_authority(job, db)
    safe_path = _safe_repo_path(path)
    recorded_sha = entry.get("sha256")
    recorded_size = entry.get("byte_size")
    media = entry.get("media_type")
    if (
        entry.get("kind") not in WORKER_KINDS
        or entry.get("producer") != "worker"
        or source.get("type") != "job_repo"
        or not _valid_revision(pinned_revision)
        or canonical_repository is None
        or canonical_ref is None
        or pinned_repository != canonical_repository
        or pinned_ref != canonical_ref
        or safe_path is None
        or safe_path != path
        or not _worker_declared_repo_path(job, safe_path)
        or revision != pinned_revision
        or repo != canonical_repository
        or source.get("ref") != canonical_ref
        or not _valid_sha256(recorded_sha)
        or not _valid_recorded_size(recorded_size)
        or not isinstance(media, str)
        or not media
    ):
        return _refused(public_entry)
    size_ceiling = (
        BINARY_LIMIT_BYTES if entry.get("kind") == "screenshot" else TEXT_LIMIT_BYTES
    )
    if recorded_size > size_ceiling:
        return _refused(public_entry)
    if gitea is None or not getattr(gitea, "is_initialized", False):
        return {
            "entry": public_entry,
            "note": "content unavailable: Gitea is not reachable right now",
        }
    try:
        content = await gitea.get_file_bytes(
            canonical_repository,
            path,
            ref=pinned_revision,
            redact_coordinates=True,
        )
    except Exception:  # noqa: BLE001 — optional object tier, generic result only
        logger.warning(
            "evidence: pinned object read failed for job %s evidence %s",
            str(job.get("id") or "")[:8],
            str(entry.get("id") or "")[:20],
        )
        return {
            "entry": public_entry,
            "note": "content unavailable: pinned evidence store read failed",
        }
    if content is None:
        return {
            "entry": public_entry,
            "note": (
                "content unavailable: pinned revision no longer resolvable "
                "(repository pruned or rewritten)"
            ),
        }
    if len(content) != recorded_size or _sha256(content) != recorded_sha:
        return {
            "entry": public_entry,
            "note": (
                "content REFUSED: bytes at the pinned revision no longer match "
                "the recorded measurement — treat this evidence as tampered"
            ),
        }

    media = str(media)
    if entry.get("kind") == "screenshot":
        if len(content) > BINARY_LIMIT_BYTES:
            return {
                "entry": public_entry,
                "note": "content REFUSED: screenshot exceeds the byte ceiling",
            }
        image_info = _actual_image_type(content)
        if image_info is None or image_info[0] != media:
            return {
                "entry": public_entry,
                "note": (
                    "content REFUSED: bytes are not the recorded allowed image type"
                ),
            }
        return {
            "entry": public_entry,
            "attachment": {
                "type": "image",
                "media_type": image_info[0],
                "base64_data": base64.b64encode(content).decode("ascii"),
                "byte_size": len(content),
                "width": image_info[1],
                "height": image_info[2],
            },
        }
    if not _is_text_media(media):
        return {
            "entry": public_entry,
            "note": "content REFUSED: unsupported binary evidence type",
        }
    text = content.decode("utf-8", errors="replace")
    clean = sanitize(text)
    page = _paginate(clean.text, offset)
    return {"entry": public_entry, **page, **_redaction_note(clean)}


__all__ = [
    "ALLOWED_KINDS",
    "BINARY_LIMIT_BYTES",
    "CONTEXT_KEY",
    "DIFF_LINE_LIMIT",
    "MAX_IMAGES_PER_JOB",
    "MAX_IMAGE_PIXELS",
    "IMAGE_MEDIA_TYPES",
    "READ_PAGE_CHARS",
    "SERVER_KINDS",
    "TEXT_LIMIT_BYTES",
    "WORKER_KINDS",
    "build_evidence_manifest",
    "find_entry",
    "parse_manifest",
    "public_manifest",
    "read_evidence_entry",
]
