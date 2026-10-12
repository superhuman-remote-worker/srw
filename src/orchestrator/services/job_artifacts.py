"""Inspect committed todo state and revision-pinned job evidence.

Authorization is completed by the HTTP adapter. Evidence authority/redaction
and todo parsing remain with their existing dedicated services.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import json
import logging
from typing import Any, Protocol

from fastapi import HTTPException

from orchestrator.services.job_todos import (
    build_archive_listing,
    parse_archived_todos,
    parse_current_todos,
)
from shared.content_redaction import sanitize_data

logger = logging.getLogger(__name__)


class ArtifactStore(Protocol):
    async def get_job(self, job_id: str) -> dict[str, Any] | None: ...


class ArtifactForge(Protocol):
    @property
    def is_initialized(self) -> bool: ...
    async def list_contents(
        self, repo_name: str, path: str, *, ref: str | None = None
    ) -> list[dict[str, Any]] | None: ...
    async def get_file_content(
        self, repo_name: str, path: str, *, ref: str | None = None
    ) -> str | None: ...
    async def get_file_bytes(
        self,
        repo_name: str,
        path: str,
        *,
        ref: str | None = None,
        redact_coordinates: bool = False,
    ) -> bytes | None: ...


class JobEvidenceOperations(Protocol):
    """The existing evidence authority, supplied by application composition.

    Its completion/deliverable collaborators belong to later workstreams;
    importing this inspection boundary must not initialize those workflows.
    """

    def parse_manifest(self, job: dict[str, Any]) -> dict[str, Any] | None: ...
    def public_manifest(self, manifest: dict[str, Any]) -> dict[str, Any]: ...
    def find_entry(
        self, manifest: dict[str, Any], evidence_id: str
    ) -> dict[str, Any] | None: ...
    async def read_evidence_entry(
        self,
        job: dict[str, Any],
        entry: dict[str, Any],
        *,
        offset: int,
        db: ArtifactStore,
        gitea: ArtifactForge,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class JobArtifactDependencies:
    store: ArtifactStore
    forge: ArtifactForge
    resolve_job_repo: Callable[[str], Awaitable[tuple[str, str | None]]]
    evidence: JobEvidenceOperations


async def list_job_evidence_route(
    *,
    job_id: str,
    authorized_job: dict[str, Any],
    dependencies: JobArtifactDependencies,
) -> dict[str, Any]:
    """Read only the already-authorized job's committed inspection artifacts."""
    manifest = dependencies.evidence.parse_manifest(authorized_job)
    if manifest is None:
        return {"job_id": job_id, "recorded_at": None, "entries": []}
    return dependencies.evidence.public_manifest(manifest)


async def get_job_completion_report_route(
    *,
    job_id: str,
    authorized_job: dict[str, Any],
    dependencies: JobArtifactDependencies,
) -> dict[str, Any]:
    """Read only the already-authorized job's committed inspection artifacts."""
    manifest = dependencies.evidence.parse_manifest(authorized_job)
    entry = next(
        (
            candidate
            for candidate in (manifest or {}).get("entries") or []
            if candidate.get("kind") == "completion_report"
        ),
        None,
    )
    if not entry:
        raise HTTPException(
            status_code=404,
            detail=f"No completion report recorded for job '{job_id}'",
        )
    try:
        report = json.loads(entry.get("inline_content") or "{}")
    except (json.JSONDecodeError, TypeError):
        report = {}
    # The report is worker text that officers and owners read (audit OC-05):
    # the same sanitizer as the evidence page that reads this very entry, so
    # the two views of one report cannot disagree about what was withheld.
    # Parsed first, then cleaned per string, so the result stays valid JSON.
    clean = sanitize_data(report)
    body: dict[str, Any] = {
        "job_id": job_id,
        "recorded_at": manifest.get("recorded_at"),
        "source_revision": (entry.get("source") or {}).get("revision"),
        "report": clean.value,
    }
    if clean.redacted:
        body.update({"redacted": True, "redacted_count": clean.count})
    return body


async def read_job_evidence_route(
    *,
    job_id: str,
    authorized_job: dict[str, Any],
    evidence_id: str,
    offset: int,
    dependencies: JobArtifactDependencies,
) -> dict[str, Any]:
    """Read only the already-authorized job's committed inspection artifacts."""
    manifest = dependencies.evidence.parse_manifest(authorized_job)
    entry = (
        dependencies.evidence.find_entry(manifest, evidence_id) if manifest else None
    )
    if not entry:
        raise HTTPException(
            status_code=404,
            detail=f"Evidence '{evidence_id}' not found for job '{job_id}'",
        )
    try:
        return await dependencies.evidence.read_evidence_entry(
            authorized_job,
            entry,
            offset=offset,
            db=dependencies.store,
            gitea=dependencies.forge,
        )
    except Exception:  # noqa: BLE001 -- never expose private object coordinates
        logger.warning(
            "Evidence read failed safely for job %s evidence %s",
            str(job_id)[:8],
            str(evidence_id)[:20],
        )
        raise HTTPException(
            status_code=500,
            detail="Evidence read failed without exposing private object details",
        ) from None


async def get_job_todos(
    *, job_id: str, dependencies: JobArtifactDependencies
) -> dict[str, Any]:
    """Read only the already-authorized job's committed inspection artifacts."""
    current: dict[str, Any] | None = None
    archives: list[dict[str, Any]] = []
    has_workspace = False

    if dependencies.forge.is_initialized:
        repo_name, job_branch = await dependencies.resolve_job_repo(job_id)
        root_entries = await dependencies.forge.list_contents(
            repo_name, "", ref=job_branch
        )
        if root_entries is not None:
            has_workspace = True
            content = await dependencies.forge.get_file_content(
                repo_name, "todos.yaml", ref=job_branch
            )
            if content is not None:
                current = parse_current_todos(content)
            archive_entries = await dependencies.forge.list_contents(
                repo_name, "archive", ref=job_branch
            )
            archives = build_archive_listing(archive_entries)

    return {
        "job_id": job_id,
        "current": current,
        "archives": archives,
        "has_workspace": has_workspace,
    }


async def get_current_todos(
    *, job_id: str, dependencies: JobArtifactDependencies
) -> dict[str, Any]:
    """Read only the already-authorized job's committed inspection artifacts."""
    result: dict[str, Any] | None = None
    if dependencies.forge.is_initialized:
        repo_name, job_branch = await dependencies.resolve_job_repo(job_id)
        content = await dependencies.forge.get_file_content(
            repo_name, "todos.yaml", ref=job_branch
        )
        if content is not None:
            result = parse_current_todos(content)
    if result is None:
        raise HTTPException(
            status_code=404, detail=f"No current todos found for job '{job_id}'"
        )
    return result


async def list_todo_archives(
    *, job_id: str, dependencies: JobArtifactDependencies
) -> list[dict[str, Any]]:
    """Read only the already-authorized job's committed inspection artifacts."""
    if not dependencies.forge.is_initialized:
        return []
    repo_name, job_branch = await dependencies.resolve_job_repo(job_id)
    entries = await dependencies.forge.list_contents(
        repo_name, "archive", ref=job_branch
    )
    return build_archive_listing(entries)


async def get_archived_todos(
    *, job_id: str, filename: str, dependencies: JobArtifactDependencies
) -> dict[str, Any]:
    """Read only the already-authorized job's committed inspection artifacts."""
    result: dict[str, Any] | None = None
    # Security: same filename sanitation the local-disk route enforced
    safe = not (".." in filename or "/" in filename or "\\" in filename)
    if safe and dependencies.forge.is_initialized:
        repo_name, job_branch = await dependencies.resolve_job_repo(job_id)
        content = await dependencies.forge.get_file_content(
            repo_name, f"archive/{filename}", ref=job_branch
        )
        if content is not None:
            result = parse_archived_todos(content, filename)
    if result is None:
        raise HTTPException(
            status_code=404, detail=f"Archive '{filename}' not found for job '{job_id}'"
        )
    return result
