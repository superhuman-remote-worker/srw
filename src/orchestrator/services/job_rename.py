"""Rename a job: its display title, never its task text.

``jobs.description`` is what the agent was asked to do and stays as written.
``jobs.title`` is the name a person gives the job afterwards. A job that was
never named has no title, and every reader shows the description instead.
"""

from __future__ import annotations

from typing import Any

import asyncpg
from fastapi import HTTPException

JOB_TITLE_MAX_LENGTH = 200


def normalize_job_title(raw: str | None) -> str | None:
    """One trimmed line; blank or missing means "no title".

    A title is a list label, so line breaks and runs of whitespace collapse to
    single spaces instead of being refused.
    """
    if raw is None:
        return None
    return " ".join(raw.split()) or None


async def rename_job(store: Any, job_id: str, raw_title: str | None) -> dict[str, Any]:
    """Store the new title (or clear it) and return what is now shown."""
    title = normalize_job_title(raw_title)
    if title is not None and len(title) > JOB_TITLE_MAX_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"Title too long (max {JOB_TITLE_MAX_LENGTH})",
        )
    try:
        found = await store.update_job_title(job_id, title)
    except asyncpg.CheckViolationError as exc:
        # Some row guards refuse every write to a job for a short time (a
        # pinned job whose VM is being released, for one). A rename can wait.
        raise HTTPException(
            status_code=409,
            detail="The job cannot be renamed right now; try again shortly",
        ) from exc
    if not found:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return {"job_id": job_id, "title": title}
