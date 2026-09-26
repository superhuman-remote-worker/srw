"""Total, nullable keyset order for bounded Job discovery (no authority)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import UUID


@dataclass(frozen=True, slots=True)
class JobDiscoveryCursor:
    priority: int
    created_at: datetime | None
    job_id: str

    @classmethod
    def from_job(cls, job: Mapping[str, Any]) -> JobDiscoveryCursor:
        return cls(int(job.get("priority") or 0), job.get("created_at"), str(job["id"]))


def discovery_order(job: Mapping[str, Any]) -> tuple[int, bool, datetime, str]:
    """Mirror priority DESC, created_at ASC NULLS LAST, UUID ASC in memory."""
    created = job.get("created_at")
    return (
        -int(job.get("priority") or 0),
        created is None,
        created if created is not None else datetime.max.replace(tzinfo=timezone.utc),
        str(job["id"]),
    )


def discovery_page_bounds(
    after: JobDiscoveryCursor | None, cutoff: datetime | None
) -> tuple[str, tuple[Any, ...]]:
    """Additional predicates/arguments after the existing $1 LIMIT parameter."""
    predicates = []
    arguments: list[Any] = []
    if cutoff is not None:
        arguments.append(cutoff)
        predicates.append(
            "AND (j.created_at IS NULL OR j.created_at <= $2::timestamptz)"
        )
    if after is not None:
        index = len(arguments) + 2
        arguments.extend((after.priority, after.created_at, UUID(after.job_id)))
        priority, created, job_id = (f"${index + offset}" for offset in range(3))
        predicates.append(f"""AND (
            j.priority < {priority}::int OR (j.priority = {priority}::int AND (
                ({created}::timestamptz IS NOT NULL AND
                    (j.created_at > {created}::timestamptz OR j.created_at IS NULL))
                OR (j.created_at IS NOT DISTINCT FROM {created}::timestamptz
                    AND j.id > {job_id}::uuid)
            )))""")
    return "\n".join(predicates), tuple(arguments)
