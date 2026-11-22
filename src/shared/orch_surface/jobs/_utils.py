"""Internal helpers shared by framework-independent job handlers."""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from shared.orch_surface import formatters as fmt
from shared.orch_surface.client import AsyncCockpitClient, MutationOutcomeUnknown
from shared.orch_surface.jobs.descriptors import CallerCtx
from shared.orch_surface.jobs.envelope import friendly_reason, http_status_of

logger = logging.getLogger(__name__)

# ``tls_pinned_cert``: an endpoint's pinned certificate is transport too. Only
# dispatch writes one, from the endpoint row an admin trusted.
_TRANSPORT_DENY = frozenset({"api_key", "base_url", "env_keys", "tls_pinned_cert"})
_TRANSPORT_DENY_SUFFIX = ("_api_key", "_tls_pinned_cert")


def transport_key_paths(value: Any, prefix: str = "") -> list[str]:
    """Return forbidden credential/transport paths in a config override."""
    found: list[str] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            normalized = str(key).lower()
            if normalized in _TRANSPORT_DENY or normalized.endswith(
                _TRANSPORT_DENY_SUFFIX
            ):
                found.append(path)
            else:
                found.extend(transport_key_paths(nested, path))
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            found.extend(transport_key_paths(nested, f"{prefix}[{index}]"))
    return found


def clean_job_id(job_id: str) -> str:
    cleaned = str(job_id or "").strip()
    while cleaned.endswith("...") or cleaned.endswith("\u2026"):
        cleaned = cleaned[:-3] if cleaned.endswith("...") else cleaned[:-1]
        cleaned = cleaned.strip()
    return cleaned


def _is_uuid(value: str) -> bool:
    try:
        UUID(value)
        return True
    except (TypeError, ValueError):
        return False


async def resolve_job_id(
    client: AsyncCockpitClient,
    caller: CallerCtx,
    job_id: str,
) -> str:
    """Resolve visible UUID prefixes only for adapters that historically did."""
    if not caller.resolve_job_id_prefixes:
        # MCP's pre-unification wrappers forwarded the supplied ID verbatim.
        # Prefix/ellipsis cleanup is an agent/session compatibility feature,
        # not a silent change to the existing MCP request contract.
        return str(job_id)

    cleaned = clean_job_id(job_id)
    if not cleaned or _is_uuid(cleaned) or len(cleaned) < 8:
        return cleaned

    jobs = (await client.list_jobs(limit=500)).get("jobs", [])
    matches = [
        str(job.get("id"))
        for job in jobs
        if job.get("id") and str(job.get("id")).startswith(cleaned)
    ]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        sample = ", ".join(matches[:5])
        raise ValueError(
            f"Job ID prefix '{cleaned}' is ambiguous; matches include: {sample}"
        )
    return cleaned


def format_action_error(action: str, target: str, error: Exception) -> str:
    """Keep an ambiguous mutation distinct from a confirmed failure."""
    if isinstance(error, MutationOutcomeUnknown):
        return f"Action '{action}' has an unknown outcome for {target}:\n{error}"
    return fmt.format_action_error(action, target, error)


def job_read_error(operation: str, job_id: str, error: Exception) -> str:
    """Friendly failure text for a job-scoped read (F6).

    * 404 → the historical ``Job 'x' not found.`` form;
    * anything else → the sanitized reason from :mod:`envelope` — the raw
      httpx message (which embeds internal orchestrator URLs) never leaks.
    """
    if isinstance(error, ValueError):
        # Ambiguous-prefix resolution errors are already user-facing text.
        return str(error)
    if http_status_of(error) == 404:
        return f"Job '{job_id}' not found."
    return f"Failed to {operation} for job {job_id}:\n{friendly_reason(error)}"


async def repo_head_line(
    client: AsyncCockpitClient,
    job_id: str,
    ref: str | None,
) -> str:
    """One-line staleness header for Gitea-backed workspace reads (F9).

    ``[repo head: <sha7> <date> — <first commit subject>]`` — the E1 ``stale``
    marker for repo-backed reads: it names the exact revision the answer came
    from so "committed state as of the last push" is verifiable, not vibes.

    Best-effort by contract: a repo read must still return its content when
    the commits lookup fails, so failures degrade to naming the explicit ref
    (or to no header at all for branch-head reads) instead of raising.
    """
    label = f"ref '{ref}'" if ref else "repo head"
    try:
        # sha="main" is the route's sentinel for "resolve the job's branch".
        result = await client.list_job_commits(job_id, sha=ref or "main", limit=1)
        commits = result.get("commits") if isinstance(result, dict) else None
        head = commits[0] if commits else {}
        sha = str(head.get("sha") or "")
        if not sha:
            raise ValueError("commits response carried no sha")
        line = f"[{label}: {sha[:7]}"
        date = str(head.get("date") or "").strip()
        if date:
            line += f" {date}"
        subject = str(head.get("message") or "").strip().splitlines()
        if subject and subject[0].strip():
            first = subject[0].strip()
            if len(first) > 100:
                first = first[:97].rstrip() + "..."
            line += f" — {first}"
        return line + "]"
    except Exception as error:  # noqa: BLE001 — header is best-effort
        logger.debug("Repo head lookup failed for job %s: %s", job_id, error)
        return f"[reading {label}]" if ref else ""


def short_id(value: Any) -> str:
    text = str(value or "")
    return text[:8] if text else "unknown"
