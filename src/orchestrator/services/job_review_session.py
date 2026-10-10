"""Live pull-request status and the server-derived job review session.

Extracted verbatim from ``orchestrator.main`` (R1.B04 lane C). Four
properties are load-bearing and moved unchanged:

* **Connector credentials never cross REST.** The caller supplies only a job
  id; the repository connector is resolved server-side after the ordinary
  job-access check and only its derived forge target is used.
* **The review thread's seed is server-authored.** There is deliberately no
  request body: model, scope, connectors and the delivered branch are all
  derived from stored state, and the seed is written to a Pydantic *private*
  attribute, which JSON can never populate — so neither the public
  thread-create endpoint nor the model-facing MCP tool can author it.
* **Untrusted job strings are bounded, never interpolated raw.**
  ``_brief_text`` collapses whitespace and truncates; ``_review_session_config_values``
  reports the *keys* of dropped overrides and never their values, because job
  config can carry environment/credential material that must not enter the
  transcript.
* **Worker and session profiles are not interchangeable.** A worker expert
  falls back to the session base rather than being copied across schemas, and
  the opening event says so.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import logging
from typing import Any

from fastapi import HTTPException, Request

from shared.runtime.core.loader import canonical_config_name

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobReviewSessionDependencies:
    """Collaborators for one review-session operation, resolved per invocation.

    ``create_thread`` and the two request types stay owned by the application
    (the thread-creation batch); they arrive here as an injected callable and
    injected types so this module never imports the application module.
    """

    store: Any
    create_thread: Callable[[Any, Request], Awaitable[dict[str, Any]]]
    thread_create_request: type
    trusted_thread_seed: type
    bundled_expert_bundle: Callable[[str], dict[str, Any] | None]


async def get_job_pull_request_status(
    *,
    job_id: str,
    authorized_job: dict[str, Any],
    dependencies: JobReviewSessionDependencies,
) -> dict[str, Any]:
    """Read the live state of the PR recorded by ``repo_open_pr``.

    The caller supplies only a job id. The server resolves both the persisted
    PR identity and its credential-bearing repository connector after the
    ordinary job-access check; connector credentials never cross REST.
    """
    job = authorized_job
    from orchestrator.services.job_delivery import (
        find_pull_request_repository,
        forge_repo_from_datasource,
        parse_job_pull_request,
    )
    from shared.runtime.services.forge import ForgeError, get_pull_request_status

    pull_request = parse_job_pull_request(job.get("context"))
    if pull_request is None:
        raise HTTPException(
            status_code=404, detail="This job has no recorded pull request"
        )

    try:
        datasources = await dependencies.store.resolve_datasources_for_job(job_id)
    except Exception as exc:
        logger.warning(
            "Could not resolve repository connector for job PR status: %s",
            type(exc).__name__,
        )
        raise HTTPException(
            status_code=500, detail="Could not resolve this job's repository"
        ) from exc
    datasource = find_pull_request_repository(pull_request, datasources)
    if datasource is None:
        raise HTTPException(
            status_code=409,
            detail="The recorded pull request's repository is no longer attached",
        )

    try:
        target = forge_repo_from_datasource(datasource)
        status = await get_pull_request_status(target, pull_request.number)
    except ForgeError as exc:
        logger.warning(
            "Live pull request status failed for job %s: %s",
            job_id,
            type(exc).__name__,
        )
        raise HTTPException(
            status_code=502, detail="Live pull request status is unavailable"
        ) from exc

    return {
        "forge": pull_request.forge,
        "repo": pull_request.repo,
        **status,
        # A forge response may omit these display fields; the record written at
        # creation is still authoritative for the delivery identity.
        "url": status.get("url") or pull_request.url,
        "head": status.get("head") or pull_request.head,
        "base": status.get("base") or pull_request.base,
    }


def _review_session_config_values(
    job: dict[str, Any],
) -> tuple[str | None, float | None, list[str]]:
    """Extract only fields supported by the ordinary session-create surface."""
    from orchestrator.services.job_delivery import parse_json_object

    override = parse_json_object(job.get("config_override")) or {}
    resolved = parse_json_object(job.get("resolved_config")) or {}
    resolved_agent = parse_json_object(resolved.get("agent")) or {}
    resolved_llm = parse_json_object(resolved_agent.get("llm")) or {}
    override_llm = parse_json_object(override.get("llm")) or {}

    raw_model = resolved_llm.get("model", override_llm.get("model"))
    model = (
        raw_model.strip() if isinstance(raw_model, str) and raw_model.strip() else None
    )

    raw_temperature = resolved_llm.get("temperature", override_llm.get("temperature"))
    temperature = (
        float(raw_temperature)
        if isinstance(raw_temperature, (int, float))
        and not isinstance(raw_temperature, bool)
        else None
    )

    dropped: list[str] = []
    for key, value in override.items():
        if key == "llm" and isinstance(value, dict):
            dropped.extend(
                f"llm.{subkey}"
                for subkey in value
                if subkey not in {"model", "temperature"}
            )
        elif key == "interactive" and isinstance(value, dict):
            dropped.extend(f"interactive.{subkey}" for subkey in value)
        elif key not in {"llm", "interactive"}:
            # State only the key, never its value: unsupported job config can
            # contain environment/credential material that must not enter the
            # transcript.
            dropped.append(str(key))
    return model, temperature, dropped


def _review_session_config_name(
    job: dict[str, Any],
    *,
    bundled_expert_bundle: Callable[[str], dict[str, Any] | None],
) -> str:
    """Keep a session profile when one is already present; map workers safely."""
    source = canonical_config_name(str(job.get("config_name") or "worker_base"))
    if source == "session_base":
        return source
    # Bundled expert type is inferred from its declared base by the existing
    # catalogue reader. Arbitrary paths and worker experts deliberately fall
    # back to the session base; worker/session schemas are not interchangeable.
    bundle = bundled_expert_bundle(source)
    if bundle and bundle.get("expert_type") == "session":
        return source
    return "session_base"


async def _review_session_datasource_ids(
    store: Any, datasources: list[dict[str, Any]], *, owner_id: str
) -> list[str]:
    """The job's connectors its review session can attach, in job order.

    The session takes them as an explicit list, which session admission
    authorizes whole, so a row no session of ``owner_id`` may attach would
    refuse the review: a connector its driver serves to jobs only (the
    project folder copy, ``execution_kind_refuses``), and another user's own
    connector (the job owner's outbox, when someone else reviews). An ID
    without a policy row stays and fails closed there.
    """
    from orchestrator.services.datasource_policy import execution_kind_refuses
    from shared.connectors.platform import native_user

    ids = [str(datasource["id"]) for datasource in datasources]
    rows = {
        str(row["id"]): row
        for row in (await store.get_datasource_policy_rows(ids) or [])
    }

    def attachable(row: Any) -> bool:
        if row is None:
            return True
        if execution_kind_refuses(row, "session"):
            return False
        user = native_user(row)
        return user is None or user == owner_id

    return [value for value in ids if attachable(rows.get(value))]


def _brief_text(value: Any, *, limit: int) -> str:
    """Bound one untrusted job string for a plain-text opening event."""
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())[:limit]


def _review_session_opening_event(
    job: dict[str, Any],
    *,
    pull_request: Any,
    session_config_name: str,
    dropped_settings: list[str],
) -> str:
    """Render the durable, non-user-bubble context for a review thread."""
    from orchestrator.services.job_delivery import parse_json_object

    context = parse_json_object(job.get("context")) or {}
    freeze = parse_json_object(job.get("freeze_data")) or {}
    deliverables = context.get("required_deliverables")
    if not isinstance(deliverables, list):
        deliverables = freeze.get("deliverables")
    safe_deliverables = [
        text
        for item in (deliverables if isinstance(deliverables, list) else [])[:50]
        if (text := _brief_text(item, limit=250))
    ]

    source_config = canonical_config_name(str(job.get("config_name") or "worker_base"))
    source_label = _brief_text(source_config, limit=200)
    session_label = _brief_text(session_config_name, limit=200)
    repository = _brief_text(pull_request.repo, limit=500)
    delivered_branch = _brief_text(pull_request.head, limit=500)
    pull_request_url = _brief_text(pull_request.url, limit=2_000)
    base_branch = _brief_text(pull_request.base, limit=500)
    lines = [
        "[Server-derived job review context]",
        f"Job: {job['id']}",
        f"Task: {_brief_text(job.get('description'), limit=2_000)}",
        f"Source repository: {repository}",
        f"Delivered branch: {delivered_branch}",
        f"Pull request: #{pull_request.number} ({pull_request_url})",
        f"Base branch: {base_branch}",
        (
            "Workspace: fresh sandbox checkout through the job's repository "
            "connector; the scratch job workspace is not reused."
        ),
        (
            "Permission mode: supervised for review; the worker's automation "
            "mode is not inherited."
        ),
    ]
    summary = _brief_text(freeze.get("summary"), limit=2_000)
    if summary:
        lines.append(f"Completion summary: {summary}")
    if safe_deliverables:
        lines.append("Declared deliverables:")
        lines.extend(f"- {item}" for item in safe_deliverables)
    if source_config != session_config_name:
        lines.append(
            f"Session profile: {session_label}; worker profile "
            f"{source_label} is not copied because worker and session "
            "profiles have different schemas."
        )
    if dropped_settings:
        safe_dropped = [
            text
            for setting in dropped_settings[:100]
            if (text := _brief_text(setting, limit=200))
        ]
        lines.append(
            "Job-only settings not inherited: " + ", ".join(safe_dropped) + "."
        )
    rendered = "\n".join(lines)
    if len(rendered) > 19_500:
        return rendered[:19_499].rstrip() + "…"
    return rendered


async def create_job_review_session(
    *,
    request: Request,
    job_id: str,
    user: dict[str, Any],
    authorized_job: dict[str, Any],
    dependencies: JobReviewSessionDependencies,
) -> dict[str, Any]:
    """Create a fresh interactive review from an access-checked job id.

    There is intentionally no request body. Model, scope, connectors and the
    delivered branch are all derived from stored server state; the MCP session
    tool remains unchanged and has no route to ``config_override``. ``user``
    is the caller, whose session it is: it takes the job's connectors a
    session of theirs can attach (``_review_session_datasource_ids``).
    """
    job = authorized_job
    from orchestrator.services.job_delivery import (
        find_pull_request_repository,
        parse_job_pull_request,
        repository_host,
    )

    pull_request = parse_job_pull_request(job.get("context"))
    if pull_request is None:
        raise HTTPException(
            status_code=409,
            detail="This job has no recorded delivered branch to review",
        )

    try:
        datasources = await dependencies.store.resolve_datasources_for_job(job_id)
    except Exception as exc:
        logger.warning(
            "Could not resolve connectors for job review session %s: %s",
            job_id,
            type(exc).__name__,
        )
        raise HTTPException(
            status_code=500,
            detail="Could not resolve this job's connectors",
        ) from exc

    source = find_pull_request_repository(pull_request, datasources)
    if source is None or not source.get("id"):
        raise HTTPException(
            status_code=409,
            detail="The delivered repository is no longer attached to this job",
        )
    if any(not datasource.get("id") for datasource in datasources):
        raise HTTPException(
            status_code=500,
            detail="Could not resolve this job's connectors",
        )

    session_datasource_ids = await _review_session_datasource_ids(
        dependencies.store, datasources, owner_id=str(user["id"])
    )

    model, temperature, dropped = _review_session_config_values(job)
    session_config_name = _review_session_config_name(
        job, bundled_expert_bundle=dependencies.bundled_expert_bundle
    )
    title_description = _brief_text(job.get("description"), limit=80)
    title = f"Review job {str(job['id'])[:8]}"
    if title_description:
        title = f"{title}: {title_description}"

    review_delivery = {
        "job_id": str(job["id"]),
        "datasource_id": str(source["id"]),
        "forge": pull_request.forge,
        "repository_host": repository_host(str(source["connection_url"])),
        "repo": pull_request.repo,
        "branch": pull_request.head,
        "base": pull_request.base,
        "pull_request": {
            "number": pull_request.number,
            "url": pull_request.url,
        },
    }
    body = dependencies.thread_create_request(
        title=title,
        config_name=session_config_name,
        project_ids=[str(job["project_id"])] if job.get("project_id") else None,
        datasource_ids=session_datasource_ids,
        model=model,
        temperature=temperature,
        permission_mode="supervised",
        # A clone-based repository requires a shell workspace. This is a fixed
        # property of the review workflow, not a copied or caller-supplied job
        # override (the job's vm/virtual choice is deliberately ignored).
        config_override={"workspace": {"backend": "sandbox"}},
    )
    body._trusted_seed = dependencies.trusted_thread_seed(
        metadata={"review_delivery": review_delivery},
        opening_event=_review_session_opening_event(
            job,
            pull_request=pull_request,
            session_config_name=session_config_name,
            dropped_settings=dropped,
        ),
    )
    created = await dependencies.create_thread(body, request)
    return {
        "job_id": str(job["id"]),
        "thread_id": created["thread_id"],
        "status": created.get("status", "created"),
    }
