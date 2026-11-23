"""Name a new job from its task text, with the owner's auxiliary model.

A job is created from its task text alone (``jobs.description``) and is listed
by that text. For a job a person started, the orchestrator asks the auxiliary
model for a short title right after creation and stores it in ``jobs.title``,
the way a session gets its name after its first turn.

Rules this module keeps:

- **A person's name wins.** The write is ``set_job_title_if_unnamed``: it
  fills ``title`` only while it is NULL, so a rename that lands first is kept
  and a later rename simply replaces the generated name.
- **Best effort, off the request path.** Naming is scheduled after the job
  exists and is never awaited by admission. No model, no credential, a refusal
  or a timeout all leave the job listed by its task text, which is what every
  job was before.
- **The same model the job would use.** The auxiliary model is the account's
  default (else the installation's), routed by ``inject_model_credentials``,
  the injector dispatch uses for a job's auxiliary slot.
- **Only where a name helps.** Jobs a person or their session started, with a
  task text too long to be its own name. Loops, officers, automations and
  subjobs write a short description on purpose and are left alone.

The call is one request, one attempt, with reasoning off; its tokens are
recorded on the job like any other model use.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from orchestrator.services.usage_ledger import UsageEvent

logger = logging.getLogger(__name__)

#: Origins whose jobs a person reads in the default job list.
NAMED_ORIGINS = frozenset({"user", "session"})
#: A one-line task text up to this length is its own name.
SHORT_TASK_CHARS = 60
#: How much of the task text the model is shown. A title needs the gist, and
#: a brief states it early.
TASK_EXCERPT_CHARS = 1500
TITLE_MAX_CHARS = 80
TITLE_MAX_WORDS = 12
TITLE_TIMEOUT_SECONDS = 30.0

TITLE_SYSTEM_PROMPT = (
    "You write a short title (3-8 words) for a task someone gave to an AI "
    "agent. You are given only the task text, which may be truncated and may "
    "reference files or images you cannot see — this is expected; never "
    "comment on it, never carry out the task and never reply to it. Produce "
    "ONLY a title: a plain noun phrase that names the work, in the language "
    "the task is written in, no quotes, no punctuation, no sentences, never "
    'beginning with "I", "It", "You", or "Sorry".'
)


class JobTitle(BaseModel):
    """The naming call's structured answer: one title-shaped field.

    A schema slot has no room for a reply, which is what keeps a chat model
    from answering the task ("I don't see the attached file") instead of
    naming it. Session titles are guarded the same way.
    """

    title: str = Field(
        description=(
            "A short 3-8 word title for the task, written as a plain noun "
            "phrase in the language of the task. No quotes, no punctuation, "
            "no sentences. Never a reply to, or a comment on, the task."
        )
    )


# A model that answers the task text instead of naming it. The structured
# output is the main guard; this is the backstop, as for session titles.
_REPLY_MARKERS = (
    "i'm sorry",
    "i am sorry",
    "sorry, ",
    "as an ai",
    "i can't",
    "i cannot",
    "i'm not able",
    "i am not able",
    "i don't see",
    "i do not see",
    "it sounds like",
    "your message",
    "cut off",
)


def _collapsed(text: Any) -> str:
    return " ".join(str(text or "").split())


def task_needs_name(job: Mapping[str, Any]) -> bool:
    """Whether this new job should be named by the auxiliary model."""
    if job.get("title") or job.get("parent_job_id") or not job.get("user_id"):
        return False
    if job.get("origin") not in NAMED_ORIGINS:
        return False
    description = str(job.get("description") or "").strip()
    return "\n" in description or len(_collapsed(description)) > SHORT_TASK_CHARS


def task_excerpt(description: str, cap: int = TASK_EXCERPT_CHARS) -> str:
    """The start of the task text, cut at a word and marked as cut.

    A text chopped mid-word reads to a chat model as a message that "got cut
    off", and it replies to that instead of naming the task.
    """
    text = str(description or "").strip()
    if len(text) <= cap:
        return text
    return f"{text[:cap].rsplit(None, 1)[0]} …"


def clean_generated_title(raw: Any, *, task_text: str = "") -> str | None:
    """A usable title from the model's answer, or ``None`` to keep the task text."""
    title = _collapsed(raw).strip("\"'`“”„‚‘’«» ").rstrip(".:;,!").strip()
    if not title:
        return None
    lowered = title.lower()
    if len(title.split()) > TITLE_MAX_WORDS:
        return None
    if any(marker in lowered for marker in _REPLY_MARKERS):
        return None
    if len(title) > TITLE_MAX_CHARS:
        title = title[:TITLE_MAX_CHARS].rsplit(None, 1)[0].rstrip(".:;,-–— ")
    if not title or title == _collapsed(task_text):
        return None
    return title


@dataclass(frozen=True)
class GeneratedTitle:
    """What one naming call returned: the raw title and what it cost."""

    title: str | None
    prompt_tokens: int = 0
    cached_prompt_tokens: int = 0
    completion_tokens: int = 0


class _CapturedCall:
    """Keeps the model's reply so its token counts can be recorded.

    ``AuxiliaryLLM`` hands every call to an archiver. The agent's writes the
    audit row; here the reply is all that is needed.
    """

    def __init__(self) -> None:
        self.response: Any = None

    def archive(self, *, response: Any = None, **_: Any) -> None:
        self.response = response

    def archive_error(self, **_: Any) -> None:
        return None


def _token_counts(response: Any) -> tuple[int, int, int]:
    """(uncached prompt, cached prompt, completion) from a model reply."""
    usage = getattr(response, "usage_metadata", None) or {}
    prompt = int(usage.get("input_tokens") or 0)
    completion = int(usage.get("output_tokens") or 0)
    details = usage.get("input_token_details") or {}
    cached = min(int(details.get("cache_read") or 0), prompt)
    return prompt - cached, cached, completion


async def generate_title_with_auxiliary_model(
    section: Mapping[str, Any], excerpt: str, *, job_id: str
) -> GeneratedTitle:
    """One structured call to the routed auxiliary model.

    ``section`` is an ``auxiliary`` config section as dispatch fills it
    (model, provider, base_url, api_key, route headers, served model), so the
    client is built by the same factory, for every provider, as the agent's.
    """
    # Imported here so the rules above (who gets a name, what counts as a
    # title) can be imported and tested without the model client libraries.
    from shared.runtime.core.llm_retry import NO_RETRY
    from shared.runtime.core.loader import (
        AuxiliaryConfig,
        build_auxiliary_llm_config,
        create_llm,
        resolve_model_settings,
    )
    from shared.runtime.services.auxiliary import AuxiliaryLLM, AuxTask

    class NameJobTask(AuxTask):
        @property
        def system_prompt(self) -> str:
            return TITLE_SYSTEM_PROMPT

        def build_context(self) -> str:
            return (
                "Task text (may be truncated; may mention files or images you "
                'cannot see):\n"""\n' + excerpt + '\n"""'
            )

        @property
        def output_schema(self):
            return JobTitle

    known = {f.name for f in fields(AuxiliaryConfig)} - {"tasks"}
    auxiliary = AuxiliaryConfig(
        **{key: value for key, value in section.items() if key in known}
    )
    settings = resolve_model_settings(str(auxiliary.model))
    # Naming a task needs no reasoning, and nobody waits minutes for a title.
    llm_config = replace(
        build_auxiliary_llm_config(auxiliary, settings),
        reasoning_level="off",
        timeout=TITLE_TIMEOUT_SECONDS,
    )
    captured = _CapturedCall()
    model = AuxiliaryLLM(
        llm=create_llm(llm_config),
        structured_output_method=settings.get(
            "structured_output_method", "json_schema"
        ),
        timeout=TITLE_TIMEOUT_SECONDS,
        max_context_tokens=llm_config.model_max_context_tokens,
        archiver=captured,
        job_id=job_id,
        agent_type="orchestrator",
    )
    # One attempt: a job without a generated name is the state every job was
    # in before, so a retry would only spend tokens on it.
    parsed = await model.chain(NameJobTask(), retry_policy=NO_RETRY)
    prompt, cached, completion = _token_counts(captured.response)
    return GeneratedTitle(
        title=getattr(parsed, "title", None),
        prompt_tokens=prompt,
        cached_prompt_tokens=cached,
        completion_tokens=completion,
    )


GenerateTitle = Callable[..., Awaitable[GeneratedTitle]]


@dataclass(frozen=True)
class JobNamingDependencies:
    store: Any
    #: ``dispatch_credentials.inject_model_credentials`` bound to its owner.
    inject_model_credentials: Callable[..., Awaitable[Any]]
    #: The usage ledger, or ``None`` without the audit tier.
    ledger: Any | None = None
    generate: GenerateTitle = generate_title_with_auxiliary_model
    logger: logging.Logger = field(default=logger)


async def _auxiliary_section(
    job: Mapping[str, Any], dependencies: JobNamingDependencies
) -> dict[str, Any] | None:
    """The routed auxiliary model for this job's owner, or ``None``.

    The account's default first, then the installation's, as dispatch chooses
    a job's auxiliary model when its expert pins none. A model that cannot be
    routed is skipped rather than reported: the job loses nothing.
    """
    store = dependencies.store
    user_id = str(job["user_id"])
    project_id = str(job["project_id"]) if job.get("project_id") else None
    user_settings = await store.get_user_settings(user_id) or {}
    resolved_keys = await store.resolve_api_keys_for_job(user_id, project_id)
    candidates: list[str] = []
    for model in (
        user_settings.get("default_auxiliary_model"),
        await store.resolve_default_for_capability("auxiliary"),
    ):
        if model and str(model) not in candidates:
            candidates.append(str(model))
    for model in candidates:
        section: dict[str, Any] = {"model": model}
        reason = await dependencies.inject_model_credentials(
            section=section,
            model_id=model,
            user_id=user_id,
            resolved_keys=resolved_keys,
            capability="auxiliary",
        )
        if reason is None and (section.get("api_key") or section.get("base_url")):
            return section
    return None


def _usage_events(
    generated: GeneratedTitle, *, job: Mapping[str, Any], model: str
) -> list[UsageEvent]:
    common = dict(
        category="llm",
        resource=model,
        source="orchestrator",
        source_id=uuid.uuid4().hex,
        ts=datetime.now(timezone.utc),
        user_id=str(job["user_id"]) if job.get("user_id") else None,
        project_id=str(job["project_id"]) if job.get("project_id") else None,
        ref_kind="job",
        ref_id=str(job["id"]),
        details={"via": "job_title"},
    )
    return [
        UsageEvent(quantity=quantity, unit=unit, **common)
        for unit, quantity in (
            ("prompt-token", generated.prompt_tokens),
            ("cached-prompt-token", generated.cached_prompt_tokens),
            ("completion-token", generated.completion_tokens),
        )
        if quantity
    ]


async def _record_usage(
    generated: GeneratedTitle,
    *,
    job: Mapping[str, Any],
    model: str,
    dependencies: JobNamingDependencies,
) -> None:
    ledger = dependencies.ledger
    if ledger is None or not getattr(ledger, "is_available", False):
        return
    events = _usage_events(generated, job=job, model=model)
    if not events:
        return
    try:
        await ledger.record_events(events)
    except Exception:
        dependencies.logger.debug(
            "Job naming: usage of job %s was not recorded", job.get("id"), exc_info=True
        )


async def name_job(
    job: Mapping[str, Any], *, dependencies: JobNamingDependencies
) -> str | None:
    """Give ``job`` a generated title unless it has a name by then.

    Returns the stored title, or ``None`` when the job keeps its task text as
    its name. Never raises: nothing depends on a job having a title.
    """
    job_id = str(job.get("id") or "")
    log = dependencies.logger
    try:
        if not job_id or not task_needs_name(job):
            return None
        section = await _auxiliary_section(job, dependencies)
        if section is None:
            log.info("Job naming: no auxiliary model is usable for job %s", job_id)
            return None
        description = str(job.get("description") or "")
        generated = await dependencies.generate(
            section, task_excerpt(description), job_id=job_id
        )
        await _record_usage(
            generated, job=job, model=str(section["model"]), dependencies=dependencies
        )
        title = clean_generated_title(generated.title, task_text=description)
        if title is None:
            log.info(
                "Job naming: the answer for job %s is not a title (%r)",
                job_id,
                str(generated.title or "")[:120],
            )
            return None
        if not await dependencies.store.set_job_title_if_unnamed(job_id, title):
            # Renamed by a person, or deleted, while the model answered.
            return None
        log.info("Job naming: job %s is named %r", job_id, title)
        return title
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.warning(
            "Job naming: job %s keeps its task text as its name (%s: %s)",
            job_id,
            type(exc).__name__,
            str(exc)[:200],
        )
        return None


class JobNamingTasks:
    """The naming calls in flight, so shutdown can stop them before the
    stores they write to are closed."""

    def __init__(self) -> None:
        self._tasks: set[asyncio.Task] = set()

    def track(self, task: asyncio.Task) -> asyncio.Task:
        """Own ``task`` until it finishes."""
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def schedule(self, coro: Coroutine[Any, Any, Any], *, name: str) -> asyncio.Task:
        return self.track(asyncio.create_task(coro, name=name))

    def pending(self) -> list[asyncio.Task]:
        return list(self._tasks)

    async def drain(self) -> None:
        """Cancel and await every naming call. A cancelled call wrote nothing."""
        pending = self.pending()
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


def schedule_job_naming(
    job: Mapping[str, Any],
    *,
    tasks: JobNamingTasks,
    dependencies: Callable[[], JobNamingDependencies],
) -> asyncio.Task | None:
    """Start naming a just-created job in the background, if it wants a name.

    Called by job creation with the new row. The eligibility check runs here,
    so a job that needs no name costs nothing, not even a task.
    """
    if not task_needs_name(job):
        return None
    return tasks.schedule(
        name_job(dict(job), dependencies=dependencies()),
        name=f"job-naming-{job.get('id')}",
    )
