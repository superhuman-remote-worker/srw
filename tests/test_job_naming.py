"""A new job is named by the auxiliary model, and only where that helps.

The rules live in ``orchestrator.services.job_naming``: which jobs get a name,
which model answers, what counts as a title, and that a person's own name is
never replaced. The guarded SQL write runs in
test_job_rename_real_postgres.py.
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage

from orchestrator.services import job_naming
from orchestrator.services.job_naming import (
    GeneratedTitle,
    JobNamingDependencies,
    JobNamingTasks,
    JobTitle,
    clean_generated_title,
    generate_title_with_auxiliary_model,
    name_job,
    schedule_job_naming,
    task_excerpt,
    task_needs_name,
)

JOB = "33333333-3333-4333-8333-333333333333"
USER = "11111111-1111-4111-8111-111111111111"
PROJECT = "22222222-2222-4222-8222-222222222222"
LONG_TASK = (
    "Read every pull request merged this week and write a summary for the "
    "release notes.\n\nGroup the changes by component and link each one."
)


def job(**fields):
    return {
        "id": JOB,
        "user_id": USER,
        "project_id": PROJECT,
        "origin": "user",
        "parent_job_id": None,
        "title": None,
        "description": LONG_TASK,
        **fields,
    }


class TestTaskNeedsName:
    def test_a_long_task_a_person_started_gets_a_name(self):
        assert task_needs_name(job()) is True

    def test_a_job_a_session_started_gets_one_too(self):
        assert task_needs_name(job(origin="session")) is True

    def test_a_short_one_line_task_is_its_own_name(self):
        assert task_needs_name(job(description="Fix the login redirect bug")) is False
        assert task_needs_name(job(description="x" * 60)) is False
        assert task_needs_name(job(description="x" * 61)) is True

    def test_a_short_task_over_several_lines_still_gets_a_name(self):
        """A list row shows one line; a multi-line text is not a name."""
        assert task_needs_name(job(description="Fix the bug.\nAdd a test.")) is True

    @pytest.mark.parametrize(
        "origin", ["automation", "loop", "officer", "subjob", "bench", None]
    )
    def test_machine_made_jobs_keep_the_description_they_were_given(self, origin):
        assert task_needs_name(job(origin=origin)) is False

    def test_a_subjob_a_named_job_or_an_ownerless_job_is_left_alone(self):
        assert task_needs_name(job(parent_job_id=JOB)) is False
        assert task_needs_name(job(title="Release notes")) is False
        assert task_needs_name(job(user_id=None)) is False


class TestTaskExcerpt:
    def test_a_task_that_fits_is_sent_whole(self):
        assert task_excerpt("  Summarise the contract.  ") == "Summarise the contract."

    def test_a_long_task_is_cut_at_a_word_and_marked_as_cut(self):
        text = "word " * 500
        excerpt = task_excerpt(text, cap=103)

        assert excerpt.endswith(" …")
        # Never mid-word: every piece before the mark is a whole word.
        assert set(excerpt[:-2].split()) == {"word"}
        assert len(excerpt) <= 105


class TestCleanGeneratedTitle:
    def test_a_plain_title_is_kept(self):
        assert clean_generated_title("Weekly release notes summary") == (
            "Weekly release notes summary"
        )

    def test_quotes_whitespace_and_a_trailing_full_stop_are_removed(self):
        assert clean_generated_title('  "Weekly  release\nnotes." ') == (
            "Weekly release notes"
        )
        assert clean_generated_title("„Wöchentliche Versionshinweise“") == (
            "Wöchentliche Versionshinweise"
        )

    @pytest.mark.parametrize("raw", [None, "", "   ", '""', "."])
    def test_nothing_is_not_a_title(self, raw):
        assert clean_generated_title(raw) is None

    def test_a_sentence_long_answer_is_not_a_title(self):
        assert clean_generated_title(" ".join(["word"] * 13)) is None
        assert clean_generated_title(" ".join(["word"] * 12)) is not None

    @pytest.mark.parametrize(
        "reply",
        [
            "I'm sorry, I can't see the attached file",
            "It sounds like your message got cut off",
            "As an AI I cannot open files",
        ],
    )
    def test_a_reply_to_the_task_is_never_a_title(self, reply):
        assert clean_generated_title(reply) is None

    def test_an_overlong_title_is_cut_at_a_word(self):
        raw = "Extraordinarily " * 6 + "comprehensive documentation overhaul"
        title = clean_generated_title(raw)

        assert title is not None and len(title) <= 80
        assert raw.startswith(title) and raw[len(title)] == " "

    def test_the_task_text_given_back_is_not_a_name(self):
        assert clean_generated_title("Fix the bug", task_text="Fix  the bug") is None


def dependencies(
    *,
    account_model=None,
    system_model="aux-system",
    routable=("aux-system", "aux-account"),
    generated=GeneratedTitle(title="Weekly release notes summary"),
    named=True,
    ledger=None,
):
    store = SimpleNamespace(
        get_user_settings=AsyncMock(
            return_value={"default_auxiliary_model": account_model}
        ),
        resolve_api_keys_for_job=AsyncMock(return_value={"openai": "sk-user"}),
        resolve_default_for_capability=AsyncMock(return_value=system_model),
        set_job_title_if_unnamed=AsyncMock(return_value=named),
    )

    async def inject(*, section, model_id, user_id, resolved_keys, capability):
        if model_id not in routable:
            return "disabled"
        section.update(provider="openai", api_key=resolved_keys["openai"])
        return None

    generate = AsyncMock(
        side_effect=generated if isinstance(generated, Exception) else None,
        return_value=None if isinstance(generated, Exception) else generated,
    )
    return JobNamingDependencies(
        store=store,
        inject_model_credentials=AsyncMock(side_effect=inject),
        ledger=ledger,
        generate=generate,
    )


class TestNameJob:
    @pytest.mark.asyncio
    async def test_the_generated_title_is_stored_for_an_unnamed_job(self):
        deps = dependencies()

        assert (
            await name_job(job(), dependencies=deps) == "Weekly release notes summary"
        )

        deps.store.set_job_title_if_unnamed.assert_awaited_once_with(
            JOB, "Weekly release notes summary"
        )
        section, excerpt = deps.generate.await_args.args
        assert section == {
            "model": "aux-system",
            "provider": "openai",
            "api_key": "sk-user",
        }
        assert excerpt == LONG_TASK
        assert deps.generate.await_args.kwargs == {"job_id": JOB}

    @pytest.mark.asyncio
    async def test_the_route_is_resolved_for_the_jobs_owner_and_project(self):
        deps = dependencies()

        await name_job(job(), dependencies=deps)

        deps.store.resolve_api_keys_for_job.assert_awaited_once_with(USER, PROJECT)
        call = deps.inject_model_credentials.await_args.kwargs
        assert (call["user_id"], call["capability"]) == (USER, "auxiliary")

    @pytest.mark.asyncio
    async def test_the_accounts_auxiliary_model_comes_before_the_installations(self):
        deps = dependencies(account_model="aux-account")

        await name_job(job(), dependencies=deps)

        assert deps.generate.await_args.args[0]["model"] == "aux-account"
        assert deps.inject_model_credentials.await_count == 1

    @pytest.mark.asyncio
    async def test_an_account_model_that_cannot_run_falls_back_to_the_installations(
        self,
    ):
        deps = dependencies(account_model="retired-model")

        assert await name_job(job(), dependencies=deps) is not None

        assert deps.generate.await_args.args[0]["model"] == "aux-system"

    @pytest.mark.asyncio
    async def test_the_same_model_from_both_places_is_tried_once(self):
        deps = dependencies(account_model="aux-system")

        await name_job(job(), dependencies=deps)

        assert deps.inject_model_credentials.await_count == 1

    @pytest.mark.asyncio
    async def test_without_a_usable_auxiliary_model_the_job_keeps_its_text(self):
        deps = dependencies(system_model=None)

        assert await name_job(job(), dependencies=deps) is None

        deps.generate.assert_not_awaited()
        deps.store.set_job_title_if_unnamed.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_model_routed_without_any_transport_is_not_called(self):
        """No key and no endpoint would send the request to a default host."""
        deps = dependencies()
        deps.inject_model_credentials.side_effect = AsyncMock(return_value=None)

        assert await name_job(job(), dependencies=deps) is None
        deps.generate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_rename_that_landed_first_is_kept(self):
        deps = dependencies(named=False)

        assert await name_job(job(), dependencies=deps) is None

    @pytest.mark.asyncio
    async def test_a_reply_instead_of_a_title_is_not_stored(self):
        deps = dependencies(
            generated=GeneratedTitle(title="I'm sorry, I can't see the attached file")
        )

        assert await name_job(job(), dependencies=deps) is None
        deps.store.set_job_title_if_unnamed.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failing_model_never_fails_anything_else(self, caplog):
        deps = dependencies(generated=TimeoutError("auxiliary endpoint timed out"))

        with caplog.at_level(logging.WARNING, logger=job_naming.__name__):
            assert await name_job(job(), dependencies=deps) is None

        deps.store.set_job_title_if_unnamed.assert_not_awaited()
        assert "keeps its task text as its name" in caplog.text

    @pytest.mark.asyncio
    async def test_a_job_that_needs_no_name_costs_no_lookup_and_no_call(self):
        deps = dependencies()

        assert await name_job(job(origin="loop"), dependencies=deps) is None

        deps.store.get_user_settings.assert_not_awaited()
        deps.generate.assert_not_awaited()


class TestUsage:
    def ledger(self, available=True):
        return SimpleNamespace(is_available=available, record_events=AsyncMock())

    @pytest.mark.asyncio
    async def test_the_calls_tokens_are_recorded_on_the_job(self):
        ledger = self.ledger()
        deps = dependencies(
            generated=GeneratedTitle(
                title="Weekly release notes summary",
                prompt_tokens=180,
                cached_prompt_tokens=40,
                completion_tokens=9,
            ),
            ledger=ledger,
        )

        await name_job(job(), dependencies=deps)

        events = ledger.record_events.await_args.args[0]
        assert {(e.unit, e.quantity) for e in events} == {
            ("prompt-token", 180),
            ("cached-prompt-token", 40),
            ("completion-token", 9),
        }
        assert {
            (
                e.category,
                e.resource,
                e.source,
                e.ref_kind,
                e.ref_id,
                e.user_id,
                e.project_id,
            )
            for e in events
        } == {("llm", "aux-system", "orchestrator", "job", JOB, USER, PROJECT)}
        assert all(e.details == {"via": "job_title"} for e in events)
        # One call, one idempotency key.
        assert len({e.source_id for e in events}) == 1

    @pytest.mark.asyncio
    async def test_tokens_are_recorded_even_when_the_answer_is_not_used(self):
        """The call was paid for whether or not its answer became the name."""
        ledger = self.ledger()
        deps = dependencies(
            generated=GeneratedTitle(title="I'm sorry", prompt_tokens=120),
            ledger=ledger,
        )

        assert await name_job(job(), dependencies=deps) is None
        ledger.record_events.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_ledger_an_unavailable_one_or_no_counts_record_nothing(self):
        unavailable = self.ledger(available=False)
        await name_job(job(), dependencies=dependencies(ledger=unavailable))
        unavailable.record_events.assert_not_awaited()

        without_counts = self.ledger()
        await name_job(job(), dependencies=dependencies(ledger=without_counts))
        without_counts.record_events.assert_not_awaited()

        assert await name_job(job(), dependencies=dependencies(ledger=None)) is not None

    @pytest.mark.asyncio
    async def test_a_ledger_error_does_not_lose_the_name(self):
        ledger = self.ledger()
        ledger.record_events.side_effect = RuntimeError("audit store is down")
        deps = dependencies(
            generated=GeneratedTitle(title="Release notes", prompt_tokens=10),
            ledger=ledger,
        )

        assert await name_job(job(), dependencies=deps) == "Release notes"


class TestScheduling:
    @pytest.mark.asyncio
    async def test_a_job_that_wants_a_name_is_named_in_the_background(self):
        tasks = JobNamingTasks()
        deps = dependencies()

        task = schedule_job_naming(job(), tasks=tasks, dependencies=lambda: deps)

        assert task is not None and tasks.pending() == [task]
        assert await task == "Weekly release notes summary"
        await asyncio.sleep(0)
        assert tasks.pending() == []

    @pytest.mark.asyncio
    async def test_a_job_that_needs_no_name_starts_nothing(self):
        tasks = JobNamingTasks()
        build = MagicMock()

        assert (
            schedule_job_naming(job(origin="officer"), tasks=tasks, dependencies=build)
            is None
        )

        assert tasks.pending() == []
        build.assert_not_called()

    @pytest.mark.asyncio
    async def test_shutdown_stops_a_call_in_flight_before_it_writes(self):
        tasks = JobNamingTasks()
        deps = dependencies()
        started = asyncio.Event()

        async def never_answers(*_args, **_kwargs):
            started.set()
            await asyncio.Event().wait()

        deps.generate.side_effect = never_answers
        task = schedule_job_naming(job(), tasks=tasks, dependencies=lambda: deps)
        await started.wait()

        await tasks.drain()

        assert task.cancelled()
        assert tasks.pending() == []
        deps.store.set_job_title_if_unnamed.assert_not_awaited()
        # A second drain has nothing left to do.
        await tasks.drain()


class TestGenerateWithAuxiliaryModel:
    """The real call path, with the provider client replaced."""

    def fake_llm(self, parsed, usage=None):
        llm = MagicMock()
        structured = AsyncMock()
        structured.ainvoke = AsyncMock(
            return_value={
                "raw": AIMessage(content="{}", usage_metadata=usage),
                "parsed": parsed,
                "parsing_error": None,
            }
        )
        llm.with_structured_output = MagicMock(return_value=structured)
        return llm, structured

    @pytest.mark.asyncio
    async def test_one_structured_call_on_the_routed_model_with_reasoning_off(
        self, monkeypatch
    ):
        from shared.runtime.core import loader

        llm, structured = self.fake_llm(
            JobTitle(title="Weekly release notes summary"),
            usage={
                "input_tokens": 220,
                "output_tokens": 9,
                "total_tokens": 229,
                "input_token_details": {"cache_read": 40},
            },
        )
        built = []
        monkeypatch.setattr(
            loader,
            "create_llm",
            lambda config, limits=None: built.append(config) or llm,
        )
        section = {
            "model": "gpt-5-mini",
            "provider": "openai",
            "api_key": "sk-user",
            "base_url": None,
            "extra_headers": None,
            "served_model": "gpt-5-mini-2026",
            # Written by dispatch, not a field of the auxiliary config.
            "client_protocol": "chat",
        }

        generated = await generate_title_with_auxiliary_model(
            section, "Read every pull request merged this week …", job_id=JOB
        )

        assert generated == GeneratedTitle(
            title="Weekly release notes summary",
            prompt_tokens=180,
            cached_prompt_tokens=40,
            completion_tokens=9,
        )
        (config,) = built
        assert (config.model, config.provider, config.api_key, config.served_model) == (
            "gpt-5-mini",
            "openai",
            "sk-user",
            "gpt-5-mini-2026",
        )
        assert config.reasoning_level == "off"
        assert config.timeout == job_naming.TITLE_TIMEOUT_SECONDS
        assert llm.with_structured_output.call_args.args[0] is JobTitle
        # One attempt: nothing is worth retrying for a name.
        structured.ainvoke.assert_awaited_once()
        system, human = structured.ainvoke.await_args.args[0]
        assert system.content == job_naming.TITLE_SYSTEM_PROMPT
        assert "Read every pull request merged this week …" in human.content

    @pytest.mark.asyncio
    async def test_a_reply_without_token_counts_reports_zero(self, monkeypatch):
        from shared.runtime.core import loader

        llm, _ = self.fake_llm(JobTitle(title="Release notes"))
        monkeypatch.setattr(loader, "create_llm", lambda config, limits=None: llm)

        generated = await generate_title_with_auxiliary_model(
            {"model": "gpt-5-mini", "api_key": "sk"}, "Summarise it", job_id=JOB
        )

        assert generated == GeneratedTitle(title="Release notes")

    @pytest.mark.asyncio
    async def test_a_provider_error_is_raised_to_the_caller_after_one_attempt(
        self, monkeypatch
    ):
        from shared.runtime.core import loader

        llm = MagicMock()
        structured = AsyncMock()
        structured.ainvoke = AsyncMock(side_effect=RuntimeError("502 bad gateway"))
        llm.with_structured_output = MagicMock(return_value=structured)
        llm.ainvoke = AsyncMock(side_effect=RuntimeError("502 bad gateway"))
        monkeypatch.setattr(loader, "create_llm", lambda config, limits=None: llm)

        with pytest.raises(Exception):
            await generate_title_with_auxiliary_model(
                {"model": "gpt-5-mini", "api_key": "sk"}, "Summarise it", job_id=JOB
            )

        structured.ainvoke.assert_awaited_once()


class TestComposition:
    """How the application binds the namer (``application/jobs.py``)."""

    def resources(self):
        return SimpleNamespace(
            postgres_db=SimpleNamespace(name="app-store"),
            usage_ledger=SimpleNamespace(name="ledger"),
            job_naming_tasks=JobNamingTasks(),
        )

    @pytest.mark.asyncio
    async def test_the_route_is_dispatchs_own_injector_on_this_apps_store(
        self, monkeypatch
    ):
        from orchestrator.application import jobs as jobs_composition
        from orchestrator.services import dispatch_credentials

        seen = {}

        async def inject(*, dependencies, **kwargs):
            seen.update(kwargs, store=dependencies.store)
            return None

        monkeypatch.setattr(dispatch_credentials, "inject_model_credentials", inject)
        resources = self.resources()

        deps = jobs_composition.job_naming_dependencies(resources)
        await deps.inject_model_credentials(
            section={},
            model_id="aux",
            user_id=USER,
            resolved_keys={},
            capability="auxiliary",
        )

        assert deps.store is resources.postgres_db
        assert deps.ledger is resources.usage_ledger
        assert seen["store"] is resources.postgres_db
        assert (seen["model_id"], seen["capability"]) == ("aux", "auxiliary")

    def test_app_resources_own_the_naming_calls_in_flight(self):
        from dataclasses import fields

        from orchestrator.application.resources import ApplicationResources

        field = next(
            f for f in fields(ApplicationResources) if f.name == "job_naming_tasks"
        )
        assert isinstance(field.default_factory(), JobNamingTasks)
