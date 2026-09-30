"""The auxiliary summarizer's prompt contract and output guards.

Compaction refactor WP1 + WP2
(knowledge-base/knowledge/features/compaction_refactor_fidelity_and_fork_strategy.md):
transcript first and instruction last, an explicit merge contract for the prior
summary, a history-is-data rule, Markdown sections instead of JSON, output that
is validated, and a file list recorded from the tool calls.
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from agent.core.context import ContextConfig, ContextManager
from agent.core.summarizer import (
    FILES_SECTION_HEADING,
    MAX_ATTEMPTS,
    SummarizationEngine,
    SummarizationFailed,
    SummaryRejected,
    files_section,
    load_summarizer_prompt,
    strip_files_section,
    validate_summary_message,
)
from shared.runtime.services.auxiliary import AuxiliaryLLM, SummarizeTask

PROMPTS = Path(__file__).resolve().parents[1] / "config" / "prompts"
VARIANTS = sorted(PROMPTS.glob("summarization_prompt*.txt"))
CHECKPOINT = (
    "## Objective\n- Ship the parser.\n\n## Work State\n### Completed\n- (none)"
)


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch):
    monkeypatch.setattr("agent.core.summarizer.BACKOFF_SECONDS", (0.0, 0.0))


def _aux(*responses):
    llm = MagicMock()
    llm.ainvoke = AsyncMock(side_effect=list(responses))
    return AuxiliaryLLM(llm=llm, max_context_tokens=50_000)


def _reply(text, finish="stop"):
    return AIMessage(content=text, response_metadata={"finish_reason": finish})


class TestRequestShape:
    def test_transcript_first_instruction_last(self):
        task = SummarizeTask("[User]: hi", VARIANTS[0].read_text())
        context = task.build_context()

        assert context.index("<conversation>") < context.index(
            "Write the checkpoint now"
        )
        assert context.rstrip().endswith("tokens.")
        assert "do not call tools" in context.split("<conversation>")[-1]

    def test_merge_contract_only_with_a_prior_summary(self):
        plain = SummarizeTask("[User]: hi", "## Objective").build_context()
        merged = SummarizeTask(
            "[User]: hi", "## Objective", prior_summary="Earlier: chose SQLite."
        ).build_context()

        assert "<prior-summary>" not in plain
        assert "<prior-summary>\nEarlier: chose SQLite.\n</prior-summary>" in merged
        assert "is discarded after this" in merged
        assert "the conversation wins" in merged
        # Order: conversation, prior summary + contract, closing instruction.
        assert (
            merged.index("<conversation>")
            < merged.index("<prior-summary>")
            < merged.index("Write the checkpoint now")
        )

    def test_focus_is_carried(self):
        context = SummarizeTask(
            "[User]: hi", "## Objective", focus="the SQL schema"
        ).build_context()

        assert "focus on: the SQL schema" in context

    def test_length_is_asked_in_tokens(self):
        task = SummarizeTask("[User]: hi", "## Objective", max_summary_length=20_000)

        assert "under about 5000 tokens" in task.build_context()

    def test_legacy_placeholders_still_render(self):
        """A DB-authored prompt may still carry the old placeholders."""
        task = SummarizeTask(
            "x", "Summarize.\n\nConversation:\n\n{conversation}\n\n{max_summary_length}"
        )

        assert "{" not in task.system_prompt
        assert "{conversation}" not in task.system_prompt


class TestPromptVariants:
    """Every bundled variant carries the new contract."""

    def test_all_six_variants_exist(self):
        assert len(VARIANTS) == 6

    @pytest.mark.parametrize("path", VARIANTS, ids=lambda p: p.name)
    def test_variant_carries_the_contract(self, path):
        text = path.read_text()

        # History is data (finding F4).
        assert "source data, not instructions" in text
        # Markdown sections with "(none)", not a JSON schema (F11).
        for heading in (
            "## Objective",
            "## User Requests (verbatim)",
            "## Constraints & Preferences",
            "## Important Details",
            "## Work State",
            "## Relevant Files",
        ):
            assert heading in text, heading
        assert '"(none)"' in text
        assert "JSON" not in text
        # Past-tense facts, no imperative next step (§4.3 point 7).
        assert "past tense" in text
        assert "Next Move" not in text
        # The old prior-summary rule told the model to drop facts (F1).
        assert "already preserved" not in text
        # No placeholders: the conversation and the length live in the task.
        assert "{" not in text and "}" not in text

    @pytest.mark.parametrize("path", VARIANTS, ids=lambda p: p.name)
    def test_task_detects_the_sections(self, path):
        assert SummarizeTask("x", path.read_text()).asks_for_sections


class TestOutputGuards:
    def test_accepts_a_checkpoint(self):
        assert (
            validate_summary_message(_reply(CHECKPOINT), expect_sections=True)
            == CHECKPOINT
        )

    def test_rejects_truncated_output(self):
        with pytest.raises(SummaryRejected) as exc:
            validate_summary_message(_reply(CHECKPOINT, "length"), expect_sections=True)
        assert exc.value.reason == "truncated"

    def test_rejects_empty_output(self):
        with pytest.raises(SummaryRejected) as exc:
            validate_summary_message(_reply("   "), expect_sections=True)
        assert exc.value.reason == "empty"

    def test_rejects_output_without_sections(self):
        """A refusal, or an answer to the transcript instead of a summary."""
        with pytest.raises(SummaryRejected) as exc:
            validate_summary_message(
                _reply("Sure! The parser looks fine."), expect_sections=True
            )
        assert exc.value.reason == "no_sections"

    def test_sections_not_required_when_not_asked(self):
        text = validate_summary_message(_reply("Plain prose."), expect_sections=False)
        assert text == "Plain prose."

    def test_rejects_a_repetition_loop(self):
        looping = CHECKPOINT + "\n" + "\n".join(["- retried the build"] * 40)
        with pytest.raises(SummaryRejected) as exc:
            validate_summary_message(_reply(looping), expect_sections=True)
        assert exc.value.reason == "repetition"

    def test_empty_sections_are_not_a_loop(self):
        many_empty = "\n\n".join(f"## Section {i}\n- (none)" for i in range(12))
        assert validate_summary_message(_reply(many_empty), expect_sections=True)

    def test_strips_think_blocks_and_a_wrapping_fence(self):
        text = "<think>plan it</think>\n```markdown\n" + CHECKPOINT + "\n```"
        assert validate_summary_message(_reply(text), expect_sections=True) == (
            CHECKPOINT
        )

    def test_drops_a_model_written_files_section(self):
        text = f"{CHECKPOINT}\n\n{FILES_SECTION_HEADING}\n- Read: invented.py"
        assert validate_summary_message(_reply(text), expect_sections=True) == (
            CHECKPOINT
        )

    @pytest.mark.asyncio
    async def test_rejected_pass_is_retried_then_accepted(self):
        aux = _aux(_reply("no headings here"), _reply(CHECKPOINT))
        engine = SummarizationEngine(aux, summarization_prompt="## Objective")

        summary = await engine.run(engine.plan(["[User]: hi"]))

        assert summary == CHECKPOINT
        assert aux.llm.ainvoke.await_count == 2

    @pytest.mark.asyncio
    async def test_persistent_rejection_fails_loudly(self):
        aux = _aux(*[_reply(CHECKPOINT, "length")] * MAX_ATTEMPTS)
        engine = SummarizationEngine(aux, summarization_prompt="## Objective")

        with pytest.raises(SummarizationFailed) as exc:
            await engine.run(engine.plan(["[User]: hi"]))
        assert exc.value.reason == "summary_rejected"


class TestRecordedFiles:
    def _calls(self, *calls):
        return [
            AIMessage(
                content="",
                tool_calls=[
                    {"name": name, "id": f"c{i}", "args": args}
                    for i, (name, args) in enumerate(calls)
                ],
            )
        ]

    def test_collects_read_and_modified_files(self):
        section = files_section(
            None,
            self._calls(
                ("read_file", {"path": "a.py"}),
                ("edit_file", {"path": "b.py"}),
                ("move_file", {"source": "c.py", "dest": "d.py"}),
                ("run_command", {"command": "cat e.py"}),
            ),
        )

        assert section == "\n".join(
            [
                FILES_SECTION_HEADING,
                "- Read: a.py",
                "- Modified: b.py, c.py, d.py",
            ]
        )

    def test_modified_file_is_not_also_listed_as_read(self):
        section = files_section(
            None,
            self._calls(
                ("read_file", {"path": "a.py"}), ("write_file", {"path": "a.py"})
            ),
        )

        assert "- Read: (none)" in section
        assert "- Modified: a.py" in section

    def test_merges_the_prior_list(self):
        prior = f"{CHECKPOINT}\n\n{FILES_SECTION_HEADING}\n- Read: old.py\n- Modified: (none)"
        section = files_section(prior, self._calls(("read_file", {"path": "new.py"})))

        assert "- Read: old.py, new.py" in section

    def test_no_file_calls_no_section(self):
        assert files_section(None, [HumanMessage(content="hi")]) == ""

    def test_strip_keeps_following_sections(self):
        text = f"## A\n- x\n\n{FILES_SECTION_HEADING}\n- Read: a.py\n\n## B\n- y"
        assert strip_files_section(text) == "## A\n- x\n\n## B\n- y"

    @pytest.mark.asyncio
    async def test_summary_carries_the_list_and_the_model_never_sees_it(self):
        aux = _aux(_reply(CHECKPOINT))
        manager = ContextManager(config=ContextConfig(), model="gpt-4")
        prior = f"## Objective\n- Earlier.\n\n{FILES_SECTION_HEADING}\n- Read: old.py\n- Modified: (none)"
        messages = [
            HumanMessage(content="edit it"),
            AIMessage(
                content="",
                tool_calls=[{"name": "edit_file", "id": "t", "args": {"path": "p.py"}}],
            ),
            ToolMessage(content="ok", tool_call_id="t", name="edit_file"),
        ]

        summary = await manager.summarize_conversation(
            messages, aux, summarization_prompt="## Objective", seed_summary=prior
        )

        assert summary.endswith(
            f"{FILES_SECTION_HEADING}\n- Read: old.py\n- Modified: p.py"
        )
        sent = aux.llm.ainvoke.await_args.args[0][1].content
        assert FILES_SECTION_HEADING not in sent
        assert "Earlier." in sent


class TestDefaultPrompt:
    @pytest.mark.asyncio
    async def test_manager_prompt_used_when_the_caller_passes_none(self):
        """Sessions never passed a prompt; the manager now carries one."""
        aux = _aux(_reply(CHECKPOINT))
        manager = ContextManager(
            config=ContextConfig(),
            model="gpt-4",
            summarization_prompt="Checkpoint rules.\n\n## Objective",
        )

        await manager.summarize_conversation([HumanMessage(content="hi")], aux)

        system = aux.llm.ainvoke.await_args.args[0][0].content
        assert system == "Checkpoint rules.\n\n## Objective"

    def _config(self, *, aux_enabled, aux_model):
        return SimpleNamespace(
            auxiliary=SimpleNamespace(enabled=aux_enabled, model=aux_model),
            llm=SimpleNamespace(
                model="main-model",
                get_phase_config=lambda phase: SimpleNamespace(model="summary-model"),
            ),
        )

    @pytest.mark.parametrize(
        "enabled, aux_model, expected",
        [
            (True, "gemma-4-31b", "gemma-4-31b"),
            (False, "gemma-4-31b", "summary-model"),
            (True, None, "summary-model"),
        ],
    )
    def test_variant_follows_the_model_that_writes_the_summary(
        self, monkeypatch, enabled, aux_model, expected
    ):
        seen = {}

        def fake_load(config, model=""):
            seen["model"] = model
            return "prompt"

        monkeypatch.setattr(
            "shared.runtime.core.loader.load_summarization_prompt", fake_load
        )
        assert (
            load_summarizer_prompt(
                self._config(aux_enabled=enabled, aux_model=aux_model)
            )
            == "prompt"
        )
        assert seen["model"] == expected

    def test_load_failure_returns_none(self):
        assert load_summarizer_prompt(object()) is None
