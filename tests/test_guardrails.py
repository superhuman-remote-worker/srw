"""Tests for the guardrails matrix: resolution + tool description injection.

Covers:
  - resolve_guardrails default-only / family-override / deep-merge
  - apply_guardrails_to_tools strip-and-inject behavior
  - _replace_examples_block edge cases (no block, trailing-newline, indent)
  - format_nudge placeholder validation
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import yaml
from langchain_core.tools import tool

from shared.runtime.core.loader import resolve_guardrails
from shared.runtime.services.guardrails import (
    GuardrailFormatError,
    KNOWN_NUDGES,
    _replace_examples_block,
    apply_guardrails_to_tools,
    format_nudge,
)


# =============================================================================
# resolve_guardrails — backed by the real config files
# =============================================================================


class TestResolveGuardrails:
    def test_default_family_returns_default_yaml(self):
        g = resolve_guardrails("gpt-4o")
        assert "tool_examples" in g
        assert "nudges" in g
        # parens-form sentinel from default.yaml
        assert "git_log()" in g["tool_examples"]["git_log"]

    def test_gemma_overrides_tool_examples(self):
        g = resolve_guardrails("google/gemma-4-31b")
        # Gemma file replaces git_log with brace-form
        assert "<|tool_call>call:git_log{}<tool_call|>" in g["tool_examples"]["git_log"]
        # parens-form from default must be gone
        assert "git_log()" not in g["tool_examples"]["git_log"]

    def test_gemma_overrides_only_some_nudges(self):
        g = resolve_guardrails("google/gemma-4-31b")
        # todo_action is overridden — mentions Gemma format
        assert "Gemma" in g["nudges"]["todo_action"]
        # loop_warning_suffix is NOT overridden — comes from default
        assert "[LOOP WARNING]" in g["nudges"]["loop_warning_suffix"]

    def test_unknown_family_falls_back_to_default(self):
        # An unrecognized model resolves to family "default"
        g = resolve_guardrails("some-unknown-model-xyz")
        assert "git_log()" in g["tool_examples"]["git_log"]

    def test_gpt_oss_inherits_most_from_default(self):
        g = resolve_guardrails("openai/gpt-oss-120b")
        # gpt_oss.yaml only overrides next_phase_todos
        assert (
            "harmony" in g["tool_examples"]["next_phase_todos"].lower()
            or "commentary" in g["tool_examples"]["next_phase_todos"].lower()
        )
        # Other tools come from default
        assert "git_log()" in g["tool_examples"]["git_log"]

    def test_all_shell_example_overrides_set_working_dir_without_inline_cd(self):
        guardrail_dir = Path(__file__).parents[1] / "config" / "guardrails"
        for guardrail_path in guardrail_dir.glob("*.yaml"):
            config = yaml.safe_load(guardrail_path.read_text(encoding="utf-8"))
            examples = config.get("tool_examples", {})
            for tool_name in ("run_command", "shell_execute"):
                if tool_name not in examples:
                    continue
                calls = [
                    line.strip()
                    for line in examples[tool_name].splitlines()
                    if tool_name in line
                ]
                assert calls, f"{guardrail_path}: no {tool_name} calls"
                for call_text in calls:
                    assert "working_dir" in call_text, (
                        f"{guardrail_path}: {call_text} omits working_dir"
                    )
                    assert 'command="cd ' not in call_text
                    assert 'command:<|"|>cd ' not in call_text


# =============================================================================
# _replace_examples_block — pure-function tests
# =============================================================================


class TestReplaceExamplesBlock:
    REPLACEMENT = "Examples:\n    new_call(x=1)\n    new_call(x=2)"

    def test_strips_existing_block_with_trailing_newline(self):
        original = textwrap.dedent(
            """\
            Do a thing.

            Args:
                x: some arg

            Returns:
                str

            Example:
                old_call()
                old_call(x=1)
            """
        )
        out = _replace_examples_block(original, self.REPLACEMENT)
        assert "old_call" not in out
        assert "new_call(x=1)" in out
        assert "Returns:" in out

    def test_strips_block_at_end_without_trailing_newline(self):
        # Common LangChain @tool dedent shape
        original = (
            "Do a thing.\n"
            "\n"
            "Args:\n"
            "    x: arg\n"
            "\n"
            "Example:\n"
            "    old_call()\n"
            "    old_call(x=1)"
        )
        out = _replace_examples_block(original, self.REPLACEMENT)
        assert "old_call" not in out
        assert "new_call(x=1)" in out

    def test_appends_when_no_examples_block(self):
        original = "Do a thing.\n\nArgs:\n    x: arg\n\nReturns:\n    str"
        out = _replace_examples_block(original, self.REPLACEMENT)
        assert out.startswith("Do a thing.")
        assert out.rstrip().endswith("new_call(x=2)")

    def test_handles_examples_plural_heading(self):
        original = "Heading.\n\nExamples:\n    a()\n    b()\n"
        out = _replace_examples_block(original, self.REPLACEMENT)
        assert "a()" not in out
        assert "new_call" in out

    def test_handles_empty_description(self):
        out = _replace_examples_block("", self.REPLACEMENT)
        assert "new_call" in out


# =============================================================================
# apply_guardrails_to_tools — wiring against fake guardrails dicts
# =============================================================================


def _fake_guardrails(examples=None, nudges=None):
    return {
        "tool_examples": examples or {},
        "nudges": nudges or {},
    }


@tool
def _fake_git_log(max_count: int = 10) -> str:
    """View commit history.

    Args:
        max_count: max number

    Returns:
        history

    Example:
        git_log()
        git_log(max_count=5)
    """
    return ""


@tool
def _fake_unknown(x: int = 0) -> str:
    """Unknown tool with no guardrails entry.

    Args:
        x: arg

    Returns:
        str
    """
    return ""


class TestApplyGuardrailsToTools:
    def test_replaces_examples_for_known_tool(self):
        guardrails = _fake_guardrails(
            examples={"_fake_git_log": "Examples:\n    BRACE_FORM_PLACEHOLDER"}
        )
        out = apply_guardrails_to_tools([_fake_git_log], guardrails=guardrails)
        assert "BRACE_FORM_PLACEHOLDER" in out[0].description
        assert "git_log()" not in out[0].description

    def test_leaves_unknown_tool_untouched(self):
        guardrails = _fake_guardrails(examples={"_fake_git_log": "X"})
        out = apply_guardrails_to_tools([_fake_unknown], guardrails=guardrails)
        # Same description (object equality not required, but content is)
        assert out[0].description == _fake_unknown.description

    def test_does_not_mutate_source_tool(self):
        original_description = _fake_git_log.description
        guardrails = _fake_guardrails(
            examples={"_fake_git_log": "Examples:\n    NEW_CONTENT"}
        )
        apply_guardrails_to_tools([_fake_git_log], guardrails=guardrails)
        assert _fake_git_log.description == original_description

    def test_falls_back_to_default_family_when_no_model(self):
        # Bind sites pass model="" (or None) when the model name isn't yet
        # known. apply_guardrails_to_tools must tolerate this and resolve
        # the default family rather than erroring.
        out = apply_guardrails_to_tools([_fake_git_log], model=None)
        assert out[0].description == _fake_git_log.description

    def test_resolves_via_model_when_no_guardrails_passed(self):
        # Hits the real config files — sanity check, not full coverage
        out = apply_guardrails_to_tools([_fake_git_log], model="gpt-4o")
        # _fake_git_log is not in the real config, so it's untouched
        assert out[0].description == _fake_git_log.description


# =============================================================================
# format_nudge — placeholder validation
# =============================================================================


class TestFormatNudge:
    def test_resolves_default_nudge(self):
        out = format_nudge("todo_action", model="gpt-4o", todo_id="todo_3")
        assert "todo_3" in out

    def test_gemma_overrides_todo_action(self):
        out = format_nudge("todo_action", model="google/gemma-4-31b", todo_id="todo_3")
        assert "Gemma" in out
        assert "{todo_id}" not in out  # placeholder must be substituted

    def test_unknown_key_raises(self):
        with pytest.raises(GuardrailFormatError, match="Unknown nudge key"):
            format_nudge("not_a_nudge", model="gpt-4o")

    def test_unexpected_placeholder_raises(self):
        with pytest.raises(GuardrailFormatError, match="unexpected placeholders"):
            format_nudge("todo_action", model="gpt-4o", todo_id="x", surprise="y")

    def test_missing_placeholder_raises_keyerror(self):
        with pytest.raises(KeyError):
            format_nudge("todo_action", model="gpt-4o")

    def test_known_nudges_includes_all_call_sites(self):
        # Sanity — every key actually has at least an empty placeholder set
        for key, expected in KNOWN_NUDGES.items():
            assert isinstance(expected, set)


# =============================================================================
# Cross-cutting: every nudge key in KNOWN_NUDGES must exist in default.yaml
# =============================================================================


class TestNudgeCoverage:
    def test_every_known_nudge_resolves_for_default_family(self):
        g = resolve_guardrails("gpt-4o")
        nudges = g.get("nudges", {})
        missing = sorted(set(KNOWN_NUDGES) - set(nudges))
        assert not missing, (
            f"KNOWN_NUDGES has keys not present in config/guardrails/default.yaml: "
            f"{missing}"
        )


# =============================================================================
# todo_list_footer — must never enumerate the tool surface
# =============================================================================


class TestTodoListFooter:
    """Regression: the todo-list footer must never enumerate the tool surface.

    The footer is appended to the todo list and injected as the final
    HumanMessage of every request (end-of-prompt = max attention). A
    "Tools available: ..." list there reads as exhaustive and convinces the
    model its other bound tools are gone — proven root cause of the job
    1cab4b88 rewind loop (nine tactical phases burned to todo_rewind) and
    the job edd06963 "stale palette" memory spiral. See
    knowledge-base/knowledge/issues/agent_phase_guardrails_burn_legitimate_work.md.
    """

    def test_no_family_enumerates_tool_surface(self):
        from shared.runtime.core.loader import _load_guardrails_matrix

        matrix = _load_guardrails_matrix(None)
        assert "default" in matrix
        for family in sorted(matrix):
            footer = format_nudge("todo_list_footer", family=family)
            # The forbidden pattern: presenting a closed tool list.
            assert "Tools available" not in footer, family
            assert "request_replan" not in footer, family
            assert "mark_complete" not in footer, family
            # It still teaches how to advance the list...
            assert "todo_complete" in footer, family
            # ...and explicitly disclaims being a tool list.
            assert "not a tool list" in footer, family


# =============================================================================
# phase_gate_* — the per-call phase gate's rejection (U2 WP3)
# =============================================================================


class TestPhaseGateNudges:
    """The rejection a phase-illegal call gets back names the tool's phase, the
    current phase and what advances the job — never a list of other tools
    (same rule as the todo-list footer, same failure mode if broken)."""

    def test_no_family_enumerates_tool_surface(self):
        from shared.runtime.core.loader import _load_guardrails_matrix

        matrix = _load_guardrails_matrix(None)
        for family in sorted(matrix):
            tactical = format_nudge(
                "phase_gate_strategic_tool_in_tactical",
                family=family,
                tool="x_tool",
                phase_number=4,
            )
            strategic = format_nudge(
                "phase_gate_tactical_tool_in_strategic",
                family=family,
                tool="x_tool",
                phase_number=3,
            )
            for text in (tactical, strategic):
                assert text.startswith("Error: 'x_tool' is a "), family
                assert "Tools available" not in text, family
            assert "strategic-phase tool" in tactical and "(phase 4)" in tactical
            assert "tactical-phase tool" in strategic and "(phase 3)" in strategic
            # The tactical-phase rejection names no other tool at all; the
            # strategic one names only the tool that stages the work.
            assert "request_replan" not in tactical, family
            assert "next_phase_todos" not in tactical, family
            assert "next_phase_todos" in strategic, family
            assert "job_complete" not in strategic, family
            note = format_nudge("phase_gate_batch_note", family=family)
            assert note == "Other calls in this batch were executed normally.", family


# =============================================================================
# *_job_complete — the variants for a job without todo_complete
# =============================================================================


#: Each nudge that names todo_complete (or a todo to mark blocked), and the
#: variant the caller picks when the job's loaded tools lack todo_complete.
_JOB_COMPLETE_VARIANTS = {
    "todo_action": "todo_action_job_complete",
    "todo_list_footer": "todo_list_footer_job_complete",
    "loop_warning_suffix": "loop_warning_suffix_job_complete",
    "tool_not_found_suffix": "tool_not_found_suffix_job_complete",
}


class TestJobCompleteVariants:
    """A job whose tools do not include todo_complete closes its one task
    with job_complete. The texts it gets in place of the originals name
    job_complete and never todo_complete, and a family file that overrides
    an original (for its wire format) overrides the variant too."""

    def test_variants_are_known_keys_without_placeholders(self):
        for variant in _JOB_COMPLETE_VARIANTS.values():
            assert KNOWN_NUDGES[variant] == set()

    def test_every_family_overriding_an_original_overrides_its_variant(self):
        from shared.runtime.core.loader import _load_guardrails_matrix

        matrix = _load_guardrails_matrix(None)
        for family in sorted(matrix):
            own = matrix[family].get("nudges") or {}
            for original, variant in _JOB_COMPLETE_VARIANTS.items():
                if original in own:
                    assert variant in own, (family, variant)

    def test_no_family_names_todo_complete_in_a_variant(self):
        from shared.runtime.core.loader import _load_guardrails_matrix

        matrix = _load_guardrails_matrix(None)
        for family in sorted(matrix):
            for variant in _JOB_COMPLETE_VARIANTS.values():
                text = format_nudge(variant, family=family)
                assert "todo_complete" not in text, (family, variant)
                assert "job_complete" in text, (family, variant)
                assert "Tools available" not in text, (family, variant)

    def test_reminder_says_end_or_continue(self):
        text = format_nudge("todo_action_job_complete", model="gpt-4o")
        assert text.startswith("Your last reply contained no tool call.")
        assert "If the task is done, call the `job_complete` tool now." in text
        assert "say so in the summary" in text
        assert "Otherwise continue with a tool call." in text

    def test_footer_keeps_the_status_reminder_sentence(self):
        from shared.runtime.core.loader import _load_guardrails_matrix

        for family in sorted(_load_guardrails_matrix(None)):
            footer = format_nudge("todo_list_footer_job_complete", family=family)
            assert "not a tool list" in footer, family
            assert "When the task is done, invoke the `job_complete` tool." in footer

    def test_gemma_variants_carry_the_wire_format(self):
        model = "google/gemma-4-31b"
        assert "Gemma" in format_nudge("todo_action_job_complete", model=model)
        assert "Gemma" in format_nudge("todo_list_footer_job_complete", model=model)
        # Not overridden: the default text, like the originals.
        assert format_nudge("loop_warning_suffix_job_complete", model=model) == (
            format_nudge("loop_warning_suffix_job_complete", model="gpt-4o")
        )
