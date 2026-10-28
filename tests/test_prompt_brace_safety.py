"""Regression guard: every shipped prompt must survive the real render pipeline.

The loop job `bf9183cf` hard-failed because the former
`config/experts/product-qa/tactical.txt`
carried a literal `{py,sh,md}` and the assembler rendered the phase component with
`str.format`, which read it as a replacement field and raised `KeyError: 'py,sh,md'`
at phase render. The fix (src/core/loader.py::render_placeholders) substitutes only
an explicit allow-list of tokens and leaves every other brace literal.

This module sweeps all bundled phase skills and base system prompts through
the actual two stages — Jinja2 (`render_instruction_content`) then
`render_placeholders` — and asserts none raise and none render empty. It catches a
new prompt shipping with a Jinja syntax error or a stray brace pattern, and pairs
with `test_prompt_matrix.py::TestRenderPlaceholders` (which proves a literal brace
survives end-to-end and would crash if anyone reverts the assembler to str.format).
"""

import re
from pathlib import Path

import pytest

from shared.runtime.core.expert_resolution import ASSEMBLER_OWNED_PROMPT_TOKENS
from shared.runtime.core.loader import render_instruction_content, render_placeholders

_REPO_ROOT = Path(__file__).resolve().parents[1]

# Every placeholder the assembler owns, filled with sentinels for the sweep.
_KNOWN = {
    "phase_number": "1",
    "agent_display_name": "SENTINEL_NAME",
    "expert_identity": "SENTINEL_IDENTITY",
    "available_skills": "SENTINEL_SKILLS",
    "subagent_environment": "SENTINEL_SUBAGENT_ENVIRONMENT",
    "prompt_content": "SENTINEL_CONTENT",
}

_ASSEMBLER_PLACEHOLDERS = tuple(
    token.removeprefix("{").removesuffix("}") for token in ASSEMBLER_OWNED_PROMPT_TOKENS
)
_ASSEMBLER_PLACEHOLDER_RE = re.compile(
    r"\{(?:" + "|".join(map(re.escape, _ASSEMBLER_PLACEHOLDERS)) + r")\}"
)

_PERSONA_GLOBS = [
    "config/experts/*/persona*.txt",
    "config/subagents/*/persona*.txt",
    "config/prompts/persona*.txt",
]

# Files that flow through the worker render sites: phase skills, personas, and
# base system templates. Summarization, guardrail runtime messages, and todo
# templates use other render paths and their own kwargs, so they are out of scope.
_PROMPT_GLOBS = [
    "config/skills/*-phase/SKILL.md",
    "config/experts/*/skills/*-phase/SKILL.md",
    "config/experts/*/persona.txt",
    "config/subagents/*/persona.txt",
    "config/prompts/systemprompt*.txt",
    "config/prompts/persona*.txt",
]


def _shipped_prompts() -> list[Path]:
    found: list[Path] = []
    for pattern in _PROMPT_GLOBS:
        found.extend(sorted(_REPO_ROOT.glob(pattern)))
    return found


def _shipped_personas() -> list[Path]:
    found: list[Path] = []
    for pattern in _PERSONA_GLOBS:
        found.extend(sorted(_REPO_ROOT.glob(pattern)))
    assert found, "persona source-policy sweep matched no files"
    return found


def _has_tool_names(content: str) -> list[str]:
    """Tool names referenced by ``has_tool("x")`` so the true Jinja branch renders."""
    return re.findall(r'has_tool\(\s*["\']([^"\']+)["\']\s*\)', content)


def test_prompt_sweep_values_cover_the_assembler_contract():
    assert tuple(_KNOWN) == _ASSEMBLER_PLACEHOLDERS


@pytest.mark.parametrize(
    "prompt_path",
    _shipped_prompts(),
    ids=lambda p: str(p.relative_to(_REPO_ROOT)),
)
def test_shipped_prompt_survives_render_pipeline(prompt_path: Path):
    """Jinja2 → render_placeholders over each shipped prompt: no raise, non-empty.

    Rendered with tools ON (has_tool branches taken) and OFF, since either side of
    a conditional may hold a stray brace.
    """
    content = prompt_path.read_text()
    tools = _has_tool_names(content)
    for tool_names in (tools, []):
        jinja_rendered = render_instruction_content(content, tool_names)
        # Must not raise regardless of which tokens are literal prose vs. ours.
        rendered = render_placeholders(jinja_rendered, **_KNOWN)
        assert rendered.strip(), f"{prompt_path} rendered empty (tools={tool_names!r})"


@pytest.mark.parametrize(
    "persona_path",
    _shipped_personas(),
    ids=lambda p: str(p.relative_to(_REPO_ROOT)),
)
def test_shipped_persona_contains_no_assembler_placeholder(persona_path: Path):
    """Personas are content, never a nested source for the prompt assembler."""
    content = persona_path.read_text(encoding="utf-8")
    found = sorted(set(_ASSEMBLER_PLACEHOLDER_RE.findall(content)))
    assert not found, f"{persona_path}: reserved assembler placeholders: {found}"


def test_sweep_covers_the_known_offenders():
    """The migrated forms of the three offenders remain in the sweep."""
    swept = {str(p.relative_to(_REPO_ROOT)) for p in _shipped_prompts()}
    for f in (
        "config/experts/product-qa/skills/tactical-phase/SKILL.md",
        "config/experts/bughunter/skills/tactical-phase/SKILL.md",
        "config/experts/designer/skills/tactical-phase/SKILL.md",
    ):
        assert f in swept, f"{f} not covered by the brace-safety sweep"


# Task 15 (F-C1 protected cloud): the interactive variants carry a
# `{% if protected_cloud %}` honesty block that the main sweep above never
# takes (it renders with the flag defaulting to False). Sweep the real files
# with the flag ON too, so a Jinja typo inside the block in any variant is
# caught here rather than at session attach.
_INTERACTIVE_PROMPTS = sorted(
    _REPO_ROOT.glob("config/prompts/systemprompt_interactive*.txt")
)


@pytest.mark.parametrize(
    "prompt_path",
    _INTERACTIVE_PROMPTS,
    ids=lambda p: str(p.relative_to(_REPO_ROOT)),
)
def test_interactive_prompt_protected_cloud_block_renders(prompt_path: Path):
    """protected_cloud=True must render the staged-for-review block with no
    Jinja residue; False must drop it entirely."""
    content = prompt_path.read_text()
    tools = _has_tool_names(content)
    on = render_placeholders(
        render_instruction_content(content, tools, protected_cloud=True),
        **_KNOWN,
    )
    off = render_placeholders(
        render_instruction_content(content, tools, protected_cloud=False),
        **_KNOWN,
    )
    assert "staged for your review" in on, f"{prompt_path}: block missing when ON"
    assert "{%" not in on, f"{prompt_path}: Jinja residue when ON"
    assert "staged for your review" not in off, f"{prompt_path}: block leaked when OFF"
    assert "{%" not in off, f"{prompt_path}: Jinja residue when OFF"


@pytest.mark.parametrize(
    "prompt_path",
    _INTERACTIVE_PROMPTS,
    ids=lambda p: str(p.relative_to(_REPO_ROOT)),
)
def test_interactive_prompt_says_where_a_save_lands(prompt_path: Path):
    """Session fa2a9885 (agent_claims_lasting_memory_for_session_local_files):
    every scaffold offered notes/ as an equal kind of persistence and called
    the workspace a place files are "saved", so the agent told its user
    "Gespeichert" and "from now on" for files only that session could see.
    The review panel it named ("Cloud changes" in the session header) is now
    a banner whose button is "Review changes"."""
    text = " ".join(prompt_path.read_text().split())
    assert "Files you create or modify are saved there" not in text
    assert "kb_write or notes/." not in text
    assert "other sessions do not see them" in text
    assert "notes/ files keep context for this session" in text
    assert "unless a durable write (kb_write, a cloud folder) succeeded" in text
    assert 'starts with "Error:" saved nothing' in text
    assert '"Cloud changes" in the session header' not in text
    assert '"Review changes" on the banner above the conversation' in text


def test_protected_cloud_sweep_covers_all_interactive_variants():
    """Guards the glob: all four shipped interactive variants must be swept."""
    names = {p.name for p in _INTERACTIVE_PROMPTS}
    expected = {
        "systemprompt_interactive.txt",
        "systemprompt_interactive_deepseek.txt",
        "systemprompt_interactive_glm.txt",
        "systemprompt_interactive_gpt_5.txt",
    }
    missing = expected - names
    assert not missing, f"interactive variants missing from sweep: {missing}"
