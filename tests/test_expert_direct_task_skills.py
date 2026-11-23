"""The bundled experts that plan in phases carry their method into a job without phases.

An expert with ``phases: true`` delivers its method through its phase skills.
A job can override the setting (``phases: false``); it then gets one skill at
its start, ``direct-task``. Without a copy of its own the expert's job would
get the framework's generic text and lose the expert's method, so each of
these experts ships ``skills/direct-task/SKILL.md``.

What such a job reads must not name what it does not have: the planning
tools, phases, the plan file. That holds for the skill and for the expert's
standing instructions, which every job on the expert gets.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from orchestrator.services.config_resolver import resolve_config
from shared.job_phases import DIRECT_TASK_SKILL, PHASE_PROCEDURE_TOOLS
from shared.runtime.core.loader import canonical_config_name, render_instruction_content

REPO = Path(__file__).resolve().parents[1]

#: Experts that plan in phases and can be asked not to. The critic and the
#: receptionist are always phased, whatever a job says.
EXPERTS = ("developer", "scholar", "curator", "designer", "bughunter", "product-qa")

_PROCEDURE_WORDS = re.compile(
    r"\bphases?\b|strategic|tactical|plan\.md|todos\.yaml|"
    + "|".join(re.escape(tool) for tool in PHASE_PROCEDURE_TOOLS),
    re.IGNORECASE,
)


def _blob(expert: str) -> dict:
    return resolve_config(
        base_config_name=canonical_config_name(expert),
        expert_type="worker",
        request_override={"phases": False},
        db_refs={},
    )


def _tools(blob: dict) -> list[str]:
    groups = (blob["agent"].get("tools") or {}).values()
    return sorted(
        {name for group in groups if isinstance(group, list) for name in group}
    )


def _body(text: str) -> str:
    """A skill's instructions without its catalogue frontmatter."""
    return text.split("---", 2)[2]


@pytest.mark.parametrize("expert", EXPERTS)
def test_the_expert_ships_its_own_direct_task_skill(expert):
    path = (
        REPO / "config" / "experts" / expert / "skills" / DIRECT_TASK_SKILL / "SKILL.md"
    )
    assert path.is_file(), f"{expert} has no direct-task skill of its own"
    text = path.read_text(encoding="utf-8")
    assert text.startswith("---\nname: direct-task\n")
    assert "catalog: hidden" in text.split("---", 2)[1]
    generic = (REPO / "config" / "skills" / DIRECT_TASK_SKILL / "SKILL.md").read_text(
        encoding="utf-8"
    )
    assert _body(text).strip() != _body(generic).strip()


@pytest.mark.parametrize("expert", EXPERTS)
def test_a_job_without_phases_freezes_the_experts_own_skill(expert):
    blob = _blob(expert)
    assert blob["agent"]["phases"] is False
    assert not set(_tools(blob)) & set(PHASE_PROCEDURE_TOOLS)
    folder = REPO / "config" / "experts" / expert
    own = (folder / "skills" / DIRECT_TASK_SKILL / "SKILL.md").read_text(
        encoding="utf-8"
    )
    assert blob["instructions"][DIRECT_TASK_SKILL] == own
    instructions = folder / "instructions.md"
    if instructions.is_file():
        assert blob["instructions"]["instructions"] == instructions.read_text(
            encoding="utf-8"
        )


@pytest.mark.parametrize("expert", EXPERTS)
def test_the_skill_names_nothing_of_the_phased_procedure(expert):
    blob = _blob(expert)
    path = (
        REPO / "config" / "experts" / expert / "skills" / DIRECT_TASK_SKILL / "SKILL.md"
    )
    rendered = render_instruction_content(
        _body(path.read_text(encoding="utf-8")), _tools(blob), origin=str(path)
    )
    assert "{%" not in rendered and "{{" not in rendered
    assert not _PROCEDURE_WORDS.findall(rendered)
    assert "`job_complete`" in rendered


@pytest.mark.parametrize("expert", EXPERTS)
def test_the_skill_renders_without_any_optional_tool(expert):
    path = (
        REPO / "config" / "experts" / expert / "skills" / DIRECT_TASK_SKILL / "SKILL.md"
    )
    rendered = render_instruction_content(
        _body(path.read_text(encoding="utf-8")), ["job_complete"], origin=str(path)
    )
    assert "{%" not in rendered and "{{" not in rendered
    assert not _PROCEDURE_WORDS.findall(rendered)
    assert "`job_complete`" in rendered


@pytest.mark.parametrize("expert", EXPERTS)
def test_the_standing_instructions_fit_a_job_without_phases(expert):
    path = REPO / "config" / "experts" / expert / "instructions.md"
    if not path.is_file():
        pytest.skip(f"{expert} has no instructions file of its own")
    rendered = render_instruction_content(
        path.read_text(encoding="utf-8"), _tools(_blob(expert)), origin=str(path)
    )
    assert "{%" not in rendered
    assert not _PROCEDURE_WORDS.findall(rendered)
