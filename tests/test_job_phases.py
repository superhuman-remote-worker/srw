"""``shared.job_phases``: the ``phases`` key, its default and its readers.

``normalize_phases`` is the strict reader for values that enter the system (a
request, an override written by hand); ``plans_in_phases`` is the lenient
reader for stored and merged configuration. Both accept a boolean and the
words on/off, true/false and yes/no in any case.
"""

from __future__ import annotations

from types import MappingProxyType

import pytest

from shared.job_phases import (
    DEFAULT_PHASES,
    DIRECT_TASK_SKILL,
    PHASE_PROCEDURE_SKILLS,
    PHASE_PROCEDURE_TOOLS,
    PHASES_KEY,
    is_phases_value,
    normalize_phases,
    plans_in_phases,
)

TRUE_SPELLINGS = (True, "on", "true", "yes", "ON", "True", "YES", " on ", "On\n")
FALSE_SPELLINGS = (False, "off", "false", "no", "OFF", "False", "No", "\toff ")
NOT_A_VALUE = (
    "light",
    "agent",
    "worker",
    "1",
    "0",
    "y",
    "n",
    "enabled",
    1,
    0,
    1.0,
    [],
    ["on"],
    {},
    {"phases": True},
    object(),
)


class TestConstants:
    def test_the_key_and_its_default(self):
        assert PHASES_KEY == "phases"
        assert DEFAULT_PHASES is False

    def test_the_procedure_tools(self):
        assert PHASE_PROCEDURE_TOOLS == (
            "next_phase_todos",
            "todo_complete",
            "todo_list",
            "request_replan",
            "mark_complete",
        )
        # job_complete ends a job without phases; it is never a procedure tool.
        assert "job_complete" not in PHASE_PROCEDURE_TOOLS

    def test_the_procedure_tools_are_registered(self):
        from agent.tools.registry import TOOL_REGISTRY

        assert set(PHASE_PROCEDURE_TOOLS) <= set(TOOL_REGISTRY)

    def test_the_procedure_skills_and_their_replacement(self):
        assert PHASE_PROCEDURE_SKILLS == (
            "strategic-phase",
            "tactical-phase",
            "todo-guide",
            "verify-before-done",
        )
        assert DIRECT_TASK_SKILL == "direct-task"
        assert DIRECT_TASK_SKILL not in PHASE_PROCEDURE_SKILLS

    def test_every_skill_ships(self):
        from shared.runtime.core.loader import get_project_root

        skills = get_project_root() / "config" / "skills"
        for name in (*PHASE_PROCEDURE_SKILLS, DIRECT_TASK_SKILL):
            assert (skills / name / "SKILL.md").is_file(), name

    def test_the_constants_are_immutable(self):
        assert isinstance(PHASE_PROCEDURE_TOOLS, tuple)
        assert isinstance(PHASE_PROCEDURE_SKILLS, tuple)


class TestIsPhasesValue:
    @pytest.mark.parametrize("value", TRUE_SPELLINGS + FALSE_SPELLINGS)
    def test_a_boolean_or_a_word_states_the_setting(self, value):
        assert is_phases_value(value) is True

    @pytest.mark.parametrize("value", (None, "", " ", *NOT_A_VALUE))
    def test_anything_else_does_not(self, value):
        assert is_phases_value(value) is False


class TestNormalizePhases:
    @pytest.mark.parametrize("value", TRUE_SPELLINGS)
    def test_true_spellings(self, value):
        assert normalize_phases(value) is True

    @pytest.mark.parametrize("value", FALSE_SPELLINGS)
    def test_false_spellings(self, value):
        assert normalize_phases(value) is False

    @pytest.mark.parametrize("value", [None, ""])
    def test_none_and_empty_mean_the_default(self, value):
        assert normalize_phases(value) is DEFAULT_PHASES

    @pytest.mark.parametrize("value", NOT_A_VALUE)
    def test_anything_else_is_refused(self, value):
        with pytest.raises(ValueError) as error:
            normalize_phases(value)
        message = str(error.value)
        assert message.startswith("phases must be true or false (on or off); got ")
        assert message.endswith(repr(value))

    def test_a_blank_word_is_refused(self):
        # Only exactly "" means "not given"; whitespace is a typo.
        with pytest.raises(ValueError):
            normalize_phases("  ")

    def test_the_result_is_a_real_bool(self):
        for value in ("on", "off", True, False, None):
            assert type(normalize_phases(value)) is bool


class TestPlansInPhases:
    @pytest.mark.parametrize("value", TRUE_SPELLINGS)
    def test_a_config_that_asks_for_phases(self, value):
        assert plans_in_phases({PHASES_KEY: value}) is True

    @pytest.mark.parametrize("value", FALSE_SPELLINGS)
    def test_a_config_that_says_no(self, value):
        assert plans_in_phases({PHASES_KEY: value}) is False

    @pytest.mark.parametrize("config", [None, {}, {"autonomy": "full"}])
    def test_without_the_key_the_default(self, config):
        assert plans_in_phases(config) is DEFAULT_PHASES

    @pytest.mark.parametrize("value", (None, "", *NOT_A_VALUE))
    def test_an_unreadable_value_is_the_default(self, value):
        # Lenient: stored configuration never raises here.
        assert plans_in_phases({PHASES_KEY: value}) is DEFAULT_PHASES

    @pytest.mark.parametrize("config", ["phases", ["phases"], 1, True, object()])
    def test_a_config_that_is_no_mapping_is_the_default(self, config):
        assert plans_in_phases(config) is DEFAULT_PHASES

    def test_any_mapping_is_read(self):
        assert plans_in_phases(MappingProxyType({PHASES_KEY: "on"})) is True

    def test_only_the_top_level_key_counts(self):
        assert plans_in_phases({"agent": {PHASES_KEY: True}}) is False
        assert plans_in_phases({"phase_settings": {PHASES_KEY: True}}) is False
