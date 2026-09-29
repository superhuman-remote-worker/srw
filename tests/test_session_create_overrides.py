"""The create-time override bridge and its warn-phase ignored-key report.

Pins Defect B of
knowledge-base/knowledge/issues/live_settings_silently_dropped_on_stateless_sessions.md:
the New Session form nests ``reasoning_level`` / ``temperature`` under
``config_override.llm`` while ``create_thread`` bridged only the top-level
request fields, so a create-time reasoning pick was dropped.
"""

from __future__ import annotations

import pytest

from orchestrator.services.session_create_overrides import (
    SessionOverrideError,
    bridge_nested_llm_override,
    ignored_override_paths,
)


def _validate(level):
    if level not in {"low", "medium", "high", "xhigh", "max", "none"}:
        raise ValueError(f"bad level {level!r}")
    return level


class TestBridgeNestedLlmOverride:
    def test_nested_reasoning_and_temperature_are_bridged(self):
        rebuilt = {"llm": {"model": "gpt-5.6-sol"}}
        bridged = bridge_nested_llm_override(
            {
                "llm": {
                    "model": "gpt-5.6-sol",
                    "reasoning_level": "max",
                    "temperature": 0.2,
                }
            },
            rebuilt,
            validate_reasoning_level=_validate,
        )
        assert rebuilt["llm"] == {
            "model": "gpt-5.6-sol",
            "reasoning_level": "max",
            "temperature": 0.2,
        }
        assert bridged == ["llm.temperature", "llm.reasoning_level"]

    def test_top_level_field_wins_over_its_nested_twin(self):
        # The caller already bridged the explicit request field; the nested
        # copy (stale form state, or a conflicting API body) must not clobber it.
        rebuilt = {"llm": {"reasoning_level": "high"}}
        bridge_nested_llm_override(
            {"llm": {"reasoning_level": "low"}},
            rebuilt,
            validate_reasoning_level=_validate,
        )
        assert rebuilt["llm"]["reasoning_level"] == "high"

    def test_nested_model_is_bridged_when_no_top_level_model(self):
        rebuilt: dict = {}
        bridge_nested_llm_override(
            {"llm": {"model": "gpt-6-astra"}},
            rebuilt,
            validate_reasoning_level=_validate,
        )
        assert rebuilt == {"llm": {"model": "gpt-6-astra"}}

    def test_nested_reasoning_runs_the_vocabulary_check(self):
        with pytest.raises(ValueError, match="bad level"):
            bridge_nested_llm_override(
                {"llm": {"reasoning_level": "ultra"}},
                {},
                validate_reasoning_level=_validate,
            )

    @pytest.mark.parametrize("bad", ["0.2", True, [0.2]])
    def test_malformed_temperature_is_refused_not_dropped(self, bad):
        with pytest.raises(SessionOverrideError):
            bridge_nested_llm_override(
                {"llm": {"temperature": bad}}, {}, validate_reasoning_level=_validate
            )

    @pytest.mark.parametrize("bad", ["", "   ", 7])
    def test_malformed_model_is_refused(self, bad):
        with pytest.raises(SessionOverrideError):
            bridge_nested_llm_override(
                {"llm": {"model": bad}}, {}, validate_reasoning_level=_validate
            )

    def test_null_and_absent_values_are_not_bridged(self):
        rebuilt: dict = {}
        assert (
            bridge_nested_llm_override(
                {"llm": {"reasoning_level": None}},
                rebuilt,
                validate_reasoning_level=_validate,
            )
            == []
        )
        assert (
            bridge_nested_llm_override(
                None, rebuilt, validate_reasoning_level=_validate
            )
            == []
        )
        assert (
            bridge_nested_llm_override({}, rebuilt, validate_reasoning_level=_validate)
            == []
        )
        # A touched-but-empty llm section is not left behind.
        assert rebuilt.get("llm", {}) == {}


class TestIgnoredOverridePaths:
    def test_reports_nested_keys_the_rebuild_did_not_carry(self):
        sent = {
            "llm": {"model": "m", "base_url": "http://x"},
            "memory": {"enabled": False},
            "interactive": {
                "permission_mode": "autonomous",
                "narration_mode": "silent",
            },
        }
        rebuilt = {
            "llm": {"model": "m"},
            "interactive": {"permission_mode": "autonomous"},
        }
        assert ignored_override_paths(sent, rebuilt) == [
            "interactive.narration_mode",
            "llm.base_url",
            "memory.enabled",
        ]

    def test_a_key_sent_twice_is_not_reported_when_carried(self):
        # The form lifts `model` top-level AND leaves it nested; the rebuild
        # carries it once, so nothing was lost.
        assert (
            ignored_override_paths({"llm": {"model": "m"}}, {"llm": {"model": "m"}})
            == []
        )

    def test_value_mismatch_counts_as_ignored(self):
        # Carried under the same key but with a different value means the
        # caller's value was not what won — report it.
        assert ignored_override_paths(
            {"llm": {"model": "a"}}, {"llm": {"model": "b"}}
        ) == ["llm.model"]

    def test_non_dict_section_is_reported_whole(self):
        assert ignored_override_paths({"skills": ["x"]}, {}) == ["skills"]
        assert ignored_override_paths({"skills": ["x"]}, {"skills": ["x"]}) == []

    def test_empty_and_none_inputs(self):
        assert ignored_override_paths(None, {"llm": {}}) == []
        assert ignored_override_paths({}, {}) == []


class TestDelegationOverride:
    """The gate half of the Delegation toggle. ``delegate_agent`` is
    ``grant: explicit``: names in ``tools.delegation`` AND ``delegation.enabled``
    — the create rebuild carried only the names, so a ticked Delegation box
    produced "configured tool(s) did not bind" for all five."""

    def test_live_keys_pass_and_are_typed(self):
        from orchestrator.services.session_create_overrides import (
            validate_delegation_override,
        )

        assert validate_delegation_override(
            {"enabled": True, "max_concurrent": 3, "run_in_background_default": False}
        ) == {"enabled": True, "max_concurrent": 3, "run_in_background_default": False}

    @pytest.mark.parametrize(
        "bad",
        [
            {"enabled": "yes"},
            {"enabled": 1},
            {"max_concurrent": 0},
            {"max_concurrent": True},
            {"max_concurrent": "4"},
            {"run_in_background_default": "no"},
            {"something_else": 1},
            "enabled",
        ],
    )
    def test_malformed_is_refused_not_dropped(self, bad):
        from orchestrator.services.session_create_overrides import (
            validate_delegation_override,
        )

        with pytest.raises(SessionOverrideError):
            validate_delegation_override(bad)

    @pytest.mark.parametrize("cap", [1, 6, 20])
    def test_a_session_may_set_its_own_fanout_cap(self, cap):
        # parallel_subagents.md D2: the session cap is overridable per session.
        from orchestrator.services.session_create_overrides import (
            validate_delegation_override,
        )

        assert validate_delegation_override(
            {"enabled": True, "session_max_concurrent": cap}
        ) == {"enabled": True, "session_max_concurrent": cap}

    @pytest.mark.parametrize("cap", [0, 21, -1, True, False, "6", 6.0, None])
    def test_the_session_cap_is_an_integer_from_1_to_20(self, cap):
        from orchestrator.services.session_create_overrides import (
            validate_delegation_override,
        )

        with pytest.raises(SessionOverrideError, match="from 1 to 20"):
            validate_delegation_override({"session_max_concurrent": cap})

    @pytest.mark.parametrize("gate", ["session_fanout", "session_fanout_pinned"])
    def test_no_session_key_opens_fanout(self, gate):
        """Fan-out is the orchestrator's deployment setting, advertised per
        claim (parallel_subagents.md §12, WP3c); the WP3a config gate names
        are plain unknown keys."""
        from orchestrator.services.session_create_overrides import (
            validate_delegation_override,
        )

        with pytest.raises(SessionOverrideError, match="not a session delegation"):
            validate_delegation_override({"enabled": True, gate: True})

    def test_the_bridge_carries_the_session_cap(self):
        from orchestrator.services.session_create_overrides import (
            bridge_nested_delegation_override,
        )

        rebuilt: dict = {"tools": {"delegation": ["delegate_agent"]}}
        bridged = bridge_nested_delegation_override(
            {"delegation": {"enabled": True, "session_max_concurrent": 8}}, rebuilt
        )
        assert rebuilt["delegation"] == {"enabled": True, "session_max_concurrent": 8}
        assert bridged == ["delegation.enabled", "delegation.session_max_concurrent"]

    def test_legacy_keys_are_tolerated_and_dropped(self):
        # The loader drops these with a deprecation warning; a stored layer may
        # still carry them, so they must not turn a valid request into a 400.
        from orchestrator.services.session_create_overrides import (
            validate_delegation_override,
        )

        assert validate_delegation_override(
            {"enabled": True, "mode": "light", "default_timeout": 30}
        ) == {"enabled": True}

    def test_bridge_folds_the_block_and_reports_paths(self):
        from orchestrator.services.session_create_overrides import (
            bridge_nested_delegation_override,
        )

        rebuilt: dict = {"tools": {"delegation": ["delegate_agent"]}}
        bridged = bridge_nested_delegation_override(
            {"delegation": {"enabled": True, "max_concurrent": 2}}, rebuilt
        )
        assert rebuilt["delegation"] == {"enabled": True, "max_concurrent": 2}
        assert bridged == ["delegation.enabled", "delegation.max_concurrent"]

    def test_bridge_is_a_no_op_without_the_block(self):
        from orchestrator.services.session_create_overrides import (
            bridge_nested_delegation_override,
        )

        rebuilt: dict = {}
        assert bridge_nested_delegation_override(None, rebuilt) == []
        assert bridge_nested_delegation_override({"llm": {}}, rebuilt) == []
        assert bridge_nested_delegation_override({"delegation": {}}, rebuilt) == []
        assert rebuilt == {}

    def test_bridge_never_clobbers_a_key_the_caller_already_set(self):
        from orchestrator.services.session_create_overrides import (
            bridge_nested_delegation_override,
        )

        rebuilt: dict = {"delegation": {"enabled": False}}
        bridge_nested_delegation_override({"delegation": {"enabled": True}}, rebuilt)
        assert rebuilt["delegation"] == {"enabled": False}

    def test_ignored_paths_no_longer_report_a_bridged_delegation_block(self):
        rebuilt: dict = {"delegation": {"enabled": True}}
        assert ignored_override_paths({"delegation": {"enabled": True}}, rebuilt) == []
