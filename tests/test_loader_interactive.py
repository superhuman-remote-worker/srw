"""Tests for InteractiveConfig loading in src/core/loader.py.

Covers section 8 of persistent_agent_tests.md:
  8.1 InteractiveConfig defaults
  8.2 Config parsing
  8.3 Prompt resolution for interactive mode
"""

from unittest.mock import patch

from shared.runtime.core.loader import (
    InteractiveConfig,
    AgentConfig,
    load_agent_config_from_dict,
    get_phase_system_prompt,
)
from shared.runtime.core.skill_resolution import APP_GUIDE_LOADER_TOOL, APP_GUIDE_SKILL


# =============================================================================
# Helpers
# =============================================================================


def _minimal_config_data(**overrides):
    """Return the minimum valid config dict for load_agent_config_from_dict."""
    data = {
        "agent_id": "test_agent",
        "display_name": "Test Agent",
    }
    data.update(overrides)
    return data


# =============================================================================
# 8.1: InteractiveConfig defaults
# =============================================================================


class TestInteractiveConfigDefaults:
    """Tests for InteractiveConfig default values."""

    def test_permission_mode_defaults_to_read_only(self):
        ic = InteractiveConfig()
        assert ic.permission_mode == "read_only"

    def test_idle_timeout_defaults_to_30(self):
        ic = InteractiveConfig()
        assert ic.idle_timeout_minutes == 30

    def test_agent_config_has_interactive_field(self):
        """AgentConfig includes interactive as a field with default factory."""
        ac = AgentConfig(agent_id="x", display_name="X")
        assert isinstance(ac.interactive, InteractiveConfig)

    def test_agent_config_interactive_defaults_match(self):
        """AgentConfig's default InteractiveConfig matches standalone defaults."""
        ac = AgentConfig(agent_id="x", display_name="X")
        standalone = InteractiveConfig()
        assert ac.interactive.permission_mode == standalone.permission_mode
        assert ac.interactive.idle_timeout_minutes == standalone.idle_timeout_minutes


# =============================================================================
# 8.2: Config parsing
# =============================================================================


class TestInteractiveConfigParsing:
    """Tests for parsing interactive section from config dicts."""

    def test_missing_interactive_section_uses_defaults(self):
        """When no 'interactive' key in data, defaults are used."""
        data = _minimal_config_data()
        config = load_agent_config_from_dict(data)
        assert config.interactive.permission_mode == "read_only"
        assert config.interactive.idle_timeout_minutes == 30

    def test_explicit_permission_mode_overrides_default(self):
        data = _minimal_config_data(interactive={"permission_mode": "autonomous"})
        config = load_agent_config_from_dict(data)
        assert config.interactive.permission_mode == "autonomous"

    def test_explicit_idle_timeout_overrides_default(self):
        data = _minimal_config_data(interactive={"idle_timeout_minutes": 120})
        config = load_agent_config_from_dict(data)
        assert config.interactive.idle_timeout_minutes == 120

    def test_partial_interactive_section(self):
        """Only specified fields override; others keep defaults."""
        data = _minimal_config_data(interactive={"permission_mode": "auto_accept"})
        config = load_agent_config_from_dict(data)
        assert config.interactive.permission_mode == "auto_accept"
        assert config.interactive.idle_timeout_minutes == 30

    def test_empty_interactive_section_uses_defaults(self):
        """Empty interactive dict is the same as missing."""
        data = _minimal_config_data(interactive={})
        config = load_agent_config_from_dict(data)
        assert config.interactive.permission_mode == "read_only"
        assert config.interactive.idle_timeout_minutes == 30

    def test_interactive_config_is_correct_type(self):
        data = _minimal_config_data(interactive={"permission_mode": "read_only"})
        config = load_agent_config_from_dict(data)
        assert isinstance(config.interactive, InteractiveConfig)


class TestContextManagementParsing:
    """Context management parse tests for keep-window truncation config."""

    def test_keep_window_cap_defaults(self):
        cfg = load_agent_config_from_dict(_minimal_config_data())
        assert cfg.context_management.keep_window_max_tool_result_chars == 16000

    def test_keep_window_cap_override(self):
        cfg = load_agent_config_from_dict(
            _minimal_config_data(
                context_management={"keep_window_max_tool_result_chars": 12345}
            )
        )
        assert cfg.context_management.keep_window_max_tool_result_chars == 12345


# =============================================================================
# 8.3: Prompt resolution for interactive mode
# =============================================================================


class TestInteractivePromptResolution:
    """Tests for get_phase_system_prompt with prompt_type='interactive'."""

    def _make_config(self, resolved_prompts=None, deployment_dir=None):
        """Create an AgentConfig suitable for prompt resolution."""
        extra = {}
        if resolved_prompts:
            extra["_resolved_prompts"] = resolved_prompts
        return AgentConfig(
            agent_id="test",
            display_name="Test Agent",
            extra=extra,
            _deployment_dir=deployment_dir,
        )

    def test_uses_resolved_systemprompt_interactive(self):
        """When resolved_prompts has systemprompt_interactive, use it."""
        template = "Hello {agent_display_name}! {expert_identity}"
        config = self._make_config(
            resolved_prompts={"systemprompt_interactive": template}
        )

        result = get_phase_system_prompt(
            config=config,
            is_strategic=False,
            prompt_type="interactive",
            model="gpt-4o",
        )
        assert "Test Agent" in result

    def test_expert_identity_from_resolved_prompts(self):
        """persona from resolved_prompts is injected into expert_identity."""
        template = "Agent: {agent_display_name}. Identity: {expert_identity}"
        config = self._make_config(
            resolved_prompts={
                "systemprompt_interactive": template,
                "persona": "I am a scholar.",
            }
        )

        result = get_phase_system_prompt(
            config=config,
            is_strategic=False,
            prompt_type="interactive",
            model="gpt-4o",
        )
        assert "I am a scholar." in result

    def test_missing_persona_uses_empty_string(self):
        """When no persona in resolved_prompts or filesystem, expert_identity is empty."""
        template = "Agent: {agent_display_name}. Identity: [{expert_identity}]"
        config = self._make_config(
            resolved_prompts={"systemprompt_interactive": template}
        )

        # Also patch resolver.load to raise FileNotFoundError for persona
        with patch(
            "shared.runtime.core.loader.PromptMatrixResolver.load",
            side_effect=FileNotFoundError("no persona"),
        ):
            result = get_phase_system_prompt(
                config=config,
                is_strategic=False,
                prompt_type="interactive",
                model="gpt-4o",
            )
        assert "Identity: []" in result

    def test_falls_back_to_matrix_resolver(self):
        """When not in resolved_prompts, falls back to resolver.load()."""
        template = "Hello {agent_display_name}! {expert_identity}"
        config = self._make_config(resolved_prompts={})

        with patch(
            "shared.runtime.core.loader.PromptMatrixResolver.load",
            return_value=template,
        ):
            result = get_phase_system_prompt(
                config=config,
                is_strategic=False,
                prompt_type="interactive",
                model="gpt-4o",
            )
        assert "Test Agent" in result

    def test_jinja2_conditionals_rendered_with_tool_names(self):
        """Jinja2 tool conditionals should be rendered when tool_names provided."""
        template = (
            "{agent_display_name}{expert_identity}"
            "{% if has_tool('web_search') %}HAS_SEARCH{% endif %}"
        )
        config = self._make_config(
            resolved_prompts={"systemprompt_interactive": template}
        )

        result = get_phase_system_prompt(
            config=config,
            is_strategic=False,
            prompt_type="interactive",
            model="gpt-4o",
            tool_names=["web_search", "read_file"],
        )
        assert "HAS_SEARCH" in result

    def test_jinja2_conditionals_without_tool(self):
        """Jinja2 block should be excluded when tool is not present."""
        template = (
            "{agent_display_name}{expert_identity}"
            "{% if has_tool('web_search') %}HAS_SEARCH{% endif %}"
        )
        config = self._make_config(
            resolved_prompts={"systemprompt_interactive": template}
        )

        result = get_phase_system_prompt(
            config=config,
            is_strategic=False,
            prompt_type="interactive",
            model="gpt-4o",
            tool_names=["read_file"],
        )
        assert "HAS_SEARCH" not in result

    def test_managed_product_guide_floor_does_not_need_skills_placeholder(self):
        """Every prompt family gets the trusted floor when the reader is live."""
        digest = "a" * 64
        template = "SYS {agent_display_name} {expert_identity}"
        config = self._make_config(
            resolved_prompts={"systemprompt_interactive": template}
        )
        config.extra["_resolved_skills"] = {
            "menu": [
                {
                    "name": APP_GUIDE_SKILL,
                    "system_managed": True,
                    "loader_tool": APP_GUIDE_LOADER_TOOL,
                    "bundle_digest": digest,
                }
            ]
        }

        result = get_phase_system_prompt(
            config=config,
            is_strategic=False,
            prompt_type="interactive",
            model="gpt-5",
            tool_names=[APP_GUIDE_LOADER_TOOL],
        )

        assert "<managed_product_guide" in result
        assert f'current_bundle_sha256="{digest}"' in result
        assert "on every relevant turn" in result
        assert "summaries, memories, prior tool results" in result
        assert result.endswith("</managed_product_guide>")

    def test_managed_product_guide_floor_requires_live_reader(self):
        digest = "b" * 64
        template = "SYS {agent_display_name} {expert_identity}"
        config = self._make_config(
            resolved_prompts={"systemprompt_interactive": template}
        )
        config.extra["_resolved_skills"] = {
            "menu": [
                {
                    "name": APP_GUIDE_SKILL,
                    "system_managed": True,
                    "loader_tool": APP_GUIDE_LOADER_TOOL,
                    "bundle_digest": digest,
                }
            ]
        }

        result = get_phase_system_prompt(
            config=config,
            is_strategic=False,
            prompt_type="interactive",
            model="gpt-5",
            tool_names=["read_file"],
        )

        assert "<managed_product_guide" not in result

    def test_managed_product_guide_floor_rejects_unmanaged_spoof(self):
        template = "SYS {agent_display_name} {expert_identity}"
        config = self._make_config(
            resolved_prompts={"systemprompt_interactive": template}
        )
        config.extra["_resolved_skills"] = {
            "menu": [
                {
                    "name": APP_GUIDE_SKILL,
                    "system_managed": False,
                    "loader_tool": APP_GUIDE_LOADER_TOOL,
                    "bundle_digest": "c" * 64,
                }
            ]
        }

        result = get_phase_system_prompt(
            config=config,
            is_strategic=False,
            prompt_type="interactive",
            model="gpt-5",
            tool_names=[APP_GUIDE_LOADER_TOOL],
        )

        assert "<managed_product_guide" not in result

    def test_reasoning_directive_prepended_for_prompt_method(self):
        """OSS models with reasoning_method='prompt' get 'Reasoning: X' prefix."""
        template = "{agent_display_name}{expert_identity}"
        config = self._make_config(
            resolved_prompts={"systemprompt_interactive": template}
        )
        config.llm.reasoning_method = "prompt"
        config.llm.reasoning_level = "high"

        with patch(
            "shared.runtime.core.loader.detect_reasoning_method", return_value="prompt"
        ):
            result = get_phase_system_prompt(
                config=config,
                is_strategic=False,
                prompt_type="interactive",
                model="deepseek-reasoner",
            )
        assert result.startswith("Reasoning: high")

    def test_no_reasoning_directive_for_native_method(self):
        """Standard models should NOT get reasoning directive prefix."""
        template = "{agent_display_name}{expert_identity}"
        config = self._make_config(
            resolved_prompts={"systemprompt_interactive": template}
        )

        with patch(
            "shared.runtime.core.loader.detect_reasoning_method", return_value="native"
        ):
            result = get_phase_system_prompt(
                config=config,
                is_strategic=False,
                prompt_type="interactive",
                model="gpt-4o",
            )
        assert not result.startswith("Reasoning:")
