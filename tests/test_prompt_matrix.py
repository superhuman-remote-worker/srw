"""Tests for prompt matrix resolution: family_of + PromptMatrixResolver."""

import textwrap
from dataclasses import replace
from unittest.mock import patch

import pytest

from shared.runtime.core.loader import (
    AgentConfig,
    LLMConfig,
    PhaseLLMOverride,
    PromptMatrixResolver,
    PromptResolver,
    detect_reasoning_method,
    get_phase_system_prompt,
    load_base_system_prompt,
    render_placeholders,
    load_agent_config_from_dict,
    serialize_resolved_config,
)
from shared.runtime.core.model_registry import family_of


# =============================================================================
# family_of tests — registry-backed replacement for detect_model_family
# =============================================================================


class TestFamilyOf:
    """Pure prefix heuristic post chunk 6 — the YAML registry that used to
    return the explicit ``family`` field is gone. Catalog rows carry
    ``family`` directly; ``family_of`` is the sync fallback for callers
    without a row."""

    def test_claude_opus(self):
        assert family_of("claude-opus-4-6") == "claude-opus"

    def test_claude_sonnet(self):
        assert family_of("claude-sonnet-4-5-20250929") == "claude-sonnet"

    def test_gpt4o(self):
        # Heuristic-only post chunk 6: `gpt-4o` matches the gpt-4o prefix.
        # The modern family-matcher service routes gpt-4o to "default" for
        # discovery — this helper is a different surface and stays prefix-
        # accurate.
        assert family_of("gpt-4o") == "gpt-4o"
        assert family_of("gpt-4o-mini") == "gpt-4o"

    def test_groq_gpt_oss(self):
        assert family_of("groq/gpt-oss-120b") == "gpt-oss"

    def test_gemini(self):
        assert family_of("gemini-2.5-flash") == "gemini"
        assert family_of("gemini-2.5-pro") == "gemini"

    def test_openrouter_minimax(self):
        assert family_of("openrouter/minimax/minimax-m2.7") == "minimax"

    def test_codex_gpt5(self):
        # `gpt-5.3-codex` is the codex family — the heuristic recognises
        # `codex` substring + `gpt-5` prefix as codex, not bare gpt-5.
        assert family_of("codex/gpt-5.3-codex") == "codex"

    def test_mistral(self):
        # Mistral 3 family incl. Codestral and the `-latest` aliases. Native
        # api.mistral.ai serves bare ids; OpenRouter's `mistralai/` prefix is
        # stripped by the heuristic before matching.
        assert family_of("mistral-large-latest") == "mistral"
        assert family_of("mistral-medium-latest") == "mistral"
        assert family_of("mistral-small-latest") == "mistral"
        assert family_of("codestral-latest") == "mistral"
        assert family_of("openrouter/mistralai/mistral-large") == "mistral"

    def test_unknown_model_returns_default(self):
        # After heuristic fallback kicks in, unrecognized IDs still fall through
        # to 'default'. Use an ID with no known substring match.
        assert family_of("some-unknown-model") == "default"

    def test_unknown_returns_custom_default(self):
        """Callers can override the 'unknown' fallback."""
        assert family_of("unrecognized-model", default="custom") == "custom"

    def test_heuristic_fallback_for_bare_names(self):
        # Bare IDs (no catalog entry) get heuristic detection — needed
        # for custom endpoints that don't set an explicit family.
        assert family_of("deepseek-r1") == "deepseek"
        assert family_of("qwen-72b") == "qwen"
        assert family_of("llama-3.3-70b") == "llama"


# =============================================================================
# detect_reasoning_method tests — uses family_of() internally
# =============================================================================


class TestDetectReasoningMethod:
    """Tests for detect_reasoning_method() auto-detection and explicit override.

    Uses real catalog IDs only: synthetic IDs now return family 'default'
    which maps to reasoning_method='api'.
    """

    def test_groq_gpt_oss_returns_prompt(self):
        assert detect_reasoning_method("groq/gpt-oss-120b") == "prompt"

    def test_claude_returns_api(self):
        # Claude reaches adaptive thinking through `reasoning_effort` on the
        # OpenAI-shaped paths (subscription proxy / OpenRouter); the native
        # Anthropic factory injects nothing either way.
        assert detect_reasoning_method("claude-opus-4-6") == "api"
        assert detect_reasoning_method("claude-sonnet-4-5-20250929") == "api"

    def test_gemini_returns_none(self):
        assert detect_reasoning_method("gemini-2.5-flash") == "none"

    def test_gpt4o_returns_api(self):
        # gpt-4o resolves through family_of to "gpt-4o"; reasoning method
        # falls through to the default-family handling → 'api'.
        assert detect_reasoning_method("gpt-4o") == "api"

    def test_codex_returns_api(self):
        assert detect_reasoning_method("codex/gpt-5.4") == "api"

    def test_unknown_returns_api(self):
        # Unknown → family 'default' → 'api'.
        assert detect_reasoning_method("some-unknown-model") == "api"

    def test_glm_returns_api(self):
        # GLM-5.2 (OpenRouter) uses the standard reasoning API param; family
        # 'glm' must fall through to 'api', i.e. NOT be added to the "none" tuple.
        assert detect_reasoning_method("openrouter/z-ai/glm-5.2") == "api"

    def test_explicit_override(self):
        assert (
            detect_reasoning_method("groq/gpt-oss-120b", explicit_method="none")
            == "none"
        )
        assert (
            detect_reasoning_method("claude-opus-4-6", explicit_method="prompt")
            == "prompt"
        )
        assert detect_reasoning_method("gpt-4o", explicit_method="api") == "api"


# =============================================================================
# PromptMatrixResolver tests
# =============================================================================


class TestPromptMatrixResolver:
    """Tests for PromptMatrixResolver fallback chain."""

    def test_default_fallback_no_matrix_files(self, tmp_path):
        """Without any matrix files, hardcoded defaults are used."""
        resolver = PromptMatrixResolver(
            deployment_dir=str(tmp_path),
            model_family="claude-opus",
        )
        assert resolver.resolve_filename("systemprompt") == "systemprompt.txt"
        assert resolver.resolve_filename("summarization") == "summarization_prompt.txt"
        assert "strategic" not in PromptMatrixResolver.HARDCODED_DEFAULTS
        assert "tactical" not in PromptMatrixResolver.HARDCODED_DEFAULTS
        # Note: "instructions" moved to InstructionMatrixResolver

    def test_base_matrix_default_resolution(self, tmp_path):
        """Base matrix default entries are used when no expert matrix exists."""
        # Create base matrix
        base_matrix = tmp_path / "config" / "model_config_matrix.yaml"
        base_matrix.parent.mkdir(parents=True)
        base_matrix.write_text(
            textwrap.dedent("""\
            default:
              prompts:
                systemprompt: custom_systemprompt.txt
                persona: custom_persona.txt
        """)
        )

        with patch.object(
            PromptMatrixResolver, "__init__", lambda self, *a, **kw: None
        ):
            resolver = PromptMatrixResolver.__new__(PromptMatrixResolver)
            resolver.deployment_dir = None
            resolver.model_family = "default"
            resolver._prompt_resolver = PromptResolver(None)
            resolver._expert_matrix = {}
            resolver._base_matrix = PromptMatrixResolver._load_matrix_from_path(
                base_matrix
            )

        assert resolver.resolve_filename("systemprompt") == "custom_systemprompt.txt"
        assert resolver.resolve_filename("persona") == "custom_persona.txt"
        assert resolver.resolve_filename("summarization") == "summarization_prompt.txt"

    def test_expert_override(self, tmp_path):
        """Expert matrix entries override base matrix entries."""
        # Create base matrix
        base_matrix_path = tmp_path / "base_matrix.yaml"
        base_matrix_path.write_text(
            textwrap.dedent("""\
            default:
              prompts:
                systemprompt: base_system.txt
                persona: base_persona.txt
        """)
        )

        # Create expert matrix
        expert_matrix_path = tmp_path / "expert_matrix.yaml"
        expert_matrix_path.write_text(
            textwrap.dedent("""\
            default:
              prompts:
                persona: expert_persona.txt
        """)
        )

        with patch.object(
            PromptMatrixResolver, "__init__", lambda self, *a, **kw: None
        ):
            resolver = PromptMatrixResolver.__new__(PromptMatrixResolver)
            resolver.deployment_dir = tmp_path
            resolver.model_family = "default"
            resolver._prompt_resolver = PromptResolver(str(tmp_path))
            resolver._expert_matrix = PromptMatrixResolver._load_matrix_from_path(
                expert_matrix_path
            )
            resolver._base_matrix = PromptMatrixResolver._load_matrix_from_path(
                base_matrix_path
            )

        # Expert overrides persona
        assert resolver.resolve_filename("persona") == "expert_persona.txt"
        # Base provides systemprompt (expert doesn't override it)
        assert resolver.resolve_filename("systemprompt") == "base_system.txt"

    def test_model_specific_entry(self, tmp_path):
        """Model-specific entries take priority over default entries."""
        base_matrix_path = tmp_path / "base_matrix.yaml"
        base_matrix_path.write_text(
            textwrap.dedent("""\
            default:
              prompts:
                systemprompt: systemprompt.txt
                persona: persona.txt
            claude-opus:
              prompts:
                systemprompt: systemprompt_claude_opus.txt
        """)
        )

        with patch.object(
            PromptMatrixResolver, "__init__", lambda self, *a, **kw: None
        ):
            resolver = PromptMatrixResolver.__new__(PromptMatrixResolver)
            resolver.deployment_dir = None
            resolver.model_family = "claude-opus"
            resolver._prompt_resolver = PromptResolver(None)
            resolver._expert_matrix = {}
            resolver._base_matrix = PromptMatrixResolver._load_matrix_from_path(
                base_matrix_path
            )

        # Model-specific entry wins for systemprompt
        assert (
            resolver.resolve_filename("systemprompt") == "systemprompt_claude_opus.txt"
        )
        # Falls back to default for persona (no model-specific entry)
        assert resolver.resolve_filename("persona") == "persona.txt"

    def test_full_chain_4_levels(self, tmp_path):
        """Exercise the full 4-level fallback chain."""
        base_matrix_path = tmp_path / "base_matrix.yaml"
        base_matrix_path.write_text(
            textwrap.dedent("""\
            default:
              prompts:
                systemprompt: base_default_system.txt
                persona: base_default_persona.txt
                summarization: base_default_summarization.txt
                citation_verification: base_default_citation.txt
            claude-opus:
              prompts:
                summarization: base_claude_summarization.txt
        """)
        )

        expert_matrix_path = tmp_path / "expert_matrix.yaml"
        expert_matrix_path.write_text(
            textwrap.dedent("""\
            default:
              prompts:
                persona: expert_default_persona.txt
            claude-opus:
              prompts:
                systemprompt: expert_claude_system.txt
        """)
        )

        with patch.object(
            PromptMatrixResolver, "__init__", lambda self, *a, **kw: None
        ):
            resolver = PromptMatrixResolver.__new__(PromptMatrixResolver)
            resolver.deployment_dir = tmp_path
            resolver.model_family = "claude-opus"
            resolver._prompt_resolver = PromptResolver(str(tmp_path))
            resolver._expert_matrix = PromptMatrixResolver._load_matrix_from_path(
                expert_matrix_path
            )
            resolver._base_matrix = PromptMatrixResolver._load_matrix_from_path(
                base_matrix_path
            )

        # Level 1: Expert model-specific
        assert resolver.resolve_filename("systemprompt") == "expert_claude_system.txt"
        # Level 2: Expert default
        assert resolver.resolve_filename("persona") == "expert_default_persona.txt"
        # Level 3: Base model-specific
        assert (
            resolver.resolve_filename("summarization")
            == "base_claude_summarization.txt"
        )
        # Level 4: Base default
        assert (
            resolver.resolve_filename("citation_verification")
            == "base_default_citation.txt"
        )

    def test_load_matrix_invalid_yaml(self, tmp_path):
        """Invalid YAML in matrix file returns empty dict gracefully."""
        matrix_path = tmp_path / "model_config_matrix.yaml"
        matrix_path.write_text(":{invalid yaml")

        # Bypass the per-path cache so the malformed write is actually parsed.
        from shared.runtime.core import loader as _loader

        _loader._model_config_matrix_cache.pop(matrix_path, None)
        result = PromptMatrixResolver._load_matrix_from_path(matrix_path)
        assert result == {}

    def test_load_matrix_non_dict(self, tmp_path):
        """Non-dict YAML content returns empty dict."""
        matrix_path = tmp_path / "model_config_matrix.yaml"
        matrix_path.write_text("- just\n- a\n- list\n")

        from shared.runtime.core import loader as _loader

        _loader._model_config_matrix_cache.pop(matrix_path, None)
        result = PromptMatrixResolver._load_matrix_from_path(matrix_path)
        assert result == {}

    def test_load_matrix_nonexistent(self, tmp_path):
        """Non-existent matrix file returns empty dict."""
        from shared.runtime.core import loader as _loader

        _loader._model_config_matrix_cache.pop(tmp_path / "nope.yaml", None)
        result = PromptMatrixResolver._load_matrix_from_path(tmp_path / "nope.yaml")
        assert result == {}

    def test_unknown_prompt_type_fallback(self, tmp_path):
        """Unknown prompt types fall through to hardcoded default pattern."""
        resolver = PromptMatrixResolver(
            deployment_dir=str(tmp_path),
            model_family="default",
        )
        # Unknown type gets "{type}.txt" fallback
        assert resolver.resolve_filename("something_new") == "something_new.txt"

    def test_default_model_family_skips_model_levels(self, tmp_path):
        """When model_family is 'default', levels 1 and 3 are skipped."""
        base_matrix_path = tmp_path / "base_matrix.yaml"
        base_matrix_path.write_text(
            textwrap.dedent("""\
            default:
              prompts:
                systemprompt: default_system.txt
            claude-opus:
              prompts:
                systemprompt: claude_system.txt
        """)
        )

        with patch.object(
            PromptMatrixResolver, "__init__", lambda self, *a, **kw: None
        ):
            resolver = PromptMatrixResolver.__new__(PromptMatrixResolver)
            resolver.deployment_dir = None
            resolver.model_family = "default"
            resolver._prompt_resolver = PromptResolver(None)
            resolver._expert_matrix = {}
            resolver._base_matrix = PromptMatrixResolver._load_matrix_from_path(
                base_matrix_path
            )

        # Should use default, not claude-opus
        assert resolver.resolve_filename("systemprompt") == "default_system.txt"


# =============================================================================
# Default resolution tests (matrix-only, no legacy fallback)
# =============================================================================


class TestDefaultResolution:
    """Verify that functions work correctly with the matrix-only resolution path."""

    def test_load_base_system_prompt_with_resolver(self, tmp_path):
        """load_base_system_prompt uses PromptMatrixResolver."""
        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            config_prompts = tmp_path / "config" / "prompts"
            config_prompts.mkdir(parents=True)
            (config_prompts / "systemprompt.txt").write_text(
                "base template {prompt_content}"
            )

            resolver = PromptMatrixResolver(model_family="default")
            result = load_base_system_prompt(resolver)
            assert "base template" in result

    def test_get_phase_system_prompt_no_model(self, tmp_path):
        """Without model param, default model family is used."""
        config = AgentConfig(
            agent_id="test",
            display_name="Test Agent",
        )

        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            config_prompts = tmp_path / "config" / "prompts"
            config_prompts.mkdir(parents=True)
            (config_prompts / "systemprompt.txt").write_text(
                "{agent_display_name} phase-agnostic"
            )

            result = get_phase_system_prompt(
                config=config,
                is_strategic=True,
                phase_number=1,
            )
            assert "Test Agent" in result
            assert "phase-agnostic" in result

    def test_get_phase_system_prompt_with_model(self, tmp_path):
        """With model param, model family is detected and used."""
        config = AgentConfig(
            agent_id="test",
            display_name="Test Agent",
        )

        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            config_prompts = tmp_path / "config" / "prompts"
            config_prompts.mkdir(parents=True)
            (config_prompts / "systemprompt.txt").write_text(
                "{agent_display_name} phase-agnostic"
            )

            result = get_phase_system_prompt(
                config=config,
                is_strategic=True,
                phase_number=1,
                model="claude-opus-4-6",
            )
            assert "Test Agent" in result
            assert "phase-agnostic" in result
            # Claude models should not have reasoning directive injected
            assert "Reasoning:" not in result


# =============================================================================
# render_placeholders: literal braces in trusted prose must survive render
# (regression: the former product-qa tactical prompt's `{py,sh,md}` hard-failed via
#  str.format KeyError at phase render — vault issues/ brace-format crash)
# =============================================================================


class TestRenderPlaceholders:
    """The prompt assembler substitutes only an explicit allow-list of tokens;
    every other brace is literal prose (CSS, repro-path hints, JSON) and must
    pass through untouched instead of raising KeyError."""

    def test_known_token_substitutes(self):
        assert (
            render_placeholders("phase {phase_number}", phase_number="2") == "phase 2"
        )

    def test_literal_brace_survives(self):
        # The exact pattern that crashed the loop job.
        text = "save repro under output/repros/NNN_slug.{py,sh,md}"
        assert render_placeholders(text, phase_number="2") == text

    def test_css_colon_brace_survives(self):
        # A colon inside braces is the case a lenient str.format_map would still
        # choke on (parsed as a format spec) — render_placeholders leaves it be.
        css = "  :root { --app-bg: #1e1e2e; }"
        assert render_placeholders(css, phase_number="2") == css

    def test_single_pass_no_reexpansion(self):
        # A placeholder appearing inside a substituted value is NOT re-expanded
        # (matches str.format's single-pass semantics).
        out = render_placeholders(
            "n={agent_display_name} b={prompt_content}",
            agent_display_name="Ada",
            prompt_content="literal {agent_display_name} kept",
        )
        assert out == "n=Ada b=literal {agent_display_name} kept"

    def test_allowlisted_token_without_value_left_literal(self):
        assert (
            render_placeholders("keep {phase_number}", agent_display_name="x")
            == "keep {phase_number}"
        )

    def test_non_allowlisted_token_always_literal(self):
        assert render_placeholders("{tool_name} x", phase_number="1") == "{tool_name} x"

    def test_frozen_legacy_prompt_with_literal_braces(self):
        """The one-release frozen-config compatibility render remains
        brace-safe while substituting its old phase placeholders."""
        config = AgentConfig(agent_id="test", display_name="QA Agent")
        config.extra["_resolved_prompts"] = {
            "systemprompt": "{agent_display_name}\n{prompt_content}",
            "tactical": (
                "Tactical phase {phase_number}. "
                "Save a repro under output/repros/NNN_slug.{py,sh,md}."
            ),
        }

        result = get_phase_system_prompt(
            config=config,
            is_strategic=False,
            phase_number=2,
        )
        assert "QA Agent" in result
        assert "Tactical phase 2." in result  # {phase_number} substituted
        assert "{py,sh,md}" in result  # literal brace survived


# =============================================================================
# Integration: PromptMatrixResolver.load() with real files
# =============================================================================


class TestPromptMatrixResolverLoad:
    """Test that load() correctly resolves filenames and reads files."""

    def test_load_from_expert_directory(self, tmp_path):
        """Expert file takes priority in file search."""
        # Create expert directory with custom file
        expert_dir = tmp_path / "expert"
        expert_dir.mkdir()
        (expert_dir / "persona.txt").write_text("expert persona content")
        (expert_dir / "model_config_matrix.yaml").write_text(
            textwrap.dedent("""\
            default:
              prompts:
                persona: persona.txt
        """)
        )

        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            # Also create base files
            config_prompts = tmp_path / "config" / "prompts"
            config_prompts.mkdir(parents=True)
            (config_prompts / "persona.txt").write_text("base persona content")

            resolver = PromptMatrixResolver(
                deployment_dir=str(expert_dir),
                model_family="default",
            )
            result = resolver.load("persona")
            assert result == "expert persona content"

    def test_load_falls_through_to_framework(self, tmp_path):
        """When expert dir doesn't have the file, framework dir is used."""
        expert_dir = tmp_path / "expert"
        expert_dir.mkdir()
        # No systemprompt.txt in expert dir, but the framework provides it.

        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            config_prompts = tmp_path / "config" / "prompts"
            config_prompts.mkdir(parents=True)
            (config_prompts / "systemprompt.txt").write_text("framework systemprompt")
            # No base matrix file either — hardcoded defaults will be used

            resolver = PromptMatrixResolver(
                deployment_dir=str(expert_dir),
                model_family="default",
            )
            result = resolver.load("systemprompt")
            assert result == "framework systemprompt"

    def test_exists_returns_true_for_resolvable(self, tmp_path):
        """exists() returns True when the file can be found."""
        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            config_prompts = tmp_path / "config" / "prompts"
            config_prompts.mkdir(parents=True)
            (config_prompts / "systemprompt.txt").write_text("exists")

            resolver = PromptMatrixResolver(model_family="default")
            assert resolver.exists("systemprompt") is True

    def test_exists_returns_false_for_missing(self, tmp_path):
        """exists() returns False when file cannot be found."""
        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            config_prompts = tmp_path / "config" / "prompts"
            config_prompts.mkdir(parents=True)

            resolver = PromptMatrixResolver(model_family="default")
            assert resolver.exists("nonexistent_type") is False


# =============================================================================
# Location-primary resolution — regression cover for family-variant shadowing
# (knowledge-base/knowledge/issues/expert_prompts_shadowed_by_family_variants.md)
# =============================================================================


class TestLocationPrimaryResolution:
    """An expert (deployment-dir) file outranks a framework file; family-specific
    is tried before base WITHIN each dir. Order:
    expert/<family> -> expert/<base> -> framework/<family> -> framework/<base>.
    """

    GEMMA_MATRIX = textwrap.dedent("""\
        gemma:
          prompts:
            persona: persona_gemma.txt
    """)

    @pytest.mark.parametrize("entry_type", ["persona"])
    def test_expert_base_beats_framework_family_variant(self, tmp_path, entry_type):
        """THE bug: expert ships only <type>.txt; base matrix maps the family to
        <type>_gemma.txt which exists in the framework dir. The expert's base
        file must win (it never did before this fix)."""
        expert_dir = tmp_path / "expert"
        expert_dir.mkdir()
        (expert_dir / f"{entry_type}.txt").write_text(f"expert base {entry_type}")

        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            config_dir = tmp_path / "config"
            prompts = config_dir / "prompts"
            prompts.mkdir(parents=True)
            (prompts / f"{entry_type}.txt").write_text(f"framework base {entry_type}")
            (prompts / f"{entry_type}_gemma.txt").write_text(
                f"framework gemma {entry_type}"
            )
            (config_dir / "model_config_matrix.yaml").write_text(self.GEMMA_MATRIX)

            resolver = PromptMatrixResolver(
                deployment_dir=str(expert_dir), model_family="gemma"
            )
            assert resolver.load(entry_type) == f"expert base {entry_type}"

    def test_expert_family_variant_wins_rank1(self, tmp_path):
        """Expert that ships its own family variant gets it (rank 1)."""
        expert_dir = tmp_path / "expert"
        expert_dir.mkdir()
        (expert_dir / "persona.txt").write_text("expert base")
        (expert_dir / "persona_gemma.txt").write_text("expert gemma")

        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            config_dir = tmp_path / "config"
            prompts = config_dir / "prompts"
            prompts.mkdir(parents=True)
            (prompts / "persona_gemma.txt").write_text("framework gemma")
            (config_dir / "model_config_matrix.yaml").write_text(self.GEMMA_MATRIX)

            resolver = PromptMatrixResolver(
                deployment_dir=str(expert_dir), model_family="gemma"
            )
            assert resolver.load("persona") == "expert gemma"

    def test_expert_empty_uses_framework_family_rank3(self, tmp_path):
        """Expert overrides nothing -> framework family variant (rank 3)."""
        expert_dir = tmp_path / "expert"
        expert_dir.mkdir()

        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            config_dir = tmp_path / "config"
            prompts = config_dir / "prompts"
            prompts.mkdir(parents=True)
            (prompts / "persona.txt").write_text("framework base")
            (prompts / "persona_gemma.txt").write_text("framework gemma")
            (config_dir / "model_config_matrix.yaml").write_text(self.GEMMA_MATRIX)

            resolver = PromptMatrixResolver(
                deployment_dir=str(expert_dir), model_family="gemma"
            )
            assert resolver.load("persona") == "framework gemma"

    def test_no_family_variant_uses_framework_base_rank4(self, tmp_path):
        """Family with no variant -> framework base (rank 4)."""
        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            prompts = tmp_path / "config" / "prompts"
            prompts.mkdir(parents=True)
            (prompts / "persona.txt").write_text("framework base")

            resolver = PromptMatrixResolver(model_family="gemma")
            assert resolver.load("persona") == "framework base"

    def test_expert_matrix_remap_respected(self, tmp_path):
        """An expert model_config_matrix override names a custom file; it's used
        from the expert dir (proves _resolve_path consumes resolve_filename)."""
        expert_dir = tmp_path / "expert"
        expert_dir.mkdir()
        (expert_dir / "custom_persona.txt").write_text("expert custom")
        (expert_dir / "model_config_matrix.yaml").write_text(
            textwrap.dedent("""\
            gemma:
              prompts:
                persona: custom_persona.txt
        """)
        )

        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            config_dir = tmp_path / "config"
            prompts = config_dir / "prompts"
            prompts.mkdir(parents=True)
            (prompts / "persona_gemma.txt").write_text("framework gemma")
            (config_dir / "model_config_matrix.yaml").write_text(self.GEMMA_MATRIX)

            resolver = PromptMatrixResolver(
                deployment_dir=str(expert_dir), model_family="gemma"
            )
            assert resolver.load("persona") == "expert custom"

    def test_deployment_dir_none_is_framework_only(self, tmp_path):
        """deployment_dir=None (sessions/admin) -> framework chain, no crash."""
        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            config_dir = tmp_path / "config"
            prompts = config_dir / "prompts"
            prompts.mkdir(parents=True)
            (prompts / "persona.txt").write_text("framework base")
            (prompts / "persona_gemma.txt").write_text("framework gemma")
            (config_dir / "model_config_matrix.yaml").write_text(self.GEMMA_MATRIX)

            resolver = PromptMatrixResolver(model_family="gemma")
            assert resolver.load("persona") == "framework gemma"

    def test_db_override_short_circuits_before_files(self, tmp_path):
        """A DB config override wins over any file; bundled_only bypasses it and
        reads the expert base via location-primary resolution."""
        expert_dir = tmp_path / "expert"
        expert_dir.mkdir()
        (expert_dir / "persona.txt").write_text("expert base persona")

        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            (tmp_path / "config" / "prompts").mkdir(parents=True)
            resolver = PromptMatrixResolver(
                deployment_dir=str(expert_dir), model_family="default"
            )
            with patch(
                "shared.runtime.core.loader._db_lookup", return_value="DB OVERRIDE"
            ):
                assert resolver.load("persona") == "DB OVERRIDE"
                assert (
                    resolver.load("persona", bundled_only=True) == "expert base persona"
                )

    def test_exists_true_for_expert_base_on_gemma(self, tmp_path):
        """exists() finds the expert base even when the matrix names a (missing)
        family variant."""
        expert_dir = tmp_path / "expert"
        expert_dir.mkdir()
        (expert_dir / "persona.txt").write_text("expert base")

        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            config_dir = tmp_path / "config"
            (config_dir / "prompts").mkdir(parents=True)
            (config_dir / "model_config_matrix.yaml").write_text(self.GEMMA_MATRIX)

            resolver = PromptMatrixResolver(
                deployment_dir=str(expert_dir), model_family="gemma"
            )
            assert resolver.exists("persona") is True


# =============================================================================
# LLMConfig.get_phase_config() — model_max_context_tokens merge tests
# =============================================================================


class TestPhaseConfigContextTokens:
    """Tests for model_max_context_tokens merge in get_phase_config().

    Since U1 ``summarization`` is the only override slot; the legacy phase
    names resolve to the single model (identity).
    """

    def test_base_value_inherited_when_no_override(self):
        """Resolved config inherits base model_max_context_tokens when unset."""
        config = LLMConfig(model="gpt-4o", model_max_context_tokens=128000)
        phase = config.get_phase_config("summarization")
        assert phase.model_max_context_tokens == 128000

    def test_override_replaces_base(self):
        """The override's model_max_context_tokens replaces the base value."""
        config = LLMConfig(
            model="gpt-4o",
            model_max_context_tokens=128000,
            summarization=PhaseLLMOverride(
                model="gpt-4o-mini", model_max_context_tokens=32000
            ),
        )
        phase = config.get_phase_config("summarization")
        assert phase.model_max_context_tokens == 32000
        assert phase.model == "gpt-4o-mini"

    def test_override_none_keeps_base(self):
        """When override has model_max_context_tokens=None, base is kept."""
        config = LLMConfig(
            model="gpt-4o",
            model_max_context_tokens=128000,
            summarization=PhaseLLMOverride(temperature=0.5),
        )
        phase = config.get_phase_config("summarization")
        assert phase.model_max_context_tokens == 128000
        assert phase.temperature == 0.5

    def test_base_none_override_sets(self):
        """When base has no model_max_context_tokens, override can set it."""
        config = LLMConfig(
            model="gpt-4o",
            summarization=PhaseLLMOverride(model_max_context_tokens=32000),
        )
        assert config.model_max_context_tokens is None
        phase = config.get_phase_config("summarization")
        assert phase.model_max_context_tokens == 32000

    def test_no_override_returns_self(self):
        """No override returns self (identity) — and so do the legacy names."""
        config = LLMConfig(model="gpt-4o", model_max_context_tokens=128000)
        assert config.get_phase_config("summarization") is config
        assert config.get_phase_config("strategic") is config
        assert config.get_phase_config("tactical") is config

    def test_resolved_config_has_no_phase_fields(self):
        """Resolved config carries no override of its own (and no tier fields)."""
        config = LLMConfig(
            model="gpt-4o",
            model_max_context_tokens=128000,
            summarization=PhaseLLMOverride(model_max_context_tokens=200000),
        )
        phase = config.get_phase_config("summarization")
        assert phase.summarization is None
        assert not hasattr(phase, "strategic")
        assert not hasattr(phase, "tactical")


# =============================================================================
# LLM reuse — full config equality tests
# =============================================================================


class TestLLMReuseEquality:
    """LLM reuse compares the full resolved config, not just the model name.

    ``_create_phase_llms`` reuses the main client for summarization when the
    resolved override equals ``replace(llm, summarization=None)``.
    """

    @staticmethod
    def _main(config):
        return replace(config, summarization=None)

    def test_same_model_same_settings_are_equal(self):
        """An override that resolves to the main config is equal (reuse)."""
        config = LLMConfig(
            model="gpt-4o",
            temperature=0.3,
            model_max_context_tokens=128000,
            summarization=PhaseLLMOverride(model="gpt-4o"),
        )
        assert config.get_phase_config("summarization") == self._main(config)

    def test_same_model_different_context_tokens_not_equal(self):
        """Same model but different context tokens should NOT be equal."""
        config = LLMConfig(
            model="gpt-4o",
            model_max_context_tokens=128000,
            summarization=PhaseLLMOverride(model_max_context_tokens=32000),
        )
        assert config.get_phase_config("summarization") != self._main(config)

    def test_same_model_different_temperature_not_equal(self):
        """Same model but different temperature should NOT be equal."""
        config = LLMConfig(
            model="gpt-4o",
            temperature=0.3,
            summarization=PhaseLLMOverride(temperature=0.0),
        )
        assert config.get_phase_config("summarization") != self._main(config)

    def test_different_models_not_equal(self):
        """Different models are obviously not equal."""
        config = LLMConfig(
            model="gpt-4o",
            summarization=PhaseLLMOverride(model="gpt-4o-mini"),
        )
        assert config.get_phase_config("summarization") != self._main(config)


# =============================================================================
# U2 WP6: one phase-agnostic prompt, with frozen pre-U2 resume compatibility
# =============================================================================


class TestSingleWorkerPrompt:
    """``get_system_prompt`` is the worker's one prompt; ``get_phase_system_prompt``
    delegates to it unless the frozen template is a pre-U2 one with the bare
    ``{prompt_content}`` slot."""

    _TEMPLATE = (
        "{agent_display_name}\n<phase_model>alternating phases</phase_model>\nEND"
    )

    def _write(self, tmp_path):
        config_prompts = tmp_path / "config" / "prompts"
        config_prompts.mkdir(parents=True)
        (config_prompts / "systemprompt.txt").write_text(self._TEMPLATE)

    def test_get_system_prompt_is_phase_agnostic(self, tmp_path):
        from shared.runtime.core.loader import get_system_prompt

        config = AgentConfig(agent_id="test", display_name="Test Agent")
        with patch(
            "shared.runtime.core.loader.get_project_root", return_value=tmp_path
        ):
            self._write(tmp_path)
            one = get_system_prompt(config, tool_names=[])
            strategic = get_phase_system_prompt(
                config, is_strategic=True, phase_number=1, tool_names=[]
            )
            tactical = get_phase_system_prompt(
                config, is_strategic=False, phase_number=2, tool_names=[]
            )
        assert "Test Agent" in one
        assert "<phase_model>alternating phases</phase_model>" in one
        assert "phase_directive" not in one and "{prompt_content}" not in one
        assert "strategic phase" not in one and "tactical phase" not in one
        assert "{%" not in one and "legacy_phase_prompt" not in one
        assert one == strategic == tactical  # the swap is gone

    def test_frozen_legacy_config_still_renders(self):
        config = AgentConfig(agent_id="test", display_name="Test Agent")
        config.extra["_resolved_prompts"] = {
            "systemprompt": (
                "OLD {agent_display_name}\n"
                "<phase_directive>{prompt_content}</phase_directive>"
            ),
            "strategic": "strategic phase {phase_number}",
            "tactical": "tactical phase {phase_number}",
        }
        strategic = get_phase_system_prompt(
            config, is_strategic=True, phase_number=1, tool_names=[]
        )
        tactical = get_phase_system_prompt(
            config, is_strategic=False, phase_number=2, tool_names=[]
        )
        assert "<phase_directive>strategic phase 1</phase_directive>" in strategic
        assert "<phase_directive>tactical phase 2</phase_directive>" in tactical
        assert all("{prompt_content}" not in out for out in (strategic, tactical))

    def test_removed_mode_keys_in_old_snapshots_are_ignored(self):
        config = load_agent_config_from_dict(
            {
                "agent_id": "test",
                "display_name": "Test Agent",
                "phase_settings": {
                    "min_todos": 2,
                    "max_todos": 7,
                    "prompt_mode": "legacy",
                    "tool_binding_mode": "filtered",
                },
            }
        )
        assert config.phase_settings.min_todos == 2
        assert config.phase_settings.max_todos == 7
        assert not hasattr(config.phase_settings, "prompt_mode")
        assert not hasattr(config.phase_settings, "tool_binding_mode")

    def test_new_frozen_blob_has_no_phase_prompt_or_mode_keys(self):
        config = load_agent_config_from_dict(
            {"agent_id": "test", "display_name": "Test Agent"}
        )
        blob = serialize_resolved_config(config)
        assert "strategic" not in blob["prompts"]
        assert "tactical" not in blob["prompts"]
        assert set(blob["agent"]["phase_settings"]) == {
            "min_todos",
            "max_todos",
            "strategic_tools",
        }
