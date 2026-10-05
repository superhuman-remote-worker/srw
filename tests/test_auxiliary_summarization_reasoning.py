"""Summarization on a dedicated auxiliary model runs with thinking off.

``auxiliary.summarization_reasoning_level`` (default ``"off"``) gives the
summarizer its own client of the same aux model; the other aux tasks keep the
client's level. "off" is the family's off switch where it has one, else its
lowest listed effort — sending no effort is not off, the provider's default
applies. See knowledge-base/knowledge/issues/auxiliary_reasoning_level_not_configurable.md.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml
from langchain_core.messages import AIMessage

from shared.runtime.core.loader import (
    AuxiliaryConfig,
    LLMConfig,
    _create_codex_llm,
    _create_openai_llm,
    _create_openrouter_llm,
    _parse_auxiliary_config,
    build_auxiliary_llm_config,
    create_auxiliary_llms,
    get_project_root,
    load_agent_config,
    resolve_reasoning_plan,
)
from shared.runtime.core.llm_retry import NO_RETRY
from shared.runtime.services.auxiliary import (
    AuxiliaryLLM,
    SummarizeTask,
)


class TestSummarizationReasoningConfig:
    def test_defaults_to_off(self):
        assert AuxiliaryConfig().summarization_reasoning_level == "off"
        assert (
            _parse_auxiliary_config({"model": "x"}).summarization_reasoning_level
            == "off"
        )

    def test_unquoted_yaml_off_still_means_off(self):
        data = yaml.safe_load("summarization_reasoning_level: off")
        assert data["summarization_reasoning_level"] is False
        assert _parse_auxiliary_config(data).summarization_reasoning_level == "off"

    def test_null_means_no_separate_level(self):
        cfg = _parse_auxiliary_config({"summarization_reasoning_level": None})
        assert cfg.summarization_reasoning_level is None

    def test_explicit_level_is_kept(self):
        cfg = _parse_auxiliary_config({"summarization_reasoning_level": "medium"})
        assert cfg.summarization_reasoning_level == "medium"

    def test_expert_base_ships_off(self):
        cfg = load_agent_config(str(get_project_root() / "config/worker_base.yaml"))
        assert cfg.auxiliary.summarization_reasoning_level == "off"


class TestOffPerFamily:
    """What "off" resolves to for each kind of family capability."""

    @pytest.mark.parametrize(
        "model, value",
        [
            ("gpt-6.1-sol", "low"),  # lowest listed; Astra 400s on `none`
            ("gpt-6-astra", "low"),
            ("claude-opus-5-5", "low"),
            ("muse-spark-1.3", "minimal"),
            ("deepseek-v4-flash", "none"),  # OpenRouter's own off
            ("kimi-k3", "low"),
        ],
    )
    def test_effort_families_get_their_lowest_setting(self, model, value):
        plan = resolve_reasoning_plan(LLMConfig(model=model, reasoning_level="off"))
        assert plan["method"] == "effort_enum"
        assert plan["value"] == value

    def test_toggle_family_switches_thinking_off(self):
        plan = resolve_reasoning_plan(
            LLMConfig(model="MiniMax-M3", reasoning_level="off")
        )
        assert plan == {**plan, "method": "binary_toggle", "value": "off"}

    def test_no_reasoning_family_injects_nothing(self):
        plan = resolve_reasoning_plan(
            LLMConfig(model="gemini-3-pro", reasoning_level="off")
        )
        assert plan["value"] is None

    def test_none_still_injects_nothing(self):
        """``none`` keeps its old meaning (send no effort); only ``off`` is new."""
        plan = resolve_reasoning_plan(
            LLMConfig(model="gpt-6.1-sol", reasoning_level="none")
        )
        assert plan["value"] is None


class TestOffOnTheWire:
    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_sol_responses_api_gets_low_effort(self, mock_chat):
        _create_openai_llm(
            LLMConfig(model="gpt-6.1-sol", reasoning_level="off", api_key="k")
        )
        assert mock_chat.call_args[1]["reasoning"] == {
            "effort": "low",
            "summary": "auto",
        }

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_codex_proxy_gets_low_effort(self, mock_chat):
        _create_codex_llm(
            LLMConfig(
                model="gpt-6-astra",
                reasoning_level="off",
                api_key="k",
                base_url="http://srw-codex-proxy:8317/v1",
            )
        )
        assert mock_chat.call_args[1]["reasoning"]["effort"] == "low"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_openrouter_deepseek_gets_effort_none(self, mock_chat):
        _create_openrouter_llm(
            LLMConfig(
                model="deepseek/deepseek-v4-flash", reasoning_level="off", api_key="k"
            )
        )
        assert mock_chat.call_args[1]["extra_body"]["reasoning"] == {"effort": "none"}

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_minimax_m3_gets_thinking_disabled(self, mock_chat):
        _create_openai_llm(
            LLMConfig(
                model="MiniMax-M3",
                reasoning_level="off",
                api_key="k",
                base_url="https://api.minimax.io/v1",
            )
        )
        assert mock_chat.call_args[1]["extra_body"]["thinking"] == {"type": "disabled"}


def _aux(**overrides) -> AuxiliaryConfig:
    base = dict(
        model="MiniMax-M3",
        base_url="https://api.minimax.io/v1",
        api_key="k",
        provider="openai",
        extra_headers={"X-Route": "1"},
        temperature=0.0,
    )
    return AuxiliaryConfig(**{**base, **overrides})


SETTINGS = {
    "top_p": 0.95,
    "top_k": 40,
    "model_max_context_tokens": 200_000,
    "extra_body": {"reasoning_split": True},
}


class TestAuxiliaryBuilder:
    def test_config_carries_transport_and_family_settings(self):
        cfg = build_auxiliary_llm_config(_aux(), SETTINGS)
        assert (cfg.model, cfg.base_url, cfg.api_key, cfg.provider) == (
            "MiniMax-M3",
            "https://api.minimax.io/v1",
            "k",
            "openai",
        )
        assert cfg.extra_headers == {"X-Route": "1"}
        assert (cfg.top_p, cfg.top_k) == (0.95, 40)
        assert cfg.model_max_context_tokens == 200_000
        assert cfg.extra_body == {"reasoning_split": True}
        assert cfg.max_retries == 1
        # The other aux tasks keep the client's level, unchanged by this work.
        assert cfg.reasoning_level == "high"

    def _create(self, aux):
        created = []

        def fake_create_llm(cfg, limits=None):
            created.append(cfg)
            return SimpleNamespace(cfg=cfg)

        with patch(
            "shared.runtime.core.loader.create_llm", side_effect=fake_create_llm
        ):
            return create_auxiliary_llms(aux, SETTINGS), created

    def test_summarization_client_is_the_same_model_with_thinking_off(self):
        clients, created = self._create(_aux())
        assert [c.reasoning_level for c in created] == ["high", "off"]
        main, summary = created
        assert clients.llm.cfg is main
        assert clients.summarization_llm.cfg is summary
        # Only the level differs.
        assert summary == LLMConfig(**{**vars(main), "reasoning_level": "off"})

    def test_unset_level_builds_one_client(self):
        clients, created = self._create(_aux(summarization_reasoning_level=None))
        assert len(created) == 1
        assert clients.summarization_llm is None

    def test_level_equal_to_the_client_builds_one_client(self):
        clients, created = self._create(_aux(summarization_reasoning_level="high"))
        assert len(created) == 1
        assert clients.summarization_llm is None


def _mock_llm(name: str, *, result=None, error: Exception | None = None):
    llm = MagicMock()
    llm.model_name = name
    if error is not None:
        llm.ainvoke = AsyncMock(side_effect=error)
    else:
        llm.ainvoke = AsyncMock(
            return_value=result
            if result is not None
            else AIMessage(content="ok", response_metadata={"finish_reason": "stop"})
        )
    return llm


class TestSummarizeTaskUsesTheSummarizationClient:
    @pytest.mark.asyncio
    async def test_summary_goes_to_the_thinking_off_client(self):
        aux, summary = _mock_llm("m3"), _mock_llm("m3")
        llm = AuxiliaryLLM(llm=aux, summarization_llm=summary)

        await llm.complete(SummarizeTask("[User]: hi", "## Objective"))

        summary.ainvoke.assert_awaited_once()
        aux.ainvoke.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_other_tasks_stay_on_the_main_aux_client(self):
        aux, summary = _mock_llm("m3"), _mock_llm("m3")
        llm = AuxiliaryLLM(llm=aux, summarization_llm=summary)

        await llm.ainvoke([], task_name="title")

        aux.ainvoke.assert_awaited_once()
        summary.ainvoke.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_without_a_summarization_client_the_aux_client_summarizes(self):
        aux = _mock_llm("m3")
        llm = AuxiliaryLLM(llm=aux)

        await llm.complete(SummarizeTask("[User]: hi", "## Objective"))

        aux.ainvoke.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_failed_summary_still_falls_back_to_the_main_model(self):
        summary = _mock_llm("m3", error=RuntimeError("401 unauthorized"))
        main = _mock_llm("gpt-6.1-sol")
        llm = AuxiliaryLLM(
            llm=_mock_llm("m3"), summarization_llm=summary, fallback_llm=main
        )

        await llm.complete(
            SummarizeTask("[User]: hi", "## Objective"), retry_policy=NO_RETRY
        )

        summary.ainvoke.assert_awaited_once()
        main.ainvoke.assert_awaited_once()


class TestWorkerWiresTheSummarizationClient:
    def test_dedicated_aux_model_gets_a_thinking_off_summarizer(self):
        from agent.agent import UniversalAgent

        agent = UniversalAgent.__new__(UniversalAgent)
        cfg = load_agent_config(str(get_project_root() / "config/worker_base.yaml"))
        cfg.auxiliary.model = "MiniMax-M3"
        cfg.auxiliary.base_url = "https://api.minimax.io/v1"
        cfg.auxiliary.api_key = "k"
        agent.config = cfg
        agent._summarization_llm = MagicMock(name="summarization_llm")
        agent._auxiliary_llm = None
        agent._citation_verify_aux = None

        with patch(
            "shared.runtime.core.loader.create_llm",
            side_effect=lambda c, limits=None: SimpleNamespace(
                model_name=c.model, cfg=c
            ),
        ):
            agent._initialize_auxiliary_llm(
                cfg.llm, MagicMock(model_max_context_tokens=100_000)
            )

        aux = agent._auxiliary_llm
        assert aux.llm.cfg.reasoning_level == "high"
        assert aux.summarization_llm.cfg.reasoning_level == "off"
        assert aux.summarization_llm.cfg.model == "MiniMax-M3"
