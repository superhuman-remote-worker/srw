"""A family with ``send_max_tokens`` gets its output cap as ``max_tokens``.

langchain-openai renames the cap to ``max_completion_tokens`` on every Chat
Completions request. DeepSeek's own API does not apply a cap sent under that
name: measured on 2026-11-16, a request with ``max_completion_tokens: 96``
came back with 2,425 completion tokens and ``finish_reason: stop``. The
``deepseek`` family therefore sends the cap under the name the endpoint
honours (model_config_matrix.yaml).

These tests pin the request body with the setting on and off through the real
SDK behind an offline transport, and that the setting belongs to the model
family the client is built for.
"""

from __future__ import annotations

import pytest

from shared.runtime.core import loader
from shared.runtime.core.loader import PhaseLLMOverride
from tests.test_reasoning_replay import (
    _capture,
    _deepseek_config,
    _history,
    _load,
    _reply,
)

pytestmark = pytest.mark.filterwarnings(
    "ignore:Parameters .*reasoning_effort.* should be specified explicitly:UserWarning"
)


def _close(llm) -> None:
    llm.http_client.close()


class TestTheRequestBody:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("mode", ["sync", "async"])
    async def test_deepseek_gets_max_tokens(self, tmp_path, monkeypatch, mode):
        captured = _capture(monkeypatch, _reply)
        config = _deepseek_config(tmp_path, streaming=False, max_output_tokens=96)
        llm = loader.create_llm(config.llm, limits=config.limits)
        try:
            assert llm.send_max_tokens is True
            if mode == "async":
                await llm.ainvoke(_history())
            else:
                llm.invoke(_history())
        finally:
            _close(llm)

        (body,) = captured
        assert body["max_tokens"] == 96
        assert "max_completion_tokens" not in body

    @pytest.mark.asyncio
    async def test_a_streamed_request_too(self, tmp_path, monkeypatch):
        """The cap is in the body the client builds, whichever way the reply
        is read."""
        config = _deepseek_config(tmp_path, streaming=True, max_output_tokens=512)
        llm = loader.create_llm(config.llm, limits=config.limits)
        try:
            payload = llm._get_request_payload(_history(), stream=True)
        finally:
            _close(llm)

        assert payload["max_tokens"] == 512
        assert "max_completion_tokens" not in payload

    def test_a_family_without_the_setting_is_unchanged(self, tmp_path, monkeypatch):
        captured = _capture(monkeypatch, _reply)
        config = _load(
            tmp_path,
            "gpt-oss-120b",
            provider="openai",
            api_key="fixture-key",
            base_url="https://other-fixture.invalid/v1",
            max_retries=0,
            streaming=False,
            max_output_tokens=96,
        )
        llm = loader.create_llm(config.llm, limits=config.limits)
        try:
            assert llm.send_max_tokens is False
            llm.invoke(_history())
        finally:
            _close(llm)

        (body,) = captured
        assert body["max_completion_tokens"] == 96
        assert "max_tokens" not in body


class TestTheSettingIsTheFamilys:
    @pytest.mark.parametrize(
        "model", ["deepseek-flash", "deepseek/deepseek-v4-pro", "deepseek-v4-flash"]
    )
    def test_deepseek_has_it(self, model):
        assert loader.resolve_model_settings(model).get("send_max_tokens") is True

    @pytest.mark.parametrize(
        "model", ["gpt-oss-120b", "kimi-k3", "qwen/qwen3.8-max", "gpt-5.5", ""]
    )
    def test_no_other_family_has_it(self, model):
        assert loader.resolve_model_settings(model).get("send_max_tokens") is not True

    def test_a_summarization_model_of_another_family_is_not_armed(
        self, tmp_path, monkeypatch
    ):
        """The factory asks the family of the model the client is built for."""
        _capture(monkeypatch, _reply)
        main = _deepseek_config(tmp_path).llm
        other = main.with_override(
            PhaseLLMOverride(model="gpt-oss-120b", base_url="https://o.invalid/v1")
        )
        llm = loader.create_llm(other)
        try:
            assert llm.send_max_tokens is False
        finally:
            _close(llm)
