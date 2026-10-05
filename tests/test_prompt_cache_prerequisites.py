"""Prompt-cache prerequisites for the subscription proxy.

Design: knowledge-base/knowledge/features/compaction_refactor_fidelity_and_fork_strategy.md
(F13, F14, F17, WP0).

- Claude over the proxy gets explicit cache breakpoints on the system prompt and
  on the last message before the per-turn injections, never on the injected tail.
- A conversation key (``LLMConfig.prompt_cache_key``) reaches the proxy as the
  ``X-Session-ID`` session-affinity header, and the Codex lane also forwards it
  as ``prompt_cache_key``.
- The chart turns on CLIProxyAPI session affinity.
"""

import dataclasses
import json
import shutil
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
import yaml
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from shared.runtime.core import loader
from shared.runtime.core.injection_markers import (
    KNOWLEDGE_TOOL_CALL_ID_PREFIX,
    MEMORY_TOOL_CALL_ID_PREFIX,
)
from shared.runtime.llm import reasoning_chat
from shared.runtime.llm.reasoning_chat import mark_anthropic_cache_breakpoints

PROXY_URL = "http://srw-codex-proxy:8317/v1"
CC = {"type": "ephemeral"}


def _worker_request(with_tail: bool = True) -> list:
    """A worker-shaped request: stable history, then the per-turn injections."""
    messages = [
        SystemMessage(content="System prompt."),
        HumanMessage(content="# Task brief"),
        AIMessage(
            content="",
            tool_calls=[{"id": "call_1", "name": "read_file", "args": {"path": "a"}}],
        ),
        ToolMessage(content="file contents", tool_call_id="call_1"),
    ]
    if with_tail:
        for prefix, text in (
            (MEMORY_TOOL_CALL_ID_PREFIX, "--- Pinned Memories ---"),
            (KNOWLEDGE_TOOL_CALL_ID_PREFIX, "--- Project Knowledge ---"),
        ):
            call_id = f"{prefix}1"
            messages.append(
                AIMessage(
                    content="",
                    tool_calls=[{"id": call_id, "name": "recall", "args": {}}],
                )
            )
            messages.append(ToolMessage(content=text, tool_call_id=call_id))
    return messages


def _payload(messages: list) -> list:
    return [{"role": "x", "content": str(i)} for i in range(len(messages))]


class TestMarkAnthropicCacheBreakpoints:
    def test_marks_system_and_last_message_before_the_injections(self):
        messages = _worker_request()
        payload = _payload(messages)
        payload[0]["role"] = "system"

        out = mark_anthropic_cache_breakpoints(messages, payload)

        marked = [i for i, m in enumerate(out) if "cache_control" in m]
        # 0 = system prompt, 3 = the real tool result; 4..7 are the tail.
        assert marked == [0, 3]
        assert out[3]["cache_control"] == CC

    def test_without_injections_the_last_message_is_marked(self):
        messages = _worker_request(with_tail=False)
        payload = _payload(messages)
        payload[0]["role"] = "system"

        out = mark_anthropic_cache_breakpoints(messages, payload)

        assert [i for i, m in enumerate(out) if "cache_control" in m] == [0, 3]

    def test_input_dicts_are_not_mutated(self):
        messages = _worker_request()
        payload = _payload(messages)
        payload[0]["role"] = "system"

        mark_anthropic_cache_breakpoints(messages, payload)

        assert all("cache_control" not in m for m in payload)

    def test_length_mismatch_leaves_the_payload_unchanged(self):
        messages = _worker_request()
        payload = _payload(messages)[:-1]

        assert mark_anthropic_cache_breakpoints(messages, payload) is payload


def _capture(monkeypatch):
    captured = []

    def respond(request):
        captured.append(
            {"headers": dict(request.headers), "body": json.loads(request.content)}
        )
        return httpx.Response(
            200,
            json={
                "id": "fixture",
                "object": "chat.completion",
                "created": 0,
                "model": "fixture",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "OK"},
                    }
                ],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 1,
                    "total_tokens": 11,
                },
            },
        )

    original_client = reasoning_chat.ReasoningCapturingClient
    original_async_client = reasoning_chat.AsyncReasoningCapturingClient
    monkeypatch.setattr(
        reasoning_chat,
        "ReasoningCapturingClient",
        lambda **kw: original_client(**kw, transport=httpx.MockTransport(respond)),
    )
    monkeypatch.setattr(
        reasoning_chat,
        "AsyncReasoningCapturingClient",
        lambda **kw: original_async_client(
            **kw, transport=httpx.MockTransport(respond)
        ),
    )
    monkeypatch.setattr(reasoning_chat, "count_request_tokens", lambda *a, **kw: 10)
    return captured


def _llm_config(model: str, base_url: str, **overrides) -> loader.LLMConfig:
    return dataclasses.replace(
        loader.LLMConfig(),
        model=model,
        provider="openai",
        base_url=base_url,
        api_key="fixture-key",
        max_retries=0,
        **overrides,
    )


class TestSubscriptionProxyWire:
    def test_claude_request_carries_breakpoints_and_session_header(self, monkeypatch):
        captured = _capture(monkeypatch)
        llm = loader.create_llm(
            _llm_config("claude-opus-5-5", PROXY_URL, prompt_cache_key="srw-job-j1")
        )
        try:
            assert llm.anthropic_cache_breakpoints is True
            llm.invoke(_worker_request())
        finally:
            llm.http_client.close()

        (request,) = captured
        assert request["headers"]["x-session-id"] == "srw-job-j1"
        # The body field is for the Codex lane only; this is chat completions.
        assert "prompt_cache_key" not in request["body"]
        marked = [
            (m["role"], i)
            for i, m in enumerate(request["body"]["messages"])
            if m.get("cache_control") == CC
        ]
        assert marked == [("system", 0), ("tool", 3)]

    def test_non_claude_models_get_no_breakpoints(self, monkeypatch):
        captured = _capture(monkeypatch)
        llm = loader.create_llm(
            _llm_config("gpt-5.6-sol", PROXY_URL, prompt_cache_key="srw-job-j1")
        )
        try:
            assert llm.anthropic_cache_breakpoints is False
            llm.invoke(_worker_request())
        finally:
            llm.http_client.close()

        (request,) = captured
        assert not any("cache_control" in m for m in request["body"]["messages"])
        assert request["headers"]["x-session-id"] == "srw-job-j1"

    def test_other_endpoints_get_neither_header_nor_breakpoints(self, monkeypatch):
        captured = _capture(monkeypatch)
        llm = loader.create_llm(
            _llm_config(
                "claude-opus-5-5",
                "https://claude-fixture.invalid/v1",
                prompt_cache_key="srw-job-j1",
            )
        )
        try:
            assert llm.anthropic_cache_breakpoints is False
            llm.invoke(_worker_request())
        finally:
            llm.http_client.close()

        (request,) = captured
        assert "x-session-id" not in request["headers"]
        assert not any("cache_control" in m for m in request["body"]["messages"])

    def test_no_conversation_key_means_no_session_header(self, monkeypatch):
        captured = _capture(monkeypatch)
        llm = loader.create_llm(_llm_config("claude-opus-5-5", PROXY_URL))
        try:
            llm.invoke(_worker_request())
        finally:
            llm.http_client.close()

        assert "x-session-id" not in captured[0]["headers"]


class TestCodexLaneSessionKey:
    @staticmethod
    def _config(**overrides):
        config = MagicMock()
        config.model = "gpt-5.6-sol"
        config.base_url = overrides.get("base_url", PROXY_URL)
        config.api_key = "fixture-key"
        config.temperature = 1.0
        config.top_p = None
        config.top_k = None
        config.max_retries = 0
        config.timeout = None
        config.reasoning_level = None
        config.max_output_tokens = None
        config.model_max_context_tokens = None
        config.extra_body = None
        config.extra_headers = None
        config.prompt_cache_key = overrides.get("prompt_cache_key")
        return config

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_proxy_gets_header_and_body_key(self, mock_chat):
        mock_chat.return_value = MagicMock()
        loader._create_codex_llm(self._config(prompt_cache_key="srw-thread-t1"))

        kwargs = mock_chat.call_args[1]
        assert kwargs["default_headers"] == {"X-Session-ID": "srw-thread-t1"}
        assert kwargs["extra_body"]["prompt_cache_key"] == "srw-thread-t1"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_without_a_key_nothing_is_added(self, mock_chat):
        mock_chat.return_value = MagicMock()
        loader._create_codex_llm(self._config())

        kwargs = mock_chat.call_args[1]
        assert "default_headers" not in kwargs
        assert "prompt_cache_key" not in (kwargs.get("extra_body") or {})


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
@pytest.mark.parametrize("affinity", [True, False])
def test_chart_renders_proxy_session_affinity(tmp_path, affinity):
    values = tmp_path / "values.yaml"
    values.write_text(
        yaml.safe_dump(
            {
                "fullnameOverride": "srw",
                "codexProxy": {"enabled": True, "sessionAffinity": affinity},
            }
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            "helm",
            "template",
            "srw",
            str(CHART),
            "--namespace",
            "srw",
            "-f",
            str(CHART / "ci/test-values.yaml"),
            "-f",
            str(values),
            "--show-only",
            "templates/optional/codex-proxy.yaml",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    config_map = next(
        d for d in yaml.safe_load_all(result.stdout) if d and d["kind"] == "ConfigMap"
    )
    proxy_config = yaml.safe_load(config_map["data"]["config.yaml"])
    assert proxy_config["routing"] == {
        "session-affinity": affinity,
        "session-affinity-ttl": "1h",
    }
