"""Tests for ReasoningChatOpenAI — Layer 0 overflow protection and reasoning capture.

Tests count_request_tokens, ContextOverflowError raising, reasoning_content
extraction, _extract_responses_api_reasoning, the _is_quota_error heuristic,
and the pinned endpoint certificate on both capture clients.
"""
# ruff: noqa: F811  (the imported fixture is requested by name as a test argument)

import json
from unittest.mock import MagicMock

import httpx
import pytest

from shared.runtime.llm.exceptions import ContextOverflowError
from shared.runtime.llm.reasoning_chat import (
    count_request_tokens,
    _extract_reasoning_from_delta,
    _extract_reasoning_from_response,
    _extract_responses_api_reasoning,
    _install_streaming_reasoning_tap,
    _is_debug_stream,
    _get_debug_tail_chars,
    _dump_codex_raw_response,
    _SSEReasoningTap,
    ReasoningCapturingClient,
    ReasoningChatOpenAI,
    record_wire_attempts,
)
from shared.tls_pinning import (
    AsyncPinnedTransport,
    CertificateChangedError,
    PinnedTransport,
    TlsPin,
)
from tests.tls_pin_fixtures import caused_by
from tests.tls_pin_fixtures import tls_certs, tls_server  # noqa: F401


# =============================================================================
# ContextOverflowError
# =============================================================================


class TestContextOverflowError:
    """Tests for the ContextOverflowError exception."""

    def test_stores_attributes(self):
        err = ContextOverflowError(
            token_count=150_000, limit=100_000, request_size_bytes=500_000
        )
        assert err.token_count == 150_000
        assert err.limit == 100_000
        assert err.request_size_bytes == 500_000

    def test_default_message(self):
        err = ContextOverflowError(token_count=120_000, limit=100_000)
        assert "120,000" in str(err)
        assert "100,000" in str(err)

    def test_custom_message(self):
        err = ContextOverflowError(token_count=1, limit=1, message="custom")
        assert str(err) == "custom"


# =============================================================================
# count_request_tokens
# =============================================================================


class TestCountRequestTokens:
    """Tests for count_request_tokens."""

    def test_empty_body(self):
        """Empty request should have minimal tokens (overhead only)."""
        result = count_request_tokens({})
        assert result >= 0
        assert result < 50  # Just overhead

    def test_counts_messages(self):
        """Should count tokens in message content."""
        body = {
            "messages": [
                {"role": "system", "content": "You are helpful."},
                {"role": "user", "content": "Hello " * 100},
            ]
        }
        result = count_request_tokens(body)
        assert result > 50  # Non-trivial content

    def test_more_content_means_more_tokens(self):
        """Larger messages should produce higher token counts."""
        small = {"messages": [{"role": "user", "content": "Hi"}]}
        large = {"messages": [{"role": "user", "content": "Hello " * 1000}]}
        assert count_request_tokens(large) > count_request_tokens(small)

    def test_counts_tool_definitions(self):
        """Should count tokens in tool definitions."""
        without_tools = {"messages": [{"role": "user", "content": "test"}]}
        with_tools = {
            "messages": [{"role": "user", "content": "test"}],
            "tools": [
                {
                    "type": "function",
                    "function": {"name": "read_file", "parameters": {"type": "object"}},
                },
                {
                    "type": "function",
                    "function": {
                        "name": "write_file",
                        "parameters": {"type": "object"},
                    },
                },
            ],
        }
        assert count_request_tokens(with_tools) > count_request_tokens(without_tools)

    def test_counts_tool_calls_in_messages(self):
        """Should count tokens in assistant tool_calls."""
        body = {
            "messages": [
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {"name": "read_file", "arguments": "{}"},
                        }
                    ],
                },
            ]
        }
        result = count_request_tokens(body)
        assert result > 10

    def test_counts_multimodal_text_parts(self):
        """Should count text in multimodal content parts."""
        body = {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Describe this image " * 50},
                        {
                            "type": "image_url",
                            "image_url": {"url": "data:image/png;base64,abc"},
                        },
                    ],
                }
            ]
        }
        result = count_request_tokens(body)
        assert result > 50

    def test_handles_responses_api_format(self):
        """Should count tokens in Responses API 'input' field."""
        body = {
            "input": [
                {"role": "user", "content": "Hello " * 100},
            ],
            "instructions": "Be helpful " * 50,
        }
        result = count_request_tokens(body)
        assert result > 50

    def test_handles_string_input_items(self):
        """Should handle plain string items in Responses API input."""
        body = {"input": ["Hello world " * 100]}
        result = count_request_tokens(body)
        assert result > 10


# =============================================================================
# _extract_responses_api_reasoning
# =============================================================================


class TestExtractResponsesApiReasoning:
    """Tests for extracting reasoning from Responses API format."""

    def test_skips_string_content(self):
        """Should not modify messages with string content."""
        msg = MagicMock()
        msg.content = "Just a string"
        msg.additional_kwargs = {}
        _extract_responses_api_reasoning(msg)
        assert msg.content == "Just a string"

    def test_extracts_reasoning_blocks(self):
        """Should extract reasoning from typed blocks."""
        msg = MagicMock()
        msg.content = [
            {
                "type": "reasoning",
                "summary": [{"type": "summary_text", "text": "thinking..."}],
            },
            {"type": "output_text", "text": "Final answer"},
        ]
        msg.additional_kwargs = {}
        _extract_responses_api_reasoning(msg)

        assert msg.additional_kwargs["reasoning_content"] == "thinking..."
        assert msg.content == "Final answer"

    def test_handles_no_reasoning(self):
        """Should leave content alone when no reasoning blocks."""
        msg = MagicMock()
        msg.content = [
            {"type": "output_text", "text": "Just output"},
        ]
        msg.additional_kwargs = {}
        _extract_responses_api_reasoning(msg)
        assert "reasoning_content" not in msg.additional_kwargs

    def test_handles_empty_list(self):
        """Should handle empty content list."""
        msg = MagicMock()
        msg.content = []
        msg.additional_kwargs = {}
        _extract_responses_api_reasoning(msg)
        assert msg.content == ""


# =============================================================================
# ReasoningCapturingClient — token validation
# =============================================================================


class TestReasoningCapturingClientTokenValidation:
    """Tests for token validation in ReasoningCapturingClient."""

    def test_max_context_from_param(self):
        """Should use explicit max_context_tokens parameter."""
        client = ReasoningCapturingClient(max_context_tokens=50_000)
        assert client._max_context_tokens == 50_000

    def test_max_context_default(self):
        """Should default to DEFAULT_MAX_CONTEXT_TOKENS."""
        client = ReasoningCapturingClient()
        assert client._max_context_tokens == 100_000


# =============================================================================
# _is_quota_error heuristic
# =============================================================================


class TestIsQuotaError:
    """Tests for quota vs rate-limit distinction."""

    def _make_response(self, status_code=429, text="", headers=None):
        resp = MagicMock()
        resp.status_code = status_code
        resp.text = text
        resp.headers = headers or {}
        return resp

    def test_quota_keyword_in_body(self):
        """Body with 'quota' should be quota error."""
        client = ReasoningCapturingClient()
        resp = self._make_response(text='{"error": "insufficient_quota"}')
        assert client._is_quota_error(resp) is True

    def test_billing_keyword_in_body(self):
        """Body with 'billing' should be quota error."""
        client = ReasoningCapturingClient()
        resp = self._make_response(text='{"error": "billing issue"}')
        assert client._is_quota_error(resp) is True

    def test_short_retry_after_is_rate_limit(self):
        """Short retry-after without quota signal -> rate limit (not quota)."""
        client = ReasoningCapturingClient()
        resp = self._make_response(
            text='{"error": "too many requests"}', headers={"retry-after": "5"}
        )
        assert client._is_quota_error(resp) is False

    def test_no_signals_defaults_to_quota(self):
        """No retry-after, no quota signal -> conservatively assume quota."""
        client = ReasoningCapturingClient()
        resp = self._make_response(text='{"error": "unknown"}')
        assert client._is_quota_error(resp) is True


# =============================================================================
# Helper functions
# =============================================================================


class TestHelperFunctions:
    """Tests for module-level helper functions."""

    def test_is_debug_stream_default(self):
        """Should return False when env not set."""
        import os

        old = os.environ.pop("DEBUG_LLM_STREAM", None)
        try:
            assert _is_debug_stream() is False
        finally:
            if old is not None:
                os.environ["DEBUG_LLM_STREAM"] = old

    def test_get_debug_tail_default(self):
        """Should return 500 by default."""
        import os

        old = os.environ.pop("DEBUG_LLM_TAIL", None)
        try:
            assert _get_debug_tail_chars() == 500
        finally:
            if old is not None:
                os.environ["DEBUG_LLM_TAIL"] = old


# =============================================================================
# _extract_reasoning_from_response
# =============================================================================


class TestExtractReasoningFromResponse:
    """Tests for multi-provider reasoning extraction from chat completion responses."""

    def _make_response(self, message_fields: dict) -> dict:
        """Build a minimal chat completion response dict."""
        return {"choices": [{"message": message_fields}]}

    def test_deepseek_format(self):
        """Should extract reasoning_content (DeepSeek R1 format)."""
        data = self._make_response(
            {"reasoning_content": "Let me think step by step..."}
        )
        assert _extract_reasoning_from_response(data) == "Let me think step by step..."

    def test_openrouter_string_format(self):
        """Should extract reasoning (OpenRouter plain string format)."""
        data = self._make_response({"reasoning": "I need to consider..."})
        assert _extract_reasoning_from_response(data) == "I need to consider..."

    def test_openrouter_details_format(self):
        """Should extract and join reasoning_details (OpenRouter array format)."""
        data = self._make_response(
            {
                "reasoning_details": [
                    {"type": "thinking", "text": "First step."},
                    {"type": "thinking", "text": "Second step."},
                ]
            }
        )
        assert _extract_reasoning_from_response(data) == "First step.\nSecond step."

    def test_deepseek_priority_over_openrouter(self):
        """DeepSeek format should win when both are present."""
        data = self._make_response(
            {
                "reasoning_content": "DeepSeek thinking",
                "reasoning": "OpenRouter thinking",
            }
        )
        assert _extract_reasoning_from_response(data) == "DeepSeek thinking"

    def test_no_reasoning_returns_none(self):
        """Should return None when no reasoning fields are present."""
        data = self._make_response({"content": "Just a normal response"})
        assert _extract_reasoning_from_response(data) is None

    def test_empty_response(self):
        """Should return None for empty/minimal response."""
        assert _extract_reasoning_from_response({}) is None
        assert _extract_reasoning_from_response({"choices": []}) is None
        assert _extract_reasoning_from_response({"choices": [{}]}) is None

    def test_empty_reasoning_details(self):
        """Should return None for empty reasoning_details array."""
        data = self._make_response({"reasoning_details": []})
        assert _extract_reasoning_from_response(data) is None

    def test_malformed_reasoning_details(self):
        """Should skip non-dict items and items without text."""
        data = self._make_response(
            {
                "reasoning_details": [
                    "not a dict",
                    {"type": "thinking"},  # no text key
                    {"type": "thinking", "text": ""},  # empty text (falsy)
                    {"type": "thinking", "text": "Valid part"},
                ]
            }
        )
        assert _extract_reasoning_from_response(data) == "Valid part"

    def test_reasoning_details_nested_text(self):
        """Should extract readable nested text without storing encrypted blobs."""
        data = self._make_response(
            {
                "reasoning_details": [
                    {
                        "type": "reasoning",
                        "encrypted_content": "opaque",
                        "summary": [{"text": "Summarized step."}],
                    },
                    {
                        "type": "reasoning",
                        "content": [{"text": "Detailed step."}],
                    },
                ]
            }
        )
        assert (
            _extract_reasoning_from_response(data) == "Summarized step.\nDetailed step."
        )


# =============================================================================
# _dump_codex_raw_response
# =============================================================================


class TestDumpCodexRawResponse:
    """Tests for the codex raw-response diagnostic dumper."""

    def _make_request(self, body: bytes = b'{"model": "gpt-5"}'):
        req = MagicMock()
        req.url = "https://srw-codex-proxy/v1/responses"
        req.method = "POST"
        req.content = body
        return req

    def _make_response(self, body: bytes, status: int = 200):
        resp = MagicMock()
        resp.status_code = status
        resp.headers = {"content-type": "application/json"}
        resp.content = body
        return resp

    def test_dumps_valid_json_response(self, tmp_path, monkeypatch):
        """A valid JSON response is captured to disk as structured JSON."""
        monkeypatch.setenv("CODEX_RAW_DUMP_DIR", str(tmp_path))
        req = self._make_request()
        resp = self._make_response(b'{"output": [{"type": "function_call"}]}')

        _dump_codex_raw_response(req, resp)

        files = list(tmp_path.glob("codex-raw-*.json"))
        assert len(files) == 1
        import json as _json

        capture = _json.loads(files[0].read_text())
        assert capture["status_code"] == 200
        assert capture["url"] == "https://srw-codex-proxy/v1/responses"
        assert capture["method"] == "POST"
        assert capture["request_body"] == {"model": "gpt-5"}
        assert capture["response_body"] == {"output": [{"type": "function_call"}]}
        assert capture["response_body_size_bytes"] > 0

    def test_dumps_non_json_response_as_text(self, tmp_path, monkeypatch):
        """Non-JSON response bodies are stored as decoded text, not dropped."""
        monkeypatch.setenv("CODEX_RAW_DUMP_DIR", str(tmp_path))
        req = self._make_request(body=b"not-json-either")
        resp = self._make_response(b"<html>oops</html>", status=502)

        _dump_codex_raw_response(req, resp)

        files = list(tmp_path.glob("codex-raw-*.json"))
        assert len(files) == 1
        import json as _json

        capture = _json.loads(files[0].read_text())
        assert capture["status_code"] == 502
        assert capture["request_body"] == "not-json-either"
        assert capture["response_body"] == "<html>oops</html>"

    def test_failure_is_silent(self, tmp_path, monkeypatch):
        """Errors during capture must not propagate."""
        # Point at a path that cannot be created (a file used as a directory)
        bad = tmp_path / "blocker"
        bad.write_text("i am a file, not a dir")
        monkeypatch.setenv("CODEX_RAW_DUMP_DIR", str(bad))

        req = self._make_request()
        resp = self._make_response(b"{}")
        # Must not raise
        _dump_codex_raw_response(req, resp)


# =============================================================================
# _extract_reasoning_from_delta
# =============================================================================


class TestExtractReasoningFromDelta:
    """Per-chunk delta reasoning extraction (Chat Completions SSE)."""

    def test_none_input(self):
        assert _extract_reasoning_from_delta(None) is None  # type: ignore[arg-type]

    def test_empty_dict(self):
        assert _extract_reasoning_from_delta({}) is None

    def test_reasoning_content_field(self):
        assert _extract_reasoning_from_delta({"reasoning_content": "hi"}) == "hi"

    def test_openrouter_reasoning_field(self):
        assert _extract_reasoning_from_delta({"reasoning": "ok"}) == "ok"

    def test_reasoning_content_wins_over_reasoning(self):
        d = {"reasoning_content": "primary", "reasoning": "secondary"}
        assert _extract_reasoning_from_delta(d) == "primary"

    def test_empty_string_returns_none(self):
        assert _extract_reasoning_from_delta({"reasoning_content": ""}) is None

    def test_non_string_value_returns_none(self):
        assert _extract_reasoning_from_delta({"reasoning_content": 42}) is None

    def test_openrouter_reasoning_details_delta(self):
        delta = {
            "reasoning_details": [
                {"type": "thinking", "text": "First chunk."},
                {"type": "thinking", "text": "Second chunk."},
            ]
        }
        assert _extract_reasoning_from_delta(delta) == "First chunk.\nSecond chunk."

    def test_openrouter_reasoning_details_delta_nested_text(self):
        delta = {
            "reasoning_details": [
                {
                    "type": "reasoning",
                    "encrypted_content": "opaque",
                    "summary": [{"text": "Visible summary."}],
                }
            ]
        }
        assert _extract_reasoning_from_delta(delta) == "Visible summary."


# =============================================================================
# _SSEReasoningTap + _install_streaming_reasoning_tap
# =============================================================================


def _sse_lines(events: list[dict]) -> bytes:
    """Encode chat-completion-style chunks as SSE bytes."""
    out: list[bytes] = []
    for ev in events:
        out.append(b"data: " + json.dumps(ev).encode("utf-8") + b"\n\n")
    out.append(b"data: [DONE]\n\n")
    return b"".join(out)


class _FakeResponse:
    """Minimal httpx.Response stand-in with iter_bytes/aiter_bytes."""

    def __init__(self, body: bytes, chunk_size: int = 32):
        self._body = body
        self._chunk_size = chunk_size

    def iter_bytes(self, _size=None):
        for i in range(0, len(self._body), self._chunk_size):
            yield self._body[i : i + self._chunk_size]

    async def aiter_bytes(self, _size=None):
        for i in range(0, len(self._body), self._chunk_size):
            yield self._body[i : i + self._chunk_size]


class TestSSEReasoningTap:
    """Tap parses reasoning_content from SSE while forwarding bytes unchanged."""

    def test_sync_extracts_reasoning_content(self):
        body = _sse_lines(
            [
                {"choices": [{"delta": {"reasoning_content": "**Think**"}}]},
                {"choices": [{"delta": {"reasoning_content": "ing..."}}]},
                {"choices": [{"delta": {"content": "Done."}}]},
            ]
        )
        resp = _FakeResponse(body, chunk_size=16)
        tap = _SSEReasoningTap(resp)

        forwarded = b"".join(tap.iter_bytes())

        assert forwarded == body, "bytes must pass through unchanged"
        assert tap.reasoning_content == "**Think**ing..."

    def test_sync_returns_none_when_no_reasoning(self):
        body = _sse_lines(
            [
                {"choices": [{"delta": {"content": "Hello"}}]},
                {"choices": [{"delta": {"content": " world"}}]},
            ]
        )
        tap = _SSEReasoningTap(_FakeResponse(body))
        for _ in tap.iter_bytes():
            pass
        assert tap.reasoning_content is None

    def test_sync_handles_split_lines_across_chunks(self):
        """SSE chunks may split a line mid-payload — tap must buffer."""
        body = _sse_lines(
            [{"choices": [{"delta": {"reasoning_content": "abcdefghij"}}]}]
        )
        # Force a tiny chunk_size so a single SSE line spans many chunks
        tap = _SSEReasoningTap(_FakeResponse(body, chunk_size=4))
        for _ in tap.iter_bytes():
            pass
        assert tap.reasoning_content == "abcdefghij"

    def test_openrouter_reasoning_field(self):
        body = _sse_lines(
            [
                {"choices": [{"delta": {"reasoning": "open"}}]},
                {"choices": [{"delta": {"reasoning": "router"}}]},
            ]
        )
        tap = _SSEReasoningTap(_FakeResponse(body))
        for _ in tap.iter_bytes():
            pass
        assert tap.reasoning_content == "openrouter"

    def test_openrouter_reasoning_details_field(self):
        body = _sse_lines(
            [
                {
                    "choices": [
                        {
                            "delta": {
                                "reasoning_details": [
                                    {"type": "thinking", "text": "mini"}
                                ]
                            }
                        }
                    ]
                },
                {
                    "choices": [
                        {
                            "delta": {
                                "reasoning_details": [
                                    {"type": "thinking", "text": "max"}
                                ]
                            }
                        }
                    ]
                },
            ]
        )
        tap = _SSEReasoningTap(_FakeResponse(body))
        for _ in tap.iter_bytes():
            pass
        assert tap.reasoning_content == "minimax"

    def test_malformed_json_is_skipped(self):
        body = (
            b"data: {not json}\n\n"
            b"data: "
            + json.dumps(
                {"choices": [{"delta": {"reasoning_content": "saved"}}]}
            ).encode("utf-8")
            + b"\n\n"
            b"data: [DONE]\n\n"
        )
        tap = _SSEReasoningTap(_FakeResponse(body))
        for _ in tap.iter_bytes():
            pass
        assert tap.reasoning_content == "saved"

    def test_done_marker_ignored(self):
        body = b"data: [DONE]\n\n"
        tap = _SSEReasoningTap(_FakeResponse(body))
        for _ in tap.iter_bytes():
            pass
        assert tap.reasoning_content is None

    def test_trailing_line_without_newline_is_flushed(self):
        """Some servers omit the trailing \\n before closing the stream."""
        body = b"data: " + json.dumps(
            {"choices": [{"delta": {"reasoning_content": "tail"}}]}
        ).encode("utf-8")
        tap = _SSEReasoningTap(_FakeResponse(body))
        for _ in tap.iter_bytes():
            pass
        assert tap.reasoning_content == "tail"

    @pytest.mark.asyncio
    async def test_async_extracts_reasoning_content(self):
        body = _sse_lines(
            [
                {"choices": [{"delta": {"reasoning_content": "First "}}]},
                {"choices": [{"delta": {"reasoning_content": "second"}}]},
            ]
        )
        resp = _FakeResponse(body)
        tap = _SSEReasoningTap(resp)

        forwarded = b""
        async for chunk in tap.aiter_bytes():
            forwarded += chunk

        assert forwarded == body
        assert tap.reasoning_content == "First second"


class TestInstallStreamingReasoningTap:
    """Tap installation replaces aiter_bytes / iter_bytes on the response."""

    def test_replaces_iter_methods(self):
        body = _sse_lines([{"choices": [{"delta": {"reasoning_content": "hello"}}]}])
        resp = _FakeResponse(body)
        original_iter = resp.iter_bytes
        tap = _install_streaming_reasoning_tap(resp)

        assert resp.iter_bytes is not original_iter
        # The replaced method comes from the tap; consuming it captures
        # reasoning while forwarding bytes unchanged.
        for _ in resp.iter_bytes():
            pass
        assert tap.reasoning_content == "hello"


# =============================================================================
# Pinned endpoint certificate: both capture clients trust only the pin
# =============================================================================


class TestPinnedCertificate:
    def _llm(self, base_url, pin=None, **kwargs):
        return ReasoningChatOpenAI(
            model="m1",
            api_key="sk-test",
            base_url=base_url,
            max_retries=0,
            timeout=20,
            tls_pin=pin,
            **kwargs,
        )

    def test_both_capture_clients_use_the_pinned_transport(self, tls_certs):
        pin = TlsPin.from_pem(tls_certs.leaf.read_text())
        llm = self._llm("https://127.0.0.1:9/v1", pin)
        sync_transport = llm._reasoning_client._transport
        async_transport = llm._async_reasoning_client._transport
        assert type(sync_transport) is PinnedTransport and sync_transport._pin == pin
        assert type(async_transport) is AsyncPinnedTransport
        assert async_transport._pin == pin
        # The SDK sends through exactly these two clients.
        assert llm.http_client is llm._reasoning_client
        assert llm.http_async_client is llm._async_reasoning_client

    def test_no_pin_keeps_the_default_transports(self):
        llm = self._llm("https://127.0.0.1:9/v1")
        assert type(llm._reasoning_client._transport) is httpx.HTTPTransport
        assert type(llm._async_reasoning_client._transport) is httpx.AsyncHTTPTransport

    @pytest.mark.asyncio
    async def test_a_pinned_call_is_recorded_as_it_went_out(self, tls_certs):
        pin = TlsPin.from_pem(tls_certs.leaf.read_text())
        with tls_server(tls_certs.leaf, tls_certs.leaf_key) as server:
            llm = self._llm(server.url, pin)
            with record_wire_attempts() as mine:
                reply = await llm.ainvoke("hi")
        assert reply.content == "ok"
        assert [a["status"] for a in mine] == [200]
        assert mine[0]["path"] == "/v1/chat/completions"

    @pytest.mark.asyncio
    async def test_a_changed_certificate_is_recorded_and_nothing_sent(self, tls_certs):
        pin = TlsPin.from_pem(tls_certs.leaf.read_text())
        with tls_server(tls_certs.swap, tls_certs.swap_key) as server:
            llm = self._llm(server.url, pin)
            with record_wire_attempts() as mine:
                with pytest.raises(Exception) as raised:
                    await llm.ainvoke("hi")
        assert caused_by(raised.value, CertificateChangedError)
        assert [a["error"] for a in mine] == ["CertificateChangedError"]
        assert "TLS certificate changed" in mine[0]["error_message"]
        assert server.requests == []

    def test_the_sync_client_is_pinned_too(self, tls_certs):
        pin = TlsPin.from_pem(tls_certs.leaf.read_text())
        with tls_server(tls_certs.leaf, tls_certs.leaf_key) as server:
            assert self._llm(server.url, pin).invoke("hi").content == "ok"
        with tls_server(tls_certs.swap, tls_certs.swap_key) as server:
            with pytest.raises(Exception) as raised:
                self._llm(server.url, pin).invoke("hi")
        assert caused_by(raised.value, CertificateChangedError)
        assert server.requests == []
