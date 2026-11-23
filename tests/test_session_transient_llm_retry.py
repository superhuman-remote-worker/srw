"""Sessions must retry transient LLM errors instead of surfacing them raw.

Incident 2026-07-25 (session ``b1758f38``, gpt-5.6-sol via ``srw-codex-proxy``):
the provider returned HTTP 200, began streaming, then emitted an ``error`` event
inside the SSE body ~8s in. ``openai/_streaming.py`` turns that into a bare
``openai.APIError`` — the *base* class, so it carries no ``status_code`` and the
SDK's own ``max_retries`` no longer applies (the response body had already
started).

The worker path classifies that exact exception ``transient`` via
``_classify_llm_error`` and retries it with backoff. The session path had no
classification and no retry at all: ``run_persistent_loop`` caught every
exception and pushed the raw provider string at the user. These tests pin the
session path to the same classifier.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from langchain_core.messages import AIMessage
from openai import APIError, APIStatusError, AuthenticationError

from agent.persistent_graph import PersistentLoopCallbacks, run_persistent_loop

_MIDSTREAM_MSG = (
    "An error occurred while processing your request. You can retry your "
    "request, or contact us through our help center at help.openai.com if the "
    "error persists. Please include the request ID "
    "089059c1-140b-4928-addf-3a9b849d96a2 in your message."
)


def _midstream_api_error() -> APIError:
    """The exact exception ``openai/_streaming.py`` raises on an SSE error event."""
    return APIError(
        message=_MIDSTREAM_MSG,
        request=httpx.Request("POST", "http://srw-codex-proxy:8317/v1/responses"),
        body={"message": _MIDSTREAM_MSG, "type": "server_error"},
    )


def _auth_error(url: str) -> AuthenticationError:
    """A 401 from ``url`` — the host decides permanent vs. token-refresh blip."""
    return AuthenticationError(
        "Invalid API key",
        response=httpx.Response(401, request=httpx.Request("POST", url)),
        body=None,
    )


def _context_overflow_413() -> APIStatusError:
    """The synthetic 413 `reasoning_chat` raises for a pre-flight context overflow.

    Deliberately non-retryable by design (session_silent_failure_audit.md #3):
    the request is deterministically too big, so every retry re-sends the same
    oversized body.
    """
    body = {
        "error": {
            "message": "request too large",
            "type": "invalid_request_error",
            "code": "context_overflow",
            "token_count": 500_000,
            "limit": 400_000,
        }
    }
    return APIStatusError(
        "request too large",
        response=httpx.Response(
            413,
            request=httpx.Request("POST", "http://srw-codex-proxy:8317/v1/responses"),
            json=body,
        ),
        body=body,
    )


def _make_callbacks(**overrides) -> PersistentLoopCallbacks:
    defaults = dict(
        get_user_input=AsyncMock(return_value="hello"),
        on_token=AsyncMock(),
        on_thinking=AsyncMock(),
        on_tool_start=AsyncMock(),
        on_tool_result=AsyncMock(),
        permission_check=AsyncMock(return_value=True),
        on_turn_start=AsyncMock(),
        on_turn_complete=AsyncMock(),
        on_error=AsyncMock(),
        check_interrupt=MagicMock(return_value=False),
        on_vm_upgrade_needed=None,
    )
    defaults.update(overrides)
    return PersistentLoopCallbacks(**defaults)


def _loop_config() -> MagicMock:
    cfg = MagicMock()
    cfg.llm.timeout = 600
    cfg.context_management.max_summary_length = 10000
    return cfg


def _flaky_llm(errors: list[Exception], final: AIMessage, pre_tokens: str = ""):
    """LLM whose stream raises each queued error before finally yielding ``final``.

    ``pre_tokens`` streams a content chunk *before* raising, reproducing a
    failure that already painted tokens in the UI.
    """
    llm = AsyncMock()
    llm.reasoning = None
    attempts = {"n": 0}

    async def _astream(messages, **kw):
        attempts["n"] += 1
        if errors:
            err = errors.pop(0)
            if pre_tokens:
                yield AIMessage(content=pre_tokens)
            raise err
        yield final

    llm.astream = _astream
    llm.ainvoke = AsyncMock(return_value=final)
    return llm, attempts


async def _run_one_turn(llm, callbacks) -> None:
    """Drive ``run_persistent_loop`` through exactly one user turn."""
    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[],
        context_manager=AsyncMock(
            ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
        ),
        config=_loop_config(),
        system_prompt="sys",
        callbacks=callbacks,
        messages=[],
    )


def _single_turn_input():
    calls = {"n": 0}

    async def _input():
        calls["n"] += 1
        if calls["n"] == 1:
            return "do it"
        raise asyncio.CancelledError

    return _input


@pytest.mark.asyncio
async def test_transient_midstream_api_error_is_retried(monkeypatch):
    """A mid-stream APIError retries and the user sees the answer, not the error."""
    monkeypatch.setattr("agent.persistent_graph._SESSION_LLM_RETRY_BASE_DELAY", 0.0)

    on_error = AsyncMock()
    on_token = AsyncMock()
    llm, attempts = _flaky_llm(
        [_midstream_api_error()], AIMessage(content="here is your answer")
    )

    await _run_one_turn(
        llm,
        _make_callbacks(
            get_user_input=_single_turn_input(),
            on_error=on_error,
            on_token=on_token,
        ),
    )

    assert attempts["n"] == 2, "transient mid-stream error was not retried"
    assert not on_error.called, (
        f"transient error surfaced to the user: {on_error.call_args}"
    )
    streamed = "".join(
        c.args[0]
        for c in on_token.call_args_list
        if c.args and isinstance(c.args[0], str)
    )
    assert "here is your answer" in streamed


@pytest.mark.asyncio
async def test_permanent_auth_error_is_not_retried(monkeypatch):
    """Fail-fast verdicts keep failing fast — no retry budget burned on a bad key."""
    monkeypatch.setattr("agent.persistent_graph._SESSION_LLM_RETRY_BASE_DELAY", 0.0)

    # Deliberately NOT via the codex proxy: a 401 straight from the provider is
    # a genuinely bad key. (A proxy 401 is a token-refresh blip — see the test
    # below.) The classifier draws that line and sessions now inherit it.
    auth_err = _auth_error("https://api.openai.com/v1/responses")
    on_error = AsyncMock()
    llm, attempts = _flaky_llm([auth_err], AIMessage(content="unreachable"))

    await _run_one_turn(
        llm,
        _make_callbacks(get_user_input=_single_turn_input(), on_error=on_error),
    )

    assert attempts["n"] == 1, "a permanent auth error must not be retried"
    assert on_error.called, "permanent error should surface to the user"


@pytest.mark.asyncio
async def test_codex_proxy_401_is_retried_like_worker_jobs(monkeypatch):
    """Sessions inherit the worker verdict that a proxy 401 is a refresh blip.

    `_classify_llm_error` calls this `auth_unavailable`, not `permanent`, because
    the Codex/CLIProxyAPI token may be mid-refresh. Worker jobs have retried it
    since the 2026-06-22 incident; sessions failed the turn outright.
    """
    monkeypatch.setattr("agent.persistent_graph._SESSION_LLM_RETRY_BASE_DELAY", 0.0)

    on_error = AsyncMock()
    llm, attempts = _flaky_llm(
        [_auth_error("http://srw-codex-proxy:8317/v1/responses")],
        AIMessage(content="recovered after token refresh"),
    )

    await _run_one_turn(
        llm,
        _make_callbacks(get_user_input=_single_turn_input(), on_error=on_error),
    )

    assert attempts["n"] == 2, "a codex-proxy 401 should be retried, not fatal"
    assert not on_error.called


@pytest.mark.asyncio
async def test_transient_error_after_tokens_streamed_is_not_retried(monkeypatch):
    """Once tokens reached the client, a restart would duplicate them — surface instead."""
    monkeypatch.setattr("agent.persistent_graph._SESSION_LLM_RETRY_BASE_DELAY", 0.0)

    on_error = AsyncMock()
    on_token = AsyncMock()
    llm, attempts = _flaky_llm(
        [_midstream_api_error()],
        AIMessage(content="second attempt text"),
        pre_tokens="partial answer already shown",
    )

    await _run_one_turn(
        llm,
        _make_callbacks(
            get_user_input=_single_turn_input(),
            on_error=on_error,
            on_token=on_token,
        ),
    )

    assert attempts["n"] == 1, (
        "must not restart a stream whose tokens already reached the client"
    )
    streamed = "".join(
        c.args[0]
        for c in on_token.call_args_list
        if c.args and isinstance(c.args[0], str)
    )
    assert streamed.count("partial answer already shown") == 1, "tokens were duplicated"
    assert on_error.called


@pytest.mark.asyncio
async def test_context_overflow_is_not_retried(monkeypatch):
    """A deterministic overflow must not be retried — every attempt re-sends it.

    The classifier's catch-all verdict is `transient`, so the session retry has
    to exclude context overflow explicitly or it re-creates the retry storm that
    session_silent_failure_audit.md #3 removed.
    """
    monkeypatch.setattr("agent.persistent_graph._SESSION_LLM_RETRY_BASE_DELAY", 0.0)

    on_error = AsyncMock()
    llm, attempts = _flaky_llm(
        [_context_overflow_413()], AIMessage(content="unreachable")
    )

    await _run_one_turn(
        llm,
        _make_callbacks(get_user_input=_single_turn_input(), on_error=on_error),
    )

    assert attempts["n"] == 1, "an oversized request must not be re-sent"
    # NOTE: the copy the user gets for this case is the provider's bare
    # "request too large" — `_user_facing_turn_error` looks for the literal
    # "context_overflow" in `str(e)`, but the synthetic 413 keeps that code in
    # the *body*, not the message. Pre-existing and orthogonal to the retry
    # decision pinned here; asserting only what this change guarantees.
    assert on_error.called


@pytest.mark.asyncio
async def test_retries_are_bounded_and_then_surface(monkeypatch):
    """A provider that keeps failing exhausts the budget instead of looping forever."""
    monkeypatch.setattr("agent.persistent_graph._SESSION_LLM_RETRY_BASE_DELAY", 0.0)

    on_error = AsyncMock()
    llm, attempts = _flaky_llm(
        [_midstream_api_error() for _ in range(10)], AIMessage(content="never reached")
    )

    await _run_one_turn(
        llm,
        _make_callbacks(get_user_input=_single_turn_input(), on_error=on_error),
    )

    assert attempts["n"] == 3, "retry budget should be _SESSION_LLM_MAX_ATTEMPTS"
    assert on_error.called, "exhausted retries must still surface to the user"


@pytest.mark.asyncio
async def test_changed_pinned_certificate_is_not_retried_and_says_why(monkeypatch):
    """The session shows the pin sentence whole after exactly one attempt."""
    from openai import APIConnectionError

    from shared.tls_pinning import CertificateChangedError

    monkeypatch.setattr("agent.persistent_graph._SESSION_LLM_RETRY_BASE_DELAY", 0.0)
    request = httpx.Request("POST", "https://llm.example/v1/chat/completions")
    pin_error = CertificateChangedError(
        "llm.example", 443, "AA:BB:CC", "DD:EE:FF", request=request
    )
    sdk_error = APIConnectionError(request=request)
    sdk_error.__cause__ = pin_error

    on_error = AsyncMock()
    llm, attempts = _flaky_llm([sdk_error], AIMessage(content="unreachable"))

    await _run_one_turn(
        llm,
        _make_callbacks(get_user_input=_single_turn_input(), on_error=on_error),
    )

    assert attempts["n"] == 1, "a pin failure must not be retried"
    assert on_error.call_args.args[0] == str(pin_error)
