"""A deterministic provider rejection is sent once and fails on every layer.

knowledge-base/knowledge/issues/deterministic_provider_rejection_retried_unchanged.md:
pilot ``9792db96`` (Contributor ``max`` -> HTTP 400 ``invalid_request_error``)
replayed the identical request in two pause cycles of six attempts, and
stateless acceptance ``c1a63bec`` (``400 invalid model ID``) was released and
re-claimed across five authorized bundles.

Root cause: the openai SDK -- the transport of every OpenAI-compatible route
(OpenAI, OpenRouter, Mistral, the subscription proxy, self-hosted endpoints)
-- stores the response envelope's ``error`` member already UNWRAPPED on
``exc.body``, while ``_classify_llm_error`` only read ``body["error"]`` (the
anthropic SDK's shape). Every OpenAI-compatible 400 therefore fell through to
the "400 without a parseable body -> transient" default.

The fixtures here are raised by the REAL provider SDKs against an offline
``httpx.MockTransport``, so they cannot drift from the wire shape the way the
duck-typed fixtures in test_graph_helpers.py (all anthropic-envelope shaped)
did. The worker cases drive the production ``ReasoningChatOpenAI`` through the
real execute node and hand its actual result to both outer layers: the
orchestrator's status authority (pinned lane) and the stateless claim driver.
"""

from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from typing import Any, Callable
from unittest.mock import patch

import anthropic
import httpx
import openai
import pytest
import yaml

import shared.runtime.llm.reasoning_chat as reasoning_chat
from agent.api.turn_executor import StatelessTurnExecutor
from agent.core.context import ToolRetryManager
from agent.persistent_graph import _is_retryable_llm_error
from orchestrator.services.completion import (
    determine_job_status,
    llm_outage_fingerprint,
)
from shared.runtime.core import loader
from shared.runtime.core.llm_retry import (
    RetryPolicy,
    _classify_llm_error,
    invoke_with_retry,
)
from tests.test_execute_prepared_layout import (
    TOOL_SCHEMAS,
    _make_node,
    _run,
    _state,
    env as env,
)
from tests.test_stateless_worker_runtime import (
    _claim,
    _install,
    worker_runtime as worker_runtime,
)

CONTRIBUTOR = "meta/muse-spark-1.3-contributor"
FIXTURE_KEY = "fixture-key-must-never-appear-in-errors"
BASE_URL = "https://provider.invalid/v1"

# OpenAI-compatible unsupported-value rejection -- the class of pilot 9792db96
# (its exact body was not preserved; this is the documented OpenAI shape).
UNSUPPORTED_EFFORT = {
    "error": {
        "message": (
            "Unsupported value: 'reasoning_effort' does not support 'max' with "
            "this model. Supported values are: 'minimal', 'low', 'medium', "
            "'high', and 'xhigh'."
        ),
        "type": "invalid_request_error",
        "param": "reasoning_effort",
        "code": "unsupported_value",
    }
}
# OpenRouter's own validation: untyped, integer ``code`` (stateless c1a63bec
# reported "400 invalid model ID"; its exact body was not preserved either).
OPENROUTER_INVALID_MODEL = {
    "error": {"message": f"{CONTRIBUTOR} is not a valid model ID", "code": 400}
}
# The subscription proxy (CLIProxyAPI) answers every chat request with this
# between "API server started" and "full client load complete" -- verbatim
# from a v7.2.129 request log. The chart runs it Recreate with no readiness
# gate, so every restart, image bump or credential reload opens this window.
PROXY_URL = "http://srw-codex-proxy:8317/v1"
PROXY_NOT_LOADED = {
    "error": {
        "message": "unknown provider for model claude-opus-5",
        "type": "invalid_request_error",
        "code": "model_not_found",
        "param": "model",
    }
}
PROVIDER_OUTAGE = {
    "error": {"message": "The server is overloaded", "type": "server_error"}
}
EDGE_400 = (
    "<html><head><title>400 Bad Request</title></head><body><center>"
    "<h1>400 Bad Request</h1></center><hr><center>nginx</center></body></html>"
)


# ---------------------------------------------------------------------------
# Real-SDK error factories (offline)
# ---------------------------------------------------------------------------


def _through_openai(
    handler: Callable[[httpx.Request], Any], *, stream=False, base_url=BASE_URL
):
    """The exception the real openai SDK raises for one canned exchange."""
    client = openai.OpenAI(
        api_key=FIXTURE_KEY,
        base_url=base_url,
        max_retries=0,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    try:
        result = client.chat.completions.create(
            model="fixture-model",
            messages=[{"role": "user", "content": "hi"}],
            stream=stream,
        )
        for _ in result if stream else ():
            pass
    except Exception as exc:  # noqa: BLE001 - the exception IS the fixture
        return exc
    finally:
        client.close()
    raise AssertionError("fixture exchange did not raise")


def _openai_status(
    status: int, payload: Any = None, *, text: str | None = None, base_url=BASE_URL
):
    def handler(request):
        if text is not None:
            return httpx.Response(status, text=text)
        return httpx.Response(status, json=payload)

    return _through_openai(handler, base_url=base_url)


def _openai_transport(error_cls):
    def handler(request):
        raise error_cls("fixture transport failure", request=request)

    return _through_openai(handler)


class _InterruptedSSE(httpx.SyncByteStream):
    """One chunk, then the connection drops mid-body."""

    def __iter__(self):
        chunk = {
            "id": "c",
            "object": "chat.completion.chunk",
            "created": 0,
            "model": "fixture-model",
            "choices": [{"index": 0, "delta": {"content": "par"}}],
        }
        yield f"data: {json.dumps(chunk)}\n\n".encode()
        raise httpx.RemoteProtocolError(
            "peer closed connection without sending complete message body "
            "(incomplete chunked read)"
        )


def _openai_interrupted_stream():
    return _through_openai(
        lambda request: httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=_InterruptedSSE(),
        ),
        stream=True,
    )


def _openai_stream_error_event(error: dict):
    body = f"data: {json.dumps({'error': error})}\n\n".encode()
    return _through_openai(
        lambda request: httpx.Response(
            200, headers={"content-type": "text/event-stream"}, content=body
        ),
        stream=True,
    )


def _anthropic_http():
    """The HTTP package this anthropic SDK is built on.

    anthropic 1.x runs on ``httpx2`` and rejects an ``httpx.Client``
    ("Invalid `http_client` argument"); 0.x runs on ``httpx``. The lock pins
    1.x while an older local venv may still carry 0.x, so the fixture follows
    the installed SDK instead of assuming one.
    """
    if int(anthropic.__version__.split(".", 1)[0]) >= 1:
        import httpx2

        return httpx2
    return httpx


def _anthropic_status(status: int, payload: Any):
    http = _anthropic_http()
    client = anthropic.Anthropic(
        api_key=FIXTURE_KEY,
        base_url="https://provider.invalid",
        max_retries=0,
        http_client=http.Client(
            transport=http.MockTransport(
                lambda request: http.Response(status, json=payload)
            )
        ),
    )
    try:
        client.messages.create(
            model="fixture-model",
            max_tokens=8,
            messages=[{"role": "user", "content": "hi"}],
        )
    except Exception as exc:  # noqa: BLE001
        return exc
    finally:
        client.close()
    raise AssertionError("fixture exchange did not raise")


# ---------------------------------------------------------------------------
# 1. One verdict per real provider shape
# ---------------------------------------------------------------------------

_CASES = [
    # --- deterministic: an identical request fails identically ------------
    (
        "unsupported reasoning effort",
        lambda: _openai_status(400, UNSUPPORTED_EFFORT),
        "permanent",
    ),
    (
        "rejection label carried in code only",
        lambda: _openai_status(
            400,
            {
                "error": {
                    "message": "reasoning_effort 'max' is not supported for this model",
                    "code": "invalid_request_error",
                    "param": "reasoning_effort",
                }
            },
        ),
        "permanent",
    ),
    (
        "openrouter invalid model id (untyped, int code)",
        lambda: _openai_status(400, OPENROUTER_INVALID_MODEL),
        "permanent",
    ),
    (
        "proxy-shaped model_not_found from an ordinary endpoint",
        lambda: _openai_status(400, PROXY_NOT_LOADED),
        "permanent",
    ),
    (
        "mistral top-level error object",
        lambda: _openai_status(
            400,
            {
                "object": "error",
                "message": "Invalid model: muse-spark-9",
                "type": "invalid_model",
                "param": None,
                "code": "1500",
            },
        ),
        "permanent",
    ),
    (
        "minimax bad_request_error over the openai transport",
        lambda: _openai_status(
            400,
            {
                "type": "error",
                "error": {
                    "type": "bad_request_error",
                    "message": "invalid params, invalid function arguments json string",
                    "http_code": "400",
                },
            },
        ),
        "permanent",
    ),
    (
        "subscription proxy validator rejection",
        lambda: _openai_status(
            400,
            {
                "error": {
                    "message": "thinking: validation failed: unsupported effort 'bogus'",
                    "type": "invalid_request_error",
                }
            },
        ),
        "permanent",
    ),
    (
        "provider-side context overflow",
        lambda: _openai_status(
            400,
            {
                "error": {
                    "message": (
                        "This model's maximum context length is 128000 tokens. "
                        "However, your messages resulted in 131072 tokens."
                    ),
                    "type": "invalid_request_error",
                    "param": "messages",
                    "code": "context_length_exceeded",
                }
            },
        ),
        "permanent",
    ),
    (
        "anthropic invalid_request_error envelope",
        lambda: _anthropic_status(
            400,
            {
                "type": "error",
                "error": {
                    "type": "invalid_request_error",
                    "message": "thinking.budget_tokens: must be >= 1024",
                },
            },
        ),
        "permanent",
    ),
    (
        "openai 404 model_not_found",
        lambda: _openai_status(
            404,
            {
                "error": {
                    "message": "The model `muse-spark-9` does not exist.",
                    "type": "invalid_request_error",
                    "param": None,
                    "code": "model_not_found",
                }
            },
        ),
        "permanent",
    ),
    # --- 400s that stay retryable -----------------------------------------
    (
        "rate limit disguised as a 400",
        lambda: _openai_status(
            400,
            {
                "error": {
                    "message": "Rate limit reached for requests",
                    "type": "invalid_request_error",
                    "code": "rate_limit_exceeded",
                }
            },
        ),
        "rate_limit",
    ),
    (
        "groq tool_use_failed",
        lambda: _openai_status(
            400,
            {
                "error": {
                    "message": "Failed to call a function.",
                    "type": "invalid_request_error",
                    "code": "tool_use_failed",
                    "failed_generation": '{"name": "write_file"',
                }
            },
        ),
        "transient",
    ),
    (
        "stream disconnect mislabeled as a 400",
        lambda: _openai_status(
            400,
            {
                "error": {
                    "message": (
                        "stream error: stream disconnected before completion: "
                        "stream closed before response.completed"
                    ),
                    "type": "invalid_request_error",
                }
            },
        ),
        "transient",
    ),
    (
        "openrouter untyped upstream passthrough",
        lambda: _openai_status(
            400,
            {
                "error": {
                    "message": "Provider returned error",
                    "code": 400,
                    "metadata": {"provider_name": "Upstream", "raw": "no"},
                }
            },
        ),
        "transient",
    ),
    (
        "400 plain-text body",
        lambda: _openai_status(400, text="Bad Request"),
        "transient",
    ),
    (
        "subscription proxy restart window",
        lambda: _openai_status(400, PROXY_NOT_LOADED, base_url=PROXY_URL),
        "transient",
    ),
    (
        "subscription proxy restart window, localhost default port",
        lambda: _openai_status(
            400, PROXY_NOT_LOADED, base_url="http://localhost:8317/v1"
        ),
        "transient",
    ),
    ("400 edge html page", lambda: _openai_status(400, text=EDGE_400), "transient"),
    ("400 empty json object", lambda: _openai_status(400, {}), "transient"),
    (
        "400 error member is a string",
        lambda: _openai_status(400, {"error": "boom"}),
        "transient",
    ),
    (
        "400 malformed field types",
        lambda: _openai_status(
            400, {"error": {"type": None, "code": ["x"], "message": 42}}
        ),
        "transient",
    ),
    (
        "400 top-level json list",
        lambda: _openai_status(
            400, [{"error": {"code": 400, "status": "INVALID_ARGUMENT"}}]
        ),
        "transient",
    ),
    # --- transport, timeouts, streams, 429, 5xx ---------------------------
    (
        "408 stream disconnect",
        lambda: _openai_status(
            408,
            {
                "error": {
                    "message": "stream disconnected before completion",
                    "type": "invalid_request_error",
                }
            },
        ),
        "transient",
    ),
    ("client read timeout", lambda: _openai_transport(httpx.ReadTimeout), "transient"),
    ("connection refused", lambda: _openai_transport(httpx.ConnectError), "transient"),
    ("interrupted stream", _openai_interrupted_stream, "transient"),
    (
        "mid-stream error event with a rejection label",
        lambda: _openai_stream_error_event(
            {"message": "upstream went away", "type": "invalid_request_error"}
        ),
        "transient",
    ),
    (
        "429 rate limit",
        lambda: _openai_status(
            429,
            {
                "error": {
                    "message": "Rate limit reached",
                    "type": "requests",
                    "code": "rate_limit_exceeded",
                }
            },
        ),
        "rate_limit",
    ),
    (
        "429 openrouter integer code",
        lambda: _openai_status(
            429, {"error": {"message": "Rate limit exceeded", "code": 429}}
        ),
        "rate_limit",
    ),
    (
        "429 insufficient_quota",
        lambda: _openai_status(
            429,
            {
                "error": {
                    "message": "You exceeded your current quota",
                    "type": "insufficient_quota",
                    "code": "insufficient_quota",
                }
            },
        ),
        "quota_exhausted",
    ),
    (
        "anthropic 429 with an integer code",
        lambda: _anthropic_status(
            429,
            {
                "type": "error",
                "error": {
                    "type": "rate_limit_error",
                    "code": 429,
                    "message": "slow down",
                },
            },
        ),
        "rate_limit",
    ),
    (
        "anthropic 401 with an integer code",
        lambda: _anthropic_status(
            401,
            {
                "type": "error",
                "error": {
                    "type": "authentication_error",
                    "code": 401,
                    "message": "invalid x-api-key",
                },
            },
        ),
        "permanent",
    ),
    ("500 server error", lambda: _openai_status(500, PROVIDER_OUTAGE), "transient"),
    (
        "502 edge page",
        lambda: _openai_status(502, text="<html>502 Bad Gateway</html>"),
        "transient",
    ),
    (
        "503 subscription-proxy credential bench",
        lambda: _openai_status(
            503,
            {
                "error": {
                    "message": "auth_unavailable: no auth available (providers=claude)",
                    "type": "server_error",
                    "code": "auth_unavailable",
                }
            },
        ),
        "transient",
    ),
    (
        "anthropic 529 overloaded",
        lambda: _anthropic_status(
            529,
            {
                "type": "error",
                "error": {"type": "overloaded_error", "message": "Overloaded"},
            },
        ),
        "transient",
    ),
]


@pytest.mark.parametrize(
    ("make_error", "expected"),
    [pytest.param(make, expected, id=name) for name, make, expected in _CASES],
)
def test_real_sdk_error_shapes_get_one_calibrated_verdict(make_error, expected):
    assert _classify_llm_error(make_error()) == expected


def test_session_turns_share_the_worker_verdict():
    """One verdict product-wide: a session turn does not replay it either."""
    assert not _is_retryable_llm_error(_openai_status(400, UNSUPPORTED_EFFORT))
    assert not _is_retryable_llm_error(_openai_status(400, OPENROUTER_INVALID_MODEL))
    assert _is_retryable_llm_error(_openai_status(503, PROVIDER_OUTAGE))
    assert _is_retryable_llm_error(
        _openai_status(400, PROXY_NOT_LOADED, base_url=PROXY_URL)
    )
    assert _is_retryable_llm_error(_openai_interrupted_stream())


# ---------------------------------------------------------------------------
# 2. Inner layer -- the shared retry loop (auxiliary tasks, embeddings)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "status", "attempts"),
    [
        pytest.param(UNSUPPORTED_EFFORT, 400, 1, id="deterministic-once"),
        pytest.param(OPENROUTER_INVALID_MODEL, 400, 1, id="invalid-model-once"),
        pytest.param(PROVIDER_OUTAGE, 503, 3, id="outage-retried"),
    ],
)
async def test_shared_retry_loop_replays_only_retryable_failures(
    payload, status, attempts
):
    calls = []

    async def attempt():
        calls.append(1)
        raise _openai_status(status, payload)

    with pytest.raises(openai.APIStatusError):
        await invoke_with_retry(
            attempt, policy=RetryPolicy(max_attempts=3, base_delay=0.0)
        )
    assert len(calls) == attempts


# ---------------------------------------------------------------------------
# 3. Worker execute node over the production transport, then both outer layers
# ---------------------------------------------------------------------------


def _wire_llm(tmp_path, monkeypatch, respond, base_url=BASE_URL):
    """The production ReasoningChatOpenAI over an offline transport.

    Same seam as test_muse_spark_1_3_family.test_serialized_request.
    """
    original = reasoning_chat.ReasoningCapturingClient
    original_async = reasoning_chat.AsyncReasoningCapturingClient
    monkeypatch.setattr(
        reasoning_chat,
        "ReasoningCapturingClient",
        lambda **kw: original(**kw, transport=httpx.MockTransport(respond)),
    )
    monkeypatch.setattr(
        reasoning_chat,
        "AsyncReasoningCapturingClient",
        lambda **kw: original_async(**kw, transport=httpx.MockTransport(respond)),
    )
    monkeypatch.setattr(reasoning_chat, "count_request_tokens", lambda *a, **kw: 10)
    path = tmp_path / "wire-llm.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "$extends": "worker_base",
                "agent_id": "wire-test",
                "display_name": "Wire Test",
                "llm": {
                    "model": CONTRIBUTOR,
                    "provider": "openai",
                    "api_key": FIXTURE_KEY,
                    "base_url": base_url,
                    "reasoning_level": "xhigh",
                    "max_retries": 0,
                    "streaming": False,
                },
            }
        )
    )
    config = loader.load_agent_config(str(path))
    llm = loader.create_llm(config.llm, limits=config.limits)
    return llm, llm.bind_tools(copy.deepcopy(TOOL_SCHEMAS)), config


_COMPLETION = {
    "id": "fixture",
    "object": "chat.completion",
    "created": 0,
    "model": CONTRIBUTOR,
    "choices": [
        {
            "index": 0,
            "finish_reason": "stop",
            "message": {"role": "assistant", "content": "Recovered."},
        }
    ],
    "usage": {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12},
}


async def _execute_once(
    request,
    tmp_path,
    monkeypatch,
    status,
    payload,
    *,
    base_url=BASE_URL,
    recover_after=None,
):
    """One worker execute invocation; returns (node result, wire requests).

    ``recover_after``: answer 200 once that many requests have failed.
    """
    wire = []

    def respond(http_request):
        wire.append(json.loads(http_request.content))
        if recover_after is not None and len(wire) > recover_after:
            return httpx.Response(200, json=_COMPLETION)
        return httpx.Response(status, json=payload)

    llm, bound, config = _wire_llm(tmp_path, monkeypatch, respond, base_url)
    node = _make_node(
        request.getfixturevalue("env"),
        bound,
        config=config,
        # The production budget (limits.llm_inproc_retries: 5), without sleeps.
        retry_manager=ToolRetryManager(max_retries=5, base_delay=0.0, max_delay=0.0),
    )
    try:
        # The shared Postgres checkpointer is what arms the outage freeze.
        with patch("agent.graph.checkpointer_backend", return_value="postgres"):
            result = await _run(node, _state())
    finally:
        llm.http_client.close()
        await llm.http_async_client.aclose()
    return result, wire


@pytest.mark.asyncio
@pytest.mark.usefixtures("worker_runtime")
@pytest.mark.parametrize(
    ("payload", "detail"),
    [
        pytest.param(UNSUPPORTED_EFFORT, "reasoning_effort", id="pilot-9792db96"),
        pytest.param(
            OPENROUTER_INVALID_MODEL, "not a valid model ID", id="stateless-c1a63bec"
        ),
    ],
)
async def test_deterministic_rejection_is_one_request_and_fails_on_both_lanes(
    request, tmp_path, monkeypatch, payload, detail
):
    result, wire = await _execute_once(request, tmp_path, monkeypatch, 400, payload)

    # Inner layer: exactly one provider request, no outage freeze.
    assert len(wire) == 1
    assert wire[0]["model"] == CONTRIBUTOR
    assert result["should_stop"] is True
    assert result.get("freeze_data") is None
    error = result["error"]
    assert error["recoverable"] is False
    # Actionable: names the model and the rejected parameter/value, no secrets.
    assert CONTRIBUTOR in error["message"]
    assert detail in error["message"]
    assert "HTTP 400" in error["message"]
    assert FIXTURE_KEY not in error["message"]

    # Outer layer, pinned lane: the status authority fails it -- no pause.
    status, message = determine_job_status(
        {"id": "job", "status": "processing", "context": {}}, result
    )
    assert status == "failed"
    assert CONTRIBUTOR in message

    # Outer layer, stateless lane: one terminal report, no release/re-claim.
    assert StatelessTurnExecutor._worker_stop_is_recoverable(result, None) is False
    claim = _claim(input_seq=3, prior="processing", attempts=1, max_attempts=5)
    executor, agent, client, _, rotate, complete, release = _install(
        monkeypatch, claim, result
    )
    await executor._serve_worker_claim(claim)
    client.report_completion.assert_awaited_once()
    assert client.report_completion.await_args.args[1]["error"]["recoverable"] is False
    complete.assert_awaited_once()
    release.assert_not_awaited()
    rotate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.usefixtures("worker_runtime")
async def test_provider_outage_keeps_inner_retries_and_outer_redispatch(
    request, tmp_path, monkeypatch
):
    """The contrast: a genuine outage still rides every retry layer."""
    result, wire = await _execute_once(
        request, tmp_path, monkeypatch, 503, PROVIDER_OUTAGE
    )

    assert len(wire) == 6
    freeze = result["freeze_data"]
    assert freeze["freeze_type"] == "llm_unavailable"
    assert freeze["classification"] == "transient"
    assert result.get("error") is None

    status, _ = determine_job_status(
        {"id": "job", "status": "processing", "context": {}}, result
    )
    assert status == "paused"

    assert StatelessTurnExecutor._worker_stop_is_recoverable(result, "llm_unavailable")
    claim = _claim(input_seq=3, prior="processing", attempts=1, max_attempts=5)
    executor, agent, client, _, rotate, complete, release = _install(
        monkeypatch, claim, result
    )
    await executor._serve_worker_claim(claim)
    client.report_completion.assert_not_awaited()
    release.assert_awaited_once()


@pytest.mark.asyncio
async def test_proxy_restart_window_is_ridden_out_by_the_inner_retry(
    request, tmp_path, monkeypatch
):
    """Two not-yet-loaded 400s from the proxy, then it is up: the turn succeeds."""
    result, wire = await _execute_once(
        request,
        tmp_path,
        monkeypatch,
        400,
        PROXY_NOT_LOADED,
        base_url=PROXY_URL,
        recover_after=2,
    )

    assert len(wire) == 3
    assert result.get("error") is None
    assert not result.get("should_stop")
    assert result.get("freeze_data") is None


@pytest.mark.asyncio
async def test_model_the_proxy_never_routes_still_fails_at_the_outer_layer(
    request, tmp_path, monkeypatch
):
    """The same 400 for good: inner retries, one pause, then the pinned lane's
    4xx fingerprint fails the job on the second identical cycle."""
    result, wire = await _execute_once(
        request, tmp_path, monkeypatch, 400, PROXY_NOT_LOADED, base_url=PROXY_URL
    )
    assert len(wire) == 6
    freeze = result["freeze_data"]
    assert freeze["freeze_type"] == "llm_unavailable"

    job = {"id": "job", "status": "processing", "context": {}}
    assert determine_job_status(job, result) == ("paused", None)

    now = datetime.now(timezone.utc).isoformat()
    job["context"] = {
        "llm_outage": {
            "attempt": 1,
            "first_failed_at": now,
            "last_failed_at": now,
            "fingerprint": llm_outage_fingerprint(freeze),
        }
    }
    status, message = determine_job_status(job, result)
    assert status == "failed"
    assert "unknown provider for model" in message


# ---------------------------------------------------------------------------
# 4. The operator-facing message
# ---------------------------------------------------------------------------


def test_rejection_message_names_model_status_and_provider_fields():
    from shared.runtime.core.llm_retry import _describe_llm_rejection

    message = _describe_llm_rejection(
        _openai_status(400, UNSUPPORTED_EFFORT), CONTRIBUTOR
    )
    assert message.startswith(f"Model '{CONTRIBUTOR}' rejected the request (HTTP 400")
    assert "invalid_request_error" in message
    assert "param 'reasoning_effort'" in message
    assert "code 'unsupported_value'" in message
    assert "does not support 'max'" in message
    assert "Admin → Models" in message
    assert FIXTURE_KEY not in message


def test_rejection_message_without_model_or_provider_fields_stays_legible():
    from shared.runtime.core.llm_retry import _describe_llm_rejection

    untyped = _describe_llm_rejection(_openai_status(400, OPENROUTER_INVALID_MODEL))
    assert untyped.startswith("The provider rejected the request (HTTP 400")
    assert f"{CONTRIBUTOR} is not a valid model ID" in untyped
    # A non-API body keeps the edge summary rather than inventing fields.
    edge = _describe_llm_rejection(_openai_status(400, text=EDGE_400), CONTRIBUTOR)
    assert "provider edge" in edge
    assert "<html>" not in edge
