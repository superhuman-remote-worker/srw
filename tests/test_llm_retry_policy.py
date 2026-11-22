"""RetryPolicy + invoke_with_retry — the shared retry mechanism.

The classifier half of `src/core/llm_retry.py` is pinned by
`tests/test_graph_helpers.py` (it lived in `src/graph.py` until the extraction).
This file covers the loop that was previously hand-rolled at four call sites
with four different backoff schedules, and absent entirely at two more.

knowledge-history/done/llm_retry_and_fallback_reimplemented_per_call_site.md
"""

import asyncio

import pytest

from shared.runtime.core.llm_retry import (
    RETRYABLE_CLASSIFICATIONS,
    RetryPolicy,
    invoke_with_retry,
)


class _Status(Exception):
    """Provider error carrying an HTTP status, like the openai/anthropic SDKs."""

    def __init__(self, status_code, message=""):
        super().__init__(message or f"Error code: {status_code}")
        self.status_code = status_code


def _counting(*outcomes):
    """Coroutine factory replaying `outcomes`; raises Exceptions, returns others."""
    calls = {"n": 0}

    async def fn():
        i = calls["n"]
        calls["n"] += 1
        outcome = outcomes[min(i, len(outcomes) - 1)]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    return fn, calls


# Zero-delay policy so tests don't actually sleep.
_FAST = RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0)


class TestRetryPolicyDecisions:
    def test_transient_is_retryable(self):
        assert _FAST.should_retry(_Status(408, "stream disconnected"), attempt=0)

    def test_permanent_is_not_retryable(self):
        assert not _FAST.should_retry(Exception("model gpt-x does not exist"), 0)

    def test_last_attempt_never_retries(self):
        # attempt is 0-indexed: with max_attempts=3, attempt 2 is the last.
        err = _Status(500)
        assert _FAST.should_retry(err, attempt=1)
        assert not _FAST.should_retry(err, attempt=2)

    def test_max_attempts_one_disables_retrying(self):
        policy = RetryPolicy(max_attempts=1)
        assert not policy.should_retry(_Status(500), attempt=0)

    def test_max_attempts_must_be_positive(self):
        with pytest.raises(ValueError):
            RetryPolicy(max_attempts=0)

    def test_never_retry_types_bypass_classification(self):
        # A timeout classifies transient by the catch-all, but a caller may still
        # want to escalate rather than burn a second full timeout.
        policy = RetryPolicy(max_attempts=3, never_retry=(asyncio.TimeoutError,))
        assert policy.should_retry(_Status(500), attempt=0)
        assert not policy.should_retry(asyncio.TimeoutError(), attempt=0)

    def test_retryable_set_is_configurable(self):
        strict = RetryPolicy(max_attempts=3, retryable=frozenset({"transient"}))
        assert not strict.should_retry(_Status(429), attempt=0)  # rate_limit
        assert strict.should_retry(_Status(500), attempt=0)

    def test_quota_and_cooldown_are_not_retryable_by_default(self):
        # No wait fixes a billing wall or a multi-day quota reset.
        assert "quota_exhausted" not in RETRYABLE_CLASSIFICATIONS
        assert "cooldown" not in RETRYABLE_CLASSIFICATIONS
        assert "permanent" not in RETRYABLE_CLASSIFICATIONS


class TestRetryPolicyBackoff:
    def test_delay_is_exponential(self):
        policy = RetryPolicy(base_delay=1.0, max_delay=100.0)
        err = _Status(500)
        assert policy.delay_for(err, 0) == 1.0
        assert policy.delay_for(err, 1) == 2.0
        assert policy.delay_for(err, 2) == 4.0

    def test_max_delay_caps_the_sleep(self):
        policy = RetryPolicy(base_delay=1.0, max_delay=3.0)
        assert policy.delay_for(_Status(500), 5) == 3.0

    def test_provider_retry_after_floors_the_delay(self):
        policy = RetryPolicy(base_delay=1.0, max_delay=1000.0)
        err = Exception("429 rate limit; retry-after: 30")
        assert policy.delay_for(err, 0) >= 30.0

    def test_retry_after_can_be_ignored(self):
        # An aux task escalating to the main model should not sit out a 90s
        # provider window — answering from the fallback is strictly better.
        policy = RetryPolicy(base_delay=1.0, max_delay=5.0, respect_retry_after=False)
        err = Exception("429 rate limit; retry-after: 90")
        assert policy.delay_for(err, 0) == 1.0

    def test_max_delay_still_caps_a_provider_floor(self):
        policy = RetryPolicy(base_delay=1.0, max_delay=10.0)
        err = Exception("429 rate limit; retry-after: 600")
        assert policy.delay_for(err, 0) == 10.0


class TestInvokeWithRetry:
    @pytest.mark.asyncio
    async def test_returns_first_success_without_retrying(self):
        fn, calls = _counting("ok")
        assert await invoke_with_retry(fn, policy=_FAST) == "ok"
        assert calls["n"] == 1

    @pytest.mark.asyncio
    async def test_retries_transient_then_succeeds(self):
        fn, calls = _counting(_Status(408, "stream disconnected"), "ok")
        assert await invoke_with_retry(fn, policy=_FAST) == "ok"
        assert calls["n"] == 2

    @pytest.mark.asyncio
    async def test_raises_permanent_immediately(self):
        fn, calls = _counting(Exception("model gpt-x does not exist"))
        with pytest.raises(Exception, match="does not exist"):
            await invoke_with_retry(fn, policy=_FAST)
        assert calls["n"] == 1

    @pytest.mark.asyncio
    async def test_reraises_last_error_when_budget_exhausted(self):
        fn, calls = _counting(
            _Status(500, "boom-1"), _Status(500, "boom-2"), _Status(500, "boom-3")
        )
        with pytest.raises(_Status, match="boom-3"):
            await invoke_with_retry(fn, policy=_FAST)
        assert calls["n"] == 3

    @pytest.mark.asyncio
    async def test_each_attempt_reinvokes_the_factory(self):
        # The whole point of taking a factory rather than a coroutine: a bare
        # coroutine can only be awaited once, so a retry would raise
        # RuntimeError instead of making a second real call.
        fn, calls = _counting(_Status(503), _Status(503), "ok")
        assert await invoke_with_retry(fn, policy=_FAST) == "ok"
        assert calls["n"] == 3

    @pytest.mark.asyncio
    async def test_cancellation_propagates_and_is_not_retried(self):
        fn, calls = _counting(asyncio.CancelledError())
        with pytest.raises(asyncio.CancelledError):
            await invoke_with_retry(fn, policy=_FAST)
        assert calls["n"] == 1

    @pytest.mark.asyncio
    async def test_on_retry_hook_receives_error_attempt_and_delay(self):
        seen = []
        fn, _ = _counting(_Status(500), "ok")
        await invoke_with_retry(
            fn, policy=_FAST, on_retry=lambda e, a, d: seen.append((type(e), a, d))
        )
        assert seen == [(_Status, 1, 0.0)]

    @pytest.mark.asyncio
    async def test_on_retry_hook_failure_never_breaks_the_retry(self):
        def boom(*_):
            raise RuntimeError("hook is broken")

        fn, _ = _counting(_Status(500), "ok")
        assert await invoke_with_retry(fn, policy=_FAST, on_retry=boom) == "ok"

    @pytest.mark.asyncio
    async def test_actually_sleeps_between_attempts(self, monkeypatch):
        slept = []

        async def fake_sleep(d):
            slept.append(d)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        fn, _ = _counting(_Status(500), _Status(500), "ok")
        policy = RetryPolicy(max_attempts=3, base_delay=2.0, max_delay=100.0)
        assert await invoke_with_retry(fn, policy=policy) == "ok"
        assert slept == [2.0, 4.0]


# ---- A certificate-pin failure is permanent and says why --------------------


def _pin_chain(pin_error, *, langchain_layer=False):
    """An openai connection error with ``pin_error`` in its cause chain."""
    import httpx
    import openai

    request = httpx.Request("POST", "https://llm.example/v1/chat/completions")
    inner = httpx.ConnectError("handshake failed", request=request)
    inner.__cause__ = pin_error
    sdk = openai.APIConnectionError(request=request)
    sdk.__cause__ = inner
    if not langchain_layer:
        return sdk
    outer = RuntimeError("langchain wrapper")
    outer.__cause__ = sdk
    return outer


def _pin_errors():
    from shared.tls_pinning import (
        CertificateChangedError,
        PinnedCertificateRejectedError,
    )

    return [
        CertificateChangedError("llm.example", 443, "AA:BB:CC", "DD:EE:FF"),
        CertificateChangedError("llm.example", 443, "AA:BB:CC", None),
        PinnedCertificateRejectedError(
            "llm.example", 443, "AA:BB:CC", "certificate has expired"
        ),
    ]


@pytest.mark.parametrize("wrapped", [False, True], ids=["sdk", "sdk-in-wrapper"])
@pytest.mark.parametrize("index", [0, 1, 2])
def test_pin_failure_is_permanent_and_described_whole(index, wrapped):
    from shared.runtime.core.llm_retry import (
        _classify_llm_error,
        _describe_llm_rejection,
        _summarize_llm_error,
    )

    pin_error = _pin_errors()[index]
    err = _pin_chain(pin_error, langchain_layer=wrapped)

    assert _classify_llm_error(err) == "permanent"
    assert _describe_llm_rejection(err, "some-model") == str(pin_error)
    assert _summarize_llm_error(err, "some-model") == str(pin_error)
    assert "Connection error." not in _describe_llm_rejection(err)


def test_plain_connection_error_stays_transient():
    import httpx
    import openai

    from shared.runtime.core.llm_retry import _classify_llm_error

    err = openai.APIConnectionError(
        request=httpx.Request("POST", "https://llm.example/v1")
    )
    assert _classify_llm_error(err) == "transient"
