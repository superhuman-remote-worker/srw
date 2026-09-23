"""llm_retry — provider-error triage and the retry loop, shared by every LLM call site.

This module answers two questions: *what kind of failure is this?*
(:func:`_classify_llm_error`) and *should I try again, and after how long?*
(:class:`RetryPolicy` + :func:`invoke_with_retry`). It deliberately does NOT
decide what happens when retries run out. Terminal disposition stays with each
caller — a worker job freezes for pause+backoff re-dispatch, a session turn
surfaces the error and stays alive, a summarizer fold raises so the caller
degrades to trimming, a light subagent reader returns partial synthesis, an
auxiliary task falls back to the main model. That divergence is correct policy,
not accident, and folding it in here is what would make this unusable and get
it bypassed.

Everything here is pure exception inspection — ``status_code`` attributes,
class-name matching, and regex over stringified provider bodies. No graph
state, no config, no I/O, and no imports beyond the standard library. That
purity is load-bearing: the subagent child driver (``src/subagents``) and the
persistent loop are unit-tested with a fake LLM and no SSH, and neither can
reach triage that lives inside the graph. This code
previously lived in ``src/agent/graph.py``, which forced ``src/agent/persistent_graph.py``
to import it lazily inside a function to dodge the 5k-line module; that dodge
is gone now.

``src/agent/graph.py`` re-exports every name below, so the historical import path
still works.

Two failure directions are both real, and the classifier is calibrated between
them — bias for retry, but do not retry blindly:

* over-eager ``permanent`` destroys finished work
  (knowledge-history/done/transient_408_stream_disconnect_misclassified_as_permanent.md)
* over-eager ``transient`` loops forever
  (knowledge-history/done/agent_infinite_retry_on_permanent_llm_errors.md)

See knowledge-history/done/llm_retry_and_fallback_reimplemented_per_call_site.md.
"""

import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, TypeVar
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _extract_rate_limit_delay(error: Exception) -> Optional[float]:
    """Extract retry-after delay from rate limit errors.

    Checks the exception and its chain for rate limit indicators (HTTP 429)
    and extracts the retry-after value from headers or error messages.

    Args:
        error: The exception to inspect

    Returns:
        Delay in seconds if rate limit detected, None otherwise
    """
    error_str = str(error)

    # Check if this is a rate limit error
    is_rate_limit = (
        "429" in error_str
        or "rate limit" in error_str.lower()
        or "too many requests" in error_str.lower()
    )
    if not is_rate_limit:
        return None

    # Try to extract retry-after from the exception chain
    # Anthropic/OpenAI SDK exceptions may have response headers
    current = error
    while current is not None:
        # Check for response attribute with headers (httpx/SDK exceptions)
        response = getattr(current, "response", None)
        if response is not None:
            headers = getattr(response, "headers", {})
            retry_after = headers.get("retry-after")
            if retry_after:
                try:
                    return float(retry_after) + 5.0  # Add buffer
                except (ValueError, TypeError):
                    pass

        current = current.__cause__ if current.__cause__ != current else None

    # Fallback: try to extract retry-after from error message text
    match = re.search(
        r"retry.?after['\"]?\s*[:=]\s*['\"]?(\d+)", error_str, re.IGNORECASE
    )
    if match:
        return float(match.group(1)) + 5.0

    # Rate limit detected but no retry-after found — use conservative default
    return 90.0


def _api_error_object(exc: BaseException) -> Optional[Dict[str, Any]]:
    """The provider's error object carried on an SDK exception, for either SDK shape.

    The anthropic and groq SDKs keep the whole response envelope on
    ``exc.body`` (``{"type": "error", "error": {"type": ..., "message": ...}}``).
    The openai SDK — the transport of every OpenAI-compatible route: OpenAI,
    OpenRouter, Mistral, the subscription proxy, self-hosted endpoints — stores
    the envelope's ``error`` member ALREADY UNWRAPPED
    (``_make_status_error``: ``body.get("error", body)``), and a provider that
    sends no envelope at all (Mistral's top-level ``{"object": "error", ...}``)
    arrives the same way. Reading only ``body["error"]`` saw nothing on any
    OpenAI-compatible rejection, so every one of them fell through to the
    "400 without a parseable body" retry: pilot 9792db96 replayed an
    unsupported ``reasoning_effort`` in two pause cycles of six identical
    attempts (knowledge-base/knowledge/issues/deterministic_provider_rejection_retried_unchanged.md).

    Returns ``None`` when the body is not a dict — an edge page, a bare text
    response, a stream closed before it was read.
    """
    body = getattr(exc, "body", None)
    if not isinstance(body, dict):
        return None
    inner = body.get("error")
    return inner if isinstance(inner, dict) else body


def _error_field(err_obj: Dict[str, Any], name: str) -> str:
    """``err_obj[name]`` as text, or ``""`` when absent or structured.

    Providers disagree on field types — OpenRouter sends ``code`` as the
    integer ``400`` — so never call ``.lower()`` on a raw field: a classifier
    that raises inside the caller's ``except`` handler takes the whole node
    down with it.
    """
    value = err_obj.get(name)
    if value is None or isinstance(value, (dict, list)):
        return ""
    return str(value)


def _is_codex_auth_unavailable(exc: BaseException) -> bool:
    """True if a 401 is a Codex/OAuth-proxy *token-unavailable* error rather
    than a genuinely-bad API key.

    The Codex proxy (CLIProxyAPI) surfaces an invalidated/expired OAuth token
    — or one stuck mid-refresh — as ``code: auth_unavailable`` /
    "invalidated oauth token for user". Unlike a bad API key, that clears after
    a proxy re-auth or the next token refresh, so the caller retries (bounded)
    instead of failing the job permanently. Inspects a single exception; the
    caller walks the ``__cause__`` chain.
    """
    err_obj = _api_error_object(exc)
    if err_obj is not None:
        code = _error_field(err_obj, "code").lower()
        msg = _error_field(err_obj, "message").lower()
        if code == "auth_unavailable" or "invalidated oauth token" in msg:
            return True
    text = str(exc).lower()
    return "auth_unavailable" in text or "invalidated oauth token" in text


def _request_url_str(exc: BaseException) -> Optional[str]:
    """Best-effort extraction of the HTTP request URL from an SDK error.

    openai/anthropic ``APIStatusError`` carry ``.request`` (an httpx.Request)
    and ``.response`` (whose ``.request`` also exposes the URL). Duck-typed to
    match the rest of the classifier — no SDK import required.
    """
    for attr_path in (("request", "url"), ("response", "request", "url")):
        obj: Any = exc
        for attr in attr_path:
            obj = getattr(obj, attr, None)
            if obj is None:
                break
        if obj is not None:
            return str(obj)
    return None


# Host tokens that identify the shared subscription proxy when no explicit
# base-URL env var is set. The Kubernetes Service is still named
# ``*-codex-proxy`` (the display rename deliberately did not rename storage or
# Services), and ``subscription-proxy`` / ``cli-proxy`` cover an installation
# that has since renamed it.
_PROXY_HOST_TOKENS = ("codex", "subscription-proxy", "cli-proxy")


def _is_codex_proxy_url(url: str) -> bool:
    """True if ``url`` targets the CLIProxyAPI subscription proxy.

    Subscription models are reached over an OpenAI-compatible transport
    (``provider="openai"``/``codex"`` + the proxy's ``base_url``), so the
    request URL is the only thing that tells a proxy call apart from a real
    ``api.openai.com`` call *at exception-classification time* — the exception
    carries a URL, not a ModelMeta. Markers, in order of authority:

    * an explicit ``SUBSCRIPTION_PROXY_URL`` / ``CODEX_BASE_URL`` /
      ``CODEX_PROXY_URL`` host (operator-set — the only authoritative one)
    * a host containing a well-known proxy token — ``codex`` (the in-cluster
      Service is still ``*-codex-proxy``), ``subscription-proxy`` or
      ``cli-proxy`` for an installation that has renamed it
    * CLIProxyAPI's default port ``8317`` (the local-dev default base URL,
      see ``loader._create_codex_llm``)

    The behaviour it gates — treat a 401 as a transient token-refresh blip — is
    a property of the *proxy*, not of the Codex product, so it is correct for
    every subscription routed through it.
    """
    u = url.lower()
    for env in ("SUBSCRIPTION_PROXY_URL", "CODEX_BASE_URL", "CODEX_PROXY_URL"):
        base = os.environ.get(env)
        if base:
            host = urlsplit(base).netloc.lower()
            if host and host in u:
                return True
    if any(token in u for token in _PROXY_HOST_TOKENS):
        return True
    return ":8317" in u


def _is_codex_proxy_error(exc: BaseException) -> bool:
    """True if ``exc`` came from a request routed through the Codex proxy.

    A 401 from that proxy is a *transient* token-refresh / account-auth blip:
    CLIProxyAPI surfaces it as a generic ``authentication_error`` WITHOUT the
    ``auth_unavailable`` marker (so :func:`_is_codex_auth_unavailable` misses
    it), yet it clears on the next refresh just the same. Treating it as
    recoverable (bounded retry + resume) instead of permanent stops a one-off
    proxy hiccup from hard-failing an autonomous job, while a real
    ``api.openai.com`` bad-key 401 (different host) still fails fast.

    Incident: 2026-06-22 job "Research 01" died on the first strategic call
    with this exact generic 401 while a session ran the same gpt-5.5 through
    the same proxy ~50s later.
    """
    url = _request_url_str(exc)
    return url is not None and _is_codex_proxy_url(url)


# A 429 whose reset window exceeds this is treated as a quota COOLDOWN (fail
# fast), not a per-minute rate limit (retry): retrying within the window is
# futile. Anything at/under it is a normal rate limit. See Defect C in
# knowledge-base/knowledge/issues/loop_ran_codex_spark_not_selected_model_then_hung_on_cooldown.md.
_COOLDOWN_MIN_RESET_SECONDS = 300.0

# The pause-vs-fail-fast cutoff for a quota cooldown: a cooldown whose
# provider-stated reset fits inside this budget PAUSES + resumes from its
# checkpoint (via the llm_unavailable outage path); a longer one (a multi-day
# quota wall) fails fast — pausing that long helps nobody and needs an operator.
# Shares the orchestrator's outage give-up ceiling via the same
# LLM_OUTAGE_CEILING_SECONDS env var so pause-admission (agent-side, here) and
# give-up (orchestrator-side) can never disagree — keep the 43_200 (12h) default
# in sync with orchestrator/services/completion.py (a different pod reads it).
# knowledge-base/knowledge/features/llm_cooldown_pause_and_resume.md
try:
    _COOLDOWN_MAX_PAUSE_SECONDS = float(
        os.getenv("LLM_OUTAGE_CEILING_SECONDS") or 43200
    )
except (ValueError, TypeError):
    _COOLDOWN_MAX_PAUSE_SECONDS = 43200.0


def _cooldown_within_pause_budget(reset_seconds: Optional[float]) -> bool:
    """True if a quota cooldown with ``reset_seconds`` should pause (not fail fast).

    A cooldown whose provider-stated reset window fits inside the pause budget is
    waited out via the outage pause/resume path; an unknown reset (``None``) or a
    window longer than the budget fails fast.
    """
    return reset_seconds is not None and reset_seconds <= _COOLDOWN_MAX_PAUSE_SECONDS


def _cooldown_reset_seconds(exc: BaseException) -> Optional[float]:
    """If ``exc`` is a quota/model cooldown, return its reset window in seconds.

    A cooldown is a 429 whose body carries a ``model_cooldown`` code (all
    credentials exhausted) or a long ``reset_seconds`` — a multi-day quota wall,
    NOT a per-minute throttle. Inspects a SINGLE exception (the classifier walks
    the ``__cause__`` chain). Returns ``None`` when it is not a cooldown.
    """
    code = ""
    reset: Optional[float] = None
    err = _api_error_object(exc)
    if err is not None:
        code = _error_field(err, "code").lower()
        rs = err.get("reset_seconds")
        if isinstance(rs, (int, float)):
            reset = float(rs)
    text = str(exc).lower()
    if reset is None:
        m = re.search(r"reset_seconds['\"]?\s*[:=]\s*([0-9.]+)", text)
        if m:
            reset = float(m.group(1))
    if code == "model_cooldown" or "model_cooldown" in text:
        return reset if reset is not None else _COOLDOWN_MIN_RESET_SECONDS
    if reset is not None and reset > _COOLDOWN_MIN_RESET_SECONDS:
        return reset
    return None


def _cooldown_detail(error: Exception) -> tuple[Optional[float], Optional[str]]:
    """Walk the cause chain and return ``(reset_seconds, model)`` for a cooldown
    error (for an operator-facing message), or ``(None, None)``."""
    current: Optional[BaseException] = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        reset = _cooldown_reset_seconds(current)
        if reset is not None:
            err = _api_error_object(current)
            return reset, (err.get("model") if err is not None else None)
        nxt = getattr(current, "__cause__", None)
        current = nxt if nxt is not current else None
    return None, None


def _cooldown_failfast_error(
    message: str, model: Optional[str], reset_seconds: Optional[float]
) -> Dict[str, Any]:
    """Structured error payload for a cooldown fail-fast.

    Carries ``classification``/``model``/``reset_at`` so the orchestrator's
    loop engine can park the next member instead of spawning it into the same
    frozen model. ``reset_at`` is ABSOLUTE epoch seconds — the payload is read
    by the loop advance minutes later and by the heal path hours later, so a
    relative ``reset_seconds`` would rot; ``None`` when the provider stated no
    reset. knowledge-base/knowledge/issues/loop_advances_into_active_model_cooldown.md
    """
    return {
        "message": message,
        "type": "llm_error",
        "recoverable": False,
        "classification": "cooldown",
        "model": model,
        "reset_at": (
            time.time() + reset_seconds if reset_seconds is not None else None
        ),
    }


def _is_insufficient_quota(exc: BaseException) -> bool:
    """True if ``exc`` is an OpenAI-style ``insufficient_quota`` billing error.

    This is the 429 that means "you exceeded your current quota / check your
    plan and billing" — a spend wall, NOT a per-minute throttle. No wait fixes
    it, so the classifier routes it to fail-fast (``quota_exhausted``) rather
    than pausing the job for hours (llm_outage_pause_and_backoff_redispatch.md).
    Distinct from the ordinary ``rate_limit_exceeded`` 429. Inspects a SINGLE
    exception (the classifier walks the ``__cause__`` chain).

    Google's ``RESOURCE_EXHAUSTED`` is deliberately NOT matched here — it doubles
    as an ordinary per-minute quota/rate-limit signal, so treating it as a
    billing wall would wrongly fail-fast a recoverable rate limit.
    """
    err = _api_error_object(exc)
    if err is not None:
        code = _error_field(err, "code").lower()
        etype = _error_field(err, "type").lower()
        if code == "insufficient_quota" or etype == "insufficient_quota":
            return True
    return "insufficient_quota" in str(exc).lower()


# Transport-level phrases that mark a *transient* mid-stream disconnect. The
# Codex/CLIProxyAPI proxy (and some providers) surface a dropped SSE/HTTP
# response stream as an ``invalid_request_error`` — the same ``type`` a
# deterministic bad-request uses — so the ``type`` alone would misroute it to
# ``permanent``. The tell is the message, not the type: a stream that
# "disconnected before completion" / "closed before response.completed" is
# transport, not input, and a retry clears it. Incident: scholar subjob
# 35b23256 lost 3.5h of finished research when a 408 "stream disconnected
# before completion" (type=invalid_request_error) was classified permanent and
# hard-failed on the very first attempt.
_STREAM_DISCONNECT_MARKERS = (
    "stream disconnected",
    "disconnected before completion",
    "stream closed before",
    "closed before response.completed",
    "incomplete chunked read",
    "peer closed connection",
    "connection reset by peer",
)


def _is_stream_disconnect(text: str) -> bool:
    """True if ``text`` (already lowercased) describes a transient stream drop."""
    return any(marker in text for marker in _STREAM_DISCONNECT_MARKERS)


def _has_api_error_body(exc: BaseException) -> bool:
    """True if ``exc`` carries a parseable API error body (a dict).

    The openai SDK parses the error response body as JSON when it can and
    stores the result — unwrapped to its ``error`` member, see
    :func:`_api_error_object` — on ``exc.body``; a non-JSON body (an nginx/LB default
    error page, a bare text response) is left as the raw string, and a
    closed-before-read stream leaves it ``None``. A dict body therefore means
    the response came from the provider's API application; anything else means
    it came from infrastructure in front of it.
    """
    return isinstance(getattr(exc, "body", None), dict)


def _infra_edge_status(error: Exception) -> Optional[int]:
    """Status code if the failing response is edge-shaped, else ``None``.

    Walks the ``__cause__`` chain (LangChain wraps the provider exception) for
    an exception carrying an HTTP ``status_code`` whose body is NOT a
    parseable API error object — i.e. the provider's gateway/proxy answered
    and the request never reached the API application. Used to exempt such
    failures from the determinism fingerprint (an identical edge page across
    pause cycles means the edge is still down, not that the request is
    deterministic) and to compose a legible error summary.
    """
    current: Optional[BaseException] = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        status_code = getattr(current, "status_code", None)
        if isinstance(status_code, int):
            return None if _has_api_error_body(current) else status_code
        nxt = getattr(current, "__cause__", None)
        current = nxt if nxt is not current else None
    return None


def _summarize_llm_error(error: Exception, model: Optional[str] = None) -> str:
    """Readable one-liner for user-facing error fields.

    ``str(e)`` for an edge-shaped failure IS the raw response body (the openai
    SDK stringifies a non-JSON error body verbatim), so without this a nginx
    HTML 404 page lands untouched in ``jobs.error_message`` and the cockpit
    shows raw markup as the failure reason. Compose a legible summary instead;
    every other exception passes through unchanged. The raw text still goes to
    the audit trail for forensics.
    """
    status = _infra_edge_status(error)
    if status is None:
        return str(error)
    snippet = re.sub(r"<[^>]+>", " ", str(error))
    snippet = re.sub(r"\s+", " ", snippet).strip()[:200]
    model_part = f" (model '{model}')" if model else ""
    return (
        f"LLM endpoint returned HTTP {status}{model_part} — non-API response "
        f"from the provider edge (gateway/proxy); the request never reached "
        f"the API. Detail: {snippet}"
    )


def _api_rejection(error: Exception) -> Optional[Tuple[int, Dict[str, Any]]]:
    """``(status, error object)`` of the first API-answered status error in the
    ``__cause__`` chain, or ``None``."""
    current: Optional[BaseException] = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        status = getattr(current, "status_code", None)
        err_obj = _api_error_object(current)
        if isinstance(status, int) and err_obj is not None:
            return status, err_obj
        nxt = getattr(current, "__cause__", None)
        current = nxt if nxt is not current else None
    return None


def _describe_llm_rejection(error: Exception, model: Optional[str] = None) -> str:
    """Actionable one-liner for a request the provider rejected (``permanent``).

    ``str(e)`` of an SDK rejection is ``Error code: 400 - {<repr of the body>}``:
    accurate, but it omits the model and buries the one useful field in a dict
    repr. Compose the rejection from the provider's error object instead —
    model, HTTP status, ``type``, ``param``, ``code`` and ``message`` — so the
    failed job names what to fix. Only the provider's own error fields are
    used, never request headers or URLs, so no credential rides along. Falls
    back to :func:`_summarize_llm_error` when no status error in the chain
    carries an API error object (an edge page, a stringified error).
    """
    rejection = _api_rejection(error)
    if rejection is None:
        return _summarize_llm_error(error, model)
    status, err_obj = rejection
    etype = _error_field(err_obj, "type")
    param = _error_field(err_obj, "param")
    code = _error_field(err_obj, "code")
    detail = re.sub(r"\s+", " ", _error_field(err_obj, "message")).strip()
    detail = detail[:300].rstrip(".")
    facts = [f"HTTP {status}"]
    if etype:
        facts.append(etype)
    if param:
        facts.append(f"param '{param}'")
    if code and code != str(status):
        facts.append(f"code '{code}'")
    who = f"Model '{model}'" if model else "The provider"
    return (
        f"{who} rejected the request ({', '.join(facts)})"
        + (f": {detail}" if detail else "")
        + ". Not retried: the identical request fails identically. Fix the "
        "model configuration (Admin → Models) or the job's model/reasoning "
        "override, then re-run."
    )


# Statuses whose bodies are genuinely deterministic input rejections, i.e. the
# only ones the stringified ``invalid_request_error`` rule below is allowed to
# claim. 401/403/404 are handled by their own text rules above it; every other
# status (408, 409, 425, 499, 5xx, …) is transport and must stay retryable even
# when the provider stamps an input-rejection *label* on it.
_TEXT_INPUT_REJECTION_STATUS = frozenset({"400", "422"})

# "No such model here" in the words of providers whose 400 carries no
# machine-readable ``type``: OpenRouter ("<id> is not a valid model ID", with
# an integer ``code``) and Mistral ("Invalid model: <id>"). An id that does not
# resolve fails every attempt identically. Deliberately narrow: OpenRouter also
# forwards upstream rejections untyped ("Provider returned error"), and those
# stay retryable — its router may pick a different upstream next time.
_INVALID_MODEL_MARKERS = (
    "not a valid model",
    "invalid model",
    "unknown model",
    "model not found",
    "model_not_found",
    "no such model",
)


def _bad_request_verdict(exc: BaseException) -> Optional[str]:
    """The verdict a 400's provider error object carries, or ``None``.

    A 400 is an input rejection by definition, but providers overload it, so
    it is only ``permanent`` when the error object says so: the
    ``invalid_request_error`` / ``bad_request_error`` label (as ``type``, or as
    ``code`` where a provider files it there) or an untyped "no such model"
    message. The overloads keep retrying: rate limits and Groq's
    ``tool_use_failed`` disguised as 400s, a dropped stream mislabeled as
    one, and the subscription proxy's not-yet-loaded "unknown provider for
    model" answer. ``None`` — no parseable body, or one naming nothing we
    recognise — is left to the caller's conservative default.
    """
    err_obj = _api_error_object(exc)
    if err_obj is None:
        return None
    code = _error_field(err_obj, "code").lower()
    etype = _error_field(err_obj, "type").lower()
    message = _error_field(err_obj, "message").lower()
    if "rate" in code or "rate" in etype:
        return "rate_limit"
    if code == "tool_use_failed":
        return "transient"
    if _is_stream_disconnect(message):
        # A dropped stream mislabeled as a 400 invalid_request_error —
        # transient transport, not a deterministic input rejection.
        return "transient"
    if (
        code == "model_not_found"
        and "unknown provider for model" in message
        and _is_codex_proxy_error(exc)
    ):
        # The subscription proxy's restart window: CLIProxyAPI starts serving
        # before its client load completes, and meanwhile answers every chat
        # request with exactly this invalid_request_error 400. The chart runs
        # it Recreate with no readiness gate (none is possible: /healthz and
        # /v1/models already 200 inside the window), so an image bump or
        # credential reload must not fail every subscription job. A model the
        # proxy genuinely cannot route still fails at the outer layer: the
        # orchestrator's 4xx fingerprint after two identical pause cycles
        # (pinned), the run queue's attempt cap (stateless).
        return "transient"
    # 'invalid_request_error' is the OpenAI/Anthropic vocabulary; MiniMax says
    # 'bad_request_error' (e.g. "invalid function arguments json string" — the
    # 2026-07-11 wedge). Both are deterministic input rejections: no retry can
    # fix them.
    if {etype, code} & {"invalid_request_error", "bad_request_error"}:
        return "permanent"
    if any(marker in message for marker in _INVALID_MODEL_MARKERS):
        return "permanent"
    return None


def _classify_llm_error(error: Exception) -> str:
    """Classify an LLM exception as ``permanent``, ``rate_limit``, or ``transient``.

    Drives the retry decision in ``create_execute_node`` so non-retriable
    failures (404 model-not-found, 401/403 auth, 400 invalid_request) fail
    the job fast instead of looping forever — see
    knowledge-history/done/agent_infinite_retry_on_permanent_llm_errors.md for the
    incident this prevents, and
    knowledge-history/done/transient_408_stream_disconnect_misclassified_as_permanent.md
    for the inverse failure (an over-eager ``permanent`` verdict destroying
    3.5h of work) that the status gate in the text fallback guards against.
    Both directions are real: bias for retry, but do not retry blindly.

    Walks the exception's ``__cause__`` chain because LangChain wraps the
    underlying provider exception. Inspection order:

    1. ``status_code`` attribute (``openai.APIStatusError`` and the
       ``anthropic`` SDK use the same convention) — most reliable signal.
    2. Class name match against the well-known SDK error types — avoids
       a hard dependency on every provider SDK at import time.
    3. Error-message text fallback for stringified provider errors that
       made it through without preserving the original class (already
       observed in production audit logs).

    Returns one of:

    * ``permanent``  — short-circuit retries, mark the job failed.
    * ``quota_exhausted`` — an OpenAI ``insufficient_quota`` billing wall (a
      429 that no wait fixes); fail fast like ``permanent`` rather than pausing
      the job for hours. See :func:`_is_insufficient_quota`.
    * ``cooldown``   — a quota/model cooldown (long reset window); the caller
      fails fast because retrying within the window is futile. See
      :func:`_cooldown_reset_seconds`.
    * ``rate_limit`` — transient, but the caller should respect Retry-After
      via :func:`_extract_rate_limit_delay`.
    * ``transient``  — retry with the existing backoff schedule.
    """
    _PERMANENT_STATUS = {400, 401, 403, 404}

    current: Optional[BaseException] = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))

        status_code = getattr(current, "status_code", None)
        if isinstance(status_code, int):
            if status_code == 429:
                if _is_insufficient_quota(current):
                    return "quota_exhausted"
                if _cooldown_reset_seconds(current) is not None:
                    return "cooldown"
                return "rate_limit"
            if status_code in _PERMANENT_STATUS:
                # 400 needs to be disambiguated — Groq's tool_use_failed and
                # some rate-limit-disguised-as-400 errors are NOT permanent.
                if status_code == 400:
                    # A 400 whose body names nothing we recognise (or has no
                    # parseable body at all) — be conservative, retry.
                    return _bad_request_verdict(current) or "transient"
                if status_code == 401 and (
                    _is_codex_auth_unavailable(current)
                    or _is_codex_proxy_error(current)
                ):
                    # Codex/OAuth-proxy token invalidated, mid-refresh, or a
                    # transient proxy auth blip — recoverable by a proxy
                    # re-auth/refresh + resume, so retry rather than fail the
                    # job. Detected either by the proxy's ``auth_unavailable``
                    # marker OR by the request routing through the codex proxy
                    # (the proxy sometimes returns a *generic* 401 with no
                    # marker — the 2026-06-22 "Research 01" incident). A
                    # genuinely-bad key hits a different host and stays
                    # "permanent" below.
                    return "auth_unavailable"
                if status_code == 404 and not _has_api_error_body(current):
                    # A 404 whose body is not a parseable API error object is
                    # the provider's *edge* (nginx/LB) answering — the request
                    # never reached the API application. Infra, not input:
                    # retryable. A genuine model-not-found 404 carries a JSON
                    # error body and stays permanent. Incident: the 2026-07-17
                    # MiniMax edge outage hard-failed two jobs on attempt 1
                    # (knowledge-base/knowledge/issues/llm_infra_404_misclassified_permanent_kills_jobs.md).
                    return "transient"
                return "permanent"
            if 500 <= status_code < 600:
                return "transient"
            if status_code == 408:
                # 408 Request Timeout is how a dropped response stream most
                # often surfaces (frequently mislabeled type=invalid_request_error
                # in the body — see _is_stream_disconnect). Always retryable.
                return "transient"

        cls_name = type(current).__name__
        if cls_name == "NotFoundError":
            # Same body-shape gate as the 404 status branch above — this
            # fallback matters when a wrapped exception lost its status_code.
            if _has_api_error_body(current):
                return "permanent"
            return "transient"
        if cls_name in (
            "AuthenticationError",
            "PermissionDeniedError",
        ):
            return "permanent"
        if cls_name == "RateLimitError":
            if _is_insufficient_quota(current):
                return "quota_exhausted"
            if _cooldown_reset_seconds(current) is not None:
                return "cooldown"
            return "rate_limit"
        if cls_name == "BadRequestError":
            # Same disambiguation as the 400 status branch above; an
            # unrecognised body keeps walking to the text fallback.
            verdict = _bad_request_verdict(current)
            if verdict is not None:
                return verdict

        nxt = getattr(current, "__cause__", None)
        current = nxt if nxt is not current else None

    error_str = str(error).lower()
    if "auth_unavailable" in error_str or "invalidated oauth token" in error_str:
        return "auth_unavailable"
    if "model" in error_str and (
        "not found" in error_str or "does not exist" in error_str
    ):
        return "permanent"
    if "404" in error_str and "model" in error_str:
        return "permanent"
    if "authenticationerror" in error_str or "invalid_api_key" in error_str:
        return "permanent"
    if "permissiondenied" in error_str:
        return "permanent"
    if "model_cooldown" in error_str:
        return "cooldown"
    if "insufficient_quota" in error_str:
        return "quota_exhausted"
    if (
        "429" in error_str
        or "rate limit" in error_str
        or "too many requests" in error_str
    ):
        return "rate_limit"
    # Gate the stringified input-rejection rule below on the status the SDK
    # stamped into the message. It is written for "a 400 that lost its exception
    # class", but providers reuse the ``invalid_request_error`` *label* on
    # failures carrying a completely different status — the 408 stream-disconnect
    # that cost scholar 35b23256 3.5h of work. Keying on the status rather than
    # on the message wording generalises to every future transport status
    # (409, 425, 499, 5xx, …) instead of needing a new marker each time one bites.
    m_status = re.search(r"error code:\s*(\d{3})", error_str)
    if m_status and m_status.group(1) not in _TEXT_INPUT_REJECTION_STATUS:
        return "transient"
    if _is_stream_disconnect(error_str):
        # No status in the text, but unambiguously a dropped stream: transport,
        # not input.
        return "transient"
    if (
        "bad_request_error" in error_str or "invalid_request_error" in error_str
    ) and "tool_use_failed" not in error_str:
        # Stringified provider 400s that lost their exception class
        # (observed in production audit logs). Deterministic input
        # rejections — rate-disguised 400s and Groq tool_use_failed were
        # already caught above / excluded here.
        return "permanent"

    return "transient"


def initial_error_freeze_fields(
    first_summary: Optional[str],
    first_classification: Optional[str],
    last_summary: str,
    last_classification: str,
) -> dict:
    """Freeze fields naming the FIRST error of a retry sequence, when it differs.

    The last error of an exhausted retry ladder is frequently a *symptom* of the
    first. On 2026-07-29 job ``d251e513`` the real failure was a ``408`` upstream
    stream drop, which flipped the Codex proxy's single auth entry to
    ``status: error`` — so retries 2-6 all came back ``503 auth_unavailable`` and
    overwrote the only useful evidence. The freeze then named a phantom auth
    problem, and every operator who read it went and re-authenticated a token
    that was never broken.

    Returns ``{}`` when the head matches the tail (the common single-cause case),
    so freeze payloads do not carry a duplicate copy of ``error_summary``.
    """
    if first_summary is None:
        return {}
    if first_summary == last_summary and first_classification == last_classification:
        return {}
    return {
        "initial_error_summary": first_summary[:500],
        "initial_classification": first_classification,
    }


# ---------------------------------------------------------------------------
# Retry policy + loop
#
# The mechanism half of this module. Six call sites used to hand-roll this loop
# with four different backoff schedules, and two of them (the light subagent
# readers and the auxiliary tasks) had no loop at all — a single transient 408
# killed both readers of a parallel fan-out on critic job 37c418d2.
# knowledge-history/done/llm_retry_and_fallback_reimplemented_per_call_site.md
# ---------------------------------------------------------------------------

# Verdicts that another identical attempt could plausibly clear. `permanent`,
# `quota_exhausted` and `cooldown` are deliberately absent: no wait fixes a bad
# model name, a billing wall, or a multi-day quota reset, and retrying them is
# what produced the 2026-05-12 cluster outage.
RETRYABLE_CLASSIFICATIONS = frozenset({"transient", "rate_limit", "auth_unavailable"})


@dataclass(frozen=True)
class RetryPolicy:
    """How hard to try, and how long to wait between tries.

    Deliberately small: attempts, spacing, and which verdicts qualify. What to
    do once it gives up belongs to the caller, not here.

    Attributes:
        max_attempts: Total attempts including the first (1 disables retrying).
        base_delay: Exponential base — attempt *n* sleeps ``base * 2**n``.
        max_delay: Ceiling on any single sleep, applied last.
        retryable: Classifications worth another attempt.
        never_retry: Exception types that bypass classification and always
            raise. Use for failures where a second identical attempt is not
            just useless but actively harmful — e.g. an ``asyncio.TimeoutError``
            against a hung model, where retrying burns another full timeout
            when escalating elsewhere would have answered immediately.
        respect_retry_after: Floor the sleep at any provider-stated Retry-After.
            Leave off where a long provider wait is worse than escalating (an
            auxiliary task falling back to the main model shouldn't sit out a
            90 s rate-limit window).
    """

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    retryable: frozenset = RETRYABLE_CLASSIFICATIONS
    never_retry: Tuple[type, ...] = field(default_factory=tuple)
    respect_retry_after: bool = True

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

    def should_retry(self, error: BaseException, attempt: int) -> bool:
        """True if ``error`` on 0-indexed ``attempt`` warrants another try."""
        if attempt + 1 >= self.max_attempts:
            return False
        if self.never_retry and isinstance(error, self.never_retry):
            return False
        if not isinstance(error, Exception):
            return False
        return _classify_llm_error(error) in self.retryable

    def delay_for(self, error: BaseException, attempt: int) -> float:
        """Seconds to sleep before the attempt after 0-indexed ``attempt``."""
        delay = self.base_delay * (2**attempt)
        if self.respect_retry_after and isinstance(error, Exception):
            try:
                provider = _extract_rate_limit_delay(error)
            except Exception:  # pragma: no cover - defensive
                provider = None
            if provider is not None:
                delay = max(delay, provider)
        return min(delay, self.max_delay)


# For a caller that already owns a retry loop one layer up. Retry belongs at
# exactly ONE layer per call path: nesting two loops multiplies provider calls
# and hides the inner failures from the outer layer's attempt accounting and
# progress events. Pass this rather than deleting the wrapping, so the call site
# still reads as "retry is a decision made here".
NO_RETRY = RetryPolicy(max_attempts=1)


async def invoke_with_retry(
    fn: Callable[[], Awaitable[T]],
    *,
    policy: RetryPolicy,
    description: str = "LLM call",
    on_retry: Optional[Callable[[BaseException, int, float], None]] = None,
) -> T:
    """Await ``fn()``, retrying per ``policy``; re-raise the last error if it gives up.

    ``fn`` is re-invoked from scratch each attempt, so wrap the *whole* call —
    including any per-attempt ``asyncio.wait_for`` — inside it, or every retry
    will share one already-consumed timeout budget.

    This never swallows: the caller always sees the final exception and decides
    the disposition. ``asyncio.CancelledError`` is a ``BaseException`` and so
    propagates untouched rather than being treated as a failed attempt.

    Args:
        fn: Zero-arg coroutine factory performing one attempt.
        policy: Attempts, backoff, and which verdicts qualify.
        description: Used in the retry log line.
        on_retry: Called as ``(error, attempt_number_1_indexed, delay)`` before
            each sleep — for metrics or a caller-specific "degraded" signal.

    Returns:
        Whatever ``fn()`` returns on the first successful attempt.

    Raises:
        The exception from the final attempt.
    """
    attempt = 0
    while True:
        try:
            return await fn()
        except Exception as exc:
            if not policy.should_retry(exc, attempt):
                raise
            delay = policy.delay_for(exc, attempt)
            logger.warning(
                "%s failed (%s: %s) — attempt %d/%d, retrying in %.1fs",
                description,
                type(exc).__name__,
                str(exc)[:200],
                attempt + 1,
                policy.max_attempts,
                delay,
            )
            if on_retry is not None:
                try:
                    on_retry(exc, attempt + 1, delay)
                except Exception:  # pragma: no cover - never let a hook break retry
                    logger.debug("on_retry hook raised; continuing", exc_info=True)
            await asyncio.sleep(delay)
            attempt += 1
