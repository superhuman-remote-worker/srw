"""ChatOpenAI wrapper that captures reasoning content from reasoning models.

LangChain's ChatOpenAI doesn't capture reasoning fields that DeepSeek R1,
OpenRouter, and similar reasoning models return. This module provides a custom
wrapper that intercepts the raw HTTP response to capture and preserve reasoning
content across multiple provider formats (reasoning_content, reasoning,
reasoning_details).

Also implements Layer 0 context overflow protection by counting tokens in the
actual HTTP request body before sending.

Set DEBUG_LLM_STREAM=1 to print a tail of LLM responses to stderr after each call.
"""

import json
import logging
import os
import sys
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, AsyncIterator, Callable, Iterator, Optional

import httpx
from langchain_core.messages import AIMessage, AIMessageChunk
from langchain_core.outputs import ChatGenerationChunk
from langchain_openai import ChatOpenAI
from pydantic import PrivateAttr

from shared.runtime.llm.exceptions import ContextOverflowError

if TYPE_CHECKING:
    from shared.runtime.llm.key_ring import KeyRing

logger = logging.getLogger(__name__)

# Request-scoped sink for live reasoning deltas. The SSE tap parses
# ``reasoning_content`` off the wire (Chat Completions reasoning models —
# gemma/DeepSeek/OpenRouter) below the LangChain layer that drops it, and only
# the merged response surfaces it — *after* every answer token. A caller that
# wants reasoning in true chronological order (before the answer) sets this
# contextvar to a sync callback; the tap invokes it per delta as the bytes
# arrive. Defaults to None ⇒ no live emission, identical to legacy behavior.
# See knowledge-base/knowledge/issues/persistent_chat_reasoning_after_answer_and_replay_duplication.md
_STREAM_REASONING_SINK: ContextVar[Optional[Callable[[str], None]]] = ContextVar(
    "stream_reasoning_sink", default=None
)

# Token counting constants
DEFAULT_MAX_CONTEXT_TOKENS = 100_000
WARNING_THRESHOLD_RATIO = 0.9

# Try to import tiktoken for accurate token counting
try:
    import tiktoken

    TIKTOKEN_AVAILABLE = True
except ImportError:
    TIKTOKEN_AVAILABLE = False
    logger.warning("tiktoken not available for HTTP-layer token counting")


def _is_debug_stream() -> bool:
    """Check at call time whether debug streaming is enabled."""
    return os.environ.get("DEBUG_LLM_STREAM", "").strip() in ("1", "true", "yes")


def _get_debug_tail_chars() -> int:
    """Get tail buffer size at call time."""
    return int(os.environ.get("DEBUG_LLM_TAIL", "500"))


def _dump_codex_raw_response(
    request: "httpx.Request", response: "httpx.Response"
) -> None:
    """Dump a raw /v1/responses request+response pair to disk for inspection.

    Diagnostic for the codex-proxy non-streaming failure mode where the
    proxy returns real LLM output but the agent ends up with an empty
    AIMessage. See knowledge-base/knowledge/issues/langchain_responses_api_streaming.md for
    background. Captures land in $CODEX_RAW_DUMP_DIR (default /tmp). All
    failures are silent — this must never break the live request path.
    """
    try:
        import uuid
        from datetime import datetime, timezone
        from pathlib import Path

        out_dir = Path(os.environ.get("CODEX_RAW_DUMP_DIR", "/tmp"))
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique = uuid.uuid4().hex[:8]
        out_path = out_dir / f"codex-raw-{ts}-{unique}.json"

        try:
            req_body = json.loads(request.content) if request.content else None
        except (json.JSONDecodeError, ValueError):
            req_body = (request.content or b"").decode("utf-8", errors="replace")

        body_bytes = response.content
        try:
            resp_body = json.loads(body_bytes) if body_bytes else None
        except (json.JSONDecodeError, ValueError):
            resp_body = body_bytes.decode("utf-8", errors="replace")

        capture = {
            "captured_at": ts,
            "url": str(request.url),
            "method": request.method,
            "request_body": req_body,
            "status_code": response.status_code,
            "response_headers": dict(response.headers),
            "response_body": resp_body,
            "response_body_size_bytes": len(body_bytes) if body_bytes else 0,
        }
        out_path.write_text(json.dumps(capture, indent=2, default=str))
        logger.info(
            f"Codex raw response captured: {out_path} "
            f"({capture['response_body_size_bytes']} bytes)"
        )
    except Exception as exc:
        logger.warning(f"Codex raw capture failed (non-fatal): {exc}")


def _overflow_response_413(
    request: Any, overflow: ContextOverflowError
) -> httpx.Response:
    """Convert a pre-flight context overflow into a non-retryable HTTP response.

    Raising from inside ``send()`` gets wrapped into a retryable
    ``openai.APIConnectionError`` by the SDK (``openai/_base_client.py``), so a
    deterministic "request too big" failure was retried with backoff and
    surfaced as a bare "Connection error." with the real cause buried in
    ``__cause__`` — and ``persistent_graph`` misread it as "streaming not
    supported". A synthetic 413 instead surfaces immediately as a typed,
    non-retried ``APIStatusError`` carrying the real message; callers detect
    it via ``code == "context_overflow"``.
    See knowledge-base/knowledge/issues/session_silent_failure_audit.md #3.
    """
    return httpx.Response(
        status_code=413,
        request=request,
        json={
            "error": {
                "message": overflow.message,
                "type": "invalid_request_error",
                "code": "context_overflow",
                "token_count": overflow.token_count,
                "limit": overflow.limit,
            }
        },
    )


def count_request_tokens(body: dict, model: str = "gpt-4") -> int:
    """Count tokens in OpenAI API request body.

    Counts tokens in messages, tool definitions, and request overhead.
    This gives an accurate count of what's actually being sent to the API.

    Args:
        body: Parsed JSON body of the API request
        model: Model name for tokenizer selection

    Returns:
        Estimated token count
    """
    if not TIKTOKEN_AVAILABLE:
        # Fallback: approximate as ~4 chars per token
        return len(json.dumps(body)) // 4

    try:
        enc = tiktoken.encoding_for_model(model)
    except KeyError:
        enc = tiktoken.get_encoding("cl100k_base")

    total = 0

    # Count messages
    for msg in body.get("messages", []):
        # Count role
        total += len(enc.encode(msg.get("role", "")))

        # Count content
        content = msg.get("content", "")
        if isinstance(content, str):
            total += len(enc.encode(content))
        elif isinstance(content, list):
            # Handle multimodal content (text parts)
            for part in content:
                if isinstance(part, dict) and "text" in part:
                    total += len(enc.encode(part["text"]))

        # Count tool calls in assistant messages
        if "tool_calls" in msg:
            total += len(enc.encode(json.dumps(msg["tool_calls"])))

        # Count tool_call_id in tool messages
        if "tool_call_id" in msg:
            total += len(enc.encode(msg["tool_call_id"]))

        # Message structure overhead (~4 tokens per message)
        total += 4

    # Handle Responses API format (input field instead of messages)
    if not body.get("messages"):
        for item in body.get("input", []):
            if isinstance(item, str):
                total += len(enc.encode(item))
            elif isinstance(item, dict):
                total += len(enc.encode(item.get("role", "")))
                content = item.get("content", "")
                if isinstance(content, str):
                    total += len(enc.encode(content))
                elif isinstance(content, list):
                    for part in content:
                        if isinstance(part, dict) and "text" in part:
                            total += len(enc.encode(part["text"]))
                total += 4
        instructions = body.get("instructions", "")
        if instructions:
            total += len(enc.encode(instructions))

    # Count tool definitions
    for tool in body.get("tools", []):
        total += len(enc.encode(json.dumps(tool)))

    # Request structure overhead
    total += 10

    return total


def extract_reasoning_text_from_block(block: dict) -> str:
    """Pull plain reasoning text out of a Responses API reasoning content block.

    Tolerates both the merged-full shape (``summary``/``content`` lists with
    ``{"type": "...", "text": "..."}`` items) and the streaming-delta shapes
    (an item with a bare ``text`` field, or the block carrying ``text``
    directly). Returns an empty string when nothing text-shaped is present —
    callers can use that to skip empty emissions.
    """
    parts: list[str] = []

    direct = block.get("text")
    if isinstance(direct, str):
        parts.append(direct)

    for item in block.get("summary") or []:
        if isinstance(item, dict):
            t = item.get("text")
            if isinstance(t, str):
                parts.append(t)

    for item in block.get("content") or []:
        if isinstance(item, dict):
            t = item.get("text")
            if isinstance(t, str):
                parts.append(t)

    return "".join(parts)


def _extract_responses_api_reasoning(message) -> None:
    """Extract reasoning from Responses API content blocks into additional_kwargs.

    The Responses API returns content as a list of typed blocks. Reasoning blocks
    have type=="reasoning" with summary/content lists containing text items.
    This function moves reasoning text into additional_kwargs["reasoning_content"]
    and flattens remaining content blocks to a plain string.
    """
    content = message.content
    if not isinstance(content, list):
        return

    reasoning_parts = []
    non_reasoning = []

    for block in content:
        if isinstance(block, dict) and block.get("type") == "reasoning":
            for item in block.get("summary", []):
                if isinstance(item, dict) and "text" in item:
                    reasoning_parts.append(item["text"])
            for item in block.get("content", []):
                if isinstance(item, dict) and "text" in item:
                    reasoning_parts.append(item["text"])
        else:
            non_reasoning.append(block)

    if reasoning_parts:
        message.additional_kwargs["reasoning_content"] = "\n".join(reasoning_parts)

    # Flatten remaining content to strings
    cleaned = []
    for block in non_reasoning:
        if isinstance(block, str):
            cleaned.append(block)
        elif isinstance(block, dict) and "text" in block:
            cleaned.append(block["text"])
        # Skip non-text blocks (function_call items are already in tool_calls)

    message.content = (
        " ".join(cleaned).strip()
        if all(isinstance(c, str) for c in cleaned)
        else cleaned or ""
    )


def _extract_text_from_reasoning_details(details: Any) -> Optional[str]:
    """Extract readable text from OpenRouter reasoning_details blocks."""
    if not isinstance(details, list):
        return None

    parts: list[str] = []
    for item in details:
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if isinstance(text, str) and text:
            parts.append(text)

        for key in ("summary", "content"):
            nested = item.get(key)
            if isinstance(nested, str) and nested:
                parts.append(nested)
            elif isinstance(nested, list):
                for nested_item in nested:
                    if isinstance(nested_item, str) and nested_item:
                        parts.append(nested_item)
                    elif isinstance(nested_item, dict):
                        nested_text = nested_item.get("text")
                        if isinstance(nested_text, str) and nested_text:
                            parts.append(nested_text)

    if not parts:
        return None
    return "\n".join(parts)


def _extract_reasoning_from_response(data: dict) -> Optional[str]:
    """Extract reasoning text from a chat completion response.

    Supports multiple provider formats:
    1. DeepSeek: message.reasoning_content (string)
    2. OpenRouter: message.reasoning (string)
    3. OpenRouter: message.reasoning_details (array of {type, text} objects)
    """
    choices = data.get("choices") or []
    if not choices:
        return None
    msg = choices[0].get("message", {})

    # 1. DeepSeek format
    if msg.get("reasoning_content"):
        return msg["reasoning_content"]

    # 2. OpenRouter plain string
    if msg.get("reasoning"):
        return msg["reasoning"]

    # 3. OpenRouter reasoning_details array
    details_text = _extract_text_from_reasoning_details(msg.get("reasoning_details"))
    if details_text:
        return details_text

    return None


def _extract_reasoning_from_delta(delta: dict) -> Optional[str]:
    """Pull reasoning text out of a Chat Completions streaming delta.

    Mirrors :func:`_extract_reasoning_from_response` but operates on the
    per-chunk ``delta`` object rather than a finished ``message``.
    Returns ``None`` when the chunk carries no readable reasoning text.

    OpenRouter also streams ``reasoning_details`` arrays on some models.
    """
    if not isinstance(delta, dict):
        return None
    rc = delta.get("reasoning_content")
    if isinstance(rc, str) and rc:
        return rc
    r = delta.get("reasoning")
    if isinstance(r, str) and r:
        return r
    details_text = _extract_text_from_reasoning_details(delta.get("reasoning_details"))
    if details_text:
        return details_text
    return None


class _SSEReasoningTap:
    """Tee an httpx streaming response to extract reasoning text from SSE deltas.

    The OpenAI SDK consumes streaming Chat Completions responses by iterating
    ``response.aiter_bytes()`` (or ``iter_bytes()`` in sync mode). This tap
    intercepts that iteration: it forwards bytes to the SDK unchanged while
    locally parsing ``data: {...}`` lines to harvest
    ``choices[0].delta.reasoning_content`` (and ``.reasoning``). LangChain's
    ``_convert_delta_to_message_chunk`` drops these non-standard fields, so
    capture has to happen at the wire layer before LangChain converts.

    The constructor snapshots the response's *original* iter methods so the
    tap delegates to them rather than to whatever may later overwrite the
    instance attributes (which is how :func:`_install_streaming_reasoning_tap`
    installs the tap — by reassigning ``response.iter_bytes``).

    After the stream is consumed, ``reasoning_content`` returns the
    accumulated text or ``None``. The tap is single-use; install one per
    streaming request.
    """

    def __init__(self, response: httpx.Response) -> None:
        self._response = response
        # Snapshot the originals so iter_bytes() / aiter_bytes() don't
        # recurse into themselves after _install_streaming_reasoning_tap
        # overwrites the response's attributes.
        self._upstream_iter_bytes = response.iter_bytes
        self._upstream_aiter_bytes = response.aiter_bytes
        self._buffer = b""
        self._parts: list[str] = []

    @property
    def reasoning_content(self) -> Optional[str]:
        if not self._parts:
            return None
        return "".join(self._parts)

    def iter_bytes(self, chunk_size: Optional[int] = None) -> Iterator[bytes]:
        for chunk in self._upstream_iter_bytes(chunk_size):
            self._consume(chunk)
            yield chunk
        self._flush()

    async def aiter_bytes(
        self, chunk_size: Optional[int] = None
    ) -> AsyncIterator[bytes]:
        async for chunk in self._upstream_aiter_bytes(chunk_size):
            self._consume(chunk)
            yield chunk
        self._flush()

    def _consume(self, chunk: bytes) -> None:
        if not chunk:
            return
        self._buffer += chunk
        while b"\n" in self._buffer:
            line, self._buffer = self._buffer.split(b"\n", 1)
            self._parse_line(line)

    def _flush(self) -> None:
        # Handle trailing line without newline (rare; some servers omit
        # the final \n before the connection closes).
        if self._buffer:
            self._parse_line(self._buffer)
            self._buffer = b""

    def _parse_line(self, raw: bytes) -> None:
        line = raw.strip()
        if not line.startswith(b"data: ") or line == b"data: [DONE]":
            return
        try:
            data = json.loads(line[6:])
        except (json.JSONDecodeError, UnicodeDecodeError):
            return
        choices = data.get("choices") if isinstance(data, dict) else None
        if not choices:
            return
        delta = choices[0].get("delta") if isinstance(choices[0], dict) else None
        text = _extract_reasoning_from_delta(delta or {})
        if text:
            self._parts.append(text)
            # Emit live (before the answer tokens) if a caller installed a
            # sink for this request. Best-effort: a sink failure must never
            # break the stream the SDK is consuming through this tap.
            sink = _STREAM_REASONING_SINK.get()
            if sink is not None:
                try:
                    sink(text)
                except Exception:  # noqa: BLE001 - never break the wire stream
                    logger.debug("reasoning delta sink failed", exc_info=True)


def _install_streaming_reasoning_tap(response: httpx.Response) -> _SSEReasoningTap:
    """Attach an :class:`_SSEReasoningTap` to a streaming response.

    Replaces ``aiter_bytes``/``iter_bytes`` on the response instance so that
    downstream consumers (the OpenAI SDK) get the tapped iterator. Returns
    the tap so callers can read accumulated reasoning after the stream is
    exhausted.
    """
    tap = _SSEReasoningTap(response)
    response.iter_bytes = tap.iter_bytes  # type: ignore[method-assign]
    response.aiter_bytes = tap.aiter_bytes  # type: ignore[method-assign]
    return tap


def _prepare_reasoning_request(
    request: httpx.Request,
    *,
    model: str,
    max_context_tokens: int,
    key_ring: Optional["KeyRing"],
) -> tuple[bool, bool, Optional[httpx.Response]]:
    """Classify and validate one request before either client's transport.

    Return the chat/Responses flags and any synthetic overflow response.
    Keep key rotation, response lifetime and reasoning capture in each client.
    """
    url_str = str(request.url)
    is_chat = "/chat/completions" in url_str
    is_responses = (
        url_str.rstrip("/").endswith("/responses") or "/responses/" in url_str
    )
    is_llm_request = is_chat or is_responses

    # Inject current key from KeyRing into the request header
    if key_ring and is_llm_request:
        try:
            current = key_ring.current_key
            request.headers["authorization"] = f"Bearer {current}"
        except RuntimeError:
            # All keys exhausted — let the request go with whatever header it has
            logger.error("KeyRing: all keys exhausted, sending with original header")

    # Token validation for LLM requests (Layer 0 safety check)
    if is_llm_request:
        overflow: Optional[ContextOverflowError] = None
        try:
            body = json.loads(request.content)
            token_count = count_request_tokens(body, model)

            # Log warning if approaching limit (90% threshold)
            if token_count > max_context_tokens * WARNING_THRESHOLD_RATIO:
                logger.warning(
                    f"Request approaching context limit: "
                    f"{token_count:,}/{max_context_tokens:,} tokens "
                    f"({token_count / max_context_tokens * 100:.1f}%)"
                )

            if token_count > max_context_tokens:
                logger.error(
                    f"Context overflow at HTTP layer: "
                    f"{token_count:,} tokens exceeds limit of {max_context_tokens:,}"
                )
                overflow = ContextOverflowError(
                    token_count=token_count,
                    limit=max_context_tokens,
                    request_size_bytes=len(request.content),
                )

        except json.JSONDecodeError:
            # Non-JSON request body, skip validation
            logger.debug("Skipping token count for non-JSON request")
        except Exception as e:
            # Log but don't fail on counting errors - let the request through
            logger.warning(f"Token counting failed, allowing request: {e}")

        if overflow is not None:
            # Don't raise — return a synthetic 413 the SDK won't retry.
            return is_chat, is_responses, _overflow_response_413(request, overflow)

    return is_chat, is_responses, None


class ReasoningCapturingClient(httpx.Client):
    """HTTP client that captures reasoning_content and validates context limits.

    This client intercepts all HTTP requests to:
    1. Override the Authorization header with the KeyRing's current key (if configured)
    2. Count tokens in chat completion requests before sending
    3. Raise ContextOverflowError if tokens exceed the limit
    4. Capture reasoning_content from responses (for DeepSeek-style models)
    5. Rotate to next API key on auth/quota failures (401, 403, quota-429)
    """

    def __init__(
        self,
        *args,
        timeout: Optional[float] = None,
        max_context_tokens: Optional[int] = None,
        model: str = "gpt-4",
        key_ring: Optional["KeyRing"] = None,
        **kwargs,
    ):
        # Apply timeout if specified. Granular: bound connect (and write/pool)
        # tightly so a dead endpoint fails fast, while keeping the read budget
        # at the full `timeout` so legitimately slow generations aren't cut off.
        if timeout is not None:
            kwargs["timeout"] = httpx.Timeout(timeout, connect=min(10.0, timeout))
        super().__init__(*args, **kwargs)

        self._last_reasoning_content: Optional[str] = None
        # Streaming tap for the most recent request; consumed by
        # ReasoningChatOpenAI._astream / _stream after the SDK finishes
        # iterating the response.
        self._active_stream_tap: Optional[_SSEReasoningTap] = None
        self._model = model
        self._key_ring = key_ring

        # Set max context tokens with fallback chain:
        # 1. Explicit parameter
        # 2. Environment variable
        # 3. Default constant
        self._max_context_tokens = (
            max_context_tokens
            or int(os.environ.get("MAX_CONTEXT_TOKENS", "0"))
            or DEFAULT_MAX_CONTEXT_TOKENS
        )

        logger.debug(
            f"ReasoningCapturingClient initialized: "
            f"max_context_tokens={self._max_context_tokens}, model={self._model}, "
            f"key_ring={'yes' if key_ring else 'no'}"
        )

    def send(self, request, **kwargs):
        is_chat, is_responses, overflow_response = _prepare_reasoning_request(
            request,
            model=self._model,
            max_context_tokens=self._max_context_tokens,
            key_ring=self._key_ring,
        )
        if overflow_response is not None:
            return overflow_response
        is_llm_request = is_chat or is_responses

        # Send the request
        response = super().send(request, **kwargs)

        # Key rotation: retry once on auth/quota errors
        if is_llm_request and self._key_ring and self._key_ring.has_alternatives:
            rotated_response = self._handle_key_rotation(request, response, **kwargs)
            if rotated_response is not None:
                response = rotated_response

        # Capture reasoning_content from response.
        # When stream=True, httpx doesn't eagerly read the body — install an
        # SSE tap on the response so reasoning is harvested per-delta as the
        # SDK iterates. The tap is consumed by ReasoningChatOpenAI._stream
        # after the stream finishes. Non-streaming case parses the full
        # body directly.
        if is_chat:
            if kwargs.get("stream", False):
                self._active_stream_tap = _install_streaming_reasoning_tap(response)
            else:
                try:
                    data = json.loads(response.content)
                    self._last_reasoning_content = _extract_reasoning_from_response(
                        data
                    )
                except (json.JSONDecodeError, KeyError, IndexError):
                    pass

        return response

    def consume_streamed_reasoning(self) -> Optional[str]:
        """Return reasoning text captured by the most recent streaming response.

        Single-shot: clears the tap once read. Returns ``None`` when no tap
        was installed (non-streaming request) or the stream carried no
        reasoning deltas.
        """
        tap = self._active_stream_tap
        if tap is None:
            return None
        self._active_stream_tap = None
        return tap.reasoning_content

    def _handle_key_rotation(
        self, request: httpx.Request, response: httpx.Response, **kwargs
    ) -> Optional[httpx.Response]:
        """Check if response indicates auth/quota failure and retry with next key.

        Returns a new response if rotation succeeded, or None to keep the original.
        """
        status = response.status_code

        if status == 401 or status == 403:
            return self._rotate_and_retry(
                request, f"HTTP {status} auth error", **kwargs
            )

        if status == 429 and self._is_quota_error(response):
            return self._rotate_and_retry(request, "quota exceeded (429)", **kwargs)

        return None

    def _is_quota_error(self, response: httpx.Response) -> bool:
        """Distinguish quota-429 (rotate) from rate-limit-429 (don't rotate).

        Heuristics:
        - retry-after < 3600 with no quota signals -> rate limit (don't rotate)
        - Body contains quota/billing keywords -> quota (rotate)
        - No retry-after and no clear signal -> assume quota (conservative)
        """
        # Check retry-after header
        retry_after_raw = response.headers.get("retry-after")
        retry_after = None
        if retry_after_raw:
            try:
                retry_after = float(retry_after_raw)
            except (ValueError, TypeError):
                pass

        # Check response body for quota signals
        quota_keywords = ("quota", "billing", "insufficient_quota", "exceeded")
        body_text = ""
        try:
            body_text = response.text.lower()
        except Exception:
            pass

        has_quota_signal = any(kw in body_text for kw in quota_keywords)

        if has_quota_signal:
            return True

        if retry_after is not None and retry_after < 3600:
            # Short retry-after without quota signal -> rate limit
            return False

        # No retry-after and no clear signal -> assume quota (conservative)
        return True

    def _rotate_and_retry(
        self, request: httpx.Request, reason: str, **kwargs
    ) -> Optional[httpx.Response]:
        """Rotate to next key and retry the request once.

        Returns the retry response, or None if rotation failed.
        """
        new_key = self._key_ring.rotate(reason)
        if new_key is None:
            logger.error(
                f"Key rotation failed: no alternative keys available ({reason})"
            )
            return None

        # Override header with new key and retry
        request.headers["authorization"] = f"Bearer {new_key}"
        logger.info(f"Retrying request with rotated key after: {reason}")
        return super().send(request, **kwargs)


class AsyncReasoningCapturingClient(httpx.AsyncClient):
    """Async HTTP client that captures reasoning_content and validates context limits.

    Async counterpart of ReasoningCapturingClient. Used by LangChain's async path
    (ainvoke/agenerate) which creates its own httpx.AsyncClient by default, bypassing
    the sync http_client entirely. Passing this as http_async_client ensures key
    rotation, Layer 0 overflow checks, and reasoning capture work in async mode.
    """

    def __init__(
        self,
        *args,
        timeout: Optional[float] = None,
        max_context_tokens: Optional[int] = None,
        model: str = "gpt-4",
        key_ring: Optional["KeyRing"] = None,
        **kwargs,
    ):
        # Granular timeout (see sibling builder above): short connect so a dead
        # endpoint fails fast; read budget stays at the full `timeout`.
        if timeout is not None:
            kwargs["timeout"] = httpx.Timeout(timeout, connect=min(10.0, timeout))
        super().__init__(*args, **kwargs)

        self._last_reasoning_content: Optional[str] = None
        # Streaming tap for the most recent request; consumed by
        # ReasoningChatOpenAI._astream / _stream after the SDK finishes
        # iterating the response.
        self._active_stream_tap: Optional[_SSEReasoningTap] = None
        self._model = model
        self._key_ring = key_ring

        self._max_context_tokens = (
            max_context_tokens
            or int(os.environ.get("MAX_CONTEXT_TOKENS", "0"))
            or DEFAULT_MAX_CONTEXT_TOKENS
        )

        logger.debug(
            f"AsyncReasoningCapturingClient initialized: "
            f"max_context_tokens={self._max_context_tokens}, model={self._model}, "
            f"key_ring={'yes' if key_ring else 'no'}"
        )

    async def send(self, request, **kwargs):
        is_chat, is_responses, overflow_response = _prepare_reasoning_request(
            request,
            model=self._model,
            max_context_tokens=self._max_context_tokens,
            key_ring=self._key_ring,
        )
        if overflow_response is not None:
            return overflow_response
        is_llm_request = is_chat or is_responses

        # Send the request (async)
        response = await super().send(request, **kwargs)

        # Key rotation: retry once on auth/quota errors
        if is_llm_request and self._key_ring and self._key_ring.has_alternatives:
            rotated_response = await self._handle_key_rotation(
                request, response, **kwargs
            )
            if rotated_response is not None:
                response = rotated_response

        # Capture reasoning_content from response.
        # Streaming path: install an SSE tap on the response so reasoning is
        # harvested per-delta as the SDK iterates aiter_bytes. The tap is
        # consumed by ReasoningChatOpenAI._astream once the stream finishes.
        # Non-streaming path: parse the full body directly.
        if is_chat:
            if kwargs.get("stream", False):
                self._active_stream_tap = _install_streaming_reasoning_tap(response)
            else:
                try:
                    data = json.loads(response.content)
                    self._last_reasoning_content = _extract_reasoning_from_response(
                        data
                    )
                except (json.JSONDecodeError, KeyError, IndexError):
                    pass

        # Diagnostic: dump raw /v1/responses payloads when DEBUG_CODEX_RAW_RESPONSE=1.
        # The codex proxy + openai SDK Pydantic deserialization path can produce
        # empty AIMessages from non-empty proxy responses (see
        # knowledge-base/knowledge/issues/langchain_responses_api_streaming.md "Non-streaming failure
        # mode"). This capture lets us inspect what the proxy is actually
        # returning vs. what langchain ends up with. Non-streaming only —
        # accessing response.content on a streamed response would raise.
        if (
            is_responses
            and not kwargs.get("stream", False)
            and os.environ.get("DEBUG_CODEX_RAW_RESPONSE", "").strip()
            in ("1", "true", "yes")
        ):
            _dump_codex_raw_response(request, response)

        return response

    def consume_streamed_reasoning(self) -> Optional[str]:
        """Return reasoning text captured by the most recent streaming response.

        Single-shot: clears the tap once read. Returns ``None`` when no tap
        was installed (non-streaming request) or the stream carried no
        reasoning deltas.
        """
        tap = self._active_stream_tap
        if tap is None:
            return None
        self._active_stream_tap = None
        return tap.reasoning_content

    async def _handle_key_rotation(
        self, request: httpx.Request, response: httpx.Response, **kwargs
    ) -> Optional[httpx.Response]:
        """Check if response indicates auth/quota failure and retry with next key."""
        status = response.status_code

        if status == 401 or status == 403:
            return await self._rotate_and_retry(
                request, f"HTTP {status} auth error", **kwargs
            )

        if status == 429 and self._is_quota_error(response):
            return await self._rotate_and_retry(
                request, "quota exceeded (429)", **kwargs
            )

        return None

    def _is_quota_error(self, response: httpx.Response) -> bool:
        """Distinguish quota-429 (rotate) from rate-limit-429 (don't rotate)."""
        retry_after_raw = response.headers.get("retry-after")
        retry_after = None
        if retry_after_raw:
            try:
                retry_after = float(retry_after_raw)
            except (ValueError, TypeError):
                pass

        quota_keywords = ("quota", "billing", "insufficient_quota", "exceeded")
        body_text = ""
        try:
            body_text = response.text.lower()
        except Exception:
            pass

        has_quota_signal = any(kw in body_text for kw in quota_keywords)

        if has_quota_signal:
            return True

        if retry_after is not None and retry_after < 3600:
            return False

        return True

    async def _rotate_and_retry(
        self, request: httpx.Request, reason: str, **kwargs
    ) -> Optional[httpx.Response]:
        """Rotate to next key and retry the request once."""
        new_key = self._key_ring.rotate(reason)
        if new_key is None:
            logger.error(
                f"Key rotation failed: no alternative keys available ({reason})"
            )
            return None

        request.headers["authorization"] = f"Bearer {new_key}"
        logger.info(f"Retrying request with rotated key after: {reason}")
        return await super().send(request, **kwargs)


def _content_parts(content: Any) -> list:
    if isinstance(content, str):
        return [{"type": "text", "text": content}] if content else []
    return list(content or [])


def fold_system_messages(messages: list) -> list:
    """Leave at most one system message, and only as the first message.

    For chat templates that accept a single leading system turn — Qwen3.x
    raises ``System message must be at the beginning.`` on any other — while
    SRW places the compaction summary as a second system message and may add
    system nudges mid-history. The leading run of system messages merges into
    one; each later system message becomes a user turn where it stands, so the
    messages before it (and the provider's cached prefix) are unchanged.
    Returns a new list; the input dicts are not mutated.
    """
    lead = 0
    while lead < len(messages) and messages[lead].get("role") == "system":
        lead += 1
    out = []
    if lead:
        contents = [m.get("content") for m in messages[:lead]]
        if all(isinstance(c, str) for c in contents):
            merged: Any = "\n\n".join(c for c in contents if c)
        else:
            merged = []
            for c in contents:
                parts = _content_parts(c)
                if merged and parts:
                    merged.append({"type": "text", "text": "\n\n"})
                merged.extend(parts)
        out.append({**messages[0], "content": merged})
    for m in messages[lead:]:
        out.append({**m, "role": "user"} if m.get("role") == "system" else m)
    return out


def mark_anthropic_cache_breakpoints(messages: list, payload_messages: list) -> list:
    """Put Anthropic cache breakpoints on the system prompt and the stable history.

    ``messages`` is the request view (context entries already folded into
    their carriers). Besides the system prompt it marks, by request shape
    (WP2 spec §H):

    - **legacy tail** (any legacy injection in the request): the per-turn
      injections (memory, knowledge, guidance, the App Guide turn boundary,
      ...) sit at the tail and change each turn. A breakpoint on the last
      message, which is what the subscription proxy places when the caller
      sends none, writes the cache entry inside that tail, so no later
      request can read it. The last message that is not an injection is
      marked instead. Measured through CLIProxyAPI on 2026-09-29: 81-84% of
      input cached with these two markers, against a fixed ~2.6k tokens
      without.
    - **append-only** (a folded carrier in the request): nothing is rebuilt,
      so the newest AIMessage (the message right before the newest carrier
      group) reads the previous request's entry, and the last message writes
      the next one. Three of Anthropic's four breakpoints.
    - otherwise the last message.

    ``messages`` are the LangChain messages ``payload_messages`` were converted
    from. The conversion is one-to-one; on a length mismatch the indices cannot
    be mapped and the payload is returned unchanged. Returns a new list.
    """
    from shared.runtime.core.context_entries import (
        has_folded_carrier,
        is_context_injection,
        is_legacy_injection,
    )

    if len(messages) != len(payload_messages):
        return payload_messages
    system_idx = next(
        (
            i
            for i, m in enumerate(payload_messages)
            if m.get("role") in ("system", "developer")
        ),
        None,
    )
    last_idx = len(messages) - 1 if messages else None
    anchors: set = {system_idx}
    if any(is_legacy_injection(m) for m in messages):
        anchors.add(
            next(
                (
                    i
                    for i in range(len(messages) - 1, -1, -1)
                    if not is_context_injection(messages[i])
                ),
                None,
            )
        )
    elif has_folded_carrier(messages):
        anchors.add(
            next(
                (
                    i
                    for i in range(len(messages) - 1, -1, -1)
                    if isinstance(messages[i], AIMessage)
                ),
                None,
            )
        )
        anchors.add(last_idx)
    else:
        anchors.add(last_idx)
    out = list(payload_messages)
    for idx in anchors - {None}:
        out[idx] = {**out[idx], "cache_control": {"type": "ephemeral"}}
    return out


class ReasoningChatOpenAI(ChatOpenAI):
    """ChatOpenAI that captures reasoning_content and validates context limits.

    This class wraps LangChain's ChatOpenAI to:
    1. Capture the `reasoning_content` field from DeepSeek-style reasoning models
    2. Validate context limits at the HTTP layer (Layer 0 safety check)

    The reasoning content is stored in `additional_kwargs['reasoning_content']`.

    When DEBUG_LLM_STREAM=1 is set, prints the last N characters of each LLM
    response to stderr (default 500, override with DEBUG_LLM_TAIL).

    Usage:
        llm = ReasoningChatOpenAI(
            model="deepseek-reasoner",
            base_url="https://api.deepseek.com/v1",
            api_key="your-key",
            max_context_tokens=128000,  # Optional: context limit
        )
        response = llm.invoke("Solve this problem step by step...")
        reasoning = response.additional_kwargs.get("reasoning_content")
    """

    # Family setting `single_system_message` (model_config_matrix.yaml): fold
    # the request down to one leading system message before it is sent.
    single_system_message: bool = False

    # Claude over an OpenAI-format transport (the subscription proxy): add
    # explicit cache breakpoints — see mark_anthropic_cache_breakpoints.
    anthropic_cache_breakpoints: bool = False

    # Use PrivateAttr for Pydantic compatibility
    _reasoning_client: ReasoningCapturingClient = PrivateAttr(default=None)
    _async_reasoning_client: AsyncReasoningCapturingClient = PrivateAttr(default=None)

    def __init__(
        self,
        max_context_tokens: Optional[int] = None,
        key_ring: Optional["KeyRing"] = None,
        **kwargs,
    ):
        # Extract config for our custom client
        timeout = kwargs.get("timeout")
        model = kwargs.get("model", "gpt-4")

        # Create sync client (used by invoke/_generate)
        reasoning_client = ReasoningCapturingClient(
            timeout=timeout,
            max_context_tokens=max_context_tokens,
            model=model,
            key_ring=key_ring,
        )
        # Create async client (used by ainvoke/_agenerate — the actual production path)
        async_reasoning_client = AsyncReasoningCapturingClient(
            timeout=timeout,
            max_context_tokens=max_context_tokens,
            model=model,
            key_ring=key_ring,
        )
        kwargs["http_client"] = reasoning_client
        kwargs["http_async_client"] = async_reasoning_client
        super().__init__(**kwargs)
        # Store after init
        self._reasoning_client = reasoning_client
        self._async_reasoning_client = async_reasoning_client

    def _get_request_payload(self, input_, *, stop=None, **kwargs):
        payload = super()._get_request_payload(input_, stop=stop, **kwargs)
        if self.single_system_message and isinstance(payload.get("messages"), list):
            payload["messages"] = fold_system_messages(payload["messages"])
        if self.anthropic_cache_breakpoints and isinstance(
            payload.get("messages"), list
        ):
            payload["messages"] = mark_anthropic_cache_breakpoints(
                self._convert_input(input_).to_messages(), payload["messages"]
            )
        return payload

    def _post_process_result(self, result):
        """Post-process LLM result: capture reasoning and debug output.

        Handles both Chat Completions (reasoning via HTTP client) and
        Responses API (reasoning in content blocks) formats.
        """
        # 1. Inject Chat Completions reasoning from HTTP layer (sync or async client)
        reasoning_content = None
        if self._reasoning_client and self._reasoning_client._last_reasoning_content:
            reasoning_content = self._reasoning_client._last_reasoning_content
            self._reasoning_client._last_reasoning_content = None
        elif (
            self._async_reasoning_client
            and self._async_reasoning_client._last_reasoning_content
        ):
            reasoning_content = self._async_reasoning_client._last_reasoning_content
            self._async_reasoning_client._last_reasoning_content = None

        if reasoning_content:
            for gen in result.generations:
                if hasattr(gen, "message"):
                    gen.message.additional_kwargs["reasoning_content"] = (
                        reasoning_content
                    )
                    logger.debug(
                        f"Captured reasoning_content: {len(reasoning_content)} chars"
                    )

        # 2. Extract Responses API reasoning from content blocks
        for gen in result.generations:
            if hasattr(gen, "message"):
                _extract_responses_api_reasoning(gen.message)

        # 3. Debug: print tail of response to stderr
        if _is_debug_stream():
            tail_chars = _get_debug_tail_chars()
            for gen in result.generations:
                msg = getattr(gen, "message", None)
                if not msg:
                    continue
                content = getattr(msg, "content", "") or ""
                tool_calls = getattr(msg, "tool_calls", None) or []
                reasoning = (msg.additional_kwargs or {}).get("reasoning_content", "")

                # Build debug output
                parts = []
                if reasoning:
                    r_tail = (
                        reasoning[-tail_chars:]
                        if len(reasoning) > tail_chars
                        else reasoning
                    )
                    parts.append(
                        f"\033[33m[reasoning {len(reasoning)} chars]\033[0m ...{r_tail}"
                    )
                if content:
                    c_tail = (
                        content[-tail_chars:] if len(content) > tail_chars else content
                    )
                    parts.append(
                        f"\033[36m[content {len(content)} chars]\033[0m ...{c_tail}"
                    )
                if tool_calls:
                    tc_summary = ", ".join(tc.get("name", "?") for tc in tool_calls)
                    parts.append(f"\033[32m[tools: {tc_summary}]\033[0m")
                if not content and not tool_calls:
                    parts.append(
                        "\033[31m[empty response — no content, no tools]\033[0m"
                    )

                for part in parts:
                    sys.stderr.write(f"\n{part}\n")
                sys.stderr.flush()

        return result

    def _generate(self, *args, **kwargs):
        result = super()._generate(*args, **kwargs)
        return self._post_process_result(result)

    async def _agenerate(self, *args, **kwargs):
        result = await super()._agenerate(*args, **kwargs)
        return self._post_process_result(result)

    def _stream(self, *args, **kwargs):
        """Wrap the parent stream and emit a trailing reasoning chunk.

        LangChain's ``_convert_delta_to_message_chunk`` discards the
        non-standard ``reasoning_content`` field on Chat Completions deltas,
        so the HTTP client taps it server-side instead. Once the SDK has
        consumed the streaming response, the tap holds the accumulated
        reasoning text; we surface it as a trailing synthetic chunk so the
        merged AIMessage carries ``additional_kwargs.reasoning_content``,
        matching the non-streaming code path's contract.
        """
        for chunk in super()._stream(*args, **kwargs):
            yield chunk
        rc = (
            self._reasoning_client.consume_streamed_reasoning()
            if self._reasoning_client
            else None
        )
        if rc:
            logger.debug(f"Captured reasoning_content from stream: {len(rc)} chars")
            yield ChatGenerationChunk(
                message=AIMessageChunk(
                    content="",
                    additional_kwargs={"reasoning_content": rc},
                )
            )

    async def _astream(self, *args, **kwargs):
        """Async counterpart of :meth:`_stream`."""
        async for chunk in super()._astream(*args, **kwargs):
            yield chunk
        rc = (
            self._async_reasoning_client.consume_streamed_reasoning()
            if self._async_reasoning_client
            else None
        )
        if rc:
            logger.debug(f"Captured reasoning_content from astream: {len(rc)} chars")
            yield ChatGenerationChunk(
                message=AIMessageChunk(
                    content="",
                    additional_kwargs={"reasoning_content": rc},
                )
            )
