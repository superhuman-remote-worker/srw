# src/services/vision_helper.py
"""
Vision Helper service for image and document analysis.

Used when the primary model is text-only (multimodal: false) and we need to
generate text descriptions of visual content (images, document pages).

Ported from Advanced-LLM-Chat/backend/services/vision_helper.py with adaptations:
- Added sync wrappers for use with sync tool signatures
- Configurable via environment variables
"""

import asyncio
import base64
import logging
import os
from typing import Optional, Union

import openai
from openai import AsyncOpenAI

from agent.services.image_content import make_image_content_block_from_b64
from shared.tls_pinning import TlsPin, pin_failure_in, pinned_sdk_http_client

logger = logging.getLogger(__name__)

#: Longest edge sent to the vision model. Providers downscale larger images
#: anyway (Anthropic and OpenAI to ~1568 px / 2048 px), so sending more only
#: costs upload time and request size (image_generation.md WP1).
DEFAULT_VISION_MAX_EDGE = 1568

#: Every failed description starts with this, so neither the model nor a
#: cache can mistake it for a description of the image.
VISION_FAILURE_PREFIX = "[You did not see this image"


def vision_failed(text: Optional[str]) -> bool:
    """Whether ``text`` is a VisionHelper failure rather than a description."""
    return not text or str(text).startswith(VISION_FAILURE_PREFIX)


def _failure(what: str, error: Exception) -> str:
    # The SDK wraps a pin failure as "Connection error."; the admin needs the
    # certificate message that says what to do.
    cause = pin_failure_in(error) or error
    return (
        f"{VISION_FAILURE_PREFIX}: the vision model could not describe {what} "
        f"({type(cause).__name__}: {str(cause)[:200]}). Do not describe it "
        "from its name or from guesses.]"
    )


#: The environment a VisionHelper reads its transport from.
_VISION_ENV_NAMES = (
    "VISION_BASE_URL",
    "VISION_API_KEY",
    "VISION_TLS_PINNED_CERT",
    "VISION_MODEL",
    "VISION_SERVED_MODEL",
    "VISION_TIMEOUT",
    "VISION_MAX_EDGE",
    "OPENAI_API_KEY",
)


def _vision_env_signature() -> tuple:
    return tuple(os.environ.get(name) for name in _VISION_ENV_NAMES)


def fit_for_vision(image_b64: str, mime_type: str, max_edge: int) -> tuple[str, str]:
    """``image_b64`` downscaled to ``max_edge`` when larger; unchanged when it
    already fits or cannot be decoded (the provider then decides)."""
    try:
        from agent.services.image_compose import fit_reference, image_size

        raw = base64.b64decode(image_b64)
        size = image_size(raw)
        if size is None or max(size) <= max_edge:
            return image_b64, mime_type
        data, mime = fit_reference(raw, max_edge=max_edge)
        return base64.b64encode(data).decode("ascii"), mime
    except Exception:  # noqa: BLE001 - downscaling is best effort
        return image_b64, mime_type


def _usage_of(response) -> dict:
    """The OpenAI-shape token usage of a response, for metering."""
    usage = getattr(response, "usage", None)
    if usage is None:
        return {}
    out = {}
    for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
        value = getattr(usage, key, None)
        if isinstance(value, int):
            out[key] = value
    return out


def run_async(coro):
    """Run an async coroutine synchronously.

    Used to bridge async VisionHelper methods with sync tool signatures.
    Creates a new event loop if none exists (safe for tool context).
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # We're already in an async context - use nest_asyncio pattern
        # or run in a thread. For simplicity, create new loop in thread.
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result()
    else:
        return asyncio.run(coro)


class VisionHelper:
    """
    Helper service for vision tasks using a dedicated multimodal model.

    Used when the primary agent model is text-only (multimodal: false) and we
    need to generate text descriptions of visual content (images, document pages).

    Configuration is via environment variables:
    - VISION_API_KEY: API key for vision model (falls back to OPENAI_API_KEY)
    - VISION_BASE_URL: Base URL for vision API (defaults to OpenAI: https://api.openai.com/v1)
    - VISION_MODEL: Model to use (default: gpt-4o)
    - VISION_TIMEOUT: Request timeout in seconds (default: 120)

    Example:
        ```python
        helper = VisionHelper()

        # Async usage
        description = await helper.describe_image(image_bytes)

        # Sync usage (for tools)
        description = helper.describe_image_sync(image_bytes)
        ```
    """

    # Default to OpenAI API - vision models like gpt-4o are only available there
    OPENAI_API_URL = "https://api.openai.com/v1"
    #: Overridden per instance from VISION_MAX_EDGE.
    max_edge = DEFAULT_VISION_MAX_EDGE
    #: Why the configured pin can't be used (no client is built then).
    pin_error: Optional[str] = None

    def __init__(self):
        """Initialize the Vision Helper with configuration from environment."""
        # Load vision-specific config (separate from primary LLM)
        # This allows using OpenAI for vision tasks (e.g., gpt-4o)
        # while the primary agent uses a text-only model on a custom endpoint
        from shared.runtime.core.transport_resolution import (
            is_openai_default_endpoint,
            served_model_from_env,
            wire_model_id,
        )

        # Vision-specific overrides
        # Base URL: Use VISION_BASE_URL if set, otherwise default to OpenAI
        #           (NOT LLM_BASE_URL - vision models are typically only on OpenAI)
        # API key: Use VISION_API_KEY if set, otherwise fall back to
        #          OPENAI_API_KEY — but only when the endpoint IS OpenAI (a key
        #          follows its endpoint).
        self.env_signature = _vision_env_signature()
        self.api_base = os.getenv("VISION_BASE_URL", self.OPENAI_API_URL)
        primary_key = (
            os.getenv("OPENAI_API_KEY", "")
            if is_openai_default_endpoint(self.api_base)
            else ""
        )
        self.api_key = os.getenv("VISION_API_KEY", primary_key)
        # VISION_MODEL is the catalog id; the request carries the upstream
        # string its catalog row names (VISION_SERVED_MODEL), when it names one.
        catalog_model = os.getenv("VISION_MODEL")
        served_model = served_model_from_env("VISION") if catalog_model else None
        self.model = wire_model_id(
            served_model or catalog_model or "gpt-4o", self.api_base
        )
        self.timeout = float(os.getenv("VISION_TIMEOUT", "120"))
        try:
            self.max_edge = max(256, int(os.getenv("VISION_MAX_EDGE", "")))
        except ValueError:
            self.max_edge = DEFAULT_VISION_MAX_EDGE

        # A pin that can't be used means no client and no request: vision is
        # "not available" (no key) and every call reports why.
        client_kwargs: dict = {}
        try:
            pin = TlsPin.from_env("VISION")
            if pin is not None and not os.getenv("VISION_BASE_URL"):
                raise ValueError(
                    "VISION_TLS_PINNED_CERT is set but VISION_BASE_URL is not: "
                    "a pin needs the https:// endpoint it belongs to"
                )
            http_client = pinned_sdk_http_client(
                openai.DefaultAsyncHttpxClient, pin, self.api_base
            )
            if http_client is not None:
                client_kwargs["http_client"] = http_client
        except ValueError as exc:
            self.pin_error = str(exc)
            self.api_key = ""
            logger.error("Vision is unavailable: %s", exc)

        if not self.api_key and not self.pin_error:
            logger.warning(
                "No VISION_API_KEY or OPENAI_API_KEY configured - vision tasks will fail"
            )

        self.client = (
            None
            if self.pin_error
            else AsyncOpenAI(
                api_key=self.api_key,
                base_url=self.api_base,
                timeout=self.timeout,
                **client_kwargs,
            )
        )

        # Log configuration (hiding API key)
        key_source = (
            "VISION_API_KEY" if os.getenv("VISION_API_KEY") else "OPENAI_API_KEY"
        )
        base_source = (
            "VISION_BASE_URL" if os.getenv("VISION_BASE_URL") else "default (OpenAI)"
        )
        logger.info(
            f"VisionHelper initialized: model={self.model}, "
            f"base_url={self.api_base} (from {base_source}), "
            f"api_key from {key_source}, timeout={self.timeout}s"
        )

    async def describe_image(
        self,
        image_data: Union[bytes, str],
        mime_type: str = "image/png",
        query: Optional[str] = None,
        job_id: Optional[str] = None,
    ) -> str:
        """
        Generate a description of an image.

        Args:
            image_data: Raw image bytes or base64-encoded string
            mime_type: MIME type of the image (e.g., "image/jpeg", "image/png")
            query: Optional specific question about the image
            job_id: Optional job ID for archiving the LLM call

        Returns:
            Text description of the image
        """
        if isinstance(image_data, bytes):
            image_data = base64.b64encode(image_data).decode("utf-8")
        image_data, mime_type = fit_for_vision(image_data, mime_type, self.max_edge)

        default_prompt = (
            "Describe this image in detail, including all visible text, "
            "objects, colors, layout, and any other relevant information."
        )

        prompt_text = query or default_prompt

        try:
            if self.pin_error:
                raise ValueError(self.pin_error)
            import time as _time

            start = _time.monotonic()
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt_text},
                            make_image_content_block_from_b64(image_data, mime_type),
                        ],
                    }
                ],
                max_tokens=1000,
            )
            latency_ms = int((_time.monotonic() - start) * 1000)
            content = response.choices[0].message.content

            self._archive_vision_call(
                job_id=job_id,
                prompt_text=prompt_text,
                response_text=content,
                mime_type=mime_type,
                image_size=len(image_data),
                latency_ms=latency_ms,
                usage=_usage_of(response),
            )
            if not content:
                return _failure("the image", ValueError("empty answer"))
            return content

        except Exception as e:
            logger.error(f"Error in describe_image: {e}", exc_info=True)
            return _failure("the image", e)

    def describe_image_sync(
        self,
        image_data: Union[bytes, str],
        mime_type: str = "image/png",
        query: Optional[str] = None,
        job_id: Optional[str] = None,
    ) -> str:
        """Synchronous wrapper for describe_image.

        Use this in sync tool implementations.
        """
        return run_async(self.describe_image(image_data, mime_type, query, job_id))

    async def describe_document_page(
        self,
        page_image: Union[bytes, str],
        page_num: int,
        mime_type: str = "image/png",
        query: Optional[str] = None,
        job_id: Optional[str] = None,
    ) -> str:
        """
        Describe a rendered document page (e.g., PDF page, PowerPoint slide).

        Args:
            page_image: Raw image bytes or base64-encoded string of the rendered page
            page_num: Page number (for context in the prompt)
            mime_type: MIME type of the image (default: "image/png")
            query: Optional specific question about the page
            job_id: Optional job ID for archiving the LLM call

        Returns:
            Text description of the page's visual content
        """
        if isinstance(page_image, bytes):
            page_image = base64.b64encode(page_image).decode("utf-8")
        page_image, mime_type = fit_for_vision(page_image, mime_type, self.max_edge)

        if query:
            prompt = f"Regarding page {page_num}: {query}"
        else:
            prompt = (
                f"Analyze page {page_num} of this document. Describe:\n"
                "1. All visual elements (charts, diagrams, figures, images)\n"
                "2. Any tables or structured data\n"
                "3. Layout and formatting that conveys meaning\n"
                "4. Key information that wouldn't be captured by text extraction alone\n\n"
                "Focus on visual content - text is extracted separately."
            )

        try:
            if self.pin_error:
                raise ValueError(self.pin_error)
            import time as _time

            start = _time.monotonic()
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            make_image_content_block_from_b64(page_image, mime_type),
                        ],
                    }
                ],
                max_tokens=2000,
            )
            latency_ms = int((_time.monotonic() - start) * 1000)
            content = response.choices[0].message.content

            self._archive_vision_call(
                job_id=job_id,
                prompt_text=prompt,
                response_text=content,
                mime_type=mime_type,
                image_size=len(page_image),
                latency_ms=latency_ms,
                page_num=page_num,
                usage=_usage_of(response),
            )
            if not content:
                return _failure(f"page {page_num}", ValueError("empty answer"))
            return content

        except Exception as e:
            logger.error(f"Error in describe_document_page: {e}", exc_info=True)
            return _failure(f"page {page_num}", e)

    def describe_document_page_sync(
        self,
        page_image: Union[bytes, str],
        page_num: int,
        mime_type: str = "image/png",
        query: Optional[str] = None,
        job_id: Optional[str] = None,
    ) -> str:
        """Synchronous wrapper for describe_document_page.

        Use this in sync tool implementations.
        """
        return run_async(
            self.describe_document_page(page_image, page_num, mime_type, query, job_id)
        )

    def _archive_vision_call(
        self,
        job_id: Optional[str],
        prompt_text: str,
        response_text: str,
        mime_type: str,
        image_size: int,
        latency_ms: int,
        page_num: Optional[int] = None,
        usage: Optional[dict] = None,
    ) -> None:
        """Archive a vision LLM call. Fire-and-forget — never raises."""
        if not job_id:
            return

        try:
            from agent.core.archiver import get_archiver
            from langchain_core.messages import HumanMessage as _HM, AIMessage as _AI

            archiver = get_archiver()
            if not archiver:
                return

            # Build synthetic LangChain messages (image data replaced with placeholder)
            human_content = f"{prompt_text}\n\n[image: {mime_type}, {image_size} bytes]"
            messages = [_HM(content=human_content)]
            # The archiver reads token usage from response_metadata, so a
            # vision call is metered like any other call.
            response = _AI(
                content=response_text or "",
                response_metadata={"token_usage": dict(usage or {})},
            )

            aux_meta = {"trigger": "vision"}
            if page_num is not None:
                aux_meta["page_num"] = page_num

            archiver.archive(
                job_id=job_id,
                agent_type="vision",
                messages=messages,
                response=response,
                model=self.model,
                latency_ms=latency_ms,
                call_type="vision",
                auxiliary_metadata=aux_meta,
            )
        except Exception as e:
            logger.warning(f"Failed to archive vision call: {e}")


# Module-level singleton instance (lazy-loaded)
_vision_helper: Optional[VisionHelper] = None


def get_vision_helper() -> VisionHelper:
    """Get or create the VisionHelper singleton instance.

    Use this to get a shared VisionHelper instance rather than
    creating new instances. An agent pod serves one job or Session after
    another, each exporting its own VISION_* block, so the instance is rebuilt
    whenever that environment (the pin included) changed.

    Returns:
        Shared VisionHelper instance
    """
    global _vision_helper
    if _vision_helper is None or _vision_helper.env_signature != (
        _vision_env_signature()
    ):
        _vision_helper = VisionHelper()
    return _vision_helper
