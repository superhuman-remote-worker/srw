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

from openai import AsyncOpenAI

from agent.services.image_content import make_image_content_block_from_b64

logger = logging.getLogger(__name__)


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

    def __init__(self):
        """Initialize the Vision Helper with configuration from environment."""
        # Load vision-specific config (separate from primary LLM)
        # This allows using OpenAI for vision tasks (e.g., gpt-4o)
        # while the primary agent uses a text-only model on a custom endpoint
        from shared.runtime.core.transport_resolution import (
            is_openai_default_endpoint,
        )

        # Vision-specific overrides
        # Base URL: Use VISION_BASE_URL if set, otherwise default to OpenAI
        #           (NOT LLM_BASE_URL - vision models are typically only on OpenAI)
        # API key: Use VISION_API_KEY if set, otherwise fall back to
        #          OPENAI_API_KEY — but only when the endpoint IS OpenAI (a key
        #          follows its endpoint).
        self.api_base = os.getenv("VISION_BASE_URL", self.OPENAI_API_URL)
        primary_key = (
            os.getenv("OPENAI_API_KEY", "")
            if is_openai_default_endpoint(self.api_base)
            else ""
        )
        self.api_key = os.getenv("VISION_API_KEY", primary_key)
        self.model = os.getenv("VISION_MODEL", "gpt-4o")
        self.timeout = float(os.getenv("VISION_TIMEOUT", "120"))

        if not self.api_key:
            logger.warning(
                "No VISION_API_KEY or OPENAI_API_KEY configured - vision tasks will fail"
            )

        self.client = AsyncOpenAI(
            api_key=self.api_key,
            base_url=self.api_base,
            timeout=self.timeout,
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

        default_prompt = (
            "Describe this image in detail, including all visible text, "
            "objects, colors, layout, and any other relevant information."
        )

        prompt_text = query or default_prompt

        try:
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
            )

            return content

        except Exception as e:
            logger.error(f"Error in describe_image: {e}", exc_info=True)
            return f"[Error analyzing image: {str(e)}]"

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
            )

            return content

        except Exception as e:
            logger.error(f"Error in describe_document_page: {e}", exc_info=True)
            return f"[Error analyzing document page {page_num}: {str(e)}]"

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
            response = _AI(content=response_text)

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
    creating new instances.

    Returns:
        Shared VisionHelper instance
    """
    global _vision_helper
    if _vision_helper is None:
        _vision_helper = VisionHelper()
    return _vision_helper
