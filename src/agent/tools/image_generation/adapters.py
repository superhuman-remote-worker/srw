"""Image provider adapters for ``generate_image``.

Two wire shapes cover the first providers (owner decision D2, 2026-11-17):

- ``openai-images``: OpenAI's Images API (``/images/generations`` and
  ``/images/edits``). The subscription proxy (CLIProxyAPI) passes this shape
  through to ChatGPT's backend unchanged, so one adapter serves OpenAI direct,
  the proxy and other compatible endpoints. Always JSON; edits send reference
  images as data URLs (``images: [{"image_url": ...}]``, at most 16); never
  ``response_format`` or ``file_id``; ``input_fidelity`` only for gpt-image-1.5.
- ``gemini``: Google's native ``generateContent`` with image output (the
  OpenAI-compatible layer has no edits and no references). One image per call;
  thought parts are skipped; HTTP 200 without an image is a failure that names
  its ``finishReason``.

One call path, one attempt: no retry here. The proxy already retries
internally, a moderation refusal must never be retried, and an image call
costs money or quota (image_generation.md §6 FM2, FM4). Errors never carry
the key.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

import httpx

OPENAI_DEFAULT_BASE_URL = "https://api.openai.com/v1"
GEMINI_DEFAULT_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"

#: Seconds one image call may take ("complex prompts may take up to 2 minutes").
CALL_TIMEOUT_SECONDS = 240.0
MAX_REFERENCES = {"openai-images": 16, "gemini": 14}

_OPENAI_SIZES = {
    "square": "1024x1024",
    "1:1": "1024x1024",
    "landscape": "1536x1024",
    "3:2": "1536x1024",
    "16:9": "1536x1024",
    "4:3": "1536x1024",
    "portrait": "1024x1536",
    "2:3": "1024x1536",
    "9:16": "1024x1536",
    "3:4": "1024x1536",
}
_GEMINI_RATIOS = (
    "1:1", "2:3", "3:2", "3:4", "4:3", "4:5", "5:4", "9:16", "16:9", "21:9",
)  # fmt: skip
_QUALITIES = ("low", "medium", "high", "auto")


class ImageProviderError(Exception):
    """A provider refused or failed an image call.

    ``kind`` is one of: ``auth``, ``no_plan``, ``quota``, ``rate_limit``,
    ``moderation``, ``blocked``, ``bad_request``, ``unavailable``,
    ``timeout``, ``malformed``.
    """

    def __init__(self, kind: str, message: str, *, status: int | None = None):
        super().__init__(message)
        self.kind = kind
        self.status = status


@dataclass
class GeneratedImage:
    data: bytes
    mime_type: str


@dataclass
class ImageCallResult:
    images: list[GeneratedImage]
    #: Normalised token usage: input_text_tokens, input_image_tokens,
    #: output_image_tokens, output_text_tokens (absent keys = unknown).
    usage: dict[str, int] = field(default_factory=dict)
    finish_note: str | None = None


@dataclass(frozen=True)
class ImageRequest:
    prompt: str
    n: int = 1
    size: str | None = None
    quality: str | None = None
    references: Sequence[tuple[bytes, str]] = ()


def _data_url(data: bytes, mime: str) -> str:
    return f"data:{mime};base64,{base64.b64encode(data).decode()}"


def _error_text(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text[:300]
    if isinstance(body, Mapping):
        error = body.get("error")
        if isinstance(error, Mapping):
            parts = [str(error.get(k)) for k in ("code", "message") if error.get(k)]
            return ": ".join(parts)[:300] or str(error)[:300]
        if isinstance(error, str):
            return error[:300]
    return str(body)[:300]


def _classify(status: int, text: str) -> str:
    lowered = text.lower()
    if "moderation" in lowered or "safety" in lowered:
        return "moderation"
    if "auth_not_found" in lowered or "no auth available" in lowered:
        return "no_plan"
    if status in (401, 403):
        return "auth"
    if status == 429:
        if "quota" in lowered or "usage_limit" in lowered or "billing" in lowered:
            return "quota"
        return "rate_limit"
    if status == 402:
        return "quota"
    if status in (400, 404, 413, 422):
        return "bad_request"
    return "unavailable"


#: gpt-image-2.x custom sizes (OpenAI image guide, read 2026-11-17;
#: research reference_1 E4): edges multiples of 16, aspect within 3:1, no
#: edge above 3840, 655,360–8,294,400 pixels in total.
_CUSTOM_MIN_PIXELS = 655_360
_CUSTOM_MAX_PIXELS = 8_294_400
_CUSTOM_MAX_EDGE = 3840


def _takes_custom_sizes(model: str | None) -> bool:
    name = (model or "").strip().lower().rsplit("/", 1)[-1]
    return name.startswith(("gpt-image-2", "chatgpt-image-2"))


def _fit_custom_size(width: int, height: int) -> str:
    """The nearest size the gpt-image-2.x bounds accept, same aspect."""
    import math

    ratio = min(max(width / height, 1 / 3), 3.0)
    pixels = min(max(width * height, _CUSTOM_MIN_PIXELS), _CUSTOM_MAX_PIXELS)
    h = math.sqrt(pixels / ratio)
    w = h * ratio
    scale = min(1.0, _CUSTOM_MAX_EDGE / max(w, h))
    w, h = w * scale, h * scale
    w16 = max(16, int(round(w / 16)) * 16)
    h16 = max(16, int(round(h / 16)) * 16)
    while w16 * h16 < _CUSTOM_MIN_PIXELS:
        if w16 <= h16 and w16 + 16 <= _CUSTOM_MAX_EDGE:
            w16 += 16
        else:
            h16 += 16
    while w16 * h16 > _CUSTOM_MAX_PIXELS:
        if w16 >= h16:
            w16 -= 16
        else:
            h16 -= 16
    return f"{min(w16, _CUSTOM_MAX_EDGE)}x{min(h16, _CUSTOM_MAX_EDGE)}"


def openai_size(size: str | None, model: str | None = None) -> str | None:
    """The Images API ``size`` for a size, an aspect ratio or a word.

    A ``WxH`` the model would refuse is fitted instead of sent: gpt-image-2.x
    gets the nearest size inside its custom-size bounds (a 512x512 request
    becomes 816x816), older models the standard size of the same shape.
    ``None`` (the model's default) for anything unreadable.
    """
    if not size:
        return None
    text = size.strip().lower()
    if text in _OPENAI_SIZES:
        return _OPENAI_SIZES[text]
    parts = text.replace("×", "x").split("x")
    try:
        width, height = (int(parts[0]), int(parts[1])) if len(parts) == 2 else (0, 0)
    except ValueError:
        return None
    if width <= 0 or height <= 0:
        return None
    if _takes_custom_sizes(model):
        return _fit_custom_size(width, height)
    ratio = width / height
    if ratio >= 1.2:
        return "1536x1024"
    if ratio <= 1 / 1.2:
        return "1024x1536"
    return "1024x1024"


def _normalise_openai_usage(usage: Any) -> dict[str, int]:
    if not isinstance(usage, Mapping):
        return {}
    out: dict[str, int] = {}
    details = usage.get("input_tokens_details")
    if isinstance(details, Mapping):
        for src, dst in (
            ("text_tokens", "input_text_tokens"),
            ("image_tokens", "input_image_tokens"),
        ):
            if isinstance(details.get(src), int):
                out[dst] = details[src]
    elif isinstance(usage.get("input_tokens"), int):
        out["input_text_tokens"] = usage["input_tokens"]
    out_details = usage.get("output_tokens_details")
    if isinstance(out_details, Mapping) and isinstance(
        out_details.get("image_tokens"), int
    ):
        out["output_image_tokens"] = out_details["image_tokens"]
        if isinstance(out_details.get("text_tokens"), int):
            out["output_text_tokens"] = out_details["text_tokens"]
    elif isinstance(usage.get("output_tokens"), int):
        out["output_image_tokens"] = usage["output_tokens"]
    return out


async def _post(
    client: httpx.AsyncClient, url: str, payload: dict[str, Any], headers: dict
) -> httpx.Response:
    try:
        return await client.post(url, json=payload, headers=headers)
    except httpx.TimeoutException as exc:
        raise ImageProviderError(
            "timeout",
            f"the image provider did not answer within {CALL_TIMEOUT_SECONDS:.0f}s",
        ) from exc
    except httpx.HTTPError as exc:
        raise ImageProviderError(
            "unavailable",
            f"the image provider could not be reached ({type(exc).__name__})",
        ) from exc


async def call_openai_images(
    section: Mapping[str, Any],
    request: ImageRequest,
    *,
    client: httpx.AsyncClient | None = None,
) -> ImageCallResult:
    """One generations or edits call against an OpenAI-Images-shaped endpoint."""
    base = (section.get("base_url") or OPENAI_DEFAULT_BASE_URL).rstrip("/")
    model = str(section.get("model") or "")
    payload: dict[str, Any] = {
        "model": model,
        "prompt": request.prompt,
        "n": max(1, int(request.n)),
    }
    size = openai_size(request.size, str(section.get("model") or ""))
    if size:
        payload["size"] = size
    if request.quality in _QUALITIES:
        payload["quality"] = request.quality
    if request.references:
        payload["images"] = [
            {"image_url": _data_url(data, mime)} for data, mime in request.references
        ]
        if model.startswith("gpt-image-1.5"):
            payload["input_fidelity"] = "high"
        path = "/images/edits"
    else:
        path = "/images/generations"
    headers = {}
    if section.get("api_key"):
        headers["Authorization"] = f"Bearer {section['api_key']}"
    owns = client is None
    client = client or httpx.AsyncClient(
        timeout=httpx.Timeout(CALL_TIMEOUT_SECONDS, connect=15.0)
    )
    try:
        response = await _post(client, base + path, payload, headers)
    finally:
        if owns:
            await client.aclose()
    if response.status_code >= 400:
        text = _error_text(response)
        raise ImageProviderError(
            _classify(response.status_code, text),
            f"HTTP {response.status_code}: {text}",
            status=response.status_code,
        )
    try:
        body = response.json()
    except ValueError as exc:
        raise ImageProviderError("malformed", "the provider sent no JSON") from exc
    images: list[GeneratedImage] = []
    for item in body.get("data") or []:
        b64 = item.get("b64_json") if isinstance(item, Mapping) else None
        if not b64:
            continue
        try:
            images.append(GeneratedImage(base64.b64decode(b64), "image/png"))
        except (ValueError, TypeError):
            continue
    if not images:
        raise ImageProviderError("malformed", "the provider returned no image data")
    fmt = body.get("output_format")
    if isinstance(fmt, str) and fmt in ("jpeg", "webp", "png"):
        for image in images:
            image.mime_type = f"image/{fmt}"
    return ImageCallResult(
        images=images, usage=_normalise_openai_usage(body.get("usage"))
    )


def gemini_image_config(size: str | None) -> dict[str, str]:
    """``imageConfig`` for a size (``WxH``), an aspect ratio or a word."""
    if not size:
        return {}
    text = size.strip().lower()
    words = {"square": "1:1", "landscape": "3:2", "portrait": "2:3"}
    if text in words:
        return {"aspectRatio": words[text]}
    if text in _GEMINI_RATIOS:
        return {"aspectRatio": text}
    if "x" in text:
        try:
            width, height = (int(v) for v in text.split("x", 1))
        except ValueError:
            return {}
        if width <= 0 or height <= 0:
            return {}
        target = width / height
        ratio = min(
            _GEMINI_RATIOS,
            key=lambda r: abs(int(r.split(":")[0]) / int(r.split(":")[1]) - target),
        )
        edge = max(width, height)
        image_size = "1K" if edge <= 1024 else "2K" if edge <= 2048 else "4K"
        return {"aspectRatio": ratio, "imageSize": image_size}
    return {}


def _normalise_gemini_usage(usage: Any) -> dict[str, int]:
    if not isinstance(usage, Mapping):
        return {}
    out: dict[str, int] = {}
    if isinstance(usage.get("promptTokenCount"), int):
        out["input_text_tokens"] = usage["promptTokenCount"]
    image_out = 0
    text_out = 0
    for detail in usage.get("candidatesTokensDetails") or []:
        if not isinstance(detail, Mapping):
            continue
        count = detail.get("tokenCount")
        if not isinstance(count, int):
            continue
        if str(detail.get("modality", "")).upper() == "IMAGE":
            image_out += count
        else:
            text_out += count
    thoughts = usage.get("thoughtsTokenCount")
    if isinstance(thoughts, int):
        text_out += thoughts
    if image_out:
        out["output_image_tokens"] = image_out
    if text_out:
        out["output_text_tokens"] = text_out
    return out


async def _gemini_once(
    client: httpx.AsyncClient, section: Mapping[str, Any], request: ImageRequest
) -> ImageCallResult:
    base = (section.get("base_url") or GEMINI_DEFAULT_BASE_URL).rstrip("/")
    model = str(section.get("model") or "")
    parts: list[dict[str, Any]] = [{"text": request.prompt}]
    for data, mime in request.references:
        parts.append(
            {"inlineData": {"mimeType": mime, "data": base64.b64encode(data).decode()}}
        )
    generation: dict[str, Any] = {"responseModalities": ["TEXT", "IMAGE"]}
    image_config = gemini_image_config(request.size)
    if image_config:
        generation["imageConfig"] = image_config
    payload = {
        "contents": [{"role": "user", "parts": parts}],
        "generationConfig": generation,
    }
    headers = {}
    if section.get("api_key"):
        headers["x-goog-api-key"] = str(section["api_key"])
    response = await _post(
        client, f"{base}/models/{model}:generateContent", payload, headers
    )
    if response.status_code >= 400:
        text = _error_text(response)
        raise ImageProviderError(
            _classify(response.status_code, text),
            f"HTTP {response.status_code}: {text}",
            status=response.status_code,
        )
    try:
        body = response.json()
    except ValueError as exc:
        raise ImageProviderError("malformed", "the provider sent no JSON") from exc
    feedback = body.get("promptFeedback") or {}
    if isinstance(feedback, Mapping) and feedback.get("blockReason"):
        raise ImageProviderError(
            "blocked", f"the prompt was blocked ({feedback['blockReason']})"
        )
    images: list[GeneratedImage] = []
    reasons: list[str] = []
    texts: list[str] = []
    for candidate in body.get("candidates") or []:
        if not isinstance(candidate, Mapping):
            continue
        if candidate.get("finishReason"):
            reasons.append(str(candidate["finishReason"]))
        content = candidate.get("content") or {}
        for part in content.get("parts") or []:
            if not isinstance(part, Mapping) or part.get("thought"):
                continue
            inline = part.get("inlineData") or part.get("inline_data")
            if isinstance(inline, Mapping) and inline.get("data"):
                try:
                    data = base64.b64decode(inline["data"])
                except (ValueError, TypeError):
                    continue
                mime = inline.get("mimeType") or inline.get("mime_type") or "image/png"
                images.append(GeneratedImage(data, str(mime)))
            elif isinstance(part.get("text"), str):
                texts.append(part["text"])
    if not images:
        why = ", ".join(reasons) or "no image"
        said = (" The model said: " + " ".join(texts)[:300]) if texts else ""
        kind = (
            "blocked"
            if any("SAFETY" in r or "PROHIBITED" in r for r in reasons)
            else "malformed"
        )
        raise ImageProviderError(kind, f"no image came back ({why}).{said}")
    return ImageCallResult(
        images=images,
        usage=_normalise_gemini_usage(body.get("usageMetadata")),
        finish_note=" ".join(texts)[:300] or None,
    )


async def call_gemini(
    section: Mapping[str, Any],
    request: ImageRequest,
    *,
    client: httpx.AsyncClient | None = None,
) -> ImageCallResult:
    """``n`` sequential ``generateContent`` calls (Gemini returns one image each)."""
    owns = client is None
    client = client or httpx.AsyncClient(
        timeout=httpx.Timeout(CALL_TIMEOUT_SECONDS, connect=15.0)
    )
    try:
        merged = ImageCallResult(images=[])
        wanted = max(1, int(request.n))
        for index in range(wanted):
            try:
                one = await _gemini_once(client, section, request)
            except ImageProviderError as exc:
                if not merged.images:
                    raise
                # Keep what was already made (and paid for); say what stopped.
                merged.finish_note = f"stopped after {index} of {wanted} images: {exc}"
                break
            merged.images.extend(one.images[:1])
            for key, value in one.usage.items():
                merged.usage[key] = merged.usage.get(key, 0) + value
            merged.finish_note = merged.finish_note or one.finish_note
        return merged
    finally:
        if owns:
            await client.aclose()


ADAPTERS = {"openai-images": call_openai_images, "gemini": call_gemini}
