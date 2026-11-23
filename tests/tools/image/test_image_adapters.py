"""The generate_image provider adapters (image generation WP4).

``openai-images`` serves OpenAI and the subscription proxy (same wire shape);
``gemini`` speaks native generateContent. One attempt, errors classified,
the key never in a message.
"""

from __future__ import annotations

import asyncio
import base64
import io
import json

import httpx
import pytest
from PIL import Image

from agent.tools.image_generation.adapters import (
    ImageProviderError,
    ImageRequest,
    call_gemini,
    call_openai_images,
    gemini_image_config,
    openai_size,
)


def _png(color=(200, 0, 0), size=(8, 8)) -> bytes:
    out = io.BytesIO()
    Image.new("RGB", size, color).save(out, format="PNG")
    return out.getvalue()


PNG = _png()
B64 = base64.b64encode(PNG).decode()


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _run(coro):
    return asyncio.run(coro)


SECTION = {
    "provider": "openai-images",
    "base_url": "http://proxy:8317/v1",
    "api_key": "sk-very-secret",
    "model": "gpt-image-2",
}


def test_generation_sends_json_and_reads_b64_and_usage():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "created": 1,
                "data": [{"b64_json": B64}, {"b64_json": B64}],
                "usage": {
                    "input_tokens": 50,
                    "input_tokens_details": {"text_tokens": 50, "image_tokens": 0},
                    "output_tokens": 400,
                },
            },
        )

    result = _run(
        call_openai_images(
            SECTION,
            ImageRequest(prompt="a fox", n=2, size="16:9", quality="low"),
            client=_client(handler),
        )
    )
    assert seen["url"] == "http://proxy:8317/v1/images/generations"
    assert seen["auth"] == "Bearer sk-very-secret"
    assert seen["body"] == {
        "model": "gpt-image-2",
        "prompt": "a fox",
        "n": 2,
        "size": "1664x944",
        "quality": "low",
    }
    assert "response_format" not in seen["body"]
    assert [img.data for img in result.images] == [PNG, PNG]
    assert result.usage == {
        "input_text_tokens": 50,
        "input_image_tokens": 0,
        "output_image_tokens": 400,
    }


def test_edits_send_references_as_data_urls_and_no_input_fidelity_on_2x():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"data": [{"b64_json": B64}]})

    _run(
        call_openai_images(
            SECTION,
            ImageRequest(prompt="same fox, waving", references=[(PNG, "image/png")]),
            client=_client(handler),
        )
    )
    assert seen["url"].endswith("/images/edits")
    assert seen["body"]["images"] == [{"image_url": "data:image/png;base64," + B64}]
    assert "input_fidelity" not in seen["body"]


@pytest.mark.parametrize(
    "status, body, kind",
    [
        (
            503,
            {
                "error": {
                    "message": "auth_not_found: no auth available (providers=codex)"
                }
            },
            "no_plan",
        ),
        (
            400,
            {"error": {"code": "moderation_blocked", "message": "rejected"}},
            "moderation",
        ),
        (401, {"error": {"message": "bad key"}}, "auth"),
        (429, {"error": {"message": "usage_limit_reached"}}, "quota"),
        (429, {"error": {"message": "slow down"}}, "rate_limit"),
        (502, {"error": {"message": "upstream"}}, "unavailable"),
    ],
)
def test_errors_are_classified_and_never_carry_the_key(status, body, kind):
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(status, json=body)

    with pytest.raises(ImageProviderError) as raised:
        _run(
            call_openai_images(
                SECTION, ImageRequest(prompt="x"), client=_client(handler)
            )
        )
    assert raised.value.kind == kind
    assert "sk-very-secret" not in str(raised.value)
    assert len(calls) == 1  # one attempt: no retry layer here


def test_no_image_data_is_a_failure():
    def handler(request):
        return httpx.Response(200, json={"data": [{"url": "https://x"}]})

    with pytest.raises(ImageProviderError) as raised:
        _run(
            call_openai_images(
                SECTION, ImageRequest(prompt="x"), client=_client(handler)
            )
        )
    assert raised.value.kind == "malformed"


def test_size_words_map_to_the_images_api():
    assert openai_size("portrait") == "1024x1536"
    assert openai_size("") is None
    assert openai_size("big please", "gpt-image-2") is None


@pytest.mark.parametrize(
    "size, model, expected",
    [
        # Inside the gpt-image-2.x bounds: sent as asked.
        ("2048x1152", "gpt-image-2.6-flare", "2048x1152"),
        ("1536x1024", "gpt-image-2", "1536x1024"),
        # Too few pixels (the live k3d 400, 2026-11-17): scaled up, same shape.
        ("512x512", "gpt-image-2.6-flare", "816x816"),
        # Too many pixels, or an aspect beyond 3:1: fitted.
        ("8000x8000", "gpt-image-2", "2880x2880"),
        ("3840x1000", "gpt-image-2", "3392x1136"),
        # Older models know only the three standard sizes.
        ("512x512", "gpt-image-1.5", "1024x1024"),
        ("1920x1080", "gpt-image-1.5", "1536x1024"),
        ("1080x1920", None, "1024x1536"),
        # A ratio gets its exact shape on 2.x (the k3d skill check: "16:9"
        # came back 3:2 and the model had to generate again).
        ("16:9", "gpt-image-2.6-flare", "1664x944"),
        ("9:16", "gpt-image-2", "944x1664"),
        ("square", "gpt-image-2", "1248x1248"),
        ("16:9", "gpt-image-1.5", "1536x1024"),
    ],
)
def test_sizes_the_model_would_refuse_are_fitted(size, model, expected):
    fitted = openai_size(size, model)
    assert fitted == expected
    if model and model.startswith("gpt-image-2"):
        width, height = (int(v) for v in fitted.split("x"))
        assert width % 16 == 0 and height % 16 == 0
        assert 655_360 <= width * height <= 8_294_400
        assert max(width, height) <= 3840
        assert max(width, height) / min(width, height) <= 3


GEMINI = {
    "provider": "gemini",
    "base_url": None,
    "api_key": "g-secret",
    "model": "gemini-nano-banana-2.1",
}


def _gemini_ok(data_b64=B64, thought=False):
    parts = []
    if thought:
        parts.append(
            {"thought": True, "inlineData": {"mimeType": "image/png", "data": "AAAA"}}
        )
    parts.append({"text": "here you go"})
    parts.append({"inlineData": {"mimeType": "image/png", "data": data_b64}})
    return {
        "candidates": [{"content": {"parts": parts}, "finishReason": "STOP"}],
        "usageMetadata": {
            "promptTokenCount": 30,
            "candidatesTokensDetails": [{"modality": "IMAGE", "tokenCount": 1120}],
            "thoughtsTokenCount": 100,
        },
    }


def test_gemini_calls_generate_content_skips_thoughts_and_repeats_for_n():
    seen = []

    def handler(request):
        seen.append(
            (
                str(request.url),
                request.headers.get("x-goog-api-key"),
                json.loads(request.content),
            )
        )
        return httpx.Response(200, json=_gemini_ok(thought=True))

    result = _run(
        call_gemini(
            GEMINI,
            ImageRequest(
                prompt="a lighthouse",
                n=2,
                size="1536x1024",
                references=[(PNG, "image/png")],
            ),
            client=_client(handler),
        )
    )
    assert len(seen) == 2
    url, key, body = seen[0]
    assert url == (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        "gemini-nano-banana-2.1:generateContent"
    )
    assert key == "g-secret"
    assert body["generationConfig"]["imageConfig"] == {
        "aspectRatio": "3:2",
        "imageSize": "2K",
    }
    assert body["contents"][0]["parts"][1]["inlineData"]["mimeType"] == "image/png"
    assert [img.data for img in result.images] == [PNG, PNG]
    assert result.usage == {
        "input_text_tokens": 60,
        "output_image_tokens": 2240,
        "output_text_tokens": 200,
    }


def test_gemini_200_without_an_image_is_a_named_failure():
    def handler(request):
        return httpx.Response(
            200,
            json={
                "candidates": [
                    {
                        "content": {"parts": [{"text": "I can't"}]},
                        "finishReason": "IMAGE_SAFETY",
                    }
                ]
            },
        )

    with pytest.raises(ImageProviderError) as raised:
        _run(call_gemini(GEMINI, ImageRequest(prompt="x"), client=_client(handler)))
    assert raised.value.kind == "blocked"
    assert "IMAGE_SAFETY" in str(raised.value)


def test_gemini_keeps_the_images_made_before_a_failure():
    replies = iter(
        [
            httpx.Response(200, json=_gemini_ok()),
            httpx.Response(503, json={"error": {"message": "busy"}}),
        ]
    )

    def handler(request):
        return next(replies)

    result = _run(
        call_gemini(GEMINI, ImageRequest(prompt="x", n=2), client=_client(handler))
    )
    assert len(result.images) == 1
    assert "stopped after 1 of 2" in (result.finish_note or "")


def test_gemini_image_config():
    assert gemini_image_config("16:9") == {"aspectRatio": "16:9"}
    assert gemini_image_config("portrait") == {"aspectRatio": "2:3"}
    assert gemini_image_config("4096x4096") == {"aspectRatio": "1:1", "imageSize": "4K"}
    assert gemini_image_config("") == {}
