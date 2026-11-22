"""Regression-guard tests for src/services/vision_helper.py.

These do not exercise a real network call. Instead they monkeypatch the
underlying `AsyncOpenAI` client to capture the request body and verify
that the chat-completions payload matches the documented multimodal
schema.

The point is to lock in the wire shape so future refactors of the
shared `image_content` utility cannot silently change what `vision_helper`
sends to its side-channel vision model.
"""
# ruff: noqa: F811  (the imported fixture is requested by name as a test argument)

from __future__ import annotations

import asyncio
import base64
from unittest.mock import AsyncMock, MagicMock

import pytest

from shared.tls_pinning import UNREADABLE_PIN, CertificateChangedError, TlsPin
from tests.tls_pin_fixtures import tls_certs, tls_server  # noqa: F401

from agent.services.vision_helper import (
    VISION_FAILURE_PREFIX,
    VisionHelper,
    vision_failed,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_helper_with_capture():
    """Return (helper, captured_calls) where captured_calls is a list of
    the kwargs that were passed to ``client.chat.completions.create``.

    The OpenAI client is fully mocked — no network access.
    """
    helper = VisionHelper.__new__(VisionHelper)
    helper.model = "test-vision-model"
    helper.timeout = 60

    captured_calls: list[dict] = []

    async def _fake_create(**kwargs):
        captured_calls.append(kwargs)
        # Build a minimal response object the helper can read.
        response = MagicMock()
        choice = MagicMock()
        choice.message.content = "fake description"
        response.choices = [choice]
        return response

    fake_client = MagicMock()
    fake_client.chat.completions.create = AsyncMock(side_effect=_fake_create)
    helper.client = fake_client

    # The archiver path is fire-and-forget; stub it so we don't touch
    # any orchestrator/database modules.
    helper._archive_vision_call = MagicMock()

    return helper, captured_calls


# =============================================================================
# Tests
# =============================================================================


class TestVisionHelperContentConstruction:
    def test_describe_image_uses_shared_content_block_shape(self):
        helper, captured = _make_helper_with_capture()
        bytes_in = b"\x89PNG\r\n\x1a\n_fake_png_body_"
        b64_in = base64.b64encode(bytes_in).decode()

        result = asyncio.run(
            helper.describe_image(image_data=bytes_in, mime_type="image/png")
        )

        assert result == "fake description"
        assert len(captured) == 1
        kwargs = captured[0]
        assert kwargs["model"] == "test-vision-model"
        msg = kwargs["messages"][0]
        assert msg["role"] == "user"
        # First content part: text prompt
        assert msg["content"][0]["type"] == "text"
        # Second content part: image_url block built by shared utility
        assert msg["content"][1] == {
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{b64_in}"},
        }

    def test_describe_image_accepts_already_b64_input(self):
        """Caller may pass a pre-encoded base64 string instead of bytes."""
        helper, captured = _make_helper_with_capture()
        b64_in = base64.b64encode(b"already-encoded").decode()

        asyncio.run(helper.describe_image(image_data=b64_in, mime_type="image/jpeg"))

        msg = captured[0]["messages"][0]
        assert msg["content"][1] == {
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{b64_in}"},
        }

    def test_describe_document_page_uses_shared_content_block_shape(self):
        helper, captured = _make_helper_with_capture()
        bytes_in = b"\x89PNG\r\n\x1a\nfake-page-body"
        b64_in = base64.b64encode(bytes_in).decode()

        result = asyncio.run(
            helper.describe_document_page(
                page_image=bytes_in,
                page_num=4,
                mime_type="image/png",
            )
        )

        assert result == "fake description"
        msg = captured[0]["messages"][0]
        assert msg["content"][0]["type"] == "text"
        assert "page" in msg["content"][0]["text"].lower()
        assert msg["content"][1] == {
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{b64_in}"},
        }


@pytest.mark.parametrize(
    "mime",
    ["image/png", "image/jpeg", "image/webp"],
)
def test_data_url_mime_type_propagates(mime):
    helper, captured = _make_helper_with_capture()
    asyncio.run(helper.describe_image(image_data=b"abcd", mime_type=mime))
    url = captured[0]["messages"][0]["content"][1]["image_url"]["url"]
    assert url.startswith(f"data:{mime};base64,")


# =============================================================================
# Hardening (image_generation.md WP1)
# =============================================================================


def _png(size):
    import io

    from PIL import Image

    out = io.BytesIO()
    Image.new("RGB", size, (10, 120, 200)).save(out, format="PNG")
    return out.getvalue()


def test_a_large_image_is_downscaled_to_the_max_edge():
    import io

    from PIL import Image

    helper, captured = _make_helper_with_capture()
    helper.max_edge = 1568
    asyncio.run(
        helper.describe_image(image_data=_png((4000, 2000)), mime_type="image/png")
    )
    url = captured[0]["messages"][0]["content"][1]["image_url"]["url"]
    sent = Image.open(io.BytesIO(base64.b64decode(url.split(",", 1)[1])))
    assert max(sent.size) == 1568

    small = _png((800, 600))
    asyncio.run(helper.describe_image(image_data=small, mime_type="image/png"))
    url = captured[1]["messages"][0]["content"][1]["image_url"]["url"]
    assert base64.b64decode(url.split(",", 1)[1]) == small  # fits: unchanged


def test_a_failure_says_the_image_was_not_seen():
    helper, _captured = _make_helper_with_capture()
    helper.client.chat.completions.create = AsyncMock(side_effect=RuntimeError("503"))
    result = asyncio.run(helper.describe_image(image_data=b"abcd"))
    assert result.startswith(VISION_FAILURE_PREFIX)
    assert "RuntimeError: 503" in result
    assert vision_failed(result) and not vision_failed("A blue square.")
    page = asyncio.run(helper.describe_document_page(page_image=b"abcd", page_num=2))
    assert page.startswith(VISION_FAILURE_PREFIX) and "page 2" in page


def test_token_usage_is_archived_for_metering(monkeypatch):
    helper, _captured = _make_helper_with_capture()
    response = MagicMock()
    response.choices = [MagicMock()]
    response.choices[0].message.content = "a cat"
    response.usage = MagicMock(
        prompt_tokens=812, completion_tokens=40, total_tokens=852
    )
    helper.client.chat.completions.create = AsyncMock(return_value=response)
    del helper._archive_vision_call  # use the real one
    archiver = MagicMock()
    monkeypatch.setattr("agent.core.archiver.get_archiver", lambda: archiver)
    asyncio.run(helper.describe_image(image_data=b"abcd", job_id="job-1"))
    sent = archiver.archive.call_args.kwargs["response"]
    assert sent.response_metadata["token_usage"] == {
        "prompt_tokens": 812,
        "completion_tokens": 40,
        "total_tokens": 852,
    }


def test_read_file_never_caches_a_failed_description(tmp_path, monkeypatch):
    from agent.core.workspace import WorkspaceManager
    from agent.services import description_cache as cache_module
    from agent.tools.context import ToolContext
    from agent.tools.registry import load_tools
    from tests._fs_backend import FilesystemTestBackend

    root = tmp_path / "ws"
    root.mkdir()
    workspace = WorkspaceManager(
        job_id="job-vision", base_path=root, backend=FilesystemTestBackend(root)
    )
    workspace.initialize()
    (root / "pic.png").write_bytes(_png((40, 30)))
    context = ToolContext(workspace_manager=workspace, config={"multimodal": False})
    read_file = {t.name: t for t in load_tools(["read_file"], context)}["read_file"]

    answers = iter([RuntimeError("boom"), "A small blue rectangle."])

    async def describe(*_args, **_kwargs):
        answer = next(answers)
        if isinstance(answer, Exception):
            return f"{VISION_FAILURE_PREFIX}: the vision model could not describe it]"
        return answer

    helper = MagicMock()
    helper.describe_image_sync = lambda *a, **k: asyncio.run(describe())
    monkeypatch.setattr(
        "agent.services.vision_helper.get_vision_helper", lambda: helper
    )
    monkeypatch.setattr(
        cache_module,
        "_description_cache",
        cache_module.DescriptionCache(tmp_path / "vision-cache"),
    )

    first = read_file.invoke({"path": "pic.png"})
    assert VISION_FAILURE_PREFIX in first
    second = read_file.invoke({"path": "pic.png"})
    assert "A small blue rectangle." in second
    assert "<untrusted_image_content" in second  # fenced as third-party text


# =============================================================================
# Certificate pinning (A5)
# =============================================================================


def _pin(path) -> TlsPin:
    return TlsPin.from_pem(path.read_text())


def _caused_by(exc, cls) -> bool:
    seen: set[int] = set()
    cur = exc
    while cur is not None and id(cur) not in seen:
        if isinstance(cur, cls):
            return True
        seen.add(id(cur))
        cur = cur.__cause__ or cur.__context__
    return False


@pytest.fixture
def vision_env(monkeypatch):
    for name in (
        "VISION_BASE_URL",
        "VISION_API_KEY",
        "VISION_MODEL",
        "VISION_SERVED_MODEL",
        "VISION_TLS_PINNED_CERT",
        "VISION_MAX_EDGE",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-openai")
    return monkeypatch


class TestVisionPin:
    @pytest.mark.asyncio
    async def test_describes_through_the_pin(self, vision_env, tls_certs):
        with tls_server(tls_certs.leaf, tls_certs.leaf_key) as server:
            vision_env.setenv("VISION_BASE_URL", server.url)
            vision_env.setenv("VISION_API_KEY", "k")
            vision_env.setenv("VISION_TLS_PINNED_CERT", _pin(tls_certs.leaf).wire)
            helper = VisionHelper()
            text = await helper.describe_image(b"\x89PNG", "image/png")
            await helper.client.close()
        assert text == "ok"
        assert server.requests[0]["path"] == "/v1/chat/completions"

    @pytest.mark.asyncio
    async def test_swapped_certificate_sends_nothing(self, vision_env, tls_certs):
        with tls_server(tls_certs.swap, tls_certs.swap_key) as server:
            vision_env.setenv("VISION_BASE_URL", server.url)
            vision_env.setenv("VISION_API_KEY", "secret")
            vision_env.setenv("VISION_TLS_PINNED_CERT", _pin(tls_certs.leaf).wire)
            helper = VisionHelper()
            with pytest.raises(Exception) as exc:
                await helper.client.chat.completions.create(
                    model="m", messages=[{"role": "user", "content": "x"}]
                )
            text = await helper.describe_image(b"\x89PNG", "image/png")
            await helper.client.close()
        assert _caused_by(exc.value, CertificateChangedError)
        assert server.requests == []
        # the failure text names the change, not just "Connection error."
        assert vision_failed(text) and "TLS certificate changed" in text
        assert "An admin must re-trust it in Admin" in text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [UNREADABLE_PIN, "not-base64!"])
    async def test_a_broken_pin_makes_vision_unavailable(self, vision_env, value):
        vision_env.setenv("VISION_BASE_URL", "https://vision.example/v1")
        vision_env.setenv("VISION_API_KEY", "k")
        vision_env.setenv("VISION_TLS_PINNED_CERT", value)
        helper = VisionHelper()
        assert helper.api_key == ""  # "can the agent see?" asks this
        text = await helper.describe_image(b"\x89PNG", "image/png")
        assert vision_failed(text) and "VISION_TLS_PINNED_CERT" in text
        text = await helper.describe_document_page(b"\x89PNG", 1)
        assert vision_failed(text) and "VISION_TLS_PINNED_CERT" in text

    @pytest.mark.asyncio
    async def test_a_pin_on_the_default_url_makes_vision_unavailable(
        self, vision_env, tls_certs
    ):
        vision_env.setenv("VISION_TLS_PINNED_CERT", _pin(tls_certs.leaf).wire)
        helper = VisionHelper()
        assert helper.api_key == ""
        assert vision_failed(await helper.describe_image(b"\x89PNG", "image/png"))


class TestGetVisionHelperRebuilds:
    def test_rebuilt_when_the_vision_env_changes(self, vision_env, tls_certs):
        import agent.services.vision_helper as mod

        vision_env.setattr(mod, "_vision_helper", None)
        vision_env.setenv("VISION_API_KEY", "k")
        vision_env.setenv("VISION_BASE_URL", "https://a.example/v1")
        first = mod.get_vision_helper()
        assert mod.get_vision_helper() is first
        vision_env.setenv("VISION_BASE_URL", "https://b.example/v1")
        second = mod.get_vision_helper()
        assert second is not first
        assert mod.get_vision_helper() is second
        vision_env.setenv("VISION_TLS_PINNED_CERT", _pin(tls_certs.leaf).wire)
        third = mod.get_vision_helper()
        assert third is not second
        vision_env.setenv("VISION_TLS_PINNED_CERT", _pin(tls_certs.swap).wire)
        assert mod.get_vision_helper() is not third
