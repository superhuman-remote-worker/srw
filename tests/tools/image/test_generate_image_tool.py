"""generate_image (image generation WP4): built only from a pinned image
section; saves files with provenance; one image or one numbered contact sheet
back to the model; paid rows reserve against the cap, subscription rows ask the
quota first; one attempt; metered as one audit row."""

from __future__ import annotations

import asyncio
import io
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from PIL import Image

from agent.core.workspace import WorkspaceManager
from agent.tools.context import ToolContext
from agent.tools.image_generation import tool as image_tool
from agent.tools.image_generation.adapters import (
    GeneratedImage,
    ImageCallResult,
    ImageProviderError,
)
from agent.tools.registry import load_tools
from tests._fs_backend import FilesystemTestBackend


def _png(color, size=(64, 48)) -> bytes:
    out = io.BytesIO()
    Image.new("RGB", size, color).save(out, format="PNG")
    return out.getvalue()


RED, BLUE = _png((220, 20, 20)), _png((20, 20, 220))

PAID = {
    "provider": "openai-images",
    "base_url": None,
    "api_key": "sk-secret",
    "model": "gpt-image-2.6-flare",
    "catalog_model": "gpt-image-2.6-flare",
    "ops": ["generate", "edit"],
    "billing": "api",
    "max_paid_images": 3,
}
PROXY = {
    **PAID,
    "model": "gpt-image-2",
    "catalog_model": "gpt-image-2",
    "billing": "subscription",
}


def _workspace(tmp_path) -> WorkspaceManager:
    root = tmp_path / "workspace"
    root.mkdir()
    workspace = WorkspaceManager(
        job_id="00000000-0000-0000-0000-0000000000aa",
        base_path=root,
        backend=FilesystemTestBackend(root),
    )
    workspace.initialize()
    return workspace


def _tool(workspace, section, *, multimodal=True, store=None):
    context = ToolContext(
        workspace_manager=workspace,
        config={"image": section, "multimodal": multimodal},
        postgres_db=store,
    )
    tools = load_tools(["generate_image"], context)
    return {t.name: t for t in tools}.get("generate_image")


def _adapter(images, usage=None, error=None):
    async def call(section, request):
        call.requests.append(request)
        if error is not None:
            raise error
        return ImageCallResult(
            images=[GeneratedImage(data, "image/png") for data in images],
            usage=usage or {"output_image_tokens": 1000},
        )

    call.requests = []
    return call


def _store(ok=True, used=1):
    store = MagicMock()
    store.reserve_image_generation = AsyncMock(return_value=(ok, used))
    store.release_image_generation = AsyncMock()
    return store


def test_no_image_section_means_no_tool(tmp_path):
    context = ToolContext(workspace_manager=_workspace(tmp_path), config={})
    assert load_tools(["generate_image"], context) == []


def test_one_image_is_saved_with_provenance_and_shown(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path)
    adapter = _adapter([RED])
    monkeypatch.setitem(image_tool.ADAPTERS, "openai-images", adapter)
    store = _store()
    tool = _tool(workspace, PAID, store=store)
    archived = MagicMock()
    with patch("agent.core.archiver.get_archiver", return_value=archived):
        out = asyncio.run(
            tool.ainvoke(
                {"prompt": "A red fox in snow", "path": "output/images/fox.png"}
            )
        )
    assert "output/images/fox.png" in out
    saved = (tmp_path / "workspace/output/images/fox.png").read_bytes()
    stamped = Image.open(io.BytesIO(saved))
    assert stamped.text["parameters"] == "A red fox in snow"
    assert json.loads(stamped.text["srw-provenance"])["model"] == "gpt-image-2.6-flare"
    # The pixels are untouched; only text chunks were added.
    assert stamped.tobytes() == Image.open(io.BytesIO(RED)).tobytes()
    provenance = json.loads(
        (tmp_path / "workspace/.provenance/output/images/fox.png.json").read_text()
    )
    assert provenance["prompt"] == "A red fox in snow"
    assert provenance["model"] == "gpt-image-2.6-flare"
    assert provenance["billing"] == "api"
    assert '<image_data mime_type="image/png">' in out
    assert adapter.requests[0].quality == "low"  # drafts by default
    store.reserve_image_generation.assert_awaited_once_with(
        "job", "00000000-0000-0000-0000-0000000000aa", 1, 3
    )
    kwargs = archived.archive.call_args.kwargs
    assert kwargs["call_type"] == "image_generation"
    meta = kwargs["auxiliary_metadata"]["image_generation"]
    assert meta["count"] == 1 and meta["billing"] == "api"
    assert meta["usage"] == {"output_image_tokens": 1000}
    assert "sk-secret" not in out


def test_several_images_come_back_as_one_numbered_sheet(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path)
    monkeypatch.setitem(image_tool.ADAPTERS, "openai-images", _adapter([RED, BLUE]))
    tool = _tool(workspace, PAID, store=_store())
    with patch("agent.core.archiver.get_archiver", return_value=None):
        out = asyncio.run(tool.ainvoke({"prompt": "Two foxes", "n": 2}))
    assert "output/images/two-foxes-1.png" in out and "two-foxes-2.png" in out
    assert out.count("<image_data") == 1  # one sheet, not two images
    assert "contact sheet" in out


def test_the_cap_refuses_before_any_provider_call(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path)
    adapter = _adapter([RED])
    monkeypatch.setitem(image_tool.ADAPTERS, "openai-images", adapter)
    tool = _tool(workspace, PAID, store=_store(ok=False, used=3))
    out = asyncio.run(tool.ainvoke({"prompt": "One more"}))
    assert out.startswith("Error: no image was generated")
    assert "3 of its 3 paid images" in out and "0 left" in out
    assert "have an admin raise the cap" in out
    assert adapter.requests == []


def test_a_request_larger_than_what_is_left_says_how_many_are_left(
    tmp_path, monkeypatch
):
    workspace = _workspace(tmp_path)
    monkeypatch.setitem(image_tool.ADAPTERS, "openai-images", _adapter([RED]))
    tool = _tool(workspace, PAID, store=_store(ok=False, used=2))
    out = asyncio.run(tool.ainvoke({"prompt": "Two more", "n": 2}))
    assert "2 of its 3 paid images" in out and "1 left" in out
    assert "asked for 2" in out and "at most 1" in out


def test_a_refused_call_gives_the_reservation_back_and_is_not_retried(
    tmp_path, monkeypatch
):
    workspace = _workspace(tmp_path)
    adapter = _adapter([], error=ImageProviderError("auth", "HTTP 401: bad key"))
    monkeypatch.setitem(image_tool.ADAPTERS, "openai-images", adapter)
    store = _store()
    tool = _tool(workspace, PAID, store=store)
    out = asyncio.run(tool.ainvoke({"prompt": "Something refused"}))
    assert "(auth)" in out
    assert len(adapter.requests) == 1
    store.release_image_generation.assert_awaited_once()


@pytest.mark.parametrize("kind", ["moderation", "timeout", "unavailable", "blocked"])
def test_a_call_that_may_have_been_billed_stays_counted(tmp_path, monkeypatch, kind):
    workspace = _workspace(tmp_path)
    adapter = _adapter([], error=ImageProviderError(kind, "it failed"))
    monkeypatch.setitem(image_tool.ADAPTERS, "openai-images", adapter)
    store = _store()
    tool = _tool(workspace, PAID, store=store)
    out = asyncio.run(tool.ainvoke({"prompt": "x"}))
    assert f"({kind})" in out
    assert len(adapter.requests) == 1  # never retried
    store.release_image_generation.assert_not_awaited()


def test_not_bound_when_nobody_can_look_at_the_result(tmp_path):
    helper = MagicMock()
    helper.api_key = ""
    (tmp_path / "b").mkdir()
    with patch("agent.services.vision_helper.get_vision_helper", return_value=helper):
        assert _tool(_workspace(tmp_path), PAID, multimodal=False) is None
        helper.api_key = "sk-vision"
        assert _tool(_workspace(tmp_path / "b"), PAID, multimodal=False) is not None


def test_the_subscription_lane_asks_the_quota_and_skips_the_cap(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path)
    adapter = _adapter([RED, BLUE])
    monkeypatch.setitem(image_tool.ADAPTERS, "openai-images", adapter)
    store = _store()
    tool = _tool(workspace, PROXY, store=store)
    monkeypatch.setattr(
        image_tool, "_proxy_quota_verdict", AsyncMock(return_value={"allowed": True})
    )
    with patch("agent.core.archiver.get_archiver", return_value=None):
        out = asyncio.run(tool.ainvoke({"prompt": "Proxy foxes", "n": 4}))
    assert "subscription" in out
    assert adapter.requests[0].n == 2  # at most 2 per call on the proxy
    store.reserve_image_generation.assert_not_awaited()


def test_a_full_subscription_refuses_with_the_reason(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path)
    adapter = _adapter([RED])
    monkeypatch.setitem(image_tool.ADAPTERS, "openai-images", adapter)
    tool = _tool(workspace, PROXY, store=_store())
    monkeypatch.setattr(
        image_tool,
        "_proxy_quota_verdict",
        AsyncMock(
            return_value={
                "allowed": False,
                "reason": "window_at_threshold",
                "threshold_percent": 80,
                "windows": [{"window": "primary", "used_percent": 91}],
            }
        ),
    )
    out = asyncio.run(tool.ainvoke({"prompt": "Proxy fox"}))
    assert "at or above the 80% image threshold" in out
    assert "primary window 91% used" in out
    assert adapter.requests == []


def test_reference_images_are_read_from_the_workspace_and_fitted(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path)
    big = _png((0, 200, 0), size=(3000, 1500))
    (tmp_path / "workspace/sheet.png").write_bytes(big)
    adapter = _adapter([RED])
    monkeypatch.setitem(image_tool.ADAPTERS, "openai-images", adapter)
    tool = _tool(workspace, PAID, store=_store())
    with patch("agent.core.archiver.get_archiver", return_value=None):
        asyncio.run(
            tool.ainvoke(
                {"prompt": "Same character, waving", "reference_images": ["sheet.png"]}
            )
        )
    data, mime = adapter.requests[0].references[0]
    assert max(Image.open(io.BytesIO(data)).size) == 2048
    missing = asyncio.run(
        tool.ainvoke({"prompt": "x", "reference_images": ["nope.png"]})
    )
    assert missing.startswith("Error: cannot use reference image nope.png")


def test_a_text_only_model_gets_a_description_marked_unverified(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path)
    monkeypatch.setitem(image_tool.ADAPTERS, "openai-images", _adapter([RED]))
    helper = MagicMock()
    helper.api_key = "sk-vision"
    helper.describe_image = AsyncMock(return_value="A red square.")
    with patch("agent.services.vision_helper.get_vision_helper", return_value=helper):
        tool = _tool(workspace, PAID, multimodal=False, store=_store())
    with (
        patch("agent.core.archiver.get_archiver", return_value=None),
        patch("agent.services.vision_helper.get_vision_helper", return_value=helper),
    ):
        out = asyncio.run(tool.ainvoke({"prompt": "Red"}))
    assert "<image_data" not in out
    assert "unverified" in out and "A red square." in out


def test_an_existing_file_is_kept_unless_overwrite(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path)
    (tmp_path / "workspace/output/images").mkdir(parents=True)
    (tmp_path / "workspace/output/images/fox.png").write_bytes(BLUE)
    monkeypatch.setitem(image_tool.ADAPTERS, "openai-images", _adapter([RED]))
    tool = _tool(workspace, PAID, store=_store())
    with patch("agent.core.archiver.get_archiver", return_value=None):
        out = asyncio.run(
            tool.ainvoke({"prompt": "fox", "path": "output/images/fox.png"})
        )
    assert "could not be saved" in out
    assert (tmp_path / "workspace/output/images/fox.png").read_bytes() == BLUE


@pytest.mark.parametrize(
    "path, prompt, count, expected",
    [
        ("", "A fox!", 1, ["output/images/a-fox.png"]),
        ("art/", "Fox", 2, ["art/fox-1.png", "art/fox-2.png"]),
        ("art/hero.jpg", "x", 1, ["art/hero.jpg"]),
    ],
)
def test_target_paths(path, prompt, count, expected):
    assert image_tool._target_paths(path, prompt, count, "image/png") == expected
