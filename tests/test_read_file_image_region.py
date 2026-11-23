"""read_file(region=...) and compose_images (image generation WP5).

A region is cropped at the original resolution after EXIF orientation and
then shown like any image; a text-only model gets the crop described, with
the region in the cache key; decoding is capped. compose_images builds a
labelled sheet on any tier and saves it as a PNG.
"""

from __future__ import annotations

import asyncio
import base64
import io
from unittest.mock import MagicMock

import pytest
from PIL import Image

from agent.core.workspace import WorkspaceManager
from agent.services.image_content import extract_image_tags
from agent.tools.context import ToolContext
from agent.tools.registry import load_tools
from tests._fs_backend import FilesystemTestBackend


def _workspace(tmp_path) -> WorkspaceManager:
    root = tmp_path / "ws"
    root.mkdir()
    workspace = WorkspaceManager(
        job_id="job-region", base_path=root, backend=FilesystemTestBackend(root)
    )
    workspace.initialize()
    return workspace


def _quadrants(width=400, height=200) -> Image.Image:
    """Red top-left, green top-right, blue bottom-left, white bottom-right."""
    image = Image.new("RGB", (width, height))
    half_w, half_h = width // 2, height // 2
    for box, color in (
        ((0, 0, half_w, half_h), (255, 0, 0)),
        ((half_w, 0, width, half_h), (0, 255, 0)),
        ((0, half_h, half_w, height), (0, 0, 255)),
        ((half_w, half_h, width, height), (255, 255, 255)),
    ):
        image.paste(color, box)
    return image


def _save(image: Image.Image, fmt="PNG", **kwargs) -> bytes:
    out = io.BytesIO()
    image.save(out, format=fmt, **kwargs)
    return out.getvalue()


def _tools(workspace, *, multimodal=True, max_edge=None):
    config = {"multimodal": multimodal}
    if max_edge:
        config["image_max_edge"] = max_edge
    context = ToolContext(workspace_manager=workspace, config=config)
    return {t.name: t for t in load_tools(["read_file", "compose_images"], context)}


def _shown(result: str) -> Image.Image:
    _text, images = extract_image_tags(result)
    assert len(images) == 1, result[:300]
    return Image.open(io.BytesIO(base64.b64decode(images[0].base64_data))).convert(
        "RGB"
    )


def _center(image: Image.Image) -> tuple[int, int, int]:
    return image.getpixel((image.width // 2, image.height // 2))


def _is_green(pixel) -> bool:
    r, g, b = pixel
    return g > 200 and r < 60 and b < 60


def test_a_region_is_cropped_at_full_resolution(tmp_path):
    workspace = _workspace(tmp_path)
    (tmp_path / "ws/q.png").write_bytes(_save(_quadrants()))
    read = _tools(workspace)["read_file"]
    result = read.invoke({"path": "q.png", "region": [50, 0, 100, 50]})
    crop = _shown(result)
    assert crop.size == (200, 100)
    assert _center(crop) == (0, 255, 0)  # the green top-right quarter, lossless
    assert "image/png" in result
    assert "Region: 50–100% across, 0–50% down of 400×200 px → 200×100 px" in result


@pytest.mark.parametrize("orientation", range(1, 9))
def test_regions_follow_the_exif_orientation(tmp_path, orientation):
    """The region is in the picture as it is shown, not as it is stored."""
    upright = _quadrants(400, 200)
    # Store the pixels so that applying ``orientation`` gives ``upright``.
    inverse = {
        1: lambda im: im,
        2: lambda im: im.transpose(Image.Transpose.FLIP_LEFT_RIGHT),
        3: lambda im: im.transpose(Image.Transpose.ROTATE_180),
        4: lambda im: im.transpose(Image.Transpose.FLIP_TOP_BOTTOM),
        5: lambda im: im.transpose(Image.Transpose.TRANSPOSE),
        6: lambda im: im.transpose(Image.Transpose.ROTATE_90),
        7: lambda im: im.transpose(Image.Transpose.TRANSVERSE),
        8: lambda im: im.transpose(Image.Transpose.ROTATE_270),
    }[orientation]
    stored = inverse(upright)
    exif = Image.Exif()
    exif[0x0112] = orientation
    workspace = _workspace(tmp_path)
    (tmp_path / "ws/photo.jpg").write_bytes(
        _save(stored, "JPEG", quality=95, exif=exif.tobytes())
    )
    read = _tools(workspace)["read_file"]
    crop = _shown(read.invoke({"path": "photo.jpg", "region": [0, 50, 50, 100]}))
    assert crop.size == (200, 100)
    r, g, b = _center(crop)
    assert b > 200 and r < 60 and g < 60  # the blue bottom-left quarter


def test_a_crop_goes_through_the_same_tier_note_as_a_whole_image(tmp_path):
    workspace = _workspace(tmp_path)
    (tmp_path / "ws/big.png").write_bytes(_save(_quadrants(3200, 1600)))
    read = _tools(workspace, max_edge=1568)["read_file"]
    whole = read.invoke({"path": "big.png"})
    assert "You see this at about 1568×784 (the file is 3200×1600)" in whole
    assert "region=[left, top, right, bottom]" in whole
    part = read.invoke({"path": "big.png", "region": [0, 0, 25, 25]})
    assert "You see this" not in part  # 800×400 fits: seen as it is
    small = _tools(workspace, max_edge=4096)["read_file"]
    assert "You see this" not in small.invoke({"path": "big.png"})


@pytest.mark.parametrize(
    "region, problem",
    [
        ([10, 10, 5, 50], "right > left"),
        ([0, 0, 120, 50], "from 0 to 100"),
        ([0, 0, 50], "four numbers"),
    ],
)
def test_a_bad_region_is_refused_with_the_rule(tmp_path, region, problem):
    workspace = _workspace(tmp_path)
    (tmp_path / "ws/q.png").write_bytes(_save(_quadrants()))
    result = _tools(workspace)["read_file"].invoke({"path": "q.png", "region": region})
    assert result.startswith("Error: cannot read a region of q.png")
    assert problem in result


def test_decoding_is_capped(tmp_path, monkeypatch):
    import agent.services.image_compose as compose

    monkeypatch.setattr(compose, "MAX_DECODE_PIXELS", 1000)
    workspace = _workspace(tmp_path)
    (tmp_path / "ws/q.png").write_bytes(_save(_quadrants()))
    result = _tools(workspace)["read_file"].invoke(
        {"path": "q.png", "region": [0, 0, 50, 50]}
    )
    assert "pixel limit" in result


def test_a_text_only_model_gets_the_crop_described_and_cached_per_region(
    tmp_path, monkeypatch
):
    from agent.services import description_cache as cache_module

    workspace = _workspace(tmp_path)
    (tmp_path / "ws/q.png").write_bytes(_save(_quadrants()))
    seen = []

    def describe(data, mime_type="image/png", query=None, job_id=None):
        image = Image.open(io.BytesIO(data)).convert("RGB")
        seen.append(image.size)
        return f"a {'green' if _is_green(_center(image)) else 'other'} block"

    helper = MagicMock()
    helper.describe_image_sync = describe
    monkeypatch.setattr(
        "agent.services.vision_helper.get_vision_helper", lambda: helper
    )
    monkeypatch.setattr(
        cache_module,
        "_description_cache",
        cache_module.DescriptionCache(tmp_path / "c"),
    )
    read = _tools(workspace, multimodal=False)["read_file"]
    first = read.invoke({"path": "q.png", "region": [50, 0, 100, 50]})
    assert "a green block" in first and seen == [(200, 100)]
    read.invoke({"path": "q.png", "region": [50, 0, 100, 50]})
    assert seen == [(200, 100)]  # served from the cache
    whole = read.invoke({"path": "q.png"})
    assert seen == [(200, 100), (400, 200)]  # another key: described again
    assert "a other block" in whole


def test_compose_images_saves_a_labelled_sheet_and_shows_it(tmp_path):
    workspace = _workspace(tmp_path)
    for name, color in (("a", (255, 0, 0)), ("b", (0, 0, 255)), ("c", (0, 255, 0))):
        (tmp_path / f"ws/{name}.png").write_bytes(
            _save(Image.new("RGB", (300, 300), color))
        )
    compose = _tools(workspace)["compose_images"]
    result = asyncio.run(
        compose.ainvoke(
            {
                "paths": ["a.png", "b.png", "c.png"],
                "path": "output/sheet.jpg",
                "labels": ["front", "side", "back"],
                "cell": 128,
            }
        )
    )
    assert "Saved a sheet of 3 image(s) as output/sheet.png" in result
    sheet = Image.open(tmp_path / "ws/output/sheet.png")
    assert sheet.format == "PNG" and sheet.width > 2 * 128
    assert _shown(result).size == sheet.size

    wrong = asyncio.run(
        compose.ainvoke(
            {"paths": ["a.png", "b.png"], "path": "x.png", "labels": ["one"]}
        )
    )
    assert wrong.startswith("Error: 1 labels for 2 images")
    missing = asyncio.run(compose.ainvoke({"paths": ["nope.png"], "path": "x.png"}))
    assert missing.startswith("Error: cannot read nope.png")
    again = asyncio.run(
        compose.ainvoke({"paths": ["a.png"], "path": "output/sheet.png"})
    )
    assert "already exists" in again
