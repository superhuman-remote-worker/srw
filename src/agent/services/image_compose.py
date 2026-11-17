"""Compose several images into one: numbered contact sheets and grids.

Used by ``generate_image`` (several images come back to the model as one
numbered sheet, so a batch costs one image in context, not N) and by the
``compose_images`` tool (contact sheets and character sheets on every tier,
including the shell-less ``virtual`` one). Pillow only; runs in the agent
process. Decoding is capped (:data:`MAX_DECODE_PIXELS`) so a hostile or huge
file cannot exhaust the pod's memory.

Design: knowledge-base/knowledge/features/image_generation.md §3.4, §5 WP4/WP5.
"""

from __future__ import annotations

import io
import math
from dataclasses import dataclass
from typing import Sequence

from PIL import Image, ImageDraw, ImageFont, ImageOps

#: Largest image (width × height) this module decodes. The orchestrator's
#: Canvas guard uses 40 MP; generated images are at most ~8.3 MP.
MAX_DECODE_PIXELS = 40_000_000

#: Default cell edge of a contact sheet. 3×3 at 512 px keeps every cell
#: legible inside one downscaled image the model sees.
DEFAULT_CELL = 512
MAX_CELLS = 64


class ImageComposeError(ValueError):
    """An input could not be decoded or the request is out of bounds."""


@dataclass(frozen=True)
class SheetTile:
    data: bytes
    label: str


def open_image(data: bytes) -> Image.Image:
    """Decode ``data`` (orientation fixed, RGBA/RGB), refusing oversize images."""
    try:
        probe = Image.open(io.BytesIO(data))
        width, height = probe.size
    except Exception as exc:  # noqa: BLE001 - Pillow raises many types
        raise ImageComposeError(f"not a readable image ({type(exc).__name__})") from exc
    if width * height > MAX_DECODE_PIXELS:
        raise ImageComposeError(
            f"image is {width}×{height} px, above the {MAX_DECODE_PIXELS:,}-pixel limit"
        )
    try:
        image = ImageOps.exif_transpose(probe)
        image.load()
    except Exception as exc:  # noqa: BLE001
        raise ImageComposeError(f"not a readable image ({type(exc).__name__})") from exc
    if image.mode not in ("RGB", "RGBA"):
        image = image.convert("RGBA" if "A" in image.getbands() else "RGB")
    return image


def _font(size: int) -> ImageFont.ImageFont:
    try:
        return ImageFont.load_default(size=size)
    except TypeError:  # older Pillow without the size argument
        return ImageFont.load_default()


def contact_sheet(
    tiles: Sequence[SheetTile],
    *,
    columns: int | None = None,
    cell: int = DEFAULT_CELL,
    background: str = "#1e1e2e",
    numbered: bool = True,
) -> bytes:
    """A grid of ``tiles`` with a number and label under each, as PNG bytes.

    Each image is fitted whole into its cell (never cropped). ``columns``
    defaults to a near-square grid.
    """
    if not tiles:
        raise ImageComposeError("no images to compose")
    if len(tiles) > MAX_CELLS:
        raise ImageComposeError(f"at most {MAX_CELLS} images per sheet")
    cell = max(64, min(int(cell), 2048))
    count = len(tiles)
    cols = columns or math.ceil(math.sqrt(count))
    cols = max(1, min(cols, count))
    rows = math.ceil(count / cols)
    caption_h = max(18, cell // 12)
    pad = max(4, cell // 64)
    sheet_w = cols * (cell + pad) + pad
    sheet_h = rows * (cell + caption_h + pad) + pad
    sheet = Image.new("RGB", (sheet_w, sheet_h), background)
    draw = ImageDraw.Draw(sheet)
    font = _font(max(12, caption_h - 6))
    for index, tile in enumerate(tiles):
        image = open_image(tile.data)
        image.thumbnail((cell, cell), Image.Resampling.LANCZOS)
        col, row = index % cols, index // cols
        x0 = pad + col * (cell + pad)
        y0 = pad + row * (cell + caption_h + pad)
        offset = (x0 + (cell - image.width) // 2, y0 + (cell - image.height) // 2)
        if image.mode == "RGBA":
            sheet.paste(image, offset, image)
        else:
            sheet.paste(image, offset)
        caption = f"{index + 1}. {tile.label}" if numbered else tile.label
        draw.text((x0, y0 + cell + 2), caption[:80], fill="#cdd6f4", font=font)
    out = io.BytesIO()
    sheet.save(out, format="PNG", optimize=True)
    return out.getvalue()


def image_size(data: bytes) -> tuple[int, int] | None:
    """Width and height of encoded image bytes, or None when unreadable."""
    try:
        return Image.open(io.BytesIO(data)).size
    except Exception:  # noqa: BLE001
        return None


def sniff_mime(data: bytes) -> str | None:
    """The image MIME type by content (never by the name a server gave it)."""
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    return None


def fit_reference(data: bytes, *, max_edge: int = 2048) -> tuple[bytes, str]:
    """A reference image re-encoded to fit ``max_edge`` (PNG keeps alpha).

    Providers cap request sizes (Gemini: 20 MB per request) and the proxy
    buffers whole bodies, so references are never sent at full resolution.
    """
    image = open_image(data)
    if max(image.size) > max_edge:
        image.thumbnail((max_edge, max_edge), Image.Resampling.LANCZOS)
    out = io.BytesIO()
    if image.mode == "RGBA":
        image.save(out, format="PNG", optimize=True)
        return out.getvalue(), "image/png"
    image.save(out, format="JPEG", quality=90)
    return out.getvalue(), "image/jpeg"


_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def with_png_text(data: bytes, entries: dict[str, str]) -> bytes:
    """``data`` with UTF-8 ``iTXt`` chunks added before ``IEND``.

    The A1111/ComfyUI convention for keeping the prompt inside the file (a
    ``parameters`` chunk is what their readers show). The pixels are not
    re-encoded. Anything that is not a well-formed PNG comes back unchanged.
    """
    if not data.startswith(_PNG_SIGNATURE) or not entries:
        return data
    import struct
    import zlib

    position = len(_PNG_SIGNATURE)
    iend_at = None
    while position + 8 <= len(data):
        (length,) = struct.unpack(">I", data[position : position + 4])
        kind = data[position + 4 : position + 8]
        if kind == b"IEND":
            iend_at = position
            break
        position += 12 + length
    if iend_at is None:
        return data
    chunks = []
    for keyword, text in entries.items():
        key = keyword.encode("latin-1", "replace")[:79]
        if not key:
            continue
        body = key + b"\x00\x00\x00" + b"\x00" + b"\x00" + text.encode("utf-8")
        chunk_type = b"iTXt"
        crc = zlib.crc32(chunk_type + body) & 0xFFFFFFFF
        chunks.append(
            struct.pack(">I", len(body)) + chunk_type + body + struct.pack(">I", crc)
        )
    return data[:iend_at] + b"".join(chunks) + data[iend_at:]
