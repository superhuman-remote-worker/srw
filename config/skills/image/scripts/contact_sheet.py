#!/usr/bin/env python3
"""A numbered contact sheet of images, for judging a set at a glance.

    python3 skills/image/scripts/contact_sheet.py output/set/*.png \\
        --out output/review/sheet-01.png [--columns 3] [--cell 512] [--per-sheet 9]

With more images than --per-sheet, several sheets are written
(sheet-01.png, sheet-02.png, …). Each image is fitted whole into its cell
and captioned "<number>. <file name>". 2×2 or 3×3 sheets keep every cell
large enough to judge faces and hands; open a candidate on its own to check.
Needs Pillow (in the workspace image).
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont, ImageOps

MAX_PIXELS = 40_000_000


def _font(size: int):
    try:
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


def _open(path: Path) -> Image.Image:
    with Image.open(path) as probe:
        if probe.width * probe.height > MAX_PIXELS:
            raise ValueError(f"{path} is larger than {MAX_PIXELS:,} pixels")
        image = ImageOps.exif_transpose(probe)
        image.load()
    return image.convert("RGBA") if "A" in image.getbands() else image.convert("RGB")


def sheet(paths: list[Path], start: int, *, columns: int | None, cell: int) -> Image.Image:
    count = len(paths)
    cols = max(1, min(columns or math.ceil(math.sqrt(count)), count))
    rows = math.ceil(count / cols)
    caption = max(18, cell // 12)
    pad = max(4, cell // 64)
    canvas = Image.new("RGB", (cols * (cell + pad) + pad, rows * (cell + caption + pad) + pad), "#1e1e2e")
    draw = ImageDraw.Draw(canvas)
    font = _font(max(12, caption - 6))
    for index, path in enumerate(paths):
        image = _open(path)
        image.thumbnail((cell, cell), Image.Resampling.LANCZOS)
        col, row = index % cols, index // cols
        x0 = pad + col * (cell + pad)
        y0 = pad + row * (cell + caption + pad)
        offset = (x0 + (cell - image.width) // 2, y0 + (cell - image.height) // 2)
        canvas.paste(image, offset, image if image.mode == "RGBA" else None)
        draw.text((x0, y0 + cell + 2), f"{start + index}. {path.name}"[:80], fill="#cdd6f4", font=font)
    return canvas


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("images", nargs="+")
    parser.add_argument("--out", required=True, help="sheet path; numbered when there are several")
    parser.add_argument("--columns", type=int)
    parser.add_argument("--cell", type=int, default=512)
    parser.add_argument("--per-sheet", type=int, default=9)
    args = parser.parse_args(argv)
    paths = [Path(p) for p in args.images]
    missing = [str(p) for p in paths if not p.is_file()]
    if missing:
        print(f"Error: not found: {', '.join(missing)}", file=sys.stderr)
        return 1
    cell = max(64, min(args.cell, 2048))
    per = max(1, min(args.per_sheet, 64))
    out = Path(args.out).with_suffix(".png")
    out.parent.mkdir(parents=True, exist_ok=True)
    chunks = [paths[i : i + per] for i in range(0, len(paths), per)]
    for number, chunk in enumerate(chunks, 1):
        target = out if len(chunks) == 1 else out.with_name(f"{out.stem}-{number:02d}.png")
        try:
            sheet(chunk, 1 + (number - 1) * per, columns=args.columns, cell=cell).save(target, optimize=True)
        except (OSError, ValueError) as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
        print(target)
    return 0


if __name__ == "__main__":
    sys.exit(main())
