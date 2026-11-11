#!/usr/bin/env python3
"""Render a document's pages to PNG so you can look at them with read_file.

usage: python render_pages.py FILE [--out DIR] [--dpi N] [--first N --last N]

FILE is a PDF, or a .docx/.pptx/.xlsx/.odt/.html which is converted to PDF
first (LibreOffice for Office files, Chromium for HTML). Writes page-1.png …
and sheet.png (up to 12 pages side by side, for the overall look) into DIR
(default: a folder next to the file) and prints the paths.
"""

from __future__ import annotations

import argparse
import math
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

OFFICE = {
    ".docx",
    ".doc",
    ".pptx",
    ".ppt",
    ".xlsx",
    ".xls",
    ".odt",
    ".odp",
    ".ods",
    ".rtf",
}


def to_pdf(src: Path, tmp: Path) -> Path:
    suffix = src.suffix.lower()
    if suffix == ".pdf":
        return src
    if suffix in OFFICE:
        # A throwaway profile: a second soffice cannot share the default one.
        profile = tmp / "lo-profile"
        subprocess.run(
            [
                "soffice",
                f"-env:UserInstallation={profile.as_uri()}",
                "--headless",
                "--convert-to",
                "pdf",
                "--outdir",
                str(tmp),
                str(src),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=180,
        )
        return tmp / (src.stem + ".pdf")
    if suffix in {".html", ".htm"}:
        out = tmp / (src.stem + ".pdf")
        script = Path(__file__).with_name("render_pdf.py")
        subprocess.run(
            [sys.executable, str(script), str(src), str(out)],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        return out
    raise SystemExit(f"cannot render {suffix} files; give a PDF, Office or HTML file")


def contact_sheet(pages: list[Path], out: Path) -> None:
    from PIL import Image, ImageDraw

    pages = pages[:12]
    thumbs = [Image.open(p).convert("RGB") for p in pages]
    width = 360
    thumbs = [t.resize((width, int(t.height * width / t.width))) for t in thumbs]
    cols = min(len(thumbs), 4)
    rows = math.ceil(len(thumbs) / cols)
    cell_h = max(t.height for t in thumbs)
    gap = 16
    sheet = Image.new(
        "RGB", (cols * (width + gap) + gap, rows * (cell_h + gap + 14) + gap), "#9aa1ad"
    )
    draw = ImageDraw.Draw(sheet)
    for i, t in enumerate(thumbs):
        x = gap + (i % cols) * (width + gap)
        y = gap + (i // cols) * (cell_h + gap + 14)
        sheet.paste(t, (x, y + 14))
        draw.text((x, y), f"page {i + 1}", fill="#ffffff")
    sheet.save(out)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("file")
    ap.add_argument(
        "--out", help="output folder (default: <file>.pages/ next to the file)"
    )
    ap.add_argument(
        "--dpi", type=int, default=80, help="80 to check layout, 150 for small text"
    )
    ap.add_argument("--first", type=int)
    ap.add_argument("--last", type=int)
    args = ap.parse_args()
    src = Path(args.file)
    if not src.is_file():
        print(f"no such file: {src}", file=sys.stderr)
        return 2
    if not shutil.which("pdftoppm"):
        print("pdftoppm is missing (poppler-utils)", file=sys.stderr)
        return 2
    out = Path(args.out) if args.out else src.with_name(src.name + ".pages")
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("page-*.png"):
        old.unlink()
    with tempfile.TemporaryDirectory() as tmp:
        pdf = to_pdf(src, Path(tmp))
        cmd = ["pdftoppm", "-png", "-r", str(args.dpi)]
        if args.first:
            cmd += ["-f", str(args.first)]
        if args.last:
            cmd += ["-l", str(args.last)]
        subprocess.run(cmd + [str(pdf), str(out / "page")], check=True)
    pages = sorted(out.glob("page-*.png"), key=lambda p: int(p.stem.split("-")[-1]))
    if not pages:
        print("no pages rendered", file=sys.stderr)
        return 1
    # pdftoppm pads numbers to the page count's width; normalise to page-N.png.
    renamed = []
    for p in pages:
        target = out / f"page-{int(p.stem.split('-')[-1])}.png"
        if p != target:
            p.rename(target)
        renamed.append(target)
    for p in renamed:
        print(p)
    if len(renamed) > 1:
        contact_sheet(renamed, out / "sheet.png")
        print(out / "sheet.png")
    print(
        f"{len(renamed)} page(s); read_file the sheet first, then pages to check closely"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
