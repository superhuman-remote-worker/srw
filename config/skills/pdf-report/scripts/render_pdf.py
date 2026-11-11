#!/usr/bin/env python3
"""Print an HTML file to PDF with the workspace's headless Chromium.

usage: python render_pdf.py INPUT.html OUTPUT.pdf [--landscape] [--letter]

Page size, margins, footer and page numbers come from the page's CSS
(@page). The script waits for fonts and images, then warns about defects a
reader would see: content wider than the page, images that did not load and
leftover placeholders. Exit code 0 means a PDF was written; warnings do not
change it, so read them.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

PLACEHOLDERS = re.compile(
    r"lorem ipsum|\{\{|\}\}|\bTODO\b|\bTBD\b|PLACEHOLDER|\bNaN\b|undefined"
)

LINT_JS = """
() => {
  const out = {overflow: [], broken: []};
  const limit = document.documentElement.clientWidth + 2;
  const wide = [];
  for (const el of document.querySelectorAll('body *')) {
    const r = el.getBoundingClientRect();
    if (r.width === 0 || r.height === 0) continue;
    if (r.right > limit) wide.push(el);
  }
  // Report the outermost offenders only; their children overflow with them.
  for (const el of wide) {
    if (wide.some(other => other !== el && other.contains(el))) continue;
    const cls = el.classList.length ? '.' + el.classList[0] : '';
    const text = (el.innerText || '').trim().slice(0, 40).replace(/\\s+/g, ' ');
    out.overflow.push(`${el.tagName.toLowerCase()}${cls} "${text}" runs ` +
      `${Math.round(el.getBoundingClientRect().right - limit)}px past the page edge`);
  }
  for (const img of document.images) {
    if (!img.complete || img.naturalWidth === 0) out.broken.push(img.getAttribute('src'));
  }
  out.text = document.body.innerText;
  return out;
}
"""


def chromium() -> str | None:
    return (
        shutil.which("agent-chromium")
        or shutil.which("chromium")
        or shutil.which("google-chrome")
    )


def render_with_playwright(
    src: Path, dst: Path, landscape: bool, letter: bool
) -> list[str]:
    from playwright.sync_api import sync_playwright

    warnings: list[str] = []
    with sync_playwright() as p:
        exe = chromium()
        browser = p.chromium.launch(executable_path=exe) if exe else p.chromium.launch()
        # Lay the page out at the printed text width so overflow shows up:
        # A4 minus the template's 17 mm side margins is 176 mm (665 px),
        # landscape 263 mm (994 px), Letter about 182 mm (688 px).
        width = 994 if landscape else 688 if letter else 665
        page = browser.new_page(viewport={"width": width, "height": 1100})
        page.goto(src.resolve().as_uri(), wait_until="networkidle")
        page.evaluate("document.fonts.ready")
        page.emulate_media(media="print")
        lint = page.evaluate(LINT_JS)
        warnings += [f"overflow: {item}" for item in lint["overflow"]]
        warnings += [f"broken image: {s}" for s in lint["broken"]]
        for match in sorted(
            set(m.group(0) for m in PLACEHOLDERS.finditer(lint["text"]))
        ):
            warnings.append(f"placeholder text left: {match!r}")
        options = dict(path=str(dst), print_background=True, prefer_css_page_size=True)
        if landscape:
            options["landscape"] = True
        if letter:
            options["format"] = "Letter"
        page.pdf(**options)
        browser.close()
    return warnings[:20]


def render_with_cli(src: Path, dst: Path) -> list[str]:
    exe = chromium()
    if not exe:
        raise SystemExit(
            "no Chromium found (agent-chromium is missing from this workspace)"
        )
    subprocess.run(
        [
            exe,
            "--headless",
            "--no-sandbox",
            "--disable-gpu",
            "--no-pdf-header-footer",
            "--virtual-time-budget=5000",
            f"--print-to-pdf={dst}",
            src.resolve().as_uri(),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return ["rendered without layout checks (Playwright unavailable)"]


def page_count(pdf: Path) -> str:
    try:
        from pypdf import PdfReader

        return str(len(PdfReader(str(pdf)).pages))
    except Exception:  # noqa: BLE001 — informational only
        return "?"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("input")
    ap.add_argument("output")
    ap.add_argument(
        "--landscape", action="store_true", help="force landscape (normally set in CSS)"
    )
    ap.add_argument(
        "--letter", action="store_true", help="US Letter when the CSS sets no size"
    )
    args = ap.parse_args()
    src, dst = Path(args.input), Path(args.output)
    if not src.is_file():
        print(f"no such file: {src}", file=sys.stderr)
        return 2
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        warnings = render_with_playwright(src, dst, args.landscape, args.letter)
    except ImportError:
        warnings = render_with_cli(src, dst)
    print(f"wrote {dst} ({page_count(dst)} pages, {dst.stat().st_size // 1024} KB)")
    for w in warnings:
        print(f"WARNING {w}")
    if not warnings:
        print("no layout warnings; now render the pages and look at them")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
