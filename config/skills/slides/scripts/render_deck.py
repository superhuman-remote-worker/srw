#!/usr/bin/env python3
"""Print an HTML slide deck to PDF (one slide per page) and check each slide.

usage: python render_deck.py DECK.html OUTPUT.pdf [--no-pages]

Every <section class="slide"> must fit 1280 x 720 px. When the page does not
size its slides that way (deck.css not linked), the house deck.css is applied
so the PDF still comes out as 16:9 slides. The script reports slides whose
content is cut off, slides with more than 60 words, images that did not load
and leftover placeholders, checks the PDF has one 16:9 page per slide, then
renders the pages under .render/ and prints the read_file command to look at
them.
"""

from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
HOUSE_CSS = HERE.parent / "assets" / "deck.css"

PLACEHOLDERS = re.compile(
    r"lorem ipsum|\{\{|\}\}|\bTODO\b|\bTBD\b|PLACEHOLDER|\bNaN\b|undefined"
)

SIZED_JS = """
() => {
  const slides = [...document.querySelectorAll('.slide')];
  return {count: slides.length,
          sized: slides.length > 0 && slides.every(s => {
            const r = s.getBoundingClientRect();
            return Math.abs(r.width - 1280) < 2 && Math.abs(r.height - 720) < 2;
          })};
}
"""

CHECK_JS = """
() => [...document.querySelectorAll('.slide')].map((slide, i) => {
  const box = slide.getBoundingClientRect();
  let cut = false;
  for (const el of slide.querySelectorAll('*')) {
    const r = el.getBoundingClientRect();
    if (r.width === 0 || r.height === 0) continue;
    if (getComputedStyle(el).position === 'absolute') continue;
    if (r.bottom > box.bottom - 40 + 1 || r.right > box.right - 40 + 1) { cut = true; break; }
  }
  const broken = [...slide.querySelectorAll('img')].filter(img => !img.complete || img.naturalWidth === 0)
    .map(img => img.getAttribute('src'));
  const text = slide.innerText;
  const title = (slide.querySelector('h1, h2') || {}).innerText || '';
  return {n: i + 1, title: title.slice(0, 60), cut, broken, words: text.split(/\\s+/).filter(Boolean).length, text};
})
"""


def pdf_pages(pdf: Path) -> list[tuple[float, float]]:
    from pypdf import PdfReader

    return [
        (float(p.mediabox.width), float(p.mediabox.height))
        for p in PdfReader(str(pdf)).pages
    ]


def main(argv: list[str]) -> int:
    args = [a for a in argv[1:] if a != "--no-pages"]
    if len(args) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    src, dst = Path(args[0]), Path(args[1])
    if not src.is_file():
        print(f"no such file: {src}", file=sys.stderr)
        return 2
    dst.parent.mkdir(parents=True, exist_ok=True)
    errors: list[str] = []

    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        exe = shutil.which("agent-chromium")
        browser = p.chromium.launch(executable_path=exe) if exe else p.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 720})
        page.goto(src.resolve().as_uri(), wait_until="networkidle")
        sized = page.evaluate(SIZED_JS)
        if sized["count"] == 0:
            errors.append(
                'no <section class="slide"> elements: build each slide as one; '
                "start from python skills/slides/scripts/new_deck.py"
            )
        elif not sized["sized"]:
            page.add_style_tag(path=str(HOUSE_CSS))
            print(
                "NOTE the slides were not 1280 x 720 px, so the house deck.css was "
                "applied; link deck.css in your HTML to keep that look"
            )
        page.evaluate("document.fonts.ready")
        slides = page.evaluate(CHECK_JS)
        page.pdf(path=str(dst), print_background=True, prefer_css_page_size=True)
        browser.close()

    problems = 0
    for s in slides:
        notes = []
        if s["cut"]:
            notes.append("content runs into the margin or off the slide")
        if s["broken"]:
            notes.append("broken image " + ", ".join(map(str, s["broken"])))
        if s["words"] > 60:
            notes.append(f"{s['words']} words, too many to read while listening")
        found = sorted({m.group(0) for m in PLACEHOLDERS.finditer(s["text"])})
        if found:
            notes.append("placeholder text " + ", ".join(found))
        problems += bool(notes)
        status = "WARNING " + "; ".join(notes) if notes else "ok"
        print(f"slide {s['n']:2d} {s['title']!r}: {status}")

    pages = pdf_pages(dst)
    if slides and len(pages) != len(slides):
        errors.append(
            f"{len(slides)} slides but {len(pages)} PDF pages: a slide spills onto "
            "a second page or slides share one; keep each slide 1280 x 720"
        )
    if pages and any(abs(w / h - 16 / 9) > 0.02 for w, h in pages):
        errors.append("the PDF pages are not 16:9; link deck.css (it sets @page)")
    print(f"wrote {dst} ({len(slides)} slides, {len(pages)} pages)")
    for e in errors:
        print(f"ERROR {e}")
    if not problems and not errors:
        print("no slide warnings")
    if "--no-pages" not in argv:
        sys.path.insert(0, str(HERE.parent.parent / "pdf-report" / "scripts"))
        try:
            from render_pages import look_hint, render

            print(look_hint(render(dst)))
        except ImportError:
            print(
                f"LOOK NOW: pdftoppm -png -r 60 {dst} .render/deck, then read_file the PNGs"
            )
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
