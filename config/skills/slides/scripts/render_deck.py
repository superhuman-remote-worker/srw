#!/usr/bin/env python3
"""Print an HTML slide deck to PDF (one slide per page) and check each slide.

usage: python render_deck.py DECK.html OUTPUT.pdf

Every <section class="slide"> must fit 1280 x 720 px. The script reports
slides whose content is cut off at the bottom or right, slides with more
than 60 words, images that did not load and leftover placeholders, then
prints the PDF with the page size from deck.css.
"""

from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

PLACEHOLDERS = re.compile(
    r"lorem ipsum|\{\{|\}\}|\bTODO\b|\bTBD\b|PLACEHOLDER|\bNaN\b|undefined"
)

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


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    src, dst = Path(argv[1]), Path(argv[2])
    if not src.is_file():
        print(f"no such file: {src}", file=sys.stderr)
        return 2
    dst.parent.mkdir(parents=True, exist_ok=True)

    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        exe = shutil.which("agent-chromium")
        browser = p.chromium.launch(executable_path=exe) if exe else p.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 720})
        page.goto(src.resolve().as_uri(), wait_until="networkidle")
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
    print(f"wrote {dst} ({len(slides)} slides)")
    if not problems:
        print("no slide warnings; now render the pages and look at them")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
