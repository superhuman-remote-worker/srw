#!/usr/bin/env python3
"""Replace placeholders in a .docx and keep its formatting.

usage: python docx_fill.py IN.docx OUT.docx MAPPING.json
       MAPPING.json = {"{{NAME}}": "Erika Mustermann", "{{DATE}}": "11.11.2026"}

Works in body paragraphs, tables, headers and footers, also when Word split
a placeholder across several runs (the replacement takes the formatting of
the run where the placeholder starts). Prints how often each key was
replaced and lists keys that were not found.
"""

from __future__ import annotations

import json
import sys
from collections import Counter

from docx import Document


def paragraphs(doc):
    def walk(container):
        for p in container.paragraphs:
            yield p
        for table in getattr(container, "tables", []):
            for row in table.rows:
                for cell in row.cells:
                    yield from walk(cell)

    yield from walk(doc)
    for section in doc.sections:
        for part in (
            section.header,
            section.footer,
            section.first_page_header,
            section.first_page_footer,
        ):
            yield from walk(part)


def replace_in_paragraph(paragraph, old: str, new: str) -> int:
    count = 0
    search_from = 0
    for _ in range(paragraph.text.count(old)):
        runs = paragraph.runs
        text = "".join(r.text for r in runs)
        start = text.find(old, search_from)
        if start < 0 or not runs:
            break
        search_from = start + len(new)
        end = start + len(old)
        pos = 0
        first = None
        for run in runs:
            run_start, run_end = pos, pos + len(run.text)
            pos = run_end
            if run_end <= start or run_start >= end:
                continue
            keep_before = run.text[: max(0, start - run_start)]
            keep_after = run.text[max(0, end - run_start) :] if run_end > end else ""
            if first is None:
                first = run
                run.text = keep_before + new + keep_after
            else:
                run.text = keep_after
        count += 1
    return count


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print(__doc__, file=sys.stderr)
        return 2
    src, dst, mapping_path = argv[1:]
    with open(mapping_path, encoding="utf-8") as fh:
        mapping = json.load(fh)
    doc = Document(src)
    hits: Counter[str] = Counter()
    for paragraph in paragraphs(doc):
        for old, new in mapping.items():
            if old in paragraph.text:
                hits[old] += replace_in_paragraph(paragraph, old, str(new))
    doc.save(dst)
    for key in mapping:
        print(f"{hits[key]:3d}  {key}")
    missing = [k for k in mapping if hits[k] == 0]
    if missing:
        print("not found: " + ", ".join(missing))
    print(dst)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
