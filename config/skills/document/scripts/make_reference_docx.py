#!/usr/bin/env python3
"""Write a Word style template (reference.docx) in the house style.

usage: python make_reference_docx.py OUT.docx [--font Calibri] [--letter]

Use it with pandoc (`pandoc in.md -o out.docx --reference-doc OUT.docx`) or
open it with python-docx as the starting document. Styles set: Normal, body
text, Title, Subtitle, Heading 1-3, Caption, page size A4 (or Letter) with
2.5 cm side margins and page numbers in the footer. Calibri is the default
because every Office installation has it (LibreOffice here renders it with
Carlito, which has the same metrics).
"""

from __future__ import annotations

import argparse
import subprocess
import tempfile
from pathlib import Path

from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Pt, RGBColor

INK = RGBColor(0x1B, 0x22, 0x30)
MUTED = RGBColor(0x5D, 0x66, 0x77)
ACCENT = RGBColor(0x24, 0x56, 0xC9)


def pandoc_default(path: Path):
    """Start from pandoc's own reference.docx so every style pandoc emits exists."""
    subprocess.run(
        ["pandoc", "-o", str(path), "--print-default-data-file", "reference.docx"],
        check=True,
    )
    return Document(str(path))


def by_name(doc, name: str):
    """A style by its displayed name. doc.styles[name] translates "Heading 1"
    to Word's internal "heading 1", which pandoc's template does not use."""
    for style in doc.styles:
        if style.name == name:
            return style
    return None


def set_font(
    style, name: str, size: float, bold: bool | None = None, color=None
) -> None:
    if style is None:
        return
    style.font.name = name
    rpr = style.element.get_or_add_rPr()
    fonts = rpr.find(qn("w:rFonts"))
    if fonts is None:
        fonts = OxmlElement("w:rFonts")
        rpr.append(fonts)
    for attr in ("w:ascii", "w:hAnsi", "w:cs", "w:eastAsia"):
        fonts.set(qn(attr), name)
    for attr in ("w:asciiTheme", "w:hAnsiTheme", "w:cstheme", "w:eastAsiaTheme"):
        fonts.attrib.pop(qn(attr), None)
    style.font.size = Pt(size)
    style.font.italic = False
    if bold is not None:
        style.font.bold = bold
    if color is not None:
        style.font.color.rgb = color


def add_page_number_footer(section) -> None:
    paragraph = section.footer.paragraphs[0]
    paragraph.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    run = paragraph.add_run()
    for kind, text in (("begin", None), (None, "PAGE"), ("end", None)):
        if kind:
            el = OxmlElement("w:fldChar")
            el.set(qn("w:fldCharType"), kind)
        else:
            el = OxmlElement("w:instrText")
            el.set(qn("xml:space"), "preserve")
            el.text = text
        run._r.append(el)
    run.font.size = Pt(8)
    run.font.color.rgb = MUTED


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("out")
    ap.add_argument("--font", default="Calibri")
    ap.add_argument("--letter", action="store_true", help="US Letter instead of A4")
    args = ap.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        doc = pandoc_default(Path(tmp) / "default.docx")
        for name in ("Normal", "Body Text", "First Paragraph", "Compact"):
            set_font(by_name(doc, name), args.font, 11, color=INK)
        normal = by_name(doc, "Normal").paragraph_format
        normal.space_after = Pt(6)
        normal.line_spacing = 1.15
        set_font(by_name(doc, "Title"), args.font, 26, bold=True, color=INK)
        set_font(by_name(doc, "Subtitle"), args.font, 13, bold=False, color=MUTED)
        for level, size in ((1, 16), (2, 13), (3, 11.5)):
            style = by_name(doc, f"Heading {level}")
            set_font(
                style, args.font, size, bold=True, color=ACCENT if level == 1 else INK
            )
            style.paragraph_format.space_before = Pt(14 if level == 1 else 10)
            style.paragraph_format.space_after = Pt(4)
            style.paragraph_format.keep_with_next = True
        for name in ("Caption", "Table Caption", "Image Caption"):
            set_font(by_name(doc, name), args.font, 9, color=MUTED)

        for section in doc.sections:
            if args.letter:
                section.page_width, section.page_height = Cm(21.59), Cm(27.94)
            else:
                section.page_width, section.page_height = Cm(21.0), Cm(29.7)
            section.left_margin = section.right_margin = Cm(2.5)
            section.top_margin = section.bottom_margin = Cm(2.2)
            add_page_number_footer(section)

        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        doc.save(str(out))
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
