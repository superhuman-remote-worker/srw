"""Build an editable 16:9 PowerPoint deck in the house style with python-pptx.

    import sys; sys.path.insert(0, "skills/slides/scripts")
    import pptx_kit as kit
    deck = kit.Deck()
    deck.title("Lunch is down, dinner carries the year", "Restaurant review · November 2026")
    deck.numbers("The year in four numbers", [("€1.28M", "Revenue", "+2.1%"), ("31,920", "Covers", "−4.8%")])
    deck.chart("Weekday lunch lost a third of its guests from June", "work/charts/lunch.png",
               bullets=["41 a day until May, 28 from June"], source="POS export")
    deck.bullets("Three reasons we see", ["…", "…"], notes="Speaker notes go here")
    deck.table("Month by month", ["Month", "Lunch", "Dinner"], [["May", "902", "1,811"]])
    deck.statement("Close weekday lunch from January.", "Brunch is booked out six weeks ahead.")
    deck.save("output/review.pptx")

Every slide is drawn on the blank layout with text boxes, so positions are
exact and nothing depends on a template. Fonts: Calibri (on every Office
install; LibreOffice renders it with Carlito). Charts go in as PNG from the
charts skill (figsize about 11 x 5.5 in, larger fonts).
"""

from __future__ import annotations

from pathlib import Path

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.util import Emu, Inches, Pt

INK = RGBColor(0x1B, 0x22, 0x30)
MUTED = RGBColor(0x5D, 0x66, 0x77)
ACCENT = RGBColor(0x24, 0x56, 0xC9)
HIGHLIGHT = RGBColor(0xD9, 0x82, 0x2B)
GOOD = RGBColor(0x2F, 0x8F, 0x5B)
BAD = RGBColor(0xC4, 0x3D, 0x3D)
RULE = RGBColor(0xDF, 0xE3, 0xEA)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
FONT = "Calibri"

W, H = Inches(13.333), Inches(7.5)
MARGIN = Inches(0.8)


class Deck:
    def __init__(self, font: str = FONT):
        self.prs = Presentation()
        self.prs.slide_width, self.prs.slide_height = W, H
        self.font = font
        self._blank = self.prs.slide_layouts[6]

    # -- building blocks -------------------------------------------------
    def _slide(self, notes: str | None = None):
        slide = self.prs.slides.add_slide(self._blank)
        if notes:
            slide.notes_slide.notes_text_frame.text = notes
        return slide

    def _text(
        self,
        slide,
        x,
        y,
        w,
        h,
        text,
        size,
        bold=False,
        color=INK,
        align=PP_ALIGN.LEFT,
        anchor=MSO_ANCHOR.TOP,
    ):
        box = slide.shapes.add_textbox(x, y, w, h)
        tf = box.text_frame
        tf.word_wrap = True
        tf.vertical_anchor = anchor
        tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
        lines = text if isinstance(text, list) else [text]
        for i, line in enumerate(lines):
            para = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            para.alignment = align
            run = para.add_run()
            run.text = line
            run.font.name = self.font
            run.font.size = Pt(size)
            run.font.bold = bold
            run.font.color.rgb = color
        return box

    def _title(self, slide, text):
        self._text(
            slide, MARGIN, Inches(0.6), W - 2 * MARGIN, Inches(1.3), text, 32, bold=True
        )

    def _number(self, slide):
        n = len(self.prs.slides)
        self._text(
            slide,
            W - MARGIN - Inches(1),
            H - Inches(0.55),
            Inches(1),
            Inches(0.3),
            str(n),
            11,
            color=MUTED,
            align=PP_ALIGN.RIGHT,
        )

    def _source(self, slide, source):
        if source:
            self._text(
                slide,
                MARGIN,
                H - Inches(0.55),
                Inches(9),
                Inches(0.3),
                source,
                11,
                color=MUTED,
            )

    def _bullets(self, slide, x, y, w, h, items, size=20):
        box = slide.shapes.add_textbox(x, y, w, h)
        tf = box.text_frame
        tf.word_wrap = True
        for i, item in enumerate(items):
            para = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            para.space_after = Pt(12)
            bullet = para.add_run()
            bullet.text = "■  "
            bullet.font.size = Pt(size * 0.55)
            bullet.font.color.rgb = ACCENT
            run = para.add_run()
            run.text = item
            run.font.name = self.font
            run.font.size = Pt(size)
            run.font.color.rgb = INK
        return box

    # -- slide types -----------------------------------------------------
    def title(self, title: str, subtitle: str = "", notes: str | None = None):
        slide = self._slide(notes)
        bg = slide.background.fill
        bg.solid()
        bg.fore_color.rgb = INK
        bar = slide.shapes.add_shape(1, MARGIN, Inches(2.4), Inches(1.2), Inches(0.08))
        bar.fill.solid()
        bar.fill.fore_color.rgb = ACCENT
        bar.line.fill.background()
        self._text(
            slide,
            MARGIN,
            Inches(2.75),
            Inches(10.5),
            Inches(2),
            title,
            44,
            bold=True,
            color=WHITE,
        )
        if subtitle:
            self._text(
                slide,
                MARGIN,
                Inches(4.9),
                Inches(10.5),
                Inches(0.6),
                subtitle,
                18,
                color=RGBColor(0xC6, 0xCC, 0xD8),
            )
        return slide

    def bullets(
        self, title: str, items: list[str], source: str = "", notes: str | None = None
    ):
        slide = self._slide(notes)
        self._title(slide, title)
        self._bullets(slide, MARGIN, Inches(2.1), W - 2 * MARGIN, Inches(4.6), items)
        self._source(slide, source)
        self._number(slide)
        return slide

    def chart(
        self,
        title: str,
        image: str,
        bullets: list[str] | None = None,
        source: str = "",
        notes: str | None = None,
    ):
        slide = self._slide(notes)
        self._title(slide, title)
        top, height = Inches(2.0), Inches(4.7)
        width = (W - 2 * MARGIN) * (0.62 if bullets else 1.0)
        pic = slide.shapes.add_picture(str(Path(image)), MARGIN, top)
        scale = min(width / pic.width, height / pic.height)
        pic.width, pic.height = int(pic.width * scale), int(pic.height * scale)
        pic.top = top + (height - pic.height) // 2
        if bullets:
            x = MARGIN + width + Inches(0.5)
            self._bullets(
                slide, x, Inches(2.3), W - MARGIN - x, height, bullets, size=18
            )
        self._source(slide, source)
        self._number(slide)
        return slide

    def numbers(
        self,
        title: str,
        items: list[tuple[str, str, str]],
        highlight: int | None = None,
        source: str = "",
        notes: str | None = None,
    ):
        """items: (value, label, delta) — delta starting with + is green, − or - red."""
        slide = self._slide(notes)
        self._title(slide, title)
        n = max(1, len(items))
        gap = Inches(0.45)
        cell = Emu(int((W - 2 * MARGIN - gap * (n - 1)) / n))
        for i, (value, label, delta) in enumerate(items):
            x = MARGIN + (cell + gap) * i
            rule = slide.shapes.add_shape(1, x, Inches(2.6), cell, Inches(0.05))
            rule.fill.solid()
            rule.fill.fore_color.rgb = HIGHLIGHT if i == highlight else INK
            rule.line.fill.background()
            self._text(slide, x, Inches(2.8), cell, Inches(1), value, 40, bold=True)
            self._text(slide, x, Inches(3.8), cell, Inches(0.8), label, 16, color=MUTED)
            if delta:
                color = (
                    GOOD
                    if delta.startswith("+")
                    else BAD
                    if delta[:1] in "−-"
                    else MUTED
                )
                self._text(
                    slide, x, Inches(4.55), cell, Inches(0.4), delta, 14, color=color
                )
        self._source(slide, source)
        self._number(slide)
        return slide

    def table(
        self,
        title: str,
        header: list[str],
        rows: list[list[str]],
        numeric: set[int] | None = None,
        source: str = "",
        notes: str | None = None,
    ):
        """numeric: column indexes to right-align (default: all but the first)."""
        slide = self._slide(notes)
        self._title(slide, title)
        numeric = set(range(1, len(header))) if numeric is None else numeric
        shape = slide.shapes.add_table(
            len(rows) + 1,
            len(header),
            MARGIN,
            Inches(2.1),
            W - 2 * MARGIN,
            Inches(0.5) * (len(rows) + 1),
        )
        table = shape.table
        for r, values in enumerate([header] + rows):
            for c, value in enumerate(values):
                cell = table.cell(r, c)
                cell.fill.solid()
                cell.fill.fore_color.rgb = WHITE
                tf = cell.text_frame
                tf.text = str(value)
                para = tf.paragraphs[0]
                para.alignment = PP_ALIGN.RIGHT if c in numeric else PP_ALIGN.LEFT
                for run in para.runs:
                    run.font.name = self.font
                    run.font.size = Pt(16)
                    run.font.bold = r == 0
                    run.font.color.rgb = INK
        self._source(slide, source)
        self._number(slide)
        return slide

    def statement(self, text: str, sub: str = "", notes: str | None = None):
        slide = self._slide(notes)
        self._text(
            slide,
            MARGIN,
            Inches(2.2),
            Inches(11),
            Inches(2.2),
            text,
            40,
            bold=True,
            anchor=MSO_ANCHOR.BOTTOM,
        )
        if sub:
            self._text(
                slide,
                MARGIN,
                Inches(4.7),
                Inches(10.5),
                Inches(1),
                sub,
                20,
                color=MUTED,
            )
        self._number(slide)
        return slide

    def save(self, path: str) -> Path:
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        self.prs.save(str(out))
        print(out)
        return out
