---
name: pdf-report
description: Use when the deliverable is a PDF or any document a person will read, print or forward — a report, one-pager, overview, briefing, summary, handout, invoice or certificate. Write it as HTML with print CSS, print it with the workspace's headless Chromium, then render the pages to PNG and look at them before delivering. Also covers reading, merging, splitting and filling existing PDFs. Charts inside the report come from `charts`, the numbers behind it from `data-analysis`; an editable Word file is `document`, a deck is `slides`.
display_name: PDF Report
icon: picture_as_pdf
color: "#f38ba8"
tags:
  - documents
  - pdf
  - reports
  - design
---

# PDF Report

A report is judged in the first ten seconds by how it looks and whether the
answer is on page one. You write HTML and CSS fluently and the workspace has a
headless Chromium that prints them exactly, with real fonts, SVG charts, tables
and page numbers. So build every PDF as an HTML page and print it. Do not hand
back Markdown when a PDF was asked for, and do not use pandoc's PDF output (it
has no LaTeX engine here). Use `reportlab` only for fixed-coordinate forms.

Everything below runs in the workspace shell. The bundled files are in
`skills/pdf-report/`.

## The procedure

1. **Write the content before you style it.** Who reads this, and what should
   they know or decide after page one? Draft the title, a one- or two-sentence
   answer, the three to five numbers that carry it (each compared with
   something: last year, the plan, the average), then the sections, then
   method and sources. Write in the language the user writes in.
2. **Start from the template:**

       python skills/pdf-report/scripts/new_report.py work/report.html

   It puts `report.html` and `report.css` into `work/` (never inside
   `skills/` or `output/`). Replace the example content and keep the
   `<link>` to report.css. The template has a title block, a summary box, KPI
   tiles, a figure, a data table and a sources block. Delete the parts you do
   not need rather than leaving them empty.
3. **Add charts as SVG** (`charts` skill: `fig.savefig("chart.svg")`) and
   reference them with `<img src="chart.svg">` or paste the SVG inline. SVG
   stays sharp at any zoom and keeps the PDF small.
4. **Print it:**

       python skills/pdf-report/scripts/render_pdf.py work/report.html output/report.pdf

   The script waits for fonts and images, prints with the page size and
   margins from the CSS, warns about content wider than the page, broken
   images and leftover placeholders (treat every warning as a defect), and
   renders the pages to PNG under `.render/`.
5. **Look at every page.** The script ends with a `LOOK NOW: read_file …`
   line: do exactly that. (`read_file` on the PDF itself skips pages that
   hold only text and vector graphics, so it is no substitute.) Read the
   sheet for the overall rhythm, then each page you need to check closely,
   and go through the check list below. For any other PDF or Office file:
   `python skills/pdf-report/scripts/render_pages.py FILE`.
6. **Fix and print again** until a page you would send to a customer comes
   out. Two or three rounds is normal. Deliver the PDF in `output/`; keep the
   HTML source in the work folder.

## Check list for each page

- The answer and the key numbers are on page one.
- Nothing is cut off at the right edge or bottom; no table runs off the page.
- No heading sits alone at the bottom of a page; no figure is split.
- Numbers line up (right-aligned, same decimals, thousands separators, units).
- Text is readable: body at least 10 pt, captions at least 8 pt.
- Colours mean something; at most one accent plus grey.
- Page numbers and the footer show.
- The last page is not a near-empty spill-over: tighten spacing, shorten, or
  move a block so the pages end cleanly.
- No placeholder, no "Lorem", no empty section, no `undefined` or `NaN`.

## Design rules (the template follows them; keep them when you change it)

- A4 portrait unless the user is in the US (Letter) or the content is a wide
  table (landscape). Margins about 18 mm.
- One type family. `Noto Sans` for most work, `Noto Serif` for formal letters
  and long reading. Both are installed, as are DejaVu and Liberation; do not
  load web fonts.
- Hierarchy through size and weight, not colour: title 22–26 pt, section 14 pt,
  body 10–11 pt with line height about 1.45.
- Tables: header row with a bottom rule, light row separators, numbers
  right-aligned with `font-variant-numeric: tabular-nums`, a bold total row.
  Long tables repeat their header on each page (the template does this).
- Number formats follow the reader: German `1.234,5 €` and `11.11.2026`,
  English `€1,234.5` and `11 Nov 2026`.
- Highlight what the reader should notice (the month that stands out, the
  figure above plan) with the accent colour; everything else stays neutral.

## Existing PDFs

- Text: `pdftotext -layout in.pdf -` (fast), or `pdfplumber` for tables:
  `pdfplumber.open(p).pages[i].extract_tables()`.
- Merge, split, rotate, reorder: `pypdf` (`PdfReader`, `PdfWriter`).
- Fill a form: `PdfReader(p).get_fields()` to list the fields, then
  `writer.update_page_form_field_values(writer.pages[0], {...})`.
- A scanned PDF has no text layer (no OCR is installed): render the pages
  with `render_pages.py` and read the images.
- Convert Word, Excel or PowerPoint to PDF:
  `soffice --headless --convert-to pdf --outdir out/ file.docx`.

## Don't

- Don't report the layout as fine without having looked at the rendered pages.
- Don't pad the report to look substantial; a clear two-page report beats a
  ten-page one.
- Don't put the method before the answer.
- Don't invent numbers to fill a KPI tile; leave the tile out.
