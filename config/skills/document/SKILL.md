---
name: document
description: Use when the deliverable is a Word document (.docx) — a letter, memo, contract or policy draft, CV, minutes, a manual or any text the user will edit further — or when an existing .docx or Word template must be read, filled in or changed. Write the text, build the file with pandoc and a house style template or with python-docx, render it with LibreOffice and look at the pages before delivering. A finished document only to read or print is usually better as `pdf-report`.
display_name: Word Document
icon: description
color: "#89dceb"
tags:
  - documents
  - word
  - writing
  - letters
---

# Word Document

Choose Word when the reader will edit the text, sign it, or drop it into
their own process; choose `pdf-report` when they only read it. The workspace
has pandoc, python-docx and a headless LibreOffice that renders `.docx`
faithfully (Calibri and Cambria map to metric twins, so line breaks match
Word). Helpers are in `skills/document/scripts/`.

## The procedure

1. **Write the text first**, in the user's language and register. A letter
   has one purpose stated in the first paragraph; a memo opens with the
   recommendation; minutes list decisions and owners before discussion.
2. **Build the file**, by the shape of the content:
   - **Running text with headings, lists and tables** (memo, policy, manual,
     report to edit): write Markdown, then
     ```
     python skills/document/scripts/make_reference_docx.py work/reference.docx
     pandoc work/memo.md -o output/memo.docx --reference-doc work/reference.docx
     ```
     Add `--toc` for long documents. Pandoc maps `#` headings, lists,
     tables, footnotes and images (`![Caption](chart.png)`) to real Word
     styles, so the user's Word navigation and styles keep working. Table
     column widths follow the dashes in a pipe table's separator row once a
     row is longer than the line; for exact widths, set them with python-docx
     after the conversion.
   - **Exact layout** (letter, form, CV, a table-heavy one-pager): build it
     with python-docx. Start from `Document("work/reference.docx")` to inherit
     the house styles, use real styles (`doc.add_heading`, `style="List
     Bullet"`) rather than manual bold, and set widths on tables.
   - **The user's own template**: never rebuild it. Fill it:
     `python skills/document/scripts/docx_fill.py template.docx output/out.docx map.json`
     replaces `{{PLACEHOLDER}}` keys (or any literal text) in body, tables,
     headers and footers, keeping the formatting, and lists keys it did not
     find. Templates without placeholders: open with python-docx and edit
     the paragraphs and table cells in place.
3. **Look at it:**

       python skills/pdf-report/scripts/render_pages.py output/memo.docx

   and `read_file` the sheet and pages. Check headings, page breaks, table
   widths, the letterhead area, and that no style fell back to a default
   font.
4. **Deliver** the `.docx` in `output/`. Add a PDF next to it when the user
   will send it on: `soffice --headless --convert-to pdf --outdir output/ output/memo.docx`.

## German business letters (DIN 5008)

When writing a formal German letter with python-docx: A4, left margin 2.5 cm,
right margin 2 cm; sender line and address field in the top left (address
field starts 4.5 cm from the top edge for form B, 9 lines, recipient's
postal address with the town in the last line); date right-aligned or in the
information block as `11.11.2026`; subject line bold, without the word
"Betreff"; salutation "Sehr geehrte Frau …," followed by a lower-case start
of the first sentence; closing "Mit freundlichen Grüßen" with no comma, three
blank lines for the signature, then the name. English letters: sender block,
date written out ("11 November 2026"), "Dear …," and "Kind regards,".

## Reading and changing existing documents

- Text and structure: `pandoc in.docx -t markdown` (fast and readable), or
  python-docx for paragraphs, runs and tables.
- Comments and tracked changes are not supported by python-docx. Do not
  claim to have tracked your edits; deliver a clean edited copy plus a short
  list of what changed, or a comparison PDF of old and new text.
- `.doc` (old format): `soffice --headless --convert-to docx file.doc` first.

## Don't

- Don't fake structure with bold text and blank lines; use heading and list
  styles.
- Don't deliver a Word file you have not rendered and looked at.
- Don't change a user's template layout or fonts unless asked.
- Don't leave `{{placeholders}}`, "[Name]" or example text in the result.
