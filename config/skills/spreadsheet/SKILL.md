---
name: spreadsheet
description: Use when the deliverable is an Excel workbook (.xlsx) or a table the user will edit, filter or recalculate — a budget, tracker, price list, plan, forecast, schedule or data export — or when an existing .xlsx or .csv must be read, cleaned or changed. Build it with openpyxl using live formulas, clear input cells and proper number formats, then recalculate it with LibreOffice and check every formula result before delivering. For the analysis itself see `data-analysis`; for a printable summary see `pdf-report`.
display_name: Spreadsheet
icon: table_chart
color: "#a6e3a1"
tags:
  - spreadsheets
  - excel
  - data
  - documents
---

# Spreadsheet

People open a workbook to change a number and watch the rest follow. So a
good workbook has live formulas, inputs that are obviously inputs, and
numbers formatted the way the reader expects. A table of pasted values is a
CSV with extra steps. The workspace has openpyxl, xlsxwriter, pandas and a
headless LibreOffice to recalculate and render the result; the checker is
`skills/spreadsheet/scripts/xlsx_check.py`.

## The procedure

1. **Sketch the layout first.** Which sheets (inputs/assumptions, data,
   calculations, summary)? Which cells does the user change? What should the
   summary show? One table per sheet, starting at A1, one header row.
2. **Build it with openpyxl** (or write the data with pandas and style it
   with openpyxl afterwards; xlsxwriter when you need rich native charts and
   only write once):
   - Derived cells are formulas: `ws["D2"] = "=B2*C2"`, totals
     `=SUM(D2:D40)`, shares `=D2/$D$41`. Use English function names and
     commas between arguments in openpyxl, whatever the user's locale.
   - Inputs and assumptions sit together, visibly marked (light yellow
     fill or blue font), and formulas point at them instead of repeating a
     constant.
   - Formats: `#,##0` counts, `#,##0.00 [$€-de-DE]` or `"€"#,##0.00` money,
     `0.0%` shares, `DD.MM.YYYY` or `YYYY-MM-DD` dates. Set them on every
     numeric column.
   - Name the font on every style you set (`Font(name="Calibri", bold=True)`);
     a `Font()` without a name falls back to a serif face in LibreOffice and
     some viewers.
   - Header row bold with a bottom border, `ws.freeze_panes = "A2"`,
     `ws.auto_filter.ref = ws.dimensions`, column widths set
     (`ws.column_dimensions["A"].width = 18`), totals row bold.
   - Sheet names that say what they hold; no merged cells inside data.
   - A short "About" sheet when there are assumptions: source, date, how to
     use the inputs.
3. **Recalculate and check:**

       python skills/spreadsheet/scripts/xlsx_check.py output/budget.xlsx

   It recalculates the workbook with LibreOffice and reports, per sheet:
   size, formula count, every cell that evaluates to an error (`#REF!`,
   `#DIV/0!`, `#NAME?`, `#VALUE!`, `#N/A`), numeric columns with no number
   format, and the computed values of the last rows (totals). Fix every
   error. Compare one or two totals with a sum you compute in pandas.
   `--store-values` writes the recalculated values into the file, so viewers
   that do not calculate (previews, pandas) show numbers instead of blanks;
   use it on workbooks without charts.
4. **Look at it** when the layout matters (a sheet that will be printed or
   presented):

       python skills/pdf-report/scripts/render_pages.py output/budget.xlsx

   and `read_file` the page images.
5. **Deliver** the `.xlsx` in `output/`, and say in one line which cells
   the user edits.

## Charts in the workbook

openpyxl: `from openpyxl.chart import BarChart, LineChart, Reference`, add
the data range with `titles_from_data=True`, set `chart.title`,
`chart.y_axis.title`, then `ws.add_chart(chart, "H2")`. Keep the house
colours where the library allows it. A picture of a matplotlib chart
(`openpyxl.drawing.image.Image("chart.png")`) is fine for a printed summary
sheet but does not update.

## CSV

- For German Excel: `df.to_csv(p, sep=";", decimal=",", index=False,
  encoding="utf-8-sig")`. The BOM and the semicolon make Excel open it in
  columns with umlauts intact.
- For other tools: comma, dot, UTF-8, no BOM.
- Reading: see `skills/data-analysis/references/pandas-recipes.md`.

## Changing an existing workbook

Load with `openpyxl.load_workbook(p)` (not `data_only=True`, which would drop
the formulas on save). Change cells in place, keep the styles, and never
round-trip the file through pandas, which loses formats, formulas and
charts. `.xls` (old format): convert first with
`soffice --headless --convert-to xlsx file.xls`.

## Don't

- Don't paste computed values where a formula belongs.
- Don't leave an error cell, a `#####` column or a formula that points at an
  empty range.
- Don't hide the assumptions inside formulas (`=B2*1.19` → a VAT input cell).
- Don't claim the totals are right without having run the checker.
