---
name: data-analysis
description: Use when asked to analyse data or answer a question from it — find out what changed and since when, compare periods, groups or plans, spot what stands out, explain a trend — from a CSV, Excel file, database export, table or report. Load and inspect the data, compute with a saved pandas script, sanity-check the numbers against the source, then answer with numbers in context and say what the data cannot tell. For showing the result see `charts`; for the deliverable see `pdf-report`, `spreadsheet` or `slides`.
display_name: Data Analysis
icon: query_stats
color: "#94e2d5"
tags:
  - data
  - analysis
  - research
---

# Data Analysis

The user wants an answer they can trust and act on, not a tour of the data.
Every number you report must come from code you ran on the real file, and
every important number needs a comparison: against last year, the plan, the
average, the other group. The workspace has pandas, numpy, scipy and
matplotlib; `references/pandas-recipes.md` has the snippets that trip people
up (German CSVs, Excel dates, months, weekdays).

## The procedure

1. **Pin down the question.** Restate it in one sentence with the measure,
   the period and the comparison ("weekday lunch covers per day, June–August
   2026 against September 2025–May 2026"). If the request is vague
   ("highlight what stands out"), decide the measures and the rule for
   "stands out" yourself and say which you chose.
2. **Look before you compute.** Load the file and print: shape, column names
   and types, the first rows, missing values per column, the date range,
   obvious units and currencies, duplicate rows. Note anything odd (a month
   with half the rows, negative quantities, a total row inside the data).
3. **Clean on purpose and write it down.** Parse dates, fix decimal commas,
   drop exact duplicates, exclude test or cancelled rows if the data marks
   them. Keep a short list of every decision; it goes into the method note.
   Never drop rows silently.
4. **Compute in a script you keep.** Put the analysis in
   `work/analysis.py` (or a notebook) and run it, so every number can be
   reproduced and changed. Print the result tables; save the ones you will
   deliver as CSV.
5. **Check the numbers.** Totals match the source (row count, revenue sum).
   Shares add up to 100%. Spot-check two or three results by hand from raw
   rows. Compare with a figure the user already knows if there is one. A
   result that looks too good or too strange is usually a join, a filter or
   a unit error: find it before you report it.
6. **Answer.** Lead with the answer in one or two sentences, with the size
   of the effect and since when. Then the evidence: a small table, a chart if
   it helps (`charts`), the months or groups that stand out and why. Then a
   short method note: source, period, definitions, cleaning decisions.
7. **Say what the data cannot tell.** Correlation is not cause; name what
   would confirm one (a menu change, a closed street, a competitor). If the
   sample is small or a period is incomplete, say so next to the number.

## Deciding what stands out

Use a stated rule and apply it the same way everywhere, for example:

- more than ±15% against the same month last year, or
- outside 1.5 × the interquartile range of the other months, or
- the largest change in the series, with its size.

Then name the outliers with their numbers ("July: 594 lunch covers, −34% on
July 2025"). For "is the difference real?" with enough data,
`scipy.stats.mannwhitneyu` or `ttest_ind` gives a p-value; report the effect
size first, the test second, and keep the wording plain.

## Answer format

- Numbers with units and their comparison, rounded to what matters (28
  covers a day, not 27.8364).
- Means and medians are not interchangeable; say which, and prefer the
  median when a few large values dominate.
- Percent change vs percentage points: name which one.
- The user's number format and language (German: `1.234,5`, `12 %`).
- Tables: few columns, sorted by what matters, outliers marked.

## Don't

- Don't report a number you did not compute from the data in this run.
- Don't extrapolate beyond the period the data covers without saying so.
- Don't bury the answer under the method.
- Don't present a cause as found when the data only shows timing.
