---
name: charts
description: Use when numbers should be shown as a chart, graph or plot — in a report, a slide, a Word file, a chat answer or on the canvas. Pick the chart from the question, make the point the reader should see stand out, render it with matplotlib in the house style (SVG for PDF and HTML, PNG for everything else), then look at the image before using it. For the analysis behind the numbers see `data-analysis`; for putting the chart into a document see `pdf-report`, `document` or `slides`.
display_name: Charts
icon: bar_chart
color: "#89b4fa"
tags:
  - charts
  - data
  - visualization
  - design
---

# Charts

A chart exists to make one comparison obvious. Decide what the reader should
see first, then pick the form that shows exactly that and strip the rest. The
workspace has matplotlib, pandas and the fonts the house style uses; the
style file and small helpers are in `skills/charts/`.

## The procedure

1. **Write the message as a sentence** before choosing a chart: "Weekday
   lunch fell by a third from June." That sentence becomes the title (or the
   caption, when the document already has a heading above the chart).
2. **Pick the form from the question:**

   | the question | the chart |
   |---|---|
   | how does it change over time | line (many points) or columns (≤ 15 periods) |
   | which is bigger, ranking | horizontal bars, sorted, labels on the left |
   | part of a whole | one stacked bar or a sorted bar chart; a pie only for 2–3 parts |
   | how values spread | histogram or box plot |
   | does A move with B | scatter, with the outliers named |
   | plan vs actual, this year vs last | paired bars or two lines, the reference in grey |

   No 3D, no donuts with ten slices, no second y-axis unless the user insists
   (two charts side by side read better).
3. **Draw it in the house style:**

   ```python
   import sys; sys.path.insert(0, "skills/charts/scripts")
   import chartkit as ck
   ck.use()                                   # style file + headless backend
   fig, ax = ck.plt.subplots()                # 7 x 3.6 in by default
   bars = ax.bar(months, covers, color=ck.highlight(months, {"Jun", "Jul", "Aug"}))
   ck.label_bars(ax, bars)                    # values on the bars
   ax.set_title("Weekday lunch fell by a third from June")
   ax.set_ylabel("covers per day")
   ax.yaxis.set_major_formatter(ck.number_format("de"))   # 1.234 for German readers
   ck.save(fig, "work/charts/lunch_covers")   # .svg and .png
   ```

   Without the helpers: `plt.style.use("skills/charts/assets/srw.mplstyle")`
   and `fig.savefig("x.svg")` / `fig.savefig("x.png")` do the same.
4. **Look at it.** `read_file` the PNG. Check: can you read every label at the
   size it will be shown? Does the highlighted part catch the eye first? Do
   labels overlap? Is the zero line there for bars? Fix and save again.
5. **Use the right file:** the SVG inside a PDF or HTML page (sharp, small,
   text stays text); the PNG for chat, the canvas, Word and PowerPoint.

## Rules that make charts look deliberate

- Highlight, don't decorate. The one thing that matters gets the accent or
  highlight colour; context is light grey-blue (`ck.COLORS["context"]`).
- Label directly. Values on bars, names at the end of lines
  (`ck.label_line_ends(ax)`), and drop the legend when there are four series
  or fewer.
- Bars start at zero. Lines may start elsewhere; say so in the axis.
- Units on the axis label or in the title ("covers per day", "€ thousand").
- Sort categories by value unless they have a natural order (months, sizes).
- Format numbers for the reader: `ck.number_format("de")` gives `1.234`,
  `"en"` gives `1,234`; percent as `12%` (English) or `12 %` (German).
- One chart, one message. Two messages are two charts.
- Same colour for the same thing across every chart in one deliverable.
- Size for the destination: 7 × 3.6 in for a report column; for a slide
  `ck.use(scale=1.6)` and `ck.plt.subplots(figsize=(11, 5.5))`, so the axis
  labels stay readable from the back of the room.

## Interactive charts

When the user wants to hover and filter (the canvas renders interactive HTML),
write one self-contained HTML file with the data inline and a chart library
from a CDN (for example Chart.js or Vega-Lite). Keep the same colours and
the same one-message rule. A static PNG remains the default.

## Don't

- Don't plot numbers you have not checked (see `data-analysis`).
- Don't let matplotlib defaults through: the default blue cycle, a boxed
  legend, gridlines on both axes and rotated date labels all look generated.
- Don't truncate a bar axis to exaggerate a difference.
- Don't describe a chart you have not looked at.
