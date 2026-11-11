---
name: slides
description: Use when the deliverable is a presentation — slides, a deck, a pitch, a talk, a training, a board or team update — as PDF or as an editable PowerPoint (.pptx), or when an existing .pptx must be read or changed. Write the storyline first with one message per slide, build the deck from the house template (HTML printed to PDF, or python-pptx for PowerPoint), then render every slide to an image and look at it before delivering. Charts come from `charts`, numbers from `data-analysis`.
display_name: Slides
icon: slideshow
color: "#fab387"
tags:
  - presentations
  - slides
  - powerpoint
  - design
---

# Slides

A deck is a sequence of claims, one per slide, each with the evidence that
carries it. Most bad decks are documents cut into pages. Get the storyline
right and the design is mostly restraint: big type, one idea, lots of space.
Templates and helpers are in `skills/slides/`.

## The procedure

1. **Write the storyline as slide titles only.** Each title is a full
   sentence stating the takeaway ("Weekday lunch lost a third of its guests
   from June", not "Lunch covers"). Read the titles alone, top to bottom:
   they must tell the whole story. Typical arc: title → the answer or ask →
   two to five evidence slides → recommendation → next steps. About one slide
   per minute of talk.
2. **Choose the format.**
   - **PDF (default)** for sending, reading and presenting from any laptop:
     an HTML deck printed with Chromium. Best typography, exact layout.
   - **PowerPoint** when the user asks for .pptx or will edit the deck in
     Office: python-pptx with `scripts/pptx_kit.py`.
3. **Build it.**

   PDF: copy `assets/deck.html` and `assets/deck.css` to a work folder. The
   template has a title slide, big numbers, chart with bullets, a one-sentence
   statement and next steps; duplicate and fill the types you need. Then

       python skills/slides/scripts/render_deck.py work/deck.html output/deck.pdf

   which reports per slide: content running off the slide, more than 60
   words, broken images, placeholders.

   PowerPoint:

   ```python
   import sys; sys.path.insert(0, "skills/slides/scripts")
   import pptx_kit as kit
   deck = kit.Deck()
   deck.title("Lunch is down, dinner carries the year", "Restaurant review · November 2026")
   deck.numbers("The year in four numbers", [("€1.28M", "Revenue", "+2.1%"), ...], highlight=2)
   deck.chart("Dinner spend per cover rose all summer", "work/charts/spend.png",
              bullets=["+€4.10 per dinner cover"], source="Source: POS export",
              notes="What to say on this slide")
   deck.table("Month by month", ["Month", "Lunch", "Dinner"], rows)
   deck.statement("Close weekday lunch from January.", "Brunch is booked out six weeks ahead.")
   deck.save("output/review.pptx")
   ```

   Charts for slides: `ck.use(scale=1.6)` and `figsize=(11, 5.5)` in the
   `charts` helpers, saved as PNG.
4. **Look at every slide:**

       python skills/pdf-report/scripts/render_pages.py output/deck.pdf     # or output/review.pptx

   `read_file` the sheet for the rhythm of the deck, then each slide. Check:
   readable from the back of a room, nothing touching the edges, the title
   says the takeaway, the highlighted number or bar is the one the title
   talks about, consistent positions from slide to slide.
5. **Fix, render again, deliver** the PDF or PPTX in `output/`. For a talk,
   put what to say in the speaker notes (`notes=` in pptx_kit) rather than on
   the slide.

## Design rules

- Titles 32–44 pt, body at least 18 pt, labels at least 14 pt. If text does
  not fit at that size, it belongs on another slide or in the notes.
- At most five bullets of one line each, or one chart, or one table of up to
  six rows. Never all three.
- One accent colour for the thing that matters, grey for context, the same
  colours as the charts.
- Same title position, margins and source line on every slide.
- A number on a slide gets its comparison ("28 a day, −31%").

## Existing decks

- Read text and notes: `python-pptx` (`for shape in slide.shapes: if
  shape.has_text_frame: ...`, `slide.notes_slide.notes_text_frame.text`).
- Change text in place shape by shape to keep the user's design; do not
  rebuild their deck in the house style unless asked.
- Render it first to see what you are changing (`render_pages.py deck.pptx`).
- `.ppt` (old format): `soffice --headless --convert-to pptx file.ppt`.

## Don't

- Don't write paragraphs on slides.
- Don't deliver a deck you have not looked at slide by slide.
- Don't use stock decoration (clip art, gradients, drop shadows, word art).
- Don't add an agenda slide to a deck of fewer than eight slides.
