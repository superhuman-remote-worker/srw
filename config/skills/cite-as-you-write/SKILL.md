---
name: cite-as-you-write
description: Use whenever you state a fact, figure, quote, or technical claim that rests on a source — how to cite it with cite_web / cite_document and carry the returned [^c…] handle into your answer so the reader can see and check the source.
display_name: Cite As You Write
icon: format_quote
color: "#cba6f7"
tags:
  - citations
  - research
  - writing
---

# Cite As You Write

You can look things up and call `cite_web` / `cite_document` — but a citation the
reader never sees does nothing. The common failure is **cite-and-forget**: you
search, you record four sources, and then you write an answer with no markers in
it. The citations sit in the database, invisible on the page. This skill closes
that gap.

The rule: **every claim that rests on a source carries its citation's handle, in
the text, where the claim is made.** Creating the citation and placing its handle
are one action, not two.

## The loop

For each factual, numeric, quoted, or technical claim you make from a source:

1. **Cite it.** Call `cite_web` (for a URL) or `cite_document` (for a workspace
   document), passing the claim and the supporting quote/locator. See the tool
   description for the exact arguments.
2. **Take the handle.** The tool returns a handle like `[^c1241]`. Copy it
   verbatim — it names the citation record, it is not a footnote number. Readers
   see ¹ ² ³: the interface numbers the handles of each answer (and of each
   delivered document) by first mention, so never write a number of your own.
3. **Place it.** Put the handle right after the claim (or clause) the source
   supports, before the period: `... roughly 40% of cases [^c1241].` Write the
   sentence and attach the handle in the same breath.
4. **Read the state.** Most citations come back "verification pending" and are
   checked in the background. One that comes back UNVERIFIED (the quoted words
   are not in the source) is shown to the reader as flagged: fix it with
   `edit_citation` (quote exactly, or set `extraction_method` to `paraphrase` if
   you reworded it), or withdraw it with `withdraw_citation` and drop its handle.

If a sentence draws on two sources, give it both: `... within 24 hours [^c1241][^c1250].`
If you make several claims from one source, reuse its handle each time.

### Example

`cite_web(text="The regulation entered into force on 1 January 2025.", url="https://example.org/reg", claim="The regulation applies from 2025")`
returns `Citation [^c1302] created; verification pending.` Then you write:

> The regulation has applied since 1 January 2025 [^c1302], so filings for 2024 still
> follow the old rules.

## When to cite

Cite when a reader could reasonably ask "says who?":

- A specific fact, statistic, date, name, or measurement.
- A direct or paraphrased quote.
- A technical claim, recommendation, or cause/effect you took from a source rather
  than derived yourself.

You don't need a marker for your own reasoning, summary, or connective prose —
only for the load-bearing claims that come from somewhere.

## In a conversation

When you're answering a user turn (not writing a file), the handles go **inline in
your reply** — the same `[^c…]` at the end of each supported sentence. These inline
handles are how the reader connects a specific claim to the specific source behind
it; an answer without them reads as unsourced even when you did the research.
Don't replace inline handles with a single "Sources:" list at the end — the
per-claim handle is the point.

## In a document

In a file you write (a report, a chapter), place handles the same way, as you
write each section. When the document is delivered, footnote definitions for the
handles it uses are appended in first-mention order, so it reads as numbered
footnotes in GitHub, Obsidian or Pandoc. Don't write a references list or
footnote definitions by hand.

## Memories and notes

A memory is not a source: cite what the memory came from, or don't cite. A
knowledge-base note is a project document; cite it with `cite_document` like any
other file.

## Don't

- **Don't cite and then write marker-free prose.** That is the failure this skill
  exists to prevent — the citation is wasted if it isn't in the text.
- **Don't fabricate or guess a handle.** Use only a handle a cite tool actually
  returned. If you didn't cite it, don't mark it.
- **Don't attach a handle to a claim you didn't draw from a source.** A handle
  promises "this came from that source" — keep it honest.
- **Don't renumber.** Emit the handle the tool gave you; the interface handles
  display numbering.
- **Don't cite a source id.** `search_library` and `list_sources` name sources as
  `S12`; that is not a citation. Cite the passage and use the handle that returns.
