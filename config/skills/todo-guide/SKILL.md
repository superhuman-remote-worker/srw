---
name: todo-guide
description: Use before planning a phase or calling next_phase_todos — how to craft focused, well-scoped, verifiable todos.
display_name: Todo Guide
icon: checklist
color: "#89b4fa"
tags:
  - planning
  - todos
---

# Todo Crafting Guide

Good todos are what make a phase reviewable: each one should be specific enough
that a single check tells you whether it's done. Vague todos hide failure until
the end and burn a whole phase before anyone notices. This guide is the craft of
turning the next phase into a tight, verifiable list. (You're seeing it because
the `next_phase_todos` gate requires it — it's short.)

## Short phases, tight focus

Target **2–5 todos per tactical phase**. Go lower (2–3) for a focused phase
like verifying one section; higher (5–6) only for repetitive batch work. If
you need more than 6, it's two phases. A correct 2-todo phase beats a padded
5-todo one.

A phase is **one coherent unit of work** — "research the topic," "write section
3," "verify chapter 2's citations" — not a whole project stage. Every phase ends
in a strategic review, so shorter phases mean earlier course-correction and less
wasted work when priorities shift.

## What makes a good todo

Specific enough that you know *exactly* when it's done:

1. **Names the artifact** — file path, section number, page range, citation handles.
2. **Names the tool** — "use `read_file`," "use `web_search`," "use `write_file`."
3. **Has a measurable outcome** — "produces X," "updates Y to contain Z," "verifies N items."
4. **Fits in 1–3 tool calls** — if it needs more, split it.

The test: *"Could I confirm this is done by checking one specific thing?"*

| Vague (fails) | Specific (works) |
|---|---|
| "Check all citations" | "Verify citations 1–10 against the source documents in `documents/`" |
| "Write the analysis" | "Write `output/report.md` §2.1 (Market Overview) from phase-2 findings" |
| "Improve the quality" | "Add 3 supporting citations to §2 from the phase-3 sources" |
| "Handle edge cases" | "Add empty-input handling in `output/script.py` lines 45–60" |

## Pick the right phase type

Scaffold the deliverable file in phase 1 and update it at every phase boundary —
research fills the scaffold in, it doesn't precede it. Then match each phase to
where you are:

| Phase type | Typical todos | When to use |
|---|---|---|
| Research | 2–4 | New/unfamiliar topic — gather what the scaffold needs |
| Elaboration | 2–4 | Turn a rough plan into a concrete, sequenced breakdown |
| Execution | 3–5 | Produce one specific section or artifact |
| Batch processing | 4–6 | Repetitive operation over many similar items |
| Integration | 3–5 | Combine separately-produced parts into a coherent whole |
| Verification | 2–4 | Quality check before declaring done |

Two of these defer to dedicated skills — load them when you plan that phase:
- **Verification** → the `verify-before-done` skill covers *how* to produce
  evidence (so a verification todo is more than "review the output").
- **Research** → the `research-guide` skill covers search + citation method.

For example todos for each phase type and a full multi-phase worked example, read
**`skills/todo-guide/references/phase-patterns.md`**.

## Rules of thumb

- **One kind of work per phase.** Keep research, writing, and verification in
  separate phases — mixing them makes a phase hard to review.
- **Every todo advances a deliverable.** Workspace bookkeeping (archiving, status
  updates) happens automatically at phase boundaries — don't spend todos on it.
- **Scaffold first, research to fill.** The deliverable file exists from phase
  1; research an unfamiliar topic before writing its content into the scaffold.
- **Reconcile at the end.** Before closing a phase, mark each todo done (with a
  note on what it produced) or carry the remainder into the next phase.
