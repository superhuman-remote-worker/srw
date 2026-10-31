# Phase Patterns & Worked Example

Reference for the [todo-guide](../SKILL.md) skill: example todos for each phase
type, plus a full multi-phase worked example. Read this when you want a concrete
model to copy while planning a phase.

> Where a step says `kb_write`, use the knowledge base if you have it; otherwise
> record to `notes/` (e.g. `notes/research_notes.md`).

## Contents
- Phase patterns: Research · Elaboration · Execution · Batch · Integration · Verification
- Worked example: a multi-phase research paper

## Phase patterns

### 1. Research — do this first for unfamiliar topics
Understand the domain before committing to an approach.
- "Web search 'topic X state of the art 2025'; record key findings via `kb_write` (or `notes/research_notes.md`)"
- "Read `documents/brief.pdf` pages 1–10; record key themes the same way"
- "Web search 'best practices for Y'; note the common approaches"
- "Read `documents/example_output.pdf` to learn the expected format and style"

### 2. Elaboration — plan the details before executing
Turn a rough plan into a concrete, sequenced breakdown.
- "Read `plan.md` and break Phase 3 into specific sub-tasks with file paths"
- "Create an outline for `output/report.md` with section headers + bullets"
- "Map which source documents feed which sections of the deliverable"
- "Update `plan.md` with the detailed breakdown for the next 2 phases"

### 3. Execution — produce one specific section or artifact
- "Write `output/chapter2.md` §2.1 (Market Analysis) from `documents/market_*.pdf`, citing each claim as you write it"
- "Write `output/chapter2.md` §2.2 (Competitor Landscape) from the phase-3 findings, citing as you go"
- "Fix or withdraw the unverified citations of `output/chapter2.md` (`list_citations` with status `failed`; `edit_citation` / `withdraw_citation`)"
- "Verify `output/chapter2.md`: all sections present, every sourced claim carries a handle, 800–1200 words"

Citing is part of writing a section, not a todo of its own: a separate "add
citations" pass afterwards produces sections whose claims never get their
handles (the bookmark effect).

### 4. Batch processing — repetitive operations over many items
Process N similar items without a strategic review between each (go to 5–7 todos).
- "Process `documents/input_01.pdf`–`input_05.pdf`: extract key findings to `output/findings.md`"
- "Tag documents 1–10 using the classification schema (from `kb_search` or `notes/classification.md`)"
- "Run a web search for each of the 5 case-study cities; save notes to `output/case_studies/`"
- "Verify all 5 items: output files exist and contain the expected content"

### 5. Integration — combine and cross-reference
Merge separately-produced parts into a coherent whole.
- "Read all chapter files in `output/`; check terminology consistency"
- "Write `output/introduction.md` referencing key findings from chapters 1–4"
- "Write `output/conclusion.md` tying back to the objectives in `instructions.md`"
- "Create `output/references.md` with every citation used across chapters"
- "Final read-through of `output/report.md`: flow, cross-references, completeness"

### 6. Verification — confirm quality before declaring done
A systematic quality check. The `verify-before-done` skill covers *how* to
produce evidence; these are the todos that schedule it.
- "Compare the `output/` file list against the required deliverables in `instructions.md`"
- "Verify `output/report.md` §1–3: every required topic covered per `instructions.md`"
- "Run the tests from `instructions.md`; record pass/fail to `output/test_results.md`"
- "Check every citation resolves to a real source in the library"

## Worked example: a multi-phase research paper

**Instructions:** *"Write a 15-page research paper on sustainable urban transport
with ≥20 citations. Sections: Introduction, Background, Policy Analysis, Case
Studies (3 cities), Recommendations, Conclusion."*

- **Strategic 1 (init):** read instructions, create a rough `plan.md`, set up the workspace.
- **Tactical 1 — Domain research (5):** web-search the overview, policy frameworks, and candidate cities; read + summarize into `notes/`; record findings.
- **Strategic 2:** review research, elaborate the plan with specific sections.
- **Tactical 2 — Case-study research (5):** one search per city, save per-city notes, record findings.
- **Strategic 3:** review notes, plan the writing phases.
- **Tactical 3 — Write Intro + Background (5):** create `output/paper.md` structure; write Introduction (300–400 w), citing as you go; write Background (600–800 w), citing as you go; fix or withdraw the unverified citations; verify sections / word counts / citations.
- **Tactical 4…N — one section (or two closely-related ones) per phase**, same shape.
- **Tactical N — Final integration (5):** write Recommendations + Conclusion, citing as you go; fix or withdraw any unverified citation left; full read-through for flow (the footnote definitions are appended at delivery); verify all `instructions.md` requirements (page count, citation count, sections).

The pattern: **research → elaborate → write one section per phase → integrate**,
with a strategic review between each, and a verification todo closing every
writing phase.
