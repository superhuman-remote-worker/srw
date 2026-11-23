---
name: research-guide
description: Use when researching or exploring a question — methodology for framing it, searching broadly, citing every claim, and persisting findings.
display_name: Research Guide
icon: science
color: "#f9e2af"
tags:
  - research
  - methodology
---

# Research Guide

This guide covers research methodology for your exploration work.

## Research Workflow

1. **Define the question** — What specific thing are you trying to learn? Write it down before searching.
2. **Search broadly** — Use `web_search` with varied queries. Don't stop at the first result.
3. **Save sources** — Use `cite_web` or `cite_document` for every factual claim. No citation = no claim.
4. **Persist findings** — Write results to workspace files immediately. Don't hold findings only in memory.
5. **Synthesize** — After gathering sources, write a summary connecting findings to the task.

## Tool Usage

### Web Research
- `web_search` — broad keyword/topic search; vary your queries
- `extract_webpage` — fetch full content from a promising result
- `research_topic` — automated multi-source research workflow

### Academic Sources
- `search_papers` — find academic papers (arxiv or Semantic Scholar)
- `download_paper` — save the paper PDF to `documents/`
- `get_paper_info` — metadata lookup

### Document Analysis
- `read_file` — read a document by path; supports text lines (offset/limit) and document pages (page_start/page_end)
- `get_document_info` — page count and structure preview before opening a large document

### Citations
Every factual or technical claim must cite its source:
- `cite_web` — verify a quoted claim against a URL
- `cite_document` — verify a quoted claim against a workspace document by page or section

See each tool's description for the exact wire format and arguments.

## Output Conventions

- Save idea artifacts to `output/ideas/` — one file per idea with evidence and proposal.
- Save experiment results to `output/experiments/` — include methodology and findings.
- Save raw research notes to `notes/` — these are working files, not deliverables.
- Reference material goes in `reference/` — domain knowledge for future phases.

## Anti-Patterns

- Don't research without a question. "Learn about X" is not a research task. "What are the top 3 approaches to X and their tradeoffs?" is.
- Don't cite from memory. If you can't point to a source, it's not a fact — it's an assumption.
- Don't hold findings in context only. Context gets compacted. Write findings to files.
- Don't deep-dive on the first interesting result. Breadth first, depth second.
