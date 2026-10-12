#!/usr/bin/env python3
"""Diagnostic for Gemma 4 reasoning-channel and tool-call wire formats.

Hits the workstation router at https://ai.h4ll.app/v1 against both Gemma 4
models and runs five checks per model:

  A. Thinking prompt, NON-streaming, default chat-template settings
     - Does the model emit thinking content?
     - Does vLLM's `--reasoning-parser gemma4` lift it into `reasoning_content`?
     - Does the user-facing `content` contain leaked `<|channel>thought` /
       `<channel|>` delimiters?

  B. Thinking prompt, STREAMING, default chat-template settings
     - Same checks against the accumulated stream + per-chunk delta inspection
       (token-boundary parser bugs only show up on the streaming path).

  C. Tool-call prompt, NON-streaming, structured `tools=[]`
     - Pass an OpenAI-format tool definition.
     - Does the model emit a tool call?
     - Does vLLM's `--tool-call-parser gemma4` lift it into structured
       `tool_calls`, or does it leak as content text?
     - If it leaks, inspect the wire format used (canonical braces vs Python
       parens vs JSON) — confirms the pattern from job 3c30d72e.

  D. Thinking prompt, with `chat_template_kwargs.enable_thinking=True`
     forced via `extra_body`. Both non-stream (D) and streaming (D_stream).
     Probes whether explicit thinking-mode activation reproduces the
     `<|channel>thought ... <channel|>` leak observed in the cockpit
     screenshot. The default chat template may keep thinking off — Gemma
     only emits the thought channel when explicitly told to.

  E. Tool-call prompt, NON-streaming, **NO** structured `tools=[]`. The tool
     definition is embedded in the system prompt as text along with the
     canonical Gemma wire-format teaching block. Probes whether the absence
     of the OpenAI tool envelope causes the model to drift to Python-style
     parens or JSON-quoted-key syntax — a hypothesis for why the agent
     looped 1385× in job 3c30d72e even though scenario C works correctly.

  F. Production-shape tool-call replay, NON-streaming, structured `tools=[]`
     (parser ACTIVE), large multi-system-message context resembling the
     Gemma worker stack at iter ~18-21 of job 3c30d72e: rendered
     `systemprompt_gemma` + tactical phase directive (~2k tokens), task
     instructions (~400 tokens), ~38 OpenAI-format tool definitions, and a
     multi-turn history with prior canonical tool_calls. The final `user`
     turn is the verbatim format-neutral recovery nudge that
     `src/graph.py:1373` now emits via the guardrails matrix
     (`config/guardrails/gemma.yaml` `todo_action` + the
     pending-todos suffix). If the assistant turn comes back as parens
     despite the patched prompts AND an active parser, the trigger is
     prompt scale or shape — not the prompts that were patched in the
     2026-05-05 session.

  G. Thinking prompt with `<|think|>` literal token as a system-prompt
     prefix. Probes whether Gemma's chat template gates the
     `<|channel>thought` block on a literal token in the prompt rather
     than on `chat_template_kwargs.enable_thinking` (which scenario D
     established does nothing on this deployment).

  H. Thinking prompt with a `developer`-role system message that
     explicitly instructs the thought-channel format. OpenAI introduced
     `developer` role in late 2024; some chat templates branch on it
     for instruction-priority reasons. If the request fails outright,
     that's still data — the router or vLLM doesn't accept that role.

  I. Thinking prompt combined with `tools=[]` envelope and
     `tool_choice="none"` (so the model isn't forced to call). Tests
     whether tools-mode forces thinking off — production worker
     requests always ship with `tools=[]`, so if tools-mode kills
     thinking, our agent will never see `reasoning_content` regardless
     of any activation knob we find.

  J. Thinking prompt with BOTH `chat_template_kwargs.enable_thinking=True`
     AND `skip_special_tokens=False`. Per vLLM Issue #38855, vLLM's
     tokenizer detokenization runs with `skip_special_tokens=True` by
     default and strips `<|channel>thought` / `<channel|>` from the
     text stream BEFORE the gemma4 reasoning parser scans for them —
     so even when the model is correctly emitting thought channels
     (which scenario D's tripled completion_tokens already proved),
     the parser sees text with no delimiters and returns empty.
     Passing `skip_special_tokens=False` preserves the delimiters.
     This is the two-knob combo from the vLLM Gemma 4 recipe page.

  K. Same as J but streaming. vLLM's streaming reasoning parser is a
     separate incremental state machine that matches delimiters across
     chunk boundaries; it may have its own #38855 variant fixed (or
     not) by the same flag. If K leaks where J doesn't, that pinpoints
     a streaming-only parser bug — exactly the fault profile that
     would explain the cockpit `<|channel>thought` leak observed on
     the persistent (streaming) path.

Plus a one-shot deployment-introspection probe per model: `GET
/v1/models/{id}` and `POST /v1/tokenize` with `add_generation_prompt=
True`. The rendered prompt from the latter shows what tokens the chat
template actually inserts — including any thinking-mode opener it adds
automatically. None of these endpoints are required by the OpenAI spec;
the router or vLLM may not expose them. Whatever comes back is printed
raw for the operator to read.

Distinguishes three failure surfaces:
  - vLLM bug (parser receives content but fails to lift) — reasoning_content
    empty AND content has leaked delimiters
  - Cockpit bug (parser works, UI drops the field) — reasoning_content
    populated AND content clean → the leak is downstream
  - Streaming-only bug — case B leaks but case A doesn't

Usage:
    GEMMA_API_KEY=sk-... python tests/manual_test_gemma_reasoning.py

GEMMA_API_KEY is required (no key is embedded). Pass --model to test a
single model:

    python tests/manual_test_gemma_reasoning.py --model gemma-4-moe

Exit code is 0 if the script ran to completion (regardless of findings); the
findings are reported to stdout in a per-model breakdown plus a final summary
table. The script does not assert anything — it's a diagnostic, not a test.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any

import httpx

BASE_URL = "https://ai.h4ll.app/v1"
DEFAULT_API_KEY = ""  # never embed a key here — pass GEMMA_API_KEY
DEFAULT_MODELS = ["gemma-4-moe", "gemma-4-31b"]

# Patterns that should NEVER appear in `content` if vLLM's parsers are doing
# their job. Their presence in `content` is the diagnostic signal.
REASONING_DELIMITERS = [
    "<|channel>",
    "<channel|>",
    "<|channel>thought",
]
TOOL_CALL_DELIMITERS = [
    "<|tool_call>",
    "<tool_call|>",
    "<|tool_response>",
    "<tool_response|>",
]
# Wire-format variants we want to detect inside leaked tool calls.
WIRE_FORMAT_PATTERNS = {
    "canonical_braces": re.compile(r"<\|tool_call>call:\w+\{[^}]*\}<tool_call\|>"),
    "python_parens": re.compile(r"<\|tool_call>call:\w+\([^)]*\)<tool_call\|>"),
    "json_quoted_keys": re.compile(r'<\|tool_call>call:\w+\{\s*"\w+":'),
    "equals_sign": re.compile(r"<\|tool_call>call:\w+\{[^}]*\w+="),
}

THINKING_PROMPT = (
    "Think step by step, then give a one-line final answer. "
    "What is 17 multiplied by 23?"
)
TOOL_PROMPT = (
    "What is the current weather in Tokyo? Use the get_weather tool. "
    "If you need to think first, do so, then call the tool."
)

# Scenario E system prompt — teaches Gemma's canonical wire format and
# describes the weather tool inline, instead of passing it through the
# OpenAI `tools=[]` envelope. Mirrors how an agent harness might present
# tools when it doesn't structure the call (or when tool-binding is
# bypassed).  The format anchor here is intentionally close to what
# the tactical phase skill ships, so any drift the model exhibits is
# attributable to the same prompt class our agent emits.
TOOL_IN_PROMPT_SYSTEM = """You are a helpful assistant with one tool available.

## Tool Call Format (load-bearing)

Every tool call MUST use this exact wire format:

```
<|tool_call>call:TOOL_NAME{arg:<|"|>string val<|"|>,num:42,flag:true}<tool_call|>
```

- Curly braces — never parentheses.
- String values wrapped in `<|"|>...<|"|>` — never `"..."` or `'...'`.
- Numbers, booleans, null appear bare: `count:42`, `enabled:true`, `value:null`.
- Closing tag `<tool_call|>` (pipe on right).
- One call per turn.

## Tool: get_weather

Get the current weather for a city.

- `city` (string, required): City name, e.g. `Tokyo` or `London`.
- `units` (string, optional, one of `celsius` / `fahrenheit`): Temperature units.

Example: `<|tool_call>call:get_weather{city:<|"|>Berlin<|"|>,units:<|"|>celsius<|"|>}<tool_call|>`

When the user asks for weather, emit exactly one tool call in the format above.
"""
WEATHER_TOOL = {
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Get the current weather for a city.",
        "parameters": {
            "type": "object",
            "properties": {
                "city": {
                    "type": "string",
                    "description": "City name, e.g. 'Tokyo' or 'London'",
                },
                "units": {
                    "type": "string",
                    "enum": ["celsius", "fahrenheit"],
                    "description": "Temperature units",
                },
            },
            "required": ["city"],
        },
    },
}

# ---------------------------------------------------------------------------
# Scenario F — production-shape replay of job 3c30d72e mid-job conditions.
# The system prompt mirrors `config/prompts/systemprompt_gemma.txt` rendered
# with realistic worker placeholders (scholar role, has_tool("kb_write")
# branch). Phase guidance mirrors `config/skills/tactical-phase/SKILL.md`. The
# recovery nudge is the verbatim format-neutral output of
# `format_nudge("todo_action", model="gemma-4-moe", todo_id="todo_1")` (see
# `config/guardrails/gemma.yaml`) plus the pending-todos suffix wired in
# `src/graph.py:1380`. Drift in the rendered nudge wording is what we are
# testing for — do not paraphrase.
# ---------------------------------------------------------------------------

PROD_SYSTEM_PROMPT = """\
# Instruction Priority

system prompt > phase directive > knowledge base > plan.md > instructions.md > tool results > conversation history.
Higher priority wins on conflict.

# Identity

You are scholar, an autonomous agent that completes long, multiphase tasks within a managed workspace.
You are a research and synthesis specialist with deep familiarity with academic writing, source evaluation, and citation discipline.

# Tool Call Format

When you invoke a tool, emit EXACTLY this wire format and nothing else on that line:

```
<|tool_call>call:TOOL_NAME{arg1:<|"|>string value<|"|>,arg2:42,arg3:true}<tool_call|>
```

Rules:
- Curly braces around arguments. Never parentheses.
- String values wrapped in `<|"|>` ... `<|"|>`. Never regular quotes `"..."` or `'...'`.
- Numbers and booleans appear bare: `count:42`, `enabled:true`, `score:0.95`.
- Comma-separate arguments, no spaces between them.
- Closing tag is `<tool_call|>` (pipe on the right). Not `<|tool_call|>`.

Examples:

```
<|tool_call>call:read_file{path:<|"|>workspace.md<|"|>}<tool_call|>
<|tool_call>call:todo_complete{todo_id:<|"|>todo_1<|"|>}<tool_call|>
<|tool_call>call:next_phase_todos{phase:2,reason:<|"|>research complete<|"|>}<tool_call|>
```

Calls in any other format — Python-style `fn(arg="x")`, JSON-style with quoted keys, equals signs `key=value`, single quotes — will not be parsed and your call will silently fail. The harness will detect repeated parser failures and abort the job.

Make exactly one tool call per turn. Do not chain multiple tool-call blocks in a single response.

# Memory Model

Your context window is finite. These systems are your persistent memory:

- **Knowledge base** — Persistent notes stored via kb_write/kb_search/kb_update. Injected every LLM call as transient messages. Your ground truth for facts, decisions, and discovered information.
- **Memory system** — Automatically extracted insights recalled via hybrid search. Injected every LLM call as transient messages. Persists across context compaction automatically.
- **plan.md** — Execution plan. Read when you need strategic context; it is NOT in your prompt.
- **todos.yaml** — Current phase tasks, injected every call. Work them in order.
- **archive/** — Phase retrospectives and archived todos.
- **tools/** — Per-tool documentation. Read tools/<tool_name>.md before first use.

Use kb_write to persist important facts and decisions to the knowledge base. Write important state EARLY — context can be compacted at any time.

# Constraints

1. Produce substantive output. Headings-only skeletons, placeholder text, and structural scaffolding without real content are NOT acceptable deliverables.
2. Verify content quality, not just file existence.
3. Call `todo_complete` immediately when a todo is done. Never edit files without completing the corresponding todo.
4. One todo at a time. Verify results before proceeding. Never batch-complete.
5. After context compaction, the memory system persists automatically — review the injected context and continue.
6. Record critical rules using kb_write so they persist in the knowledge base across compaction.
7. Distinguish planned vs. executed. Never report work as done that you have not verified.
8. Run the exact verification steps from instructions. Do not substitute generic checks.
9. Before calling `job_complete`, cross-check every deliverable against original requirements.
10. Report confidence honestly. Empty files or skeleton content = confidence 0.1.

# Phase Directive

You are in TACTICAL mode. Purpose: execute the current task with focused, sequential action.

**Primary constraint**: Stay on the current task. Do not start new tasks, reorganize the plan, or explore tangents.

## Tool Call Format (load-bearing)

Every tool call this phase MUST use this exact wire format:

```
<|tool_call>call:TOOL_NAME{arg:<|"|>string val<|"|>,num:42,flag:true}<tool_call|>
```

- Curly braces — never parentheses.
- String values wrapped in `<|"|>...<|"|>` — never `"..."` or `'...'`.
- Numbers, booleans, null appear bare.
- Closing tag `<tool_call|>` (pipe on right).
- One call per turn.

Concrete examples for tools you will use most:

```
<|tool_call>call:read_file{path:<|"|>plan.md<|"|>}<tool_call|>
<|tool_call>call:write_file{path:<|"|>output/notes.md<|"|>,content:<|"|>...<|"|>}<tool_call|>
<|tool_call>call:edit_file{path:<|"|>src/main.py<|"|>,old_string:<|"|>foo<|"|>,new_string:<|"|>bar<|"|>}<tool_call|>
<|tool_call>call:run_command{command:<|"|>pytest tests/ -x<|"|>}<tool_call|>
<|tool_call>call:kb_write{type:<|"|>state<|"|>,tag:<|"|>blocker<|"|>,content:<|"|>...<|"|>}<tool_call|>
<|tool_call>call:todo_complete{todo_id:<|"|>todo_3<|"|>,result:<|"|>verified<|"|>}<tool_call|>
```

Malformed calls (Python-style `fn(arg="x")`, JSON-quoted-key style, `key=value`) will not be parsed and silently fail. The harness will abort the job after a few consecutive parser failures.

## Execution Protocol

1. Identify the single next action from your task list.
2. Read any files you need before modifying them.
3. Execute the action using the appropriate tool with the format shown above.
4. Verify the result — check for errors, confirm expected output.
5. If a tool call fails, do NOT proceed as if it succeeded.
6. Proceed to the next action, or signal completion if the task is done.

## Output Discipline

- One tool call per turn. Do not chain multiple `<|tool_call>...<tool_call|>` blocks in a single response.
- When all todos for this phase are complete, call `todo_complete` on the last one and stop.
- If your last tool call returned no result (silent parser failure), re-emit using the exact format above. Do not retry the same malformed syntax.

# Identity Anchor

You are scholar. Ground claims in evidence. Persist important state to the knowledge base. Deliver substantive work, not organizational scaffolding.

Tool-call format reminder: `<|tool_call>call:NAME{key:<|"|>val<|"|>,n:42}<tool_call|>` — braces, `<|"|>` for strings, bare numbers/booleans, closing tag `<tool_call|>`. One call per turn.
"""

PROD_INSTRUCTIONS = """\
# Task instructions

Goal: Produce a written brief on speculative decoding for LLM serving, ~1500-2000 words, with at least 8 sources cited via kb_write entries.

## Acceptance criteria

1. `output/brief.md` exists and contains substantive content (>1500 words).
2. At least 8 distinct sources cited (papers, blog posts, vendor docs).
3. Coverage of: definition, motivation, decoding strategies (draft model, EAGLE, Medusa), throughput vs latency tradeoffs, current production deployments.
4. All factual claims grounded in a kb_write source entry — no unsupported claims.
5. `workspace.md` updated with the final outline and source list.

## Tools available this phase

- `kb_search`, `kb_read`, `kb_write`, `kb_update` — knowledge base access
- `web_search`, `extract_webpage` — research
- `search_papers`, `download_paper` — academic search
- `read_file`, `write_file`, `edit_file`, `move_file`, `list_dir` — workspace
- `cite_web`, `cite_document` — citations
- `todo_complete`, `request_replan`, `next_phase_todos`, `mark_complete` — todo management
- `run_command`, `shell_execute`, `shell_read` — shell

## Workflow

For each todo: read sources first → draft section → verify quality → call `todo_complete` with a one-line result summary.
"""


def _arg_str(desc: str = "") -> dict[str, Any]:
    return {"type": "string", "description": desc}


def _arg_int(desc: str = "") -> dict[str, Any]:
    return {"type": "integer", "description": desc}


def _build_prod_tools() -> list[dict[str, Any]]:
    """Build ~38 OpenAI-format tool defs that mirror the worker's registry."""
    tools: list[dict[str, Any]] = []

    def t(
        name: str,
        desc: str,
        params: dict[str, Any],
        required: list[str] | None = None,
    ) -> None:
        tools.append(
            {
                "type": "function",
                "function": {
                    "name": name,
                    "description": desc,
                    "parameters": {
                        "type": "object",
                        "properties": params,
                        "required": required or [],
                    },
                },
            }
        )

    t(
        "read_file",
        "Read a file from the workspace.",
        {
            "path": _arg_str("Path relative to workspace root"),
            "offset": _arg_int("Line offset (optional)"),
            "limit": _arg_int("Max lines to read (optional)"),
        },
        ["path"],
    )
    t(
        "write_file",
        "Create or overwrite a workspace file.",
        {"path": _arg_str(), "content": _arg_str()},
        ["path", "content"],
    )
    t(
        "edit_file",
        "Replace text within an existing file.",
        {
            "path": _arg_str(),
            "old_string": _arg_str(),
            "new_string": _arg_str(),
            "position": _arg_str("'replace' (default), 'start', or 'end'"),
        },
        ["path"],
    )
    t(
        "move_file",
        "Move or rename a file.",
        {"source": _arg_str(), "dest": _arg_str()},
        ["source", "dest"],
    )
    t("list_dir", "List directory contents.", {"path": _arg_str()}, ["path"])
    t(
        "get_document_info",
        "Inspect document structure (PDF page count, etc).",
        {"path": _arg_str()},
        ["path"],
    )
    t(
        "kb_search",
        "Hybrid search over the project knowledge base.",
        {"query": _arg_str(), "max_results": _arg_int()},
        ["query"],
    )
    t("kb_read", "Read a knowledge note by slug.", {"note": _arg_str()}, ["note"])
    t(
        "kb_write",
        "Write a new knowledge note.",
        {
            "title": _arg_str(),
            "type": _arg_str("decision | learning | state | source"),
            "content": _arg_str(),
            "tags": {"type": "array", "items": {"type": "string"}},
        },
        ["title", "type", "content"],
    )
    t(
        "kb_update",
        "Update an existing knowledge note.",
        {
            "note": _arg_str(),
            "append": _arg_str(),
            "status": _arg_str(),
            "confidence": _arg_str(),
        },
        ["note"],
    )
    t(
        "cite_web",
        "Record a web-source citation.",
        {"text": _arg_str(), "url": _arg_str(), "claim": _arg_str()},
        ["text", "url", "claim"],
    )
    t(
        "cite_document",
        "Record a document-source citation.",
        {
            "text": _arg_str(),
            "document_path": _arg_str(),
            "page": _arg_int(),
            "claim": _arg_str(),
        },
        ["text", "document_path", "claim"],
    )
    t(
        "web_search",
        "Search the public web.",
        {
            "query": _arg_str(),
            "topic": _arg_str(),
            "time_range": _arg_str(),
            "search_depth": _arg_str(),
        },
        ["query"],
    )
    t(
        "extract_webpage",
        "Fetch and extract text from one or more URLs.",
        {"urls": _arg_str(), "query": _arg_str()},
        ["urls"],
    )
    t(
        "search_papers",
        "Search academic literature (arxiv, semantic scholar).",
        {"query": _arg_str(), "source": _arg_str(), "max_results": _arg_int()},
        ["query"],
    )
    t(
        "download_paper",
        "Fetch a paper PDF by DOI / arxiv ID / URL.",
        {"identifier": _arg_str()},
        ["identifier"],
    )
    t(
        "run_command",
        "Run a one-shot shell command.",
        {"command": _arg_str(), "timeout": _arg_int(), "tail": _arg_int()},
        ["command"],
    )
    t(
        "shell_execute",
        "Execute in a named (possibly persistent) shell tab.",
        {
            "command": _arg_str(),
            "name": _arg_str(),
            "is_async": {"type": "boolean"},
            "keys": {"type": "boolean"},
        },
        ["command"],
    )
    t(
        "shell_read",
        "Read output from a shell tab.",
        {"name": _arg_str(), "lines": _arg_int(), "offset": _arg_int()},
    )
    t("shell_status", "Show status of all shell tabs.", {})
    t("shell_close", "Close a shell tab.", {"name": _arg_str()}, ["name"])
    t("git_status", "Show working-copy status.", {})
    t(
        "git_log",
        "Show commit log.",
        {"max_count": _arg_int(), "oneline": {"type": "boolean"}},
    )
    t(
        "git_show",
        "Show a commit.",
        {"commit_ref": _arg_str(), "stat_only": {"type": "boolean"}},
    )
    t(
        "git_diff",
        "Show diff between refs or working copy.",
        {"ref1": _arg_str(), "ref2": _arg_str(), "file_path": _arg_str()},
    )
    t(
        "git_tags",
        "List phase tags.",
        {"pattern": _arg_str(), "all_jobs": {"type": "boolean"}},
    )
    t(
        "git_merge_squash",
        "Squash-merge a subagent branch.",
        {"branch": _arg_str(), "commit_message": _arg_str()},
        ["branch"],
    )
    t(
        "git_worktree_cleanup",
        "Remove a subagent worktree.",
        {"branch": _arg_str(), "force": {"type": "boolean"}},
        ["branch"],
    )
    t(
        "delegate_agent",
        "Delegate one bounded brief to a roster subagent; returns its report.",
        {
            "description": _arg_str(),
            "prompt": _arg_str(),
            "subagent_type": _arg_str(),
        },
        ["description", "prompt", "subagent_type"],
    )
    t(
        "todo_complete",
        "Mark a todo as complete. Pass todo_id for a specific id, or omit for the first pending todo.",
        {
            "todo_id": _arg_str(
                "Todo id, e.g. 'todo_3'. Omit to complete the first pending."
            ),
            "result": _arg_str("One-line summary of what was accomplished"),
        },
    )
    t(
        "request_replan",
        "Reopen a previously completed todo.",
        {"todo_id": _arg_str()},
        ["todo_id"],
    )
    t(
        "next_phase_todos",
        "Stage todos for the next tactical phase.",
        {
            "todos": {"type": "array", "items": {"type": "string"}},
            "phase_name": _arg_str(),
            "reason": _arg_str(),
        },
        ["todos"],
    )
    t("mark_complete", "Signal that the current phase is complete.", {})
    t(
        "job_complete",
        "Mark the entire job as complete.",
        {"summary": _arg_str(), "confidence": {"type": "number"}},
    )
    t(
        "memory_pin",
        "Pin a memory so it stays in context across compaction.",
        {"memory_id": _arg_str()},
        ["memory_id"],
    )
    t(
        "memory_unpin",
        "Unpin a previously pinned memory.",
        {"memory_id": _arg_str()},
        ["memory_id"],
    )
    t("kb_archive", "Archive a knowledge note.", {"note": _arg_str()}, ["note"])
    t(
        "knowledge_index",
        "Reindex the knowledge base.",
        {"force": {"type": "boolean"}},
    )
    return tools


_KB_SEARCH_RESULT = """\
3 results for 'speculative decoding decoding strategies':

1. note: speculative-decoding-overview (decision, 2026-04-22)
   Title: Use draft+target two-stage decoding for vLLM 0.19
   Excerpt: Speculative decoding accelerates inference by speculating k tokens with a small draft model, then verifying them in parallel with the target model. Acceptance rates of 60-80% on chat workloads translate to 1.7-2.5x throughput at modest quality loss. Critical knobs: draft length k, temperature gating, KV-cache stewardship.
   Tags: [serving, throughput, decoding]

2. note: eagle-vs-medusa (learning, 2026-04-23)
   Title: EAGLE outperforms Medusa on long-context workloads
   Excerpt: EAGLE-2 (Li et al. 2024) extends speculation by sharing hidden states between draft and target. On 8k+ contexts our internal benchmark showed 1.4x advantage over Medusa-2 due to better acceptance rates on later tokens. Medusa is simpler to deploy.
   Tags: [eagle, medusa, decoding, paper]

3. note: vllm-spec-decoding-config (state, 2026-04-21)
   Title: vLLM 0.19 speculative decoding config knobs
   Excerpt: Set --speculative-config '{"method":"draft_model","model":"...","num_speculative_tokens":5}' on the entrypoint. Draft model must share tokenizer with target. Acceptance metric exposed as 'spec_acceptance_rate' in /metrics.
   Tags: [vllm, ops, config]
"""

_PLAN_MD = """\
# Plan: Speculative Decoding Brief

Goal: Produce a 1500-2000 word brief on speculative decoding for LLM serving with at least 8 cited sources.

## Phase 1 (research, current)

todos:
  - todo_1: Search KB and external sources for prior art on draft-model speculation
  - todo_2: Identify the key research questions and define the brief's outline
  - todo_3: Draft Section 1 (definition + motivation) and verify against sources

## Phase 2 (drafting, planned)

todos:
  - Draft Sections 2-4 (decoding strategies, throughput/latency, deployments)
  - Cross-check every factual claim against a cited source
  - Run a final substance audit

## Phase 3 (review)

  - Self-review for placeholder text, missing citations, and acceptance-criteria gaps
  - Call `job_complete` with confidence reflecting verified outcomes
"""

_WRITE_RESULT = "OK — wrote 421 lines to output/section_1.md."

# The verbatim format-neutral nudge produced by graph.py:1373 via
# format_nudge("todo_action", model="gemma-4-moe", todo_id="todo_1") plus
# the suffix wired inline at graph.py:1380. Drift in this string is what
# we are testing for — do not paraphrase.
PROD_RECOVERY_NUDGE = (
    "Action required: complete the current todo `todo_1` now by invoking the `todo_complete` tool.\n"
    'The Gemma tool-call format is `<|tool_call>call:fn{key:<|"|>val<|"|>}<tool_call|>` — use braces, never parens.\n\n'
    "If you already finished the work for this todo, that's perfectly fine — "
    "invoke `todo_complete` now to record it. You don't need to redo anything.\n\n"
    "Pending todos (3):\n"
    "  - todo_1: Search KB and external sources for prior art on draft-model speculation\n"
    "  - todo_2: Identify the key research questions and define the brief's outline\n"
    "  - todo_3: Draft Section 1 (definition + motivation) and verify against sources"
)


def _build_prod_history() -> list[dict[str, Any]]:
    """Mid-job conversation history mirroring iter ~18-21 of job 3c30d72e.

    Prior assistant turns return structured `tool_calls` (vLLM's parser
    correctly lifted them from canonical wire format). The final user
    turn is the recovery nudge — the model is expected to respond by
    calling `todo_complete{todo_id:<|"|>todo_1<|"|>}`.
    """
    return [
        {"role": "system", "content": PROD_SYSTEM_PROMPT},
        {"role": "system", "content": PROD_INSTRUCTIONS},
        {
            "role": "user",
            "content": "Begin work on the speculative-decoding brief. Start with the research and outline phase.",
        },
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "id": "call_kb_1",
                    "type": "function",
                    "function": {
                        "name": "kb_search",
                        "arguments": json.dumps(
                            {
                                "query": "speculative decoding decoding strategies",
                                "max_results": 5,
                            }
                        ),
                    },
                }
            ],
        },
        {"role": "tool", "tool_call_id": "call_kb_1", "content": _KB_SEARCH_RESULT},
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "id": "call_read_1",
                    "type": "function",
                    "function": {
                        "name": "read_file",
                        "arguments": json.dumps({"path": "plan.md"}),
                    },
                }
            ],
        },
        {"role": "tool", "tool_call_id": "call_read_1", "content": _PLAN_MD},
        {
            "role": "assistant",
            "content": (
                "I've reviewed the prior knowledge notes and the plan. The "
                "speculative-decoding-overview note covers definition + "
                "motivation cleanly, so I'll lean on it for Section 1 and "
                "supplement with one paper citation."
            ),
            "tool_calls": [
                {
                    "id": "call_write_1",
                    "type": "function",
                    "function": {
                        "name": "write_file",
                        "arguments": json.dumps(
                            {
                                "path": "output/section_1.md",
                                "content": "# Section 1: What is speculative decoding?\n\nSpeculative decoding is a serving-time technique...\n\n[~400 lines of draft, omitted in this trace]",
                            }
                        ),
                    },
                }
            ],
        },
        {"role": "tool", "tool_call_id": "call_write_1", "content": _WRITE_RESULT},
        {"role": "user", "content": PROD_RECOVERY_NUDGE},
    ]


@dataclass
class Finding:
    """One diagnostic observation per (model, scenario)."""

    model: str
    scenario: str  # 'A_think_nonstream' | ... | 'K_think_workaround_stream'
    http_ok: bool
    error: str | None = None
    content: str = ""
    reasoning_content: str | None = None
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    finish_reason: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    # Derived flags
    leaked_reasoning_delims: list[str] = field(default_factory=list)
    leaked_tool_delims: list[str] = field(default_factory=list)
    wire_format_hits: dict[str, int] = field(default_factory=dict)

    def derive_flags(self) -> None:
        for d in REASONING_DELIMITERS:
            if d in self.content:
                self.leaked_reasoning_delims.append(d)
        for d in TOOL_CALL_DELIMITERS:
            if d in self.content:
                self.leaked_tool_delims.append(d)
        for name, pat in WIRE_FORMAT_PATTERNS.items():
            hits = len(pat.findall(self.content))
            if hits:
                self.wire_format_hits[name] = hits


def _post(
    client: httpx.Client,
    api_key: str,
    payload: dict[str, Any],
    stream: bool = False,
) -> httpx.Response:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if stream:
        return client.stream(
            "POST",
            f"{BASE_URL}/chat/completions",
            headers=headers,
            json=payload,
            timeout=120.0,
        )
    return client.post(
        f"{BASE_URL}/chat/completions",
        headers=headers,
        json=payload,
        timeout=120.0,
    )


def list_models(client: httpx.Client, api_key: str) -> list[str]:
    r = client.get(
        f"{BASE_URL}/models",
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=30.0,
    )
    r.raise_for_status()
    return [m["id"] for m in r.json().get("data", [])]


def run_thinking_nonstream(client: httpx.Client, api_key: str, model: str) -> Finding:
    f = Finding(model=model, scenario="A_think_nonstream", http_ok=False)
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": THINKING_PROMPT}],
        "temperature": 0.3,
        "max_tokens": 600,
        "stream": False,
    }
    try:
        r = _post(client, api_key, payload, stream=False)
        r.raise_for_status()
        f.http_ok = True
        body = r.json()
        choice = body["choices"][0]
        msg = choice.get("message", {})
        f.content = msg.get("content") or ""
        # vLLM's reasoning parser surfaces this on `reasoning_content`
        f.reasoning_content = msg.get("reasoning_content")
        f.finish_reason = choice.get("finish_reason")
        usage = body.get("usage", {})
        f.prompt_tokens = usage.get("prompt_tokens")
        f.completion_tokens = usage.get("completion_tokens")
    except httpx.HTTPStatusError as e:
        f.error = f"HTTP {e.response.status_code}: {e.response.text[:300]}"
    except Exception as e:
        f.error = f"{type(e).__name__}: {e}"
    f.derive_flags()
    return f


def run_thinking_stream(client: httpx.Client, api_key: str, model: str) -> Finding:
    f = Finding(model=model, scenario="B_think_stream", http_ok=False)
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": THINKING_PROMPT}],
        "temperature": 0.3,
        "max_tokens": 600,
        "stream": True,
    }
    accumulated_content = []
    accumulated_reasoning = []
    delta_count = 0
    try:
        with _post(client, api_key, payload, stream=True) as r:
            r.raise_for_status()
            f.http_ok = True
            for line in r.iter_lines():
                if not line or not line.startswith("data: "):
                    continue
                data_str = line[len("data: ") :]
                if data_str.strip() == "[DONE]":
                    break
                try:
                    chunk = json.loads(data_str)
                except json.JSONDecodeError:
                    continue
                choice = (chunk.get("choices") or [{}])[0]
                delta = choice.get("delta") or {}
                if "content" in delta and delta["content"]:
                    accumulated_content.append(delta["content"])
                if "reasoning_content" in delta and delta["reasoning_content"]:
                    accumulated_reasoning.append(delta["reasoning_content"])
                if choice.get("finish_reason"):
                    f.finish_reason = choice["finish_reason"]
                delta_count += 1
        f.content = "".join(accumulated_content)
        rc = "".join(accumulated_reasoning)
        f.reasoning_content = rc if rc else None
    except httpx.HTTPStatusError as e:
        f.error = f"HTTP {e.response.status_code}: {e.response.text[:300]}"
    except Exception as e:
        f.error = f"{type(e).__name__}: {e}"
    f.derive_flags()
    return f


def run_tool_call_nonstream(client: httpx.Client, api_key: str, model: str) -> Finding:
    f = Finding(model=model, scenario="C_tool_nonstream", http_ok=False)
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": TOOL_PROMPT}],
        "tools": [WEATHER_TOOL],
        "tool_choice": "auto",
        "temperature": 0.3,
        "max_tokens": 400,
        "stream": False,
    }
    try:
        r = _post(client, api_key, payload, stream=False)
        r.raise_for_status()
        f.http_ok = True
        body = r.json()
        choice = body["choices"][0]
        msg = choice.get("message", {})
        f.content = msg.get("content") or ""
        f.reasoning_content = msg.get("reasoning_content")
        f.tool_calls = msg.get("tool_calls") or []
        f.finish_reason = choice.get("finish_reason")
        usage = body.get("usage", {})
        f.prompt_tokens = usage.get("prompt_tokens")
        f.completion_tokens = usage.get("completion_tokens")
    except httpx.HTTPStatusError as e:
        f.error = f"HTTP {e.response.status_code}: {e.response.text[:300]}"
    except Exception as e:
        f.error = f"{type(e).__name__}: {e}"
    f.derive_flags()
    return f


def run_thinking_force_nonstream(
    client: httpx.Client, api_key: str, model: str
) -> Finding:
    """Scenario D — thinking mode forced ON via chat_template_kwargs.

    vLLM accepts `chat_template_kwargs` as a top-level field on the chat
    completions request and forwards it to the chat template renderer.
    Gemma's chat template gates the `<|channel>thought` block on
    `enable_thinking`, so this should be the regime where the reasoning
    parser is exercised.
    """
    f = Finding(model=model, scenario="D_think_force_nonstream", http_ok=False)
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": THINKING_PROMPT}],
        "temperature": 0.3,
        "max_tokens": 800,
        "stream": False,
        "chat_template_kwargs": {"enable_thinking": True},
    }
    try:
        r = _post(client, api_key, payload, stream=False)
        r.raise_for_status()
        f.http_ok = True
        body = r.json()
        choice = body["choices"][0]
        msg = choice.get("message", {})
        f.content = msg.get("content") or ""
        f.reasoning_content = msg.get("reasoning_content")
        f.finish_reason = choice.get("finish_reason")
        usage = body.get("usage", {})
        f.prompt_tokens = usage.get("prompt_tokens")
        f.completion_tokens = usage.get("completion_tokens")
    except httpx.HTTPStatusError as e:
        f.error = f"HTTP {e.response.status_code}: {e.response.text[:300]}"
    except Exception as e:
        f.error = f"{type(e).__name__}: {e}"
    f.derive_flags()
    return f


def run_thinking_force_stream(
    client: httpx.Client, api_key: str, model: str
) -> Finding:
    """Scenario D_stream — same as D but streaming.

    Streaming exercises a different code path in vLLM's reasoning parser
    (incremental delimiter matching across chunk boundaries). If A and D
    both come back clean but D_stream leaks, that points at the
    streaming-state-machine bug specifically.
    """
    f = Finding(model=model, scenario="D_think_force_stream", http_ok=False)
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": THINKING_PROMPT}],
        "temperature": 0.3,
        "max_tokens": 800,
        "stream": True,
        "chat_template_kwargs": {"enable_thinking": True},
    }
    accumulated_content: list[str] = []
    accumulated_reasoning: list[str] = []
    try:
        with _post(client, api_key, payload, stream=True) as r:
            r.raise_for_status()
            f.http_ok = True
            for line in r.iter_lines():
                if not line or not line.startswith("data: "):
                    continue
                data_str = line[len("data: ") :]
                if data_str.strip() == "[DONE]":
                    break
                try:
                    chunk = json.loads(data_str)
                except json.JSONDecodeError:
                    continue
                choice = (chunk.get("choices") or [{}])[0]
                delta = choice.get("delta") or {}
                if "content" in delta and delta["content"]:
                    accumulated_content.append(delta["content"])
                if "reasoning_content" in delta and delta["reasoning_content"]:
                    accumulated_reasoning.append(delta["reasoning_content"])
                if choice.get("finish_reason"):
                    f.finish_reason = choice["finish_reason"]
        f.content = "".join(accumulated_content)
        rc = "".join(accumulated_reasoning)
        f.reasoning_content = rc if rc else None
    except httpx.HTTPStatusError as e:
        f.error = f"HTTP {e.response.status_code}: {e.response.text[:300]}"
    except Exception as e:
        f.error = f"{type(e).__name__}: {e}"
    f.derive_flags()
    return f


def run_tool_in_prompt_nonstream(
    client: httpx.Client, api_key: str, model: str
) -> Finding:
    """Scenario E — tool described in system prompt, NO `tools=[]` envelope.

    The `gemma4` tool-call parser regex-matches the canonical wire format
    in the assistant's *content* stream. When tools are passed via
    `tools=[]`, vLLM splices a tool-definition block into the rendered
    chat template, and the model has clear cues. When they're not, the
    model has to invent the call from the system-prompt teaching alone.
    Drift to parens, JSON, or `key=value` form means our system prompts
    aren't load-bearing enough on their own — the structured tools
    envelope is what's keeping us out of the parens trap.
    """
    f = Finding(model=model, scenario="E_tool_in_prompt", http_ok=False)
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": TOOL_IN_PROMPT_SYSTEM},
            {"role": "user", "content": "What is the current weather in Tokyo?"},
        ],
        "temperature": 0.3,
        "max_tokens": 400,
        "stream": False,
    }
    try:
        r = _post(client, api_key, payload, stream=False)
        r.raise_for_status()
        f.http_ok = True
        body = r.json()
        choice = body["choices"][0]
        msg = choice.get("message", {})
        f.content = msg.get("content") or ""
        f.reasoning_content = msg.get("reasoning_content")
        f.tool_calls = msg.get("tool_calls") or []
        f.finish_reason = choice.get("finish_reason")
        usage = body.get("usage", {})
        f.prompt_tokens = usage.get("prompt_tokens")
        f.completion_tokens = usage.get("completion_tokens")
    except httpx.HTTPStatusError as e:
        f.error = f"HTTP {e.response.status_code}: {e.response.text[:300]}"
    except Exception as e:
        f.error = f"{type(e).__name__}: {e}"
    f.derive_flags()
    return f


def run_prod_shape_tool_nonstream(
    client: httpx.Client, api_key: str, model: str
) -> Finding:
    """Scenario F — production-shape tool call.

    Mirrors how a Gemma worker submits to vLLM mid-job: structured
    `tools=[]` envelope (parser ACTIVE), large multi-system-message
    context resembling the worker stack at iter ~18-21 of job 3c30d72e,
    ~38 tool definitions, multi-turn history with prior canonical
    tool_calls, and a final user turn that's the verbatim format-neutral
    recovery nudge from `config/guardrails/gemma.yaml`. The model is
    expected to respond with `todo_complete{todo_id:<|"|>todo_1<|"|>}`
    lifted into structured tool_calls.

    Reading the result:
      - tool_calls=1 structured + content=clean + wire=(none) → patched
        prompts hold up at production scale; shipped fixes are sufficient.
      - tool_calls=0 + wire=['python_parens'] → drift even with parser
        active and patched prompts; trigger is prompt scale or shape.
      - wire=['canonical_braces'] but tool_calls=0 → unexpected; parser
        regex matches but vLLM refused to lift (parser bug at scale).
    """
    f = Finding(model=model, scenario="F_prod_shape_tool", http_ok=False)
    payload = {
        "model": model,
        "messages": _build_prod_history(),
        "tools": _build_prod_tools(),
        "tool_choice": "auto",
        "temperature": 0.3,
        "max_tokens": 400,
        "stream": False,
    }
    try:
        r = _post(client, api_key, payload, stream=False)
        r.raise_for_status()
        f.http_ok = True
        body = r.json()
        choice = body["choices"][0]
        msg = choice.get("message", {})
        f.content = msg.get("content") or ""
        f.reasoning_content = msg.get("reasoning_content")
        f.tool_calls = msg.get("tool_calls") or []
        f.finish_reason = choice.get("finish_reason")
        usage = body.get("usage", {})
        f.prompt_tokens = usage.get("prompt_tokens")
        f.completion_tokens = usage.get("completion_tokens")
    except httpx.HTTPStatusError as e:
        f.error = f"HTTP {e.response.status_code}: {e.response.text[:300]}"
    except Exception as e:
        f.error = f"{type(e).__name__}: {e}"
    f.derive_flags()
    return f


def probe_deployment(client: httpx.Client, api_key: str, model: str) -> None:
    """One-time deployment introspection for the thinking-mode activation hunt.

    Probes endpoints that *might* reveal the chat template Jinja or what
    tokens the template inserts automatically. None of these are required
    by the OpenAI spec — the router or vLLM may not expose them. We just
    print whatever comes back so the operator can read.

    The most informative probe is `/v1/tokenize` with
    `add_generation_prompt=True`: it returns the rendered prompt the
    model would actually see, including any thinking-mode opener token
    the chat template adds at render time. If we see something like
    `<|think|>` or `<|channel>thought` in the rendered output, that's
    the activation knob — copy it into a system-prompt prefix on the
    agent side.
    """
    print(f"\n  --- Deployment probes for {model} ---")
    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        r = client.get(
            f"{BASE_URL}/models/{model}",
            headers=headers,
            timeout=15.0,
        )
        if r.status_code < 400:
            body_preview = r.text[:600].replace("\n", " ⏎ ")
            print(f"    GET /v1/models/{model}: {body_preview}")
        else:
            print(f"    GET /v1/models/{model}: HTTP {r.status_code}")
    except Exception as e:
        print(f"    GET /v1/models/{model}: {type(e).__name__}: {e}")

    try:
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": THINKING_PROMPT}],
            "add_generation_prompt": True,
            "add_special_tokens": True,
        }
        r = client.post(
            f"{BASE_URL}/tokenize",
            headers={**headers, "Content-Type": "application/json"},
            json=payload,
            timeout=15.0,
        )
        if r.status_code < 400:
            body = r.json()
            rendered = (
                body.get("prompt")
                or body.get("rendered_prompt")
                or body.get("token_strs")
                or body
            )
            preview = str(rendered)[:1200].replace("\n", " ⏎ ")
            print(f"    POST /v1/tokenize rendered: {preview}")
        else:
            print(f"    POST /v1/tokenize: HTTP {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"    POST /v1/tokenize: {type(e).__name__}: {e}")


def run_thinking_token_prefix(
    client: httpx.Client, api_key: str, model: str
) -> Finding:
    """Scenario G — thinking prompt with `<|think|>` token as system prefix.

    Some chat templates gate the thought channel on a literal token
    rather than a kwarg. If `reasoning_content` populates here but D
    came back empty, the activation pattern is "literal token in
    system prompt", not "kwarg in extra_body".
    """
    f = Finding(model=model, scenario="G_think_token_prefix", http_ok=False)
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "<|think|>"},
            {"role": "user", "content": THINKING_PROMPT},
        ],
        "temperature": 0.3,
        "max_tokens": 800,
        "stream": False,
    }
    try:
        r = _post(client, api_key, payload, stream=False)
        r.raise_for_status()
        f.http_ok = True
        body = r.json()
        choice = body["choices"][0]
        msg = choice.get("message", {})
        f.content = msg.get("content") or ""
        f.reasoning_content = msg.get("reasoning_content")
        f.finish_reason = choice.get("finish_reason")
        usage = body.get("usage", {})
        f.prompt_tokens = usage.get("prompt_tokens")
        f.completion_tokens = usage.get("completion_tokens")
    except httpx.HTTPStatusError as e:
        f.error = f"HTTP {e.response.status_code}: {e.response.text[:300]}"
    except Exception as e:
        f.error = f"{type(e).__name__}: {e}"
    f.derive_flags()
    return f


def run_thinking_developer_role(
    client: httpx.Client, api_key: str, model: str
) -> Finding:
    """Scenario H — `developer`-role system message instructing thought format.

    OpenAI's late-2024 API change introduced `developer` as a role
    distinct from `system`. Some chat templates branch on it for
    instruction-priority reasons. If the deployment rejects the role
    (HTTP 4xx), that's still informative — confirms one hypothesis is
    dead.
    """
    f = Finding(model=model, scenario="H_dev_role_instruction", http_ok=False)
    payload = {
        "model": model,
        "messages": [
            {
                "role": "developer",
                "content": (
                    "Before answering, think step-by-step inside "
                    "`<|channel>thought` ... `<channel|>` markers. "
                    "After the thinking block, give a single one-line "
                    "final answer."
                ),
            },
            {"role": "user", "content": THINKING_PROMPT},
        ],
        "temperature": 0.3,
        "max_tokens": 800,
        "stream": False,
    }
    try:
        r = _post(client, api_key, payload, stream=False)
        r.raise_for_status()
        f.http_ok = True
        body = r.json()
        choice = body["choices"][0]
        msg = choice.get("message", {})
        f.content = msg.get("content") or ""
        f.reasoning_content = msg.get("reasoning_content")
        f.finish_reason = choice.get("finish_reason")
        usage = body.get("usage", {})
        f.prompt_tokens = usage.get("prompt_tokens")
        f.completion_tokens = usage.get("completion_tokens")
    except httpx.HTTPStatusError as e:
        f.error = f"HTTP {e.response.status_code}: {e.response.text[:300]}"
    except Exception as e:
        f.error = f"{type(e).__name__}: {e}"
    f.derive_flags()
    return f


def run_thinking_with_tools(client: httpx.Client, api_key: str, model: str) -> Finding:
    """Scenario I — thinking prompt + `tools=[]` envelope.

    `tool_choice="none"` so the model isn't forced to call get_weather
    on a math question. The hypothesis: tools-mode may force thinking
    off entirely. If true, every production worker request (which
    always ships with `tools=[]`) will never see `reasoning_content`,
    regardless of any activation knob we find — the agent would have
    to choose between tool-calling and thinking, not both.

    Reading the result:
      - reasoning_content populated + content clean → thinking
        coexists with tools mode; activation knob would work in
        production.
      - reasoning_content empty + content clean → tools mode silently
        suppresses thinking on this deployment; need a different
        approach (e.g. a separate non-tool-mode request to capture
        thinking, or a different model).
    """
    f = Finding(model=model, scenario="I_think_with_tools", http_ok=False)
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": THINKING_PROMPT}],
        "tools": [WEATHER_TOOL],
        "tool_choice": "none",
        "temperature": 0.3,
        "max_tokens": 800,
        "stream": False,
    }
    try:
        r = _post(client, api_key, payload, stream=False)
        r.raise_for_status()
        f.http_ok = True
        body = r.json()
        choice = body["choices"][0]
        msg = choice.get("message", {})
        f.content = msg.get("content") or ""
        f.reasoning_content = msg.get("reasoning_content")
        f.tool_calls = msg.get("tool_calls") or []
        f.finish_reason = choice.get("finish_reason")
        usage = body.get("usage", {})
        f.prompt_tokens = usage.get("prompt_tokens")
        f.completion_tokens = usage.get("completion_tokens")
    except httpx.HTTPStatusError as e:
        f.error = f"HTTP {e.response.status_code}: {e.response.text[:300]}"
    except Exception as e:
        f.error = f"{type(e).__name__}: {e}"
    f.derive_flags()
    return f


def run_thinking_workaround_nonstream(
    client: httpx.Client, api_key: str, model: str
) -> Finding:
    """Scenario J — vLLM #38855 workaround: enable_thinking + skip_special_tokens=False.

    Per vLLM Issue #38855: vLLM's tokenizer detokenization runs with
    `skip_special_tokens=True` by default, which strips
    `<|channel>thought` / `<channel|>` from the text stream BEFORE the
    `gemma4` reasoning parser scans for them. The parser then sees
    text with no delimiters, has nothing to lift, and returns empty
    `reasoning_content`. The reasoning prose ends up in `content`
    (without the delimiters, since they were already stripped). This
    matches our scenario-D observation exactly: `completion_tokens=790`
    (model is thinking longer) but `reasoning_content` empty.

    The fix is the two-knob combo from the vLLM Gemma 4 recipe page:
    pass `skip_special_tokens=False` alongside
    `chat_template_kwargs.enable_thinking=True`.

    Reading the result vs. scenario D:
      - D=empty + J=populated → confirms #38855 is the cause; production
        wiring needs `skip_special_tokens=False` for the gemma family.
      - D=empty + J=empty → activation does nothing on this deployment
        regardless of token preservation; chat template doesn't gate on
        `enable_thinking` here, look for a different knob.
      - J=populated + content=clean → ideal: parser working as designed
        once delimiters survive detokenization.
      - J=populated + content=LEAKED `<|channel>thought` → parser
        partially working; some delimiters preserved but not lifted.
    """
    f = Finding(model=model, scenario="J_think_workaround_nonstream", http_ok=False)
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": THINKING_PROMPT}],
        "temperature": 0.3,
        "max_tokens": 800,
        "stream": False,
        "chat_template_kwargs": {"enable_thinking": True},
        "skip_special_tokens": False,
    }
    try:
        r = _post(client, api_key, payload, stream=False)
        r.raise_for_status()
        f.http_ok = True
        body = r.json()
        choice = body["choices"][0]
        msg = choice.get("message", {})
        f.content = msg.get("content") or ""
        f.reasoning_content = msg.get("reasoning_content")
        f.finish_reason = choice.get("finish_reason")
        usage = body.get("usage", {})
        f.prompt_tokens = usage.get("prompt_tokens")
        f.completion_tokens = usage.get("completion_tokens")
    except httpx.HTTPStatusError as e:
        f.error = f"HTTP {e.response.status_code}: {e.response.text[:300]}"
    except Exception as e:
        f.error = f"{type(e).__name__}: {e}"
    f.derive_flags()
    return f


def run_thinking_workaround_stream(
    client: httpx.Client, api_key: str, model: str
) -> Finding:
    """Scenario K — same as J but streaming.

    vLLM's streaming reasoning parser is a separate incremental state
    machine that matches delimiters across chunk boundaries. The
    workaround for #38855 may not apply uniformly — the streaming
    parser could still drop fragments at chunk seams.

    Cross-check vs. cockpit screenshot:
      - K=populated + content=clean → streaming path also fixed by the
        flag. The cockpit `<|channel>thought` leak must be in our
        LangChain wrapper (`src/llm/reasoning_chat.py`) or in the
        cockpit UI dropping reasoning_content into the visible
        content stream.
      - K=empty + content=LEAKED `<|channel>thought` → streaming
        parser doesn't lift even with the flag; cockpit leak traced
        to vLLM streaming bug, not our code. Persistent path needs a
        different mitigation (server-side flag, cockpit-side parsing,
        or skip thinking on persistent path entirely).
      - J=populated + K=empty → flag works for non-stream but not
        stream; production wiring should set the flag on workers and
        keep thinking off on the persistent path until vLLM fixes
        the streaming variant.
    """
    f = Finding(model=model, scenario="K_think_workaround_stream", http_ok=False)
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": THINKING_PROMPT}],
        "temperature": 0.3,
        "max_tokens": 800,
        "stream": True,
        "chat_template_kwargs": {"enable_thinking": True},
        "skip_special_tokens": False,
    }
    accumulated_content: list[str] = []
    accumulated_reasoning: list[str] = []
    try:
        with _post(client, api_key, payload, stream=True) as r:
            r.raise_for_status()
            f.http_ok = True
            for line in r.iter_lines():
                if not line or not line.startswith("data: "):
                    continue
                data_str = line[len("data: ") :]
                if data_str.strip() == "[DONE]":
                    break
                try:
                    chunk = json.loads(data_str)
                except json.JSONDecodeError:
                    continue
                choice = (chunk.get("choices") or [{}])[0]
                delta = choice.get("delta") or {}
                if "content" in delta and delta["content"]:
                    accumulated_content.append(delta["content"])
                if "reasoning_content" in delta and delta["reasoning_content"]:
                    accumulated_reasoning.append(delta["reasoning_content"])
                if choice.get("finish_reason"):
                    f.finish_reason = choice["finish_reason"]
        f.content = "".join(accumulated_content)
        rc = "".join(accumulated_reasoning)
        f.reasoning_content = rc if rc else None
    except httpx.HTTPStatusError as e:
        f.error = f"HTTP {e.response.status_code}: {e.response.text[:300]}"
    except Exception as e:
        f.error = f"{type(e).__name__}: {e}"
    f.derive_flags()
    return f


def _truncate(s: str | None, n: int = 200) -> str:
    if not s:
        return "<empty>"
    s = s.replace("\n", " ⏎ ")
    return s if len(s) <= n else s[:n] + "…"


def report_finding(f: Finding) -> None:
    print(f"\n  [{f.scenario}]")
    if not f.http_ok:
        print(f"    ERROR: {f.error}")
        return
    print(
        f"    finish={f.finish_reason} "
        f"prompt_tokens={f.prompt_tokens} completion_tokens={f.completion_tokens}"
    )
    print(f"    content        : {_truncate(f.content)}")
    if f.reasoning_content is not None:
        print(f"    reasoning_content: {_truncate(f.reasoning_content)}")
    else:
        print("    reasoning_content: <field absent on response>")
    if f.tool_calls:
        for i, tc in enumerate(f.tool_calls):
            fn = tc.get("function") or {}
            print(
                f"    tool_calls[{i}]   : name={fn.get('name')} "
                f"args={_truncate(fn.get('arguments'), 120)}"
            )
    elif f.scenario.startswith("C_"):
        print("    tool_calls       : <empty>")

    if f.leaked_reasoning_delims:
        print(
            f"    *** REASONING DELIMS LEAKED in content: {f.leaked_reasoning_delims}"
        )
    if f.leaked_tool_delims:
        print(f"    *** TOOL-CALL DELIMS LEAKED in content: {f.leaked_tool_delims}")
    if f.wire_format_hits:
        print(f"    wire-format detected: {f.wire_format_hits}")


def diagnose_model(client: httpx.Client, api_key: str, model: str) -> list[Finding]:
    print(f"\n{'=' * 72}")
    print(f"  MODEL: {model}")
    print(f"{'=' * 72}")
    probe_deployment(client, api_key, model)
    findings = [
        run_thinking_nonstream(client, api_key, model),
        run_thinking_stream(client, api_key, model),
        run_tool_call_nonstream(client, api_key, model),
        run_thinking_force_nonstream(client, api_key, model),
        run_thinking_force_stream(client, api_key, model),
        run_tool_in_prompt_nonstream(client, api_key, model),
        run_prod_shape_tool_nonstream(client, api_key, model),
        run_thinking_token_prefix(client, api_key, model),
        run_thinking_developer_role(client, api_key, model),
        run_thinking_with_tools(client, api_key, model),
        run_thinking_workaround_nonstream(client, api_key, model),
        run_thinking_workaround_stream(client, api_key, model),
    ]
    for f in findings:
        report_finding(f)
    return findings


def summary_table(all_findings: list[Finding]) -> None:
    print(f"\n{'=' * 72}")
    print("  SUMMARY")
    print(f"{'=' * 72}\n")
    by_model: dict[str, dict[str, Finding]] = {}
    for f in all_findings:
        by_model.setdefault(f.model, {})[f.scenario] = f

    def _rc_line(label: str, f: Finding) -> str:
        rc_status = "populated" if f.reasoning_content else "empty/absent"
        leak = "LEAKED" if f.leaked_reasoning_delims else "clean"
        return f"    {label} : reasoning_content={rc_status}, content={leak}"

    def _tool_line(label: str, f: Finding) -> str:
        tc_status = (
            f"{len(f.tool_calls)} structured" if f.tool_calls else "0 structured"
        )
        leak_marker = "LEAKED" if f.leaked_tool_delims else "clean"
        wire = list(f.wire_format_hits.keys()) or ["(none detected)"]
        return (
            f"    {label} : tool_calls={tc_status}, content={leak_marker}, wire={wire}"
        )

    for model, scenarios in by_model.items():
        print(f"  {model}")
        a = scenarios.get("A_think_nonstream")
        b = scenarios.get("B_think_stream")
        c = scenarios.get("C_tool_nonstream")
        d = scenarios.get("D_think_force_nonstream")
        ds = scenarios.get("D_think_force_stream")
        e = scenarios.get("E_tool_in_prompt")
        fp = scenarios.get("F_prod_shape_tool")
        g = scenarios.get("G_think_token_prefix")
        h = scenarios.get("H_dev_role_instruction")
        i = scenarios.get("I_think_with_tools")
        j = scenarios.get("J_think_workaround_nonstream")
        k = scenarios.get("K_think_workaround_stream")

        if a and a.http_ok:
            print(_rc_line("A non-stream thinking (default)              ", a))
        if b and b.http_ok:
            print(_rc_line("B streaming  thinking (default)              ", b))
        if c and c.http_ok:
            print(_tool_line("C non-stream tool   (tools=[])               ", c))
        if d and d.http_ok:
            print(_rc_line("D non-stream thinking (enable_thinking)       ", d))
        if ds and ds.http_ok:
            print(_rc_line("D streaming  thinking (enable_thinking)       ", ds))
        if e and e.http_ok:
            print(_tool_line("E non-stream tool   (in-prompt, no tools=[]) ", e))
        if fp and fp.http_ok:
            print(
                _tool_line(
                    "F non-stream tool   (prod-shape, tools=[], large ctx)",
                    fp,
                )
            )
        if g and g.http_ok:
            print(_rc_line("G non-stream thinking (<|think|> prefix)      ", g))
        elif g:
            print(
                f"    G non-stream thinking (<|think|> prefix)      : ERROR {g.error}"
            )
        if h and h.http_ok:
            print(_rc_line("H non-stream thinking (developer role)        ", h))
        elif h:
            print(
                f"    H non-stream thinking (developer role)        : ERROR {h.error}"
            )
        if i and i.http_ok:
            print(_rc_line("I non-stream thinking (+ tools=[], none)      ", i))
        if j and j.http_ok:
            print(_rc_line("J non-stream thinking (#38855 workaround)     ", j))
        if k and k.http_ok:
            print(_rc_line("K streaming  thinking (#38855 workaround)     ", k))

    # Verdicts
    print("\n  VERDICT (per check)")
    print(
        "    Reasoning parser: 'populated + clean' → working. "
        "'empty + LEAKED' → vLLM bug (#38855 candidate). "
        "'populated + LEAKED' → partial. "
        "'empty + clean' → model not emitting thoughts (try a stronger prompt "
        "or D — explicit enable_thinking)."
    )
    print(
        "    Tool parser:      'structured + clean' → working. "
        "'0 structured + LEAKED canonical_braces' → unexpected (regex matches "
        "but parser refuses). '0 structured + LEAKED python_parens' → confirms "
        "job 3c30d72e pattern (model emits wrong format)."
    )
    print(
        "    Cross-check:      A clean + D LEAKED → bug only when thinking "
        "explicitly enabled (matches cockpit screenshot). "
        "C structured + E parens/JSON → drift only without `tools=[]` envelope; "
        "implicates how LangChain or our agent presents tools. "
        "C structured + E structured → model survives even without the "
        "envelope; the bug is something else (multi-turn, large context, etc). "
        "C structured + F structured → patched prompts + `tools=[]` hold up "
        "at production scale; shipped fixes are sufficient. "
        "C structured + F parens → drift even with parser active and patched "
        "prompts; trigger is prompt scale or shape, not the prompts that were "
        "patched in the 2026-05-05 session."
    )
    print(
        "    Thinking activation hunt: D empty + (G or H) populated → activation "
        "is a literal token / role in the prompt, not the kwarg; copy that "
        "into a system-prompt prefix on the agent side. "
        "D empty + I empty + (G or H) populated → thinking works but only "
        "without `tools=[]`; production worker can't use it without architectural "
        "change. "
        "All of D/G/H/I empty → none of the obvious activation knobs work on this "
        "deployment; read the chat template (probe output above) to find what "
        "the template actually gates on, or accept that thinking is off."
    )
    print(
        "    #38855 workaround crosscheck: D empty + J populated → confirms the "
        "delimiters were being stripped by `skip_special_tokens=True`; production "
        "wiring should pass `skip_special_tokens=False` + `enable_thinking=True` "
        "for the gemma family. "
        "J populated + K empty/LEAKED → non-stream parser fixed by the flag, "
        "streaming parser still bugged; persistent path needs a different "
        "mitigation. "
        "J populated + K populated → both paths fixed by the flag; cockpit "
        "screenshot leak is downstream (LangChain wrapper or cockpit UI), not "
        "vLLM. "
        "J empty → workaround insufficient on this deployment; bug deeper than "
        "tokenizer detokenization, escalate to vLLM upstream."
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--model",
        action="append",
        help="Model id (repeatable). Default: gemma-4-moe and gemma-4-31b.",
    )
    ap.add_argument(
        "--list-only",
        action="store_true",
        help="Only list available models from /v1/models and exit.",
    )
    args = ap.parse_args()

    api_key = os.environ.get("GEMMA_API_KEY") or DEFAULT_API_KEY
    if not api_key or api_key.startswith("sk-PASTE-PLACEHOLDER"):
        print("ERROR: set the GEMMA_API_KEY env var")
        return 2

    with httpx.Client() as client:
        try:
            available = list_models(client, api_key)
        except httpx.HTTPStatusError as e:
            print(
                f"ERROR: failed to list models — HTTP "
                f"{e.response.status_code}: {e.response.text[:300]}"
            )
            return 1
        except Exception as e:
            print(f"ERROR: failed to list models — {type(e).__name__}: {e}")
            return 1

        print(f"Endpoint: {BASE_URL}")
        print(f"Available models ({len(available)}):")
        for m in sorted(available):
            print(f"  - {m}")

        if args.list_only:
            return 0

        targets = args.model or DEFAULT_MODELS
        missing = [m for m in targets if m not in available]
        if missing:
            print(
                f"\nWARN: requested models not in /v1/models: {missing}. "
                f"Will still try them — the router may resolve aliases."
            )

        all_findings: list[Finding] = []
        for model in targets:
            all_findings.extend(diagnose_model(client, api_key, model))

        summary_table(all_findings)
    return 0


if __name__ == "__main__":
    sys.exit(main())
