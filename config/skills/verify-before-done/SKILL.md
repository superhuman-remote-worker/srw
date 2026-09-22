---
name: verify-before-done
description: Use before marking a todo complete or signaling the goal is achieved. How to prove a task is actually done by running a workspace check and reconciling its real output against an explicit definition of done — instead of claiming success from assumption or inspection.
---

# Verify Before Done

You are fluent by design, so finished *looks* finished — which is exactly why
agents declare victory on work that doesn't hold up. Before you mark a todo
complete or signal `goal_achieved`, close the gap between "looks done" and
"proven done" with external evidence. (Missing, incomplete, or incorrect
verification is ~1 in 4 of all multi-agent failures — MAST, arXiv 2503.13657.)

The rule: a completion claim rests on the **fresh output of a check you actually
ran**, never on your own reading of your own work.

## When this is the final job review

Reconstruct the definition of done from the authoritative request, not from the
agent's own plan alone:

1. Read `instructions.md` and any runtime-declared required deliverables visible
   in the job context.
2. Turn every requested output, constraint, and acceptance criterion into a
   concrete checklist.
3. Use `plan.md` only as a tracking aid. If it omitted an original requirement,
   the requirement still exists.
4. Reconcile that checklist against the **current final artifacts**, including
   their substantive content. A filename by itself proves only existence.

{% if has_tool("git_diff") %}
Git history is optional orientation, not a completion prerequisite. When it
would help recover what changed, you may use `git_diff` once to locate the
relevant artifacts. Judge completion from their current state and the original
requirements. If Git history is unavailable or errors, skip it immediately;
virtual workspaces can be valid without Git history.
{% else %}
No Git-history check is required in this workspace. Verify the current artifacts
directly; absence of Git history is not a failed deliverable.
{% endif %}

Finish the review with one concise handoff verdict:

- `PASS: <evidence and any honest verification limitations>` when no material
  requirement needs more work.
- `GAPS: <specific missing, incomplete, or incorrect requirements>` when another
  execution phase is needed.

When the current todo asks for it, save that verdict in `todo_complete`'s
`completion_note`. This makes the result survive compaction and lets the next
todo decide whether to close or continue without rerunning unchanged checks.

## The gate — four steps

**1. Define "done", pick the check.** Write the concrete, observable criteria
that prove this task is finished, then choose the smallest set of available
checks that produces that evidence:
{% if has_tool("run_command") or has_tool("shell_execute") %}
{% if has_tool("run_command") %}
- Code → `run_command` with the test/build/lint command — done = exit 0, 0 failures.
- Writing → `run_command`: `wc -w`, `grep` for required headers — structure and
  length match the spec.
- Data/analysis → `run_command` with a check script or query — numbers reconcile,
  row counts match.
{% else %}
- Code → `shell_execute` with the test/build/lint command — done = exit 0, 0 failures.
- Writing → `shell_execute`: `wc -w`, `grep` for required headers — structure and
  length match the spec.
- Data/analysis → `shell_execute` with a check script or query — numbers reconcile,
  row counts match.
{% endif %}
{% else %}
This workspace has no command runner. Use the file and domain tools that are
actually available; do not invent a shell tool or emulate one with repeated
searches.
{% if has_tool("file_exists") %}
- Required paths → call `file_exists` once for each required artifact after its
  final write.
{% endif %}
{% if has_tool("read_file") %}
- Writing/data → call `read_file` once after the final write and compare that
  result directly with the required sections, values, and other visible criteria.
{% endif %}
{% if not has_tool("file_exists") and not has_tool("read_file") %}
- Artifact inspection → no deterministic workspace checker is available. State
  that limitation honestly instead of manufacturing evidence.
{% endif %}
- Checks that require execution, such as tests or exact computed metrics, remain
  unverified unless another available domain tool establishes them. Distinguish
  "the verifier is unavailable" from "the artifact failed verification."
{% endif %}
{% if has_tool("get_citation") %}
- Research → use `get_citation` to confirm cited claims resolve to sources that
  contain the claimed facts.
{% elif has_tool("cite_web") or has_tool("cite_document") %}
- Research → use the available citation tools to connect factual claims to real
  sources that contain the claimed facts.
{% endif %}

**2. Run it fresh, once.** Execute each selected check after the last relevant
change. Independent checks may be called together in one turn. Do not rerun a
successful check unless the verified deliverable or implementation artifact
changed afterwards; updating tracking metadata such as `plan.md`, or receiving a
newly re-injected copy of this skill, is not a reason to restart verification.

**3. Reconcile immediately — read the actual output.** On the next decision,
compare the returned evidence with every criterion before calling another tool.
Did it say `0 failed`, or did the build error after the linter passed? Did the
source return the claimed text, or a 404? Is every required section present?

**4. Decide on the evidence.**
- Falls short → do not complete. State the specific gap in the output, plan the
  fix, keep going.
- Meets "done" → complete now, and include the exact verifying output in your
  message.
- A check is unavailable → report exactly what was and was not verified, use any
  bounded fallback above once, and decide. Do not retry an unavailable capability
  or repeat an unchanged evidence bundle.

## What counts as evidence

Acceptable: an unmodified quote of a tool's output run in the current state. One
tool result may establish several related criteria; evidence is not improved by
asking for the same bytes again.
Not acceptable: a success claim from your reasoning rather than a tool result —
- "I reviewed it, it handles the edge cases" → use an available executable
  check, or state explicitly that execution was unavailable.
- "It meets the 5,000-word requirement" → only if an available check measured it;
  visual inspection is not an exact count.
- "The build passes" → only if an available check ran it after the last change;
  stale output doesn't count.
- "The citation links to a real paper" → only if you resolved it this run.

## When there is no deterministic check (qualitative work)

Some criteria are genuinely subjective (tone, argument quality). Don't fake a
check — and don't skip verification either:
1. Verify every *checkable* aspect deterministically — structure, length, each
   required element present, each factual claim sourced.
2. For the subjective remainder, review it criterion-by-criterion against the
   instructions and **label it as judgement, not proof** ("structural checks
   passed via script; tone assessed by review against the 4 stated criteria").
   Honest scope beats a fabricated metric.

## Don't

- Assume tool output from how the work looks — run it. (Incorrect verification is
  the strongest single predictor of a fatal run.)
- Treat an unavailable verifier as proof that the artifact failed. Record the
  limitation and use an available bounded fallback where possible.
- Re-verify forever. Once the stated criteria pass, complete.
- Repeat an identical tool call or evidence bundle without an intervening change.
- Promise a check in words and then shortcut it. One fresh result can support
  multiple claims when it contains the relevant evidence.
