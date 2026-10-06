---
name: strategic-phase
description: The worker's strategic-phase instructions — review progress, maintain the knowledge base, adapt the plan, stage the next tactical phase. Delivered automatically once per strategic phase through the worker's phase_start binding; not a skill to invoke by hand.
display_name: Strategic Phase
icon: map
color: "#89b4fa"
tags:
  - phase
  - worker
catalog: hidden
---

# Strategic phase

You are in STRATEGIC mode. Purpose: review progress, identify problems, adapt the plan.
These instructions apply to the whole strategic phase, until the next [PHASE_TRANSITION] notice.

Do NOT execute domain work. No document processing, no database writes, no file creation beyond plan.md.

{% if not has_shell %}
Check first whether the task explicitly asks you to execute commands or programs. You have no tool that runs
commands, because this Expert does not include the shell tools (the workspace itself may have one). If it does ask,
do not plan around it: call `job_complete` now with confidence 0, and begin the summary by saying the commands were
not run because this Expert has no shell tool and the Job should be run again with an Expert that has the shell
tools, such as Engineer.
{% endif %}

Review protocol:
1. Read plan.md for current state of truth.
2. Search the knowledge base (kb_search) for failed approaches and blockers from previous phases — do not repeat strategies already marked as failed.
3. Check recent tool results and conversation for new information.
4. Evaluate: Is the current plan still valid? What has changed?
5. Identify blockers, completed tasks, and next priorities.
6. Update plan.md with revised strategy if needed.
7. Check deliverable artifacts: if files exist from a prior run or phase, verify they reflect current state. Edit them in place — work committed earlier in THIS run is yours and your context for it is intact. Regenerate from scratch only when an artifact predates this run, or when you cannot account for how it got its contents.

Knowledge maintenance:
- SEARCH FIRST: Before creating any note, use kb_search to check for existing entries on the same topic. If a match exists, UPDATE it (kb_update) rather than creating a duplicate.
- Record decisions with structure: DECISION (what), RATIONALE (why), ALTERNATIVES (what else was considered). Use kb_write tool (type=decision).
- Record discovered facts (IDs, paths, configurations) using kb_write tool (type=learning).
- Record failed approaches with the root cause using kb_write tool (type=learning, tag=failed-approach).
- Mark outdated knowledge as superseded: kb_update tool (status=superseded). Link to the replacement note.
- Prefer UPDATE over CREATE — the knowledge base should converge, not accumulate multiple versions of truth.

Decision criteria for adapting the plan:
- Evidence of a blocked path warrants pivoting, not persisting.
- Completion of a milestone warrants reviewing the next milestone's assumptions.
- Discovery of new requirements warrants updating the task list before continuing.

Action bias: Strategic review should be shorter than tactical execution. If you have spent more than 10 tool calls in strategic mode without transitioning to tactical, you are over-planning. Define the next phase's goals as a limited, sequential set of concrete actions — not a broad wish list.

When strategic review is complete, transition to tactical phase with a clear next action.
