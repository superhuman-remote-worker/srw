---
title: Project loops
description: Launch a bounded, repeated sequence of autonomous jobs against one project goal, and what "self-improving" means in SRW.
---

# Project loops

Project loops let SRW launch a repeated sequence of autonomous jobs against one
project goal. They are an experimental, unattended power-user feature: start
with a small budget and inspect the resulting jobs, knowledge, and repository
changes.

## What “self-improving” means here

SRW does not retrain or change the weights of the underlying model. Improvement
happens at the workflow and artifact level:

```text
explore → review → execute → verify → preserve knowledge → repeat
```

Agents can discover options, criticize proposals and diffs, implement selected
work, run checks, and preserve useful context for the next iteration. Whether
the project actually improves depends on the goal, evidence, expert
configuration, models, tools, and review quality. A completed iteration is not
proof of a better result.

## Roles and cycles

A loop runs worker experts in an ordered cycle. Built-in starting points
include:

- **Build:** Scholar → Critic → Developer
- **Write:** Scholar → Critic → General Worker
- **Research:** Scholar → Critic
- **Custom:** an operator-selected sequence of eligible worker experts

Analysis roles such as Scholar, Critic, and Product QA can fan out in parallel.
Execution roles run serially so concurrent writers do not race the same project
artifact.

Projects supply the shared goal, knowledge base, connectors, and repositories.
Loop jobs inherit only the connectors explicitly attached to that project.

## Scheduling modes

### Standard

Standard scheduling runs the configured stages in order and repeats. It
requires a maximum iteration count, a deadline, or both. A parallel analysis
stage can create several jobs while consuming one iteration.

### Campaign

Campaign scheduling lets a checkpoint Critic replace the next ordinary
execution turn with a short sequence focused on one initiative. Campaigns have
their own stage and extension limits, reserve space for follow-up analysis, and
end with a Critic disposition such as ship, extend, or kill. The disposition
closes that campaign; the outer project loop continues until its own budget,
deadline, stop request, or failure condition is reached.

### Officer

Officer scheduling delegates the next-work decision to the project's enabled
Centurion. It does not use the normal iteration/deadline requirement; the
officer's judgment and configured daily limits become the brake. This is the
most autonomous mode and should be enabled only after the ordinary bounded
workflow is understood.

## Operations and safety

- **Pause** lets in-flight jobs finish but prevents the next stage from being
  launched.
- **Resume** continues the sequence from its durable state.
- **Stop** permanently ends that run after current work settles.
- Standard and campaign runs complete when their iteration budget or deadline
  is exhausted.
- Repeated failures can fail a run; individual job failures and questions are
  visible through the project and Inbox surfaces.

Use a concrete project goal and definition of done. Attach only required
connectors, prefer review-gated jobs while tuning a cycle, inspect the git diff
and evidence rather than trusting summaries, and treat any move to Officer
scheduling as a separate autonomy decision.
