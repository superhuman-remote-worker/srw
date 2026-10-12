---
guide_id: memory-and-knowledge.understand
content_type: explanation
capability_ids:
  - memory.recall
  - projects.knowledge
journey_ids:
  - memory.configure-scope
  - projects.knowledge.browse
  - projects.knowledge.record
---

# Memory and knowledge — what persists

SRW has three related but different mechanisms:

1. **Conversation context** is the messages currently available to one running
   job or session.
2. **Memory** is automatic, fact-sized extraction and retrieval when the
   memory pipeline is enabled and healthy.
3. A project's **native knowledge base** is deliberate, typed, browsable
   project documentation.

Do not treat any one of these as a guaranteed substitute for the others.

## Context compaction is not long-term memory

As a conversation grows, SRW can compact older messages into a summary so the
agent stays within its model context window. In a session, `/compact` requests
that condensation. The configured memory pipeline may extract facts before
compaction and may store a compaction summary, but those writes depend on the
memory and embedding services succeeding.

Compaction therefore preserves a working summary for this conversation; it is
not by itself a promise that a fact will be recalled by a future job or
session.

## Memory — automatic, scoped recall

When memory is enabled and its database, embedding, reranking, and extraction
dependencies are available, SRW can extract reusable facts from work and
retrieve relevant records into later model context. Extraction is selective,
retrieval is relevance-based, and failures can degrade the feature. A run
configured to require memory may pause or fail loudly instead of continuing
without it. This static guide cannot tell which state the current deployment
is in.

Memory scope follows the effective configuration:

- With **Share memories across jobs** / `project_scoped` enabled and a project
  attached, retrieval uses that project. A session has one project or none.
- With project sharing disabled, or without a project scope, memory stays with
  the current job or session thread.

The project setting is under **Projects → choose a project → Settings**. A
project job can also expose a project-memory override in **Agent Settings**.
Memory has no general Cockpit browsing/editor surface, and no guide should
promise that a later run will recall a particular sentence.

## Native project knowledge — explicit, browsable notes

The native knowledge base is project-scoped. Agents whose actual tool list
includes the **Knowledge** tools can write, update, read, list, search, and
relate typed notes such as goals, plans, decisions, learnings, code insights,
sources, questions, state, and retrospectives. Project loops use these notes as
a coordination surface, but only work that actually writes notes appears
there.

Open **Projects → choose a project → Knowledge** to:

- see summary counts, search, and page through notes;
- filter the list by the displayed note types and by **Active**, **Resolved**,
  **Superseded**, or **Archived**;
- open content, tags, confidence, phase, and relationships;
- change a note's status or delete it; and
- **Export** the knowledge base.

The current Knowledge tab does not provide a free-form create/content editor.
To record a new durable note conversationally, ask a session attached to the
project to write it and verify that the session actually has Knowledge tools.
Notes can be resolved, superseded, or archived, but convergence still depends
on agents and users maintaining them.

## Native knowledge is not an external OKF connector

An **OKF Knowledge Base** connector indexes Markdown/OKF notes from an external
Git repository. It is an additional, reusable, read-only library for agents in
this release; its repository remains the source of truth. Linking or selecting
one does not merge it into the writable native project knowledge base —
though a connector can be *attached* as a project's knowledge base, which
converts it into that project's writable vault. Route connection, readiness,
attachment, and reindexing questions to the focused **external OKF Knowledge
Base** guide.

## Where to put something important

| Need | Best current surface |
|---|---|
| Keep steering this conversation | Say it in the session; compact when needed, understanding that compaction is a summary |
| Preserve a reviewed project fact or decision | Ask an attached, Knowledge-enabled session to write a native knowledge note, then verify it on the project Knowledge tab |
| Keep the project's durable purpose visible | Put the outcome in the project goal/description |
| Let automatic recall help when available | Enable project memory and state the fact clearly, but do not use memory as the only record of something critical |
| Search an existing external notes repository | Connect/select an OKF Knowledge Base and use its focused guide |
