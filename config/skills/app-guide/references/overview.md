---
guide_id: product.overview
content_type: explanation
capability_ids:
  - jobs.create
  - sessions.permission-mode
  - projects.manage
journey_ids:
  - product.first-run
---

# Overview — what this app is and how the pieces fit

**Superhuman Remote Worker (SRW)** runs AI agents that do real work for you:
research, writing, analysis, design, and software development. Agents work in
isolated environments ranging from instant cloud-backed files to full
git-versioned containers and VMs. They can be given scoped access to your data,
check in with you as much or as little as you choose, and get smarter over time
through explicit project knowledge and, when enabled and healthy, shared
project memory.

## The mental model

Three things, one triangle:

- **Sessions** — interactive. A conversation with an agent that works while
  you watch and steer. Best for exploration, quick tasks, and anything you
  want to shape as it happens.
- **Jobs** — autonomous. You describe a goal, an agent plans and executes it
  on its own, and the result comes back for your review. Best for
  well-defined work you don't want to babysit.
- **Projects** — the container. A project ties sessions and jobs together
  with a shared goal, team members, knowledge base, optional shared memory,
  connectors, and git repos — so work has durable context instead of relying
  only on one conversation.

Two more concepts complete the picture: **experts** (preconfigured agent
roles — who does the work) and **connectors** (stored access to your external
databases, files, and repos — what the agent may touch).

## The map

The sidebar:

- **Sessions** — your conversations; create and resume them here.
- **Jobs** — all autonomous work: create, watch, pause, review.
- **Projects** — project homes: goal, jobs, knowledge, members, and bounded
  continuous loops/campaigns.
- **Connectors** — access to databases, cloud folders, repos, MCP servers, and
  credentials.
- **Experts** — the agent roster; deployments can allow custom ones.
- **Skills** — reusable how-to procedures agents can load; same story.
- **Automations** — jobs on a schedule (cron).
- **Settings** — your keys, models, voice, notifications, and integrations.

The **Inbox** is where everything that needs *you* collects: job reviews,
questions from agents, and permission requests — with approve/deny/reply
right there. Admins additionally get an Administration group inside Settings (LLM
providers and models, users, configuration, capability grants, usage and cost).

## Your first fifteen minutes

1. **Check Settings first** — agents need language-model access. If your
   deployment doesn't provide it centrally, add a provider key under
   **Settings → Provider keys**, and pick default models under
   **Settings → Defaults**.
2. **Start a session** (Sessions → New Session). The initial application
   default is Assistant, but project, personal, or operator defaults may
   replace it. Ask for something real—research a question or draft a document.
   Select **Read-only** first if you want every change and command to wait
   for approval; do not assume it is the deployment's current default.
3. **Create a job** (Jobs → New Job): a concrete description, an expert,
   autonomy **Review**. Watch it move Created → Processing, then handle the
   review from the Inbox — approve it or send it back with feedback.
4. **Make a project** for anything ongoing, and create the next jobs inside
   it. Record durable facts in its knowledge base; project memory can also
   supply automatic recall when it is enabled and available.

A useful shortcut: when the session's **Fleet Management** tools are enabled,
the agent can create and steer worker jobs for you. Ask it to “run this as a
background job,” then follow the durable progress and review from **Jobs** and
the **Inbox**. Without those tools, use **Jobs → New Job** yourself.

## How much autonomy to give

Start with **read-only** sessions and **review**-autonomy jobs: you approve
every change and review completion yourself. As trust builds, loosen —
auto-accept sessions, full-autonomy jobs, then scheduled automations and
project loops (continuous agent cycles working toward a project goal). Critic
verification and Scholar research are optional job settings; check the
effective expert/job configuration rather than assuming either runs.

## Where results live

Depending on its workspace and tools, agent work can land as files in
`output/`, git commits you can browse, citations for researched material, and
project knowledge notes. The exact surfaces depend on the selected workspace:
Virtual has durable files but no git, Container and VM add git and IDE-style
access, and None has no workspace files. Deliverables can also be exported to
shared cloud storage.
