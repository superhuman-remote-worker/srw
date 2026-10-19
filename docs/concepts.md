---
title: Concepts
description: The ideas behind SRW — experts, workspaces, connectors and the runtime; sessions and jobs; phases, statuses and autonomy; projects and delegation.
---

# Concepts

SRW runs AI work on infrastructure you control. Every piece of work combines
three bricks — an **expert**, a **workspace** and **connectors** — and the
**runtime** puts them together, runs them and keeps the record. This page
explains those ideas; [Architecture](architecture.md) shows how the components
implement them.

## The three bricks

### Expert

An expert is *who* does the work: a system prompt (persona), a model and a set
of tools, plus limits and review behaviour. SRW ships experts such as
`general-worker` (the default for jobs), `assistant` (the default for
sessions), `scholar` for research, `developer` and `engineer` for software,
`writer`, `critic`, `curator`, `designer` and `product-qa`. Experts are
configuration, not separate programs: every one is the same agent runtime with
a different YAML configuration, and administrators can copy, change and add
experts. [Expert configuration](../config/README.md) describes the format.

### Workspace

A workspace is *where* the work happens. Choose one of four tiers:

| Tier | What the agent gets |
|---|---|
| **None** | No files and no shell; conversation and remote tools only. |
| **Virtual** | Files stored in object storage and edited through file tools. No shell, Git or browser. |
| **Container** | A workspace pod of its own with a shell, Git, a browser and an IDE, reached over SSH. Its image and size come from a workspace template. |
| **VM** | A KubeVirt virtual machine with its own kernel, where `sudo` is gated by your approval. |

Out of the box, jobs get a container and sessions start virtual; a project can
change these defaults. A live session can move up to a higher tier (its files
come along) but not back down. Workspaces always run apart from the agent
process: pods and VMs are separate from the control plane, and the agent
reaches them through a narrow interface. [Build your own workspace
image](workspace-images.md) shows how to add your tools.

### Connector

A connector is *what the agent may reach*: a Git repository, a PostgreSQL,
Neo4j or MongoDB database, a WebDAV share, an e-mail account, an MCP server, a
kubeconfig, an SSH key, a file or a generic credential, or a knowledge base.
SRW also creates connectors for the folders of its main cloud (see [Cloud
folders](cloud-folders.md)). Attaching a connector to a session or job binds
its credential there and adds the matching tools; an agent reaches a
connector's system only while that connector is attached. When SRW has no
connector type for a credential, you can [build a connector
driver](connector-drivers.md).

### The runtime

The runtime is everything that turns those bricks into running work: the
**orchestrator** (the API and the authority over every job and session), the
**agents** (LangGraph processes that call the model and the tools) and
**Cockpit** (the web app). They install together as one Helm chart, with
PostgreSQL, a vector database and an optional audit database for state. The
orchestrator decides what an agent may do, provisions its workspace, and alone
sets a job's final status.

## Sessions and jobs

SRW runs work in two shapes.

- A **session** is a live conversation. You type, the agent answers and acts,
  and you can steer it while it works or come back to it later. Its
  **Permission Mode** controls approvals: **Read-only** runs reads and
  research freely and asks before changes, outbound actions and shell
  commands; **Auto-accept** also runs workspace edits and asks only before
  irreversible or outbound actions and shell commands; **Autonomous** never
  asks.
- A **job** is a goal the agent works on without you: you describe it, SRW
  queues it, an agent picks it up and works through it in phases, and you
  review the result. Jobs are the shape for long, unattended work.

Both use the same experts, workspaces and connectors.

## Job statuses

| Status | Meaning |
|---|---|
| `created` | Accepted and waiting for a workspace and an agent. |
| `processing` | An agent is working on it. |
| `pending_review` | The agent stopped for a person: the work is done under an autonomy level that requires review, or it reached a phase boundary where its autonomy level pauses. Approving continues or completes it. |
| `paused` | Stopped, to be resumed — for example after its agent went offline, during a model or memory outage, or when its workspace must be upgraded. The orchestrator dispatches such jobs again from their last checkpoint. |
| `completed` | Finished, either on its own under **Full** autonomy or after approval. |
| `failed` | Ended with an error, or a retry limit was exhausted. |

You may also see `cancelled` (stopped by a person), `reviewing` (a critic is
verifying the result), `waiting` (waiting for delegated work) and
`waiting_for_reply` (the agent asked a person a question). The agent never
writes its own final status: it reports what happened, and the orchestrator
decides.

## Strategic and tactical phases

A job runs as one loop that alternates between two kinds of phase:

- **Strategic phases** plan. The agent reviews where it stands, updates its
  notes and memory, and stages the todo list for the next phase. Only here can
  it declare the whole job complete.
- **Tactical phases** execute. The agent works through the staged todos one by
  one, marking each complete.

At each transition SRW archives the finished todos, clears the conversation
and carries knowledge forward through workspace files, project knowledge and
memory. This keeps long jobs inside the model's context window and gives
review points between planning and execution. The phase number counts up from
0, the first strategic phase. Sessions do not use phases; they run a
continuous conversation loop.

## Autonomy levels

A job's autonomy level decides when it stops for review.

| Level | Stops after the first strategic phase | Stops after later strategic phases | Stops after tactical phases | When the work is done |
|---|:---:|:---:|:---:|---|
| `full` | – | – | – | completes on its own |
| `review` (default for jobs) | – | – | – | stops for review |
| `partial` | yes | – | – | stops for review |
| `guided` | yes | yes | – | stops for review |
| `dependent` | yes | yes | yes | stops for review |

Approving a job stopped at a phase boundary lets it continue; approving a
finished job completes it. Administrators can limit the highest autonomy level
each user may choose.

## Projects

A project is the durable scope that several sessions and jobs share. It holds
the goal, the members (owners, editors and viewers), connectors and
repositories, the experts and workspace defaults its work uses, and a
knowledge base that later work can search, so a new job does not start from an
empty history. With a main cloud installed, a project also gets a cloud folder.
A project can have a standing officer — a session that looks after it — and
can run [project loops](project-loops.md): bounded sequences of autonomous
jobs against one goal.

## Subagents and delegation

An expert can delegate part of its work to a **subagent**: a short-lived child
agent from the expert's roster (for example an explorer, an implementer or a
reviewer) that runs on the parent's workspace and reports back. Delegation is
off unless the expert enables it and names the `delegate_agent` tool, and it
is one level deep: subagents never delegate further.

## Where it runs

Everything runs on Kubernetes. The orchestrator, Cockpit and the databases are
ordinary deployments; agents run in pods that SRW provisions on demand; and
each container workspace is a pod of its own. VM
workspaces run as KubeVirt virtual machines, on the same cluster or on a
separate VM cluster. See [Get started](getting-started.md) to install it and
the [Security model](security-model.md) for the isolation each tier gives.
