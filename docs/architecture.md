# Architecture

Superhuman Remote Worker is a Kubernetes-native control plane for interactive
and autonomous AI work. It separates the service that decides and enforces what
an agent may do from the environment where model-generated code runs.

## System map

```text
                                      model providers
                                            ▲
                                            │
 Browser / Cockpit UI                       │
    │ HTTPS / WebSocket                     │
    ▼                                       │
┌──────────────── TRUSTED CONTROL PLANE ────┴─────────────────────┐
│                                                                │
│  Cockpit BFF  ─────►  Orchestrator  ──►  Agent runtime pods      │
│                           │                    │                 │
│                           │                    ├── tools / MCP    │
│                           ▼                    │                 │
│                  persistent state             │                 │
│           PostgreSQL · pgvector · audit       │                 │
│                                                                │
└───────────────────────────────────────────────┼─────────────────┘
                                                │ job-scoped
                                                │ workspace backend
┌────────────── UNTRUSTED EXECUTION / CONTENT ──▼─────────────────┐
│                                                                │
│   Virtual files       Workspace pod        Workspace VM         │
│   (no process)        (container shell)    (guest kernel)       │
│                                                                │
└─────────────────────────────────────────────────────────────────┘
```

The boundary is explained in detail in the [security model](security-model.md).

## Core components

| Component | Responsibility |
|---|---|
| **Cockpit** | Angular web application for sessions, jobs, projects, reviews, files, configuration, and operational state |
| **Orchestrator** | FastAPI authority for authentication, APIs, persistence, dispatch, provisioning, recovery, and final job state |
| **Agent runtime** | LangGraph-based harness that resolves an expert, calls models, selects tools, checkpoints work, and reports outcomes |
| **Workspace backend** | Virtual files, a remote container, a remote VM, or no workspace; exposes only the capabilities of the selected tier |
| **MCP server** | Optional external interface for inspecting and operating SRW through scoped tools |
| **Application PostgreSQL** | Users, projects, jobs, sessions, configuration, and other authoritative application records |
| **pgvector / memory services** | Embeddings, hybrid retrieval, citations, and project-scoped recalled memory |
| **Audit PostgreSQL** | LLM, agent, chat, tool, and operational traces when the audit tier is enabled |
| **Optional services** | Git, cloud files, Neo4j, search, browser infrastructure, notifications, and deployment-specific connectors |

The Helm chart can run many dependencies inside the cluster or connect SRW to
externally managed equivalents.

## Product concepts

### Sessions

A session is an interactive conversation backed by a persistent thread. The
orchestrator provisions or claims an eligible agent runtime, resolves its
expert, model, grants, tools, and workspace, then returns a short-lived
connection credential for the browser-to-agent WebSocket. The user can steer
work while it happens and resume the thread later.

Sessions use a continuous tool-calling loop rather than the worker job's
strategic/tactical phase cycle.

### Jobs

A job is a durable background goal. Its normal lifecycle is:

```text
created → processing → completed | failed | pending_review | paused
```

At a high level:

1. The orchestrator validates and stores the requested job.
2. Dispatch resolves the expert, models, tools, connectors, limits, and
   workspace policy.
3. Kubernetes provisions or assigns the agent runtime and, when needed, a
   separate workspace.
4. The agent alternates strategic planning and tactical execution, persisting
   checkpoints and artifacts as it works.
5. The agent reports completion data, freeze/review state, or a recoverable
   failure.
6. The orchestrator remains the authority that commits the final job status.

That final point is deliberate: a model-facing runtime reports evidence and
intent, but it does not grant itself authoritative completion.

### Projects

A project ties sessions and jobs to a shared goal, membership policy, knowledge
base, optional memory, connectors, repositories, and automations. Projects are
the durable coordination scope; they prevent every new agent run from starting
with only one prompt and an empty history.

Projects can also run repeated, controlled job sequences. See
[project loops](project-loops.md).

## Workspace tiers

Tools are the intersection of expert configuration, deployment availability,
user grants, connector attachment, and workspace capability. Selecting a
workspace alone never grants a tool.

| Tier | Process and storage model | Typical capabilities | Important limits |
|---|---|---|---|
| **Virtual** | Durable object-backed files, materialized through the workspace API | File read/write and eligible Canvas flows | No shell, git, repository checkout, direct browser, or live application |
| **Container** | Per-job or per-session workspace pod, normally reached by SSH/SFTP; its image, CPU, memory and storage come from the selected WorkspaceTemplate | Files, shell, git, repository checkout, browser, and IDE flows | Shares the Kubernetes node kernel; FUSE mode may require a privileged container, which custom template images get only when the operator allows it |
| **VM** | Per-workload QEMU/KubeVirt virtual machine | Full workspace and gated privileged operations | More expensive and slower; requires an enabled provisioner and user grant |
| **None** | No workspace | Conversation and independently configured remote tools | No workspace files, shell, git, or direct browser |

The platform default is Virtual. A live session can move only along supported
upgrade paths; moving back to a lower tier requires a new session.

Container and VM workspaces are shaped by WorkspaceTemplates, and admission
freezes the selected template into the execution. For containers, the
provisioner applies it to every pod it creates for that Job or Session. See
[container workspace templates](../examples/manifests/container-workspace-templates.md)
for images, resources and the privilege rules.

## Experts, skills, and tools

An expert is a resolved configuration, not a separately implemented agent
service. Built-in experts cover general work, research, criticism, software
development, product verification, design, writing, curation, and interactive
assistance. They inherit from shared worker or session overlays and can select
different prompts, models, tool groups, limits, and review behavior.

Skills are reusable instruction packages loaded when relevant. Tools are
registered centrally and filtered by strategic/tactical phase, runtime
capability, expert configuration, user grants, and service readiness. MCP
connectors can extend the tool surface without making them universally
available.

See [expert configuration](../config/README.md) for the resolution model and
the current built-in configurations.

## State, knowledge, and recovery

SRW keeps several kinds of durable state outside the model context window:

- **Authoritative application state** records users, jobs, threads, projects,
  review state, and configuration in PostgreSQL.
- **LangGraph checkpoints** preserve executable worker state. PostgreSQL is the
  default shared checkpointer; SQLite remains a legacy/local option.
- **Workspace artifacts** hold inputs, notes, plans, output files, and git
  history when the selected tier supports them.
- **Project knowledge** stores reviewed decisions, facts, and learnings for
  retrieval by later work.
- **RecallStore memory** optionally extracts and retrieves project-scoped
  memories through hybrid dense and sparse search.
- **Audit records** make model requests, tool activity, and lifecycle events
  inspectable when the audit path is enabled.

Recovery combines these layers. A replacement runtime can reattach or recreate
the workspace, restore durable artifacts, and resume a shared checkpoint,
subject to the configured storage and retention policy.

## Deployment model

The Helm chart is the deployment artifact for local, self-hosted, and
production-shaped installations. A small evaluation cluster can bundle the
databases, identity provider, git server, and cloud service. Production
operators can replace each with managed services and enable a separate HA
database topology.

See the [Helm chart guide](../helm/README.md) for the concrete topology and
[local Kubernetes guide](local-kubernetes.md) for a single-machine evaluation.
