---
guide_id: jobs.run
content_type: how_to
capability_ids:
  - jobs.create
  - jobs.review
journey_ids:
  - jobs.create
  - jobs.monitor
  - jobs.review
---

# Jobs — autonomous work

A **job** is a durable task an agent works through away from the chat: you
describe the outcome, choose a worker expert and settings, and review the
result when the job checks in or finishes. A job can use a full isolated
workspace, lightweight cloud files, or no filesystem at all. Do not assume
that every job has git, shell access, an IDE, or files.

## Create a job

Open **Jobs → New Job**. A session can create one for you only when its actual
tool list includes Fleet Management; loading this App Guide does not grant
those tools. The form includes:

- **The task box** — one box at the top for what the agent should do. State
  the desired outcome, constraints, and what evidence would count as done.
  **Attach** (or dropping files on the box) adds input files. **Create Job**
  is in the box; Ctrl+Enter creates the job too. After creating, the app
  returns to the job list.
- **Project** — use a project when the work should share its goal, eligible
  connectors, repositories, knowledge, and (when enabled) project memory.
- **Autonomy** — when the job stops for a person's review.
- **Plans in phases** — a setting of the expert, not of the form. Most experts
  have it off: the job does its task directly and ends when it is done. An
  expert meant for long work has it on: the job plans, works through todos in
  phases and reviews its result. A job without phases stops once, at its end,
  so Partial, Guided and Dependent act like Review there.
- **Priority** — **Low (backfill)**, **Normal (default)**, or **High (preempts
  lower)**.
- **Expert** — a worker expert; session experts are not eligible for jobs.
- Project cloud access, when the selected project has cloud storage.
- **Agent Settings** — autonomy, models, tool categories, selected connectors,
  custom instructions, memory, quality helpers, and limits.
- **Workspace** — see below.

A new job is listed by its task text. When that text is long, the user's
auxiliary model gives the job a short name a few seconds after it is created;
a short one-line task stays its own name, and jobs that loops, officers and
automations start keep the description they were given. To set a name
yourself, hover the job's row in **Jobs** and use the pencil; a name you set
is never replaced. The task text does not change, and search finds the job by
either.

### Choosing a workspace

New Job and New Session have a **Workspace** field. Its first entry, **Default**, says what the
project (or, without one, the installation) will use, for example "Default: Container ·
container-full (installation default)". Keep it unless the work needs something else.

The other entries are workspace templates:
- **Shared** templates come with SRW: `container-minimal`, `container-full`, `virtual`, and
  `vm-full` when VMs are on.
- The project's own templates.
- **Mine**: your own templates.

Two entries are special:
- **No workspace** runs without files or a shell.
- **Customize…** changes CPU, memory, disk or image for this one run, or saves the result under
  Mine.

Templates live under **Customize → Workspaces**. Built-in templates are read-only; duplicate
one to change it. A template can name any image. Images that aren't SRW's run without elevated
privileges, so they get no cloud-folder mount unless your operator sets the Helm value
`workspace.customImages.privileged`. Try a new image with a Job before using it in a
Session. To run work on your own tools, follow
[Build your own workspace image](https://github.com/superhuman-remote-worker/srw/blob/main/docs/workspace-images.md).

A template's tier is **Container**, eligible **VM (QEMU)**, or **Virtual (cloud files)**; the
**None (no workspace)** entry has no template. Virtual keeps file tools but disables shell,
browser, and git tools. None also disables file tools. Container and VM are the full workspace
tiers; their exact tools still depend on the expert and grants.

To run commands in a workspace, for example to try an image or to produce a PDF, pick an Expert
with the shell tools: the default General Worker and Engineer both have them. The `shell_tools`
grant is on by default; an administrator can restrict it.

## Choose the review cadence

Autonomy controls normal phase-boundary check-ins. It does not bypass tool
authorization, privileged-command approval, service failures, or other safety
pauses.

- **Full** — no planned freeze; runs toward completion autonomously.
- **Review** — freezes at job completion for human review.
- **Partial** — freezes at phase boundaries and at completion.
- **Guided** — freezes after every tactical phase.
- **Dependent** — freezes after every strategic and tactical phase.

## Read the status

- **Created** — ready or queued for dispatch.
- **Processing** — an agent is currently assigned and working.
- **Reviewing** — an enabled Critic is evaluating the result.
- **Pending review** — waiting for your review or a configured check-in.
- **Waiting** — a parent job is waiting for a Scholar/delegated child or
  another orchestration dependency.
- **Paused** — parked. Some pause reasons are retried or redispatched
  automatically; others need a reply, approval, configuration repair, or
  manual resume. Read the reason or Inbox item instead of assuming capacity is
  the cause.
- **Completed**, **Failed**, or **Cancelled** — terminal outcomes.

Progress is reported as **liveness**, not a percentage: a server-computed
state (`active`, `waiting`, `paused`, `suspected_stuck`, `unavailable`,
`terminal`) with reasons and a last-activity time. Any `progress_percent`
field is honestly `null` — SRW does not fabricate a percent or an ETA.
`suspected_stuck` means "investigate" (open the job, its log, or its audit
trail), not "failed"; and `unavailable` means a telemetry source could not
be reached — never present it as "no activity".

## Watch the work

A job on an expert that plans in phases alternates strategic planning and
tactical execution; with file tools its workspace can contain `plan.md`,
todos, archives, inputs, notes, and `output/` deliverables. A job without
phases does its task directly, without `plan.md` or todos, and leaves its
deliverables in `output/`. The exact files depend on the expert and task.

Use **View** for the durable job record. **Workspace** appears when a repository
URL is available, and **IDE** appears only for eligible live, snapshotted, or
repository-backed root jobs. Git commits and phase tags exist only when the
chosen workspace/config enables git versioning; Virtual and None jobs do not
gain git merely because they are jobs.

## Respond when the job needs you

Reviews, agent messages, and permission requests appear in the **Inbox** and
the relevant job view. Depending on the reason, you can:

- **Approve** a finished result or **Continue** a phase-boundary check-in.
- **Continue with feedback** to resume with corrections; failed and paused
  jobs can also be resumed when their underlying blocker is resolved.
- Reply to blocking agent messages.
- Approve or deny the exact privileged command requested.
- Approve a requested workspace/VM upgrade, or continue without it when that
  option is offered.
- **Pause**, **Resume**, **Cancel**, or, when the job is inactive, **Delete**
  from the Jobs surface.

Some project jobs use a file-by-file **diff view** at review time. **Accept**
lands the proposed change set in the project; **Reject** declines that landing.
Do not promise this review mode for every project, cloud provider, or
workspace.

## Find and reuse the result

- The review records the agent summary, confidence, and declared
  deliverables. File deliverables live under `output/` only when the job had a
  file-capable workspace.
- Citations are available when the agent used the citation system.
- Project knowledge notes may be written when knowledge curation and its
  backing services are enabled; this is not guaranteed for every job.
- **Promote to project** is for an eligible completed one-off/default-project
  job and creates a dedicated project from it.
- **Export to Cloud** appears only for an eligible completed job in the
  open-folder cloud workflow. Use the button's presence as the current UI
  signal rather than assuming every deliverable can be exported.

## Optional quality helpers

Critic review and the Scholar research pre-pass are configurable; neither
should be described as running for every job. When Critic is enabled, the job
can enter **Reviewing** and cycle through feedback rounds before reaching you.
When Scholar is enabled, the parent can wait while the research pre-pass runs.

Scheduled jobs have additional trigger, catch-up, retry, connector, and safety
rules. Route those questions to the focused **Automations** guide.
