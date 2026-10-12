---
guide_id: sessions.delegate
content_type: how_to
capability_ids:
  - sessions.delegate
  - jobs.create
  - jobs.review
journey_ids:
  - sessions.delegate-job
---

# Send work to the fleet and use delegation

There are two different ways SRW can split work:

- **Fleet Management** lets an interactive session create and steer independent
  worker jobs. The chat remains available while those jobs run.
- **Delegation** lets an eligible agent split its own job into focused
  subagents. It is an expert/job capability, not another independent chat.

For “run this in the background while we keep talking,” Fleet Management is
the direct path. Several independent jobs can be queued together, but they run
in parallel only when fleet capacity permits; never promise simultaneous
starts.

## Launch a worker job from a session

**Capacity:** three independent jobs may be queued together, but they run in
parallel only when fleet capacity permits. Always surface that distinction in
the answer: queueing three jobs is supported; three simultaneous starts are
not guaranteed.

1. In a new session's **Agent Settings**, or the current chat's **Settings**,
   open **Tools** and enable **Fleet Management**.
2. Tell the session the outcome, which worker expert to use, the project if
   any, what “done” means, and whether the job should have normal or high
   priority.
   You can also ask for a specific model, or for a VM-backed job when the work
   needs root — the session applies those as per-job settings, and anything
   above your permissions is refused when the job is created rather than
   failing later.
3. If connector access matters, say which of this session's selected
   connectors the worker needs.
4. Ask the session to create the job. It should return the job ID; the job
   also appears under **Jobs**.
5. Keep chatting, or follow progress from **Jobs**. Reviews and questions that
   need you appear in the **Inbox**.

A Fleet-created job normally inherits this session's selected connectors
unless the agent explicitly selects none or another allowed set. Merely
putting the job in a project does not silently attach every project connector.

If Fleet Management is unavailable, use **Jobs → New Job** and fill the same
brief, expert, project, priority, autonomy, and connector choices manually.

## What Fleet Management can do

When the corresponding tools are actually loaded, a session can:

- inspect its session and project scope;
- create a worker job and list or inspect visible jobs;
- list and read files a worker job has pushed to its workspace repo (state
  as of the worker's last checkpoint push, not live mid-phase edits);
- read a job's published completion evidence: `get_job_completion_report`
  returns the recorded completion report, `list_job_evidence` the typed
  manifest (test reports, screenshots, deliverable checks), and
  `read_job_evidence` one bounded entry by its ID. Evidence is pinned at
  completion and paginated — judgment material, not a live file browser, and
  an entry marked unavailable is a source that could not be reached, not an
  empty result;
- approve a job that is pending review;
- resume a paused or frozen job with feedback;
- request a safe-point pause or cancel a job; and
- inspect the current project's jobs and repository metadata.

`create_job` also takes `workspace`, which can be:
- a template name, such as `"container-minimal"`;
- `"none"`;
- omitted, for the project default.

A child job inherits its parent's workspace.

Some workspace tiers also expose a project-repository checkout or a workspace
upgrade request. Treat those as current-session capabilities, not universal
Fleet features.

Every operation is authorized when it runs and still depends on job state,
project membership, deployment health, and worker capacity. The app guide does
not grant Fleet Management, and seeing this guide is not evidence that the
tools are loaded. A session without the relevant visible tool can explain the
Cockpit path but must not claim it created, approved, or paused anything.

Fleet Management does not delete jobs, guarantee that several jobs start at
the same instant, or continuously monitor a job in the background on the
session's behalf. The Jobs page and Inbox are the durable monitoring surfaces;
the session can check again when you ask.

## Run several independent jobs

Create one job per independent outcome. They can be queued together and run in
parallel when fleet capacity permits. Give each a complete brief and avoid
having two jobs write the same project files unless you have planned how their
changes will be reconciled.

Use this pattern when the work products are independently useful—for example,
one research job, one implementation job, and one review job.

## Delegation inside one job

**Delegation** is useful when one parent agent can divide a single outcome into
independent investigations or workstreams and then synthesize the results. It
must be enabled in the selected expert (`delegation.enabled` plus the
`delegate_agent` tool) or in **Agent Settings → Tools → Delegation**, and
non-admin users need the delegation grant.

A delegated subagent is a short-lived, fresh-context helper of a type the
expert's roster defines (for example an explorer that reads and reports). It
runs inside the parent's job on the same workspace and sees only the brief it
was given. Foreground delegation waits and returns the report as the tool
result. Background delegation first creates a durable child record, returns
its handle in an immediate receipt, and pushes the completion report into a
later parent turn automatically. Which subagent types exist, their tools,
write access and turn/token budgets are set by the expert's configuration,
not by the parent at call time. Subagents cannot delegate further, and the
full report of every child is kept under `.subagents/<handle>/report.md` in
the job workspace.

The bundled rosters use `implementer` for one bounded owned-path change,
`tester` for commands, `reviewer` for diff findings, `verifier` for criterion
evidence, `reader` for one research question, and `probe` for one raw-evidence
vector. Run only one shared-tree writer at a time; partition its `owned_paths`
and let the parent review and commit. Foreground delegation returns when the
child finishes. For background children, `list_agents` gives a bounded status
roster, `message_agent` steers a child, and `stop_agent` requests a bounded
partial report before stopping. Completion is pushed without any control call:
do not poll `list_agents` or `wait_agent`; wait once only when the child's next
update is immediately blocking progress. Interactive sessions still use
`create_job` when the work should run as an independently managed Fleet job.

Background delegation does not make the child an independently managed Fleet
job: the parent remains responsible for its shared outcome and cannot complete
while child work or a pushed report is outstanding. For user-visible,
independently managed tasks, create separate Fleet jobs instead.
