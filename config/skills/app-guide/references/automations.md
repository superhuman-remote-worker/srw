---
guide_id: automations.schedule
content_type: how_to
capability_ids:
  - automations.manage
journey_ids:
  - automations.create
---

# Schedule recurring jobs with Automations

An **automation** is a saved schedule and job template. At each scheduled
time, SRW creates an ordinary worker job from the selected expert and prompt.
The current built-in automation surface is schedule-only: it does not provide
event triggers, inbound-email triggers, webhooks, branching, or a workflow
graph.

## Coverage boundary: a schedule does not attach other features

An automation can schedule a worker job with a prompt, expert, autonomy, job
mode, priority, and optional project. Mentioning a connector or product
feature in the prompt does not attach it to the resulting job. Related
features are not proof of a supported combined workflow.

In particular, an exact recurring flow that needs any connector—for example,
scheduled work that reads a mailbox or database, or sends through
email—is not a supported built-in Automation setup today because
automation-fired jobs attach no connectors. Say that the guide does not
document or support that exact built-in flow. Do not invent a connector
attachment through project linkage, **Review** autonomy, the Inbox, or prompt
wording. Those controls do not make the connector available to the fired job.

## Create and test one

1. Open **Automations** and choose **New automation**. To keep it inside a
   project, use **Manage automations** on that project's page; the resulting
   Automations view is scoped to the project.
2. Enter a name and optional description.
3. Pick a schedule preset—daily, weekdays, Mondays, monthly, or hourly—or
   enter a custom five-field cron expression.
4. Choose the timezone and check the schedule preview. Timezones use IANA
   names and follow daylight-saving changes.
5. Select the worker expert and write the prompt that should run every time.
   Make the prompt self-contained and state what a successful run should
   produce.
6. Start with **Review** autonomy unless unattended completion is genuinely
   safe. Under **Advanced options**, you can also set priority, the maximum
   scheduled fires per day, and the catchup window.
7. Choose **Create automation**, then use **Run now** once and inspect the
   spawned job under **Jobs** before trusting the schedule.

**Run now** fires immediately even when the automation is paused. It does not
move the next scheduled run.

## What the schedule creates

Each fire creates a normal job with the automation's current prompt, expert,
autonomy, priority, and optional project. Whether the job plans in phases
follows its expert: most experts do the task directly, without a plan or
phases, which suits one step with an obvious end. An expert meant for long,
open-ended work has **Plans in phases** on. Project scope
supplies the project's context and knowledge, but automation-fired jobs do
not attach connectors: there is no connector selector on an automation today.
If recurring work needs a mailbox, database, MCP server, cloud folder, or
other connector, use another workflow until per-automation connector
selection is implemented.

An expert selected from the database-backed worker roster is pinned by its
stable ID, so renaming it does not silently repoint the schedule; later edits
to that same expert do affect future fires, and SRW blocks deleting it while
the automation still references it. Bundled experts remain name-based. An API
automation stored as unpinned `worker_base` resolves the owner's effective
worker default at each fire.

Use **Run now** after changing the prompt, expert configuration, project
override, or autonomy and inspect the resulting job before relying on the next
scheduled fire.

## Pause, resume, edit, and delete

- The row switch pauses or resumes future scheduled fires. Resuming calculates
  the next run from the current time; it does not replay everything missed
  while paused.
- **Edit** changes the template used by later fires.
- **Run now** is a manual test or one-off fire; the normal schedule remains
  unchanged.
- **Delete** removes the schedule, not jobs it already created. Past jobs and
  their outputs remain available.
- The Automations list shows the next run, last fire, and last status. Use
  **Jobs** and the **Inbox** to inspect and review the actual runs.

For shared project automations, viewer membership grants visibility and editor
membership grants management; the owner and administrators retain their normal
access.

## Safety and missed runs

- **Max fires per day** is the scheduled-run circuit breaker. Reaching it
  automatically pauses the automation so a bad schedule cannot continue
  launching jobs.
- The **catchup window** controls an overdue scheduled fire after an
  orchestrator outage. A fire still within the window may run; an older one is
  skipped and the schedule advances. It does not backfill every missed
  occurrence.
- Scheduled delivery is at-least-once. A rare infrastructure failure can cause
  a fire to be retried, so prompts and external side effects should be
  idempotent—for example, check whether today's report already exists before
  publishing another one.
- A high automation count produces a warning at 20. It is a recommendation,
  not a hard creation limit.

## What a session agent can do

If this session actually has the **Automations & Loops** tool group, it can
list and inspect automations, list their spawned jobs, and prepare an
automation proposal. The default session group cannot save, enable, run,
pause, edit, or delete an automation. Use Cockpit for those actions.

The app guide itself does not grant workflow tools. Check the current tool
list before offering to act.

For an event-driven flow, use an external scheduler or workflow system such as
n8n or Zapier to call the orchestrator API. Do not describe that as a built-in
SRW event trigger.
