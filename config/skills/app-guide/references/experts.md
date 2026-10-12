---
guide_id: experts.choose-and-customize
content_type: reference
capability_ids:
  - experts.select
  - experts.manage
journey_ids:
  - experts.choose
  - experts.customize
---

# Experts — which agent for which task

An **expert** is a reusable agent profile: it fixes whether the profile is for
an autonomous **worker job** or an interactive **session**, then supplies a
persona, instructions, tools, and configuration. The type matters: a worker
expert cannot start a session, and a session expert cannot run a job.

## Bundled roster

| Expert | Type | Use it for |
|---|---|---|
| **General Worker** | Worker | Safe general-purpose research, writing, analysis, planning, and file deliverables. This is the initial application worker default. |
| **Assistant** | Session | General turn-by-turn research, writing, analysis, planning, and light coding. This is the initial application session default. |
| **Centurion** | Session | The standing officer of a project: supervises its worker jobs, owns the backlog, and briefs the user. Runs as an always-on wake/sleep session when a project enables its officer; the same profile dresses interactive conferences with him. |
| **Scholar** | Worker | Broad exploration: web/codebase research, experiments, and high-volume findings for later evaluation. |
| **Critic** | Worker | Evidence-based review of diffs, proposals, tests, and quality. It is also used by the optional job verification workflow. |
| **Developer** | Worker | Strict test-first implementation (specification, red, green, refactor phases) for briefs that explicitly ask for TDD. Several times slower than Engineer on ordinary work. |
| **Engineer** | Worker | The default for code and terminal work: features, fixes, refactors, scripts, frontend and backend code, installs and ops chores, verified with the project's own tests, build and lint. |
| **Curator** | Worker | Extracting structured, reusable notes from job artifacts into project knowledge. It may be launched by curation workflows when configured; it does not run beside every job. |
| **Designer** | Worker | UI/UX analysis, self-contained HTML/CSS mockups, and implementation specifications. |
| **Design Studio** | Session | Interactive iteration on mockups and design specifications. |
| **Bug Hunter** | Worker | Adversarial testing with a reproduction for each finding; it hunts rather than fixes. |
| **Product QA Tester** | Worker | Product-level usability and integration auditing with evidence-backed issue candidates; it does not fix them. |
| **Writer** | Worker | User-facing prose — reports, documentation, guides, release notes, and copy — written for a named reader from the brief, the knowledge base, and cited sources. |
| **Phone Desk** | Worker | The back office for one live phone call on a configured phone line: answers the questions the voice assistant hands over while the call lasts, then carries out the follow-ups. Started by the phone line, not chosen by hand. |

Choose **General Worker** or **Assistant** when the task does not need a
specialized role. Choose the role from the work you want performed, not from
the model name: model and tool choices can be inherited or overridden
separately in Agent Settings.

## Which default wins

Assistant and General Worker are seeded as the initial managed application
defaults, but an operator can replace either pointer. For a new root job or
session, the selection order is:

1. the expert explicitly selected for this run;
2. the selected project's default for that expert type;
3. your personal default, when personal defaults are allowed; then
4. the application default.

The create form shows the effective selection and its source. A project
override can further tune an expert used in that project. Do not promise that
every installation or user still defaults to the two seed profiles.

## Create or adapt an expert

Bundled disk experts are read-only. When DB-backed user experts are enabled,
open **Experts** and use **Duplicate** to turn a bundled profile into an owned,
editable copy; customize the copy rather than the bundled original.

- **New** creates an owned worker or session expert. Choose the type carefully;
  it is immutable after creation.
- **Duplicate** forks any visible bundled or database expert into an owned
  copy. This is the normal way to customize a bundled profile.
- **Import** creates an owned copy from an expert JSON bundle; **Export**
  downloads a portable bundle.
- Owned database experts can be edited by their owner or an administrator.
- Managed platform defaults can be edited by their administrator/owner policy
  but cannot be deleted. Other database experts cannot be deleted while jobs,
  sessions, automations, project links, or default pointers still depend on
  them; repoint or remove those blockers first.

The editor can change persona, instructions, prompts, tools, models, workspace,
and other allowed configuration. Capability grants still restrict what the
saved expert may enable, and credential-bearing configuration is rejected.

If **New**, **Import**, or editing is unavailable, the deployment may have
DB-backed experts or user expert authoring disabled. This static guide cannot
inspect that state. Bundled experts remain usable, and project repositories can
also expose project-owned expert definitions under `experts/`.
