---
guide_id: projects.organize
content_type: how_to
capability_ids:
  - projects.manage
journey_ids:
  - projects.create
  - projects.configure
---

# Projects — organizing and compounding work

A **project** is the container that makes work compound: it bundles a team,
shared knowledge, connectors, git repositories, and jobs around one goal.
Attached sessions and jobs can read explicit project context. Automatic
cross-run memory is additional and applies only when project-scoped memory is
enabled and healthy; do not assume every observation from one run reaches the
next. Every user also has a **personal project** as their default home for
one-off work.

Create one from **Projects → New Project** (name, description, goal), or
**promote** a finished job into a project to keep building on its results.

## Inside a project

The project page has tabs:

- **Overview** — description, goal, and stats; edit them anytime. The goal
  matters: agents read it to understand what the project is for.
- **Jobs** — the project's jobs, plus a **New job in project** shortcut.
- **Knowledge** — the shared knowledge base agents read and write: searchable
  notes typed as decisions, learnings, goals, plans, questions, and more (see
  the memory-and-knowledge guide).
- **Connectors** — link shared external systems and data; per-project access can be
  overridden (read-only vs read-write).
- **Repos** — the project's git repositories: the managed jobs repo, plus any
  source or reference repos you add.
- **Experts** — experts available to this project.
- **Members** — add people as **Owner**, **Editor**, or **Viewer**; membership
  also grants matching access to the project's git repositories.
- **Loop** — configure and monitor continuous project improvement. Route setup,
  campaign, budget, and control questions to the focused project-loops guide.
- **Centurion** — the project's standing officer. Provision him here (assign a
  slot roster: named worker allocations with count, model, and workspace),
  watch his status (next wake, queued events, pages used, digest), open his
  log (his session transcript), open or rejoin the **conference** (an
  interactive session wearing his identity — while it is open his background
  loop holds), or retire him. He supervises jobs, owns the backlog, and pages
  only when something genuinely needs the user.
- **Settings** — rename, default agent config, memory sharing across jobs, a
  link to the project's cloud folder under **Cloud Storage**, archive/delete.
  Workspace network access is admin-controlled.

There is no read-only switch in Settings. What agents may do in the project's
cloud folder is the folder's **Default access** on the **Connectors** tab under
**Cloud folders**: **Read-only**, **Protected**, or **Read-write**, as far as
the cloud offers each. A session can choose another level when it is created,
within what your role allows. **Protected** stages a session's writes for the
user's review and has its own focused guide. A personal project has no folder
of its own: its cloud is the owner's **My cloud**, and adding a member shares
no files.

## Practical guidance

- Put ongoing work in a project even if you're solo—the knowledge base
  provides durable context, and optional shared memory can add automatic
  recall.
- Write the goal as an outcome ("users can X"), not a task list; agents plan
  the tasks themselves.
- Attach connectors at the project level when several jobs need them.
- Use the focused project-loops guide before starting unattended continuous
  work; its Definition of Done is steering, not an automatic stop condition.
