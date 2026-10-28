---
guide_id: sessions.use
content_type: how_to
capability_ids:
  - sessions.permission-mode
  - workspaces.select
journey_ids:
  - sessions.start
  - sessions.resume
---

# Sessions — working with an agent interactively

A **session** is a live, turn-by-turn conversation with an agent. Unlike a
job (which runs off on its own), a session keeps you in the loop: you steer,
the agent works, you see everything as it happens. **Assistant** is the initial
application session default, but an explicit selection, project default,
personal default, or operator-chosen application default can replace it.

Start one from **Sessions → New Session**: pick a title, a **Project** (a
session has one project or none; choose **No project** for none), an expert,
and adjust the agent settings (model, permission mode, tools, connectors) if
the defaults don't fit. On eligible deployments a non-default Nextcloud
project's cloud folder can be attached at the **Protected** access level,
which stages the agent's changes to it for review; the level is chosen at
creation. Use the focused protected-cloud guide before relying on it.

## What the session agent can do

Treat the list below as possible session capabilities, not proof of what this
particular session can do. In a broad capability answer, explicitly say that
the current workspace, selected tools in **Agent Settings**, effective grants,
and deployment configuration decide what is actually available. After reading
this reference, use `get_product_capabilities` without filters for a broad
current-session inventory, or with the exact relevant `capability_ids` above
for a focused check. The current tool list and **Settings** panel remain useful
user-visible evidence. Before offering to act, require the exact operation tool
and report that operation's own result. Its action-time checks are
tool-specific; do not infer that it re-fetched every upstream policy merely
because an earlier capability snapshot said `can_execute`.

- **Work with files** when the session uses a Virtual or full workspace.
  Container and VM workspaces add git; None is chat-only. These files,
  `notes/` included, belong to this session: other sessions do not see them,
  and they are not in your cloud storage or the project's knowledge. Work
  that must reach later sessions or other people goes to project knowledge,
  a cloud folder, or a pushed repository.
- **Run shell commands** when a shell-capable Container or VM workspace, the
  expert configuration, and the user's grants all allow it.
- **Research the web** — search, read pages, crawl sites, find and download
  academic papers.
- **Drive a browser** step by step for pages that need real interaction when
  direct browser tools are loaded. Web research can exist without them.
- **Present files on Canvas** and, on deployments that enable it, share the
  workspace browser. Those are persistent-session capabilities with their own
  workspace and deployment requirements.
- **Cite as it writes** — sources are tracked and shown inline as numbered
  chips you can click; each says whether its source checked out.
- **Read and write project knowledge** when the project and Knowledge tools are
  attached; read git history only on a git-capable workspace.
- **Create and manage jobs for you when Job Control is enabled** — the
  agent can hand heavy work to the autonomous worker pool (create, check,
  approve, resume, pause, cancel jobs) while you keep chatting. The guide does
  not grant those tools; enable **Job Control** in Settings → Tools, and
  **Job Inspection** for the available status and supervision reads. Check
  the current session's actual tool list.
- **Use skills** — bundled how-to procedures it loads when relevant. The
  **Skills** tool category turns this off or on; with it off the agent sees
  no skill list. An expert that requires a skill before a tool keeps the
  skill loader even when the category is off.

## Permission modes — how much it asks first

- **Read-only** — reads, search, web research and browsing run without
  asking; changes, outbound actions and shell commands wait for your approval.
  An inline card shows what the agent wants to do, with **Approve**,
  **Auto-accept** (approve and switch modes), or **Stop**.
- **Auto-accept** — workspace edits run without asking too; irreversible or
  outbound actions (deletes, mail, pushes, database writes, jobs, delegation)
  and shell commands still wait for approval.
- **Autonomous** — everything runs unattended.

Switch modes anytime in the chat's settings panel or with `/readonly`,
`/auto`, `/autonomous`. Higher modes may be restricted by your account's
grants — options you can't use appear disabled. Permission mode controls
approval prompts; it does not add tools or bypass connector, project, or
deployment authorization.

## The chat view

- **Composer**: attach files (documents, images, audio, video — drag-and-drop
  and paste work too; on mobile you can take a photo), and use slash commands.
  Typing `/` opens a menu that lists only the commands this session supports:
  `/compact` (condense the conversation), `/done` (end the session), `/undo`
  (revert the last turn's file changes; needs a workspace with file history),
  `/auto`, `/readonly`, `/autonomous`, `/silent`, `/verbose`, and `/rewind`
  where the session offers it.
  `/compact` takes optional focus text (`/compact keep the pricing decisions`).
  Sent while the agent is still replying, it waits and runs after that reply.
  Compaction also happens automatically when the **CTX** bar above the
  composer reaches 100%, and the summary stays part of the conversation after
  reloads and resumes. A "Nothing to compact" notice means there was not
  enough older conversation to fold.
  Compaction is a context-window operation, not a durable-recording answer;
  questions about future recall or preserving a project decision require the
  focused **Memory and knowledge** guide.
- **Voice**: a mic button dictates your message (speech-to-text), and
  finished agent replies have a read-aloud button. Both depend on your
  deployment having voice models configured; the voice and speaking style are
  set in Settings.
- **Send / Stop**: the send button becomes a stop button while the agent is
  working — interrupt it anytime.
- **Progress**: a collapsible checklist shows the agent's current todos; a
  token-usage panel shows context fill; reasoning and tool calls can be
  expanded or collapsed.
- **Header**: the model and permission-mode badges (each opens **Session
  settings**), the **Canvas** and shared-browser buttons when the session
  offers them, **Disconnect**, and a **More actions** (⋮) menu. That menu
  always holds **Copy session ID**, **Session settings** (including the live
  workspace and tool controls), and **View options**; it adds **Citations**
  once anything is cited, **Files** only when the session has a session folder
  in the cloud, and **Git**, **IDE**, or **SSH** when the workspace provides
  them. A session without a session folder has no **Files** entry anywhere.
- **Downloads**: a workspace file link the agent writes in chat downloads that
  file when tapped; the Canvas toolbar has **Download** for the presented
  file.

## Change this session while keeping the conversation

1. While connected, open the chat header's **More actions** (⋮) menu and
   choose **Session settings**, or click the model or permission-mode badge
   in the header. **Session settings** opens beside the conversation.
2. In its **Settings** tab, change **Workspace**, permission mode, narration,
   model, temperature, or the model's supported reasoning level. The **Tools**
   and **Connectors** sections are in the same tab. The live pane has no
   Advanced tab.
3. Enable the tool category the task needs, such as **Browser**, **Shell**,
   **Canvas**, or **Job Control**. The panel shows the session's resolved
   categories and a reason for unavailable or locked choices. Browser and
   Shell are workspace- and grant-dependent; an upgrade may be needed first.
4. Changes apply automatically starting with the **next response**; there is
   no separate Save button. A change during a response does not add tools to
   that response. Check any reported error, wait for a workspace upgrade to
   complete if requested, then send the agent a follow-up to continue.

For a Virtual session that needs a shell, choose **VM** in **Workspace** when
VMs are allowed and confirm **Upgrade**; existing files carry over. A Virtual or None
session served by the shared executors can also choose **Container** and
upgrade in place, and can also choose **VM** (or send `/upgrade-workspace vm`)
to move to a VM in place when VMs are allowed; a session whose Container choice
says **not available in this session** can't, so start a new Container session
when you need one (for example, for the direct or shared browser). Then check
the needed tool category under **Tools**. If the Workspace choices say **not
available in this session**, this session cannot change workspace in place:
start a new session with the needed workspace instead. Use the focused
`permissions-and-availability` guide for grants, other tiers, and locked
controls, and `canvas-and-browser` for browser setup and shared login.

Workspace changes are upgrade-only; use a new session to move down to Virtual
or None. Expert, project, and Protected Cloud mode are set at creation.
Most eligible connectors can be added or removed live; some attachments are
fixed for the session. Read `datasources` for attachment limits. Session
Settings changes this conversation; account defaults for future sessions
live under **Settings → Persistent Agent**.

## Session lifecycle

Sessions idle out after a period of inactivity (default 30 minutes) but are
not lost: resume any session from the Sessions list. Ended sessions show a
resume card in the chat. Rename sessions inline from the list or chat header;
delete them from the list.
