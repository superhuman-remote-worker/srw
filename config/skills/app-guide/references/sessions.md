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
the defaults don't fit. Eligible deployments also offer a
creation-time **Protected cloud** checkbox for staging changes to a
non-default Nextcloud project; use its focused guide before relying on it.

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
  Container and VM workspaces add git; None is chat-only.
- **Run shell commands** when a shell-capable Container or VM workspace, the
  expert configuration, and the user's grants all allow it.
- **Research the web** — search, read pages, crawl sites, find and download
  academic papers.
- **Drive a browser** step by step for pages that need real interaction when
  direct browser tools are loaded. Web research can exist without them.
- **Present files on Canvas** and, on deployments that enable it, share the
  workspace browser. Those are persistent-session capabilities with their own
  workspace and deployment requirements.
- **Cite as it writes** — sources are tracked and referenced inline as `[N]`
  markers you can click.
- **Read and write project knowledge** when the project and Knowledge tools are
  attached; read git history only on a git-capable workspace.
- **Create and manage jobs for you when Job Control is enabled** — the
  agent can hand heavy work to the autonomous worker pool (create, check,
  approve, resume, pause, cancel jobs) while you keep chatting. The guide does
  not grant those tools; enable **Job Control** in Settings → Tools, and
  **Job Inspection** for the available status and supervision reads. Check
  the current session's actual tool list.
- **Use skills** — bundled how-to procedures it loads when relevant.

## Permission modes — how much it asks first

- **Supervised** — every tool call waits for your approval. An inline card
  shows what the agent wants to do, with **Approve**, **Auto-accept**
  (approve and switch modes), or **Stop**.
- **Auto-accept** — non-shell tools run without asking; shell tools still wait
  for approval.
- **Autonomous** — everything runs unattended.

Switch modes anytime in the chat's settings panel or with `/supervised`,
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
  `/auto`, `/supervised`, `/autonomous`, `/silent`, `/verbose`, and `/rewind`
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
- **Header buttons**: **Settings** (including the live workspace and tool
  controls), **Citations** (once anything is cited), **Files** (when a cloud
  folder is available), and workspace-dependent **Git**, **IDE**, **Canvas**,
  or shared-browser actions.

## Change this session while keeping the conversation

1. While connected, open **Settings** using the sliders icon in the chat
   header. On a narrow screen, open the header's three-dot menu and choose
   **Settings**. This opens **Session settings** beside the conversation.
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
VMs are allowed and confirm **Upgrade**; existing files carry over. A running
session can't upgrade to a Container, so start a new Container session when you
need one (for example, for the direct or shared browser). Then check
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
