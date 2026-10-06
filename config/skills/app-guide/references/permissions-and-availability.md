---
guide_id: sessions.permissions-and-workspaces
content_type: how_to
capability_ids:
  - sessions.permission-mode
  - workspaces.select
journey_ids:
  - sessions.configure-permissions
  - sessions.choose-workspace
---

# Permissions, grants, workspaces, and unavailable features

“SRW supports this” and “this session can do it now” are different claims. A
feature can be:

1. implemented in the running product;
2. enabled and configured on this deployment;
3. allowed by the user's effective capability grants;
4. selected by the expert/session and compatible with its workspace;
5. connected to the required project or connector; and
6. ready and present in the agent's current tool list.

The app guide explains those layers but does not itself inspect current flags,
grants, attachments, services, or loaded tools. For a current-state question,
the agent first reads this reference and then uses
`get_product_capabilities` with the exact relevant `capability_ids` from the
reference when that tool is available. The result is an advisory, time-stamped
observation, not authorization. If the tool is unavailable, partial, truncated,
or does not cover the requested fact, the affected state is unknown. An exact
disabled-control reason, current Settings value, or resource readiness shown
by Cockpit can still provide user-visible evidence; do not turn a missing tool
alone into a diagnosis.

## Permission mode is the approval policy

Persistent sessions currently have three modes:

- **Supervised** asks for approval before every tool call.
- **Auto-accept** runs non-shell tools without asking, but still asks for
  `run_command`, `shell_execute`, and `shell_read`.
- **Autonomous** runs tool calls without per-call approval.

New session experts default to Supervised unless an account, expert, or
per-session setting overrides it. Change the current session from its header
**Settings** panel; change the account fallback under
**Settings → Persistent Agent → Permission Mode**.

Permission mode does not add tools, attach connectors, make a service healthy,
or bypass authorization. Autonomous means “do not ask for each tool,” not
“ignore access controls.” Every real operation still enforces its own user,
project, connector, deployment, and safety checks. A user's
`permission_mode` grant is a ceiling; modes above it are unavailable even if
an expert requests them. The built-in non-admin ceiling allows Auto-accept;
Autonomous needs an explicit grant unless an administrator narrows or widens
the applicable policy.

## Capability grants are restrict-only ceilings

For non-admin users, effective grants combine global, project, and user scope.
More specific grants cannot widen a restriction imposed above them, and when
several values apply the most restrictive result wins. A session belongs to one
project or none.
Admins bypass this grant policy, but not deployment kill switches, missing
services, ownership checks, or other operation-specific authorization.

The current grant catalog covers:

- `personal_default_experts` — set or fork personal default experts;
- `vm_workspace` — select or upgrade to a VM workspace (VM templates need it);
- `shell_tools` — load shell tools;
- `delegation` — enable subagent delegation;
- `public_datasources` — publish a connector to all users;
- `email_autonomous_send` — allow fail-closed unattended email sending;
- `datasource_tools` — load connector-backed tools;
- `browser` — load the agent's direct browser tools;
- `catalog_authoring` — let the agent create and update the user's own experts,
  skills, and automations. The writes are owner-scoped by the same endpoints the
  UI uses, and a new automation is created switched off; the grant exists because
  an enabled automation goes on to spawn jobs;
- `unattended_operations` — start a project self-improvement loop and commission
  a project officer (centurion). These are the two surfaces that spawn jobs with
  no human clicking anything, so the grant bounds unattended token spend as much
  as permission. Without it the project's **Loop** and **Centurion** tabs do not
  appear, starting/resuming a loop or converting it to officer scheduling is
  refused, and a session configuration that switches `officer.enabled` on is
  rejected. Reading a loop's state, pausing it, and stopping it stay available —
  nobody is locked out of halting work that is already running;
- `complete_unmerged_pr` — approve or auto-complete a job whose delivered pull
  request has not been merged. Without it, a job that opened a pull request stays
  in review until that pull request lands: approval is refused, and a job that
  would complete itself is routed to review instead. Merging the pull request is
  the normal way past this, not the grant;
- `model_selection` — restrict the selectable model set;
- `autonomy_ceiling` — cap worker-job autonomy; and
- `permission_mode` — cap persistent-session permission mode.

An unavailable choice may be greyed out with **Requires a capability grant**.
Administrators manage scoped values under **Admin → Grants**. Changing the UI
does not replace server enforcement: session creation, attachment,
provisioning, dispatch, and individual actions recheck the applicable policy.

## Choose the workspace for the work

Workspace templates are edited under **Customize → Workspaces**: Shared templates
by admins only. A project's templates can be changed by admins and by project
members with the editor role, and not in an archived project. VM templates need
VM permission.

A new session or job picks its workspace in the **Workspace** field of
**Sessions → New Session** or **Jobs → New Job**. Its **Default** entry follows
the project's default, then the installation default. The shipped installation
default is Container for jobs and Virtual for sessions; an operator can change it.

| Workspace | What it provides | Important limits |
|---|---|---|
| **Virtual** | Fast, durable cloud-backed file tools without starting a workspace container; file Canvas can work when the backing store is materializable by the orchestrator | No shell, direct browser, git, repository checkout, live application, or shared browser. A process-local development store cannot be presented on Canvas. |
| **Container** (`sandbox` internally) | Isolated files plus shell, git, repository checkout, and direct browser support; the current Protected Cloud session path also requires this tier | Slower cold start. Individual tools still depend on the expert, tool selection, grants, and service configuration. This is the proven live-app/shared-browser path. |
| **None** | Chat with no workspace | No workspace file, shell, direct browser, git, or repository tools. Web research, databases, and knowledge may still work when independently configured and attached. |
| **VM** | A full per-session virtual machine for work that needs VM isolation or sudo/root access | Per-session opt-in only; never a saved default. Requires the `vm_workspace` grant, operator enablement, and an available VM provisioner. Current file Canvas and shared-browser VM support are not promised. |

A running session's header **Settings** panel shows **Workspace** in its main
**Settings** tab. On narrow screens, use the header's three-dot menu →
**Settings**. Choose **Container** to upgrade a Virtual session, then confirm
**Upgrade**. The session keeps its conversation and carries existing files
over. Wait for provisioning to complete before checking the newly available
tools. Virtual or Container can offer VM when allowed. None currently shows
a fixed workspace value in this selector; start a new Container session when
no upgrade action is offered. Some sessions cannot change workspace in place
at all: their Workspace choices say **not available in this session** and
`/upgrade-workspace` is refused. Start a new session with the needed workspace
then.

Workspace changes are upgrade-only: to move down to Virtual or None, start a
new session. A workspace upgrade does not automatically enable a missing tool
category. If `request_workspace_upgrade` is currently visible, the agent can
use it to present an upgrade request for the user's decision. It does not
provision the workspace or automatically resume work; the user can send a
follow-up message after the upgrade completes.

## Tool selection and live changes

At session creation, **Agent Settings** shows the tool categories allowed by
the selected expert and workspace. Direct Browser, Shell, Git, connectors, and
other categories can still be removed by backend or grant checks.

For an already-running session, open header **Settings → Tools**. The list
reflects the session's resolved tool categories, including **Browser**,
**Shell**, **Research**, **Git**, **Canvas**, **Job Control**, **Job Inspection**,
**Experts & Skills**, and **Automations & Loops** where applicable. Live editing
is not limited to the older four application groups. **Author Experts &
Automations** is a separate category with its own grant.

Rows can be on, off, or unavailable with a reason. Some tools are supplied by
the runtime or a connector and cannot be switched off through the category
checkbox. Use the row's explanation; a locked checked row is not a disabled
capability. If the panel cannot obtain a current resolved inventory, its
fallback list does not prove the tools are loaded.

Model, temperature, supported reasoning level, permission mode, narration,
tool selection, delegation settings, and eligible connector changes apply
automatically starting with the **next response**. There is no Save button.
Read any error instead of assuming the change succeeded. A current response
keeps its original tool list; send a follow-up after the change to continue
with the updated tools. Changing the model, tools, or connectors can reset the
conversation's prompt cache, making the next response slower. A checked group
is a request to load its tools, not proof that every operation is ready.

Expert, project, and Protected Cloud mode remain creation-time
choices. Connector-specific live limits are in `datasources`.

## Diagnose a missing capability

Use the narrowest observable check:

1. **Check the live capability observation.** For the capability IDs listed in
   this reference, distinguish build, deployment, user, and session results;
   preserve partial or unknown layers.
2. **Look for the actual operation or Cockpit action.** A capability snapshot
   does not authorize an operation. If the exact operation tool is not
   currently visible, explain how to enable it or give the user the reviewed
   UI steps; do not claim to execute it now.
3. **Read the control's reason.** A grant tooltip, workspace-required message,
   connector readiness state, or deployment-disabled message identifies a
   specific layer.
4. **Check the current session Settings.** Confirm the tool group and workspace;
   apply a supported upgrade if the work needs shell, git, repository checkout,
   direct browser, or shared browser.
5. **Check scope and attachment.** A connector existing in **Connectors** does
   not mean it is attached to this session or project, and “Testing” is not
   “Ready.”
6. **Escalate deployment state.** Hidden feature-off controls, missing
   transports, unavailable provisioners, and service failures need an
   administrator.

Do not recommend switching to Autonomous as a cure for a missing tool or grant.
It changes approval prompts only.

Finish the diagnosis with the smallest actionable next step: a workspace
upgrade, a named tool toggle, a connector attachment, or an administrator
checking a specific grant or service. Keep useful preparation moving while
that step is pending. A permission restriction is not a reason to route
around the same restriction through another tool.
