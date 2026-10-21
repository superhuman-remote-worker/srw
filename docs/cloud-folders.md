---
title: Cloud folders
description: The main cloud's project folders, My cloud, outboxes and job copies, with their access levels, provider support and the reasons a folder is left out.
---

# Cloud folders

SRW can give agents folders of your installation's *main cloud*: the Nextcloud
or OpenCloud the chart deploys or connects (`nextcloud.enabled`,
`opencloud.enabled`, `cloud.externalBackend`). Every folder an execution gets
is a connector SRW creates and keeps for a project or a user. You select it
like any connector, it binds at an access level, and it either arrives or is
shown as left out with the reason. **Nothing takes a missing folder's place.**

`cloud.externalBackend: webdav` is not a main cloud: it seeds an ordinary
WebDAV connector, and nothing on this page applies to it.

## The connectors

| Connector | Type | What the execution gets | Attaches to |
| --- | --- | --- | --- |
| **Project folder** | `cloud_folder`, folder kind `project` | The project's folder, mounted at `cloud/<project slug>` in the workspace and read on demand. Every project with a folder in the main cloud has one; a personal project has My cloud instead. | Sessions |
| **My cloud** | `cloud_folder`, folder kind `user_root` | Your own files, everything you see in Files (the project folders you belong to included), at `cloud/my_cloud`, once you [connect it](#connect-your-cloud). | Sessions of your personal project |
| **Session outbox** | `cloud_outbox` | A folder SRW creates for the execution and shares with you. A Session mounts it at `cloud/outbox`; a Job's deliverables are copied into it when the Job finishes. Every approved user has one. | Sessions and Jobs |
| **Project folder copy (jobs)** | `cloud_folder_checkout` | The project's folder copied into the Job's repository under `projects/<slug>/` before it starts (see [Jobs](#jobs)). | Jobs |

These are platform-owned. **Settings → Connectors** shows them as managed
("Managed by its project", "Managed by SRW for you"), and nobody can create,
edit, re-point, link, unlink or delete one; a project owner only sets its
default level. A project's go with the project, a user's with their
account. None reaches the agent as a connector or a
credential: the mount, or the copy, is the delivery. The agent is told every
folder it has, whose it is, its level and what is missing, and
`srw_cloud_status` reports the same.

A cloud folder's selection and level are fixed when the Session's workspace
starts. Attaching, detaching or a new level on a live Session take effect at
its next start; switching a live Session to or from protected is refused
(409 `protected_cloud_fixed`).

## Access levels

| Level | Of | What it lets through | What enforces it |
| --- | --- | --- | --- |
| Read-only (`read_only`) | Project folder | Nothing | An account of the cloud's own, created for this folder and this member, that may only read it: the cloud refuses every write. |
| Protected (`protected`) | Project folder | Writes, after review | That reader below an overlay: the agent's writes stay in the workspace until an owner or editor of the folder's project approves them in review. |
| Read-write (`read_write`) | Project folder, My cloud, Session outbox | Writes, directly | A credential that can write the folder; see the [support matrix](#provider-support-matrix) for which. The cockpit calls My cloud's level "Full access". |
| Read-only (`read_only`) | Project folder copy | Nothing | SRW never applies the copy's changes. |
| Reviewed write-back (`reviewed_write_back`) | Project folder copy | Writes, after review | A diff the Job's owner reviews; SRW writes back only an accepted diff, and refuses one whose files changed in the cloud since the copy was made. |

The levels are strict. A level a folder can't have where it is used is
refused, never swapped for another, and a binding that may only read takes
the lowest level that writes nothing or gets no folder. Read-only is the
credential's doing, never an rclone `--read-only` flag on a credential that
can write.

### Which level a folder gets

1. **The level the Session asks for.** New Session shows a level select on
   each selected cloud folder, with the levels not offered there disabled and
   why. Over the API, `datasource_access` on
   `POST /api/persistent/threads` maps connector ids to levels
   (`{"<connector id>": "protected"}`), as does
   `execution.connectors.<alias>.access`. A level asked for binds as asked or
   the Session is not created: 422 `connector_access_unavailable` with the
   reason (`level_not_offered`, `above_ceiling`…), or
   `connector_access_not_selected` for a connector the Session doesn't
   attach.
2. **Otherwise the project's default.** A project owner sets each cloud
   folder's default access in the project's **Connectors → Cloud folders**
   list (`PATCH /api/projects/<project>/datasources/<connector>` with
   `{"access": "protected"}`; `null` restores the driver's default, the
   highest level offered). It is a default, not a ceiling. A default the
   Session can't have is lowered to the highest level it can, never raised,
   and the folder is left out when there is none. A default is resolved
   again each time the workspace mounts it: after an upgrade to a VM, or
   once its user is a viewer, it mounts read-only instead of going missing.
   A level the Session chose is bound as chosen or not at all.
3. **Within the member's ceiling.** A project's folder may also be attached
   to a Session of another project by a member of the folder's project,
   never as a default there. A viewer of the folder's project gets at most
   `read_only`, in that project's own Sessions too, and a viewer's Job gets a
   `read_only` copy. Only members reach a project's folder, administrators
   included, in its own project's Sessions too.

A Job binds at its project link's level: a Job (or a manifest execution)
that names one is refused (422 `connector_access_unsupported`,
`UnsupportedField`).

### Protected Sessions

Protected is a level of one folder. A Session with a folder at `protected`
mounts only that folder, read-only with a capture overlay; its other cloud
folders, its outbox included, are left out as `beside_protected`, never
mounted beside it. At most one folder per Session can be protected (422
`protected_cloud_single_folder`). It needs Nextcloud, the Container tier and
`agent.protectedCloudModeEnabled`, and is not offered to an officer
Session. A project whose folder's default is `protected` makes its Sessions
protected wherever that is allowed; elsewhere the default is lowered to
`read_only`. The older `protected_cloud: true` request field is an alias,
for one release, of `protected` on the Session project's own folder, and is
refused when it conflicts.

## What a Session or Job gets by default

- The create forms preselect the work's own connectors, each removable: the
  project's knowledge base, its folder (Sessions) or folder copy (Jobs), your
  outbox, and for a Session of your personal project My cloud.
- Over the REST API, omitting `datasource_ids` attaches the same; with
  `agent.datasourceDefaultsOnOmission` on, every default. The MCP tools
  that omit the list ask for every default.
- **An explicit empty list attaches nothing**, the project's own connectors
  included: no cloud folder mounts. The agent still reaches the project's
  knowledge base as project memory, which follows project membership, not
  the connector list.
- Work nobody starts by hand (project loops, automations, an officer's
  tickets) does not default to your outbox.
- A Job never gets a cloud folder: a Job request that names one is refused,
  and a Job that inherits a Session's selection drops it.

## Provider support matrix

What each provider delivers, as its adapter declares it. Administrators see
the same table, live, under **Settings → Main cloud**. A level is offered only
where it is enforced; anything else is unsupported, with the reason.
Container is the `sandbox` tier. The Virtual and None tiers get no cloud
folder. A new Session whose own folders (its outbox, its project's folder, My
cloud) would attach gets a Container instead of Virtual, when its tier comes
from the defaults and the Container tier is available and allowed for you. A
Session on Virtual by choice, or because no Container is allowed, names the
folders it left out.

| Connector · level | Nextcloud | OpenCloud |
| --- | --- | --- |
| Project folder · `read_only` | Container, VM | Not supported: the read-only reader exists in code, but nothing calls it yet |
| Project folder · `read_write` | Container | Container |
| Project folder · `protected` | Container | Not supported: the protected review lane is built for Nextcloud only |
| My cloud · `read_write` (full access) | Container | Not supported: it works only through deprecated Keycloak impersonation, so it is not offered |
| My cloud · `read_only`, `protected` | Not supported: an app password can't be limited to read-only or to one folder | Not supported: no per-user read-only credential exists |
| Project folder copy · `read_only` | Container, VM | Container, VM |
| Project folder copy · `reviewed_write_back` | Container, VM | Container, VM |
| Session outbox · `read_write` | Container; a Job's copy: Container, VM | Container; a Job's copy: Container, VM |

How each is enforced, plainly:

- **Nextcloud project folders** mount as an account of their own per folder,
  member and kind: a reader that may only read the folder, or a writer that
  may read and write it and nothing else. On a VM the reader's password sits
  in the workspace's rclone config, so only the reader is offered there.
- **OpenCloud project folders** mount with the orchestrator's Keycloak
  service account, which is a member of every project Space. The workspace
  holds only its short-lived tokens, but they reach every project Space
  while they last. OpenCloud therefore offers no read-only or protected
  level, and a project viewer, capped at `read_only`, gets no folder there.
- **My cloud** is an app password of your own account, which you grant (see
  below). It reaches everything your account can, with your permissions.
- **The session outbox** lives in the service account's storage (Nextcloud's
  `agent-service` home, OpenCloud's agent-home Space), and a Session mounts
  it with the service account's credential. On Nextcloud that account can
  still write every project folder, so the outbox is not offered on a VM.

See the [security model](security-model.md#cloud-folders-and-their-credentials)
for where each credential sits.

## Connect your cloud

My cloud needs your consent once (Nextcloud only). **Settings → Connectors**
shows **My cloud** above the connector list:

1. Click **Connect**. A tab opens on your cloud's sign-in page (Nextcloud's
   Login Flow v2). Sign in as yourself, single sign-on included, and grant
   access. The page updates on its own; the flow lasts up to 20 minutes and
   survives an orchestrator restart.
2. SRW checks the account before it keeps anything: its e-mail must be your
   SRW account's, it must not administer the cloud or any of its groups, it
   must not be one of SRW's own service accounts (`agent-service`, the
   per-folder accounts, anything in an `srw-` group), it must be the
   account SRW already knows as yours when it knows one (from an earlier
   connection or your project memberships), it must be enabled, and the new
   app password must read its files. Otherwise SRW revokes the app password
   at once and says why (another person's account, a cloud or group
   administrator, a service account, not your known account, a disabled
   account, an account with no files yet: open Files in the cloud once,
   then connect again).
3. Connected: Sessions of your personal project preselect My cloud and mount
   it at their next start.

Your cloud lists SRW's access as "Superhuman Remote Worker" under its
devices and sessions. Remove it there and SRW notices at your next Session's
start (**Reconnect**). **Disconnect** in SRW asks the cloud to revoke it; if
the cloud can't be asked, SRW forgets it and tells you to remove it there.
Deleting your SRW account revokes it too.

My cloud has full access only: the cloud can limit an app password neither
to read-only nor to one folder. Attach a project folder when you want less.

## Jobs

- **Project folder copy.** Before the Job starts, SRW copies the project's
  folder into its repository under `projects/<slug>/`, and the Job's brief
  names that path. At `reviewed_write_back` (the default where the project
  sets none), the Job's changes there become a diff when it completes; the
  Job waits in review, and its owner's accept writes the diff back. At
  `read_only` nothing is written back. Remove the chip for no copy.
- **Outbox.** A top-level Job a user starts, with their outbox attached, has
  its declared deliverables (or `output/`) copied into a cloud folder shared
  with them once it completes or reaches review. The review page says what
  landed or why not, and whether it is shared yet: a fresh cloud account
  must sign in to the cloud once, and the cloud limits how many shares SRW
  may create at a time. **Re-sync**
  (`POST /api/jobs/<id>/export-to-shared-folder`) copies again from any
  finished Job.

## When a folder is left out

Every attached folder is named, mounted or with its reason: the Session
header lists each with its level and state, and the agent is told the
same.

| The Session says | Code | What to do |
| --- | --- | --- |
| no access level is offered for it in this kind of workspace | `access_unavailable` | The folder can't have any allowed level on this tier (My cloud or the outbox on a VM; a read-only binding on OpenCloud). Use a Container workspace, or ask the project owner for another default. |
| this session may no longer use it | `not_eligible` | You left the folder's project, or it is someone else's. |
| it is not a cloud folder SRW set up | `config_invalid` | Tell your operator. |
| SRW has not created this session's outbox folder | `no_session_folder` | SRW could not create the folder when the Session started; start a new Session. |
| the main cloud was not reachable when the session started | `backend_unavailable` | Start a new Session once the cloud is back. |
| not mounted beside a protected folder | `beside_protected` | A protected Session mounts only its protected folder; use an unprotected Session for the others. |
| its account for this folder is not ready yet; its read-only account for this folder is not ready yet | `grant_unavailable`, `reader_unavailable` | SRW could not create or verify the folder's own account in the cloud. Start a new Session; if it persists, your operator checks the cloud's admin API. |
| no cloud account yet: connect your cloud | `cloud_not_connected` | [Connect your cloud](#connect-your-cloud); it mounts at the next start. |
| the cloud no longer accepts your connection | `cloud_grant_revoked` | Reconnect it under **Settings → Connectors**. |
| the session has more cloud folders than a workspace mounts | `too_many_mounts` | Detach some; the limit is `connectors.inPodPlane.maxMounts`. |
| this deployment syncs files instead of mounting them; cloud folders aren't delivered here | `delivery_unsupported` | The deployment syncs instead of mounting; see [below](#known-limitation-sync-instead-of-mount). |
| this workspace can't mount cloud folders; they aren't delivered here | `mount_unsupported` | The workspace refuses mounts (`cloud.rcloneMount.allowContainer: false`, or FUSE denied); see [below](#known-limitation-sync-instead-of-mount). On a Virtual Session (`virtual_workspace`): start a new Session with a Container workspace. |
| it is mounted only when the session's workspace starts again | `mount_unsupported` (`needs_new_pod`, `plan_failed`, `next_start`) | The folder was attached while the workspace ran, or the workspace started without the mount service planning it. Its credential never goes into a running workspace: it mounts in the next one (start a new Session, or resume one whose workspace was released). |
| it could not be prepared for this session | `unbuildable` | The cloud could not build this mount; start a new Session, and tell your operator if it persists. |

A folder that was attached but did not come up (the cloud refused its
credential, the folder is gone…) is shown as unavailable instead; see
[Build your own workspace image](workspace-images.md#when-it-doesnt-start).

## Known limitation: sync instead of mount

Project folders and My cloud are delivered as mounts only. A deployment
that sets `cloud.workspaceDriver: sync` delivers none of them: each attached
one is left out as `delivery_unsupported`. Its Sessions keep only their
outbox, which the sync mirrors the workspace into. With
`cloud.rcloneMount.allowContainer: false`, or a workspace that denies FUSE,
a Container Session gets the same outbox-only delivery, and each missing
folder is left out as `mount_unsupported`. Keep the defaults
(`rclone_mount`, `true`).

## For operators

- The provider is chosen by the chart only; **Settings → Main cloud** is
  read-only.
- At startup each replica creates any missing platform connector (project
  folders and copies, outboxes, user roots) and logs a count line. It also
  deletes the two connectors the older wiring made with a service account's
  credential to a whole cloud home, "Cloud Storage (Personal)" and the
  bundled Nextcloud row at `admin`'s home, in the exact shape SRW made them
  (`Legacy cloud WebDAV connectors retired: …`).
- Protected Sessions need `agent.protectedCloudModeEnabled` and, on bundled
  Nextcloud, `nextcloud.protectedEffect.enabled`; see the
  [Helm guide](../helm/README.md#protected-nextcloud-effect-lane).
