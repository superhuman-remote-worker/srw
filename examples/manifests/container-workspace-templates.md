# Container workspace templates

## From the cockpit

**Customize → Workspaces** lists the templates you can use: Shared (including the built-ins), your
own (Mine) and a chosen project's.
- **New template** opens a form for the tier, image, CPU, memory and disk. Container templates can
  also set guaranteed CPU/memory and a pull policy. VM templates can also set setup steps (one shell
  command per line).
- Saving checks the template the way admission will, then applies it as a manifest.
- Built-in templates open read-only; **Duplicate** copies one into Mine.

New Job and New Session pick a template in their **Workspace** field. **Default** follows the
project, then the installation. **Customize…** sends a one-off inline copy. MCP callers pass
`workspace` to `create_job`, `create_project_job` or `create_persistent_thread`: a template name or
`"none"`. Inline recipes go through `manifest_apply`. A child Job inherits its parent's workspace and can't set `workspace`.

A `backend: sandbox` WorkspaceTemplate chooses the image, CPU, memory and storage
of a Job's or Session's container. See [the example](srw-container-workspace.yaml).

```yaml
spec:
  backend: sandbox
  resources:
    cpu: 2            # maximum
    memory: 4Gi       # maximum
    storage: 30Gi
    requests:         # reserved, optional
      cpu: 0.5
      memory: 1Gi
  environment:
    image: registry.example/team/workspace@sha256:<digest>
    pullPolicy: IfNotPresent
```

An execution freezes the template when it is admitted. Every later pod for that
Job or Session uses the same values, including restores, wakes and End/Resume.
Editing the template affects new executions only. A Session's settings can't
change its frozen image or resources; such a change is refused. A pinned virtual
Session that switches to a container changes only its backend and gets the
installation defaults. A stateless Session can't change its tier at all.

## How the numbers map to Kubernetes

| Field | Pod |
| --- | --- |
| `memory` | limit = the value. The request is `requests.memory` if you give one, otherwise the same value. |
| `cpu` | limit = the value. The request is `requests.cpu` if you give one, otherwise a quarter of the limit (for example `cpu: 2` → `500m` request, `2000m` limit). |
| `requests` | What Kubernetes reserves on the node. Each request needs its maximum in the same template and can't exceed it. Containers only. |
| `storage` | With PVC workspaces, the claim size. Otherwise the emptyDir `sizeLimit` plus an `ephemeral-storage` request of the same size. |
| `pullPolicy` | The container's `imagePullPolicy`. Resolution fills in `IfNotPresent` when you give an image. |

A request below its maximum means the workspace is overcommitted. For memory,
Kubernetes may evict such a workspace when the node runs short. The installation
default runs that way too: it reserves 1Gi and allows 4Gi. Leave `requests.memory`
out if the workspace must never be evicted for memory.

A field you leave out keeps the installation default: 500m/1Gi requested,
2 CPU/4Gi limit, and the installation's storage size. An existing workspace
volume is never resized.

SRW sets no ceilings of its own. Limit what one workspace may request with a
namespace `LimitRange`, and the namespace total with a `ResourceQuota`. The
chart's `workspace.resourceQuota` can cap workspace volumes, but it is off by
default and applies only with `workspace.pvcEnabled`. A pod that such a limit
rejects fails its Job with the cluster's message. A rejected volume claim (a PVC
quota or `LimitRange`) fails the Job with a generic error; the reason is in the
orchestrator log.

## Built-in templates

Every installation ships these templates in the shared Catalog:

| Name | Workspace |
| --- | --- |
| `virtual` | No container and no VM. |
| `container-minimal` | The `srw-workspace-minimal` image. |
| `container-full` | The `srw-workspace` image, which is the installation image. |
| `vm-full` | The installation's VM image with 8 CPU, 16Gi memory and a 30Gi disk. Present only when VMs are enabled. |

The two container templates state 2 CPU and 4Gi as the maximum, and reserve
0.5 CPU and 1Gi. That equals a workspace created without a template.

`vm-full` has a larger disk than a VM created without a template, which gets the
controller's disk size (20Gi by default). CPU and memory are the same.

Select one by name and scope. The scope is required; SRW doesn't search other
scopes for the name.

```yaml
workspace:
  template:
    ref:
      name: container-minimal
      scope: {kind: Catalog, name: shared}
```

- **They are read-only.** An edit or a delete is refused, for administrators too.
  To change one, save a copy under your own name and edit the copy.
- **Every release updates them.** A new release points the container templates
  at its own images. Work that is already admitted keeps the image it was
  admitted with, like any template. That includes Jobs that named no workspace:
  they get `container-full` (see [Workspace defaults](#workspace-defaults)).
- **They pull like a pod without a pull policy.** Each container template gets
  the policy Kubernetes would give its image: `Always` for a `latest` tag
  without a digest (the shipped values), `IfNotPresent` for a pinned tag or a
  digest. The chart's `image.workspace.pullPolicy` doesn't change it. To pull a
  pinned tag every time, save a copy with its own `pullPolicy`.
- **A Project copies a template when the Project is applied.** A Project whose
  workspace refers to a built-in keeps that copy, image included, until the
  Project is applied again.
- **Operators** size the container templates with
  `workspace.builtinTemplates.containerResources` and `vm-full` with
  `workspace.builtinTemplates.vmResources`. They turn the set off with
  `workspace.builtinTemplates.enabled: false`. The VM controller never makes a
  disk smaller than `vmController.vmDiskSize`.
- **A name conflict skips that built-in.** If the shared Catalog already holds
  a template with a built-in's name that the installation doesn't manage — an
  administrator created it, for example — the orchestrator skips the built-in
  and logs an error at startup. Rename or delete the existing template to get
  the built-in.
- **A built-in a Project still references stays.** It stays, read-only, after
  it stops being declared or after `workspace.builtinTemplates.enabled: false`,
  and is retired at a later start once nothing references it.

## Workspace defaults

When a Job or Session doesn't name a workspace, SRW answers two questions. Each
answer comes from the Project first, then the installation.

1. **Which tier.** A mode per role: one for Jobs, one for Sessions.
2. **Which template for that tier.** One template per tier (`container`, `vm`),
   shared by both roles: the Project's, otherwise the installation's, otherwise
   the built-in (`container-full`, `vm-full`).

| Mode | Backend | Template |
| --- | --- | --- |
| `none` | `none`: no files, no shell | none |
| `virtual` | `virtual` | none |
| `container` | `sandbox` | the container template |
| `vm` | `vm` | the VM template |

Modes say `container`; a template's backend says `sandbox`. Both name the same
tier.

With nothing configured, Jobs get a container from `container-full` and Sessions
get `virtual`. `container-full` has the installation image and the installation
sizes, so this is what work got before defaults existed.

The defaults are applied once, when the work is admitted. Changing them later
doesn't move running work. The Job or Session records which layer gave each
answer, in `context.workspace_sources` (Jobs) or `metadata.workspace_sources`
(Sessions):

```json
{"tier": "project", "template": "project", "template_name": "container-minimal"}
```

The tier comes from `explicit`, `project`, `installation` or `upgrade`; the
template from `explicit`, `project`, `installation` or `builtin`. Admission
writes this record; a value sent by the caller is dropped. `GET /api/jobs/{id}`
returns it inside `context`, and `GET /api/persistent/threads/{id}` inside
`metadata`.

### A Project's defaults

The Project page's **Settings** tab has a **Workspace defaults** section: Jobs,
Sessions, Container template and VM template. The VM fields are hidden while VMs
are off. Each "Installation default (…)" entry shows what the installation
gives. Project owners and administrators can change the values; other members
see them read-only. The template pickers offer Catalog templates and the
Project's own templates, which every member can read. A personal project also
offers its owner's Account templates. A template that no longer exists is
flagged on its field.

There is no per-user workspace setting. A user's personal project stands in for
it, and the Settings page links there. A value saved under the old personal
"Workspace backend" setting was copied to the personal project's Sessions mode
at the upgrade.

The same values are available through the API:

- `GET /api/projects/{id}/workspace-defaults` returns the stored values, the
  effective ones with their layers, the installation's values and any problems.
  Any member may read it.
- `PUT /api/projects/{id}/workspace-defaults` replaces all four values. A field
  that is left out or `null` falls through to the installation.

  ```json
  {"jobs": "container", "sessions": "virtual",
   "container": {"name": "container-minimal", "scope": {"kind": "Catalog", "name": "shared"}},
   "vm": null}
  ```

  It answers 403 unless the caller is a Project owner or an administrator, and
  409 "These defaults are managed by the Project manifest." when a manifest
  owns them. It answers 422 for:
  - a template whose backend doesn't match its tier ("The container template
    must be a container workspace.");
  - a template outside the pickers' set ("Pick a Catalog template or one of this
    Project's templates.");
  - a VM value while VMs are off ("VM workspaces are not available on this
    installation.").

### In a Project manifest

`defaults.workspace` takes an alias (the shorthand), `null`, or an object:

```yaml
spec:
  resources:
    workspaces:
      development: {ref: {name: cpp-terraform-react, scope: {kind: Catalog, name: shared}}}
      devbox: {ref: {name: vm-full, scope: {kind: Catalog, name: shared}}}
  defaults:
    workspace:
      jobs: container          # none | virtual | container | vm
      sessions: virtual
      container: development   # alias from resources.workspaces
      vm: devbox
```

- **Object.** All four fields are optional. `container` and `vm` must name
  aliases in `resources.workspaces` whose backend matches (`sandbox`, `vm`), or
  the apply fails validation.
- **Shorthand `workspace: <alias>`.** Both modes become the alias's tier, and the
  alias becomes the template for that tier (none for `virtual`).
- **`workspace: null`.** Both modes become `none`.

Activating a Project revision that sets `defaults.workspace` writes these values
and makes the manifest their owner. The Settings tab then shows them read-only
("These defaults are managed by the Project manifest"), and `PUT` answers 409. A
revision without the field leaves values made in the Settings tab alone. A
revision that drops the field, or deleting the Project manifest, clears the
values and hands them back to the Settings tab. A manifest-owned template is
copied when the revision is applied; only a new revision changes it.

### Jobs and Sessions

`execution.workspace` in a Job manifest, and `workspace` in `POST /api/jobs` and
`POST /api/persistent/threads`:

| Value | Workspace |
| --- | --- |
| omitted | the defaults for the Job or Session role |
| `null` | none (backend `none`) |
| a binding | used as given |

A generic-image Job (`adapter: generic`) that omits `workspace` gets its
Project's container template when the Project's Jobs mode is `container` and a
container template is set, which is what the shorthand to a `sandbox` alias
sets. Otherwise it gets no workspace. The installation's values and the
built-ins never apply to generic Jobs.

The Job and Session create forms show where the default comes from under the
workspace picker, for example "Default from this Project · container-minimal".

### Installation defaults (Helm)

```yaml
workspace:
  defaults:
    jobs: container     # none | virtual | container | vm
    sessions: virtual
    container: ""       # empty = the built-in; or a template name in Catalog/shared
    vm: ""
```

The values above are the shipped ones. A named template must exist in the
shared Catalog with the matching backend; any template there works, not only a
built-in. With `workspace.builtinTemplates.enabled: false`, an empty name means
the plain backend: the installation image and sizes, without a template. The
orchestrator checks these values at startup; see the
[Helm chart guide](../../helm/README.md#workspace-defaults).

### When a default is broken

Admission fails closed. There is no silent fallback, because work would run
without its toolchain. The message names the layer:

- "This Project's container template 'x' no longer exists."
- "The installation's container template 'x' (Helm workspace.defaults.container)
  no longer exists."
- "The built-in container template 'container-full' is missing; see the
  orchestrator's startup log."
- "The installation's workspace defaults (Helm workspace.defaults) are invalid:
  …"
- "The container template must be a container workspace." (409): the template
  was edited to another backend after it was chosen.

Work that starts without anyone watching doesn't hold up other work:

- A scheduled automation skips that run and stores the message as its last
  status. The next successful run clears it.
- The officer's backlog report shows "Pool …: CANNOT DISPATCH until the
  workspace defaults are fixed: …" for that pool.
- A project loop stops with "spawn failed: …".

"The Project changed during workspace selection; submit again." is not a
refusal. Those paths retry it.

### Upgrades

An upgrade moves to the tier it asks for, or to the next one up (`none` and
`virtual` go to a container, a container goes to a VM). It gets that tier's
default template from the lookup above, unless it names a template.

| Trigger | Result |
| --- | --- |
| `/upgrade-workspace vm` in a Session | a VM from the default VM template |
| `/upgrade-workspace <template name>` in a Session | that template. SRW looks for the name in the Session's Project, then in your Account, then in the shared Catalog. In a running Session it must be a VM template. |
| `/upgrade-workspace` without an argument | the next tier (for a `virtual` Session, see below) |
| The agent's approval request (it needs `sudo`) | a VM from the default VM template |
| A Job frozen for a VM (`POST /api/jobs/{id}/upgrade-to-vm`) | a VM from the default VM template |
| A running `virtual` or `none` Job that needs a container | a container from the default container template |

- **Container upgrades of a running Session are unavailable.** In a `virtual`
  Session, `/upgrade-workspace container` and `/upgrade-workspace` without an
  argument are refused, and a container template name is refused with
  "Container upgrades of a running Session are unavailable; start a new Session
  with this template." Start a new Session with the container you need.
- **Refusals.**
  - A template that isn't a higher tier than the current one: 400 "An upgrade
    must move to a higher tier than the current one."
  - A template that keeps its workspace (`retention: Retain`): 409 "Upgrades
    can't use a template that keeps its workspace; start new work with this
    template."
  - An unknown name: 404 "No template named 'x' is available here."
  - As before: a VM upgrade while VMs are off, a stateless Session (409), and a
    Session admitted from a manifest are refused, and nothing moves down a tier.
- **Work without an owner** (internal Jobs, their subjobs, agent child threads)
  upgrades without a template, as before. Naming a template for it answers 409
  "The execution owner is unavailable."
- **VM sizes.** A VM upgrade takes the template's sizes. With the shipped values
  that is `vm-full`: its 30Gi disk is larger than the 20Gi a VM upgrade got
  before.
- **Recorded.** The upgrade stores the template's settings as `upgrade_config`
  and their layers as `upgrade_sources`: in `metadata.vm` for a Session, in
  `context.vm` for a Job's VM upgrade, and in `context.workspace_container` for
  a Job's container upgrade.

## Images

Any image may be used. It must implement the SRW workspace contract. The simplest
way is to build `FROM` an SRW base image, which already does.
[Build your own workspace image](../../docs/workspace-images.md) walks through it
step by step.

| The image provides | SRW needs it for |
| --- | --- |
| The `agent-host` user (uid 1000, shell bash) | every login |
| sshd on port 30022 with SRW's key, certificate and principals settings; `ssh-keygen`; the sftp server | readiness, file tools, host identity |
| An entrypoint that installs the key from `/tmp/ssh-pubkey`, generates host keys and starts sshd | readiness |
| `tmux`, `bash`, `flock`, `timeout`, `mktemp`, `grep`, `awk`, `sed` | shell tools and attach |
| `python3` | ending a workspace, installing credentials, restoring snapshots |
| `tar`, `zstd` | suspend and restore |
| `git`, `ssh-agent`, `ssh-add` | git versioning and repository datasources |
| code-server on port 38080 with `auth: password` and the `HASHED_PASSWORD` value SRW injects; without that value it must not start | the Session IDE, and recovery after an aborted attach |
| `rclone` 1.70.0 or later, `fusermount3`, `mountpoint`, `fuse-overlayfs` 1.13 or later, `/cloud` owned by `agent-host` | the cloud mount (trusted images only) |
| No `sudo` | the sudo policy |

SRW base images contain `/usr/local/bin/assert-workspace-contract`. Run it as the
last step of your own build to check your image:

```dockerfile
FROM ghcr.io/superhuman-remote-worker/srw-workspace-minimal:<tag>
RUN apt-get update && apt-get install -y --no-install-recommends golang
RUN /usr/local/bin/assert-workspace-contract
```

### SRW base images

| Image | Contains |
| --- | --- |
| `srw-workspace-minimal` | The contract above, the browser stack (Chromium, Playwright, browser-use) and a small CLI set: curl, wget, jq, less, vim-tiny, nano, ripgrep, zip. About 3.1 GB unpacked. |
| `srw-workspace` | Everything in minimal, plus Node.js 22 with TypeScript and Prettier, compilers and `-dev` libraries, `psql`, `mongosh` and `cypher-shell`, pandoc, poppler and ffmpeg. About 5.7 GB unpacked. |

An Expert whose instructions assume Node, a compiler or a database client fails
on `srw-workspace-minimal` with "command not found". Use `srw-workspace`, or add
what you need in your own image.

**Install system-wide in your own Dockerfile.** The base images set `PIP_TARGET`
and `npm_config_prefix` to directories under `/home/agent-host`, so that the
workspace user can install packages without root. The workspace volume is
mounted over that directory, so anything your build installs there is hidden.
Unset the variable for build-time installs:

```dockerfile
RUN env -u PIP_TARGET pip install --break-system-packages <package>
RUN env -u npm_config_prefix npm install -g <package>
```

- **Use images only from authors you trust.** The image runs with the workspace
  owner's secrets: the credential connectors attached to the work and its
  repository credentials. With `workspace.customImages.privileged: true` it also
  gets the owner's cloud-storage credentials. On a protected-cloud Session, an
  unprivileged custom image briefly receives the read-only cloud credential
  before the Session fails (see [Privilege](#privilege)).
- **`config_override` can't set the image or resources.** Both
  `config_override.workspace.container` (the old unvalidated side door) and
  `config_override.workspace.sandbox` (the template's own rendered form) fail
  with 422. Put the image and resources in a WorkspaceTemplate and select it
  with `workspace`. A project whose stored `default_config_override` still
  carries the old key must remove it through the API before its other default
  overrides can be updated.
- **Pin digests or immutable tags.** SRW doesn't pin tags. A restored or
  rewoken pod may pull a newer image behind the same tag, exactly like a
  Kubernetes Deployment.
- **Install software system-wide.** The workspace volume is mounted over
  `/home/agent-host`, and the home directory is seeded from
  `/etc/skel.agent-host` only once, so anything installed only under
  `/home/agent-host` in the image is hidden.
- **Container templates can't run `prepare` steps.** `prepare`,
  `cache: Rebuild` and `initialize` are rejected. Build your own image instead.
- **Private registries.** Workspace pods set no `imagePullSecrets` and no
  `serviceAccountName`, so they run as the namespace's `default` ServiceAccount;
  the chart's `global.imagePullSecrets` doesn't reach them. Add your pull secret
  to that ServiceAccount, or configure registry credentials on the nodes. An
  `unauthorized` pull is retried until the pull budget runs out.

## When a workspace can't start

**Test a new image with a Job first.** A Job reports more about why its
workspace didn't start. A Session may report nothing: a pinned container Session
(the default lane) has no workspace status, and a stateless Session without
startup-stage tracking keeps showing "Checking workspace scheduling; the result
is not yet confirmed." End still works for a Session whose image can't be
pulled when it first starts (see below). Startup-stage tracking is Helm
`orchestrator.containerStartupStageAuthority.enabled`, off by default.

- **How pull failures are classified.** These rules apply to custom images. A
  pod using the installation image keeps the plain 120-second readiness wait.
  - `InvalidImageName` and `ErrImageNeverPull` fail at once.
  - `ErrImagePull`, `ImagePullBackOff` and `CreateContainerConfigError` fail once
    `workspace.imagePullTimeoutSeconds` has passed (default 600 seconds).
  - A pod the cluster itself rejects (a `ResourceQuota` or `LimitRange` 403)
    fails at once with the cluster's own message.
- **Jobs.**
  - Without startup-stage tracking, a Job fails with "Workspace image `<ref>`
    could not be pulled: `<reason>`".
  - On a Job's first creation on a fresh volume, its pod, service and volume are
    then cleaned up within about a minute. A restored Job, or one recreated over
    a kept volume, keeps them.
  - Deleting the Job during that minute may return 503 once; retry and it
    succeeds.
  - Without startup-stage tracking, a Job's first creation whose container
    exits before it becomes ready fails with "Workspace container exited with
    code `<code>` (`<reason>`) before it became ready. A workspace image must
    keep running SRW's SSH server: build it FROM an SRW base image and don't
    override its ENTRYPOINT or USER." The advice is left out when the cluster
    stopped the container (`OOMKilled`, `Evicted`, `ContainerStatusUnknown`).
    Its pod, service and volume are then cleaned up the same way. A restored
    Job, one recreated over a kept volume, and a workspace requested while the
    Job runs (a tier upgrade, or a scholar's shared parent workspace) keep
    waiting instead (see below).
  - With startup-stage tracking, the Job doesn't fail by itself. It stays
    **Created** and the Jobs list shows "Workspace needs attention: its image is
    invalid." at once for a malformed reference, or "Workspace needs attention:
    its image didn't finish pulling in time. Check the image reference and that
    the cluster can pull from its registry." once the pull budget has passed.
    Cancel it.
- **Workspace preparation is bounded.** Each orchestrator admits two workspace
  mutations and two independent Ready checks at a time. A pulling image or busy
  workspace mutation lock keeps its own operation pending while discovered
  Ready Jobs can dispatch. Up to 100 Jobs are owned locally, including active
  work. Each discovery tick checks new priority arrivals and advances through
  eligible Jobs, reading at most 50 rows per execution lane. A full local queue
  can replace a waiting mutation hint with Ready work without cancelling active
  creators. Normal new arrivals cannot extend an existing discovery sweep
  indefinitely; continuously changing priorities have no fixed latency guarantee.
  Leadership loss and shutdown drain owned work, including started Kubernetes
  calls, before releasing its mutation guards. This behavior requires an orchestrator upgrade
  and does not repair historical pending creation attempts after restart.
- **Jobs can be cancelled while their image is pulling.** Once resource
  creation has returned and its exact identities are recorded, cancellation
  can commit during image or SSH readiness polling. The creator stops when it
  observes the cancelled claim, and normal retirement owns cleanup. A started
  resource write must finish before cancellation can commit: that busy
  interval returns HTTP 409 with `Retry-After: 1`; retry the request. This
  response does not mean cancellation was accepted. Ready publication and any
  final workspace seeding also retain their mutation guard.
- **Session End can interrupt image or SSH readiness waits.** Once the exact
  workspace resources are recorded, the creator finishes its current probe
  and yields to End. A resource write or final Ready publication must finish
  first. HTTP 503 with `session_workspace_lifecycle_busy` means End was not
  accepted and should be retried. A protected Session can return `ending`
  while its agent is still stopping. If the request disconnects before End is
  accepted, reconnecting or polling can continue the same pending workspace.
- **Background recovery continues exact initial Session creations.** After a
  caller disconnects or an orchestrator restarts, the sweeper can rediscover an
  open initial container creation whose Pod and expected resource UIDs were
  already recorded. It continues that source; it does not start unused Sessions
  or create replacement resources. Busy owners and held sources do not block
  later pages. Each process owns at most two observers and yields between
  completed effects after a two-second observation quantum. The quantum begins
  after bounded recorded-resource validation and the first exact Pod
  observation; preparation still checks stop and lifecycle authority. A
  successful SSH probe may complete exact Ready finalization after the quantum;
  lifecycle End and current-source checks still apply. Started SDK calls and
  probes stay joined during End and shutdown.

  This excludes retained successors, restore/history, VM workspaces, and pinned
  attempts whose original actor/attach tuple differs from the current one.
  Their existing explicit lifecycle paths remain available. The background
  container runner does not repair or replace VM maintenance.

  Background timeouts retain the exact Pod's creation clock. Physical readiness
  uses the larger of the existing readiness and applicable custom-image pull
  budgets. SSH uses the exact Ready transition plus its authentication budget,
  capped by the physical deadline plus that same SSH budget, so repeated Ready
  transitions cannot extend it indefinitely. Missing or malformed timestamps
  hold recovery; a new observation never starts a new budget.
- **Other start failures give no error.** When a templated Job's pod never
  becomes ready for another reason, the Job stays **Created** with no error
  until you cancel it or its execution deadline passes. Examples:
  - an image without the workspace contract whose container keeps running but
    never opens sshd;
  - a container that exits, when startup-stage tracking is on, or the Job was
    restored or recreated over a kept volume;
  - resources no node can fit, when no `LimitRange` rejects them.

  A workspace requested after the Job has started (a tier upgrade, or a scholar
  preparing its parent's workspace) whose container exits doesn't change the
  Job's status either: the Job keeps its status and the new workspace stays
  `creating`.

  Without startup-stage tracking, its workspace stays `creating` and the Jobs
  list shows "Checking an older workspace creation; completion evidence is
  unavailable." With it, the Jobs list names the stage: a scheduling wait while
  no node fits, or "Workspace needs attention: it didn't become ready in time.
  If it uses your own image, check that the image keeps running: build it FROM
  an SRW base image and don't override its ENTRYPOINT or USER." once a
  scheduled pod's readiness budget has passed.

  Check the pod's status and events with `kubectl describe pod
  workspace-<first 12 characters of the Job ID>` in the workspace namespace.
- **Stateless Sessions can continue an open creation.** If the original Pod
  later becomes ready, a stateless Session can finish that same creation while
  its reservation remains open. Retries preserve the original image-pull budget;
  they do not reopen closed historical creation attempts.
- **Stateless Ready publication settles creation atomically.** The trusted Ready
  binding, creation-marker removal, and exact reservation settlement share one
  database transaction. An interruption before commit leaves the creation open
  for the same Pod; losing the commit response leaves Ready and settlement
  committed together, so a normal retry observes that runtime. This guarantee
  requires the updated orchestrator. Upgrading does not repair historical Ready
  workspaces whose reservation was left open by an earlier version.
- **Stateless restore preserves the suspension and retained volume.** Once the
  suspended Pod is proven absent, a separate exact cleanup receipt clears its
  physical runtime projection. The original suspension remains the authority
  for the replacement. A restore with a retained PVC requires that exact PVC
  before creating resources; a missing or replaced PVC leaves restore pending
  without a fresh volume fallback. Clearing an absent Pod alone does not prove
  its PVC survived.
  The replacement endpoint and its creation settlement commit together as
  `restoring`. Only exact restore-work completion clears snapshot debt and
  publishes Ready for work and Canvas. This requires the updated orchestrator;
  it does not repair historical unproven lifecycle projections.
- **End also handles an initial stateless startup failure.** If the first fresh
  workspace has a recorded Pod identity and has never been published Ready, normal
  End can stop it without waiting for its image or SSH. End retains its exact
  PVC; Resume reuses that volume and refuses a missing or replaced PVC.
  Permanent End deletes the recorded resources, including when upgrading an
  earlier soft End. Initial emptyDir workspaces can resume with fresh storage.
  Once accepted, cleanup and End settlement retry after a client disconnect;
  physical deletion may take more than one pass. A Running Pod whose SSH never
  became ready can use this path too; cleanup still requires positive evidence
  that every container stopped.

  This applies only to an open initial creation with exact resource identity.
  Missing Pod identity, restored or previously retained stateless workspaces,
  closed historical creations, and historical Ready workspaces with an open
  reservation remain outside this recovery path.
- **Pinned Sessions can End a failed workspace creation.** Normal End retains the
  exact PVC and Service while retiring the recorded Pod, including when an
  agent attached after workspace creation began. Permanent End purges that
  storage. Accepted End requests remain durable after a disconnect and retry
  through the background reconciler.

  Resume waits until the previous creation's inert resource fences have passed
  their ten-minute request horizon and normal cleanup removes them. During this
  wait the API returns `workspace_predecessor_cleanup_pending` with a
  `retry_after` time. The Session remains ended; Resume has not been accepted
  or scheduled, so retry it after cleanup finishes. Permanent End remains
  available during the wait.

  Once Resume is accepted, it claims one successor creation with the exact retained
  PVC and Service. A lost response reuses that claim. End remains available
  before the successor starts, and another pull failure preserves the same
  storage. A failed restart follows the same flow. Ready still requires
  authenticated workspace readiness. Resume requires the original creation
  plan; changed image or template settings may hold creation until those
  original settings are restored. A missing
  or replaced resource, missing issued Pod identity, or unproven process stop
  leaves an explicit cleanup hold; `force` does not bypass that evidence.

  Creation retries read and adopt resources whose UIDs are already recorded in
  the pinned intent. They may update ownership on the exact seed ConfigMap, but
  never create a replacement Pod, PVC, ConfigMap, or Service when that recorded
  object is missing, terminating, or replaced. The original intent remains held
  for normal lifecycle recovery.

## Privilege

Workspaces from SRW's own images (`srw-workspace` and `srw-workspace-minimal`),
and from repositories listed in `workspace.images.trustedRepositories`, keep the
privileged FUSE profile used for the cloud-storage mount. An entry matches its
repository with any tag or digest; a tag or digest written in the entry is
ignored.

Any other image runs unprivileged: no `/dev/fuse`, no `SYS_ADMIN` and seccomp
`RuntimeDefault`. It therefore gets no rclone cloud mount. Protected cloud
Sessions aren't supported on such an image: their mount requires FUSE, so the
Session fails instead of falling back. An operator who trusts every template
author can set `workspace.customImages.privileged: true` to give custom images
the full profile.

## Known limitations

- A Job's separate IDE pod still runs the installation image, so its terminal
  lacks your image's tools. Sessions run code-server inside the workspace, which
  your image must provide.
- A running Session can't upgrade to a container with `/upgrade-workspace`
  (see [Upgrades](#upgrades)).
- A workspace created from a built-in template keeps its image across releases,
  including work that got `container-full` because it named no workspace. Only
  the plain backend, used when the built-ins are turned off, follows the
  installation image.

## Recovery after a running workspace becomes unavailable

When a Kubernetes workspace's unavailable completion reports exhaust
`WORKSPACE_RECOVERY_MAX_ATTEMPTS` (default `3`), the Job pauses and requires an
explicit Resume. Its workspace files and checkpoints remain retained. Messages
and internal retries do not lift this hold. The workspace remains allocated;
Resume retries work on the retained workspace, while Cancel uses normal terminal
cleanup. The status detail explains that workspace recovery needs attention.

This requires the updated orchestrator and applies to workspace-unavailable
completion reports. Worker queue exhaustion has a separate recovery path.
Previously failed Jobs are not retroactively restored by this change.

For stateless Kubernetes container Jobs, an exhausted worker report that retains
a typed workspace failure also enters this explicit Resume hold. The worker
queue budget and the workspace recovery budget remain separate: this hold does
not consume a new workspace recovery attempt or authorize another graph step,
a connection probe, or cleanup. The original checkpoint and freeze remain
available. Reports without a known typed workspace cause keep their existing
completion behavior; error-message text alone is never treated as evidence.

For an accepted completion reporting an unavailable Kubernetes workspace, a
failed TCP probe now pauses the Job before cleanup. It retains the exact runtime
identity and admits preserve-only cleanup for that Pod. Cleanup must prove
process zero and exact Pod absence before Resume; a lost cleanup reply retries
the same durable intent. Files on the PVC and checkpoints remain retained. A
legacy pinned report without an accepted completion command receives an attention
hold without automatic cleanup. Stateless reports without a durable completion
command are refused by this cleanup path. The interrupted command's outcome is
unknown, so no automatic redispatch is authorized by the failed probe.

After cleanup settles, explicit Resume verifies the captured PVC still exists
with the same UID. The successor creator checks it again and reuses that volume.
A missing or replaced PVC, an uncaptured volume, or incomplete cleanup refuses
Resume; it does not create an empty replacement volume. Cancel retains its
normal terminal cleanup policy, and an older recovery receipt cannot follow
that policy into storage deletion.

If the TCP probe succeeds, the Job still pauses for explicit Resume: connectivity
does not establish whether the interrupted command ran. This path retains the
running workspace and creates no cleanup intent. Duplicate accepted reports
acknowledge the same hold without another attempt or automatic dispatch.

With completion-command admission enabled, stateless Kubernetes container workers
report a typed `workspace_unavailable` stop on its first observation instead of
releasing it for another graph attempt. Other backend and retry contracts remain
separate. These guarantees require the updated worker and orchestrator.

With migration 0293 and the updated worker and orchestrator, a typed container
stop whose report fails before acceptance, or an expired container worker
attempt after bundle authorization, enters a separate execution hold. The queue
token is revoked; the workspace, checkpoint and retry counters are preserved.
The command outcome is unknown, so no successor automatically replays it.

This is a safety-only hold. Resume, admin reassignment and reprovisioning are
blocked while previous execution remains unresolved; Cancel keeps its normal
policy. The hold does not prove the executor or its remote commands stopped and
does not authorize cleanup. This release has no settlement or manual clearing
path for the marker. Historical attempts lacking exact identity also cannot
be made resumable by inferring a replacement Pod's identity. A separate stop
evidence protocol is still required to support recovery from these holds.

Deploy this boundary with a coordinated upgrade: stop new claims, drain work,
and stop every old worker, orchestrator, reaper and control writer before
applying the migration and upgrading all components. Verify their versions
before re-enabling execution and controls. Migration 0293 preserves the marker;
only the updated application enforces all execution admission checks. A mixed
version rolling deployment is not protected. Do not roll back to older binaries
while any execution holds exist unless equivalent guards have been backported.

Stateless execution with completion-command admission disabled also retains its
existing pre-report retry behavior. Automatic replay remains unqualified across
that boundary; an accepted hold does not prove an interrupted command safe.
