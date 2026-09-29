# Container workspace templates

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

## Images

Any image may be used. It must implement the SRW workspace contract. The simplest
way is to build `FROM` an SRW base image, which already does.

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

**Test a new image with a Job first.** A Job reports why its image failed and
cleans up after itself; a Session doesn't (see below).

- **How pull failures are classified.** These rules apply to custom images. A
  pod using the installation image keeps the plain 120-second readiness wait.
  - `InvalidImageName` and `ErrImageNeverPull` fail at once.
  - `ErrImagePull`, `ImagePullBackOff` and `CreateContainerConfigError` fail once
    `workspace.imagePullTimeoutSeconds` has passed (default 600 seconds).
  - A pod the cluster itself rejects (a `ResourceQuota` or `LimitRange` 403)
    fails at once with the cluster's own message.
- **Jobs.**
  - A Job fails with "Workspace image `<ref>` could not be pulled: `<reason>`".
  - On a Job's first creation on a fresh volume, its pod, service and volume are
    then cleaned up within about a minute. A restored Job, or one recreated over
    a kept volume, keeps them.
  - Deleting the Job during that minute may return 503 once; retry and it
    succeeds.
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
- **Other start failures give no message.** When a templated Job's pod never
  becomes ready for another reason, its workspace stays `creating` and the Job
  waits with no error. Examples:
  - an image without the workspace contract, whose container exits or never
    opens sshd;
  - resources no node can fit, when no `LimitRange` rejects them.

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

Workspaces from the installation image, and from repositories listed in
`workspace.images.trustedRepositories`, keep the privileged FUSE profile used for
the cloud-storage mount. An entry matches its repository with any tag or digest;
a tag or digest written in the entry is ignored.

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
- A virtual Session upgraded to a container gets the installation defaults.

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
