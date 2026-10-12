# Security model

This document describes SRW's intended trust boundary and its limitations. It
is not a security certification and does not make arbitrary model-generated
code safe.

For vulnerability reporting, supported versions, and disclosure expectations,
see [SECURITY.md](../SECURITY.md).

## Threat model

SRW assumes the following may be hostile:

- repository contents and dependencies;
- files supplied by users or external systems;
- web pages, emails, documents, and other prompt-injection sources;
- model-generated files, commands, and child processes;
- tool output returned to the model; and
- a shell-capable workspace after the first command runs.

A capable attacker in the same operating-system security context as the agent
harness can try ordinary debugging facilities, process and filesystem
inspection, dependency attacks, IPC manipulation, native-code injection, or
exploit chains. A working directory, path allowlist, or prompt instruction does
not prevent those attacks.

The trusted computing base therefore includes the Kubernetes control plane and
nodes, SRW orchestrator, agent harness, policy enforcement, authoritative data
stores, and the credentials those services require. A compromise of that base,
the container runtime, host kernel, hypervisor, or a configured external service
is outside the workspace boundary described here.

## Separation of control and execution

```text
TRUSTED CONTROL PLANE                     UNTRUSTED EXECUTION PLANE

┌──────────────────────────────┐          ┌──────────────────────────────┐
│ Orchestrator and policy      │          │ Per-job/session workspace    │
│ Agent harness                │ scoped   │ Repository and generated code│
│ Model and control credentials├─────────►│ File operations and commands │
│ Final job-state authority    │ channel  │ Container or dedicated VM    │
└──────────────────────────────┘          └──────────────────────────────┘
```

The agent harness does not use its own filesystem as a shell-capable job
workspace. The orchestrator provisions a separate workspace and gives the
harness a job- or session-specific backend. If a required remote workspace or
its attested connection cannot be established, work fails closed instead of
falling back to local execution.

This protects normal workspace activity from directly rewriting the harness,
its policy checks, or broad control-plane credentials. Features should pass
only the capability and scoped credential required for one operation. An agent
runtime that requires its harness and subscription credentials to live inside
the writable workspace crosses this boundary and needs a separate threat
analysis; it is not a drop-in model provider.

## Workspace boundaries

| Tier | Security properties | Main residual risk |
|---|---|---|
| **Virtual** | No user-controlled process; file operations are mediated by the backend | Malicious content can still influence later model or human actions |
| **Container** | Separate pod, process namespace, filesystem, service identity, and NetworkPolicy target | Shares the node kernel and container runtime with other workloads |
| **VM** | Separate guest kernel behind QEMU/KubeVirt in addition to pod-level controls | Hypervisor, virtual-device, network, and cluster-control-plane attack surface remains |
| **None** | No workspace filesystem or process | Remote tools and connected data remain in scope |

Container workspaces deserve special attention. The chart supports rclone/FUSE
mounts inside the workspace. On clusters where FUSE requires it,
`workspace.fuse.privileged: true` runs that container privileged with an
unconfined seccomp profile. This is useful functionality, but it is a materially
weaker adversarial boundary than a restricted container. Disable FUSE or its
privileged mode where possible, isolate workspace nodes, and use the VM tier for
work that needs a stronger boundary.

That FUSE profile applies only to SRW's own workspace images (`image.workspace`
and `image.workspaceMinimal`) and to repositories listed in
`workspace.images.trustedRepositories`. A WorkspaceTemplate
may name any other image. Such a custom image runs unprivileged: no `/dev/fuse`,
no `SYS_ADMIN` and seccomp `RuntimeDefault`. It therefore gets no cloud mount
inside the workspace; a Session still gets its folders from the in-pod plane
(below).
`workspace.customImages.privileged: true` gives custom images the full profile.
Enable it only if you trust everyone who can author workspace templates, because
the template's image then decides what runs as root in a privileged container.

Even unprivileged, a template image runs with the workspace owner's secrets, so
use images only from authors you trust. Container resources come only from the
selected template; callers can't set them through `config_override`. See
[container workspace templates](../examples/manifests/container-workspace-templates.md).

### Cloud folders from the in-pod plane

With `connectors.inPodPlane.enabled` (on by default), a Session's container
workspace gets its cloud folders from two sidecars in its Pod, not from
rclone inside the workspace container:

- `srw-fuse-opener` is the only privileged container. It opens `/dev/fuse`,
  mounts it at each folder's place with `nosuid`, `nodev` and, for a
  read-only folder, `ro`, and passes the descriptor to the supervisor. It has
  no network listener and never reads the folders' contents.
- `srw-cloud-mount` runs rclone as uid 65534 with every capability dropped
  and a read-only root filesystem. It alone mounts the cloud credential: an
  rclone config file from a Secret, never an environment variable. Its remote
  controls listen on Unix sockets only.

The workspace container then runs unprivileged: no `/dev/fuse`, no
`SYS_ADMIN`, no privilege, and seccomp `RuntimeDefault` instead of
`Unconfined`, the profile SRW's own images had for FUSE. Its folders are
created when the Session's workspace Pod is created. A folder that does not
mount never holds the workspace back; the Session shows it as unavailable,
with the reason. A folder still coming up, or unavailable, is an empty
read-only directory, so nothing is written where nobody syncs it.

**What users notice:** a shell in such a Session runs like an unprivileged
container. It can't mount FUSE filesystems itself (`sshfs`, `rclone mount`
and `fuse-overlayfs` fail there), and the system calls the default seccomp
profile blocks fail too. Use a VM workspace for that, or turn the plane off.

A Pod that gets no sidecar mount keeps today's profile, FUSE and seccomp
included: with nothing to mount (the main cloud off, say) it gets no plan at
all; when every folder was left out (none has its account yet, say) its plan
only names them, so the Session can say why.

These keep the old in-workspace path and `workspace.fuse`:

- Jobs, which mount no cloud folder: a Job gets its project's folder as a
  copy in its repository.
- VM and lite workspaces.
- Officer sessions.
- OpenCloud folders, whose bearer tokens the agent refreshes.
- Protected Sessions. A protected Session keeps the FUSE profile for its
  capture overlay, so its workspace stays privileged. When its reader grant is
  active as the Pod is created, the read-only lower layer comes from the
  sidecars, using the per-mount reader credential. Read-only remains assisted,
  not guaranteed: it rests on that credential and rclone's `--read-only`. The
  overlay mounts beside the lower, so such a workspace sees the sidecars'
  volume writable; being privileged, it was never held back by a read-only
  view. When that cloud layer does not come up, the Session starts with no
  cloud folder at all and says "protected cloud unavailable" with the reason;
  nothing it writes reaches the cloud unreviewed. A capture overlay an earlier
  attach left is unmounted first, its captured changes kept for the next
  attach, and a `workspace/cloud` link to it is removed. Only the sidecar
  lower starts without its cloud. A protected Session whose lower rclone runs
  in the workspace (its grant was not active as the Pod was created) still
  fails to attach when that lower fails. So does one whose protected mode is
  refused before attach, its grant refused or replaced since the Pod was
  made.

The orchestrator needs `create` on Secrets in the workspace namespace. The
chart grants nothing more: not `get`, `list` or `delete`. Each credential
Secret is immutable and owned by its Pod, so it is garbage-collected with
the Pod. The namespace must admit the privileged opener.

The workspace drops its drain and refresh requests into a directory the
supervisor reads. The supervisor never follows a link there, never blocks on
a FIFO, and reads at most a small regular file. The caches of a Pod's
folders share one emptyDir with no size limit: past a limit the kubelet
would evict the whole workspace Pod, and rclone's cap is soft. They stay
under `connectors.inPodPlane.cacheSize` together, on node disk. The
supervisor requests that much ephemeral storage, so the scheduler places the
Pod on a node with the disk for it, and each rclone stops caching while the
node has less than 2 GB free. The supervisor's memory limit scales with the
folders (`connectors.inPodPlane.supervisorMemory`). An out-of-memory kill
restarts only the supervisor, and its folders come back.

End of a stateless Session waits until every writable folder has uploaded
what it holds. A folder that is unreachable for now (a timeout, a refused
mount, a dead rclone) with uploads still in its cache holds End back, and
End can be retried. When the folder is gone or refuses its credential,
retrying can't help: End goes ahead and logs how many uploads were lost, and
the supervisor's termination message says so when the Pod stops.

### Cloud folders and their credentials

Each cloud folder a Session mounts is a connector with one folder, one
credential and one access level (see [Cloud folders](cloud-folders.md)). What
that credential can reach is the boundary. SRW assists read-only; it never
guarantees it.

- **Read-only is the credential's job.** A Nextcloud project folder at
  `read_only` mounts as a reader account of its own, one per folder and
  project member, whose group the folder grants read permission only. It
  becomes active only after a read-only probe passes as that account. SRW
  never passes off an rclone `--read-only` flag on a credential that can
  write as read-only: where no reader exists (OpenCloud), the level is not
  offered, and a binding that may only read gets no folder.
- **Project folders never mount as the service account** on Nextcloud. A
  `read_write` folder mounts as a writer account per folder and member, which
  may read, change, create and delete in that folder, never share, and can't
  reach another project's folder. Without its account the folder is left out
  (`grant_unavailable`, `reader_unavailable`); it never falls back to
  `agent-service`. The accounts are revoked when the project is deleted, the
  member leaves (or, for the writer, becomes a viewer), the connector goes,
  the user is deleted or the installation changes; a sweep retries revokes
  and removes half-made accounts. A revoke that still fails after 12 tries
  is recorded as a `cloud_folder_grant_revoke_abandoned` security event
  naming the account, which may still be live, and the sweep keeps trying
  it every hour.
- **What still uses a service account**, plainly:
  - Nextcloud's session outbox lives in `agent-service`'s home, and a Session
    mounts it with `agent-service`'s password. That account is a member of
    the group every project Group Folder grants, so it can still write every
    project folder; the outbox is therefore not offered on a VM.
  - The orchestrator's own work on Nextcloud project folders runs as
    `agent-service`: a Job's folder copy and its reviewed write-back, a
    protected Session's approved apply, and the canary file a reader's
    read-only probe tries to change.
  - OpenCloud reaches every project folder and outbox with the orchestrator's
    Keycloak service account, a member of every project Space. A Session's
    workspace holds its short-lived tokens, which reach every project Space
    while they last.
- **A per-folder account is a full Nextcloud user.** Each reader and writer
  is an ordinary account with a password only SRW sets, and it lasts until
  SRW revokes it. Whoever holds the password can sign in as it everywhere a
  user can (WebDAV, the OCS API, the web UI), not only mount its folder. It
  is granted that one folder and no other, and SRW sets its quota to 0 B, so
  it can't store files of its own. What else it can do follows the
  installation's global settings like any user's: it may share only what it
  may share (its folder's grant has no share permission), find other users
  as far as the sharing settings allow, and use the apps enabled for
  everyone. Nextcloud can restrict sharing and user lookup only
  installation-wide, so SRW does not restrict them per account.
- **Where a credential sits.** With the in-pod plane, a folder's credential
  reaches only the mount sidecar. On a VM (Nextcloud `read_only` only) the
  reader's password is in the guest's rclone config: anyone in the guest
  can use that account as above, which reads that one folder. Where a
  Session mounts inside its workspace (the plane off, an officer Session,
  OpenCloud), every folder's credential is in the workspace; on Nextcloud
  that includes the outbox's `agent-service` password.
- **Protected is a connector level.** At most one folder per Session is
  bound at `protected`, and that Session mounts only it: every other
  attached cloud folder is left out as `beside_protected`, never mounted
  beside it. The folder is frozen when the Session is created. The protected
  lower layer is engaged only when the Session's owner is an owner or editor
  of the folder's project, and applying a reviewed change requires the
  person applying it to be one (403 `protected_cloud_apply_not_member`, an
  administrator outside the project included).
- **My cloud is one user's app password with full access.** Nextcloud's
  Login Flow v2 has the user sign in to their own cloud and grant SRW an app
  password. SRW keeps it only when the account's e-mail is the user's, the
  account is enabled, administers neither the cloud nor any of its groups,
  isn't one of SRW's own service accounts, and is the account SRW already
  resolved as the user's in that installation when it has resolved one,
  and the password reads its files; any refusal revokes it at once. The
  e-mail check trusts the cloud's e-mail field, which Nextcloud by default
  lets the account's own user (and its group admins) change; the
  known-account check is what pins a reconnect. It can be limited neither to
  read-only nor to one folder, so it is offered at full access only, and
  only the owner's own Sessions attach it: no administrator or other member
  of the personal project can. It is stored encrypted on the connector and
  never logged, revoked when the user disconnects or their account is
  deleted, and a probe before each new workspace Pod notices a revocation in
  the cloud.
- **My cloud reaches what the user's cloud account reaches, not what SRW
  grants.** A project's folder is shared in the cloud with the project's
  cloud group, at full permission, and a member is added to that group
  directly. A full-account token follows Nextcloud group membership, so an
  SRW viewer of a project can write its folder through My cloud: SRW's
  viewer ceiling binds only the project folder connector. Removing a member
  from the project also removes them from the cloud group, best effort; a
  removal the cloud refused is logged, and the member keeps the folder in
  their own account until it is retried by hand.
- **Only SRW creates cloud folder connectors.** A folder handle someone
  could point would mount with the main cloud's credential, so the API
  refuses the types. The older connectors that held a service account's
  credential to a whole cloud home ("Cloud Storage (Personal)", and the
  bundled Nextcloud row at `admin`'s home) are deleted at startup, and so
  is any other ownerless WebDAV connector that signs in to the main cloud's
  host as `agent-service` or the cloud's admin, however it was renamed or
  linked. A connector someone owns is theirs and stays, whatever credential
  it holds.
- **An explicit empty connector list means no connectors.** No folder
  mounts and no connector binds. The project's knowledge base stays
  reachable as project memory, which follows project membership, not the
  connector list.
- **Nothing takes a failed folder's place.** A folder that can't be bound
  is shown left out, with the reason, and the others mount anyway; SRW
  mounts no other folder in its stead.

## Defense in depth

The separation above is the primary architectural boundary. Other controls
limit impact when individual assumptions fail:

- **Capability and grant checks** filter tools by deployment support, expert
  configuration, workspace tier, user grants, project attachment, and current
  service readiness. Individual operations still authorize at execution time.
- **Human gates** can require approval for tool calls, privileged commands,
  workspace upgrades, job completion, or file-effect application.
- **NetworkPolicy tiers** constrain workspace egress and in-cluster reach. They
  work only when the cluster CNI or paired controller actually enforces
  Kubernetes NetworkPolicy.
- **Scoped credentials** keep broad administrative and model credentials in the
  trusted plane where possible. Credentials intentionally delivered to a
  workspace must be treated as exposed to code in that workspace.
- **Durable audit records** make model requests, tool activity, lifecycle
  events, approvals, and selected effects inspectable when audit collection is
  enabled.
- **Disposable runtimes** reduce persistence. Durable files and checkpoints are
  restored through explicit storage paths rather than by trusting a long-lived
  process.
- **Final-state authority** remains with the orchestrator. An agent reports
  completion or a freeze condition; it does not directly persist its own final
  job status.

These controls should not be represented as proof that model output is safe or
correct. Prompt injection, mistaken authorization, vulnerable dependencies,
supply-chain compromise, kernel or hypervisor escape, and malicious external
services remain possible.

## Deployment guidance

Before exposing a deployment or connecting valuable systems:

1. Pin component images to verified digests or registry-enforced immutable tags;
   do not rely on the source chart's moving development defaults.
2. Put model, encryption, identity, and infrastructure credentials in a secret
   manager rather than values files.
3. Confirm NetworkPolicy enforcement with the actual cluster CNI.
4. Disable privileged FUSE workspaces unless the feature is required; isolate
   workspace nodes when it is enabled. The in-pod plane already removes it
   from most Session workspaces (see above). Keep `workspace.customImages.privileged`
   off, and add only images you trust to
   `workspace.images.trustedRepositories`. Cap what one workspace may request
   with a namespace `LimitRange` and `ResourceQuota`; SRW sets no ceilings of
   its own.
5. Prefer VM workspaces for adversarial repositories or privileged commands.
6. Restrict connector permissions, network destinations, and data scopes to the
   minimum needed by one project.
7. Begin with supervised sessions and review-gated jobs before enabling full
   autonomy, schedules, or project loops.
8. Configure backups outside the failure domain of the SRW cluster and test
   restoration.
9. Monitor audit and lifecycle events, and define retention for workspaces,
   checkpoints, credentials, and logs.
10. Keep Kubernetes, the container runtime, KubeVirt, SRW, and bundled services
    patched.

The local k3d values intentionally contain published development credentials
and a single-node data plane. They are for evaluation only.
