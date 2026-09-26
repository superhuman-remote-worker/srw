# Container workspace templates

A `backend: sandbox` WorkspaceTemplate chooses the image, CPU, memory and storage
of a Job's or Session's container. See [the example](srw-container-workspace.yaml).

```yaml
spec:
  backend: sandbox
  resources: {cpu: 2, memory: 4Gi, storage: 30Gi}
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
| `memory` | request = limit = the value. Memory is reserved, never overcommitted. |
| `cpu` | limit = the value; request = a quarter of it (for example `cpu: 2` → `500m` request, `2000m` limit). |
| `storage` | With PVC workspaces, the claim size. Otherwise the emptyDir `sizeLimit` plus an `ephemeral-storage` request of the same size. |
| `pullPolicy` | The container's `imagePullPolicy`. Resolution fills in `IfNotPresent` when you give an image. |

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

Any image may be used. It must implement the SRW workspace contract: the
`agent-host` user, sshd on port 30022 with SRW's certificate settings, and tmux.
For Sessions, code-server must listen on port 38080 with `auth: password` and
the `HASHED_PASSWORD` value SRW injects; without that value it must not start.
Build your image `FROM` an SRW base image so it inherits that contract.

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
- **Dispatch waits for pulls.** The job dispatcher creates Job containers one at
  a time. While a custom image is still pulling, dispatch of other Jobs waits
  for up to the pull budget. Prefer small images, and lower the budget if that
  matters to you.
- **Don't cancel a Job while its image is pulling.** That can leave its
  workspace resources behind (a known issue). Let the Job fail on its own; it
  fails within the pull budget.
- **Other start failures give no message.** When a templated Job's pod never
  becomes ready for another reason, its workspace stays `creating` and the Job
  waits with no error. Examples:
  - an image without the workspace contract, whose container exits or never
    opens sshd;
  - resources no node can fit, when no `LimitRange` rejects them.

  Check the pod's status and events with `kubectl describe pod
  workspace-<first 12 characters of the Job ID>` in the workspace namespace.
- **Sessions can't recover from a pull failure.** A Session whose image can't be
  pulled logs the reason, and its workspace then stays stuck. It isn't retried,
  and the Session can't be ended or deleted from the UI or the API. An operator
  has to remove it.

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
