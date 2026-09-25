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
Editing the template affects new executions only.

## How the numbers map to Kubernetes

| Field | Pod |
| --- | --- |
| `memory` | request = limit = the value. Memory is reserved, never overcommitted. |
| `cpu` | limit = the value; request = a quarter of it (for example `cpu: 2` → `500m` request, `2000m` limit). |
| `storage` | With PVC workspaces, the claim size. Otherwise the emptyDir `sizeLimit` plus an `ephemeral-storage` request of the same size. |
| `pullPolicy` | The container's `imagePullPolicy`. Resolution fills in `IfNotPresent` when you give an image. |

A field you leave out keeps the installation default: 500m/1Gi requested,
2 CPU/4Gi limit, and the installation's storage size. An existing Session volume
is never resized.

SRW sets no ceilings of its own. Limit what one workspace may request with a
namespace `LimitRange`, and the namespace total with a `ResourceQuota`. The
chart's `workspace.resourceQuota` already covers storage. A pod that such a limit
rejects fails its Job with the cluster's message.

## Images

Any image may be used. It must implement the SRW workspace contract: the
`agent-host` user, sshd on port 30022 with SRW's certificate settings, and tmux.
Build your image `FROM` an SRW base image so it inherits that contract.

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
- **Pull failures.** `InvalidImageName` and `ErrImageNeverPull` fail at once.
  `ErrImagePull`, `ImagePullBackOff` and `CreateContainerConfigError` fail after
  `workspace.imagePullTimeoutSeconds` (default 600 seconds). A pod the cluster
  itself rejects (a `ResourceQuota` or `LimitRange` 403) fails at once with the
  cluster's own message. A Job fails with "Workspace image `<ref>` could not be
  pulled: `<reason>`"; a Session logs the reason and retries on its next
  workspace check. The job dispatcher creates Job containers one at a time, so
  while a custom image is still pulling, dispatch of other Jobs waits up to
  that budget — prefer small images, and lower the budget if that matters to
  you.

## Privilege

Workspaces from the installation image, and from repositories listed in
`workspace.images.trustedRepositories`, keep the privileged FUSE profile used for
the cloud-storage mount.

Any other image runs unprivileged: no `/dev/fuse`, no `SYS_ADMIN` and seccomp
`RuntimeDefault`. It therefore gets no rclone cloud mount. An operator who trusts
every template author can set `workspace.customImages.privileged: true` to give
custom images the full profile.

## Known limitations

- A Job's separate IDE pod still runs the installation image, so its terminal
  lacks your image's tools. Sessions run code-server inside the workspace, which
  your image must provide.
- A virtual Session upgraded to a container gets the installation defaults.
