# Build your own workspace image

A Job or Session on the Container tier runs in a container whose image comes
from a workspace template. This guide takes you from "I need tool X" to Jobs and
Sessions that run on your own image: pick a base, write a Dockerfile, check the
image locally, push it, and point a template at it.

The reference for templates and the image contract is
[Container workspace templates](../examples/manifests/container-workspace-templates.md).
This guide links to it instead of repeating it.

## When to build one

Try a built-in template first. By default an installation has
`container-minimal` and `container-full`, and `vm-full` when your operator has
turned VMs on; an operator can turn the built-ins off with
`workspace.builtinTemplates.enabled`. Their contents are listed under
[SRW base images](../examples/manifests/container-workspace-templates.md#srw-base-images).

Build your own image when a base lacks something you need on every run, such as
a system package, a compiler or a command-line tool. The workspace has no
`sudo`, so the agent can't install system packages at run time.

## 1. Pick a base image

Build `FROM` one of SRW's two base images. Both already implement the
[workspace contract](../examples/manifests/container-workspace-templates.md#images)
that SRW needs to reach and manage a workspace.

- `srw-workspace-minimal` has the contract, a browser and a small set of
  command-line tools. Start here for a lean image.
- `srw-workspace` adds Node.js, compilers, database clients and document tools.
  Start here if you need Node.js or a compiler.

Use the base with the tag your installation runs, so that your image matches the
SRW release around it. The built-in templates name exactly those images:

1. Open **Customize → Workspaces**. The **Shared** group lists the built-ins by
   display name: **Container (minimal)** is `container-minimal`, and
   **Container (full)** is `container-full`.
2. Open the one you want to build on. The list's Image column leaves out the
   registry, so open the template: its **Image** field shows the full
   reference, for example
   `ghcr.io/superhuman-remote-worker/srw-workspace-minimal:<tag>`.
3. Copy the reference. The form is read-only. If your browser won't let you
   select the text, click **Duplicate**, copy the reference from the editable
   copy, and leave with **All workspaces** without saving.

Over MCP, `manifest_list` with `scope_kind="Catalog"`, `scope_name="shared"` and
`kind="WorkspaceTemplate"` returns the same templates with their
`spec.environment.image`. Your operator can also read the references from the
Helm values `image.workspaceMinimal` and `image.workspace`.

Set two shell variables for the commands that follow:

```bash
BASE=ghcr.io/superhuman-remote-worker/srw-workspace-minimal:<tag>   # the reference you copied
IMAGE=registry.example/team/lint-workspace:1.0                      # where you will push your image
```

The base images are built for `linux/amd64`. On another architecture, add
`--platform linux/amd64` to the build command in step 3.

## 2. Write the Dockerfile

Create an empty directory with this `Dockerfile`. It adds ShellCheck from
Ubuntu's packages and yamllint from PyPI:

```dockerfile
ARG BASE
FROM ${BASE}

# System packages: apt installs system-wide.
RUN apt-get update \
    && apt-get install -y --no-install-recommends shellcheck \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Python packages: unset PIP_TARGET so that pip installs system-wide.
RUN env -u PIP_TARGET pip install --break-system-packages --no-cache-dir yamllint

# Last step: fail the build if the image no longer meets the workspace contract.
RUN /usr/local/bin/assert-workspace-contract
```

Follow three rules:

- **Install system-wide.** A workspace mounts its volume over
  `/home/agent-host`, so anything your build puts there is hidden. The base
  images point `PIP_TARGET` and `npm_config_prefix` into that directory for the
  agent's own installs; unset them for yours, as above. For npm, write
  `RUN env -u npm_config_prefix npm install -g <package>`. Node.js and npm are
  only in `srw-workspace`, not in the minimal base.
- **Don't set `ENTRYPOINT`, `CMD` or `USER`.** The base's entrypoint runs as
  root, starts sshd and keeps the container running. SRW reaches the workspace
  over SSH, so a container that exits never becomes ready. Replacing
  `ENTRYPOINT` or `USER` stops the workspace from starting. The entrypoint
  ignores `CMD`, so a `CMD` does nothing and only misleads whoever reads the
  Dockerfile.
- **End with `assert-workspace-contract`.** It checks the programs and settings
  SRW relies on and fails the build when one is missing. The
  [contract table](../examples/manifests/container-workspace-templates.md#images)
  says what each one is for.

## 3. Build and check it locally

The commands use Podman. With Docker, write `docker` instead of `podman`; the
options in this step are the same, except in the last command: Docker's `rm`
has no `-t` option and kills at once, so write `docker rm -f workspace-test`.

1. Build the image:

   ```bash
   podman build --build-arg BASE="$BASE" -t "$IMAGE" .
   ```

   The last step runs the contract check. If an item is `MISSING`, the build
   fails and names it.

2. Start it in the background and check, after a few seconds, that it is still
   running:

   ```bash
   podman run -d --name workspace-test "$IMAGE"
   sleep 10
   podman inspect --format '{{.State.Status}}' workspace-test
   ```

   It prints `running`. The base's entrypoint keeps the container up even
   without SRW's environment. Its log says that code-server was not started
   because no credential was injected; that is expected outside SRW. If the
   status is `exited`, read `podman logs workspace-test` and check the rules in
   step 2.

3. Check your tools as the workspace user, `agent-host`:

   ```bash
   podman exec --user agent-host workspace-test shellcheck --version
   podman exec --user agent-host workspace-test yamllint --version
   ```

4. Remove the test container:

   ```bash
   podman rm -f -t 0 workspace-test
   ```

   The entrypoint doesn't stop on `SIGTERM`, so without `-t 0` Podman waits 10
   seconds and then kills it with `SIGKILL`.

## 4. Push it

Push the image to a registry your cluster's nodes can pull from. Give the
template a digest (`…@sha256:…`) or a tag you never move: SRW doesn't pin tags,
so a restored or rewoken workspace may pull a newer image behind the same tag (see
"Pin digests or immutable tags" under
[Images](../examples/manifests/container-workspace-templates.md#images)).

With Podman, write the digest to a file and print the full reference:

```bash
podman push --digestfile image-digest.txt "$IMAGE"
echo "${IMAGE%:*}@$(cat image-digest.txt)"
```

With Docker, read it from the pushed image:

```bash
docker push "$IMAGE"
docker inspect --format '{{index .RepoDigests 0}}' "$IMAGE"
```

Either prints the reference for the template, for example
`registry.example/team/lint-workspace@sha256:…`.

A private registry needs credentials on the cluster side. Workspace pods don't
use the chart's `global.imagePullSecrets`; your operator adds the pull secret to
the namespace's `default` ServiceAccount or configures the nodes. See "Private
registries" under
[Images](../examples/manifests/container-workspace-templates.md#images).

On the local cluster from [Local Kubernetes with k3d](local-kubernetes.md), your
machine reaches the cluster's registry as `localhost:5005` and the cluster's
nodes reach it as `srw-registry:5000`:

- In `BASE`, replace `srw-registry:5000/` with `localhost:5005/`.
- Set `IMAGE` to `localhost:5005/<name>:<tag>`, with a tag.
- In the template's reference, replace `localhost:5005/` with
  `srw-registry:5000/`, which gives `srw-registry:5000/<name>@sha256:…`.
- The registry serves plain HTTP: with Podman, add `--tls-verify=false` to
  `podman build` and `podman push`.

## 5. Create the template

In the cockpit:

1. Open **Customize → Workspaces** and click **New template**.
2. Enter a **Name**: lowercase letters, digits and hyphens. It can't be changed
   later.
3. Choose where to **Save to**:
   - **Mine**: only you can use it.
   - **Project: …**: the project's members can use it. You need the owner or
     editor role in that project.
   - **Shared**: everyone on the installation can use it. Only administrators
     can save here.
4. Leave **Tier** on **Container** and paste your reference into **Image**. A
   notice says that this isn't one of SRW's images; see
   [When it doesn't start](#when-it-doesnt-start) for what that changes. The
   image runs with your connector and repository credentials, so use only
   images whose authors you trust (see "Use images only from authors you
   trust" under
   [Images](../examples/manifests/container-workspace-templates.md#images)).
5. Set **CPU**, **Memory** and **Disk**, or leave them empty for the
   installation's defaults. **Advanced** holds the guaranteed CPU and memory and
   the pull policy.
6. Click **Save**.

Saving checks the template but doesn't pull the image. A wrong reference shows
up when the first Job starts.

You can write the same template as a manifest instead, based on
[the example](../examples/manifests/srw-container-workspace.yaml):

```yaml
apiVersion: srw/v1alpha1
kind: WorkspaceTemplate
metadata:
  name: lint-workspace
  scope: {kind: Account, name: me}
spec:
  backend: sandbox
  resources:
    cpu: 2
    memory: 4Gi
    storage: 20Gi
  environment:
    image: registry.example/team/lint-workspace@sha256:<digest>
```

Apply it with the MCP tool `manifest_apply` or with
`srw apply -f lint-workspace.yaml`; see
[Native CLI and MCP](../examples/manifests/README.md#native-cli-and-mcp). For a
project template, write `scope: {kind: Project, name: <project UUID>}`.

## 6. Use it

- **New Job and New Session.** Pick the template in the **Workspace** field.
  Your own templates are under **Mine**, a project's under **This project**.
- **As a project's default.** On the project page, open **Settings → Workspace
  defaults**, set **Jobs** or **Sessions** to **Container**, and choose your
  template as the **Container template**. Project owners and administrators can
  change these. A project's default must be a Shared template or one of the
  project's own; only your personal project can also use a template from Mine.
  A Mine template can still be picked for a single Job or Session in any
  project. See
  [A Project's defaults](../examples/manifests/container-workspace-templates.md#a-projects-defaults).
- **Over MCP.** Pass `workspace="lint-workspace"` to `create_job`,
  `create_project_job` or `create_persistent_thread`. SRW looks the name up in
  the project, then in your own templates, then in Shared.

**Try a new image with a Job first.** A Job shows more about why its workspace
didn't start; see [When it doesn't start](#when-it-doesnt-start). A Session may
show nothing. On a default installation a container Session has no workspace
status line at all. Where your operator runs Sessions on stateless executors
(`agent.stateless.enabled`) without startup-stage tracking, it keeps saying
"Checking workspace scheduling; the result is not yet confirmed." You can still
End a Session whose image can't be pulled. See
[When a workspace can't start](../examples/manifests/container-workspace-templates.md#when-a-workspace-cant-start).
To have the trial Job run your tools, give it an Expert with a shell, such as
**Engineer**: pick it under **Agent Expert** on New Job, or pass
`expert="engineer"` over MCP. A non-admin needs the `shell_tools` capability
grant from an administrator to use an Expert with the shell tools. To make it
your default, use **Duplicate** on Engineer in **Experts**, then pick the copy
under **Settings → Defaults → New jobs**. A Project owner can set a Project's
default Expert with `defaults.expert` in the Project manifest.

The agent runs your tools through its shell, so only an Expert with a shell can
use them. The shipped default Expert for Jobs, **General Worker**, has none; a
Job on it can't run your tools even when the image has them. A Job's IDE
terminal runs in a separate pod with the installation's image, so it doesn't
have your tools either. See
[Known limitations](../examples/manifests/container-workspace-templates.md#known-limitations).

## When it doesn't start

| Symptom | Cause | Fix |
| --- | --- | --- |
| The Job stays **Created** and never starts, with no error message. Without startup-stage tracking (the default), the Jobs list says "Checking an older workspace creation; completion evidence is unavailable." after about two minutes. Where your installation reports container startup stages (see below), it says "Workspace needs attention: it didn't become ready in time. If it uses your own image, check that the image keeps running: build it FROM an SRW base image and don't override its ENTRYPOINT or USER." once the startup limit has passed: for your own image, `workspace.imagePullTimeoutSeconds` (600 seconds by default). | The container exited or never opened SSH, for example because the image replaces the base's `ENTRYPOINT` or `USER`, or isn't built `FROM` an SRW base. Without startup-stage tracking, SRW doesn't name the cause. | Cancel the Job. Rebuild `FROM` a base without those settings, and run step 3 before you push. |
| The Job fails with "Workspace image `<ref>` could not be pulled: `<reason>`". Where your installation reports startup stages, the Job stays **Created** instead and the Jobs list says "Workspace needs attention: its image didn't finish pulling in time. Check the image reference and that the cluster can pull from its registry.", or "Workspace needs attention: its image is invalid." for a malformed reference. | The reference is wrong, the pull secret is missing, or the nodes can't reach the registry. | Check the reference from step 4 and the registry access. Where startup stages are reported, Cancel the Job. See [When a workspace can't start](../examples/manifests/container-workspace-templates.md#when-a-workspace-cant-start). |
| `/cloud` is empty; your cloud folder isn't mounted. | Images that aren't SRW's run unprivileged, without the FUSE mount. | Your operator adds your image's repository to `workspace.images.trustedRepositories`. `workspace.customImages.privileged: true` gives every custom image that profile, which is one step from root on the node; it suits only an installation that trusts everyone who can author templates. See [Privilege](../examples/manifests/container-workspace-templates.md#privilege). |
| "command not found" for a tool your Dockerfile installed. | It was installed under `/home/agent-host`, which the workspace volume hides, for example by `pip` or `npm` without unsetting `PIP_TARGET` or `npm_config_prefix`. | Install system-wide, as in step 2. |

Startup stages are reported when the operator enables Helm
`orchestrator.containerStartupStageAuthority.enabled` (off by default); see
[Container startup stage rollout](../helm/README.md#container-startup-stage-rollout).

An operator can read a Job's pod status and events with
`kubectl describe pod workspace-<first 12 characters of the Job ID, hyphen included>`
in the workspace namespace. For example, Job `1ec4b61d-51fc-…` has the pod
`workspace-1ec4b61d-51f`.

## VM workspaces

A VM workspace boots a VM disk image (shipped as an OCI image), not a workspace
container image, so the steps above don't apply. To add software to a VM
workspace, give a VM template `prepare` steps. They run as root in an isolated
builder before the VM starts; SRW caches the prepared disk and reuses it for
later VMs with the same recipe.

Your operator must turn preparation on first: `vmController.preparation.enabled`
is off by default, and it needs VMs in the installation's own cluster
(`vm.mode: same-cluster`). Until then, SRW refuses `prepare` steps with "VM
workspace preparation requires enabled same-cluster hosting." Preparation runs
offline unless the operator also enables its network, which package downloads
such as the one below need. See
[Operator setup](../examples/manifests/workspace-preparation.md#operator-setup).

This template is `vm-full` with a C++ toolchain added:

```yaml
apiVersion: srw/v1alpha1
kind: WorkspaceTemplate
metadata:
  name: vm-cpp
  scope: {kind: Account, name: me}
spec:
  backend: vm
  resources:
    cpu: 8
    memory: 16Gi
    storage: 30Gi
  environment:
    image: <the image of vm-full>
    cache: Reuse
    prepare:
      - command:
          - sh
          - -c
          - apt-get update && apt-get install -y --no-install-recommends g++ cmake ninja-build
```

Copy `image` from `vm-full` the way step 1 describes, and apply the template as
in step 5. You need permission to use VM workspaces.

The cockpit's template form doesn't edit `prepare` steps, and it keeps them
unchanged when you save. For the VM tier, its **Setup steps** field under
**Advanced** holds `initialize` steps instead: one shell command per line, run
as `agent-host` in each new VM. Use them for per-workspace setup such as
directories, and keep system packages in `prepare`.
[Prepared VM workspaces](../examples/manifests/workspace-preparation.md) covers
caching, limits and failures.

Building your own bootable VM disk is outside this guide. Such a disk must
implement SRW's guest contract; see
[Execution-owned workspace selection](../examples/manifests/README.md#execution-owned-workspace-selection).
