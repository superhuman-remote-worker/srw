# Prepared VM workspaces

The SRW adapter can prepare a reusable VM disk before allocating a workspace.
Jobs and Sessions select the same `WorkspaceTemplate` through a reference or
inline configuration. Applying a template stores its recipe; preparation starts
when an execution needs it. See [the prepared development template](srw-prepared-development-vm.yaml).

`environment.prepare` runs ordered argv commands as root **inside a disposable
libguestfs appliance**. Use it for system packages and shared toolchains.
`initialize` runs as `agent-host` inside each allocated VM. Use it for work
directories and per-workspace setup. SRW waits for initialization before
dispatching work into the workspace. Shell expansion requires an explicit `sh -c`
command.

For example, on the Ubuntu development image with online preparation enabled:

```yaml
environment:
  image: YOUR_PINNED_SRW_VM_IMAGE
  cache: Reuse
  prepare:
    - command:
        - sh
        - -c
        - apt-get update && apt-get install -y --no-install-recommends g++ cmake ninja-build
```

The next Job using the same recipe clones the completed toolchain. Repository
checkout and execution credentials still belong to the Job and its Connectors.

A prepared artifact is immutable. Each fresh workspace receives its own writable
clone, machine identity and SSH credentials. Account and Project caches are
separate. Initialization and ordinary working files never flow back into the
cache. `retention: Retain` and `instanceRef` preserve one working disk across Jobs;
they do not turn that disk into a template. Reusing a retained instance works
with new preparation disabled and does not rerun successful initialization.
Its reported initialization success also survives a later attachment that fails
before producing an initialization receipt.
Starting the SRW harness preserves initialized files and existing working trees.
An existing delivery repository must match the requested remote; a different
remote is rejected without clearing the workspace.

The SRW adapter carries the workspace's sudo decision into the admitted harness
configuration: VM commands reach the guest's sudo gate, and a denied sandbox
upgrade remains blocked. The preparation acceptance script's `--sudo-version`
option tests this delivery with a top-level version query in every successful VM
Job; it executes no privileged command.

## Policies

| Setting | Behavior |
| --- | --- |
| `pullPolicy: IfNotPresent` | Reuse the scope's resolved base digest while its cached disk exists; otherwise resolve/import it. |
| `pullPolicy: Always` | Resolve the base reference for every new execution allocation. An unchanged digest can still reuse a prepared artifact. |
| `pullPolicy: Never` | Use a ready matching artifact or an imported base in this scope. Otherwise fail with `ImageNotCached` without pulling the base image. A new recipe may still prepare a clone of a cached base. |
| `cache: Reuse` | Share one completed preparation for matching immutable inputs. Concurrent callers share the build. |
| `cache: Rebuild` | Build once per Job allocation or Session runtime generation. Polls and transport retries keep that allocation; another execution gets a new artifact. |

The key includes the scope, resolved base and builder digests, builder protocol,
ordered commands, architecture, preparation disk size and operator network policy
revision and, when enabled, the complete Pod firewall profile. CPU and RAM affect the allocated workspace, so they do not invalidate
its prepared contents. Changing a template affects new executions; existing
executions retain their captured recipe. Updating a package repository without
changing those inputs needs `Rebuild` or explicit cache eviction.

For compatibility, prebuilt templates with no `prepare` and the default
`IfNotPresent`/`Reuse` keep the existing golden-disk path. Add `prepare: []` to opt
into digest resolution and these cache policies without extra commands.

## Operator setup

This backend requires same-cluster KubeVirt/CDI, persistent VM rootdisks, lifecycle
HMAC authentication and an amd64 SRW-compatible VM image. Build/publish the
`docker/Dockerfile.vm-preparer` image, or use the image published by this release's
CI. Deploy the chart, orchestrator and controller from the same revision before
enabling preparation. The default is disabled.

On a multi-node K3s cluster, the startup network-policy check allowed both
public and private traffic, so online preparation stays disabled there unless
the preparation Pod firewall described below is enabled. Offline commands still
run with libguestfs `--no-network`. An offline acceptance run on such a cluster
passed cold preparation, fresh cache reuse, retained handoff, build failure,
running-builder cancellation and cleanup through MCP and the real SRW harness.
The successful Jobs verified their prepared tool and workspace files through SSH.
The sample cold Job took 16m43s from admission to completion; the cache-hit Job
took 4m29s, including its independent disk clone and VM boot.

With the preparation Pod firewall, the
[offline](verification/k3d-pod-firewall-prepared-srw-2026-09-14.json) and
[online](verification/k3d-online-prepared-srw-2026-09-14.json) local k3d gates
both passed all six MCP/VM/harness cases and cleanup. Online Jobs executed a
package downloaded during preparation, including cache reuse and retained-disk
handoff. Firewall Pods also passed startup checks on every node of a five-node
K3s cluster in owned test namespaces.

```yaml
vm:
  mode: same-cluster
  lifecycleAuthSecretName: srw-vm-lifecycle-auth # Existing Secret with a VM_LIFECYCLE_HMAC_SECRET key.
vmController:
  persistentRootdisk:
    enabled: true
  preparation:
    enabled: true
    diskSize: 30Gi
    maxConcurrent: 2
    maxCacheEntries: 128
    timeoutSeconds: 3600
    importTimeoutSeconds: 2700
    cacheTtlSeconds: 604800
    network:
      enabled: false
```

`diskSize` is the preparation/cache disk capacity, independent of an execution's
larger writable disk. It must fit the base image and installed software.
Preparation does not boot cloud-init; the base guest filesystem must have enough
free space for its install commands. Increasing PVC capacity alone does not promise offline
partition/filesystem growth for arbitrary custom images. The
workspace's `resources.storage` must be at least this size; omission defaults to
this size. Imports, cloning and preparation have bounded waits. Their overall
wait budget is three import windows plus one build window and five minutes for
handoff; polling never resets it or spends ordinary boot retries.

Builds receive no service-account token, host socket, host devices, workspace SSH
key or Connector credentials. Networking is off by default. Package downloads
require `network.enabled: true` and `network.enforcementVerified: true` after the
operator verifies isolation from the first application packet, including new Pod
startup. Merely having a NetworkPolicy object is insufficient; both the local
k3d K3s profile and a multi-node K3s cluster allowed startup traffic before
enforcing the policy.

The optional `network.podFirewall: true` profile installs default-deny rules in
each builder Pod before preparation starts. A separate trusted init container
uses the same pinned preparer image and receives `NET_ADMIN` only in the Pod's
network namespace. It mounts neither the disk nor authored input. The ordinary
builder stays UID 107, drops all capabilities and cannot alter these rules.
There is no host network, device, credential or service-account mount. The
namespace's admission policy must permit that trusted init capability; SRW does
not relax Pod Security settings automatically. The preparer image must include
the matching firewall module and iptables tools.

Online mode permits IPv4 HTTP/HTTPS outside the configured blocked CIDRs and
DNS to the Pod's exact IPv4 resolver addresses on port 53. IPv6 is denied.
The profile requires the default private/special exclusions, allows additional
blocked CIDRs, and rejects `network.additionalEgress` because arbitrary Kubernetes
rules cannot be translated into this bounded profile. Offline mode permits no
network traffic. The chart's CNI NetworkPolicy remains in place in both modes.
An init failure prevents the builder from starting; uncertain process state
still quarantines the artifact rather than releasing its disk.

Run the dedicated gate with the candidate preparer image before attesting this
profile on an installation:

```bash
python scripts/workspace-preparation-firewall-gate.py \
  --context YOUR_CONTEXT --image YOUR_PREPARER_IMAGE_AT_DIGEST \
  --output /tmp/srw-preparation-firewall.json
```

It uses an owned namespace, tests every selected node without CNI isolation and
then with the actual chart policy, includes positive network controls and a
failed-init case, and verifies cleanup. It changes no existing policies.
`--node` can select individual nodes; `--waves` controls repeated Pod startups.
This network gate does not by itself prove that a particular guest image can
install packages; also exercise that image's real preparation recipe.

On local k3d, after passing that network gate, set `network.podFirewall`,
`network.enabled` and `network.enforcementVerified` to `true` in the preparation
values and deploy through Tilt. Then run the complete MCP/harness acceptance
with actual package installation:

```bash
python scripts/workspace-preparation-srw-k3d-gate.py --online-package \
  --output /tmp/srw-prepared-online.json
```

This mode requires an Ubuntu-compatible base without the `hello` package. It
installs the package during preparation and requires the real guest SSH tool to
execute it before accepting completion, including cache reuse and retained-disk
handoff. It also exercises failed and cancelled builds. Without the option, the
gate requires offline preparation and uses its self-contained prepared tool.
Tilt replaces saved MCP/preparer pins with the digest of its freshly pushed
image, using Tilt's local registry address. The controller therefore does not
need to resolve k3d's node-side registry hostname. Ordinary Helm installations
retain their explicit digest pins.

For installations relying solely on an independently verified CNI,
`scripts/workspace-preparation-network-gate.py` remains the policy-only check.
Its generated policy permits cluster DNS and public HTTP/HTTPS, excluding
private and link-local ranges. Explicit `network.additionalEgress` can authorize
internal mirrors only when Pod firewall mode is off. All these settings are
installation-owned and apply only to preparation Pods; they do not enable
generic harness hosting or change execution workspace networking.

Public OCI base images are resolved only through `registryHosts` and the separate
`tokenHosts` allowlist. Private base-registry authentication and preparation-time
Connector secrets are not implemented. `global.imagePullSecrets` applies to the
trusted builder image. `insecureRegistryHosts` is an explicit development-only
opt-in for registries such as a local k3d registry.

For a private builder image, pin its digest and allow its registry host; the
controller does not resolve private tags with image-pull credentials. Tilt
supplies a verified digest for its builder, which Kubernetes pulls through the
node's configured registry route. Any unpinned image still needs a registry
address the controller can resolve and reach; CDI must likewise reach a base
image's registry. Allow the reference's host and configure `insecureRegistryHosts`
when it serves HTTP. A node-only mirror or Docker-network hostname alone does
not provide that Pod DNS route.

## Cache operations and failures

Using the existing manifest CLI authentication (`SRW_TOKEN`):

```bash
python -m orchestrator.operator_cli.manifest_resources cache list
python -m orchestrator.operator_cli.manifest_resources cache list --scope-kind Project --scope-name PROJECT_UUID
python -m orchestrator.operator_cli.manifest_resources cache delete ARTIFACT_UUID
```

The API equivalents are `GET /api/workspace-cache` and
`DELETE /api/workspace-cache/{artifact_id}`, with `scope_kind` and `scope_name`
query parameters. Reads use scope visibility; deletion requires scope write
permission. Deletion addresses an immutable artifact UUID and returns a conflict
while the artifact is building, referenced by a pending allocation, or still used
by CDI. Eviction does not delete already cloned workspaces.

The TTL collects unused prepared disks and base imports. Zero disables TTL
collection. `maxCacheEntries` bounds the number of base and prepared entries;
cache hits remain available at capacity, but new entries fail with
`CacheCapacityExceeded` until space is available. These shared cache PVCs count
as platform storage overhead; allocated workspace disks retain their normal
execution/instance attribution.

Build output is available through the preparation Pod logs while it exists.
A cluster log collector can retain that output after Pod retirement; raw logs
are not copied into manifests or API responses.

Failed commands never publish an artifact or start a VM. Cancellation waits for
the builder's terminal state before removing its disk. Controller restarts recover
durable build records and never substitute a new writer for a missing one.
Deleting work that failed before VM allocation uses signed cancellation evidence
that no workspace source was ever supplied, plus exact runtime absence and the
current owner generation. A supplied source or uncertain allocation still requires
the existing VM retirement proof.
An unaccounted-for or replaced builder is quarantined as `Lost`; its disk cannot
be reused automatically. Inspect its exact Pod/PVC identities and establish that
the writer has stopped before operator recovery. A timeout alone is insufficient.

Sessions persist preparation separately from VM identity. Ending a Session while
it is building closes admission and cancels its allocation; resuming uses the new
runtime generation. Physical VM retirement keeps the existing VM/PVC identity
checks. Disabling preparation rejects new builds and retains controller cleanup
permissions and NetworkPolicy so existing allocations can be cancelled and collected.

The implemented cache is a CDI PVC in the controller's namespace. OCI/S3 artifact
export, cross-cluster distribution, generic-harness VM providers and sandbox image
builders are separate capabilities. This feature does not enable those backends.

## Repeatable local acceptance

After a coherent Tilt deployment with the offline preparation settings above,
run this gate from the same checkout using its installed Python dependencies:

```bash
python scripts/workspace-preparation-srw-k3d-gate.py --output /tmp/srw-prepared-mcp-evidence.json
```

The gate requires the local `k3d-srw` cluster, the normal development test login
and Keycloak bootstrap access, trusted localhost TLS, and the local registry on
port 5005. It verifies deployed source and hosting capabilities before creating
resources. `--base-image` can select a different digest-pinned compatible VM
image; the default is the pinned SRW development disk. A cold import can take
tens of minutes.

An owned deterministic model provider drives the installed SRW harness through
real MCP admission and SSH tool calls. The gate checks two fresh Jobs sharing one
prepared artifact with separate writable disks, a retained Job handoff, successful
initialization exactly once per disk, metadata updates without execution replay,
build failure and cancellation while the builder runs. The provider requires the
actual shell proof before it permits the assignment to finish.

Cleanup retires the owned Jobs, retained disks, recipe artifacts, model fixture
and temporary credentials. The scope's imported base image remains subject to the
configured cache TTL. Evidence includes resource identities and results; arbitrary
tool output, logs and credentials are excluded. A failed cleanup keeps the gate
failed and records the stage for recovery.

The [2026-09-13 acceptance record](verification/k3d-prepared-srw-mcp-2026-09-13.json)
passed all six cases and cleanup on the installed candidate. Each successful Job
verified prepared software and disk contents through the actual SRW shell tool.
