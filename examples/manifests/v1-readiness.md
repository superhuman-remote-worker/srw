# First-release readiness

The implemented resource API is `srw/v1alpha1`. The candidate first release
contains the core below; this is not a stable `srw/v1` compatibility declaration.
The [manifest guide](README.md) describes the contract and executable examples.
The [compatibility contract](compatibility.md) records supported adapter/backend
combinations, retry semantics, version handling and upgrade/rollback boundaries.
Its portable alpha fixtures preserve resolution behavior from published develop.

| Area | Implemented core | Current boundary |
| --- | --- | --- |
| Resources | Five authored kinds, JSON/YAML, references and inline definitions, validation, resolution, preview, export, versioned apply and immutable execution snapshots | Generic private configuration uses JSON semantics. The shipped SRW adapter alone interprets its legacy configuration language. |
| Experts | Installation-managed SRW harness and hosting of operator-approved images with optional integration hooks | Generic hosting runs only digest-pinned images on the installation's approved list, with their own entrypoint (`command`, `args` and `probes` are refused), for holders of the `custom_harness_images` grant. It requires a verified installation or the operator's opt-in for unverified clusters. The local K3s startup network-policy check failed; the separate Cilium profile passed. |
| Workspaces | Independent Job/Session selection, compatible custom VM images and resource sizes, initialization, retained Job disks, scoped preparation and caching | Preparation requires same-cluster KubeVirt/CDI and compatible VM images. Cache artifacts are local CDI disks. Existing Sessions retain their own lifecycle; cross-execution Session `instanceRef` selection is separate work. |
| Connectors | Named or inline external-resource configuration, authorization and credential references; explicit environment/file delivery for the native image host | Connectors are broader than MCP. A runtime must support the requested delivery protocol; an arbitrary protocol name does not install an implementation. |
| Projects | Resource collections, defaults, existing Officer configuration and atomic activation of a resolved revision | Automatic team commissioning, package reconciliation and a Project-wide concurrency ceiling remain unsupported. |
| Tags and labels | Common metadata on all five kinds, including version-checked metadata edits on admitted Jobs | Metadata does not grant permissions, select resources or trigger execution. A Job metadata edit preserves its admitted specification, dependencies and execution identity. |
| Clients | Canonical API, thin resource CLI and MCP operations, including workspace-cache management | The broader operational CLI and distributable team packages remain extensions. |

## Local acceptance records

Each record below comes from a disposable local k3d cluster and names the
source revision it tested. It describes that revision, not the current release.

- The [real SSH/tmux gate](verification/k3d-shell-pane-loss-2026-09-14.json)
  passed closed-tab isolation, whole-session failure and stale-token refusal,
  followed by exact cleanup. It is not a VM-recovery or RunQueue admission gate.
- The [ordinary SRW Job/Session smoke](verification/k3d-final-shell-srw-adapter-2026-09-14.json)
  passed all seven workloads and cleanup; running agent Pod source hashes match
  the tested backend.
- The complete MCP/VM/harness preparation sequence passed in both
  [offline](verification/k3d-pod-firewall-prepared-srw-2026-09-14.json) and
  [online](verification/k3d-online-prepared-srw-2026-09-14.json) modes. Each run
  covered cold preparation, independent cache reuse, retained allocation and
  handoff, failed preparation and running-builder cancellation. Every successful
  Job required real guest SSH output. Online proof also required execution of
  the package downloaded during preparation. Owned workloads, retained disks,
  artifacts and credentials retired; scope-local base imports follow cache TTL.
- The [online sudo/retention rerun](verification/k3d-online-sudo-retained-srw-2026-09-14.json)
  passed all six cases; every successful Job also ran a top-level
  `sudo --version` query, which executes no privileged command.
- The [integrated Job/Session smoke](verification/k3d-integrated-srw-adapter-2026-09-13.json)
  passed sandbox/virtual Jobs, existing Job API workspace selection, frozen
  Session configuration, next-turn changes, End/Resume, and the same Session
  Expert on sandbox, virtual and no workspace. All seven workloads and owned
  fixture registrations retired.
- The [integrated MCP/harness preparation gate](verification/k3d-integrated-prepared-srw-2026-09-13.json)
  passed all six cases with a deterministic model fixture, real MCP admission,
  the installed SRW harness and SSH tools. All four successful Jobs required
  actual guest shell output before completion. See
  [repeatable local acceptance](workspace-preparation.md#repeatable-local-acceptance).
- The service-level [preparation gate](workspace-preparation-k3d-evidence.json)
  includes additional cache-policy checks and a separate Cilium network test.
  Its network result does not certify the ordinary K3s profile or online guest
  package installation.

These gates do not establish unattended recovery for retained workspaces, and
the preparation gates do not exercise the VM remote-I/O lease path. Supported
recovery and fault-injection acceptance for retained workspaces remains
release-hardening work.

## Release decisions still required

Keep `srw/v1alpha1` until supported backend behavior, migration/rollback handling
and version compatibility guarantees have been reviewed. Changing a version
string does not establish those guarantees. Automatic team reconciliation, a
complete operational CLI, OCI/S3 distribution and additional adapters remain on
the broader roadmap; this candidate does not claim those features are complete.
