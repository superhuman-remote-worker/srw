# Manifest compatibility contract

The authored API is **`srw/v1alpha1`**. These are the supported boundaries for
the first-release candidate. They do not declare a stable `srw/v1` API or promise
that every runtime implements every field accepted by the resource schema.
See [first-release readiness](v1-readiness.md) for deployment and acceptance evidence.

## Versions and resource identity

| Identifier | Meaning |
| --- | --- |
| `apiVersion: srw/v1alpha1` | Authored Expert, WorkspaceTemplate, Connector, Project and Job documents. Other authored API versions are rejected before resolution. |
| `runtime.adapter: srw/v1` | The shipped harness adapter. This is independent of the authored API version. |
| `srw/resolved-config-v1` | Internal SRW execution snapshot format. It is not an authored manifest API. |
| Resource UID and `resourceVersion` | Server identity and optimistic concurrency token. A resource update must supply its observed version. |
| Execution/attempt/attachment generation | Runtime identities used for admission, retry and fencing. They are not interchangeable with resource versions. |

JSON and YAML express the same data model. Generic private configuration preserves
JSON `null`, ordered arrays and unknown private keys. There is no generic `$ref`,
`$extends`, null-deletion or deep-merge language. Those legacy operations belong
only to the versioned SRW adapter. Unknown envelope/spec fields fail validation;
an unknown key inside an explicitly private configuration object is opaque.

References resolve within an explicit scope. A Project can compose resources and
provide execution defaults. Omitted selections inherit applicable defaults;
`workspace: null` selects no workspace and `connectors: {}` selects no connectors.
An Expert's workspace preference is advisory. Tags, labels and annotations do not
grant authority, select a resource or start work.

Apply replaces an authored resource under an expected-version check. It is not
Kubernetes server-side apply with field managers, strategic merging or CRDs.
Project activation captures one fully resolved generation for new admissions;
editing a dependency does not partially rewrite an active generation. A failed
Project update leaves its prior active generation intact. A bundle is not a
cross-resource transaction: unrelated resources may have been stored before a
later document fails. Validate/preview first, then read back an interrupted apply.

An admitted Job's assignment, selections and resolved configuration are immutable.
Reapply, metadata updates and installation restarts do not create a new execution
of completed work. A deliberate new assignment needs a new Job identity. Session
configuration changes create new captured generations through the Session API;
they do not mutate a Job snapshot or silently adopt newer template defaults.

## Installed runtime support

| Capability | Shipped `srw/v1` adapter | Generic image host |
| --- | --- | --- |
| Harness image | Omit the image to follow the installed SRW harness. An explicit image must match the installation at admission. | An ordinary image, with its image defaults or explicit launch settings; no mandatory SRW hooks. Requires enabled, verified hosting. |
| No workspace | Supported through explicit `workspace: null`. | Supported. |
| Virtual workspace | Backend selection supported. | Not implemented. |
| Sandbox workspace | Image, pull policy, CPU, memory and storage supported ([reference](container-workspace-templates.md)); custom images run unprivileged unless the operator allows the full profile. prepare, cache: Rebuild, initialize and Retain are rejected. | Compatible SSH workspace image, resources, initialization and retained instances supported. Harness and workspace have separate lifecycles. |
| VM workspace | Compatible bootable VM image, whole-core CPU, RAM, storage and initialization supported. | Not implemented. |
| Prepared VM cache | Same-cluster KubeVirt/CDI, persistent rootdisks, authenticated lifecycle and enabled preparation required. | Not implemented. |
| Cross-Job retained workspace | Same-cluster VM instances. | Sandbox instances. |
| Session instance selection | Sessions keep their own suspend/resume disk lifecycle. Cross-execution `instanceRef` selection is rejected. | Generic interactive Session hosting is not implemented. |
| Connector delivery | `srw.datasource/v1`, referencing an authorized datasource. Other drivers are rejected at Job admission. | Explicit `srw.env/v1` and `srw.files/v1` delivery. Datasource and unknown drivers are rejected. |
| Manifest completion/retry | `Reported`, one manifest attempt. Existing SRW pause/resume and completion controls keep their own contracts. | Process exit or enabled optional reported-completion integration, with bounded attempts and a total execution timeout. |

A Connector describes an external resource; MCP is one possible implementation
behind an adapter. Defining a Connector or naming a tool cannot install a driver
or grant access. Runtime permission checks and workspace/network/credential
boundaries enforce access. A valid portable definition can still receive a clear
admission error when the selected installation lacks its backend or driver.

Generic retries reuse the execution's bound workspace disk and successful
initialization, with a new attachment generation after prior processes retire.
`Delete` releases the disk at final execution retirement; `Retain` also permits a
later Job to attach it. There is no separate fresh-disk-per-attempt setting in this
release. The execution timeout includes queueing and earlier attempts. Uncertain
termination cannot be treated as permission to start a competing writer.

## Preparation and persistence

`prepare` changes a clone of a compatible base disk inside libguestfs. `initialize`
runs in each allocated workspace. Prepared artifacts are immutable and scoped to
an Account or Project; every fresh workspace gets an independent writable clone.
Working files and initialization changes do not flow back into the template cache.

Cache identity includes the resolved base and builder digests, preparation steps,
scope, disk format/size, architecture and operator network policy. When enabled,
the complete Pod firewall profile is also part of that identity. CPU/RAM sizing
does not alter cached contents. Changing a template affects new admissions; bound
preparation retains its captured profile across controller restart.

Package repositories are external mutable inputs. An unchanged recipe with `Reuse`
can intentionally keep older packages. Select `Rebuild`, change the recipe or evict
the unused artifact when a fresh package resolution is required. Use immutable
base image references for reproducibility. Cache TTL and explicit eviction are
resource management operations, not backups.

Retained instances preserve their files across permitted Job handoffs, including
files an earlier Job saved. A new Connector selection does not erase credentials
previously written to that disk. New attachment generations receive fresh VM/SSH
identity and are subject to current authorization and retirement checks.

The optional preparation Pod firewall supports public IPv4 HTTP/HTTPS and exact
IPv4 DNS resolver addresses. IPv6 and private/special destinations remain denied.
It rejects arbitrary additional egress rules and removed baseline exclusions.
It is a preparation-specific capability; it does not certify generic image hosting
or change the installation's other network boundaries. See
[operator setup and acceptance](workspace-preparation.md#operator-setup).

## Upgrade and rollback

Deploy the chart, orchestrator and controller as one coherent installation. The
chart keeps the VM controller single-writer with `Recreate`. New admission settings
must agree between orchestrator and controller. Disabling admission preserves
cleanup authority for already admitted work; it is not a deletion operation.

The manifest migration is a configuration-contract migration. Stored Experts,
Project generations and execution snapshots are authoritative after cutover.
Legacy endpoints can remain compatibility projections without making legacy
configuration another source of truth. Existing historical snapshots are preserved;
the system must not invent past resolved settings from today's defaults.

Database migrations are forward-only; this repository does not provide general
down migrations. A Helm rollback alone does not undo schema changes, restore
retained disks or make a pre-cutover orchestrator compatible with migrated data.
Before an upgrade, preserve the application database, encryption/lifecycle key
identities, relevant retained storage and the installed chart/image references.
Use the normal platform backup mechanism without putting secret material in
manifests, exported examples or test evidence.

A rollback target must read every stored resource, execution snapshot and durable
preparation record it may encounter. In particular, a controller predating Pod
firewall support must not adopt pending firewall-enabled preparations. Drain or
cancel those allocations with the newer controller and confirm writer retirement
before downgrading. Restoring a backup requires a coherent database/storage point
and reconciliation of the exact runtime identities; restoring only one side can
leave live writers or dangling references.

## Executable compatibility baseline

[The portable bundle](conformance/v1alpha1/portable.yaml) covers all five authored
kinds, Project references/defaults and private JSON values. Its
[frozen baseline](conformance/v1alpha1/baseline.json) was produced by published
`develop` revision `5809c97f549f45f226984da300ddda9850cb2174`, before the release
contract changes. `tests/test_manifest_compatibility.py` checks the candidate's
resolved output and JSON/YAML round trips against that baseline, plus safe
unsupported-version diagnostics. It requires no cluster, credentials or images.

Keep that fixture immutable when evolving this API. Add cases and an explicit
version/migration decision for a behavior change; do not regenerate the expected
answer merely to make a changed resolver pass. These parser/resolver checks are
one layer of acceptance. They do not replace admission/authorization tests, real
runtime tests, migration replay or a deployment-specific upgrade/rollback exercise.
