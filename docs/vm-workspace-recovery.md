# VM workspace networking and recovery

Ordinary workspace recovery can reconnect a held stateless VM Job to its existing
runtime. It requires fresh authenticated runtime and guest-network evidence;
it does not assume that a Running VM is usable. Unknown command outcomes require
attention rather than automatic replay.

Both automatic recovery and the clean-image network profile ship disabled.
Replacement recovery and idle release have separate controls and qualification
requirements. Enabling ordinary recovery does not enable either feature.

## Qualify the image before enabling it

The network profile applies only to a new, clean, single-interface disk whose
immutable `repository@sha256:digest` reference appears in the operator allowlist.
It uses NoCloud DHCP configuration matched by interface name (`enp1s0`).
A mutable tag does not enroll a disk, even if it currently resolves to an
allowlisted image.

Before allowlisting an image, verify pinned SSH, actual DHCP address and route,
DNS, and exact retained-file contents across a graceful retained-disk restart
with a changed VM/VMI and MAC. Keep the disk's native identity and first-boot
profile provenance in the qualification evidence. A rendered manifest or a
Ready condition alone is insufficient.

Existing unprofiled and prepared disks retain their original contract. Turning
on enrollment does not upgrade them or supply missing provenance. A proven
ordinary retained Job Resume keeps an absent profile absent; it is not refused
solely because enrollment is now enabled. Missing, mixed or invalid provenance
still refuses profile-dependent operations. Prepared images need their own
qualification and cannot inherit the clean base image's profile.

## Enable a qualified path

Use the exact verified image reference in both the allowlist and the new
workspace request. For example, after substituting the verified digest:

```yaml
vmController:
  networkProfile:
    enabled: true
    imageAllowlist:
      - ghcr.io/superhuman-remote-worker/srw-agent-vm-base@sha256:<verified-digest>
orchestrator:
  vmWorkspaceRecovery:
    enabled: true
    replacementEnabled: false
```

The allowlist does not change `vmController.defaultVmImage`, existing workspace
requests, or preparation selection. Those paths must independently select a
qualified clean image to receive this profile.

Roll out hold-aware Core, Controller and Worker versions before enabling
automation, and replace old Worker processes so their environment agrees with
the new settings. Verify actual Pods and their effective settings after the
rollout. Keep `vmWorkspaceRecoveryAcceptanceGate` and
`vmRetainedResumeAcceptanceGate` disabled on shared installations; they are
exclusively disposable-cluster test adapters.

## Recovery outcomes and limits

Protocol v1 requires a 900-second budget from the first observation. It is
persisted and survives service restart. Retry or probe activity does not extend
that operation's deadline. Global probe concurrency defaults to four and is configurable. One probe per
known node is fixed; unknown-node probes are serialized. Durable claims are
shared by overlapping reconcilers.

A successful same-runtime recovery rechecks the exact runtime, pinned SSH
identity, guest network and safe continuation before releasing the held queue.
A pre-bundle refusal can refund the attempt only when execution was never
authorized. Once an outcome is uncertain, the system keeps a recoverable hold
instead of claiming success or blindly rerunning a command. Expiry reaches an
attention state and retains the workspace; restored connectivity by itself does
not authorize a new execution after that pause.

Use the normal recovery controls or Cancel after inspection. Cleanup must settle
under its own exact authority before reservations are released. Retaining a disk
does not itself prove an S3 backup or a restorable checkpoint. See
[VM capacity diagnostics](vm-capacity.md) for held reservations and unavailable
capacity.

Container workspaces use separate recovery adapters. For example, a failed
Worker transport with a healthy Core-side workspace probe can retain the existing
container under an operator attention hold with a done, unleased queue. Explicit
Resume may continue the supported path. VM recovery flags do not qualify
container recovery or imply identical queue states across backends.
