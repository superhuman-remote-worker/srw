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

That FUSE profile applies only to the installation workspace image and to
repositories listed in `workspace.images.trustedRepositories`. A WorkspaceTemplate
may name any other image. Such a custom image runs unprivileged: no `/dev/fuse`,
no `SYS_ADMIN` and seccomp `RuntimeDefault`. It therefore gets no cloud mount.
`workspace.customImages.privileged: true` gives custom images the full profile.
Enable it only if you trust everyone who can author workspace templates, because
the template's image then decides what runs as root in a privileged container.

Even unprivileged, a template image runs with the workspace owner's secrets, so
use images only from authors you trust. Container resources come only from the
selected template; callers can't set them through `config_override`. See
[container workspace templates](../examples/manifests/container-workspace-templates.md).

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
   workspace nodes when it is enabled. Keep `workspace.customImages.privileged`
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
