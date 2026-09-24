# Superhuman Remote Worker documentation

This directory contains the stable, public documentation for running,
understanding, and contributing to SRW. Design drafts and deployment-specific
notes are intentionally not prerequisites for using the public repository.

## Start here

| If you want to… | Read… |
|---|---|
| Understand what SRW is | [Project README](../README.md) |
| Evaluate SRW on a Linux workstation | [Local Kubernetes with k3d](local-kubernetes.md) |
| Prepare K3s on a shared server with separate volume storage | [K3s host preparation](k3s-host-preparation.md) |
| Install on an existing cluster | [Helm chart guide](../helm/README.md) |
| Understand the runtime and data flow | [Architecture](architecture.md) |
| Evaluate the isolation boundary | [Security model](security-model.md) |
| Understand continuous project work | [Project loops](project-loops.md) |
| Develop and test SRW | [Development guide](development.md) |
| Submit a change | [Contributing guide](../CONTRIBUTING.md) |

## User and operator guides

- [Local Kubernetes with k3d](local-kubernetes.md) — single-machine evaluation,
  smoke tests, lifecycle, and troubleshooting.
- [Helm chart guide](../helm/README.md) — production-shaped installation,
  secrets, external services, HA databases, upgrades, and VM workspaces.
- [SSH access](../ssh-access.md) — connect a terminal or IDE to a live session
  workspace.
- [VM capacity diagnostics](vm-capacity.md) — interpret resource inventory,
  retained reservations and unavailable capacity.
- [Expert configuration](../config/README.md) — configure agent roles, overlays,
  prompts, models, and tools.

## Concepts

- [Architecture](architecture.md) — control plane, runtimes, workspaces, state,
  and job/session lifecycles.
- [Security model](security-model.md) — trust assumptions, isolation choices,
  limitations, and deployment guidance.
- [Project loops](project-loops.md) — how repeated agent workflows are bounded
  and what “self-improving” means in SRW.

## Further references

- [Helm chart](../helm/README.md)
- [Expert configuration](../config/README.md)
- [SSH access](../ssh-access.md)

If documentation and observed behavior disagree, treat that as a bug. Please
open an issue with the release tag, deployment shape, and the smallest useful
reproduction.
