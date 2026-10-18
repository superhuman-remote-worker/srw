---
title: SRW documentation
description: Index of the public documentation for installing, using, operating and developing Superhuman Remote Worker.
---

# SRW documentation

Superhuman Remote Worker (SRW) is a self-hosted runtime for AI work. You
combine an **expert** (prompt, model and tools), a **workspace** (none,
virtual, a container pod or a VM) and **connectors** (repositories, databases,
credentials, knowledge bases), and SRW runs them on your Kubernetes cluster as
live sessions or autonomous jobs.

These pages are also published, with search, at
<https://docs.superhuman-remote-worker.com>. Each page here is the source of
the site; edit it on GitHub and the site follows.

## Start here

| If you want to… | Read… |
|---|---|
| Install SRW and run a first session and job | [Get started](getting-started.md) |
| Understand experts, workspaces, connectors, sessions and jobs | [Concepts](concepts.md) |
| Connect model providers | [Models](models.md) |
| Fix a failing installation | [Troubleshooting](troubleshooting.md) |

## Guides

- [Models](models.md) — choosing and connecting the models SRW runs on.
- [Local Kubernetes with k3d](local-kubernetes.md) — single-machine
  evaluation and development, smoke tests, lifecycle and troubleshooting.
- [Prepare a shared Linux host for SRW on K3s](k3s-host-preparation.md) —
  K3s on a server with other services, several interfaces or a separate
  volume disk.
- [Build your own connector driver](connector-drivers.md) — write, test,
  register and use a bind-time driver image.
- [Cloud folders](cloud-folders.md) — the main cloud's project folders, My
  cloud, outboxes and job copies.
- [Build your own workspace image](workspace-images.md) — add your tools and
  run jobs and sessions on them.
- [SSH access](../ssh-access.md) — connect a terminal or IDE to a live session
  workspace.
- [VM capacity diagnostics](vm-capacity.md) — resource inventory, retained
  reservations and unavailable capacity.
- [VM workspace networking and recovery](vm-workspace-recovery.md) — qualify
  an immutable image, enable bounded recovery, interpret retained holds.
- [Project loops](project-loops.md) — bounded, repeated job sequences against
  one project goal.

## Operations

- [Versioning and upgrades](versioning.md) — release versions and how to
  upgrade an installation.
- [Security model](security-model.md) — trust assumptions, isolation choices,
  limitations and deployment guidance.
- [Troubleshooting](troubleshooting.md) — symptoms, causes and fixes.

## Reference

- [Architecture](architecture.md) — control plane, runtimes, workspaces,
  state, and job and session lifecycles.
- [Helm values](https://docs.superhuman-remote-worker.com/reference/helm-values/)
  — every chart value with type, default and description, generated from
  [`helm/values.yaml`](../helm/values.yaml) and
  [`helm/values.schema.json`](../helm/values.schema.json).
- [Helm chart guide](../helm/README.md) — production installation, secrets,
  external services, HA databases, upgrades and VM workspaces.
- [Expert configuration](../config/README.md) — expert roles, overlays,
  prompts, models and tools.

## Development

- [Development guide](development.md) — the inner loop against a local
  cluster, tests and linters.
- [Contributing guide](../CONTRIBUTING.md) — how to submit a change.

## Writing these docs

Every page starts with YAML frontmatter (`title` and a one-sentence
`description`) followed by a `# Title` heading with the same text, so it reads
well on GitHub and on the site. Link to other pages by file name
(`[Models](models.md)`) and to anything else in the repository by relative
path (`../helm/README.md`); the site turns the first into its own pages and the
second into GitHub links. The site itself lives in
[`docs-site/`](../docs-site); the [Development guide](development.md#documentation-site)
shows how to build and preview it.

If the documentation and observed behaviour disagree, treat that as a bug and
open an issue with the release tag, the deployment shape and the smallest
useful reproduction.
