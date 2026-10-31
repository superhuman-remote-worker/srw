---
title: Get started
description: Install SRW with the generated installer on a fresh k3s node or an existing cluster, or locally on k3d, then run your first session and job.
---

# Get started

SRW installs as one Helm chart. There are two ways to get a running system:

- **The generated installer** (recommended). A form on the SRW website writes
  your Helm values, Secrets, an `install.sh` and a runbook. It works on a fresh
  Linux machine running k3s or on a Kubernetes cluster you already have.
- **Local k3d from a clone**, for development and for evaluating changes to
  SRW itself on a Linux workstation.

Both end in the same place: Cockpit, SRW's web app, where you add models and
start work. That part is described at the end of this page.

## Install with the generated installer

Open <https://superhuman-remote-worker.com/configure>. The page runs in your
browser and your answers stay there. It asks where SRW runs, which setup you
want, how people reach it, who administers it and how you will use AI models.
Then it shows a runbook and offers **Download installation ZIP**.

### Where it runs

- **New single node (k3s).** One Linux machine becomes a single-node cluster.
  k3s brings Traefik as the ingress controller and `local-path` storage, so the
  generated values use ReadWriteOnce workspace claims. The runbook gives the
  commands to install k3s and Helm. If the server also runs other services,
  has several network interfaces, or must keep volumes on a separate disk,
  follow [Prepare a shared Linux host for SRW on K3s](k3s-host-preparation.md)
  first.
- **Existing Kubernetes cluster.** You name the ingress class (`traefik` or
  `nginx`) and optionally a storage class. Leave **My storage class supports
  ReadWriteMany** ticked only if it does (Longhorn, CephFS, NFS and most cloud
  file stores do); untick it for block-only storage.

### Choose a profile

| Profile | Use it for | What it sets up |
|---|---|---|
| **Evaluation** — everything bundled | Trying SRW on a machine only you can reach | Postgres, identity, Git, cloud storage and search in one release, with every generated password in `values.yaml`. Agent pods scale to zero when idle. |
| **Production** — your Secrets, your services | An installation people rely on | Credentials in a separate Secret you apply. Each bundled service (Postgres, identity provider, Git, cloud storage) can be swapped for one you already run. Optional **HA databases with CloudNativePG**: two instances per database with automatic failover and a second orchestrator replica. |
| **Production + VMs** — full virtual machines per job | Work that needs its own kernel or root | Everything in Production, plus KubeVirt workspaces with gated `sudo`. Needs nodes with hardware virtualization. |

### What you download

The ZIP contains:

- `values.yaml` — the Helm values for your answers.
- `srw-secrets.yaml` — Production profiles only: the Secret with every
  generated credential. Replace each `CHANGE_ME` with a credential for
  something you run yourself (for example an external database); `install.sh`
  refuses to start while one is left. Store the file in your secret manager
  afterwards, not in Git.
- `install.sh` — everything after "kubectl can reach the cluster".
- `runbook.md` — the same steps the page shows, from preparing the node to the
  first login.

### Requirements

- **A single k3s node:** at least 8 vCPU and 16 GiB RAM for the platform
  itself. Each concurrently running agent pod needs roughly another 1 vCPU and
  1 GiB (the form sets the maximum number of workers). With VMs, add the guest
  memory of each VM.
- **An existing cluster:** Kubernetes 1.28 or newer, an ingress controller, a
  default StorageClass, and a CNI that enforces NetworkPolicy (Calico, Cilium,
  Antrea, OVN, or k3s with kube-router; Flannel alone ignores policies). Budget
  the same 8 vCPU and 16 GiB for the namespace.
- **On the machine you install from:** Bash, `kubectl`, Helm 3.12 or newer,
  `ssh-keygen` and `base64`.
- **DNS and network:** with a domain, point a wildcard record `*.<domain>` and
  `<domain>` itself at the ingress. SRW serves Cockpit on `<domain>` and uses
  `api.`, `mcp.` and, for bundled services, `auth.`, `git.` and `cloud.`
  subdomains. Ports 80 and 443 must reach the ingress; for Let's Encrypt,
  port 80 must be reachable from the internet. Without a domain, choose
  **Self-signed HTTPS through proxy** and reach SRW on one IP address or
  `localhost`.

### Run it

Extract the ZIP and run the script from its directory with `kubectl` pointed at
the cluster:

```bash
chmod +x install.sh
./install.sh
```

The script checks its tools and the cluster, installs cert-manager,
CloudNativePG or KubeVirt only when your choices need them and they are
missing, applies the Secrets, then runs `helm upgrade --install … --wait
--timeout 20m` and waits for the orchestrator and Cockpit. The orchestrator
starts last because it waits for the databases, identity, Git and migrations;
ten minutes on a first install is normal. The script is idempotent: if a step
fails, fix the cause and run it again. See
[Troubleshooting](troubleshooting.md) for the common failures.

When it finishes, it prints the Cockpit address and the login: your admin
username with the `KC_REALM_ADMIN_PASSWORD` from `values.yaml` (Evaluation) or
`srw-secrets.yaml` (Production). It also prints the command to back up
`APP_ENCRYPTION_KEY`. Do that now: every stored API key is encrypted with it
and is unrecoverable without it.

The [Helm chart guide](../helm/README.md) covers the same installation by hand,
every value it sets, upgrades and VM workspaces.

### Models

SRW needs models for four capabilities before it accepts jobs and sessions:
**chat**, **embedding** (4096-dimensional vectors), **auxiliary** (memory
extraction, curation and titles) and **rerank** (memory recall ordering). One
OpenRouter API key can supply all four. [Models](models.md) lists suitable
models and other providers.

You add models in Cockpit after the first login (below). The form's **Use AI
subscriptions** option instead installs a proxy that lets you connect a
supported subscription account under **Settings → AI Subscriptions**; a
subscription may not supply every capability, so you may still need a
provider for the rest.

## Run SRW locally with k3d

To develop SRW, or to evaluate a change before it is released, run the chart
from a clone on a single-node k3d cluster on a Linux workstation (8 vCPU and
16 GiB RAM for the cluster). The values file there contains published
development credentials; never expose that installation to other people.

```bash
git clone https://github.com/superhuman-remote-worker/srw.git
cd srw
cp deployment/values-local.yaml.example deployment/values-local.yaml
```

[Local Kubernetes with k3d](local-kubernetes.md) walks through the
prerequisites, the bootstrap script, the Helm install and a smoke test. The
[Development guide](development.md) adds the fast edit loop with Tilt.

## First steps in Cockpit

Open the Cockpit address and sign in. You land on an empty chat.

### 1. Add models

As long as setup is incomplete, a **System not ready.** banner lists what is
missing, and creating a job or session fails with HTTP 503.

1. Open the account menu at the bottom of the left rail and choose
   **Settings**. Administrators see an **Administration** group; choose
   **Models**.
2. On the **Providers** tab, add your API key under **Provider API Keys** (for
   example OpenRouter), or register an OpenAI-compatible server under **System
   Endpoints**.
3. On the **Catalog** tab, add a model for each capability: pick the provider,
   tick its **Capabilities**, enter the **Model ID** and choose **Add model**.
   For a system endpoint, **Discover available models** lists what it serves.
4. On the **Defaults** tab, check the default for chat, embedding, auxiliary
   and rerank. SRW pins the first model of a capability as its default
   automatically when none is set.

The banner disappears when every required capability has a model and a
default. Installs made with a model already seeded may skip this step.

### 2. Start a session

A session is a live conversation with an agent. Type into the empty chat (or
choose **New chat** in the rail) and send: the session starts when you send
the first message. Cockpit shows **Starting session** while it creates the
thread, provisions the agent and connects. For more control, choose
**Advanced options** to pick the **Expert**, **Workspace** and **Connectors**
before you start.

### 3. Create a job

A job is autonomous work that runs without you watching. Open **Jobs** and
choose **New Job**. **Description** is the only required field: describe what
the agent should accomplish, optionally pick a **Project**, an expert and
documents, then choose **Create Job**.

The job's status badge moves from **Created** (waiting for a workspace and an
agent) to **Processing**. With the default **Review** autonomy, the agent
stops at **Pending Review** when it believes the work is done: open
**Review** on the jobs page, inspect the result and choose **Approve** to mark
it **Completed**. To let a job complete on its own, set **Autonomy** to
**Full** in the job's execution settings (your administrator may need to allow
that level for you). [Concepts](concepts.md) explains statuses and autonomy
levels.
