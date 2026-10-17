# Security policy

## Supported versions

Superhuman Remote Worker is under active development. Security reports are
evaluated against the current `main` branch and the latest published tag. If a
report affects an older release, maintainers may ask the reporter to confirm
whether the issue is still present in a current version.

Deployments should use matching, immutable chart and component versions rather
than a moving development branch. Operators remain responsible for Kubernetes,
container-runtime, hypervisor, identity-provider, database, and bundled-service
updates.

## Supported deployment model (public beta)

SRW is a public beta. The deployment model it supports is:

- **One organisation per deployment.** SRW is single-tenant. Every person with
  an approved account is a member of the same organisation, and an
  administrator approves each account before it can use SRW.
- **Users who trust one another.** SRW keeps users' sessions, jobs and
  projects apart. That separation has not yet been hardened or verified
  against a hostile user of the same deployment. Don't rely on it to protect
  one user's data or credentials from another.
- **Administrators are fully trusted.** An administrator manages every user,
  project and setting of the deployment.
- **Disposable data first.** SRW runs model-generated code. Begin with data,
  repositories and credentials you could afford to lose, under supervised or
  review-gated workflows.

Do not offer one deployment to several customers or to users who don't trust
one another. If you need that, run a separate deployment for each.

## Report a vulnerability

Do not open a public issue for a suspected vulnerability.

Use
[GitHub Private Vulnerability Reporting](https://github.com/superhuman-remote-worker/srw/security/advisories/new)
and include:

- a description of the vulnerability and likely impact;
- the affected component and release or commit;
- deployment assumptions needed to reproduce it;
- reproduction steps or a proof of concept; and
- any known mitigation or evidence that the issue is being exploited.

You can expect an initial response within seven days. Please keep the report and
proof of concept private until maintainers have coordinated a fix and
disclosure.

## Scope

Security reports may include:

| Area | Examples |
|---|---|
| Authentication and authorization | OIDC/BFF bypass, role or grant bypass, IDOR, token confusion |
| Workspace isolation | Container or VM escape, harness/workspace boundary bypass, cross-job access |
| Agent execution | Prompt injection that crosses an enforced capability boundary, privileged-command gate bypass |
| APIs and realtime transport | Injection, cross-user WebSocket access, unsafe dispatch or lifecycle mutation |
| Data stores | Query injection, tenant-scope bypass, unauthorized knowledge or audit access |
| Secrets | Credential exposure in workspaces, logs, images, APIs, or rendered manifests |
| Supply chain | Dependency, image, build, release, or CI/CD compromise |

Ordinary model mistakes, incorrect answers, or prompt injection that causes only
actions already and intentionally authorized for that user are generally
product-safety issues rather than security-boundary vulnerabilities. Reports
that demonstrate a bypass of an enforced boundary are in scope.

During the beta, the boundaries SRW is expected to hold are:

- between the internet and the deployment: without an approved account or
  an SRW workload credential, nothing beyond signing in and the endpoints
  meant to be public, such as health checks;
- between other applications on the same identity provider and the API: a
  token Keycloak issues to another client (Gitea, Nextcloud, OpenCloud, or
  any client the operator has not listed in `auth.oidc`), and any ID token,
  is not an SRW credential; and
- between a workspace, including the model-generated code running in it, and
  the orchestrator, the agent harness and their credentials.

A report that an approved user can reach another user's or project's data is
still welcome, and is fixed as a bug. Under the beta's supported model,
though, it is not a breach of a guarantee SRW makes.

The following are normally out of scope:

- deployments outside the supported model above, such as one deployment
  shared by customers or users who don't trust one another;
- anything an administrator can do;
- vulnerabilities wholly inside an unmodified third-party service that should
  be reported to its upstream project;
- denial of service against a disposable local development deployment;
- social engineering without a product vulnerability; and
- reports produced only by automated scanners without a reproducible impact.

## Security architecture

SRW treats agent inputs, generated code, repositories, and shell-capable
workspaces as potentially hostile. The orchestrator and agent harness are kept
outside those workspaces, and the orchestrator remains authoritative for final
job state. Containers provide a weaker boundary than dedicated VMs, especially
when privileged FUSE support is enabled.

Read the public [security model](docs/security-model.md) for trust assumptions,
defense in depth, known limitations, and deployment guidance.
