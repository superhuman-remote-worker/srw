# Superhuman Remote Worker — Helm Chart

A self-improving AI agent system: Orchestrator (FastAPI) coordinates jobs,
Agents (LangGraph) execute them in isolated workspaces, Cockpit (Angular)
provides the web UI. Backed by PostgreSQL, pgvector, and (optionally)
Neo4j.

This chart deploys the full stack to a Kubernetes cluster. Internal databases
and supporting services can each be replaced with externally managed
equivalents (managed Postgres, an external OIDC provider, an existing git
server, etc.) — see [Production install](#production-install-bring-your-own).

- **Chart:** `oci://ghcr.io/superhuman-remote-worker/charts/superhuman-remote-worker`
- **Source:** <https://github.com/superhuman-remote-worker/srw>
- **License:** see [LICENSE](https://github.com/superhuman-remote-worker/srw/blob/main/LICENSE) — you must accept the terms to install (`license.acceptTerms: true`).

---

## What gets deployed

| Component | Purpose | Toggle |
|---|---|---|
| `orchestrator` | FastAPI control plane (REST API, job dispatch, MCP) | always on |
| `agent` | LangGraph job executor (worker + persistent session pools) | always on |
| `cockpit` | Angular web UI | always on |
| `mcp` | MCP server for Claude Code / AI platform integration | `mcp.enabled` |
| `workspace` | Per-job isolated PVC + SSH workspace pods | always on |
| `databases.postgres` | Application database (jobs, users, projects) | internal or external |
| `databases.vector` | pgvector for embeddings, citations, memories | internal or external |
| `databases.keycloak` | Dedicated Postgres for the bundled Keycloak (only relevant when `keycloak.internal: true`) | internal or external |
| `databases.audit` | Audit trail — LLM/agent/chat traces (optional but recommended) | internal or external |
| `databases.neo4j` | Project knowledge graph (optional). `edition: community` (default) or `enterprise` (set `acceptLicense` to `"yes"` for Startup Program / commercial, or `"eval"` for non-production). | internal or external |
| `keycloak` | OIDC provider | internal or external |
| `gitea` | Git server for agent code workspaces | internal or external |
| `opencloud` / `nextcloud` | Cloud storage backend | one or external |
| `pgadmin`, `dozzle` | Admin UIs (off by default) | `*.enabled` |
| `reloader` | Watches Secret/ConfigMap changes, triggers rolling restarts | `reloader.enabled` |
| `vmController` | KubeVirt VM lifecycle controller (HTTP or NATS transport) | `vmController.enabled` |

---

## Prerequisites

For a fresh K3s server, complete [host preparation](../docs/k3s-host-preparation.md)
first, especially on shared Fedora hosts or when volumes must use a separate disk.

- **Kubernetes** 1.28+ with at least 8 vCPU / 16 GiB RAM available for the namespace
- **Ingress controller** — `nginx`, `traefik`, or another. The chart defaults to `traefik` (override via `ingress.className`). The opt-in single-origin profile supplies its own namespaced gateway instead.
- **cert-manager** with a `ClusterIssuer` for multi-host TLS (set `ingress.tls.issuerName`). It is not required by the self-signed single-origin profile.
- **DNS** — wildcard or per-subdomain records pointing at your ingress LB for `*.<global.domain>` (`api`, `auth`, `git`, `cloud`, `mcp`). An IP-based single-origin profile needs no DNS.
- **Helm** 3.12+
- **Workspace SSH Secret** `srw-vm-ssh-key` with matching `ssh-privatekey` and
  `ssh-publickey`, including for container-only installations. The generator's
  `install.sh` creates or validates this before Helm; manual installs must supply it.
- **A `dockerconfigjson` pull secret** if you're pulling private GHCR images. Public images need no credentials.

Optional but recommended:
- **External Secrets Operator** + a backing store (Vault, AWS Secrets Manager, etc.) — see `externalSecrets.*`
- **An OIDC IdP** if you don't want the bundled Keycloak (Azure AD, Google Workspace, Okta, etc.)
- **Managed databases** for production (any standard Postgres 14+ for app + pgvector + audit, Neo4j 5+)
- **CloudNativePG 1.30+ on Kubernetes 1.29+**, only if a bundled database uses
  `databases.<name>.engine: cnpg|migrating`; add the **Barman Cloud plugin**
  only for object-store backups — see
  [Bundled databases on CloudNativePG](#bundled-databases-on-cloudnativepg).
  Neither is needed for the default StatefulSet engine.

---

## Bundled databases on CloudNativePG

Each bundled database carries its own `databases.<name>.engine`:

| value | renders | host helper points at |
|---|---|---|
| `statefulset` | the bundled single-replica StatefulSet | that Service |
| `migrating` | **both** — the Cluster imports from the legacy Service | the legacy Service |
| `cnpg` | the CloudNativePG `Cluster` only | `<name>-rw` |

`statefulset` is the default for every database. Together with
`databases.profile: single`, it is the supported non-HA posture and requires no
CNPG installation. The profile controls replica counts only; setting
`profile: ha` does **not** silently replace database engines.

### Default non-HA installation

No database override is required. These are the effective defaults:

```yaml
databases:
  profile: single
  postgres:
    engine: statefulset
  vector:
    engine: statefulset
  audit:
    engine: statefulset
  gitea:
    engine: statefulset
  keycloak:
    engine: statefulset
```

This keeps each enabled internal PostgreSQL database on one chart-owned
StatefulSet. It is appropriate for evaluation, local development, and
non-HA/self-hosted installations that accept single-node database availability.

### HA installation: install CNPG first

The SRW chart owns its namespaced `Cluster`, `DatabaseRole`, credential, backup,
and policy resources. It deliberately does **not** install, upgrade, or remove
the cluster-scoped CNPG operator and CRDs. One operator normally serves many
application namespaces and needs an independent lifecycle.

First check whether the cluster already has CNPG. Never install a second
cluster-wide operator:

```bash
kubectl get deployment --all-namespaces \
  -l app.kubernetes.io/name=cloudnative-pg
kubectl get crd clusters.postgresql.cnpg.io databaseroles.postgresql.cnpg.io
```

If an operator exists, record its namespace and verify its controller image is
CNPG 1.30 or newer. Automatic Dynamic Canvas role provisioning uses the 1.30
`DatabaseRole` API.

If none exists, install the pinned official operator as its own Helm release:

```bash
helm repo add cloudnative-pg https://cloudnative-pg.github.io/charts --force-update
helm upgrade --install cnpg cloudnative-pg/cloudnative-pg \
  --version 0.29.0 \
  --namespace cnpg-system \
  --create-namespace \
  --wait \
  --timeout 10m
```

Do not proceed merely because Helm returned successfully. Verify the
controller, its admission-webhook endpoint, and both CRDs used by this chart:

```bash
kubectl -n cnpg-system rollout status deployment/cnpg-cloudnative-pg \
  --timeout=5m
test -n "$(kubectl -n cnpg-system get endpointslice \
  -l kubernetes.io/service-name=cnpg-webhook-service \
  -o jsonpath='{.items[0].endpoints[0].addresses[0]}')"
kubectl wait --for=condition=Established \
  crd/clusters.postgresql.cnpg.io \
  crd/databaseroles.postgresql.cnpg.io \
  --timeout=2m
kubectl -n cnpg-system get deployment/cnpg-cloudnative-pg \
  -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
```

Then select CNPG explicitly in the SRW values. The operator namespace must
match the namespace verified above because database NetworkPolicies permit its
health and failover traffic by that exact label:

```yaml
databases:
  profile: ha
  operator:
    namespace: cnpg-system
  postgres:
    engine: cnpg
  vector:
    engine: cnpg
  audit:
    engine: cnpg
  gitea:
    engine: cnpg
  keycloak:
    engine: cnpg
```

Install SRW normally, then wait for every enabled internal cluster:

```bash
helm upgrade --install srw \
  oci://ghcr.io/superhuman-remote-worker/charts/superhuman-remote-worker \
  --version <chart-version> \
  --namespace srw \
  --create-namespace \
  --values my-values.yaml \
  --wait \
  --timeout 20m

kubectl -n srw get clusters.postgresql.cnpg.io
kubectl -n srw wait --for=condition=Ready \
  clusters.postgresql.cnpg.io --all --timeout=15m
```

For an existing StatefulSet installation, do not jump directly to `cnpg`.
Migrate one database at a time through `engine: migrating`, verify the imported
target, and only then cut over. CNPG/CRD upgrades likewise happen in the
separate operator release before an SRW chart version that needs the newer API.

The removed `databases.operator.install` key remains only as a schema tombstone:
an old stored value of `false` is accepted during upgrade, while `true` fails
before Helm changes anything. If a prior release actually installed the
operator subchart, do not bypass that failure with `--reset-values`; hand the
operator to a separate infrastructure release first or the upgrade could remove
the controller while retained database clusters are still running.

### The Barman Cloud plugin — the chart cannot install this

Backups (`databases.backup.method: objectstore`) additionally require the
[Barman Cloud plugin](https://github.com/cloudnative-pg/plugin-barman-cloud).
`barmanObjectStore` on the `Cluster` resource was deprecated in CloudNativePG
1.26 and is slated for removal in 1.31, so the plugin is the supported path.

**This chart cannot install it, and will not try.** The plugin ships as a raw
manifest with no Helm chart, and every namespaced object in it hardcodes the
operator's namespace (`cnpg-system`) — a release deployed into its own
namespace does not own that one. Install it yourself:

```bash
kubectl apply -f https://github.com/cloudnative-pg/plugin-barman-cloud/releases/download/v0.14.0/manifest.yaml
kubectl -n cnpg-system rollout status deployment barman-cloud
kubectl -n cnpg-system logs deploy/cnpg-cloudnative-pg | grep "Registered plugin"
```

That last line is the one that matters. A running plugin pod does not mean the
operator found it — discovery goes through the plugin's Service and its
cert-manager-issued mTLS secrets, and a plugin the operator has not registered
is invisible to every `Cluster`.

It in turn requires **cert-manager** (for that mTLS) and **CloudNativePG
>= 1.26**. If your operator is not in `cnpg-system`, re-namespace the manifest
before applying it.

`ObjectStore` itself is namespaced, so the chart does render that one alongside
your clusters — only the plugin Deployment is confined to the operator's
namespace.

### Backups are off by default, deliberately

`databases.backup.method` defaults to `none`. Pointing it at the chart's own
bundled Garage would be **worse than having no backups**, because it would look
like having them: Garage here is a single node on a single PVC in the same
cluster as the databases it would be backing up, so one node loss takes both.
`NOTES.txt` warns on install in both cases — no backups at all, and backups
aimed at this release's own object store.

---

## Quick start (evaluation, single command)

For a self-contained evaluation install with everything in-cluster (Postgres,
Neo4j, Keycloak, Gitea, OpenCloud all bundled):

```bash
helm install srw oci://ghcr.io/superhuman-remote-worker/charts/superhuman-remote-worker \
  --version <chart-version> \
  --namespace srw --create-namespace \
  --set license.acceptTerms=true \
  --set global.domain=srw.example.com \
  --set ingress.tls.issuerName=letsencrypt-prod \
  --set secrets.create=true
```

`secrets.create=true` makes the chart auto-generate `APP_ENCRYPTION_KEY`,
`MCP_INTERNAL_KEY`, and `IDE_CREDENTIAL_KEY`, and create internal credentials.
**This mode is for evaluation only** — see
[Secrets](#secrets) for production options.

After install, follow the printed `NOTES.txt` to back up the encryption key.

---

## Single-origin self-signed HTTPS

Keycloak uses a recreate rollout in this profile to prevent mixed old/new login
addresses from being cached during an address change. Identity configuration
upgrades briefly interrupt login. The default multi-host strategy is unchanged.


For an evaluation server that has a reachable IPv4 address but no domain or
certificate issuer, start from
`deployment/values-single-origin-server.yaml.example`. The browser-visible
contract is one origin such as `https://192.0.2.10:30443`:

```yaml
exposure:
  mode: single-origin
  singleOrigin:
    address: "192.0.2.10"
    publicPort: 30443
    service:
      nodePort: 30443
    tls:
      mode: self-signed
      validityDays: 365
```

`publicPort` is the address users open; `service.nodePort` is the Kubernetes
NodePort. Setting one does not create NAT or a load-balancer rule for the
other. The server example exposes NodePort 30443 directly. Local k3d instead
advertises `https://localhost:8443` and maps host port 8443 to NodePort 30443.
For operator-managed NAT or a load balancer, configure the external mapping
and keep `publicPort` equal to the port users actually see.

This preset requires bundled Keycloak, Gitea, and Nextcloud. It disables
OpenCloud, Neo4j Bolt TLS, the external SSH gateway, Collabora, and the Canvas
viewer. It creates no cert-manager resources or Traefik CRDs. The chart may
generate a new certificate during an upgrade; users then accept the replacement
warning at the same origin.

The preset also stages Nextcloud's internal signed installation-attestation
lane. This lets ordinary cloud effects bind the exact persistent Nextcloud
installation; it does not enable protected cloud mode. When `secrets.create`
is true, the chart creates and retains the lane's dedicated immutable Secret.
The standalone server example uses operator-owned secrets, so create its HMAC
root once before installing and preserve it with the Nextcloud data:

```bash
kubectl create namespace srw --dry-run=client -o yaml | kubectl apply -f -
kubectl -n srw create secret generic srw-protected-effect \
  --from-literal=NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY="$(openssl rand -hex 32)"
kubectl -n srw patch secret srw-protected-effect \
  --type=merge --patch '{"immutable":true}'
```

Do not rotate or recreate this Secret during an upgrade, and do not copy its
key into the application Secret consumed by agent Pods.

The normal `srw-secrets` application Secret must separately contain an
independently generated `IDE_CREDENTIAL_KEY` of at least 32 random bytes. The
browser IDE fails closed and withholds its URL when this key is absent.

The supported promise covers browser Cockpit/login/API/session WebSockets,
top-level browser IDE, Gitea, and Nextcloud. Chrome service workers are disabled
for this preset because click-through certificates do not make their script
fetch trusted. Standalone Git, WebDAV/desktop sync, MCP, SSH/JetBrains clients,
managed browsers that prohibit certificate exceptions, embedded IDE webviews,
and generated application previews need separate trust or exposure design.

Keep the normal application Secret, storage, image pins, database sizing, and
workspace prerequisites. The self-signed gateway only replaces public DNS and
certificate bootstrap.

### Returning a retained Nextcloud installation to multi-host

Nextcloud persists some reverse-proxy settings on its data volume. Switching
Helm values back to `multi-host` does not clear the old IP address and `/cloud`
prefix automatically. After applying the multi-host values and completing the
Nextcloud rollout, update its trusted domains and remove the old overrides
before opening the new cloud hostname. Keep the existing data volume and HMAC
Secret. Back up the Nextcloud configuration before changing it.

For the bundled Apache image, run `occ` as its configuration owner, `www-data`:

```bash
SRW_NAMESPACE=srw
SRW_NEXTCLOUD_DEPLOYMENT=srw-nextcloud # adjust to the rendered Deployment name
nextcloud_occ() {
  kubectl -n "$SRW_NAMESPACE" exec "deployment/$SRW_NEXTCLOUD_DEPLOYMENT" \
    -c nextcloud -- runuser -u www-data -- php /var/www/html/occ "$@"
}

# Replace these with the internal Service name and actual new cloud hostname.
# Include any additional trusted domains your installation needs.
nextcloud_occ config:system:set trusted_domains --type=json \
  --value='["srw-nextcloud","cloud.example.org"]'
for key in overwritehost overwritewebroot overwrite.cli.url; do
  nextcloud_occ config:system:delete "$key"
done

nextcloud_occ config:system:get trusted_domains
nextcloud_occ config:system:get trusted_proxies
nextcloud_occ config:system:get overwriteprotocol
```

The last checks should show the new domains, the configured multi-host proxy
ranges, and `https`. Confirm that the three deleted overrides are absent, then
check login and file access at the new cloud URL. The chart reconciles the OIDC
provider in both modes. Automatic reverse migration of these persisted settings
is outside this preset's scope; see the
[Nextcloud Docker reverse-proxy configuration guidance](https://github.com/nextcloud/docker#using-the-image-behind-a-reverse-proxy-and-specifying-the-server-host-and-protocol).

---

## Production install (bring your own)

For pilot and customer deployments, you bring your own external services and
manage your own secrets. Start by extracting the example values:

```bash
helm pull oci://ghcr.io/superhuman-remote-worker/charts/superhuman-remote-worker \
  --version <chart-version> --untar
cp superhuman-remote-worker/values.example.yaml my-values.yaml
$EDITOR my-values.yaml
```

Edit at minimum:
- `license.acceptTerms` → `true`
- `global.domain` → your base hostname
- `image.{orchestrator,agent,cockpit,mcp,workspace}.tag` → the matching
  `vX.Y.Z` release tag, or set each component's verified `digest`
- `secrets.existingSecret` → name of a Secret you create yourself (see below)
- `databases.*.externalUrl` → connection strings for managed Postgres, vector
- `keycloak.externalIssuerUrl` → your IdP issuer URL
- `gitea.internal: false` + `*.externalUrl` → your git server URLs
- `cloud.externalBackend` + `cloud.externalUrl` → your cloud storage endpoint
- `ingress.className` and `ingress.tls.issuerName` → your cluster's ingress + cert-manager issuer

The chart source keeps those five image tags at `latest` for the development
workflow, and selecting an OCI chart version alone does not pin them. Production
values must override every component tag or digest. If VM workspaces are
enabled, pin `vmController.image.tag` as well; the default VM base image already
derives its version from the released chart unless explicitly overridden.

### Per-component hostname overrides

By default every subservice ingress is `<subdomain>.<global.domain>` (`api`,
`auth`, `git`, `cloud`, `mcp`, `neo4j`, …). When one of those flat names is
already in use in the parent zone — e.g. you already run a Gitea at
`git.example.com` and want SRW's bundled Gitea on a separate host — override
just that hostname:

```yaml
global:
  domain: srw.example.com
  hostnames:
    git: git-srw.example.com   # takes precedence over the default git.srw.example.com
```

The override propagates to the Ingress host + TLS SAN, the `GITEA_URL`
ConfigMap entry, the cockpit deep-link, and the Keycloak gitea-client
redirect URI. TLS Secret names stay tied to the release name, so two
SRW installs in the same cluster never collide on cert storage. See
`global.hostnames` in `values.yaml` for the full key list (cockpit, api,
auth, git, cloud, mcp, neo4j, neo4jBolt, pgadmin, dozzle, headscale,
minio).

Pre-create the secret with all required keys (see [Secret schema](#secret-schema)):

```bash
kubectl create namespace srw
kubectl -n srw create secret generic srw-secrets \
  --from-env-file=./srw.env
```

Install:

```bash
helm install srw oci://ghcr.io/superhuman-remote-worker/charts/superhuman-remote-worker \
  --version <chart-version> \
  --namespace srw \
  -f my-values.yaml
```

---

## Secrets

The chart supports three mutually exclusive modes. Pick one.

### Mode 1 — External Secrets Operator (recommended for production)

Reads secrets from Vault (or another ESO-supported backend) and projects them
into a K8s Secret. Survives chart upgrades, rotation, and cluster rebuilds.

```yaml
externalSecrets:
  enabled: true
  refreshInterval: "1h"
  secretStoreRef: "vault-backend"
  secretStoreKind: "ClusterSecretStore"
  vaultPath: "secret/data/srw/srw-secrets"
  vmSshKeyVaultPath: "secret/data/srw/vm-ssh-key"  # optional, only if using VM workspace backend
```

The Vault payload at `vaultPath` must contain every key listed in
[Secret schema](#secret-schema) (lowercased — ESO uppercases them on
projection). Missing `app_encryption_key` will block orchestrator startup.

### Mode 2 — Pre-existing K8s Secret (typical for customer-managed installs)

You create the Secret out of band (Sealed Secrets, kubectl, your CD pipeline,
SOPS, whatever). The chart just references it.

```yaml
secrets:
  existingSecret: srw-secrets
```

### Mode 3 — Chart-created Secret (evaluation / dev only)

The chart generates `APP_ENCRYPTION_KEY`, `MCP_INTERNAL_KEY`, and
`IDE_CREDENTIAL_KEY` independently when absent, preserves them across upgrades
via `lookup`, and inlines any keys you provide in `secrets.values`.
**Do not use in production** — values end up
in `helm get values` output.

```yaml
secrets:
  create: true
  values:
    OPENAI_API_KEY: "sk-..."
    POSTGRES_PASSWORD: "..."
```

### Secret schema

These keys are referenced by chart templates. Not all are required for every
deployment — what you need depends on which optional components you enable.

**Always required:**
- `APP_ENCRYPTION_KEY` — base64-encoded 32-byte key. Encrypts user/system API
  keys and LLM endpoint credentials at rest. **If lost, all stored credentials
  become unrecoverable.** Back up immediately after install.
- `IDE_CREDENTIAL_KEY` — at least 32 random bytes. Derives a credential bound
  to each workspace runtime for the browser IDE. If absent, the orchestrator
  deliberately withholds every browser-IDE URL rather than starting an
  unauthenticated code-server. Chart-created mode generates and preserves it.

**Database credentials** — discrete user + password keys only. The chart
composes the DSN at runtime from these + ConfigMap-provided host/port/db,
so `/`, `@`, `=`, and `+` in passwords are URL-quoted automatically. Don't
ship a bundled `DATABASE_URL` / `VECTOR_DB_URL` Vault
key — both layouts coexist (the app falls back to the URL if user+password
aren't set), but the URL form is legacy and a footgun under `urlsplit`.
- `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `VECTOR_POSTGRES_USER`, `VECTOR_POSTGRES_PASSWORD` — pgvector has its
  own superuser password (separate from the main Postgres) so a credential
  leak on one instance doesn't compromise the other. Citations, embeddings,
  and memories all live in this instance (`srw_vector`); the citation engine
  is a native SRW subsystem on the vector pool, **not** a separate role or
  database. The former `srw_citations` / `citation_engine` database is retired.
- `NEO4J_USERNAME`, `NEO4J_PASSWORD` — both live in Vault (mirroring
  the `POSTGRES_USER` / `VECTOR_POSTGRES_USER`
  pattern, so all DB credentials sit in one place). Community edition
  expects `NEO4J_USERNAME=neo4j`; enterprise can use a different value.
  The Neo4j server image's `NEO4J_AUTH` is composed at pod-start from
  these two; don't ship `NEO4J_AUTH` as a separate Vault key — it's
  dead. **Don't include `/` in the password** — the Neo4j image splits
  `NEO4J_AUTH` on the first `/`, so a slash mis-parses server-side and
  the Bolt port comes up unauthenticated. URL-safe base64 (or any
  alphabet without `/`) avoids it.

For external-mode databases (`internal: false`), the host/port/db come
from `databases.<which>.externalHost/externalPort/externalDb` in values;
only the credentials live in Vault.

An enabled Dynamic Canvas viewer uses a separate PostgreSQL login and never
receives the application `POSTGRES_*` credential. For a chart-owned
`databases.postgres.engine=cnpg` database, set `provisionRole=true` and choose
exactly one credential source. `credentials.create=true` is the completely
self-contained path: the chart creates a gateway credential Secret plus a
dedicated CNPG basic-auth projection from the same generated password. CNPG
1.30's `DatabaseRole` reconciles the fixed login and password, and a tracked
revision-scoped Job applies and attests the database/object allowlist as the
ordinary application owner. This works for production and development and
requires no database administrator credential, pre-created Secret, or
pre-install role step.

`credentials.vaultPath` provides the same automatic path through ESO. Both
Kubernetes Secrets read the property named by `passwordKey` from that one Vault
entry; do not create or copy a second Vault value. The separate Kubernetes
objects are deliberate because Kubernetes does not permit an existing Opaque
Secret to be changed to `kubernetes.io/basic-auth` during an upgrade. The
gateway Secret keeps its existing type, while the CNPG-only Secret always has
the operator-required shape.

A pre-created `credentials.existingSecret` can also be used; with automatic
CNPG provisioning it must already be
`kubernetes.io/basic-auth`, contain matching `username`/`password`, carry
`cnpg.io/reload=true`, and set `existingSecretCnpgCompatible=true` as an
explicit offline-render assertion. The Vault-backed ExternalSecret is rendered
only while `viewer.enabled=true`.

External PostgreSQL remains operator-provisioned because the Helm release does
not own its control plane. The legacy bundled StatefulSet keeps its existing
development-only identity branch until that engine is retired. Keep the
gateway's database lifecycle, Secret rotation, and backup policy aligned with
the operator that owns the external database.

**OIDC / SSO** (when Keycloak or external IdP enabled):
- `KEYCLOAK_ADMIN_USER`, `KEYCLOAK_ADMIN_PASSWORD` (internal Keycloak only)
- `KC_DB_PASSWORD` (internal Keycloak only) — used as the Postgres superuser password on the dedicated `srw-keycloakdb` StatefulSet *and* as the connection password the Keycloak pod presents. When pointing the bundled Keycloak at an external Postgres (`databases.keycloak.internal: false`), the same value is sent over the wire — pre-provision a `keycloak` role with this password on your managed instance.
- `KC_REALM_ADMIN_PASSWORD` — password for the initial SRW administrator
  selected by `keycloak.bootstrapAdmin.username` (bundled Keycloak).
- `MCP_OIDC_CLIENT_SECRET` (if `mcp.enabled`)
- `GITEA_OIDC_CLIENT_SECRET`, `NEXTCLOUD_OIDC_CLIENT_SECRET`, `OPENCLOUD_KEYCLOAK_CLIENT_SECRET`, `PGADMIN_OIDC_CLIENT_SECRET` (per enabled component)

For a new installation with bundled Keycloak, choose the initial SRW login in
your values file:

```yaml
keycloak:
  bootstrapAdmin:
    username: admin
```

Use 1–64 ASCII letters, digits, dots, underscores, or hyphens, starting with a
letter or digit. The name must differ from enabled `keycloak.devUsers` accounts,
including case variants. Supply its password through `KC_REALM_ADMIN_PASSWORD`
in your chosen Secret source. Keycloak requires at least 16 characters and a
password different from the username. For bundled Keycloak 26.2 realm import,
exclude double quotes, backslashes, control characters, and `${` sequences from
this password: import substitutes environment variables before parsing JSON.
Other punctuation and Unicode are supported. This user receives the SRW `admin` role;
Keycloak's master administrator (`KEYCLOAK_ADMIN_USER`) and the Gitea service
administrator (`GITEA_ADMIN_USER`) keep their separate credentials.

An empty or omitted username retains the historical `test` login on fresh
installs. On connected Helm upgrades, the chart reads the existing realm
ConfigMap, preserves its initial username when the value is empty, and rejects
an explicit change. This setting seeds a new realm; it does not rename a user
in an existing Keycloak database. Keep the same explicit value in offline or
GitOps rendering, where Helm cannot read the existing ConfigMap. Create further
administrators through SRW user management. Password reconciliation on pod
restart continues to target the selected initial user.

**Git, cloud, admin credentials:**
- `GITEA_ADMIN_USER`, `GITEA_ADMIN_PASSWORD` (internal Gitea only)
- `NEXTCLOUD_ADMIN_USER`, `NEXTCLOUD_ADMIN_PASSWORD`, `NEXTCLOUD_AGENT_USER`, `NEXTCLOUD_AGENT_PASSWORD` (internal Nextcloud only)
- `NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY` when
  `nextcloud.protectedEffect.enabled=true`. Use at least 32 bytes from a
  dedicated random source. Chart-created mode generates and preserves it;
  ExternalSecret mode reads it only from
  `nextcloud.protectedEffect.hmacVaultPath`, while pre-created Secret mode
  names a dedicated Secret with `hmacSecretName`.
- `CLOUD_SERVICE_USER`, `CLOUD_SERVICE_PASSWORD` (the agent's account on whichever cloud backend is active)

### Protected Nextcloud effect lane

`agent.protectedCloudModeEnabled=true` with bundled Nextcloud requires
`nextcloud.protectedEffect.enabled=true`. The chart then adds an internal-only
Nginx Service and a dedicated Nextcloud PHP-FPM pool. The pool authenticates an
exact attempt-scoped request and checks its absolute deadline in an
`auto_prepend_file` before Nextcloud starts; FPM's
`request_terminate_timeout` supplies the hard wall-clock handler bound. The
ordinary Nextcloud Service remains the read and cleanup path.

The chart derives one non-secret configuration digest from all timing values
and the exact verifier/FPM/Nginx files, and supplies that digest to both the
server capability and the orchestrator's retained backend authority. Do not
manually set the URL or digest in Helm. Do not rotate
`NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY` while any `cloud_ro_effect_intents` row is
retained; drain/reconcile those attempts first and keep the old key resolvable
for historical cleanup. Never place this key in the main application Vault
path or Secret: dynamic agent Pods consume that shared bundle through
`envFrom`, while only the orchestrator and bounded FPM pool may sign effects.
The chart-created dedicated Secret is immutable and ESO uses CreatedOnce
refresh semantics. External Nextcloud is fail-closed for protected mode:
client or reverse-proxy timeouts are not an equivalent causal fence, so an
external installation needs the same server-enforced capability/deadline/FPM
contract before it can be admitted.

The bundled Nextcloud Deployment uses the `Recreate` strategy because every
container in the Pod shares one data PVC. Upgrades therefore include a brief,
deliberate Nextcloud restart instead of risking a cross-node ReadWriteOnce
multi-attach deadlock.

### Self-hosted web research

The chart deploys SearXNG by default (`searxng.enabled=true`) and seeds its
keyless in-cluster Service as the primary search provider on a fresh install.
If a Tavily key or an admin-selected search default already exists, SearXNG is
seeded into the empty fallback slot instead. Both catalog and default writes
are insert-only; later admin changes are not repaired or overwritten at boot.

Crawl4AI is available with `crawl4ai.enabled=true`, but remains off by default
because its browser service has a 4 GiB memory limit. Before enabling it, add a
strong `CRAWL4AI_API_TOKEN` to the Secret selected by
`crawl4ai.apiTokenSecret`; the chart uses the same high-entropy value as
Crawl4AI's stable JWT signing key.

Enabling it also registers it: the same post-install hook seeds a `crawl4ai`
catalog row (adapter `crawl4ai`, operations `extract` and `crawl`, `api_key`
carrying the bearer credential) and fills the **fetch** slot when that slot is
empty. SearXNG cannot serve `fetch` at all, so on a keyless install this is
what gives experts a provider-backed way to read the pages they find. A keyed
provider that already holds `fetch` (Tavily, Firecrawl) keeps it — there is no
fetch fallback slot — and Crawl4AI stays in the catalog for an admin to select
under Admin → Providers. If the token key is missing the hook logs a warning
and registers nothing rather than failing the release. Like the SearXNG and
Tavily seeds, catalog and default writes are insert-only: later admin changes
are never repaired or overwritten at boot.

Both workloads always render an egress NetworkPolicy. They may resolve DNS and
reach public HTTP(S), but RFC1918, cluster/service, link-local/metadata, and
loopback ranges are excluded. There is no value that deploys either workload
without this policy.

**LLM provider keys** (any combination, depending on which providers you use):
- `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GROQ_API_KEY`, `GOOGLE_API_KEY`, `OPENROUTER_API_KEY`, `TAVILY_API_KEY`, `SEMANTIC_SCHOLAR_API_KEY`, `UNPAYWALL_EMAIL`

**Optional integrations:**
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM` (when `email.enabled`)
- `IMAP_HOST`, `IMAP_PORT`, `IMAP_USER`, `IMAP_PASSWORD` (when email polling enabled)
- `NTFY_URL`, `NTFY_TOPIC`, `NTFY_TOKEN` (push notifications)
- `DISCORD_WEBHOOK_URL`, `SLACK_WEBHOOK_URL` (chat notifications)
- `TAILSCALE_AUTH_KEY` (when `agent.tailscale.enabled`)
- `CODEX_MANAGEMENT_KEY` (when `codexProxy.enabled`) — the AI Subscriptions
  proxy's management credential. The Secret key, the Service name and the auth
  PVC keep their historical `codex` names on purpose: the surface was renamed
  to "Subscription proxy" without renaming storage, so an upgrade needs no
  credential migration. Optionally set `codexProxy.inferenceApiKeySecret.name`
  to supply a separate *inference* credential.
- `CRAWL4AI_API_TOKEN` (required when `crawl4ai.enabled`; use a long random
  bearer token)
- `MCP_INTERNAL_KEY` (when `mcp.enabled` or delegated Dynamic Canvas tools are
  enabled). External-Secret and pre-existing-Secret deployments must provide
  this independently generated shared secret. If it is absent, the
  orchestrator remains available, but persistent agents withhold the Canvas
  tools and the `present-with-canvas` companion skill rather than advertising
  a broken capability; an enabled MCP pod also requires the key to start.
  Chart-created mode generates and preserves it automatically.
- `DEFAULT_DS_WEBDAV_*` (auto-configure a default WebDAV datasource for new users)

A skeleton `srw.env` to feed into `kubectl create secret generic ... --from-env-file=`:

```env
APP_ENCRYPTION_KEY=<base64-encoded 32-byte key>
MCP_INTERNAL_KEY=<independently-generated random shared secret>
IDE_CREDENTIAL_KEY=<independently-generated random workspace credential root>
POSTGRES_USER=srw
POSTGRES_PASSWORD=changeme
VECTOR_POSTGRES_USER=srw
VECTOR_POSTGRES_PASSWORD=changeme
KC_REALM_ADMIN_PASSWORD=changeme
OPENAI_API_KEY=sk-...
CLOUD_SERVICE_USER=agent
CLOUD_SERVICE_PASSWORD=changeme
```

Generate the independent keys with:

```bash
openssl rand -base64 32  # APP_ENCRYPTION_KEY
openssl rand -base64 48  # MCP_INTERNAL_KEY
openssl rand -base64 48  # IDE_CREDENTIAL_KEY
```

---

## VM workspaces on your cluster

Agent workspaces run as containers by default. Enabling the VM tier gives an agent a full
virtual machine instead — its own kernel, `systemd`, `sudo` (gated by a human approval in
the cockpit) and a root disk that survives a crash or a suspend. In this mode the VMs run on
**the same cluster as the rest of the stack**, managed by KubeVirt; the chart deploys a
small controller and the rest of the platform talks to the VMs over the pod network.

What you get, honestly stated: VMs are isolated from each other and from the control plane by
a hypervisor boundary, the workspace NetworkPolicy, and a node taint you apply — not by a
separate Kubernetes control plane. That is a stronger boundary than the container tier, and
the right trade for a single box or a small cluster. If you run untrusted tenants and need a
separate control plane for the VMs, that is the cross-cluster topology (`vm.mode: external`,
NATS + Headscale), which is not covered here.

### Prerequisites

The chart does **not** install KubeVirt or CDI — they are cluster-scoped operators with
their own lifecycle. Install them first; the chart's pre-install hook refuses to proceed
while the CRDs are missing and prints a pointer to this section.

**Hardware.** Every node that will run VMs needs hardware virtualization:

```bash
egrep -c '(vmx|svm)' /proc/cpuinfo     # > 0
test -c /dev/kvm && echo kvm-ok
virt-host-validate qemu                # optional, needs libvirt-client
lsmod | grep -E 'kvm|vhost_net'        # kvm_intel|kvm_amd and vhost_net loaded
```

If the node is itself a VM, enable nested virtualization on its hypervisor (KVM: `nested=1`
on `kvm_intel`/`kvm_amd`; Proxmox: `qm set <id> --cpu host`; Hyper-V:
`Set-VMProcessor -VMName <vm> -ExposeVirtualizationExtensions $true`; vSphere: "Expose
hardware assisted virtualization to the guest OS"). VirtualBox cannot. Without KVM, KubeVirt
can fall back to software emulation (`useEmulation: true`), which is fine for a smoke test
and useless for the real agent image.

**Sizing.** Budget ~2 GiB for KubeVirt's control plane (with `infra.replicas: 1`) plus,
per VM, the guest memory and about `guest/512 + 240 MiB + 8 MiB × vCPU` of launcher
overhead — a 4 vCPU / 8 GiB VM requests roughly 8.3 GiB. Disk: one 20 GiB golden image per
image digest plus 20 GiB per VM (root disks are full copies on `local-path`). The chart's
defaults (`vmController.defaultCpu: 4`, `defaultMemory: 8Gi`, `maxConcurrentVms: 4`) fit a
64 GiB node; 16 GiB total is the practical floor for the platform plus one small VM.

**Kubernetes version and the KubeVirt line.** KubeVirt supports the three newest Kubernetes
minors at each release. Pick the line that covers your cluster:

| Kubernetes | KubeVirt | CDI |
|---|---|---|
| 1.34 and newer | v1.9.x | v1.66.0 |
| 1.33 | v1.8.x (e.g. v1.8.4) | v1.66.0 |
| 1.31 / 1.32 | v1.6.x (e.g. v1.6.6) | v1.66.0 |

**Install KubeVirt and CDI.**

```bash
export KUBEVIRT_VERSION=v1.8.4   # see the table
export CDI_VERSION=v1.66.0
kubectl apply -f https://github.com/kubevirt/kubevirt/releases/download/${KUBEVIRT_VERSION}/kubevirt-operator.yaml
kubectl apply -f https://github.com/kubevirt/kubevirt/releases/download/${KUBEVIRT_VERSION}/kubevirt-cr.yaml
# single node: one replica of each control-plane component is enough
kubectl -n kubevirt patch kubevirt kubevirt --type merge -p '{"spec":{"infra":{"replicas":1}}}'
kubectl -n kubevirt wait kv kubevirt --for condition=Available --timeout=10m

kubectl apply --server-side -f https://github.com/kubevirt/containerized-data-importer/releases/download/${CDI_VERSION}/cdi-operator.yaml
kubectl apply --server-side -f https://github.com/kubevirt/containerized-data-importer/releases/download/${CDI_VERSION}/cdi-cr.yaml
kubectl patch cdi cdi --type merge -p '{"spec":{"config":{"featureGates":["HonorWaitForFirstConsumer"],"scratchSpaceStorageClass":"local-path"}}}'
kubectl wait cdi cdi --for condition=Available --timeout=5m
```

No KubeVirt feature gates are needed for importing, cloning and booting DataVolumes. On a
dedicated VM node, also tolerate your taint in the KubeVirt CR (`spec.workloads.nodePlacement`)
and the CDI CR (`spec.workload`), or the image import and the first bind of each root disk land
on another node and the VM never schedules.

`scripts/local-kubevirt-up.sh` does all of the above for the local k3d cluster (version
selection, KVM detection, the patches below, and a smoke test).

**Storage.** The VM root disks are CDI DataVolumes on `vmController.vmStorageClass`
(default `local-path`).

Every DataVolume the chart and the controller create names **both** its access modes and
`volumeMode: Filesystem` explicitly, and never lets CDI infer them from the target class's
StorageProfile. Keep doing that if you write your own, because inference is storage-dependent
and fails in opposite directions: `local-path` has no capabilities entry so there is nothing to
infer, while a real CSI usually *does* have one and resolves to `Block` — and a Block root disk
cannot be imported on a node running SELinux with the importer's capabilities dropped
(`blockdev: cannot open /dev/cdi-block-volume: Permission denied`). Because the chart names them,
no `kubectl patch storageprofile` is required on any class.

With `local-path`:

- clones are host-assisted full copies (no snapshots), and volumes carry a
  `kubernetes.io/hostname` affinity, so a VM can only mount its disk on the node that created
  it — **set `vmController.nodeSelector` on any multi-node cluster**, or a VM scheduled
  elsewhere will never bind its root disk.

With a CSI whose volumes attach on any node (Longhorn, Ceph, most cloud disks) that pinning is
**not** needed — it is a property of node-local storage, not of the VM tier. Leave
`vmController.nodeSelector` empty and let the scheduler place VMs, so VM and container workloads
draw on one capacity pool. KubeVirt decides per node whether VMs can run there at all: check
allocatable `devices.kubevirt.io/kvm`, **not** the `kubevirt.io/schedulable` label, which only
tracks whether virt-handler is healthy and is true even on nodes with no usable virtualisation.

Either way, set CDI's `scratchSpaceStorageClass` to the same class (done above). If your cluster
has several default StorageClasses, every VM-related object must name its class.

**Network.** The workspace NetworkPolicy must actually be enforced by your CNI. Calico,
Cilium, OVN-Kubernetes and Antrea enforce natively; **k3s** enforces through its embedded
kube-router controller (ingress and egress) unless you started it with
`--disable-network-policy`; Flannel alone does not.

### Chart values

```yaml
vm:
  mode: same-cluster
  lifecycleAuthSecretName: srw-vm-lifecycle-hmac   # see below
  preflight:
    enabled: true          # pre-install/upgrade hook: fails when the CRDs are missing

vmController:
  image:
    tag: <release or sha tag>
  defaultVmImage: ghcr.io/superhuman-remote-worker/srw-agent-vm-base:<tag>
  defaultCpu: 4
  defaultMemory: 8Gi
  maxConcurrentVms: 4
  vmStorageClass: local-path
  vmDiskSize: 20Gi
  nodeSelector: {}         # mandatory on multi-node clusters ONLY with node-local storage
                           # (e.g. local-path): {srw.io/vm-node: "true"}. Leave empty on a CSI.
  tolerations: []          #   and the matching toleration for your taint
  goldenImage:
    enabled: true          # import the base image once, clone per VM

agent:
  tailscale:
    enabled: false         # the mesh belongs to the cross-cluster topology
```

Two Secrets must exist in the release namespace:

- **`<release>-vm-ssh-key`** with `ssh-privatekey` and `ssh-publickey` — the key the
  platform uses to reach every workspace. The chart mounts the private half into the
  orchestrator and agents and injects the public half into each VM. `scripts/local-dev-up.sh`
  mints it locally; in production provide it via `secrets.existingVmSshKeySecret` or
  `externalSecrets.vmSshKeyVaultPath`. When `sshGateway.enabled`, this Secret must carry a
  third key, `user-ca.pub` — see [SSH gateway](#ssh-gateway) below.
- **`vm.lifecycleAuthSecretName`** with key `VM_LIFECYCLE_HMAC_SECRET` (≥ 32 random bytes,
  e.g. `python3 -c 'import secrets; print(secrets.token_hex(32))'`). The orchestrator and the
  controller share it to sign lifecycle requests, and the controller derives each VM's guest
  token from it. Keep it separate from the application Secret.

Pin `vmController.defaultVmImage` to a published tag. The default points at the tag that
matches the chart's `appVersion`, which exists only for released charts.

### Durable VM workspace recovery rollout

VM startup uses separate clocks for disk preparation, placement and guest boot.
`orchestrator.vmProvisioning.rootdiskStallTimeoutSeconds` defaults to `2700`
(`VM_ROOTDISK_STALL_TIMEOUT_S`). It measures active disk preparation without
forward progress; dependency/placement waits pause that clock. A stalled disk or
unverifiable phase produces a visible attention message and retains the disk.
The existing `VM_PROVISION_TIMEOUT_S` boot budget (default `600`) begins only
after the first authenticated observation of the exact guest VMI as Running.
Initialization keeps its separate deadline. Roll out the phase-aware controller
before the orchestrator; older controller responses cannot authorize a boot
timeout cleanup. Phase observations never bypass SSH or initialization checks.

`orchestrator.vmProvisioning.creationRetryEnabled` defaults to `false`
(`VM_CREATION_RETRY_ENABLED`). It admits new Job VM creates through durable
configuration resolution, fenced creation effects, and a separate SSH/init Ready
release. Existing operations keep reconciling when admission is disabled.
Migration `0260_vm_creation_ready_release.sql` and compatible controllers are
required. Keep this switch off until the source, attachment, cancellation and
live acceptance gates in the failed-creation retry plan have passed; the current
controller deliberately does not advertise the complete protocol capability.

Retained-disk restarts can opt into the closed single-NIC NoCloud DHCP profile
with `vmController.networkProfile.enabled: true` and an explicit
`vmController.networkProfile.imageAllowlist` of full `repository@sha256:<digest>`
image references. Both settings default to disabled/empty and are sent to the
controller and orchestrator as `VM_NETWORK_PROFILE_ENABLED` and
`VM_NETWORK_PROFILE_IMAGE_ALLOWLIST` (a comma-separated list). Only admit clean
images whose `enp1s0` network behavior has been verified. Prepared VM artifacts
need their own exact compatible provenance and are held by this initial policy.
New profile selection also requires same-cluster mode and
`orchestrator.vmProvisioning.creationRetryEnabled: true`; the legacy direct
create path cannot select it. The disposable recovery gate reads its explicit
image from `vmController.defaultVmImage`, which must be the exact allowlisted
digest when the profile is enabled.

A selected profile is frozen in creation intent and inherited by retries, idle
wakes and retained workspace handoffs even if new admission is later disabled.
Reuse also requires authenticated first-boot evidence of the effective
name-only DHCP rule and current guest network identity. Existing disks without
that immutable profile and proof remain warm at idle release and enter a visible
recoverable hold on retained successor admission, regardless of the fresh
admission switch;
the setting does not migrate or edit their guest configuration.

Durable recovery is installed reader-first. Its database records, job projection,
Retry/Cancel controls, disk retention pins and cleanup guards remain active even
when automatic recovery is disabled. The automatic reconciler and replacement
adoption are separate default-off gates:

```yaml
orchestrator:
  vmWorkspaceRecovery:
    enabled: false
    replacementEnabled: false
    deadlineSeconds: 900
    maxGlobalProbes: 4
    maxProbesPerNode: 1
    claimTtlSeconds: 30
    permitTtlSeconds: 30
    externalCallTimeoutSeconds: 10
```

Roll out schema and hold-aware server/cleanup code first. Next roll out workers
that understand the version-1 `hold_committed` receipt and drain every older
stateless worker that treats an arbitrary HTTP 409 as an ordinary failed attempt.
Verify the controller exposes version-1 signed recovery observations and durable
pin reconciliation before setting `enabled: true`. Do not downgrade to a server,
worker or controller that is unaware of unresolved recovery holds.

Keep `replacementEnabled: false` until the exact application, controller and
guest-image revisions pass the retained-disk gate. The gate requires a real
Kubernetes/KubeVirt/CDI/Longhorn substrate, exact stop receipts, pinned guest
identity, network qualification, and the same PVC marker/checkpoint after a
replacement. It creates and owns its disposable cluster; its default preflight
mode is read-only:

```bash
PYTHONPATH=src python scripts/vm-workspace-recovery-k3d-gate.py --preflight \
  --values-file /path/to/disposable-values.yaml \
  --guest-image registry.example/srw-vm@sha256:<digest> \
  --container-engine docker

PYTHONPATH=src python scripts/vm-workspace-recovery-k3d-gate.py --run \
  --cluster-name srw-vm-recovery-gate-$(date +%s) \
  --values-file /path/to/disposable-values.yaml \
  --guest-image registry.example/srw-vm@sha256:<digest> \
  --container-engine docker \
  --evidence-output ./vm-recovery-evidence.json
```

The default scenario driver and in-image adapter are source controlled. The gate
currently requires Docker because it compares platform-pruned archive config IDs
with containerd and deployed Pod image IDs; setting `CONTAINER_ENGINE=podman`
produces an explicit preflight skip. Missing KVM/vhost/tun, active iSCSI,
`longhornctl`, tools, values, or the immutable guest digest prints `SKIPPED`
before any cluster command. A created cluster must also pass Longhorn's node
preflight plus manager/CSI, node readiness, and a real RWO write/recreate/read
probe before the application scenario starts.
Successful command execution is insufficient: the gate prints `PASS` only after
it validates live substrate plus API/database evidence for response loss, leader
overlap, slow boot, deadline pause, forced deletion, missing stop evidence, and
marker/checkpoint/PVC survival. The cluster is deleted on success and failure;
`--keep-on-failure` is the explicit diagnostic exception.

The separate A1 retained-disk Resume adapter is disabled by default at
`orchestrator.vmRetainedResumeAcceptanceGate.enabled`. On an exclusively owned
disposable cluster, enable it together with same-cluster VM mode, durable
creation retries, the remote operation protocol, retained rootdisks, and the
network profile's explicit immutable image allowlist. Run
`scripts/vm-retained-resume-gate.py --help` for its exact context, cluster UID,
Job, dedicated owner (display name `A1 retained Resume gate <run-id>`), PVC,
provider Pod and private-output inputs. The wrapper installs a
run-owned temporary VM-count ResourceQuota only after the predecessor's
production cleanup settles; it removes that exact quota after the owner Resume
has joined a rejected VM effect. The application service account receives no
quota write permission. The adapter refuses any unrelated runnable worker
batch, so older queued work must be resolved through normal owner controls
before enabling the real worker pool. The provider holds `job_complete` until
the gate captures the actual worker and re-reads the retained sentinel over
pinned SSH; normal terminal cleanup may then remove the VM. Its local-path
result proves a retained
PVC and real worker flow; it is distinct from the Longhorn durability gate.

For a fresh isolated A1 run, `scripts/vm-retained-resume-fixture.py` creates the
dedicated paused Job through the ordinary PostgreSQL Job writer, freezes its
Expert/model and VM image in the execution snapshot, and calls the real VM
creation preflight. It waits for authenticated first-boot Ready, network-profile
proof, and the bound PVC/PV before returning the Job, owner, and PVC UIDs needed
by the acceptance wrapper. The fixture command requires the default-off gate
flag, an empty Job database, an owned namespace and provider model, and exact
cluster, orchestrator Pod, and image identities. It grants no worker lease.
It seeds or verifies the run-owned provider endpoint/model through normal
database writers, passing the provider inference key only through private stdin.
Keep the worker deployment at zero replicas until this fixture and the
acceptance wrapper's queue isolation checks have passed.

### Verify

```bash
kubectl get nodes -l kubevirt.io/schedulable=true
kubectl get pods -l app.kubernetes.io/component=vm-controller
```

## SSH gateway

`sshGateway.enabled` adds a component that lets a user `ssh s-<handle>@<sshGateway.hostname>`
straight into their session workspace. It runs the orchestrator image with a different command
(`uvicorn orchestrator.ssh_gateway:create_app --factory`), authenticates the user's own public key, and mints
a short-lived certificate for the inner hop to the workspace. It is off by default.

### Three Secrets, three places

The chart never generates key material. A template-time `genPrivateKey` guarded by `lookup`
silently returns empty under `helm template` and `--dry-run`, which would rotate the host key on
every Argo sync and break every user's `known_hosts`.

```bash
# Ed25519 ONLY. services/ssh_gateway_config._require_ed25519_host_key raises on any
# other algorithm at load_config, so an RSA host key means the gateway will not start:
# a server's advertised host-key algorithms come straight from the loaded key material,
# and an RSA key drags in legacy SHA-1 ssh-rsa.
ssh-keygen -t ed25519 -N "" -C srw-ssh-gateway -f ./ssh_host_ed25519_key
ssh-keygen -t ed25519 -N "" -C srw-user-ca      -f ./user-ca

kubectl -n <ns> create secret generic srw-ssh-gateway-hostkey \
  --from-file=ssh_host_ed25519_key --from-file=ssh_host_ed25519_key.pub
kubectl -n <ns> create secret generic srw-ssh-gateway-ca \
  --from-file=user-ca --from-file=user-ca.pub
```

1. **`sshGateway.hostKeySecret`** — one entry per name in `sshGateway.hostKeyNames`, and **both
   halves of each**. The private half is mounted into the gateway
   (`SSH_GATEWAY_HOST_KEYS`); the `.pub` half is mounted into the **orchestrator**, which is
   where `GET /api/ssh/host-keys` runs (`SSH_GATEWAY_PUBLIC_HOST_KEYS`). Both variables are
   rendered from that one `hostKeyNames` list, because when the served and published key sets
   drift a client sees a host-key mismatch indistinguishable from an active MITM. Omit the
   `.pub` halves and the orchestrator pod will not start — deliberately, because the
   alternative is publishing an empty key list forever while every client silently degrades to
   trust-on-first-use.
2. **`sshGateway.userCaSecret`** — key `user-ca`, the private CA half the gateway signs
   inner-hop certificates with.
3. **`user-ca.pub` inside the `vm-ssh-key` Secret.** This one is easy to miss and nothing else
   catches it. `container_provisioner` projects `user-ca.pub` out of the Secret named by
   `WORKSPACE_SSH_SECRET` (i.e. `vm-ssh-key`) into every workspace pod, where the entrypoint
   installs it as sshd's `TrustedUserCAKeys`. It does **not** travel through
   `sshGateway.userCaSecret`. Without it the pod starts fine (the projection is `optional`),
   the entrypoint skips the write, and every attach ends in `PermissionDenied` — fail-closed,
   but the whole feature inert.

   There are four ways that Secret gets filled, and the fix differs for each:

   | Supply path | What to do |
   |---|---|
   | `externalSecrets.vmSshKeyVaultPath` (layout A, `dataFrom: extract`) | the chart adds a `data:` entry alongside the bundle pull; put `SSH_GATEWAY_USER_CA_PUBLIC_KEY` in the bundle at `externalSecrets.vaultPath`. (ESO permits `data` next to `dataFrom`, and `data` wins on conflict.) There is no value that drops that entry: it renders whenever `sshGateway.enabled`, so a layout A bundle that already carries its own `user-ca.pub` must **still** have `SSH_GATEWAY_USER_CA_PUBLIC_KEY` at `externalSecrets.vaultPath` or ESO fails the whole `vm-ssh-key` sync. |
   | `externalSecrets.vaultPath` (layout B, the default) | the chart adds `secretKey: user-ca.pub`; put `SSH_GATEWAY_USER_CA_PUBLIC_KEY` in the same bundle. |
   | `secrets.existingVmSshKeySecret` | the chart renders no template here at all. Add the key yourself: `kubectl -n <ns> patch secret <name> -p "{\"data\":{\"user-ca.pub\":\"$(base64 -w0 user-ca.pub)\"}}"` |
   | `scripts/local-dev-up.sh` (k3d) | the script creates `srw-vm-ssh-key` with two keys only. Patch the third in with the same command before enabling the gateway. |

   Both ESO entries render **only** when `sshGateway.enabled`: ESO fails the whole
   ExternalSecret sync when a `data` entry names a property the bundle lacks, and an ungated
   entry would break `vm-ssh-key` — the key the platform reaches every workspace with — for
   every install that never asked for an ssh-gateway.

   Verify it by reading the projected file inside a running workspace pod, not by inspecting
   the template:

   ```bash
   kubectl -n <ns> exec deploy/<workspace-pod> -- cat /etc/ssh/srw_user_ca.pub
   ```

### Required values

| Value | Why it has no default |
|---|---|
| `allowedOrigins` | an empty list accepts cross-site WebSocket handshakes; `load_config` refuses to boot |
| `trustedProxies` | unset behind an ingress, every WSS client presents the *ingress's* address, so all of them share one source's 16-slot concurrency bucket and the seventeenth concurrent user is refused. Set it to cover **every** proxy hop in front of the gateway — the ingress *and* anything ahead of it (a Cloudflare Tunnel connector runs as an in-cluster pod and the ingress appends *its* address, not the client's); the gateway walks `X-Forwarded-For` right-to-left past trusted entries, so a CIDR covering the chain yields the real client at any depth. Use the literal `none` when nothing proxies the gateway. Both possible defaults are wrong |
| `hostKeySecret`, `userCaSecret` | operator-provided; see above |
| `sessionRouter.jwtSecret` **or** `sessionRouter.jwtSecretName` | `SESSION_JWT_SECRET` is the HMAC key the gateway verifies the attach token the orchestrator minted. With neither set, no Secret is rendered, the gateway's `secretKeyRef` (`optional: true`) resolves to nothing, and `load_config` refuses to boot — a crash-loop, not a render error. Set `jwtSecret` for the chart-rendered Secret (layout A) or `jwtSecretName` for one you own (layout B) |
| `tcp.allowedClientCIDRs` | required when `tcp.enabled`: an unscoped SSH LoadBalancer is not a supported default |
| `tcp.port` | has a default (2222) but is range-checked: the gateway runs as uid 999 with all capabilities dropped, so anything below 1024 never binds and `/healthz` answers 503 forever |

The chart `fail`s at render time on each of these rather than shipping a pod that crash-loops.

### Doors

`/api/ssh/attach` (the WSS transport) rides the existing API ingress at Traefik
`router.priority: 130`, as `pathType: Exact`. Exact is load-bearing, not tidiness: Traefik renders
`Prefix` as `PathPrefix()`, a raw string prefix, and the explicit priority beats the `/api` rule —
so `Prefix` here would route `POST /api/ssh/attach-token` to the gateway too and 404 every attempt
to mint a token. Both `/api/ssh/attach-token` and `/api/ssh/host-keys` stay on the orchestrator.
The raw TCP listener
always runs inside the pod on `tcp.port` (`/healthz` reports 503 while its accept loop is down)
and the ClusterIP Service always carries it, so a port-forward works with `tcp.enabled: false`;
`tcp.enabled` only adds the MetalLB LoadBalancer. Set `tcp.externalTrafficPolicy: Local` if you
want the NetworkPolicy's ipBlock rules and the per-source connection bucket to see real client
addresses — `Cluster` SNATs every client to a node IP and collapses them into one source.

### Behind a CDN or tunnel (Cloudflare Tunnel, etc.)

The Ingress above is only half the path when something other than Traefik terminates the public
hostname. Verified end to end on the dev deployment 2026-09-02; each of these cost a debugging round.

1. **The CDN must be told to route `/api/ssh/attach` to Traefik.** If it forwards `api.<domain>`
   straight to the orchestrator Service — a very normal setup, since every *other* `/api` route
   belongs there — then Traefik, and therefore this Ingress, is never consulted. The attach lands on
   the orchestrator's ASGI app, which has no route for it, and every client gets a bare rejection
   with **nothing in the gateway pod's log**, indistinguishable from an auth failure. Add a
   CDN-side rule for exactly `^/api/ssh/attach$`, ahead of the catch-all, and **anchor it**:
   swallowing `/api/ssh/attach-token` kills the feature one layer further in.

2. **Send it to Traefik's TLS entrypoint, not port 80.** With `ingress.tls.enabled` this Ingress
   carries `router.entrypoints: websecure`, so no router for it exists on `web`. Port 80 returns
   Traefik's own 404 while the config looks entirely correct.

3. **Tell the three 404s apart by body, not status.** They come from three different components:

   | Body | Length | Who answered | Meaning |
   |---|---|---|---|
   | `{"detail":"Not Found"}` | 22 B, JSON | FastAPI — the orchestrator | the CDN never handed off to Traefik (step 1) |
   | `404 page not found` | 19 B, plain | Traefik | no router matched: host, path, or **entrypoint** (step 2) |
   | `Not Found` | 9 B, plain | the gateway itself | **routing is correct** — this is the right answer to a non-WebSocket GET |

4. **Probe with `--http1.1`.** A `curl` upgrade probe that negotiates HTTP/2 returns 404 and looks
   exactly like the CDN stripping the upgrade; the classic `Upgrade:` handshake does not exist in
   h2. Over HTTP/1.1 a *successful* upgrade presents as curl hanging on 101, not as a 200.

5. Many tunnel daemons do not hot-reload their config — restart the daemon after editing it.

### Concurrency

The gateway's caps are `GatewayConfig` dataclass defaults with no environment lever, so there is
deliberately no chart value for them. **Per gateway pod:** 64 concurrent SSH connections, 16 per
source, 12 channels per connection, 4 attachments per workspace. The pre-auth slot is held for a
connection's whole life, so despite the name these are session limits, not startup limits.
`SshTcpListener` is AF_INET only — it does not bind IPv6.

None of those numbers is fleet-wide. `GatewayLimiter` is constructed once per process with no
shared store, and the WSS ingress has no session affinity, so **`sshGateway.replicas` multiplies
every one of them** — including `max_attachments_per_workspace`, which bounds how many people can
be attached to a single workspace at once. That is a security cap, not a capacity cap, and
`replicas: 2` doubles it to 8 with nothing in the chart or the logs saying so. `replicas: 1` is
load-bearing; raise it only with that understood.

Then create a job with the VM backend (Cockpit → Create → workspace: VM, or
`"workspace": {"backend": "vm"}` in the API call) and watch:

```bash
kubectl get vm,vmi,dv             # the VM reaches Running, the DataVolume Succeeded
kubectl get pod -l srw.io/component=agent-workspace -o wide
```

The job's VM context should show `ssh_ready_source: provisioner_probe` once the orchestrator
has proven SSH to the VM's pod IP. Inside the job, `sudo apt-get install -y <pkg>` raises an
approval request in the cockpit; approve it and the command runs.

### Troubleshooting

| Symptom | Cause |
|---|---|
| `helm install` fails in the `vm-preflight` hook | KubeVirt/CDI CRDs not installed; install the operators first |
| VMI stuck in `Scheduling` | no node with `kubevirt.io/schedulable=true` that matches `nodeSelector`/tolerations, or `devices.kubevirt.io/kvm` missing (no KVM) |
| DataVolume `Pending` forever | WaitForFirstConsumer with no consumer — normal until the VM starts; for a standalone DataVolume add the `cdi.kubevirt.io/storage.bind.immediate.requested: "true"` annotation |
| DataVolume stuck in `ImportScheduled` | CDI has no scratch space: set `scratchSpaceStorageClass` |
| `UnrecognizedProvisioner` on the StorageProfile | normal for `local-path`; the chart names access modes and volume mode explicitly, so nothing needs to be inferred |
| importer dies with `blockdev: cannot open /dev/cdi-block-volume: Permission denied` | the DataVolume left `volumeMode` unset and CDI inferred `Block` from the class's StorageProfile. The importer runs unprivileged with capabilities dropped and cannot open a raw device on an SELinux-enforcing node. Name `volumeMode: Filesystem` on the DataVolume |
| VM `Stopped` after the guest powered off | KubeVirt does not restart a voluntary shutdown; the orchestrator recovers the job with the kept root disk |
| `sudo` inside the VM is denied with "orchestrator unreachable" | the guest daemon cannot reach the orchestrator Service on 8085 — check the workspace NetworkPolicy and that `vm.mode` is `same-cluster` |
| the agent logs `the final git push did NOT land` and the job's Gitea repo stays at "Initial commit" | the workspace was handed a remote it cannot authenticate. The orchestrator logs `Dispatch: repository transport for job …` at every dispatch: it must name an `ssh://srw-repo-…` alias with `1 managed credential(s)`; a plain `http://…` with `0 managed credential(s)` means the job row reached dispatch without `repo_name` or the managed repository authority could not be proven (a `No managed repository authority for job …` warning precedes it) |
| the job page reports the IDE as `unavailable: code-server is not running on the live VM` | expected: the VM image ships `code-server.service` disabled and the live-VM IDE is not wired yet; the snapshot-based IDE still works after the job ends |
| the orchestrator logs `VM controller rejected delete … persistentvolumeclaims … is forbidden` after a job completes | a chart older than 2026-08-25 granted the controller only `get,list` on PersistentVolumeClaims; the captured teardown deletes the exact rootdisk PVC by UID and needs `delete`. Upgrade the chart |
| `srw-vmi-metering` / `srw-storage-metering` crash-loop with `configuration is invalid` or post to an empty URL | a chart older than 2026-08-25 rendered the same-cluster collectors' orchestrator URL as `""` and `maxSnapshotBytes` as `6.7108864e+07`; upgrade the chart |
| the orchestrator logs `Infrastructure metering collection requested but capabilities are incomplete (vm/claim-requested durable source shadow activation)` and every collector gets `503` on `/v1/tickets` | a Helm shadow gate (`vmShadowEnabled`, `vmPvcShadowEnabled`) is on before the durable activation row is in `shadow`. Order: inventory gates on → fleet-admin `…/compute-activation/workspace_vm/shadow` and `…/storage-source-activation/vm/claim-requested/shadow` → shadow gates on |
| `POST …/compute-activation/workspace_vm/schedule` answers `409 … requires a fresh item-for-item shadow snapshot` while VMs are running, or `500 … scheduling failed` | metering-engine limits fixed after 2026-08-25 (VMI shadow comparisons; initial authority on a recovery epoch) — upgrade the orchestrator; on an older release the class can only be scheduled with no VM in the last relist snapshot |

### Metering the VM tier

VM compute and root-disk storage are metered by two extra collectors that the chart
renders only in `same-cluster` mode. They are dark-launched: inventory and shadow
evidence first, publication never before a fleet-admin activation boundary.

```yaml
infrastructureMetering:
  collectorEnabled: true              # mandatory master (also runs the Pod collector)
  shadowEnabled: true
  stableClusterId: my-cluster
  vmInventoryEnabled: true            # VMI collector
  vmShadowEnabled: true               # only after the durable row is in shadow — see below
  vmIngestionSecretName: srw-infra-metering-vmi-ingestion
  vmPvcInventoryEnabled: true         # root-disk PVC collector
  vmPvcShadowEnabled: true
  vmStorageIngestionSecretName: srw-infra-metering-vm-storage-ingestion
  networkPolicy:
    enabled: true                     # or allowUnrestrictedEgress: true on a dev cluster
    apiServerCidrs: ["10.43.0.1/32"]
```

`vmStableClusterId` and `vmNamespace` default to the release's own values in this mode.
The two ingestion Secrets hold `INFRASTRUCTURE_METERING_VMI_INGESTION_KEY` and
`INFRASTRUCTURE_METERING_VM_STORAGE_INGESTION_KEY` (≥ 32 random bytes each) and must be
distinct from each other, from the application Secret and from the chart-managed
`<release>-infra-ingestion`. Leave `pvcInventoryEnabled` off: the release namespace holds the
root-disk PVCs and inventorying them twice is refused at render time.

Enable in this order, rolling the orchestrator between steps: inventory gates on → as a
fleet admin `POST /api/admin/usage/v2/compute-activation/workspace_vm/shadow` and
`POST /api/admin/usage/v2/storage-source-activation/vm/claim-requested/shadow` → shadow gates
on. Evidence lands in `compute_shadow_observations` (one row per VMI per relist,
`owner_kind=job|thread`) and `storage_shadow_observations` (`vm_rootdisk_claim`); evidence
is written at relist time (`relistIntervalSeconds`, default 300 s), so a VM shorter than
that may not be observed. Durable intervals require scheduling the class for a future UTC
midnight; nothing is backfilled.

### Network isolation

Same-cluster VMs are covered by the **same NetworkPolicy** as workspace
containers (`workspace.networkPolicy.enabled`, default `true`). KubeVirt
propagates labels from `VMI.spec.template.metadata.labels` to the
virt-launcher pod, so a single `podSelector` on
`srw.io/component: agent-workspace` matches both pod and VM workspaces.
Disabling the flag removes the policy for both — there is no separate
VM-only toggle.

The unified policy enforces:

- **Ingress**: SSH/CDP only from the agent, IDE only from the orchestrator
  / Traefik.
- **Egress**: DNS, TCP 22/80/443, Tailscale (UDP/41641 + UDP/3478), in-cluster
  database services. No general internet beyond HTTP/S + SSH (SSH is needed
  for git clone of SSH-auth repository datasources).
- **VM↔VM and container↔container lateral isolation**: the policy does not
  list the workspace label as an allowed source.

**CNI requirement.** NetworkPolicy on KubeVirt VMs is only enforced by CNIs
that implement the standard policy resource on virt-launcher pods:

| CNI                | NetworkPolicy enforced? |
|--------------------|-------------------------|
| Calico             | yes                     |
| Cilium             | yes                     |
| OVN-Kubernetes     | yes                     |
| Antrea             | yes                     |
| K3s (Flannel + embedded kube-router) | yes (ingress and egress) |
| **Flannel**        | **no** (any policy)     |
| Kube-OVN           | partial (KubeVirt bugs) |

Operators on Flannel without a policy add-on see the resource applied with
no effect — the YAML is accepted but does nothing. Verify your CNI before
relying on the isolation guarantee.

A note on Tailscale: VMs run their own `tailscaled` inside the guest, and
KubeVirt's masquerade networking NATs the VM's NIC through virt-launcher's
veth. Pod-level NetworkPolicy therefore only sees the encrypted WireGuard
envelope (UDP/41641 + DERP/443), not the in-VM traffic. Egress restriction
on virt-launcher pods is meaningful for boot-time + tunnel handshake
traffic and meaningless for everything inside the tunnel — Headscale ACLs
are the right layer for tailnet-source filtering. See the public
[security model](../docs/security-model.md) for the surrounding trust assumptions.

## Post-install verification

```bash
# Wait for pods to be ready
kubectl -n srw rollout status deploy/srw-orchestrator
kubectl -n srw rollout status deploy/srw-cockpit

# Tail orchestrator logs
kubectl -n srw logs -l app.kubernetes.io/component=orchestrator -f

# Back up the encryption key — losing it is unrecoverable
kubectl -n srw get secret srw-secrets \
  -o jsonpath='{.data.APP_ENCRYPTION_KEY}' | base64 -d
```

Then visit `https://<global.domain>` for a multi-host deployment, or the exact
`https://<exposure.singleOrigin.address>:<publicPort>` origin for single-origin
mode; omit `:443` when using the default HTTPS port. The seeded realm administrator
credentials (when using internal Keycloak) are the chosen
`keycloak.bootstrapAdmin.username` (historical default `test`) / value of
`KC_REALM_ADMIN_PASSWORD`. The separate Keycloak server
administrator uses `KEYCLOAK_ADMIN_USER` and `KEYCLOAK_ADMIN_PASSWORD`.

---

## Upgrade

```bash
helm upgrade srw oci://ghcr.io/superhuman-remote-worker/charts/superhuman-remote-worker \
  --version <new-version> \
  -n srw -f my-values.yaml
```

The chart preserves `APP_ENCRYPTION_KEY` across upgrades when `secrets.create=true`
via `lookup`. ESO mode reads from Vault on each refresh interval.

### Migration 0185 pinned-runtime cutover

Migration 0185 is not rolling-compatible with pre-0185 pinned-runtime writers.
An ordinary upgrade leaves
`orchestrator.runtimeAuthorityMigrationMaintenanceAck=false`: a new pod refuses
the pending migration while the old ready pod remains available.

For the one-time cutover, first put create, Resume, prepare, and input admission
into maintenance. End or suspend all pinned sessions, wait for in-flight
provisioning/engage/stage/terminal work, and remove old warm-pool and dedicated
agent pods. Then upgrade with:

```bash
helm upgrade srw oci://ghcr.io/superhuman-remote-worker/charts/superhuman-remote-worker \
  --version <new-version> \
  -n srw -f my-values.yaml \
  --set orchestrator.runtimeAuthorityMigrationMaintenanceAck=true
```

That value supplies the exact migration acknowledgement and changes only the
orchestrator Deployment to `Recreate`, preventing old/new orchestrator overlap.
Do not use `helm upgrade --atomic` for this cutover. After migration 0185
commits, never roll back to an image with old pinned-runtime writers; roll
forward instead. Verify the migration ledger and lifecycle smoke tests before
reopening admission, then clear the acknowledgement on a later all-new-version
upgrade.

---

## Uninstall

```bash
helm uninstall srw -n srw

# PVCs are NOT deleted automatically — keep them, or remove explicitly:
kubectl -n srw delete pvc -l app.kubernetes.io/instance=srw
kubectl delete namespace srw
```

---

## Configuration reference

The full configurable surface is documented inline in `values.yaml`
(every option has a `# --` comment):

```bash
helm show values oci://ghcr.io/superhuman-remote-worker/charts/superhuman-remote-worker \
  --version <chart-version>
```

A reference customer overlay is shipped as `values.example.yaml` inside the
chart tarball.

---

## Support

- **Issues:** <https://github.com/superhuman-remote-worker/srw/issues>
- **License:** [LICENSE](https://github.com/superhuman-remote-worker/srw/blob/main/LICENSE)
