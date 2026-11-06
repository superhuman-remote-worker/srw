---
title: Troubleshooting
description: Symptoms, causes and fixes for the problems a new SRW installation hits most often.
---

# Troubleshooting

Each entry names the symptom you see, the cause, and the fix. Commands assume
the release and namespace `srw` that `install.sh` uses; adjust them if you
chose other names. For the local k3d setup, also see the troubleshooting
section of [Local Kubernetes with k3d](local-kubernetes.md#troubleshooting).

Two commands answer most questions about a stuck installation:

```bash
kubectl -n srw get pods
kubectl -n srw get events --sort-by=.lastTimestamp | tail -n 30
```

## Creating a job or session fails with 503

**Symptom.** Cockpit shows a **System not ready.** banner, and creating a job
or session fails. The API answers `503` with `"error": "system_not_ready"` and
a message that starts with `System not ready —`.

**Cause.** SRW refuses new jobs and sessions until the model setup is
complete. It needs:

- at least one provider API key or system endpoint;
- an enabled model for each of the four required capabilities: **chat**,
  **embedding**, **auxiliary** and **rerank**;
- a default model pinned for each of them; and
- an application default expert for jobs and for sessions (seeded on install).

**Fix.** The banner lists the missing steps, and so does
`GET /api/system/readiness` for any signed-in user (`missing_providers`,
`missing_capabilities`, `missing_defaults`, `missing_expert_defaults`). Add
what is missing under **Settings → Administration → Models**, as described in
[Get started](getting-started.md#1-add-models). One OpenRouter key can supply
all four capabilities; see [Models](models.md).

**Also check the embedding size.** SRW stores embeddings as 4096-dimensional
vectors, and the readiness check does not test the size. An embedding model
that returns any other size passes the check, but the first embedding call
then logs `EMBEDDING DIMENSION MISMATCH`, and that process switches off memory
and knowledge-base vector search until it restarts. Choose an embedding model
that returns 4096 dimensions.

## A workspace claim stays Pending

**Symptom.** `install.sh` or `helm upgrade` times out, and pods that mount the
shared workspace volume stay `Pending`. `kubectl -n srw get pvc` shows
`srw-workspace` as `Pending`, and `kubectl -n srw describe pvc srw-workspace`
reports that the access mode is not supported or provisioning failed.

**Cause.** The values ask for a `ReadWriteMany` workspace claim
(`workspace.accessMode`; on the configure page, **My storage class supports
ReadWriteMany** was ticked), but the storage class only offers block storage,
which supports `ReadWriteOnce`. k3s `local-path` is one of these.

**Fix.** Set `workspace.accessMode: ReadWriteOnce` in `values.yaml` (or untick
the box and download the files again). The access mode of an existing claim
cannot be changed, and the chart keeps this claim on purpose, so delete the
Pending claim — it holds no data yet — and run the installation again:

```bash
kubectl -n srw delete pvc srw-workspace
./install.sh
```

With `ReadWriteOnce`, workspace pods run on one node at a time.

## Certificates are not issued

**Symptom.** The browser warns about an untrusted or self-signed certificate on
your domain, and `kubectl -n srw get certificate` shows `READY` as `False`.

**Cause.** With **Let's Encrypt**, the chart asks cert-manager for a
certificate per hostname, and Let's Encrypt validates each one over HTTP on
port 80 (the HTTP-01 challenge). That fails when:

- the DNS records do not resolve to the ingress yet;
- port 80 does not reach the ingress from the internet (firewall, router or
  cloud security group);
- the records are proxied through Cloudflare (orange cloud), so the challenge
  request reaches Cloudflare first and, depending on its settings, never
  reaches cert-manager; or
- Let's Encrypt's production rate limits were hit by repeated reinstalls: it
  issues only a few certificates for exactly the same set of hostnames per
  week, and further orders fail with a "too many certificates" error until the
  window passes.

**Fix.** Find the reason in the challenge:

```bash
kubectl -n srw get certificate,order,challenge
kubectl -n srw describe challenge
```

Make `<domain>` and `*.<domain>` resolve to the ingress before you run
`install.sh`, and open port 80. On Cloudflare, set the records to **DNS
only** until the certificates are issued, or issue them through a DNS-01
ClusterIssuer of your own and choose **Existing ClusterIssuer** on the
configure page. Against rate limits, wait for the window the error names,
keep the namespace between reinstalls (the issued certificates live there as
Secrets; `helm uninstall` keeps the namespace), and test repeated installs
against the staging endpoint
`https://acme-staging-v02.api.letsencrypt.org/directory` before switching
back.

## Image pulls fail with `toomanyrequests`

**Symptom.** Pods stay in `ErrImagePull` or `ImagePullBackOff`, and their
events show `toomanyrequests` from Docker Hub.

**Cause.** Several bundled components (for example Nextcloud, Valkey, nginx,
Collabora and Crawl4AI) and some init containers use images from Docker Hub,
which limits anonymous pulls per IP address. A fresh install pulls many images
at once, and machines behind a shared address share the limit.

**Fix.** Kubernetes retries failed pulls with a growing delay, so waiting and
running `install.sh` again is often enough. To avoid the limit for good, give
the node a registry mirror or Docker Hub credentials. On k3s, create
`/etc/rancher/k3s/registries.yaml`, for example with Google's public Docker
Hub cache:

```yaml
mirrors:
  docker.io:
    endpoint:
      - "https://mirror.gcr.io"
```

then restart k3s (`sudo systemctl restart k3s`). Images the mirror does not
have are still pulled from Docker Hub. On other clusters, configure the
container runtime's mirror or credentials, or create a `docker-registry`
Secret for Docker Hub and list it in `global.imagePullSecrets` for the chart's
pods.

## `install.sh` times out

**Symptom.** `install.sh` stops at the Helm step with a timeout (it waits up
to 20 minutes), or at a `rollout status` check afterwards.

**Cause.** A first install pulls every image and starts the components in
order; the orchestrator comes last because it waits for the databases,
identity provider, Git and migrations. Slow downloads, rate-limited pulls, a
Pending volume or a pod that keeps crashing push it past the timeout.

**Fix.** Find the pod that is not ready and read its events and logs:

```bash
kubectl -n srw get pods
kubectl -n srw describe pod <pod>
kubectl -n srw logs deploy/srw-orchestrator
```

Fix the cause — often one of the entries above — and run `./install.sh`
again. Re-running is safe: the script installs add-ons only when they are
missing, reuses the chart version it saved in `chart-version.txt` and the
existing workspace SSH key, and with Production refuses to overwrite the
credentials of a running installation. Ten minutes for a first install is
normal.

## A session stays on "Provisioning agent"

**Symptom.** A new session shows **Starting session** and never gets past
**Provisioning agent**. The browser console may show WebSocket errors such as
`Unexpected response code: 200`.

**Cause.** Usually one of:

- the browser still runs a Cockpit service worker from an older release,
  whose cached files use a retired WebSocket route;
- the agent pod cannot start (no capacity, an image that cannot be pulled, a
  quota); or
- the `srw-session-jwt` Secret that signs session connections is missing.

**Fix.** In the browser's developer tools, open **Application**, unregister
the service worker for the Cockpit address under **Service workers**, clear
the site data under **Storage**, and reload. Then check the agent pods and
the Secret:

```bash
kubectl -n srw get pods -l srw/managed-by=agent-provisioner
kubectl -n srw logs deploy/srw-orchestrator | grep -i provision
kubectl -n srw get secret srw-session-jwt
```

## Self-registered users are stuck at "Failed to send email"

**Symptom.** A user registers on the Keycloak sign-in page and lands on
**Failed to send email, please try again later.** Every later sign-in stops at
the same page, and the user never reaches SRW. **Forgot password?** fails the
same way.

**Cause.** The realm asks new users to verify their address, but the
installation has no mail server. Chart 0.1.0 and earlier created every realm
with **Verify email** and **Forgot password** switched on. Newer charts switch
both on only when `email.enabled` is true and `email.smtp.host` is set.
Keycloak imports the realm on the first install only, so an existing realm
keeps its settings through upgrades and restarts, and the chart does not
change them. Fix an existing realm by hand. The same applies the other way
round: if you configure SMTP later, switch both on yourself.

SRW itself does not need a verified address. A user gets in once an
administrator approves them in SRW or gives them the `user` realm role.

**Fix.** Open the Keycloak admin console (`https://auth.<domain>/admin/`, or
`https://<origin>/identity/admin/` in single-origin mode), sign in as the
Keycloak server administrator (`KEYCLOAK_ADMIN_USER`), and select the realm
`srw`:

1. **Stop new cases.** Under **Realm settings → Login**, switch
   **Verify email** off. Switch **Forgot password** off as well: its mail
   cannot be sent either.
2. **Unblock each stuck user.** Under **Users**, open the user, switch
   **Email verified** on, remove **Verify Email** from
   **Required user actions**, and select **Save**.
3. **Start a new sign-in.** Ask the user to open the SRW address again rather
   than retry on the old error page.
4. **Check admission.** If SRW still refuses the user, approve them in SRW, or
   give them the `user` realm role. A group called `user` is not the role.

Without verification, nobody proves they own the address they register with.
SRW, Git and the cloud link accounts by email address, so someone could
register with another person's address first. Check the address before you
approve a user.

For many users, run step 2 from a workstation with `kubectl` and `jq`. The
script lists every user who holds the **Verify Email** action, then marks
their address verified and removes only that action; other required actions,
such as **Update Password**, stay. Review the list before you run the loop.
**This script has not been run against a live Keycloak yet;** try it on a
single user first.

```bash
NS=srw                      # namespace
KC_POD=deploy/srw-keycloak  # <release>-keycloak
REALM=srw                   # keycloak.realm
kc() {
  kubectl -n "$NS" exec "$KC_POD" -c keycloak -- \
    /opt/keycloak/bin/kcadm.sh "$@" </dev/null
}

# Sign kcadm in as the Keycloak server administrator. The session is stored
# inside the pod and reused by the calls below.
kubectl -n "$NS" exec "$KC_POD" -c keycloak -- sh -c \
  '/opt/keycloak/bin/kcadm.sh config credentials \
     --server "http://localhost:8080${KC_HTTP_RELATIVE_PATH:-}" \
     --realm master --user "$KEYCLOAK_ADMIN" --password "$KEYCLOAK_ADMIN_PASSWORD"'

# 1. Who is stuck?
kc get users -r "$REALM" --limit 100000 \
  --fields id,username,requiredActions > users.json
jq -r '.[] | select((.requiredActions // []) | index("VERIFY_EMAIL"))
       | .username' users.json

# 2. Unblock them.
jq -r '.[] | select((.requiredActions // []) | index("VERIFY_EMAIL"))
       | [.id, .username, ((.requiredActions - ["VERIFY_EMAIL"]) | tojson)]
       | @tsv' users.json |
while IFS=$'\t' read -r id username actions; do
  echo "unblocking $username"
  kc update "users/$id" -r "$REALM" \
    -s emailVerified=true -s "requiredActions=$actions"
done
```

## Local k3d: cloud or Keycloak sign-in fails after recreating the cluster

**Symptom.** On a local k3d cluster, signing in to the cloud or Git through
Keycloak fails after `k3d cluster delete` and `create`. With OpenCloud
enabled, it lands on `/access-denied` and its logs show `connect: connection
refused` for `auth.localhost`.

**Cause.** Pods reach `auth.localhost`, `cloud.localhost` and `git.localhost`
through the Traefik ClusterIP. `scripts/local-dev-up.sh` writes that address
into a CoreDNS override, and an OpenCloud you enabled in `values-local.yaml`
carries it in `opencloud.hostAliases`. A recreated cluster gives Traefik a
new ClusterIP, so the stored address is stale.

**Fix.** Run `./scripts/local-dev-up.sh` again; it rewrites the CoreDNS
override with the current address. If your `values-local.yaml` sets
`opencloud.hostAliases`, update the IP there and upgrade the release:

```bash
kubectl -n kube-system get svc traefik -o jsonpath='{.spec.clusterIP}'
helm upgrade srw ./helm -n srw --kube-context=k3d-srw -f deployment/values-local.yaml
```

## A project answers 409 "This project is being deleted."

**Symptom.** Opening a project, or starting a job or session in it, fails
with `409` and `This project is being deleted.` The project is missing from
the project list. Deleting it again answers `202` with `"status": "deleting"`,
or `"deleting": true` through the manifest API.

**Cause.** A delete that passes its checks marks the project first. From then
on SRW refuses every request to the project except the delete. The delete
then removes the project's Git repositories, folder grants, database row,
search index, Keycloak group and cloud folder. If a step fails, for example
because Gitea, Keycloak or the cloud is unreachable, the orchestrator records
the error and retries in the background: after 30 seconds at first, with the
wait doubling up to an hour. A decided delete cannot be cancelled.

**Fix.** Find the step that fails, in the orchestrator log or in the deletion
record (the app database pod is `srw-postgres-0` in the default install):

```bash
kubectl -n srw logs deploy/srw-orchestrator | grep "deletion is not finished"
kubectl -n srw exec srw-postgres-0 -- sh -c 'psql -U "$POSTGRES_USER" -d "${POSTGRES_DB:-srw}" -c "SELECT project_id, attempts, pending_steps, last_error, next_attempt_at FROM project_deletions WHERE completed_at IS NULL"'
```

Fix what `last_error` names, such as a service that is down or a credential
that changed. The next background pass then finishes the delete; nothing else
is needed.

## Still stuck

The [Helm chart guide](../helm/README.md) covers each value and the failure
modes behind it. If the documentation and the behaviour disagree, please open
an issue on [GitHub](https://github.com/superhuman-remote-worker/srw/issues)
with the chart version, the deployment shape and the smallest useful
reproduction.
