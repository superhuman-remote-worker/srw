# Owned application E2E harness

The authoritative local command is:

```bash
./scripts/e2e-app.sh run
```

It creates a uniquely named `srw-e2e-*` k3d cluster, uses an isolated
kubeconfig, builds and imports the current checkout's production images, starts
the deterministic provider, deploys the minimal Helm profile, generates fresh
Keycloak identities, runs the Chromium journey in the version-matched official
Playwright image, captures sanitized diagnostics on failure, and attempts to
delete only the exact cluster it created. Teardown refuses destructive action
if it cannot re-prove that exact cluster's ownership.

Lifecycle commands are also available individually:

```bash
./scripts/e2e-app.sh up
./scripts/e2e-app.sh test
./scripts/e2e-app.sh diagnostics
./scripts/e2e-app.sh cleanup
./scripts/e2e-app.sh down
```

The first owned browser attempt requires the generated journey user to begin
pending and exercises administrator approval. A later `test` against the same
owned stack is recorded as a rerun and may reuse that now-approved identity;
this iteration path never relaxes the first-attempt assertion used by `run`.

## Profiles

`up`, `run` and `test` take `--profile`. Each profile is the shared baseline
plus zero or more overlays; the baseline is never edited to suit one batch.

| profile | values | what it adds | use it for |
|---|---|---|---|
| `pinned-virtual` (default) | `values-e2e.yaml` | nothing — the cheap baseline: pinned execution lane, virtual workspace, no workspace image built | the browser journey and anything that does not touch a workspace or a forge |
| `stateless-sandbox` | + `values-stateless-sandbox.yaml` | stateless executor Deployment, sandbox workspace backend, a `local-path` workspace PVC, and the current-source workspace image | workspace-backed sessions, SSH attach, real worker jobs, retirement/reclaim inventories |
| `forge-sandbox` | + `values-forge-sandbox.yaml` | the bundled Gitea StatefulSet on sqlite3 (no second Postgres) | project provisioning, project repositories, knowledge-vault materialisation — anything that goes through the forge |
| `cloud-sandbox` | + `values-cloud-sandbox.yaml` | the bundled Nextcloud on SQLite + filesystem storage (OpenCloud off), `rclone_mount` as the workspace driver, and **protected cloud mode** with the chart-derived protected-effect lane | cloud mounts, protected-cloud engage/await, staged cloud-diff review, main-cloud configuration/reload — anything that needs a real backend rather than "no backend bound" |

Each profile *composes* the one above it, it does not replace it:
`forge-sandbox` work still needs a workspace-backed session, and `cloud-sandbox`
needs both. Adding a dependency to the shared `values-e2e.yaml` is what broke
the journey's determinism contract when SearXNG was tried there — the browser
asserts exactly one provider endpoint and exactly two enabled models — so a new
dependency belongs in an overlay.

The browser bootstrap selects container sessions through the generated journey
user's personal Project workspace defaults. Account preferences no longer select
a workspace backend. Attach mode only verifies the existing Project selection.

`cloud-sandbox` turns `agent.protectedCloudModeEnabled` on, which is a separate
decision from binding a backend: with a backend but no mode, every protected
route answers the "disabled" refusal and the mount builders are never asked for
a payload. The chart derives `nextcloud.protectedEffect` from that flag, so the
overlay only has to name the dedicated HMAC Secret — `secrets.create` is false
on this stack, so the harness mints every Secret itself, including that one.
That root is deliberately *not* a key in `srw-e2e-app-secrets`: agent Pods mount
that bundle wholesale through `envFrom`.

State lives below `cockpit/test-results/app-harness/` (already ignored). The
harness creates a private ownership marker and publishes `active.json` with an
exclusive claim, so concurrent runners cannot replace one another's deletion
authority. Only one default state directory may be active at a time. For
concurrent runs, set `APP_E2E_STATE_DIR` to a distinct, not-yet-created child of
an existing directory; pre-existing unmarked directories are rejected rather
than chmodded or adopted. Credential, kubeconfig, and auth-state files are mode
`0600` and are never part of the diagnostic allowlist.

An existing stack may be used only for non-authoritative browser iteration:

```bash
APP_E2E_ALLOW_ATTACH=1 \
APP_E2E_BASE_URL=http://localhost \
APP_E2E_USERNAME=... APP_E2E_PASSWORD=... \
APP_E2E_ADMIN_USERNAME=... APP_E2E_ADMIN_PASSWORD=... \
APP_E2E_CONTROL_URL=http://127.0.0.1:... \
APP_E2E_CONTROL_TOKEN=... \
APP_E2E_PROVIDER_BASE_URL=http://fixture:8000/v1 \
./scripts/e2e-app.sh test --attach
```

Attach mode is deliberately verification-only: it does not install the chart,
seed or pin models, approve users, clean resources through the lifecycle
runner, or delete a cluster. Remote origins additionally require
`APP_E2E_ALLOW_REMOTE=1`. It is not release evidence.

The owned profile uses HTTP only inside a disposable Docker network. Thus this
suite validates application/auth routing but intentionally does not claim
public TLS correctness. The inference Service exposes port 8000 only; the
token-protected control port is reached through a run-owned `kubectl
port-forward` and is never routed by ingress or a Service.

Docker images are exported as host-platform-only archives and imported through
k3d direct mode. The harness parses each archive's config digests and requires
every exact canonical tag/digest on both runtime nodes before Helm starts; a
successful k3d command alone is not accepted as import evidence.

Teardown verifies the exact k3d server identity before deletion, proves the
cluster and server are absent afterward, and then removes only label-verified
run image tags and the exact k3d image volume. The browser container is also
run-named and label-verified; its disposable `node_modules` bind is removed
after execution while reports, ledgers, and sanitized evidence remain.

Failure diagnostics are bounded and written beneath the owned run directory.
They include readiness/restart/image projections, recent events, provider
counters, per-layer elapsed times, pod descriptions, and severity/status-only
log metadata. Free-form application log messages, prompt bodies, full Pod JSON,
auth state, and credential files are not copied into that bundle.

`run` fails closed when the Git worktree is dirty, because an image labelled as
the clean HEAD would not be reproducible. `APP_E2E_ALLOW_DIRTY=1` permits a
local implementation pass while marking the ledger and image release metadata
dirty/non-authoritative; it must not be used for release evidence.
