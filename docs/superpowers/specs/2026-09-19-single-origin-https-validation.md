# Single-origin HTTPS implementation evidence

Implementation date: 2026-09-19. Validation used `feat/single-origin-https`, based
on SRW commit `cfca2a4f7`, and matching generator changes in `srw-cloud`.
Source integration targets `develop` in both repositories. These checks did not
deploy to an existing installation.

The opt-in profile exposes Cockpit, BFF/API, direct session WebSockets, Keycloak,
Gitea, Nextcloud, and the browser IDE through one chart-owned Traefik gateway.
The default remains `multi-host`. The chart generates a 365-day self-signed
certificate without cert-manager; replacement on Helm upgrade is intentional.

## Verification completed

| Check | Result |
| --- | --- |
| Focused Python security, routing, URL, chart and launcher suites | 274 passed; eight existing warnings |
| Focused Cockpit suites | 236 passed |
| Production Cockpit build | Passed; existing bundle/style/CommonJS warnings |
| Helm lint and kubeconform | Existing test profile and four single-origin profiles passed; each single-origin profile has 87 valid resources, zero missing schemas |
| Dependency exclusion | No cert-manager resources/annotations, Traefik CRDs, or legacy public ingress in the new profile |
| Generator full test suite | 85 passed against this chart |
| Generated fixture parity | Both exported installer fixtures match the private generator's deterministic exporter |
| API-server admission | Static hostless routes admitted by K3s; dedicated controller uses namespace-scoped RBAC and the release-specific annotation class |
| TLS selection and upgrades | No-SNI handshake presents the exact chart Secret certificate. Helm upgrades across localhost, loopback IPv4 and non-loopback private IPv4 changed the certificate/SAN and reconciled service origins |
| Real browser warning | Fresh Chrome 153.0.8010.36 and Firefox 148.0.2 profiles, actual warning acceptance, no TLS bypass and no local trust-store installation |
| Localhost browser journey | Login, refresh, logout/relogin, Git and Nextcloud SSO passed in both browsers. Chrome completed the session/IDE case; Firefox transport and file persistence were observed while refining the harness |
| Loopback IPv4 browser journey | Above plus host-only BFF response cookie, service cookie paths, secure-context crypto/media API exposure, encoded filename upload/download (>1 MiB), and 35-second no-service-worker observation passed in both browsers |
| Non-loopback private-IP full browser suite | **4 passed in 6.0 minutes**: both integration and session/IDE journeys passed in Chrome and Firefox at `https://172.19.0.2:30443` |
| Direct session transport | Both browsers streamed the deterministic response via `wss://O/p/<thread>/ws`, reconnected after reload, and retired their exact threads normally |
| Top-level browser IDE | Both browsers created a file through the terminal, opened and edited it in Monaco, saved, reloaded, reopened the file and found exactly one saved marker |
| Provider accounting and cleanup | Corrected focused session rerun: **2 passed in 3.9 minutes**. Each consumed exactly one response, with zero remaining/reserved/unexpected/pending calls and no increase in the cumulative unscoped counter; no active scenarios or session workers/workspaces remained |
| Keycloak context/backchannel | Real 26.2.5 discovery, internal token/userinfo/JWKS, public issuer and unchanged management health path verified |
| Identity rollout regression | Single-origin Recreate and legacy RollingUpdate verified; 14 service-chart tests passed after the rollout change |
| Nextcloud persistence | Real 31.0.14 and user_oidc 8.11.0 retained provider/identity linkage and adopted overwrite/trusted-domain settings on an existing PVC |
| Main cloud and internal WebDAV | Stable installation attested, active authority resolved, upload/download verified byte-for-byte, and test objects deleted |
| Worker job and public Git | Pinned sandbox job reached review after 12 deterministic tool steps; repository, commit history and raw file returned 200 through `/git`; normal approval/deletion removed the job and workspace |
| Gitea startup reconciliation | Real 1.22.6, fresh and existing SQLite, preserved source ID and group settings while updating discovery/client credentials before web startup |

Before integration into `develop`, the changed Python files were formatted with
the CI-pinned Ruff 0.14.10; all 14 syntax trees were unchanged. Lint and formatting
checks passed, followed by 210 focused Python tests (six existing warnings).
The generator feature was applied to its existing `develop` branch without the
unrelated site changes on `main`, retaining the current installer repository and
chart URLs. Its full `www/test/*.mjs` run passed 79 tests with one existing skipped
page-budget test. Both regenerated chart fixtures matched byte-for-byte.

## Findings incorporated

- The Keycloak Python client requires a trailing slash to preserve `/identity`
  while resolving relative admin/token paths. A real local HTTP-server regression
  test covers both context-path spellings and the legacy root.
- Empty `global.domain` now produces valid seeded account email addresses.
- Cockpit runtime environment changes trigger a rollout; a ConfigMap subPath
  mount alone does not refresh an existing container's `env.js`.
- Gitea caches discovery in its web process. Startup now waits for the configured
  issuer and reconciles before starting web, preventing an old Keycloak pod from
  restoring the previous browser address during an upgrade. The single-origin
  Keycloak Deployment uses `Recreate` so discovery cannot alternate between old
  and new issuers; identity configuration upgrades therefore interrupt login
  briefly. Multi-host retains its rolling strategy. Gitea's
  OpenID provider does not consume the custom URL flags used by its GitHub/GitLab
  providers ([upstream 1.22.6 source](https://github.com/go-gitea/gitea/blob/v1.22.6/services/auth/source/oauth2/providers_openid.go)).
- Presets explicitly enable the existing Nextcloud attestation lane, with a
  dedicated immutable HMAC Secret. Protected-cloud routing remains disabled.
- Production generator secret instructions include the browser IDE credential.
- Git and IDE menu entries now project correctly into the real shared menu.
  Regression coverage opens the actual menu instead of stubbing its projection.
- The browser harness completes VS Code workspace trust, uses the visible command
  center, waits for terminal filesystem effects, and reopens the exact file after
  reload. It uses ordinary key events for editing and requires exactly one saved
  marker, avoiding browser automation and restored-tab assumptions.
- The initial manual provider setup left multiple scenarios armed concurrently.
  Although all four browser cases passed, the final audit found 36 untagged
  embedding requests rejected because the fixture could not attribute them to
  one run. The browser project now arms exactly one scenario lazily, refuses
  pre-existing runs, checks both per-run accounting and the cumulative global
  counter, and deletes only its own scenario. The historical counter was retained
  while resetting the owned old scenarios for the focused session rerun. Both
  browser sessions then passed with the cumulative counter unchanged at 36
  (zero new unattributed calls); no provider scenarios remained afterward.
- Port 443 is omitted from public origins to match browser URL normalization;
  non-default ports are preserved. Local launcher checks both directions of a
  profile change before reporting an existing cluster ready.

## Installation and release boundary

Source integration into `develop` is separate from release publication and
deployment. The validation run did not publish a release image/chart or update
the hosted generator or an existing installation. Publish matching application
images and the chart together when releasing the feature; the old released
images do not contain the new runtime URL/capability handling.

The remaining deployment checks are the actual university server's routable
address/port, firewall or VPN rules, storage/runtime prerequisites, and any browser
policies that prohibit certificate exceptions. The disposable-cluster results do
not replace those environment checks.

## Environment and limits

Live checks used an isolated Docker/k3d cluster named `srw-single-origin-test`,
namespace `srw-single-origin`, with host port 18443 mapped to NodePort 30443. The additional server-address
check uses the node's non-loopback private IPv4 address on NodePort 30443.
The disposable cluster and its owned control port-forward were removed after
validation; the existing `srw` developer cluster remains and was not changed.
The orchestrator image and production Cockpit build contain this working
tree's changes. Unchanged agent/workspace runtimes use cached development images;
bundled service images are the chart's resolved versions. Test provider calls use the repository's deterministic
fixture, not a paid external model.

Cached images were imported explicitly, and the disposable local-path provisioner
used cached BusyBox 1.36 after a registry pull stalled. The stock local bootstrap
script was tested with command fixtures; the live cluster was prepared explicitly.
An early no-domain bootstrap exposed an invalid seeded email. The chart now renders
a valid fallback domain; that disposable account was corrected through Keycloak
admin before continuing persisted-state upgrade tests. A fresh full installation
from the final chart was not repeated. A physical university server, VPN/firewall setup,
Safari/iOS, embedded IDE webviews/extensions, and external Git/SSH/MCP clients
are outside the verified boundary. Media hardware permission/recording was not
exercised.

Returning an existing Nextcloud PVC to multi-host mode requires the manual
trusted-domain and overwrite-setting cleanup documented in `helm/README.md`.
The procedure was checked against the image and persisted configuration; a live
reverse migration was not performed. Fresh installations and address changes
within single-origin mode were exercised.

The full browser log is `/tmp/srw-single-origin-browser-privateip.log`; the
corrected session rerun is `/tmp/srw-single-origin-browser-accounting.log`, and
the final accounting audit is `/tmp/srw-single-origin-provider-final.log`. These
are local verification artifacts, not runtime dependencies.

The isolated browser project is `cockpit/e2e/single-origin`. Its full command
requires the deterministic session fixture and fails if it is missing. Warning
screenshots, browser versions, request-origin ledgers, console output and traces
are retained in ignored test output; traces can contain disposable credentials
and are not publication artifacts.
