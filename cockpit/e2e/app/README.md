# Application journey E2E

This suite drives a production SRW deployment through real Keycloak/BFF auth,
REST, durable SSE, persistence, and the Cockpit UI. It does not start a server
and it never intercepts application requests. The lifecycle owner is:

```bash
./scripts/e2e-app.sh run
./scripts/e2e-app.sh run --profile stateless-sandbox
```

The default `pinned-virtual` profile is the low-cost memory-backed baseline.
`stateless-sandbox` reuses the same browser journey but enables the shared
executor pool, selects a physical Kubernetes workspace, and includes the
current-source workspace image in the build/import/deployment proof. The
journey fails if orchestration silently falls back to the pinned lane.

The initial blocking journey proves that opening `/` creates nothing, the first
visible Send creates exactly one session and submits exactly one input, the
deterministic multi-chunk reply renders, reload hydrates one durable transcript,
the session list finds the correlation marker, and exact-id teardown deletes
only the resource recorded by that test.

For `stateless-sandbox`, setup additionally proves the user's saved workspace
preference is `sandbox`, the created thread is on the `stateless` lane, and a
physical Kubernetes workspace is materialized. These are profile preconditions,
not brittle browser assertions: the public chat journey remains identical across
both profiles. The harness builds the workspace image from the current checkout
and verifies every application tag plus its CRI config digest on both k3d nodes
before deployment, so a cached pinned/virtual fallback cannot produce a green
stateless result.

Permanent cleanup is part of the gate. The browser transport closes first, then
the exact ledger-owned thread is deleted under a 180-second graceful deadline.
Retryable `409`/`503` responses keep retrying the same terminal authority; if the
graceful phase does not settle, a separately bounded 60-second force phase may
run. Each HTTP request receives only the time remaining in its phase. A green run
requires a verified `404`, successful retaining provider close, and deletion of the exact
owned cluster.

Use a separate private state root for a dirty-tree development run and expect the
harness to label its result non-authoritative:

```bash
APP_E2E_ALLOW_DIRTY=1 \
APP_E2E_STATE_DIR="$PWD/cockpit/test-results/app-harness-stateless-local" \
  ./scripts/e2e-app.sh run --profile stateless-sandbox
```

Do not reuse a state root with an active ownership ledger; finish its `cleanup`
and `down` lifecycle first.

## Direct Playwright use

Use this only after the committed E2E stack is ready. From `cockpit/`:

```bash
npm run test:e2e:app
npm run test:e2e:app:headed
```

Required runtime variables have no credential defaults:

```text
APP_E2E_BASE_URL
APP_E2E_USERNAME
APP_E2E_PASSWORD
APP_E2E_ADMIN_USERNAME
APP_E2E_ADMIN_PASSWORD
APP_E2E_PROVIDER_BASE_URL
APP_E2E_CONTROL_URL
APP_E2E_CONTROL_TOKEN
APP_E2E_WORKSPACE_BACKEND
APP_E2E_EXPECT_EXECUTION_LANE
```

The base URL accepts loopback and `.localhost` origins by default. A disposable
container/cluster hostname additionally requires `APP_E2E_ALLOW_REMOTE=1`.
`APP_E2E_PROVIDER_BASE_URL` is the exact in-cluster inference URL ending in
`/v1`; setup rejects any catalog transport that points elsewhere.
The topology variables form one of two accepted pairs: `virtual` + `pinned`,
or `sandbox` + `stateless`. They default to the pinned pair for direct local
Playwright use. Attach mode never changes an existing user's workspace
preference, so a stateless attach run requires that user to be preconfigured
for the sandbox tier.

The owned-cluster runner may set `APP_E2E_AUTH_STATE` and
`APP_E2E_RESOURCE_LEDGER` to its private run directory. Auth state is written
with mode `0600`; the resource ledger is atomically updated as soon as the
create response yields an id. Playwright clears only the run directory's
dedicated `artifacts/` child; it never clears the harness-owned run root. Never
upload `.auth/` or an entire results tree.
Allowlisted browser artifacts are the HTML report, trace/video/screenshot on
failure, the sanitized network/provider attachments, and the exact resource
ledger. Delete the disposable cluster before exposing credential-bearing
traces.

The authoritative lifecycle runner sets `APP_E2E_DEFER_FAILED_CLEANUP=1`.
After a browser/body/network failure, the fixture closes transport and captures
sanitized evidence but leaves exact ledger ids and the provider scenario intact
until the runner has collected cluster diagnostics. The runner must then invoke
its unconditional exact-ledger cleanup and retain that run's closed provider accounting.
Without this opt-in, the Playwright fixture cleans failed tests itself.
The fixture refuses to overwrite an earlier incomplete resource ledger, so a
manual owned-stack rerun after failure must run `diagnostics` and `cleanup`
first.

## Attach mode

An explicitly prepared stack can be checked with both
`APP_E2E_ATTACH_MODE=1` and `APP_E2E_ALLOW_ATTACH=1`. Attach setup is strictly
verification-only: it does not approve a user or change model pins. The given
journey user must already be approved/non-admin, and the provider catalogue,
three defaults, expert defaults, and readiness contract must already match the
E2E profile. The journey still creates and permanently removes its own exact
thread.

## Discovery and debugging

```bash
npm run test:e2e:app:unit
npx playwright test --config=e2e/app/playwright.config.ts --list
```

`--list` is the sole zero-execution exception. Executable zero-test or
all-skipped runs fail via the custom reporter. Retries are disabled, one
Chromium worker is used, service workers are blocked, and neither fixed sleeps
nor `networkidle` are used. The auth setup is a visible project dependency;
`--no-deps` skips it, and Playwright UI mode does not run setup projects
automatically.
