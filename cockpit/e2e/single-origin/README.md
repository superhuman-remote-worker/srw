# Single-origin HTTPS browser acceptance

Run against a disposable deployment containing these chart and application changes.
The target and credentials are mandatory; there is no default cluster or account.
Install the test browsers with `npx playwright install chromium firefox`.

```bash
export SRW_SINGLE_ORIGIN_URL=https://localhost:8443
export SRW_SINGLE_ORIGIN_USERNAME=test
read -rs SRW_SINGLE_ORIGIN_PASSWORD
export SRW_SINGLE_ORIGIN_PASSWORD
npm run test:e2e:single-origin -- --grep "one warning"
```

Each journey starts a fresh persistent Chromium/Firefox profile, requires the real
certificate warning, and clicks through it once. TLS error ignoring, certificate
bypass flags, pretrusted profiles and service-worker blocking are prohibited. The
browser performs API fetches itself because Playwright's API client does not share
its certificate exception. Optional `SRW_CHROMIUM_EXECUTABLE` selects an installed
Chrome binary, and `SRW_SINGLE_ORIGIN_HEADED=1` shows the browsers.

The first journey checks BFF login, Secure/HttpOnly/host-only cookies, refresh,
logout/relogin, secure-context crypto and media API availability, absent Angular
service workers and unsupported setup controls, plus actual Git and Nextcloud SSO.
It records document, fetch, XHR, EventSource and WebSocket origins and requires
the configured origin only.
Media permission prompts and hardware recording are not tested.

The command above is explicitly login/integration smoke coverage. Full acceptance
(`npm run test:e2e:single-origin` without the grep) also requires a deterministic
provider from `tests/e2e/app/deterministic_provider`, configured as `e2e-chat`,
a pinned sandbox execution lane, and these settings:

```bash
export SRW_SINGLE_ORIGIN_RUN_ID=<unique-prefix>
export SRW_SINGLE_ORIGIN_PROVIDER_CONTROL_URL=http://127.0.0.1:<control-port>
read -rs SRW_SINGLE_ORIGIN_PROVIDER_CONTROL_TOKEN
export SRW_SINGLE_ORIGIN_PROVIDER_CONTROL_TOKEN
npm run test:e2e:single-origin
```

Expose the provider control port only on loopback; the fixture rejects any other
control URL. The session fixture requires the provider to have no active runs,
records its cumulative unscoped-call count, and arms one `reply` scenario for the
current browser project immediately before the journey. After the session cleanup
it requires exactly one consumed response, no remaining, pending, or unexpected
calls, and no change to the global unscoped count, then deletes only that owned
run. The configured single worker therefore keeps one scenario active at a time.
The login-only smoke command above neither reads the provider settings nor arms a
scenario. Full acceptance fails if its provider setup is missing; it cannot
silently skip. It creates its own exact thread, sends input, checks streaming and
reload/reconnection, and opens a rendered top-level IDE.
It accepts the new workspace's normal VS Code trust prompt, creates a file in
the integrated terminal, edits/saves it in Monaco, and
verifies the content after reloading the IDE.
It retires that exact thread through the normal application lifecycle even on
failure. The ignored artifacts record the owned thread ID for recovery if the
application is unavailable during cleanup.

Test artifacts include browser versions, the real warning screenshot, console
messages, service-worker requests, an origin ledger, and traces in
`cockpit/test-results/single-origin`. Traces can contain credentials and session tokens: keep this ignored directory private and do not
publish it. Temporary browser profiles are deleted at teardown.

Repeat with an IPv4 origin and after a Helm upgrade to cover both supported address
forms, certificate replacement, and persisted service configuration. A generated
certificate may change on upgrade; a fresh exception is expected then. These checks
do not establish Safari, external Git/SSH/MCP client, or embedded IDE support.
