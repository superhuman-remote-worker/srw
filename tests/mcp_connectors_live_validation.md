# Live validation — MCP server connectors

**Type:** deployed end-to-end validation and rollout checklist. This is not a
pytest file; it requires the local k3d/Tilt stack, disposable MCP servers, and
test users.

**Status (2026-07-24):** **PENDING.** Tasks 1–13 are implemented and automated
coverage is green. The release gate in Task 14 has not been run. At the last
closure audit, the `srw` k3d containers were running and both MCP flags were
enabled in `deployment/values-local.yaml`, but the Kubernetes API was
unresponsive and Tilt was not running.

**Superseded for stdio (connector drivers D5b):** stdio MCP servers no longer
run in agent pods, and `MCP_STDIO_ENABLED` / `agent.mcpStdioEnabled` are gone
(setting the Helm key to true fails the render). A stored stdio connector is
refused on create and edit, never delivered, and its Test answers
"unsupported". Every stdio check below (the local connector, the npm/uvx
runtimes, stdio canaries and subprocess reaping, the stdio gate matrix rows)
no longer applies; a stdio image runs as a managed MCP server behind the
front, gated by `scripts/k3d-managed-mcp-stdio-gate.py`. The remote (http and
sse) checks still stand.

**Source documents:**

- `knowledge-base/knowledge/features/mcp_datasources.md`
- `knowledge-base/knowledge/superpowers/plans/2026-07-23-mcp-datasources.md`

This runbook is the authoritative list of MCP validation still requiring live
or deployment evidence. Update it in place as checks are run. Do not put real
tokens, authorization headers, mailbox credentials, or private server URLs in
this document, screenshots, logs, or failure reports.

---

## Completion levels

### P0 — v1 release gate

Every P0 checkbox must pass before moving the MCP design and implementation
plan to `knowledge-history/done/`. These checks cover the original Task 14 acceptance gate
plus credential redaction and process cleanup.

### P1 — required before broad stdio/dev rollout

P1 exercises the full transport/auth/runtime matrix and failure recovery. A P1
failure may be accepted only with a linked issue, an explicit rollout
restriction, and an owner.

### P2 — hardening and production-readiness

P2 is not required to call the gated v1 implementation complete, but it should
be resolved before enabling MCP broadly or enabling stdio in a multi-tenant
deployment.

---

## Already automated — do not duplicate as manual sign-off

The following passed during the closure audit:

| Layer | Result |
|---|---|
| MCP and adjacent Python suites | 472 passed |
| Focused Cockpit connector suites | 26 passed |
| Helm lint with `helm/ci/test-values.yaml` | passed |
| Helm lint with `helm/ci/customer-external-values.yaml` | passed |
| Full Cockpit suite, TypeScript, i18n, Ruff, broader affected Python suite | passed during implementation |

The automated suites cover naming/truncation, config validation, owner-task
lifecycle, stdio/HTTP/SSE test servers, one reconnect, tool-call errors,
dynamic registry loading, wildcard expansion, datasource processing, job and
session plumbing, CRUD gates, connection probing, grants, and the Cockpit form.

Quick regression command:

```bash
.venv/bin/python -m pytest \
  tests/test_mcp_naming.py \
  tests/test_datasource_tool_categories.py \
  tests/test_mcp_manager.py \
  tests/test_mcp_registry.py \
  tests/test_mcp_datasource_setup.py \
  tests/test_mcp_agent_wiring.py \
  tests/test_mcp_datasource_api.py \
  tests/test_capability_grants.py \
  tests/test_datasource_config_persistence.py \
  tests/test_live_datasource_update.py \
  tests/test_persistent_app.py \
  tests/test_session_config_plumbing.py \
  -q --tb=short

cd cockpit
npx vitest run \
  src/app/views/datasources/datasource-list.component.spec.ts \
  src/app/views/datasources/connector-terminology.spec.ts
cd ..

helm lint helm/ -f helm/ci/test-values.yaml
helm lint helm/ -f helm/ci/customer-external-values.yaml
```

Passing these commands is necessary but does not replace the live checks below.

---

## Evidence record

Fill this in before starting:

| Item | Value |
|---|---|
| Date / tester | |
| Git SHA | |
| Orchestrator image/tag | |
| Agent image/tag | |
| Cockpit image/tag | |
| Kubernetes context / namespace | `k3d-srw` / `srw` |
| Remote HTTP test server | disposable alias only |
| Remote SSE test server | disposable alias only |
| stdio npm package/version | |
| stdio uvx package/version | |
| Test project ID | |
| Test job IDs | |
| Test thread IDs | |
| Follow-up issue links | |

Use only disposable credentials and non-secret canary values such as
`MCP_TEST_CANARY_20260724`. Store full logs outside git if they could contain
user content; record only sanitized excerpts and identifiers here.

---

## Prerequisites

1. The cluster API responds and Tilt is running:

   ```bash
   k3d cluster list
   timeout 10s kubectl --context=k3d-srw get pods -n srw
   tilt get uiresources
   ```

2. The local overlay enables both flags:

   ```bash
   helm template srw helm/ -f deployment/values-local.yaml \
     | grep -E 'MCP_(DATASOURCES|STDIO)_ENABLED'

   kubectl --context=k3d-srw -n srw get configmap srw-config \
     -o jsonpath='{.data.MCP_DATASOURCES_ENABLED}{" "}{.data.MCP_STDIO_ENABLED}{"\n"}'
   ```

   Expected for the full local matrix: `true true`.

3. Orchestrator, Cockpit, and at least one newly provisioned agent use images
   containing the MCP commits. Do not rely only on a mutable `latest` tag;
   record the actual image ID/tag in the evidence table.

4. Prepare disposable MCP servers:

   - a controlled streamable-HTTP server with at least `echo` and `add`;
   - a controlled SSE server exposing equivalent tools;
   - a slow tool whose delay can exceed 60 seconds;
   - a tool that returns an ordinary server error;
   - a server process that can be deliberately stopped and restarted;
   - a remote auth endpoint supporting bearer and custom-header canaries;
   - one known npm stdio package and one known uvx stdio package.

5. Prepare two ordinary test users:

   - **owner** can create connectors and projects;
   - **other user** must not see or attach the owner's private connector;
   - one user/config must be testable with `datasource_tools` denied.

6. Use a throwaway project, jobs, sessions, connectors, and credentials. Do not
   use production connectors for destructive or failure-path tests.

---

# P0 — v1 release gate

## P0.1 Cluster and deployment preflight

- [ ] Kubernetes responds without timeouts and all required SRW pods are
      Ready.
- [ ] Tilt reports the relevant resources healthy; no stale failed build is
      serving older code.
- [ ] `srw-config` contains
      `MCP_DATASOURCES_ENABLED=true` and `MCP_STDIO_ENABLED=true`.
- [ ] A newly created agent pod contains the expected MCP Python packages.
- [ ] The agent image has working `node`, `npx`, `uv`, and `uvx` executables.
- [ ] The recorded image identities correspond to the Git SHA under test.

If this section fails, stop. Results from a stale or partially deployed stack
do not count.

## P0.2 Cockpit authoring and terminology

As the owner:

1. Open **Connectors**.
2. Confirm the primary action is **New Connector**.
3. Confirm the form uses **Connector Type** and offers **MCP Server**.
4. Switch between Remote and Local (stdio).

Pass criteria:

- [ ] English uses **Connectors**, **New Connector**, **Connector Type**, and
      **Test Connection**—not “MCPs & Data Sources.”
- [ ] German uses **Konnektoren**, **Neuer Konnektor**,
      **Konnektortyp**, and **Verbindung testen**.
- [ ] Remote fields show URL plus none/bearer/custom-header auth.
- [ ] stdio fields show command, arguments, and environment variables plus the
      warning that the command runs in the agent environment.
- [ ] Secret inputs are masked and are not repopulated as plaintext after save
      or edit.
- [ ] MCP does not show a misleading read-only toggle; the server/credential
      access boundary is explained.
- [ ] Validation errors are actionable and use Connector terminology.

## P0.3 Remote streamable-HTTP connector

1. Create a remote MCP connector using the controlled HTTP server with no auth.
2. Run **Test Connection**.
3. Save it, reopen it, and test it again.

Pass criteria:

- [ ] Test Connection reports success, tool count, and expected tool names.
- [ ] The saved connector can be listed and edited without exposing
      credentials.
- [ ] Updating the URL or description persists correctly.
- [ ] An invalid URL/shape is rejected without leaking supplied values.
- [ ] The orchestrator remains healthy after repeated tests.

## P0.4 Local stdio connector

1. Create a local connector using `npx` and a known disposable MCP package.
2. Add a non-secret canary environment variable.
3. Run **Test Connection**.

Expected: when the orchestrator image lacks the stdio runtime, the endpoint
returns the explicit “not testable here; resolves at job start” outcome. It
must not claim that the connector is broken.

Pass criteria:

- [ ] The connector saves with `connection_url = null`.
- [ ] Test Connection returns the expected untestable-here result, or succeeds
      if the orchestrator intentionally contains the runtime.
- [ ] The command, arguments, and environment shape survive edit/save.
- [ ] The canary value never appears in list/get API responses, Cockpit
      notifications, or orchestrator logs.

## P0.5 Project link and job execution

1. Link the HTTP and stdio connectors to a throwaway project.
2. Create a job with both explicitly selected.
3. Ask: “List the tools available from the MCP servers, call the HTTP echo
   tool, then call one stdio tool.”

Pass criteria:

- [ ] Project linking succeeds and both connectors appear in the job picker.
- [ ] The job reaches `processing` and completes normally.
- [ ] Both servers' tools are namespaced
      `mcp__<server_slug>__<tool>`.
- [ ] At least one remote and one stdio tool call succeeds.
- [ ] `README.md` contains **Available Connectors** and an
      `### MCP Servers` section with status and namespaced tool names.
- [ ] `README.md` contains no URL credentials, headers, tokens, stdio
      environment values, or other secret material.
- [ ] The audit trail shows the successful `mcp__…` calls and correct job ID.
- [ ] Native tools still bind and work; MCP registration does not replace the
      ordinary tool registry.

## P0.6 Graceful startup degradation

1. Add a third remote connector pointing at a guaranteed-unreachable endpoint.
2. Select it together with one healthy connector for a new job.

Pass criteria:

- [ ] Job startup is delayed by no more than the configured per-server connect
      bound (approximately 10 seconds for the bad server).
- [ ] The job still runs and can call the healthy MCP tool.
- [ ] The bad server is marked `unavailable` in `README.md`.
- [ ] The error is concise and contains no credential/header values.
- [ ] The job is not marked failed solely because one MCP server is down.

## P0.7 Persistent session and live detach

1. Start a persistent session with the healthy HTTP connector selected.
2. Ask the agent to list and call its MCP tool.
3. Attach the stdio connector live and use one of its tools on the next turn.
4. Detach the HTTP connector while the session remains active.
5. On the next turn, ask the agent to call the detached tool.

Pass criteria:

- [ ] Initially selected MCP tools are available and callable.
- [ ] A live attachment becomes available on the next turn.
- [ ] A live detachment removes the corresponding tools on the next turn.
- [ ] The detached tool is not silently retained in the bound-tool list.
- [ ] Content already read remains in conversation context, but live access is
      gone.
- [ ] The remaining connector continues working.
- [ ] Session and agent stay healthy with no cancel-scope teardown error.

## P0.8 Capability grant restriction

Run a job/session as a user whose effective grants deny `datasource_tools`.

Pass criteria:

- [ ] The connector may remain stored/selected according to ordinary UI
      visibility rules, but no `mcp__…` tools are bound.
- [ ] The agent cannot call an MCP tool by guessing its namespaced name.
- [ ] Native tools allowed by the user's other grants continue working.
- [ ] Re-enabling the grant restores MCP tools on a new bind/turn as designed.

## P0.9 Ownership and private-connector isolation

Using the second user:

- [ ] The owner's private MCP connector is absent from list and eligible-picker
      responses.
- [ ] Guessing its UUID cannot retrieve, update, delete, test, or link it.
- [ ] Error behavior does not disclose whether the guessed connector exists.
- [ ] Project membership grants only the intended linked access.
- [ ] Removing project access removes connector eligibility.

## P0.10 Credential, log, and error redaction

Use disposable canaries—not real secrets—for bearer tokens, custom headers,
and stdio environment values. Search only for those known canaries.

- [ ] No canary appears in `README.md`.
- [ ] No canary appears in orchestrator or agent logs.
- [ ] No canary appears in connection-test errors or job error strings.
- [ ] No canary appears in Cockpit toasts or browser console output.
- [ ] No canary appears in audit-trail arguments/results unless the test MCP
      tool itself was deliberately designed to return that canary.
- [ ] A stdio server that writes a canary to stderr does not leak it into agent
      logs.
- [ ] The stdio canary is present for the child server but absent from the
      parent agent process environment.

## P0.11 Cleanup and process lifecycle

After the job completes and after detaching/closing the stdio session:

- [ ] stdio server subprocesses exit; no orphan/zombie process remains.
- [ ] MCP owner tasks close without anyio cancel-scope errors.
- [ ] Repeating create/use/close three times does not accumulate subprocesses,
      file descriptors, or stuck agent tasks.
- [ ] Deleting a connector prevents future selection but does not corrupt
      historical job/audit records.
- [ ] Agent shutdown/restart reaps active stdio children.

---

# P1 — full transport, auth, and recovery matrix

## P1.1 SSE transport

- [ ] Create and test an SSE connector against the controlled server.
- [ ] Tool discovery succeeds and a namespaced SSE tool call completes.
- [ ] Disconnecting the SSE stream produces a bounded error/reconnect rather
      than hanging the agent.
- [ ] SSE auth headers remain redacted.

## P1.2 Remote authentication

Test each remote auth mode with disposable canaries:

- [ ] No authentication.
- [ ] Bearer token.
- [ ] One custom header.
- [ ] Multiple custom headers.
- [ ] Incorrect token/header returns a safe, useful error.
- [ ] Updating credentials replaces the old credential without exposing either
      value.

## P1.3 Both stdio runtimes

- [ ] An npm MCP server starts via `npx` and serves a successful tool call.
- [ ] A Python MCP server starts via `uvx` and serves a successful tool call.
- [ ] Runtime versions match the pinned image intent.
- [ ] Package download/startup failure degrades only that connector.
- [ ] Arguments containing spaces or shell metacharacters are passed literally;
      no shell expansion or command injection occurs.

## P1.4 Mid-run death and one reconnect

1. Call a tool successfully.
2. Stop the server process or remote endpoint.
3. Restore it before the next tool call, then call again.
4. Repeat while leaving it unavailable.

- [ ] First post-failure call attempts one reconnect and succeeds when the
      server is back.
- [ ] A failed reconnect returns a string tool error rather than raising into
      and killing the graph.
- [ ] Only one reconnect is attempted for the guarded call.
- [ ] Status changes to unavailable when recovery fails.
- [ ] Other MCP and native tools remain usable.

## P1.5 Timeout and ordinary tool errors

- [ ] A tool taking longer than 60 seconds returns a bounded timeout error.
- [ ] A server-declared tool error becomes a readable tool-error result.
- [ ] Neither path fails the whole job/session.
- [ ] Timeout cleanup leaves the connection usable for a later normal call, or
      reconnects once according to the documented lifecycle.

## P1.6 Multiple servers, collisions, and large catalogs

- [ ] Two connectors with the same display name receive distinct stable server
      slugs.
- [ ] Two servers exposing the same tool name remain independently callable.
- [ ] Long/invalid names are accepted by the actual configured LLM provider
      after namespacing and remain at most 64 characters.
- [ ] Rebinding does not duplicate dynamic registry entries.
- [ ] A server exposing more than 40 tools registers all tools while
      `README.md` lists 40 plus a correct `+N more` tail.
- [ ] A 100+ tool catalog does not make the agent/job fail at bind time.

## P1.7 Restart and resume

- [ ] A persistent thread resumed on a newly provisioned agent rediscovers its
      selected MCP tools.
- [ ] Orchestrator restart does not corrupt stored connector credentials or
      links.
- [ ] Agent restart does not reuse stale in-process tool objects.
- [ ] A connector edited while a session is stopped uses the updated config on
      resume.

## P1.8 Feature-gate matrix

Test via a controlled local values change and rollout:

| MCP gate | stdio gate | Expected |
|---|---|---|
| false | false | MCP create/update/test is denied; existing records do not bind |
| true | false | Remote MCP works; stdio authoring/use is denied |
| true | true | Remote and stdio work |

- [ ] Helm defaults remain off without a local override.
- [ ] The orchestrator and provisioned agents observe the same effective flags.
- [ ] Turning a gate off fails closed after rollout/rebind.
- [ ] Gate errors use Connector terminology and reveal no credentials.

## P1.9 Provider compatibility

Run one simple MCP call with each model/provider family supported by the target
deployment:

- [ ] Default OpenAI-compatible provider.
- [ ] Anthropic-compatible path, if enabled.
- [ ] Any locally hosted/OpenRouter-compatible model intended for MCP use.
- [ ] Namespaced tool schemas bind without provider-specific validation errors.
- [ ] Tool result serialization is readable and does not lose structured
      content needed by the agent.

---

# P2 — hardening and production-readiness

## P2.1 Network and tenant boundaries

- [ ] NetworkPolicy permits intended external MCP endpoints.
- [ ] Cluster-internal/private endpoints remain unreachable where the
      deployment policy intends them to fail closed.
- [ ] One user's connector cannot be reached through another user's job,
      project, session, or guessed UUID.
- [ ] The multi-tenant stdio threat model has an explicit security decision
      before `MCP_STDIO_ENABLED` is enabled outside trusted/dev environments.

## P2.2 Load and soak

- [ ] Concurrent jobs with separate MCP connectors do not share manager state
      or credentials.
- [ ] Multiple MCP servers in one job connect concurrently; startup time is
      bounded per server rather than multiplied unnecessarily.
- [ ] A multi-hour persistent session does not leak tasks, sockets, subprocesses,
      or file descriptors.
- [ ] Repeated live attach/detach cycles remain stable.
- [ ] Large tool outputs respect normal result/context limits.

## P2.3 Operational visibility

- [ ] Operators can identify connector/server name, transport, and safe status
      from logs without seeing credentials.
- [ ] Audit records distinguish namespaced MCP tools from native tools.
- [ ] Unavailable/reconnect/timeout events are observable enough to diagnose.
- [ ] Any desired last-success/tool-count UI health surface is tracked as a
      fast-follow rather than silently assumed present.

## P2.4 Rollout

- [ ] Dev remote MCP is explicitly enabled after P0 passes.
- [ ] Dev stdio enablement has an owner and accepted security boundary.
- [ ] Production remains gated until the stdio/multi-tenant review is complete.
- [ ] Rollback is verified by disabling flags and rolling the affected
      components without deleting stored connector records.

---

## Explicitly not v1 acceptance tests

Do not fail v1 because these intentionally unimplemented fast-follows are
absent:

- per-server enabled-tool allowlists;
- OAuth 2.1 browser flows for hosted MCP servers;
- MCP resources, prompts, or sampling;
- searchable KB ingestion of MCP tool descriptions;
- connector health-history/tool-drift UI;
- sandboxing stdio beyond the existing agent-pod boundary;
- session-scoped ad-hoc MCP servers created through chat.

If one is required for a rollout, create a separate feature/issue rather than
quietly expanding this acceptance gate.

---

## Failure report template

For every failed box, record:

```text
Test ID:
Git SHA / image IDs:
Connector transport:
Job or thread ID:
Expected:
Observed:
Sanitized logs/evidence:
Credential-redaction check:
Reproduction reliability:
Issue link:
Rollout impact / temporary restriction:
```

Stop immediately and rotate the disposable credential if any secret value
appears in logs, API output, `README.md`, Cockpit, or audit records.

---

## Final sign-off

- [ ] Every P0 checkbox passes on one recorded deployment revision.
- [ ] P1 is complete, or every deferred P1 item has a linked issue and explicit
      rollout restriction.
- [ ] No credential/redaction failure remains open.
- [ ] No stdio subprocess/cancel-scope leak remains open.
- [ ] Task 14 in
      `knowledge-base/knowledge/superpowers/plans/2026-07-23-mcp-datasources.md` is checked.
- [ ] The status block in `knowledge-base/knowledge/features/mcp_datasources.md` records the live
      verification date and tested image revisions.
- [ ] Both MCP documents are moved to `knowledge-history/done/`.
- [ ] This runbook is updated to **LIVE-VERIFIED** with date, tester, revision,
      sanitized evidence, and follow-up issue links.
