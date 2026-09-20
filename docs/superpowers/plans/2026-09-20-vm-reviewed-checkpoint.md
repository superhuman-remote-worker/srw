# Reviewed VM reliability checkpoint for develop

This release integrates reviewed implementation through `83ac143f0`
with the existing develop changes through `9e2fb32dc0e6cebe51f04b7688368193c8924e0e`.
Publication verification and formatting corrections follow on the integration
branch.

## Behavior available after rollout

- Provisioning distinguishes root-disk work, placement waits and guest boot.
  Placement and capacity waiting do not consume active boot time or repeatedly
  count the same VM admission.
- Capacity shortage remains a durable wait. Prolonged controller transport
  failure produces bounded attention, and original Job deadlines still apply.
- Job progress exposes bounded provisioning and retry reasons. Existing VM
  identity, cleanup and retained-disk protections remain authoritative.
- Controller images package their shared dependencies and verify installed
  imports during the build.
- Routed replies retain the exact question fence. Accepted execution and terminal
  transitions clear existing idle metadata atomically.

## Features that remain disabled

Keep these settings false during this checkpoint rollout:

```yaml
orchestrator:
  vmProvisioning:
    creationRetryEnabled: false
  vmWorkspaceRecovery:
    enabled: false
    replacementEnabled: false
  vmWorkspaceRecoveryAcceptanceGate:
    enabled: false
vm:
  resourceAdmission:
    observerEnabled: false
    shadowEnabled: false
    enforcementEnabled: false
    clusterWidePodReadAcknowledged: false
```

`WORKSPACE_IDLE_RELEASE_ENABLED` remains false. This checkpoint does not provide
physical idle compute release or shared access wake. Resource-policy and
reservation foundations have no runtime enforcement wiring; the operational
`vmController.maxConcurrentVms` count backstop is unchanged.

## Database and unfinished work boundary

The application schema advances through `0268_workspace_idle_terminal_exit.sql`.
Migration 0269, the unfinished cancellation settlement writer, attachment
cancellation handoff and their in-progress tests are excluded. They remain in the
original implementation worktree for a subsequent bounded development job.

Integration verification found one read of the pending 0269
`cancellation_completion` column in an otherwise reviewed commit. This release
removes that read so the published code depends only on the shipped 0268 schema.
The PostgreSQL actuation and inherited-attachment regressions exercise this
boundary against the clean release schema.

Creation retry stays disabled because final cancellation settlement and its
acceptance remain incomplete. Do not enable the feature merely because its API,
schema, progress UI and controller foundations are present.

The two exact migration lint exceptions for 0258 and 0261 are documented in
`.squawk.toml`: their constraints concern new ledgers in this rollout, and 0261
only widens the prior accepted values. Preserve the SQL checksums already applied
to the disposable acceptance database.

## Validation and rollout boundary

The isolated KubeVirt/local-path deployment of frozen `a4e3081f8` reached Ready
with zero restarts and preserved all four application PVC/PV identities. A
disposable lower-level stop probe proved truthful terminal evidence before Halted
and the expected refusal after grace-zero deletion. These results do not establish
a successful end-to-end recovery: the seven-scenario fault matrix, Longhorn
durability and actual worker continuation remain open.

Publication checks ran on a clean integration worktree, independently of unfinished
source changes. The changed Python suites and additional compatibility checks
passed **2,883 tests** in 938.86 seconds, with six dependency/OpenAPI warnings and
no skipped tests. The schema-boundary correction separately passed all 68 affected
PostgreSQL cases. Scoped Cockpit tests passed **71 tests**, and the production
frontend build passed. Full application schema replay matched the committed
artifact, migration lint found zero issues in the ten non-exempt files, strict
chart lint passed, and endpoint authentication, Ruff and formatting checks passed.
Formatting preserved the Python AST in all nine affected production files. Four
randomized parameter IDs were made stable so parallel pytest collection agrees.

Pushing develop starts the existing component-image and dev-chart pipeline;
Fleet may deploy the resulting chart. Start the development pilot with one VM
job and inspect readiness, available disk space and real test execution before
increasing concurrency. This release does not establish a new safe host capacity.
