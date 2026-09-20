# Task 8 Report: Rollout Configuration and Fault-Injection Acceptance

## Status

Complete. Automatic recovery and replacement adoption remain separately
default-off. The production process consumes one validated settings object,
recovery transitions emit aggregate metrics plus redacted structured audits,
and the repository now carries an executable disposable-cluster acceptance
gate. This host could not satisfy the real Longhorn gate prerequisites, so the
live destructive matrix was skipped before cluster mutation and no mocked PASS
was substituted.

## Implementation

- Added the exact Helm/runtime rollout contract:
  - `VM_WORKSPACE_RECOVERY_ENABLED=false`
  - `VM_WORKSPACE_REPLACEMENT_RECOVERY_ENABLED=false`
  - immutable deadline `900` seconds
  - `4` global and `1` per-node probes
  - `30` second claim and permit leases
  - `10` second external-call timeout
- Added schema and template validation for dependent flags, fixed protocol-v1
  limits, and timeout/lease relationships. The orchestrator Deployment receives
  every value explicitly and rolls when the configuration changes.
- Added `VMWorkspaceRecoverySettings.from_env()` and wired the same validated
  instance through the production service. Claim TTL, permit TTL, global and
  node permit budgets, deadline, and external timeout now reach the store and
  reconciler instead of remaining inert rendered values.
- Added `VMWorkspaceRecoveryTelemetry` to real hold, queue-fence, probe, pause,
  retry, cancel, cleanup-block, release, and controller-pin paths. Metric labels
  are limited to bounded event/phase/code/result values. Audit records hash raw
  operation, job, VM, and PVC identifiers, truncate validated observation
  digests, and allow only reviewed reason/blocker vocabulary.
- Added a source-controlled disposable acceptance system:
  - read-only host preflight and `--help` mode;
  - uniquely named k3d cluster creation with captured runtime ownership before
    cleanup is allowed;
  - pinned k3s, KubeVirt, CDI, and Longhorn versions;
  - KVM/vhost/tun, iSCSI, mount-propagation, and upstream Longhorn preflight;
  - real Longhorn manager/CSI/node readiness plus RWO write, Pod replacement,
    and retained read;
  - checkout builds/imports for orchestrator, agent, and VM controller, with
    Docker archive config IDs checked on each node and against deployed Pod
    image IDs;
  - an opt-in Helm adapter, scoped RBAC, source-controlled driver, and in-image
    acceptance command;
  - live marker/checkpoint/PVC, exact stop receipt, full resume receipt,
    retention-pin acknowledgement, queue occupancy, network, SSH host-key,
    API, PostgreSQL, Kubernetes, and Longhorn evidence;
  - pause-only checks for absent stop evidence and forced VMI deletion;
  - explicit fault labels for committed-response replay, a controlled
    two-reconciler leader handoff, slow VM boot, and a live claimed observation
    held across the immutable deadline.
- Updated the older stateless resilience gate and Helm documentation so a Pod
  replacement cannot be mistaken for retained-VM recovery evidence.
- Added the two reviewed `qualify_recovery_successor` SSH call sites to the
  runtime-coordinate inventory as `pinned-host-key`.

## TDD Evidence

The initial Helm/default tests failed on nine missing rendered settings. After
the values existed, two additional RED cases rejected unsafe flag and timeout
relationships. Runtime configuration tests initially failed because the
settings module and production consumer did not exist.

Telemetry tests initially failed on the missing module and then on missing
production call sites. A focused audit test produced one RED failure when an
unbounded reason could expose caller-supplied text; the implementation now uses
the canonical bounded reason `captured_workspace_identity_changed_or_ambiguous`.

The gate/adapter stages failed first because no repository driver or in-image
command existed. The authoritative-evidence review then produced:

```text
4 failed, 4 passed
```

Those failures proved that incomplete receipt/pin/API evidence could pass, the
source driver injected an authority assertion, cluster deletion lacked a
captured ownership set, and deployed image IDs were not bound to imported
config digests. Each path now fails closed.

The selector-expanded suite exposed two acceptance-command defects: an
unsupported Kubernetes `_content_type` argument and a raw jobs INSERT outside
the production creation boundary. Focused regression tests now exercise the
exact CustomObjects API call and require `PostgresDB.create_job()` before the
queue/attempt fixture writes.

Final focused GREEN:

```text
PYTHONPATH=src python -m pytest \
  tests/test_vm_workspace_recovery.py \
  tests/test_vm_workspace_recovery_real_postgres.py \
  tests/test_vm_workspace_recovery_config.py \
  tests/test_vm_workspace_recovery_telemetry.py \
  tests/test_vm_workspace_recovery_gate.py \
  tests/test_vm_workspace_recovery_acceptance.py \
  tests/test_helm_vm_workspace_recovery.py -q --tb=short

145 passed, 1 warning in 67.03s
```

After adding the two focused acceptance regressions, the gate/adapter/Helm
subset reported `25 passed in 9.91s`.

## Required Verification

The required backend gate was rerun after the runtime fixes:

```text
PYTHONPATH=src ./scripts/pytest-fast.sh \
  tests/test_worker_queue.py tests/test_worker_driver_real_postgres.py \
  tests/test_claim_bundle.py tests/test_stateless_worker_runtime.py \
  tests/test_turn_executor.py tests/test_run_queue_reaper.py \
  tests/test_fenced_checkpointer.py \
  tests/test_vm_workspace_recovery_real_postgres.py \
  tests/test_completion_effect_transactional.py \
  tests/test_lifecycle_vm_manager.py tests/test_retained_vm_workspaces.py \
  tests/test_checkpoint_retention.py tests/test_job_projection.py \
  tests/test_job_list_schema.py tests/test_vm_workspace_recovery.py \
  tests/test_vm_readiness.py tests/test_vm_controller.py \
  tests/test_vm_retained_storage.py \
  tests/test_vm_remote_operation_real_postgres.py -q --tb=short
```

Result: `1054 passed, 11 skipped, 18 warnings in 106.65s`.

The literal Cockpit command in the plan, `npm test -- --runInBand`, is not
accepted by this repository's Vitest runner. The equivalent serial run passed:

```text
cd cockpit && npm test -- --maxWorkers=1
167 files, 3167 tests passed in 159.40s
```

One parallel Cockpit run had a single five-second canvas timeout; that test
passed alone, and the clean serial full run above passed.

Schema replay and drift check:

```text
PYTHONPATH=src CONTAINER_ENGINE=podman scripts/schema-snapshot.sh app
PYTHONPATH=src CONTAINER_ENGINE=podman scripts/schema-snapshot.sh --check app
```

Both replayed all 220 transactional migrations. The final result was
`OK: schema artifacts are up to date.` and produced no snapshot diff.

Repository checks:

- Ruff passed on every changed Python file.
- `git diff --check` passed.
- `python scripts/check_runtime_coordinate_callers.py` passed with the two
  reviewed successor call sites.
- `python scripts/select_affected_tests.py --help` passed.
- Repeating `--changed` for every Task 8 file selected `ALL`.
- The focused policy/acceptance run after the inventory ruling reported
  `33 passed in 25.41s`.
- `helm lint helm` remains blocked by a pre-existing chart-package mismatch:
  `chart metadata is missing these dependencies: cloudnative-pg`. Focused Helm
  schema/render tests pass.

The selector-expanded xdist run completed with:

```text
31593 passed, 180 skipped, 228 failed, 7 errors in 974.32s
```

It was not repeated under the same contaminated environment, per the parent
scope ruling. The failures/errors group into missing optional `libcst` and
`importlinter` packages (plus Kubernetes in an isolated interpreter), installed
`langgraph-checkpoint` 4.0.0 versus the expected 4.1.1, cross-suite xdist
import/global-state contamination, and stale baseline expectations for the
migration head, Task 7 Helm resources, and an e2e secret reference without a
`key`. The two Task 8 acceptance defects found by that run and the runtime
inventory category are green in the focused checks above.

## Live Gate Result

```text
PYTHONPATH=src python scripts/vm-workspace-recovery-k3d-gate.py --help
```

Passed and displayed preflight/run, pinned component versions, explicit
container engine, immutable guest image, evidence output, and cleanup controls.

Read-only preflight on this Fedora/Podman host reported:

```text
SKIPPED: missing capability: binary:longhornctl, container-engine:k3d-docker-required, socket:/run/iscsid/socket, module:iscsi_tcp, file:disposable-values, image:guest-digest
```

The command returned before any cluster lookup or mutation. A destructive gate
was therefore not run. Replacement recovery remains disabled by default until
a capable disposable host produces complete live evidence.

## Scope Rulings and Self-Review

- The runtime inventory classifies both successor SSH calls as
  `pinned-host-key`: the call path validates the pinned fingerprint and
  successor incarnation before network I/O. If this classification is wrong,
  it could hide recipient drift; the correction would be to move those calls
  behind an exact-runtime recipient boundary and reclassify them.
- The gate defaults to the repository's Fedora/Podman environment but emits the
  explicit Docker-required capability skip. A future Podman path must reproduce
  archive pruning, direct import, ownership, node CRI ID, and deployed image ID
  proof before removing that skip.
- Reviewed all Task 8 identifiers in metric attributes and structured logs;
  only hashed references appear in logs and none appears in metric labels.
- Reviewed PASS construction: command success, a StorageClass, a mock Pod, or
  constants in the wrapper cannot satisfy it. Every recovery result comes from
  the in-image application and is cross-checked with live substrate and image
  provenance in the outer process.
- Reviewed cleanup: only the uniquely prefixed cluster whose original labeled
  container-ID set still matches can be deleted; `--keep-on-failure` is the
  sole explicit retention path.
- Confirmed the unrelated `srw-public-preview-desktop.png` and nested
  knowledge-base modifications are not part of this task.

## Review Fix Round 1

The first scoped review found four acceptance-proof gaps. All four now fail
closed:

- The outer driver discovers exactly one Helm-rendered adapter by its dedicated
  label. It no longer assumes the release name. Ordinary `srw` and
  `fullnameOverride` renders are covered, and a missing or ambiguous adapter
  after the gate was enabled is a deployment failure rather than a capability
  skip.
- The deadline case starts a real production controller observation while its
  PostgreSQL claim is live, blocks it until database time crosses the original
  900-second deadline, then lets the controller call finish. A default-off
  acceptance hook extends only the local wait; the production PostgreSQL CAS
  remains unchanged. The evidence records that the deadline CAS returned no
  staged claim, no final release occurred, the queue stayed parked, and the
  disk and checkpoint survived. The scenario does not rewrite
  `first_observed_at` or `deadline_at`.
- The overlap case runs two `VMWorkspaceRecoveryService` instances with
  distinct durable worker identities. The first owns a live probe while the
  gate performs an exact claim/permit handoff; the second claims and completes
  the operation before the first external observation returns. Live DB
  evidence proves one operation, one dispatch, bounded global/node permits, an
  unchanged deadline, a higher winning claim token, and rejection of the stale
  result.
- Marker and checkpoint I/O now uses `pinned_agent_ssh_command` with the
  captured SSH host-key fingerprint and bounded subprocess I/O. The regression
  suite proves a wrong host key is rejected before any command can run.

The review tests were written RED before each implementation step. The first
focused run reported `7 failed, 23 passed`; the deadline barrier contract then
reported `2 failed`; the gate-only timeout assertion, late-observation hook,
and durable-CAS evidence each failed individually before their implementations.
Final focused GREEN:

```text
PYTHONPATH=src python -m pytest \
  tests/test_vm_workspace_recovery.py \
  tests/test_vm_workspace_recovery_acceptance.py \
  tests/test_vm_workspace_recovery_gate.py \
  tests/test_helm_vm_workspace_recovery.py -q --tb=short

58 passed in 14.49s
```

Because the deterministic gate hook changes the recovery service, the complete
required backend gate was rerun after the final edit:

```text
1055 passed, 11 skipped, 18 warnings in 122.71s
```

The schema drift check replayed all 220 transactional migrations and reported
`OK: schema artifacts are up to date.` Ruff passed for all changed Python
files, `git diff --check` and the runtime-coordinate inventory passed, and the
changed-file selector returned `ALL`. `--help` passed. The final read-only
preflight returned before cluster mutation with:

```text
SKIPPED: missing capability: binary:longhornctl, container-engine:k3d-docker-required, socket:/run/iscsid/socket, module:iscsi_tcp, file:disposable-values, image:guest-digest
```

No destructive cluster run was attempted on this incapable host, so this round
adds executable live proof logic but does not claim a completed live matrix.

## Review Fix Round 2

The second scoped review found three live-execution gaps. Focused tests first
reported `4 failed`: the acceptance command had no owner for three ordinary
reconciliation waits, the deadline evidence named a later CAS that production
never reached, the overlap scenario called `reconcile_once()` directly, and
the validator still accepted the inaccurate deadline field.

The live command now owns reconciliation end to end:

- Replacement, missing-stop, and forced-deletion scenarios each run a scoped
  `VMWorkspaceRecoveryService.run()` loop. The loop starts only for that
  scenario and shuts down before the controlled leader and deadline cases.
  Retention pins are synchronized and their controller acknowledgements are
  read back before launcher/VMI fault injection, and the fault is injected
  before the generic loop can claim the operation.
- Leader overlap uses two complete reconciler loops with distinct durable
  worker identities. Each loop is behind a separate PostgreSQL session
  contending for the same gate-specific advisory lock. Evidence proves B could
  not acquire while A held leadership, A released its lock, B acquired before
  starting, and B minted the higher recovery claim token. A's already-started
  external observation returns only after B completed. The validator requires
  the distinct identities and successful leadership transfer in addition to
  one operation, one dispatch, permit limits, an unchanged deadline, and stale
  result rejection.
- The deadline observer still begins under a live claim and returns after the
  immutable database deadline. Production rejects it at
  `recovery_preconditions()`, before `stage_observation()`. The gate now records
  that exact rejection, requires that staging and release were never attempted,
  runs the ordinary expired `claim_due()` path to materialize the attention
  pause, and proves zero dispatches plus a still-parked queue. No production
  authority predicate was weakened to reach a later CAS.

Additional behavior tests cover scoped reconciler startup/shutdown, exclusive
advisory-lock transfer, execution ordering around the injected faults, and the
new evidence fields. Final focused GREEN:

```text
PYTHONPATH=src python -m pytest \
  tests/test_vm_workspace_recovery_acceptance.py \
  tests/test_vm_workspace_recovery_gate.py \
  tests/test_helm_vm_workspace_recovery.py \
  tests/test_vm_workspace_recovery.py -q --tb=short

62 passed in 11.47s
```

The required backend gate reported `1055 passed, 11 skipped, 18 warnings in
112.20s`. The schema drift check replayed all 220 transactional migrations and
reported `OK: schema artifacts are up to date.` Ruff, `git diff --check`, the
runtime-coordinate inventory, selector help, and gate help passed. The changed
file selector returned `ALL`.

The final read-only preflight again stopped before cluster mutation:

```text
SKIPPED: missing capability: binary:longhornctl, container-engine:k3d-docker-required, socket:/run/iscsid/socket, module:iscsi_tcp, file:disposable-values, image:guest-digest
```

This host still cannot execute the destructive KubeVirt/Longhorn matrix, so the
report does not claim live PASS evidence for either controlled handoff or the
15-minute deadline crossing.

## Review Fix Round 3

The third review found that cancelling A's reconciler loop at handoff also
cancelled `_observe()`. Although the barrier returned later, the cancelled
production coroutine could not consume that result or attempt any durable
fenced store call. The old `stale_result_rejected` value therefore inferred
rejection only from B's recovery, one dispatch, and token ordering.

Two focused regressions first failed: one rejected the explicit
`stale_task.cancel()` path and required direct stale-boundary evidence; the
other proved the outer validator still accepted missing direct evidence.

The controlled handoff now models the normal leadership polling window without
changing production cancellation behavior:

- A and B hold separate checked-out PostgreSQL sessions. A proves exclusive
  acquisition, unlocks its advisory lock while retaining its session, and B
  acquires on another session before A's session returns to the pool. Evidence
  includes distinct positive `pg_backend_pid()` values as well as the distinct
  durable worker identities.
- A's already-started service loop remains blocked in the real controller
  observation while B acquires leadership, mints a higher recovery claim token,
  and completes the single dispatch. B retains the leader lock while A's
  delayed observation returns.
- A then attempts the next production claim-fenced store boundary. The gate
  records the exact rejected boundary from
  `accept_stop_evidence`, `trusted_stop_receipt`,
  `recovery_preconditions`, or `stage_observation`, and records whether staging
  was attempted. `stale_result_rejected` now requires this direct rejection in
  addition to B's recovered state, one dispatch, and increasing claim token.
  The outer validator rejects absent, contradictory, or inferred evidence.

Final verification:

```text
Focused recovery/acceptance/gate/Helm: 62 passed in 12.03s
Required backend: 1055 passed, 11 skipped, 18 warnings in 124.95s
Schema drift: 220 migrations replayed; artifacts are up to date
```

Ruff, `git diff --check`, the runtime-coordinate inventory, selector help, and
gate help passed. The changed-file selector returned `ALL`. Read-only preflight
again exited before mutation:

```text
SKIPPED: missing capability: binary:longhornctl, container-engine:k3d-docker-required, socket:/run/iscsid/socket, module:iscsi_tcp, file:disposable-values, image:guest-digest
```

The capable-cluster live matrix remains the only unexecuted verification; no
mocked or inferred live PASS is claimed.

## Review Fix Round 4

The fourth review identified the exact point where the old leader is fenced.
While A's controller observation is still blocked, production `_observe()`
polls `claim_is_current()`. Once B owns the higher claim, that check returns
false, cancels and gathers A's probe, and returns without calling any later
stop-evidence or staging method. The previous gate waited for a later store
boundary, so a capable live run would time out even though production rejected
the stale work correctly.

The focused regressions were written first and reported `2 failed`: the
acceptance module did not expose an instrumentable store wrapper for the real
`_observe()` path, and the outer validator rejected truthful
`claim_is_current` evidence. The gate now holds A's claim check at a controlled
barrier until the database proves B durably owns the successor claim, then
delegates the check to the real store and records its false result. The
production service cancels the delayed probe through its normal lost-claim
path. Evidence requires `stale_store_boundary=claim_is_current`,
`stale_store_boundary_rejected=true`, and `stale_stage_attempted=false`.
An additional validator regression then failed `1 failed, 10 passed` while the
old allowed-boundary set still accepted `recovery_preconditions`; it is GREEN
after requiring the exact claim-current boundary.
The validator rejects a later-boundary assertion because this controlled
ordering must stop at the claim-current fence. The two leaders still use
separate held PostgreSQL connections; their distinct positive
`pg_backend_pid()` values and the advisory-lock exclusivity/transfer proof are
unchanged.

The new behavioral test drives `VMWorkspaceRecoveryService._observe()` with a
blocked controller observer. It proves the store check cannot run before the
controlled handoff, then observes the real false claim-current result, probe
cancellation, no staging, and a final `None` after the delayed observer is
released. Final verification:

```text
Focused recovery/acceptance/gate/Helm: 63 passed in 11.99s
Required backend: 1055 passed, 11 skipped, 18 warnings in 134.05s
Schema drift: 220 migrations replayed; artifacts are up to date
```

Ruff, `git diff --check`, the runtime-coordinate inventory, selector help, and
gate help passed. The changed-file selector returned `ALL`. Read-only preflight
again exited before any cluster mutation:

```text
SKIPPED: missing capability: binary:longhornctl, container-engine:k3d-docker-required, socket:/run/iscsid/socket, module:iscsi_tcp, file:disposable-values, image:guest-digest
```

This host still cannot execute the destructive KubeVirt/Longhorn matrix, so
round 4 corrects the executable evidence contract without claiming a live
cluster PASS.
