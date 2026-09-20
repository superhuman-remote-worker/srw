# Task 7 Report: Controller Retention and Exact Stop Evidence

## Status

Complete. The controller now exposes authenticated, read-only recovery
observation; PostgreSQL pins are durably reconciled to Kubernetes Lease records;
destructive disk paths fail closed around exact pins; and only append-only,
validated stop receipts can authorize replacement recovery.

## Implementation

- Added migration `0255_vm_workspace_recovery_controller_evidence.sql` and
  regenerated the schema snapshot. Retention pins now retain durable controller
  activation/release desire, acknowledgement identity, retry count, retry time,
  and last error across process restarts.
- Added signed controller endpoints for recovery observation and pin
  reconciliation. Pin creation/replay/release uses the existing lifecycle HMAC,
  durable nonce/replay checks, exact recovery/PVC/generation identity, and UID
  preconditions for deletion.
- Recovery refuses observation until the exact active controller pin is
  acknowledged. Release commits in PostgreSQL first; a failed controller release
  leaves the Lease active and retries from durable state.
- The read-only observation validates VM to VMI to sole launcher ownership,
  migration absence, exact PVC ownership, immutable node identity, current
  `state.terminated` container evidence, restart counts, timestamps, and terminal
  restart policy. `lastState`, restarts, status unknown, NodeLost, force deletion,
  404, multiple launchers, and ownership/PVC mismatches remain pause-only.
- Positive controller evidence is canonicalized and appended under the live
  recovery claim. Callers may consume an exact historical receipt in a later
  claim term, but cannot trust a digest supplied in an observation response.
- Rootdisk GC, captured deletion, Failed DataVolume recreation, retained-storage
  claim/ensure/detach/delete, and reserve paths consult exact controller pins and
  refuse work when pin authority cannot be read.
- Added pinned-SSH successor qualification for retained disks. It collects
  interface, address, route, DNS, netplan/networkd, cloud-init instance, MAC, and
  registration evidence using a read-only guest helper. It never cleans legacy
  cloud-init cache. A stable MAC was not persisted because no compatible-image
  fixture demonstrated that MAC matching is required.
- Added the separate, default-off
  `VM_WORKSPACE_REPLACEMENT_RECOVERY_ENABLED` gate and the node-read RBAC required
  to bind launcher evidence to an immutable Node UID.

## Authorized Scope Ruling

The parent approved a signed controller pin reconciliation channel backed by
Kubernetes Lease records after inspection showed that PostgreSQL-only pins were
not visible to autonomous controller cleanup. The approved scope required
durable desired/acknowledged activation and release, observation gating on exact
pin acknowledgement, fail-closed destructive paths, exact idempotent release,
safe storage leaks after failed release, existing signed replay semantics, and
restart/response-loss/stale-release/DB-pin-before-controller-pin race coverage.
Migration `0255`, the orchestrator store/provisioner changes, and Helm RBAC are
the minimum files needed to implement that ruling.

## TDD Evidence

The first integrated retained-storage run produced six failures when new
destructive-operation guards found that the test controller exposed no pin
authority. The fixture was changed to supply an explicit authoritative empty
pin set; pin-specific cases then drove exact-PVC refusal behavior. Further RED
cases covered missing old launchers, `lastState`, restarted containers,
ambiguous migrations, response loss, stale release, and the database pin race.

Final broad command:

```text
PYTHONPATH=src python -m pytest tests/test_vm_controller.py tests/test_vm_retained_storage.py tests/test_vm_remote_operation_real_postgres.py tests/test_vm_workspace_recovery.py tests/test_vm_workspace_recovery_real_postgres.py tests/test_vm_readiness.py -q --tb=short
```

Result: `385 passed, 2 warnings in 172.72s`. The warnings are existing
testcontainers and Pydantic/Python 3.14 deprecations.

Helm verification:

```text
PYTHONPATH=src python -m pytest tests/test_vm_preparation_helm.py tests/test_infrastructure_metering_helm.py -q --tb=short
```

Result: `33 passed`; the final targeted Helm replay was `13 passed`.

Schema verification:

```text
PYTHONPATH=src CONTAINER_ENGINE=podman scripts/schema-snapshot.sh app --check
```

Result: `OK: schema artifacts are up to date.`

After the final receipt-boundary hardening:

```text
python -m ruff check src/orchestrator/services/vm_workspace_recovery.py src/orchestrator/services/vm_workspace_recovery_store.py tests/test_vm_workspace_recovery.py
PYTHONPATH=src python -m pytest tests/test_vm_workspace_recovery.py -q --tb=short
git diff --check
```

Result: Ruff passed, `19 passed`, and the diff check passed.

## Self-review

- Confirmed the observation path never invokes status/identity persistence.
- Confirmed missing and ambiguous runtime evidence cannot create a stop receipt.
- Confirmed duplicate evidence still requires a live exact claim before receipt
  reuse and final release independently rechecks the receipt identity.
- Confirmed pin release is exact and idempotent; a failed release does not clear
  the Lease or its durable retry command.
- Confirmed controller restart and response-loss replay use Kubernetes object
  identity rather than process memory.
- Confirmed destructive paths compare immutable PVC UID, and malformed or
  unavailable Lease listings abort rather than permit deletion.
- Confirmed the unrelated `srw-public-preview-desktop.png` remains untracked and
  untouched.

## Concerns

- Automatic replacement remains disabled by default and should stay disabled
  until a disposable-cluster acceptance run proves the guest image's retained
  disk networking behavior. The implementation deliberately pauses on any
  missing evidence.
- Kubernetes/operator deletion outside the application/controller paths remains
  outside this guarantee, as documented in the issue design.

## Review Fix Round 1

### Changes

- Added a common owner-keyed controller lifecycle boundary with safe same-task
  reentrancy. Pin activation/release, VM create/delete, Failed-DV recreation,
  orphan rootdisk GC, preparation cleanup, and every retained-storage
  claim/ensure/detach/delete now serialize on that boundary.
- Pin activation now validates the exact current PVC and its owning DataVolume
  under the boundary before acknowledging. GC and Failed-DV recreation re-read
  the exact owner, DataVolume UID, PVC UID, and active pins immediately before
  an external delete. Deterministic interleavings prove that a pin cannot ACK
  after either destructive path crosses its admission boundary.
- Unknown PVC identity is now a refusal for GC and Failed-DV recreation.
  Ordinary purge requests without a captured PVC UID leave the disk and its
  Headscale identity intact. Captured rootdisk deletion reacquires the same
  lifecycle boundary and uses UID preconditions.
- Stop evidence now requires exact declared regular/init container coverage,
  one current status per name, an explicit compute container, current
  termination state, empty `lastState`, zero restarts, container IDs, terminal
  reasons, and finished timestamps. The append-only store independently
  validates the complete structure before accepting a receipt.
- Observation now proves the captured PVC is controller-owned by the exact
  DataVolume and that the VM template, VMI, and launcher/compute mount all
  reference that exact name. VM to VMI and VMI to launcher ownership requires
  an explicit `controller=true` owner reference.
- Successor qualification now sends a fresh nonce through pinned SSH and
  requires the guest to return that nonce with its boot ID and guest-derived
  registration response. Complete interface/MAC, address, default route, DNS,
  effective netplan/networkd, and cloud-init instance/cache identity are
  mandatory. The orchestrator no longer synthesizes a registration ID from
  controller identifiers, and the guest helper remains read-only.

### RED Evidence

The initial focused controller/readiness run reported `23 failed, 5 passed`.
Every new negative case failed for the intended permissive behavior: pin ACK
during GC/Failed-DV deletion, deletion with unknown PVC identity, name-only
purge, incomplete/unknown container status, unrelated disk chains, missing
controller owner bits, partial telemetry, and a replayed challenge.

The independent PostgreSQL receipt-validation run reported `4 failed`; each
malformed container set was accepted before the store validator was added.

### GREEN and Verification

Focused amended controller, retained-storage, and readiness tests passed:
`56 passed in 1.14s`. The PostgreSQL receipt tests passed: `5 passed in 9.10s`.

The controller, retained-storage, readiness, and Helm-focused regression run
passed: `296 passed in 13.02s`.

The required Task 7 gate passed:

```text
PYTHONPATH=src python -m pytest tests/test_vm_controller.py tests/test_vm_retained_storage.py tests/test_vm_remote_operation_real_postgres.py tests/test_vm_workspace_recovery.py tests/test_vm_workspace_recovery_real_postgres.py tests/test_vm_readiness.py -q --tb=short
```

Result: `412 passed, 2 warnings in 85.19s`. The warnings are the existing
testcontainers and Python 3.14/Pydantic deprecations.

## Review Fix Round 2

### Evidence Contract Ruling

The installed Kubernetes client schema exposes
`V1ContainerStateTerminated.container_id` as JSON `containerID`. Stop evidence
therefore requires both the outer current `ContainerStatus.containerID` and the
current `state.terminated.containerID`, and requires exact equality. It also
requires an explicit Pod `restartPolicy: Never`; a missing policy is no longer
defaulted. The serialized evidence carries both identities, and the append-only
receipt store independently rejects missing or mismatched values.

The guest qualification helper no longer creates or returns a registration ID.
It echoes the fresh nonce plus canonical boot and systemd machine identities
over the pinned SSH channel. After strict response and network validation, the
orchestrator mints a new server registration UUID, following the existing guest
registration authority. Initial/final attestation excludes the fresh nonce and
rotating server registration ID, while comparing boot ID, machine ID,
runtime/storage identities, and all stable guest network telemetry. Release
persists the final server-minted registration ID.

### RED Evidence

The focused RED run reported `9 failed, 4 passed`. Missing restart policy and
missing/mismatched current termination IDs were accepted; an arbitrary
guest-supplied registration ID was trusted; valid telemetry without that old
field was rejected; a rotating server registration caused a false conflict;
changed guest identity was missed; and the receipt store accepted an
outer/current container-ID mismatch.

### GREEN and Verification

- Focused controller, readiness, and reconciliation contract tests:
  `38 passed`.
- Focused PostgreSQL receipt and stable-attestation tests: `8 passed`.
- Controller, readiness, recovery, and Helm regression tests: `305 passed`.
- Required Task 7 gate: `422 passed, 2 warnings in 89.28s`; warnings are the
  existing testcontainers and Python 3.14/Pydantic warnings.
- Final Helm helper check: `13 passed`.
- Ruff check, Ruff format check for all touched Python files, and
  `git diff --check` passed. The first format check identified three touched
  files; they were formatted and the final check reports all nine files
  formatted.

The final store regression proves that two observations with different nonces
and different server registration IDs release only when boot, machine, and
stable network identity match, and that the final registration ID is the value
persisted into the workspace projection.

## Review Fix Round 3

The installed Kubernetes `ApiClient` deserializes `lastState: {}` as a truthy
`V1ContainerState` whose `running`, `waiting`, and `terminated` fields are all
`None`. The controller now recognizes only that exact empty model/serialized
shape as no previous state. Any populated state, unknown key, or unrecognized
object remains ambiguous and cannot mint stop evidence.

The corrected RED reproduction failed because the valid terminal launcher was
reported as `running` instead of `stopped`. The focused evidence class passed
after the change (`20 passed`), followed by the full controller suite
(`225 passed`). Ruff format/check and `git diff --check` passed.

The required Task 7 gate completed with `425 passed, 1 failed, 2 warnings in
86.40s`. The only failure was the concurrently added Task 8 test
`test_reconciler_production_path_emits_redacted_probe_and_pause_audits`: its
expected `recovery_reason` was `captured_identity_changed`, while the concurrent
telemetry implementation emitted
`captured_workspace_identity_changed_or_ambiguous`. This test and its telemetry
files are outside the Task 7 controller change and were left to Task 8. The two
warnings remain the existing testcontainers and Python 3.14/Pydantic warnings.
