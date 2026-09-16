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
