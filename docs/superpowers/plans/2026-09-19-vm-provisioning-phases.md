# VM provisioning phases and resilient waiting

Status (2026-09-20): A2 and A3 code and independent reviews are published to
develop through `11a33debf`; live acceptance is pending. See the
[delegation handoff](2026-09-20-vm-delegation-handoff.md).
A3 checkpoints are `59dbf3e19` and `2e8a7c194`.
This follows the VM reliability roadmap, A2 and A3. A1 owns all creation
issuance, reservation, cancellation and frozen-request authority.

## Decisions

Boot time begins at the first authenticated, exact VMI Running observation,
persisted once per generation. VM creation, Pending/Scheduling, rootdisk cloning,
placement waits and credential-runtime possibility do not start it. Readiness
continues to require SSH attestation, host-key pin and successful initialization.

Rootdisk import/clone gets a 2700-second no-progress threshold, followed by
retained attention. Increasing progress or a previously unseen forward disk stage
counts as progress; polls, resourceVersion changes and phase oscillations do not.
There is no total timeout for a progressing clone. Consumer-dependent disk waits
are placement waits. Placement/capacity wait indefinitely, subject to the existing
immutable explicit Job deadline; an overdue warning at 3600 seconds is informational.
Boot keeps VM_PROVISION_TIMEOUT_S (default 600); initialization keeps 900 + 60.
Cleanup retains its existing authority and precedence. Unknown or conflicting
phase identity cannot authorize recycling.

Version 1 observations bind owner, generation, namespace, exact VM UID, nullable
exact VMI/DV/PVC UIDs, disk mode and normalized disk/VMI phase. The controller
must validate Kubernetes ownership and association before signing observations.
The pure reducer consumes authenticated observations only; the database adapter
provides generation/revision CAS and control/recovery fences. Null identities
may bind once on allocation, never substitute for the loss of an already bound
identity. A VMI replacement within a generation is attention, never a new budget.

Clock state is bounded JSON persisted in context.vm.provisioning. Database time
is authoritative. Observers capture revision before I/O; stale results lose CAS.
The pure reducer validates stored clock state and identity before decisions.
This state gives permission only to request existing guarded boot-timeout cleanup,
never to delete directly or promote readiness.

A3 counts one authenticated VM admission per generation. Deferred creates, polls,
lost responses, same-generation replay and readiness probes consume zero extra
VM/worker attempts. Durable due/backoff and transport attention reuse A1's claim
and issuance record; controller transport ambiguity never proves non-issuance.
Continuous outage at 900 seconds produces retained attention; authenticated
capacity clears that outage clock. No explicit deadline is extended by Resume.

## Implementation and validation

- [x] Pure validated phase reducer and decisions, with progression, stall,
  placement, once-only boot, malformed/future state and identity tests.
- [x] Controller exact-identity phase evidence, signed by the existing envelope;
  test ownership conflicts, retained PVC without DV, Pending versus Running,
  and consumer-dependent disk wait.
- [x] Atomic database observation CAS at the authenticated provisioner boundary;
  real PostgreSQL races, restart, cancellation, generation/recovery fences.
- [x] Dispatcher attention/boot decisions and safe legacy-controller behavior;
  integration tests assert no deletion for clone/placement/unknown evidence.
- [x] A3 durable waiting, one admission count, immutable deadline enforcement;
  real PostgreSQL concurrency and lost-response tests, ordinary capacity >26h.
- [x] A2 Helm/projection documentation and independent review; focused suites and
  real-PostgreSQL phase/cleanup races passed.
- [ ] Disposable k3d acceptance; record the full Longhorn storage gate separately.

Full implementation seams and reconnaissance evidence are retained in the local
work report `.superpowers/a2-a3-implementation-seams.md`. A2 is integrated locally and its independent review passed. Live acceptance
remains pending. A3 removes capacity expiration, counts each authenticated legacy
admission once while leaving protocol accounting with A1, and resets only at
verified Ready. Paused infrastructure waits use their original execution
deadline through the existing guarded cancellation transaction. Migration 0262
adds a bounded persistent scan cursor so older blocked cancels cannot starve
later expired jobs. The deadline/control/migration-head suite passed 81 tests;
independent actual-PostgreSQL starvation and cursor-race checks passed.
