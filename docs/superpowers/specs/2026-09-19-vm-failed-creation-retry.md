# Safe retry of failed VM creation

**Status:** Planned, 2026-09-19. First slice of the owner-approved VM reliability
roadmap; no application change or live job repair is included in this document.

**Roadmap:** `knowledge-base/knowledge/issues/vm_reliability_roadmap_2026_09_19.md`.
**Incident:** `knowledge-base/knowledge/issues/legacy_job_resume_missing_vm_retirement_authority.md`.

## Problem and outcome

Ordinary Resume sees a missing VM and tries to shed its context before queuing a
fresh generation. For job `d8436836-b7a3-407d-a395-8f456322dfdd`, the predecessor
was genuinely retired, its disk retained, and its cleanup completed; a replacement
create had already failed against the then-open cleanup admission. That replacement
has a generation but no authenticated VM UID or retirement receipt. Clearing it
correctly fails the database's process-zero guard.

Add an authorized creation retry that preserves that generation, immutable create
parameters and exact retained disk. Ordinary Resume queues it for the dispatcher
without claiming the failed generation has been retired. Readiness and worker
execution still require the existing workspace authority checks.

## Chosen approach and alternatives

Use the existing same-generation `create_vm(..., fresh=False)` capability behind
a durable retry admission and exact controller replay checks. This is smaller than
a general legacy-runtime repair system and does not need a forged stop receipt.
Extend the established job-control and cleanup authorities rather than introducing
an independent lock hierarchy or bypass route.

Rejected: clearing VM context/new generation on absence alone loses authority and
can race an accepted create. Creating another job abandons the original lifecycle.
An explicit controller cancellation/non-issuance proof could authorize retirement,
but broad repair of unknown historical generations is a separate follow-up; this
slice must refuse such cases rather than pretend to solve them.

## Scope and invariants

- Same-cluster Job VM creation only; preserve `provision_generation` on retry.
- Preserve exact retained PVC UID and immutable canonical provisioning options.
- Never clear `context.vm`, invent process-zero evidence, or reuse a predecessor's
  receipt as proof that its successor stopped.
- No network, controller or Kubernetes I/O while database row locks are held.
- A lost reply, empty request counter, missing VM/VMI or expired lease is not proof
  that creation or execution did not occur.
- Preserve existing job-control, completion, recovery-hold and cleanup fences.
- Retry must not extend an immutable Job deadline or change its execution manifest.
- Recovery flags remain false; this path is ordinary provisioning, not outage recovery.
- Historical ambiguous attempts remain blocked with an explicit, actionable reason.

For retained-disk attempts, predecessor retirement and completed cleanup must match
the original workspace and disk. A disk with the same name but another UID is not
acceptable. Do not infer disk identity from the failed replacement's empty fields.
Reconstruct it from durable predecessor/cleanup records and revalidate it through
the authenticated controller. If that provenance is unavailable, refuse the repair.

## Durable state and admission

Persist one immutable retry request per `(job_id, provision_generation)` in a
dedicated application table `vm_creation_retries`. It contains `request_id`,
canonical unsigned request data/hash (no credentials), expected retained PVC UID,
predecessor cleanup-admission reference when applicable, timestamps, revision,
claim token, next-probe time and structured outcome/reason. Preserve historical
records; a new generation gets a different row, not an overwritten identity.

State transitions:

```text
queued -> reconciling -> succeeded
                    -> attention
                    -> queued (retryable infrastructure wait)
queued/reconciling/attention -> cancel_requested -> settled
```

`succeeded` means exact VM admission/reconciliation succeeded, not that the job
completed or even reached Ready. `settled` requires definitive reconciliation of
in-flight create and the existing cancellation/cleanup path. Claim expiry permits
another observer to reconcile the same immutable request; it never proves absence,
releases a pending create's authority, or permits a new generation.

Admission is a single transaction composed with the existing Resume/job-control
transaction and queue locking order. Compare job status/version, generation,
canonical request hash, lifecycle control claim, explicit deadline, open recovery
and cleanup holds, and predecessor evidence. Repeated Resume returns the existing
request ID. Competing cancellation/completion either wins before admission or
leaves a durable cancel request that reconciliation must settle.

Existing creation failures need a structured reason going forward. Allow only
explicit creation failures/waits with a recoverable request; do not whitelist
arbitrary strings containing `409` or accept an unauthenticated response. Legacy
records require reconstructed durable provenance plus fresh authenticated evidence;
historical error text and `total_requests=0` alone are insufficient.

## Controller and dispatcher contract

Before side effects, the controller validates the current retry admission over the
existing authenticated lifecycle channel and binds the request to the existing
rootdisk adoption/cleanup authority. The reservation remains unresolved while a
create may still complete. A cancellation or stale token denies a not-yet-admitted
create; an already-admitted create must be reconciled and, if needed, retired by
the existing exact-generation path. Do not complete a reservation on transport
timeout or interpret an expired coordinator claim as controller completion.

Revalidate owner, generation, canonical request digest, exact disk UID, open holds
and cleanup state at the actuation boundary. Reuse existing rootdisk reservations
and controller cleanup carriers; do not create a second independent disk lock.
All retry create paths, including replay after restart, obey this contract.

If an exact same-generation VM exists, reconcile its immutable identity and return
it without replacing its cloud-init Secret or host-key pin. If a conflicting VM,
different disk, active cleanup or recovery hold exists, retain the request with a
typed blocked reason. A plain Kubernetes name collision is not sufficient identity.
Never purge/reclone a retained rootdisk to make a retry succeed.

The dispatcher claims due retry requests before ordinary `failed -> PARKED` logic,
performs authenticated I/O outside the transaction, and commits through revision
and claim-token CAS. It reuses frozen canonical options, not changing project
defaults. Lost replies trigger status/reconciliation before same-generation replay.
Stale responses cannot overwrite a successor or clear retirement metadata.

Use durable transport backoff of 5, 10, 20, 40, 80, 160, then 300 seconds (plus
bounded jitter up to 20%). After 900 seconds of unresolved controller transport,
move this explicit retry to `attention`, preserving disk/admission and allowing
the owner to request another observation cycle. This is not a capacity timeout:
an authenticated capacity wait remains queued and uses the same capped polling
backoff. A later capacity wait does not inherit a spent transport-outage clock.
Boot attempts and worker execution attempts do not increment for observation or
replay. Integrate the successful admission with existing provisioning accounting
exactly once; an already-counted generation cannot be counted again.

## Public behavior and compatibility

Reuse ordinary Resume authorization and its response envelope. Successful retry
admission returns queued plus the durable request ID; response loss is idempotent.
Expose safe reasons such as `vm_creation_retry_pending`, `vm_creation_retry_blocked`,
and `job_admission_expired` through existing projections. Do not expose credentials,
raw endpoints or controller response bodies. Leave generic failures and true
runtime retirement on their existing guarded paths.

Use an additive migration and an explicit controller protocol capability for retry
validation. Deploy schema/controller support before callers. If capability is
missing, Resume explains the unsupported repair and keeps the existing job intact.
Add `VM_CREATION_RETRY_ENABLED=false`, rendered from
`orchestrator.vmCreationRetry.enabled` in Helm, to gate new retry admission only.
Enable it after the disposable acceptance gate; reconciliation of existing requests
must not depend on this flag. On rollback, stop new retry admission but continue reconciling admitted operations;
do not roll back to an image that cannot recognize unresolved retry records.

## Acceptance and limits

Required local tests cover duplicate Resume on two replicas, generation/disk drift,
late old responses, cancel/completion/recovery races, create response loss before
and after API acceptance, controller restart, cleanup-carrier replay, exact key-pin
preservation, expired Job admission and unprovable legacy records. Use real
PostgreSQL for transaction/lock/trigger tests, not only mocks.

On a disposable workspace, create an uncommitted sentinel, retire the predecessor
through supported authority, reproduce replacement rejection while cleanup is open,
settle cleanup, then Resume normally. The same replacement generation and disk must
reach Ready, acquire a worker lease and execute while the sentinel survives. Repeat
with a lost create response and concurrent cancellation; verify no duplicate runtime
or successor corruption and no deletion of retained data.

The historical `d8436836…` job is an operational follow-up, not a test fixture to
mutate during development. Recheck its deadline, canonical request, provenance and
live state after deployment. If expired or ambiguous, record the supported refusal
and remaining repair need. Do not claim this issue closed because a disposable job
passed while the original attempt remains without a supported disposition.

Phase-aware deadlines, general capacity/controller waiting, automatic outage
recovery rollout, idle release/wake, and resource reservations have separate roadmap
slices. This design does not silently expand their scope or mark them complete.
