# Safe VM Failed-Creation Retry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Keep execution inline unless the owner requests delegation.

**Goal:** Let eligible failed VM creates use ordinary Resume to retry the same
generation and retained disk without clearing context or bypassing retirement proof.

**Architecture:** A durable retry admission composes with existing job-control and
cleanup authority. The dispatcher reconciles the immutable creation request outside
database locks; authenticated controller replay validates exact disk/generation and
keeps ambiguous side effects protected. A pure policy module separates retry
eligibility/backoff from transaction and transport code.

**Tech Stack:** Python >=3.11, FastAPI, asyncpg/PostgreSQL, Kubernetes/KubeVirt,
Longhorn, pytest and the existing Podman-compatible PostgreSQL test fixtures.

**Spec:** `docs/superpowers/specs/2026-09-19-vm-failed-creation-retry.md`.

**Status (2026-09-20):** Implemented locally through ordinary Resume and public
progress (`5e3ef3231`) on `feat/vm-reliability-20260919`; not pushed or deployed to
main dev. Tasks 1, 2a, 2b, 4 and 5 passed scoped independent review. Task 3 has
reviewed configuration, per-effect issuance, golden/prepared sources and exact
adoption; new/same-owner, inherited and prepared-target attachment now also passed
review (`90eb9f1bc` includes inherited prepared reuse). Immutable partial-creation
cancellation disposition and pre-grant source-hold protection (`67e56398e`,
migration 0266) passed independent review and 94 distinct local checks. Authenticated
controller disposition discovery now enters that frozen cancellation record through
`a15dd4c69`; it is observation-only and grants neither deletion nor completion.
Exact partial Secret/DV/PVC disposal (`f5fe3e037`, corrected by `32588c40a`)
passed independent review and 116 checks. Source-only reconciliation (`7835fc6fc`)
is now also reviewed: immutable source-disposition intent, exact golden/prepared
pin CAS, prepared-allocation receipts, completed retained-clone checks, lost replies
and delayed-publisher fencing. Final source/progress checks: 39 passed; legacy
source/preparation checks: 125 passed; independent concurrency checks: two passed.
The source stage remains a plan and cannot complete the parent. Attachment
settlement and durable typed completion, including source evidence before cache
collection, remain. Controller capability remains absent and admission defaults off. Task 6
live acceptance is open. A2 and A3 code/review are complete locally; their live
gates remain open. B, C and D have their own remaining work and gates.

Task 5 evidence: 107 actual-PostgreSQL tests, 86 projection/compatibility tests,
124 focused frontend tests, production AOT build and independent reviews passed.
A production-bundle Chromium fixture at 1440, 768 and 390 pixels verified bounded
messages, eligible-only Resume, mobile menus, keyboard activation and zero page
overflow; no browser exceptions. These are local results, not VM execution proof.

The dedicated local-path k3d recovery run remains **0/7** at frozen image checkpoint
`e872154b5`: it demonstrated a conservative identity-conflict pause with disk
protection, not successful recovery. That image predates `a15dd4c69` and the other
2026-09-20 checkpoints. No current-branch VM execution, worker execution or Longhorn
acceptance follows from that run.

## Global Constraints

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

## Baseline and file boundaries

Baseline `develop` was `679ea13d3` when this plan was written. Recheck the branch,
remote tip and both Git worktrees before execution. The knowledge-base is a nested
repository and has unrelated work; never stage it wholesale. Use an isolated
worktree for implementation if the primary checkout has concurrent work.

Existing seams:

- `services/job_controls.py::_resume_job_internal`: missing-workspace branch
  currently requests context shedding before requeue.
- `database/postgres.py::queue_job_for_resume` and
  `_queue_job_for_resume_on_conn`: existing guarded Resume transaction/composition.
- `services/vm_provisioner.py::create_vm`: `fresh=False` preserves generation,
  but still writes `provisioned_at` using an unguarded merge. Retry must not copy
  that write blindly; keep its own clocks and use generation-CAS.
- `vm_controller/controller.py::_do_create_serialized`: same-generation 409
  readback, retained-disk reservation/carrier and cloud-init host-key behavior.
- `services/vm_provisioning_cleanup.py`: pending cleanup marker precedes external
  deletion; completion and status normalization are generation-fenced.
- `services/dispatch_guards.py`: generic failed state parks; retry admission must
  be considered before that branch without weakening cleanup precedence.

Keep new policy in `shared/vm_creation_retry.py`, durable operations in
`services/vm_creation_retry_store.py`, and I/O orchestration in
`services/vm_creation_retry.py`. Existing large modules should only gain adapters
and guarded branches. Do not extract unrelated code.

## Task 1: Define retry policy and the wire contract

**Files:**
- Create `src/shared/vm_creation_retry.py`.
- Create `tests/test_vm_creation_retry.py`.
- Read `src/shared/vm_lifecycle_auth.py` and the spec.

**Interfaces:**
- `retry_block_reason(facts: Mapping[str, object]) -> str | None` consumes facts
  assembled by authority-aware store/controller code, not public request fields.
- `retry_delay_seconds(attempt: int, jitter_fraction: float = 0.0) -> float`.
- `VM_CREATION_RETRY_PROTOCOL = 1`; typed request fields are `request_id`,
  `job_id`, `provision_generation`, `request_digest`, `expected_pvc_uid`, and
  `claim_token`. MAC envelope/correlation uses the existing lifecycle primitives.

- [x] Write failing pure-policy tests, including this complete refusal matrix:

```python
import pytest
from shared.vm_creation_retry import retry_block_reason, retry_delay_seconds

@pytest.mark.parametrize("field,reason", [
    ("deadline_valid", "job_admission_expired"),
    ("canonical_request_proven", "creation_request_unproven"),
    ("generation_matches", "generation_changed"),
    ("disk_matches", "retained_disk_changed"),
    ("predecessor_settled", "predecessor_cleanup_pending"),
    ("control_available", "job_control_busy"),
    ("recovery_clear", "workspace_recovery_held"),
    ("controller_capable", "retry_protocol_unavailable"),
])
def test_unproven_retry_is_refused(field, reason):
    facts = dict.fromkeys([
        "deadline_valid", "canonical_request_proven", "generation_matches",
        "disk_matches", "predecessor_settled", "control_available",
        "recovery_clear", "controller_capable",
    ], True)
    facts[field] = False
    assert retry_block_reason(facts) == reason

def test_missing_evidence_does_not_admit():
    assert retry_block_reason({}) is not None

def test_transport_backoff_is_bounded():
    assert [retry_delay_seconds(i) for i in range(1, 9)] == [
        5, 10, 20, 40, 80, 160, 300, 300,
    ]
    assert retry_delay_seconds(8, 0.2) == 360
```

- [x] Run `.venv/bin/python -m pytest tests/test_vm_creation_retry.py -q`
  with `PYTHONPATH=src`; confirm missing-module failure before implementation.
- [x] Implement strict evidence checks (`is True`, missing/invalid is refusal),
  deterministic reason precedence, wire-field validation and backoff. Reject
  attempt <1 and jitter outside 0..0.2. Canonical hashes exclude credentials,
  transport signatures and timestamps; retain image/resources/network/storage/
  preparation/initialization/config identity that determines the created workspace.
- [x] Add all-proven, malformed UUID/hash, changed request digest and invalid
  backoff-input cases; rerun and commit `feat: define VM creation retry contract`.

## Task 2a: Freeze initial create intent before transport

**Files:**
- Create `src/orchestrator/services/vm_creation_request.py` and
  `tests/test_vm_creation_request.py`.
- Modify `src/orchestrator/services/vm_provisioner.py`; extend
  `tests/test_vm_provisioner.py`.

**Interfaces:** Factor `build_vm_creation_request(...) -> dict` from the existing
unsigned HTTP payload construction, using the same explicit field names. Add
`capture_vm_creation_request(...)` for generation-CAS persistence of versioned
canonical request/digest before the first POST. Its stored identity includes a
separate authenticated controller configuration digest; absent controller identity
means the record is not yet eligible for retry, not permission to infer defaults.
Task 3 supplies the authenticated configuration resolution/issuance contract.

- [x] Write failing tests for initial snapshot before POST, failed CAS causing no
  POST, identical deferred-create replay after current network/project options
  change, and stale-generation refusal.
- [x] Implement a shared unsigned request builder using Task 1's canonical digest.
  Persist exact caller-owned options under the current generation before I/O; no
  raw credentials/signatures enter the stored snapshot. Preserve public call
  compatibility and old-controller behavior while retry admission is disabled.
- [x] Add structured authenticated rejection versus transport-unknown facts without
  treating a historical error string as authority. A rejected/stale context write
  must not authorize side effects. Replay captured inputs; never overwrite the
  first snapshot with freshly resolved defaults on a poll.
- [x] Run focused creation/provisioner tests and commit this foundation. Do not
  advertise end-to-end retry readiness: controller effective config/defaults and
  original issuance fencing must be completed in Task 3 before admission is enabled.

## Task 2b: Persist immutable admission and compose guarded Resume

**Files:**
- Create `src/orchestrator/database/migrations/app/0257_vm_creation_retries.sql`.
- Create `src/orchestrator/services/vm_creation_retry_store.py`.
- Modify `src/orchestrator/database/postgres.py` and `schema_current.sql`.
- Modify `src/orchestrator/services/vm_workspace_recovery_store.py` to extract
  transaction-composable cleanup admission while retaining the existing wrapper.
- Create `tests/test_vm_creation_retry_real_postgres.py`.
- Extend `tests/test_queue_job_for_resume.py` as needed for transaction composition.

Use the next free migration number if 0257 has been allocated meanwhile; update
the plan/schema references in the same change. Preserve existing schema discovery
and migration-head contracts.

**Interfaces:** `VMCreationRetryStore(db)` provides:

```python
async def admit_on_conn(self, conn, *, job_id: str,
                        expected_generation: str, request_id: str,
                        proposal: dict) -> dict: ...
async def claim_due(self, *, limit: int) -> list[dict]: ...
async def authorize_controller(self, *, request_id: str,
                               claim_token: str,
                               observed: dict) -> dict: ...
async def apply_observation(self, *, request_id: str, claim_token: str,
                            expected_revision: int, observation: dict) -> bool: ...
async def request_cancel_on_conn(self, conn, *, job_id: str,
                                 expected_generation: str) -> bool: ...
```

Every returned record contains the immutable fields from Task 1 plus `state`,
`revision`, `claim_token`, `next_probe_at`, `reason` and timestamps. `proposal`
contains frozen canonical options, their digest, expected PVC UID, and predecessor
admission reference; these are reconstructed server-side, never trusted from Resume.

- [x] Reuse the real PostgreSQL/schema fixtures from
  `tests/test_vm_preparation_retirement.py`. Add tests proving two concurrent Resume
  admissions return one request ID, stale generation/digest is refused, and the
  existing process-zero trigger still refuses raw context shedding.
- [x] Run `PYTHONPATH=src .venv/bin/python -m pytest
  tests/test_vm_creation_retry_real_postgres.py -q`; require failures at the missing
  relation/API, not connection errors or fixture skips.
- [x] Add the table with foreign keys, unique `(job_id, provision_generation)`,
  constrained states and immutable identity/request columns. Adapt this core shape
  to the repository's migration conventions:

```sql
CREATE TABLE vm_creation_retries (
    request_id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES jobs(id),
    provision_generation uuid NOT NULL,
    request_digest text NOT NULL,
    canonical_request jsonb NOT NULL,
    expected_pvc_uid text,
    predecessor_cleanup_admission_id uuid
        REFERENCES vm_workspace_cleanup_admissions(id),
    state text NOT NULL CHECK (state IN
        ('queued','reconciling','succeeded','attention','cancel_requested','settled')),
    revision bigint NOT NULL DEFAULT 0,
    claim_token uuid,
    claim_expires_at timestamptz,
    next_probe_at timestamptz NOT NULL DEFAULT now(),
    transport_outage_started_at timestamptz,
    reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (job_id, provision_generation)
);
```

- [x] Compose `admit_on_conn` with the existing queue-first Resume transaction.
  Retain job-control/completion checks and `void_completion_decision=False` for
  this infrastructure-only retry. Under the same transaction verify the immutable
  Job deadline and predecessor receipt/cleanup chain, insert-or-read the request,
  and queue the job while preserving its VM context. Use DB time for due/lease
  comparisons. Do not call a helper that opens a second independent transaction.
- [x] Add controller authorization and CAS observation updates. Record the active
  controller create reservation durably using existing cleanup-admission/carrier
  identity; add a reference column if required by that established schema. An
  expired observer claim cannot settle that reservation or mint a successor.
- [x] Make identity columns immutable in PostgreSQL, enforce digest/JSON/state/claim
  consistency, use UUID PVC identities as in cleanup admissions, and add a due index
  plus durable backoff/boot-accounting fields. Compose authorization and the exact
  adoption reservation in one transaction with the established owner/PVC then
  queue/job lock order. Integrate cancellation into actual pinned/stateless control
  transactions; do not rely only on a background status read.
- [x] Test concurrent completion/cancel/cleanup/recovery admission, claim takeover,
  late observations and duplicate admission after a lost response. Assert one
  outcome, intact receipts, no partial requeue and no lock-order deadlock. Generate
  and check the schema snapshot; rerun tests and commit the schema/store slice.

## Task 3: Authenticate and fence controller retry actuation

**Files:**
- Create `src/orchestrator/routers/vm_creation_retry_authority.py`.
- Modify `src/orchestrator/main.py` for router wiring.
- Modify `src/vm_controller/controller.py` and `src/shared/vm_creation_retry.py`.
- Extend `tests/test_vm_controller.py`; create
  `tests/test_vm_creation_retry_authority.py`.
- Update endpoint-auth inventory/contracts using the existing repository workflow.

**Interfaces:** Add authenticated `POST /api/internal/vm-creation-retries/authorize`
using lifecycle MAC operation `creation_retry_authorize` and correlation checking.
Input is Task 1's retry identity plus controller-observed VM/disk/adoption identity;
output is a typed allow/blocked disposition bound to the existing reservation.
Controller capability will advertise `vm_creation_retry_protocol: 1` only after
all source, attachment, cancellation and acceptance gates pass. The dedicated
`/vm-creation/create` route prevents old replicas from accepting an envelope they
do not understand. Current migrations are 0257–0260; attachment reserves 0261.

- [ ] Write tests for MAC/correlation/generation/digest/disk mismatches, expired
  claims, cancellation-before-authorization and missing capability. Assert refusal
  occurs before Secret, rootdisk or VM writes.
- [ ] Run the authority tests and controller retry subset, observing failure before
  adding the route and controller branch.
- [ ] Bind successful authorization to the existing rootdisk adoption reservation
  under controller lifecycle serialization; hold unresolved side effects through
  response loss and cancellation. Extend both request validation and the existing
  cleanup-admission checks so a newly opened hold cannot be crossed between
  authorization and VM create. Revalidate exact disk identity at the existing
  pre-create check, not only during the earlier status probe.
- [ ] Bind initial creates as well as retries to immutable issuance authority and
  authenticated controller effective-configuration identity. Protocol capability
  must not be advertised until both paths are fenced. For cancellation: revoke
  unissued admission definitively; for an exact issued VM, settle adoption before
  ordinary exact retirement; for unknown issuance, retain its reservation and
  schedule reconciliation. Do not let the open adoption block its own cancellation
  indefinitely or release it based on a lease/absence observation.
- [ ] Implement same-generation replay with these branches:

```text
retry identity invalid or capability absent -> typed refusal, no side effects
cleanup/recovery/control blocks authority -> retain request, report blocked
exact VM already admitted -> validate owner/generation/PVC/pin, return identity
VM absent, create reservation current -> create with frozen request and exact disk
create result unknown -> preserve reservation; reconcile before another actuation
conflicting VM/PVC or completed adoption without its VM -> attention, never recreate
cancel after create admission -> reconcile then existing exact retirement path
```

- [ ] Exercise reply loss after Kubernetes acceptance, delayed acceptance during
  cancellation, controller restart with an unresolved carrier, same-name foreign VM,
  retained PVC replacement, and completed-adoption replay. Assert retries never
  delete the live VM's cloud-init Secret or rotate its admitted host-key pin.
  The existing exception cleanup path needs explicit regression coverage here.
- [ ] Run controller/authority tests and endpoint-auth checks; commit the protocol
  and controller slice. Do not claim cross-replica safety from the process-local
  lifecycle lock alone; database reservation tests must demonstrate exclusion.

## Task 4: Reconcile requests through the dispatcher

**Files:**
- Create `src/orchestrator/services/vm_creation_retry.py`.
- Modify `src/orchestrator/services/vm_provisioner.py` and
  `src/orchestrator/services/dispatch_guards.py`.
- Modify `src/orchestrator/main.py` for due reconciliation and dispatch ordering.
- Extend `tests/test_vm_creation_retry.py`, `tests/test_vm_provisioner.py`,
  `tests/test_dispatch_guards.py`, and `tests/test_vm_provisioning_cleanup.py`.

**Interfaces:** `reconcile_creation_retry(record: dict, *, store, provisioner,
now: datetime) -> str` returns the committed retry state. Add a provisioner method
`retry_create_vm(record: dict) -> dict` that transmits the frozen request plus
retry authority without generating a new provision context or resetting clocks.
The ordinary initial-create signature remains compatible.

- [x] Write tests with `AsyncMock` collaborators: transport timeout before/after
  acceptance, no I/O before `next_probe_at`, late token rejection, cancellation,
  confirmed same-generation creation and an active retirement-pending marker.
- [x] Run the targeted tests and confirm missing retry behavior fails.
- [x] Implement this orchestration order; every store update uses the captured
  request/revision/claim, and external calls run after transaction exit:

```text
claim due request -> read current control/generation/deadline
cancel/retirement pending -> settle through existing lifecycle, never create
observe authenticated controller identity and outstanding create reservation
exact admitted VM -> persist identity, mark admission success, leave Ready to readiness
unknown transport -> record backoff; at 900s retain authority in attention
eligible unresolved create -> replay same immutable request under controller authority
authenticated capacity wait -> remain queued without boot/worker attempt increment
stale generation/token or conflicting identity -> refuse update/retain attention
```

- [x] Give retry reconciliation precedence over the generic failed-state park, but
  preserve cleanup/recovery/cancellation precedence over retry. Persist transport
  outage start once, clear it after authenticated progress, and do not count
  successful reconciliation of an already-counted generation as another boot.
- [x] Test restart/backoff continuity, concurrent observers, no endpoint promotion
  before readiness, capacity waits longer than 2700 seconds on this retry path,
  and generation change during a request. Run the focused suites and commit.

## Task 5: Wire ordinary Resume and safe public progress

**Files:**
- Modify `src/orchestrator/services/job_controls.py`,
  `src/orchestrator/services/job_projection.py`, and their dependency protocols.
- Modify `helm/values.yaml`, `helm/values.schema.json` and
  `helm/templates/configmap.yaml`; create `tests/test_helm_vm_creation_retry.py`.
- Extend `tests/test_resume_missing_workspace.py`,
  `tests/test_job_control_operations.py`, `tests/test_job_projection.py`, and
  `tests/test_vm_creation_retry_real_postgres.py`.

**Interfaces:** Before the missing-workspace shedding branch, eligible VM creation
failures invoke Task 2's admission in the existing control transaction. Return the
existing queued response shape plus `vm_creation_retry_request_id`; repeated
requests return the same ID. Noneligible runtime/legacy cases retain guarded behavior.

- [x] Add failing regressions for the d843 incident shape, duplicate Resume, an
  expired canonical Job deadline, a changed manifest, unknown predecessor identity,
  missing controller capability and a genuine executed runtime requiring retirement.
- [x] Implement the adapter without public client-supplied disk/generation authority.
  Reconstruct evidence from immutable admission and cleanup records. If the legacy
  create request cannot be reconstructed exactly, return a structured 409 with
  `creation_request_unproven`; do not guess from current defaults or error text.
- [x] Add projection reasons for queued/reconciling/attention states. Validate that
  error details omit credentials, endpoint coordinates and raw controller bodies.
  Keep existing Resume authorization and feedback behavior; infrastructure-only
  requeue must preserve checkpoint/completion state.
- [x] Add `orchestrator.vmProvisioning.creationRetryEnabled: false` and render
  `VM_CREATION_RETRY_ENABLED`. Test false by default, explicit true, invalid value
  rejection and admission refusal while disabled. Existing request reconciliation
  remains active when the flag is turned off; test that rollback behavior too.
- [x] Run the Resume/projection suites and real-PostgreSQL race tests. Assert job ID,
  generation, original PVC identity and deadline are unchanged through successful
  admission; context-shedding spies must not be called. Commit the integration.

## Task 6: Acceptance, deployment contract and issue closeout

**Files:**
- Create `scripts/vm-creation-retry-scenario.py` and
  `tests/test_vm_creation_retry_scenario.py`.
- Update this plan, its spec, the roadmap and the owning issue in the KB.
- Update schema-capability declarations/test fixtures discovered by migration
  replay, without weakening their expected-head assertions.

**Interfaces:** Scenario CLI takes explicit `--context`, `--namespace`,
`--job-id` (disposable test owner only), `--expected-pvc-uid` and `--output`.
It refuses a non-disposable owner. It uses supported APIs, records identities,
image digests, schema head, flags, outcomes and redacted timestamps, and never
uses raw deletion or fabricated receipts to make its assertions pass.

- [ ] Add failing harness tests for refusal of a nondisposable owner, context/PVC
  mismatch, failure evidence persistence and supported cleanup on interruption.
- [ ] Implement the exact disposable scenario from the spec: retained sentinel,
  rejected successor while old cleanup is open, completed old cleanup, ordinary
  Resume, identical successor generation/PVC, Ready, worker claim and execution.
  Add lost-response and concurrent-cancel variants; include original file hash
  and final physical VM/VMI/PVC state in evidence.
- [ ] Run scoped backend tests, real PostgreSQL tests, Ruff, import contracts,
  endpoint-auth/runtime-coordinate inventories and schema replay/check. Reuse the
  established recovery/cleanup regression gate; do not replace it with mocks.
  Example focused invocation:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_vm_creation_retry.py \
  tests/test_vm_creation_retry_real_postgres.py \
  tests/test_vm_creation_retry_authority.py \
  tests/test_vm_creation_retry_scenario.py \
  tests/test_helm_vm_creation_retry.py \
  tests/test_vm_provisioner.py tests/test_vm_controller.py \
  tests/test_vm_provisioning_cleanup.py tests/test_dispatch_guards.py \
  tests/test_resume_missing_workspace.py tests/test_job_control_operations.py \
  tests/test_job_projection.py -q
git diff --check
```

- [ ] Run the disposable live scenario against an explicitly selected test
  environment. Missing prerequisites are a pending gate, not success. Deployment
  order is additive schema, compatible controller, then orchestrator caller.
  Record compatibility and rollback behavior with unresolved requests present.
- [ ] Commit the acceptance/tooling evidence. Update the roadmap with separate
  code/test/push/deployment/live columns; leave automatic recovery flags disabled.
- [ ] After deployment, re-inspect the actual d843 job and attempt only its supported
  ordinary Resume when current authority and deadline permit. Record execution
  evidence or the exact remaining refusal. Do not close historical repair merely
  because the disposable scenario passed. Continue to A2's phase-deadline design
  once A1's deliverable and remaining operational disposition are explicit.

## Plan self-review

- [x] Scope limited to A1; later milestones remain visible in the roadmap.
- [x] All spec requirements mapped to policy, store, controller, dispatcher,
  Resume/projection or acceptance tasks.
- [x] Immutable request/generation/disk and stale-token contracts are consistent.
- [x] Cancellation, ambiguous creates, legacy provenance and expired admissions
  have explicit outcomes; absence is never substituted for stop proof.
- [x] Local verification, live acceptance and historical-job repair are separate.

The snippets above define new interfaces and test seeds, not existing implemented
APIs. Implement each task with a failing test first, review its completed contract,
then commit that slice. No application tests were run while writing this plan.
