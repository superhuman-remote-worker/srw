# Durable VM Workspace Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve stateless VM jobs through temporary workspace outages by fencing execution, retaining the exact disk and checkpoint, reconciling for 15 minutes, and pausing safely when continuation cannot be proven.

**Architecture:** PostgreSQL owns one durable recovery operation per canonical workspace and one participant row per affected job. Exact queue-token fences, per-claim attempt receipts, controller retention pins, and immutable stop evidence prevent stale workers or cleanup from crossing the recovery boundary. A leader-gated reconciler performs bounded external observations and commits results only through version and claim-token compare-and-swap updates.

**Tech Stack:** Python 3.12+, FastAPI, asyncpg/PostgreSQL 15+, KubeVirt, Longhorn, Helm, React/TypeScript Cockpit, pytest, Podman/testcontainers.

**Spec:** `knowledge-base/knowledge/issues/durable_vm_workspace_recovery.md`

## Global Constraints

- Initial automatic recovery covers same-cluster stateless VM jobs with a known root PVC UID.
- Recovery never deletes or creates a VM in version 1; it reconnects the original runtime or validates an infrastructure-created replacement.
- The default deadline is immutable `first_observed_at + 900 seconds`; duplicate events and retries never extend it.
- Queue park advances lease token `N` to `N+1`, preserves watermarks and prior genuine failures, and makes late saves and completion reports stale.
- A missing attempt row, Pod/VMI absence, lease expiry, `ReadWriteOnce`, or a new boot ID is never proof that prior execution stopped.
- Automatic replacement recovery stays disabled unless exact old-incarnation stop evidence and retained-disk networking pass their acceptance gates.
- Missing or ambiguous execution, tool, checkpoint, identity, membership, or stop evidence produces an attention pause while retaining the hold.
- Network, SSH, Kubernetes, and controller reads never occur while database row locks are held.
- `VM_WORKSPACE_RECOVERY_ENABLED` and `VM_WORKSPACE_REPLACEMENT_RECOVERY_ENABLED` default to false.
- Preserve unrelated local changes, including `srw-public-preview-desktop.png` and the nested knowledge-base worktree.

---

### Task 1: Durable Recovery Schema and Shared Contract

**Files:**
- Create: `src/orchestrator/database/migrations/app/0249_vm_workspace_recovery.sql`
- Create: `src/shared/workspace_recovery.py`
- Create: `src/orchestrator/services/vm_workspace_recovery_store.py`
- Create: `tests/test_vm_workspace_recovery_real_postgres.py`
- Modify: `src/orchestrator/database/schema_current.sql`

**Interfaces:**
- Produces: `WorkspaceRecoveryCode`, `WorkspaceRecoveryDisposition`, `RecoveryAttemptDisposition`, and `workspace_recovery_enabled()` in `shared.workspace_recovery`.
- Produces: `VMWorkspaceRecoveryStore` methods `admit_hold`, `get_attempt_disposition`, `record_bundle_authorized`, `claim_due`, `apply_observation`, `pause_for_attention`, and `release_recovered`.
- Produces PostgreSQL relations `vm_workspace_recoveries`, `vm_workspace_recovery_jobs`, `worker_batch_attempts`, `vm_workspace_recovery_requests`, `vm_workspace_recovery_stop_receipts`, `vm_workspace_recovery_probe_slots`, and `vm_workspace_recovery_retention_pins`.

- [ ] **Step 1: Write migration and shared-contract tests that fail because the tables and module are absent**

```python
def test_workspace_recovery_disposition_wire_shape() -> None:
    disposition = WorkspaceRecoveryDisposition.hold_committed(
        operation_id=UUID("00000000-0000-0000-0000-000000000001"),
        accepted_lease_token=27,
        hold_lease_token=28,
        code=WorkspaceRecoveryCode.RUNTIME_NOT_READY,
    )
    assert disposition.as_error_detail()["recovery"] == {
        "version": 1,
        "action": "hold_committed",
        "operation_id": "00000000-0000-0000-0000-000000000001",
        "accepted_lease_token": 27,
        "hold_lease_token": 28,
    }

async def test_schema_allows_only_one_unresolved_recovery_per_owner(app_pg) -> None:
    await insert_recovery(app_pg, owner_id=OWNER_ID)
    with pytest.raises(asyncpg.UniqueViolationError):
        await insert_recovery(app_pg, owner_id=OWNER_ID)
```

- [ ] **Step 2: Run the new tests and confirm missing imports/relations cause the failures**

Run: `PYTHONPATH=src python -m pytest tests/test_vm_workspace_recovery_real_postgres.py -q --tb=short`

Expected: FAIL because `shared.workspace_recovery` and migration 0249 do not exist.

- [ ] **Step 3: Implement the SQL schema with immutable identities and partial uniqueness**

```sql
CREATE UNIQUE INDEX vm_workspace_recoveries_one_open_owner
ON vm_workspace_recoveries (owner_kind, owner_id)
WHERE resolved_at IS NULL;

CREATE UNIQUE INDEX vm_workspace_recovery_jobs_one_open_job
ON vm_workspace_recovery_jobs (job_id)
WHERE resolved_at IS NULL;

CREATE TABLE worker_batch_attempts (
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    lease_token bigint NOT NULL,
    claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_attempt integer NOT NULL CHECK (claimed_attempt > 0),
    protocol_version integer NOT NULL DEFAULT 1 CHECK (protocol_version = 1),
    bundle_authorized_at timestamptz,
    authority_digest text,
    disposition jsonb,
    recovery_id uuid REFERENCES vm_workspace_recoveries(id),
    refunded_at timestamptz,
    PRIMARY KEY (job_id, lease_token)
);
```

Include checks for recovery phases, immutable deadline fields, positive claim tokens, exact identity columns, request idempotency `(scope_kind, scope_id, request_id)`, append-only stop evidence, unique leased global/node slots, and exact `(recovery_id, pvc_uid)` retention pins.

- [ ] **Step 4: Implement stdlib-only shared wire types and the asyncpg store boundary**

```python
class WorkspaceRecoveryCode(str, Enum):
    RUNTIME_NOT_READY = "workspace_runtime_not_ready"
    TRANSPORT_UNAVAILABLE = "workspace_transport_unavailable"
    REPLACEMENT_OBSERVED = "workspace_replacement_observed"
    IDENTITY_CONFLICT = "workspace_identity_conflict"
    PRIOR_RUNTIME_UNFENCED = "prior_runtime_unfenced"
    TOOL_OUTCOME_UNKNOWN = "tool_outcome_unknown"
    CHECKPOINT_UNAVAILABLE = "checkpoint_unavailable"
    DEADLINE_EXCEEDED = "workspace_recovery_deadline_exceeded"

@dataclass(frozen=True, slots=True)
class WorkspaceRecoveryDisposition:
    code: WorkspaceRecoveryCode
    action: Literal["hold_committed", "paused_attention", "recovered"]
    operation_id: UUID
    accepted_lease_token: int
    hold_lease_token: int
```

Store methods must use database time, short transactions, queue-before-job ordering, typed return values, and compare-and-swap predicates on both `version` and `claim_token`.

- [ ] **Step 5: Regenerate the app schema snapshot with Podman and run the migration tests**

Run: `CONTAINER_ENGINE=podman scripts/schema-snapshot.sh app`

Run: `PYTHONPATH=src python -m pytest tests/test_vm_workspace_recovery_real_postgres.py tests/test_schema_capabilities_migration.py -q --tb=short`

Expected: PASS, including partial-unique, deadline, phase, request-idempotency, and stop-receipt constraints.

- [ ] **Step 6: Commit the durable schema and contract**

```bash
git add src/orchestrator/database/migrations/app/0249_vm_workspace_recovery.sql src/orchestrator/database/schema_current.sql src/shared/workspace_recovery.py src/orchestrator/services/vm_workspace_recovery_store.py tests/test_vm_workspace_recovery_real_postgres.py
git commit -m "feat: add durable VM workspace recovery records"
```

### Task 2: Exact Queue Hold and Attempt Accounting

**Files:**
- Modify: `src/shared/worker_queue.py`
- Modify: `src/shared/run_queue/queries.py`
- Modify: `src/orchestrator/services/vm_workspace_recovery_store.py`
- Modify: `tests/test_worker_queue.py`
- Modify: `tests/test_worker_driver_real_postgres.py`
- Modify: `tests/test_fenced_checkpointer.py`
- Modify: `tests/test_vm_workspace_recovery_real_postgres.py`

**Interfaces:**
- Consumes: Task 1 recovery tables and wire types.
- Produces: `park_worker_batch_for_workspace_recovery`, `release_worker_batch_from_workspace_recovery`, `record_worker_bundle_authorized`, and `get_worker_attempt_disposition`.
- Changes `claim_worker_batch` so its queue token increment, attempt counter increment, and `worker_batch_attempts` insert commit atomically.

- [ ] **Step 1: Add failing queue and PostgreSQL race tests**

```python
async def test_recovery_hold_rotates_token_without_resetting_failures(conn) -> None:
    await seed_leased_worker(conn, token=27, attempts=4, input_seq=9, consumed_seq=8)
    held = await park_worker_batch_for_workspace_recovery(
        conn, job_id=JOB_ID, accepted_lease_token=27, recovery_id=RECOVERY_ID
    )
    assert held.hold_lease_token == 28
    row = await fetch_queue(conn, JOB_ID)
    assert (row["state"], row["attempts_since_completion"]) == ("parked", 3)
    assert (row["input_seq"], row["consumed_seq"]) == (9, 8)
```

Cover one-time refund only when the exact existing attempt has `bundle_authorized_at IS NULL`, missing rows remaining unknown, concurrent saver finishing before the hold or failing afterward, and claimers skipping unresolved recovery participants.

- [ ] **Step 2: Run the targeted tests and confirm they fail on absent helpers/ledger writes**

Run: `PYTHONPATH=src python -m pytest tests/test_worker_queue.py tests/test_worker_driver_real_postgres.py tests/test_fenced_checkpointer.py tests/test_vm_workspace_recovery_real_postgres.py -q --tb=short`

Expected: FAIL on the new recovery-hold behavior.

- [ ] **Step 3: Make claim insertion atomic and add the exact-token park/release SQL**

```sql
UPDATE run_queue
SET state = 'parked',
    lease_token = lease_token + 1,
    attempts_since_completion = attempts_since_completion - $4::integer,
    leased_by = NULL,
    last_leased_by = NULL,
    leased_until = NULL,
    run_after = 'infinity'::timestamptz
WHERE unit_id = $1::uuid
  AND unit_kind = 'worker_batch'
  AND state = 'leased'
  AND lease_token = $2::bigint
RETURNING lease_token, attempts_since_completion, input_seq, consumed_seq;
```

Compute `$4` as one only after locking and validating the exact pre-bundle attempt. Release by recovery/participant CAS, preserve the adjusted counter, advance a synthetic input watermark once, and never call `reset_worker_batch_attempts` or generic `unpark_unit`.

- [ ] **Step 4: Add unresolved-recovery predicates to claim, renewal, completion, reaper composition, and membership writers**

```sql
AND NOT EXISTS (
    SELECT 1
    FROM vm_workspace_recovery_jobs AS recovery_job
    WHERE recovery_job.job_id = job.id
      AND recovery_job.resolved_at IS NULL
)
```

Child creation and reassignment take the canonical workspace advisory lock before publishing membership. Recovery materializes a parked queue row for any participant without one, then locks queues and jobs in UUID order.

- [ ] **Step 5: Run queue, saver, and concurrency tests**

Run: `PYTHONPATH=src python -m pytest tests/test_worker_queue.py tests/test_worker_driver_real_postgres.py tests/test_fenced_checkpointer.py tests/test_vm_workspace_recovery_real_postgres.py -q --tb=short`

Expected: PASS with stale saver/completion fencing and preserved genuine failures.

- [ ] **Step 6: Commit queue fencing and accounting**

```bash
git add src/shared/worker_queue.py src/shared/run_queue/queries.py src/orchestrator/services/vm_workspace_recovery_store.py tests/test_worker_queue.py tests/test_worker_driver_real_postgres.py tests/test_fenced_checkpointer.py tests/test_vm_workspace_recovery_real_postgres.py
git commit -m "feat: fence worker queues for workspace recovery"
```

### Task 3: Structured Bundle and Mid-Batch Recovery Protocol

**Files:**
- Modify: `src/orchestrator/services/unit_claim_bundle.py`
- Modify: `src/orchestrator/routers/unit_claim.py`
- Modify: `src/agent/api/orchestrator_client.py`
- Modify: `src/orchestrator/main.py`
- Modify: `tests/test_claim_bundle.py`
- Modify: `tests/test_turn_executor.py`

**Interfaces:**
- Consumes: Task 2 hold/disposition helpers.
- Produces: `POST /internal/units/{unit_id}/workspace-recovery` and `GET /internal/units/{unit_id}/workspace-recovery-disposition?lease_token=N`.
- Extends `ClaimBundleError` with `code: WorkspaceRecoveryCode | None` and `recovery: WorkspaceRecoveryDisposition | None`.

- [ ] **Step 1: Add failing service, router, and client tests for structured recovery**

```python
async def test_bundle_runtime_refusal_commits_and_replays_hold() -> None:
    first = await request_bundle(unit_id=UNIT_ID, lease_token=27)
    assert first.status_code == 409
    assert first.json()["recovery"]["action"] == "hold_committed"
    replay = await request_disposition(unit_id=UNIT_ID, lease_token=27)
    assert replay.json() == first.json()
```

Cover lost bundle response, lost hold-POST response, same request ID with a different intent, stale token replay lookup before ordinary authorization rejection, unknown 409 remaining generic, and no executable credential replay to stale tokens.

- [ ] **Step 2: Run protocol tests and confirm the endpoints and structured exception are missing**

Run: `PYTHONPATH=src python -m pytest tests/test_claim_bundle.py tests/test_turn_executor.py -q --tb=short`

Expected: FAIL for missing routes and fields.

- [ ] **Step 3: Record bundle authorization before credentials leave the service**

```python
await dependencies.recovery_store.record_bundle_authorized(
    conn,
    job_id=unit_id,
    lease_token=lease_token,
    authority_digest=authority_digest,
)
return response_payload
```

The record and live-lease/worker-Pod identity checks remain inside the existing authenticated service boundary. A recovery-eligible refusal calls `admit_hold` and returns its committed disposition.

- [ ] **Step 4: Add idempotent observation and disposition routes**

```python
@router.post("/internal/units/{unit_id}/workspace-recovery")
async def internal_workspace_recovery(unit_id: UUID, body: WorkspaceRecoveryReport, request: Request):
    require_internal(request)
    return await service.report_workspace_recovery(unit_id=unit_id, report=body)
```

The report carries exact lease token, authenticated worker identity, allowlisted reason code, and stable request ID. It cannot provide stop proof or successor identity.

- [ ] **Step 5: Parse the structured error and expose exact-disposition client methods**

```python
class ClaimBundleError(Exception):
    def __init__(self, status_code: int, detail: str, *, code=None, recovery=None):
        self.status_code = status_code
        self.detail = detail
        self.code = code
        self.recovery = recovery
        super().__init__(f"claim-bundle {status_code}: {detail[:200]}")
```

Add `report_workspace_recovery(...)` and `get_workspace_recovery_disposition(...)` using the same stable request/lease identities and bounded request timeouts.

- [ ] **Step 6: Run the bundle and client tests**

Run: `PYTHONPATH=src python -m pytest tests/test_claim_bundle.py tests/test_turn_executor.py -q --tb=short`

Expected: PASS for both first responses and exact idempotent replays.

- [ ] **Step 7: Commit the internal protocol**

```bash
git add src/orchestrator/services/unit_claim_bundle.py src/orchestrator/routers/unit_claim.py src/agent/api/orchestrator_client.py src/orchestrator/main.py tests/test_claim_bundle.py tests/test_turn_executor.py
git commit -m "feat: add workspace recovery worker protocol"
```

### Task 4: Worker Handoff and Reaper Composition

**Files:**
- Modify: `src/agent/api/turn_executor.py`
- Modify: `src/orchestrator/services/run_queue_reaper.py`
- Modify: `tests/test_stateless_worker_runtime.py`
- Modify: `tests/test_turn_executor.py`
- Modify: `tests/test_run_queue_reaper.py`

**Interfaces:**
- Consumes: Task 3 typed bundle and mid-batch protocol.
- Produces: worker-local `WorkspaceRecoveryHandoff` and `_handoff_workspace_recovery` behavior that quiesces claim-local work before releasing the executor slot.
- Produces: reaper logic that resolves exact attempt disposition before generic exhaustion.

- [ ] **Step 1: Extend the three named exhaustion regressions with failing recovery cases**

```python
@pytest.mark.parametrize("attempt", [1, 5])
async def test_workspace_refusal_hands_off_without_terminal_report(attempt: int) -> None:
    result = await run_worker_with_workspace_recovery_refusal(attempt=attempt)
    assert result.terminal_reports == []
    assert result.recovery_reports == 1
    assert result.executor_slot_released is True
```

Extend `test_last_pregraph_driver_failure_reports_visible_terminal_give_up`, `test_last_recoverable_attempt_reports_visible_terminal_give_up`, and `test_command_accept_queue_closure_is_not_misclassified_as_lease_loss`. Cover bundle acquisition, heartbeat/renewal, graph shutdown tail, claim five, ambiguous response disposition lookup, and local-quiescence failure quarantining the executor.

- [ ] **Step 2: Run the worker and reaper tests and observe generic terminal reporting**

Run: `PYTHONPATH=src python -m pytest tests/test_stateless_worker_runtime.py tests/test_turn_executor.py tests/test_run_queue_reaper.py -q --tb=short`

Expected: FAIL because typed recovery still reaches normal exhaustion paths.

- [ ] **Step 3: Add the recovery branch before every terminal/exhaustion branch**

```python
if workspace_recovery is not None:
    return await self._handoff_workspace_recovery(
        claim=claim,
        disposition=workspace_recovery,
        runtime=runtime,
    )
```

The handoff stops new tool admission, closes the graph generator, joins claim-local children and synchronous resource/SFTP calls, retires transports and immutable saver/graph references, preserves the remote shell, and leaves the process-wide checkpoint pool open. A non-quiescent runtime quarantines the executor.

- [ ] **Step 4: Reconcile attempt receipts in the expired-lease reaper path**

```python
disposition = await recovery_store.get_attempt_disposition(
    conn, job_id=unit_id, lease_token=expired_token
)
if disposition.requires_recovery_hold:
    await recovery_store.admit_hold_from_reaper(conn, disposition=disposition)
    return ReapOutcome.WORKSPACE_RECOVERY
```

Unknown post-authorization outcomes pause with `tool_outcome_unknown`; missing ledger rows never trigger a refund or replay.

- [ ] **Step 5: Run the worker, reaper, and original S1 baseline**

Run: `PYTHONPATH=src ./scripts/pytest-fast.sh tests/test_worker_queue.py tests/test_claim_bundle.py tests/test_stateless_worker_runtime.py tests/test_turn_executor.py tests/test_run_queue_reaper.py tests/test_fenced_checkpointer.py -q --tb=short`

Expected: PASS, including ordinary task failures retaining their existing terminal behavior.

- [ ] **Step 6: Commit worker handoff and reaper composition**

```bash
git add src/agent/api/turn_executor.py src/orchestrator/services/run_queue_reaper.py tests/test_stateless_worker_runtime.py tests/test_turn_executor.py tests/test_run_queue_reaper.py
git commit -m "feat: hand off workspace outages without exhausting jobs"
```

### Task 5: Preservation, Job Controls, and Safe Projection

**Files:**
- Modify: `src/orchestrator/services/completion_effects.py`
- Modify: `src/orchestrator/services/completion_sweep_router.py`
- Modify: `src/orchestrator/services/lifecycle/vm_manager.py`
- Modify: `src/orchestrator/services/retained_vm_workspaces.py`
- Modify: `src/orchestrator/services/job_projection.py`
- Modify: `src/orchestrator/schemas/job_list.py`
- Modify: `src/orchestrator/services/job_inspection.py`
- Modify: `src/orchestrator/services/job_liveness.py`
- Modify: `src/orchestrator/routers/job_controls.py`
- Modify: `src/orchestrator/services/job_controls.py`
- Modify: `policy/endpoint_inventory.txt`
- Modify: Cockpit job API model, list/detail components, status helpers, English translation, and German translation discovered with `rg "Resume|job status" cockpit/src`
- Modify: `tests/test_completion_effect_transactional.py`
- Modify: `tests/test_lifecycle_vm_manager.py`
- Modify: `tests/test_retained_vm_workspaces.py`
- Modify: `tests/test_checkpoint_retention.py`
- Modify: `tests/test_job_projection.py`
- Modify: `tests/test_job_list_schema.py`

**Interfaces:**
- Consumes: unresolved recovery participant and retention-pin authority.
- Produces: safe `workspace_recovery` projection and `POST /api/jobs/{job_id}/workspace-recovery/retry`.
- Routes generic Resume through recovery retry whenever unresolved participation exists.

- [ ] **Step 1: Add failing cleanup-race, projection-redaction, Resume, Retry, and Cancel tests**

```python
def test_projection_redacts_recovery_authority() -> None:
    projected = project_job(job_with_recovery(private_endpoint="10.0.0.9"))
    assert projected["workspace_recovery"] == {
        "operation_id": str(OPERATION_ID),
        "state": "paused_attention",
        "reason_code": "prior_runtime_unfenced",
        "message": "Previous workspace execution could not be proven stopped.",
        "started_at": STARTED_AT,
        "deadline_at": DEADLINE_AT,
        "next_check_at": None,
        "retryable": True,
        "cleanup_pending": False,
    }
    assert "10.0.0.9" not in json.dumps(projected)
```

Cover cleanup winning before hold, hold winning before cleanup, checkpoint retention, same request replay, different intent rejection, child-only retry refusal, generic Resume not clearing the hold, and Cancel resolving only the requesting participant.

- [ ] **Step 2: Run the targeted service and projection tests**

Run: `PYTHONPATH=src python -m pytest tests/test_completion_effect_transactional.py tests/test_lifecycle_vm_manager.py tests/test_retained_vm_workspaces.py tests/test_checkpoint_retention.py tests/test_job_projection.py tests/test_job_list_schema.py -q --tb=short`

Expected: FAIL because cleanup does not consult recovery authority and the projection/route are absent.

- [ ] **Step 3: Add cleanup admission guards before external delete or prune boundaries**

```python
permit = await recovery_store.acquire_cleanup_permit(
    conn, owner_kind=owner_kind, owner_id=owner_id, pvc_uid=pvc_uid
)
if not permit.allowed:
    return CleanupDisposition.HELD_FOR_WORKSPACE_RECOVERY
```

Preserve exact checkpoint/blob references and `context.vm`. Existing completion/lifecycle claims remain authoritative; recovery stands down if destructive cleanup already crossed its admission boundary.

- [ ] **Step 4: Add retry/control service behavior and the safe projection**

```python
class WorkspaceRecoveryView(BaseModel):
    operation_id: UUID
    state: str
    reason_code: str
    message: str
    started_at: datetime
    deadline_at: datetime
    next_check_at: datetime | None
    retryable: bool
    cleanup_pending: bool
```

Retry accepts `operation_id` and `request_id`, returns the stored result for an exact duplicate, and atomically transfers all unresolved participants to one successor recovery without a runnable gap.

- [ ] **Step 5: Update Cockpit rendering and interaction tests**

Run: `cd cockpit && npm test -- --runInBand`

Expected after implementation: list/detail distinguish recovery from active execution, show reason/deadline/retry/cancel, and expose no controller diagnostics.

- [ ] **Step 6: Run backend and Cockpit tests**

Run: `PYTHONPATH=src python -m pytest tests/test_completion_effect_transactional.py tests/test_lifecycle_vm_manager.py tests/test_retained_vm_workspaces.py tests/test_checkpoint_retention.py tests/test_job_projection.py tests/test_job_list_schema.py -q --tb=short`

Run: `cd cockpit && npm test -- --runInBand`

Expected: PASS.

- [ ] **Step 7: Commit preservation and controls**

```bash
git add src/orchestrator policy/endpoint_inventory.txt cockpit tests/test_completion_effect_transactional.py tests/test_lifecycle_vm_manager.py tests/test_retained_vm_workspaces.py tests/test_checkpoint_retention.py tests/test_job_projection.py tests/test_job_list_schema.py
git commit -m "feat: preserve and expose recoverable VM workspaces"
```

### Task 6: Bounded Recovery Reconciler

**Files:**
- Create: `src/orchestrator/services/vm_workspace_recovery.py`
- Create: `tests/test_vm_workspace_recovery.py`
- Modify: `src/orchestrator/services/vm_readiness.py`
- Modify: `src/orchestrator/services/vm_workspace_recovery_store.py`
- Modify: `src/orchestrator/main.py`
- Modify: `tests/test_vm_readiness.py`
- Modify: `tests/test_vm_workspace_recovery_real_postgres.py`

**Interfaces:**
- Consumes: durable store, cleanup holds, existing `VMReadinessService`, and controller observation client.
- Produces: `VMWorkspaceRecoveryService.run()`, `reconcile_once()`, and phase transitions `observing`, `waiting_runtime`, `verifying_stop`, `attesting`, `reconciling_outcome`, `paused_attention`, `recovered`, `cancelled`, `superseded`.

- [ ] **Step 1: Add failing fake-clock and competing-leader tests**

```python
async def test_probe_finishing_after_deadline_cannot_release() -> None:
    probe = DeferredProbe()
    task = asyncio.create_task(service.reconcile_once(OPERATION_ID, probe=probe))
    clock.advance(timedelta(seconds=901))
    probe.complete(ready_observation())
    await task
    assert await store.phase(OPERATION_ID) == "paused_attention"
    assert await queue_state(JOB_ID) == "parked"
```

Cover immutable deadline, restart/leader overlap, lost claim, cancellation, four global and one-per-node durable probe limits, jitter sequence capped at 60 seconds, changed generation/PVC/owner, unresolved remote operation, unknown tool outcome, and final re-attestation under CAS.

- [ ] **Step 2: Run recovery/readiness tests and confirm the service is absent**

Run: `PYTHONPATH=src python -m pytest tests/test_vm_workspace_recovery.py tests/test_vm_readiness.py tests/test_vm_workspace_recovery_real_postgres.py -q --tb=short`

Expected: FAIL on missing reconciler.

- [ ] **Step 3: Implement the short-claim/external-read/apply-result loop**

```python
async def reconcile_once(self, operation_id: UUID) -> None:
    claim = await self.store.claim_due(operation_id, ttl_seconds=30)
    if claim is None:
        return
    observation = await asyncio.wait_for(
        self.observer.observe(claim.captured_identity),
        timeout=min(10.0, claim.remaining_seconds),
    )
    await self.store.apply_observation(
        operation_id=claim.operation_id,
        version=claim.version,
        claim_token=claim.claim_token,
        observation=observation,
    )
```

Attention-paused operations are not claimed. Claim loss cancels local probes. Final release rechecks database time, participant hold tokens, immutable identity, readiness, checkpoint/tool disposition, and user/completion intent.

- [ ] **Step 4: Make readiness observation recovery-aware**

Existing readiness may probe but cannot promote `context.vm` while recovery owns authority. Only the recovery final CAS can bind successor VMI/launcher/IP/registration and release participants.

- [ ] **Step 5: Wire a supervised, leader-gated loop with feature-off pause behavior**

```python
if workspace_recovery_enabled():
    supervisors.start("vm-workspace-recovery", recovery_service.run)
```

Disabling automation leaves readers, holds, controls, and cleanup protection active; it moves due automatic work to a visible attention pause rather than releasing it.

- [ ] **Step 6: Run unit and real PostgreSQL recovery tests**

Run: `PYTHONPATH=src python -m pytest tests/test_vm_workspace_recovery.py tests/test_vm_readiness.py tests/test_vm_workspace_recovery_real_postgres.py -q --tb=short`

Expected: PASS for overlap, deadline, cancellation, limits, and stale-result rejection.

- [ ] **Step 7: Commit the reconciler**

```bash
git add src/orchestrator/services/vm_workspace_recovery.py src/orchestrator/services/vm_workspace_recovery_store.py src/orchestrator/services/vm_readiness.py src/orchestrator/main.py tests/test_vm_workspace_recovery.py tests/test_vm_readiness.py tests/test_vm_workspace_recovery_real_postgres.py
git commit -m "feat: reconcile VM workspace recovery durably"
```

### Task 7: Controller Retention and Exact Stop Evidence

**Files:**
- Modify: `src/vm_controller/controller.py`
- Modify: `src/vm_controller/retained_storage.py`
- Modify: the controller request/response model module discovered with `rg "query_status|status response" src/vm_controller`
- Modify: `src/orchestrator/services/vm_readiness.py`
- Modify: `src/orchestrator/services/vm_workspace_recovery.py`
- Modify: `helm/templates/vm-controller/configmap.yaml`
- Modify: controller RBAC only if the observation implementation demonstrates a missing read permission
- Modify: `tests/test_vm_controller.py`
- Modify: `tests/test_vm_retained_storage.py`
- Modify: `tests/test_vm_remote_operation_real_postgres.py`
- Modify: `tests/test_vm_workspace_recovery.py`

**Interfaces:**
- Consumes: exact recovery identity and retention pins.
- Produces: read-only `observe_workspace_recovery(captured_identity)` with VM/VMI/launcher/node/PVC identities, migration ambiguity, container termination evidence, and successor network observation.
- Produces: append-only trusted stop receipts; only validated receipts enable replacement recovery.

- [ ] **Step 1: Add failing controller provenance and destructive-GC tests**

```python
def test_missing_old_launcher_is_not_stop_proof() -> None:
    observation = controller.observe_workspace_recovery(captured_identity())
    assert observation.stop_evidence == "unknown"

def test_recovery_pin_prevents_failed_datavolume_recreation() -> None:
    controller._ensure_rootdisk(vm_with_failed_dv(), recovery_pins=[PIN])
    assert kube.deleted_data_volumes == []
    assert kube.created_data_volumes == []
```

Cover exact current `state.terminated` container IDs/restart counts/finished times, `lastState` rejection, restarted container rejection, NodeLost/ContainerStatusUnknown, force deletion/404, multiple launchers, migration, PVC mismatch, owner-reference mismatch, controller restart, rootdisk GC, and remote-operation lease blockers.

- [ ] **Step 2: Run controller/recovery tests and observe permissive current status behavior**

Run: `PYTHONPATH=src python -m pytest tests/test_vm_controller.py tests/test_vm_retained_storage.py tests/test_vm_remote_operation_real_postgres.py tests/test_vm_workspace_recovery.py -q --tb=short`

Expected: FAIL for missing read-only observation and retention pin behavior.

- [ ] **Step 3: Implement read-only exact observation and retention-aware GC**

```python
@dataclass(frozen=True, slots=True)
class WorkspaceRecoveryObservation:
    vm_uid: str
    vmi_uid: str | None
    launcher_uids: tuple[str, ...]
    node_uid: str | None
    root_pvc_uid: str
    migration_ambiguous: bool
    stop_evidence: Literal["proven", "unknown"]
    observed_at: datetime
```

The observation path must not call `_persist_status_identity`. `_gc_rootdisks`, `_ensure_rootdisk`, retained-storage detach/reserve, and cleanup permits honor exact PVC/recovery pins and fail closed when hold authority is unavailable.

- [ ] **Step 4: Validate and persist exact stop receipts before successor binding**

Require VM-to-VMI-to-Pod ownership, sole launcher/no migration, exact captured container IDs and restart counts, current terminated states for compute/QEMU and relevant sidecars/init containers, finished timestamps, terminal retirement preventing restart, and no other launcher for the old VMI.

- [ ] **Step 5: Add retained-disk network qualification behind the replacement feature flag**

Capture old/new interface MAC, effective netplan/networkd rules, cloud-init instance/cache identity, address, route, DNS, and guest registration. Persist a stable locally administered MAC for newly provisioned compatible images when the fixture proves MAC matching is required. Legacy unreachable guests stay paused; no automatic cloud-init cache cleaning occurs.

- [ ] **Step 6: Run controller, remote-operation, and recovery suites**

Run: `PYTHONPATH=src python -m pytest tests/test_vm_controller.py tests/test_vm_retained_storage.py tests/test_vm_remote_operation_real_postgres.py tests/test_vm_workspace_recovery.py -q --tb=short`

Expected: PASS for exact evidence, destructive-operation refusal, and pause-only fallback.

- [ ] **Step 7: Commit controller retention and evidence**

```bash
git add src/vm_controller src/orchestrator/services/vm_readiness.py src/orchestrator/services/vm_workspace_recovery.py helm tests/test_vm_controller.py tests/test_vm_retained_storage.py tests/test_vm_remote_operation_real_postgres.py tests/test_vm_workspace_recovery.py
git commit -m "feat: verify and retain recovering VM workspaces"
```

### Task 8: Rollout Configuration and Fault-Injection Acceptance

**Files:**
- Modify: Helm values and deployment templates discovered with `rg "VM_MAX_CONCURRENT" helm charts`
- Modify: `scripts/stateless-resilience-k3d-gate.py`
- Create: `scripts/vm-workspace-recovery-k3d-gate.py`
- Modify: deployment/configuration documentation that inventories worker/controller protocol compatibility
- Modify: tests for Helm values and environment parsing discovered with `rg "VM_MAX_CONCURRENT" tests`

**Interfaces:**
- Consumes: all prior tasks.
- Produces: default-off feature flags, `900` second budget, `4` global probes, `1` per-node probe, `30` second claims/permits, and `10` second external-call timeout.
- Produces: retained-disk fault gate covering response loss, leader overlap, slow boot, deadline, stop-evidence rejection, and marker/checkpoint survival.

- [ ] **Step 1: Add failing Helm/default and acceptance-harness tests**

```python
def test_workspace_recovery_defaults_are_safe() -> None:
    env = render_orchestrator_env(default_values())
    assert env["VM_WORKSPACE_RECOVERY_ENABLED"] == "false"
    assert env["VM_WORKSPACE_REPLACEMENT_RECOVERY_ENABLED"] == "false"
    assert env["VM_WORKSPACE_RECOVERY_DEADLINE_SECONDS"] == "900"
```

- [ ] **Step 2: Run Helm and harness tests and confirm the values are absent**

Run: `PYTHONPATH=src python -m pytest tests/test_helm_* tests/test_vm_workspace_recovery.py -q --tb=short`

Expected: FAIL on missing rollout settings.

- [ ] **Step 3: Add values, environment wiring, metrics, and structured audit events**

Expose operation state, age, reason, probe results, pause/retry/cancel, cleanup blockers, queue token transitions, and redacted controller observation digests. Metrics aggregate by code/phase and never label by job, VM, PVC, or operation ID.

- [ ] **Step 4: Implement the disposable retained-disk fault gate**

The gate requires real Kubernetes, KubeVirt, and Longhorn capabilities. It writes a marker and checkpoint, interrupts the launcher/storage path, observes the infrastructure-created replacement, verifies the original PVC UID and pinned identity, and accepts automatic continuation only with exact stop evidence. Missing termination evidence and forced deletion must end in attention pause.

- [ ] **Step 5: Regenerate schema and run targeted backend/Cockpit checks**

Run: `CONTAINER_ENGINE=podman scripts/schema-snapshot.sh app`

Run: `PYTHONPATH=src ./scripts/pytest-fast.sh tests/test_worker_queue.py tests/test_worker_driver_real_postgres.py tests/test_claim_bundle.py tests/test_stateless_worker_runtime.py tests/test_turn_executor.py tests/test_run_queue_reaper.py tests/test_fenced_checkpointer.py tests/test_vm_workspace_recovery_real_postgres.py tests/test_completion_effect_transactional.py tests/test_lifecycle_vm_manager.py tests/test_retained_vm_workspaces.py tests/test_checkpoint_retention.py tests/test_job_projection.py tests/test_job_list_schema.py tests/test_vm_workspace_recovery.py tests/test_vm_readiness.py tests/test_vm_controller.py tests/test_vm_retained_storage.py tests/test_vm_remote_operation_real_postgres.py -q --tb=short`

Run: `cd cockpit && npm test -- --runInBand`

Expected: PASS.

- [ ] **Step 6: Run lint/type/schema checks selected by the repository**

Run: `git diff --check`

Run: `python scripts/select_affected_tests.py --help`

Use the repository's documented changed-file selector output to run any additional Python, Helm, policy-inventory, i18n, and TypeScript checks it names.

- [ ] **Step 7: Run the disposable VM fault gate when the cluster advertises the required capabilities**

Run: `PYTHONPATH=src python scripts/vm-workspace-recovery-k3d-gate.py --help`

Then run its documented destructive-disposable-cluster command only against the gate-created cluster. Record `SKIPPED` with the missing capability if KubeVirt/Longhorn is unavailable; never substitute mocked Pod replacement as passing evidence.

- [ ] **Step 8: Commit rollout controls and acceptance coverage**

```bash
git add helm charts scripts docs src tests cockpit
git commit -m "feat: add VM workspace recovery rollout gates"
```
