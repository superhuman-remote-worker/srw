# C: idle compute release and authorized wake implementation seams

> For implementation: use `superpowers:executing-plans`, with review at each stage. This is a read-only design against the current worktree, not a claim that C is implemented or safe to enable.

**Goal:** Release compute after 15 minutes awaiting human input, retain checkpoint/files/environment, and let authorized Resume, SSH, or IDE restore the same workspace without granting execution or approval through access alone.

**Architecture:** One owner-aware policy and wake coordinator above existing retirement, cleanup, creation, and restore authorities. New scheduling/activity metadata must not become a second permission to create or delete resources. Physical operations continue to require the current exact runtime authority and its durable receipts.

**Scope:** Pinned/stateless jobs and sessions on supported VM/container backends. Resource reservations, count-limit replacement, numeric capacity budgets, and fleet occupancy UI are D and excluded. Preserve existing terminal retention policy and explicit warm/never-suspend overrides; C does not invent new terminal retention periods.

**Sources:** `.superpowers/next-vm-slices.md`; `knowledge-base/knowledge/issues/vm_reliability_roadmap_2026_09_19.md` C; `knowledge-base/knowledge/issues/workspace_idle_release_and_resource_based_admission.md`, especially its acceptance criteria. No cluster inspection or mutation performed.

## Defaults and invariants

- Proposed opt-in flag `WORKSPACE_IDLE_RELEASE_ENABLED=false`; human wait default 900 seconds. Reconcile every 60 seconds, matching existing lifecycle polling. Expiry means eligible for admission, never permission to issue an unfenced DELETE. Slow/blocked retirement remains visible.
- Human-wait entry changes only on an actual transition into a new wait episode. Polls, status updates, worker heartbeats, and access renewals cannot reset it. Leave/reenter wait creates a new episode. Explicit existing Extend behavior should use a bounded override for that episode; preserve existing configured Never semantics.
- Expired execution/access leases remove a policy veto; they do not prove remote processes stopped. Current claimant acknowledgement, operation settlement, process-zero proof, and exact captured teardown remain mandatory.
- Ordinary idle is compute-only release. It never purges the retained root disk or prunes the resume checkpoint. Finalization/cancellation/permanent failure use existing terminal funnels and retention rules, with visible blocked cleanup rather than pretending success.
- Access wake preserves job/thread state, pending questions, approvals, frozen state, execution deadline, and worker attempts. Only an explicit authorized execution action may resume the agent. Workspace Ready is separate from execution admission.
- New generation, cancellation, recovery enrollment, access acquisition, and wake all compete through the existing canonical owner and runtime authority locks. Sample database time and reread recovery participants after lock waits. A stale release must not stop a replacement.
- No Git-only fallback for a wake of an existing workspace. If required checkpoint, storage, or environment proof is missing, return bounded attention and retain existing compute/data. A separately requested fresh environment remains a different action.

## Exact existing seams and blockers

| Area | Current seam | Required integration / blocker |
| --- | --- | --- |
| Policy clock | `main.py:attention_sleep_sweeper` already handles pinned awaiting-user sessions; `agent_thread_status.py` and `shared/session_retirement.py:update_stateless_claim_status` maintain `threads.awaiting_user_since`. | Reuse the actual human-wait transition. Existing timestamp semantics include presence/explicit Extend behavior; normalize episode identity and bounded override deliberately. Jobs lack an equivalent dedicated field. Never use `jobs.updated_at`. |
| Misleading timers | `workspace_idle_sweeper` only repairs missing/failed sessions; `WorkspaceSuspensionService.check_idle_all/check_idle_threads` are unused and use activity timestamps. | Do not call these unused functions as the implementation. Route opted-in owners through one policy; existing attention timer must not compete with it. |
| Execution/dependents | `lifecycle/vm_manager.py` and `workspace_manager.py` exclude stateless and protect shared children/completion. Existing worker/session claims and pinned retirement identify the claimant. | Preserve protections until equivalent lease-aware admission exists. Child work must protect the canonical shared runtime; a child's stale status alone is not indefinite authority. |
| VM job suspension | `workspace_suspension.py:suspend_workspace` has captured VM identity, renewable remote-operation lease, closed-I/O marker, cleanup permit, and `release_vm_captured`. | Adapt to explicit idle reason and **nonpurging retained-root** capability. Current `is_enabled` depends on S3 even when an exact retained root disk is sufficient; split capability checks, not safety checks. |
| VM restore | `restore_workspace` sets restoring then calls ordinary `create_vm(job_id)` with current options. | This is not a complete idempotent wake contract. Use frozen source/request and exact predecessor retirement through the existing creation authority; preserve attention/capacity state and wait for actual Ready. |
| A1 creation | `vm_creation_preflight.py`, retry store/transport, readiness, dispatch. | Currently job/execution-oriented. Access-only wake must be an explicit admitted purpose preserving owner status/freeze/queue, not public Resume or normal execution dispatch. Session VM creation requires extension of the same owner-aware protocol; do not clone a second create authority. |
| Pinned session retirement | `thread_retirement.py:end_thread_flow(...permanent=False, settle_status="suspended")`, `pinned_retirement.py`. | Reuse captured agent/runtime and acknowledged quiescence. Suspending a workspace is not ending a conversation; preserve checkpoint and question. |
| Stateless session retirement | `reconcile_stateless_thread_retirement`, `shared/session_retirement.py`, queue closure, exact claimant acknowledgement, strict snapshot/restore debt. | Existing legacy suspension explicitly refuses stateless, and acknowledgement paths have `ended` assumptions. Add typed compute-suspension intent through the existing acknowledged funnel. Never remove the refusal before that path works. |
| Container suspension | `WorkspaceSuspensionService.suspend_workspace` refuses Kubernetes pre-delete capture and static Docker recreation. | Genuine authority gap: cleanup permission at teardown does not authorize earlier capture. Need durable exact-runtime capture lease/receipt, writer quiescence, verified snapshot, then teardown. Unsupported static Docker remains an explicit refusal until attested recreation exists. |
| Container restore | `workspace_lifecycle.ensure_workspace`, suspension `restore/restore_thread_workspace`, `_claim_workspace_restore_work`, restore heartbeat and strict Ready publication. | Reuse existing creation reservation and B restore ownership; required snapshot extraction cannot fall through to empty workspace/Ready. Lost replies query the durable result. |
| SSH | `ssh_access.resolve_target` authorizes first, then rejects VM tier; gateway `_attached_target` caches successful per-connection resolution, attachment records audit open/close. | Add bounded ensure-awake after authorization, then obtain a new exact attested target. Audit rows are not expiring activity leases. VM routing/host-key evidence needs real implementation; removing `STATE_VM_UNSUPPORTED` alone is unsafe. |
| IDE | `ide_proxy.py`, `routers/ide.py`, `ide_session.py`. | Existing active/idle labels can pin compute; replace only after bounded access proof exists. Mutating HTTP currently returns `ide_mutation_operation_lease_unavailable`; point-in-time attestation cannot authorize writes during capture. Restore claim/heartbeat is reusable. |
| Presence | `thread_presence.py`, migration 0125. | Existing 30-second browser presence is explicitly UX-only, not authorization or runtime fencing. SSE polling must not become an idle hold or reset the human clock. |
| Checkpoints | `agent/agent.py` distinguishes canonical Postgres and local SQLite resume; stateless quiescence acknowledges after turn unwind. | Prove the correct durable checkpoint before release. Local SQLite must be included in the preserved artifact/disk and flushed. Audit normal checkpoint retention against suspended owners; never create a second checkpoint owner mapping or call prune as part of idle. |

## Stage 1 — pure policy and durable human-wait episode

**Own:** new `src/shared/workspace_idle_policy.py`, its unit tests; one migration and bounded database helpers; actual transition adapters in job completion/blocking-message/control and thread status services. Do not edit destructive controller methods in this stage.

Provide typed inputs `IdleEpisode`, `ActivityLeaseView`, `RuntimeIdentity`, and a pure decision (`warm`, `held`, `eligible`, `blocked`) with bounded reasons. Eligibility requires supported lane, current human-wait episode, elapsed warm interval, no active claim/dependent/access lease, and no incompatible recovery/cleanup/restore state. Malformed identity/time is blocked. Terminal finalization is a separate reason using existing finalization authority, not an artificial human-wait clock.

Store a monotonically fenced episode/revision with entry time on the existing owner; reuse thread entry semantics where possible and add a dedicated job clock. Expose an explicit override expiry rather than mutating entry on polling. Initialize historical rows without proven entry conservatively at migration/first valid transition, not from an arbitrary old `updated_at`. Plain paused-but-dispatchable, capacity wait, preparation, and provisioning are not human idle.

**RED/GREEN:** 899/900-second boundary; repeated polls; wait exit/reentry; explicit Extend/Never; malformed/future/naive time; 26-hour capacity wait not idle; paused unfrozen not idle; new-generation/reset; existing child/completion/recovery holds; database race between transition and old episode admission.

## Stage 2 — bounded activity holds and atomic release admission

**Own:** a small owner-aware store/coordinator module and database helpers; adapters for existing worker/session/pinned claim facts; SSH/IDE lease methods. Reuse `vm_remote_operation.py` and existing container lifecycle scope for runtime I/O authority.

Do not mint duplicate worker execution leases. Read and fence existing claims; derive dependent protection from actual live claimant plus shared owner/runtime identity. Access liveness metadata may use a new expiring row keyed by canonical owner, exact runtime generation/UID, token/revision, kind, and expiry, but it confers no user permission or physical-effect authority.

Use existing renewable operation TTLs where applicable (VM remote operations default to 300 seconds); configure a renewal cadence shorter than expiry and bound it to connection/operation lifetime. No new human idle period is inferred from that technical TTL. Lease renew/acquire and release admission must serialize: lease wins => no release; release wins => access receives restoring/retry and no old-runtime target. Renewal cannot revive a closed old generation.

SSH/IDE keepalive and browser polling are not human activity. Renew for a live authorized operation/channel with actual relevant work; long-running exec needs an explicit work hold, even when silent. Abandonment cancels/joins local I/O and settles the existing operation lease. If a remote command persists, process retirement still decides whether release is safe. Generic `ide_session.status=active` cannot substitute for this proof.

**RED/GREEN:** lease expiry/renewal races across two DB connections; lease token/generation mismatch; quiet live execution protected; stale child status expires only through its real claimant lifecycle; disconnected access eventually eligible; live remote process still blocks teardown; fresh post-lock recovery enrollment; acquisition denied after admitted capture closure.

## Stage 3 — VM job compute-only release

**Own:** `workspace_suspension.py`, `vm_remote_operation.py` only if its operation contract needs extension, existing cleanup admission helpers, focused tests. Keep VM controller teardown authority and process-zero implementation intact.

Add a typed release reason/episode to the existing suspension entry. Revalidate clock/leases/control/recovery/exact VM/PVC after authority locks. Persist intent referencing the existing captured cleanup permit, rather than granting deletion itself. Establish no active execution plus checkpoint persistence; flush local checkpoint if applicable. Require exact retained root and `purge_disk=False` for ordinary idle. Close writes, obtain process-zero proof, perform captured teardown, and mark suspended only from the matching completed receipt. A crash between steps reconciles that same receipt. No S3 dependency is necessary for a proven persistent root, but absence of either retained storage or a verified full required snapshot is blocked.

**RED/GREEN:** sentinel uncommitted file + installed-tool path + checkpoint retained; snapshot failure with durable root allowed only under explicit retained-root proof; no durable root refused; lost delete reply; restart; wake/control change during job-row wait; stale VM UID; process-zero refusal; failed release remains counted as running/cleanup pending (physical state, not a new D reservation).

## Stage 4 — shared idempotent ensure-awake, VM job first

**Own:** new `workspace_awake.py` coordinator, existing A1 preflight/retry/readiness adapters, suspension restore entry; scoped real-PG tests. Coordinate store edits with A1 owner.

Interface: `ensure_awake(owner, purpose, authorized_context, expected_owner_revision=None)` -> `ready | waking | waiting | blocked`, durable operation ID and bounded reason; exact target only after readiness attestation. Purposes distinguish `access` from explicit execution request. Authorization is checked by the caller before admission and must be preserved through the request; this method is not a public permission bypass.

Under canonical locks, reuse a matching in-progress wake; otherwise require settled predecessor cleanup and durable source proof and admit one successor through existing creation/restore authority. Freeze its request before I/O. Access admission must preserve status, queue, attempts, approval/frozen flags and original execution deadline. Existing A1 job Resume/admission is not automatically suitable: its execution guards must be extended explicitly. An expired execution deadline must not be silently extended to make wake possible; define access eligibility using existing owner-access policy, keeping execution ineligible. Until the existing admission can express that separation, return blocked rather than reset deadline or enqueue.

Ready closes only the matching workspace operation, never dispatches a worker for access. Concurrent Resume/SSH/IDE share the runtime operation; execution admission remains the ordinary authorized dispatcher after repository/credential/workspace checks. Old teardown remains fenced to the predecessor UID. Distinguish actual readiness from a truthy create acknowledgement.

**RED/GREEN:** two-replica simultaneous three-purpose wake produces one successor; canceled/expired execution cannot restart agent; access leaves pending question and queue unchanged; restart/lost create reply adopts; recovery starts during lock wait; wrong predecessor receipt; missing retained source; successful workspace Ready still no worker lease; explicit later Resume resumes the stored checkpoint.

## Stage 5 — pinned/stateless session adapters and container durability

Split this stage into independently reviewable slices; do not claim lane coverage from VM-job success.

1. **Pinned session compute suspension:** own `thread_retirement.py`, `pinned_retirement.py`, session restore adapter. Use existing `settle_status="suspended"` with exact captured agent quiescence. Preserve local durable checkpoint, workspace files and required environment; avoid sending an execution-resume message just to restore access.
2. **Stateless job/session suspension:** own `shared/session_retirement.py`, its worker acknowledgement caller, queue helpers and retirement reconciler. Add compute-suspension intent to the existing closure/ack protocol, including current `ended` predicates. No acknowledgement from lease expiry alone; no worker attempt spent by suspension/replay. Session VM wake must extend existing owner-aware creation protocol rather than calling the job API with a thread UUID.
3. **Kubernetes durable capture and restore:** own suspension service, container lifecycle capture/restore store and controller-attested operations. Capture authority must precede network capture, bind exact pod UID/generation/owner, close writers and survive restart. Verified manifest + settled capture must precede deletion. Reuse existing strict restore reservation/lease and Ready publication. Establish required environment coverage: preserving a workspace tar alone does not prove tools installed into a disposable container writable layer survive. Require supported persistent mounts or an explicitly verified image/environment capture contract; do not label an unsupported layer restored.

**RED/GREEN for each lane:** same sentinel+checkpoint+tool fixture; active claimant finishes then exact ack; claim loss without ack blocks; capture failure retains runtime; successor pod UID immune to stale delete; tampered manifest refused; extraction debt survives restart and never publishes Ready early; restore twice one runtime; first access no agent execution. Static Docker remains refused until recreation/identity authority is implemented and tested.

## Stage 6 — authorized access and lifecycle integration

**Own:** `ssh_access.py`, `ssh_gateway_server.py`, `ide_proxy.py`, `routers/ide.py`, `ide_session.py`; then `main.py`, lifecycle VM/workspace managers and minimal bounded owner projection.

Authorize owner before calling ensure-awake. SSH uses existing bounded orchestrator request budget (currently 10 seconds); a slow boot returns retryable progress rather than blocking indefinitely or fabricating an endpoint. Successful resolution reattests the successor. VM SSH needs actual runtime routing and pinned host-key support. IDE shows restoring/waiting/blocked and obtains the same wake ID; mutations and streams require operation lifetime authority, not only a target lookup. Recheck authorization/identity before publishing the final target.

Wire one 60-second policy reconciler behind the default-off flag. Existing pinned attention sleep remains compatible for owners outside the new capability/flag; opted-in owners use one clock and funnel. Keep stateless/recovery/completion exclusions until their new adapters pass; do not globally weaken generic reaper guards. Disable a lane's new admission safely while continuing already admitted retirement/restore settlement. Terminal paths reuse verified finalization and expose blocked reasons/explicit existing retention expiry.

**RED/GREEN:** unauthorized/not-found never wakes or leaks existence; repeated authorized requests share operation; no automatic approval or Resume; HTTP mutation during capture refused; gateway disconnect closes hold; WS lease loss closes tasks; no browser heartbeat extends wait; default flag false preserves legacy behavior; disabling after admission still reconciles; terminal cleanup cannot be masked by idle state.

## Release gates and narrow ownership order

Implement stages 1–2 without physical effects, then VM-job stages 3–4. Review and prove that lane on disposable resources before enabling it. Implement the three stage-5 slices serially where they share retirement/store files; advertise support only after each lane's acceptance passes. Stage 6 can add access presentation first, but must not advertise wake for unimplemented backends.

Use the existing Helm config path for the default-off flag and 900-second/60-second settings. Preserve explicit existing per-owner overrides. Do not change production configuration in this work. Live acceptance must cover two orchestrator/controller replicas, process restart/lost replies, a sentinel uncommitted file, a required installed tool, checkpoint continuation, active SSH/IDE/dependent work, expired abandoned holds, simultaneous wake, stale cleanup against successor, and terminal cleanup blockage. Confirm physical compute disappearance while storage remains. Unit/PG results alone do not establish this gate.

No numerical capacity budgets or new reservations are selected here. When D lands, it consumes the same exact physical lifecycle facts; C must not manufacture free capacity by clearing a context marker.
